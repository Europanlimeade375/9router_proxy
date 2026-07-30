use crate::config::Config;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use rand::seq::SliceRandom;
use reqwest::{Client, Proxy, Url, redirect::Policy};
use serde::Deserialize;
use std::{
    collections::{HashMap, HashSet},
    error::Error,
    fmt, fs, io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpStream, lookup_host},
    sync::{RwLock, Semaphore},
    task::JoinSet,
    time::{sleep, timeout},
};
use tokio_socks::tcp::{Socks4Stream, Socks5Stream};

const SOURCE_CATALOG_MAX_BYTES: u64 = 2 * 1024 * 1024;
const MAX_PROXY_LINE_BYTES: usize = 512;
const MAX_CONSECUTIVE_FAILURES: u32 = 2;

#[derive(Debug)]
pub(crate) struct ProxyPool {
    config: Config,
    snapshot: RwLock<PoolSnapshot>,
    dynamic: bool,
}

#[derive(Debug)]
struct PoolSnapshot {
    entries: Vec<Arc<ProxyEntry>>,
    generation: u64,
    last_successful_refresh: Instant,
}

#[derive(Debug)]
struct ProxyEntry {
    client: Client,
    proxy_url: Option<Url>,
    public: bool,
    consecutive_failures: AtomicU32,
}

#[derive(Clone, Debug)]
struct Candidate {
    url: Url,
    public: bool,
}

#[derive(Debug)]
struct WarmedCandidate {
    entry: Arc<ProxyEntry>,
    latency: Duration,
}

#[derive(Debug, Deserialize)]
struct SourceCatalog {
    #[serde(default)]
    sources: Vec<SourceDefinition>,
}

#[derive(Clone, Debug, Deserialize)]
struct SourceDefinition {
    url: String,
    #[serde(rename = "type", default)]
    kind: String,
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    validated: bool,
    #[serde(default)]
    protocol: Option<String>,
    #[serde(default)]
    name: String,
}

#[derive(Clone, Copy, Debug)]
enum BareProxyProtocol {
    Http,
    Socks4a,
    Socks5h,
}

impl BareProxyProtocol {
    fn scheme(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Socks4a => "socks4a",
            Self::Socks5h => "socks5h",
        }
    }
}

#[derive(Debug)]
pub(crate) struct SelectedProxy {
    entry: Arc<ProxyEntry>,
}

pub(crate) trait TunnelIo: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T> TunnelIo for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

pub(crate) type TunnelStream = Box<dyn TunnelIo>;

impl SelectedProxy {
    pub(crate) fn client(&self) -> Client {
        self.entry.client.clone()
    }

    pub(crate) fn record_connect_failure(&self) {
        self.entry
            .consecutive_failures
            .fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Debug)]
pub(crate) struct PoolHealth {
    pub(crate) ready: bool,
    pub(crate) available: usize,
    pub(crate) total: usize,
    pub(crate) tunnel_available: usize,
    pub(crate) generation: u64,
    pub(crate) age_secs: u64,
}

#[derive(Debug)]
pub(crate) struct ProxyPoolError(String);

impl ProxyPoolError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for ProxyPoolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for ProxyPoolError {}

impl ProxyPool {
    pub(crate) async fn initialize(config: &Config) -> Result<Arc<Self>, ProxyPoolError> {
        if config.allow_direct {
            let client = production_client(config, None)?;
            return Ok(Arc::new(Self {
                config: config.clone(),
                snapshot: RwLock::new(PoolSnapshot {
                    entries: vec![Arc::new(ProxyEntry {
                        client,
                        proxy_url: None,
                        public: false,
                        consecutive_failures: AtomicU32::new(0),
                    })],
                    generation: 1,
                    last_successful_refresh: Instant::now(),
                }),
                dynamic: false,
            }));
        }

        let pool = Arc::new(Self {
            config: config.clone(),
            snapshot: RwLock::new(PoolSnapshot {
                entries: Vec::new(),
                generation: 0,
                last_successful_refresh: Instant::now(),
            }),
            dynamic: true,
        });
        pool.refresh().await?;
        Ok(pool)
    }

    pub(crate) fn spawn_refresh(self: &Arc<Self>) {
        if !self.dynamic {
            return;
        }
        let pool = self.clone();
        tokio::spawn(async move {
            loop {
                let jitter_limit = pool.config.proxy_refresh_interval.as_secs() / 10;
                let jitter = if jitter_limit == 0 {
                    0
                } else {
                    rand::random::<u64>() % (jitter_limit + 1)
                };
                sleep(pool.config.proxy_refresh_interval + Duration::from_secs(jitter)).await;
                if let Err(error) = pool.refresh().await {
                    tracing::warn!(%error, "proxy pool refresh failed; retaining previous pool");
                }
            }
        });
    }

    pub(crate) async fn select(&self) -> Option<SelectedProxy> {
        let snapshot = self.snapshot.read().await;
        let eligible = snapshot
            .entries
            .iter()
            .filter(|entry| {
                entry.consecutive_failures.load(Ordering::Relaxed) < MAX_CONSECUTIVE_FAILURES
            })
            .collect::<Vec<_>>();
        let entry = eligible.choose(&mut rand::thread_rng())?;
        Some(SelectedProxy {
            entry: (*entry).clone(),
        })
    }

    pub(crate) async fn open_tunnel(&self, target: &str) -> Result<TunnelStream, ProxyPoolError> {
        let selected = self
            .select_tunnel()
            .await
            .ok_or_else(|| ProxyPoolError::new("no warmed proxy supports CONNECT tunneling"))?;
        match timeout(
            self.config.forward_connect_timeout,
            open_tunnel(&selected.entry, target),
        )
        .await
        {
            Ok(Ok(stream)) => Ok(stream),
            Ok(Err(error)) => {
                selected.record_connect_failure();
                Err(error)
            }
            Err(_) => {
                selected.record_connect_failure();
                Err(ProxyPoolError::new("upstream CONNECT tunnel timed out"))
            }
        }
    }

    async fn select_tunnel(&self) -> Option<SelectedProxy> {
        let snapshot = self.snapshot.read().await;
        let eligible = snapshot
            .entries
            .iter()
            .filter(|entry| {
                entry.consecutive_failures.load(Ordering::Relaxed) < MAX_CONSECUTIVE_FAILURES
                    && tunnel_capable(entry)
            })
            .collect::<Vec<_>>();
        let entry = eligible.choose(&mut rand::thread_rng())?;
        Some(SelectedProxy {
            entry: (*entry).clone(),
        })
    }

    pub(crate) async fn health(&self) -> PoolHealth {
        let snapshot = self.snapshot.read().await;
        let available = snapshot
            .entries
            .iter()
            .filter(|entry| {
                entry.consecutive_failures.load(Ordering::Relaxed) < MAX_CONSECUTIVE_FAILURES
            })
            .count();
        let tunnel_available = snapshot
            .entries
            .iter()
            .filter(|entry| {
                entry.consecutive_failures.load(Ordering::Relaxed) < MAX_CONSECUTIVE_FAILURES
                    && tunnel_capable(entry)
            })
            .count();
        let age = snapshot.last_successful_refresh.elapsed();
        let fresh = !self.dynamic || age <= self.config.proxy_pool_stale_after;
        PoolHealth {
            ready: available > 0 && fresh,
            available,
            total: snapshot.entries.len(),
            tunnel_available,
            generation: snapshot.generation,
            age_secs: age.as_secs(),
        }
    }

    async fn refresh(&self) -> Result<(), ProxyPoolError> {
        let candidates = self.collect_candidates().await;
        if candidates.is_empty() {
            return Err(ProxyPoolError::new("no proxy candidates were available"));
        }

        let warmed = self.warm_candidates(candidates).await;
        let entries = select_best(warmed, self.config.max_warmed_proxies);
        if entries.is_empty() {
            return Err(ProxyPoolError::new(
                "no proxy candidate could connect to TARGET_BACKEND_URL",
            ));
        }

        let public_urls = entries
            .iter()
            .filter(|entry| entry.public)
            .filter_map(|entry| entry.proxy_url.as_ref())
            .filter(|url| url.username().is_empty() && url.password().is_none())
            .cloned()
            .collect::<Vec<_>>();

        let (generation, count) = {
            let mut snapshot = self.snapshot.write().await;
            snapshot.entries = entries;
            snapshot.generation = snapshot.generation.saturating_add(1);
            snapshot.last_successful_refresh = Instant::now();
            (snapshot.generation, snapshot.entries.len())
        };

        if let Err(error) = write_cache(&self.config.proxy_cache_path, &public_urls) {
            tracing::warn!(%error, "failed to persist warmed proxy cache");
        }
        tracing::info!(
            generation,
            proxies = count,
            "proxy pool warmed and replaced"
        );
        Ok(())
    }

    async fn collect_candidates(&self) -> Vec<Candidate> {
        let mut candidates = Vec::new();
        for url in &self.config.proxy_urls {
            candidates.push(Candidate {
                url: url.clone(),
                public: false,
            });
        }

        if let Some(path) = &self.config.outbound_proxies_file {
            match read_bounded(path, self.config.proxy_local_file_max_bytes as u64) {
                Ok(contents) => candidates.extend(parse_proxy_lines(
                    &contents,
                    None,
                    true,
                    &self.config.public_proxy_allowed_ports,
                    self.config.proxy_candidates_per_source,
                )),
                Err(error) => tracing::warn!(%error, "failed to read OUTBOUND_PROXIES_FILE"),
            }
        }

        if let Ok(contents) = read_cache_with_backup(
            &self.config.proxy_cache_path,
            self.config.proxy_local_file_max_bytes as u64,
        ) {
            candidates.extend(parse_proxy_lines(
                &contents,
                None,
                true,
                &self.config.public_proxy_allowed_ports,
                self.config.proxy_candidates_per_source,
            ));
        }

        {
            let snapshot = self.snapshot.read().await;
            candidates.extend(snapshot.entries.iter().filter_map(|entry| {
                entry.proxy_url.as_ref().and_then(|url| {
                    entry.public.then(|| Candidate {
                        url: url.clone(),
                        public: true,
                    })
                })
            }));
        }

        if let Some(path) = &self.config.proxy_sources_path {
            match load_source_catalog(path, self.config.max_proxy_sources) {
                Ok(sources) => candidates.extend(self.fetch_sources(sources).await),
                Err(error) => tracing::warn!(%error, "failed to load PROXY_SOURCES_PATH"),
            }
        }

        deduplicate_candidates(candidates, self.config.max_proxy_warm_candidates)
    }

    async fn fetch_sources(&self, sources: Vec<SourceDefinition>) -> Vec<Candidate> {
        let mut tasks = JoinSet::new();
        for source in sources {
            let config = self.config.clone();
            tasks.spawn(async move { fetch_source(&config, source).await });
        }

        let mut candidates = Vec::new();
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(Ok(mut source_candidates)) => candidates.append(&mut source_candidates),
                Ok(Err(error)) => tracing::warn!(%error, "proxy source fetch failed"),
                Err(error) => tracing::warn!(%error, "proxy source task failed"),
            }
        }
        candidates
    }

    async fn warm_candidates(&self, candidates: Vec<Candidate>) -> Vec<WarmedCandidate> {
        let semaphore = Arc::new(Semaphore::new(self.config.proxy_warm_concurrency));
        let mut tasks = JoinSet::new();
        for candidate in candidates {
            let semaphore = semaphore.clone();
            let config = self.config.clone();
            tasks.spawn(async move {
                let permit = semaphore.acquire_owned().await.ok()?;
                let result = warm_candidate(&config, candidate).await;
                drop(permit);
                result
            });
        }

        let mut warmed = Vec::new();
        while let Some(result) = tasks.join_next().await {
            if let Ok(Some(candidate)) = result {
                warmed.push(candidate);
            }
        }
        warmed
    }

    #[cfg(test)]
    pub(crate) fn direct_for_test(client: Client) -> Arc<Self> {
        let config = Config::test_direct();
        Arc::new(Self {
            config,
            snapshot: RwLock::new(PoolSnapshot {
                entries: vec![Arc::new(ProxyEntry {
                    client,
                    proxy_url: None,
                    public: false,
                    consecutive_failures: AtomicU32::new(0),
                })],
                generation: 1,
                last_successful_refresh: Instant::now(),
            }),
            dynamic: false,
        })
    }
}

fn tunnel_capable(entry: &ProxyEntry) -> bool {
    let Some(url) = entry.proxy_url.as_ref() else {
        return true;
    };
    match url.scheme() {
        "http" | "socks5" | "socks5h" => true,
        "socks4" | "socks4a" => url.username().is_empty() && url.password().is_none(),
        _ => false,
    }
}

async fn open_tunnel(entry: &ProxyEntry, target: &str) -> Result<TunnelStream, ProxyPoolError> {
    let Some(proxy_url) = entry.proxy_url.as_ref() else {
        let stream = TcpStream::connect(target)
            .await
            .map_err(|_| ProxyPoolError::new("direct CONNECT target failed"))?;
        return Ok(Box::new(stream) as TunnelStream);
    };

    match proxy_url.scheme() {
        "http" => open_http_proxy_tunnel(proxy_url, target).await,
        "socks4" | "socks4a" => {
            let proxy = proxy_socket_address(proxy_url)?;
            let stream = Socks4Stream::connect(proxy.as_str(), target)
                .await
                .map_err(|_| ProxyPoolError::new("SOCKS4 CONNECT failed"))?;
            Ok(Box::new(stream) as TunnelStream)
        }
        "socks5" | "socks5h" => {
            let proxy = proxy_socket_address(proxy_url)?;
            let stream = if proxy_url.username().is_empty() {
                Socks5Stream::connect(proxy.as_str(), target)
                    .await
                    .map_err(|_| ProxyPoolError::new("SOCKS5 CONNECT failed"))?
            } else {
                Socks5Stream::connect_with_password(
                    proxy.as_str(),
                    target,
                    proxy_url.username(),
                    proxy_url.password().unwrap_or_default(),
                )
                .await
                .map_err(|_| ProxyPoolError::new("authenticated SOCKS5 CONNECT failed"))?
            };
            Ok(Box::new(stream) as TunnelStream)
        }
        _ => Err(ProxyPoolError::new(
            "selected proxy scheme does not support raw CONNECT tunneling",
        )),
    }
}

async fn open_http_proxy_tunnel(
    proxy_url: &Url,
    target: &str,
) -> Result<TunnelStream, ProxyPoolError> {
    let proxy_host = proxy_url
        .host_str()
        .ok_or_else(|| ProxyPoolError::new("HTTP proxy has no host"))?
        .trim_start_matches('[')
        .trim_end_matches(']');
    let proxy_port = proxy_url
        .port()
        .ok_or_else(|| ProxyPoolError::new("HTTP proxy has no port"))?;
    let mut stream = TcpStream::connect((proxy_host, proxy_port))
        .await
        .map_err(|_| ProxyPoolError::new("HTTP proxy connection failed"))?;

    let authorization = if proxy_url.username().is_empty() {
        String::new()
    } else {
        let credentials = format!(
            "{}:{}",
            proxy_url.username(),
            proxy_url.password().unwrap_or_default()
        );
        format!(
            "Proxy-Authorization: Basic {}\r\n",
            BASE64_STANDARD.encode(credentials)
        )
    };
    let request = format!(
        "CONNECT {target} HTTP/1.1\r\nHost: {target}\r\nProxy-Connection: Keep-Alive\r\n{authorization}\r\n"
    );
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|_| ProxyPoolError::new("failed to write HTTP CONNECT request"))?;

    let mut headers = Vec::with_capacity(1024);
    let mut byte = [0_u8; 1];
    while !headers.ends_with(b"\r\n\r\n") {
        if headers.len() >= 16 * 1024 {
            return Err(ProxyPoolError::new(
                "HTTP proxy CONNECT response headers were too large",
            ));
        }
        let read = stream
            .read(&mut byte)
            .await
            .map_err(|_| ProxyPoolError::new("failed to read HTTP CONNECT response"))?;
        if read == 0 {
            return Err(ProxyPoolError::new(
                "HTTP proxy closed the CONNECT response",
            ));
        }
        headers.push(byte[0]);
    }
    let status = std::str::from_utf8(&headers)
        .ok()
        .and_then(|headers| headers.lines().next())
        .and_then(|line| line.split_ascii_whitespace().nth(1));
    if status != Some("200") {
        return Err(ProxyPoolError::new(
            "HTTP proxy rejected the CONNECT request",
        ));
    }
    Ok(Box::new(stream) as TunnelStream)
}

fn proxy_socket_address(url: &Url) -> Result<String, ProxyPoolError> {
    let host = url
        .host_str()
        .ok_or_else(|| ProxyPoolError::new("SOCKS proxy has no host"))?;
    let port = url
        .port()
        .ok_or_else(|| ProxyPoolError::new("SOCKS proxy has no port"))?;
    Ok(format!("{host}:{port}"))
}

fn production_client(config: &Config, proxy_url: Option<&Url>) -> Result<Client, ProxyPoolError> {
    let mut builder = Client::builder()
        .no_proxy()
        .connect_timeout(config.connect_timeout)
        .read_timeout(config.read_timeout)
        .redirect(Policy::none());
    if let Some(url) = proxy_url {
        let proxy = Proxy::all(url.as_str())
            .map_err(|_| ProxyPoolError::new("failed to configure an outbound proxy"))?;
        builder = builder.proxy(proxy);
    }
    builder
        .build()
        .map_err(|_| ProxyPoolError::new("failed to build an outbound client"))
}

async fn warm_candidate(config: &Config, candidate: Candidate) -> Option<WarmedCandidate> {
    let proxy = Proxy::all(candidate.url.as_str()).ok()?;
    let probe = Client::builder()
        .no_proxy()
        .proxy(proxy)
        .connect_timeout(config.proxy_warm_timeout)
        .timeout(config.proxy_warm_timeout)
        .redirect(Policy::none())
        .build()
        .ok()?;
    let started = Instant::now();
    probe
        .head(config.target_backend.clone())
        .send()
        .await
        .ok()?;
    let latency = started.elapsed();
    let client = production_client(config, Some(&candidate.url)).ok()?;
    Some(WarmedCandidate {
        entry: Arc::new(ProxyEntry {
            client,
            proxy_url: Some(candidate.url),
            public: candidate.public,
            consecutive_failures: AtomicU32::new(0),
        }),
        latency,
    })
}

fn select_best(mut warmed: Vec<WarmedCandidate>, maximum: usize) -> Vec<Arc<ProxyEntry>> {
    warmed.sort_by_key(|candidate| candidate.latency);
    let mut trusted = Vec::new();
    let mut public = Vec::new();
    for candidate in warmed {
        if candidate.entry.public {
            public.push(candidate.entry);
        } else {
            trusted.push(candidate.entry);
        }
    }

    let mut selected = trusted.into_iter().take(maximum).collect::<Vec<_>>();
    let mut prefixes = HashMap::<String, usize>::new();
    for entry in public {
        if selected.len() >= maximum {
            break;
        }
        let Some(url) = entry.proxy_url.as_ref() else {
            continue;
        };
        let Some(prefix) = public_network_prefix(url) else {
            continue;
        };
        let count = prefixes.entry(prefix).or_default();
        if *count >= 2 {
            continue;
        }
        *count += 1;
        selected.push(entry);
    }
    selected
}

fn deduplicate_candidates(candidates: Vec<Candidate>, public_cap: usize) -> Vec<Candidate> {
    let (mut trusted, mut public): (Vec<_>, Vec<_>) = candidates
        .into_iter()
        .partition(|candidate| !candidate.public);
    let mut seen = HashSet::new();
    trusted.retain(|candidate| seen.insert(candidate.url.as_str().to_owned()));
    public.retain(|candidate| seen.insert(candidate.url.as_str().to_owned()));
    public.shuffle(&mut rand::thread_rng());
    public.truncate(public_cap);
    trusted.extend(public);
    trusted
}

fn load_source_catalog(
    path: &Path,
    maximum: usize,
) -> Result<Vec<SourceDefinition>, ProxyPoolError> {
    let contents = read_bounded(path, SOURCE_CATALOG_MAX_BYTES)?;
    let catalog = match path
        .extension()
        .and_then(|extension| extension.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "json" => serde_json::from_str::<SourceCatalog>(&contents)
            .map_err(|error| ProxyPoolError::new(format!("invalid proxy source JSON: {error}")))?,
        "toml" => toml::from_str::<SourceCatalog>(&contents)
            .map_err(|error| ProxyPoolError::new(format!("invalid proxy source TOML: {error}")))?,
        _ => {
            return Err(ProxyPoolError::new(
                "PROXY_SOURCES_PATH must end in .json or .toml",
            ));
        }
    };

    Ok(catalog
        .sources
        .into_iter()
        .filter(|source| source.enabled && source.validated)
        .filter(|source| matches!(source.kind.as_str(), "github_raw" | "generic_text"))
        .take(maximum)
        .collect())
}

async fn fetch_source(
    config: &Config,
    source: SourceDefinition,
) -> Result<Vec<Candidate>, ProxyPoolError> {
    let source_url =
        Url::parse(&source.url).map_err(|_| ProxyPoolError::new("proxy source URL is invalid"))?;
    if source_url.scheme() != "https"
        || source_url.host_str().is_none()
        || !source_url.username().is_empty()
        || source_url.password().is_some()
        || source_url.fragment().is_some()
    {
        return Err(ProxyPoolError::new(
            "proxy source URL must be credential-free HTTPS",
        ));
    }

    let host = source_url
        .host_str()
        .ok_or_else(|| ProxyPoolError::new("proxy source URL has no host"))?;
    let port = source_url
        .port_or_known_default()
        .ok_or_else(|| ProxyPoolError::new("proxy source URL has no port"))?;
    let addresses = lookup_host((host, port))
        .await
        .map_err(|_| ProxyPoolError::new("proxy source DNS lookup failed"))?
        .collect::<Vec<_>>();
    if addresses.is_empty() || addresses.iter().any(|address| !is_global_ip(address.ip())) {
        return Err(ProxyPoolError::new(
            "proxy source resolved to a non-public address",
        ));
    }

    let client = Client::builder()
        .no_proxy()
        .resolve(host, addresses[0])
        .connect_timeout(config.proxy_source_timeout)
        .timeout(config.proxy_source_timeout)
        .redirect(Policy::none())
        .build()
        .map_err(|_| ProxyPoolError::new("failed to build proxy source client"))?;
    let mut response = client
        .get(source_url)
        .send()
        .await
        .map_err(|_| ProxyPoolError::new("proxy source request failed"))?
        .error_for_status()
        .map_err(|_| ProxyPoolError::new("proxy source returned an error status"))?;

    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| ProxyPoolError::new("failed to read proxy source response"))?
    {
        if bytes.len().saturating_add(chunk.len()) > config.proxy_source_max_bytes {
            return Err(ProxyPoolError::new(
                "proxy source response exceeded PROXY_SOURCE_MAX_BYTES",
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    let contents = String::from_utf8(bytes)
        .map_err(|_| ProxyPoolError::new("proxy source response was not UTF-8"))?;
    let protocol = source_protocol(&source);
    Ok(parse_proxy_lines(
        &contents,
        protocol,
        true,
        &config.public_proxy_allowed_ports,
        config.proxy_candidates_per_source,
    ))
}

fn source_protocol(source: &SourceDefinition) -> Option<BareProxyProtocol> {
    if let Some(protocol) = source.protocol.as_deref() {
        return match protocol.to_ascii_lowercase().as_str() {
            "http" | "https" => Some(BareProxyProtocol::Http),
            "socks4" | "socks4a" => Some(BareProxyProtocol::Socks4a),
            "socks5" | "socks5h" => Some(BareProxyProtocol::Socks5h),
            _ => None,
        };
    }

    let hint = Url::parse(&source.url)
        .map(|url| {
            format!(
                "{} {} {}",
                url.path(),
                url.query().unwrap_or_default(),
                source.name
            )
        })
        .unwrap_or_else(|_| source.name.clone())
        .to_ascii_lowercase();
    if hint.contains("socks5") {
        Some(BareProxyProtocol::Socks5h)
    } else if hint.contains("socks4") {
        Some(BareProxyProtocol::Socks4a)
    } else if hint.contains("http") {
        Some(BareProxyProtocol::Http)
    } else {
        None
    }
}

fn parse_proxy_lines(
    contents: &str,
    bare_protocol: Option<BareProxyProtocol>,
    public: bool,
    allowed_ports: &HashSet<u16>,
    maximum: usize,
) -> Vec<Candidate> {
    let mut candidates = contents
        .lines()
        .filter_map(|line| {
            let line = line.trim().trim_start_matches('\u{feff}');
            if line.is_empty() || line.starts_with('#') || line.len() > MAX_PROXY_LINE_BYTES {
                return None;
            }
            let parsed = Url::parse(line).ok().or_else(|| {
                bare_protocol.and_then(|protocol| {
                    Url::parse(&format!("{}://{line}", protocol.scheme())).ok()
                })
            })?;
            if public {
                validate_public_proxy_url(&parsed, allowed_ports).then_some(Candidate {
                    url: parsed,
                    public: true,
                })
            } else {
                Some(Candidate {
                    url: parsed,
                    public: false,
                })
            }
        })
        .collect::<Vec<_>>();
    candidates.shuffle(&mut rand::thread_rng());
    candidates.truncate(maximum);
    candidates
}

fn validate_public_proxy_url(url: &Url, allowed_ports: &HashSet<u16>) -> bool {
    if !matches!(
        url.scheme(),
        "http" | "https" | "socks4" | "socks4a" | "socks5" | "socks5h"
    ) || !url.username().is_empty()
        || url.password().is_some()
        || !matches!(url.path(), "" | "/")
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return false;
    }
    let Some(port) = url.port() else {
        return false;
    };
    if !allowed_ports.contains(&port) {
        return false;
    }
    url.host_str()
        .and_then(parse_ip_literal)
        .is_some_and(is_global_ip)
}

fn parse_ip_literal(host: &str) -> Option<IpAddr> {
    host.trim_start_matches('[')
        .trim_end_matches(']')
        .parse()
        .ok()
}

fn is_global_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => is_global_ipv4(ip),
        IpAddr::V6(ip) => {
            if let Some(mapped) = ip.to_ipv4_mapped() {
                return is_global_ipv4(mapped);
            }
            is_global_ipv6(ip)
        }
    }
}

fn is_global_ipv4(ip: Ipv4Addr) -> bool {
    let [a, b, c, _] = ip.octets();
    !(a == 0
        || a == 10
        || a == 127
        || a >= 224
        || (a == 100 && (64..=127).contains(&b))
        || (a == 169 && b == 254)
        || (a == 172 && (16..=31).contains(&b))
        || (a == 192 && b == 0 && c == 0)
        || (a == 192 && b == 0 && c == 2)
        || (a == 192 && b == 168)
        || (a == 198 && (b == 18 || b == 19))
        || (a == 198 && b == 51 && c == 100)
        || (a == 203 && b == 0 && c == 113))
}

fn is_global_ipv6(ip: Ipv6Addr) -> bool {
    let segments = ip.segments();
    !(ip.is_unspecified()
        || ip.is_loopback()
        || ip.is_multicast()
        || (segments[0] & 0xfe00) == 0xfc00
        || (segments[0] & 0xffc0) == 0xfe80
        || (segments[0] == 0x2001 && segments[1] == 0x0db8))
}

fn public_network_prefix(url: &Url) -> Option<String> {
    match url.host_str().and_then(parse_ip_literal)? {
        IpAddr::V4(ip) => {
            let octets = ip.octets();
            Some(format!("v4:{}:{}.{}", octets[0], octets[1], octets[2]))
        }
        IpAddr::V6(ip) => {
            let segments = ip.segments();
            Some(format!(
                "v6:{:x}:{:x}:{:x}:{:x}",
                segments[0], segments[1], segments[2], segments[3]
            ))
        }
    }
}

fn read_bounded(path: &Path, maximum: u64) -> Result<String, ProxyPoolError> {
    let metadata = fs::metadata(path).map_err(|error| {
        ProxyPoolError::new(format!("failed to inspect {}: {error}", path.display()))
    })?;
    if metadata.len() > maximum {
        return Err(ProxyPoolError::new(format!(
            "{} exceeds its configured size limit",
            path.display()
        )));
    }
    fs::read_to_string(path)
        .map_err(|error| ProxyPoolError::new(format!("failed to read {}: {error}", path.display())))
}

fn read_cache_with_backup(path: &Path, maximum: u64) -> Result<String, ProxyPoolError> {
    read_bounded(path, maximum).or_else(|_| read_bounded(&backup_path(path), maximum))
}

fn write_cache(path: &Path, urls: &[Url]) -> Result<(), io::Error> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let temporary = temporary_path(path);
    let backup = backup_path(path);
    let contents = urls.iter().map(Url::as_str).collect::<Vec<_>>().join("\n");
    fs::write(&temporary, format!("{contents}\n"))?;

    if backup.exists() {
        fs::remove_file(&backup)?;
    }
    if path.exists() {
        fs::rename(path, &backup)?;
    }
    if let Err(error) = fs::rename(&temporary, path) {
        if backup.exists() {
            let _ = fs::rename(&backup, path);
        }
        return Err(error);
    }
    if backup.exists() {
        fs::remove_file(backup)?;
    }
    Ok(())
}

fn temporary_path(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_owned();
    value.push(format!(".{}.tmp", std::process::id()));
    PathBuf::from(value)
}

fn backup_path(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_owned();
    value.push(".bak");
    PathBuf::from(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn allowed_ports() -> HashSet<u16> {
        [80, 443, 1080, 8080].into_iter().collect()
    }

    #[test]
    fn parses_bare_and_url_proxy_lines_strictly() {
        let parsed = parse_proxy_lines(
            "\u{feff}1.1.1.1:8080\r\nsocks5h://8.8.8.8:1080\n# comment\n127.0.0.1:8080\n",
            Some(BareProxyProtocol::Http),
            true,
            &allowed_ports(),
            10,
        );

        assert_eq!(parsed.len(), 2);
        assert!(parsed.iter().all(|candidate| candidate.public));
        assert!(
            parsed
                .iter()
                .any(|candidate| candidate.url.as_str() == "http://1.1.1.1:8080/")
        );
        assert!(parsed.iter().any(|candidate| {
            candidate.url.scheme() == "socks5h"
                && candidate.url.host_str() == Some("8.8.8.8")
                && candidate.url.port() == Some(1080)
        }));
    }

    #[test]
    fn rejects_private_reserved_credentials_and_disallowed_ports() {
        for value in [
            "http://127.0.0.1:8080",
            "http://10.0.0.1:8080",
            "http://100.64.0.1:8080",
            "http://192.0.2.1:8080",
            "http://[::1]:8080",
            "http://[::ffff:10.0.0.1]:8080",
            "http://user:secret@1.1.1.1:8080",
            "http://1.1.1.1:22",
            "http://1.1.1.1:8080/path",
        ] {
            let url = Url::parse(value).expect("test URL should parse");
            assert!(
                !validate_public_proxy_url(&url, &allowed_ports()),
                "{value}"
            );
        }
    }

    #[test]
    fn source_protocol_inference_is_conservative() {
        let source = |url: &str, protocol: Option<&str>| SourceDefinition {
            url: url.to_owned(),
            kind: "github_raw".to_owned(),
            enabled: true,
            validated: true,
            protocol: protocol.map(str::to_owned),
            name: String::new(),
        };

        assert!(matches!(
            source_protocol(&source("https://example.test/socks5.txt", None)),
            Some(BareProxyProtocol::Socks5h)
        ));
        assert!(matches!(
            source_protocol(&source("https://example.test/list.txt", Some("socks4"))),
            Some(BareProxyProtocol::Socks4a)
        ));
        assert!(source_protocol(&source("https://example.test/list.txt", None)).is_none());
    }

    #[test]
    fn bundled_source_catalog_is_valid_and_explicit() {
        let catalog = serde_json::from_str::<SourceCatalog>(include_str!("../proxy-sources.json"))
            .expect("bundled source catalog should parse");
        let enabled = catalog
            .sources
            .iter()
            .filter(|source| source.enabled && source.validated)
            .collect::<Vec<_>>();

        assert!(!enabled.is_empty());
        assert!(enabled.iter().all(|source| source.protocol.is_some()));
        assert!(
            enabled
                .iter()
                .all(|source| source.url.starts_with("https://"))
        );
    }

    #[test]
    fn trusted_candidates_win_deduplication_and_public_cap() {
        let trusted_url = Url::parse("http://proxy.example:8080").expect("URL should parse");
        let public_url = Url::parse("http://1.1.1.1:8080").expect("URL should parse");
        let candidates = deduplicate_candidates(
            vec![
                Candidate {
                    url: trusted_url.clone(),
                    public: false,
                },
                Candidate {
                    url: trusted_url,
                    public: true,
                },
                Candidate {
                    url: public_url,
                    public: true,
                },
            ],
            1,
        );

        assert_eq!(candidates.len(), 2);
        assert!(!candidates[0].public);
    }
}
