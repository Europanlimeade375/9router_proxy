use reqwest::Url;
use std::{
    collections::HashSet, env, error::Error, fmt, net::SocketAddr, path::PathBuf, time::Duration,
};

const DEFAULT_LISTEN_ADDR: &str = "127.0.0.1:8080";
const DEFAULT_MODEL_CONFIG_PATH: &str = "config.toml";
const DEFAULT_PROXY_CACHE_PATH: &str = "proxy-cache.txt";
const DEFAULT_MAX_JSON_BODY_BYTES: usize = 2 * 1024 * 1024;
const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 10;
const DEFAULT_READ_TIMEOUT_SECS: u64 = 120;
const DEFAULT_SHUTDOWN_GRACE_SECS: u64 = 30;
const DEFAULT_PROXY_REFRESH_SECS: u64 = 900;
const DEFAULT_PROXY_WARM_TIMEOUT_SECS: u64 = 8;
const DEFAULT_PROXY_WARM_CONCURRENCY: usize = 50;
const DEFAULT_MAX_PROXY_WARM_CANDIDATES: usize = 300;
const DEFAULT_PROXY_CANDIDATES_PER_SOURCE: usize = 100;
const DEFAULT_MAX_WARMED_PROXIES: usize = 20;
const DEFAULT_PROXY_SOURCE_TIMEOUT_SECS: u64 = 20;
const DEFAULT_PROXY_SOURCE_MAX_BYTES: usize = 5 * 1024 * 1024;
const DEFAULT_PROXY_LOCAL_FILE_MAX_BYTES: usize = 5 * 1024 * 1024;
const DEFAULT_MAX_PROXY_SOURCES: usize = 16;
const DEFAULT_FORWARD_CONNECT_TIMEOUT_SECS: u64 = 15;
const DEFAULT_FORWARD_PROXY_ALLOWED_PORTS: &str = "443";
const DEFAULT_PUBLIC_PROXY_ALLOWED_PORTS: &str =
    "80,81,443,1080,3128,4145,8000,8080,8081,8888,9000,9999,10000";

#[derive(Clone, Debug)]
pub(crate) struct Config {
    pub(crate) listen_addr: SocketAddr,
    pub(crate) target_backend: Url,
    pub(crate) proxy_urls: Vec<Url>,
    pub(crate) outbound_proxies_file: Option<PathBuf>,
    pub(crate) proxy_sources_path: Option<PathBuf>,
    pub(crate) proxy_cache_path: PathBuf,
    pub(crate) allow_direct: bool,
    pub(crate) model_config_path: PathBuf,
    pub(crate) max_json_body_bytes: usize,
    pub(crate) connect_timeout: Duration,
    pub(crate) read_timeout: Duration,
    pub(crate) shutdown_grace: Duration,
    pub(crate) proxy_refresh_interval: Duration,
    pub(crate) proxy_pool_stale_after: Duration,
    pub(crate) proxy_warm_timeout: Duration,
    pub(crate) proxy_warm_concurrency: usize,
    pub(crate) max_proxy_warm_candidates: usize,
    pub(crate) proxy_candidates_per_source: usize,
    pub(crate) max_warmed_proxies: usize,
    pub(crate) proxy_source_timeout: Duration,
    pub(crate) proxy_source_max_bytes: usize,
    pub(crate) proxy_local_file_max_bytes: usize,
    pub(crate) max_proxy_sources: usize,
    pub(crate) public_proxy_allowed_ports: HashSet<u16>,
    pub(crate) forward_proxy_enabled: bool,
    pub(crate) forward_proxy_allow_any_host: bool,
    pub(crate) forward_proxy_allowed_hosts: HashSet<String>,
    pub(crate) forward_proxy_allowed_ports: HashSet<u16>,
    pub(crate) forward_connect_timeout: Duration,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ConfigError(String);

impl ConfigError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for ConfigError {}

impl Config {
    pub(crate) fn from_env() -> Result<Self, ConfigError> {
        Self::from_lookup(|key| env::var(key).ok())
    }

    fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self, ConfigError> {
        let allow_direct = parse_bool("ALLOW_DIRECT", lookup("ALLOW_DIRECT"), false)?;
        let allow_insecure_backend = parse_bool(
            "ALLOW_INSECURE_BACKEND",
            lookup("ALLOW_INSECURE_BACKEND"),
            false,
        )?;

        let target_backend_raw = required("TARGET_BACKEND_URL", lookup("TARGET_BACKEND_URL"))?;
        let target_backend = parse_backend_url(&target_backend_raw, allow_insecure_backend)?;
        let listen_addr: SocketAddr = lookup("LISTEN_ADDR")
            .unwrap_or_else(|| DEFAULT_LISTEN_ADDR.to_owned())
            .parse()
            .map_err(|_| ConfigError::new("LISTEN_ADDR must be a valid IP socket address"))?;

        let forward_proxy_enabled = parse_bool(
            "FORWARD_PROXY_ENABLED",
            lookup("FORWARD_PROXY_ENABLED"),
            true,
        )?;
        let (forward_proxy_allow_any_host, forward_proxy_allowed_hosts) = parse_forward_hosts(
            lookup("FORWARD_PROXY_ALLOWED_HOSTS"),
            target_backend
                .host_str()
                .ok_or_else(|| ConfigError::new("TARGET_BACKEND_URL has no host"))?,
            listen_addr.ip().is_loopback(),
        )?;

        let proxy_urls = parse_proxy_urls(lookup("OUTBOUND_PROXIES"))?;
        let outbound_proxies_file = optional_path(lookup("OUTBOUND_PROXIES_FILE"));
        let proxy_sources_path = optional_path(lookup("PROXY_SOURCES_PATH"));
        let has_public_candidates = outbound_proxies_file.is_some() || proxy_sources_path.is_some();
        let has_proxy_candidates = !proxy_urls.is_empty() || has_public_candidates;
        match (has_proxy_candidates, allow_direct) {
            (false, false) => {
                return Err(ConfigError::new(
                    "set OUTBOUND_PROXIES, OUTBOUND_PROXIES_FILE, or PROXY_SOURCES_PATH; otherwise explicitly set ALLOW_DIRECT=true",
                ));
            }
            (true, true) => {
                return Err(ConfigError::new(
                    "ALLOW_DIRECT=true cannot be combined with outbound proxy configuration",
                ));
            }
            _ => {}
        }
        if has_public_candidates && target_backend.scheme() != "https" {
            return Err(ConfigError::new(
                "public proxy files and sources require an HTTPS TARGET_BACKEND_URL",
            ));
        }

        let proxy_refresh_secs = parse_positive(
            "PROXY_REFRESH_SECS",
            lookup("PROXY_REFRESH_SECS"),
            DEFAULT_PROXY_REFRESH_SECS,
        )?;

        Ok(Self {
            listen_addr,
            target_backend,
            proxy_urls,
            outbound_proxies_file,
            proxy_sources_path,
            proxy_cache_path: PathBuf::from(
                lookup("PROXY_CACHE_PATH")
                    .filter(|value| !value.trim().is_empty())
                    .unwrap_or_else(|| DEFAULT_PROXY_CACHE_PATH.to_owned()),
            ),
            allow_direct,
            model_config_path: PathBuf::from(
                lookup("MODEL_CONFIG_PATH")
                    .filter(|value| !value.trim().is_empty())
                    .unwrap_or_else(|| DEFAULT_MODEL_CONFIG_PATH.to_owned()),
            ),
            max_json_body_bytes: parse_positive(
                "MAX_JSON_BODY_BYTES",
                lookup("MAX_JSON_BODY_BYTES"),
                DEFAULT_MAX_JSON_BODY_BYTES,
            )?,
            connect_timeout: Duration::from_secs(parse_positive(
                "OUTBOUND_CONNECT_TIMEOUT_SECS",
                lookup("OUTBOUND_CONNECT_TIMEOUT_SECS"),
                DEFAULT_CONNECT_TIMEOUT_SECS,
            )?),
            read_timeout: Duration::from_secs(parse_positive(
                "OUTBOUND_READ_TIMEOUT_SECS",
                lookup("OUTBOUND_READ_TIMEOUT_SECS"),
                DEFAULT_READ_TIMEOUT_SECS,
            )?),
            shutdown_grace: Duration::from_secs(parse_positive(
                "SHUTDOWN_GRACE_SECS",
                lookup("SHUTDOWN_GRACE_SECS"),
                DEFAULT_SHUTDOWN_GRACE_SECS,
            )?),
            proxy_refresh_interval: Duration::from_secs(proxy_refresh_secs),
            proxy_pool_stale_after: Duration::from_secs(proxy_refresh_secs.saturating_mul(3)),
            proxy_warm_timeout: Duration::from_secs(parse_positive(
                "PROXY_WARM_TIMEOUT_SECS",
                lookup("PROXY_WARM_TIMEOUT_SECS"),
                DEFAULT_PROXY_WARM_TIMEOUT_SECS,
            )?),
            proxy_warm_concurrency: parse_positive(
                "PROXY_WARM_CONCURRENCY",
                lookup("PROXY_WARM_CONCURRENCY"),
                DEFAULT_PROXY_WARM_CONCURRENCY,
            )?,
            max_proxy_warm_candidates: parse_positive(
                "MAX_PROXY_WARM_CANDIDATES",
                lookup("MAX_PROXY_WARM_CANDIDATES"),
                DEFAULT_MAX_PROXY_WARM_CANDIDATES,
            )?,
            proxy_candidates_per_source: parse_positive(
                "PROXY_CANDIDATES_PER_SOURCE",
                lookup("PROXY_CANDIDATES_PER_SOURCE"),
                DEFAULT_PROXY_CANDIDATES_PER_SOURCE,
            )?,
            max_warmed_proxies: parse_positive(
                "MAX_WARMED_PROXIES",
                lookup("MAX_WARMED_PROXIES"),
                DEFAULT_MAX_WARMED_PROXIES,
            )?,
            proxy_source_timeout: Duration::from_secs(parse_positive(
                "PROXY_SOURCE_TIMEOUT_SECS",
                lookup("PROXY_SOURCE_TIMEOUT_SECS"),
                DEFAULT_PROXY_SOURCE_TIMEOUT_SECS,
            )?),
            proxy_source_max_bytes: parse_positive(
                "PROXY_SOURCE_MAX_BYTES",
                lookup("PROXY_SOURCE_MAX_BYTES"),
                DEFAULT_PROXY_SOURCE_MAX_BYTES,
            )?,
            proxy_local_file_max_bytes: parse_positive(
                "PROXY_LOCAL_FILE_MAX_BYTES",
                lookup("PROXY_LOCAL_FILE_MAX_BYTES"),
                DEFAULT_PROXY_LOCAL_FILE_MAX_BYTES,
            )?,
            max_proxy_sources: parse_positive(
                "MAX_PROXY_SOURCES",
                lookup("MAX_PROXY_SOURCES"),
                DEFAULT_MAX_PROXY_SOURCES,
            )?,
            public_proxy_allowed_ports: parse_ports(
                "PUBLIC_PROXY_ALLOWED_PORTS",
                lookup("PUBLIC_PROXY_ALLOWED_PORTS"),
                DEFAULT_PUBLIC_PROXY_ALLOWED_PORTS,
            )?,
            forward_proxy_enabled,
            forward_proxy_allow_any_host,
            forward_proxy_allowed_hosts,
            forward_proxy_allowed_ports: parse_ports(
                "FORWARD_PROXY_ALLOWED_PORTS",
                lookup("FORWARD_PROXY_ALLOWED_PORTS"),
                DEFAULT_FORWARD_PROXY_ALLOWED_PORTS,
            )?,
            forward_connect_timeout: Duration::from_secs(parse_positive(
                "FORWARD_CONNECT_TIMEOUT_SECS",
                lookup("FORWARD_CONNECT_TIMEOUT_SECS"),
                DEFAULT_FORWARD_CONNECT_TIMEOUT_SECS,
            )?),
        })
    }

    #[cfg(test)]
    pub(crate) fn test_direct() -> Self {
        Self::from_lookup(|key| match key {
            "TARGET_BACKEND_URL" => Some("https://api.example.com".to_owned()),
            "ALLOW_DIRECT" => Some("true".to_owned()),
            _ => None,
        })
        .expect("test direct configuration should be valid")
    }
}

fn required(key: &str, value: Option<String>) -> Result<String, ConfigError> {
    value
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| ConfigError::new(format!("{key} is required")))
}

fn optional_path(value: Option<String>) -> Option<PathBuf> {
    value
        .filter(|value| !value.trim().is_empty())
        .map(PathBuf::from)
}

fn parse_bool(key: &str, value: Option<String>, default: bool) -> Result<bool, ConfigError> {
    match value.as_deref() {
        None => Ok(default),
        Some("true") => Ok(true),
        Some("false") => Ok(false),
        Some(_) => Err(ConfigError::new(format!(
            "{key} must be either true or false"
        ))),
    }
}

fn parse_positive<T>(key: &str, value: Option<String>, default: T) -> Result<T, ConfigError>
where
    T: Copy + PartialEq + From<u8> + std::str::FromStr,
{
    let Some(value) = value else {
        return Ok(default);
    };
    let parsed = value
        .parse::<T>()
        .map_err(|_| ConfigError::new(format!("{key} must be a positive integer")))?;
    if parsed == T::from(0) {
        return Err(ConfigError::new(format!(
            "{key} must be a positive integer"
        )));
    }
    Ok(parsed)
}

fn parse_forward_hosts(
    value: Option<String>,
    target_host: &str,
    listen_is_loopback: bool,
) -> Result<(bool, HashSet<String>), ConfigError> {
    let Some(value) = value else {
        if listen_is_loopback {
            return Ok((true, HashSet::new()));
        }
        return Ok((false, [normalize_host(target_host)].into_iter().collect()));
    };
    let value = value.trim();
    if value == "*" {
        return Ok((true, HashSet::new()));
    }
    let hosts = value
        .split(',')
        .map(normalize_host)
        .filter(|host| !host.is_empty())
        .collect::<HashSet<_>>();
    if hosts.is_empty()
        || hosts
            .iter()
            .any(|host| host.contains('/') || host.contains('@'))
    {
        return Err(ConfigError::new(
            "FORWARD_PROXY_ALLOWED_HOSTS must be * or comma-separated hostnames",
        ));
    }
    Ok((false, hosts))
}

fn normalize_host(host: &str) -> String {
    host.trim()
        .trim_start_matches('[')
        .trim_end_matches(']')
        .trim_end_matches('.')
        .to_ascii_lowercase()
}

fn parse_ports(
    key: &str,
    value: Option<String>,
    default: &str,
) -> Result<HashSet<u16>, ConfigError> {
    let value = value.unwrap_or_else(|| default.to_owned());
    let ports = value
        .split(',')
        .map(str::trim)
        .map(|port| {
            port.parse::<u16>()
                .map_err(|_| ConfigError::new(format!("{key} must be comma-separated ports")))
        })
        .collect::<Result<HashSet<_>, _>>()?;
    if ports.is_empty() || ports.contains(&0) {
        return Err(ConfigError::new(format!(
            "{key} must contain non-zero ports"
        )));
    }
    Ok(ports)
}

fn parse_backend_url(raw: &str, allow_insecure: bool) -> Result<Url, ConfigError> {
    let url = Url::parse(raw).map_err(|_| ConfigError::new("TARGET_BACKEND_URL is invalid"))?;
    if url.host_str().is_none() || url.cannot_be_a_base() {
        return Err(ConfigError::new(
            "TARGET_BACKEND_URL must be an absolute URL with a host",
        ));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(ConfigError::new(
            "TARGET_BACKEND_URL must not contain credentials",
        ));
    }
    if url.query().is_some() || url.fragment().is_some() {
        return Err(ConfigError::new(
            "TARGET_BACKEND_URL must not contain a query or fragment",
        ));
    }
    match url.scheme() {
        "https" => Ok(url),
        "http" if allow_insecure => Ok(url),
        "http" => Err(ConfigError::new(
            "HTTP backends require ALLOW_INSECURE_BACKEND=true",
        )),
        _ => Err(ConfigError::new(
            "TARGET_BACKEND_URL must use HTTPS (or explicitly allowed HTTP)",
        )),
    }
}

fn parse_proxy_urls(raw: Option<String>) -> Result<Vec<Url>, ConfigError> {
    let Some(raw) = raw.filter(|value| !value.trim().is_empty()) else {
        return Ok(Vec::new());
    };

    raw.split(',')
        .enumerate()
        .map(|(index, value)| {
            let url = Url::parse(value.trim()).map_err(|_| {
                ConfigError::new(format!("OUTBOUND_PROXIES entry {} is invalid", index + 1))
            })?;
            if url.host_str().is_none()
                || url.port().is_none()
                || url.query().is_some()
                || url.fragment().is_some()
                || !matches!(
                    url.scheme(),
                    "http" | "https" | "socks4" | "socks4a" | "socks5" | "socks5h"
                )
            {
                return Err(ConfigError::new(format!(
                    "OUTBOUND_PROXIES entry {} has an unsupported form or no explicit port",
                    index + 1
                )));
            }
            if !matches!(url.path(), "" | "/") {
                return Err(ConfigError::new(format!(
                    "OUTBOUND_PROXIES entry {} must not contain a path",
                    index + 1
                )));
            }
            Ok(url)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn parse(values: &[(&str, &str)]) -> Result<Config, ConfigError> {
        let values = values.iter().copied().collect::<HashMap<_, _>>();
        Config::from_lookup(|key| values.get(key).map(|value| (*value).to_owned()))
    }

    #[test]
    fn requires_an_explicit_outbound_route() {
        let error = parse(&[("TARGET_BACKEND_URL", "https://api.example.com")])
            .expect_err("configuration should fail closed");

        assert!(error.to_string().contains("set OUTBOUND_PROXIES"));
    }

    #[test]
    fn accepts_an_explicit_direct_route() {
        let config = parse(&[
            ("TARGET_BACKEND_URL", "https://api.example.com/base"),
            ("ALLOW_DIRECT", "true"),
        ])
        .expect("configuration should be valid");

        assert!(config.allow_direct);
        assert!(config.proxy_urls.is_empty());
        assert_eq!(config.listen_addr.to_string(), DEFAULT_LISTEN_ADDR);
        assert_eq!(
            config.model_config_path,
            PathBuf::from(DEFAULT_MODEL_CONFIG_PATH)
        );
        assert!(config.forward_proxy_enabled);
        assert!(config.forward_proxy_allow_any_host);
        assert_eq!(
            config.forward_proxy_allowed_ports,
            [443].into_iter().collect()
        );
    }

    #[test]
    fn restricts_forward_proxy_hosts_when_listening_publicly() {
        let config = parse(&[
            ("TARGET_BACKEND_URL", "https://api.example.com"),
            ("ALLOW_DIRECT", "true"),
            ("LISTEN_ADDR", "0.0.0.0:8080"),
        ])
        .expect("configuration should be valid");

        assert!(!config.forward_proxy_allow_any_host);
        assert_eq!(
            config.forward_proxy_allowed_hosts,
            ["api.example.com".to_owned()].into_iter().collect()
        );
    }

    #[test]
    fn accepts_valid_proxy_schemes_without_direct_fallback() {
        let config = parse(&[
            ("TARGET_BACKEND_URL", "https://api.example.com"),
            (
                "OUTBOUND_PROXIES",
                "http://user:secret@proxy.example:8080,socks5h://proxy.example:1080",
            ),
        ])
        .expect("configuration should be valid");

        assert!(!config.allow_direct);
        assert_eq!(config.proxy_urls.len(), 2);
    }

    #[test]
    fn accepts_public_file_and_source_configuration() {
        let config = parse(&[
            ("TARGET_BACKEND_URL", "https://api.example.com"),
            ("OUTBOUND_PROXIES_FILE", "proxies.txt"),
            ("PROXY_SOURCES_PATH", "proxy-sources.json"),
            ("PUBLIC_PROXY_ALLOWED_PORTS", "443,8080"),
        ])
        .expect("public proxy configuration should be valid");

        assert_eq!(
            config.outbound_proxies_file,
            Some(PathBuf::from("proxies.txt"))
        );
        assert_eq!(
            config.proxy_sources_path,
            Some(PathBuf::from("proxy-sources.json"))
        );
        assert_eq!(
            config.public_proxy_allowed_ports,
            [443, 8080].into_iter().collect()
        );
    }

    #[test]
    fn rejects_public_sources_for_plaintext_backend() {
        let error = parse(&[
            ("TARGET_BACKEND_URL", "http://api.example.com"),
            ("ALLOW_INSECURE_BACKEND", "true"),
            ("PROXY_SOURCES_PATH", "proxy-sources.json"),
        ])
        .expect_err("public proxies must not carry plaintext backend traffic");

        assert_eq!(
            error.to_string(),
            "public proxy files and sources require an HTTPS TARGET_BACKEND_URL"
        );
    }

    #[test]
    fn rejects_ambiguous_direct_and_proxy_configuration() {
        let error = parse(&[
            ("TARGET_BACKEND_URL", "https://api.example.com"),
            ("OUTBOUND_PROXIES", "http://proxy.example:8080"),
            ("ALLOW_DIRECT", "true"),
        ])
        .expect_err("ambiguous routes should be rejected");

        assert_eq!(
            error.to_string(),
            "ALLOW_DIRECT=true cannot be combined with outbound proxy configuration"
        );
    }

    #[test]
    fn rejects_http_backend_without_explicit_opt_in() {
        let error = parse(&[
            ("TARGET_BACKEND_URL", "http://api.example.com"),
            ("ALLOW_DIRECT", "true"),
        ])
        .expect_err("plaintext backend should be rejected");

        assert_eq!(
            error.to_string(),
            "HTTP backends require ALLOW_INSECURE_BACKEND=true"
        );
    }

    #[test]
    fn permits_http_backend_only_with_explicit_direct_opt_in() {
        let config = parse(&[
            ("TARGET_BACKEND_URL", "http://127.0.0.1:9000"),
            ("ALLOW_INSECURE_BACKEND", "true"),
            ("ALLOW_DIRECT", "true"),
        ])
        .expect("explicit plaintext configuration should be accepted");

        assert_eq!(config.target_backend.scheme(), "http");
    }

    #[test]
    fn rejects_malformed_boolean_values() {
        let error = parse(&[
            ("TARGET_BACKEND_URL", "https://api.example.com"),
            ("ALLOW_DIRECT", "yes"),
        ])
        .expect_err("boolean parsing should be strict");

        assert_eq!(
            error.to_string(),
            "ALLOW_DIRECT must be either true or false"
        );
    }
}
