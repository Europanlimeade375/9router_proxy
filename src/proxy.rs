use crate::{config::Config, models::ModelRegistry, proxy_pool::ProxyPool};
use axum::{
    Json, Router,
    body::{Body, Bytes, to_bytes},
    extract::{Request, State},
    http::{
        HeaderMap, HeaderName, Method, StatusCode, Uri,
        header::{CONNECTION, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE, UPGRADE},
    },
    response::{IntoResponse, Response},
    routing::get,
};
use hyper_util::rt::TokioIo;
use reqwest::{Body as UpstreamBody, Url};
use serde_json::{Value, json};
use std::{collections::HashSet, convert::Infallible, sync::Arc};
use tokio::io::copy_bidirectional;
use tower::{Service, ServiceExt, make::Shared, service_fn};

pub(crate) struct AppState {
    proxy_pool: Arc<ProxyPool>,
    target_backend: Url,
    max_json_body_bytes: usize,
    models: ModelRegistry,
    forward_proxy_enabled: bool,
    forward_proxy_allow_any_host: bool,
    forward_proxy_allowed_hosts: HashSet<String>,
    forward_proxy_allowed_ports: HashSet<u16>,
}

pub(crate) fn build_state(
    config: &Config,
    models: ModelRegistry,
    proxy_pool: Arc<ProxyPool>,
) -> Arc<AppState> {
    Arc::new(AppState {
        proxy_pool,
        target_backend: config.target_backend.clone(),
        max_json_body_bytes: config.max_json_body_bytes,
        models,
        forward_proxy_enabled: config.forward_proxy_enabled,
        forward_proxy_allow_any_host: config.forward_proxy_allow_any_host,
        forward_proxy_allowed_hosts: config.forward_proxy_allowed_hosts.clone(),
        forward_proxy_allowed_ports: config.forward_proxy_allowed_ports.clone(),
    })
}

pub(crate) fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/health/live", get(liveness))
        .route("/health/ready", get(readiness))
        .fallback(forward)
        .with_state(state)
}

pub(crate) fn service(
    state: Arc<AppState>,
) -> Shared<impl Service<Request, Response = Response, Error = Infallible, Future: Send> + Clone> {
    let router = router(state.clone());
    Shared::new(service_fn(move |request: Request| {
        let router = router.clone();
        let state = state.clone();
        async move {
            let response = if request.method() == Method::CONNECT {
                forward_connect(&state, request).await
            } else {
                match router.oneshot(request).await {
                    Ok(response) => response,
                    Err(error) => match error {},
                }
            };
            Ok::<_, Infallible>(response)
        }
    }))
}

async fn liveness() -> Json<Value> {
    Json(json!({"status": "live"}))
}

async fn readiness(State(state): State<Arc<AppState>>) -> Response {
    let health = state.proxy_pool.health().await;
    let status = if health.ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(json!({
            "status": if health.ready { "ready" } else { "not_ready" },
            "available_proxies": health.available,
            "total_proxies": health.total,
            "connect_capable_proxies": health.tunnel_available,
            "pool_generation": health.generation,
            "pool_age_seconds": health.age_secs,
            "model_aliases": state.models.alias_count()
        })),
    )
        .into_response()
}

async fn forward(State(state): State<Arc<AppState>>, request: Request) -> Response {
    if request.method() == Method::CONNECT {
        return forward_connect(&state, request).await;
    }
    if request.method() == Method::TRACE {
        return error_response(
            StatusCode::METHOD_NOT_ALLOWED,
            "unsupported_method",
            "TRACE is not supported",
        );
    }
    if request.headers().contains_key(UPGRADE) {
        return error_response(
            StatusCode::NOT_IMPLEMENTED,
            "upgrade_not_supported",
            "protocol upgrades are not supported",
        );
    }

    let target_url = match build_target_url(&state.target_backend, request.uri()) {
        Ok(url) => url,
        Err(()) => {
            tracing::warn!(path = request.uri().path(), "rejected invalid target path");
            return error_response(
                StatusCode::BAD_REQUEST,
                "invalid_target_path",
                "request path cannot be mapped to the configured backend",
            );
        }
    };

    let path = request.uri().path().to_owned();
    let (mut parts, body) = request.into_parts();
    let rewrite_json = is_model_json_request(&path, &parts.headers);
    if rewrite_json && has_unsupported_content_encoding(&parts.headers) {
        return error_response(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "encoded_json_not_supported",
            "compressed model request bodies are not supported",
        );
    }

    sanitize_request_headers(&mut parts.headers);

    let upstream_body = if rewrite_json {
        let bytes = match to_bytes(body, state.max_json_body_bytes).await {
            Ok(bytes) => bytes,
            Err(error) => {
                tracing::warn!(%error, "rejected model JSON body that exceeded its limit");
                return error_response(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "body_too_large",
                    "model JSON body exceeds MAX_JSON_BODY_BYTES",
                );
            }
        };
        match rewrite_model_alias(&bytes, &state.models) {
            Ok(Some(rewritten)) => {
                parts.headers.remove(CONTENT_LENGTH);
                UpstreamBody::from(rewritten)
            }
            Ok(None) => UpstreamBody::from(bytes),
            Err(()) => {
                return error_response(
                    StatusCode::BAD_REQUEST,
                    "invalid_json",
                    "model request body must be valid JSON",
                );
            }
        }
    } else {
        UpstreamBody::wrap_stream(body.into_data_stream())
    };

    let Some(selected_proxy) = state.proxy_pool.select().await else {
        return error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "proxy_pool_unavailable",
            "no warmed proxy is currently available",
        );
    };
    tracing::debug!(method = %parts.method, path, "forwarding request");

    let result = selected_proxy
        .client()
        .request(parts.method, target_url)
        .headers(parts.headers)
        .body(upstream_body)
        .send()
        .await;

    let upstream = match result {
        Ok(response) => response,
        Err(error) => {
            let status = if error.is_timeout() {
                StatusCode::GATEWAY_TIMEOUT
            } else {
                StatusCode::BAD_GATEWAY
            };
            if error.is_connect() {
                selected_proxy.record_connect_failure();
            }
            tracing::warn!(
                timeout = error.is_timeout(),
                connect = error.is_connect(),
                "upstream request failed"
            );
            return error_response(
                status,
                "upstream_request_failed",
                "the upstream request failed",
            );
        }
    };

    let status = upstream.status();
    let mut headers = upstream.headers().clone();
    sanitize_hop_by_hop_headers(&mut headers);

    let mut response = Response::new(Body::from_stream(upstream.bytes_stream()));
    *response.status_mut() = status;
    *response.headers_mut() = headers;
    response
}

async fn forward_connect(state: &AppState, mut request: Request) -> Response {
    if !state.forward_proxy_enabled {
        return error_response(
            StatusCode::METHOD_NOT_ALLOWED,
            "forward_proxy_disabled",
            "forward proxy mode is disabled",
        );
    }
    let Some(authority) = request.uri().authority() else {
        return error_response(
            StatusCode::BAD_REQUEST,
            "invalid_connect_target",
            "CONNECT requires a host and explicit port",
        );
    };
    let Some(port) = authority.port_u16() else {
        return error_response(
            StatusCode::BAD_REQUEST,
            "invalid_connect_target",
            "CONNECT requires an explicit port",
        );
    };
    let host = normalize_connect_host(authority.host());
    if !state.forward_proxy_allowed_ports.contains(&port)
        || (!state.forward_proxy_allow_any_host
            && !state.forward_proxy_allowed_hosts.contains(&host))
    {
        tracing::warn!(host, port, "rejected CONNECT target outside allowlist");
        return error_response(
            StatusCode::FORBIDDEN,
            "connect_target_forbidden",
            "CONNECT target is not allowed",
        );
    }

    let target = authority.as_str().to_owned();
    tracing::info!(host, port, "opening forward-proxy CONNECT tunnel");
    let mut outbound = match state.proxy_pool.open_tunnel(&target).await {
        Ok(stream) => stream,
        Err(error) => {
            tracing::warn!(%error, host, port, "failed to open upstream CONNECT tunnel");
            return error_response(
                StatusCode::BAD_GATEWAY,
                "connect_tunnel_failed",
                "failed to open an upstream CONNECT tunnel",
            );
        }
    };

    let on_upgrade = hyper::upgrade::on(&mut request);
    tokio::spawn(async move {
        match on_upgrade.await {
            Ok(upgraded) => {
                let mut downstream = TokioIo::new(upgraded);
                match copy_bidirectional(&mut downstream, &mut outbound).await {
                    Ok((uploaded, downloaded)) => {
                        tracing::debug!(uploaded, downloaded, "CONNECT tunnel closed")
                    }
                    Err(error) => tracing::debug!(%error, "CONNECT tunnel I/O failed"),
                }
            }
            Err(error) => tracing::warn!(%error, "downstream CONNECT upgrade failed"),
        }
    });

    Response::new(Body::empty())
}

fn normalize_connect_host(host: &str) -> String {
    host.trim_start_matches('[')
        .trim_end_matches(']')
        .trim_end_matches('.')
        .to_ascii_lowercase()
}

fn error_response(status: StatusCode, code: &'static str, message: &'static str) -> Response {
    (
        status,
        Json(json!({"error": {"code": code, "message": message}})),
    )
        .into_response()
}

fn is_model_json_request(path: &str, headers: &HeaderMap) -> bool {
    let is_model_path = [
        "/chat/completions",
        "/completions",
        "/responses",
        "/messages",
    ]
    .iter()
    .any(|suffix| path == *suffix || path.ends_with(suffix));
    let is_json = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"));
    is_model_path && is_json
}

fn has_unsupported_content_encoding(headers: &HeaderMap) -> bool {
    headers.get_all(CONTENT_ENCODING).iter().any(|value| {
        value
            .to_str()
            .map_or(true, |value| !value.trim().eq_ignore_ascii_case("identity"))
    })
}

fn rewrite_model_alias(body: &[u8], models: &ModelRegistry) -> Result<Option<Bytes>, ()> {
    let mut value = serde_json::from_slice::<Value>(body).map_err(|_| ())?;
    if !models.apply(&mut value) {
        return Ok(None);
    }
    serde_json::to_vec(&value)
        .map(Bytes::from)
        .map(Some)
        .map_err(|_| ())
}

fn build_target_url(base: &Url, uri: &Uri) -> Result<Url, ()> {
    let suffix = uri
        .path_and_query()
        .map_or("/", axum::http::uri::PathAndQuery::as_str);
    let raw = format!("{}{}", base.as_str().trim_end_matches('/'), suffix);
    let target = Url::parse(&raw).map_err(|_| ())?;

    if target.scheme() != base.scheme()
        || target.host_str() != base.host_str()
        || target.port_or_known_default() != base.port_or_known_default()
    {
        return Err(());
    }

    let base_path = base.path().trim_end_matches('/');
    if !base_path.is_empty()
        && base_path != "/"
        && target.path() != base_path
        && !target
            .path()
            .strip_prefix(base_path)
            .is_some_and(|suffix| suffix.starts_with('/'))
    {
        return Err(());
    }

    Ok(target)
}

fn sanitize_request_headers(headers: &mut HeaderMap) {
    sanitize_hop_by_hop_headers(headers);
    for name in [
        "host",
        "forwarded",
        "x-forwarded-for",
        "x-forwarded-host",
        "x-forwarded-proto",
        "x-forwarded-port",
        "x-real-ip",
        "via",
    ] {
        headers.remove(name);
    }
}

fn sanitize_hop_by_hop_headers(headers: &mut HeaderMap) {
    let nominated = headers
        .get_all(CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .filter_map(|name| HeaderName::from_bytes(name.trim().as_bytes()).ok())
        .collect::<Vec<_>>();

    for name in nominated {
        headers.remove(name);
    }
    for name in [
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "proxy-connection",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    ] {
        headers.remove(name);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderValue, Request as HttpRequest};
    use reqwest::{Client, redirect::Policy};
    use serde_json::json;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        task::JoinHandle,
    };
    use tower::ServiceExt;

    fn test_models() -> ModelRegistry {
        ModelRegistry::parse_toml(include_str!("../config.toml"))
            .expect("bundled model config should parse")
    }

    #[test]
    fn rewrites_only_the_configured_model_alias() {
        let models = test_models();
        let original = br#"{"model":"thinkingmachines/inkling(high)","messages":[],"extra":1}"#;
        let rewritten = rewrite_model_alias(original, &models)
            .expect("JSON should parse")
            .expect("alias should be rewritten");
        let value: Value = serde_json::from_slice(&rewritten).expect("rewritten JSON should parse");

        assert_eq!(value["model"], "thinkingmachines/inkling");
        assert_eq!(value["reasoning_effort"], "high");
        assert_eq!(value["extra"], 1);

        assert!(
            rewrite_model_alias(br#"{"model":"another-model"}"#, &models)
                .expect("JSON should parse")
                .is_none()
        );
    }

    #[test]
    fn recognizes_supported_model_json_endpoints() {
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/json; charset=utf-8"),
        );

        for path in [
            "/v1/chat/completions",
            "/v1/completions",
            "/v1/responses",
            "/v1/messages",
        ] {
            assert!(is_model_json_request(path, &headers), "path: {path}");
        }
        assert!(!is_model_json_request("/v1/files", &headers));
    }

    #[test]
    fn preserves_base_path_encoded_path_and_query() {
        let base = Url::parse("https://api.example.com/prefix/").expect("base URL should parse");
        let uri = "/v1/files/a%2Fb?download=%2Fexact"
            .parse::<Uri>()
            .expect("URI should parse");

        let target = build_target_url(&base, &uri).expect("target URL should build");

        assert_eq!(
            target.as_str(),
            "https://api.example.com/prefix/v1/files/a%2Fb?download=%2Fexact"
        );
    }

    #[test]
    fn rejects_paths_that_escape_the_backend_prefix() {
        let base = Url::parse("https://api.example.com/prefix/").expect("base URL should parse");
        let uri = "/../admin".parse::<Uri>().expect("URI should parse");

        assert!(build_target_url(&base, &uri).is_err());
    }

    #[test]
    fn strips_fixed_dynamic_and_forwarding_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(
            CONNECTION,
            HeaderValue::from_static("x-private, keep-alive"),
        );
        headers.insert("x-private", HeaderValue::from_static("secret"));
        headers.insert("keep-alive", HeaderValue::from_static("timeout=5"));
        headers.insert("proxy-authorization", HeaderValue::from_static("secret"));
        headers.insert("forwarded", HeaderValue::from_static("for=192.0.2.1"));
        headers.insert("x-real-ip", HeaderValue::from_static("192.0.2.1"));
        headers.insert("authorization", HeaderValue::from_static("Bearer intended"));

        sanitize_request_headers(&mut headers);

        assert!(!headers.contains_key(CONNECTION));
        assert!(!headers.contains_key("x-private"));
        assert!(!headers.contains_key("keep-alive"));
        assert!(!headers.contains_key("proxy-authorization"));
        assert!(!headers.contains_key("forwarded"));
        assert!(!headers.contains_key("x-real-ip"));
        assert_eq!(headers["authorization"], "Bearer intended");
    }

    #[tokio::test]
    async fn forwards_http_semantics_and_streams_bodies() {
        let (backend, backend_task) = start_backend().await;
        let client = Client::builder()
            .no_proxy()
            .redirect(Policy::none())
            .build()
            .expect("test client should build");
        let state = Arc::new(AppState {
            proxy_pool: ProxyPool::direct_for_test(client),
            target_backend: backend,
            max_json_body_bytes: 1024,
            models: test_models(),
            forward_proxy_enabled: true,
            forward_proxy_allow_any_host: true,
            forward_proxy_allowed_hosts: HashSet::new(),
            forward_proxy_allowed_ports: [443].into_iter().collect(),
        });
        let app = router(state);

        let request = HttpRequest::builder()
            .method(Method::POST)
            .uri("/v1/chat/completions?raw=%2F")
            .header(CONTENT_TYPE, "application/json; charset=utf-8")
            .header(CONNECTION, "x-remove")
            .header("x-remove", "secret")
            .header("forwarded", "for=192.0.2.1")
            .header("authorization", "Bearer intended")
            .body(Body::from(
                br#"{"model":"thinkingmachines/inkling(high)","messages":[]}"#.as_slice(),
            ))
            .expect("request should build");

        let response = app
            .clone()
            .oneshot(request)
            .await
            .expect("proxy should respond");

        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(response.headers()["x-upstream"], "present");
        assert!(!response.headers().contains_key("x-hop"));
        let body = to_bytes(response.into_body(), 16 * 1024)
            .await
            .expect("response body should stream");
        let echoed: Value = serde_json::from_slice(&body).expect("response should be JSON");
        assert_eq!(echoed["method"], "POST");
        assert_eq!(echoed["uri"], "/base/v1/chat/completions?raw=%2F");
        assert_eq!(echoed["authorization"], true);
        assert_eq!(echoed["removed_connection_header"], true);
        assert_eq!(echoed["removed_forwarded_header"], true);
        assert_eq!(echoed["body"]["model"], "thinkingmachines/inkling");
        assert_eq!(echoed["body"]["reasoning_effort"], "high");
        assert_eq!(
            echoed["content_length"].as_u64(),
            echoed["received_body_bytes"].as_u64()
        );

        let large_body = vec![b'x'; 128 * 1024];
        let request = HttpRequest::builder()
            .method(Method::PUT)
            .uri("/upload")
            .body(Body::from(large_body.clone()))
            .expect("request should build");
        let response = app
            .oneshot(request)
            .await
            .expect("proxy should stream a non-JSON request");
        let body = to_bytes(response.into_body(), 16 * 1024)
            .await
            .expect("response body should stream");
        let echoed: Value = serde_json::from_slice(&body).expect("response should be JSON");
        assert_eq!(echoed["method"], "PUT");
        assert_eq!(echoed["uri"], "/base/upload");
        assert_eq!(echoed["received_body_bytes"], large_body.len());

        backend_task.abort();
    }

    #[tokio::test]
    async fn supports_forward_proxy_connect_tunnels() {
        let echo_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("echo listener should bind");
        let echo_address = echo_listener
            .local_addr()
            .expect("echo listener should have an address");
        let echo_task = tokio::spawn(async move {
            let (mut stream, _) = echo_listener.accept().await.expect("echo should accept");
            let mut request = [0_u8; 4];
            stream
                .read_exact(&mut request)
                .await
                .expect("echo should read");
            assert_eq!(&request, b"ping");
            stream.write_all(b"pong").await.expect("echo should write");
        });

        let client = Client::builder()
            .no_proxy()
            .redirect(Policy::none())
            .build()
            .expect("test client should build");
        let state = Arc::new(AppState {
            proxy_pool: ProxyPool::direct_for_test(client),
            target_backend: Url::parse("https://api.example.com").expect("target URL should parse"),
            max_json_body_bytes: 1024,
            models: test_models(),
            forward_proxy_enabled: true,
            forward_proxy_allow_any_host: true,
            forward_proxy_allowed_hosts: HashSet::new(),
            forward_proxy_allowed_ports: [echo_address.port()].into_iter().collect(),
        });
        let proxy_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("proxy listener should bind");
        let proxy_address = proxy_listener
            .local_addr()
            .expect("proxy listener should have an address");
        let proxy_task = tokio::spawn(async move {
            axum::serve(proxy_listener, service(state))
                .await
                .expect("proxy server should run");
        });

        let mut downstream = TcpStream::connect(proxy_address)
            .await
            .expect("client should connect to proxy");
        downstream
            .write_all(
                format!("CONNECT {echo_address} HTTP/1.1\r\nHost: {echo_address}\r\n\r\n")
                    .as_bytes(),
            )
            .await
            .expect("client should write CONNECT");
        let mut response = Vec::new();
        let mut byte = [0_u8; 1];
        while !response.ends_with(b"\r\n\r\n") {
            downstream
                .read_exact(&mut byte)
                .await
                .expect("client should read CONNECT response");
            response.push(byte[0]);
            assert!(response.len() < 16 * 1024);
        }
        let response = String::from_utf8(response).expect("response should be UTF-8");
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");

        downstream
            .write_all(b"ping")
            .await
            .expect("client should write tunnel bytes");
        let mut reply = [0_u8; 4];
        downstream
            .read_exact(&mut reply)
            .await
            .expect("client should read tunnel bytes");
        assert_eq!(&reply, b"pong");

        echo_task.await.expect("echo task should finish");
        proxy_task.abort();
    }

    async fn start_backend() -> (Url, JoinHandle<()>) {
        let app = Router::new().fallback(|request: Request| async move {
            let (parts, body) = request.into_parts();
            let bytes = to_bytes(body, 1024 * 1024)
                .await
                .expect("backend should receive request body");
            let parsed_body = serde_json::from_slice::<Value>(&bytes).unwrap_or(Value::Null);
            let content_length = parts
                .headers
                .get(CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<usize>().ok());
            let payload = json!({
                "method": parts.method.as_str(),
                "uri": parts.uri.to_string(),
                "authorization": parts.headers.contains_key("authorization"),
                "removed_connection_header": !parts.headers.contains_key("x-remove"),
                "removed_forwarded_header": !parts.headers.contains_key("forwarded"),
                "content_length": content_length,
                "received_body_bytes": bytes.len(),
                "body": parsed_body,
            });
            let mut response = (StatusCode::CREATED, Json(payload)).into_response();
            response
                .headers_mut()
                .insert(CONNECTION, HeaderValue::from_static("x-hop"));
            response
                .headers_mut()
                .insert("x-hop", HeaderValue::from_static("secret"));
            response
                .headers_mut()
                .insert("x-upstream", HeaderValue::from_static("present"));
            response
        });
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test listener should bind");
        let address = listener
            .local_addr()
            .expect("test listener should have an address");
        let task = tokio::spawn(async move {
            if let Err(error) = axum::serve(listener, app).await {
                panic!("test backend failed: {error}");
            }
        });
        let url =
            Url::parse(&format!("http://{address}/base")).expect("test backend URL should parse");
        (url, task)
    }
}
