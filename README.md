# 9router_proxy

A streaming reverse proxy and local HTTP forward-proxy relay for OpenAI-compatible APIs. Reverse mode can rewrite model request bodies; forward mode supports standard HTTPS `CONNECT` tunneling for clients such as 9router. Both modes route through the warmed outbound pool.

## Security model

- Public proxy sources are used only when explicitly configured. Only HTTPS sources marked both `enabled` and `validated` are fetched.
- TLS certificate and hostname verification remain enabled for source downloads, proxy transport, and the target backend.
- The backend must use HTTPS unless plaintext HTTP is explicitly enabled.
- Direct traffic is disabled unless `ALLOW_DIRECT=true` is explicitly set.
- Direct and configured-proxy modes are mutually exclusive, so a failed proxy can never silently fall back to the host connection.
- Direct, source-download, probe, and configured-proxy clients ignore system proxy environment variables. Configured proxy clients use only their assigned proxy.
- Remotely discovered proxies must be credential-free public IP literals on explicitly allowed ports; private, reserved, link-local, documentation, and multicast addresses are rejected.
- Source downloads are DNS-validated and pinned to a public resolved address, redirects are disabled, and response sizes are bounded.
- Redirect following is disabled to avoid forwarding credentials to an unexpected destination.
- Hop-by-hop, proxy-authentication, and client-address forwarding headers are removed.
- Standard HTTP `CONNECT` tunneling is supported with explicit host/port policy. `TRACE`, WebSockets, arbitrary upgrades, and HTTP trailers are not supported.
- Request and response bodies are streamed. JSON for recognized model endpoints is buffered only when profile rewriting can apply and is bounded by `MAX_JSON_BODY_BYTES`.
- Requests are not retried because streamed bodies are not safely replayable and retrying non-idempotent API operations can duplicate work.

Explicit `OUTBOUND_PROXIES` are administrator-trusted infrastructure. Remotely discovered public proxies are untrusted: HTTPS prevents them from reading or modifying authenticated request contents, but they still observe the target hostname, timing, volume, and connection metadata and can selectively block traffic. Public pools therefore require an HTTPS target. Never disable TLS verification, and verify that proxy use complies with the backend provider's terms.

## Configuration

Network/runtime configuration is read from environment variables at startup. Model quality profiles are loaded from a versioned TOML or JSON file. Invalid or ambiguous configuration fails closed.

| Variable | Required/default | Description |
| --- | --- | --- |
| `TARGET_BACKEND_URL` | Required | HTTPS backend base URL, for example `https://api.openai.com`. An optional path prefix is preserved. |
| `OUTBOUND_PROXIES` | One outbound mode | Comma-separated trusted `http`, `https`, `socks4`, `socks4a`, `socks5`, or `socks5h` proxy URLs with explicit ports. These entries are also warmed before use. |
| `OUTBOUND_PROXIES_FILE` | One outbound mode | Local UTF-8 text file containing one complete proxy URL per line. Entries are treated as untrusted public candidates. |
| `PROXY_SOURCES_PATH` | One outbound mode | JSON or TOML source catalog, such as `proxy-sources.json`. Enabled sources are refreshed periodically. |
| `ALLOW_DIRECT` | `false` | Must be `true` when no outbound proxies are configured. Cannot be combined with `OUTBOUND_PROXIES`. |
| `ALLOW_INSECURE_BACKEND` | `false` | Allows a plaintext HTTP backend. This can expose authorization headers and bodies; use only for trusted local development. |
| `LISTEN_ADDR` | `127.0.0.1:8080` | Local socket address. Bind publicly only behind appropriate authentication and network controls. |
| `MODEL_CONFIG_PATH` | `config.toml` | Model profile file. The extension must be `.toml` or `.json`; changes require a restart. |
| `MAX_JSON_BODY_BYTES` | `2097152` | Maximum buffered model JSON body size. Other request bodies remain streamed. |
| `OUTBOUND_CONNECT_TIMEOUT_SECS` | `10` | Maximum time to establish an outbound connection. |
| `OUTBOUND_READ_TIMEOUT_SECS` | `120` | Maximum idle time between upstream body reads. It does not impose a total duration on SSE streams. |
| `SHUTDOWN_GRACE_SECS` | `30` | Maximum drain time after a shutdown signal. |
| `FORWARD_PROXY_ENABLED` | `true` | Enables standard HTTP `CONNECT` forward-proxy mode. |
| `FORWARD_PROXY_ALLOWED_HOSTS` | Loopback: `*`; public bind: target host | `*` or comma-separated CONNECT destination hosts. Never use `*` on a publicly reachable listener. |
| `FORWARD_PROXY_ALLOWED_PORTS` | `443` | Comma-separated CONNECT destination ports. |
| `FORWARD_CONNECT_TIMEOUT_SECS` | `15` | Maximum time to establish a chained CONNECT tunnel. |
| `PROXY_CACHE_PATH` | `proxy-cache.txt` | Last-known-good public proxy URL cache. Cache entries are always rewarmed before use. |
| `PROXY_REFRESH_SECS` | `900` | Base refresh interval; a small random jitter is added across replicas. |
| `PROXY_WARM_TIMEOUT_SECS` | `8` | Total timeout for one anonymous `HEAD` probe to `TARGET_BACKEND_URL`. |
| `PROXY_WARM_CONCURRENCY` | `50` | Maximum concurrent warm-up probes. |
| `MAX_PROXY_WARM_CANDIDATES` | `300` | Maximum public candidates sampled during one refresh. Trusted explicit entries are not crowded out by this cap. |
| `PROXY_CANDIDATES_PER_SOURCE` | `100` | Random candidate sample retained from each source. |
| `MAX_WARMED_PROXIES` | `20` | Best successful proxies retained in memory, ranked by probe latency with public network-prefix diversity. |
| `PROXY_SOURCE_TIMEOUT_SECS` | `20` | Source download timeout. |
| `PROXY_SOURCE_MAX_BYTES` | `5242880` | Maximum streamed response size for each remote source. |
| `PROXY_LOCAL_FILE_MAX_BYTES` | `5242880` | Maximum local proxy/cache file size. |
| `MAX_PROXY_SOURCES` | `16` | Maximum enabled and validated sources fetched per refresh. |
| `PUBLIC_PROXY_ALLOWED_PORTS` | Common proxy ports | Comma-separated ports allowed for untrusted proxy entries. |
`RUST_LOG` | `info` | Tracing filter, for example `nine_router_proxy=debug`.

Proxy URLs may include credentials, but environment variables can be visible to privileged local users and deployment tooling. The application never logs configured proxy URLs.

See `.env.example` for a non-secret template.

## Proxy discovery, warming, and cache

The recommended public-proxy strategy is hybrid rather than cache-only or fetch-per-request:

1. Read `OUTBOUND_PROXIES`, `OUTBOUND_PROXIES_FILE`, the previous `PROXY_CACHE_PATH`, and the currently active public pool as candidates.
2. Fetch enabled and validated entries from `PROXY_SOURCES_PATH` over pinned HTTPS connections.
3. Parse strict URL lines. Remote bare `IP:port` lines require a source `protocol`; configured source protocols normalize SOCKS to `socks4a` or `socks5h` so target DNS does not leak locally.
4. Deduplicate, randomly sample bounded candidates, and probe them concurrently with an anonymous `HEAD` request to `TARGET_BACKEND_URL`.
5. Keep the fastest successful clients in memory, while limiting each public IPv4 `/24` or IPv6 `/64` to two entries.
6. Atomically replace the live pool only when a nonempty refresh succeeds. A failed refresh retains the old pool.
7. Persist only credential-free public winners to the line-based cache. Every cached entry is treated as untrusted and probed again at the next startup or refresh.
8. Select a random eligible warmed proxy for each incoming request. Requests are never retried and never fall back directly. Repeated connection failures quarantine an entry until the next successful refresh.

Fetching a list for every incoming request is not recommended: it adds request latency, couples availability to list hosts, creates rate-limit pressure, and does not prove that a listed proxy works. A cache-only design also becomes stale quickly. The hybrid approach gives fast restart candidates while still refreshing and revalidating them.

A local proxy file uses complete URL lines:

```text
http://198.51.100.30:8080
socks5h://203.0.113.40:1080
```

The addresses above are documentation examples and will be rejected by the public-address filter. Real entries must use globally routable IP literals and allowed ports.

The source catalog schema is compatible with the supplied JSON fields. Unknown metadata is ignored; only entries with `enabled: true`, `validated: true`, and type `github_raw` or `generic_text` are considered. `validated` is an administrative opt-in, not a cryptographic trust guarantee. An explicit protocol is recommended:

```json
{
  "sources": [
    {
      "url": "https://example.com/http-proxies.txt",
      "type": "generic_text",
      "protocol": "http",
      "enabled": true,
      "validated": true,
      "name": "example-http"
    }
  ]
}
```

`proxy-sources.json` contains a small curated subset of the provided raw/API sources rather than HTML pages, Tor exits, or encoded VPN subscriptions. Startup waits for at least one successful warm-up before binding the server. `/health/ready` reports pool availability, generation, and age and becomes unavailable when all entries are quarantined or the dynamic pool is stale.

Warm-up proves that the proxy completed a TLS-verified connection and returned an HTTP response from the configured target. It does not guarantee long-lived stream quality or that the proxy will remain available. Public proxies are inherently volatile and unsuitable for high-assurance production traffic.

## Forward proxy mode and 9router

9router's Proxy Pool setting expects a standard HTTP forward proxy. For an HTTPS provider it sends an authority-form request similar to:

```http
CONNECT integrate.api.nvidia.com:443 HTTP/1.1
Host: integrate.api.nvidia.com:443
```

Configure the 9router pool URL as:

```text
http://127.0.0.1:8080
```

With the default loopback listener, CONNECT destinations are unrestricted but ports default to `443`, which allows generic HTTPS proxy tests. If `LISTEN_ADDR` is non-loopback, the default host allowlist narrows to `TARGET_BACKEND_URL`. Set an explicit policy when needed:

```text
FORWARD_PROXY_ENABLED=true
FORWARD_PROXY_ALLOWED_HOSTS=integrate.api.nvidia.com,api.ipify.org
FORWARD_PROXY_ALLOWED_PORTS=443
```

`FORWARD_PROXY_ALLOWED_HOSTS=*` is convenient for a local-only 9router installation but would create an open proxy if the listener were exposed to other machines. Keep `LISTEN_ADDR=127.0.0.1:8080` when using the wildcard.

CONNECT tunnels can chain through warmed `http`, `socks4`, `socks4a`, `socks5`, and `socks5h` entries. Authenticated SOCKS5 and HTTP proxies are supported. `https://` proxy endpoints can still handle reverse-proxy requests through Reqwest but are not selected for raw CONNECT chaining. `/health/ready` reports `connect_capable_proxies` separately.

Forward mode is an encrypted byte tunnel. The TLS session belongs to 9router and the target provider, so this service cannot inspect or modify the request body. Consequently, aliases and custom bodies from `config.toml` do **not** apply to requests sent through 9router's Proxy Pool. To use model/body rewriting, configure this service as the provider's reverse-proxy base URL instead of as a forward proxy.

CONNECT requests and failures are logged with destination host and port, while upstream proxy URLs and credentials remain hidden.

## Routing behavior

The complete incoming path and query are appended to `TARGET_BACKEND_URL`. For example:

```text
TARGET_BACKEND_URL=https://api.example.com/provider
POST /v1/chat/completions?stream=true
→ POST https://api.example.com/provider/v1/chat/completions?stream=true
```

`/health/live` and `/health/ready` are reserved locally. Readiness indicates valid startup configuration and an available configured client; it deliberately does not probe the backend to avoid restart loops during upstream outages.

For JSON requests ending in `/chat/completions`, `/completions`, `/responses`, or `/messages`, model aliases and custom request bodies are applied from `MODEL_CONFIG_PATH`. Other valid JSON fields are preserved. Compressed model JSON is rejected because safely decoding it would require a separate bounded decompression policy.

## Model quality profiles

TOML is recommended for hand-maintained profiles because it supports comments and readable nested tables. JSON uses the same schema and is useful when another program generates the configuration.

```toml
version = 1

[[models]]
aliases = ["quality/claude", "anthropic/claude(thinking)"]
target = "anthropic/claude"
remove = ["/reasoning_effort", "/thinking_mode"]

[models.body.thinking]
type = "enabled"
budget_tokens = 32000

[models.body.extra_body.routing]
quality_tier = "maximum"
```

Each profile has:

- `aliases`: one or more exact, case-sensitive names accepted in the incoming `model` field.
- `target`: the actual model name sent to the backend.
- `body`: an arbitrary JSON-compatible object recursively merged into the request after alias matching. Profile values win on conflicts; objects merge recursively, while arrays and scalars replace existing values.
- `remove`: optional RFC 6901 JSON Pointers removed before merging `body`. This is useful when providers reject competing parameters such as `reasoning_effort`, `thinking`, `thinking_mode`, or `enable_thinking`.

The `model` field cannot appear in `body` or `/model` removal rules; use `target` so model selection remains unambiguous. Unknown schema keys, duplicate aliases, invalid pointers, and unsupported versions stop startup instead of silently degrading output quality.

`config.toml` includes editable examples for:

- OpenAI-style `reasoning_effort` and `verbosity`.
- Anthropic-style nested `thinking.type` and `thinking.budget_tokens`.
- Gemini-style deeply nested `extra_body.google.thinking_config`.
- Qwen-style `enable_thinking`.
- Gateway-neutral nested `reasoning.effort`.

Provider schemas and model IDs change independently. Treat these profiles as explicit policy owned by your deployment, verify each field against the selected backend, and benchmark quality, cost, latency, and token usage before promoting a profile. Large thinking budgets can improve difficult-task quality but are not universally beneficial.

Equivalent JSON configuration:

```json
{
  "version": 1,
  "models": [
    {
      "aliases": ["quality/example"],
      "target": "provider/example",
      "remove": ["/thinking_mode"],
      "body": {
        "reasoning": {
          "effort": "high"
        }
      }
    }
  ]
}
```

## Development

The lockfile is authoritative for reproducible application builds:

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --locked
cargo build --release --locked
```

Run locally without an outbound proxy only when that behavior is intended:

```sh
TARGET_BACKEND_URL=https://api.openai.com ALLOW_DIRECT=true cargo run --locked
```

The server supports graceful `Ctrl+C` shutdown. Unix builds also handle `SIGTERM`.
