# Configuration & Security

[Documentation Index](README.md) | [Deployment](deployment.md) | [API & Clients](api.md)

Estuary separates **process settings** (immutable after startup; passed via CLI flags or `ESTUARY_*` environment variables) from **node & IP-limit settings** (persisted in SQLite and updated live via the management UI or API).

---

## Process Settings

Every CLI flag has a matching `ESTUARY_*` environment variable. Run `estuary --help` to inspect compiled defaults.

### Listeners, Database & Control Plane

| Environment variable | CLI flag | Default | Purpose |
| --- | --- | --- | --- |
| `ESTUARY_DATABASE` | `--database` | `estuary.db` | Local SQLite database path for node configs and IP limits. |
| `ESTUARY_LISTEN` | `--listen` | `0.0.0.0:8080` | Public inference TCP listen address. Must use a different port from `--admin-listen`. |
| `ESTUARY_ADMIN_LISTEN` | `--admin-listen` | `127.0.0.1:9090` | Management UI, admin API, `/metrics`, and health listen address. |
| `ESTUARY_ADMIN_TOKEN` | `--admin-token` | unset | Secret token protecting all admin routes (except `/health/*`). Required when `--admin-listen` is non-loopback. |
| `ESTUARY_LOG_JSON` | `--log-json` | `false` | Emit JSON-formatted tracing logs (`true` in Docker image). |
| `RUST_LOG` | *(env only)* | `info` | `tracing-subscriber` log filter (for example `estuary=info`). |
| `ESTUARY_EXPOSE_NODE_HEADER` | `--expose-node-header` | `false` | Add `x-gateway-node: <node-id>` header to inference responses. |
| `ESTUARY_CONTROL_SYNC_INTERVAL_MS` | `--control-sync-interval-ms` | `500` | Interval for polling SQLite `control_state.revision` across shared local processes. |
| `ESTUARY_NODE_MUTATION_TIMEOUT_MS` | `--node-mutation-timeout-ms` | `30000` | Default wait timeout for draining active leases during node deletion or `drain?wait=true`. |
| `ESTUARY_OBSERVABILITY_MAX_SCOPES` | `--observability-max-scopes` | `32` | Maximum tracked node scopes and model scopes in memory (`1`–`4096`). |

### Public Security, Rate Limiting & Credential Encryption

| Environment variable | CLI flag | Default | Purpose |
| --- | --- | --- | --- |
| `ESTUARY_PUBLIC_API_KEYS` | `--public-api-keys` | unset | Comma-separated static API keys accepted on the public listener (`Authorization: Bearer` or `x-api-key`). |
| `ESTUARY_PUBLIC_RPM_LIMIT` | `--public-rpm-limit` | unset | Positive per-client requests-per-minute cap (60-second fixed window) on the public listener. |
| `ESTUARY_TRUST_PROXY_HEADERS` | `--trust-proxy-headers` | `false` | When `true`, extract the client IP from the first `x-forwarded-for` entry for RPM bucketing when no API key is present. |
| `ESTUARY_CONFIG_ENCRYPTION_KEY` | *(env only)* | unset | Master passphrase enabling authenticated encryption of stored node `api_key` and `headers` values in SQLite. |

### Connections, Buffers & Timeouts

| Environment variable | CLI flag | Default | Purpose |
| --- | --- | --- | --- |
| `ESTUARY_MAX_CONNECTIONS` | `--max-connections` | `2048` | Hard limit on concurrent accepted public TCP connections. |
| `ESTUARY_MAX_ADMIN_CONNECTIONS` | `--max-admin-connections` | `128` | Hard limit on concurrent accepted management TCP connections. |
| `ESTUARY_MAX_REQUEST_BODY_BYTES` | `--max-request-body-bytes` | `16777216` (16 MiB) | Maximum inference request body size. |
| `ESTUARY_MAX_NON_STREAMING_RESPONSE_BYTES` | `--max-non-streaming-response-bytes` | `67108864` (64 MiB) | Maximum single buffered non-streaming upstream response body (up to 4 GiB). |
| `ESTUARY_MAX_BUFFERED_RESPONSE_BYTES` | `--max-buffered-response-bytes` | `268435456` (256 MiB) | Aggregate buffer budget across all concurrent non-streaming responses (`>= max_non_streaming_response_bytes`). |
| `ESTUARY_CONNECT_TIMEOUT_MS` | `--connect-timeout-ms` | `5000` | Upstream TCP/TLS connection timeout. |
| `ESTUARY_REQUEST_BODY_IDLE_TIMEOUT_MS` | `--request-body-idle-timeout-ms` | `30000` | Idle timeout while reading a client request body. |
| `ESTUARY_REQUEST_BODY_TIMEOUT_MS` | `--request-body-timeout-ms` | `300000` | Total timeout for reading a client request body. |
| `ESTUARY_UPSTREAM_HEADER_TIMEOUT_MS` | `--upstream-header-timeout-ms` | `120000` | Timeout waiting for upstream response headers. |
| `ESTUARY_STREAM_IDLE_TIMEOUT_MS` | `--stream-idle-timeout-ms` | `300000` | Idle timeout between upstream SSE chunks. |
| `ESTUARY_UPSTREAM_BODY_TIMEOUT_MS` | `--upstream-body-timeout-ms` | `3600000` | Total timeout for reading an upstream response body. |
| `ESTUARY_DOWNSTREAM_STALL_TIMEOUT_MS` | `--downstream-stall-timeout-ms` | `30000` | Maximum time a streaming pump waits for a stalled downstream consumer. |
| `ESTUARY_WITHDRAWAL_DELAY_MS` | `--withdrawal-delay-ms` | `10000` | Wait duration after disabling `/health/ready` before stopping public accepts during shutdown. |
| `ESTUARY_SHUTDOWN_GRACE_MS` | `--shutdown-grace-ms` | `3660000` | Maximum duration to drain accepted queues and responses during shutdown. |

### Routing, Queueing & Prefix Affinity

| Environment variable | CLI flag | Default | Purpose |
| --- | --- | --- | --- |
| `ESTUARY_QUEUE_MAX_REQUESTS` | `--queue-max-requests` | `512` | Maximum concurrent POST `/v1/*` requests admitted into ingress/scheduling. |
| `ESTUARY_QUEUE_MAX_BYTES` | `--queue-max-bytes` | `268435456` (256 MiB) | Aggregate KiB-rounded request-body admission budget (`>= max_request_body_bytes`). |
| `ESTUARY_LOAD_WEIGHT` | `--load-weight` | `1.0` | Weight of normalized concurrency load in node score. |
| `ESTUARY_LATENCY_WEIGHT` | `--latency-weight` | `0.20` | Weight of normalized TTFT/header latency in node score. |
| `ESTUARY_ERROR_WEIGHT` | `--error-weight` | `1.0` | Weight of error EWMA plus health penalty in node score. |
| `ESTUARY_PREFILL_WEIGHT` | `--prefill-weight` | `1.0` | Weight of pending + incoming uncached prefill tokens (`0.0` disables token prefill scoring). |
| `ESTUARY_DECODE_WEIGHT` | `--decode-weight` | `0.25` | Weight of pending + estimated output tokens (`0.0` disables token decode scoring). |
| `ESTUARY_PREFILL_TOKEN_SCALE` | `--prefill-token-scale` | `4096` | Prefill tokens per score unit (before node weight). |
| `ESTUARY_DECODE_TOKEN_SCALE` | `--decode-token-scale` | `1024` | Decode tokens per score unit (before node weight). |
| `ESTUARY_TARGET_LATENCY_MS` | `--target-latency-ms` | `1000.0` | Latency normalization denominator in milliseconds. |
| `ESTUARY_REQUEST_STATS_STALE_MS` | `--request-stats-stale-ms` | `60000` | Staleness window for per-node latency, error, and typical output token samples. |
| `ESTUARY_PREFIX_ENABLED` | `--prefix-enabled` | `true` | Enable prompt-prefix cache affinity (exact and approximate). |
| `ESTUARY_PREFIX_CACHE_THRESHOLD` | `--prefix-cache-threshold` | `0.5` | Minimum approximate match ratio (`0.0`–`1.0`) for approximate credit and remote vLLM tokenization gating. |
| `ESTUARY_PREFIX_APPROXIMATE_HALF_LIFE_MS` | `--prefix-approximate-half-life-ms` | `300000` | Half-life of historical character-prefix confidence. |
| `ESTUARY_PREFIX_BALANCE_ABS_THRESHOLD` | `--prefix-balance-abs-threshold` | `2` | Absolute excess-request divisor scaling cache-credit decay under load imbalance. |
| `ESTUARY_PREFIX_BALANCE_REL_THRESHOLD` | `--prefix-balance-rel-threshold` | `1.1` | Relative multiplier (`> 1.0`) scaling cache-credit decay under load imbalance. |
| `ESTUARY_PREFIX_MAX_REQUEST_CHARS` | `--prefix-max-request-chars` | `131072` (128 Ki) | Maximum canonical prompt characters indexed per request. |
| `ESTUARY_PREFIX_MAX_TREE_CHARS_PER_NODE` | `--prefix-max-tree-chars-per-node` | `1000000` | Per-node character budget in the approximate radix directory. |
| `ESTUARY_PREFIX_MAX_TREES` | `--prefix-max-trees` | `256` | Maximum distinct `(endpoint, model)` trees retained. |
| `ESTUARY_PREFIX_MAX_DIRECTORY_CHARS` | `--prefix-max-directory-chars` | `16000000` | Global character budget across all approximate radix trees. |

### Health Checks, Circuit Breaker & Retries

| Environment variable | CLI flag | Default | Purpose |
| --- | --- | --- | --- |
| `ESTUARY_HEALTH_INTERVAL_MS` | `--health-interval-ms` | `5000` | Active probe interval. |
| `ESTUARY_HEALTH_TIMEOUT_MS` | `--health-timeout-ms` | `2000` | Per-probe HTTP timeout. |
| `ESTUARY_HEALTH_UNHEALTHY_THRESHOLD` | `--health-unhealthy-threshold` | `3` | Consecutive active probe failures to mark a node `unhealthy`. |
| `ESTUARY_HEALTH_HEALTHY_THRESHOLD` | `--health-healthy-threshold` | `2` | Consecutive active probe successes to recover an `unhealthy` node. |
| `ESTUARY_HEALTH_PASSIVE_FAILURE_THRESHOLD` | `--health-passive-failure-threshold` | `3` | Consecutive inference transport/5xx failures to mark a node `unhealthy`. |
| `ESTUARY_HEALTH_ROUTE_WHILE_STARTING` | `--health-route-while-starting` | `false` | Permit routing before the first active health probe succeeds. |
| `ESTUARY_HEALTH_JITTER_PERCENT` | `--health-jitter-percent` | `20` | Deterministic per-node probe jitter (`0`–`50`). |
| `ESTUARY_CIRCUIT_FAILURE_THRESHOLD` | `--circuit-failure-threshold` | `5` | Consecutive inference failures to open the circuit breaker. |
| `ESTUARY_CIRCUIT_OPEN_MS` | `--circuit-open-ms` | `10000` | Duration a circuit remains open before entering half-open state. |
| `ESTUARY_CIRCUIT_HALF_OPEN_MAX_REQUESTS` | `--circuit-half-open-max-requests` | `1` | Concurrent probe requests allowed while half-open. |
| `ESTUARY_CIRCUIT_HALF_OPEN_SUCCESS_THRESHOLD` | `--circuit-half-open-success-threshold` | `2` | Consecutive half-open successes required to close the circuit. |
| `ESTUARY_RETRY_MAX_ATTEMPTS` | `--retry-max-attempts` | `1` | Total upstream attempts per request (`1`–`3`). |
| `ESTUARY_RETRY_STATUSES` | `--retry-statuses` | `429,502,503,504` | Comma-separated transient HTTP statuses eligible for retry (allowed values: `408`, `409`, `425`, `429`, `500`–`599`). |

### Session Logging Settings

Session logging is disabled by default and configured via eight `--session-log-*` / `ESTUARY_SESSION_LOG_*` options documented in [Session Logging](session-logging.md#configuration).

---

## Node Configuration

Nodes are persisted in SQLite (`node_configs`) and managed through `/admin/` or `/admin/api/nodes`. Every document is strictly validated (`deny_unknown_fields`).

| Field | Required | Default (API / UI preset) | Description |
| --- | --- | --- | --- |
| `id` | Yes | — | Unique immutable identifier. |
| `base_url` | Yes | `""` / `http://127.0.0.1:8000/v1` | Absolute `http://` or `https://` URL without credentials, query string, or fragment. |
| `models` | Yes | `{}` | Non-empty map of `public_model -> upstream_model`. Value `*` preserves the requested model name; key `*` matches any unlisted public model. |
| `model_capabilities` | No | `{}` | Per-public-model capability overrides (`*` as fallback). Supports `{"multimodal": false}` to replace image blocks with a text notice before routing. Models without an explicit entry default to `multimodal: true`. |
| `max_concurrency` | No | `1` / `16` | Positive per-process hard concurrency limit. |
| `weight` | No | `1.0` | Positive finite scheduling weight. |
| `draining` | No | `false` | When `true`, stops new assignments while letting in-flight requests finish. |
| `health_path` | No | `"/v1/models"` | Active health probe path resolved against `base_url` (sent with node credentials). |
| `api_key` | No | `null` | Upstream Bearer token stored in SQLite (encrypted when `ESTUARY_CONFIG_ENCRYPTION_KEY` is set). |
| `api_key_env` | No | `null` | Environment variable name holding the upstream Bearer token. |
| `headers` | No | `{}` | Custom static headers sent to upstream (values encrypted when `ESTUARY_CONFIG_ENCRYPTION_KEY` is set). |
| `headers_from_env` | No | `{}` | Map of `Header-Name -> ENV_VAR_NAME` resolved from process environment. |
| `provider` | No | `openai` / `vllm` | Provider block: `type` (`"openai"` or `"vllm"`) plus vLLM telemetry/KV settings; see [vLLM Provider](vllm.md#configuration). |

Reserved headers rejected in `headers` and `headers_from_env`: `connection`, `content-length`, `host`, `keep-alive`, `proxy-authenticate`, `proxy-authorization`, `proxy-connection`, `te`, `trailer`, `transfer-encoding`, `upgrade`, `x-gateway-request-id`.

Example node document with a text-only model capability override:

```json
{
  "id": "gpu-node-1",
  "base_url": "http://10.0.0.12:8000/v1",
  "models": {
    "gateway-chat": "Qwen/Qwen3-32B",
    "gateway-text-only": "Qwen/Qwen3-8B"
  },
  "model_capabilities": {
    "gateway-text-only": { "multimodal": false }
  },
  "max_concurrency": 16,
  "weight": 1.0,
  "health_path": "/v1/models",
  "provider": { "type": "vllm" }
}
```

### Live Node Updates & Concurrency

Every stored node carries an integer `revision`. Updates require the current `revision` and return `409 Conflict` (`revision_conflict`) on stale writes. Node updates apply **live without draining active requests**:

| Changed fields | Live update behavior |
| --- | --- |
| `models`, `model_capabilities`, `weight` | Applied immediately to future scheduling decisions; no upstream preflight probe. |
| `max_concurrency` | Shared resizable semaphore: increases wake queued waiters immediately; decreases reclaim free permits immediately and absorb busy permits on release without cancelling requests. |
| `draining` | Immediately stops or resumes new assignments; in-flight leases continue. |
| `base_url`, credentials/headers, `health_path`, `provider.*` | Probed while the existing node keeps serving; once verified and persisted, new requests use the new snapshot while in-flight requests finish on their original immutable snapshot. |

Any configuration replacement clears cached prefix/KV ownership for that node and restarts its provider monitors. `DELETE /admin/api/nodes/{id}` drains the node and waits for all active leases across every live configuration generation to finish before deleting; if draining times out, it returns `409 Conflict` (`node_still_active`) and leaves the node draining.

---

## Security Model

### Public Inference Listener

- **Authentication**: Disabled by default. Set `ESTUARY_PUBLIC_API_KEYS` (comma-separated) to require a valid key via `Authorization: Bearer <key>` or `x-api-key: <key>` (compared in constant time). Without static keys configured, keep the public port on a trusted network or behind an authenticating reverse proxy.
- **Rate limiting (RPM)**: Set `ESTUARY_PUBLIC_RPM_LIMIT` to enforce a bounded per-client 60-second fixed-window cap (returning `429 Too Many Requests` with `Retry-After`). Buckets are keyed by BLAKE3 hash of the presented API key; when no key is presented, requests bucket by the first `x-forwarded-for` IP only if `ESTUARY_TRUST_PROXY_HEADERS=true` (otherwise all unauthenticated callers share a single `ip:default` bucket). Enable `ESTUARY_TRUST_PROXY_HEADERS` only when Estuary sits behind a trusted proxy that overwrites `x-forwarded-for`.
- **Per-IP TCP connection limits**: Administrators can cap concurrent public TCP connections per source IP via `PUT /admin/api/ip-limits/{ip}` (persisted in SQLite and reconciled across shared local workers). Connections above a configured IP's limit are rejected at TCP accept time.

### Management Listener

- `/health/live` and `/health/ready` are unauthenticated for orchestrator probes.
- All other routes (`/admin/*`, `/metrics`) require authentication whenever `ESTUARY_ADMIN_TOKEN` is set (and setting it is mandatory whenever `--admin-listen` is not loopback). Browsers authenticate via HTTP Basic Auth (any username, token as password); automation may send `Authorization: Bearer <token>`.
- Keep `--admin-listen` on a private interface or loopback even with token auth enabled. Embedded UI responses enforce `Content-Security-Policy`, `X-Frame-Options: DENY`, `X-Content-Type-Options: nosniff`, `Referrer-Policy: no-referrer`, and `Permissions-Policy`.

### Upstream Isolation & Secret Storage

- **Inbound header scrubbing**: Estuary strips `authorization`, `cookie`, `x-api-key`, `openai-organization`, `openai-project`, all `anthropic-*` headers, `x-request-id`, `x-gateway-request-id`, `content-length`, and hop-by-hop headers before forwarding, injecting only the selected node's configured credentials and custom headers.
- **No redirects**: Upstream HTTP redirects are disabled (`Policy::none()`) to prevent credential leakage and SSRF pivots.
- **Encryption at rest**: Set `ESTUARY_CONFIG_ENCRYPTION_KEY` to encrypt node `api_key` and `headers` values before writing to SQLite using BLAKE3 XOF stream encryption with Encrypt-then-MAC (`enc:v1:<base64>`) bound via AAD to the node ID and field name. Legacy plaintext records remain readable when the key is introduced and are encrypted on next save; if a database contains `enc:v1:` secrets, startup and reads fail cleanly unless `ESTUARY_CONFIG_ENCRYPTION_KEY` is provided. When `ESTUARY_CONFIG_ENCRYPTION_KEY` is unset, stored secrets are plaintext in SQLite — protect the database, `-wal`, snapshots, and backups accordingly. Management API responses always redact stored `api_key` and `headers` values.
- **vLLM ZMQ transport**: ZMQ KV-event publish and replay sockets carry no application-level authentication or encryption; restrict them to a private network.
- **Retry semantics**: Generation retries (`ESTUARY_RETRY_MAX_ATTEMPTS > 1`) are at-least-once and can duplicate upstream compute or billing. Default is `1` (no retry).
