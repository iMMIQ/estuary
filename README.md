# Estuary

`estuary` is a Rust gateway for a pool of OpenAI-compatible LLM servers. It exposes a stable OpenAI-style endpoint, rewrites public model names to node-specific names, enforces per-node concurrency, and selects a healthy node using current load, recent latency/error signals, and approximate prompt-prefix locality.

This repository is the phase-one foundation. It deliberately does not yet implement client API-key management, tenant quotas or priorities, durable Responses state, or the Anthropic/Claude protocol.

## Supported API surface

| Endpoint | Phase-one behavior |
| --- | --- |
| `GET /v1/models` | Returns the configured public model names, de-duplicated and sorted. Wildcard mappings are not listed. |
| `GET /v1/models/{model}` | Returns a configured public model or an OpenAI-shaped `404` error. |
| `POST /v1/chat/completions` | Foreground, non-streaming and SSE streaming pass-through, including unknown OpenAI-compatible fields. |
| `POST /v1/responses` | Foreground create, non-streaming and SSE streaming pass-through. Durable follow-up operations are not supported. |
| `POST /v1/completions` | Compatibility pass-through with model routing and prefix affinity. |
| `POST /v1/embeddings` | Compatibility pass-through with model routing. |

The four POST routes above are an explicit allowlist. Other methods and `/v1/*` paths return an OpenAI-shaped `404` instead of being blindly proxied.

Responses state is explicitly rejected with an OpenAI-shaped `400` response:

- `background: true`
- non-null `previous_response_id`
- non-null `conversation`
- retrieve, delete, and cancel routes under `/v1/responses/{id}`

These operations require a durable response-to-node mapping. Silently routing them to another node would be incorrect.

The `store` field is not changed or rejected. If it is true, or an upstream stores Responses by default, that object may be persisted upstream but cannot be retrieved through this phase-one gateway. Set `store: false` when that distinction matters.

## Important production boundaries

### No inbound authentication yet

The gateway does not authenticate callers in phase one. Run the public listener only on a trusted network or behind an authenticating reverse proxy/API gateway. Client `Authorization`, `x-api-key`, `OpenAI-Organization`, and `OpenAI-Project` headers are stripped and are never reused as upstream credentials.

Upstream credentials are configured independently per node through environment-variable references. Multi-key lifecycle management, tenant quotas, and priority scheduling are planned follow-up work.

### Concurrency limits are process-local

`max_concurrency` is a hard limit for each node within one gateway process. Its permit is held while an independent pump reads the upstream body into a one-chunk bounded buffer, and is released on upstream EOF, error, configured timeout, or client cancellation. A client that stops draining the buffer cannot hold a node slot beyond `downstream_stall_timeout_ms`. With `N` active-active gateway replicas, a node can receive up to `N * max_concurrency` requests.

Use one active gateway instance in phase one. If multiple active instances are required, divide each node's total budget among the instances and accept that each instance has independent queue and prefix state. A distributed lease service is required for a strict fleet-wide limit.

### Prefix locality is approximate

The gateway does not receive real KV-cache events from generic OpenAI-compatible servers. Following vLLM Router's cache-aware policy, it stores canonical prompt material in an in-memory multi-tenant radix tree and estimates which node handled the longest matching character prefix:

- it improves repeated-system-prompt and multi-turn locality;
- it never bypasses model, health, or concurrency checks;
- it switches completely to load-first routing when both configured load-imbalance thresholds are exceeded;
- it is lost on restart and can differ from the server's real tokenizer or cache eviction state.

For exact KV-aware routing, a later provider adapter must consume server-specific cache events or metrics.

### Retry semantics

Retries are limited to configured statuses and connection failures, use a different eligible node, and stop at `retry.max_attempts`. Once a successful response body is returned to the client, a stream is never switched or spliced.

The default `max_attempts: 1` never replays a generation inside the gateway. Setting it above one explicitly selects at-least-once behavior: a retry after an upstream status can duplicate work or billing, and client SDK retries can multiply gateway retries. Forwarded idempotency headers help only when the upstream nodes share an implementation that honors them.

## Scheduling model

The implementation-level request flow, scoring formula, state transitions, and planned extension seams are documented in [`docs/architecture.md`](docs/architecture.md).

For each request the gateway:

1. filters nodes by model mapping, exclusion set, and health;
2. detects load imbalance using the configured absolute and relative active-request thresholds;
3. while balanced, prefers the radix-tree owner only when the longest-prefix match ratio exceeds `prefix.cache_threshold`; otherwise it uses the load/latency/error score;
4. tries candidates in score order and atomically acquires a node permit;
5. if every eligible node is full, waits in a request-count and byte-bounded queue until capacity is released or `queue_timeout_ms` expires.

Queued requests register in each eligible node semaphore's FIFO wait list. A newly arriving fast-path request cannot take a permit already assigned to an older waiter, while requests for independent model pools do not share a global head-of-line lock. Queue admission failures return `429` with `Retry-After`. No healthy node returns `503`. Upstream transport/protocol failures return `502`, and the upstream response-header timeout returns `504`. Errors generated by the gateway use the OpenAI `{"error": {...}}` envelope.

## Quick start

Rust 1.85 or newer is required.

This repository routes Cargo's crates.io sparse index and crate downloads through the USTC mirror in [`.cargo/config.toml`](.cargo/config.toml). CI also uses USTC's Rust distribution mirror, and the runtime image installs Debian packages from USTC.

```bash
cp config.example.yaml config.yaml
export UPSTREAM_A_API_KEY='replace-me'
export UPSTREAM_B_API_KEY='replace-me'
cargo run --release -- --config config.yaml
```

The config path can also be supplied with `ESTUARY_CONFIG`. Logging is controlled with `RUST_LOG`; `server.log_json` selects JSON or compact formatting.

Check the model catalog:

```bash
curl -sS http://127.0.0.1:8080/v1/models
```

Create a non-streaming Chat Completion:

```bash
curl -sS http://127.0.0.1:8080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "gateway-chat",
    "messages": [
      {"role": "system", "content": "Answer precisely."},
      {"role": "user", "content": "What is prefix caching?"}
    ]
  }'
```

Stream a Chat Completion. `-N` disables curl's output buffering:

```bash
curl -N http://127.0.0.1:8080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "gateway-chat",
    "stream": true,
    "messages": [{"role": "user", "content": "Count from one to five."}]
  }'
```

Create a foreground Response:

```bash
curl -sS http://127.0.0.1:8080/v1/responses \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "gateway-chat",
    "input": "Summarize why bounded queues matter.",
    "store": false
  }'
```

For a streaming Response, add `"stream": true` and use `curl -N`. The gateway forwards upstream SSE bytes and unknown events without rebuilding them.

## Configuration

Configuration is strict YAML: unknown fields and invalid values stop startup. It is loaded once; changing the file requires a restart. See [`config.example.yaml`](config.example.yaml) for every available field.

### Server

| Field | Meaning |
| --- | --- |
| `listen` | Public OpenAI-compatible listener. |
| `admin_listen` | Health, metrics, and node-status listener. Keep it private. Use `0.0.0.0` inside a container when the port must be probed externally. |
| `connect_timeout_ms` | Upstream TCP/TLS connection timeout. |
| `upstream_header_timeout_ms` | Maximum time to receive upstream response headers. A timeout returns `504`. |
| `stream_idle_timeout_ms` | Maximum idle gap between upstream response-body chunks. It applies to SSE and non-SSE bodies and closes a stalled body. |
| `upstream_body_timeout_ms` | Absolute deadline for reading a complete upstream body, including a streaming generation. |
| `downstream_stall_timeout_ms` | Maximum wait for a client to drain the one-chunk response buffer before the upstream reader is cancelled. |
| `shutdown_grace_ms` | Time allowed for active requests/streams to finish after shutdown starts; remaining server tasks are aborted when it expires. |
| `max_request_body_bytes` | Axum request-body hard limit. Queued bodies are additionally controlled by `queue_max_bytes`. |
| `expose_node_header` | Adds `x-gateway-node` to responses. Leave disabled when topology is sensitive. |
| `log_json` | Emit structured JSON logs when true. Prompt and response bodies are not logged. |

### Routing and prefix affinity

`queue_max_requests` and `queue_max_bytes` bound requests waiting for node capacity. `load_weight`, `latency_weight`, and `error_weight` control the fallback score; lower is better. A larger node `weight` makes the node relatively more attractive.

`prefix.cache_threshold` is the strict minimum `matched_chars / input_chars` ratio for cache routing. The scheduler enters load-first mode only when both `max_load - min_load > balance_abs_threshold` and `max_load > min_load * balance_rel_threshold`. `max_request_chars` caps matching work, while `max_tree_chars_per_node` bounds each node's approximate radix-tree footprint with leaf-LRU eviction. Canonical prompt text is stored in process memory, so treat gateway memory as sensitive.

### Nodes and model aliases

`base_url` should include the upstream API prefix, normally `/v1`. The gateway appends `chat/completions`, `responses`, and other endpoint paths. `health_path` is requested with the node headers and credentials.

`api_key_env` is the name of an environment variable containing a Bearer token, not the key itself and not `${VARIABLE}` syntax. For non-Bearer credentials, `headers_from_env` maps a header name to an environment-variable name, for example `api-key: AZURE_OPENAI_API_KEY`. The process fails startup if any referenced variable is missing or empty.

`headers` contains literal static values and should be used only for non-secret metadata. YAML does not perform environment substitution. If both mechanisms set the same header, environment-backed headers replace static values; `api_key_env` finally sets `Authorization: Bearer ...`.

Model mappings are `public-name: upstream-name`. The special value `"*"` preserves the requested public name. A wildcard public key routes models not explicitly listed:

```yaml
models:
  gateway-chat: vendor-model-name
  "*": "*"
```

Only explicit public names appear in `/v1/models`.

## Health, metrics, and operations

The admin listener exposes:

| Endpoint | Meaning |
| --- | --- |
| `GET /health/live` | Process liveness only; always `200` while the server can answer. |
| `GET /health/ready` | `200` when at least one node is healthy or degraded, otherwise `503`. |
| `GET /metrics` | OpenMetrics text exposition. |
| `GET /admin/nodes` | Node URL, health, active/available permits, weights, EWMA values, and last error. Treat as sensitive operational data. |

Do not use upstream availability as a liveness probe: restarting the gateway cannot repair an unavailable model server. Use `/health/live` for container liveness and `/health/ready` for traffic admission.

With the production default `health.route_while_starting: false`, a node is not routable until its first successful active probe; that first success immediately marks it healthy. After a node becomes unhealthy, `healthy_threshold` consecutive successful probes are required for recovery. Active probes are spread by up to `jitter_percent` of the interval to avoid synchronized bursts.

Important metric families include:

- `estuary_requests_total`
- `estuary_upstream_attempts_total`
- `estuary_retries_total`
- `estuary_stream_cancellations_total`
- `estuary_stream_errors_total`
- `estuary_node_active`
- `estuary_node_health`
- `estuary_request_duration_seconds`
- `estuary_queue_duration_seconds`
- `estuary_prefix_match_chars`

Request duration currently ends when the response headers/body handle is created; it is not the full lifetime of a streaming response. Node permits remain held by the upstream pump until EOF, error, cancellation, or a configured body/stall timeout.

## Docker Compose

Compose binds both ports to loopback by default and requires both example upstream keys:

```bash
export UPSTREAM_A_API_KEY='replace-me'
export UPSTREAM_B_API_KEY='replace-me'
docker compose up --build
```

Set `ESTUARY_CONFIG_FILE` to mount another YAML file. The included example addresses upstreams through `host.docker.internal`; Compose maps that hostname to the Docker host on Linux.

The image runs as UID/GID `10001`, has a read-only root filesystem under Compose, does not follow upstream redirects, and uses `/health/live` for its image health check. For Kubernetes, keep the admin port behind a NetworkPolicy and set the pod termination grace period above `shutdown_grace_ms`; after that application grace period, remaining public or admin server tasks and their active streams are aborted.

## Development checks

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
```

Contract tests should additionally exercise slow/cancelled SSE clients, split SSE frames, retry boundaries, queue byte exhaustion, and the concurrency limit with real TCP mock upstreams.

## Planned extension points

The existing separation between request metadata, scheduling, node runtime, and byte-stream proxying is intended to support:

- gateway API-key lifecycle, tenant identity, quota, and priority/fair queues;
- durable Responses state-to-node affinity and background operations;
- Anthropic Messages/Claude Code protocol adapters;
- provider-specific tokenizers, KV-cache events, queue-depth, and GPU telemetry;
- shared prefix/admission state or distributed node leases for active-active gateways;
- atomic configuration reload and controlled node draining.
