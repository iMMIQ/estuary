# Estuary

[![CI](https://github.com/iMMIQ/estuary/actions/workflows/ci.yml/badge.svg)](https://github.com/iMMIQ/estuary/actions/workflows/ci.yml)
[![Release](https://github.com/iMMIQ/estuary/actions/workflows/release.yml/badge.svg)](https://github.com/iMMIQ/estuary/actions/workflows/release.yml)

`estuary` is a Rust gateway for a pool of OpenAI-compatible LLM servers. It exposes OpenAI and Anthropic client protocols on one public listener, rewrites public model names to node-specific names, enforces per-node concurrency, and selects a healthy node using current load, recent latency/error signals, and prompt-prefix locality.

This repository contains the phase-one gateway foundation plus a native vLLM provider for vLLM 0.25 and newer. It deliberately does not yet implement client API-key management, tenant quotas or priorities, or durable Responses state.

## Supported API surface

| Endpoint | Phase-one behavior |
| --- | --- |
| `GET /v1/models` | Returns the configured public model names, de-duplicated and sorted. Wildcard mappings are not listed. |
| `GET /v1/models/{model}` | Returns a configured public model or an OpenAI-shaped `404` error. |
| `POST /v1/chat/completions` | Foreground, non-streaming and SSE streaming pass-through, including unknown OpenAI-compatible fields. |
| `POST /v1/responses` | Foreground create, non-streaming and SSE streaming. Ordinary clients pass through; detected Codex requests selected onto vLLM receive the compatibility mapping described below. Durable follow-up operations are not supported. |
| `POST /v1/completions` | Compatibility pass-through with model routing and prefix affinity. |
| `POST /v1/embeddings` | Compatibility pass-through with model routing. |
| `POST /v1/messages` | Anthropic Messages, including streaming, thinking, tools, model aliases, Claude Code request cleanup, and keep-alive pings. Each node can use native Messages, OpenAI Responses, or Chat Completions upstream. |
| `POST /v1/messages/count_tokens` | Exact Anthropic token counting through a node configured for native Messages. |
| `HEAD /api/hello` | Claude Code gateway capability probe. |

The six POST routes above are an explicit allowlist. Other methods and `/v1/*` paths return a shaped `404` instead of being blindly proxied. OpenAI routes use OpenAI error envelopes; both Messages routes use Anthropic error envelopes and expose `request-id` plus `x-request-id`. Claude Code file-download paths return an explicit Anthropic `400`: vLLM has no Files storage service, so Estuary cannot resolve a session file ID without a separately configured file backend.

vLLM 0.25's generate router natively exposes `/v1/messages` and `/v1/messages/count_tokens`. Estuary uses those native routes after removing Claude Code's billing marker, normalizing its no-op `clear_thinking` edit, rewriting the model alias, and mapping Anthropic thinking to vLLM's `chat_template_kwargs.enable_thinking`. Native responses and SSE retain thinking text, signatures, and future fields while their model name is mapped back to the public alias. For vLLM nodes, Estuary removes `thinking.display: omitted` and returns the generated thinking so Claude Code can carry it into the next turn; vLLM signatures alone cannot restore omitted reasoning. This is the preferred Claude Code path.

vLLM 0.25's native Messages request model does not expose its `thinking_token_budget` sampling field. Estuary therefore preserves Anthropic `budget_tokens`, enables the model's thinking template, and lets `max_tokens` enforce the total output ceiling, but cannot enforce the exact thinking-only ceiling. Such responses carry `x-estuary-thinking-budget: approximated-by-max-tokens`. Non-no-op Anthropic context-management edits are rejected explicitly instead of being silently ignored by vLLM.

Each node has an `anthropic_protocol` capability mode. `auto` resolves to native Messages for vLLM and Chat Completions for generic OpenAI-compatible nodes; operators can explicitly select `native`, `responses`, or `chat`. The Responses adapter sends the complete conversation with `store: false`, supports multimodal tool results and parallel function calls, and converts both buffered responses and SSE. For adaptive thinking it carries `reasoning.encrypted_content` inside an Estuary-formatted Anthropic signature and restores that state on the next request. The Chat adapter uses the standard `max_completion_tokens` and does not claim thinking continuity. Before queueing, Estuary excludes adapters that cannot represent the request. Server tools, redacted thinking, exact thinking budgets, and other unsupported features therefore fail over only to a capable node instead of being silently removed. All Anthropic streams emit gateway keep-alive `ping` events without extending upstream idle deadlines.

Responses state is explicitly rejected with an OpenAI-shaped `400` response:

- `background: true`
- non-null `previous_response_id`
- non-null `conversation`
- retrieve, delete, and cancel routes under `/v1/responses/{id}`

These operations require a durable response-to-node mapping. Silently routing them to another node would be incorrect.

The `store` field is not changed or rejected. If it is true, or an upstream stores Responses by default, that object may be persisted upstream but cannot be retrieved through this phase-one gateway. Set `store: false` when that distinction matters.

## Codex with vLLM

Codex uses the Responses API and replays the complete turn history. Point Codex at Estuary with a provider in `~/.codex/config.toml`:

```toml
model_provider = "estuary"
model = "gpt-oss-20b"
web_search = "disabled"

[model_providers.estuary]
name = "Estuary"
base_url = "http://127.0.0.1:8080/v1"
wire_api = "responses"
requires_openai_auth = false

[features]
multi_agent = true
```

Use the public model name configured in Estuary. Multi-agent mode can remain enabled: when a request is identifiable as Codex and is assigned to a vLLM node, Estuary flattens Codex namespace functions to collision-checked vLLM function names and restores `namespace` plus `name` in both buffered and SSE responses. Standard functions, structured output, image input, full-history replay, and `prompt_cache_key` otherwise retain their Responses shapes.

The selected Codex model profile must use the full Responses request shape (`use_responses_lite: false` in Codex model metadata). Responses Lite custom calls, tool-search items, and `additional_tools` cannot be rendered by vLLM 0.25's Harmony path and receive an actionable `400`. Estuary also rejects Codex web search because this deployment has no search backend; keep `web_search = "disabled"`. If an existing thread already contains one of these unsupported history items, start a new thread after changing the setting. These checks apply only to detected Codex requests routed to vLLM nodes; ordinary OpenAI-compatible Responses traffic remains unchanged.

## Important production boundaries

### No inbound authentication yet

The gateway does not authenticate callers in phase one. Run the public listener only on a trusted network or behind an authenticating reverse proxy/API gateway. Client `Authorization`, `x-api-key`, `OpenAI-Organization`, and `OpenAI-Project` headers are stripped and are never reused as upstream credentials.

Upstream Bearer credentials are configured independently per node in the management UI and stored as plaintext inside the SQLite node document. The management API reports whether a key is configured but never returns its value. Existing `api_key_env` and `headers_from_env` references remain supported for compatibility. Multi-key lifecycle management, tenant quotas, and priority scheduling are planned follow-up work.

This limitation applies to the public inference listener. The management listener has an independent `ESTUARY_ADMIN_TOKEN`: when configured, browsers use HTTP Basic Auth (any username, token as password) and automation may send `Authorization: Bearer <token>`. A non-loopback management bind is rejected unless this token is configured. `/health/live` and `/health/ready` remain unauthenticated for load-balancer probes.

### Concurrency limits are process-local

`max_concurrency` is a hard limit for each node within one gateway process. Streaming responses use an independent one-chunk bounded pump; non-streaming successes are buffered up to `max_non_streaming_response_bytes` before downstream commit. The permit is released on upstream EOF, error, configured timeout, or client cancellation. A client that stops draining a stream cannot hold a node slot beyond `downstream_stall_timeout_ms`. With `N` active-active gateway replicas, a node can receive up to `N * max_concurrency` requests.

The supported production layout runs two or more fixed process slots on one host. Divide each node's total budget among the serving slots; queues, health, circuits, and prefix state remain process-local. The rolling deployer replaces one slot at a time without overlapping generations in that slot. A distributed lease service is still required for a strict fleet-wide limit across arbitrary replicas.

### Prefix locality and vLLM

For generic OpenAI-compatible servers, the gateway stores canonical prompt material in an in-memory multi-tenant radix tree and estimates which node handled the longest matching character prefix:

- it improves repeated-system-prompt and multi-turn locality;
- it never bypasses model, health, or concurrency checks;
- it switches completely to load-first routing when both configured load-imbalance thresholds are exceeded;
- it is lost on restart and can differ from the server's real tokenizer or cache eviction state.

Before routing or forwarding a JSON inference request, Estuary removes Claude Code's standalone `x-anthropic-billing-header:` system text block. That block contains the CLI version/build identifier and entrypoint but no model instruction, so retaining it fragments both local affinity and upstream KV prefixes across Claude Code releases. The match is intentionally narrow: multiline content, environment details, tool instructions, ordinary system messages, and user content are preserved. This cleanup applies before Messages-to-Chat conversion and vLLM tokenization.

Nodes configured with `provider.type: vllm` are version-gated to vLLM 0.25.0 or newer. Estuary scrapes native running, waiting, and KV-use metrics; conditionally calls `/tokenize` for Chat Completions and string Completions; and consumes vLLM's ZMQ KV events. Remote tokenization is attempted only when the approximate prefix already exceeds `prefix.cache_threshold` and an authoritative exact directory contains blocks. One selected tokenizer gets one total `provider.request_timeout_ms` deadline; failure degrades immediately instead of fanning out across the pool. Exact token-block matches take precedence over the character estimate. A missing metric, tokenize failure, event disconnect, sequence gap, or unsupported request shape degrades safely to local load and approximate affinity instead of blocking inference.

### Retry semantics

Retries are limited to configured statuses, connection failures, and failed or invalid non-streaming success bodies. They use a different eligible node and stop at `retry.max_attempts`. Non-streaming success bodies are committed atomically; streaming responses are fixed to one node as soon as successful response headers arrive and are never switched or spliced.

The default `max_attempts: 1` never replays a generation inside the gateway. Setting it above one explicitly selects at-least-once upstream execution: atomic downstream delivery prevents mixed or partial non-streaming responses, but a retry after an upstream status or body failure can still duplicate model work or billing. Client SDK retries can multiply gateway retries. Forwarded idempotency headers help only when the upstream nodes share an implementation that honors them.

## Scheduling model

The implementation-level request flow, scoring formula, state transitions, and planned extension seams are documented in [`docs/architecture.md`](docs/architecture.md).

For each request the gateway:

1. filters nodes by model mapping, exclusion set, and health;
2. holds a vLLM node out of admission while fresh `waiting` telemetry is at or above its `provider.waiting_threshold`;
3. detects load imbalance using the configured absolute and relative active-request thresholds;
4. while balanced, prefers the radix-tree owner only when the longest-prefix match ratio exceeds `prefix.cache_threshold`; otherwise it uses the load/latency/error score;
5. tries candidates in score order and atomically acquires a node permit;
6. if every eligible node is full or above the vLLM waiting watermark, keeps the request pending until capacity becomes available or the client disconnects.

Queued requests use one scheduling class and register in each eligible node semaphore's FIFO wait list. A newly arriving fast-path request cannot take a permit already assigned to an older waiter, while requests for independent model pools do not share a global head-of-line lock. Capacity release wakes the affected semaphore FIFO rather than broadcasting to every queued request; registry, health, and telemetry changes retain the global eligibility notification. The count and byte limits bound how many requests simultaneously register with node semaphores; excess requests wait for queue admission instead of receiving a gateway-generated `429`. No healthy node returns `503`. Upstream transport/protocol failures return `502`, and the upstream response-header timeout returns `504`. Gateway errors follow the protocol of the selected public endpoint.

Each node also has an independent circuit breaker. Consecutive transport, 5xx, or upstream-body failures open it and remove the node from routing. After `open_ms`, a bounded number of real inference requests enter half-open state; the configured success streak closes the circuit, while any half-open failure reopens it. Upstream `429` is treated as load pressure rather than a circuit failure.

## Quick start

Rust 1.85 or newer is required.

This repository routes Cargo's crates.io sparse index and crate downloads through the USTC mirror in [`.cargo/config.toml`](.cargo/config.toml). GitHub Actions deliberately uses the official Cargo, Rust, npm, and GitHub sources.

```bash
mkdir -p data
cargo run --release -- \
  --database ./data/estuary.db \
  --listen 0.0.0.0:8080 \
  --admin-listen 127.0.0.1:9090
```

Open `http://127.0.0.1:9090/admin/` and add the first upstream. The process starts normally with an empty database; readiness remains `503` until a routable node exists. CLI options have `ESTUARY_*` environment-variable equivalents. In particular, `ESTUARY_WITHDRAWAL_DELAY_MS` controls load-balancer propagation and `ESTUARY_SHUTDOWN_GRACE_MS` bounds response draining. Logging filters use `RUST_LOG`.

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

Use the Anthropic endpoint through the same public port:

```bash
curl -N http://127.0.0.1:8080/v1/messages \
  -H 'Content-Type: application/json' \
  -H 'anthropic-version: 2023-06-01' \
  -d '{
    "model": "gateway-chat",
    "max_tokens": 256,
    "stream": true,
    "messages": [{"role": "user", "content": "Inspect this request path."}]
  }'
```

Claude Code can use the same listener. Any non-empty placeholder key is accepted until inbound API-key management is implemented:

```bash
ANTHROPIC_BASE_URL=http://127.0.0.1:8080 \
ANTHROPIC_API_KEY=estuary \
claude
```

## Configuration and persistence

SQLite is the only node-configuration source. The schema is initialized automatically, uses WAL mode and optimistic revisions, and supports an empty first boot. Additive triggers maintain a database-wide control revision; processes sharing the same local database poll that revision and reconcile node create, update, delete, drain, and resume operations into their own schedulers. Node candidates are validated and probed before entering any scheduler. Updating or deleting a node first drains it and waits for active leases; a timeout leaves the node safely draining for a later retry.

The management UI at `/admin/` edits node URLs, model aliases, concurrency, weights, provider settings, the Anthropic upstream protocol, a direct Bearer API key, and vLLM KV event endpoints. Direct keys are persisted unencrypted in the node's SQLite `config_json`; list and detail responses replace the value with `null` and expose only credential status. Direct custom header values are also omitted; only their names are returned, and an empty header map preserves the stored values unless `clear_headers: true` is explicitly sent. An empty Bearer key while editing preserves the stored value, while **Remove key** explicitly clears it. Treat the database, WAL files, filesystem snapshots, and backups as secrets. Legacy `api_key_env` and `headers_from_env` references remain valid and are used when no direct key is configured.

The public and admin listeners are process bootstrap settings because they are needed before SQLite and the UI can be reached. Keep the admin listener on a private network even when authentication is enabled. Global routing, prefix, health, retry, circuit, timeout, request-body, response-body, and shutdown policies can be overridden through CLI flags or their `ESTUARY_*` environment variables; `estuary --help` is the authoritative list. Node-specific configuration remains exclusively in SQLite and the management UI.

`ESTUARY_MAX_NON_STREAMING_RESPONSE_BYTES` limits one upstream response (64 MiB by default). `ESTUARY_MAX_BUFFERED_RESPONSE_BYTES` limits all non-streaming response buffers in the process (256 MiB by default) and must be at least the single-response limit. A response reserves its maximum allowance before reading, then releases unused bytes after EOF. When the global allowance is occupied, upstream body reads wait and apply transport backpressure; Estuary does not synthesize `429`. The remaining reservation follows the downstream response body and is released when delivery finishes or is cancelled.

`ESTUARY_QUEUE_MAX_REQUESTS` and `ESTUARY_QUEUE_MAX_BYTES` are pre-body ingress budgets for inference requests. When either budget is exhausted, new requests remain pending before their body is read, applying transport backpressure without returning `429`. Requests without `Content-Length` conservatively reserve one maximum-sized body. The queue counters in the UI count the admitted requests currently waiting for a node.

Model mappings associate a public model name with a node-specific upstream name. The special upstream value `"*"` preserves the requested public name; a wildcard public key routes unlisted models. Only explicit public names appear in `/v1/models`.

### Native vLLM provider

Declare `provider.type: vllm` on each independently addressable vLLM engine. Estuary checks the origin-root `/version` endpoint and keeps versions below 0.25.0 out of rotation. `/metrics` supplies `vllm:num_requests_running`, `vllm:num_requests_waiting`, and `vllm:kv_cache_usage_perc`; stale telemetry is ignored for scheduling. Fresh waiting depth at or above the routing watermark stops new admission to that node, including cache-affine requests, until a later scrape reports recovery.

The full compatibility contract and failure semantics are in [`docs/vllm.md`](docs/vllm.md).

Gateway overhead can be measured without a GPU using the standalone [performance benchmark](docs/performance.md).

Enable KV events on vLLM with a replay endpoint:

```bash
vllm serve MODEL \
  --kv-events-config \
  '{"enable_kv_cache_events":true,"publisher":"zmq","endpoint":"tcp://*:5557","replay_endpoint":"tcp://*:5558","topic":"kv-events","buffer_steps":10000}'
```

The vLLM process uses bind addresses such as `tcp://*:5557`. Estuary configuration must use concrete connect addresses visible from the gateway, such as `tcp://vllm-0.internal:5557`. ZMQ KV event ports have no application-level authentication in this protocol; expose them only on a private network.

vLLM 0.25 and 0.26 use different replay frame layouts; Estuary accepts both. Estuary actively requests replay after connecting. Sequence gaps and disconnects suspend exact affinity until history is recovered contiguously. Replay-buffer overflow, invalid payloads, and sequence rollback clear learned state and remain degraded until replay from sequence zero succeeds or `AllBlocksCleared` establishes a new baseline. The current exact path intentionally ignores remote/non-GPU blocks, LoRA-specific blocks, and blocks with multimodal or salted extra keys. Responses requests and unsupported prompt shapes retain approximate affinity.

For data parallel deployments, each Estuary node must map to a vLLM cache domain that the gateway can actually select. If one HTTP endpoint randomly dispatches across multiple hidden DP ranks, rank-level KV locality is not actionable. vLLM offsets event ports by DP rank; expose ranks separately when exact rank affinity is required.

Every vLLM node advertising the same public model must use tokenization-equivalent model revisions, chat templates, and prompt-processing settings. Give incompatible pools different public model names; Estuary does not assume that two different token sequences are interchangeable.

## Health, metrics, and operations

The admin listener exposes:

| Endpoint | Meaning |
| --- | --- |
| `GET /health/live` | Process liveness only; always `200` while the server can answer. |
| `GET /health/ready` | `200` when at least one node is serving and passes health, provider, and circuit gates; otherwise `503`. |
| `GET /metrics` | OpenMetrics text exposition. |
| `GET /admin/` | Embedded management application. |
| `GET /admin/api/process` | Process lifecycle, in-flight response, and local queue status. |
| `PUT /admin/api/process/drain` | Disable readiness, wait for LB withdrawal, stop accepting new connections, finish accepted responses, and exit. |
| `GET /admin/nodes` | Node URL, health, active/available permits, weights, EWMA values, and last error. Treat as sensitive operational data. |
| `GET/POST /admin/api/nodes` | List or preflight and create persisted nodes. |
| `GET/PUT/DELETE /admin/api/nodes/{id}` | Read, revision-checked update, or graceful deletion. |
| `PUT /admin/nodes/{id}/drain` | Stop new assignments while active requests finish. Add `?wait=true&timeout_ms=30000` to wait for zero active leases; timeout returns `202`. |
| `DELETE /admin/nodes/{id}/drain` | Resume assignments, subject to health, provider, and circuit gates. |

Do not use upstream availability as a liveness probe: restarting the gateway cannot repair an unavailable model server. Use `/health/live` for process supervision and `/health/ready` for traffic admission.

With the production default `health.route_while_starting: false`, a node is not routable until its first successful active probe; that first success immediately marks it healthy. After a node becomes unhealthy, `healthy_threshold` consecutive successful probes are required for recovery. Active probes are spread by up to `jitter_percent` of the interval to avoid synchronized bursts.

Important metric families include:

- `estuary_requests_total`
- `estuary_upstream_attempts_total`
- `estuary_retries_total`
- `estuary_stream_cancellations_total`
- `estuary_stream_errors_total`
- `estuary_node_active`
- `estuary_node_health`
- `estuary_node_accepting_requests`
- `estuary_node_circuit_state`
- `estuary_node_provider_ready`
- `estuary_node_provider_state`
- `estuary_node_upstream_running`
- `estuary_node_upstream_waiting`
- `estuary_node_kv_cache_usage_ratio`
- `estuary_node_exact_kv_ready`
- `estuary_node_exact_kv_blocks`
- `estuary_node_exact_kv_bytes`
- `estuary_request_duration_seconds`
- `estuary_queue_duration_seconds`
- `estuary_response_buffer_bytes`
- `estuary_response_buffer_waiters`
- `estuary_tokenization_outcomes_total`
- `estuary_tokenization_duration_seconds`
- `estuary_prefix_match_chars`
- `estuary_prefix_match_tokens`

Request duration includes the complete buffered body for non-streaming successes, but ends when the streaming response handle is created. Streaming node permits remain held by the upstream pump until EOF, error, cancellation, or a configured body/stall timeout. SIGINT, SIGTERM, and the process-drain API first make readiness fail, wait `withdrawal_delay_ms`, stop the public listener, and then let accepted queue entries and complete response bodies finish within `shutdown_grace_ms`. Upstream nodes remain routable to accepted requests during this process drain.

## Zero-downtime binary deployment

GitHub Releases contain static Linux binaries for `amd64` and `arm64`, together with the files under [`deploy/`](deploy/). The supported deployment uses HAProxy on stable ports and two fixed systemd process slots on one host:

```console
sudo ./deploy/install.sh ./estuary
```

Review `/etc/estuary/common.env` for gateway settings and any legacy environment-backed credentials. HAProxy exposes the public API on `:8080` and management on loopback `:9090`; slot listeners stay on loopback-only internal ports.

Roll out a staged binary one slot at a time:

```console
sudo ./deploy/rollout.sh ./estuary
```

The script drains slot A and waits for every accepted response before replacing it, verifies readiness, then repeats for slot B. Failed replacements automatically restore the previous binary for that slot. A stream exceeding the configured deploy deadline leaves its old process alive and drained instead of being killed. Full topology, capacity, package, and rollback details are in [`deploy/README.md`](deploy/README.md).

## Development checks

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
cd web && bun install --frozen-lockfile && bun run test && bun run build
bunx --bun playwright install chromium && bun run test:e2e
```

Contract tests should additionally exercise slow/cancelled SSE clients, split SSE frames, retry boundaries, queue byte exhaustion, and the concurrency limit with real TCP mock upstreams.

## Planned extension points

The existing separation between request metadata, scheduling, node runtime, and byte-stream proxying is intended to support:

- gateway API-key lifecycle, tenant identity, quota, and priority/fair queues;
- durable Responses state-to-node affinity and background operations;
- additional Anthropic mappings for server tools, citations, and Files;
- additional provider adapters and local tokenizer implementations;
- shared prefix/admission state or distributed node leases for active-active gateways;
- persisted global policy management and fleet-coordinated node lifecycle state.
