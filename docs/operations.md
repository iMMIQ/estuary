# Configuration and Operations

[Documentation index](README.md) | [Deployment](../deploy/README.md) |
[Architecture](architecture.md)

Estuary separates process settings from node settings. Process settings are
resolved at startup from CLI flags and `ESTUARY_*` environment variables. Node
settings are stored in SQLite and managed through the embedded application or
management API.

## Runtime Configuration

The executable help is the complete configuration reference:

```bash
estuary --help
estuary supervisor --help
```

Common settings and defaults:

| Environment variable | Default | Purpose |
| --- | --- | --- |
| `ESTUARY_DATABASE` | `estuary.db` | SQLite database path. |
| `ESTUARY_LISTEN` | `0.0.0.0:8080` | Public inference listener. |
| `ESTUARY_ADMIN_LISTEN` | `127.0.0.1:9090` | Management and health listener. |
| `ESTUARY_ADMIN_TOKEN` | unset | Basic/Bearer token for protected management routes. Required for a non-loopback management listener. |
| `ESTUARY_MAX_CONNECTIONS` | `2048` | Accepted public TCP connections per process. |
| `ESTUARY_MAX_REQUEST_BODY_BYTES` | `16777216` | Maximum inference request body. |
| `ESTUARY_QUEUE_MAX_REQUESTS` | `512` | Requests admitted to upstream-capacity waiting. |
| `ESTUARY_QUEUE_MAX_BYTES` | `268435456` | Aggregate admitted request-body budget. |
| `ESTUARY_MAX_NON_STREAMING_RESPONSE_BYTES` | `67108864` | Maximum buffered response body. |
| `ESTUARY_MAX_BUFFERED_RESPONSE_BYTES` | `268435456` | Aggregate non-streaming response-buffer budget. |
| `ESTUARY_WITHDRAWAL_DELAY_MS` | `10000` | Delay between failed readiness and stopping public accepts. |
| `ESTUARY_SHUTDOWN_GRACE_MS` | `3660000` | Maximum time to drain accepted responses during process shutdown. |
| `ESTUARY_RETRY_MAX_ATTEMPTS` | `1` | Total upstream attempts, validated between 1 and 3. |
| `RUST_LOG` | `info` | Tracing filter. |

Timeouts exist independently for client body reads, upstream connection and
headers, upstream streaming idle time, total upstream body time, and downstream
stall time. Use `estuary --help` for their exact flags and environment names.

## Node Configuration

Each node contains:

| Field | Meaning |
| --- | --- |
| `id` | Unique stable identifier. |
| `base_url` | Absolute HTTP(S) OpenAI-compatible base URL. Credentials, query strings, and fragments are rejected. |
| `models` | Public-to-upstream model mappings. A value of `*` preserves the requested model; a public key of `*` matches unlisted models. |
| `max_concurrency` | Hard in-flight limit in this gateway process. |
| `weight` | Positive scheduling weight. |
| `health_path` | Authenticated active-probe path, default `/v1/models`. |
| `api_key` / `api_key_env` | Stored Bearer key or legacy environment-variable reference. |
| `headers` / `headers_from_env` | Stored custom headers or environment-backed values. Hop-by-hop and gateway-owned headers are rejected. |
| `provider.type` | `openai` or `vllm`. |
| `provider.anthropic_protocol` | `auto`, `native`, `responses`, or `chat`. |
| `provider.kv_events` | Optional vLLM ZMQ KV-event connection and memory limits. |

Create and update operations validate the complete document and probe the
candidate before changing the active scheduler. Updates and deletes drain the
old node and wait for its active leases. Every stored record has an optimistic
revision; stale updates return a conflict rather than overwriting newer data.

SQLite uses WAL mode and transactional migrations. Workers sharing a local
database poll its control revision and reconcile node changes into their own
schedulers. The database and WAL files must remain on a local filesystem.

## Management Listener

The following routes are exposed on the management listener:

| Method and path | Purpose |
| --- | --- |
| `GET /health/live` | Process liveness. Returns `200` while the process can answer. |
| `GET /health/ready` | Traffic readiness. Returns `200` when the process accepts traffic and at least one node is routable; otherwise `503`. |
| `GET /admin/` | Embedded management application. |
| `GET /metrics` | OpenMetrics exposition. |
| `GET /admin/api/status` | Fleet, queue, connection, and response-buffer summary. |
| `GET /admin/api/process` | Local process lifecycle and queue state. |
| `PUT /admin/api/process/drain` | Disable readiness, stop accepting after withdrawal, drain accepted responses, and exit. |
| `POST /admin/api/nodes/preflight` | Validate and probe a candidate without saving it. |
| `GET, POST /admin/api/nodes` | List or create nodes. |
| `GET, PUT, DELETE /admin/api/nodes/{id}` | Read, revision-check update, or gracefully delete a node. |
| `PUT /admin/api/nodes/{id}/drain` | Stop new assignments to a node. |
| `DELETE /admin/api/nodes/{id}/drain` | Resume node assignments, subject to health and provider gates. |

`/health/live` and `/health/ready` are unauthenticated. All other management
routes require authentication when `ESTUARY_ADMIN_TOKEN` is set. Browsers use
HTTP Basic Auth with any username and the token as the password; automation may
send `Authorization: Bearer TOKEN`.

Use liveness for process restart policy and readiness for traffic admission.
Upstream failure intentionally changes readiness, not liveness.

## Metrics

Important metric families include:

- requests, upstream attempts, retries, streaming cancellations, and streaming
  body errors;
- current node activity, health, circuit state, provider readiness, and vLLM
  running/waiting/KV telemetry;
- queued requests and bytes, admission waiters, public connections, and response
  buffer use;
- request and queue duration;
- tokenization outcomes and duration;
- approximate prefix characters and exact prefix tokens selected;
- exact vLLM directory readiness, blocks, and accounted bytes.

All names use the `estuary_` prefix. Labels are bounded to endpoints, statuses,
nodes, and predefined outcomes; credentials are never used as labels.

## Security

- The public inference listener has no inbound authentication. Restrict it by
  network policy or place it behind an authenticating proxy.
- Client authorization, API-key, organization, and project headers are removed.
  Estuary injects only the selected node's configured credentials.
- Redirects to upstream servers are disabled.
- Direct upstream keys and custom header values are plaintext inside SQLite.
  Management responses redact values but do not encrypt storage.
- The management listener should remain private even when token authentication
  is enabled.
- vLLM KV-event ZMQ ports have no application-level authentication or
  encryption and must remain on a private network.

## Shutdown and Capacity

On SIGINT, SIGTERM, or process drain, Estuary disables readiness, waits the
configured withdrawal delay, stops accepting public connections, and drains
accepted queues and response bodies until the shutdown grace expires.

Concurrency limits, queues, health state, circuit state, and prefix directories
are process-local. The built-in supervisor runs two workers, so each node's
configured `max_concurrency` should normally be half of the intended host-wide
limit. See [deployment](../deploy/README.md) for the supported rollout topology.
