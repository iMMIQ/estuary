# API & Clients

[Documentation Index](README.md) | [Configuration & Security](configuration.md) | [vLLM Provider](vllm.md)

Estuary exposes two HTTP planes on separate ports: a **public inference API** (`--listen`, default `0.0.0.0:8080`) and a **management control plane** (`--admin-listen`, default `127.0.0.1:9090`).

---

## Public Inference API

Public routes are a strict allowlist. Unknown paths or unsupported HTTP methods return a protocol-shaped `404`.

| Method and path | Upstream target | Behavior |
| --- | --- | --- |
| `GET /v1/models` | *(local)* | Lists explicit public model names configured across all nodes. |
| `GET /v1/models/{model}` | *(local)* | Returns metadata for one configured public model (`404` if unmapped). |
| `POST /v1/chat/completions` | `/v1/chat/completions` | Buffered or SSE Chat Completions forwarding; unknown request fields pass through unchanged. |
| `POST /v1/responses` | `/v1/chat/completions` | Foreground buffered or SSE OpenAI Responses adapted onto the Chat backend. |
| `POST /v1/completions` | `/v1/completions` | Buffered or SSE legacy Completions forwarding. |
| `POST /v1/embeddings` | `/v1/embeddings` | Buffered Embeddings forwarding. |
| `POST /v1/messages` | `/v1/chat/completions` | Buffered or SSE Anthropic Messages (text, images, tools, thinking) adapted onto the Chat backend. |
| `POST /v1/messages/count_tokens` | `/v1/chat/completions/render` | Counts rendered prompt tokens without generating output. |

### Headers & Request IDs

- **Request ID**: If the client supplies `x-request-id` (1–128 bytes), Estuary echoes it; otherwise it generates a UUIDv7. Responses always include `x-request-id` (and Anthropic routes also include `request-id`). Estuary forwards the ID upstream as `x-gateway-request-id` and exposes any upstream `x-request-id` downstream as `x-upstream-request-id`.
- **Selected node header**: Set `ESTUARY_EXPOSE_NODE_HEADER=true` to include `x-gateway-node: <node-id>` on inference responses.
- **Streaming**: SSE responses include `x-accel-buffering: no`.
- **Session grouping**: Supply `x-estuary-session-id` (up to 128 bytes) or JSON `metadata.session_id` to group requests in [Session Logging](session-logging.md).
- **Authentication**: When `ESTUARY_PUBLIC_API_KEYS` is configured, pass `Authorization: Bearer <key>` or `x-api-key: <key>`.

### Error Envelopes

Gateway errors match the caller's protocol (`{"error": {"message", "type", "param", "code"}}` on OpenAI endpoints; `{"type": "error", "error": {"type", "message"}, "request_id": "..."}` on `/v1/messages*` and `/v1/files/*`):

| HTTP status | Meaning |
| --- | --- |
| `400 Bad Request` | Invalid JSON, missing `model`, invalid request parameters, or explicitly unsupported feature. |
| `401 Unauthorized` | Missing or invalid public API key (when `ESTUARY_PUBLIC_API_KEYS` is set). |
| `404 Not Found` | Unknown route or unconfigured public model. |
| `408 Request Timeout` | Client request body idle or total read timeout expired. |
| `413 Payload Too Large` | Request body exceeds `ESTUARY_MAX_REQUEST_BODY_BYTES`. |
| `429 Too Many Requests` | Public RPM limit exceeded (includes `Retry-After`) or upstream `429`. |
| `502 Bad Gateway` | Upstream transport failure or invalid upstream response. |
| `503 Service Unavailable` | No healthy/routable upstream node available for the requested model (includes `Retry-After: 1`). |
| `504 Gateway Timeout` | Upstream did not return response headers before `ESTUARY_UPSTREAM_HEADER_TIMEOUT_MS`. |

---

## Protocol Compatibility & Shared Chat Backend

Client `/v1/messages`, `/v1/responses`, and `/v1/chat/completions` all target upstream `/v1/chat/completions`, regardless of provider type (`openai` or `vllm`). Protocol conversion happens once before prefix lookup and node selection, independent of `User-Agent` or model family, using the vendored MIT-licensed `deepseek-recipe` protocol types (without DeepSeek prompt encoders, tokenizers, or sampling overrides).

### Anthropic Messages (`/v1/messages`, `/v1/messages/count_tokens`)

- Preserves text, images, tool definitions/calls/results, reasoning/thinking blocks, and reported usage.
- Maps thinking enablement to `chat_template_kwargs.enable_thinking` and requested budget to `chat_template_kwargs.thinking_budget`; `max_tokens` (required on `/v1/messages`) remains the overall output limit.
- Emits SSE `ping` keep-alive events every 10 seconds during idle periods.
- Strips Claude Code's standalone single-line `x-anthropic-billing-header:` system blocks and removes no-op `context_management` edits (`clear_thinking_20251015` with `keep: "all"`).
- Keeps inline system reminders in their original conversation position as user blocks rather than merging them into the leading system prompt.
- `/v1/messages/count_tokens` converts the request to Chat JSON and posts it to upstream `/v1/chat/completions/render`, returning the rendered token count. (On vLLM 0.30+, enable `--enable-scale-out`; see [vLLM Provider](vllm.md).)

### OpenAI Responses (`/v1/responses`)

- Supports buffered and SSE streaming, standard functions, the `apply_patch` custom tool, structured output formats, image input, and plaintext reasoning history.
- Encodes Responses namespace tools into collision-checked Chat function names and restores original namespaces in buffered and SSE responses.
- Converts named `function_call_output` items without a `call_id` (such as Codex Goals `agent.goal_objective`) into user context preserving tool name, namespace, text, and images rather than emitting an unpaired Chat tool message.
- Buffers parallel tool argument fragments until completion (capped at 16 MiB per response).

### Text-Only Models (`multimodal: false`)

When a public model is configured with `model_capabilities.<model>.multimodal = false` (or via `"*"` fallback), Estuary removes `image`, `image_url`, and `input_image` blocks across Chat, Responses, and Messages payloads (including nested tool results) before routing and replaces each with a text notice informing the model it cannot inspect the image. Image URLs and base64 bytes are never sent upstream.

### Explicitly Rejected Features

Features that require stateful backend storage, server-side search, or non-Chat services fail with `400 Bad Request` instead of being silently dropped:

- `/v1/responses/{id}` (retrieve, delete, cancel) and `/v1/files/*`;
- Responses `background: true`, `previous_response_id`, and `conversation`;
- `document`, `input_file`, `web_search_call`, and `redacted_thinking` content blocks;
- Server-side web search tool choices (`web_search*`);
- Encrypted-only reasoning (`encrypted_content` without plaintext reasoning `content`);
- Non-noop Anthropic `context_management.edits`.

---

## Client Configuration

### OpenAI-Compatible SDKs

Point the base URL at `http://HOST:8080/v1` and request any configured public model name.

### Claude Code

```bash
ANTHROPIC_BASE_URL=http://127.0.0.1:8080 \
ANTHROPIC_API_KEY=estuary \
claude
```

Set `ANTHROPIC_API_KEY` to one of your `ESTUARY_PUBLIC_API_KEYS` values if public auth is enabled (any non-empty value works when public auth is disabled).

### Codex

Configure Codex to use the Responses API with web search disabled:

```toml
model_provider = "estuary"
model = "gateway-chat"
web_search = "disabled"

[model_providers.estuary]
name = "Estuary"
base_url = "http://127.0.0.1:8080/v1"
wire_api = "responses"
requires_openai_auth = false
```

### OpenCode

Configure an OpenAI-compatible provider pointing to `http://127.0.0.1:8080/v1` (supports both Chat Completions and Responses adapters).

---

## Management Control Plane API

`/health/live` and `/health/ready` are unauthenticated. All other management routes require authentication when `ESTUARY_ADMIN_TOKEN` is set (HTTP Basic password for browsers; `Authorization: Bearer <token>` for automation).

### Health, Metrics & Status

| Method and path | Behavior |
| --- | --- |
| `GET /health/live` | Process liveness (`200 {"status": "ok"}`). Use for restart policies. |
| `GET /health/ready` | Traffic readiness (`200` when accepting traffic and at least one node is routable; otherwise `503`). |
| `GET /metrics` | OpenMetrics 1.0.0 exposition; see [Observability & Metrics](observability.md#openmetrics-metrics-reference). |
| `GET /admin/` | Embedded English / Simplified Chinese management application (also redirected from `/` and `/admin`). |
| `GET /admin/api/status` | Fleet, queue, connection, top active client IPs, persisted IP limits, and response-buffer summary. |
| `GET /admin/api/events` | SSE stream emitting `event: revision` whenever SQLite `control_state.revision` advances (15s keep-alive). |
| `GET /admin/api/observability` | Windowed operational telemetry (`scope=fleet\|node\|model`, `id=...`, `window=60\|300\|900`). See [Observability](observability.md#observability-api). |
| `GET /admin/api/process` | Local process lifecycle state, queue counters, and response-buffer state. |
| `PUT /admin/api/process/drain` | Initiates graceful shutdown (`202 Accepted`); disables readiness, waits withdrawal delay, drains, and exits. |

### Node Management

| Method and path | Query / Body | Behavior |
| --- | --- | --- |
| `POST /admin/api/nodes/preflight` | `?clear_api_key=bool&clear_headers=bool`; body: `NodeConfig` | Validates config and probes health/vLLM compatibility without saving. Preserves stored credentials for an existing `id` unless explicitly cleared. |
| `GET /admin/api/nodes` | Optional `If-None-Match` | Lists stored nodes merged with live runtime, admission, and exact-KV snapshots. Supports weak `ETag: W/"rev-<n>"` (`304 Not Modified`). |
| `POST /admin/api/nodes` | Body: `NodeConfig` | Validates, probes, persists, and adds a new node (`201 Created`). |
| `GET /admin/api/nodes/{id}` | — | Returns one node's redacted config, credential source metadata, revision, runtime, and admission state. |
| `PUT /admin/api/nodes/{id}` | Body: `{"revision": n, "config": NodeConfig, "clear_api_key": bool, "clear_headers": bool}` | Revision-checked live update without draining. Probes upstream only if upstream URL, credentials/headers, `health_path`, or `provider` changed. |
| `DELETE /admin/api/nodes/{id}` | `?revision=n&timeout_ms=ms` | Drains the node, waits for active leases across all generations to finish, then deletes (`409 node_still_active` on timeout). |
| `PUT /admin/api/nodes/{id}/drain` | `?wait=bool&timeout_ms=ms` | Marks node draining (`200` if idle, `202` if active leases remain). |
| `DELETE /admin/api/nodes/{id}/drain` | — | Resumes node serving (subject to health, provider, and circuit gates). |

Legacy read/drain aliases `GET /admin/nodes` and `PUT`/`DELETE /admin/nodes/{id}/drain` remain mounted for backward compatibility.

### Per-IP Connection Limits

| Method and path | Body | Behavior |
| --- | --- | --- |
| `PUT /admin/api/ip-limits/{ip}` | `{"limit": n}` | Persists and enforces a per-IP public TCP connection limit (`1 <= limit <= ESTUARY_MAX_CONNECTIONS`). |
| `DELETE /admin/api/ip-limits/{ip}` | — | Removes a persisted per-IP connection limit. |

### Session Log Queries

| Method and path | Behavior |
| --- | --- |
| `GET /admin/api/logs/status` | In-memory session logger health and loss counters (never queries SQLite). |
| `GET /admin/api/logs/requests?since=...&session=...&cursor=...&limit=...` | Cursor-paginated request metadata (default last 7 days, default 50 rows, max 100). |
| `GET /admin/api/logs/requests/{id}` | Full detail for one internal UUIDv7 request: attempts, sparse events, and reconstructed payloads. |
| `GET /admin/api/logs/sessions?since=...` | Up to 100 most recent explicit sessions since Unix ms timestamp `since`. |

### Embedded Management Application

Served from `/admin/` out of embedded precompressed `.gz` assets (decompressed on the fly if the client does not advertise `Accept-Encoding: gzip`), the web UI provides:

- **Overview**: Fleet health, capacity, smoothed throughput/latency charts, active client IP counts, and per-IP connection limit controls.
- **Upstreams & Node Details**: Searchable/filterable node list, runtime & vLLM telemetry, model mapping/protocol review, and drain/resume/delete actions.
- **Node Editor**: Three-step wizard with live validation, connection preflight testing (invalidated on draft edits), unsaved-draft warnings, and optimistic revision-conflict resolution (reload or field-level merge).
- **Session Logs**: Time/session filtering, cursor pagination, per-attempt timings, and expandable client/upstream payloads.
- **Live Sync**: Subscribes to `/admin/api/events` for immediate config-change refreshes alongside 5-second visibility-aware polling with exponential backoff and explicit stale-data notices.
