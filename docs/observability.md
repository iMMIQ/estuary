# Observability & Metrics

[Documentation Index](README.md) | [API & Clients](api.md) | [Session Logging](session-logging.md)

Estuary maintains bounded, process-local operational telemetry for the embedded dashboard, `GET /admin/api/observability`, and OpenMetrics `GET /metrics`. Collection is always active, never reads session-log SQLite tables, and never stores prompt or response content.

---

## Sampling, Smoothing & History

- **Server-side 1-second sampler**: Independently of browser connections, Estuary samples counters every second using the monotonic clock.
- **Rate variants**: Each throughput rate reports:
  - `raw`: counter delta divided by actual monotonic elapsed seconds;
  - `smoothed`: time-aware EWMA with a 10-second half-life (gateway scopes start from a known `0.0` baseline to avoid cold-start spikes; engine rates warm up from the first valid delta);
  - `mean_60s`: trailing 60-second average.
- **Windowed distributions (`1m` / `5m` / `15m`)**: Counters and histograms are stored in 10-second buckets (up to 90 buckets per scope). Once a full window exists, the oldest partial bucket is excluded, so `observed_seconds` may be up to 10 seconds shorter than the requested window.
- **15-minute history charts**: Retains up to 180 five-second points (`900` seconds). Each point stores smoothed rates, rolling 5-minute `ttft_p95` and `queue_p95`, live pressure, and sampled peaks (`output`, `active`, `queued`, `waiting`, `kv_usage`). In-flight `active` peaks also track every request start/completion between ticks so sub-second bursts remain visible. A backward wall-clock jump clears chart history.
- **Histograms**: 80 fixed bins (`0`, geometric boundaries starting at `0.0005` multiplying by `1.25`, and `+Inf` overflow). Units are seconds except `generation_rate` (tokens/s). Quantiles (`p50`, `p95`) are interpolated and return `null` if the rank falls in the overflow bin.
- **Dashboard refresh**: The UI refreshes via `/admin/api/events` SSE notifications on config changes plus 5-second polling when visible (pausing in hidden tabs and backing off exponentially on failure while marking stale data).

---

## Metric Definitions

Primary gateway request telemetry covers `POST` to `/v1/chat/completions`, `/v1/responses`, `/v1/completions`, `/v1/messages`, and `/v1/embeddings`. Health probes, `/v1/models*`, and `/v1/messages/count_tokens` are excluded. Embeddings contribute request and input-token counts, not generation-speed samples. Failures before model attribution count in fleet totals without a model scope.

| Metric | Definition |
| --- | --- |
| Incoming / completed req/s | Logical downstream request starts / final successes. |
| Gateway input / output tok/s | Upstream-reported token usage booked at downstream completion or cancellation (can lag live engine generation). |
| Gateway TTFT (`ttft`) | Gateway receipt to first observed text, reasoning, or tool generation event. Includes admission and routing wait; excludes final hop to client. Streaming only. |
| Upstream TTFT (`upstream_ttft`) | Upstream attempt dispatch to first observed generation event. Streaming only. |
| Full response lifetime (`duration`) | Gateway receipt until downstream body is exhausted or dropped (includes failures and cancellations). |
| Admission wait (`ingress`) | Time waiting for ingress count/byte admission (including cancellations while waiting). |
| Routing wait (`queue`) | Time selecting a node or waiting for node concurrency permits, summed across attempts. |
| TPOT / generation speed (`tpot`, `generation_rate`) | Provider mean ITL when reported; otherwise first-to-last generation time divided by `(output_tokens - 1)` for single completions (`generation_rate = 1 / tpot`). SSE chunks are never counted as tokens. |
| In-flight requests (`active`) | Started minus finalized requests (covers ingress wait through response delivery). |
| Occupied upstream slots (`upstream_active`) | Active node concurrency leases (excludes ingress wait). |
| Outcome ratios | `success`, `failed`, `cancelled`, `unknown` each divided by total finalized outcomes in the window. |
| Retries | Total retry attempts plus fraction of finalized logical requests that retried at least once. |
| Output usage coverage | Completed generation requests with reported output tokens divided by all completed generation requests. |
| Gateway cached-token ratio | Reported `cached_tokens / input_tokens` across requests reporting both. |
| Exact / approximate routing matches | Routing affinity decisions, independent of actual upstream KV hit rates. |

**Streaming outcomes**: HTTP `200` alone does not establish success. Recognized terminal events mark `success`, explicit stream errors mark `failed`, downstream drops mark `cancelled`, and unrecognized EOF marks `unknown`. Gateway/model scopes count logical requests once; node scopes count attempts (including failed attempts before retry), attributing final latency and usage to the final node.

### Engine Telemetry (vLLM) & Missing Data

The vLLM engine section reflects **all traffic processed by the vLLM engine**, including traffic bypassing Estuary. It aggregates per-series counter deltas before summing across nodes so counter resets (for example an engine restart) reset the baseline without spiking throughput. Fleet KV-cache usage is the maximum across fresh reporting nodes.

- `null` means unavailable, unsupported, stale, resetting, or not yet sampled (never fabricated as `0`).
- Valid idle observations report `0` and decay EWMA toward zero.
- Estuary does not inject `stream_options.include_usage` into native passthrough requests; clients or upstreams must report usage for usage-dependent metrics.

---

## Observability API

`GET /admin/api/observability` uses standard management authentication:

| Query parameter | Allowed values | Default |
| --- | --- | --- |
| `scope` | `fleet`, `node`, `model` | `fleet` |
| `id` | Node ID or public model name (required when `scope != fleet`, max 1024 bytes) | `""` |
| `window` | `60`, `300`, `900` (seconds) | `300` |

```bash
curl -H "Authorization: Bearer $ESTUARY_ADMIN_TOKEN" \
  'http://127.0.0.1:9090/admin/api/observability?scope=model&id=gateway-chat&window=300'
```

Invalid `scope` or `window` returns `400`. Unknown node/model IDs return `200` with `selected: null` and empty `history`.

---

## OpenMetrics `/metrics` Reference

`GET /metrics` exports a single `prometheus-client` OpenMetrics 1.0.0 registry (single `# TYPE` per family, terminating `# EOF`). All names carry the `estuary_` prefix; credentials, request IDs, and session IDs never appear as labels.

### Request, Attempt & Stream Counters

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `estuary_requests_total` | Counter | `endpoint`, `status` | Downstream responses created (streaming bodies may still be in progress). |
| `estuary_upstream_attempts_total` | Counter | `node`, `outcome` | Upstream attempts by node and outcome (`200`, `transport_error`, `header_timeout`, etc.). |
| `estuary_retries_total` | Counter | `node`, `outcome` | Retry attempts by failed node and reason (`connect_error`, HTTP status, `body_error`). |
| `estuary_tokenization_outcomes_total` | Counter | `outcome` | Exact routing tokenization decisions/results (`cache_hit`, `upstream_success`, `prefix_gate`, `directory_unavailable`, `deadline`, `upstream_error`, etc.). |
| `estuary_stream_cancellations_total` | Counter | `node` | Streaming bodies dropped before upstream EOF (usually client cancellation). |
| `estuary_stream_errors_total` | Counter | `node` | Errors reading upstream response bodies. |

### Node State, Workload & vLLM Gauges

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `estuary_node_active` | Gauge | `node` | Current in-flight leases on the node. |
| `estuary_node_pending_prefill_tokens` | Gauge | `node` | Estimated uncached prefill tokens held by local leases. |
| `estuary_node_pending_decode_tokens` | Gauge | `node` | Estimated decode tokens held by local leases. |
| `estuary_node_health` | Gauge | `node` | `0=starting`, `1=healthy`, `2=degraded`, `3=unhealthy`. |
| `estuary_node_accepting_requests` | Gauge | `node` | `1` if serving, healthy, provider-ready, and allowed by circuit breaker; else `0`. |
| `estuary_node_circuit_state` | Gauge | `node` | `0=closed`, `1=open`, `2=half_open`. |
| `estuary_node_provider_ready` | Gauge | `node` | `1` if provider compatibility permits routing; else `0`. |
| `estuary_node_provider_state` | Gauge | `node` | `0=generic`, `1=checking`, `2=ready`, `3=incompatible`. |
| `estuary_node_upstream_running` | Gauge | `node` | Latest fresh vLLM running-request count. |
| `estuary_node_upstream_waiting` | Gauge | `node` | Latest fresh vLLM waiting-request count. |
| `estuary_node_kv_cache_usage_ratio` | Gauge | `node` | Latest vLLM KV-cache utilization ratio (`0.0`–`1.0`). |
| `estuary_node_exact_kv_blocks` | Gauge | `node` | Blocks tracked in the node's exact KV directory. |
| `estuary_node_exact_kv_bytes` | Gauge | `node` | Accounted memory bytes of the node's exact KV directory. |
| `estuary_node_exact_kv_ready` | Gauge | `node` | `1` if the node's exact KV directory is authoritative; else `0`. |

### Queue, Connection & Buffer Gauges

| Metric | Type | Meaning |
| --- | --- | --- |
| `estuary_queue_requests` | Gauge | Requests currently waiting for an upstream concurrency permit. |
| `estuary_queue_bytes` | Gauge | KiB-rounded request bytes held by queued requests. |
| `estuary_admission_waiters` | Gauge | Requests waiting to enter ingress admission. |
| `estuary_public_connections` | Gauge | Active accepted public TCP connections. |
| `estuary_response_buffer_bytes` | Gauge | Bytes reserved by in-progress/completed non-streaming response buffers. |
| `estuary_response_buffer_waiters` | Gauge | Non-streaming responses waiting for global response buffer capacity. |

### Latency, Prefix & Token Histograms

| Metric | Type | Meaning |
| --- | --- | --- |
| `estuary_request_duration_seconds` | Histogram | Admission to response creation (includes full body for buffered responses). |
| `estuary_queue_duration_seconds` | Histogram | Time selecting or waiting for upstream capacity. |
| `estuary_tokenization_duration_seconds` | Histogram | Time spent deciding, caching, or requesting exact routing tokenization. |
| `estuary_prefix_match_chars` | Histogram | Selected approximate matched prefix length in canonical characters. |
| `estuary_prefix_match_tokens` | Histogram | Selected exact vLLM matched prefix length in tokens. |
| `estuary_upstream_first_token_duration_seconds` | Histogram | Upstream dispatch to first observed generation event. |
| `estuary_inference_input_tokens` | Histogram | Reported input tokens on completed requests. |
| `estuary_inference_cached_tokens` | Histogram | Reported cached input tokens on completed requests. |
| `estuary_inference_output_tokens` | Histogram | Reported output tokens on completed requests. |

### Scoped Gateway Counters

All carry labels `{scope, name}` where `scope` is `fleet`, `node`, or `model` (bounded by `ESTUARY_OBSERVABILITY_MAX_SCOPES`):

- `estuary_gateway_started_total`
- `estuary_gateway_success_total`
- `estuary_gateway_failed_total`
- `estuary_gateway_cancelled_total`
- `estuary_gateway_unknown_total`
- `estuary_gateway_retries_total`
- `estuary_gateway_input_tokens_total`
- `estuary_gateway_output_tokens_total`
- `estuary_gateway_cached_tokens_total`

### Session Logging Metrics

| Metric | Type | Meaning |
| --- | --- | --- |
| `estuary_session_log_enabled` | Gauge | `1` if session logging is configured; else `0`. |
| `estuary_session_log_available` | Gauge | `1` if the session log SQLite writer is healthy; else `0`. |
| `estuary_session_log_queued` | Gauge | Commands queued for background SQLite persistence. |
| `estuary_session_log_content_bytes` | Gauge | Captured payload bytes currently buffered in memory. |
| `estuary_session_log_dropped_total` | Counter | Rejected observations or discarded write commands due to backpressure/write failure. |
| `estuary_session_log_truncated_total` | Counter | Stage/attempt payloads truncated to respect byte budgets. |
| `estuary_session_log_write_errors_total` | Counter | SQLite write or recovery errors. |
| `estuary_session_log_committed_total` | Counter | Completed requests committed to SQLite. |
| `estuary_session_log_last_commit_timestamp_ms` | Gauge | Unix ms timestamp of the last committed batch. |

---

## Cardinality Bounds & Lifecycle

- **Scope cap**: Retains up to `ESTUARY_OBSERVABILITY_MAX_SCOPES` (default `32`, max `4096`) node scopes and model scopes plus `fleet`. Excess events increment `limited_events`; `fleet` totals always include every request.
- **Identifier & series caps**: Scope IDs are limited to 1,024 bytes. Each vLLM node tracks up to 512 relevant labelled counter series (exceeding that reports unavailable rates instead of a truncated sum).
- **Resets**: Process restart clears all in-memory telemetry and generates a new `instance_id`. Replacing a node configuration advances its node instance ID and resets its per-node observability series so previous in-flight completions cannot contaminate the replacement. Scrape `/metrics` into an external time-series database for long-term retention or cross-instance aggregation.
