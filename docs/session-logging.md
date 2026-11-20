# Session Logging

[Documentation Index](README.md) | [Configuration & Security](configuration.md) | [API & Clients](api.md)

Session logging captures inference request metadata, upstream attempts, timings, and optionally deduplicated request/response payloads in a **separate local SQLite database**. It is disabled by default.

```bash
estuary \
  --database ./data/estuary.db \
  --session-log-database ./data/sessions.db
```

- The session log path **must not** point to the configuration database, and the logger refuses non-empty unrecognized SQLite files.
- Newly created database files use Unix permissions `0600`.
- Keep the database, `-wal`, and `-shm` files on a local filesystem (never NFS) and back up using SQLite's online backup API rather than copying only the main file.

---

## Configuration

| Environment variable | CLI flag | Default | Validated range / Meaning |
| --- | --- | --- | --- |
| `ESTUARY_SESSION_LOG_DATABASE` | `--session-log-database` | unset | Path to the separate SQLite log database. Setting this enables logging. |
| `ESTUARY_SESSION_LOG_CAPTURE_CONTENT` | `--session-log-capture-content` | `true` | Set `false` to store metadata, timings, and usage without payload bodies. |
| `ESTUARY_SESSION_LOG_QUEUE_CAPACITY` | `--session-log-queue-capacity` | `4096` | In-memory command queue capacity (`2`–`65536`), reserving permits for finalization. |
| `ESTUARY_SESSION_LOG_MAX_CONTENT_BYTES` | `--session-log-max-content-bytes` | `67108864` (64 MiB) | Global in-memory captured payload budget (`max_payload_bytes`–`1 GiB`). |
| `ESTUARY_SESSION_LOG_MAX_PAYLOAD_BYTES` | `--session-log-max-payload-bytes` | `2097152` (2 MiB) | Maximum captured bytes per stage and attempt (`1 B`–`16 MiB`). |
| `ESTUARY_SESSION_LOG_RETENTION_DAYS` | `--session-log-retention-days` | `30` | Retention for requests, attempts, and events (`>= 1`). |
| `ESTUARY_SESSION_LOG_CONTENT_RETENTION_DAYS` | `--session-log-content-retention-days` | `7` | Retention for captured payload roots (`1`–`retention_days`). |
| `ESTUARY_SESSION_LOG_FLUSH_INTERVAL_MS` | `--session-log-flush-interval-ms` | `100` | Maximum batch flush interval when idle (`1`–`60000`). |

---

## Requests, Attempts & Sessions

- Every `POST /v1/*` request receives a unique internal UUIDv7; reused client `x-request-id` values never collapse distinct records.
- Upstream retries are stored as numbered `attempts` on the parent request with node ID, node instance, provider, endpoint, upstream model, adapter mode, HTTP status, retry reason, route score, prefix match, and load snapshot. Request-level usage reflects the final attempt; inspect per-attempt usage to see total upstream work across retries.
- **Explicit session grouping**: Pass header `x-estuary-session-id` (up to 128 bytes) or JSON `metadata.session_id` (header takes precedence). Requests without an explicit ID remain ungrouped; Estuary does not infer sessions or conversation branches from prompt similarity.
- **Inspection**: Browse via the **Session logs** view in `/admin/` or query `GET /admin/api/logs/{status,requests,requests/{id},sessions}` documented in [API & Clients](api.md#session-log-queries). Captured content is protected by management authentication and never exposed on the public listener.

---

## Storage, Deduplication & Retention

Seven SQLite tables (`sessions`, `requests`, `attempts`, `request_events`, `payloads`, `content_blobs`, `sequence_nodes`) separate queryable metadata from deduplicated content:

- **Content-addressed DAG**: JSON payloads are stored as typed manifests referencing content-addressed compressed blobs, and JSON arrays share immutable prefix `sequence_nodes`. Multi-turn conversations reuse older messages and array prefixes across turns, and client/upstream converted payloads share unchanged leaves.
- **Non-streaming vs SSE capture**: Valid non-streaming JSON is stored normalized (`json`); invalid or truncated bodies become bounded `partial_text`. SSE streams are summarized into one payload per stage (`stream_summary` — text, reasoning, tool arguments, usage, terminal marker, and final Responses object when present) and marked `summary` or `partial` rather than storing raw wire bytes or one row per token.
- **Reference-counted retention**: Maintenance runs once per minute in bounded passes (up to 1,000 expired requests, payloads, and orphaned sessions per pass), removing expired payload roots and garbage-collecting unreferenced blobs and sequence nodes while preserving prefixes still referenced by newer requests. Deletion marks SQLite pages reusable rather than immediately shrinking the file or performing secure erasure.

---

## Timings, Usage, and Outcomes

- **Gateway timers (`timings_us`)**: Record `ingress_wait`, `body_read`, `tokenization`, `headers_ready`, `total`, and per-attempt `scheduler_wait`, `headers`, first chunk/output/visible text, upstream completion, and downstream backpressure wait. These are gateway-side observations affected by network and stream backpressure.
- **vLLM engine timers**: When vLLM reports per-request `metrics` (`--enable-per-request-metrics`), each attempt additionally records `engine_time_to_first_token`, `engine_generation_time`, `engine_queue_time`, and `engine_mean_itl` in `timings_us` (converted from milliseconds to microseconds, bounded to 64 KiB observation, active even in metadata-only mode).
- **Token usage**: Missing usage stays `null`. Provider usage preserves cache-read, cache-write (`created_cache_tokens`, `cache_creation_input_tokens`, Responses `input_tokens_details.cache_write_tokens`), and reasoning counts. Anthropic client input totals sum reported uncached, cache-read, and cache-creation tokens; Responses input totals already include cached tokens. Metadata retains a redacted raw usage preview up to 4 KiB.
- **Semantic outcomes & crash recovery**: HTTP status and semantic outcome are tracked separately (an HTTP `200` SSE stream can end with `error`, `cancelled`, or `unknown`). On Linux, crash recovery inspects host boot ID, PID, and process start time so overlapping live workers are not falsely marked interrupted.

---

## Redaction & Privacy

Before hashing or persisting payloads and metadata, Estuary omits `Authorization`/`Cookie` headers and redacts common credential keys (`authorization`, `cookie`, `api_key`, `apikey`, `access_token`, `password`, `secret`, `refresh_token`) as well as embedded `sk-`, `sk_`, and `Bearer ` token patterns.

> [!WARNING]
> Automatic redaction is a bounded safety net, not a comprehensive DLP/secret scanner. Prompts, source code, tool outputs, and opaque tokens can still contain sensitive data. Set `ESTUARY_SESSION_LOG_CAPTURE_CONTENT=false` when storing request/response bodies is unacceptable.

---

## Failure Isolation, Durability & Query Limits

- **Non-blocking data path**: Inference tasks never await SQLite. A dedicated writer thread batches commits at 100 commands, 1 MiB of captured content, or `flush_interval_ms`.
- **Contention & fallback**: SQLite uses WAL mode, `synchronous=NORMAL`, and a 250 ms busy timeout. On transient `SQLITE_BUSY` / `SQLITE_LOCKED` contention across overlapping local workers, the writer retries up to 3 times with short exponential backoff (`10ms`, `20ms`, `40ms`). If content storage still fails, it attempts a metadata-only fallback with `capture_state = "storage_error"`. Capture or storage errors never fail inference requests.
- **Best-effort durability**: Exhausting queue capacity drops log records (counted in `estuary_session_log_dropped_total`); exceeding byte budgets marks payloads `partial` (`estuary_session_log_truncated_total`). Process crashes or power loss can lose queued or recent `NORMAL`-mode commits — session logging is diagnostic telemetry, not an audit log.
- **Read query budgets**: Management queries use independent read-only connections capped at 4 concurrent queries per process, a 2-second SQLite execution limit, and a 2-second / 32 MiB / 100,000-node DAG reconstruction limit. Queries exceeding these limits return `503 Service Unavailable`.
- **Health metrics**: Monitor `estuary_session_log_*` on `GET /metrics` (see [Observability & Metrics](observability.md#session-logging-metrics)).
