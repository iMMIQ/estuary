# Session logging

[Documentation index](README.md) | [Research and design](session-logging-design.md)

Session logging stores inference requests in a **separate local SQLite database**.
It is disabled until a database path is configured. Both the direct worker and
supervisor accept the same runtime options:

```bash
estuary --database ./data/estuary.db \
  --session-log-database ./data/sessions.db
```

Use `ESTUARY_SESSION_LOG_DATABASE` for environment configuration. The path must
not identify the configuration database. The logger also refuses a nonempty
unrecognized database. Newly created database files have mode `0600` on Unix;
keep the database, WAL and SHM files on a local filesystem and back them up
with SQLite's backup API rather than copying only the main file.

## Configuration

| Flag (`ESTUARY_` environment suffix in capitals) | Default | Meaning |
| --- | --- | --- |
| `--session-log-database` | unset | Enables logging at this path. |
| `--session-log-capture-content` | `true` | Use `false` to record metadata and usage without content. |
| `--session-log-queue-capacity` | `4096` | Command capacity, including reserved finalization permits. |
| `--session-log-max-content-bytes` | `67108864` | Global captured-content byte budget per worker. |
| `--session-log-max-payload-bytes` | `2097152` | Maximum captured bytes per stage and attempt. |
| `--session-log-retention-days` | `30` | Request/attempt/event retention. |
| `--session-log-content-retention-days` | `7` | Content-reference retention; must not exceed metadata retention. |
| `--session-log-flush-interval-ms` | `100` | Maximum scheduled batching interval when the writer is idle. |

For example, `ESTUARY_SESSION_LOG_CAPTURE_CONTENT=false` selects metadata-only
logging. These are process startup settings, not mutable node settings. Restart
or roll out compatible workers to change them. A rollback to a binary predating
this feature requires removing enabled session logging from its worker settings.
Disabled logging is omitted from serialized settings for old-worker compatibility.

## Requests and sessions

Each incoming POST under `/v1/` gets an independent internal UUID. Reused
`x-request-id` values never collapse two requests. Upstream retries are recorded
as attempts on that request, with the node instance, provider, endpoint, model,
adapter, status, retry reason, route score, prefix match and load snapshot.
Request usage describes the selected final attempt; use attempt usage when
examining total known upstream work.

Set `x-estuary-session-id` (up to 128 bytes), or `metadata.session_id` in JSON,
to explicitly group requests. The header takes precedence. Requests without
an explicit identifier remain ungrouped. This release does not infer agent
sessions, turn ancestry, branches, or tool execution from similar prompts.
The gateway observes tool calls and tool results present in request/response
content; it cannot measure execution inside the client.

The management application's **Session logs** page lists requests with a time
and session filter and cursor pagination. Details show timings, attempts, sparse
events, and separately expandable client/upstream inputs and outputs.

Protected management endpoints:

| Endpoint | Response |
| --- | --- |
| `GET /admin/api/logs/status` | In-memory logger health and loss counters; does not query SQLite. |
| `GET /admin/api/logs/requests?since=...&session=...&cursor=...&limit=...` | Metadata only; default last seven days, default 50 rows, maximum 100. |
| `GET /admin/api/logs/requests/{internal_uuid}` | Request, attempts, events, captured payloads. |
| `GET /admin/api/logs/sessions?since=...` | Most recent 100 explicit sessions within the time range. |

`since` uses Unix milliseconds. A next cursor is opaque to callers. Reads use
independent readonly connections, at most four concurrent queries per worker,
a two-second SQLite execution budget, and a two-second / 32 MiB / 100,000-node reconstruction
budget. Expensive or unavailable reads return `503`; an invalid filter returns
`400`, and a missing request returns `404`. Existing management authorization
protects both metadata and content. Content is never exposed on the public API.

## Storage and deduplication

Seven tables separate sessions, requests, attempts, request events, payload
roots, content blobs and persistent sequence nodes. Queryable identity/time/
result columns accompany bounded diagnostic JSON. JSON values are stored as
explicitly typed manifests and content-addressed leaves; arrays share immutable
prefix nodes. A growing message/input history reuses old content and array
prefixes. A bounded transaction-local cache avoids repeated SQL and compression
within a batch without retaining stale hashes across concurrent garbage collection.
Model mapping and client/upstream conversion can share unchanged
leaves without storing a second full prompt. Unknown JSON fields retain their
meaning after reconstruction, subject to capture limits and redaction.

Non-streaming bodies are normalized JSON; invalid or truncated JSON becomes a
bounded text fragment. SSE is summarized into text, reasoning, tool arguments,
usage and terminal observations; a Responses completion can retain its final
response object. One stream produces one payload per stage, not a row per token.
This release does not retain exact wire bytes, all unknown SSE events, or a
cryptographically verifiable replay. SSE payloads are explicitly marked `summary`
or `partial`, never exact raw captures.

Content expiry removes payload roots first. Reference-counted, transactional
GC releases only unreachable blobs/nodes, preserving prefixes referenced by
newer requests. Cleanup runs once per minute in bounded passes, so large backlogs
may need several passes. Deletion makes database pages reusable; it does not
immediately shrink the SQLite file or guarantee physical secure erasure.

## Timing, failures and privacy

Timers distinguish admission, body read, tokenization, scheduler wait, response
headers, first output, first visible text, upstream completion and total observed
body lifetime. Attempt timings also record first chunk and cumulative downstream
capacity wait. They are gateway observations; streaming backpressure can affect
measurements. They do not measure GPU kernel time or client receipt/processing.
Missing token usage stays null. Provider usage includes cache read/write and
reasoning counts when available; Anthropic input totals add reported uncached,
cache-read and cache-creation tokens. Captured content preserves redacted raw
usage; metadata retains a raw usage preview up to 4 KiB, with larger values explicitly omitted; metadata-only mode uses the bounded provider observer.

HTTP status and semantic outcome are separate. A stream can have HTTP `200`
and still report an error or missing terminal marker. An oversized event that
exceeds the bounded observer records an unknown result instead of claiming a
complete observation. Body drops record cancellation separately. Crash recovery
on Linux checks host boot identity, PID and process start time; it does not mark
still-running overlapping workers interrupted. On other platforms, only clean
writer shutdown marks its own unfinished requests; crash liveness stays unknown.
The `streaming` flag remains true when streaming was requested, including errors
returned as JSON; an observed SSE response also sets the flag.
Final upstream HTTP errors use the `upstream` phase and `upstream_status` class.
A failed attempt followed by a successful retry does not mark the request failed.

Capture omits authorization/cookie headers, redacts common credential fields and
embedded `sk-`, `sk_`, and `Bearer` token formats before hashing/storage, and
limits payload bytes. This is a limited automatic policy, not a comprehensive
secret detector: arbitrary source code, tool output, opaque strings and other
personal content can still be sensitive. Use metadata-only mode when content
retention is unsuitable.

## Failure isolation and durability

Request capture never awaits SQLite. A dedicated writer batches at 100 commands,
1 MiB of captured content, or the configured interval. Finalization reserves a
queue permit when accepting a log observation. Content budgets cause explicit
partial capture; unavailable queue capacity causes counted log loss. Capture,
compression, SQL and GC failures do not fail inference requests. A failed content
transaction attempts a metadata-only fallback with `capture_state=storage_error`.

SQLite uses WAL, `synchronous=NORMAL`, a 250 ms busy timeout, schema/application
identification, and short write transactions. Overlapping workers can share the
local log database. The background writer retries transient write-lock failures
up to four attempts (about one second of SQLite busy waits) before its normal
metadata fallback; it does not block inference tasks. Shutdown flush runs within
the existing remaining drain budget. Logging is best effort: process termination
can lose queued records and power loss can lose recent NORMAL-mode commits. It is not an audit guarantee.
No durable spool, external collector, multi-tenant namespaces, inference-based
session grouping, CDC, full-text search, deletion UI or offline agent-loop analysis
is implemented in this first release.

`/metrics` exposes `estuary_session_log_*` availability, queue, captured bytes,
dropped/truncated/write-error counters, committed records and last commit time.
Request IDs, session IDs and body text never appear as metric labels. Counters
are per-process and reset on restart; they are not totals for every stored row.
`dropped` counts rejected observations or discarded write commands, not unique
requests; `truncated` counts stage/attempt payloads rather than whole requests.

## Verification

Coverage includes real HTTP requests through the gateway for growing history,
reused request IDs, retries, stream usage, missing terminal markers, client drops,
DeepSeek's Responses and Messages recipes, partial content and protected admin
reads. Storage tests cover branches, compaction, unknown JSON fields, sensitive
fields in partial JSON, reference-counted retention, saturated capture budgets,
recovery and overlapping writers. Desktop/mobile browser tests exercise the
session page, filtering, pagination and content expansion.

The opt-in 1,000-turn storage fixture is reproducible with:

```bash
cargo test --lib thousand_turn_history -- --ignored --nocapture
```

One local debug-profile run reconstructed the final input, stored 1,001 sequence
nodes and 435,687 bytes of compressed blobs for 37,470,495 bytes of cumulative
input JSON. These figures exclude request metadata, sequence row/index storage,
SQLite page and WAL overhead; this fixture measures content sharing and
transaction-local encoding, not production request latency or model throughput.
Gateway latency/CPU/RSS still need deployment-specific load measurements.
