# vLLM Provider

[Documentation Index](README.md) | [Configuration & Security](configuration.md) | [Architecture](architecture.md)

The `vllm` provider integrates Estuary with vLLM's version endpoint, Prometheus telemetry, prompt rendering/tokenization endpoints, ZMQ KV-cache event stream, and per-request engine metrics.

---

## Requirements & Version Gate

Estuary targets **vLLM 0.25.0 and newer**, including development and pre-release builds.

The monitor probes `GET /version` at the upstream origin and compares the numeric release prefix:

- Accepted: `0.25`, `v0.25.0`, `0.25.0.dev123+gabcdef`, `0.25.0-rc1`, and opaque non-empty dev labels without a numeric prefix (such as `dev`).
- Rejected: recognizable releases below `0.25.0` (even with suffixes), empty versions, malformed JSON, or failed version probes.

An incompatible node enters `provider_state = incompatible` and is excluded from routing until a valid version is observed.

---

## Provider Interfaces

All provider HTTP paths are resolved against the **upstream origin** (not the `/v1` base path) and must stay on the same origin without query strings or fragments.

| Interface | Estuary use |
| --- | --- |
| `GET /version` | Startup preflight and periodic compatibility recheck (every 30 monitor ticks). |
| `GET /metrics` | Scraped every `monitor_interval_ms` for load gauges and engine counters. |
| `POST /v1/chat/completions/render` | Primary token-ID source for exact KV routing and Anthropic `/v1/messages/count_tokens`. |
| `POST /tokenize` | Exact token-ID source for Completions and fallback for representable Chat requests when `/render` returns `403`/`404`/`405`/`501`. |
| ZMQ KV publisher (`SUB`) | Real-time block store, removal, and clear events. |
| ZMQ KV replay (`DEALER`) | Contiguous sequence recovery on startup, reconnect, or sequence gap. |

### Telemetry Signals Scraped from `/metrics`

Estuary parses scalar `vllm:*` / `vllm_*` samples:

- **Routing load gauges**: `num_requests_running` (summed across engine labels), `num_requests_waiting` (summed), and `kv_cache_usage_perc` (maximum across labels, clamped to `0.0`–`1.0`).
- **Observability counters**: `prompt_tokens`, `generation_tokens`, `request_success`, `prefix_cache_queries`, `prefix_cache_hits`, and `num_preemptions` (with or without `_total`).

Scheduling uses `observed_load = max(local_active, upstream_running + upstream_waiting)`. When fresh `upstream_waiting >= provider.waiting_threshold`, the node is temporarily removed from admission until telemetry reports recovery.

---

## Configuration

Select `provider.type = "vllm"` on the node. Default provider settings:

```json
{
  "type": "vllm",
  "version_path": "/version",
  "metrics_path": "/metrics",
  "tokenize_path": "/tokenize",
  "monitor_interval_ms": 1000,
  "request_timeout_ms": 2000,
  "telemetry_stale_ms": 5000,
  "waiting_threshold": 8,
  "tokenize_cache_entries": 4096,
  "kv_events": null
}
```

Validation rules:

- `monitor_interval_ms >= 100`
- `telemetry_stale_ms >= monitor_interval_ms`
- `1 <= tokenize_cache_entries <= 65536`
- `request_timeout_ms >= 1`, `waiting_threshold >= 1`

---

## ZMQ KV Events

### Starting vLLM

Start vLLM with its ZMQ KV-event publisher and replay endpoint:

```bash
vllm serve MODEL \
  --kv-events-config \
  '{"enable_kv_cache_events":true,"publisher":"zmq","endpoint":"tcp://*:5557","replay_endpoint":"tcp://*:5558","topic":"kv-events","buffer_steps":10000}'
```

- **vLLM 0.30+**: Also pass `--enable-scale-out` to expose `/v1/chat/completions/render` (needed for exact rendered-prefix routing and `/v1/messages/count_tokens`). Versions `0.25`–`0.29` expose the render route without this flag.
- **Tool calling**: Enable a tool-capable model with the appropriate vLLM parser flags (for example `--enable-auto-tool-choice --tool-call-parser PARSER`).
- **Per-request timings**: Pass `--enable-per-request-metrics` to emit engine duration fields.

### Configuring `provider.kv_events` in Estuary

vLLM accepts wildcard bind addresses (`tcp://*:5557`), whereas Estuary requires concrete connectable `tcp://host:port` endpoints:

```json
{
  "endpoint": "tcp://vllm-0.internal:5557",
  "replay_endpoint": "tcp://vllm-0.internal:5558",
  "topic": "kv-events",
  "reconnect_ms": 1000,
  "max_blocks": 1000000,
  "max_directory_bytes": 536870912,
  "max_event_bytes": 16777216
}
```

> [!WARNING]
> ZMQ KV-event and replay ports have no application-level authentication or encryption. Keep both ports on a private network.

### Synchronization & Invalidation Rules

- Estuary subscribes to `endpoint` **before** requesting replay from `replay_endpoint`, ensuring live events remain queued during catch-up.
- Both vLLM 0.25 replay frames `(sequence, payload)` and vLLM 0.26+ frames `(topic, sequence, payload)` are accepted.
- Replay must be contiguous. Disconnects, sequence gaps without replay recovery, sequence rollbacks, replay overflows, malformed MessagePack, conflicting block hashes, node unhealthiness, or `max_blocks` / `max_directory_bytes` limit violations immediately invalidate exact authority for that node.
- While exact authority is suspended or invalid, Estuary falls back smoothly to approximate character-prefix affinity until replay or `AllBlocksCleared` re-establishes a trustworthy baseline.

---

## Exact KV-Block Routing

Because Messages, Responses, and Chat requests are all converted into the final Chat payload before routing, every client protocol shares the exact same tokenization and KV-matching pipeline:

1. **Gating**: Remote tokenization runs only when `ESTUARY_PREFIX_ENABLED=true`, the approximate prefix ratio exceeds `ESTUARY_PREFIX_CACHE_THRESHOLD`, and at least one routable vLLM node for that model has an authoritative exact directory with `blocks > 0`. Pre-tokenized Completions (`prompt` as an integer token array) bypass this gate. Requests with non-null `cache_salt` or media blocks skip exact tokenization.
2. **Shared pool cache & singleflight**: `VllmManager` maintains a shared bounded LRU tokenization cache (bounded by the highest configured `tokenize_cache_entries` across nodes and a 128 MiB byte cap), keyed by endpoint, public model, provider generation, and payload. Concurrent cache misses for the same key coalesce via singleflight so bursts do not stampede upstream `/render`.
3. **Single-node deadline & fallback**: A cache miss queries the least-loaded ready vLLM node under one total `request_timeout_ms` deadline. Chat payloads call `/v1/chat/completions/render` (`return_mm_kwargs: false`); if that returns `403`/`404`/`405`/`501`, representable prompts (those without `tool_choice`, `parallel_tool_calls`, `reasoning_effort`, `response_format`, `structured_outputs`, `truncate_prompt_tokens`, or `documents`) fall back to `tokenize_path` on the same node. Any failure or timeout falls back to approximate routing.
4. **Conservative exact directory**:
   - Accepts only local GPU block events.
   - Ignores LoRA events and non-null `extra_keys`.
   - Multimodal prompts remain on approximate routing because token IDs alone do not identify multimodal KV keys.
   - Removals and clears delete exact blocks rather than guessing eviction.
   - An authoritative exact match (including `0`) overrides approximate character history for that worker, so evicted prefixes do not retain stale affinity credit.
   - Across multiple KV groups on a node, the usable matched prefix is the minimum match across groups.

---

## Topology & Pool Constraints

- **One node per cache domain**: Each configured Estuary node should map to one addressable vLLM cache domain. If a single HTTP endpoint load-balances across hidden data-parallel ranks behind a proxy, Estuary cannot direct requests to the specific rank whose KV events it received. Expose ranks individually when rank-level locality is required.
- **Tokenization equivalence**: All vLLM nodes mapped under the same public model name must share equivalent model weights/revisions, tokenizers, chat templates, and prompt-processing settings. Place incompatible pools under distinct public model names.

---

## Per-Request Engine Measurements

When vLLM is started with `--enable-per-request-metrics`, Estuary observes optional per-request duration fields on buffered responses and terminal SSE events (within a 64 KiB observation bound) without altering forwarded bytes:

- `engine_time_to_first_token`
- `engine_generation_time`
- `engine_queue_time`
- `engine_mean_itl`

These are recorded per upstream attempt in [Session Logging](session-logging.md#timings-usage-and-outcomes) (converted from `_ms` to microseconds in `timings_us`) separately from gateway network timers, and `engine_mean_itl` feeds per-request TPOT in [Observability](observability.md). Estuary also records cache-write token counters (`created_cache_tokens`, `cache_creation_input_tokens`, and Responses `input_tokens_details.cache_write_tokens`) alongside cache reads and reasoning tokens.
