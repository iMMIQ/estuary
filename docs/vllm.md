# Native vLLM Provider

[Documentation index](README.md) | [Architecture](architecture.md) |
[Configuration and operations](operations.md)

Estuary's native provider supports vLLM 0.25.0 and newer. A vLLM node remains
outside the routing set until its origin-root `/version` response passes this
fixed compatibility gate.

## Provider Interfaces

| Interface | Estuary use |
| --- | --- |
| `GET /version` | Version gate and reported provider version. |
| `GET /metrics` | Running, waiting, KV utilization, throughput, cache hit, and preemption telemetry. |
| `POST /tokenize` | Exact token sequence for supported Chat and Completions requests. |
| ZMQ KV publisher | Block store, removal, and clear events. |
| ZMQ replay router | Contiguous recovery after startup, disconnect, or sequence gap. |

The routing load signals are:

- `vllm:num_requests_running`;
- `vllm:num_requests_waiting`;
- `vllm:kv_cache_usage_perc`.

Estuary sums running and waiting samples across engine labels and uses the
largest KV-use ratio. Scheduling observes
`max(local_active, upstream_running + upstream_waiting)`, while the node's
`max_concurrency` remains the local hard limit. Fresh waiting depth at or above
`provider.waiting_threshold` temporarily removes the node from admission.

## Configure vLLM

Choose provider type `vllm` in the management application. The HTTP paths are
resolved against the upstream origin, not its `/v1` base path. Default provider
settings are equivalent to:

```json
{
  "type": "vllm",
  "anthropic_protocol": "auto",
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

`auto` selects vLLM's native Anthropic Messages endpoint. Set `responses` or
`chat` only when that conversion is required by the selected server.

## KV Events

Start vLLM with its ZMQ publisher and replay endpoint:

```bash
vllm serve MODEL \
  --kv-events-config \
  '{"enable_kv_cache_events":true,"publisher":"zmq","endpoint":"tcp://*:5557","replay_endpoint":"tcp://*:5558","topic":"kv-events","buffer_steps":10000}'
```

The vLLM arguments are bind addresses. Estuary requires concrete addresses that
it can connect to:

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

The transport has no application-level authentication or encryption. Keep both
ports on a private network.

Estuary accepts the vLLM 0.25 replay frame `(sequence, payload)` and the 0.26+
frame `(topic, sequence, payload)`. It subscribes before requesting replay so
new events remain queued during recovery. Replay must be contiguous.
Disconnects, gaps, sequence rollback, replay overflow, malformed MessagePack,
conflicting hashes, and memory-limit violations invalidate exact state. Routing
then uses approximate prefix affinity until replay or `AllBlocksCleared`
establishes a trustworthy baseline.

`max_blocks` limits stored hashes. `max_directory_bytes` separately accounts for
token edges, block nodes, child references, and hash keys. A limit violation
clears authority rather than leaving a partially trusted directory active.

## Exact Routing

Remote tokenization is attempted only when the approximate match already exceeds
`routing.prefix.cache_threshold` and at least one authoritative exact directory
contains blocks. Pre-tokenized Completions do not need this initial gate. The
least-loaded tokenizer's process-local LRU is checked before one `/tokenize`
request is sent; failure or timeout falls back immediately to approximate
routing.

The exact directory is conservative:

- only local GPU block events are accepted;
- LoRA events and non-null `extra_keys` are ignored;
- Chat Completions and single string or pre-tokenized Completions can use exact
  matching;
- unsupported or failed tokenization uses character-prefix affinity;
- removals and clear events delete learned state instead of estimating eviction;
- for multiple KV groups, the usable prefix is the minimum match across groups.

Each Estuary node should represent one addressable vLLM cache domain. If one
HTTP endpoint randomly dispatches to hidden data-parallel ranks, Estuary cannot
target the rank whose KV event it observed. Expose ranks separately when
rank-level locality is required.

Nodes sharing a public model must use tokenization-equivalent model revisions,
chat templates, and prompt-processing settings. Give incompatible pools
different public model names.

## Anthropic Messages

vLLM 0.25+ exposes native `/v1/messages` and `/v1/messages/count_tokens` routes.
Estuary removes Claude Code's standalone billing marker, removes its no-op
`clear_thinking` edit, rewrites the model alias, and maps thinking enablement to
`chat_template_kwargs.enable_thinking`.

The vLLM 0.25 request model does not expose an exact thinking-only token budget.
Estuary preserves `budget_tokens`, uses `max_tokens` as the total output ceiling,
and adds `x-estuary-thinking-budget: approximated-by-max-tokens`. Generated
thinking is retained so Claude Code can carry it into the next turn. Unsupported
context edits and file-download requests return explicit Anthropic errors.

`messages/count_tokens` requires a node using native Messages. The Responses and
Chat adapters cannot provide this native token count.

## Codex Responses

Codex should use the full Responses request shape and disable web search:

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

For Codex requests selected onto vLLM, Estuary collision-checks and flattens
namespace tools, then restores namespace and name fields in buffered and SSE
responses. Standard functions, structured output, image input, full-history
replay, and `prompt_cache_key` retain their Responses shapes.

Responses Lite custom calls, tool-search items, `additional_tools`, and web
search are rejected because vLLM's Harmony path cannot represent them. These
checks apply only to detected Codex requests routed to vLLM; other Responses
traffic uses the normal pass-through path.

## Related Documentation

- [Request scheduling and prefix state](architecture.md)
- [Node settings, metrics, and security](operations.md)
- [Deployment and rolling updates](../deploy/README.md)
