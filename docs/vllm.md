# vLLM Provider

[Documentation index](README.md) | [Architecture](architecture.md) |
[Configuration and operations](operations.md)

Estuary's vLLM provider targets vLLM 0.25.0 and newer, including development
builds. The origin-root `/version` gate compares the numeric release prefix,
accepting versions such as `0.25`, `v0.25.0`, `0.25.0.dev123+gabcdef`, and
`0.25.0-rc1`. Recognizable releases below 0.25.0 are rejected, including builds
with suffixes. Nonempty labels without a comparable release, such as `dev`,
are accepted and reported unchanged; their compatibility depends on the
upstream implementing the interfaces below. Empty versions, malformed JSON,
and failed version probes still fail the check.

## Provider Interfaces

| Interface | Estuary use |
| --- | --- |
| `GET /version` | Version gate and reported provider version. |
| `GET /metrics` | Running, waiting, KV utilization, throughput, cache hit, and preemption telemetry. |
| `POST /tokenize` | Exact token sequence for supported Chat and Completions requests. |
| `POST /v1/chat/completions/render` | Final Chat prompt token IDs for cache routing, without generation. |
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

Client Messages and Responses requests are converted to Chat Completions before
routing. All models use `/v1/chat/completions`; no native Messages or Responses
backend is required.

## KV Events

Start vLLM with its ZMQ publisher and replay endpoint:

```bash
vllm serve MODEL \
  --kv-events-config \
  '{"enable_kv_cache_events":true,"publisher":"zmq","endpoint":"tcp://*:5557","replay_endpoint":"tcp://*:5558","topic":"kv-events","buffer_steps":10000}'
```

On vLLM 0.30+, enable the renderer endpoints with
`vllm serve MODEL --enable-scale-out` (alongside the KV-event configuration
above). The flag does not exist in 0.25–0.29; those versions register the Chat
render endpoint without it. Intermediate development builds may instead use
`VLLM_ENABLE_SCALE_OUT_ENDPOINTS`; consult that build's configuration.

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
least-loaded compatible tokenizer's process-local LRU is checked before sending
a request. Rendering and any legacy tokenization fallback stay on one node and
share one total deadline; failure or timeout falls back to approximate routing.

All client protocols share the same final Chat payload, tokenization gate,
bounded LRU, one-node request deadline, exact KV matching, scheduler scoring,
and approximate fallback. The token source is `/v1/chat/completions/render`
on vLLM 0.25 and newer. Render routes may require `--enable-scale-out` on
versions that gate them. A renderer must return a nonempty `token_ids` sequence.

When render access is unavailable, representable Chat requests can fall back to
`/tokenize`. Tool-choice, reasoning-effort, structured-output and truncation
options that cannot be reproduced by `/tokenize` disable that exact fallback.
Multimodal requests retain approximate routing: token IDs alone do not identify
multimodal cache keys. Tokens apply only to nodes with compatible preprocessing.

The exact directory is conservative:

- only local GPU block events are accepted;
- LoRA events and non-null `extra_keys` are ignored;
- text-only Messages, Responses, Chat Completions, and single string or
  pre-tokenized Completions can use exact matching when their token source is
  available; multimodal prompts retain approximate routing because token IDs
  alone do not describe multimodal cache keys;
- unsupported or failed tokenization uses character-prefix affinity;
- removals and clear events delete exact state instead of estimating eviction;
- an authoritative zero or partial match overrides character history for that
  worker, so an evicted prefix cannot regain credit through historical affinity;
- for multiple KV groups, the usable prefix is the minimum match across groups.

Each Estuary node should represent one addressable vLLM cache domain. If one
HTTP endpoint randomly dispatches to hidden data-parallel ranks, Estuary cannot
target the rank whose KV event it observed. Expose ranks separately when
rank-level locality is required.

Nodes sharing a public model must use tokenization-equivalent model revisions,
chat templates, and prompt-processing settings. Give incompatible pools
different public model names.

## Anthropic Messages

Client Messages requests use the shared protocol adapter and become generic Chat
requests before scheduling. Text, images, tool calls/results and thinking history
are preserved. Inline system reminders become user system-reminder messages in
place. Cache-control annotations do not affect the normalized prompt key.

Thinking enablement maps to `chat_template_kwargs.enable_thinking`; a requested
budget is passed as `chat_template_kwargs.thinking_budget`. Support and precise
budget behavior depend on the model's template. `max_tokens` remains the overall
output limit. Reasoning effort is preserved without DeepSeek-specific remapping.
No model-specific prompt encoding is applied by Estuary.

`messages/count_tokens` sends the same normalized Chat request to the Chat render
endpoint and counts returned token IDs. This requires render access. On vLLM
versions that gate render routes behind `--enable-scale-out`, enable that flag
when using token counting or exact rendered-prefix routing.

Tools require a tool-capable model and the appropriate vLLM tool parser options,
such as `--enable-auto-tool-choice --tool-call-parser PARSER`. Empty/default tool
declarations are omitted. Documents, server search, encrypted-only reasoning and
unsupported context edits fail with explicit client-protocol errors.

See [Local end-to-end tests](local-e2e.md) for CLI checks. Old database protocol settings are discarded when loaded.

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

Responses uses the same Chat backend for Codex and other clients. Namespace tools
are converted into safe Chat function names and restored in buffered and SSE
responses. Standard functions, the supported `apply_patch` custom tool, structured
output, image input, and plaintext full-history reasoning are adapted through the
shared library. Parallel tool calls are returned after their arguments complete.
The old namespace-flattening and Anthropic protocol settings no longer affect
backend selection. Server search and stateful provider features are unsupported.

## Per-request measurements

Start vLLM with `--enable-per-request-metrics` to report optional engine timings.
Estuary records the four duration fields in buffered responses and terminal SSE
`response.completed` events separately from its own network/body timers. Chat
Completions can report these metrics from 0.25; Responses adds them in 0.31.
Missing fields remain absent, including Responses flows where upstream built-in
tools span multiple engine generations and vLLM suppresses timing metrics.

Responses `usage.input_tokens_details.cache_write_tokens` is recorded alongside
cache reads, output and reasoning tokens. Estuary also retains support for Chat
`created_cache_tokens` and Anthropic `cache_creation_input_tokens`. These
observations require neither a version gate nor captured response content and
do not change protocol bytes. See [Session logging](session-logging.md) for
units, limits and attribution.

## Related Documentation

- [Request scheduling and prefix state](architecture.md)
- [Node settings, metrics, and security](operations.md)
- [Deployment and rolling updates](../deploy/README.md)
