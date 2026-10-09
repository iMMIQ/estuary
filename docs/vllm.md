# Native vLLM Provider

[Documentation index](README.md) | [Architecture](architecture.md) |
[Configuration and operations](operations.md)

Estuary's native provider targets vLLM 0.25.0 and newer, including development
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

vLLM 0.25+ exposes native `/v1/messages` and `/v1/messages/count_tokens` routes.
Estuary removes Claude Code's standalone billing marker, removes its no-op
`clear_thinking` edit, rewrites the model alias, and maps thinking enablement to
`chat_template_kwargs.enable_thinking`.
The Chat and Responses adapters also preserve Claude Code's inline text-only
`role: system` messages, including environment context, in their original order.

The vLLM 0.25 request model does not expose an exact thinking-only token budget.
Estuary preserves `budget_tokens`, uses `max_tokens` as the total output ceiling,
and adds `x-estuary-thinking-budget: approximated-by-max-tokens`. Generated
thinking is retained so Claude Code can carry it into the next turn. Unsupported
context edits and file-download requests return explicit Anthropic errors.

`messages/count_tokens` requires a node using native Messages. The Responses and
Chat adapters cannot provide this native token count.

Claude Code tool calls require a tool-capable model and vLLM's
`--enable-auto-tool-choice --tool-call-parser PARSER` options (for example,
`hermes` for Qwen2.5). Empty `tools: []` lists with default/automatic tool choice
are omitted in native requests and converted payloads, so text-only calls do not
require a tool parser.

An opt-in real Claude Code/CPU test runs against an already started local server:

See [Local end-to-end tests](local-e2e.md) for prebuilt llama.cpp and Ollama
alternatives when local vLLM kernels are unavailable.

```bash
cargo build --locked
python3 tests/claude_vllm_cpu_e2e.py \
  --upstream http://127.0.0.1:18000/v1 --model qwen-cpu
```

The test uses isolated CLI settings and fresh control/log databases under
`target/`, checks native and Chat conversion, reads a random JSON nonce through
a local stdio MCP tool executed by Claude Code, and verifies logging and released
reservations. Use `--tool-mode read` for its built-in `Read` tool; small models
can generate invalid optional arguments or return a tool call as plain text.
Use a sufficient context window for the CLI's environment and tool definitions;
the test replaces the main system prompt to keep CPU inference practical.
Use `--protocols native chat responses` to also verify the explicit rejection of
Claude Code's `output_config.effort` in the Responses adapter. Its semantics
cannot be represented losslessly; use native or Chat for current CLI requests. On
the local AVX2 CPU with vLLM `0.31.0+cpu`, the default V2 runner stalled on the
first Qwen2.5 inference; `VLLM_USE_V2_MODEL_RUNNER=0` and two CPU threads allowed
this test to complete. This is a tested local workaround, not a compatibility
requirement for every vLLM version or CPU.

Qwen3.5's smallest official model is `0.8B` (`0.6B` belongs to Qwen3).
Its tool parser is `qwen3_coder`; use the model's official chat template if a
quantized repository omits it. A local test of Qwen3.5-0.8B AWQ INT4 with
vLLM `0.31.0+cpu` loaded successfully but crashed on its first inference because
the AVX2 extension does not register `cpu_gemm_wna16`. Thus a successful health
check does not establish INT4 inference support on an AVX2-only CPU. The real
Claude test must pass before treating that model/backend combination as working.

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
