# DeepSeek protocol conversion

Estuary uses [deepseek-recipe](https://github.com/deepseek-ai/deepseek-recipe)
0.1 for models explicitly configured as `deepseek`.

In the node editor, select **DeepSeek (recipe)** in the model mapping's **Model
family** field. The equivalent node configuration is:

```json
{
  "models": { "coding": "deepseek-chat", "general": "another-model" },
  "model_capabilities": {
    "coding": { "multimodal": false, "family": "deepseek" },
    "general": { "multimodal": true, "family": "generic" }
  }
}
```

This is a fragment of a node configuration. Model families are per public model,
not per node. Missing family settings default to `generic`; the gateway does not
infer families from public aliases or upstream model names. A `*` capability is
used only when there is no exact capability entry for the public model.

## Request flow

For a DeepSeek model, both `POST /v1/responses` (Codex) and `POST /v1/messages`
(Claude Code) use this path:

1. Recipe validates and converts the request to its shared `Conversation`.
2. Estuary adapts that conversation to an upstream `/v1/chat/completions` request.
3. Estuary translates the upstream's text, `reasoning_content`, tool calls, and
   usage to recipe output events.
4. Recipe generates the original client protocol's complete JSON or SSE response.

This path overrides the node's Anthropic protocol setting for generation requests
for that model. `messages/count_tokens` keeps the existing native-only behavior.
The incoming URL selects the response protocol; User-Agent and Anthropic headers
do not switch a Responses request to Messages. Upstream Anthropic error envelopes
are normalized to OpenAI errors for OpenAI clients, preserving the HTTP status,
error message, and `Retry-After` header.
Chat Completions requests themselves keep their normal forwarding behavior.
Generic models retain the existing protocol settings and Responses forwarding.
There is no V4/V4.1 prompt template, tokenizer download, raw Completions transport,
or model inference inside the gateway.

## Compatibility

- The upstream must implement Chat Completions, including streaming tools and
  `reasoning_content` when thinking is requested. Generation settings are subject
  to upstream support; an exact thinking token budget is not enforced by recipe.
- Function tools, namespace tools, tool-result history, and the Responses
  `apply_patch` custom tool use recipe's normalization. Wire function names are
  mapped back to their original names/namespaces in client responses.
- Image references are forwarded as Chat Completions image content, without
  downloading or preprocessing images inside the gateway. Model image capability
  settings still apply.
- Streaming tool arguments are assembled and validated before emitting tool calls,
  allowing interleaved parallel calls without mixing argument fragments. Text and
  reasoning are streamed as received. Adapter input is limited to 16 MiB per
  response, in addition to the gateway's normal buffering and timeout limits.
- Hosted tools (including web search), encrypted reasoning, stateful Responses,
  and unsupported custom tools are rejected. JSON Schema output constraints are
  rejected rather than silently discarded. See the upstream library for the full
  supported request subset.
- Exact vLLM cache token routing is skipped for these adapted models because it
  would otherwise tokenize the original request with a different protocol adapter.
  Approximate prefix routing, scheduling, retries, and stream backpressure remain.

Source builds use Rust 1.99, pinned in `rust-toolchain.toml`. Docker and CI use
the same compiler version.

## Live end-to-end verification

The opt-in test uses a real DeepSeek API key and incurs API charges. Put only the
key in `.cache/deepseek-e2e/api-key`, then run:

```sh
cargo build --locked
python3 tests/deepseek_e2e.py --api-key-file .cache/deepseek-e2e/api-key
```

The test discovers available models, starts an isolated local gateway, and checks
JSON/SSE responses, concurrent Responses and Messages requests with conflicting
client headers, namespace tool results, and custom `apply_patch` calls. If Codex
CLI is installed, it also checks an actual Codex turn and a command/tool-result
round trip using temporary command-line settings. Existing Codex settings are
not edited. Use `--model` to select a model or `--skip-codex` for HTTP-only runs.

The gateway reads the key from a child-process environment variable; it is not
written into the test database or passed in command-line arguments. Test reports
and protocol traces are saved in ignored `target/deepseek-e2e/`, and the gateway
is stopped when the test finishes. To diagnose adapter selection, enable
`RUST_LOG=info,estuary::proxy=debug`: `upstream selected` records the client URL,
upstream URL, and adapter (`deepseek_responses` or `deepseek_messages`).
