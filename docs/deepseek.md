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

The dependency uses Rust let chains, so source builds now require Rust 1.88 or
newer. Docker and CI toolchain settings use the same minimum.
