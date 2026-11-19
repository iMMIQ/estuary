# Vendored upstream source

Source: https://github.com/deepseek-ai/deepseek-recipe
Revision: 935817c66f5d6e64838f419692545f06787e485f
Version: 0.2.0
License: MIT (see LICENSE).

The protocol, shared conversation and procedural macro crates are included. Image
preprocessing, model prompt encoders, Python bindings, demo servers and tokenizer
assets are excluded. The macros provide the upstream request deserializers and
shared prompt types; Estuary does not render model prompts.

Local changes:
- Reduce workspace members to the three included crates and remove dependencies
  belonging to excluded encoders, image processing, Python bindings and servers.
- Make the Hugging Face tokenizer dependency and its StreamTokenizer implementation
  optional behind the `huggingface-tokenizer` feature (disabled by default).
  Estuary sends Chat requests to vLLM, which renders and tokenizes them.
- Gate the upstream stream processor tests behind the same feature; they require
  the excluded tokenizer assets. Other upstream unit tests remain available.
- Replace the full-workspace README with a description of the vendored subset.
- Keep upstream JSON Schema tool validation enabled.
- Retain Estuary's refreshed versions of existing workspace dependencies. Keep the
  Tokio dev-dependency, which the 0.2.0 upstream unit tests now use.

The Chat bridge registers each requested model as a V4.1 conversion alias to preserve
images, system messages and namespace names for all backends. This selects protocol
conversion rules only. Sampling values and thinking budgets come from the original
request, and token limits are left to the Chat backend within the protocol's u32
range. Response settings from conversion are passed to the upstream chunk generator.
Responses output formats are forwarded from the original request to Chat rather
than applying DeepSeek prompt constraints; tool JSON Schema validation stays enabled.

To update, replace the three crate source directories from the chosen upstream
revision, reapply the optional tokenizer feature and implementation/test guards,
adapt the Chat bridge, and run the protocol integration tests. Do not enable
DeepSeek-specific prompt encoders for other models.
