# Vendored upstream source

Source: https://github.com/deepseek-ai/deepseek-recipe
Revision: 8cadfede7063c896b944e7bae05daa3549ae97ea
License: MIT (see LICENSE).

Only the protocol and shared conversation crates are included. Image preprocessing,
model prompt encoders, Python bindings, demo servers and tokenizer assets are excluded.

Local changes:
- Reduce workspace members to the two included crates and remove dependencies
  belonging to excluded encoders, image processing, Python bindings and servers.
- Make the Hugging Face tokenizer dependency and its TokenizerDecoder implementation
  optional behind the `huggingface-tokenizer` feature (disabled by default).
  Estuary sends Chat requests to vLLM, which renders and tokenizes them.
- Replace the full-workspace README with a description of the vendored subset.
- Keep upstream JSON Schema tool validation enabled.

To update, replace the two crate source directories from the chosen upstream revision,
reapply the optional tokenizer feature and implementation guard, and run the protocol
integration tests. Do not enable DeepSeek-specific prompt encoders for other models.
