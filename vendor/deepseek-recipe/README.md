# Vendored deepseek-recipe protocol libraries

This directory contains the MIT-licensed `deepseek-recipe`,
`deepseek-recipe-core`, and `deepseek-recipe-macros` crates (version 0.2.0) from
[deepseek-ai/deepseek-recipe](https://github.com/deepseek-ai/deepseek-recipe), pinned
to revision `935817c66f5d6e64838f419692545f06787e485f`.

Estuary uses the protocol adapters and conversation types to convert Messages
and Responses clients to a generic Chat Completions backend. Model prompt
rendering, tokenization and inference are performed by the backend.

Model prompt encoders, image preprocessing, Python bindings, demonstration
servers and tokenizer assets are excluded. Hugging Face tokenizer integration
is optional and disabled in Estuary. Tool parameter JSON Schema validation
remains enabled.

See [ESTUARY.md](ESTUARY.md) for local patches and update instructions, and
[LICENSE](LICENSE) for the upstream MIT license. The complete upstream README
and documentation are available in the source repository at the pinned revision.
