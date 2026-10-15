# Vendored deepseek-recipe protocol libraries

This directory contains the MIT-licensed `deepseek-recipe` and
`deepseek-recipe-core` crates from
[deepseek-ai/deepseek-recipe](https://github.com/deepseek-ai/deepseek-recipe), pinned
to revision `8cadfede7063c896b944e7bae05daa3549ae97ea`.

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
