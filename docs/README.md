# Documentation

[Estuary](../README.md) keeps the root README focused on installation and first
use. The maintained technical documentation is organized by task:

| Document | Use it for |
| --- | --- |
| [Architecture](architecture.md) | Request flow, scheduling, persistence, protocol adaptation, backpressure, and process lifecycle. |
| [Configuration and operations](operations.md) | Runtime settings, node configuration, security, health checks, management endpoints, and metrics. |
| [DeepSeek protocol conversion](deepseek.md) | Model family configuration and recipe adapters for Codex and Claude Code. |
| [Native vLLM provider](vllm.md) | vLLM version requirements, telemetry, tokenization, KV events, and client compatibility. |
| [Local end-to-end tests](local-e2e.md) | Real Claude Code tests with local vLLM, prebuilt llama.cpp, or an existing Ollama server. |
| [Deployment](../deploy/README.md) | Static binary installation, Docker, zero-downtime rollout, rollback, and persistent paths. |
| [Performance benchmark](performance.md) | Reproducible gateway-overhead benchmark commands and output. |
| [Generated configuration contract](config-contract.md) | Canonical config types, defaults, validation rules, shared cases, and module boundaries. |
| [Code quality review](code-quality.md) | Review findings, completed debt cleanup, remaining risks, and verification results. |
| [Session logging](session-logging.md) | Enable independent storage, inspect sessions, and understand capture limits and retention. |
| [Session logging design](session-logging-design.md) | Research and proposed schema, agent-context deduplication, performance/error capture, and independent log storage. |

The executable is authoritative for runtime flags and environment variables:

```bash
estuary --help
estuary supervisor --help
estuary rollout --help
```

Node-specific configuration is authoritative in SQLite and is managed through
the embedded application or `/admin/api/nodes` endpoints.
