# Documentation

[Estuary](../README.md) keeps the root README focused on installation and first
use. The maintained technical documentation is organized by task:

| Document | Use it for |
| --- | --- |
| [Architecture](architecture.md) | Request flow, scheduling, persistence, protocol adaptation, backpressure, and process lifecycle. |
| [Configuration and operations](operations.md) | Runtime settings, node configuration, security, health checks, management endpoints, and metrics. |
| [Native vLLM provider](vllm.md) | vLLM version requirements, telemetry, tokenization, KV events, and client compatibility. |
| [Deployment](../deploy/README.md) | Static binary installation, Docker, zero-downtime rollout, rollback, and persistent paths. |
| [Performance benchmark](performance.md) | Reproducible gateway-overhead benchmark commands and output. |

The executable is authoritative for runtime flags and environment variables:

```bash
estuary --help
estuary supervisor --help
estuary rollout --help
```

Node-specific configuration is authoritative in SQLite and is managed through
the embedded application or `/admin/api/nodes` endpoints.
