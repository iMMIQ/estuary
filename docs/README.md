# Estuary Documentation

[README](../README.md) ([简体中文](../README.zh-CN.md) | [日本語](../README.ja.md))

Each topic has a single authoritative document organized by role:

## Operate & Deploy

| Document | Covers |
| --- | --- |
| [Deployment](deployment.md) | Static Linux binaries, Docker image builds, Docker Compose runtime, volumes, graceful shutdown, and rolling updates. |
| [Configuration & Security](configuration.md) | Process CLI flags and environment variables, SQLite node schema, live node hot-updates, public/admin authentication, rate limits, and credential encryption. |
| [Observability & Metrics](observability.md) | Real-time dashboard rates/histograms, `/admin/api/observability`, and the complete OpenMetrics `/metrics` catalog. |
| [Session Logging](session-logging.md) | Independent SQLite conversation logging, content deduplication, automatic redaction, retention, and query limits. |

## Integrate Clients & Engines

| Document | Covers |
| --- | --- |
| [API & Clients](api.md) | Public inference routes, shared Chat Completions protocol bridge, compatibility boundaries, Claude Code / Codex / OpenCode setup, and the complete Admin API. |
| [vLLM Provider](vllm.md) | vLLM 0.25+ version gate, Prometheus telemetry, shared tokenization cache, ZMQ KV events, exact routing, and per-request engine timings. |

## Understand & Contribute

| Document | Covers |
| --- | --- |
| [Architecture](architecture.md) | Request pipeline, backpressure, continuous scheduling formula, prefix/KV directories, live node replacement, and system boundaries. |
| [Contributing](../CONTRIBUTING.md) | Local toolchains, build & CI commands, shared Rust/TypeScript config contract generation, local coding-agent E2E suites, and benchmarks. |

## Authoritative Runtime Sources

- Process flags and defaults (except `ESTUARY_CONFIG_ENCRYPTION_KEY`, which is environment-only): run `estuary --help`.
- Persisted node configurations and IP connection limits: stored in SQLite and managed via `/admin/` or `/admin/api/*`.
