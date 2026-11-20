English | [简体中文](README.zh-CN.md) | [日本語](README.ja.md)

# Estuary

[![CI](https://github.com/iMMIQ/estuary/actions/workflows/ci.yml/badge.svg)](https://github.com/iMMIQ/estuary/actions/workflows/ci.yml)
[![Release](https://github.com/iMMIQ/estuary/actions/workflows/release.yml/badge.svg)](https://github.com/iMMIQ/estuary/actions/workflows/release.yml)

Estuary is a self-hosted Rust gateway for OpenAI-compatible model servers. It exposes OpenAI and Anthropic client APIs on a single public listener, routes every request to healthy upstreams using live load, latency, error, workload, and prompt-prefix signals, and ships as a single binary with an embedded English / Simplified Chinese management application.

## Features

- **Unified client protocols**: OpenAI Chat Completions, Responses, Completions, Embeddings, Models, and Anthropic Messages (including `count_tokens`), all normalized onto upstream Chat Completions for generation.
- **Load- and prefix-aware scheduling**: Per-node concurrency caps, weighted load/prefill/decode/latency/error scoring, approximate radix-tree prefix affinity for any server, and exact KV-block routing for vLLM 0.25+.
- **Live control plane**: SQLite-backed node registry with live configuration updates without draining active requests, optimistic revision locking, active health probes, circuit breakers, and bounded retries.
- **Built-in security controls**: Optional static public API keys, per-key/IP RPM throttling, persisted per-IP connection limits, management token authentication, and optional BLAKE3 authenticated encryption for stored upstream credentials.
- **Observability & optional session logs**: Real-time dashboard with smoothed rates and histograms, OpenMetrics `/metrics`, and optional content-deduplicated session logging in an independent SQLite database.
- **Bounded & self-contained**: Bounded connection, ingress queue, response buffer, and streaming backpressure with graceful shutdown; ships as static Linux binaries or a minimal Alpine Docker image.

## Quick Start

### Docker

Build the image (auto-selecting the fastest reachable registry mirror) and start with Docker Compose:

```bash
export ESTUARY_ADMIN_TOKEN="$(openssl rand -hex 32)"
./deploy/docker-build.sh
docker compose up -d
```

- Public inference API: `http://127.0.0.1:8080`
- Management UI: `http://127.0.0.1:9090/admin/` (HTTP Basic Auth: any username, password = `ESTUARY_ADMIN_TOKEN`)

See [Deployment](docs/deployment.md) for prebuilt static Linux binaries, build versions, volumes, and graceful rolling updates.

### From Source

Install the Rust toolchain in [rust-toolchain.toml](rust-toolchain.toml) with LLD (`ld.lld`), plus Node.js ([.node-version](.node-version)) and Bun. Build the embedded web UI once, then run Estuary:

```bash
(cd web && bun install --frozen-lockfile && bun run build)
mkdir -p data
cargo run --release -- \
  --database ./data/estuary.db \
  --listen 0.0.0.0:8080 \
  --admin-listen 127.0.0.1:9090
```

## Configure a Node & Verify

Open `http://127.0.0.1:9090/admin/` and add an upstream node with:

- a unique node ID;
- an absolute OpenAI-compatible `base_url` (typically ending in `/v1`);
- at least one public-to-upstream model mapping (`*` preserves the requested model name);
- a positive `max_concurrency` limit and provider type (`openai` or `vllm`).

An empty database starts normally, and `/health/ready` remains `503` until at least one node is routable.

Once saved, verify the model catalog and send a Chat Completion:

```bash
curl -sS http://127.0.0.1:8080/v1/models

curl -sS http://127.0.0.1:8080/v1/chat/completions \
  -H 'content-type: application/json' \
  -d '{
    "model": "gateway-chat",
    "messages": [{"role": "user", "content": "Hello"}]
  }'
```

For Claude Code, Codex, OpenCode, and full protocol details, see [API & Clients](docs/api.md).

## Documentation

| Guide | Covers |
| --- | --- |
| [Documentation Index](docs/README.md) | Role-based map of all documentation. |
| [Deployment](docs/deployment.md) | Static Linux binaries, Docker builds, Compose runtime, graceful shutdown, and rolling updates. |
| [Configuration & Security](docs/configuration.md) | Process flags/env vars, node schema, live hot-updates, authentication, rate limiting, and credential encryption. |
| [API & Clients](docs/api.md) | Public inference endpoints, protocol adaptation boundaries, Claude Code / Codex / OpenCode setup, and Admin API. |
| [vLLM Provider](docs/vllm.md) | vLLM 0.25+ version gate, telemetry, shared tokenization, ZMQ KV events, and exact cache routing. |
| [Observability & Metrics](docs/observability.md) | Dashboard smoothing/histograms, `/admin/api/observability`, and OpenMetrics `/metrics` reference. |
| [Session Logging](docs/session-logging.md) | Independent SQLite request/session capture, content deduplication, redaction, retention, and query limits. |
| [Architecture](docs/architecture.md) | Request pipeline, scheduling math, prefix/KV directories, hot-updates, and system boundaries. |
| [Contributing](CONTRIBUTING.md) | Local development, CI checks, shared config contract generation, E2E harnesses, and benchmarks. |

## License

[Apache License 2.0](LICENSE)
