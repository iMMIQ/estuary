# Estuary

[![CI](https://github.com/iMMIQ/estuary/actions/workflows/ci.yml/badge.svg)](https://github.com/iMMIQ/estuary/actions/workflows/ci.yml)
[![Release](https://github.com/iMMIQ/estuary/actions/workflows/release.yml/badge.svg)](https://github.com/iMMIQ/estuary/actions/workflows/release.yml)

Estuary is a self-hosted Rust gateway for OpenAI-compatible model servers. It
serves OpenAI and Anthropic client protocols from one public listener, maps
public model names to upstream models, and routes each request to a healthy node
using concurrency, observed load, latency, errors, and prompt-prefix locality.

## Features

- OpenAI Chat Completions, Responses, Completions, Embeddings, and Models APIs.
- Anthropic Messages and token counting through native Messages, Responses, or
  Chat Completions upstream protocols.
- Per-node model mappings, concurrency limits, weights, health checks, circuit
  breakers, and bounded retries.
- Approximate prompt-prefix affinity for generic servers and exact KV-block
  affinity for vLLM 0.25 and newer.
- SQLite-backed node configuration with an embedded management application.
- Bounded request admission, response buffering, streaming backpressure, and
  graceful shutdown.
- A built-in two-worker supervisor for zero-downtime binary rollout and rollback.
- Docker and static Linux binary deployment.

## API

| Method and path | Behavior |
| --- | --- |
| `GET /v1/models` | Lists configured public model names. |
| `GET /v1/models/{model}` | Returns one configured public model. |
| `POST /v1/chat/completions` | Buffered or SSE Chat Completions forwarding. |
| `POST /v1/responses` | Foreground buffered or SSE Responses forwarding. |
| `POST /v1/completions` | Completions forwarding. |
| `POST /v1/embeddings` | Embeddings forwarding. |
| `POST /v1/messages` | Anthropic Messages, including tools, thinking, and SSE. |
| `POST /v1/messages/count_tokens` | Token counting through a native Messages upstream. |
| `HEAD /api/hello` | Claude Code gateway capability probe. |

Inference routes are an allowlist. Other `/v1/*` operations return a
protocol-shaped `404`. Responses background mode, conversations,
`previous_response_id`, and `/v1/responses/{id}` operations are rejected because
the gateway does not persist response-to-node state.

## Quick Start

### Docker

Build and runtime are separate. Local builds select the fastest reachable
official or mainland mirror; the Dockerfile itself defaults to official sources.

```bash
export ESTUARY_ADMIN_TOKEN="$(openssl rand -hex 32)"
./deploy/docker-build.sh
docker compose up -d
```

The public API is available on `http://127.0.0.1:8080`. The management
application is available on `http://127.0.0.1:9090/admin/` and uses the token as
the HTTP Basic Auth password. See [deployment](deploy/README.md) for image tags,
volumes, shutdown, and rolling updates.

### From Source

Rust 1.85 or newer is required.

```bash
mkdir -p data
cargo run --release -- \
  --database ./data/estuary.db \
  --listen 0.0.0.0:8080 \
  --admin-listen 127.0.0.1:9090
```

Open `http://127.0.0.1:9090/admin/` and add an upstream node. An empty database
is valid, but readiness remains `503` until at least one node is routable.

## Configure A Node

The management application validates and probes a node before saving it. The
required fields are:

- a unique node ID;
- an absolute OpenAI-compatible base URL, normally ending in `/v1`;
- at least one public-to-upstream model mapping;
- a positive per-process concurrency limit.

Select `openai` for a generic OpenAI-compatible server or `vllm` for the native
vLLM integration. Upstream Bearer keys and custom headers can be stored with the
node. Model mappings rewrite the public request model before forwarding; a
mapping value of `*` preserves the requested name.

Verify the configured model catalog:

```bash
curl -sS http://127.0.0.1:8080/v1/models
```

Send a Chat Completion using a public model name:

```bash
curl -sS http://127.0.0.1:8080/v1/chat/completions \
  -H 'content-type: application/json' \
  -d '{
    "model": "gateway-chat",
    "messages": [{"role": "user", "content": "Hello"}]
  }'
```

## Clients

OpenAI-compatible clients use `http://HOST:8080/v1` as their base URL.

Claude Code uses the Anthropic listener:

```bash
ANTHROPIC_BASE_URL=http://127.0.0.1:8080 \
ANTHROPIC_API_KEY=estuary \
claude
```

Codex uses the Responses API:

```toml
model_provider = "estuary"
model = "gateway-chat"
web_search = "disabled"

[model_providers.estuary]
name = "Estuary"
base_url = "http://127.0.0.1:8080/v1"
wire_api = "responses"
requires_openai_auth = false
```

When Codex is routed to vLLM, Estuary supports the full Responses request shape,
including namespace tools. Responses Lite custom calls, tool search, additional
tools, and web search are rejected rather than silently changed. See the
[vLLM provider guide](docs/vllm.md) for protocol details.

## Production Notes

- The public inference listener does not authenticate callers. Keep it on a
  trusted network or place it behind an authenticating proxy.
- A non-loopback management listener requires `ESTUARY_ADMIN_TOKEN`. Health
  probes remain unauthenticated.
- Upstream keys and custom header values are stored unencrypted in SQLite.
  Protect the database, WAL files, snapshots, and backups.
- SQLite must be on a local filesystem. Do not place it on NFS.
- Node concurrency, queues, health, circuits, and prefix state are process-local.
  With the built-in two-worker supervisor, set each node's concurrency to half
  of the intended host-wide limit.
- A retry can repeat upstream generation or billing. The default is one attempt.

## Documentation

The [documentation index](docs/README.md) links the maintained guides:

- [Architecture](docs/architecture.md)
- [Configuration and operations](docs/operations.md)
- [Native vLLM provider](docs/vllm.md)
- [Deployment](deploy/README.md)
- [Performance benchmark](docs/performance.md)

## Development

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-targets
cd web
bun install --frozen-lockfile
bun run test
bun run build
```

Browser tests additionally require Playwright Chromium:

```bash
cd web
bunx --bun playwright install chromium
bun run test:e2e
```

## License

[Apache License 2.0](LICENSE)
