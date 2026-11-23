# Deployment

[Documentation Index](README.md) | [Configuration & Security](configuration.md) | [Architecture](architecture.md)

Estuary runs as a single self-contained binary with an embedded management web application. Runtime state (concurrency leases, ingress queues, health/circuit state, observability histories, and prefix/KV directories) is process-local; persisted node configurations and IP connection limits live in SQLite.

> [!IMPORTANT]
> SQLite databases and their `-wal` / `-shm` sidecar files must reside on a local Linux filesystem. Do not place them on NFS or other network filesystems.

## Static Linux Binaries

GitHub Releases publish static `musl` binaries for `amd64` and `arm64`:

```bash
sha256sum --check estuary-v0.5.0-linux-amd64.tar.gz.sha256
tar -xzf estuary-v0.5.0-linux-amd64.tar.gz
./estuary-v0.5.0-linux-amd64/estuary \
  --database /var/lib/estuary/estuary.db \
  --listen 0.0.0.0:8080 \
  --admin-listen 127.0.0.1:9090
```

When supervising `estuary` with systemd or another process manager:

- Run as an unprivileged user (for example `estuary`) owning `/var/lib/estuary`.
- Forward `SIGTERM` and set a kill grace period of at least `ESTUARY_WITHDRAWAL_DELAY_MS + ESTUARY_SHUTDOWN_GRACE_MS` before sending `SIGKILL`.
- Retain stdout/stderr (set `ESTUARY_LOG_JSON=true` for structured JSON logs).
- Raise the open-file descriptor limit (`LimitNOFILE=65536` or higher for high connection counts).
- Configure liveness against `GET /health/live` and load-balancer traffic admission against `GET /health/ready`.

## Docker Image Build

Build and runtime are separate. The multi-stage [Dockerfile](../Dockerfile) compiles the web UI and Rust binary in builder stages and produces a minimal Alpine image containing only CA certificates, the static `estuary` binary, and an unprivileged `estuary` user (`uid 10001`).

### Standard Build (Official Sources)

Because `.git` is excluded by `.dockerignore`, pass `ESTUARY_BUILD_VERSION` explicitly when running `docker build` directly:

```bash
docker build \
  --build-arg ESTUARY_BUILD_VERSION="$(sh deploy/build-version.sh)" \
  -t estuary:local .
```

### Mirror-Aware Local Build Wrapper

To probe official and mainland mirrors (Docker Hub, Alpine APK, Cargo sparse index, npm) in parallel and build with the fastest reachable sources:

```bash
./deploy/docker-build.sh
```

Override the output tag with `ESTUARY_IMAGE`:

```bash
ESTUARY_IMAGE=registry.example.com/estuary:0.5.0 ./deploy/docker-build.sh
```

### Build Version Resolution

[deploy/build-version.sh](../deploy/build-version.sh) and [crates/estuary-version/build.rs](../crates/estuary-version/build.rs) resolve the embedded version identically for both the CLI and management UI:

- If `ESTUARY_BUILD_VERSION` is set, that value is used.
- On a commit tagged `v<Cargo.toml version>` (e.g. `v0.5.0`), the version is `0.5.0`.
- On other commits, the version is `<version>+<12-char Git SHA>`.
- Without Git metadata or `ESTUARY_BUILD_VERSION`, the version falls back to `<version>+unknown`.

## Docker Compose Runtime

[compose.yaml](../compose.yaml) consumes a prebuilt image and never builds source code:

```bash
export ESTUARY_ADMIN_TOKEN="$(openssl rand -hex 32)"
export ESTUARY_IMAGE=estuary:local   # optional; defaults to estuary:local
docker compose up -d
```

Default container and Compose behavior:

| Setting | Container / Compose default | Notes |
| --- | --- | --- |
| Public listener | `0.0.0.0:8080` → host `:8080` | Open inference port. See [Security](configuration.md#security-model) to enable API keys or RPM limits. |
| Management listener | `0.0.0.0:9090` → host `127.0.0.1:9090` | `Dockerfile` binds `0.0.0.0:9090` inside the container, so `ESTUARY_ADMIN_TOKEN` is mandatory; Compose exposes it only on host loopback. |
| Persistent volume | `estuary_data:/var/lib/estuary` | Holds `estuary.db` (and optional session log DB if configured under `/var/lib/estuary`). |
| Host access | `host.docker.internal:host-gateway` | Allows container nodes to reach model servers running directly on the Linux host. |
| Container healthcheck | `GET http://127.0.0.1:9090/health/live` | Checks process liveness every 10s. |
| Stop grace period | `62m` | Covers default 10s withdrawal + 61m response drain (`ESTUARY_SHUTDOWN_GRACE_MS=3660000`). |
| Open files (`nofile`) | `65536` | Configured via Compose `ulimits`. |

> [!NOTE]
> The CLI binary itself defaults `--admin-listen` to `127.0.0.1:9090` and `--log-json` to `false`. The Docker image overrides these via `ENV` to `0.0.0.0:9090` and `true`.

## Graceful Shutdown & Rolling Updates

Shutdown is triggered by `SIGINT`, `SIGTERM`, or `PUT /admin/api/process/drain`:

1. `/health/ready` immediately switches to `503 Service Unavailable` while `/health/live` remains `200 OK`.
2. Estuary waits `ESTUARY_WITHDRAWAL_DELAY_MS` (default `10000` ms) so upstream load balancers stop sending new traffic.
3. The public listener stops accepting new TCP connections and waits for accepted connections to begin their first request.
4. All admitted queues, in-flight requests, and active streaming responses drain until idle or until `ESTUARY_SHUTDOWN_GRACE_MS` (default `3660000` ms) expires.
5. Background monitors stop, pending session log batches flush (up to 1 second within remaining grace), and the process exits.

Because concurrency leases, health, circuits, and prefix state are process-local, perform zero-downtime upgrades behind a load balancer or container orchestrator (start the new instance, wait for `/health/ready` to return `200`, then send `SIGTERM` or `PUT /admin/api/process/drain` to the old instance). Multiple local processes can share the same local SQLite database file; each reconciles persisted node and IP-limit changes periodically via `ESTUARY_CONTROL_SYNC_INTERVAL_MS`.
