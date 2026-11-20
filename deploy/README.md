# Deployment

[Documentation index](../docs/README.md) | [Architecture](../docs/architecture.md) |
[Configuration and operations](../docs/operations.md)

Estuary runs as a single self-contained gateway binary with an embedded management
application. Graceful shutdown disables `/health/ready`, waits for the configured
withdrawal delay (`ESTUARY_WITHDRAWAL_DELAY_MS`), stops accepting new public
connections, and drains accepted queues and responses until
`ESTUARY_SHUTDOWN_GRACE_MS` expires. Use an external process manager (systemd,
Docker, Kubernetes) and load balancer for restarts and rolling upgrades.

The SQLite database and its WAL files must reside on a local Linux filesystem.
Do not place them on NFS.

## Static Binary Deployment

GitHub Releases provide static Linux binaries for `amd64` and `arm64`:

```bash
sha256sum --check estuary-v0.4.0-linux-amd64.tar.gz.sha256
tar -xzf estuary-v0.4.0-linux-amd64.tar.gz
./estuary-v0.4.0-linux-amd64/estuary \
  --database /var/lib/estuary/estuary.db \
  --listen 0.0.0.0:8080 \
  --admin-listen 127.0.0.1:9090
```

Configure the host process manager to:

- run `estuary` as an unprivileged user (for example `estuary`);
- forward `SIGTERM` and allow at least `ESTUARY_WITHDRAWAL_DELAY_MS + ESTUARY_SHUTDOWN_GRACE_MS`
  before `SIGKILL`;
- retain stdout/stderr and set an appropriate open-file limit (`nofile`).

You can also trigger graceful drain over the management listener with
`PUT /admin/api/process/drain`.

## Docker Image Build

Build and runtime are intentionally separate. A normal Docker build uses only
the official Docker, Alpine, Cargo, and npm sources:

```bash
docker build -t estuary:local .
```

For local builds where mainland mirrors may be faster, run the provided build
wrapper:

```bash
./deploy/docker-build.sh
```

The wrapper reads the base image tags from `Dockerfile`, probes official and
mainland endpoints in parallel, then passes the fastest reachable Docker
Registry, Alpine APK, Cargo sparse registry, and npm registry to `docker build`.
It produces `estuary:local` by default; override the tag with `ESTUARY_IMAGE`:

```bash
ESTUARY_IMAGE=registry.example.com/estuary:0.4.0 \
./deploy/docker-build.sh
```

The Dockerfile is multi-stage. Bun and Rust toolchains remain in build stages;
the final Alpine image contains only CA certificates and the static `estuary`
binary.

Build versions are determined by `deploy/build-version.sh`: a matching
`v<package-version>` tag on the current commit uses the plain package version;
all other commits append `+<12-character Git hash>`. The build wrapper passes
the result into Docker, and Cargo source builds resolve it automatically.

For a direct Docker build, pass the version explicitly because `.git` is
excluded from the build context:

```bash
docker build --build-arg ESTUARY_BUILD_VERSION="$(sh deploy/build-version.sh)" \
  -t estuary:local .
```

Source archives without Git metadata report `<package-version>+unknown` unless
`ESTUARY_BUILD_VERSION` is supplied. For a release, update `Cargo.toml`, its
package entry in `Cargo.lock`, and `web/package.json` together before tagging
the commit; release CI checks that the package versions match the tag.

## Docker Runtime

`compose.yaml` consumes an image and never builds source code. Set a management
token and optionally select an image:

```bash
export ESTUARY_ADMIN_TOKEN="$(openssl rand -hex 32)"
export ESTUARY_IMAGE=estuary:local
docker compose up -d
```

Compose publishes:

- public API: `0.0.0.0:8080`;
- management application: `127.0.0.1:9090`.

Inside the Docker bridge, `host.docker.internal` resolves to the Linux host and
can be used for model servers running directly on that host.

Persistent state uses one named volume:

| Volume | Mount | Contents |
| --- | --- | --- |
| `estuary_data` | `/var/lib/estuary` | SQLite database and WAL files. |

Compose grants a 62-minute `stop_grace_period` so `estuary` can drain
long-running responses on `SIGTERM`.

## Capacity and Security

- Concurrency, queues, health, circuits, and prefix state are process-local.
- Protect `/var/lib/estuary`: upstream keys and custom header values are stored
  as plaintext in SQLite.
- Keep the management listener private and use `ESTUARY_ADMIN_TOKEN` whenever it
  is not loopback-only.
- The public listener supports optional static API keys (`ESTUARY_PUBLIC_API_KEYS`)
  and RPM limiting (`ESTUARY_PUBLIC_RPM_LIMIT`), or can be placed behind an
  authenticating proxy.
