# Deployment

[Documentation index](../docs/README.md) | [Architecture](../docs/architecture.md) |
[Configuration and operations](../docs/operations.md)

Estuary's production process model is one supervisor with two fixed worker
slots. The supervisor owns the stable public socket and passes it directly to
both workers. A rollout drains and replaces one worker at a time while the other
continues to accept from the kernel queue.

The database, release state, and runtime files must be on a local Linux
filesystem. Do not use NFS for SQLite or its WAL files.

## Static Binary Deployment

GitHub Releases provide static Linux binaries for `amd64` and `arm64`. Install
the first release as root:

```bash
sudo ./deploy/install.sh ./estuary
```

The installer creates:

| Path | Contents |
| --- | --- |
| `/opt/estuary/releases` | Immutable versioned binaries. |
| `/opt/estuary/state` | Stable current and A/B slot links plus rollout journal. |
| `/opt/estuary/bin/run` | Foreground supervisor launcher. |
| `/var/lib/estuary` | SQLite database and runtime directory. |
| `/etc/estuary/common.env` | Process configuration. |

It also creates the unprivileged `estuary` user. It does not register or start a
host service.

Review `/etc/estuary/common.env`, then configure the host process manager to:

- run `/opt/estuary/bin/run` as `estuary`;
- keep exactly one supervisor running and restart it after failure;
- forward SIGTERM and allow at least the configured shutdown/drain deadline;
- retain stdout/stderr and set a suitable open-file limit.

For an interactive start:

```bash
sudo -u estuary /opt/estuary/bin/run
```

The public listener defaults to `:8080`; the slot-A management listener defaults
to `127.0.0.1:9090`. Configure the first upstream at
`http://127.0.0.1:9090/admin/`.

## Binary Rollout

Deploy a new static binary without restarting the supervisor:

```bash
sudo ./deploy/rollout.sh ./estuary
```

The rollout client validates `--version`, content-checks an existing version,
stages and fsyncs the candidate, and requests a serialized A/B rollout:

1. Management writes are frozen and a rollout journal is persisted.
2. Slot A stops accepting and drains every accepted response.
3. Its replacement starts paused, initializes, passes the readiness requirement,
   and activates the inherited public listener.
4. Slot B repeats the same transition.
5. The stable `current` link is updated and management writes resume.

If a replacement fails, its previous binary is restored. If slot B fails after
slot A succeeded, slot A is also rolled back. A worker exceeding the rollout
drain deadline is left alive and drained; it is not killed to complete a deploy.

Inspect supervisor and worker state:

```bash
/opt/estuary/state/current/estuary status
```

If the supervisor restarts during rollout, equal slot links are reconciled and
the transaction is finalized. Mixed links keep management writes frozen until a
later rollout converges both slots.

The running supervisor itself is not replaced during worker rollout. The new
`current` binary becomes supervisor on the next process-manager restart.
Restarting the only supervisor closes its public socket and is not a
zero-downtime operation.

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

The wrapper probes official and mainland endpoints in parallel, verifies all
three base-image manifests plus the required Alpine repositories, then passes
the fastest available Docker Registry, Alpine APK, Cargo sparse registry, and
npm registry to `docker build`. It produces `estuary:local` by default; override
the tag with `ESTUARY_IMAGE`:

```bash
ESTUARY_IMAGE=registry.example.com/estuary:0.3.1 \
./deploy/docker-build.sh
```

The Dockerfile is multi-stage. Bun and Rust toolchains remain in build stages;
the final Alpine image contains only CA certificates, the Estuary binary, and
the initialized release layout.

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

Two named volumes are required:

| Volume | Mount | Contents |
| --- | --- | --- |
| `estuary_releases` | `/opt/estuary` | Versioned binaries, slot links, and rollout journal. |
| `estuary_data` | `/var/lib/estuary` | SQLite, WAL, and runtime socket. |

Do not replace `/opt/estuary` with an empty bind mount: it hides the initial
release included in the image. Compose grants a 62-minute stop grace so the
supervisor can drain long-running responses.

## Docker Binary Rollout

Updating the single container replaces its supervisor and interrupts the owned
public listener. For a zero-downtime application update, keep the container
running and use the built-in binary rollout:

```bash
docker cp ./estuary estuary:/tmp/estuary.new
docker exec --user root estuary \
  /opt/estuary/state/current/estuary rollout /tmp/estuary.new
docker exec estuary /opt/estuary/state/current/estuary status
docker exec --user root estuary rm /tmp/estuary.new
```

Use the official static binary matching the container architecture. The release
volume is root-owned, so only an operator with Docker-level privilege can stage
a new executable; workers continue to run as the unprivileged `estuary` user.

## Capacity and Security

- Both workers have process-local node semaphores. Set a node's
  `max_concurrency` to half of its intended host-wide concurrency.
- Capacity is reduced while one worker drains; old and new generations never
  overlap in the same slot.
- Protect `/var/lib/estuary`: upstream keys and custom header values are stored
  as plaintext in SQLite.
- Keep the management listener private and use `ESTUARY_ADMIN_TOKEN` whenever it
  is not loopback-only.
- The public listener has no inbound authentication and should be restricted or
  placed behind an authenticating proxy.
- Container or host failure is outside the two-worker rollout boundary; the
  external process manager or Docker restart policy restores the supervisor.
