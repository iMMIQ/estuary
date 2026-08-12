# Deployment

[Documentation index](../docs/README.md) | [Architecture](../docs/architecture.md) |
[Configuration and operations](../docs/operations.md)

Estuary's production process model is one stable deployment process and one
active gateway worker. The deployment process owns the public socket and passes
it directly to the active worker, so inference bytes do not cross a proxy. A
version switch warms a candidate worker, activates it, then drains the previous
worker without interrupting accepted requests.

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
| `/opt/estuary/state` | Current version, alternating worker slots, and switch journal. |
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

The public listener defaults to `:8080`. The stable management listener defaults
to `127.0.0.1:9090`: configure gateways at `/admin/` and manage executable
versions at `/deploy/`.

## Version Management

Open `http://127.0.0.1:9090/deploy/` to upload a static Estuary binary, list
installed versions, switch to any compatible version, or delete an inactive
version. The deployment API uses the same `ESTUARY_ADMIN_TOKEN` authentication
as the gateway management interface.

Uploaded files are size-limited, written to a temporary file, synced, validated
with `estuary --version`, content-checked when a version already exists, and
then installed into the immutable release directory. Uploading an executable is
equivalent to granting code execution and the management listener must remain
private.

## Binary Rollout

Deploy a new static binary without restarting the supervisor:

```bash
sudo ./deploy/rollout.sh ./estuary
```

The rollout client remains available for local automation. It performs the same
validation and requests a serialized version switch:

1. Management writes are frozen and a rollout journal is persisted.
2. The candidate starts paused and passes the current readiness requirement.
3. The candidate activates the inherited public listener.
4. The previous worker stops accepting and drains every accepted response.
5. The stable `current` link is updated and management writes resume.

If candidate startup fails, the active worker is unchanged. After a successful
switch, the previous worker may remain visible while long responses drain; a
second switch is rejected until that worker exits.

Inspect supervisor and worker state:

```bash
/opt/estuary/state/current/estuary status
```

If the deployment process restarts during a switch, it starts the stable
`current` version and clears the interrupted switch journal.

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
ESTUARY_IMAGE=registry.example.com/estuary:0.3.3 \
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

Use the official static binary matching the container architecture. The
deployment process and workers run as the unprivileged `estuary` user; the
release directory is writable only by that account inside the container.

## Capacity and Security

- One worker accepts new traffic in steady state, so configured concurrency and
  in-memory connection statistics apply to the whole host.
- Old and new generations overlap only while the previous worker drains.
- Protect `/var/lib/estuary`: upstream keys and custom header values are stored
  as plaintext in SQLite.
- Keep the management listener private and use `ESTUARY_ADMIN_TOKEN` whenever it
  is not loopback-only.
- The public listener has no inbound authentication and should be restricted or
  placed behind an authenticating proxy.
- Container or host failure is outside the version-switch boundary; the
  external process manager or Docker restart policy restores the supervisor.
