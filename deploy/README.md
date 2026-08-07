# Binary rolling deployment

The production layout uses Estuary's built-in supervisor and two fixed worker
slots. The supervisor owns the stable public listener and passes the same
listening file descriptor directly to both workers. Inference bytes do not pass
through a separate proxy process.

Requirements: Linux and a process manager capable of running a foreground
process as a dedicated user. Estuary is not coupled to a particular init
system. Both workers use the same SQLite database on a local filesystem. Do not
place the database or its WAL files on NFS.

Install the first release:

```console
sudo ./deploy/install.sh ./estuary
```

The installer creates the `estuary` user, immutable release and slot links,
runtime directories, `/etc/estuary/common.env`, and the stable foreground
launcher at `/opt/estuary/bin/run`. It deliberately does not register or start a
host service.

Review `/etc/estuary/common.env`, then configure your process manager to:

- run `/opt/estuary/bin/run` as the `estuary` user;
- keep exactly one supervisor instance running and restart it after failure;
- pass `SIGTERM` for shutdown and allow up to the configured drain deadline;
- retain stdout and stderr and set an open-file limit suitable for expected
  concurrency.

For an interactive first start, the same foreground entrypoint can be run
directly:

```console
sudo -u estuary /opt/estuary/bin/run
```

Open `http://127.0.0.1:9090/admin/` and configure upstream nodes. Direct
upstream keys are stored unencrypted in `/var/lib/estuary/estuary.db`; protect
that file, its WAL files, snapshots, and backups.

Roll out another release:

```console
sudo ./deploy/rollout.sh ./estuary
```

The Rust rollout client validates and stages the executable in an immutable
version directory, then asks the running supervisor to update slot A followed by
slot B. Each old worker stops accepting connections and drains every accepted
response before its replacement starts. The other slot keeps accepting from the
shared kernel queue. A stream exceeding the deployment deadline is left alive
and drained; it is never forcefully killed by the rollout.

The replacement starts paused, loads SQLite, and starts health and provider
monitors. Once a slot has reached runtime readiness, every replacement must
reach it again before the supervisor activates its inherited public listener. A
failed replacement is immediately restored to the previous binary. If slot B
fails after slot A was updated, slot A is also rolled back, preserving a single
worker version.

Management writes are frozen through a shared file for the entire transaction.
The freeze survives a supervisor failure. On restart, matching slot releases are
reconciled only after both workers start successfully; mixed releases keep
writes frozen until another rollout converges both slots. Read-only management
views remain available from slot A.

Inspect the local process state:

```console
/opt/estuary/state/current/estuary status
```

The supervisor remains the version that originally started the process while
workers roll forward. After a successful rollout the stable `current` link is
updated, so the new supervisor is used the next time the process manager starts
it. Worker-control protocol changes must therefore remain backward compatible.

The built-in supervisor protects worker availability, but it cannot restart
itself after a supervisor crash or host reboot. That outer lifecycle remains the
responsibility of the chosen process manager. Restarting the supervisor itself
is not a zero-downtime operation because it owns the public listener.

Node concurrency remains process-local. With two serving slots, configure each
node's `max_concurrency` to half of the desired host-wide limit. A rollout never
overlaps two generations in one slot, so it cannot temporarily exceed that
budget; capacity is reduced while one slot drains.

## Docker

The repository image runs the same supervisor and persists its releases, slot
links, SQLite database, and rollout journal in named volumes. Set a management
token and start it with Compose:

```console
export ESTUARY_ADMIN_TOKEN="$(openssl rand -hex 32)"
./deploy/docker-build.sh
docker compose up -d
```

`docker-build.sh` probes the official and mainland Docker, Alpine, Cargo, and
npm endpoints in parallel, then builds `estuary:local` with the fastest usable
source. A plain `docker build -t estuary:local .` keeps the Dockerfile's official
defaults and is suitable for GitHub-hosted CI. Build and runtime are separate:
`compose.yaml` only runs `ESTUARY_IMAGE` (default `estuary:local`) and never
builds source code.

The public API listens on `:8080`. The management UI is published only on
`http://127.0.0.1:9090/admin/`. From a node configured in the UI,
`host.docker.internal` resolves to the Docker host on Linux.

Use an official static binary for the container architecture to update workers
without replacing the container:

```console
docker cp ./estuary estuary:/tmp/estuary.new
docker exec --user root estuary \
  /opt/estuary/state/current/estuary rollout /tmp/estuary.new
docker exec estuary /opt/estuary/state/current/estuary status
docker exec --user root estuary rm /tmp/estuary.new
```

The release volume is root-owned so a compromised gateway worker cannot replace
its own executable. Recreating the only container still interrupts the public
listener; the rollout command above is the zero-downtime application update
path. Keep the named volumes on a local filesystem and do not use an empty bind
mount for `/opt/estuary`, because it would hide the image's initial release.
