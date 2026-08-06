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
