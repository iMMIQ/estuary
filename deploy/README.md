# Binary rolling deployment

The production layout uses Estuary's built-in supervisor and two fixed worker
slots. The supervisor owns the stable public listener and passes the same
listening file descriptor directly to both workers. Inference bytes do not pass
through a separate proxy process.

Requirements: Linux with systemd. Both workers use the same SQLite database on
a local filesystem. Do not place the database or its WAL files on NFS.

Install the first release:

```console
sudo ./deploy/install.sh ./estuary
```

Review `/etc/estuary/common.env`, then open `http://127.0.0.1:9090/admin/` and
configure upstream nodes. Direct upstream keys are stored unencrypted in
`/var/lib/estuary/estuary.db`; protect that file, its WAL files, snapshots, and
backups.

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

The supervisor remains the version that originally started the service while
workers roll forward. After a successful rollout the stable `current` link is
updated, so the new supervisor is used on the next service or host restart.
Worker-control protocol changes must therefore remain backward compatible.

Node concurrency remains process-local. With two serving slots, configure each
node's `max_concurrency` to half of the desired host-wide limit. A rollout never
overlaps two generations in one slot, so it cannot temporarily exceed that
budget; capacity is reduced while one slot drains.
