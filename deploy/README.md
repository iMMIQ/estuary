# Binary rolling deployment

The production layout uses one HAProxy frontend and two fixed Estuary process
slots. A rollout drains and replaces one slot at a time, so existing response
bodies and SSE streams stay attached to the old process until EOF.

Requirements: Linux with systemd, HAProxy 2.4 or newer, `curl`, `socat`, and
`flock`. Both gateway processes must run on the same host and use the same
SQLite database on a local filesystem. Do not place the WAL database on NFS.

Install the first release:

```console
sudo ./deploy/install.sh ./estuary
```

Review `/etc/estuary/common.env`, add upstream credential environment
variables, and then open `http://127.0.0.1:9090/admin/`.

Roll out another release:

```console
sudo ./deploy/rollout.sh ./estuary
```

The script stages an immutable release, drains slot A, verifies its replacement,
then repeats for slot B. A replacement that does not become ready is rolled back
to that slot's previous binary. If an old stream exceeds the drain deadline, the
script leaves the old process alive and drained instead of terminating it.

The stable admin frontend is disabled while versions are mixed, preventing an
older process from writing configuration that a newer runtime has already
interpreted. Inference remains available. Database changes shipped in a rolling
release must be additive and readable by both the current and previous binary;
destructive schema or JSON-field removal requires a later contract release.

Node concurrency is process-local. With two serving slots, configure each
node's `max_concurrency` to half of the desired fleet-wide limit. A rollout does
not overlap old and new binaries in one slot, so it cannot increase that budget;
capacity temporarily falls while a slot drains. Run three or more fixed slots
when retaining more capacity during maintenance is required.
