#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "usage: $0 /path/to/new/estuary" >&2
    exit 2
fi
if [[ ${EUID} -ne 0 ]]; then
    echo "rollout.sh must run as root" >&2
    exit 1
fi

binary=$(readlink -f -- "$1")
if [[ ! -x ${binary} ]]; then
    echo "invalid Estuary binary: ${binary}" >&2
    exit 1
fi
if [[ -r /etc/estuary/common.env ]]; then
    set -a
    # shellcheck disable=SC1091
    source /etc/estuary/common.env
    set +a
fi

current=${ESTUARY_STATE_ROOT:-/opt/estuary/state}/current/estuary
exec "${current}" rollout "${binary}"
