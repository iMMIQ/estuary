#!/usr/bin/env bash
set -euo pipefail

config=${ESTUARY_ENV_FILE:-/etc/estuary/common.env}
if [[ ! -r ${config} ]]; then
    echo "Estuary environment file is not readable: ${config}" >&2
    exit 1
fi
if [[ $(id -un) != estuary ]]; then
    echo "run Estuary as the estuary user, not as $(id -un)" >&2
    exit 1
fi

set -a
# shellcheck disable=SC1090
source "${config}"
set +a

current=${ESTUARY_STATE_ROOT:-/opt/estuary/state}/current/estuary
if [[ ! -x ${current} ]]; then
    echo "installed Estuary binary is not executable: ${current}" >&2
    exit 1
fi

exec "${current}" supervisor
