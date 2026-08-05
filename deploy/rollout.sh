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
for command in curl flock socat systemctl; do
    command -v "${command}" >/dev/null || {
        echo "required command is missing: ${command}" >&2
        exit 1
    }
done

binary=$(readlink -f -- "$1")
version=$("${binary}" --version | awk '{print $2}')
if [[ ! -x ${binary} || ! ${version} =~ ^[0-9A-Za-z._+-]+$ ]]; then
    echo "invalid Estuary binary: ${binary}" >&2
    exit 1
fi

exec 9>/run/lock/estuary-rollout.lock
flock -n 9 || {
    echo "another Estuary rollout is already running" >&2
    exit 1
}

release_dir=/opt/estuary/releases/${version}
install -d -o root -g root -m 0755 "${release_dir}"
if [[ -e ${release_dir}/estuary ]] && ! cmp --silent "${binary}" "${release_dir}/estuary"; then
    echo "release ${version} already exists with different content" >&2
    exit 1
fi
if [[ ! -e ${release_dir}/estuary ]]; then
    install -o root -g root -m 0755 "${binary}" "${release_dir}/estuary"
fi

haproxy_socket=${ESTUARY_HAPROXY_SOCKET:-/run/estuary/haproxy.sock}
start_timeout=${ESTUARY_START_TIMEOUT_SECONDS:-180}
drain_timeout=${ESTUARY_DRAIN_TIMEOUT_SECONDS:-3700}

haproxy_command() {
    printf '%s\n' "$1" | socat - "UNIX-CONNECT:${haproxy_socket}" >/dev/null
}

wait_ready() {
    local port=$1
    local deadline=$((SECONDS + start_timeout))
    until curl --fail --silent --show-error "http://127.0.0.1:${port}/health/ready" >/dev/null; do
        if (( SECONDS >= deadline )); then
            return 1
        fi
        sleep 1
    done
}

wait_stopped() {
    local unit=$1
    local deadline=$((SECONDS + drain_timeout))
    while systemctl is-active --quiet "${unit}"; do
        if (( SECONDS >= deadline )); then
            return 1
        fi
        sleep 1
    done
}

activate_slot() {
    local slot=$1
    haproxy_command "set server estuary_public/slot-${slot} state ready"
    echo "slot ${slot} is serving ${version}"
}

unfreeze_admin() {
    haproxy_command "set server estuary_admin/slot-a state ready" || true
    haproxy_command "set server estuary_admin/slot-b state ready" || true
}

rollback_slot() {
    local slot=$1
    local port=$2
    local previous=$3
    echo "slot ${slot} failed readiness; rolling back" >&2
    systemctl stop "estuary@${slot}.service" || true
    if [[ -n ${previous} && -d ${previous} ]]; then
        ln -sfn "${previous}" "/opt/estuary/slots/${slot}/current"
        if systemctl start "estuary@${slot}.service" && wait_ready "${port}"; then
            haproxy_command "set server estuary_public/slot-${slot} state ready"
        else
            echo "slot ${slot} rollback failed; operator intervention is required" >&2
        fi
    fi
    return 1
}

roll_slot() {
    local slot=$1
    local port=$2
    local unit="estuary@${slot}.service"
    local previous
    previous=$(readlink -f -- "/opt/estuary/slots/${slot}/current" 2>/dev/null || true)

    echo "draining slot ${slot}"
    curl --fail --silent --show-error --request PUT \
        "http://127.0.0.1:${port}/admin/api/process/drain" >/dev/null
    haproxy_command "set server estuary_public/slot-${slot} state drain"
    haproxy_command "set server estuary_admin/slot-${slot} state drain"
    if ! wait_stopped "${unit}"; then
        echo "slot ${slot} did not drain within ${drain_timeout}s; leaving it alive and drained" >&2
        return 1
    fi

    ln -sfn "${release_dir}" "/opt/estuary/slots/${slot}/current"
    if ! systemctl start "${unit}" || ! wait_ready "${port}"; then
        rollback_slot "${slot}" "${port}" "${previous}"
    fi
    activate_slot "${slot}"
}

haproxy_command "set server estuary_admin/slot-a state drain"
haproxy_command "set server estuary_admin/slot-b state drain"
trap unfreeze_admin EXIT
roll_slot a 19091
roll_slot b 19092
unfreeze_admin
trap - EXIT
echo "Estuary rollout ${version} completed"
