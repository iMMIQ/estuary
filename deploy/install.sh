#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "usage: $0 /path/to/estuary" >&2
    exit 2
fi
if [[ ${EUID} -ne 0 ]]; then
    echo "install.sh must run as root" >&2
    exit 1
fi

binary=$(readlink -f -- "$1")
if [[ ! -x ${binary} ]]; then
    echo "binary is not executable: ${binary}" >&2
    exit 1
fi
version=$("${binary}" --version | awk '{print $2}')
if [[ ! ${version} =~ ^[0-9A-Za-z._+-]+$ ]]; then
    echo "could not determine a safe version from ${binary}" >&2
    exit 1
fi

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
if ! id estuary >/dev/null 2>&1; then
    useradd --system --home-dir /var/lib/estuary --shell /usr/sbin/nologin estuary
fi
install -d -o estuary -g estuary -m 0750 /var/lib/estuary
install -d -o estuary -g estuary -m 0750 /var/lib/estuary/run
install -d -o root -g estuary -m 0750 /etc/estuary
install -d -o root -g root -m 0755 \
    /opt/estuary \
    /opt/estuary/bin \
    /opt/estuary/releases
install -d -o estuary -g estuary -m 0755 \
    /opt/estuary/state \
    /opt/estuary/state/slots/a \
    /opt/estuary/state/slots/b

release_dir=/opt/estuary/releases/${version}
install -d -o root -g root -m 0755 "${release_dir}"
if [[ -e ${release_dir}/estuary ]] && ! cmp --silent "${binary}" "${release_dir}/estuary"; then
    echo "release ${version} already exists with different content" >&2
    exit 1
fi
if [[ ! -e ${release_dir}/estuary ]]; then
    install -o root -g root -m 0755 "${binary}" "${release_dir}/estuary"
fi
ln -sfn "${release_dir}" /opt/estuary/state/current
ln -sfn "${release_dir}" /opt/estuary/state/slots/a/current
ln -sfn "${release_dir}" /opt/estuary/state/slots/b/current

if [[ ! -e /etc/estuary/common.env ]]; then
    install -o root -g estuary -m 0640 "${script_dir}/env/common.env.example" /etc/estuary/common.env
fi
install -o root -g root -m 0755 "${script_dir}/run.sh" /opt/estuary/bin/run

cat <<EOF
Estuary ${version} is installed but not started.

Review /etc/estuary/common.env, then run the foreground supervisor as the
estuary user under your process manager:

  sudo -u estuary /opt/estuary/bin/run

Public: :8080, admin: 127.0.0.1:9090
EOF
