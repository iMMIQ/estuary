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
for command in haproxy systemctl; do
    command -v "${command}" >/dev/null || {
        echo "required command is missing: ${command}" >&2
        exit 1
    }
done

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
version=$("${binary}" --version | awk '{print $2}')
if [[ ! ${version} =~ ^[0-9A-Za-z._+-]+$ ]]; then
    echo "could not determine a safe version from ${binary}" >&2
    exit 1
fi

if ! id estuary >/dev/null 2>&1; then
    useradd --system --home-dir /var/lib/estuary --shell /usr/sbin/nologin estuary
fi
install -d -o estuary -g estuary -m 0750 /var/lib/estuary
install -d -o root -g estuary -m 0750 /etc/estuary /etc/estuary/slots
install -d -o root -g root -m 0755 /opt/estuary/releases /opt/estuary/slots/a /opt/estuary/slots/b

release_dir=/opt/estuary/releases/${version}
install -d -o root -g root -m 0755 "${release_dir}"
if [[ -e ${release_dir}/estuary ]] && ! cmp --silent "${binary}" "${release_dir}/estuary"; then
    echo "release ${version} already exists with different content" >&2
    exit 1
fi
if [[ ! -e ${release_dir}/estuary ]]; then
    install -o root -g root -m 0755 "${binary}" "${release_dir}/estuary"
fi
ln -sfn "${release_dir}" /opt/estuary/slots/a/current
ln -sfn "${release_dir}" /opt/estuary/slots/b/current

if [[ ! -e /etc/estuary/common.env ]]; then
    install -o root -g estuary -m 0640 "${script_dir}/env/common.env.example" /etc/estuary/common.env
fi
install -o root -g estuary -m 0640 "${script_dir}/env/a.env" /etc/estuary/slots/a.env
install -o root -g estuary -m 0640 "${script_dir}/env/b.env" /etc/estuary/slots/b.env
install -o root -g estuary -m 0644 "${script_dir}/haproxy.cfg" /etc/estuary/haproxy.cfg
install -o root -g root -m 0644 "${script_dir}/systemd/estuary@.service" /etc/systemd/system/estuary@.service
install -o root -g root -m 0644 "${script_dir}/systemd/estuary-haproxy.service" /etc/systemd/system/estuary-haproxy.service

systemctl daemon-reload
systemctl enable --now estuary@a.service estuary@b.service
systemctl enable --now estuary-haproxy.service

echo "Estuary ${version} is installed. Public: :8080, admin: 127.0.0.1:9090"
