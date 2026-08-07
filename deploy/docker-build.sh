#!/usr/bin/env bash
set -euo pipefail

root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
results=$(mktemp -d)
trap 'rm -rf -- "${results}"' EXIT
case $(uname -m) in
    x86_64) alpine_arch=x86_64 ;;
    aarch64 | arm64) alpine_arch=aarch64 ;;
    *) echo "unsupported build architecture: $(uname -m)" >&2; exit 1 ;;
esac

probe_http() {
    local group=$1 name=$2 url=$3 value=$4 start elapsed
    start=$(date +%s%3N)
    if curl --fail --location --silent \
        --connect-timeout 3 --max-time 10 --output /dev/null "${url}"; then
        elapsed=$(($(date +%s%3N) - start))
        printf '%s\t%s\t%s\n' "${elapsed}" "${name}" "${value}" \
            > "${results}/${group}-${name}"
    fi
}

probe_apk() {
    local name=$1 base=$2 start elapsed repository
    start=$(date +%s%3N)
    for repository in main community; do
        curl --fail --location --silent --connect-timeout 3 --max-time 10 \
            --output /dev/null \
            "${base}/v3.21/${repository}/${alpine_arch}/APKINDEX.tar.gz" || return 0
    done
    elapsed=$(($(date +%s%3N) - start))
    printf '%s\t%s\t%s\n' "${elapsed}" "${name}" "${base}" \
        > "${results}/apk-${name}"
}

probe_docker() {
    local name=$1 prefix=$2 start elapsed code image
    start=$(date +%s%3N)
    for image in library/alpine:3.21 library/rust:1.85-alpine3.21 oven/bun:1.3.14-alpine; do
        code=$(curl --location --silent --output /dev/null --write-out '%{http_code}' \
            --connect-timeout 3 --max-time 8 \
            --header 'Accept: application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json' \
            "https://${prefix}/v2/${image/:/\/manifests/}") || return 0
        if [[ ${name} == official ]]; then
            [[ ${code} == 200 || ${code} == 401 ]] || return 0
        else
            [[ ${code} == 200 ]] || return 0
        fi
    done
    elapsed=$(($(date +%s%3N) - start))
    printf '%s\t%s\t%s\n' "${elapsed}" "${name}" "${prefix}" \
        > "${results}/docker-${name}"
}

fastest() {
    local group=$1 fallback=$2 match
    match=$(find "${results}" -type f -name "${group}-*" -exec cat {} + 2>/dev/null \
        | sort -n | head -n 1 || true)
    if [[ -z ${match} ]]; then
        printf '%s' "${fallback}"
        return
    fi
    printf '%s' "${match}" | cut -f3
    printf '  %-7s %-10s %sms\n' "${group}" "$(printf '%s' "${match}" | cut -f2)" \
        "$(printf '%s' "${match}" | cut -f1)" >&2
}

probe_docker official docker.io &
probe_docker daocloud docker.m.daocloud.io &
probe_docker one_ms docker.1ms.run &

probe_apk official https://dl-cdn.alpinelinux.org/alpine &
probe_apk aliyun https://mirrors.aliyun.com/alpine &
probe_apk ustc https://mirrors.ustc.edu.cn/alpine &

probe_http cargo official https://index.crates.io/config.json '' &
probe_http cargo rsproxy https://rsproxy.cn/index/config.json \
    sparse+https://rsproxy.cn/index/ &
probe_http cargo ustc https://mirrors.ustc.edu.cn/crates.io-index/config.json \
    sparse+https://mirrors.ustc.edu.cn/crates.io-index/ &

probe_http npm official https://registry.npmjs.org/react/latest \
    https://registry.npmjs.org &
probe_http npm npmmirror https://registry.npmmirror.com/react/latest \
    https://registry.npmmirror.com &
probe_http npm huawei https://repo.huaweicloud.com/repository/npm/react/latest \
    https://repo.huaweicloud.com/repository/npm &
wait

printf 'Selected build sources:\n' >&2
docker_registry=$(fastest docker docker.io)
apk_repository=$(fastest apk https://dl-cdn.alpinelinux.org/alpine)
cargo_registry=$(fastest cargo '')
npm_registry=$(fastest npm https://registry.npmjs.org)

cd "${root}"
exec docker build --network host --tag "${ESTUARY_IMAGE:-estuary:local}" \
    --build-arg "ALPINE_IMAGE=${docker_registry}/library/alpine:3.21" \
    --build-arg "RUST_IMAGE=${docker_registry}/library/rust:1.85-alpine3.21" \
    --build-arg "BUN_IMAGE=${docker_registry}/oven/bun:1.3.14-alpine" \
    --build-arg "APK_REPOSITORY=${apk_repository}" \
    --build-arg "CARGO_REGISTRY=${cargo_registry}" \
    --build-arg "NPM_REGISTRY=${npm_registry}" \
    "$@" "${root}"
