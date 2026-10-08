#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

if [ -n "${ESTUARY_BUILD_VERSION:-}" ]; then
    version=${ESTUARY_BUILD_VERSION}
else
    base_version=$(sed -n '/^\[package\]/,/^\[/s/^version = "\([^"]*\)"/\1/p' "${root}/Cargo.toml")
    if [ -z "${base_version}" ]; then
        echo 'cannot read package version from Cargo.toml' >&2
        exit 1
    fi
    if git -C "${root}" tag --points-at HEAD --list "v${base_version}" 2>/dev/null \
        | grep -Fxq "v${base_version}"; then
        version=${base_version}
    else
        commit=$(git -C "${root}" rev-parse --short=12 HEAD 2>/dev/null || printf unknown)
        version=${base_version}+${commit}
    fi
fi

case ${version} in
    ''|*[!a-zA-Z0-9._+-]*)
        echo 'build version must contain only letters, digits, dots, underscores, plus signs, or hyphens' >&2
        exit 1
        ;;
esac
printf '%s\n' "${version}"
