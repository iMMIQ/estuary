#!/usr/bin/env bash

set -euo pipefail

target="${1:?usage: install-rust-target.sh <target-triple>}"
dist_server="${RUSTUP_DIST_SERVER:-https://mirrors.ustc.edu.cn/rust-static}"
sysroot="$(rustc --print sysroot)"
target_libdir="$sysroot/lib/rustlib/$target/lib"

if find "$target_libdir" -maxdepth 1 -name 'libstd-*.rlib' -print -quit 2>/dev/null | grep -q .; then
  echo "Rust target $target is already installed"
  exit 0
fi

release="$(rustc -Vv | sed -n 's/^release: //p')"
release_date="$(rustc -Vv | sed -n 's/^commit-date: //p')"
if [[ -z "$release" || -z "$release_date" ]]; then
  echo "Unable to determine the active Rust release and commit date" >&2
  exit 1
fi

archive="rust-std-${release}-${target}.tar.xz"
url="${dist_server%/}/dist/${release_date}/${archive}"
temp_dir="$(mktemp -d)"
trap 'rm -rf "$temp_dir"' EXIT

rustup_version="$(rustup -V | awk '{print $2}')"
user_agent="rustup/${rustup_version} (Linux; $(uname -m))"
curl_args=(
  --fail
  --location
  --silent
  --show-error
  --retry 5
  --retry-all-errors
  --retry-delay 2
  --connect-timeout 20
  --max-time 300
  --user-agent "$user_agent"
)

echo "Downloading $target standard library from $dist_server"
curl "${curl_args[@]}" --output "$temp_dir/$archive" "$url"
curl "${curl_args[@]}" --output "$temp_dir/$archive.sha256" "$url.sha256"

(
  cd "$temp_dir"
  sha256sum --check "$archive.sha256"
)

tar -xJf "$temp_dir/$archive" -C "$temp_dir"
bash "$temp_dir/rust-std-${release}-${target}/install.sh" \
  --prefix="$sysroot" \
  --disable-ldconfig

if ! find "$target_libdir" -maxdepth 1 -name 'libstd-*.rlib' -print -quit | grep -q .; then
  echo "Rust target $target was not installed into $sysroot" >&2
  exit 1
fi
