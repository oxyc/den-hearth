#!/usr/bin/env bash
set -euo pipefail

dockerfile=${1:-Dockerfile}
install_dir=${2:?usage: install-pinned-deno.sh Dockerfile INSTALL_DIR}
version=$(grep -oP '^ARG DENO_VERSION=\K\S+' "$dockerfile")
sha=$(grep -oP '^ARG DENO_SHA256_AMD64=\K\S+' "$dockerfile")
archive=${RUNNER_TEMP:-${TMPDIR:-/tmp}}/deno-${version}.zip
curl -fsSL "https://github.com/denoland/deno/releases/download/v${version}/deno-x86_64-unknown-linux-gnu.zip" -o "$archive"
echo "${sha}  ${archive}" | sha256sum -c -
mkdir -p "$install_dir"
unzip -q -o -d "$install_dir" "$archive"
rm "$archive"
