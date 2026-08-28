#!/usr/bin/env bash
set -euo pipefail

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
source "$root/scripts/dependency-proxy-env.sh"
"$root/scripts/check_api.sh"
docker build \
    --add-host "host.docker.internal:host-gateway" \
    --build-arg "CARGO_REGISTRIES_CRATES_IO_INDEX=${CARGO_REGISTRIES_CRATES_IO_INDEX:-sparse+https://index.crates.io/}" \
    --build-arg "DEPENDENCY_APT_DEBIAN_URL=${DEPENDENCY_APT_DEBIAN_URL:-}" \
    --build-arg "DEPENDENCY_APT_DEBIAN_SECURITY_URL=${DEPENDENCY_APT_DEBIAN_SECURITY_URL:-}" \
    --build-arg "DEPENDENCY_GIT_MIRROR_URL=${DEPENDENCY_GIT_MIRROR_URL:-}" \
    --target test --tag rustservicelib-test .
