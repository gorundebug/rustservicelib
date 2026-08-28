#!/usr/bin/env bash

if [[ -n "${DEPENDENCY_PROXY_DIR:-}" ]]; then
  dependency_proxy_docker_host="${DEPENDENCY_PROXY_DOCKER_HOST:-host.docker.internal}"
  dependency_proxy_port="${DEPENDENCY_PROXY_PORT:-18081}"
  dependency_git_mirror_port="${DEPENDENCY_GIT_MIRROR_PORT:-18084}"
  dependency_proxy_base="http://${dependency_proxy_docker_host}:${dependency_proxy_port}/repository"
  dependency_git_mirror="http://${dependency_proxy_docker_host}:${dependency_git_mirror_port}/cgi-bin/git"
  dependency_registry_host="${DEPENDENCY_PROXY_HOST:-localhost}"
  dependency_registry_port="${DEPENDENCY_PROXY_DOCKER_PORT:-18083}"

  export DEPENDENCY_DOCKER_REGISTRY="${dependency_registry_host}:${dependency_registry_port}"
  export CARGO_REGISTRIES_CRATES_IO_INDEX="sparse+${dependency_proxy_base}/cargo-proxy/"
  export DEPENDENCY_APT_DEBIAN_URL="${dependency_proxy_base}/apt-debian"
  export DEPENDENCY_APT_DEBIAN_SECURITY_URL="${dependency_proxy_base}/apt-debian-security"
  export DEPENDENCY_GIT_MIRROR_URL="${dependency_git_mirror}"
fi
