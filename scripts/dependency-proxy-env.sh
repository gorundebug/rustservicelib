#!/usr/bin/env bash

if [[ -n "${SERVICEGEN_DEPENDENCY_PROXY_DIR:-}" ]]; then
  servicegen_proxy_docker_host="${SERVICEGEN_DEPENDENCY_PROXY_DOCKER_HOST:-host.docker.internal}"
  servicegen_proxy_port="${SERVICEGEN_DEPENDENCY_PROXY_PORT:-${SERVICEGEN_NEXUS_PORT:-18081}}"
  servicegen_git_mirror_port="${SERVICEGEN_GIT_MIRROR_PORT:-18084}"
  servicegen_proxy_base="http://${servicegen_proxy_docker_host}:${servicegen_proxy_port}/repository"
  servicegen_git_mirror="http://${servicegen_proxy_docker_host}:${servicegen_git_mirror_port}/cgi-bin/git"

  export CARGO_REGISTRIES_CRATES_IO_INDEX="sparse+${servicegen_proxy_base}/cargo-proxy/"
  export SERVICEGEN_APT_DEBIAN_URL="${servicegen_proxy_base}/apt-debian"
  export SERVICEGEN_APT_DEBIAN_SECURITY_URL="${servicegen_proxy_base}/apt-debian-security"
  export SERVICEGEN_GIT_MIRROR_URL="${servicegen_git_mirror}"
fi
