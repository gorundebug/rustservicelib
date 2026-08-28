FROM rust:1.97-bookworm AS toolchain

ARG DEPENDENCY_APT_DEBIAN_URL=
ARG DEPENDENCY_APT_DEBIAN_SECURITY_URL=
RUN if [ -n "$DEPENDENCY_APT_DEBIAN_URL$DEPENDENCY_APT_DEBIAN_SECURITY_URL" ]; then \
      find /etc/apt -type f \( -name '*.list' -o -name '*.sources' \) -exec sed -i \
        -e "s|http://deb.debian.org/debian-security|$DEPENDENCY_APT_DEBIAN_SECURITY_URL|g" \
        -e "s|http://deb.debian.org/debian|$DEPENDENCY_APT_DEBIAN_URL|g" {} +; \
    fi
RUN apt-get update \
    && apt-get install --yes --no-install-recommends cmake \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

ARG CARGO_REGISTRIES_CRATES_IO_INDEX=sparse+https://index.crates.io/
ARG DEPENDENCY_GIT_MIRROR_URL=
RUN if [ -n "$DEPENDENCY_GIT_MIRROR_URL" ]; then \
      git config --global \
        "url.${DEPENDENCY_GIT_MIRROR_URL}/github.com/.insteadOf" \
        "https://github.com/"; \
      git config --global \
        "url.${DEPENDENCY_GIT_MIRROR_URL}/gitlab.com/.insteadOf" \
        "https://gitlab.com/"; \
    fi
ENV CARGO_REGISTRIES_CRATES_IO_INDEX=${CARGO_REGISTRIES_CRATES_IO_INDEX}

FROM toolchain AS source
COPY . .
RUN ./scripts/check_api.sh

FROM source AS test
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/workspace/target \
    cargo test --locked --all-targets

FROM source AS lint
RUN rustup component add clippy
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/workspace/target \
    cargo clippy --locked --all-targets -- -D warnings -A clippy::type-complexity
