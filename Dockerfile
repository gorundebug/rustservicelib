FROM rust:1.97-bookworm AS toolchain

RUN apt-get update \
    && apt-get install --yes --no-install-recommends cmake \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

FROM toolchain AS source
COPY . .
RUN ./scripts/check_api.sh

FROM source AS test
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/workspace/target \
    cargo test --all-targets

FROM source AS lint
RUN rustup component add clippy
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/workspace/target \
    cargo clippy --all-targets -- -D warnings -A clippy::type-complexity
