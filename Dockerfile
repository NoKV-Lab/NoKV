# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

# syntax=docker/dockerfile:1

ARG RUST_VERSION=1.88
ARG DEBIAN_VERSION=bookworm
ARG NOKV_PACKAGE=nokv
ARG NOKV_BINARY=nokv

FROM rust:${RUST_VERSION}-${DEBIAN_VERSION} AS builder
ARG NOKV_PACKAGE
ARG NOKV_BINARY
WORKDIR /workspace

RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY bench ./bench

RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/workspace/target,sharing=locked \
    cargo build --release --locked -p "${NOKV_PACKAGE}" --bin "${NOKV_BINARY}" \
    && cp "/workspace/target/release/${NOKV_BINARY}" /usr/local/bin/nokv

FROM debian:${DEBIAN_VERSION}-slim
RUN useradd --system --create-home --home-dir /var/lib/nokv nokv \
    && apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /var/lib/nokv/.nokv \
    && chown -R nokv:nokv /var/lib/nokv

COPY --from=builder /usr/local/bin/nokv /usr/local/bin/nokv

USER nokv
WORKDIR /var/lib/nokv
ENTRYPOINT ["/usr/local/bin/nokv"]
CMD ["serve"]
