# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

# syntax=docker/dockerfile:1

FROM --platform=$BUILDPLATFORM golang:1.26 AS builder
WORKDIR /workspace
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download
COPY . .
# BuildKit provides TARGETOS/TARGETARCH from the requested image platform.
# Do not default TARGETARCH to amd64: on Apple Silicon that silently builds
# x86-64 binaries into an arm64 image and runs them through emulation.
ARG TARGETOS
ARG TARGETARCH
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS:-$(go env GOOS)} GOARCH=${TARGETARCH:-$(go env GOARCH)} go build -o /out/nokv ./cmd/nokv
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS:-$(go env GOOS)} GOARCH=${TARGETARCH:-$(go env GOARCH)} go build -o /out/nokv-config ./cmd/nokv-config
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS:-$(go env GOOS)} GOARCH=${TARGETARCH:-$(go env GOARCH)} go build -o /out/nokv-fsmeta ./cmd/nokv-fsmeta

FROM debian:bookworm-slim
RUN useradd --system --create-home --home-dir /var/lib/nokv nokv \
    && apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /out/nokv /usr/local/bin/nokv
COPY --from=builder /out/nokv-config /usr/local/bin/nokv-config
COPY --from=builder /out/nokv-fsmeta /usr/local/bin/nokv-fsmeta
COPY scripts /usr/local/lib/nokv-scripts
# Wrappers instead of symlinks: the scripts resolve $SCRIPT_DIR from
# $BASH_SOURCE[0], which with a symlink points at /usr/local/bin and breaks
# the sibling `../lib/common.sh` source. `exec <real path>` makes the target
# script see its real location as BASH_SOURCE[0].
RUN chmod +x /usr/local/lib/nokv-scripts/ops/serve-store.sh /usr/local/lib/nokv-scripts/ops/bootstrap.sh \
    && printf '#!/usr/bin/env bash\nexec /usr/local/lib/nokv-scripts/ops/serve-store.sh "$@"\n' > /usr/local/bin/serve-store.sh \
    && printf '#!/usr/bin/env bash\nexec /usr/local/lib/nokv-scripts/ops/bootstrap.sh "$@"\n' > /usr/local/bin/bootstrap.sh \
    && chmod +x /usr/local/bin/serve-store.sh /usr/local/bin/bootstrap.sh \
    && mkdir -p /etc/nokv /var/lib/nokv/store /var/lib/nokv/peras-visible-log /var/lib/nokv-meta-root \
               /volumes/store-1 /volumes/store-2 /volumes/store-3 \
    && chown -R nokv:nokv /var/lib/nokv /var/lib/nokv-meta-root /volumes
COPY raft_config.example.json /etc/nokv/raft_config.json
USER nokv
WORKDIR /var/lib/nokv
ENTRYPOINT ["/usr/local/bin/nokv"]
CMD ["help"]
