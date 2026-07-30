# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

NOKV_FS_MANIFEST := Cargo.toml

.PHONY: help build test fmt lint verify clean

help:
	@echo "NoKV development commands:"
	@echo ""
	@echo "  make build       - Build the Rust workspace"
	@echo "  make test        - Run the Rust workspace tests"
	@echo "  make fmt         - Format the Rust workspace"
	@echo "  make lint        - Run cargo clippy"
	@echo "  make verify      - Run fmt check, clippy, and tests"
	@echo "  make clean       - Remove build artifacts"

build:
	cargo build --manifest-path $(NOKV_FS_MANIFEST) --workspace

test:
	cargo test --manifest-path $(NOKV_FS_MANIFEST) --workspace

fmt:
	cargo fmt --manifest-path $(NOKV_FS_MANIFEST) --all

lint:
	cargo clippy --manifest-path $(NOKV_FS_MANIFEST) --workspace --all-targets -- -D warnings

verify:
	cargo fmt --manifest-path $(NOKV_FS_MANIFEST) --all -- --check
	cargo clippy --manifest-path $(NOKV_FS_MANIFEST) --workspace --all-targets -- -D warnings
	cargo test --manifest-path $(NOKV_FS_MANIFEST) --workspace

clean:
	rm -rf target
