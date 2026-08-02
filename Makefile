# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

NOKV_MANIFEST := Cargo.toml

.PHONY: help build test fmt lint workbench-test verify clean

help:
	@echo "NoKV development commands:"
	@echo ""
	@echo "  make build       - Build the Rust workspace"
	@echo "  make test        - Run the Rust workspace tests"
	@echo "  make fmt         - Format the Rust workspace"
	@echo "  make lint        - Run cargo clippy"
	@echo "  make workbench-test - Run Workbench contract and LingTai first-client tests"
	@echo "  make verify      - Run Rust and Workbench validation"
	@echo "  make clean       - Remove build artifacts"

build:
	cargo build --manifest-path $(NOKV_MANIFEST) --workspace

test:
	cargo test --manifest-path $(NOKV_MANIFEST) --workspace
	cargo test --manifest-path $(NOKV_MANIFEST) -p nokv-meta --features metadata-read-stats
	cargo test --manifest-path $(NOKV_MANIFEST) -p nokv-bench --features metadata-read-stats

fmt:
	cargo fmt --manifest-path $(NOKV_MANIFEST) --all

lint:
	cargo clippy --manifest-path $(NOKV_MANIFEST) --workspace --all-targets -- -D warnings
	cargo clippy --manifest-path $(NOKV_MANIFEST) -p nokv-meta -p nokv-bench --all-targets --features metadata-read-stats -- -D warnings

workbench-test:
	python3 scripts/lingtai-workbench/workbench_contract_test.py
	python3 scripts/lingtai-workbench/live_first_client_test.py

verify:
	cargo fmt --manifest-path $(NOKV_MANIFEST) --all -- --check
	cargo clippy --manifest-path $(NOKV_MANIFEST) --workspace --all-targets -- -D warnings
	cargo clippy --manifest-path $(NOKV_MANIFEST) -p nokv-meta -p nokv-bench --all-targets --features metadata-read-stats -- -D warnings
	cargo test --manifest-path $(NOKV_MANIFEST) --workspace
	cargo test --manifest-path $(NOKV_MANIFEST) -p nokv-meta --features metadata-read-stats
	cargo test --manifest-path $(NOKV_MANIFEST) -p nokv-bench --features metadata-read-stats
	python3 scripts/lingtai-workbench/workbench_contract_test.py
	python3 scripts/lingtai-workbench/live_first_client_test.py
	git diff --check

clean:
	rm -rf target
