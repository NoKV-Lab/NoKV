<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# NoKV Documentation

NoKV is a metadata control plane for agent workspaces. It publishes
crash-consistent, versioned workspace state over S3-compatible object storage
through a filesystem-shaped namespace.

The product overview and design-partner information live at
[nokv.io](https://nokv.io/). This directory is the source of truth for NoKV's
technical contracts, implementation status, and operating limits.

## Status

- **Current default:** one `nokv-server` backed by an embedded Holt metadata
  engine, with Rust SDK, CLI, FUSE, Python/fsspec, snapshots, typed watches,
  object-reference GC, and same-shard CoW workspace primitives.
- **Experimental on `main`:** longest-prefix path sharding, fleet routing,
  one active Holt-backed owner per shard, etcd lease/epoch fencing, and
  checkpoint plus logical shared-log recovery.
- **Next / hardening:** multi-machine fault qualification, enterprise small-file
  throughput validation, online resharding, Python fleet routing, metadata
  consensus replication or another production HA mechanism, tenant identity
  and policy enforcement, live-workspace freeze, and broader POSIX coverage.

Experimental path sharding is horizontal partitioning, not consensus
replication. A metadata publication is crash-atomic within its owning shard;
NoKV does not provide cross-shard transactions. Workflows that need a stable
multi-read view must pin a snapshot.

## Start Here

- [Architecture](./architecture.md) — current implementation, experimental
  fleet path, consistency boundaries, and known limits.
- [Product Design](./product-design.md) — what NoKV owns, what Holt owns, and
  the Current / Experimental / Next capability matrix.
- [RustFS Backend](./rustfs.md) — local S3-compatible development setup.
- [Contributing](../CONTRIBUTING.md) — development setup, change boundaries,
  and validation expectations.

## Storage and Consistency

- [Metadata Schema](./metadata-schema.md)
- [Object Layout](./object-layout.md)
- [Checkpointing](./checkpointing.md)
- [CoW Workspaces](./cow-workspaces.md)
- [Metadata Sharding and Recovery](./metadata-sharding-and-recovery.md)
- [Experimental Multi-Shard Fleet Runbook](./multishard-fleet-runbook.md)
- [Native Batch and Range Reads](./ai-training.md)

## Agent and Workbench Integration

- [Agent Interface Contributor Guide](./development/nokv-agent.md)
- [LingTai Workbench Setup and Preflight](./lingtai-workbench-preflight.md)
- [Workbench Checkpoint Lifecycle](./development/workbench-checkpoint-lifecycle.md)
- [LingTai Workbench Scripts](../scripts/lingtai-workbench/README.md)

The generic `agent` MCP profile is a read-only seven-tool surface. The separate
Workbench profile exposes 17 base tools and conditionally adds restore as an
eighteenth tool when the configured owners advertise that capability. Path
scoping is not authentication, authorization, or a security-grade tenant
boundary.

## Performance and Evidence

- [Benchmark Guide](./benchmarks.md)
- [Historical Agent-Interface Benchmark](../bench/agent-interface/README.md)

Benchmark claims must identify the tested commit, topology, object backend,
cache state, durability mode, workload, and raw result artifact. Current
multi-process sharding smoke tests validate selected routing and recovery
contracts; they are not evidence of enterprise multi-machine throughput.

## Development Contracts

- [Code Contract](./development/code_contract.md)
- [PR Review Checklist](./development/pr_review_checklist.md)
- [Security Policy](../SECURITY.md)

## Historical Material

Documents under `design-history/` record earlier explorations. They are not
current product descriptions, support statements, or roadmap commitments.

- [Historical AI-Infra Storage Architecture](./design-history/ai-infra-architecture.md)
