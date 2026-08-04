<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

<div align="center">
  <img src="./docs/public/img/logo.png" width="320" alt="NoKV" />

  <h3>Durable agent workspaces.</h3>

  <p>
    NoKV is an Agent-native distributed workspace and artifact store. It
    publishes crash-consistent, versioned workspace state over immutable
    S3-compatible artifacts, with ordered shard-local metadata in Holt.
  </p>

  <p>
    <a href="https://nokv.io/"><strong>Website</strong></a> ·
    <a href="./docs/index.md"><strong>Documentation</strong></a> ·
    <a href="#quick-start"><strong>Quick Start</strong></a> ·
    <a href="https://github.com/orgs/NoKV-Lab/discussions"><strong>Discussions</strong></a> ·
    <a href="#contributing"><strong>Contributing</strong></a>
  </p>
</div>

## Our building partners include:

| Partner | Project |
| --- | --- |
| **OpenViking** | [Website](https://openviking.ai/) · [GitHub](https://github.com/volcengine/OpenViking) · ![OpenViking stars](https://img.shields.io/github/stars/volcengine/OpenViking?style=flat&label=stars) |
| **Hermes Agent** | [GitHub](https://github.com/NousResearch/hermes-agent) · ![Hermes Agent stars](https://img.shields.io/github/stars/NousResearch/hermes-agent?style=flat&label=stars) |
| **LingTai** | [Website](https://lingtai.ai/en/) · [GitHub](https://github.com/Lingtai-AI/lingtai) · ![LingTai stars](https://img.shields.io/github/stars/Lingtai-AI/lingtai?style=flat&label=stars) |
| **LoopX** | [GitHub](https://github.com/huangruiteng/loopx) · ![LoopX stars](https://img.shields.io/github/stars/huangruiteng/loopx?style=flat&label=stars) |
| **heima** | [GitHub](https://github.com/litentry/heima) · ![heima stars](https://img.shields.io/github/stars/litentry/heima?style=flat&label=stars) |

Building-partner status denotes an active collaboration. It does not by itself
imply a production deployment, support SLA, or completed enterprise
qualification.

## Recognition

<table>
  <tr>
    <td align="center" width="120">
      <a href="https://landscape.cncf.io/?group=projects-and-products&item=runtime--cloud-native-storage--nokv">
        <img src="./docs/public/img/recognition/cncf.svg" width="56" alt="CNCF Landscape" />
      </a>
    </td>
    <td>
      <strong>CNCF Landscape</strong><br/>
      Listed in the CNCF Landscape.
    </td>
  </tr>
  <tr>
    <td align="center" width="120">
      <a href="https://dbdb.io/db/nokv">
        <img src="./docs/public/img/recognition/dbdb.svg" width="56" alt="DBDB.io" />
      </a>
    </td>
    <td>
      <strong>DBDB.io</strong><br/>
      NoKV system profile on DBDB.io.
    </td>
  </tr>
</table>

## What NoKV Owns

Agent runs produce datasets, scripts, logs, outputs, checkpoints, reports, and
provenance across files and object keys. NoKV gives that state one path-shaped
workspace and owns the metadata needed to publish, inspect, query, snapshot,
commit, restore, retain, and collect it.

```text
Agent / Workbench / SDK / custom CLI / MCP
                     |
                     v
          NoKV workspace service
 full-path namespace, versions, snapshots,
 commits, query indexes, retention, and GC
                     |
          +----------+----------+
          |                     |
          v                     v
   shard-local Holt     S3-compatible storage
   metadata truth       immutable artifact bytes
```

NoKV owns namespace truth, shard-local metadata transactions, versioned body
descriptors, snapshots, commits, typed change events, query indexes, restore
operations, and object-reference GC policy. The object provider owns the
physical durability, replication, availability, and access policy of artifact
bytes.

NoKV is not a semantic-memory database or an Agent orchestrator. Context
retrieval, ranking, planning, validation, and runtime policy stay above the
storage layer. FUSE, POSIX emulation, CSI, transparent fsspec access, and a
general NAS replacement are outside the product architecture.

## Workspace Guarantees

- **Atomic publication.** Artifact bytes are uploaded first; one bounded Holt
  command makes the new metadata generation visible last.
- **Canonical path reads.**
  `PathCurrent(root, workspace_incarnation, normalized_relative_path)` is the
  only namespace truth, and directories are implicit prefixes.
- **Immutable bodies.** Every published body has a never-reused
  `ArtifactRevisionId`, immutable revision-owned blocks, and a whole-body
  digest.
- **Stable historical reads.** Leased MVCC snapshots hold a consistent read
  version for short-lived recovery and inspection.
- **Durable reuse.** Sealed commits and tags retain exact artifact revisions
  without pinning the global history floor.
- **Safe restore.** A restore reuses immutable revisions in a fresh hidden
  same-root incarnation, then publishes visibility atomically.
- **Deterministic Agent surface.** The same exact 18-tool Workbench contract is
  exposed through SDK, CLI, and MCP adapters.

These guarantees are shard-local. NoKV does not provide cross-shard
transactions. Snapshot protection is leased, and root or Workbench scoping is
not an authentication or RBAC boundary.

## Distributed Status

| Status | Capabilities and limits |
| --- | --- |
| **Current product** | Persisted `RootId -> LogicalShardId` affinity; one epoch-fenced active owner per shard; canonical full-path Holt keys; immutable S3-compatible bodies; Rust and Python SDKs; custom CLI; MCP; exact 18-tool Workbench; snapshots, commits, restore, queries, and reference-fenced GC |
| **Current durability profile** | Acknowledged metadata writes are synchronously durable in the owning shard's local Holt WAL. Each mutation also appends canonical hash-chained replay material in the same store. First-owner creation and exact current-lease resume are admitted; unknown, mixed, or unverified successor stores fail closed. |
| **Not qualified** | Remote checkpoint/log recovery, shared metadata durability, multi-machine failover, production metadata HA, tenant identity/RBAC, cross-shard transactions, and complete provider fault-injection qualification |

Root placement is persisted control-plane state. NoKV never derives shard
placement from a filename, path prefix, or the current shard count, so a
populated root remains on its logical shard. Holt layout remains internal and
never leaks into the SDK or Workbench contract.

See the [architecture](docs/architecture.md),
[metadata schema](docs/metadata-schema.md), and
[workspace acceptance checklist](docs/development/workspace-acceptance.md) for
the exact contracts and qualification gates.

## Interfaces

- **Rust Agent SDK** through [`nokv-client`](crates/nokv-client).
- **Python Agent SDK** through [`nokv-python`](crates/nokv-python), including
  explicit materialize/collect adapters for local executables.
- **Custom `nokv` CLI** with `workbench`, `mcp`, `materialize`, `collect`,
  `provision`, `serve`, and `schema` commands.
- **Native MCP over stdio** exposing the exact 18 Workbench tools.
- **Transport-free Agent contracts** in
  [`nokv-agent`](crates/nokv-agent), shared by every adapter.

RootId is the only storage and routing identity. A Workbench presentation root
shapes Agent-facing paths and manifests but never enters Holt keys.

## Stable Workbench

NoKV exposes exactly these 18 tools:

```text
workbench_create
workbench_put_file
workbench_append
workbench_edit
workbench_list
workbench_stat
workbench_read
workbench_grep
workbench_search
workbench_aggregate
workbench_catalog
workbench_find
workbench_commit
workbench_snapshot
workbench_snapshot_renew
workbench_snapshot_retire
workbench_snapshot_list
workbench_restore
```

Tool names, normalized input schemas, create/replace semantics, generation and
digest relationships, commit identity, snapshot lifecycle, and restore
idempotency form the stable contract. Workbench result shaping remains an
adapter concern and does not dictate durable metadata families.

See the [Workbench Contract](docs/workbench-contract.md).

## First Client

LingTai is the active design partner and first Workbench client. Its scientific
reconstruction workflow exercises the product boundary end to end:

```text
upload dataset
  -> seal immutable input commit/tag
  -> run multiple Workbenches against the same input
  -> materialize verified files for a local executable
  -> collect declared outputs, logs, and run metadata
  -> commit lineage
  -> query, compare, snapshot, and restore
```

Materialization creates a disposable local sandbox. It is not a NoKV namespace
or a transparent host-filesystem access path.

## Quick Start

Build the custom CLI and inspect the checked-in Workbench schema:

```bash
cargo build --release -p nokv --bin nokv
./target/release/nokv --help
./target/release/nokv schema
```

Run the offline Workbench contract gate:

```bash
python3 scripts/lingtai-workbench/workbench_contract_test.py
```

A live deployment additionally needs a root id, persisted logical-shard
placement, one admitted shard owner, and S3-compatible object coordinates. The
[LingTai setup and preflight guide](docs/lingtai-workbench-preflight.md) gives
the complete provision, serve, MCP, materialize, collect, and acceptance flow
without hiding the current recovery limitations.

## Documentation

- [Documentation Index](docs/index.md)
- [Product Design](docs/product-design.md)
- [Architecture](docs/architecture.md)
- [Workbench Contract](docs/workbench-contract.md)
- [Metadata Schema](docs/metadata-schema.md)
- [Object Layout](docs/object-layout.md)
- [Benchmarks and Evidence](docs/benchmarks.md)
- [Workspace Acceptance](docs/development/workspace-acceptance.md)
- [Path-Native Metadata Comparison](docs/development/path-native-metadata-comparison.md)
- [Agent Contributor Handbook](docs/development/nokv-agent.md)
- [Code Contract](docs/development/code_contract.md)
- [PR Review Checklist](docs/development/pr_review_checklist.md)
- [LingTai Workbench Setup](docs/lingtai-workbench-preflight.md)

## Contributing

Contributions are welcome. Read [CONTRIBUTING.md](CONTRIBUTING.md) and the
[code contract](docs/development/code_contract.md) before changing package
boundaries or durable storage semantics. Open work suitable for newcomers is
listed under the dynamic
[good first issue query](https://github.com/NoKV-Lab/NoKV/issues?q=is%3Aissue%20state%3Aopen%20label%3A%22good%20first%20issue%22).

All commits must include a DCO `Signed-off-by` trailer.

Before pushing a substantial change, run:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
python3 scripts/lingtai-workbench/workbench_contract_test.py
git diff --check
```

## Crates

| Crate | Role |
| --- | --- |
| [`nokv-types`](crates/nokv-types) | Storage-neutral Agent workspace domain types |
| [`nokv-protocol`](crates/nokv-protocol) | Versioned metadata and lifecycle RPC DTOs and framing |
| [`nokv-meta`](crates/nokv-meta) | Workspace schema, commands, Holt binding, history, indexes, commits, snapshots, restore, and GC |
| [`nokv-control`](crates/nokv-control) | Persisted root placement, shard ownership, epoch fencing, and recovery coordination |
| [`nokv-object`](crates/nokv-object) | Immutable S3-compatible artifact storage and local hot tier |
| [`nokv-client`](crates/nokv-client) | Root-routed Rust Agent SDK and direct immutable-object data path |
| [`nokv-agent`](crates/nokv-agent) | Transport-free 18-tool Workbench facade and stable result shaping |
| [`nokv-python`](crates/nokv-python) | Direct Python SDK and explicit materialize/collect adapters |
| [`nokv-server`](crates/nokv-server) | Root-affine shard-owner RPC server and lifecycle workers |
| [`nokv`](crates/nokv) | Thin custom CLI and MCP wiring |
| [`nokv-bench`](bench) | Non-product contract, recovery, and performance workloads |

## License

Apache-2.0. See [LICENSE](LICENSE).
