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
    S3-compatible artifacts. `TxnStore` persists ordered shard-local metadata.
    Holt is the current serving local adapter.
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
 MetaShard + TxnStore   S3-compatible storage
  metadata truth       immutable artifact bytes
 (local: HoltStore)
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

- **Atomic publication.** NoKV uploads artifact bytes first. One bounded
  metadata command makes the new generation visible last.
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

## Provenance and Evidence, Without a SQL Sidecar

Run provenance is the workload NoKV is shaped for. The default answer — a
relational database next to the object store — rebuilds everything the
workspace already owns: tables of paths, hand-rolled versioning, a second
GC, and a two-phase dance to keep rows and S3 bytes agreeing.

Agent evidence is path-shaped, append-heavy, and immutable once sealed.
NoKV stores it with the guarantees above, plus:

- **Tamper-evident artifacts.** Never-reused revision ids and whole-body
  digests on every published body; commits bind a caller-computed content
  digest and replay idempotently.
- **Hash-chained history.** Every acknowledged metadata mutation is
  synchronously durable in the owning shard's WAL and appends canonical
  hash-chained replay material in the same store.
- **Crash-tested durability.** The metadata engine's checkpoint rewrites
  are shadow-paged and power-loss tested down to torn 512 KiB frames
  (holt >= 0.8.3); a torn-frame guard probe pins that property in this
  repository's own test suite.
- **Lineage as data.** Sealed commits and tags retain exact revisions,
  leased snapshots freeze a consistent view, and restore forks it without
  rewriting history.
- **Evidence layout.** A Workbench keeps `logs/` as the tool-call evidence
  stream and `metadata/` as run manifests, next to the inputs and outputs
  they describe — one namespace, one query surface, one GC.

Reach for SQL when you need relational reporting across many runs. Storing
the evidence itself — files, manifests, lineage, and the bytes they attest
— belongs in the workspace that publishes them atomically.

## Distributed Status

| Status | Capabilities and limits |
| --- | --- |
| **Current product** | Persisted `RootId -> LogicalShardId` affinity; one epoch-fenced active owner per shard; canonical full-path metadata keys; immutable S3-compatible bodies; Rust and Python SDKs; custom CLI; MCP; exact 18-tool Workbench; snapshots, commits, restore, queries, and reference-fenced GC |
| **Current durability profile** | Acknowledged metadata writes are synchronously durable in the owning shard's local Holt WAL. Each mutation also appends canonical hash-chained replay material in the same store. First-owner acquisition accepts a new or prepared epoch-zero store. Exact current-lease resume is also admitted. Unknown, mixed, or unverified successor stores fail closed. |
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
shapes Agent-facing paths and manifests but never enters canonical metadata
keys.

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

## Integration Model

The Workbench contract is runtime-neutral. Any MCP-compatible Agent runtime can
exercise the product boundary end to end through the same scientific
reconstruction workflow:

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

LingTai is the active design partner and first integrated client, but it uses
this same public boundary rather than a partner-specific NoKV route.

## Use Case: A Research Workbench for Agents

An Agent runtime uses NoKV as the durable artifact store behind its research
agents. Runtime state — locks, heartbeats, mailboxes, and event logs — can stay
in a disposable local workdir; what a task produces and needs to prove crosses
into a Workbench.

One MCP registry entry is the whole integration. Each agent spawns the `nokv`
binary as a stdio MCP server:

```text
nokv ... --workbench-root /agents/{agent_id}/wb mcp
```

The runtime expands `{agent_id}` per agent, so a single registry template gives
every agent its own path-scoped Workbench root, and the 18 `workbench_*`
tools land next to the agent's local file tools instead of replacing them.
There is no client library and no NoKV-specific client code beyond that
placeholder expansion; the whole contract lives server-side.

A research run then follows the fixed section layout — `input`, `scripts`,
`outputs`, `logs`, `metadata`:

```text
workbench_create spedas-task-001
  put    input/    task payload and dataset references
  put    scripts/  the exact analysis code a rerun needs
  append logs/     tool-call evidence while the run executes
  put    outputs/  figures, tables, reports
  workbench_commit
    -> seals the run, writes metadata/run_manifest.json,
       binds the caller-computed content digest
  workbench_snapshot        (leased checkpoint, default 7 days)
  workbench_restore
    -> forks the frozen view into a fresh Workbench
       for handoff or replay
```

The write semantics are collaboration discipline, not convenience:
`workbench_put_file` is create-only or replace-only, never upsert, and
appends serialize server-side. A parent agent creates the Workbench, assigns
paths, and commits; spawned child agents write only the paths they were
assigned. When a local executable needs real files, `materialize` copies
verified inputs into the sandbox and `collect` brings declared outputs back.

What this buys an agent fleet: a task's artifacts, provenance, and history
outlive any single context window, and a leased snapshot plus restore is how
an agent — or its successor — finds its work again after a context reset.

## Quick Start

Build the custom CLI and inspect the checked-in Workbench schema:

```bash
cargo build --release -p nokv --bin nokv
./target/release/nokv --help
./target/release/nokv schema
```

Maintainers and integration partners with access to the private tap can instead
build the same locked source release through Homebrew:

```bash
brew tap NoKV-Lab/tap
brew install nokv
nokv version --json
nokv schema
```

Run the offline Workbench contract gate:

```bash
python3 scripts/workbench/workbench_contract_test.py
```

A live deployment additionally needs a root id, persisted logical-shard
placement, one admitted shard owner, and S3-compatible object coordinates. The
[Workbench deployment preflight](docs/workbench-preflight.md) gives
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
- [Workbench Deployment Preflight](docs/workbench-preflight.md)
- [Source-only Homebrew Release](scripts/release/README.md)

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
python3 scripts/workbench/workbench_contract_test.py
git diff --check
```

## Crates

| Crate | Role |
| --- | --- |
| [`nokv-types`](crates/nokv-types) | Storage-neutral Agent workspace domain types |
| [`nokv-protocol`](crates/nokv-protocol) | Versioned metadata and lifecycle RPC DTOs and framing |
| [`nokv-meta-store`](crates/nokv-meta-store) | Storage-neutral ordered metadata transaction contract and conformance suite |
| [`nokv-meta-holt`](crates/nokv-meta-holt) | Serving local Holt adapter, strict open/reopen, recovery, and physical diagnostics |
| [`nokv-meta`](crates/nokv-meta) | Workspace schema, commands, history, indexes, commits, snapshots, restore, and GC over `TxnStore` |
| [`nokv-control`](crates/nokv-control) | Persisted root placement, shard ownership, epoch fencing, and recovery coordination |
| [`nokv-object`](crates/nokv-object) | Immutable S3-compatible artifact storage and local hot tier |
| [`nokv-client`](crates/nokv-client) | Root-routed Rust Agent SDK and direct immutable-object data path |
| [`nokv-agent`](crates/nokv-agent) | Transport-free 18-tool Workbench facade and stable result shaping |
| [`nokv-python`](crates/nokv-python) | Direct Python SDK and explicit materialize/collect adapters |
| [`nokv-server`](crates/nokv-server) | Logical-shard owner, metadata adapter composition, RPC server, and root-affine lifecycle workers |
| [`nokv`](crates/nokv) | Thin custom CLI and MCP wiring |
| [`nokv-bench`](bench) | Non-product contract, recovery, and performance workloads |

## License

Apache-2.0. See [LICENSE](LICENSE).
