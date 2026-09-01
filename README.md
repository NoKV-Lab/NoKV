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
    <a href="https://github.com/NoKV-Lab/NoKV/actions/workflows/rust.yml">
      <img alt="Rust CI" src="https://github.com/NoKV-Lab/NoKV/actions/workflows/rust.yml/badge.svg?branch=main" />
    </a>
    <a href="https://github.com/NoKV-Lab/NoKV/actions/workflows/python-sdk.yml">
      <img alt="Python SDK" src="https://github.com/NoKV-Lab/NoKV/actions/workflows/python-sdk.yml/badge.svg?branch=main" />
    </a>
    <a href="https://github.com/NoKV-Lab/NoKV/actions/workflows/docker-image.yml">
      <img alt="Multi-architecture container build" src="https://img.shields.io/github/actions/workflow/status/NoKV-Lab/NoKV/docker-image.yml?branch=main&amp;label=multi-arch%20build" />
    </a>
    <a href="https://github.com/NoKV-Lab/NoKV/actions/workflows/dco.yml">
      <img alt="DCO" src="https://github.com/NoKV-Lab/NoKV/actions/workflows/dco.yml/badge.svg?branch=main" />
    </a>
    <a href="https://github.com/NoKV-Lab/NoKV/releases/latest">
      <img alt="Latest release" src="https://img.shields.io/github/v/release/NoKV-Lab/NoKV?sort=date&amp;display_name=tag&amp;label=release" />
    </a>
    <a href="https://github.com/NoKV-Lab/homebrew-tap/blob/main/Formula/nokv.rb">
      <img alt="Homebrew source release" src="https://img.shields.io/badge/Homebrew-source%20release-FBB040?logo=homebrew&amp;logoColor=black" />
    </a>
    <a href="./LICENSE">
      <img alt="Apache-2.0 license" src="https://img.shields.io/github/license/NoKV-Lab/NoKV" />
    </a>
    <a href="./SECURITY.md">
      <img alt="Security policy with private reporting" src="https://img.shields.io/badge/security-private%20reporting-2ea44f" />
    </a>
  </p>

  <p>
    <a href="https://landscape.cncf.io/?group=projects-and-products&amp;item=runtime--cloud-native-storage--nokv">
      <img alt="Listed in the CNCF Landscape" src="https://img.shields.io/badge/CNCF%20Landscape-listed-0086FF" />
    </a>
    <a href="https://landscape.lfai.foundation/?group=projects-and-products&amp;item=data--store-format--nokv">
      <img alt="Listed in the LF AI &amp; Data Landscape" src="https://img.shields.io/badge/LF%20AI%20%26%20Data%20Landscape-listed-003764" />
    </a>
    <a href="https://github.com/rust-unofficial/awesome-rust#database">
      <img alt="Listed in Awesome Rust" src="https://img.shields.io/badge/Awesome%20Rust-listed-DEA584?logo=rust&amp;logoColor=white" />
    </a>
    <a href="https://dbdb.io/db/nokv">
      <img alt="Historical DBDB.io profile" src="https://img.shields.io/badge/DBDB.io-historical%20profile-244A64" />
    </a>
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

<table>
  <tr>
    <th align="left" width="190">Partner</th>
    <th align="left" width="330">Project</th>
    <th align="center" width="150">Stars</th>
  </tr>
  <tr>
    <td><strong>LoopX</strong></td>
    <td><a href="https://github.com/huangruiteng/loopx">GitHub</a></td>
    <td align="center"><img alt="LoopX stars" src="https://img.shields.io/github/stars/huangruiteng/loopx?style=flat&amp;label=stars" /></td>
  </tr>
  <tr>
    <td><strong>LingTai</strong></td>
    <td><a href="https://lingtai.ai/en/">Website</a> · <a href="https://github.com/Lingtai-AI/lingtai">GitHub</a></td>
    <td align="center"><img alt="LingTai stars" src="https://img.shields.io/github/stars/Lingtai-AI/lingtai?style=flat&amp;label=stars" /></td>
  </tr>
  <tr>
    <td><strong>OpenViking</strong></td>
    <td><a href="https://openviking.ai/">Website</a> · <a href="https://github.com/volcengine/OpenViking">GitHub</a></td>
    <td align="center"><img alt="OpenViking stars" src="https://img.shields.io/github/stars/volcengine/OpenViking?style=flat&amp;label=stars" /></td>
  </tr>
  <tr>
    <td><strong>Hermes Agent</strong></td>
    <td><a href="https://github.com/NousResearch/hermes-agent">GitHub</a></td>
    <td align="center"><img alt="Hermes Agent stars" src="https://img.shields.io/github/stars/NousResearch/hermes-agent?style=flat&amp;label=stars" /></td>
  </tr>
  <tr>
    <td><strong>heima</strong></td>
    <td><a href="https://github.com/litentry/heima">GitHub</a></td>
    <td align="center"><img alt="heima stars" src="https://img.shields.io/github/stars/litentry/heima?style=flat&amp;label=stars" /></td>
  </tr>
</table>

Building-partner status denotes an active collaboration. It does not by itself
imply a production deployment, support SLA, or completed enterprise
qualification.

## Third-party listings

<table>
  <tr>
    <td align="center" width="120">
      <a href="https://landscape.cncf.io/?group=projects-and-products&item=runtime--cloud-native-storage--nokv">
        <img src="./docs/public/img/recognition/cncf.svg" width="56" alt="CNCF Landscape" />
      </a>
    </td>
    <td>
      <strong>CNCF Landscape</strong><br/>
      Listed under Runtime / Cloud Native Storage, with AI Native Infra / Storage
      as an additional path.
    </td>
  </tr>
  <tr>
    <td align="center" width="120">
      <a href="https://landscape.lfai.foundation/?group=projects-and-products&item=data--store-format--nokv">
        <img src="./docs/public/img/recognition/lfai.svg" width="56" alt="LF AI &amp; Data Landscape" />
      </a>
    </td>
    <td>
      <strong>LF AI &amp; Data Landscape</strong><br/>
      Listed under Data / Store &amp; Format.
    </td>
  </tr>
  <tr>
    <td align="center" width="120">
      <a href="https://github.com/rust-unofficial/awesome-rust#database">
        <img src="https://awesome.re/mentioned-badge.svg" width="110" alt="Mentioned in Awesome Rust" />
      </a>
    </td>
    <td>
      <strong>Awesome Rust</strong><br/>
      Listed under Applications / Database.
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
      Historical profile of NoKV's earlier Go storage-engine line.
    </td>
  </tr>
</table>

Landscape entries are third-party catalog listings, not foundation-hosted
project status. The DBDB.io entry describes the earlier Go implementation and
is preserved here with that scope made explicit.

## What NoKV Owns

Agent runs produce datasets, scripts, logs, outputs, checkpoints, reports, and
provenance across files and object keys. NoKV gives that state one path-shaped
workspace and owns the metadata needed to publish, inspect, query, snapshot,
commit, restore, retain, and collect it.

```text
Downstream skills -> native full nokv CLI
Embedded callers  -> direct Python SDK
Native callers    -> lower-level Rust SDK
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
- **Deterministic Agent surface.** The same exact 18-tool Workbench semantics
  are available through the primary native CLI, the direct Python SDK, and the
  lower-level Rust SDK.

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
| **Current product** | Persisted `RootId -> LogicalShardId` affinity; one epoch-fenced active owner per shard; canonical full-path metadata keys; immutable S3-compatible bodies; native full CLI; direct Python and Rust SDKs; exact 18-tool Workbench semantics; snapshots, commits, restore, queries, and reference-fenced GC |
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

- **Native full `nokv` CLI — primary.** It exposes every Workbench operation
  through `nokv workbench <tool> '<json arguments>'`, plus `materialize`,
  `collect`, `workspace-path`, `provision`, `serve`, and `schema` commands.
- **Direct Python SDK — secondary.** [`nokv-python`](crates/nokv-python)
  serves embedded programmatic callers and includes explicit
  materialize/collect adapters for local executables.
- **Rust Agent SDK.** [`nokv-client`](crates/nokv-client) is the lower-level
  native integration and shared implementation boundary.
- **Transport-free Agent contracts** in
  [`nokv-agent`](crates/nokv-agent), shared by every adapter.

Downstream Agent systems should normally write skills that invoke the native
CLI. Use the Python SDK when an in-process boundary is preferable. Every
surface delegates to the same transport-free facade; none of them is a separate
metadata or lifecycle authority.

RootId is the only storage and routing identity. A Workbench presentation root
shapes Agent-facing paths and manifests but never enters canonical metadata
keys.

## Integration Model

The Workbench contract is runtime-neutral. A downstream Agent runtime exposes
skills over the native CLI, or embeds the Python SDK, and exercises the same
scientific reconstruction workflow:

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

The default integration is a downstream skill that calls the native CLI. It
can invoke the same 18 operations with `nokv workbench <tool> '<json
arguments>'`; an embedded host can instead call the Python SDK directly.

In every integration shape, the runtime persists a stable `AgentId` and a
distinct `RootId` for each
isolation boundary. Provisioning immutably binds that root to the AgentId;
`--workbench-root` remains only the human-facing path projection and cannot
grant isolation by itself. The binding prevents accidental root reuse, but is
not an authentication credential. The 18 `workbench_*` tools land next to the
agent's local file tools instead of replacing them, and grant no new authority.

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

`nokv schema` reports the frozen contract marker
`nokv.workbench.mcp_input_schemas.v1`, and `nokv --help` still lists an `mcp`
subcommand. Both are retained wire identity for the qualification harness. The
MCP sidecar is deprecated and is not a supported NoKV integration surface; use
the native CLI or the Python SDK.

Anyone can instead install the current stable source release from NoKV's public
Homebrew tap. The fully qualified command adds the tap and trusts only the
`nokv` Formula:

```bash
brew install NoKV-Lab/tap/nokv
nokv version --json
nokv schema
```

The current source Formula release gate covers Apple Silicon and Intel macOS.
Linuxbrew is not yet a qualified release target.

To add the tap separately before installing by short name, grant the same
Formula-scoped trust explicitly:

```bash
brew tap NoKV-Lab/tap
brew trust --formula NoKV-Lab/tap/nokv
brew install nokv
```

Pull a merged tap update and install a newer NoKV release with:

```bash
brew update
brew upgrade nokv
nokv version --json
```

The Formula keeps Homebrew `version_scheme 1` so the corrected pre-1.0 release
line upgrades cleanly from the earlier `1.0.0` Formula.

The Formula version follows the stable NoKV release tag and the `crates/nokv`
package version, not Holt, Rust, or protobuf. Every NoKV release carries its
own `Cargo.lock` and embeds the exact Holt version, source, and checksum in the
installed identity. A Holt update reaches the tap only as part of a new NoKV
release whose generated Formula has been merged into the tap.

Run the offline Workbench contract gate:

```bash
python3 scripts/workbench/workbench_contract_test.py
```

A live deployment additionally needs a root id, persisted logical-shard
placement, one admitted shard owner, and S3-compatible object coordinates. The
[live deployment preflight](docs/workbench-preflight.md) gives the provision,
serve, admission, and acceptance flow.

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
- [Live Deployment Preflight](docs/workbench-preflight.md)
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
| [`nokv`](crates/nokv) | Thin native full CLI wiring |
| [`nokv-bench`](bench) | Non-product contract, recovery, and performance workloads |

## License

Apache-2.0. See [LICENSE](LICENSE).
