<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

<div align="center">
  <img src="./docs/public/img/logo.png" width="320" alt="NoKV" />

  <h3>Durable, versioned workspaces for disposable agents.</h3>

  <p>
    NoKV keeps declared inputs, code, outputs, logs, and lineage alive when an
    agent sandbox disappears. It publishes immutable S3-compatible artifact
    bytes through a path-native, transactional metadata control plane; it does
    not pretend to restore process memory, a model session, or a VM.
  </p>

  <p>
    <strong>18-operation native CLI</strong> ·
    <strong>direct Python and Rust SDKs</strong> ·
    <strong>commit, snapshot, and restore</strong> ·
    <strong>conditional publication and exact replay</strong>
  </p>

  <p>
    <a href="https://github.com/NoKV-Lab/NoKV/actions/workflows/rust.yml">
      <img alt="Rust CI" src="https://github.com/NoKV-Lab/NoKV/actions/workflows/rust.yml/badge.svg?branch=main" />
    </a>
    <a href="https://github.com/NoKV-Lab/NoKV/actions/workflows/python-sdk.yml">
      <img alt="Python SDK" src="https://github.com/NoKV-Lab/NoKV/actions/workflows/python-sdk.yml/badge.svg?branch=main" />
    </a>
    <a href="https://github.com/NoKV-Lab/NoKV/releases/latest">
      <img alt="Latest release" src="https://img.shields.io/github/v/release/NoKV-Lab/NoKV?sort=date&amp;display_name=tag&amp;label=release" />
    </a>
    <a href="./LICENSE">
      <img alt="Apache-2.0 license" src="https://img.shields.io/github/license/NoKV-Lab/NoKV" />
    </a>
  </p>

  <p>
    <a href="#what-nokv-adds"><strong>What it adds</strong></a> ·
    <a href="#use-cases"><strong>Use cases</strong></a> ·
    <a href="#feature-map"><strong>Feature map</strong></a> ·
    <a href="#choose-an-interface"><strong>Integrate</strong></a> ·
    <a href="#evidence-and-qualification"><strong>Evidence</strong></a> ·
    <a href="#quick-start"><strong>Quick start</strong></a> ·
    <a href="./docs/index.md"><strong>Docs</strong></a>
  </p>
</div>

> [!IMPORTANT]
> This README describes the current `main` contract. An installed release may
> pin different component revisions; run `nokv version --json` to identify the
> exact NoKV commit, Workbench schema, and
> [Holt](https://github.com/NoKV-Lab/holt) build. Implemented and
> deterministic-test-passing do not mean live or production-qualified. The
> current qualification boundary is explicit below.

## What NoKV Adds

Most agent systems already have an object store and a database. Their hard
problem is the failure window between them: bytes may exist without a visible
workspace entry, a retry may publish twice, a stale worker may overwrite a
newer result, or cleanup may delete an object that history still references.

NoKV closes that window as one artifact lifecycle:

```text
produce bytes
    |
    v
upload immutable blocks + verify provider admission
    |
    v
one final bounded metadata command makes the revision visible
  - compare the current path generation and owner epoch
  - publish the immutable revision and advance the path head
  - update indexes, references, change history, and operation receipt
    |
    v
visible workspace result, or a typed conflict / recoverable exact replay
```

The current hard-to-substitute increment is this combined closure, not one
data structure or a claim that alternatives cannot exist. A replacement must
preserve all of the following under response loss and restart, then pass the
same conformance and fault evidence:

- **Atomic visibility:** artifact bytes are staged first and become reachable
  only when their revision, path head, indexes, references, and receipt commit.
- **No silent overwrite:** create, replace, edit, append, rename, and remove
  are explicit and generation-fenced. A stale writer gets a conflict.
- **Exact retry:** request identity and operation records distinguish
  definitely-not-applied, applied-with-lost-response, and unknown outcomes.
- **Durable decision points:** commits retain an immutable revision closure;
  restore builds a hidden same-root incarnation and publishes it atomically.
- **History-aware collection:** object listing is never namespace truth. GC is
  fenced by path references, commits, snapshots, restore operations, and
  quarantine state.
- **Fenced ownership:** persisted root placement and owner epochs prevent an
  obsolete shard owner from continuing to publish metadata.

### Find a capability by job

| I need to... | Use | What the caller gets |
| --- | --- | --- |
| Persist one run | `workbench_create`, `workbench_put_file`, `workbench_append`, `workbench_edit`, `workbench_commit` | Five-section workspace, explicit write modes, immutable revisions, and a sealed run manifest |
| Reopen work from another process | `workbench_list`, `workbench_stat`, `workbench_read`, `workbench_find` | Path-shaped discovery and verified reads without retaining the original sandbox |
| Prevent stale or duplicate writes | generations, explicit request identity, exact replay | Typed conflict for stale state; the same outcome for an exact retry after response loss |
| Search many artifacts or runs | `workbench_grep`, `workbench_search`, `workbench_aggregate`, `workbench_catalog`, `workbench_find` | Literal body search plus typed indexed metadata query and aggregation |
| Freeze a short-lived view | `workbench_snapshot`, `workbench_snapshot_renew`, `workbench_snapshot_list` | Leased point-in-time reads with explicit lifecycle state |
| Create a durable replay point | `workbench_commit` then `workbench_restore` | Retained revision closure and atomic restore into a new destination |
| Run a local executable | `nokv materialize` then `nokv collect` | Verified input files in disposable scratch and explicit output publication |
| Embed NoKV in Python | direct `Client`, Workbench fsspec, checkpoint, optional torch DCP | In-process path/artifact/range/query/lifecycle APIs bounded to an explicit Workbench |
| Build a native provider or control plane | Rust `WorkspaceClient`, change polling, custom-index registration, operation status | Typed lower-level integration and recovery primitives |

## Use Cases

### State provider for disposable agent sandboxes

An agent sandbox should be disposable; its durable state should not be. Any
agent, loop harness, or sandbox runner can keep transient execution local while
using NoKV as the durable state provider for everything that must survive:
task state, inputs, code, tool outputs, logs, checkpoints, and lineage.

```text
run inside a sandbox
  -> publish immutable state and artifacts with generation fences
  -> seal a deterministic Workbench commit

sandbox exits or disappears
  -> reopen the workspace from its durable identity
  -> continue from the commit, or restore it into a fresh Workbench
  -> retry the same request exactly; reject an obsolete writer
```

**The value is not another state-blob write.** NoKV is not raw storage alone;
it is an implemented, composable, sandbox-native functional storage layer. A
harness can use its lifecycle primitives through the native CLI or SDK:

- **Atomic checkpoint visibility:** artifact bytes are published first; a
  Workbench commit then seals one canonical manifest and its exact immutable
  revision closure as the durable decision point.
- **Idempotent recovery:** for the same request identity and inputs, durable
  operation receipts return the prior outcome after a lost response instead of
  applying the same transition twice.
- **Stale-writer rejection:** a caller submits the generation it observed;
  generation compare-and-swap rejects its update if newer state won first.
  Owner epochs separately fence an obsolete shard owner.
- **Restore without payload copying:** a retained commit can populate a hidden
  same-root incarnation whose destination becomes visible atomically. Leased
  snapshots provide shorter-lived point-in-time recovery and inspection.
- **One recoverable workspace:** verified materialize/collect, indexed search,
  immutable lineage, and retention-aware GC share one workspace contract. The
  Rust SDK additionally exposes change polling; quarantine and reconciliation
  remain internal lifecycle mechanisms rather than CLI commands.

The harness still decides which state is authoritative and when execution may
resume. Restoring state does not grant a replacement worker permission to act;
the harness must reacquire its own leases and gates. NoKV recovers declared
durable state, not RAM, sockets, credentials, container or VM state, or an
in-flight model session. These lifecycle primitives are implemented and
layer-tested at the evidence revision below; a complete harness adapter and
authority handoff remain caller-owned and require separate live qualification.

### AI-for-science: durable evidence for adaptive HPC loops

An adaptive AI-for-science campaign can use a fast surrogate to screen a broad
candidate space, spend expensive high-fidelity compute on a selected subset,
and feed the verified results into the next round. [atomate2](https://materialsproject.github.io/atomate2/)
and [jobflow](https://materialsproject.github.io/jobflow/) continue to own the
scientific Jobs and Flows, dependencies, result schemas, JobStore, and
execution environment. NoKV adds the durable state and evidence boundary
between rounds and around the expensive jobs:

1. Publish exact inputs, code, configuration, model artifacts, and candidate
   sets as immutable, versioned artifacts.
2. Before high-fidelity work is launched, seal the selected tasks and decision
   inputs as one deterministic Workbench commit.
3. Record the workflow's calculation identity and attach returned outcomes,
   including failures, as new immutable revisions instead of overwriting prior
   evidence.
4. Retrain, re-analyse, or restore from a retained commit identity; populate a
   fresh Workbench from its closure without silently mixing incompatible
   inputs or results.

This target shape makes each expensive selection a reproducible checkpoint:
what was chosen, from which inputs and model state, under which code and
configuration, and what came back. NoKV does not build the workflow graph,
submit or cancel external jobs, validate scientific correctness, or replace
the workflow
engine, scheduler, or JobStore. This is a target integration shape, not a
currently bundled atomate2/jobflow adapter, partner adoption, or live-qualified
HPC deployment. A future adapter would bind workflow job ids, output
references, and scheduler receipts to Workbench revisions; scientific workflow
semantics remain above NoKV.

## Feature Map

### Native Workbench CLI: the stable agent surface

`nokv workbench <tool> '<json arguments>'` is the primary integration surface.
The names and normalized input schemas below are fixed by the
[Workbench contract](docs/workbench-contract.md).

| Operation | Downstream outcome |
| --- | --- |
| `workbench_create` | Create one jailed Workbench with `input`, `scripts`, `outputs`, `logs`, and `metadata` sections. |
| `workbench_put_file` | Publish create-only or replace-only bytes; it is never upsert. |
| `workbench_append` | Append through immutable publication plus generation CAS and bounded conflict retry. |
| `workbench_edit` | Replace exact UTF-8 text, revalidate after conflicts, and avoid a new revision for a byte-identical result. |
| `workbench_list` | List direct children with scope-bound cursors at live state or a leased snapshot. |
| `workbench_stat` | Read a compact artifact or implicit-prefix card without loading the body. |
| `workbench_read` | Read verified whole bodies, byte ranges, or structured JSON/YAML/text at live state or a snapshot. |
| `workbench_grep` | Run bounded case-insensitive literal body search with optional basename glob; it is not regex. |
| `workbench_search` | Query indexed metadata with predicates, sort, projection, and facets. |
| `workbench_aggregate` | Compute bounded count, sum, average, min, max, and grouped aggregates over indexed metadata. |
| `workbench_catalog` | Discover stable field ids and the query operations actually available for them. |
| `workbench_find` | Find Workbenches by committed state and canonical run-manifest literal match. |
| `workbench_commit` | Seal a canonical run manifest and immutable revision closure with deterministic identity and exact replay. |
| `workbench_snapshot` | Mint a leased point-in-time view; the default is seven days and the maximum is 90 days. |
| `workbench_snapshot_renew` | Extend, but never shorten, a live snapshot lease. |
| `workbench_snapshot_retire` | Retire a root-bound snapshot idempotently and release its hold when allowed. |
| `workbench_snapshot_list` | Inspect aliases, deadlines, annotations, and `alive`/`expired`/`retired`/`reaped` state. |
| `workbench_restore` | Restore a snapshot or commit into an absent destination without copying immutable payload blocks. |

All 18 operations have complete parser-to-metadata implementation chains, with
object-store effects for body-bearing operations, and deterministic
adjacent-layer tests at the evidence revision below. Their native-CLI
real-service acceptance gate is still **not qualified**.

### Other CLI capabilities

| Command | Purpose | Boundary |
| --- | --- | --- |
| `nokv version --json` | Report the exact NoKV revision, lockfile identity, Holt source/version/checksum, schema, and tool count. | Identity/readback only. |
| `nokv schema` | Emit the exact 18 normalized Workbench input schemas. | Contract inventory, not a live health check. |
| `nokv materialize` | Copy a verified workspace artifact to a new local path. | The destination is disposable scratch, not a namespace or mount. |
| `nokv collect` | Publish one bounded regular local file, create-only or generation-fenced replace. | Symlinks and unbounded/non-regular inputs fail closed. |
| `nokv workspace-path rename` / `nokv workspace-path remove` | Apply an explicit generation- and request-id-fenced path mutation. | Custom CLI surface; not one of the 18 Workbench tools. |
| `nokv provision` | Bind `RootId` to `AgentId`, object namespace, logical shard, and persisted placement through etcd. | `AgentId` prevents accidental root reuse; it is not authentication. |
| `nokv serve` | Start one explicit metadata owner from create, same-namespace reopen, or recovery-log state. | Shared recovery publication is opt-in and not currently qualified. |
| `nokv mcp` | Deprecated stdio transport retained only because qualification runners still use it. | **Unsupported for integration.** |

### Programmatic and non-CLI capabilities

The interfaces share protocol, metadata, object, and lifecycle semantics. They
do not expose identical method shapes.

| Surface | Current public capability | Additional boundary |
| --- | --- | --- |
| Direct Python `Client` | 23 methods covering create/stat/exists/list/remove/rename, byte/file publish, whole/range/batch reads, query/aggregate/catalog/find, commit/restore, snapshot lifecycle, materialize, and collect | Direct SDK, not a tool-for-tool copy of the 18-name CLI facade |
| Python adapters | Workbench-scoped fsspec, checkpoint helpers, and optional torch Distributed Checkpoint reader/writer | Bounded to an explicit Workbench; not arbitrary-root POSIX or FUSE |
| Rust `WorkspaceClient` | Lower-level typed workspace, publication, query, lifecycle, routing, and batch-range workflows | Recommended when the caller must own typed retry and recovery integration |
| Rust-only extensions | Polling change feed, generic custom-index registration, raw operation status, and phased publish/restore primitives | No native CLI or Python method today; change feed is polling, not push |
| Metadata and server lifecycle | snapshot reap, commit/tag holds, reference-fenced GC, quarantine reconciliation, Holt reopen, and optional shared recovery records | Internal/operator mechanisms, not independent end-user commands |

The Python SDK does not currently expose CLI-equivalent append, exact-string
edit, or body grep methods. Use the CLI contract for those exact behaviors.
Custom SDK compositions are caller-owned and are not equivalent qualification
evidence.

## Choose an Interface

```text
Downstream skill or shell harness
  -> native nokv workbench CLI       exact 18-operation contract

Embedded Python application
  -> nokv Python Client/adapters     direct path/artifact/lifecycle API

Native control plane or provider
  -> nokv-client Rust crate          lower-level typed integration
```

Use the native CLI for agent skills because it provides one inspectable schema,
stable JSON envelopes, and fail-closed admission. Use Python when the caller
needs in-process range reads, fsspec/checkpoint integration, or direct typed
methods. Use Rust for provider, routing, change-feed, or recovery-aware work.

Every agent-facing CLI command requires self-refreshing etcd routing plus the
durable `RootId` to `AgentId` binding. Static route pins remain an SDK/testing
option and are rejected by the agent-facing CLI.

## Core Semantics

### Namespace and publication

- `RootId` is the only storage and routing identity. A presentation path such
  as `/agents/research/wb` shapes returned paths and manifests but grants no
  storage authority.
- `PathCurrent(root, incarnation, normalized_path)` is canonical namespace
  truth. Directories are implicit prefixes; an object-store listing is never
  authoritative.
- Every published body has a never-reused artifact revision id, immutable
  revision-owned blocks, a whole-body digest, and a caller-visible generation.
- Append is not a server-side lock. It reads the current head, attempts a
  generation-fenced immutable publication, and retries bounded conflicts.

### Commit, snapshot, and restore

- A commit is the durable decision point: it binds a canonical run manifest
  and retains the exact artifact revision closure.
- A snapshot is a leased liveness hold, not archival retention. Use snapshot
  plus restore while the lease is active; use commit plus restore when a
  decision point must remain recoverable after the lease expires.
- Restore preserves its source, reuses immutable payload blocks, stages a
  fresh same-root incarnation invisibly, and atomically publishes the new
  destination. It is not a cross-root or cross-shard copy transaction.

### Sharding, identity, and authority

- Root placement is persisted control-plane state, never derived from a path
  prefix or the current shard count.
- One epoch-fenced active owner executes a shard-local metadata command.
- Transactions, snapshots, commit closure, restore, and GC barriers are
  shard-local. NoKV does not provide cross-shard transactions.
- Workbench jailing and `RootId` to `AgentId` binding are safety boundaries,
  not tenant authentication, authorization, or RBAC.

## Why Holt Fits the Current Local Profile

[Holt](https://github.com/NoKV-Lab/holt) is NoKV's purpose-built embedded Rust
metadata engine. Its persistent adaptive radix trees provide ordered point and
prefix access for path-shaped metadata while its WAL and atomic batches provide
the local durability boundary. NoKV owns the workspace semantics in `nokv-meta`
over the storage-neutral `TxnStore` contract; Holt is the currently wired and
tested `LocalSync` implementation, while S3-compatible storage still owns
artifact bytes.

This evidence snapshot covers NoKV's exact `holt = "=0.8.6"` dependency.
Later upstream Holt `main` features are outside this section's claims; identify
an installed build with `nokv version --json`.

Holt maps NoKV metadata families to named persistent adaptive radix trees:

- adaptive Node4/16/48/256 fanout and path compression fit NoKV's
  prefix-rich encodings for workspace paths, commit members, and indexes;
- the same persistent ART index supports point lookup and native ordered
  prefix/start-after/delimiter pages, which maps directly to hierarchical
  listing without rebuilding order in the application;
- one bounded `DB::atomic` batch checks and mutates multiple metadata families
  through a single WAL record, matching one NoKV metadata command;
- one scoped `DB::view` captures a consistent multi-tree read view for a NoKV
  read batch; it is not the same object as a durable NoKV snapshot;
- NoKV's file-backed Holt profile forces synchronous WAL acknowledgement;
  replay, shadow checkpoint slots, and an exclusive manifest writer define the
  tested same-namespace restart boundary;
- compacted blobs support page-granular routed reads, while corrupt read
  accelerators fall back to the authoritative blob.

This is a workload fit, not proof that ART is universally faster or uniquely
able to implement NoKV. The adapter contract deliberately permits another
`TxnStore` after equivalent conformance, receipt, snapshot, scan, restart, and
fault qualification. Current tests cover deterministic torn-frame injection
and a real child `SIGKILL` after a durable non-empty partial manifest record;
they do **not** constitute a physical power-cycle, remote failover, cross-host
fencing, or metadata-HA test. No current third-party A/B number is claimed.

## Evidence and Qualification

The labels below mean exactly:

- **Implemented:** the typed call chain and durable effect exist in current
  source.
- **Layer-tested:** deterministic tests exercise real adjacent implementations;
  fake/mock tests remain identified as such.
- **Live-qualified:** the supported entrypoint ran against the required real
  dependencies at the pinned revision and produced accepted evidence.

Evidence snapshot:

- NoKV commit [`0f1995ebee96`](https://github.com/NoKV-Lab/NoKV/commit/0f1995ebee96048e5d4f9d4745d84c3518c64351);
- pinned Holt `0.8.6` dependency.

| Surface | Current evidence | Status |
| --- | --- | --- |
| Rust workspace | 1,090 passed, 0 failed; 10 ignored real-etcd/S3 cases | Implemented and layer-tested; ignored cases are NQ |
| 18 Workbench operations | all names execute against typed backend primitives; parser, facade, client/protocol, server, metadata, Holt, and object layers pass focused tests | Implemented and layer-tested |
| Built CLI identity/schema/fail-closed admission | real process checks passed for help, version, exact schema, missing-etcd/static-route rejection, generation/request-id validation, and absence of a mount command | Local smoke passed |
| Native CLI against real etcd + server + S3 | partial real-service CLI paths exist, but no accepted full-surface Gate 0 transcript covers the complete lifecycle; the current black-box runner uses deprecated MCP and also lacks terminal snapshot-reap evidence | **Not qualified** |
| Installed Python wheel against real service | native ABI surface checks and focused unit/mock tests pass; fsspec/checkpoint/torch tests are mock/stub based | **Not qualified** |
| Holt 0.8.6 | 705 listed entries; 10 marked ignored; all executed entries passed; the NoKV adapter adds 32 unit and one reopen/conformance integration test | Layer-tested for the embedded local boundary |
| Holt third-party A/B | the current v0.8.6 Holt/RocksDB/SQLite/sled comparator target was attempted; local `librocksdb-sys`/`libclang` build failed before measurement | No result; no performance claim |

The 10 ignored NoKV workspace tests are six real-etcd tests, two direct object
provider tests, and two Python S3-admission tests. They are gaps, not passes.
See [Workspace acceptance](docs/development/workspace-acceptance.md) for the
normative gate and [Benchmarks and evidence](docs/benchmarks.md) for evidence
rules.

### Implemented, but not currently live-qualified

- opt-in shared recovery publication and receipt-directed recovery-log install.

### Not qualified by current evidence

- remote checkpoint compaction, copied-directory failover, shared metadata
  durability, multi-machine failover, or metadata HA;
- accepted full-surface native-CLI and installed-Python Gate 0 live acceptance;
- complete provider timeout and ambiguous-delete fault coverage;
- cross-host writer fencing;
- physical power-cycle behavior, production tail latency, write amplification,
  RSS advantage, or universal ART performance.

### Not offered by NoKV

- tenant identity, authentication, or RBAC;
- cross-shard transactions;
- transparent POSIX/FUSE/CSI/NAS behavior.

## Quick Start

### Inspect the contract locally

Build the CLI, identify the exact bits, and inspect the frozen schema:

```bash
cargo build --release -p nokv --bin nokv
./target/release/nokv version --json
./target/release/nokv schema
python3 scripts/workbench/workbench_contract_test.py
```

These commands prove build identity and the offline contract. They do not
exercise a deployed metadata owner or object provider.

A source Formula is available from NoKV's public Homebrew tap:

```bash
brew install NoKV-Lab/tap/nokv
nokv version --json
nokv schema
```

The Formula may trail the latest GitHub release. Read back `nokv version --json`
and verify release-specific target evidence before treating a version or
platform as supported.

### Call a provisioned deployment

A live deployment requires a persisted root placement, a leased shard owner,
etcd routing, and admitted S3-compatible object coordinates. After following
the [live deployment preflight](docs/workbench-preflight.md), the current
source-level CLI shape is below. The command form is documented; full
native-CLI real-service Gate 0 remains not qualified.

```bash
nokv \
  --root-id "$NOKV_ROOT_ID" \
  --agent-id "$NOKV_AGENT_ID" \
  --workbench-root /agents/research/wb \
  --etcd-endpoint "$NOKV_ETCD_ENDPOINT" \
  --object-bucket "$NOKV_BUCKET" \
  --object-endpoint "$NOKV_OBJECT_ENDPOINT" \
  workbench workbench_create '{"id":"run-001"}'

nokv \
  --root-id "$NOKV_ROOT_ID" \
  --agent-id "$NOKV_AGENT_ID" \
  --workbench-root /agents/research/wb \
  --etcd-endpoint "$NOKV_ETCD_ENDPOINT" \
  --object-bucket "$NOKV_BUCKET" \
  --object-endpoint "$NOKV_OBJECT_ENDPOINT" \
  workbench workbench_put_file \
  '{"id":"run-001","section":"scripts","path":"main.py","text":"print(42)","replace":false}'

nokv \
  --root-id "$NOKV_ROOT_ID" \
  --agent-id "$NOKV_AGENT_ID" \
  --workbench-root /agents/research/wb \
  --etcd-endpoint "$NOKV_ETCD_ENDPOINT" \
  --object-bucket "$NOKV_BUCKET" \
  --object-endpoint "$NOKV_OBJECT_ENDPOINT" \
  workbench workbench_read \
  '{"id":"run-001","section":"scripts","path":"main.py","format":"structured"}'
```

Credentials use the provider's normal chain or
`--object-access-key-id`, `--object-secret-access-key`, and optional
`--object-session-token`; never embed them in Workbench content or manifests.

## Architecture and Ownership

```text
agent skill / harness       embedded Python         native control plane
        |                        |                         |
        v                        v                         v
  18-tool CLI facade      direct Python API       lower-level Rust SDK
        +------------------------+-------------------------+
                                 |
                         typed NoKV protocol
                                 |
                   epoch-fenced logical-shard owner
                                 |
                    nokv-meta state machine
                 path, revisions, receipts, holds,
                    queries, restore, and GC
                       /                   \
                      v                     v
        TxnStore metadata adapter      artifact object store
          current local: Holt          S3-compatible bytes
```

NoKV is not a semantic-memory database or an agent orchestrator. Retrieval
ranking, planning, scientific validation, trace/scorer semantics, and runtime
policy remain above it. The object provider owns physical durability,
replication, availability, and access policy for bytes. NoKV owns which
immutable revisions are visible and retained by a workspace.

## Documentation

- [Documentation index](docs/index.md)
- [Product design](docs/product-design.md)
- [Architecture](docs/architecture.md)
- [Workbench contract](docs/workbench-contract.md)
- [Metadata schema](docs/metadata-schema.md)
- [Object layout](docs/object-layout.md)
- [Path-native metadata comparison](docs/development/path-native-metadata-comparison.md)
- [Workspace acceptance](docs/development/workspace-acceptance.md)
- [Live deployment preflight](docs/workbench-preflight.md)
- [Agent contributor handbook](docs/development/nokv-agent.md)
- [Code contract](docs/development/code_contract.md)
- [PR review checklist](docs/development/pr_review_checklist.md)
- [Source-only Homebrew release](scripts/release/README.md)

## Third-party Listings

NoKV is listed in the
[LF AI & Data Landscape](https://landscape.lfai.foundation/?group=projects-and-products&item=data--store-format--nokv)
and [Awesome Rust](https://github.com/rust-unofficial/awesome-rust#database).
The [DBDB.io profile](https://dbdb.io/db/nokv) documents NoKV's earlier Go
storage-engine line. Listings are discovery metadata, not foundation-hosted
status, integration, deployment, or qualification evidence.

**Open-source collaborations.** Active: [LoopX](https://github.com/huangruiteng/loopx),
[OpenViking](https://github.com/volcengine/OpenViking), and
[LingTai AI](https://github.com/Lingtai-AI/lingtai). Projects initiated:
[Hermes Agent](https://github.com/NousResearch/hermes-agent) and
[heima](https://github.com/litentry/heima). These labels describe collaboration
stage, not production adoption or completed qualification.

## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md), the
[code contract](docs/development/code_contract.md), and the
[PR review checklist](docs/development/pr_review_checklist.md) before changing
package boundaries or durable storage semantics. Every commit requires a DCO
`Signed-off-by` trailer.

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
| [`nokv-types`](crates/nokv-types) | Storage-neutral agent-workspace domain types |
| [`nokv-protocol`](crates/nokv-protocol) | Versioned metadata and lifecycle RPC DTOs and framing |
| [`nokv-meta-store`](crates/nokv-meta-store) | Ordered metadata transaction contract and conformance suite |
| [`nokv-meta-holt`](crates/nokv-meta-holt) | Embedded Holt adapter, strict open/reopen, recovery, and physical diagnostics |
| [`nokv-meta`](crates/nokv-meta) | Workspace schema, commands, history, indexes, commits, snapshots, restore, and GC |
| [`nokv-control`](crates/nokv-control) | Persisted root placement, shard ownership, epoch fencing, and recovery coordination |
| [`nokv-object`](crates/nokv-object) | Immutable S3-compatible artifact storage and optional local hot tier |
| [`nokv-client`](crates/nokv-client) | Root-routed Rust SDK and immutable-object data path |
| [`nokv-agent`](crates/nokv-agent) | Transport-free 18-operation Workbench facade and result shaping |
| [`nokv-python`](crates/nokv-python) | Direct Python SDK and bounded fsspec/checkpoint adapters |
| [`nokv-server`](crates/nokv-server) | Logical-shard owner, RPC executor, adapter composition, and lifecycle workers |
| [`nokv`](crates/nokv) | Native CLI wiring |
| [`nokv-bench`](bench) | Non-product contract, recovery, and performance workloads |

## License

Apache-2.0. See [LICENSE](LICENSE).
