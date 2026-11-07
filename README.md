<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

<div align="center">
  <img src="./docs/public/img/logo.png" width="320" alt="NoKV" />

  <h3>A file system for agent workspaces and AI-for-science campaigns.</h3>

  <p>
    Agents and experiment pipelines publish versioned artifacts that are never
    silently overwritten, retry any write exactly, seal checkpoints, and fork a
    checkpoint into a new branch of the work. Artifact bytes stay in any
    S3-compatible store; NoKV decides which revisions are visible, records who
    wrote them, and keeps alive what history still references.
  </p>

  <p>
    <strong>native CLI, Python and Rust SDKs</strong> ·
    <strong>exact replay</strong> ·
    <strong>commit, snapshot, and fork</strong> ·
    <strong>tenants, roles, and audit</strong> ·
    <strong>pluggable storage</strong>
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
    <a href="https://artifacthub.io/packages/container/nokv/nokv">
      <img alt="Artifact Hub" src="https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/nokv" />
    </a>
    <a href="./LICENSE">
      <img alt="Apache-2.0 license" src="https://img.shields.io/github/license/NoKV-Lab/NoKV" />
    </a>
  </p>

  <p>
    <a href="#a-agent-workspace"><strong>Agent workspace</strong></a> ·
    <a href="#b-ai-for-science"><strong>AI for Science</strong></a> ·
    <a href="#core-primitives"><strong>Primitives</strong></a> ·
    <a href="#quick-start"><strong>Quick start</strong></a> ·
    <a href="#evidence-and-qualification"><strong>Evidence</strong></a> ·
    <a href="./docs/index.md"><strong>Docs</strong></a>
  </p>
</div>

> [!IMPORTANT]
> This README describes the current `main` contract, released as NoKV 0.20.0.
> An installed build may differ; run `nokv version --json` to identify the
> exact NoKV commit, Workbench schema, and
> [Holt](https://github.com/NoKV-Lab/holt) build. Implemented and
> deterministic-test-passing do not mean live or production-qualified; the
> qualification boundary is explicit [below](#evidence-and-qualification).

## What's new in 0.20

NoKV 0.20.0 lets many agents share one deployment safely
([release notes](https://github.com/NoKV-Lab/NoKV/releases/tag/v0.20.0)):

- **Tenants, principals, and roles.** Over TLS 1.3, an owner admits only a
  current credential of the tenant that owns its roots and authorizes every
  request as `reader`, `writer`, or `operator`. Revoking a credential or a role
  takes effect within about a second, on open connections too.
- **Attribution and audit.** Every durable operation records the principal
  that began it, and only that principal or an operator may continue it.
  `nokv serve --audit-log` writes one JSON line for every refusal.
- **Read-only agents on the object store.** The owner writes every object
  block, so agents need only read access under their root's prefix.
- **A hardened control plane.** etcd with mutual TLS and least-privilege
  roles; a single-host control directory that refuses unsafe permissions
  before anything changes.
- **One session, many roots.** Rust and Python `Session`s share one transport
  and one set of credentials across roots, and an owner serves a root that is
  provisioned after it started.
- **CLI contract v2.** Exit codes 10 (`unauthenticated`) and 11
  (`permission_denied`), Workbench writes that replay under a caller operation
  id, and bounded retry backoff.
- **Holt 0.8.8.** A metadata batch that fails part-way can no longer reach
  disk.
- **Removed:** the deprecated `nokv mcp` sidecar.

Upgrading from 0.12.x is breaking: the wire protocol (v15), the metadata format
(15, with no migration), and the CLI contract (v2) change together.

## Core primitives

Both use cases below are built from the same small set of primitives.

| Primitive | What it guarantees | Surface |
| --- | --- | --- |
| Workbench | One jailed workspace with `input`, `scripts`, `outputs`, `logs`, and `metadata` sections, addressed by path | `workbench_create`, `workbench_list`, `workbench_stat` |
| Explicit writes | Create-only, replace-only, exact-text edit, or append, each fenced by the generation the writer observed: a stale writer gets a conflict instead of overwriting newer state | `workbench_put_file`, `workbench_edit`, `workbench_append`, `workspace-path put`/`append`/`rename`/`remove` |
| Exact replay | A retry with the same operation id and inputs returns the original receipt after a lost response or a restart; other inputs are an `identity_conflict` | `--operation-id`, `nokv operation status` |
| Commit | A durable decision point: one run manifest plus the exact immutable revisions present at that moment, retained until the commit is retired | `workbench_commit` |
| Snapshot | A leased point-in-time view for reads and short-lived recovery (seven days by default, at most 90) | `workbench_snapshot` and its `_renew`, `_list`, `_retire` tools; reads with `at_snapshot` |
| Fork | Restore a retained commit, or a snapshot whose lease is alive, into a new Workbench: a new branch of the work, without copying payload bytes; the source stays unchanged | `workbench_restore` |
| Query | Literal body search, typed indexed-metadata predicates, aggregation, and field discovery | `workbench_grep`, `workbench_search`, `workbench_aggregate`, `workbench_catalog`, `workbench_find` |
| Local execution | Copy verified inputs to disposable scratch, then publish outputs back explicitly | `nokv materialize`, `nokv collect` |
| Identity | TLS client authentication, roles per root, initiator attribution, refusal audit | `nokv identity`, `nokv serve --rpc-tls-*`, `--audit-log` |

NoKV stores declared durable state. It does not checkpoint process memory,
sockets, credentials, a container or VM, or an in-flight model session.

## A. Agent workspace

### A pluggable state layer for agent sandboxes

An agent sandbox should be disposable; its state should not be. Any agent
harness or sandbox runner keeps execution local and uses NoKV as the state
provider for what must survive: task state, inputs, code, tool outputs, logs,
checkpoints, and lineage.

```text
run inside a sandbox
  -> publish immutable state and artifacts with generation fences
  -> seal a deterministic Workbench commit

sandbox exits or disappears
  -> reopen the workspace from its durable identity
  -> continue from the commit, or fork it into a new branch
  -> retry the same request exactly; reject an obsolete writer
```

- **Atomic checkpoint visibility.** Artifact bytes are staged first; a commit
  then seals one canonical manifest and its exact immutable revision closure.
- **Idempotent recovery.** Durable operation receipts return the prior outcome
  after a lost response instead of applying the same transition twice.
- **Stale-writer rejection.** Generation compare-and-swap rejects an update if
  newer state won first; owner epochs separately fence an obsolete shard owner.
- **One recoverable workspace.** Verified materialize and collect, indexed
  search, immutable lineage, and retention-aware garbage collection share one
  workspace contract.

The harness still decides which state is authoritative and when execution may
resume. Restoring state does not grant a replacement worker permission to act;
the harness reacquires its own leases and gates.

### Fork a checkpoint into a new branch for an agent

An agent that wants to try an alternative, or a supervisor that gives a second
agent the same starting point, forks a checkpoint into a new branch: a new
Workbench that starts from exactly the revisions the checkpoint holds.

```bash
nokv [route options] workbench workbench_restore \
  '{"id":"run-42","destination_id":"run-42-try-b","at_commit":"<64-hex commit identity>"}'
```

- The source Workbench stays unchanged, and the new branch reuses its immutable
  payload blocks instead of copying them. Staging stays hidden until the branch
  appears atomically, and an exact retry converges to one result.
- Fork from a commit when the starting point must stay citable: a commit is
  retained until it is retired. A fork from a snapshot (`at_snapshot`) works
  only while the snapshot's lease is alive.
- The new branch is an ordinary writable Workbench in the same root. There is
  no in-place rollback and no fork across roots or shards.

### Many agents on one root

With RPC TLS:

- Each agent presents its own certificate, which `nokv identity` maps to a
  principal of the root's tenant. The CLI needs no `--agent-id`, so many
  agents write one root concurrently, fenced by generations.
- Every request needs a role on its root. A refused credential exits 10
  (`unauthenticated`) and a refused role 11 (`permission_denied`); both tell
  the agent to stop and hand over to an operator.
- Revoking a credential or a role takes effect within about a second, on open
  connections too.
- `operation status` and change events name the principal that began each
  operation; only that principal or an operator may continue, abort, or
  complete it.
- `nokv serve --audit-log PATH` appends one JSON line for every refused
  connection and every denied request.
- A Rust or Python `Session` derives clients for many roots from one transport
  and one set of credentials, and an owner starts serving a root provisioned
  after it started, without a restart.

[SECURITY.md](SECURITY.md) and the [threat model](docs/development/threat-model.md)
state what each mechanism covers and what it does not.

### Validated with Hermes and Pi agents

NoKV's acceptance runs drive [Hermes](https://github.com/NousResearch/hermes-agent)
and Pi agents through the native CLI, each harness with its own skill:

- two writer agents edit one file concurrently: no lost update, and each write
  is attributed to the agent that made it;
- a reader agent asked to publish is refused with `permission_denied` before
  anything changes, then stops and reports;
- a credential revoked mid-session is refused within about a second, and the
  agent stops;
- every refusal appears in the owner's audit log;
- a control directory made group-writable mid-task is refused as
  `operator_required` before any request; the agent stops instead of retrying,
  and service resumes once an operator fixes the permissions;
- 16 and 64 Pi agents, each with its own principal, share one root without
  errors.

NoKV ships no Hermes or Pi plugin: the native CLI and its
[contract](docs/cli-contract.md) are the integration surface, and an agent
system provides its own skill over them.

### The Workbench CLI

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
| `workbench_restore` | Fork a snapshot or commit into an absent destination without copying immutable payload blocks. |

`put_file`, `edit`, and `append` accept an optional caller `operation_id` and
replay under it from any process and after an owner restart.

Other commands:

| Command | Purpose |
| --- | --- |
| `nokv version --json`, `nokv schema` | Report the exact revision, lockfile, Holt build, and the 18 frozen input schemas. |
| `nokv materialize`, `nokv collect` | Copy a verified artifact to disposable local scratch; publish one bounded regular file back, create-only or generation-fenced. |
| `nokv workspace-path put`, `stat`, `rename`, `remove` | Publish one whole body under a caller `--operation-id`; read a path's generation in one metadata read; apply fenced path mutations. |
| `nokv workspace-path append` | Append under one durable logical identity that survives process restarts; see [append recovery](docs/append-recovery.md). |
| `nokv operation status`, `inspect`, `recover` | Query any append, publish, commit, or restore by identity; inspect and recover a quarantined append without object credentials. |
| `nokv provision`, `nokv serve` | Bind a root to its Workbench root, object namespace, and shard; run one epoch-fenced shard owner. |
| `nokv identity` | Manage tenants, principals, certificate credentials, and tenant- and root-scoped roles. |

A failed command writes one JSON line to stderr with its failure `class` and
`next_action` and exits with that class's code; see the
[CLI contract](docs/cli-contract.md).

## B. AI for Science

### Frozen evidence for adaptive campaigns

An adaptive campaign screens a large candidate space with a cheap surrogate,
spends expensive high-fidelity compute (for example DFT jobs on a batch
scheduler) on a selected subset, and retrains on the results. Workflow engines
such as [atomate2](https://materialsproject.github.io/atomate2/) and
[jobflow](https://materialsproject.github.io/jobflow/) already own jobs,
dependencies, per-calculation provenance, and execution. They do not answer
the campaign-level question: **what evidence was available at each decision,
and what would a different selection policy have done with it?**

NoKV holds that campaign record: a create-only log of inputs, model
predictions, reference calculations, and decisions, frozen at each decision
point and replayable offline.

### Exactly-once ingestion under scheduler retries

Cluster jobs are resubmitted, restarted, and re-parsed. Ingestion stays
idempotent:

- Derive each publication's operation id from a stable identity of what it
  records, for example the calculation identity and the content digest, and
  save it before sending.
- A resubmitted job with the same id and inputs gets the original receipt and
  creates no second record. The same id with different inputs is refused as
  `identity_conflict` instead of overwriting.
- `nokv operation status <id>` tells a restarted pipeline whether an ingestion
  committed, is still pending, or must be resubmitted, without resending the
  payload.
- Record a genuine rerun as a new artifact next to the calculation it repeats,
  not as a replacement, so run-to-run variation stays part of the record.

### Decision points and as-of reads

- Before launching expensive work, seal the selection: commit the Workbench
  with a run manifest that names the decision's inputs. The commit retains the
  exact immutable revisions present at that point until it is retired.
- Read the campaign as it was at a point in time through a snapshot
  (`at_snapshot`), or fork the commit to reconstruct it after the snapshot's
  lease has expired.
- The run manifest and its digest are supplied by the committer: NoKV binds
  them to the commit but does not recompute them from content. A fork of a
  commit is an ordinary writable Workbench; the frozen guarantee lives in the
  commit and in snapshot reads, not in the copy.

### Counterfactual replay: fork a decision point

To ask what another acquisition policy would have chosen with the same
evidence, fork the decision point's commit into a new branch, run the policy
against exactly the evidence that existed then, and compare outcomes. Many
policies can run against one frozen point, each in its own branch, without
touching the original record. On a densely precomputed benchmark, where every
candidate's predictions and reference calculations exist in advance, any
policy restricted to it can be replayed exactly.

### Campaign queries, provenance, and isolation

- **Queries in place.** Typed indexed metadata answers campaign questions such
  as compute spent per decision or which candidates have reference results
  (`workbench_search`, `workbench_aggregate`, `workbench_catalog`).
- **Provenance.** Each durable operation records the principal that began it,
  and refusals are audited.
- **Isolation.** Groups that share one deployment get separate tenants, roots,
  and roles; analysis jobs can run on read-only object credentials.
- **Your storage.** Artifact bytes stay in your own S3-compatible store.
- **Model checkpoints.** The Python SDK's Workbench-scoped fsspec filesystem and
  PyTorch Distributed Checkpoint reader and writer keep training checkpoints in
  the same versioned workspace, in non-executable formats; see
  [AI training](docs/ai-training.md).

### What stays above NoKV

NoKV does not build the workflow graph, submit or cancel jobs, validate
scientific results, or replace the workflow engine, scheduler, or JobStore. No
atomate2 or jobflow adapter ships with NoKV; an adapter binds workflow job ids,
calculation identities, and scheduler receipts to Workbench revisions and stays
above NoKV.

## Quick Start

### Install

```bash
brew install NoKV-Lab/tap/nokv          # macOS, built from the release source
nokv version --json
```

```bash
pip install "nokv==0.20.0" \
  --find-links "https://github.com/NoKV-Lab/NoKV/releases/expanded_assets/v0.20.0"
```

The Python wheels are abi3 for CPython 3.9+ on Linux x86_64/aarch64 and macOS
arm64/x86_64. The container image `ghcr.io/nokv-lab/nokv` and its quick start
are described in [Container image](docs/container-image.md). From source
(Rust 1.91 or later):

```bash
cargo build --release -p nokv --bin nokv
./target/release/nokv schema
python3 scripts/workbench/workbench_contract_test.py
```

These commands prove build identity and the offline contract. They do not
exercise a deployed metadata owner or object provider.

### Call a provisioned deployment

A live deployment requires a persisted root placement, a leased shard owner,
control routing (`--etcd-endpoint` across hosts, or `--control-dir` on one
host), and admitted S3-compatible object coordinates. After the
[live deployment preflight](docs/workbench-preflight.md):

```bash
nokv \
  --root-id "$NOKV_ROOT_ID" \
  --agent-id "$NOKV_AGENT_ID" \
  --etcd-endpoint "$NOKV_ETCD_ENDPOINT" \
  --object-bucket "$NOKV_BUCKET" \
  --object-endpoint "$NOKV_OBJECT_ENDPOINT" \
  workbench workbench_create '{"id":"run-001"}'

nokv \
  --root-id "$NOKV_ROOT_ID" \
  --agent-id "$NOKV_AGENT_ID" \
  --etcd-endpoint "$NOKV_ETCD_ENDPOINT" \
  --object-bucket "$NOKV_BUCKET" \
  --object-endpoint "$NOKV_OBJECT_ENDPOINT" \
  workbench workbench_put_file \
  '{"id":"run-001","section":"scripts","path":"main.py","text":"print(42)","replace":false}'
```

Credentials use the provider's normal chain or `--object-access-key-id`,
`--object-secret-access-key`, and optional `--object-session-token`; never
embed them in Workbench content or manifests. Agents need only read access
under the root's prefix; [Object Store Credentials](docs/object-credentials.md)
lists the keys each role touches with example policies.

To carry workspace RPC over TLS 1.3, start the owner with `--rpc-tls-cert` and
`--rpc-tls-key`, and give each client `--rpc-tls-ca`, `--rpc-tls-cert`,
`--rpc-tls-key`, and `--rpc-tls-server-name`; the Python SDK takes the same
material through `TlsConfig`. Without RPC TLS, the CLI requires the durable
`RootId` to `AgentId` binding shown above, a misconfiguration check that is not
authorization. [Control plane credentials](docs/control-plane-credentials.md)
covers etcd with mutual TLS and the single-host control directory.

## Choose an Interface

```text
Downstream skill or shell harness
  -> native nokv CLI                 workspace operations and stable append recovery

Embedded Python application
  -> nokv Python Client/Session      direct path/artifact/lifecycle API, fsspec, torch DCP

Native control plane or provider
  -> nokv-client Rust crate          lower-level typed integration
```

Use the native CLI for agent skills: it gives one inspectable schema, stable
JSON envelopes, and fail-closed admission. Use Python when the caller needs
in-process range reads, fsspec or checkpoint integration, or direct typed
methods. Use Rust for provider, routing, change-feed, or recovery-aware work.

| Surface | Current public capability | Boundary |
| --- | --- | --- |
| Direct Python `Client` and `Session` | Create/stat/exists/list/remove/rename, byte/file publish, stable append with status/inspect/recover, whole/range/batch reads, query/aggregate/catalog/find, commit/restore, snapshot lifecycle, materialize, collect; one `Session` across roots, with TLS | A direct SDK, not a tool-for-tool copy of the 18-name CLI facade |
| Python adapters | Workbench-scoped fsspec, checkpoint helpers, and an optional torch Distributed Checkpoint reader/writer | Bounded to an explicit Workbench; not POSIX or FUSE |
| Rust `WorkspaceClient` and `Session` | Lower-level typed workspace, publication, query, lifecycle, routing, and batch-range workflows | Recommended when the caller owns typed retry and recovery |
| Rust-only extensions | Polling change feed, custom-index registration, raw operation status, phased publish/restore primitives | Change feed is polling, not push |

## Architecture and Pluggable Components

```text
agent skill / harness       embedded Python         native control plane
        |                        |                         |
        v                        v                         v
  18-tool CLI facade      direct Python API       lower-level Rust SDK
        +------------------------+-------------------------+
                                 |
                  typed NoKV protocol (optionally TLS 1.3)
                                 |
              epoch-fenced logical-shard owner, role checks
                                 |
                    nokv-meta state machine
                 path, revisions, receipts, holds,
                    queries, restore, and GC
                       /                   \
                      v                     v
        TxnStore metadata adapter      artifact object store
          current local: Holt          S3-compatible bytes
```

| Component | Today | Replaceable through |
| --- | --- | --- |
| Artifact bytes | Any S3-compatible object store, such as AWS S3, MinIO, RustFS, or Ceph RGW | Object store options; the provider owns physical durability, replication, and access policy |
| Metadata engine | [Holt](https://github.com/NoKV-Lab/holt), an embedded Rust engine with a write-ahead log and atomic batches | The storage-neutral `TxnStore` contract and its conformance suite |
| Control plane | etcd across hosts, optionally with mutual TLS and RBAC, or a local control directory on one host | `--etcd-endpoint` or `--control-dir` |

A write becomes visible in one bounded step: the client hands each immutable
block to the shard owner, which verifies and writes it; then one metadata
command compares the path generation and owner epoch, publishes the revision,
advances the path head, and updates indexes, references, change history, and
the operation receipt. The result is a visible revision, a typed conflict, or
a recoverable exact replay; object listing is never namespace truth.

NoKV is not a semantic-memory database or an agent orchestrator. Retrieval
ranking, planning, scientific validation, and runtime policy remain above it.

## Core Semantics

### Namespace and publication

- `RootId` is the only storage and routing identity. A presentation path such
  as `/agents/research/wb` shapes returned paths and manifests but grants no
  storage authority.
- `PathCurrent(root, incarnation, normalized_path)` is canonical namespace
  truth. Directories are implicit prefixes.
- Every published body has a never-reused artifact revision id, immutable
  revision-owned blocks, a whole-body digest, and a caller-visible generation.
- Stable append records a logical operation before publication; one logical
  append has at most one committed result.

### Commit, snapshot, and fork

- A commit is the durable decision point: it binds a run manifest and retains
  the exact artifact revision closure.
- A snapshot is a leased liveness hold, not archival retention. Use a snapshot
  while its lease is active; use a commit when a decision point must stay
  recoverable after the lease expires.
- Restore (fork) preserves its source, reuses immutable payload blocks, stages
  a fresh same-root incarnation invisibly, and publishes the new Workbench
  atomically. It is not a cross-root or cross-shard copy transaction.

### Sharding, identity, and authority

- Root placement is persisted control-plane state, never derived from a path
  prefix or the current shard count.
- One epoch-fenced active owner executes a shard-local metadata command.
  Transactions, snapshots, commit closure, restore, and GC barriers are
  shard-local. NoKV does not provide cross-shard transactions.
- Over RPC TLS, the owner authenticates each client, authorizes each request
  by role before routing, and records the initiating principal of every
  durable operation. Without TLS, Workbench jailing and the `RootId` to
  `AgentId` binding are safety checks, not authentication.

### Why Holt fits

Holt's persistent adaptive radix trees give ordered point and prefix access for
path-shaped metadata, and one bounded atomic batch checks and mutates several
metadata families through a single WAL record, matching one NoKV metadata
command. NoKV's file-backed profile forces synchronous WAL acknowledgement.
Holt 0.8.8 latches the database closed when a write fails after changing cached
frames, so a partial batch can never reach a checkpoint. This is a workload
fit, not a claim that ART is universally faster: the adapter contract permits
another `TxnStore` after equivalent qualification.

## Evidence and Qualification

The labels below mean exactly:

- **Implemented:** the typed call chain and durable effect exist in current
  source.
- **Layer-tested:** deterministic tests exercise real adjacent implementations;
  fake or mock tests remain identified as such.
- **Live-qualified:** the supported entrypoint ran against the required real
  dependencies at the pinned revision and produced accepted evidence.

Evidence for NoKV 0.20.0 (2026-10-31 to 11-01). Tag `v0.20.0` is commit
[`6e8a84e43d`](https://github.com/NoKV-Lab/NoKV/commit/6e8a84e43d1617c8366c0424ffc8898e52573db7)
(workspace RPC `nokv.workspace.rpc.v15`, metadata format 15, Holt `=0.8.8`); its
release pull request ran CI on the identical tree.

| Surface | Evidence | Status |
| --- | --- | --- |
| Rust workspace | Linux CI on the release tree: `cargo test --workspace` 1,436 passed, 0 failed, 15 ignored; the metadata read-stats feature set 449 passed; the restore crash-support set 247 passed, 1 ignored | Implemented and layer-tested; ignored cases are gaps, not passes |
| Bounded live gates | The same CI run with real etcd and moto or digest-pinned RustFS: native CLI contract, workspace RPC over TLS (t0–t23), control plane over mutual TLS (c0–c6), direct Python SDK in plaintext and over TLS, restore composition, local-WAL successor recovery, object-namespace recovery, fork-restore recovery, and tenant identity records on etcd all pass | Pass for the scenarios each gate names |
| Full-surface Gate 0 | The live Workbench gate's workflow passes; its full-surface Gate 0 remains not qualified, as CI expects | **Not qualified** |
| Downstream agents | Before merge, on one Apple M3 Pro laptop: Pi scenarios m1–m5 and 16- and 64-agent contention, and contained Hermes runs for concurrent writers, reader refusal, refusal audit, mid-session revocation, and control-directory failure, all pass | Single-host acceptance; not a production result |
| Old stores | A 0.12.1 metadata store is refused by a 0.20.0 owner with no bytes changed | Fail-closed upgrade boundary |
| Release artifacts | The Homebrew formula installs and passes its test on macOS 15 Apple Silicon and Intel; the Python wheels build and pass the SDK tests on all four platforms; published checksums verified | Released |
| Performance | One Apple M3 Pro laptop, release builds: authorization 61–115 ns per request; TLS 1.3 plus authorization about 0.7 ms more per CLI process; revocation p50 about 1 s; owner-mediated put of 1, 16, and 64 MiB in 0.34×, 0.83×, and 1.15× the time of the previous direct upload; 16 agents on one root at 49.6 turns/s with a turn p99 of 406 ms | Single-host measurements; no production, tail-latency, or HA claim |
| Holt 0.8.8 | 712 tests passed, 8 ignored; Nightly Validation (fault matrix, property tests, crash and normal soaks, fuzz) passed; 0.8.7 and 0.8.8 open each other's stores; the crates.io package equals tag `v0.8.8` | Layer-tested for the embedded local boundary |

The CI rows come from the release pull request's
[Rust workflow run](https://github.com/NoKV-Lab/NoKV/actions/runs/41212564642);
the release rows from the
[Homebrew](https://github.com/NoKV-Lab/NoKV/actions/runs/41213468560) and
[Python SDK](https://github.com/NoKV-Lab/NoKV/actions/runs/41213471697) release
runs. Agent-harness throughput against SQLite was measured for 0.12.0 (see its
[release notes](https://github.com/NoKV-Lab/NoKV/releases/tag/v0.12.0) and the
[raw evidence](https://gist.github.com/wchwawa/e9f86ba66c3211f1514d618e9827546a))
and was not rerun for 0.20.0. See
[Workspace acceptance](docs/development/workspace-acceptance.md) for the
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

### Planned, not yet offered

- replicated metadata and multi-machine failover.

### Not offered by NoKV

- cross-shard transactions;
- in-place rollback, or a fork across roots or shards;
- transparent POSIX/FUSE/CSI/NAS behavior.

## Documentation

- [Documentation index](docs/index.md)
- [Product design](docs/product-design.md) and [Architecture](docs/architecture.md)
- [Workbench contract](docs/workbench-contract.md) and [CLI contract](docs/cli-contract.md)
- [Append recovery](docs/append-recovery.md)
- [Metadata schema](docs/metadata-schema.md) and [Object layout](docs/object-layout.md)
- [Object store credentials](docs/object-credentials.md) and [Control plane credentials](docs/control-plane-credentials.md)
- [Threat model](docs/development/threat-model.md) and [SECURITY.md](SECURITY.md)
- [AI training](docs/ai-training.md)
- [Live deployment preflight](docs/workbench-preflight.md) and [Workspace acceptance](docs/development/workspace-acceptance.md)
- [Container image](docs/container-image.md) and its [Artifact Hub listing](docs/development/artifact-hub.md)
- [Path-native metadata comparison](docs/development/path-native-metadata-comparison.md)
- [Agent contributor handbook](docs/development/nokv-agent.md), [Code contract](docs/development/code_contract.md), and [PR review checklist](docs/development/pr_review_checklist.md)
- [Source-only Homebrew release](scripts/release/README.md)

## Third-party Listings

NoKV is listed in the
[LF AI & Data Landscape](https://landscape.lfai.foundation/?group=projects-and-products&item=data--store-format--nokv)
and [Awesome Rust](https://github.com/rust-unofficial/awesome-rust#database).
The [DBDB.io profile](https://dbdb.io/db/nokv) documents NoKV's earlier Go
storage-engine line. Listings are discovery metadata, not foundation-hosted
status, integration, deployment, or qualification evidence.

**Open-source collaborations.** Active: [LoopX](https://github.com/huangruiteng/loopx)
and [OpenViking](https://github.com/volcengine/OpenViking). Projects initiated:
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
| [`nokv-control`](crates/nokv-control) | Persisted root placement, shard ownership, epoch fencing, identity records, and recovery coordination |
| [`nokv-object`](crates/nokv-object) | Immutable S3-compatible artifact storage and optional local hot tier |
| [`nokv-client`](crates/nokv-client) | Root-routed Rust SDK, sessions, and immutable-object data path |
| [`nokv-agent`](crates/nokv-agent) | Transport-free 18-operation Workbench facade and result shaping |
| [`nokv-python`](crates/nokv-python) | Direct Python SDK and bounded fsspec/checkpoint adapters |
| [`nokv-server`](crates/nokv-server) | Logical-shard owner, RPC executor and authorization, adapter composition, and lifecycle workers |
| [`nokv`](crates/nokv) | Native CLI wiring |
| [`nokv-bench`](bench) | Non-product contract, recovery, and performance workloads |

## License

Apache-2.0. See [LICENSE](LICENSE).
