<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Product Design

Status: normative Agent-native product design.

## Product

NoKV is a distributed workspace and artifact store for Agent infrastructure.
It gives datasets, scripts, logs, outputs, checkpoints, reports, and provenance
stable path-shaped addresses while keeping their bytes in object storage.

The supported front doors, in delivery order, are:

1. the native full `nokv` CLI, including all 18 Workbench operations;
2. the direct Python SDK for embedded programmatic callers;
3. the Rust SDK for lower-level native integrations.

The complete 18-tool [Workbench contract](./workbench-contract.md) fixes shared
behavior across these surfaces. Downstream Agent systems normally provide
skills that invoke the CLI, or call the Python SDK when an in-process boundary
is preferable.

NoKV also provides explicit materialize/collect adapters for executables that
require local files.

The Workbench experience stays independent of storage internals. Agents can
list, stat, read, grep, search, aggregate, commit, snapshot, and restore. They
do not need to know Holt keys, object keys, revision ids, or shard owners.

## Deliberate Non-Goals

NoKV is not:

- a FUSE mount;
- a general NAS or POSIX filesystem;
- a transparent fsspec backend;
- a raw S3 key browser;
- a runtime trace database;
- a globally deduplicated content-addressed store.

It does not implement inode/dentry, uid/gid/mode, hardlink, symlink, xattr,
locks, special nodes, arbitrary directory rename, empty directory identity, or
random in-place writes.

This is a product simplification, not merely an omitted frontend. Agent
workflows primarily need immutable publication, conditional replacement,
streaming/range I/O, metadata discovery, provenance, recovery points, and
reproducible outputs. Modeling unused POSIX behavior would add metadata rows,
round trips, invalidation rules, recovery cases, and compatibility obligations
to every hot path.

## Stable Workbench Surface

A Workbench has five virtual sections:

```text
input
scripts
outputs
logs
metadata
```

They are logical prefixes, not stored directories. The upper adapter preserves
all 18 tool names, schemas, generation/digest behavior, commit identity,
snapshot lifecycle, and restore idempotency.

Workbench-specific result shaping stays above the storage core:

- JSON/YAML/text decoding and base64 ranges;
- exact-string edit behavior;
- grep matching;
- section projection;
- the delta digest returned by append;
- friendly errors, and the JSON-RPC result envelope the qualification harness
  consumes;
- stable `run_manifest.json` and `restore_manifest.json` projections.

Workbench responses do not contain storage-specific node identities. Stable
result fields change only through an explicit reviewed contract change.

## Core Data Model

```text
Agent root
  RootId -> one persisted logical-shard placement

Workbench name
  -> one visible WorkspaceIncarnationId

Artifact path
  (RootId, WorkspaceIncarnationId, normalized relative path)
  -> PathEntry

PathEntry
  -> complete immutable PathMetadata projection
  -> immutable ArtifactRevisionId for manifest/lifetime ownership

ArtifactRevisionId
  -> compact metadata manifest
  -> immutable S3-compatible object blocks
```

The complete normalized filename is stored in the canonical ordered metadata key.
Directories are derived from prefixes. The path value atomically projects the
revision digest, manifest digest, size, dependency bounds, content type, and
typed index fields needed by stat/list. This makes a cached-marker exact read
one path point lookup and a child listing one delimiter scan, without
inode-to-dentry traversal, revision-row fanout, or per-child reads.

A cold request first resolves the Workbench marker. Its never-reused
incarnation prevents abandoned restore rows from becoming visible under a
reclaimed name.

## Transaction Store Role

`MetaShard` owns workspace semantics. `TxnStore` supplies ordered reads and
checked atomic writes. Holt is the current serving local adapter, not the
product API.

```text
NoKV metadata layer
  path/workspace semantics
  command validation and idempotency
  visibility and secondary indexes
  snapshot/commit/restore lifecycle
  revision references and GC
  root/owner fencing

TxnStore
  point reads
  prefix/delimiter scans
  checked atomic multi-keyspace writes

HoltStore
  keyspace-to-tree mapping
  synchronous local WAL
  checkpoints, views, and reopen recovery
```

The Holt adapter uses Holt's ART-shaped point and prefix behavior. NoKV does
not leak the physical tree layout into metadata records, SDKs, protocol DTOs,
Workbench tools, or object providers.

## Immutable Bodies And Publication

The object layout is revision-owned:

```text
nokv/artifacts/{logical_shard_id}/{root_id}/{artifact_revision_id}/blocks/{object_index}
```

Object bytes upload first. One bounded metadata command then stages an
invisible secondary-index generation. One final metadata command atomically
publishes the immutable revision, manifest, path, workspace revision,
generation locator, event, reference changes, and deterministic replay result.
The final command predicates every staged index row.

Readers therefore see the previous complete revision or the new complete
revision, never a partial body. A response lost after commit is safely replayed
with the same request id. Failed or abandoned uploads remain invisible and are
recovered from a durable staged-object ledger.

Whole-artifact replacement is the generic write primitive. Logs use immutable
append segments plus a conditional stream-head advance. Range reads and
multipart uploads remain first-class SDK operations.

## Discovery And Provenance

Typed secondary indexes are staged in bounded transactions and become visible
atomically with the path entry through an exact generation-locator flip. Stale
published generations are filtered on reads and reclaimed asynchronously. They
serve:

- dataset/version lookup;
- run and producer lookup;
- parameter and metric filtering;
- output-to-input lineage;
- commit/tag discovery;
- Workbench search, aggregate, catalog, and find.

Indexes are derived and repairable. `PathCurrent` remains namespace truth, and
index results recheck the visible workspace incarnation at the same read
version.

NoKV is not the source of truth for high-volume runtime events. Traces may live
in JSONL, SQLite, or a telemetry database; NoKV stores the durable artifacts
and evidence an Agent needs to find and cite.

## Recovery Products

NoKV exposes three different promises:

| Mechanism | Purpose | Retention |
| --- | --- | --- |
| Leased snapshot | Short-term frozen MVCC recovery point | A read-version history hold until retire/reap |
| Durable commit | Immutable reusable dataset/run/workspace tree | Exact artifact revision references |
| Durable tag | Mutable human name for a commit | CAS-protected pointer; no implicit commit deletion |

Restore creates a new Workbench. It copies path metadata in bounded batches,
shares immutable revisions inside one root/shard, and reveals the destination
with one final marker transition. Normal reads do not pay for a lazy overlay
chain.

## Safe Object Lifetime

Every visible path and durable commit owns an exact strong revision reference.
A child revision that reuses blocks owned by older revisions also owns sealed
dependency references to those block owners. Adding or removing a reference
atomically changes the target revision's count and epoch. A zero-reference GC
candidate is valid only for that epoch.

GC claims `Available -> Deleting` against the epoch. New references also
require `Available`, so restore/commit/publication cannot race with deletion.
Snapshots and in-progress scans protect older metadata with read-version
history holds. Ambiguous provider deletes are quarantined rather than guessed.

This reference model intentionally stays root/shard-local.

## Distribution

The control plane persists `RootId -> logical_shard_id` before the first write.
The shard installs a matching local root fence. Placement is never recomputed
from filenames or modulo the current shard count.

A populated root stays on its logical shard, which keeps its Workbenches,
queries, commits, snapshots, restores, references, and object GC local. The
logical shard may move to another physical owner under a new lease/epoch
without changing object identity.

Splitting one root and cross-shard transactions are deferred. They require
version vectors, k-way query/list merge, distributed restore, and explicit
cross-shard reference ownership.

## Reference Research Workflow

The runtime-neutral reference validates the product through a scientific
reconstruction workflow:

```text
upload input dataset
  -> seal immutable input commit/tag
  -> create multiple run Workbenches
  -> materialize verified inputs/scripts for the local executable
  -> execute
  -> collect declared outputs/logs/metadata
  -> commit each run with lineage to the same input
  -> query, compare, snapshot, and restore through CLI/Python SDK/Workbench
```

Materialization verifies digests before execution. Collection publishes only
declared files. The local sandbox can disappear without losing the NoKV
identity or provenance graph.

## Qualification Boundary

The supported system has one workspace schema, one path model, one routing
model, and one implementation for each lifecycle. Startup rejects an unmarked,
unknown, malformed, incompatible, or mixed store before serving requests.

Source presence and unit tests do not prove durability, recovery, failover, GC,
or product behavior. Required boundary-level evidence is defined in
[Workspace Acceptance](./development/workspace-acceptance.md).
