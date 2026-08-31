<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# NoKV Code Contract

NoKV is an Agent-native distributed workspace and artifact store. Its primary
product surface is the native full `nokv` CLI; the direct Python SDK is the
secondary embedded surface. The stable
[Workbench contract](../workbench-contract.md) defines shared semantics. NoKV
persists
workspace metadata through the storage-neutral transaction-store contract.
Holt is the current serving local adapter. S3-compatible storage owns durable artifact
bytes.

The lower-layer architecture is the
[workspace metadata schema](../metadata-schema.md). It is the only supported
namespace, metadata, routing, and object-lifetime contract. Code, tests, and
documentation must describe that architecture directly.

NoKV accepts breaking internal changes when they remove ambiguity or reduce
long-term maintenance. Delete superseded interfaces and schemas instead of
adding forwarding wrappers, aliases, fallback layouts, or parallel execution
paths.

## Package Boundaries

These are the package boundaries:

| Package | Owns | Must Not Do |
| --- | --- | --- |
| `crates/nokv-types/` | Storage-neutral Agent root/workbench ids, normalized paths, generations, artifact revisions, commits, snapshots, holds, operations, errors, and typed events. | Import metadata layout, Holt, Raft, object clients, FUSE, protobuf, server, or provider packages. |
| `crates/nokv-protocol/` | Versioned, storage-neutral metadata and lifecycle RPC DTOs shared by clients and servers. | Execute commands, normalize paths independently, own metadata layout, or import Holt, object providers, FUSE, or server/client implementations. |
| `crates/nokv-meta-store/` | Storage-neutral ordered byte-key reads, conditional writes, hard limits, planner targets, acknowledgement boundaries, recovery mode and authority, and physical store errors. | Own workspace records or codecs, import `nokv-meta`, bind Holt or FoundationDB, or define server composition. |
| `crates/nokv-meta-holt/` | The serving local Holt implementation of the metadata transaction-store contract, including physical options, read diagnostics, strict open checks, and adapter conformance. | Import `nokv-meta`, own workspace records or codecs, define server composition, or claim shared recovery authority. |
| `crates/nokv-fdb/` | Process-global FoundationDB API/network lifetime, database and transaction handles, common connection options, versioned physical store prefix/subspaces, and shared error classification. | Own workspace/control records, metadata-store limits, server composition, client routing, or automatic transaction retries. |
| `crates/nokv-meta-fdb/` | The non-default FoundationDB characterization adapter, including metadata keyspace encoding, conservative affected-byte limits, `TxnStore` mapping, and environment-gated conformance. | Import `foundationdb` or `nokv-meta`, own workspace records or codecs, define server composition, add automatic commit retries, or claim serving qualification. |
| `crates/nokv-meta/` | `nokv_workspace` schema and codec, `MetaShard`, `MetadataCommand`, workspace visibility, history, indexes, commits, snapshots, holds, operations, change events, GC policy, and recovery/fsck semantics over `nokv-meta-store`. | Import Holt or FoundationDB in production code, own provider-specific object I/O, client routing, Workbench result shaping, Python bindings, FUSE, or wire fallback schemas. Adapter-backed dev tests may use the package test-support boundary. |
| `crates/nokv-control/` | Persisted root placement, shard map, owner leases and epochs, checkpoint/log pointers, movement, and failover coordination. | Own path/artifact semantics, Holt records, object GC policy, query behavior, data-cache placement, or filename-based shard routing. |
| `crates/nokv-control-fdb/` | FoundationDB store-manifest, catalog, route, stable owner-session, heartbeat, local monotonic observation, and exact control-transaction encoding over `nokv-control` types. | Import `foundationdb` directly, own path/artifact/recovery-log semantics, define server composition or client discovery, use wall-clock expiry, or retry raw commits. |
| `crates/nokv-object/` | Immutable object upload/read, multipart and range planning, S3-compatible providers, local hot-tier soft cache, integrity verification, and in-memory test backend. | Own namespace metadata, revision reachability, metadata transactions, root placement, Holt, protobuf, FUSE, or Workbench semantics. |
| `crates/nokv-client/` | Rust Agent SDK, routing, conditional path operations, lifecycle APIs, retries, and the direct immutable-object data path. | Depend on `nokv-meta` or `nokv-server`, know Holt keys, expose provider internals, implement FUSE semantics, or define wire DTOs. |
| `crates/nokv-agent/` | Transport-free Workbench/Agent tool schemas, the 18-tool Workbench facade, the seven-tool generic Agent profile, stable result shaping, and adapters over SDK traits. | Import Holt/layout, object providers, server implementations, FUSE, or duplicate SDK state machines. |
| `crates/nokv-python/` | Direct Python SDK, explicit materialize/collect adapters, and Workbench-scoped immutable compatibility adapters for fsspec, checkpoint, and torch DCP callers. | Own metadata layout, bypass `nokv-client`, reimplement range/retry planning, promise POSIX directory/inode semantics or an arbitrary root filesystem, or import FUSE. |
| `crates/nokv-server/` | Shard-owner process, versioned RPC, startup/schema gates, health, backup/log sync, and background lifecycle workers. | Own domain semantics outside `nokv-meta`, leak provider internals into RPC, or silently migrate/fallback between schemas. |
| `crates/nokv/` | Thin native full `nokv` CLI wiring over client and Agent interfaces. | Own metadata semantics, durable layout, object-provider behavior, or embed a second implementation of the SDK. |
| `bench/` | Contract, recovery, and performance workloads with explicit environment and workload profiles. | Own product APIs, add benchmark-only product behavior, or compare results from materially different profiles as equivalent. |

FUSE, POSIX emulation, CSI, and arbitrary-root generic filesystem integration
are outside the NoKV product architecture. They must not appear in production
routes or Workbench behavior. A Python compatibility adapter may implement the
bounded fsspec protocol only inside one explicit Workbench and its five virtual
sections, with whole-object immutable publication, typed generations, and the
existing `nokv-client` retry/range authority. It may project the five virtual
sections and artifact prefixes as fsspec directory-shaped results, but must not
create durable directory objects or emulate inodes, permissions, mounts, or
another namespace.

## Architecture Discipline

Every production path uses the workspace schema. Startup rejects an unmarked,
unknown, or mixed store. There is one writable schema, one authoritative path
model, and one routing model. A change must not introduce an inode/dentry
namespace, filesystem semantics, an alternative durable layout, or a second
implementation of publication, retention, recovery, or routing.

The 18 Workbench tools, their normalized input schemas, and the observable
semantics in the Workbench contract are stable. Adapter result shaping cannot
become canonical metadata or introduce fields outside that contract.

Delivery priority and semantic ownership are separate. The CLI is the default
integration surface, the Python SDK is second for in-process callers, and the
Rust SDK is third for native integrations. The `nokv mcp` sidecar is deprecated
and is not a supported surface. None of these transports may fork the stable
Workbench semantics or duplicate the client state machines.

The Agent adapter owns canonical Workbench projections. It must recompute any
projection commitment from typed facade inputs for every fresh request and
recovery attempt; a caller cannot provide that commitment. Protocol and server
code treat the commitment as opaque and exact-bind it to the durable operation,
but cannot claim to validate facade-only inputs that the wire request does not
carry. Direct construction of a raw protocol commit request is an internal
trusted boundary, not a second public Agent contract.

## File Layout

Use responsibility-based names. Avoid `utils.rs`, `helpers.rs`, `common.rs`,
and `misc.rs` unless a tiny package has one genuinely shared responsibility.

| File | Contents |
| --- | --- |
| `lib.rs` | Package contract and public exports. |
| `types.rs` | Package-owned domain types and interfaces. |
| `options.rs` | Construction options and validation. |
| `errors.rs` | Package error enum and conversions. |
| `codec.rs` | Durable encoding/decoding owned by the storage package. |
| `store.rs` | Authoritative store object. |
| `service.rs` | Service boundary. |
| `tests.rs` / `*_test.rs` | Focused behavior tests. |

Errors, validation, metrics, stats, recovery, and encoding live with the domain
that owns them. Do not move domain-specific or single-use logic into a generic
`utils` module.

## Storage Rules

- `PathCurrent(root_id, workspace_incarnation_id,
  normalized_relative_path)` is workspace namespace truth. Workbench names resolve
  through `WorkspaceCurrent`; directories are implicit prefixes.
- `WorkspaceIncarnationClaim(root_id, workspace_incarnation_id)` permanently
  binds each never-reused incarnation to one Workbench id. Direct create and
  restore staging claim it atomically with the visibility marker.
- All path identities use the one storage-neutral normalizer and
  component-safe key codec.
- `WorkspaceCurrent` is the visibility marker; staging must be absent from
  every query surface until marker publication.
- SecondaryIndexV2 rows are derived, generation-fenced data. Publication and
  rename may stage them in one bounded command, but only an atomic
  `PathIndexLocator(Staged -> Published)` plus matching `PathCurrent`
  transition makes them visible. Queries recheck the exact current path, and
  asynchronous cleanup deletes only stale `Published` generations.
- Keep object bytes out of the metadata store except compact immutable
  descriptors.
- Publish objects first and metadata last. Metadata failure leaves no
  user-visible path.
- Every published body has a never-reused immutable
  `ArtifactRevisionId`; physical object keys are revision-owned and
  shard-local.
- Every path/commit reference is a strong `RevisionRef`; add/remove updates the
  revision count and epoch in the same command, and GC claims
  `Available -> Deleting` against that epoch.
- A child revision that reuses older blocks holds a sealed strong dependency
  reference to every distinct physical owner revision until the child is
  deleted.
- Manifest row position and `physical_object_index` are distinct. Object-key
  validation and GC use the physical owner's local index, never the child's
  ordered row position.
- Content digests prove identity/integrity but do not imply global physical
  deduplication.
- Use bounded `MetadataCommand` predicates as the atomicity and idempotency
  fence.
- Snapshots are leased MVCC read versions. Durable commits/tags retain exact
  revisions and do not pin the global history floor.
- GC must account for current paths, retained history, commits, build/restore/
  fork/publish holds, operations, and the current fenced shard owner.
- Persist control-plane root placement and install the matching shard-local
  `RootFence` before the first write. Never derive placement from a filename or
  modulo the current shard count; a populated root does not change logical
  shard.
- Keep local hot-tier placement and cache slots as soft state keyed by
  immutable revisions/blocks.
- Do not leak physical-store internals into metadata domain types, protocol,
  object, client, Agent adapter, Python, or CLI packages.
- Do not introduce provider-specific RustFS metadata; RustFS uses the common
  S3-compatible boundary.
- Prefer explicit invariants and local reasoning over manager-style wrappers.

## Validation

Before pushing substantial code changes:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
git diff --check
```

For Workbench-facing changes also run:

```bash
python3 scripts/workbench/workbench_contract_test.py
```

When a checked-in documentation build exists, run it for documentation or
navigation changes. Until then, validate Markdown links and run
`git diff --check`.

System acceptance follows
[`workspace-acceptance.md`](./workspace-acceptance.md). Every applicable
gate reports `PASS`, `FAIL`, or `NOT QUALIFIED`; passing unit tests alone is not
evidence for durability, failover, GC, or performance claims.
