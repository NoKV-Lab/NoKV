<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# NoKV Code Contract

NoKV is an Agent-native distributed workspace and artifact store. Its supported
product surfaces are SDKs, a custom CLI, MCP/Agent adapters, and the stable
[Workbench contract](../workbench-contract.md). Holt is the ordered metadata
engine; S3-compatible storage owns durable artifact bytes.

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
| `crates/nokv-meta/` | `nokv_workspace` schema and codec, `MetadataCommand`, Holt binding, workspace visibility, history, indexes, commits, snapshots, holds, operations, change events, GC policy, and recovery/fsck semantics. | Own provider-specific object I/O, client routing, Workbench result shaping, Python bindings, FUSE, or wire fallback schemas. |
| `crates/nokv-control/` | Persisted root placement, shard map, owner leases and epochs, checkpoint/log pointers, movement, and failover coordination. | Own path/artifact semantics, Holt records, object GC policy, query behavior, data-cache placement, or filename-based shard routing. |
| `crates/nokv-object/` | Immutable object upload/read, multipart and range planning, S3-compatible providers, local hot-tier soft cache, integrity verification, and in-memory test backend. | Own namespace metadata, revision reachability, metadata transactions, root placement, Holt, protobuf, FUSE, or Workbench semantics. |
| `crates/nokv-client/` | Rust Agent SDK, routing, conditional path operations, lifecycle APIs, retries, and the direct immutable-object data path. | Depend on `nokv-meta` or `nokv-server`, know Holt keys, expose provider internals, implement FUSE semantics, or define wire DTOs. |
| `crates/nokv-agent/` | Transport-free Workbench/Agent tool schemas, the 18-tool facade, stable result shaping, and adapters over SDK traits. | Import Holt/layout, object providers, server implementations, FUSE, or duplicate SDK state machines. |
| `crates/nokv-python/` | Direct Python SDK plus explicit materialize/collect adapters for local executables. | Own metadata layout, bypass `nokv-client`, reimplement range/retry planning, promise fsspec/POSIX semantics, or import FUSE. |
| `crates/nokv-server/` | Shard-owner process, versioned RPC, startup/schema gates, health, backup/log sync, and background lifecycle workers. | Own domain semantics outside `nokv-meta`, leak provider internals into RPC, or silently migrate/fallback between schemas. |
| `crates/nokv/` | Thin `nokv` CLI and MCP wiring over client and Agent interfaces. | Own metadata semantics, durable layout, object-provider behavior, or embed a second implementation of the SDK. |
| `bench/` | Contract, recovery, and performance workloads with explicit environment and workload profiles. | Own product APIs, add benchmark-only product behavior, or compare results from materially different profiles as equivalent. |

FUSE, POSIX emulation, CSI, and generic fsspec integration are outside the NoKV
product architecture. They must not appear in package APIs, production routes,
tests presented as product acceptance, or Workbench behavior.

## Architecture Discipline

Every production path uses the workspace schema. Startup rejects an unmarked,
unknown, or mixed store. There is one writable schema, one authoritative path
model, and one routing model. A change must not introduce an inode/dentry
namespace, filesystem semantics, an alternative durable layout, or a second
implementation of publication, retention, recovery, or routing.

The 18 Workbench tools, their normalized input schemas, and the observable
semantics in the Workbench contract are stable. Adapter result shaping cannot
become canonical metadata or introduce fields outside that contract.

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
- Keep object bytes out of Holt except compact immutable descriptors.
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
- Do not leak Holt internals into types, protocol, object, client, Agent
  adapter, Python, or CLI packages.
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
python3 scripts/lingtai-workbench/workbench_contract_test.py
```

When a checked-in documentation build exists, run it for documentation or
navigation changes. Until then, validate Markdown links and run
`git diff --check`.

System acceptance follows
[`workspace-acceptance.md`](./workspace-acceptance.md). Every applicable
gate reports `PASS`, `FAIL`, or `NOT QUALIFIED`; passing unit tests alone is not
evidence for durability, failover, GC, or performance claims.
