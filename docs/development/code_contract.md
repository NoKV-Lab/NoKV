<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# NoKV Code Contract

This repository is the Rust NoKV product line: an object-backed metadata
control plane for durable agent workspaces, with Holt-backed metadata and
S3-compatible object storage.

NoKV accepts breaking internal changes when they reduce ambiguity or remove old
migration paths. Do not add forwarding wrappers, aliases, or dual execution
paths unless the PR states the operational need and removal condition.

## Package Boundaries

| Package | Owns | Must Not Do |
| --- | --- | --- |
| `crates/nokv-types/` | Storage-neutral namespace model: mount ids, inode ids, dentry names, inode attrs, body descriptors, record families, and typed watch events. | Import metadata layout, Holt, Raft, object-store clients, FUSE, protobuf, or service packages. |
| `crates/nokv-protocol/` | Storage-neutral metadata RPC DTOs shared by server and service clients. | Execute metadata commands, own path resolution, import Holt, object-store clients, FUSE, metadata layout, or server/client implementations. |
| `crates/nokv-meta/` | Metadata schema, Holt-friendly layout, `MetadataCommand`, Holt-backed metastore, in-process metadata service, snapshot retention pins, object-reference GC queue policy, history pruning, and service-level GC workers. | Own provider-specific object behavior, FUSE/kernel cache policy, Python bindings, CSI behavior, or wire-protocol migration behavior. |
| `crates/nokv-control/` | Shard map, owner leases, shard epochs, routing metadata, checkpoint/log pointers, and failover coordination. | Own inode/dentry semantics, chunk manifests, object GC policy, Holt internals, data-plane cache placement, FUSE behavior, or provider-specific object-store behavior. |
| `crates/nokv-object/` | Object-store boundary, S3-compatible backend, batch object reads, local hot-tier object store, tiered data-fabric helpers, soft placement resolution, and in-memory test object store for file bodies. | Own namespace metadata, import Holt/FUSE/protobuf, implement metadata transactions, or expose filesystem-directory object storage as a product backend. |
| `crates/nokv-agent/` | Transport-free agent tool definitions, JSON schemas, argument validation, namespace dispatch, and result shaping. | Own network/MCP transport, remote metadata connections, metadata layout, durable storage, or control-plane routing. |
| `crates/nokv-client/` | Path-oriented Rust SDK over the metadata service and object backend. | Own metadata layout, bypass `nokv-meta`, expose object-store internals, implement FUSE/kernel cache semantics, depend on `nokv-fuse`, or define metadata wire formats. |
| `crates/nokv-fuse/` | FUSE low-level frontend, inode mapping, kernel-facing attr conversion, range reads, and close-to-open buffered writes through the metadata client/server boundary. | Resolve paths through the path SDK hot path, own metadata layout, import Holt directly, open a production metadata store, or implement object-provider-specific behavior. |
| `crates/nokv-python/` | Python SDK and fsspec binding for training workflows over `nokv-client`. | Own metadata layout, bypass `nokv-client`, implement object-provider-specific behavior, import FUSE, or reimplement Rust SDK range planning in Python. |
| `crates/nokv-server/` | Long-running metadata service process, startup config, background GC ownership, health/status/control HTTP, framed metadata RPC, shard-owner lifecycle, and recovery wiring. | Own metadata semantics, durable layout, object provider internals, FUSE/kernel cache policy, or hidden migration behavior. |
| `crates/nokv/` | `nokv` CLI binary, command parsing, local service startup, MCP stdio transport, and CLI wiring across client, FUSE, metadata, and object config. | Own metadata semantics, durable layout, object backend internals, agent tool schemas/dispatch, or FUSE filesystem implementation. |
| `bench/` | System workload harnesses for metadata smoke, generated training-profile reads, checkpoint publish/read, demo dataset shapes, and the agent-interface experiment. | Own product APIs, metadata layout, object backend implementation, FUSE/kernel cache policy, or hidden benchmark-only behavior in product crates. |

Planned package owners:

| Package | Owns |
| --- | --- |
| `crates/nokv-csi/` | Kubernetes CSI integration and mount lifecycle. |

## File Layout

Use responsibility-based file names. Avoid `utils.rs`, `helpers.rs`,
`common.rs`, and `misc.rs` unless the package is tiny and the file has one
clear responsibility.

Recommended package layout:

| File | Contents |
| --- | --- |
| `lib.rs` | Package contract and public exports. |
| `types.rs` | Core domain types and interfaces. |
| `options.rs` | Construction options and validation. |
| `errors.rs` | Package error enum and conversions. |
| `codec.rs` | Durable encoding and decoding. |
| `store.rs` | Authoritative store object. |
| `service.rs` | Service boundary. |
| `tests.rs` / `*_test.rs` | Focused behavior tests. |

## Rules

- Keep filesystem semantics above storage engine bindings.
- Keep object bytes out of metadata values except compact descriptors.
- Keep local hot-tier placement and cache slots out of metadata values; they
  are data-path soft state keyed by immutable blocks.
- Use inode/dentry as canonical truth; path indexes are derived acceleration.
- Use `MetadataCommand` predicates as the atomicity fence.
- Do not leak Holt internals into types, object, client, or FUSE.
- Do not introduce provider-specific RustFS metadata semantics; RustFS uses the
  S3-compatible object backend.
- NoKV owns path routing and horizontal sharding. Holt remains a shard-local
  metadata engine; it does not own distributed placement or consensus.
- `nokv-control` owns shard maps, owner leases, epochs, and recovery pointers;
  it is not the source of truth for inode, dentry, or workspace metadata.
- Preserve one active writer per shard and reject stale epochs at the metadata
  commit boundary. Metadata atomicity is shard-local; do not imply a
  cross-shard transaction or cross-shard rename guarantee.
- Keep agent schemas and dispatch transport-free in `nokv-agent`. Remote
  namespace implementations belong in `nokv-client`; MCP transport belongs in
  the `nokv` CLI.
- A namespace or workbench-root jail is a path boundary, not tenant identity,
  authentication, or authorization.
- Benchmark harnesses must observe product behavior, not add benchmark-only
  semantics to product crates.
- Prefer explicit invariants and local reasoning over manager-style wrappers.

## Validation

Before pushing substantial changes:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
git diff --check
```

For documentation-only changes, validate local Markdown links and references
in the changed files and run `git diff --check`. The repository does not
currently ship a documentation site build target.
