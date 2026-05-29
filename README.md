<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# NoKV

NoKV is a metadata execution framework for AI agent workspaces and distributed
namespace services.

The stable product core is `fsmeta`: a filesystem-shaped metadata model with
inode, dentry, watch, snapshot, quota, session, atomic publish, remove, and
subtree-move semantics. The distributed modules provide rooted authority,
routing, TSO, Raft execution, MVCC transactions, and recovery. The bottom layer
is a replaceable ordered key/value storage backend.

NoKV does not own object bodies, model weights, checkpoint payloads, or POSIX
file data. Those bytes stay in object stores, local filesystems, model stores,
or application-defined body stores. NoKV owns namespace truth, compact body
references, metadata versions, watches, and lifecycle coordination.

## Architecture

```text
application / SDK
  -> fsmeta
  -> fsmeta/runtime/local or fsmeta/runtime/raftstore
  -> txn MVCC / Percolator
  -> storage/kv ordered backend
  -> storage/pebble today, future storage/holt adapter over third_party/holt
```

The main package boundaries are:

| Package | Responsibility |
|---|---|
| `fsmeta/model` | Inode, dentry, mount, quota, session, watch, and snapshot domain model. |
| `fsmeta/layout` | Ordered namespace key layout and value codecs. |
| `fsmeta/backend` | Minimal MVCC metadata backend contract consumed by fsmeta execution. |
| `fsmeta/exec` | Semantic executor and compiler for namespace operations. |
| `fsmeta/runtime/local` | Embedded fsmeta runtime for local deployments and tests. |
| `fsmeta/runtime/raftstore` | Adapter from fsmeta execution to distributed raftstore. |
| `meta/root` | Rooted truth for topology, authority, lifecycle facts, grants, and seals. |
| `coordinator` | Routing, TSO, store discovery, root-event publish, and serving-plane rebuild. |
| `raftstore` | Replicated region execution, peer lifecycle, apply, and raft snapshot bootstrap. |
| `txn` | MVCC storage keys, read/write planning, latches, and Percolator-style 2PC. |
| `storage/kv` | Ordered key/value backend interface. |
| `storage/pebble` | Default Pebble-backed storage backend. |
| `third_party/holt` | Pinned Holt source checkout for the future Rust-backed adapter. |
| `local` | Embedded DB facade over NoKV MVCC encoding and storage backend. |
| `experimental` | Research mechanisms such as Peras and Thermos. |

The important split is between `fsmeta/backend` and `storage/kv`.
`fsmeta/backend` is an MVCC metadata contract with timestamps, predicates,
mutations, scans, and optional one-phase atomic mutation. `storage/kv` stores
opaque ordered bytes only. That keeps fsmeta semantics and distributed transaction
behavior independent from the physical storage engine.

## fsmeta

`fsmeta` is a durable, versioned, watchable workspace namespace. It is not a
FUSE frontend, not a POSIX filesystem, and not an object store. It is the
metadata kernel consumed by AI workload stores, artifact SDKs, workspace
services, DFS frontends, and object namespace layers.

Core primitives:

| Primitive | Semantics |
|---|---|
| `Create` | Atomically creates a dentry and inode. |
| `Lookup` / `LookupPlus` | Reads a dentry, optionally fused with inode attributes. |
| `ReadDir` / `ReadDirPlus` | Reads a directory page, optionally fused with inode attrs. |
| `UpdateInode` | Updates mutable inode metadata and bounded opaque attrs. |
| `Rename` | Moves one namespace entry when the destination does not exist. |
| `RenameReplace` | Atomically publishes or overwrites a file entry. |
| `RenameSubtree` | Moves a subtree root with rooted authority handoff. |
| `Remove` | Removes one non-directory entry and returns removed inode metadata. |
| `RemoveDirectory` | Removes one empty directory after child-count verification. |
| `Link` / `Unlink` | Non-directory hard-link semantics with link-count updates. |
| `SnapshotSubtree` | Creates an MVCC read-version token for point-in-time reads. |
| `WatchSubtree` | Prefix-scoped change feed with cursor, ack, replay, and overflow handling. |
| Writer sessions | Open, heartbeat, close, and expire staged writers. |
| Quota usage | Persistent counters plus rooted quota fences. |

`InodeRecord.OpaqueAttrs` is application-owned metadata capped at 16 KiB. It is
for compact body references, checksums, media types, or small descriptors, not
large artifact bodies.

## Artifact Publish Model

Large bodies are written outside `fsmeta`; the metadata commit happens inside
`fsmeta`.

1. Write the body to a `BodyStore`.
2. Encode the body reference into inode opaque attrs.
3. Create a hidden staged namespace entry.
4. Publish it to the final path with `RenameReplace`.

Readers observe either the old body reference or the new one, never a
half-published artifact path.

## Local Runtime

The local runtime is the default path for demos, small teams, and local agent
workspace deployments.

```go
package main

import (
	"context"

	"github.com/feichai0017/NoKV/fsmeta/model"
	fsmetalocal "github.com/feichai0017/NoKV/fsmeta/runtime/local"
)

func main() {
	ctx := context.Background()
	rt, err := fsmetalocal.Open(ctx, fsmetalocal.Options{
		WorkDir: "./nokv-fsmeta-local",
		Mount:   model.MountIdentity{MountID: "default", MountKeyID: 1},
	})
	if err != nil {
		panic(err)
	}
	defer rt.Close()

	_, err = rt.Executor.Create(ctx, model.CreateRequest{
		Mount:  "default",
		Parent: model.RootInode,
		Name:   "hello.txt",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile, Size: 13},
	})
	if err != nil {
		panic(err)
	}
}
```

## Distributed Runtime

The distributed path runs the same fsmeta contract through raftstore:

```text
fsmeta client/server
  -> fsmeta/runtime/raftstore
  -> coordinator + meta/root + TSO
  -> raftstore
  -> txn
  -> storage/kv
```

Use it for larger agent platforms, distributed workspace services, and
DFS-scale namespace metadata.

```sh
./scripts/dev/cluster.sh --config ./raft_config.example.json
docker compose up -d
```

Bootstrap a mount before the first distributed write:

```sh
nokv mount register --coordinator-addr 127.0.0.1:2379 \
  --mount default --root-inode 1 --schema-version 1
```

## Development

Common checks:

```sh
go test ./...
make lint
make test
make fsmeta-bench
```

Repository structure and review rules live in
`docs/guide/development/code_contract.md` and
`docs/guide/development/pr_review_checklist.md`. Those two files are kept as
the source of truth for package boundaries, naming, tests, DCO, generated code,
and distributed-safety review.

## License

[Apache-2.0](./LICENSE)
