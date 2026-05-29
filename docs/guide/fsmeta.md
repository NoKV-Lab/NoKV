<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# FSMetadata

## TL;DR

- Topic: NoKV's workspace namespace metadata substrate.
- Core objects: Mount, Inode, Dentry, SubtreeAuthority, SnapshotEpoch, QuotaFence, UsageCounter.
- Default local call chain: `fsmeta/runtime/local -> fsmeta/exec -> fsmeta/backend.Store -> local MVCC`.
- Distributed call chain: `fsmeta/client -> fsmeta/server -> fsmeta/exec -> fsmeta/backend.Store implemented by fsmeta/runtime/raftstore -> raftstore + txn/percolator + coordinator/meta-root`.
- Experimental Peras call chain: `fsmeta/exec -> Peras visible runtime -> witness quorum + segment install`.
- Code contract: wire is in `pb/fsmeta/fsmeta.proto`, the executor is in `fsmeta/exec`, local runtime is `fsmeta/runtime/local.Open`, and the scale-out adapter is `fsmeta/runtime/raftstore.Open`.

## 1. Conclusion

`fsmeta` is NoKV's native workspace metadata service. It isn't a FUSE frontend, it doesn't handle object body I/O, and it doesn't promise full POSIX. What it provides is a durable, versioned, watchable namespace shaped like filesystem metadata. Agent workspaces are the primary product path; distributed filesystems and object-storage namespaces can consume the same contract when they need a metadata service.

The value of this layer isn't picking a few keys to encode inode/dentry. The real boundary is: common namespace operations are exposed as server-side primitives, instead of asking each upper-layer application to stitch a protocol out of `Get` / `Put` / `Scan`.

## 2. Current API

The current v1 API is defined by `pb/fsmeta/fsmeta.proto`. `fsmeta/server` exposes gRPC; `fsmeta/client` provides a Go typed client.

| RPC | Current semantics |
|---|---|
| `Create` | Atomically creates a dentry and inode; the server uses `AssertionNotExist` to reject duplicate creation. |
| `UpdateInode` | Atomically updates mutable inode fields (`size`, `mode`, `updated_unix_ns`, `opaque_attrs`) and applies the size quota delta. v1 is path-anchored by `(parent, name, inode)` and rejects hard-linked inode updates. |
| `Lookup` | Read a dentry by `(mount, parent_inode, name)`. |
| `ReadDir` | Scan one directory page by dentry prefix. |
| `ReadDirPlus` | Scan dentries and batch-read inode attrs under the same snapshot version. |
| `WatchSubtree` | A prefix-scoped change feed; supports ready, ack, back-pressure, and bounded cursor replay. Cursor expiry requires full-state reconcile. |
| `SnapshotSubtree` | Publishes an MVCC read-version token; subsequent `ReadDir` / `ReadDirPlus` can use it to read that version. Data-plane GC retention is a separate boundary. |
| `RetireSnapshotSubtree` | Proactively retire a snapshot epoch. |
| `GetQuotaUsage` | Read the persistent quota usage counter for a mount/scope. |
| `RenameSubtree` | Atomically move the root dentry of a subtree; descendants follow naturally via inode references. |
| `Link` | Create a second dentry for an existing non-directory inode and increment link count in the same transaction. |
| `Unlink` | Delete a dentry; decrement link count and delete the inode record when the last link is removed. |
| `Remove` | Product-facing remove primitive for one non-directory namespace entry; it shares `Unlink`'s link-count semantics. |
| `RemoveDirectory` | Delete one empty directory dentry; atomically checks the target inode is a directory and has no children. |
| `OpenWriteSession` / `HeartbeatWriteSession` / `CloseWriteSession` / `ExpireWriteSessions` | Maintain exclusive writer leases for file inodes with a session-id key plus an inode-owner key. Expiry cleanup uses server time and is bounded/repeatable. |

Expired writer sessions are reclaimed by a bounded background pass. Set the
cleanup interval to roughly half of the smallest expected session TTL when fast
lease takeover matters; after a writer stops heartbeating, a stale session may
remain visible until the next cleanup pass.

## 3. Data model

`fsmeta`'s ordered key schema is in `fsmeta/layout`; storage-engine-neutral
record types live in `fsmeta/model`.

| Object | Storage location | Notes |
|---|---|---|
| Mount metadata key | `EncodeMountKey` | Reserved mount-level data key; mount lifecycle truth does not live here. |
| Inode | `EncodeInodeKey(mount, inode)` | File/directory attributes, including `size`, `mode`, `link_count`, and a bounded `opaque_attrs` payload. |
| Dentry | `EncodeDentryKey(mount, parent, name)` | Mapping from parent/name to inode. |
| Chunk | `EncodeChunkKey(mount, inode, chunk)` | Schema is in place; the current fsmeta API doesn't expose object body / chunk I/O. |
| Session | `EncodeSessionKey(mount, session)` and `EncodeInodeSessionKey(mount, inode)` | Writer lease state. The session key supports heartbeat/close by session ID; the inode-owner key enforces one live writer per inode. |
| Usage | `EncodeUsageKey(mount, scope)` | Quota usage counter; scope=0 means mount-wide, non-zero means a direct accounting scope. |

Both keys and values carry a magic + schema version. Values use a hand-written binary layout — not JSON.

`InodeRecord.opaque_attrs` is application-owned bytes capped at 16 KiB. It is intended for compact body references, checksums, content type, or a caller-defined protobuf payload. NoKV stores and returns it, but does not parse, index, authorize, or quota it separately.

## 4. Execution boundary

`fsmeta/exec.Executor` depends on one narrow storage contract in
`fsmeta/backend`:

```go
type Store interface {
    ReserveTimestamp(ctx context.Context, count uint64) (uint64, error)
    Get(ctx context.Context, key []byte, version uint64) ([]byte, bool, error)
    BatchGet(ctx context.Context, keys [][]byte, version uint64) (map[string][]byte, error)
    Scan(ctx context.Context, startKey []byte, limit uint32, version uint64) ([]backend.KV, error)
    Mutate(ctx context.Context, primary []byte, mutations []*backend.Mutation, startVersion, commitVersion, lockTTL uint64) (uint64, error)
    MutateAtCommit(ctx context.Context, primary []byte, mutations []*backend.Mutation, startVersion, commitVersion, lockTTL uint64) (uint64, error)
}
```

`fsmeta/backend` deliberately has no protobuf, raftstore, local DB, concrete
storage backend, SST, or migration types. `fsmeta/exec` produces backend-neutral
mutations and predicates; runtime adapters translate those into their concrete
commit path.
For example, `fsmeta/runtime/local` applies them through the embedded local
MVCC implementation, while `fsmeta/runtime/raftstore` converts them to
raftstore KV RPC/proto mutations inside the adapter package.

The default product runtime is `fsmeta/runtime/local.Open`. It wires one embedded database, one local mount admission record, a local inode allocator, a durable MVCC runner, and local watch/snapshot adapters. This is the path for demos, agent workspace integrations, and small deployments.

The scale-out runtime is `fsmeta/runtime/raftstore.Open`. It wires coordinator, raftstore client, TSO, watch source, mount/quota cache, snapshot publisher, and subtree handoff publisher. It exists for DFS-scale metadata and multi-node agent platforms.

Peras is an experimental runtime path for visible-before-durable metadata execution. A Peras write may return at a **visible** boundary before the **durable** boundary is reached by witness quorum plus segment install into raftstore. That mechanism should not be required to understand the stable fsmeta API.

The layering constraints are:

- `Executor` does not directly know about raft region / store routing.
- `fsmeta/runtime/local` is the default embedded adapter.
- `fsmeta/runtime/raftstore` is the scale-out adapter; it owns the raftstore wiring.
- `meta/root` does not store high-frequency inode/dentry data — only lifecycle / authority truth.
- `raftstore` and `percolator` don't understand fsmeta operations; they provide transactions and apply observation. Existing Peras install support is being isolated behind the experimental boundary.
- Raftstore peer bootstrap snapshots and concrete storage diagnostics are
  operational capabilities of the distributed runtime. They are not part of the
  generic fsmeta backend contract.

## 5. Native primitives

### ReadDirPlus

`ReadDirPlus` returns dentries and inode attrs under one snapshot version. The
service performs the dentry scan and inode attr fetch as one metadata primitive,
so callers do not have to stitch a directory page from a scan plus per-entry
point reads.

Strict semantics: if any inode is missing or fails to decode, the whole page returns an error. fsmeta does not return half-true directory pages.

### WatchSubtree

`WatchSubtree` subscribes to an fsmeta key prefix and externally exposes a `(region_id, term, index)` cursor and a `commit_version`. Event sources include:

- a successful `CMD_COMMIT`;
- `CMD_RESOLVE_LOCK` with `commit_version != 0`;
- a successful `CMD_INSTALL_PREPARED_MVCC` carrying `watch_keys`.

`CMD_PREWRITE`, rollback, and diagnostic commands do not produce visible events.

v1 already supports:

- ready signal;
- back-pressure window;
- ack;
- per-region recent ring;
- resume cursor replay;
- `ErrWatchCursorExpired` when a cursor has expired.

`ErrWatchCursorExpired` is not a fatal protocol state. The client-side recovery
pattern is: open a fresh watch without the old cursor, take a full `ReadDirPlus`
baseline for the watched directory, then apply live events idempotently. The Go
typed client exposes this as `client.WatchDirectoryWithReconcile`.

### SnapshotSubtree

`SnapshotSubtree` only publishes a read epoch — it does not copy the directory tree. The token shape is `(mount, root_inode, read_version)`. Subsequent `ReadDir` / `ReadDirPlus` use `snapshot_version` to read the same MVCC view.

`SnapshotEpochPublished` / `SnapshotEpochRetired` rooted events are already in place. Coordinator root views carry active snapshot epochs and expose the oldest active `read_version` as the MVCC-GC retention floor. The raftstore MVCC maintenance worker uses that floor when planning replicated version cleanup, so active snapshot epochs keep their required MVCC history until retired.

### RenameSubtree

The current dentry schema references the parent directory via `parent_inode_id`, so a subtree rename's physical write volume is the same as a regular rename: delete the old root dentry, write the new root dentry. Descendants don't have to be rewritten one by one.

The extra semantics live in the authority layer:

- publish `SubtreeHandoffStarted` before mutation;
- the dentry mutation goes through `backend.Store.MutateAtCommit` using the rooted handoff commit timestamp;
- publish `SubtreeHandoffCompleted` after mutation;
- the runtime monitor uses `WatchRootEvents` to discover pending handoffs and complete them.

This design prioritizes guaranteeing that rooted authority can never get stuck in an unknown state forever. In extreme cases it may advance an empty era — but it will not leave behind an orphaned pending handoff that no one will repair.

### Staged publish

Large artifact bodies live outside fsmeta, but the namespace commit point can be
made atomic. The Go typed client provides
`client.PublishStagedNamespaceEntry` for the standard pattern:

1. create a staged dentry/inode, usually under a private staging parent or
   hidden staging name;
2. let the caller write or validate the external body reference;
3. rename the staged entry to the final path with `RenameSubtree`.

If body preparation fails, the helper best-effort unlinks the staged entry. If
the final rename fails, it leaves staging in place so the caller can inspect or
retry without losing prepared external state. NoKV still does not parse or write
the object body; callers can store compact body references in
`InodeRecord.opaque_attrs`.

### Link / Unlink / Remove / RemoveDirectory

`Link` is allowed only for non-directory inodes. It creates a new dentry and increments `InodeRecord.LinkCount` in the same transaction.

`Unlink` deletes one dentry and updates the inode based on link count:

- link count > 1: decrement and write back the inode;
- link count ≤ 1: delete the inode record.

`Remove` exposes the same single-entry delete semantics under the product-facing
name used by workspace clients. It is deliberately non-directory.

`RemoveDirectory` is the directory counterpart for empty directories. It removes
the directory dentry only when the target inode is a directory and its
`ChildCount` is zero in the same transaction. This is intentionally narrower
than recursive subtree deletion: callers can safely implement recursive cleanup
above it today, while a future `DeleteSubtree` can return removed body refs for
GC without weakening the empty-directory contract.

Hard links and removes of directories remain illegal.

`UpdateInode` is deliberately narrower than POSIX `setattr`. It does not change
inode identity, type, creation time, or link count. Because v1 has no reverse
index from inode to every parent dentry, updating a hard-linked inode is rejected
instead of producing stale quota counters or stale `ReadDirPlus` pages.

### Quota Fence

The quota fence is rooted truth; the usage counter is a data-plane key. The write path packs the usage counter mutation and the dentry/inode mutation into the same Percolator transaction.

This solves two problems:

- multiple `nokv-fsmeta` gateways won't each maintain a local counter and breach the limit;
- a gateway restart won't lose usage.

Fence changes are pushed to the fsmeta runtime via the coordinator root-event stream; on cache miss, the runtime falls back to querying the coordinator.

## 6. Rooted truth vs runtime view

| Domain | Rooted truth | Runtime view |
|---|---|---|
| Mount | `MountRegistered` / `MountRetired` | fsmeta mount admission cache; a retired mount closes related watch subscriptions. |
| Subtree authority | `SubtreeAuthorityDeclared` / `SubtreeHandoffStarted` / `SubtreeHandoffCompleted` | RenameSubtree frontier, pending handoff repair. |
| Snapshot epoch | `SnapshotEpochPublished` / `SnapshotEpochRetired` | snapshot-version reads. |
| Quota fence | `QuotaFenceUpdated` | quota fence cache + persisted usage counter keys. |
| WatchSubtree | Not in `meta/root` | raftstore apply observer + fsmeta router. |

On startup, `nokv-fsmeta` first pulls `ListMounts` / `ListQuotaFences` / `ListSubtreeAuthorities` for bootstrap, then follows subsequent changes via `WatchRootEvents`. `MonitorInterval` is the reconnect backoff after the root-event stream drops — not a steady-state polling interval.

## 7. Deployment

Docker Compose brings up meta-root, coordinator, raftstore, and fsmeta gateway, and registers a default mount via `mount-init`:

```bash
docker compose up -d
```

Start the fsmeta gateway directly:

```bash
go run ./cmd/nokv-fsmeta \
  --addr 127.0.0.1:8090 \
  --coordinator-addr 127.0.0.1:2390,127.0.0.1:2391,127.0.0.1:2392 \
  --metrics-addr 127.0.0.1:9400 \
  --session-cleanup-interval 30s
```

Register a mount:

```bash
nokv mount register \
  --coordinator-addr 127.0.0.1:2390,127.0.0.1:2391,127.0.0.1:2392 \
  --mount default \
  --root-inode 1 \
  --schema-version 1
```

Set quota:

```bash
nokv quota set \
  --coordinator-addr 127.0.0.1:2390,127.0.0.1:2391,127.0.0.1:2392 \
  --mount default \
  --limit-bytes 10737418240 \
  --limit-inodes 10000000
```

## 8. Metrics

`nokv-fsmeta --metrics-addr` exposes five expvar groups:

| Namespace | Meaning |
|---|---|
| `nokv_fsmeta_executor` | transaction retry / retry exhausted. |
| `nokv_fsmeta_watch` | subscribers, events, delivered, dropped, overflow, remote source state. |
| `nokv_fsmeta_mount` | mount cache hit/miss, admission rejects. |
| `nokv_fsmeta_quota` | fence check/reject, cache hit/miss, fence updates, usage mutations. |
| `nokv_fsmeta_sessions` | stale writer-session cleanup runs, expired sessions, and last error. |

## 9. Benchmarks

The fsmeta benchmark lives in `benchmark/fsmeta` and drives the native
`nokv-fsmeta` gRPC service against a running NoKV cluster. The default Docker
Compose helper runs each workload in isolation:

```bash
make fsmeta-bench
```

Workload provenance and default scales are kept in
`benchmark/fsmeta/profiles/official/workloads.yaml`, which records the public
IO500/mdtest, Filebench varmail, MimesisBench, and MLPerf Storage
checkpointing source links, their official shape, and NoKV's fsmeta projection.
It is a normalized service-benchmark profile, not a vendored copy of those
benchmark implementations and not a certified third-party score.

The default script scale is the `median` profile and is intentionally larger
than a smoke test: 12 clients, mdtest/mimesis directory trees of 16 x 256 files,
16 varmail users with 128 messages each, and 4 AI workspaces with 64 checkpoint
publishes each. Use `NOKV_FSMETA_PROFILE=long` for scheduled larger runs or
`NOKV_FSMETA_PROFILE=official` for the profile's official-size shape. After the
ports are reachable, the script waits 20 seconds for Raft leadership and
coordinator grants to settle. Override `NOKV_FSMETA_STABILIZE_SECONDS` when
testing an already-warm cluster.

The matrix is isolated by default: the script runs one workload per Go test
process against a fresh Docker Compose data volume, writes one CSV per workload,
and then appends the rows into an `_isolated.csv` summary. This keeps one
workload from measuring the previous workload's Peras overlay, recovery state,
or background segment installation backlog. Set
`NOKV_FSMETA_RESET_BETWEEN_WORKLOADS=0` for same-cluster soak profiling; in
that mode the harness waits for install and seal queues to become idle between
workloads. `NOKV_FSMETA_PERAS_IDLE_REQUIRE_PENDING=1` is intentionally opt-in
for durable-drain experiments, because `pending` counts visible operations that
may remain in the holder overlay.
CSV rows include both `throughput_ops_sec` (operation count divided by whole
workload wall time) and `active_ops_per_sec` (operation count divided by that
operation's measured active latency). Use the active column when a workload has
watch waits, snapshot barriers, or slow side operations.

The default suite is official-aligned rather than a synthetic KV loop:

- `mdtest-easy`: private-directory create/stat/ReadDirPlus/unlink, following the
  easy IO500/mdtest metadata shape with zero-byte files.
- `mdtest-hard`: shared-directory create/stat/ReadDirPlus/unlink, following the
  hard IO500/mdtest contention shape with 3901-byte file metadata records.
- `filebench-varmail`: mailbox-style create/update/session/readdir/unlink,
  following Filebench varmail's small-file mail-spool pattern. The upstream
  `varmail.f` personality defaults to `nfiles=1000`, `nthreads=16`, mean file
  size 16 KiB, and mean append size 16 KiB; fsmeta maps body appends and fsyncs
  to inode-size updates and writer-session lifecycle operations.
- `mimesis-namespace`: namespace churn with create/rename/setattr/lookup/scan
  and unlink, aligned with MimesisBench-style trace replay.
- `ai-checkpoint-agent`: agent/ML checkpoint publication with artifact fan-out,
  manifest update, writer-session lifecycle, watch notification, snapshot read,
  and snapshot retirement. The scale motivation comes from MLPerf Storage v2.0
  checkpointing, which defines Llama-style 8B/70B/405B/1T model scales; NoKV
  measures the metadata publish side rather than checkpoint body throughput.

Every fsmeta benchmark CSV includes `source`, `source_url`, and `projection`
columns. The test also writes a sibling `.manifest.txt` file with exact scale
parameters, selected scale profile, official source links, and the fsmeta
projection used for each workload. Treat results without this manifest as
incomplete evidence.

## 10. Non-goals

- No FUSE / NFS / SMB frontend.
- No S3 HTTP gateway or object body I/O.
- No indexing or interpretation of `InodeRecord.opaque_attrs`.
- No write of every inode/dentry mutation into `meta/root`.
- No recursive materialized snapshot — `SnapshotSubtree` is an MVCC read epoch.
- No POSIX symlink/device/FIFO surface in the current fsmeta API.
- No automatic fsmeta object repair beyond stale writer-session cleanup. Normal
  `Unlink` deletes the last-link inode in the same transaction; broader
  corruption repair needs an explicit reverse-index or audit design rather than
  heuristic background deletion.
