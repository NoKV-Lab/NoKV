<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Architecture

NoKV is a Rust-first filesystem for AI training and agent workspaces. The
repository is intentionally product-shaped: metadata semantics, object body
storage, clients, FUSE, docs, and examples live at the repository root instead
of behind a nested workspace.

The implemented tree is the Rust client/server filesystem slice: FUSE and the
SDK talk to `nokv-server`, which commits semantic metadata commands into an
embedded Holt MVCC engine on a **single node**. Distributed metadata is **not
implemented** — the planned direction (subtree sharding + owner-lease + epoch
fencing, *not* consensus-replicated metadata) is described under
[Distributed Direction](#distributed-direction). Python/fsspec now has an
SDK binding over the native batch range-read path plus a caller-owned buffer
staging shape for dataloaders; production metadata HA, CSI, and node-local cache
are recorded in [Product Design](./product-design.md).

## Layers

```text
Application surface
  nokv-client    Rust SDK
  nokv-python    Python/fsspec binding over the Rust SDK
  nokv           CLI
  nokv-fuse      low-level FUSE frontend
  nokv-csi       planned Kubernetes CSI integration

Metadata layer
  nokv-types     mount, inode, dentry, body descriptor, watch event types
  nokv-protocol  metadata RPC wire DTOs
  nokv-meta      schema, MetadataCommand, Holt store, service core
  nokv-server    long-running metad process, RPC, health, and control plane

Body storage layer
  nokv-object    S3-compatible object storage, including RustFS
```

## Write Path

```mermaid
flowchart LR
    App["AI training / agent client"] --> API["NoKV metad"]
    App["FUSE / SDK / CLI"] --> API["nokv-meta service"]
    API --> Command["MetadataCommand"]
    Command --> Holt["Holt metadata store"]
    API --> Object["S3-compatible object store"]
```

For artifact publication, object bytes are uploaded first. The metadata commit
then publishes the dentry, inode projection, and body descriptor atomically.
Failed metadata publish leaves staged objects for later garbage collection.

`nokv-server` runs the same local `nokv-meta` service in a long-lived
process. It owns health, readiness, stats, manual GC endpoints, and the first
metadata RPC. The SDK hot path uses a length-prefixed framed RPC on the same
port; HTTP stays limited to health, stats, and manual GC control. The RPC
supports both inode/name operations and path-oriented SDK operations, so
server-side path resolution can avoid multi-round-trip nested creates. It also
supports ordered non-atomic batches: each subrequest has its own result/error,
but the batch removes per-operation network round trips for SDK workloads. The
Rust SDK has a metadata client for namespace operations and an object-backed
file client that uploads object blocks directly, asks `metad` to atomically publish
the body manifest, opens a native layout plan through `OpenPathReadPlan`, and
reads object ranges directly from the configured object store. `ReadBodyPlan`
remains available for generation-scoped follow-up reads and prefetch. Read plans
carry immutable block keys, range offsets, and `digest_uri`, so the data path can
choose local hot-tier reads or S3-compatible object reads without adding
placement truth to metadata.
The server stats endpoint reports
metadata-store write attribution counters so benchmark runs can distinguish
current writes, history writes, watch writes, and dedupe writes. The FUSE
frontend uses the same metadata client/server boundary as the SDK.

## FUSE Path

The current FUSE frontend is inode-first. It maps kernel `lookup`, `getattr`,
`readdir`, `open`, and `read` calls to `metad` inode APIs and object-store range
reads. It does not resolve paths through the Rust SDK and does not own metadata
semantics. Live mounts register observed directory scopes with the metadata
watch log and translate typed watch events into FUSE `inval_entry` and
`inval_inode` notifications. Snapshot mounts are read-only and do not start the
invalidation worker.

## Metadata Layout

The canonical model is inode/dentry, described in
[Metadata Schema](./metadata-schema.md):

```text
inode_current:
  mount_id | inode_id -> inode attributes

dentry_current:
  mount_id | parent_inode | name -> dentry + inode projection

chunk_manifest_current:
  mount_id | inode_id | generation | u64::MAX -> body summary
  mount_id | inode_id | generation | chunk_index -> block manifest

history:
  family | user_key_len | user_key | inverted_commit_version -> old value
```

Path indexes are derived accelerators for artifact and checkpoint fast paths;
they are not namespace truth.

## Data Fabric And Object Storage

NoKV stores file bodies outside the metadata service. File bytes are split
into immutable object blocks and published through metadata manifests. The first
production body backend is S3-compatible storage. RustFS, MinIO, Ceph RGW, and
AWS S3 all use the same object-store boundary. See
[Object Layout](./object-layout.md).

The metadata manifest is the durable truth for block identity and cold storage:
`inode`, `generation`, logical offsets, block digest, and S3-compatible object
key. It must not record node-local NVMe paths or cache slots. Those belong to
the data path as soft placement state.

The planned hot path is layered behind the same immutable block contract:

```text
layout lease -> block descriptors -> data fabric
  -> local NVMe hot tier
  -> S3-compatible cold durable tier
```

That boundary keeps local placement out of metadata semantics. A hot-tier read
can miss or fail; the S3-compatible object key remains the durable fallback.

The current `nokv-object` data-fabric skeleton provides `LocalObjectStore` for a
node-local hot tier, `TieredObjectStore` for hot-first/cold-fallback reads,
`ObjectStore::get_many` for batched block fetches, and
`resolve_block_placements` for soft local-vs-object placement decisions. The
local hot tier rebuilds its residency index from disk on open, can enforce a
configured byte cap with LRU eviction, and reports resident bytes, evictions,
and admission rejections. Cold-read hot fills can run inline or in the
background; background fills coalesce duplicate in-flight object keys.
`LayoutReadExecutor` consumes the metadata layout-open plan through the existing
read pipeline, records transport counters, and preserves the batch/coalescing
contract. Its batch layout path combines blocks from multiple read plans before
calling the object store, so adjacent ranges and multi-sample reads can share one
`get_many` call. The SDK range path sits above this layer and only coalesces
logical offsets within one immutable file generation; metadata still sees normal
layout opens, and the object layer still sees immutable block descriptors and
durable object keys.

## Metadata Disaster Recovery

File bodies are durable in the object store, but the namespace that gives them
meaning — inodes, dentries, versions, and CoW relationships — lives in the local
Holt engine. Losing that node would lose the namespace even though every object
survives. To close that single point of total loss, the metadata engine is
periodically archived to the same object store.

A background worker exports a Holt checkpoint image and publishes it under a
configurable object-key prefix (`--metadata-checkpoint-archive-prefix`, on by
default; disable with `--no-metadata-checkpoint-archive`). Publication mirrors
the body write path — **object-first, pointer-second**:

```text
1. checkpoint image  -> {prefix}/ckpt/{seq}.image     (object-first)
2. CURRENT manifest  -> {prefix}/CURRENT              (atomic pointer swap)
3. prune checkpoints older than the retained window   (after the swap)
```

The single `CURRENT` object names the live checkpoint and the retained-checkpoint
window, so retention works without an object `list`. A crash between steps 1 and
2 leaves an orphan checkpoint object (reclaimed on a later backup), never a
manifest that points at a missing checkpoint.

Recovery runs on a replacement node with an empty metadata directory:

```text
nokv restore        # GET CURRENT -> GET checkpoint -> install into a fresh store
nokv serve          # resume serving the recovered namespace
```

`restore` installs the checkpoint into a fresh Holt store (which must be empty — a
checkpoint install cannot merge into a populated store) and rehydrates the
allocator, so the recovered node both serves the prior namespace and accepts new
writes. `nokv backup` triggers an out-of-band archive on a running server, and
`/stats` reports the worker's `metadata_backup` state. The recovery-point
objective is the worker interval; the bodies were always safe in the object store.

## Consistency Checking

`nokv fsck` performs the explicit, full consistency scan. It walks current file
and object-backed symlink bodies plus every body reachable from an unexpired
snapshot pin or durable `ForkBinding`. Sparse/append bodies are resolved through
their complete `base_generation` chain. Every referenced block is checked with
`head` for both existence and exact length, so the report catches out-of-band
deletion, truncated replacement, object-store anomalies, and latent metadata
bugs without treating retained history as orphaned.

Durable workbench restore adds a second graph to this check. Fsck validates the
operation/claim/root graph, temporary history binding, staging member inverses,
exact object owner/inverse/seal rows, initialization tombstones, cleanup and
release jobs, quarantine envelopes, and the private index overlay (including
its physical MVCC envelope and owner/inverse closure). Optional object
verification confirms each exact borrowed reference still has both a backing
object and an effective borrower manifest. A metadata write concurrent with
either scan invalidates the report instead of returning a mixed-version result;
the operator retries.

Full fsck is intentionally expensive. `/stats` does not run it: restore gauges
count private keyspaces in bounded pages, strictly decode only operation rows,
and cache the last exact, version-labelled sample briefly at the server. This
keeps monitoring memory bounded without presenting a sampled count as exact.
Process-local metadata-service counters separately expose restore request,
success/failure, total elapsed nanoseconds, and maximum elapsed nanoseconds;
they include lock wait and exact terminal retries but reset on owner restart.
Reclaiming the opposite drift—objects written but never referenced and absent
from a restore initialization tombstone—still requires an object-store `list`
and remains a separate extension.

The `restore_to_fork_v1` downgrade fence may be removed only through the typed
drain API after deployment routing has disabled the capability and every restore
writer has been globally stopped or fenced. The drain re-deletes and CAS-removes
late-PUT tombstones, requires every registered restore control/index prefix to
be empty, then atomically rewrites allocator v2 to v1, removes the active marker,
and rotates the open object-GC claim. A clean full fsck and a fresh metadata
checkpoint are still mandatory before starting a pre-restore metadata binary.

## Distributed Direction

**Status: not implemented.** Today NoKV is a single-node metadata service — one
embedded Holt MVCC engine owns the entire namespace. There is no replication, no
consensus group, and no `nokv-cluster` crate. This section records the *planned*
direction, not shipped behavior.

The planned distributed layer is deliberately **not** consensus-replicated
metadata (that would double-log against Holt's own MVCC and erase the
embedded-engine advantage) and **not** a mandatory external transactional KV. The
direction is **subtree sharding + owner-lease + epoch fencing**:

- **Shard by subtree.** Every key is already mount-prefixed and dentries are
  parent-clustered, so a subtree maps to contiguous key ranges with no key-format
  change. One single-owner Holt engine serves each shard. Because all N shards of
  one checkpoint live under one subtree, the common atomic publish stays a
  single-shard, single-engine transaction — no cross-shard commit on the hot path.

- **A small control group grants leases and holds only the shard map.** A 3–5 node
  consensus group (the *only* consensus in the system) replicates a
  kilobyte-scale routing table `{range → owner, epoch, lease, image_pointer}` and
  grants / renews / revokes owner-leases. It never replicates the metadata log —
  the metadata *truth* stays single-owner in each shard's Holt.

- **Epoch fencing.** Each shard carries a monotonic ownership epoch (the
  `allocator` record already persists one, recovered with `fetch_max`). On owner
  change the control group bumps the epoch and a deposed owner's commits are
  rejected at the metadata commit boundary. The service-local commit fence exists
  today for single-command and independent-batch commits, and `nokv-server` can
  acquire a `nokv-control` shard lease, install that epoch into the fence, and
  renew it from a server-owned background worker. An optional etcd-backed
  store/session backend is wired through server and CLI config, and the local
  multi-process HA smoke covers owner death, epoch-2 failover, shared-log replay,
  post-failover writes without replayed-inode reuse, and machine-readable timing
  metrics for local RTO rows. A local stale-owner mode pauses and resumes owner
  A to verify the resumed epoch-1 owner rejects writes after epoch-2 failover.
  Multi-machine deployment and network-partition chaos gates remain distributed
  hardening work.

- **Failover reuses the DR path.** The metadata backup/restore mechanism (see
  Metadata Disaster Recovery, above) — export a Holt checkpoint image to object
  storage, install it on a fresh node — is also the shard-handoff primitive. A new
  owner restores the shard image, replays the WAL tail, and takes the new epoch.
  Zero-loss failover additionally requires per-epoch WAL-tail streaming,
  allocation-independent request IDs for dedupe, and an atomic install-into-live
  primitive in Holt — none of which exist yet.

Cross-shard atomic operations (a rename that straddles shards) are out of the v1
contract; the hot path never needs them.

The data fabric is separate from this ownership protocol. NVMe residency is
cache state keyed by immutable block identity; it does not decide namespace
visibility and does not participate in `MetadataCommand` atomicity.
