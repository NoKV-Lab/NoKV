<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Historical: NoKV as AI-Infra Storage — Architecture & Design

> [!WARNING]
> **Historical design exploration — archived 2026-07-30.** This document
> records an earlier AI-training/data-plane direction and is not the current
> product contract. NoKV is now positioned as an object-backed metadata control
> plane for durable agent workspaces. Current distributed support is
> experimental path sharding with one active writer per Holt-backed shard,
> epoch-fenced failover, and shard-local atomic publication; it is not consensus
> replication or a cross-shard transaction layer. Enterprise multi-machine
> throughput and hardening remain work in progress. See the current
> [README](../../README.md), [architecture](../architecture.md),
> [product design](../product-design.md),
> [metadata sharding and recovery](../metadata-sharding-and-recovery.md), and
> [benchmark boundary](../benchmarks.md).

> **Thesis.** NoKV is an **object-backed, metadata-first file system** for AI
> infrastructure. Its wedge is **atomic, parallelism-agnostic checkpoint publish +
> metadata correctness + cloud-native cost** — *not* raw bandwidth. The one law
> that governs every decision: **storage exists to keep the GPU fed**, so the
> design is ruthlessly specialized to the narrow, known AI access patterns
> (dataset-shard read, checkpoint write/read, weight cold-load, KV-cache) on a
> disaggregated object+NVMe substrate, with **metadata structurally off the data
> path**.

This document calibrates NoKV against the SOTA (DeepSeek 3FS, JuiceFS, Alluxio,
WekaFS/VAST, NVIDIA AIStore, Mooncake/Tair KV-cache, ByteCheckpoint/PyTorch DCP)
and records the design decisions, the honest non-goals, and the build roadmap.

---

## 1. Positioning & Non-Goals

NoKV sits in the **JuiceFS / Alluxio architectural family**: file bodies as
immutable blocks in S3-compatible object storage + a separable metadata engine +
FUSE/SDK access + a local cache. It adds a **checkpoint / CoW-lifecycle
specialization** that JuiceFS and Alluxio do not have.

| | metadata plane | data path | checkpoint | KV-cache | substrate |
|---|---|---|---|---|---|
| **3FS** | stateless servers over FoundationDB (SSI txns) | RDMA + USRBIO zero-copy, no page cache | async batch, >10 GiB/s/node | 3FS-KV on-disk | captive NVMe + IB |
| **WekaFS / VAST** | sharded/distributed, always on flash | NVMe + GPUDirect, TiB/s | snapshot-based | Augmented Memory Grid | captive NVMe-oF |
| **JuiceFS** | pluggable external KV (Redis/TiKV/FDB) | object + multi-tier cache | bolt-on atomicity | — | object storage |
| **Alluxio** | master/worker | distributed cache/tiering | — | — | over under-stores |
| **Mooncake/Tair** | external KV + light manager | tiered DRAM/SSD/RDMA | — | prefix-hash KV pool | DRAM/SSD/RDMA |
| **NoKV** | **embedded Holt (MVCC) + owner-lease**, denormalized dentries | object + BlockCache/Writeback/Prefetch | **native atomic publish + CoW version pin** | content/prefix-hash namespace (planned) | object storage |

**Non-goals (stated up front, because claiming otherwise loses credibility with
these teams):**
- **Not a TiB/s bandwidth competitor.** 3FS (6.6 TiB/s), WekaFS/VAST (8–9 TB/s,
  192 GiB/s/client GDS) win the bandwidth axis with captive RDMA hardware. NoKV
  does not, and will not pretend to.
- **Not an inference-engine cache.** RadixAttention / LMCache live in
  vLLM/SGLang. NoKV is the **backend** those connectors target.
- **Not a mandatory-FoundationDB system.** A required heavy external metadata
  authority erases NoKV's embedded-Holt ownability advantage.
- **Not a consensus-replicated metadata payload.** No SOTA AI-storage system
  Raft-replicates metadata for scale-out.
- **Not a general, POSIX-complete DFS.** Completeness is sacrificed wherever it
  costs the GPU-feed law.

---

## 2. The Load-Bearing Principle

> **The control plane holds ALL truth; the data plane holds NO truth; the two are
> decoupled only by an immutable layout lease.**

Two consequences make the whole design tractable:
- **Content-addressed immutable blocks ⟹ caches never need invalidation.** AI
  data is write-once-read-many (datasets, checkpoints, weights). The only mutable
  surface in the whole system is `dentry → (inode, generation) + latest pointer`.
  Everything below it is immutable and infinitely cacheable.
- **Metadata off the data path ⟹ bandwidth is a cache+hardware problem, not a
  metadata problem.** After one resolution, bytes flow client↔object/NVMe with
  the metadata engine touching zero bytes.

This is why NoKV's bet is metadata + correctness + cost, not bandwidth: the data
path is increasingly commoditized (RDMA/NVMe/object), while **metadata at AI
scale is a distributed-systems problem you can actually win by design.**

---

## 3. Metadata Plane

**Truth model.** Inode and dentry records, with **child attrs denormalized into
the dentry** so `readdir` returns attrs in a single range scan and never does a
second inode read. This is the *same* small-file metadata win that 3FS
(`DENT`+parent range scan) and JuiceFS (`A{inode}D{name}`) rely on — NoKV already
has it (`DentryProjection` carries the full child `InodeAttr` + optional
`BodyDescriptor`).

**Dual key-clustering** (3FS's load-bearing schema trick, adopt for sharding):
- **Dentries clustered by parent** (`mount||parent||name`) → one-scan readdir,
  in-directory rename is a local key rewrite. *Already done.*
- **Inode-id keys scattered** (little-endian / hashed) → kills the monotonic-ID
  write hotspot when metadata shards across the cluster. *To do in the Holt key
  encoding before sharding.* Opposite clustering on purpose: scatter inodes for
  write balance, cluster dentries for list locality.

**Read-mode open creates zero metadata state.** Thousands of training ranks
opening the same dataset shard for read must not each persist fd/session state
(3FS persists fd only for write-mode opens). Critical for parallel-job bootstrap.

**Consistency choice (decision record).** The SOTA splits into *delegate-to-
transactional-KV* (3FS→FoundationDB, JuiceFS→TiKV: cross-shard atomicity for free,
but a heavy external dependency + a single-cluster ceiling + ~10MB/~5s txn limits)
vs *owner-lease* (per-shard single-writer, cheap consistency, cross-shard ops
restricted). **NoKV chooses owner-lease** — it keeps the embedded-Holt ownability
advantage and matches the cloud path in §10. Holt stays behind a narrow trait
(the JuiceFS pluggable-seam lesson: the embedded engine is a feature *if the seam
stays narrow*).

---

## 4. Layout Lease & the Read-Path Contract

The mechanism that makes "metadata off the data path" real and provable.

The target native `open()` returns an **immutable layout lease** for the file's
generation:

```text
open(path) -> {
  inode, generation, manifest_version,
  layout: [ chunk -> [ block -> { object_key, offset, len, digest } ] ],
  lease_epoch, ttl
}
```

The client then computes `block → object range` itself and reads object ranges
directly; **metad is provably off the byte-transfer path** (this is the 3FS
layout-on-open contract). The current implementation exposes that boundary as
`OpenPathReadPlan`: one RPC resolves the file, returns a
generation-scoped read lease, and returns immutable block read plans carrying
`digest_uri`, all from one metadata snapshot. `ReadBodyPlan` remains for
generation-scoped follow-up reads and prefetch. Freshness comes from the watch
log + a short TTL; a divergent write mints a new generation (new keys), so a
stale lease can never read wrong bytes — it just misses the newest write.

---

## 5. Data Path & Tiering

**Capacity tier:** content-addressed immutable object blocks (the durable truth).
**Bandwidth tier:** local NVMe hot tier + S3-compatible object storage.

The metadata manifest records only durable, portable block identity:
`generation`, logical offsets, block digest, and S3-compatible object key. It
does **not** record local NVMe paths or cache slots. Those are soft placement
decisions made by the data path.

```text
layout lease
  -> block descriptors
  -> data fabric transport choice:
       ObjectTcpGet        S3-compatible cold read
       LocalNvmeRead       node-local hot-tier read
```

NoKV's existing pieces map directly: `BlockCache{off|memory|disk}` +
`WritebackCache` + `ObjectPrefetcher` + `ObjectReadPlanCache` +
`ObjectStore::get_many`. The first data fabric target is a tiered NVMe/S3 path:
check node-local NVMe, fall back to S3, batch coalesced read-plan ranges through
the object layer, and refresh hot blocks asynchronously. The
`resolve_block_placements` helper is the first placement resolver: it marks
local hot hits as `LocalNvmeRead` and cold fallbacks as `ObjectTcpGet`. The
`LayoutReadExecutor` is the SDK data-plane entry: it consumes the layout-open
plan, keeps readahead/cache behavior in the read pipeline, and records
`local_nvme_hits`, `object_fallbacks`, and `coalesced_ranges`.

The native performance path is an SDK `GetBatch`/layout-lease path, not FUSE:
batch read plans, coalesced object ranges, local hot-tier hits, and background
hot refresh.

---

## 6. Cache & Prefetch — Per Access Pattern

The common mistake is one cache policy for everything. There are **four distinct
patterns**, each wanting a different design:

| pattern | unit | cache | prefetch |
|---|---|---|---|
| **dataset-shard train read** (shuffled, reused/epoch) | ~1 GB tar/WebDataset **shard**, not a file | warm-NVMe write-back + page-cache for hot shards; **preload before epoch-1** | two-level shuffle (shuffle shard *names* globally + client shuffle buffer); tunable readahead; maximize **hit rate** |
| **checkpoint** | per-rank tensor-shard blob | compute-side NVMe write-back **absorb** | async object persist (off critical path); parallel range prefetch on restart |
| **weight cold-load** | 100s of GB of weights | — | extreme-parallel concurrent cold-read saturating NICs/SSDs into HBM |
| **KV-cache** | fixed token block (16/256) | tiered HBM→DRAM→NVMe→object | **prefix-aware** eviction (evict suffix before parent); water-level(soft)+quota(hard) admission |

Sharding billions of small samples into sequentially-readable shards converts
billions of random tiny reads into a few large sequential reads (DataComp:
sharding alone +11.2% throughput). The `GetBatch` primitive — "fetch this ordered
set of N items as one streamed TAR," assembled across owners by a designated-
target coordinator — is **15× faster than individual GETs at 10 KiB**. Re-target
NoKV's `ObjectPrefetcher` to **prefix/shard locality**; make cache a per-mode
choice, off for the one-shot scan.

---

## 7. Checkpoint Engine — the HERO chapter

This is NoKV's differentiator: it already has the **atomic-commit + CoW-version-
pin** primitives that nobody else combines, and they are *exactly* the contract
ByteCheckpoint (NSDI'25) and PyTorch DCP converged on. Five properties, all of
which map onto existing NoKV primitives:

1. **Data/metadata disaggregated, parallelism-agnostic layout.** Storage files =
   concatenated raw per-rank tensor-shard bytes (immutable). One global index =
   `TensorMeta(dtype/shape)` + `ShardMeta(FQN, nD_offsets, nD_lengths)` +
   `ByteMeta(byte start+len of each shard)`, keyed to **logical** tensor shards,
   decoupled from TP/PP/DP/FSDP and the framework. → Add this descriptor to
   NoKV's chunk manifest (it already denormalizes body descriptors).
2. **Async pipelined save** exploiting the immutability window (model+optimizer
   state is read-only during fwd+bwd): D2H copy into **ping-pong pinned host
   buffers** → CPU serialize → object upload, fully overlapped; training resumes
   after the fast D2H copy, not the slow persist.
3. **Atomic publish** = object-first → single metadata-command commit (tmp →
   atomic visibility behind a barrier). **NoKV has this natively** (`PublishArtifact`
   / the object-first-then-metadata-atomic ordering); JuiceFS/Alluxio bolt it on.
4. **Reshard-on-read.** Load is a metadata **range-intersection** over the
   `ByteMeta` index — read exactly the byte ranges the new parallelism needs.
5. **CoW version pin** (leased snapshot pins) = a parallelism-agnostic checkpoint
   *version*, plus cheap dataset/checkpoint branching. SSD→object cool-down is a
   **pure-metadata pointer remap**, never a critical-path copy; replica/DP-aware
   dedup avoids writing the same shard N times.

**Benchmark plan:** MLPerf Storage v2.0 (8B/70B/405B/1T, worst-process scoring) +
the average-wasted-time equation `t_ckpt + 1/(2f) + t_retrieve` (GEMINI) — the
language the target teams use.

---

## 8. Dataset Serving & the Training-Read Hot Path

The honest boundary: **on the training-read hot path, rich metadata is dead
weight** — training deliberately bypasses POSIX (WebDataset/Mosaic/Ray/AIStore
exist *because* sequential shard streaming beats random FS reads 3–10×). So:
- The **shard** (tar/WebDataset, range-indexed) is the unit, not the file.
- Add an `ishard`/`dSort`-equivalent: pack/repack small files into ~1 GB shards +
  a distributed global shuffle.
- `GetBatch` (designated-target streaming, continue-on-error) for the batch read.
- NoKV's metadata richness (dentries, attrs, xattrs) is reserved for the
  **FS / checkpoint / artifact** path, where it pays — not forced onto the
  dataloader.

---

## 9. KV-Cache / Serving Backend

Be the **storage backend** Mooncake/Tair/LMCache/InfiniStore target, not the
engine. The economic framing: re-reading cached KV from DRAM/NVMe/RDMA is cheaper
than recomputing attention, so **hit rate (not IOPS) is the metric.**

- **A second, content/prefix-hash-keyed namespace** alongside the POSIX dentry
  tree, reusing NoKV's immutable-block + atomic-publish machinery. Key =
  prefix-chain hash `hash(parent_block_hash, block_token_ids, tenant_salt)`
  (non-crypto **xxhash**, not sha256 — vLLM moved off crypto on this hot path).
  Parent-hash chaining gives prefix-tree dedup *without* a tree (blocks alloc/free
  like OS pages). **Prefix-scoped identity:** same tokens under a different prefix
  are a different block.
- Fixed token block (16 vLLM / 256 LMCache / configurable). Tiered
  HBM→DRAM→NVMe→object with prefix-aware eviction + water-level/quota admission.
- **Metadata strictly off the transfer path**; index is a flat global hash table
  (vLLM) or a light central manager (Mooncake Conductor / Tair manager) — **not**
  consensus-replicated.
- Implement the existing connector contracts (LMCache / Mooncake / NIXL /
  vLLM-v1 / Dynamo) — don't invent a new one.

---

## 10. Cloud-Scale Sharding & Control Plane

The metadata plane goes distributed **without** consensus-replicating metadata —
matching the HDFS-RBF State-Store + FoundationDB-pattern hybrid:

- **A tiny etcd-class control plane holding ONLY the shard map + owner-leases**
  (like RBF's State Store). Stateless metadata workers hold no durable truth.
- **Partition unit = subtree** owned by one shard (contiguous inode-id range,
  dentry co-located) → `readdir` + in-directory rename are single-shard atomic.
- **Lease-epoch fencing** at the `MetadataCommand` commit boundary (extend the
  existing fence) — a partitioned old owner cannot keep writing.
- **Cross-shard rename = `EXDEV` in v1**, later a 2-phase subtree handoff with
  pending-intent recovery — **not** a multi-key distributed txn, **not** a
  CephFS-style runtime dynamic subtree migration (operationally fragile).
- Reconcile with the existing single-node durability: Holt WAL is the local
  single-owner durability boundary; a future replicated command/WAL tail belongs
  to the shard owner layer, not to Holt's public API and not to the scale-out
  control plane.
- **Durable two-level allocator** (inode-id ranges + epoch; per-shard monotonic
  commit-version as a Holt counter) — recover by reading the counter, never by
  scanning. (NoKV already recovers the allocator from a System record.)

**Avoid:** hash-by-parent-inode/by-id sharding (balances load but destroys readdir
locality and shatters huge AI dataset dirs — the CephFS lesson); use
striped-directory / ephemeral pinning **only** for the pathological single-huge-
directory case, capped.

---

## 11. Durability, DR & Consistency

Already built (this is real, defensible reliability engineering):
- **Object-first, pointer-second** publish ordering — a crash leaves GC-able
  orphan objects, never a dangling pointer.
- **Metadata DR**: periodic Holt checkpoint → object store under `CURRENT` (atomic
  pointer swap; retained window tracked in the manifest because `ObjectStore` has
  no `list`); a tested restore path onto a fresh node.
- **fsck**: the read-side complement — walk live files at current generation,
  `head` every referenced block, report drift the ordering cannot prevent
  post-commit.
- **GC** driven by the metadata `gc_queue` + epoch + leased snapshot pins, **never
  by listing the object store**; `owns_block_object_key` keeps fork/clone GC-safe.
- **External object-store eventual consistency**: rely on read-after-write
  (strong on S3/GCS since 2020); `head`-before-`get` for existence.

---

## 12. Benchmarks & Evidence Plan

Credibility = numbers in the teams' own language, **not a bandwidth leaderboard**:
- **MLPerf Storage v2.0** (8B/70B/405B/1T checkpoint, worst-process scoring).
- **Holt-vs-SQLite/RocksDB metadata throughput** — validates the "embedded engine,
  off the data path" thesis.
- **Checkpoint stall / save / reshard vs PyTorch DCP baseline**; the
  average-wasted-time equation.
- **cache-hit → GPU-util curve** (the linear law).
- **KV-cache hit-rate + TTFT reduction**.

---

## 13. Roadmap & Sequencing

Critical path (metadata correctness across failover; a native data path beating
`s3fs`/`goofys`) before decoration (CSI/operator/Python). Ordered:

1. **Durable two-level allocator** (inode ranges + epoch; per-shard counter).
2. **Metadata throughput benchmark** (Holt vs SQLite/RocksDB) — prove the thesis.
3. **Native layout-open**: already exposed over metadata RPC; next work is making
   the SDK/FUSE read paths consume it through a JuiceFS-style pipelined data path.
4. **Checkpoint shard-index + async pipelined save** (the HERO) — `(FQN, offsets,
   lengths)` + `ByteMeta`, batch/scatter write, reshard-on-read. *MLPerf bench.*
5. **Tiered cache/prefetch maturation** per access pattern (§6).
6. **Subtree sharding + lease control plane** (§10).
7. **KV-cache / serving backend** (§9) — the Alibaba/ByteDance-inference wedge.
8. **Native read client** with chunk/slice planning, bounded buffers,
   coalesced object reads, and local hot-tier refresh.
9. **Minimal CSI** for per-pod workspace mounts.

**Decide now:** tenancy (today `MountId`/object-key/`RecordFamily` have no tenant
binding — v1 vs v3?).

---

## 14. Per-Target Positioning

- **Alibaba KVCache / checkpoint storage.** Lead with the two primitives NoKV
  uniquely already has and Tair/EasyCkpt validate: (1) **atomic artifact publish +
  CoW version pin** = the EasyCkpt/CPFS+3FS async-D2H-then-atomic-commit,
  parallelism-agnostic checkpoint contract — *"the durable, atomic, reshard-on-read
  checkpoint store"*, benchmarked on MLPerf Storage v2.0 + GEMINI wasted-time.
  (2) Map immutable-block + atomic-publish + leased-pin onto Tair's
  `KVCacheManager` pattern: be the **persistent NVMe/object KV-cache tier** behind
  their manager — content/prefix-hash keyed, prefix-aware eviction, and a
  practical TCP/NVMe/object path. *Honest line: "I build the correct,
  cost-efficient storage substrate your KVCacheManager/EasyCkpt plug into; I'm not
  re-implementing the engine or chasing 3FS bandwidth."*
- **ByteDance training/inference infra.** They wrote ByteCheckpoint (NSDI'25) —
  speak its exact language (parallelism-agnostic `(FQN, offsets, lengths)` layout,
  async pipelined save, reshard-on-read) and show NoKV has the atomic-publish +
  CoW-pin primitives natively.
- **Doris / SelectDB 存算分离.** The cleanest, most honest fit — NoKV's body *is*
  metadata + object-storage cache + compaction-adjacent tiering. Lead with the
  metadata engine + tiered cache + the cache-hit→throughput law; the FS framing is
  secondary.

---

### How this project got here (the honest evolution narrative)

NoKV started as a distributed KV / storage engine (with a Raft control plane and
TLA+-modeled fencing/failover), then **pivoted to a single-node, object-storage-
backed AI file system** — metadata engine + immutable blocks + atomic checkpoint
publish + CoW lifecycle. The cloud-scale path forward is **sharding + owner-lease**
(a tiny control plane, not consensus-replicated metadata), as designed in §10. The
range — distributed consensus, a storage engine, and a filesystem — is the point.
