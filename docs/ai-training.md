<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Native Batch And Range Reads

NoKV exposes batch and range-read primitives for workloads that keep immutable
file bodies in object storage but need filesystem-shaped metadata. Agent
workspaces and RAGFS integrations are the primary product context; scientific
pipelines, packed indexes, model artifacts, datasets, and checkpoints can use
the same data path.

The feature is not specific to AI training, despite historical benchmark and
script names that still use `training-read` or `ai-shard`.

## Access Paths

```text
agent runtime / RAGFS / data process
  -> FUSE, Rust SDK, or Python/fsspec binding
  -> NoKV metadata service
  -> shard-local Holt metadata
  -> S3-compatible object provider
```

FUSE is the compatibility path for programs that require mounted files. It maps
inode operations to metadata requests and object range reads, and publishes
buffered writes on close. Read-only snapshot mounts expose a pinned subtree and
reject mutation.

Native clients bypass kernel/FUSE crossings when an integration can call NoKV
directly:

```text
compatibility path
  existing program -> FUSE -> metadata + object data

native path
  agent/RAGFS/reader -> Rust or Python client -> metadata + object data
```

The Rust SDK, CLI, and FUSE frontend can use the experimental control-plane
fleet router. The current Python binding accepts one `metadata_addr`; it does
not yet construct a multi-shard fleet client from etcd.

In fleet mode, a high-level Rust range batch may contain paths from different
shards. The client groups its metadata opens into shard-local batch RPCs and
re-scatters the results into caller order. That is parallel request routing, not
a cross-shard transaction or snapshot.

## Rust Batch Primitives

The Rust file client exposes:

- `read_path_ranges_batch` for multiple paths with multiple logical ranges;
- `read_path_ranges_batch_packed` for one packed result per path request;
- `read_path_ranges_batch_into` for caller-provided staging memory;
- `prepare_path_ranges_batch` plus
  `read_prepared_path_ranges_batch_into` when request geometry is reused.

Each execution batch-opens the metadata read plans, then resolves the immutable
generation's manifests into object ranges. Compatible nearby ranges can be
coalesced to reduce object requests, while the returned logical windows preserve
the caller's requested order.

Prepared batches cache request geometry and output layout, not namespace truth.
They still open metadata plans when a read executes, so current visibility and
generation checks are not silently bypassed.

## Python Batch Primitives

The Python binding exposes the same native pipeline rather than rebuilding
range planning over POSIX calls:

- `read_ranges_batch` returns individual requested byte ranges;
- `read_ranges_batch_packed` returns packed bytes per path request;
- `read_ranges_batch_into` fills a caller-provided `bytearray`;
- `prepare_range_batch` reuses normalized request and output geometry;
- `prepare_range_batch_reader` combines a prepared plan with reusable NoKV-owned
  staging memory;
- `prepare_range_batch_epoch` cycles through multiple prepared readers and can
  execute them through a bounded persistent worker pool;
- `read_ranges_batch_buffer` fills a reusable `ReadBuffer`.

The blocking Rust read is released from the Python GIL where the binding can do
so safely. A caller-owned `bytearray` remains under the GIL while its raw storage
is mutated.

`ReadBuffer` supports `memory_kind="system"` and, on Unix,
`memory_kind="page_locked"`. Page-locked mode uses host `mlock` for resident CPU
staging pages. It is not CUDA pinned allocation, RDMA registration, HBM storage,
or a zero-copy path to an accelerator.

A `ReadBufferView` token prevents resize/refill while an exported logical view
is live. With the current `abi3-py39` package boundary this is not a general PEP
3118 `memoryview`; callers should treat it as NoKV-owned staging memory.

## Consistency Boundary

Opening a file produces a read plan for one immutable `(inode, generation)`.
Range reads validate that generation against current metadata and fail rather
than silently reading a different body after replacement.

Important limits:

- each underlying batch RPC routes to one metadata shard, even when the Rust
  fleet client groups a high-level request across several shards;
- separate unpinned opens do not form a cross-path or cross-shard transaction;
- use a snapshot pin for a stable historical subtree view;
- a prepared range layout does not pin a generation by itself;
- object-provider durability and availability remain provider responsibilities.

## Current Cache Layers

The client and object pipeline currently provide library-local acceleration,
including object block caches, read-plan/read-window reuse, range coalescing, and
prefetch. Memory and disk-backed block-cache policies are available in the
object layer, and the FUSE path exposes related cache metrics.

Cache placement is not stored in metadata and is never authoritative. A cache
hit must still correspond to the object key/range selected by the validated
metadata plan.

A separate node-local cache daemon that coordinates all agents or processes on
a machine is a possible future deployment component. It is not part of the
current implementation and should not be described as a shipped NoKV service.

## Workload Examples

- an agent runtime batch-reading workspace artifacts after metadata retrieval;
- an OpenViking/RAGFS storage integration reading many small immutable files or
  ranges from packed resources;
- a scientific agent reading selected ranges from large observation products;
- a checkpoint or artifact consumer resuming with a different read
  parallelism;
- an ML data loader reusing prepared packed-range geometry across iterations.

These are applications of the same storage primitives, not separate consistency
models.

## Performance Qualification

Batching reduces per-request overhead only when the workload, shard placement,
range geometry, and object provider permit it. Coalescing can also read extra
bytes, and hot prefixes can still saturate one single-owner shard.

Before making an enterprise-throughput claim, measure:

- metadata batch-open latency and operations per second per shard;
- object request count, useful bytes, and coalescing amplification;
- cold and warm cache behavior;
- packed versus small-object layouts;
- scaling across independent shard owners;
- p95/p99 latency under concurrent publication, snapshot, and GC activity.

The repository's current single-node and local matrix workloads are development
evidence. They are not a published enterprise small-file or multi-machine fleet
qualification.
