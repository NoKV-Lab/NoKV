<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# AI Training

Status: optional Agent workload profile, not a separate product
architecture or namespace.

NoKV serves training data, checkpoints, experiment outputs, and provenance
through the native full CLI first, the direct Python SDK for embedded jobs,
lower-level Rust SDK calls where needed, and explicit local adapters. Canonical
identity remains a root, Workbench, and normalized path; a materialized local
path is disposable.

## Workloads

- immutable dataset shards and manifests;
- repeatable script and configuration inputs;
- ranged and batched sample reads;
- checkpoint publication and resume;
- logs, metrics, reports, and model artifacts;
- comparison and lineage across runs.

## Access Paths

```text
training process
  -> native CLI skill, Python SDK, or lower-level Rust SDK
  -> root router and fenced shard owner
  -> full-path Holt metadata
  -> immutable S3-compatible object blocks
```

Native jobs use point reads and ranged object reads. Programs that require
local files use:

```text
materialize verified inputs
  -> execute inside a disposable sandbox
  -> collect declared outputs
  -> publish immutable revisions
  -> commit run metadata and lineage
```

The adapter never turns the sandbox into namespace truth. Changes become
visible only after explicit collection and metadata publication.

## Dataset Publication

A dataset publisher:

1. creates or selects a Workbench;
2. uploads immutable blocks for each artifact revision;
3. verifies size and digest evidence;
4. atomically publishes paths, references, indexes, and events;
5. seals an immutable commit and optional durable tag.

Training jobs consume the commit or tag rather than a mutable collection of
object keys. This preserves exact revision membership and allows multiple runs
to share object bodies safely.

## Batched Read Qualification Target

A qualified batched reader should:

1. resolve all requested paths at one declared live or snapshot context;
2. obtain immutable revision and range plans;
3. coalesce compatible physical reads without changing semantic ranges;
4. check the local soft cache;
5. read and verify the remaining object blocks;
6. return results in request order with per-item errors.

Prepared range layouts may be reused across steps, but metadata visibility and
generation checks still run for each live read. Snapshot reads retain their
fixed read version. The current source tree does not qualify metadata batch-open
or cross-artifact read coalescing performance; those require an executable
product path and workload-matched evidence.

## Checkpoints

Checkpoint writers publish immutable model, optimizer, scheduler, and run-state
artifacts under normalized paths, then seal a commit only after every required
artifact is visible and verified.

Resume uses:

- a leased snapshot for short operational recovery;
- an immutable commit or durable tag for long-lived reuse.

The run manifest records model/framework versions, source dataset commit,
training configuration, producer identity, and content digests. It does not
embed physical owner addresses or provider credentials.

## Cache

A node-local cache is soft state keyed by immutable revision and block
identity. It may prefetch dataset shards or checkpoint ranges, but loss of the
cache cannot affect correctness or reachability.

Any future metadata cache must be bounded by read version, generation, and
typed change events. A Workbench marker is the only safely cacheable part of
the exact namespace lookup; canonical path entries remain authoritative at the
selected read context.

## Qualification

Training claims include:

- dataset and sample-size distributions;
- range shape and batch size;
- cold/warm cache state;
- metadata and object latency separately;
- throughput and p50/p95/p99/maximum latency;
- retries, conflicts, integrity failures, and per-item errors;
- worker count, host memory, local cache device, network, and object provider;
- exact NoKV commit and durability profile.

See [Benchmarks](./benchmarks.md) and
[Workspace Acceptance](./development/workspace-acceptance.md).
