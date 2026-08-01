<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Benchmarks

Status: workload and evidence contract for the supported NoKV workspace.

NoKV performance claims must exercise the same full-path metadata model,
revision-owned object layout, root routing, and durability profile used by the
product. An internal Holt or codec measurement is useful diagnostic evidence,
but it is not a Workbench, SDK, recovery, or failover result.

The normative qualification gates are in
[Workspace Acceptance](./development/workspace-acceptance.md).

## Evidence Levels

| Level | Boundary | What it can establish |
| --- | --- | --- |
| Key/codec | Canonical key and durable record functions | Encoding cost, size, ordering, and allocation behavior |
| Holt engine | Named trees, point/range reads, atomic batches | Engine throughput, conflicts, WAL cost, checkpoint behavior |
| Metadata domain | Workspace marker, paths, commands, operations | Namespace amplification, visibility, replay, lifecycle cost |
| Service | Protocol through shard owner and object provider | Routing, serialization, fencing, provider latency, recovery |
| Product | SDK, CLI, MCP, or Workbench facade | User-visible latency, results, errors, retries, and end-to-end throughput |

A report names its level. Results from different levels are not interchangeable.

## Required Profile

Every row records:

```text
commit
dirty_worktree
rust_toolchain
machine
operating_system
metadata_device
object_provider
object_endpoint_class
durability_profile
logical_shards
physical_owners
roots
workspace_count
paths_per_workspace
payload_distribution
concurrency
duration
seed
cache_state
```

The two durability profiles remain separate:

- `local_wal`: acknowledge after the configured shard-local Holt WAL boundary;
- `shared_log`: acknowledge after the configured shared logical-log boundary.

Do not merge or average rows across those profiles.

## Core Metadata Workloads

### Exact read

Resolve a visible `WorkspaceCurrent` marker and point-read one canonical
`PathCurrent` entry. Report marker-cache policy separately from path-read
latency.

Matrix:

- existing and missing paths;
- cold and warm metadata;
- short, deep, ASCII, and multibyte paths;
- one root and many roots;
- concurrency 1 through saturation.

### Ordered list

Resolve one visible marker and scan one component-safe prefix at a fixed read
version.

Matrix:

- empty, partial, full, and maximum qualified pages;
- shallow and deep parents;
- `a`, `ab`, and `a/child` boundary cases;
- first, middle, and final cursor pages;
- sparse deleted history at the selected read version.

### Conditional publication

Upload and verify immutable blocks, then execute one bounded metadata command.

Matrix:

- create-only success and exists conflict;
- replace-only success, missing path, and stale generation;
- append head CAS;
- byte-identical edit;
- exact request replay;
- request-id mismatch;
- response loss before and after the metadata acknowledgment;
- object upload and verification failure.

Report object time and metadata time separately, plus the complete user-visible
latency.

### Query

Search and aggregate run at one read version over declared index projections.

Matrix:

- predicate selectivity;
- projection width;
- sort and group cardinality;
- one workspace and root-wide scope;
- live and snapshot reads;
- visible and hidden incarnations.

## Lifecycle Workloads

Required workloads include:

- snapshot mint, frozen read, renew, retire, and reap;
- commit construction across member and unique-revision distributions;
- commit-head/tag replacement and commit retirement;
- restore across entry-count and shared-revision distributions;
- publication abort and staged-object cleanup;
- revision GC under create/replace/remove churn;
- ambiguous provider deletion and quarantine reconciliation.

Lifecycle reports retain metadata rows scanned or written, object bytes copied,
object bytes reused, retries, cursor pages, and recovery work.

## Distribution And Recovery

Required scenarios:

- many roots distributed across logical shards;
- one hot root assigned to a dedicated physical owner;
- owner epoch replacement during reads and writes;
- checkpoint creation while commands continue;
- process loss before and after acknowledgment;
- checkpoint plus command-log replay;
- stale-owner write and delete rejection.

Root placement never varies by filename. A benchmark that hashes paths across
shards measures a different system and cannot be compared with NoKV.

## Metrics

Every workload retains:

- attempted, successful, conflicted, retried, and failed operations;
- achieved operations or bytes per second;
- p50, p95, p99, and maximum latency;
- metadata point reads, scans, predicates, mutations, history writes, event
  writes, index writes, and dedupe writes;
- object requests and transferred bytes;
- CPU, memory, device I/O, and network utilization;
- recovery duration and remaining background work, when applicable.

An average without the distribution and error counts is insufficient.

## Comparison Rules

Compare only rows with matching workload semantics, payload distribution,
concurrency, durability, cache state, topology, object provider, and machine
class. When any dimension changes, report a separate workload instead of a
single percentage.

External systems may have different namespace or acknowledgment semantics.
State those differences next to the result and avoid presenting unlike
operations as equivalents.

## Qualification

A benchmark result is qualified only when:

1. correctness assertions pass before and after the timed interval;
2. the exact command and raw output are retained;
3. skipped or ignored environment checks are reported as `NOT QUALIFIED`;
4. no benchmark-only product behavior changes the measured path;
5. the report links the corresponding
   [Workspace Acceptance](./development/workspace-acceptance.md) gate.

The repository intentionally publishes no headline number without a complete
qualified record.
