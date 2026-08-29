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
| Product | Primary native CLI, secondary Python SDK, Rust SDK, or transport-free Workbench facade | User-visible latency, results, errors, retries, and end-to-end throughput |

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

## Executable Metadata Read Workload

`nokv-bench metadata` exercises the production protocol DTO executor over one
`MetaShard` backed by a real `HoltStore`. It creates one root, one owner, one
visible workbench, and a deterministic path tree. Untimed setup uses bounded
metadata commands to install each zero-length `PathCurrent` artifact together
with its available `ArtifactRevision` and strong path `RevisionRef`; it does
not pretend to measure publication or object upload. The timed interval then
measures:

- existing and missing exact reads at shallow and deep paths;
- a small first page from a recursive prefix;
- first, middle, and final cursor pages from a non-recursive prefix.

Every direct child in the non-recursive workload is both an exact artifact and
the parent of a deep subtree. This keeps old and new logical results equivalent
while exposing whether listing can skip the subtree.

Use a new, nonexistent metadata directory for every run:

```bash
cargo run --release --locked -p nokv-bench --features metadata-read-stats \
  --bin nokv-bench -- \
  metadata \
  --metadata-dir /absolute/path/to/new-metadata-dir \
  --iterations 1000 \
  --warmup 100 \
  --direct-children 96 \
  --leaves-per-child 64 \
  --page-limit 32 \
  --seed 42 \
  --revision <commit-or-patch-label> \
  --harness-revision <benchmark-tree-digest> \
  --dirty-worktree
```

Omit `--metadata-dir` for an in-memory diagnostic run. The v3 JSON report records
the dataset, source and harness labels, dirty state, durability, warmup,
latency distribution, throughput, result checksum, pre/post correctness
assertions, a normalized logical-result digest, per-workload metadata read
amplification, and the qualification boundary.
Set `NOKV_BENCH_MACHINE` and `NOKV_BENCH_METADATA_DEVICE` to reviewed machine
and physical-device labels when retaining a file-backed comparison; missing
device information is reported as `unknown` rather than inferred from the store
directory.

This runner measures metadata-domain behavior through
`MetadataWorkspaceRequestExecutor`; it does not frame bytes, open a network
connection, access object storage, invoke the SDK, or invoke the OpenViking
facade. Every workload completes warmup before it starts two thread-bound
sessions. One session collects logical metadata counters. The other session
collects Holt diagnostics. The runner times only the requested iterations and
stops the timer before it finishes either session. Setup, warmup, correctness
checks, and session setup and finish do not affect the reported latency.

The timed path updates the logical counters and the Holt adapter's cursor
counters. Holt database snapshots occur outside the timer. The non-default
`nokv-meta/metadata-read-stats` and `nokv-meta-holt/read-stats` features remove
these hooks from ordinary production builds. The benchmark enables both
features through `nokv-bench/metadata-read-stats`. A normal
`cargo build --workspace` does not enable this instrumentation through Cargo
feature unification.

With `--warmup > 0`, each row is labelled `same_request_warmup`: the exact same
request runs before that row, but this is not a claim that the operating-system
page cache was controlled. With `--warmup 0`, the runner reports
`cache_state = uncontrolled`; it never labels that profile cold.

The report separates three different quantities:

- thread-local NoKV logical point reads, split into system/fence reads and
  authoritative `WorkspaceCurrent`, `PathCurrent`, and other metadata-family
  reads;
- cursor-local Holt scan work (`visited` work units, returned keys, common
  prefixes, and restarts) plus emitted key/value bytes;
- store-wide Holt cache, full-blob, page, and read-index counter deltas.

`visited` is a Holt cursor work unit, not a physical row or device read.
Emitted value bytes are materialized bytes, not device bytes or a claim about
decoder CPU cost. Holt 0.8.5 does not expose an exact internal seek count, so
the report leaves that metric unavailable rather than inferring it from scan
calls. Logical counters exclude reads performed concurrently on other threads
and by other stores. Their coverage is the fenced query paths used by these
read workloads, not write-transaction or recovery-internal reads. The runner
emits no report after a failed operation, because cursor work on a failing range
may be incomplete. Store-wide physical deltas are exactly attributable only for
this runner's dedicated store and `concurrency = 1` profile; background Holt
work may still contribute and must not be described as request-local device
I/O. Accordingly, each workload labels its scope as successful logical
operations plus the surrounding physical-counter time interval.

For an old/new comparison, build both revisions in release mode with separate
target directories and create separate metadata stores from the same seed. If
the baseline predates this binary or its read-stat instrumentation, export
declared baseline and candidate instrumentation patches that implement the same
named counter semantics without changing either product read algorithm. Keep
the runner and its implementation-invariant tests byte-identical. Retain both
patches, record their SHA-256 values, record a deterministic manifest of the
shared runner files as `--harness-revision`, and review the implementation-
specific hooks before comparing results. `dirty_worktree` describes the
product source under test after excluding the declared harness and
instrumentation patches.
Never open one revision's store with another schema. Compare only matching
profile fields and normalized semantic digests, and retain each raw JSON
report. The runner aborts without emitting a report on the first operation or
correctness failure.

This workload is explicitly a diagnostic and reports Workspace Acceptance Gate
8 as `NOT QUALIFIED`: it omits the product boundary, cold-cache and concurrent
matrices, host utilization, exact Holt seek accounting, and failure/recovery
matrices required for a release performance record.

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
