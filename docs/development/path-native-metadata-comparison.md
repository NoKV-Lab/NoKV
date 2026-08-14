<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Path-Native Metadata Comparison

Status: code-structure comparison and optimization acceptance boundary.

This document compares the pre-cutover inode/dentry implementation at commit
`98cac201affee7ca1a654fea39373108b81d31ef` with the path-native workspace
model under the same Agent operations. It does not compare POSIX behavior with
Workbench behavior and does not treat a code-shape reduction as measured
latency or throughput.

The stable 18-tool Workbench facade is unchanged. The comparison stops at the
metadata boundary and excludes object-body I/O, transport, WAL device cost,
and recovery-log chunking unless stated otherwise.

## Evidence Classes

- **Code fact** means the named record, key, branch, or synchronization point
  is directly present in the referenced implementation.
- **Complexity derivation** describes asymptotic metadata work implied by that
  code. It is not a benchmark result.
- **Measured result** requires matched workload semantics, payload,
  concurrency, durability, cache state, topology, provider, and machine under
  the [benchmark contract](../benchmarks.md).

The old source can be audited without restoring it into the product tree:

```bash
git show 98cac201affee7ca1a654fea39373108b81d31ef:crates/nokv-meta/src/service/read.rs
git show 98cac201affee7ca1a654fea39373108b81d31ef:crates/nokv-meta/src/service/namespace.rs
git show 98cac201affee7ca1a654fea39373108b81d31ef:crates/nokv-meta/src/holtstore/families.rs
git show 98cac201affee7ca1a654fea39373108b81d31ef:crates/nokv-control/src/types.rs
```

The path-native sources are
[`publication_records.rs`](../../crates/nokv-meta/src/workspace/publication_records.rs),
[`namespace.rs`](../../crates/nokv-meta/src/workspace/namespace.rs),
[`engine.rs`](../../crates/nokv-meta/src/workspace/engine.rs), and
[`executor.rs`](../../crates/nokv-server/src/executor.rs).

## Same-Semantics Operation Shape

Let `d` be path depth, `p` the returned page size, `s` the target-prefix rows
advanced before the page is full, `b` the revision's manifest-block rows, and
`i` its secondary-index rows.

| Agent operation | Pre-cutover inode/dentry | Path-native workspace | Classification |
| --- | --- | --- | --- |
| Get metadata for one artifact path | Cold lookup walks parent components and then reads the final dentry. The optional full-path index still validates the parent chain and canonical dentry on an uncached hit. | Resolve one visible `WorkspaceCurrent`, then point-read one canonical `PathCurrent`. `PathEntry` contains the complete immutable metadata projection returned by this operation, so it does not fan out to `ArtifactRevision`. | Code fact |
| Complexity of exact get | `O(d)` cold namespace reads; caches can reduce work but do not change the uncached contract. | `O(1)`: exactly one workspace-marker point read plus one path point read. Only the marker is eligible for a validity-bounded cache. | Complexity derivation |
| List direct children | Resolve the parent inode, then scan the parent-keyed dentry range with cursor and limit. This gives natural direct-child selectivity after parent resolution. | Resolve one marker, seek to the encoded component-safe descendant prefix, and stream from the exclusive cursor. Stop after the bounded result plus lookahead; only after descendant EOF, optionally point-read the exact requested path. Semantic filtering may make `s` greater than `p`, but the scan no longer starts at the workspace root. | Code fact and complexity derivation |
| List a recursive subtree | Walk discovered directories and issue parent-dentry scans for their children. | Ordered full-path prefix scans advance one exclusive marker across bounded engine pages and stop when the protocol page is full. Live work is `O(s)`, not `O(all workspace paths)`. | Complexity derivation |
| Publish or replace an artifact | Maintain mutable inode and dentry projections, optional path index rows, body/chunk descriptors, and filesystem-owned attributes. Parent-path resolution can add depth-dependent reads. | One bounded `MetadataCommand` atomically maintains `PathCurrent`, `WorkspaceCurrent`, immutable revision/manifest rows, strong references, indexes, event, history, recovery, and dedupe state. Namespace work is independent of `d`; total mutation work is operation-specific and approximately `O(b + i + references)`. | Code fact and complexity derivation |
| Remove an artifact | Update inode/dentry namespace state and its associated filesystem lifetime/index state. | Remove the path reference and atomically update workspace revision, revision lifetime/GC candidacy, event, history, recovery, and dedupe state. | Code fact |

The table does **not** claim that every path-native write touches fewer rows.
The new model deliberately records Agent durability, replay, revision lifetime,
and recovery evidence that the old namespace operation did not express in the
same way. Write amplification must be compared with per-family counters under
a matched durability profile.

## Optimized Live Read Boundary

The first path-native server composition still performed approximately:

```text
WorkspaceCurrent
WorkspaceCurrent  # repeated inside visible-path resolution
PathCurrent
ArtifactRevision  # result shaping
```

The accepted exact-read target is:

```text
WorkspaceCurrent
PathCurrent
```

It is achieved by passing the already-resolved visible workspace into the path
primitive and storing the immutable result projection in `PathCurrent` in the
same publication command as `ArtifactRevision`. `ArtifactRevision` remains the
revision-lifetime authority. Ordinary path metadata does not read it.
This describes two authoritative metadata payload reads, not two physical
point operations. The current storage-neutral path first reads owner fence,
root fence, and commit clock. It then repeats those three guards in the same
`ReadBatch` as `WorkspaceCurrent`, and again with `PathCurrent`, for eleven
point operations on a cold exact get. `TxnStore` deliberately has no read
session that spans calls. A later retained or declarative read-view design can
remove the repeated guards, but it must preserve one owner-fenced snapshot
across the marker-dependent path lookup.

The first listing composition scanned the whole incarnation and filtered the
requested prefix in server memory. The metadata layer also materialized the
complete prefix before it applied the cursor and limit. The accepted live-list
target is one marker check, a store seek at the encoded descendant prefix, an
exclusive cursor, and termination when the page is full. An
optional exact-path probe is deferred until the descendant iterator reaches
EOF, preserving "exact prefix or descendants" semantics without paying that
point read on every hot-directory page. Each bounded store page batches its
owner fence, root fence, and commit clock with the scan. `ScanPage::more` is the
only completion signal. Neither adapter nor metadata layer materializes the
complete physical prefix before applying the page bounds.

Non-recursive pages also pass the encoded component delimiter to `TxnStore`.
The adapter folds each deeper subtree into a `CommonPrefix`. NoKV retains that
as one storage-neutral implicit `Prefix` item. An exact artifact at the same
child wins during logical coalescing. Prefixes and artifacts both count toward the
page limit and can become cursor anchors. Recursive pages retain the ordinary
ordered prefix iterator and emit artifacts only. System format version 9
retains the version-8 path layout: NUL between components and `0x01` at exact
keys give a rollup, exact child, and longer sibling one pagination-safe order.
Each UTF-8 component byte is shifted by one so the delimiter and exact marker
cannot occur inside an encoded component, while byte order and key length stay
unchanged.

Historical MVCC is a distinct path. A key that is absent or newer in current
state may still be visible at the requested version, so historical listing
continues to reconstruct a sorted visible set from current rows and `History`
before delimiter folding and pagination. Its work is not bounded by `p`; the
live bounded-work result does not apply to snapshot pages with dense history.

## Sharding And Write Concurrency

The pre-cutover control plane routed path prefixes to subtree shards and used
foreign-inode graft dentries at subtree boundaries. That enabled a namespace
tree to span shards, but required inode ownership, graft reconciliation, and
cross-shard filesystem rules.

The path-native authority is instead:

```text
RootId -> immutable LogicalShardId -> current fenced physical owner
```

All workspaces, paths, revision references, and revision-owned object
identities for one root remain on that logical shard. Many roots can scale
horizontally across shards; one hot root cannot be split by filename.

Current metadata mutations also take the shard-wide `command_gate` write lock,
read one shard-wide commit clock, and require the command's read version to
equal that clock. Therefore unrelated writes on the same logical shard are
serialized and a plan based on an older clock may need retry after another
write commits. Static command validation, canonical digest calculation, and
dedupe-key construction now happen before that lock is acquired; planning,
atomic publication, recovery ordering, and the synchronous durability
acknowledgement remain inside it. This is a code fact, not a measured
contention result.

Removing that boundary is **NOT QUALIFIED** by the read optimizations.
Any later concurrency design must preserve deterministic commit-version and
recovery-LSN order, history visibility, exact request replay, root fencing,
and owner-epoch validation at the atomic commit boundary. Until such a design
and its crash/replay tests exist, the single-root/single-shard write hotspot is
explicitly retained.

## Performance Qualification

| Claim | Status | Reason |
| --- | --- | --- |
| Path-native exact get is faster than the inode/dentry baseline | **NOT QUALIFIED** | Read amplification is lower by code inspection, but no matched metadata or service latency run exists. |
| Target-prefix live listing is faster than the baseline | **NOT QUALIFIED** | It removes workspace-wide scanning, but non-recursive selectivity, cursor position, history density, and cache state require matched workloads. |
| Path-native publication has lower write amplification | **NOT QUALIFIED** | The two models persist different lifecycle and recovery evidence; no per-family matched counters exist. |
| Shard write throughput improved | **NOT QUALIFIED** | The shard-wide write gate and global commit clock remain. |

`nokv-workspace-bench` remains a protocol-codec diagnostic.
`nokv-bench metadata` now provides a matched metadata read workload for exact,
recursive-list, and direct-list cursor paths, with pre/post semantic assertions
and explicit qualification boundaries. A baseline that predates the runner
must receive semantically equivalent, reviewable instrumentation while the
runner and its implementation-invariant tests remain byte-identical; both
patches are bound by the harness digest. Published comparisons must retain raw
old/new reports from matched release builds and profiles. Even then, this
metadata-domain diagnostic does not qualify Workspace Acceptance Gate 8.
`bench/workbench-live` remains correctness and interoperability evidence
rather than a performance result.
