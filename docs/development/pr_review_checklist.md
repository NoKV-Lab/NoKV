<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# PR Review Checklist

Findings come first, ordered by severity. Passing tests does not excuse weaker
metadata atomicity, object publication, retention, ownership fencing, package
boundaries, or Workbench behavior.

## Merge Governance

- If additions plus deletions exceed 5,000 lines, does one core maintainer
  other than the current-head pusher approve the exact current head commit?
- Did a new push make every older approval ineligible?
- Does the protected native `large-change-review` check pass for every change?
- Is the change actually reviewable as one logical boundary even if the formal
  approval count and status checks pass?
- Are every required status and review conversation complete before merge?

## Scope And Architecture

- Does the change implement only the workspace architecture?
- Does it change one logical package or lifecycle boundary?
- Are unrelated schema, object, control, SDK, adapter, docs, or benchmark
  changes split out?
- Is every observable behavior change described?
- Does every non-merge commit include a `Signed-off-by` trailer?
- Is LingTai the active integration partner?
- Does the change avoid FUSE, POSIX, CSI, fsspec, inode, and dentry behavior?
- Does the change preserve one authoritative schema, route, and implementation
  for each lifecycle state machine?

## Boundaries

- Does import direction match the code contract?
- Does `nokv-types` remain storage-neutral?
- Does `nokv-protocol` contain DTOs rather than storage or execution logic?
- Does `nokv-meta-store` expose only ordered byte-key transaction primitives,
  limits, profiles, and physical store errors?
- Does `nokv-meta-holt` implement only the local transaction-store adapter and
  avoid workspace records, codecs, server composition, and shared authority?
- Are reads linearizable, scan completion explicit, and `Applied` writes
  visible to later reads on the same store instance?
- Do unknown commit states prevent raw transaction retries and poison an
  uncertain local store before it can serve another request?
- Does `nokv-meta` own schema, command execution, history, indexes, holds,
  lifecycle, GC policy, and recovery semantics without importing a physical
  store implementation?
- Does `nokv-control` own placement/leases/epochs without learning path or
  artifact semantics?
- Does `nokv-object` avoid namespace, reachability, and metadata transaction
  ownership?
- Does `nokv-client` avoid dependencies on `nokv-meta` and `nokv-server`?
- Does `nokv-agent` shape the stable tool facade over SDK traits without
  duplicating SDK state machines?
- Does `nokv-python` use the SDK and explicit materialize/collect adapters
  without promising a host filesystem?
- Are server and CLI thin over their owned service/client boundaries?
- Are filesystem frontends and semantics absent from the product dependency
  graph?

## Namespace And Visibility

- Is `PathCurrent` the only workspace namespace truth?
- Does exact lookup use one canonical point key?
- Do child/subtree scans append the component delimiter so `a` cannot match
  `ab`?
- Do every request id, index key, operation id, and path key use the same
  normalization?
- Are directories implicit and the five Workbench sections virtual?
- Does `WorkspaceCurrent` gate point, list, search, aggregate, catalog, watch,
  restore, and GC visibility consistently?
- Do direct create and restore staging atomically create one permanent
  `WorkspaceIncarnationClaim`, preventing two names from sharing PathCurrent?
- Can any staging row leak through a secondary index or root-wide query?
- Does startup reject any store without the exact supported marker, including
  unmarked nonempty, unknown, or mixed schemas?

## Publication And Idempotency

- Are object blocks immutable, revision-owned, and uploaded/verified before
  metadata publication?
- Can upload or metadata failure expose a partial artifact?
- Does one bounded `MetadataCommand` atomically publish the revision, manifest,
  path, workspace revision, indexes, event, old-revision candidacy, and dedupe
  result?
- Are all predicates checked before every mutation?
- Does an exact request replay return the same result?
- Does reuse of a request id with different inputs fail?
- Do create-only, replace-only, generation CAS, append-head CAS, and commit-head
  CAS retain their distinct semantics?
- Does a response-loss retry avoid creating a second revision or generation?
- Does every strong-reference add/remove atomically update the revision count
  and epoch, making older GC candidates stale?
- If a new revision reuses old blocks, does its sealed dependency set retain
  every physical owner revision until the child is deleted?
- Do object-key validation and GC use the row's physical owner-local object
  index rather than the child manifest's ordered row position?
- Do publish finalization and staged-object cleanup race through one durable
  operation CAS before either metadata visibility or external deletion?

## Snapshot, Commit, Restore, And GC

- Is a leased snapshot kept distinct from a durable commit/tag?
- Does snapshot renew race the reaper through one lifecycle CAS?
- Does commit construction hold its frozen input with a read-version
  `HistoryHold` independent of the user snapshot lease?
- Does commit retirement fence new consumers with `Sealed -> Retiring` before
  releasing an unbounded reference set through a recovery cursor?
- Does a commit hold exact revisions instead of pinning unbounded metadata
  history?
- Is restore same-root/shard, destination-creating, source-preserving, hidden
  until marker publication, and idempotent after process/owner failure?
- Are current, historical, committed, building, restoring, forking, and
  publishing references all considered before revision deletion?
- Can only the current fenced logical-shard owner claim and delete its objects?
- Is an uncertain provider deletion quarantined and reconciled?
- Is reachability derived from metadata rather than object-store listing?

## Sharding And Recovery

- Is root placement persisted before the first write?
- Does routing avoid filename hashing and modulo-N recomputation?
- Are unsupported cross-shard operations rejected before partial work?
- Is owner epoch validated in the same physical transaction as the metadata
  commit?
- Are acknowledgement durability, checkpoint, logical-log replay, and recovery
  behavior stated and tested for the claimed mode?
- Do permanent object keys exclude physical owner addresses and epochs?

## Performance

- Does cold exact get perform exactly one logical workspace-marker read plus
  one logical authoritative path read, while accounting separately for the
  physical owner, root-fence, and commit-clock guards?
- Does non-recursive list seek only the targeted prefix through bounded
  delimiter-aware cursor scans, defer any exact-prefix point read until
  descendant EOF, and avoid per-entry fanout?
- Does ordinary put/replace/remove avoid prefix scans and stay within its
  documented predicate/mutation bound?
- Are index updates bounded and atomic with the authoritative entry?
- Does restore report metadata rows copied and object bytes copied?
- Are metadata write amplification, history writes, event writes, index writes,
  and dedupe writes attributable?
- Are benchmark claims tied to exact workload, payload, concurrency, machine,
  shard, backend, and durability profiles?
- Are p50, p95, p99, maximum, errors, retries, and achieved throughput retained
  rather than only an average?

## Workbench Contract

- Do public docs and examples present the native full CLI first, the Python SDK
  second, and the Rust SDK third, without presenting the deprecated `nokv mcp`
  sidecar as a supported integration surface?
- Do all 18 tool names and normalized input schemas remain stable?
- Does golden-transcript validation cover observable result and error behavior,
  rather than treating input-schema validation as equivalent?
- Is put still create-only or replace-only, never upsert?
- Do generation and digest relationships remain stable?
- Are snapshot state transitions and frozen reads preserved?
- Are `run_manifest.json` and `restore_manifest.json` stable projections?
- Are `inode`, `source_root`, `destination_root`, and `checkpoints.jsonl`
  absent from Workbench responses and contract state?

## Tests And Evidence

- Is there a package test for each local invariant?
- Is there a command/object/SDK/adapter contract test across the real boundary?
- Are predicate, replay, conflict, response-loss, crash, owner-change, and
  ambiguous-provider paths covered?
- Are S3/RustFS integration tests environment-gated rather than silently
  skipped while claiming coverage?
- Does durability, recovery, GC, or performance language link raw evidence?
- Does every applicable
  [acceptance gate](./workspace-acceptance.md) report `PASS`, `FAIL`, or
  `NOT QUALIFIED`?

## Required Validation

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
python3 scripts/workbench/workbench_contract_test.py
git diff --check
```
