<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Workspace Acceptance

Status: normative qualification gates for the supported NoKV workspace.

NoKV is qualified as one Agent-facing system: Workbench contract, SDK, custom
CLI, MCP adapters, metadata semantics, object publication, root routing,
recovery, and garbage collection must run against the same workspace format.
Passing a codec or Holt microbenchmark alone does not qualify the product.

Every applicable gate reports exactly one status:

- `PASS`: the required test ran and retained reviewable evidence;
- `FAIL`: the test ran and violated the contract;
- `NOT QUALIFIED`: required evidence is absent, incomplete, or from a different
  workload or durability profile.

An ignored, skipped, environment-gated, or manually described test is
`NOT QUALIFIED` unless its required environment ran and retained its output.

## Evidence Record

Every qualification record includes:

- NoKV commit and dirty-worktree state;
- Rust toolchain, operating system, CPU, memory, and storage devices;
- Holt commit and durability profile;
- logical-shard count, physical owners, root placement, and owner epochs;
- object provider, endpoint class, bucket policy, and consistency assumptions;
- client/adapter versions and the Workbench schema digest;
- exact command, configuration, workload seed, start/end time, and raw output;
- `PASS`, `FAIL`, or `NOT QUALIFIED` for every gate below.

Performance records additionally retain warm/cold state, object and metadata
payload distributions, concurrency, duration, retries, error counts,
throughput, and p50/p95/p99/maximum latency.

## Gate 0: Workbench Contract And Live Workflow

The scientific reconstruction workflow must exercise the complete 18-tool
Workbench surface through the real adapter boundary.
The black-box runner is
[`scripts/workbench/live_workbench.py`](../../scripts/workbench/live_workbench.py).
Its dry-run proves only command construction and tool coverage. A live run
retains exact MCP and process evidence; absent etcd, S3-compatible storage, or
the requested binary is `NOT QUALIFIED`, never `PASS`.

Required evidence:

- exact normalized schemas for all 18 tools;
- golden result and error transcripts, not only input-schema validation;
- create-only and replace-only publication, never upsert;
- generation and digest relationships for put, append, edit, read, and stat;
- commit identity and exact replay;
- snapshot mint, frozen read, renew, retire, reap, and list states;
- source-preserving restore into an absent destination;
- materialize verified inputs and collect declared outputs without treating the
  local sandbox as namespace truth;
- stable `metadata/run_manifest.json` and
  `metadata/restore_manifest.json` projections.

The bounded live Workbench runner uses a minimum one-day snapshot lease. Even
when its 18-tool workflow passes, Gate 0 remains `NOT QUALIFIED` until separate
retained evidence observes expiry and the terminal `reaped` state.

Workbench responses must not expose storage keys, owner addresses, internal
incarnations, or host-filesystem identities.

## Gate 1: Schema And Startup

Required evidence:

- a fresh store writes the exact `nokv_workspace` schema marker and complete
  tree registry;
- reopen accepts only that marker, tree registry, shard identity, and required
  system records;
- unmarked, unknown, malformed, nonempty incompatible, and mixed stores fail
  closed before serving reads or writes;
- every durable value has golden bytes, round-trip tests, unknown-version
  rejection, invalid-enum rejection, and reopen coverage;
- every durable key codec has component-boundary, Unicode, ordering, and
  malformed-key tests.

No alternate reader, writer, schema alias, or automatic conversion path is
part of acceptance.

## Gate 2: Namespace And Visibility

Required evidence:

- `WorkspaceCurrent(root_id, workbench_id)` is the only name-to-incarnation
  visibility marker;
- `PathCurrent(root_id, incarnation, normalized_relative_path)` is the only
  namespace truth;
- exact reads use one logical marker payload read plus one canonical logical
  path payload read, with physical fence reads reported separately;
- non-recursive lists use one marker check plus one bounded delimiter scan;
- `a`, `ab`, and `a/child` remain component-safe;
- full-path pagination is ordered, exclusive, stable at one read version, and
  rejects malformed keys or records;
- staging and retired workspaces are absent from point, list, query, aggregate,
  catalog, watch, restore, and GC-visible surfaces;
- public domain reads cannot bypass the marker with a raw incarnation;
- request ids, indexes, restore members, and path references use the same path
  normalizer and canonical encoding.

## Gate 3: Publication And Idempotency

Required evidence:

- immutable object blocks are uploaded and verified before metadata visibility;
- one bounded, owner-fenced metadata command publishes the revision, manifest,
  path, workspace revision, references, indexes, event, GC candidacy, and
  deterministic result;
- every mutation has the exact value or absence predicate required by its
  operation;
- exact request replay returns the original typed result and commit version;
- reuse of a request id with different inputs fails;
- create-only, replace-only, generation CAS, append-head CAS, and commit-head
  CAS retain distinct behavior;
- response loss never creates a second revision or generation;
- failed upload, verification, predicate, command, or acknowledgment cannot
  expose a partial artifact;
- abort and cleanup race publication through one durable operation state.

## Gate 4: Snapshot, Commit, And Restore

Required evidence:

- snapshot mint creates a leased history hold at one read version;
- renew is extend-only and races reap through one lifecycle CAS;
- a durable commit freezes its input with a construction hold, writes an
  ordered member closure, adds exact revision references, verifies its seals,
  and releases the temporary hold;
- commit retirement fences new consumers before releasing members through a
  recoverable cursor;
- tags and Workbench heads update consumer references atomically;
- restore remains within one root and logical shard, preserves the source,
  requires an absent destination, and stages a fresh hidden incarnation;
- restore verifies member count/digest and reference closure before one final
  visibility command;
- retries after process loss, owner loss, or response loss converge to the same
  terminal result;
- after A succeeds and B replaces it, exact retry of A returns A's original
  terminal result for both original `replace=false` and `replace=true`, without
  reading the current workspace head or run-manifest path;
- commit recovery rejects any mismatch in the complete durable request,
  workspace incarnation, canonical manifest bytes, immutable manifest binding,
  or durable publish-operation result;
- recovery of a Running commit whose manifest is not yet staged binds the
  original presentation path and canonical Agent projection; a changed
  projection fails before any commit resubmission or artifact publication;
- restore copies metadata rows while reusing immutable object revisions.

## Gate 5: Reference Safety And Garbage Collection

Required evidence:

- every current path, commit member, and reused-block dependency owns an exact
  strong revision reference;
- reference add/remove, count, and epoch change atomically;
- a zero-reference candidate is claimable only for the current epoch and only
  while the revision is available;
- a new reference cannot race a claimed deletion;
- current paths, retained history, commits, build/restore/publish holds,
  operations, and owner fences all participate in reachability;
- object listing is never used as reachability truth;
- provider timeout or ambiguous delete is quarantined and reconciled;
- crash/reopen tests cover every claim, cursor, cleanup, and quarantine phase;
- fsck recomputes counts and closure seals from authoritative metadata.

## Gate 6: Routing, Ownership, And Durability

The real-etcd local-WAL epoch runner is
[`scripts/workbench/local_wal_recovery_gate.py`](../../scripts/workbench/local_wal_recovery_gate.py).
Its bench-owned fault process acquires the real control lease and holds the
same Holt authority; the production CLI contains no fault-only admission path.

Required evidence:

- root placement is persisted before the first write;
- a populated root stays on one logical shard;
- routing never hashes a filename or recomputes placement with modulo shard
  count;
- unsupported cross-shard operations fail before any partial work;
- every write and destructive provider action validates placement generation
  and owner epoch in the same physical transaction as the metadata commit;
- an expired or replaced owner cannot acknowledge writes or delete objects;
- local-WAL and shared-log profiles state different acknowledgment boundaries;
- checkpoint plus logical-log replay recovers the exact committed command
  sequence and deterministic results;
- failover tests inject loss before and after each durability boundary.
- local-WAL restart kills `Recovering(E+1)` both before and after the local
  owner fence advances, waits for the lease-attached etcd session to disappear,
  and proves retry reaches `Serving(E+1)` without allocating `E+2`;
- the epoch kill/retry record names the exact NoKV commit and dirty state,
  binary digests, etcd version, control records, local crash epoch, commands,
  process exits, and terminal metadata probe.

## Gate 7: SDK, CLI, MCP, And Package Boundaries

Required evidence:

- the SDK routes by root placement and never imports Holt layout;
- direct immutable-object reads and uploads obey server-issued plans and
  integrity checks;
- Python uses the SDK and explicit materialize/collect adapters;
- `nokv-agent` remains transport-free and shapes the stable 18 tools over SDK
  traits;
- CLI and MCP are thin consumers of client and Agent interfaces;
- protocol DTOs are versioned and storage-neutral;
- provider-specific behavior stays inside the object package;
- no second implementation of namespace, publication, restore, references, or
  routing exists in another package.

## Gate 8: Performance And Scale

The required workload matrix includes:

- cold and warm exact stat/open;
- non-recursive list across small, medium, and maximum qualified pages;
- create-only, replace-only, append, remove, and conflict-heavy publication;
- search and aggregate with declared selectivity and projection sizes;
- snapshot creation/renewal/reap;
- commit construction and retirement across member-count distributions;
- restore across entry-count and shared-revision distributions;
- GC under publication churn;
- one hot root, many roots, owner movement, and failover.

Each row names payload sizes, concurrency, durability, object provider, cache
state, machine profile, shard topology, and revision. Absolute numbers from
different rows are not directly comparable. A benchmark that exercises an
internal store API instead of the real product boundary is diagnostic only.

## Release Decision

A release is qualified only when:

1. every applicable correctness, durability, recovery, and contract gate is
   `PASS`;
2. performance gates have workload-matched evidence and no unexplained
   regression;
3. the package and dependency graph contains one authoritative implementation;
4. operator documentation names the exact schema, placement, object backend,
   backup, restore, fsck, and evidence-retention procedures;
5. the live Workbench golden workflow passes against the release artifacts.

The decision record links raw evidence. Design documents, source presence, and
unit-test counts are context, not substitutes for boundary-level results.

## Required Local Validation

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
python3 scripts/workbench/workbench_contract_test.py
git diff --check
```
