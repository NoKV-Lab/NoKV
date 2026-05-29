<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# TLA Specifications

This directory holds bounded TLA+/TLC models for protocol-level correctness
claims in NoKV. The goal is not function-level coverage. The specs cover
authority boundaries, crash windows, replay rules, and metadata invariants that
ordinary unit tests cannot exhaustively interleave.

## Install

```bash
make install-tla-tools
```

This downloads the pinned `tla2tools.jar` under `third_party/tla/`.

## Run

```bash
make test-tla-smoke
make test-tla-nightly
make record-formal-artifacts
```

`test-tla-smoke` is the CI-sized positive model set. `test-tla-nightly` also
runs the larger contrast matrix whose specs are expected to produce
counterexamples.

## Coverage Matrix

| Spec | Coverage boundary | Main invariants | Related implementation tests |
| --- | --- | --- | --- |
| `Eunomia.tla` | Current root-issued bounded-grant authority protocol for detached coordinators. | Primacy, bounded inheritance, evidence usage coverage, verifier silence, retired-floor finality. | `coordinator/integration/*`, `coordinator/audit/*`, `meta/root/state/*`, `meta/root/replicated/*`. |
| `EunomiaMultiDim.tla` | Lease-start coverage model for served-read handoff. | No write behind a served read after successor issue. | `coordinator/integration/root_model_test.go`. |
| `MountLifecycle.tla` | Rooted fsmeta mount registration and retirement. | No implicit mount, terminal retirement, stable mount identity. | `fsmeta/exec/mount_test.go`, `fsmeta/server/service_test.go`. |
| `SubtreeAuthority.tla` | Namespace subtree authority handoff consumed by subtree rename. | One active authority, successor frontier coverage, sealed-reply rejection, predecessor finality. | `fsmeta/integration/namespace_chaos_test.go`, `fsmeta/contract/*`. |
| `Percolator2PC.tla` | 2PC primary/secondary decision authority and lock lifecycle. | Secondary follows primary, min commit ts, rollback marker exclusion, commit before TTL expiry, GC below live locks. | `txn/percolator/txn_model_test.go`, `txn/percolator/crash_matrix_test.go`, `raftstore/mvcc/*_test.go`. |
| `MVCCGC.tla` | MVCC safepoint admission and destructive cleanup. | Safepoint below active snapshots/locks, visible version retained, rollback/default tombstone/version removal only below safepoint. | `raftstore/mvcc/*_test.go`, `txn/mvcc/gc_test.go`. |
| `RaftstoreApplyPublish.tla` | Raft Ready apply/publish/advance/send and snapshot publish boundary. | Publish requires apply, send requires advance, snapshot publish requires completed installation, aborted install is never published. | `raftstore/peer/peer_test.go`, `raftstore/store/peer_lifecycle_test.go`. |
| `RootReplayWatch.tla` | Root epoch, snapshot replay, follower catch-up, and watch gap reconciliation. | Snapshot/follower/watch cursor never ahead of root, no silent watch gap, replay lag is explicit through pending replay or reconcile. | `coordinator/integration/root_model_test.go`, `meta/root/state/*_test.go`, `fsmeta/exec/watch/*_test.go`. |
| `FSMetaNamespace.tla` | Small root-directory namespace model for create/link/unlink/rename/snapshot/session. | Dentries point to live inodes, link counts match dentries, sessions target live non-expired inodes, snapshots only reference known inodes. | `fsmeta/contract/*`, `fsmeta/integration/history_contract_test.go`. |
| `PerasVisibleCommit.tla` | Peras visible-log, optional witness layer, store-install, optional root-seal, runtime-install, visible-applied marker, GC, and holder handoff boundary. | Acked ops retain a recovery source until runtime install, configured witness-required ops require witness before store install, publish-required root seal requires witness-backed store install, visible-log compaction requires an applied marker, successor handoff requires acked ops to be drained. | `experimental/peras/runtime/*_test.go`, `experimental/peras/exec/*_test.go`. |

## Contrast Models

These specs intentionally omit one safety boundary and should fail under TLC:

- `LeaseOnly.tla`: no reply-side guard and no rooted handover record; expected
  to violate `NoOldReplyAfterSuccessor`.
- `LeaseStartOnly.tla`: no lease-start coverage check on predecessor served-read
  summaries; expected to violate `NoWriteBehindServedRead`.
- `TokenOnly.tla`: bounded-freshness token only; still expected to violate
  `NoOldReplyAfterSuccessor`.
- `ChubbyFencedLease.tla`: per-reply sequencer fencing; expected to preserve
  stale-reply rejection but violate successor coverage.
- `SubtreeWithoutFrontierCoverage.tla`: subtree handoff without successor
  frontier coverage; expected to violate `Inheritance`.
- `SubtreeWithoutSeal.tla`: subtree handoff that leaves the predecessor active;
  expected to violate `Primacy`.

## Policy

New protocol or metadata lifecycle work should update this matrix in the same
patch when it changes one of these boundaries. If the change is implementation
only, update the related Go history/simulation test instead and say why the TLA
state machine is unchanged.

`record-formal-artifacts` stores sanitized TLC outputs under `artifacts/`.
These are bounded model checks, not a TLAPS / Coq / Dafny proof.
