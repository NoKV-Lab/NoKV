<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# FDB Unknown-Outcome Qualification Implementation Plan

Date: 2026-09-01

Status: completed; Gate 2 passed on source
`4f33c6fca4c099d49d91d5a575d58669dc3d6c10`.

Qualification record:
[FoundationDB Metadata Root-Fix Qualification, 2026-08-31](./fdb-root-fix-qualification-2026-08-31.md#gate-2-unknown-outcome-qualification)

Design: [FDB Unknown-Outcome Qualification Design](./fdb-unknown-outcome-qualification-design.md)

## Objective

Close FDB serving Gate 2 with operation-specific exact readback, deterministic
real-commit lost-ack injection, a complete mutation-family matrix, and one
retained clean-candidate evidence bundle. Do not change Gate 2 to `PASS` until
the complete live bundle and repository gates pass.

This is the first of four independent remaining qualification projects. Gate
8 lifecycle, Gate 9 physical limits, and Gate 10 performance each require a
separate design and implementation cycle after Gate 2 is complete.

## Commit 1: Synchronize The Current Main Branch

Merge `origin/main` without rebasing or force-pushing.

Expected conflict disposition:

- accept the main-branch Holt `0.8.6` pin in `Cargo.toml`, `Cargo.lock`, build
  identity, metadata benchmark labeling, and benchmark documentation;
- keep `crates/nokv-control/src/etcd.rs` deleted;
- keep the retired `crates/nokv-control/src/store.rs` deleted;
- do not restore etcd features, dependencies, CLI parameters, bootstrap
  composition, or recovery-publication compatibility;
- retain the current `DistributedControlStore`, `FdbControlStore`, Holt local
  runtime, and FDB runtime boundaries.

The new main commits make an expired etcd recovery session idempotently
suspendable while fencing a rebound same-epoch lease. That recovery model is
not present in the accepted FDB runtime: FDB takeover advances both owner epoch
and session generation, and every mutation predicates on the exact session.
Therefore the old files remain deleted. Add or retain a provider-neutral/FDB
regression proving that a stale session cannot renew, fail-close, or release a
successor after two ownership generations.

Validation after the merge:

```text
cargo fmt --all -- --check
cargo test -p nokv-control
cargo test -p nokv-server --features fdb
cargo metadata --no-deps
git diff --check
```

The merge commit carries a DCO trailer and preserves all existing branch
commits.

## Commit 2: Reconcile Metadata Unknown Outcomes

### Owner epoch

Update `MetaShard::advance_owner_epoch_unlocked` in
`crates/nokv-meta/src/workspace/engine.rs`:

1. submit the existing checked transaction once;
2. on `Commit::Applied`, return success unchanged;
3. on `Commit::Conflict`, retain `MetaError::WriteConflict`;
4. only on `StoreError::OutcomeUnknown`, read the durable owner fence through
   the same session-fenced store;
5. return success only when it equals the exact requested `next` epoch;
6. otherwise return the original typed unknown error.

Do not call `advance_owner_epoch` recursively and do not resubmit the raw
transaction. A readback that is fenced, unavailable, corrupt, absent, behind,
or ahead cannot turn the original unknown result into success.

### Lease-clock high-water

Update `MetaShard::observe_lease_clock_unlocked` beside the existing write:

1. retain the exact schema, shard, owner, root-fence, and prior high-water
   values used to build the checked write;
2. after only an unknown commit result, perform one consistent read batch of
   the current owner fence, root fence, and lease-clock high-water;
3. validate the same logical shard, root, placement generation, owner epoch,
   and `Active` fence;
4. accept only a high-water at least as large as `observed_ms`;
5. return the original unknown error when exact fencing cannot be proved.

The live isolated case additionally requires equality with `observed_ms` to
prove one apply. The production method accepts a larger value because another
legal observation may have advanced the monotonic clock.

### Tests

Extend the existing `AppliedThenLostAckStore` tests in
`crates/nokv-meta/src/workspace/engine.rs` to cover:

- applied owner-epoch advancement followed by lost acknowledgement;
- unknown without apply retaining the typed error;
- readback of a mismatching/ahead epoch retaining the typed error;
- applied lease-clock advancement followed by lost acknowledgement;
- a larger legal high-water under the same fence;
- changed owner or root fence rejecting reconciliation; and
- exactly one inner commit for every case.

## Commit 3: Reconcile Distributed-Control Unknown Outcomes

Keep `nokv-control-fdb` as a one-attempt raw transaction adapter. Put
operation-specific reconciliation in `crates/nokv-server/src/fdb_runtime.rs`.

### Exact ownership helpers

For each ownership operation:

1. read one complete `OwnershipSnapshot`;
2. derive the exact expected `OwnershipUpdate` with the provider-neutral plan
   function;
3. issue the raw control mutation once;
4. validate the typed successful return;
5. on `CommitOutcomeUnknown` only, read a new complete snapshot and accept only
   equality with the expected snapshot.

Apply this pattern to:

- provisioning and serving owner acquisition;
- heartbeat renewal;
- route activation;
- route fail-close; and
- owner release.

The renewal oracle is the exact next heartbeat sequence for the same session;
there is no persisted wall-clock deadline. Steady-state renewal and bootstrap
keepalive must both call the same exact helper. If renewal cannot be proved,
the existing local admission fail-close remains authoritative.

### Catalog helpers

Strengthen unknown-outcome branches for:

- shard catalog creation plus its initial unassigned ownership state;
- root catalog creation;
- root Ready CAS; and
- shard Ready CAS.

The helper may accept an exact requested record or a fully validated legal
successor already produced by the same immutable catalog identity. Merely
observing a matching state enum is insufficient.

### Tests

Use operation-owned test doubles in `fdb_runtime.rs`; do not add public fault
hooks. Cover applied-then-unknown, unknown-without-apply, mismatching readback,
and successor replacement for every helper. Add the double-generation stale
cleanup test required by the main-branch audit.

## Commit 4: Add The External Lost-Ack Shim

Add:

```text
bench/fault_injection/fdb_commit_unknown.c
bench/fault_injection/build_fdb_commit_unknown_shim.sh
```

The C shared object interposes the exact FDB C symbols needed to associate a
target mutation with its commit future:

- `fdb_transaction_set`;
- `fdb_transaction_clear`;
- `fdb_transaction_clear_range`;
- `fdb_transaction_atomic_op`;
- `fdb_transaction_commit`;
- `fdb_future_get_error`; and
- `fdb_future_destroy`.

### Selector contract

The shim reads a versioned selector owned only by the qualification process:

- exact binary target key encoded as lowercase hex;
- mutation kind;
- selection mode and ordinal, or a private inherited one-shot arm descriptor;
- run nonce; and
- write-only event descriptor.

The controller passes descriptors, not credential-bearing paths. The shim
starts transparent. It never mutates an FDB transaction. Once the exact target
commit future reports real success, it returns `1021` once and writes a
bounded event record containing the run nonce, process/thread identities,
selector digest, real result, substituted result, and counters.

Real FDB errors pass through unchanged. Zero target commits, multiple target
transactions, duplicate arming, a second substitution, target-future
destruction before observation, malformed selector input, or an event write
failure prevents qualification.

### Concurrency and lifetime

Use one private mutex-protected state object initialized through
`pthread_once`. Track transaction and future pointers only until completion or
destruction. Do not allocate or call FDB recursively while holding the state
mutex. Bound every retained key and event field. Scrub the decoded raw target
key during shutdown.

### Contract fixture

Add a small bench-owned fake FDB C library/test executable beside the shim.
The build script compiles and runs it in Linux before producing the live shim.
It proves transparency, exact selector matching, real-error passthrough,
one-shot substitution, duplicate rejection, future destruction, and
multi-thread pointer isolation.

No workspace `build.rs`, new crate, production feature, or installed system
file is added.

## Commit 5: Add The Gate 2 Qualification Workload

Add the design-approved layout:

```text
bench/src/bin/nokv-fdb-unknown-outcome-qualification.rs
bench/src/fdb_unknown_outcome/mod.rs
bench/src/fdb_unknown_outcome/evidence.rs
bench/src/fdb_unknown_outcome/metadata.rs
bench/src/fdb_unknown_outcome/control.rs
```

Update `bench/Cargo.toml` and `bench/src/lib.rs` with one bench-only required
feature. Reuse `qualification_runtime::EvidenceBundle`, hashing, process
supervision, and atomic finalization.

### Process model

The top-level controller is never preloaded. It prepares isolated state,
launches one preloaded child, consumes the shim event stream, performs
independent readback, validates the child result, and cleans the prefix.

Use the exact production `nokv-fdb` candidate for format/provision/serve and
the ordinary workspace-command failover case. For an internal control method
without an isolated CLI/RPC trigger, launch the qualification binary in a
one-operation child mode linked to the real production crate. Retain both
binary digests and refuse mixed-source binaries.

### Required cases

Metadata groups:

1. initialize;
2. owner epoch;
3. root-fence install;
4. root-fence activation;
5. ordinary `MetadataCommand` with owner A fail-close, owner B takeover, and
   byte-identical request replay; and
6. lease-clock high-water.

Distributed-control groups:

1. manifest format;
2. shard catalog create;
3. root catalog create;
4. root catalog Ready CAS;
5. shard catalog Ready CAS;
6. provisioning-owner acquisition;
7. serving-owner acquisition;
8. owner renewal;
9. route activation;
10. route fail-close; and
11. owner release.

Every case uses a fresh process, prefix, logical identities, selector, and
event stream. Final qualification requires three clean repetitions of every
case. The no-injection control and one injected smoke case run first and do not
count as repetitions.

### Evidence

Implement the approved environment, candidate, injector, per-scenario, and
terminal schemas. Validate:

- clean source and identical binary digests;
- FDB/RustFS identities and health;
- exactly one real-success-to-`1021` event;
- typed product outcome;
- complete exact readback;
- one logical apply;
- no unproved `Serving` route;
- process exits and cleanup; and
- credential redaction plus an inventory digest.

Only the controller can atomically publish top-level `PASS`.

## Commit 6: Retain Live Gate 2 Evidence

Build one clean Linux candidate and qualification binary. Run against the
pinned real FDB 7.3.x cluster topology and pinned RustFS identity used by the
existing distributed qualification unless an explicitly recorded environment
change is required.

Run, in order:

```text
shim contract fixture
no-injection control
injected smoke case
complete matrix repetition 1
complete matrix repetition 2
complete matrix repetition 3
bundle audit
```

Then run the repository gates on the exact evidence source:

```text
cargo fmt --all -- --check
cargo test --workspace
cargo test --workspace --all-features --exclude nokv-python
cargo test -p nokv-python
cargo clippy --workspace --all-targets --all-features -- -D warnings
python3 scripts/workbench/workbench_contract_test.py
git diff --check
```

Use the repository-supported Python/FDB builder split documented in the
current qualification report. Verify with `readelf` that `nokv-fdb` has no
dynamic dependency on the shim and scan production sources/Cargo metadata for
fault-selector surfaces.

Only after the retained bundle is `PASS`, add its source revision, candidate,
qualification binary, FDB client/server, RustFS, shim, environment, and
terminal-result digests to
`docs/development/fdb-root-fix-qualification-2026-08-31.md`, and change Gate 2
to `PASS`. Gates 8, 9, and 10 remain `NOT QUALIFIED`.

## Stop Conditions

Do not publish a false partial PASS. Stop Gate 2 qualification and retain the
failure evidence when:

- the shim cannot prove the underlying real commit returned success;
- an operation needs a production-only fault hook;
- a raw ambiguous transaction is retried;
- exact readback accepts only a partial record;
- an old owner can affect a successor;
- any case applies twice or publishes an unproved route;
- the source or binaries change between repetitions; or
- the evidence bundle is incomplete or contains credentials.

An implementation failure is fixed and the complete matrix restarted from a
fresh clean candidate. Previous partial repetitions are diagnostic evidence,
not qualification evidence.
