<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# FDB Main Synchronization And Lease-Safety Design

Date: 2026-09-01

Status: implemented; Gate 6 and Gate 7 clean-candidate evidence passed.

## Decision

Merge `origin/main` at `f2daa0d2e7f8d3c829fd6d107571279199af7e77`
into `codex/fdb-characterization` with one merge commit. Preserve the existing
32 branch commits and do not force-push. Keep etcd and the retired bootstrap
composition deleted, while adapting the safety semantics from the main-branch
bootstrap keepalive fixes to the Holt/FDB dual-runtime architecture.

This design follows the package boundaries in
[`code_contract.md`](code_contract.md) and the storage-safety checks in
[`pr_review_checklist.md`](pr_review_checklist.md). It does not restore a
compatibility path.

## Scope

This change owns four related outcomes:

1. resolve the current pull-request conflict with `main` without restoring
   etcd, the retired local/shared-log bootstrap, or its benchmark;
2. carry typed owner-loss and lifecycle drain semantics into the current
   server registry and FDB composition;
3. keep every acquired FDB owner session alive continuously from acquisition
   through route activation and the first steady-state server renewal; and
4. retain live Gate 6 evidence for pre-activation failure, post-activation
   owner loss, renewal failure, stale-owner rejection, and successor takeover.

This change does not qualify unknown commit outcomes, the complete lifecycle
matrix, physical transaction limits, or performance. Gates 2, 8, 9, and 10
remain `NOT QUALIFIED` after Gate 6 work.

## Conflict Disposition

| Path | Resolution |
| --- | --- |
| `bench/src/bin/nokv-restore-crash-owner.rs` | Keep deleted. It exercises the retired distributed-local-log composition and must not become the FDB crash harness. |
| `crates/nokv-control/src/etcd.rs` | Keep deleted. FDB control I/O remains in `nokv-control-fdb`. |
| `crates/nokv-control/src/options.rs` | Keep deleted. Provider-neutral domain types stay in the responsibility-named `nokv-control` modules. |
| `crates/nokv-control/src/store.rs` | Keep deleted. `DistributedControlStore` remains the current storage-neutral contract. |
| `crates/nokv-server/src/bootstrap.rs` | Keep deleted. Holt and FDB composition remain in `holt_runtime.rs` and `fdb_runtime.rs`. |
| `crates/nokv-server/src/registry.rs` | Accept the main-branch generalized shard-operation permit and typed fail-close entry points, then retain the current route model. |
| `crates/nokv-server/src/lifecycle.rs` | Accept the main-branch provider-delete admission/drain barriers for both aborted publication cleanup and revision GC. |
| `crates/nokv-server/src/server.rs` | Resolve manually: retain route discovery and dual-runtime constructors, while adopting typed owner-loss propagation and exact renewal-policy validation. |
| `crates/nokv/src/main.rs` | Resolve manually: retain `--meta-url`, Holt/FDB composition, seed discovery, and the current CLI deprecation text. Do not restore etcd options. |

The merge is not complete merely because textual conflicts are gone. The
result must preserve the behavior described below and pass the complete
validation matrix.

## Owner-Loss State

`OwnerLossSignal` becomes shared state containing:

- an atomic lost flag; and
- the first typed `nokv_control::ControlError`, protected by a mutex.

`fail_closed_with_control` stores the first typed cause before publishing the
lost flag. Later failures cannot replace the first cause. A caller that checks
owner retention returns `ServerError::Control` when a typed cause exists and
uses a generic fail-closed error only when no control cause was recorded.

`RootOwnerRegistry` owns the admission boundary for both RPC responses and
owner-fenced background operations. Its permit becomes a
`ShardOperationPermit`. `fail_closed_shard_with_control` records the typed
cause, closes admission, removes every route for that logical shard, and waits
until all previously admitted permits drain.

This ordering provides the required guarantee:

```text
owner check -> shard permit -> provider delete -> permit drop
                       ^
                       |
fail-close closes admission and waits here
```

A lifecycle worker must obtain the permit after its final owner and durability
checks but before each destructive provider call. Therefore a fail-close
either wins before dispatch and prevents the delete, or waits for an already
admitted delete to finish before ownership can be handed off.

## FDB Bootstrap Keepalive

`fdb_runtime.rs` owns an FDB-specific bootstrap keepalive because it owns the
distributed serving composition. The implementation is an RAII object with:

- the exact `FdbControlStore`;
- the current set of acquired `OwnerSession` values;
- the shared `RootOwnerRegistry`;
- a stop signal, first typed failure, and one named worker handle; and
- the exact renewal interval derived from the configured FDB lease TTL.

The keepalive starts before the first acquired session is exposed to slow
bootstrap work. Each session is registered immediately after exact
acquisition and before metadata open, owner-epoch advancement, root-fence
validation, object-provider admission, lifecycle construction, or route
activation. The worker renews only the exact stable sessions. It does not
retry a failed raw transaction automatically.

On a renewal failure, the worker records the typed control error and
fail-closes every acquired shard through the registry and FDB control store.
On a worker panic, an unwind guard synthesizes a typed backend control error,
records it, and performs the same fail-close. Bootstrap health checks surface
the first typed cause rather than converting it to a generic string.

The keepalive remains owned by `FdbOwnership` after `serve_fdb` returns. It
continues across object-store setup, listener binding, lifecycle-worker
startup, and `Activating -> Serving` route publication. It is not stopped at
the end of metadata open.

## Bootstrap-To-Steady-State Handoff

The handoff is serialized by the FDB ownership maintenance lock:

1. the server socket loop invokes renewal immediately before admitting its
   first connection;
2. the renewal path stops and joins the bootstrap worker;
3. it checks the worker's typed health result;
4. it renews every exact session once synchronously; and
5. only then does it mark steady-state renewal as active and allow admission.

No bootstrap and steady-state renewals run concurrently. If the bootstrap
worker failed before the handoff, the first server renewal returns the typed
cause and the RPC loop admits nothing. If the synchronous handoff renewal
fails, the same fail-close and rollback path runs.

`OwnershipMaintenance` exposes its required renewal interval. The distributed
server constructor rejects a `ServerOptions` interval that differs from the
FDB ownership policy. Standalone Holt has no expiring control lease and does
not acquire a bootstrap keepalive.

## Activation, Failure, And Release Ordering

The successful FDB sequence is:

```text
acquire exact session
  -> register with bootstrap keepalive
  -> open fenced metadata and advance owner epoch
  -> install local registry routes while shared route is Activating
  -> construct object and lifecycle dependencies
  -> publish every shared route as Serving
  -> first immediate server renewal and keepalive handoff
  -> admit RPCs and continue steady-state renewal
```

Every error before the first admitted RPC follows the inverse order:

```text
publish owner loss
  -> close local shard admission and drain permits
  -> exact fail-close of shared FDB routes
  -> stop and join bootstrap keepalive
  -> exact release where still owned
  -> return the primary typed error plus bounded cleanup errors
```

Release remains idempotent. A stale session cannot fail-close or release a
successor because every FDB control mutation retains the exact stable-session
predicate. Shutdown cannot hide a previously observed owner loss: owner loss
wins over a concurrently requested graceful shutdown.

## Tests

### Deterministic Rust tests

The implementation adds or retains focused tests for:

- a bootstrap phase longer than one lease TTL while the exact session remains
  renewable;
- renewal between owner acquisition, metadata open, registry admission, and
  route activation;
- typed renewal failure before `serve_fdb` returns;
- bootstrap keepalive panic fail-closing the complete owner scope;
- constructor rejection of renewal cadence drift;
- first steady-state renewal handing off without a renewal gap or concurrent
  renewer;
- fail-close winning the check-to-provider-dispatch race;
- fail-close waiting for an already admitted provider delete;
- both aborted-publication cleanup and revision-GC destructive paths; and
- repeated fail-close/release preserving a successor's newer session.

Test probes may inject control outcomes and block provider dispatch inside
`#[cfg(test)]` code. Benchmark-only behavior must not enter product APIs.

### Live Gate 6 qualification

An environment-gated workload in `bench/` uses the exact `nokv-fdb` candidate,
a real FDB cluster, and the pinned S3-compatible RustFS service. It retains:

1. a pre-activation kill while the shared route is `Activating`, proving it
   never becomes `Serving` under the dead session;
2. successor takeover after unchanged-session monotonic observation, proving
   owner epoch and session generation advance;
3. a post-activation owner kill after one committed workspace mutation;
4. a retained stale-session write attempt rejected by the metadata fence;
5. an injected renewal failure that closes admission before another mutation;
6. successor mutation and read-back after takeover; and
7. FDB/RustFS health before and after every scenario.

The evidence records the clean source revision, candidate digest and version,
FDB client/server versions, cluster and prefix digests, route/session/
heartbeat observations, process exits, protocol transcripts, and final
cleanup. It records no credentials.

## Validation And Acceptance

The synchronization is acceptable only when all of the following hold:

- the worktree contains no tracked etcd dependency, option, CLI route, old
  bootstrap module, or `docs/superpowers` artifact;
- every non-merge commit has a `Signed-off-by` trailer;
- `cargo fmt --all -- --check` passes;
- `cargo clippy --workspace --all-targets -- -D warnings` passes;
- the FDB-featured server and CLI targets compile;
- `cargo test --workspace` passes with the repository-supported Python test
  interpreter;
- `python3 scripts/workbench/workbench_contract_test.py` passes;
- `git diff --check` passes;
- the existing Gate 7 seed-discovery evidence remains valid on the merged
  candidate or is rerun if the candidate behavior changes;
- the new Gate 6 live bundle passes every scenario above; and
- the qualification report changes Gate 6 to `PASS` only after the clean-head
  evidence audit succeeds.

Even after Gate 6 passes, the overall FDB serving profile remains
`NOT QUALIFIED` while Gates 2, 8, 9, and 10 are incomplete.
