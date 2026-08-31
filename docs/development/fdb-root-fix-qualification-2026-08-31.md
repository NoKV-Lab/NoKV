<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# FoundationDB Metadata Root-Fix Qualification, 2026-08-31

## Result

The distributed-metadata root fixes pass their scoped acceptance checks. The
FoundationDB serving mode remains **NOT QUALIFIED** because the complete live
matrix in [Metadata Store Interface](./metadata-store-interface.md) has not
reached ten of ten gates.

This distinction is intentional:

- **Root-fix acceptance: PASS.** Recoverable provisioning, durable secondary-
  index replay, Ready-shard preservation, transient metadata availability,
  concurrent publication, owner/seed failover, and one-node FDB loss were
  exercised without a stranded catalog, semantic replay mismatch, or stale
  write.
- **FDB serving qualification: NOT QUALIFIED.** Commit-unknown injection,
  every serve crash cut, the complete lifecycle matrix, a measured maximum
  physical transaction envelope, and controlled performance evidence remain
  incomplete.

The normative runtime and package boundaries are the
[metadata-store interface](./metadata-store-interface.md),
[code contract](./code_contract.md), and
[metadata schema](../metadata-schema.md). This root-fix record covers
recoverable prepare/admit/finalize provisioning, immutable secondary-index
stage replay, Ready-shard preservation, and retry of settled transient metadata
reads. It does not expand the serving qualification boundary below.

## Candidate And Topology

| Role | Exact candidate |
| --- | --- |
| Source | `6399006adae8ce54c0c4e06e44f222d85a2681fa` |
| `nokv-fdb` SHA-256 | `f6ee02906a0ac5985ac501da402cd90d06bd8137e31b15356be4cf0093f83d91` |
| `nokv` SHA-256 | `e139dcb21827d102211f322f7435e49718e21ab156c61e299b0021b319fc2eb2` |
| Rust | `rustc 1.97.1 (8bab26f4f 2026-07-14)` |
| FDB server | `7.3.79`, protocol `fdb00b073000000` |
| `libfdb_c.so` SHA-256 | `f677f883c30869e8d00dbc15ef8a38228070a723a600d7219f5a1b10c0d3d7d0` |
| FDB image | `foundationdb/foundationdb@sha256:d3530c3066f94abffb61facac527c9c3517f6553ee0e75efa69d54296290156a` |
| RustFS image | `sha256:e620d37756fff072b10bf648c7bb9d370d7e91a928b7e6a5e1ac85bdfb4e4dab` |

The live FDB topology was three storage processes, three coordinators, three
logs, and `double` redundancy on Docker network `172.28.79.0/24`. Two NoKV
servers advertised `172.28.79.31:7750` and `172.28.79.32:7750`. RustFS provided
the durable object namespace. The retained bundle is:

```text
target/qualification/fdb-root-fix-6399006a-20260831T151806Z/
```

The earlier fresh-format and deliberately failed object-admission evidence is
retained separately because it predates only the lifecycle retry change:

```text
target/qualification/fdb-root-fix-288a6c80-20260831T145502Z/
```

## Gate 7 Seed Discovery Qualification

Gate 7 is **PASS**. The environment-gated Rust workload described by the
[seed-discovery qualification contract](./fdb-seed-discovery-qualification.md)
ran from one clean source revision against the exact `nokv-fdb` candidate, a
fresh FDB prefix, and a fresh RustFS object root. The retained bundle is:

```text
target/qualification/fdb-seed-gate7-final-20260901/
```

The bundle contains the candidate and cluster-file digests, FDB 7.3.79 status
before and after the run, pinned RustFS service identity and HTTP health before
and after the run, complete A/B route tuples, process PIDs and takeover
timeline, client transport attempts, typed peer transcripts with wire-frame
hashes, and the atomic terminal result.

| Scenario | Result | Retained oracle |
| --- | --- | --- |
| Multiple seeds | PASS | Two distinct configured seeds were contacted in recorded order. |
| Failed first seed | PASS | The first connection was refused and the later typed seed resolved A. |
| Owner endpoint change | PASS | The same client failed at A, refreshed through seeds, and succeeded through the strictly newer B route. |
| Stale discovery | PASS | An authentic A observation after B was cached could not regress the resolver. |
| Stale owner hint | PASS | A typed `NotOwner` response carrying A was ignored; authoritative refresh retained B. |
| Same-generation endpoint drift | PASS | A B-generation tuple with only its endpoint changed was rejected. |
| Immutable identity drift | PASS | A foreign logical-shard identity was rejected and its endpoint received no workspace request. |
| Final mutation/read | PASS | The persistent client created and read back one workspace through B. |

The qualification client used only the NoKV seed and workspace TCP protocol;
only the candidate server processes connected to FDB. This result closes Gate
7 only. Gates 2, 6, 8, 9, and 10 remain `NOT QUALIFIED`, so the overall FDB
serving profile remains **NOT QUALIFIED**.

## Root-Fix Acceptance Matrix

| Check | Result | Evidence |
| --- | --- | --- |
| Missing object configuration performs no FDB mutation | PASS | A fresh prefix failed before preparation; the following valid provision reported `preexisting=false`. |
| Object admission failure remains recoverable | PASS | A fresh root remained recoverable after an intentionally invalid endpoint; retry with RustFS reported `preexisting=true`, reached Ready, and Ready replay was idempotent. |
| Provision crash cuts | PASS | The live FDB test proved preparation releases ownership, root-Ready/shard-Provisioning convergence, abandoned admission retry, new root on a Ready shared shard, and Ready idempotence. |
| Durable secondary-index stage replay | PASS | Replay succeeded after an unrelated workspace publication and an operation heartbeat; changed locator or rows and the retired v1 result still fail closed. |
| Transaction-shape regressions | PASS | Four shape tests, two byte-budget tests, and the large secondary-index cleanup target passed. |
| Concurrent publication | PASS | Three consecutive rounds of 32 clients completed with `failures=0`; 96 response files contained no `RequestReplayMismatch` or `OperationInputMismatch`. |
| Owner failover | PASS | Killing the active owner produced a successor in 11 seconds; post-takeover write/read succeeded. |
| Failed seed first | PASS | With `172.28.79.32:7750` stopped and listed first, the client discovered through `172.28.79.31:7750`; write/read returned `failed-seed-first`. |
| One FDB process lost | PASS | The cluster remained available under double redundancy. An ambiguous control renewal fenced the old owner; the successor served a successful write/read while the FDB process was still down, and the cluster returned healthy after restart. |
| Lifecycle availability classification | PASS | A settled metadata read timeout (`FDB 1031`) motivated the fix. `StoreError::Unavailable` now retries, while metadata `Fenced`, control owner loss, corruption, and commit-unknown remain terminal. The regression test and the rerun under node loss passed. |
| Holt + RustFS non-regression | PASS | Fresh Holt format/provision/serve, Workbench create/write/read, graceful process restart, read-after-reopen, and a new write after restart all succeeded. |

FDB error `1021` during a control lease renewal is not treated as harmless
availability. Its commit outcome is ambiguous, so the owner fails closed and a
successor must take over. That observed failover is the required safety
behavior, not a false availability failure.

## Repository Gates

| Command or check | Result |
| --- | --- |
| `cargo fmt --all -- --check` | PASS |
| `cargo test --workspace` with `PYO3_PYTHON` bound to Homebrew Python 3.14 | PASS |
| `cargo test --workspace --all-features --exclude nokv-python` in the FDB builder | PASS |
| `cargo test -p nokv-python` with default features | PASS, 23 passed and 2 live S3 tests ignored |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` in the FDB builder | PASS |
| `python3 scripts/workbench/workbench_contract_test.py` | PASS, 8 passed |
| `git diff --check` | PASS |
| DCO trailers on every branch commit | PASS |

Running `cargo test --workspace --all-features` as one command is not a valid
Python test composition: PyO3's `extension-module` feature deliberately omits
the `libpython` link required by a Rust test executable. The split invocation
above tests the all-feature Rust workspace and the Python crate in its
executable test configuration without weakening either check.

## Ten-Gate FDB Serving Status

| Gate | Status | Qualification boundary |
| --- | --- | --- |
| 1. Conformance | PASS | Real FDB point reads, scans, stable versions, predicates, conflicts, bounds, namespace isolation, and reopen ran. |
| 2. Unknown outcomes | NOT QUALIFIED | Code-level mapping and observed fail-closed control behavior exist, but injected metadata commit-unknown readback across every mutation family is incomplete. |
| 3. Session fencing | PASS | Real control and metadata tests rejected stale renew, read, write, and release behavior. |
| 4. Takeover | PASS | Concurrent contenders, monotonic expiry observation, generation advance, owner kill, and live takeover ran. |
| 5. Provision crashes | PASS | Preparation/finalization crash cuts and external admission retry converged. |
| 6. Serve crashes | NOT QUALIFIED | Active-owner loss passed, but every pre-activation and post-activation crash cut has not been retained. |
| 7. Seed discovery | PASS | One clean-head live bundle retained multiple seeds, failed-first fallback, A-to-B refresh, stale discovery and owner hints, same-generation endpoint drift, immutable identity drift, and final mutation/read through B. |
| 8. Lifecycle | NOT QUALIFIED | Live publication ran through FDB, but restore, snapshot, retirement, GC, and ambiguous-delete quarantine have not all run live. |
| 9. Limits | NOT QUALIFIED | Deterministic 900,000-byte planning tests pass, but the maximum physical affected-byte envelope is not yet measured and retained against the live cluster. |
| 10. Performance | NOT QUALIFIED | Concurrency is a correctness stress result, not controlled latency/throughput qualification. |

Therefore the accepted runtime direction remains:

```text
holt:///absolute/path
fdb:///absolute/fdb.cluster?prefix=nokv-prod
```

Holt is the standalone runtime. FDB is the distributed metadata authority and
has passed the root fixes in this record, but its public serving status remains
**NOT QUALIFIED** until the remaining live gates are complete.
