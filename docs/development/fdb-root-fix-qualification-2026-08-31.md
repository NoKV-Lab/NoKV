<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# FoundationDB Metadata Root-Fix Qualification, 2026-08-31

## Result

The distributed-metadata root fixes and FDB serving Gates 1 through 9 pass
their scoped acceptance checks. The FoundationDB serving mode remains
**NOT QUALIFIED** because the complete live matrix in
[Metadata Store Interface](./metadata-store-interface.md) has not reached ten
of ten gates.

This distinction is intentional:

- **Root-fix acceptance: PASS.** Recoverable provisioning, durable secondary-
  index replay, Ready-shard preservation, transient metadata availability,
  concurrent publication, owner/seed failover, and one-node FDB loss were
  exercised without a stranded catalog, semantic replay mismatch, or stale
  write.
- **Gate 2 unknown-outcome qualification: PASS.** One clean candidate passed
  the no-injection control, injected smoke case, production bootstrap cases,
  ordinary cross-owner request replay, and all 17 required mutation families
  for three clean repetitions under deterministic real-commit lost-ACK
  injection.
- **Gate 6 serve-crash qualification: PASS.** The approved pre-activation,
  post-activation, stale-session, renewal-failure, and successor-recovery
  matrix passed on the merged lease-safe candidate.
- **Gate 8 lifecycle qualification: PASS.** Public publication, replacement,
  snapshot renewal and retirement, restore, revision GC, ambiguous-delete
  quarantine, and zero-consumer commit retirement passed against real FDB and
  RustFS under one serving session.
- **Gate 9 limits qualification: PASS.** Real `FdbStore` transactions retained
  logical, conservative, and FDB-observed physical byte measurements through
  2,800,000 logical bytes. A 3,200,000-byte object round-tripped through the
  exact candidate while a full-prefix scan found no payload marker in FDB.
- **FDB serving qualification: NOT QUALIFIED.** Commit-unknown injection,
  exact reconciliation, session fencing, takeover, provisioning recovery,
  serve-crash recovery, seed failover, and the complete lifecycle matrix have
  passed. Controlled performance evidence remains incomplete. Gates 8 and 9
  must also be rerun with Gate 10 on the final accepted source revision.

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

## Gate 2 Unknown-Outcome Qualification

Gate 2 is **PASS**. The approved
[unknown-outcome qualification plan](./fdb-unknown-outcome-qualification-plan.md)
ran one clean Linux candidate against a real three-node FDB 7.3.79 cluster and
the pinned RustFS service. The retained bundle is in the builder target volume:

```text
/target/gate2-52fd14a2-run1/
```

| Evidence identity | Exact value |
| --- | --- |
| Source | `52fd14a29dd344973c5cc06752c5a51bd440a0a3` |
| `nokv-fdb` SHA-256 | `287efde4dd3841d36da715cca6d64731d9fbe51b7ed340275eb67140cfc834b9` |
| Qualification binary SHA-256 | `7ccbee0b1f3b28eef74fd357ca1f401069ecb3ae082c1614ffca3c74ffe8682f` |
| Lost-ACK shim SHA-256 | `8a984761c8955560a91354b5458b8190695f36cc5920d6f299fcf6a80aef3401` |
| `libfdb_c.so` SHA-256 | `f677f883c30869e8d00dbc15ef8a38228070a723a600d7219f5a1b10c0d3d7d0` |
| FDB | `7.3.79`, protocol `0fdb00b073000000`, three coordinators, `double` redundancy |
| FDB image | `foundationdb/foundationdb@sha256:d3530c3066f94abffb61facac527c9c3517f6553ee0e75efa69d54296290156a` |
| RustFS image | `rustfs/rustfs@sha256:e620d37756fff072b10bf648c7bb9d370d7e91a928b7e6a5e1ac85bdfb4e4dab` |
| Environment SHA-256 | `516100ad934308b5a7cd30cfed426caa6934b3e25dd7ba0ac50477224b5fdb86` |
| Terminal result SHA-256 | `a1e3d7c8652712de5f4aabd792cba6e05a11f51111b45a1d5b4d9552b71a57b3` |
| Inventory SHA-256 | `2b5f238d7544c321c01709dc3a922f21f726f55add1e75dc91dcbfd5b68ba422` |

The atomic terminal result is `PASS`: 64 of 64 required scenarios completed.
Those scenarios comprise one no-injection control, one injected smoke case,
ten production format/provision bootstrap cases, one production ordinary
workspace-command failover case, and all 17 mutation families repeated three
times. The bundle contains 419 files, 64 scenario results, and 63 injector
event streams. Every injector stream ends with exactly one target commit,
exactly one successful-result substitution, and `invalid=false`.

The ordinary production path proved the cross-owner boundary directly. Owner
A held epoch/generation `3/3`; its metadata create committed but returned the
injected retryable lost-ACK outcome. Owner B took over at `4/4`, the client
tried the now-closed A seed before B, and byte-identical logical replay returned
the original result at commit version `5` without a second logical apply.
Changed business input remains a replay mismatch. The physical command digest
continues to bind the exact owner fence while the durable logical replay digest
normalizes only the superseded owner epoch.

The candidate uses workspace format `12`. Control manifests and both Holt and
FDB runtimes compile-time bind to that same metadata format. Format `11` is
rejected without rewriting its schema marker or recovery tail. FDB was healthy,
available, and quorum-reachable before and after qualification; RustFS returned
HTTP `200` before and after qualification. The bundle contains no credentials.
`readelf` shows that the production candidate depends on `libfdb_c`, libc,
libm, and libgcc only, with no dynamic dependency on the qualification shim;
production crates expose no lost-ACK selector or fault-injection surface.
This exact source also preserves typed FDB keepalive failures through a
poisoned failure slot and converges concurrent restore finalizers only after
an exact monotonic durable readback. Both regressions passed in the repository
gates below.

## Gate 6 Serve-Crash Qualification

Gate 6 is **PASS** for the matrix approved in
[FDB Main Synchronization And Lease-Safety Design](./fdb-main-sync-lease-safety-design.md).
The environment-gated workload used the exact `nokv-fdb` candidate, a real
three-node FDB cluster, and the pinned RustFS service. The retained bundle is:

```text
target/qualification/fdb-serve-gate6-4ba27f23-20260901/
```

| Evidence identity | Exact value |
| --- | --- |
| Source | `4ba27f237b59497cffd3b0753430f607a8d495a8` |
| `nokv-fdb` SHA-256 | `f465deaf522e61705de1ef4be679fea4dc25f2c32853743bdc9b8dcb118eefe2` |
| Qualification binary SHA-256 | `edd6902ea09a1f126aab574265ab6e52972464e1596559b8f2446ba2133c5809` |
| `libfdb_c.so` SHA-256 | `f677f883c30869e8d00dbc15ef8a38228070a723a600d7219f5a1b10c0d3d7d0` |
| FDB | `7.3.79`, protocol `fdb00b073000000`, API `730` |
| RustFS image | `rustfs/rustfs@sha256:e620d37756fff072b10bf648c7bb9d370d7e91a928b7e6a5e1ac85bdfb4e4dab` |
| Terminal result SHA-256 | `13e8ec0e0a27d3fb977fc19946123cabf47298b3c378a5c48de97f524d62b75b` |
| Environment SHA-256 | `15cb6fffcd8b7782ac87b003344b9f45db5ab0c857aba5c387f01dfc9ffdd3ce` |

The controller stopped all three FDB containers while leaving RustFS online,
then restarted the exact containers. It was external to product code; no
benchmark-only failure hook entered the server API. The bundle contains 132
files, an atomic `PASS` result, 33 successful recorded commands, 29 FDB/RustFS
health or outage observations, process exits, route/session/heartbeat
snapshots, and typed workspace protocol evidence. It contains no credentials.

| Scenario | Result | Retained oracle |
| --- | --- | --- |
| Pre-activation crash | PASS | Owner A was killed while `Activating`; epoch/generation `3/3` and heartbeat sequence `5` remained unchanged, and the dead session never became `Serving`. |
| Pre-activation successor | PASS | Owner B took over at `4/4`; its heartbeat advanced `6 -> 7` before the first retained mutation. |
| Post-activation owner loss | PASS | After B committed workspace version `5` and died, owner C took over at `5/5` and read back B's exact workspace incarnation. |
| Stale-session write | PASS | A retained raw B metadata handle returned exact `StoreError::Fenced { expected_owner_epoch: 4, expected_session_generation: 4 }`. |
| Renewal failure | PASS | During complete FDB outage, RustFS stayed HTTP `200`; C exited with the typed FDB `1031` control-plane failure, did not accept another mutation, and its heartbeat stopped at `9`. |
| Final successor | PASS | After FDB recovery, owner D took over at `6/6` and completed a new workspace mutation and read-back. |

This closes Gate 6 only. A dead route may remain durably `Serving` while FDB
is unavailable because fail-close cannot commit to the unavailable control
store; local admission closes immediately, the endpoint exits, and takeover
still requires monotonic lease expiry plus both fence advances after recovery.

## Gate 7 Seed Discovery Qualification

Gate 7 is **PASS**. The environment-gated Rust workload described by the
[seed-discovery qualification contract](./fdb-seed-discovery-qualification.md)
ran from one clean source revision against the exact `nokv-fdb` candidate, a
fresh FDB prefix, and a fresh RustFS object root. The retained bundle is:

```text
target/qualification/fdb-seed-gate7-4ba27f23-20260901/
```

The rerun used source `4ba27f237b59497cffd3b0753430f607a8d495a8`
and the same `nokv-fdb` SHA-256 as Gate 6. Its atomic result and environment
SHA-256 values are respectively
`4dc4bae4fa6d0d860b3e6deea279ea85c5b3051970d0f9428855acfdd8e68351`
and
`3951a61b1248be96295fd364d02507df851ce08ff7ac94219047e07519cfd2f5`.
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
7 only; the following sections record the independent Gate 8 and Gate 9
results. The overall FDB serving profile remains **NOT QUALIFIED**.

## Gate 8 Lifecycle Qualification

Gate 8 is **PASS**. The environment-gated workload described by
[FDB Remaining Qualification Design](./fdb-remaining-qualification-design.md)
ran one exact `nokv-fdb` candidate against the real three-node FDB 7.3.79
cluster and pinned RustFS service. Its external proxy forwarded the selected
S3 `DELETE`, retained the successful upstream response digest, forwarded zero
response bytes, and closed the candidate connection. No qualification-only
fault surface entered a production crate. The retained bundle is:

```text
/target/fdb-lifecycle-gate8-905e1193-run1/
```

| Evidence identity | Exact value |
| --- | --- |
| Source | `905e1193a9a794a4446cb5c015a39187f2a90fee` |
| `nokv-fdb` SHA-256 | `11a2cc5c2152e9a90d5e0edc8fe3eaa4b478a1ec91ec4ef02f90afcacac76af0` |
| Qualification binary SHA-256 | `1807959858f8b5799502dcf4244c1722171492c73a16910ce117141b8f68f27c` |
| `libfdb_c.so` SHA-256 | `f677f883c30869e8d00dbc15ef8a38228070a723a600d7219f5a1b10c0d3d7d0` |
| RustFS image | `rustfs/rustfs@sha256:e620d37756fff072b10bf648c7bb9d370d7e91a928b7e6a5e1ac85bdfb4e4dab` |
| Terminal result SHA-256 | `0e1591f45e61376555cdeb28bb93e2daf445db0cf758090b60357448b892fa22` |
| Environment SHA-256 | `e2d50c3d251416289591c0bd15f64d869d54e07d9ef79ddacac08195159802b9` |

The atomic terminal result contains five passing scenarios and 81 retained
files. Public CLI operations created and replaced an artifact, minted and
renewed a frozen snapshot, restored and read it, then retired the snapshot.
Exact-generation removal completed normal RustFS deletion. In the ambiguous
case RustFS returned `204`, the proxy retained hashes for all 144 response
bytes while forwarding zero, and FDB retained the required quarantine state.
Finally, the exact candidate discovered and retired a codec-valid seeded
zero-consumer commit. All transitions remained fenced by one exact serving
owner session. Before/after FDB and RustFS health checks passed, and a scan of
the complete evidence bundle found no access or secret key bytes.

This result closes Gate 8 only. Gate 10 must still retain controlled
client-visible latency and throughput evidence, after which Gates 8 and 9 must
be rerun on that same final accepted source revision.

## Gate 9 Limits Qualification

Gate 9 is **PASS**. The environment-gated workload described by
[FDB Remaining Qualification Design](./fdb-remaining-qualification-design.md)
used adapter-owned, read-only atomic diagnostics around real
`get_approximate_size` observations. The counters cannot mutate a transaction,
change a limit, inject a failure, or select a result. The object-separation
case used public `collect` and `materialize` commands through the exact
`nokv-fdb` candidate. The retained bundle is:

```text
/target/fdb-limits-gate9-fdc70710-run1/
```

| Evidence identity | Exact value |
| --- | --- |
| Source | `fdc70710aaee2524558b998e43edd8162e7d81ca` |
| `nokv-fdb` SHA-256 | `5cc7c64f7eb27d9b8f7c0ba591fcb65e11139cd42ce1e304afdf2d86d82cbd4c` |
| Qualification binary SHA-256 | `5430cb73b24fc3423eaab1753cdcfe1f6a363e49dd722c6c876c33cde69cdf6f` |
| Terminal result SHA-256 | `99797319948b4b9612038753e9c162d176b879560c3e585e262ba4e1a78ea102` |
| Environment SHA-256 | `502162ed25e41a9caeca2b0aeb59450bed81b29d324467c3cc77f27e18aecd24` |
| Envelope SHA-256 | `eaaf82e6a52031b2ce6eb06668f6967544e283579b2c25f25ac36a10063644de` |
| Object-separation SHA-256 | `b2f69abc16bb1bb43b4b94937df4f7a0feebae5b3767138ee0d45c33f54b53e5` |

Five valid transactions applied with zero conflicts, errors, or physical-guard
rejections. The exact logical, conservative, and FDB-observed approximate
physical byte triplets were:

| Logical bytes | Conservative affected bytes | Observed physical bytes |
| ---: | ---: | ---: |
| 65,536 | 65,920 | 65,947 |
| 262,144 | 263,104 | 263,335 |
| 900,000 | 902,880 | 903,791 |
| 1,800,000 | 1,805,568 | 1,807,431 |
| 2,800,000 | 2,808,448 | 2,811,331 |

The required 900,000-byte planner point is therefore below the configured
9,500,000-byte physical guard by 8,596,209 observed bytes. This measurement is
specific to the retained FDB 7.3.79 client/server, API 730, cluster topology,
and candidate; it is not promoted to a universal FoundationDB limit.

The 3,200,000-byte object exceeds the 2,900,000-byte logical metadata limit.
Its input digest, candidate publication digest, and materialized digest were
the same SHA-256 value. A consistent scan of all 150 retained rows under the
FDB store prefix observed at most 65,535 bytes in one value, zero values at or
above the payload length, and zero matches for three domain-separated 64-byte
payload markers. The bundle contains 39 files, pre/post healthy FDB and RustFS
observations, and no access or secret key bytes.

This result closes Gate 9 only. Gate 10 remains `NOT QUALIFIED`, and final
qualification still requires Gates 8-10 to pass on one accepted source.

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

The repository gates below were rerun on source
`52fd14a29dd344973c5cc06752c5a51bd440a0a3` after the latest `main` merge and
the concurrent restore-finalizer fix.

| Command or check | Result |
| --- | --- |
| `cargo fmt --all -- --check` | PASS |
| `cargo test --workspace` with `PYO3_PYTHON` bound to Homebrew Python 3.14 | PASS |
| `cargo test --workspace --all-features --exclude nokv-python` in the FDB builder | PASS |
| `cargo test -p nokv-python` with default features | PASS, 23 passed and 2 live S3 tests ignored |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` in the FDB builder | PASS |
| Real FDB metadata conformance and exact session fencing | PASS, 1 environment-gated integration test |
| Real FDB concurrent control contenders and takeover fencing | PASS, 1 environment-gated integration test |
| Real FDB provision release and catalog crash recovery | PASS, 1 environment-gated integration test |
| Gate 2 deterministic lost-ACK qualification | PASS, 64 of 64 scenarios, 63 exact one-shot injections, and 419 retained evidence files |
| Gate 6 serve-crash qualification | PASS, 6 scenarios and 132 retained evidence files |
| Gate 7 seed-discovery qualification rerun | PASS, 8 scenarios and 35 retained evidence files |
| Gate 8 lifecycle qualification | PASS, 5 scenarios and 81 retained evidence files |
| Gate 9 limits qualification | PASS, 5 measured transaction points, one 3,200,000-byte object round-trip, and 39 retained evidence files |
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
| 2. Unknown outcomes | PASS | One clean candidate passed deterministic real-commit lost-ACK injection for all 17 mutation families across three repetitions, plus production bootstrap and cross-owner ordinary request replay. |
| 3. Session fencing | PASS | Real control and metadata tests rejected stale renew, read, write, and release behavior. |
| 4. Takeover | PASS | Concurrent contenders, monotonic expiry observation, generation advance, owner kill, and live takeover ran. |
| 5. Provision crashes | PASS | Preparation/finalization crash cuts and external admission retry converged. |
| 6. Serve crashes | PASS | The approved live matrix retained an `Activating` owner kill, exact successor fence advancement, post-commit owner loss and read-back, stale raw metadata rejection, full-control-plane renewal failure with local fail-close, and recovery takeover mutation. |
| 7. Seed discovery | PASS | One clean-head live bundle retained multiple seeds, failed-first fallback, A-to-B refresh, stale discovery and owner hints, same-generation endpoint drift, immutable identity drift, and final mutation/read through B. |
| 8. Lifecycle | PASS | One exact-candidate live bundle retained public publication, replacement, snapshot renew/retire, restore/read, normal revision GC, one successful lost `DELETE` acknowledgement with quarantine, zero-consumer commit retirement, and one continuous serving fence. |
| 9. Limits | PASS | Real FDB diagnostics retained five logical/conservative/observed byte points through 2,800,000 bytes; a 3,200,000-byte object round-tripped while the complete retained FDB prefix contained no payload marker. |
| 10. Performance | NOT QUALIFIED | Concurrency is a correctness stress result, not controlled latency/throughput qualification. |

Therefore the accepted runtime direction remains:

```text
holt:///absolute/path
fdb:///absolute/fdb.cluster?prefix=nokv-prod
```

Holt is the standalone runtime. FDB is the distributed metadata authority and
has passed Gates 1 through 9 in this record, but its public serving status
remains **NOT QUALIFIED** until Gate 10 passes and Gates 8-10 are rerun on one
accepted source revision.
