<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Testing & Validation Matrix

This document inventories NoKV's automated coverage and provides guidance for extending tests. It aligns module-level unit tests, integration suites, and benchmarking harnesses with the architectural features described elsewhere.

---

## 1. Quick Commands

```bash
# All unit + integration tests, matching CI's package-serial default
make test

# Package dependency boundary guards (run as part of `make lint`)
make lint

# Same full sweep with local module caches
GOCACHE=$PWD/.gocache GOMODCACHE=$PWD/.gomodcache go test -p 1 ./...

# Focused distributed transaction suite
go test ./txn/percolator/... ./raftstore/client/... -run 'Test.*(Commit|Prewrite|TwoPhaseCommit)'

# Focused distributed migration / membership / restart suite
go test ./raftstore/integration -count=1

# Core chaos gate for GC + raftstore + fsmeta runtime stability
go test ./raftstore/mvcc ./raftstore/store ./raftstore/server ./raftstore/integration ./fsmeta/exec ./fsmeta/integration -count=1

# Seeded fsmeta contract/model smoke
make test-contract-smoke

# Same generated fsmeta contract scripts through raftstore/client and txn/percolator
make test-raftstore-contract-smoke

# Bounded generated model/fault schedules for PR CI
make test-model-smoke

# Explicit crash-window matrix around 2PC and raft Ready apply/send
make test-crash-matrix-smoke

# Seeded deterministic fault simulation over a real split-region cluster
make test-deterministic-simulation-smoke

# Bounded concurrent fsmeta history checks for PR CI
make test-history-smoke

# Curated distributed correctness smoke used by CI before the full sweep
make test-correctness-smoke

# Longer seeded correctness/failpoint matrix for nightly CI
make test-correctness-nightly

# Docker-compose black-box history checker with service restarts / crashes
make test-docker-chaos

# Short soak smoke; use NOKV_SOAK_DURATION=24h or 72h for release hardening
make test-soak-smoke

# Docker Compose fsmeta benchmark over all service workloads
make fsmeta-bench

# Crash recovery scenarios
RECOVERY_TRACE_METRICS=1 \
go test ./... -run 'TestRecovery(FailsOnMissingSST|FailsOnCorruptSST|ManifestRewriteCrash|SlowFollowerSnapshotBacklog|SnapshotExportRoundTrip|WALReplayRestoresData)' -count=1 -v

# Protobuf schema hygiene
make proto-check

# gRPC transport chaos tests + watchdog metrics
CHAOS_TRACE_METRICS=1 \
go test -run 'TestGRPCTransport(HandlesPartition|MetricsWatchdog|MetricsBlockedPeers)' -count=1 -v ./raftstore/transport

# Sample Coordinator service for shared TSO / routing in distributed tests
go run ./cmd/nokv coordinator --addr 127.0.0.1:2379 --id-start 1 --ts-start 100 --workdir ./artifacts/coordinator

# Local three-node cluster (includes catalog bootstrap + Coordinator)
./scripts/dev/cluster.sh --config ./raft_config.example.json
# Tear down with Ctrl+C

# Docker-compose sandbox (3 nodes + Coordinator)
docker compose up -d
docker compose down -v

# Build RocksDB locally (installs into ./third_party/rocksdb/dist by default)
./scripts/build_rocksdb.sh
# YCSB baseline (records=1e6, ops=1e6, warmup=1e5, conc=16)
./scripts/run_benchmarks.sh
# YCSB with RocksDB (requires CGO, `benchmark_rocksdb`, and the RocksDB build above)
LD_LIBRARY_PATH="$(pwd)/third_party/rocksdb/dist/lib:${LD_LIBRARY_PATH}" \
CGO_CFLAGS="-I$(pwd)/third_party/rocksdb/dist/include" \
CGO_LDFLAGS="-L$(pwd)/third_party/rocksdb/dist/lib -lrocksdb -lz -lbz2 -lsnappy -lzstd -llz4" \
YCSB_ENGINES="nokv,badger,rocksdb" ./scripts/run_benchmarks.sh
# One-click script (auto-detect RocksDB, supports `YCSB_*` env vars to override defaults)
./scripts/run_benchmarks.sh
# Quick smoke run (smaller dataset)
NOKV_RUN_BENCHMARKS=1 YCSB_RECORDS=10000 YCSB_OPS=50000 YCSB_WARM_OPS=0 \
./scripts/run_benchmarks.sh -ycsb_workloads=A -ycsb_engines=nokv
```

> Tip: Pin `GOCACHE`/`GOMODCACHE` in CI to keep build artefacts local and avoid permission issues.

---

## 2. Module Coverage Overview

| Module | Tests | Coverage Highlights | Gaps / Next Steps |
| --- | --- | --- | --- |
| WAL | `engine/wal/manager_test.go` | Segment rotation, sync semantics, replay tolerance for truncation, directory bootstrap. | Add IO fault injection, concurrent append stress. |
| LSM / Flush / Compaction | `engine/lsm/lsm_test.go`, `engine/lsm/picker_test.go`, `engine/lsm/planner_test.go`, `engine/lsm/compaction_test.go`, `engine/lsm/flush_runtime_test.go` | Memtable correctness, iterator merging, flush pipeline metrics, compaction scheduling. | Extend backpressure assertions and workload-shape coverage. |
| Manifest | `engine/manifest/manager_test.go`, `engine/lsm/manifest_test.go` | CURRENT swap safety, rewrite crash handling, SST metadata persistence. | Simulate partial edit corruption, column family extensions. |
| Percolator / Distributed Txn | `txn/percolator/*_test.go`, `raftstore/client/client_test.go`, `stats_test.go` | Prewrite/Commit/ResolveLock flows, 2PC retries, timestamp-driven MVCC behaviour, generated transaction-history serializability, rollback marker visibility, lock cleanup, explicit primary-committed / primary-rollback crash windows, restart idempotency, and metrics accounting. | Expand generated recovery schedules for long-transaction heartbeat churn. |
| FS Metadata Contract | `fsmeta/contract/*_test.go`, `fsmeta/exec/runner_test.go`, `fsmeta/integration/contract_test.go`, `fsmeta/integration/history_contract_test.go` | Seeded model-based coverage for create/update/lookup/readdir/snapshot/rename/link/unlink/remove/remove-directory/session expiry against both the executor API and a real split-region raftstore-backed runner. Bounded concurrent history checks linearize overlapped fsmeta operations against the same reference model. | Add fault-injected retry schedules around the raftstore-backed contract. |
| DB Integration | `local/db_test.go`, `local/db_bench_test.go` | End-to-end writes, recovery, and throttle behaviour. | Combine compaction stress and multi-DB interference. |
| CLI & Stats | `cmd/nokv/main_test.go`, `stats_test.go` | Golden JSON output, stats snapshot correctness, hot key ranking. | CLI error handling, expvar HTTP integration tests. |
| Scripts & Tooling | `cmd/nokv-config/main_test.go`, `cmd/nokv/serve_test.go` | `nokv-config` JSON/simple formats, catalog bootstrap CLI, serve bootstrap behavior. | Add direct shell-script golden tests (currently not present) and failure-path diagnostics for `cluster.sh`. |
| Distributed Migration & Membership | `raftstore/integration/*_test.go`, `raftstore/migrate/*_test.go`, `raftstore/admin/service_test.go` | Standalone -> seeded -> cluster flow, snapshot install, add/remove peer, leader transfer, restart/dehost recovery, Coordinator outage after startup, quorum-loss context propagation, multi-region 2PC deadline propagation, repeated link flap during membership changes, partitioned follower catch-up, deterministic split-region simulation schedules, and snapshot-install interruption before publish. | Keep expanding publish-boundary coverage and larger fault matrices around runtime/transport interleavings. |
| Benchmark | `benchmark/ycsb/ycsb_test.go`, `benchmark/ycsb/ycsb_runner.go` | YCSB throughput/latency comparisons across engines (A-F) with detailed percentile + operation mix reporting. | Automate multi-node deployments and add longer-running, multi-GB stability baselines. |
| Architecture Boundaries | `tools/lint/analyzers/importboundary/` (run via `make lint`) | CI guard for fsmeta executor neutrality, local/distributed separation, meta-root/coordinator separation, removed package paths, and the single raftstore-backed fsmeta runtime adapter. | Add new rules whenever a module boundary becomes a correctness contract. |

---

## 3. System Scenarios

| Scenario | Coverage | Focus |
| --- | --- | --- |
| Crash recovery | `db_test.go` | WAL replay, fail-fast on missing/corrupt SST (manifest preserved for investigation), manifest rewrite safety. |
| WAL pointer desync | `raftstore/raftlog/wal_storage_test.go::TestWALStorageDetectsTruncatedSegment` | Detects store-local raft pointer offsets beyond truncated WAL tails to avoid silent corruption. |
| Distributed transaction contention | `raftstore/client/client_test.go::TestClientTwoPhaseCommitAndGet`, `txn/percolator/*_test.go` | Lock conflicts, retries, and 2PC sequencing under region routing. |
| Iterator consistency | `engine/lsm/iterator_test.go` | Snapshot visibility, merging iterators across levels and memtables. |
| Throttling / backpressure | `engine/lsm/compaction_test.go`, `db_test.go::TestWriteThrottle` | L0 backlog triggers, flush queue growth, metrics observation. |
| Distributed NoKV client | `raftstore/client/client_test.go::TestClientTwoPhaseCommitAndGet`, `raftstore/transport/grpc_transport_test.go::TestGRPCTransportManualTicksDriveElection` | Region-aware routing, NotLeader retries, manual tick-driven elections, cross-region 2PC sequencing. |
| Migration & membership orchestration | `raftstore/integration/migration_flow_test.go`, `raftstore/integration/restart_recovery_test.go`, `raftstore/integration/coordinator_degraded_test.go`, `raftstore/integration/snapshot_interruption_test.go`, `raftstore/integration/context_propagation_test.go`, `raftstore/integration/transport_chaos_test.go` | Seed bootstrap, multi-peer rollout, leader transfer, peer removal, restarted follower recovery, removed-peer dehost after restart, Coordinator outage after startup, quorum-loss read/write timeouts, split-region 2PC deadline propagation, repeated link flap during membership changes, partitioned follower catch-up, transfer-leader retry after partition recovery, and snapshot-install interruption before publish. |
| Performance regression | `benchmark` package | Compare NoKV vs Badger/Pebble by default (RocksDB optional), produce human-readable reports under `benchmark/benchmark_results`. |

---

## 4. Observability in Tests

- **RECOVERY_METRIC logs** – produced when `RECOVERY_TRACE_METRICS=1`; helpful when triaging targeted recovery suites and CI failures.
- **TRANSPORT_METRIC logs** – emitted by transport chaos tests when `CHAOS_TRACE_METRICS=1`, capturing gRPC watchdog counters during network partitions and retries.
- **Stats snapshots** – `stats_test.go` verifies JSON structure so CLI output remains backwards compatible.
- **Benchmark artefacts** – stored under `benchmark/results/` for shared suites and under suite-local result directories where applicable, for example `benchmark/results/ycsb/`.

---

## 5. Extending Coverage

1. **Property-based testing** – integrate `testing/quick` or third-party generators to randomise distributed 2PC sequences (prewrite/commit/rollback ordering).
2. **Stress harness** – add a Go-based stress driver to run mixed read/write workloads for hours, capturing metrics akin to RocksDB's `db_stress` tool.
3. **Distributed readiness** – strengthen raftstore fault-injection and long-run tests (leader transfer, transport chaos, snapshot catch-up) with reproducible CI artifacts.
4. **CLI smoke tests** – simulate corrupted directories to ensure CLI emits actionable errors.

## 6. Distributed Test Layers

- **Protocol unit tests**: package-local tests under `raftstore/peer`, `raftstore/store`, `raftstore/admin`, `raftstore/snapshot`, and `raftstore/migrate` validate one protocol surface at a time.
- **Node-local integration tests**: store/admin tests verify snapshot install, membership application, and region runtime publication without booting a full cluster.
- **Multi-node deterministic data-plane integration tests**: `raftstore/integration` uses `raftstore/testcluster` to boot real stores, wire transports, and drive migration/member flows against live runtimes.
- **Multi-node deterministic control-plane integration tests**: `coordinator/integration/*_test.go` uses `coordinator/testcluster` to boot `3 coordinator + replicated meta`, exercise rooted watch/reload propagation, follower write rejection, allocator-fence/remove-region propagation, and control-plane read staleness without mixing those cases into store/data-plane tests.
- **Model/contract tests**: `txn/percolator/txn_model_test.go` generates transaction histories and checks timestamp-order serializability against the real protocol API. `fsmeta/contract` generates deterministic operation scripts and compares the fsmeta executor against a reference model before the suite reaches raftstore/coordinator/meta fault matrices. `fsmeta/contract/history_test.go` runs bounded concurrent batches and searches for a valid linearization of each observed history. `fsmeta/integration/contract_test.go` and `fsmeta/integration/history_contract_test.go` reuse those scripts against the real raftstore/client plus txn/percolator path on a split-region test cluster.
- **Generated data-plane fault tests**: `raftstore/integration/twopc_fault_model_test.go` runs bounded generated 2PC schedules across split regions, leader transfer, partial quorum failure, client retry, and lock resolution.
- **Generated control-plane model tests**: `coordinator/integration/root_model_test.go` runs rooted event schedules and verifies epoch deltas, snapshot replay/materialization, and follower watch catch-up.
- **Crash-consistency matrix tests**: `txn/percolator/crash_matrix_test.go` covers primary committed / secondary unresolved, primary rollback / secondary unresolved, and commit/rollback idempotency after restart. `raftstore/peer/peer_test.go::TestPeerFailpointAfterReadyAdvanceBeforeSendRecoversOnLaterTicks` covers the raft apply/advance/send boundary.
- **Deterministic simulation smoke**: `raftstore/integration/deterministic_simulation_test.go` uses seeded schedules over a real split-region cluster to replay commit, leader-transfer, partial-quorum rollback, delayed transport recovery, target-store restart, and stale lock-resolution pressure.
- **Restart and recovery suites**: `raftstore/integration/restart_recovery_test.go` covers restarted followers, removed-peer dehost persistence, and leader restart with subsequent membership changes.
- **Control-plane degradation and publish-boundary tests**: `raftstore/integration/coordinator_degraded_test.go` and `raftstore/integration/snapshot_interruption_test.go` cover live Coordinator outage after startup and failpoint-driven snapshot interruption before peer publication.
- **Black-box Docker chaos**: `scripts/chaos/docker_fsmeta_history.sh` boots the Docker HA stack and runs `cmd/nokv-fsmeta-history`, a bounded external fsmeta history checker, after coordinator/store/meta-root/fsmeta restarts or single-process kills.
- **Long soak**: `scripts/soak/fsmeta_soak.sh` runs `cmd/nokv-fsmeta-soak`, which repeatedly mixes namespace history checking with session, snapshot, watch, and cleanup probes. Use `NOKV_SOAK_DURATION=24h` or `72h` for release hardening; PR CI should keep this as a short manual smoke.

When adding new distributed tests:

- use `raftstore/testcluster` for store/data-plane behavior
- use `coordinator/testcluster` for control-plane / replicated-root behavior
- avoid embedding ad-hoc cluster bootstrap helpers into feature-specific test files

## 7. Distributed Fault Matrix

| Fault Class | Current Coverage | Primary Tests | Notes |
| --- | --- | --- | --- |
| Snapshot export/install failure | Covered | `raftstore/migrate/expand_test.go`, `raftstore/store/peer_lifecycle_test.go`, `raftstore/admin/service_test.go` | Covers leader export failure, target install failure, and corrupt payload rejection without partially hosted peers. |
| Membership wait timeouts | Covered | `raftstore/migrate/expand_test.go`, `raftstore/migrate/remove_peer_test.go`, `raftstore/migrate/transfer_leader_test.go` | Verifies timeout surfaces when leader metadata does not publish, target never hosts, peer removal never converges, or leader transfer stalls. |
| Follower restart after snapshot install | Covered | `raftstore/integration/restart_recovery_test.go::TestExpandedPeerRestartPreservesRegionAndData` | Ensures installed peer persists region metadata and data after restart. |
| Removed peer restart | Covered | `raftstore/integration/restart_recovery_test.go::TestRemovedPeerRestartDoesNotRehost` | Ensures dehosted peers do not come back after restart. |
| Leader restart with follow-up membership change | Covered | `raftstore/integration/restart_recovery_test.go::TestLeaderRestartStillAllowsMembershipChanges` | Exercises leadership churn before a later remove-peer operation. |
| Control-plane degraded / Coordinator unavailable | Covered | `coordinator/storecontrol/client_test.go`, `raftstore/store/command_ops_test.go::TestStoreProposeCommandSurvivesSchedulerUnavailable`, `raftstore/integration/coordinator_degraded_test.go::TestClusterSurvivesCoordinatorUnavailableAfterStartup` | Covers both local degraded store-control semantics and live multi-node Coordinator outage after route cache warmup; new cold-route misses still fail with `RouteUnavailable` as expected. |
| Scheduler queue overflow / dropped operations | Covered | `raftstore/store/scheduler_runtime_test.go::TestStoreSchedulerStatusTracksQueueDrop` | Validates local degraded status and dropped operation accounting. |
| Snapshot install interrupted before publish | Covered | `raftstore/integration/snapshot_interruption_test.go::TestExpandSnapshotInstallInterruptedBeforePublish`, `raftstore/store/peer_lifecycle_test.go::TestStoreInstallRegionSnapshotRejectsCorruptPayload` | Uses failpoint injection to verify target install aborts without leaving a hosted peer or polluted region metadata, then retries cleanly after restart. |
| Request cancel / deadline propagation | Covered | `raftstore/client/client_test.go::TestClientGetHonorsCanceledContextDuringRouteLookup`, `raftstore/client/client_test.go::TestClientGetHonorsCanceledContextDuringRPC`, `raftstore/client/client_test.go::TestClientPutHonorsCanceledContextDuringRouteLookup`, `raftstore/client/client_test.go::TestClientPutHonorsCanceledContextDuringRPC`, `raftstore/client/client_test.go::TestClientTwoPhaseCommitHonorsCanceledContextDuringMultiRegionRouteLookup`, `raftstore/client/client_test.go::TestClientTwoPhaseCommitHonorsCanceledContextDuringMultiRegionRPC`, `raftstore/client/client_test.go::TestClientResolveLocksHonorsCanceledContextDuringMultiRegionRPC`, `raftstore/integration/context_propagation_test.go::TestClientReadWriteHonorContextUnderQuorumLoss`, `raftstore/integration/context_propagation_test.go::TestClientTwoPhaseCommitHonorsContextAcrossSplitRegionsUnderPartialQuorumLoss` | Verifies read/write paths plus multi-region 2PC and resolve-lock flows preserve caller cancellation/deadlines through route lookup, RPC, and live split-region quorum loss instead of collapsing to generic retry exhaustion. |
| Transport partition / interleave recovery | Covered | `raftstore/transport/grpc_transport_test.go::TestGRPCTransportHandlesPartition`, `raftstore/transport/grpc_transport_test.go::TestGRPCTransportFailpointBeforeSendRPCRecoversAfterClear`, `raftstore/peer/peer_test.go::TestPeerFailpointAfterReadyAdvanceBeforeSendRecoversOnLaterTicks`, `raftstore/integration/transport_chaos_test.go::TestPartitionedFollowerCatchesUpAfterRecovery`, `raftstore/integration/transport_chaos_test.go::TestTransferLeaderRecoversAfterPartitionedTargetReturns`, `raftstore/integration/transport_chaos_test.go::TestRepeatedLinkFlapConvergesDuringMembershipChanges` | Covers low-level gRPC link blocking, send-boundary failpoints, Ready advance/send publication gaps, repeated link flaps during membership operations, and live cluster recovery after follower isolation/restart plus transfer-leader timeout/retry under transport partitions. |
| Split/merge restart safety | Covered | `raftstore/store/store_test.go::TestStoreRestartPreservesSplitMergeLocalMeta`, `raftstore/integration/split_merge_recovery_test.go::TestSplitMergeRestartSafetyAcrossStores` | Covers store-local recovery plus live multi-store split -> restart -> merge -> restart flow after making split/merge admin replay idempotent across restart. |

## 8. Core Chaos Gate Matrix

This matrix is the production-readiness gate for the metadata substrate. A row
is not "covered" unless it is backed by an automated test that can run in CI
without manual timing assumptions or known flaky behaviour.

| Subsystem | Chaos Condition | Required Invariant | Current Tests | Status |
| --- | --- | --- | --- | --- |
| MVCC GC planner | active snapshot floor, active lock floor, corrupt write payload, overlong version chain | planner stays read-only, clamps safe point per key, and fails closed on corrupt metadata | `raftstore/mvcc/plan_test.go`, `raftstore/mvcc/policy_test.go`, `raftstore/mvcc/txn_floor_test.go`, `raftstore/mvcc/planner_test.go` | Covered |
| MVCC maintenance worker | resolve-lock failure, apply failure, orphan-default cleanup, safepoint disabled, worker close during pass | each stage is bounded and independently observable; failed stages do not silently corrupt later stages | `raftstore/mvcc/worker_test.go`, `raftstore/mvcc/lock_resolver_test.go`, `raftstore/mvcc/orphan_default_test.go` | Covered |
| Replicated GC propose | not leader, partial region failure, post-split routing, invalid tombstone batch | destructive GC goes through raft, is region-atomic, and converges after partial multi-region success | `raftstore/store/command_ops_test.go`, `raftstore/kv/apply_test.go` | Covered |
| GC auto-start | node starts with maintenance interval and configured proposer paths | `nokv serve` / `server.Node` starts destructive maintenance only through replicated paths | `raftstore/server/node_test.go::TestNodeAutoStartsMVCCMaintenanceWorker` | Covered |
| Region catalog lookup | many regions and split-derived ranges | maintenance routing uses indexed region lookup, not O(region-count) scans on every tombstone | `raftstore/store/region_catalog_test.go::TestStoreRegionMetaByKeyUsesRangeIndex`, `raftstore/store/command_ops_test.go::TestStoreProposeMVCCMaintenanceRoutesAfterSplit` | Covered |
| Raftstore quorum loss | read/write and split-region 2PC under partition | caller context/deadline is preserved; recovery checks wait for cluster convergence instead of assuming instant raft recovery | `raftstore/integration/context_propagation_test.go` | Covered |
| Raftstore membership chaos | follower partition/restart, transfer-leader timeout, repeated link flap | partitioned peers catch up; failed leadership movement can be retried; link flaps converge without metadata corruption | `raftstore/integration/transport_chaos_test.go` | Covered |
| Raftstore restart / split-merge | split, merge, restart, removed peer restart, leader restart | local recovery metadata never rehosts removed peers and remains compatible with later membership changes | `raftstore/integration/restart_recovery_test.go`, `raftstore/integration/split_merge_recovery_test.go` | Covered |
| Snapshot install boundary | interrupted install before publish, corrupt import, retry after failure | failed snapshot install does not publish a hosted peer or polluted region metadata | `raftstore/integration/snapshot_interruption_test.go`, `raftstore/store/peer_lifecycle_test.go`, `raftstore/admin/service_test.go` | Covered |
| fsmeta watch | slow subscriber, expired cursor, reconnect/reconcile | watch replay is bounded; expired cursors force full-state reconcile instead of pretending exactly-once delivery | `fsmeta/exec/watch/router_test.go`, `fsmeta/client/reconcile_test.go`, `fsmeta/integration/e2e_test.go` | Covered |
| fsmeta snapshot retention | SnapshotSubtree publish/retire, read-version use, MVCC retention floor | active snapshot epochs retain required MVCC history until explicit retire | `fsmeta/exec/runner_test.go`, `fsmeta/server/service_test.go`, `fsmeta/integration/e2e_test.go`, `raftstore/mvcc/policy_test.go` | Covered |
| fsmeta session lifecycle | writer crash / heartbeat expiry / directory rejection / cleaner errors | stale writer sessions are expired by server time; directories cannot take file writer leases | `fsmeta/exec/runner_test.go`, `fsmeta/exec/session_cleaner_test.go` | Covered |
| fsmeta operation contract | mixed namespace mutations, snapshot reads, hardlinks, writer sessions, time advance, split-region raftstore routing, bounded concurrent histories | executor-visible results match the reference model; overlapped API calls admit a legal serial order; stale owner cleanup cannot delete a reused live session | `fsmeta/contract/*_test.go`, `fsmeta/integration/contract_test.go`, `fsmeta/integration/history_contract_test.go`, `fsmeta/exec/runner_test.go::TestExecutorExpireWriteSessionsDoesNotDeleteReusedLiveSession` | Covered |
| fsmeta namespace chaos | gateway restart, mixed mutations, subtree rename handoff | namespace operations remain transactionally visible and rooted handoff state converges after restart | `fsmeta/integration/namespace_chaos_test.go`, `fsmeta/integration/raftstore_runtime_test.go` | Covered |
| Percolator history model | generated Put/Delete/rollback transaction histories | reads match a timestamp-ordered serial history; committed and rolled-back records do not leave visible stale locks or hide older values | `txn/percolator/txn_model_test.go`, `txn/percolator/txn_test.go` | Covered |
| Percolator crash matrix | primary committed with secondary unresolved, primary rollback with secondary unresolved, restart between commit/rollback retries | secondary resolution follows primary authority; repeated commit/rollback after restart is idempotent and does not change visibility | `txn/percolator/crash_matrix_test.go` | Covered |
| Raft Ready advance/send boundary | fail after Ready has been handled and raft node advanced but before outbound messages are sent | the peer can process later Ready batches and still serve linearizable reads after the failpoint is cleared | `raftstore/peer/peer_test.go::TestPeerFailpointAfterReadyAdvanceBeforeSendRecoversOnLaterTicks` | Covered |
| Raftstore 2PC generated fault schedule | split-region writes, leader transfer, partial child-region quorum failure, client retry, rollback resolution | committed cross-region transactions remain visible and failed partial-quorum attempts do not leak committed writes | `raftstore/integration/twopc_fault_model_test.go` | Covered |
| Deterministic split-region simulation | generated commit, leader transfer, partial quorum, delayed link recovery, target-store restart, stale lock resolution | every seed is reproducible; committed model state remains visible after each injected transition | `raftstore/integration/deterministic_simulation_test.go` | Covered |
| Rooted control-plane generated model | store/mount/snapshot/quota/region event schedule with follower watch catch-up | cluster/membership epochs advance only on authority-changing events; replayed state equals live snapshot; follower watch converges | `coordinator/integration/root_model_test.go` | Covered |

CI policy for this matrix:

1. `go test ./...` must be green before merging production-path changes.
2. Any test listed above that fails under `-count=3` must be treated as a
   product bug or a test bug and fixed before adding new feature work.
3. New GC, raftstore, or fsmeta lifecycle code must add or update one row in
   this matrix in the same patch.
4. Chaos tests should wait for explicit convergence signals such as committed
   data, tombstoned MVCC entries, leader descriptors, or rooted events. They
   must not assert a fixed raft recovery latency unless latency is the feature
   being tested.

Nightly policy:

1. PR CI runs bounded smoke through `make test-correctness-smoke`, including
   `make test-history-smoke` and `make test-model-smoke`.
2. PR benchmark CI also runs the median `make fsmeta-bench` profile with
   `NOKV_FSMETA_BENCH_MODE=local`, so the gating benchmark measures the
   embedded fsmeta backend instead of spending runner budget on a full
   raftstore/root/coordinator Compose cluster. Each workload gets a fresh local
   workdir, its own CSV, and the script also emits a
   combined `_isolated.csv` summary so durable-barrier workloads do not absorb
   earlier visible-commit backlog. CSVs
   report both wall-clock `throughput_ops_sec` and per-operation
   `active_ops_per_sec` to keep watch and snapshot waits from hiding the real
   active operation cost. The median, long, and official fsmeta scales are read
   from `benchmark/fsmeta/profiles/official/workloads.yaml`, and each run emits
   a manifest with the selected scale and official workload provenance. The
   default CI path does not exercise Peras witness/install queues; use Compose
   mode explicitly for distributed durable-drain experiments.
3. Nightly CI runs `make test-correctness-nightly`, which raises model seeds
   and steps, replays raftstore/fsmeta contract/history schedules with longer
   bounds, repeats crash-matrix boundaries, replays deterministic split-region
   fault simulation, and repeats failpoint-heavy coordinator/meta/raftstore suites.
4. Scheduled fsmeta benchmark CI runs
   `NOKV_FSMETA_BENCH_MODE=local NOKV_FSMETA_PROFILE=long make fsmeta-bench`
   with the same isolated matrix shape, larger data volumes, and longer timeout,
   then uploads the same artifacts. Manual dispatch can select
   `NOKV_FSMETA_PROFILE=official`, but use it only when the runner budget can
   absorb the official-size profile.
5. A nightly-only failure should still be triaged as a correctness issue unless
   the failure is clearly in the test harness.

Black-box and soak policy:

1. `make test-docker-chaos` is the Jepsen-style Docker smoke: it validates
   external fsmeta histories against the reference model while one service at a
   time is restarted or killed between histories.
2. `.github/workflows/docker-chaos.yml` runs that Docker smoke on schedule and
   on demand.
3. `make test-soak-smoke` is intentionally short by default. Real release
   hardening should run `NOKV_SOAK_DURATION=24h ./scripts/soak/fsmeta_soak.sh`
   and a separate `72h` pass on a machine where Docker is allowed to keep state
   for the whole run.
4. `.github/workflows/soak-correctness.yml` provides a bounded hosted-runner
   soak. It is not a replacement for a 24h/72h self-hosted soak.

Next fault-matrix additions should focus on:

- more publish-boundary failpoints around snapshot install and migration init
- deeper transport/interleave chaos beyond partition + recovery, especially more concurrent membership combinations and repeated multi-link flaps
- unknown-result aware history checking for operations interrupted by gateway
  restart mid-RPC
- fake-clock driven long-transaction simulation that can force heartbeat,
  expiry, resolver, and GC interaction without wall-clock sleeps

Keep this matrix updated when adding new modules or scenarios so documentation and automation remain aligned.
