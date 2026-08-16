<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Workbench Validation

These assets validate the same 18-tool Workbench surface exported by
`crates/nokv-agent` and served by `nokv mcp`. They do not define a
runtime-specific metadata layout, capability alias, migration helper, or
filesystem frontend.

The checked-in integration assets are deliberately small:

- `workbench_contract.py` verifies an MCP `tools/list` response against the
  exact Rust-owned schema at
  `crates/nokv-agent/workbench_contract_schema.json`.
- `workbench_contract_test.py` tests normalization and exact surface matching.
- `live_workbench.py` provisions one root, starts one explicit metadata
  owner and the flat `nokv mcp` command, then records a real
  scientific reconstruction workflow through all 18 tools.
- `live_workbench_test.py` checks exact coverage/order, flat commands,
  secret redaction, dry-run evidence, and fail-closed qualification.
- `local_wal_recovery_gate.py` starts an isolated real etcd process and proves
  that a killed `Recovering(2)` owner is retried at epoch 2 both before and
  after the local Holt fence advances.
- `local_wal_recovery_gate_test.py` freezes the gate's crash-stage and terminal
  evidence contract. Its fault process is the bench-owned
  `nokv-local-wal-recovery-fault` binary, not a production CLI test hook.
- `object_namespace_recovery_gate.py` owns an isolated real etcd member and a
  digest-pinned RustFS container. It records deterministic PhyMat evidence,
  kills and reopens the owner, rejects a healthy wrong prefix, and proves the
  retryable outage/recovery contract over one long-lived MCP process.
- `object_namespace_recovery_gate_test.py` freezes those terminal assertions
  and the independent GitHub Actions evidence-upload contract.
- `fork_restore_recovery_gate.py` qualifies path-native, whole-Workbench
  fork-to-restore against isolated real etcd and digest-pinned RustFS. It
  injects response loss, kills and reopens the owner, races exact concurrent
  forks, chains a nested fork, retires the source snapshot, and inventories
  immutable objects to prove zero-copy reuse.
- `fork_restore_recovery_gate_test.py` freezes the fault classifier, 1 GiB
  profile, terminal evidence, and independent GitHub Actions job contract.
- `start_rustfs.sh` starts the optional local S3-compatible artifact backend
  with a digest-pinned image and bounded AWS CLI readiness attempts. It uses a
  Docker-managed volume by default so RustFS's non-root UID owns `/data` on
  Linux and macOS alike. `NOKV_WORKBENCH_RUSTFS_DATA_DIR` opts into a host bind
  mount; that directory must be writable by UID/GID `10001:10001`.

Build and validate the product directly:

```bash
cargo build -p nokv --bin nokv
cargo test --workspace
python3 scripts/workbench/workbench_contract_test.py
python3 scripts/workbench/live_workbench_test.py
python3 scripts/workbench/local_wal_recovery_gate_test.py
python3 scripts/workbench/object_namespace_recovery_gate_test.py
python3 scripts/workbench/fork_restore_recovery_gate_test.py
```

Register the built binary in any MCP-compatible Agent runtime as a stdio MCP
command:

```text
/absolute/path/to/nokv <root, route, object, and workbench options> mcp
```

The deployment must provide one persisted `RootId` placement, its
`LogicalShardId`, current placement generation and owner epoch, a reachable
metadata owner, and S3-compatible artifact credentials. Unknown or mixed
metadata schemas are rejected; the sole marker is `nokv_workspace`.

## Live Workbench evidence

Dry-run validates the redacted command graph and exact 18-tool coverage without
claiming that any dependency ran:

```bash
python3 scripts/workbench/live_workbench.py \
  --dry-run \
  --evidence-dir target/workbench-live/evidence/dry-run
```

A live run consumes already-running etcd and S3-compatible services.
Credentials may be supplied with `NOKV_LIVE_S3_ACCESS_KEY_ID` and
`NOKV_LIVE_S3_SECRET_ACCESS_KEY`; evidence hashes and redacts secrets.

```bash
python3 scripts/workbench/live_workbench.py \
  --build \
  --root-id 11111111111111111111111111111111 \
  --logical-shard-id 22222222222222222222222222222222 \
  --etcd-endpoint http://127.0.0.1:2379 \
  --object-endpoint http://127.0.0.1:9000 \
  --object-bucket nokv-workbench-live \
  --metadata-mode create \
  --metadata-dir target/workbench-live/metadata/live-01 \
  --evidence-dir target/workbench-live/evidence/live-01
```

`--metadata-mode reopen` restarts the same explicit local-WAL namespace after
the prior owner session is gone. Startup exclusively opens Holt, replays and
validates its WAL, checks the workspace schema, shard identity, recovery chain,
and local/control owner epochs, then either acquires the next epoch or resumes
an interrupted `Recovering` epoch. It does not qualify another directory, a
copied/rolled-back namespace, cross-host failover, or a non-empty shared
checkpoint/log frontier. The harness always calls `nokv provision`, starts
`nokv serve` with exactly one of `--metadata-create` or `--metadata-reopen`,
and starts `nokv ... --workbench-root /agents/{agent}/wb mcp`. The scientific
step uses the explicit `materialize` and `collect` commands; its local sandbox
is not a NoKV namespace. Keep the configured Workbench root stable across
restarts because canonical v1 manifest presentation paths are replay-bound;
`RootId`, not this display root, remains the storage/routing identity.

The deterministic evidence directory contains `plan.json`, exact paired
requests/responses in `mcp-transcript.jsonl`, `processes.jsonl` and process
logs, build/config facts in `environment.json`, validated schema evidence in
`contract.json`, and explicit statuses in `qualification.json`.

Exit status `3` means a required live dependency is absent and the workflow is
`NOT QUALIFIED`, never a pass. Exit status `2` means a configured live boundary
violated an assertion. A successful live run marks the 18-tool workflow
`PASS`, while overall Gate 0 remains `NOT QUALIFIED`: this bounded harness does
not wait for the minimum one-day snapshot lease to expire and reach `reaped`.

Live durability, failover, object-provider, and end-to-end Workbench
qualification must be reported separately according to
`docs/development/workspace-acceptance.md`. Unit tests or schema checks are not
substitutes for those gates.

## Local-WAL epoch recovery evidence

The release recovery gate requires local `etcd` and `etcdctl` binaries. It
starts a fresh single-member etcd under the evidence directory, builds the real
`nokv` CLI plus the bench fault driver, and retains every control record,
process log, crash-boundary record, binary digest, and terminal result:

```bash
python3 scripts/workbench/local_wal_recovery_gate.py \
  --build \
  --target-dir target/local-wal-recovery-gate/build \
  --evidence-dir target/local-wal-recovery-gate/evidence/run-01 \
  --object-endpoint http://127.0.0.1:9000 \
  --object-bucket nokv-local-wal-recovery-gate
```

The two mandatory boundaries are:

1. etcd is `Recovering(2)` while the local Holt fence is still epoch 1;
2. etcd is `Recovering(2)` after the local Holt fence has committed epoch 2.

The `nokv-workspace` GitHub Actions check runs both boundaries against a
checksum-pinned real etcd release. It uploads the complete evidence directory
for seven days even when the gate fails, so a failed retry retains its control
records, process logs, and terminal `qualification.json` instead of exposing
only a transient CI error line.

For each boundary the driver holds a live lease and the same exclusive Holt
path until the gate sends `SIGKILL`. Retry begins only after the lease-attached
session key disappears. The real CLI must reopen the path, preserve the
metadata probe, and publish `Serving(2)`; observing epoch 3 is a hard `FAIL`.

This gate isolates control/local-WAL recovery and performs only the namespace
marker admission required by the production CLI. RustFS/S3 payload publication,
wrong-prefix isolation, outage retryability, and PhyMat recovery remain the
responsibility of the independent gate below. Missing dependencies report
`NOT QUALIFIED`; an invariant violation reports `FAIL`.

## Object-namespace and PhyMat recovery evidence

The independent live gate requires local `etcd`, `etcdctl`, Docker, and the AWS
CLI. It starts and cleans up its own digest-pinned RustFS container:

```bash
python3 scripts/workbench/object_namespace_recovery_gate.py \
  --build \
  --target-dir target/object-namespace-recovery/build \
  --evidence-dir target/object-namespace-recovery/evidence/run-01
```

The deterministic PhyMat fixture represents structure intake, ML-potential
screening, a converged DFT relaxation, thermodynamic evidence, and their
provenance digest. It is a release-gate workload, not a scientific validation
of the fixture values. The gate requires all of the following:

1. a `SIGKILL`ed owner loses its lease and the same Holt directory reopens at
   exactly the next stable epoch;
2. structure and relaxation bytes remain exact after the restart;
3. a second healthy RustFS prefix has its own marker and is rejected before
   changing workspace metadata, the first root's control record, or payloads;
4. a live MCP read during a RustFS outage exhausts the configured attempts as
   redacted `ObjectUnavailable` with `retryable: true`;
5. the same logical read succeeds with exact PhyMat evidence after RustFS is
   restarted.

GitHub Actions runs this as the separate `object-namespace-recovery` job and
uploads its complete evidence directory even on failure.

## Path-native fork-to-restore recovery evidence

The fork gate exercises the supported whole-Workbench restore contract. It
does not restore an inode/dentry subtree and does not mutate an existing
destination in place. The destination must be absent, receives a fresh hidden
incarnation, and becomes visible only after its path closure and restore
manifest are complete.

The live gate requires local `etcd`, `etcdctl`, Docker, and the AWS CLI. Its
default 64-file profile crosses the restore copy-batch boundary in CI;
`--require-full` fixes the profile at 256 files of 4 MiB, exactly 1 GiB of
logical artifact data:

```bash
python3 scripts/workbench/fork_restore_recovery_gate.py \
  --build \
  --target-dir target/fork-restore-recovery/build \
  --fixture-files 256 \
  --require-full \
  --evidence-dir target/fork-restore-recovery/evidence/run-01
```

The gate drops the real owner response after `prepare_restore` and again after
`finalize_restore`, sends `SIGKILL`, waits for the etcd session to disappear,
and retries against the same Holt directory at the next owner epoch. It also
requires sixteen identical concurrent requests to converge to one operation,
chains a fork from a recommitted fork, retires and diverges the source, and
proves every fork adds only its restore-manifest object while overwriting no
source payload object. GitHub Actions runs the bounded profile as the separate
`fork-restore-recovery` job and retains the complete evidence directory even
on failure.
