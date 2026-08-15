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
- `start_rustfs.sh` starts the optional local S3-compatible artifact backend.

Build and validate the product directly:

```bash
cargo build -p nokv --bin nokv
cargo test --workspace
python3 scripts/workbench/workbench_contract_test.py
python3 scripts/workbench/live_workbench_test.py
python3 scripts/workbench/local_wal_recovery_gate_test.py
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
  --evidence-dir target/local-wal-recovery-gate/evidence/run-01
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

This gate isolates control/local-WAL recovery and intentionally performs no
object I/O. RustFS/S3 publication and reads remain the responsibility of
`live_workbench.py`. Missing local etcd dependencies report `NOT QUALIFIED`;
an invariant violation reports `FAIL`.
