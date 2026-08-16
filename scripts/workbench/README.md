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

- `pre423_contract_ledger.json` is the 47-item machine-readable behavior and
  migration-decision ledger for the pre-#423 recovery effort;
  `pre423_contract_ledger.py` and its unit test keep stable ids, class and
  disposition policy, evidence, ownership, and required gates complete.
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
- `restore_composition_gate.py` carries the exact externally observable
  pre-#423 restore-composition oracle onto the current RootId/path-native
  architecture: committed A to snapshot A to restored B, dirty B mutation
  without recommit, snapshot B, committed C, and C retention after snapshot B
  retires. `restore_composition_gate_test.py` freezes its command graph,
  evidence schema, exclusions, and fail-closed qualification states.
- `start_rustfs.sh` starts the optional local S3-compatible artifact backend
  with a digest-pinned image and bounded AWS CLI readiness attempts. It uses a
  Docker-managed volume by default so RustFS's non-root UID owns `/data` on
  Linux and macOS alike. `NOKV_WORKBENCH_RUSTFS_DATA_DIR` opts into a host bind
  mount; that directory must be writable by UID/GID `10001:10001`.

Build and validate the product directly:

```bash
cargo build -p nokv --bin nokv
cargo test --workspace
python3 scripts/workbench/pre423_contract_ledger.py
python3 scripts/workbench/pre423_contract_ledger_test.py
python3 scripts/workbench/workbench_contract_test.py
python3 scripts/workbench/live_workbench_test.py
python3 scripts/workbench/local_wal_recovery_gate_test.py
python3 scripts/workbench/object_namespace_recovery_gate_test.py
python3 scripts/workbench/restore_composition_gate_test.py
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
`NOKV_LIVE_S3_SECRET_ACCESS_KEY`; evidence records only their presence and
redacts secret values without retaining a digest verifier.

```bash
python3 scripts/workbench/live_workbench.py \
  --build \
  --root-id 11111111111111111111111111111111 \
  --agent-id 44444444444444444444444444444444 \
  --agent-name research-agent \
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
and starts `nokv ... --agent-id {stable-id} --workbench-root
/agents/{agent-name}/wb mcp`. `--agent-id` is the durable Root admission
identity and must not be derived from the presentation name or path;
`--agent-name` only selects the human-facing projection. The scientific
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

## Restore-composition evidence

The composition gate uses a real flat `nokv mcp` process for the stable
18-tool Workbench surface. Atomic path mutations use the separate,
Workbench-scoped custom CLI and never absolute paths:

```text
nokv [root, Agent, route, and object options] workspace-path rename \
  WORKBENCH SECTION SOURCE DESTINATION \
  --expected-generation N --request-id HEX32
nokv [root, Agent, route, and object options] workspace-path remove \
  WORKBENCH SECTION PATH \
  --expected-generation N --request-id HEX32
```

The gate derives stable, non-secret request ids from its seed and normalized
mutation fields. It does not add rename or removal to the 18 MCP tools and
does not recreate FUSE, POSIX, Yanex, inode, dentry, or physical-layout
behavior.

Freeze the exact command and assertion graph without claiming live coverage:

```bash
python3 scripts/workbench/restore_composition_gate.py \
  --dry-run \
  --evidence-dir target/restore-composition-gate/evidence/dry-run
```

Run the live gate with isolated real etcd and digest-pinned RustFS:

```bash
python3 scripts/workbench/restore_composition_gate.py \
  --build \
  --target-dir target/restore-composition-gate/build \
  --evidence-dir target/restore-composition-gate/evidence/live-01
```

The required main chain is:

1. commit A at generation 1 and mint snapshot A;
2. mutate live A, restore frozen A into immediately committed B, and prove A
   and B are independent;
3. atomically rename one B path, remove another, and publish another without a
   B recommit;
4. mint snapshot B, restore it into immediately committed C at generation 1,
   and prove the old/deleted paths stay absent and all surviving bytes match;
5. mutate B again, retire snapshot B, and prove C remains independent and
   readable through its child retention;
6. replay the same C restore after retirement and require the unique terminal
   receipt and destination commit identity.

Each restore must add exactly two destination-owned manifest objects and no
payload copy. A clean A-to-B restore keeps the source content digest while
changing destination commit identity. The dirty B-to-C restore must change
both the effective content digest and destination commit identity.

`mcp-transcript.jsonl` retains exact requests and responses;
`processes.jsonl` retains redacted commands including stable mutation request
ids; `environment.json` binds the binary SHA, source revision and dirty state,
etcd and provider versions, RootId, AgentId, logical shard, and owner epoch.
Exit status `3` is `NOT QUALIFIED`, including a missing `workspace-path`
capability. Exit status `2` is a contract `FAIL`.

The deterministic object-first, dual-manifest-published, pre-Complete crash
point is not currently injectable through a public boundary. The gate records
that phase as `NOT QUALIFIED`; a timed sleep followed by `SIGKILL` is not
accepted as evidence. The 8/16-caller exact-replay matrix and full release/GC
drain remain later phases. The `workbench-contract` job compiles and unit-tests
the gate, while the required `nokv-workspace` job runs the isolated live
composition against pinned RustFS and uploads its complete evidence. This is a
runtime contract gate, not the background Docker Image build.

### Pre-#423 capability mapping

The old acceptance script also depended on surfaces beyond the core
composition chain. They are classified explicitly rather than rediscovered
one failure at a time:

- `put-artifact` and `cat` outcomes map to current `collect`, `materialize`, and
  MCP byte reads; the old command spellings are not required.
- the old profiled MCP launch maps to the current flat `nokv mcp` command; the
  exact 18-tool schema remains authoritative.
- old one-manifest restore accounting is superseded by two destination-owned
  manifests: `run_manifest.json` and `restore_manifest.json`.
- numeric mounts, projected source/destination roots, inode/dentry identities,
  and internal restore-operation id formulas are implementation details and
  are deliberately absent from public evidence.
- root/subtree rename-replace, recursive Workbench deletion, and directory
  removal are not needed by the A-to-B-to-C oracle and remain separate API
  decisions; the scoped artifact rename/remove commands above do not imply
  those broader semantics.
- the old `/gc`, `/stats`, and `/fsck` administration endpoints and its
  environment-driven restore crash barriers have no current public
  replacement. Provider recovery, GC drain, and exact crash qualification
  therefore remain separate gates, never inferred from this no-fault chain.
- an optional second Agent-runtime entry must consume the same flat MCP launch
  and produce the same transcript contract; it cannot weaken or substitute for
  the native gate.
