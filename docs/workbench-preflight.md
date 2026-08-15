<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Workbench Deployment Preflight

This guide prepares any MCP-compatible Agent runtime to use the normal
`nokv mcp` stdio endpoint. There is no runtime-specific metadata format or
compatibility route.

## Required Inputs

Before registration, obtain:

- one 16-byte `RootId`;
- its persisted 16-byte `LogicalShardId` affinity;
- the current non-zero placement generation and owner epoch;
- the reachable workspace RPC owner address;
- an S3-compatible bucket, region, endpoint policy, and credentials;
- an absolute path to the exact `nokv` binary being registered.

The metadata owner must have opened a store containing only the exact
`nokv_workspace` schema and installed the matching active root fence. The
artifact backend must provide immutable create-if-absent, head, range read, and
delete semantics.

## Offline Gates

Run:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
python3 scripts/workbench/workbench_contract_test.py
git diff --check
```

The contract check proves only the exact 18 names and normalized input schemas.
It does not qualify persistence, object I/O, failover, restore, or latency.

## Live Contract Check

Start `nokv mcp` with the same root, route, owner, and object configuration that
the Agent runtime will use. Send `initialize`, then `tools/list`, and validate
the response with `workbench_contract.validate_tool_contract`.

For the complete live Workbench path, run
`scripts/workbench/live_workbench.py`. It calls `nokv provision`,
starts `nokv serve` with explicit metadata create/reopen intent, starts
`nokv ... --workbench-root /agents/{agent}/wb mcp`, exercises all 18 tools, and
retains exact requests/responses plus materialize/collect evidence. Run
`--dry-run` first to inspect the redacted command and normalized-input plan.
In the local-WAL profile, `reopen` qualifies only a restart of the same
exclusive Holt namespace. Admission validates Holt WAL recovery, the exact
workspace schema and shard identity, the complete recovery-outbox chain, and
the local/control owner-epoch relation before consuming a new epoch. An
unfinished `Recovering` epoch is rebound rather than skipped. This remains
restart evidence, not copied-directory, cross-host, shared-log, or rolling
upgrade failover evidence.
The release-level epoch proof is the real-etcd fence-before/fence-after
`SIGKILL` runner in
[`scripts/workbench/local_wal_recovery_gate.py`](../scripts/workbench/local_wal_recovery_gate.py);
a normal reopen alone does not cover interrupted `Recovering` retries.
The selected `--workbench-root` is durable presentation configuration because
canonical v1 manifests contain its projected paths. Keep it identical across
restart/replay; it never replaces `RootId` as the storage or routing identity.
Before reading MCP stdin or advertising tools, the flat CLI performs the typed
workspace RPC preflight for every capability required by this 18-tool profile;
a missing capability or route mismatch stops startup.

Registration must stop if:

- the tool set is not exactly 18 tools;
- any normalized input schema differs;
- the root route is stale or belongs to another logical shard;
- the metadata schema marker differs from `nokv_workspace`;
- the object backend cannot guarantee immutable creation;
- a write/read/snapshot/restore probe returns a placeholder or unsupported
  success.

## Qualification

Report each applicable gate in
[Workspace Acceptance](./development/workspace-acceptance.md) as `PASS`, `FAIL`,
or `NOT QUALIFIED`. In particular, a production handoff needs independent
evidence for:

- metadata reopen and exact request replay;
- stale-owner rejection and owner failover;
- immutable object upload, range verification, and ambiguous-provider errors;
- hidden-then-atomic restore;
- revision retention and GC fencing;
- golden Workbench results and errors, not only input schemas.

Keep raw commands, environment profile, logs, and result artifacts with the
qualification report.

Current source-level/unit evidence does not qualify a production handoff. In
addition to the unavailable shared recovery path, live qualification must still
prove or implement all of the following:

- provider-attested upload completion across the direct SDK data path, not a
  forgeable client assertion alone;
- production adoption or bounded abort/cleanup for interrupted commit and
  restore operations, including release of their history/revision holds;
- a tracked resolution for late direct PUT completion after publication abort;
- reconciliation that drives ambiguous object deletion out of quarantine;
- destructive provider operations fenced against control-plane lease transfer,
  not only a preceding shard-local owner check.

Until those rows have executable fault-injection evidence they are `NOT
QUALIFIED`, even when the exact 18-tool contract and local happy path pass.
