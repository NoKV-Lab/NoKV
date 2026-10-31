<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Live Deployment Preflight

This guide covers bringing up a live NoKV deployment and qualifying it before
a production handoff. Downstream systems provide skills over the native full
CLI; embedded callers use the Python SDK. Every surface uses the same workspace
format and compatibility rules. Without RPC TLS, only the native CLI checks the
RootId-to-AgentId binding and no surface authenticates callers; over RPC TLS,
the owner authenticates each client and authorizes each request by role
([SECURITY.md](../SECURITY.md)).

## Required Inputs

Before registration, obtain:

- one 16-byte `RootId`;
- one persisted 16-byte `AgentId` and its immutable control-plane binding to
  that RootId;
- the absolute Workbench root every Agent presents the root under, bound once
  by `nokv provision --workbench-root`;
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

## Default Deployment Shape

A serving shard is one `nokv serve` process over one exclusive Holt store. That
process is the metadata authority: it holds the owner lease, applies every
metadata command to its local Holt WAL, and acknowledges once that WAL is
durable. Nothing else has to be running for reads and writes to work.

Control is required, but for two narrow jobs: resolving which process owns a
root, and fencing that ownership with an epoch. It holds no metadata. Its only
step on the write path is the proof, before every acknowledgement, that the
owner session is still live.

On one host, `--control-dir PATH` keeps the control records in a local
directory, and the owner session is an exclusive file lock that the kernel
releases when the owner process exits, so no etcd process runs and the proof
is an in-process check. The directory must be on a local filesystem, and
every process that uses it runs as its owner: NoKV creates it private and
refuses one that group or other users can write or another user owns. Use
`--etcd-endpoint` when owners or clients span hosts or users; the proof is
then one linearizable etcd read per acknowledgement.

Publishing the recovery log to Control (`--recovery-publication shared`) is
an option, and an immature one: without checkpoint compaction the shared log
chain grows without bound and the shard stops serving after roughly a hundred
acknowledged publications. Leave it off unless you are qualifying the shared
recovery path itself. It is switched on implicitly by
`--metadata-recover-log`, which resumes a shard from a shared frontier.

The corresponding invariant for operators: back up the Holt directory. In the
default shape it is the only copy of the metadata.

## Live Contract Check

For the complete live Workbench path, run
`scripts/workbench/live_workbench.py`. It calls `nokv provision`,
starts `nokv serve` with explicit metadata create/reopen intent, exercises all
18 tools, and
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
`live_workbench.py` drives the 18 tools as `nokv workbench <tool>` processes
graded against [`docs/cli-contract.md`](cli-contract.md) and retains
`cli-transcript.jsonl`; NoKV ships no MCP transport.
The Workbench root is durable presentation configuration because canonical v1
manifests contain its projected paths. `nokv provision --workbench-root` binds
it to the root once, and Agent-facing commands read it from the control plane,
so every Agent of the root presents it the same way; it never replaces `RootId`
as the storage or routing identity.
Agent-facing commands require control routing (a local control directory or
etcd). Without RPC TLS they also verify the immutable
RootId-to-AgentId binding before RPC preflight, object binding, stdin reads, or
tool advertisement. This is a fail-closed deployment identity check, not
authentication; over RPC TLS the owner authorizes each request by role instead
and the binding is not checked. A legacy root without a binding requires a one-time,
operator-verified provision with `--adopt-legacy-agent-binding`; NoKV never
infers identity from the presentation path.
Before serving any Agent-facing command, the CLI performs the typed workspace
RPC preflight for every capability required by the 18-tool profile; a missing
capability or route mismatch stops startup.

Bring-up must stop if:

- the tool set is not exactly 18 tools;
- any normalized input schema differs;
- the root route is stale or belongs to another logical shard;
- without RPC TLS, the root has no durable Agent binding or is bound to another
  AgentId;
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
- golden Workbench results and errors, not only input schemas;
- the trusted-deployment boundary in [SECURITY.md](../SECURITY.md): network
  isolation, transport security, and scoped object-store and control-plane
  credentials.

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
