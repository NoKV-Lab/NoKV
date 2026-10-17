<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Live Deployment Preflight

This guide covers bringing up a live NoKV deployment and qualifying it before
a production handoff. Downstream systems provide skills over the native full
CLI; embedded callers use the Python SDK. Every surface uses the same workspace
format and grants no additional authority or compatibility route.

## Required Inputs

Before registration, obtain:

- one 16-byte `RootId`;
- one persisted 16-byte `AgentId` and its immutable control-plane binding to
  that RootId;
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

## Stable Append Deployment

For cross-process append retries, use native `workspace-path append` and
`operation status|inspect|recover`, or their direct Python SDK equivalents.
The frozen `workbench_append` tool has no caller-supplied stable id and is not
a durable queue's redelivery boundary. See the [append guide](./append.md).

Stable append requires the current RPC v12, system format 13, and publication
value format 7. Its public cleanup APIs require `artifact_append_recovery_v1`.
Use matching owner, CLI, and Python builds; old formats are rejected before
modification, not automatically migrated. Record the linked backend from
`nokv version --json`: the qualified build uses registry Holt 0.8.6, which is
independent of a developer's local Holt checkout.

In addition to ordinary write admission, the concrete object-provider handle
must pass append seal admission: conditional creation and replacement of a
zero-byte guard must prevent a delayed create-if-absent PUT from resurrecting
an aborted revision. Preserve those guards and failed revision reservations.
External deletion, overwrite, or bucket expiration of guards breaks this
contract. A provider's brand alone does not establish conformance.

Persist the logical id, root, complete intent, and delta before dispatch. For
quarantined cleanup, inspect publicly, save `operation_token.state_digest`,
then call `operation recover` with that digest on every retry of the same
recovery request. Status, inspection, and recovery need metadata routing but
no caller-side S3 credentials or delta. The owner still needs a healthy,
admitted provider to finish cleanup. `requested=true` acknowledges recovery
admission; the queue may acknowledge its event only after append commitment.

The server's `--append-activity-lease-ms` defaults to 1,800,000 ms and accepts
1,000 through 86,400,000 ms, with an additional 30-second clock grace. The local
fault qualification uses 1,000 ms explicitly; its recovery timings do not
measure the default lease. Run the
[stable append gates](../scripts/workbench/README.md#stable-append-gates) in fresh
isolated evidence directories. Their [qualification record](./development/append-qualification.md)
states the tested profile and limits separately from the full deployment gates.

## Default Deployment Shape

A serving shard is one `nokv serve` process over one exclusive Holt store. That
process is the metadata authority: it holds the owner lease, applies every
metadata command to its local Holt WAL, and acknowledges once that WAL is
durable. Nothing else has to be running for reads and writes to work.

Control (etcd) is required, but for two narrow jobs: resolving which process
owns a root, and fencing that ownership with an epoch. It is not on the write
path and it does not hold metadata.

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
`live_workbench.py` currently drives the 18 tools through a `nokv mcp` child
process. That sidecar is deprecated and is not a supported NoKV integration
surface; it remains only as this harness's transport, and evidence produced
over it qualifies neither the CLI nor the Python SDK path.
The selected `--workbench-root` is durable presentation configuration because
canonical v1 manifests contain its projected paths. Keep it identical across
restart/replay; it never replaces `RootId` as the storage or routing identity.
Agent-facing commands require etcd control routing and verify the immutable
RootId-to-AgentId binding before RPC preflight, object binding, stdin reads, or
tool advertisement. This is a fail-closed deployment identity check, not
authentication. A legacy root without a binding requires a one-time,
operator-verified provision with `--adopt-legacy-agent-binding`; NoKV never
infers identity from the presentation path.
Before serving the Workbench facade, the CLI performs the typed workspace
RPC preflight for every capability required by the 18-tool profile; a missing
capability or route mismatch stops startup.

Bring-up must stop if:

- the Workbench facade tool set is not exactly 18 tools;
- any normalized input schema differs;
- the root route is stale or belongs to another logical shard;
- the root has no durable Agent binding or is bound to another AgentId;
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

Stable append has separate native CLI/Python fault evidence and a real demo
consumer redelivery test. Those results do not replace the complete 18-tool
workflow or qualify unrelated commit, restore, and GC lifecycles.

Keep raw commands, environment profile, logs, and result artifacts with the
qualification report.

Current source-level/unit evidence does not qualify a production handoff. In
addition to the unavailable shared recovery path, live qualification must still
prove or implement all of the following:

- provider-attested upload completion across the direct SDK data path, not a
  forgeable client assertion alone;
- production adoption or bounded abort/cleanup for interrupted commit and
  restore operations, including release of their history/revision holds;
- late direct PUT completion after generic publication abort; stable append's
  conditional-seal closure is separately tested, not evidence for every
  publication lifecycle;
- reconciliation that drives ambiguous object deletion out of quarantine;
- destructive provider operations fenced against control-plane lease transfer,
  not only a preceding shard-local owner check.

Until those rows have executable fault-injection evidence they are `NOT
QUALIFIED`, even when the exact 18-tool contract and local happy path pass.
