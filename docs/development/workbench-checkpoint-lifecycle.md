<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Workbench Checkpoint Lifecycle

Status: current contract on `main`.

This document describes the shipped NoKV-side lifecycle for LingTai Workbench
snapshots and durable restore. It replaces the historical phase plan that
predated snapshot renewal, typed expiry, and `workbench_restore`.

## 1. Vocabulary

| Term | Meaning | Lifetime |
| --- | --- | --- |
| Snapshot pin | A subtree root plus a stable metadata read version | Leased; retired explicitly or expires |
| Checkpoint name | An alias recorded in `metadata/checkpoints.jsonl` | Discoverability only; not an independent GC root |
| At-snapshot read | `stat`, `list`, or `read` resolved at the snapshot version | Valid only while the pin is live |
| Durable restore | Replayable CoW materialization into a different workbench | Survives process restart; exact retries are idempotent |
| Fork/object retention | A generic clone binding, or sealed exact-reference rows for a completed Workbench restore | Managed with the fork lifecycle, independently of the construction pin |

A snapshot is a stable historical read view. It does not make the live
workspace read-only, freeze concurrent writers, or establish an authorization
or tenant policy boundary.

## 2. Lease Contract

The low-level metadata snapshot API defaults to a one-hour lease. The Workbench
surface intentionally uses a longer user-facing policy:

- `workbench_snapshot` accepts optional `ttl_days`;
- the Workbench default is 7 days;
- the Workbench maximum is 90 days;
- the response reports the authoritative lease expiry;
- `workbench_snapshot_renew` is extend-only and cannot shorten protection;
- an expired snapshot returns the typed `SnapshotLeaseExpired` failure instead
  of silently serving a partial historical view;
- `workbench_snapshot_retire` is the explicit way to release a live pin.

Once a lease expires, the pin stops holding the metadata-history retention
floor. GC may then reap the pin and history or objects that have no other live
reference. A generic clone's durable binding or a completed Workbench restore's
sealed exact-reference rows continue to protect borrowed objects without a
permanent snapshot lease.

Renewal is a liveness mechanism, not archival retention. A checkpoint that must
outlive the Workbench maximum needs a future durable named-reference or export
contract rather than an indefinitely refreshed anonymous pin.

## 3. Registry and Discoverability

Workbench snapshot mint, renew, and attributable retirement append lifecycle
events to `metadata/checkpoints.jsonl`. An optional checkpoint `name` resolves
through this registry. The registry also preserves bounded `reason` and
`metadata` annotations.

The name is an alias, not a non-expiring reference. If the underlying lease
expires, resolving the name does not revive the snapshot or prevent GC.
`workbench_snapshot_list` joins registry history with live pin state and reports
states such as `alive`, `expired`, `retired`, or `reaped`.

The snapshot pin is authoritative and is created before the registry event is
appended. If registry publication fails after pin creation, the tool returns
`SnapshotRegistryWritePartial` with the created snapshot id and compensation
guidance. The caller must retry registry publication or retire the pin; it must
not assume the snapshot creation was rolled back.

Retirement is idempotent. The call that actually removes the pin reports
`retired=true`; an exact retry after the pin is absent reports `retired=false`.
The registry does not fabricate an explicit retirement for a pin that merely
expired and was reaped.

## 4. MCP Capability Boundary

The Workbench MCP profile has 17 base tools. `workbench_restore` is added as the
eighteenth tool only when every metadata owner relevant to the configured
workbench root confirms `restore_to_fork_v1`.

This yields two valid runtime surfaces:

- 17 tools: base Workbench operations, without durable restore;
- 18 tools: the same base plus capability-enabled durable restore.

The guarded LingTai setup deliberately requires the complete 18-tool surface.
It fails closed when owner capability is mixed or the canonical schema differs.
That integration requirement must not be generalized into a claim that the raw
Workbench profile always has exactly 18 tools.

The MCP adapter validates the workbench id and confines relative paths beneath
the configured root, normally `/agents/{agent_id}/wb`. This is namespace and
path scoping. It is not authentication, RBAC, or a tenant security boundary;
the configured metadata and object-store credentials define the actual trust
boundary.

## 5. Stable Historical Reads

`workbench_stat`, `workbench_list`, and `workbench_read` accept
`at_snapshot`. The tool resolves a numeric id or checkpoint name, verifies the
pin and its lease, and reads at the pinned metadata version.

Use the same snapshot for every file that belongs to one historical view. Reads
against the live namespace are separate operations and can observe later
commits. Structured JSON/YAML record parsing is supported for live reads;
snapshot reads currently expose bytes or snapshot-specific UTF-8 text-line
shaping rather than a full structured-record parser.

## 6. Durable Restore-to-Fork

`workbench_restore` is the recommended recovery operation for Agent workspaces:

1. The source workbench must have a committed run manifest and a live snapshot.
2. The caller supplies a different destination workbench id.
3. Source and destination must route to the same metadata shard.
4. NoKV materializes a detached CoW destination, seals the exact borrowed-object
   references, publishes the index overlay, and attaches the destination.
5. The source remains unchanged. Exact retries converge on the same result.

The restore state machine and cleanup/release work are durable across process
restart and lost responses. Capability is checked before the tool is advertised
and again when the operation runs. NoKV does not currently provide a
cross-metadata-shard restore transaction.

Generic `nokv rollback PATH SNAPSHOT_ID` still exists as a low-level in-place
operation and has no additional confirmation flag. Agent integrations should
prefer restore-to-fork so the source remains available for inspection and the
recovered workspace can be validated before use.

## 7. Current and Next

| Current on `main` | Not a current guarantee |
| --- | --- |
| Leased snapshot pins with explicit expiry | Non-expiring named checkpoints as independent GC roots |
| Extend-only renew and idempotent retire | Archival export that survives the metadata store |
| Registry aliases and bounded annotations | Full structured-record parsing at a snapshot |
| Typed lease-expiry errors | Live workspace freezing or writer fencing through the snapshot API |
| 17-tool base and capability-gated eighteenth restore tool | Authentication, RBAC, or tenant policy from path scoping alone |
| Same-shard durable CoW restore-to-fork | Cross-shard atomic restore or publication |

## 8. Verification

The canonical schema and exact guarded LingTai contract are checked by:

```bash
python3 ./scripts/lingtai-workbench/workbench_contract_test.py
```

The full live acceptance gate runs a real object store and validates LingTai
registration/reconnect, restore idempotency, CoW object accounting, crash and
restart barriers, source retirement, borrowed-object lifetime, index overlay,
cleanup, fsck, and final inventory:

```bash
uv run --project /path/to/lingtai-kernel \
  python ./scripts/lingtai-workbench/durable_restore_live_e2e.py \
  --lingtai-kernel-dir /path/to/lingtai-kernel \
  --profile full \
  --require-all
```

This gate is recovery and lifecycle evidence. It is not an enterprise
small-file throughput benchmark or proof of metadata high availability.

See [Checkpointing](../checkpointing.md),
[Copy-on-Write Workspaces](../cow-workspaces.md), and the
[LingTai Workbench preflight guide](../lingtai-workbench-preflight.md).
