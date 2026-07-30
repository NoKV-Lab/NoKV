<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Copy-on-Write Workspaces

NoKV provides copy-on-write workspace primitives in its durable workspace
metadata control plane. A fork reuses immutable object bodies from its source,
copies the namespace metadata needed to materialize the destination, and writes
new object generations only for data that diverges.

This is useful for long-running agents that need a recoverable workspace rather
than an in-place mutation history: pin a known-good view, restore it into a new
workspace, validate the fork, and only then direct work to the recovered copy.

## Operations

| Operation | Service API | Current semantics |
| --- | --- | --- |
| **snapshot** | `snapshot_subtree(root) -> SnapshotPin` | Leased, read-only historical MVCC view. It protects the history and object references needed by that view until retirement or lease expiry. It does not freeze the live workspace. |
| **clone** | `clone_subtree(src) -> CloneHandle` | Writable CoW fork. Object bodies are shared by key; destination metadata is materialized in proportion to the number of entries. Source and destination must route to the same metadata shard. |
| **diff** | `diff_subtrees(a, b) -> Vec<SubtreeDelta>` | Reports added, removed, and modified paths with digest and size information. The current walk is O(tree), and both paths must route to the same metadata shard. |
| **rollback** | `rollback_subtree(target, snapshot_id)` | Low-level in-place replacement of a target subtree from a snapshot. It remains available through the service and CLI, but is not the preferred Agent recovery workflow. |
| **restore to fork** | Workbench `workbench_restore` | Creates a different destination workspace from a source snapshot. The source remains unchanged, exact retries are idempotent, and the operation is capability-gated and same-shard only. |

Path variants such as `clone_subtree_path`, `clone_subtree_path_into`,
`diff_subtrees_path`, and `rollback_subtree_path` accept string paths.

## Recommended Agent Recovery Flow

```text
committed workspace ──snapshot──▶ source@snapshot       (leased historical view)
          │
          └── workbench_restore(new destination) ──▶ recovered fork
                                                        │
                                               validate / inspect / diff
                                                        │
                                      ┌── accept: route work to the fork
                                      └── reject: release the fork
```

Restore-to-fork avoids destroying the source and makes validation explicit.
Generic in-place `rollback` is better treated as a low-level administrative or
debugging primitive when the caller intentionally accepts that mutation.

## CLI

The generic workspace primitives are wired into the CLI:

```sh
# Pin a leased historical view. LEASE_MS is optional.
nokv snapshot /base [LEASE_MS]

# Create a writable fork and compare it with the source.
nokv mkdir /forks
nokv clone /base /forks/agent-1
nokv diff /base /forks/agent-1

# After intentionally changing /base, restore that same root in place.
# Prefer Workbench restore-to-fork for Agent recovery.
nokv rollback /base SNAPSHOT_ID
```

Workbench recovery is exposed through the capability-gated MCP
`workbench_restore` tool rather than this generic CLI surface.

## Guarantees and Limits

- **Object-body sharing.** A clone references immutable source object keys and
  writes a fresh generation when a file diverges. It does not copy the source
  file bodies merely to create the fork.
- **Metadata work is not constant.** Clone materialization is O(entries). Large
  namespace trees can therefore require substantial metadata work even when the
  object bodies remain zero-copy.
- **CoW data divergence.** Post-clone writes in one workspace use new object
  generations and are not observed through the other workspace's namespace.
  This is data and namespace isolation, not an authentication, RBAC, or tenant
  security boundary.
- **Lease-aware GC.** A snapshot lease is renewable and extend-only. An expired
  pin stops holding the metadata-history retention floor and can be reaped by
  GC. Generic clones retain borrowed data through a durable fork binding;
  Workbench restore replaces its temporary construction binding with sealed
  exact-object references before attach.
- **Shard-local publication.** Metadata attach, graft, and publication points
  are predicate-guarded and crash-consistent within one metadata owner. Clone,
  diff, rollback, and restore do not provide cross-shard transactions.
- **Recovery is explicit.** `workbench_restore` uses a replayable state machine,
  a new destination, and idempotent retries. It is exposed only when every
  relevant owner confirms `restore_to_fork_v1`.

## Scaling Model

Single-node metadata is the default deployment. NoKV also has experimental
path-based sharding: independent workspace roots can be routed to independent
metadata owners while each shard keeps one active writer. Holt remains the
lightweight embedded metadata engine inside an owner; NoKV does not add a Raft
quorum to every metadata operation, and consensus-replicated metadata HA is not
a current guarantee.

This model targets small-file throughput by keeping the metadata hot path local
and scaling independent workspace shards horizontally. Atomic publication and
CoW recovery remain shard-local. Any enterprise throughput claim must be backed
by a reproducible benchmark that states workload, hardware, object store,
concurrency, build revision, and run artifact; this document intentionally does
not publish an unsupported headline number.

See [Architecture](architecture.md) and
[Checkpointing](checkpointing.md) for the owner and publication boundaries.
