<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Checkpointing

NoKV stores file bodies in an external object store and makes them visible
through metadata. Checkpoint correctness therefore depends on separating body
staging from metadata publication and stating the metadata-shard boundary
explicitly.

## Three Related Primitives

### Artifact publication

`PublishArtifact` uploads one object body first, then publishes its inode,
dentry projection, and body descriptor with one metadata command:

```text
body bytes -> object store
metadata command:
  - inode attr
  - dentry projection
  - body descriptor
```

The namespace entry appears only after that metadata command commits. Replacing
an existing artifact follows the same object-first pattern and returns the old
body descriptor for retention and GC accounting.

### Multi-file checkpoint publication

`publish_checkpoint` stages every component file and then publishes all of them
with one metadata command. This gives all-or-nothing visibility for files under
one parent served by one metadata owner.

In this API, a checkpoint "shard" is a component or rank file. It is not a NoKV
metadata shard. NoKV does not currently provide a transaction that atomically
publishes paths owned by different metadata shards.

### Snapshot pin

`snapshot_subtree` records a subtree root and metadata read version. Reads at
that snapshot see a stable historical view while its lease remains valid. A
snapshot does not make the live workspace read-only and is not the same feature
as workspace freezing or an authorization policy.

Snapshot pins are leased GC roots. They protect the required metadata history
and object references until they are retired or their lease expires. A durable
fork records a separate retention relationship: generic clones use a fork
binding, while Workbench restore seals exact borrowed-object references before
attach. Neither relies on keeping the construction snapshot leased forever. See
[Workbench checkpoint lifecycle](development/workbench-checkpoint-lifecycle.md)
for the Workbench defaults and restore contract.

## Atomicity Boundary

| Operation | Atomic visibility boundary |
| --- | --- |
| Publish or replace one artifact | One metadata command on one metadata owner |
| Publish one multi-file checkpoint | All component files in one parent on one metadata owner |
| Read several files at one historical point | One leased snapshot read version |
| Publish across metadata shards | Not currently a transaction |

An application that needs a consistent multi-read view should pin a snapshot
and use that snapshot for every read. It should not assume the live namespace
will remain unchanged between separate calls.

## Failure Handling

The caller must distinguish an authoritative rejection from an indeterminate
commit outcome:

- If object upload fails, no metadata publication is attempted.
- If metadata authoritatively rejects the command before commit, the returned
  staged object references may be scheduled for cleanup.
- If transport fails or the commit acknowledgement is lost, publication may
  already be visible. Do not blindly delete the staged objects. Reconcile by
  reading back the namespace or retrying with the operation's idempotency or
  identity contract before cleanup.
- A successful remove or replace persists the old body references in the
  metadata GC queue and returns the old body descriptor to the caller.
- A live snapshot pin protects the historical metadata and body references it
  needs. Protection ends after retirement or lease expiry.
- Typed watch replay uses the durable watch log rather than MVCC history. Live
  FUSE mounts use it for kernel entry and inode cache invalidation; broader SDK
  watch-consumer integration remains future work.
