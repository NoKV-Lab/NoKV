<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Metadata Schema

This document describes the metadata families and ordered key layouts present
on the current `main` branch. The schema is local to one metadata shard. A fleet
composes multiple independent shard schemas; it does not place one atomic Holt
transaction across them.

NoKV uses inode and dentry records as canonical namespace truth. Path indexes
are maintained accelerators for path, agent, and artifact queries. They can be
validated or rebuilt from canonical namespace state and must not redefine
namespace semantics.

## Holt Trees And Encoding

Holt stores each current record family in a separate named tree. History uses a
dedicated `history` tree plus `history_key_index` for ordered candidate scans.
The logical family names are part of NoKV's command and recovery formats; they
are not a claim that Holt itself understands filesystem semantics.

Ordered numeric key components use fixed-width, big-endian integers. Variable
path and name suffixes preserve byte ordering inside their family-specific
prefix. The primary current trees are:

| Logical family | Holt tree | Current purpose |
| --- | --- | --- |
| `System` | `system_current` | allocator, recovery, GC, fsck, and other internal state |
| `Mount` | `mount_current` | mount-scoped records |
| `Inode` | `inode_current` | canonical inode attributes |
| `Dentry` | `dentry_current` | canonical directory edges and projected child metadata |
| `Parent` | `parent_current` | reverse parent/link index |
| `Xattr` | `xattr_current` | inode extended attributes |
| `ChunkManifest` | `chunk_manifest_current` | immutable body summaries and chunk manifests |
| `Session` | `session_current` | session-scoped metadata family |
| `PathIndex` | `path_index_current` | canonical path accelerator and typed agent index rows |
| `Watch` | `watch_current` | typed watch event log |
| `Snapshot` | `snapshot_current` | leased snapshot pins |
| `Gc` | `gc_current` | durable object-reclamation queue |
| `CommandDedupe` | `command_dedupe_current` | idempotent command results by request ID |
| `ForkBinding` | `fork_binding_current` | lazy CoW fork anchors |
| `ForkShadow` | `fork_shadow_current` | fork-inode to source-inode fall-through mapping |
| `History` | `history` | previous versioned values |

Some internal `System` keys are subsystem-private and evolve with restore and
fsck state machines. Callers should use typed service APIs rather than parsing
those keys.

## Canonical Namespace Keys

```text
inode_current
  key: mount_id | inode_id
  val: InodeAttr

dentry_current
  key: mount_id | parent_inode | name
  val: DentryProjection
       = DentryRecord + InodeAttr + optional BodyDescriptor

parent_current
  key: mount_id | child_inode | parent_inode | name
  val: reverse link metadata

xattr_current
  key: mount_id | inode_id | xattr_name
  val: xattr bytes
```

The dentry projection lets `ReadDirPlus` obtain the common child attributes and
optional body summary from one ordered prefix scan. The authoritative identity
and link semantics still come from the inode/dentry command invariants.

In a multi-shard fleet, `inode_id` carries its owning `shard_index` in the high
bits. That makes a bare-inode request routable, but it does not change the local
key layout or create cross-shard atomicity.

## Object Manifest Keys

```text
chunk_manifest_current
  key: mount_id | inode_id | generation | chunk_index
  val: BodyDescriptor  when chunk_index = u64::MAX
  val: ChunkManifest   for a real chunk index
```

The sentinel row summarizes one immutable file-body generation. Real chunk rows
contain the slice and block descriptors required to construct object range
reads. Object bytes are not stored in Holt; see [Object Layout](./object-layout.md).

## Path Index Keys

The `PathIndex` family currently contains three key shapes:

```text
canonical path accelerator
  mount_id | "/" | normalized path components

typed index catalog
  mount_id | "catalog\0" | index root path

typed index row
  mount_id | "row\0" | index root path | "\0" | row path
```

Canonical path entries accelerate full-path resolution. Typed catalog and row
records support structured agent/workspace indexes. Path-index mutations are
committed with their related canonical mutations when one command owns both.
A stale or absent accelerator must fall back to canonical traversal rather than
inventing a namespace result.

## Watch, Snapshot, Fork, And GC Keys

These are implemented current families, not planned placeholders:

```text
watch_current
  key: mount_id | scope_inode | apply_index | event_id
  val: typed WatchEvent

snapshot_current
  key: mount_id | snapshot_id
  val: root_inode, read_version, created_version, lease expiry

fork_binding_current
  key: mount_id | fork_root_inode
  val: source_root, pinned_read_version, snapshot_id, created_version

fork_shadow_current
  key: mount_id | fork_inode
  val: source inode fall-through mapping

gc_current
  key: mount_id | enqueue_version | inode | generation | chunk | block
  val: object key, size, digest, enqueue version, enqueue time
```

Snapshot pins retain the history and object generations required by a stable
read view until retirement or lease expiry. A lazy CoW fork retains a frozen
source view through its binding and snapshot, while shadow rows let bare-inode
reads resolve undiverged fork entries. GC rows are enqueued in the same
shard-local metadata command that removes namespace reachability.

## History And Dedupe

```text
history
  key: family_tag | user_key_length | user_key | (u64::MAX - commit_version)
  val: previous versioned value

history_key_index
  key: family_tag | user_key
  val: latest indexed historical version

command_dedupe_current
  key: request_id
  val: encoded result of the committed command
```

Inverting the commit version orders newer historical values before older values
for one logical key. Snapshot reads combine current rows and history candidates
at a pinned read version. Dedupe records let a caller repeat the same request ID
after a lost or ambiguous response without creating a second logical mutation.

## Command Boundary

All metadata changes flow through `MetadataCommand`:

```text
request_id
kind
read_version
commit_version
primary family/key
predicates
mutations
watch projection
```

Holt evaluates predicates and applies the planned mutations, history records,
watch records, allocator updates, and dedupe result in one atomic local batch.
A failed predicate applies none of that command's mutations.

The boundary is one shard. The RPC batch layer rejects a request batch that
routes to multiple shards, and inode-addressed dual-endpoint operations reject
different shard indices. Cross-shard transactions require an explicit future
protocol; they must not be inferred from the presence of globally unique inode
IDs.

## Scope Boundaries

- `mount_id` scopes keys inside a shard; it is not tenant authentication.
- `shard_index` supports routing and inode uniqueness; it is not an isolation or
  authorization policy.
- Path indexes are derived accelerators; inode/dentry state remains canonical.
- Snapshot consistency is tied to an explicit read version. Independent live
  reads do not automatically form one stable view.
- Object-provider replication and durability are outside this schema; metadata
  records only describe logical reachability and recovery state.
