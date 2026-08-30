<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Architecture

Status: normative workspace architecture.

## System Shape

```mermaid
flowchart LR
    Skills["Downstream Agent skills"] --> CLI["Native full nokv CLI"]
    CLI --> Agent["Transport-free Workbench facade"]
    Agent --> SDK["Rust Agent SDK"]
    CLI --> SDK
    Python["Python SDK"] --> SDK
    Local["Materialize / collect"] --> CLI
    Local --> Python

    SDK --> Router["Root router"]
    Router --> Control["Control plane<br/>root placement + owner lease"]
    Router --> Owner["Fenced logical-shard owner"]

    Owner --> Meta["NoKV metadata semantics"]
    Meta --> Store["TxnStore<br/>ordered reads + checked writes"]
    Store --> Holt["HoltStore<br/>serving local adapter"]

    SDK --> Data["Direct immutable-object data path"]
    Data --> Cache["Local NVMe soft cache"]
    Data --> Object["S3-compatible durable objects"]
    Owner --> Object
```

The metadata and object paths are separate. Small control and namespace records
go through the shard owner. Clients stream immutable blocks directly through
the object boundary after receiving a revision/upload plan.

FUSE, POSIX, CSI, and fsspec are not architecture layers.

## Package Direction

```mermaid
flowchart TD
    CLI["native full nokv CLI"] --> Agent["nokv-agent"]
    CLI --> Client["nokv-client"]
    Python["nokv-python"] --> Client
    Agent --> Client
    Client --> Protocol["nokv-protocol"]
    Client --> Object["nokv-object"]
    Client --> Types["nokv-types"]

    Server["nokv-server"] --> Protocol
    Server --> Meta["nokv-meta"]
    Server --> Store["nokv-meta-store"]
    Server --> HoltAdapter["nokv-meta-holt"]
    Server --> Control["nokv-control"]
    Server --> Object
    Meta --> Types
    Meta --> Store
    HoltAdapter --> Store
    HoltAdapter --> Holt["Holt"]
    Control --> Types
    Object --> Types
```

Arrows point from a consumer to its dependency. The
[code contract](./development/code_contract.md) is normative.

Key constraints:

- types and protocol are storage-neutral;
- metadata owns durable semantics, logical keyspaces, and record codecs;
- the Holt adapter owns the physical tree mapping and local durability;
- control owns root placement and owner fencing, not path semantics;
- object owns provider I/O, not reachability;
- client uses protocol/routing and never imports meta/server;
- Agent adapters shape tools over SDK traits and remain transport-free;
- the native full CLI is the primary integration surface;
- the Python SDK is the secondary embedded surface.

## Identity And Namespace

```text
RootPlacement(root_id)                    control-plane truth
RootFence(root_id)                        installed shard-local fence

WorkspaceCurrent(root_id, workbench_id)
  -> incarnation, revision, lifecycle

WorkspaceIncarnationClaim(root_id, incarnation)
  -> stable workbench_id; permanent and never reused

PathCurrent(root_id, incarnation, path)
  -> generation, immutable revision, body/manifest digests, size,
     dependency bounds, content type, typed projection
```

`PathCurrent` is the only namespace truth. Workbench names are separated from
path keys by a never-reused incarnation. This prevents a failed restore or
retired Workbench's rows from appearing under a later claim.

Paths are exact case-sensitive UTF-8. The physical codec adds one to each UTF-8
byte, separates components with NUL, and ends an exact key with marker `0x01`.
This reserves both markers, places a child's delimiter rollup before its exact
artifact and both before longer siblings, and prevents an exact key from being
a strict prefix of another valid path key. The same normalizer/codec owns
storage keys, request identities, index identities, and restore member ids.
System format version 9 retains and gates this layout.

Directories are implicit. The Workbench root and five standard sections are
virtual. A file stat is a point read; an implicit-directory stat is a prefix
existence check.

See [Metadata Schema](./metadata-schema.md) for byte-level and state-machine
rules.

## Read Path

```mermaid
sequenceDiagram
    participant C as SDK
    participant R as Router
    participant M as Shard owner
    participant S as TxnStore (Holt local profile)
    participant O as Object backend/cache

    C->>R: stat/open(root, workbench, path)
    R->>M: versioned request + placement generation
    M->>S: read visible WorkspaceCurrent
    M->>S: point-get PathCurrent
    M-->>C: generation + immutable read plan
    C->>O: ranged block reads
    O-->>C: verified bytes
```

A valid cached Workbench marker can avoid its routing/lookup work, but the
client still uses generation/version validation. The authoritative artifact
lookup is one exact schema key and does not follow the revision-lifetime row.
The current portable read path first captures owner, root-fence, and commit
clock state. Each dependent marker or path point read then batches those three
guards with its data key in one `TxnStore::read` call. This costs eleven point
reads for an uncached live exact get, but it does not rely on a backend-specific
snapshot session. A later declarative batch API can remove the repeated guards
without weakening owner fencing.

A direct-child list replaces the path lookup with one ordered prefix-scan path.
Its common-prefix rollups become implicit-prefix page items. A recursive list
uses the same prefix without delimiter rollup. Each physical
page batches its owner, root-fence, and commit-clock guards with a bounded store
scan. One protocol page may use multiple scans of at most 255 logical items
each when its requested limit is larger. The metadata listing may merge an
exact-prefix point read only after descendant EOF, while the Workbench adapter
exposes only direct children and drops that self row.
There is no per-entry metadata fanout. The returned cursor is bound to the
workbench scope, read view, continuation fence, and child anchor. Snapshot
continuations retain one exact root read version. Live continuations may move
to a newer root read version only while the target workspace incarnation and
revision remain unchanged; target drift fails closed, and an initial bounded
collection may restart in full but never merges workspace revisions. This
contract is gated by operation schema `nokv.workspace.rpc.v9`; v7 added the
exact read-version fence used by path point reads, v8 added the required typed
`Prefix`/`Exact` catalog path match so an artifact cannot inherit fields from
same-name descendants, and v9 adds an explicit query profile plus tagged query
rows and authoritative search, aggregate, and catalog totals/facets. V3 first
added the provider-neutral object namespace identity to every root route.

Each TCP connection starts with the fixed-width, schema-neutral transport
handshake v1. The client offers one exact operation schema, and the server
accepts only `nokv.workspace.rpc.v9`; the handshake version remains stable when
the operation schema changes. For upgrade diagnostics, the server recognizes
only operation-first envelopes from the public v2 client and the post-tag v3
client. It parses a bounded route/request header, ignores the operation, emits
one schema-readable failure, and closes without dispatch. The v2 client gets
its exact v2 failure envelope; the v3 client gets a v9 failure envelope so its
decoder reports the v9/v3 schema mismatch. Unknown legacy schemas, malformed
envelopes, and malformed handshakes fail closed. This is a read-only rejection
path, not a legacy response decoder or operation fallback.

Secondary-index queries run at one read version and filter every result by the
matching visible incarnation. `ArtifactV1` remains the default Workbench query
contract. `GenericNamespaceV1` carries a bounded, normalized, display-only
presentation root and materializes Workbench, section, implicit-directory, and
artifact rows at that same read version. Its transient `path` scalar is the full
presentation path, while row identity stays the canonical Workbench and relative
path. An exact artifact wins over a same-name virtual directory without hiding
descendants. Its cursor binds the profile, presentation root, storage root, read
version, and query. Presentation paths are never persisted as metadata or used
as storage authority.
The generic catalog exposes the seven legacy built-ins plus declared durable
custom indexes. Custom-index registration builds an immutable, paged generation
behind one current-pointer CAS; rows retain ordered duplicate values and exact
directory or artifact-revision binding. Commit, snapshot, restore, retirement,
and reference GC carry the same sealed generation closure, so a dirty snapshot
can be restored without an intervening commit and an old generation cannot be
reused after reclamation. Object bodies are read only when the selected tool
actually requires them.

## Publish Path

```mermaid
sequenceDiagram
    participant C as SDK
    participant M as Shard owner
    participant O as Object backend
    participant S as TxnStore (Holt local profile)

    C->>M: begin publish + request id
    M-->>C: operation/revision/object plan
    C->>O: stream immutable blocks
    C->>M: complete(lengths, digests)
    M->>O: verify completion evidence
    M->>S: one fenced MetadataCommand transaction
    S-->>M: deterministic commit result
    M-->>C: generation + revision + digest
```

The command validates schema, local root fence, owner epoch, request id,
workspace/path generations, and revision reference state before applying any
mutation. It atomically publishes:

- the revision and block manifest;
- the new path and workspace revision;
- strong-reference changes;
- secondary indexes;
- one typed event;
- GC candidacy for a replaced revision;
- the deterministic replay result.

Failed uploads never become visible. Response loss after commit returns the
same result on retry. Generic random writes are absent; append is immutable
segment publication plus stream-head CAS.

Commit replay resolves its deterministic build-operation identity before any
live workspace lookup. A terminal retry authenticates the complete stored
request and commit-owned run-manifest binding, then verifies the corresponding
durable publish-operation result. Consequently a later replacement head cannot
change the result of retrying an older exact commit.

The durable request also stores a domain-separated digest of every caller-known
run-manifest projection input except the owner-supplied commit time. Recovery
checks that digest before rebuilding or publishing a manifest, including while
the commit is still Running and has no staged-manifest binding.

## Revision Ownership

```text
nokv/artifacts/{logical_shard_id}/{root_id}/{artifact_revision_id}/blocks/{object_index}
```

Revision ids never name a physical owner. A process owner may change while
logical-shard/root/revision identity stays stable.

Every current path and durable commit has an exact `RevisionRef`. A revision
that reuses older blocks has a sealed dependency reference to each distinct
owner revision. The child revision stores a strong-reference count and epoch:

```text
reference add/remove
  -> mutate RevisionRef
  -> update count
  -> increment epoch
  -> if count == 0, create candidate(epoch, last_zero_version)
```

GC claims only the current zero-count epoch and atomically moves the revision
from `Available` to `Deleting`. New references require `Available`, closing the
restore/commit-versus-delete race.

## Snapshot And Commit Reads

NoKV's logical history and hold records support two different products:

- a leased snapshot creates a `HistoryHold(read_version)`;
- a durable commit scans under a temporary `HistoryHold`, writes an ordered
  tree manifest, adds exact commit revision refs, verifies closure seals, then
  releases the history hold.

The snapshot reaper and renew operation race through one durable lifecycle CAS.
Once `ReapClaimed` wins, the history hold is gone and renewal fails.

`TxnStore` supplies a consistent snapshot for each read batch. `MetaShard`
reconstructs historical state across bounded batches and restarts if the
domain commit clock changes. NoKV does not expose a retained Holt view as a
snapshot or commit contract.

Commits do not pin global history. Tags are CAS-protected names for commits.
Commit retirement is explicit and checks all heads, tags, leases, lineage
children, and restore/fork consumers.

## Restore

Restore uses an operation-owned fresh Workbench incarnation:

```mermaid
stateDiagram-v2
    [*] --> Staging: claim name + source hold
    Staging --> Sealed: stage paths/refs + member digest
    Sealed --> Ready: recovery verifies closure
    Ready --> Visible: one marker/event/complete command
    Staging --> Cleaning: abort
    Sealed --> Cleaning: abort
    Cleaning --> Retired: remove members and refs
```

Staged paths have strong references but no visible marker. Root-wide query and
watch surfaces therefore cannot leak them. The final command verifies the exact
incarnation and member seal, publishes it, completes the operation, emits one
event, and releases the source hold.

Restore copies O(entries) metadata and zero object bytes inside one root/shard.
NoKV deliberately avoids a lazy overlay that would tax every later read/list.

## Sharding And Ownership

The control plane persists placement before a root's first write:

```text
RootId -> immutable LogicalShardId
LogicalShardId -> current physical owner, lease, epoch
```

The owner installs or validates `RootFence` in its metadata shard and checks
the lease epoch in the same physical transaction as each metadata commit.
Placement is never inferred from a path or modulo the number of owners.

A hot root's logical shard may be assigned to a dedicated physical process.
That is owner movement, not a change to logical shard or object keys.

One root is not split across logical shards. Cross-shard operations fail before
partial work.

## Recovery And Durability

Each production profile names its acknowledgement boundary:

```text
local
  ACK after shard-local Holt WAL boundary

durable distributed
  ACK after the configured shared logical-log boundary
```

The two modes have separate SLOs and benchmark rows. Recovery uses checkpoint
images plus the logical command log. Owner epoch prevents an old process from
committing or deleting objects after failover.

Current implementation status: the local Holt WAL boundary and the
object-backed shared-log boundary are executable. Every acknowledged metadata
mutation first commits to the shard-local hash-chained recovery outbox. The
shared boundary then persists an exact upload intent in Control, creates the
receipt-addressed immutable chunks and manifest, and atomically publishes the
new log frontier before returning. Control bounds both the logical chain and
the canonical encoded record so an upload is rejected before its first object
write if its pending or finalized etcd record cannot fit the admitted request
budget.

`Reopen` restarts the same exclusive Holt namespace. `RecoverLog` creates or
resumes a local namespace from strict shared-log receipts. Both paths validate
canonical receipts and prove the Control frontier is an exact prefix before
owner acquisition, then renew and repeat the proof after acquisition before
installing the owner fence. Complete pending uploads are replayed exactly;
incomplete uploads retain their intent until every receipt-derived cleanup key
is confirmed deleted or absent, after which an owner-fenced CAS may abort the
intent. Ambiguous cleanup remains fail-closed. Crashes after pending replay or
owner activation are resumable because a verified local-ahead prefix is not
mistaken for divergence. A completed owner gets the next epoch; an unfinished
`Recovering` owner rebinds the same epoch, so repeated crashes cannot create an
epoch gap.

Cold checkpoint installation is not yet admitted by the default product build:
the pinned published Holt dependency does not expose the bounded borrowed
checkpoint API. A Control checkpoint frontier therefore remains fail-closed in
`RecoverLog`. Log truncation/compaction, copied-directory identity, cross-host
checkpoint failover, and fsck also remain outside this qualification.

Because no checkpoint publisher exists yet, the shared log chain only grows:
every acknowledged mutation appends one segment reference to the Control
logical-shard record, and Control bounds that record (`MAX_LOGICAL_SHARD_RECORD_BYTES`)
and the chain length (`MAX_RECOVERY_LOG_SEGMENTS`). A shard that reaches either
bound loses its owner fence and stops serving; on the current record size that
happens after roughly a hundred acknowledged publications. Shared publication
is therefore an opt-in, not the resting state.

`nokv serve --recovery-publication` selects the recovery authority:

- `local-only` (**default**) keeps the exclusive Holt WAL as the only recovery
  authority, publishes no segments, leaves any earlier shared frontier frozen,
  and still proves owner liveness before every acknowledgement. Control (etcd)
  is used for routing and the owner lease only. A `local-only` shard has no
  shared-log successor path: losing its Holt directory loses the metadata,
  exactly as with the `local` mode above.
- `shared` is the object-first boundary described above. It is implied by
  `--metadata-recover-log`, which can only resume from a shared frontier, and
  it cannot be combined with `local-only` on that open mode. Do not select it
  for a shard that will accept more than a bounded burst of writes until
  checkpoint compaction lands.

Durable ledgers, not object listing, recover:

- staged/multipart uploads;
- commit construction;
- restore staging and cleanup;
- GC claims and ambiguous deletes.

The required fsck recomputes reference counts and closure seals from metadata;
source or design text alone is not fsck evidence.

## Architecture Acceptance

The architecture is accepted as one system, not as independent storage
experiments. The required evidence covers:

1. namespace point reads, delimiter scans, conflict handling, and command
   amplification across the declared workload matrix;
2. revision-owned publication, visibility, lifecycle, references, GC, and
   recovery;
3. protocol, server, native CLI, Python and Rust SDKs,
   control routing, and lifecycle workers on
   the same schema and identity model;
4. owner failover, checkpoint/log recovery, ambiguous provider outcomes, and
   live Workbench workflows;
5. the complete [acceptance plan](./development/workspace-acceptance.md),
   with each applicable gate reported as `PASS`, `FAIL`, or `NOT QUALIFIED`.
