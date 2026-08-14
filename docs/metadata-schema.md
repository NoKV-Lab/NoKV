<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Metadata Schema

Status: normative workspace durable schema.

This is NoKV's only durable metadata contract. The
[Workbench contract](./workbench-contract.md) defines the stable product
facade.

## Schema Gate

Every logical-shard store has one authoritative marker:

```text
System("schema")
  -> value_format_version = 1
     schema_id = "nokv_workspace"
     format_version = 8
```

Startup is fail-closed:

- an empty store is initialized with the exact supported marker and logical
  keyspace catalog;
- a nonempty store opens only when its marker, value format, and configured
  adapter catalog match this contract;
- a missing, malformed, unknown-version, or inconsistent store is rejected.

## Ids, Names, And Keys

Each family owns one stable logical keyspace; keys inside it do not repeat the
keyspace identifier. A store adapter maps the catalog to Holt trees,
FoundationDB subspaces, or a replicated namespace. The `History` keyspace
prefixes its composite key with the source family's stable one-byte format tag
because it contains records from several families.

Fixed-width storage ids:

```text
RootId                  16 bytes, globally unique
LogicalShardId          16 bytes, globally unique
WorkspaceIncarnationId  16 bytes, never reused
ArtifactRevisionId      16 bytes, unique within one RootId
SnapshotId               8 bytes, unsigned big-endian and unique within one RootId;
                                  exactly the numeric Workbench facade id
OperationId             16 bytes, unique within one RootId
CommitId                32 bytes, root-global SHA-256 identity
```

All integers are unsigned big-endian. General variable byte strings use a
four-byte big-endian length followed by exact bytes.

An external `workbench_id`:

- starts with an ASCII letter or digit;
- contains only ASCII letters, digits, `_`, and `-`;
- is at most 128 bytes;
- is case-sensitive.

Paths are case-sensitive UTF-8 with no implicit Unicode normalization. The
canonical path key is:

```text
PathCurrent key =
  root_id
  | workspace_incarnation_id
  | ordered(component_0) | NUL | ... | ordered(component_n) | 0x01
```

`ordered(component)` adds one to each valid UTF-8 byte. Valid UTF-8 never uses
`0xff`, so the transform is length-preserving, reversible, and preserves byte
order while reserving `0x00` and `0x01`. NUL is the physical component
delimiter; `0x01` terminates an exact row. The resulting order is `a/NUL
subtree`, exact `a`, then longer siblings such as `a\u{1}` and `ab`, and no
exact key is a strict prefix of another valid path key. A child/subtree prefix
appends NUL, so `a` cannot match `ab`. The empty path has no `PathCurrent`
record; the workspace root is synthesized from `WorkspaceCurrent`. This
key-layout cutover is gated by system format version 8; older stores are
rejected rather than dual-read.

The one shared normalizer enforces:

- relative paths only;
- non-empty components that are neither `.` nor `..`;
- no `/`, backslash, or NUL inside a component;
- at most 4096 UTF-8 bytes and 64 components;
- no lossy cleanup, case folding, or Unicode normalization.

Request identities, path-reference ids, index keys, restore members, and
canonical path keys must all use that implementation.

Snapshot aliases and tag names are case-sensitive UTF-8 without NUL, at most
128 bytes, and encoded with a two-byte big-endian length. A secondary index
never concatenates an unescaped value: each index declares a versioned,
order-preserving codec for null, boolean, signed/unsigned integer, finite
float, timestamp, bytes, and string values.

## Durable Format Registry

`System.format_version` is `8`. Durable codecs are independently versioned:
publication-owned workspace/path/revision records and immutable commit records
use value version `2`; `ChangeEvent` and the logical recovery-outbox record use
value version `2`; other ordinary workspace records and the recovery storage
header/chunk records currently use value version `1`; `CommandDedupe` uses
version `2` to bind its
exact result to a recovery LSN; `BuildCommitOperation` uses version `5` to
retain the complete exact commit request, opaque Agent projection-input digest,
first owner-observed commit time, run-manifest publication condition, and
immutable staged-manifest binding needed for head- and path-independent replay;
and `RestoreOperation` uses version `4` to retain the complete source
workbench/incarnation and concrete source selector needed for
source-independent terminal replay. Unknown versions fail closed. Keys do not
repeat a version because the store-level schema marker gates their codec.

The metadata schema assigns each logical keyspace a stable `u16` identifier
and name. Store adapters map this catalog to their physical layout.

| Name | Keyspace ID | Format tag |
| --- | ---: | ---: |
| `system` | `0x0101` | - |
| `root_fence` | `0x0102` | `0x01` (reserved) |
| `command_dedupe` | `0x0103` | `0x14` (reserved) |
| `change_event` | `0x0104` | `0x10` (reserved) |
| `history` | `0x0105` | - |
| `recovery_outbox` | `0x0106` | - |
| `workspace_current` | `0x0202` | `0x02` |
| `path_current` | `0x0203` | `0x03` |
| `artifact_revision` | `0x0204` | `0x04` |
| `artifact_manifest` | `0x0205` | `0x05` |
| `revision_ref` | `0x0206` | `0x06` |
| `commit` | `0x0207` | `0x07` |
| `commit_member` | `0x0208` | `0x08` |
| `workbench_commit_head` | `0x0209` | `0x09` |
| `tag` | `0x020a` | `0x0a` |
| `snapshot_ref` | `0x020b` | `0x0b` |
| `snapshot_alias` | `0x020c` | `0x0c` |
| `history_hold` | `0x020d` | `0x0d` |
| `commit_consumer` | `0x020e` | `0x0e` |
| `secondary_index` | `0x020f` | `0x0f` |
| `operation` | `0x0211` | `0x11` |
| `restore_member` | `0x0212` | `0x12` |
| `staged_object` | `0x0213` | `0x13` |
| `gc_candidate` | `0x0215` | `0x15` |
| `gc_barrier` | `0x0216` | `0x16` |
| `workspace_incarnation_claim` | `0x0217` | `0x17` |

System keyspaces use IDs `0x0101` through `0x0106`. Domain keyspaces use
`0x0200 | format_tag`. The `MetadataFamily` format tags remain one byte and keep
their existing recovery and history encoding. Reserved format tags do not name
caller-mutable metadata families. `MetaShard` appends `recovery_outbox` rows in
the same transaction as each authoritative mutation.

Initial durable enum discriminants:

```text
WorkspaceState:  Staging=1, Visible=2, Retired=3
RevisionState:   Available=1, Deleting=2, Deleted=3, Quarantined=4
CommitState:     Sealed=1, Retiring=2, Retired=3
SnapshotState:   Active=1, ReapClaimed=2, Reaped=3, Retired=4
ReferenceKind:   Path=1, Commit=2, RevisionDependency=3
HistoryHoldKind: Snapshot=1, BuildCommit=2, Restore=3
HistoryHoldState: Active=1, Releasing=2
RootActivationState: Installing=1, Active=2, Draining=3, Fenced=4
RootPlacementLifecycle: Provisioning=1, Active=2, Draining=3, Retired=4
CommitConsumerKind: WorkbenchHead=1, Tag=2, Lease=3, ChildCommit=4
RestoreSourceKind: Snapshot=1, Commit=2
OperationKind:   Publish=1, BuildCommit=2, Restore=3, CommitRetire=4, Gc=5
PublishPhase:    Uploading=1, Finalizing=2, Published=3, Aborting=4,
                 Cleaning=5, Cleaned=6, Quarantined=7
BuildCommitPhase: Building=1, Sealing=2, Complete=3, Aborting=4,
                  Cleaning=5, Cleaned=6, Quarantined=7
RestorePhase:    Preparing=1, Copying=2, SourceSealed=3, Ready=4,
                 Complete=5, Aborting=6, Cleaning=7, Cleaned=8,
                 Quarantined=9
CommitRetirePhase: Claiming=1, Releasing=2, Complete=3, Quarantined=4
GcPhase:         Queued=1, Claimed=2, Deleting=3, Deleted=4, Quarantined=5
StagedProviderState: Planned=1, Uploading=2, Uploaded=3, AbortPending=4,
                     Aborted=5, Ambiguous=6
StagedCleanupState: Owned=1, DeletePending=2, Deleted=3, Quarantined=4
GcClaimState:    Candidate=1, Claimed=2, Complete=3, Quarantined=4
```

These numeric values are the complete initial registry, not implementation
suggestions. Unknown kinds, states, or phases fail closed. Adding a durable
discriminant requires a schema contract change plus golden-byte encode/decode,
reopen, and unknown-value rejection tests.

`RevisionRef.reference_owner_id` is discriminated by its kind:

```text
Path:
  workspace_incarnation_id | u32(path_bytes) | ordered, NUL-separated path bytes
Commit:
  commit_id
RevisionDependency:
  child_artifact_revision_id
```

`CommitConsumer.consumer_owner_id` is likewise discriminated:

```text
WorkbenchHead:
  workspace_incarnation_id
Tag:
  workspace_incarnation_id | u16(tag_name_bytes) | tag_name
Lease:
  operation_id
ChildCommit:
  child_commit_id
```

Ids in object keys are lowercase hex with no prefix: 32 characters for
logical-shard/root/revision ids. `CommitId` uses 64 lowercase hex characters
at API/object-manifest boundaries. `object_index` is exactly 16 lowercase
hexadecimal characters. No UUID punctuation, variable-width
numeric field, or process address is permitted.

## Placement Authority

Root placement is not a shard-local metadata family. The control plane owns:

```text
RootPlacement
  key: root_id
  val: logical_shard_id, placement_generation, RootPlacementLifecycle
```

The selected logical shard installs:

```text
RootFence
  key: root_id
  val: logical_shard_id, placement_generation, activation_state
```

`RootPlacement` exists before the root's first write. A populated root never
changes logical shard, because object ownership includes that shard id.
Physical ownership of the same logical shard may move between processes.
The control-plane record uses `RootPlacementLifecycle`; the shard-local
`RootFence.activation_state` independently uses `RootActivationState`. The two
enums are not interchangeable, and both reject unknown values on reopen.

Each metadata command carries the placement generation and owner epoch and
validates them against the local `RootFence` and shard-owner fence in the same
physical transaction as the metadata commit. A router's remote control-plane
lookup is not part of that atomic batch.

## Shard-Local Families

```text
System
  key: system key
  val: schema and shard lifecycle records, applied recovery LSN,
       recovery chain digest

RecoveryOutbox
  header key: 0x00 | recovery_lsn
  chunk key:  0x01 | recovery_lsn | chunk_index
  val: canonical mutation plus typed deterministic result evidence,
       previous/current chain digest, and strict storage framing

RootFence
  key: root_id
  val: installed placement generation and activation state

WorkspaceCurrent
  key: root_id | len(workbench_id) | workbench_id
  val: workspace_incarnation_id, workspace_revision,
       Staging | Visible | Retired, owning operation,
       created_version, modified_version

WorkspaceIncarnationClaim
  key: root_id | workspace_incarnation_id
  val: stable workbench_id

  The claim is created atomically with a direct Workbench create or restore
  staging marker and is never deleted. It prevents two names from sharing the
  same PathCurrent namespace and enforces never-reused incarnation identities.

PathCurrent
  key: root_id | workspace_incarnation_id | normalized_relative_path
  val: PathEntry: generation, artifact_revision_id, logical_size,
       body_digest_uri, manifest_digest_uri, dependency_count,
       dependency_depth, content_type, producer, manifest_id,
       typed_index_projection

ArtifactRevision
  key: root_id | artifact_revision_id
  val: logical_size, body_digest_uri, manifest_digest_uri, block_count,
       dependency_count, dependency_depth, dependency_digest,
       content_type, Available | Deleting | Deleted | Quarantined,
       reference_epoch, strong_reference_count, last_zero_ref_version,
       created_version

ArtifactRevisionClaim (reserved key inside the artifact_revision tree)
  key: root_id | 0xff | artifact_revision_id
  val: owning publish operation_id

  Begin-publish atomically creates this in-flight exclusive claim; a second
  begin with a different operation id fails while it exists. Staged rows
  derive permanent object keys from the revision id alone, so without the
  claim two operations could own identical provider keys and an aborted
  loser's cleanup could delete the winner's published objects. The claim is
  deleted in the same command that publishes the revision or finishes the
  owning operation's cleanup. A quarantined operation keeps its claim
  fail-closed: its provider-side object state is unresolved, so the revision
  identity stays unclaimable until `ReconcileQuarantinedArtifactPublish`
  resolves the operation under an operator verdict and releases the claim in
  the same command that transitions it to `Cleaned`.
  Exact revision keys are 32 bytes, so the 33-byte discriminated key can
  never collide with one.

RevisionRef
  key: root_id | reference_kind | reference_owner_id | artifact_revision_id
  val: reference_epoch_at_add, created_version

ArtifactManifest
  key: root_id | artifact_revision_id | object_index
  val: physical_owner_revision_id, physical_object_index,
       object_key, logical_offset, object_offset, length, digest_uri,
       optional append segment

Commit
  key: root_id | commit_id
  val: source_workspace_incarnation, facade identity inputs,
       tree_manifest_revision,
       tree_digest_uri = "sha256:" + lowercase_hex(member_digest),
       member_count/member_digest, unique_revision_count/revision_digest,
       parent commits, parent_count/parent_digest, producer/lineage projection,
       consumer_count, consumer_epoch, last_zero_consumer_version,
       Sealed | Retiring | Retired, retirement cursor, created_version

CommitMember
  key: root_id | commit_id | normalized_relative_path
  val: artifact_revision_id, path_generation, body_digest_uri,
       manifest_digest_uri, logical_size, dependency_count,
       dependency_depth, content_type, producer, manifest_id,
       typed projection

WorkbenchCommitHead
  key: root_id | workspace_incarnation_id
  val: commit_id, head_generation

Tag
  key: root_id | workspace_incarnation_id | len(tag_name) | tag_name
  val: commit_id, tag_generation

SnapshotRef
  key: root_id | workspace_incarnation_id | snapshot_id
  val: read_version, alias, lease_deadline, lifecycle state,
       consumer_count, consumer_epoch, annotation

SnapshotAlias
  key: root_id | workspace_incarnation_id | len(alias) | alias
  val: snapshot_id, alias_generation, terminal lifecycle projection

HistoryHold
  key: root_id | hold_kind | hold_id
  val: read_version, optional source_snapshot_id,
       Active | Releasing, created_version

CommitConsumer
  key: root_id | commit_id | consumer_kind | consumer_owner_id
  val: consumer_epoch_at_add, created_version

SecondaryIndex
  key: root_id | index_id | encoded_value
       | workspace_incarnation_id | ordered(normalized_relative_path)
  val: path_generation, compact projection

ChangeEvent
  key: root_id | commit_version | event_sequence
  val: stable workbench_id, workspace_incarnation_id,
       typed event, compact before/after projection

Operation
  key: root_id | operation_kind | operation_id
  val: input/identity/initialization digests as applicable,
       source/destination identities, phase, cursor,
       member count/digest, cleanup cursor, result or terminal error

RestoreMember
  key: root_id | operation_id | member_sequence
  val: destination path, artifact revision, path generation, row digest

StagedObject
  key: root_id | publish_operation_id | object_sequence
  val: artifact revision, object key, multipart/upload id,
       expected length/digest, provider state, cleanup state

CommandDedupe
  key: root_id | request_id
  val: command digest, deterministic result, commit_version, recovery_lsn

GcCandidate
  key: root_id | artifact_revision_id | reference_epoch
  val: last_zero_ref_version, claim state, retry and quarantine evidence

GcBarrier
  key: root_id
  val: monotonic generation used to advance a quiescent GC history floor

History
  key: source_family_tag | user_key_length | user_key
       | inverted_commit_version
  val: previous versioned value or tombstone
```

`ReadChanges` treats `(commit_version, event_sequence)` as an append-only log
position. Its opaque cursor is bound to the root, query scope, and optional
`after_commit_version`; unlike frozen search and catalog cursors, it may resume
against a later root read version. The engine seeks strictly after that event
key and streams until one visible-page lookahead is found. A version-only
`after_commit_version` excludes every sequence in that commit. Workspace
visibility is still re-evaluated at each event's own commit version, so staging
incarnations do not leak through the feed. Each event stores its stable
Workbench id, so root- and workspace-scoped feeds point-read that exact marker
and verify its incarnation instead of scanning every marker at each event
version. Repeated events for one Workbench in one commit share the marker
result. An `after_commit_version` newer than the fenced root version fails
closed.

Both scopes currently seek the root-wide `ChangeEvent` keyspace. A
workspace-scoped feed filters on the embedded stable Workbench id after that
seek, so a sparse Workbench on a hot Root still does work proportional to the
intervening root events. A future per-Workbench event index must be an atomic,
repairable projection before that workload can claim Workbench-local seek.

The current format does not truncate `ChangeEvent` or the visibility History
needed to interpret it. A cursor is therefore not a retention lease. Any future
event/history GC must first define one shared consumer frontier and a typed
expired-position failure.

The outbox covers the three real shard-local write entrypoints: metadata
commands, monotonic lease-clock observations, and physical-owner epoch
advancement. Replays invoke those same entrypoints; there is no second
namespace apply state machine. Reopen verifies contiguous LSNs, the complete
hash chain, declared/missing/orphan chunks, and the `System` tail. This is
local recovery material only: no remote consumer ACK, shared-log replication,
checkpoint install/replay, truncation protocol, or fsck is currently wired.

## Namespace And Workspace Visibility

`PathCurrent` is the only namespace truth. `PathEntry` is compact:

```text
path_generation
artifact_revision_id
body_digest_uri
manifest_digest_uri
logical_size
dependency_count
dependency_depth
content_type
producer/provenance summary
created_version
modified_version
typed index projection
```

There is no canonical inode, dentry, parent pointer, link count, directory
record, or fallback path index.

Clients attach queryable fields to `ArtifactDescriptor.index_fields` on the
artifact publication request. The workspace executor validates and encodes
that typed projection, and final publication updates `PathCurrent` and the
corresponding `SecondaryIndex` rows in the same metadata command. There is no
separate namespace-index registration RPC or second mutation path.

A cold exact artifact lookup needs one logical `WorkspaceCurrent` point read
and, only when its state is `Visible`, one logical `PathCurrent` point read.
`PathEntry` atomically retains the immutable revision fields required to shape
complete `PathMetadata`; exact stat/list reads never fan out to
`ArtifactRevision`. A client/router may cache the incarnation because `Visible`
is immutable, but every physical read batch still validates the active
`RootFence`, owner epoch, and commit clock.

A live direct-child listing performs the marker check followed by one
delimiter-aware ordered prefix-scan path. Each metadata call returns at most
255 logical items; a protocol page with a larger limit advances the exclusive
marker through multiple bounded store calls. Each store common-prefix rollup
becomes one storage-neutral `Prefix` page item; an exact artifact at the same
logical child wins. Recursive listing emits only artifact items. When the
requested prefix can itself be a published file, the metadata listing also
performs one exact-prefix point read after descendant EOF; the Workbench
direct-child adapter does not expose the requested path as its own child. No
listing performs per-entry revision reads.

Each list page reports the exact `RootReadContext.read_version`. Continuations
must send that version as an expected fence; an owner that has advanced returns
a typed `ReadVersion` precondition failure instead of serving a mixed-version
page. The Workbench cursor wraps that fence together with a digest of the
workbench, normalized prefix, and live-or-snapshot selector plus the last child
anchor. Catalog cursors are metadata-owned and bind the query digest, read
version, and field anchor. A caller without an incoming cursor may restart a
whole bounded collection after version drift. These fences detect drift; they
do not authorize an arbitrary historical read without a live history hold.

Directories are implicit. Statting an implicit directory requires a bounded
prefix-existence probe; it is not claimed to be a point lookup. Empty directory
identity is unsupported. A synthesized directory's generation is the visible
workspace revision at the read version; the adapter does not invent stable
inode or POSIX timestamps.

The five Workbench sections are virtual:

```text
input
scripts
outputs
logs
metadata
```

Their exact paths cannot be shadowed. They exist whenever the workspace is
`Visible`, even with no descendants, and use the same synthesized generation
rule.

`WorkspaceCurrent.state == Visible` is the publication marker. `Visible` is
terminal with respect to visibility: no operation changes it to
`Retired`. `Staging` rows are absent from point, list, search, aggregate,
catalog, snapshot, and watch results. Secondary-index consumers recheck the
workspace incarnation/state at their read version. Events are visible
according to the workspace state at the event's commit version; staging
produces no user event. Restore emits one publication event in its final marker
command.

A never-reused `WorkspaceIncarnationId` prevents abandoned or retired rows from
becoming visible when a workbench name is reclaimed. A previously visible
Workbench id is not reused. A failed staging claim may be retried with a
new incarnation only after its old operation reaches terminal cleanup.
`Retired` is therefore reachable only as `Staging -> Retired` for a failed or
aborted unpublished incarnation; `Visible -> Retired` is invalid in format
version 1.

## Metadata Command

All durable mutations flow through one bounded `MetadataCommand`:

```text
schema_id
root_id
logical_shard_id
placement_generation
owner_epoch
request_id
command_digest
read_version
predicates[]
mutations[]
history_projection[]
event_projection[]
deterministic_result
```

Before any mutation, the shard validates:

- exact schema marker and active local `RootFence`;
- current owner lease and monotonic epoch;
- `read_version` exactly equals the current shard commit clock, so the command's
  commit version is deterministically `read_version + 1`;
- request-id replay or mismatch;
- expected workspace incarnation/revision and path generation;
- artifact revision state, reference epoch, and strong-reference count;
- operation, seal, snapshot, commit, and hold transitions;
- every other command predicate.

An unrelated intervening commit makes a write's read version stale and the
caller must rebuild the command. An exact request-id replay is checked before
that fence and returns the stored result. Reusing a request id with different
inputs is an error. A failed predicate applies no mutation.

Ordinary put/replace/remove has a fixed upper bound on predicates and mutations
apart from bounded manifest and index rows. It performs no namespace prefix
scan.

## Artifact Revisions And Strong References

Each successful body publication creates a never-reused immutable revision.
Multiple paths, commits, and same-root restores may share it.

Physical object identity includes every ownership boundary:

```text
nokv/artifacts/{logical_shard_id}/{root_id}/{artifact_revision_id}/blocks/{object_index}
```

The ids use a canonical object-key-safe encoding. Physical process addresses
and owner epochs never appear. SHA-256 is integrity/logical identity, not
global physical ownership. Cross-shard import creates destination-owned
revisions and copies bytes before publication. Global physical deduplication is
outside this schema.

Every strong reference has one `RevisionRef` row. The same command that
adds/removes a reference updates `ArtifactRevision.strong_reference_count` and
increments `reference_epoch`.

Reference creation requires `ArtifactRevision.state == Available` with the
expected epoch. The important reference kinds are:

```text
Path(workspace_incarnation, normalized_path)
Commit(commit_id)                  # one per unique revision
RevisionDependency(child_revision) # one per distinct owner of reused blocks
```

When the count becomes zero, that command stores
`last_zero_ref_version` and creates a `GcCandidate` keyed by the new
`reference_epoch`. A later reference addition increments the epoch and makes
the old candidate stale.

## Upload And Append

Publication is object-first and metadata-last. Before upload, a
`PublishOperation` and exact `StagedObject` ledger own all object keys,
multipart ids, lengths, and digests.

The mutually exclusive operation transitions are:

```text
Uploading -> Finalizing -> Published
Uploading -> Aborting -> Cleaning -> Cleaned
                                  -> Quarantined -> Cleaned # operator reconcile
Finalizing -> Aborting # fenced proof of no path/dedupe publication
```

Finalization first CASes `Uploading -> Finalizing`; cleanup first CASes
`Uploading -> Aborting`, so only one can win. The metadata publication command
changes `Finalizing -> Published` atomically with the new path/revision. A
crash in `Finalizing` is resumed from the ledger; cleanup may take it over only
through the shown `Finalizing -> Aborting` CAS after proving that no
path/dedupe publication exists. Publication and takeover both change the same
operation row, so one wins. Cleanup may mutate the ledger or issue external
DELETE only while it owns `Aborting`/`Cleaning`.

A late upload completion must observe the operation state; after abort it joins
cleanup instead of publishing. Ambiguous multipart completion, late PUT, or
DELETE remains ledger-owned and `Quarantined` until reconciled. Object listing
is never used to discover staged ownership.

Reconciliation is operator-driven, never scanner-driven. The operator verifies
provider-side object state for the operation's staged keys out-of-band and
presents one of two verdicts through
`ReconcileQuarantinedArtifactPublish`: every staged key verified absent with
the revision unpublished, or the revision already published by another
operation (staged keys are then that revision's live objects and only the
quarantined operation's private bookkeeping rows are removed). Each durable
command pins the verdict against the authoritative `ArtifactRevision` row and
refuses loudly on contradiction; the final command rewrites the terminal error
to `OperatorReconciled` with an evidence chain binding the original quarantine
evidence and the operator's verification transcript, transitions
`Quarantined -> Cleaned`, and releases the revision claim when this operation
owns it.

The final metadata command creates the `ArtifactRevision` as `Available`, its
manifest, the first path reference, `PathCurrent`, workspace revision, indexes,
event, and dedupe result. A failed upload is invisible. A response-loss retry
returns the stored result without allocating another revision.

Append stores immutable segments in the new revision manifest and atomically
advances the path generation. A manifest row names the revision that physically
owns each referenced block and that owner's local `physical_object_index`.
The `ArtifactManifest` key's `object_index` is only the ordered row position in
the child revision; GC never substitutes it when reconstructing a physical key.

If a revision reuses any block owned by an older revision, publication
adds one `RevisionDependency(child_revision)` strong reference to every
distinct owner revision. The `ArtifactRevision` seals the dependency count and
digest. A revision may depend on at most 64 distinct owner revisions and
the sealed dependency graph may be at most eight revisions deep. Publication
that would exceed either limit rematerializes the complete body under the new
revision and records zero dependencies. GC of the child deletes its own objects
first, then releases the bounded sealed dependency set; it cannot delete a base
while the child remains readable. Reads follow the manifest's direct physical
owner ids and never recursively resolve the dependency graph.

`PathMetadata` exposes the current revision's sealed `dependency_count` and
`dependency_depth` as bounded, typed SDK metadata. Native append validates those
values against the complete base manifest before `Begin`; an over-limit next
closure uses the same publication pipeline with a fully rematerialized,
new-revision-owned body and an empty dependency set. The request remains an
`Append` generation CAS rather than becoming an unconditional replace.

`ArtifactRevision.body_digest_uri` and `PathEntry.body_digest_uri` cover the
complete resulting body. The Workbench `digest` output is an adapter
projection of the appended delta's SHA-256; it is intentionally separate from
the whole-body digest.

## Snapshot Lifecycle And History Holds

A leased snapshot has a `SnapshotRef`, an optional exact `SnapshotAlias`, and
an `Active` `HistoryHold` for the same read version, created atomically.
There is exactly one current alias row within a workspace incarnation. Minting
the same name again atomically advances
`alias_generation` and makes the latest mint the name's resolution, even if the
older snapshot remains active. Renewal and retirement events never move the
alias, and a terminal latest snapshot does not fall back to an earlier mint. An
older snapshot remains addressable by numeric id. Name-based read/renew/retire
predicates the exact alias id/generation together with the selected
`SnapshotRef`, so concurrent remint cannot redirect an in-flight command.
`expired` is a derived status while the durable lifecycle remains active.

Renewal is extend-only and CASes an active record. It may revive an expired but
not-yet-claimed snapshot, matching the Workbench facade. The reaper waits the
configured maximum clock-skew grace, then atomically changes
`Active -> ReapClaimed` and releases the `HistoryHold`. Renewal after that CAS
fails. Retirement uses the same fence.

An in-progress restore/fork from a snapshot creates an exact
`HistoryHold(Restore, operation_id)` carrying the source snapshot id and
atomically increments `SnapshotRef.consumer_count` and `consumer_epoch`.
Retire/reap predicates zero consumers at the expected epoch; a live consumer
returns the stable `ForkRetentionActive` facade error. Consumer release removes
the hold and changes the same count/epoch, so source attachment and retirement
have one CAS winner. `Reaped` and `Retired` are terminal.

Lease deadlines use the shard's persisted lease clock. An owner that observes
wall-clock regression below its persisted high-water does not reap until time
has caught up plus the skew grace.

History is retained according to the minimum active `HistoryHold`, in-flight
recovery floor, and configured diagnostic floor. Durable commits retain exact
revisions rather than pinning unbounded history.

## Commit Closure

`CommitId` is root-global. The stable Workbench commit identity is the facade
id; a sealed record also binds it to the server-derived workspace tree
digest.

Commit construction:

Before metadata preparation, the Agent adapter computes:

```text
projection_input_digest =
  sha256(
    "nokv.workbench.run_manifest.projection_input.v1\0"
    || len64be(workbench_id)          || workbench_id
    || len64be(workbench_path)        || workbench_path
    || len64be(content_digest_uri)    || content_digest_uri
    || len64be(canonical_manifest)    || canonical_manifest
    || len64be(manifest_digest_uri)   || manifest_digest_uri
    || commit_identity                                      # exact 32 bytes
  )
```

These are exactly the caller-known `run_manifest.v1` projection inputs except
`committed_at_unix_seconds`, which the first metadata owner supplies and stores.
`replace` is not a projection input and remains a separate exact request field.
`BuildCommitOperation.initialization_digest` uses the exact domain separator
`nokv.build-commit.initialization.v5\0` and binds the projection-input digest,
frozen source/head, explicit digests, tree revision, `replace`, run-manifest
condition, durable time, producer, lineage, and ordered parents. Changing any
of them is an operation input mismatch.

```text
1. create Operation(BuildCommit), freeze the complete exact request including
   the projection-input digest, `replace`, the expected head, source
   incarnation/read version, and exact run-manifest publication condition,
   retain the first owner-observed
   `committed_at_unix_seconds`, and create HistoryHold(read_version); retries
   use that durable request and time even after process loss or wall-clock
   advance
2. upload the canonical run manifest under CommitStaging; one command creates
   its hidden ArtifactRevision plus Commit RevisionRef and records an immutable
   binding of incarnation, revision, logical size, body/manifest digests, and
   content type in the build operation, without writing PathCurrent
3. scan the frozen workspace in canonical path order, replacing or inserting
   metadata/run_manifest.json as one virtual member backed by that staged
   revision
4. write CommitMember rows and build member_count/member_digest
5. add one Commit RevisionRef per unique revision; the staged run-manifest ref
   already exists and is counted exactly once
6. for every unique parent, add
   CommitConsumer(parent, ChildCommit, child_commit_id) against the parent's
   exact Sealed state/consumer epoch and build parent_count/parent_digest
7. verify the revision and parent count/digest pairs against their exact rows
8. CAS BuildCommit Building -> Sealing against all three closure digests
9. one command publishes PathCurrent(metadata/run_manifest.json), its path ref,
   WorkspaceCurrent, the sealed Commit, WorkbenchCommitHead, and the old/new
   CommitConsumer rows/counts/epochs; it also releases the replaced path ref,
   changes Sealing -> Complete, emits the event, and releases HistoryHold
```

The `Commit` seal is the closure proof. `CommitMember` path membership, unique
`RevisionRef(Commit, commit_id, revision)` rows, and unique outbound
`CommitConsumer(parent, ChildCommit, child_commit_id)` rows must match the
member, revision, and parent count/digest pairs. A partial build has no
`Commit` record and remains invisible but retained by its `HistoryHold` and
already-created revision/parent references. Recovery resumes or removes every
set from the operation cursor.

Operation lookup precedes all live workspace, head, and run-manifest reads.
Fresh construction alone evaluates those live preconditions. An exact retry
authenticates the complete durable request and returns the original terminal
result even when a later replacement commit has advanced the current head and
path. Terminal success reconstructs and verifies the canonical envelope against
the build operation's manifest binding and the exact durable publish-operation
result; it never treats current `PathCurrent(metadata/run_manifest.json)` as
replay authority. This guarantee requires retaining both terminal operation
rows. The current schema has no terminal-operation GC; any future operation GC
must first add an explicit retention/tombstone contract that preserves exact
replay.

`CommitStaging` is authorized only for `metadata/run_manifest.json` and
`RestoreStaging` only for `metadata/restore_manifest.json`. Generic visible
publication and direct removal reject both paths. This prevents a second
publication route from splitting the typed commit/restore state from its stable
Workbench projection.

Publication and cleanup are mutually exclusive:

```text
Building -> Sealing -> Complete
Building -> Aborting -> Cleaning -> Cleaned | Quarantined
Sealing  -> Aborting # only after fenced proof that no Commit/head/dedupe exists
```

Cleanup must own `Aborting` before removing any member, revision reference,
parent consumer, or hold. The final seal command and cleanup both CAS the same
operation phase, so cleanup cannot tear down a published commit and publication
cannot revive a cleaned build. A crash in `Sealing` resumes publication when
its exact commit or dedupe result exists; otherwise only the fenced takeover
may abort it.

Build, cleanup, and retirement select the largest prefix whose
fully derived metadata transaction fits the serving store limits. A valid new
row must fit by itself. A legacy row that does not fit fails closed in
`Quarantined` and retains its hold for operator repair. NoKV does not qualify
automatic repair of that state.

Every Workbench head, tag, restore/fork lease, and child commit owns one exact
`CommitConsumer` row. Adding or removing one updates
`Commit.consumer_count` and increments `consumer_epoch` in the same metadata
command. Consumer creation requires `Commit.state == Sealed` at the expected
epoch.

Tag movement/deletion never retires a commit. Explicit retirement does not
depend on a preceding scan: one command predicates `Sealed`,
`consumer_count == 0`, and the expected `consumer_epoch`, then changes
`Sealed -> Retiring`. A concurrent head, tag, lease, or child creation changes
the same commit row and invalidates that CAS; after `Retiring`, no new consumer
can attach.

A durable `CommitRetire` operation then releases the sealed unique revision set
and every outbound parent consumer in bounded batches with typed cursors.
Recovery resumes each cursor and rechecks both seals. Only after all members,
indexes, revision refs, and parent consumers are released does one command
publish `Retired`.

## Restore Closure

Restore is same-root/logical-shard and destination-creating. It never rolls
back a visible workspace in place.

The restore operation identity is deterministic and does not include
initialization bytes, because the stable Workbench restore manifest itself
contains the operation id:

```text
identity_digest =
  sha256(
    "nokv.restore.operation.v2\0"
    || root_id                                      # 16 bytes
    || u32be(source_workbench_id_bytes)
    || source_workbench_id                           # exact validated ASCII bytes
    || source_workspace_incarnation_id              # 16 bytes
    || u8(source_kind)                              # Snapshot=1, Commit=2
    || source_identity                              # u64be snapshot id or 32-byte commit id
    || u32be(destination_workbench_id_bytes)
    || destination_workbench_id                     # exact validated ASCII bytes
    || destination_workspace_incarnation_id         # 16 bytes
  )
operation_id = first_16_bytes(identity_digest)
```

Snapshot aliases are point-resolved to their numeric id before `PrepareRestore`;
the internal restore DTO rejects aliases. The `Operation` row stores the source
workbench, source incarnation, concrete selector, destination identity, complete
32-byte `identity_digest`, and a separate 32-byte `initialization_digest`.
`operation_id` must equal the first 16 bytes of `identity_digest`. The same
short id with a different identity digest is a typed collision, and the same
identity with a different initialization digest is a typed request mismatch.

Initialization is canonicalized before hashing:

```text
initialization_digest =
  sha256(
    "nokv.restore.initialization.v3\0"
    || identity_digest
    || u32be(1)                                     # one reserved projection
    || u8(1)                                        # put entry
    || u32be(path_bytes) || "metadata/restore_manifest.json"
    || u32be(encoded_path_entry_bytes) || encoded_path_entry
  )
```

The encoded path entry must exactly match the descriptor sealed into the
restore operation. An exact identity and initialization digest resumes or
returns the same terminal result.

The durable state machine is:

```text
1. create Operation(Restore), destination WorkspaceCurrent(Staging)
   with a fresh incarnation, and either:
     - CAS the exact `Active` SnapshotRef whose lease deadline is later than
       the persisted shard lease clock, increment its consumer count/epoch,
       and create a separate HistoryHold(source snapshot/read_version), or
     - add a `CommitConsumer(Lease, operation_id)` against the exact
       `Sealed` Commit state/consumer epoch
2. for each source entry, one bounded batch writes:
     - destination PathCurrent under the new incarnation
     - its Path RevisionRef
     - ordered RestoreMember with row digest
     - the next source cursor and member sequence
3. at end-of-source, record EOF and seal member_count/member_digest
4. recovery verifies the ordered member index and source closure:
     - commit source must match the sealed Commit member count/digest
     - MVCC source is rescanned at its held read version and must produce
       the same count/digest
   then changes operation to Ready
5. one final command predicates the exact Ready seal and Staging marker,
   CASes Restore Ready -> Complete, changes the workspace to Visible,
   emits one restore event, and releases the source hold/consumer
```

No destination path is visible before step 5, but its strong reference protects
the object during staging. Abort/cleanup is driven by `RestoreMember`, not by a
path or object listing, and removes each staged path/reference before the
workspace name can be reclaimed with another incarnation. Exact retries return
the terminal operation result.

Restore publication and cleanup also share one phase fence:

```text
Preparing -> Copying -> SourceSealed -> Ready -> Complete
Preparing | Copying | SourceSealed | Ready
  -> Aborting -> Cleaning -> Cleaned | Quarantined
```

Cleanup first CASes the exact observed nonterminal phase to `Aborting`; only
then may it remove staged rows or change the destination
`WorkspaceCurrent(Staging) -> Retired`. In particular, final publication and a
Ready-state abort both CAS the same operation record, so exactly one can win.
After the cleanup cursor proves every destination path/reference is removed,
its terminal command releases the source `HistoryHold` plus snapshot consumer,
or the source commit consumer, exactly once. `Quarantined` retains that source
for operator repair. NoKV does not qualify automatic reconciliation.

New copy commands admit a member only after its worst valid single-member
cleanup command also fits the serving store limits. Cleanup never touches a
`Visible` incarnation.

## Garbage Collection State Machine

A candidate may be claimed only when:

- `strong_reference_count == 0`;
- its key matches the current `reference_epoch`;
- the history floor is newer than `last_zero_ref_version`;
- no publish operation can still create the revision;
- the caller is the current fenced owner of the revision's logical shard.

The claim atomically changes `Available -> Deleting` with the expected
reference epoch. Every reference addition requires `Available`, so it cannot
race past that claim.

After all manifest objects are confirmed absent, the revision becomes
`Deleted` and its manifest/candidate rows may be pruned according to audit
retention. An ambiguous provider result changes it to `Quarantined`; neither
reference addition nor metadata deletion is allowed until reconciliation
proves all objects present or absent and performs an explicit state transition.

The required fsck recomputes strong-reference counts and seal digests from
paths, commits, operations, holds, revisions, and manifests. It never treats
object-store listing as namespace truth. This is a qualification requirement,
not a claim that the current runtime already exposes an fsck implementation.

## Forbidden Families

The schema must not introduce:

```text
inode_current
dentry_current
parent_index
path_index as fallback namespace truth
xattr
hardlink or symlink records
fork_shadow or lazy-overlay namespace records
```

Any new authoritative family must update this contract, specify its ownership,
visibility, retention, recovery, and GC rules, and include point-read, scan,
logical command amplification, and fault-injection evidence.
