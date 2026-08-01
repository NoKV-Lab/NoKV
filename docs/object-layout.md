<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Object Layout

NoKV keeps artifact bytes outside Holt. Metadata stores compact immutable
revision and manifest records; an S3-compatible provider stores the blocks.

## Permanent Block Identity

```text
nokv/artifacts/{logical_shard_id}/{root_id}/{artifact_revision_id}/blocks/{object_index}
```

- ids are lowercase fixed-width hexadecimal;
- `object_index` is a fixed-width hexadecimal counter;
- physical process addresses, owner epochs, bucket endpoints, and Workbench
  names are absent;
- an object key is never reused for different bytes.

The logical shard, root, and revision remain stable when physical ownership
moves.

## Artifact Revision

Each successful body publication creates a never-reused
`ArtifactRevisionId`. Its metadata records:

```text
logical_size
whole_body_digest_uri
manifest_digest_uri
block_count
dependency_set
content_type
producer_and_provenance
lifecycle_state
strong_reference_count
reference_epoch
```

Content digests prove identity and integrity. They do not imply provider-wide
physical deduplication.

## Manifest

`ArtifactManifest(root, revision, object_index)` maps an ordered logical range
to:

```text
physical_owner_revision_id
physical_object_index
object_key
logical_offset
object_offset
length
digest_uri
optional_append_segment
```

The manifest-key `object_index` is the row's position in the child revision;
`physical_object_index` is the block number inside the named physical owner.
Append may therefore renumber logical rows without changing borrowed or
newly-uploaded object identities. The manifest is immutable after publication.
Range plans are derived from it and may be cached by revision identity.

## Publication

```text
allocate operation and revision
  -> upload immutable blocks
  -> verify size, digest, and provider completion
  -> one fenced metadata command publishes manifest and references
  -> PathCurrent becomes visible
```

Object upload success alone never creates a namespace entry. Metadata
publication failure leaves operation-owned staged objects for explicit,
recoverable cleanup.

Create, replace, append, and edit retain distinct metadata predicates. An exact
request retry returns the same deterministic result and never allocates a
second published revision.

## Reused Blocks

A new revision may reference blocks physically owned by older revisions. The
new revision seals one dependency reference for every distinct owner revision.
Those dependencies remain strong until the child revision is deleted.

Dependency count, depth, and digest are bounded and verified before the child
becomes available. This prevents an append or sparse update from losing reused
blocks during garbage collection.

## References And Deletion

Current paths and durable commit members own exact `RevisionRef` rows.
Reference add/remove atomically changes:

```text
reference row
strong_reference_count
reference_epoch
zero-reference candidate, when count reaches zero
```

GC may claim a revision only when:

- state is `Available`;
- strong reference count is zero;
- candidate epoch matches the current reference epoch;
- retained metadata history and active operation holds permit deletion;
- the current fenced shard owner performs the claim.

A claimed revision rejects new references. Provider deletion then advances
through durable states. Timeout or uncertain completion is quarantined and
reconciled; object listing is never used as reachability truth.

## Provider Boundary

AWS S3, RustFS, MinIO, and Ceph RGW use the same provider-neutral interface:

- immutable put or multipart completion;
- ranged get;
- head/integrity evidence;
- idempotent delete with explicit ambiguous-outcome handling.

Provider credentials and endpoints are deployment configuration, not durable
object identity.

See [Metadata Schema](./metadata-schema.md) for exact metadata families and
[RustFS Backend](./rustfs.md) for the local S3-compatible profile.
