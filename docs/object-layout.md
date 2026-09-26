<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Object Layout

NoKV keeps artifact bytes outside the serving Holt 0.8.6 adapter. Metadata
stores compact immutable revision and manifest records; an S3-compatible
provider stores the blocks.

## Permanent Block Identity

```text
nokv/artifacts/{logical_shard_id}/{root_id}/{artifact_revision_id}/blocks/{object_index}
```

- ids are lowercase fixed-width hexadecimal;
- `object_index` is a fixed-width hexadecimal counter;
- physical process addresses, owner epochs, bucket endpoints, and Workbench
  names are absent;
- a published block key is never reused for a different payload;
- an abandoned stable-append key may only move to its permanent zero-byte seal,
  as described below; it cannot be reused for another artifact.

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

For [durable append](append.md), the logical action ID survives across physical
publication attempts. The current child must be durably cleaned before a new
child/revision can be admitted. A failed child's keys are not reused by that
successor. A successful logical receipt is installed atomically with the
publication and remains queryable independently of the current live path.

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

## Failed Append Keys

Stable append cleanup permanently occupies every registered failed-child key
with an empty object. It conditionally creates an absent key with
`If-None-Match: *`, or replaces an existing payload with an empty seal using
`If-Match` on its observed ETag. This path never DELETEs a key: a delayed DELETE
could otherwise remove a newer seal, allowing a late upload to recreate an
unreachable payload. A nonempty immutable create cannot replace a seal.

Only the fenced owner performs this operation after verifying the current
parent/child binding, revision reservation, and absence of a published
revision. Metadata cleanup records `Sealed`, retires the processed staging
rows, and eventually marks the child `Cleaned`. The failed child's revision
claim is retained permanently, including after a successor succeeds. A missing
key alone is not sealing proof; an uncertain result quarantines the child.

Public `operation recover` requests owner cleanup using a saved logical state
token. It neither supplies a trusted provider verdict nor writes the objects
from the caller. Generic publication reconciliation and ordinary
published-revision GC continue to use their existing deletion semantics.

These seals and revision reservations have no automatic expiration. They retain
key/metadata overhead even though the live object has zero payload bytes.
Logical receipts also remain, but do not pin historical body contents against
normal reference/hold/GC rules. With bucket versioning, replacing the current
payload does not prove removal of older versions. External expiration or
overwrite policies must not remove live seals or other NoKV-owned objects.

## Provider Boundary

AWS S3, RustFS, MinIO, and Ceph RGW can use the same provider-neutral
interface, but a provider brand or static capability flag is not evidence that
a concrete endpoint satisfies the contract.

Before business publication, write-conformance admission performs actual
writes under reserved, unreferenced system keys and verifies:

- a fresh single-PUT create-if-absent;
- exact-byte replay and different-byte collision;
- exact whole-object readback and ranged readback;
- a concurrent different-byte create race with exactly one stored winner.

The resulting receipt is bound to the exact provider handle and admitted
single-PUT size profile. A receipt from another handle, a missing receipt, or a
block larger than the admitted maximum fails before object or metadata
publication. Admission v1 deliberately does not qualify multipart creation,
completion, or abort; those paths cannot inherit a single-PUT receipt.

Stable append and serving owners advertising it additionally require the
append-sealing admission profile: conditional creation of an empty seal,
ETag-conditional replacement, rejection of late immutable writes, and stable
empty-key observations must be exercised on that handle. A generic single-PUT
receipt is insufficient. A tiered store seals the durable provider and attempts
to evict its cached copy; cache eviction is best effort. Only the durable seal
is authoritative; an evictable cache cannot provide the proof.

The provider interface also retains idempotent deletion with explicit
ambiguous-outcome handling. Public provider-admission errors and ambiguous
create/delete/seal errors do not render endpoint, bucket, or physical object-key
details.

Provider credentials and endpoints are deployment configuration, not durable
object identity.

See [Metadata Schema](./metadata-schema.md) for exact metadata families and
[RustFS Backend](./rustfs.md) for the local S3-compatible profile.
