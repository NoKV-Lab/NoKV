<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Object Layout

NoKV separates namespace metadata from file bodies. Holt stores compact body
descriptors and manifests; immutable blocks live in the configured object
provider.

The responsibility boundary is explicit:

- **NoKV** owns logical object identity, object-first publication, namespace
  reachability, generation fencing, snapshot retention, and GC eligibility.
- **The object provider** owns physical byte durability, replication,
  availability, encryption, and failure-domain behavior.

Using a replicated S3-compatible service does not by itself make NoKV metadata
highly available. Conversely, metadata recovery cannot restore body bytes that
the provider has lost.

## Chunk Layout

Files are split into immutable object blocks:

```text
file inode
  -> body descriptor
  -> chunk manifests
  -> object blocks
```

Default logical sizes are:

```text
chunk_size = 64 MiB
block_size = 4 MiB
```

NoKV derives canonical block keys from metadata-issued identity:

```text
blocks/<mount>/<inode>/<generation>/<chunk>/<block>
```

The SDK, FUSE data path, or another client-side executor may perform the actual
PUT. The mount, inode, generation, chunk, and block components must still match
the identity validated during metadata publication.

Blocks are never modified in place. A replace, overwrite, or partial-write
publication creates a new generation and atomically switches the visible
manifest in the owning metadata shard.

## Body Descriptor

```text
producer
digest_uri
size
content_type
generation
manifest_id
base_generation
chunk_size
block_size
```

`manifest_id` is provider-neutral and stable for the artifact publish request;
it is not a physical object key. `base_generation` identifies the generation
used by sparse manifests for chunks that were not rewritten. A value of `0`
means the generation is self-contained.

`digest_uri` is a compact integrity summary:

- SDK artifact uploads normally use `sha256:<content-digest>`.
- Chunk block entries use `xxh3-64:<block-checksum>` so the write path does not
  require a cryptographic digest per block.
- FUSE write sessions use `manifest-sha256:<manifest-digest>` so publication is
  proportional to the changed manifest rather than a full-body reread.

These digests support NoKV integrity checks. They do not replace the object
provider's own durability and corruption-detection policy.

The same logical layout works with AWS S3, RustFS, MinIO, and Ceph RGW. Use
`--object-backend rustfs` for the repository's local RustFS shape or
`--object-backend s3` for another S3-compatible provider. See
[RustFS Backend](./rustfs.md).

## Shard-Local Publication Rule

Artifact publication is staged:

```text
1. allocate or resolve the target inode and generation
2. PUT every immutable object block
3. commit inode, dentry projection, body summary, and chunk manifests
4. expose the new generation through the namespace
```

Step 3 is one `MetadataCommand` in the shard that owns the path. This makes the
new generation atomically visible inside that shard. It is not a cross-shard
transaction, and it does not make a multi-object PUT atomic inside the object
provider.

If a PUT fails, metadata publication must not expose the incomplete generation.
If all PUTs succeed but metadata publication fails, the uploaded objects are
unreachable from the namespace. The caller can pass its known staged-object set
to the explicit cleanup helper.

A process can also crash after uploading bytes but before retaining that staged
set. Such unknown orphans are not discoverable from metadata alone because they
were never referenced. Reclaiming them requires a provider listing/scrub policy
that compares physical keys with NoKV reachability and applies an appropriate
safety window.

## Replacement, Removal, And GC

When a replace or remove command makes an old generation unreachable, it also
enqueues that generation's owned blocks in the shard-local durable GC queue.
The current local service exposes an explicit cleanup API and a background
object-GC worker. `nokv-server` starts the worker for every hosted shard; a FUSE
mount is a client and does not own server-side GC.

GC remains conservative around readers:

- active snapshot pins protect the historical metadata and blocks required by
  their read version;
- retiring or expiring a pin permits later reclamation;
- each GC record stores its enqueue time;
- the background worker applies a read-lease grace window before deleting an
  eligible object;
- the explicit cleanup path can use a zero grace window for tests and deliberate
  manual recovery.

GC queues and snapshot pins are metadata-shard local. A fleet operator must run
and observe cleanup for every shard; one shard's progress is not a fleet-wide GC
result.

Metadata history follows the same retention floor. Active snapshot pins define
the oldest read version that must remain reconstructible. Cleanup keeps the
per-key anchor required by that version and can remove older history; with no
pins, all unneeded historical records may be removed. `nokv-server` starts the
history-GC worker alongside object GC for each hosted shard.

## Chunk Manifest

Each real `chunk_manifest` row stores the slice stack for one logical chunk.
Newer slices overlay older slices, allowing a partial write to publish only
dirty blocks while reusing unchanged blocks through the generation chain:

```text
chunk_index
logical_offset
len
slices:
  slice_id
  logical_offset
  len
  blocks:
    object_key
    logical_offset
    object_offset
    len
    digest_uri
```

Readers resolve the body descriptor and manifests into an immutable range-read
plan, fetch the required object ranges, and assemble the requested file range.
The optional local block cache is a read-through acceleration layer keyed by
object range; cache placement is not durable metadata truth.

## Operational Consequences

- Size object-provider capacity for both live data and temporary unreachable
  uploads, recovery logs, and metadata checkpoints.
- Treat provider lifecycle rules as dangerous unless they preserve NoKV's
  snapshot and GC retention requirements.
- Benchmark small-file PUT/GET pressure separately from metadata operations;
  sharding metadata cannot remove an object-provider bottleneck.
- Validate provider consistency and durability assumptions for the intended
  deployment. NoKV does not add body replication beneath an S3-compatible API.
