<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# RustFS Backend

Status: optional S3-compatible provider profile, not a metadata or namespace
architecture.

RustFS is one S3-compatible object provider for NoKV artifact blocks. It does
not own namespace metadata, root placement, revision reachability, or garbage
collection policy.

## Boundary

```text
Workbench / SDK / CLI
  -> NoKV metadata and publication service
  -> S3-compatible object interface
  -> RustFS
```

NoKV sends immutable block puts, ranged gets, integrity checks, and fenced
deletes through the common object package. The NoKV metadata schema remains the
authority for paths, manifests, references, operations, and deletion
eligibility. Holt is the current serving local metadata adapter.

## Deployment Identity

Treat these values as one reviewed deployment profile:

```text
endpoint
region
bucket
access identity
credential source
TLS and certificate policy
path-style or virtual-host addressing
multipart limits
request timeout and retry policy
```

Credentials are supplied by environment or a secret manager and must not be
written into metadata, manifests, logs, examples, or object keys.

The durable block key remains:

```text
nokv/artifacts/{logical_shard_id}/{root_id}/{artifact_revision_id}/blocks/{object_index}
```

Changing the endpoint or physical RustFS node does not change that identity.

The bucket plus configured prefix contains one immutable, provider-neutral
identity marker:

```text
nokv/system/object-namespace
```

The control plane binds each `RootId` to the marker's `ObjectNamespaceId`
exactly once. Routes carry that ID, and the Holt root fence persists it in the
same metadata authority that enforces placement generation and owner epoch.
Endpoint, bucket, prefix, and credentials remain deployment configuration; NoKV
does not copy provider-specific values into metadata.

Consequently, two healthy RustFS prefixes are not interchangeable. A process
configured with the wrong prefix observes a different or absent marker and is
rejected before it can publish a route or use that provider for artifact or
lifecycle operations. A changed endpoint is safe only when it resolves to the
same durable namespace marker.

## Required Bucket Behavior

Before serving writes, verify:

- the configured identity can put, head, range-read, and delete inside the
  exact NoKV prefix;
- multipart upload, completion, abort, and completed-object head behave as
  expected;
- a repeated immutable put cannot silently replace different bytes;
- ranged reads return exact byte windows and integrity evidence;
- timeout and retry settings preserve ambiguous outcomes for reconciliation;
- lifecycle policies cannot delete reachable NoKV objects independently;
- bucket listing is not required for metadata recovery or reachability.

NoKV should use a dedicated bucket or an exclusive prefix with a policy that
prevents writes outside that boundary.

## Local Development

A local RustFS deployment is suitable for integration tests when the test
records:

- exact RustFS image or binary version;
- endpoint and TLS mode;
- clean or reused data directory;
- bucket initialization;
- NoKV durability profile;
- injected provider failures.

Keep RustFS data and Holt metadata in separate durable directories. Removing
one does not safely reset the other; create a fresh paired test deployment
instead of mixing prior metadata with an empty object directory.

The checked-in local launcher uses a Docker-managed volume by default because
the pinned RustFS image runs as non-root UID/GID `10001:10001`. An explicit
`NOKV_WORKBENCH_RUSTFS_DATA_DIR` host bind mount is supported only when that
directory is already writable by the container user. CI gates use isolated
named volumes; the gate or workflow cleanup removes them after qualification.

## Failure Semantics

Publication follows object-first, metadata-last ordering:

1. upload blocks;
2. verify completion evidence;
3. publish metadata;
4. acknowledge the deterministic result.

If metadata publication fails, staged-object records drive cleanup. If delete
completion is uncertain, the operation enters quarantine and reconciliation.
The system does not infer success from a later bucket listing.

Temporary provider failures remain retryable across the object, client, and
Workbench error boundaries. After bounded attempts, callers receive
`ObjectUnavailable` with `retryable: true` and an attempt count. Public errors
do not include endpoint, bucket, prefix, or physical object keys. Immutable
create and delete operations with ambiguous completion remain reconciliation
cases rather than blind retries.

## Existing Roots

Roots created before object-namespace binding have no durable evidence from
which NoKV could infer the historical bucket/prefix. Automatic adoption would
therefore make a typo authoritative. Upgrade them only while all owners are
stopped and after an operator verifies the exact existing object profile:

```bash
nokv \
  --root-id ROOT_HEX32 \
  --etcd-endpoint ETCD_URL \
  --object-bucket BUCKET \
  --object-endpoint S3_URL \
  --object-root PREFIX \
  provision LOGICAL_SHARD_HEX32 \
  --adopt-legacy-object-namespace
```

Without the explicit flag, provisioning an existing unbound root fails before
creating a marker or control binding. The next `--metadata-reopen` validates
the control binding and upgrades the legacy Holt root fence through the normal
owner-fenced metadata command and recovery outbox. Existing v1 root-fence and
v2 recovery bytes remain readable and re-encode canonically.

The legacy `None` decode and explicit adoption path may be removed only after
every supported deployment proves that all root control bindings and Holt root
fences contain an `ObjectNamespaceId`, and the documented upgrade window has
ended. It must never be replaced by implicit adoption.

## Qualification

RustFS qualification covers:

- single and multipart publication;
- exact and ranged reads;
- checksum and length mismatch;
- timeout before and after provider completion;
- abort and staged cleanup;
- zero-reference deletion;
- ambiguous-delete reconciliation;
- process restart and owner replacement;
- healthy wrong-prefix rejection without owner-epoch or payload mutation;
- provider outage returning a redacted retryable error and the same logical
  request succeeding after provider recovery;
- throughput and latency under the declared payload/concurrency matrix.

Retain raw evidence using [Benchmarks](./benchmarks.md) and
[Workspace Acceptance](./development/workspace-acceptance.md).
