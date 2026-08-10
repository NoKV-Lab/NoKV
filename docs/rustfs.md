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

## Failure Semantics

Publication follows object-first, metadata-last ordering:

1. upload blocks;
2. verify completion evidence;
3. publish metadata;
4. acknowledge the deterministic result.

If metadata publication fails, staged-object records drive cleanup. If delete
completion is uncertain, the operation enters quarantine and reconciliation.
The system does not infer success from a later bucket listing.

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
- throughput and latency under the declared payload/concurrency matrix.

Retain raw evidence using [Benchmarks](./benchmarks.md) and
[Workspace Acceptance](./development/workspace-acceptance.md).
