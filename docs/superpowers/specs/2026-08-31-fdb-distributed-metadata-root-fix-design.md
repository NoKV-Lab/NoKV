<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# FoundationDB Distributed Metadata Root Fix Design

**Decision date:** 2026-08-31

**Status:** Approved and in implementation.

**Qualification:** FoundationDB remains `NOT QUALIFIED` until this design is
implemented and the real-cluster gates complete without an unsupported claim.

## Decision

Fix the two observed distributed-metadata failures at their state-machine
boundaries:

1. FDB provisioning becomes a recoverable prepare/admit/finalize flow. A new
   root remains `Provisioning` until the exact object namespace exists and
   passes provider admission. A newly created shard remains `Provisioning` as
   well; an existing shared shard that is already `Ready` never rolls back.
2. secondary-index stage replay binds only immutable staged-write intent.
   Volatile operation heartbeats, workspace revisions, and current path
   payloads remain transaction predicates but are not request-reuse identity.

The fix does not increase retry counts, roll a `Ready` catalog backward, add a
compatibility path, or weaken any ownership or publication predicate.

## Observed Failures

### Catalog Ready precedes object admission

The current CLI calls `provision_fdb`, which initializes metadata and marks the
root and shard `Ready`. Only after that call returns does the CLI construct the
object provider, create or verify its namespace marker, bind the provider, and
run capability admission.

A live invocation without `--object-bucket` returned an argument error after
the FDB catalog had already become `Ready`. A correct retry reported the root
as preexisting. This violates the documented requirement that the exact object
namespace be verified before the root becomes ready to serve.

### Valid finalize retry becomes RequestReplayMismatch

The current secondary-index stage result records a digest over both its staged
writes and volatile predicate payloads. In particular, the digest includes the
complete `WorkspaceCurrent` payload. Concurrent publications to distinct paths
advance the same workspace revision.

If secondary-index staging commits and final publication then conflicts, an
exact retry finds the durable stage result. The fresh workspace payload no
longer matches the stored digest, so a valid retry becomes
`OperationInputMismatch`, which the RPC layer exposes as
`RequestReplayMismatch`.

In the retained live run, 24 concurrent publications with 16 configured
attempts produced five such failures. Packet capture showed identical encoded
publication intent across the failed retries; the mismatch arose during
`CompleteArtifactPublish`, after stage commit and repeated final-transaction
conflicts.

## Scope

This change owns only:

- FDB provision composition in `nokv-server` and its CLI ordering in `nokv`;
- secondary-index stage replay identity in `nokv-meta`;
- focused deterministic tests and the real FDB/RustFS regression matrix;
- documentation needed to state the corrected state transitions.

It does not redesign workspace revisions, FDB routing, object layout,
transaction targets, client retry policy, or the public Workbench schema.

## Recoverable FDB Provisioning

### Prepared handle

`nokv-server` exposes a prepared FDB provisioning handle rather than returning
a `Ready` outcome immediately. The handle retains the process-global
`FdbRuntime` guard and exact manifest, catalog, and control-store binding while
the CLI performs object admission. Retaining the runtime is required because
the FoundationDB network cannot be stopped and restarted in one process.

Preparation performs these steps:

1. inspect the exact formatted manifest and prefix;
2. derive and create-or-load the root, object namespace, and logical shard;
3. validate that any existing root has the same agent, namespace, shard, and
   placement identity;
4. acquire an exact owner session for the shard's current lifecycle state;
5. open or initialize metadata, advance the shared owner fence, and reconcile
   the root fence;
6. release the exact provisioning session;
7. return a handle whose root catalog is still `Provisioning`, unless the
   exact root was already `Ready`; preserve an existing shard's `Ready` state.

No owner session remains live while the CLI calls the external object service.
This avoids coupling provider latency to the ten-second ownership lease.

### Object admission

The CLI constructs the object store before any FDB preparation so missing or
inconsistent local object options fail without catalog mutation. After
preparation exposes the immutable namespace id, the CLI:

1. creates or exact-verifies the namespace marker;
2. binds the object store to that namespace;
3. runs the required immutable-create and provider capability admission.

Only success permits the prepared handle's explicitly named
`finalize_after_namespace_admission` transition. The server package does not
learn bucket credentials, endpoints, or provider-specific metadata.

An exact root that is already `Ready` does not bypass this sequence. A repeated
CLI provision still verifies the namespace marker and provider capabilities
before it reports success, allowing a previously interrupted or historically
misordered deployment to repair object admission without changing catalog
state.

### Finalization

Finalization rereads the root and shard catalogs. If both are already exact and
`Ready`, it returns the deterministic preexisting outcome. Otherwise it:

1. reacquires an exact owner session for the shard's current lifecycle state;
2. reopens the same FDB metadata namespace under that session;
3. advances and reconciles the metadata/root fences to the new exact session;
4. conditionally changes the root to `Ready`;
5. conditionally changes the shard to `Ready`;
6. releases the exact session.

The existing root-then-shard ordering is retained. A crash between the two
conditional transitions leaves an explicit partial provisioning state that an
exact retry completes. `serve_fdb` continues to enumerate only `Ready` roots
on a `Ready` shard, so no partial state becomes discoverable.

### Crash behavior

| Interruption | Durable state | Retry behavior |
| --- | --- | --- |
| Before catalog creation | No root | Create exact identities |
| After catalog creation | `Provisioning` | Reuse exact identities |
| While a preparation session is live | `Provisioning` plus expiring session | Observe TTL, acquire a successor, advance fences |
| After metadata initialization | `Provisioning` | Reopen and verify the same metadata root |
| During object admission | `Provisioning`; marker may be absent or exact | Re-run idempotent ensure and admission |
| After object admission, before finalization | `Provisioning` plus exact marker | Re-verify marker, then finalize |
| Root `Ready`, shard `Provisioning` | Not serveable | Complete the shard transition |
| New root `Provisioning`, existing shard `Ready` | Existing roots remain serveable; new root is hidden | Admit the new namespace binding, then finalize only the new root |
| Both `Ready`, response lost | Ready and serveable | Exact readback returns the same outcome |

No path deletes catalog rows or attempts to roll `Ready` back to
`Provisioning`. Dropping a prepared handle performs no catalog compensation;
because preparation has already released its exact session, the durable state
is the explicit recovery point.

## Secondary-Index Stage Replay

### Immutable replay identity

The secondary-index stage result digest includes only values that define the
invisible staged writes:

- root and publication operation identity;
- stable operation, workspace, and path keys;
- staged locator key and payload;
- the ordered secondary-index row keys and payloads.

It excludes:

- the encoded operation payload, which may change through a valid heartbeat;
- the encoded workspace payload, whose revision changes after unrelated
  publications;
- the current path payload, which may change and must be revalidated as a
  domain claim rather than treated as request-id reuse.

The first stage transaction retains exact predicates over all three volatile
payloads. Removing them from the replay digest therefore does not weaken the
atomic stage admission.

### Retry flow

On an exact stage replay, the durable stage result proves that the same locator
and index rows committed. Finalization uses the fresh publication read version,
revalidates the current operation, workspace, path claim, dependencies, and
owner session, then rebuilds the final metadata command.

If immutable locator or index intent differs under the same derived stage
request id, replay still fails with `OperationInputMismatch`. Reusing the outer
RPC request id with a different encoded RPC remains rejected by the existing
request claim before publication finalization begins.

No new stage generation is minted for a retry. This prevents invisible index
row accumulation and keeps existing cleanup ownership unchanged.

## Package Boundaries

- `nokv-server` owns the FDB provisioning handle, session transitions, and
  catalog Ready transition.
- `nokv` owns CLI composition and calls the common object-provider admission
  APIs; it does not derive FDB identities.
- `nokv-object` continues to own namespace markers and provider capability
  receipts.
- `nokv-meta` owns staged-index intent, predicates, replay validation, and
  final publication semantics without importing FoundationDB.
- `nokv-meta-fdb` and `nokv-control-fdb` require no new automatic raw
  transaction retry.

No forwarding wrapper or compatibility API remains after callers move to the
prepared provisioning flow.

## Alternatives Rejected

### Hold one provisioning session across object admission

This is a smaller surface change, but an external S3/RustFS call can outlive
the lease or require a renewal worker. It couples provider latency and failure
handling to control ownership, so it is rejected.

### Mark Ready and compensate on provider failure

Rolling a catalog backward races serving discovery and cannot safely undo an
unknown provider outcome. It is rejected.

### Mint a new index stage generation for every retry

This avoids replay lookup but creates multiple invisible generations for one
publication and expands cleanup ambiguity. It is rejected.

### Collapse index staging and publication into one transaction

This removes the replay boundary but discards the existing bounded staging
design and requires a new maximum-transaction proof. It is broader than the
observed failure and is rejected.

## Verification

Deterministic tests must prove:

- missing object configuration causes no FDB catalog mutation;
- namespace ensure or provider admission failure leaves root and shard in
  `Provisioning`;
- retry after every preparation/finalization cut point converges to the same
  identities and `Ready` state;
- a stale preparation owner cannot finalize or release a successor session;
- a stage-commit/final-commit conflict followed by an unrelated workspace
  revision advance replays and publishes successfully;
- a valid operation heartbeat between attempts replays successfully;
- changed locator or index rows under the same stage request id still fail;
- outer RPC request reuse with changed input still fails.

The real-boundary regression uses the exact built `nokv-fdb` binary, a
three-process FDB 7.3 cluster, and RustFS. It must run:

- fresh format, failed admission, retry, and Ready verification;
- 24 and 32 same-workbench concurrent publications for three consecutive
  rounds, retaining successes, conflicts, retries, and errors;
- owner kill, seed failover, generation advance, stale-owner rejection, and
  post-takeover read/write;
- one FDB process loss and recovery during metadata publication;
- the 900,000-byte logical maximum matrix with encoded and observed physical
  affected-byte evidence.

Any `RequestReplayMismatch`, stranded `Ready` catalog, stale-owner write,
missing evidence role, or unmeasured maximum transaction keeps FDB serving
`NOT QUALIFIED`.
