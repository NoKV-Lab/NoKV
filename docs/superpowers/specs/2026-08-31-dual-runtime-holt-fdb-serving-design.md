<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Dual Runtime Holt And FoundationDB Serving Design

**Decision date:** 2026-08-31

**Status:** Approved for implementation planning; not implemented.

**Qualification:** FoundationDB remains `NOT QUALIFIED` for NoKV serving until
every gate in this document passes against a real cluster.

## Decision

NoKV will expose one metadata URL and two explicit runtime modes:

```text
holt:///absolute/path
fdb:///absolute/fdb.cluster?prefix=nokv-prod
```

- `holt://` is the standalone mode. One process exclusively opens one Holt
  metadata store. It has no distributed control plane, no lease service, and
  no remote takeover.
- `fdb://` is the distributed mode. FoundationDB is the shared authority for
  workspace metadata, root catalog, placement, routes, owner sessions,
  heartbeats, and leases.
- NoKV clients never link the FoundationDB client library or read FoundationDB
  directly. They discover routes through one or more NoKV seed endpoints.
- etcd, its feature flags, its command-line options, its client-side route
  resolver, and its dependency are removed rather than retained as a
  compatibility path.

This design takes the useful part of JuiceFS's model—select a metadata engine
by URI scheme and keep FoundationDB optional—without copying its filesystem
schema or session cleanup semantics into NoKV. NoKV retains its single
workspace schema, persisted root placement, logical-shard ownership, and
owner-fenced metadata commands.

This is a breaking change. The workspace format advances from version 10 to
version 11. A new binary rejects version-10 stores. The first implementation
does not provide an automatic migration, compatibility decoder, silent Holt
fallback, or provider conversion tool.

## Goals

- Make standalone operation require only an absolute Holt path and an
  exclusive local open.
- Make distributed operation use one FoundationDB prefix as the shared
  metadata and control authority.
- Preserve one storage-neutral workspace schema and one domain implementation
  in `nokv-meta`.
- Fence a former FDB owner in the same physical transaction as every metadata
  read or write that requires ownership.
- Let clients bootstrap and refresh routes through NoKV servers without a
  control-store dependency.
- Remove the local recovery-log publication chain from the FDB hot path while
  retaining local Holt recovery.
- Redesign secondary-index maintenance so every FDB serving transaction is
  bounded well below FoundationDB's hard affected-data limit.
- Keep the default build and standalone binary independent of `libfdb_c`.
- Preserve explicit format, schema, and configuration admission. Opening the
  wrong store must fail closed.

## Non-goals

- No active-active writes to one logical shard. One fenced owner serves a
  logical shard at a time.
- No multi-process or multi-host Holt mode, network filesystem takeover, or
  local-WAL shipping between Holt owners.
- No direct FoundationDB access in `nokv-client`, Python bindings, Agent
  adapters, or other clients.
- No FoundationDB tenant, directory layer, tuple-layer workspace schema, or
  provider-specific records in `nokv-meta` domain types.
- No automatic provider selection, provider fallback, mixed Holt/FDB store, or
  runtime conversion between providers.
- No retention of etcd aliases, deprecated flags, forwarding wrappers, or
  dual control implementations.
- No claim that the existing characterization adapter is already safe for
  production serving.
- No new transport authentication design. Seed discovery follows the same
  deployment security boundary as the existing NoKV RPC endpoint.
- No FUSE, POSIX, inode, dentry, or generic filesystem behavior.

## Baseline At Design Approval And Required Break

The tree captured when this design was approved had these relevant properties:

- `nokv` enables etcd by default and exposes static or etcd routing.
- `nokv-control` contains a provider-neutral control contract plus an etcd
  implementation.
- `nokv-server` bootstraps a local Holt store and local recovery publication;
  its distributed composition is tied to etcd.
- `nokv-meta-fdb` is a non-default characterization adapter. Its real-cluster
  conformance covers physical transaction-store behavior, but its profile is
  intentionally rejected by `MetaShard::bind`.
- workspace format 10 always writes `RecoveryOutbox` material and admits a
  16 MB logical metadata transaction profile.
- the current secondary-index key repeats the complete normalized path, and
  each indexed-field value repeats the complete typed projection. A measured
  maximum 60-field disjoint republish shape is 9,860,707 logical bytes before
  FoundationDB's conservative physical accounting.

Wiring the characterization adapter directly into `nokv-server` would
therefore either reject valid domain commands after admission or exceed the
FoundationDB serving budget. Distributed serving requires the schema,
recovery, routing, and ownership changes in this design; selecting the current
adapter alone is not an implementation shortcut.

## Approaches Considered

### 1. Reimplement the current etcd store on FoundationDB

This has the smallest control-plane diff, but it preserves an etcd-shaped
client route path, keeps two recovery authorities, and leaves oversized
metadata transactions unchanged. It is rejected.

### 2. Mode-specific composition with one FDB authority — selected

Holt remains a local transaction store with local recovery. FoundationDB owns
both shared metadata and control state. Provider-neutral domain logic selects
behavior through advertised store capabilities, and clients use NoKV seed
discovery. This preserves package ownership while removing duplicated
authorities.

### 3. Let every NoKV server write every shard concurrently

This is superficially closest to a stateless metadata service, but it removes
the existing owner-epoch boundary and would require a different routing,
worker, lifecycle, and object-GC design. It is deferred. This design keeps one
fenced owner per logical shard.

## User-Facing Configuration

### Metadata URL

The metadata URL is a required, parsed configuration object rather than a raw
string passed into adapters.

`holt:///absolute/path` has these rules:

- the authority is empty;
- the decoded path is absolute and non-empty;
- query parameters and fragments are rejected;
- the path identifies exactly one Holt store directory;
- no implicit create occurs during `serve`.

`fdb:///absolute/fdb.cluster?prefix=nokv-prod` has these rules:

- the authority is empty;
- the decoded cluster-file path is absolute, non-empty, and valid UTF-8 for
  the selected FoundationDB binding;
- exactly one `prefix` parameter is required;
- the percent-decoded prefix is 1 through 64 UTF-8 bytes and is used only as a
  physical isolation token;
- duplicate or unknown parameters and fragments are rejected;
- the cluster-file path is local connection configuration, while the prefix
  and durable store manifest identify the NoKV store.

The parser reports the invalid field and never treats a malformed `fdb://` URL
as Holt.

### Commands

The native command surface becomes:

```text
nokv format    --meta-url holt:///absolute/path
nokv provision --meta-url holt:///absolute/path ...
nokv serve     --meta-url holt:///absolute/path

nokv-fdb format    --meta-url 'fdb:///absolute/fdb.cluster?prefix=nokv-prod'
nokv-fdb provision --meta-url 'fdb:///absolute/fdb.cluster?prefix=nokv-prod' ...
nokv-fdb serve     --meta-url 'fdb:///absolute/fdb.cluster?prefix=nokv-prod' ...

nokv ... --seed node-a.example:7750 --seed node-b.example:7750
```

The default `nokv` binary contains Holt support only. A feature-enabled release
binary named `nokv-fdb` contains both URI parsers and FoundationDB serving
support. Giving an FDB URL to a binary without FDB support returns an explicit
unsupported-provider error.

`format` creates only the store-level manifest and global schema marker.
`provision` creates the requested root, object namespace binding, initial
placement, and any per-shard metadata markers using the selected mode. `serve`
only opens an already formatted store. None of the three commands automatically
changes mode or retries another provider.

The implementation removes these old public configuration surfaces:

- every `--etcd-*` option and etcd feature;
- `--metadata-create`, `--metadata-reopen`, and `--metadata-recover-log`;
- `--recovery-publication` and distributed recovery installer/publisher CLI
  wiring;
- CLI-supplied logical-shard, object-namespace, placement-generation, and
  owner-epoch pins;
- Python `RoutingConfig.etcd` and other client-side etcd construction.

The Rust SDK may retain `StaticRouteResolver` for focused tests and explicitly
embedded callers. It is not a second distributed discovery path exposed by the
native CLI.

## Package Boundaries

The target dependency direction is:

```text
nokv-fdb          -> foundationdb
nokv-meta-fdb     -> nokv-fdb + nokv-meta-store
nokv-control-fdb  -> nokv-fdb + nokv-control
nokv-server       -> nokv-meta-holt | (nokv-meta-fdb + nokv-control-fdb)
nokv-client       -> nokv-protocol
```

### `nokv-fdb`

Owns the process-global FoundationDB network runtime, API-version selection,
database handles, physical prefix encoding, common FDB options, and shared
error classification. FoundationDB permits one network runtime per process;
this crate starts it once and stops it only after all FDB-backed server state
has shut down. It never attempts to restart a stopped network.

It does not own workspace records, metadata commands, control records, server
composition, or client routing.

### `nokv-meta-fdb`

Becomes the serving physical `TxnStore` adapter after qualification. It owns
metadata subspace encoding, physical reads and writes, conservative affected-
data accounting, exact owner-session checks, and FoundationDB error mapping.
It continues not to import `nokv-meta`.

### `nokv-control` and `nokv-control-fdb`

`nokv-control` retains provider-neutral catalog, placement, route, session,
lease, and transition types. Its etcd module, options, feature, and dependency
are deleted.

`nokv-control-fdb` owns the FoundationDB encoding and transactions for those
types. It does not know workspace paths, artifacts, history, indexes, or object
GC policy.

### `nokv-protocol` and `nokv-client`

`nokv-protocol` owns the versioned discovery request and response DTOs.
`nokv-client` owns seed rotation, route caching, route refresh, and request
retry policy. Neither package imports FoundationDB or a control-store adapter.

### `nokv-server`

Remains the production composition root. It parses the already validated mode,
constructs exactly one provider stack, performs schema and manifest gates,
starts ownership workers, exposes discovery, and starts lifecycle workers
allowed by that mode. Domain semantics remain in `nokv-meta`.

### `nokv`

Remains thin CLI wiring. URI parsing types may live in a small provider-neutral
configuration package if both server and CLI need them; they must not be
duplicated.

## One Store Manifest

Both modes require a durable store manifest before any root is provisioned.
The logical fields are:

```text
store_id
provider_kind              # holt | fdb
workspace_format_version   # 11
physical_encoding_version
provider_namespace_digest  # path identity for Holt, prefix identity for FDB
created_by_version
```

The digest is an admission fence, not a secret or user-visible identifier. An
open fails if the provider kind, namespace identity, physical encoding, or
workspace format differs from the request. An unmarked non-empty store,
unknown version, mixed provider layout, or partial format fails closed.

The cluster-file path itself is not durable FDB store identity because
different hosts can use different files for the same cluster. The durable FDB
manifest binds the selected prefix. Holt binds the canonical store directory
identity and also verifies the store-local marker under the exclusive lock.

`format` uses create-only predicates so concurrent format attempts produce one
winner or one exact already-formatted result. It never rewrites a mismatching
manifest.

## Standalone Holt State Flow

All standalone roots use one Holt database and one persisted logical-shard
identity.

```text
Unformatted --format--> Formatted --provision--> Ready
                                           |
                                           v
                               ExclusiveOpen -> Serving
                                           |
                                           v
                                        Closed
```

1. `format` creates the Holt store, format-11 manifest, stable store ID,
   logical-shard ID, object-namespace allocation state, and local recovery
   state.
2. `provision` persists root placement and the matching `RootFence` in that
   same Holt authority before the root can serve.
3. `serve` obtains an OS-backed exclusive store lock before reopening Holt.
   Failure to obtain the lock is fatal; it does not start a read-only or
   secondary owner.
4. While holding the lock, startup advances the durable local owner epoch and
   matching root fences, completes local recovery/fsck gates, and only then
   accepts requests.
5. A restart reopens the same absolute path. Another host cannot take over a
   copied path under this mode's contract.

Holt keeps its local WAL, local recovery-outbox evidence, and the 16 MB logical
transaction envelope. It does not run the distributed recovery
installer/publisher, and it has no FDB runtime, heartbeat, distributed lease,
or external control-plane dependency.

The standalone server can answer seed discovery from its manifest and local
catalog, but discovery always resolves to that single server and does not turn
Holt into a distributed mode.

## FoundationDB Physical Layout

All records live below the explicit FDB prefix. The following names are
conceptual subspaces; `nokv-fdb` owns a versioned, component-safe binary
encoding rather than slash-delimited strings:

```text
system/manifest
catalog/root/<root-id>
catalog/shard/<logical-shard-id>
route/shard/<logical-shard-id>
lease/shard/<logical-shard-id>/session
lease/shard/<logical-shard-id>/heartbeat
meta/<logical-shard-id>/<keyspace>/<logical-key>
```

- `catalog/root` binds a root to one Agent identity, object namespace, logical
  shard, and placement generation.
- `catalog/shard` stores stable shard identity and lifecycle state.
- `route/shard` stores acquisition/serving state, advertised RPC endpoint,
  owner epoch, and session generation.
- `session` is the stable ownership fence checked by metadata transactions.
- `heartbeat` is updated independently so routine renewals do not conflict
  with every metadata transaction.
- `meta` contains only the storage-neutral `TxnStore` keyspaces and values.

Root placement is written before its metadata `RootFence`, and the root is not
discoverable until both are installed. Provisioning and failover use explicit
intermediate states; a crash cannot expose a catalog entry whose metadata
fence is not ready.

FoundationDB's shared database is the recovery authority. The design does not
write a local authoritative metadata log or upload a recovery chain in FDB
mode.

## FDB Ownership, Heartbeats, And Fencing

The ownership token is:

```text
(owner_epoch, session_generation)
```

The heartbeat value additionally carries a monotonically increasing
`heartbeat_sequence`. Wall-clock timestamps are diagnostic only and are never
used as a cross-machine correctness predicate.

The serving state flow is:

```text
Unassigned -> ObserveHeartbeat -> Acquire -> Activating -> Serving
                                             |             |
                                             v             v
                                          FailClosed <- LeaseLost
                                             |
                                             v
                                          Reconcile
```

### Acquisition

1. A contender reads the current session token and heartbeat sequence.
2. It observes that exact pair. If the sequence remains unchanged for the
   configured TTL according to the contender's local monotonic clock, it may
   attempt acquisition. An explicitly unassigned shard with no session can be
   acquired immediately through a create-only transaction.
3. One FoundationDB transaction rechecks the session and heartbeat, increments
   the owner epoch and session generation, installs the new session token, and
   changes the shard route to `Activating` with the contender's endpoint.
4. Transaction conflicts make simultaneous contenders retry observation. Only
   one token can win.
5. The winner opens the shared metadata subspace and advances matching
   `RootFence` records in bounded batches. During this phase discovery does not
   return the route as serving.
6. A final control transaction verifies the same session token and changes the
   route to `Serving`.

The activation scan is resumable. A process that dies during activation leaves
an expirable session and a non-serving route; its successor restarts activation
from persisted catalog and metadata state.

### Renewal

The owner periodically executes one small transaction that checks the exact
session token and advances `heartbeat_sequence` with checked arithmetic.
Metadata transactions do not read the heartbeat key.

If renewal cannot be confirmed before the local safety deadline, the owner
immediately unloads its routes and stops accepting owner-required operations.
It does not continue serving through a grace period based on wall-clock time.

### Metadata fence

Every owner-required FDB metadata read or write adds an exact read-conflict
check on the session key in the same FoundationDB transaction. The expected
token is immutable in the live `FdbStore` handle. A takeover changes that key,
so a stale owner's next transaction conflicts or reports not-owner even if its
process, network connection, or cached route remains alive.

The metadata transaction also validates the provider-neutral root fence. The
FDB session key is the external takeover fence; `RootFence` retains NoKV's
domain invariant and object-GC ownership epoch. Advancing root fences may occur
after the acquisition transaction because no new route becomes serving until
activation completes, and the previous owner is already fenced by the session
key.

## Seed Discovery And Route Refresh

The transport gains a top-level versioned request envelope so discovery does
not require a route it is trying to discover:

```text
RpcRequest::DiscoverRoute { root }
RpcRequest::Workspace { route, request }
```

The exact Rust names may differ, but the wire distinction is required and is a
schema-breaking protocol change. Discovery returns:

```text
root_id
logical_shard_id
object_namespace_id
placement_generation
owner_epoch
session_generation
owner_endpoint
route_state
```

Only `Serving` routes are returned as usable. An activating, missing, expired,
or mismatching route returns a typed retryable discovery failure rather than a
stale endpoint.

Any healthy FDB-mode NoKV server can answer discovery by reading the shared
catalog and route. A Holt-mode seed answers only from its local manifest and
catalog. A seed does not proxy the workspace request.

The client:

1. validates and deduplicates its configured seed list;
2. rotates through seeds until one returns a valid route;
3. caches the route by root and its placement/session generations;
4. sends workspace RPCs directly to the returned owner endpoint;
5. refreshes on `NotOwner`, connection failure, a higher generation hint, or
   an explicit route-expired response;
6. accepts only a route that is at least as new as the cached route;
7. uses bounded backoff and never queries FoundationDB or etcd.

An RPC failure may include a newer route and endpoint as a hint, but the client
validates its root and generations before installing it. When the hint is
incomplete or suspect, the client returns to seed discovery.

## Transaction And Recovery Profiles

`StoreProfile` must distinguish a hard adapter limit from the serving planner's
preferred transaction target. Domain code remains provider-neutral: it plans
against advertised capabilities rather than matching on `holt` or `fdb`.

### Holt profile

- shared workspace schema version 11;
- local recovery authority and local acknowledgement boundary;
- existing 16 MB logical command envelope;
- `RecoveryOutbox` receipt required in each metadata command;
- local unknown-outcome behavior continues to poison and recover the uncertain
  local store as specified by the Holt contract.

### FDB profile

- shared workspace schema version 11;
- shared recovery authority and shared-commit acknowledgement;
- 900,000 affected bytes as the serving planning target;
- conservative adapter preflight below FoundationDB's 10,000,000-byte hard
  affected-data limit; the existing 9,500,000-byte physical guard remains a
  last line of defense, not a planner target;
- no local recovery-outbox mutation in the hot transaction;
- no automatic raw transaction retry after a maybe-committed result.

The 900 KB target is a qualification invariant, not FoundationDB's physical
limit. Every lifecycle planner and batcher must prove that its final commit and
staging batches remain below the target for the documented maximum input
shape. The adapter still rejects any request that exceeds its hard logical or
physical bounds.

### Provider-neutral recovery receipt

Format 11 replaces the mandatory local recovery binding in the dedupe record
with a provider-neutral optional receipt:

```text
CommandDedupeRecordV3 {
    request_id,
    command_digest,
    result,
    recovery_receipt: Option<LocalRecoveryReceipt>,
}
```

Holt writes `Some(LocalRecoveryReceipt)` and its `RecoveryOutbox` in the same
transaction. FDB writes `None`; the atomic shared database and the dedupe row
are authoritative. This is one schema with capability-selected behavior, not
two workspace schemas.

On `commit_unknown_result`, FDB serving never retries the raw `WriteTxn`.
Instead it performs a fresh linearizable lookup by `request_id`:

- an exact `command_digest` reconstructs and returns the stored result;
- a different digest is request-ID reuse and fails closed;
- a confirmed absent record from a fresh linearizable transaction started
  after the unknown result was received permits re-executing the same domain
  request with the same ID and digest, whose predicates and dedupe write remain
  atomic;
- malformed or contradictory evidence returns corruption and unloads the
  affected route.

## Secondary Index V2

Format 11 replaces the current write-amplifying secondary index. The goals are
to avoid repeating a maximum-length path and complete typed projection for
every indexed field, and to make visibility switch through one small
authoritative transaction.

### Records

Conceptually, the new records are:

```text
PathCurrentV2 {
    ...existing authoritative path fields,
    index_generation,
    path_digest,
    typed_projection,
}

PathLocatorV2(root, workspace_incarnation, path_digest, index_generation) {
    normalized_path,
}

SecondaryIndexV2(
    root,
    field,
    ordered_scalar,
    workspace_incarnation,
    path_digest,
    index_generation,
) {
    path_digest,
    index_generation,
}
```

The 256-bit `path_digest` is computed from the canonical normalized path. A
locator stores the full path once per path generation. Creation checks that an
existing locator either contains that exact path or fails closed as a digest
collision/corrupt store. Secondary rows do not repeat the full path or typed
projection.

### Visibility protocol

1. Select a never-reused `index_generation` for the new path state.
2. Write the locator and new secondary rows in bounded, resumable staging
   batches. They are invisible because no `PathCurrentV2` names that
   generation yet.
3. Execute one bounded final metadata command that applies normal predicates,
   revision references, history, event, dedupe, and operation state, then
   creates or flips `PathCurrentV2` to the staged generation.
4. Queries treat an index row as visible only when its digest and generation
   match the current path record reached through the locator.
5. Delete older index rows and locators asynchronously with generation
   predicates. Cleanup can never delete the current generation.

Publication, replace, rename, restore, and any projection-changing operation
use this protocol. Remove makes old rows invisible by removing `PathCurrentV2`
in its authoritative command, then cleans them asynchronously. A crash before
the final flip leaves invisible staging rows; a crash after the flip leaves
only harmless stale rows. Both cases are resumable from durable operation
state.

Index queries use bounded batched locator and `PathCurrentV2` reads. They
discard stale generations and preserve the existing result/cursor contract.
Exact path lookup remains one workspace-marker read plus one authoritative
path read; it does not traverse the secondary index.

### Admission and qualification

`MetaShard::command_fit` stops assuming one global 16 MB profile and stops
unconditionally charging a local recovery outbox. It estimates the exact
provider-neutral command selected by the bound store capabilities.

Before FDB can serve, measured encoded affected-data evidence must show every
final and staging transaction at or below 900 KB for at least this maximum
matrix:

- maximum normalized path length;
- 60 indexed fields with disjoint old and new values;
- 64 revision dependencies;
- maximum change-event and dedupe result payloads;
- create, replace, remove, rename, restore, replay, and cleanup;
- conflict and response-loss reconciliation variants.

Any shape that does not fit must gain another visibility-safe bounded phase;
the implementation must not raise the target to make the test pass.

## Provisioning And Recovery Flows

### FDB provision

1. Verify the format-11 manifest and exact prefix.
2. Allocate stable root, object-namespace, and logical-shard identities through
   create-only control records.
3. Persist root placement in `Provisioning` state.
4. Under the current shard session, initialize the metadata root fence and
   workspace state through bounded commands.
5. Change the catalog entry to `Ready` only after metadata initialization is
   confirmed.

Exact retries return the existing identities. Different inputs under the same
provision request ID fail closed.

### FDB owner loss

1. The old owner loses or cannot confirm its session and unloads the route.
2. A contender observes the unchanged heartbeat, acquires a new session, and
   fences the old token.
3. It opens the same shared metadata namespace; it does not install a local
   log or copy metadata.
4. It reconciles any durable operations and unknown request outcomes from
   shared metadata.
5. It advances root fences, starts lifecycle workers, and publishes a serving
   route only after the activation gates pass.

### Holt recovery

Holt reopens only the same store path under the exclusive lock. It validates
and installs the local recovery chain according to its existing authority
contract before serving. There is no cross-host successor protocol.

## Failure Behavior

| Condition | Required behavior |
| --- | --- |
| Unsupported URI scheme | Reject configuration; no fallback |
| Relative or malformed path | Reject the named field |
| Missing/duplicate FDB prefix | Reject configuration |
| Store manifest mismatch | Fail closed before route publication |
| Holt lock already held | Refuse to serve |
| FDB network cannot start | Refuse FDB mode; default Holt binary remains usable |
| FDB cluster unavailable at startup | Refuse to publish routes |
| Heartbeat unchanged for less than TTL | Do not contend |
| Session changes during acquisition | Conflict and restart observation |
| Renewal cannot be confirmed safely | Unload routes immediately |
| Stale owner metadata request | Same-transaction session check rejects it |
| Route is activating or expired | Discovery returns retryable unavailable |
| FDB commit outcome unknown | Reconcile by request ID and digest; no raw retry |
| Dedupe digest mismatch | Fail closed as request-ID reuse |
| Unknown/corrupt schema or index row | Fail closed and report the owning record |
| Transaction exceeds 900 KB target | Split before serving qualification |
| Transaction exceeds adapter hard limit | Reject before commit |

## Removal Scope

Completion includes deleting, not deprecating:

- the workspace `etcd-client` dependency;
- all `etcd` Cargo features and conditional modules;
- `EtcdControlStore`, `EtcdControlStoreOptions`, and etcd route options;
- CLI parsing, help, tests, examples, and Python API for etcd;
- etcd-specific acceptance runners and documentation claims;
- server branches that require etcd to enter distributed serving;
- distributed recovery installer/publisher wiring that exists only for
  cross-owner local-store recovery;
- stale compatibility wording that describes etcd as a current authority.

Historical benchmark artifacts remain historical only when repository policy
requires retaining evidence. They cannot be invoked by current preflight,
qualification, or product documentation.

## Validation And Qualification

### Static and package tests

- exhaustive metadata-URL parsing and canonicalization;
- manifest create/open/mismatch and concurrent-format tests;
- proof that the default build neither compiles nor links FoundationDB;
- process-global FDB runtime start/use/shutdown tests;
- control encoding, ordering, conditional transition, and corruption tests;
- metadata adapter conformance with the session fence enabled;
- seed rotation, cache monotonicity, stale hint, and protocol-version tests;
- format-11 codec and explicit format-10 rejection tests;
- `SecondaryIndexV2` staging, visibility, replay, collision, and cleanup tests;
- recovery-receipt behavior for both advertised authority profiles;
- source and dependency checks proving etcd is absent from product code.

### Real-boundary integration tests

- standalone Holt format, provision, serve, restart, recovery, and exclusive
  open;
- real FoundationDB format, provision, multi-server discovery, owner kill,
  heartbeat expiry, takeover, and stale-owner fencing;
- concurrent contenders with exactly one winner;
- response loss and injected `commit_unknown_result` reconciliation;
- FDB restart/network interruption without raw transaction replay;
- route activation crash at every persisted transition;
- maximum transaction matrix with encoded logical and physical byte evidence;
- existing workspace command, object, SDK, Agent, Python, lifecycle, and GC
  acceptance suites in both applicable modes.

### Performance evidence

FDB serving qualification records workload, payload, concurrency, machine,
cluster topology, FoundationDB version, durability mode, p50/p95/p99/max,
throughput, errors, conflicts, retries, and affected bytes. It includes steady
state, takeover, index-heavy publication, query joins, and lifecycle cleanup.

Passing unit tests alone does not qualify FDB durability, failover, GC, or
performance. Every applicable workspace acceptance gate reports `PASS`,
`FAIL`, or `NOT QUALIFIED`. Public documentation continues to call FDB
`NOT QUALIFIED` until all required gates pass and the code contract is updated
in the same reviewed implementation series.

### Repository validation

Each implementation slice runs its focused tests. Before completion, run:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
python3 scripts/workbench/workbench_contract_test.py
git diff --check
```

Feature and real-cluster commands must report their exact environment. A
skipped or unavailable FoundationDB test is `NOT RUN`, never `PASS`.

## Implementation Slices

The implementation is delivered as reviewable dependency-ordered slices:

1. Add the shared metadata-URL type, store manifest contract, format-11 schema
   specification, and capability-based transaction planning interfaces.
2. Add `nokv-fdb` and move the process-global runtime/common FDB encoding out
   of the characterization-only adapter internals.
3. Add `nokv-control-fdb` with catalog, route, session, heartbeat, acquisition,
   activation, and lease conformance tests.
4. Add the discovery protocol and client seed resolver without selecting FDB
   in the server yet.
5. Implement format-11 recovery receipts and `SecondaryIndexV2`, including the
   900 KB maximum-shape evidence on Holt-backed deterministic tests.
6. Bind the qualified metadata adapter to the FDB session fence and compose FDB
   format/provision/serve in `nokv-server` and `nokv-fdb`.
7. Simplify Holt format/provision/serve around one store and exclusive open.
8. Remove etcd and every superseded CLI, SDK, Python, test, documentation, and
   dependency surface.
9. Run dual-mode integration, failure, recovery, transaction-size, and
   performance qualification; update serving claims only from retained
   evidence.

No slice may temporarily expose an unfenced FDB serving route. Intermediate
commits can compile with FDB unavailable to server composition until the
required schema, session, and transaction gates exist.

## Acceptance Criteria

This design is complete only when all of the following are true:

- `holt:///absolute/path` is sufficient to format, provision, serve, restart,
  and recover one exclusive Holt store without a control service.
- `fdb:///absolute/fdb.cluster?prefix=...` is sufficient for shared metadata,
  catalog, route, session, heartbeat, and lease state.
- clients can start from NoKV seeds, refresh after takeover, and have no FDB or
  control-adapter dependency.
- a stale FDB owner cannot complete an owner-required metadata transaction
  after its session token changes.
- no FDB serving transaction in the maximum qualification matrix exceeds the
  900 KB target.
- unknown FDB commit outcomes reconcile through exact dedupe evidence without
  raw transaction replay.
- Holt retains local recovery while FDB does not write local recovery outbox
  material in its hot path.
- format-10, unmarked, mixed, and mismatching stores are rejected without
  automatic migration or fallback.
- etcd is absent from product dependencies, features, configuration, code,
  tests, and current product documentation.
- the default build remains independent of `libfdb_c`.
- every applicable acceptance gate carries retained evidence and an honest
  `PASS`, `FAIL`, or `NOT QUALIFIED` result.
