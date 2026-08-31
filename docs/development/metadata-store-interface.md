<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Metadata Store Interface

## Status

NoKV has two explicit metadata runtimes:

```text
holt:///absolute/path
fdb:///absolute/fdb.cluster?prefix=nokv-prod
```

Holt is the default standalone runtime. The feature-gated FDB runtime is
implemented as a serving candidate but remains **NOT QUALIFIED** until every
live gate in this document passes against a real FoundationDB cluster.

There is no automatic provider selection, migration, fallback, or dual-write
mode. A URL is required by `format`, `provision`, and `serve`; an unsupported
or unavailable provider fails closed.

The [code contract](./code_contract.md), [architecture](../architecture.md),
and [metadata schema](../metadata-schema.md) remain normative.

## Package Boundaries

- `nokv-meta` owns workspace metadata commands, predicates, mutations,
  lifecycle state, history, root fences, and deterministic results.
- `nokv-meta-store` defines ordered reads and checked atomic writes through
  `TxnStore`. It also defines store limits and commit-outcome classification.
- `nokv-meta-holt` maps `TxnStore` to one Holt database.
- `nokv-fdb` owns the one process-global FoundationDB API/network runtime,
  database handles, transactions, connection options, and physical prefix.
- `nokv-meta-fdb` maps `TxnStore` to FDB transactions and binds every handle to
  an exact stable owner-session predicate.
- `nokv-control` defines provider-neutral manifests, catalogs, routes, owner
  sessions, heartbeats, and legal transitions.
- `nokv-control-fdb` persists those distributed-control records in FDB.
- `nokv-server` composes the selected provider, installs exact routes, serves
  seed discovery, and supervises lifecycle workers.
- `nokv-client` discovers routes through NoKV seeds. It never opens the
  metadata database.

Domain state does not move into a generic `utils` package. Provider encoding
does not move into `nokv-meta`, and client routing does not move into the FDB
adapter.

## Store Contract

`MetaShard` consumes an injected ordered transaction store:

```text
MetaShard -> TxnStore -> HoltStore
                       -> FdbStore
```

Each checked write carries:

- an expected read version;
- explicit predicates;
- a bounded set of writes and deletes;
- one declared acknowledgement boundary;
- typed handling for definite rejection, applied commit, conflict, and unknown
  commit outcome.

The metadata layer plans to and enforces a 900,000-byte logical transaction
target for the FDB profile before dispatch. The FDB adapter independently
rejects requests above its advertised hard logical or conservative physical
affected-byte limits. It must not split one logical metadata command into
independently visible transactions. Large secondary-index work is staged
through domain-owned, generation-fenced records and one authoritative final
command.

## Persistent Manifest

`format` creates one immutable `StoreManifest` containing:

- store identity;
- provider identity;
- workspace format version;
- physical encoding version;
- digest of the exact provider namespace;
- creator version.

Re-running `format` on the same exact store returns the existing compatible
manifest. It never rewrites a mismatched manifest. `provision` and `serve`
require that manifest and do not initialize an unformatted store implicitly.

Provider namespace binding is exact:

- Holt binds the canonical absolute database location.
- FDB binds the canonical cluster file and binary prefix.

Moving a Holt directory or changing the FDB cluster file/prefix fails closed.

## Standalone Holt Runtime

`holt:///absolute/path` means:

- one Holt database;
- one derived logical shard;
- one process at a time, enforced by Holt's physical lock;
- no distributed catalog or lease service;
- metadata commit acknowledgement is the durability authority;
- restart reopens the same exact store and advances the local owner epoch.

`format_holt` initializes the manifest and metadata shard. `provision_holt`
creates or reconciles one root catalog entry and its active metadata fence.
`serve_holt` opens all Ready roots, installs their exact routes in one local
registry, and exposes those routes through the seed RPC.

Holt does not claim replacement-host or copied-directory failover. Losing the
only Holt store loses the standalone metadata authority. A separate shared-log
successor path is not part of this runtime.

## Distributed FDB Runtime

`fdb:///absolute/fdb.cluster?prefix=NAME` means one shared FDB authority for:

- store manifest;
- root and shard catalogs;
- routes;
- stable owner sessions;
- heartbeats and takeover observation;
- all workspace metadata rows.

The process starts the FDB network exactly once. All control and metadata
handles share that runtime. A process does not attempt to stop and restart the
FDB network for another store.

### Format

`format_fdb` creates only the immutable manifest below the selected prefix.
It creates no root, shard, route, session, heartbeat, or workspace row.

### Provision

`provision_fdb` derives the store's logical shard and artifact namespace,
creates the root and shard catalogs in `Provisioning`, and acquires a
provisioning-only owner session. It then:

1. opens or initializes session-fenced metadata;
2. advances the metadata owner epoch to the exact acquired session;
3. installs and activates the root fence;
4. marks root and shard catalogs `Ready`;
5. releases the provisioning session.

Unknown commit outcomes are reconciled through exact readback. The code never
blindly retries a raw transaction whose result may have committed.

### Serve

`serve_fdb` enumerates only Ready roots, groups them by Ready logical shard,
and acquires one exact owner session per shard. For each shard it opens an FDB
metadata handle whose every transaction checks the stable session key and
generation.

Routes start `Activating`. The server installs all local registry routes and
lifecycle workers before `activate_routes` publishes any route as `Serving`.
Renewal failure removes registry admission and fails the complete owner scope
closed. Shutdown drains admitted RPCs and releases each exact session.

### Takeover

Session and heartbeat are separate records. A contender measures expiry only
with its local monotonic clock while repeatedly observing an unchanged exact
session/heartbeat pair. Server wall clocks do not determine takeover.

A takeover transaction advances owner epoch and session generation, installs a
new stable session, resets the heartbeat sequence, and leaves the route
`Activating`. A stale process then fails its stable-session predicate before a
metadata commit can apply.

## Seed Discovery

Client configuration is a root identity plus one or more NoKV seed endpoints:

```text
--root-id HEX32 --seed host-a:7750 --seed host-b:7750
```

The client asks seeds for a complete route containing root, logical shard,
object namespace, placement generation, owner epoch, owner endpoint, and
session generation. It caches only monotonic observations and rejects:

- another root or object namespace;
- a generation rollback;
- an endpoint change without generation advance;
- malformed or non-retryable discovery failures.

Owner hints are advisory. A retryable ownership failure triggers seed refresh;
the client never treats a direct database read as a routing fallback.

## Durability And Lifecycle

Both runtimes use `CommittedMetadataDurability` for lifecycle work:

- Holt returns from a metadata commit after its local journal boundary.
- FDB returns after the shared database commit.

Lifecycle workers remain root-affine and owner-fenced. They discover work from
authoritative metadata, never object listing. Object deletion uncertainty is
quarantined rather than retried as if no destructive call occurred.

The retired distributed-local-log publication and recovery installer are not
compatibility paths. They have no Cargo feature, CLI option, server module, or
qualification gate.

## FDB Live Qualification Gates

Source wiring, unit tests, mocks, and dry runs do not qualify FDB serving. A
retained live evidence bundle must prove all of the following with the exact
candidate binary and FDB client/server versions:

1. **Conformance:** point reads, ordered scans, read-version stability,
   predicate failure, write conflict, and bounded transaction rejection.
2. **Unknown outcomes:** injected commit-unknown results reconcile by exact
   readback without double apply or route publication from an unproved state.
3. **Session fencing:** a stale owner cannot commit metadata, renew, activate,
   fail-close, or release a successor session.
4. **Takeover:** unchanged session/heartbeat observation respects the local
   monotonic TTL; changed tokens restart observation; successful takeover
   advances both owner and session generations.
5. **Provision crashes:** interruption before metadata open, after epoch
   advance, after fence activation, and before catalog Ready all converge or
   remain explicitly recoverable without stranding the shard.
6. **Serve crashes:** interruption before route activation never publishes a
   serving route; interruption after activation expires and hands off without
   accepting stale writes.
7. **Seed discovery:** multiple seeds, one failed seed, stale hints, refresh,
   and endpoint changes are exercised over the real wire protocol.
8. **Lifecycle:** publication, restore, snapshot, commit retirement, and GC run
   through session-fenced FDB metadata with ambiguous deletes quarantined.
9. **Limits:** the 900,000-byte logical plan stays below the measured FDB
   transaction envelope, and large artifact/object payloads never enter one
   FDB transaction.
10. **Performance:** retained latency and throughput evidence states the exact
    workload, cluster topology, transaction sizes, conflict rate, and thermal
    or frequency controls where relevant.

Until all ten gates pass, user-facing status remains **NOT QUALIFIED**.
