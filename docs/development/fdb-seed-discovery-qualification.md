<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# FoundationDB Seed Discovery Live Qualification

Status: implemented and qualified for FoundationDB serving Gate 7. The complete
FDB serving profile remains not qualified; see the
[qualification record](./fdb-root-fix-qualification-2026-08-31.md#gate-7-seed-discovery-qualification).

## Decision

NoKV will qualify distributed seed discovery with one checked-in,
environment-gated Rust workload owned by `bench`. The workload keeps one real
`SeedRouteResolver` alive across an FDB owner change, drives requests over the
versioned TCP protocol, and uses typed qualification-only peers to return stale
or invalid route observations.

The workload must not add product behavior, a second client route algorithm, an
FDB dependency to `nokv-client`, or a direct client-to-FDB fallback. It invokes
the exact candidate `nokv-fdb` binary for format, provision, and serving, and it
uses a fresh FDB prefix and RustFS object namespace on every run.

## Qualification Boundary

This workload closes only
[FDB live qualification Gate 7](./metadata-store-interface.md#fdb-live-qualification-gates).
It may reuse a real owner takeover to produce a newer route, but that does not
replace or extend the session-fencing and takeover evidence for Gates 3 and 4.
It does not qualify lifecycle, transaction limits, or performance.

The live result is `PASS` only when every scenario below passes against the
same source revision, candidate binary, FDB cluster, and RustFS service. Unit
tests, a scripted transport without TCP, a dry run, or a manually edited result
cannot produce a live `PASS`.

## Alternatives

### Client integration test

Putting the orchestration in `nokv-client` would be smaller, but it would make a
storage-neutral SDK test own FDB, RustFS, and server-process lifecycle. That
violates the package boundary and is rejected.

### Python, socat, or packet replay

An external script could replay captured bytes quickly, but it would either
duplicate the versioned codec or mutate MessagePack frames without typed
validation. The resulting evidence would be difficult to audit and is
rejected.

### Checked-in bench workload

The selected approach keeps qualification orchestration outside product crates,
uses the public client and protocol contracts, and can emit deterministic,
source-bound evidence. `bench` already owns contract, recovery, and performance
workloads.

## Components

The implementation is divided into four responsibilities:

- a thin `nokv-fdb-seed-qualification` binary parses explicit environment and
  candidate inputs;
- an orchestrator creates the fresh store, provisions one root, starts owner A
  and owner B, advances phases, and always reaps child processes;
- typed TCP peers use the repository protocol codec and handshake to return
  controlled discovery responses or `NotOwner` hints while recording every
  request and response;
- an evidence writer emits the environment, route observations, scenario
  results, process logs, and final qualification result atomically.

The production `SeedRouteResolver` is the only route state machine under test.
Qualification peers choose inputs but cannot install a route, change the client
cache, or decide the expected result.

## Live Topology

```text
fresh fdb://...cluster?prefix=nokv-seed-<run-id>
fresh RustFS object namespace

qualification process
  persistent SeedRouteResolver
    seed 1 -> typed qualification peer
    seed 2 -> real NoKV seed / owner B

owner A -> endpoint A -> shared FDB authority
owner B -> endpoint B -> shared FDB authority
```

Owner A first acquires and publishes the route. Owner B starts as a contender.
After the client caches A, the orchestrator terminates A and waits for B to
publish a strictly newer owner/session observation at endpoint B. Fixed sleeps
are not a success oracle; every transition is polled through the wire protocol
with a bounded deadline.

## Route Observations

Let the initial and successor observations be:

```text
A = (root, shard, namespace, placement, owner_epoch_a, session_a, endpoint_a)
B = (root, shard, namespace, placement, owner_epoch_b, session_b, endpoint_b)
```

The harness requires B to preserve the immutable root, shard, namespace, and
placement identities while advancing the owner epoch and session generation.
The qualification peer can then emit:

- `stale(A)`, an authentic earlier discovery response;
- `drift(B, endpoint_x)`, which preserves every B generation but changes only
  the endpoint;
- `foreign(B)`, which changes one immutable identity;
- a typed `NotOwner` response carrying either A or B as an advisory hint.

Every synthetic response is encoded and decoded through `nokv-protocol` and is
served over a real TCP connection. It is qualification input, not evidence of
an FDB control record.

## Scenario Matrix

| Scenario | Required observation | Pass oracle |
| --- | --- | --- |
| Multiple seeds | Two distinct seeds are configured and contacted in deterministic recorded order. | A healthy later seed resolves the root. |
| Failed first seed | The first endpoint refuses or drops the connection. | Discovery continues to the healthy seed within the declared attempt bound. |
| Owner endpoint change | The persistent client caches A, A exits, and B publishes a newer route. | A retry refreshes through seeds and a metadata command succeeds through B. |
| Stale discovery | After B is cached, the qualification seed returns `stale(A)` before the authoritative seed. | The cache never regresses; the resolver rejects A and retains or rediscovers B. |
| Stale owner hint | A wire response carries an advisory A hint after B is cached. | The hint is ignored and the following authoritative refresh remains at B. |
| Endpoint drift | The qualification seed returns `drift(B, endpoint_x)`. | The resolver rejects the same-generation endpoint change and accepts the authoritative B response. |
| Immutable identity drift | The qualification seed returns `foreign(B)`. | The resolver fails that observation closed and never sends a workspace request to its endpoint. |
| Final mutation | All fault phases have completed. | The same persistent client applies and reads back a metadata-visible result through B. |

The harness records which seed was contacted, the response class, the complete
non-secret route tuple, the resolver result, and the endpoint that received each
workspace request. Merely obtaining a successful final response is
insufficient if the expected stale or invalid observation was not exercised.

## Inputs And Secrets

The live command requires explicit values for:

- the exact `nokv-fdb` binary;
- an absolute FDB cluster file and a fresh prefix base;
- RustFS/S3 endpoint, bucket, region, and fresh object-root base;
- owner, seed-peer, and client endpoint ranges;
- evidence directory and bounded takeover/operation deadlines.

Credentials are passed to child processes but are never written to evidence.
The evidence contains only the provider endpoint, bucket, region, object-root,
and a digest of the provider namespace binding.

## Failure And Cleanup

The workload fails closed on any malformed route, generation rollback,
unchanged endpoint after the required takeover, unexpected process exit,
deadline, missing wire observation, response mismatch, or final metadata
mismatch.

Cleanup is installed before the first child starts. It terminates both owners
and qualification peers, waits for them, and records their exit status even
after a scenario failure. It does not delete the FDB prefix or RustFS objects;
retained evidence must continue to identify the exact authority that was used.
Runs never reuse an existing prefix, root identity, object namespace, or
evidence directory.

## Evidence

One retained bundle contains:

- source commit and clean/dirty state;
- candidate binary path, SHA-256, and version output;
- Rust version, OS, architecture, and monotonic timing source;
- FDB client/server versions, cluster-file digest, topology, and health before
  and after the run;
- RustFS image or service identity and non-secret namespace configuration;
- owner A/B stdout, stderr, pid, endpoint, start, takeover, and exit records;
- typed peer transcripts and hashes of raw wire frames;
- every route tuple and seed-attempt sequence;
- one result per scenario plus a final `PASS`, `FAIL`, or `NOT QUALIFIED`.

The final result is written last with a temporary-file rename. An interrupted
run therefore has raw diagnostics but no terminal `PASS`.

## Package And File Boundary

The implementation surface is:

```text
bench/Cargo.toml
bench/src/bin/nokv-fdb-seed-qualification.rs
bench/src/seed_qualification/mod.rs
bench/src/seed_qualification/evidence.rs
bench/src/seed_qualification/orchestrator.rs
bench/src/seed_qualification/peer.rs
bench/src/seed_qualification/scenario.rs
```

`bench` may depend on the public `nokv-client` route and transport contracts.
No product crate may depend on `bench`, and no production feature flag, CLI
option, wire field, retry branch, or route fallback is added for the harness.

## Validation And Acceptance

Default repository validation covers argument checks, peer phase transitions,
typed frame generation, evidence fail-closed behavior, cleanup, and scenario
oracle tests without external services. The live workload is explicit and
environment-gated.

After the live run, Gate 7 can change from `NOT QUALIFIED` to `PASS` only when:

1. every scenario in the matrix passed in one retained bundle;
2. the bundle binds the exact source and candidate binary;
3. FDB and RustFS remained healthy or recovered to healthy;
4. no client connected directly to FDB;
5. the repository validation suite passes; and
6. the qualification report links the retained bundle and states that the
   other incomplete FDB gates remain `NOT QUALIFIED`.
