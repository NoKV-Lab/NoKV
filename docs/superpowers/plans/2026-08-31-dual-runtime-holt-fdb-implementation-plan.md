<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Dual Runtime Holt And FoundationDB Implementation Plan

**Approved design:**
[`2026-08-31-dual-runtime-holt-fdb-serving-design.md`](../specs/2026-08-31-dual-runtime-holt-fdb-serving-design.md)

**Starting commit:** `8df10c66`

**Delivery rule:** each task is a reviewable commit with DCO. No intermediate
commit may expose an unfenced FoundationDB serving route. FoundationDB remains
`NOT QUALIFIED` until Task 12 retains real-cluster evidence for every applicable
gate.

## Outcome

Deliver exactly two explicit metadata runtimes:

```text
holt:///absolute/path
fdb:///absolute/fdb.cluster?prefix=nokv-prod
```

Standalone mode owns one exclusively opened Holt store and no control plane.
Distributed mode uses FoundationDB for metadata, catalog, routes, owner
sessions, heartbeats, and leases. Clients discover routes through NoKV seed
servers and have no FoundationDB or control-adapter dependency. etcd and every
superseded public surface are removed.

## Working Rules

- Keep `nokv-meta` provider-neutral. Capability checks are allowed; matching on
  provider names is not.
- Keep FoundationDB runtime and types below `nokv-server`; do not leak them into
  protocol, client, Agent, object, or Python packages.
- Preserve one workspace schema and one implementation of each lifecycle state
  machine.
- Preserve unrelated worktree changes. This branch currently has no unrelated
  changes.
- Use explicit format/open/provision operations. Never add fallback or implicit
  migration while transitioning the CLI.
- Retain raw affected-byte evidence for transaction-shape claims.
- Report environment-gated FDB checks as `PASS`, `FAIL`, or `NOT RUN`.

## Task 1: Add Strict Metadata URL Types

**Files**

- Modify: `Cargo.toml`
- Modify: `crates/nokv-server/Cargo.toml`
- Modify: `crates/nokv-server/src/lib.rs`
- Create: `crates/nokv-server/src/metadata_url.rs`

**Implementation**

1. Add the standard `url` parser as a workspace dependency; keep it independent
   of FoundationDB features.
2. Add public, immutable `MetadataUrl`, `HoltMetadataUrl`, and
   `FoundationDbMetadataUrl` types owned by server startup configuration.
3. Implement `FromStr` with exact scheme dispatch and structured
   `MetadataUrlError` variants.
4. For `holt://`, require an empty authority, absolute non-empty decoded path,
   and no query or fragment.
5. For `fdb://`, require an empty authority, absolute UTF-8 cluster-file path,
   one strict UTF-8 `prefix` of 1 through 64 bytes, and no other query or
   fragment.
6. Preserve the typed FDB URL in default builds. Provider selection later
   returns an explicit unsupported error when the binary lacks FDB support.
7. Add table tests for valid paths, percent encoding, Unicode prefixes,
   missing/duplicate/unknown parameters, authority/userinfo/port, relative or
   empty paths, fragments, and unsupported schemes.

**Acceptance**

```bash
cargo test -p nokv-server metadata_url
cargo check -p nokv-server
git diff --check
```

**Commit:** `feat: add metadata runtime URLs`

## Task 2: Advertise Planner Target And Recovery Journal Capability

**Files**

- Modify: `crates/nokv-meta-store/src/types.rs`
- Modify: `crates/nokv-meta-store/src/conformance.rs`
- Modify: every `StoreProfile` constructor under `crates/`
- Modify: `docs/development/metadata-store-interface.md`
- Modify: `docs/development/code_contract.md`

**Implementation**

1. Extend `StoreProfile` with a preferred transaction-planning target bounded
   by `limits.max_transaction_bytes`.
2. Add a provider-neutral recovery-journal capability that distinguishes a
   required local receipt/outbox from a shared-store commit authority.
3. Set Holt and test-local targets to their existing hard transaction limit and
   require the local journal.
4. Set the characterization FDB target to 900,000 bytes and declare shared
   authority without a local journal. Keep its existing conservative hard
   physical guard.
5. Make conformance reject zero targets, targets above the hard limit, and
   authority/journal combinations that cannot satisfy acknowledgement and
   recovery semantics.
6. Do not weaken `MetaShard::bind`; FDB remains rejected at this task.

**Acceptance**

```bash
cargo test -p nokv-meta-store
cargo test -p nokv-meta-holt
cargo test -p nokv-meta-fdb
cargo test -p nokv-meta workspace::engine
git diff --check
```

**Commit:** `feat: advertise metadata planning capabilities`

## Task 3: Add Format-11 Store Admission And Recovery Receipts

**Files**

- Modify: `crates/nokv-meta/src/workspace/codec.rs`
- Modify: `crates/nokv-meta/src/workspace/engine.rs`
- Modify: `crates/nokv-meta/src/workspace/recovery.rs`
- Modify: the dedupe-record owner under `crates/nokv-meta/src/workspace/`
- Modify: `docs/metadata-schema.md`
- Modify: `docs/development/metadata-store-interface.md`

**Implementation**

1. Advance the exact workspace marker to format 11. Retain no format-10 read or
   write path.
2. Replace the mandatory recovery LSN/digest fields in the dedupe record with
   an optional, typed local recovery receipt.
3. Build command transactions from the bound store's journal capability:
   local authority writes the receipt and `RecoveryOutbox`; shared authority
   writes neither while retaining the same request ID, command digest, result,
   and atomic dedupe fence.
4. Make `command_fit` estimate the transaction actually selected by the bound
   profile instead of always charging `RecoveryOutbox`.
5. Preserve exact replay and request-ID mismatch behavior in both profiles.
6. Add format-10 rejection, local-receipt, shared-no-receipt, replay,
   corruption, and command-fit regression tests.

**Acceptance**

```bash
cargo test -p nokv-meta recovery
cargo test -p nokv-meta command_fit
cargo test -p nokv-server recovery
cargo test -p nokv-meta-holt
git diff --check
```

**Commit:** `feat: add shared-authority recovery receipts`

## Task 4: Implement Secondary Index V2

**Files**

- Modify: `crates/nokv-meta/src/workspace/query_records.rs`
- Modify: `crates/nokv-meta/src/workspace/publication.rs`
- Modify: `crates/nokv-meta/src/workspace/rename.rs`
- Modify: `crates/nokv-meta/src/workspace/remove.rs`
- Modify: `crates/nokv-meta/src/workspace/restore.rs`
- Modify: query execution and operation-record owners under
  `crates/nokv-meta/src/workspace/`
- Modify: `docs/metadata-schema.md`
- Add or modify: metadata transaction-shape tests and retained evidence

**Implementation**

1. Add `index_generation` and canonical 256-bit path digest to the authoritative
   path record.
2. Add a one-per-path-generation locator containing the full normalized path.
   Exact-create predicates reject a digest collision with another path.
3. Change secondary index keys to carry field, ordered scalar, workspace,
   digest, and generation. Store no full projection or full path in each row.
4. Add durable, resumable operation state for bounded locator/index staging.
5. Make the final command atomically publish or flip `PathCurrent`, normal
   history/event/reference mutations, operation state, and dedupe result.
6. Resolve query candidates through bounded locator and current-path batches;
   return only exact generation/digest matches.
7. Move old-row deletion to generation-fenced asynchronous cleanup.
8. Apply the same visibility protocol to create, replace, rename, restore,
   projection change, remove, replay, and cleanup.
9. Retain encoded affected-byte evidence proving every tested final/staging
   transaction is at or below 900,000 bytes for the approved maximum matrix.

**Acceptance**

```bash
cargo test -p nokv-meta publication
cargo test -p nokv-meta query
cargo test -p nokv-meta rename
cargo test -p nokv-meta remove
cargo test -p nokv-meta restore
cargo test -p nokv-meta transaction_shape
git diff --check
```

**Commit:** `feat: stage bounded secondary indexes`

## Task 5: Add The Process-Global FoundationDB Runtime

**Files**

- Create: `crates/nokv-fdb/Cargo.toml`
- Create: responsibility-named modules under `crates/nokv-fdb/src/`
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Modify: `crates/nokv-meta-fdb/Cargo.toml`
- Modify: `crates/nokv-meta-fdb/src/`
- Modify: `docs/development/code_contract.md`
- Modify: FDB characterization specification/status notes

**Implementation**

1. Move API-version selection, one-time network boot, network-thread lifetime,
   database opening, common options, prefix envelope, and common error
   classification into `nokv-fdb`.
2. Make network boot process-global and non-restartable. Multiple metadata and
   control handles share the same runtime guard.
3. Keep all FoundationDB imports behind a non-default feature and prove the
   default workspace does not link `libfdb_c`.
4. Refactor `nokv-meta-fdb` to use the shared runtime without changing its
   `TxnStore` conformance behavior.
5. Keep automatic raw commit retries forbidden.

**Acceptance**

```bash
cargo test -p nokv-fdb
cargo test -p nokv-meta-fdb
cargo check -p nokv-fdb --features fdb
cargo check -p nokv-meta-fdb --features fdb
git diff --check
```

Run the environment-gated live conformance when the FDB client and test cluster
are available; record `NOT RUN` otherwise.

**Commit:** `refactor: share the fdb process runtime`

## Task 6: Implement FoundationDB Catalog And Ownership Control

**Files**

- Create: `crates/nokv-control-fdb/Cargo.toml`
- Create: responsibility-named modules under `crates/nokv-control-fdb/src/`
- Modify: `Cargo.toml`
- Modify: `crates/nokv-control/src/` provider-neutral types and store contract
- Modify: `docs/development/code_contract.md`
- Modify: `docs/development/metadata-store-interface.md`

**Implementation**

1. Add versioned, component-safe encodings for store manifest, root catalog,
   shard catalog, route, session, and heartbeat subspaces.
2. Add explicit `Unassigned`, `Activating`, `Serving`, and fail-closed
   transitions without path or artifact semantics.
3. Implement create-only provisioning and exact CAS transitions.
4. Implement first acquisition, TTL observation using local monotonic time,
   takeover CAS, checked owner/session increments, heartbeat renewal, route
   activation, release, and corrupt-record rejection.
5. Keep session and heartbeat keys separate. Acquisition reads both; metadata
   transactions later check only the stable session key.
6. Add deterministic clock/observer tests and live concurrent-contender tests.

**Acceptance**

```bash
cargo test -p nokv-control
cargo test -p nokv-control-fdb
cargo check -p nokv-control-fdb --features fdb
git diff --check
```

**Commit:** `feat: add fdb catalog and ownership control`

## Task 7: Add Versioned Seed Discovery

**Files**

- Modify: `crates/nokv-protocol/src/request.rs`
- Modify: `crates/nokv-protocol/src/response.rs`
- Modify: `crates/nokv-protocol/src/types.rs`
- Modify: `crates/nokv-protocol/src/error.rs`
- Modify: `crates/nokv-protocol/src/codec.rs`
- Modify: `crates/nokv-server/src/server.rs`
- Modify: `crates/nokv-client/src/route.rs`
- Modify: `crates/nokv-client/src/transport.rs`
- Modify: `crates/nokv-client/src/sdk.rs`

**Implementation**

1. Introduce a top-level request/response envelope that separates discovery
   from routed workspace requests; bump the exact wire schema.
2. Add a storage-neutral discovered route with owner endpoint and session
   generation. Validate endpoint, identities, nonzero generations, and serving
   state at the protocol boundary.
3. Add a server discovery source trait. Holt and FDB composition will provide
   implementations later; protocol tests use deterministic in-memory sources.
4. Add `SeedRouteResolver` with ordered seed rotation, bounded retry/backoff,
   per-root caching, monotonic generation replacement, stale-hint rejection,
   and refresh after `NotOwner` or transport failure.
5. Ensure `nokv-client` no longer needs `nokv-control` for authoritative route
   refresh after this path is complete.

**Acceptance**

```bash
cargo test -p nokv-protocol
cargo test -p nokv-client route
cargo test -p nokv-client transport
cargo test -p nokv-server discovery
git diff --check
```

**Commit:** `feat: discover routes through nokv seeds`

## Task 8: Fence FDB Metadata With The Exact Session

**Files**

- Modify: `crates/nokv-meta-fdb/src/options.rs`
- Modify: `crates/nokv-meta-fdb/src/store.rs`
- Modify: `crates/nokv-meta-fdb/src/profile.rs`
- Modify: adapter conformance and live FDB tests
- Modify: `docs/development/metadata-store-interface.md`

**Implementation**

1. Require an immutable logical-shard session key and expected
   `(owner_epoch, session_generation)` when opening a serving FDB store.
2. Add the exact session read-conflict check to every owner-required read and
   write transaction. Do not read the heartbeat key.
3. Map a mismatching or absent session to typed not-owner/fenced behavior; do
   not surface it as a generic conflict that raw transaction code retries.
4. Add tests proving ordinary heartbeat updates do not conflict, while one
   takeover fences an already-open old store on its next read and write.
5. Retain characterization-only construction separately only if tests need it,
   with an explicit removal condition before serving qualification.

**Acceptance**

```bash
cargo test -p nokv-meta-fdb
cargo test -p nokv-meta-fdb --features fdb --test fdb_conformance
git diff --check
```

**Commit:** `feat: fence fdb metadata sessions`

## Task 9: Compose Explicit Holt And FDB Runtimes

**Files**

- Modify: `crates/nokv-server/src/lib.rs`
- Refactor: `crates/nokv-server/src/bootstrap.rs`
- Modify: `crates/nokv-server/src/server.rs`
- Modify/remove: recovery installer/publisher modules and exports
- Modify: `crates/nokv-server/Cargo.toml`
- Modify: `crates/nokv/src/cli.rs`
- Modify: `crates/nokv/src/main.rs`
- Modify: `crates/nokv/Cargo.toml`
- Add: feature-enabled `nokv-fdb` binary target

**Implementation**

1. Add explicit `format`, `provision`, and `serve` composition APIs over
   `MetadataUrl`; opening never creates or changes provider.
2. Holt format creates one store identity and one logical shard. Serve takes an
   OS-backed exclusive lock, reopens the exact path, advances local root
   fences, completes local recovery, and then publishes local discovery.
3. FDB format/provision use the shared manifest/catalog and remain unavailable
   without the FDB feature.
4. FDB serve starts the process-global runtime, observes/acquires a session,
   opens the session-fenced metadata store, activates root fences in bounded
   batches, starts workers, and publishes `Serving` last.
5. On renewal uncertainty or session loss, remove routes and stop admission
   before returning the error.
6. Remove distributed local-log installer/publisher wiring; Holt keeps only
   its local recovery authority.
7. Replace old metadata create/reopen/recover CLI selection with one required
   `--meta-url`. Replace static/etcd client configuration with repeatable
   `--seed` endpoints while keeping the test-only Rust static resolver.

**Acceptance**

```bash
cargo test -p nokv-server
cargo test -p nokv
cargo test -p nokv --test cli_help
cargo check -p nokv --no-default-features
cargo check -p nokv --features fdb
git diff --check
```

**Commit:** split into one Holt composition commit and one FDB composition
commit if the diff crosses both runtime boundaries.

## Task 10: Remove Etcd And Superseded APIs

**Files**

- Remove: `crates/nokv-control/src/etcd.rs`
- Modify: `Cargo.toml`, `Cargo.lock`, and affected crate manifests
- Modify: `crates/nokv-control/src/lib.rs` and options
- Modify: `crates/nokv-client/src/route.rs` and exports
- Modify: `crates/nokv-python/` routing API/tests
- Modify: CLI tests, acceptance scripts, examples, and current documentation

**Implementation**

1. Delete `etcd-client`, all `etcd` and obsolete `control` feature wiring, etcd
   options/types/connectors, CLI flags, Python constructors, and server gates.
2. Delete old static distributed CLI pins and recovery-publication flags.
3. Replace current documentation and acceptance wiring with Holt/FDB mode
   terminology. Preserve clearly marked historical evidence only when policy
   requires it.
4. Add a repository check that fails if product manifests or source files
   reintroduce etcd.

**Acceptance**

```bash
rg -n "etcd-client|EtcdControlStore|EtcdRouteOptions|--etcd-" Cargo.toml Cargo.lock crates
cargo tree --workspace | rg "etcd" && exit 1 || true
cargo test --workspace
git diff --check
```

The first `rg` must produce no product-code matches. Any retained historical
documentation match is reviewed manually and cannot affect current behavior.

**Commit:** `refactor: remove etcd control paths`

## Task 11: Update Contracts And Dual-Mode Acceptance

**Files**

- Modify: `docs/development/code_contract.md`
- Modify: `docs/development/pr_review_checklist.md`
- Modify: `docs/development/workspace-acceptance.md`
- Modify: `docs/development/metadata-store-interface.md`
- Modify: `docs/metadata-schema.md`
- Modify: product README and CLI examples
- Add/modify: isolated Holt/FDB acceptance runners and evidence schemas

**Implementation**

1. Make the final package graph and provider responsibilities normative.
2. Replace etcd/local-WAL failover claims with standalone Holt and shared FDB
   gates.
3. Add explicit manifest, seed discovery, session fencing, unknown outcome,
   index staging, affected-byte, exclusive-open, and takeover gates.
4. Ensure LingTai remains the active integration target; do not route around or
   add compatibility for historical Yanex material.
5. Keep FDB `NOT QUALIFIED` in public docs until Task 12 succeeds.

**Acceptance**

```bash
python3 scripts/workbench/workbench_contract_test.py
git diff --check
```

Validate every changed Markdown link.

**Commit:** `docs: define dual runtime acceptance`

## Task 12: Qualification And Final Review

**Validation**

1. Run the full required repository suite:

   ```bash
   cargo fmt --all -- --check
   cargo clippy --workspace --all-targets -- -D warnings
   cargo test --workspace
   python3 scripts/workbench/workbench_contract_test.py
   git diff --check
   ```

2. On a pinned FoundationDB 7.3 environment, run live adapter, control,
   discovery, multi-owner, crash-transition, takeover, and unknown-outcome
   suites at least three times.
3. Run the approved maximum transaction matrix and retain logical plus
   conservative physical affected-byte evidence. Every FDB serving transaction
   must remain at or below 900,000 bytes.
4. Run standalone Holt format/provision/serve/restart/exclusive-open/recovery
   acceptance.
5. Run FDB steady-state and failover benchmarks with the exact cluster,
   workload, concurrency, payload, binary, and durability profile retained.
6. Review the exact changed files against `code_contract.md` and
   `pr_review_checklist.md`; report findings first and verify DCO on every
   non-merge commit.
7. Change FDB qualification from `NOT QUALIFIED` only if every applicable gate
   has retained `PASS` evidence. Otherwise leave the honest blocker in place.

## Commit Order

The intended dependency order is:

```text
metadata URL
  -> store capabilities
  -> format 11 recovery receipt
  -> SecondaryIndexV2
  -> shared FDB runtime
  -> FDB control
  -> seed discovery
  -> session-fenced metadata
  -> Holt/FDB server composition
  -> etcd removal
  -> contracts and qualification
```

If a task reveals a missing invariant, update the approved design before
changing behavior. Do not solve sequencing pressure with a compatibility shim,
silent fallback, or temporary unfenced serving path.
