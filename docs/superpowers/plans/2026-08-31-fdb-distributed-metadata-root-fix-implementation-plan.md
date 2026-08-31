<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# FoundationDB Distributed Metadata Root-Fix Implementation Plan

**Approved design:**
[`2026-08-31-fdb-distributed-metadata-root-fix-design.md`](../specs/2026-08-31-fdb-distributed-metadata-root-fix-design.md)

**Starting commit:** `6c8043b3`

**Delivery rule:** each root cause is a reviewable commit with DCO. Do not add
retry-count workarounds, Ready-state rollback, compatibility wrappers, or
weakened transaction predicates. FoundationDB remains `NOT QUALIFIED` until
the final task retains real-cluster evidence for the complete acceptance
matrix.

## Outcome

Make the distributed metadata path recoverable at its two confirmed failure
boundaries:

1. Provision a root as `Provisioning`, release its owner session, admit the
   object namespace, then reacquire ownership and finalize the catalogs as
   `Ready`.
2. Make a durable secondary-index stage replay depend only on immutable staged
   write intent, while keeping the first-stage transaction's exact predicates
   over the current operation, workspace, and path payloads.

## Working Rules

- Keep FoundationDB runtime and control types below `nokv-server`; do not leak
  them into protocol, client, Agent, object, or Python packages.
- Keep object-provider configuration in the CLI orchestration layer.
- Never call an object provider while an FDB owner session is live.
- Retain the process-global FDB runtime across prepare and finalize; dropping
  the final runtime guard makes the network permanently non-restartable in the
  process.
- Preserve exact catalog identity, root fence, transaction-shape, and dedupe
  checks.
- Use a fresh execution context/read version and rebuild the final command on
  every retry after a durable stage replay.
- Preserve unrelated worktree changes and report environment-gated checks as
  `PASS`, `FAIL`, or `NOT RUN`.

## Task 1: Split FoundationDB Provision Into Prepare And Finalize

**Files**

- Modify: `crates/nokv-server/src/fdb_runtime.rs`
- Modify: `crates/nokv-server/src/lib.rs`
- Modify: focused server tests in `crates/nokv-server/src/fdb_runtime.rs`

**Implementation**

1. Add a prepared-provision handle that owns the process-global FDB runtime,
   control store, URL, exact root catalog, exact shard catalog, and preexisting
   outcome state.
2. Change preparation to create or validate the `Provisioning` catalogs,
   acquire the exact shard session, initialize/open metadata, advance the
   shared owner fence, reconcile the root fence, and release the session before
   returning the handle.
3. Leave both catalogs `Provisioning` until external namespace admission has
   succeeded. An already-Ready root is returned through the same handle so the
   caller still revalidates its object namespace.
4. Add `finalize_after_namespace_admission`: reread and validate the exact
   catalogs, reject invalid partial states, reacquire a fresh exact session,
   reopen and reconcile the metadata fence, CAS root then shard to `Ready`, and
   release the session on every result path.
5. Preserve unknown-outcome reconciliation and make repeated prepare/finalize
   calls converge without Ready rollback.

**Acceptance**

```bash
cargo test -p nokv-server --features fdb fdb_runtime
cargo check -p nokv-server --features fdb
git diff --check
```

**Commit:** `fix: make fdb provisioning recoverable`

## Task 2: Admit The Object Namespace Between Provision Phases

**Files**

- Modify: `crates/nokv/src/main.rs`
- Modify: CLI/startup tests where available

**Implementation**

1. Build and validate local object-store configuration before mutating FDB.
2. Call FDB prepare and obtain the exact object namespace from the prepared
   root.
3. Ensure the namespace marker, bind the namespace, and validate Agent object
   capabilities while no owner session exists.
4. Invoke finalization only after namespace admission succeeds.
5. Keep already-Ready roots on the same admission path so a missing or invalid
   object marker fails before serving.

**Acceptance**

```bash
cargo test -p nokv --features fdb
cargo check -p nokv --features fdb
git diff --check
```

**Commit:** include with Task 1 in `fix: make fdb provisioning recoverable`

## Task 3: Bind Durable Index-Stage Replay To Immutable Intent

**Files**

- Modify: `crates/nokv-meta/src/workspace/publication.rs`
- Modify: publication/replay tests in the same module
- Modify: `docs/metadata-schema.md` if the stored replay version changes

**Implementation**

1. Replace the stage-input digest with an immutable-intent digest containing
   the root and operation identity, stable operation/workspace/path keys,
   locator key and payload, and ordered secondary-index rows.
2. Exclude the operation payload, workspace payload, and current path payload
   from replay identity because unrelated revision, heartbeat, or clock changes
   legitimately replace those bytes after the stage commits.
3. Keep the first stage transaction's exact assertions over every volatile
   payload; only replay validation changes.
4. Version the durable replay meaning explicitly and fail closed on old or
   malformed in-flight results; add no compatibility shim while FDB is not
   qualified.
5. On replay, create a fresh execution context/read version and rebuild the
   final command against the current authoritative records.

**Acceptance**

```bash
cargo test -p nokv-meta finalize_resumes_a_durable_index_stage
cargo test -p nokv-meta secondary_index_stage
cargo test -p nokv-meta publication
git diff --check
```

**Commit:** `fix: replay durable fdb index stages`

## Task 4: Prove Recovery, Concurrency, And Failover On Real Services

**Files**

- Modify: retained qualification evidence under `target/qualification/`
- Modify: qualification/status documentation only after the evidence exists

**Implementation**

1. Run unit and workspace regression suites with the exact FDB client library
   used by the live cluster.
2. On a fresh FDB prefix, inject failures after catalog preparation, during
   object namespace admission, after root-Ready CAS, and after durable index
   staging; prove reruns converge without manual cleanup.
3. Run concurrent writers against two NoKV servers and require zero semantic
   replay mismatches under the approved retry budget.
4. Kill the active owner and prove session fencing plus seed failover; then
   stop one FDB node and prove write/read continuity under the configured
   redundancy mode.
5. Retain logs, catalog snapshots, transaction-shape evidence, container and
   library versions, exact commands, and an explicit `PASS`/`FAIL`/`NOT RUN`
   matrix.
6. Keep the distributed metadata mode `NOT QUALIFIED` if any required gate is
   missing, flaky, or fails.

**Acceptance**

```bash
cargo test --workspace
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
git diff --check
```

Plus the retained real-Holt/RustFS/FDB recovery, concurrency, owner-failover,
seed-failover, FDB-node-loss, and transaction-boundary evidence described
above.

**Commit:** `test: qualify fdb metadata root fixes`
