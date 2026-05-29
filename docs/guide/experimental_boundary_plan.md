<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Product Core And Experimental Boundary Plan

## Goal

NoKV should present one product line:

> NoKV is a metadata engine for AI agent workspaces. It exposes a durable,
> versioned, watchable namespace shaped like filesystem metadata, and the same
> contract can be used as a metadata service for distributed filesystems.

The repository should make that product line obvious. Today the codebase mixes
four axes in the same mental model:

- product contract: `fsmeta` namespace metadata
- deployment mode: local and distributed
- distributed substrate: `raftstore`, `coordinator`, `meta/root`, Eunomia
- experimental mechanisms: Peras, Thermos, visible logs, witness quorums, segment
  install, hotspot experiments

The target structure keeps `fsmeta` as the stable product contract and moves
research mechanisms into an explicit `experimental/` area.

## Product Positioning

The stable product is `fsmeta`.

`fsmeta` owns:

- workspace-shaped namespace metadata
- inode, dentry, directory, rename, unlink, and listing semantics
- watch and snapshot semantics
- metadata-to-data pointers stored in inode attributes or future layout values
- a local embedded runtime for teams, demos, and small deployments
- a distributed runtime for DFS-scale metadata and larger agent platforms

The default story should be:

```text
fsmeta API
  -> local runtime
  -> single-node MVCC storage
```

The scale-out story should be:

```text
fsmeta API
  -> raftstore runtime
  -> coordinator + meta/root + raftstore + transaction substrate
```

The research story should be:

```text
fsmeta API
  -> experimental runtime
  -> Peras / Thermos / future research mechanisms
```

DFS metadata is an important target, but it is not the first product sentence.
It proves that the same workspace metadata contract can scale into a serious
distributed metadata service.

## Target Repository Shape

The long-term package map should look like this:

```text
fsmeta/
  client/
  server/
  exec/
  runtime/
    local/
    raftstore/
  internal/
    layout/
    snapshot/
    watch/

local/
storage/
txn/
raftstore/
coordinator/
meta/root/

experimental/
  peras/
    exec/
    runtime/
    raftstore/
    witness/
    spec/
    bench/
  thermos/
    bench/
    docs/
```

The exact final Go package names can be adjusted during migration, but the
ownership rule should not change:

- `fsmeta` is the stable namespace API.
- `local` and `raftstore` are stable storage/runtime substrates.
- `experimental/peras` may depend on stable fsmeta and raftstore interfaces.
- `experimental/thermos` may depend on stable engine observation interfaces.
- stable lower layers should not import experimental packages except through
  explicitly allowlisted migration adapters. The final target is no stable
  imports from `experimental/peras`.

## Stable Versus Experimental

### Stable Core

Stable packages are maintained as product and platform code:

- `fsmeta`
- `fsmeta/runtime/local`
- `fsmeta/runtime/raftstore`
- `engine`
- `local`
- `txn`
- `raftstore`
- `coordinator`
- `meta/root`

Stable packages must have clean contracts, compatibility expectations, and
regular CI coverage.

### Experimental Packages

Experimental packages are allowed to move faster, but they must not leak into
stable package boundaries.

Peras belongs in `experimental/peras` because it is a research mechanism for
visible-before-durable metadata execution:

- visible log
- witness quorum
- segment build and recovery
- segment install
- authority seal experiments
- TLA+ models and benchmark harnesses

Thermos belongs in `experimental/thermos` because its current role is an
optional hotspot detector and throttling experiment, not a required engine
feature.

Experimental packages may still be useful and well-tested. The distinction is
not about quality. It is about the maintenance contract: an experimental
mechanism should be removable without changing the stable product API.

## Boundary Rules

1. `fsmeta` must not import `experimental/peras` outside explicit migration
   adapters.
2. `raftstore` must not import `experimental/peras/exec` outside explicit
   migration adapters.
3. `meta/root` should store generic authority truth, not Peras-specific segment
   truth in the stable state path.
4. `coordinator` should expose generic rooted events and routing views, not
   Peras-specific control paths.
5. Peras-specific fields must not appear in public fsmeta tokens unless they are
   hidden behind a stable opaque evidence type.
6. Raft commands should describe generic storage transitions, not fsmeta/Peras
   internals.
7. Experimental RPCs should be served by experimental services, not by the
   stable `StoreKV` API.
8. CI should run stable product tests by default. Experimental CI should be
   explicit and separately named.

## Peras Migration Plan

### Phase 1: Make The Product Path Boring

Document and configure the default path as local fsmeta:

```text
fsmeta -> runtime/local -> MVCC
```

Actions:

- Update docs to describe local fsmeta as the default product path.
- Keep distributed fsmeta as the scale-out path.
- Mark Peras as experimental in docs and configuration.
- Ensure default benchmarks run local fsmeta unless a distributed or Peras
  profile is requested.
- Keep Buf and protobuf generation workflow unchanged.

Exit criteria:

- README and guide pages do not require understanding Peras to understand NoKV.
- Local fsmeta benchmark and test entry points are obvious.
- Distributed and Peras benchmark profiles are opt-in.

### Phase 2: Hide Peras From Public fsmeta API

Peras currently leaks through public token and watch surfaces. The stable API
should expose visibility, durability, watch, and snapshot contracts without
naming the implementation.

Actions:

- Replace public `PerasSegmentRefs` exposure with an opaque runtime evidence
  field or internal registry.
- Rename Peras-specific watch source values into stable semantic names such as
  `Visible` or `RuntimeVisible`.
- Keep Peras-specific diagnostics under an experimental stats namespace.
- Add contract tests that local and distributed fsmeta return equivalent public
  semantics.

Exit criteria:

- A user can read fsmeta proto/types without seeing Peras-specific concepts.
- Peras can still carry internal recovery evidence.
- Existing snapshot/watch semantics remain test-covered.

### Phase 3: Introduce A Generic Raftstore Install Primitive

Raftstore should install prepared MVCC entries. It should not decode Peras
segments or understand fsmeta namespace semantics.

Actions:

- Add a generic internal command such as `InstallPreparedMVCCEntries`.
- The command should carry:
  - routing key
  - commit version
  - prepared MVCC entries
  - dependency keys
  - idempotency key
  - optional opaque diagnostic label
- Move segment validation, catalog layout, payload digest, and read-header checks
  out of `raftstore`.
- Keep region key-range validation in raftstore.

Exit criteria:

- `raftstore` can validate and apply prepared entries without importing
  `experimental/peras/exec`.
- Existing Peras install path can be reimplemented through the generic command.
- Raftstore tests cover the generic install primitive directly.

### Phase 4: Move Peras Runtime Into `experimental/peras`

Once raftstore exposes a generic install primitive, Peras can become a consumer
of stable interfaces.

Actions:

- Move Peras admission, visible log, flush pipeline, segment build, and recovery
  under `experimental/peras`.
- Keep a narrow adapter from experimental Peras runtime to `fsmeta/exec` if
  needed.
- Make `fsmeta/runtime/raftstore` choose between stable transaction execution
  and experimental Peras execution through configuration.
- Put Peras tests next to the moved implementation.

Exit criteria:

- Stable fsmeta code does not import Peras implementation packages.
- Peras imports stable fsmeta and raftstore interfaces.
- Peras can be disabled without changing the stable runtime.

Current migration state:

- Peras implementation packages have moved to `experimental/peras/exec`,
  `experimental/peras/runtime`, and `experimental/peras/adapters/raftstore`.
- Stable packages still have explicit Peras adapter files. These imports are
  temporary migration edges and should shrink as later phases remove the
  old StoreKV/raft command surface.

### Phase 5: Move Witness Out Of StoreKV

Witness is a Peras recovery/durability mechanism. It should not be a stable
StoreKV RPC surface.

Actions:

- Move witness node and witness WAL code from
  `experimental/peras/adapters/raftstore` into `experimental/peras/witness`.
- Define an experimental witness service if remote witness RPC is still needed.
- Remove `PerasWitnessSegments` and `PerasWitnessProbe` from the stable StoreKV
  service after Peras uses the experimental service.
- Keep witness metrics under an experimental stats namespace.

Exit criteria:

- StoreKV exposes only stable KV/raftstore operations.
- Witness can be started only when the experimental Peras profile is enabled.
- Stable raftstore tests do not construct witness records.

### Phase 6: Generalize Root Authority Or Move It To Experimental

Authority handoff is a stable distributed-safety concept. Peras segment seals are
not necessarily stable product truth.

Actions:

- Decide whether current Peras authority grants are actually generic Eunomia
  authority grants.
- If yes, rename and reshape root state into generic authority fields.
- If no, move Peras authority and seal state into an experimental extension.
- Keep rooted truth responsible for safety-critical ownership facts only.

Exit criteria:

- Root state names do not imply Peras unless the state is truly Peras-specific.
- Peras seal state is not required for normal fsmeta operation.
- Recovery and handoff tests cover the chosen authority boundary.

### Phase 7: Remove Old Peras Raft Commands And Proto Surface

After Peras uses generic install and experimental witness services, delete the
old stable proto surface.

Actions:

- Remove `PerasInstallSegment` from raft command proto.
- Remove StoreKV Peras install and witness RPCs.
- Regenerate protobuf code through the existing Buf workflow.
- Remove raftstore tests that directly build fsmeta Peras segments.
- Replace them with:
  - raftstore generic prepared-install tests
  - Peras segment-to-install tests
  - integration tests for experimental Peras profile

Exit criteria:

- `rg "experimental/peras/exec" raftstore` returns no production imports.
- `rg "PerasInstallSegment" raftstore pb/raft pb/kv` returns no stable command
  surface.
- Buf generation leaves the tree clean.

### Phase 8: Add Boundary Tests

The new structure should be enforced mechanically.

Actions:

- Add an import-boundary script under `tools/lint` for:
  - `raftstore/...` must not import `fsmeta/...`
  - stable packages must not import `experimental/...`
  - `fsmeta` domain model must not import `fsmeta/runtime/...`
  - `engine` and `local` must not import distributed or fsmeta packages
- Add a docs check that default benchmark docs do not require Peras.

Exit criteria:

- Boundary violations fail CI.
- Reviewers do not need to catch this class of drift manually every time.

## Thermos Migration Plan

Thermos should move more gently than Peras because it is already narrower and
less entangled with raftstore.

Actions:

- Move optional detector code into `experimental/thermos`.
- Keep a stable engine observation interface for write-hot events.
- Keep production throttling disabled by default unless explicitly configured.
- Keep stats export stable enough for operators, but mark Thermos stats as
  experimental.
- Remove Thermos from storage-engine docs that describe the default read/write
  path.

Exit criteria:

- Engine hot path does not require Thermos.
- Thermos can be built and benchmarked as an experiment.
- Default engine documentation is understandable without Thermos.

## PR Split

This work should not be one large PR.

Recommended PR sequence:

1. Docs and configuration positioning: local fsmeta default, distributed as
   scale-out, Peras/Thermos experimental.
2. Public fsmeta API cleanup: hide Peras-specific token/watch fields behind
   stable semantics.
3. Generic raftstore prepared-install primitive.
4. Peras runtime migration to generic install.
5. Move witness service out of StoreKV.
6. Root authority generalization or Peras root extension.
7. Delete old Peras raft/proto surfaces.
8. Move Thermos under `experimental/thermos`.
9. Add import-boundary CI checks.

Each PR should state:

- which boundary it changes
- whether it changes public API
- what recovery or authority invariant is affected
- exact validation commands
- benchmark evidence if performance claims are made

## Validation Matrix

Stable path:

```bash
make fmt
make lint
make test
go test -count=1 ./fsmeta/... ./local/... ./storage/... ./txn/... ./raftstore/...
```

Distributed path:

```bash
go test -count=1 ./coordinator/... ./meta/root/... ./raftstore/...
go test -count=1 ./fsmeta/runtime/raftstore/...
```

Experimental Peras path:

```bash
go test -count=1 ./experimental/peras/...
```

Experimental Thermos path:

```bash
go test -count=1 ./experimental/thermos/...
```

Proto changes:

```bash
buf generate
git diff --exit-code -- pb
```

Benchmark evidence should be reported separately for:

- local fsmeta
- distributed fsmeta
- experimental Peras profile

Do not compare Peras and non-Peras results without listing the runtime profile,
durability boundary, and benchmark configuration.

## Risks

### Risk: Moving Too Much At Once

The largest risk is mixing public API cleanup, raftstore command changes, root
authority changes, and file moves in the same PR.

Mitigation: follow the PR split above. Keep compatibility only inside a single
PR when it is needed to migrate callers, and delete it before the PR lands when
possible.

### Risk: Weakening Recovery Semantics

Peras currently carries recovery evidence through witness records, segment
catalogs, root seals, and snapshot references. Moving it can accidentally lose a
durability boundary.

Mitigation: before moving each piece, write down:

- authority owner
- visibility boundary
- durability boundary
- recovery source
- GC condition
- duplicate-request behavior

### Risk: Turning `experimental/` Into A Dumping Ground

`experimental/` must not become a place for unclear code. It should have stricter
boundaries, not looser ownership.

Mitigation: every experimental package still needs a `doc.go`, tests, metrics
ownership, and a clear statement of what stable package interfaces it consumes.

## Desired End State

A new contributor should be able to understand NoKV in this order:

1. `fsmeta` is the product contract.
2. local runtime is the default deployment.
3. distributed runtime is the scale-out deployment.
4. raftstore/root/coordinator provide distributed substrate.
5. Peras and Thermos are optional experiments under `experimental/`.

The code should match that story:

- stable packages do not depend on experimental packages
- raftstore does not understand fsmeta Peras segments
- public fsmeta API does not expose Peras-specific internals
- default benchmarks and CI measure the stable path
- experimental benchmarks and CI are explicit
