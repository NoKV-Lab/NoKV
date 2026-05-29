<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# NoKV Code Contract

This document is the repository-level code contract for NoKV. It is stricter
than a Go style guide: it defines ownership boundaries, file placement, naming,
error taxonomy, metrics ownership, tests, and PR hygiene. Contributors and
review agents must check this document before approving a change.

NoKV accepts breaking changes when they keep the codebase simpler and safer.
Do not add compatibility shims, forwarding wrappers, deprecated aliases, or
dual execution paths unless the PR explains the operational need and the
removal condition.

## 1. Pull Request Scope

- One PR changes one logical boundary.
- Do not mix behavior changes, broad refactors, benchmarks, generated-code
  rewrites, and documentation churn unless they are inseparable.
- A root/control-plane fix must not include unrelated fsmeta, Peras, engine, or
  benchmark changes.
- A performance PR must include benchmark evidence and must not hide semantic
  changes.
- A recovery or failover PR must include the crash/failover boundary it changes.
- Every non-merge commit must include a DCO `Signed-off-by` trailer.

Use:

```bash
git commit -s -m "fix: catch up root leader before writes"
```

If a commit already exists without DCO:

```bash
git commit --amend -s --no-edit
git rebase --signoff origin/main
```

## 2. Package Boundaries

Package boundaries follow ownership of truth, not convenience.

| Package | Owns | Must Not Do |
| --- | --- | --- |
| `storage/kv/` | Raw ordered key/value backend contract. | Expose MVCC, fsmeta, raftstore, migration, SST, or protobuf semantics. |
| `storage/pebble/`, `storage/holt/`, `storage/memory/` | Concrete raw ordered KV backends. | Own MVCC timestamps, column families, transactions, fsmeta layout, raftstore routing, or migration policy. |
| `storage/wal/`, `storage/file/`, `storage/vfs/` | Low-level file, WAL, and VFS support for concrete runtime internals. | Become public fsmeta, txn, raftstore, or migration contracts. |
| `local/` | Embedded DB facade and local runtime assembly. | Know fsmeta, coordinator, root, or raftstore semantics. |
| `txn/storage/` | MVCC internal keys, column families, timestamp encoding, entries, and transaction storage contract. | Import Percolator, raftstore, fsmeta, coordinator, root, or concrete storage backends. |
| `txn/` | Transaction protocol layers. | Let `mvcc`, `storage`, or `latch` depend on Percolator or raftstore. |
| `raftstore/` | Region Raft execution and data-plane apply. | Interpret fsmeta namespace semantics. |
| `raftstore/snapshot/` | Raftstore-internal MVCC entry snapshot protocol for peer bootstrap and raft snapshot apply. | Import concrete storage engines, local DB, fsmeta, migration, or SST/table/manifest implementations. |
| `meta/root/` | Rooted truth for authority, topology, grants, seals, and lifecycle facts. | Import coordinator service/client packages. |
| `coordinator/` | Rebuildable control-plane view, routing, and service orchestration. | Become the source of truth for rooted facts. |
| `fsmeta/model/` | Storage-engine-neutral namespace model: inode/dentry records, operation request/result shapes, and model validation. | Import key/value layout, protobuf, raftstore, coordinator, root, Peras, or concrete backend packages. |
| `fsmeta/layout/` | Ordered-key backend layout: key/value codecs, key-family kinds, affinity buckets, placement ranges, and operation key plans. | Own namespace semantics, protobuf, raftstore, coordinator, root, Peras, or concrete backend clients. |
| `fsmeta/backend/` | Storage-engine-neutral MVCC metadata backend contract consumed by fsmeta execution. | Import protobuf, engine, local, raftstore, coordinator, root, Peras, or concrete backend packages. |
| `fsmeta/observe/` | Runtime-neutral watch and snapshot observation surfaces: requests, cursors, events, subscriptions, apply notifications, and snapshot publication hooks. | Own namespace model objects, storage runtimes, protobuf conversion, raftstore, coordinator, root, Peras, or backend clients. |
| `fsmeta/` | Package-level architecture anchor. | Re-own model/layout/observe types or add forwarding aliases for `fsmeta/model`, `fsmeta/layout`, or `fsmeta/observe`. |
| `fsmeta/exec/` | Semantic execution, compiler, and holder logic over the `fsmeta/backend` contract. | Import protobuf, `raftstore`, `coordinator`, `meta/root`, or concrete backend packages. |
| `fsmeta/runtime/` | Runtime adapters that bind fsmeta execution to storage backends. | Reinterpret compiler semantics without going through the compiler contract. |
| `cmd/` | Binary assembly, flags, env, and config wiring. | Contain core protocol or storage logic. |
| `pb/`, `*/wire/` | Proto definitions and conversion glue. | Leak protobuf structs into storage or semantic cores when a domain type exists. |
| `utils/` | Domain-neutral helpers shared by multiple packages. | Import global error taxonomy or own storage, raft, fsmeta, coordinator, or root semantics. |

If a lower layer needs a higher-layer operation, define a narrow interface at
the caller boundary instead of importing the higher layer.

Every new package must have a clear owner and one sentence of responsibility in
`doc.go` or the package-level comment. Reviewers should be able to answer:

- What authoritative state, if any, does this package own?
- Which package is allowed to mutate that state?
- Which package is allowed to observe it?
- Which lower-layer packages may it import?
- Which higher-layer packages must not import it?

The current responsibility map is:

- `storage/kv/*`: raw ordered KV contract only.
- `storage/pebble/*`: default Pebble-backed raw KV implementation.
- `storage/holt/*`: owned Holt backend adapter once it is wired into this repo.
- `storage/memory/*`: test raw KV implementation.
- `storage/wal`, `storage/file`, `storage/vfs`: low-level support packages used
  by concrete runtimes. They are not fsmeta or migration contracts.
- `fsmeta/cache/*`: derived namespace sidecar caches such as dirpage and
  negative-cache slabs. They can be rebuilt and do not own authoritative
  namespace truth.
- `local/*`: embedded DB assembly around the raw storage backend, NoKV MVCC key
  encoding, local stats, workdir mode, and commit queues. It may collect local
  stats, but it does not own distributed truth.
- `txn/mvcc`, `txn/storage`, `txn/latch`: reusable transaction building blocks.
  They remain protocol-neutral.
- `txn/percolator`: 2PC/MVCC protocol logic on top of transaction primitives.
- `raftstore/*`: replicated region execution, apply, split/merge, peer
  lifecycle, and internal install commands.
- `raftstore/snapshot/*`: raftstore-internal MVCC entry snapshot payloads for
  peer bootstrap and raft snapshot apply. Operator migration and SST fast paths
  are not part of the mainline backend contract.
- `meta/root/*`: rooted truth for cluster and metadata authority facts.
- `coordinator/*`: rebuildable serving layer over root facts.
- `fsmeta/model/*`: storage-engine-neutral inode/dentry/session/quota/snapshot
  model objects, operation request/result types, and model validation.
- `fsmeta/layout/*`: namespace key layout, value codecs, placement planning,
  and operation key plans for ordered storage backends.
- `fsmeta/backend/*`: minimal MVCC metadata backend contract. It contains
  backend-neutral key/value, mutation, predicate, atomic mutation, and stats
  surfaces only. Migration, SST ingest/export, LSM diagnostics, raw storage
  stats, and raftstore RPC conversion remain in concrete runtime or operations
  packages.
- `fsmeta/observe/*`: runtime-neutral watch and snapshot observation surfaces.
- `fsmeta/*`: package-level architecture anchor only.
- `fsmeta/exec/*`: semantic compiler, executor, and runtime-neutral holder
  logic. It may depend on `fsmeta/backend`, but it must not import protobuf or
  concrete storage runtimes.
- `fsmeta/runtime/*`: concrete runtime bindings from fsmeta execution to
  raftstore or other storage backends.
- `experimental/peras/*`: Peras admission, visible-log, witness, segment, and
  recovery experiments.
- `experimental/thermos/*`: optional Thermos hotspot/admission experiments.
- `metrics/*`: reusable metric value types, not subsystem ownership.
- `*/stats/*`: subsystem-specific typed diagnostic adapters.

## 3. Shared Helpers and `utils`

Before adding helper code, check the standard library and existing repository
packages. Do not reimplement common retry loops, throttles, closers, backoff,
cloning, sorting, or context helpers when an existing helper already fits.

Use `utils/` only when all of these are true:

- The helper is domain-neutral.
- At least two non-test packages use it, or the second use is part of the same
  PR.
- It has no hidden global state.
- It does not import `errors`, `engine`, `local`, `txn`, `raftstore`,
  `coordinator`, `meta/root`, or `fsmeta`.
- It has focused tests.

Do not move code into `utils/` just to avoid thinking about ownership.
Single-use helpers belong next to the flow that uses them. Domain helpers belong
in the domain package, for example `fsmeta`, `raftstore`, or `meta/root`, not in
`utils/`.

## 4. File Layout

Use responsibility-based file names. Avoid `utils.go`, `helpers.go`,
`common.go`, and `misc.go` unless the package is tiny and the file has a single
clear purpose.

Recommended package layout:

| File | Contents |
| --- | --- |
| `doc.go` | Package responsibility, authority/truth boundary, and major invariants. |
| `types.go` | Core domain types, small enums, and interfaces. |
| `options.go` | Options, defaults, and `Validate` methods. |
| `errors.go` | Package sentinel errors and error helpers. |
| `metrics.go` | Runtime counters and `recordX` methods. |
| `stats.go` | Typed diagnostic snapshots and aggregation. |
| `store.go` | Authoritative in-memory state or the package's primary object. |
| `service.go` | RPC/service boundary and request/response conversion. |
| `client.go` | Remote client wrapper. |
| `recovery.go` | Recovery, replay, bootstrap, and restart behavior. |
| `encode.go` | Durable format encoding/decoding. |
| `validation.go` | Input, invariant, and state validation. |
| `*_test.go` | Tests for the file or behavior under test. |
| `test_helpers_test.go` | Test-only helpers shared inside the package. |

When a file grows because it has multiple responsibilities, split by protocol
stage or data owner. Examples: `grant.go`, `seal.go`, `frontier.go`,
`catalog.go`, `witness.go`, `install.go`, `flush_pipeline.go`.

## 5. File Naming

- File names are lowercase snake_case.
- Files should name the owner or behavior, not the implementation trick.
- Generated files must have a stable suffix such as `.program.go` or `.pb.go`.
- Test files should mirror the behavior under test:
  - `store_test.go` for store-local invariants.
  - `service_test.go` for RPC/service behavior.
  - `recovery_test.go` for recovery and replay behavior.
  - `*_integration_test.go` only when the test crosses package/runtime
    boundaries.
- Benchmarks belong in `*_bench_test.go`.
- Do not create catch-all files named `new.go`, `manager.go`, `impl.go`,
  `handler.go`, or `runtime.go` unless the package truly has one runtime owner.

## 6. Type, Interface, and Field Naming

- Exported types must describe domain responsibility: `AuthorityGrant`,
  `SegmentWitnessRecord`, `RootStore`, `RuntimeStats`.
- Avoid vague names such as `Manager`, `Handler`, `Processor`, `Data`,
  `Info`, and `Config` when the package already has multiple authorities.
- Interfaces should name the behavior required by the caller:
  `SegmentInstaller`, `AuthoritySealer`, `RootWritePreparer`.
- Keep interfaces small and define them near the consumer, not the producer.
- Use `Options` for construction-time configuration and give it a `Validate`
  method when invalid combinations are possible.
- Use `Stats` for live collectors and `StatsSnapshot` for read-only snapshots.
- Use `Metrics` for counters and histograms owned by a runtime component.
- Use `Record`, `Entry`, `Frame`, or `Snapshot` only when the durable or
  diagnostic boundary is clear.
- Boolean fields should read naturally at call sites: `Durable`, `Sealed`,
  `Ready`, `RequiresPublish`, `AllowOpaqueKeys`.
- Avoid negative boolean names such as `DisableX` in internal structs. Prefer a
  positive mode enum when there are more than two states.

## 7. Function Naming and Placement

Function names should make the state transition explicit.

- Use `Load`, `Open`, `Start`, `Close` for lifecycle.
- Use `Acquire`, `Issue`, `Seal`, `Retire`, `Install`, `Recover`, `Replay`,
  `Fence`, `Publish`, and `Observe` for distributed state transitions.
- Use `ValidateX` for pure validation and `EnsureX` only when the function may
  mutate state to satisfy the condition.
- Use `recordX` for metrics updates.
- Use `cloneX` for deep copies and `copyX` only for byte/slice copying.
- Use `encodeX` / `decodeX` for internal formats and `EncodeX` / `DecodeX`
  only when the format is a public package contract.

Place functions in a file in this order:

1. Exported types and constructors.
2. Public methods for the main type.
3. The main state-transition functions in call order.
4. Private helpers used only by the transition immediately above them.
5. Small pure helpers, sorting helpers, clone helpers, and digest helpers.

Do not add one-line forwarding functions:

```go
func (s *Store) Foo(ctx context.Context, req Request) error {
    return s.backend.Foo(ctx, req)
}
```

Forwarding is allowed only for:

- RPC or CLI boundary adapters.
- Interface adaptation between packages.
- Generated code.
- Test helpers.
- A temporary migration shim with a removal issue and deadline.

## 8. Errors

The root `errors` package owns only stable cross-package error kinds, retry
classification, and RPC mapping.

- Package-specific sentinel errors belong in that package's `errors.go`.
- Cross-package errors must be classifiable through `errors.KindOf`.
- Callers must branch on `errors.Is`, `errors.As`, or `errors.KindOf`; never
  match message strings.
- RPC handlers should convert errors at the service boundary. Core packages
  should not return gRPC status errors directly.
- Do not define `ErrXxx` or `errXxx` in random implementation files.
- Lower storage packages must not import the global error taxonomy unless the
  architecture guard explicitly allows it.

## 9. Metrics and Stats

Metrics and stats are owned code, not incidental counters.

- Runtime counters live in `metrics.go`.
- Typed snapshots and aggregators live in `stats.go` or a dedicated `*/stats`
  package.
- Business logic should call `recordX` helpers instead of mutating atomics
  directly.
- Internal diagnostics should prefer typed snapshots over `map[string]any`.
- `map[string]any` is allowed only at external diagnostics/export boundaries.
- Global expvar/prometheus registration must be centralized. Packages must not
  register ad hoc global metric names.
- Metrics names are stable API. Renames require docs and test updates.
- A subsystem-level stats collector may aggregate child metrics, but child
  packages still own their counters.

## 10. Generated Code

- Handwritten specs are the source of truth.
- Generated files must not be edited manually.
- Generated files must include a stable header that names the generator.
- `go generate` must be deterministic and must leave `git diff` clean.
- Do not keep two semantic sources of truth. A migration may temporarily keep
  old and generated paths, but the PR must state the removal condition.

For fsmeta semantic program generation, the contract is:

```text
specs/operations.go -> internal/opgen -> *.program.go -> materialized runtime descriptor
```

Runtime code must consume generated descriptors rather than reinterpreting
operation semantics by hand.

## 11. Tests

Use package-local tests for invariants and external-package tests for public
API behavior.

| Test Type | Placement |
| --- | --- |
| Private invariant tests | Same package in `*_test.go`. |
| Public API tests | External package, e.g. `package local_test`. |
| Cross-module behavior | `integration/` package or explicit `*_integration_test.go`. |
| Model/contract tests | `contract/` or model-specific package. |
| Benchmarks | `*_bench_test.go`. |
| Test helpers | `test_helpers_test.go`. |

Bug-fix tests should name the failure mode:

```go
func TestRootLeaderHandoffCatchesUpBeforeGrantWrite(t *testing.T)
func TestPerasRecoveryRejectsSegmentOutsideAuthority(t *testing.T)
func TestWitnessGCDoesNotDropUnsealedAckedSegment(t *testing.T)
```

Distributed changes must test at least one non-happy-path boundary: stale
leader, stale epoch, retry, context cancellation, crash window, recovery replay,
duplicate request, or GC frontier.

## 12. Distributed Safety

Every distributed write path must make these boundaries explicit:

- Authority owner.
- Freshness/fence check.
- Visibility boundary.
- Durability boundary.
- Recovery source.
- GC or seal condition.
- Slow/fallback path.

Rules:

- Root writes must prepare against the latest committed root state before
  issuing new authority facts.
- Raftstore writes must pass region/epoch/fence checks unless they are a
  validated internal install command.
- fsmeta fast paths must go through holder authority.
- Peras install and recovery paths must validate the segment authority and
  payload digest before publishing read-state.
- Witness records, seals, and catalog entries must be self-describing enough
  for recovery.
- Background repair is not a substitute for safety.

## 13. Compatibility and Breaking Changes

NoKV prefers simple breaking changes over compatibility debt.

- Do not add deprecated aliases by default.
- Do not keep old and new runtime paths unless the PR names the removal point.
- Do not add config aliases unless an already released CLI/config requires it.
- When changing a persisted format, RPC, or CLI flag, update docs and tests in
  the same PR.
- Compatibility exceptions must include a removal issue, owner, and deadline.

## 14. Local Validation

Before opening a PR, run the smallest meaningful loop first, then the full
repository gates:

```bash
make fmt
make lint
make test
```

Dependency-boundary checks are part of `make lint` (the `importboundary`
analyzer in the `nokvcontract` plugin). Run the full lint pipeline before any
PR that moves an import:

```bash
make lint
```

For generated code:

```bash
go generate ./fsmeta/exec/compile
git diff --exit-code -- fsmeta/exec/compile
```

For concurrency-sensitive or distributed-recovery changes, run the relevant
smoke or failpoint suite and include the exact commands in the PR.
