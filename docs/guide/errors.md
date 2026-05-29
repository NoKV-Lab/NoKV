<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Error Handling Guide

This document defines how NoKV should own, define, and propagate errors.

---

## 1. Ownership Rules

1. Domain errors stay in domain packages.
2. Cross-cutting runtime errors may live in `utils` only when shared by multiple subsystems.
3. Command-local/business-flow errors should be unexported (`errXxx`) and stay in command/service packages.

Examples:

- `kv`: entry codec/read-decode errors.
- `vfs`: filesystem contract errors.
- `coordinator/catalog`: control-plane validation/conflict errors.

---

## 2. Propagation Rules

1. Wrap with `%w` when crossing package boundaries.
2. Match via `errors.Is`, not string compare.
3. Keep stable sentinel values for retryable / control-flow decisions.
4. Add context in upper layers; do not lose original cause.
5. Cross-process, cross-runtime, and public API boundaries should classify errors
   with `errors.Kind`; message text is diagnostic only.

---

## 3. Naming Rules

1. Exported sentinels use `ErrXxx`.
2. Error text should be lowercase and package-scoped when useful (for example `coordinator/catalog: ...`, `coordinator/idalloc: ...`, `vfs: ...`).
3. Avoid duplicate sentinels with identical semantics in different packages.

---

## 4. Current Error Map

### Shared runtime sentinels

- `utils/errors.go`: common cross-package sentinels such as invalid request,
  key/value validation errors, throttling, and lifecycle guards.
- `errors/errors.go`: stable cross-boundary `Kind`, typed transaction key
  errors, gRPC status classification, and retry classification.
- `local/errkind/classify.go`: embedded-engine sentinel to `errors.Kind`
  mapping for DB, RPC, and future `fsmeta/runtime/local` boundaries.

### Domain-specific sentinels

- `txn/storage/errors.go`: checksum and partial-entry decode sentinels
- `storage/vfs/vfs.go`: `ErrRenameNoReplaceUnsupported`
- `raftstore/peer/errors.go`: peer lifecycle/state errors
- `txn/percolator/errors.go`: transaction protocol key-error builders and
  Percolator protocol sentinels
- `errors/errors.go`: transaction key-error carrier used by transaction retry
  logic
- `raftstore/client/errors.go`: route errors, retry-budget exhaustion, and
  protocol-contract violations
- `raftstore/store/errors.go`: raftstore lifecycle sentinels plus typed
  transaction-maintenance region routing and protocol errors
- `raftstore/mvcc/errors.go`: replicated MVCC maintenance and lock-resolution
  sentinels
- `pb/errorpb.proto`: region/store routing protobuf errors (`RegionError`,
  `StoreNotMatch`, `RegionNotFound`, `KeyNotInRegion`, ...)
- `storage/wal/errors.go`: WAL encode/decode and segment errors
- `coordinator/catalog/errors.go`: Coordinator metadata and range validation errors
- `experimental/peras/exec/errors.go`: Peras admission, segment, replay, and witness
  sentinels used by holder/runtime control flow
- `experimental/peras/runtime/errors.go` and `experimental/peras/runtime/authority.go`:
  authority acquisition, active-authority view, and runtime lifecycle sentinels
- `experimental/peras/adapters/raftstore/*.go`: Peras segment install and
  witness authority protocol sentinels

---

## 5. Propagation in Hot Paths

1. Embedded write path (`DB.Set*` -> commit pipeline -> Pebble raw store):
   - validation returns direct sentinel (`ErrEmptyKey`, `ErrNilValue`, `ErrInvalidRequest`);
   - storage boundary errors are wrapped with context and preserved via `%w`.
2. Distributed command path (`kv.Service` -> `Store.*Command` -> `kv.Apply`):
   - region/leader/store/range failures are mapped to `errorpb` messages in protobuf responses;
   - execution failures return Go errors to RPC layer and are translated to gRPC status.
3. Recovery/replay path:
   - Pebble owns raw KV recovery; raft/control WAL consumers surface partial or
     corrupt records through domain sentinels and fail startup when the owner
     cannot prove safe replay.

---

## 6. Local Storage Boundary Map

The raw storage packages (`storage/kv`, `storage/pebble`, `storage/memory`) and
transaction-storage primitives must not import the root `errors` package. The
`local/errkind` mapper is the explicit DB boundary where local sentinels become
the stable cross-boundary error taxonomy. The root error package owns gRPC and
distributed transaction adaptation, so importing it from storage would invert
the architecture.

Use `errkind.Classify(err)` from `local/errkind` at DB facade, RPC, or
local fsmeta runtime boundaries. Current mapping:

| Local error family | Boundary kind |
| --- | --- |
| `utils.ErrKeyNotFound` | `KindNotFound` |
| `utils.ErrEmptyKey`, `utils.ErrNilValue`, `utils.ErrInvalidRequest` | `KindInvalidArgument` |
| invalid storage options / WAL manager wiring | `KindInvalidArgument` |
| unsupported required VFS capability | `KindInvalidArgument` |
| `utils.ErrTxnTooBig` | `KindResourceExhausted` |
| blocked writes, hot-key throttle, WAL backpressure, retained WAL segment | `KindRetryable` |
| `utils.ErrDBClosed`, closed storage runtime | `KindAborted` |
| KV checksum/partial-entry and WAL partial/empty-record errors | `KindCorruption` |
| nil storage/runtime wiring | `KindProtocolViolation` |

Pure package-local control-flow sentinels, such as `utils.ErrStop`, stay local
and map to `KindUnknown` if accidentally observed outside their package.
