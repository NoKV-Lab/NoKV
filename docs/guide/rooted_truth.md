<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Rooted Truth — `meta/root`

The `meta/root/` tree implements NoKV's **rooted truth kernel**: a typed, append-only event log whose committed tail is the single source of truth for cluster-level metadata (root-issued authority grants, allocator fences, region lifecycle, pending peer/range changes).

> If the distributed system has a "brain", it does **not** live in the coordinator. It lives here. The coordinator is a service+view on top of this log.

---

## 1. Why a separate truth layer

In a typical multi-raft system, the metadata used by the control plane (routes, TSO, authority grants, scheduling decisions) is either:

1. Stored inside one of the raft groups (mixed with user data)
2. Owned by a single coordinator node (coordinator becomes the bottleneck)
3. Split across ad-hoc persistence files

NoKV makes it **explicit**: there is a small, typed metadata log with its own durability and replication shape, and everything control-plane-related goes through it. This matches the "virtual consensus" pattern (Delos-lite): the log is the truth; services above are views that can be rebuilt.

The benefits are concrete:

- **Coordinator is stateless at restart** — the only persistent thing about a coordinator is its configured holder ID; everything else is rebuilt from `meta/root` on boot
- **The log can be swapped** between local (single-node) and replicated (embedded-raft) backends without changing coordinator code
- **Authority transfer is auditable** — every grant issue, retirement, and inheritance is a committed log record with a cursor

---

## 2. Package layout

```
meta/
├── root/
│   ├── protocol/       # Pure protocol types (Cursor, Duty, Grant, Evidence, ...)
│   ├── event/          # Typed events (KindStoreJoined, KindGrantIssued, ...)
│   ├── state/          # Compact applied state (State, Snapshot, ApplyEventToSnapshot)
│   ├── materialize/    # Helpers that build Snapshot from raw events
│   ├── storage/        # Virtual log file layout + checkpoint format
│   │   └── file/       # Actual on-disk file operations
│   ├── replicated/     # Embedded raft-backed log (quorum durability)
│   ├── server/         # gRPC service for rooted access
│   └── client/         # gRPC client for rooted access
└── wire/               # proto <-> Go conversions (Event, Snapshot, Cursor)
```

---

## 3. What lives in rooted state

[`meta/root/state/state.go`](https://github.com/feichai0017/NoKV/blob/main/meta/root/state/state.go) defines `State`, the applied snapshot. Everything the control plane cares about is here:

```go
type State struct {
    ClusterEpoch       uint64         // bumped on topology event
    MembershipEpoch    uint64         // bumped on store join/leave
    LastCommitted      Cursor         // highest committed (term, index)
    IDFence            uint64         // globally fenced ID allocator floor
    TSOFence           uint64         // globally fenced TSO allocator floor
    ActiveGrants       []AuthorityGrant // mutually exclusive by {DutyID, Scope}
    RetiredGrants      []GrantRetirement
    GrantInheritances  []GrantInheritance
    RetiredEraFloors   []AuthorityRetiredEraFloor
    ActivePerasGrants   []PerasAuthorityGrant
    PerasAuthorityEpoch uint64
    PerasAuthoritySeals []PerasAuthoritySeal
}
```

`AuthorityGrant` is the Eunomia grant for coordinator duties such as `alloc_id`,
`tso`, and `region_lookup`. `PerasAuthorityGrant` is a separate fsmeta Peras
authority object; its root seals record segment digest and raftstore install
cursor, not segment payload bytes.

`Snapshot` wraps `State` together with descriptors and pending peer/range changes:

```go
type Snapshot struct {
    State               State
    Descriptors         map[uint64]topology.Descriptor
    PendingPeerChanges  map[uint64]PendingPeerChange
    PendingRangeChanges map[uint64]PendingRangeChange
}
```

Every event kind has a deterministic effect on `Snapshot`. See [`meta/root/state/snapshot_apply.go`](https://github.com/feichai0017/NoKV/blob/main/meta/root/state/snapshot_apply.go) and [`meta/root/state/state.go:ApplyEventToState`](https://github.com/feichai0017/NoKV/blob/main/meta/root/state/state.go).

---

## 4. The Append protocol

The core interface any backend must satisfy:

```go
type Backend interface {
    Snapshot() (rootstate.Snapshot, error)
    Append(ctx context.Context, events ...rootevent.Event) (rootstate.CommitInfo, error)
    FenceAllocator(ctx context.Context, kind AllocatorKind, min uint64) (uint64, error)
}
```

`Append` does five things atomically:

1. Validate events against the current `Snapshot` (reject duplicate region IDs, invalid transitions, stale epochs)
2. Assign each event a committed cursor `(Term, Index)`
3. Persist the batch to the backing log
4. Persist an updated compact `Checkpoint`
5. Advance in-memory `State` + `Descriptors` + pending maps

After `Append` returns successfully, callers can observe the new state via `Snapshot()`. The `CommitInfo.Cursor` they get back is the globally ordered cursor for the **last** event in the batch.

`FenceAllocator` is separate because it's an **authoritative minimum** — backends may promote the fence further (e.g., to account for outstanding windows) but must never return a value below `min`.

---

## 5. Single backend: replicated

NoKV ships one meta-root backend: the 3-peer raft-replicated cluster.
Historical single-process "local" backend has been removed.

[`meta/root/replicated/store.go`](https://github.com/feichai0017/NoKV/blob/main/meta/root/replicated/store.go) — embedded raft-backed root log, quorum-durable commits.

- exactly 3 replicas, one leader
- `Append` proposes a raft log entry; returns after it's committed to quorum
- Non-leader nodes reject `Append` with `codes.FailedPrecondition`
- Leader changes trigger root write-access / `LeaderID()` state updates that
  coordinator consumes. In remote-root deployments, the coordinator sees
  `CanSubmitRootWrites()` rather than raw local Raft leadership because the
  root client routes writes to the current meta-root leader.
- On-disk state per peer: `root.events.wal`, `root.checkpoint.binpb`, `root.raft.bin`
  (raft hard state + snapshot + retained entries)

---

## 6. Coordinator commands — how grants flow in

In addition to "raw" events, backends expose command APIs for control-plane-specific operations:

```go
ApplyGrant(ctx, cmd GrantCommand)
    (EunomiaState, GrantCertificate, error)
ApplyPerasAuthority(ctx, cmd PerasAuthorityCommand)
    (State, PerasAuthorityGrant, error)
```

These are **validated, typed writes** that internally:

1. Validate the command against current state: issue, graceful exact seal,
   expiry-bound retirement, or inheritance
2. Emit the appropriate `KindGrantIssued` / `KindGrantSealed` /
   `KindGrantRetired` / `KindGrantInherited` event
3. Append through the normal log path
4. Return the new `EunomiaState = { ActiveGrants, RetiredGrants,
   GrantInheritances, RetiredEraFloor }` and, for issue, a root-signed
   deterministic `GrantCertificate`

Crash-before-seal is handled conservatively: root never fabricates an exact
served frontier. After expiry, the successor retires the predecessor using the
predecessor grant's root-known upper bound and then inherits that bound.

`ApplyPerasAuthority` is the analogous typed command path for fsmeta Peras
authority acquire/retire/seal. It updates `ActivePerasGrants`,
`PerasAuthorityEpoch`, and `PerasAuthoritySeals`, but it is not part of the
Eunomia `DutySpec` set.

---

## 7. Tail subscription — how coordinator consumes

Coordinators don't poll `Snapshot()` — they subscribe:

```go
sub := rootstorage.NewTailSubscription(afterToken, waitFn)
advance, err := sub.Next(ctx, fallback)
if advance.Action == rootstorage.TailCatchUpAction_Reload {
    // backend advanced far; reload snapshot
} else if advance.Action == rootstorage.TailCatchUpAction_Bootstrap {
    // backend advanced past our retention window; install from compact state
}
sub.Acknowledge(advance)
```

[`meta/root/storage/virtual_log.go`](https://github.com/feichai0017/NoKV/blob/main/meta/root/storage/virtual_log.go) defines:

- `TailToken` — opaque position in the log
- `TailAdvance` — either new events, a reload signal, or a bootstrap install
- `TailSubscription` — stateful iterator that survives across reloads

This is what lets `coordinator/` run as a thin service without duplicating rooted storage.

---

## 8. Recovery model

On coordinator boot:

1. Open the replicated backend (each peer via `rootreplicated.Open`, or connect through `coordinator/rootview` for the client side)
2. Call `Snapshot()` — backend replays/bootstraps internally
3. Build a `TailSubscription` from the snapshot's `LastCommitted`
4. Start the grant campaign loop, which will eventually `ApplyGrant(Issue)` when it's leader

If the backend file is corrupted, the coordinator fails fast — it does **not** try to reconstruct rooted state from raftstore local metadata. The two are deliberately partitioned.

---

## 9. Remote access

`meta/root/server` and `meta/root/client` provide gRPC rooted access. This exists so a raftstore store can read rooted state without being colocated with the replicated backend:

- `meta/root/server` serves `Snapshot`, `Append`, `WaitForTail`, `ObserveTail`, etc.
- `meta/root/client` implements the same `rootBackend` interface by calling over gRPC
- Leader redirect is automatic: if the target returns `NotLeader`, client re-dials the returned leader

This is what keeps `coordinator/` deployable separately from the rooted log, if you ever want to.

---

## 10. Source map

| File | Responsibility |
|---|---|
| [`meta/root/protocol/types.go`](https://github.com/feichai0017/NoKV/blob/main/meta/root/protocol/types.go) | Pure protocol types (no persistence logic) |
| [`meta/root/event/types.go`](https://github.com/feichai0017/NoKV/blob/main/meta/root/event/types.go) | Typed event constructors |
| [`meta/root/state/state.go`](https://github.com/feichai0017/NoKV/blob/main/meta/root/state/state.go) | `State`, `Snapshot`, `ApplyEventToSnapshot` |
| [`meta/root/state/eunomia.go`](https://github.com/feichai0017/NoKV/blob/main/meta/root/state/eunomia.go) | Grant lifecycle projection |
| [`meta/root/state/transition.go`](https://github.com/feichai0017/NoKV/blob/main/meta/root/state/transition.go) | Cross-event transition rules |
| [`meta/root/storage/virtual_log.go`](https://github.com/feichai0017/NoKV/blob/main/meta/root/storage/virtual_log.go) | Tail subscription + checkpoint primitives |
| [`meta/root/replicated/store.go`](https://github.com/feichai0017/NoKV/blob/main/meta/root/replicated/store.go) | The only backend: 3-peer raft-replicated meta-root |
| [`meta/root/server/service.go`](https://github.com/feichai0017/NoKV/blob/main/meta/root/server/service.go), [`meta/root/client/client.go`](https://github.com/feichai0017/NoKV/blob/main/meta/root/client/client.go) | gRPC service + client for meta-root |
| [`meta/wire/root.go`](https://github.com/feichai0017/NoKV/blob/main/meta/wire/root.go) | proto ↔ Go conversions |

Related docs:

- [Coordinator](coordinator.md) — how the control plane consumes rooted state,
  including the Eunomia production-hardening backlog
- [Control and Execution Plane Protocols](control_and_execution_protocols.md) — the full contract between `meta/root`, `coordinator/`, and `raftstore/`
- [Migration Status](migration.md) — current storage-backend migration policy
