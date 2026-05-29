<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# NoKV Architecture Overview

NoKV is organized as a three-layer metadata system:

1. `fsmeta` owns inode, dentry, workspace namespace, watch, snapshot, session,
   quota, and artifact-style metadata semantics.
2. The distributed execution layer (`meta/root`, `coordinator`, `raftstore`,
   `txn`) owns rooted authority, routing, TSO, replicated execution, MVCC
   transactions, and recovery facts.
3. `storage/*` owns raw ordered key/value persistence. Pebble is the default
   implementation in this repo. Holt is the owned backend target that should
   plug in at the same `storage/kv` boundary.

Pebble and Holt replace only the physical ordered-KV substrate. NoKV keeps its
own MVCC internal-key encoding (`txn/storage`), transaction protocols
(`txn/mvcc`, `txn/percolator`), raft execution, and fsmeta inode/dentry model.

## Package Layout

```text
fsmeta/
  model/          # inode/dentry/session/quota/snapshot domain model
  layout/         # ordered namespace key layout and value codecs
  backend/        # minimal MVCC metadata backend contract
  exec/           # semantic execution and compiler
  runtime/local/  # embedded fsmeta runtime
  runtime/raftstore/

txn/
  storage/        # MVCC internal keys, column families, entries, timestamps
  mvcc/           # MVCC read/write planning
  percolator/     # 2PC transaction protocol
  latch/

raftstore/
  kv/             # StoreKV apply bridge to MVCC/percolator/prepared install
  store/          # peer lifecycle and routing inside one store process
  peer/           # raft peer runtime
  snapshot/       # internal MVCC-entry snapshot payloads for peer bootstrap

storage/
  kv/             # raw ordered KV contract
  pebble/         # default Pebble backend
  memory/         # tests
  wal/file/vfs/   # low-level runtime support
```

`storage/holt` is the expected placement for the Holt adapter once it is wired
into this repository. It should implement `storage/kv.Store`; it should not
import fsmeta, raftstore, coordinator, root, protobuf, or MVCC packages.

`engine/*`, operator-facing `raftstore/migrate`, and SST import/export are not
mainline packages. This version does not provide online migration from old
self-managed LSM workdirs into Pebble or Holt workdirs.

## Write Path

```mermaid
flowchart LR
    Client["Client / fsmeta API"] --> Exec["fsmeta/exec"]
    Exec --> Backend["fsmeta/backend.Store"]
    Backend --> Local["runtime/local"] --> LocalDB["local.DB"]
    Backend --> Raft["runtime/raftstore"] --> StoreKV["raftstore/kv"]
    StoreKV --> Txn["txn/mvcc + txn/percolator"]
    LocalDB --> MVCC["txn/storage internal keys"]
    Txn --> MVCC
    MVCC --> Raw["storage/kv raw ordered bytes"]
    Raw --> Pebble["storage/pebble"]
    Raw -. "same contract" .-> Holt["storage/holt target"]
```

The important boundary is between `fsmeta/backend` and `storage/kv`:

- `fsmeta/backend` is an MVCC metadata contract with timestamps, predicates,
  mutations, scans, and atomic mutation semantics.
- `storage/kv` is raw ordered bytes: get, put, delete, range delete, iterator,
  batch, snapshot, sync, close, and small stats.

Keeping both contracts separate lets NoKV swap the physical engine without
changing fsmeta semantics or distributed transaction behavior.

## Storage Backend Contract

The raw backend must provide:

- ordered point reads and range iteration;
- atomic batch apply;
- range delete;
- point-in-time snapshot reads;
- explicit `Sync`, `Close`, and small backend-neutral stats.

The backend must not own:

- MVCC timestamp ordering or column-family semantics;
- fsmeta inode/dentry layout;
- raftstore region routing;
- root/coordinator authority facts;
- migration, SST ingest/export, or product-level backup semantics.

That rule is what lets Pebble and Holt be interchangeable under NoKV's metadata
execution model.

## Local Runtime

`local.DB` is the embedded database facade. It opens a raw storage backend and
preserves NoKV's internal-key layout:

- column families are encoded into the raw key;
- MVCC timestamps keep the existing inverted ordering;
- `local.DB` exposes internal-entry iterators for MVCC, raftlog, and raftstore
  snapshot code;
- control/raft WAL utilities remain under `storage/wal`; they are not the
  physical Pebble or Holt WAL.

The local fsmeta runtime (`fsmeta/runtime/local`) uses the same
`fsmeta/backend` contract as the raftstore runtime. It is the default path for
demos, local agent-workspace deployments, and storage-backend experimentation.

## Distributed Runtime

The distributed path keeps these responsibilities separate:

- `meta/root` is rooted truth for topology, authority, grants, seals, and
  lifecycle facts.
- `coordinator` is a rebuildable serving plane for routing, TSO, and store
  discovery.
- `raftstore` hosts replicated region execution and applies StoreKV commands.
- `txn/*` owns MVCC and transaction semantics.
- `fsmeta/runtime/raftstore` adapts fsmeta execution to StoreKV/MVCC. Raftstore
  itself must not understand inode/dentry semantics.

`raftstore/snapshot` is an internal MVCC-entry snapshot format for raft peer
bootstrap and raft snapshot apply. It is not a generic migration feature and it
does not expose concrete storage-engine table files.

## Experimental Systems

`experimental/*` is the boundary for research mechanisms. Peras, Thermos, and
future experiments can attach to neutral fsmeta or raftstore extension points,
but stable fsmeta, txn, raftstore, and storage packages should not import
experimental packages directly unless the code contract explicitly allows the
adapter.
