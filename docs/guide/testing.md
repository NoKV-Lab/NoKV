<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Testing Strategy

NoKV tests follow the current three-layer architecture: fsmeta semantics,
distributed execution, and raw storage backends.

## Fast Local Matrix

```bash
go test -count=1 ./storage/kv ./storage/memory ./storage/pebble ./txn/storage
go test -count=1 ./local/... ./txn/...
go test -count=1 ./raftstore/kv ./raftstore/mvcc ./raftstore/store ./raftstore/server ./raftstore/admin
go test -count=1 ./fsmeta/backend ./fsmeta/exec ./fsmeta/runtime/local ./fsmeta/runtime/raftstore ./fsmeta/contract ./fsmeta/integration
go test -count=1 ./cmd/nokv ./cmd/nokv-fsmeta
```

## Ownership Checks

| Layer | Primary tests |
| --- | --- |
| Raw storage | `storage/kv`, `storage/pebble`, `storage/memory` |
| MVCC storage encoding | `txn/storage`, `txn/mvcc`, `txn/percolator` |
| Local runtime | `local/...`, `fsmeta/runtime/local` |
| Distributed runtime | `raftstore/kv`, `raftstore/store`, `raftstore/server`, `raftstore/admin`, `fsmeta/runtime/raftstore` |
| fsmeta semantics | `fsmeta/exec`, `fsmeta/contract`, `fsmeta/integration` |
| CLI wiring | `cmd/nokv`, `cmd/nokv-fsmeta` |
| Package boundaries | `make lint` |

## Migration Status

Operator-facing migration tests for `nokv migrate`, `raftstore/migrate`, and
SST import/export were removed with the migration feature. Raftstore still has
internal snapshot tests for peer bootstrap and raft snapshot apply.

## Full Gates

Use these before merging broad architecture changes:

```bash
git diff --check
make lint
make test
```

Use Docker chaos and long soak scripts for release hardening rather than every
PR edit loop.
