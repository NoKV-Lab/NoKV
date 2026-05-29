<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Migration Status

The operator-facing standalone-to-cluster migration workflow has been removed
from the mainline architecture.

Current policy:

- new local workdirs use the selected storage backend format
- Pebble is the default backend in this repo; Holt is the owned backend target
- NoKV keeps fsmeta inode/dentry semantics and MVCC transaction semantics above
  the storage backend
- raftstore retains internal raft snapshots for peer bootstrap and recovery
- `nokv migrate`, `nokv manifest`, `raftstore/migrate`, and SST import/export
  are not product surfaces in this version

There is no online migration path from old self-managed LSM workdirs to Pebble
or Holt workdirs in this release. If migration is reintroduced later, it should
live as an explicit operations package with its own recovery contract, not as
part of `fsmeta/backend` or `storage/kv`.
