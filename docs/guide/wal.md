<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# WAL Boundaries

`storage/wal` contains typed WAL segment utilities used by control-log and raft
runtime internals. It is not the physical data-plane WAL for raw storage
backends. Pebble owns its own WAL below `storage/pebble`; Holt should own its
own durability machinery below `storage/holt`.

NoKV keeps WAL consumers explicit:

- raft logs use `raftstore/raftlog`
- local control-log shards use `storage/wal`
- raw key/value persistence uses Pebble through `storage/pebble` today and the
  same `storage/kv` contract for Holt once wired

The old self-managed LSM WAL/flush/manifest coupling has been removed from the
mainline storage architecture.
