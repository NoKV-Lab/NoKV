<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Legacy Flush Design

The old NoKV-owned LSM flush pipeline has been removed from the mainline.
Physical memtable flush and compaction belong to the concrete storage backend:
Pebble today, Holt once its adapter is wired, or any future implementation of
`storage/kv.Store`.

NoKV still owns higher-level commit, MVCC, raft, and fsmeta semantics above
that boundary.
