<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Legacy Flush Design

The old NoKV-owned LSM flush pipeline has been removed from the mainline.
Pebble now owns physical memtable flush and compaction below the `storage/kv`
raw ordered-KV contract.

NoKV still owns higher-level commit, MVCC, raft, and fsmeta semantics above
that boundary.
