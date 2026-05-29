<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Legacy Compaction Design

The old self-managed LSM compaction planner is no longer a mainline package.
Physical compaction belongs to the concrete raw backend: Pebble below
`storage/pebble`, and Holt below the future `storage/holt` adapter. NoKV keeps
transaction, raftstore, and fsmeta semantics above the raw ordered-KV boundary.
