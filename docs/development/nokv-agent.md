<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# `nokv-agent` Contributor Handbook

`nokv-agent` is the agent runtime and index crate. It owns the agent-local
event model, idempotent event ingestion, secondary indexes, coverage state,
typed query execution, and the agent-native fs-shaped tool surface. The durable
backend is Holt through `HoltAgentStore`.

This crate does not expose NoKV DFS namespace tools. It must not depend on
`nokv-meta`, `nokv-object`, `nokv-client`, `nokv-protocol`, `nokv-control`, or
`nokv-fuse`.

## Package Boundary

`nokv-agent` owns:

- `AgentStore` and agent-local batch/scan primitives.
- `HoltAgentStore`, the only in-tree backend for agent index state.
- `AgentEventIndex`, `EventRecord`, `EventQuery`, `IndexCoverage`, and
  idempotent event ingestion.
- `AgentFs`, agent-native file/body nodes, catalog registrations, and
  `ls`/`stat`/`catalog`/`read`/`aggregate`/`find`/`grep` tools over those
  records.
- The `nokv-agent` CLI, including the stdio MCP entry point for the
  agent-native tool surface.

`nokv-agent` must not own:

- NoKV DFS inode/dentry semantics.
- NoKV object body descriptors, object-reference GC, or S3/object providers.
- FUSE/kernel cache behavior.
- Metadata RPC, shard routing, owner leases, or client connection pools.
- Compatibility shims that make NoKV DFS namespace metadata look like
  `nokv-agent` records.

The dependency check is:

```bash
cargo tree -p nokv-agent -e normal
```

That tree must not mention any NoKV DFS crate.

## Current Surface

The public store boundary is intentionally narrow:

```rust
pub trait AgentStore {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, AgentIndexError>;
    fn scan(
        &self,
        prefix: &[u8],
        cursor: Option<&[u8]>,
        limit: usize,
        direction: ScanDirection,
    ) -> Result<AgentScanPage, AgentIndexError>;
    fn apply(&self, batch: AgentBatch) -> Result<AgentCommit, AgentIndexError>;
}
```

`AgentEventIndex<S>` builds event ingestion and query semantics on top of that
trait. `AgentFs<S>` builds the agent-native fs-shaped model and tool surface on
the same boundary. The current target is replacing derived event indexes such
as Lingtai's SQLite `logs/log.sqlite` sidecar while preserving `events.jsonl`
as source of truth and fallback.

The stdio MCP server also lives in this crate. `run_mcp_stream` accepts an
`AgentFs<S>` and serves the same seven tool definitions through JSON-RPC. The
`nokv-agent mcp` command opens a local Holt-backed agent store and runs that
server. Product crates must not add NoKV DFS namespace adapters here.

Durable key layout is private to `nokv-agent`:

```text
event/{agent}/{event_id}
source/{agent}/{source_file}/{source_offset}
type_id/{agent}/{event_type}/{event_id}
type_ts/{agent}/{event_type}/{ts_desc}/{event_id_desc}
coverage/{agent}/{source_file}
node/{agent}/{path}
body/{agent}/{path}
child/{agent}/{parent}/{name}
catalog/{agent}/{root}
row/{agent}/{root}/{path}
```

Do not expose those keys as public API. The public API is `EventQuery`,
`EventPage`, `EventRecord`, `IndexCoverage`, `AgentFs`, and the typed
agent-native model/tool structs.

## Adding Agent Index Capabilities

For a new agent-native capability:

1. Add or extend public domain types in `event.rs`, `query.rs`, or a new
   responsibility-named file.
2. Add store-neutral logic on top of `AgentStore`.
3. Add Holt-backed tests in the same change.
4. Expose model-facing tools only after the typed API has deterministic
   idempotency, pagination, and failure semantics.

For Lingtai-style event indexing, preserve these invariants:

- `events.jsonl` remains source of truth unless a separate migration explicitly
  changes that contract.
- Ingestion is idempotent by `(source_file, source_offset)`.
- Query cursors are opaque and must not expose durable key layout.
- Coverage state must be cheap to read and sufficient for callers to decide
  whether to trust the index or fall back.
- Holt batch commits must keep primary event rows, secondary indexes, source
  dedupe rows, and coverage rows in one atomic mutation batch.

## File Map

| Concern | Location |
| --- | --- |
| Crate exports | `crates/nokv-agent/src/lib.rs` |
| Store-neutral API | `crates/nokv-agent/src/store.rs` |
| Holt backend | `crates/nokv-agent/src/holt.rs` |
| Event and coverage types | `crates/nokv-agent/src/event.rs` |
| Query request/result types | `crates/nokv-agent/src/query.rs` |
| Event index implementation | `crates/nokv-agent/src/index.rs` |
| Agent-native fs-shaped model | `crates/nokv-agent/src/fs.rs`, `crates/nokv-agent/src/model.rs` |
| Agent tool surface | `crates/nokv-agent/src/tool.rs` |
| MCP stdio server | `crates/nokv-agent/src/mcp.rs` |
| CLI binary | `crates/nokv-agent/src/bin/nokv-agent.rs` |
| Private durable key layout | `crates/nokv-agent/src/key.rs` |

See also the [code contract](code_contract.md) and the
[PR review checklist](pr_review_checklist.md).
