<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# `nokv-agent` — Contributor Handbook

`nokv-agent` is the **agent surface** crate. Its current shipped surface is the
LLM-facing NoKV namespace tool layer: tool definitions, the dispatcher that maps
a tool call onto a namespace verb, argument validation, result shaping, and the
transport-neutral `AgentError`. That surface is deliberately **transport-free**
and **read-only**.

The crate is also the home for LingTai-facing agent event indexes. That index is
a derived local view over LingTai `logs/events.jsonl`, not a NoKV DFS namespace
adapter and not a SQL-compatibility layer. Keep this second surface separate
from the seven namespace verbs so the DFS product path and LingTai event-index
path can evolve independently.

This handbook is for contributors touching either agent surface. It covers where
the crate sits in the workspace, the invariants you must preserve, how to add a
namespace tool, and how to keep the LingTai event-index work inside its own
boundary.

## 1. Why this crate exists

The seven agent verbs used to live inside `nokv-client/src/agent.rs`, which
forced every consumer of the tool surface (the benchmark harness, future SDKs,
an MCP server) to depend on the whole client stack — `nokv-protocol`,
`nokv-control`, framed RPC, connection pools — even when running fully
in-process against an embedded engine.

`nokv-agent` converges that surface into one crate whose only dependencies are
the metadata engine, the object store, and the shared types. The result is a
short, honest dependency chain for the embedded (in-process) agent path, and a
single place to evolve the tool contract.

## 2. Where it sits — the dependency chain

There are **two** agent paths with opposite dependency profiles. Keep them
straight.

### Embedded (in-process) — what `nokv-agent` owns

```
caller (bench / SDK / MCP)
  └─ nokv_agent::execute_agent_tool(&namespace, name, args)   ← dispatch + limits + JSON
       └─ impl AgentNamespace for NoKvFs<M, O>                ← lives in nokv-agent
            └─ NoKvFs::{stat_card,list_page,find_paths,
                        aggregate_paths,grep_paths,read_page}  ← inherent methods in nokv-meta
                 ├─ MetadataStore (Holt)                       ← metadata, in nokv-meta
                 └─ ObjectStore  (S3)                          ← bytes for grep/read, in nokv-object
```

The namespace tool surface currently depends on exactly three workspace crates
plus `serde_json`:

| Dependency | Why it is load-bearing |
| --- | --- |
| `nokv-meta` | The six read verbs are `pub` inherent methods on `NoKvFs<M, O>`; the namespace vocabulary types (`Namespace*`) live here. |
| `nokv-object` | `grep`/`read` read real bytes through the `ObjectStore` bound on `NoKvFs<M, O>`. |
| `nokv-types` | Shared domain structs (`FileType`, `PathMetadata`, body descriptors, …). |
| `serde_json` | Tool argument parsing and result JSON. The **only** serde dependency. |

It depends on **none** of `nokv-client`, `nokv-protocol`, `nokv-control`. That
is the whole point — and it is enforced (see [the cycle rule](#4-invariants-do-not-break-these)).

### LingTai event index — what `nokv-agent` owns

LingTai's runtime keeps `logs/events.jsonl` as the authoritative log and uses
`logs/log.sqlite` as a derived index for TUI/session queries. NoKV's integration
target is the derived-index layer:

```
LingTai logs/events.jsonl
  └─ nokv-agent lingtai ingest                         ← streaming, offset-aware
       └─ AgentEventStore                              ← semantic event-index API
            └─ Holt-backed local agent index           ← derived view, rebuildable
                 ├─ coverage by source file
                 ├─ event type / timestamp indexes
                 ├─ tool/action facets
                 └─ tool_call_id / trace correlation
```

The event index may open its own Holt store because it is an independent local
agent-index directory. It must not open or mutate a NoKV metadata directory and
must not become a second owner for NoKV namespace state.

The index is allowed to expose CLI/server entry points such as
`nokv-agent lingtai ingest`, `coverage`, `latest`, `session`, `session-rows`,
`recent`, `molt-windows`, `errors`, `completion-after`, `clear-completion`,
`notification-blocks`, `notification-block-snapshots`, `notification-events`,
`notification-by-id`, `notification-before`, `notification-after`,
`notifications`, `facets`, and `trace`. These entry points return stable JSON
for LingTai's Go adapter and the benchmark harness.

### Remote (RPC) — stays in `nokv-client`

```
caller → execute_agent_tool → impl AgentNamespace for MetadataClient
       → metadata RPC (nokv-protocol) → fleet routing (nokv-control) → framed TCP
       → server process → NoKvFs → Holt
```

The two remote trait impls (`for MetadataClient`, `for NoKvFsClient<O>`) live in
`nokv-client/src/agent.rs`, which now also re-exports the whole surface from
`nokv-agent` so existing `nokv_client::{execute_agent_tool, …}` call sites keep
compiling unchanged.

### Workspace position

```
nokv-types ─┬─ nokv-object ─┐
            ├─ nokv-meta ───┼─ nokv-agent ── nokv-client ── nokv-server / bench / SDK
            └───────────────┘
```

`nokv-agent` sits **above** `nokv-meta`/`nokv-object` and **below**
`nokv-client`. The edge `nokv-client → nokv-agent` is the only one that crosses
between them; there is no reverse edge.

## 3. Public API

```rust
// The verb contract. Implemented for NoKvFs (embedded, in nokv-agent) and for
// MetadataClient / NoKvFsClient (remote, in nokv-client).
pub trait AgentNamespace {
    fn agent_stat_card(&self, path: &str) -> Result<Option<NamespaceCard>, AgentError>;
    fn agent_list_page(&self, path: &str, opts: NamespaceListOptions)  -> Result<NamespaceListPage, AgentError>;
    fn agent_find_paths(&self, req: NamespaceFindRequest)              -> Result<NamespaceFindResult, AgentError>;
    fn agent_aggregate_paths(&self, req: NamespaceAggregateRequest)    -> Result<NamespaceAggregateResult, AgentError>;
    fn agent_grep_paths(&self, req: NamespaceGrepRequest)              -> Result<NamespaceGrepResult, AgentError>;
    fn agent_read_page(&self, path: &str, opts: NamespaceReadOptions)  -> Result<NamespaceReadPage, AgentError>;
}

// The LLM-facing tool layer.
pub struct AgentToolDefinition { pub name: &'static str, pub description: &'static str, pub parameters: serde_json::Value }
pub fn agent_tool_definitions() -> Vec<AgentToolDefinition>;   // 7 tools: ls, stat, catalog, read, find, aggregate, grep
pub fn execute_agent_tool<T: AgentNamespace + ?Sized>(ns: &T, name: &str, args: &serde_json::Value)
    -> Result<serde_json::Value, AgentError>;

// Transport-neutral error. Implements std::error::Error (hand-rolled, no thiserror).
pub enum AgentError { Metadata(nokv_meta::MetadError), NotFound(String), InvalidArgument(String), Other(String) }
```

The seven tools are **read-only**: `ls`, `stat`, `catalog`, `read`, `find`,
`aggregate`, `grep`. They form a progressive-disclosure surface — discover what
exists (`ls`/`stat`/`catalog`), then query and read only what is needed
(`find`/`aggregate`/`read`/`grep`).

The LingTai event-index API is a separate surface. Its initial semantic queries
mirror the current SQLite consumers instead of exposing raw SQL:

- `coverage(agent, source_file)`;
- `stream_session_events(agent, filter, order = id_asc)`;
- `stream_session_rows(agent, order = id_asc)`, a compact LingTai
  `SessionEventRow { ts, type, fields_json }` projection for replacing
  SQLite's `StreamSessionEvents` hot path;
- `latest_events(type, limit)`;
- `recent_times(type, limit)`;
- `molt_session_windows(agent)`;
- `error_events(limit)`;
- `completion_after(type = clear_received, source_offset)`;
- `tui_clear_completion(source_offset)`;
- parsed `notification-blocks` rows for replacing `QueryNotificationBlocks`;
- parsed `notification-block-snapshots` rows for replacing
  `QueryNotificationBlockSnapshots`;
- `notification_events(limit)`;
- `notification_event_by_id(event_id)`;
- `notification_neighbor(event_id, before | after)`;
- `notification_lifecycle(ref_id/event_id/call_id/channel)`;
- `tool_facets(window, group_by = tool/action)`;
- `tool_trace(tool_call_id)`.

## 4. Invariants (do not break these)

1. **Transport-free.** `nokv-agent` must never depend on `nokv-client`,
   `nokv-protocol`, or `nokv-control`. A reverse edge would create a dependency
   cycle and defeat the crate. Assert it: `cargo tree -p nokv-agent -e normal`
   must not mention any of those three.
2. **Read-only verb surface.** The `AgentNamespace` trait exposes only the six
   read verbs. Writes (e.g. `register_namespace_index`) stay as inherent methods
   on `NoKvFs` in `nokv-meta`, off the trait. Keep the model-facing surface
   read-only unless a write contract is explicitly designed (see
   [the roadmap](#7-roadmap-verbs-we-expect-to-add)).
3. **Orphan-rule placement.** `impl AgentNamespace for NoKvFs<M, O>` lives in
   `nokv-agent` (local trait + foreign type → legal). This is what lets the
   embedded path work **without** `nokv-meta` gaining a dependency on
   `nokv-agent`. Do not move it into `nokv-meta`.
4. **Borrow the engine handle; never open it.** The embedded impl operates on a
   borrowed `&NoKvFs<M, O>` that already holds a shared, cloned
   `HoltMetadataStore`. `nokv-agent` must not call `HoltMetadataStore::open_*`
   on a live data directory — Holt takes an exclusive `flock` and rejects a
   second opener even in-process.
5. **`AgentError: std::error::Error`.** Downstream funnels (e.g. the bench's
   `from_nokv(err: impl Error)`) depend on this. Keep the hand-rolled `Debug` +
   `Display` + `Error` impls; do not introduce `thiserror` only here.
6. **Byte-stable output.** Tool result JSON, the limit constants, and error
   `Display` strings are observed by judges, telemetry, and the model. Treat any
   change to them as a behavior change, not a refactor — snapshot-test before
   and after.
7. **JSONL remains authoritative for LingTai.** The event index is a derived
   acceleration layer. LingTai fallback behavior must remain possible when
   coverage is incomplete, a source file is truncated, or the index is absent.
8. **Idempotent event ingest.** The event index keys rows by
   `(agent_id, source_file, source_offset)`. Replaying the same JSONL range must
   not duplicate events or roll coverage backward.
9. **No raw-log benchmark shortcuts.** Product code may expose semantic event
   queries and compact facets, but it must not bake benchmark task answers into
   hidden files, tables, or special cases.

## 5. LingTai event-index design rules

The first implementation should use responsibility-based files instead of
growing `lib.rs`:

| File | Contents |
| --- | --- |
| `namespace.rs` | Existing namespace trait, seven tool definitions, dispatcher, parsers, and result JSON. |
| `event/types.rs` | `AgentId`, `SourceFile`, `EventId`, `EventRecord`, projected fields, coverage, and query results. |
| `event/codec.rs` | Durable value encoding with explicit version bytes. |
| `event/key.rs` | Holt key layout for coverage, source-offset de-dupe, event rows, type/time indexes, facets, and traces. |
| `event/notification.rs` | LingTai notification summary/snapshot parsing for modern `_meta` envelopes and legacy `payload`/`meta` rows. |
| `event/store.rs` | `AgentEventStore` trait and batch-ingest/query contracts. |
| `event/holt.rs` | Holt-backed implementation over an independent agent-index directory. |
| `event/ingest.rs` | JSONL streaming parser that records byte offsets and handles partial lines. |
| `src/bin/nokv-agent.rs` | LingTai CLI entry point for ingest and typed queries. |

Required key families:

| Family | Purpose |
| --- | --- |
| `coverage/{agent}/{source_file_hash}` | O(1) file coverage: file size, min/max offset, row count. |
| `source/{agent}/{source_file_hash}/{offset}` | Idempotent source-offset de-dupe. |
| `event/{agent}/{event_id}` | Compact event record; event id is derived from source-file hash and source offset. |
| `payloads:fields_json/{agent}/{event_id}/{chunk}` | External `fields_json` chunks for events whose full payload exceeds Holt's single-value limit. Reads transparently rehydrate this into `EventRecord.fields_json`. |
| `type_id/{agent}/{type}/{rev_id}` | Latest-by-type scans. |
| `type_ts/{agent}/{type}/{rev_ts}/{event_id}` | Recent timestamp queries. |
| `session/{agent}/{event_id}` | Ordered session-event pointer; values store event ids and replay rows are reconstructed from `event/{agent}/{event_id}`. |
| `notification_id/{agent}/{event_id}` | LingTai notification browser before/after traversal. |
| `notification_rev/{agent}/{rev_id}` | Newest-first `type LIKE '%notification%'` replacement. |
| `notification_prev/{agent}/{event_id}` | Direct older-neighbor pointer for notification browser traversal. |
| `notification_next/{agent}/{event_id}` | Direct newer-neighbor pointer for notification browser traversal. |
| `notification_tail/{agent}` | Append-order tail pointer for notification neighbor materialization. |
| `notification/{agent}/{field}/{value}/{event_id}` | Notification lifecycle lookup by `ref_id`, event id, call id, or channel. |
| `tui_clear_rev/{agent}/{rev_id}` | Newest-first TUI-originated `psyche_molt` / `clear_received` completion checks. |
| `tool/{agent}/{tool_name}/{action}/{rev_id}` | Tool/action history. |
| `trace/{agent}/{tool_call_id}/{phase}` | Tool call/result/reasoning correlation. |
| `facet/{agent}/{facet_name}/{bucket}` | Materialized top-N/facet counts for tool/action grouping. |

Ingest commits bounded batches so a large LingTai log does not exceed Holt WAL
record limits. Each batch atomically commits event rows, secondary indexes,
materialized facet counts, and coverage. `fields_json` is preserved in the
event record so LingTai's row projections, pretty-fields rendering, and parsed
notification views do not lose payload content relative to SQLite.

When the LingTai adapter shells out to `nokv-agent`, process-level timeout,
freshness cache, and stale-on-error fallback remain adapter responsibilities.
The Rust event index returns deterministic derived-index state; it does not own
the TUI worker scheduling policy.

Tests must cover replay, duplicate offsets inside one batch, chunked ingest,
partial trailing lines, large-field preservation, source-file truncation, and
coverage monotonicity.

## 6. Adding a new read tool

The flow crosses two crates because the verb logic stays in `nokv-meta`:

1. **`nokv-meta`** — implement the verb as a `pub` inherent method on
   `NoKvFs<M, O>` returning `Result<_, MetadError>`, and add its
   request/result/`Namespace*` types. Register any new indexed fields.
2. **`nokv-agent`** — add the method to the `AgentNamespace` trait; implement it
   in `impl AgentNamespace for NoKvFs` by calling the inherent method and
   `map_err`-ing into `AgentError`; add an `execute_<tool>` dispatcher arm, the
   argument parser, the result-builder, and an entry in
   `agent_tool_definitions()`.
3. **`nokv-client`** — implement the new trait method for `MetadataClient` and
   `NoKvFsClient<O>` (the remote path) using the RPC client.
4. **Tests** — unit-test the dispatcher against the `FakeNamespace` mock in
   `nokv-agent` (no engine needed), and confirm the benchmark tool-registry test
   still matches the arm surface.

## 7. Roadmap (verbs we expect to add)

The current surface answers **read** questions over a namespace. NoKV already
implements the write/stateful semantics below in `nokv-meta`/`nokv-client`; the
agent work is to give each a model-facing tool contract (idempotency, evidence
URIs, limits) and, where a verb mutates state, to extend the surface beyond
read-only with the same care as the read verbs.

- **Workspace checkpoints.** Atomic checkpoint publish already exists
  (`nokv-meta` publish/checkpoint paths; see [checkpointing](../checkpointing.md)).
  Expected verbs: publish a checkpoint generation and resolve "latest complete"
  so an agent can write a run's outputs and read back a crash-consistent view.
- **Copy-on-write workspaces.** Clone, snapshot, diff, and rollback exist
  (`nokv-meta` clone/snapshot/rollback; see [cow-workspaces](../cow-workspaces.md)).
  Expected verbs: `snapshot` (pin a frozen subtree view), `clone` (branch a
  workspace cheaply), `diff` (what changed between two generations), `rollback`.
- **Artifacts.** The artifact repository publishes bodies with digests and
  cleans up failed staged uploads (`nokv-client` artifact path). Expected verbs:
  publish an artifact and read it (including byte-range reads) with the digest
  and body manifest as citable evidence.
- **Events / watch.** Creates, renames, and publishes land as typed, replayable
  events with a cursor (`nokv-meta` watch). Expected verb: `watch`/`tail` a
  subtree from a cursor so an agent can react to changes instead of polling.

Design notes for these: most are **stateful or write** operations, so they do
not slot into today's read-only trait unchanged. Each needs an explicit tool
contract — argument grammar, idempotency/iflag semantics, evidence URIs, and
limits — and an answer to "what does the model see on partial failure." Open a
design issue before adding a write verb to the trait.

## 8. Where things live

| Concern | Location |
| --- | --- |
| Namespace tool definitions, dispatch, validation, `AgentError`, embedded impl | `crates/nokv-agent/src/namespace.rs` |
| LingTai event-index types, codecs, keys, ingest, Holt backend | `crates/nokv-agent/src/event/` |
| LingTai agent-index CLI | `crates/nokv-agent/src/bin/nokv-agent.rs` |
| LingTai workload extractor and SQLite baseline probes | `bench/agent-interface/scripts/lingtai_workload_baseline.py` |
| LingTai SQLite vs `nokv-agent` benchmark binary | `bench/src/bin/lingtai-index-bench.rs` |
| Public-surface integration test (the seven tools, `AgentError: Error`) | `crates/nokv-agent/tests/public_surface.rs` |
| LingTai event-index integration tests | `crates/nokv-agent/tests/event_index.rs` |
| Verb implementations (the six read methods) | `crates/nokv-meta/src/service/agent.rs` |
| Remote trait impls + re-export + `From<ClientError>` | `crates/nokv-client/src/agent.rs` |

See also the [code contract](code_contract.md) and the
[PR review checklist](pr_review_checklist.md).
