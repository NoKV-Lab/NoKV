<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# `nokv-agent` — Contributor Handbook

`nokv-agent` is the **agent tool surface** crate: the LLM-facing tool
definitions, the dispatcher that maps a tool call onto a namespace verb,
argument validation, result shaping, and the transport-neutral `AgentError`.
It is deliberately small, deliberately **transport-free**, and deliberately
**read-only** today.

This handbook is for contributors touching the agent surface. It covers where
the crate sits in the workspace, the invariants you must preserve, how to add a
tool, and the roadmap for the verbs we expect to add next.

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

`nokv-agent` depends on exactly three workspace crates plus `serde_json`:

| Dependency | Why it is load-bearing |
| --- | --- |
| `nokv-meta` | The six read verbs are `pub` inherent methods on `NoKvFs<M, O>`; the namespace vocabulary types (`Namespace*`) live here. |
| `nokv-object` | `grep`/`read` read real bytes through the `ObjectStore` bound on `NoKvFs<M, O>`. |
| `nokv-types` | Shared domain structs (`FileType`, `PathMetadata`, body descriptors, …). |
| `serde_json` | Tool argument parsing and result JSON. The **only** serde dependency. |

It depends on **none** of `nokv-client`, `nokv-protocol`, `nokv-control`. That
is the whole point — and it is enforced (see [the cycle rule](#4-invariants-do-not-break-these)).

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

## 4. Invariants (do not break these)

1. **Transport-free.** `nokv-agent` must never depend on `nokv-client`,
   `nokv-protocol`, or `nokv-control`. A reverse edge would create a dependency
   cycle and defeat the crate. Assert it: `cargo tree -p nokv-agent -e normal`
   must not mention any of those three.
2. **Read-only verb surface.** The `AgentNamespace` trait exposes only the six
   read verbs. Writes (e.g. `register_namespace_index`) stay as inherent methods
   on `NoKvFs` in `nokv-meta`, off the trait. Keep the model-facing surface
   read-only unless a write contract is explicitly designed (see [the roadmap](#6-roadmap-verbs-we-expect-to-add)).
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

## 5. Adding a new read tool

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

## 6. Roadmap (verbs we expect to add)

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

## 7. Where things live

| Concern | Location |
| --- | --- |
| Tool definitions, dispatch, validation, `AgentError`, embedded impl | `crates/nokv-agent/src/lib.rs` |
| Public-surface integration test (the seven tools, `AgentError: Error`) | `crates/nokv-agent/tests/public_surface.rs` |
| Verb implementations (the six read methods) | `crates/nokv-meta/src/service/agent.rs` |
| Remote trait impls + re-export + `From<ClientError>` | `crates/nokv-client/src/agent.rs` |
| Real-world consumer (the agent-interface benchmark) | `bench/src/bin/yanex-agent-bench.rs` |

See also the [code contract](code_contract.md) and the
[PR review checklist](pr_review_checklist.md).
