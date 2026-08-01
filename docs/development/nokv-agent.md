<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# `nokv-agent` Contributor Handbook

`nokv-agent` owns the transport-free Workbench facade: tool schemas, calls into
SDK traits, and stable Agent-facing result/error shaping. It does not own
routing, Holt keys, object-provider clients, or lifecycle state machines.

The normative behavior is [Workbench Contract](../workbench-contract.md).

## Package Position

```text
CLI / MCP adapter
  -> nokv-agent
  -> nokv-client traits
  -> versioned protocol and direct object data path
```

Allowed dependencies are storage-neutral types and SDK interfaces. The crate
must not import:

- `nokv-meta` or Holt;
- `nokv-server`;
- provider-specific object implementations;
- control-plane storage;
- host-filesystem emulation.

An embedded test adapter may implement an SDK trait, but it cannot become a
second namespace, publication, snapshot, commit, or restore implementation.

## Stable Tool Surface

A supported deployment exposes exactly:

```text
workbench_create
workbench_put_file
workbench_append
workbench_edit
workbench_list
workbench_stat
workbench_read
workbench_grep
workbench_search
workbench_aggregate
workbench_catalog
workbench_find
workbench_commit
workbench_snapshot
workbench_snapshot_renew
workbench_snapshot_retire
workbench_snapshot_list
workbench_restore
```

The normalized input schemas are frozen in
`crates/nokv-agent/workbench_contract_schema.json`. Tool registration
fails closed when a name or normalized schema differs.

## Adapter Responsibilities

The crate may own:

- section/path jail validation before calling the SDK;
- base64 and structured read shaping;
- exact-string edit planning over SDK reads and conditional writes;
- grep pattern validation and presentation;
- stable field names and result projections;
- friendly typed error messages and retryability projection;
- commit and restore manifest presentation required by the Workbench contract.

The crate must delegate:

- root placement and retries to `nokv-client`;
- path normalization authority to `nokv-types`;
- metadata transactions and lifecycle CAS operations to the server/meta domain;
- immutable object upload/read execution to SDK and object interfaces;
- ownership fences and durability to their owning packages.

## Contract Invariants

- `workbench_put_file(replace=false)` is create-only.
- `workbench_put_file(replace=true)` is replace-only.
- Append, edit, and replace preserve generation CAS behavior.
- An exact retry returns the same result.
- Reusing a request identity with different inputs fails.
- Snapshot reads stay at their fixed read version.
- Snapshot renewal extends only and never revives a reaped snapshot.
- Restore preserves the source, requires an absent destination, hides staging,
  and converges on one terminal result.
- The five standard sections remain virtual projections.
- Internal keys, owners, incarnations, and provider credentials never appear in
  Agent-facing results.

## Adding Or Changing Behavior

1. Start from the stable Workbench behavior, result, and error contract.
2. Add or change the storage-neutral SDK trait method in the client boundary.
3. Implement result shaping in `nokv-agent` without importing storage details.
4. Update the normalized schema snapshot only for an explicitly approved
   contract change.
5. Add boundary tests for success, typed failure, conflict, and exact replay.
6. Run the complete 18-tool schema and golden-transcript checks.

Do not add forwarding aliases, optional duplicate tool names, or fallback
results. NoKV accepts an explicit contract change when it is required; it does
not keep two observable behaviors indefinitely.

## Tests

Required local coverage includes:

- jail and path rejection;
- exact normalized input schemas;
- create/replace distinction;
- generation and digest relationships;
- structured and ranged reads;
- query pagination and projections;
- commit identity and replay;
- snapshot states and frozen results;
- restore staging invisibility and terminal replay;
- stable error code, message fields, and retryability.

Run:

```bash
cargo test -p nokv-agent
python3 scripts/lingtai-workbench/workbench_contract_test.py
```

Schema-only success is not complete conformance. Product qualification follows
[Workspace Acceptance](./workspace-acceptance.md).
