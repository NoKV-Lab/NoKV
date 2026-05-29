<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Stats And Observability

NoKV exposes runtime health through:

- structured local snapshots;
- expvar (`/debug/vars`);
- `nokv stats` plain-text and JSON output;
- fsmeta-specific expvar groups from `nokv-fsmeta`.

The stats surface follows the current three-layer architecture. It exposes NoKV
metadata and distributed-runtime health, while keeping physical backend metrics
small and backend-neutral.

## Snapshot Domains

Current domains include:

| Domain | Owner |
| --- | --- |
| `storage` | Storage backend summary such as size bytes and key estimate. |
| `write` | Local commit queue and apply pipeline. |
| `raft` | Raft group and replay state. |
| `region` | Store-local region catalog counts. |
| `hot` | Optional Thermos hot-key observations. |
| `cache` | fsmeta sidecar cache diagnostics. |
| `transport` | gRPC/raft transport metrics. |

Removed self-managed LSM domains such as flush backlog, compaction score,
table-level range filters, and manifest counters are no longer NoKV mainline
stats. Pebble owns Pebble-specific internal metrics. Holt should expose its own
engine-internal counters behind a small adapter summary rather than leaking
Holt internals into fsmeta or raftstore.

## CLI

Offline:

```bash
nokv stats --workdir ./artifacts/cluster/store-1 --json
```

Online:

```bash
nokv stats --expvar http://127.0.0.1:9101/debug/vars --json
```

fsmeta gateway metrics:

```bash
curl http://127.0.0.1:9101/debug/vars | jq '.nokv_fsmeta_executor'
```

## fsmeta expvar Groups

`nokv-fsmeta --metrics-addr` exports:

- `nokv_fsmeta_executor`
- `nokv_fsmeta_watch`
- `nokv_fsmeta_quota`
- `nokv_fsmeta_mount`
- `nokv_fsmeta_sessions`
- `nokv_fsmeta_peras` when the experimental Peras path is enabled

## Backend Metrics Policy

`storage/kv.Stats` is intentionally small:

- `keys_estimate`
- `size_bytes`

Backend-specific details stay in the concrete backend package. This keeps the
main operational contract stable when switching from Pebble to Holt or another
ordered engine.
