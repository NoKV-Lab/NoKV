<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# CLI (`cmd/nokv`) Reference

`nokv` provides operational visibility for NoKV metadata and distributed
runtime state, with script-friendly JSON output.

---

## Installation

```bash
go install ./cmd/nokv
```

---

## Shared Flags

- `--workdir <path>`: NoKV database directory
- `--json`: JSON output (default is plain text)
- `--expvar <url>`: for `stats`, fetch from `/debug/vars`
- `--no-region-metrics`: for offline `stats`, skip attaching runtime region metrics

---

## Subcommands

### `nokv stats`

- Reads `StatsSnapshot` either offline (`--workdir`) or online (`--expvar`)
- JSON output is nested by domain (not flat)

Common fields:

- `storage.keys_estimate`, `storage.size_bytes`
- `wal.active_segment`, `wal.segment_count`, `wal.typed_record_ratio`
- `write.queue_depth`, `write.queue_entries`, `write.hot_key_limited`
- `region.total`, `region.running`, `region.removing`
- `hot.write_keys`
- `transport.*`

Example:

```bash
nokv stats --workdir ./testdata/db --json | jq '.storage.size_bytes'
```

### `nokv execution`

- Queries execution-plane diagnostics from a running raftstore admin endpoint
- Exposes:
  - last admission decision observed by the store
  - restart-status summary (`state`, hosted region count, raft-group count, missing raft pointers)
  - current topology transition status as seen by the execution plane
- Supports plain-text or `--json` output
- Common flags:
  - `--addr` raftstore admin address (required)
  - `--region` optional region filter
  - `--transition` optional transition id filter
  - `--timeout`
  - `--json`

Example:

```bash
nokv execution --addr 127.0.0.1:20161 --json
```

### `nokv regions`

- Dumps the local peer catalog used for store recovery (state/range/epoch/peers)
- Supports `--json`

### `nokv serve`

- Starts StoreKV gRPC service backed by local `raftstore`
- Requires `--store-id`
- Also requires enough address metadata to reach the Coordinator and all remote
  raft peers:
  - either explicit flags (`--workdir`, `--addr`, `--coordinator-addr`, `--store-addr`)
  - or `--config <raft_config.json> --scope host|docker`
- Common flags:
  - `--addr` (default `127.0.0.1:20160`)
  - `--config`, `--scope host|docker`
  - `--metrics-addr` (optional expvar endpoint, exposes `/debug/vars`)
  - `--store-addr storeID=address` (repeatable override for remote store transport addresses)
  - `--election-tick`, `--heartbeat-tick`
  - `--raft-max-msg-bytes`, `--raft-max-inflight`
  - `--raft-tick-interval`, `--raft-debug-log`

Example:

```bash
nokv serve \
  --config ./raft_config.example.json \
  --scope host \
  --store-id 1 \
  --workdir ./artifacts/cluster/store-1
```

Restart semantics:

- hosted peers come from `raftstore/localmeta`, not `raft_config.json` region lines
- `--config` is used only to resolve:
  - store listen address
  - Coordinator address
  - `storeID -> addr`, which `serve` expands into remote `peerID -> addr`
- `--store-id` must match the durable workdir identity once the workdir has been used
- `--store-addr` is only an exceptional static override; it is keyed by stable
  `storeID`, not by mutable runtime `peerID`

### Removed migration commands

The old operator-facing `nokv migrate` and `nokv manifest` commands are no
longer part of the mainline CLI. Current workdirs use the selected storage backend
format: Pebble today, Holt once its adapter is wired. This version does not
provide an online migration path from old self-managed LSM workdirs.

### `nokv coordinator`

- Starts the Coordinator gRPC service. NoKV only supports the separated
  topology: coordinator always connects to an external 3-peer meta-root
  cluster via gRPC.
- Required flags:
  - `--coordinator-id` (stable grant holder id)
  - `--root-peer nodeID=addr` (exactly 3 meta-root gRPC endpoints)
- Common flags:
  - `--addr` (default `127.0.0.1:2379`)
  - `--grant-ttl`, `--grant-renew-before` (default `10s` / `3s`)
  - `--shutdown-grace` (default: `--grant-ttl`, or `10s` when grant TTL is disabled)
  - `--root-refresh` (default `200ms`)
  - `--id-start`, `--ts-start` (allocator seeds; only used when the meta-root cluster has no allocator state yet)
  - `--config` + `--scope host|docker` (resolves `--addr` from `raft_config.json`)
  - `--metrics-addr` (optional expvar endpoint, exposes `/debug/vars`)

Example:

```bash
nokv coordinator \
  --addr 127.0.0.1:2379 \
  --coordinator-id c1 \
  --root-peer 1=127.0.0.1:2380 \
  --root-peer 2=127.0.0.1:2381 \
  --root-peer 3=127.0.0.1:2382
```

### `nokv meta-root`

- Starts one peer of the 3-peer replicated metadata-root cluster. NoKV only
  supports the replicated topology; single-process local mode has been
  removed from the CLI.
- Required flags:
  - `--workdir`, `--node-id`, `--transport-addr`
  - `--peer nodeID=addr` (repeatable, exactly 3)
- Common flags:
  - `--addr` (default `127.0.0.1:2380`, gRPC listen)
  - `--tick-interval` (default `1000ms`)
  - `--metrics-addr` (optional expvar endpoint)

Example:

```bash
nokv meta-root \
  --addr 127.0.0.1:2380 \
  --workdir ./artifacts/meta-root-1 \
  --node-id 1 \
  --transport-addr 127.0.0.1:3380 \
  --peer 1=127.0.0.1:3380 \
  --peer 2=127.0.0.1:3381 \
  --peer 3=127.0.0.1:3382
```

### Script Helpers

- `scripts/ops/serve-meta-root.sh`
  - Starts one replicated `meta-root` peer and forwards shutdown signals.
  - Requires `--config` and `--node-id`; `meta_root.peers` is the only peer-list source.
- `scripts/ops/serve-coordinator.sh`
  - Starts one `nokv coordinator` against an external meta-root cluster.
  - Requires `--coordinator-id` and 3 `--root-peer` values (meta-root gRPC endpoints).
- `scripts/ops/serve-store.sh`
  - Starts one `nokv serve` store against an existing durable workdir.
- `scripts/ops/bootstrap.sh`
  - Seeds fresh store workdirs from `config.regions`; not a restart tool.
- `scripts/dev/cluster.sh`
  - Dev bootstrap for the 333 separated layout: `3 meta-root + 1 coordinator(remote) + stores`.
  - Uses fixed local root endpoints:
    - gRPC: `127.0.0.1:2380/2381/2382`
    - raft transport: `127.0.0.1:3380/3381/3382`

### Expvar Keys

- `nokv_coordinator`
  - Published by `nokv coordinator --metrics-addr ...`
  - Includes:
    - `root_mode`
    - rooted read-state summary
    - grant state
    - allocator window state

---

## Integration Tips

- Combine with `RECOVERY_TRACE_METRICS=1` for recovery validation.
- In CI, compare JSON snapshots to detect observability regressions.
- Use `nokv stats --expvar` for online diagnostics and `--workdir` for offline forensics.
