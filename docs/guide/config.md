<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Configuration & Options

NoKV exposes two configuration surfaces:

1. **Runtime options** for the embedded DB facade (`Options` in
   `local/options.go`).
2. **Cluster topology** for distributed mode (`raft_config.example.json` via
   `config.LoadFile/Validate`).

---

## 1. Runtime Options (Embedded DB)

`local.NewDefaultOptions()` returns a tuned baseline. Override fields before
calling `local.Open(opt)`, which returns `(*local.DB, error)`.

Key option groups (see `options.go` for the full list):

- **Paths & durability**
  - `WorkDir`, `SyncWrites`
- **Write pipeline**
  - `WriteBatchMaxCount`, `WriteBatchMaxSize`, `WriteBatchWait`
  - `MaxBatchCount`, `MaxBatchSize`
  - `WriteThrottleMinRate`, `WriteThrottleMaxRate`
- **Storage backend backend**
  - `StorageBackendFactory`
  - `StorageWriteBufferBytes`
  - `BlockCacheBytes`
- **Hot key throttling**
  - `WriteHotKeyLimit`
  - `ThermosEnabled`, `ThermosTopK`, decay/window settings
  - `ThermosNodeCap`, `ThermosNodeSampleBits`, `ThermosRotationInterval`
- **Control-log WAL watchdog**
  - `EnableControlWALWatchdog`, `ControlWALAutoGCInterval`
  - `ControlWALAutoGCMinRemovable`, `ControlWALAutoGCMaxBatch`
  - `ControlWALTypedRecordWarnRatio`, `ControlWALTypedRecordWarnSegments`
- **Raft lag warnings (stats only)**
  - `ControlLogLagWarnSegments`

Example:

```go
opt := local.NewDefaultOptions()
opt.WorkDir = "./data"
opt.SyncWrites = true
opt.WriteBatchMaxCount = 128
db, err := local.Open(opt)
if err != nil {
	log.Fatalf("open failed: %v", err)
}
defer db.Close()
```

Notes:
- `NewDefaultOptions()` populates the embedded local runtime defaults up front.
  The selected storage backend is Pebble today; Holt should plug in later through
  `StorageBackendFactory` and the same `storage/kv` boundary. `Open()` resolves
  constructor-owned defaults once, then the DB and storage backend consume
  the resolved values directly.
- `WriteBatchMaxCount`, `WriteBatchMaxSize`, `MaxBatchCount`, `MaxBatchSize`,
  `WriteThrottleMinRate`, `WriteThrottleMaxRate`, `StorageWriteBufferBytes`, and
  `ControlWALBufferSize` expose concrete defaults through
  `NewDefaultOptions()`. If you construct `Options` manually, leaving these
  fields at zero lets `Open()` resolve the constructor defaults.
- Batch knobs are split by owner:
  - `WriteBatchMaxCount` / `WriteBatchMaxSize` bound commit-worker request
    coalescing.
  - `MaxBatchCount` / `MaxBatchSize` bound internal apply/rewrite batches such
    as `batchSet`.
- Write slowdown is bandwidth-driven: `WriteThrottleMaxRate` applies when
  slowdown first becomes active, and pressure lowers the target rate toward
  `WriteThrottleMinRate`.

## 2. Raft Topology File

`raft_config.example.json` is consumed by every CLI in distributed mode.

### Two-layer semantics

The file has **two independent layers** with different lifecycles. Confusing
them is a common deployment mistake, so be explicit:

| Layer | Keys | Lifecycle | Source of truth |
|---|---|---|---|
| **Address directory** | `meta_root.peers`, `coordinator`, `stores`, `store_work_dir_template`, `max_retries` | Read on **every** CLI invocation. Keep in sync with deployed containers/hosts. | **This file is the source of truth.** Nothing else knows where to dial. |
| **Bootstrap seed** | `regions` or `fsmeta_region_bootstrap` | Read **only** on first startup by `scripts/ops/bootstrap.sh`. Once a store has `CURRENT`, bootstrap skips it. | After first bootstrap, **meta-root** owns the runtime region topology. Inspect with `nokv-config regions`. |

Consequence: editing bootstrap ranges after bootstrap is a no-op for running
clusters. Editing addresses is effective on the next CLI invocation
(restart / docker compose up).

### Precedence

When a value can come from both CLI and config file, **CLI wins**. Config is
a source of defaults:

```
--root-peer=1=host:2380   → explicit, used
(absent)                  → fall back to meta_root.peers[0].addr
```

### Minimal shape

```jsonc
{
  "max_retries": 8,
  "meta_root": {
    "peers": [
      { "node_id": 1,
        "addr": "127.0.0.1:2380",         // coordinator/host audit tools dial here
        "docker_addr": "nokv-meta-root-1:2380",
        "transport_addr": "127.0.0.1:3380", // sibling meta-root peers dial here for raft
        "docker_transport_addr": "nokv-meta-root-1:2480",
        "work_dir": "./artifacts/cluster/meta-root-1",
        "docker_work_dir": "/var/lib/nokv-meta-root" },
      { "node_id": 2, "...": "..." },
      { "node_id": 3, "...": "..." }
    ]
  },
  "coordinator": {
    "addr": "127.0.0.1:2379",
    "docker_addr": "nokv-coordinator-1:2379,nokv-coordinator-2:2379,nokv-coordinator-3:2379"
  },
  "store_work_dir_template": "./artifacts/cluster/store-{id}",
  "store_docker_work_dir_template": "/var/lib/nokv/store-{id}",
  "stores": [
    { "store_id": 1,
      "listen_addr": "127.0.0.1:20170",
      "addr": "127.0.0.1:20170",
      "docker_listen_addr": "0.0.0.0:20160",
      "docker_addr": "nokv-store-1:20160" }
  ],
  "fsmeta_region_bootstrap": {
    "mounts": ["default", "fsmeta-bench"],
    "bucket_count": 16,
    "region_id_base": 1000,
    "peer_id_base": 100000,
    "leader_store_ids": [1, 2, 3]
  }
}
```

### Field notes

- **`meta_root.peers`**: exactly 3 entries. `addr` is the gRPC service port
  (coordinators / host audit tools dial it). `transport_addr` is the raft
  transport port (sibling meta-root peers dial it for raft messages). They
  MUST be different ports on the same host.
- **`coordinator.addr` / `docker_addr`**: may be a single endpoint or
  comma-separated for multi-coord HA (`coord1:2379,coord2:2379,coord3:2379`).
  Gateways and stores use this list to failover on grant-not-held errors.
- **`stores[i]`**: `addr` is what other processes dial; `listen_addr` is what
  the store binds locally. Usually the same on host scope; different on
  docker scope (`0.0.0.0:20160` vs `nokv-store-1:20160`).
- **Store workdir resolution** (`ResolveStoreWorkDir`):
  1. store-scoped override (`stores[i].work_dir` / `docker_work_dir`)
  2. global template (must contain `{id}`)
  3. empty — caller falls back to its own default
- **`start_key` / `end_key`** accept plain strings, `hex:<bytes>`, or base64.
  Empty or `"-"` means unbounded.
- **`leader_store_id`** is bootstrap metadata only. Runtime routing comes
  from coordinator (`GetRegionByKey`), never from this field.
- **`fsmeta_region_bootstrap`** is mutually exclusive with explicit `regions`.
  It expands to continuous byte-range regions: gap ranges plus one range per
  fsmeta mount/bucket. This keeps the store/coordinator layer generic while
  giving fsmeta workloads stable locality and round-robin bootstrap leaders.

### CLI integration

`nokv meta-root --config <file> --node-id N` resolves `--peer`,
`--transport-addr`, `--workdir` from the meta_root section. Explicit flags
still override.

`nokv coordinator --config <file>` resolves `--addr` from `coordinator.addr`
and `--root-peer` from `meta_root.peers`. This is how docker-compose keeps
meta-root addresses in a single file.

Programmatic loading:

```go
cfg, _ := config.LoadFile("raft_config.example.json")
if err := cfg.Validate(); err != nil { /* handle */ }
peers := cfg.MetaRootServicePeers("docker") // id → gRPC addr
```

### Related tools

- `scripts/dev/cluster.sh --config raft_config.example.json`
- `scripts/ops/serve-meta-root.sh --config ... --node-id 1`
- `scripts/ops/serve-coordinator.sh --config ... --coordinator-id c1`
- `nokv-config stores` / `nokv-config regions` — query current rooted
  topology (not the JSON). Use these to diff against the deployment manifest
  after scheduler operations.
