<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Scripts Overview

NoKV now groups shell entrypoints by role instead of keeping every helper flat under `scripts/`.

## Layout

| Path | Role |
| --- | --- |
| `scripts/dev` | Local development and bootstrap helpers for running a cluster from `raft_config*.json`. |
| `scripts/ops` | Operator-style wrappers for bootstrap and long-running processes. |
| `scripts/lib` | Shared shell helpers for config lookup, workdir hygiene, and build/bootstrap rules. |
| `scripts/*.sh` | Tooling or benchmark entrypoints that are still intentionally top-level. |

This split is deliberate:
- `dev` scripts are allowed to help with local experiments and smoke tests.
- `ops` scripts must treat durable workdirs and config parsing strictly.
- `lib` is where shared rules live, so shell semantics do not drift across scripts.

## Bootstrap & Local Launch

### `scripts/dev/cluster.sh`
- Purpose: build `nokv` and `nokv-config`, start the canonical 333 separated dev cluster,
  seed fresh store workdirs, then start stores from `raft_config.json`.
- Starts:
  - three `nokv meta-root` processes (Truth plane; replicated is the only mode)
  - one `nokv coordinator` process (Service plane; always remote-rooted)
  - all configured stores (Execution plane)
- Uses shared rules from:
  - `scripts/lib/common.sh`
  - `scripts/lib/config.sh`
  - `scripts/lib/workdir.sh`
- Example:
  ```bash
  ./scripts/dev/cluster.sh --config ./raft_config.example.json --workdir ./artifacts/cluster
  ```
- Notes:
  - `--config` defaults to `./raft_config.example.json`
  - `--workdir` defaults to `./artifacts/cluster`
  - uses fixed local metadata-root gRPC endpoints `127.0.0.1:2380/2381/2382`
  - uses fixed local metadata-root raft transport endpoints `127.0.0.1:3380/3381/3382`
  - store workdirs are rejected if they contain unexpected files
  - logs stream with `[meta-root-<id>]` / `[coordinator]` / `[store-<id>]` prefixes
    and are still written to `root.log` / `coordinator.log` / `server.log`
  - this is a bootstrap/dev launcher, not a restart command
  - production-style restarts should run `scripts/ops/serve-meta-root.sh`,
    `scripts/ops/serve-coordinator.sh`, and `scripts/ops/serve-store.sh`
    directly against the same durable workdirs

### `scripts/ops/bootstrap.sh`
- Purpose: seed local peer catalog metadata into a set of store directories derived from a path template.
- Intended for:
  - Docker Compose bootstrap
  - local static-topology experiments
- Example:
  ```bash
  ./scripts/ops/bootstrap.sh --config /etc/nokv/raft_config.json --path-template /data/store-{id}
  ```
- Notes:
  - skips stores that already contain `CURRENT`
  - refuses to seed into dirty directories
  - this is bootstrap-only; it does not recover runtime topology
  - use `nokv serve` / `scripts/ops/serve-store.sh` to restart an existing store from the same workdir

### `scripts/ops/serve-store.sh`
- Purpose: thin wrapper around `nokv serve` for one store.
- Example:
  ```bash
  ./scripts/ops/serve-store.sh \
    --config ./raft_config.example.json \
    --store-id 1 \
    --workdir ./artifacts/cluster/store-1 \
    --scope local
  ```
- Notes:
  - resolves store listen/workdir/Coordinator defaults through `nokv serve --config`
  - remote peer recovery comes from `raftstore/localmeta`; config `stores` only
    provide `storeID -> addr`
  - no longer treats `config.regions` as restart-time topology truth
  - if a static transport override is needed, use `--store-addr <store-id>=<addr>`
    rather than a peer-id keyed mapping
  - `--scope docker` selects container-friendly addresses

### `scripts/ops/serve-meta-root.sh`
- Purpose: thin wrapper around `nokv meta-root` for one replicated metadata-root peer.
- Example:
  ```bash
  ./scripts/ops/serve-meta-root.sh \
    --config ./raft_config.example.json \
    --scope host \
    --addr 127.0.0.1:2380 \
    --workdir ./artifacts/cluster/meta-root-1 \
    --node-id 1
  ```
- Notes:
  - `meta_root.peers` in the config is the only peer-list source
  - `--workdir` is only a local directory override for the selected node
  - `--config` and `--node-id` are required; there is no single-process local mode
  - forwards shutdown signals to `nokv meta-root`

### `scripts/ops/serve-coordinator.sh`
- Purpose: thin wrapper around `nokv coordinator` for one coordinator process
  wired to an external 3-peer meta-root cluster.
- Example:
  ```bash
  ./scripts/ops/serve-coordinator.sh \
    --addr 127.0.0.1:2379 \
    --coordinator-id c1 \
    --root-peer 1=127.0.0.1:2380 \
    --root-peer 2=127.0.0.1:2381 \
    --root-peer 3=127.0.0.1:2382
  ```
- Notes:
  - `--root-peer` values are metadata-root gRPC service addresses, not raft transport
  - exactly 3 `--root-peer` values are required (mirrors the Truth-plane quorum)
  - `--coordinator-id` is required (stable grant holder id)
  - forwards shutdown signals to `nokv coordinator`

## Removed Migration Workflow

The old `scripts/ops/migrate-cluster.sh` wrapper has been removed together with
the operator-facing `nokv migrate` CLI. Current workdirs use the selected raw
backend format: Pebble today, Holt once its adapter is wired. This version does
not provide online migration from old self-managed LSM workdirs.

## Shared Shell Rules

### `scripts/lib/common.sh`
- shared repo-root detection
- shared build helpers for `nokv` and `nokv-config`
- shared TCP readiness helper

### `scripts/lib/config.sh`
- shared `nokv-config` lookups for:
  - store lines
  - region lines
  - Coordinator address
  - Coordinator workdir

### `scripts/lib/workdir.sh`
- shared workdir hygiene rules:
  - remove stale `LOCK`
  - reject unexpected directory contents
  - assert a directory is fresh before seeding or bootstrap

This is where shell-level correctness rules should keep living. New scripts should reuse these helpers instead of open-coding workdir or config parsing logic again.

## Tooling & Benchmarks

| Script | Purpose |
| --- | --- |
| `scripts/run_fsmeta_benchmarks.sh` | Execute the fsmeta workload matrix in local or Docker Compose mode. |
| `scripts/debug.sh` | Wrap `dlv test` for focused debugging. |
| `scripts/gen.sh` | Format protobufs and regenerate Go bindings through Buf. |

For recovery and transport fault validation, use direct Go tests instead of shell wrappers:

```bash
CHAOS_TRACE_METRICS=1 \
go test -run 'TestGRPCTransport(HandlesPartition|MetricsWatchdog|MetricsBlockedPeers)' -count=1 -v ./raftstore/transport
```

## Relationship with `nokv-config`

- `nokv-config stores` / `regions` / `coordinator` remain the structured topology source for shell scripts.
- `config.regions` remain bootstrap/deployment metadata, not restart-time peer truth.
- `nokv-config catalog` writes Region metadata into the local peer catalog.
- Go tools can import `github.com/feichai0017/NoKV/config` and call `config.LoadFile` / `Validate` directly.

Maintaining a single `raft_config.json` still keeps development scripts, Docker Compose, and automated tests aligned. The difference now is that shell behavior is shared and explicit instead of repeated across four separate entrypoints.
