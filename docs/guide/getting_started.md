<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Getting Started

This guide gets you from zero to a running NoKV cluster (or an embedded DB) in a few minutes.

## Prerequisites
- Go 1.26+
- Git
- (Optional) Docker + Docker Compose for containerized runs

## Option A: Local Cluster (recommended for dev)
This launches the 333 separated layout: 3 replicated meta-root peers (Truth plane),
1 coordinator (Service plane), and the stores declared in the config (Execution plane).

```bash
./scripts/dev/cluster.sh --config ./raft_config.example.json
```

The launcher stays attached, streams meta-root/Coordinator/store logs to the terminal, and also writes them under `./artifacts/cluster/`.

If you stop one store and want to restart it later, restart it against the same
workdir:

```bash
./scripts/ops/serve-store.sh \
  --config ./raft_config.example.json \
  --store-id 1 \
  --workdir ./artifacts/cluster/store-1
```

On restart, NoKV recovers hosted peers from local metadata in the store workdir.
The config file is only used to start stores and Coordinator. Runtime clients
discover store addresses from Coordinator heartbeats.
Do not rerun `scripts/ops/bootstrap.sh` or treat `scripts/dev/cluster.sh` as the
restart path for an already-running store.

### Inspect stats

```bash
go run ./cmd/nokv stats --workdir ./artifacts/cluster/store-1
```

## Option B: Docker Compose
This runs the full cluster (3 meta-root + 3 coordinator + 3 store + fsmeta gateway)
in containers with the published GHCR image.

```bash
docker compose up -d
docker compose logs -f
```

To force-refresh `:latest` before startup, use:

```bash
make docker-up
```

`make docker-up` pulls the published image first. If the GHCR package is not
published or public yet, it falls back to a local Docker build.

For local Docker development builds from this checkout:

```bash
docker compose up -d --build
```

Local builds are tagged as the configured NoKV image. If you build locally and
then want to return to the published `:latest`, run the pull command above.
For reproducible runs, pin a published SHA tag:

```bash
NOKV_IMAGE_TAG=<commit-sha> docker compose up -d
```

Tear down:

```bash
docker compose down -v
```

## Embedded Usage (single-process)
Use NoKV as a library when you do not need raftstore.

```go
package main

import (
	"fmt"
	"log"

	"github.com/feichai0017/NoKV/local"
)

func main() {
	opt := local.NewDefaultOptions()
	opt.WorkDir = "./workdir-demo"

	db, err := local.Open(opt)
	if err != nil {
		log.Fatalf("open failed: %v", err)
	}
	defer db.Close()

	key := []byte("hello")
	if err := db.Set(key, []byte("world")); err != nil {
		log.Fatalf("set failed: %v", err)
	}

	entry, err := db.Get(key)
	if err != nil {
		log.Fatalf("get failed: %v", err)
	}
	fmt.Printf("value=%s\n", entry.Value)
}
```

> Note:
> - `DB.Get` returns detached entries (do not call `DecrRef`).
> - `DB.GetInternalEntry` returns borrowed entries and callers must call `DecrRef` exactly once.
> - `DB.SetWithTTL` accepts `time.Duration` (relative TTL). `DB.Set`/`DB.SetBatch`/`DB.SetWithTTL` reject `nil` values; use `DB.Del` or `DB.DeleteRange(start,end)` for deletes.
> - `DB.NewIterator` exposes user-facing entries, while `DB.NewInternalIterator` scans raw internal keys (`cf+user_key+ts`).

## Benchmarks
Micro benchmarks:

```bash
go test -bench=. -run=^$ ./...
```

fsmeta workload matrix:

```bash
NOKV_FSMETA_BENCH_MODE=local make fsmeta-bench
```

Override defaults with env vars:

```bash
NOKV_FSMETA_PROFILE=official NOKV_FSMETA_WORKLOADS=mdtest-easy make fsmeta-bench
```
Detailed benchmark methodology and latest result snapshots are maintained in:
[`benchmark/README.md`](https://github.com/feichai0017/NoKV/blob/main/benchmark/README.md).

## Cleanup
If a local run crashes or you want a clean slate:

```bash
make clean
```

## Troubleshooting
- **WAL replay errors after crash**: wipe the workdir and restart the cluster.
- **Port conflicts**: adjust addresses in `raft_config.example.json`.
- **Slow startup**: use `NOKV_FSMETA_PROFILE=median` for a smaller local benchmark run.
