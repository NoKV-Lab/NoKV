<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Benchmarks

This document captures the most recent results from running the default
benchmark script (`scripts/run_benchmarks.sh`).

## YCSB Framework Overview

The benchmark harness uses the YCSB workloads (A/B/C/D/E/F) to exercise NoKV,
Badger, and Pebble by default (RocksDB is optional via build tags) with a fixed total operation count and report both
throughput and latency percentiles. The default `nokv` engine uses the local
Pebble-backed NoKV runtime. The default script runs a load phase to seed data,
then executes each workload and collects:
- Ops/s, average latency, and latency percentiles (P50/P95/P99)
- Operation mix counts (reads, updates, inserts, scans, read-modify-write)
- Value size stats and total data size

## Test Environment

- Machine: MacBook Pro (Apple M3 Pro)
- Memory: 36 GB

## YCSB Architecture

The YCSB harness is organized as a Go test entrypoint plus a small engine
abstraction so every storage engine is driven by the same workload generator,
key distribution, and metrics pipeline.

Flow:

```mermaid
flowchart TD
    A["scripts/run_benchmarks.sh"] --> B["go test ./ycsb -run TestBenchmarkYCSB -args <flags>"]
    B --> C["TestBenchmarkYCSB (benchmark/ycsb/ycsb_test.go)"]
    C --> D["runYCSBBenchmarks (benchmark/ycsb/ycsb_runner.go)"]
    D --> E["engine.Open(clean)"]
    D --> F["ycsbLoad (parallel preload)"]
    D --> G["optional warm-up"]
    D --> H["ycsbRunWorkload (parallel workload run)"]
    D --> I["engine.Close"]
```

Key components:

- Engine interface: `benchmark/ycsb/ycsb_engine.go` defines `Read/Insert/Update/Scan`
  and per-engine implementations live in `benchmark/ycsb/ycsb_engine_*`.
- Engine profiles: each engine is constructed from an explicit benchmark
  profile in `benchmark/ycsb/ycsb_profiles.go`; the harness does not inherit
  `local.NewDefaultOptions()` or `badger.DefaultOptions()` implicitly, which
  keeps benchmark semantics stable across runtime default changes. The default
  profile uses a 512MB total cache budget and splits it explicitly per engine:
  Pebble uses a single 512MB cache, Badger defaults to 256MB block + 256MB
  index, and NoKV defaults to 384MB block + 128MB index.
- Benchmark script defaults now pass `value_threshold=2048` unless overridden
  through `YCSB_VALUE_THRESHOLD`; `ycsb_value_size` remains `1000` by default,
  so the stock CI run still measures inline values unless the value size or
  threshold is changed explicitly.
- Workload model: `benchmark/ycsb/ycsb_runner.go` defines YCSB A/B/C/D/E/F mixes,
  request ratios, and key distributions (zipfian/uniform/latest).
- Official-aligned defaults: insert order uses `hashed`, workload E uses
  `maxscanlength` + `uniform` scan length distribution, warm-up is disabled
  by default, and value size defaults to ~1KB.
- Value generator: fixed/uniform/normal/percentile sizing with a shared buffer
  pool to reduce allocations (`valuePool`).
- Concurrency model: each workload runs with `ycsb_conc` goroutines; each op
  records latency samples and operation counts; optional global throttling is
  available via `ycsb_target_ops`.
- Workload isolation: each workload reopens and reloads the engine to avoid
  cross-workload state pollution (compaction debt/history carry-over).
- Results pipeline: summaries are printed to stdout, written as CSV under
  `data/ycsb/results`, and a text report is saved under
  `results/ycsb/ycsb_results_*.txt`.

## FSMetadata Service Evaluation

The fsmeta benchmark drives the native `nokv-fsmeta` gateway against a running
NoKV cluster. It is a service benchmark for fsmeta's server-side API surface:

- `mdtest-easy`: IO500/mdtest easy metadata projection. Each client owns a
  private directory, creates zero-byte files, stats them, scans with
  `ReadDirPlus`, and unlinks them.
- `mdtest-hard`: IO500/mdtest hard metadata projection. All clients contend on
  one shared directory and create 3901-byte metadata records before stat, scan,
  and unlink.
- `filebench-varmail`: Filebench `workloads/varmail.f` projection. The official
  personality defaults to `nfiles=1000`, `nthreads=16`, mean file size 16 KiB,
  and mean append size 16 KiB; the fsmeta projection maps file-body work to
  inode-size updates and writer-session lifecycle operations.
- `mimesis-namespace`: MimesisBench namespace-model projection. It exercises
  create, rename, setattr, lookup, directory scan, and unlink churn.
- `ai-checkpoint-agent`: MLPerf Storage checkpointing-inspired metadata
  projection. MLPerf v2.0 defines Llama-style 8B/70B/405B/1T checkpoint scales;
  this benchmark measures the metadata side of checkpoint publication: artifact
  fan-out, manifest update/rename, watch, snapshot read, and snapshot retire.

Prerequisites for the default Compose mode:

- a running NoKV Docker Compose cluster, or equivalent coordinator/store/fsmeta
  deployment
- the `nokv-fsmeta` gRPC endpoint reachable from the benchmark process
- the coordinator endpoint reachable for mount bootstrap

Default Docker Compose run from the repository root:

```bash
make fsmeta-bench
```

The helper starts Docker Compose with a local image build, waits for fsmeta and
coordinator ports, then writes a CSV under `benchmark/data/fsmeta/results/`.
Compose enables both `--negative-cache-dir` and `--dirpage-cache-dir` on the
fsmeta gateway. The workload source of truth is
`benchmark/fsmeta/profiles/official/workloads.yaml`: it records the public
IO500/mdtest, Filebench varmail, MimesisBench, and MLPerf Storage
checkpointing source links, the official shape, and NoKV's metadata-service
projection. It does not vendor third-party benchmark code or claim a certified
IO500/Filebench/MLPerf score.

For CI and single-node tuning, run the same workload driver against the embedded
fsmeta backend:

```bash
NOKV_FSMETA_BENCH_MODE=local make fsmeta-bench
```

Local mode builds `cmd/nokv-fsmeta`, starts it with `--backend local`, skips
coordinator mount bootstrap, and writes `fsmeta_local_*` CSV/manifests. It is
the right benchmark for the single-node product shape; Compose mode remains the
distributed raftstore/Peras benchmark.

Default scale is the PR-oriented `median` service run from that profile: 12
clients, 16 mdtest/mimesis directories x 256 files, 16 varmail users x 128
messages, and 4 AI workspaces x 64 checkpoint publishes x 8 artifact files.
Use `NOKV_FSMETA_PROFILE=long` for the scheduled larger profile or
`NOKV_FSMETA_PROFILE=official` for the profile's official-size shape. Override
scale with environment variables such as `NOKV_FSMETA_CLIENTS`,
`NOKV_FSMETA_DIRS`, `NOKV_FSMETA_FILES_PER_DIR`, `NOKV_FSMETA_USERS`,
`NOKV_FSMETA_MESSAGES_PER_USER`, `NOKV_FSMETA_WORKSPACES`,
`NOKV_FSMETA_CHECKPOINTS_PER_WORKSPACE`, `NOKV_FSMETA_FILES_PER_CHECKPOINT`,
and `NOKV_FSMETA_WORKLOADS`. The default writer-session TTL is 5 minutes so
session leases do not dominate throughput measurements. The script also waits
20 seconds after ports open so a
fresh Compose cluster can finish Raft leader election and coordinator grant
publication; set `NOKV_FSMETA_STABILIZE_SECONDS=0` for an already-warm cluster.
The underlying script is `scripts/run_fsmeta_benchmarks.sh`; set
`NOKV_FSMETA_BENCH_MODE=local`, `compose`, or `derived-cache` to choose the
runtime shape. The cache on/off slice still requires a running distributed
cluster because it compares two raftstore-backed gateways.

By default the Compose matrix resets Docker volumes between workloads
(`NOKV_FSMETA_RESET_BETWEEN_WORKLOADS=1`). This keeps each workload from
measuring the previous workload's Peras overlay, recovery state, or background
segment backlog. For same-cluster soak profiling, set it to `0`; in that mode
the idle gate waits for install and seal queues to drain. Set
`NOKV_FSMETA_PERAS_IDLE_REQUIRE_PENDING=1` only when the run is explicitly
measuring synchronous durable drain, because `pending` counts visible
operations that may remain in the holder overlay.

The Go benchmark writes both the CSV and a sibling `.manifest.txt` file. The CSV
contains `source`, `source_url`, and `projection` columns; the manifest records
the exact resolved scale parameters, the selected profile, and official
workload provenance used for the run.

For server-side profiling, set `NOKV_FSMETA_CAPTURE_PROFILES=1`. The helper
captures concurrent CPU profiles from fsmeta, stores, coordinators, and
meta-root processes through their diagnostics ports, then packages CPU, heap,
allocs, goroutine, and expvar snapshots under
`benchmark/data/fsmeta/profiles/`. Main-push and scheduled long fsmeta CI runs
enable this automatically; PR median runs keep it disabled to avoid adding
diagnostic overhead to the gating path. Override capture length with
`NOKV_FSMETA_PROFILE_SECONDS` or endpoints with `NOKV_FSMETA_PROFILE_TARGETS`
using `name=host:port` comma-separated entries.

Direct run from inside the `benchmark/` Go module:

```bash
cd benchmark
NOKV_FSMETA_BENCH=1 go test ./fsmeta -run TestBenchmarkFSMeta -count=1 -v -args \
  -fsmeta_addr 127.0.0.1:8090 \
  -fsmeta_coordinator_addr 127.0.0.1:2390,127.0.0.1:2391,127.0.0.1:2392 \
  -fsmeta_scale_profile median \
  -fsmeta_workloads mdtest-easy,mdtest-hard,filebench-varmail,mimesis-namespace,ai-checkpoint-agent
```

Native fsmeta now assigns Create inode IDs inside the fsmeta service using the
coordinator `AllocID` authority and a shard-affine allocator.

For derived-cache runs, start `nokv-fsmeta` with:

```bash
nokv-fsmeta \
  --negative-cache-dir /tmp/nokv-fsmeta-negative \
  --dirpage-cache-dir /tmp/nokv-fsmeta-dirpage
```

Then compare the read-heavy official-aligned slices:

- `mdtest-hard` for a shared-directory ReadDirPlus path after metadata writes.
- `mimesis-namespace` for mixed rename/setattr/lookup/ReadDirPlus churn.

For a fixed on/off comparison, run the helper from the repository root after
the coordinator and stores are already up:

```bash
NOKV_FSMETA_COORDINATOR_ADDR=127.0.0.1:2390,127.0.0.1:2391,127.0.0.1:2392 \
  NOKV_FSMETA_BENCH_MODE=derived-cache scripts/run_fsmeta_benchmarks.sh
```

The helper starts two fsmeta gateways against the same cluster:

- cache-off at `127.0.0.1:8090`
- cache-on at `127.0.0.1:8091` with both `--negative-cache-dir` and
  `--dirpage-cache-dir`

It writes two CSV files named `fsmeta_derived_cache_off_*` and
`fsmeta_derived_cache_on_*` under `data/fsmeta/results/`.

The summary CSV is written under `data/fsmeta/results/` unless
`-fsmeta_output` is set. Rows include a `driver` column with the fixed value
`native-fsmeta` to identify the service driver. CI uploads these runtime outputs
as artifacts instead of committing benchmark result packages.

## Research Plotting

The `benchmark/plot` subpackage provides publication-oriented plotting helpers
for benchmark outputs. It is intended for paper figures rather than ad-hoc
console visualization:

- consistent academic theme
- colorblind-safe palette
- grouped bar charts for engine/workload comparison
- direct support for `[]BenchmarkResult`
- direct parsing of `data/ycsb/results/*.csv`
- vector output (`.svg`, `.pdf`) as well as bitmap output (`.png`)

Minimal example:

```go
package main

import (
    bench "github.com/feichai0017/NoKV/benchmark/ycsb"
    benchplot "github.com/feichai0017/NoKV/benchmark/plot"
)

func render(results []bench.BenchmarkResult) error {
    return benchplot.WriteGroupedBarChartFromResults(results, benchplot.ResultGroupedBarChartConfig{
        Metric: benchplot.MetricP95LatencyUS,
        GroupedBarChartConfig: benchplot.GroupedBarChartConfig{
            Title:  "YCSB P95 Latency",
            Output: "figures/ycsb_p95.svg",
        },
    })
}
```

If the results already exist as CSV:

```go
results, err := benchplot.ReadYCSBResultsCSV("data/ycsb/results/ycsb_results_20260416_120000.csv")
if err != nil {
    return err
}
```

Recommended usage for paper figures:

- use `.svg` during drafting for clean vector output
- group by workload or engine, not both at once in overly dense charts
- keep one figure tied to one claim
- prefer throughput, P95/P99 latency, and rebuild/materialize cost over dumping every metric

There is also a small CLI entrypoint for repeatable figure generation:

```bash
go run ./cmd/plotbench \
  -format ycsb \
  -input data/ycsb/results/ycsb_results_20260416_120000.csv \
  -metric p95_latency_us \
  -title "YCSB P95 Latency" \
  -output figures/ycsb_p95.svg
```

For namespace / metadata-service figures, use the generic `observations` CSV
format. By default the data columns are:

```text
category,series,value
steady-paginated,secondary-index,231.1
steady-paginated,repairing-read-plane,35.9
steady-paginated,strict-read-plane,35.4
```

Then render with a domain preset:

```bash
go run ./cmd/plotbench \
  -format observations \
  -preset namespace_pagination_modes \
  -input figures/namespace_latency.csv \
  -title "Steady-State Paginated Listing" \
  -output figures/namespace_latency.svg
```

The plotting path is now configuration-driven rather than preset-driven. A
single config CSV can control:

- chart title and axis labels
- figure size
- legend / grid visibility
- category / series ordering
- observation CSV column mapping

Example chart config CSV:

```text
title,Steady-State Paginated Listing
xlabel,Workload Slice
ylabel,Latency (µs)
width_in,6.8
height_in,3.9
show_grid,true
hide_legend,false
category_order,steady-paginated
series_order,secondary-index,repairing-read-plane,strict-read-plane
category_column,mode
series_column,implementation
value_column,latency_us
```

Example observation CSV using custom columns:

```text
mode,implementation,latency_us
steady-paginated,secondary-index,231.1
steady-paginated,repairing-read-plane,35.9
steady-paginated,strict-read-plane,35.4
```

Render with the generic config:

```bash
go run ./cmd/plotbench \
  -format observations \
  -config figures/steady_pagination_config.csv \
  -input figures/steady_pagination.csv \
  -output figures/steady_pagination.svg
```

Flags still override config CSV values when needed, for example:

```bash
go run ./cmd/plotbench \
  -format observations \
  -config figures/steady_pagination_config.csv \
  -input figures/steady_pagination.csv \
  -output figures/steady_pagination.svg \
  -title "Strict Listing Steady-State Comparison" \
  -hide-legend
```

Current metadata-oriented presets:

- `namespace_steady_state`
- `namespace_pagination_modes`
- `namespace_mixed_pagination`
- `namespace_deep_descendants`
- `namespace_repair_cost`
- `metadata_latency`

Run:

```bash
cd benchmark
go test ./plot ./fsmeta ./fsmeta/workload
```
