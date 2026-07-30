---
title: Benchmarks
---

<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Benchmarks

NoKV keeps product microbenchmarks inside the product crates and puts benchmark
entry points in the root-level `bench/` package. The `nokv-bench` binary covers
metadata smoke, metadata durability modes, MLPerf Storage/DLIO-style generated
training reads, and checkpoint publish/read paths:

```bash
cargo run --release -p nokv-bench --bin nokv-bench -- \
  --profile smoke \
  --workload all
```

The default object backend is a local RustFS endpoint at
`http://127.0.0.1:9000`, bucket `nokv`, with the standard local RustFS
development credentials. Start RustFS first when running object-backed
workloads.

The Yanex agent-interface benchmark is a second binary in the same package, with
assets and published telemetry under `bench/agent-interface/`:

```bash
cargo run --release -p nokv-bench --bin yanex-agent-bench -- list-tasks
bench/agent-interface/scripts/run_phase1_batch.sh --help
```

For a disposable local RustFS-backed end-to-end run, use the repository script:

```bash
scripts/run-rustfs-e2e.sh
```

The script starts RustFS with the training-profile buffer settings, creates the
default bucket, runs `nokv-bench`, and removes its temporary RustFS data
directory. Override the workload with environment variables, for example:

```bash
NOKV_E2E_PROFILE=standard \
NOKV_E2E_WORKLOAD=checkpoint-publish \
NOKV_E2E_OBJECT_CONCURRENCY=8 \
scripts/run-rustfs-e2e.sh
```

For the generated training-profile smoke harness, use:

```bash
scripts/run-ai-training-smoke.sh
```

The default gate runs the local client/server path for metadata reads,
checkpoint publish, and MLPerf/DLIO-shaped data movement. You can narrow it to
a single workload while iterating:

```bash
scripts/run-ai-training-smoke.sh checkpoint-publish
```

For the packed-shard data-path matrix over RustFS, use:

```bash
scripts/run-ai-shard-range-matrix.sh
```

That script runs three isolated `ai-shard-range-read` cases through disposable
RustFS instances: small sparse exact reads, small sparse gap-coalesced reads,
and the 1 MiB read-ahead admission smoke. It streams each case to the terminal,
writes per-case raw logs, and writes one combined CSV with a leading
`matrix_case` column. Set `NOKV_AI_SHARD_MATRIX_OUTPUT_DIR` or
`NOKV_AI_SHARD_MATRIX_CSV` when the result should land in a stable artifact
path. This is the local NoKV/RustFS data-path matrix; keep mounted JuiceFS
comparisons in the L2 matrix below.

For the metadata HA control-plane gate, use:

```bash
scripts/run-metadata-ha-smoke.sh
```

That script starts temporary RustFS and etcd processes, starts owner A with
`--control-backend etcd` and sync shared-log archive enabled, publishes a
checkpoint, writes a post-checkpoint record, kills owner A, then starts owner B
with `--failover-from-epoch 1`. The pass condition is that owner B serves epoch
2, reads both checkpoint and replayed-log data, accepts a new write, and passes
`fsck`. The script re-reads the replayed data after the post-failover write to
catch allocator high-water or replay overwrite regressions. Set
`NOKV_HA_ETCD_ENDPOINTS` to point at an external etcd cluster instead of starting
a local one. Successful runs emit a machine-readable line:

```text
HA_SMOKE_METRICS {"lease_ttl_seconds":3,...,"failover_observed_ms":5350,...}
```

Set `NOKV_HA_METRICS_JSON=/path/to/metrics.json` when CI or benchmark scripts
need to archive the exact JSON. `failover_observed_ms` is measured from owner A
kill to owner B ready, so it includes the configured lease expiry wait. Treat it
as a smoke-gate RTO row, not a production SLA.

For the local stale-owner fence gate, run:

```bash
NOKV_HA_STALE_OWNER_CHAOS=1 scripts/run-metadata-ha-smoke.sh
```

This uses two metadata server binds. It pauses owner A with `SIGSTOP`, lets the
etcd lease expire, starts owner B at epoch 2, resumes owner A with `SIGCONT`,
and requires owner A to observe the new epoch and reject a stale write. The
metrics JSON adds `stale_owner_detect_after_resume_ms` and
`stale_owner_fence_after_detect_ms`. This is a local process-stall gate; keep
real multi-machine network partition tests separate.

For the experimental multi-shard correctness and failover gate, run:

```bash
scripts/run-multishard-fleet-smoke.sh
```

This local, multi-process smoke test assigns two path shards to independent
metadata server processes, routes both through one fleet client, replaces one
shard owner at the next epoch, restores it from checkpoint-image plus shared
log, and verifies that the unaffected shard remains available. It validates a
specific routing/recovery path; it is not an enterprise throughput result, a
multi-machine partition test, or evidence of consensus replication.

## The L2 benchmark framework (NoKV vs JuiceFS)

The headline comparison is a **boundary-labeled L2 (mounted FUSE) matrix**. Every
row carries the canonical label prefix
`boundary,system,metadata_tier,object_backend,cache_state,concurrency,tool,…`
(`bench/drivers/schema.py`), so the comparison is always **L2-vs-L2 under a
declared metadata tier and cache state** — never NoKV's faster L1 service path
against JuiceFS's mount. One entry point starts a disposable RustFS + Redis,
builds and mounts NoKV and JuiceFS, and runs the grid:

```bash
# quick: one NoKV tier (direct/WAL) + JuiceFS, concurrency 1
scripts/run-fs-benchmark.sh

# full matrix: NoKV local Holt tier + JuiceFS, concurrency sweep
NOKV_BENCH_PROFILE=standard scripts/run-fs-benchmark.sh --matrix \
  --repeats 3 \
  --result-csv "bench/results/$(hostname).raw.csv" \
  --aggregate-csv "bench/results/$(hostname).aggregate.csv" \
  --env-json "bench/results/$(hostname).env.json" \
  --decompose-csv "bench/results/$(hostname).decompose.csv"
```

It runs **two co-equal drivers** against each mount:

- `bench/drivers/posix_bench.py` — a self-contained, `juicefs bench`-shaped driver
  (`bigfile` / `smallfile` / `metadata`-tree, plus workload-specific
  `metadata_create_list` / `checkpoint` / `training_read` /
  `ai_shard_range_read`). Metadata/checkpoint/training-read product workloads
  run once at `p=1`; `ai_shard_range_read` and primitive workloads are swept
  across the selected concurrency levels.
  `metadata_create_list` emits separate `create` and `list` rows so create
  latency and directory-enumeration latency are not mixed. `training_read` and
  `ai_shard_range_read` emit **cold** rows (kernel page cache bypassed via
  `F_NOCACHE` / `posix_fadvise`) and **warm** rows, so cache effects are
  visible, not conflated. They can also emit a **hot** row: first warm the
  filesystem/client cache with a kernel-cache-bypassed pass, then measure with
  the same bypass mode. That keeps kernel page cache out of the timing while
  still making NoKV/JuiceFS client-cache behavior visible in NoKV `/stats`.
  NoKV hot rows with a stats URL also record FUSE-read coverage in
  `warmup_stats_coverage=[...]` and `measured_stats_coverage=[...]`; low
  coverage means the timed pass was mostly served above NoKV and should not be
  used as an object/data-plane hot-path claim.
  When cold rows are requested, their
  seed writes also opt out of cache residency after fsync so a write-populated
  page cache does not masquerade as a cold read. Set
  `NOKV_BENCH_CACHE_STATES=cold`, `warm`, or `hot` when a decompose sidecar
  should isolate one read cache state instead of wrapping multiple rows in one
  stats delta.
- `bench/drivers/real_tools.py` — the actual `fio`, `mdtest` (+`mpirun`), and
  `juicefs bench` binaries parsed into the same schema. Absent tools surface as
  explicit `tool-missing` rows, never silently dropped. Point at local binaries
  via `NOKV_BENCH_FIO_BIN` / `NOKV_BENCH_MDTEST_BIN` / `NOKV_JUICEFS_BIN`.

**Tier fairness:** the current NoKV row is the single local Holt metadata server
with S3-compatible object storage. It is the apples-to-apples local metadata
opponent for JuiceFS-on-Redis.

**Regression gate** — store a baseline and fail on drift:

```bash
scripts/run-fs-benchmark-baseline.sh
scripts/run-fs-benchmark.sh --matrix --repeats 3 \
  --aggregate-csv /tmp/nokv-run.aggregate.csv \
  --baseline "bench/baselines/$(hostname).aggregate.csv"
# or standalone:
python3 scripts/compare-baseline.py --baseline bench/baselines/host.aggregate.csv run.aggregate.csv
```

`scripts/run-fs-benchmark-baseline.sh` is the clean local baseline command. By
default it runs `standard` profile, `--matrix`, `p=1 4 16`, the NoKV local Holt
tier, and three repeats. It writes:

- `*.raw.csv` — every measured phase from every repeat.
- `*.aggregate.csv` — one row per comparison key, with canonical metric columns
  set to the median and extra `samples` / `*_p95` / `run_ids` / `env_ids`
  columns.
- `*.env.json` — host, git, third-party IOR/mdtest, RustFS, JuiceFS, Redis, fio,
  MPI, Rust, and run-parameter versions.
- `*.decompose.csv` — NoKV `/stats` before/after deltas for each measured phase.

The baseline gate should compare aggregate CSVs, not single-run raw CSVs. It
fails on metric regressions, baseline rows missing from the run, and blocking
caveats such as `workload-failed`, `parse-failed`, `tool-missing`, `fio-failed`,
`mdtest-failed`, or `juicefs-bench-failed`. For local exploratory runs, use
explicit allowances such as `--allow-missing-rows` or
`--allow-caveat-prefix tool-missing:`; CI should avoid those allowances unless
the missing coverage is intentional.

Native workload selection is split so product workloads and primitive workloads
cannot be mixed accidentally:

```bash
scripts/run-fs-benchmark.sh \
  --product-workloads metadata_create_list,training_read \
  --primitive-workloads smallfile,metadata

# Skip one side of the native driver:
scripts/run-fs-benchmark.sh --product-workloads none
scripts/run-fs-benchmark.sh --primitive-workloads none
```

For a focused AI data-plane L2 comparison, use:

```bash
scripts/run-ai-dataplane-l2.sh
```

This wrapper runs the mounted NoKV-vs-JuiceFS path with product workloads
`checkpoint,training_read,ai_shard_range_read`, skips `fio` / `mdtest` /
`juicefs bench`, and writes raw, aggregate, environment, and NoKV
stats-decomposition artifacts under `bench/results` by default. It is the
mounted user-boundary companion to `scripts/run-ai-shard-range-matrix.sh`: the
shard-range matrix is NoKV-only L1 service/object evidence, while this script is
L2-vs-L2 evidence against JuiceFS. Tune the packed-shard L2 shape with
`NOKV_AI_L2_RANGE_STRIDE` and `NOKV_AI_L2_RANGE_COALESCE_GAP_BYTES`. Use
`NOKV_AI_L2_CACHE_STATES=cold`, `warm`, or `hot` when investigating a
cache-state-specific object or cache counter; the default remains `cold,warm`.
Use a space-separated `NOKV_AI_L2_CONCURRENCY`, for example
`NOKV_AI_L2_CONCURRENCY="1 4"`, when measuring mounted packed-shard throughput
under multiple shard-reader workers. The checkpoint and ordinary training-read
rows remain p=1 latency rows; the packed-shard row is the product workload that
participates in the concurrency sweep.

For the seed-fsync/cache validation matrix, use:

```bash
scripts/run-ai-dataplane-l2-matrix.sh
```

This wrapper runs the same mounted L2 comparison but splits variants by
benchmark seed `fsync=0/1` and isolated `cold` / `warm` / `hot` cache states. It
delegates to `scripts/run-ai-dataplane-l2.sh` for each case, writes ordinary
`*.raw.csv`, `*.aggregate.csv`, `*.env.json`, and `*.decompose.csv` artifacts,
and also writes combined `matrix.raw.csv` / `matrix.aggregate.csv` files with
`matrix_case`, `fsync`, and `cache_state_scope` columns. Use it when comparing
NoKV's local-hot/NVMe behavior and seed-write flush effects because each NoKV
decompose sidecar covers one cache state instead of mixing cold and warm object
counters. This flag controls the benchmark driver's POSIX fsync of seeded data;
it does not switch the NoKV metadata tier from `nokv-direct-wal-async` to a
different metadata durability mode.

For a layered AI data-plane run across native SDK and mounted boundaries, use:

```bash
scripts/run-ai-dataplane-layered-matrix.sh
```

This wrapper runs Rust SDK L1, Python/fsspec L1, and optionally the mounted L2
NoKV-vs-JuiceFS matrix over the same packed-shard case names, then writes
`layered.raw.csv` and `layered.aggregate.csv`. The combined files add
`benchmark_layer`, `source_script`, and `layer_case` columns before the
canonical benchmark columns, so Rust SDK rows, Python/fsspec rows, NoKV FUSE
rows, and JuiceFS FUSE rows remain separable. Use
`NOKV_AI_LAYERED_RUN_L2=0` for a native-only SDK smoke when JuiceFS/Redis are
not available. The default cases are `sparse-exact` and `sparse-coalesced`;
`large-window` validates 1 MiB semantic windows on the native path and skips L2
because the mounted L2 profile owns sample size.

The companion `scripts/merge-layered-benchmark-csv.py` handles the CSV shape
gap between Rust `nokv-bench` extended L1 rows and the canonical Python/L2 rows.
It aggregates with layer/case dimensions in the key; do not feed the layered raw
CSV to the ordinary aggregate script if you need per-case results.

For a standard local engineering run over one sparse packed-shard case:

```bash
NOKV_AI_LAYERED_PROFILE=standard \
NOKV_AI_LAYERED_L2_CONCURRENCY="1 4" \
NOKV_AI_LAYERED_L2_CACHE_STATES="cold warm hot" \
NOKV_AI_LAYERED_L2_FSYNC=0 \
NOKV_AI_LAYERED_L2_REPEATS=2 \
scripts/run-ai-dataplane-layered-matrix.sh sparse-exact
```

Treat the result as a layered diagnostic: compare NoKV to JuiceFS only inside
`benchmark_layer=L2`, and use the L1 Rust/Python rows to estimate native SDK
headroom and Python binding overhead.

Common focused variants:

```bash
# smoke validation of one cold async case
NOKV_AI_L2_MATRIX_PROFILE=smoke \
NOKV_AI_L2_MATRIX_CONCURRENCY=1 \
NOKV_AI_L2_MATRIX_CACHE_STATES=cold \
NOKV_AI_L2_MATRIX_FSYNC=0 \
scripts/run-ai-dataplane-l2-matrix.sh

# standard p=1/p=4 cold+warm+hot, async+fsync rows
NOKV_AI_L2_MATRIX_PROFILE=standard \
NOKV_AI_L2_MATRIX_REPEATS=2 \
NOKV_AI_L2_MATRIX_CONCURRENCY="1 4" \
NOKV_AI_L2_MATRIX_CACHE_STATES="cold warm hot" \
scripts/run-ai-dataplane-l2-matrix.sh
```

NoKV object prefetch defaults to two background workers. Small exact-read
misses can warm the object block cache in 1 MiB offset-aligned segments with the
default 4 MiB object block size, so a decompose sidecar may show more prefetch
bytes while foreground object GET count falls. Initial small exact reads do not
enqueue that background warmup; the read handle must first prove a continued
stream or a small forward sparse stream on the same open shard. Large forward
jumps stay on the exact foreground path.
Covering-range cache hits use a structured range index for
`object_key:offset:len` entries, and exact cache hits use the primary cache key
before falling back to the range index, so dense shard-sample readers do not
scan cache keys by string prefix on every hit. Read-plan covering hits also
materialize exact sparse-window plans, so a full-file plan published during
writeback can seed repeated AI sample windows without rescanning the covering
plan on every measured read. The mounted FUSE client keeps a `128K` read-plan
budget split across `16` shards to preserve hot epoch windows under p4 sparse
training reads. Each open read handle also keeps a small local read-plan cache;
for shard-packed datasets this lets repeated `pread` windows on the same shard
slice the already-published full-file plan without contending on the shared
backend cache. Treat p4 `hot` rows as stable only after repeated gates show
consistent read-plan/cache-hit behavior in the NoKV decompose sidecar; a single
best five-repeat median is not enough. For `ai_shard_range_read` hot rows, the
canonical `cost_breakdown` records both measured `physical_ranges` /
`physical_read_bytes` and warm-up `warmup_physical_ranges` /
`warmup_physical_read_bytes`; mismatches there mean the benchmark did not warm
the same physical windows it measured. NoKV rows also receive the mount stats
URL and append `warmup_stats=[...]` / `measured_stats=[...]` when the endpoint
is available; those fields are diagnostic and are not expected on JuiceFS rows.
They also append `warmup_stats_coverage=[observed_fuse_read_requests=...
expected_fuse_read_requests=... coverage=...]` and the matching
`measured_stats_coverage=[...]` field so a repeated hot median can be separated
from a kernel-cache-served row.
The NoKV mount stats include `fuse_read_requests` /
`fuse_read_request_bytes`, which count requests that reached the FUSE read
handler, separately from object pipeline counters such as `object_gets`,
`cache_hits`, and `block_cache_hits`. On macOS hot rows can therefore show more
benchmark-planned pread windows than FUSE/object-pipeline reads because the
kernel page cache may satisfy part of the measured pass before NoKV sees it.
Use `NOKV_BENCH_NOKV_MOUNT_OPTIONS="--direct-io --no-kernel-cache"` for a
NoKV-only diagnostic run when the claim is about object hot-path overhead rather
than end-to-end mounted cache behavior. Add `--no-prefetch` to that diagnostic
when isolating sparse hot reads that should not benefit from sequential
read-ahead. The default memory block cache is sharded by object key for
training-size cache budgets. It keeps typed exact-range and alias indexes keyed
by `(object_key, offset, len)` so common 16KiB shard-sample hits do not rebuild
the `object_key:offset:len` string or scan the covering-range index before
copying cached bytes. Covering range hits can install small exact-range aliases
that point at the already cached segment without copying the 16KiB sample bytes
again; this keeps repeated sparse hot reads off the covering-range index while
avoiding the cold-path cost of duplicating exact sample entries. Mounted FUSE
read handles also slice repeated sparse windows directly from a cached full-file
read plan instead of inserting one local read-plan entry per 16KiB sample
window. Do not infer a throughput win from either cache index alone unless a
direct-io run shows the same `fuse_read_requests`, `block_cache_hits`, and
`read_plan_cache_hits` before and after the change.
On macOS, `--direct-io` also passes macFUSE `direct_io,iosize=1048576`, but
large coalesced preads can still arrive as many 16 KiB FUSE reads. If the NoKV
row reports far more `fuse_read_requests` than planned `physical_ranges`, treat
that run as a FUSE direct-io limit study, not as proof that the object cache
prefers small ranges. For AI-training throughput work, prefer the
SDK/fsspec/native batched range path when the workload can issue semantic range
batches instead of POSIX preads. The Rust SDK path is
`NoKvFsClient::read_path_ranges_batch`, which batch-opens all shard range
windows before object reads; Python/fsspec should bind to that primitive rather
than rebuilding range planning above POSIX. The initial binding lives in
`crates/nokv-python` as `nokv._native.Client.read_ranges_batch` and
`nokv.fsspec.NoKVFileSystem.read_ranges_batch`. For dataloaders that can reuse
their own contiguous CPU staging buffer, the binding also exposes
`read_ranges_batch_into`; that path avoids returning one Python `bytes` object
per sample or per shard. `read_ranges_batch_buffer` uses a Rust-owned
`ReadBuffer` so the SDK read can run with the Python GIL released; use
`--read-shape buffer` to measure that path. The current buffer is behind an
explicit staging-memory boundary and supports `--read-buffer-memory-kind
system|page_locked` when `--read-shape buffer` is selected. `page_locked` is
Unix `mlock` host memory, not CUDA/RDMA registration. The result row records
`read_buffer_memory_kind=...` in both shape and cost breakdown. Use
`--read-shape planned_buffer` when the dataloader can reuse a Rust-side
`RangeBatchPlan`; the benchmark prepares normalized requests, packed output
layout, coalesced read windows, and ordered metadata batch-open requests outside
the timed read pass and records `range_batch_plan=true`. This still does not
cache metadata read plans, object layout, or generations; every timed read
batch-opens metadata again. It isolates static batch/window planning overhead
from the repeated training read loop, and the SDK executor borrows that static
layout while filling the staging buffer. Use `--read-shape batch_reader` to
measure the `RangeBatchReader` lifecycle shape, where the prepared plan and
NoKV-owned staging buffer are created before the timed pass and each measured
batch calls `reader.read()`. That shape is closest to a long-lived PyTorch
DataLoader worker. Use `--read-shape epoch_reader` to prepare all path-batch
readers once and cycle through them with a resettable epoch iterator. The
benchmark uses `RangeBatchEpochReader.read_all()` so one timed Python call fills
all prepared batch buffers through persistent bounded native worker threads; the row adds
`range_batch_epoch=true`, `range_batch_epoch_read_all=true`,
`range_batch_epoch_parallel=true`,
`range_batch_epoch_persistent_workers=true`, and
`range_batch_epoch_parallelism=...` to separate the worker-epoch lifecycle from
a single batch reader. Its p50/p99 fields are full-epoch call latencies, not
per-batch latencies. Both rows should still be interpreted as L1 Python/fsspec
rows rather than mounted filesystem rows.
For every Python/fsspec read shape, the cost breakdown also records
`native_read_us=...` and `python_consume_us=...`. `native_read_us` is the timed
SDK/fsspec read call surface, while `python_consume_us` is the benchmark's
post-read layout validation and checksum loop over returned or staged bytes.
The canonical `seconds` and throughput still include both; use the split to
diagnose whether the next optimization belongs in the read path or in the
Python dataloader consumption boundary.
`ReadBuffer.export()` returns a read-only `ReadBufferView` token and blocks
clear/refill while the view is alive, which is the safe staging contract under
the current `abi3-py39` extension build. It is not a PEP 3118 memoryview yet;
do not count it as Python zero-copy until the package grows a Python 3.11+ or
non-abi3 memoryview feature. Exact single-range windows write through the
object executor directly into the staging buffer. Coalesced windows that
contain multiple semantic ranges use guarded scatter direct-write only when
scatter does not increase the physical block plan; gap-coalesced windows keep
the internal window buffer on cold paths to preserve object GET count. Once a
gap-coalesced window is hot in the local block cache, the client can scatter
semantic ranges directly from cache into the caller buffer.
Override worker count for a focused run with
`NOKV_BENCH_NOKV_MOUNT_OPTIONS="--prefetch-workers N"` and compare throughput,
p99, foreground object GET bytes, and prefetch completion bytes before treating
a worker-count change as a real improvement. More workers can reduce foreground
object GETs while worsening tail latency through object-store and FUSE
scheduling pressure.

**Latency decomposition** is part of the main entry point for NoKV rows. Pass
`--decompose` or `--decompose-csv PATH`; the runner snapshots the NoKV
`/stats` endpoint before and after each measured phase and writes a sidecar CSV
with metadata commit, object writeback, object GET/PUT, and
read-plan-cache deltas. Read-path rows also expose object prefetch enqueue,
drop, completion, failure, cache-hit, and cache-hit-byte deltas, which helps
separate useful block warmup from accidental read-ahead amplification. The
canonical result CSV stays comparable across NoKV and JuiceFS; the NoKV-only
decomposition lives in the sidecar.

Orchestration is shared in `scripts/lib/fs-bench-common.sh` (RustFS / Redis /
server / mount lifecycle, cache-state mount flags, OS page-cache drop).
Configuration is via `NOKV_BENCH_*` env vars documented in that library; required
infra tools (rustfs/redis/juicefs/aws/python) must be present.

### L1 service-path reference (not the headline)

To measure NoKV's **L1 service path** (the SDK/client against the framed metadata
RPC) end to end, use `scripts/run-rustfs-e2e.sh`, which starts a disposable RustFS
endpoint and runs `nokv-bench`. Those rows are labeled `boundary=L1`; read them as
the engine/service ceiling, not as an L2 filesystem comparison — comparing them to
JuiceFS's mount would mix boundaries, which is exactly what the L2 matrix above
exists to prevent. The L2 matrix is the product-surface measurement.

To validate and time the Python/fsspec binding over the same native service
path, run `scripts/run-python-fsspec-smoke.sh`. It builds the PyO3 package with
maturin, starts RustFS and `nokv serve`, writes a packed shard dataset through
the CLI, and checks semantic sample ranges through
`nokv.fsspec.NoKVFileSystem.read_ranges_batch`. It then runs
`bench/drivers/native_fsspec_bench.py` and emits canonical `boundary=L1` rows
for the Python SDK/fsspec path. Tune the generated packed dataset with
`NOKV_PYTHON_SMOKE_SHARD_COUNT`, `NOKV_PYTHON_SMOKE_FILES_PER_DIR`,
`NOKV_PYTHON_SMOKE_SAMPLE_BYTES`, `NOKV_PYTHON_SMOKE_RANGE_STRIDE`,
`NOKV_PYTHON_SMOKE_CACHE_STATES`, and `NOKV_PYTHON_SMOKE_CONCURRENCY`; set
`NOKV_PYTHON_SMOKE_READ_SHAPE=ranges|packed|into|buffer|planned_buffer|batch_reader|epoch_reader` to
compare Python return shapes, `NOKV_PYTHON_SMOKE_READ_BUFFER_MEMORY_KIND` to
choose `system` or `page_locked` for buffer shapes, and set
`NOKV_PYTHON_SMOKE_RESULT_CSV` to archive the raw CSV. The
Python binding also exposes `NoKVFileSystem.stats()`, and the native fsspec
driver records
`warmup_stats=[...]` / `measured_stats=[...]` deltas for object GETs, cache
hits, prefetch, read-plan cache, and data-fabric counters. These rows measure
the native NoKV Python client boundary. They are useful for finding FUSE
overhead and Python binding overhead, but they are not a direct JuiceFS mount
comparison.

These scripts produce local engineering evidence only. They are not MLCommons
official submission results and do not replace running the official MLPerf
Storage or DLIO harness against a NoKV FUSE mount. To capture an official
harness run reproducibly, mount NoKV first and wrap the official command:

```bash
scripts/run-official-training-benchmark.sh \
  --harness dlio \
  --mount /mnt/nokv \
  --result-dir bench/results/dlio-nokv \
  -- /path/to/official/dlio/command --data-dir /mnt/nokv
```

The wrapper records stdout/stderr/status, the exact command, mount path, and the
same environment JSON used by the local matrix. The score still comes from the
official harness, not from NoKV's generated local workload.

The gate also accepts the special workload `fuse-smoke` for the mounted POSIX
semantics check. It is not part of the default list because it requires a
working local FUSE installation and mount permissions:

```bash
scripts/run-ai-training-smoke.sh fuse-smoke
NOKV_AI_SMOKE_INCLUDE_FUSE=1 scripts/run-ai-training-smoke.sh
```

For a disposable local RustFS-backed FUSE semantics smoke, use:

```bash
scripts/run-fuse-smoke.sh
```

The script builds `nokv`, starts RustFS, mounts a temporary NoKV FUSE
filesystem, and exercises mkdir, file write/read, file fsync, directory fsync,
rename, readdir, truncate, symlink/readlink, xattr roundtrip, access(2),
hardlink, statfs, lseek, fallocate, copy_file_range, rm, and rmdir through the
mounted filesystem. This is a correctness smoke, not a performance benchmark.

For JuiceFS-style mounted-filesystem POSIX gates, mount NoKV first and run the
external-suite driver against that mountpoint:

```bash
NOKV_FUSE_MOUNT=/mnt/nokv \
scripts/run-fuse-posix-gate.sh
```

The default POSIX gate runs a small namespace/data smoke plus xattr roundtrip.
Heavier suites are opt-in because they require external tools and will expose
remaining POSIX hardening gaps until NoKV reaches the full external-suite gate:

```bash
NOKV_FUSE_MOUNT=/mnt/nokv \
NOKV_FUSE_POSIX_TESTS="basic xattr pjdfstest ltp" \
scripts/run-fuse-posix-gate.sh
```

This mirrors the way mature FUSE filesystems such as JuiceFS validate the
mounted boundary: `pjdfstest` for syscall-level POSIX behavior, LTP filesystem
groups for kernel-facing semantics, randomized filesystem operation tests for
state-space coverage, and separate performance runs for mdtest/fio/object-store
paths. NoKV keeps the default CI smoke smaller, then uses these external gates
when claiming POSIX conformance.

The harness prints CSV. The exact column set is owned by
`bench/src/bin/nokv-bench.rs`; the important column families are:

```text
workload, profile, throughput, object stats, metadata store stats,
data_fabric stats, tiered_object stats, path_index stats, ReadDirPlus projection
stats, caveat
```

Most benchmark workloads start a real single-node `nokv serve` metadata service and run the
Rust service client against its framed metadata RPC. Object bytes are still read
and written directly by the client against the configured S3-compatible object
store. This keeps benchmark numbers attached to the deployable service boundary
instead of an in-process metadata shortcut.

Metadata smoke workloads use the SDK's ordered non-atomic `create_files`
batching for file create bursts. This measures the deployable SDK/server path
without charging one network round trip per independent file create; each
subrequest still has its own success or error result.

`metadata_atomic_applies`, `metadata_atomic_apply_commands`, and
`metadata_atomic_apply_max_batch` report how command batching reaches the Holt
atomic apply boundary. They are the key columns for checking whether request
coalescing reduced storage-engine apply calls rather than only coalescing
requests above the metadata engine.

Object-backed workloads can be scaled without editing code:

```bash
cargo run --release -p nokv-bench --bin nokv-bench -- \
  --profile standard \
  --workload mlperf-dlio \
  --object-backend rustfs \
  --object-concurrency 8 \
  --checkpoint-bytes 1048576 \
  --sample-bytes 65536 \
  --read-repeats 2 \
  --block-cache on
```

`--object-concurrency` controls parallel object publish/read work inside the
benchmark. `checkpoint-publish` stages checkpoint objects in parallel, then
keeps the final `rename_replace` sequence ordered so the latest-checkpoint
semantics stay clear. `--read-repeats` intentionally rereads the same sample per
directory; use it with `--block-cache on|off` to separate object-store latency
from cache reuse.

`native-layout-read` is the L1 data-fabric baseline. It uses the SDK
`read_path` layout-open path and emits two rows for the same seeded objects:
`phase=cold` and `phase=warm`. By default this workload creates a local hot tier
under the benchmark root, writes objects to the cold S3-compatible backend, then
lets full-object layout reads populate the hot tier.

`ai-dataset-batch-read` is the SDK dataset-batch path. It seeds sample files and
reads each batch through bounded metadata layout-open batches plus parallel
object reads for distinct files. Large training batches are chunked at the SDK
metadata boundary instead of becoming one oversized RPC. It emits `phase=cold`
and `phase=warm`, and defaults to a local hot tier so the warm row can show
local-NVMe hits without extra flags.

`ai-shard-range-read` is the shard-packed dataset path. It stores many samples in
each shard file, keeps sample offsets in the benchmark request table, and reads
each shard through SDK coalesced range reads. Adjacent or overlapping sample
ranges share one layout-open/object-read window, so this measures the packed
dataset path rather than repeated per-sample path reads. It also emits cold/warm
rows and defaults to the same local hot tier.

For multiple sparse windows in one shard, the SDK pins the first opened
generation and can prefetch the next admitted window through the existing body
read-plan, block-cache, and object-prefetch path. Small windows below
`DEFAULT_BLOCK_SIZE / 4` stay on the foreground path to avoid speculative
overhead. Foreground windows still use path opens with the pinned expected
generation, so stale shard replacements remain fenced by metadata.

For sparse sample readers, `--range-stride N` reads every Nth sample from each
shard and `--range-coalesce-gap-bytes N` lets the SDK merge reads separated by
at most N bytes. This exposes the object-store tradeoff directly: fewer GETs at
the cost of reading small gaps between selected samples.

Use `--sample-bytes 1048576 --range-stride 32 --range-coalesce-gap-bytes 0`
with the smoke profile to validate the admitted large-window read-ahead path.
That shape reads two 1 MiB windows per shard and should show non-zero
`prefetch_enqueued` without adding a product tuning knob.

The `data_fabric_*` columns show planned blocks, local hot hits, cold object
fallbacks, coalesced ranges, and cache hits. Use `--hot-object-root PATH` to
force a local hot tier for other object-backed workloads as well.
`--hot-object-max-bytes N` caps the local hot tier and makes LRU eviction
visible in `local_hot_evictions`;
`--hot-fill-mode inline|background` selects whether cold-read hot fills happen
before the read returns or in a coalesced background worker.
When a row uses a local hot tier, the `object_backend` label includes the write
durability policy, for example `rustfs+local-hot+put=cold-only` for read
workloads that seed durable cold objects before measuring hot reads,
`rustfs+local-hot+put=cold-then-hot` for production-style tiered writes that
acknowledge after the cold object store, and
`rustfs+local-hot+put=hot-background` for cache-first write workloads that return
after local hot staging and enqueue the cold put in the background.

For tiered object backends, the `tiered_*` columns report the object's real
hot/cold backend activity: hot probes and hits, cold gets and bytes, hot fills,
background fill enqueue/coalesce counts, and hot-tier maintenance errors.
`local_hot_*` columns report the hot tier's end-of-window residency and
capacity-pressure deltas. `object_gets` remains the object-layer `get_many` call
count, so use `tiered_cold_gets` and `tiered_cold_get_bytes` when checking
whether a warm `native-layout-read` row avoided the cold S3-compatible backend.

## Workloads

`mdtest-easy` creates files across many directories. File bodies are
metadata-only, so this isolates namespace create cost.

`mdtest-hard` creates many files in one shared directory. This stresses the hot
directory dentry prefix and Holt current tree.

`metadata-negative-lookup` creates present files, then times missing-path
lookups in the same directory shape. It verifies that live misses do not fall
back into history scans.

`artifact-index-lookup` seeds artifact paths and times indexed `stat_path`
lookups plus indexed directory listing. It measures the path-index fast path
without object reads.

`metadata-concurrent-read` uses the same artifact-index shape but runs
`stat_path` and indexed list operations through parallel workers after warmup.
Use it to measure metadata read concurrency, cache contention, and framed RPC
worker behavior.

`metadata-durability-batch` times batch file creation across many shard
directories and emits two rows over the same shape: `local-only` and
`sync-shared-log`. The sync row uses a controlled single Holt owner with an
in-memory control store, grouped shared-log segment archive, and `LogRef`
publish before ACK. It is an L1 durability-mode workload, not an etcd quorum or
L2 JuiceFS comparison. Use `metadata_log_segments_archived`,
`metadata_log_entries_archived`, and `metadata_log_archive_bytes` to inspect
shared-log grouping; ordinary `object_puts` remains the file-object data path
counter.

`checkpoint-publish` writes checkpoint bodies to the configured
S3-compatible object backend, then atomically promotes staged files with
`rename_replace`. This measures object put plus metadata publish, not metadata
alone.

`training-read` seeds a dataset tree, then times directory listing plus one
sample read per shard. The reported time excludes seed time and represents a
warm object read path through the configured backend.

`ai_shard_range_read` is the mounted L2 packed-shard reader. It writes shard
files through the mount, then uses POSIX `pread` windows at sample offsets so
NoKV FUSE and JuiceFS FUSE see the same user-boundary request stream. The row's
`operations` and throughput are logical selected samples and bytes; the
`cost_breakdown` records physical POSIX read windows, physical bytes, and read
amplification. `NOKV_BENCH_RANGE_STRIDE` selects every Nth sample, and
`NOKV_BENCH_RANGE_COALESCE_GAP_BYTES` lets the driver merge adjacent sparse
windows before issuing `pread`. With `--concurrency N`, shard files are read by
up to N worker threads and the shape records `shard_read_workers=N`.

`native-layout-read` seeds one checkpoint-sized object and one shuffled sample
per shard, then reads them through the layout-open SDK path. The cold row should
show object fallbacks; the warm row should show local hot hits when the hot tier
is enabled.

`ai-dataset-batch-read` seeds a dataset-shaped tree and reads sample batches
through the SDK batch layout-open path with bounded metadata batch RPCs and
parallel object reads for distinct files. Use it to measure layout-open
overhead, local hot-tier fill/hit behavior, and future dataset-prefetch changes.

`ai-shard-range-read` seeds shard files and reads many sample offset ranges from
each shard through SDK batch coalesced range reads and one-window read-ahead.
The timed phase groups shards by `--object-concurrency`, calls
`NoKvFsClient::read_path_ranges_batch`, and records `range_batch_open=true` in
the shape. Use it to measure shard packing overhead, metadata batch-open
amortization, object prefetch behavior, and local hot-tier reuse for AI
training-style sample readers.

Use `--range-stride 2 --range-coalesce-gap-bytes 512` with the smoke profile to
measure a sparse packed-shard reader that merges across one 512-byte skipped
sample.

Use `--sample-bytes 1048576 --range-stride 32 --range-coalesce-gap-bytes 0`
to validate read-ahead admission on MB-scale shard windows. This is a policy
smoke, not a full throughput limit test.

`mlperf-dlio` uses deterministic generated data in an MLPerf Storage/DLIO-style
shape: dataset seed, timed training reads, and checkpoint writes with atomic
latest-checkpoint replacement. It is an official-style local gate, not an
MLCommons submission result.

`demo-dataset` uses a small public-dataset-shaped class/sample directory tree
without downloading external data. It is meant for demos and CLI validation, not
for CI performance claims.

## Profiles

| Profile | Use |
| --- | --- |
| `smoke` | Fast correctness and shape check: 4 KiB checkpoints and 512 B samples. |
| `standard` | Local performance baseline: 1 MiB checkpoints and 16 KiB samples. |
| `long` | Larger local stress run: 8 MiB checkpoints and 256 KiB samples. |

## JuiceFS-Style Evidence Ladder

JuiceFS is a useful reference because its tests are layered rather than a
single benchmark number:

| Layer | JuiceFS practice | NoKV equivalent |
| --- | --- | --- |
| Quick mounted smoke | `juicefs bench` and targeted mount checks | `scripts/run-fuse-smoke.sh` |
| POSIX gate | `pjdfstest`, LTP fs/fsx/io/fcntl groups, xattr tests | `scripts/run-fuse-posix-gate.sh` with `pjdfstest ltp xattr` |
| Random operation stress | Hypothesis fs state machines and long random-test runs with create/read/ls/delete/rename/link/truncate/walk mixes | Planned NoKV mounted random-operation gate; current coverage is unit/contract tests plus FUSE smoke |
| Metadata regression | mdtest scenarios across metadata engines, current-vs-previous comparison with tolerance | `mdtest-easy`, `mdtest-hard`, L2 `mdtest`, and `compare-baseline.py` over aggregate CSV |
| Metadata HA smoke | lease/session failover, checkpoint restore, logical log replay | `scripts/run-metadata-ha-smoke.sh` over RustFS plus etcd |
| Data path regression | fio and object-store bench paths | `checkpoint-publish`, `training-read`, `mlperf-dlio`, and future object microbench expansion |
| Long-run stress | vdbench and scheduled workflows | Planned long profile plus object-store soak |

Use this ladder when interpreting results. A NoKV metadata microbench can prove
a Holt hot path improved; it cannot by itself prove POSIX correctness,
object-store behavior, or training-cluster readiness.

## Current Caveats

Performance workloads currently run a single-node `nokv serve` metadata service with a
configured S3-compatible object backend. The local multi-process
`run-multishard-fleet-smoke.sh` adds correctness coverage for path routing,
per-shard owner replacement, epoch fencing, checkpoint/log recovery, and
unaffected-shard continuity; it does not add a multi-node throughput result.

Enterprise multi-machine small-file throughput, network-partition chaos,
online resharding, and sustained mixed-workload operation remain outside the
published benchmark surface. Sharding is horizontal partitioning with one
active writer per shard, not consensus replication, and atomic publication is
currently shard-local rather than a cross-shard transaction.

The harness still does not include FUSE kernel caching, Python DataLoader
overhead, object-store multipart upload, or a multi-machine training cluster.
Treat metadata-only numbers as a metadata-service baseline, and object-backed
numbers as specific to the configured endpoint. Cluster claims must report the
metadata topology, network, object-store, cache, and durability settings.
