# Yanex Agent Interface Benchmark

This directory contains the experimental Yanex agent-interface benchmark
harness. It compares how different agent-facing surfaces affect correctness,
token cost, tool calls, and bytes read over the same fixed Yanex experiment
corpus.

This is not a storage-engine throughput benchmark. The product benchmark for
metadata, training-read, and checkpoint workloads lives in the root `bench/`
crate and is documented in `docs/benchmarks.md`.

## Current Status

The measured NoKV arm is the dedicated `nokv-agent` crate, not the DFS
metadata service. `nokv-agent` owns an agent-native, fs-shaped Holt store and
exposes `ls`, `stat`, `catalog`, `read`, `aggregate`, `find`, and `grep` over
its own records.

Native grep matches a case-insensitive literal substring and returns matching
lines with line numbers, so log-extraction tasks resolve body facts without
full file reads. The raw `nokv-*` commands in this harness still exist for DFS
namespace debugging and verification, but they are not the measured NoKV arm.

The default Phase 1 API surface is `openai_agents_responses_schema_once`. The
Rust harness still owns batch planning, local judging, telemetry JSONL, and
NoKV/SQLite tool execution. A Python runner uses `openai-agents>=0.7.0,<0.8.0`
for the model/tool loop and a custom Responses model wrapper. Continuation
requests keep the same tool schemas available so the model can make additional
tool calls after observing tool outputs. Tool calls are
posted back to a per-run loopback bridge in the Rust harness.

## Corpus

The benchmark uses a fixed Yanex experiment tracking corpus. The materialized
state includes:

- experiment metadata;
- params;
- metrics;
- dependencies;
- artifacts;
- git state;
- generated read-only index files.

Default local data root:

```text
bench/data/yanex-demo
```

Expected local layout after preparation:

```text
bench/data/yanex-demo/
  corpus/
  sqlite/yanex.db
  nokv/meta
  nokv-agent/holt
  rustfs/
  manifest.json
  results/
```

The local corpus archive path is intentionally not committed. Pass it with
`--archive` when preparing data.

## Arms

The Phase 1 harness compares two read-only arms:

| Arm | Surface |
| --- | --- |
| `sqlite_raw_v1` | Raw SQLite schema/query/blob tools (including line-oriented `grep_blob`) plus ETL-maintained agent-index materialization tables. |
| `nokv_native_v1` | `nokv-agent` agent-native fs-shaped Holt store with `ls`, `stat`, `catalog`, `read`, `aggregate`, `find`, and `grep`. |

The fixed Phase 1 tasks live in `tasks/phase1_readonly.yaml`. The rubric
lives in `rubric/phase1_readonly.yaml`.

## Phase 1 Task Shape

The active read-only workload is a five-task researcher-shaped set:

| Task | Shape |
| --- | --- |
| `train_top_configs_report` | Report the 5 best completed train.py runs by minimum finite val_loss with learning rate, batch size, stdout size, and git dirty state. |
| `eval_fidelity_leaderboard` | Report the 5 completed eval.py runs with the highest latest fidelity plus related utility, detection, and privacy metrics and stderr size. |
| `tabdiff_ddxplus_dcr_checkpoint_provenance` | For every ddxplus_dcr TabDiff sampling run, extract the loaded checkpoint file and model parameter count from the sampler stdout log. |
| `best_detection_eval_method_audit` | Find the completed eval.py run with the highest latest detection_roc_auc and extract the detection method name from its eval log. |
| `cancelled_train_interrupt_triage` | For every non-completed run, report stderr size and the stderr line number of the last KeyboardInterrupt occurrence. |

The two structured report tasks are judged against gold SQL. The three
log-extraction tasks are judged against harness-side file-body oracles;
oracles are judge-side data and are never exposed to either arm.

## Valid Comparisons

The benchmark has one core A/B comparison:

- Raw SQLite tools vs NoKV Agent Native.

## Materialized Index Fairness

Precomputed indexes are a normal experiment-tracking system behavior and are
part of the intended benchmark model. They make the benchmark closer to a real
agent workload, where agents inspect catalogs and facets instead of scanning
all raw blobs for every question.

The fairness rule is that every valid A/B comparison must expose logically
equal introspection over the benchmarked facts. The syntax and access pattern
can differ by surface, but the visible catalog fields, index facts, and
limitations must not give one arm hidden task answers that the paired arm
cannot discover through its own public interface.

## NoKV Agent Native Definition

In this benchmark, "NoKV native" means that the agent-facing tools map to
`nokv-agent` APIs over an agent-native Holt store. The harness may adapt OpenAI
tool calls and enforce limits, but it must not own the measured index or body
lookup semantics.

Target native behavior:

- `ls`/`stat` return typed agent-native directory/file cards, not flat file
  entries.
- `entry_count`, `record_count`, `schema`, `sample`, compact body descriptors,
  catalog fields, and indexed values are available through `stat`.
- `stat` cards avoid evidence, snapshot, generation, and storage-internal body
  fields in the agent-visible payload.
- `read` defaults to structured pagination; raw bytes require explicit
  `format = "bytes"`.
- `find(path, predicates, sort, fields, facets, cursor, limit)` searches paths
  and projects indexed field values; body/schema/sample inspection uses `stat`
  or `read`.
- `find` uses a constrained predicate grammar declared by stat/catalog cards.
- `catalog(path, field_prefix, include_facets)` provides compact field
  discovery without requiring a full stat card; facets are opt-in.
- `aggregate(path, predicates, group_by, measures, sort, limit)` provides
  compact summaries over indexed agent records.
- paginated results include truncation state and `next_cursor` when more
  results exist.

Generated `/index/*.json` files must not become hidden answer files. They can
support facet-count tasks, but they are not a substitute for product-level
typed agent indexes or future derived metric/set-pipeline APIs.

## Prepare Data

Start RustFS:

```bash
./bench/agent-interface/scripts/start_rustfs.sh
```

Always prepare from a clean slate with `--reset` before measuring; `verify`
checks that the agent-visible NoKV catalog matches the registered index
fields and fails on stale fields from older encoders.

Prepare the fixed corpus:

```bash
cargo run -p nokv-bench --bin yanex-agent-bench -- prepare \
  --archive /path/to/yanex-experiment-metadata-origami-data-gen-project.tar.gz \
  --data-root bench/data/yanex-demo \
  --reset
```

Verify materialization:

```bash
cargo run -p nokv-bench --bin yanex-agent-bench -- verify \
  --data-root bench/data/yanex-demo
```

## Run Phase 1

Set OpenAI credentials and model:

```bash
export OPENAI_API_KEY=...
export OPENAI_MODEL=gpt-5.5
```

Run one task for one arm:

```bash
./bench/agent-interface/scripts/run_phase1_batch.sh \
  --arm nokv_native_v1 \
  --task-id cancelled_train_interrupt_triage \
  --repeats 1 \
  --output-jsonl bench/data/yanex-demo/results/phase1.jsonl
```

Install runner dependencies in the local benchmark virtual environment. The
wrapper automatically uses this environment when `PYTHON` is not set:

```bash
python3.12 -m venv bench/agent-interface/.venv
bench/agent-interface/.venv/bin/python -m pip install -r bench/agent-interface/agents_runner/requirements.txt
```

Set `YANEX_BENCH_AGENT_SDK_LIVE_PROBE=1` to make the Agent SDK runner perform a
real schema-once Responses continuation probe before a run. Probe failure is a
run failure.

Run all fixed Phase 1 tasks for all arms:

```bash
./bench/agent-interface/scripts/run_phase1_batch.sh \
  --repeats 10 \
  --output-jsonl bench/data/yanex-demo/results/phase1.jsonl
```

## Direct Tool Checks

Inspect the tool registry:

```bash
cargo run -p nokv-bench --bin yanex-agent-bench -- tools \
  --arm nokv_native_v1
```

Inspect a NoKV raw namespace path:

```bash
cargo run -p nokv-bench --bin yanex-agent-bench -- nokv-stat \
  --data-root bench/data/yanex-demo \
  --path /yanex/runs/00023013/metadata.json
```

The `nokv-*` direct commands above remain raw debugging commands. The
`nokv_native_v1` benchmark arm reads the prepared `nokv-agent/holt` store.

Inspect SQLite schema:

```bash
cargo run -p nokv-bench --bin yanex-agent-bench -- sqlite-show-schema \
  --db bench/data/yanex-demo/sqlite/yanex.db
```

## Cost Accounting

Set pricing environment variables before a batch to record per-run USD cost
in the telemetry (`all_in_cost_usd`):

```bash
export OPENAI_INPUT_USD_PER_1M_TOKENS=0.75
export OPENAI_CACHED_INPUT_USD_PER_1M_TOKENS=0.075
export OPENAI_OUTPUT_USD_PER_1M_TOKENS=4.50
```

Cached prompt tokens are billed at the cached rate; uncached prompt tokens at
the input rate; completion tokens at the output rate.

## Methodology Notes

- Run at least 5 repeats per arm/task pair before quoting numbers; small
  models show high per-run variance on compound tasks.
- Judge-side gold (gold SQL or file-body oracles) is never exposed to either
  arm.
- Do not implement one-sided semantics in the harness as benchmark-only
  shortcuts. The harness adapter should stay thin over `nokv-agent`, and the
  raw SQLite arm keeps line-oriented `grep_blob` parity for body search.
- The published benchmark report lives at `bench/agent-interface/BENCHMARK_REPORT.md`;
  its raw telemetry is committed under `bench/agent-interface/results/`.
