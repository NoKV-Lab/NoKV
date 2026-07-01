# Agent Interface Benchmark Report: Namespace Surface vs Raw SQL

**NoKV agent-interface benchmark — final results, 2026-06-10**

## Executive Summary

We benchmarked two agent-facing interfaces over the same fixed ML-experiment
corpus (875 Yanex runs: metadata, params, 806k metric rows, artifacts, git
state, and raw stdout/stderr logs):

- `sqlite_raw_v1` — a raw SQLite surface: live schema discovery, bounded
  read-only SQL, byte-range blob reads, and a line-oriented `grep_blob`.
- `nokv_native_v1` — the NoKV product-native namespace surface: `ls`, `stat`,
  `catalog`, `find`, `aggregate`, `read`, and namespace-recursive `grep`.

The agent model is `gpt-5.4-mini`, 10 repeats per arm/task pair
(2 batches x 5 repeats, 100 runs total), identical prompts, judged against
deterministic gold facts that neither arm can see. Every run is a fully
stateless episode: the agent starts from a cleared context each time and
carries nothing over from previous tasks or repeats.

| Set mean (per 5-task pass) | Raw SQLite | NoKV namespace | SQLite / NoKV |
| --- | --- | --- | --- |
| Tasks solved correctly | 4.40 / 5 | **4.50 / 5** | — |
| Prompt tokens (incl. cached) | 151,572 | **82,827** | **1.83x** |
| Total tokens (incl. completion) | 156,098 | **87,418** | 1.79x |
| Cost-weighted tokens | 94,356 | **57,748** | 1.63x |
| Cost (USD) | $0.0708 | **$0.0433** | **1.63x** |

On the three compound exploration tasks — the workloads agents actually
struggle with — the gap widens:

| Compound tasks (T1+T3+T5) | Raw SQLite | NoKV namespace | SQLite / NoKV |
| --- | --- | --- | --- |
| Prompt tokens | 127,450 | **53,300** | **2.39x** |
| Cost (USD) | $0.0558 | **$0.0286** | **1.95x** |
| Mean correctness | 83.3% | **86.7%** | — |

## Cost Model

- Prices are gpt-5.4-mini list rates: input $0.75/M, cached input $0.075/M
  (90% caching discount), output $4.50/M.
- Cost-weighted tokens = `uncached + 0.1 x cached + 6 x completion`; the
  weights are the list-price ratios, so this equals USD cost expressed in
  input-token units.
- Every run's USD cost is recorded in telemetry (`all_in_cost_usd`) by the
  harness; the tables above are arithmetic means over 10 repeats.

## Task-Level Results (10-repeat means)

| Task | SQL ok% | NoKV ok% | SQL prompt | NoKV prompt | SQL USD | NoKV USD |
| --- | --- | --- | --- | --- | --- | --- |
| T1 `train_top_configs_report` | 50% | **100%** | 23,626 | **7,914** | $0.0132 | $0.0058 |
| T2 `eval_fidelity_leaderboard` | 100% | 100% | **4,787** | 9,314 | **$0.0062** | $0.0066 |
| T3 `tabdiff_ddxplus_dcr_checkpoint_provenance` | **100%** | 60% | 84,607 | **35,778** | $0.0322 | **$0.0178** |
| T4 `best_detection_eval_method_audit` | 90% | 90% | **19,334** | 20,213 | $0.0087 | **$0.0081** |
| T5 `cancelled_train_interrupt_triage` | 100% | 100% | 19,217 | **9,608** | $0.0104 | **$0.0050** |

## The Compound Exploration Story (T1, T3, T5)

These tasks mirror what an ML researcher actually asks an agent to do:
locate a cohort of runs, pull facts out of log bodies, and cite where each
fact came from. They require the agent to *compose* several interface
operations, and that is where the surface design dominates the bill.

### T1 — Sweep report (cohort + multi-field projection)

*"Find the 5 best completed training runs by minimum val_loss; report
learning rate, batch size, stdout size, and git state for each."*

- **NoKV (100% correct, 7.9k tokens):** one `catalog` call discovers the
  indexed fields, one `find` call pushes the predicates, the sort, the
  limit, and a six-field projection into the engine. Three to four turns.
- **SQLite (50% correct, 23.6k tokens):** the same answer needs a min-per-run
  aggregation over a 806k-row metric table joined against params, artifacts,
  and git state — and the model writes that query wrong half the time,
  silently. The runs that fail still burn the tokens.

### T3 — Checkpoint provenance (needle cohort + log extraction)

*"For every TabDiff sampling run of the ddxplus_dcr dataset, report which
checkpoint file the sampler loaded and the loaded model's parameter count
(both printed only in the sampler's stdout log)."*

- **NoKV (35.8k tokens):** one namespace-recursive `grep` for the dataset
  line identifies the ten run directories — the path *is* the cohort
  handle. Scoped greps per run directory return the `Checkpoint:` and
  `Model parameters:` lines directly, issued as parallel calls.
- **SQLite (84.6k tokens, 2.4x):** even with a line-oriented `grep_blob`
  available, the model must first resolve params -> artifacts -> blob_ref
  indirection, then orchestrate one call per blob handle. In most repeats
  it falls back to dragging whole stdout blobs through query results.
  It gets the right answer — by paying for it.
- Honesty note: NoKV's correctness on this task is 60% (one cohort
  over-inclusion, one missed extraction across 10 repeats); SQLite is 100%
  at 2.4x the price. Per *correct* answer the namespace surface is still
  cheaper ($0.0297 vs $0.0322).

### T5 — Incident triage (cohort + line-level citation)

*"For every non-completed run: status, stderr size, whether stderr contains
a KeyboardInterrupt, and the line number of its last occurrence."*

- **NoKV (100% correct, 9.6k tokens):** `find` returns the four
  non-completed runs with stderr sizes in one call; one scoped `grep` per
  run returns the matching lines *with line numbers* — citations come free
  with the surface.
- **SQLite (100% correct, 19.2k tokens, 2.0x):** line numbers exist nowhere
  in the relational projection; the model either drives `grep_blob` per
  blob handle or reads bodies and counts. It now succeeds — at twice the
  cost — and only since the arm gained a line-oriented search tool.

### Why the namespace surface wins here

1. **Paths are cohort handles.** A run is a directory; finding it and
   reading from it are the same address space. The relational surface
   interposes blob_ref indirection between "which runs" and "what their
   logs say".
2. **Recursive, scoped search.** `grep` operates over any subtree —
   corpus-wide for discovery, run-scoped for extraction — so the same tool
   serves both ends of a compound task. `grep_blob` can only see one blob
   at a time.
3. **Line numbers are native citations.** Audit-grade answers ("which line
   says so") fall out of the surface instead of requiring newline
   arithmetic in SQL.
4. **Push-down keeps turns short.** `find`/`aggregate` accept predicates,
   sort, limit, and projection in one call; every avoided turn avoids
   re-billing the whole conversation.

## Where Raw SQL Holds Its Ground

Honesty matters for this comparison: on single-shot structured analytics the
relational surface remains excellent. T2 (a metric leaderboard) is a
schema-dump plus one SELECT — 4.8k tokens, hard to beat. T4 (find the best
run, then check one log line) is a statistical tie. The namespace surface
wins *compound* exploration, not every query shape — which is consistent
with where agent workloads are actually heading.

## Fairness Posture

- The harness guarantees stateless episodes. It refuses to run unless the
  profile sets `stateless: true` and `clear_messages_after_run: true`; each
  run launches a fresh agent-runner process whose conversation is rebuilt
  from only the base system message, the arm card, and the task prompt, with
  no `previous_response_id` chaining across runs (test-asserted per repeat).
  The per-run tool bridge also rejects calls carrying another run's id, so
  neither conversation state nor tool state can leak between runs.
- Both arms expose a case-insensitive, line-oriented body search with line
  numbers: NoKV `grep` (namespace-recursive) and SQLite `grep_blob`
  (per blob handle). The residual difference is the surface itself, not a
  missing tool.
- Both arms see logically equal index facts: the NoKV catalog fields and
  the SQLite `run_agent_index_*` tables are materialized from the same
  registration, and `verify` fails if the NoKV catalog drifts from the
  registered field set.
- Judge-side gold (gold SQL for the structured tasks, file-body oracles for
  the log tasks) is never exposed to either arm.
- All harness, client, and metadata test suites pass; runs require a fresh
  `prepare --reset` followed by `verify`.

## Reproduction

```bash
./bench/agent-interface/scripts/start_rustfs.sh
cargo run -p nokv-bench --bin yanex-agent-bench -- prepare \
  --archive /path/to/corpus.tar.gz --data-root bench/data/yanex-demo --reset
cargo run -p nokv-bench --bin yanex-agent-bench -- verify \
  --data-root bench/data/yanex-demo

export OPENAI_API_KEY=...
export OPENAI_INPUT_USD_PER_1M_TOKENS=0.75
export OPENAI_CACHED_INPUT_USD_PER_1M_TOKENS=0.075
export OPENAI_OUTPUT_USD_PER_1M_TOKENS=4.50
./bench/agent-interface/scripts/run_phase1_batch.sh \
  --arm sqlite_raw_v1 --arm nokv_native_v1 \
  --repeats 5 --model gpt-5.4-mini \
  --output-jsonl bench/data/yanex-demo/results/run1.jsonl
```

Result telemetry for the published numbers is committed at
`bench/agent-interface/results/gpt-5.4-mini-5repeats-run1.jsonl`
and `.../gpt-5.4-mini-5repeats-run2.jsonl` (two 5-repeat batches, 100 runs).
Locally produced result files under `bench/data/` stay uncommitted.

## Limitations

- Single agent model (gpt-5.4-mini); stronger models narrow the SQL
  correctness gap on T1 and may orchestrate `grep_blob` better.
- One corpus, five tasks; the task set deliberately weights compound
  exploration because that is the target workload.
- Token prices are list prices at publication time; ratios, not absolute
  dollars, are the durable result.
