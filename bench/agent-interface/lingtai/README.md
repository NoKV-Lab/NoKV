# LingTai Agent-Index Benchmark Track

This track measures LingTai's real agent-log query workload. It is separate
from the historical Yanex benchmark corpus and uses LingTai exports as the
active integration target.

LingTai keeps `logs/events.jsonl` as the authoritative log. `logs/log.sqlite`
is a derived index used by the TUI and session rebuild paths. NoKV's target is
the derived-index role: build a compact, semantic, agent-native index in
`nokv-agent` and compare it against both current and optimized SQLite baselines.

## Baseline Arms

| Arm | Meaning |
| --- | --- |
| `sqlite_current_v1` | The exported LingTai `logs/log.sqlite` exactly as produced by LingTai. |
| `sqlite_projected_v1` | SQLite with explicit projected/expression/materialized indexes for the measured query shapes. This is the fairness baseline. |
| `sqlite_rebuilt_from_jsonl_v1` | A SQLite database rebuilt from the same `logs/events.jsonl` input used by `nokv_agent_index_v1`, with the same projected indexes. This is the equality baseline when the exported SQLite sidecar has different coverage from JSONL. |
| `nokv_agent_index_v1` | Holt-backed `nokv-agent` event index built from `logs/events.jsonl`. |

Do not claim a NoKV win unless the report includes the projected SQLite arm.
Current SQLite can be slow on JSON-field grouping or coverage scans, but many
point queries become fast once SQLite is given the right expression index.

## Workload Extraction

Use the read-only extractor to summarize a LingTai export without printing raw
message bodies, tool argument values, or user text:

```bash
python3 bench/agent-interface/scripts/lingtai_workload_baseline.py \
  --root /path/to/extracted/lingtai-export \
  --projected-sqlite-out /tmp/lingtai-projected.sqlite \
  --json-out /tmp/lingtai-workload.json \
  --markdown-out /tmp/lingtai-workload.md
```

For split archives, pass the parts in order:

```bash
python3 bench/agent-interface/scripts/lingtai_workload_baseline.py \
  --parts /path/to/yuanjiang_lingtai_filtered_logs_20260630.part000 \
          /path/to/yuanjiang_lingtai_filtered_logs_20260630.part001 \
          /path/to/yuanjiang_lingtai_filtered_logs_20260630.part002 \
          /path/to/yuanjiang_lingtai_filtered_logs_20260630.part003 \
          /path/to/yuanjiang_lingtai_filtered_logs_20260630.part004 \
          /path/to/yuanjiang_lingtai_filtered_logs_20260630.part005 \
          /path/to/yuanjiang_lingtai_filtered_logs_20260630.part006 \
  --json-out /tmp/lingtai-workload.json
```

The split-archive mode streams `logs/events.jsonl` directly. SQLite timing
requires either `--root` or an explicit `--sqlite /path/to/log.sqlite`.
`--projected-sqlite-out` always writes a copy and leaves the source SQLite file
untouched.

## SQLite vs NoKV Agent Index

Use the Rust benchmark when comparing `sqlite_current_v1` with
`nokv_agent_index_v1` in the same process. It ingests JSONL into a Holt-backed
agent index, times the typed queries, and emits safe fingerprints instead of raw
user text or tool arguments:

```bash
cargo run -p nokv-bench --release --bin lingtai-index-bench -- \
  --events-jsonl /path/to/logs/events.jsonl \
  --sqlite /path/to/logs/log.sqlite \
  --projected-sqlite /tmp/lingtai-projected.sqlite \
  --rebuilt-sqlite /tmp/lingtai-rebuilt.sqlite \
  --agent-index /tmp/lingtai-agent-index \
  --iterations 5 \
  --reset
```

## LingTai Adapter Contract

The LingTai-side replacement should keep `logs/events.jsonl` as the
authoritative log and replace `tui/internal/sqlitelog` as a derived query
sidecar:

Before querying, run:

```bash
nokv-agent lingtai ingest \
  --store <agent-index-dir> \
  --events <orchDir>/logs/events.jsonl \
  --source-file logs/events.jsonl \
  --agent-id <orchName>
```

Then map the current `sqlitelog` calls to typed commands:

| LingTai call | NoKV-agent command |
| --- | --- |
| `QueryEventsIndexCoverage` | `coverage --source-file logs/events.jsonl` |
| `StreamSessionEvents` | `session-rows`; map `ts`, `event_type`, `fields_json` back to `SessionEventRow{TS, Type, FieldsJSON}` |
| `QueryErrorEvents` | `errors` |
| `HasTUIClearCompletionEvent` | `clear-completion --source-offset <offset>` |
| `QueryMoltSessionWindows` | `molt-windows` |
| `QueryRecentMoltTimes` | `recent --type psyche_molt` |
| `QueryRecentRefreshCompleteTimes` | `recent --type refresh_complete` |
| `QueryNotificationBlocks` | `notification-blocks` |
| `QueryNotificationBlockSnapshots` | `notification-block-snapshots` |
| `QueryNotifications` | `notification-events` or `notifications --ref-id/--event-id/--call-id/--channel` when a lifecycle filter is needed |
| `QueryNotificationByID` | `notification-by-id --event-id <id>` |
| `QueryNotificationBefore` | `notification-before --event-id <id>` |
| `QueryNotificationAfter` | `notification-after --event-id <id>` |

Keep the existing JSONL fallback when ingest, coverage, or row streaming fails,
or when coverage does not reach the tail of `events.jsonl`.

Do not replace LingTai's live JSONL tail path yet. The current NoKV index is a
derived local index with one writer per index directory; live tail remains the
correct source for fresh rows until LingTai has an explicit background ingest
owner.

The real-data validation on the filtered 2026-06-30 export found three clear
optimization points even after adding projected SQLite indexes, plus several
SQLite-favored point-query shapes:

| Query | SQLite current p50 | SQLite projected p50 | JSONL-rebuilt SQLite p50 | `nokv_agent_index_v1` p50 | Notes |
| --- | ---: | ---: | ---: | ---: | --- |
| coverage | 120.339 ms | 109.028 ms | 109.913 ms | 0.001 ms | O(1) coverage key; SQLite still computes over rows. |
| tool/action grouping | 117.273 ms | 1.418 ms | 1.788 ms | 0.028 ms | Materialized projection/facet counts beat expression-indexed SQLite. `nokv-agent` also parses stringified `tool_args`, so facet fingerprints intentionally differ from raw SQLite JSON expressions. |
| tool-name grouping | 109.305 ms | 0.845 ms | 1.063 ms | 0.028 ms | Same materialized facet advantage. |
| TUI clear completion | 0.187 ms | 0.224 ms | 0.225 ms | 0.005 ms | Dedicated newest-first TUI clear index avoids scanning `psyche_molt` / `clear_received` rows. |
| notification before/after | 0.011 / 0.011 ms | 0.010 / 0.010 ms | 0.012 / 0.010 ms | 0.004 / 0.006 ms | Direct neighbor keys are slightly faster than SQLite point traversal. |
| notification by id | 0.008 ms | 0.007 ms | 0.008 ms | 0.014 ms | SQLite wins primary-key lookup. |
| notification events 50 | 0.072 ms | 0.047 ms | 0.049 ms | 0.495 ms | SQLite wins newest-first page materialization. |
| latest notification block/pair | 0.188 / 0.074 ms | 0.024 / 0.020 ms | 0.022 / 0.017 ms | 0.178 / 0.095 ms | Projected SQLite wins this point-query shape. |
| recent molt/refresh times | 0.012 / 0.012 ms | 0.017 / 0.015 ms | 0.016 / 0.015 ms | 0.051 / 0.060 ms | SQLite wins small type/time LIMIT queries. |
| `StreamSessionEvents` row projection | 568.077 ms | 578.335 ms | 628.612 ms | 783.415 ms | JSONL-rebuilt SQLite and `nokv-agent` rows/fingerprints match; SQLite is faster for full row replay. Session index values are event-id pointers to avoid duplicating `fields_json`. |
| full event replay diagnostic | n/a | n/a | n/a | 890.113 ms | Internal diagnostic only; LingTai replacement should use `stream_session_rows`. |

That export's filtered `events.jsonl` and `log.sqlite` do not have identical
coverage (`events.jsonl` accepted 366127 parseable rows and had 112076 session
events; SQLite had 260642 rows, 86477 session-query rows, and a different
source-offset range), so equality fingerprints are not a merge criterion for
the exported sidecar. The `sqlite_rebuilt_from_jsonl_v1` arm rebuilds SQLite
from the same JSONL source and is the equality check: all public sqlitelog-shaped
queries matched rows and fingerprints against `nokv_agent_index_v1`.

In the final run, projected-current SQLite index build took 1535.123 ms and the
projected DB was 457809920 bytes; JSONL-rebuilt SQLite took 4433.903 ms and was
391921664 bytes; NoKV ingest took 7173.482 ms and the Holt agent index was
5610024663 bytes. That size is still the main tradeoff for materialized
semantic indexes and should be reduced further before proposing NoKV-agent as a
universal drop-in sidecar.

## Query Shapes

The first benchmark task set should cover:

- index coverage and freshness;
- session event replay;
- latest notification block/pair queries and notification browser navigation;
- recent `psyche_molt` and `refresh_complete` times;
- TUI clear completion checks;
- tool/action top-N and windowed facets;
- slow or failed `bash` triage;
- `tool_call_id` / `tool_trace_id` correlation from call to result;
- notification lifecycle by `ref_id`, event id, or channel;
- file-tool workload summaries such as read offsets, file extensions, and grep
  shape.

These are semantic agent-runtime questions. They should be exposed to NoKV as
typed event-index queries, not as SQL compatibility strings.

## Evidence Rules

- JSONL remains the source of truth. A missing or stale index must be detectable
  through coverage.
- No benchmark task may depend on hidden answer files or special-case product
  logic.
- Reports must include ingest time, index size, warm query latency, and result
  equality against SQLite for the comparable event set.
- Do not treat filtered JSONL and SQLite files with mismatched coverage as an
  equivalence benchmark. They are still useful for finding optimization points.
- Agent-interface runs must also report token count, turns, tool calls,
  correctness, and cost.
