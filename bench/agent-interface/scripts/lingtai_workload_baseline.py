#!/usr/bin/env python3
"""Summarize LingTai agent logs and run SQLite baseline probes.

The script is deliberately read-only. It emits aggregate counts and timing
statistics without printing raw message bodies, tool arguments, or user text.
"""

from __future__ import annotations

import argparse
import codecs
import collections
import contextlib
import io
import json
import math
import shutil
import sqlite3
import statistics
import tarfile
import time
from pathlib import Path
from typing import BinaryIO, Iterable, Iterator


SESSION_EVENT_TYPES = (
    "thinking",
    "diary",
    "text_input",
    "text_output",
    "tool_call",
    "tool_result",
    "llm_call",
    "llm_response",
    "insight",
    "consultation_fire",
    "notification_pair_injected",
    "apriori_summary_generated",
    "apriori_summary_cap_refused",
    "apriori_summary_failed",
    "apriori_summary_empty",
    "apriori_summary_no_summarizer",
    "aed_attempt",
    "aed_exhausted",
    "aed_timeout",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze LingTai events.jsonl and log.sqlite benchmark baselines."
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--root",
        type=Path,
        help="Extracted LingTai export root containing logs/events.jsonl and logs/log.sqlite.",
    )
    source.add_argument(
        "--parts",
        nargs="+",
        type=Path,
        help="Ordered split archive parts. The archive must contain logs/events.jsonl and logs/log.sqlite.",
    )
    parser.add_argument("--sqlite", type=Path, help="Override SQLite database path.")
    parser.add_argument(
        "--projected-sqlite-out",
        type=Path,
        help="Copy SQLite here, add projected indexes, and measure sqlite_projected_v1.",
    )
    parser.add_argument("--top", type=int, default=20, help="Top-N size for counters.")
    parser.add_argument(
        "--iterations",
        type=int,
        default=20,
        help="SQLite timing iterations after one warmup.",
    )
    parser.add_argument(
        "--json-out",
        type=Path,
        help="Write the full summary JSON to this path instead of stdout.",
    )
    parser.add_argument(
        "--markdown-out",
        type=Path,
        help="Optional compact Markdown report path.",
    )
    return parser.parse_args()


class MultiFileReader(io.RawIOBase):
    """Concatenate split archive parts without materializing a full archive."""

    def __init__(self, paths: Iterable[Path]) -> None:
        self._paths = list(paths)
        if not self._paths:
            raise ValueError("at least one archive part is required")
        self._index = 0
        self._current: BinaryIO | None = None

    def readable(self) -> bool:
        return True

    def close(self) -> None:
        if self._current is not None:
            self._current.close()
            self._current = None
        super().close()

    def readinto(self, buffer: bytearray) -> int:
        view = memoryview(buffer)
        total = 0
        while total < len(view):
            if self._current is None:
                if self._index >= len(self._paths):
                    break
                self._current = self._paths[self._index].open("rb")
                self._index += 1
            chunk = self._current.read(len(view) - total)
            if not chunk:
                self._current.close()
                self._current = None
                continue
            view[total : total + len(chunk)] = chunk
            total += len(chunk)
        return total


@contextlib.contextmanager
def open_tar_member(parts: list[Path], member_name: str) -> Iterator[BinaryIO]:
    reader = io.BufferedReader(MultiFileReader(parts), buffer_size=1024 * 1024)
    with reader:
        with tarfile.open(fileobj=reader, mode="r|gz") as archive:
            for member in archive:
                if member.name == member_name:
                    extracted = archive.extractfile(member)
                    if extracted is None:
                        raise RuntimeError(f"archive member is not readable: {member_name}")
                    with extracted:
                        yield extracted
                    return
    raise FileNotFoundError(f"archive member not found: {member_name}")


def open_event_lines(args: argparse.Namespace) -> Iterator[str]:
    if args.root is not None:
        with (args.root / "logs" / "events.jsonl").open(
            "r", encoding="utf-8", errors="replace"
        ) as file:
            yield from file
        return
    with open_tar_member(args.parts, "logs/events.jsonl") as member:
        yield from codecs.iterdecode(member, "utf-8", errors="replace")


def sqlite_path(args: argparse.Namespace) -> Path | None:
    if args.sqlite is not None:
        return args.sqlite
    if args.root is not None:
        candidate = args.root / "logs" / "log.sqlite"
        return candidate if candidate.exists() else None
    return None


def sorted_counter(counter: collections.Counter, limit: int) -> list[dict[str, object]]:
    return [
        {"value": str(value), "count": count}
        for value, count in counter.most_common(limit)
    ]


def bucket_int(value: object, buckets: tuple[tuple[int, str], ...]) -> str:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return "missing" if value is None else "non_integer"
    for upper, label in buckets:
        if number <= upper:
            return label
    return f">{buckets[-1][0]}"


def parse_tool_args(raw: object) -> dict[str, object]:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            decoded = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        if isinstance(decoded, dict):
            return decoded
    return {}


def command_head(command: object) -> str:
    if not isinstance(command, str):
        return "missing"
    words = command.strip().split()
    if not words:
        return "empty"
    head = words[0]
    if "/" in head:
        return f"path:{Path(head).name or 'unknown'}"
    return head


def path_ext(path: object) -> str:
    if not isinstance(path, str) or not path:
        return "missing"
    suffix = Path(path).suffix.lower()
    return suffix if suffix else "none"


def path_class(path: object) -> str:
    if not isinstance(path, str) or not path:
        return "missing"
    parts = [part for part in path.split("/") if part]
    if not parts:
        return "/"
    if path.startswith("/Users/") or path.startswith("/home/"):
        return "home_absolute"
    if path.startswith("/tmp/") or path.startswith("/var/folders/"):
        return "tmp_absolute"
    if path.startswith("/"):
        return "other_absolute"
    if path.startswith("."):
        return "relative_dot"
    return "relative"


def glob_shape(glob: object) -> str:
    if not isinstance(glob, str) or not glob:
        return "missing"
    if glob == "*":
        return "all"
    if glob.startswith("*.") and "/" not in glob:
        return "extension"
    if glob.startswith("*.{") and glob.endswith("}"):
        return "extension_set"
    if "*" not in glob and "/" not in glob:
        return "basename"
    return "other"


def summarize_events(args: argparse.Namespace) -> dict[str, object]:
    event_types: collections.Counter[str] = collections.Counter()
    field_keys: collections.Counter[str] = collections.Counter()
    tool_names: collections.Counter[str] = collections.Counter()
    tool_actions: collections.Counter[tuple[str, str]] = collections.Counter()
    tool_arg_key_sets: collections.Counter[str] = collections.Counter()
    query_tools: collections.Counter[str] = collections.Counter()
    bash_heads: collections.Counter[str] = collections.Counter()
    read_exts: collections.Counter[str] = collections.Counter()
    read_classes: collections.Counter[str] = collections.Counter()
    read_offsets: collections.Counter[str] = collections.Counter()
    read_limits: collections.Counter[str] = collections.Counter()
    grep_globs: collections.Counter[str] = collections.Counter()
    grep_limits: collections.Counter[str] = collections.Counter()
    parse_errors = 0
    line_count = 0

    for line in open_event_lines(args):
        if not line.strip():
            continue
        line_count += 1
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            parse_errors += 1
            continue
        event_type = str(event.get("type", "missing"))
        event_types[event_type] += 1
        for key in event:
            field_keys[key] += 1
        if event_type != "tool_call":
            continue
        tool_name = str(event.get("tool_name") or event.get("tool") or "missing")
        tool_names[tool_name] += 1
        tool_args = parse_tool_args(event.get("tool_args"))
        action = str(tool_args.get("action", "missing"))
        tool_actions[(tool_name, action)] += 1
        tool_arg_key_sets[",".join(sorted(tool_args))] += 1
        if tool_args.get("query"):
            query_tools[tool_name] += 1
        if tool_name == "bash":
            bash_heads[command_head(tool_args.get("command"))] += 1
        elif tool_name == "read":
            read_exts[path_ext(tool_args.get("file_path"))] += 1
            read_classes[path_class(tool_args.get("file_path"))] += 1
            read_offsets[
                "missing"
                if tool_args.get("offset") is None
                else "zero" if str(tool_args.get("offset")) == "0" else "nonzero"
            ] += 1
            read_limits[
                bucket_int(
                    tool_args.get("limit", tool_args.get("max_chars")),
                    ((100, "<=100"), (500, "<=500"), (2000, "<=2000"), (10000, "<=10000")),
                )
            ] += 1
        elif tool_name == "grep":
            grep_globs[glob_shape(tool_args.get("glob"))] += 1
            grep_limits[str(tool_args.get("max_matches", "missing"))] += 1

    top = args.top
    return {
        "events_jsonl": {
            "lines": line_count,
            "parse_errors": parse_errors,
            "event_types": sorted_counter(event_types, top),
            "field_keys": sorted_counter(field_keys, top),
        },
        "tool_calls": {
            "count": sum(tool_names.values()),
            "tools": sorted_counter(tool_names, top),
            "tool_actions": [
                {"tool": tool, "action": action, "count": count}
                for (tool, action), count in tool_actions.most_common(top)
            ],
            "arg_key_sets": sorted_counter(tool_arg_key_sets, top),
            "query_tools": sorted_counter(query_tools, top),
            "bash_command_heads": sorted_counter(bash_heads, top),
            "read_extensions": sorted_counter(read_exts, top),
            "read_path_classes": sorted_counter(read_classes, top),
            "read_offsets": sorted_counter(read_offsets, top),
            "read_limits": sorted_counter(read_limits, top),
            "grep_globs": sorted_counter(grep_globs, top),
            "grep_limits": sorted_counter(grep_limits, top),
        },
    }


def percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    index = min(len(values) - 1, max(0, math.ceil(len(values) * fraction) - 1))
    return sorted(values)[index]


def timed_query(
    conn: sqlite3.Connection, sql: str, params: tuple[object, ...], iterations: int
) -> dict[str, object]:
    list(conn.execute(sql, params))
    timings: list[float] = []
    rows = 0
    for _ in range(iterations):
        started = time.perf_counter()
        result = list(conn.execute(sql, params))
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        timings.append(elapsed_ms)
        rows = len(result)
    return {
        "rows": rows,
        "p50_ms": round(statistics.median(timings), 3),
        "p95_ms": round(percentile(timings, 0.95), 3),
        "avg_ms": round(statistics.mean(timings), 3),
    }


def sqlite_baseline(db_path: Path, iterations: int) -> dict[str, object]:
    conn = sqlite3.connect(db_path)
    conn.execute("pragma query_only=on")
    placeholders = ",".join("?" for _ in SESSION_EVENT_TYPES)
    queries = {
        "coverage": (
            "select coalesce(min(source_offset), -1), "
            "coalesce(max(source_offset), -1), count(source_offset) from events",
            (),
        ),
        "stream_session_events": (
            f"select ts, type, fields_json from events where type in ({placeholders}) order by id asc",
            SESSION_EVENT_TYPES,
        ),
        "latest_notification_block_10": (
            "select id, ts, fields_json, coalesce(source_file, '') "
            "from events where type = 'notification_block_injected' order by id desc limit 10",
            (),
        ),
        "latest_notification_pair_10": (
            "select id, ts, fields_json, coalesce(source_file, '') "
            "from events where type = 'notification_pair_injected' order by id desc limit 10",
            (),
        ),
        "recent_psyche_molt_10": (
            "select ts from events where type = 'psyche_molt' order by ts desc limit 10",
            (),
        ),
        "recent_refresh_complete_10": (
            "select ts from events where type = 'refresh_complete' order by ts desc limit 10",
            (),
        ),
        "error_events": (
            "select ts, type, fields_json from events "
            "where type in ('aed_attempt', 'aed_exhausted', 'refresh_init_error') "
            "order by id desc",
            (),
        ),
        "tool_name_group_json_extract": (
            "select json_extract(fields_json, '$.tool_name') as tool_name, count(*) "
            "from events where type = 'tool_call' and json_valid(fields_json) "
            "group by tool_name order by count(*) desc limit 30",
            (),
        ),
        "tool_action_group_json_extract": (
            "select json_extract(fields_json, '$.tool_name') as tool_name, "
            "json_extract(fields_json, '$.tool_args.action') as action, count(*) "
            "from events where type = 'tool_call' and json_valid(fields_json) "
            "group by tool_name, action order by count(*) desc limit 50",
            (),
        ),
    }
    stats = {
        name: timed_query(conn, sql, tuple(params), iterations)
        for name, (sql, params) in queries.items()
    }
    return {
        "path": str(db_path),
        "bytes": db_path.stat().st_size,
        "sqlite_version": conn.execute("select sqlite_version()").fetchone()[0],
        "event_rows": conn.execute("select count(*) from events").fetchone()[0],
        "invalid_fields_json": conn.execute(
            "select count(*) from events where not json_valid(fields_json)"
        ).fetchone()[0],
        "queries": stats,
    }


def sqlite_index_sizes(conn: sqlite3.Connection, prefix: str) -> list[dict[str, object]]:
    try:
        rows = conn.execute(
            "select name, sum(pgsize) from dbstat where name like ? group by name order by name",
            (f"{prefix}%",),
        ).fetchall()
    except sqlite3.DatabaseError:
        return []
    return [{"name": name, "bytes": bytes_} for name, bytes_ in rows]


def projected_sqlite_baseline(
    source_db: Path, projected_db: Path, iterations: int
) -> dict[str, object]:
    started = time.perf_counter()
    projected_db.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_db, projected_db)
    conn = sqlite3.connect(projected_db)
    index_sql = (
        "create index if not exists idx_events_source_offset_only "
        "on events(source_offset) where source_offset is not null",
        "create index if not exists idx_events_type_id_desc "
        "on events(type, id desc)",
        "create index if not exists idx_events_tool_name_expr "
        "on events(json_extract(fields_json, '$.tool_name')) "
        "where type = 'tool_call' and json_valid(fields_json)",
        "create index if not exists idx_events_tool_action_expr "
        "on events(json_extract(fields_json, '$.tool_name'), "
        "json_extract(fields_json, '$.tool_args.action')) "
        "where type = 'tool_call' and json_valid(fields_json)",
    )
    for statement in index_sql:
        conn.execute(statement)
    conn.commit()
    build_ms = (time.perf_counter() - started) * 1000.0
    conn.execute("pragma query_only=on")
    placeholders = ",".join("?" for _ in SESSION_EVENT_TYPES)
    queries = {
        "coverage": (
            "select coalesce(min(source_offset), -1), "
            "coalesce(max(source_offset), -1), count(source_offset) from events",
            (),
        ),
        "stream_session_events": (
            f"select ts, type, fields_json from events where type in ({placeholders}) order by id asc",
            SESSION_EVENT_TYPES,
        ),
        "latest_notification_block_10": (
            "select id, ts, fields_json, coalesce(source_file, '') "
            "from events where type = 'notification_block_injected' order by id desc limit 10",
            (),
        ),
        "latest_notification_pair_10": (
            "select id, ts, fields_json, coalesce(source_file, '') "
            "from events where type = 'notification_pair_injected' order by id desc limit 10",
            (),
        ),
        "tool_name_group_forced_projection": (
            "select json_extract(fields_json, '$.tool_name') as tool_name, count(*) "
            "from events indexed by idx_events_tool_name_expr "
            "where type = 'tool_call' and json_valid(fields_json) "
            "group by tool_name order by count(*) desc limit 30",
            (),
        ),
        "tool_action_group_forced_projection": (
            "select json_extract(fields_json, '$.tool_name') as tool_name, "
            "json_extract(fields_json, '$.tool_args.action') as action, count(*) "
            "from events indexed by idx_events_tool_action_expr "
            "where type = 'tool_call' and json_valid(fields_json) "
            "group by tool_name, action order by count(*) desc limit 50",
            (),
        ),
        "latest_read_tool_calls_100": (
            "select id, ts, fields_json from events indexed by idx_events_tool_name_expr "
            "where type = 'tool_call' and json_valid(fields_json) "
            "and json_extract(fields_json, '$.tool_name') = 'read' "
            "order by id desc limit 100",
            (),
        ),
    }
    return {
        "path": str(projected_db),
        "bytes": projected_db.stat().st_size,
        "build_ms": round(build_ms, 3),
        "projected_index_sizes": sqlite_index_sizes(conn, "idx_events_"),
        "queries": {
            name: timed_query(conn, sql, tuple(params), iterations)
            for name, (sql, params) in queries.items()
        },
    }


def write_markdown(summary: dict[str, object], path: Path) -> None:
    events = summary["events_jsonl"]
    tools = summary["tool_calls"]
    sqlite = summary.get("sqlite_current_v1")
    lines = [
        "# LingTai Workload Baseline",
        "",
        f"- JSONL lines: {events['lines']}",
        f"- JSON parse errors: {events['parse_errors']}",
        f"- tool_call rows: {tools['count']}",
        "",
        "## Top Tools",
        "",
        "| Tool | Count |",
        "| --- | ---: |",
    ]
    lines.extend(
        f"| `{row['value']}` | {row['count']} |" for row in tools["tools"]
    )
    lines.extend(["", "## Top Tool Actions", "", "| Tool | Action | Count |", "| --- | --- | ---: |"])
    lines.extend(
        f"| `{row['tool']}` | `{row['action']}` | {row['count']} |"
        for row in tools["tool_actions"]
    )
    if sqlite:
        lines.extend(["", "## SQLite Current Baseline", "", "| Query | Rows | p50 ms | p95 ms |", "| --- | ---: | ---: | ---: |"])
        for name, row in sqlite["queries"].items():
            lines.append(f"| `{name}` | {row['rows']} | {row['p50_ms']} | {row['p95_ms']} |")
    projected = summary.get("sqlite_projected_v1")
    if projected:
        lines.extend(
            [
                "",
                "## SQLite Projected Baseline",
                "",
                f"- build ms: {projected['build_ms']}",
                f"- db bytes: {projected['bytes']}",
                "",
                "| Query | Rows | p50 ms | p95 ms |",
                "| --- | ---: | ---: | ---: |",
            ]
        )
        for name, row in projected["queries"].items():
            lines.append(f"| `{name}` | {row['rows']} | {row['p50_ms']} | {row['p95_ms']} |")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    summary = summarize_events(args)
    db_path = sqlite_path(args)
    if db_path is not None and db_path.exists():
        summary["sqlite_current_v1"] = sqlite_baseline(db_path, args.iterations)
        if args.projected_sqlite_out is not None:
            summary["sqlite_projected_v1"] = projected_sqlite_baseline(
                db_path, args.projected_sqlite_out, args.iterations
            )
    if args.markdown_out is not None:
        write_markdown(summary, args.markdown_out)
    data = json.dumps(summary, indent=2, sort_keys=True)
    if args.json_out is not None:
        args.json_out.write_text(data + "\n", encoding="utf-8")
    else:
        print(data)


if __name__ == "__main__":
    main()
