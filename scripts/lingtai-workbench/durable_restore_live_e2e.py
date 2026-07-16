#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Live acceptance gate for durable NoKV workbench restore-to-fork.

The gate owns an isolated RustFS container, a counting S3 proxy, a NoKV
metadata server, and disposable LingTai Agent registrations. It drives restore
through independent real LingTai MCP clients; the NoKV CLI is used only for
namespace mutations and streaming the large binary fixture.

The full profile is the merge gate.  ``--profile full --require-all`` always
uses a real 1 GiB COW fixture and treats every missing dependency or unexecuted
assertion as a failure.
"""

from __future__ import annotations

import argparse
import collections
import concurrent.futures
import dataclasses
import hashlib
import http.client
import http.server
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Callable
from unittest.mock import MagicMock


WORKBENCH_ROOT_TEMPLATE = "/agents/{agent_id}/wb"
RESTORE_TOOL = "workbench_restore"
SECTIONS = ("input", "scripts", "outputs", "logs", "metadata")
BASE_WORKBENCH_TOOLS = {
    "workbench_create",
    "workbench_put_file",
    "workbench_append",
    "workbench_edit",
    "workbench_list",
    "workbench_stat",
    "workbench_read",
    "workbench_grep",
    "workbench_search",
    "workbench_aggregate",
    "workbench_catalog",
    "workbench_find",
    "workbench_commit",
    "workbench_snapshot",
    "workbench_snapshot_renew",
    "workbench_snapshot_list",
}
HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "proxy-connection",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}
CLEANUP_CRASH_PHASE = "cleanup-batch-000000"
RELEASE_CRASH_PHASE = "release-batch-000000"
RETRYABLE_FSCK_CONFLICT = (
    "object-reference fsck raced a metadata write; retry the scan"
)
DEFAULT_LIVE_MOUNT_ID = 1
CONCURRENT_RESTORE_CALLS = 16
# The public workbench_search schema caps one page at 10. Keep this below the
# cap so even the quick workload is forced through multiple real cursor pages.
INDEX_PAGE_LIMIT = 7
COW_BLOCK_BYTES = 4 << 20
FULL_COW_BLOCK_COUNT = 256
MAX_DISCOVERED_BATCH_PHASES = 16_384
# Manual GC calls exercise the release/retention races deterministically. Keep
# periodic workers far enough apart that the online full fsck can obtain the
# stable metadata epoch its public contract requires.
# Every lifecycle assertion below explicitly drives GC, including a dedicated
# concurrent pin-retirement race. Keep passive ticks outside the acceptance
# window so stable-epoch fsck is not nondeterministically invalidated by a
# timer that provides no additional coverage.
BACKGROUND_GC_INTERVAL_MS = 3_600_000


class AcceptanceError(RuntimeError):
    """A deterministic acceptance assertion failed."""


@dataclasses.dataclass(frozen=True)
class WorkloadProfile:
    name: str
    indexed_files: int
    large_bytes: int


def workload_profile(name: str) -> WorkloadProfile:
    if name == "quick":
        return WorkloadProfile(name, indexed_files=24, large_bytes=16 << 20)
    if name == "full":
        return WorkloadProfile(name, indexed_files=96, large_bytes=1 << 30)
    raise AcceptanceError(f"unknown workload profile: {name}")


def indexed_restore_phase(kind: str, index: int) -> str:
    if kind not in {"materialize", "reference"}:
        raise AcceptanceError(f"unknown indexed restore phase kind: {kind!r}")
    if index < 0 or index > 999_999:
        raise AcceptanceError(f"restore phase index is out of range: {index}")
    return f"{kind}-batch-{index:06d}"


def create_crash_phases(
    materialization_batches: int, reference_batches: int
) -> tuple[str, ...]:
    if materialization_batches < 1 or reference_batches < 1:
        raise AcceptanceError("restore crash matrix requires non-empty batch phases")
    return (
        "hold-applied",
        *(
            indexed_restore_phase("materialize", index)
            for index in range(materialization_batches)
        ),
        "initialization-put-before-000000",
        "initialization-put-after-000000",
        "index-sealed",
        *(
            indexed_restore_phase("reference", index)
            for index in range(reference_batches)
        ),
        "references-sealed",
        "attach-applied",
    )


def restore_phase_requires_manifest_rebuild(phase: str) -> bool:
    """Return whether a crash leaves an uploaded pre-attach manifest behind."""
    return (
        phase == "initialization-put-after-000000"
        or phase == "index-sealed"
        or phase == "references-sealed"
        or phase.startswith("reference-batch-")
    )


def fixture_block_count(size: int) -> int:
    if size <= 0 or size % COW_BLOCK_BYTES != 0:
        raise AcceptanceError("COW fixture size must be a positive multiple of 4 MiB")
    return size // COW_BLOCK_BYTES


def fixture_block_marker(index: int) -> bytes:
    if index < 0:
        raise AcceptanceError("COW block index must be non-negative")
    identity = index.to_bytes(8, "big")
    return (
        f"nokv-durable-restore-v1:block:{index:06d}:".encode()
        + hashlib.sha256(b"nokv-cow-block-marker-v1\0" + identity).digest()
        + b"\n"
    )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(8 << 20):
            digest.update(chunk)
    return digest.hexdigest()


@dataclasses.dataclass(frozen=True)
class ToolError:
    code: str
    message: str
    retryable: bool
    details: dict[str, Any]


@dataclasses.dataclass(frozen=True)
class McpLaunch:
    command: str
    args: list[str]
    env: dict[str, str]
    root: str


@dataclasses.dataclass(frozen=True)
class ObjectFingerprint:
    size: int
    etag: str
    last_modified: str


@dataclasses.dataclass(frozen=True)
class ObjectMutation:
    method: str
    target: str


@dataclasses.dataclass(frozen=True)
class BarrierHandle:
    directory: Path
    stem: str

    @property
    def arm_path(self) -> Path:
        return self.directory / f"{self.stem}.arm"

    @property
    def ready_path(self) -> Path:
        return self.directory / f"{self.stem}.ready"

    @property
    def continue_path(self) -> Path:
        return self.directory / f"{self.stem}.continue"

    def arm(self) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        for path in (self.ready_path, self.continue_path):
            path.unlink(missing_ok=True)
        self.arm_path.write_text("armed\n", encoding="utf-8")

    def wait_ready(self, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        while not self.ready_path.exists():
            if time.monotonic() >= deadline:
                raise AcceptanceError(
                    f"restore crash barrier did not become ready: {self.ready_path}"
                )
            time.sleep(0.002)

    def wait_ready_or_done(
        self, future: concurrent.futures.Future[Any], timeout: float
    ) -> bool:
        deadline = time.monotonic() + timeout
        while True:
            if self.ready_path.exists():
                return True
            if future.done():
                return False
            if time.monotonic() >= deadline:
                raise AcceptanceError(
                    f"restore crash barrier did not become ready: {self.ready_path}"
                )
            time.sleep(0.002)

    def disarm_after_crash(self) -> None:
        self.arm_path.unlink(missing_ok=True)
        self.ready_path.unlink(missing_ok=True)
        self.continue_path.unlink(missing_ok=True)

    def release(self) -> None:
        self.continue_path.write_text("continue\n", encoding="utf-8")


class RestoreBarrierController:
    def __init__(self, directory: Path) -> None:
        self.directory = directory

    def handle(self, operation_id: str, phase: str) -> BarrierHandle:
        if not operation_id.startswith("restore-"):
            raise AcceptanceError(f"invalid restore operation id: {operation_id!r}")
        if not phase or "/" in phase or "\\" in phase or "\0" in phase:
            raise AcceptanceError(f"invalid restore barrier phase: {phase!r}")
        return BarrierHandle(self.directory, f"{operation_id}.{phase}")

    def clear(self) -> None:
        shutil.rmtree(self.directory, ignore_errors=True)
        self.directory.mkdir(parents=True, exist_ok=True)


def canonical_absolute_path(path: str) -> str:
    if not path.startswith("/"):
        raise AcceptanceError(f"restore operation path must be absolute: {path!r}")
    components = [component for component in path.split("/") if component]
    if any(component in {".", ".."} for component in components):
        raise AcceptanceError(f"restore operation path is not canonical: {path!r}")
    return "/" + "/".join(components)


def restore_operation_id(
    mount_id: int, source_path: str, snapshot_id: int, destination_path: str
) -> str:
    if mount_id <= 0 or snapshot_id < 0:
        raise AcceptanceError("mount id must be positive and snapshot id non-negative")
    source = canonical_absolute_path(source_path).encode()
    destination = canonical_absolute_path(destination_path).encode()
    digest = hashlib.sha256()
    digest.update(b"nokv-restore-to-fork-request-v1\0")
    digest.update(mount_id.to_bytes(8, "big"))
    digest.update(len(source).to_bytes(8, "big"))
    digest.update(source)
    digest.update(snapshot_id.to_bytes(8, "big"))
    digest.update(len(destination).to_bytes(8, "big"))
    digest.update(destination)
    return f"restore-{digest.hexdigest()}"


def changed_objects(
    before: dict[str, ObjectFingerprint],
    after: dict[str, ObjectFingerprint],
) -> set[str]:
    """Return new, deleted, or overwritten S3 keys."""
    return {
        key for key in before.keys() | after.keys() if before.get(key) != after.get(key)
    }


def assert_native_tool_error(result: dict[str, Any], expected_code: str) -> ToolError:
    """Require LingTai to preserve the MCP structured error at the top level."""
    if result.get("status") != "error":
        raise AcceptanceError(f"MCP error lacks top-level error status: {result!r}")
    code = result.get("code")
    message = result.get("message")
    retryable = result.get("retryable")
    details = result.get("details")
    if not isinstance(code, str) or not code:
        raise AcceptanceError(f"MCP error lacks top-level code: {result!r}")
    if not isinstance(message, str) or not message:
        raise AcceptanceError(f"MCP error lacks top-level message: {result!r}")
    if not isinstance(retryable, bool):
        raise AcceptanceError(f"MCP error lacks top-level retryable: {result!r}")
    if not isinstance(details, dict):
        raise AcceptanceError(f"MCP error lacks top-level details: {result!r}")
    if code != expected_code:
        raise AcceptanceError(
            f"MCP error code differs: expected {expected_code!r}, observed {code!r}"
        )
    return ToolError(code, message, retryable, details)


def decode_tool_error(result: dict[str, Any]) -> ToolError:
    payload: Any = result
    if not isinstance(payload.get("code"), str):
        payload = result.get("message")
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError as exc:
                raise AcceptanceError(
                    f"MCP error is not structured JSON: {payload!r}"
                ) from exc
    if not isinstance(payload, dict):
        raise AcceptanceError(f"MCP error payload is not an object: {payload!r}")
    code = payload.get("code")
    message = payload.get("message")
    retryable = payload.get("retryable")
    details = payload.get("details")
    if not isinstance(code, str) or not isinstance(message, str):
        raise AcceptanceError(f"typed MCP error lacks code/message: {payload!r}")
    if not isinstance(retryable, bool) or not isinstance(details, dict):
        raise AcceptanceError(f"typed MCP error lacks retryable/details: {payload!r}")
    return ToolError(code, message, retryable, details)


def validate_restore_manifest(
    manifest: dict[str, Any],
    *,
    operation_id: str,
    source_workbench_id: str,
    source_path: str,
    destination_workbench_id: str,
    destination_path: str,
    snapshot_id: int,
) -> None:
    restored_from = manifest.get("restored_from")
    if (
        manifest.get("schema") != "nokv.workbench.restore_manifest.v1"
        or manifest.get("operation_id") != operation_id
        or manifest.get("source_workbench_id") != source_workbench_id
        or manifest.get("source_path") != source_path
        or manifest.get("destination_workbench_id") != destination_workbench_id
        or manifest.get("destination_path") != destination_path
        or manifest.get("snapshot_id") != snapshot_id
        or not isinstance(restored_from, dict)
        or restored_from.get("workbench_id") != source_workbench_id
        or restored_from.get("path") != source_path
        or restored_from.get("snapshot_id") != snapshot_id
    ):
        raise AcceptanceError(f"restore manifest is malformed: {manifest!r}")


def error_text(result: dict[str, Any]) -> str:
    message = result.get("message")
    if isinstance(message, str):
        return message
    return json.dumps(result, sort_keys=True)


def assert_tool_error(result: dict[str, Any], context: str) -> None:
    if result.get("status") != "error" and "code" not in result:
        raise AcceptanceError(f"{context} unexpectedly succeeded: {result!r}")


def validate_tool_contract(tools: list[dict[str, Any]]) -> None:
    """Validate the capability-gated 17-tool surface and raw restore schema."""
    by_name = {tool.get("name"): tool for tool in tools}
    expected = BASE_WORKBENCH_TOOLS | {RESTORE_TOOL}
    if set(by_name) != expected:
        raise AcceptanceError(
            "unexpected workbench tool surface; "
            f"missing={sorted(expected - set(by_name))}, "
            f"extra={sorted(set(by_name) - expected)}"
        )
    schema = by_name[RESTORE_TOOL].get("schema")
    if not isinstance(schema, dict):
        raise AcceptanceError("workbench_restore lacks inputSchema")
    expected_fields = {"id", "at_snapshot", "destination_id"}
    if schema.get("type") != "object":
        raise AcceptanceError("workbench_restore schema must be an object")
    if set(schema.get("required", [])) != expected_fields:
        raise AcceptanceError("workbench_restore must require exactly three fields")
    properties = schema.get("properties")
    if not isinstance(properties, dict) or set(properties) != expected_fields:
        raise AcceptanceError("workbench_restore schema contains wrong properties")
    if schema.get("additionalProperties") is not False:
        raise AcceptanceError("workbench_restore must reject additional properties")
    for field in ("id", "destination_id"):
        field_schema = properties.get(field)
        if not isinstance(field_schema, dict) or field_schema.get("type") != "string":
            raise AcceptanceError(f"workbench_restore {field} must be a string")
        if field_schema.get("minLength") != 1:
            raise AcceptanceError(f"workbench_restore {field} must be non-empty")
    alternatives = properties.get("at_snapshot", {}).get("anyOf")
    if not isinstance(alternatives, list) or len(alternatives) != 2:
        raise AcceptanceError("at_snapshot must have exactly two alternatives")
    by_type = {
        item.get("type"): item for item in alternatives if isinstance(item, dict)
    }
    if set(by_type) != {"integer", "string"}:
        raise AcceptanceError("at_snapshot accepts a type other than integer/string")
    if by_type["integer"].get("minimum") != 0:
        raise AcceptanceError("numeric at_snapshot must be non-negative")
    if by_type["string"].get("minLength") != 1:
        raise AcceptanceError("string at_snapshot must be non-empty")


def read_json_object(result: dict[str, Any]) -> dict[str, Any]:
    if (
        result.get("format") != "structured"
        or result.get("record_type") != "json_object"
    ):
        raise AcceptanceError(f"expected structured JSON object: {result!r}")
    if result.get("truncated") is True or result.get("next_cursor") is not None:
        raise AcceptanceError("manifest read was truncated")
    value: dict[str, Any] = {}
    items = result.get("items")
    if not isinstance(items, list):
        raise AcceptanceError("structured JSON response lacks items")
    for item in items:
        record = item.get("value") if isinstance(item, dict) else None
        key = record.get("key") if isinstance(record, dict) else None
        if not isinstance(key, str) or "value" not in record:
            raise AcceptanceError(f"malformed JSON record: {item!r}")
        value[key] = record["value"]
    return value


RESTORE_OPERATION_STATES = (
    "preparing",
    "ready_to_attach",
    "complete",
    "cleaning",
    "discarding",
    "releasing",
)
RESTORE_DURABLE_LEDGER_ROWS = {
    "init_upload_tombstone",
    "init_upload_tombstone_cursor",
    "release_cursor",
}


def nonnegative_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise AcceptanceError(f"{field} must be a non-negative integer: {value!r}")
    return value


def restore_release_graph_drained(metrics: Any) -> bool:
    if not isinstance(metrics, dict):
        raise AcceptanceError(f"restore metrics are not an object: {metrics!r}")
    operations = metrics.get("operations")
    if not isinstance(operations, dict) or set(operations) != set(
        RESTORE_OPERATION_STATES
    ):
        raise AcceptanceError(f"restore operation metrics are malformed: {metrics!r}")
    operation_rows = sum(
        nonnegative_int(operations[state], f"restore.operations.{state}")
        for state in RESTORE_OPERATION_STATES
    )
    private_rows = sum(
        nonnegative_int(metrics.get(field), f"restore.{field}")
        for field in (
            "staging_rows",
            "exact_reference_rows",
            "index_rows",
            "cleanup_backlog",
            "release_backlog",
        )
    )
    if nonnegative_int(metrics.get("quarantine_rows"), "restore.quarantine_rows"):
        raise AcceptanceError(f"restore release entered quarantine: {metrics!r}")
    return operation_rows == 0 and private_rows == 0


def validate_restore_metrics_object(
    metrics: Any, *, expected_complete: int, expect_empty: bool
) -> dict[str, Any]:
    if not isinstance(metrics, dict):
        raise AcceptanceError(f"restore metrics are not an object: {metrics!r}")
    if metrics.get("active_marker") is not True:
        raise AcceptanceError(f"restore active marker is not durable: {metrics!r}")
    if metrics.get("allocator_v2_fenced") is not True:
        raise AcceptanceError(
            f"restore allocator downgrade fence is absent: {metrics!r}"
        )
    operations = metrics.get("operations")
    if not isinstance(operations, dict) or set(operations) != set(
        RESTORE_OPERATION_STATES
    ):
        raise AcceptanceError(f"restore operation metrics are malformed: {metrics!r}")
    counts = {
        state: nonnegative_int(operations[state], f"restore.operations.{state}")
        for state in RESTORE_OPERATION_STATES
    }
    if counts["complete"] != expected_complete or any(
        count != 0 for state, count in counts.items() if state != "complete"
    ):
        raise AcceptanceError(
            "restore operation graph is not uniquely terminal: "
            f"expected_complete={expected_complete}, counts={counts!r}"
        )
    for field in (
        "staging_rows",
        "exact_reference_rows",
        "index_rows",
        "cleanup_backlog",
        "release_backlog",
        "quarantine_rows",
    ):
        nonnegative_int(metrics.get(field), f"restore.{field}")
    control_rows = metrics.get("control_rows")
    if not isinstance(control_rows, dict) or not control_rows:
        raise AcceptanceError(f"restore control row metrics are malformed: {metrics!r}")
    for name, count in control_rows.items():
        if not isinstance(name, str) or not name:
            raise AcceptanceError(f"restore control row name is malformed: {name!r}")
        nonnegative_int(count, f"restore.control_rows.{name}")
    if nonnegative_int(
        control_rows.get("operation"), "restore.control_rows.operation"
    ) != sum(counts.values()):
        raise AcceptanceError("restore operation row count disagrees with state counts")
    if any(
        nonnegative_int(metrics[field], f"restore.{field}") != 0
        for field in ("cleanup_backlog", "release_backlog", "quarantine_rows")
    ):
        raise AcceptanceError(f"restore backlog/quarantine is non-zero: {metrics!r}")
    if expect_empty:
        leaked_rows = {
            name: nonnegative_int(count, f"restore.control_rows.{name}")
            for name, count in control_rows.items()
            if name not in RESTORE_DURABLE_LEDGER_ROWS and count != 0
        }
        if leaked_rows or any(
            nonnegative_int(metrics[field], f"restore.{field}") != 0
            for field in ("staging_rows", "exact_reference_rows", "index_rows")
        ):
            raise AcceptanceError(
                "released restore graph rows did not drain to the durable-ledger "
                f"baseline: leaked={leaked_rows!r}, metrics={metrics!r}"
            )
    elif expected_complete > 0 and any(
        nonnegative_int(metrics[field], f"restore.{field}") == 0
        for field in ("staging_rows", "exact_reference_rows", "index_rows")
    ):
        raise AcceptanceError(f"complete restore graph lacks private rows: {metrics!r}")
    return metrics


def validate_restore_metrics(
    stats: Any, *, expected_complete: int, expect_empty: bool
) -> dict[str, Any]:
    if not isinstance(stats, dict):
        raise AcceptanceError(f"server stats are not an object: {stats!r}")
    metrics = stats.get("restore")
    if not isinstance(metrics, dict) or metrics.get("available") is not True:
        raise AcceptanceError(f"restore metrics are unavailable: {metrics!r}")
    return validate_restore_metrics_object(
        metrics, expected_complete=expected_complete, expect_empty=expect_empty
    )


def validate_fsck_report(
    report: Any,
    *,
    expected_complete: int,
    expected_snapshot_pins: int,
    expected_fork_bindings: int,
) -> dict[str, Any]:
    if not isinstance(report, dict) or report.get("consistent") is not True:
        raise AcceptanceError(f"fsck is not consistent: {report!r}")
    for count_field, rows_field in (
        ("dangling_count", "dangling"),
        ("size_mismatch_count", "size_mismatches"),
    ):
        if nonnegative_int(report.get(count_field), count_field) != 0:
            raise AcceptanceError(f"fsck {count_field} is non-zero: {report!r}")
        if report.get(rows_field) != []:
            raise AcceptanceError(f"fsck {rows_field} is not empty: {report!r}")
    if (
        nonnegative_int(report.get("snapshot_pins_scanned"), "snapshot_pins_scanned")
        != expected_snapshot_pins
    ):
        raise AcceptanceError(f"fsck snapshot pin count changed: {report!r}")
    if (
        nonnegative_int(report.get("fork_bindings_scanned"), "fork_bindings_scanned")
        != expected_fork_bindings
    ):
        raise AcceptanceError(f"fsck ForkBinding count changed: {report!r}")
    shards = report.get("restore_shards")
    if not isinstance(shards, list) or len(shards) != 1:
        raise AcceptanceError(f"fsck must report exactly one restore shard: {shards!r}")
    shard = shards[0]
    if not isinstance(shard, dict) or shard.get("mount_id") != DEFAULT_LIVE_MOUNT_ID:
        raise AcceptanceError(f"fsck restore shard identity is wrong: {shard!r}")
    restore = shard.get("report")
    if not isinstance(restore, dict) or restore.get("consistent") is not True:
        raise AcceptanceError(f"restore fsck is not consistent: {restore!r}")
    for field in (
        "issues",
        "dangling_borrowed_objects",
        "borrowed_object_size_mismatches",
    ):
        if restore.get(field) != []:
            raise AcceptanceError(f"restore fsck {field} is not empty: {restore!r}")
    validate_restore_metrics_object(
        restore.get("metrics"),
        expected_complete=expected_complete,
        expect_empty=expected_complete == 0,
    )
    borrowed = nonnegative_int(
        restore.get("borrowed_objects_checked"), "borrowed_objects_checked"
    )
    if expected_complete > 0 and borrowed == 0:
        raise AcceptanceError("restore fsck checked no borrowed objects")
    return report


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def tail(path: Path, lines: int = 120) -> str:
    if not path.exists():
        return "<log not created>"
    return "\n".join(
        path.read_text(encoding="utf-8", errors="replace").splitlines()[-lines:]
    )


class CountingProxyServer(http.server.ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, bind: tuple[str, int], backend_port: int) -> None:
        super().__init__(bind, CountingProxyHandler)
        self.backend_port = backend_port
        self._lock = threading.Lock()
        self._successful_puts: list[str] = []
        self._successful_mutations: list[ObjectMutation] = []

    def record_put(self, target: str) -> None:
        with self._lock:
            self._successful_puts.append(target)
            self._successful_mutations.append(ObjectMutation("PUT", target))

    def record_delete(self, target: str) -> None:
        with self._lock:
            self._successful_mutations.append(ObjectMutation("DELETE", target))

    def put_records(self) -> list[str]:
        with self._lock:
            return list(self._successful_puts)

    def mutation_records(self) -> list[ObjectMutation]:
        with self._lock:
            return list(self._successful_mutations)


class CountingProxyHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server: CountingProxyServer

    def log_message(self, _format: str, *_args: Any) -> None:
        return

    def do_DELETE(self) -> None:
        self._forward()

    def do_GET(self) -> None:
        self._forward()

    def do_HEAD(self) -> None:
        self._forward()

    def do_POST(self) -> None:
        self._forward()

    def do_PUT(self) -> None:
        self._forward()

    def _forward(self) -> None:
        transfer_encoding = self.headers.get("Transfer-Encoding", "").lower()
        if transfer_encoding and transfer_encoding != "identity":
            self.send_error(
                502,
                "counting proxy requires Content-Length; chunked S3 requests are unsupported",
            )
            return
        raw_length = self.headers.get("Content-Length")
        try:
            length = int(raw_length) if raw_length is not None else 0
        except ValueError:
            self.send_error(400, "invalid Content-Length")
            return
        backend = http.client.HTTPConnection(
            "127.0.0.1", self.server.backend_port, timeout=300
        )
        try:
            backend.putrequest(
                self.command,
                self.path,
                skip_host=True,
                skip_accept_encoding=True,
            )
            for name, value in self.headers.items():
                if name.lower() not in HOP_BY_HOP_HEADERS:
                    backend.putheader(name, value)
            backend.putheader("Connection", "close")
            backend.endheaders()
            remaining = length
            while remaining:
                chunk = self.rfile.read(min(1 << 20, remaining))
                if not chunk:
                    raise ConnectionError("client closed before request body completed")
                backend.send(chunk)
                remaining -= len(chunk)
            response = backend.getresponse()
            if self.command == "PUT" and 200 <= response.status < 300:
                self.server.record_put(self.path)
            elif self.command == "DELETE" and 200 <= response.status < 300:
                self.server.record_delete(self.path)
            self.send_response(response.status, response.reason)
            for name, value in response.getheaders():
                if name.lower() not in HOP_BY_HOP_HEADERS:
                    self.send_header(name, value)
            self.send_header("Connection", "close")
            self.end_headers()
            if self.command != "HEAD":
                while True:
                    chunk = response.read(1 << 20)
                    if not chunk:
                        break
                    self.wfile.write(chunk)
            self.close_connection = True
        except Exception as exc:
            if not self.wfile.closed:
                try:
                    self.send_error(502, f"S3 counting proxy failure: {exc}")
                except (BrokenPipeError, ConnectionError):
                    pass
        finally:
            backend.close()


@dataclasses.dataclass
class LiveConfig:
    repo_root: Path
    cargo_bin: Path
    nokv_bin: Path
    lingtai_kernel_dir: Path
    state_dir: Path
    server_port: int
    rustfs_port: int
    rustfs_console_port: int
    proxy_port: int
    rustfs_image: str
    bucket: str
    container: str
    profile: WorkloadProfile
    command_timeout: float
    tool_timeout: float
    startup_timeout: float
    gc_deadline: float
    build: bool
    keep_state: bool
    require_all: bool


class LiveEnvironment:
    def __init__(self, config: LiveConfig) -> None:
        self.config = config
        self.server_bind = f"127.0.0.1:{config.server_port}"
        self.rustfs_endpoint = f"http://127.0.0.1:{config.rustfs_port}"
        self.s3_endpoint = f"http://127.0.0.1:{config.proxy_port}"
        self.server_log = config.state_dir / "nokv-server.log"
        self.restore_barrier_dir = config.state_dir / "restore-barriers"
        self._server: subprocess.Popen[str] | None = None
        self._server_log_handle: Any = None
        self._container_started = False
        self._proxy: CountingProxyServer | None = None
        self._proxy_thread: threading.Thread | None = None
        self.nokv_binary_sha256 = ""
        self.repo_revision = ""
        self.lingtai_revision = ""

    @property
    def aws_env(self) -> dict[str, str]:
        env = os.environ.copy()
        env.update(
            {
                "AWS_ACCESS_KEY_ID": "rustfsadmin",
                "AWS_SECRET_ACCESS_KEY": "rustfsadmin",
                "AWS_DEFAULT_REGION": "us-east-1",
                "AWS_EC2_METADATA_DISABLED": "true",
                "NOKV_TEST_RESTORE_BARRIER_DIR": str(self.restore_barrier_dir),
                "NOKV_TEST_BARRIER_TIMEOUT_MS": str(
                    max(60_000, int(self.config.tool_timeout * 1000))
                ),
            }
        )
        return env

    def common_nokv_args(self) -> list[str]:
        return [
            "--server-bind",
            self.server_bind,
            "--object-backend",
            "rustfs",
            "--s3-endpoint",
            self.s3_endpoint,
            "--s3-bucket",
            self.config.bucket,
            "--s3-access-key-id",
            "rustfsadmin",
            "--s3-secret-access-key",
            "rustfsadmin",
            "--no-metadata-checkpoint-archive",
        ]

    def mcp_args(self, root: str) -> list[str]:
        return self.common_nokv_args() + [
            "mcp",
            "--profile",
            "workbench",
            "--workbench-root",
            root,
        ]

    def run(
        self,
        args: list[str],
        *,
        timeout: float | None = None,
        env: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        try:
            result = subprocess.run(
                args,
                cwd=self.config.repo_root,
                env=env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=timeout or self.config.command_timeout,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise AcceptanceError(f"command timed out: {args!r}") from exc
        if result.returncode != 0:
            raise AcceptanceError(
                f"command failed ({result.returncode}): {args!r}\n"
                f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
            )
        return result

    def build(self) -> None:
        if self.config.build:
            self.run(
                [
                    str(self.config.cargo_bin),
                    "build",
                    "-p",
                    "nokv",
                    "--bin",
                    "nokv",
                    "--target-dir",
                    str(self.config.repo_root / "target"),
                ],
                timeout=max(1200, self.config.startup_timeout),
            )
        if not self.config.nokv_bin.is_file():
            raise AcceptanceError(f"NoKV binary not found: {self.config.nokv_bin}")
        if not os.access(self.config.nokv_bin, os.X_OK):
            raise AcceptanceError(
                f"NoKV binary is not executable: {self.config.nokv_bin}"
            )
        self.nokv_binary_sha256 = sha256_file(self.config.nokv_bin)
        self.repo_revision = self.git_revision(self.config.repo_root)
        self.lingtai_revision = self.git_revision(self.config.lingtai_kernel_dir)

    def git_revision(self, directory: Path) -> str:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=directory,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=30,
            check=False,
        )
        if result.returncode != 0:
            raise AcceptanceError(
                f"failed to resolve git revision for {directory}: {result.stderr}"
            )
        revision = result.stdout.strip()
        if len(revision) != 40 or any(
            character not in "0123456789abcdef" for character in revision
        ):
            raise AcceptanceError(f"invalid git revision for {directory}: {revision!r}")
        return revision

    def assert_binary_unchanged(self) -> None:
        current = sha256_file(self.config.nokv_bin)
        if not self.nokv_binary_sha256 or current != self.nokv_binary_sha256:
            raise AcceptanceError(
                "NoKV binary changed during acceptance: "
                f"started={self.nokv_binary_sha256!r}, current={current!r}"
            )

    def start_rustfs(self) -> None:
        env = self.aws_env
        env.update(
            {
                "LINGTAI_WORKBENCH_DATA_ROOT": str(self.config.state_dir),
                "LINGTAI_WORKBENCH_RUSTFS_CONTAINER": self.config.container,
                "LINGTAI_WORKBENCH_RUSTFS_IMAGE": self.config.rustfs_image,
                "LINGTAI_WORKBENCH_RUSTFS_PORT": str(self.config.rustfs_port),
                "LINGTAI_WORKBENCH_RUSTFS_CONSOLE_PORT": str(
                    self.config.rustfs_console_port
                ),
                "LINGTAI_WORKBENCH_S3_ENDPOINT": self.rustfs_endpoint,
                "LINGTAI_WORKBENCH_S3_BUCKET": self.config.bucket,
                "LINGTAI_WORKBENCH_RUSTFS_DATA_DIR": str(
                    self.config.state_dir / "rustfs"
                ),
            }
        )
        self.run(
            [str(self.config.repo_root / "scripts/lingtai-workbench/start_rustfs.sh")],
            timeout=self.config.startup_timeout,
            env=env,
        )
        self._container_started = True

    def start_proxy(self) -> None:
        if self._proxy is not None:
            raise AcceptanceError("S3 proxy is already running")
        self._proxy = CountingProxyServer(
            ("127.0.0.1", self.config.proxy_port), self.config.rustfs_port
        )
        self._proxy_thread = threading.Thread(
            target=self._proxy.serve_forever, name="s3-counting-proxy", daemon=True
        )
        self._proxy_thread.start()
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            try:
                self.run(
                    [
                        "aws",
                        "--endpoint-url",
                        self.s3_endpoint,
                        "s3api",
                        "head-bucket",
                        "--bucket",
                        self.config.bucket,
                    ],
                    timeout=2,
                    env=self.aws_env,
                )
                return
            except AcceptanceError:
                time.sleep(0.05)
        raise AcceptanceError("S3 counting proxy did not become ready")

    def successful_puts(self) -> list[str]:
        if self._proxy is None:
            raise AcceptanceError("S3 proxy is not running")
        return self._proxy.put_records()

    def successful_mutations(self) -> list[ObjectMutation]:
        if self._proxy is None:
            raise AcceptanceError("S3 proxy is not running")
        return self._proxy.mutation_records()

    def start_server(self) -> None:
        if self._server is not None:
            raise AcceptanceError("NoKV server is already running")
        self._server_log_handle = self.server_log.open("a", encoding="utf-8")
        args = (
            [str(self.config.nokv_bin)]
            + self.common_nokv_args()
            + [
                "--meta",
                str(self.config.state_dir / "meta"),
                "--object-gc-interval-ms",
                str(BACKGROUND_GC_INTERVAL_MS),
                "--object-gc-limit",
                "4096",
                "--history-gc-interval-ms",
                str(BACKGROUND_GC_INTERVAL_MS),
                "--history-gc-limit",
                "4096",
                "serve",
            ]
        )
        self._server = subprocess.Popen(
            args,
            cwd=self.config.repo_root,
            env=self.aws_env,
            text=True,
            stdout=self._server_log_handle,
            stderr=subprocess.STDOUT,
        )
        deadline = time.monotonic() + self.config.startup_timeout
        while time.monotonic() < deadline:
            if self._server.poll() is not None:
                raise AcceptanceError(
                    f"NoKV server exited during startup\n{tail(self.server_log)}"
                )
            try:
                if self.http_text("/readyz", timeout=1).strip() == "ready":
                    return
            except (OSError, urllib.error.URLError):
                pass
            time.sleep(0.1)
        raise AcceptanceError(f"NoKV server startup timed out\n{tail(self.server_log)}")

    def stop_server(self, *, kill: bool = False) -> None:
        process = self._server
        self._server = None
        if process is not None and process.poll() is None:
            if kill:
                process.kill()
            else:
                process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        if self._server_log_handle is not None:
            self._server_log_handle.close()
            self._server_log_handle = None

    def cleanup(self) -> None:
        self.stop_server()
        if self._proxy is not None:
            self._proxy.shutdown()
            self._proxy.server_close()
            self._proxy = None
        if self._proxy_thread is not None:
            self._proxy_thread.join(timeout=5)
            self._proxy_thread = None
        if self._container_started or shutil.which("docker"):
            subprocess.run(
                ["docker", "rm", "-f", self.config.container],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=30,
                check=False,
            )
        if not self.config.keep_state:
            shutil.rmtree(self.config.state_dir, ignore_errors=True)

    def cli(self, *command: str) -> str:
        return self.run(
            [str(self.config.nokv_bin)] + self.common_nokv_args() + list(command),
            env=self.aws_env,
        ).stdout

    def hash_remote_file(self, path: str) -> tuple[str, int]:
        args = [str(self.config.nokv_bin)] + self.common_nokv_args() + ["cat", path]
        process = subprocess.Popen(
            args,
            cwd=self.config.repo_root,
            env=self.aws_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert process.stdout is not None
        digest = hashlib.sha256()
        size = 0
        deadline = time.monotonic() + max(self.config.command_timeout, 900)
        while True:
            if time.monotonic() >= deadline:
                process.kill()
                raise AcceptanceError(f"hashing {path} exceeded deadline")
            chunk = process.stdout.read(8 << 20)
            if not chunk:
                break
            digest.update(chunk)
            size += len(chunk)
        stderr = (
            process.stderr.read().decode("utf-8", errors="replace")
            if process.stderr
            else ""
        )
        if process.wait(timeout=10) != 0:
            raise AcceptanceError(f"cat {path} failed: {stderr}")
        return digest.hexdigest(), size

    def inventory(self) -> dict[str, ObjectFingerprint]:
        result = self.run(
            [
                "aws",
                "--endpoint-url",
                self.s3_endpoint,
                "s3api",
                "list-objects-v2",
                "--bucket",
                self.config.bucket,
                "--output",
                "json",
            ],
            env=self.aws_env,
        )
        payload = json.loads(result.stdout)
        inventory: dict[str, ObjectFingerprint] = {}
        for item in payload.get("Contents", []):
            inventory[item["Key"]] = ObjectFingerprint(
                size=int(item["Size"]),
                etag=str(item["ETag"]),
                last_modified=str(item["LastModified"]),
            )
        return inventory

    def http_text(self, path: str, *, timeout: float = 5, method: str = "GET") -> str:
        request = urllib.request.Request(
            f"http://{self.server_bind}{path}", method=method
        )
        started = time.monotonic()
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return response.read().decode("utf-8")
        except TimeoutError as error:
            elapsed = time.monotonic() - started
            raise AcceptanceError(
                f"NoKV {method} {path} timed out after {elapsed:.2f}s "
                f"(limit={timeout:.2f}s)"
            ) from error
        except urllib.error.HTTPError as error:
            body = error.read().decode("utf-8", errors="replace")
            raise AcceptanceError(
                f"NoKV {method} {path} returned HTTP {error.code}: {body}"
            ) from error

    def manual_gc(self, *, limit: int = 64) -> dict[str, Any]:
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 4096:
            raise AcceptanceError(f"manual GC limit is invalid: {limit!r}")
        raw = self.http_text(f"/gc?limit={limit}", timeout=60, method="POST")
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise AcceptanceError(f"manual GC returned non-object: {value!r}")
        return value

    def stats(self) -> dict[str, Any]:
        value = json.loads(self.http_text("/stats", timeout=60))
        if not isinstance(value, dict):
            raise AcceptanceError(f"stats returned non-object: {value!r}")
        return value

    def fsck(self) -> dict[str, Any]:
        deadline = time.monotonic() + min(
            self.config.tool_timeout, self.config.gc_deadline
        )
        while True:
            try:
                value = json.loads(self.http_text("/fsck", timeout=300))
            except AcceptanceError as exc:
                if (
                    RETRYABLE_FSCK_CONFLICT not in str(exc)
                    or time.monotonic() >= deadline
                ):
                    raise
                time.sleep(0.05)
                continue
            if not isinstance(value, dict):
                raise AcceptanceError(f"fsck returned non-object: {value!r}")
            return value

    def raw_mcp_tools(self, root: str) -> tuple[list[dict[str, Any]], str]:
        request = json.dumps(
            {"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
            separators=(",", ":"),
        )
        try:
            result = subprocess.run(
                [str(self.config.nokv_bin), *self.mcp_args(root)],
                cwd=self.config.repo_root,
                env=self.aws_env,
                input=request + "\n",
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=min(60, self.config.tool_timeout),
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise AcceptanceError("raw MCP tools/list preflight timed out") from exc
        if result.returncode != 0:
            raise AcceptanceError(
                "raw MCP tools/list preflight failed: "
                f"stdout={result.stdout!r}, stderr={result.stderr!r}"
            )
        lines = [line for line in result.stdout.splitlines() if line.strip()]
        if len(lines) != 1:
            raise AcceptanceError(
                f"raw MCP tools/list returned {len(lines)} responses: {lines!r}"
            )
        try:
            response = json.loads(lines[0])
        except json.JSONDecodeError as exc:
            raise AcceptanceError(
                f"raw MCP tools/list returned invalid JSON: {lines[0]!r}"
            ) from exc
        if (
            not isinstance(response, dict)
            or response.get("id") != 1
            or "error" in response
        ):
            raise AcceptanceError(f"raw MCP tools/list protocol failure: {response!r}")
        raw_tools = response.get("result", {}).get("tools")
        if not isinstance(raw_tools, list):
            raise AcceptanceError(f"raw MCP tools/list lacks tools: {response!r}")
        tools: list[dict[str, Any]] = []
        for raw in raw_tools:
            if not isinstance(raw, dict) or not isinstance(
                raw.get("inputSchema"), dict
            ):
                raise AcceptanceError(f"raw MCP tool lacks inputSchema: {raw!r}")
            tools.append(
                {
                    "name": raw.get("name"),
                    "description": raw.get("description"),
                    "schema": raw["inputSchema"],
                }
            )
        canonical = json.dumps(raw_tools, separators=(",", ":"), sort_keys=True)
        return tools, hashlib.sha256(canonical.encode()).hexdigest()


class WorkbenchClient:
    def __init__(self, client_class: type, launch: McpLaunch, timeout: float) -> None:
        self.root = launch.root
        self.timeout = timeout
        self._client = client_class(
            command=launch.command, args=launch.args, env=launch.env
        )
        self._client.start()

    def close(self) -> None:
        self._client.close()

    def tools(self) -> list[dict[str, Any]]:
        return self._client.list_tools(timeout=min(30, self.timeout))

    def raw_call(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        result = self._client.call_tool(name, arguments, timeout=self.timeout)
        if not isinstance(result, dict):
            raise AcceptanceError(f"{name} returned non-object: {result!r}")
        return result

    def call(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        result = self.raw_call(name, arguments)
        if result.get("status") == "error" or "code" in result:
            raise AcceptanceError(f"{name} failed: {result!r}")
        if result.get("status") != "success":
            raise AcceptanceError(f"{name} returned malformed success: {result!r}")
        return result


class AcceptanceSuite:
    def __init__(
        self, env: LiveEnvironment, agent_class: type, client_class: type
    ) -> None:
        self.env = env
        self.agent_class = agent_class
        self.client_class = client_class
        self.clients: list[WorkbenchClient] = []
        self.root = ""
        self.results: dict[str, dict[str, Any]] = {}
        self.source = "durable-source"
        self.destination = "durable-fork"
        self.moved = "durable-fork-moved"
        self.nested = "durable-nested"
        self.replaced = "durable-replaced"
        self.snapshot_id = 0
        self.snapshot_version = 0
        self.operation_id = ""
        self.nested_snapshot_id = 0
        self.nested_operation_id = ""
        self.expected_digest = ""
        self.fork_owned_keys: set[str] = set()
        self.large_object_keys: set[str] = set()
        self.restore_manifest_keys: set[str] = set()
        self.snapshot_deferred_release_keys: set[str] = set()
        self.initial_inventory: dict[str, ObjectFingerprint] = {}
        self.baseline_index: dict[str, Any] = {}
        self.mutated_index: dict[str, Any] = {}
        self.launch: McpLaunch | None = None
        self.fixture_block_count = 0
        self.fixture_marker_digest = ""
        self.raw_preflight_schema_sha256 = ""
        self.restore_control_baseline: dict[str, int] = {}
        self.registration_generation = 0
        self.barriers = RestoreBarrierController(env.restore_barrier_dir)
        self.barriers.clear()

    @property
    def client_a(self) -> WorkbenchClient:
        return self.clients[0]

    @property
    def client_b(self) -> WorkbenchClient:
        return self.clients[1]

    def scenario(self, name: str, function: Callable[[], dict[str, Any]]) -> None:
        print(f"[restore-live-e2e] START {name}", flush=True)
        started = time.monotonic()
        try:
            details = function()
        except Exception as exc:
            self.results[name] = {
                "status": "failed",
                "duration_seconds": round(time.monotonic() - started, 3),
                "details": {"error": str(exc)},
            }
            print(f"[restore-live-e2e] FAIL  {name}: {exc}", flush=True)
            raise
        self.results[name] = {
            "status": "passed",
            "duration_seconds": round(time.monotonic() - started, 3),
            "details": details,
        }
        print(f"[restore-live-e2e] PASS  {name}", flush=True)

    @staticmethod
    def mock_service() -> MagicMock:
        service = MagicMock()
        service.get_adapter.return_value = MagicMock()
        service.provider = "gemini"
        service.model = "nokv-restore-live-e2e"
        return service

    @staticmethod
    def resolved_launch(client: Any) -> McpLaunch:
        command = getattr(client, "_command", None)
        args = getattr(client, "_args", None)
        env = getattr(client, "_env", None)
        if not isinstance(command, str) or not isinstance(args, list):
            raise AcceptanceError("LingTai MCP client lacks resolved launch config")
        if not isinstance(env, dict):
            raise AcceptanceError("LingTai MCP client lacks resolved environment")
        try:
            root = args[args.index("--workbench-root") + 1]
        except (ValueError, IndexError) as exc:
            raise AcceptanceError("resolved MCP args lack workbench root") from exc
        if not isinstance(root, str) or not root.startswith("/") or "{" in root:
            raise AcceptanceError(f"LingTai left an unresolved root: {root!r}")
        return McpLaunch(command, list(args), dict(env), root)

    def registered_launch(self) -> McpLaunch:
        if not self.raw_preflight_schema_sha256:
            preflight_root = "/agents/deployment-preflight/wb"
            preflight_launch = McpLaunch(
                str(self.env.config.nokv_bin),
                self.env.mcp_args(preflight_root),
                self.env.aws_env,
                preflight_root,
            )
            self.validate_launch(preflight_launch)
            raw_tools, schema_digest = self.env.raw_mcp_tools(preflight_root)
            validate_tool_contract(raw_tools)
            self.raw_preflight_schema_sha256 = schema_digest
        agent_name = "durable-restore-agent"
        workdir = self.env.config.state_dir / "agents" / agent_name
        workdir.mkdir(parents=True, exist_ok=True)
        registry = {
            "name": "nokv-workbench",
            "summary": "NoKV durable restore live E2E.",
            "transport": "stdio",
            "command": str(self.env.config.nokv_bin),
            "args": self.env.mcp_args(WORKBENCH_ROOT_TEMPLATE),
            "source": "durable-restore-live-e2e",
        }
        (workdir / "mcp_registry.jsonl").write_text(
            json.dumps(registry, separators=(",", ":")) + "\n", encoding="utf-8"
        )
        (workdir / "init.json").write_text(
            json.dumps(
                {
                    "mcp": {
                        "nokv-workbench": {
                            "type": "stdio",
                            "command": str(self.env.config.nokv_bin),
                            "args": self.env.mcp_args(WORKBENCH_ROOT_TEMPLATE),
                            "env": self.env.aws_env,
                        }
                    }
                },
                separators=(",", ":"),
            ),
            encoding="utf-8",
        )
        agent: Any | None = None
        try:
            agent = self.agent_class(
                service=self.mock_service(),
                agent_name=agent_name,
                working_dir=workdir,
                capabilities={"mcp": {}},
            )
            spec = agent._mcp_init_specs.get("nokv-workbench")
            initial = spec.get("client") if isinstance(spec, dict) else None
            if initial is None:
                raise AcceptanceError("LingTai Agent did not register NoKV MCP")
            before = self.resolved_launch(initial)
            self.validate_launch(before)
            validate_tool_contract(initial.list_tools(timeout=30))
            initial.close()
            retry = agent._retry_failed_mcps()
            if retry.get("recovered") != ["nokv-workbench"]:
                raise AcceptanceError(f"Agent MCP reconnect failed: {retry!r}")
            retried = agent._mcp_init_specs["nokv-workbench"].get("client")
            if retried is None:
                raise AcceptanceError("Agent reconnect produced no MCP client")
            after = self.resolved_launch(retried)
            self.validate_launch(after)
            validate_tool_contract(retried.list_tools(timeout=30))
            if before.root != after.root:
                raise AcceptanceError("workbench root changed across Agent reconnect")
            handler = getattr(agent, "_tool_handlers", {}).get("workbench_create")
            if not callable(handler):
                raise AcceptanceError("Agent did not install workbench tool handler")
            self.registration_generation += 1
            probe = handler(
                {"id": f"agent-registration-probe-{self.registration_generation}"}
            )
            if not isinstance(probe, dict) or probe.get("status") != "success":
                raise AcceptanceError(f"Agent MCP handler failed: {probe!r}")
            if not callable(getattr(agent, "_tool_handlers", {}).get(RESTORE_TOOL)):
                raise AcceptanceError("Agent did not install workbench_restore handler")
            return after
        finally:
            if agent is not None:
                agent.stop(timeout=5.0)

    def validate_launch(self, launch: McpLaunch) -> None:
        expected_binary = self.env.config.nokv_bin.resolve()
        actual_binary = Path(launch.command).expanduser().resolve()
        if actual_binary != expected_binary:
            raise AcceptanceError(
                f"LingTai launched stale/wrong NoKV binary: {actual_binary} != {expected_binary}"
            )
        self.env.assert_binary_unchanged()
        if sha256_file(actual_binary) != self.env.nokv_binary_sha256:
            raise AcceptanceError(
                "LingTai launch binary hash differs from the built binary"
            )
        expected_args = self.env.mcp_args(launch.root)
        if launch.args != expected_args:
            raise AcceptanceError(
                "LingTai resolved MCP launch differs from deployment contract: "
                f"actual={launch.args!r}, expected={expected_args!r}"
            )
        if any("{" in argument or "}" in argument for argument in launch.args):
            raise AcceptanceError(
                f"LingTai launch retained a placeholder: {launch.args!r}"
            )
        for name, expected in self.env.aws_env.items():
            if (
                name.startswith(("AWS_", "NOKV_TEST_"))
                and launch.env.get(name) != expected
            ):
                raise AcceptanceError(
                    f"LingTai launch environment changed {name}: {launch.env.get(name)!r}"
                )

    def connect_clients(self) -> None:
        launch = self.registered_launch()
        if self.root and self.root != launch.root:
            raise AcceptanceError("Agent workbench root changed after restart")
        self.root = launch.root
        self.launch = launch
        self.clients = [
            WorkbenchClient(self.client_class, launch, self.env.config.tool_timeout),
            WorkbenchClient(self.client_class, launch, self.env.config.tool_timeout),
        ]

    def agent_restore_redrive(self, conflicting_snapshot_id: int) -> dict[str, Any]:
        """Use the Agent handler to redrive and preserve a native MCP error."""
        agent_name = "durable-restore-agent"
        workdir = self.env.config.state_dir / "agents" / agent_name
        request = self.restore_request(self.destination)
        canonical_request = json.dumps(request, separators=(",", ":"), sort_keys=True)
        agent: Any | None = None
        try:
            agent = self.agent_class(
                service=self.mock_service(),
                agent_name=agent_name,
                working_dir=workdir,
                capabilities={"mcp": {}},
            )
            initial_client = agent._mcp_init_specs["nokv-workbench"].get("client")
            if initial_client is None:
                raise AcceptanceError("Agent redrive has no initial NoKV MCP client")
            initial_launch = self.resolved_launch(initial_client)
            self.validate_launch(initial_launch)
            validate_tool_contract(initial_client.list_tools(timeout=30))
            handler = getattr(agent, "_tool_handlers", {}).get(RESTORE_TOOL)
            if not callable(handler):
                raise AcceptanceError("Agent redrive lacks workbench_restore handler")
            first = handler(dict(request))
            self.assert_complete_outcome(first, self.destination, self.operation_id)
            initial_client.close()
            retry = agent._retry_failed_mcps()
            if retry.get("recovered") != ["nokv-workbench"]:
                raise AcceptanceError(
                    f"Agent restore redrive reconnect failed: {retry!r}"
                )
            retried_client = agent._mcp_init_specs["nokv-workbench"].get("client")
            if retried_client is None:
                raise AcceptanceError("Agent restore redrive produced no MCP client")
            retried_launch = self.resolved_launch(retried_client)
            self.validate_launch(retried_launch)
            if retried_launch != initial_launch:
                raise AcceptanceError(
                    "Agent restore redrive changed the resolved MCP launch"
                )
            validate_tool_contract(retried_client.list_tools(timeout=30))
            retried_handler = getattr(agent, "_tool_handlers", {}).get(RESTORE_TOOL)
            if not callable(retried_handler):
                raise AcceptanceError(
                    "Agent lost workbench_restore handler after reconnect"
                )
            second_request = json.loads(canonical_request)
            if (
                json.dumps(second_request, separators=(",", ":"), sort_keys=True)
                != canonical_request
            ):
                raise AcceptanceError(
                    "Agent redrive changed the persisted restore request"
                )
            second = retried_handler(second_request)
            self.assert_complete_outcome(second, self.destination, self.operation_id)
            if first != second:
                raise AcceptanceError(
                    f"Agent exact terminal redrive changed outcome: {first!r} != {second!r}"
                )
            native_error = assert_native_tool_error(
                retried_handler(
                    {
                        "id": self.source,
                        "at_snapshot": conflicting_snapshot_id,
                        "destination_id": self.destination,
                    }
                ),
                "RestoreDestinationConflict",
            )
            return {
                "operation_id": self.operation_id,
                "snapshot_id": self.snapshot_id,
                "request": second_request,
                "reconnect": retry,
                "native_structured_error": {
                    "code": native_error.code,
                    "message": native_error.message,
                    "retryable": native_error.retryable,
                    "details": native_error.details,
                },
            }
        finally:
            if agent is not None:
                agent.stop(timeout=5.0)

    def close_clients(self) -> None:
        for client in self.clients:
            try:
                client.close()
            except Exception:
                pass
        self.clients = []

    def reconnect_after_server_kill(self) -> None:
        self.env.stop_server(kill=True)
        self.close_clients()
        self.env.start_server()
        self.connect_clients()

    def absolute(
        self, workbench_id: str, section: str | None = None, path: str = ""
    ) -> str:
        value = f"{self.root}/{workbench_id}"
        if section is not None:
            value += f"/{section}"
        if path:
            value += f"/{path}"
        return value

    def restore_request(self, destination_id: str) -> dict[str, Any]:
        return {
            "id": self.source,
            "at_snapshot": self.snapshot_id,
            "destination_id": destination_id,
        }

    def expected_restore_operation_id(self, destination_id: str) -> str:
        return restore_operation_id(
            DEFAULT_LIVE_MOUNT_ID,
            self.absolute(self.source),
            self.snapshot_id,
            self.absolute(destination_id),
        )

    def assert_complete_outcome(
        self,
        outcome: dict[str, Any],
        destination_id: str,
        operation_id: str,
    ) -> None:
        if (
            outcome.get("status") != "success"
            or outcome.get("state") != "complete"
            or outcome.get("operation_id") != operation_id
            or outcome.get("source_workbench_id") != self.source
            or outcome.get("destination_workbench_id") != destination_id
            or outcome.get("snapshot_id") != self.snapshot_id
            or outcome.get("read_version") != self.snapshot_version
            or outcome.get("cleanup_pending") is not False
            or not isinstance(outcome.get("source_root"), int)
            or not isinstance(outcome.get("destination_root"), int)
        ):
            raise AcceptanceError(f"malformed durable restore outcome: {outcome!r}")

    def retry_restore_until_complete(
        self, destination_id: str, operation_id: str
    ) -> tuple[dict[str, Any], int]:
        request = self.restore_request(destination_id)
        deadline = time.monotonic() + self.env.config.tool_timeout
        attempts = 0
        while time.monotonic() < deadline:
            attempts += 1
            outcome = self.client_a.raw_call(RESTORE_TOOL, request)
            if outcome.get("status") == "success":
                self.assert_complete_outcome(outcome, destination_id, operation_id)
                return outcome, attempts
            error = decode_tool_error(outcome)
            if error.code != "RestoreInProgress" or not error.retryable:
                raise AcceptanceError(
                    "exact restore retry returned a non-retryable intermediate error: "
                    f"{outcome!r}"
                )
            time.sleep(0.02)
        raise AcceptanceError(
            f"exact restore retry did not complete before deadline: {operation_id}"
        )

    def wait_for_restore_error(
        self, destination_id: str, expected_code: str
    ) -> tuple[ToolError, int]:
        request = self.restore_request(destination_id)
        deadline = time.monotonic() + self.env.config.tool_timeout
        attempts = 0
        release_drained = False
        while time.monotonic() < deadline:
            attempts += 1
            outcome = self.client_a.raw_call(RESTORE_TOOL, request)
            if outcome.get("status") == "success":
                raise AcceptanceError(
                    "released restore operation unexpectedly remained terminal: "
                    f"{outcome!r}"
                )
            error = decode_tool_error(outcome)
            if error.code == expected_code:
                return error, attempts
            if error.code != "RestoreInProgress" or not error.retryable:
                raise AcceptanceError(
                    f"waiting for {expected_code} observed {outcome!r}"
                )
            if release_drained:
                raise AcceptanceError(
                    "restore remained in progress after the release backlog drained: "
                    f"{outcome!r}"
                )
            # The release worker is deliberately paged and passive workers stay
            # outside this acceptance window. A full restore subtree can
            # require hundreds of durable member/index subpages, so drive one
            # bounded worker page per exact retry.
            gc_outcome = self.env.manual_gc(limit=64)
            object_gc = gc_outcome.get("object_gc")
            if not isinstance(object_gc, dict) or any(
                not isinstance(object_gc.get(field), int)
                for field in (
                    "restore_release_jobs_processed",
                    "restore_release_backlog",
                    "restore_release_quarantine",
                    "restore_release_mount_wide_quarantine",
                )
            ):
                raise AcceptanceError(
                    f"manual release GC returned malformed evidence: {gc_outcome!r}"
                )
            if (
                object_gc["restore_release_quarantine"] != 0
                or object_gc["restore_release_mount_wide_quarantine"] != 0
            ):
                raise AcceptanceError(
                    f"manual release GC quarantined durable state: {gc_outcome!r}"
                )
            release_drained = object_gc["restore_release_backlog"] == 0
            time.sleep(0.02)
        raise AcceptanceError(
            f"restore did not converge to {expected_code}: {destination_id}"
        )

    def crash_at_restore_barrier(
        self,
        operation_id: str,
        phase: str,
        operation: Callable[[], Any],
        *,
        require_interrupted_result: bool,
        allow_missing_phase: bool = False,
    ) -> dict[str, Any]:
        handle = self.barriers.handle(operation_id, phase)
        handle.arm()
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        future = executor.submit(operation)
        server_crashed = False
        try:
            try:
                reached = handle.wait_ready_or_done(
                    future, self.env.config.tool_timeout
                )
            except Exception as barrier_error:
                handle.release()
                try:
                    observation: Any = future.result(timeout=30)
                except Exception as operation_error:
                    observation = f"{type(operation_error).__name__}: {operation_error}"
                raise AcceptanceError(
                    f"restore crash phase {phase!r} was not reached; "
                    f"operation observation={observation!r}"
                ) from barrier_error

            if not reached:
                result = future.result(timeout=30)
                if not allow_missing_phase:
                    raise AcceptanceError(
                        f"restore crash phase {phase!r} was not reached; "
                        f"operation completed as {result!r}"
                    )
                return {"kind": "phase-absent", "value": result}

            self.env.stop_server(kill=True)
            server_crashed = True
            handle.disarm_after_crash()
            try:
                result = future.result(timeout=min(30, self.env.config.tool_timeout))
                observation = {"kind": "result", "value": result}
            except concurrent.futures.TimeoutError:
                self.close_clients()
                try:
                    result = future.result(timeout=10)
                    observation = {"kind": "result", "value": result}
                except Exception as exc:
                    observation = {
                        "kind": "exception",
                        "value": f"{type(exc).__name__}: {exc}",
                    }
            except Exception as exc:
                observation = {
                    "kind": "exception",
                    "value": f"{type(exc).__name__}: {exc}",
                }
        finally:
            handle.disarm_after_crash()
            executor.shutdown(wait=True, cancel_futures=True)

        if server_crashed:
            self.close_clients()
            self.env.start_server()
            self.connect_clients()
        value = observation.get("value")
        if (
            require_interrupted_result
            and observation.get("kind") == "result"
            and isinstance(value, dict)
            and value.get("status") == "success"
        ):
            raise AcceptanceError(
                f"restore crash phase {phase!r} returned success before SIGKILL"
            )
        return observation

    def put_text(
        self,
        workbench_id: str,
        path: str,
        text: str,
        *,
        content_type: str = "text/plain",
        replace: bool = False,
    ) -> dict[str, Any]:
        return self.client_a.call(
            "workbench_put_file",
            {
                "id": workbench_id,
                "section": "outputs",
                "path": path,
                "text": text,
                "content_type": content_type,
                "replace": replace,
            },
        )

    def create_large_fixture(self) -> Path:
        path = self.env.config.state_dir / "cow-fixture.bin"
        size = self.env.config.profile.large_bytes
        self.fixture_block_count = fixture_block_count(size)
        marker_digest = hashlib.sha256()
        with path.open("wb") as output:
            for index in range(self.fixture_block_count):
                marker = fixture_block_marker(index)
                marker_digest.update(marker)
                output.seek(index * COW_BLOCK_BYTES)
                output.write(marker)
            output.truncate(size)
        self.fixture_marker_digest = marker_digest.hexdigest()
        self.expected_digest = sha256_file(path)
        return path

    def index_signature(
        self, workbench_id: str, client: WorkbenchClient | None = None
    ) -> dict[str, Any]:
        client = client or self.client_a
        search_arguments: dict[str, Any] = {
            "id": workbench_id,
            "section": "outputs",
            "predicates": [{"field": "name", "op": "suffix", "value": ".csv"}],
            "fields": ["name", "body.content_type"],
            "limit": INDEX_PAGE_LIMIT,
        }
        matches: list[dict[str, Any]] = []
        cursor: str | None = None
        pages = 0
        while True:
            if cursor is not None:
                search_arguments["cursor"] = cursor
            search = client.call("workbench_search", search_arguments)
            page_matches = search.get("matches")
            if not isinstance(page_matches, list):
                raise AcceptanceError(f"search lacks matches: {search!r}")
            for match_ in page_matches:
                if not isinstance(match_, dict):
                    raise AcceptanceError(
                        f"search returned malformed match: {match_!r}"
                    )
                matches.append(match_)
            pages += 1
            if search.get("truncated") is not True:
                if search.get("next_cursor") is not None:
                    raise AcceptanceError(
                        "untruncated search unexpectedly returned a cursor"
                    )
                break
            cursor = search.get("next_cursor")
            if not isinstance(cursor, str) or not cursor:
                raise AcceptanceError("truncated search lacks next_cursor")
            if pages > self.env.config.profile.indexed_files + 16:
                raise AcceptanceError("search pagination did not converge")
        paths = [match_.get("path") for match_ in matches]
        if not all(isinstance(path, str) for path in paths) or len(set(paths)) != len(
            paths
        ):
            raise AcceptanceError(
                f"search returned duplicate/malformed paths: {paths!r}"
            )
        relative_paths = [match_.get("relative_path") for match_ in matches]
        if not all(isinstance(path, str) for path in relative_paths) or len(
            set(relative_paths)
        ) != len(relative_paths):
            raise AcceptanceError(
                f"search returned duplicate/malformed relative paths: {relative_paths!r}"
            )
        for match_ in matches:
            if (
                match_.get("workbench_id") != workbench_id
                or match_.get("section") != "outputs"
            ):
                raise AcceptanceError(f"search escaped workbench scope: {match_!r}")
        aggregate = client.call(
            "workbench_aggregate",
            {
                "id": workbench_id,
                "section": "outputs",
                "predicates": [{"field": "kind", "op": "eq", "value": "file"}],
                "measures": [{"name": "files", "op": "count"}],
            },
        )
        catalog = client.call("workbench_catalog", {"id": workbench_id})
        groups = aggregate.get("groups")
        if not isinstance(groups, list) or not groups:
            raise AcceptanceError(f"aggregate returned no groups: {aggregate!r}")
        filterable = catalog.get("catalog", {}).get("filterable")
        if not isinstance(filterable, list):
            raise AcceptanceError(f"catalog lacks filterable fields: {catalog!r}")
        fields = sorted(
            field
            for group in filterable
            for field in group.get("fields", [])
            if isinstance(field, str)
        )
        return {
            "csv_match_count": len(matches),
            "csv_names": sorted(
                match_.get("values", {}).get("name")
                for match_ in matches
                if isinstance(match_.get("values", {}).get("name"), str)
            ),
            "csv_paths": sorted(
                path for path in relative_paths if isinstance(path, str)
            ),
            "search_pages": pages,
            "file_count": groups[0].get("values", {}).get("files"),
            "catalog_fields": fields,
        }

    def assert_manifest(
        self,
        workbench_id: str,
        operation_id: str,
        source: str,
        snapshot_id: int,
        client: WorkbenchClient | None = None,
    ) -> None:
        client = client or self.client_b
        checkpoints = client.call("workbench_snapshot_list", {"id": workbench_id})
        if checkpoints.get("checkpoint_count") != 0:
            raise AcceptanceError("restored workbench inherited checkpoint aliases")
        inherited = client.raw_call(
            "workbench_stat",
            {
                "id": workbench_id,
                "section": "metadata",
                "path": "checkpoints.jsonl",
            },
        )
        assert_tool_error(inherited, "inherited checkpoint registry")
        manifest = read_json_object(
            client.call(
                "workbench_read",
                {
                    "id": workbench_id,
                    "section": "metadata",
                    "path": "restore_manifest.json",
                },
            )
        )
        validate_restore_manifest(
            manifest,
            operation_id=operation_id,
            source_workbench_id=source,
            source_path=self.absolute(source),
            destination_workbench_id=workbench_id,
            destination_path=self.absolute(workbench_id),
            snapshot_id=snapshot_id,
        )

    def scenario_contract_and_fixture(self) -> dict[str, Any]:
        validate_tool_contract(self.client_a.tools())
        validate_tool_contract(self.client_b.tools())
        self.initial_inventory = self.env.inventory()
        if self.initial_inventory:
            raise AcceptanceError(
                "isolated RustFS bucket is not initially empty: "
                f"{sorted(self.initial_inventory)!r}"
            )
        before_source = dict(self.initial_inventory)
        self.client_a.call("workbench_create", {"id": self.source})
        for index in range(self.env.config.profile.indexed_files):
            self.put_text(
                self.source,
                f"indexed-{index:03d}.csv",
                f"index,value\n{index},{index * 2}\n",
                content_type="text/csv",
            )
        self.put_text(
            self.source,
            "state.json",
            json.dumps({"state": "checkpoint", "rank": 7}),
            content_type="application/json",
        )
        before_large = self.env.inventory()
        large = self.create_large_fixture()
        self.env.cli(
            "put-artifact",
            self.absolute(self.source, "outputs", "cow-large.bin"),
            str(large),
        )
        after_large = self.env.inventory()
        self.large_object_keys = set(after_large) - set(before_large)
        if len(self.large_object_keys) != self.fixture_block_count:
            raise AcceptanceError(
                "large fixture did not upload exactly one object per 4 MiB block: "
                f"expected={self.fixture_block_count}, keys={sorted(self.large_object_keys)!r}"
            )
        wrong_block_sizes = {
            key: after_large[key].size
            for key in self.large_object_keys
            if after_large[key].size != COW_BLOCK_BYTES
        }
        if wrong_block_sizes:
            raise AcceptanceError(
                f"large fixture block objects have wrong sizes: {wrong_block_sizes!r}"
            )
        if (
            self.env.config.profile.name == "full"
            and self.fixture_block_count != FULL_COW_BLOCK_COUNT
        ):
            raise AcceptanceError(
                f"full COW fixture has {self.fixture_block_count} blocks, expected 256"
            )
        self.client_a.call(
            "workbench_commit",
            {"id": self.source, "manifest": {"acceptance": "durable-restore"}},
        )
        after_source = self.env.inventory()
        self.fork_owned_keys = set(after_source) - set(before_source)
        if not self.fork_owned_keys:
            raise AcceptanceError("source fixture uploaded no RustFS objects")
        snapshot = self.client_a.call(
            "workbench_snapshot",
            {"id": self.source, "name": "durable-point", "ttl_days": 7},
        )
        self.snapshot_id = int(snapshot["snapshot_id"])
        self.snapshot_version = int(snapshot["read_version"])
        self.baseline_index = self.index_signature(self.source)
        expected_csv_names = [
            f"indexed-{index:03d}.csv"
            for index in range(self.env.config.profile.indexed_files)
        ]
        if (
            self.baseline_index["csv_match_count"]
            != self.env.config.profile.indexed_files
            or self.baseline_index["csv_names"] != expected_csv_names
            or len(self.baseline_index["csv_paths"]) != len(expected_csv_names)
        ):
            raise AcceptanceError(
                f"source search index incomplete: {self.baseline_index!r}"
            )
        if (
            self.env.config.profile.name == "full"
            and self.baseline_index["search_pages"] < 2
        ):
            raise AcceptanceError("full source index did not exercise pagination")

        invalid_requests = [
            {"id": self.source, "at_snapshot": None, "destination_id": "invalid-null"},
            {"id": self.source, "at_snapshot": "", "destination_id": "invalid-empty"},
            {
                "id": self.source,
                "at_snapshot": -1,
                "destination_id": "invalid-negative",
            },
            {
                "id": self.source,
                "at_snapshot": self.snapshot_id,
                "destination_id": "invalid-extra",
                "extra": True,
            },
            {
                "id": self.source,
                "at_snapshot": self.snapshot_id,
                "destination_id": self.source,
            },
        ]
        for request in invalid_requests:
            assert_tool_error(
                self.client_a.raw_call(RESTORE_TOOL, request),
                f"strict schema {request!r}",
            )

        self.put_text(
            self.source,
            "indexed-000.csv",
            "index,value\n0,current\n",
            content_type="text/csv",
            replace=True,
        )
        self.env.cli("rm", self.absolute(self.source, "outputs", "indexed-001.csv"))
        self.put_text(self.source, "current-only.txt", "not in checkpoint\n")
        return {
            "tool_count": len(self.client_a.tools()),
            "snapshot_id": self.snapshot_id,
            "fixture_bytes": self.env.config.profile.large_bytes,
            "fixture_4mib_blocks": self.fixture_block_count,
            "fixture_marker_digest": self.fixture_marker_digest,
            "source_object_keys": len(self.fork_owned_keys),
            "nokv_binary_sha256": self.env.nokv_binary_sha256,
            "nokv_revision": self.env.repo_revision,
            "lingtai_revision": self.env.lingtai_revision,
            "raw_tools_schema_sha256": self.raw_preflight_schema_sha256,
        }

    def exercise_create_crash_phase(
        self, phase: str, destination: str, *, allow_missing_phase: bool
    ) -> dict[str, Any] | None:
        operation_id = self.expected_restore_operation_id(destination)
        request = self.restore_request(destination)
        inventory_before = self.env.inventory()
        puts_before = len(self.env.successful_puts())
        mutations_before = len(self.env.successful_mutations())
        observation = self.crash_at_restore_barrier(
            operation_id,
            phase,
            lambda request=request: self.client_a.raw_call(RESTORE_TOOL, request),
            require_interrupted_result=True,
            allow_missing_phase=allow_missing_phase,
        )
        if observation["kind"] == "phase-absent":
            completed = observation["value"]
            self.assert_complete_outcome(completed, destination, operation_id)
            changed = changed_objects(inventory_before, self.env.inventory())
            put_records = self.env.successful_puts()[puts_before:]
            if len(changed) != 1 or len(put_records) != 1:
                raise AcceptanceError(
                    f"batch discovery probe for {phase!r} was not a clean COW restore: "
                    f"changed={sorted(changed)!r}, puts={put_records!r}"
                )
            self.delete_workbench(self.client_a, destination)
            self.record_snapshot_deferred_release(changed)
            return None

        inventory_after_crash = self.env.inventory()
        crash_created = set(inventory_after_crash) - set(inventory_before)
        requires_rebuild = restore_phase_requires_manifest_rebuild(phase)
        if requires_rebuild and len(crash_created) != 1:
            raise AcceptanceError(
                f"post-manifest crash at {phase!r} must expose one old incarnation: "
                f"{sorted(crash_created)!r}"
            )
        if not requires_rebuild and phase != "attach-applied" and crash_created:
            raise AcceptanceError(
                f"pre-manifest crash at {phase!r} left object mutations: "
                f"{sorted(crash_created)!r}"
            )
        if phase == "attach-applied" and len(crash_created) != 1:
            raise AcceptanceError(
                "post-attach crash must retain exactly the committed manifest: "
                f"{sorted(crash_created)!r}"
            )

        outcome, retry_attempts = self.retry_restore_until_complete(
            destination, operation_id
        )
        self.assert_manifest(destination, operation_id, self.source, self.snapshot_id)
        if self.index_signature(destination) != self.baseline_index:
            raise AcceptanceError(
                f"crash recovery at {phase!r} attached an incomplete index"
            )

        inventory_after = self.env.inventory()
        changed = changed_objects(inventory_before, inventory_after)
        put_records = self.env.successful_puts()[puts_before:]
        expected_puts = 2 if requires_rebuild else 1
        if len(changed) != 1 or len(put_records) != expected_puts:
            raise AcceptanceError(
                f"crash recovery at {phase!r} published the wrong manifest count: "
                f"changed={sorted(changed)!r}, puts={put_records!r}"
            )
        if changed & self.fork_owned_keys:
            raise AcceptanceError(
                f"crash recovery at {phase!r} copied/overwrote source blocks"
            )

        orphan_cleanup_sequence: list[str] = []
        if requires_rebuild:
            if len(set(put_records)) != 2:
                raise AcceptanceError(
                    f"recovery at {phase!r} reused the old incarnation key: {put_records!r}"
                )
            orphan_target, rebuilt_target = put_records
            orphan_key = next(iter(crash_created))
            rebuilt_key = next(iter(changed))
            if not orphan_target.endswith(f"/{orphan_key}"):
                raise AcceptanceError(
                    "counted old PUT does not match crash inventory: "
                    f"target={orphan_target!r}, key={orphan_key!r}"
                )
            if not rebuilt_target.endswith(f"/{rebuilt_key}"):
                raise AcceptanceError(
                    "counted rebuilt PUT does not match final inventory: "
                    f"target={rebuilt_target!r}, key={rebuilt_key!r}"
                )
            relevant = [
                mutation
                for mutation in self.env.successful_mutations()[mutations_before:]
                if mutation.target in {orphan_target, rebuilt_target}
            ]
            expected = [
                ObjectMutation("PUT", orphan_target),
                ObjectMutation("DELETE", orphan_target),
                ObjectMutation("PUT", rebuilt_target),
            ]
            positions: list[int] = []
            start = 0
            for expected_mutation in expected:
                position = next(
                    (
                        index
                        for index in range(start, len(relevant))
                        if relevant[index] == expected_mutation
                    ),
                    None,
                )
                if position is None:
                    raise AcceptanceError(
                        f"old PUT was not deleted before rebuild at {phase!r}: {relevant!r}"
                    )
                positions.append(position)
                start = position + 1
            orphan_cleanup_sequence = ["PUT(old)", "DELETE(old)", "PUT(new)"]

        self.delete_workbench(self.client_a, destination)
        release_gc = self.record_snapshot_deferred_release(changed)
        return {
            "phase": phase,
            "operation_id": outcome["operation_id"],
            "destination_root": outcome["destination_root"],
            "interruption": observation["kind"],
            "retry_attempts": retry_attempts,
            "manifest_puts": len(put_records),
            "orphan_cleanup_sequence": orphan_cleanup_sequence,
            "release_gc": release_gc,
        }

    def discover_create_batch_crashes(
        self, kind: str, records: list[dict[str, Any]]
    ) -> int:
        for index in range(MAX_DISCOVERED_BATCH_PHASES):
            phase = indexed_restore_phase(kind, index)
            destination = f"crash-{kind}-{index:06d}"
            record = self.exercise_create_crash_phase(
                phase, destination, allow_missing_phase=True
            )
            if record is None:
                if index < 2:
                    raise AcceptanceError(
                        f"full fixture exercised only {index} {kind} batch(es)"
                    )
                return index
            records.append(record)
        raise AcceptanceError(
            f"{kind} batch discovery exceeded {MAX_DISCOVERED_BATCH_PHASES} phases"
        )

    def scenario_create_crash_matrix(self) -> dict[str, Any]:
        if self.env.config.profile.name != "full":
            raise AcceptanceError("create crash matrix requires the full profile")
        if self.env.config.profile.indexed_files <= 64:
            raise AcceptanceError(
                "full crash fixture must cross the 64-entry materialization/ref batch"
            )

        records: list[dict[str, Any]] = []
        hold = self.exercise_create_crash_phase(
            "hold-applied", "crash-hold", allow_missing_phase=False
        )
        assert hold is not None
        records.append(hold)
        materialization_batches = self.discover_create_batch_crashes(
            "materialize", records
        )
        for phase, suffix in (
            ("initialization-put-before-000000", "init-before"),
            ("initialization-put-after-000000", "init-after"),
            ("index-sealed", "index-sealed"),
        ):
            record = self.exercise_create_crash_phase(
                phase, f"crash-{suffix}", allow_missing_phase=False
            )
            assert record is not None
            records.append(record)
        reference_batches = self.discover_create_batch_crashes("reference", records)
        for phase, suffix in (
            ("references-sealed", "references-sealed"),
            ("attach-applied", "attach"),
        ):
            record = self.exercise_create_crash_phase(
                phase, f"crash-{suffix}", allow_missing_phase=False
            )
            assert record is not None
            records.append(record)
        expected_phases = create_crash_phases(
            materialization_batches, reference_batches
        )
        observed_phases = tuple(record["phase"] for record in records)
        if observed_phases != expected_phases:
            raise AcceptanceError(
                "dynamic crash matrix is incomplete or out of order: "
                f"expected={expected_phases!r}, observed={observed_phases!r}"
            )
        released_graph = self.private_graph_evidence(
            expected_complete=0,
            expected_snapshot_pins=1,
            expect_empty=True,
        )
        self.restore_control_baseline = dict(released_graph["control_rows"])
        return {
            "phase_count": len(records),
            "materialization_batches": materialization_batches,
            "reference_batches": reference_batches,
            "phases": records,
            "released_graph": released_graph,
            "durable_ledger_baseline": self.restore_control_baseline,
        }

    def scenario_cleanup_release_crash_recovery(self) -> dict[str, Any]:
        if self.env.config.profile.name != "full":
            raise AcceptanceError(
                "cleanup/release crash recovery requires full profile"
            )

        cleanup_destination = "crash-cleanup"
        cleanup_operation_id = self.expected_restore_operation_id(cleanup_destination)
        cleanup_request = self.restore_request(cleanup_destination)
        cleanup_inventory_before = self.env.inventory()
        cleanup_puts_before = len(self.env.successful_puts())
        preparing_observation = self.crash_at_restore_barrier(
            cleanup_operation_id,
            "materialize-batch-000000",
            lambda: self.client_a.raw_call(RESTORE_TOOL, cleanup_request),
            require_interrupted_result=True,
        )
        cleanup_observation = self.crash_at_restore_barrier(
            cleanup_operation_id,
            CLEANUP_CRASH_PHASE,
            lambda: self.client_a.raw_call(RESTORE_TOOL, cleanup_request),
            require_interrupted_result=True,
        )
        cleanup_outcome, cleanup_retry_attempts = self.retry_restore_until_complete(
            cleanup_destination, cleanup_operation_id
        )
        self.assert_manifest(
            cleanup_destination,
            cleanup_operation_id,
            self.source,
            self.snapshot_id,
        )
        if self.index_signature(cleanup_destination) != self.baseline_index:
            raise AcceptanceError("cleanup/rebuild restored an incomplete index")
        cleanup_changed = changed_objects(
            cleanup_inventory_before, self.env.inventory()
        )
        cleanup_puts = self.env.successful_puts()[cleanup_puts_before:]
        if len(cleanup_changed) != 1 or len(cleanup_puts) != 1:
            raise AcceptanceError(
                "cleanup/rebuild must publish one deterministic manifest: "
                f"changed={sorted(cleanup_changed)!r}, puts={cleanup_puts!r}"
            )
        self.delete_workbench(self.client_a, cleanup_destination)
        cleanup_release_gc = self.record_snapshot_deferred_release(cleanup_changed)

        replacement = "crash-release-replacement"
        release_destination = "crash-release"
        replacement_inventory_before = self.env.inventory()
        self.client_a.call("workbench_create", {"id": replacement})
        self.put_text(replacement, "replacement.txt", "replacement survived\n")
        self.client_a.call(
            "workbench_commit",
            {"id": replacement, "manifest": {"release_crash": True}},
        )
        replacement_keys = set(self.env.inventory()) - set(replacement_inventory_before)
        if not replacement_keys:
            raise AcceptanceError("release replacement uploaded no RustFS objects")

        release_inventory_before = self.env.inventory()
        release_puts_before = len(self.env.successful_puts())
        release_outcome = self.client_a.call(
            RESTORE_TOOL, self.restore_request(release_destination)
        )
        release_operation_id = self.expected_restore_operation_id(release_destination)
        self.assert_complete_outcome(
            release_outcome, release_destination, release_operation_id
        )
        self.assert_manifest(
            release_destination,
            release_operation_id,
            self.source,
            self.snapshot_id,
        )
        release_changed = changed_objects(
            release_inventory_before, self.env.inventory()
        )
        release_puts = self.env.successful_puts()[release_puts_before:]
        if len(release_changed) != 1 or len(release_puts) != 1:
            raise AcceptanceError(
                "release crash setup must publish one manifest: "
                f"changed={sorted(release_changed)!r}, puts={release_puts!r}"
            )

        def replace_and_drive_release_worker() -> str:
            result = self.env.cli(
                "rename-replace",
                self.absolute(replacement),
                self.absolute(release_destination),
            )
            # Root replacement only commits the Releasing transition. Drive
            # the asynchronous worker explicitly so the release-batch barrier
            # cannot lose a race to the CLI returning between background ticks.
            self.env.manual_gc()
            return result

        release_observation = self.crash_at_restore_barrier(
            release_operation_id,
            RELEASE_CRASH_PHASE,
            replace_and_drive_release_worker,
            require_interrupted_result=False,
        )
        replacement_read = self.client_b.call(
            "workbench_read",
            {
                "id": release_destination,
                "section": "outputs",
                "path": "replacement.txt",
            },
        )
        replacement_items = replacement_read.get("items")
        replacement_text = (
            replacement_items[0].get("value", {}).get("text")
            if isinstance(replacement_items, list) and replacement_items
            else None
        )
        if replacement_text != "replacement survived":
            raise AcceptanceError(
                "rename-replace was not durable across the release crash"
            )
        release_error, release_error_attempts = self.wait_for_restore_error(
            release_destination, "RestoreDestinationConflict"
        )
        release_gc = self.record_snapshot_deferred_release(release_changed)
        self.delete_workbench(self.client_a, release_destination)
        replacement_gc = self.record_snapshot_deferred_release(replacement_keys)
        released_graph = self.private_graph_evidence(
            expected_complete=0,
            expected_snapshot_pins=1,
            expect_empty=True,
        )
        self.restore_control_baseline = dict(released_graph["control_rows"])
        return {
            "cleanup_operation_id": cleanup_outcome["operation_id"],
            "preparing_interruption": preparing_observation["kind"],
            "cleanup_interruption": cleanup_observation["kind"],
            "cleanup_retry_attempts": cleanup_retry_attempts,
            "cleanup_manifest_puts": len(cleanup_puts),
            "cleanup_release_gc": cleanup_release_gc,
            "release_operation_id": release_operation_id,
            "release_interruption": release_observation["kind"],
            "release_terminal_code": release_error.code,
            "release_error_attempts": release_error_attempts,
            "release_worker_gc_calls": release_error_attempts - 1,
            "release_manifest_puts": len(release_puts),
            "release_gc": release_gc,
            "replacement_gc": replacement_gc,
            "released_graph": released_graph,
            "durable_ledger_baseline": self.restore_control_baseline,
        }

    def scenario_concurrent_restore(self) -> dict[str, Any]:
        if self.launch is None:
            raise AcceptanceError(
                "concurrent restore lacks an Agent-resolved MCP launch"
            )
        request = {
            "id": self.source,
            "at_snapshot": self.snapshot_id,
            "destination_id": self.destination,
        }
        operation_id = self.expected_restore_operation_id(self.destination)
        inventory_before = self.env.inventory()
        puts_before = len(self.env.successful_puts())
        start_barrier = threading.Barrier(CONCURRENT_RESTORE_CALLS + 1)
        hold = self.barriers.handle(operation_id, "references-sealed")
        hold.arm()
        dedicated_clients: list[WorkbenchClient] = []
        futures: list[concurrent.futures.Future[tuple[dict[str, Any], int]]] = []

        def restore(client: WorkbenchClient) -> tuple[dict[str, Any], int]:
            start_barrier.wait(timeout=self.env.config.tool_timeout)
            deadline = time.monotonic() + self.env.config.tool_timeout
            attempts = 0
            while time.monotonic() < deadline:
                attempts += 1
                outcome = client.raw_call(RESTORE_TOOL, dict(request))
                if outcome.get("status") == "success":
                    self.assert_complete_outcome(
                        outcome, self.destination, operation_id
                    )
                    return outcome, attempts
                error = decode_tool_error(outcome)
                if error.code != "RestoreInProgress" or not error.retryable:
                    raise AcceptanceError(
                        f"concurrent exact restore returned {outcome!r}"
                    )
                time.sleep(0.002)
            raise AcceptanceError("concurrent exact restore retry deadline expired")

        def validate_first_visible(observer: WorkbenchClient) -> str:
            manifest = read_json_object(
                observer.call(
                    "workbench_read",
                    {
                        "id": self.destination,
                        "section": "metadata",
                        "path": "restore_manifest.json",
                    },
                )
            )
            operation_id = manifest.get("operation_id")
            if not isinstance(operation_id, str) or not operation_id:
                raise AcceptanceError(
                    f"first-visible manifest lacks operation id: {manifest!r}"
                )
            self.assert_manifest(
                self.destination,
                operation_id,
                self.source,
                self.snapshot_id,
                observer,
            )
            first_index = self.index_signature(self.destination, observer)
            if first_index != self.baseline_index:
                raise AcceptanceError(
                    "destination became visible before its index overlay was complete: "
                    f"{first_index!r}"
                )
            return operation_id

        def assert_deterministically_hidden(observer: WorkbenchClient) -> None:
            observed = observer.raw_call("workbench_stat", {"id": self.destination})
            assert_tool_error(observed, "pre-attach destination stat")
            for name, arguments in (
                (
                    "workbench_search",
                    {
                        "id": self.destination,
                        "predicates": [],
                        "fields": ["name"],
                        "limit": INDEX_PAGE_LIMIT,
                    },
                ),
                (
                    "workbench_aggregate",
                    {
                        "id": self.destination,
                        "predicates": [],
                        "measures": [{"name": "files", "op": "count"}],
                    },
                ),
                ("workbench_catalog", {"id": self.destination}),
            ):
                result = observer.raw_call(name, arguments)
                if result.get("status") == "error" or "code" in result:
                    continue
                if name == "workbench_search" and (
                    result.get("matches") != [] or result.get("match_count") != 0
                ):
                    raise AcceptanceError(f"pre-attach search leaked rows: {result!r}")
                if name == "workbench_aggregate" and (
                    result.get("groups") != []
                    or result.get("input_match_count") not in {0, None}
                ):
                    raise AcceptanceError(
                        f"pre-attach aggregate leaked rows: {result!r}"
                    )
                catalog = result.get("catalog")
                if name == "workbench_catalog" and not (
                    result.get("catalog_empty") is True
                    or isinstance(catalog, dict)
                    and catalog.get("filterable") == []
                ):
                    raise AcceptanceError(
                        f"pre-attach catalog leaked fields: {result!r}"
                    )
            global_search = observer.call(
                "workbench_search",
                {
                    "predicates": [
                        {
                            "field": "name",
                            "op": "eq",
                            "value": f"indexed-{self.env.config.profile.indexed_files - 1:03d}.csv",
                        }
                    ],
                    "fields": ["name"],
                    "limit": INDEX_PAGE_LIMIT,
                },
            )
            if any(
                match_.get("workbench_id") == self.destination
                for match_ in global_search.get("matches", [])
                if isinstance(match_, dict)
            ):
                raise AcceptanceError("destination index leaked before root attach")

        visible = False
        visible_before_all_acks = False
        first_visible_operation_id = ""
        outcomes: list[dict[str, Any]] = []
        attempts: list[int] = []
        executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=CONCURRENT_RESTORE_CALLS
        )
        try:
            dedicated_clients = [
                WorkbenchClient(
                    self.client_class, self.launch, self.env.config.tool_timeout
                )
                for _ in range(CONCURRENT_RESTORE_CALLS + 1)
            ]
            for client in dedicated_clients:
                validate_tool_contract(client.tools())
            workers = dedicated_clients[:CONCURRENT_RESTORE_CALLS]
            observer = dedicated_clients[-1]
            futures = [executor.submit(restore, client) for client in workers]
            start_barrier.wait(timeout=self.env.config.tool_timeout)
            hold.wait_ready(self.env.config.tool_timeout)
            if any(future.done() for future in futures):
                raise AcceptanceError(
                    "a concurrent restore returned before the pre-attach hold was released"
                )
            assert_deterministically_hidden(observer)
            hold.release()
            deadline = time.monotonic() + self.env.config.tool_timeout
            while time.monotonic() < deadline:
                observed = observer.raw_call("workbench_stat", {"id": self.destination})
                if observed.get("status") == "success":
                    visible = True
                    visible_before_all_acks = not all(
                        future.done() for future in futures
                    )
                    first_visible_operation_id = validate_first_visible(observer)
                    break
                assert_tool_error(observed, "destination visibility probe")
                if all(future.done() for future in futures):
                    break
                time.sleep(0.005)
            failures: list[str] = []
            for future in futures:
                try:
                    outcome, attempt_count = future.result(
                        timeout=self.env.config.tool_timeout
                    )
                    outcomes.append(outcome)
                    attempts.append(attempt_count)
                except Exception as exc:
                    failures.append(str(exc))
            if failures:
                distribution = collections.Counter(failures)
                raise AcceptanceError(
                    f"{len(failures)}/{CONCURRENT_RESTORE_CALLS} concurrent restore calls failed: "
                    f"{dict(distribution)!r}"
                )
            if not visible:
                observed = observer.raw_call("workbench_stat", {"id": self.destination})
                if observed.get("status") != "success":
                    raise AcceptanceError("destination never became visible")
                first_visible_operation_id = validate_first_visible(observer)
        finally:
            try:
                hold.release()
            except Exception:
                pass
            executor.shutdown(wait=True, cancel_futures=True)
            hold.disarm_after_crash()
            for client in dedicated_clients:
                try:
                    client.close()
                except Exception:
                    pass
        operation_ids = {outcome.get("operation_id") for outcome in outcomes}
        roots = {outcome.get("destination_root") for outcome in outcomes}
        if len(operation_ids) != 1 or None in operation_ids or len(roots) != 1:
            raise AcceptanceError("concurrent exact retries did not converge")
        self.operation_id = str(outcomes[0]["operation_id"])
        if self.operation_id != operation_id:
            raise AcceptanceError(
                f"concurrent restore operation id is not deterministic: {self.operation_id}"
            )
        if first_visible_operation_id != self.operation_id:
            raise AcceptanceError(
                "first-visible manifest operation differs from terminal outcomes"
            )
        for outcome in outcomes:
            if (
                outcome.get("state") != "complete"
                or outcome.get("snapshot_id") != self.snapshot_id
                or outcome.get("read_version") != self.snapshot_version
                or outcome.get("cleanup_pending") is not False
            ):
                raise AcceptanceError(f"malformed restore outcome: {outcome!r}")

        self.assert_manifest(
            self.destination, self.operation_id, self.source, self.snapshot_id
        )
        restored_index = self.index_signature(self.destination)
        if restored_index != self.baseline_index:
            raise AcceptanceError(
                f"restored search/aggregate/catalog differ: source={self.baseline_index!r}, "
                f"destination={restored_index!r}"
            )
        inventory_after = self.env.inventory()
        put_records = self.env.successful_puts()[puts_before:]
        changed = changed_objects(inventory_before, inventory_after)
        if len(put_records) != 1:
            raise AcceptanceError(
                "16 concurrent restores must issue exactly one manifest PUT; "
                f"observed {len(put_records)}: {put_records!r}"
            )
        if len(changed) != 1:
            raise AcceptanceError(
                f"COW restore changed objects other than one manifest: {sorted(changed)!r}"
            )
        if self.fork_owned_keys & changed:
            raise AcceptanceError("restore overwrote or copied a source data object")
        self.restore_manifest_keys.update(changed)

        graph = self.private_graph_evidence(
            expected_complete=1,
            expected_snapshot_pins=1,
            expect_empty=False,
        )
        conflict_snapshot = self.client_a.call(
            "workbench_snapshot", {"id": self.source, "ttl_days": 7}
        )
        conflict_snapshot_id = int(conflict_snapshot["snapshot_id"])
        agent_redrive = self.agent_restore_redrive(conflict_snapshot_id)
        conflict = decode_tool_error(
            self.client_a.raw_call(
                RESTORE_TOOL,
                {
                    "id": self.source,
                    "at_snapshot": conflict_snapshot_id,
                    "destination_id": self.destination,
                },
            )
        )
        if conflict.code != "RestoreDestinationConflict":
            raise AcceptanceError(f"wrong destination conflict: {conflict!r}")
        self.env.cli(
            "retire-snapshot",
            self.absolute(self.source),
            str(conflict_snapshot_id),
        )
        return {
            "operation_id": self.operation_id,
            "concurrent_calls": len(outcomes),
            "independent_mcp_processes": len(dedicated_clients),
            "max_retry_attempts": max(attempts),
            "manifest_puts": len(put_records),
            "changed_objects": sorted(changed),
            "visible_before_all_acks": visible_before_all_acks,
            "deterministic_pre_attach_hold": "references-sealed",
            "private_graph": graph,
            "agent_handler_redrive": agent_redrive,
        }

    def scenario_restart_and_nested_restore(self) -> dict[str, Any]:
        self.reconnect_after_server_kill()
        retry = self.client_a.call(
            RESTORE_TOOL,
            {
                "id": self.source,
                "at_snapshot": self.snapshot_id,
                "destination_id": self.destination,
            },
        )
        if retry.get("operation_id") != self.operation_id:
            raise AcceptanceError("operation id changed after server/Agent restart")
        self.env.cli(
            "rename",
            self.absolute(self.destination, "outputs", "indexed-000.csv"),
            self.absolute(self.destination, "outputs", "renamed.csv"),
        )
        self.env.cli(
            "rm", self.absolute(self.destination, "outputs", "indexed-001.csv")
        )
        self.put_text(
            self.destination,
            "published.csv",
            "index,value\nnew,1\n",
            content_type="text/csv",
        )
        self.mutated_index = self.index_signature(self.destination)
        expected_names = {
            f"indexed-{index:03d}.csv"
            for index in range(self.env.config.profile.indexed_files)
        }
        expected_names.difference_update({"indexed-000.csv", "indexed-001.csv"})
        expected_names.update({"renamed.csv", "published.csv"})
        if self.mutated_index["csv_names"] != sorted(expected_names):
            raise AcceptanceError(
                "overlay rename/delete/publish left missing or ghost index rows: "
                f"{self.mutated_index!r}"
            )
        nested_snapshot = self.client_a.call(
            "workbench_snapshot",
            {"id": self.destination, "name": "nested-point", "ttl_days": 7},
        )
        self.nested_snapshot_id = int(nested_snapshot["snapshot_id"])
        inventory_before_nested = self.env.inventory()
        puts_before_nested = len(self.env.successful_puts())
        nested = self.client_b.call(
            RESTORE_TOOL,
            {
                "id": self.destination,
                "at_snapshot": self.nested_snapshot_id,
                "destination_id": self.nested,
            },
        )
        self.nested_operation_id = str(nested["operation_id"])
        self.assert_manifest(
            self.nested,
            self.nested_operation_id,
            self.destination,
            self.nested_snapshot_id,
        )
        if self.index_signature(self.nested) != self.mutated_index:
            raise AcceptanceError(
                "nested restore did not preserve completed overlay index"
            )
        nested_changed = changed_objects(inventory_before_nested, self.env.inventory())
        nested_puts = self.env.successful_puts()[puts_before_nested:]
        if len(nested_changed) != 1 or len(nested_puts) != 1:
            raise AcceptanceError(
                "nested COW restore must write only its manifest: "
                f"changed={sorted(nested_changed)!r}, puts={nested_puts!r}"
            )
        self.restore_manifest_keys.update(nested_changed)
        self.env.cli(
            "retire-snapshot",
            self.absolute(self.destination),
            str(self.nested_snapshot_id),
        )
        return {
            "server_restart": "SIGKILL",
            "agent_reconnected": True,
            "nested_operation_id": self.nested_operation_id,
            "nested_manifest_puts": len(nested_puts),
        }

    def list_entries(
        self, client: WorkbenchClient, workbench_id: str, section: str, path: str = ""
    ) -> list[dict[str, Any]]:
        entries: list[dict[str, Any]] = []
        cursor: str | None = None
        while True:
            args: dict[str, Any] = {
                "id": workbench_id,
                "section": section,
                "limit": 100,
            }
            if path:
                args["path"] = path
            if cursor is not None:
                args["cursor"] = cursor
            page = client.call("workbench_list", args)
            values = page.get("entries")
            if not isinstance(values, list):
                raise AcceptanceError(f"list lacks entries: {page!r}")
            entries.extend(values)
            if page.get("truncated") is not True:
                return entries
            cursor = page.get("next_cursor")
            if not isinstance(cursor, str) or not cursor:
                raise AcceptanceError("truncated list lacks cursor")

    def delete_section_tree(
        self, client: WorkbenchClient, workbench_id: str, section: str, path: str = ""
    ) -> None:
        entries = self.list_entries(client, workbench_id, section, path)
        directories: list[str] = []
        for entry in entries:
            name = entry.get("name")
            kind = entry.get("kind")
            if not isinstance(name, str):
                raise AcceptanceError(f"malformed list entry: {entry!r}")
            relative = f"{path}/{name}" if path else name
            if kind == "directory":
                self.delete_section_tree(client, workbench_id, section, relative)
                directories.append(relative)
            else:
                self.env.cli("rm", self.absolute(workbench_id, section, relative))
        for relative in reversed(directories):
            self.env.cli("rmdir", self.absolute(workbench_id, section, relative))

    def delete_workbench(self, client: WorkbenchClient, workbench_id: str) -> None:
        for section in SECTIONS:
            self.delete_section_tree(client, workbench_id, section)
            self.env.cli("rmdir", self.absolute(workbench_id, section))
        self.env.cli("rmdir", self.absolute(workbench_id))

    def wait_for_keys(self, keys: set[str], *, present: bool) -> dict[str, Any]:
        started = time.monotonic()
        stall_deadline = started + self.env.config.gc_deadline
        # A full 1 GiB restore has hundreds of exact references plus member and
        # index rows. One bounded release page may perform many durable CASes,
        # so a fixed wall-clock deadline can expire while every call is making
        # measurable progress. Keep the configured deadline as the maximum
        # no-progress window and retain a finite overall cap for genuine loops.
        hard_deadline = started + self.env.config.gc_deadline * 8
        last_gc: dict[str, Any] = {}
        previous_remaining = len(keys)
        previous_control_rows: int | None = None
        last_control_rows: int | None = None
        previous_commit_total: int | None = None
        last_commit_total: int | None = None
        while time.monotonic() < hard_deadline:
            # Keep the object page bounded while release jobs are still being
            # paged. Scanning thousands of snapshot-blocked object records on
            # every round does not advance a release job any faster and can
            # consume the entire lifecycle deadline before its member/index
            # pages reach the object-enqueue boundary.
            last_gc = self.env.manual_gc(limit=64)
            current = set(self.env.inventory())
            if (keys <= current) if present else keys.isdisjoint(current):
                return last_gc
            remaining = len(keys - current) if present else len(keys & current)
            stats = self.env.stats()
            restore = stats.get("restore")
            if not isinstance(restore, dict):
                raise AcceptanceError(
                    f"restore metrics disappeared while waiting for objects: {restore!r}"
                )
            control_rows = restore.get("control_rows")
            if not isinstance(control_rows, dict):
                raise AcceptanceError(
                    "restore control-row metrics disappeared while waiting for objects: "
                    f"{restore!r}"
                )
            last_control_rows = sum(
                nonnegative_int(value, f"restore.control_rows.{name}")
                for name, value in control_rows.items()
            )
            metadata = stats.get("metadata_store")
            if not isinstance(metadata, dict):
                raise AcceptanceError(
                    "metadata-store metrics disappeared while waiting for objects: "
                    f"{metadata!r}"
                )
            last_commit_total = nonnegative_int(
                metadata.get("commit_total"), "metadata_store.commit_total"
            )
            object_gc = last_gc.get("object_gc")
            if not isinstance(object_gc, dict):
                raise AcceptanceError(f"manual GC omitted object outcome: {last_gc!r}")
            queue_progress = sum(
                nonnegative_int(object_gc.get(field), f"object_gc.{field}")
                for field in ("deleted", "missing", "records_removed")
            )
            graph_progress = (
                previous_control_rows is not None
                and last_control_rows < previous_control_rows
            )
            commit_progress = (
                previous_commit_total is not None
                and last_commit_total > previous_commit_total
            )
            if (
                remaining < previous_remaining
                or graph_progress
                or commit_progress
                or queue_progress > 0
            ):
                stall_deadline = time.monotonic() + self.env.config.gc_deadline
            elif time.monotonic() >= stall_deadline:
                break
            previous_remaining = remaining
            previous_control_rows = last_control_rows
            previous_commit_total = last_commit_total
            time.sleep(0.1)
        state = sorted(keys & set(self.env.inventory()))
        raise AcceptanceError(
            f"object lifecycle deadline expired (present={present}, remaining={state!r}, "
            f"last_control_rows={last_control_rows!r}, "
            f"last_commit_total={last_commit_total!r}, last_gc={last_gc!r})"
        )

    def record_snapshot_deferred_release(self, keys: set[str]) -> dict[str, Any]:
        """Record post-snapshot objects that the live retention floor must hold.

        The source checkpoint intentionally remains pinned throughout the crash
        matrices. NoKV's mount-global history floor therefore retains objects
        enqueued after that checkpoint, including each temporary fork's own
        restore manifest. Their private restore rows must still drain now, but
        the current mount-global policy intentionally defers physical deletion
        until the source pin is retired.
        """
        if not keys:
            raise AcceptanceError("snapshot-deferred release requires object keys")
        deadline = time.monotonic() + min(
            self.env.config.gc_deadline, self.env.config.tool_timeout
        )
        last_gc: dict[str, Any] = {}
        remaining: set[str] = set(keys)
        snapshot_block_evidence: dict[str, Any] | None = None
        while time.monotonic() < deadline:
            last_gc = self.env.manual_gc(limit=64)
            remaining = keys & set(self.env.inventory())
            object_gc = last_gc.get("object_gc")
            stats = self.env.stats()
            metadata = stats.get("metadata_store")
            if (
                isinstance(object_gc, dict)
                and object_gc.get("blocked_by_snapshots", 0) > 0
                and isinstance(metadata, dict)
                and metadata.get("active_snapshot_pin_total") == 1
            ):
                snapshot_block_evidence = last_gc
            restore_metrics = stats.get("restore")
            if not restore_release_graph_drained(restore_metrics):
                time.sleep(0.02)
                continue
            validate_restore_metrics_object(
                restore_metrics, expected_complete=0, expect_empty=True
            )
            if not remaining:
                return {
                    "released_immediately": len(keys),
                    "deferred_by_snapshot": [],
                    "last_gc": last_gc,
                }
            if snapshot_block_evidence is not None:
                self.snapshot_deferred_release_keys.update(remaining)
                return {
                    "released_immediately": len(keys) - len(remaining),
                    "deferred_by_snapshot": sorted(remaining),
                    "last_gc": snapshot_block_evidence,
                }
            time.sleep(0.02)
        raise AcceptanceError(
            "released objects remained without live-snapshot GC evidence: "
            f"remaining={sorted(remaining)!r}, last_gc={last_gc!r}"
        )

    def wait_for_exact_inventory(
        self, expected: dict[str, ObjectFingerprint]
    ) -> dict[str, ObjectFingerprint]:
        deadline = time.monotonic() + self.env.config.gc_deadline
        current: dict[str, ObjectFingerprint] = {}
        while time.monotonic() < deadline:
            self.env.manual_gc()
            current = self.env.inventory()
            if current == expected:
                return current
            time.sleep(0.1)
        raise AcceptanceError(
            "RustFS inventory did not return to its exact baseline: "
            f"expected={expected!r}, current={current!r}"
        )

    def wait_for_restore_metrics(
        self, *, expected_complete: int, expect_empty: bool
    ) -> dict[str, Any]:
        deadline = time.monotonic() + self.env.config.gc_deadline
        last_error = "metrics were not sampled"
        while time.monotonic() < deadline:
            self.env.manual_gc()
            try:
                return validate_restore_metrics(
                    self.env.stats(),
                    expected_complete=expected_complete,
                    expect_empty=expect_empty,
                )
            except AcceptanceError as exc:
                last_error = str(exc)
            time.sleep(0.1)
        raise AcceptanceError(
            "restore private graph did not converge before deadline: " + last_error
        )

    def private_graph_evidence(
        self,
        *,
        expected_complete: int,
        expected_snapshot_pins: int,
        expect_empty: bool,
    ) -> dict[str, Any]:
        metrics = self.wait_for_restore_metrics(
            expected_complete=expected_complete, expect_empty=expect_empty
        )
        report = validate_fsck_report(
            self.env.fsck(),
            expected_complete=expected_complete,
            expected_snapshot_pins=expected_snapshot_pins,
            expected_fork_bindings=0,
        )
        restore_report = report["restore_shards"][0]["report"]
        return {
            "active_marker": metrics["active_marker"],
            "allocator_v2_fenced": metrics["allocator_v2_fenced"],
            "operations": metrics["operations"],
            "staging_rows": metrics["staging_rows"],
            "exact_reference_rows": metrics["exact_reference_rows"],
            "index_rows": metrics["index_rows"],
            "control_rows": metrics["control_rows"],
            "borrowed_objects_checked": restore_report["borrowed_objects_checked"],
            "snapshot_pins_scanned": report["snapshot_pins_scanned"],
            "fork_bindings_scanned": report["fork_bindings_scanned"],
        }

    def assert_no_index_ghosts(self, workbench_ids: set[str]) -> None:
        cursor: str | None = None
        pages = 0
        while True:
            arguments: dict[str, Any] = {
                "predicates": [{"field": "name", "op": "suffix", "value": ".csv"}],
                "fields": ["name"],
                "limit": INDEX_PAGE_LIMIT,
            }
            if cursor is not None:
                arguments["cursor"] = cursor
            page = self.client_a.call("workbench_search", arguments)
            matches = page.get("matches")
            if not isinstance(matches, list):
                raise AcceptanceError(f"global ghost search is malformed: {page!r}")
            ghosts = [
                match_
                for match_ in matches
                if isinstance(match_, dict)
                and match_.get("workbench_id") in workbench_ids
            ]
            if ghosts:
                raise AcceptanceError(
                    f"released restore index rows remain visible: {ghosts!r}"
                )
            pages += 1
            if page.get("truncated") is not True:
                break
            cursor = page.get("next_cursor")
            if not isinstance(cursor, str) or not cursor:
                raise AcceptanceError("global ghost search has no next cursor")
            if pages > self.env.config.profile.indexed_files + 32:
                raise AcceptanceError("global ghost search pagination did not converge")
        for workbench_id in workbench_ids:
            for name, arguments in (
                (
                    "workbench_search",
                    {
                        "id": workbench_id,
                        "predicates": [],
                        "fields": ["name"],
                        "limit": INDEX_PAGE_LIMIT,
                    },
                ),
                (
                    "workbench_aggregate",
                    {
                        "id": workbench_id,
                        "predicates": [],
                        "measures": [{"name": "files", "op": "count"}],
                    },
                ),
                ("workbench_catalog", {"id": workbench_id}),
            ):
                result = self.client_b.raw_call(name, arguments)
                if result.get("status") == "error" or "code" in result:
                    continue
                if name == "workbench_search" and result.get("matches") != []:
                    raise AcceptanceError(
                        f"released {workbench_id} search has ghosts: {result!r}"
                    )
                if name == "workbench_aggregate" and (
                    result.get("groups") != []
                    or result.get("input_match_count") not in {0, None}
                ):
                    raise AcceptanceError(
                        f"released {workbench_id} aggregate has ghosts: {result!r}"
                    )
                catalog = result.get("catalog")
                if name == "workbench_catalog" and not (
                    result.get("catalog_empty") is True
                    or isinstance(catalog, dict)
                    and catalog.get("filterable") == []
                ):
                    raise AcceptanceError(
                        f"released {workbench_id} catalog has ghosts: {result!r}"
                    )

    def scenario_source_retirement_moves_and_release(self) -> dict[str, Any]:
        def progress(step: str) -> None:
            print(f"[restore-live-e2e] STEP  source-release:{step}", flush=True)

        progress("retention-floor-preflight")
        stats_before_floor_lift = self.env.stats()
        metadata_before = stats_before_floor_lift.get("metadata_store")
        history_before = stats_before_floor_lift.get("history_gc")
        if (
            not isinstance(metadata_before, dict)
            or metadata_before.get("active_snapshot_pin_total") != 1
        ):
            raise AcceptanceError(
                f"source snapshot did not hold exactly one retention pin: {metadata_before!r}"
            )
        if not isinstance(history_before, dict) or not isinstance(
            history_before.get("iterations"), int
        ):
            raise AcceptanceError(f"history GC stats are malformed: {history_before!r}")
        history_iterations_before = int(history_before["iterations"])
        retained_floor_gc = self.env.manual_gc()
        retained_floor_history = retained_floor_gc.get("history_gc")
        if (
            not isinstance(retained_floor_history, dict)
            or nonnegative_int(
                retained_floor_history.get("retained_by_snapshots"),
                "history_gc.retained_by_snapshots",
            )
            == 0
        ):
            raise AcceptanceError(
                "source snapshot did not retain any metadata history before retirement: "
                f"{retained_floor_gc!r}"
            )
        gc_stop = threading.Event()
        gc_started = threading.Event()
        gc_outcomes: list[dict[str, Any]] = []

        def race_gc_with_pin_retirement() -> None:
            while not gc_stop.is_set():
                gc_outcomes.append(self.env.manual_gc())
                gc_started.set()
                time.sleep(0.002)

        gc_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        gc_future = gc_executor.submit(race_gc_with_pin_retirement)
        try:
            if not gc_started.wait(timeout=self.env.config.tool_timeout):
                raise AcceptanceError(
                    "concurrent GC did not start before pin retirement"
                )
            post_retirement_gc_start = len(gc_outcomes)
            self.env.cli(
                "retire-snapshot", self.absolute(self.source), str(self.snapshot_id)
            )
            self.env.cli(
                "rename", self.absolute(self.destination), self.absolute(self.moved)
            )
            self.delete_workbench(self.client_a, self.source)
        finally:
            gc_stop.set()
            gc_future.result(timeout=self.env.config.tool_timeout)
            gc_executor.shutdown(wait=True, cancel_futures=True)
        progress("pin-retirement-race-complete")
        if not gc_outcomes:
            raise AcceptanceError(
                "pin/history/object GC concurrency produced no outcome"
            )
        # Periodic workers are deliberately outside the acceptance window so
        # full fsck can prove a stable metadata epoch. Prove the floor lift from
        # the bounded manual-GC outcomes instead of waiting for the passive
        # worker's iteration counter. The slice also includes a call that may
        # have started before retirement but completed after the pin commit.
        floor_lift_gc = self.env.manual_gc()
        post_retirement_gc_outcomes = gc_outcomes[post_retirement_gc_start:] + [
            floor_lift_gc
        ]
        history_scanned_after_retirement = 0
        history_removed_after_retirement = 0
        for outcome in post_retirement_gc_outcomes:
            history = outcome.get("history_gc")
            if not isinstance(history, dict):
                raise AcceptanceError(
                    f"manual GC omitted history outcome: {outcome!r}"
                )
            history_scanned_after_retirement += nonnegative_int(
                history.get("scanned"), "history_gc.scanned"
            )
            history_removed_after_retirement += nonnegative_int(
                history.get("removed"), "history_gc.removed"
            )
        if history_removed_after_retirement == 0:
            raise AcceptanceError(
                "snapshot retirement did not release any retained metadata history: "
                f"before={retained_floor_gc!r}, "
                f"after={post_retirement_gc_outcomes!r}"
            )
        progress("history-floor-lifted")
        deferred_release_gc = self.wait_for_keys(
            self.snapshot_deferred_release_keys, present=False
        )
        deferred_release_count = len(self.snapshot_deferred_release_keys)
        self.snapshot_deferred_release_keys.clear()
        progress("snapshot-deferred-objects-released")
        # indexed-001.csv was deliberately removed from the first restore
        # before the nested snapshot. Once the source pin and namespace retire,
        # its source-owned object is allowed to disappear immediately. The COW
        # fixture is the object set that both surviving restore roots must keep.
        self.wait_for_keys(self.large_object_keys, present=True)

        floor_graph = self.private_graph_evidence(
            expected_complete=2,
            expected_snapshot_pins=0,
            expect_empty=False,
        )
        progress("surviving-restore-graph-verified")
        floor_deadline = time.monotonic() + self.env.config.gc_deadline
        stats_after_floor_lift: dict[str, Any] = {}
        while time.monotonic() < floor_deadline:
            stats_after_floor_lift = self.env.stats()
            metadata_after = stats_after_floor_lift.get("metadata_store")
            history_after = stats_after_floor_lift.get("history_gc")
            if (
                isinstance(metadata_after, dict)
                and metadata_after.get("active_snapshot_pin_total") == 0
                and isinstance(history_after, dict)
                and isinstance(history_after.get("iterations"), int)
            ):
                break
            time.sleep(0.1)
        else:
            raise AcceptanceError(
                "snapshot retirement did not lift the retention floor/history GC: "
                f"before={stats_before_floor_lift!r}, after={stats_after_floor_lift!r}"
            )

        retry = self.client_a.call(
            RESTORE_TOOL,
            {
                "id": self.source,
                "at_snapshot": self.snapshot_id,
                "destination_id": self.destination,
            },
        )
        if retry.get("operation_id") != self.operation_id:
            raise AcceptanceError("terminal retry failed after pin/source deletion")
        progress("terminal-retry-after-source-delete")
        digest, size = self.env.hash_remote_file(
            self.absolute(self.moved, "outputs", "cow-large.bin")
        )
        if (
            digest != self.expected_digest
            or size != self.env.config.profile.large_bytes
        ):
            raise AcceptanceError(
                f"restored COW digest mismatch: digest={digest}, size={size}"
            )
        if self.index_signature(self.moved) != self.mutated_index:
            raise AcceptanceError("root move lost overlay index visibility")
        progress("moved-root-content-and-index-verified")

        escaped_path = f"{self.root}/escaped-cow-large.bin"
        self.env.cli(
            "rename",
            self.absolute(self.moved, "outputs", "cow-large.bin"),
            escaped_path,
        )
        progress("borrower-escaped-restore-root")

        # Replace one Complete restore root with another. Replacing a non-empty
        # ordinary directory would violate POSIX and strand an unowned subtree;
        # this path instead exercises the PR2 contract directly: the victim
        # restore enters Releasing in the same command while the nested restore
        # remains Complete and moves to the destination.
        self.env.cli(
            "rename-replace", self.absolute(self.nested), self.absolute(self.moved)
        )
        self.env.cli("rename", self.absolute(self.moved), self.absolute(self.replaced))
        nested_read = self.client_a.call(
            "workbench_read",
            {
                "id": self.replaced,
                "section": "outputs",
                "path": "renamed.csv",
            },
        )
        if nested_read.get("total_size_bytes") is None:
            raise AcceptanceError("rename-replace lost restored borrower content")
        progress("rename-replace-verified")

        self.wait_for_keys(self.large_object_keys, present=True)
        self.delete_workbench(self.client_a, self.replaced)
        progress("replacement-root-deleted")
        all_restore_keys = self.fork_owned_keys | self.restore_manifest_keys
        release_without_escaped = all_restore_keys - self.large_object_keys
        partial_release_gc = self.wait_for_keys(release_without_escaped, present=False)
        progress("partial-release-complete")
        self.wait_for_keys(self.large_object_keys, present=True)
        escaped_digest, escaped_size = self.env.hash_remote_file(escaped_path)
        if escaped_digest != self.expected_digest or escaped_size != size:
            raise AcceptanceError(
                "escaped borrower became unreadable after restore root deletion"
            )
        self.env.cli("rm", escaped_path)
        progress("escaped-borrower-deleted")
        final_release_gc = self.wait_for_keys(all_restore_keys, present=False)
        progress("final-object-release-complete")
        final_graph = self.private_graph_evidence(
            expected_complete=0,
            expected_snapshot_pins=0,
            expect_empty=True,
        )
        control_rows_returned_to_baseline: bool | None = None
        if self.restore_control_baseline:
            if final_graph["control_rows"] != self.restore_control_baseline:
                raise AcceptanceError(
                    "released restore control rows did not return to the measured "
                    "durable-ledger baseline: "
                    f"expected={self.restore_control_baseline!r}, "
                    f"current={final_graph['control_rows']!r}"
                )
            control_rows_returned_to_baseline = True
        self.assert_no_index_ghosts(
            {
                self.source,
                self.destination,
                self.moved,
                self.nested,
                self.replaced,
            }
        )
        progress("private-graph-and-index-clean")
        self.wait_for_exact_inventory(self.initial_inventory)
        progress("inventory-returned-to-baseline")
        self.env.assert_binary_unchanged()
        return {
            "large_digest": digest,
            "large_size": size,
            "escaped_borrower_digest": escaped_digest,
            "source_deleted": True,
            "root_move": True,
            "rename_replace": True,
            "escaped_rename": True,
            "released_object_keys": len(all_restore_keys),
            "restore_manifest_keys_released": len(self.restore_manifest_keys),
            "partial_release_gc": partial_release_gc,
            "last_gc": final_release_gc,
            "concurrent_gc_calls": len(gc_outcomes),
            "snapshot_deferred_release_keys": deferred_release_count,
            "snapshot_deferred_release_gc": deferred_release_gc,
            "history_gc_iterations_before": history_iterations_before,
            "history_gc_iterations_after": stats_after_floor_lift["history_gc"][
                "iterations"
            ],
            "history_gc_retained_before_retirement": retained_floor_history[
                "retained_by_snapshots"
            ],
            "history_gc_scanned_after_retirement": history_scanned_after_retirement,
            "history_gc_removed_after_retirement": history_removed_after_retirement,
            "floor_lift_graph": floor_graph,
            "final_private_graph": final_graph,
            "durable_ledger_baseline": self.restore_control_baseline,
            "control_rows_returned_to_baseline": control_rows_returned_to_baseline,
            "final_inventory_matches_initial": True,
        }

    def run(self) -> dict[str, dict[str, Any]]:
        self.connect_clients()
        self.scenario(
            "contract_agent_reconnect_and_1g_fixture",
            self.scenario_contract_and_fixture,
        )
        if self.env.config.profile.name == "full":
            self.scenario(
                "durable_create_crash_barrier_matrix",
                self.scenario_create_crash_matrix,
            )
            self.scenario(
                "durable_cleanup_release_crash_recovery",
                self.scenario_cleanup_release_crash_recovery,
            )
        self.scenario(
            "first_visibility_16way_idempotent_cow_restore",
            self.scenario_concurrent_restore,
        )
        self.scenario(
            "server_restart_index_mutation_and_nested_restore",
            self.scenario_restart_and_nested_restore,
        )
        self.scenario(
            "source_retirement_root_moves_and_exact_ref_release",
            self.scenario_source_retirement_moves_and_release,
        )
        return self.results


def load_lingtai(lingtai_kernel_dir: Path) -> tuple[type, type]:
    source = lingtai_kernel_dir / "src"
    if not source.is_dir():
        raise AcceptanceError(f"LingTai source directory not found: {source}")
    sys.path.insert(0, str(source))
    try:
        from lingtai.agent import Agent
        from lingtai.services.mcp import MCPClient
    except Exception as exc:
        raise AcceptanceError(
            "failed to import LingTai Agent/MCPClient; run this script inside the "
            "LingTai uv environment"
        ) from exc
    module_path = Path(sys.modules[Agent.__module__].__file__).resolve()
    try:
        module_path.relative_to(source.resolve())
    except ValueError as exc:
        raise AcceptanceError(
            f"imported LingTai from {module_path}, expected {source.resolve()}"
        ) from exc
    return Agent, MCPClient


def require_commands(names: tuple[str, ...]) -> None:
    missing = [name for name in names if shutil.which(name) is None]
    if missing:
        raise AcceptanceError(f"required commands are missing: {', '.join(missing)}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=("quick", "full"), default="quick")
    parser.add_argument("--require-all", action="store_true")
    parser.add_argument("--repo-root", type=Path, default=repo_root)
    parser.add_argument(
        "--cargo-bin",
        type=Path,
        default=Path(
            os.environ.get("CARGO", shutil.which("cargo") or "~/.cargo/bin/cargo")
        ),
    )
    parser.add_argument("--nokv-bin", type=Path)
    parser.add_argument(
        "--lingtai-kernel-dir",
        type=Path,
        default=Path(os.environ.get("LINGTAI_KERNEL_DIR", "~/lingtai-kernel")),
    )
    parser.add_argument("--state-dir", type=Path)
    parser.add_argument("--server-port", type=int, default=0)
    parser.add_argument("--rustfs-port", type=int, default=0)
    parser.add_argument("--rustfs-console-port", type=int, default=0)
    parser.add_argument("--proxy-port", type=int, default=0)
    parser.add_argument("--rustfs-image", default="rustfs/rustfs:latest")
    parser.add_argument("--command-timeout", type=float, default=300)
    parser.add_argument("--tool-timeout", type=float, default=300)
    parser.add_argument("--startup-timeout", type=float, default=180)
    parser.add_argument("--gc-deadline", type=float, default=120)
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--keep-state", action="store_true")
    args = parser.parse_args(argv)
    if args.require_all and args.profile != "full":
        parser.error("--require-all requires --profile full")
    if args.require_all and args.no_build:
        parser.error("--require-all forbids --no-build to prevent stale NoKV binaries")
    return args


def live_config(args: argparse.Namespace) -> LiveConfig:
    repo_root = args.repo_root.expanduser().resolve()
    state_dir: Path
    if args.state_dir:
        state_dir = args.state_dir.expanduser().resolve()
        if state_dir.exists() and any(state_dir.iterdir()):
            raise AcceptanceError(f"state directory must be empty: {state_dir}")
        state_dir.mkdir(parents=True, exist_ok=True)
    else:
        (repo_root / "target").mkdir(parents=True, exist_ok=True)
        state_dir = Path(
            tempfile.mkdtemp(prefix="durable-restore-live-", dir=repo_root / "target")
        )
    ports = [
        args.server_port or free_port(),
        args.rustfs_port or free_port(),
        args.rustfs_console_port or free_port(),
        args.proxy_port or free_port(),
    ]
    if len(set(ports)) != len(ports):
        raise AcceptanceError("server, RustFS, console, and proxy ports must differ")
    suffix = f"{os.getpid()}-{int(time.time())}"
    nokv_bin = (
        args.nokv_bin.expanduser().resolve()
        if args.nokv_bin
        else repo_root / "target/debug/nokv"
    )
    if args.require_all and nokv_bin != (repo_root / "target/debug/nokv").resolve():
        raise AcceptanceError(
            "--require-all must build and execute this checkout's target/debug/nokv"
        )
    return LiveConfig(
        repo_root=repo_root,
        cargo_bin=args.cargo_bin.expanduser().absolute(),
        nokv_bin=nokv_bin,
        lingtai_kernel_dir=args.lingtai_kernel_dir.expanduser().resolve(),
        state_dir=state_dir,
        server_port=ports[0],
        rustfs_port=ports[1],
        rustfs_console_port=ports[2],
        proxy_port=ports[3],
        rustfs_image=args.rustfs_image,
        bucket=f"nokv-durable-restore-{suffix}",
        container=f"nokv-durable-restore-{suffix}",
        profile=workload_profile(args.profile),
        command_timeout=args.command_timeout,
        tool_timeout=args.tool_timeout,
        startup_timeout=args.startup_timeout,
        gc_deadline=args.gc_deadline,
        build=not args.no_build,
        keep_state=args.keep_state,
        require_all=args.require_all,
    )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    config = live_config(args)
    env = LiveEnvironment(config)
    suite: AcceptanceSuite | None = None
    exit_code = 0
    try:
        require_commands(("aws", "docker", "git"))
        if config.build and not config.cargo_bin.is_file():
            raise AcceptanceError(f"cargo executable not found: {config.cargo_bin}")
        agent_class, client_class = load_lingtai(config.lingtai_kernel_dir)
        env.build()
        env.start_rustfs()
        env.start_proxy()
        env.start_server()
        suite = AcceptanceSuite(env, agent_class, client_class)
        suite.run()
        if config.require_all:
            expected = {
                "contract_agent_reconnect_and_1g_fixture",
                "durable_create_crash_barrier_matrix",
                "durable_cleanup_release_crash_recovery",
                "first_visibility_16way_idempotent_cow_restore",
                "server_restart_index_mutation_and_nested_restore",
                "source_retirement_root_moves_and_exact_ref_release",
            }
            if set(suite.results) != expected or any(
                result.get("status") != "passed" for result in suite.results.values()
            ):
                raise AcceptanceError(
                    "--require-all rejected an incomplete scenario set"
                )
            if config.profile.large_bytes != 1 << 30:
                raise AcceptanceError("--require-all did not execute the 1 GiB fixture")
            contract = suite.results["contract_agent_reconnect_and_1g_fixture"][
                "details"
            ]
            crash = suite.results["durable_create_crash_barrier_matrix"]["details"]
            cleanup_release = suite.results[
                "durable_cleanup_release_crash_recovery"
            ]["details"]
            concurrent = suite.results["first_visibility_16way_idempotent_cow_restore"][
                "details"
            ]
            release = suite.results[
                "source_retirement_root_moves_and_exact_ref_release"
            ]["details"]
            if (
                contract.get("fixture_4mib_blocks") != FULL_COW_BLOCK_COUNT
                or not isinstance(contract.get("fixture_marker_digest"), str)
                or len(contract["fixture_marker_digest"]) != 64
                or not isinstance(contract.get("raw_tools_schema_sha256"), str)
                or len(contract["raw_tools_schema_sha256"]) != 64
            ):
                raise AcceptanceError(
                    "--require-all lacks block or raw tools/list preflight evidence"
                )
            if (
                crash.get("materialization_batches", 0) < 2
                or crash.get("reference_batches", 0) < 2
            ):
                raise AcceptanceError(
                    "--require-all did not discover every restore batch"
                )
            if (
                concurrent.get("independent_mcp_processes")
                != CONCURRENT_RESTORE_CALLS + 1
            ):
                raise AcceptanceError(
                    "--require-all did not launch 16 callers plus observer"
                )
            if cleanup_release.get("release_worker_gc_calls", 0) <= 1:
                raise AcceptanceError(
                    "--require-all did not prove paged release-worker recovery"
                )
            if (
                release.get("final_inventory_matches_initial") is not True
                or release.get("control_rows_returned_to_baseline") is not True
                or release.get("final_private_graph", {})
                .get("operations", {})
                .get("complete")
                != 0
                or release.get("final_private_graph", {}).get("active_marker")
                is not True
                or release.get("final_private_graph", {}).get("allocator_v2_fenced")
                is not True
            ):
                raise AcceptanceError(
                    "--require-all did not prove final graph/inventory drain"
                )
            env.assert_binary_unchanged()
    except Exception as exc:
        exit_code = 1
        print(f"[restore-live-e2e] acceptance failed: {exc}", file=sys.stderr)
        if env.server_log.exists():
            print(tail(env.server_log), file=sys.stderr)
    finally:
        if suite is not None:
            suite.close_clients()
        summary = {
            "status": "passed" if exit_code == 0 else "failed",
            "profile": args.profile,
            "require_all": args.require_all,
            "fixture_bytes": config.profile.large_bytes,
            "server_bind": env.server_bind,
            "s3_endpoint": env.s3_endpoint,
            "rustfs_endpoint": env.rustfs_endpoint,
            "bucket": config.bucket,
            "state_dir": str(config.state_dir),
            "provenance": {
                "nokv_binary": str(config.nokv_bin),
                "nokv_binary_sha256": env.nokv_binary_sha256,
                "nokv_revision": env.repo_revision,
                "lingtai_revision": env.lingtai_revision,
            },
            "results": suite.results if suite is not None else {},
        }
        print(json.dumps(summary, indent=2, sort_keys=True), flush=True)
        env.cleanup()
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
