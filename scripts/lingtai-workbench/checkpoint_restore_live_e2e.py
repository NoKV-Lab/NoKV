#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Live NoKV -> LingTai workbench checkpoint/restore acceptance harness.

The harness intentionally uses the real process boundaries:

    RustFS Docker -> nokv serve -> LingTai Agent MCP registration/retry
                  -> nokv mcp --profile workbench -> LingTai MCPClient

It owns an isolated RustFS container, bucket, metadata directory, and server
port. Every process wait and network poll has a deadline. The default quick
profile is suitable for local iteration; the full profile runs the acceptance
counts from the checkpoint/restore plan.
"""

from __future__ import annotations

import argparse
import base64
import concurrent.futures
import dataclasses
import json
import os
import re
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


BASE_WORKBENCH_TOOLS = (
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
)
RESTORE_TOOL = "workbench_restore"
WORKBENCH_ROOT_TEMPLATE = "/agents/{agent_id}/wb"


class AcceptanceError(RuntimeError):
    """A deterministic acceptance assertion failed."""


@dataclasses.dataclass(frozen=True)
class WorkloadProfile:
    renew_rounds: int
    reaper_rounds: int
    history_entries: int
    page_limit: int = 7


def workload_profile(name: str) -> WorkloadProfile:
    if name == "quick":
        return WorkloadProfile(renew_rounds=8, reaper_rounds=20, history_entries=22)
    if name == "full":
        return WorkloadProfile(renew_rounds=100, reaper_rounds=200, history_entries=101)
    raise AcceptanceError(f"unknown workload profile: {name}")


@dataclasses.dataclass(frozen=True)
class ToolError:
    code: str
    message: str
    retryable: bool
    details: dict[str, Any]


@dataclasses.dataclass(frozen=True)
class ResolvedMcpLaunch:
    command: str
    args: list[str]
    env: dict[str, str]
    root: str


def decode_tool_error(result: dict[str, Any]) -> ToolError:
    """Decode LingTai MCPClient's error envelope into the public typed error."""
    if "code" in result and "message" in result:
        payload: Any = result
    elif result.get("status") == "error":
        payload = result.get("message")
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError as exc:
                raise AcceptanceError(
                    f"MCP error is not structured JSON: {payload!r}"
                ) from exc
    else:
        raise AcceptanceError(f"result is not an MCP tool error: {result!r}")
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


def parse_snapshot_id(output: str) -> int:
    match = re.search(r"\bid=(\d+)\b", output)
    if match is None:
        raise AcceptanceError(f"snapshot CLI output lacks id=: {output!r}")
    return int(match.group(1))


def parse_snapshot_expiry(output: str) -> int:
    match = re.search(r"\blease_expires_unix_ms=(\d+)\b", output)
    if match is None:
        raise AcceptanceError(
            f"snapshot CLI output lacks lease_expires_unix_ms=: {output!r}"
        )
    return int(match.group(1))


class PageAccumulator:
    """Verify cursor progress, uniqueness, and global dentry-name ordering."""

    def __init__(self, page_limit: int) -> None:
        self.page_limit = page_limit
        self.page_count = 0
        self.page_sizes: list[int] = []
        self.names: list[str] = []
        self._seen_names: set[str] = set()
        self._seen_cursors: set[str] = set()

    def add(self, names: list[str], next_cursor: str | None, truncated: bool) -> None:
        self.page_count += 1
        self.page_sizes.append(len(names))
        if len(names) > self.page_limit:
            raise AcceptanceError(
                f"page returned {len(names)} entries above limit {self.page_limit}"
            )
        if names != sorted(names):
            raise AcceptanceError(f"page is not sorted by name: {names!r}")
        for name in names:
            if name in self._seen_names:
                raise AcceptanceError(f"duplicate entry across pages: {name}")
            if self.names and name <= self.names[-1]:
                raise AcceptanceError(
                    f"pages are not globally sorted: {self.names[-1]!r} then {name!r}"
                )
            self._seen_names.add(name)
            self.names.append(name)
        if truncated:
            if not names:
                raise AcceptanceError("truncated page is empty")
            if not isinstance(next_cursor, str) or not next_cursor:
                raise AcceptanceError("truncated page lacks next_cursor")
            if next_cursor in self._seen_cursors:
                raise AcceptanceError(f"pagination cursor repeated: {next_cursor}")
            self._seen_cursors.add(next_cursor)
        elif next_cursor is not None:
            raise AcceptanceError("terminal page returned a next_cursor")


def validate_tool_contract(tools: list[dict[str, Any]], require_restore: bool) -> bool:
    """Validate the final 17-tool workbench surface and restore schema."""
    by_name = {tool.get("name"): tool for tool in tools}
    restore_available = RESTORE_TOOL in by_name
    expected = set(BASE_WORKBENCH_TOOLS)
    if restore_available:
        expected.add(RESTORE_TOOL)
    actual = set(by_name)
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        raise AcceptanceError(
            f"unexpected workbench tool surface; missing={missing}, extra={extra}"
        )
    if require_restore and not restore_available:
        raise AcceptanceError(
            "workbench_restore is missing; integrate the restore-to-fork track "
            "or pass --allow-missing-restore while validating A+B only"
        )
    if not restore_available:
        return False
    schema = by_name[RESTORE_TOOL].get("schema")
    if not isinstance(schema, dict):
        raise AcceptanceError("workbench_restore lacks an input schema")
    required = set(schema.get("required", []))
    properties = schema.get("properties")
    expected_fields = {"id", "at_snapshot", "destination_id"}
    if required != expected_fields:
        raise AcceptanceError(
            f"workbench_restore required fields must be exactly {sorted(expected_fields)}"
        )
    if not isinstance(properties, dict) or set(properties) != expected_fields:
        raise AcceptanceError(
            f"workbench_restore properties must be exactly {sorted(expected_fields)}"
        )
    if schema.get("additionalProperties") is not False:
        raise AcceptanceError("workbench_restore must reject additional properties")
    at_snapshot = properties.get("at_snapshot")
    if not isinstance(at_snapshot, dict):
        raise AcceptanceError("workbench_restore at_snapshot lacks a schema")
    alternatives = at_snapshot.get("anyOf")
    if (
        not isinstance(alternatives, list)
        or len(alternatives) != 2
        or any(not isinstance(alternative, dict) for alternative in alternatives)
    ):
        raise AcceptanceError(
            "workbench_restore at_snapshot must use exactly two object alternatives"
        )
    accepted_types = {
        alternative.get("type")
        for alternative in alternatives
        if isinstance(alternative, dict)
    }
    if accepted_types != {"integer", "string"}:
        raise AcceptanceError(
            "workbench_restore at_snapshot must accept only a non-negative "
            f"snapshot id or checkpoint name, got {sorted(str(t) for t in accepted_types)}"
        )
    integer_schema = next(
        alternative
        for alternative in alternatives
        if isinstance(alternative, dict) and alternative.get("type") == "integer"
    )
    if integer_schema.get("minimum") != 0:
        raise AcceptanceError(
            "workbench_restore numeric at_snapshot must be non-negative"
        )
    string_schema = next(
        alternative
        for alternative in alternatives
        if isinstance(alternative, dict) and alternative.get("type") == "string"
    )
    if string_schema.get("minLength") != 1:
        raise AcceptanceError(
            "workbench_restore checkpoint names must be non-empty strings"
        )
    return True


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _tail(path: Path, lines: int = 100) -> str:
    if not path.exists():
        return "<log not created>"
    return "\n".join(
        path.read_text(encoding="utf-8", errors="replace").splitlines()[-lines:]
    )


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
    allow_missing_restore: bool
    require_all: bool


class LiveEnvironment:
    def __init__(self, config: LiveConfig) -> None:
        self.config = config
        self.server_bind = f"127.0.0.1:{config.server_port}"
        self.s3_endpoint = f"http://127.0.0.1:{config.rustfs_port}"
        self.server_log = config.state_dir / "nokv-server.log"
        self._server: subprocess.Popen[str] | None = None
        self._server_log_handle: Any = None
        self._container_started = False
        self._server_env_overrides: dict[str, str] = {}

    @property
    def aws_env(self) -> dict[str, str]:
        env = os.environ.copy()
        env.update(
            {
                "AWS_ACCESS_KEY_ID": "rustfsadmin",
                "AWS_SECRET_ACCESS_KEY": "rustfsadmin",
                "AWS_DEFAULT_REGION": "us-east-1",
                "AWS_EC2_METADATA_DISABLED": "true",
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
        cwd: Path | None = None,
    ) -> subprocess.CompletedProcess[str]:
        try:
            result = subprocess.run(
                args,
                cwd=cwd or self.config.repo_root,
                env=env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=timeout or self.config.command_timeout,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise AcceptanceError(
                f"command exceeded deadline ({exc.timeout}s): {args!r}"
            ) from exc
        if result.returncode != 0:
            raise AcceptanceError(
                f"command failed ({result.returncode}): {args!r}\n"
                f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
            )
        return result

    def build(self) -> None:
        if not self.config.build:
            if not self.config.nokv_bin.is_file():
                raise AcceptanceError(f"NoKV binary not found: {self.config.nokv_bin}")
            return
        self.run(
            [str(self.config.cargo_bin), "build", "-p", "nokv", "--bin", "nokv"],
            timeout=max(self.config.startup_timeout, 1200),
        )
        if not self.config.nokv_bin.is_file():
            raise AcceptanceError(f"cargo did not produce {self.config.nokv_bin}")

    def start_rustfs(self) -> None:
        env = self.aws_env
        env.update(
            {
                "LINGTAI_WORKBENCH_DATA_ROOT": str(self.config.state_dir),
                "LINGTAI_WORKBENCH_RUSTFS_CONTAINER": self.config.container,
                "LINGTAI_WORKBENCH_RUSTFS_IMAGE": self.config.rustfs_image,
                "LINGTAI_WORKBENCH_RUSTFS_HOST": "127.0.0.1",
                "LINGTAI_WORKBENCH_RUSTFS_PORT": str(self.config.rustfs_port),
                "LINGTAI_WORKBENCH_RUSTFS_CONSOLE_PORT": str(
                    self.config.rustfs_console_port
                ),
                "LINGTAI_WORKBENCH_S3_ENDPOINT": self.s3_endpoint,
                "LINGTAI_WORKBENCH_S3_BUCKET": self.config.bucket,
                "LINGTAI_WORKBENCH_RUSTFS_DATA_DIR": str(
                    self.config.state_dir / "rustfs"
                ),
            }
        )
        script = self.config.repo_root / "scripts/lingtai-workbench/start_rustfs.sh"
        self.run(
            [str(script)],
            timeout=self.config.startup_timeout,
            env=env,
        )
        self._container_started = True

    def start_server(self) -> None:
        if self._server is not None:
            raise AcceptanceError("NoKV server is already running")
        self.config.state_dir.mkdir(parents=True, exist_ok=True)
        self._server_log_handle = self.server_log.open("a", encoding="utf-8")
        args = (
            [str(self.config.nokv_bin)]
            + self.common_nokv_args()
            + [
                "--meta",
                str(self.config.state_dir / "meta"),
                "--object-gc-interval-ms",
                "100",
                "--object-gc-limit",
                "4096",
                "--history-gc-interval-ms",
                "100",
                "--history-gc-limit",
                "4096",
                "serve",
            ]
        )
        server_env = self.aws_env
        server_env["NOKV_TEST_SNAPSHOT_BARRIER_DIR"] = str(
            self.config.state_dir / "snapshot-barriers"
        )
        server_env["NOKV_TEST_BARRIER_TIMEOUT_MS"] = str(
            int(self.config.tool_timeout * 1000)
        )
        server_env.update(self._server_env_overrides)
        self._server = subprocess.Popen(
            args,
            cwd=self.config.repo_root,
            env=server_env,
            text=True,
            stdout=self._server_log_handle,
            stderr=subprocess.STDOUT,
        )
        deadline = time.monotonic() + self.config.startup_timeout
        last_error = "not attempted"
        while time.monotonic() < deadline:
            if self._server.poll() is not None:
                raise AcceptanceError(
                    f"NoKV server exited during startup ({self._server.returncode})\n"
                    f"{_tail(self.server_log)}"
                )
            try:
                if self.http_text("/readyz", timeout=1).strip() == "ready":
                    return
            except (AcceptanceError, urllib.error.URLError, OSError) as exc:
                last_error = str(exc)
            time.sleep(0.1)
        raise AcceptanceError(
            f"NoKV server did not become ready before deadline: {last_error}\n"
            f"{_tail(self.server_log)}"
        )

    def stop_server(self) -> None:
        process = self._server
        self._server = None
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        if self._server_log_handle is not None:
            self._server_log_handle.close()
            self._server_log_handle = None

    def kill_server(self) -> None:
        """Stop the server without running Rust destructors or a final checkpoint."""
        process = self._server
        self._server = None
        if process is not None and process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        if self._server_log_handle is not None:
            self._server_log_handle.close()
            self._server_log_handle = None

    def restart_server(self, env_overrides: dict[str, str] | None = None) -> None:
        self.stop_server()
        self._server_env_overrides = dict(env_overrides or {})
        self.start_server()

    def cleanup(self) -> None:
        self.stop_server()
        if self._container_started or shutil.which("docker"):
            subprocess.run(
                ["docker", "rm", "-f", self.config.container],
                text=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=30,
                check=False,
            )
        if not self.config.keep_state:
            shutil.rmtree(self.config.state_dir, ignore_errors=True)

    def cli(self, *command: str) -> str:
        result = self.run(
            [str(self.config.nokv_bin)] + self.common_nokv_args() + list(command),
            env=self.aws_env,
        )
        return result.stdout

    def cli_result(
        self, *command: str, timeout: float | None = None
    ) -> subprocess.CompletedProcess[str]:
        args = [str(self.config.nokv_bin)] + self.common_nokv_args() + list(command)
        try:
            return subprocess.run(
                args,
                cwd=self.config.repo_root,
                env=self.aws_env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=timeout or self.config.command_timeout,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise AcceptanceError(
                f"CLI probe exceeded deadline ({exc.timeout}s): {args!r}"
            ) from exc

    def http_text(self, path: str, *, timeout: float = 5, method: str = "GET") -> str:
        request = urllib.request.Request(
            f"http://{self.server_bind}{path}", method=method
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return response.read().decode("utf-8")
        except urllib.error.URLError:
            raise
        except Exception as exc:
            raise AcceptanceError(f"HTTP {method} {path} failed: {exc}") from exc

    def manual_gc(self) -> dict[str, Any]:
        raw = self.http_text("/gc?limit=4096", timeout=30, method="POST")
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise AcceptanceError(f"manual GC returned invalid JSON: {raw!r}") from exc
        if not isinstance(value, dict):
            raise AcceptanceError(f"manual GC did not return an object: {value!r}")
        return value

    def stats(self) -> dict[str, Any]:
        raw = self.http_text("/stats", timeout=10)
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise AcceptanceError(f"stats returned invalid JSON: {raw!r}") from exc
        if not isinstance(value, dict):
            raise AcceptanceError(f"stats did not return an object: {value!r}")
        return value

    def object_puts(self) -> int:
        value = self.stats().get("object_puts")
        if not isinstance(value, int) or value < 0:
            raise AcceptanceError(f"stats lacks a valid object_puts counter: {value!r}")
        return value

    def object_keys(self) -> set[str]:
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
        return {
            item["Key"]
            for item in payload.get("Contents", [])
            if isinstance(item, dict) and isinstance(item.get("Key"), str)
        }


class WorkbenchClient:
    def __init__(
        self,
        mcp_client_class: type,
        environment: LiveEnvironment,
        launch: ResolvedMcpLaunch,
    ) -> None:
        self.root = launch.root
        self._timeout = environment.config.tool_timeout
        self._client = mcp_client_class(
            command=launch.command,
            args=launch.args,
            env=launch.env,
        )
        self._client.start()

    def close(self) -> None:
        self._client.close()

    def tools(self) -> list[dict[str, Any]]:
        return self._client.list_tools(timeout=min(self._timeout, 30))

    def raw_call(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        result = self._client.call_tool(name, arguments, timeout=self._timeout)
        if not isinstance(result, dict):
            raise AcceptanceError(f"{name} returned a non-object: {result!r}")
        return result

    def call(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        result = self.raw_call(name, arguments)
        if result.get("status") == "error" or "code" in result:
            error = decode_tool_error(result)
            raise AcceptanceError(
                f"{name} failed with {error.code}: {error.message}; "
                f"retryable={error.retryable}, details={error.details}"
            )
        if result.get("status") != "success":
            raise AcceptanceError(f"{name} returned malformed success: {result!r}")
        return result

    def expect_error(
        self,
        name: str,
        arguments: dict[str, Any],
        expected_code: str,
    ) -> ToolError:
        error = decode_tool_error(self.raw_call(name, arguments))
        if error.code != expected_code:
            raise AcceptanceError(
                f"{name} returned {error.code}, expected {expected_code}: {error.message}"
            )
        return error


def _read_lines(result: dict[str, Any]) -> list[str]:
    if result.get("bytes_encoding") == "base64":
        raw = base64.b64decode(result.get("bytes", ""), validate=True)
        return raw.decode("utf-8").splitlines()
    items = result.get("items")
    if not isinstance(items, list):
        raise AcceptanceError(f"read response lacks items: {result!r}")
    lines: list[str] = []
    for item in items:
        value = item.get("value") if isinstance(item, dict) else None
        text = value.get("text") if isinstance(value, dict) else None
        if not isinstance(text, str):
            raise AcceptanceError(f"read response contains a non-text item: {item!r}")
        lines.append(text)
    return lines


def _read_json_object(result: dict[str, Any]) -> dict[str, Any]:
    if result.get("format") != "structured":
        raise AcceptanceError(f"JSON object read has the wrong format: {result!r}")
    if result.get("record_type") != "json_object":
        raise AcceptanceError(f"read response is not a JSON object: {result!r}")
    items = result.get("items")
    if not isinstance(items, list):
        raise AcceptanceError(f"JSON object read lacks items: {result!r}")
    value: dict[str, Any] = {}
    for item in items:
        record = item.get("value") if isinstance(item, dict) else None
        key = record.get("key") if isinstance(record, dict) else None
        if not isinstance(key, str) or "value" not in record:
            raise AcceptanceError(
                f"JSON object read contains a malformed record: {item!r}"
            )
        if key in value:
            raise AcceptanceError(f"JSON object read repeated key {key!r}")
        value[key] = record["value"]
    if result.get("truncated") is True:
        raise AcceptanceError(
            "JSON object read was unexpectedly truncated; manifest validation "
            "requires every top-level field"
        )
    if result.get("next_cursor") is not None:
        raise AcceptanceError(
            f"terminal JSON object read returned next_cursor: {result!r}"
        )
    return value


class AcceptanceSuite:
    def __init__(
        self,
        environment: LiveEnvironment,
        agent_class: type,
        mcp_client_class: type,
    ) -> None:
        self.env = environment
        self.agent_class = agent_class
        self.mcp_client_class = mcp_client_class
        self.clients: list[WorkbenchClient] = []
        self.results: dict[str, dict[str, Any]] = {}
        self.restore_available = False
        self.root_a = ""
        self.root_b = ""
        self.client_a: WorkbenchClient
        self.client_a2: WorkbenchClient
        self.client_b: WorkbenchClient
        self.foreign_workbench_id = "shared-root-binding"
        self.foreign_snapshot_id = 0
        self.foreign_snapshot_name = "forged-agent-a"
        self.restore_state: dict[str, Any] = {}
        self._registration_generation = 0

    def _record(
        self,
        name: str,
        status: str,
        started: float,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.results[name] = {
            "status": status,
            "duration_seconds": round(time.monotonic() - started, 3),
            "details": details or {},
        }

    def scenario(
        self, name: str, function: Callable[[], dict[str, Any] | None]
    ) -> None:
        print(f"[live-e2e] START {name}", flush=True)
        started = time.monotonic()
        try:
            details = function() or {}
        except Exception as exc:
            self._record(name, "failed", started, {"error": str(exc)})
            print(f"[live-e2e] FAIL  {name}: {exc}", flush=True)
            raise
        self._record(name, "passed", started, details)
        print(f"[live-e2e] PASS  {name}", flush=True)

    def skip(self, name: str, reason: str) -> None:
        started = time.monotonic()
        self._record(name, "skipped", started, {"reason": reason})
        print(f"[live-e2e] SKIP  {name}: {reason}", flush=True)

    @staticmethod
    def _mock_lingtai_service() -> MagicMock:
        service = MagicMock()
        service.get_adapter.return_value = MagicMock()
        service.provider = "gemini"
        service.model = "nokv-live-acceptance"
        return service

    @staticmethod
    def _resolved_launch_from_client(client: Any) -> ResolvedMcpLaunch:
        command = getattr(client, "_command", None)
        args = getattr(client, "_args", None)
        env = getattr(client, "_env", None)
        if not isinstance(command, str) or not isinstance(args, list):
            raise AcceptanceError("LingTai MCP client lacks its resolved launch config")
        if any(not isinstance(argument, str) for argument in args):
            raise AcceptanceError(f"LingTai MCP args are malformed: {args!r}")
        if not isinstance(env, dict) or any(
            not isinstance(key, str) or not isinstance(value, str)
            for key, value in env.items()
        ):
            raise AcceptanceError("LingTai MCP client lacks a string environment")
        try:
            root_index = args.index("--workbench-root") + 1
            root = args[root_index]
        except (ValueError, IndexError) as exc:
            raise AcceptanceError(
                f"LingTai resolved MCP args lack --workbench-root: {args!r}"
            ) from exc
        if "{" in root or not root.startswith("/"):
            raise AcceptanceError(f"LingTai left an unresolved root: {root!r}")
        return ResolvedMcpLaunch(command, list(args), dict(env), root)

    def _registered_agent_launch(self, agent_name: str) -> ResolvedMcpLaunch:
        """Exercise real Agent registration+retry, then reuse its resolved config."""
        workdir = self.env.config.state_dir / "agents" / agent_name
        workdir.mkdir(parents=True, exist_ok=True)
        mcp_env = {
            key: value
            for key, value in self.env.aws_env.items()
            if key not in {"LINGTAI_AGENT_DIR", "LINGTAI_MCP_NAME"}
        }
        registry = {
            "name": "nokv-workbench",
            "summary": "NoKV live acceptance workbench.",
            "transport": "stdio",
            "command": str(self.env.config.nokv_bin),
            "args": self.env.mcp_args(WORKBENCH_ROOT_TEMPLATE),
            "source": "nokv-live-acceptance",
        }
        (workdir / "mcp_registry.jsonl").write_text(
            json.dumps(registry, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )
        (workdir / "init.json").write_text(
            json.dumps(
                {
                    "mcp": {
                        "nokv-workbench": {
                            "type": "stdio",
                            "command": str(self.env.config.nokv_bin),
                            "args": self.env.mcp_args(WORKBENCH_ROOT_TEMPLATE),
                            "env": mcp_env,
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
                service=self._mock_lingtai_service(),
                agent_name=agent_name,
                working_dir=workdir,
                capabilities={"mcp": {}},
            )
            specs = getattr(agent, "_mcp_init_specs", {})
            spec = specs.get("nokv-workbench") if isinstance(specs, dict) else None
            initial_client = spec.get("client") if isinstance(spec, dict) else None
            if initial_client is None:
                raise AcceptanceError("LingTai Agent did not register the NoKV MCP")
            initial_launch = self._resolved_launch_from_client(initial_client)

            initial_client.close()
            retry = agent._retry_failed_mcps()
            if retry.get("recovered") != ["nokv-workbench"]:
                raise AcceptanceError(
                    f"LingTai Agent MCP retry did not recover NoKV: {retry!r}"
                )
            retried_spec = agent._mcp_init_specs.get("nokv-workbench")
            retried_client = (
                retried_spec.get("client") if isinstance(retried_spec, dict) else None
            )
            if retried_client is None:
                raise AcceptanceError("LingTai MCP retry returned no live client")
            retried_launch = self._resolved_launch_from_client(retried_client)
            if retried_launch.root != initial_launch.root:
                raise AcceptanceError(
                    "LingTai resolved workbench root changed during MCP retry"
                )

            self._registration_generation += 1
            handler = getattr(agent, "_tool_handlers", {}).get("workbench_create")
            if not callable(handler):
                raise AcceptanceError(
                    "LingTai Agent did not install the registered MCP handler"
                )
            handler_result = handler(
                {"id": f"lingtai-registration-probe-{self._registration_generation}"}
            )
            if not isinstance(handler_result, dict) or handler_result.get(
                "status"
            ) != "success":
                raise AcceptanceError(
                    f"registered LingTai MCP handler failed: {handler_result!r}"
                )
            return retried_launch
        finally:
            if agent is not None:
                agent.stop(timeout=5.0)

    def connect_clients(self) -> None:
        launch_a = self._registered_agent_launch("checkpoint-agent-a")
        launch_b = self._registered_agent_launch("checkpoint-agent-b")
        expected_a = launch_a.root
        expected_b = launch_b.root
        if expected_a == expected_b:
            raise AcceptanceError(
                "LingTai placeholder expansion did not isolate agents"
            )
        if self.root_a and (self.root_a != expected_a or self.root_b != expected_b):
            raise AcceptanceError(
                "LingTai resolved workbench root changed after restart"
            )
        self.root_a, self.root_b = expected_a, expected_b
        self.client_a = WorkbenchClient(self.mcp_client_class, self.env, launch_a)
        self.client_a2 = WorkbenchClient(self.mcp_client_class, self.env, launch_a)
        self.client_b = WorkbenchClient(self.mcp_client_class, self.env, launch_b)
        self.clients = [self.client_a, self.client_a2, self.client_b]
        self.restore_available = validate_tool_contract(
            self.client_a.tools(), not self.env.config.allow_missing_restore
        )

    def close_clients(self) -> None:
        for client in self.clients:
            try:
                client.close()
            except Exception:
                pass
        self.clients = []

    def restart_stack(self, env_overrides: dict[str, str] | None = None) -> None:
        self.close_clients()
        self.env.restart_server(env_overrides)
        self.connect_clients()

    def crash_restart_stack(self) -> None:
        # Kill immediately after the caller-observed ACK. Closing MCP clients
        # first would add enough delay to hide an async WAL acknowledgment bug.
        self.env.kill_server()
        self.close_clients()
        self.env.start_server()
        self.connect_clients()

    @staticmethod
    def _put(
        client: WorkbenchClient,
        workbench_id: str,
        path: str,
        text: str,
        *,
        replace: bool = False,
    ) -> dict[str, Any]:
        return client.call(
            "workbench_put_file",
            {
                "id": workbench_id,
                "section": "outputs",
                "path": path,
                "text": text,
                "replace": replace,
            },
        )

    @staticmethod
    def _create_committed(client: WorkbenchClient, workbench_id: str) -> None:
        client.call("workbench_create", {"id": workbench_id})
        client.call(
            "workbench_commit",
            {"id": workbench_id, "manifest": {"acceptance": workbench_id}},
        )

    @staticmethod
    def _assert_read(
        client: WorkbenchClient,
        workbench_id: str,
        path: str,
        expected_line: str,
        *,
        at_snapshot: int | str | None = None,
    ) -> None:
        args: dict[str, Any] = {
            "id": workbench_id,
            "section": "outputs",
            "path": path,
        }
        if at_snapshot is not None:
            args["at_snapshot"] = at_snapshot
        lines = _read_lines(client.call("workbench_read", args))
        if lines != [expected_line]:
            raise AcceptanceError(
                f"unexpected content for {workbench_id}/{path}: {lines!r}, "
                f"expected {[expected_line]!r}"
            )

    @staticmethod
    def _assert_output_absent(
        client: WorkbenchClient, workbench_id: str, path: str
    ) -> None:
        result = client.raw_call(
            "workbench_stat",
            {"id": workbench_id, "section": "outputs", "path": path},
        )
        error = decode_tool_error(result)
        if "not found" not in error.message.lower():
            raise AcceptanceError(
                f"expected {workbench_id}/{path} to be absent, got {error}"
            )

    def _list_all(
        self,
        client: WorkbenchClient,
        workbench_id: str,
        *,
        at_snapshot: int | str | None = None,
    ) -> PageAccumulator:
        cursor: str | None = None
        pages = PageAccumulator(self.env.config.profile.page_limit)
        deadline = time.monotonic() + self.env.config.tool_timeout
        while True:
            if time.monotonic() >= deadline:
                raise AcceptanceError("workbench_list pagination exceeded deadline")
            args: dict[str, Any] = {
                "id": workbench_id,
                "section": "outputs",
                "limit": self.env.config.profile.page_limit,
            }
            if cursor is not None:
                args["cursor"] = cursor
            if at_snapshot is not None:
                args["at_snapshot"] = at_snapshot
            result = client.call("workbench_list", args)
            entries = result.get("entries")
            if not isinstance(entries, list):
                raise AcceptanceError(f"list response lacks entries: {result!r}")
            names = [entry.get("name") for entry in entries]
            if any(not isinstance(name, str) for name in names):
                raise AcceptanceError(
                    f"list response contains malformed names: {names!r}"
                )
            next_cursor = result.get("next_cursor")
            truncated = result.get("truncated")
            if not isinstance(truncated, bool):
                raise AcceptanceError(
                    f"list response lacks truncated boolean: {result!r}"
                )
            pages.add(names, next_cursor, truncated)
            if not truncated:
                return pages
            cursor = next_cursor

    def scenario_placeholder_and_tools(self) -> dict[str, Any]:
        return {
            "root_a": self.root_a,
            "root_b": self.root_b,
            "tool_count": len(self.client_a.tools()),
            "restore_available": self.restore_available,
            "agent_registration_retry_handler": True,
        }

    def scenario_linearizable_renew(self) -> dict[str, Any]:
        workbench_id = "lease-linearization"
        self._create_committed(self.client_a, workbench_id)
        self._put(self.client_a, workbench_id, "lease.txt", "lease-base\n")
        minted = self.client_a.call(
            "workbench_snapshot",
            {"id": workbench_id, "name": "lease-base", "ttl_days": 1},
        )
        snapshot_id = int(minted["snapshot_id"])
        barrier = threading.Barrier(2)

        def renew_worker(client: WorkbenchClient, ttl_days: int) -> list[int]:
            barrier.wait(timeout=self.env.config.tool_timeout)
            expiries: list[int] = []
            for _ in range(self.env.config.profile.renew_rounds):
                result = client.call(
                    "workbench_snapshot_renew",
                    {
                        "id": workbench_id,
                        "snapshot_id": snapshot_id,
                        "ttl_days": ttl_days,
                    },
                )
                expiry = result.get("lease_expires_at")
                if not isinstance(expiry, int):
                    raise AcceptanceError(
                        f"renew lacks authoritative expiry: {result!r}"
                    )
                expiries.append(expiry)
            return expiries

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            short = executor.submit(renew_worker, self.client_a, 1)
            long = executor.submit(renew_worker, self.client_a2, 90)
            promised = short.result(timeout=self.env.config.tool_timeout * 2)
            promised.extend(long.result(timeout=self.env.config.tool_timeout * 2))
        listed = self.client_a.call("workbench_snapshot_list", {"id": workbench_id})
        checkpoints = listed.get("checkpoints")
        if not isinstance(checkpoints, list):
            raise AcceptanceError(f"snapshot list malformed: {listed!r}")
        row = next(
            (row for row in checkpoints if row.get("snapshot_id") == snapshot_id), None
        )
        if row is None:
            raise AcceptanceError(f"renewed snapshot missing from registry: {listed!r}")
        final_expiry = row.get("live_lease_expires_unix_ms")
        if not isinstance(final_expiry, int) or final_expiry < max(promised):
            raise AcceptanceError(
                f"renew shortened a successful promise: final={final_expiry}, "
                f"max_success={max(promised)}"
            )
        self.env.cli(
            "retire-snapshot",
            f"{self.root_a}/{workbench_id}",
            str(snapshot_id),
        )
        return {
            "snapshot_id": snapshot_id,
            "calls": len(promised),
            "max_promised_expiry": max(promised),
            "final_expiry": final_expiry,
        }

    def scenario_reaper_accounting(self) -> dict[str, Any]:
        gc = self.env.manual_gc()
        candidates, reaped, conflicted = self._reap_counts(gc)
        return {
            "expired_candidates": candidates,
            "reaped": reaped,
            "conflicted": conflicted,
        }

    @staticmethod
    def _reap_counts(payload: dict[str, Any]) -> tuple[int, int, int]:
        try:
            reap = payload["object_gc"]["snapshot_reap"]
            candidates = int(reap["expired_candidates"])
            reaped = int(reap["reaped"])
            conflicted = int(reap["conflicted"])
        except (KeyError, TypeError, ValueError) as exc:
            raise AcceptanceError(
                f"GC/stats payload lacks snapshot_reap counters: {payload!r}"
            ) from exc
        if reaped + conflicted != candidates:
            raise AcceptanceError(
                "snapshot reaper accounting violated: "
                f"reaped={reaped}, conflicted={conflicted}, candidates={candidates}"
            )
        return candidates, reaped, conflicted

    def probe_short_lease_mint(self) -> tuple[bool, str]:
        workbench_id = "renew-reaper-race"
        self._create_committed(self.client_a, workbench_id)
        self._put(self.client_a, workbench_id, "race.txt", "race-value\n")
        root = f"{self.root_a}/{workbench_id}"
        result = self.env.cli_result("snapshot", root, "200")
        if result.returncode != 0:
            diagnostic = f"{result.stdout}\n{result.stderr}".lower()
            if "too many arguments" in diagnostic or "usage:" in diagnostic:
                return (
                    False,
                    "NoKV CLI does not yet support breaking `snapshot PATH [LEASE_MS]`; "
                    "the final integration capability probe will enable this scenario",
                )
            raise AcceptanceError(
                "short-lease snapshot capability probe failed unexpectedly: "
                f"stdout={result.stdout!r}, stderr={result.stderr!r}"
            )
        snapshot_id = parse_snapshot_id(result.stdout)
        self.env.cli("retire-snapshot", root, str(snapshot_id))
        return True, ""

    def _wait_for_marker(self, path: Path) -> None:
        deadline = time.monotonic() + self.env.config.tool_timeout
        while not path.exists():
            if time.monotonic() >= deadline:
                raise AcceptanceError(
                    f"live-test barrier marker did not appear before deadline: {path}"
                )
            time.sleep(0.002)

    @staticmethod
    def _release_marker(path: Path) -> None:
        path.write_text("continue\n", encoding="utf-8")

    def _deterministic_reaper_conflict(
        self, workbench_id: str, root: str
    ) -> dict[str, int]:
        _, _, health_conflicts_before = self._reap_counts(self.env.stats())
        barrier_dir = self.env.config.state_dir / "snapshot-barriers"
        shutil.rmtree(barrier_dir, ignore_errors=True)
        barrier_dir.mkdir(parents=True)
        minted = self.env.cli_result("snapshot", root, "300")
        if minted.returncode != 0:
            raise AcceptanceError(
                "deterministic short-lease mint failed: "
                f"stdout={minted.stdout!r}, stderr={minted.stderr!r}"
            )
        snapshot_id = parse_snapshot_id(minted.stdout)
        lease_expiry = parse_snapshot_expiry(minted.stdout)
        renew_arm = barrier_dir / f"{snapshot_id}.renew-read.arm"
        renew_ready = barrier_dir / f"{snapshot_id}.renew-read.ready"
        renew_continue = barrier_dir / f"{snapshot_id}.renew-read.continue"
        reaper_arm = barrier_dir / f"{snapshot_id}.reaper-scan.arm"
        reaper_ready = barrier_dir / f"{snapshot_id}.reaper-scan.ready"
        reaper_continue = barrier_dir / f"{snapshot_id}.reaper-scan.continue"
        renew_arm.write_text("armed\n", encoding="utf-8")
        reaper_arm.write_text("armed\n", encoding="utf-8")

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            renew_future = executor.submit(
                self.client_a.raw_call,
                "workbench_snapshot_renew",
                {
                    "id": workbench_id,
                    "snapshot_id": snapshot_id,
                    "ttl_days": 1,
                },
            )
            self._wait_for_marker(renew_ready)
            if time.time_ns() // 1_000_000 >= lease_expiry:
                raise AcceptanceError(
                    "renew did not reach its pre-commit barrier before lease expiry"
                )
            while True:
                remaining_ms = lease_expiry - time.time_ns() // 1_000_000
                if remaining_ms <= 0:
                    break
                time.sleep(min(remaining_ms / 1000, 0.002))

            # The server's real background worker must own the old scan so its
            # durable health counter, not only a one-off manual GC response,
            # records the version conflict.
            self._wait_for_marker(reaper_ready)
            self._release_marker(renew_continue)
            renew_result = renew_future.result(timeout=self.env.config.tool_timeout)
            if renew_result.get("status") != "success":
                error = decode_tool_error(renew_result)
                raise AcceptanceError(
                    "pre-expiry renew lost the deterministic race: "
                    f"{error.code}: {error.message}"
                )
            self._release_marker(reaper_continue)

        health_conflicts = health_conflicts_before
        deadline = time.monotonic() + self.env.config.tool_timeout
        while health_conflicts <= health_conflicts_before:
            _, _, health_conflicts = self._reap_counts(self.env.stats())
            if time.monotonic() >= deadline:
                raise AcceptanceError(
                    "background reaper health never recorded the forced CAS conflict"
                )
            time.sleep(0.002)
        self._assert_read(
            self.client_a,
            workbench_id,
            "race.txt",
            "race-value",
            at_snapshot=snapshot_id,
        )
        self.env.cli("retire-snapshot", root, str(snapshot_id))
        return {
            "manual_conflicts": 0,
            "health_conflicts": health_conflicts - health_conflicts_before,
        }

    def scenario_renew_reaper_short_lease(self) -> dict[str, Any]:
        workbench_id = "renew-reaper-race"
        root = f"{self.root_a}/{workbench_id}"
        deterministic = self._deterministic_reaper_conflict(workbench_id, root)
        successes = 1
        terminal_failures = 0
        manual_conflicts = deterministic["manual_conflicts"]
        health_conflicts_max = deterministic["health_conflicts"]
        offsets_ms = (185, 190, 195, 198, 200)

        for round_index in range(self.env.config.profile.reaper_rounds - 1):
            minted = self.env.cli_result("snapshot", root, "200")
            if minted.returncode != 0:
                raise AcceptanceError(
                    f"short-lease mint failed in round {round_index}: "
                    f"stdout={minted.stdout!r}, stderr={minted.stderr!r}"
                )
            snapshot_id = parse_snapshot_id(minted.stdout)
            target = time.monotonic() + offsets_ms[round_index % len(offsets_ms)] / 1000
            barrier = threading.Barrier(3)

            def renew() -> dict[str, Any]:
                barrier.wait(timeout=self.env.config.tool_timeout)
                return self.client_a.raw_call(
                    "workbench_snapshot_renew",
                    {
                        "id": workbench_id,
                        "snapshot_id": snapshot_id,
                        "ttl_days": 1,
                    },
                )

            def reap() -> dict[str, Any]:
                barrier.wait(timeout=self.env.config.tool_timeout)
                return self.env.manual_gc()

            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                renew_future = executor.submit(renew)
                reap_future = executor.submit(reap)
                while True:
                    remaining = target - time.monotonic()
                    if remaining <= 0:
                        break
                    time.sleep(min(remaining, 0.002))
                barrier.wait(timeout=self.env.config.tool_timeout)
                renew_result = renew_future.result(timeout=self.env.config.tool_timeout)
                gc_result = reap_future.result(timeout=self.env.config.tool_timeout)

            _, _, conflicted = self._reap_counts(gc_result)
            manual_conflicts += conflicted
            try:
                _, _, health_conflicted = self._reap_counts(self.env.stats())
            except AcceptanceError:
                health_conflicted = 0
            health_conflicts_max = max(health_conflicts_max, health_conflicted)

            if renew_result.get("status") == "success":
                promised_expiry = renew_result.get("lease_expires_at")
                if not isinstance(promised_expiry, int):
                    raise AcceptanceError(
                        f"successful race renew lacks authoritative expiry: {renew_result!r}"
                    )
                successes += 1
                # Drain newer scans before asserting the old scan cannot reap a
                # successfully renewed version. Each drain is itself bounded by
                # the HTTP request deadline; no probabilistic fixed sleep is used.
                for _ in range(3):
                    drained = self.env.manual_gc()
                    _, _, drained_conflicts = self._reap_counts(drained)
                    manual_conflicts += drained_conflicts
                self._assert_read(
                    self.client_a,
                    workbench_id,
                    "race.txt",
                    "race-value",
                    at_snapshot=snapshot_id,
                )
                self.env.cli("retire-snapshot", root, str(snapshot_id))
            else:
                error = decode_tool_error(renew_result)
                if error.code not in {"SnapshotLeaseExpired", "SnapshotNotFound"}:
                    raise AcceptanceError(
                        f"race renew failed with unexpected typed error: {error}"
                    )
                terminal_failures += 1
                self.env.manual_gc()

        if successes == 0:
            raise AcceptanceError(
                "short-lease race produced no successful renew to validate"
            )
        observed_conflicts = manual_conflicts + health_conflicts_max
        if observed_conflicts == 0:
            raise AcceptanceError(
                "short-lease race observed no reaper CAS conflict; rerun is not an "
                "acceptance substitute because conflicted > 0 is a required invariant"
            )
        return {
            "rounds": self.env.config.profile.reaper_rounds,
            "deterministic_barrier": True,
            "renew_successes": successes,
            "terminal_failures": terminal_failures,
            "manual_conflicts": manual_conflicts,
            "health_conflicts_max": health_conflicts_max,
        }

    def _assert_foreign_snapshot_rejected(self, at_snapshot: int | str) -> int:
        workbench_id = self.foreign_workbench_id
        checks = [
            (
                "workbench_stat",
                {"id": workbench_id, "section": "outputs", "path": "owner.txt"},
            ),
            ("workbench_list", {"id": workbench_id, "section": "outputs"}),
            (
                "workbench_read",
                {"id": workbench_id, "section": "outputs", "path": "owner.txt"},
            ),
        ]
        for tool, args in checks:
            args["at_snapshot"] = at_snapshot
            self.client_b.expect_error(tool, args, "SnapshotRootMismatch")
        renew_target = (
            {"snapshot_id": at_snapshot}
            if isinstance(at_snapshot, int)
            else {"name": at_snapshot}
        )
        self.client_b.expect_error(
            "workbench_snapshot_renew",
            {"id": workbench_id, "ttl_days": 30, **renew_target},
            "SnapshotRootMismatch",
        )
        return len(checks) + 1

    def scenario_root_binding(self) -> dict[str, Any]:
        workbench_id = self.foreign_workbench_id
        for client, content in ((self.client_a, "agent-a"), (self.client_b, "agent-b")):
            self._create_committed(client, workbench_id)
            self._put(client, workbench_id, "owner.txt", content + "\n")
        minted = self.client_a.call(
            "workbench_snapshot",
            {"id": workbench_id, "name": "agent-a-owned", "ttl_days": 7},
        )
        snapshot_id = int(minted["snapshot_id"])
        self.foreign_snapshot_id = snapshot_id
        typed_checks = self._assert_foreign_snapshot_rejected(snapshot_id)

        registry_row = json.dumps(
            {
                "name": self.foreign_snapshot_name,
                "snapshot_id": snapshot_id,
                "read_version": minted["read_version"],
                "lease_expires_unix_ms": minted["lease_expires_at"],
                "created_at": 1,
                "reason": "mint",
            },
            separators=(",", ":"),
        )
        self.client_b.call(
            "workbench_put_file",
            {
                "id": workbench_id,
                "section": "metadata",
                "path": "checkpoints.jsonl",
                "text": registry_row + "\n",
            },
        )
        typed_checks += self._assert_foreign_snapshot_rejected(
            self.foreign_snapshot_name
        )
        self._assert_read(self.client_a, workbench_id, "owner.txt", "agent-a")
        self._assert_read(self.client_b, workbench_id, "owner.txt", "agent-b")
        return {"snapshot_id": snapshot_id, "typed_checks": typed_checks}

    def scenario_historical_scan(self) -> dict[str, Any]:
        workbench_id = "history-pagination"
        self._create_committed(self.client_a, workbench_id)
        expected = [
            f"entry-{index:03d}.txt"
            for index in range(self.env.config.profile.history_entries)
        ]
        for index, name in enumerate(expected):
            self._put(
                self.client_a,
                workbench_id,
                name,
                f"snapshot-{index:03d}\n",
            )
        minted = self.client_a.call(
            "workbench_snapshot",
            {"id": workbench_id, "name": "history-base", "ttl_days": 7},
        )
        snapshot_id = int(minted["snapshot_id"])
        outputs = f"{self.root_a}/{workbench_id}/outputs"
        self.env.cli("rm", f"{outputs}/{expected[0]}")
        self.env.cli(
            "rename", f"{outputs}/{expected[1]}", f"{outputs}/renamed-entry.txt"
        )
        self.env.cli("rm", f"{outputs}/{expected[2]}")
        self._put(
            self.client_a,
            workbench_id,
            expected[2],
            "current-recreated\n",
        )

        historical = self._list_all(
            self.client_a, workbench_id, at_snapshot=snapshot_id
        )
        if historical.names != expected:
            raise AcceptanceError(
                f"snapshot pagination mismatch: got={historical.names!r}, expected={expected!r}"
            )
        current = self._list_all(self.client_a, workbench_id)
        expected_current = sorted(
            (set(expected) - {expected[0], expected[1]}) | {"renamed-entry.txt"}
        )
        if current.names != expected_current:
            raise AcceptanceError(
                f"current pagination mismatch: got={current.names!r}, "
                f"expected={expected_current!r}"
            )
        page_limit = self.env.config.profile.page_limit
        if len(expected) > page_limit and historical.page_count <= 1:
            raise AcceptanceError("snapshot list did not exercise multiple pages")
        if len(expected_current) > page_limit and current.page_count <= 1:
            raise AcceptanceError("current list did not exercise multiple pages")
        if len(expected) == 101 and page_limit == 7:
            expected_historical_pages = (len(expected) + page_limit - 1) // page_limit
            expected_current_pages = (
                len(expected_current) + page_limit - 1
            ) // page_limit
            if historical.page_count != expected_historical_pages:
                raise AcceptanceError(
                    "full snapshot pagination did not produce exactly 15 pages: "
                    f"{historical.page_count}"
                )
            if current.page_count != expected_current_pages:
                raise AcceptanceError(
                    "full current pagination did not produce exactly 15 pages: "
                    f"{current.page_count}"
                )
        for index in range(3):
            self._assert_read(
                self.client_a,
                workbench_id,
                expected[index],
                f"snapshot-{index:03d}",
                at_snapshot=snapshot_id,
            )
        self._assert_read(self.client_a, workbench_id, expected[2], "current-recreated")
        self.env.cli("retire-snapshot", f"{self.root_a}/{workbench_id}", str(snapshot_id))
        return {
            "snapshot_id": snapshot_id,
            "entries": len(expected),
            "page_limit": page_limit,
            "historical_pages": historical.page_count,
            "current_pages": current.page_count,
        }

    def scenario_restart_root_binding(self) -> dict[str, Any]:
        old_roots = (self.root_a, self.root_b)
        self.restart_stack()
        if old_roots != (self.root_a, self.root_b):
            raise AcceptanceError("resolved roots changed across server/MCP restart")
        typed_checks = self._assert_foreign_snapshot_rejected(
            self.foreign_snapshot_id
        )
        typed_checks += self._assert_foreign_snapshot_rejected(
            self.foreign_snapshot_name
        )
        return {
            "root_a": self.root_a,
            "root_b": self.root_b,
            "typed_checks": typed_checks,
        }

    def scenario_ack_durability_after_sigkill(self) -> dict[str, Any]:
        workbench_id = "ack-durability"
        self._create_committed(self.client_a, workbench_id)
        self._put(self.client_a, workbench_id, "durable.txt", "durable-before-kill\n")
        minted = self.client_a.call(
            "workbench_snapshot",
            {"id": workbench_id, "name": "acked-before-kill", "ttl_days": 7},
        )
        snapshot_id = int(minted["snapshot_id"])

        self.crash_restart_stack()

        listed = self.client_a.call("workbench_snapshot_list", {"id": workbench_id})
        checkpoints = listed.get("checkpoints")
        if not isinstance(checkpoints, list) or not any(
            row.get("snapshot_id") == snapshot_id
            and row.get("name") == "acked-before-kill"
            for row in checkpoints
        ):
            raise AcceptanceError(
                "an acknowledged snapshot/registry update disappeared after SIGKILL: "
                f"{listed!r}"
            )
        self._assert_read(
            self.client_a,
            workbench_id,
            "durable.txt",
            "durable-before-kill",
            at_snapshot=snapshot_id,
        )
        self.env.cli("retire-snapshot", f"{self.root_a}/{workbench_id}", str(snapshot_id))
        return {"snapshot_id": snapshot_id, "signal": "SIGKILL"}

    def scenario_restore_to_fork(self) -> dict[str, Any]:
        source = "restore-source"
        destination = "restore-destination"
        self._create_committed(self.client_a, source)
        keys_before_old_body = self.env.object_keys()
        old_put = self._put(self.client_a, source, "result.txt", "checkpoint-value\n")
        self._put(
            self.client_a,
            source,
            "nested/kept.txt",
            "checkpoint-nested\n",
        )
        self._put(
            self.client_a,
            source,
            "renamed-before.txt",
            "checkpoint-renamed\n",
        )
        self._put(
            self.client_a,
            source,
            "deleted-after.txt",
            "checkpoint-deleted\n",
        )
        keys_after_old_body = self.env.object_keys()
        old_body_keys = keys_after_old_body - keys_before_old_body
        if not old_body_keys:
            raise AcceptanceError("source file upload produced no RustFS object")

        older = self.client_a.call(
            "workbench_snapshot",
            {"id": source, "name": "older-checkpoint", "ttl_days": 7},
        )
        target = self.client_a.call(
            "workbench_snapshot",
            {"id": source, "name": "restore-point", "ttl_days": 7},
        )
        target_id = int(target["snapshot_id"])
        self._put(
            self.client_a,
            source,
            "result.txt",
            "source-current\n",
            replace=True,
        )
        self._put(
            self.client_a,
            source,
            "nested/kept.txt",
            "source-current-nested\n",
            replace=True,
        )
        source_outputs = f"{self.root_a}/{source}/outputs"
        self.env.cli("rm", f"{source_outputs}/deleted-after.txt")
        self.env.cli(
            "rename",
            f"{source_outputs}/renamed-before.txt",
            f"{source_outputs}/renamed-after.txt",
        )
        self._put(
            self.client_a,
            source,
            "current-only.txt",
            "source-current-only\n",
        )
        objects_before_restore = self.env.object_keys()
        object_puts_before_restore = self.env.object_puts()
        restore_args = {
            "id": source,
            "at_snapshot": "restore-point",
            "destination_id": destination,
        }
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            restore_future = executor.submit(
                self.client_a.call, RESTORE_TOOL, restore_args
            )
            visibility_deadline = time.monotonic() + self.env.config.tool_timeout
            while time.monotonic() < visibility_deadline:
                if restore_future.done():
                    # Surface a restore failure immediately instead of masking
                    # it as a destination visibility timeout.
                    restore_future.result()
                observed = self.client_a2.raw_call(
                    "workbench_stat", {"id": destination}
                )
                if observed.get("status") == "success":
                    break
                error = decode_tool_error(observed)
                if "not found" not in error.message.lower():
                    raise AcceptanceError(
                        f"destination visibility probe failed unexpectedly: {error}"
                    )
            else:
                raise AcceptanceError(
                    "restored destination did not become visible before deadline"
                )

            # First-visible destination contract: source-owned checkpoint aliases
            # are already absent and the restore manifest is already readable.
            destination_checkpoints = self.client_a2.call(
                "workbench_snapshot_list", {"id": destination}
            )
            if destination_checkpoints.get("checkpoint_count") != 0:
                raise AcceptanceError(
                    "destination became visible with inherited checkpoint aliases: "
                    f"{destination_checkpoints!r}"
                )
            inherited_registry = self.client_a2.raw_call(
                "workbench_stat",
                {
                    "id": destination,
                    "section": "metadata",
                    "path": "checkpoints.jsonl",
                },
            )
            inherited_registry_error = decode_tool_error(inherited_registry)
            if "not found" not in inherited_registry_error.message.lower():
                raise AcceptanceError(
                    "destination checkpoints.jsonl did not report absence at first "
                    f"visibility: {inherited_registry_error}"
                )
            first_manifest = self.client_a2.call(
                "workbench_read",
                {
                    "id": destination,
                    "section": "metadata",
                    "path": "restore_manifest.json",
                },
            )
            manifest = _read_json_object(first_manifest)
            first_source_checkpoints = self.client_a2.call(
                "workbench_snapshot_list", {"id": source}
            )
            if first_source_checkpoints.get("checkpoint_count", 0) < 2:
                raise AcceptanceError(
                    "source registry changed when destination first became visible: "
                    f"{first_source_checkpoints!r}"
                )
            restored = restore_future.result(timeout=self.env.config.tool_timeout)
        operation_id = restored.get("operation_id")
        if not isinstance(operation_id, str) or not operation_id:
            raise AcceptanceError(f"restore lacks operation_id: {restored!r}")
        expected_manifest_path = (
            f"{self.root_a}/{destination}/metadata/restore_manifest.json"
        )
        source_root = restored.get("source_root")
        destination_root = restored.get("destination_root")
        if (
            restored.get("state") != "complete"
            or restored.get("source_workbench_id") != source
            or restored.get("destination_workbench_id") != destination
            or restored.get("snapshot_id") != target_id
            or restored.get("read_version") != target.get("read_version")
            or not isinstance(source_root, int)
            or source_root <= 0
            or not isinstance(destination_root, int)
            or destination_root <= 0
            or destination_root == source_root
            or restored.get("cleanup_pending") is not False
            or restored.get("restore_manifest") != expected_manifest_path
        ):
            raise AcceptanceError(f"restore response contract is malformed: {restored!r}")

        # Recheck the terminal response state as well as the first-visible state.
        destination_checkpoints = self.client_a.call(
            "workbench_snapshot_list", {"id": destination}
        )
        if destination_checkpoints.get("checkpoint_count") != 0:
            raise AcceptanceError(
                "restored destination inherited source checkpoint aliases: "
                f"{destination_checkpoints!r}"
            )
        restored_from = manifest.get("restored_from")
        if (
            manifest.get("operation_id") != operation_id
            or not isinstance(restored_from, dict)
            or restored_from.get("workbench_id") != source
            or restored_from.get("path") != f"{self.root_a}/{source}"
            or restored_from.get("snapshot_id") != target_id
        ):
            raise AcceptanceError(
                f"restore manifest does not bind operation/source/snapshot: {manifest!r}"
            )
        source_checkpoints = self.client_a.call(
            "workbench_snapshot_list", {"id": source}
        )
        if source_checkpoints.get("checkpoint_count", 0) < 2:
            raise AcceptanceError(
                f"restore modified the source checkpoint registry: {source_checkpoints!r}"
            )
        self._assert_read(self.client_a, destination, "result.txt", "checkpoint-value")
        self._assert_read(self.client_a, source, "result.txt", "source-current")
        self._assert_read(
            self.client_a, destination, "nested/kept.txt", "checkpoint-nested"
        )
        self._assert_read(
            self.client_a,
            destination,
            "renamed-before.txt",
            "checkpoint-renamed",
        )
        self._assert_read(
            self.client_a,
            destination,
            "deleted-after.txt",
            "checkpoint-deleted",
        )
        self._assert_output_absent(self.client_a, destination, "renamed-after.txt")
        self._assert_output_absent(self.client_a, destination, "current-only.txt")
        self._assert_read(
            self.client_a,
            source,
            "nested/kept.txt",
            "source-current-nested",
        )
        self._assert_read(
            self.client_a,
            source,
            "renamed-after.txt",
            "checkpoint-renamed",
        )
        self._assert_read(
            self.client_a, source, "current-only.txt", "source-current-only"
        )
        self._assert_output_absent(self.client_a, source, "renamed-before.txt")
        self._assert_output_absent(self.client_a, source, "deleted-after.txt")
        destination_outputs = self._list_all(self.client_a, destination)
        source_current_outputs = self._list_all(self.client_a, source)
        if destination_outputs.names != [
            "deleted-after.txt",
            "nested",
            "renamed-before.txt",
            "result.txt",
        ]:
            raise AcceptanceError(
                f"restored output tree differs from checkpoint: {destination_outputs.names!r}"
            )
        if source_current_outputs.names != [
            "current-only.txt",
            "nested",
            "renamed-after.txt",
            "result.txt",
        ]:
            raise AcceptanceError(
                f"restore changed source current tree: {source_current_outputs.names!r}"
            )

        objects_after_restore = self.env.object_keys()
        object_puts_after_restore = self.env.object_puts()
        uploaded_by_restore = objects_after_restore - objects_before_restore
        if len(uploaded_by_restore) > 1:
            raise AcceptanceError(
                "COW restore uploaded more than the one allowed restore_manifest body: "
                f"{sorted(uploaded_by_restore)!r}"
            )
        restore_object_puts = object_puts_after_restore - object_puts_before_restore
        if restore_object_puts < 0 or restore_object_puts > 1:
            raise AcceptanceError(
                "COW restore issued body PUTs beyond restore_manifest: "
                f"delta={restore_object_puts}"
            )
        object_puts_before_retry = self.env.object_puts()
        retried = self.client_a2.call(
            RESTORE_TOOL,
            {"id": source, "at_snapshot": target_id, "destination_id": destination},
        )
        if retried.get("operation_id") != operation_id:
            raise AcceptanceError(
                f"restore retry changed operation id: {operation_id} -> "
                f"{retried.get('operation_id')}"
            )
        if self.env.object_keys() != objects_after_restore:
            raise AcceptanceError("idempotent restore retry uploaded new object bodies")
        if self.env.object_puts() != object_puts_before_retry:
            raise AcceptanceError("idempotent restore retry repeated an object PUT")

        conflict_snapshot = self.client_a.call(
            "workbench_snapshot",
            {"id": source, "name": "different-point", "ttl_days": 7},
        )
        self.client_a.expect_error(
            RESTORE_TOOL,
            {
                "id": source,
                "at_snapshot": int(conflict_snapshot["snapshot_id"]),
                "destination_id": destination,
            },
            "RestoreDestinationConflict",
        )

        self.restore_state = {
            "source": source,
            "destination": destination,
            "older_snapshot_id": int(older["snapshot_id"]),
            "target_snapshot_id": target_id,
            "conflict_snapshot_id": int(conflict_snapshot["snapshot_id"]),
            "operation_id": operation_id,
            "old_body_keys": old_body_keys,
            "old_digest_uri": old_put.get("digest_uri"),
        }
        return {
            "operation_id": operation_id,
            "snapshot_id": target_id,
            "new_objects": len(uploaded_by_restore),
            "object_puts": restore_object_puts,
        }

    def scenario_restore_retry_after_restart(self) -> dict[str, Any]:
        state = self.restore_state
        self.restart_stack()
        retried = self.client_a.call(
            RESTORE_TOOL,
            {
                "id": state["source"],
                "at_snapshot": state["target_snapshot_id"],
                "destination_id": state["destination"],
            },
        )
        if (
            retried.get("operation_id") != state["operation_id"]
            or retried.get("state") != "complete"
            or retried.get("snapshot_id") != state["target_snapshot_id"]
            or not isinstance(retried.get("source_root"), int)
            or not isinstance(retried.get("destination_root"), int)
            or retried.get("cleanup_pending") is not False
        ):
            raise AcceptanceError(f"restart retry response is inconsistent: {retried!r}")
        self._assert_read(
            self.client_a,
            state["destination"],
            "result.txt",
            "checkpoint-value",
        )
        self._assert_read(
            self.client_a, state["source"], "result.txt", "source-current"
        )
        self._assert_read(
            self.client_a,
            state["destination"],
            "nested/kept.txt",
            "checkpoint-nested",
        )
        self._assert_read(
            self.client_a,
            state["source"],
            "current-only.txt",
            "source-current-only",
        )
        self.client_b.expect_error(
            "workbench_read",
            {
                "id": self.foreign_workbench_id,
                "section": "outputs",
                "path": "owner.txt",
                "at_snapshot": self.foreign_snapshot_id,
            },
            "SnapshotRootMismatch",
        )
        self.env.cli(
            "retire-snapshot",
            f"{self.root_a}/{self.foreign_workbench_id}",
            str(self.foreign_snapshot_id),
        )
        return {"operation_id": retried["operation_id"]}

    def _delete_restored_workbench(self, destination: str) -> None:
        root = f"{self.root_a}/{destination}"
        for path in (
            f"{root}/outputs/result.txt",
            f"{root}/outputs/nested/kept.txt",
            f"{root}/outputs/renamed-before.txt",
            f"{root}/outputs/deleted-after.txt",
            f"{root}/metadata/run_manifest.json",
            f"{root}/metadata/restore_manifest.json",
        ):
            self.env.cli("rm", path)
        self.env.cli("rmdir", f"{root}/outputs/nested")
        for section in ("input", "scripts", "outputs", "logs", "metadata"):
            self.env.cli("rmdir", f"{root}/{section}")
        self.env.cli("rmdir", root)

    def _delete_restore_source(self, source: str) -> None:
        root = f"{self.root_a}/{source}"
        for path in (
            f"{root}/outputs/result.txt",
            f"{root}/outputs/nested/kept.txt",
            f"{root}/outputs/renamed-after.txt",
            f"{root}/outputs/current-only.txt",
            f"{root}/metadata/run_manifest.json",
            f"{root}/metadata/checkpoints.jsonl",
        ):
            self.env.cli("rm", path)
        self.env.cli("rmdir", f"{root}/outputs/nested")
        for section in ("input", "scripts", "outputs", "logs", "metadata"):
            self.env.cli("rmdir", f"{root}/{section}")
        self.env.cli("rmdir", root)

    def scenario_fork_retention_and_release(self) -> dict[str, Any]:
        state = self.restore_state
        source_root = f"{self.root_a}/{state['source']}"
        for snapshot_id in (
            state["older_snapshot_id"],
            state["target_snapshot_id"],
            state["conflict_snapshot_id"],
        ):
            self.env.cli("retire-snapshot", source_root, str(snapshot_id))
        self._delete_restore_source(state["source"])
        old_keys = set(state["old_body_keys"])
        retention_deadline = time.monotonic() + self.env.config.gc_deadline
        last_retention_gc: dict[str, Any] = {}
        while time.monotonic() < retention_deadline:
            last_retention_gc = self.env.manual_gc()
            try:
                blocked = int(last_retention_gc["object_gc"]["blocked_by_snapshots"])
            except (KeyError, TypeError, ValueError) as exc:
                raise AcceptanceError(
                    f"manual GC lacks blocked_by_snapshots: {last_retention_gc!r}"
                ) from exc
            if blocked > 0:
                break
            if not old_keys.issubset(self.env.object_keys()):
                raise AcceptanceError(
                    "source cleanup reclaimed a fork-owned body before reporting retention"
                )
            time.sleep(0.05)
        else:
            raise AcceptanceError(
                "GC never exercised the durable fork-base retention hold: "
                f"last_gc={last_retention_gc!r}"
            )
        if not old_keys.issubset(self.env.object_keys()):
            raise AcceptanceError(
                "source checkpoint retirement reclaimed a body still referenced by the fork"
            )
        self._assert_read(
            self.client_a,
            state["destination"],
            "result.txt",
            "checkpoint-value",
        )
        self._assert_read(
            self.client_a,
            state["destination"],
            "nested/kept.txt",
            "checkpoint-nested",
        )
        self._delete_restored_workbench(state["destination"])

        deadline = time.monotonic() + self.env.config.gc_deadline
        last_gc: dict[str, Any] = {}
        while time.monotonic() < deadline:
            last_gc = self.env.manual_gc()
            if old_keys.isdisjoint(self.env.object_keys()):
                break
            time.sleep(0.1)
        else:
            raise AcceptanceError(
                f"fork deletion did not release shared objects before deadline; "
                f"keys={sorted(old_keys)!r}, last_gc={last_gc!r}"
            )
        source_stat = self.client_a.raw_call("workbench_stat", {"id": state["source"]})
        source_error = decode_tool_error(source_stat)
        if "not found" not in source_error.message.lower():
            raise AcceptanceError(
                f"deleted source unexpectedly remained visible: {source_error}"
            )
        return {
            "source_deleted": True,
            "released_object_keys": sorted(old_keys),
            "retention_gc": last_retention_gc,
        }

    def run(self) -> dict[str, dict[str, Any]]:
        self.connect_clients()
        self.scenario(
            "lingtai_placeholder_and_tool_contract", self.scenario_placeholder_and_tools
        )
        self.scenario("linearizable_snapshot_renew", self.scenario_linearizable_renew)
        self.scenario("snapshot_reaper_accounting", self.scenario_reaper_accounting)
        short_lease_supported, reason = self.probe_short_lease_mint()
        if short_lease_supported:
            self.scenario(
                "renew_reaper_short_lease_race",
                self.scenario_renew_reaper_short_lease,
            )
        else:
            self.skip("renew_reaper_short_lease_race", reason)
        self.scenario("service_root_binding", self.scenario_root_binding)
        self.scenario(
            "snapshot_historical_scan_and_pagination", self.scenario_historical_scan
        )
        self.scenario(
            "acked_metadata_survives_sigkill",
            self.scenario_ack_durability_after_sigkill,
        )
        self.scenario("root_binding_after_restart", self.scenario_restart_root_binding)

        if self.restore_available:
            self.scenario("restore_to_fork", self.scenario_restore_to_fork)
            self.scenario(
                "restore_idempotency_after_restart",
                self.scenario_restore_retry_after_restart,
            )
            self.scenario(
                "fork_base_retention_and_release",
                self.scenario_fork_retention_and_release,
            )
        else:
            reason = "workbench_restore is not present on the A+B-only binary"
            for name in (
                "restore_to_fork",
                "restore_idempotency_after_restart",
                "fork_base_retention_and_release",
            ):
                self.skip(name, reason)
        return self.results


def _load_lingtai(lingtai_kernel_dir: Path) -> tuple[type, type]:
    source = lingtai_kernel_dir / "src"
    if not source.is_dir():
        raise AcceptanceError(f"LingTai source directory not found: {source}")
    sys.path.insert(0, str(source))
    try:
        from lingtai.agent import Agent
        from lingtai.services.mcp import MCPClient
    except Exception as exc:
        raise AcceptanceError(
            "failed to import the real LingTai Agent/MCPClient; run with the "
            "LingTai environment, for example `uv run --project ~/lingtai-kernel ...`"
        ) from exc
    agent_source = Path(sys.modules[Agent.__module__].__file__).resolve()
    try:
        agent_source.relative_to(source.resolve())
    except ValueError as exc:
        raise AcceptanceError(
            f"imported LingTai from {agent_source}, expected checkout {source.resolve()}"
        ) from exc
    if not hasattr(Agent, "_expand_agent_placeholders"):
        raise AcceptanceError(
            "LingTai checkout lacks Agent._expand_agent_placeholders; use PR #813 or newer"
        )
    return Agent, MCPClient


def _require_commands(names: tuple[str, ...]) -> None:
    missing = [name for name in names if shutil.which(name) is None]
    if missing:
        raise AcceptanceError(f"required commands are missing: {', '.join(missing)}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    script = Path(__file__).resolve()
    repo_root = script.parents[2]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=("quick", "full"), default="quick")
    parser.add_argument("--repo-root", type=Path, default=repo_root)
    parser.add_argument(
        "--cargo-bin",
        type=Path,
        default=Path(
            os.environ.get(
                "CARGO",
                shutil.which("cargo") or str(Path("~/.cargo/bin/cargo").expanduser()),
            )
        ),
    )
    parser.add_argument("--nokv-bin", type=Path)
    parser.add_argument(
        "--lingtai-kernel-dir",
        type=Path,
        default=Path(
            os.environ.get("LINGTAI_KERNEL_DIR", "~/lingtai-kernel")
        ).expanduser(),
    )
    parser.add_argument("--state-dir", type=Path)
    parser.add_argument("--server-port", type=int, default=0)
    parser.add_argument("--rustfs-port", type=int, default=0)
    parser.add_argument("--rustfs-console-port", type=int, default=0)
    parser.add_argument("--rustfs-image", default="rustfs/rustfs:latest")
    parser.add_argument("--command-timeout", type=float, default=120)
    parser.add_argument("--tool-timeout", type=float, default=120)
    parser.add_argument("--startup-timeout", type=float, default=180)
    parser.add_argument("--gc-deadline", type=float, default=30)
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--keep-state", action="store_true")
    parser.add_argument(
        "--allow-missing-restore",
        action="store_true",
        help="Run A+B scenarios when workbench_restore has not been integrated yet.",
    )
    parser.add_argument(
        "--require-all",
        action="store_true",
        help="Fail when any acceptance scenario is skipped.",
    )
    args = parser.parse_args(argv)
    if args.require_all and args.profile != "full":
        parser.error("--require-all requires --profile full")
    return args


def _live_config(args: argparse.Namespace) -> LiveConfig:
    repo_root = args.repo_root.expanduser().resolve()
    nokv_bin = (
        args.nokv_bin.expanduser().resolve()
        if args.nokv_bin
        else repo_root / "target/debug/nokv"
    )
    if args.state_dir:
        state_dir = args.state_dir.expanduser().resolve()
        if state_dir.exists() and any(state_dir.iterdir()):
            raise AcceptanceError(f"state directory must be empty: {state_dir}")
        state_dir.mkdir(parents=True, exist_ok=True)
    else:
        target = repo_root / "target"
        target.mkdir(parents=True, exist_ok=True)
        state_dir = Path(tempfile.mkdtemp(prefix="checkpoint-live-e2e-", dir=target))
    suffix = f"{os.getpid()}-{int(time.time())}"
    server_port = args.server_port or _free_port()
    rustfs_port = args.rustfs_port or _free_port()
    rustfs_console_port = args.rustfs_console_port or _free_port()
    if len({server_port, rustfs_port, rustfs_console_port}) != 3:
        raise AcceptanceError("server and RustFS ports must be distinct")
    return LiveConfig(
        repo_root=repo_root,
        # Preserve rustup's `cargo` symlink. Resolving it to the rustup binary
        # changes argv[0] and makes rustup parse `build` as its own command.
        cargo_bin=args.cargo_bin.expanduser().absolute(),
        nokv_bin=nokv_bin,
        lingtai_kernel_dir=args.lingtai_kernel_dir.expanduser().resolve(),
        state_dir=state_dir,
        server_port=server_port,
        rustfs_port=rustfs_port,
        rustfs_console_port=rustfs_console_port,
        rustfs_image=args.rustfs_image,
        bucket=f"nokv-checkpoint-e2e-{suffix}",
        container=f"nokv-checkpoint-e2e-{suffix}",
        profile=workload_profile(args.profile),
        command_timeout=args.command_timeout,
        tool_timeout=args.tool_timeout,
        startup_timeout=args.startup_timeout,
        gc_deadline=args.gc_deadline,
        build=not args.no_build,
        keep_state=args.keep_state,
        allow_missing_restore=args.allow_missing_restore,
        require_all=args.require_all,
    )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    config = _live_config(args)
    environment = LiveEnvironment(config)
    suite: AcceptanceSuite | None = None
    exit_code = 0
    try:
        _require_commands(("docker", "aws"))
        if config.build and not config.cargo_bin.is_file():
            raise AcceptanceError(f"cargo executable not found: {config.cargo_bin}")
        agent_class, mcp_client_class = _load_lingtai(config.lingtai_kernel_dir)
        environment.build()
        environment.start_rustfs()
        environment.start_server()
        suite = AcceptanceSuite(environment, agent_class, mcp_client_class)
        suite.run()
        if config.require_all and any(
            result["status"] == "skipped" for result in suite.results.values()
        ):
            raise AcceptanceError(
                "--require-all rejected one or more skipped scenarios"
            )
    except Exception as exc:
        exit_code = 1
        print(f"[live-e2e] acceptance failed: {exc}", file=sys.stderr, flush=True)
    finally:
        if suite is not None:
            suite.close_clients()
        summary = {
            "status": "passed" if exit_code == 0 else "failed",
            "profile": args.profile,
            "server_bind": environment.server_bind,
            "s3_endpoint": environment.s3_endpoint,
            "bucket": config.bucket,
            "state_dir": str(config.state_dir),
            "results": suite.results if suite is not None else {},
        }
        print(json.dumps(summary, indent=2, sort_keys=True), flush=True)
        environment.cleanup()
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
