#!/usr/bin/env python3
"""Black-box LingTai/Viking/Ptycho evidence over the flat ``nokv`` binary."""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import hashlib
import json
import os
import platform
import re
import selectors
import shutil
import signal
import socket
import subprocess
import sys
import time
import urllib.parse
from pathlib import Path
from typing import Any, Iterable, TextIO

from workbench_contract import WORKBENCH_TOOLS, contract_evidence, validate_tool_contract


SCHEMA = "nokv.lingtai.first_client_evidence.v1"
PROTOCOL_VERSION = "2025-11-25"
HEX_ID = re.compile(r"^[0-9a-f]{32}$")
WORKBENCH_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")
SECRET_FLAGS = {"--object-secret-access-key"}
INTERNAL_KEYS = {
    "artifact_revision_id",
    "dentry",
    "destination_workspace_incarnation_id",
    "holt_key",
    "incarnation_id",
    "inode",
    "logical_shard_id",
    "object_key",
    "owner_address",
    "owner_epoch",
    "placement_generation",
    "root_id",
    "source_root",
    "destination_root",
    "workspace_incarnation_id",
}


class NotQualified(RuntimeError):
    pass


class WorkflowFailure(RuntimeError):
    pass


@dataclasses.dataclass(frozen=True)
class Config:
    repo: Path
    binary: Path
    evidence: Path
    metadata: Path
    metadata_mode: str
    root_id: str
    shard_id: str
    agent: str
    workbench: str
    restored: str
    snapshot: str
    etcd: tuple[str, ...]
    etcd_prefix: str
    bind: str
    advertise: str
    node: str
    bucket: str
    object_endpoint: str
    object_root: str
    object_region: str
    access_key: str | None
    secret_key: str | None
    build: bool
    dry_run: bool
    timeout: float

    @property
    def workbench_root(self) -> str:
        return f"/agents/{self.agent}/wb"


@dataclasses.dataclass(frozen=True)
class ToolStep:
    label: str
    name: str
    arguments: dict[str, Any]
    error_code: str | None = None


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def digest_file(path: Path) -> str:
    state = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            state.update(block)
    return state.hexdigest()


def now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def scan_bytes(state: str, revision: int) -> bytes:
    value = {
        "experiment": "ptycho",
        "frames": 2,
        "partner": "viking",
        "revision": revision,
        "state": state,
    }
    return (canonical_json(value) + "\n").encode()


def reconstruction_bytes() -> bytes:
    value = {
        "algorithm": "viking-ptycho",
        "frames": 2,
        "source_state": "calibrated",
        "status": "converged",
    }
    return (canonical_json(value) + "\n").encode()


def tool_plan(config: Config) -> list[ToolStep]:
    wb, restored, snapshot = config.workbench, config.restored, config.snapshot
    raw = scan_bytes("raw", 0).decode()
    replacement = scan_bytes("raw", 1).decode()
    commit_digest = "sha256:" + digest(reconstruction_bytes())
    return [
        ToolStep("create", "workbench_create", {"id": wb}),
        ToolStep(
            "put-input",
            "workbench_put_file",
            {"id": wb, "section": "input", "path": "scan.json", "text": raw,
             "content_type": "application/json", "replace": False},
        ),
        ToolStep(
            "create-only-error",
            "workbench_put_file",
            {"id": wb, "section": "input", "path": "scan.json", "text": raw,
             "replace": False},
            "AlreadyExists",
        ),
        ToolStep(
            "replace-input",
            "workbench_put_file",
            {"id": wb, "section": "input", "path": "scan.json", "text": replacement,
             "content_type": "application/json", "replace": True},
        ),
        ToolStep(
            "append-log",
            "workbench_append",
            {"id": wb, "section": "logs", "path": "reconstruction.log",
             "text": "ptycho reconstruction started\n", "content_type": "text/plain"},
        ),
        ToolStep(
            "read-input",
            "workbench_read",
            {"id": wb, "section": "input", "path": "scan.json",
             "format": "structured", "limit": 10},
        ),
        ToolStep(
            "stat-input",
            "workbench_stat",
            {"id": wb, "section": "input", "path": "scan.json"},
        ),
        ToolStep(
            "list-input",
            "workbench_list",
            {"id": wb, "section": "input", "limit": 100},
        ),
        ToolStep(
            "grep-input",
            "workbench_grep",
            {"id": wb, "section": "input", "pattern": "PTYCHO", "glob": "*.json",
             "recursive": True, "limit": 10},
        ),
        ToolStep(
            "edit-input",
            "workbench_edit",
            {"id": wb, "section": "input", "path": "scan.json",
             "old_string": '"state":"raw"', "new_string": '"state":"calibrated"',
             "replace_all": False},
        ),
        ToolStep("search", "workbench_search", {"id": wb, "limit": 10}),
        ToolStep(
            "aggregate",
            "workbench_aggregate",
            {"id": wb, "measures": [{"name": "artifacts", "op": "count"}],
             "limit": 100},
        ),
        ToolStep(
            "catalog",
            "workbench_catalog",
            {"id": wb, "include_facets": True},
        ),
        ToolStep(
            "commit",
            "workbench_commit",
            {"id": wb,
             "manifest": {"dataset": "viking-first-client",
                          "model": "scientific-reconstruction", "task": "ptycho"},
             "content_digest_uri": commit_digest, "replace": False},
        ),
        ToolStep(
            "commit-replay",
            "workbench_commit",
            {"id": wb,
             "manifest": {"dataset": "viking-first-client",
                          "model": "scientific-reconstruction", "task": "ptycho"},
             "content_digest_uri": commit_digest, "replace": False},
        ),
        ToolStep(
            "read-run-manifest",
            "workbench_read",
            {"id": wb, "section": "metadata", "path": "run_manifest.json",
             "format": "structured", "limit": 10},
        ),
        ToolStep(
            "snapshot",
            "workbench_snapshot",
            {"id": wb, "name": snapshot, "ttl_days": 1,
             "reason": "first-client frozen reconstruction",
             "metadata": {"partner": "viking", "workflow": "ptycho"}},
        ),
        ToolStep(
            "post-snapshot-edit",
            "workbench_edit",
            {"id": wb, "section": "input", "path": "scan.json",
             "old_string": '"state":"calibrated"',
             "new_string": '"state":"post-snapshot"', "replace_all": False},
        ),
        ToolStep(
            "frozen-read",
            "workbench_read",
            {"id": wb, "section": "input", "path": "scan.json", "format": "structured",
             "at_snapshot": snapshot, "limit": 10},
        ),
        ToolStep(
            "live-read-after-snapshot",
            "workbench_read",
            {"id": wb, "section": "input", "path": "scan.json",
             "format": "structured", "limit": 10},
        ),
        ToolStep(
            "snapshot-list-alive", "workbench_snapshot_list", {"id": wb}
        ),
        ToolStep(
            "snapshot-renew",
            "workbench_snapshot_renew",
            {"id": wb, "name": snapshot, "ttl_days": 2},
        ),
        ToolStep(
            "restore",
            "workbench_restore",
            {"id": wb, "at_snapshot": snapshot, "destination_id": restored},
        ),
        ToolStep(
            "read-restored-input",
            "workbench_read",
            {"id": restored, "section": "input", "path": "scan.json",
             "format": "structured", "limit": 10},
        ),
        ToolStep(
            "read-restore-manifest",
            "workbench_read",
            {"id": restored, "section": "metadata", "path": "restore_manifest.json",
             "format": "structured", "limit": 10},
        ),
        ToolStep(
            "find",
            "workbench_find",
            {"committed": True, "manifest_pattern": "ptycho",
             "include_manifest": True, "limit": 100},
        ),
        ToolStep(
            "snapshot-retire",
            "workbench_snapshot_retire",
            {"id": wb, "name": snapshot, "reason": "first-client workflow complete"},
        ),
        ToolStep(
            "snapshot-retire-replay",
            "workbench_snapshot_retire",
            {"id": wb, "name": snapshot, "reason": "must not replace durable evidence"},
        ),
        ToolStep(
            "snapshot-list-retired", "workbench_snapshot_list", {"id": wb}
        ),
    ]


def planned_tool_coverage(steps: Iterable[ToolStep]) -> set[str]:
    return {step.name for step in steps}


def control_args(config: Config) -> list[str]:
    result = ["--root-id", config.root_id]
    for endpoint in config.etcd:
        result += ["--etcd-endpoint", endpoint]
    return result + ["--etcd-key-prefix", config.etcd_prefix]


def object_args(config: Config) -> list[str]:
    result = [
        "--object-bucket", config.bucket,
        "--object-endpoint", config.object_endpoint,
        "--object-root", config.object_root,
        "--object-region", config.object_region,
    ]
    if config.access_key is not None:
        result += ["--object-access-key-id", config.access_key]
    if config.secret_key is not None:
        result += ["--object-secret-access-key", config.secret_key]
    return result


def client_args(config: Config) -> list[str]:
    return [str(config.binary), *control_args(config), "--workbench-root",
            config.workbench_root, *object_args(config)]


def provision_command(config: Config) -> list[str]:
    return [str(config.binary), *control_args(config), "provision", config.shard_id]


def server_command(config: Config) -> list[str]:
    return [
        str(config.binary), *control_args(config), *object_args(config),
        "--bind", config.bind, "--advertise-endpoint", config.advertise,
        "--node-id", config.node, f"--metadata-{config.metadata_mode}",
        str(config.metadata), "--lifecycle-interval-millis", "100", "serve",
    ]


def mcp_command(config: Config) -> list[str]:
    return [*client_args(config), "mcp"]


def materialize_command(config: Config, destination: Path) -> list[str]:
    return [*client_args(config), "materialize", config.workbench, "input", "scan.json",
            str(destination)]


def collect_command(config: Config, source: Path) -> list[str]:
    return [*client_args(config), "collect", config.workbench, "outputs", str(source),
            "reconstruction.json", "--content-type", "application/json"]


def redact_argv(argv: Iterable[str]) -> list[str]:
    output, redact_next = [], False
    for argument in argv:
        if redact_next:
            output.append(f"<redacted:sha256:{digest(argument.encode())[:12]}>")
            redact_next = False
        else:
            output.append(argument)
            redact_next = argument in SECRET_FLAGS
    return output


class Evidence:
    def __init__(self, root: Path) -> None:
        self.root = root

    def prepare(self) -> None:
        if self.root.exists() and any(self.root.iterdir()):
            raise NotQualified(f"evidence directory is not empty: {self.root}")
        self.root.mkdir(parents=True, exist_ok=True)

    def json(self, name: str, value: Any) -> None:
        (self.root / name).write_text(
            json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )

    def line(self, name: str, value: Any) -> None:
        with (self.root / name).open("a", encoding="utf-8") as output:
            output.write(canonical_json(value) + "\n")


def completed_process(
    evidence: Evidence, label: str, command: list[str], config: Config,
) -> subprocess.CompletedProcess[str]:
    started = now()
    try:
        result = subprocess.run(
            command, cwd=config.repo, text=True, capture_output=True,
            timeout=config.timeout, check=False,
        )
    except subprocess.TimeoutExpired as error:
        evidence.line("processes.jsonl", {
            "schema": SCHEMA, "label": label, "argv": redact_argv(command),
            "started_at": started, "finished_at": now(), "timed_out": True,
            "stdout": error.stdout or "", "stderr": error.stderr or "",
        })
        raise WorkflowFailure(f"{label} timed out") from error
    evidence.line("processes.jsonl", {
        "schema": SCHEMA, "label": label, "argv": redact_argv(command),
        "started_at": started, "finished_at": now(), "returncode": result.returncode,
        "stdout": result.stdout, "stderr": result.stderr,
    })
    if result.returncode:
        detail = result.stderr.strip() or result.stdout.strip() or "no output"
        raise WorkflowFailure(f"{label} failed ({result.returncode}): {detail}")
    return result


class Mcp:
    def __init__(self, process: subprocess.Popen[str], evidence: Evidence, attempt: int,
                 timeout: float) -> None:
        self.process, self.evidence = process, evidence
        self.attempt, self.timeout, self.next_id = attempt, timeout, 1

    def request(self, method: str, params: dict[str, Any] | None = None,
                normalized: dict[str, Any] | None = None) -> dict[str, Any]:
        request_id, self.next_id = self.next_id, self.next_id + 1
        request: dict[str, Any] = {"jsonrpc": "2.0", "id": request_id, "method": method}
        if params is not None:
            request["params"] = params
        sent = canonical_json(request)
        if self.process.stdin is None or self.process.stdout is None:
            raise WorkflowFailure("MCP pipes are unavailable")
        self.process.stdin.write(sent + "\n")
        self.process.stdin.flush()
        selector = selectors.DefaultSelector()
        selector.register(self.process.stdout, selectors.EVENT_READ)
        try:
            if not selector.select(self.timeout):
                raise WorkflowFailure("MCP response timed out")
            received = self.process.stdout.readline().rstrip("\n")
        finally:
            selector.close()
        if not received:
            raise WorkflowFailure(f"MCP exited before responding ({self.process.poll()})")
        try:
            response = json.loads(received)
        except json.JSONDecodeError as error:
            raise WorkflowFailure(f"MCP returned invalid JSON: {received!r}") from error
        self.evidence.line("mcp-transcript.jsonl", {
            "schema": SCHEMA, "attempt": self.attempt, "sequence": request_id,
            "request_raw": sent, "request": request, "normalized_input": normalized,
            "response_raw": received, "response": response,
        })
        if response.get("jsonrpc") != "2.0" or response.get("id") != request_id:
            raise WorkflowFailure("MCP response envelope mismatch")
        return response

    def notify(self, method: str) -> None:
        request = {"jsonrpc": "2.0", "method": method}
        sent = canonical_json(request)
        assert self.process.stdin is not None
        self.process.stdin.write(sent + "\n")
        self.process.stdin.flush()
        self.evidence.line("mcp-transcript.jsonl", {
            "schema": SCHEMA, "attempt": self.attempt, "sequence": None,
            "request_raw": sent, "request": request, "normalized_input": None,
            "response_raw": None, "response": None,
        })

    def call(self, step: ToolStep) -> dict[str, Any]:
        response = self.request(
            "tools/call", {"name": step.name, "arguments": step.arguments},
            {"name": step.name, "arguments": json.loads(canonical_json(step.arguments))},
        )
        result = response.get("result")
        structured = result.get("structuredContent") if isinstance(result, dict) else None
        if not isinstance(structured, dict):
            raise WorkflowFailure(f"{step.label} lacks structuredContent")
        content = result.get("content")
        try:
            text_result = json.loads(content[0]["text"])
        except (IndexError, KeyError, TypeError, json.JSONDecodeError) as error:
            raise WorkflowFailure(f"{step.label} lacks JSON text content") from error
        if text_result != structured:
            raise WorkflowFailure(f"{step.label} text and structured results differ")
        is_error = result.get("isError") is True
        if step.error_code is not None:
            if not is_error or structured.get("code") != step.error_code:
                raise WorkflowFailure(f"{step.label} did not return {step.error_code}")
        elif is_error or structured.get("status") != "success":
            raise WorkflowFailure(f"{step.label} failed: {structured}")
        else:
            reject_internal_keys(structured, step.label)
        return structured


def reject_internal_keys(value: Any, label: str) -> None:
    if isinstance(value, dict):
        leaked = INTERNAL_KEYS.intersection(value)
        if leaked:
            raise WorkflowFailure(f"{label} leaked internal keys: {sorted(leaked)}")
        for child in value.values():
            reject_internal_keys(child, label)
    elif isinstance(value, list):
        for child in value:
            reject_internal_keys(child, label)


def document(result: dict[str, Any], label: str) -> dict[str, Any]:
    items = result.get("items")
    if result.get("record_type") != "json_object" or not isinstance(items, list) \
            or len(items) != 1 or not isinstance(items[0].get("value"), dict):
        raise WorkflowFailure(f"{label} did not return one JSON object")
    return items[0]["value"]


def assert_results(results: dict[str, dict[str, Any]], config: Config) -> None:
    put, replacement = results["put-input"], results["replace-input"]
    if put.get("digest_uri") != "sha256:" + digest(scan_bytes("raw", 0)):
        raise WorkflowFailure("put digest differs from exact input bytes")
    if replacement.get("digest_uri") != "sha256:" + digest(scan_bytes("raw", 1)):
        raise WorkflowFailure("replace digest differs from exact replacement bytes")
    if replacement.get("replace") is not True or replacement.get("generation") == put.get("generation"):
        raise WorkflowFailure("replace-only publication did not advance generation")
    read_generation = results["read-input"].get("generation")
    if read_generation != replacement.get("generation") \
            or results["stat-input"].get("card", {}).get("generation") != read_generation:
        raise WorkflowFailure("put/read/stat generations differ")
    delta = b"ptycho reconstruction started\n"
    append = results["append-log"]
    if append.get("digest") != "sha256:" + digest(delta) \
            or append.get("appended_bytes") != len(delta):
        raise WorkflowFailure("append digest or byte count differs from its delta")
    if results["edit-input"].get("generation") == read_generation:
        raise WorkflowFailure("edit did not advance generation")

    commit, replay = results["commit"], results["commit-replay"]
    expected_content = "sha256:" + digest(reconstruction_bytes())
    if commit.get("content_digest_uri") != expected_content:
        raise WorkflowFailure("commit digest differs from collected output")
    if commit.get("idempotent_replay") is not False \
            or replay.get("idempotent_replay") is not True \
            or commit.get("commit_identity") != replay.get("commit_identity"):
        raise WorkflowFailure("commit exact replay did not converge")

    run_manifest = document(results["read-run-manifest"], "run manifest")
    if run_manifest.get("schema") != "nokv.workbench.run_manifest.v1" \
            or run_manifest.get("workbench_path") != \
            f"{config.workbench_root}/{config.workbench}":
        raise WorkflowFailure("run manifest projection differs from v1")
    frozen = document(results["frozen-read"], "frozen read")
    live = document(results["live-read-after-snapshot"], "live read")
    restored = document(results["read-restored-input"], "restored read")
    if frozen.get("state") != "calibrated" or live.get("state") != "post-snapshot" \
            or restored != frozen:
        raise WorkflowFailure("snapshot frozen/live/restore relationship differs")
    restore_manifest = document(results["read-restore-manifest"], "restore manifest")
    if restore_manifest.get("schema") != "nokv.workbench.restore_manifest.v1" \
            or restore_manifest.get("source_workbench_id") != config.workbench \
            or restore_manifest.get("destination_workbench_id") != config.restored:
        raise WorkflowFailure("restore manifest projection differs from v1")
    reject_internal_keys(run_manifest, "run manifest")
    reject_internal_keys(restore_manifest, "restore manifest")
    minted = results["snapshot"]
    alive_rows = results["snapshot-list-alive"].get("snapshots")
    retired_rows = results["snapshot-list-retired"].get("snapshots")
    if not isinstance(alive_rows, list) or not any(
        row.get("snapshot_id") == minted.get("snapshot_id")
        and row.get("state") == "alive"
        for row in alive_rows
    ):
        raise WorkflowFailure("snapshot list did not retain the live minted snapshot")
    if results["snapshot-renew"].get("renewed") is not True:
        raise WorkflowFailure("snapshot renewal did not report an extension")
    retired = results["snapshot-retire"]
    replayed_retire = results["snapshot-retire-replay"]
    expected_retire_annotation = {
        "metadata": None,
        "reason": "first-client workflow complete",
    }
    if retired.get("retired") is not True or retired.get("state") != "retired" \
            or retired.get("retire_annotation") != expected_retire_annotation:
        raise WorkflowFailure("snapshot did not retire")
    if replayed_retire.get("retired") is not False \
            or replayed_retire.get("retire_annotation") != expected_retire_annotation:
        raise WorkflowFailure("snapshot retire replay replaced durable evidence")
    if not isinstance(retired_rows, list) or not any(
        row.get("snapshot_id") == minted.get("snapshot_id")
        and row.get("state") == "retired"
        for row in retired_rows
    ):
        raise WorkflowFailure("snapshot list did not retain the retired state")


def endpoint(endpoint_value: str) -> tuple[str, int]:
    parsed = urllib.parse.urlparse(
        endpoint_value if "://" in endpoint_value else f"tcp://{endpoint_value}"
    )
    if parsed.hostname is None:
        raise NotQualified(f"endpoint has no host: {endpoint_value}")
    try:
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
    except ValueError as error:
        raise NotQualified(f"endpoint has invalid port: {endpoint_value}") from error
    return parsed.hostname, port


def validate(config: Config, live: bool) -> None:
    if not HEX_ID.fullmatch(config.root_id) or not HEX_ID.fullmatch(config.shard_id):
        raise NotQualified("root and logical shard ids must be 32 lowercase hex characters")
    for value in (config.agent, config.workbench, config.restored):
        if not WORKBENCH_ID.fullmatch(value):
            raise NotQualified(f"invalid Workbench identifier: {value}")
    if config.workbench == config.restored:
        raise NotQualified("source and destination Workbench ids must differ")
    if not config.etcd or not config.bucket or not config.object_endpoint:
        raise NotQualified("etcd endpoint, object endpoint, and bucket are required")
    if (config.access_key is None) != (config.secret_key is None):
        raise NotQualified("object access and secret keys must be supplied together")
    if not live:
        return
    if not config.binary.is_file() or not os.access(config.binary, os.X_OK):
        raise NotQualified(f"nokv binary is missing or not executable: {config.binary}")
    if config.metadata_mode == "create" and config.metadata.exists():
        raise NotQualified(f"metadata create path already exists: {config.metadata}")
    if config.metadata_mode == "reopen" and not config.metadata.exists():
        raise NotQualified(f"metadata reopen path does not exist: {config.metadata}")
    for dependency in (*config.etcd, config.object_endpoint):
        host, port = endpoint(dependency)
        try:
            with socket.create_connection((host, port), timeout=1.5):
                pass
        except OSError as error:
            raise NotQualified(f"dependency unreachable at {dependency}: {error}") from error


def stop(process: subprocess.Popen[str]) -> int | None:
    if process.poll() is None:
        try:
            os.killpg(process.pid, signal.SIGTERM)
            process.wait(timeout=5)
        except (ProcessLookupError, subprocess.TimeoutExpired):
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=5)
    return process.returncode


def start_mcp(config: Config, evidence: Evidence, server: subprocess.Popen[str]) \
        -> tuple[Mcp, TextIO]:
    deadline, last_error, attempt = time.monotonic() + config.timeout, "", 0
    while time.monotonic() < deadline:
        attempt += 1
        if server.poll() is not None:
            raise WorkflowFailure(f"serve exited during startup ({server.returncode})")
        stderr = (evidence.root / f"mcp.stderr.attempt-{attempt}.log").open("w")
        process = subprocess.Popen(
            mcp_command(config), cwd=config.repo, stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=stderr, text=True, bufsize=1,
            start_new_session=True,
        )
        session = Mcp(process, evidence, attempt, min(config.timeout, 5))
        try:
            initialized = session.request("initialize", {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {"name": "nokv-first-client-harness", "version": "1"},
            })
            result = initialized.get("result", {})
            if result.get("serverInfo", {}).get("name") != "nokv-mcp" \
                    or result.get("capabilities") != {"tools": {}}:
                raise WorkflowFailure("unexpected MCP initialize result")
            session.notify("notifications/initialized")
            evidence.line("processes.jsonl", {
                "schema": SCHEMA, "label": "mcp", "argv": redact_argv(mcp_command(config)),
                "pid": process.pid, "attempt": attempt, "started_at": now(),
            })
            return session, stderr
        except WorkflowFailure as error:
            last_error = str(error)
            stop(process)
            stderr.close()
            time.sleep(0.25)
    raise WorkflowFailure(f"MCP startup failed: {last_error}")


def transfer(config: Config, evidence: Evidence) -> None:
    sandbox = evidence.root / "sandbox"
    sandbox.mkdir()
    materialized, collected = sandbox / "scan.json", sandbox / "reconstruction.json"
    completed_process(evidence, "materialize", materialize_command(config, materialized), config)
    if json.loads(materialized.read_text()).get("state") != "calibrated":
        raise WorkflowFailure("materialized input is not calibrated")
    collected.write_bytes(reconstruction_bytes())
    result = completed_process(evidence, "collect", collect_command(config, collected), config)
    if json.loads(result.stdout).get("status") != "success":
        raise WorkflowFailure("collect did not return success")


def run_live(config: Config, evidence: Evidence, steps: list[ToolStep]) -> None:
    provision = completed_process(evidence, "provision", provision_command(config), config)
    if json.loads(provision.stdout).get("lifecycle") != "active":
        raise WorkflowFailure("provision did not activate root placement")
    serve_out = (evidence.root / "serve.stdout.log").open("w")
    serve_err = (evidence.root / "serve.stderr.log").open("w")
    server = subprocess.Popen(
        server_command(config), cwd=config.repo, stdin=subprocess.DEVNULL,
        stdout=serve_out, stderr=serve_err, text=True, start_new_session=True,
    )
    evidence.line("processes.jsonl", {
        "schema": SCHEMA, "label": "serve", "argv": redact_argv(server_command(config)),
        "pid": server.pid, "started_at": now(),
    })
    session, mcp_err = None, None
    try:
        session, mcp_err = start_mcp(config, evidence, server)
        listed = session.request("tools/list")
        tools = listed.get("result", {}).get("tools")
        if not isinstance(tools, list):
            raise WorkflowFailure("tools/list did not return a tools array")
        validate_tool_contract(tools)
        evidence.json("contract.json", contract_evidence(tools))
        results: dict[str, dict[str, Any]] = {}
        for step in steps:
            results[step.label] = session.call(step)
            if step.label == "edit-input":
                transfer(config, evidence)
        assert_results(results, config)
    finally:
        if session is not None:
            if session.process.stdin is not None:
                session.process.stdin.close()
            evidence.line("processes.jsonl", {
                "schema": SCHEMA, "label": "mcp-exit", "returncode": stop(session.process),
                "finished_at": now(),
            })
        if mcp_err is not None:
            mcp_err.close()
        evidence.line("processes.jsonl", {
            "schema": SCHEMA, "label": "serve-exit", "returncode": stop(server),
            "finished_at": now(),
        })
        serve_out.close()
        serve_err.close()


def shell_output(command: list[str], cwd: Path) -> str | None:
    try:
        result = subprocess.run(command, cwd=cwd, text=True, capture_output=True,
                                timeout=10, check=False)
    except (OSError, subprocess.SubprocessError):
        return None
    return result.stdout.strip() if result.returncode == 0 else None


def environment(config: Config) -> dict[str, Any]:
    binary = config.binary
    return {
        "schema": SCHEMA, "captured_at": now(),
        "git_commit": shell_output(["git", "rev-parse", "HEAD"], config.repo),
        "git_status_porcelain": shell_output(["git", "status", "--porcelain=v1"], config.repo),
        "rustc": shell_output(["rustc", "--version", "--verbose"], config.repo),
        "cargo": shell_output(["cargo", "--version"], config.repo),
        "python": sys.version, "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "binary": {"path": str(binary), "size_bytes": binary.stat().st_size,
                   "sha256": digest_file(binary)},
        "config": {
            "root_id": config.root_id, "logical_shard_id": config.shard_id,
            "agent": config.agent, "workbench_root": config.workbench_root,
            "workbench": config.workbench, "restored_workbench": config.restored,
            "etcd": config.etcd, "metadata": str(config.metadata),
            "metadata_mode": config.metadata_mode, "bind": config.bind,
            "object_endpoint": config.object_endpoint, "object_bucket": config.bucket,
            "object_root": config.object_root, "access_key_present": config.access_key is not None,
            "secret_key_sha256": digest(config.secret_key.encode())
            if config.secret_key is not None else None,
        },
    }


def plan(config: Config, steps: list[ToolStep]) -> dict[str, Any]:
    sandbox = config.evidence / "sandbox"
    coverage = planned_tool_coverage(steps)
    return {
        "schema": SCHEMA, "mode": "dry-run" if config.dry_run else "live",
        "commands": {
            "build": ["cargo", "build", "-p", "nokv", "--bin", "nokv"]
            if config.build else None,
            "provision": redact_argv(provision_command(config)),
            "serve": redact_argv(server_command(config)),
            "mcp": redact_argv(mcp_command(config)),
            "materialize": redact_argv(materialize_command(config, sandbox / "scan.json")),
            "collect": redact_argv(collect_command(config, sandbox / "reconstruction.json")),
        },
        "tool_steps": [dataclasses.asdict(step) for step in steps],
        "tool_coverage": {"expected": sorted(WORKBENCH_TOOLS), "planned": sorted(coverage),
                          "count": len(coverage), "complete": coverage == WORKBENCH_TOOLS},
    }


def qualification(state: str, reason: str, workflow: str,
                  transcript: str | None = None) -> dict[str, Any]:
    gate_zero = "FAIL" if state == "FAIL" else "NOT QUALIFIED"
    gate_reason = reason
    if workflow == "PASS":
        gate_reason = ("The live 18-tool workflow passed, but the one-day snapshot lease "
                       "did not expire and reach reaped state; Gate 0 is partial evidence.")
    return {
        "schema": SCHEMA, "recorded_at": now(), "overall_status": state, "reason": reason,
        "first_client_workflow": {"status": workflow, "transcript_sha256": transcript},
        "acceptance_gates": {
            str(index): {
                "status": gate_zero if index == 0 else "NOT QUALIFIED",
                "reason": gate_reason if index == 0
                else "This first-client harness does not qualify this gate.",
            }
            for index in range(9)
        },
    }


def parse_args(argv: list[str] | None = None) -> Config:
    repo = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--nokv-bin", type=Path,
                        default=Path(os.getenv("NOKV_LIVE_NOKV_BIN", repo / "target/debug/nokv")))
    parser.add_argument("--evidence-dir", type=Path)
    parser.add_argument("--metadata-dir", type=Path)
    parser.add_argument("--metadata-mode", choices=("create", "reopen"), default="create")
    parser.add_argument("--root-id", default=os.getenv("NOKV_LIVE_ROOT_ID", "11" * 16))
    parser.add_argument("--logical-shard-id",
                        default=os.getenv("NOKV_LIVE_LOGICAL_SHARD_ID", "22" * 16))
    parser.add_argument("--agent-id", default="lingtai")
    parser.add_argument("--workbench-id", default="ptycho-first-client")
    parser.add_argument("--restored-workbench-id", default="ptycho-first-client-restored")
    parser.add_argument("--snapshot-name", default="ptycho-frozen")
    parser.add_argument("--etcd-endpoint", action="append")
    parser.add_argument("--etcd-key-prefix", default="/nokv/control")
    parser.add_argument("--server-bind", default="127.0.0.1:7750")
    parser.add_argument("--advertise-endpoint", default="127.0.0.1:7750")
    parser.add_argument("--node-id")
    parser.add_argument("--object-bucket", default=os.getenv("NOKV_LIVE_S3_BUCKET", ""))
    parser.add_argument("--object-endpoint", default=os.getenv("NOKV_LIVE_S3_ENDPOINT", ""))
    parser.add_argument("--object-root", default="lingtai-first-client")
    parser.add_argument("--object-region", default="us-east-1")
    parser.add_argument("--object-access-key-id",
                        default=os.getenv("NOKV_LIVE_S3_ACCESS_KEY_ID"))
    parser.add_argument("--object-secret-access-key",
                        default=os.getenv("NOKV_LIVE_S3_SECRET_ACCESS_KEY"))
    parser.add_argument("--command-timeout-seconds", type=float, default=30)
    args = parser.parse_args(argv)
    etcd = args.etcd_endpoint or [item for item in
                                  os.getenv("NOKV_LIVE_ETCD_ENDPOINTS", "").split(",") if item]
    if args.dry_run:
        etcd = etcd or ["http://127.0.0.1:2379"]
        args.object_bucket = args.object_bucket or "nokv-live-dry-run"
        args.object_endpoint = args.object_endpoint or "http://127.0.0.1:9000"
    evidence = args.evidence_dir or (
        repo / "target/lingtai-workbench/evidence" /
        f"gate0-{args.agent_id}-{args.workbench_id}-{args.root_id[:8]}"
    )
    metadata = args.metadata_dir or (
        repo / "target/lingtai-workbench/metadata" /
        f"{args.root_id}-{args.metadata_mode}"
    )
    return Config(
        repo, args.nokv_bin.resolve(), evidence.resolve(), metadata.resolve(),
        args.metadata_mode, args.root_id, args.logical_shard_id, args.agent_id,
        args.workbench_id, args.restored_workbench_id, args.snapshot_name, tuple(etcd),
        args.etcd_key_prefix, args.server_bind, args.advertise_endpoint,
        args.node_id or f"lingtai-{args.root_id[:8]}", args.object_bucket,
        args.object_endpoint, args.object_root, args.object_region,
        args.object_access_key_id, args.object_secret_access_key, args.build,
        args.dry_run, args.command_timeout_seconds,
    )


def main(argv: list[str] | None = None) -> int:
    config = parse_args(argv)
    evidence, steps, prepared = Evidence(config.evidence), tool_plan(config), False
    try:
        evidence.prepare()
        prepared = True
        evidence.json("plan.json", plan(config, steps))
        if planned_tool_coverage(steps) != WORKBENCH_TOOLS:
            raise WorkflowFailure("tool plan does not cover exactly 18 tools")
        validate(config, live=False)
        if config.dry_run:
            record = qualification(
                "NOT QUALIFIED", "Dry-run validated commands and exact 18-tool coverage; "
                "no live dependency ran.", "NOT QUALIFIED"
            )
            evidence.json("qualification.json", record)
            print(json.dumps(record, indent=2, sort_keys=True))
            return 0
        if config.build:
            if shutil.which("cargo") is None:
                raise NotQualified("cargo is unavailable for --build")
            old_timeout = config.timeout
            config = dataclasses.replace(config, timeout=max(old_timeout, 900))
            completed_process(evidence, "build",
                              ["cargo", "build", "-p", "nokv", "--bin", "nokv"], config)
            config = dataclasses.replace(config, timeout=old_timeout)
        validate(config, live=True)
        evidence.json("environment.json", environment(config))
        run_live(config, evidence, steps)
        transcript = digest_file(evidence.root / "mcp-transcript.jsonl")
        record = qualification(
            "NOT QUALIFIED", "Live first-client workflow passed; full system acceptance "
            "requires the remaining gates.", "PASS", transcript
        )
        evidence.json("qualification.json", record)
        print(json.dumps(record, indent=2, sort_keys=True))
        return 0
    except NotQualified as error:
        record = qualification("NOT QUALIFIED", str(error), "NOT QUALIFIED")
        if prepared:
            evidence.json("qualification.json", record)
        print(json.dumps(record, indent=2, sort_keys=True))
        return 3
    except (WorkflowFailure, OSError, ValueError, json.JSONDecodeError) as error:
        record = qualification("FAIL", str(error), "FAIL")
        if prepared:
            evidence.json("qualification.json", record)
        print(json.dumps(record, indent=2, sort_keys=True))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
