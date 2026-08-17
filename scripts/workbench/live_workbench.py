#!/usr/bin/env python3
"""Black-box scientific Workbench evidence over the flat ``nokv`` binary."""

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

from source_bound_producer import ProducerError, ScenarioContract
from typed_live_qualification import (
    gap_record,
    load_live_context,
    publish_live_result,
)
from workbench_contract import (
    WORKBENCH_TOOLS,
    contract_evidence,
    validate_tool_contract,
)


SCHEMA = "nokv.workbench.live_evidence.v1"
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
OPTIONAL_SCOPE_RESULT_PAIRS = (
    ("stat-input-root", "stat-input-root-empty-path"),
    ("list-input", "list-input-empty-path"),
    ("grep-input", "grep-input-empty-path"),
    ("search", "search-empty-path"),
    ("aggregate", "aggregate-empty-path"),
    ("catalog", "catalog-empty-path"),
)
TYPED_EVIDENCE_ROLES = ("producer-result", "qualification", "mcp-transcript")
TYPED_SCENARIOS = {
    "t01.create-schema": ScenarioContract("T01", "schema-surface"),
    "t01.create-live": ScenarioContract("T01", "native-workbench-e2e"),
    "t02.put-schema": ScenarioContract("T02", "schema-surface"),
    "t02.put-absent-and-exact-bytes-live": ScenarioContract(
        "T02", "native-workbench-e2e"
    ),
    "t03.append-schema": ScenarioContract("T03", "schema-surface"),
    "t03.append-absent-and-atomic-live": ScenarioContract(
        "T03", "native-workbench-e2e"
    ),
    "t04.edit-schema": ScenarioContract("T04", "schema-surface"),
    "t04.edit-revalidate-conflict-live": ScenarioContract(
        "T04", "native-workbench-e2e"
    ),
    "t05.list-live-and-frozen-live": ScenarioContract("T05", "native-workbench-e2e"),
    "t06.stat-live-frozen-root-live": ScenarioContract("T06", "native-workbench-e2e"),
    "t07.get-exact-live-and-frozen-bytes": ScenarioContract(
        "T07", "native-workbench-e2e"
    ),
    "t08.grep-empty-root-live": ScenarioContract("T08", "native-workbench-e2e"),
    "t09.search-missing-root-live": ScenarioContract("T09", "native-workbench-e2e"),
    "t10.aggregate-empty-root-live": ScenarioContract("T10", "native-workbench-e2e"),
    "t11.describe-empty-catalog-live": ScenarioContract("T11", "native-workbench-e2e"),
    "t12.find-committed-state-live": ScenarioContract("T12", "native-workbench-e2e"),
    "t13.commit-absent-workbench-live": ScenarioContract("T13", "native-workbench-e2e"),
    "t14.snapshot-committed-workbench-live": ScenarioContract(
        "T14", "native-workbench-e2e"
    ),
    "t15.renew-live": ScenarioContract("T15", "native-workbench-e2e"),
    "t16.retire-live": ScenarioContract("T16", "native-workbench-e2e"),
    "c01.exact-18-tool-schema": ScenarioContract("C01", "schema-surface"),
    "c02.closed-input-schema": ScenarioContract("C02", "schema-surface"),
    "c03.path-jail-and-five-sections-live": ScenarioContract(
        "C03", "native-workbench-e2e"
    ),
    "c04.put-append-commit-absent-live": ScenarioContract(
        "C04", "native-workbench-e2e"
    ),
    "c05.empty-path-equivalence-live": ScenarioContract("C05", "native-workbench-e2e"),
    "c06.two-persisted-rootids-isolate-all-operations": ScenarioContract(
        "C06", "root-authority-e2e"
    ),
    "c07.exclusive-payload-schema": ScenarioContract("C07", "schema-surface"),
    "c08.create-replace-generation-digest-live": ScenarioContract(
        "C08", "native-workbench-e2e"
    ),
    "c09.append-absent-file-live": ScenarioContract("C09", "native-workbench-e2e"),
    "c10.edit-exact-and-global-live": ScenarioContract("C10", "native-workbench-e2e"),
    "c11.revision-authority-pagination-live": ScenarioContract(
        "C11", "native-workbench-e2e"
    ),
    "c12.live-and-frozen-read-metadata-live": ScenarioContract(
        "C12", "native-workbench-e2e"
    ),
    "c13.grep-live": ScenarioContract("C13", "native-workbench-e2e"),
    "c14.query-missing-root-live": ScenarioContract("C14", "native-workbench-e2e"),
    "c15.find-committed-manifest-live": ScenarioContract("C15", "native-workbench-e2e"),
    "c16.commit-head-authority-live": ScenarioContract("C16", "native-workbench-e2e"),
    "c19.frozen-read-after-live-mutation": ScenarioContract(
        "C19", "native-workbench-e2e"
    ),
    "c20.restore-validation-and-publication-live": ScenarioContract(
        "C20", "native-workbench-e2e"
    ),
    "l01.generic-seven-tool-profile-schema": ScenarioContract("L01", "schema-surface"),
    "l01.generic-seven-tool-profile-live": ScenarioContract(
        "L01", "native-workbench-e2e"
    ),
    "l02.rootid-workspace-client-live": ScenarioContract("L02", "native-workbench-e2e"),
    "l08.current-workbench-cli-live": ScenarioContract("L08", "native-workbench-e2e"),
}
TYPED_UNSUPPORTED_SCENARIOS = frozenset(
    {
        "l01.generic-seven-tool-profile-live",
        "l02.rootid-workspace-client-live",
        "l08.current-workbench-cli-live",
    }
)


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
    agent_name: str
    agent_id: str
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
    qualification_result: Path | None = None

    @property
    def workbench_root(self) -> str:
        return f"/agents/{self.agent_name}/wb"


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
        "experiment": "ptychography",
        "frames": 2,
        "revision": revision,
        "state": state,
    }
    return (canonical_json(value) + "\n").encode()


def reconstruction_bytes() -> bytes:
    value = {
        "algorithm": "phase-retrieval",
        "frames": 2,
        "source_state": "calibrated",
        "status": "converged",
    }
    return (canonical_json(value) + "\n").encode()


def phase_one_workbench_ids(config: Config) -> dict[str, str]:
    def fixture_id(operation: str) -> str:
        suffix = digest(f"{config.root_id}\0{operation}".encode())[:12]
        return f"phase1-{operation}-{suffix}"

    return {
        operation: fixture_id(operation) for operation in ("put", "append", "commit")
    }


def authority_configs(config: Config) -> tuple[Config, Config]:
    """Build one legitimate peer and one wrong-principal replay for the same shard."""

    def derived_id(domain: bytes, value: str) -> str:
        return digest(domain + b"\0" + value.encode())[:32]

    peer = dataclasses.replace(
        config,
        root_id=derived_id(b"nokv.workbench.authority.peer.root.v1", config.root_id),
        agent_id=derived_id(b"nokv.workbench.authority.peer.agent.v1", config.agent_id),
        agent_name="authority-peer",
    )
    mismatch = dataclasses.replace(
        peer,
        root_id=config.root_id,
        agent_name="authority-mismatch",
    )
    return peer, mismatch


def tool_plan(config: Config) -> list[ToolStep]:
    wb, restored, snapshot = config.workbench, config.restored, config.snapshot
    phase_one = phase_one_workbench_ids(config)
    raw = scan_bytes("raw", 0).decode()
    replacement = scan_bytes("raw", 1).decode()
    commit_digest = "sha256:" + digest(reconstruction_bytes())
    implicit_commit_digest = "sha256:" + digest(b"phase1 implicit commit\n")
    return [
        ToolStep("create", "workbench_create", {"id": wb}),
        ToolStep(
            "put-input",
            "workbench_put_file",
            {
                "id": wb,
                "section": "input",
                "path": "scan.json",
                "text": raw,
                "content_type": "application/json",
                "replace": False,
            },
        ),
        ToolStep(
            "implicit-put",
            "workbench_put_file",
            {
                "id": phase_one["put"],
                "section": "outputs",
                "path": "a.txt",
                "text": "first needle\n",
                "content_type": "text/plain",
                "replace": False,
            },
        ),
        ToolStep(
            "implicit-put-replay",
            "workbench_put_file",
            {
                "id": phase_one["put"],
                "section": "outputs",
                "path": "a.txt",
                "text": "first needle\n",
                "content_type": "text/plain",
                "replace": False,
            },
            "AlreadyExists",
        ),
        ToolStep(
            "implicit-put-second",
            "workbench_put_file",
            {
                "id": phase_one["put"],
                "section": "outputs",
                "path": "b.txt",
                "text": "second NEEDLE\n",
                "content_type": "text/plain",
                "replace": False,
            },
        ),
        ToolStep(
            "create-only-error",
            "workbench_put_file",
            {
                "id": wb,
                "section": "input",
                "path": "scan.json",
                "text": raw,
                "replace": False,
            },
            "AlreadyExists",
        ),
        ToolStep(
            "replace-input",
            "workbench_put_file",
            {
                "id": wb,
                "section": "input",
                "path": "scan.json",
                "text": replacement,
                "content_type": "application/json",
                "replace": True,
            },
        ),
        ToolStep(
            "append-log",
            "workbench_append",
            {
                "id": wb,
                "section": "logs",
                "path": "reconstruction.log",
                "text": "ptychography reconstruction started\n",
                "content_type": "text/plain",
            },
        ),
        ToolStep(
            "implicit-append",
            "workbench_append",
            {
                "id": phase_one["append"],
                "section": "logs",
                "path": "events.log",
                "text": "implicit append\n",
                "content_type": "text/plain",
            },
        ),
        ToolStep(
            "read-input",
            "workbench_read",
            {
                "id": wb,
                "section": "input",
                "path": "scan.json",
                "format": "structured",
                "limit": 10,
            },
        ),
        ToolStep(
            "stat-input",
            "workbench_stat",
            {"id": wb, "section": "input", "path": "scan.json"},
        ),
        ToolStep("stat-input-root", "workbench_stat", {"id": wb, "section": "input"}),
        ToolStep(
            "stat-input-root-empty-path",
            "workbench_stat",
            {"id": wb, "section": "input", "path": ""},
        ),
        ToolStep(
            "list-input",
            "workbench_list",
            {"id": wb, "section": "input", "limit": 100},
        ),
        ToolStep(
            "list-input-empty-path",
            "workbench_list",
            {"id": wb, "section": "input", "path": "", "limit": 100},
        ),
        ToolStep(
            "grep-input",
            "workbench_grep",
            {
                "id": wb,
                "section": "input",
                "pattern": "PTYCHOGRAPHY",
                "glob": "*.json",
                "recursive": True,
                "limit": 10,
            },
        ),
        ToolStep(
            "grep-input-empty-path",
            "workbench_grep",
            {
                "id": wb,
                "section": "input",
                "path": "",
                "pattern": "PTYCHOGRAPHY",
                "glob": "*.json",
                "recursive": True,
                "limit": 10,
            },
        ),
        ToolStep(
            "grep-phase1-page-1",
            "workbench_grep",
            {
                "id": phase_one["put"],
                "section": "outputs",
                "pattern": "NeEdLe",
                "glob": "*.txt",
                "recursive": True,
                "limit": 1,
            },
        ),
        ToolStep(
            "edit-input",
            "workbench_edit",
            {
                "id": wb,
                "section": "input",
                "path": "scan.json",
                "old_string": '"state":"raw"',
                "new_string": '"state":"calibrated"',
                "replace_all": False,
            },
        ),
        ToolStep("search", "workbench_search", {"id": wb, "limit": 10}),
        ToolStep(
            "search-empty-path", "workbench_search", {"id": wb, "path": "", "limit": 10}
        ),
        ToolStep(
            "aggregate",
            "workbench_aggregate",
            {
                "id": wb,
                "measures": [{"name": "artifacts", "op": "count"}],
                "limit": 100,
            },
        ),
        ToolStep(
            "aggregate-empty-path",
            "workbench_aggregate",
            {
                "id": wb,
                "path": "",
                "measures": [{"name": "artifacts", "op": "count"}],
                "limit": 100,
            },
        ),
        ToolStep(
            "catalog",
            "workbench_catalog",
            {"id": wb, "include_facets": True},
        ),
        ToolStep(
            "catalog-empty-path",
            "workbench_catalog",
            {"id": wb, "path": "", "include_facets": True},
        ),
        ToolStep(
            "commit",
            "workbench_commit",
            {
                "id": wb,
                "manifest": {
                    "dataset": "ptychography-scan",
                    "model": "scientific-reconstruction",
                    "task": "ptychography",
                },
                "content_digest_uri": commit_digest,
                "replace": False,
            },
        ),
        ToolStep(
            "commit-replay",
            "workbench_commit",
            {
                "id": wb,
                "manifest": {
                    "dataset": "ptychography-scan",
                    "model": "scientific-reconstruction",
                    "task": "ptychography",
                },
                "content_digest_uri": commit_digest,
                "replace": False,
            },
        ),
        ToolStep(
            "implicit-commit",
            "workbench_commit",
            {
                "id": phase_one["commit"],
                "manifest": {"fixture": "phase1-implicit-commit", "phase": 1},
                "content_digest_uri": implicit_commit_digest,
                "replace": False,
            },
        ),
        ToolStep(
            "implicit-commit-replay",
            "workbench_commit",
            {
                "id": phase_one["commit"],
                "manifest": {"fixture": "phase1-implicit-commit", "phase": 1},
                "content_digest_uri": implicit_commit_digest,
                "replace": False,
            },
        ),
        ToolStep(
            "read-run-manifest",
            "workbench_read",
            {
                "id": wb,
                "section": "metadata",
                "path": "run_manifest.json",
                "format": "structured",
                "limit": 10,
            },
        ),
        ToolStep(
            "snapshot",
            "workbench_snapshot",
            {
                "id": wb,
                "name": snapshot,
                "ttl_days": 1,
                "reason": "research workbench frozen reconstruction",
                "metadata": {"domain": "imaging", "workflow": "ptychography"},
            },
        ),
        ToolStep(
            "post-snapshot-edit",
            "workbench_edit",
            {
                "id": wb,
                "section": "input",
                "path": "scan.json",
                "old_string": '"state":"calibrated"',
                "new_string": '"state":"post-snapshot"',
                "replace_all": False,
            },
        ),
        ToolStep(
            "frozen-read",
            "workbench_read",
            {
                "id": wb,
                "section": "input",
                "path": "scan.json",
                "format": "structured",
                "at_snapshot": snapshot,
                "limit": 10,
            },
        ),
        ToolStep(
            "live-read-after-snapshot",
            "workbench_read",
            {
                "id": wb,
                "section": "input",
                "path": "scan.json",
                "format": "structured",
                "limit": 10,
            },
        ),
        ToolStep("snapshot-list-alive", "workbench_snapshot_list", {"id": wb}),
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
            {
                "id": restored,
                "section": "input",
                "path": "scan.json",
                "format": "structured",
                "limit": 10,
            },
        ),
        ToolStep(
            "read-restore-manifest",
            "workbench_read",
            {
                "id": restored,
                "section": "metadata",
                "path": "restore_manifest.json",
                "format": "structured",
                "limit": 10,
            },
        ),
        ToolStep(
            "find",
            "workbench_find",
            {
                "committed": True,
                "manifest_pattern": "PtYcHoGrApHy",
                "include_manifest": True,
                "limit": 100,
            },
        ),
        ToolStep(
            "snapshot-retire",
            "workbench_snapshot_retire",
            {"id": wb, "name": snapshot, "reason": "workbench workflow complete"},
        ),
        ToolStep(
            "snapshot-retire-replay",
            "workbench_snapshot_retire",
            {"id": wb, "name": snapshot, "reason": "must not replace durable evidence"},
        ),
        ToolStep("snapshot-list-retired", "workbench_snapshot_list", {"id": wb}),
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
        "--object-bucket",
        config.bucket,
        "--object-endpoint",
        config.object_endpoint,
        "--object-root",
        config.object_root,
        "--object-region",
        config.object_region,
    ]
    if config.access_key is not None:
        result += ["--object-access-key-id", config.access_key]
    if config.secret_key is not None:
        result += ["--object-secret-access-key", config.secret_key]
    return result


def client_args(config: Config) -> list[str]:
    return [
        str(config.binary),
        *control_args(config),
        "--agent-id",
        config.agent_id,
        "--workbench-root",
        config.workbench_root,
        *object_args(config),
    ]


def provision_command(config: Config) -> list[str]:
    return [
        str(config.binary),
        *control_args(config),
        "--agent-id",
        config.agent_id,
        *object_args(config),
        "provision",
        config.shard_id,
    ]


def server_command(config: Config) -> list[str]:
    return [
        str(config.binary),
        *control_args(config),
        *object_args(config),
        "--bind",
        config.bind,
        "--advertise-endpoint",
        config.advertise,
        "--node-id",
        config.node,
        f"--metadata-{config.metadata_mode}",
        str(config.metadata),
        "--lifecycle-interval-millis",
        "100",
        "serve",
    ]


def mcp_command(config: Config) -> list[str]:
    return [*client_args(config), "mcp"]


def materialize_command(config: Config, destination: Path) -> list[str]:
    return [
        *client_args(config),
        "materialize",
        config.workbench,
        "input",
        "scan.json",
        str(destination),
    ]


def collect_command(config: Config, source: Path) -> list[str]:
    return [
        *client_args(config),
        "collect",
        config.workbench,
        "outputs",
        str(source),
        "reconstruction.json",
        "--content-type",
        "application/json",
    ]


def redact_argv(argv: Iterable[str]) -> list[str]:
    output, redact_next = [], False
    for argument in argv:
        if redact_next:
            output.append("<redacted>")
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
    evidence: Evidence,
    label: str,
    command: list[str],
    config: Config,
) -> subprocess.CompletedProcess[str]:
    started = now()
    try:
        result = subprocess.run(
            command,
            cwd=config.repo,
            text=True,
            capture_output=True,
            timeout=config.timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as error:
        evidence.line(
            "processes.jsonl",
            {
                "schema": SCHEMA,
                "label": label,
                "argv": redact_argv(command),
                "started_at": started,
                "finished_at": now(),
                "timed_out": True,
                "stdout": error.stdout or "",
                "stderr": error.stderr or "",
            },
        )
        raise WorkflowFailure(f"{label} timed out") from error
    evidence.line(
        "processes.jsonl",
        {
            "schema": SCHEMA,
            "label": label,
            "argv": redact_argv(command),
            "started_at": started,
            "finished_at": now(),
            "returncode": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
        },
    )
    if result.returncode:
        detail = result.stderr.strip() or result.stdout.strip() or "no output"
        raise WorkflowFailure(f"{label} failed ({result.returncode}): {detail}")
    return result


def expected_failure_process(
    evidence: Evidence,
    label: str,
    command: list[str],
    config: Config,
    stdin: str,
) -> subprocess.CompletedProcess[str]:
    started = now()
    try:
        result = subprocess.run(
            command,
            cwd=config.repo,
            input=stdin,
            text=True,
            capture_output=True,
            timeout=config.timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as error:
        evidence.line(
            "processes.jsonl",
            {
                "schema": SCHEMA,
                "label": label,
                "argv": redact_argv(command),
                "started_at": started,
                "finished_at": now(),
                "timed_out": True,
                "stdout": error.stdout or "",
                "stderr": error.stderr or "",
            },
        )
        raise WorkflowFailure(f"{label} timed out instead of failing closed") from error
    evidence.line(
        "processes.jsonl",
        {
            "schema": SCHEMA,
            "label": label,
            "argv": redact_argv(command),
            "started_at": started,
            "finished_at": now(),
            "returncode": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
        },
    )
    if result.returncode == 0:
        raise WorkflowFailure(f"{label} unexpectedly succeeded")
    return result


class Mcp:
    def __init__(
        self,
        process: subprocess.Popen[str],
        evidence: Evidence,
        attempt: int,
        timeout: float,
    ) -> None:
        self.process, self.evidence = process, evidence
        self.attempt, self.timeout, self.next_id = attempt, timeout, 1

    def request(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        normalized: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
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
            raise WorkflowFailure(
                f"MCP exited before responding ({self.process.poll()})"
            )
        try:
            response = json.loads(received)
        except json.JSONDecodeError as error:
            raise WorkflowFailure(f"MCP returned invalid JSON: {received!r}") from error
        self.evidence.line(
            "mcp-transcript.jsonl",
            {
                "schema": SCHEMA,
                "attempt": self.attempt,
                "sequence": request_id,
                "request_raw": sent,
                "request": request,
                "normalized_input": normalized,
                "response_raw": received,
                "response": response,
            },
        )
        if response.get("jsonrpc") != "2.0" or response.get("id") != request_id:
            raise WorkflowFailure("MCP response envelope mismatch")
        return response

    def notify(self, method: str) -> None:
        request = {"jsonrpc": "2.0", "method": method}
        sent = canonical_json(request)
        assert self.process.stdin is not None
        self.process.stdin.write(sent + "\n")
        self.process.stdin.flush()
        self.evidence.line(
            "mcp-transcript.jsonl",
            {
                "schema": SCHEMA,
                "attempt": self.attempt,
                "sequence": None,
                "request_raw": sent,
                "request": request,
                "normalized_input": None,
                "response_raw": None,
                "response": None,
            },
        )

    def call(self, step: ToolStep) -> dict[str, Any]:
        response = self.request(
            "tools/call",
            {"name": step.name, "arguments": step.arguments},
            {
                "name": step.name,
                "arguments": json.loads(canonical_json(step.arguments)),
            },
        )
        result = response.get("result")
        structured = (
            result.get("structuredContent") if isinstance(result, dict) else None
        )
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
    if (
        result.get("record_type") != "json_object"
        or not isinstance(items, list)
        or len(items) != 1
        or not isinstance(items[0].get("value"), dict)
    ):
        raise WorkflowFailure(f"{label} did not return one JSON object")
    return items[0]["value"]


def assert_phase_one_results(
    results: dict[str, dict[str, Any]],
    config: Config,
) -> dict[str, Any]:
    scope_digests: dict[str, str] = {}
    for omitted, explicit_empty in OPTIONAL_SCOPE_RESULT_PAIRS:
        if results[omitted] != results[explicit_empty]:
            raise WorkflowFailure(
                f"optional scope path differs between {omitted} and {explicit_empty}"
            )
        scope_digests[omitted] = digest(canonical_json(results[omitted]).encode())

    phase_one = phase_one_workbench_ids(config)
    implicit_put = results["implicit-put"]
    implicit_put_second = results["implicit-put-second"]
    if (
        implicit_put.get("workbench_id") != phase_one["put"]
        or implicit_put.get("generation") != 1
        or implicit_put.get("replace") is not False
        or implicit_put_second.get("workbench_id") != phase_one["put"]
        or implicit_put_second.get("generation") != 1
        or implicit_put_second.get("replace") is not False
        or results["implicit-put-replay"].get("code") != "AlreadyExists"
    ):
        raise WorkflowFailure("implicit put did not create one path-native Workbench")

    implicit_append = results["implicit-append"]
    if (
        implicit_append.get("workbench_id") != phase_one["append"]
        or implicit_append.get("created") is not True
        or implicit_append.get("generation") != 1
    ):
        raise WorkflowFailure(
            "implicit append did not create one path-native Workbench"
        )

    implicit_commit = results["implicit-commit"]
    implicit_commit_replay = results["implicit-commit-replay"]
    if (
        implicit_commit.get("workbench_id") != phase_one["commit"]
        or implicit_commit.get("idempotent_replay") is not False
        or implicit_commit_replay.get("workbench_id") != phase_one["commit"]
        or implicit_commit_replay.get("idempotent_replay") is not True
        or implicit_commit.get("commit_identity")
        != implicit_commit_replay.get("commit_identity")
    ):
        raise WorkflowFailure("implicit commit exact replay did not converge")

    matches = results["find"].get("matches")
    if not isinstance(matches, list) or not any(
        match.get("workbench_id") == config.workbench
        and match.get("committed") is True
        and match.get("commit_identity_verified") is True
        for match in matches
        if isinstance(match, dict)
    ):
        raise WorkflowFailure(
            "mixed-case manifest find omitted the committed Workbench"
        )

    first_page = results["grep-phase1-page-1"]
    second_page = results["grep-phase1-page-2"]
    first_matches = first_page.get("matches")
    second_matches = second_page.get("matches")
    cursor = first_page.get("next_cursor")
    if (
        not isinstance(first_matches, list)
        or len(first_matches) != 1
        or not isinstance(second_matches, list)
        or len(second_matches) != 1
        or not isinstance(cursor, str)
        or not cursor
        or first_page.get("truncated") is not True
        or second_page.get("next_cursor") is not None
        or second_page.get("truncated") is not False
    ):
        raise WorkflowFailure(
            "grep continuation did not return two bounded terminal pages"
        )
    first_paths = {
        match.get("path") for match in first_matches if isinstance(match, dict)
    }
    second_paths = {
        match.get("path") for match in second_matches if isinstance(match, dict)
    }
    expected_paths = {
        f"{config.workbench_root}/{phase_one['put']}/outputs/a.txt",
        f"{config.workbench_root}/{phase_one['put']}/outputs/b.txt",
    }
    if (
        len(first_paths) != 1
        or len(second_paths) != 1
        or first_paths.intersection(second_paths)
        or first_paths.union(second_paths) != expected_paths
    ):
        raise WorkflowFailure("grep continuation pages overlap or omit fixture paths")

    matched_ids = sorted(
        {
            match["workbench_id"]
            for match in matches
            if isinstance(match, dict) and isinstance(match.get("workbench_id"), str)
        }
    )
    return {
        "schema": SCHEMA,
        "checks": {
            "C04": {
                "status": "PASS",
                "workbench_ids": phase_one,
                "implicit_commit_identity": implicit_commit.get("commit_identity"),
            },
            "C05": {"status": "PASS", "result_sha256": scope_digests},
            "C15": {
                "status": "PASS",
                "manifest_pattern": "PtYcHoGrApHy",
                "matched_workbench_ids": matched_ids,
            },
            "T08": {
                "status": "PASS",
                "matched_paths": sorted(expected_paths),
                "page_one_cursor_sha256": digest(cursor.encode()),
            },
        },
    }


def assert_authority_results(
    results: dict[str, dict[str, Any]],
    mismatch: subprocess.CompletedProcess[str],
    config: Config,
    peer: Config,
) -> dict[str, Any]:
    if results["peer-read-before-create"].get("code") != "NotFound":
        raise WorkflowFailure(
            "peer RootId observed the primary Workbench before creation"
        )
    peer_put = results["peer-put"]
    if (
        peer_put.get("workbench_id") != config.workbench
        or peer_put.get("generation") != 1
        or peer_put.get("replace") is not False
    ):
        raise WorkflowFailure(
            "peer RootId did not create an independent same-name Workbench"
        )
    peer_document = document(results["peer-read"], "peer authority read")
    reconnect_document = document(
        results["peer-reconnect-read"], "peer authority reconnect read"
    )
    primary_document = document(
        results["primary-read-after-peer-write"], "primary authority read"
    )
    if peer_document != {"authority": "peer"} or reconnect_document != peer_document:
        raise WorkflowFailure("peer Agent binding did not survive an exact reconnect")
    if (
        primary_document.get("state") != "post-snapshot"
        or "authority" in primary_document
    ):
        raise WorkflowFailure(
            "RootId isolation allowed a peer write into the primary Workbench"
        )

    combined_error = f"{mismatch.stdout}\n{mismatch.stderr}"
    if (
        mismatch.returncode == 0
        or mismatch.stdout.strip()
        or "already bound to another Agent" not in combined_error
        or "jsonrpc" in combined_error.lower()
    ):
        raise WorkflowFailure(
            "wrong AgentId was not rejected before MCP initialization"
        )
    if config.agent_id in combined_error or peer.agent_id in combined_error:
        raise WorkflowFailure(
            "Agent binding mismatch disclosed a stable Agent identity"
        )
    return {
        "schema": SCHEMA,
        "status": "PASS",
        "contract_id": "C06",
        "workbench_id": config.workbench,
        "distinct_root_count": 2,
        "same_logical_shard": True,
        "peer_reconnect": "PASS",
        "wrong_agent_admission": "rejected-before-initialize",
    }


def assert_results(
    results: dict[str, dict[str, Any]],
    config: Config,
) -> dict[str, Any]:
    put, replacement = results["put-input"], results["replace-input"]
    if put.get("digest_uri") != "sha256:" + digest(scan_bytes("raw", 0)):
        raise WorkflowFailure("put digest differs from exact input bytes")
    if replacement.get("digest_uri") != "sha256:" + digest(scan_bytes("raw", 1)):
        raise WorkflowFailure("replace digest differs from exact replacement bytes")
    if replacement.get("replace") is not True or replacement.get(
        "generation"
    ) == put.get("generation"):
        raise WorkflowFailure("replace-only publication did not advance generation")
    read_generation = results["read-input"].get("generation")
    if (
        read_generation != replacement.get("generation")
        or results["stat-input"].get("card", {}).get("generation") != read_generation
    ):
        raise WorkflowFailure("put/read/stat generations differ")
    delta = b"ptychography reconstruction started\n"
    append = results["append-log"]
    if append.get("digest") != "sha256:" + digest(delta) or append.get(
        "appended_bytes"
    ) != len(delta):
        raise WorkflowFailure("append digest or byte count differs from its delta")
    if results["edit-input"].get("generation") == read_generation:
        raise WorkflowFailure("edit did not advance generation")

    commit, replay = results["commit"], results["commit-replay"]
    expected_content = "sha256:" + digest(reconstruction_bytes())
    if commit.get("content_digest_uri") != expected_content:
        raise WorkflowFailure("commit digest differs from collected output")
    if (
        commit.get("idempotent_replay") is not False
        or replay.get("idempotent_replay") is not True
        or commit.get("commit_identity") != replay.get("commit_identity")
    ):
        raise WorkflowFailure("commit exact replay did not converge")

    run_manifest = document(results["read-run-manifest"], "run manifest")
    if (
        run_manifest.get("schema") != "nokv.workbench.run_manifest.v1"
        or run_manifest.get("workbench_path")
        != f"{config.workbench_root}/{config.workbench}"
    ):
        raise WorkflowFailure("run manifest projection differs from v1")
    frozen = document(results["frozen-read"], "frozen read")
    live = document(results["live-read-after-snapshot"], "live read")
    restored = document(results["read-restored-input"], "restored read")
    if (
        frozen.get("state") != "calibrated"
        or live.get("state") != "post-snapshot"
        or restored != frozen
    ):
        raise WorkflowFailure("snapshot frozen/live/restore relationship differs")
    restore_manifest = document(results["read-restore-manifest"], "restore manifest")
    if (
        restore_manifest.get("schema") != "nokv.workbench.restore_manifest.v1"
        or restore_manifest.get("source_workbench_id") != config.workbench
        or restore_manifest.get("destination_workbench_id") != config.restored
    ):
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
        "reason": "workbench workflow complete",
    }
    if (
        retired.get("retired") is not True
        or retired.get("state") != "retired"
        or retired.get("retire_annotation") != expected_retire_annotation
    ):
        raise WorkflowFailure("snapshot did not retire")
    if (
        replayed_retire.get("retired") is not False
        or replayed_retire.get("retire_annotation") != expected_retire_annotation
    ):
        raise WorkflowFailure("snapshot retire replay replaced durable evidence")
    if not isinstance(retired_rows, list) or not any(
        row.get("snapshot_id") == minted.get("snapshot_id")
        and row.get("state") == "retired"
        for row in retired_rows
    ):
        raise WorkflowFailure("snapshot list did not retain the retired state")
    return assert_phase_one_results(results, config)


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
        raise NotQualified(
            "root and logical shard ids must be 32 lowercase hex characters"
        )
    if not HEX_ID.fullmatch(config.agent_id):
        raise NotQualified("AgentId must be 32 lowercase hex characters")
    for value in (config.agent_name, config.workbench, config.restored):
        if not WORKBENCH_ID.fullmatch(value):
            raise NotQualified(f"invalid Workbench identifier: {value}")
    if config.workbench == config.restored:
        raise NotQualified("source and destination Workbench ids must differ")
    phase_one_ids = set(phase_one_workbench_ids(config).values())
    if phase_one_ids.intersection((config.workbench, config.restored)):
        raise NotQualified(
            "Phase 1 fixture ids must differ from configured Workbench ids"
        )
    if not config.etcd or not config.bucket or not config.object_endpoint:
        raise NotQualified("etcd endpoint, object endpoint, and bucket are required")
    peer, mismatch = authority_configs(config)
    if (
        peer.root_id == config.root_id
        or peer.agent_id == config.agent_id
        or peer.shard_id != config.shard_id
        or mismatch.root_id != config.root_id
        or mismatch.agent_id != peer.agent_id
    ):
        raise NotQualified("authority probe identities do not form two isolated roots")
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
            raise NotQualified(
                f"dependency unreachable at {dependency}: {error}"
            ) from error


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


def require_running(label: str, process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        raise WorkflowFailure(
            f"{label} exited before live qualification ({process.returncode})"
        )


def start_mcp(
    config: Config,
    evidence: Evidence,
    server: subprocess.Popen[str],
    label: str = "mcp",
    attempt_offset: int = 0,
) -> tuple[Mcp, TextIO]:
    deadline, last_error, attempt = (
        time.monotonic() + config.timeout,
        "",
        attempt_offset,
    )
    while time.monotonic() < deadline:
        attempt += 1
        if server.poll() is not None:
            raise WorkflowFailure(f"serve exited during startup ({server.returncode})")
        stderr = (evidence.root / f"{label}.stderr.attempt-{attempt}.log").open("w")
        process = subprocess.Popen(
            mcp_command(config),
            cwd=config.repo,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=stderr,
            text=True,
            bufsize=1,
            start_new_session=True,
        )
        session = Mcp(process, evidence, attempt, min(config.timeout, 5))
        try:
            initialized = session.request(
                "initialize",
                {
                    "protocolVersion": PROTOCOL_VERSION,
                    "capabilities": {},
                    "clientInfo": {"name": "nokv-workbench-harness", "version": "1"},
                },
            )
            result = initialized.get("result", {})
            if result.get("serverInfo", {}).get("name") != "nokv-mcp" or result.get(
                "capabilities"
            ) != {"tools": {}}:
                raise WorkflowFailure("unexpected MCP initialize result")
            session.notify("notifications/initialized")
            evidence.line(
                "processes.jsonl",
                {
                    "schema": SCHEMA,
                    "label": label,
                    "argv": redact_argv(mcp_command(config)),
                    "pid": process.pid,
                    "attempt": attempt,
                    "started_at": now(),
                },
            )
            return session, stderr
        except WorkflowFailure as error:
            last_error = str(error)
            stop(process)
            stderr.close()
            time.sleep(0.25)
    raise WorkflowFailure(f"MCP startup failed: {last_error}")


def close_mcp(
    session: Mcp | None, stderr: TextIO | None, evidence: Evidence, label: str
) -> None:
    try:
        if session is not None:
            if session.process.stdin is not None:
                try:
                    session.process.stdin.close()
                except OSError as error:
                    evidence.line(
                        "processes.jsonl",
                        {
                            "schema": SCHEMA,
                            "label": f"{label}-stdin-close",
                            "finished_at": now(),
                            "error": str(error),
                        },
                    )
            evidence.line(
                "processes.jsonl",
                {
                    "schema": SCHEMA,
                    "label": f"{label}-exit",
                    "returncode": stop(session.process),
                    "finished_at": now(),
                },
            )
    finally:
        if stderr is not None:
            stderr.close()


def transfer(config: Config, evidence: Evidence) -> None:
    sandbox = evidence.root / "sandbox"
    sandbox.mkdir()
    materialized, collected = sandbox / "scan.json", sandbox / "reconstruction.json"
    completed_process(
        evidence, "materialize", materialize_command(config, materialized), config
    )
    if json.loads(materialized.read_text()).get("state") != "calibrated":
        raise WorkflowFailure("materialized input is not calibrated")
    collected.write_bytes(reconstruction_bytes())
    result = completed_process(
        evidence, "collect", collect_command(config, collected), config
    )
    if json.loads(result.stdout).get("status") != "success":
        raise WorkflowFailure("collect did not return success")


def grep_continuation_step(
    first_step: ToolStep, first_result: dict[str, Any]
) -> ToolStep:
    cursor = first_result.get("next_cursor")
    if (
        first_result.get("truncated") is not True
        or not isinstance(cursor, str)
        or not cursor
    ):
        raise WorkflowFailure("grep page one did not return a continuation cursor")
    arguments = dict(first_step.arguments)
    arguments["cursor"] = cursor
    return ToolStep("grep-phase1-page-2", first_step.name, arguments)


def run_authority_probe(
    config: Config,
    evidence: Evidence,
    server: subprocess.Popen[str],
    primary: Mcp,
) -> dict[str, Any]:
    peer, mismatch_config = authority_configs(config)
    peer_stderr: TextIO | None = None
    peer_session: Mcp | None = None
    reconnect_stderr: TextIO | None = None
    reconnect_session: Mcp | None = None
    results: dict[str, dict[str, Any]] = {}
    peer_payload = canonical_json({"authority": "peer"}) + "\n"
    try:
        peer_session, peer_stderr = start_mcp(
            peer, evidence, server, "mcp-authority-peer", 100
        )
        listed = peer_session.request("tools/list")
        tools = listed.get("result", {}).get("tools")
        if not isinstance(tools, list):
            raise WorkflowFailure("peer tools/list did not return a tools array")
        validate_tool_contract(tools)
        results["peer-read-before-create"] = peer_session.call(
            ToolStep(
                "peer-read-before-create",
                "workbench_read",
                {"id": config.workbench, "section": "input", "path": "scan.json"},
                "NotFound",
            )
        )
        results["peer-put"] = peer_session.call(
            ToolStep(
                "peer-put",
                "workbench_put_file",
                {
                    "id": config.workbench,
                    "section": "input",
                    "path": "scan.json",
                    "text": peer_payload,
                    "content_type": "application/json",
                    "replace": False,
                },
            )
        )
        results["peer-read"] = peer_session.call(
            ToolStep(
                "peer-read",
                "workbench_read",
                {"id": config.workbench, "section": "input", "path": "scan.json"},
            )
        )
    finally:
        close_mcp(peer_session, peer_stderr, evidence, "mcp-authority-peer")

    try:
        reconnect_session, reconnect_stderr = start_mcp(
            peer, evidence, server, "mcp-authority-reconnect", 200
        )
        results["peer-reconnect-read"] = reconnect_session.call(
            ToolStep(
                "peer-reconnect-read",
                "workbench_read",
                {"id": config.workbench, "section": "input", "path": "scan.json"},
            )
        )
    finally:
        close_mcp(
            reconnect_session, reconnect_stderr, evidence, "mcp-authority-reconnect"
        )

    results["primary-read-after-peer-write"] = primary.call(
        ToolStep(
            "primary-read-after-peer-write",
            "workbench_read",
            {"id": config.workbench, "section": "input", "path": "scan.json"},
        )
    )
    initialize = (
        canonical_json(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": PROTOCOL_VERSION,
                    "capabilities": {},
                    "clientInfo": {"name": "nokv-authority-negative", "version": "1"},
                },
            }
        )
        + "\n"
    )
    mismatch = expected_failure_process(
        evidence,
        "mcp-authority-mismatch",
        mcp_command(mismatch_config),
        mismatch_config,
        initialize,
    )
    return assert_authority_results(results, mismatch, config, peer)


def run_live(config: Config, evidence: Evidence, steps: list[ToolStep]) -> None:
    provision = completed_process(
        evidence, "provision", provision_command(config), config
    )
    if json.loads(provision.stdout).get("lifecycle") != "active":
        raise WorkflowFailure("provision did not activate root placement")
    peer, _ = authority_configs(config)
    peer_provision = completed_process(
        evidence, "provision-authority-peer", provision_command(peer), peer
    )
    if json.loads(peer_provision.stdout).get("lifecycle") != "active":
        raise WorkflowFailure("peer provision did not activate root placement")
    serve_out = (evidence.root / "serve.stdout.log").open("w")
    serve_err = (evidence.root / "serve.stderr.log").open("w")
    server = subprocess.Popen(
        server_command(config),
        cwd=config.repo,
        stdin=subprocess.DEVNULL,
        stdout=serve_out,
        stderr=serve_err,
        text=True,
        start_new_session=True,
    )
    evidence.line(
        "processes.jsonl",
        {
            "schema": SCHEMA,
            "label": "serve",
            "argv": redact_argv(server_command(config)),
            "pid": server.pid,
            "started_at": now(),
        },
    )
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
            if step.label == "grep-phase1-page-1":
                continuation = grep_continuation_step(step, results[step.label])
                results[continuation.label] = session.call(continuation)
            if step.label == "edit-input":
                transfer(config, evidence)
        phase_one_evidence = assert_results(results, config)
        authority_evidence = run_authority_probe(config, evidence, server, session)
        require_running("mcp", session.process)
        require_running("serve", server)
        evidence.json("phase1-contracts.json", phase_one_evidence)
        evidence.json("authority-contracts.json", authority_evidence)
    finally:
        try:
            close_mcp(session, mcp_err, evidence, "mcp")
        finally:
            try:
                evidence.line(
                    "processes.jsonl",
                    {
                        "schema": SCHEMA,
                        "label": "serve-exit",
                        "returncode": stop(server),
                        "finished_at": now(),
                    },
                )
            finally:
                serve_out.close()
                serve_err.close()


def shell_output(command: list[str], cwd: Path) -> str | None:
    try:
        result = subprocess.run(
            command, cwd=cwd, text=True, capture_output=True, timeout=10, check=False
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return result.stdout.strip() if result.returncode == 0 else None


def environment(config: Config) -> dict[str, Any]:
    binary = config.binary
    return {
        "schema": SCHEMA,
        "captured_at": now(),
        "git_commit": shell_output(["git", "rev-parse", "HEAD"], config.repo),
        "git_status_porcelain": shell_output(
            ["git", "status", "--porcelain=v1"], config.repo
        ),
        "rustc": shell_output(["rustc", "--version", "--verbose"], config.repo),
        "cargo": shell_output(["cargo", "--version"], config.repo),
        "python": sys.version,
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "binary": {
            "path": str(binary),
            "size_bytes": binary.stat().st_size,
            "sha256": digest_file(binary),
        },
        "config": {
            "root_id": config.root_id,
            "logical_shard_id": config.shard_id,
            "agent_name": config.agent_name,
            "agent_id": config.agent_id,
            "workbench_root": config.workbench_root,
            "workbench": config.workbench,
            "restored_workbench": config.restored,
            "etcd": config.etcd,
            "metadata": str(config.metadata),
            "metadata_mode": config.metadata_mode,
            "bind": config.bind,
            "object_endpoint": config.object_endpoint,
            "object_bucket": config.bucket,
            "object_root": config.object_root,
            "access_key_present": config.access_key is not None,
            "secret_key_present": config.secret_key is not None,
        },
    }


def plan(config: Config, steps: list[ToolStep]) -> dict[str, Any]:
    sandbox = config.evidence / "sandbox"
    coverage = planned_tool_coverage(steps)
    peer, mismatch = authority_configs(config)
    return {
        "schema": SCHEMA,
        "mode": "dry-run" if config.dry_run else "live",
        "commands": {
            "build": ["cargo", "build", "-p", "nokv", "--bin", "nokv"]
            if config.build
            else None,
            "provision": redact_argv(provision_command(config)),
            "provision_authority_peer": redact_argv(provision_command(peer)),
            "serve": redact_argv(server_command(config)),
            "mcp": redact_argv(mcp_command(config)),
            "mcp_authority_peer": redact_argv(mcp_command(peer)),
            "mcp_authority_mismatch": redact_argv(mcp_command(mismatch)),
            "materialize": redact_argv(
                materialize_command(config, sandbox / "scan.json")
            ),
            "collect": redact_argv(
                collect_command(config, sandbox / "reconstruction.json")
            ),
        },
        "tool_steps": [dataclasses.asdict(step) for step in steps],
        "dynamic_tool_steps": [
            {
                "label": "grep-phase1-page-2",
                "cursor_from": "grep-phase1-page-1.next_cursor",
            }
        ],
        "tool_coverage": {
            "expected": sorted(WORKBENCH_TOOLS),
            "planned": sorted(coverage),
            "count": len(coverage),
            "complete": coverage == WORKBENCH_TOOLS,
        },
    }


def qualification(
    state: str, reason: str, workflow: str, transcript: str | None = None
) -> dict[str, Any]:
    gate_zero = "FAIL" if state == "FAIL" else "NOT QUALIFIED"
    gate_reason = reason
    if workflow == "PASS":
        gate_reason = (
            "The live 18-tool workflow passed, but the one-day snapshot lease "
            "did not expire and reach reaped state; Gate 0 is partial evidence."
        )
    return {
        "schema": SCHEMA,
        "recorded_at": now(),
        "overall_status": state,
        "reason": reason,
        "workbench_workflow": {"status": workflow, "transcript_sha256": transcript},
        "acceptance_gates": {
            str(index): {
                "status": gate_zero if index == 0 else "NOT QUALIFIED",
                "reason": gate_reason
                if index == 0
                else "This Workbench harness does not qualify this gate.",
            }
            for index in range(9)
        },
    }


def parse_args(argv: list[str] | None = None) -> Config:
    repo = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--qualification-result", type=Path)
    parser.add_argument(
        "--nokv-bin",
        type=Path,
        default=Path(os.getenv("NOKV_LIVE_NOKV_BIN", repo / "target/debug/nokv")),
    )
    parser.add_argument("--evidence-dir", type=Path)
    parser.add_argument("--metadata-dir", type=Path)
    parser.add_argument(
        "--metadata-mode", choices=("create", "reopen"), default="create"
    )
    parser.add_argument("--root-id", default=os.getenv("NOKV_LIVE_ROOT_ID", "11" * 16))
    parser.add_argument(
        "--logical-shard-id", default=os.getenv("NOKV_LIVE_LOGICAL_SHARD_ID", "22" * 16)
    )
    parser.add_argument("--agent-name", default="research-agent")
    parser.add_argument(
        "--agent-id", default=os.getenv("NOKV_LIVE_AGENT_ID", "44" * 16)
    )
    parser.add_argument("--workbench-id", default="ptychography-run")
    parser.add_argument("--restored-workbench-id", default="ptychography-run-restored")
    parser.add_argument("--snapshot-name", default="ptychography-frozen")
    parser.add_argument("--etcd-endpoint", action="append")
    parser.add_argument("--etcd-key-prefix", default="/nokv/control")
    parser.add_argument("--server-bind", default="127.0.0.1:7750")
    parser.add_argument("--advertise-endpoint", default="127.0.0.1:7750")
    parser.add_argument("--node-id")
    parser.add_argument("--object-bucket", default=os.getenv("NOKV_LIVE_S3_BUCKET", ""))
    parser.add_argument(
        "--object-endpoint", default=os.getenv("NOKV_LIVE_S3_ENDPOINT", "")
    )
    parser.add_argument("--object-root", default="workbench-live")
    parser.add_argument("--object-region", default="us-east-1")
    parser.add_argument(
        "--object-access-key-id", default=os.getenv("NOKV_LIVE_S3_ACCESS_KEY_ID")
    )
    parser.add_argument(
        "--object-secret-access-key",
        default=os.getenv("NOKV_LIVE_S3_SECRET_ACCESS_KEY"),
    )
    parser.add_argument("--command-timeout-seconds", type=float, default=30)
    args = parser.parse_args(argv)
    etcd = args.etcd_endpoint or [
        item for item in os.getenv("NOKV_LIVE_ETCD_ENDPOINTS", "").split(",") if item
    ]
    if args.dry_run:
        etcd = etcd or ["http://127.0.0.1:2379"]
        args.object_bucket = args.object_bucket or "nokv-live-dry-run"
        args.object_endpoint = args.object_endpoint or "http://127.0.0.1:9000"
    evidence = args.evidence_dir or (
        repo
        / "target/workbench-live/evidence"
        / f"gate0-{args.agent_name}-{args.workbench_id}-{args.root_id[:8]}"
    )
    metadata = args.metadata_dir or (
        repo / "target/workbench-live/metadata" / f"{args.root_id}-{args.metadata_mode}"
    )
    if args.qualification_result is not None and args.dry_run:
        parser.error("--qualification-result cannot be combined with --dry-run")
    return Config(
        repo,
        args.nokv_bin.resolve(),
        evidence.resolve(),
        metadata.resolve(),
        args.metadata_mode,
        args.root_id,
        args.logical_shard_id,
        args.agent_name,
        args.agent_id,
        args.workbench_id,
        args.restored_workbench_id,
        args.snapshot_name,
        tuple(etcd),
        args.etcd_key_prefix,
        args.server_bind,
        args.advertise_endpoint,
        args.node_id or f"workbench-{args.root_id[:8]}",
        args.object_bucket,
        args.object_endpoint,
        args.object_root,
        args.object_region,
        args.object_access_key_id,
        args.object_secret_access_key,
        args.build,
        args.dry_run,
        args.command_timeout_seconds,
        args.qualification_result.resolve() if args.qualification_result else None,
    )


def main(argv: list[str] | None = None) -> int:
    config = parse_args(argv)
    evidence, steps, prepared = Evidence(config.evidence), tool_plan(config), False
    typed_context = None

    def finish(code: int, record: dict[str, Any]) -> int:
        if typed_context is None or config.qualification_result is None:
            return code
        outcome = "PASS" if code == 0 else "NQ" if code == 3 else "FAIL"
        transcript_path = evidence.root / "mcp-transcript.jsonl"
        transcript = transcript_path.read_bytes() if transcript_path.is_file() else None
        try:
            publish_live_result(
                result_path=config.qualification_result,
                context=typed_context,
                outcome=outcome,
                qualification=record,
                evidence_roles=TYPED_EVIDENCE_ROLES,
                transcript=transcript,
            )
        except (OSError, ProducerError) as error:
            print(f"FAIL: {error}", file=sys.stderr)
            return 2
        return code

    try:
        if config.qualification_result is not None:
            if config.evidence == config.qualification_result.parent:
                raise ProducerError(
                    "live workflow evidence must not overlap typed direct-child evidence"
                )
            typed_context = load_live_context(
                producer_id="live-workbench",
                scenarios=TYPED_SCENARIOS,
                dependency_names=("etcd", "object-store"),
                product_binary=config.binary,
                evidence_roles=TYPED_EVIDENCE_ROLES,
            )
            unsupported = sorted(
                set(typed_context.scenarios).intersection(TYPED_UNSUPPORTED_SCENARIOS)
            )
            if unsupported:
                reason = (
                    "The live 18-tool harness does not execute the generic seven-tool "
                    "profile, direct RootId WorkspaceClient, or current operational CLI "
                    f"surfaces required by {unsupported}."
                )
                record = gap_record(producer="live-workbench", reason=reason)
                print(json.dumps(record, indent=2, sort_keys=True))
                return finish(3, record)
        evidence.prepare()
        prepared = True
        evidence.json("plan.json", plan(config, steps))
        if planned_tool_coverage(steps) != WORKBENCH_TOOLS:
            raise WorkflowFailure("tool plan does not cover exactly 18 tools")
        validate(config, live=False)
        if config.dry_run:
            record = qualification(
                "NOT QUALIFIED",
                "Dry-run validated commands and exact 18-tool coverage; "
                "no live dependency ran.",
                "NOT QUALIFIED",
            )
            evidence.json("qualification.json", record)
            print(json.dumps(record, indent=2, sort_keys=True))
            return finish(0, record)
        if config.build:
            if shutil.which("cargo") is None:
                raise NotQualified("cargo is unavailable for --build")
            old_timeout = config.timeout
            config = dataclasses.replace(config, timeout=max(old_timeout, 900))
            completed_process(
                evidence,
                "build",
                ["cargo", "build", "-p", "nokv", "--bin", "nokv"],
                config,
            )
            config = dataclasses.replace(config, timeout=old_timeout)
        validate(config, live=True)
        evidence.json("environment.json", environment(config))
        run_live(config, evidence, steps)
        transcript = digest_file(evidence.root / "mcp-transcript.jsonl")
        record = qualification(
            "NOT QUALIFIED",
            "Live Workbench workflow passed; full system acceptance "
            "requires the remaining gates.",
            "PASS",
            transcript,
        )
        evidence.json("qualification.json", record)
        print(json.dumps(record, indent=2, sort_keys=True))
        return finish(0, record)
    except NotQualified as error:
        record = qualification("NOT QUALIFIED", str(error), "NOT QUALIFIED")
        if prepared:
            evidence.json("qualification.json", record)
        print(json.dumps(record, indent=2, sort_keys=True))
        return finish(3, record)
    except (
        WorkflowFailure,
        OSError,
        ProducerError,
        ValueError,
        json.JSONDecodeError,
    ) as error:
        record = qualification("FAIL", str(error), "FAIL")
        if prepared:
            evidence.json("qualification.json", record)
        print(json.dumps(record, indent=2, sort_keys=True))
        return finish(2, record)


if __name__ == "__main__":
    raise SystemExit(main())
