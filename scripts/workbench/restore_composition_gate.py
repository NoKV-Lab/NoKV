#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Real etcd/RustFS gate for the pre-#423 Workbench restore composition.

This gate deliberately tests public Workbench behavior rather than the removed
inode/dentry layout.  Its oracle is the externally observable chain from
``98cac201:scripts/lingtai-workbench/durable_restore_live_e2e.py``:

    commit A -> snapshot A -> mutate A -> restore B -> mutate B without a
    recommit -> snapshot B -> restore C -> retire snapshot B -> read C

The 18-tool surface is driven through one flat ``nokv mcp`` process.  Atomic
rename and removal are driven through the separate, Workbench-scoped
``workspace-path`` CLI so the gate neither expands MCP nor recreates POSIX
absolute-path semantics.
"""

from __future__ import annotations

import argparse
import base64
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
from pathlib import Path
from typing import Any, Iterable, Sequence, TextIO


SCHEMA = "nokv.restore_composition_gate.v1"
PROTOCOL_VERSION = "2025-11-25"
PRE423_ORACLE_REVISION = "98cac201"
PINNED_RUSTFS_IMAGE = (
    "rustfs/rustfs@sha256:"
    "e620d37756fff072b10bf648c7bb9d370d7e91a928b7e6a5e1ac85bdfb4e4dab"
)
EXCLUDED_SURFACES = frozenset(
    {"FUSE", "POSIX", "Yanex", "inode", "dentry", "physical layout"}
)
SECRET_FLAGS = {"--object-secret-access-key"}
HEX_32 = re.compile(r"^[0-9a-f]{32}$")
MANDATORY_LABELS = (
    "create-a",
    "put-a-rename-source",
    "put-a-delete-source",
    "put-a-payload",
    "commit-a",
    "snapshot-a",
    "mutate-a-after-snapshot",
    "restore-b",
    "find-b-committed",
    "rename-b",
    "remove-b",
    "publish-b",
    "snapshot-b-no-recommit",
    "restore-c",
    "find-c-committed",
    "mutate-b-after-snapshot",
    "retire-snapshot-b",
    "read-c-after-retire",
    "restore-c-terminal-replay",
)


class NotQualified(RuntimeError):
    """The requested live boundary could not be exercised."""


class WorkflowFailure(RuntimeError):
    """A live or recorded contract invariant was violated."""


@dataclasses.dataclass(frozen=True)
class Config:
    repo: Path
    binary: Path
    evidence: Path
    target: Path
    etcd: Path | None
    etcdctl: Path | None
    docker: Path | None
    aws: Path | None
    rustfs_image: str
    seed: str
    build: bool
    dry_run: bool
    timeout: float
    keep_resources: bool


@dataclasses.dataclass(frozen=True)
class OracleStep:
    label: str
    surface: str
    operation: str
    arguments: dict[str, Any]


@dataclasses.dataclass(frozen=True)
class Runtime:
    root_id: str
    agent_id: str
    shard_id: str
    etcd_endpoint: str
    etcd_prefix: str
    object_endpoint: str
    object_bucket: str
    object_root: str
    access_key: str
    secret_key: str
    owner_port: int
    metadata: Path

    @property
    def workbench_root(self) -> str:
        return "/agents/composition/wb"


def now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def digest_uri(data: bytes) -> str:
    return "sha256:" + sha256(data)


def digest_file(path: Path) -> str:
    state = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            state.update(chunk)
    return state.hexdigest()


def fixed_id(seed: str, label: str) -> str:
    return sha256(f"{seed}:{label}".encode())[:32]


def initial_payload() -> bytes:
    return b"pre423 restore composition payload\n"


def renamed_payload() -> bytes:
    return b"rename survives nested restore\n"


def deleted_payload() -> bytes:
    return b"this path must be absent in B and C\n"


def published_payload() -> bytes:
    return b"published after restore B without recommit\n"


def post_snapshot_payload() -> bytes:
    return b"A changed after snapshot A\n"


def post_nested_snapshot_payload() -> bytes:
    return b"B changed after snapshot B\n"


def commit_content_digest() -> str:
    payload = b"\0".join((initial_payload(), renamed_payload(), deleted_payload()))
    return digest_uri(payload)


def oracle_plan(config: Config) -> list[OracleStep]:
    del config
    return [
        OracleStep("create-a", "mcp", "workbench_create", {"id": "composition-a"}),
        OracleStep(
            "put-a-rename-source",
            "mcp",
            "workbench_put_file",
            {
                "id": "composition-a",
                "section": "outputs",
                "path": "rename-source.txt",
                "text": renamed_payload().decode(),
                "content_type": "text/plain",
                "replace": False,
            },
        ),
        OracleStep(
            "put-a-delete-source",
            "mcp",
            "workbench_put_file",
            {
                "id": "composition-a",
                "section": "outputs",
                "path": "delete-source.txt",
                "text": deleted_payload().decode(),
                "content_type": "text/plain",
                "replace": False,
            },
        ),
        OracleStep(
            "put-a-payload",
            "mcp",
            "workbench_put_file",
            {
                "id": "composition-a",
                "section": "outputs",
                "path": "payload.txt",
                "text": initial_payload().decode(),
                "content_type": "text/plain",
                "replace": False,
            },
        ),
        OracleStep(
            "commit-a",
            "mcp",
            "workbench_commit",
            {
                "id": "composition-a",
                "manifest": {
                    "task": "restore-composition",
                    "oracle_revision": PRE423_ORACLE_REVISION,
                },
                "content_digest_uri": commit_content_digest(),
                "replace": False,
            },
        ),
        OracleStep(
            "snapshot-a",
            "mcp",
            "workbench_snapshot",
            {
                "id": "composition-a",
                "name": "composition-a-frozen",
                "ttl_days": 1,
                "reason": "pre423 restore composition A",
            },
        ),
        OracleStep(
            "mutate-a-after-snapshot",
            "mcp",
            "workbench_put_file",
            {
                "id": "composition-a",
                "section": "outputs",
                "path": "payload.txt",
                "text": post_snapshot_payload().decode(),
                "content_type": "text/plain",
                "replace": True,
            },
        ),
        OracleStep(
            "restore-b",
            "mcp",
            "workbench_restore",
            {
                "id": "composition-a",
                "at_snapshot": "composition-a-frozen",
                "destination_id": "composition-b",
            },
        ),
        OracleStep(
            "find-b-committed",
            "mcp",
            "workbench_find",
            {"committed": True, "include_manifest": True, "limit": 100},
        ),
        OracleStep(
            "rename-b",
            "workspace-path",
            "rename",
            {
                "workbench": "composition-b",
                "section": "outputs",
                "path": "rename-source.txt",
                "destination_path": "renamed.txt",
                "expected_generation": 1,
            },
        ),
        OracleStep(
            "remove-b",
            "workspace-path",
            "remove",
            {
                "workbench": "composition-b",
                "section": "outputs",
                "path": "delete-source.txt",
                "expected_generation": 1,
            },
        ),
        OracleStep(
            "publish-b",
            "mcp",
            "workbench_put_file",
            {
                "id": "composition-b",
                "section": "outputs",
                "path": "published.txt",
                "text": published_payload().decode(),
                "content_type": "text/plain",
                "replace": False,
            },
        ),
        OracleStep(
            "snapshot-b-no-recommit",
            "mcp",
            "workbench_snapshot",
            {
                "id": "composition-b",
                "name": "composition-b-dirty",
                "ttl_days": 1,
                "reason": "dirty restored destination without recommit",
            },
        ),
        OracleStep(
            "restore-c",
            "mcp",
            "workbench_restore",
            {
                "id": "composition-b",
                "at_snapshot": "composition-b-dirty",
                "destination_id": "composition-c",
            },
        ),
        OracleStep(
            "find-c-committed",
            "mcp",
            "workbench_find",
            {"committed": True, "include_manifest": True, "limit": 100},
        ),
        OracleStep(
            "mutate-b-after-snapshot",
            "mcp",
            "workbench_put_file",
            {
                "id": "composition-b",
                "section": "outputs",
                "path": "published.txt",
                "text": post_nested_snapshot_payload().decode(),
                "content_type": "text/plain",
                "replace": True,
            },
        ),
        OracleStep(
            "retire-snapshot-b",
            "mcp",
            "workbench_snapshot_retire",
            {
                "id": "composition-b",
                "name": "composition-b-dirty",
                "reason": "composition C owns child retention",
            },
        ),
        OracleStep(
            "read-c-after-retire",
            "mcp",
            "workbench_read",
            {
                "id": "composition-c",
                "section": "outputs",
                "path": "published.txt",
                "format": "bytes",
                "limit": 300,
            },
        ),
        OracleStep(
            "restore-c-terminal-replay",
            "mcp",
            "workbench_restore",
            {
                "id": "composition-b",
                "at_snapshot": "composition-b-dirty",
                "destination_id": "composition-c",
            },
        ),
    ]


def mutation_request_id(
    config: Config,
    operation: str,
    workbench: str,
    section: str,
    path: str,
    destination_path: str | None,
    expected_generation: int,
) -> str:
    fields = {
        "seed": config.seed,
        "operation": operation,
        "workbench": workbench,
        "section": section,
        "path": path,
        "destination_path": destination_path,
        "expected_generation": expected_generation,
    }
    return sha256(
        b"nokv.restore-composition.mutation.v1\0" + canonical_json(fields).encode()
    )[:32]


def mutation_command(
    config: Config,
    operation: str,
    *,
    workbench: str,
    section: str,
    path: str,
    expected_generation: int,
    destination_path: str | None = None,
    client_prefix: Sequence[str] | None = None,
) -> list[str]:
    if operation not in {"rename", "remove"}:
        raise WorkflowFailure(f"unsupported workspace-path mutation: {operation}")
    if operation == "rename" and not destination_path:
        raise WorkflowFailure("workspace-path rename requires destination_path")
    if operation == "remove" and destination_path is not None:
        raise WorkflowFailure("workspace-path remove forbids destination_path")
    request_id = mutation_request_id(
        config,
        operation,
        workbench,
        section,
        path,
        destination_path,
        expected_generation,
    )
    command = [
        *(client_prefix or (str(config.binary),)),
        "workspace-path",
        operation,
        workbench,
        section,
        path,
    ]
    if destination_path is not None:
        command.append(destination_path)
    return [
        *command,
        "--expected-generation",
        str(expected_generation),
        "--request-id",
        request_id,
    ]


def _mapping(parent: dict[str, Any], key: str) -> dict[str, Any]:
    value = parent.get(key)
    if not isinstance(value, dict):
        raise WorkflowFailure(f"composition evidence lacks object {key}")
    return value


def _string(parent: dict[str, Any], key: str, label: str) -> str:
    value = parent.get(key)
    if not isinstance(value, str) or not value:
        raise WorkflowFailure(f"{label}.{key} must be a non-empty string")
    return value


def _validate_run_manifest(
    manifest_value: dict[str, Any], workbench: str, label: str
) -> tuple[str, str]:
    if (
        manifest_value.get("schema") != "nokv.workbench.run_manifest.v1"
        or manifest_value.get("workbench_id") != workbench
        or manifest_value.get("workbench_path") != f"/agents/composition/wb/{workbench}"
    ):
        raise WorkflowFailure(f"{label} is not destination-owned")
    identity = _string(manifest_value, "commit_identity", label)
    content = _string(manifest_value, "content_digest_uri", label)
    if not re.fullmatch(r"[0-9a-f]{64}", identity):
        raise WorkflowFailure(f"{label}.commit_identity is not canonical")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", content):
        raise WorkflowFailure(f"{label}.content_digest_uri is not canonical")
    return identity, content


def validate_composition_evidence(evidence: dict[str, Any]) -> None:
    if evidence.get("schema") != SCHEMA or evidence.get("status") != "PASS":
        raise WorkflowFailure("composition terminal evidence is not PASS v1")
    if evidence.get("oracle_revision") != PRE423_ORACLE_REVISION:
        raise WorkflowFailure("composition evidence does not bind the pre-#423 oracle")
    exclusions = evidence.get("excluded_surfaces")
    if not isinstance(exclusions, list) or set(exclusions) != EXCLUDED_SURFACES:
        raise WorkflowFailure("composition excluded surfaces are incomplete")

    labels = evidence.get("call_labels")
    if not isinstance(labels, list) or not all(
        isinstance(label, str) for label in labels
    ):
        raise WorkflowFailure("composition call_labels are malformed")
    restore_b = labels.index("restore-b") if "restore-b" in labels else -1
    snapshot_b = (
        labels.index("snapshot-b-no-recommit")
        if "snapshot-b-no-recommit" in labels
        else -1
    )
    if restore_b < 0 or snapshot_b <= restore_b:
        raise WorkflowFailure("composition B restore/snapshot ordering is absent")
    if any(label == "commit-b" for label in labels[restore_b + 1 : snapshot_b]):
        raise WorkflowFailure("composition improperly inserted a B recommit")
    if labels != list(MANDATORY_LABELS):
        raise WorkflowFailure(
            "composition command graph differs from the frozen oracle"
        )

    workbenches = _mapping(evidence, "workbenches")
    a, b, c = (_mapping(workbenches, key) for key in ("a", "b", "c"))
    ids = [
        _string(value, "id", label) for value, label in ((a, "A"), (b, "B"), (c, "C"))
    ]
    if ids != ["composition-a", "composition-b", "composition-c"] or len(set(ids)) != 3:
        raise WorkflowFailure("A/B/C logical identities are not independent")
    if a.get("commit_generation") != 1:
        raise WorkflowFailure("A commit head is not generation 1")
    if b.get("destination_generation") != 1 or c.get("destination_generation") != 1:
        raise WorkflowFailure("restored destination head is not generation 1")
    if any(value.get("committed") is not True for value in (a, b, c)):
        raise WorkflowFailure("A/B/C are not all immediately discoverable as committed")

    a_identity, a_content = _validate_run_manifest(
        _mapping(a, "run_manifest"), ids[0], "A run manifest"
    )
    b_identity, b_content = _validate_run_manifest(
        _mapping(b, "run_manifest"), ids[1], "B run manifest"
    )
    c_identity, c_content = _validate_run_manifest(
        _mapping(c, "run_manifest"), ids[2], "C run manifest"
    )
    if len({a_identity, b_identity, c_identity}) != 3:
        raise WorkflowFailure(
            "destination commit identities are not independently owned"
        )
    if b_content != a_content:
        raise WorkflowFailure("clean restore B did not preserve A's content digest")
    if c_content == b_content:
        raise WorkflowFailure("dirty B snapshot reused B's clean content digest")

    for value, source, destination, label in (
        (b, ids[0], ids[1], "B"),
        (c, ids[1], ids[2], "C"),
    ):
        restore_manifest = _mapping(value, "restore_manifest")
        if (
            restore_manifest.get("schema") != "nokv.workbench.restore_manifest.v1"
            or restore_manifest.get("source_workbench_id") != source
            or restore_manifest.get("destination_workbench_id") != destination
        ):
            if label == "C":
                raise WorkflowFailure(
                    "C restore manifest does not identify B as its source"
                )
            raise WorkflowFailure(
                "B restore manifest does not identify A as its source"
            )
        if value.get("destination_owned_manifest_objects") != 2:
            raise WorkflowFailure(
                f"{label} restore did not publish exactly two manifests"
            )
        if value.get("old_rename_path_absent") is not True:
            raise WorkflowFailure(f"{label} retained the old renamed path")
        if value.get("deleted_path_absent") is not True:
            raise WorkflowFailure(f"{label} retained the deleted path")
        for digest_field in ("renamed_bytes_sha256", "published_bytes_sha256"):
            if not re.fullmatch(r"[0-9a-f]{64}", str(value.get(digest_field, ""))):
                raise WorkflowFailure(f"{label}.{digest_field} is missing")
    if (
        b["renamed_bytes_sha256"] != c["renamed_bytes_sha256"]
        or b["published_bytes_sha256"] != c["published_bytes_sha256"]
    ):
        raise WorkflowFailure("C bytes differ from the dirty B snapshot")
    if c.get("readable_after_snapshot_retire") is not True:
        raise WorkflowFailure("snapshot retirement broke C child retention")

    snapshots = _mapping(evidence, "snapshots")
    snapshot_a, snapshot_b_record = (_mapping(snapshots, key) for key in ("a", "b"))
    if snapshot_a.get("source_workbench_id") != ids[0]:
        raise WorkflowFailure("snapshot A is not bound to A")
    if (
        snapshot_b_record.get("source_workbench_id") != ids[1]
        or snapshot_b_record.get("minted_without_recommit") is not True
        or snapshot_b_record.get("retired") is not True
    ):
        raise WorkflowFailure("dirty snapshot B lifecycle is incomplete")

    independence = _mapping(evidence, "independence")
    if any(
        independence.get(field) is not True
        for field in (
            "a_post_snapshot_bytes_excluded_from_b",
            "b_post_snapshot_bytes_excluded_from_c",
            "a_b_c_distinct",
        )
    ):
        raise WorkflowFailure("A/B/C content independence was not proven")
    replay = _mapping(evidence, "terminal_replay")
    if any(
        replay.get(field) is not True
        for field in (
            "operation_id_stable",
            "destination_commit_identity_stable",
            "idempotent_replay",
        )
    ):
        raise WorkflowFailure("terminal restore replay did not converge uniquely")
    fault = _mapping(evidence, "fault_injection")
    if fault.get("status") not in {"PASS", "NOT QUALIFIED"}:
        raise WorkflowFailure(
            "partial-publication recovery is neither PASS nor NOT QUALIFIED"
        )
    if fault.get("status") == "NOT QUALIFIED" and not fault.get("reason"):
        raise WorkflowFailure("NOT QUALIFIED fault evidence lacks a reason")


def qualification(
    *, composition: str, fault: str, reason: str, transcript_sha256: str | None = None
) -> dict[str, Any]:
    if composition == "FAIL" or fault == "FAIL":
        overall = "FAIL"
    elif composition == "PASS":
        overall = "PASS"
    else:
        overall = "NOT QUALIFIED"
    return {
        "schema": SCHEMA,
        "recorded_at": now(),
        "overall_status": overall,
        "reason": reason,
        "restore_composition": {
            "status": composition,
            "transcript_sha256": transcript_sha256,
        },
        "partial_publication_recovery": {
            "status": fault,
            "reason": reason if fault != "PASS" else "Qualified exact crash boundary.",
        },
    }


class Evidence:
    def __init__(self, root: Path) -> None:
        self.root = root

    def prepare(self) -> None:
        if self.root.exists() and any(self.root.iterdir()):
            raise NotQualified(f"evidence directory must be empty: {self.root}")
        self.root.mkdir(parents=True, exist_ok=True)

    def json(self, name: str, value: Any) -> None:
        (self.root / name).write_text(
            json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )

    def line(self, name: str, value: Any) -> None:
        with (self.root / name).open("a", encoding="utf-8") as output:
            output.write(canonical_json(value) + "\n")


def redact_argv(argv: Iterable[os.PathLike[str] | str]) -> list[str]:
    output: list[str] = []
    redact_next = False
    for raw in argv:
        argument = str(raw)
        if redact_next:
            output.append("<redacted>")
            redact_next = False
        elif argument.startswith("RUSTFS_SECRET_KEY="):
            output.append("RUSTFS_SECRET_KEY=<redacted>")
        else:
            output.append(argument)
            redact_next = argument in SECRET_FLAGS
    return output


def run(
    argv: Sequence[os.PathLike[str] | str],
    *,
    config: Config,
    evidence: Evidence | None = None,
    label: str | None = None,
    check: bool = True,
    env: dict[str, str] | None = None,
    timeout: float | None = None,
) -> subprocess.CompletedProcess[str]:
    started = now()
    try:
        result = subprocess.run(
            [str(item) for item in argv],
            cwd=config.repo,
            text=True,
            capture_output=True,
            check=False,
            timeout=timeout or config.timeout,
            env=env,
        )
    except subprocess.TimeoutExpired as error:
        if evidence is not None:
            evidence.line(
                "processes.jsonl",
                {
                    "schema": SCHEMA,
                    "label": label,
                    "argv": redact_argv(argv),
                    "started_at": started,
                    "finished_at": now(),
                    "status": "timed_out",
                },
            )
        raise WorkflowFailure(f"{label or 'command'} timed out") from error
    if evidence is not None:
        evidence.line(
            "processes.jsonl",
            {
                "schema": SCHEMA,
                "label": label,
                "argv": redact_argv(argv),
                "started_at": started,
                "finished_at": now(),
                "returncode": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
            },
        )
    if check and result.returncode != 0:
        detail = (result.stderr or result.stdout).strip() or "no output"
        raise WorkflowFailure(
            f"{label or 'command'} failed ({result.returncode}): {detail}"
        )
    return result


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def start_process(
    argv: Sequence[os.PathLike[str] | str], config: Config, log: TextIO
) -> subprocess.Popen[str]:
    return subprocess.Popen(
        [str(item) for item in argv],
        cwd=config.repo,
        stdin=subprocess.DEVNULL,
        stdout=log,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )


def stop_process(
    process: subprocess.Popen[str] | None, sig: signal.Signals = signal.SIGTERM
) -> int | None:
    if process is None or process.poll() is not None:
        return None if process is None else process.returncode
    try:
        os.killpg(process.pid, sig)
        process.wait(timeout=10)
    except (ProcessLookupError, subprocess.TimeoutExpired):
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=10)
    return process.returncode


def wait_tcp(process: subprocess.Popen[str], port: int, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise WorkflowFailure(
                f"owner exited before readiness ({process.returncode})"
            )
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise WorkflowFailure(f"owner did not listen on 127.0.0.1:{port}")


def aws_environment(access_key: str, secret_key: str) -> dict[str, str]:
    environment = os.environ.copy()
    environment.update(
        {
            "AWS_ACCESS_KEY_ID": access_key,
            "AWS_SECRET_ACCESS_KEY": secret_key,
            "AWS_DEFAULT_REGION": "us-east-1",
            "AWS_EC2_METADATA_DISABLED": "true",
            "AWS_MAX_ATTEMPTS": "1",
        }
    )
    return environment


def aws_command(aws: Path, endpoint: str, *arguments: str) -> list[str]:
    return [
        str(aws),
        "--cli-connect-timeout",
        "1",
        "--cli-read-timeout",
        "2",
        "--endpoint-url",
        endpoint,
        *arguments,
    ]


def rustfs_container_command(
    docker: Path,
    *,
    container: str,
    volume: str,
    s3_port: int,
    console_port: int,
    image: str,
    access_key: str,
    secret_key: str,
) -> list[str]:
    return [
        str(docker),
        "run",
        "-d",
        "--name",
        container,
        "-p",
        f"127.0.0.1:{s3_port}:9000",
        "-p",
        f"127.0.0.1:{console_port}:9001",
        "-e",
        f"RUSTFS_ACCESS_KEY={access_key}",
        "-e",
        f"RUSTFS_SECRET_KEY={secret_key}",
        "-e",
        "RUSTFS_CONSOLE_ENABLE=true",
        "--mount",
        f"type=volume,source={volume},target=/data",
        image,
        "--address",
        ":9000",
        "--console-enable",
        "/data",
    ]


def control_args(config: Config, runtime: Runtime) -> list[str]:
    return [
        str(config.binary),
        "--root-id",
        runtime.root_id,
        "--etcd-endpoint",
        runtime.etcd_endpoint,
        "--etcd-key-prefix",
        runtime.etcd_prefix,
        "--etcd-lease-ttl-seconds",
        "2",
    ]


def object_args(runtime: Runtime) -> list[str]:
    return [
        "--object-bucket",
        runtime.object_bucket,
        "--object-endpoint",
        runtime.object_endpoint,
        "--object-root",
        runtime.object_root,
        "--object-region",
        "us-east-1",
        "--object-access-key-id",
        runtime.access_key,
        "--object-secret-access-key",
        runtime.secret_key,
    ]


def client_args(config: Config, runtime: Runtime) -> list[str]:
    return [
        *control_args(config, runtime),
        "--agent-id",
        runtime.agent_id,
        "--workbench-root",
        runtime.workbench_root,
        *object_args(runtime),
    ]


def owner_command(config: Config, runtime: Runtime) -> list[str]:
    return [
        *control_args(config, runtime),
        *object_args(runtime),
        "--bind",
        f"127.0.0.1:{runtime.owner_port}",
        "--advertise-endpoint",
        f"127.0.0.1:{runtime.owner_port}",
        "--node-id",
        "restore-composition-owner",
        "--metadata-create",
        str(runtime.metadata),
        "--lifecycle-interval-millis",
        "100",
        "serve",
    ]


class Mcp:
    def __init__(
        self,
        process: subprocess.Popen[str],
        evidence: Evidence,
        timeout: float,
    ) -> None:
        self.process = process
        self.evidence = evidence
        self.timeout = timeout
        self.next_id = 1

    def request(
        self, method: str, params: dict[str, Any] | None = None, *, label: str
    ) -> dict[str, Any]:
        request_id = self.next_id
        self.next_id += 1
        request: dict[str, Any] = {"jsonrpc": "2.0", "id": request_id, "method": method}
        if params is not None:
            request["params"] = params
        encoded = canonical_json(request)
        if self.process.stdin is None or self.process.stdout is None:
            raise WorkflowFailure("MCP stdio pipes are unavailable")
        self.process.stdin.write(encoded + "\n")
        self.process.stdin.flush()
        selector = selectors.DefaultSelector()
        selector.register(self.process.stdout, selectors.EVENT_READ)
        try:
            if not selector.select(self.timeout):
                raise WorkflowFailure(f"{label} MCP response timed out")
            raw = self.process.stdout.readline().rstrip("\n")
        finally:
            selector.close()
        if not raw:
            raise WorkflowFailure(f"{label} MCP process exited before responding")
        try:
            response = json.loads(raw)
        except json.JSONDecodeError as error:
            raise WorkflowFailure(f"{label} MCP response is not JSON") from error
        self.evidence.line(
            "mcp-transcript.jsonl",
            {
                "schema": SCHEMA,
                "label": label,
                "recorded_at": now(),
                "request_raw": encoded,
                "request": request,
                "response_raw": raw,
                "response": response,
            },
        )
        if response.get("jsonrpc") != "2.0" or response.get("id") != request_id:
            raise WorkflowFailure(f"{label} MCP response envelope differs")
        return response

    def notify(self, method: str) -> None:
        if self.process.stdin is None:
            raise WorkflowFailure("MCP stdin is unavailable")
        self.process.stdin.write(
            canonical_json({"jsonrpc": "2.0", "method": method}) + "\n"
        )
        self.process.stdin.flush()

    def initialize(self) -> None:
        response = self.request(
            "initialize",
            {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {"name": "restore-composition-gate", "version": "1"},
            },
            label="initialize",
        )
        result = response.get("result")
        if (
            not isinstance(result, dict)
            or result.get("serverInfo", {}).get("name") != "nokv-mcp"
            or result.get("capabilities") != {"tools": {}}
        ):
            raise WorkflowFailure("MCP initialize result differs")
        self.notify("notifications/initialized")

    def call(
        self,
        label: str,
        name: str,
        arguments: dict[str, Any],
        *,
        expected_error: str | None = None,
    ) -> dict[str, Any]:
        response = self.request(
            "tools/call", {"name": name, "arguments": arguments}, label=label
        )
        result = response.get("result")
        structured = (
            result.get("structuredContent") if isinstance(result, dict) else None
        )
        if not isinstance(structured, dict):
            raise WorkflowFailure(f"{label} lacks structuredContent")
        try:
            text_value = json.loads(result["content"][0]["text"])
        except (IndexError, KeyError, TypeError, json.JSONDecodeError) as error:
            raise WorkflowFailure(
                f"{label} lacks matching JSON text content"
            ) from error
        if text_value != structured:
            raise WorkflowFailure(f"{label} text and structured results differ")
        is_error = result.get("isError") is True
        if expected_error is not None:
            if not is_error or structured.get("code") != expected_error:
                raise WorkflowFailure(f"{label} did not return {expected_error}")
        elif is_error or structured.get("status") != "success":
            raise WorkflowFailure(f"{label} failed: {structured!r}")
        return structured


def decode_bytes(result: dict[str, Any], label: str) -> bytes:
    if (
        result.get("format") != "bytes"
        or result.get("bytes_encoding") != "base64"
        or result.get("truncated") is not False
        or result.get("next_cursor") is not None
        or not isinstance(result.get("bytes"), str)
    ):
        raise WorkflowFailure(f"{label} is not one complete byte read")
    try:
        return base64.b64decode(result["bytes"], validate=True)
    except ValueError as error:
        raise WorkflowFailure(f"{label} returned invalid base64") from error


def decode_json_document(result: dict[str, Any], label: str) -> dict[str, Any]:
    items = result.get("items")
    if (
        result.get("format") != "structured"
        or result.get("record_type") != "json_object"
        or result.get("truncated") is not False
        or result.get("next_cursor") is not None
        or not isinstance(items, list)
        or len(items) != 1
        or not isinstance(items[0], dict)
        or not isinstance(items[0].get("value"), dict)
    ):
        raise WorkflowFailure(f"{label} is not one complete JSON document")
    return items[0]["value"]


def read_bytes(
    mcp: Mcp,
    label: str,
    workbench: str,
    path: str,
    *,
    expected_error: str | None = None,
) -> bytes | None:
    result = mcp.call(
        label,
        "workbench_read",
        {
            "id": workbench,
            "section": "outputs",
            "path": path,
            "format": "bytes",
            "limit": 300,
        },
        expected_error=expected_error,
    )
    return None if expected_error else decode_bytes(result, label)


def read_manifest(mcp: Mcp, label: str, workbench: str, path: str) -> dict[str, Any]:
    result = mcp.call(
        label,
        "workbench_read",
        {
            "id": workbench,
            "section": "metadata",
            "path": path,
            "format": "structured",
            "limit": 300,
        },
    )
    return decode_json_document(result, label)


def inventory(
    config: Config,
    runtime: Runtime,
    evidence: Evidence,
    label: str,
    env: dict[str, str],
) -> dict[str, dict[str, Any]]:
    assert config.aws is not None
    result = run(
        aws_command(
            config.aws,
            runtime.object_endpoint,
            "s3api",
            "list-objects-v2",
            "--bucket",
            runtime.object_bucket,
            "--output",
            "json",
        ),
        config=config,
        evidence=evidence,
        label=label,
        env=env,
    )
    try:
        value = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise WorkflowFailure(f"{label} inventory is not JSON") from error
    output: dict[str, dict[str, Any]] = {}
    for item in value.get("Contents", []):
        if not isinstance(item, dict) or not isinstance(item.get("Key"), str):
            raise WorkflowFailure(f"{label} inventory contains a malformed object")
        output[item["Key"]] = {
            "size": item.get("Size"),
            "etag": item.get("ETag"),
            "last_modified": item.get("LastModified"),
        }
    return output


def changed_objects(
    before: dict[str, dict[str, Any]], after: dict[str, dict[str, Any]]
) -> set[str]:
    return {
        key for key in before.keys() | after.keys() if before.get(key) != after.get(key)
    }


def find_committed(
    result: dict[str, Any], workbench: str, expected_identity: str | None = None
) -> dict[str, Any]:
    matches = result.get("matches")
    if not isinstance(matches, list):
        raise WorkflowFailure("workbench_find lacks matches")
    candidates = [
        value
        for value in matches
        if isinstance(value, dict) and value.get("workbench_id") == workbench
    ]
    if len(candidates) != 1:
        raise WorkflowFailure(f"committed find did not return exactly one {workbench}")
    match = candidates[0]
    if (
        match.get("committed") is not True
        or match.get("commit_identity_verified") is not True
    ):
        raise WorkflowFailure(f"{workbench} is not a verified committed result")
    if (
        expected_identity is not None
        and match.get("commit_identity") != expected_identity
    ):
        raise WorkflowFailure(f"{workbench} find returned a different commit head")
    return match


def parse_json_stdout(
    result: subprocess.CompletedProcess[str], label: str
) -> dict[str, Any]:
    try:
        value = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise WorkflowFailure(f"{label} did not return JSON") from error
    if not isinstance(value, dict):
        raise WorkflowFailure(f"{label} did not return an object")
    return value


def validate_mutation_result(
    result: dict[str, Any], operation: str, workbench: str, request_id: str
) -> None:
    if result.get("status") != "success" or result.get("workbench_id") != workbench:
        raise WorkflowFailure(
            f"workspace-path {operation} did not succeed for {workbench}"
        )
    if result.get("request_id") != request_id:
        raise WorkflowFailure(f"workspace-path {operation} changed request identity")
    generation = result.get("generation")
    if (
        isinstance(generation, bool)
        or not isinstance(generation, int)
        or generation < 1
    ):
        raise WorkflowFailure(f"workspace-path {operation} lacks a positive generation")


def composition_evidence(
    *,
    a_commit: dict[str, Any],
    a_manifest: dict[str, Any],
    snapshot_a: dict[str, Any],
    restore_b: dict[str, Any],
    b_manifest: dict[str, Any],
    b_restore_manifest: dict[str, Any],
    b_find: dict[str, Any],
    b_changed_objects: set[str],
    snapshot_b: dict[str, Any],
    restore_c: dict[str, Any],
    replay_c: dict[str, Any],
    c_manifest: dict[str, Any],
    c_restore_manifest: dict[str, Any],
    c_find: dict[str, Any],
    c_changed_objects: set[str],
    retire_b: dict[str, Any],
    b_renamed: bytes,
    b_published: bytes,
    c_renamed: bytes,
    c_published: bytes,
    c_after_retire: bytes,
    b_frozen_payload: bytes,
    a_live_payload: bytes,
    b_live_after_nested_snapshot: bytes,
) -> dict[str, Any]:
    return {
        "schema": SCHEMA,
        "status": "PASS",
        "oracle_revision": PRE423_ORACLE_REVISION,
        "excluded_surfaces": sorted(EXCLUDED_SURFACES),
        "call_labels": list(MANDATORY_LABELS),
        "workbenches": {
            "a": {
                "id": "composition-a",
                "commit_generation": a_commit.get("generation"),
                "committed": True,
                "run_manifest": a_manifest,
            },
            "b": {
                "id": "composition-b",
                "destination_generation": restore_b.get("destination_generation"),
                "committed": b_find.get("committed"),
                "run_manifest": b_manifest,
                "restore_manifest": b_restore_manifest,
                "destination_owned_manifest_objects": len(b_changed_objects),
                "changed_object_digests": sorted(
                    sha256(key.encode()) for key in b_changed_objects
                ),
                "old_rename_path_absent": True,
                "deleted_path_absent": True,
                "renamed_bytes_sha256": sha256(b_renamed),
                "published_bytes_sha256": sha256(b_published),
            },
            "c": {
                "id": "composition-c",
                "destination_generation": restore_c.get("destination_generation"),
                "committed": c_find.get("committed"),
                "run_manifest": c_manifest,
                "restore_manifest": c_restore_manifest,
                "destination_owned_manifest_objects": len(c_changed_objects),
                "changed_object_digests": sorted(
                    sha256(key.encode()) for key in c_changed_objects
                ),
                "old_rename_path_absent": True,
                "deleted_path_absent": True,
                "renamed_bytes_sha256": sha256(c_renamed),
                "published_bytes_sha256": sha256(c_published),
                "readable_after_snapshot_retire": c_after_retire == c_published,
            },
        },
        "snapshots": {
            "a": {
                "snapshot_id": snapshot_a.get("snapshot_id"),
                "source_workbench_id": "composition-a",
            },
            "b": {
                "snapshot_id": snapshot_b.get("snapshot_id"),
                "source_workbench_id": "composition-b",
                "minted_without_recommit": True,
                "retired": retire_b.get("retired") is True,
            },
        },
        "independence": {
            "a_post_snapshot_bytes_excluded_from_b": (
                b_frozen_payload == initial_payload()
                and a_live_payload == post_snapshot_payload()
            ),
            "b_post_snapshot_bytes_excluded_from_c": (
                b_live_after_nested_snapshot == post_nested_snapshot_payload()
                and c_published == published_payload()
            ),
            "a_b_c_distinct": True,
        },
        "terminal_replay": {
            "operation_id_stable": replay_c.get("operation_id")
            == restore_c.get("operation_id"),
            "destination_commit_identity_stable": c_find.get("commit_identity")
            == c_manifest.get("commit_identity"),
            "idempotent_replay": replay_c.get("idempotent_replay") is True,
        },
        "fault_injection": {
            "status": "NOT QUALIFIED",
            "reason": (
                "The public CLI has no deterministic object-first, dual-manifest-"
                "published, pre-Complete crash barrier; a generic timed SIGKILL would "
                "not prove this boundary."
            ),
        },
    }


def wait_etcd(config: Config, endpoint: str, process: subprocess.Popen[str]) -> None:
    assert config.etcdctl is not None
    deadline = time.monotonic() + config.timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise WorkflowFailure(
                f"etcd exited before readiness ({process.returncode})"
            )
        result = run(
            [config.etcdctl, f"--endpoints={endpoint}", "endpoint", "health"],
            config=config,
            check=False,
            timeout=min(config.timeout, 5),
        )
        if result.returncode == 0:
            return
        time.sleep(0.1)
    raise WorkflowFailure("etcd did not become healthy")


def wait_rustfs(
    config: Config,
    endpoint: str,
    container: str,
    environment: dict[str, str],
) -> None:
    assert config.aws is not None and config.docker is not None
    deadline = time.monotonic() + config.timeout
    while time.monotonic() < deadline:
        result = run(
            aws_command(config.aws, endpoint, "s3api", "list-buckets"),
            config=config,
            check=False,
            env=environment,
            timeout=min(config.timeout, 5),
        )
        if result.returncode == 0:
            return
        state = run(
            [
                config.docker,
                "inspect",
                "--format",
                "{{.State.Status}} {{.State.ExitCode}}",
                container,
            ],
            config=config,
            check=False,
            timeout=min(config.timeout, 5),
        )
        if state.returncode != 0 or state.stdout.split()[:1] in (["dead"], ["exited"]):
            raise WorkflowFailure("RustFS exited before readiness")
        time.sleep(0.25)
    raise WorkflowFailure("RustFS did not become ready")


def workspace_path_capable(config: Config) -> bool:
    result = run(
        [config.binary, "--help"],
        config=config,
        check=False,
        timeout=min(config.timeout, 10),
    )
    text = result.stdout + result.stderr
    return result.returncode == 0 and "workspace-path" in text


def start_mcp(
    config: Config,
    runtime: Runtime,
    evidence: Evidence,
    owner: subprocess.Popen[str],
    stderr: TextIO,
) -> Mcp:
    deadline = time.monotonic() + config.timeout
    last_error = ""
    while time.monotonic() < deadline:
        if owner.poll() is not None:
            raise WorkflowFailure(
                f"owner exited before MCP startup ({owner.returncode})"
            )
        process = subprocess.Popen(
            [*client_args(config, runtime), "mcp"],
            cwd=config.repo,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=stderr,
            text=True,
            bufsize=1,
            start_new_session=True,
        )
        mcp = Mcp(process, evidence, min(config.timeout, 10))
        try:
            mcp.initialize()
            evidence.line(
                "processes.jsonl",
                {
                    "schema": SCHEMA,
                    "label": "mcp",
                    "argv": redact_argv([*client_args(config, runtime), "mcp"]),
                    "pid": process.pid,
                    "started_at": now(),
                },
            )
            return mcp
        except WorkflowFailure as error:
            last_error = str(error)
            stop_process(process)
            time.sleep(0.25)
    raise WorkflowFailure(f"MCP did not initialize: {last_error}")


def execute(config: Config, evidence: Evidence) -> dict[str, Any]:
    if config.timeout <= 0:
        raise NotQualified("--timeout must be positive")
    if "@sha256:" not in config.rustfs_image:
        raise WorkflowFailure("RustFS image must be digest pinned")
    for name, value in (
        ("etcd", config.etcd),
        ("etcdctl", config.etcdctl),
        ("docker", config.docker),
        ("aws", config.aws),
    ):
        if value is None or not value.is_file():
            raise NotQualified(f"{name} executable is required")

    if config.build:
        environment = os.environ.copy()
        environment["CARGO_TARGET_DIR"] = str(config.target)
        run(
            ["cargo", "build", "-p", "nokv", "--bin", "nokv"],
            config=config,
            evidence=evidence,
            label="build",
            env=environment,
            timeout=max(config.timeout, 1200),
        )
    if not config.binary.is_file() or not os.access(config.binary, os.X_OK):
        raise NotQualified(f"nokv binary is missing or not executable: {config.binary}")
    if not workspace_path_capable(config):
        raise NotQualified(
            "nokv lacks the Workbench-scoped workspace-path rename/remove CLI required "
            "by the pre-#423 composition oracle"
        )
    binary_sha256 = digest_file(config.binary)

    etcd_port, peer_port, s3_port, console_port, owner_port = (
        free_port() for _ in range(5)
    )
    runtime = Runtime(
        root_id=fixed_id(config.seed, "root"),
        agent_id=fixed_id(config.seed, "agent"),
        shard_id=fixed_id(config.seed, "shard"),
        etcd_endpoint=f"http://127.0.0.1:{etcd_port}",
        etcd_prefix=f"/nokv/restore-composition/{fixed_id(config.seed, 'control')}",
        object_endpoint=f"http://127.0.0.1:{s3_port}",
        object_bucket=f"nokv-composition-{fixed_id(config.seed, 'bucket')[:12]}",
        object_root=f"restore-composition/{fixed_id(config.seed, 'objects')}",
        access_key="rustfsadmin",
        secret_key="rustfsadmin",
        owner_port=owner_port,
        metadata=evidence.root / "metadata",
    )
    container = f"nokv-restore-composition-{os.getpid()}"
    volume = f"{container}-data"
    environment = aws_environment(runtime.access_key, runtime.secret_key)
    etcd_log = (evidence.root / "etcd.log").open("w", encoding="utf-8")
    owner_log = (evidence.root / "owner.log").open("w", encoding="utf-8")
    mcp_log = (evidence.root / "mcp.stderr.log").open("w", encoding="utf-8")
    etcd_process: subprocess.Popen[str] | None = None
    owner: subprocess.Popen[str] | None = None
    mcp: Mcp | None = None
    container_created = False
    volume_created = False
    try:
        assert config.etcd is not None and config.etcdctl is not None
        assert config.docker is not None and config.aws is not None
        peer = f"http://127.0.0.1:{peer_port}"
        etcd_process = start_process(
            [
                config.etcd,
                "--name",
                "nokv-restore-composition",
                "--data-dir",
                evidence.root / "etcd-data",
                "--listen-client-urls",
                runtime.etcd_endpoint,
                "--advertise-client-urls",
                runtime.etcd_endpoint,
                "--listen-peer-urls",
                peer,
                "--initial-advertise-peer-urls",
                peer,
                "--initial-cluster",
                f"nokv-restore-composition={peer}",
                "--initial-cluster-state",
                "new",
                "--log-level",
                "warn",
            ],
            config,
            etcd_log,
        )
        wait_etcd(config, runtime.etcd_endpoint, etcd_process)
        run(
            [config.docker, "volume", "create", volume],
            config=config,
            evidence=evidence,
            label="create-rustfs-volume",
        )
        volume_created = True
        run(
            rustfs_container_command(
                config.docker,
                container=container,
                volume=volume,
                s3_port=s3_port,
                console_port=console_port,
                image=config.rustfs_image,
                access_key=runtime.access_key,
                secret_key=runtime.secret_key,
            ),
            config=config,
            evidence=evidence,
            label="start-rustfs",
        )
        container_created = True
        wait_rustfs(config, runtime.object_endpoint, container, environment)
        run(
            aws_command(
                config.aws,
                runtime.object_endpoint,
                "s3api",
                "create-bucket",
                "--bucket",
                runtime.object_bucket,
            ),
            config=config,
            evidence=evidence,
            label="create-bucket",
            env=environment,
        )
        provision = run(
            [
                *control_args(config, runtime),
                "--agent-id",
                runtime.agent_id,
                *object_args(runtime),
                "provision",
                runtime.shard_id,
            ],
            config=config,
            evidence=evidence,
            label="provision",
        )
        provision_value = parse_json_stdout(provision, "provision")
        if provision_value.get("lifecycle") != "active":
            raise WorkflowFailure("provision did not activate root placement")
        owner = start_process(owner_command(config, runtime), config, owner_log)
        wait_tcp(owner, runtime.owner_port, config.timeout)
        mcp = start_mcp(config, runtime, evidence, owner, mcp_log)

        plan = {step.label: step for step in oracle_plan(config)}
        for label in (
            "create-a",
            "put-a-rename-source",
            "put-a-delete-source",
            "put-a-payload",
        ):
            step = plan[label]
            mcp.call(step.label, step.operation, step.arguments)
        commit_step = plan["commit-a"]
        a_commit = mcp.call(
            commit_step.label, commit_step.operation, commit_step.arguments
        )
        if (
            a_commit.get("generation") != 1
            or a_commit.get("idempotent_replay") is not False
        ):
            raise WorkflowFailure(
                "A did not establish one fresh generation-1 commit head"
            )
        a_manifest = read_manifest(
            mcp, "read-a-run-manifest", "composition-a", "run_manifest.json"
        )
        snapshot_step = plan["snapshot-a"]
        snapshot_a = mcp.call(
            snapshot_step.label, snapshot_step.operation, snapshot_step.arguments
        )
        if snapshot_a.get("state") != "alive":
            raise WorkflowFailure("snapshot A is not alive")
        mutate_a = plan["mutate-a-after-snapshot"]
        mcp.call(mutate_a.label, mutate_a.operation, mutate_a.arguments)

        before_b = inventory(
            config, runtime, evidence, "inventory-before-b", environment
        )
        restore_b_step = plan["restore-b"]
        restore_b = mcp.call(
            restore_b_step.label, restore_b_step.operation, restore_b_step.arguments
        )
        if (
            restore_b.get("state") != "complete"
            or restore_b.get("idempotent_replay") is not False
        ):
            raise WorkflowFailure("restore B did not return one fresh terminal receipt")
        after_b = inventory(config, runtime, evidence, "inventory-after-b", environment)
        b_changed = changed_objects(before_b, after_b)
        if len(b_changed) != 2:
            raise WorkflowFailure(
                f"restore B must publish exactly two destination manifests, observed {len(b_changed)}"
            )
        b_manifest = read_manifest(
            mcp, "read-b-run-manifest", "composition-b", "run_manifest.json"
        )
        b_restore_manifest = read_manifest(
            mcp, "read-b-restore-manifest", "composition-b", "restore_manifest.json"
        )
        b_frozen_payload = read_bytes(
            mcp, "read-b-frozen-payload", "composition-b", "payload.txt"
        )
        a_live_payload = read_bytes(
            mcp, "read-a-live-payload", "composition-a", "payload.txt"
        )
        assert b_frozen_payload is not None and a_live_payload is not None
        b_find_result = mcp.call(
            "find-b-committed",
            "workbench_find",
            plan["find-b-committed"].arguments,
        )
        b_find = find_committed(
            b_find_result, "composition-b", b_manifest.get("commit_identity")
        )

        prefix = client_args(config, runtime)
        rename_arguments = plan["rename-b"].arguments
        rename = mutation_command(
            config,
            "rename",
            workbench=str(rename_arguments["workbench"]),
            section=str(rename_arguments["section"]),
            path=str(rename_arguments["path"]),
            destination_path=str(rename_arguments["destination_path"]),
            expected_generation=int(rename_arguments["expected_generation"]),
            client_prefix=prefix,
        )
        rename_result = run(
            rename,
            config=config,
            evidence=evidence,
            label="rename-b",
        )
        rename_value = parse_json_stdout(rename_result, "rename-b")
        validate_mutation_result(
            rename_value,
            "rename",
            "composition-b",
            rename[rename.index("--request-id") + 1],
        )
        remove_arguments = plan["remove-b"].arguments
        remove = mutation_command(
            config,
            "remove",
            workbench=str(remove_arguments["workbench"]),
            section=str(remove_arguments["section"]),
            path=str(remove_arguments["path"]),
            expected_generation=int(remove_arguments["expected_generation"]),
            client_prefix=prefix,
        )
        remove_result = run(
            remove,
            config=config,
            evidence=evidence,
            label="remove-b",
        )
        remove_value = parse_json_stdout(remove_result, "remove-b")
        validate_mutation_result(
            remove_value,
            "remove",
            "composition-b",
            remove[remove.index("--request-id") + 1],
        )
        publish_b = plan["publish-b"]
        mcp.call(publish_b.label, publish_b.operation, publish_b.arguments)
        read_bytes(
            mcp,
            "assert-b-old-rename-absent",
            "composition-b",
            "rename-source.txt",
            expected_error="NotFound",
        )
        read_bytes(
            mcp,
            "assert-b-deleted-absent",
            "composition-b",
            "delete-source.txt",
            expected_error="NotFound",
        )
        b_renamed = read_bytes(mcp, "read-b-renamed", "composition-b", "renamed.txt")
        b_published = read_bytes(
            mcp, "read-b-published", "composition-b", "published.txt"
        )
        assert b_renamed is not None and b_published is not None
        if b_renamed != renamed_payload() or b_published != published_payload():
            raise WorkflowFailure("B mutation bytes differ from the oracle")

        snapshot_b_step = plan["snapshot-b-no-recommit"]
        snapshot_b = mcp.call(
            snapshot_b_step.label, snapshot_b_step.operation, snapshot_b_step.arguments
        )
        if snapshot_b.get("state") != "alive":
            raise WorkflowFailure(
                "dirty B could not mint an alive snapshot without recommit"
            )
        before_c = inventory(
            config, runtime, evidence, "inventory-before-c", environment
        )
        restore_c_step = plan["restore-c"]
        restore_c = mcp.call(
            restore_c_step.label, restore_c_step.operation, restore_c_step.arguments
        )
        if (
            restore_c.get("state") != "complete"
            or restore_c.get("idempotent_replay") is not False
        ):
            raise WorkflowFailure("restore C did not return one fresh terminal receipt")
        after_c = inventory(config, runtime, evidence, "inventory-after-c", environment)
        c_changed = changed_objects(before_c, after_c)
        if len(c_changed) != 2:
            raise WorkflowFailure(
                f"restore C must publish exactly two destination manifests, observed {len(c_changed)}"
            )
        c_manifest = read_manifest(
            mcp, "read-c-run-manifest", "composition-c", "run_manifest.json"
        )
        c_restore_manifest = read_manifest(
            mcp, "read-c-restore-manifest", "composition-c", "restore_manifest.json"
        )
        c_find_result = mcp.call(
            "find-c-committed",
            "workbench_find",
            plan["find-c-committed"].arguments,
        )
        c_find = find_committed(
            c_find_result, "composition-c", c_manifest.get("commit_identity")
        )
        read_bytes(
            mcp,
            "assert-c-old-rename-absent",
            "composition-c",
            "rename-source.txt",
            expected_error="NotFound",
        )
        read_bytes(
            mcp,
            "assert-c-deleted-absent",
            "composition-c",
            "delete-source.txt",
            expected_error="NotFound",
        )
        c_renamed = read_bytes(mcp, "read-c-renamed", "composition-c", "renamed.txt")
        c_published = read_bytes(
            mcp, "read-c-published", "composition-c", "published.txt"
        )
        assert c_renamed is not None and c_published is not None
        mutate_b = plan["mutate-b-after-snapshot"]
        mcp.call(mutate_b.label, mutate_b.operation, mutate_b.arguments)
        b_live_after_nested = read_bytes(
            mcp, "read-b-after-nested-snapshot", "composition-b", "published.txt"
        )
        assert b_live_after_nested is not None
        c_unchanged = read_bytes(
            mcp, "read-c-after-b-mutation", "composition-c", "published.txt"
        )
        if c_unchanged != published_payload():
            raise WorkflowFailure("C changed after a later B mutation")
        retire_step = plan["retire-snapshot-b"]
        retire_b = mcp.call(
            retire_step.label, retire_step.operation, retire_step.arguments
        )
        if retire_b.get("retired") is not True or retire_b.get("state") != "retired":
            raise WorkflowFailure("snapshot B did not retire")
        after_retire_step = plan["read-c-after-retire"]
        c_after_retire_result = mcp.call(
            after_retire_step.label,
            after_retire_step.operation,
            after_retire_step.arguments,
        )
        c_after_retire = decode_bytes(c_after_retire_result, "read-c-after-retire")
        replay_step = plan["restore-c-terminal-replay"]
        replay_c = mcp.call(
            replay_step.label, replay_step.operation, replay_step.arguments
        )

        for label, process in (
            ("etcd", etcd_process),
            ("metadata owner", owner),
            ("MCP", mcp.process),
        ):
            if process is None or process.poll() is not None:
                returncode = None if process is None else process.returncode
                raise WorkflowFailure(
                    f"{label} exited before terminal qualification ({returncode})"
                )
        rustfs_state = run(
            [
                config.docker,
                "inspect",
                "--format",
                "{{.State.Running}}",
                container,
            ],
            config=config,
            evidence=evidence,
            label="verify-rustfs-running",
        )
        if rustfs_state.stdout.strip() != "true":
            raise WorkflowFailure("RustFS was not running at terminal qualification")
        if digest_file(config.binary) != binary_sha256:
            raise WorkflowFailure("nokv binary changed during live qualification")

        record = composition_evidence(
            a_commit=a_commit,
            a_manifest=a_manifest,
            snapshot_a=snapshot_a,
            restore_b=restore_b,
            b_manifest=b_manifest,
            b_restore_manifest=b_restore_manifest,
            b_find=b_find,
            b_changed_objects=b_changed,
            snapshot_b=snapshot_b,
            restore_c=restore_c,
            replay_c=replay_c,
            c_manifest=c_manifest,
            c_restore_manifest=c_restore_manifest,
            c_find=c_find,
            c_changed_objects=c_changed,
            retire_b=retire_b,
            b_renamed=b_renamed,
            b_published=b_published,
            c_renamed=c_renamed,
            c_published=c_published,
            c_after_retire=c_after_retire,
            b_frozen_payload=b_frozen_payload,
            a_live_payload=a_live_payload,
            b_live_after_nested_snapshot=b_live_after_nested,
        )
        validate_composition_evidence(record)

        owner_key = f"{runtime.etcd_prefix}/logical-shards/{runtime.shard_id}"
        owner_value = run(
            [
                config.etcdctl,
                f"--endpoints={runtime.etcd_endpoint}",
                "get",
                owner_key,
                "--print-value-only",
            ],
            config=config,
            evidence=evidence,
            label="capture-owner-record",
        ).stdout.strip()
        try:
            owner_record = json.loads(owner_value)
        except json.JSONDecodeError as error:
            raise WorkflowFailure("owner control record is not JSON") from error
        if not isinstance(owner_record.get("owner_epoch"), int):
            raise WorkflowFailure("owner control record lacks owner_epoch")
        evidence.json("composition.json", record)
        evidence.json(
            "environment.json",
            environment_evidence(config, runtime, owner_record, container),
        )
        return record
    finally:
        if mcp is not None:
            if mcp.process.stdin is not None:
                try:
                    mcp.process.stdin.close()
                except OSError:
                    pass
            stop_process(mcp.process)
        stop_process(owner)
        stop_process(etcd_process)
        mcp_log.close()
        owner_log.close()
        etcd_log.close()
        if not config.keep_resources:
            if container_created:
                run(
                    [config.docker, "rm", "-f", container],
                    config=config,
                    check=False,
                    timeout=min(config.timeout, 30),
                )
            if volume_created:
                run(
                    [config.docker, "volume", "rm", "-f", volume],
                    config=config,
                    check=False,
                    timeout=min(config.timeout, 30),
                )


def shell_fact(config: Config, argv: Sequence[os.PathLike[str] | str]) -> str | None:
    try:
        result = run(argv, config=config, check=False, timeout=min(config.timeout, 10))
    except (OSError, WorkflowFailure):
        return None
    return result.stdout.strip() if result.returncode == 0 else None


def environment_evidence(
    config: Config,
    runtime: Runtime,
    owner_record: dict[str, Any],
    container: str,
) -> dict[str, Any]:
    assert (
        config.etcd is not None
        and config.etcdctl is not None
        and config.docker is not None
    )
    return {
        "schema": SCHEMA,
        "captured_at": now(),
        "oracle_revision": PRE423_ORACLE_REVISION,
        "git_commit": shell_fact(config, ["git", "rev-parse", "HEAD"]),
        "git_status_porcelain": shell_fact(config, ["git", "status", "--porcelain=v1"]),
        "binary": {
            "path": str(config.binary),
            "sha256": digest_file(config.binary),
            "version": shell_fact(config, [config.binary, "version", "--json"]),
        },
        "provider": {
            "rustfs_image": config.rustfs_image,
            "container_image_id": shell_fact(
                config, [config.docker, "inspect", "--format", "{{.Image}}", container]
            ),
        },
        "etcd": {
            "version": shell_fact(config, [config.etcd, "--version"]),
            "etcdctl_version": shell_fact(config, [config.etcdctl, "version"]),
        },
        "identity": {
            "root_id": runtime.root_id,
            "agent_id": runtime.agent_id,
            "logical_shard_id": runtime.shard_id,
            "owner_epoch": owner_record.get("owner_epoch"),
            "workbench_root": runtime.workbench_root,
        },
        "python": sys.version,
        "platform": platform.platform(),
    }


def plan_evidence(config: Config) -> dict[str, Any]:
    steps = oracle_plan(config)
    mutations = []
    for step in steps:
        if step.surface != "workspace-path":
            continue
        arguments = step.arguments
        mutations.append(
            {
                "label": step.label,
                "command": redact_argv(
                    mutation_command(
                        config,
                        step.operation,
                        workbench=str(arguments["workbench"]),
                        section=str(arguments["section"]),
                        path=str(arguments["path"]),
                        destination_path=(
                            str(arguments["destination_path"])
                            if "destination_path" in arguments
                            else None
                        ),
                        expected_generation=int(arguments["expected_generation"]),
                    )
                ),
            }
        )
    return {
        "schema": SCHEMA,
        "mode": "dry-run" if config.dry_run else "live",
        "oracle": {
            "revision": PRE423_ORACLE_REVISION,
            "path": "scripts/lingtai-workbench/durable_restore_live_e2e.py",
            "excluded_surfaces": sorted(EXCLUDED_SURFACES),
        },
        "steps": [dataclasses.asdict(step) for step in steps],
        "workspace_path_mutations": mutations,
        "fault_injection": {
            "status": "NOT QUALIFIED",
            "reason": (
                "No public deterministic object-first/pre-Complete barrier is available; "
                "timed sleeps and generic SIGKILL are not evidence."
            ),
        },
        "future_phases": {
            "concurrent_exact_restore": [8, 16],
            "retire_gc_release": "pending after the core composition gate",
        },
    }


def parse_args(argv: list[str] | None = None) -> Config:
    repo = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=repo)
    parser.add_argument("--nokv-bin", type=Path)
    parser.add_argument("--target-dir", type=Path)
    parser.add_argument("--evidence-dir", type=Path)
    parser.add_argument("--etcd-bin", type=Path, default=shutil.which("etcd"))
    parser.add_argument("--etcdctl-bin", type=Path, default=shutil.which("etcdctl"))
    parser.add_argument("--docker-bin", type=Path, default=shutil.which("docker"))
    parser.add_argument("--aws-bin", type=Path, default=shutil.which("aws"))
    parser.add_argument("--rustfs-image", default=PINNED_RUSTFS_IMAGE)
    parser.add_argument("--seed", default="pre423-restore-composition")
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--keep-resources", action="store_true")
    args = parser.parse_args(argv)
    resolved_repo = args.repo.resolve()
    target = (args.target_dir or resolved_repo / "target").resolve()
    binary = (args.nokv_bin or target / "debug" / "nokv").resolve()
    evidence = (
        args.evidence_dir
        or target
        / "restore-composition-gate"
        / "evidence"
        / f"{fixed_id(args.seed, 'run')[:12]}"
    ).resolve()
    return Config(
        repo=resolved_repo,
        binary=binary,
        evidence=evidence,
        target=target,
        etcd=args.etcd_bin.resolve() if args.etcd_bin else None,
        etcdctl=args.etcdctl_bin.resolve() if args.etcdctl_bin else None,
        docker=args.docker_bin.resolve() if args.docker_bin else None,
        aws=args.aws_bin.resolve() if args.aws_bin else None,
        rustfs_image=args.rustfs_image,
        seed=args.seed,
        build=args.build,
        dry_run=args.dry_run,
        timeout=args.timeout,
        keep_resources=args.keep_resources,
    )


def main(argv: list[str] | None = None) -> int:
    config = parse_args(argv)
    evidence = Evidence(config.evidence)
    prepared = False
    try:
        evidence.prepare()
        prepared = True
        evidence.json("plan.json", plan_evidence(config))
        if config.dry_run:
            record = qualification(
                composition="NOT QUALIFIED",
                fault="NOT QUALIFIED",
                reason=(
                    "Dry-run froze the pre-#423 command and assertion graph; no etcd, "
                    "RustFS, metadata owner, MCP process, or workspace-path mutation ran."
                ),
            )
            evidence.json("qualification.json", record)
            print(json.dumps(record, indent=2, sort_keys=True))
            return 0
        result = execute(config, evidence)
        transcript = digest_file(evidence.root / "mcp-transcript.jsonl")
        reason = str(_mapping(result, "fault_injection")["reason"])
        record = qualification(
            composition="PASS",
            fault=str(_mapping(result, "fault_injection")["status"]),
            reason=reason,
            transcript_sha256=transcript,
        )
        evidence.json("qualification.json", record)
        print(json.dumps(record, indent=2, sort_keys=True))
        return 0
    except NotQualified as error:
        record = qualification(
            composition="NOT QUALIFIED", fault="NOT QUALIFIED", reason=str(error)
        )
        if prepared:
            evidence.json("qualification.json", record)
        print(json.dumps(record, indent=2, sort_keys=True))
        return 3
    except (WorkflowFailure, OSError, ValueError, json.JSONDecodeError) as error:
        record = qualification(
            composition="FAIL", fault="NOT QUALIFIED", reason=str(error)
        )
        if prepared:
            evidence.json("qualification.json", record)
        print(json.dumps(record, indent=2, sort_keys=True))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
