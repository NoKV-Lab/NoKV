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

from source_bound_producer import ProducerError, ScenarioContract
from typed_live_qualification import load_live_context, publish_live_result

SCHEMA = "nokv.restore_composition_gate.v1"
PROTOCOL_VERSION = "2025-11-25"
PRE423_ORACLE_REVISION = "98cac201"
PINNED_RUSTFS_IMAGE = (
    "rustfs/rustfs@sha256:"
    "e620d37756fff072b10bf648c7bb9d370d7e91a928b7e6a5e1ac85bdfb4e4dab"
)
BUILD_TIMEOUT_SECONDS = 1_200.0
EXCLUDED_SURFACES = frozenset(
    {"FUSE", "POSIX", "Yanex", "inode", "dentry", "physical layout"}
)
SECRET_FLAGS = {"--object-access-key-id", "--object-secret-access-key"}
SECRET_ENV_PREFIXES = ("RUSTFS_ACCESS_KEY=", "RUSTFS_SECRET_KEY=")
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
TYPED_EVIDENCE_ROLES = ("producer-result", "qualification")
TYPED_SCENARIOS = {
    "t14.restored-destination-resnapshot": ScenarioContract(
        "T14", "restore-composition"
    ),
    "t18.restore-independent-destination": ScenarioContract(
        "T18", "restore-composition"
    ),
    "t18.restore-terminal-replay": ScenarioContract("T18", "restore-composition"),
    "c20.hidden-staging-cow-independent-destination": ScenarioContract(
        "C20", "restore-composition"
    ),
    "c20.restore-terminal-replay": ScenarioContract("C20", "restore-composition"),
    "c21.restore-snapshot-restore-composition": ScenarioContract(
        "C21", "restore-composition"
    ),
}


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
    fault_binary: Path
    fault_target: Path
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
    qualification_result: Path | None = None


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


def fixed_hex_value(value: Any, size: int, label: str) -> str:
    if isinstance(value, str) and re.fullmatch(rf"[0-9a-f]{{{size * 2}}}", value):
        return value
    if (
        isinstance(value, list)
        and len(value) == size
        and all(
            isinstance(byte, int) and not isinstance(byte, bool) and 0 <= byte <= 255
            for byte in value
        )
    ):
        return bytes(value).hex()
    raise WorkflowFailure(f"{label} must be exactly {size} bytes")


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
    if fault.get("status") != "PASS":
        raise WorkflowFailure("partial-publication recovery is not PASS")
    if (
        fault.get("arm_schema") != "nokv.restore-crash.arm.v1"
        or fault.get("evidence_schema") != "nokv.restore-crash.evidence.v1"
        or not HEX_32.fullmatch(str(fault.get("run_id", "")))
        or not HEX_32.fullmatch(str(fault.get("root_id", "")))
        or not HEX_32.fullmatch(
            str(fault.get("destination_workspace_incarnation_id", ""))
        )
        or fault.get("destination_generation") != 1
        or fault.get("interruption_label") != "restore-b-pre-complete-crash"
        or fault.get("interrupted_oracle_label") != "restore-b"
        or fault.get("replay_label") != fault.get("interrupted_oracle_label")
        or MANDATORY_LABELS[7] != fault.get("interrupted_oracle_label")
    ):
        raise WorkflowFailure(
            "fault arm, root, destination, or generation is malformed"
        )
    operation_id = _string(fault, "operation_id", "fault_injection")
    if (
        not HEX_32.fullmatch(operation_id)
        or fault.get("replay_operation_id") != operation_id
    ):
        raise WorkflowFailure("fault operation identity drifted during exact replay")
    destination_commit_id = _string(fault, "destination_commit_id", "fault_injection")
    if (
        not re.fullmatch(r"[0-9a-f]{64}", destination_commit_id)
        or fault.get("replay_destination_commit_id") != destination_commit_id
    ):
        raise WorkflowFailure("fault destination commit identity drifted during replay")
    if (
        fault.get("phase") != "destination_building"
        or not isinstance(fault.get("durable_read_version"), int)
        or isinstance(fault.get("durable_read_version"), bool)
        or fault["durable_read_version"] <= 0
        or fault.get("built_commit_members") != 0
        or fault.get("sealed_revisions") != 0
    ):
        raise WorkflowFailure("fault barrier lacks durable zero closure progress")
    publication_ids = fault.get("manifest_publication_operation_ids")
    revision_ids = fault.get("manifest_artifact_revision_ids")
    if (
        fault.get("publication_states_before_replay") != ["succeeded", "succeeded"]
        or not isinstance(publication_ids, list)
        or len(publication_ids) != 2
        or not all(
            isinstance(value, str) and HEX_32.fullmatch(value)
            for value in publication_ids
        )
        or len(set(publication_ids)) != 2
        or not isinstance(revision_ids, list)
        or len(revision_ids) != 2
        or not all(
            isinstance(value, str) and HEX_32.fullmatch(value) for value in revision_ids
        )
        or len(set(revision_ids)) != 2
        or fault.get("manifest_bindings_exact") is not True
        or fault.get("manifest_objects_published_before_crash") != 2
    ):
        raise WorkflowFailure("fault manifest publications are incomplete or drifted")
    pre_inventory = _string(
        fault, "pre_replay_object_inventory_sha256", "fault_injection"
    )
    if (
        not re.fullmatch(r"[0-9a-f]{64}", pre_inventory)
        or fault.get("post_replay_object_inventory_sha256") != pre_inventory
        or fault.get("object_inventory_stable_across_replay") is not True
    ):
        raise WorkflowFailure("fault object inventory drifted across exact replay")
    if (
        any(
            fault.get(field) is not True
            for field in (
                "initial_owner_session_absent_before_fault",
                "fault_owner_session_absent_before_reopen",
                "destination_hidden_before_replay",
                "idempotent_replay",
                "fault_owner_socket_ready",
                "successor_owner_socket_ready",
                "mcp_survived_fault_owner_exit",
            )
        )
        or fault.get("owner_exit_code") != 86
        or fault.get("operation_state_before_replay") != "running"
    ):
        raise WorkflowFailure("fault successor/replay boundary is incomplete")
    validate_owner_loss_error(
        fault.get("client_failure"), "fault injection client failure"
    )


def qualification(
    *, composition: str, fault: str, reason: str, transcript_sha256: str | None = None
) -> dict[str, Any]:
    if composition == "FAIL" or fault == "FAIL":
        overall = "FAIL"
    elif composition == "PASS" and fault == "PASS":
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
        elif argument.startswith(SECRET_ENV_PREFIXES):
            name = argument.split("=", 1)[0]
            output.append(f"{name}=<redacted>")
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
    argv: Sequence[os.PathLike[str] | str],
    config: Config,
    log: TextIO,
    *,
    evidence: Evidence,
    label: str,
) -> subprocess.Popen[str]:
    started_at = now()
    process = subprocess.Popen(
        [str(item) for item in argv],
        cwd=config.repo,
        stdin=subprocess.DEVNULL,
        stdout=log,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )
    evidence.line(
        "processes.jsonl",
        {
            "schema": SCHEMA,
            "label": label,
            "argv": redact_argv(argv),
            "pid": process.pid,
            "started_at": started_at,
        },
    )
    return process


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


def successor_owner_command(config: Config, runtime: Runtime) -> list[str]:
    return [
        *control_args(config, runtime),
        *object_args(runtime),
        "--bind",
        f"127.0.0.1:{runtime.owner_port}",
        "--advertise-endpoint",
        f"127.0.0.1:{runtime.owner_port}",
        "--node-id",
        "restore-composition-successor",
        "--metadata-reopen",
        str(runtime.metadata),
        "--lifecycle-interval-millis",
        "100",
        "serve",
    ]


def fault_route_args(config: Config, runtime: Runtime) -> list[str]:
    return [
        str(config.fault_binary),
        "--etcd-endpoint",
        runtime.etcd_endpoint,
        "--etcd-key-prefix",
        runtime.etcd_prefix,
        "--lease-ttl-seconds",
        "2",
        "--root-id",
        runtime.root_id,
    ]


def fault_arm_command(
    config: Config,
    runtime: Runtime,
    *,
    snapshot_id: int,
    arm_file: Path,
) -> list[str]:
    return [
        str(config.fault_binary),
        "arm",
        *fault_route_args(config, runtime)[1:],
        "--run-id",
        fixed_id(config.seed, "fault-run"),
        "--source-workbench",
        "composition-a",
        "--snapshot-id",
        str(snapshot_id),
        "--destination-workbench",
        "composition-b",
        "--output",
        str(arm_file),
    ]


def fault_owner_command(
    config: Config,
    runtime: Runtime,
    *,
    arm_file: Path,
    evidence_file: Path,
) -> list[str]:
    return [
        str(config.fault_binary),
        "serve",
        *fault_route_args(config, runtime)[1:],
        "--node-id",
        "restore-composition-fault-owner",
        "--advertise-endpoint",
        f"127.0.0.1:{runtime.owner_port}",
        "--bind",
        f"127.0.0.1:{runtime.owner_port}",
        "--metadata-reopen",
        str(runtime.metadata),
        *object_args(runtime),
        "--arm-file",
        str(arm_file),
        "--evidence-file",
        str(evidence_file),
    ]


def fault_inspect_command(
    config: Config, runtime: Runtime, operation_id: str
) -> list[str]:
    return [
        str(config.fault_binary),
        "inspect",
        *fault_route_args(config, runtime)[1:],
        "--operation-id",
        operation_id,
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
        structured = matching_mcp_structured_content(result, label)
        is_error = result.get("isError") is True
        if expected_error is not None:
            if not is_error or structured.get("code") != expected_error:
                raise WorkflowFailure(f"{label} did not return {expected_error}")
        elif is_error or structured.get("status") != "success":
            raise WorkflowFailure(f"{label} failed: {structured!r}")
        return structured

    def call_until_owner_loss(
        self,
        label: str,
        name: str,
        arguments: dict[str, Any],
    ) -> dict[str, Any]:
        response = self.request(
            "tools/call", {"name": name, "arguments": arguments}, label=label
        )
        result = response.get("result")
        if not isinstance(result, dict) or result.get("isError") is not True:
            raise WorkflowFailure(
                f"{label} did not return the bounded client failure after owner loss"
            )
        structured = matching_mcp_structured_content(result, label)
        validate_owner_loss_error(structured, label)
        return structured


def matching_mcp_structured_content(value: Any, label: str) -> dict[str, Any]:
    structured = value.get("structuredContent") if isinstance(value, dict) else None
    if not isinstance(structured, dict):
        raise WorkflowFailure(f"{label} lacks structuredContent")
    try:
        text_value = json.loads(value["content"][0]["text"])
    except (IndexError, KeyError, TypeError, json.JSONDecodeError) as error:
        raise WorkflowFailure(f"{label} lacks matching JSON text content") from error
    if text_value != structured:
        raise WorkflowFailure(f"{label} text and structured results differ")
    return structured


def validate_owner_loss_error(value: Any, label: str) -> None:
    details = value.get("details") if isinstance(value, dict) else None
    if (
        not isinstance(value, dict)
        or value.get("status") != "error"
        or value.get("code") != "ClientFailure"
        or value.get("retryable") is not True
        or not isinstance(details, dict)
        or details.get("source") != "nokv-client"
        or details.get("attempts") != 3
    ):
        raise WorkflowFailure(
            f"{label} did not return the bounded client failure after owner loss"
        )


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
    # The zero-copy assertions concern the artifact keyspace only. Recovery
    # log segments and provider-admission probes live under sibling prefixes
    # of the same object root and must not count as restore-published objects.
    result = run(
        aws_command(
            config.aws,
            runtime.object_endpoint,
            "s3api",
            "list-objects-v2",
            "--bucket",
            runtime.object_bucket,
            "--prefix",
            artifact_inventory_prefix(runtime),
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


def artifact_inventory_prefix(runtime: Runtime) -> str:
    return f"{runtime.object_root.strip('/')}/nokv/artifacts/"


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


def load_json_object(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise WorkflowFailure(f"{label} is not readable JSON") from error
    if not isinstance(value, dict):
        raise WorkflowFailure(f"{label} is not an object")
    return value


def validate_fault_arm(
    arm: dict[str, Any],
    *,
    runtime: Runtime,
    snapshot_id: int,
    run_id: str,
) -> str:
    operation_id = fixed_hex_value(
        arm.get("operation_id"), 16, "restore crash arm.operation_id"
    )
    if arm.get("schema") != "nokv.restore-crash.arm.v1" or arm.get("run_id") != run_id:
        raise WorkflowFailure("restore crash arm schema or run identity differs")
    if (
        fixed_hex_value(arm.get("root_id"), 16, "restore crash arm.root_id")
        != runtime.root_id
        or arm.get("source_workbench") != "composition-a"
        or arm.get("destination_workbench") != "composition-b"
        or arm.get("snapshot_id") != snapshot_id
    ):
        raise WorkflowFailure(
            "restore crash arm identities differ from the live workflow"
        )
    fixed_hex_value(
        arm.get("source_workspace_incarnation_id"),
        16,
        "restore crash arm.source_workspace_incarnation_id",
    )
    fixed_hex_value(
        arm.get("destination_workspace_incarnation_id"),
        16,
        "restore crash arm.destination_workspace_incarnation_id",
    )
    return operation_id


def validate_fault_barrier_evidence(
    envelope: dict[str, Any], arm: dict[str, Any]
) -> dict[str, Any]:
    if (
        envelope.get("schema") != "nokv.restore-crash.evidence.v1"
        or envelope.get("run_id") != arm.get("run_id")
        or envelope.get("root_id") != arm.get("root_id")
        or envelope.get("operation_id") != arm.get("operation_id")
    ):
        raise WorkflowFailure("restore crash evidence envelope differs from its arm")
    value = _mapping(envelope, "evidence")
    route = _mapping(value, "route")
    operation_id = fixed_hex_value(
        value.get("operation_id"), 16, "restore crash evidence.operation_id"
    )
    destination_id = fixed_hex_value(
        value.get("destination_workspace_incarnation_id"),
        16,
        "restore crash evidence.destination_workspace_incarnation_id",
    )
    destination_commit_id = fixed_hex_value(
        value.get("destination_commit_id"),
        32,
        "restore crash evidence.destination_commit_id",
    )
    if (
        fixed_hex_value(route.get("root_id"), 16, "restore crash route.root_id")
        != fixed_hex_value(arm.get("root_id"), 16, "restore crash arm.root_id")
        or operation_id
        != fixed_hex_value(
            arm.get("operation_id"), 16, "restore crash arm.operation_id"
        )
        or destination_id
        != fixed_hex_value(
            arm.get("destination_workspace_incarnation_id"),
            16,
            "restore crash arm.destination_workspace_incarnation_id",
        )
        or value.get("phase") != "destination_building"
        or not isinstance(value.get("durable_read_version"), int)
        or isinstance(value.get("durable_read_version"), bool)
        or value["durable_read_version"] <= 0
        or value.get("built_commit_members") != 0
        or value.get("sealed_revisions") != 0
    ):
        raise WorkflowFailure(
            "restore crash barrier is not pristine DestinationBuilding"
        )
    publication_ids: list[str] = []
    revision_ids: list[str] = []
    for key in ("run_manifest", "restore_manifest"):
        binding = _mapping(value, key)
        expected = _mapping(binding, "expected")
        actual = _mapping(binding, "actual")
        identity = _mapping(actual, "identity")
        if expected != identity:
            raise WorkflowFailure(f"restore crash {key} binding is not exact")
        publication_id = fixed_hex_value(
            expected.get("publication_operation_id"),
            16,
            f"restore crash {key}.publication_operation_id",
        )
        revision_id = fixed_hex_value(
            expected.get("artifact_revision_id"),
            16,
            f"restore crash {key}.artifact_revision_id",
        )
        if (
            fixed_hex_value(
                actual.get("workspace_incarnation_id"),
                16,
                f"restore crash {key}.workspace_incarnation_id",
            )
            != destination_id
            or not re.fullmatch(
                r"sha256:[0-9a-f]{64}", str(actual.get("body_digest_uri", ""))
            )
            or not re.fullmatch(
                r"sha256:[0-9a-f]{64}", str(actual.get("manifest_digest_uri", ""))
            )
            or not isinstance(actual.get("logical_size"), int)
            or isinstance(actual.get("logical_size"), bool)
            or actual["logical_size"] <= 0
            or actual.get("content_type") != "application/json"
        ):
            raise WorkflowFailure(f"restore crash {key} publication is malformed")
        publication_ids.append(publication_id)
        revision_ids.append(revision_id)
    if len(set(publication_ids)) != 2:
        raise WorkflowFailure("restore crash manifest publications are not distinct")
    return {
        "operation_id": operation_id,
        "destination_commit_id": destination_commit_id,
        "phase": value["phase"],
        "durable_read_version": value["durable_read_version"],
        "built_commit_members": value["built_commit_members"],
        "sealed_revisions": value["sealed_revisions"],
        "manifest_publication_operation_ids": publication_ids,
        "manifest_artifact_revision_ids": revision_ids,
        "manifest_bindings_exact": True,
    }


def validate_operation_inspection(
    inspection: dict[str, Any],
    *,
    root_id: str,
    operation_id: str,
    kind: str,
    state: str,
    artifact_revision_id: str | None = None,
    destination_workspace_incarnation_id: str | None = None,
    destination_commit_id: str | None = None,
) -> None:
    operation = _mapping(inspection, "operation")
    token = _mapping(operation, "token")
    if (
        inspection.get("schema") != "nokv.restore-crash.operation-inspection.v1"
        or fixed_hex_value(
            inspection.get("root_id"), 16, "operation inspection.root_id"
        )
        != root_id
        or fixed_hex_value(
            inspection.get("operation_id"), 16, "operation inspection.operation_id"
        )
        != operation_id
        or fixed_hex_value(
            token.get("operation_id"), 16, "operation inspection.token.operation_id"
        )
        != operation_id
        or operation.get("kind") != kind
        or operation.get("state") != state
        or inspection.get("commit_version") is not None
        or inspection.get("replayed") is not False
    ):
        raise WorkflowFailure(f"{kind} operation inspection differs")
    if state == "running":
        if operation.get("result") is not None or operation.get("failure") is not None:
            raise WorkflowFailure(f"running {kind} operation is unexpectedly terminal")
        return
    result = _mapping(operation, "result")
    terminal = _mapping(result, "result")
    if state != "succeeded" or operation.get("failure") is not None:
        raise WorkflowFailure(f"terminal {kind} operation is malformed")
    if kind == "artifact_publish":
        if (
            result.get("kind") != "artifact_publish"
            or fixed_hex_value(
                terminal.get("operation_id"),
                16,
                "artifact publication result.operation_id",
            )
            != operation_id
            or fixed_hex_value(
                terminal.get("artifact_revision_id"),
                16,
                "artifact publication result.artifact_revision_id",
            )
            != artifact_revision_id
        ):
            raise WorkflowFailure(
                "manifest publication operation is not exact and terminal"
            )
        return
    if kind == "restore":
        destination = _mapping(terminal, "destination")
        if (
            result.get("kind") != "restore"
            or fixed_hex_value(
                terminal.get("operation_id"), 16, "restore result.operation_id"
            )
            != operation_id
            or fixed_hex_value(
                destination.get("workspace_incarnation_id"),
                16,
                "restore result.destination.workspace_incarnation_id",
            )
            != destination_workspace_incarnation_id
            or fixed_hex_value(
                destination.get("commit_head"),
                32,
                "restore result.destination.commit_head",
            )
            != destination_commit_id
        ):
            raise WorkflowFailure("restore operation terminal receipt drifted")
        return
    raise WorkflowFailure(f"unsupported terminal operation kind {kind}")


def validate_restore_inspection_binding(
    inspection: dict[str, Any],
    arm: dict[str, Any],
    barrier_envelope: dict[str, Any],
) -> None:
    operation = _mapping(inspection, "operation")
    preparation = _mapping(operation, "restore_preparation")
    request = _mapping(preparation, "request")
    binding = _mapping(preparation, "destination_binding")
    evidence = _mapping(barrier_envelope, "evidence")
    if (
        request.get("operation_id") != arm.get("operation_id")
        or request.get("destination_workspace_incarnation_id")
        != arm.get("destination_workspace_incarnation_id")
        or binding.get("destination_commit_id") != evidence.get("destination_commit_id")
        or binding.get("destination_run_manifest_identity")
        != _mapping(_mapping(evidence, "run_manifest"), "expected")
        or binding.get("destination_restore_manifest_identity")
        != _mapping(_mapping(evidence, "restore_manifest"), "expected")
    ):
        raise WorkflowFailure("restore operation durable destination binding drifted")
    manifests = _mapping(binding, "destination_manifests")
    for key in ("run_manifest", "restore_manifest"):
        operation_manifest = _mapping(manifests, key)
        actual = _mapping(_mapping(evidence, key), "actual")
        descriptor = _mapping(operation_manifest, "descriptor")
        if (
            operation_manifest.get("publication_operation_id")
            != _mapping(actual, "identity").get("publication_operation_id")
            or operation_manifest.get("artifact_revision_id")
            != _mapping(actual, "identity").get("artifact_revision_id")
            or operation_manifest.get("workspace_incarnation_id")
            != actual.get("workspace_incarnation_id")
            or descriptor.get("body_digest") != actual.get("body_digest_uri")
            or descriptor.get("manifest_digest") != actual.get("manifest_digest_uri")
            or descriptor.get("logical_size") != actual.get("logical_size")
            or descriptor.get("content_type") != actual.get("content_type")
        ):
            raise WorkflowFailure(f"restore operation {key} binding drifted")


def inventory_digest(value: dict[str, dict[str, Any]]) -> str:
    return sha256(canonical_json(value).encode())


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
    fault_injection: dict[str, Any],
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
        "fault_injection": fault_injection,
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


def owner_session_key(runtime: Runtime) -> str:
    return f"{runtime.etcd_prefix.rstrip('/')}/sessions/{runtime.shard_id}"


def read_owner_session(config: Config, runtime: Runtime) -> str:
    assert config.etcdctl is not None
    return run(
        [
            config.etcdctl,
            f"--endpoints={runtime.etcd_endpoint}",
            "get",
            owner_session_key(runtime),
            "--print-value-only",
        ],
        config=config,
        check=True,
        timeout=min(config.timeout, 5),
    ).stdout.strip()


def require_owner_session(config: Config, runtime: Runtime, label: str) -> None:
    if not read_owner_session(config, runtime):
        raise WorkflowFailure(f"{label} did not install its lease-attached session key")


def wait_owner_session_absent(
    config: Config, runtime: Runtime, label: str, deadline: float
) -> None:
    while time.monotonic() < deadline:
        if not read_owner_session(config, runtime):
            return
        time.sleep(0.05)
    raise WorkflowFailure(f"{label} session remained present after its lease TTL")


def remaining(deadline: float, label: str) -> float:
    value = deadline - time.monotonic()
    if value <= 0:
        raise WorkflowFailure(f"{label} exhausted the absolute fault deadline")
    return value


def wait_exact_exit(
    process: subprocess.Popen[str], expected: int, timeout: float, label: str
) -> int:
    try:
        returncode = process.wait(timeout=timeout)
    except subprocess.TimeoutExpired as error:
        raise WorkflowFailure(
            f"{label} did not reach its deterministic exit"
        ) from error
    if returncode != expected:
        raise WorkflowFailure(f"{label} exited {returncode}, expected {expected}")
    return returncode


def build_timeout(runtime_timeout: float) -> float:
    return max(runtime_timeout, BUILD_TIMEOUT_SECONDS)


def close_mcp(mcp: Mcp | None) -> None:
    if mcp is None:
        return
    if mcp.process.stdin is not None:
        try:
            mcp.process.stdin.close()
        except OSError:
            pass
    stop_process(mcp.process)


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
    return (
        result.returncode == 0
        and "workspace-path" in text
        and "restore-crash" not in text
        and "--arm-file" not in text
        and "--evidence-file" not in text
    )


def start_mcp(
    config: Config,
    runtime: Runtime,
    evidence: Evidence,
    owner: subprocess.Popen[str],
    stderr: TextIO,
    *,
    deadline: float | None = None,
) -> Mcp:
    deadline = deadline or time.monotonic() + config.timeout
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
        mcp = Mcp(process, evidence, min(remaining(deadline, "MCP startup"), 10))
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
    if config.target == config.fault_target:
        raise NotQualified("default and fault-owner Cargo targets must be independent")

    if config.build:
        environment = os.environ.copy()
        environment["CARGO_TARGET_DIR"] = str(config.target)
        run(
            ["cargo", "build", "-p", "nokv", "--bin", "nokv"],
            config=config,
            evidence=evidence,
            label="build",
            env=environment,
            timeout=build_timeout(config.timeout),
        )
        fault_environment = os.environ.copy()
        fault_environment["CARGO_TARGET_DIR"] = str(config.fault_target)
        run(
            [
                "cargo",
                "build",
                "-p",
                "nokv-bench",
                "--bin",
                "nokv-restore-crash-owner",
                "--no-default-features",
                "--features",
                "restore-crash-test-support",
            ],
            config=config,
            evidence=evidence,
            label="build-fault-owner",
            env=fault_environment,
            timeout=build_timeout(config.timeout),
        )
    if not config.binary.is_file() or not os.access(config.binary, os.X_OK):
        raise NotQualified(f"nokv binary is missing or not executable: {config.binary}")
    if not config.fault_binary.is_file() or not os.access(config.fault_binary, os.X_OK):
        raise NotQualified(
            f"feature-only restore crash owner is missing or not executable: {config.fault_binary}"
        )
    if not workspace_path_capable(config):
        raise NotQualified(
            "nokv lacks the Workbench-scoped workspace-path rename/remove CLI required "
            "by the pre-#423 composition oracle"
        )
    binary_sha256 = digest_file(config.binary)
    fault_binary_sha256 = digest_file(config.fault_binary)

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
            evidence=evidence,
            label="etcd-owner-control",
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
        owner = start_process(
            owner_command(config, runtime),
            config,
            owner_log,
            evidence=evidence,
            label="initial-owner",
        )
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

        snapshot_a_id = snapshot_a.get("snapshot_id")
        if (
            not isinstance(snapshot_a_id, int)
            or isinstance(snapshot_a_id, bool)
            or snapshot_a_id <= 0
        ):
            raise WorkflowFailure("snapshot A lacks a positive snapshot identity")
        arm_file = evidence.root / "restore-b-crash-arm.json"
        barrier_file = evidence.root / "restore-b-crash-evidence.json"
        arm_result = run(
            fault_arm_command(
                config,
                runtime,
                snapshot_id=snapshot_a_id,
                arm_file=arm_file,
            ),
            config=config,
            evidence=evidence,
            label="arm-restore-b-crash",
        )
        arm = parse_json_stdout(arm_result, "arm-restore-b-crash")
        if load_json_object(arm_file, "restore crash arm file") != arm:
            raise WorkflowFailure("restore crash arm stdout/file identities differ")
        run_id = fixed_id(config.seed, "fault-run")
        restore_b_operation_id = validate_fault_arm(
            arm,
            runtime=runtime,
            snapshot_id=snapshot_a_id,
            run_id=run_id,
        )

        before_b = inventory(
            config, runtime, evidence, "inventory-before-b", environment
        )
        fault_deadline = time.monotonic() + config.timeout
        require_owner_session(config, runtime, "initial owner")
        close_mcp(mcp)
        mcp = None
        if stop_process(owner, signal.SIGTERM) is None:
            raise WorkflowFailure("initial owner was absent before fault-owner handoff")
        wait_owner_session_absent(config, runtime, "initial owner", fault_deadline)

        owner = start_process(
            fault_owner_command(
                config,
                runtime,
                arm_file=arm_file,
                evidence_file=barrier_file,
            ),
            config,
            owner_log,
            evidence=evidence,
            label="fault-owner",
        )
        wait_tcp(
            owner,
            runtime.owner_port,
            remaining(fault_deadline, "fault owner startup"),
        )
        fault_owner_socket_ready = True
        evidence.line(
            "processes.jsonl",
            {
                "schema": SCHEMA,
                "label": "fault-owner-socket-ready",
                "pid": owner.pid,
                "address": f"127.0.0.1:{runtime.owner_port}",
                "recorded_at": now(),
            },
        )
        require_owner_session(config, runtime, "fault owner")
        mcp = start_mcp(
            config,
            runtime,
            evidence,
            owner,
            mcp_log,
            deadline=fault_deadline,
        )
        restore_b_step = plan["restore-b"]
        fault_client_error = mcp.call_until_owner_loss(
            "restore-b-pre-complete-crash",
            restore_b_step.operation,
            restore_b_step.arguments,
        )
        if mcp.process.poll() is not None:
            raise WorkflowFailure("MCP exited with the restore crash owner")
        mcp_survived_fault_owner_exit = True
        fault_owner_exit_code = wait_exact_exit(
            owner,
            86,
            remaining(fault_deadline, "restore crash owner exit"),
            "restore crash owner",
        )
        evidence.line(
            "processes.jsonl",
            {
                "schema": SCHEMA,
                "label": "fault-owner-controlled-exit",
                "pid": owner.pid,
                "returncode": fault_owner_exit_code,
                "recorded_at": now(),
            },
        )
        close_mcp(mcp)
        mcp = None

        barrier_envelope = load_json_object(
            barrier_file, "restore crash barrier evidence"
        )
        barrier_summary = validate_fault_barrier_evidence(barrier_envelope, arm)
        after_crash_b = inventory(
            config, runtime, evidence, "inventory-after-restore-b-crash", environment
        )
        b_changed = changed_objects(before_b, after_crash_b)
        if len(b_changed) != 2:
            raise WorkflowFailure(
                "restore B crash boundary did not publish exactly two manifest objects"
            )

        wait_owner_session_absent(config, runtime, "fault owner", fault_deadline)
        owner = start_process(
            successor_owner_command(config, runtime),
            config,
            owner_log,
            evidence=evidence,
            label="successor-owner",
        )
        wait_tcp(
            owner,
            runtime.owner_port,
            remaining(fault_deadline, "successor owner startup"),
        )
        successor_owner_socket_ready = True
        evidence.line(
            "processes.jsonl",
            {
                "schema": SCHEMA,
                "label": "successor-owner-socket-ready",
                "pid": owner.pid,
                "address": f"127.0.0.1:{runtime.owner_port}",
                "recorded_at": now(),
            },
        )
        require_owner_session(config, runtime, "successor owner")
        mcp = start_mcp(
            config,
            runtime,
            evidence,
            owner,
            mcp_log,
            deadline=fault_deadline,
        )

        hidden = mcp.call(
            "find-b-hidden-before-replay",
            "workbench_find",
            {"include_manifest": False, "limit": 100},
        )
        hidden_matches = hidden.get("matches")
        if (
            not isinstance(hidden_matches, list)
            or hidden.get("truncated") is not False
            or hidden.get("next_cursor") is not None
            or any(
                isinstance(value, dict) and value.get("workbench_id") == "composition-b"
                for value in hidden_matches
            )
        ):
            raise WorkflowFailure("restore destination B became visible before replay")

        restore_inspection = parse_json_stdout(
            run(
                fault_inspect_command(config, runtime, restore_b_operation_id),
                config=config,
                evidence=evidence,
                label="inspect-running-restore-b",
            ),
            "inspect-running-restore-b",
        )
        validate_operation_inspection(
            restore_inspection,
            root_id=runtime.root_id,
            operation_id=restore_b_operation_id,
            kind="restore",
            state="running",
        )
        validate_restore_inspection_binding(restore_inspection, arm, barrier_envelope)
        publication_states: list[str] = []
        barrier_value = _mapping(barrier_envelope, "evidence")
        for key, operation_id in zip(
            ("run_manifest", "restore_manifest"),
            barrier_summary["manifest_publication_operation_ids"],
            strict=True,
        ):
            binding = _mapping(barrier_value, key)
            actual_identity = _mapping(_mapping(binding, "actual"), "identity")
            inspection = parse_json_stdout(
                run(
                    fault_inspect_command(config, runtime, operation_id),
                    config=config,
                    evidence=evidence,
                    label=f"inspect-{key}-publication",
                ),
                f"inspect-{key}-publication",
            )
            validate_operation_inspection(
                inspection,
                root_id=runtime.root_id,
                operation_id=operation_id,
                kind="artifact_publish",
                state="succeeded",
                artifact_revision_id=fixed_hex_value(
                    actual_identity.get("artifact_revision_id"),
                    16,
                    f"restore crash {key}.artifact_revision_id",
                ),
            )
            publication_states.append("succeeded")

        pre_replay_inventory = inventory(
            config, runtime, evidence, "inventory-before-restore-b-replay", environment
        )
        if pre_replay_inventory != after_crash_b:
            raise WorkflowFailure(
                "object inventory changed while waiting for successor reopen"
            )
        restore_b = mcp.call(
            restore_b_step.label,
            restore_b_step.operation,
            restore_b_step.arguments,
        )
        if (
            restore_b.get("state") != "complete"
            or restore_b.get("idempotent_replay") is not True
            or restore_b.get("operation_id") != restore_b_operation_id
        ):
            raise WorkflowFailure("restore B exact replay did not converge terminally")
        after_b = inventory(config, runtime, evidence, "inventory-after-b", environment)
        if after_b != pre_replay_inventory:
            raise WorkflowFailure("restore B exact replay changed object inventory")
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
        if (
            b_manifest.get("commit_identity")
            != barrier_summary["destination_commit_id"]
        ):
            raise WorkflowFailure(
                "restore B replay destination commit differs from crash evidence"
            )
        terminal_restore_inspection = parse_json_stdout(
            run(
                fault_inspect_command(config, runtime, restore_b_operation_id),
                config=config,
                evidence=evidence,
                label="inspect-terminal-restore-b",
            ),
            "inspect-terminal-restore-b",
        )
        validate_operation_inspection(
            terminal_restore_inspection,
            root_id=runtime.root_id,
            operation_id=restore_b_operation_id,
            kind="restore",
            state="succeeded",
            destination_workspace_incarnation_id=fixed_hex_value(
                arm.get("destination_workspace_incarnation_id"),
                16,
                "restore crash arm.destination_workspace_incarnation_id",
            ),
            destination_commit_id=barrier_summary["destination_commit_id"],
        )
        validate_restore_inspection_binding(
            terminal_restore_inspection, arm, barrier_envelope
        )
        fault_injection = {
            "status": "PASS",
            "reason": "Qualified exact pre-Complete owner-loss recovery.",
            "arm_schema": arm.get("schema"),
            "evidence_schema": barrier_envelope.get("schema"),
            "run_id": arm.get("run_id"),
            "root_id": fixed_hex_value(
                arm.get("root_id"), 16, "restore crash arm.root_id"
            ),
            "destination_workspace_incarnation_id": fixed_hex_value(
                arm.get("destination_workspace_incarnation_id"),
                16,
                "restore crash arm.destination_workspace_incarnation_id",
            ),
            **barrier_summary,
            "replay_operation_id": restore_b.get("operation_id"),
            "replay_destination_commit_id": b_manifest.get("commit_identity"),
            "destination_generation": restore_b.get("destination_generation"),
            "interruption_label": "restore-b-pre-complete-crash",
            "interrupted_oracle_label": restore_b_step.label,
            "replay_label": restore_b_step.label,
            "owner_exit_code": fault_owner_exit_code,
            "fault_owner_socket_ready": fault_owner_socket_ready,
            "successor_owner_socket_ready": successor_owner_socket_ready,
            "mcp_survived_fault_owner_exit": mcp_survived_fault_owner_exit,
            "initial_owner_session_absent_before_fault": True,
            "fault_owner_session_absent_before_reopen": True,
            "destination_hidden_before_replay": True,
            "operation_state_before_replay": "running",
            "publication_states_before_replay": publication_states,
            "manifest_objects_published_before_crash": len(b_changed),
            "pre_replay_object_inventory_sha256": inventory_digest(
                pre_replay_inventory
            ),
            "post_replay_object_inventory_sha256": inventory_digest(after_b),
            "object_inventory_stable_across_replay": after_b == pre_replay_inventory,
            "idempotent_replay": restore_b.get("idempotent_replay") is True,
            "client_failure": fault_client_error,
        }

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
        if digest_file(config.fault_binary) != fault_binary_sha256:
            raise WorkflowFailure("restore crash owner changed during qualification")

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
            fault_injection=fault_injection,
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
        environment = environment_evidence(config, runtime, owner_record, container)
        validate_environment_evidence(
            environment,
            expected_binary_sha256=binary_sha256,
            expected_fault_binary_sha256=fault_binary_sha256,
        )
        evidence.json("environment.json", environment)
        return record
    finally:
        close_mcp(mcp)
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
        "fault_binary": {
            "path": str(config.fault_binary),
            "sha256": digest_file(config.fault_binary),
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


def validate_environment_evidence(
    value: dict[str, Any],
    *,
    expected_binary_sha256: str,
    expected_fault_binary_sha256: str,
) -> None:
    if value.get("schema") != SCHEMA:
        raise WorkflowFailure("environment evidence uses an unexpected schema")
    binary = value.get("binary")
    fault_binary = value.get("fault_binary")
    if not isinstance(binary, dict) or not isinstance(fault_binary, dict):
        raise WorkflowFailure(
            "environment evidence must bind both executable identities"
        )
    binary_sha256 = binary.get("sha256")
    fault_binary_sha256 = fault_binary.get("sha256")
    if (
        not isinstance(binary.get("path"), str)
        or not binary["path"]
        or not isinstance(fault_binary.get("path"), str)
        or not fault_binary["path"]
        or not isinstance(binary_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", binary_sha256) is None
        or not isinstance(fault_binary_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", fault_binary_sha256) is None
    ):
        raise WorkflowFailure("environment executable identities are malformed")
    if binary_sha256 != expected_binary_sha256:
        raise WorkflowFailure("environment default binary digest drifted")
    if fault_binary_sha256 != expected_fault_binary_sha256:
        raise WorkflowFailure("environment fault owner digest drifted")
    if binary_sha256 == fault_binary_sha256:
        raise WorkflowFailure(
            "default and fault executables must be independently built"
        )


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
            "status": "PLANNED",
            "reason": (
                "Live mode uses the publish=false feature-only owner to exit at the "
                "exact dual-manifest-published pre-Complete boundary; dry-run does not "
                "execute it."
            ),
            "fault_owner": str(config.fault_binary),
            "default_successor": str(config.binary),
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
    parser.add_argument("--qualification-result", type=Path)
    parser.add_argument("--target-dir", type=Path)
    parser.add_argument("--fault-owner-bin", type=Path)
    parser.add_argument("--fault-target-dir", type=Path)
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
    if args.qualification_result is not None and args.dry_run:
        parser.error("--qualification-result cannot be combined with --dry-run")
    resolved_repo = args.repo.resolve()
    target = (args.target_dir or resolved_repo / "target").resolve()
    binary = (args.nokv_bin or target / "debug" / "nokv").resolve()
    fault_target = (
        args.fault_target_dir or target.parent / f"{target.name}-restore-crash"
    ).resolve()
    fault_binary = (
        args.fault_owner_bin or fault_target / "debug" / "nokv-restore-crash-owner"
    ).resolve()
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
        fault_binary=fault_binary,
        fault_target=fault_target,
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
        qualification_result=(
            args.qualification_result.resolve() if args.qualification_result else None
        ),
    )


def main(argv: list[str] | None = None) -> int:
    config = parse_args(argv)
    evidence = Evidence(config.evidence)
    prepared = False
    typed_context = None

    def finish(code: int, record: dict[str, Any]) -> int:
        if typed_context is None or config.qualification_result is None:
            return code
        outcome = "PASS" if code == 0 else "NQ" if code == 3 else "FAIL"
        try:
            publish_live_result(
                result_path=config.qualification_result,
                context=typed_context,
                outcome=outcome,
                qualification=record,
                evidence_roles=TYPED_EVIDENCE_ROLES,
            )
        except (OSError, ProducerError) as error:
            print(f"FAIL: {error}", file=sys.stderr)
            return 2
        return code

    try:
        if config.qualification_result is not None:
            if config.evidence == config.qualification_result.parent:
                raise ProducerError(
                    "gate evidence must not overlap typed direct-child evidence"
                )
            typed_context = load_live_context(
                producer_id="restore-composition",
                scenarios=TYPED_SCENARIOS,
                dependency_names=("etcd", "rustfs"),
                product_binary=config.binary,
                evidence_roles=TYPED_EVIDENCE_ROLES,
            )
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
            return finish(0, record)
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
        return finish(0, record)
    except NotQualified as error:
        record = qualification(
            composition="NOT QUALIFIED", fault="NOT QUALIFIED", reason=str(error)
        )
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
        record = qualification(
            composition="FAIL", fault="NOT QUALIFIED", reason=str(error)
        )
        if prepared:
            evidence.json("qualification.json", record)
        print(json.dumps(record, indent=2, sort_keys=True))
        return finish(2, record)


if __name__ == "__main__":
    raise SystemExit(main())
