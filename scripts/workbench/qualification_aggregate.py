#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Aggregate pre-#423 Workbench contract ledger receipts by stable ID."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

import pre423_contract_ledger as ledger_module
from qualification_receipt import (
    DEPENDENCY_IDENTITIES,
    OUTCOMES,
    PRODUCER_RESULT_SCHEMA,
    RECEIPT_SCHEMA,
    ReceiptError,
    derive_rust_toolchain_subject,
    lexical_final_path,
    tracked_regular_file_identity,
    validate_rust_toolchain_subject,
)


AGGREGATE_SCHEMA = "nokv.pre423.qualification_aggregate.v1"
PRODUCT_ARTIFACT_MANIFEST_SCHEMA = "nokv.pre423.product_artifact_manifest.v1"
PRODUCT_ARTIFACT_PROVIDER = "github-actions"
STATUS_EXIT_CODES = {"PASS": 0, "FAIL": 2, "NQ": 3}
STATUS_PRIORITY = {"PASS": 0, "NQ": 1, "FAIL": 2}
SHA256_IDENTITY_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
ARTIFACT_ID_PATTERN = re.compile(r"^[1-9][0-9]*$")
MAX_PRODUCT_ARTIFACT_MANIFEST_BYTES = 1024 * 1024


class AggregateError(ValueError):
    """The aggregate invocation or receipt bundle is structurally invalid."""


class InvalidReceiptError(ValueError):
    """A current receipt is malformed, policy-invalid, or tampered."""


class RejectedReceiptError(ValueError):
    """A well-formed receipt does not belong to the current qualification."""


@dataclass(frozen=True)
class AggregationResult:
    status: str
    exit_code: int
    report: dict[str, Any]


@dataclass(frozen=True)
class AcceptedReceipt:
    path: str
    stable_id: str
    gate: str
    scenarios: tuple[str, ...]
    producer: str
    workflow_run_id: str
    job: str
    attempt: int
    operation_id: str
    outcome: str
    canonical_sha256: str
    product_artifact: ProductArtifactBinding | None


@dataclass(frozen=True)
class ReceiptEnvelope:
    producer: str
    workflow_run_id: str
    job: str
    attempt: int
    source_sha: str


@dataclass(frozen=True)
class ProductArtifactBinding:
    producer: str
    job: str
    artifact_id: str
    artifact_digest: str
    binary_path: str
    binary_sha256: str


@dataclass(frozen=True)
class ProductArtifactManifest:
    provider: str
    workflow_run_id: str
    workflow_attempt: int
    head_sha: str
    manifest_sha256: str
    artifacts: dict[tuple[str, str], ProductArtifactBinding]


def _nonempty_string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise InvalidReceiptError(f"{field} must be a non-empty string")
    return value


def _string_list(value: Any, field: str) -> list[str]:
    if (
        not isinstance(value, list)
        or not value
        or any(not isinstance(element, str) or not element for element in value)
    ):
        raise InvalidReceiptError(f"{field} must be a non-empty string array")
    if len(value) != len(set(value)):
        raise InvalidReceiptError(f"{field} must not contain duplicates")
    return value


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _valid_sha256(value: Any, field: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise InvalidReceiptError(f"{field} must be a lowercase SHA-256")
    return value


def _valid_source_sha(value: Any, field: str = "source.sha") -> str:
    if (
        not isinstance(value, str)
        or len(value) != 40
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise InvalidReceiptError(f"{field} must be a lowercase full git SHA")
    return value


def _manifest_nonempty_string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise AggregateError(f"{field} must be a non-empty string")
    return value


def _manifest_sha256_identity(value: Any, field: str) -> str:
    if not isinstance(value, str) or not SHA256_IDENTITY_PATTERN.fullmatch(value):
        raise AggregateError(f"{field} must use sha256:<64 lowercase hex>")
    return value


def _read_product_artifact_manifest(path: Path) -> bytes:
    """Read one bounded regular provenance manifest without following a symlink."""

    candidate = Path(os.path.abspath(path))
    try:
        lexical_stat = candidate.lstat()
    except OSError as err:
        raise AggregateError(
            f"external product artifact manifest is unavailable: {err}"
        ) from err
    if not stat.S_ISREG(lexical_stat.st_mode):
        raise AggregateError(
            "external product artifact manifest must be a regular file"
        )
    if lexical_stat.st_size > MAX_PRODUCT_ARTIFACT_MANIFEST_BYTES:
        raise AggregateError("external product artifact manifest exceeds 1 MiB")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(candidate, flags)
    except OSError as err:
        raise AggregateError(
            f"cannot safely open external product artifact manifest: {err}"
        ) from err
    chunks: list[bytes] = []
    try:
        opened_stat = os.fstat(descriptor)
        if not stat.S_ISREG(opened_stat.st_mode) or (
            opened_stat.st_dev,
            opened_stat.st_ino,
        ) != (lexical_stat.st_dev, lexical_stat.st_ino):
            raise AggregateError(
                "external product artifact manifest changed while being opened"
            )
        size = 0
        while chunk := os.read(descriptor, 64 * 1024):
            size += len(chunk)
            if size > MAX_PRODUCT_ARTIFACT_MANIFEST_BYTES:
                raise AggregateError("external product artifact manifest exceeds 1 MiB")
            chunks.append(chunk)
    finally:
        os.close(descriptor)
    try:
        final_stat = candidate.lstat()
    except OSError as err:
        raise AggregateError(
            f"external product artifact manifest disappeared after reading: {err}"
        ) from err
    if (
        final_stat.st_dev,
        final_stat.st_ino,
        final_stat.st_size,
        final_stat.st_mtime_ns,
    ) != (
        lexical_stat.st_dev,
        lexical_stat.st_ino,
        lexical_stat.st_size,
        lexical_stat.st_mtime_ns,
    ):
        raise AggregateError(
            "external product artifact manifest changed while being read"
        )
    return b"".join(chunks)


def _closed_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, child in pairs:
        if key in value:
            raise AggregateError(
                f"external product artifact manifest duplicates JSON key {key!r}"
            )
        value[key] = child
    return value


def _load_product_artifact_manifest(
    path: Path,
    *,
    ledger: dict[str, Any],
    source_sha: str,
    workflow_run_id: str | None,
    workflow_attempt: int | None,
    repo: Path,
    receipt_bundle: Path,
) -> ProductArtifactManifest:
    """Validate one closed claim emitted by the external artifact trust boundary."""

    candidate = Path(os.path.abspath(path))
    resolved_candidate = candidate.parent.resolve() / candidate.name
    for forbidden_root, label in (
        (repo.resolve(), "checkout"),
        (receipt_bundle, "receipt bundle"),
    ):
        try:
            resolved_candidate.relative_to(forbidden_root.resolve())
        except ValueError:
            continue
        raise AggregateError(
            f"external product artifact manifest must not be inside the {label}"
        )
    payload = _read_product_artifact_manifest(candidate)
    try:
        value = json.loads(
            payload.decode("utf-8"), object_pairs_hook=_closed_json_object
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as err:
        raise AggregateError(
            f"external product artifact manifest is not valid UTF-8 JSON: {err}"
        ) from err
    expected_fields = {
        "schema",
        "provider",
        "workflow_run_id",
        "workflow_attempt",
        "head_sha",
        "artifacts",
    }
    if not isinstance(value, dict) or set(value) != expected_fields:
        raise AggregateError(
            "external product artifact manifest must use the exact closed schema"
        )
    if value.get("schema") != PRODUCT_ARTIFACT_MANIFEST_SCHEMA:
        raise AggregateError(
            "external product artifact manifest has an unsupported schema"
        )
    provider = _manifest_nonempty_string(value.get("provider"), "manifest.provider")
    if provider != PRODUCT_ARTIFACT_PROVIDER:
        raise AggregateError(f"manifest.provider must be {PRODUCT_ARTIFACT_PROVIDER!r}")
    manifest_run_id = _manifest_nonempty_string(
        value.get("workflow_run_id"), "manifest.workflow_run_id"
    )
    manifest_attempt = value.get("workflow_attempt")
    if (
        not isinstance(manifest_attempt, int)
        or isinstance(manifest_attempt, bool)
        or manifest_attempt < 1
    ):
        raise AggregateError("manifest.workflow_attempt must be a positive integer")
    try:
        manifest_head = _valid_source_sha(value.get("head_sha"), "manifest.head_sha")
    except InvalidReceiptError as err:
        raise AggregateError(str(err)) from err
    if manifest_head != source_sha:
        raise AggregateError(
            "external product artifact manifest head does not match current source"
        )
    if workflow_run_id is not None and manifest_run_id != workflow_run_id:
        raise AggregateError(
            "external product artifact manifest run does not match the expected run"
        )
    if workflow_attempt is not None and manifest_attempt != workflow_attempt:
        raise AggregateError(
            "external product artifact manifest attempt does not match the expected attempt"
        )
    raw_artifacts = value.get("artifacts")
    if (
        not isinstance(raw_artifacts, list)
        or not raw_artifacts
        or len(raw_artifacts) > 64
    ):
        raise AggregateError("manifest.artifacts must be a non-empty bounded array")
    artifacts: dict[tuple[str, str], ProductArtifactBinding] = {}
    artifact_fields = {
        "producer",
        "job",
        "artifact_id",
        "artifact_digest",
        "binary_path",
        "binary_sha256",
    }
    producer_catalog = ledger["producer_catalog"]
    for index, entry in enumerate(raw_artifacts):
        field = f"manifest.artifacts[{index}]"
        if not isinstance(entry, dict) or set(entry) != artifact_fields:
            raise AggregateError(f"{field} must use the exact closed artifact schema")
        producer = _manifest_nonempty_string(entry.get("producer"), f"{field}.producer")
        job = _manifest_nonempty_string(entry.get("job"), f"{field}.job")
        producer_contract = producer_catalog.get(producer)
        if (
            not isinstance(producer_contract, dict)
            or "product_binary" not in producer_contract["required_subjects"]
            or "live" not in producer_contract["evidence_kinds"]
        ):
            raise AggregateError(
                f"{field}.producer must name a catalogued live product producer"
            )
        artifact_id = _manifest_nonempty_string(
            entry.get("artifact_id"), f"{field}.artifact_id"
        )
        if not ARTIFACT_ID_PATTERN.fullmatch(artifact_id):
            raise AggregateError(
                f"{field}.artifact_id must be a positive decimal string"
            )
        artifact_digest = _manifest_sha256_identity(
            entry.get("artifact_digest"), f"{field}.artifact_digest"
        )
        binary_sha256 = _manifest_sha256_identity(
            entry.get("binary_sha256"), f"{field}.binary_sha256"
        )
        binary_path = _manifest_nonempty_string(
            entry.get("binary_path"), f"{field}.binary_path"
        )
        relative_binary = Path(binary_path)
        if (
            "\x00" in binary_path
            or relative_binary.is_absolute()
            or ".." in relative_binary.parts
            or binary_path != relative_binary.as_posix()
            or binary_path == "."
        ):
            raise AggregateError(
                f"{field}.binary_path must be a canonical relative artifact path"
            )
        key = (producer, job)
        if key in artifacts:
            raise AggregateError(
                "external product artifact manifest has a duplicate producer/job mapping"
            )
        artifacts[key] = ProductArtifactBinding(
            producer=producer,
            job=job,
            artifact_id=artifact_id,
            artifact_digest=artifact_digest,
            binary_path=binary_path,
            binary_sha256=binary_sha256,
        )
    return ProductArtifactManifest(
        provider=provider,
        workflow_run_id=manifest_run_id,
        workflow_attempt=manifest_attempt,
        head_sha=manifest_head,
        manifest_sha256=_sha256(payload),
        artifacts=artifacts,
    )


def _status_worst(statuses: Sequence[str]) -> str:
    if not statuses:
        return "NQ"
    return max(statuses, key=STATUS_PRIORITY.__getitem__)


def _read_bundle_evidence(
    *, bundle_dir: Path, relative_path: Path, receipt_name: str
) -> bytes:
    directory_flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    file_flags = (
        os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    )
    descriptors: list[int] = []
    try:
        current = os.open(bundle_dir, directory_flags)
        descriptors.append(current)
        for component in relative_path.parts[:-1]:
            current = os.open(component, directory_flags, dir_fd=current)
            descriptors.append(current)
        file_descriptor = os.open(relative_path.parts[-1], file_flags, dir_fd=current)
        descriptors.append(file_descriptor)
        opened_stat = os.fstat(file_descriptor)
        if not stat.S_ISREG(opened_stat.st_mode):
            raise InvalidReceiptError(
                f"{receipt_name}: evidence must be a regular file: {relative_path}"
            )
        chunks: list[bytes] = []
        while chunk := os.read(file_descriptor, 1024 * 1024):
            chunks.append(chunk)
        return b"".join(chunks)
    except OSError as err:
        raise InvalidReceiptError(
            f"{receipt_name}: cannot safely open evidence {relative_path}: {err}"
        ) from err
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def _validate_evidence(
    evidence: Any, *, bundle_dir: Path, receipt_path: Path
) -> dict[str, bytes]:
    if not isinstance(evidence, list) or not evidence:
        raise InvalidReceiptError("evidence must be a non-empty array")
    seen_roles: set[str] = set()
    seen_paths: set[str] = set()
    payload_by_role: dict[str, bytes] = {}
    for index, entry in enumerate(evidence):
        field = f"evidence[{index}]"
        if not isinstance(entry, dict):
            raise InvalidReceiptError(f"{field} must be an object")
        role = _nonempty_string(entry.get("role"), f"{field}.role")
        if role in seen_roles:
            raise InvalidReceiptError(f"evidence role {role!r} is duplicated")
        seen_roles.add(role)
        raw_path = _nonempty_string(entry.get("path"), f"{field}.path")
        relative_path = Path(raw_path)
        if (
            "\x00" in raw_path
            or not relative_path.parts
            or relative_path.is_absolute()
            or ".." in relative_path.parts
            or raw_path != relative_path.as_posix()
        ):
            raise InvalidReceiptError(
                f"{field}.path must be a canonical relative path in the receipt bundle"
            )
        if raw_path in seen_paths:
            raise InvalidReceiptError(f"evidence path {raw_path!r} is duplicated")
        seen_paths.add(raw_path)
        expected_digest = _valid_sha256(entry.get("sha256"), f"{field}.sha256")
        expected_size = entry.get("size_bytes")
        if (
            not isinstance(expected_size, int)
            or isinstance(expected_size, bool)
            or expected_size < 0
        ):
            raise InvalidReceiptError(f"{field}.size_bytes must be non-negative")
        _nonempty_string(entry.get("media_type"), f"{field}.media_type")
        payload = _read_bundle_evidence(
            bundle_dir=bundle_dir,
            relative_path=relative_path,
            receipt_name=receipt_path.name,
        )
        if len(payload) != expected_size or _sha256(payload) != expected_digest:
            raise InvalidReceiptError(
                f"{receipt_path.name}: evidence hash or size mismatch for {raw_path}"
            )
        payload_by_role[role] = payload
    if not {"stdout", "stderr"}.issubset(seen_roles):
        raise InvalidReceiptError("evidence must contain stdout and stderr roles")
    return payload_by_role


def _validate_subjects(
    subjects: Any,
    producer_contract: dict[str, Any],
    current_rust_toolchain: dict[str, dict[str, str]] | None = None,
) -> dict[str, Any]:
    if not isinstance(subjects, dict):
        raise InvalidReceiptError("subjects must be an object")
    allowed_keys = {"dependencies", "product_binary", "rust_toolchain"}
    if set(subjects) - allowed_keys:
        raise InvalidReceiptError("subjects contains unknown fields")
    dependencies = subjects.get("dependencies")
    if not isinstance(dependencies, list):
        raise InvalidReceiptError("subjects.dependencies must be an array")
    dependency_names: list[str] = []
    for index, dependency in enumerate(dependencies):
        if not isinstance(dependency, dict):
            raise InvalidReceiptError(
                f"subjects.dependencies[{index}] must be an object"
            )
        dependency_names.append(
            _nonempty_string(
                dependency.get("name"), f"subjects.dependencies[{index}].name"
            )
        )
        identity = _nonempty_string(
            dependency.get("identity"),
            f"subjects.dependencies[{index}].identity",
        )
        if set(dependency) != {"name", "identity"}:
            raise InvalidReceiptError(
                f"subjects.dependencies[{index}] keys must be name and identity"
            )
        required_dependencies = producer_contract["required_dependencies"]
        if dependency_names[-1] not in required_dependencies:
            raise InvalidReceiptError(
                f"unexpected dependency identity {dependency_names[-1]!r}"
            )
        if not any(
            DEPENDENCY_IDENTITIES[kind].fullmatch(identity)
            for kind in required_dependencies[dependency_names[-1]]
        ):
            raise InvalidReceiptError(
                f"dependency {dependency_names[-1]!r} is not pinned by an allowed "
                "identity kind"
            )
    if len(dependency_names) != len(set(dependency_names)):
        raise InvalidReceiptError("subjects.dependencies names must be unique")
    expected_dependency_names = set(producer_contract["required_dependencies"])
    if set(dependency_names) != expected_dependency_names:
        raise InvalidReceiptError(
            "subjects.dependencies names must exactly match producer contract"
        )

    product_binary = subjects.get("product_binary")
    if product_binary is not None:
        if not isinstance(product_binary, dict):
            raise InvalidReceiptError("subjects.product_binary must be an object")
        if set(product_binary) != {"path", "sha256"}:
            raise InvalidReceiptError(
                "subjects.product_binary keys must be path and sha256"
            )
        _nonempty_string(product_binary.get("path"), "subjects.product_binary.path")
        _valid_sha256(product_binary.get("sha256"), "subjects.product_binary.sha256")
    required_subjects = set(producer_contract["required_subjects"])
    if (product_binary is not None) != ("product_binary" in required_subjects):
        raise InvalidReceiptError(
            "product binary subject does not match producer contract"
        )
    if bool(dependencies) != ("dependencies" in required_subjects):
        raise InvalidReceiptError("dependency subjects do not match producer contract")
    rust_toolchain = subjects.get("rust_toolchain")
    if (rust_toolchain is not None) != ("rust_toolchain" in required_subjects):
        raise InvalidReceiptError(
            "Rust toolchain subject does not match producer contract"
        )
    if rust_toolchain is not None:
        if current_rust_toolchain is None:
            try:
                current_rust_toolchain = validate_rust_toolchain_subject(rust_toolchain)
            except ReceiptError as err:
                raise InvalidReceiptError(str(err)) from err
        elif rust_toolchain != current_rust_toolchain:
            raise InvalidReceiptError(
                "rust_toolchain identity does not match the current host"
            )
    return subjects


def _validate_product_artifact_binding(
    *,
    subjects: dict[str, Any],
    producer: str,
    job: str,
    workflow_run_id: str,
    attempt: int,
    source_sha: str,
    manifest: ProductArtifactManifest | None,
) -> ProductArtifactBinding | None:
    product_binary = subjects.get("product_binary")
    if product_binary is None:
        return None
    if manifest is None:
        raise RejectedReceiptError(
            "external product artifact manifest is required for live receipts"
        )
    if (
        workflow_run_id != manifest.workflow_run_id
        or attempt != manifest.workflow_attempt
        or source_sha != manifest.head_sha
    ):
        raise RejectedReceiptError(
            "live receipt run, attempt, or source does not match the external "
            "product artifact manifest"
        )
    binding = manifest.artifacts.get((producer, job))
    if binding is None:
        raise RejectedReceiptError(
            "external product artifact manifest has no mapping for live "
            f"producer {producer!r} job {job!r}"
        )
    binary_digest = product_binary["sha256"]
    if binding.binary_sha256 != f"sha256:{binary_digest}":
        raise InvalidReceiptError(
            "receipt product binary digest disagrees with external artifact provenance"
        )
    binary_location = Path(product_binary["path"])
    if (
        not binary_location.is_absolute()
        or Path(os.path.abspath(binary_location)) != binary_location
        or binary_location.name != Path(binding.binary_path).name
    ):
        raise InvalidReceiptError(
            "receipt product binary path disagrees with external artifact provenance"
        )
    return binding


def _command_path(argument: str, cwd: str) -> Path:
    path = Path(argument)
    if not path.is_absolute():
        path = Path(cwd) / path
    return path.resolve(strict=False)


def _regular_file_sha256(path: Path, field: str) -> str:
    try:
        lexical_stat = path.lstat()
    except OSError as err:
        raise InvalidReceiptError(f"{field} is unavailable: {err}") from err
    if not stat.S_ISREG(lexical_stat.st_mode):
        raise InvalidReceiptError(f"{field} must be a regular file")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as err:
        raise InvalidReceiptError(f"cannot safely open {field}: {err}") from err
    try:
        opened_stat = os.fstat(descriptor)
        if not stat.S_ISREG(opened_stat.st_mode) or (
            opened_stat.st_dev,
            opened_stat.st_ino,
        ) != (lexical_stat.st_dev, lexical_stat.st_ino):
            raise InvalidReceiptError(f"{field} changed while being validated")
        digest = hashlib.sha256()
        while chunk := os.read(descriptor, 1024 * 1024):
            digest.update(chunk)
    finally:
        os.close(descriptor)
    return digest.hexdigest()


def _validate_command_manifest(
    *,
    execution: dict[str, Any],
    argv: list[str],
    producer: str,
    producer_contract: dict[str, Any],
    subjects: dict[str, Any],
    repo: Path,
) -> None:
    command_contract = producer_contract["command"]
    expected_contract_digest = ledger_module.json_sha256(producer_contract)
    if (
        _valid_sha256(
            execution.get("command_contract_sha256"),
            "execution.command_contract_sha256",
        )
        != expected_contract_digest
    ):
        raise InvalidReceiptError("producer command contract hash is stale")
    argv_digest = _valid_sha256(
        execution.get("command_argv_sha256"),
        "execution.command_argv_sha256",
    )
    if argv_digest != ledger_module.json_sha256(argv):
        raise InvalidReceiptError("execution argv hash does not match argv")
    if len(argv) < 2:
        raise InvalidReceiptError(
            f"producer {producer!r} lacks its source-bound Python entrypoint"
        )
    cwd = _nonempty_string(execution.get("cwd"), "execution.cwd")
    resolved_repo = repo.resolve()
    if Path(cwd).resolve(strict=False) != resolved_repo:
        raise InvalidReceiptError("execution cwd does not match the qualified checkout")
    executable = _nonempty_string(execution.get("executable"), "execution.executable")
    executable_sha256 = _valid_sha256(
        execution.get("executable_sha256"), "execution.executable_sha256"
    )
    if _command_path(argv[0], cwd) != Path(executable):
        raise InvalidReceiptError(
            "execution executable is not bound to command argv[0]"
        )
    if (
        _regular_file_sha256(Path(executable), "execution executable")
        != executable_sha256
    ):
        raise InvalidReceiptError(
            "execution executable hash does not match current file"
        )
    entrypoint = _nonempty_string(execution.get("entrypoint"), "execution.entrypoint")
    if entrypoint != command_contract["entrypoint"]:
        raise InvalidReceiptError(
            "execution entrypoint does not match producer contract"
        )
    entrypoint_sha256 = _valid_sha256(
        execution.get("entrypoint_sha256"), "execution.entrypoint_sha256"
    )
    try:
        current_entrypoint, current_entrypoint_sha256 = tracked_regular_file_identity(
            resolved_repo, entrypoint
        )
    except ReceiptError as err:
        raise InvalidReceiptError(str(err)) from err
    actual_entrypoint = Path(argv[1])
    actual_entrypoint = lexical_final_path(actual_entrypoint, resolved_repo)
    if actual_entrypoint != current_entrypoint:
        raise InvalidReceiptError(
            "command argv does not execute the declared entrypoint"
        )
    if current_entrypoint_sha256 != entrypoint_sha256:
        raise InvalidReceiptError(
            "producer entrypoint hash does not match current file"
        )
    for forbidden in command_contract["forbidden_arguments"]:
        if forbidden in argv[2:]:
            raise InvalidReceiptError(
                f"producer command contains forbidden argument {forbidden}"
            )
    result_argument = command_contract["result_argument"]
    result_positions = [
        index
        for index, argument in enumerate(argv[2:], start=2)
        if argument == result_argument
    ]
    if len(result_positions) != 1 or result_positions[0] + 1 >= len(argv):
        raise InvalidReceiptError(
            f"producer command must contain exactly one {result_argument} PATH"
        )
    producer_result_source_path = _nonempty_string(
        execution.get("producer_result_source_path"),
        "execution.producer_result_source_path",
    )
    if _command_path(argv[result_positions[0] + 1], cwd) != Path(
        producer_result_source_path
    ):
        raise InvalidReceiptError(
            "producer result source path is not bound to command argv"
        )
    binary_argument = command_contract["binary_argument"]
    if binary_argument is not None:
        binary_positions = [
            index
            for index, argument in enumerate(argv[2:], start=2)
            if argument == binary_argument
        ]
        if len(binary_positions) != 1 or binary_positions[0] + 1 >= len(argv):
            raise InvalidReceiptError(
                f"producer command must contain exactly one {binary_argument} PATH"
            )
        product_binary = subjects.get("product_binary")
        if not isinstance(product_binary, dict) or _command_path(
            argv[binary_positions[0] + 1], cwd
        ) != Path(product_binary["path"]):
            raise InvalidReceiptError(
                "producer product binary argument is not bound to receipt subject"
            )


def _validate_producer_result(
    *,
    payload: bytes,
    execution: dict[str, Any],
    producer: str,
    evidence_kind: str,
    source_sha: str,
    subjects: dict[str, Any],
    receipt_scenarios: list[str],
    evidence_roles: set[str],
    required_evidence_roles: set[str],
) -> None:
    try:
        result = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as err:
        raise InvalidReceiptError(
            f"producer-result evidence is not valid UTF-8 JSON: {err}"
        ) from err
    if not isinstance(result, dict):
        raise InvalidReceiptError("producer-result evidence must be an object")
    expected_fields = {
        "schema",
        "producer",
        "evidence_kind",
        "operation_id",
        "source_sha",
        "command_argv_sha256",
        "subjects",
        "subjects_sha256",
        "scenarios",
    }
    if set(result) != expected_fields:
        raise InvalidReceiptError("producer-result evidence has unexpected fields")
    expected_scalars = {
        "schema": PRODUCER_RESULT_SCHEMA,
        "producer": producer,
        "evidence_kind": evidence_kind,
        "operation_id": execution["operation_id"],
        "source_sha": source_sha,
        "command_argv_sha256": execution["command_argv_sha256"],
        "subjects_sha256": ledger_module.json_sha256(subjects),
    }
    for field, expected in expected_scalars.items():
        if result.get(field) != expected:
            raise InvalidReceiptError(
                f"producer-result {field} does not match receipt context"
            )
    if result.get("subjects") != subjects:
        raise InvalidReceiptError(
            "producer-result subjects do not match receipt subjects"
        )
    recorded_digest = _valid_sha256(
        execution.get("producer_result_sha256"),
        "execution.producer_result_sha256",
    )
    if recorded_digest != ledger_module.json_sha256(result):
        raise InvalidReceiptError("producer-result canonical hash does not match")
    scenarios = result.get("scenarios")
    if not isinstance(scenarios, dict) or not set(receipt_scenarios).issubset(
        scenarios
    ):
        raise InvalidReceiptError(
            "producer-result does not cover every receipt scenario"
        )
    exit_code = execution["exit_code"]
    command_outcome = "PASS" if exit_code == 0 else "NQ" if exit_code == 3 else "FAIL"
    for scenario in receipt_scenarios:
        scenario_result = scenarios[scenario]
        if not isinstance(scenario_result, dict) or set(scenario_result) != {
            "outcome",
            "evidence_roles",
        }:
            raise InvalidReceiptError(
                f"producer-result scenario {scenario!r} has invalid shape"
            )
        if scenario_result.get("outcome") != command_outcome:
            raise InvalidReceiptError(
                f"producer-result scenario {scenario!r} disagrees with command exit"
            )
        roles = scenario_result.get("evidence_roles")
        if (
            not isinstance(roles, list)
            or any(not isinstance(role, str) for role in roles)
            or len(roles) != len(set(roles))
            or not required_evidence_roles.issubset(roles)
            or not set(roles).issubset(evidence_roles)
        ):
            raise InvalidReceiptError(
                f"producer-result scenario {scenario!r} does not bind required evidence"
            )


def _receipt_envelope(value: Any) -> ReceiptEnvelope | None:
    if not isinstance(value, dict):
        return None
    source = value.get("source")
    execution = value.get("execution")
    if not isinstance(source, dict) or not isinstance(execution, dict):
        return None
    producer = execution.get("producer")
    workflow_run_id = execution.get("workflow_run_id")
    job = execution.get("job")
    attempt = execution.get("attempt")
    source_sha = source.get("sha")
    if (
        not isinstance(producer, str)
        or not producer
        or not isinstance(workflow_run_id, str)
        or not workflow_run_id
        or not isinstance(job, str)
        or not job
        or not isinstance(attempt, int)
        or isinstance(attempt, bool)
        or attempt < 1
        or not isinstance(source_sha, str)
    ):
        return None
    return ReceiptEnvelope(producer, workflow_run_id, job, attempt, source_sha)


def _validate_receipt(
    *,
    value: Any,
    receipt_path: Path,
    bundle_dir: Path,
    ledger: dict[str, Any],
    item_by_id: dict[str, dict[str, Any]],
    source_sha: str,
    workflow_run_id: str | None,
    workflow_attempt: int | None,
    repo: Path,
    current_rust_toolchain: dict[str, dict[str, str]] | None,
    product_artifact_manifest: ProductArtifactManifest | None,
) -> AcceptedReceipt:
    if not isinstance(value, dict):
        raise InvalidReceiptError("receipt must be a JSON object")
    if value.get("schema") != RECEIPT_SCHEMA:
        raise InvalidReceiptError(f"receipt must use schema {RECEIPT_SCHEMA}")

    stable_id = _nonempty_string(value.get("stable_id"), "stable_id")
    gate = _nonempty_string(value.get("gate"), "gate")
    if stable_id not in item_by_id:
        raise InvalidReceiptError(f"unknown stable id {stable_id!r}")
    item = item_by_id[stable_id]
    if gate not in item["required_gates"]:
        raise InvalidReceiptError(
            f"gate {gate!r} is not required for stable id {stable_id}"
        )
    expectation = ledger_module.resolve_gate_expectation(ledger, stable_id, gate)

    source = value.get("source")
    if not isinstance(source, dict):
        raise InvalidReceiptError("source must be an object")
    _nonempty_string(source.get("repository"), "source.repository")
    receipt_source_sha = _valid_source_sha(source.get("sha"))
    dirty = source.get("dirty")
    if not isinstance(dirty, bool):
        raise InvalidReceiptError("source.dirty must be boolean")
    item_digest = _valid_sha256(
        source.get("ledger_item_sha256"), "source.ledger_item_sha256"
    )
    expectation_digest = _valid_sha256(
        source.get("gate_expectation_sha256"),
        "source.gate_expectation_sha256",
    )
    policy_digest = _valid_sha256(
        source.get("qualification_policy_sha256"),
        "source.qualification_policy_sha256",
    )
    if receipt_source_sha != source_sha:
        raise RejectedReceiptError(
            f"source SHA {receipt_source_sha} does not match current {source_sha}"
        )
    if dirty:
        raise RejectedReceiptError("receipt was produced from a dirty source tree")
    if item_digest != ledger_module.json_sha256(item):
        raise RejectedReceiptError("ledger item hash is stale")
    if expectation_digest != ledger_module.json_sha256(expectation):
        raise RejectedReceiptError("gate expectation hash is stale")
    if policy_digest != ledger_module.QUALIFICATION_POLICY_SHA256:
        raise RejectedReceiptError("qualification policy hash is stale")

    execution = value.get("execution")
    if not isinstance(execution, dict):
        raise InvalidReceiptError("execution must be an object")
    producer = _nonempty_string(execution.get("producer"), "execution.producer")
    if producer not in ledger["producer_catalog"]:
        raise InvalidReceiptError(f"unknown producer {producer!r}")
    producer_contract = ledger["producer_catalog"][producer]
    receipt_workflow_run_id = _nonempty_string(
        execution.get("workflow_run_id"), "execution.workflow_run_id"
    )
    job = _nonempty_string(execution.get("job"), "execution.job")
    attempt = execution.get("attempt")
    if not isinstance(attempt, int) or isinstance(attempt, bool) or attempt < 1:
        raise InvalidReceiptError("execution.attempt must be a positive integer")
    argv = _string_list(execution.get("argv"), "execution.argv")
    if any("\x00" in argument for argument in argv):
        raise InvalidReceiptError("execution.argv must not contain NUL")
    _nonempty_string(execution.get("cwd"), "execution.cwd")
    _nonempty_string(execution.get("started_at"), "execution.started_at")
    _nonempty_string(execution.get("finished_at"), "execution.finished_at")
    exit_code = execution.get("exit_code")
    if not isinstance(exit_code, int) or isinstance(exit_code, bool):
        raise InvalidReceiptError("execution.exit_code must be an integer")
    if workflow_run_id is not None and receipt_workflow_run_id != workflow_run_id:
        raise RejectedReceiptError(
            f"workflow run {receipt_workflow_run_id!r} does not match "
            f"{workflow_run_id!r}"
        )
    if workflow_attempt is not None and attempt != workflow_attempt:
        raise RejectedReceiptError(
            f"workflow attempt {attempt} does not match {workflow_attempt}"
        )

    evidence_kind = _nonempty_string(value.get("evidence_kind"), "evidence_kind")
    if evidence_kind not in expectation["allowed_evidence_kinds"]:
        raise InvalidReceiptError(
            f"evidence kind {evidence_kind!r} is not allowed for {stable_id}:{gate}"
        )
    if evidence_kind not in producer_contract["evidence_kinds"]:
        raise InvalidReceiptError(
            f"producer {producer!r} cannot emit {evidence_kind!r} evidence"
        )
    if producer not in expectation["allowed_producers"]:
        raise InvalidReceiptError(
            f"producer {producer!r} is not allowed for {stable_id}:{gate}"
        )
    scenarios = _string_list(value.get("scenario_ids"), "scenario_ids")
    unknown_scenarios = set(scenarios) - set(expectation["scenarios"])
    if unknown_scenarios:
        raise InvalidReceiptError(
            f"receipt claims undeclared scenarios {sorted(unknown_scenarios)}"
        )

    outcome = value.get("outcome")
    if outcome not in OUTCOMES:
        raise InvalidReceiptError(f"outcome must be one of {sorted(OUTCOMES)}")
    qualification_errors = value.get("qualification_errors")
    if qualification_errors is not None:
        _string_list(qualification_errors, "qualification_errors")
    if outcome == "PASS" and (exit_code != 0 or qualification_errors):
        raise InvalidReceiptError("PASS receipt must bind an exact zero exit")
    if outcome == "NQ" and (exit_code != 3 or qualification_errors):
        raise InvalidReceiptError("NQ receipt must bind exact exit 3")
    if outcome == "FAIL" and exit_code in {0, 3} and not qualification_errors:
        raise InvalidReceiptError(
            "FAIL receipt with exit 0 or 3 requires qualification_errors"
        )

    subjects = _validate_subjects(
        value.get("subjects"), producer_contract, current_rust_toolchain
    )
    operation_id = _nonempty_string(
        execution.get("operation_id"), "execution.operation_id"
    )
    _validate_command_manifest(
        execution=execution,
        argv=argv,
        producer=producer,
        producer_contract=producer_contract,
        subjects=subjects,
        repo=repo,
    )
    evidence_payloads = _validate_evidence(
        value.get("evidence"),
        bundle_dir=bundle_dir,
        receipt_path=receipt_path,
    )
    required_roles = set(producer_contract["required_evidence_roles"])
    missing_roles = required_roles - set(evidence_payloads)
    if missing_roles:
        raise InvalidReceiptError(
            f"producer {producer!r} lacks required evidence roles "
            f"{sorted(missing_roles)}"
        )
    _validate_producer_result(
        payload=evidence_payloads["producer-result"],
        execution=execution,
        producer=producer,
        evidence_kind=evidence_kind,
        source_sha=receipt_source_sha,
        subjects=subjects,
        receipt_scenarios=scenarios,
        evidence_roles=set(evidence_payloads),
        required_evidence_roles=required_roles,
    )
    product_artifact = _validate_product_artifact_binding(
        subjects=subjects,
        producer=producer,
        job=job,
        workflow_run_id=receipt_workflow_run_id,
        attempt=attempt,
        source_sha=receipt_source_sha,
        manifest=product_artifact_manifest,
    )
    return AcceptedReceipt(
        path=str(receipt_path),
        stable_id=stable_id,
        gate=gate,
        scenarios=tuple(scenarios),
        producer=producer,
        workflow_run_id=receipt_workflow_run_id,
        job=job,
        attempt=attempt,
        operation_id=operation_id,
        outcome=outcome,
        canonical_sha256=ledger_module.json_sha256(value),
        product_artifact=product_artifact,
    )


def _selected_receipt_report(receipt: AcceptedReceipt) -> dict[str, Any]:
    selected: dict[str, Any] = {
        "producer": receipt.producer,
        "job": receipt.job,
        "attempt": receipt.attempt,
        "operation_id": receipt.operation_id,
        "outcome": receipt.outcome,
        "receipt": receipt.path,
    }
    if receipt.product_artifact is not None:
        binding = receipt.product_artifact
        selected["product_artifact"] = {
            "artifact_id": binding.artifact_id,
            "artifact_digest": binding.artifact_digest,
            "binary_path": binding.binary_path,
            "binary_sha256": binding.binary_sha256,
        }
    return selected


def _scenario_status(
    receipts: Sequence[AcceptedReceipt],
) -> tuple[str, list[dict[str, Any]], list[str]]:
    conflicts: list[str] = []
    grouped: dict[str, list[AcceptedReceipt]] = {}
    for receipt in receipts:
        grouped.setdefault(receipt.producer, []).append(receipt)
    selected_receipts: list[AcceptedReceipt] = []
    for producer, candidates in grouped.items():
        if len(candidates) != 1:
            conflicts.append(
                f"producer {producer!r} supplied {len(candidates)} receipts for one "
                "scenario; exactly one is allowed "
                f"at attempt {candidates[0].attempt}; "
                f"jobs={sorted({candidate.job for candidate in candidates})}; "
                "operation_ids="
                f"{sorted({candidate.operation_id for candidate in candidates})}"
            )
            continue
        selected_receipts.append(candidates[0])

    selected = [
        _selected_receipt_report(receipt)
        for receipt in sorted(
            selected_receipts,
            key=lambda candidate: (
                candidate.producer,
                candidate.job,
                candidate.operation_id,
                candidate.path,
            ),
        )
    ]
    if conflicts:
        return "FAIL", selected, conflicts
    if not selected:
        return "NQ", selected, conflicts
    status = _status_worst([entry["outcome"] for entry in selected])
    return status, selected, conflicts


def aggregate_receipts(
    *,
    ledger: dict[str, Any],
    receipt_dir: Path,
    source_sha: str,
    repo: Path,
    workflow_run_id: str | None = None,
    workflow_attempt: int | None = None,
    product_artifact_manifest: Path | None = None,
) -> AggregationResult:
    """Validate all receipts and aggregate every required scenario fail closed."""

    ledger_module.validate_ledger(ledger)
    try:
        _valid_source_sha(source_sha, "current source SHA")
    except InvalidReceiptError as err:
        raise AggregateError(str(err)) from err
    if workflow_attempt is not None and workflow_attempt < 1:
        raise AggregateError("workflow attempt must be positive")
    resolved_repo = repo.resolve()
    current_source_sha, source_dirty = _git_source_identity(resolved_repo)
    if current_source_sha != source_sha:
        raise AggregateError(
            f"current checkout SHA {current_source_sha} does not match {source_sha}"
        )
    if source_dirty:
        raise AggregateError(
            "current checkout is dirty; qualification aggregation requires a clean "
            "source tree"
        )

    resolved_receipt_dir = receipt_dir.resolve()
    bundle_dir = resolved_receipt_dir.parent
    external_product_artifacts = (
        _load_product_artifact_manifest(
            product_artifact_manifest,
            ledger=ledger,
            source_sha=source_sha,
            workflow_run_id=workflow_run_id,
            workflow_attempt=workflow_attempt,
            repo=resolved_repo,
            receipt_bundle=bundle_dir,
        )
        if product_artifact_manifest is not None
        else None
    )

    current_rust_toolchain: dict[str, dict[str, str]] | None = None
    if any(
        "rust_toolchain" in producer["required_subjects"]
        for producer in ledger["producer_catalog"].values()
    ):
        try:
            current_rust_toolchain = derive_rust_toolchain_subject(repo=resolved_repo)
        except ReceiptError as err:
            raise AggregateError(f"cannot bind the Rust toolchain: {err}") from err

    item_by_id = {item["id"]: item for item in ledger["items"]}
    accepted: list[AcceptedReceipt] = []
    rejected_receipts: list[dict[str, str]] = []
    invalid_receipts: list[dict[str, str]] = []
    envelopes: list[ReceiptEnvelope] = []
    rejected_envelopes: list[ReceiptEnvelope] = []
    receipt_paths = (
        sorted(resolved_receipt_dir.glob("*.json"))
        if resolved_receipt_dir.is_dir()
        else []
    )
    for receipt_path in receipt_paths:
        try:
            value = json.loads(receipt_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as err:
            invalid_receipts.append(
                {"receipt": str(receipt_path), "reason": f"cannot load JSON: {err}"}
            )
            continue
        envelope = _receipt_envelope(value)
        if (
            envelope is not None
            and envelope.source_sha == source_sha
            and (workflow_run_id is None or envelope.workflow_run_id == workflow_run_id)
            and (workflow_attempt is None or envelope.attempt == workflow_attempt)
        ):
            envelopes.append(envelope)
        try:
            accepted.append(
                _validate_receipt(
                    value=value,
                    receipt_path=receipt_path,
                    bundle_dir=bundle_dir,
                    ledger=ledger,
                    item_by_id=item_by_id,
                    source_sha=source_sha,
                    workflow_run_id=workflow_run_id,
                    workflow_attempt=workflow_attempt,
                    repo=resolved_repo,
                    current_rust_toolchain=current_rust_toolchain,
                    product_artifact_manifest=external_product_artifacts,
                )
            )
        except RejectedReceiptError as err:
            rejected_receipts.append({"receipt": str(receipt_path), "reason": str(err)})
            if envelope is not None and envelope in envelopes:
                rejected_envelopes.append(envelope)
        except (InvalidReceiptError, OSError) as err:
            invalid_receipts.append({"receipt": str(receipt_path), "reason": str(err)})

    accepted_runs = {receipt.workflow_run_id for receipt in accepted}
    if workflow_run_id is None and len(accepted_runs) > 1:
        invalid_receipts.append(
            {
                "receipt": str(resolved_receipt_dir),
                "reason": "receipt bundle mixes workflow runs without an expected run id",
            }
        )

    latest_attempt_by_run: dict[str, int] = {}
    for envelope in envelopes:
        latest_attempt_by_run[envelope.workflow_run_id] = max(
            latest_attempt_by_run.get(envelope.workflow_run_id, 0),
            envelope.attempt,
        )
    selected_accepted = [
        receipt
        for receipt in accepted
        if receipt.attempt
        == latest_attempt_by_run.get(
            receipt.workflow_run_id,
            receipt.attempt,
        )
    ]
    superseded_receipts = len(accepted) - len(selected_accepted)

    rejected_latest_bundles = {
        envelope
        for envelope in rejected_envelopes
        if envelope.attempt == latest_attempt_by_run[envelope.workflow_run_id]
    }

    contributions: dict[tuple[str, str, str], list[AcceptedReceipt]] = {}
    for receipt in selected_accepted:
        for scenario in receipt.scenarios:
            contributions.setdefault(
                (receipt.stable_id, receipt.gate, scenario), []
            ).append(receipt)

    items_report: list[dict[str, Any]] = []
    receipt_conflicts: list[dict[str, str]] = []
    for item in ledger["items"]:
        gates_report: list[dict[str, Any]] = []
        for gate in item["required_gates"]:
            expectation = ledger_module.resolve_gate_expectation(
                ledger, item["id"], gate
            )
            scenarios_report: list[dict[str, Any]] = []
            for scenario in expectation["scenarios"]:
                status, selected, conflicts = _scenario_status(
                    contributions.get((item["id"], gate, scenario), [])
                )
                for conflict in conflicts:
                    receipt_conflicts.append(
                        {
                            "stable_id": item["id"],
                            "gate": gate,
                            "scenario": scenario,
                            "reason": conflict,
                        }
                    )
                scenarios_report.append(
                    {
                        "scenario": scenario,
                        "status": status,
                        "selected_receipts": selected,
                    }
                )
            gates_report.append(
                {
                    "gate": gate,
                    "status": _status_worst(
                        [entry["status"] for entry in scenarios_report]
                    ),
                    "allowed_evidence_kinds": expectation["allowed_evidence_kinds"],
                    "allowed_producers": expectation["allowed_producers"],
                    "scenarios": scenarios_report,
                }
            )
        items_report.append(
            {
                "stable_id": item["id"],
                "class": item["class"],
                "disposition": item["current_disposition"],
                "status": _status_worst([entry["status"] for entry in gates_report]),
                "gates": gates_report,
            }
        )

    status = _status_worst([item["status"] for item in items_report])
    if invalid_receipts or receipt_conflicts:
        status = "FAIL"
    elif rejected_latest_bundles:
        status = _status_worst([status, "NQ"])
    status_counts = {
        value: sum(item["status"] == value for item in items_report)
        for value in ("PASS", "NQ", "FAIL")
    }
    report = {
        "schema": AGGREGATE_SCHEMA,
        "status": status,
        "source_sha": source_sha,
        "workflow_run_id": workflow_run_id,
        "product_artifact_manifest": (
            {
                "provider": external_product_artifacts.provider,
                "workflow_run_id": external_product_artifacts.workflow_run_id,
                "workflow_attempt": external_product_artifacts.workflow_attempt,
                "head_sha": external_product_artifacts.head_sha,
                "manifest_sha256": external_product_artifacts.manifest_sha256,
                "artifact_mapping_count": len(external_product_artifacts.artifacts),
            }
            if external_product_artifacts is not None
            else None
        ),
        "receipt_counts": {
            "discovered": len(receipt_paths),
            "accepted": len(accepted),
            "selected": len(selected_accepted),
            "superseded": superseded_receipts,
            "rejected": len(rejected_receipts),
            "invalid": len(invalid_receipts),
        },
        "item_status_counts": status_counts,
        "items": items_report,
        "rejected_receipts": rejected_receipts,
        "invalid_receipts": invalid_receipts,
        "receipt_conflicts": receipt_conflicts,
        "latest_attempts": [
            {
                "workflow_run_id": run_id,
                "attempt": attempt,
            }
            for run_id, attempt in sorted(latest_attempt_by_run.items())
        ],
        "rejected_latest_bundles": [
            {
                "workflow_run_id": envelope.workflow_run_id,
                "producer": envelope.producer,
                "job": envelope.job,
                "attempt": envelope.attempt,
            }
            for envelope in sorted(
                rejected_latest_bundles,
                key=lambda candidate: (
                    candidate.workflow_run_id,
                    candidate.producer,
                    candidate.job,
                    candidate.attempt,
                ),
            )
        ],
    }
    return AggregationResult(status, STATUS_EXIT_CODES[status], report)


def _git_source_identity(repo: Path) -> tuple[str, bool]:
    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise AggregateError(f"cannot resolve source SHA for {repo}: {detail}")
    sha = completed.stdout.strip()
    try:
        validated_sha = _valid_source_sha(sha, "current source SHA")
    except InvalidReceiptError as err:
        raise AggregateError(str(err)) from err
    status = subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=normal"],
        cwd=repo,
        check=False,
        capture_output=True,
        text=True,
    )
    if status.returncode != 0:
        detail = status.stderr.strip() or status.stdout.strip()
        raise AggregateError(f"cannot resolve source cleanliness for {repo}: {detail}")
    return validated_sha, bool(status.stdout.strip())


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Aggregate pre-#423 Workbench contract ledger receipts fail closed."
        )
    )
    parser.add_argument("--ledger", type=Path, default=ledger_module.LEDGER_PATH)
    parser.add_argument("--receipt-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--product-artifact-manifest", type=Path)
    parser.add_argument("--workflow-run-id", default=os.environ.get("GITHUB_RUN_ID"))
    parser.add_argument(
        "--workflow-attempt",
        type=int,
        default=(
            int(os.environ["GITHUB_RUN_ATTEMPT"])
            if "GITHUB_RUN_ATTEMPT" in os.environ
            else None
        ),
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        github_run_id = os.environ.get("GITHUB_RUN_ID")
        if github_run_id is not None and args.workflow_run_id != github_run_id:
            raise AggregateError(
                "--workflow-run-id cannot override GITHUB_RUN_ID in CI"
            )
        github_attempt = os.environ.get("GITHUB_RUN_ATTEMPT")
        if github_attempt is not None and args.workflow_attempt != int(github_attempt):
            raise AggregateError(
                "--workflow-attempt cannot override GITHUB_RUN_ATTEMPT in CI"
            )
        source_sha, source_dirty = _git_source_identity(args.repo.resolve())
        if source_dirty:
            raise AggregateError(
                "current checkout is dirty; qualification aggregation requires "
                "a clean source tree"
            )
        result = aggregate_receipts(
            ledger=ledger_module.load_ledger(args.ledger),
            receipt_dir=args.receipt_dir,
            source_sha=source_sha,
            repo=args.repo.resolve(),
            workflow_run_id=args.workflow_run_id,
            workflow_attempt=args.workflow_attempt,
            product_artifact_manifest=args.product_artifact_manifest,
        )
    except (ledger_module.LedgerError, AggregateError) as err:
        print(f"FAIL: {err}", file=sys.stderr)
        return 2
    output = args.output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + ".tmp")
    temporary.write_text(
        json.dumps(result.report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(output)
    counts = result.report["item_status_counts"]
    print(
        f"{result.status}: pre-#423 Workbench contract ledger qualification "
        f"PASS={counts['PASS']} NQ={counts['NQ']} FAIL={counts['FAIL']} "
        f"report={output}"
    )
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
