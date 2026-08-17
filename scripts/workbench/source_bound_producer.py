#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Fail-closed runtime for source-bound pre-#423 qualification producers."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import pwd
import re
import stat
import subprocess
import sys
import tempfile
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Mapping, Sequence


PRODUCER_RESULT_SCHEMA = "nokv.pre423.producer_result.v1"
EVIDENCE_SCHEMA = "nokv.pre423.producer_evidence.v1"
RUST_QUALIFICATION_SCHEMA = "nokv.pre423.rust_qualification.v1"
RESULT_ROLES = ("producer-result",)
RUST_EVIDENCE_ROLES = frozenset({"producer-result", "qualification"})
OUTCOME_EXIT_CODES = {"PASS": 0, "NQ": 3, "FAIL": 2}
SUMMARY_CHARACTERS = 2_048
HEX_32 = re.compile(r"^[0-9a-f]{32}$")
HEX_40 = re.compile(r"^[0-9a-f]{40}$")
HEX_64 = re.compile(r"^[0-9a-f]{64}$")
RUST_TOOL_NAMES = ("cargo", "rustc")
RUST_TOOL_FIELDS = {
    "launcher_path",
    "launcher_kind",
    "resolved_path",
    "resolved_sha256",
    "version_verbose_sha256",
}
RUST_ENVIRONMENT_DENYLIST = {
    "CARGO",
    "CARGO_BUILD_RUSTC",
    "CARGO_BUILD_RUSTC_WRAPPER",
    "CARGO_ENCODED_RUSTFLAGS",
    "CARGO_HOME",
    "RUSTC",
    "RUSTC_BOOTSTRAP",
    "RUSTC_WRAPPER",
    "RUSTC_WORKSPACE_WRAPPER",
    "RUSTDOCFLAGS",
    "RUSTFLAGS",
    "RUSTUP_DIST_SERVER",
    "RUSTUP_HOME",
    "RUSTUP_TOOLCHAIN",
    "RUSTUP_UPDATE_ROOT",
}
TEST_RESULT = re.compile(
    r"^test result: (?:ok|FAILED)\. ([0-9]+) passed; ([0-9]+) failed;",
    re.MULTILINE,
)


class ProducerError(ValueError):
    """A producer invocation cannot emit trustworthy typed evidence."""


@dataclass(frozen=True)
class ScenarioContract:
    stable_id: str
    gate: str


@dataclass(frozen=True)
class QualificationContext:
    producer: str
    evidence_kind: str
    operation_id: str
    source_sha: str
    command_argv_sha256: str
    subjects: dict[str, object]
    subjects_sha256: str
    scenarios: tuple[str, ...]


@dataclass(frozen=True)
class RustToolchainIdentity:
    cargo: Path
    rustc: Path
    child_environment: dict[str, str]
    evidence: tuple[dict[str, object], ...]


@dataclass(frozen=True)
class RustTestAssertion:
    assertion_id: str
    package: str
    target_args: tuple[str, ...]
    test_name: str

    def __post_init__(self) -> None:
        if not self.assertion_id or not self.package or not self.test_name:
            raise ValueError("Rust test assertions require non-empty identities")
        valid_target = self.target_args == ("--lib",) or (
            len(self.target_args) == 2 and self.target_args[0] in {"--test", "--bin"}
        )
        if not valid_target:
            raise ValueError("Rust test assertions require one exact Cargo target")


@dataclass(frozen=True)
class SourceTextAssertion:
    assertion_id: str
    path: str
    required: tuple[str, ...] = ()
    forbidden: tuple[str, ...] = ()
    before_marker: str | None = None

    def __post_init__(self) -> None:
        if not self.assertion_id or not self.path:
            raise ValueError("source assertions require non-empty identities")
        if not self.required and not self.forbidden:
            raise ValueError("source assertions require at least one predicate")
        if any(not value for value in (*self.required, *self.forbidden)):
            raise ValueError("source predicates must not be empty")


@dataclass(frozen=True)
class CargoWorkspaceGraphAssertion:
    assertion_id: str
    forbidden_tokens: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.assertion_id or not self.forbidden_tokens:
            raise ValueError("Cargo workspace assertions require identities and tokens")
        if any(not token or token != token.lower() for token in self.forbidden_tokens):
            raise ValueError("Cargo workspace forbidden tokens must be lowercase")
        if len(self.forbidden_tokens) != len(set(self.forbidden_tokens)):
            raise ValueError("Cargo workspace forbidden tokens must be unique")


@dataclass(frozen=True)
class RustScenario:
    contract: ScenarioContract
    assertions: tuple[RustTestAssertion, ...] = ()
    not_qualified_reason: str | None = None

    def __post_init__(self) -> None:
        if bool(self.assertions) == bool(self.not_qualified_reason):
            raise ValueError(
                "a Rust scenario must have exact assertions or one NQ reason"
            )


@dataclass(frozen=True)
class StaticScenario:
    contract: ScenarioContract
    assertions: tuple[SourceTextAssertion | CargoWorkspaceGraphAssertion, ...] = ()
    not_qualified_reason: str | None = None

    def __post_init__(self) -> None:
        if bool(self.assertions) == bool(self.not_qualified_reason):
            raise ValueError(
                "a static scenario must have source assertions or one NQ reason"
            )


@dataclass(frozen=True)
class AssertionResult:
    passed: bool
    record: dict[str, object]
    matched_test_count: int | None = None
    timed_out: bool = False


CommandRunner = Callable[..., subprocess.CompletedProcess[str]]
TrackedChecker = Callable[[Path, str], None]
TrackedManifestLister = Callable[[Path], Sequence[str]]
EvidenceWriter = Callable[[dict[str, object]], None]


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _json_sha256(value: object) -> str:
    return hashlib.sha256(_canonical_json(value)).hexdigest()


def _required_environment(environ: Mapping[str, str], name: str) -> str:
    value = environ.get(name)
    if value is None or not value:
        raise ProducerError(f"missing required runner environment {name}")
    return value


def _executable_path(location: str) -> Path:
    """Return an absolute executable path without dereferencing a launcher symlink."""

    return Path(os.path.abspath(location))


def _validate_rust_toolchain_shape(value: object) -> dict[str, dict[str, str]]:
    if not isinstance(value, dict) or set(value) != set(RUST_TOOL_NAMES):
        raise ProducerError("rust_toolchain must contain exactly cargo and rustc")
    tools: dict[str, dict[str, str]] = {}
    for name in RUST_TOOL_NAMES:
        raw_tool = value.get(name)
        if not isinstance(raw_tool, dict) or set(raw_tool) != RUST_TOOL_FIELDS:
            raise ProducerError(
                f"rust_toolchain.{name} must use the exact closed tool schema"
            )
        if any(not isinstance(raw_tool[field], str) for field in RUST_TOOL_FIELDS):
            raise ProducerError(f"rust_toolchain.{name} fields must be strings")
        tool = {field: raw_tool[field] for field in RUST_TOOL_FIELDS}
        if tool["launcher_kind"] not in {"regular", "symlink"}:
            raise ProducerError(
                f"rust_toolchain.{name}.launcher_kind must be regular or symlink"
            )
        for field in ("launcher_path", "resolved_path"):
            path = Path(tool[field])
            if not path.is_absolute() or _executable_path(tool[field]) != path:
                raise ProducerError(
                    f"rust_toolchain.{name}.{field} must be a normalized absolute path"
                )
        for field in ("resolved_sha256", "version_verbose_sha256"):
            if not HEX_64.fullmatch(tool[field]):
                raise ProducerError(
                    f"rust_toolchain.{name}.{field} must be lowercase SHA-256"
                )
        tools[name] = tool
    return tools


def _regular_file_sha256(path: Path, field: str) -> str:
    try:
        lexical_stat = path.lstat()
    except OSError as error:
        raise ProducerError(f"{field} is unavailable: {error}") from error
    if not stat.S_ISREG(lexical_stat.st_mode):
        raise ProducerError(f"{field} must resolve to a regular file")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise ProducerError(f"cannot safely open {field}: {error}") from error
    digest = hashlib.sha256()
    try:
        opened_stat = os.fstat(descriptor)
        if not stat.S_ISREG(opened_stat.st_mode) or (
            opened_stat.st_dev,
            opened_stat.st_ino,
        ) != (lexical_stat.st_dev, lexical_stat.st_ino):
            raise ProducerError(f"{field} changed while its identity was checked")
        while chunk := os.read(descriptor, 1024 * 1024):
            digest.update(chunk)
    finally:
        os.close(descriptor)
    try:
        final_stat = path.lstat()
    except OSError as error:
        raise ProducerError(
            f"{field} disappeared after identity check: {error}"
        ) from error
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
        raise ProducerError(f"{field} changed while its identity was checked")
    return digest.hexdigest()


def _rust_child_environment(
    environ: Mapping[str, str], tools: Mapping[str, Mapping[str, str]]
) -> dict[str, str]:
    child = {
        key: value
        for key, value in environ.items()
        if key not in RUST_ENVIRONMENT_DENYLIST
        and key != "CARGO"
        and not key.startswith("CARGO_")
        and not key.startswith("RUST")
    }
    try:
        child["HOME"] = pwd.getpwuid(os.getuid()).pw_dir
    except KeyError as error:
        raise ProducerError(
            "cannot resolve the current account home directory"
        ) from error
    child["CARGO"] = tools["cargo"]["launcher_path"]
    child["RUSTC"] = tools["rustc"]["launcher_path"]
    return child


def validate_rust_toolchain(
    subjects: Mapping[str, object],
    *,
    repo: Path,
    environ: Mapping[str, str],
    timeout_seconds: int = 30,
    command_runner: CommandRunner = subprocess.run,
) -> RustToolchainIdentity:
    """Re-derive the runner-bound Cargo and rustc identities before executing tests."""

    if not 1 <= timeout_seconds <= 60:
        raise ProducerError("toolchain identity timeout must be between 1 and 60")
    tools = _validate_rust_toolchain_shape(subjects.get("rust_toolchain"))
    child_environment = _rust_child_environment(environ, tools)
    evidence: list[dict[str, object]] = []
    launchers: dict[str, Path] = {}
    for name in RUST_TOOL_NAMES:
        tool = tools[name]
        launcher = Path(tool["launcher_path"])
        try:
            launcher_stat = launcher.lstat()
        except OSError as error:
            raise ProducerError(
                f"rust_toolchain.{name}.launcher_path is unavailable: {error}"
            ) from error
        actual_kind = (
            "symlink"
            if stat.S_ISLNK(launcher_stat.st_mode)
            else "regular"
            if stat.S_ISREG(launcher_stat.st_mode)
            else "other"
        )
        if actual_kind != tool["launcher_kind"]:
            raise ProducerError(
                f"rust_toolchain.{name} launcher kind does not match runner subject"
            )
        try:
            resolved = launcher.resolve(strict=True)
        except OSError as error:
            raise ProducerError(
                f"rust_toolchain.{name} launcher cannot be resolved: {error}"
            ) from error
        if resolved != Path(tool["resolved_path"]):
            raise ProducerError(
                f"rust_toolchain.{name} resolved path does not match runner subject"
            )
        actual_sha256 = _regular_file_sha256(
            resolved, f"rust_toolchain.{name}.resolved_path"
        )
        if actual_sha256 != tool["resolved_sha256"]:
            raise ProducerError(
                f"rust_toolchain.{name} executable hash does not match runner subject"
            )
        argv = [str(launcher), "-Vv"]
        try:
            completed = command_runner(
                argv,
                cwd=repo,
                check=False,
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
                shell=False,
                env=child_environment,
            )
        except (OSError, UnicodeError, subprocess.TimeoutExpired) as error:
            raise ProducerError(
                f"rust_toolchain.{name} version identity failed: {error}"
            ) from error
        stdout_sha256 = _stream_sha256(completed.stdout)
        version_verbose_sha256 = _command_output_sha256(
            completed.stdout, completed.stderr
        )
        passed = (
            completed.returncode == 0
            and bool(completed.stdout)
            and version_verbose_sha256 == tool["version_verbose_sha256"]
        )
        record: dict[str, object] = {
            "schema": EVIDENCE_SCHEMA,
            "kind": "rust-toolchain-identity",
            "tool": name,
            "argv": argv,
            "launcher_kind": actual_kind,
            "resolved_path": str(resolved),
            "resolved_sha256": actual_sha256,
            "exit_code": completed.returncode,
            "stdout_sha256": stdout_sha256,
            "stderr_sha256": _stream_sha256(completed.stderr),
            "version_verbose_sha256": version_verbose_sha256,
            "stdout_summary": _bounded_summary(completed.stdout),
            "stderr_summary": _bounded_summary(completed.stderr),
            "passed": passed,
        }
        evidence.append(record)
        if not passed:
            raise ProducerError(
                f"rust_toolchain.{name} version identity does not match runner subject"
            )
        launchers[name] = launcher
    return RustToolchainIdentity(
        cargo=launchers["cargo"],
        rustc=launchers["rustc"],
        child_environment=child_environment,
        evidence=tuple(evidence),
    )


def _scenario_contract(value: object) -> ScenarioContract:
    if isinstance(value, ScenarioContract):
        return value
    contract = getattr(value, "contract", None)
    if isinstance(contract, ScenarioContract):
        return contract
    raise ProducerError("scenario mapping contains an invalid contract")


def load_context(
    environ: Mapping[str, str],
    *,
    producer_id: str,
    evidence_kind: str | Sequence[str],
    scenarios: Mapping[str, object],
    require_rust_toolchain: bool = False,
    require_product_binary: bool = False,
    expected_dependencies: Sequence[str] = (),
    required_evidence_roles: Sequence[str] = RESULT_ROLES,
) -> QualificationContext:
    """Load and exact-bind the runner context for a known producer mapping."""

    actual_producer = _required_environment(environ, "NOKV_QUALIFICATION_PRODUCER")
    if actual_producer != producer_id:
        raise ProducerError(
            f"producer identity mismatch: expected {producer_id}, got {actual_producer}"
        )
    allowed_kinds = (
        {evidence_kind} if isinstance(evidence_kind, str) else set(evidence_kind)
    )
    actual_kind = _required_environment(environ, "NOKV_QUALIFICATION_EVIDENCE_KIND")
    if actual_kind not in allowed_kinds:
        raise ProducerError(
            f"evidence kind {actual_kind!r} is not allowed for {producer_id}"
        )

    operation_id = _required_environment(environ, "NOKV_QUALIFICATION_OPERATION_ID")
    source_sha = _required_environment(environ, "NOKV_QUALIFICATION_SOURCE_SHA")
    argv_sha = _required_environment(environ, "NOKV_QUALIFICATION_COMMAND_ARGV_SHA256")
    if not HEX_32.fullmatch(operation_id):
        raise ProducerError("runner operation id is not 32 lowercase hex characters")
    if not HEX_40.fullmatch(source_sha):
        raise ProducerError("runner source SHA is not 40 lowercase hex characters")
    if not HEX_64.fullmatch(argv_sha):
        raise ProducerError("runner argv hash is not 64 lowercase hex characters")

    try:
        subjects_value = json.loads(
            _required_environment(environ, "NOKV_QUALIFICATION_SUBJECTS")
        )
    except json.JSONDecodeError as error:
        raise ProducerError(f"invalid qualification subjects JSON: {error}") from error
    if not isinstance(subjects_value, dict):
        raise ProducerError("runner subjects must be an object")
    subjects: dict[str, object] = subjects_value
    expected_subject_keys = {"dependencies"}
    if require_rust_toolchain:
        expected_subject_keys.add("rust_toolchain")
    if require_product_binary:
        expected_subject_keys.add("product_binary")
    if set(subjects) != expected_subject_keys:
        raise ProducerError("runner subjects do not match this producer boundary")
    dependencies = subjects.get("dependencies")
    if not isinstance(dependencies, list):
        raise ProducerError("runner dependency subjects must be an array")
    dependency_names: list[str] = []
    for dependency in dependencies:
        if (
            not isinstance(dependency, dict)
            or set(dependency) != {"name", "identity"}
            or any(not isinstance(dependency[field], str) for field in dependency)
        ):
            raise ProducerError("runner dependency subjects use an invalid schema")
        dependency_names.append(dependency["name"])
    if dependency_names != sorted(expected_dependencies):
        raise ProducerError(
            "runner dependency subjects do not match this producer boundary"
        )
    if require_rust_toolchain:
        _validate_rust_toolchain_shape(subjects.get("rust_toolchain"))
    if require_product_binary:
        product_binary = subjects.get("product_binary")
        if (
            not isinstance(product_binary, dict)
            or set(product_binary) != {"path", "sha256"}
            or not isinstance(product_binary.get("path"), str)
            or not isinstance(product_binary.get("sha256"), str)
            or not HEX_64.fullmatch(product_binary["sha256"])
        ):
            raise ProducerError("runner product binary subject uses an invalid schema")
        binary_path = Path(product_binary["path"])
        if (
            not binary_path.is_absolute()
            or not binary_path.is_file()
            or _regular_file_sha256(binary_path, "product_binary.path")
            != product_binary["sha256"]
        ):
            raise ProducerError("runner product binary identity is not current")
    subjects_sha = _required_environment(environ, "NOKV_QUALIFICATION_SUBJECTS_SHA256")
    if subjects_sha != _json_sha256(subjects):
        raise ProducerError("runner subjects hash does not match canonical subjects")
    try:
        required_roles = json.loads(
            _required_environment(environ, "NOKV_QUALIFICATION_REQUIRED_EVIDENCE_ROLES")
        )
    except json.JSONDecodeError as error:
        raise ProducerError(f"invalid required evidence roles JSON: {error}") from error
    if required_roles != list(required_evidence_roles):
        raise ProducerError("runner evidence roles do not match this producer boundary")

    try:
        claims = json.loads(_required_environment(environ, "NOKV_QUALIFICATION_CLAIMS"))
    except json.JSONDecodeError as error:
        raise ProducerError(f"invalid qualification claims JSON: {error}") from error
    if not isinstance(claims, list) or not claims:
        raise ProducerError("producer requires at least one runner-validated claim")
    selected: list[str] = []
    seen: set[str] = set()
    for claim in claims:
        if not isinstance(claim, dict) or set(claim) != {
            "stable_id",
            "gate",
            "scenario",
        }:
            raise ProducerError("each claim must use the exact closed claim schema")
        if any(not isinstance(claim[field], str) for field in claim):
            raise ProducerError("claim fields must be strings")
        scenario = claim["scenario"]
        if scenario in seen:
            raise ProducerError(f"duplicate scenario claim {scenario!r}")
        seen.add(scenario)
        specification = scenarios.get(scenario)
        if specification is None:
            raise ProducerError(f"unknown scenario {scenario!r}")
        contract = _scenario_contract(specification)
        if (claim["stable_id"], claim["gate"]) != (
            contract.stable_id,
            contract.gate,
        ):
            raise ProducerError(
                f"scenario {scenario!r} does not belong to the supplied item and gate"
            )
        selected.append(scenario)
    return QualificationContext(
        producer=producer_id,
        evidence_kind=actual_kind,
        operation_id=operation_id,
        source_sha=source_sha,
        command_argv_sha256=argv_sha,
        subjects=subjects,
        subjects_sha256=subjects_sha,
        scenarios=tuple(selected),
    )


def _bounded_summary(value: str | bytes | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    value = value.replace("\x00", "\\0")
    return value[-SUMMARY_CHARACTERS:]


def _stream_sha256(value: str | bytes | None) -> str:
    if value is None:
        payload = b""
    elif isinstance(value, bytes):
        payload = value
    else:
        payload = value.encode("utf-8", errors="replace")
    return hashlib.sha256(payload).hexdigest()


def _command_output_sha256(
    stdout: str | bytes | None, stderr: str | bytes | None
) -> str:
    def payload(value: str | bytes | None) -> bytes:
        if value is None:
            return b""
        if isinstance(value, bytes):
            return value
        return value.encode("utf-8", errors="replace")

    return hashlib.sha256(payload(stdout) + b"\0" + payload(stderr)).hexdigest()


def execute_rust_test(
    assertion: RustTestAssertion,
    *,
    repo: Path,
    cargo: Path,
    target_dir: Path,
    timeout_seconds: int,
    environment: Mapping[str, str] | None = None,
    command_runner: CommandRunner = subprocess.run,
) -> AssertionResult:
    """Run one exact Cargo test and reject zero, partial, or broad execution."""

    argv = [
        str(cargo),
        "test",
        "--locked",
        "--color=never",
        "-p",
        assertion.package,
        "--target-dir",
        str(target_dir),
        *assertion.target_args,
        assertion.test_name,
        "--",
        "--exact",
    ]
    timed_out = False
    try:
        completed = command_runner(
            argv,
            cwd=repo,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            shell=False,
            env=None if environment is None else dict(environment),
        )
        exit_code: int | None = completed.returncode
        stdout = completed.stdout
        stderr = completed.stderr
    except subprocess.TimeoutExpired as error:
        timed_out = True
        exit_code = None
        stdout = error.output
        stderr = error.stderr
    except UnicodeError as error:
        exit_code = None
        stdout = ""
        stderr = f"command output decode failed: {error}"
    except OSError as error:
        exit_code = None
        stdout = ""
        stderr = f"command launch failed: {error}"

    stdout_text = _bounded_summary(stdout)
    full_stdout = (
        stdout.decode("utf-8", errors="replace")
        if isinstance(stdout, bytes)
        else stdout or ""
    )
    totals = TEST_RESULT.findall(full_stdout)
    matched_test_count = sum(int(passed) for passed, _ in totals)
    failed_test_count = sum(int(failed) for _, failed in totals)
    exact_line = re.compile(
        rf"^test {re.escape(assertion.test_name)} \.\.\. ok$", re.MULTILINE
    )
    exact_matches = len(exact_line.findall(full_stdout))
    passed = (
        exit_code == 0
        and not timed_out
        and matched_test_count == 1
        and failed_test_count == 0
        and exact_matches == 1
    )
    record: dict[str, object] = {
        "schema": EVIDENCE_SCHEMA,
        "kind": "exact-cargo-test",
        "assertion_id": assertion.assertion_id,
        "argv": argv,
        "exit_code": exit_code,
        "timed_out": timed_out,
        "matched_test_count": matched_test_count,
        "exact_test_line_count": exact_matches,
        "stdout_sha256": _stream_sha256(stdout),
        "stderr_sha256": _stream_sha256(stderr),
        "stdout_summary": stdout_text,
        "stderr_summary": _bounded_summary(stderr),
        "passed": passed,
    }
    return AssertionResult(
        passed=passed,
        record=record,
        matched_test_count=matched_test_count,
        timed_out=timed_out,
    )


def _default_tracked_checker(repo: Path, path: str) -> None:
    completed = subprocess.run(
        ["git", "ls-files", "--error-unmatch", "--", path],
        cwd=repo,
        check=False,
        capture_output=True,
        text=True,
        shell=False,
    )
    if completed.returncode != 0 or completed.stdout.strip() != path:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise ProducerError(
            f"qualification source is not exact and tracked: {path}: {detail}"
        )


def _source_predicate_sha256(assertion: SourceTextAssertion) -> str:
    return _json_sha256(
        {
            "path": assertion.path,
            "required": list(assertion.required),
            "forbidden": list(assertion.forbidden),
            "before_marker": assertion.before_marker,
        }
    )


def execute_source_assertion(
    assertion: SourceTextAssertion,
    *,
    repo: Path,
    tracked_checker: TrackedChecker = _default_tracked_checker,
) -> AssertionResult:
    """Evaluate one bounded predicate against one exact tracked source file."""

    relative = Path(assertion.path)
    if relative.is_absolute() or ".." in relative.parts:
        raise ProducerError(f"source assertion escapes the checkout: {assertion.path}")
    candidate = repo / relative
    if candidate.is_symlink():
        raise ProducerError(
            f"qualification source is not a regular file: {assertion.path}"
        )
    source_path = candidate.resolve()
    try:
        source_path.relative_to(repo.resolve())
    except ValueError as error:
        raise ProducerError(
            f"source assertion escapes the checkout: {assertion.path}"
        ) from error
    tracked_checker(repo, assertion.path)
    if not source_path.is_file():
        raise ProducerError(
            f"qualification source is not a regular file: {assertion.path}"
        )
    payload = source_path.read_bytes()
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ProducerError(
            f"qualification source is not UTF-8: {assertion.path}"
        ) from error
    marker_found = True
    if assertion.before_marker is not None:
        marker_index = text.find(assertion.before_marker)
        marker_found = marker_index >= 0
        if marker_found:
            text = text[:marker_index]
    missing = [
        index for index, value in enumerate(assertion.required) if value not in text
    ]
    forbidden_hits = [
        index for index, value in enumerate(assertion.forbidden) if value in text
    ]
    passed = marker_found and not missing and not forbidden_hits
    record: dict[str, object] = {
        "schema": EVIDENCE_SCHEMA,
        "kind": "tracked-source-predicate",
        "assertion_id": assertion.assertion_id,
        "path": assertion.path,
        "source_predicate_sha256": _source_predicate_sha256(assertion),
        "source_sha256": hashlib.sha256(payload).hexdigest(),
        "size_bytes": len(payload),
        "required_predicate_count": len(assertion.required),
        "forbidden_predicate_count": len(assertion.forbidden),
        "missing_required_indexes": missing,
        "forbidden_hit_indexes": forbidden_hits,
        "section_marker_found": marker_found,
        "passed": passed,
    }
    return AssertionResult(passed=passed, record=record)


def _default_tracked_manifest_lister(repo: Path) -> tuple[str, ...]:
    completed = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=repo,
        check=False,
        capture_output=True,
        shell=False,
    )
    if completed.returncode != 0:
        detail = completed.stderr.decode("utf-8", errors="replace").strip()
        raise ProducerError(f"cannot enumerate tracked Cargo sources: {detail}")
    try:
        tracked = completed.stdout.decode("utf-8").split("\0")
    except UnicodeDecodeError as error:
        raise ProducerError("tracked source paths are not UTF-8") from error
    selected = sorted(
        path
        for path in tracked
        if path == "Cargo.lock" or path == "Cargo.toml" or path.endswith("/Cargo.toml")
    )
    return tuple(selected)


def _cargo_graph_predicate_sha256(assertion: CargoWorkspaceGraphAssertion) -> str:
    return _json_sha256(
        {
            "tracked_source_policy": "all-cargo-manifests-and-lock-v1",
            "forbidden_tokens": list(assertion.forbidden_tokens),
        }
    )


def _cargo_forbidden_hits(
    value: object,
    *,
    path: str,
    location: str,
    forbidden_tokens: tuple[str, ...],
) -> list[str]:
    hits: list[str] = []
    if isinstance(value, dict):
        for key, child in value.items():
            key_location = f"{location}.{key}" if location else str(key)
            lowered_key = str(key).lower()
            for token in forbidden_tokens:
                if token in lowered_key:
                    hits.append(f"{path}:{key_location}:key:{token}")
            hits.extend(
                _cargo_forbidden_hits(
                    child,
                    path=path,
                    location=key_location,
                    forbidden_tokens=forbidden_tokens,
                )
            )
    elif isinstance(value, list):
        for index, child in enumerate(value):
            hits.extend(
                _cargo_forbidden_hits(
                    child,
                    path=path,
                    location=f"{location}[{index}]",
                    forbidden_tokens=forbidden_tokens,
                )
            )
    elif isinstance(value, str):
        lowered_value = value.lower()
        for token in forbidden_tokens:
            if token in lowered_value:
                hits.append(f"{path}:{location}:value:{token}")
    return hits


def execute_cargo_workspace_assertion(
    assertion: CargoWorkspaceGraphAssertion,
    *,
    repo: Path,
    tracked_manifest_lister: TrackedManifestLister = _default_tracked_manifest_lister,
) -> AssertionResult:
    """Parse every tracked Cargo manifest and lockfile as one closed source graph."""

    paths = tuple(tracked_manifest_lister(repo))
    if (
        not paths
        or len(paths) > 512
        or len(paths) != len(set(paths))
        or tuple(sorted(paths)) != paths
    ):
        raise ProducerError(
            "tracked Cargo source inventory must be sorted, unique, and bounded"
        )
    if "Cargo.toml" not in paths or "Cargo.lock" not in paths:
        raise ProducerError(
            "tracked Cargo graph must contain Cargo.toml and Cargo.lock"
        )
    digest = hashlib.sha256()
    total_bytes = 0
    forbidden_hits: list[str] = []
    manifest_count = 0
    for relative_path in paths:
        relative = Path(relative_path)
        if (
            relative.is_absolute()
            or ".." in relative.parts
            or (relative_path != "Cargo.lock" and relative.name != "Cargo.toml")
        ):
            raise ProducerError(f"invalid tracked Cargo source path: {relative_path}")
        candidate = repo / relative
        if candidate.is_symlink() or not candidate.is_file():
            raise ProducerError(
                f"tracked Cargo source is not a regular file: {relative_path}"
            )
        payload = candidate.read_bytes()
        if len(payload) > 1024 * 1024:
            raise ProducerError(f"tracked Cargo source is too large: {relative_path}")
        total_bytes += len(payload)
        if total_bytes > 8 * 1024 * 1024:
            raise ProducerError("tracked Cargo source graph exceeds 8 MiB")
        try:
            parsed = tomllib.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
            raise ProducerError(
                f"tracked Cargo source is not valid UTF-8 TOML: {relative_path}: {error}"
            ) from error
        if relative.name == "Cargo.toml":
            manifest_count += 1
        encoded_path = relative_path.encode("utf-8")
        digest.update(len(encoded_path).to_bytes(8, "big"))
        digest.update(encoded_path)
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
        forbidden_hits.extend(
            _cargo_forbidden_hits(
                parsed,
                path=relative_path,
                location="",
                forbidden_tokens=assertion.forbidden_tokens,
            )
        )
    passed = not forbidden_hits
    record: dict[str, object] = {
        "schema": EVIDENCE_SCHEMA,
        "kind": "tracked-cargo-workspace-graph",
        "assertion_id": assertion.assertion_id,
        "source_predicate_sha256": _cargo_graph_predicate_sha256(assertion),
        "tracked_manifest_count": manifest_count,
        "cargo_lock_scanned": True,
        "tracked_source_count": len(paths),
        "tracked_source_bytes": total_bytes,
        "tracked_graph_sha256": digest.hexdigest(),
        "forbidden_hit_count": len(forbidden_hits),
        "forbidden_hits": forbidden_hits[:32],
        "forbidden_hits_truncated": len(forbidden_hits) > 32,
        "passed": passed,
    }
    return AssertionResult(passed=passed, record=record)


def execute_static_assertion(
    assertion: SourceTextAssertion | CargoWorkspaceGraphAssertion,
    *,
    repo: Path,
    tracked_checker: TrackedChecker = _default_tracked_checker,
    tracked_manifest_lister: TrackedManifestLister = _default_tracked_manifest_lister,
) -> AssertionResult:
    if isinstance(assertion, SourceTextAssertion):
        return execute_source_assertion(
            assertion, repo=repo, tracked_checker=tracked_checker
        )
    if isinstance(assertion, CargoWorkspaceGraphAssertion):
        return execute_cargo_workspace_assertion(
            assertion,
            repo=repo,
            tracked_manifest_lister=tracked_manifest_lister,
        )
    raise ProducerError("static scenario contains an unsupported assertion")


def emit_evidence(record: dict[str, object]) -> None:
    print(_canonical_json(record).decode("utf-8"), flush=True)


def _global_outcome(outcomes: Sequence[str]) -> str:
    if any(outcome == "FAIL" for outcome in outcomes):
        return "FAIL"
    if any(outcome == "NQ" for outcome in outcomes):
        return "NQ"
    return "PASS"


def qualify_rust_scenarios(
    context: QualificationContext,
    scenarios: Mapping[str, RustScenario],
    *,
    repo: Path,
    cargo: Path,
    target_dir: Path,
    timeout_seconds: int,
    environment: Mapping[str, str] | None = None,
    command_runner: CommandRunner = subprocess.run,
    evidence_writer: EvidenceWriter = emit_evidence,
) -> str:
    outcomes: list[str] = []
    cache: dict[RustTestAssertion, AssertionResult] = {}
    for scenario in context.scenarios:
        specification = scenarios[scenario]
        if specification.not_qualified_reason is not None:
            evidence_writer(
                {
                    "schema": EVIDENCE_SCHEMA,
                    "kind": "qualification-gap",
                    "scenario": scenario,
                    "reason": specification.not_qualified_reason,
                    "outcome": "NQ",
                }
            )
            outcomes.append("NQ")
            continue
        scenario_passed = True
        for assertion in specification.assertions:
            cache_hit = assertion in cache
            result = cache.get(assertion)
            if result is None:
                result = execute_rust_test(
                    assertion,
                    repo=repo,
                    cargo=cargo,
                    target_dir=target_dir,
                    timeout_seconds=timeout_seconds,
                    environment=environment,
                    command_runner=command_runner,
                )
                cache[assertion] = result
            record = dict(result.record)
            record["scenario"] = scenario
            record["cache_hit"] = cache_hit
            evidence_writer(record)
            scenario_passed = scenario_passed and result.passed
        outcomes.append("PASS" if scenario_passed else "FAIL")
    return _global_outcome(outcomes)


def qualify_static_scenarios(
    context: QualificationContext,
    scenarios: Mapping[str, StaticScenario],
    *,
    repo: Path,
    tracked_checker: TrackedChecker = _default_tracked_checker,
    tracked_manifest_lister: TrackedManifestLister = _default_tracked_manifest_lister,
    evidence_writer: EvidenceWriter = emit_evidence,
) -> str:
    outcomes: list[str] = []
    cache: dict[
        SourceTextAssertion | CargoWorkspaceGraphAssertion, AssertionResult
    ] = {}
    for scenario in context.scenarios:
        specification = scenarios[scenario]
        if specification.not_qualified_reason is not None:
            evidence_writer(
                {
                    "schema": EVIDENCE_SCHEMA,
                    "kind": "qualification-gap",
                    "scenario": scenario,
                    "reason": specification.not_qualified_reason,
                    "outcome": "NQ",
                }
            )
            outcomes.append("NQ")
            continue
        scenario_passed = True
        for assertion in specification.assertions:
            cache_hit = assertion in cache
            result = cache.get(assertion)
            if result is None:
                try:
                    result = execute_static_assertion(
                        assertion,
                        repo=repo,
                        tracked_checker=tracked_checker,
                        tracked_manifest_lister=tracked_manifest_lister,
                    )
                except (OSError, ProducerError) as error:
                    result = AssertionResult(
                        passed=False,
                        record={
                            "schema": EVIDENCE_SCHEMA,
                            "kind": "tracked-static-assertion",
                            "assertion_id": assertion.assertion_id,
                            "error": str(error),
                            "passed": False,
                        },
                    )
                cache[assertion] = result
            record = dict(result.record)
            record["scenario"] = scenario
            record["cache_hit"] = cache_hit
            evidence_writer(record)
            scenario_passed = scenario_passed and result.passed
        outcomes.append("PASS" if scenario_passed else "FAIL")
    return _global_outcome(outcomes)


def write_create_new_evidence(
    path: Path, payload: bytes, *, operation_id: str, label: str
) -> None:
    """Durably publish one create-new evidence file without replacing a peer."""

    if not path.is_absolute():
        raise ProducerError(f"{label} must be an absolute path")
    parent = path.parent
    if not parent.is_dir():
        raise ProducerError(f"{label} parent directory does not exist")
    if path.exists() or path.is_symlink():
        raise ProducerError(f"{label} must be create-new")
    temporary = parent / f".{path.name}.{operation_id}.tmp"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
    descriptor: int | None = None
    try:
        descriptor = os.open(temporary, flags, 0o600)
        offset = 0
        while offset < len(payload):
            offset += os.write(descriptor, payload[offset:])
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = None
        try:
            os.link(temporary, path)
        except FileExistsError as error:
            raise ProducerError(f"{label} must be create-new") from error
        directory_descriptor = os.open(parent, os.O_RDONLY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    finally:
        if descriptor is not None:
            os.close(descriptor)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def write_producer_result(
    path: Path,
    context: QualificationContext,
    outcome: str,
    *,
    evidence_roles: Sequence[str] = RESULT_ROLES,
) -> None:
    """Atomically publish a create-new closed producer result."""

    if outcome not in OUTCOME_EXIT_CODES:
        raise ProducerError(f"invalid producer outcome {outcome!r}")
    value = {
        "schema": PRODUCER_RESULT_SCHEMA,
        "producer": context.producer,
        "evidence_kind": context.evidence_kind,
        "operation_id": context.operation_id,
        "source_sha": context.source_sha,
        "command_argv_sha256": context.command_argv_sha256,
        "subjects": context.subjects,
        "subjects_sha256": context.subjects_sha256,
        "scenarios": {
            scenario: {
                "outcome": outcome,
                "evidence_roles": list(evidence_roles),
            }
            for scenario in sorted(context.scenarios)
        },
    }
    payload = json.dumps(value, indent=2, sort_keys=True).encode("utf-8") + b"\n"
    write_create_new_evidence(
        path,
        payload,
        operation_id=context.operation_id,
        label="--qualification-result",
    )


def write_rust_qualification(
    path: Path,
    context: QualificationContext,
    outcome: str,
    scenarios: Mapping[str, RustScenario],
) -> None:
    """Publish the closed scenario summary required by integration producers."""

    value = {
        "schema": RUST_QUALIFICATION_SCHEMA,
        "producer": context.producer,
        "evidence_kind": context.evidence_kind,
        "operation_id": context.operation_id,
        "outcome": outcome,
        "scenarios": {
            scenario: {
                "outcome": outcome,
                "not_qualified_reason": scenarios[scenario].not_qualified_reason,
            }
            for scenario in sorted(context.scenarios)
        },
    }
    payload = json.dumps(value, indent=2, sort_keys=True).encode("utf-8") + b"\n"
    write_create_new_evidence(
        path,
        payload,
        operation_id=context.operation_id,
        label="Rust qualification evidence",
    )


def _rust_parser(description: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--qualification-result", required=True, type=Path)
    parser.add_argument("--target-dir", type=Path)
    parser.add_argument("--timeout-seconds", type=int, default=300)
    return parser


def _static_parser(description: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--qualification-result", required=True, type=Path)
    return parser


def rust_main(
    *,
    producer_id: str,
    evidence_kinds: Sequence[str],
    scenarios: Mapping[str, RustScenario],
    description: str,
    argv: Sequence[str] | None = None,
    environ: Mapping[str, str] | None = None,
    command_runner: CommandRunner = subprocess.run,
    evidence_roles: Sequence[str] = RESULT_ROLES,
) -> int:
    args = _rust_parser(description).parse_args(argv)
    if not 1 <= args.timeout_seconds <= 1_200:
        print("FAIL: --timeout-seconds must be between 1 and 1200", file=sys.stderr)
        return 2
    environment = os.environ if environ is None else environ
    try:
        roles = tuple(evidence_roles)
        if (
            not roles
            or roles[0] != "producer-result"
            or len(roles) != len(set(roles))
            or not set(roles).issubset(RUST_EVIDENCE_ROLES)
        ):
            raise ProducerError(
                "Rust evidence roles must be unique, supported, and start with "
                "producer-result"
            )
        context = load_context(
            environment,
            producer_id=producer_id,
            evidence_kind=evidence_kinds,
            scenarios=scenarios,
            require_rust_toolchain=True,
            required_evidence_roles=roles,
        )
        repo = Path.cwd().resolve()
        toolchain = validate_rust_toolchain(
            context.subjects,
            repo=repo,
            environ=environment,
            timeout_seconds=min(args.timeout_seconds, 60),
            command_runner=command_runner,
        )
        for record in toolchain.evidence:
            emit_evidence(record)
        explicit_target = args.target_dir
        if explicit_target is not None:
            target_dir = explicit_target.resolve()
            target_dir.mkdir(parents=True, exist_ok=True)
            outcome = qualify_rust_scenarios(
                context,
                scenarios,
                repo=repo,
                cargo=toolchain.cargo,
                target_dir=target_dir,
                timeout_seconds=args.timeout_seconds,
                environment=toolchain.child_environment,
                command_runner=command_runner,
            )
        else:
            with tempfile.TemporaryDirectory(
                prefix="nokv-source-bound-cargo-target-"
            ) as directory:
                outcome = qualify_rust_scenarios(
                    context,
                    scenarios,
                    repo=repo,
                    cargo=toolchain.cargo,
                    target_dir=Path(directory),
                    timeout_seconds=args.timeout_seconds,
                    environment=toolchain.child_environment,
                    command_runner=command_runner,
                )
        if "qualification" in roles:
            write_rust_qualification(
                args.qualification_result.parent / "qualification.json",
                context,
                outcome,
                scenarios,
            )
        write_producer_result(
            args.qualification_result,
            context,
            outcome,
            evidence_roles=roles,
        )
    except (OSError, ProducerError) as error:
        print(f"FAIL: {error}", file=sys.stderr)
        return 2
    return OUTCOME_EXIT_CODES[outcome]


def static_main(
    *,
    producer_id: str,
    scenarios: Mapping[str, StaticScenario],
    description: str,
    argv: Sequence[str] | None = None,
    environ: Mapping[str, str] | None = None,
    tracked_checker: TrackedChecker = _default_tracked_checker,
) -> int:
    args = _static_parser(description).parse_args(argv)
    environment = os.environ if environ is None else environ
    try:
        context = load_context(
            environment,
            producer_id=producer_id,
            evidence_kind="static",
            scenarios=scenarios,
        )
        outcome = qualify_static_scenarios(
            context,
            scenarios,
            repo=Path.cwd().resolve(),
            tracked_checker=tracked_checker,
        )
        write_producer_result(args.qualification_result, context, outcome)
    except (OSError, ProducerError) as error:
        print(f"FAIL: {error}", file=sys.stderr)
        return 2
    return OUTCOME_EXIT_CODES[outcome]
