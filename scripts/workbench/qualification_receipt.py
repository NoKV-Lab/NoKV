#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Emit source-bound receipts for the pre-#423 Workbench contract ledger."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import pwd
import re
import shutil
import stat
import subprocess
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import pre423_contract_ledger as ledger_module


RECEIPT_SCHEMA = "nokv.pre423.qualification_receipt.v1"
PRODUCER_RESULT_SCHEMA = "nokv.pre423.producer_result.v1"
OUTCOMES = frozenset({"PASS", "NQ", "FAIL"})
ROLE_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._-]*$")
DEPENDENCY_IDENTITIES = {
    "git": re.compile(r"^git:[0-9a-f]{40}$"),
    "oci": re.compile(r"^oci:[^\s@]+@sha256:[0-9a-f]{64}$"),
    "sha256": re.compile(r"^sha256:[0-9a-f]{64}$"),
}
RUST_TOOL_NAMES = ("cargo", "rustc")
RUST_TOOL_FIELDS = frozenset(
    {
        "launcher_path",
        "launcher_kind",
        "resolved_path",
        "resolved_sha256",
        "version_verbose_sha256",
    }
)
RUST_ENVIRONMENT_DENYLIST = frozenset(
    {
        "CARGO",
        "CARGO_BUILD_RUSTC",
        "CARGO_BUILD_RUSTC_WRAPPER",
        "CARGO_ENCODED_RUSTFLAGS",
        "CARGO_HOME",
        "RUSTC",
        "RUSTC_BOOTSTRAP",
        "RUSTC_WRAPPER",
        "RUSTC_WORKSPACE_WRAPPER",
        "RUSTFLAGS",
        "RUSTDOCFLAGS",
        "RUSTUP_DIST_SERVER",
        "RUSTUP_HOME",
        "RUSTUP_TOOLCHAIN",
        "RUSTUP_UPDATE_ROOT",
    }
)


class ReceiptError(ValueError):
    """The command cannot produce a policy-valid qualification receipt."""


@dataclass(frozen=True)
class Claim:
    stable_id: str
    gate: str
    scenario: str


@dataclass(frozen=True)
class ExecutionResult:
    return_code: int
    receipt_paths: tuple[Path, ...]


def _utc_now() -> str:
    return (
        datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    )


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _regular_file_sha256(path: Path, field: str) -> str:
    """Hash one exact regular file without following its final component."""

    try:
        lexical_stat = path.lstat()
    except OSError as err:
        raise ReceiptError(f"{field} is unavailable: {err}") from err
    if not stat.S_ISREG(lexical_stat.st_mode):
        raise ReceiptError(f"{field} must be a regular file")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as err:
        raise ReceiptError(f"cannot safely open {field}: {err}") from err
    digest = hashlib.sha256()
    try:
        opened_stat = os.fstat(descriptor)
        if not stat.S_ISREG(opened_stat.st_mode) or (
            opened_stat.st_dev,
            opened_stat.st_ino,
        ) != (lexical_stat.st_dev, lexical_stat.st_ino):
            raise ReceiptError(f"{field} changed while being validated")
        while chunk := os.read(descriptor, 1024 * 1024):
            digest.update(chunk)
    finally:
        os.close(descriptor)
    try:
        final_stat = path.lstat()
    except OSError as err:
        raise ReceiptError(f"{field} disappeared after validation: {err}") from err
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
        raise ReceiptError(f"{field} changed while being validated")
    return digest.hexdigest()


def _rust_child_environment(
    environ: Mapping[str, str], launchers: Mapping[str, Path]
) -> dict[str, str]:
    child = {
        key: value
        for key, value in environ.items()
        if key not in RUST_ENVIRONMENT_DENYLIST
        and not key.startswith("CARGO_")
        and not key.startswith("RUST")
    }
    child["HOME"] = pwd.getpwuid(os.getuid()).pw_dir
    child["CARGO"] = str(launchers["cargo"])
    child["RUSTC"] = str(launchers["rustc"])
    return child


def _fixed_rust_launcher(name: str) -> Path:
    """Find a host-managed Rust launcher without trusting CARGO or PATH."""

    account_home = Path(pwd.getpwuid(os.getuid()).pw_dir)
    candidates = (
        account_home / ".cargo" / "bin" / name,
        Path("/opt/homebrew/bin") / name,
        Path("/usr/local/bin") / name,
        Path("/usr/bin") / name,
    )
    for candidate in candidates:
        try:
            mode = candidate.lstat().st_mode
        except OSError:
            continue
        if stat.S_ISREG(mode) or stat.S_ISLNK(mode):
            return candidate.parent.resolve() / candidate.name
    raise ReceiptError(
        f"cannot find a host-managed {name} launcher in fixed trusted locations"
    )


def _installed_rust_tool(
    name: str,
    *,
    repo: Path,
    environ: Mapping[str, str],
) -> Path:
    """Resolve the installed toolchain binary behind a fixed rustup proxy."""

    account_home = Path(pwd.getpwuid(os.getuid()).pw_dir)
    rustup_candidates = (
        account_home / ".cargo" / "bin" / "rustup",
        Path("/opt/homebrew/bin/rustup"),
        Path("/usr/local/bin/rustup"),
        Path("/usr/bin/rustup"),
    )
    rustup = next(
        (
            candidate.parent.resolve() / candidate.name
            for candidate in rustup_candidates
            if candidate.exists() and (candidate.is_file() or candidate.is_symlink())
        ),
        None,
    )
    if rustup is None:
        return _fixed_rust_launcher(name)
    discovery_environment = {
        key: value
        for key, value in environ.items()
        if key not in RUST_ENVIRONMENT_DENYLIST
        and not key.startswith("CARGO_")
        and not key.startswith("RUST")
    }
    discovery_environment["HOME"] = str(account_home)
    try:
        completed = subprocess.run(
            [str(rustup), "which", name],
            cwd=repo,
            check=False,
            capture_output=True,
            timeout=30,
            shell=False,
            env=discovery_environment,
        )
    except (OSError, subprocess.TimeoutExpired) as err:
        raise ReceiptError(f"cannot resolve installed Rust tool {name}: {err}") from err
    try:
        output = completed.stdout.decode("utf-8", errors="strict").strip()
    except UnicodeDecodeError as err:
        raise ReceiptError(f"rustup returned a non-UTF-8 path for {name}") from err
    if completed.returncode != 0 or not output or "\n" in output:
        detail = completed.stderr.decode("utf-8", errors="replace").strip()
        raise ReceiptError(
            f"rustup could not resolve the installed {name} binary: {detail}"
        )
    candidate = Path(output)
    if not candidate.is_absolute() or Path(os.path.abspath(output)) != candidate:
        raise ReceiptError(f"rustup returned a non-canonical path for {name}")
    try:
        return candidate.resolve(strict=True)
    except OSError as err:
        raise ReceiptError(f"installed Rust tool {name} is unavailable: {err}") from err


def derive_rust_toolchain_subject(
    environ: Mapping[str, str] | None = None,
    repo: Path | None = None,
) -> dict[str, dict[str, str]]:
    """Bind Cargo and rustc launchers, resolved executables, and version output."""

    environment = os.environ if environ is None else environ
    resolved_repo = Path.cwd().resolve() if repo is None else repo.resolve()
    launchers = {
        name: _installed_rust_tool(
            name,
            repo=resolved_repo,
            environ=environment,
        )
        for name in RUST_TOOL_NAMES
    }
    child_environment = _rust_child_environment(environment, launchers)
    subject: dict[str, dict[str, str]] = {}
    for name in RUST_TOOL_NAMES:
        launcher = launchers[name]
        launcher_stat = launcher.lstat()
        launcher_kind = "symlink" if stat.S_ISLNK(launcher_stat.st_mode) else "regular"
        try:
            resolved = launcher.resolve(strict=True)
        except OSError as err:
            raise ReceiptError(
                f"cannot resolve Rust launcher {launcher}: {err}"
            ) from err
        resolved_sha256 = _regular_file_sha256(
            resolved, f"rust_toolchain.{name}.resolved_path"
        )
        try:
            completed = subprocess.run(
                [str(launcher), "-Vv"],
                check=False,
                capture_output=True,
                timeout=30,
                shell=False,
                env=child_environment,
            )
        except (OSError, subprocess.TimeoutExpired) as err:
            raise ReceiptError(
                f"cannot identify Rust launcher {launcher}: {err}"
            ) from err
        if completed.returncode != 0 or not completed.stdout:
            detail = completed.stderr.decode("utf-8", errors="replace").strip()
            raise ReceiptError(
                f"Rust launcher {launcher} did not return a usable -Vv identity: {detail}"
            )
        version_digest = hashlib.sha256(
            completed.stdout + b"\0" + completed.stderr
        ).hexdigest()
        subject[name] = {
            "launcher_path": str(launcher),
            "launcher_kind": launcher_kind,
            "resolved_path": str(resolved),
            "resolved_sha256": resolved_sha256,
            "version_verbose_sha256": version_digest,
        }
    return subject


def validate_rust_toolchain_subject(
    value: Any,
    environ: Mapping[str, str] | None = None,
    repo: Path | None = None,
) -> dict[str, dict[str, str]]:
    """Re-derive and exactly compare a closed Rust toolchain subject."""

    if not isinstance(value, dict) or set(value) != set(RUST_TOOL_NAMES):
        raise ReceiptError("rust_toolchain must contain exactly cargo and rustc")
    for name in RUST_TOOL_NAMES:
        tool = value.get(name)
        if not isinstance(tool, dict) or set(tool) != RUST_TOOL_FIELDS:
            raise ReceiptError(
                f"rust_toolchain.{name} must use the exact closed tool schema"
            )
        if tool.get("launcher_kind") not in {"regular", "symlink"}:
            raise ReceiptError(
                f"rust_toolchain.{name}.launcher_kind must be regular or symlink"
            )
        for field in ("launcher_path", "resolved_path"):
            raw_path = tool.get(field)
            if not isinstance(raw_path, str) or not Path(raw_path).is_absolute():
                raise ReceiptError(
                    f"rust_toolchain.{name}.{field} must be an absolute path"
                )
            if Path(os.path.abspath(raw_path)) != Path(raw_path):
                raise ReceiptError(f"rust_toolchain.{name}.{field} must be normalized")
        for field in ("resolved_sha256", "version_verbose_sha256"):
            digest = tool.get(field)
            if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
                raise ReceiptError(
                    f"rust_toolchain.{name}.{field} must be lowercase SHA-256"
                )
    current = derive_rust_toolchain_subject(environ, repo)
    if value != current:
        raise ReceiptError("rust_toolchain identity does not match the current host")
    return current


def _run_git(repo: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=repo,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise ReceiptError(f"git {' '.join(args)} failed for {repo}: {detail}")
    return completed.stdout.strip()


def tracked_regular_file_identity(repo: Path, relative_path: str) -> tuple[Path, str]:
    """Return a source-bound file identity without following any symlink."""

    resolved_repo = repo.resolve()
    candidate = Path(relative_path)
    if (
        not candidate.parts
        or candidate.is_absolute()
        or ".." in candidate.parts
        or relative_path != candidate.as_posix()
    ):
        raise ReceiptError(
            f"tracked path must be canonical and relative to the checkout: {relative_path!r}"
        )
    lexical_path = resolved_repo / candidate
    try:
        lexical_stat = lexical_path.lstat()
    except OSError as err:
        raise ReceiptError(
            f"producer entrypoint must be a regular tracked file: {relative_path}: {err}"
        ) from err
    if not stat.S_ISREG(lexical_stat.st_mode):
        raise ReceiptError(
            f"producer entrypoint must be a regular tracked file: {relative_path}"
        )
    try:
        resolved_path = lexical_path.resolve(strict=True)
    except OSError as err:
        raise ReceiptError(
            f"cannot resolve producer entrypoint {relative_path}: {err}"
        ) from err
    if resolved_path != lexical_path or not _is_below(resolved_path, resolved_repo):
        raise ReceiptError(
            f"producer entrypoint must be a regular tracked file inside the checkout: "
            f"{relative_path}"
        )

    stage_output = _run_git(resolved_repo, "ls-files", "--stage", "--", relative_path)
    stage_lines = stage_output.splitlines()
    if len(stage_lines) != 1:
        raise ReceiptError(
            f"producer entrypoint must have one tracked index record: {relative_path}"
        )
    metadata, separator, tracked_path = stage_lines[0].partition("\t")
    fields = metadata.split()
    if (
        not separator
        or tracked_path != relative_path
        or len(fields) != 3
        or fields[0] not in {"100644", "100755"}
        or fields[2] != "0"
    ):
        raise ReceiptError(
            f"producer entrypoint must be a regular tracked file: {relative_path}"
        )

    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(lexical_path, flags)
    except OSError as err:
        raise ReceiptError(
            f"cannot safely open producer entrypoint {relative_path}: {err}"
        ) from err
    try:
        opened_stat = os.fstat(descriptor)
        if not stat.S_ISREG(opened_stat.st_mode) or (
            opened_stat.st_dev,
            opened_stat.st_ino,
        ) != (lexical_stat.st_dev, lexical_stat.st_ino):
            raise ReceiptError(
                f"producer entrypoint changed while being validated: {relative_path}"
            )
        digest = hashlib.sha256()
        while chunk := os.read(descriptor, 1024 * 1024):
            digest.update(chunk)
    finally:
        os.close(descriptor)
    return lexical_path, digest.hexdigest()


def lexical_final_path(path: Path, base: Path) -> Path:
    """Normalize parent aliases without resolving the final path component."""

    candidate = path if path.is_absolute() else base / path
    return candidate.parent.resolve() / candidate.name


def _source_identity(repo: Path) -> dict[str, Any]:
    resolved = repo.resolve()
    if not resolved.is_dir():
        raise ReceiptError(f"repository path is not a directory: {resolved}")
    sha = _run_git(resolved, "rev-parse", "HEAD")
    if len(sha) != 40 or any(character not in "0123456789abcdef" for character in sha):
        raise ReceiptError(f"repository HEAD is not a lowercase full SHA: {sha!r}")
    dirty = bool(
        _run_git(resolved, "status", "--porcelain", "--untracked-files=normal")
    )
    return {
        "repository": resolved.name,
        "sha": sha,
        "dirty": dirty,
    }


def _parse_claim(value: str) -> Claim:
    parts = value.split(":", 2)
    if len(parts) != 3 or any(not part.strip() for part in parts):
        raise ReceiptError(f"claims must use STABLE_ID:GATE:SCENARIO, got {value!r}")
    return Claim(*(part.strip() for part in parts))


def _parse_assignment(value: str, field: str) -> tuple[str, str]:
    if "=" not in value:
        raise ReceiptError(f"{field} must use NAME=VALUE, got {value!r}")
    name, assigned = value.split("=", 1)
    if not name.strip() or not assigned.strip():
        raise ReceiptError(f"{field} must use non-empty NAME=VALUE")
    return name.strip(), assigned.strip()


def _validate_claims(
    ledger: dict[str, Any],
    claims: Sequence[Claim],
    evidence_kind: str,
    producer: str,
) -> dict[tuple[str, str], dict[str, Any]]:
    if not claims:
        raise ReceiptError("at least one --claim is required")
    if len(claims) != len(set(claims)):
        raise ReceiptError("qualification claims must not contain duplicates")
    producer_contract = ledger["producer_catalog"][producer]
    if evidence_kind not in producer_contract["evidence_kinds"]:
        raise ReceiptError(
            f"producer {producer!r} cannot emit {evidence_kind!r} evidence"
        )
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for claim in claims:
        expectation = ledger_module.resolve_gate_expectation(
            ledger, claim.stable_id, claim.gate
        )
        if claim.scenario not in expectation["scenarios"]:
            raise ReceiptError(
                f"undeclared scenario {claim.scenario!r} for "
                f"{claim.stable_id}:{claim.gate}"
            )
        if evidence_kind not in expectation["allowed_evidence_kinds"]:
            raise ReceiptError(
                f"evidence kind {evidence_kind!r} is not allowed for "
                f"{claim.stable_id}:{claim.gate}"
            )
        if producer not in expectation["allowed_producers"]:
            raise ReceiptError(
                f"producer {producer!r} is not allowed for "
                f"{claim.stable_id}:{claim.gate}"
            )
        key = (claim.stable_id, claim.gate)
        group = grouped.setdefault(
            key,
            {"expectation": expectation, "scenarios": []},
        )
        group["scenarios"].append(claim.scenario)
    return grouped


def _parse_dependencies(
    values: Sequence[str], required_dependencies: dict[str, list[str]]
) -> list[dict[str, str]]:
    dependencies: list[dict[str, str]] = []
    seen: set[str] = set()
    for value in values:
        name, identity = _parse_assignment(value, "dependency")
        if name in seen:
            raise ReceiptError(f"duplicate dependency identity {name!r}")
        seen.add(name)
        dependencies.append({"name": name, "identity": identity})
    expected_names = set(required_dependencies)
    if seen != expected_names:
        raise ReceiptError(
            "dependency names must exactly match producer contract; "
            f"missing={sorted(expected_names - seen)} extra={sorted(seen - expected_names)}"
        )
    for dependency in dependencies:
        allowed_kinds = required_dependencies[dependency["name"]]
        if not any(
            DEPENDENCY_IDENTITIES[kind].fullmatch(dependency["identity"])
            for kind in allowed_kinds
        ):
            raise ReceiptError(
                f"dependency {dependency['name']!r} identity must use pinned "
                f"kind {sorted(allowed_kinds)}"
            )
    return sorted(dependencies, key=lambda dependency: dependency["name"])


def _is_below(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def _prepare_evidence_root(path: Path, repo: Path) -> Path:
    evidence_root = path.resolve()
    if evidence_root == evidence_root.parent:
        raise ReceiptError("evidence root cannot be a filesystem root")
    if _is_below(evidence_root, repo) or _is_below(repo, evidence_root):
        raise ReceiptError("evidence root must not overlap the qualified checkout")
    if evidence_root.exists():
        if not evidence_root.is_dir():
            raise ReceiptError(f"evidence root is not a directory: {evidence_root}")
        if any(evidence_root.iterdir()):
            raise ReceiptError(f"evidence root must be new or empty: {evidence_root}")
    else:
        evidence_root.mkdir(parents=True)
    return evidence_root


def _parse_evidence(
    values: Sequence[str], evidence_root: Path
) -> list[tuple[str, Path]]:
    declared: list[tuple[str, Path]] = []
    seen: set[str] = {"stdout", "stderr"}
    for value in values:
        role, raw_path = _parse_assignment(value, "evidence")
        if not ROLE_PATTERN.fullmatch(role):
            raise ReceiptError(
                f"evidence role must match {ROLE_PATTERN.pattern}, got {role!r}"
            )
        if role in seen:
            raise ReceiptError(f"duplicate or reserved evidence role {role!r}")
        seen.add(role)
        supplied_path = Path(raw_path)
        if not supplied_path.is_absolute():
            supplied_path = evidence_root / supplied_path
        resolved_parent = supplied_path.parent.resolve()
        path = resolved_parent / supplied_path.name
        if resolved_parent != evidence_root:
            raise ReceiptError(
                f"evidence {role!r} must be a direct child of --evidence-root"
            )
        try:
            path.lstat()
        except FileNotFoundError:
            pass
        else:
            raise ReceiptError(
                f"evidence {role!r} must be newly produced, not preexisting: {path}"
            )
        declared.append((role, path))
    return declared


def _read_declared_evidence(*, role: str, path: Path, evidence_root: Path) -> bytes:
    """Read a command-created direct child without following symbolic links."""

    try:
        link_stat = path.lstat()
    except FileNotFoundError as err:
        raise ReceiptError(
            f"declared evidence {role!r} does not exist: {path}"
        ) from err
    if stat.S_ISLNK(link_stat.st_mode):
        raise ReceiptError(f"declared evidence {role!r} cannot be a symlink")
    if not stat.S_ISREG(link_stat.st_mode):
        raise ReceiptError(f"declared evidence {role!r} must be a regular file")
    try:
        resolved_path = path.resolve(strict=True)
    except OSError as err:
        raise ReceiptError(f"cannot resolve declared evidence {role!r}: {err}") from err
    if resolved_path.parent != evidence_root:
        raise ReceiptError(
            f"declared evidence {role!r} escapes --evidence-root after execution"
        )
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as err:
        raise ReceiptError(
            f"cannot safely open declared evidence {role!r}: {err}"
        ) from err
    try:
        opened_stat = os.fstat(descriptor)
        if not stat.S_ISREG(opened_stat.st_mode):
            raise ReceiptError(f"declared evidence {role!r} must remain a regular file")
        if (opened_stat.st_dev, opened_stat.st_ino) != (
            link_stat.st_dev,
            link_stat.st_ino,
        ):
            raise ReceiptError(
                f"declared evidence {role!r} changed while being validated"
            )
        chunks: list[bytes] = []
        while chunk := os.read(descriptor, 1024 * 1024):
            chunks.append(chunk)
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def _validate_command_contract(
    *,
    ledger: dict[str, Any],
    producer: str,
    repo: Path,
    argv: Sequence[str],
    evidence_by_role: dict[str, Path],
    binary_subject: dict[str, str] | None,
) -> dict[str, str]:
    producer_contract = ledger["producer_catalog"][producer]
    command_contract = producer_contract["command"]
    required_roles = set(producer_contract["required_evidence_roles"])
    missing_roles = required_roles - set(evidence_by_role)
    if missing_roles:
        raise ReceiptError(
            f"producer {producer!r} requires evidence roles {sorted(missing_roles)}"
        )
    if len(argv) < 2:
        raise ReceiptError(
            f"producer {producer!r} requires its source-bound Python entrypoint"
        )
    executable = Path(argv[0]) if Path(argv[0]).is_absolute() else None
    if executable is None:
        located = shutil.which(argv[0])
        executable = Path(located) if located else None
    if executable is None or not executable.resolve().is_file():
        raise ReceiptError(f"Python executable is unavailable: {argv[0]!r}")
    executable = executable.resolve()
    runner_executable = Path(sys.executable).resolve()
    if executable != runner_executable:
        raise ReceiptError(
            f"producer {producer!r} must use the runner Python executable "
            f"{runner_executable}"
        )

    entrypoint = command_contract["entrypoint"]
    expected_entrypoint, entrypoint_sha256 = tracked_regular_file_identity(
        repo, entrypoint
    )
    actual_entrypoint = Path(argv[1])
    actual_entrypoint = lexical_final_path(actual_entrypoint, repo)
    if actual_entrypoint != expected_entrypoint:
        raise ReceiptError(
            f"producer {producer!r} must execute exact entrypoint {entrypoint}"
        )

    for forbidden in command_contract["forbidden_arguments"]:
        if forbidden in argv[2:]:
            raise ReceiptError(
                f"producer {producer!r} forbids qualification argument {forbidden}"
            )
    result_argument = command_contract["result_argument"]
    result_positions = [
        index
        for index, argument in enumerate(argv[2:], start=2)
        if argument == result_argument
    ]
    if len(result_positions) != 1 or result_positions[0] + 1 >= len(argv):
        raise ReceiptError(
            f"producer {producer!r} requires exactly one {result_argument} PATH"
        )
    result_path = Path(argv[result_positions[0] + 1])
    if not result_path.is_absolute():
        result_path = repo / result_path
    result_path = result_path.resolve()
    if result_path != evidence_by_role["producer-result"]:
        raise ReceiptError(
            f"{result_argument} must equal producer-result evidence path"
        )
    binary_argument = command_contract["binary_argument"]
    if binary_argument is not None:
        binary_positions = [
            index
            for index, argument in enumerate(argv[2:], start=2)
            if argument == binary_argument
        ]
        if len(binary_positions) != 1 or binary_positions[0] + 1 >= len(argv):
            raise ReceiptError(
                f"producer {producer!r} requires exactly one product binary "
                f"argument {binary_argument} PATH"
            )
        command_binary = Path(argv[binary_positions[0] + 1])
        if not command_binary.is_absolute():
            command_binary = repo / command_binary
        command_binary = command_binary.resolve()
        if binary_subject is None or command_binary != Path(binary_subject["path"]):
            raise ReceiptError(
                f"product binary argument {binary_argument} must match --binary"
            )
    elif binary_subject is not None:
        raise ReceiptError(
            f"producer {producer!r} does not accept a product binary subject"
        )
    return {
        "command_contract_sha256": ledger_module.json_sha256(producer_contract),
        "entrypoint": entrypoint,
        "entrypoint_sha256": entrypoint_sha256,
        "executable": str(executable),
        "executable_sha256": _sha256_file(executable),
        "producer_result_source_path": str(result_path),
    }


def _validate_producer_result(
    *,
    payload: bytes,
    producer: str,
    evidence_kind: str,
    operation_id: str,
    source_sha: str,
    command_argv_sha256: str,
    subjects: dict[str, Any],
    scenario_ids: set[str],
    command_outcome: str,
    required_evidence_roles: set[str],
    declared_evidence_roles: set[str],
) -> dict[str, Any]:
    try:
        value = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as err:
        raise ReceiptError(f"cannot load structured producer result: {err}") from err
    if not isinstance(value, dict):
        raise ReceiptError("structured producer result must be a JSON object")
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
    if set(value) != expected_fields:
        raise ReceiptError(
            f"structured producer result keys must be exactly {sorted(expected_fields)}"
        )
    expected_scalars = {
        "schema": PRODUCER_RESULT_SCHEMA,
        "producer": producer,
        "evidence_kind": evidence_kind,
        "operation_id": operation_id,
        "source_sha": source_sha,
        "command_argv_sha256": command_argv_sha256,
        "subjects_sha256": ledger_module.json_sha256(subjects),
    }
    for field, expected in expected_scalars.items():
        if value.get(field) != expected:
            raise ReceiptError(
                f"structured producer result {field} does not match runner context"
            )
    if value.get("subjects") != subjects:
        raise ReceiptError(
            "structured producer result subjects do not match runner subjects"
        )
    scenarios = value.get("scenarios")
    if not isinstance(scenarios, dict) or set(scenarios) != scenario_ids:
        raise ReceiptError(
            "structured producer result scenarios must exactly match validated claims"
        )
    for scenario, result in scenarios.items():
        if not isinstance(result, dict) or set(result) != {
            "outcome",
            "evidence_roles",
        }:
            raise ReceiptError(
                f"structured result for {scenario} must contain outcome and evidence_roles"
            )
        if result.get("outcome") != command_outcome:
            raise ReceiptError(
                f"structured result for {scenario} disagrees with command outcome "
                f"{command_outcome}"
            )
        roles = result.get("evidence_roles")
        if (
            not isinstance(roles, list)
            or any(not isinstance(role, str) for role in roles)
            or len(roles) != len(set(roles))
        ):
            raise ReceiptError(
                f"structured result for {scenario} has invalid evidence_roles"
            )
        if not required_evidence_roles.issubset(roles) or not set(roles).issubset(
            declared_evidence_roles
        ):
            raise ReceiptError(
                f"structured result for {scenario} does not bind required evidence roles"
            )
    return value


def _evidence_entry(
    *, role: str, path: Path, bundle_dir: Path, media_type: str
) -> dict[str, Any]:
    payload_size = path.stat().st_size
    return {
        "role": role,
        "path": path.relative_to(bundle_dir).as_posix(),
        "sha256": _sha256_file(path),
        "size_bytes": payload_size,
        "media_type": media_type,
    }


def execute_qualification(
    *,
    ledger: dict[str, Any],
    repo: Path,
    output_dir: Path,
    evidence_root: Path,
    producer: str,
    evidence_kind: str,
    claim_values: Sequence[str],
    argv: Sequence[str],
    binary: Path | None = None,
    dependency_values: Sequence[str] = (),
    evidence_values: Sequence[str] = (),
    workflow_run_id: str = "local",
    job: str = "local",
    attempt: int = 1,
) -> ExecutionResult:
    """Execute ``argv`` once and emit one receipt per claimed item and gate."""

    ledger_module.validate_ledger(ledger)
    if evidence_kind not in ledger_module.ALLOWED_EVIDENCE_KINDS:
        raise ReceiptError(f"unknown evidence kind {evidence_kind!r}")
    producer_catalog = ledger.get("producer_catalog", {})
    if producer not in producer_catalog:
        raise ReceiptError(f"unknown producer {producer!r}")
    if not argv:
        raise ReceiptError("a command after -- is required")
    if attempt < 1:
        raise ReceiptError("workflow attempt must be positive")

    claims = [_parse_claim(value) for value in claim_values]
    grouped = _validate_claims(ledger, claims, evidence_kind, producer)
    producer_contract = producer_catalog[producer]
    dependencies = _parse_dependencies(
        dependency_values, producer_contract["required_dependencies"]
    )
    resolved_repo = repo.resolve()
    source = _source_identity(resolved_repo)
    resolved_evidence_root = _prepare_evidence_root(evidence_root, resolved_repo)
    declared_evidence = _parse_evidence(evidence_values, resolved_evidence_root)
    evidence_by_role = dict(declared_evidence)

    binary_subject: dict[str, str] | None = None
    if binary is not None:
        resolved_binary = binary.resolve()
        if not resolved_binary.is_file():
            raise ReceiptError(f"product binary does not exist: {resolved_binary}")
        binary_subject = {
            "path": str(resolved_binary),
            "sha256": _sha256_file(resolved_binary),
        }
    required_subjects = set(producer_contract["required_subjects"])
    if "product_binary" in required_subjects and binary_subject is None:
        raise ReceiptError(f"producer {producer!r} requires --binary product identity")
    if "dependencies" in required_subjects and not dependencies:
        raise ReceiptError(f"producer {producer!r} requires a --dependency identity")
    subjects: dict[str, Any] = {"dependencies": dependencies}
    if binary_subject is not None:
        subjects["product_binary"] = binary_subject
    if "rust_toolchain" in required_subjects:
        subjects["rust_toolchain"] = derive_rust_toolchain_subject(repo=resolved_repo)
    command_identity = _validate_command_contract(
        ledger=ledger,
        producer=producer,
        repo=resolved_repo,
        argv=argv,
        evidence_by_role=evidence_by_role,
        binary_subject=binary_subject,
    )

    run_id = uuid.uuid4().hex
    operation_id = run_id
    command_argv_sha256 = ledger_module.json_sha256(list(argv))
    subjects_sha256 = ledger_module.json_sha256(subjects)
    bundle_dir = output_dir.resolve()
    run_dir = bundle_dir / "runs" / run_id
    receipt_dir = bundle_dir / "receipts"
    run_dir.mkdir(parents=True, exist_ok=False)
    receipt_dir.mkdir(parents=True, exist_ok=True)

    started_at = _utc_now()
    launch_error: str | None = None
    if "rust_toolchain" in required_subjects:
        rust_tools = subjects["rust_toolchain"]
        command_environment = _rust_child_environment(
            os.environ,
            {name: Path(rust_tools[name]["launcher_path"]) for name in RUST_TOOL_NAMES},
        )
    else:
        command_environment = os.environ.copy()
    command_environment.update(
        {
            "NOKV_QUALIFICATION_OPERATION_ID": operation_id,
            "NOKV_QUALIFICATION_PRODUCER": producer,
            "NOKV_QUALIFICATION_EVIDENCE_KIND": evidence_kind,
            "NOKV_QUALIFICATION_SOURCE_SHA": source["sha"],
            "NOKV_QUALIFICATION_COMMAND_ARGV_SHA256": command_argv_sha256,
            "NOKV_QUALIFICATION_SUBJECTS": ledger_module.canonical_json(
                subjects
            ).decode("utf-8"),
            "NOKV_QUALIFICATION_SUBJECTS_SHA256": subjects_sha256,
            "NOKV_QUALIFICATION_CLAIMS": json.dumps(
                [
                    {
                        "stable_id": claim.stable_id,
                        "gate": claim.gate,
                        "scenario": claim.scenario,
                    }
                    for claim in claims
                ],
                separators=(",", ":"),
                sort_keys=True,
            ),
            "NOKV_QUALIFICATION_REQUIRED_EVIDENCE_ROLES": json.dumps(
                producer_contract["required_evidence_roles"],
                separators=(",", ":"),
            ),
        }
    )
    try:
        completed = subprocess.run(
            list(argv),
            cwd=resolved_repo,
            check=False,
            capture_output=True,
            env=command_environment,
            shell=False,
        )
        command_exit_code = completed.returncode
        stdout = completed.stdout
        stderr = completed.stderr
    except OSError as err:
        command_exit_code = 127
        stdout = b""
        stderr = f"cannot execute qualification command: {err}\n".encode("utf-8")
        launch_error = str(err)
    finished_at = _utc_now()

    sys.stdout.buffer.write(stdout)
    sys.stdout.buffer.flush()
    sys.stderr.buffer.write(stderr)
    sys.stderr.buffer.flush()

    stdout_path = run_dir / "stdout.log"
    stderr_path = run_dir / "stderr.log"
    stdout_path.write_bytes(stdout)
    stderr_path.write_bytes(stderr)
    evidence_entries = [
        _evidence_entry(
            role="stdout",
            path=stdout_path,
            bundle_dir=bundle_dir,
            media_type="text/plain",
        ),
        _evidence_entry(
            role="stderr",
            path=stderr_path,
            bundle_dir=bundle_dir,
            media_type="text/plain",
        ),
    ]

    command_outcome = (
        "PASS" if command_exit_code == 0 else "NQ" if command_exit_code == 3 else "FAIL"
    )
    qualification_errors: list[str] = []
    try:
        source_after_execution = _source_identity(resolved_repo)
    except ReceiptError as err:
        qualification_errors.append(
            f"cannot revalidate source identity after execution: {err}"
        )
    else:
        if source_after_execution != source:
            qualification_errors.append(
                "source identity changed during qualification: "
                f"before={source} after={source_after_execution}"
            )
    if binary_subject is not None:
        product_binary = Path(binary_subject["path"])
        if (
            not product_binary.is_file()
            or _sha256_file(product_binary) != binary_subject["sha256"]
        ):
            qualification_errors.append(
                "product binary identity changed during qualification"
            )
    if "rust_toolchain" in required_subjects:
        try:
            validate_rust_toolchain_subject(
                subjects.get("rust_toolchain"), repo=resolved_repo
            )
        except ReceiptError as err:
            qualification_errors.append(
                f"Rust toolchain identity changed during qualification: {err}"
            )
    executable_path = Path(command_identity["executable"])
    try:
        _, current_entrypoint_sha256 = tracked_regular_file_identity(
            resolved_repo, command_identity["entrypoint"]
        )
    except ReceiptError as err:
        qualification_errors.append(
            f"producer entrypoint identity changed during qualification: {err}"
        )
    else:
        if current_entrypoint_sha256 != command_identity["entrypoint_sha256"]:
            qualification_errors.append(
                "producer entrypoint identity changed during qualification"
            )
    if (
        not executable_path.is_file()
        or _sha256_file(executable_path) != command_identity["executable_sha256"]
    ):
        qualification_errors.append(
            "producer executable identity changed during qualification"
        )

    evidence_payloads: dict[str, bytes] = {}
    for role, source_path in declared_evidence:
        try:
            evidence_payloads[role] = _read_declared_evidence(
                role=role,
                path=source_path,
                evidence_root=resolved_evidence_root,
            )
        except ReceiptError as err:
            qualification_errors.append(str(err))

    producer_result: dict[str, Any] | None = None
    producer_result_payload = evidence_payloads.get("producer-result")
    if producer_result_payload is not None:
        try:
            producer_result = _validate_producer_result(
                payload=producer_result_payload,
                producer=producer,
                evidence_kind=evidence_kind,
                operation_id=operation_id,
                source_sha=source["sha"],
                command_argv_sha256=command_argv_sha256,
                subjects=subjects,
                scenario_ids={claim.scenario for claim in claims},
                command_outcome=command_outcome,
                required_evidence_roles=set(
                    producer_contract["required_evidence_roles"]
                ),
                declared_evidence_roles=set(evidence_by_role),
            )
        except ReceiptError as err:
            qualification_errors.append(str(err))
    else:
        qualification_errors.append(
            "producer did not create the required structured producer-result"
        )

    copied_evidence_dir = run_dir / "evidence"
    for index, (role, source_path) in enumerate(declared_evidence):
        payload = evidence_payloads.get(role)
        if payload is None:
            continue
        copied_evidence_dir.mkdir(parents=True, exist_ok=True)
        destination = copied_evidence_dir / f"{index:02d}-{source_path.name}"
        destination.write_bytes(payload)
        evidence_entries.append(
            _evidence_entry(
                role=role,
                path=destination,
                bundle_dir=bundle_dir,
                media_type="application/octet-stream",
            )
        )

    if launch_error is not None:
        qualification_errors.append(f"command launch failed: {launch_error}")
    if qualification_errors:
        outcome = "FAIL"
        runner_return_code = 2
    else:
        outcome = command_outcome
        runner_return_code = {
            "PASS": 0,
            "NQ": 3,
            "FAIL": command_exit_code if 0 < command_exit_code <= 255 else 2,
        }[outcome]

    item_by_id = {item["id"]: item for item in ledger["items"]}
    receipt_paths: list[Path] = []
    for (stable_id, gate), group in sorted(grouped.items()):
        expectation = group["expectation"]
        receipt = {
            "schema": RECEIPT_SCHEMA,
            "stable_id": stable_id,
            "gate": gate,
            "scenario_ids": sorted(group["scenarios"]),
            "evidence_kind": evidence_kind,
            "outcome": outcome,
            "source": {
                **source,
                "ledger_item_sha256": ledger_module.json_sha256(item_by_id[stable_id]),
                "gate_expectation_sha256": ledger_module.json_sha256(expectation),
                "qualification_policy_sha256": (
                    ledger_module.QUALIFICATION_POLICY_SHA256
                ),
            },
            "execution": {
                "producer": producer,
                "workflow_run_id": workflow_run_id,
                "job": job,
                "attempt": attempt,
                "operation_id": operation_id,
                "argv": list(argv),
                "command_argv_sha256": command_argv_sha256,
                **command_identity,
                "cwd": str(resolved_repo),
                "started_at": started_at,
                "finished_at": finished_at,
                "exit_code": command_exit_code,
                "producer_result_sha256": (
                    ledger_module.json_sha256(producer_result)
                    if producer_result is not None
                    else None
                ),
            },
            "subjects": subjects,
            "evidence": evidence_entries,
        }
        if qualification_errors:
            receipt["qualification_errors"] = qualification_errors
        receipt_path = receipt_dir / f"{stable_id}-{gate}-{run_id}.json"
        temporary_path = receipt_path.with_suffix(".json.tmp")
        temporary_path.write_text(
            json.dumps(receipt, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary_path.replace(receipt_path)
        receipt_paths.append(receipt_path)
    return ExecutionResult(runner_return_code, tuple(receipt_paths))


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Execute one pre-#423 Workbench contract ledger producer and emit "
            "source-bound receipts."
        )
    )
    parser.add_argument("--ledger", type=Path, default=ledger_module.LEDGER_PATH)
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--evidence-root", type=Path, required=True)
    parser.add_argument("--producer", required=True)
    parser.add_argument(
        "--evidence-kind",
        required=True,
        choices=sorted(ledger_module.ALLOWED_EVIDENCE_KINDS),
    )
    parser.add_argument("--claim", action="append", default=[])
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--dependency", action="append", default=[])
    parser.add_argument("--evidence", action="append", default=[])
    parser.add_argument(
        "--workflow-run-id", default=os.environ.get("GITHUB_RUN_ID", "local")
    )
    parser.add_argument("--job", default=os.environ.get("GITHUB_JOB", "local"))
    parser.add_argument(
        "--attempt",
        type=int,
        default=int(os.environ.get("GITHUB_RUN_ATTEMPT", "1")),
    )
    parser.add_argument("command", nargs=argparse.REMAINDER)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    command = list(args.command)
    if command and command[0] == "--":
        command.pop(0)
    try:
        github_run_id = os.environ.get("GITHUB_RUN_ID")
        if github_run_id is not None and args.workflow_run_id != github_run_id:
            raise ReceiptError("--workflow-run-id cannot override GITHUB_RUN_ID in CI")
        github_attempt = os.environ.get("GITHUB_RUN_ATTEMPT")
        if github_attempt is not None and args.attempt != int(github_attempt):
            raise ReceiptError("--attempt cannot override GITHUB_RUN_ATTEMPT in CI")
        github_job = os.environ.get("GITHUB_JOB")
        if github_job is not None and args.job != github_job:
            raise ReceiptError("--job cannot override GITHUB_JOB in CI")
        result = execute_qualification(
            ledger=ledger_module.load_ledger(args.ledger),
            repo=args.repo,
            output_dir=args.output_dir,
            evidence_root=args.evidence_root,
            producer=args.producer,
            evidence_kind=args.evidence_kind,
            claim_values=args.claim,
            argv=command,
            binary=args.binary,
            dependency_values=args.dependency,
            evidence_values=args.evidence,
            workflow_run_id=args.workflow_run_id,
            job=args.job,
            attempt=args.attempt,
        )
    except (ledger_module.LedgerError, ReceiptError) as err:
        print(f"FAIL: {err}", file=sys.stderr)
        return 2
    for path in result.receipt_paths:
        print(f"receipt={path}", file=sys.stderr)
    return result.return_code


if __name__ == "__main__":
    raise SystemExit(main())
