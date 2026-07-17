#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Verify and safely switch one LingTai agent to an immutable NoKV MCP."""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Iterator

import install_workbench_mcp as installer
from nokv_runtime import (
    BuildInfo,
    SourceIdentity,
    discover_nokv_binary,
    identity_from_mapping,
    infer_distribution,
    load_build_info,
    sha256_file,
    source_identity,
    stage_runtime,
    validate_revision,
    validate_sha256,
)
from workbench_contract import (
    WorkbenchContractError,
    contract_evidence,
    expected_contract_evidence,
    extract_raw_tools,
    json_sha256,
)


LOCK_SCHEMA = "nokv.lingtai.workbench_lock.v1"
LOCK_NAME = "nokv-workbench.lock.json"
SYNC_LOCK_NAME = ".nokv-workbench.sync.lock"
TRANSACTION_NAME = ".nokv-workbench.transaction.json"
TRANSACTION_SCHEMA = "nokv.lingtai.workbench_transaction.v1"


def resolve_build_info(binary: Path, explicit: str | None) -> Path | None:
    if explicit:
        return Path(explicit).expanduser().resolve()
    candidates = [
        binary.parent / "build-info.json",
        binary.parent.parent / "share" / "nokv" / "build-info.json",
    ]
    return next((path for path in candidates if path.is_file()), None)


def resolve_artifact_identity(
    binary: Path,
    *,
    build_info: str | None,
    revision: str | None,
) -> BuildInfo:
    info_path = resolve_build_info(binary, build_info)
    if info_path is None:
        raise ValueError(
            "binary identity is unavailable; use --build-source for a source build "
            "or pass --build-info from its Brew/Release artifact"
        )
    build_info_value = load_build_info(info_path)
    candidate_sha256 = sha256_file(binary)
    candidate_size = binary.stat().st_size
    if candidate_sha256 != build_info_value.binary_sha256:
        raise ValueError(
            "candidate NoKV binary does not match build-info SHA-256: "
            f"{candidate_sha256} != {build_info_value.binary_sha256}"
        )
    if candidate_size != build_info_value.binary_size_bytes:
        raise ValueError(
            "candidate NoKV binary does not match build-info size: "
            f"{candidate_size} != {build_info_value.binary_size_bytes}"
        )
    identity = build_info_value.identity
    if revision and identity.nokv_git_commit != validate_revision(revision):
        raise ValueError(
            f"build-info revision {identity.nokv_git_commit} does not match {revision}"
        )
    return build_info_value


def build_source_candidate(
    source_root: Path,
    *,
    revision: str | None,
    allow_dirty: bool,
) -> tuple[Path, SourceIdentity]:
    root = source_root.expanduser().resolve()
    before = source_identity(root, revision)
    if before.source_dirty and not allow_dirty:
        raise ValueError(
            "NoKV source identity is dirty; commit/stash it or pass --allow-dirty "
            "for local testing"
        )
    target_dir = root / "target" / "lingtai-workbench-source"
    candidate = target_dir / "release" / "nokv"
    try:
        candidate.unlink()
    except FileNotFoundError:
        pass
    completed = subprocess.run(
        [
            "cargo",
            "build",
            "--locked",
            "--release",
            "--target-dir",
            str(target_dir),
            "--manifest-path",
            str(root / "Cargo.toml"),
            "-p",
            "nokv",
            "--bin",
            "nokv",
        ],
        check=False,
    )
    if completed.returncode != 0:
        raise ValueError(f"locked NoKV source build failed with {completed.returncode}")
    after = source_identity(root, before.nokv_git_commit)
    if after != before:
        raise ValueError("NoKV source identity changed while the binary was building")
    if not candidate.is_file() or not os.access(candidate, os.X_OK):
        raise FileNotFoundError(
            f"source build did not produce an executable: {candidate}"
        )
    return candidate.resolve(), after


def concrete_workbench_root(template: str, agent_dir: Path) -> str:
    concrete = (
        template.replace("{agent_id}", agent_dir.name)
        .replace("{agent_address}", agent_dir.name)
        .replace("{agent_dir}", str(agent_dir))
    )
    if "{" in concrete or "}" in concrete:
        raise ValueError(f"workbench root contains an unknown placeholder: {template}")
    return concrete


def raw_tools_list(
    config: installer.InstallConfig,
    *,
    timeout_seconds: float,
) -> list[dict[str, Any]]:
    request = json.dumps(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
        separators=(",", ":"),
    )
    try:
        completed = subprocess.run(
            [config.nokv_bin, *installer.mcp_args(config)],
            input=request + "\n",
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as err:
        raise ValueError(
            f"NoKV tools/list timed out after {timeout_seconds:g}s"
        ) from err
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise ValueError(
            f"NoKV tools/list exited with {completed.returncode}: {detail}"
        )
    lines = [line for line in completed.stdout.splitlines() if line.strip()]
    if len(lines) != 1:
        raise ValueError(
            f"NoKV tools/list must return exactly one JSON line, got {len(lines)}"
        )
    try:
        response = json.loads(lines[0])
    except json.JSONDecodeError as err:
        raise ValueError(f"NoKV tools/list returned invalid JSON: {err}") from err
    try:
        tools = extract_raw_tools(response)
        contract_evidence(tools)
    except WorkbenchContractError as err:
        raise ValueError(str(err)) from err
    return tools


def registration_source(identity: SourceIdentity, binary_sha256: str) -> str:
    dirty = "+dirty" if identity.source_dirty else ""
    return f"NoKV-Lab/NoKV@{identity.nokv_git_commit}{dirty}#sha256:{binary_sha256}"


def build_lock(
    config: installer.InstallConfig,
    *,
    concrete_root: str,
    distribution: str,
    identity: SourceIdentity,
    binary_sha256: str,
    binary_size: int,
    tools: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "schema": LOCK_SCHEMA,
        "artifact": {
            "command": config.nokv_bin,
            "sha256": binary_sha256,
            "size_bytes": binary_size,
        },
        "source": {
            "distribution": distribution,
            **identity.as_dict(),
        },
        "launch": {
            "transport": "stdio",
            "mcp_name": config.mcp_name,
            "profile": "workbench",
            "server_bind": config.server_bind,
            "object_backend": config.object_backend,
            "s3_endpoint": config.s3_endpoint,
            "s3_bucket": config.s3_bucket,
            "workbench_root_template": config.workbench_root,
            "workbench_root": concrete_root,
            "args_sha256": json_sha256(installer.mcp_args(config)),
        },
        "contract": contract_evidence(tools),
    }


def read_lock(path: Path) -> dict[str, Any]:
    try:
        text = installer.read_regular_text(path, missing_ok=False)
    except FileNotFoundError as err:
        raise FileNotFoundError(
            f"NoKV workbench lock does not exist: {path}"
        ) from err
    assert text is not None
    data = json.loads(text)
    if not isinstance(data, dict) or data.get("schema") != LOCK_SCHEMA:
        raise ValueError(f"{path} is not a {LOCK_SCHEMA} object")
    for field in ("artifact", "source", "launch", "contract"):
        if not isinstance(data.get(field), dict):
            raise ValueError(f"{path}: {field} must be a JSON object")
    return data


def config_from_lock(lock: dict[str, Any]) -> installer.InstallConfig:
    artifact = lock["artifact"]
    launch = lock["launch"]
    source = lock["source"]
    command = artifact.get("command")
    required_strings = {
        "command": command,
        "server_bind": launch.get("server_bind"),
        "object_backend": launch.get("object_backend"),
        "s3_bucket": launch.get("s3_bucket"),
        "workbench_root_template": launch.get("workbench_root_template"),
        "mcp_name": launch.get("mcp_name"),
        "nokv_git_commit": source.get("nokv_git_commit"),
        "binary_sha256": artifact.get("sha256"),
    }
    for field, value in required_strings.items():
        if not isinstance(value, str) or not value:
            raise ValueError(f"workbench lock {field} must be a non-empty string")
    validate_revision(source["nokv_git_commit"])
    validate_sha256(artifact["sha256"])
    endpoint = launch.get("s3_endpoint")
    if endpoint is not None and not isinstance(endpoint, str):
        raise ValueError("workbench lock s3_endpoint must be a string or null")
    identity = identity_from_mapping(source, context="workbench lock source")
    return installer.InstallConfig(
        nokv_bin=command,
        server_bind=launch["server_bind"],
        object_backend=launch["object_backend"],
        s3_endpoint=endpoint,
        s3_bucket=launch["s3_bucket"],
        workbench_root=launch["workbench_root_template"],
        mcp_name=launch["mcp_name"],
        source=registration_source(identity, artifact["sha256"]),
    )


def verify_agent_configuration(
    agent_dir: Path,
    config: installer.InstallConfig,
) -> None:
    records = installer.read_registry(agent_dir / "mcp_registry.jsonl")
    matches = [record for record in records if record.get("name") == config.mcp_name]
    if matches != [installer.registry_record(config)]:
        raise ValueError("LingTai MCP registry does not match the NoKV workbench lock")
    init = installer.read_init(agent_dir / "init.json")
    mcp = init.get("mcp")
    if not isinstance(mcp, dict) or mcp.get(config.mcp_name) != installer.init_spec(
        config
    ):
        raise ValueError("LingTai init.json does not match the NoKV workbench lock")


@contextlib.contextmanager
def agent_sync_lock(agent_dir: Path, *, exclusive: bool) -> Iterator[None]:
    lock_path = agent_dir / SYNC_LOCK_NAME
    if exclusive:
        flags = os.O_CREAT | os.O_RDWR
    else:
        if not lock_path.is_file() or lock_path.is_symlink():
            raise FileNotFoundError(
                f"NoKV workbench sync lock does not exist: {lock_path}"
            )
        flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(lock_path, flags, 0o600)
    operation = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
    try:
        try:
            fcntl.flock(descriptor, operation | fcntl.LOCK_NB)
        except BlockingIOError as err:
            raise RuntimeError(
                f"another NoKV workbench sync is active for {agent_dir}"
            ) from err
        yield
    finally:
        os.close(descriptor)


def _restore_text(path: Path, text: str | None) -> None:
    if text is None:
        try:
            path.lstat()
        except FileNotFoundError:
            pass
        else:
            path.unlink()
            installer.fsync_directory(path.parent)
        return
    installer.write_text_if_changed(path, text)


def _transaction_files(agent_dir: Path) -> dict[str, Path]:
    return {
        "mcp_registry.jsonl": agent_dir / "mcp_registry.jsonl",
        "init.json": agent_dir / "init.json",
        LOCK_NAME: agent_dir / LOCK_NAME,
    }


def read_transaction(agent_dir: Path) -> dict[str, Any] | None:
    path = agent_dir / TRANSACTION_NAME
    text = installer.read_regular_text(path, missing_ok=True)
    if text is None:
        return None
    data = json.loads(text)
    if not isinstance(data, dict) or data.get("schema") != TRANSACTION_SCHEMA:
        raise ValueError(f"invalid interrupted workbench transaction: {path}")
    expected_names = set(_transaction_files(agent_dir))
    for field in ("original", "desired"):
        values = data.get(field)
        if not isinstance(values, dict) or set(values) != expected_names:
            raise ValueError(
                f"invalid interrupted workbench transaction {field}: {path}"
            )
        if any(
            value is not None and not isinstance(value, str)
            for value in values.values()
        ):
            raise ValueError(
                f"invalid interrupted workbench transaction {field} values: {path}"
            )
    return data


def recover_interrupted_update(agent_dir: Path) -> bool:
    transaction = read_transaction(agent_dir)
    if transaction is None:
        return False
    paths = _transaction_files(agent_dir)
    desired_matches = all(
        installer.read_regular_text(path, missing_ok=True)
        == transaction["desired"][name]
        for name, path in paths.items()
    )
    if not desired_matches:
        for name, path in paths.items():
            _restore_text(path, transaction["original"][name])
    (agent_dir / TRANSACTION_NAME).unlink()
    installer.fsync_directory(agent_dir)
    return True


def validate_contract_transition(
    lock_path: Path,
    *,
    new_digest: str,
    accepted_digest: str | None,
) -> None:
    if not lock_path.exists():
        return
    existing = read_lock(lock_path)
    old_digest = existing["contract"].get("tools_schema_sha256")
    if old_digest == new_digest:
        return
    if accepted_digest != new_digest:
        raise ValueError(
            "workbench input schemas changed; review the canonical tools/list "
            "contract and rerun with "
            f"--accept-contract-sha256 {new_digest} "
            f"(old={old_digest}, new={new_digest})"
        )


def offline_agent_preflight(
    agent_dir: Path,
    *,
    accepted_digest: str | None,
) -> bool:
    if not agent_dir.is_dir():
        raise FileNotFoundError(f"LingTai agent directory does not exist: {agent_dir}")
    recovered = recover_interrupted_update(agent_dir)
    installer.read_registry(agent_dir / "mcp_registry.jsonl")
    installer.read_init(agent_dir / "init.json")
    expected_digest = expected_contract_evidence()["tools_schema_sha256"]
    validate_contract_transition(
        agent_dir / LOCK_NAME,
        new_digest=expected_digest,
        accepted_digest=accepted_digest,
    )
    return recovered


def apply_agent_update(
    agent_dir: Path,
    config: installer.InstallConfig,
    lock_path: Path,
    lock_text: str,
) -> tuple[installer.InstallResult, bool]:
    paths = _transaction_files(agent_dir)
    originals = {
        name: installer.read_regular_text(path, missing_ok=True)
        for name, path in paths.items()
    }
    desired = {
        "mcp_registry.jsonl": installer.render_registry(agent_dir, config),
        "init.json": installer.render_init(agent_dir, config),
        LOCK_NAME: lock_text,
    }
    transaction_path = agent_dir / TRANSACTION_NAME
    transaction_text = (
        json.dumps(
            {
                "schema": TRANSACTION_SCHEMA,
                "original": originals,
                "desired": desired,
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )
    installer.write_text_if_changed(transaction_path, transaction_text)
    try:
        registry_changed = installer.write_text_if_changed(
            paths["mcp_registry.jsonl"], desired["mcp_registry.jsonl"]
        )
        init_changed = installer.write_text_if_changed(
            paths["init.json"], desired["init.json"]
        )
        lock_changed = installer.write_text_if_changed(lock_path, desired[LOCK_NAME])
        transaction_path.unlink()
        installer.fsync_directory(agent_dir)
    except Exception as update_error:
        rollback_errors = []
        for name, path in paths.items():
            try:
                _restore_text(path, originals[name])
            except Exception as rollback_error:  # pragma: no cover - disk failure
                rollback_errors.append(f"{path}: {rollback_error}")
        if rollback_errors:
            raise RuntimeError(
                f"agent update failed ({update_error}); rollback also failed: "
                + "; ".join(rollback_errors)
                + f"; recovery journal retained at {transaction_path}"
            ) from update_error
        try:
            transaction_path.unlink()
            installer.fsync_directory(agent_dir)
        except Exception as rollback_error:  # pragma: no cover - disk failure
            raise RuntimeError(
                f"agent update failed ({update_error}); rollback completed but "
                f"the recovery journal could not be removed: "
                f"{transaction_path}: {rollback_error}"
            ) from update_error
        raise
    result = installer.InstallResult(
        agent_dir=agent_dir,
        registry_changed=registry_changed,
        init_changed=init_changed,
    )
    return result, lock_changed


def check_lock(
    agent_dir: Path,
    *,
    candidate_binary: str | None,
    timeout_seconds: float,
) -> dict[str, Any]:
    lock_path = agent_dir / LOCK_NAME
    lock = read_lock(lock_path)
    config = config_from_lock(lock)
    command = Path(config.nokv_bin).expanduser().resolve()
    if not command.is_file():
        raise FileNotFoundError(f"locked NoKV binary does not exist: {command}")
    digest = sha256_file(command)
    if digest != lock["artifact"]["sha256"]:
        raise ValueError(
            "locked NoKV binary was replaced in place: "
            f"expected {lock['artifact']['sha256']}, got {digest}"
        )
    if command.stat().st_size != lock["artifact"].get("size_bytes"):
        raise ValueError("locked NoKV binary size does not match the lock")
    source = lock["source"]
    if (
        source.get("nokv_git_commit") not in command.parts
        or digest not in command.parts
    ):
        raise ValueError("locked NoKV command is not in its content-addressed path")
    staged_build_info = load_build_info(command.parent / "build-info.json")
    locked_identity = identity_from_mapping(source, context="workbench lock source")
    if (
        staged_build_info.identity != locked_identity
        or staged_build_info.binary_sha256 != digest
        or staged_build_info.binary_size_bytes != command.stat().st_size
    ):
        raise ValueError("staged build-info differs from the workbench lock")
    if candidate_binary:
        candidate = discover_nokv_binary(candidate_binary)
        candidate_digest = sha256_file(candidate)
        if candidate_digest != digest:
            raise ValueError(
                "candidate NoKV binary differs from the installed lock; run sync "
                "without --check after reviewing the update"
            )
    verify_agent_configuration(agent_dir, config)
    if json_sha256(installer.mcp_args(config)) != lock["launch"].get("args_sha256"):
        raise ValueError("locked MCP launch arguments have drifted")

    concrete_root = lock["launch"].get("workbench_root")
    if not isinstance(concrete_root, str) or not concrete_root:
        raise ValueError("workbench lock lacks a concrete preflight root")
    probe_config = installer.InstallConfig(
        **{
            **config.__dict__,
            "nokv_bin": str(command),
            "workbench_root": concrete_root,
        }
    )
    tools = raw_tools_list(probe_config, timeout_seconds=timeout_seconds)
    evidence = contract_evidence(tools)
    if evidence != lock["contract"]:
        raise ValueError("live NoKV MCP contract differs from the installed lock")
    return lock


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Stage an immutable NoKV binary, gate its live workbench MCP contract, "
            "and switch one LingTai agent."
        )
    )
    parser.add_argument("--project", default=".", help="LingTai project directory.")
    parser.add_argument("--agent", help="Agent directory name under PROJECT/.lingtai.")
    parser.add_argument("--agent-dir", help="Explicit LingTai agent directory.")
    parser.add_argument(
        "--nokv-bin", help="Candidate binary; defaults to NOKV_BIN, PATH, then Brew."
    )
    parser.add_argument(
        "--build-source",
        help=(
            "Build this NoKV checkout with cargo --locked --release and stage the "
            "result. Mutually exclusive with --nokv-bin/--build-info."
        ),
    )
    parser.add_argument(
        "--build-info", help="Build identity shipped with a Brew/Release candidate."
    )
    parser.add_argument("--revision", help="Expected full NoKV git commit.")
    parser.add_argument("--expected-sha256", help="Expected candidate binary SHA-256.")
    parser.add_argument(
        "--allow-dirty",
        action="store_true",
        help="Accept an explicitly dirty source identity for local testing only.",
    )
    parser.add_argument(
        "--distribution",
        choices=("source", "brew", "release", "path"),
        help="Artifact source recorded in the lock.",
    )
    parser.add_argument("--server-bind", default=installer.DEFAULT_SERVER_BIND)
    parser.add_argument("--object-backend", default="rustfs")
    parser.add_argument("--s3-endpoint", default=installer.DEFAULT_ENDPOINT)
    parser.add_argument("--s3-bucket", default=installer.DEFAULT_BUCKET)
    parser.add_argument("--workbench-root", default=installer.DEFAULT_WORKBENCH_ROOT)
    parser.add_argument("--mcp-name", default=installer.DEFAULT_MCP_NAME)
    parser.add_argument("--timeout-seconds", type=float, default=20.0)
    parser.add_argument(
        "--accept-contract-sha256",
        help="Accept exactly this reviewed canonical input-schema SHA-256.",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--stage-only",
        action="store_true",
        help="Only content-address and print the immutable binary path.",
    )
    mode.add_argument(
        "--probe-only",
        action="store_true",
        help=(
            "Stage the candidate and validate its live Workbench contract without "
            "changing Agent registration files."
        ),
    )
    mode.add_argument(
        "--check",
        action="store_true",
        help="Verify the existing lock, files, binary, and live contract without writing.",
    )
    mode.add_argument(
        "--preflight-only",
        action="store_true",
        help=(
            "Validate Agent files and a contract transition without staging or "
            "probing; recover an interrupted local sync transaction when present."
        ),
    )
    args = parser.parse_args(argv)
    if args.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be positive")
    if args.accept_contract_sha256:
        try:
            args.accept_contract_sha256 = validate_sha256(args.accept_contract_sha256)
        except ValueError as err:
            parser.error(str(err))
    if args.build_source and (args.nokv_bin or args.build_info):
        parser.error(
            "--build-source is mutually exclusive with --nokv-bin/--build-info"
        )
    if args.build_source and args.distribution not in (None, "source"):
        parser.error("--build-source requires --distribution source when specified")
    if args.preflight_only and any(
        (
            args.build_source,
            args.nokv_bin,
            args.build_info,
            args.revision,
            args.expected_sha256,
            args.distribution,
            args.allow_dirty,
        )
    ):
        parser.error("--preflight-only does not accept artifact build/staging options")
    if args.check and args.build_source:
        parser.error("--check validates the installed lock and cannot build source")
    if args.check and any(
        (
            args.build_info,
            args.revision,
            args.expected_sha256,
            args.distribution,
            args.allow_dirty,
            args.accept_contract_sha256,
        )
    ):
        parser.error(
            "--check accepts only project/Agent selection, --nokv-bin, and timeout"
        )
    return args


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    try:
        project = Path(args.project).expanduser().resolve()
        if args.preflight_only:
            agent_dir = (
                installer.resolve_agent_dir(project, args.agent, args.agent_dir)
                .expanduser()
                .resolve()
            )
            with agent_sync_lock(agent_dir, exclusive=True):
                recovered = offline_agent_preflight(
                    agent_dir,
                    accepted_digest=args.accept_contract_sha256,
                )
            print(f"agent_dir: {agent_dir}")
            print("agent_files_valid: true")
            print(f"interrupted_update_recovered: {str(recovered).lower()}")
            print(
                "expected_tools_schema_sha256: "
                f"{expected_contract_evidence()['tools_schema_sha256']}"
            )
            return 0

        if args.check:
            agent_dir = (
                installer.resolve_agent_dir(project, args.agent, args.agent_dir)
                .expanduser()
                .resolve()
            )
            with agent_sync_lock(agent_dir, exclusive=False):
                if read_transaction(agent_dir) is not None:
                    raise RuntimeError(
                        "an interrupted NoKV workbench update is pending; rerun the "
                        "normal sync to recover it before using --check"
                    )
                check_lock(
                    agent_dir,
                    candidate_binary=args.nokv_bin,
                    timeout_seconds=args.timeout_seconds,
                )
            print(f"agent_dir: {agent_dir}")
            print("lock_valid: true")
            print("live_contract_valid: true")
            return 0

        if args.build_source:
            candidate, identity = build_source_candidate(
                Path(args.build_source),
                revision=args.revision,
                allow_dirty=args.allow_dirty,
            )
            distribution = args.distribution or "source"
            artifact_sha256 = None
        else:
            candidate = discover_nokv_binary(args.nokv_bin)
            build_info = resolve_artifact_identity(
                candidate,
                build_info=args.build_info,
                revision=args.revision,
            )
            identity = build_info.identity
            artifact_sha256 = build_info.binary_sha256
            distribution = args.distribution or infer_distribution(candidate)
        if identity.source_dirty and not args.allow_dirty:
            raise ValueError(
                "NoKV source identity is dirty; commit/stash it or pass --allow-dirty "
                "for local testing"
            )
        if (
            artifact_sha256 is not None
            and args.expected_sha256 is not None
            and artifact_sha256 != validate_sha256(args.expected_sha256)
        ):
            raise ValueError(
                "artifact build-info SHA-256 differs from the independently "
                f"expected SHA-256: {artifact_sha256} != {args.expected_sha256}"
            )
        runtime = stage_runtime(
            project,
            candidate,
            identity,
            expected_sha256=artifact_sha256 or args.expected_sha256,
        )
        if args.stage_only:
            print(runtime.command)
            return 0

        if args.probe_only:
            agent_dir = (
                installer.resolve_agent_dir(project, args.agent, args.agent_dir)
                .expanduser()
                .resolve()
            )
            root = concrete_workbench_root(args.workbench_root, agent_dir)
            config = installer.InstallConfig(
                nokv_bin=str(runtime.command),
                server_bind=args.server_bind,
                object_backend=args.object_backend,
                s3_endpoint=args.s3_endpoint or None,
                s3_bucket=args.s3_bucket,
                workbench_root=root,
                mcp_name=args.mcp_name,
                source=registration_source(identity, runtime.sha256),
            )
            tools = raw_tools_list(config, timeout_seconds=args.timeout_seconds)
            evidence = contract_evidence(tools)
            validate_contract_transition(
                agent_dir / LOCK_NAME,
                new_digest=evidence["tools_schema_sha256"],
                accepted_digest=args.accept_contract_sha256,
            )
            print(f"agent_dir: {agent_dir}")
            print(f"binary_sha256: {runtime.sha256}")
            print(f"nokv_revision: {identity.nokv_git_commit}")
            print(f"tools_schema_sha256: {evidence['tools_schema_sha256']}")
            print("live_contract_valid: true")
            return 0

        agent_dir = (
            installer.resolve_agent_dir(project, args.agent, args.agent_dir)
            .expanduser()
            .resolve()
        )
        with agent_sync_lock(agent_dir, exclusive=True):
            recovered = recover_interrupted_update(agent_dir)
            root = concrete_workbench_root(args.workbench_root, agent_dir)
            source = registration_source(identity, runtime.sha256)
            config = installer.InstallConfig(
                nokv_bin=str(runtime.command),
                server_bind=args.server_bind,
                object_backend=args.object_backend,
                s3_endpoint=args.s3_endpoint or None,
                s3_bucket=args.s3_bucket,
                workbench_root=args.workbench_root,
                mcp_name=args.mcp_name,
                source=source,
            )
            probe_config = installer.InstallConfig(
                **{**config.__dict__, "workbench_root": root}
            )
            tools = raw_tools_list(probe_config, timeout_seconds=args.timeout_seconds)
            desired_lock = build_lock(
                config,
                concrete_root=root,
                distribution=distribution,
                identity=identity,
                binary_sha256=runtime.sha256,
                binary_size=runtime.size_bytes,
                tools=tools,
            )
            lock_path = agent_dir / LOCK_NAME
            validate_contract_transition(
                lock_path,
                new_digest=desired_lock["contract"]["tools_schema_sha256"],
                accepted_digest=args.accept_contract_sha256,
            )

            # Parse both files before the transaction marker and first mutation.
            installer.read_registry(agent_dir / "mcp_registry.jsonl")
            installer.read_init(agent_dir / "init.json")
            lock_text = (
                json.dumps(
                    desired_lock,
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                )
                + "\n"
            )
            result, lock_changed = apply_agent_update(
                agent_dir, config, lock_path, lock_text
            )
    except Exception as err:
        print(f"error: {err}", file=sys.stderr)
        return 1

    print(f"agent_dir: {result.agent_dir}")
    print(f"binary_sha256: {runtime.sha256}")
    print(f"nokv_revision: {identity.nokv_git_commit}")
    print(f"holt_revision: {identity.holt_git_commit}")
    print(f"tools_schema_sha256: {desired_lock['contract']['tools_schema_sha256']}")
    print(f"registry_changed: {str(result.registry_changed).lower()}")
    print(f"init_changed: {str(result.init_changed).lower()}")
    print(f"lock_changed: {str(lock_changed).lower()}")
    print(f"interrupted_update_recovered: {str(recovered).lower()}")
    if result.registry_changed or result.init_changed or lock_changed:
        print("next: run /refresh in the target LingTai agent")
    else:
        print("already synchronized")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
