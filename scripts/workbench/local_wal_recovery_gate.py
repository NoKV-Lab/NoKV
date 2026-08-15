#!/usr/bin/env python3
"""Real-etcd kill/retry qualification for one local-WAL metadata authority.

The gate creates durable metadata under owner epoch one, then uses the
bench-owned fault driver to stop at both non-terminal epoch-two boundaries:
before and after the local Holt owner fence is advanced. The driver is killed,
its lease-backed etcd session is allowed to expire, and the real ``nokv`` CLI
must reopen the same metadata path without consuming epoch three.

Artifact storage is deliberately not exercised here; the object arguments are
an unreachable, unsigned S3 configuration used only to compose the CLI. The
separate live Workbench gate owns RustFS/S3 qualification.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import platform
import shutil
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Sequence, TextIO


SCHEMA = "nokv.local_wal_recovery_gate.v1"
CRASH_STAGES = ("before-local-fence", "after-local-fence")
# Reopening admits a successor only because the local authority directory is
# itself the mutex. The crash stages prove sequential handover; this one proves
# the exclusion that replaced the blanket successor refusal, and proves it
# across real processes rather than two calls inside one.
CONCURRENT_STAGE = "concurrent-takeover"


class NotQualified(RuntimeError):
    pass


class WorkflowFailure(RuntimeError):
    pass


def now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def digest_file(path: Path) -> str:
    state = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            state.update(block)
    return state.hexdigest()


def fixed_id(seed: str, label: str) -> str:
    return hashlib.sha256(f"{seed}:{label}".encode()).hexdigest()[:32]


def validate_stage_evidence(evidence: dict[str, object]) -> None:
    stage = evidence.get("stage")
    if stage not in CRASH_STAGES:
        raise WorkflowFailure(f"unknown crash stage {stage!r}")

    previous = evidence.get("previous_owner_epoch")
    recovery = evidence.get("recovery_owner_epoch")
    final = evidence.get("final_owner_epoch")
    if previous != 1 or recovery != 2:
        raise WorkflowFailure(
            f"{stage} expected owner epochs 1 -> Recovering(2), got {previous!r} -> {recovery!r}"
        )
    if final != recovery:
        raise WorkflowFailure(
            f"{stage} retry advanced from recovery epoch {recovery} to {final}"
        )

    expected_local = 1 if stage == "before-local-fence" else 2
    if evidence.get("local_epoch_at_crash") != expected_local:
        raise WorkflowFailure(
            f"{stage} must stop at local epoch {expected_local}, got "
            f"{evidence.get('local_epoch_at_crash')!r}"
        )
    if evidence.get("fault_exit_code") != -signal.SIGKILL:
        raise WorkflowFailure(
            f"{stage} boundary process was not terminated by SIGKILL: "
            f"{evidence.get('fault_exit_code')!r}"
        )
    if evidence.get("session_absent_before_retry") is not True:
        raise WorkflowFailure(f"{stage} killed owner session remained live before retry")
    if evidence.get("final_state") != "Serving":
        raise WorkflowFailure(
            f"{stage} retry ended in {evidence.get('final_state')!r}, expected Serving"
        )
    probe = evidence.get("metadata_probe")
    if not isinstance(probe, dict) or probe.get("status") != "success":
        raise WorkflowFailure(f"{stage} terminal metadata probe did not succeed")


def validate_concurrent_evidence(evidence: dict[str, object]) -> None:
    if evidence.get("stage") != CONCURRENT_STAGE:
        raise WorkflowFailure(f"unknown concurrent stage {evidence.get('stage')!r}")

    if evidence.get("loser_exit_code") in (None, 0):
        raise WorkflowFailure(
            f"{CONCURRENT_STAGE} second owner did not fail: "
            f"{evidence.get('loser_exit_code')!r}"
        )
    if not str(evidence.get("loser_stderr") or "").strip():
        raise WorkflowFailure(f"{CONCURRENT_STAGE} second owner failed without a diagnostic")
    # The whole point of the stage: the refusal must land before the control
    # plane is touched, so the record is byte-identical across the attempt and
    # no owner epoch was spent on a takeover that never happened.
    before = evidence.get("control_record_before")
    after = evidence.get("control_record_after")
    if not isinstance(before, dict) or before != after:
        raise WorkflowFailure(
            f"{CONCURRENT_STAGE} mutated the control record: {before!r} -> {after!r}"
        )
    if before.get("owner_epoch") != 1 or before.get("state") != 3:
        raise WorkflowFailure(
            f"{CONCURRENT_STAGE} incumbent was not Serving(1): {before!r}"
        )
    if evidence.get("incumbent_alive_after") is not True:
        raise WorkflowFailure(f"{CONCURRENT_STAGE} incumbent did not survive the takeover")
    probe = evidence.get("metadata_probe")
    if not isinstance(probe, dict) or probe.get("status") != "success":
        raise WorkflowFailure(
            f"{CONCURRENT_STAGE} incumbent stopped serving after the refused takeover"
        )


def run(
    argv: Sequence[os.PathLike[str] | str],
    *,
    cwd: Path,
    timeout: float,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        [str(item) for item in argv],
        cwd=cwd,
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )
    if check and result.returncode != 0:
        rendered = " ".join(str(item) for item in argv)
        output = (result.stderr or result.stdout).strip()
        raise WorkflowFailure(f"command failed ({result.returncode}): {rendered}\n{output}")
    return result


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def stop(process: subprocess.Popen[bytes] | None, sig: signal.Signals = signal.SIGKILL) -> None:
    if process is None or process.poll() is not None:
        return
    os.kill(process.pid, sig)
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=10)


def wait_tcp(process: subprocess.Popen[bytes], port: int, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise WorkflowFailure(f"server exited before readiness with {process.returncode}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise WorkflowFailure(f"server did not listen on 127.0.0.1:{port}")


def wait_exit(process: subprocess.Popen[bytes], timeout: float) -> int:
    try:
        return int(process.wait(timeout=timeout))
    except subprocess.TimeoutExpired as error:
        raise WorkflowFailure(
            "second owner kept running against a directory another owner holds"
        ) from error


def wait_json(path: Path, process: subprocess.Popen[bytes], timeout: float) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise WorkflowFailure(
                f"fault driver exited before boundary with {process.returncode}"
            )
        if path.exists():
            try:
                value = json.loads(path.read_text())
                if isinstance(value, dict):
                    return value
                last_error = WorkflowFailure("ready record is not an object")
            except (OSError, json.JSONDecodeError) as error:
                last_error = error
        time.sleep(0.05)
    detail = f": {last_error}" if last_error is not None else ""
    raise WorkflowFailure(f"fault driver did not publish {path}{detail}")


def etcd_value(
    etcdctl: Path,
    endpoint: str,
    key: str,
    *,
    cwd: Path,
    timeout: float,
) -> str:
    return run(
        [etcdctl, f"--endpoints={endpoint}", "get", key, "--print-value-only"],
        cwd=cwd,
        timeout=timeout,
    ).stdout.strip()


def wait_session_absent(
    etcdctl: Path,
    endpoint: str,
    key: str,
    *,
    cwd: Path,
    timeout: float,
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not etcd_value(etcdctl, endpoint, key, cwd=cwd, timeout=timeout):
            return
        time.sleep(0.1)
    raise WorkflowFailure(f"owner session key remained live after SIGKILL: {key}")


def wait_etcd(
    etcdctl: Path,
    endpoint: str,
    process: subprocess.Popen[bytes],
    *,
    cwd: Path,
    timeout: float,
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise WorkflowFailure(f"etcd exited before readiness with {process.returncode}")
        result = run(
            [etcdctl, f"--endpoints={endpoint}", "endpoint", "health"],
            cwd=cwd,
            timeout=timeout,
            check=False,
        )
        if result.returncode == 0:
            return
        time.sleep(0.1)
    raise WorkflowFailure(f"etcd did not become healthy at {endpoint}")


def start_process(argv: Sequence[os.PathLike[str] | str], cwd: Path, log: TextIO) -> subprocess.Popen[bytes]:
    return subprocess.Popen(
        [str(item) for item in argv],
        cwd=cwd,
        stdin=subprocess.DEVNULL,
        stdout=log,
        stderr=subprocess.STDOUT,
    )


def control_args(binary: Path, root_id: str, endpoint: str, prefix: str) -> list[str]:
    return [
        str(binary),
        "--root-id",
        root_id,
        "--etcd-endpoint",
        endpoint,
        "--etcd-key-prefix",
        prefix,
        "--etcd-lease-ttl-seconds",
        "2",
    ]


def object_args(stage: str) -> list[str]:
    # These arguments compose the product CLI but no gate operation performs
    # object I/O. RustFS/S3 behavior belongs to the separate live Workbench gate.
    return [
        "--object-bucket",
        "nokv-local-wal-recovery-gate",
        "--object-endpoint",
        "http://127.0.0.1:1",
        "--object-root",
        f"local-wal-recovery/{stage}",
        "--object-skip-signature",
    ]


def decode_control_record(
    etcdctl: Path,
    endpoint: str,
    key: str,
    *,
    cwd: Path,
    timeout: float,
) -> dict[str, Any]:
    raw = etcd_value(etcdctl, endpoint, key, cwd=cwd, timeout=timeout)
    if not raw:
        raise WorkflowFailure(f"logical-shard control record is absent: {key}")
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as error:
        raise WorkflowFailure(f"decode logical-shard control record: {error}") from error
    if not isinstance(value, dict):
        raise WorkflowFailure("logical-shard control record is not an object")
    return value


def run_stage(
    stage: str,
    *,
    repo: Path,
    binary: Path,
    driver: Path,
    etcdctl: Path,
    etcd_endpoint: str,
    evidence: Path,
    seed: str,
    timeout: float,
) -> dict[str, object]:
    stage_dir = evidence / stage
    stage_dir.mkdir(parents=True)
    metadata = stage_dir / "metadata"
    ready_path = stage_dir / "fault-ready.json"
    config_path = stage_dir / "fault-config.json"
    root_id = fixed_id(seed, f"{stage}:root")
    shard_id = fixed_id(seed, f"{stage}:shard")
    prefix = f"/nokv/local-wal-recovery/{fixed_id(seed, stage)}"
    record_key = f"{prefix}/logical-shards/{shard_id}"
    session_key = f"{prefix}/sessions/{shard_id}"
    common = control_args(binary, root_id, etcd_endpoint, prefix)
    objects = object_args(stage)
    processes: list[subprocess.Popen[bytes]] = []
    logs: list[TextIO] = []

    try:
        provision_command = [*common, "provision", shard_id]
        provision = run(provision_command, cwd=repo, timeout=timeout)
        (stage_dir / "provision.stdout.log").write_text(provision.stdout)
        (stage_dir / "provision.stderr.log").write_text(provision.stderr)

        first_port = free_port()
        first_log = (stage_dir / "owner-e1.log").open("w")
        logs.append(first_log)
        first_command = [
            *common,
            *objects,
            "--bind",
            f"127.0.0.1:{first_port}",
            "--advertise-endpoint",
            f"127.0.0.1:{first_port}",
            "--node-id",
            f"gate-{stage}-e1",
            "--metadata-create",
            metadata,
            "serve",
        ]
        first = start_process(first_command, repo, first_log)
        processes.append(first)
        wait_tcp(first, first_port, timeout)

        client = [*common, "--workbench-root", "/agents/issue450/wb", *objects]
        create_command = [
            *client,
            "workbench",
            "workbench_create",
            canonical_json({"id": "restart-proof"}),
        ]
        create = run(create_command, cwd=repo, timeout=timeout)
        create_result = json.loads(create.stdout)
        initial = decode_control_record(
            etcdctl, etcd_endpoint, record_key, cwd=repo, timeout=timeout
        )
        if initial.get("owner_epoch") != 1 or initial.get("state") != 3:
            raise WorkflowFailure(f"initial owner did not reach Serving(1): {initial}")

        stop(first)
        first_exit_code = first.returncode
        wait_session_absent(
            etcdctl, etcd_endpoint, session_key, cwd=repo, timeout=timeout
        )

        config = {
            "etcd_endpoints": [etcd_endpoint],
            "etcd_key_prefix": prefix,
            "lease_ttl_seconds": 2,
            "logical_shard_id": shard_id,
            "metadata_path": str(metadata),
            "previous_owner_epoch": 1,
            "node_id": f"gate-{stage}-fault",
            "endpoint": "127.0.0.1:1",
            "stage": stage,
            "ready_path": str(ready_path),
        }
        config_path.write_text(json.dumps(config, indent=2, sort_keys=True) + "\n")
        fault_log = (stage_dir / "fault-driver.log").open("w")
        logs.append(fault_log)
        fault_command = [str(driver), str(config_path)]
        fault = start_process(fault_command, repo, fault_log)
        processes.append(fault)
        boundary = wait_json(ready_path, fault, timeout)
        recovering = decode_control_record(
            etcdctl, etcd_endpoint, record_key, cwd=repo, timeout=timeout
        )
        if recovering.get("owner_epoch") != 2 or recovering.get("state") != 2:
            raise WorkflowFailure(f"fault driver did not retain Recovering(2): {recovering}")
        if not etcd_value(etcdctl, etcd_endpoint, session_key, cwd=repo, timeout=timeout):
            raise WorkflowFailure("fault driver published its boundary without a live session")

        stop(fault)
        fault_exit_code = fault.returncode
        wait_session_absent(
            etcdctl, etcd_endpoint, session_key, cwd=repo, timeout=timeout
        )
        session_absent = True

        retry_port = free_port()
        retry_log = (stage_dir / "owner-e2-retry.log").open("w")
        logs.append(retry_log)
        retry_command = [
            *common,
            *objects,
            "--bind",
            f"127.0.0.1:{retry_port}",
            "--advertise-endpoint",
            f"127.0.0.1:{retry_port}",
            "--node-id",
            f"gate-{stage}-retry",
            "--metadata-reopen",
            metadata,
            "serve",
        ]
        retry = start_process(retry_command, repo, retry_log)
        processes.append(retry)
        wait_tcp(retry, retry_port, timeout)
        catalog_command = [
            *client,
            "workbench",
            "workbench_catalog",
            canonical_json({"id": "restart-proof", "include_facets": True}),
        ]
        catalog = run(catalog_command, cwd=repo, timeout=timeout)
        metadata_probe = json.loads(catalog.stdout)
        final = decode_control_record(
            etcdctl, etcd_endpoint, record_key, cwd=repo, timeout=timeout
        )
        stop(retry)
        retry_exit_code = retry.returncode
        state_name = "Serving" if final.get("state") == 3 else f"state-{final.get('state')}"
        result: dict[str, object] = {
            "stage": stage,
            "previous_owner_epoch": 1,
            "recovery_owner_epoch": boundary.get("recovery_owner_epoch"),
            "local_epoch_at_crash": boundary.get("local_epoch_at_crash"),
            "fault_exit_code": fault_exit_code,
            "session_absent_before_retry": session_absent,
            "final_owner_epoch": final.get("owner_epoch"),
            "final_state": state_name,
            "metadata_probe": metadata_probe,
            "initial_control_record": initial,
            "recovering_control_record": recovering,
            "final_control_record": final,
            "create_result": create_result,
            "etcd_prefix": prefix,
            "logical_shard_id": shard_id,
            "metadata_path": str(metadata),
            "fault_driver_boundary": boundary,
            "process_exit_codes": {
                "owner_e1": first_exit_code,
                "fault_driver": fault_exit_code,
                "owner_e2_retry": retry_exit_code,
            },
            "commands": {
                "provision": [str(item) for item in provision_command],
                "owner_e1": [str(item) for item in first_command],
                "create_metadata_probe": [str(item) for item in create_command],
                "fault_driver": fault_command,
                "owner_e2_retry": [str(item) for item in retry_command],
                "terminal_metadata_probe": [str(item) for item in catalog_command],
            },
        }
        validate_stage_evidence(result)
        return result
    finally:
        for process in reversed(processes):
            stop(process)
        for log in logs:
            log.close()


def run_concurrent_stage(
    *,
    repo: Path,
    binary: Path,
    etcdctl: Path,
    etcd_endpoint: str,
    evidence: Path,
    seed: str,
    timeout: float,
) -> dict[str, object]:
    stage = CONCURRENT_STAGE
    stage_dir = evidence / stage
    stage_dir.mkdir(parents=True)
    metadata = stage_dir / "metadata"
    root_id = fixed_id(seed, f"{stage}:root")
    shard_id = fixed_id(seed, f"{stage}:shard")
    prefix = f"/nokv/local-wal-recovery/{fixed_id(seed, stage)}"
    record_key = f"{prefix}/logical-shards/{shard_id}"
    common = control_args(binary, root_id, etcd_endpoint, prefix)
    objects = object_args(stage)
    processes: list[subprocess.Popen[bytes]] = []
    logs: list[TextIO] = []

    try:
        provision_command = [*common, "provision", shard_id]
        provision = run(provision_command, cwd=repo, timeout=timeout)
        (stage_dir / "provision.stdout.log").write_text(provision.stdout)
        (stage_dir / "provision.stderr.log").write_text(provision.stderr)

        incumbent_port = free_port()
        incumbent_log = (stage_dir / "owner-incumbent.log").open("w")
        logs.append(incumbent_log)
        incumbent_command = [
            *common,
            *objects,
            "--bind",
            f"127.0.0.1:{incumbent_port}",
            "--advertise-endpoint",
            f"127.0.0.1:{incumbent_port}",
            "--node-id",
            f"gate-{stage}-incumbent",
            "--metadata-create",
            metadata,
            "serve",
        ]
        incumbent = start_process(incumbent_command, repo, incumbent_log)
        processes.append(incumbent)
        wait_tcp(incumbent, incumbent_port, timeout)

        client = [*common, "--workbench-root", "/agents/issue450/wb", *objects]
        create_command = [
            *client,
            "workbench",
            "workbench_create",
            canonical_json({"id": "exclusion-proof"}),
        ]
        create = run(create_command, cwd=repo, timeout=timeout)
        create_result = json.loads(create.stdout)
        before = decode_control_record(
            etcdctl, etcd_endpoint, record_key, cwd=repo, timeout=timeout
        )
        if before.get("owner_epoch") != 1 or before.get("state") != 3:
            raise WorkflowFailure(f"incumbent did not reach Serving(1): {before}")

        # The incumbent keeps serving throughout. A second process now asks to
        # reopen the very directory the incumbent holds open.
        challenger_port = free_port()
        challenger_command = [
            *common,
            *objects,
            "--bind",
            f"127.0.0.1:{challenger_port}",
            "--advertise-endpoint",
            f"127.0.0.1:{challenger_port}",
            "--node-id",
            f"gate-{stage}-challenger",
            "--metadata-reopen",
            metadata,
            "serve",
        ]
        challenger = subprocess.Popen(
            [str(item) for item in challenger_command],
            cwd=repo,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        processes.append(challenger)
        challenger_exit = wait_exit(challenger, timeout)
        challenger_stdout, challenger_stderr = challenger.communicate()
        (stage_dir / "owner-challenger.stdout.log").write_text(challenger_stdout or "")
        (stage_dir / "owner-challenger.stderr.log").write_text(challenger_stderr or "")

        after = decode_control_record(
            etcdctl, etcd_endpoint, record_key, cwd=repo, timeout=timeout
        )
        incumbent_alive = incumbent.poll() is None
        catalog_command = [
            *client,
            "workbench",
            "workbench_catalog",
            canonical_json({"id": "exclusion-proof", "include_facets": True}),
        ]
        catalog = run(catalog_command, cwd=repo, timeout=timeout)
        metadata_probe = json.loads(catalog.stdout)
        stop(incumbent, signal.SIGTERM)

        result: dict[str, object] = {
            "stage": stage,
            "loser_exit_code": challenger_exit,
            "loser_stderr": (challenger_stderr or "").strip(),
            "incumbent_alive_after": incumbent_alive,
            "control_record_before": before,
            "control_record_after": after,
            "metadata_probe": metadata_probe,
            "create_result": create_result,
            "etcd_prefix": prefix,
            "commands": {
                "provision": [str(item) for item in provision_command],
                "owner_incumbent": [str(item) for item in incumbent_command],
                "create_metadata_probe": [str(item) for item in create_command],
                "owner_challenger": [str(item) for item in challenger_command],
                "terminal_metadata_probe": [str(item) for item in catalog_command],
            },
        }
        validate_concurrent_evidence(result)
        return result
    finally:
        for process in reversed(processes):
            stop(process)
        for log in logs:
            log.close()


def build_binaries(repo: Path, target: Path, timeout: float) -> tuple[Path, Path]:
    environment = os.environ.copy()
    environment["CARGO_TARGET_DIR"] = str(target)
    for command in (
        ["cargo", "build", "-p", "nokv", "--bin", "nokv"],
        [
            "cargo",
            "build",
            "-p",
            "nokv-bench",
            "--bin",
            "nokv-local-wal-recovery-fault",
        ],
    ):
        result = subprocess.run(
            command,
            cwd=repo,
            text=True,
            capture_output=True,
            timeout=timeout,
            check=False,
            env=environment,
        )
        if result.returncode != 0:
            raise WorkflowFailure(
                f"command failed ({result.returncode}): {' '.join(command)}\n"
                f"{result.stderr or result.stdout}"
            )
    return target / "debug" / "nokv", target / "debug" / "nokv-local-wal-recovery-fault"


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    repo_default = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=repo_default)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--target-dir", type=Path)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--fault-driver", type=Path)
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--etcd-bin", type=Path, default=shutil.which("etcd"))
    parser.add_argument("--etcdctl-bin", type=Path, default=shutil.which("etcdctl"))
    parser.add_argument("--seed", default="issue450-e2-kill-retry-v1")
    parser.add_argument("--timeout", type=float, default=30.0)
    return parser.parse_args(argv)


def execute(args: argparse.Namespace) -> dict[str, object]:
    repo = args.repo.resolve()
    evidence = args.evidence_dir.resolve()
    if evidence.exists() and any(evidence.iterdir()):
        raise WorkflowFailure(f"evidence directory is not empty: {evidence}")
    evidence.mkdir(parents=True, exist_ok=True)
    if args.etcd_bin is None or not Path(args.etcd_bin).is_file():
        raise NotQualified("a real etcd binary is required")
    if args.etcdctl_bin is None or not Path(args.etcdctl_bin).is_file():
        raise NotQualified("etcdctl is required")
    if args.timeout <= 0:
        raise WorkflowFailure("--timeout must be positive")

    target = (args.target_dir or repo / "target").resolve()
    binary = args.binary.resolve() if args.binary else target / "debug" / "nokv"
    driver = (
        args.fault_driver.resolve()
        if args.fault_driver
        else target / "debug" / "nokv-local-wal-recovery-fault"
    )
    if args.build:
        binary, driver = build_binaries(repo, target, max(args.timeout, 600.0))
    if not binary.is_file() or not driver.is_file():
        raise NotQualified("built nokv and recovery fault-driver binaries are required")

    client_port = free_port()
    peer_port = free_port()
    endpoint = f"http://127.0.0.1:{client_port}"
    peer = f"http://127.0.0.1:{peer_port}"
    etcd_data = evidence / "etcd-data"
    etcd_log = (evidence / "etcd.log").open("w")
    etcd: subprocess.Popen[bytes] | None = None
    started_at = now()
    stages: list[dict[str, object]] = []
    try:
        etcd = start_process(
            [
                args.etcd_bin,
                "--name",
                "nokv-local-wal-recovery",
                "--data-dir",
                etcd_data,
                "--listen-client-urls",
                endpoint,
                "--advertise-client-urls",
                endpoint,
                "--listen-peer-urls",
                peer,
                "--initial-advertise-peer-urls",
                peer,
                "--initial-cluster",
                f"nokv-local-wal-recovery={peer}",
                "--initial-cluster-state",
                "new",
                "--log-level",
                "warn",
            ],
            repo,
            etcd_log,
        )
        wait_etcd(
            Path(args.etcdctl_bin), endpoint, etcd, cwd=repo, timeout=args.timeout
        )
        for stage in CRASH_STAGES:
            stages.append(
                run_stage(
                    stage,
                    repo=repo,
                    binary=binary,
                    driver=driver,
                    etcdctl=Path(args.etcdctl_bin),
                    etcd_endpoint=endpoint,
                    evidence=evidence,
                    seed=args.seed,
                    timeout=args.timeout,
                )
            )
        stages.append(
            run_concurrent_stage(
                repo=repo,
                binary=binary,
                etcdctl=Path(args.etcdctl_bin),
                etcd_endpoint=endpoint,
                evidence=evidence,
                seed=args.seed,
                timeout=args.timeout,
            )
        )
    finally:
        stop(etcd, signal.SIGTERM)
        etcd_log.close()

    git_head = run(["git", "rev-parse", "HEAD"], cwd=repo, timeout=args.timeout).stdout.strip()
    git_status = run(
        ["git", "status", "--porcelain=v1"], cwd=repo, timeout=args.timeout
    ).stdout.splitlines()
    etcd_version = run(
        [args.etcd_bin, "--version"], cwd=repo, timeout=args.timeout
    ).stdout.splitlines()
    return {
        "schema": SCHEMA,
        "status": "PASS",
        "started_at": started_at,
        "finished_at": now(),
        "source": {"commit": git_head, "dirty": git_status},
        "environment": {
            "platform": platform.platform(),
            "python": platform.python_version(),
            "etcd": etcd_version,
            "object_profile": "not exercised; unreachable unsigned S3 composition only",
        },
        "binaries": {
            "nokv": {"path": str(binary), "sha256": digest_file(binary)},
            "fault_driver": {"path": str(driver), "sha256": digest_file(driver)},
        },
        "invariants": {
            "recovery_epoch": 2,
            "retry_must_not_allocate_epoch": 3,
            "session_must_expire_before_retry": True,
            "concurrent_takeover_must_not_touch_control": True,
            "stages": [*CRASH_STAGES, CONCURRENT_STAGE],
        },
        "stages": stages,
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    evidence = args.evidence_dir.resolve()
    try:
        result = execute(args)
        evidence.mkdir(parents=True, exist_ok=True)
        (evidence / "qualification.json").write_text(
            json.dumps(result, indent=2, sort_keys=True) + "\n"
        )
        print(evidence / "qualification.json")
        return 0
    except NotQualified as error:
        status = "NOT QUALIFIED"
        code = 3
        failure = str(error)
    except Exception as error:  # Preserve a reviewable terminal gate result.
        status = "FAIL"
        code = 2
        failure = str(error)
    evidence.mkdir(parents=True, exist_ok=True)
    (evidence / "qualification.json").write_text(
        json.dumps(
            {
                "schema": SCHEMA,
                "status": status,
                "finished_at": now(),
                "error": failure,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )
    print(f"{status}: {failure}", file=sys.stderr)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
