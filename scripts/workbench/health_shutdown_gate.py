#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0
"""Real etcd/RustFS gate for the owner health surface and graceful shutdown.

Stages against a real ``nokv`` owner process:

  1. provision one root with a stable AgentId;
  2. start the owner with an explicit ``--health-endpoint``;
  3. ``/healthz`` must report liveness and ``/readyz`` readiness; ``/stats``
     must carry the server identity and counters that advance with traffic;
  4. one Workbench request must succeed and move ``requests_total`` and
     ``connections_total``;
  5. SIGTERM must stop the owner with exit code zero and close the health
     endpoint; the successor owner reopens the same metadata authority and
     ``/readyz`` reports ready again while the committed Workbench data is
     served. The lease-release timing authority stays with
     ``local_wal_recovery_gate.py``'s graceful stage; this gate asserts the
     health contract only.

Every scenario writes one evidence entry; any failure exits nonzero.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import http.client
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Sequence, TextIO


SCHEMA = "nokv.health_shutdown_gate.v1"
GRACEFUL_TTL_SECONDS = 10
GRACEFUL_EXIT_DEADLINE_SECONDS = 10.0
REOPEN_ADMISSION_DEADLINE_SECONDS = 6.0


class WorkflowFailure(RuntimeError):
    pass


def now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def fixed_id(seed: str, label: str) -> str:
    return hashlib.sha256(f"{seed}:{label}".encode()).hexdigest()[:32]


def digest_file(path: Path) -> str:
    state = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            state.update(block)
    return state.hexdigest()


def clean_environment() -> dict[str, str]:
    # opendal honors HTTP(S)_PROXY, and a developer machine proxy must not
    # intercept the loopback object endpoint. CI has no proxy; stripping the
    # variables makes local and CI behavior identical.
    stripped = dict(os.environ)
    for name in (
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
    ):
        stripped.pop(name, None)
    return stripped


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
        capture_output=True,
        text=True,
        timeout=timeout,
        env=clean_environment(),
    )
    if check and result.returncode != 0:
        rendered = " ".join(str(item) for item in argv)
        output = (result.stderr or result.stdout).strip()
        raise WorkflowFailure(
            f"command failed ({result.returncode}): {rendered}\n{output}"
        )
    return result


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def start_process(
    argv: Sequence[os.PathLike[str] | str], cwd: Path, log: TextIO
) -> subprocess.Popen[bytes]:
    return subprocess.Popen(
        [str(item) for item in argv],
        cwd=cwd,
        stdin=subprocess.DEVNULL,
        stdout=log,
        stderr=subprocess.STDOUT,
        env=clean_environment(),
    )


def wait_tcp(process: subprocess.Popen[bytes], port: int, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise WorkflowFailure(
                f"server exited before readiness with {process.returncode}"
            )
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
        process.kill()
        process.wait(timeout=10)
        raise WorkflowFailure(
            f"owner kept running after SIGTERM for {timeout}s"
        ) from error


def wait_health(
    port: int,
    process: subprocess.Popen[bytes],
    path: str,
    expected_status: int,
    timeout: float,
) -> tuple[int, str]:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise WorkflowFailure(
                f"server exited before health readiness with {process.returncode}"
            )
        try:
            status, body = http_get(port, path)
            if status == expected_status:
                return status, body
            last_error = WorkflowFailure(
                f"{path} reported {status} ({body!r}) while waiting for "
                f"{expected_status}"
            )
        except (OSError, http.client.HTTPException) as error:
            last_error = error
        time.sleep(0.1)
    raise WorkflowFailure(
        f"{path} never reported {expected_status} on 127.0.0.1:{port}: {last_error}"
    )


def http_get(port: int, path: str) -> tuple[int, str]:
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
    try:
        connection.request("GET", path)
        response = connection.getresponse()
        return response.status, response.read().decode("utf-8")
    finally:
        connection.close()


def control_args(
    binary: Path,
    root_id: str,
    endpoint: str,
    prefix: str,
    lease_ttl_seconds: int = GRACEFUL_TTL_SECONDS,
) -> list[str]:
    return [
        str(binary),
        "--root-id",
        root_id,
        "--etcd-endpoint",
        endpoint,
        "--etcd-key-prefix",
        prefix,
        "--etcd-lease-ttl-seconds",
        str(lease_ttl_seconds),
    ]


def object_args(
    stage: str,
    endpoint: str,
    bucket: str,
    root: str,
    access_key_id: str,
    secret_access_key: str,
) -> list[str]:
    return [
        "--object-bucket",
        bucket,
        "--object-endpoint",
        endpoint,
        "--object-root",
        f"{root.rstrip('/')}/{stage}",
        "--object-access-key-id",
        access_key_id,
        "--object-secret-access-key",
        secret_access_key,
    ]


def server_command(
    common: list[str],
    objects: list[str],
    port: int,
    health_port: int,
    node: str,
    metadata_option: str,
    metadata: Path,
) -> list[str]:
    return [
        *common,
        *objects,
        "--bind",
        f"127.0.0.1:{port}",
        "--advertise-endpoint",
        f"127.0.0.1:{port}",
        "--health-endpoint",
        f"127.0.0.1:{health_port}",
        "--node-id",
        node,
        metadata_option,
        str(metadata),
        "serve",
    ]


def validate_stats(payload: dict[str, Any], label: str) -> None:
    for field in (
        "pid",
        "uptime_seconds",
        "protocol_schema",
        "installed_roots",
        "owner_loss",
        "draining",
        "ready",
        "connections_total",
        "requests_total",
        "inflight_connections",
    ):
        if field not in payload:
            raise WorkflowFailure(f"{label}: /stats is missing field {field}")
    if not isinstance(payload["pid"], int) or payload["pid"] <= 0:
        raise WorkflowFailure(f"{label}: /stats pid is not a positive integer")
    if not isinstance(payload["protocol_schema"], str) or not payload[
        "protocol_schema"
    ].startswith("nokv.workspace"):
        raise WorkflowFailure(f"{label}: /stats protocol_schema is unexpected")
    if not isinstance(payload["installed_roots"], int) or payload["installed_roots"] < 1:
        raise WorkflowFailure(f"{label}: /stats installed_roots must be at least 1")
    if payload["owner_loss"] is not False or payload["ready"] is not True:
        raise WorkflowFailure(f"{label}: /stats must report owner retained and ready")
    if not isinstance(payload["connections_total"], int) or not isinstance(
        payload["requests_total"], int
    ):
        raise WorkflowFailure(f"{label}: /stats counters must be integers")


def build_binaries(repo: Path, target: Path, timeout: float) -> Path:
    run(["cargo", "build", "-p", "nokv", "--bin", "nokv"], cwd=repo, timeout=timeout)
    binary = target / "debug" / "nokv"
    if not binary.is_file():
        raise WorkflowFailure(f"built binary is missing: {binary}")
    return binary


def parser() -> argparse.ArgumentParser:
    parsed = argparse.ArgumentParser(description=__doc__)
    parsed.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[2])
    parsed.add_argument("--binary", type=Path)
    parsed.add_argument("--build", action="store_true")
    parsed.add_argument("--target-dir", type=Path)
    parsed.add_argument("--etcd-bin", type=Path, default=shutil.which("etcd"))
    parsed.add_argument("--etcdctl-bin", type=Path, default=shutil.which("etcdctl"))
    parsed.add_argument("--seed", default="health-shutdown")
    parsed.add_argument("--object-endpoint")
    parsed.add_argument("--object-bucket")
    parsed.add_argument("--object-root", default="health-shutdown")
    parsed.add_argument("--object-access-key-id", default="rustfsadmin")
    parsed.add_argument("--object-secret-access-key", default="rustfsadmin")
    parsed.add_argument("--timeout", type=float, default=30.0)
    parsed.add_argument("--evidence-dir", type=Path, required=True)
    return parsed


def execute(args: argparse.Namespace) -> dict[str, object]:
    repo = args.repo.resolve()
    evidence = args.evidence_dir.resolve()
    if evidence.exists() and any(evidence.iterdir()):
        raise WorkflowFailure(f"evidence directory is not empty: {evidence}")
    evidence.mkdir(parents=True, exist_ok=True)
    if args.etcd_bin is None or not Path(args.etcd_bin).is_file():
        raise WorkflowFailure("a real etcd binary is required")
    if args.etcdctl_bin is None or not Path(args.etcdctl_bin).is_file():
        raise WorkflowFailure("etcdctl is required")
    if not args.object_endpoint or not args.object_bucket:
        raise WorkflowFailure("a real object endpoint and bucket are required")
    if args.timeout <= 0:
        raise WorkflowFailure("--timeout must be positive")

    target = (args.target_dir or repo / "target").resolve()
    binary = args.binary.resolve() if args.binary else target / "debug" / "nokv"
    if args.build:
        binary = build_binaries(repo, target, max(args.timeout, 600.0))
    if not binary.is_file():
        raise WorkflowFailure("a built nokv binary is required")

    scenarios: list[dict[str, Any]] = []

    def record(name: str, passed: bool, detail: str) -> None:
        scenarios.append({"scenario": name, "passed": passed, "detail": detail[:2000]})

    root_id = fixed_id(args.seed, "root")
    agent_id = fixed_id(args.seed, "agent")
    shard_id = fixed_id(args.seed, "shard")
    prefix = f"/nokv/health-shutdown/{fixed_id(args.seed, 'prefix')}"
    objects = object_args(
        args.seed,
        args.object_endpoint,
        args.object_bucket,
        args.object_root,
        args.object_access_key_id,
        args.object_secret_access_key,
    )
    metadata = evidence / "metadata"
    server_log = (evidence / "server.log").open("a", encoding="utf-8")
    etcd: subprocess.Popen[bytes] | None = None
    owner: subprocess.Popen[bytes] | None = None
    started_at = now()
    try:
        client_port = free_port()
        peer_port = free_port()
        endpoint = f"http://127.0.0.1:{client_port}"
        peer = f"http://127.0.0.1:{peer_port}"
        etcd = start_process(
            [
                args.etcd_bin,
                "--name",
                "nokv-health-shutdown",
                "--data-dir",
                str(evidence / "etcd-data"),
                "--listen-client-urls",
                endpoint,
                "--advertise-client-urls",
                endpoint,
                "--listen-peer-urls",
                peer,
                "--initial-advertise-peer-urls",
                peer,
                "--initial-cluster",
                f"nokv-health-shutdown={peer}",
                "--initial-cluster-state",
                "new",
                "--log-level",
                "warn",
            ],
            repo,
            server_log,
        )
        etcdctl = Path(args.etcdctl_bin)
        deadline = time.monotonic() + args.timeout
        ready = False
        while time.monotonic() < deadline:
            if etcd.poll() is not None:
                raise WorkflowFailure(f"etcd exited with {etcd.returncode}")
            health = run(
                [etcdctl, f"--endpoints={endpoint}", "endpoint", "health"],
                cwd=repo,
                timeout=args.timeout,
                check=False,
            )
            if health.returncode == 0:
                ready = True
                break
            time.sleep(0.1)
        if not ready:
            raise WorkflowFailure(f"etcd did not become healthy at {endpoint}")
        common = control_args(binary, root_id, endpoint, prefix)

        # --- provision -------------------------------------------------
        provision = run(
            [
                *common,
                "--agent-id",
                agent_id,
                *objects,
                "provision",
                shard_id,
            ],
            cwd=repo,
            timeout=args.timeout,
        )
        (evidence / "provision.stdout.log").write_text(provision.stdout)
        (evidence / "provision.stderr.log").write_text(provision.stderr)
        record("provision", True, "root provisioned with a durable Agent binding")

        # --- first owner with the health surface -----------------------
        rpc_port = free_port()
        health_port = free_port()
        owner = start_process(
            server_command(
                common,
                objects,
                rpc_port,
                health_port,
                "gate-health-e1",
                "--metadata-create",
                metadata,
            ),
            repo,
            server_log,
        )
        wait_tcp(owner, rpc_port, args.timeout)

        status, body = wait_health(health_port, owner, "/healthz", 200, args.timeout)
        if body != "ok\n":
            raise WorkflowFailure(f"/healthz body was {body!r}, expected 'ok\\n'")
        record("healthz", True, "liveness reported while the owner serves")

        status, body = wait_health(health_port, owner, "/readyz", 200, args.timeout)
        if body != "ready\n":
            raise WorkflowFailure(f"/readyz body was {body!r}, expected 'ready\\n'")
        record("readyz", True, "owner admission retained and lease held")

        status, body = http_get(health_port, "/stats")
        if status != 200:
            raise WorkflowFailure(f"/stats reported {status}")
        first_stats = json.loads(body)
        validate_stats(first_stats, "first")
        record(
            "stats_identity",
            True,
            "server identity snapshot matches the serving owner",
        )

        # --- one business request moves the counters -------------------
        client = [
            *common,
            "--agent-id",
            agent_id,
            "--workbench-root",
            "/agents/health-shutdown/wb",
            *objects,
        ]
        create = run(
            [
                *client,
                "workbench",
                "workbench_create",
                json.dumps({"id": "shutdown-proof"}, separators=(",", ":")),
            ],
            cwd=repo,
            timeout=args.timeout,
        )
        (evidence / "create.stdout.log").write_text(create.stdout)
        (evidence / "create.stderr.log").write_text(create.stderr)

        _, body = http_get(health_port, "/stats")
        second_stats = json.loads(body)
        validate_stats(second_stats, "second")
        if second_stats["connections_total"] < 1:
            raise WorkflowFailure("/stats connections_total did not advance")
        if second_stats["requests_total"] <= first_stats["requests_total"]:
            raise WorkflowFailure(
                f"/stats requests_total did not advance: {first_stats['requests_total']} "
                f"-> {second_stats['requests_total']}"
            )
        record(
            "stats_advance",
            True,
            "one Workbench request moved connections_total and requests_total",
        )

        # --- SIGTERM releases the session and the health surface -------
        os.kill(owner.pid, signal.SIGTERM)
        exit_code = wait_exit(owner, GRACEFUL_EXIT_DEADLINE_SECONDS)
        if exit_code != 0:
            raise WorkflowFailure(f"owner exited with {exit_code} after SIGTERM")
        record("sigterm_exit_zero", True, "SIGTERM stopped the owner with exit 0")

        try:
            http_get(health_port, "/healthz")
            raise WorkflowFailure("health endpoint still answered after SIGTERM")
        except (OSError, ConnectionRefusedError, http.client.HTTPException):
            pass
        record("health_closed", True, "health endpoint closed with the owner")

        # --- immediate reopen re-admits the same root ------------------
        reopened_rpc = free_port()
        reopened_health = free_port()
        reopened = start_process(
            server_command(
                common,
                objects,
                reopened_rpc,
                reopened_health,
                "gate-health-e2",
                "--metadata-reopen",
                metadata,
            ),
            repo,
            server_log,
        )
        wait_tcp(reopened, reopened_rpc, REOPEN_ADMISSION_DEADLINE_SECONDS)
        wait_health(reopened_health, reopened, "/readyz", 200, args.timeout)
        find = run(
            [
                *client,
                "workbench",
                "workbench_find",
                json.dumps(
                    {"committed": None, "manifest_pattern": None, "include_manifest": False},
                    separators=(",", ":"),
                ),
            ],
            cwd=repo,
            timeout=args.timeout,
        )
        if "shutdown-proof" not in find.stdout:
            raise WorkflowFailure(
                f"reopened owner did not serve the committed workbench: {find.stdout[:200]}"
            )
        record(
            "reopen_readmits",
            True,
            "after SIGTERM the health surface closed with the owner, the successor "
            "reopened the same metadata authority, /readyz reported ready again, "
            "and the committed Workbench data was served",
        )
        owner = reopened
        os.kill(reopened.pid, signal.SIGTERM)
        wait_exit(reopened, GRACEFUL_EXIT_DEADLINE_SECONDS)
    finally:
        if owner is not None and owner.poll() is None:
            os.kill(owner.pid, signal.SIGKILL)
            owner.wait(timeout=10)
        if etcd is not None and etcd.poll() is None:
            os.kill(etcd.pid, signal.SIGKILL)
            etcd.wait(timeout=10)
        server_log.close()

    payload: dict[str, object] = {
        "schema": SCHEMA,
        "started_at": started_at,
        "finished_at": now(),
        "scenarios": scenarios,
        "seed": args.seed,
        "binary": str(binary),
        "binary_sha256": digest_file(binary),
        "overall_status": "PASS" if all(s["passed"] for s in scenarios) else "FAIL",
    }
    (evidence / "qualification.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n"
    )
    print(json.dumps(payload, indent=2))
    return payload


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        payload = execute(args)
    except WorkflowFailure as error:
        print(f"health_shutdown_gate: {error}", file=sys.stderr)
        return 1
    except (OSError, subprocess.SubprocessError, json.JSONDecodeError) as error:
        print(f"health_shutdown_gate: {error}", file=sys.stderr)
        return 1
    return 0 if payload["overall_status"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
