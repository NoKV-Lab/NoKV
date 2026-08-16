#!/usr/bin/env python3
"""Real etcd/RustFS namespace, restart, and retry qualification.

The gate owns an isolated etcd member and a digest-pinned RustFS container. It
records a small PhyMat campaign, kills the metadata owner, reopens the same Holt
directory, rejects a healthy but different object prefix before metadata or
control mutation, and proves that an in-flight RustFS outage is reported as a
redacted retryable error before the same logical read succeeds after recovery.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import platform
import selectors
import shutil
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Sequence, TextIO


SCHEMA = "nokv.object_namespace_recovery_gate.v1"
PROTOCOL_VERSION = "2025-11-25"
PINNED_RUSTFS_IMAGE = (
    "rustfs/rustfs@sha256:"
    "e620d37756fff072b10bf648c7bb9d370d7e91a928b7e6a5e1ac85bdfb4e4dab"
)
MARKER_MAGIC = b"nokv.object-namespace\0"
SECRET_FLAGS = {"--object-secret-access-key", "--secret-key"}
PHYSICAL_ERROR_TOKENS = (
    "http://",
    "https://",
    "127.0.0.1",
    "bucket=",
    "endpoint=",
    "object-root",
    "nokv/artifacts/",
)


class NotQualified(RuntimeError):
    pass


class WorkflowFailure(RuntimeError):
    pass


def now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def digest_file(path: Path) -> str:
    state = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            state.update(block)
    return state.hexdigest()


def fixed_id(seed: str, label: str) -> str:
    return hashlib.sha256(f"{seed}:{label}".encode()).hexdigest()[:32]


def phymat_documents() -> tuple[dict[str, Any], dict[str, Any]]:
    """Return deterministic, simulated PhyMat campaign evidence."""
    structure = {
        "schema": "phymat.structure.v1",
        "material_id": "mp-19017",
        "formula": "LiFePO4",
        "sites": 28,
        "source": "synthetic-release-gate",
    }
    structure_bytes = (canonical_json(structure) + "\n").encode()
    relaxation = {
        "schema": "phymat.relaxation_evidence.v1",
        "material_id": "mp-19017",
        "screening": {
            "model": "MACE-MP-0 fixture",
            "accepted": True,
            "max_force_ev_per_angstrom": 0.18,
        },
        "dft": {
            "calculator": "VASP-compatible fixture",
            "converged": True,
            "energy_ev": -203.417,
            "max_force_ev_per_angstrom": 0.014,
        },
        "thermodynamics": {
            "temperature_k": 300,
            "formation_energy_ev_per_atom": -2.134,
            "hull_distance_ev_per_atom": 0.012,
        },
        "provenance": {
            "structure_digest_uri": "sha256:" + digest(structure_bytes),
            "workflow": "ml-potential-screening-to-dft-relaxation",
        },
    }
    return structure, relaxation


def _mapping(parent: dict[str, object], key: str) -> dict[str, object]:
    value = parent.get(key)
    if not isinstance(value, dict):
        raise WorkflowFailure(f"gate evidence lacks {key}")
    return value


def validate_gate_evidence(evidence: dict[str, object]) -> None:
    if evidence.get("status") != "PASS":
        raise WorkflowFailure("gate terminal status is not PASS")
    namespace = _mapping(evidence, "namespace")
    if namespace.get("bound_id") != namespace.get("marker_id"):
        raise WorkflowFailure("control binding and durable namespace marker differ")
    if namespace.get("mismatch_rejected") is not True:
        raise WorkflowFailure("healthy wrong object namespace was not rejected")
    if namespace.get("control_record_unchanged") is not True:
        raise WorkflowFailure("wrong object profile mutated the owner control record")
    if namespace.get("metadata_mutation_blocked") is not True:
        raise WorkflowFailure("wrong object profile mutated workspace metadata")
    if namespace.get("agent_identity_redacted") is not True:
        raise WorkflowFailure("wrong object profile error leaked an AgentId")
    if namespace.get("wrong_profile_objects") != 1:
        raise WorkflowFailure("wrong object profile contains payload objects")

    restart = _mapping(evidence, "restart")
    if restart.get("signal") != "SIGKILL":
        raise WorkflowFailure("metadata owner restart was not induced by SIGKILL")
    if restart.get("session_absent_before_retry") is not True:
        raise WorkflowFailure("owner session remained live before restart retry")
    before, after = restart.get("before_owner_epoch"), restart.get("after_owner_epoch")
    if not isinstance(before, int) or after != before + 1:
        raise WorkflowFailure("stable owner restart must advance the epoch exactly once")

    outage = _mapping(evidence, "outage")
    error = _mapping(outage, "error")
    if error.get("code") != "ObjectUnavailable" or error.get("retryable") is not True:
        raise WorkflowFailure("RustFS outage must be ObjectUnavailable and retryable")
    details = _mapping(error, "details")
    if not isinstance(details.get("attempts"), int) or details.get("attempts", 0) < 1:
        raise WorkflowFailure("retryable outage lacks a positive attempt count")
    rendered_error = canonical_json(error).lower()
    if any(token in rendered_error for token in PHYSICAL_ERROR_TOKENS):
        raise WorkflowFailure("public error leaked physical object identity")
    failed_request_digest = outage.get("failed_request_digest")
    recovered_request_digest = outage.get("recovered_request_digest")
    if (
        not isinstance(failed_request_digest, str)
        or len(failed_request_digest) != 64
        or failed_request_digest != recovered_request_digest
    ):
        raise WorkflowFailure("outage retry did not preserve the logical request")

    phymat = _mapping(evidence, "phymat")
    expected_structure, expected_relaxation = phymat_documents()
    if phymat.get("structure") != expected_structure or phymat.get("relaxation") != expected_relaxation:
        raise WorkflowFailure("PhyMat source evidence differs from the deterministic fixture")
    if phymat.get("commit_replay_converged") is not True:
        raise WorkflowFailure("PhyMat commit replay did not converge")
    if restart.get("structure") != expected_structure or restart.get("relaxation") != expected_relaxation:
        raise WorkflowFailure("PhyMat evidence did not survive the owner restart")
    if outage.get("recovered") != expected_relaxation:
        raise WorkflowFailure("PhyMat evidence did not survive the RustFS outage")


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def redact_argv(argv: Sequence[os.PathLike[str] | str]) -> list[str]:
    output: list[str] = []
    redact_next = False
    for raw in argv:
        argument = str(raw)
        if redact_next:
            output.append(f"<redacted:sha256:{digest(argument.encode())[:12]}>")
            redact_next = False
        elif argument.startswith("RUSTFS_SECRET_KEY="):
            output.append("RUSTFS_SECRET_KEY=<redacted>")
        else:
            output.append(argument)
            redact_next = argument in SECRET_FLAGS
    return output


class Evidence:
    def __init__(self, root: Path) -> None:
        self.root = root

    def prepare(self) -> None:
        if self.root.exists() and any(self.root.iterdir()):
            raise WorkflowFailure(f"evidence directory is not empty: {self.root}")
        self.root.mkdir(parents=True, exist_ok=True)

    def json(self, name: str, value: Any) -> None:
        (self.root / name).write_text(
            json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )

    def line(self, name: str, value: Any) -> None:
        with (self.root / name).open("a", encoding="utf-8") as output:
            output.write(canonical_json(value) + "\n")

    def text(self, name: str, value: str) -> None:
        (self.root / name).write_text(value.rstrip() + "\n", encoding="utf-8")


def run(
    argv: Sequence[os.PathLike[str] | str],
    *,
    cwd: Path,
    timeout: float,
    check: bool = True,
    env: dict[str, str] | None = None,
    evidence: Evidence | None = None,
    label: str | None = None,
) -> subprocess.CompletedProcess[str]:
    started = now()
    try:
        result = subprocess.run(
            [str(item) for item in argv],
            cwd=cwd,
            text=True,
            capture_output=True,
            timeout=timeout,
            check=False,
            env=env,
        )
    except subprocess.TimeoutExpired:
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
                    "timeout_seconds": timeout,
                },
            )
        raise WorkflowFailure(
            f"{label or 'command'} timed out after {timeout:g} seconds"
        ) from None
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
        raise WorkflowFailure(f"{label or 'command'} failed ({result.returncode}): {detail}")
    return result


def start_process(
    argv: Sequence[os.PathLike[str] | str], cwd: Path, log: TextIO
) -> subprocess.Popen[str]:
    return subprocess.Popen(
        [str(item) for item in argv],
        cwd=cwd,
        stdin=subprocess.DEVNULL,
        stdout=log,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )


def stop_process(
    process: subprocess.Popen[str] | None,
    sig: signal.Signals = signal.SIGTERM,
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
            raise WorkflowFailure(f"process exited before listening ({process.returncode})")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise WorkflowFailure(f"process did not listen on 127.0.0.1:{port}")


def wait_etcd(
    etcdctl: Path,
    endpoint: str,
    process: subprocess.Popen[str],
    repo: Path,
    timeout: float,
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise WorkflowFailure(f"etcd exited before readiness ({process.returncode})")
        result = run(
            [etcdctl, f"--endpoints={endpoint}", "endpoint", "health"],
            cwd=repo,
            timeout=timeout,
            check=False,
        )
        if result.returncode == 0:
            return
        time.sleep(0.1)
    raise WorkflowFailure(f"etcd did not become healthy at {endpoint}")


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


def aws_command(aws: Path, endpoint: str, *arguments: os.PathLike[str] | str) -> list[str]:
    return [
        str(aws),
        "--cli-connect-timeout", "1",
        "--cli-read-timeout", "2",
        "--endpoint-url", endpoint,
        *(str(argument) for argument in arguments),
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


def wait_rustfs(
    aws: Path,
    endpoint: str,
    environment: dict[str, str],
    repo: Path,
    timeout: float,
    *,
    docker: Path | None = None,
    container: str | None = None,
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = run(
            aws_command(aws, endpoint, "s3api", "list-buckets"),
            cwd=repo,
            timeout=timeout,
            check=False,
            env=environment,
        )
        if result.returncode == 0:
            return
        if docker is not None and container is not None:
            state = run(
                [
                    docker,
                    "inspect",
                    "--format",
                    "{{.State.Status}} {{.State.ExitCode}}",
                    container,
                ],
                cwd=repo,
                timeout=min(timeout, 5.0),
                check=False,
            )
            if state.returncode != 0:
                raise WorkflowFailure("RustFS container disappeared before readiness")
            status_and_code = state.stdout.strip().split()
            if status_and_code and status_and_code[0] in {"dead", "exited"}:
                exit_code = status_and_code[1] if len(status_and_code) > 1 else "unknown"
                raise WorkflowFailure(
                    f"RustFS container exited before readiness (exit code {exit_code})"
                )
        time.sleep(0.25)
    raise WorkflowFailure(f"RustFS did not become ready at {endpoint}")


def capture_rustfs_log(
    docker: Path,
    container: str,
    repo: Path,
    timeout: float,
    evidence: Evidence,
    secrets: Sequence[str],
) -> None:
    try:
        result = run(
            [docker, "logs", "--tail", "200", container],
            cwd=repo,
            timeout=min(timeout, 10.0),
            check=False,
        )
        content = result.stdout + result.stderr
    except WorkflowFailure as error:
        content = f"RustFS log capture failed: {error}"
    for secret in secrets:
        if secret:
            content = content.replace(secret, "<redacted>")
    evidence.text("rustfs.log", content or "RustFS produced no container log output")


def etcd_value(etcdctl: Path, endpoint: str, key: str, repo: Path, timeout: float) -> str:
    return run(
        [etcdctl, f"--endpoints={endpoint}", "get", key, "--print-value-only"],
        cwd=repo,
        timeout=timeout,
    ).stdout.strip()


def wait_session_absent(
    etcdctl: Path, endpoint: str, key: str, repo: Path, timeout: float
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not etcd_value(etcdctl, endpoint, key, repo, timeout):
            return
        time.sleep(0.1)
    raise WorkflowFailure(f"owner session remained live before retry: {key}")


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


def object_args(
    endpoint: str, bucket: str, root: str, access_key: str, secret_key: str
) -> list[str]:
    return [
        "--object-bucket",
        bucket,
        "--object-endpoint",
        endpoint,
        "--object-root",
        root,
        "--object-region",
        "us-east-1",
        "--object-access-key-id",
        access_key,
        "--object-secret-access-key",
        secret_key,
    ]


def server_command(
    common: list[str],
    objects: list[str],
    port: int,
    node: str,
    metadata_option: str,
    metadata: Path,
) -> list[str]:
    if "--agent-id" in common:
        raise WorkflowFailure("shard serve command must not carry one AgentId")
    return [
        *common,
        *objects,
        "--bind",
        f"127.0.0.1:{port}",
        "--advertise-endpoint",
        f"127.0.0.1:{port}",
        "--node-id",
        node,
        metadata_option,
        str(metadata),
        "--lifecycle-interval-millis",
        "100",
        "serve",
    ]


def workbench_call(
    client: list[str],
    tool: str,
    arguments: dict[str, Any],
    repo: Path,
    timeout: float,
    evidence: Evidence,
    label: str,
) -> dict[str, Any]:
    result = run(
        [*client, "workbench", tool, canonical_json(arguments)],
        cwd=repo,
        timeout=timeout,
        evidence=evidence,
        label=label,
    )
    try:
        decoded = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise WorkflowFailure(f"{label} returned invalid JSON") from error
    if not isinstance(decoded, dict) or decoded.get("status") != "success":
        raise WorkflowFailure(f"{label} did not succeed: {decoded!r}")
    return decoded


def read_document(result: dict[str, Any], label: str) -> dict[str, Any]:
    items = result.get("items")
    if (
        result.get("record_type") != "json_object"
        or not isinstance(items, list)
        or len(items) != 1
        or not isinstance(items[0], dict)
        or not isinstance(items[0].get("value"), dict)
    ):
        raise WorkflowFailure(f"{label} did not return one structured document")
    return items[0]["value"]


class McpClient:
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
        self.last_call_digest: str | None = None

    def request(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        request_id = self.next_id
        self.next_id += 1
        request: dict[str, Any] = {"jsonrpc": "2.0", "id": request_id, "method": method}
        if params is not None:
            request["params"] = params
        encoded = canonical_json(request)
        if self.process.stdin is None or self.process.stdout is None:
            raise WorkflowFailure("MCP pipes are unavailable")
        self.process.stdin.write(encoded + "\n")
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
        self.evidence.line(
            "mcp-transcript.jsonl",
            {
                "schema": SCHEMA,
                "request": request,
                "response": response,
                "recorded_at": now(),
            },
        )
        if response.get("jsonrpc") != "2.0" or response.get("id") != request_id:
            raise WorkflowFailure("MCP response envelope mismatch")
        return response

    def notify(self, method: str) -> None:
        if self.process.stdin is None:
            raise WorkflowFailure("MCP stdin is unavailable")
        self.process.stdin.write(canonical_json({"jsonrpc": "2.0", "method": method}) + "\n")
        self.process.stdin.flush()

    def initialize(self) -> None:
        response = self.request(
            "initialize",
            {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {"name": "object-namespace-gate", "version": "1"},
            },
        )
        result = response.get("result")
        if not isinstance(result, dict) or result.get("serverInfo", {}).get("name") != "nokv-mcp":
            raise WorkflowFailure("unexpected MCP initialize result")
        self.notify("notifications/initialized")

    def call(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        params = {"name": name, "arguments": arguments}
        self.last_call_digest = digest(canonical_json(params).encode())
        response = self.request("tools/call", params)
        result = response.get("result")
        structured = result.get("structuredContent") if isinstance(result, dict) else None
        if not isinstance(structured, dict):
            raise WorkflowFailure(f"{name} lacks structuredContent")
        content = result.get("content")
        try:
            text_value = json.loads(content[0]["text"])
        except (IndexError, KeyError, TypeError, json.JSONDecodeError) as error:
            raise WorkflowFailure(f"{name} lacks matching JSON text content") from error
        if text_value != structured:
            raise WorkflowFailure(f"{name} text and structured results differ")
        return structured


def parse_control_record(raw: str, label: str) -> dict[str, Any]:
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as error:
        raise WorkflowFailure(f"{label} is not JSON") from error
    if not isinstance(value, dict):
        raise WorkflowFailure(f"{label} is not an object")
    return value


def decode_marker(path: Path) -> str:
    marker = path.read_bytes()
    expected_length = len(MARKER_MAGIC) + 1 + 16
    if len(marker) != expected_length or not marker.startswith(MARKER_MAGIC):
        raise WorkflowFailure("RustFS namespace marker has invalid framing")
    if marker[len(MARKER_MAGIC)] != 1:
        raise WorkflowFailure("RustFS namespace marker has unsupported version")
    return marker[-16:].hex()


def build_binary(repo: Path, target: Path, timeout: float) -> Path:
    environment = os.environ.copy()
    environment["CARGO_TARGET_DIR"] = str(target)
    result = subprocess.run(
        ["cargo", "build", "-p", "nokv", "--bin", "nokv"],
        cwd=repo,
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
        env=environment,
    )
    if result.returncode != 0:
        raise WorkflowFailure(f"build failed: {(result.stderr or result.stdout).strip()}")
    return target / "debug" / "nokv"


def execute(args: argparse.Namespace) -> dict[str, object]:
    repo = args.repo.resolve()
    evidence = Evidence(args.evidence_dir.resolve())
    evidence.prepare()
    if args.timeout <= 0:
        raise WorkflowFailure("--timeout must be positive")
    if "@sha256:" not in args.rustfs_image:
        raise WorkflowFailure("--rustfs-image must be digest pinned")
    required = {
        "etcd": args.etcd_bin,
        "etcdctl": args.etcdctl_bin,
        "docker": args.docker_bin,
        "aws": args.aws_bin,
    }
    for name, path in required.items():
        if path is None or not Path(path).is_file():
            raise NotQualified(f"{name} executable is required")

    target = (args.target_dir or repo / "target").resolve()
    binary = args.binary.resolve() if args.binary else target / "debug" / "nokv"
    if args.build:
        binary = build_binary(repo, target, max(args.timeout, 900.0))
    if not binary.is_file():
        raise NotQualified("built nokv binary is required")

    root_id = fixed_id(args.seed, "root")
    agent_id = fixed_id(args.seed, "agent")
    shard_id = fixed_id(args.seed, "shard")
    other_root_id = fixed_id(args.seed, "wrong-root")
    other_agent_id = fixed_id(args.seed, "wrong-agent")
    other_shard_id = fixed_id(args.seed, "wrong-shard")
    prefix = f"/nokv/object-namespace-gate/{fixed_id(args.seed, 'control')}"
    other_prefix = f"/nokv/object-namespace-gate/{fixed_id(args.seed, 'other-control')}"
    object_root = f"object-namespace-gate/{fixed_id(args.seed, 'object')}/correct"
    wrong_object_root = f"object-namespace-gate/{fixed_id(args.seed, 'object')}/wrong"
    bucket = f"nokv-object-gate-{fixed_id(args.seed, 'bucket')[:12]}"
    container = f"nokv-object-gate-{os.getpid()}"
    volume = f"{container}-data"
    access_key, secret_key = "rustfsadmin", "rustfsadmin"
    etcd_port, peer_port = free_port(), free_port()
    s3_port, console_port = free_port(), free_port()
    owner_port, retry_port = free_port(), free_port()
    etcd_endpoint = f"http://127.0.0.1:{etcd_port}"
    s3_endpoint = f"http://127.0.0.1:{s3_port}"
    aws_env = aws_environment(access_key, secret_key)
    metadata = evidence.root / "metadata"
    etcd_log = (evidence.root / "etcd.log").open("w", encoding="utf-8")
    owner_log = (evidence.root / "owner-e1.log").open("w", encoding="utf-8")
    retry_log = (evidence.root / "owner-e2.log").open("w", encoding="utf-8")
    mcp_log = (evidence.root / "mcp.stderr.log").open("w", encoding="utf-8")
    etcd: subprocess.Popen[str] | None = None
    owner: subprocess.Popen[str] | None = None
    retry_owner: subprocess.Popen[str] | None = None
    mcp_process: subprocess.Popen[str] | None = None
    container_created = False
    volume_created = False
    started_at = now()

    try:
        peer = f"http://127.0.0.1:{peer_port}"
        etcd = start_process(
            [
                args.etcd_bin,
                "--name",
                "nokv-object-namespace-gate",
                "--data-dir",
                evidence.root / "etcd-data",
                "--listen-client-urls",
                etcd_endpoint,
                "--advertise-client-urls",
                etcd_endpoint,
                "--listen-peer-urls",
                peer,
                "--initial-advertise-peer-urls",
                peer,
                "--initial-cluster",
                f"nokv-object-namespace-gate={peer}",
                "--initial-cluster-state",
                "new",
                "--log-level",
                "warn",
            ],
            repo,
            etcd_log,
        )
        wait_etcd(Path(args.etcdctl_bin), etcd_endpoint, etcd, repo, args.timeout)

        docker = Path(args.docker_bin)
        run(
            [docker, "volume", "create", volume],
            cwd=repo,
            timeout=args.timeout,
            evidence=evidence,
            label="create-rustfs-volume",
        )
        volume_created = True
        run(
            rustfs_container_command(
                docker,
                container=container,
                volume=volume,
                s3_port=s3_port,
                console_port=console_port,
                image=args.rustfs_image,
                access_key=access_key,
                secret_key=secret_key,
            ),
            cwd=repo,
            timeout=args.timeout,
            evidence=evidence,
            label="start-rustfs",
        )
        container_created = True
        wait_rustfs(
            Path(args.aws_bin),
            s3_endpoint,
            aws_env,
            repo,
            args.timeout,
            docker=docker,
            container=container,
        )
        run(
            aws_command(
                Path(args.aws_bin), s3_endpoint, "s3api", "create-bucket", "--bucket", bucket
            ),
            cwd=repo,
            timeout=args.timeout,
            env=aws_env,
            evidence=evidence,
            label="create-bucket",
        )

        common = control_args(binary, root_id, etcd_endpoint, prefix)
        agent_common = [*common, "--agent-id", agent_id]
        objects = object_args(s3_endpoint, bucket, object_root, access_key, secret_key)
        provision = run(
            [*agent_common, *objects, "provision", shard_id],
            cwd=repo,
            timeout=args.timeout,
            evidence=evidence,
            label="provision",
        )
        provisioned = parse_control_record(provision.stdout, "provision result")
        namespace_id = provisioned.get("object_namespace_id")
        if not isinstance(namespace_id, str):
            raise WorkflowFailure("provision did not report object_namespace_id")

        first_command = server_command(
            common, objects, owner_port, "object-gate-e1", "--metadata-create", metadata
        )
        owner = start_process(first_command, repo, owner_log)
        wait_tcp(owner, owner_port, args.timeout)
        owner_key = f"{prefix}/logical-shards/{shard_id}"
        session_key = f"{prefix}/sessions/{shard_id}"
        first_control_raw = etcd_value(
            Path(args.etcdctl_bin), etcd_endpoint, owner_key, repo, args.timeout
        )
        first_control = parse_control_record(first_control_raw, "first owner control record")
        if first_control.get("owner_epoch") != 1 or first_control.get("state") != 3:
            raise WorkflowFailure(f"first owner did not reach Serving(1): {first_control}")

        client = [
            *agent_common,
            "--max-attempts",
            "3",
            "--workbench-root",
            "/agents/phymat/wb",
            *objects,
        ]
        structure, relaxation = phymat_documents()
        structure_text = canonical_json(structure) + "\n"
        relaxation_text = canonical_json(relaxation) + "\n"
        workbench_call(
            client,
            "workbench_create",
            {"id": "phymat-campaign"},
            repo,
            args.timeout,
            evidence,
            "phymat-create",
        )
        workbench_call(
            client,
            "workbench_put_file",
            {
                "id": "phymat-campaign",
                "section": "input",
                "path": "structure.json",
                "text": structure_text,
                "content_type": "application/json",
                "replace": False,
            },
            repo,
            args.timeout,
            evidence,
            "phymat-put-structure",
        )
        workbench_call(
            client,
            "workbench_put_file",
            {
                "id": "phymat-campaign",
                "section": "outputs",
                "path": "relaxation.json",
                "text": relaxation_text,
                "content_type": "application/json",
                "replace": False,
            },
            repo,
            args.timeout,
            evidence,
            "phymat-put-relaxation",
        )
        commit_arguments = {
            "id": "phymat-campaign",
            "manifest": {
                "domain": "materials",
                "material_id": "mp-19017",
                "task": "ml-potential-screening-to-dft-relaxation",
            },
            "content_digest_uri": "sha256:" + digest(relaxation_text.encode()),
            "replace": False,
        }
        commit = workbench_call(
            client,
            "workbench_commit",
            commit_arguments,
            repo,
            args.timeout,
            evidence,
            "phymat-commit",
        )
        replay = workbench_call(
            client,
            "workbench_commit",
            commit_arguments,
            repo,
            args.timeout,
            evidence,
            "phymat-commit-replay",
        )
        commit_replay_converged = (
            commit.get("idempotent_replay") is False
            and replay.get("idempotent_replay") is True
            and commit.get("commit_identity") == replay.get("commit_identity")
        )

        stop_process(owner, signal.SIGKILL)
        owner_exit = owner.returncode
        wait_session_absent(
            Path(args.etcdctl_bin), etcd_endpoint, session_key, repo, args.timeout
        )
        owner = None

        retry_command = server_command(
            common,
            objects,
            retry_port,
            "object-gate-e2",
            "--metadata-reopen",
            metadata,
        )
        retry_owner = start_process(retry_command, repo, retry_log)
        wait_tcp(retry_owner, retry_port, args.timeout)
        second_control_raw = etcd_value(
            Path(args.etcdctl_bin), etcd_endpoint, owner_key, repo, args.timeout
        )
        second_control = parse_control_record(second_control_raw, "retry owner control record")
        structure_after = read_document(
            workbench_call(
                client,
                "workbench_read",
                {
                    "id": "phymat-campaign",
                    "section": "input",
                    "path": "structure.json",
                    "format": "structured",
                    "limit": 10,
                },
                repo,
                args.timeout,
                evidence,
                "restart-read-structure",
            ),
            "restart structure",
        )
        read_arguments = {
            "id": "phymat-campaign",
            "section": "outputs",
            "path": "relaxation.json",
            "format": "structured",
            "limit": 10,
        }
        relaxation_after = read_document(
            workbench_call(
                client,
                "workbench_read",
                read_arguments,
                repo,
                args.timeout,
                evidence,
                "restart-read-relaxation",
            ),
            "restart relaxation",
        )

        other_common = control_args(binary, other_root_id, etcd_endpoint, other_prefix)
        other_agent_common = [*other_common, "--agent-id", other_agent_id]
        wrong_objects = object_args(
            s3_endpoint, bucket, wrong_object_root, access_key, secret_key
        )
        run(
            [*other_agent_common, *wrong_objects, "provision", other_shard_id],
            cwd=repo,
            timeout=args.timeout,
            evidence=evidence,
            label="provision-wrong-profile-owner",
        )
        before_mismatch = etcd_value(
            Path(args.etcdctl_bin), etcd_endpoint, owner_key, repo, args.timeout
        )
        wrong_client = [
            *agent_common,
            "--max-attempts",
            "3",
            "--workbench-root",
            "/agents/phymat/wb",
            *wrong_objects,
        ]
        mismatch_arguments = {"id": "namespace-mismatch-probe"}
        mismatch = run(
            [
                *wrong_client,
                "workbench",
                "workbench_create",
                canonical_json(mismatch_arguments),
            ],
            cwd=repo,
            timeout=args.timeout,
            check=False,
            evidence=evidence,
            label="reject-wrong-profile",
        )
        after_mismatch = etcd_value(
            Path(args.etcdctl_bin), etcd_endpoint, owner_key, repo, args.timeout
        )
        mismatch_rejected = (
            mismatch.returncode != 0
            and "object namespace does not match root placement" in mismatch.stderr
        )
        agent_identity_redacted = (
            agent_id not in mismatch.stderr and other_agent_id not in mismatch.stderr
        )
        metadata_mutation_blocked = False
        if mismatch_rejected:
            probe = workbench_call(
                client,
                "workbench_create",
                mismatch_arguments,
                repo,
                args.timeout,
                evidence,
                "create-after-wrong-profile-rejection",
            )
            metadata_mutation_blocked = probe.get("status") == "success"
        listed = run(
            aws_command(
                Path(args.aws_bin),
                s3_endpoint,
                "s3api",
                "list-objects-v2",
                "--bucket",
                bucket,
                "--prefix",
                f"{wrong_object_root}/",
                "--output",
                "json",
            ),
            cwd=repo,
            timeout=args.timeout,
            env=aws_env,
            evidence=evidence,
            label="list-wrong-profile",
        )
        listed_objects = json.loads(listed.stdout)
        contents = listed_objects.get("Contents", [])
        if not isinstance(contents, list):
            raise WorkflowFailure("wrong-profile object listing has invalid Contents")
        wrong_profile_objects = len(contents)

        marker_path = evidence.root / "namespace-marker.bin"
        run(
            aws_command(
                Path(args.aws_bin),
                s3_endpoint,
                "s3api",
                "get-object",
                "--bucket",
                bucket,
                "--key",
                f"{object_root}/nokv/system/object-namespace",
                marker_path,
            ),
            cwd=repo,
            timeout=args.timeout,
            env=aws_env,
            evidence=evidence,
            label="read-namespace-marker",
        )
        marker_id = decode_marker(marker_path)

        mcp_process = subprocess.Popen(
            [*client, "mcp"],
            cwd=repo,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=mcp_log,
            text=True,
            bufsize=1,
            start_new_session=True,
        )
        mcp = McpClient(mcp_process, evidence, max(args.timeout, 45.0))
        mcp.initialize()
        baseline = mcp.call("workbench_read", read_arguments)
        if baseline.get("status") != "success":
            raise WorkflowFailure("MCP baseline object read did not succeed")

        run(
            [args.docker_bin, "stop", "--timeout", "1", container],
            cwd=repo,
            timeout=args.timeout,
            evidence=evidence,
            label="stop-rustfs",
        )
        outage_error = mcp.call("workbench_read", read_arguments)
        failed_request_digest = mcp.last_call_digest
        run(
            [args.docker_bin, "start", container],
            cwd=repo,
            timeout=args.timeout,
            evidence=evidence,
            label="restart-rustfs",
        )
        wait_rustfs(
            Path(args.aws_bin),
            s3_endpoint,
            aws_env,
            repo,
            args.timeout,
            docker=Path(args.docker_bin),
            container=container,
        )
        recovered_result = mcp.call("workbench_read", read_arguments)
        recovered_request_digest = mcp.last_call_digest
        if recovered_result.get("status") != "success":
            raise WorkflowFailure("same logical read did not recover after RustFS restart")
        recovered = read_document(recovered_result, "outage recovery read")

        binding_raw = etcd_value(
            Path(args.etcdctl_bin),
            etcd_endpoint,
            f"{prefix}/root-object-namespaces/{root_id}",
            repo,
            args.timeout,
        )
        binding = parse_control_record(binding_raw, "object namespace binding")
        result: dict[str, object] = {
            "schema": SCHEMA,
            "status": "PASS",
            "started_at": started_at,
            "finished_at": now(),
            "source": {
                "commit": run(
                    ["git", "rev-parse", "HEAD"], cwd=repo, timeout=args.timeout
                ).stdout.strip(),
                "dirty": run(
                    ["git", "status", "--porcelain=v1"], cwd=repo, timeout=args.timeout
                ).stdout.splitlines(),
            },
            "environment": {
                "platform": platform.platform(),
                "python": platform.python_version(),
                "rustfs_image": args.rustfs_image,
                "etcd": run(
                    [args.etcd_bin, "--version"], cwd=repo, timeout=args.timeout
                ).stdout.splitlines(),
            },
            "binary": {"path": str(binary), "sha256": digest_file(binary)},
            "namespace": {
                "bound_id": binding.get("object_namespace_id"),
                "marker_id": marker_id,
                "mismatch_rejected": mismatch_rejected,
                "control_record_unchanged": before_mismatch == after_mismatch,
                "metadata_mutation_blocked": metadata_mutation_blocked,
                "agent_identity_redacted": agent_identity_redacted,
                "wrong_profile_objects": wrong_profile_objects,
            },
            "restart": {
                "signal": "SIGKILL" if owner_exit == -signal.SIGKILL else str(owner_exit),
                "session_absent_before_retry": True,
                "before_owner_epoch": first_control.get("owner_epoch"),
                "after_owner_epoch": second_control.get("owner_epoch"),
                "structure": structure_after,
                "relaxation": relaxation_after,
            },
            "outage": {
                "error": outage_error,
                "failed_request_digest": failed_request_digest,
                "recovered_request_digest": recovered_request_digest,
                "recovered": recovered,
            },
            "phymat": {
                "fixture_kind": "simulated deterministic release-gate evidence",
                "structure": structure,
                "relaxation": relaxation,
                "commit_replay_converged": commit_replay_converged,
            },
        }
        validate_gate_evidence(result)
        return result
    finally:
        stop_process(mcp_process)
        stop_process(retry_owner)
        stop_process(owner)
        stop_process(etcd)
        for log in (mcp_log, retry_log, owner_log, etcd_log):
            log.close()
        if container_created:
            capture_rustfs_log(
                Path(args.docker_bin),
                container,
                repo,
                args.timeout,
                evidence,
                (access_key, secret_key),
            )
            run(
                [args.docker_bin, "rm", "-f", container],
                cwd=repo,
                timeout=max(args.timeout, 10.0),
                check=False,
            )
        if volume_created:
            run(
                [args.docker_bin, "volume", "rm", "-f", volume],
                cwd=repo,
                timeout=max(args.timeout, 10.0),
                check=False,
            )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    repo = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=repo)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--target-dir", type=Path)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--etcd-bin", type=Path, default=shutil.which("etcd"))
    parser.add_argument("--etcdctl-bin", type=Path, default=shutil.which("etcdctl"))
    parser.add_argument("--docker-bin", type=Path, default=shutil.which("docker"))
    parser.add_argument("--aws-bin", type=Path, default=shutil.which("aws"))
    parser.add_argument("--rustfs-image", default=PINNED_RUSTFS_IMAGE)
    parser.add_argument("--seed", default="issue450-object-namespace-v1")
    parser.add_argument("--timeout", type=float, default=45.0)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    evidence = args.evidence_dir.resolve()
    try:
        result = execute(args)
        Evidence(evidence).json("qualification.json", result)
        print(evidence / "qualification.json")
        return 0
    except NotQualified as error:
        status, code = "NOT QUALIFIED", 3
        failure = str(error)
    except Exception as error:  # Preserve a reviewable terminal result.
        status, code = "FAIL", 2
        failure = str(error)
    evidence.mkdir(parents=True, exist_ok=True)
    Evidence(evidence).json(
        "qualification.json",
        {"schema": SCHEMA, "status": status, "finished_at": now(), "error": failure},
    )
    print(f"{status}: {failure}", file=sys.stderr)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
