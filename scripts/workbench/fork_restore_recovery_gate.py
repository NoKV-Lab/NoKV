#!/usr/bin/env python3
"""Real etcd/RustFS qualification for path-native fork-to-restore.

The gate drives the public Workbench surface against an isolated owner, etcd,
and digest-pinned RustFS. It deterministically drops the RPC response after
both restore preparation and terminal publication, kills that owner, reopens
the same Holt directory under a successor epoch, and retries the same logical
restore. It also exercises concurrent identical restores, a nested fork, source
snapshot retirement, and object-inventory proof that payload objects are never
copied by restore.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import copy
import hashlib
import json
import os
import platform
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable, Sequence, TextIO

from object_namespace_recovery_gate import (
    PINNED_RUSTFS_IMAGE,
    Evidence,
    NotQualified,
    WorkflowFailure,
    aws_command,
    aws_environment,
    build_binary,
    canonical_json,
    capture_rustfs_log,
    control_args,
    digest_file,
    etcd_value,
    fixed_id,
    free_port,
    now,
    object_args,
    parse_control_record,
    phymat_documents,
    run,
    rustfs_container_command,
    server_command,
    start_process,
    stop_process,
    wait_etcd,
    wait_rustfs,
    wait_session_absent,
    wait_tcp,
    workbench_call,
)


SCHEMA = "nokv.path_native_fork_restore_gate.v1"
BLOCK_BYTES = 4 * 1024 * 1024
FULL_PROFILE_FILES = 256
CONCURRENT_RESTORES = 16


def fixture_logical_bytes(file_count: int) -> int:
    if file_count <= 0:
        raise WorkflowFailure("--fixture-files must be positive")
    return file_count * BLOCK_BYTES


def fixture_block_marker(index: int) -> bytes:
    if index < 0:
        raise WorkflowFailure("fixture block index must be non-negative")
    identity = index.to_bytes(8, "big")
    return (
        f"nokv-path-native-fork-restore:block:{index:06d}:".encode()
        + hashlib.sha256(b"nokv.path-native-fork.block.v1\0" + identity).digest()
        + b"\n"
    )


def decode_cli_error(stderr: str) -> dict[str, Any]:
    for line in reversed(stderr.splitlines()):
        payload = line.removeprefix("nokv: ")
        try:
            decoded = json.loads(payload)
        except json.JSONDecodeError:
            continue
        if isinstance(decoded, dict) and decoded.get("status") == "error":
            return decoded
    raise WorkflowFailure("nokv CLI failure did not contain a structured error")


def is_retryable_restore_race(error: dict[str, Any]) -> bool:
    details = error.get("details")
    return (
        error.get("code") == "Conflict"
        and error.get("retryable") is True
        and isinstance(details, dict)
        and details.get("conflict") in {None, "OperationState", "ReadVersion"}
    )


def request_operation(frame: bytes) -> str | None:
    # The production codec is named MessagePack. Enum tag strings remain
    # canonical UTF-8 bytes in that frame, so the proxy can classify the two
    # acceptance barriers without importing a second wire implementation.
    for operation in ("prepare_restore", "finalize_restore"):
        if operation.encode() in frame:
            return operation
    try:
        request = json.loads(frame)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    operation = request.get("operation") if isinstance(request, dict) else None
    value = operation.get("operation") if isinstance(operation, dict) else None
    return value if isinstance(value, str) else None


def _recv_exact(stream: socket.socket, length: int) -> bytes:
    chunks: list[bytes] = []
    remaining = length
    while remaining:
        chunk = stream.recv(remaining)
        if not chunk:
            raise ConnectionError("framed peer closed before the declared length")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


class ResponseDropProxy:
    """Stable advertised endpoint with a one-shot post-commit response drop."""

    def __init__(self, port: int, timeout: float) -> None:
        self.port = port
        self.timeout = timeout
        self._listener: socket.socket | None = None
        self._thread: threading.Thread | None = None
        self._stopping = threading.Event()
        self._dropped = threading.Event()
        self._lock = threading.Lock()
        self._target: tuple[str, int] | None = None
        self._drop_operation: str | None = None
        self._on_drop: Callable[[], None] | None = None
        self._errors: list[str] = []

    def start(self) -> None:
        listener = socket.socket()
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", self.port))
        listener.listen()
        listener.settimeout(0.2)
        self._listener = listener
        self._thread = threading.Thread(target=self._serve, name="nokv-restore-proxy", daemon=True)
        self._thread.start()

    def set_target(self, port: int) -> None:
        with self._lock:
            self._target = ("127.0.0.1", port)

    def arm(self, operation: str, on_drop: Callable[[], None]) -> None:
        with self._lock:
            if self._drop_operation is not None:
                raise WorkflowFailure("response-drop proxy is already armed")
            self._drop_operation = operation
            self._on_drop = on_drop
            self._dropped.clear()

    def wait_dropped(self) -> None:
        if not self._dropped.wait(self.timeout):
            raise WorkflowFailure("armed restore response was not observed by the proxy")
        self.raise_if_failed()

    def raise_if_failed(self) -> None:
        if self._errors:
            raise WorkflowFailure(f"response-drop proxy failed: {self._errors[0]}")

    def close(self) -> None:
        self._stopping.set()
        if self._listener is not None:
            self._listener.close()
        if self._thread is not None:
            self._thread.join(timeout=2)
        self.raise_if_failed()

    def _serve(self) -> None:
        assert self._listener is not None
        while not self._stopping.is_set():
            try:
                client, _ = self._listener.accept()
            except socket.timeout:
                continue
            except OSError:
                if self._stopping.is_set():
                    return
                raise
            threading.Thread(
                target=self._forward,
                args=(client,),
                name="nokv-restore-proxy-connection",
                daemon=True,
            ).start()

    def _forward(self, client: socket.socket) -> None:
        try:
            client.settimeout(self.timeout)
            first = client.recv(4)
            if not first:
                return
            header = first + _recv_exact(client, 4 - len(first))
            request = _recv_exact(client, int.from_bytes(header, "big"))
            with self._lock:
                target = self._target
            if target is None:
                raise ConnectionError("response-drop proxy has no owner target")
            with socket.create_connection(target, timeout=self.timeout) as owner:
                owner.settimeout(self.timeout)
                owner.sendall(header + request)
                response_header = _recv_exact(owner, 4)
                response = _recv_exact(owner, int.from_bytes(response_header, "big"))

            callback: Callable[[], None] | None = None
            with self._lock:
                if request_operation(request) == self._drop_operation:
                    callback = self._on_drop
                    self._drop_operation = None
                    self._on_drop = None
            if callback is not None:
                callback()
                self._dropped.set()
                return
            client.sendall(response_header + response)
        except (ConnectionError, OSError, TimeoutError) as error:
            # Owner transitions intentionally leave a small connection-refused
            # window. Only an armed request must be fail-closed and observable.
            with self._lock:
                armed = self._drop_operation is not None
            if armed:
                self._errors.append(str(error))
                self._dropped.set()
        finally:
            client.close()


def proxy_server_command(
    common: list[str],
    objects: list[str],
    bind_port: int,
    advertise_port: int,
    node: str,
    metadata_option: str,
    metadata: Path,
) -> list[str]:
    command = server_command(common, objects, bind_port, node, metadata_option, metadata)
    index = command.index("--advertise-endpoint") + 1
    command[index] = f"127.0.0.1:{advertise_port}"
    return command


def object_inventory(
    aws: Path,
    endpoint: str,
    bucket: str,
    prefix: str,
    environment: dict[str, str],
    repo: Path,
    timeout: float,
    evidence: Evidence,
    label: str,
) -> dict[str, tuple[int, str]]:
    listed = run(
        aws_command(
            aws,
            endpoint,
            "s3api",
            "list-objects-v2",
            "--bucket",
            bucket,
            "--prefix",
            f"{prefix}/",
            "--output",
            "json",
        ),
        cwd=repo,
        timeout=timeout,
        env=environment,
        evidence=evidence,
        label=label,
    )
    decoded = json.loads(listed.stdout)
    rows = decoded.get("Contents", [])
    if not isinstance(rows, list):
        raise WorkflowFailure("RustFS inventory Contents is not a list")
    inventory: dict[str, tuple[int, str]] = {}
    for row in rows:
        if not isinstance(row, dict):
            raise WorkflowFailure("RustFS inventory row is not an object")
        key, size, etag = row.get("Key"), row.get("Size"), row.get("ETag")
        if not isinstance(key, str) or not isinstance(size, int) or not isinstance(etag, str):
            raise WorkflowFailure("RustFS inventory row lacks Key/Size/ETag")
        inventory[key] = (size, etag)
    return inventory


def manifest_only_delta(
    before: dict[str, tuple[int, str]],
    after: dict[str, tuple[int, str]],
    *,
    source: str,
    destination: str,
    aws: Path,
    endpoint: str,
    bucket: str,
    environment: dict[str, str],
    repo: Path,
    timeout: float,
    evidence: Evidence,
    label: str,
) -> str:
    overwritten = sorted(key for key in before.keys() & after.keys() if before[key] != after[key])
    added = sorted(after.keys() - before.keys())
    if overwritten or len(added) != 1:
        raise WorkflowFailure(
            f"{label} was not zero-copy: overwritten={overwritten}, added={added}"
        )
    downloaded = evidence.root / f"{label}-manifest.json"
    run(
        aws_command(
            aws,
            endpoint,
            "s3api",
            "get-object",
            "--bucket",
            bucket,
            "--key",
            added[0],
            downloaded,
        ),
        cwd=repo,
        timeout=timeout,
        env=environment,
        evidence=evidence,
        label=f"{label}-download-manifest",
    )
    try:
        manifest = json.loads(downloaded.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise WorkflowFailure(f"{label} added object is not a JSON restore manifest") from error
    validate_restore_manifest(manifest, source, destination, label)
    return added[0]


def validate_restore_manifest(
    manifest: object,
    source: str,
    destination: str,
    label: str = "restore manifest",
) -> None:
    source_path = f"/agents/materials/wb/{source}"
    destination_path = f"/agents/materials/wb/{destination}"
    if not isinstance(manifest, dict):
        raise WorkflowFailure(f"{label} is not a JSON object")
    snapshot_id = manifest.get("snapshot_id")
    operation_id = manifest.get("operation_id")
    restored_from = manifest.get("restored_from")
    if (
        manifest.get("schema") != "nokv.workbench.restore_manifest.v1"
        or manifest.get("source_workbench_id") != source
        or manifest.get("source_path") != source_path
        or manifest.get("destination_workbench_id") != destination
        or manifest.get("destination_path") != destination_path
        or not isinstance(snapshot_id, int)
        or snapshot_id <= 0
        or not isinstance(operation_id, str)
        or len(operation_id) != 32
        or restored_from
        != {
            "workbench_id": source,
            "path": source_path,
            "snapshot_id": snapshot_id,
        }
    ):
        raise WorkflowFailure(f"{label} is not bound to the exact fork identities")


def read_json_document(
    client: list[str],
    workbench: str,
    section: str,
    path: str,
    repo: Path,
    timeout: float,
    evidence: Evidence,
    label: str,
) -> dict[str, Any]:
    result = workbench_call(
        client,
        "workbench_read",
        {
            "id": workbench,
            "section": section,
            "path": path,
            "format": "structured",
            "limit": 10,
        },
        repo,
        timeout,
        evidence,
        label,
    )
    items = result.get("items")
    if (
        result.get("record_type") != "json_object"
        or not isinstance(items, list)
        or len(items) != 1
        or not isinstance(items[0], dict)
        or not isinstance(items[0].get("value"), dict)
    ):
        raise WorkflowFailure(f"{label} did not return one JSON document")
    return items[0]["value"]


def validate_restore_outcome(outcome: dict[str, Any], destination: str) -> str:
    operation_id = outcome.get("operation_id")
    if (
        outcome.get("status") != "success"
        or outcome.get("state") != "complete"
        or outcome.get("destination_workbench_id") != destination
        or not isinstance(operation_id, str)
        or len(operation_id) != 32
    ):
        raise WorkflowFailure(f"restore did not return a complete exact outcome: {outcome!r}")
    return operation_id


def validate_gate_evidence(evidence: dict[str, object]) -> None:
    if evidence.get("status") != "PASS":
        raise WorkflowFailure("fork restore gate terminal status is not PASS")
    fixture = evidence.get("fixture")
    if not isinstance(fixture, dict):
        raise WorkflowFailure("fork restore evidence lacks fixture")
    files = fixture.get("files")
    if not isinstance(files, int) or fixture.get("logical_bytes") != fixture_logical_bytes(files):
        raise WorkflowFailure("fork restore fixture size is inconsistent")
    if (
        fixture.get("object_count") != files
        or fixture.get("object_bytes") != fixture.get("logical_bytes")
        or fixture.get("distinct_etags") != files
        or not isinstance(fixture.get("marker_digest"), str)
        or len(fixture["marker_digest"]) != 64
    ):
        raise WorkflowFailure("fork restore fixture object closure is incomplete")
    if fixture.get("full_profile") is True and files != FULL_PROFILE_FILES:
        raise WorkflowFailure("full fork restore profile is not exactly one GiB")

    restarts = evidence.get("restarts")
    if not isinstance(restarts, list) or len(restarts) != 2:
        raise WorkflowFailure("fork restore gate must exercise two owner kills")
    for index, expected_phase in enumerate(("prepare_restore", "finalize_restore"), start=1):
        restart = restarts[index - 1]
        if (
            not isinstance(restart, dict)
            or restart.get("phase") != expected_phase
            or restart.get("signal") != "SIGKILL"
            or restart.get("request_failed_before_retry") is not True
            or restart.get("before_owner_epoch") != index
            or restart.get("after_owner_epoch") != index + 1
        ):
            raise WorkflowFailure(f"invalid {expected_phase} restart evidence")

    forks = evidence.get("forks")
    if not isinstance(forks, dict):
        raise WorkflowFailure("fork restore evidence lacks fork outcomes")
    prepare = forks.get("prepare_ack_loss")
    terminal = forks.get("terminal_ack_loss")
    concurrent = forks.get("concurrent")
    nested = forks.get("nested")
    for name, value in (
        ("prepare_ack_loss", prepare),
        ("terminal_ack_loss", terminal),
        ("concurrent", concurrent),
        ("nested", nested),
    ):
        if not isinstance(value, dict) or value.get("manifest_objects_added") != 1:
            raise WorkflowFailure(f"{name} did not add exactly one restore manifest")
        if value.get("payload_objects_overwritten") != 0:
            raise WorkflowFailure(f"{name} overwrote a source payload object")
    if not isinstance(terminal, dict) or terminal.get("retry_replayed") is not True:
        raise WorkflowFailure("terminal ACK-loss retry did not replay")
    if (
        not isinstance(concurrent, dict)
        or concurrent.get("calls") != CONCURRENT_RESTORES
        or concurrent.get("unique_operation_ids") != 1
        or concurrent.get("failures") != 0
        or not isinstance(concurrent.get("retry_attempts"), list)
        or len(concurrent["retry_attempts"]) != CONCURRENT_RESTORES
        or not all(
            isinstance(attempts, int) and attempts > 0
            for attempts in concurrent["retry_attempts"]
        )
    ):
        raise WorkflowFailure("concurrent exact restores did not converge")
    if (
        not isinstance(nested, dict)
        or nested.get("content_matches_parent") is not True
        or nested.get("source_commit_objects_added") != 1
    ):
        raise WorkflowFailure("nested fork did not preserve parent content")

    retention = evidence.get("retention")
    if (
        not isinstance(retention, dict)
        or retention.get("source_snapshot_retired") is not True
        or retention.get("source_diverged") is not True
        or retention.get("forks_preserved_snapshot") is not True
    ):
        raise WorkflowFailure("source retirement/divergence did not preserve fork content")


def execute(args: argparse.Namespace) -> dict[str, object]:
    repo = args.repo.resolve()
    evidence = Evidence(args.evidence_dir.resolve())
    evidence.prepare()
    if args.timeout <= 0:
        raise WorkflowFailure("--timeout must be positive")
    logical_bytes = fixture_logical_bytes(args.fixture_files)
    if args.require_full and args.fixture_files != FULL_PROFILE_FILES:
        raise WorkflowFailure("--require-full requires --fixture-files 256")
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
    shard_id = fixed_id(args.seed, "shard")
    prefix = f"/nokv/fork-restore-gate/{fixed_id(args.seed, 'control')}"
    object_root = f"fork-restore-gate/{fixed_id(args.seed, 'object')}"
    bucket = f"nokv-fork-gate-{fixed_id(args.seed, 'bucket')[:12]}"
    container = f"nokv-fork-gate-{os.getpid()}"
    volume = f"{container}-data"
    access_key, secret_key = "rustfsadmin", "rustfsadmin"
    etcd_port, peer_port = free_port(), free_port()
    s3_port, console_port, proxy_port = free_port(), free_port(), free_port()
    owner_ports = [free_port(), free_port(), free_port()]
    etcd_endpoint = f"http://127.0.0.1:{etcd_port}"
    s3_endpoint = f"http://127.0.0.1:{s3_port}"
    aws_env = aws_environment(access_key, secret_key)
    metadata = evidence.root / "metadata"
    fixture_path = evidence.root / "cow-block.bin"
    etcd_log = (evidence.root / "etcd.log").open("w", encoding="utf-8")
    owner_logs: list[TextIO] = [
        (evidence.root / f"owner-e{epoch}.log").open("w", encoding="utf-8")
        for epoch in (1, 2, 3)
    ]
    etcd: subprocess.Popen[str] | None = None
    owner: subprocess.Popen[str] | None = None
    proxy = ResponseDropProxy(proxy_port, max(args.timeout, 45.0))
    container_created = False
    volume_created = False
    started_at = now()
    restarts: list[dict[str, object]] = []

    try:
        peer = f"http://127.0.0.1:{peer_port}"
        etcd = start_process(
            [
                args.etcd_bin,
                "--name",
                "nokv-fork-restore-gate",
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
                f"nokv-fork-restore-gate={peer}",
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
        objects = object_args(s3_endpoint, bucket, object_root, access_key, secret_key)
        run(
            [*common, *objects, "provision", shard_id],
            cwd=repo,
            timeout=args.timeout,
            evidence=evidence,
            label="provision",
        )
        proxy.start()
        owner_key = f"{prefix}/logical-shards/{shard_id}"
        session_key = f"{prefix}/sessions/{shard_id}"

        def start_owner(epoch: int, metadata_option: str) -> tuple[subprocess.Popen[str], int]:
            bind_port = owner_ports[epoch - 1]
            proxy.set_target(bind_port)
            process = start_process(
                proxy_server_command(
                    common,
                    objects,
                    bind_port,
                    proxy_port,
                    f"fork-gate-e{epoch}",
                    metadata_option,
                    metadata,
                ),
                repo,
                owner_logs[epoch - 1],
            )
            wait_tcp(process, bind_port, args.timeout)
            control = parse_control_record(
                etcd_value(
                    Path(args.etcdctl_bin), etcd_endpoint, owner_key, repo, args.timeout
                ),
                f"owner e{epoch} control record",
            )
            if control.get("owner_epoch") != epoch or control.get("state") != 3:
                raise WorkflowFailure(f"owner e{epoch} did not reach Serving: {control!r}")
            return process, int(control["owner_epoch"])

        owner, owner_epoch = start_owner(1, "--metadata-create")
        client = [
            *common,
            "--max-attempts",
            "1",
            "--workbench-root",
            "/agents/materials/wb",
            *objects,
        ]

        source = "phymat-fork-source"
        structure, relaxation = phymat_documents()
        workbench_call(
            client,
            "workbench_create",
            {"id": source},
            repo,
            args.timeout,
            evidence,
            "source-create",
        )
        for section, path, value in (
            ("input", "structure.json", structure),
            ("outputs", "relaxation.json", relaxation),
        ):
            workbench_call(
                client,
                "workbench_put_file",
                {
                    "id": source,
                    "section": section,
                    "path": path,
                    "text": canonical_json(value) + "\n",
                    "content_type": "application/json",
                    "replace": False,
                },
                repo,
                args.timeout,
                evidence,
                f"source-put-{path}",
            )

        before_fixture = object_inventory(
            Path(args.aws_bin),
            s3_endpoint,
            bucket,
            object_root,
            aws_env,
            repo,
            args.timeout,
            evidence,
            "inventory-before-cow-fixture",
        )
        with fixture_path.open("wb") as fixture:
            fixture.truncate(BLOCK_BYTES)
        marker_digest = hashlib.sha256()
        for index in range(args.fixture_files):
            marker = fixture_block_marker(index)
            marker_digest.update(marker)
            with fixture_path.open("r+b") as fixture:
                fixture.write(marker)
            run(
                [
                    *client,
                    "collect",
                    source,
                    "outputs",
                    fixture_path,
                    f"cow/block-{index:04}.bin",
                    "--content-type",
                    "application/octet-stream",
                ],
                cwd=repo,
                timeout=max(args.timeout, 90.0),
                evidence=evidence,
                label=f"source-collect-{index:04}",
            )
        after_fixture = object_inventory(
            Path(args.aws_bin),
            s3_endpoint,
            bucket,
            object_root,
            aws_env,
            repo,
            args.timeout,
            evidence,
            "inventory-after-cow-fixture",
        )
        overwritten_fixture = {
            key
            for key in before_fixture.keys() & after_fixture.keys()
            if before_fixture[key] != after_fixture[key]
        }
        fixture_keys = after_fixture.keys() - before_fixture.keys()
        fixture_objects = [after_fixture[key] for key in fixture_keys]
        if (
            overwritten_fixture
            or len(fixture_keys) != args.fixture_files
            or any(size != BLOCK_BYTES for size, _ in fixture_objects)
            or len({etag for _, etag in fixture_objects}) != args.fixture_files
        ):
            raise WorkflowFailure(
                "COW fixture did not create one distinct immutable four-MiB object per file"
            )
        workbench_call(
            client,
            "workbench_commit",
            {
                "id": source,
                "manifest": {
                    "domain": "materials",
                    "fixture_files": args.fixture_files,
                    "logical_bytes": logical_bytes,
                },
                "content_digest_uri": "sha256:" + "ab" * 32,
                "replace": False,
            },
            repo,
            args.timeout,
            evidence,
            "source-commit",
        )
        snapshot_name = "fork-point"
        workbench_call(
            client,
            "workbench_snapshot",
            {"id": source, "name": snapshot_name, "ttl_days": 7},
            repo,
            args.timeout,
            evidence,
            "source-snapshot",
        )
        changed_relaxation = copy.deepcopy(relaxation)
        changed_relaxation["dft"]["energy_ev"] = -999.0
        workbench_call(
            client,
            "workbench_put_file",
            {
                "id": source,
                "section": "outputs",
                "path": "relaxation.json",
                "text": canonical_json(changed_relaxation) + "\n",
                "content_type": "application/json",
                "replace": True,
            },
            repo,
            args.timeout,
            evidence,
            "source-diverge-after-snapshot",
        )

        aws = Path(args.aws_bin)
        inventory = object_inventory(
            aws,
            s3_endpoint,
            bucket,
            object_root,
            aws_env,
            repo,
            args.timeout,
            evidence,
            "inventory-before-prepare-loss",
        )

        def kill_current_owner(process: subprocess.Popen[str]) -> Callable[[], None]:
            return lambda: os.killpg(process.pid, signal.SIGKILL)

        prepare_destination = "phymat-prepare-retry"
        prepare_request = {
            "id": source,
            "at_snapshot": snapshot_name,
            "destination_id": prepare_destination,
        }
        proxy.arm("prepare_restore", kill_current_owner(owner))
        failed_prepare = run(
            [
                *client,
                "workbench",
                "workbench_restore",
                canonical_json(prepare_request),
            ],
            cwd=repo,
            timeout=max(args.timeout, 90.0),
            check=False,
            evidence=evidence,
            label="prepare-response-loss",
        )
        proxy.wait_dropped()
        owner.wait(timeout=10)
        owner_exit = owner.returncode
        wait_session_absent(
            Path(args.etcdctl_bin), etcd_endpoint, session_key, repo, args.timeout
        )
        owner = None
        successor, next_epoch = start_owner(2, "--metadata-reopen")
        owner = successor
        prepared_retry = workbench_call(
            client,
            "workbench_restore",
            prepare_request,
            repo,
            max(args.timeout, 90.0),
            evidence,
            "prepare-exact-retry",
        )
        prepare_operation = validate_restore_outcome(prepared_retry, prepare_destination)
        after_prepare = object_inventory(
            aws,
            s3_endpoint,
            bucket,
            object_root,
            aws_env,
            repo,
            args.timeout,
            evidence,
            "inventory-after-prepare-retry",
        )
        prepare_manifest = manifest_only_delta(
            inventory,
            after_prepare,
            source=source,
            destination=prepare_destination,
            aws=aws,
            endpoint=s3_endpoint,
            bucket=bucket,
            environment=aws_env,
            repo=repo,
            timeout=args.timeout,
            evidence=evidence,
            label="prepare-ack-loss",
        )
        restarts.append(
            {
                "phase": "prepare_restore",
                "signal": "SIGKILL" if owner_exit == -signal.SIGKILL else str(owner_exit),
                "request_failed_before_retry": failed_prepare.returncode != 0,
                "before_owner_epoch": owner_epoch,
                "after_owner_epoch": next_epoch,
            }
        )
        owner_epoch = next_epoch
        inventory = after_prepare

        terminal_destination = "phymat-terminal-retry"
        terminal_request = {
            "id": source,
            "at_snapshot": snapshot_name,
            "destination_id": terminal_destination,
        }
        proxy.arm("finalize_restore", kill_current_owner(owner))
        failed_terminal = run(
            [
                *client,
                "workbench",
                "workbench_restore",
                canonical_json(terminal_request),
            ],
            cwd=repo,
            timeout=max(args.timeout, 90.0),
            check=False,
            evidence=evidence,
            label="terminal-response-loss",
        )
        proxy.wait_dropped()
        owner.wait(timeout=10)
        owner_exit = owner.returncode
        wait_session_absent(
            Path(args.etcdctl_bin), etcd_endpoint, session_key, repo, args.timeout
        )
        owner = None
        successor, next_epoch = start_owner(3, "--metadata-reopen")
        owner = successor
        terminal_retry = workbench_call(
            client,
            "workbench_restore",
            terminal_request,
            repo,
            max(args.timeout, 90.0),
            evidence,
            "terminal-exact-retry",
        )
        terminal_operation = validate_restore_outcome(terminal_retry, terminal_destination)
        after_terminal = object_inventory(
            aws,
            s3_endpoint,
            bucket,
            object_root,
            aws_env,
            repo,
            args.timeout,
            evidence,
            "inventory-after-terminal-retry",
        )
        terminal_manifest = manifest_only_delta(
            inventory,
            after_terminal,
            source=source,
            destination=terminal_destination,
            aws=aws,
            endpoint=s3_endpoint,
            bucket=bucket,
            environment=aws_env,
            repo=repo,
            timeout=args.timeout,
            evidence=evidence,
            label="terminal-ack-loss",
        )
        restarts.append(
            {
                "phase": "finalize_restore",
                "signal": "SIGKILL" if owner_exit == -signal.SIGKILL else str(owner_exit),
                "request_failed_before_retry": failed_terminal.returncode != 0,
                "before_owner_epoch": owner_epoch,
                "after_owner_epoch": next_epoch,
            }
        )
        owner_epoch = next_epoch
        inventory = after_terminal

        concurrent_destination = "phymat-concurrent-fork"
        concurrent_request = {
            "id": source,
            "at_snapshot": snapshot_name,
            "destination_id": concurrent_destination,
        }
        start_barrier = threading.Barrier(CONCURRENT_RESTORES)

        def concurrent_restore(index: int) -> tuple[dict[str, Any], int]:
            start_barrier.wait()
            deadline = time.monotonic() + max(args.timeout, 120.0)
            attempts = 0
            while time.monotonic() < deadline:
                attempts += 1
                completed = run(
                    [
                        *client,
                        "workbench",
                        "workbench_restore",
                        canonical_json(concurrent_request),
                    ],
                    cwd=repo,
                    timeout=max(args.timeout, 120.0),
                    check=False,
                    evidence=evidence,
                    label=(
                        f"concurrent-restore-{index:02}-attempt-{attempts:03}"
                    ),
                )
                if completed.returncode == 0:
                    try:
                        outcome = json.loads(completed.stdout)
                    except json.JSONDecodeError as error:
                        raise WorkflowFailure(
                            "concurrent restore returned invalid JSON"
                        ) from error
                    if not isinstance(outcome, dict):
                        raise WorkflowFailure(
                            "concurrent restore returned a non-object outcome"
                        )
                    return outcome, attempts
                error = decode_cli_error(completed.stderr)
                if not is_retryable_restore_race(error):
                    raise WorkflowFailure(
                        f"concurrent exact restore returned {error!r}"
                    )
                time.sleep(0.002)
            raise WorkflowFailure("concurrent exact restore retry deadline expired")

        failures: list[str] = []
        concurrent_outcomes: list[dict[str, Any]] = []
        concurrent_attempts: list[int] = []
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=CONCURRENT_RESTORES
        ) as executor:
            futures = [
                executor.submit(concurrent_restore, index)
                for index in range(CONCURRENT_RESTORES)
            ]
            for future in futures:
                try:
                    outcome, attempts = future.result()
                    concurrent_outcomes.append(outcome)
                    concurrent_attempts.append(attempts)
                except Exception as error:  # captured in durable evidence below
                    failures.append(str(error))
        operation_ids = {
            validate_restore_outcome(outcome, concurrent_destination)
            for outcome in concurrent_outcomes
        }
        after_concurrent = object_inventory(
            aws,
            s3_endpoint,
            bucket,
            object_root,
            aws_env,
            repo,
            args.timeout,
            evidence,
            "inventory-after-concurrent",
        )
        concurrent_manifest = manifest_only_delta(
            inventory,
            after_concurrent,
            source=source,
            destination=concurrent_destination,
            aws=aws,
            endpoint=s3_endpoint,
            bucket=bucket,
            environment=aws_env,
            repo=repo,
            timeout=args.timeout,
            evidence=evidence,
            label="concurrent-fork",
        )
        inventory = after_concurrent

        workbench_call(
            client,
            "workbench_commit",
            {
                "id": concurrent_destination,
                "manifest": {
                    "domain": "materials",
                    "fixture_files": args.fixture_files,
                    "logical_bytes": logical_bytes,
                    "fork_hop": 1,
                },
                "content_digest_uri": "sha256:" + "bc" * 32,
                "replace": False,
            },
            repo,
            args.timeout,
            evidence,
            "nested-source-commit",
        )
        after_nested_source_commit = object_inventory(
            aws,
            s3_endpoint,
            bucket,
            object_root,
            aws_env,
            repo,
            args.timeout,
            evidence,
            "inventory-after-nested-source-commit",
        )
        nested_source_overwrites = sorted(
            key
            for key in inventory.keys() & after_nested_source_commit.keys()
            if inventory[key] != after_nested_source_commit[key]
        )
        nested_source_commit_objects = sorted(
            after_nested_source_commit.keys() - inventory.keys()
        )
        if nested_source_overwrites or len(nested_source_commit_objects) != 1:
            raise WorkflowFailure(
                "recommitting the restored workbench changed unexpected objects: "
                f"overwritten={nested_source_overwrites}, "
                f"added={nested_source_commit_objects}"
            )
        inventory = after_nested_source_commit

        nested_snapshot = "nested-point"
        workbench_call(
            client,
            "workbench_snapshot",
            {"id": concurrent_destination, "name": nested_snapshot, "ttl_days": 7},
            repo,
            args.timeout,
            evidence,
            "nested-source-snapshot",
        )
        nested_destination = "phymat-nested-fork"
        nested_outcome = workbench_call(
            client,
            "workbench_restore",
            {
                "id": concurrent_destination,
                "at_snapshot": nested_snapshot,
                "destination_id": nested_destination,
            },
            repo,
            max(args.timeout, 120.0),
            evidence,
            "nested-restore",
        )
        nested_operation = validate_restore_outcome(nested_outcome, nested_destination)
        after_nested = object_inventory(
            aws,
            s3_endpoint,
            bucket,
            object_root,
            aws_env,
            repo,
            args.timeout,
            evidence,
            "inventory-after-nested",
        )
        nested_manifest = manifest_only_delta(
            inventory,
            after_nested,
            source=concurrent_destination,
            destination=nested_destination,
            aws=aws,
            endpoint=s3_endpoint,
            bucket=bucket,
            environment=aws_env,
            repo=repo,
            timeout=args.timeout,
            evidence=evidence,
            label="nested-fork",
        )

        retired = workbench_call(
            client,
            "workbench_snapshot_retire",
            {"id": source, "name": snapshot_name, "reason": "fork acceptance complete"},
            repo,
            args.timeout,
            evidence,
            "source-snapshot-retire",
        )
        source_live = read_json_document(
            client,
            source,
            "outputs",
            "relaxation.json",
            repo,
            args.timeout,
            evidence,
            "read-diverged-source",
        )
        fork_documents = {
            destination: read_json_document(
                client,
                destination,
                "outputs",
                "relaxation.json",
                repo,
                args.timeout,
                evidence,
                f"read-{destination}",
            )
            for destination in (
                prepare_destination,
                terminal_destination,
                concurrent_destination,
                nested_destination,
            )
        }
        nested_matches = fork_documents[nested_destination] == fork_documents[concurrent_destination]
        forks_preserved = all(document == relaxation for document in fork_documents.values())

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
            "fixture": {
                "files": args.fixture_files,
                "block_bytes": BLOCK_BYTES,
                "logical_bytes": logical_bytes,
                "full_profile": args.fixture_files == FULL_PROFILE_FILES,
                "object_count": len(fixture_keys),
                "object_bytes": sum(size for size, _ in fixture_objects),
                "distinct_etags": len({etag for _, etag in fixture_objects}),
                "marker_digest": marker_digest.hexdigest(),
            },
            "restarts": restarts,
            "forks": {
                "prepare_ack_loss": {
                    "operation_id": prepare_operation,
                    "manifest_object": prepare_manifest,
                    "manifest_objects_added": 1,
                    "payload_objects_overwritten": 0,
                },
                "terminal_ack_loss": {
                    "operation_id": terminal_operation,
                    "manifest_object": terminal_manifest,
                    "manifest_objects_added": 1,
                    "payload_objects_overwritten": 0,
                    "retry_replayed": terminal_retry.get("idempotent_replay") is True,
                },
                "concurrent": {
                    "calls": CONCURRENT_RESTORES,
                    "failures": len(failures),
                    "failure_messages": failures,
                    "retry_attempts": concurrent_attempts,
                    "unique_operation_ids": len(operation_ids),
                    "manifest_object": concurrent_manifest,
                    "manifest_objects_added": 1,
                    "payload_objects_overwritten": 0,
                },
                "nested": {
                    "operation_id": nested_operation,
                    "manifest_object": nested_manifest,
                    "manifest_objects_added": 1,
                    "payload_objects_overwritten": 0,
                    "content_matches_parent": nested_matches,
                    "source_commit_object": nested_source_commit_objects[0],
                    "source_commit_objects_added": 1,
                },
            },
            "retention": {
                "source_snapshot_retired": retired.get("state") == "retired",
                "source_diverged": source_live == changed_relaxation,
                "forks_preserved_snapshot": forks_preserved,
            },
            "owner_epoch": owner_epoch,
        }
        validate_gate_evidence(result)
        return result
    finally:
        active_error = sys.exc_info()[0] is not None
        stop_process(owner)
        stop_process(etcd)
        try:
            proxy.close()
        except WorkflowFailure:
            if not active_error:
                raise
        for log in [*owner_logs, etcd_log]:
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
    parser.add_argument("--seed", default="path-native-fork-restore-v1")
    parser.add_argument("--fixture-files", type=int, default=64)
    parser.add_argument("--require-full", action="store_true")
    parser.add_argument("--timeout", type=float, default=60.0)
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
    except Exception as error:
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
