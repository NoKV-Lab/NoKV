#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0
"""Bounded append product acceptance over real CLI, etcd, RustFS and Holt.

Safety, completion and performance are independent qualifications. Running every
scenario in this file does not qualify the complete NoKV workspace or cross-host
recovery. Binary provenance is independent from the current source checkout.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import contextlib
import dataclasses
import hashlib
import http.client
import http.server
import json
import os
from pathlib import Path
import platform
import shutil
import socket
import subprocess
import sys
import threading
import time
import traceback

import append_identity_recovery_gate as base


class ContractViolation(RuntimeError):
    """A live, observed product outcome violated an explicit oracle."""


def require(condition, message, evidence=None):
    # These checks must remain active under python -O.
    if not condition:
        raise ContractViolation(f"{message}: {evidence!r}")


class PublicationProxy(base.EvidenceDropProxy):
    """Metadata proxy with an exact pre-Begin rendezvous and observed ACK loss."""

    def __init__(self, port, timeout, stack):
        super().__init__(port, timeout, stack)
        self.rendezvous = None
        self.rendezvous_entries = []

    def begin_rendezvous(self, count):
        require(self.rendezvous is None, "only one admission rendezvous")
        self.rendezvous = threading.Barrier(count)
        self.rendezvous_entries = []

    def _forward(self, client):
        operation = None
        try:
            client.settimeout(self.timeout)
            with self._lock:
                target = self._target
            with socket.create_connection(target, timeout=self.timeout) as owner:
                owner.settimeout(self.timeout)
                while True:
                    first = client.recv(4)
                    if not first:
                        return
                    header = first + base._recv_exact(client, 4 - len(first))
                    request = base._recv_exact(client, int.from_bytes(header, "big"))
                    req = None
                    if not base.is_handshake_frame(request):
                        req = base.unpack(request)
                        operation = req["payload"]["operation"]["operation"]
                        barrier = None
                        with self._lock:
                            if (
                                operation == "begin_artifact_publish"
                                and self.rendezvous is not None
                            ):
                                if (
                                    len(self.rendezvous_entries)
                                    < self.rendezvous.parties
                                ):
                                    barrier = self.rendezvous
                                    self.rendezvous_entries.append(req)
                        if barrier is not None:
                            self.stack.record(
                                "admission-rendezvous.jsonl",
                                {
                                    "at": base.live.now(),
                                    "request": req,
                                    "forwarded_before_rendezvous": False,
                                },
                            )
                            barrier.wait(timeout=self.timeout)
                    owner.sendall(header + request)
                    rh = base._recv_exact(owner, 4)
                    response = base._recv_exact(owner, int.from_bytes(rh, "big"))
                    callback = None
                    if req is None:
                        self.handshake_request = request
                    else:
                        rsp = base.unpack(response)
                        with self._lock:
                            self.wire_schema = req["schema"]
                            self.latest_route = req["payload"]["route"]
                        self.stack.record(
                            "publication-rpc-trace.jsonl",
                            {
                                "at": base.live.now(),
                                "operation": operation,
                                "request": req,
                                "response": rsp,
                            },
                        )
                        if rsp["payload"]["outcome"]["status"] == "success":
                            with self._lock:
                                if self._drop_operation == operation:
                                    callback, self._on_drop = self._on_drop, None
                                    self._drop_operation = None
                            if callback is not None:
                                self.last_drop = {
                                    "at": base.live.now(),
                                    "operation": operation,
                                    "request": req,
                                    "response": rsp,
                                    "request_hex": request.hex(),
                                    "response_hex": response.hex(),
                                    "delivered_to_client": False,
                                    "owner_pid": self.stack.owner.pid,
                                }
                                self.stack.record("dropped-rpc.jsonl", self.last_drop)
                    if callback is not None:
                        callback()
                        self._dropped.set()
                        return
                    client.sendall(rh + response)
        except (ConnectionError, OSError, TimeoutError) as error:
            # Closing a killed caller/owner connection is the selected fault.
            self.stack.record(
                "proxy-connections.jsonl",
                {"at": base.live.now(), "operation": operation, "closure": repr(error)},
            )
        except Exception as error:
            self._errors.append(repr(error))
            self._dropped.set()
        finally:
            client.close()


class ObjectProxy:
    """HTTP transport faults after real provider persistence, without changing SigV4."""

    def __init__(self, stack, port, upstream_port):
        self.stack, self.port, self.upstream_port = stack, port, upstream_port
        self.lock = threading.Lock()
        self.rule = None
        self.hold_put = False
        self.held = threading.Event()
        self.release = threading.Event()
        self.released_response = threading.Event()
        self.held_request = None
        self.reject_matching = None
        self.rejected_requests = []
        self.fired = []
        self.errors = []
        outer = self

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *_args):
                pass

            def forward(self):
                try:
                    if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
                        chunks = bytearray()
                        while True:
                            line = self.rfile.readline()
                            chunks.extend(line)
                            length = int(line.split(b";", 1)[0], 16)
                            if length:
                                chunks.extend(self.rfile.read(length + 2))
                            else:
                                while True:
                                    trailer = self.rfile.readline()
                                    chunks.extend(trailer)
                                    if trailer == b"\r\n":
                                        break
                                break
                        payload = bytes(chunks)
                    else:
                        payload = self.rfile.read(
                            int(self.headers.get("Content-Length", "0"))
                        )
                    with outer.lock:
                        rejected = (
                            outer.reject_matching is not None
                            and self.command == outer.reject_matching["method"]
                            and outer.reject_matching["path_contains"] in self.path
                        )
                    if rejected:
                        event = {
                            "at": base.live.now(),
                            "method": self.command,
                            "path": self.path,
                            "injected_status": 503,
                            "forwarded_to_provider": False,
                        }
                        with outer.lock:
                            outer.rejected_requests.append(event)
                        outer.stack.record("object-http-transcript.jsonl", event)
                        body = b"<Error><Code>ServiceUnavailable</Code><Message>Injected temporary admission outage</Message></Error>"
                        self.send_response(503)
                        self.send_header("Content-Type", "application/xml")
                        self.send_header("Content-Length", str(len(body)))
                        self.send_header("Connection", "close")
                        self.end_headers()
                        self.wfile.write(body)
                        self.close_connection = True
                        return
                    held = False
                    with outer.lock:
                        if (
                            outer.hold_put
                            and self.command == "PUT"
                            and "/nokv/artifacts/" in self.path
                        ):
                            held, outer.hold_put = True, False
                            outer.held_request = {
                                "at": base.live.now(),
                                "method": self.command,
                                "path": self.path,
                                "request_bytes": len(payload),
                                "request_sha256": hashlib.sha256(payload).hexdigest(),
                                "if_none_match": self.headers.get("If-None-Match"),
                                "if_match": self.headers.get("If-Match"),
                                "upstream_sent": False,
                            }
                    if held:
                        outer.stack.record(
                            "held-object-put.jsonl", dict(outer.held_request)
                        )
                        outer.held.set()
                        require(
                            outer.release.wait(2 * outer.stack.timeout),
                            "held provider PUT was released within the gate deadline",
                        )
                    with contextlib.closing(
                        http.client.HTTPConnection(
                            "127.0.0.1",
                            outer.upstream_port,
                            timeout=outer.stack.timeout,
                        )
                    ) as upstream:
                        upstream.putrequest(
                            self.command,
                            self.path,
                            skip_host=True,
                            skip_accept_encoding=True,
                        )
                        for key, value in self.headers.items():
                            if key.lower() != "connection":
                                upstream.putheader(key, value)
                        upstream.putheader("Connection", "close")
                        upstream.endheaders(payload)
                        response = upstream.getresponse()
                        body = response.read()
                        rule = None
                        with outer.lock:
                            if (
                                outer.rule
                                and self.command == outer.rule["method"]
                                and "/nokv/artifacts/" in self.path
                                and 200 <= response.status < 300
                                and (
                                    outer.rule["action"] != "seal_ack_loss_head_failure"
                                    or (
                                        self.headers.get("Content-Length") == "0"
                                        and self.headers.get("If-Match") is not None
                                    )
                                )
                            ):
                                rule, outer.rule = outer.rule, None
                                if rule["action"] == "seal_ack_loss_head_failure":
                                    outer.reject_matching = {
                                        "method": "HEAD",
                                        "path_contains": self.path,
                                    }
                        event = {
                            "at": base.live.now(),
                            "method": self.command,
                            "path": self.path,
                            "upstream_status": response.status,
                            "request_bytes": len(payload),
                            "request_sha256": hashlib.sha256(payload).hexdigest(),
                            "response_bytes": len(body),
                            "if_none_match": self.headers.get("If-None-Match"),
                            "if_match": self.headers.get("If-Match"),
                            "fault": rule["action"] if rule else None,
                        }
                        outer.stack.record("object-http-transcript.jsonl", event)
                        if held:
                            outer.held_request["released_upstream_response"] = event
                            outer.stack.record(
                                "held-object-put.jsonl",
                                {"at": base.live.now(), "upstream_sent": True, **event},
                            )
                            outer.released_response.set()
                        if rule:
                            with outer.lock:
                                outer.fired.append(event)
                            if rule["action"] in (
                                "drop_success",
                                "seal_ack_loss_head_failure",
                            ):
                                self.close_connection = True
                                self.connection.shutdown(socket.SHUT_RDWR)
                                return
                            if rule["action"] == "corrupt_body":
                                require(
                                    bool(body),
                                    "corruption needs a nonempty provider read",
                                )
                                body = bytes([body[0] ^ 1]) + body[1:]
                            elif rule["action"] == "read_503":
                                body = b"<Error><Code>ServiceUnavailable</Code></Error>"
                        status = (
                            503
                            if rule and rule["action"] == "read_503"
                            else response.status
                        )
                        self.send_response(status)
                        for key, value in response.getheaders():
                            if key.lower() not in (
                                "transfer-encoding",
                                "content-length",
                                "connection",
                            ):
                                self.send_header(key, value)
                        self.send_header(
                            "Content-Length",
                            response.getheader("Content-Length", "0")
                            if self.command == "HEAD"
                            else str(len(body)),
                        )
                        self.send_header("Connection", "close")
                        self.end_headers()
                        if self.command != "HEAD":
                            self.wfile.write(body)
                        self.close_connection = True
                except (BrokenPipeError, ConnectionResetError):
                    self.close_connection = True
                except (ConnectionRefusedError, TimeoutError) as error:
                    outer.stack.record(
                        "object-http-transcript.jsonl",
                        {
                            "at": base.live.now(),
                            "method": self.command,
                            "path": self.path,
                            "upstream_transport_error": type(error).__name__,
                        },
                    )
                    self.close_connection = True
                except Exception as error:
                    outer.errors.append(repr(error))
                    self.close_connection = True

            do_GET = do_PUT = do_HEAD = do_DELETE = do_POST = forward

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", port), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def start(self):
        self.thread.start()

    def arm(self, method, action):
        with self.lock:
            require(self.rule is None, "object fault already armed")
            self.rule = {"method": method, "action": action}
            return len(self.fired)

    def verify_fault(self, before):
        require(
            len(self.fired) == before + 1 and self.rule is None,
            "object fault actually reached successful provider response",
            self.fired,
        )
        require(not self.errors, "object proxy errors", self.errors)
        return self.fired[-1]

    def hold_next_put(self):
        with self.lock:
            require(not self.hold_put, "provider PUT already held")
            self.hold_put = True
            self.held.clear()
            self.release.clear()
            self.released_response.clear()
            self.held_request = None

    def close(self):
        self.release.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)


class ProductStack(base.IsolatedStack):
    def __init__(self, *args, publication_lease_ms=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.proxy = PublicationProxy(self.proxy_port, self.timeout, self)
        self.callers = {}
        self.append_timings = []
        self.publication_lease_ms = publication_lease_ms
        self.object_proxy = ObjectProxy(self, base.infra.free_port(), self.s3_port)
        self.object_endpoint = f"http://127.0.0.1:{self.object_proxy.port}"
        self.config = dataclasses.replace(
            self.config, object_endpoint=self.object_endpoint
        )

    def __enter__(self):
        self.object_proxy.start()
        return super().__enter__()

    def start(self, label, command):
        command = list(command)
        if label.startswith("owner-") and self.publication_lease_ms is not None:
            command[-1:-1] = [
                "--append-activity-lease-ms",
                str(self.publication_lease_ms),
            ]
        return super().start(label, command)

    def run(
        self,
        command,
        *,
        label="command",
        check=True,
        env=None,
        timeout=None,
        input_text=None,
    ):
        command = list(map(str, command))
        started = base.live.now()
        process = subprocess.Popen(
            command,
            cwd=self.source,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.PIPE if input_text is not None else None,
            env=env,
        )
        self.processes.append(process)
        if "workspace-path" in command and "append" in command:
            with self.lock:
                self.callers[label] = process
        timed_out = False
        try:
            stdout, stderr = process.communicate(
                input_text, timeout=timeout or self.timeout
            )
        except subprocess.TimeoutExpired:
            timed_out = True
            process.kill()
            stdout, stderr = process.communicate()
        result = subprocess.CompletedProcess(
            command, process.returncode, stdout, stderr
        )
        redacted = [
            value.split("=")[0] + "=<redacted>"
            if value.startswith(("RUSTFS_ACCESS_KEY=", "RUSTFS_SECRET_KEY="))
            else value
            for value in base.live.redact_argv(command)
        ]
        self.record(
            "processes.jsonl",
            {
                "label": label,
                "pid": process.pid,
                "argv": redacted,
                "started_at": started,
                "ended_at": base.live.now(),
                "returncode": result.returncode,
                "timeout": timed_out,
                "stdout": stdout,
                "stderr": stderr,
            },
        )
        if timed_out:
            raise TimeoutError(f"{label} timed out with pid {process.pid}")
        if check and result.returncode:
            raise RuntimeError(f"{label}: {stderr or stdout}")
        return result

    def kill_caller(self, label):
        with self.lock:
            process = self.callers.get(label)
        require(
            process is not None and process.poll() is None,
            "fault targets a live caller",
            label,
        )
        self.record(
            "caller-faults.jsonl",
            {
                "at": base.live.now(),
                "caller_pid": process.pid,
                "label": label,
                "signal": "SIGKILL",
                "owner_pid": self.owner.pid,
                "owner_alive": self.owner.poll() is None,
            },
        )
        process.kill()

    def close(self):
        if self.closed:
            return
        try:
            super().close()
        finally:
            self.object_proxy.close()


def append(
    stack,
    workbench,
    operation_id,
    payload,
    *,
    label,
    path="effects.log",
    content_type="text/plain",
    max_size=16 * 1024 * 1024,
    incarnation=None,
    config=None,
    block_size=None,
):
    payload = payload.encode() if isinstance(payload, str) else payload
    fixture = stack.evidence_dir / (label + ".input")
    fixture.write_bytes(payload)
    command = [
        *base.live.client_args(config or stack.config),
        "workspace-path",
        "append",
        workbench,
        "logs",
        path,
        "--operation-id",
        operation_id,
        "--file",
        str(fixture),
    ]
    if content_type is not None:
        command += ["--content-type", content_type]
    if max_size is not None:
        command += ["--max-logical-size", str(max_size)]
    if incarnation is not None:
        command += ["--expected-workspace-incarnation-id", incarnation]
    if block_size is not None:
        command += ["--block-size", str(block_size)]
    started = time.monotonic()
    process = stack.run(command, label=label, check=False)
    elapsed = time.monotonic() - started
    if process.returncode == -9:
        result = {"status": "caller_killed", "operation_id": operation_id}
    else:
        raw = (
            process.stdout
            if process.returncode == 0
            else process.stderr.strip().removeprefix("nokv: ")
        )
        try:
            result = json.loads(raw)
        except json.JSONDecodeError as error:
            raise ContractViolation(
                f"append returned no structured result: {raw}"
            ) from error
        require(
            (process.returncode == 0) == (result.get("status") == "success"),
            "append exit/result agree",
            result,
        )
    timing = {
        "label": label,
        "operation_id": operation_id,
        "wall_seconds": elapsed,
        "payload_bytes": len(payload),
        "returncode": process.returncode,
        "status": result.get("status"),
        "code": result.get("code"),
        "cause_code": result.get("details", {}).get("cause_code"),
    }
    with stack.lock:
        stack.append_timings.append(timing)
    stack.record(
        "product-append-transcript.jsonl",
        {
            "at": base.live.now(),
            **timing,
            "workbench": workbench,
            "path": path,
            "payload_sha256": hashlib.sha256(payload).hexdigest(),
            "result": result,
        },
    )
    return result


def case_result(*, safety="NOT QUALIFIED", completion="NOT QUALIFIED", **evidence):
    return {
        "safety": safety,
        "completion": completion,
        "performance": "NOT QUALIFIED",
        **evidence,
    }


def record_machine_profile(stack):
    profile = {
        "os": platform.platform(),
        "machine": platform.machine(),
        "logical_cpu_count": os.cpu_count(),
        "python_version": sys.version,
        "python_optimization": sys.flags.optimize,
        "append_activity_lease_ms": stack.publication_lease_ms,
        "rustfs_image_digest": base.PINNED_RUSTFS_IMAGE,
        "provider_endpoint_class": "loopback HTTP to an isolated digest-pinned RustFS container through an evidence proxy",
        "bucket_policy_scope": "Fresh isolated bucket created by this runner; no external provider qualification is inferred.",
    }
    commands = {
        "observed_rust_toolchain_not_build_proof": ["rustc", "--version", "--verbose"],
        "etcd_version": [stack.etcd_binary, "--version"],
        "aws_cli_version": [stack.aws, "--version"],
        "filesystem_capacity": ["df", "-k", str(stack.evidence_dir)],
    }
    if sys.platform == "darwin":
        commands["hardware_model_cpu_memory"] = [
            "sysctl",
            "-n",
            "hw.model",
            "hw.logicalcpu",
            "hw.memsize",
        ]
    for label, command in commands.items():
        if shutil.which(str(command[0])) is None:
            profile[label] = {
                "status": "NOT QUALIFIED",
                "reason": "executable unavailable",
            }
            continue
        result = stack.run(command, label="environment-" + label, check=False)
        profile[label] = {
            "returncode": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
        }
    stack.json("machine-profile.json", profile)
    return "machine-profile.json"


def canonical_receipt(value):
    fields = (
        "operation_id",
        "publication_operation_id",
        "artifact_revision_id",
        "workbench_id",
        "path",
        "workspace_incarnation_id",
        "workspace_revision",
        "generation",
        "logical_size",
        "body_digest",
    )
    require(
        all(field in value for field in fields),
        "complete logical/publication receipt",
        value,
    )
    return {field: value[field] for field in fields}


def validate_parent_child_mapping(stack, operation_id, status, receipt):
    require(
        status.get("operation_id") == operation_id
        and status.get("state") == "committed",
        "logical parent is committed",
        status,
    )
    attempt = status.get("attempt")
    require(
        isinstance(attempt, int) and attempt >= 0,
        "status declares a nonnegative durable attempt",
        status,
    )
    suffix = (
        bytes.fromhex(stack.root_id)
        + bytes.fromhex(operation_id)
        + attempt.to_bytes(8, "big")
    )
    child = hashlib.sha256(b"nokv.append.publication.v2\0" + suffix).hexdigest()[:32]
    revision = hashlib.sha256(b"nokv.append.revision.v2\0" + suffix).hexdigest()[:32]
    require(
        status.get("publication_operation_id")
        == receipt["publication_operation_id"]
        == child,
        "root/logical-ID/attempt derive the committed child",
        status,
    )
    require(
        receipt["artifact_revision_id"] == revision
        and receipt["operation_id"] == operation_id,
        "receipt binds logical identity to derived revision",
        receipt,
    )
    require(
        canonical_receipt(status["receipt"]) == canonical_receipt(receipt),
        "parent stores complete original receipt",
        status,
    )


def operation_status(stack, operation_id, label, *, config=None):
    cfg = config or stack.config
    command = [
        str(stack.binary),
        *base.live.control_args(cfg),
        "--agent-id",
        cfg.agent_id,
        "operation",
        "status",
        operation_id,
    ]
    response = stack.run(command, label=label, check=False)
    raw = (
        response.stdout
        if response.returncode == 0
        else response.stderr.strip().removeprefix("nokv: ")
    )
    try:
        result = json.loads(raw)
    except json.JSONDecodeError as error:
        raise ContractViolation(
            f"operation status returned no structured JSON: {raw}"
        ) from error
    require(
        (response.returncode == 0) == (result.get("status") == "success"),
        "status exit/result agree",
        result,
    )
    stack.record(
        "product-status-transcript.jsonl",
        {
            "at": base.live.now(),
            "operation_id": operation_id,
            "label": label,
            "result": result,
            "object_configuration_supplied": False,
        },
    )
    return result


def live_path(stack, workbench, label, path="effects.log"):
    # The raw read is supplementary evidence through the same public owner RPC;
    # the mutation itself and final byte read always use the native product CLI.
    result = base.raw_rpc(
        stack,
        {
            "operation": "get_path",
            "request": {
                "target": {"workbench": workbench, "path": "logs/" + path},
                "view": "live",
                "expected_read_version": None,
                "range": None,
                "plan_page": None,
                "if_none_match": None,
            },
        },
        label,
    )
    require(result.get("status") == "success", "independent live path query", result)
    body = result["body"]
    # Remove read-time metadata; canonical path/revision/generation remain.
    return body


def inventory(stack, label):
    from fork_restore_recovery_gate import object_inventory

    result = object_inventory(
        stack.aws,
        stack.object_endpoint,
        stack.bucket,
        stack.object_root,
        stack.aws_env,
        stack.source,
        stack.timeout,
        stack.evidence,
        label,
    )
    return {key: value for key, value in result.items() if "/nokv/artifacts/" in key}


def replay_unchanged(
    stack,
    workbench,
    operation_id,
    payload,
    original,
    label,
    *,
    repetitions=2,
    **options,
):
    validate_parent_child_mapping(
        stack,
        operation_id,
        operation_status(stack, operation_id, label + "-parent-status"),
        original,
    )
    before = live_path(stack, workbench, label + "-live-before")
    objects_before = inventory(stack, label + "-objects-before")
    outcomes = []
    for index in range(repetitions):
        result = append(
            stack,
            workbench,
            operation_id,
            payload,
            label=f"{label}-{index:04d}",
            **options,
        )
        require(
            result.get("status") == "success" and result.get("replayed") is True,
            "same-ID replay succeeds",
            result,
        )
        require(
            canonical_receipt(result) == canonical_receipt(original),
            "complete historical receipt unchanged",
            (result, original),
        )
        outcomes.append(result)
    after = live_path(stack, workbench, label + "-live-after")
    require(
        after == before,
        "replay does not change live revision, generation or path metadata",
        (before, after),
    )
    require(
        inventory(stack, label + "-objects-after") == objects_before,
        "replay creates no payload object",
    )
    return {
        "repetitions": repetitions,
        "live_path_unchanged": True,
        "payload_inventory_unchanged": True,
        "receipt": canonical_receipt(outcomes[-1]),
    }


def caller_boundary_completion(stack, deadline, operation):
    suffix = (
        operation.removesuffix("_artifact_publish")
        .removeprefix("stage_artifact_")
        .removeprefix("mark_artifact_")
    )
    workbench = "product-caller-" + suffix.replace("_", "-")
    operation_id = hashlib.sha256(workbench.encode()).hexdigest()[:32]
    text = "caller-recovery-event\n"
    label = "caller-kill-" + suffix
    stack.cli("workbench_create", {"id": workbench})
    owner_pid = stack.owner.pid
    stack.proxy.arm(operation, lambda: stack.kill_caller(label))
    first = append(
        stack, workbench, operation_id, text, label=label, max_size=1024 * 1024
    )
    stack.proxy.wait_dropped()
    require(first.get("status") == "caller_killed", "caller was actually killed", first)
    require(
        stack.owner.pid == owner_pid and stack.owner.poll() is None,
        "same healthy owner survived caller death",
    )
    admitted = operation_status(stack, operation_id, label + "-status-after-kill")
    require(
        admitted.get("status") == "success",
        "logical identity admitted before caller death",
        admitted,
    )
    recovered, outcomes = recover_same_identity(
        stack,
        workbench,
        operation_id,
        text,
        label=label + "-recovery",
        deadline=deadline,
        observed_status=admitted,
    )
    if recovered is None:
        return case_result(
            safety="NOT QUALIFIED",
            completion="FAIL",
            failure="Caller died after a durable publication boundary and the same logical request did not complete within the declared deadline.",
            boundary=operation,
            owner_survived=True,
            status_after_kill=admitted,
            retry_outcomes=outcomes,
        )
    exact_body(stack, workbench, text.encode(), label + "-bytes")
    replay = replay_unchanged(
        stack,
        workbench,
        operation_id,
        text,
        recovered,
        label + "-replay",
        max_size=1024 * 1024,
    )
    status = operation_status(stack, operation_id, label + "-terminal-status")
    require(
        status.get("state") == "committed" and status.get("next_action") == "none",
        "terminal status is actionable",
        status,
    )
    require(
        canonical_receipt(status["receipt"]) == canonical_receipt(recovered),
        "status original receipt",
        status,
    )
    return case_result(
        safety="PASS",
        completion="PASS",
        boundary=operation,
        caller_sigkill=True,
        same_owner_pid=owner_pid,
        status_after_kill=admitted,
        terminal_status=status,
        replay=replay,
        retry_outcomes=outcomes,
    )


def deterministic_same_identity(stack, deadline):
    workbench, operation_id, text = (
        "product-admission-race",
        "c2" * 16,
        "one-concurrent-event\n",
    )
    stack.cli("workbench_create", {"id": workbench})
    stack.proxy.begin_rendezvous(2)
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            futures = [
                pool.submit(
                    append,
                    stack,
                    workbench,
                    operation_id,
                    text,
                    label=f"admission-racer-{index}",
                    max_size=1024 * 1024,
                )
                for index in range(2)
            ]
            first = [future.result() for future in futures]
        entries = stack.proxy.rendezvous_entries
        require(
            len(entries) == 2,
            "two Begin requests were held before either was forwarded",
            entries,
        )
    finally:
        stack.proxy.rendezvous = None
    recovered, outcomes = recover_same_identity(
        stack,
        workbench,
        operation_id,
        text,
        label="admission-reconcile",
        deadline=deadline,
    )
    require(recovered is not None, "concurrent same identity completes", outcomes)
    for result in first:
        if result.get("status") == "success":
            require(
                canonical_receipt(result) == canonical_receipt(recovered),
                "all acknowledged results conserved",
                first,
            )
        else:
            require(
                result.get("details", {}).get("operation_id") == operation_id
                and result.get("code") == "AppendUnresolved",
                "admission race error is typed and keeps identity",
                result,
            )
    require(
        recovered["generation"] == 1, "exactly one publication generation", recovered
    )
    exact_body(stack, workbench, text.encode(), "admission-exact-body")
    replay = replay_unchanged(
        stack,
        workbench,
        operation_id,
        text,
        recovered,
        "admission-replay",
        max_size=1024 * 1024,
    )
    intent_race = different_intent_admission(stack)
    return case_result(
        safety="PASS",
        completion="PASS",
        requests_held_before_forward=2,
        initial_results=first,
        retry_outcomes=outcomes,
        replay=replay,
        different_intent_race=intent_race,
    )


def different_intent_admission(stack):
    workbench, operation_id = "product-admission-intent-race", "cf" * 16
    payloads = (b"intent-A\n", b"intent-B\n")
    stack.cli("workbench_create", {"id": workbench})
    stack.proxy.begin_rendezvous(2)
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            futures = [
                pool.submit(
                    append,
                    stack,
                    workbench,
                    operation_id,
                    payload,
                    label=f"intent-racer-{index}",
                )
                for index, payload in enumerate(payloads)
            ]
            results = [future.result() for future in futures]
        require(
            len(stack.proxy.rendezvous_entries) == 2,
            "two distinct intents reached Begin before either was forwarded",
        )
    finally:
        stack.proxy.rendezvous = None
    successful = [
        index
        for index, result in enumerate(results)
        if result.get("status") == "success"
    ]
    require(
        len(successful) == 1,
        "healthy concurrent different intents have exactly one successful logical winner",
        results,
    )
    winner_index = successful[0]
    winner, loser = results[winner_index], results[1 - winner_index]
    require(
        loser.get("code") == "AppendUnresolved"
        and loser.get("details", {}).get("code") == "RequestReplayMismatch"
        and loser.get("details", {}).get("operation_id") == operation_id,
        "losing intent has a precise replay-mismatch error",
        loser,
    )
    exact_body(stack, workbench, payloads[winner_index], "intent-race-exact-winner")
    replay = replay_unchanged(
        stack,
        workbench,
        operation_id,
        payloads[winner_index],
        winner,
        "intent-race-winner-replay",
    )
    return {
        "initial_results": results,
        "winner_index": winner_index,
        "exact_expected_bytes_hex": payloads[winner_index].hex(),
        "replay": replay,
    }


def object_put_response_loss(stack, deadline):
    workbench, operation_id, payload = (
        "product-object-put-loss",
        "c3" * 16,
        "provider-persisted-event\n",
    )
    stack.cli("workbench_create", {"id": workbench})
    before = stack.object_proxy.arm("PUT", "drop_success")
    first = append(
        stack,
        workbench,
        operation_id,
        payload,
        label="put-loss-first",
        max_size=1024 * 1024,
    )
    fault = stack.object_proxy.verify_fault(before)
    recovered, outcomes = recover_same_identity(
        stack,
        workbench,
        operation_id,
        payload,
        label="put-loss-recover",
        deadline=deadline,
    )
    require(
        recovered is not None,
        "same identity completes after actual successful PUT response loss",
        outcomes,
    )
    if first.get("status") == "success":
        require(
            canonical_receipt(first) == canonical_receipt(recovered),
            "PUT retry preserves acknowledged receipt",
            first,
        )
    exact_body(stack, workbench, payload.encode(), "put-loss-exact-body")
    replay = replay_unchanged(
        stack,
        workbench,
        operation_id,
        payload,
        recovered,
        "put-loss-replay",
        max_size=1024 * 1024,
    )
    return case_result(
        safety="PASS",
        completion="PASS",
        fault=fault,
        first_call=first,
        retry_outcomes=outcomes,
        replay=replay,
    )


def object_read_integrity(stack, deadline):
    workbench, seed_id, append_id = "product-object-read-fault", "c4" * 16, "c5" * 16
    stack.cli("workbench_create", {"id": workbench})
    seed = append(stack, workbench, seed_id, "seed\n", label="read-fault-seed")
    require(seed.get("status") == "success", "read-fault seed", seed)
    before_live = live_path(stack, workbench, "read-fault-live-before")
    before = stack.object_proxy.arm("GET", "corrupt_body")
    result = append(
        stack,
        workbench,
        append_id,
        "tail\n",
        label="read-fault-first",
        max_size=1024 * 1024,
    )
    fault = stack.object_proxy.verify_fault(before)
    require(
        result.get("status") == "error",
        "corrupt provider bytes cannot be published",
        result,
    )
    require(
        live_path(stack, workbench, "read-fault-live-rejected") == before_live,
        "corruption leaves live path unchanged",
    )
    recovered, outcomes = recover_same_identity(
        stack,
        workbench,
        append_id,
        "tail\n",
        label="read-fault-recover",
        deadline=deadline,
    )
    require(
        recovered is not None,
        "same identity works after a pre-admission read fault clears",
        outcomes,
    )
    exact_body(stack, workbench, b"seed\ntail\n", "read-fault-final")
    return case_result(
        safety="PASS",
        completion="PASS",
        fault=fault,
        first_call=result,
        retry_outcomes=outcomes,
    )


def distinct_identity_exact_bytes(stack, deadline):
    workbench, count = "product-distinct-race", 4
    stack.cli("workbench_create", {"id": workbench})
    ids = [
        hashlib.sha256(f"distinct-{index}".encode()).hexdigest()[:32]
        for index in range(count)
    ]
    payloads = [f"event-{index:02d}\n" for index in range(count)]
    barrier = threading.Barrier(count)
    workload_started = time.monotonic()

    def call(index):
        barrier.wait(timeout=stack.timeout)
        return append(
            stack,
            workbench,
            ids[index],
            payloads[index],
            label=f"distinct-initial-{index}",
            max_size=1024 * 1024,
        )

    with concurrent.futures.ThreadPoolExecutor(max_workers=count) as pool:
        initial = list(pool.map(call, range(count)))
    receipts = []
    attempt_indexes = {}
    for index in range(count):
        result, outcomes = recover_same_identity(
            stack,
            workbench,
            ids[index],
            payloads[index],
            label=f"distinct-reconcile-{index}",
            deadline=deadline,
        )
        require(
            result is not None, "each distinct request eventually completes", outcomes
        )
        if initial[index].get("status") == "success":
            require(
                canonical_receipt(initial[index]) == canonical_receipt(result),
                "initial acknowledgement conserved",
                (initial[index], result),
            )
        receipts.append(result)
        status = operation_status(
            stack, ids[index], f"distinct-terminal-attempt-{index}"
        )
        validate_parent_child_mapping(stack, ids[index], status, result)
        attempt_indexes[ids[index]] = status["attempt"]
    workload_seconds = time.monotonic() - workload_started
    generations = [receipt["generation"] for receipt in receipts]
    require(
        sorted(generations) == list(range(1, count + 1)),
        "one contiguous generation per logical append",
        receipts,
    )
    ordered = sorted(zip(receipts, payloads), key=lambda pair: pair[0]["generation"])
    expected = "".join(payload for _, payload in ordered).encode()
    exact_body(stack, workbench, expected, "distinct-complete-byte-oracle")
    live_before = live_path(stack, workbench, "distinct-live-before-replay")
    for index in range(count):
        result = append(
            stack,
            workbench,
            ids[index],
            payloads[index],
            label=f"distinct-history-{index}",
            max_size=1024 * 1024,
        )
        require(
            canonical_receipt(result) == canonical_receipt(receipts[index]),
            "distinct historical receipts preserved",
            result,
        )
    require(
        live_path(stack, workbench, "distinct-live-after-replay") == live_before,
        "historical replays preserve final live generation",
    )
    stack.restart()
    exact_body(stack, workbench, expected, "distinct-reopened-byte-oracle")
    timings = [
        value
        for value in stack.append_timings
        if value["label"].startswith(("distinct-initial-", "distinct-reconcile-"))
    ]
    ordered_latencies = sorted(value["wall_seconds"] for value in timings)

    def percentile(fraction):
        position = fraction * (len(ordered_latencies) - 1)
        low = int(position)
        high = min(low + 1, len(ordered_latencies) - 1)
        return ordered_latencies[low] + (
            ordered_latencies[high] - ordered_latencies[low]
        ) * (position - low)

    measurement = {
        "qualification": "Measured diagnostic sample, no performance SLA is asserted.",
        "process_model": "Fresh native CLI per call; running shared services; cache state not controlled.",
        "initial_concurrency": count,
        "logical_operations": count,
        "application_calls": len(timings),
        "error_calls": sum(value["status"] != "success" for value in timings),
        "calls_per_identity": {
            op: sum(value["operation_id"] == op for value in timings) for op in ids
        },
        "last_durable_attempt_index": attempt_indexes,
        "payload_bytes": [len(payload.encode()) for payload in payloads],
        "workload_seconds_including_terminal_status_queries": workload_seconds,
        "logical_operations_per_second": count / workload_seconds,
        "latency_seconds": {
            "p50": percentile(0.50),
            "p95": percentile(0.95),
            "p99": percentile(0.99),
            "max": max(ordered_latencies),
        },
        "raw_calls": timings,
    }
    return case_result(
        safety="PASS",
        completion="PASS",
        initial_results=initial,
        receipts=receipts,
        exact_expected_utf8=expected.decode(),
        owner_reopen_verified=True,
        performance_measurement=measurement,
    )


def cross_root_identity(stack):
    workbench, operation_id = "product-cross-root", "d1" * 16
    root_b = hashlib.sha256((stack.root_id + "second-root").encode()).hexdigest()[:32]
    config_b = dataclasses.replace(stack.config, root_id=root_b)
    stack.run(base.live.provision_command(config_b), label="provision-second-root")
    stack.restart()
    stack.cli("workbench_create", {"id": workbench})
    stack.cli("workbench_create", {"id": workbench}, config=config_b)
    first = append(stack, workbench, operation_id, b"root-A\n", label="root-a-append")
    second = append(
        stack,
        workbench,
        operation_id,
        b"root-B\n",
        label="root-b-append",
        config=config_b,
    )
    require(
        first.get("status") == second.get("status") == "success",
        "same caller ID independently admitted in two roots",
        (first, second),
    )
    require(
        first["publication_operation_id"] != second["publication_operation_id"]
        and first["artifact_revision_id"] != second["artifact_revision_id"],
        "root binds child and revision identities",
        (first, second),
    )
    status_a = operation_status(stack, operation_id, "root-a-status")
    status_b = operation_status(stack, operation_id, "root-b-status", config=config_b)
    require(
        canonical_receipt(status_a["receipt"]) == canonical_receipt(first)
        and canonical_receipt(status_b["receipt"]) == canonical_receipt(second),
        "status cannot expose another root's receipt",
    )
    exact_body(stack, workbench, b"root-A\n", "root-a-final")
    target = stack.evidence_dir / "root-b-final.data"
    stack.run(
        [
            *base.live.client_args(config_b),
            "materialize",
            workbench,
            "logs",
            "effects.log",
            str(target),
        ],
        label="root-b-final",
    )
    require(target.read_bytes() == b"root-B\n", "root B exact independent content")
    return case_result(
        safety="PASS",
        completion="PASS",
        root_a=stack.root_id,
        root_b=root_b,
        receipt_a=first,
        receipt_b=second,
    )


def boundary_and_capacity(stack):
    block = 4 * 1024 * 1024
    outcomes = []
    for index, size in enumerate((0, 1, block - 1, block, block + 1)):
        workbench = f"product-size-{size}"
        operation_id = hashlib.sha256(workbench.encode()).hexdigest()[:32]
        stack.cli("workbench_create", {"id": workbench})
        payload = (b"\0\xffboundary\n" * (size // 11 + 1))[:size]
        require(len(payload) == size, "boundary fixture exact size")
        result = append(
            stack,
            workbench,
            operation_id,
            payload,
            label=f"size-{size}",
            content_type="application/octet-stream",
            max_size=None,
        )
        require(
            result.get("status") == "success"
            and result.get("logical_size") == size
            and result.get("generation") == 1,
            "zero/block-boundary append",
            result,
        )
        exact_body(stack, workbench, payload, f"size-{size}-body")
        replay = replay_unchanged(
            stack,
            workbench,
            operation_id,
            payload,
            result,
            f"size-{size}-replay",
            repetitions=1,
            content_type="application/octet-stream",
            max_size=16 * 1024 * 1024,
        )
        outcomes.append(
            {"size": size, "receipt": result, "default_explicit_limit_replay": replay}
        )
        if size == 1:
            empty_id = "d0" * 16
            empty = append(
                stack,
                workbench,
                empty_id,
                b"",
                label="existing-zero-delta",
                content_type="application/octet-stream",
                max_size=None,
            )
            require(
                empty.get("status") == "success"
                and empty["generation"] == 2
                and empty["logical_size"] == 1,
                "new identity with empty delta publishes one new generation",
                empty,
            )
            replay_unchanged(
                stack,
                workbench,
                empty_id,
                b"",
                empty,
                "existing-zero-replay",
                repetitions=1,
                content_type="application/octet-stream",
                max_size=None,
            )
            exact_body(stack, workbench, payload, "existing-zero-unchanged-bytes")
    workbench = "product-body-limit"
    stack.cli("workbench_create", {"id": workbench})
    limit = 16 * 1024 * 1024
    full = b"x" * limit
    first = append(
        stack, workbench, "d2" * 16, full, label="body-limit-exact", max_size=None
    )
    require(
        first.get("status") == "success" and first["logical_size"] == limit,
        "default exact body limit admitted",
        first,
    )
    before = live_path(stack, workbench, "body-limit-live-before")
    objects_before = inventory(stack, "body-limit-inventory-before")
    over = append(
        stack, workbench, "d3" * 16, b"+", label="body-limit-plus-one", max_size=None
    )
    require(
        over.get("status") == "error"
        and over.get("code") in ("InvalidArgument", "AppendUnresolved"),
        "body limit rejected with typed error",
        over,
    )
    oversized = append(
        stack,
        workbench,
        "d4" * 16,
        b"y" * (limit + 1),
        label="delta-limit-plus-one",
        max_size=64 * 1024 * 1024,
    )
    require(
        oversized.get("status") == "error"
        and oversized.get("code") == "InvalidArgument",
        "oversized delta rejected before admission",
        oversized,
    )
    for op in ("d3" * 16, "d4" * 16):
        status = operation_status(stack, op, "capacity-unadmitted-" + op[:2])
        require(
            status.get("status") == "error" and status.get("code") == "NotFound",
            "capacity rejects have no durable operation",
            status,
        )
    require(
        live_path(stack, workbench, "body-limit-live-after-rejects") == before,
        "capacity rejection preserves live metadata",
    )
    require(
        inventory(stack, "body-limit-inventory-after") == objects_before,
        "capacity rejection writes no payload objects",
    )
    raised = append(
        stack,
        workbench,
        "d5" * 16,
        b"+",
        label="body-limit-explicit-increase",
        max_size=32 * 1024 * 1024,
    )
    require(
        raised.get("status") == "success" and raised["logical_size"] == limit + 1,
        "explicit larger body bound allows qualified delta",
        raised,
    )
    exact_body(stack, workbench, full + b"+", "body-limit-raised-content")
    changed_bound = append(
        stack,
        workbench,
        "d5" * 16,
        b"+",
        label="body-limit-intent-mismatch",
        max_size=64 * 1024 * 1024,
    )
    require(
        changed_bound.get("details", {}).get("code") == "RequestReplayMismatch",
        "admitted explicit body bound is immutable intent",
        changed_bound,
    )
    return case_result(
        safety="PASS",
        completion="PASS",
        boundary_results=outcomes,
        delta_limit_bytes=limit,
        body_limit_default_bytes=limit,
        default_overflow=over,
        oversized_delta=oversized,
        explicit_increase=raised,
        changed_bound=changed_bound,
    )


def publication_batch_boundaries(stack):
    block_size = 65536
    outcomes = []
    for blocks in (191, 192, 193):
        workbench = f"product-batch-{blocks}"
        operation_id = hashlib.sha256(workbench.encode()).hexdigest()[:32]
        stack.cli("workbench_create", {"id": workbench})
        payload = b"".join(
            index.to_bytes(4, "big") * (block_size // 4) for index in range(blocks)
        )
        result = append(
            stack,
            workbench,
            operation_id,
            payload,
            label=f"batch-{blocks}-original",
            content_type="application/octet-stream",
            block_size=block_size,
        )
        require(
            result.get("status") == "success"
            and result.get("logical_size") == blocks * block_size,
            "bounded batch-spanning append succeeds",
            result,
        )
        exact_body(stack, workbench, payload, f"batch-{blocks}-readback")
        replay = replay_unchanged(
            stack,
            workbench,
            operation_id,
            payload,
            result,
            f"batch-{blocks}-replay",
            repetitions=1,
            content_type="application/octet-stream",
            block_size=block_size,
        )
        outcomes.append(
            {
                "blocks": blocks,
                "block_size": block_size,
                "receipt": result,
                "replay": replay,
            }
        )
    return case_result(
        safety="PASS", completion="PASS", max_publication_batch_rows=192, rows=outcomes
    )


def completed_replay_provider_unavailable(stack):
    workbench, operation_id, payload = (
        "product-replay-provider-unavailable",
        "e8" * 16,
        b"durable-before-outage\n",
    )
    stack.cli("workbench_create", {"id": workbench})
    original = append(
        stack, workbench, operation_id, payload, label="provider-outage-original"
    )
    require(original.get("status") == "success", "provider outage original", original)
    before = live_path(stack, workbench, "provider-outage-live-before")
    unavailable = dataclasses.replace(
        stack.config, object_endpoint=f"http://127.0.0.1:{base.infra.free_port()}"
    )
    replay = append(
        stack,
        workbench,
        operation_id,
        payload,
        label="provider-unavailable-replay",
        config=unavailable,
    )
    require(
        replay.get("status") == "success"
        and replay.get("replayed") is True
        and canonical_receipt(replay) == canonical_receipt(original),
        "completed append replay requires only metadata despite unreachable S3 endpoint",
        replay,
    )
    (stack.evidence_dir / "provider-outage-original.input").unlink()
    status = operation_status(
        stack, operation_id, "provider-unavailable-status-without-payload"
    )
    require(
        canonical_receipt(status["receipt"]) == canonical_receipt(original),
        "ID-only status works after payload file deletion",
    )
    require(
        live_path(stack, workbench, "provider-outage-live-after") == before,
        "offline-provider replay does not mutate live metadata",
    )
    return case_result(
        safety="PASS",
        completion="PASS",
        original_receipt=original,
        replay_receipt=replay,
        status_without_payload=status,
        object_endpoint_unreachable=True,
    )


def late_put_after_cleanup(stack, deadline):
    workbench, operation_id, payload = (
        "product-late-put",
        "fc" * 16,
        b"one-late-provider-event\n",
    )
    label = "late-put-caller"
    stack.cli("workbench_create", {"id": workbench})
    owner_pid = stack.owner.pid
    stack.object_proxy.hold_next_put()
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(
                append,
                stack,
                workbench,
                operation_id,
                payload,
                label=label,
                max_size=1024 * 1024,
            )
            require(
                stack.object_proxy.held.wait(stack.timeout),
                "actual complete artifact PUT received before forwarding",
            )
            held = stack.object_proxy.held_request
            stack.kill_caller(label)
            require(
                future.result().get("status") == "caller_killed",
                "caller SIGKILL while actual PUT is held",
            )
        require(
            stack.owner.pid == owner_pid and stack.owner.poll() is None,
            "owner survives held-PUT caller death",
        )
        until = time.monotonic() + deadline
        observations = []
        while True:
            status = operation_status(
                stack, operation_id, f"late-put-cleanup-status-{len(observations):03d}"
            )
            observations.append(status)
            if status.get("next_action") == "resubmit_same":
                break
            if time.monotonic() >= until:
                return case_result(
                    completion="FAIL",
                    failure="Held old PUT did not reach a clean recoverable predecessor before the declared deadline.",
                    status_observations=observations,
                    held_request=held,
                )
            time.sleep(0.5)
        require(
            str(status.get("attempt_phase")).lower() == "cleaned",
            "previous attempt durably cleaned before old PUT is sent",
            status,
        )
        previous_child = status["publication_operation_id"]
        object_key = held["path"].removeprefix("/" + stack.bucket + "/")
        before_release = inventory(stack, "late-put-before-release-inventory")
        guard_before = (
            object_key in before_release and before_release[object_key][0] == 0
        )
        require(
            held.get("if_none_match") == "*",
            "actual late immutable PUT carries If-None-Match *",
            held,
        )
        stack.object_proxy.release.set()
        require(
            stack.object_proxy.released_response.wait(stack.timeout),
            "old PUT reached the real provider after cleanup",
        )
        late_status = held["released_upstream_response"]["upstream_status"]
        require(
            200 <= late_status < 300 or late_status == 412,
            "held PUT reached real conditional provider decision",
            held,
        )
        recovered, outcomes = recover_same_identity(
            stack,
            workbench,
            operation_id,
            payload,
            label="late-put-logical-recovery",
            deadline=deadline,
            observed_status=status,
        )
        require(
            recovered is not None,
            "same logical append completes after old caller was killed",
            outcomes,
        )
        require(
            recovered["publication_operation_id"] != previous_child,
            "completed append uses a new child after previous cleanup",
            recovered,
        )
        exact_body(stack, workbench, payload, "late-put-exact-one-effect")
        # Observe multiple real lifecycle ticks after the new child committed.
        time.sleep(2)
        final_objects = inventory(stack, "late-put-final-inventory")
        late_object_present = object_key in final_objects
        guard_after = late_object_present and final_objects[object_key][0] == 0
        safe = guard_before and guard_after and late_status == 412
        result = case_result(
            safety="PASS" if safe else "FAIL",
            completion="PASS",
            logical_effect_safety="PASS",
            object_lifetime_safety="PASS" if safe else "FAIL",
            same_owner_pid=owner_pid,
            status_before_release=status,
            previous_publication_operation_id=previous_child,
            held_request=held,
            late_object_key=object_key,
            zero_guard_before_release=guard_before,
            zero_guard_after_release=guard_after,
            late_provider_status=late_status,
            late_object_present_after_cleanup=late_object_present,
            old_object_size_after_cleanup=final_objects.get(object_key, [None])[0],
            terminal_receipt=recovered,
            status_observations=observations,
            provider_request_lifetime_bound="No finite request-completion lifetime is assumed; this gate deliberately delays a fully received PUT until after metadata cleanup. Cleanup must keep an empty immutable-key guard and the late If-None-Match PUT must receive 412.",
        )
        if not safe:
            result["failure"] = (
                "Cleanup did not retain a zero-byte immutable-key guard that rejects the late old PUT; one logical effect does not qualify the provider object lifecycle."
            )
        return result
    finally:
        stack.object_proxy.release.set()


def seal_quarantine_operator_recovery(stack, deadline):
    """Resolve one real provider ambiguity using an inspected child token."""
    workbench, operation_id = "product-seal-quarantine", "fd" * 16
    payload, label = b"operator-recovered-event\n", "seal-quarantine-caller"
    stack.cli("workbench_create", {"id": workbench})
    owner_pid = stack.owner.pid
    stack.object_proxy.rule = {"method": "PUT", "action": "seal_ack_loss_head_failure"}
    stack.proxy.arm("mark_artifact_objects_uploaded", lambda: stack.kill_caller(label))
    try:
        first = append(
            stack,
            workbench,
            operation_id,
            payload,
            label=label,
            max_size=1024 * 1024,
            block_size=65536,
        )
        stack.proxy.wait_dropped()
        require(first.get("status") == "caller_killed", "uploaded caller killed", first)
        require(
            stack.owner.pid == owner_pid and stack.owner.poll() is None,
            "owner survives the uploaded caller",
        )
        until, observations = time.monotonic() + deadline, []
        while True:
            status = operation_status(
                stack, operation_id, f"seal-quarantine-poll-{len(observations):03d}"
            )
            observations.append(status)
            if status.get("state") == "quarantined":
                break
            require(
                time.monotonic() < until,
                "seal ambiguity durably quarantines",
                observations,
            )
            time.sleep(0.5)
        require(
            status.get("next_action") == "operator_reconcile"
            and status.get("receipt") is None
            and status.get("cause_code"),
            "quarantine is actionable and has no success receipt",
            status,
        )
        child = status["publication_operation_id"]
        faults = [
            event
            for event in stack.object_proxy.fired
            if event.get("fault") == "seal_ack_loss_head_failure"
        ]
        require(
            len(faults) == 1
            and faults[0]["request_bytes"] == 0
            and faults[0]["if_match"]
            and 200 <= faults[0]["upstream_status"] < 300,
            "a conditional empty seal really persisted before its ACK was lost",
            faults,
        )
        require(
            any(
                event["method"] == "HEAD" and event["path"] == faults[0]["path"]
                for event in stack.object_proxy.rejected_requests
            ),
            "post-seal HEAD really failed",
        )
        query = {
            "operation": "get_operation",
            "request": {"operation_id": list(bytes.fromhex(child))},
        }
        inspected = base.raw_rpc(stack, query, "quarantine-inspected-child")
        require(
            inspected.get("status") == "success"
            and inspected["body"]["value"]["state"] == "quarantined",
            "exact quarantined publication child is inspectable",
            inspected,
        )
        token = inspected["body"]["value"]["token"]
        retry = append(
            stack,
            workbench,
            operation_id,
            payload,
            label="quarantined-same-id-cannot-advance",
            max_size=1024 * 1024,
            block_size=65536,
        )
        require(
            retry.get("status") == "error" and retry.get("code") == "AppendUnresolved",
            "quarantined append cannot silently create a successor",
            retry,
        )
        after_retry = operation_status(
            stack, operation_id, "quarantine-parent-after-retry"
        )
        require(
            after_retry.get("state") == "quarantined"
            and after_retry.get("publication_operation_id") == child
            and after_retry.get("attempt") == status["attempt"],
            "same logical identity remains on the quarantined child",
            after_retry,
        )

        # This controlled fault uses the acknowledged public staging transcript
        # as its complete ledger. Status does not expose a staged-key listing API.
        rows, counts = {}, set()
        for line in (
            (stack.evidence_dir / "publication-rpc-trace.jsonl")
            .read_text()
            .splitlines()
        ):
            event = json.loads(line)
            if event["response"]["payload"]["outcome"]["status"] != "success":
                continue
            request = event["request"]["payload"]["operation"]["request"]
            if (
                event["operation"] == "begin_artifact_publish"
                and bytes(request["operation_id"]).hex() == child
            ):
                counts.add(request["staged_object_count"])
            if (
                event["operation"] == "stage_artifact_objects"
                and bytes(request["token"]["operation_id"]).hex() == child
            ):
                for row in request["objects"]:
                    require(
                        row["sequence"] not in rows or rows[row["sequence"]] == row,
                        "acknowledged staged sequence has one immutable identity",
                        row,
                    )
                    rows[row["sequence"]] = row
        require(
            counts == {1} and set(rows) == {0},
            "acknowledged Begin and staging prove the complete single-key ledger",
            (counts, rows),
        )
        stack.json(
            "operator-acknowledged-staged-ledger.json",
            {
                "child": child,
                "rows": list(rows.values()),
                "source": "Successful Begin and StageArtifactObjects public RPC transcript; controlled one-key fixture.",
            },
        )
        with stack.object_proxy.lock:
            stack.object_proxy.reject_matching = None
        verified = []
        for sequence, row in sorted(rows.items()):
            key = stack.object_root.rstrip("/") + "/" + row["object_identity"]
            output = stack.evidence_dir / f"operator-sealed-key-{sequence}.data"
            read = stack.run(
                base.infra.aws_command(
                    stack.aws,
                    stack.object_endpoint,
                    "s3api",
                    "get-object",
                    "--bucket",
                    stack.bucket,
                    "--key",
                    key,
                    str(output),
                ),
                label=f"operator-provider-read-{sequence}",
                env=stack.aws_env,
            )
            metadata = json.loads(read.stdout)
            require(
                output.read_bytes() == b"" and metadata["ContentLength"] == 0,
                "every acknowledged staged key is permanently sealed at the real provider",
                metadata,
            )
            verified.append(
                {
                    "sequence": sequence,
                    "key": key,
                    "size": 0,
                    "etag": metadata["ETag"],
                    "sha256": hashlib.sha256(b"").hexdigest(),
                }
            )
        evidence = json.dumps(
            {"child": child, "token": token, "verified_keys": verified}, sort_keys=True
        ).encode()
        (stack.evidence_dir / "operator-provider-verification.json").write_bytes(
            evidence
        )
        verdict = {
            "operation": "reconcile_quarantined_artifact_publish",
            "request": {
                "token": token,
                "resolution": "provider_objects_absent",
                "reason": "Controlled acceptance verifies the complete acknowledged key ledger as permanently sealed.",
                "evidence_digest": list(hashlib.sha256(evidence).digest()),
            },
        }
        wrong = base.raw_rpc(stack, verdict, "operator-obsolete-absence-verdict")
        require(
            wrong.get("status") == "failure"
            and wrong.get("body", {}).get("code") == "precondition_failed"
            and wrong.get("body", {}).get("conflict") == "operation_state"
            and wrong.get("body", {}).get("retryable") is False,
            "obsolete absence verdict is precisely rejected for append",
            wrong,
        )
        unchanged = base.raw_rpc(stack, query, "operator-child-after-wrong-verdict")
        require(
            unchanged == inspected,
            "rejected verdict does not change inspected child/token",
            unchanged,
        )
        verdict["request"]["resolution"] = "provider_objects_sealed"
        resolved = base.raw_rpc(stack, verdict, "operator-exact-sealed-verdict")
        require(
            resolved.get("status") == "success",
            "exact inspected child token accepts sealed proof",
            resolved,
        )
        ready = operation_status(stack, operation_id, "operator-parent-ready")
        require(
            ready.get("state") == "ready_to_retry"
            and ready.get("next_action") == "resubmit_same",
            "operator reconciliation makes the original logical ID resumable",
            ready,
        )
        # Keep identical block geometry and bound in the immutable append intent.
        recovered = append(
            stack,
            workbench,
            operation_id,
            payload,
            label="operator-same-id-completion",
            max_size=1024 * 1024,
            block_size=65536,
        )
        require(
            recovered.get("status") == "success"
            and recovered["publication_operation_id"] != child,
            "same logical ID completes through its next child",
            recovered,
        )
        exact_body(stack, workbench, payload, "operator-exact-one-effect")
        final_inventory = inventory(stack, "operator-final-inventory")
        require(
            all(final_inventory.get(row["key"], [None])[0] == 0 for row in verified),
            "operator recovery retains all old zero guards",
            final_inventory,
        )
        replay = replay_unchanged(
            stack,
            workbench,
            operation_id,
            payload,
            recovered,
            "operator-completed-replay",
            max_size=1024 * 1024,
            block_size=65536,
        )
        return case_result(
            safety="PASS",
            completion="PASS",
            caller_sigkill=True,
            same_owner_pid=owner_pid,
            quarantine_status=status,
            fault=faults[0],
            status_observations=observations,
            inspected_child=inspected,
            verified_provider_keys=verified,
            obsolete_verdict=wrong,
            sealed_verdict=resolved,
            terminal_receipt=recovered,
            replay=replay,
        )
    finally:
        with stack.object_proxy.lock:
            stack.object_proxy.reject_matching = None
            stack.object_proxy.rule = None


def identity_errors_and_status(stack):
    workbench, operation_id, payload = (
        "product-intent-status",
        "e1" * 16,
        b"status-seed\n",
    )
    stack.cli("workbench_create", {"id": workbench})
    original = append(
        stack,
        workbench,
        operation_id,
        payload,
        label="intent-original",
        content_type="application/x-events",
    )
    require(original.get("status") == "success", "intent seed", original)
    before = live_path(stack, workbench, "intent-live-before")
    rejects = []
    changes = (
        ("payload", {"payload": b"changed\n"}),
        ("path", {"path": "other.log"}),
        ("type", {"content_type": "text/plain"}),
        ("limit", {"max_size": 32 * 1024 * 1024}),
        ("block_size", {"block_size": 65536}),
        ("incarnation", {"incarnation": "ff" * 16}),
    )
    for name, changed in changes:
        options = {
            "payload": payload,
            "content_type": "application/x-events",
            **changed,
        }
        result = append(
            stack, workbench, operation_id, label="intent-mismatch-" + name, **options
        )
        details = result.get("details", {})
        expected_cause = (
            "Conflict" if name == "incarnation" else "RequestReplayMismatch"
        )
        require(
            result.get("status") == "error"
            and result.get("code") == "AppendUnresolved"
            and details.get("code") == expected_cause
            and details.get("operation_id") == operation_id,
            "altered intent has exact mismatch cause",
            result,
        )
        if name == "incarnation":
            require(
                details.get("conflict") == "WorkspaceIncarnation",
                "explicit incarnation is a typed fence conflict",
                result,
            )
        rejects.append({"changed": name, "result": result})
    require(
        live_path(stack, workbench, "intent-live-after-rejects") == before,
        "rejections do not mutate live metadata",
    )
    unknown = operation_status(stack, "ef" * 16, "status-unknown")
    require(
        unknown.get("status") == "error" and unknown.get("code") == "NotFound",
        "unknown identity has typed status",
        unknown,
    )
    current = operation_status(stack, operation_id, "status-existing")
    require(
        current.get("state") == "committed" and current.get("next_action") == "none",
        "committed status has actionable state",
        current,
    )
    require(
        canonical_receipt(current["receipt"]) == canonical_receipt(original),
        "status returns complete original receipt",
    )
    query = {
        "operation": "get_operation",
        "request": {"operation_id": list(bytes.fromhex(operation_id))},
    }
    before_collision = base.raw_rpc(stack, query, "append-family-before-collision")
    require(
        before_collision.get("status") == "success"
        and before_collision["body"]["value"]["kind"] == "artifact_append",
        "logical ID resolves to append family",
        before_collision,
    )
    collision = base.raw_rpc(
        stack,
        {
            "operation": "commit",
            "request": {
                "operation_id": list(bytes.fromhex(operation_id)),
                "workbench": workbench,
                "workspace_incarnation_id": list(
                    bytes.fromhex(original["workspace_incarnation_id"])
                ),
                "commit_id": [0xA1] * 32,
                "content_digest": "sha256:" + "a2" * 32,
                "manifest_digest": "sha256:" + "a3" * 32,
                "projection_input_digest": [0xA4] * 32,
                "tree_manifest_revision_id": [0xA5] * 16,
                "replace": False,
                "run_manifest_condition": "create_only",
                "expected_head_generation": None,
                "parents": [],
                "producer": None,
                "lineage_projection": [],
            },
        },
        "commit-reuses-logical-append-id",
    )
    require(
        collision.get("status") == "failure"
        and collision.get("body", {}).get("code")
        in ("conflict", "request_replay_mismatch"),
        "another operation family cannot admit the logical append ID",
        collision,
    )
    after_collision = base.raw_rpc(stack, query, "append-family-after-collision")
    require(
        after_collision.get("status") == "success"
        and after_collision["body"]["value"]["kind"] == "artifact_append"
        and after_collision["body"]["value"]["result"]
        == before_collision["body"]["value"]["result"],
        "cross-family rejection leaves original append queryable with the same durable result",
        after_collision,
    )
    require(
        live_path(stack, workbench, "append-family-live-after-collision") == before,
        "cross-family reuse causes no path metadata mutation",
    )
    tail = append(
        stack,
        workbench,
        "e2" * 16,
        b"tail\n",
        label="status-later-writer",
        content_type=None,
    )
    require(tail.get("status") == "success", "later independent append", tail)
    replay = replay_unchanged(
        stack,
        workbench,
        operation_id,
        payload,
        original,
        "status-historical-replay",
        content_type="application/x-events",
    )
    live = live_path(stack, workbench, "status-current-type")
    require(
        live["value"]["metadata"]["descriptor"]["content_type"]
        == "application/x-events",
        "omitted type inherits live type",
        live,
    )
    # Deleting the live path must not delete the historical operation receipt.
    stack.run(
        [
            *stack.client_args,
            "workspace-path",
            "remove",
            workbench,
            "logs",
            "effects.log",
            "--expected-generation",
            str(tail["generation"]),
            "--request-id",
            "e3" * 16,
        ],
        label="status-remove-current",
    )
    after_remove = operation_status(stack, operation_id, "status-after-path-remove")
    require(
        canonical_receipt(after_remove["receipt"]) == canonical_receipt(original),
        "status survives current path removal",
    )
    removed_replay = append(
        stack,
        workbench,
        operation_id,
        payload,
        label="status-removed-replay",
        content_type="application/x-events",
    )
    require(
        canonical_receipt(removed_replay) == canonical_receipt(original),
        "append replay after deletion returns old receipt",
    )
    exact_body(stack, workbench, None, "status-path-stays-absent")
    return case_result(
        safety="PASS",
        completion="PASS",
        mismatches=rejects,
        unknown_status=unknown,
        original_status=current,
        cross_family_rejection=collision,
        later_writer=tail,
        replay=replay,
        status_after_remove=after_remove,
    )


def repeated_identity_retention(stack, repetitions):
    workbench, operation_id, payload = (
        "product-replay-retention",
        "e4" * 16,
        b"retained-event\n",
    )
    stack.cli("workbench_create", {"id": workbench})
    original = append(
        stack, workbench, operation_id, payload, label="retention-original"
    )
    require(original.get("status") == "success", "retention original", original)
    replay = replay_unchanged(
        stack,
        workbench,
        operation_id,
        payload,
        original,
        "retention-replay",
        repetitions=repetitions,
    )
    stack.restart()
    reopened = replay_unchanged(
        stack,
        workbench,
        operation_id,
        payload,
        original,
        "retention-after-reopen",
        repetitions=2,
    )
    exact_body(stack, workbench, payload, "retention-body")
    return case_result(
        safety="PASS",
        completion="PASS",
        observed_replays=repetitions + 2,
        owner_reopen=True,
        replay=replay,
        reopened=reopened,
        retention_horizon="Only this run's elapsed time is observed; advertised long-term retention and high-cardinality storage growth remain NOT QUALIFIED.",
    )


PYTHON_STATUS_PROGRAM = r"""
import hashlib, json, sys
from pathlib import Path
import nokv
from nokv import _native
config = json.load(sys.stdin)
client = nokv.Client(config["root_id"], nokv.RoutingConfig.etcd([config["etcd_endpoint"]], key_prefix=config["etcd_prefix"]), object_store=None, workbench_root=config["workbench_root"])
try:
    result = client.operation_status(config["operation_id"])
    print(json.dumps({"result": result, "native_sha256": hashlib.sha256(Path(_native.__file__).read_bytes()).hexdigest(), "python_version": sys.version, "object_store": None}))
except Exception as error:
    print(json.dumps({"error": type(error).__name__, "message": str(error)}))
"""

PYTHON_APPEND_PROGRAM = r"""
import hashlib, json, sys
from pathlib import Path
import nokv
from nokv import _native
config = json.load(sys.stdin)
objects = None if config["metadata_only"] else nokv.ObjectStoreConfig.s3(config["bucket"], root=config["object_root"], endpoint=config["object_endpoint"], access_key_id=config["access_key"], secret_access_key=config["secret_key"])
client = nokv.Client(config["root_id"], nokv.RoutingConfig.etcd([config["etcd_endpoint"]], key_prefix=config["etcd_prefix"]), object_store=objects, workbench_root=config["workbench_root"])
try:
    result = client.append_bytes(config["workbench"], "logs/effects.log", bytes.fromhex(config["payload_hex"]), config["operation_id"], content_type=config["content_type"], block_size=config["block_size"], max_logical_size=config["max_logical_size"])
    print(json.dumps({"status":"success", "result":result, "object_store_configured":objects is not None, "native_sha256":hashlib.sha256(Path(_native.__file__).read_bytes()).hexdigest()}))
except Exception as error:
    print(json.dumps({"status":"error", "type":type(error).__name__, "message":str(error), **{field:getattr(error, field, None) for field in ("operation_id", "code", "cause_code", "next_action", "publication_operation_id", "state", "retryable")}}))
"""

PYTHON_ADMISSION_RECOVERY_PROGRAM = r"""
import json, os, sys, time
from pathlib import Path
import nokv
config = json.load(sys.stdin)
control = Path(config["control_directory"])
client = nokv.Client(config["root_id"], nokv.RoutingConfig.etcd([config["etcd_endpoint"]], key_prefix=config["etcd_prefix"]), nokv.ObjectStoreConfig.s3(config["bucket"], root=config["object_root"], endpoint=config["object_endpoint"], access_key_id=config["access_key"], secret_access_key=config["secret_key"]), workbench_root=config["workbench_root"])

def emit(name, value):
    value.update(client_id=id(client), pid=os.getpid())
    (control / (name + ".json")).write_text(json.dumps(value))
    return value

def wait(name):
    deadline = time.monotonic() + 100
    while not (control / name).exists():
        if time.monotonic() >= deadline:
            raise RuntimeError("test coordinator did not release " + name)
        time.sleep(.05)

def append_once():
    try:
        return {"status":"success", "result":client.append_bytes(config["workbench"], "logs/effects.log", b"admission-recovered\n", config["operation_id"], content_type="text/plain", max_logical_size=1048576)}
    except Exception as error:
        return {"status":"error", "type":type(error).__name__, "message":str(error), **{field:getattr(error,field,None) for field in ("operation_id","code","cause_code","next_action","state")}}

# Generic publication binds and caches this exact Client's object handle before
# the append-specific admission fault is armed by the parent coordinator.
seed = client.publish_bytes(config["workbench"], "logs/effects.log", b"python-bound-seed\n", content_type="text/plain")
ready = emit("ready", {"status":"success", "seed":seed})
wait("first-go")
first = emit("first", append_once())
wait("second-go")
second = emit("second", append_once())
print(json.dumps({"ready":ready,"first":first,"second":second}))
"""


def wait_json_file(path, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            return json.loads(path.read_text())
        except (FileNotFoundError, json.JSONDecodeError):
            time.sleep(0.05)
    raise TimeoutError(f"Python coordinator did not receive {path}")


def python_same_client_admission_recovery(stack):
    workbench, operation_id = "product-python-admission-recovery", "eb" * 16
    stack.cli("workbench_create", {"id": workbench})
    control = stack.evidence_dir / "python-same-client-control"
    control.mkdir()
    config = {
        "root_id": stack.root_id,
        "etcd_endpoint": stack.etcd_endpoint,
        "etcd_prefix": stack.etcd_prefix,
        "workbench_root": stack.config.workbench_root,
        "bucket": stack.bucket,
        "object_root": stack.object_root,
        "object_endpoint": stack.object_endpoint,
        "access_key": stack.access_key,
        "secret_key": stack.secret_key,
        "workbench": workbench,
        "operation_id": operation_id,
        "control_directory": str(control),
    }
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            stack.run,
            [stack.python_executable, "-c", PYTHON_ADMISSION_RECOVERY_PROGRAM],
            label="python-one-client-admission-recovery",
            input_text=json.dumps(config),
            timeout=2 * stack.timeout,
        )
        try:
            ready = wait_json_file(control / "ready.json", stack.timeout)
            exact_body(
                stack,
                workbench,
                b"python-bound-seed\n",
                "python-admission-seed-before-fault",
            )
            with stack.object_proxy.lock:
                prior_faults = len(stack.object_proxy.rejected_requests)
                stack.object_proxy.reject_matching = {
                    "method": "PUT",
                    "path_contains": "/seal-",
                }
            (control / "first-go").touch()
            first = wait_json_file(control / "first.json", stack.timeout)
            faults = stack.object_proxy.rejected_requests[prior_faults:]
            if not faults:
                raise base.infra.NotQualified(
                    "Append-specific seal admission was not reached by this installed Python Client; no outage was injected."
                )
            require(
                first.get("status") == "error"
                and first.get("operation_id") == operation_id
                and first.get("code") == "AppendFailed"
                and first.get("cause_code") == "ProviderAdmissionInconclusive"
                and first.get("next_action") == "query_same"
                and first.get("state") is None,
                "temporary seal admission failure is typed and keeps logical identity",
                first,
            )
            absent = operation_status(
                stack, operation_id, "python-admission-failure-unadmitted"
            )
            require(
                absent.get("code") == "NotFound",
                "provider admission failure creates no logical operation",
                absent,
            )
            exact_body(
                stack,
                workbench,
                b"python-bound-seed\n",
                "python-admission-failure-no-effect",
            )
            with stack.object_proxy.lock:
                stack.object_proxy.reject_matching = None
            (control / "second-go").touch()
            second = wait_json_file(control / "second.json", stack.timeout)
            require(
                second.get("status") == "success",
                "same live Python Client recovers after temporary seal-admission failure",
                second,
            )
            require(
                ready["client_id"] == first["client_id"] == second["client_id"]
                and ready["pid"] == first["pid"] == second["pid"],
                "both attempts use exactly one Client instance in one Python process",
            )
            process = future.result(timeout=stack.timeout)
        finally:
            with stack.object_proxy.lock:
                stack.object_proxy.reject_matching = None
            (control / "first-go").touch()
            (control / "second-go").touch()
    exact_body(
        stack,
        workbench,
        b"python-bound-seed\nadmission-recovered\n",
        "python-admission-recovered-body",
    )
    replay = replay_unchanged(
        stack,
        workbench,
        operation_id,
        b"admission-recovered\n",
        second["result"],
        "python-admission-cli-replay",
        max_size=1024 * 1024,
    )
    return {
        "same_client_process": json.loads(process.stdout),
        "injected_503_requests": faults,
        "unadmitted_status_after_failure": absent,
        "replay": replay,
    }


def python_append(
    stack,
    workbench,
    operation_id,
    payload,
    *,
    label,
    block_size=4 * 1024 * 1024,
    metadata_only=False,
):
    config = {
        "root_id": stack.root_id,
        "etcd_endpoint": stack.etcd_endpoint,
        "etcd_prefix": stack.etcd_prefix,
        "workbench_root": stack.config.workbench_root,
        "bucket": stack.bucket,
        "object_root": stack.object_root,
        "object_endpoint": stack.object_endpoint,
        "access_key": stack.access_key,
        "secret_key": stack.secret_key,
        "metadata_only": metadata_only,
        "workbench": workbench,
        "operation_id": operation_id,
        "payload_hex": payload.hex(),
        "content_type": "text/plain",
        "block_size": block_size,
        "max_logical_size": 1024 * 1024,
    }
    process = stack.run(
        [stack.python_executable, "-c", PYTHON_APPEND_PROGRAM],
        label=label,
        input_text=json.dumps(config),
    )
    result = json.loads(process.stdout)
    stack.record(
        "product-python-append.jsonl",
        {
            "at": base.live.now(),
            "label": label,
            "operation_id": operation_id,
            "block_size": block_size,
            "metadata_only": metadata_only,
            "payload_sha256": hashlib.sha256(payload).hexdigest(),
            "result": result,
        },
    )
    return result


def python_metadata_status(stack):
    if stack.python_executable is None:
        return case_result(
            reason="No isolated installed Python SDK executable supplied."
        )
    workbench, operation_id = "product-python-status", "e5" * 16
    stack.cli("workbench_create", {"id": workbench})
    original = append(
        stack,
        workbench,
        operation_id,
        "python-status-event\n",
        label="python-status-cli-original",
        max_size=1024 * 1024,
    )
    require(original.get("status") == "success", "Python status original", original)
    replay = python_append(
        stack,
        workbench,
        operation_id,
        b"python-status-event\n",
        label="python-append-cli-replay",
    )
    require(
        replay.get("status") == "success"
        and canonical_receipt(replay["result"]) == canonical_receipt(original),
        "CLI to installed Python original receipt",
        replay,
    )
    reverse_id, reverse_payload = "e9" * 16, b"python-64k-event\n"
    python_original = python_append(
        stack,
        workbench,
        reverse_id,
        reverse_payload,
        label="python-fresh-nondefault-block",
        block_size=65536,
    )
    require(
        python_original.get("status") == "success"
        and python_original["result"].get("replayed") is False,
        "Python fresh publication with nondefault block size",
        python_original,
    )
    reverse_replay = replay_unchanged(
        stack,
        workbench,
        reverse_id,
        reverse_payload,
        python_original["result"],
        "cli-python-nondefault-replay",
        repetitions=2,
        block_size=65536,
        max_size=1024 * 1024,
    )
    exact_body(
        stack,
        workbench,
        b"python-status-event\npython-64k-event\n",
        "python-interoperability-complete-bytes",
    )
    live_before = live_path(stack, workbench, "python-metadata-replay-live-before")
    metadata_only_replay = python_append(
        stack,
        workbench,
        reverse_id,
        reverse_payload,
        label="python-no-object-store-replay",
        block_size=65536,
        metadata_only=True,
    )
    require(
        metadata_only_replay.get("status") == "success"
        and metadata_only_replay.get("object_store_configured") is False
        and canonical_receipt(metadata_only_replay["result"])
        == canonical_receipt(python_original["result"]),
        "Python append replay works without configured object store",
        metadata_only_replay,
    )
    require(
        live_path(stack, workbench, "python-metadata-replay-live-after") == live_before,
        "metadata-only Python append replay does not change live head",
    )
    (stack.evidence_dir / "python-status-cli-original.input").unlink()
    config = {
        "root_id": stack.root_id,
        "etcd_endpoint": stack.etcd_endpoint,
        "etcd_prefix": stack.etcd_prefix,
        "workbench_root": stack.config.workbench_root,
        "operation_id": operation_id,
    }
    before = (stack.evidence_dir / "object-http-transcript.jsonl").read_text()
    process = stack.run(
        [stack.python_executable, "-c", PYTHON_STATUS_PROGRAM],
        label="python-metadata-only-status",
        input_text=json.dumps(config),
    )
    result = json.loads(process.stdout)
    require(
        "error" not in result
        and canonical_receipt(result["result"]["receipt"])
        == canonical_receipt(original),
        "Python status requires no object store or payload",
        result,
    )
    after = (stack.evidence_dir / "object-http-transcript.jsonl").read_text()
    require(after == before, "status performs no S3 I/O")
    mismatch = python_append(
        stack,
        workbench,
        operation_id,
        b"different\n",
        label="python-intent-mismatch",
        metadata_only=True,
    )
    require(
        mismatch.get("type") == "AppendError"
        and mismatch.get("cause_code") == "RequestReplayMismatch"
        and mismatch.get("operation_id") == operation_id,
        "Python mismatch exposes stable cause and identity",
        mismatch,
    )
    admission_recovery = python_same_client_admission_recovery(stack)
    return case_result(
        safety="PASS",
        completion="PASS",
        installed_python={
            "executable": str(stack.python_executable),
            "native_sha256": replay["native_sha256"],
        },
        cli_to_python=replay,
        python_original=python_original,
        python_to_cli=reverse_replay,
        metadata_only_append_replay=metadata_only_replay,
        metadata_only_status=result,
        mismatch=mismatch,
        same_client_admission_recovery=admission_recovery,
    )


def legacy_wire_rejected(stack, legacy_binary):
    if legacy_binary is None:
        return case_result(
            reason="No frozen incompatible legacy binary supplied for real wire rejection."
        )
    workbench, operation_id = "product-legacy-wire", "e6" * 16
    stack.cli("workbench_create", {"id": workbench})
    seed = append(
        stack, workbench, "e7" * 16, "current-format\n", label="legacy-wire-seed"
    )
    require(seed.get("status") == "success", "current server fixture", seed)
    before = live_path(stack, workbench, "legacy-wire-live-before")
    objects = inventory(stack, "legacy-wire-inventory-before")
    config = dataclasses.replace(stack.config, binary=legacy_binary)
    command = [
        *base.live.client_args(config),
        "workspace-path",
        "append",
        workbench,
        "logs",
        "effects.log",
        "--operation-id",
        operation_id,
        "--text",
        "must-not-write\n",
    ]
    result = stack.run(command, label="real-legacy-append-rejection", check=False)
    require(
        result.returncode != 0, "incompatible legacy client is rejected", result.stdout
    )
    error = result.stderr.lower()
    require(
        any(
            term in error
            for term in ("schema", "handshake", "capability", "wire version")
        ),
        "legacy rejection identifies incompatible protocol instead of unrelated CLI error",
        result.stderr,
    )
    require(
        "unknown command" not in error and "unrecognized" not in error,
        "not a parser-only legacy rejection",
        result.stderr,
    )
    # The proxy intentionally captured the rejected old client's handshake.
    # Refresh the supplementary probe profile using the current native surface.
    stack.cli(
        "workbench_stat",
        {"id": workbench, "section": "logs", "path": "effects.log"},
        label="current-client-after-legacy-rejection",
    )
    require(
        live_path(stack, workbench, "legacy-wire-live-after") == before,
        "old client causes no path/revision/generation mutation",
    )
    require(
        inventory(stack, "legacy-wire-inventory-after") == objects,
        "old client creates no payload object",
    )
    status = operation_status(stack, operation_id, "legacy-wire-operation-absent")
    require(
        status.get("code") == "NotFound",
        "old client admitted no logical operation",
        status,
    )
    return case_result(
        safety="PASS",
        completion="PASS",
        legacy_binary=str(legacy_binary),
        legacy_binary_sha256=base.live.digest_file(legacy_binary),
        rejection=result.stderr,
        operation_status=status,
        qualification="Old client against current owner only; actual old Holt-format reopen is a separate required scope.",
    )


def directory_manifest(directory):
    return {
        str(path.relative_to(directory)): base.live.digest_file(path)
        for path in sorted(directory.rglob("*"))
        if path.is_file()
    }


def control_records(stack, label):
    result = stack.run(
        [
            stack.etcdctl,
            "--endpoints=" + stack.etcd_endpoint,
            "get",
            stack.etcd_prefix,
            "--prefix",
            "--write-out=json",
        ],
        label=label,
    )
    return json.loads(result.stdout).get("kvs", [])


def legacy_format_rejected(stack, legacy_binary):
    if legacy_binary is None:
        return case_result(
            reason="No actual incompatible old binary supplied to create the durable fixture."
        )
    evidence_dir = stack.evidence_dir / "legacy-format"
    with base.IsolatedStack(
        evidence_dir,
        binary=legacy_binary,
        source=stack.source,
        label="old-format",
        timeout=stack.timeout,
    ) as old:
        old.proxy.capture_rpc = True
        workbench = "old-format-fixture"
        old.cli("workbench_create", {"id": workbench})
        first = base.native_append(
            old, workbench, "f1" * 16, "old-format-data\n", label="old-format-original"
        )
        require(
            first.get("status") == "success", "actual old binary wrote fixture", first
        )
        old.owner.kill()
        old.owner.wait(timeout=10)
        base.infra.wait_session_absent(
            old.etcdctl,
            old.etcd_endpoint,
            f"{old.etcd_prefix}/sessions/{old.shard_id}",
            old.source,
            old.timeout,
        )
        cloned = evidence_dir / "old-format-reopen-copy"
        shutil.copytree(old.config.metadata, cloned)
        before = directory_manifest(cloned)
        control_before = control_records(old, "old-format-control-before")
        objects_before = inventory(old, "old-format-objects-before")
        config = dataclasses.replace(
            old.config, binary=stack.binary, metadata=cloned, metadata_mode="reopen"
        )
        process = old.start("new-reader-old-format", base.live.server_command(config))
        try:
            code = process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)
            raise ContractViolation(
                "Current owner did not fail closed when reopening actual old format"
            )
        log = (evidence_dir / "new-reader-old-format.log").read_text()
        require(
            code != 0
            and any(
                term in log.lower()
                for term in ("schema", "registry", "incompatible", "version")
            ),
            "actual old store rejected for format incompatibility",
            {"exit_code": code, "log": log},
        )
        after = directory_manifest(cloned)
        manifest = {
            "before": before,
            "after": after,
            "exact_files_unchanged": before == after,
        }
        old.json("old-format-files.json", manifest)
        control_unchanged = (
            control_records(old, "old-format-control-after") == control_before
        )
        payload_unchanged = inventory(old, "old-format-objects-after") == objects_before
        old.restart()
        require(
            base.materialize(
                old, workbench, "logs", "effects.log", "old-format-original-reopened"
            )
            == b"old-format-data\n",
            "original old store remains independently readable",
        )
        safe = before == after and control_unchanged and payload_unchanged
        return case_result(
            safety="PASS" if safe else "FAIL",
            completion="PASS",
            legacy_binary=str(legacy_binary),
            legacy_sha256=base.live.digest_file(legacy_binary),
            original_receipt=first,
            rejected_exit_code=code,
            rejected_log=log,
            cloned_store_all_file_hashes_unchanged=before == after,
            control_unchanged=control_unchanged,
            payload_unchanged=payload_unchanged,
            original_old_store_reopened=True,
            **(
                {
                    "failure": "Actual incompatible-format rejection modified durable files or control/object state; the independent original store readback does not satisfy the no-write requirement."
                }
                if not safe
                else {}
            ),
        )


def exact_body(stack, workbench, expected, label):
    stat = stack.cli(
        "workbench_stat",
        {"id": workbench, "section": "logs", "path": "effects.log"},
        label=label + "-stat",
        check=False,
    )
    if stat.get("status") == "success":
        body = base.materialize(stack, workbench, "logs", "effects.log", label)
    else:
        require(stat.get("code") == "NotFound", "unexpected stat failure", stat)
        body = None
    require(
        body == expected, "exact visible bytes", {"actual": body, "expected": expected}
    )
    return stat


def recover_same_identity(
    stack, workbench, operation_id, text, *, label, deadline, observed_status=None
):
    outcomes = []
    statuses = []
    query_first = observed_status is not None
    until = time.monotonic() + deadline
    while True:
        if query_first:
            status = observed_status or operation_status(
                stack, operation_id, f"{label}-status-{len(statuses):03d}"
            )
            observed_status = None
            statuses.append(status)
            stack.json(label + "-status-observations.json", statuses)
            if status.get("state") == "committed":
                require(
                    status.get("receipt") is not None
                    and status.get("next_action") == "none"
                    and status["receipt"].get("operation_id") == operation_id,
                    "committed status has the caller's durable receipt and no recovery action",
                    status,
                )
                return {
                    **status["receipt"],
                    "status": "success",
                    "replayed": True,
                    "commit_version": status.get("commit_version"),
                }, outcomes
            if status.get("state") == "quarantined":
                return None, outcomes
            if status.get("next_action") == "poll" or (
                status.get("status") == "error" and status.get("code") != "NotFound"
            ):
                if time.monotonic() >= until:
                    return None, outcomes
                time.sleep(min(0.5, max(0, until - time.monotonic())))
                continue
            require(
                status.get("next_action") == "resubmit_same"
                or status.get("code") == "NotFound",
                "status gives explicit same-identity recovery instruction",
                status,
            )
        result = append(
            stack,
            workbench,
            operation_id,
            text,
            label=f"{label}-{len(outcomes):03d}",
            max_size=1024 * 1024,
        )
        outcomes.append(result)
        stack.json(label + "-outcomes.json", outcomes)
        if result.get("status") == "success":
            return result, outcomes
        require(
            result.get("status") == "error"
            and result.get("details", {}).get("operation_id") == operation_id,
            "failure retains the caller identity",
            result,
        )
        query_first = result.get("details", {}).get("next_action") == "query_same"
        if time.monotonic() >= until:
            return None, outcomes
        time.sleep(min(0.5, max(0, until - time.monotonic())))


def owner_begin_completion(stack, deadline):
    """Committed Begin, lost response, real owner SIGKILL, same-ID completion."""
    workbench = "product-owner-begin-completion"
    operation_id, text = "b1" * 16, "one-recoverable-event\n"
    stack.cli("workbench_create", {"id": workbench})
    stack.proxy.arm("begin_artifact_publish", lambda: stack.owner.kill())
    first = base.native_append(
        stack,
        workbench,
        operation_id,
        text,
        label="owner-begin-first",
        check=False,
    )
    stack.proxy.wait_dropped()
    dropped = stack.proxy.last_drop
    require(dropped is not None, "successful Begin fault must actually fire")
    require(first.get("status") == "error", "first caller observed no success", first)
    stack.owner.wait(timeout=10)
    require(
        stack.owner.returncode == -9,
        "owner exited from SIGKILL",
        stack.owner.returncode,
    )
    stack.restart()
    exact_body(stack, workbench, None, "owner-begin-before-retry")
    recovered, outcomes = recover_same_identity(
        stack,
        workbench,
        operation_id,
        text,
        label="owner-begin-same-identity",
        deadline=deadline,
    )
    expected = text.encode() if recovered is not None else None
    visible = exact_body(stack, workbench, expected, "owner-begin-final")
    result = case_result(
        safety="PASS",
        completion="PASS" if recovered is not None else "FAIL",
        operation_id=operation_id,
        first_call=first,
        retry_outcomes=outcomes,
        recovered_receipt=recovered,
        final_stat=visible,
        fault={
            "operation": dropped["operation"],
            "owner_pid": dropped["owner_pid"],
            "successful_owner_response_observed": True,
            "delivered_to_caller": False,
        },
        oracle="An admitted logical append completes after owner recovery when the same ID and payload are supplied; no second logical effect.",
    )
    if recovered is None:
        result["failure"] = (
            "Same identity remains terminally Failed after admitted Begin and owner recovery; zero effects cannot satisfy completion."
        )
    return result


def main():
    native_cases = {
        "owner_begin_completion": lambda stack, args: owner_begin_completion(
            stack, args.completion_timeout_seconds
        ),
        **{
            "caller_" + operation: (
                lambda stack, args, operation=operation: caller_boundary_completion(
                    stack, args.completion_timeout_seconds, operation
                )
            )
            for operation in (
                "begin_artifact_publish",
                "stage_artifact_objects",
                "mark_artifact_objects_uploaded",
                "stage_artifact_manifest",
                "complete_artifact_publish",
            )
        },
        "deterministic_same_identity": lambda stack, args: deterministic_same_identity(
            stack, args.completion_timeout_seconds
        ),
        "distinct_identity_exact_bytes": lambda stack,
        args: distinct_identity_exact_bytes(stack, args.completion_timeout_seconds),
        "object_put_response_loss": lambda stack, args: object_put_response_loss(
            stack, args.completion_timeout_seconds
        ),
        "object_read_integrity": lambda stack, args: object_read_integrity(
            stack, args.completion_timeout_seconds
        ),
        "cross_root_identity": lambda stack, args: cross_root_identity(stack),
        "boundary_and_capacity": lambda stack, args: boundary_and_capacity(stack),
        "publication_batch_boundaries": lambda stack,
        args: publication_batch_boundaries(stack),
        "identity_errors_and_status": lambda stack, args: identity_errors_and_status(
            stack
        ),
        "repeated_identity_retention": lambda stack, args: repeated_identity_retention(
            stack, args.replay_count
        ),
        "completed_replay_provider_unavailable": lambda stack,
        args: completed_replay_provider_unavailable(stack),
        "python_metadata_status": lambda stack, args: python_metadata_status(stack),
        "legacy_wire_rejected": lambda stack, args: legacy_wire_rejected(
            stack, args.legacy_nokv_bin
        ),
        "legacy_format_rejected": lambda stack, args: legacy_format_rejected(
            stack, args.legacy_nokv_bin
        ),
        "late_put_after_cleanup": lambda stack, args: late_put_after_cleanup(
            stack, args.completion_timeout_seconds
        ),
        "seal_quarantine_operator_recovery": lambda stack,
        args: seal_quarantine_operator_recovery(stack, args.completion_timeout_seconds),
    }
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nokv-bin", type=Path, required=True)
    parser.add_argument("--source-dir", type=Path, required=True)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--completion-timeout-seconds", type=float, default=60)
    parser.add_argument("--timeout-seconds", type=int, default=60)
    parser.add_argument(
        "--append-activity-lease-ms",
        type=int,
        help="The production server's supported activity lease; omit for its default.",
    )
    parser.add_argument("--python-executable", type=Path)
    parser.add_argument("--legacy-nokv-bin", type=Path)
    parser.add_argument("--replay-count", type=int, default=100)
    parser.add_argument("--scenario", choices=("all", *native_cases), default="all")
    parser.add_argument(
        "--scenarios",
        nargs="+",
        choices=tuple(native_cases),
        help="Run an explicit ordered subset; it never qualifies the full matrix.",
    )
    args = parser.parse_args()
    results = {
        "schema": "nokv.append_product_acceptance.v1",
        "status": "NOT QUALIFIED",
        "started_at": base.live.now(),
        "command": sys.argv,
        "scenarios": {},
        "qualification_scope": "Bounded append on one local Holt owner and real RustFS; this is not whole-workspace or cross-host commercial qualification.",
        "safety": "NOT QUALIFIED",
        "completion": "NOT QUALIFIED",
        "performance": "NOT QUALIFIED",
        "workspace_release": "NOT QUALIFIED",
        "unqualified_release_scopes": {
            "whole_workspace_gates_0_through_8": "NOT QUALIFIED",
            "owner_internal_durable_commit_boundaries": "NOT QUALIFIED",
            "long_term_retention_and_high_cardinality_gc": "NOT QUALIFIED",
            "real_demo_harness_queue_redelivery": "NOT QUALIFIED",
            "workload_matched_performance_slo": "NOT QUALIFIED",
            "cross_host_shared_recovery": "NOT QUALIFIED",
        },
        "harness_path": str(Path(__file__).resolve()),
        "harness_sha256": base.live.digest_file(Path(__file__)),
        "python_runtime": {"version": sys.version, "optimize": sys.flags.optimize},
        "os": platform.platform(),
    }
    stack = None
    exit_code = 2
    try:
        # Base stack contains assertions too; refusing optimization prevents a
        # disabled helper oracle from contaminating qualification.
        require(
            sys.flags.optimize == 0, "qualification requires Python assertions enabled"
        )
        stack = ProductStack(
            args.evidence_dir,
            binary=args.nokv_bin,
            source=args.source_dir,
            label="append-product",
            timeout=args.timeout_seconds,
            publication_lease_ms=args.append_activity_lease_ms,
        )
        stack.python_executable = args.python_executable
        shutil.copyfile(Path(__file__), args.evidence_dir / "executed-harness.py")
        with stack:
            stack.proxy.capture_rpc = True
            results["machine_profile"] = record_machine_profile(stack)
            require(
                not args.scenarios or args.scenario == "all",
                "choose --scenario or --scenarios, not both",
            )
            cases = (
                [(name, native_cases[name]) for name in args.scenarios]
                if args.scenarios
                else native_cases.items()
                if args.scenario == "all"
                else [(args.scenario, native_cases[args.scenario])]
            )
            for name, case in cases:
                print("START " + name, flush=True)
                started = time.monotonic()
                try:
                    result = case(stack, args)
                except Exception as error:
                    result = case_result(
                        safety="FAIL"
                        if isinstance(error, ContractViolation)
                        else "NOT QUALIFIED",
                        completion="NOT QUALIFIED",
                        failure=repr(error),
                        traceback=traceback.format_exc(),
                    )
                result["elapsed_seconds"] = time.monotonic() - started
                results["scenarios"][name] = result
                stack.json("product-results.json", results)
                print(
                    json.dumps(
                        {
                            "scenario": name,
                            "safety": result["safety"],
                            "completion": result["completion"],
                        }
                    ),
                    flush=True,
                )
                if "FAIL" in (result["safety"], result["completion"]) or result.get(
                    "failure"
                ):
                    break
            for dimension in ("safety", "completion"):
                statuses = [value[dimension] for value in results["scenarios"].values()]
                results[dimension] = (
                    "FAIL"
                    if "FAIL" in statuses
                    else "PASS"
                    if statuses and all(value == "PASS" for value in statuses)
                    else "NOT QUALIFIED"
                )
            results["all_listed_scenarios_executed"] = set(results["scenarios"]) == set(
                native_cases
            )
            results["status"] = (
                "FAIL"
                if "FAIL" in (results["safety"], results["completion"])
                else "PASS"
                if results["safety"] == results["completion"] == "PASS"
                else "NOT QUALIFIED"
            )
            results["qualified_scopes"] = [
                name
                for name, value in results["scenarios"].items()
                if value["safety"] == value["completion"] == "PASS"
            ]
            if "legacy_format_rejected" not in results["qualified_scopes"]:
                results["unqualified_release_scopes"][
                    "old_holt_format_reopen_no_write"
                ] = "NOT QUALIFIED"
            exit_code = {"FAIL": 1, "NOT QUALIFIED": 2, "PASS": 0}[results["status"]]
    except base.infra.NotQualified as error:
        results["not_qualified_reason"] = str(error)
    except Exception as error:
        results["status"] = (
            "FAIL" if isinstance(error, ContractViolation) else "NOT QUALIFIED"
        )
        results["error"] = repr(error)
        results["traceback"] = traceback.format_exc()
        exit_code = 1 if isinstance(error, ContractViolation) else 2
    finally:
        results["completed_at"] = base.live.now()
        args.evidence_dir.mkdir(parents=True, exist_ok=True)
        if stack is not None:
            cleanup_path = args.evidence_dir / "cleanup.json"
            if cleanup_path.is_file():
                cleanup = json.loads(cleanup_path.read_text())
                if any(
                    not isinstance(value, dict) or value.get("exit_code") != 0
                    for value in cleanup["results"]
                ):
                    results["cleanup_failure"] = cleanup
                    if results["status"] == "PASS":
                        results["status"], exit_code = "NOT QUALIFIED", 2
            stack.json("product-results.json", results)
        else:
            (args.evidence_dir / "product-results.json").write_text(
                json.dumps(results, indent=2) + "\n"
            )
        print(
            json.dumps(
                {
                    "status": results["status"],
                    "safety": results["safety"],
                    "completion": results["completion"],
                    "evidence": str(args.evidence_dir),
                }
            ),
            flush=True,
        )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
