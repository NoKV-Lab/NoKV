#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0
"""Native CLI contract qualification on one host.

The gate provisions one root on a `--control-dir` control store, starts one
owner behind a frame-level proxy, and drives the contract in
docs/cli-contract.md only through the native `nokv` binary. Objects go to an
existing S3 endpoint, or to a local moto server that the gate starts.
Credentials reach every process through AWS_* environment variables, never
argv.

A scenario is graded only from exit codes, the stderr failure envelope, oracle
read-backs, and the proxy's RPC trace. A scenario that injects a fault first
proves the fault fired; if it did not, the scenario is NOT QUALIFIED rather
than PASS. With --expect-red every scenario must FAIL instead, which
reproduces the contract gaps on a binary that predates them.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import secrets
import socket
import subprocess
import sys
import threading
import time
import urllib.request
from pathlib import Path
from typing import Any, Callable

from append_identity_recovery_gate import unpack
from cli_contract import (
    Call,
    ContractFailure,
    NotQualified,
    contract_failure,
    contract_success,
    contract_tables,
    require,
)
from fork_restore_recovery_gate import (
    ResponseDropProxy,
    _recv_exact,
    is_handshake_frame,
)

SCHEMA = "nokv.cli_contract_gate.v1"
REPOSITORY = Path(__file__).resolve().parents[2]
WORKBENCH_ROOT = "/agents/cli-contract/wb"
REGION = "us-east-1"
# A body past this bound publishes through objects (Prepare, uploads, Finish).
# Mirrors MAX_INLINE_ARTIFACT_BODY_BYTES in crates/nokv-types.
MAX_INLINE_BODY_BYTES = 16 * 1024
NOT_QUALIFIED_SCENARIOS = {
    "s6_path_lineage_aba": (
        "A path generation restarts at 1 after remove and recreate; telling the "
        "lineages apart needs artifact_revision_id or a durable path lineage (W1)."
    ),
    "s8_cross_principal_identity": (
        "Principals are reserved in the request fingerprint but not "
        "authenticated; one root is bound to one Agent."
    ),
}


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def new_id() -> str:
    return secrets.token_hex(16)


def hex_of(value: Any) -> str:
    """A decoded wire id: a MessagePack array of bytes or a binary value."""
    return value["binary_hex"] if isinstance(value, dict) else bytes(value).hex()


class TracingProxy(ResponseDropProxy):
    """Counts every owner connection and request, and drops one armed
    successful response."""

    def __init__(self, port: int, timeout: float, record: Callable[[dict], None]):
        super().__init__(port, timeout)
        self.record = record
        self.connections = 0
        self.requests: list[str] = []
        self.last_drop: dict[str, Any] | None = None

    def disarm(self) -> None:
        with self._lock:
            self._drop_operation = None
            self._on_drop = None

    def dropped(self) -> bool:
        # The faulted command has exited, so only thread handoff remains.
        return self._dropped.wait(1)

    def _forward(self, client: socket.socket) -> None:
        with self._lock:
            self.connections += 1
            target = self._target
        try:
            client.settimeout(self.timeout)
            with socket.create_connection(target, timeout=self.timeout) as owner:
                owner.settimeout(self.timeout)
                while True:
                    first = client.recv(4)
                    if not first:
                        return
                    header = first + _recv_exact(client, 4 - len(first))
                    request = _recv_exact(client, int.from_bytes(header, "big"))
                    owner.sendall(header + request)
                    response_header = _recv_exact(owner, 4)
                    response = _recv_exact(
                        owner, int.from_bytes(response_header, "big")
                    )
                    callback = None
                    if not is_handshake_frame(request):
                        decoded_request, decoded_response = unpack(request), unpack(response)
                        operation = decoded_request["payload"]["operation"]["operation"]
                        status = decoded_response["payload"]["outcome"]["status"]
                        with self._lock:
                            self.requests.append(operation)
                            if status == "success" and operation == self._drop_operation:
                                callback = self._on_drop
                                self._drop_operation = None
                                self._on_drop = None
                        self.record(
                            {
                                "operation": operation,
                                "status": status,
                                "dropped": callback is not None,
                            }
                        )
                        if callback is not None:
                            self.last_drop = {
                                "operation": operation,
                                "request": decoded_request,
                                "response": decoded_response,
                            }
                            callback()
                            self._dropped.set()
                            return
                    client.sendall(response_header + response)
        except (ConnectionError, OSError, TimeoutError, ValueError, KeyError) as error:
            # Owner restarts leave a connection-refused window; only an armed
            # response must be observable.
            with self._lock:
                armed = self._drop_operation is not None
            if armed:
                self._errors.append(repr(error))
        finally:
            client.close()


class Stack:
    """One provisioned root, one owner behind the proxy, and the CLI."""

    def __init__(self, args: argparse.Namespace, evidence: Path) -> None:
        self.binary = Path(args.nokv_bin).resolve()
        self.evidence = evidence
        self.timeout = args.timeout
        self.exits, self.actions = contract_tables(
            (REPOSITORY / "docs" / "cli-contract.md").read_text()
        )
        self.ids = {name: new_id() for name in ("root", "agent", "shard")}
        self.control = evidence / "control"
        self.metadata = evidence / "metadata"
        self.files = evidence / "files"
        self.files.mkdir(parents=True)
        self.env = dict(os.environ, AWS_DEFAULT_REGION=REGION, AWS_EC2_METADATA_DISABLED="true")
        self.moto: subprocess.Popen | None = None
        if args.moto_server:
            for name in ("AWS_PROFILE", "AWS_SESSION_TOKEN"):
                self.env.pop(name, None)
            self.env["AWS_ACCESS_KEY_ID"] = "gate" + secrets.token_hex(8)
            self.env["AWS_SECRET_ACCESS_KEY"] = secrets.token_hex(20)
            port = free_port()
            endpoint = f"http://127.0.0.1:{port}"
            self.moto = subprocess.Popen(
                [args.moto_server, "-H", "127.0.0.1", "-p", str(port)],
                stdout=(evidence / "moto.log").open("wb"),
                stderr=subprocess.STDOUT,
                env=self.env,
            )
            self.wait_tcp(port, "moto")
            urllib.request.urlopen(
                urllib.request.Request(f"{endpoint}/{args.object_bucket}", method="PUT"),
                timeout=self.timeout,
            ).close()
        else:
            endpoint = args.object_endpoint
            for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
                if not self.env.get(name):
                    raise NotQualified(f"{name} must be set for --object-endpoint")
        self.objects = [
            "--object-bucket", args.object_bucket,
            "--object-endpoint", endpoint,
            "--object-root", args.object_root or "cli-contract-" + secrets.token_hex(4),
            "--object-region", REGION,
        ]
        self.route = ["--root-id", self.ids["root"], "--control-dir", str(self.control)]
        self.owner: subprocess.Popen | None = None
        self.owner_port = free_port()
        self.owner_starts = 0
        self.proxy = TracingProxy(free_port(), self.timeout, self.trace)
        self.proxy.set_target(self.owner_port)
        self.lock = threading.Lock()

    def write(self, name: str, value: Any) -> None:
        with self.lock, (self.evidence / name).open("a") as stream:
            stream.write(json.dumps(value, sort_keys=True, default=str) + "\n")

    def trace(self, entry: dict) -> None:
        self.write("rpc-trace.jsonl", {"at": time.time(), **entry})

    def wait_tcp(self, port: int, what: str) -> None:
        deadline = time.monotonic() + self.timeout
        while time.monotonic() < deadline:
            try:
                socket.create_connection(("127.0.0.1", port), 0.2).close()
                return
            except OSError:
                time.sleep(0.1)
        raise NotQualified(f"{what} did not listen on port {port}")

    def object_options(self, command: str) -> list[str]:
        """The object-store options an invocation of `command` runs with."""
        return self.objects

    def run(self, label: str, arguments: list[str], client_options: list[str] = ()) -> Call:
        """Run one command; `client_options` are connection options such as the
        retry budget, which the CLI takes before the command."""
        prefix = [*self.route, "--agent-id", self.ids["agent"], *self.object_options(arguments[0])]
        argv = [str(self.binary), *prefix, *client_options, *arguments]
        with self.proxy._lock:
            connections, requests = self.proxy.connections, len(self.proxy.requests)
        started = time.monotonic()
        done = subprocess.run(
            argv, capture_output=True, text=True, env=self.env, timeout=self.timeout * 4
        )
        with self.proxy._lock:
            call = Call(
                label,
                done.returncode,
                done.stdout,
                done.stderr,
                self.proxy.connections - connections,
                self.proxy.requests[requests:],
            )
        self.write(
            "cli-transcript.jsonl",
            {
                "label": label,
                "arguments": arguments,
                "exit": call.exit,
                "stdout": call.stdout,
                "stderr": call.stderr,
                "owner_connections": call.connections,
                "owner_requests": call.requests,
                "elapsed_ms": round((time.monotonic() - started) * 1000, 1),
            },
        )
        return call

    def provision(self) -> None:
        call = self.run("provision", ["provision", self.ids["shard"], "--workbench-root", WORKBENCH_ROOT])
        if call.exit != 0:
            raise NotQualified(f"provision failed: {call.stderr[-600:]}")

    def start_owner(self, *serve_args: str) -> None:
        metadata = "--metadata-reopen" if self.owner_starts else "--metadata-create"
        self.owner_starts += 1
        argv = [
            str(self.binary), *self.route, *self.objects,
            "--bind", f"127.0.0.1:{self.owner_port}",
            "--advertise-endpoint", f"127.0.0.1:{self.proxy.port}",
            "--node-id", "cli-contract-owner",
            metadata, str(self.metadata), *serve_args,
            "serve",
        ]
        self.owner = subprocess.Popen(
            argv,
            stdout=(self.evidence / f"owner-{self.owner_starts}.log").open("wb"),
            stderr=subprocess.STDOUT,
            env=self.env,
        )
        self.wait_tcp(self.owner_port, "owner")
        # Ready once a metadata read reaches the owner: an unknown operation
        # is then NotFound rather than a transport failure.
        deadline = time.monotonic() + self.timeout
        while time.monotonic() < deadline:
            probe = self.run("owner-ready", ["operation", "status", new_id()])
            if '"NotFound"' in probe.stderr:
                return
            if self.owner.poll() is not None:
                break
            time.sleep(0.2)
        raise NotQualified(f"owner {self.owner_starts} did not become ready")

    def kill_owner(self) -> None:
        if self.owner is not None and self.owner.poll() is None:
            self.owner.kill()
            self.owner.wait(timeout=self.timeout)

    def restart_owner(self) -> None:
        self.kill_owner()
        self.start_owner()

    def close(self) -> None:
        self.kill_owner()
        if self.moto is not None:
            self.moto.kill()
            self.moto.wait(timeout=self.timeout)
        self.proxy.close()

    # Grading.

    def success(self, call: Call) -> dict[str, Any]:
        return contract_success(call)

    def failure(self, call: Call, klass: str, next_action: str) -> dict[str, Any]:
        """The call failed with exactly the contract envelope of `klass`."""
        return contract_failure(call, self.exits, self.actions, klass, next_action)

    def faulted(self, label: str, arguments: list[str], operation: str) -> Call:
        """Run a command whose successful `operation` response is dropped and
        whose owner is killed, so no in-process recovery can observe it."""
        self.proxy.arm(operation, self.kill_owner)
        call = self.run(label, arguments)
        if not self.proxy.dropped():
            self.proxy.disarm()
            if not call.requests:
                self.failure(call, "invalid_input", "fix_input")
                raise ContractFailure(f"{label}: the binary does not accept the command")
            raise NotQualified(f"{label}: no successful {operation} was sent: {call.requests}")
        self.proxy.raise_if_failed()
        return call

    def workbench(self, name: str) -> None:
        self.success(self.run(f"create-{name}", ["workbench", "workbench_create", json.dumps({"id": name})]))

    def put_arguments(self, label: str, workbench: str, path: str, operation_id: str,
                      body: bytes, *extra: str) -> list[str]:
        source = self.files / f"{label}.body"
        source.write_bytes(body)
        return [
            "workspace-path", "put", workbench, "outputs", path,
            "--operation-id", operation_id, "--file", str(source), *extra,
        ]

    def put(self, label: str, workbench: str, path: str, operation_id: str, body: bytes,
            *extra: str) -> Call:
        return self.run(label, self.put_arguments(label, workbench, path, operation_id, body, *extra))

    def stat(self, label: str, workbench: str, path: str) -> dict[str, Any]:
        return self.success(self.run(label, ["workspace-path", "stat", workbench, "outputs", path]))

    def body(self, label: str, workbench: str, path: str) -> bytes:
        target = self.files / f"{label}.read"
        self.success(self.run(label, ["materialize", workbench, "outputs", path, str(target)]))
        return target.read_bytes()

    def assert_live(self, label: str, workbench: str, path: str, body: bytes,
                    receipt: dict[str, Any]) -> dict[str, Any]:
        """The oracle: the live path holds exactly `receipt`'s revision and `body`."""
        stat = self.stat(label + "-stat", workbench, path)
        require(
            stat.get("exists") is True
            and stat.get("generation") == receipt["generation"]
            and stat.get("artifact_revision_id") == receipt["artifact_revision_id"],
            f"{label}: the live path is the receipt's revision",
            [stat, receipt],
        )
        require(self.body(label + "-read", workbench, path) == body, f"{label}: the live body is the published body")
        return stat


def receipt_lost_same_id_retry(stack: Stack) -> dict[str, Any]:
    """S1: a committed object publication whose reply is lost replays its
    receipt to the same command, and publishes nothing more."""
    workbench, operation_id = "s1-receipt-lost", new_id()
    body = hashlib.sha256(b"s1").hexdigest().encode() * (MAX_INLINE_BODY_BYTES // 64 + 1)
    stack.workbench(workbench)
    arguments = stack.put_arguments("s1", workbench, "s1.bin", operation_id, body)
    first = stack.faulted("s1-first", arguments, "finish_artifact_publish")
    published = stack.proxy.last_drop["response"]["payload"]["outcome"]["body"]["value"]
    envelope = stack.failure(first, "outcome_unknown", "query_same")
    require(envelope["details"].get("operation_id") == operation_id, "s1: the failure names its operation", envelope)
    stack.start_owner()
    retry = stack.success(stack.run("s1-retry", arguments))
    require(
        retry.get("replayed") is True
        and retry.get("operation_id") == operation_id
        and retry.get("generation") == 1
        and retry.get("artifact_revision_id") == hex_of(published["artifact_revision_id"]),
        "s1: the retry returns the lost receipt",
        [retry, published],
    )
    stack.assert_live("s1", workbench, "s1.bin", body, retry)
    return {"failure": envelope, "replay": retry, "effects": 1}


def owner_restart_same_id_retry(stack: Stack) -> dict[str, Any]:
    """S2: request and operation identities replay across an owner restart."""
    workbench = "s2-owner-restart"
    stack.workbench(workbench)
    for path in ("a.txt", "c.txt"):
        stack.success(stack.run(f"s2-seed-{path}", ["workbench", "workbench_put_file", json.dumps(
            {"id": workbench, "section": "outputs", "path": path, "text": path})]))
    rename = ["workspace-path", "rename", workbench, "outputs", "a.txt", "b.txt",
              "--expected-generation", "1", "--request-id", new_id()]
    remove = ["workspace-path", "remove", workbench, "outputs", "c.txt",
              "--expected-generation", "1", "--request-id", new_id()]
    renamed = stack.success(stack.run("s2-rename", rename))
    removed = stack.success(stack.run("s2-remove", remove))
    require(renamed.get("idempotent_replay") is False and removed.get("idempotent_replay") is False,
            "s2: the first requests apply", [renamed, removed])
    stack.restart_owner()
    renamed_again = stack.success(stack.run("s2-rename-replay", rename))
    removed_again = stack.success(stack.run("s2-remove-replay", remove))
    # A replay returns the original result; only its replay flag and the
    # response's read version differ.
    volatile = {"idempotent_replay", "commit_version"}
    for first, again in ((renamed, renamed_again), (removed, removed_again)):
        require(
            again.get("idempotent_replay") is True
            and {key: value for key, value in again.items() if key not in volatile}
            == {key: value for key, value in first.items() if key not in volatile},
            "s2: a request id replays its original result after the owner restarts",
            [first, again],
        )
    put_id, put_body = new_id(), b"s2 body\n"
    put = stack.success(stack.put("s2-put", workbench, "d.txt", put_id, put_body))
    stack.restart_owner()
    put_again = stack.success(stack.put("s2-put-replay", workbench, "d.txt", put_id, put_body))
    require(put_again.get("replayed") is True and put_again.get("artifact_revision_id") == put["artifact_revision_id"],
            "s2: an operation id replays after the owner restarts", [put, put_again])
    stack.assert_live("s2-renamed", workbench, "b.txt", b"a.txt", renamed)
    for path in ("a.txt", "c.txt"):
        gone = stack.stat(f"s2-{path}-gone", workbench, path)
        require(gone.get("exists") is False, f"s2: {path} stays gone", gone)
    stack.assert_live("s2-put", workbench, "d.txt", put_body, put)
    return {"rename": renamed_again, "remove": removed_again, "put": put_again}


def conflicting_writer_expected_generation(stack: Stack) -> dict[str, Any]:
    """S3: a writer pinned to a stale generation fails typed and applies nothing."""
    workbench = "s3-conflicting-writer"
    stack.workbench(workbench)
    created = stack.success(stack.put("s3-create", workbench, "a.txt", new_id(), b"A"))
    winner = stack.success(stack.put("s3-winner", workbench, "a.txt", new_id(), b"B",
                                     "--expected-generation", str(created["generation"])))
    stale_id = new_id()
    stale = stack.failure(
        stack.put("s3-stale", workbench, "a.txt", stale_id, b"C",
                  "--expected-generation", str(created["generation"])),
        "precondition_failed", "reread_then_retry",
    )
    require(stale["details"].get("current_generation") == winner["generation"],
            "s3: the refusal names the current generation", stale)
    create_only = stack.failure(stack.put("s3-create-again", workbench, "a.txt", new_id(), b"D"),
                                "precondition_failed", "reread_then_retry")
    require(create_only["details"].get("current_generation") == winner["generation"],
            "s3: a create-only refusal names the current generation", create_only)
    stack.failure(stack.run("s3-stale-status", ["operation", "status", stale_id]),
                  "not_found", "resubmit_same")
    stack.assert_live("s3", workbench, "a.txt", b"B", winner)
    return {"stale": stale, "create_only": create_only, "winner": winner}


def same_id_other_payload(stack: Stack) -> dict[str, Any]:
    """S4: an operation id reused for other inputs is refused and changes nothing."""
    workbench = "s4-same-id"
    stack.workbench(workbench)
    outcomes = {}
    for variant, size in (("inline", 32), ("object", MAX_INLINE_BODY_BYTES + 1)):
        operation_id, path = new_id(), f"{variant}.bin"
        original, other = b"a" * size, b"b" * size
        first = stack.success(stack.put(f"s4-{variant}", workbench, path, operation_id, original))
        conflicts = [
            stack.failure(stack.put(f"s4-{variant}-other-body", workbench, path, operation_id, other),
                          "identity_conflict", "do_not_retry"),
            stack.failure(stack.put(f"s4-{variant}-other-path", workbench, "elsewhere.bin", operation_id, original),
                          "identity_conflict", "do_not_retry"),
        ]
        status = stack.success(stack.run(f"s4-{variant}-status", ["operation", "status", operation_id]))
        require(
            status.get("state") == "succeeded"
            and status.get("receipt", {}).get("body_digest") == first["body_digest"]
            and status.get("receipt", {}).get("artifact_revision_id") == first["artifact_revision_id"],
            f"s4: the {variant} operation keeps its original receipt",
            [status, first],
        )
        stack.assert_live(f"s4-{variant}", workbench, path, original, first)
        absent = stack.stat(f"s4-{variant}-elsewhere", workbench, "elsewhere.bin")
        require(absent.get("exists") is False, "s4: a refused identity publishes nothing", absent)
        outcomes[variant] = {"conflicts": conflicts, "status": status}
    return outcomes


def unknown_outcome_query_then_retry(stack: Stack) -> dict[str, Any]:
    """S5: an unknown outcome is resolved by querying its operation id, and a
    resubmission of the same command replays."""
    workbench, operation_id, body = "s5-unknown-outcome", new_id(), b"event-5\n"
    stack.workbench(workbench)
    arguments = stack.put_arguments("s5", workbench, "s5.txt", operation_id, body)
    envelope = stack.failure(stack.faulted("s5-first", arguments, "publish_inline_artifact"),
                             "outcome_unknown", "query_same")
    require(envelope["details"].get("operation_id") == operation_id, "s5: the failure names its operation", envelope)
    stack.start_owner()
    observed = stack.success(stack.run("s5-status", ["operation", "status", operation_id]))
    receipt = observed.get("receipt") or {}
    require(
        observed.get("operation") == "publish"
        and observed.get("state") == "succeeded"
        and observed.get("next_action") == "none"
        and receipt.get("generation") == 1,
        "s5: the query reports the committed receipt",
        observed,
    )
    retry = stack.success(stack.run("s5-retry", arguments))
    require(
        retry.get("replayed") is True
        and retry.get("artifact_revision_id") == receipt.get("artifact_revision_id"),
        "s5: the resubmission replays the queried receipt",
        [retry, receipt],
    )
    stack.failure(stack.run("s5-status-unknown", ["operation", "status", new_id()]),
                  "not_found", "resubmit_same")
    stack.assert_live("s5", workbench, "s5.txt", body, retry)
    return {"failure": envelope, "status": observed, "replay": retry, "effects": 1}


def workbench_tool_operation_id_replays(stack: Stack) -> dict[str, Any]:
    """S9: the Workbench mutation tools replay exactly under a caller operation id
    across processes, and refuse the id for other inputs."""
    workbench = "s9-workbench-identity"
    stack.workbench(workbench)
    target = {"id": workbench, "section": "outputs", "path": "notes.txt"}

    def tool(label: str, name: str, arguments: dict[str, Any]) -> Call:
        return stack.run(label, ["workbench", name, json.dumps(arguments)])

    put_id, edit_id, append_id = new_id(), new_id(), new_id()
    put = {**target, "text": "alpha beta", "operation_id": put_id}
    first = stack.success(tool("s9-put", "workbench_put_file", put))
    again = stack.success(tool("s9-put-again", "workbench_put_file", put))
    require(first.get("idempotent_replay") is False and again.get("idempotent_replay") is True
            and again.get("generation") == first.get("generation") == 1
            and again.get("operation_id") == put_id,
            "s9: the same put under the same id replays its receipt", [first, again])
    other = stack.failure(tool("s9-put-other", "workbench_put_file", {**put, "text": "other"}),
                          "identity_conflict", "do_not_retry")
    edit = {**target, "old_string": "beta", "new_string": "gamma", "operation_id": edit_id}
    edited = stack.success(tool("s9-edit", "workbench_edit", edit))
    edited_again = stack.success(tool("s9-edit-again", "workbench_edit", edit))
    require(edited.get("generation") == 2 and edited.get("replacements") == 1
            and edited_again.get("idempotent_replay") is True and edited_again.get("generation") == 2
            and edited_again.get("replacements") is None,
            "s9: a repeated edit replays the recorded generation instead of NoMatch",
            [edited, edited_again])
    append = {**target, "text": " delta", "operation_id": append_id}
    appended = stack.success(tool("s9-append", "workbench_append", append))
    appended_again = stack.success(tool("s9-append-again", "workbench_append", append))
    require(appended.get("idempotent_replay") is False and appended_again.get("idempotent_replay") is True
            and appended_again.get("size_bytes") == appended.get("size_bytes")
            and appended_again.get("generation") == appended.get("generation") == 3,
            "s9: the same append under the same id is applied once", [appended, appended_again])
    body = stack.body("s9-body", workbench, "notes.txt")
    require(body == b"alpha gamma delta", "s9: the body holds one edit and one append", body)
    return {"put": [first, again, other], "edit": [edited, edited_again],
            "append": [appended, appended_again]}


def session_backoff_bridges_owner_restart(stack: Stack) -> dict[str, Any]:
    """S10: a `workbench-session` started with bounded retry backoff completes a
    write sent while the owner is down, once the owner is back."""
    workbench = "s10-session-backoff"
    stack.workbench(workbench)
    argv = [str(stack.binary), *stack.route, "--agent-id", stack.ids["agent"], *stack.objects,
            "--max-attempts", "16", "--retry-backoff-ms", "250", "--max-retry-backoff-ms", "2000",
            "workbench-session"]
    session = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, text=True, env=stack.env)
    transcript: list[dict[str, Any]] = []

    def send(index: int) -> None:
        request = {"id": str(index), "tool": "workbench_put_file", "arguments": {
            "id": workbench, "section": "outputs", "path": f"session-{index}.txt",
            "text": f"session {index}", "replace": False}}
        session.stdin.write(json.dumps(request) + "\n")
        session.stdin.flush()
        transcript.append({"label": f"s10-send-{index}", "request": request, "at": time.time()})

    def receive(index: int) -> dict[str, Any]:
        line = session.stdout.readline()
        reply = json.loads(line) if line else {"error": "the session closed its output"}
        transcript.append({"label": f"s10-reply-{index}", "reply": reply, "at": time.time()})
        return reply

    def connections() -> int:
        with stack.proxy._lock:
            return stack.proxy.connections

    try:
        send(1)
        before = receive(1)
        require("result" in before, "s10: the session writes while the owner is up", before)
        stack.kill_owner()
        down = connections()
        send(2)
        # 250 ms, then 500 ms: at least two spaced attempts fall in this window,
        # while 16 immediate attempts would all have failed within it.
        time.sleep(1.0)
        attempts_while_down = connections() - down
        stack.start_owner()
        after = receive(2)
    finally:
        session.stdin.close()
        session.wait(timeout=stack.timeout)
        stderr = session.stderr.read()
    for entry in transcript:
        stack.write("cli-transcript.jsonl", {"session": "s10", **entry})
    require(session.returncode == 0 and not stderr,
            "s10: the session exits 0 at end of input without complaints", [session.returncode, stderr])
    require("result" in after and after["result"].get("generation") == 1,
            "s10: the write sent during the outage lands once the owner is back", after)
    require(2 <= attempts_while_down < 16,
            "s10: the session spaced its attempts over the outage within its budget", attempts_while_down)
    stat = stack.stat("s10-landed", workbench, "session-2.txt")
    require(stat.get("exists") is True and stat.get("generation") == 1,
            "s10: the live path is the session's write", [stat, after])
    require(stack.body("s10-body", workbench, "session-2.txt") == b"session 2",
            "s10: the live body is the session's text")
    return {"before": before, "after": after, "attempts_while_down": attempts_while_down}


def in_flight_identified_write_survives_owner_restart(stack: Stack) -> dict[str, Any]:
    """S11: a caller-identified put whose owner died after admitting its attempt is
    finished by the same command against the restarted owner: the client waits
    out the orphaned attempt the owner aborts, then publishes the next attempt."""
    workbench, operation_id = "s11-in-flight", new_id()
    body = hashlib.sha256(b"s11").hexdigest().encode() * (MAX_INLINE_BODY_BYTES // 64 + 1)
    stack.workbench(workbench)
    arguments = stack.put_arguments("s11", workbench, "s11.bin", operation_id, body)
    first = stack.failure(stack.faulted("s11-first", arguments, "prepare_artifact_publish"),
                          "outcome_unknown", "query_same")
    # A long lifecycle interval keeps the aborted attempt non-terminal for
    # seconds after the restart, so the retry has to wait rather than find it
    # already failed.
    stack.start_owner("--lifecycle-interval-millis", "4000")
    started = time.monotonic()
    retry = stack.run("s11-retry", arguments, ["--max-attempts", "16", "--retry-backoff-ms", "250"])
    waited = time.monotonic() - started
    receipt = stack.success(retry)
    require(receipt.get("replayed") is False and receipt.get("attempt") == 1
            and receipt.get("generation") == 1 and receipt.get("operation_id") == operation_id,
            "s11: the retry publishes its next attempt once the orphaned one is aborted", receipt)
    require(waited >= 2.0, "s11: the retry waited out the aborting attempt", waited)
    stack.assert_live("s11", workbench, "s11.bin", body, receipt)
    status = stack.success(stack.run("s11-status", ["operation", "status", operation_id]))
    require(status.get("state") == "failed", "s11: the orphaned first attempt ended failed", status)
    stack.restart_owner()
    return {"failure": first, "receipt": receipt, "waited_s": round(waited, 2),
            "first_attempt": status, "effects": 1}


def usage_error_sends_no_request(stack: Stack) -> dict[str, Any]:
    """S7: an invalid command fails typed before it reaches the owner."""
    cases = {
        "unknown-option": ["workspace-path", "rename", "s7", "outputs", "a.txt", "b.txt",
                           "--expected-generation", "1", "--request-id", new_id(), "--force"],
        "missing-request-id": ["workspace-path", "remove", "s7", "outputs", "a.txt", "--expected-generation", "1"],
        "bad-status-id": ["operation", "status", "not-hex"],
        "unknown-command": ["frobnicate"],
        "malformed-tool-arguments": ["workbench", "workbench_stat", "{not json"],
        "bad-operation-id": ["workspace-path", "put", "s7", "outputs", "a.txt",
                             "--operation-id", "XYZ", "--text", "x"],
        "missing-payload": ["workspace-path", "put", "s7", "outputs", "a.txt", "--operation-id", new_id()],
    }
    observed = {}
    for name, arguments in cases.items():
        call = stack.run(f"s7-{name}", arguments)
        observed[name] = stack.failure(call, "invalid_input", "fix_input")
        require(call.connections == 0 and not call.requests, f"s7: {name} reaches no owner",
                [call.connections, call.requests])
    return observed


SCENARIOS = {
    "s1_receipt_lost_same_id_retry": receipt_lost_same_id_retry,
    "s2_owner_restart_same_id_retry": owner_restart_same_id_retry,
    "s3_conflicting_writer_expected_generation": conflicting_writer_expected_generation,
    "s4_same_id_other_payload": same_id_other_payload,
    "s5_unknown_outcome_query_then_retry": unknown_outcome_query_then_retry,
    "s7_usage_error_sends_no_request": usage_error_sends_no_request,
    "s9_workbench_tool_operation_id_replays": workbench_tool_operation_id_replays,
    "s10_session_backoff_bridges_owner_restart": session_backoff_bridges_owner_restart,
    "s11_in_flight_identified_write_survives_owner_restart": in_flight_identified_write_survives_owner_restart,
}


def run_scenario(stack: Stack, scenario: Callable[[Stack], dict]) -> dict[str, Any]:
    try:
        return {"status": "PASS", "evidence": scenario(stack)}
    except ContractFailure as failure:
        return {"status": "FAIL", "reason": str(failure)}
    except NotQualified as gap:
        return {"status": "NOT QUALIFIED", "reason": str(gap)}
    finally:
        # A scenario that stopped mid-fault leaves the owner down.
        if stack.owner is None or stack.owner.poll() is not None:
            stack.start_owner()


def verdict(scenarios: dict[str, dict], expect: str) -> str:
    statuses = [result["status"] for result in scenarios.values()]
    wanted = "PASS" if expect == "pass" else "FAIL"
    return "PASS" if statuses and all(status == wanted for status in statuses) else "FAIL"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--nokv-bin", required=True)
    parser.add_argument("--evidence-dir", required=True)
    parser.add_argument("--object-bucket", required=True)
    endpoint = parser.add_mutually_exclusive_group(required=True)
    endpoint.add_argument("--object-endpoint", help="an existing S3 endpoint; AWS_* credentials from the environment")
    endpoint.add_argument("--moto-server", help="start this moto_server with generated credentials")
    parser.add_argument("--object-root")
    parser.add_argument("--timeout", type=float, default=45)
    parser.add_argument("--expect", choices=("pass", "red"), default="pass",
                        help="red: every scenario must fail, as on a binary without the contract")
    parser.add_argument("--scenario", action="append", choices=sorted(SCENARIOS))
    args = parser.parse_args()
    evidence = Path(args.evidence_dir).resolve()
    evidence.mkdir(parents=True, exist_ok=False)
    binary = Path(args.nokv_bin).resolve()
    report: dict[str, Any] = {
        "schema": SCHEMA,
        "expect": args.expect,
        "binary": {
            "path": str(binary),
            "sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
            "version": subprocess.run([str(binary), "version", "--json"], capture_output=True, text=True).stdout,
        },
        "source": subprocess.run(["git", "-C", str(REPOSITORY), "rev-parse", "HEAD"],
                                 capture_output=True, text=True).stdout.strip(),
        "scenarios": {},
        "not_qualified": NOT_QUALIFIED_SCENARIOS,
    }
    stack = None
    try:
        stack = Stack(args, evidence)
        stack.proxy.start()
        stack.provision()
        stack.start_owner()
        for name in args.scenario or SCENARIOS:
            report["scenarios"][name] = run_scenario(stack, SCENARIOS[name])
            print(f"{name}: {report['scenarios'][name]['status']}", flush=True)
        report["overall_status"] = verdict(report["scenarios"], args.expect)
    except NotQualified as gap:
        report["overall_status"] = "NOT QUALIFIED"
        report["reason"] = str(gap)
    finally:
        if stack is not None:
            stack.close()
        (evidence / "qualification.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(f"overall: {report['overall_status']}")
    return 0 if report["overall_status"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
