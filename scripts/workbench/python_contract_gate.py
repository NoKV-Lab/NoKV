#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0
"""Direct Python SDK contract qualification on one host.

The gate runs the stack of the native CLI contract gate: two roots on a
`--control-dir` control store and one owner behind a frame-level proxy, with
objects on an existing S3 endpoint or on a moto server the gate starts. Every
scenario drives the SDK installed in `--python-executable`, one fresh
interpreter per call, and grades its results and exceptions against
docs/cli-contract.md. Durable state is read back through the native `nokv`
binary. A scenario that injects a fault first proves the fault fired; if it
did not, the scenario is NOT QUALIFIED rather than PASS.

With `--tls` the gate runs the workspace RPC TLS gate's stack instead: the
two roots belong to one tenant, the owner serves them over TLS, the SDK
presents the tenant's writer certificate through `TlsConfig`, and every Agent
process reads objects through a read-only endpoint. The frame-level proxy
cannot read TLS records, so the two scenarios that depend on it (the dropped
reply, and the request count of a usage error) are not applicable there.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable

import cli_contract_gate as cli
from cli_contract_gate import ContractFailure, NotQualified, new_id, require
from cli_contract_gate import Stack
from rpc_tls_gate import SERVER_NAME, TlsStack

SCHEMA = "nokv.python_contract_gate.v1"
# Attributes every SDK exception is graded on.
FAILURE_FIELDS = (
    "error_class",
    "next_action",
    "operation_id",
    "attempt",
    "current_generation",
    "code",
    "cause_code",
)
# Call metadata a replay or a later observation may change.
VOLATILE = {"replayed", "commit_version"}

# The SDK program and the session program share this prologue.
PROLOGUE = r"""
import json, os, sys
from pathlib import Path
import nokv
request = json.load(sys.stdin)
stack = request["stack"]
objects = None if request.get("metadata_only") else nokv.ObjectStoreConfig.s3(
    stack["bucket"], region=stack["region"], root=stack["root"],
    endpoint=stack["endpoint"], access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
)
tls = nokv.TlsConfig(
    Path(stack["tls"]["ca"]).read_bytes(), Path(stack["tls"]["cert"]).read_bytes(),
    Path(stack["tls"]["key"]).read_bytes(), stack["tls"]["server_name"],
) if stack.get("tls") else None
"""

PYTHON_PROGRAM = PROLOGUE + r"""
try:
    client = nokv.Client(
        stack["root_id"], nokv.RoutingConfig.local(stack["control_dir"]),
        object_store=objects, workbench_root=stack["workbench_root"], tls=tls,
    )
    arguments = dict(request["arguments"])
    if "data_hex" in arguments:
        arguments["data"] = bytes.fromhex(arguments.pop("data_hex"))
    method = request["method"]
    result = {} if method == "connect" else getattr(client, method)(**arguments)
    print(json.dumps({"status": "success", "result": result}))
except Exception as error:
    print(json.dumps({
        "status": "error", "type": type(error).__name__, "message": str(error),
        **{field: getattr(error, field, None) for field in request["fields"]},
    }))
"""

# One session, a client per root, interleaved writes, and the Workbench root
# each client learned from the control plane.
SESSION_PROGRAM = PROLOGUE + r"""
import threading
session = nokv.Session(nokv.RoutingConfig.local(stack["control_dir"]), object_store=objects, tls=tls)
outcome = {"roots": {}, "mismatch": None}
clients = {root: session.client(root) for root in request["roots"]}
for root, client in clients.items():
    outcome["roots"][root] = {"workbench_root": client.workbench_root,
                              "session_workbench_root": session.workbench_root(root), "puts": [], "stats": []}
def work(root, client):
    client.create_workspace(request["workbench"])
    for index, operation_id in enumerate(request["operation_ids"][root]):
        put = client.put_bytes(request["workbench"], f"outputs/file-{index}.bin", bytes.fromhex(request["body_hex"]) + root.encode(), operation_id)
        outcome["roots"][root]["puts"].append({k: put.get(k) for k in ("status", "state", "generation", "replayed")})
    for index in range(len(request["operation_ids"][root])):
        outcome["roots"][root]["stats"].append(client.stat(request["workbench"], f"outputs/file-{index}.bin"))
workers = [threading.Thread(target=work, args=(root, client)) for root, client in clients.items()]
for worker in workers:
    worker.start()
for worker in workers:
    worker.join()
try:
    session.client(request["roots"][0], workbench_root="/agents/other/wb")
except ValueError as error:
    outcome["mismatch"] = {"message": str(error), "error_class": getattr(error, "error_class", None)}
print(json.dumps({"status": "success", "result": outcome}))
"""

IDENTITY_PROGRAM = r"""
import hashlib, json, sys
from pathlib import Path
import nokv
from nokv import _native
print(json.dumps({
    "python": sys.executable, "module": nokv.__file__, "version": nokv.__version__,
    "native_sha256": hashlib.sha256(Path(_native.__file__).read_bytes()).hexdigest(),
}))
"""


class PythonStack(Stack):
    """The CLI gate's stack with a second root, driven through the installed
    Python SDK."""

    def __init__(self, args: argparse.Namespace, evidence: Path) -> None:
        super().__init__(args, evidence)
        # A virtual environment's interpreter is a symlink to its base; the
        # link itself selects the environment, so it is not resolved.
        self.python = Path(args.python_executable).absolute()
        self.ids["other_root"] = new_id()
        objects = dict(zip(self.sdk_objects()[::2], self.sdk_objects()[1::2]))
        self.sdk_stack = {
            "root_id": self.ids["root"],
            "roots": [self.ids["root"], self.ids["other_root"]],
            "control_dir": str(self.control),
            "workbench_root": cli.WORKBENCH_ROOT,
            "bucket": objects["--object-bucket"],
            "endpoint": objects["--object-endpoint"],
            "root": objects["--object-root"],
            "region": objects["--object-region"],
        }
        self.connect_requests: list[str] | None = None

    def sdk_objects(self) -> list[str]:
        """The object-store options the SDK program runs with."""
        return self.objects

    def session(self, label: str, workbench: str, operation_ids: dict[str, list[str]], body: bytes) -> dict[str, Any]:
        """The session program, in a fresh interpreter."""
        request = {"stack": self.sdk_stack, "workbench": workbench, "roots": self.sdk_stack["roots"],
                   "operation_ids": operation_ids, "body_hex": body.hex()}
        started = time.monotonic()
        done = subprocess.run(
            [str(self.python), "-c", SESSION_PROGRAM],
            input=json.dumps(request), capture_output=True, text=True, env=self.env, timeout=self.timeout * 4,
        )
        self.write("python-transcript.jsonl", {
            "label": label, "method": "session", "exit": done.returncode, "stdout": done.stdout,
            "stderr": done.stderr[-2000:], "elapsed_ms": round((time.monotonic() - started) * 1000, 1),
        })
        require(done.returncode == 0, f"{label}: the session program reports its outcome instead of crashing",
                [done.returncode, done.stderr[-800:]])
        call = json.loads(done.stdout)
        call["label"] = label
        return call

    def sdk_identity(self) -> dict[str, Any]:
        done = subprocess.run(
            [str(self.python), "-c", IDENTITY_PROGRAM],
            capture_output=True, text=True, env=self.env, timeout=self.timeout,
        )
        if done.returncode != 0:
            raise NotQualified(f"the Python SDK is not importable: {done.stderr[-600:]}")
        return json.loads(done.stdout)

    def sdk(
        self, label: str, method: str, *, metadata_only: bool = False, **arguments: Any
    ) -> dict[str, Any]:
        """One SDK call in a fresh interpreter, with the owner requests it sent.
        A metadata-only client is constructed without an object store."""
        request = {
            "stack": self.sdk_stack,
            "method": method,
            "arguments": arguments,
            "fields": FAILURE_FIELDS,
            "metadata_only": metadata_only,
        }
        with self.proxy._lock:
            first = len(self.proxy.requests)
        started = time.monotonic()
        done = subprocess.run(
            [str(self.python), "-c", PYTHON_PROGRAM],
            input=json.dumps(request), capture_output=True, text=True,
            env=self.env, timeout=self.timeout * 4,
        )
        with self.proxy._lock:
            requests = self.proxy.requests[first:]
        self.write(
            "python-transcript.jsonl",
            {
                "label": label,
                "method": method,
                "arguments": arguments,
                "exit": done.returncode,
                "stdout": done.stdout,
                "stderr": done.stderr[-2000:],
                "owner_requests": requests,
                "elapsed_ms": round((time.monotonic() - started) * 1000, 1),
            },
        )
        require(
            done.returncode == 0,
            f"{label}: the SDK call reports its outcome instead of crashing",
            [done.returncode, done.stderr[-600:]],
        )
        try:
            call = json.loads(done.stdout)
        except json.JSONDecodeError:
            raise ContractFailure(f"{label} prints one JSON outcome: {done.stdout[:300]}")
        call["label"] = label
        call["requests"] = requests
        return call

    def sdk_faulted(self, label: str, operation: str, method: str, **arguments: Any) -> dict[str, Any]:
        """An SDK call whose successful `operation` response is dropped and
        whose owner is killed, so no in-process recovery can observe it."""
        self.proxy.arm(operation, self.kill_owner)
        call = self.sdk(label, method, **arguments)
        if not self.proxy.dropped():
            self.proxy.disarm()
            if call.get("type") in ("AttributeError", "TypeError"):
                raise ContractFailure(f"{label}: the SDK does not accept the call: {call.get('message')}")
            raise NotQualified(f"{label}: no successful {operation} was sent: {call['requests']}")
        self.proxy.raise_if_failed()
        return call

    def sdk_success(self, call: dict[str, Any]) -> dict[str, Any]:
        require(call.get("status") == "success", f"{call['label']} succeeds", call)
        return call["result"]

    def sdk_failure(
        self, call: dict[str, Any], klass: str, next_action: str, kind: str | None = None
    ) -> dict[str, Any]:
        """The call raised an exception carrying exactly `klass` and `next_action`."""
        require(call.get("status") == "error", f"{call['label']} fails", call)
        require(call.get("error_class") in self.exits, f"{call['label']} names a published class", call)
        require(call.get("next_action") in self.actions, f"{call['label']} names a published next action", call)
        require(
            [call["error_class"], call["next_action"]] == [klass, next_action],
            f"{call['label']} is {klass} with {next_action}",
            call,
        )
        require(kind is None or call.get("type") == kind, f"{call['label']} raises {kind}", call)
        return call

    def baseline_requests(self) -> list[str]:
        """The requests constructing a client sends before any call."""
        if self.connect_requests is None:
            connected = self.sdk("connect", "connect")
            self.sdk_success(connected)
            self.connect_requests = connected["requests"]
        return self.connect_requests

    def sdk_put(self, label: str, workbench: str, path: str, operation_id: str, body: bytes,
                *, metadata_only: bool = False, **options: Any) -> dict[str, Any]:
        return self.sdk(
            label, "put_bytes", metadata_only=metadata_only, workbench=workbench,
            path=f"outputs/{path}", data_hex=body.hex(), operation_id=operation_id, **options,
        )

    def prepare(self) -> None:
        """Provision both roots; the owner starts afterwards."""
        self.provision()
        self.provision(self.ids["other_root"])


class TlsPythonStack(PythonStack, TlsStack):
    """The TLS gate's stack, driven through the installed Python SDK as the
    tenant's writer principal, with both roots bound to the tenant. The SDK
    reads objects the way an Agent does, through the read-only endpoint; its
    writes go through the owner."""

    def __init__(self, args: argparse.Namespace, evidence: Path) -> None:
        super().__init__(args, evidence)
        certificate, key = self.clients["agent"]
        self.sdk_stack["tls"] = {
            "ca": str(self.pki.directory / "ca.pem"),
            "cert": str(certificate),
            "key": str(key),
            "server_name": SERVER_NAME,
        }

    def sdk_objects(self) -> list[str]:
        return self.agent_objects

    def prepare(self) -> None:
        super().prepare()
        self.register_tenant()

    def register_tenant(self) -> None:
        """One tenant that owns both roots, with the writer principal whose
        certificate the SDK presents."""
        self.identity("tenant", "tenant", "create", self.tenant)
        self.identity("principal-agent", "principal", "create", self.tenant, self.principals["agent"])
        self.register("agent", "agent")
        self.role("grant", "agent", "writer", ["--tenant-wide"])
        for root in self.sdk_stack["roots"]:
            self.identity(f"bind-{root[:8]}", "root", "bind", root, self.tenant)

def stable(result: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in result.items() if key not in VOLATILE}


def same_report(python: dict[str, Any], native: dict[str, Any]) -> bool:
    """The SDK and the CLI report exactly the same fields and values, apart
    from call metadata."""
    return stable(python) == stable(native)


def put_replays_across_processes_owners_and_surfaces(stack: PythonStack) -> dict[str, Any]:
    """P1: a put replays its receipt to the same id and inputs from another
    process, from a client without an object store, after an owner restart,
    and through the CLI in both directions."""
    workbench = "p1-put-replay"
    stack.workbench(workbench)
    evidence = {}
    for variant, body in (
        ("inline", b"p1 inline\n"),
        ("object", b"o" * (cli.MAX_INLINE_BODY_BYTES + 1)),
    ):
        operation_id, path = new_id(), f"{variant}.bin"
        first = stack.sdk_success(stack.sdk_put(f"p1-{variant}", workbench, path, operation_id, body))
        require(
            first.get("status") == "success"
            and first.get("operation") == "put"
            and first.get("operation_id") == operation_id
            and first.get("state") == "committed"
            and first.get("next_action") == "none"
            and first.get("replayed") is False
            and first.get("generation") == 1,
            f"p1: the {variant} put publishes once",
            first,
        )
        again = stack.sdk_success(stack.sdk_put(f"p1-{variant}-again", workbench, path, operation_id, body))
        # A replay reads metadata only, so a client without an object store
        # returns the receipt too.
        metadata_only = stack.sdk_success(
            stack.sdk_put(f"p1-{variant}-metadata-only", workbench, path, operation_id, body,
                          metadata_only=True)
        )
        stack.restart_owner()
        restarted = stack.sdk_success(
            stack.sdk_put(f"p1-{variant}-restarted", workbench, path, operation_id, body)
        )
        native = stack.success(stack.put(f"p1-{variant}-cli", workbench, path, operation_id, body))
        for label, replay in (
            ("process", again),
            ("client without an object store", metadata_only),
            ("owner restart", restarted),
        ):
            require(
                replay.get("replayed") is True and stable(replay) == stable(first),
                f"p1: the {variant} receipt replays across a {label}",
                [first, replay],
            )
        require(
            native.get("replayed") is True and same_report(first, native),
            f"p1: the CLI replays the {variant} receipt the SDK published",
            [first, native],
        )
        stack.assert_live(f"p1-{variant}", workbench, path, body, first)
        evidence[variant] = {"first": first, "replays": [again, metadata_only, restarted, native]}
    reverse_id, reverse_body = new_id(), b"p1 published by the CLI\n"
    published = stack.success(stack.put("p1-reverse-cli", workbench, "reverse.bin", reverse_id, reverse_body))
    replay = stack.sdk_success(stack.sdk_put("p1-reverse", workbench, "reverse.bin", reverse_id, reverse_body))
    require(
        replay.get("replayed") is True and same_report(replay, published),
        "p1: the SDK replays the receipt the CLI published",
        [published, replay],
    )
    evidence["reverse"] = {"cli": published, "python": replay}
    return evidence


def lost_reply_is_queried_then_replayed(stack: PythonStack) -> dict[str, Any]:
    """P2: a put whose reply is lost is outcome_unknown with query_same; the
    query reports the stored receipt and the same put replays it."""
    workbench, operation_id, body = "p2-lost-reply", new_id(), b"p2 event\n"
    stack.workbench(workbench)
    failed = stack.sdk_faulted(
        "p2-first", "publish_inline_artifact", "put_bytes", workbench=workbench,
        path="outputs/p2.txt", data_hex=body.hex(), operation_id=operation_id,
    )
    stack.sdk_failure(failed, "outcome_unknown", "query_same")
    require(failed.get("operation_id") == operation_id, "p2: the failure names its operation", failed)
    stack.start_owner()
    status = stack.sdk_success(stack.sdk("p2-status", "operation_status", operation_id=operation_id))
    receipt = status.get("receipt") or {}
    require(
        status.get("operation") == "publish"
        and status.get("state") == "succeeded"
        and status.get("next_action") == "none"
        and receipt.get("generation") == 1
        and receipt.get("operation_id") == operation_id,
        "p2: the query reports the committed receipt",
        status,
    )
    native = stack.success(stack.run("p2-cli-status", ["operation", "status", operation_id]))
    require(same_report(status, native), "p2: the SDK and the CLI report one status", [status, native])
    retry = stack.sdk_success(stack.sdk_put("p2-retry", workbench, "p2.txt", operation_id, body))
    require(
        retry.get("replayed") is True
        and retry.get("artifact_revision_id") == receipt.get("artifact_revision_id"),
        "p2: the resubmission replays the queried receipt",
        [retry, receipt],
    )
    absent = stack.sdk_failure(
        stack.sdk("p2-absent", "operation_status", operation_id=new_id()),
        "not_found", "resubmit_same", "AppendError",
    )
    stack.assert_live("p2", workbench, "p2.txt", body, retry)
    return {"failure": failed, "status": status, "replay": retry, "absent": absent}


def identity_reuse_for_other_inputs_changes_nothing(stack: PythonStack) -> dict[str, Any]:
    """P3: an operation id reused for another body or path is
    identity_conflict and publishes nothing."""
    workbench = "p3-identity-conflict"
    stack.workbench(workbench)
    evidence = {}
    for variant, size in (("inline", 32), ("object", cli.MAX_INLINE_BODY_BYTES + 1)):
        operation_id, path = new_id(), f"{variant}.bin"
        original, other = b"a" * size, b"b" * size
        first = stack.sdk_success(stack.sdk_put(f"p3-{variant}", workbench, path, operation_id, original))
        conflicts = [
            stack.sdk_failure(
                stack.sdk_put(f"p3-{variant}-other-body", workbench, path, operation_id, other),
                "identity_conflict", "do_not_retry",
            ),
            stack.sdk_failure(
                stack.sdk_put(f"p3-{variant}-other-path", workbench, "elsewhere.bin", operation_id, original),
                "identity_conflict", "do_not_retry",
            ),
        ]
        for conflict in conflicts:
            require(conflict.get("operation_id") == operation_id, "p3: the conflict names its operation", conflict)
        status = stack.sdk_success(stack.sdk(f"p3-{variant}-status", "operation_status", operation_id=operation_id))
        require(
            status.get("state") == "succeeded"
            and (status.get("receipt") or {}).get("body_digest") == first["body_digest"],
            f"p3: the {variant} operation keeps its original receipt",
            [status, first],
        )
        stack.assert_live(f"p3-{variant}", workbench, path, original, first)
        absent = stack.stat(f"p3-{variant}-elsewhere", workbench, "elsewhere.bin")
        require(absent.get("exists") is False, "p3: a refused identity publishes nothing", absent)
        evidence[variant] = {"conflicts": conflicts, "status": status}
    return evidence


def stale_generation_and_fences_fail_typed(stack: PythonStack) -> dict[str, Any]:
    """P4: a stale generation, a create-only collision, and a refused
    incarnation fence are precondition_failed and apply nothing."""
    workbench = "p4-precondition"
    stack.workbench(workbench)
    created = stack.sdk_success(stack.sdk_put("p4-create", workbench, "a.txt", new_id(), b"A"))
    winner = stack.sdk_success(
        stack.sdk_put("p4-winner", workbench, "a.txt", new_id(), b"B",
                      expected_generation=created["generation"])
    )
    stale_id = new_id()
    stale = stack.sdk_failure(
        stack.sdk_put("p4-stale", workbench, "a.txt", stale_id, b"C",
                      expected_generation=created["generation"]),
        "precondition_failed", "reread_then_retry",
    )
    require(
        stale.get("current_generation") == winner["generation"] and stale.get("operation_id") == stale_id,
        "p4: the refusal names the current generation and its operation",
        stale,
    )
    create_only = stack.sdk_failure(
        stack.sdk_put("p4-create-again", workbench, "a.txt", new_id(), b"D"),
        "precondition_failed", "reread_then_retry",
    )
    require(
        create_only.get("current_generation") == winner["generation"],
        "p4: a create-only refusal names the current generation",
        create_only,
    )
    fenced = stack.sdk_failure(
        stack.sdk_put("p4-fenced", workbench, "a.txt", new_id(), b"E",
                      expected_generation=winner["generation"],
                      expected_workspace_incarnation_id="fe" * 16),
        "precondition_failed", "reread_then_retry", "WorkspaceIncarnationMismatch",
    )
    absent = stack.sdk_failure(
        stack.sdk("p4-stale-status", "operation_status", operation_id=stale_id),
        "not_found", "resubmit_same",
    )
    stack.assert_live("p4", workbench, "a.txt", b"B", winner)
    return {"stale": stale, "create_only": create_only, "fenced": fenced, "absent": absent}


def one_session_serves_two_roots_with_the_bound_workbench_root(stack: PythonStack) -> dict[str, Any]:
    """P7: one Session derives a client per root of the tenant; both learn the
    Workbench root the control plane binds, write and read back concurrently
    through one connection pool, and a workbench_root that disagrees with the
    binding is refused."""
    workbench = "p7-session"
    operation_ids = {root: [new_id() for _ in range(4)] for root in stack.sdk_stack["roots"]}
    body = b"p7 session body\n"
    outcome = stack.sdk_success(stack.session("p7", workbench, operation_ids, body))
    for root in stack.sdk_stack["roots"]:
        result = outcome["roots"][root]
        require(result["workbench_root"] == cli.WORKBENCH_ROOT and result["session_workbench_root"] == cli.WORKBENCH_ROOT,
                f"p7: the client of {root[:8]} learned the bound Workbench root", result)
        require(len(result["puts"]) == 4 and all(put["state"] == "committed" and put["replayed"] is False and put["generation"] == 1
                                                 for put in result["puts"]),
                f"p7: every put on {root[:8]} committed once", result["puts"])
        require(all(stat.get("generation") == 1 and stat.get("path") == f"outputs/file-{index}.bin"
                    for index, stat in enumerate(result["stats"])),
                f"p7: every path on {root[:8]} reads back", result["stats"])
        # The CLI, as the same principal, reads the session's writes on each root.
        for index in range(4):
            native = stack.success(stack.run(f"p7-stat-{root[:8]}-{index}",
                                             ["workspace-path", "stat", workbench, "outputs", f"file-{index}.bin"], root=root))
            require(native.get("exists") is True and native.get("generation") == 1,
                    f"p7: the CLI sees the session's write on {root[:8]}", native)
    require(outcome["mismatch"] and outcome["mismatch"]["error_class"] == "invalid_input"
            and "differs from the Workbench root" in outcome["mismatch"]["message"],
            "p7: a workbench_root that disagrees with the binding is refused", outcome["mismatch"])
    return outcome


def invalid_input_is_fixed_before_any_request(stack: PythonStack) -> dict[str, Any]:
    """P5: an invalid call fails with invalid_input and fix_input before any
    request beyond the client's own connection, including an append, which
    reported query_same before the SDK carried the contract."""
    workbench = "p5-invalid-input"
    stack.workbench(workbench)
    baseline = sorted(stack.baseline_requests())
    cases = {
        "put-operation-id": ("ValueError", "put_bytes", {
            "workbench": workbench, "path": "outputs/a.txt", "data_hex": "00",
            "operation_id": "XYZ"}),
        "put-path": ("ValueError", "put_bytes", {
            "workbench": workbench, "path": "/outputs/a.txt", "data_hex": "00",
            "operation_id": new_id()}),
        "append-path": ("AppendError", "append_bytes", {
            "workbench": workbench, "path": "/logs/a.log", "data_hex": "00",
            "operation_id": new_id()}),
        "status-two-identities": ("ValueError", "operation_status", {
            "operation_id": new_id(), "commit_identity": "ab" * 32}),
        "status-commit-identity": ("ValueError", "operation_status", {
            "commit_identity": "not-hex"}),
    }
    observed = {}
    for name, (kind, method, arguments) in cases.items():
        call = stack.sdk(f"p5-{name}", method, **arguments)
        observed[name] = stack.sdk_failure(call, "invalid_input", "fix_input", kind)
        require(
            sorted(call["requests"]) == baseline,
            f"p5: {name} sends nothing beyond connecting",
            [call["requests"], baseline],
        )
    return observed


def commit_and_restore_report_their_stored_receipts(stack: PythonStack) -> dict[str, Any]:
    """P6: a commit is observed by its commit identity and a restore by its
    operation id, each with its stored receipt, as the CLI reports them."""
    source, destination = "p6-source", "p6-restored"
    stack.workbench(source)
    stack.success(stack.put("p6-seed", source, "seed.txt", new_id(), b"seed\n"))
    arguments = {
        "workbench": source,
        "manifest": {"gate": "python-contract", "scenario": "p6"},
        "content_digest_uri": "sha256:" + "ab" * 32,
    }
    commit = stack.sdk_success(stack.sdk("p6-commit", "commit", **arguments))
    replay = stack.sdk_success(stack.sdk("p6-commit-replay", "commit", **arguments))
    require(
        replay.get("replayed") is True and replay.get("commit_id") == commit.get("commit_id"),
        "p6: the same commit content replays",
        [commit, replay],
    )
    commit_identity = commit["commit_id"]
    status = stack.sdk_success(
        stack.sdk("p6-commit-status", "operation_status", commit_identity=commit_identity)
    )
    require(
        status.get("operation") == "commit"
        and status.get("state") == "succeeded"
        and status.get("next_action") == "none"
        and (status.get("receipt") or {}).get("commit_identity") == commit_identity
        and (status.get("receipt") or {}).get("workbench_id") == source,
        "p6: the commit identity observes the stored commit receipt",
        status,
    )
    native = stack.success(
        stack.run("p6-cli-commit-status", ["operation", "status", "--commit-identity", commit_identity])
    )
    require(same_report(status, native), "p6: the SDK and the CLI report one commit status", [status, native])
    restored = stack.sdk_success(
        stack.sdk("p6-restore", "restore", workbench=source, destination=destination, at_commit=commit_identity)
    )
    restore_status = stack.sdk_success(
        stack.sdk("p6-restore-status", "operation_status", operation_id=restored["operation_id"])
    )
    require(
        restore_status.get("operation") == "restore"
        and restore_status.get("state") == "succeeded"
        and restore_status.get("next_action") == "none"
        and (restore_status.get("receipt") or {}).get("destination_workbench_id") == destination,
        "p6: the restore operation reports its stored receipt",
        restore_status,
    )
    native_restore = stack.success(
        stack.run("p6-cli-restore-status", ["operation", "status", restored["operation_id"]])
    )
    require(
        same_report(restore_status, native_restore),
        "p6: the SDK and the CLI report one restore status",
        [restore_status, native_restore],
    )
    require(stack.body("p6-restored-seed", destination, "seed.txt") == b"seed\n",
            "p6: the restored workbench holds the committed body")
    return {"commit": commit, "status": status, "restore": restored, "restore_status": restore_status}


# Graded through the frame-level proxy, which a TLS owner bypasses.
PROXY_SCENARIOS = {"p2_lost_reply_is_queried_then_replayed", "p5_invalid_input_is_fixed_before_any_request"}

SCENARIOS: dict[str, Callable[[PythonStack], dict[str, Any]]] = {
    "p1_put_replays_across_processes_owners_and_surfaces": put_replays_across_processes_owners_and_surfaces,
    "p2_lost_reply_is_queried_then_replayed": lost_reply_is_queried_then_replayed,
    "p3_identity_reuse_for_other_inputs_changes_nothing": identity_reuse_for_other_inputs_changes_nothing,
    "p4_stale_generation_and_fences_fail_typed": stale_generation_and_fences_fail_typed,
    "p5_invalid_input_is_fixed_before_any_request": invalid_input_is_fixed_before_any_request,
    "p6_commit_and_restore_report_their_stored_receipts": commit_and_restore_report_their_stored_receipts,
    "p7_one_session_serves_two_roots_with_the_bound_workbench_root":
        one_session_serves_two_roots_with_the_bound_workbench_root,
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--nokv-bin", required=True)
    parser.add_argument("--python-executable", required=True,
                        help="an interpreter with the candidate NoKV wheel installed")
    parser.add_argument("--evidence-dir", required=True)
    parser.add_argument("--object-bucket", required=True)
    endpoint = parser.add_mutually_exclusive_group(required=True)
    endpoint.add_argument("--object-endpoint", help="an existing S3 endpoint; AWS_* credentials from the environment")
    endpoint.add_argument("--moto-server", help="start this moto_server with generated credentials")
    parser.add_argument("--object-root")
    parser.add_argument("--timeout", type=float, default=45)
    parser.add_argument("--scenario", action="append", choices=sorted(SCENARIOS))
    parser.add_argument("--tls", action="store_true",
                        help="serve over TLS and drive the SDK as the tenant's writer principal")
    args = parser.parse_args()
    evidence = Path(args.evidence_dir).resolve()
    evidence.mkdir(parents=True, exist_ok=False)
    binary = Path(args.nokv_bin).resolve()
    report: dict[str, Any] = {
        "schema": SCHEMA,
        "binary": {
            "path": str(binary),
            "sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
            "version": subprocess.run([str(binary), "version", "--json"], capture_output=True, text=True).stdout,
        },
        "source": subprocess.run(["git", "-C", str(cli.REPOSITORY), "rev-parse", "HEAD"],
                                 capture_output=True, text=True).stdout.strip(),
        "scenarios": {},
    }
    stack = None
    try:
        stack = (TlsPythonStack if args.tls else PythonStack)(args, evidence)
        report["python_sdk"] = stack.sdk_identity()
        report["tls"] = args.tls
        stack.proxy.start()
        stack.prepare()
        stack.start_owner()
        applicable = [name for name in SCENARIOS if not (args.tls and name in PROXY_SCENARIOS)]
        report["not_applicable"] = {name: "needs the frame-level proxy, which cannot read TLS records"
                                    for name in SCENARIOS if name not in applicable}
        for name in args.scenario or applicable:
            report["scenarios"][name] = cli.run_scenario(stack, SCENARIOS[name])
            print(f"{name}: {report['scenarios'][name]['status']}", flush=True)
        report["overall_status"] = cli.verdict(report["scenarios"], "pass")
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
