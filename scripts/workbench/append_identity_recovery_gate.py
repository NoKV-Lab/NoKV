#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0
"""Native CLI append identity qualification using real etcd, RustFS and Holt.

This gate starts only isolated services and keeps every RPC fault and CLI result.
The baseline mode must reproduce response-loss duplication; qualified mode checks
caller-stable identities. A local owner SIGKILL does not qualify shared recovery.
Binary build identity and current source identity are recorded independently.
"""

from __future__ import annotations
import argparse
import base64
import concurrent.futures
import dataclasses
import hashlib
import json
from pathlib import Path
import shutil
import socket
import struct
import subprocess
import sys
import threading
import time
import traceback
import uuid

import live_workbench as live
import object_namespace_recovery_gate as infra
from restore_composition_gate import PINNED_RUSTFS_IMAGE
from fork_restore_recovery_gate import (
    ResponseDropProxy,
    _recv_exact,
    is_handshake_frame,
)


def unpack(data):
    """Small read-only MessagePack decoder; rejects unsupported types/trailing bytes."""
    offset = 0

    def take(n):
        nonlocal offset
        b = data[offset : offset + n]
        offset += n
        if len(b) != n:
            raise ValueError("truncated MessagePack")
        return b

    def number(n, signed=False):
        return int.from_bytes(take(n), "big", signed=signed)

    def read():
        tag = number(1)
        if tag < 0x80:
            return tag
        if tag >= 0xE0:
            return tag - 256
        if 0xA0 <= tag <= 0xBF:
            return take(tag & 31).decode()
        if 0x90 <= tag <= 0x9F:
            return [read() for _ in range(tag & 15)]
        if 0x80 <= tag <= 0x8F:
            return {read(): read() for _ in range(tag & 15)}
        if tag == 0xC0:
            return None
        if tag in (0xC2, 0xC3):
            return tag == 0xC3
        if tag in (0xCC, 0xCD, 0xCE, 0xCF):
            return number(1 << (tag - 0xCC))
        if tag in (0xD0, 0xD1, 0xD2, 0xD3):
            return number(1 << (tag - 0xD0), True)
        if tag in (0xCA, 0xCB):
            return struct.unpack(
                ">f" if tag == 0xCA else ">d", take(4 if tag == 0xCA else 8)
            )[0]
        if tag in (0xD9, 0xDA, 0xDB):
            return take(number(1 << (tag - 0xD9))).decode()
        if tag in (0xC4, 0xC5, 0xC6):
            return {"binary_hex": take(number(1 << (tag - 0xC4))).hex()}
        if tag in (0xDC, 0xDD):
            return [read() for _ in range(number(2 if tag == 0xDC else 4))]
        if tag in (0xDE, 0xDF):
            return {read(): read() for _ in range(number(2 if tag == 0xDE else 4))}
        raise ValueError(f"unsupported MessagePack marker {tag:#x}")

    value = read()
    if offset != len(data):
        raise ValueError("trailing MessagePack bytes")
    return value


class EvidenceDropProxy(ResponseDropProxy):
    def __init__(self, port, timeout, stack):
        super().__init__(port, timeout)
        self.stack = stack
        self.capture_rpc = False
        self.last_drop = None
        self.handshake_request = None
        self.wire_schema = None
        self.latest_route = None

    def wait_dropped(self):
        # Called after the only faulted CLI process has exited. At that point
        # no future RPC can reach this barrier; allow only thread handoff time.
        if not self._dropped.wait(1):
            raise infra.WorkflowFailure(
                f"append exited before the armed successful {self._drop_operation} response; "
                "inspect native-append-transcript.jsonl and publication-rpc-trace.jsonl"
            )
        self.raise_if_failed()

    def _forward(self, client):
        try:
            client.settimeout(self.timeout)
            with self._lock:
                target = self._target
            if target is None:
                raise ConnectionError("proxy target missing")
            with socket.create_connection(target, timeout=self.timeout) as owner:
                owner.settimeout(self.timeout)
                while True:
                    first = client.recv(4)
                    if not first:
                        return
                    header = first + _recv_exact(client, 4 - len(first))
                    request = _recv_exact(client, int.from_bytes(header, "big"))
                    owner.sendall(header + request)
                    rh = _recv_exact(owner, 4)
                    response = _recv_exact(owner, int.from_bytes(rh, "big"))
                    callback = None
                    if is_handshake_frame(request):
                        with self._lock:
                            self.handshake_request = request
                    else:
                        req = unpack(request)
                        rsp = unpack(response)
                        with self._lock:
                            self.wire_schema = req["schema"]
                            self.latest_route = req["payload"]["route"]
                        operation = req["payload"]["operation"]["operation"]
                        if self.capture_rpc:
                            self.stack.record(
                                "publication-rpc-trace.jsonl",
                                {
                                    "at": live.now(),
                                    "operation": operation,
                                    "request": req,
                                    "response": rsp,
                                },
                            )
                        if rsp["payload"]["outcome"]["status"] == "success":
                            # Claim exactly one successful response under the arm lock.
                            # Concurrent replies must never be labelled dropped if delivered.
                            with self._lock:
                                if self._drop_operation == operation:
                                    callback = self._on_drop
                                    self._drop_operation = None
                                    self._on_drop = None
                            if callback is not None:
                                self.last_drop = {
                                    "at": live.now(),
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
        except (ConnectionError, OSError, TimeoutError, ValueError, KeyError) as error:
            with self._lock:
                armed = self._drop_operation is not None
            if armed:
                self._errors.append(repr(error))
                self._dropped.set()
        finally:
            client.close()


def materialize(s, wb, section, path, label):
    target = s.evidence_dir / (label + ".data")
    s.run([*s.client_args, "materialize", wb, section, path, str(target)], label=label)
    return target.read_bytes()


def append_loss(s):
    wb = "append-response-loss"
    marker = "effect-0001\n"
    args = {
        "id": wb,
        "section": "logs",
        "path": "effects.log",
        "text": marker,
        "content_type": "text/plain",
    }
    # Admit the workbench first so the one-shot targets only the append publication.
    s.cli("workbench_create", {"id": wb})
    s.proxy.arm("complete_artifact_publish", lambda: s.owner.kill())
    failed = s.cli("workbench_append", args, label="append-first-ack-lost", check=False)
    s.proxy.wait_dropped()
    assert failed.get("status") != "success", failed
    s.owner.wait(timeout=10)
    s.restart()
    before = materialize(
        s, wb, "logs", "effects.log", "append-after-reopen-before-retry"
    )
    stat_before = s.cli(
        "workbench_stat",
        {"id": wb, "section": "logs", "path": "effects.log"},
        label="append-stat-before-retry",
    )
    retry = s.cli("workbench_append", args, label="append-new-process-same-input-retry")
    after = materialize(s, wb, "logs", "effects.log", "append-after-caller-retry")
    assert before == marker.encode(), before
    assert after == 2 * marker.encode(), after
    return {
        "status": "RED_REPRODUCED",
        "ownership": "NoKV CLI / Workbench append identity",
        "first_call": failed,
        "durable_before_retry": before.decode(),
        "stat_before_retry": stat_before,
        "retry": retry,
        "after_retry": after.decode(),
        "effect_count_before": 1,
        "effect_count_after": 2,
        "caller_restart": True,
        "owner_sigkill_reopen": True,
        "oracle": "One logical caller effect is duplicated after its success RPC is lost; new CLI process cannot reuse durable operation identity.",
    }


def native_append(
    stack,
    workbench,
    operation_id,
    text,
    *,
    label,
    path="effects.log",
    content_type="text/plain",
    max_size=1024 * 1024,
    incarnation=None,
    payload_form="text",
    check=True,
):
    command = [
        *stack.client_args,
        "workspace-path",
        "append",
        workbench,
        "logs",
        path,
        "--operation-id",
        operation_id,
        "--max-logical-size",
        str(max_size),
    ]
    if content_type is not None:
        command += ["--content-type", content_type]
    if payload_form == "text":
        command += ["--text", text]
    elif payload_form == "base64":
        command += ["--base64", base64.b64encode(text.encode()).decode()]
    elif payload_form == "file":
        fixture = stack.evidence_dir / (label + ".input")
        fixture.write_bytes(text.encode())
        command += ["--file", str(fixture)]
    else:
        raise ValueError(f"unsupported test payload form: {payload_form}")
    if incarnation is not None:
        command += ["--expected-workspace-incarnation-id", incarnation]
    process = stack.run(command, label=label, check=False)
    raw = (
        process.stdout
        if process.returncode == 0
        else process.stderr.strip().removeprefix("nokv: ")
    )
    try:
        result = json.loads(raw)
    except json.JSONDecodeError as error:
        raise AssertionError(
            f"native append did not return structured JSON: {raw}"
        ) from error
    stack.record(
        "native-append-transcript.jsonl",
        {
            "label": label,
            "operation_id": operation_id,
            "workbench": workbench,
            "path": path,
            "payload_sha256": hashlib.sha256(text.encode()).hexdigest(),
            "payload_bytes": len(text.encode()),
            "returncode": process.returncode,
            "result": result,
        },
    )
    if check:
        assert process.returncode == 0 and result.get("status") == "success", result
    else:
        assert (process.returncode == 0) == (result.get("status") == "success"), result
    return result


def receipt(result):
    """Compare the historical publication, excluding response timing and replay flag."""
    fields = (
        "operation_id",
        "artifact_revision_id",
        "generation",
        "logical_size",
        "body_digest",
        "workspace_incarnation_id",
        "workspace_revision",
    )
    assert all(field in result for field in fields), result
    return {field: result[field] for field in fields}


def assert_rejected(result, operation_id):
    assert result.get("status") == "error", result
    assert (
        result.get("code")
        and result.get("details", {}).get("operation_id") == operation_id
    ), result


def assert_dropped_receipt(result, dropped):
    observed = dropped["response"]["payload"]["outcome"]["body"]
    assert observed["result"] == "published", observed
    published = observed["value"]
    assert result["operation_id"] == bytes(published["operation_id"]).hex(), (
        result,
        published,
    )
    assert result["path"] == published["target"]["path"], (result, published)
    assert result["workbench_id"] == published["target"]["workbench"], (
        result,
        published,
    )
    for key in ("generation", "logical_size", "body_digest", "workspace_revision"):
        assert result[key] == published[key], (result, published)
    assert (
        result["artifact_revision_id"] == bytes(published["artifact_revision_id"]).hex()
    ), (result, published)


def terminal_response_loss(stack):
    wb, operation_id, text = "stable-response-loss", "11" * 16, "logical-event-1\n"
    stack.cli("workbench_create", {"id": wb})
    stack.proxy.arm("complete_artifact_publish", lambda: stack.owner.kill())
    failed = native_append(
        stack, wb, operation_id, text, label="terminal-ack-lost", check=False
    )
    stack.proxy.wait_dropped()
    dropped = stack.proxy.last_drop
    assert failed.get("status") == "error", failed
    stack.owner.wait(timeout=10)
    stack.restart()
    before = materialize(stack, wb, "logs", "effects.log", "terminal-after-reopen")
    assert before == text.encode(), before
    replay = native_append(
        stack, wb, operation_id, text, label="terminal-new-process-replay"
    )
    assert replay["replayed"] is True, replay
    assert_dropped_receipt(replay, dropped)
    again = native_append(stack, wb, operation_id, text, label="terminal-second-replay")
    assert again["replayed"] is True and receipt(again) == receipt(replay), (
        again,
        replay,
    )
    after = materialize(stack, wb, "logs", "effects.log", "terminal-after-replays")
    assert after == before, after
    return {
        "status": "PASS",
        "first_call": failed,
        "replay": replay,
        "expected_effects": 1,
        "actual_effects": 1,
        "exact_bytes": after.decode(),
        "original_rpc_receipt_verified": True,
        "owner_sigkill_reopen": True,
    }


def identity_binding_and_history(stack):
    wb, operation_id, text = "identity-binding", "22" * 16, "first\n"
    stack.cli("workbench_create", {"id": wb})
    original = native_append(stack, wb, operation_id, text, label="binding-original")
    assert original["replayed"] is False and original["generation"] == 1, original
    for form in ("base64", "file"):
        replay = native_append(
            stack,
            wb,
            operation_id,
            text,
            label=f"binding-{form}-replay",
            payload_form=form,
        )
        assert replay["replayed"] is True and receipt(replay) == receipt(original), (
            replay
        )
    rejected = []
    for label, changed_text, changed in (
        ("body", "different\n", {}),
        ("path", text, {"path": "other.log"}),
        ("content-type", text, {"content_type": "application/octet-stream"}),
        ("limit", text, {"max_size": 1024 * 1024 + 1}),
        ("incarnation", text, {"incarnation": "fe" * 16}),
    ):
        result = native_append(
            stack,
            wb,
            operation_id,
            changed_text,
            label=f"binding-mismatch-{label}",
            check=False,
            **changed,
        )
        assert_rejected(result, operation_id)
        rejected.append({"changed": label, "result": result})
    fresh_fence = native_append(
        stack,
        wb,
        "27" * 16,
        text,
        label="fresh-id-wrong-incarnation",
        incarnation="fe" * 16,
        check=False,
    )
    assert_rejected(fresh_fence, "27" * 16)
    rejected.append(
        {"changed": "fresh-operation-incarnation-fence", "result": fresh_fence}
    )
    other = wb + "-other"
    stack.cli("workbench_create", {"id": other})
    cross_workspace = native_append(
        stack,
        other,
        operation_id,
        text,
        label="existing-id-different-workspace",
        check=False,
    )
    assert_rejected(cross_workspace, operation_id)
    rejected.append({"changed": "workspace", "result": cross_workspace})
    stack.cli(
        "workbench_stat",
        {"id": other, "section": "logs", "path": "effects.log"},
        label="other-workspace-path-absent",
        expect_error="NotFound",
    )
    assert (
        materialize(stack, wb, "logs", "effects.log", "binding-after-rejections")
        == text.encode()
    )
    stack.cli(
        "workbench_stat",
        {"id": wb, "section": "logs", "path": "other.log"},
        label="mismatch-other-path-absent",
        expect_error="NotFound",
    )
    duplicate_text = native_append(
        stack, wb, "23" * 16, text, label="new-identity-identical-bytes"
    )
    assert duplicate_text["generation"] == 2, duplicate_text
    assert (
        materialize(stack, wb, "logs", "effects.log", "new-identity-body")
        == 2 * text.encode()
    )
    tail = native_append(
        stack, wb, "24" * 16, "later\n", label="later-independent-writer"
    )
    historical = native_append(
        stack, wb, operation_id, text, label="history-after-later-writer"
    )
    assert historical["replayed"] is True and receipt(historical) == receipt(
        original
    ), historical
    assert (
        materialize(stack, wb, "logs", "effects.log", "history-live-head")
        == (2 * text + "later\n").encode()
    )
    stack.run(
        [
            *stack.client_args,
            "workspace-path",
            "remove",
            wb,
            "logs",
            "effects.log",
            "--expected-generation",
            str(tail["generation"]),
            "--request-id",
            "25" * 16,
        ],
        label="remove-historical-target",
    )
    removed_replay = native_append(
        stack, wb, operation_id, text, label="history-after-remove"
    )
    assert receipt(removed_replay) == receipt(original), removed_replay
    stack.cli(
        "workbench_stat",
        {"id": wb, "section": "logs", "path": "effects.log"},
        label="removed-path-remains-absent",
        expect_error="NotFound",
    )
    replacement = native_append(
        stack, wb, "26" * 16, "replacement\n", label="recreate-path-with-new-identity"
    )
    replacement_replay = native_append(
        stack, wb, operation_id, text, label="history-after-replacement"
    )
    assert receipt(replacement_replay) == receipt(original), replacement_replay
    assert (
        materialize(stack, wb, "logs", "effects.log", "replacement-unchanged")
        == b"replacement\n"
    )
    stack.restart()
    reopened_replay = native_append(
        stack, wb, operation_id, text, label="history-after-reopen"
    )
    assert receipt(reopened_replay) == receipt(original), reopened_replay
    assert (
        materialize(stack, wb, "logs", "effects.log", "replacement-after-reopen")
        == b"replacement\n"
    )
    return {
        "status": "PASS",
        "original": original,
        "rejected_requests": rejected,
        "same_bytes_distinct_identity": duplicate_text,
        "later_writer": tail,
        "historical_receipt_survived": [
            "later_writer",
            "remove",
            "replacement",
            "owner_reopen",
        ],
        "replacement": replacement,
        "same_bytes_text_base64_file_replay": True,
    }


def concurrent_same_identity(stack):
    wb, operation_id, text, count = (
        "concurrent-same-identity",
        "33" * 16,
        "one-event\n",
        8,
    )
    stack.cli("workbench_create", {"id": wb})
    barrier = threading.Barrier(count)

    def invoke(index):
        barrier.wait(timeout=30)
        return native_append(
            stack,
            wb,
            operation_id,
            text,
            label=f"concurrent-same-{index:02d}",
            check=False,
        )

    with concurrent.futures.ThreadPoolExecutor(max_workers=count) as pool:
        initial = list(pool.map(invoke, range(count)))
    # Repeating the SAME identity is allowed even when one process observed an
    # intermediate pending state. There is deliberately no fresh-ID fallback.
    replay = native_append(
        stack, wb, operation_id, text, label="concurrent-same-terminal-replay"
    )
    for result in initial:
        if result.get("status") == "success":
            assert receipt(result) == receipt(replay), (result, replay)
        else:
            assert_rejected(result, operation_id)
    body = materialize(stack, wb, "logs", "effects.log", "concurrent-same-final")
    assert body == text.encode() and replay["generation"] == 1, (body, replay)
    stack.restart()
    reopened = native_append(
        stack, wb, operation_id, text, label="concurrent-same-after-reopen"
    )
    assert receipt(reopened) == receipt(replay), reopened
    assert (
        materialize(stack, wb, "logs", "effects.log", "concurrent-same-reopened-bytes")
        == body
    )
    return {
        "status": "PASS",
        "parallel_cli_processes": count,
        "initial_outcomes": initial,
        "canonical_receipt": replay,
        "actual_effects": 1,
        "owner_reopen_verified": True,
    }


def begin_response_loss(stack, *, change_live):
    suffix = "with-drift" if change_live else "without-drift"
    wb, operation_id, text = (
        "begin-loss-" + suffix,
        ("44" if change_live else "45") * 16,
        "uncertain-event\n",
    )
    stack.cli("workbench_create", {"id": wb})
    stack.proxy.arm("begin_artifact_publish", lambda: stack.owner.kill())
    failed = native_append(
        stack, wb, operation_id, text, label="begin-ack-lost-" + suffix, check=False
    )
    stack.proxy.wait_dropped()
    assert failed.get("status") == "error", failed
    stack.owner.wait(timeout=10)
    stack.restart()
    if change_live:
        native_append(
            stack, wb, "46" * 16, "independent\n", label="begin-drift-later-writer"
        )
    outcomes = [
        native_append(
            stack,
            wb,
            operation_id,
            text,
            label=f"begin-retry-{suffix}-{index}",
            check=False,
        )
        for index in range(2)
    ]
    for result in outcomes:
        if result.get("status") != "success":
            assert_rejected(result, operation_id)
            assert result.get("details", {}).get("state"), result
    if change_live:
        assert all(result.get("status") == "error" for result in outcomes), outcomes
        body = materialize(stack, wb, "logs", "effects.log", "begin-drift-final")
        assert body == b"independent\n", body
    else:
        stat = stack.cli(
            "workbench_stat",
            {"id": wb, "section": "logs", "path": "effects.log"},
            label="begin-no-drift-stat",
            check=False,
        )
        if stat.get("status") == "success":
            body = materialize(stack, wb, "logs", "effects.log", "begin-no-drift-final")
            assert body == text.encode(), body
        else:
            assert stat.get("code") == "NotFound", stat
            body = b""
    successful = [result for result in outcomes if result.get("status") == "success"]
    if successful:
        assert not change_live and body == text.encode(), (body, successful)
        assert all(
            receipt(result) == receipt(successful[0]) for result in successful
        ), successful
    stack.restart()
    if body:
        assert (
            materialize(stack, wb, "logs", "effects.log", "begin-reopened-" + suffix)
            == body
        )
    else:
        stack.cli(
            "workbench_stat",
            {"id": wb, "section": "logs", "path": "effects.log"},
            label="begin-reopened-absent",
            expect_error="NotFound",
        )
    return {
        "status": "PASS",
        "first_call": failed,
        "same_identity_outcomes": outcomes,
        "live_changed_before_retry": change_live,
        "uncertain_append_effects": body.count(text.encode()),
        "completion_qualified": bool(successful),
        "safety_qualified": True,
        "oracle": "A pending operation may remain explicitly unresolved; it must not switch identity or rebase onto a later writer.",
    }


def distinct_identity_contention(stack):
    wb, count = "distinct-identity-contention", 8
    stack.cli("workbench_create", {"id": wb})
    native_append(stack, wb, "50" * 16, "seed\n", label="contention-seed")
    barrier = threading.Barrier(count)

    def invoke(index):
        barrier.wait(timeout=30)
        return native_append(
            stack,
            wb,
            f"{0x60 + index:02x}" * 16,
            f"event-{index:02d}\n",
            label=f"distinct-{index:02d}",
            check=False,
        )

    with concurrent.futures.ThreadPoolExecutor(max_workers=count) as pool:
        initial = list(pool.map(invoke, range(count)))
    final = []
    for index in range(count):
        operation_id = f"{0x60 + index:02x}" * 16
        result = native_append(
            stack,
            wb,
            operation_id,
            f"event-{index:02d}\n",
            label=f"distinct-same-id-replay-{index:02d}",
            check=False,
        )
        if result.get("status") != "success":
            assert_rejected(result, operation_id)
        if initial[index].get("status") == "success":
            assert result.get("status") == "success" and receipt(
                initial[index]
            ) == receipt(result), (initial[index], result)
        final.append(result)
    body = materialize(stack, wb, "logs", "effects.log", "distinct-final").decode()
    assert body.splitlines().count("seed") == 1, body
    for index, result in enumerate(final):
        copies = body.splitlines().count(f"event-{index:02d}")
        assert copies == (1 if result.get("status") == "success" else 0), (
            index,
            copies,
            result,
        )
    stack.restart()
    assert (
        materialize(stack, wb, "logs", "effects.log", "distinct-after-reopen").decode()
        == body
    )
    return {
        "status": "PASS",
        "parallel_cli_processes": count,
        "initial_outcomes": initial,
        "same_identity_replay_outcomes": final,
        "exact_final_body": body,
        "accepted_effects_exactly_once": True,
        "unaccepted_effects_absent": True,
    }


def multiblock_replay(stack):
    wb, operation_id = "multiblock-identity", "77" * 16
    stack.cli("workbench_create", {"id": wb})
    size = 5 * 1024 * 1024 + 31
    pattern = "append-multiblock-fixture\n"
    text = (pattern * ((size // len(pattern)) + 1))[:size]
    assert len(text.encode()) == size
    original = native_append(
        stack,
        wb,
        operation_id,
        text,
        label="multiblock-original",
        payload_form="file",
        max_size=16 * 1024 * 1024,
    )
    assert original["generation"] == 1 and original["logical_size"] == size, original
    tail = "after-multiblock\n"
    native_append(
        stack, wb, "78" * 16, tail, label="multiblock-tail", max_size=16 * 1024 * 1024
    )
    replay = native_append(
        stack,
        wb,
        operation_id,
        text,
        label="multiblock-original-replay",
        payload_form="file",
        max_size=16 * 1024 * 1024,
    )
    assert replay["replayed"] is True and receipt(replay) == receipt(original), replay
    expected = (text + tail).encode()
    body = materialize(stack, wb, "logs", "effects.log", "multiblock-live")
    assert body == expected
    stack.restart()
    body = materialize(stack, wb, "logs", "effects.log", "multiblock-reopened")
    assert body == expected
    return {
        "status": "PASS",
        "original_bytes": size,
        "default_4mib_blocks": 2,
        "final_bytes": len(body),
        "sha256": hashlib.sha256(body).hexdigest(),
        "original_receipt": original,
        "replay_receipt": replay,
        "owner_reopen_verified": True,
    }


PYTHON_INTEROP_PROGRAM = r"""
import hashlib, json, sys
from pathlib import Path
import nokv
from nokv import _native
config = json.load(sys.stdin)
client = nokv.Client(
    config["root_id"],
    nokv.RoutingConfig.etcd([config["etcd_endpoint"]], key_prefix=config["etcd_prefix"]),
    nokv.ObjectStoreConfig.s3(
        config["bucket"], root=config["object_root"], endpoint=config["object_endpoint"],
        access_key_id=config["access_key"], secret_access_key=config["secret_key"],
    ),
    workbench_root=config["workbench_root"],
)
request = config["request"]
identity = {
    "python": sys.executable, "module": nokv.__file__, "version": nokv.__version__,
    "native_module": _native.__file__,
    "native_sha256": hashlib.sha256(Path(_native.__file__).read_bytes()).hexdigest(),
}
try:
    if request["action"] == "seed":
        result = client.publish_bytes(
            request["workbench"], "logs/effects.log", request["text"].encode(),
            content_type=request.get("content_type", "text/plain"), producer="downstream-harness",
            manifest_identity="run-42", index_fields=[("run.step", "signed", "7")],
        )
    else:
        result = client.append_bytes(
            request["workbench"], "logs/effects.log", request["text"].encode(),
            request["operation_id"], content_type=request.get("content_type", "text/plain"), max_logical_size=1048576,
            expected_workspace_incarnation_id=request.get("incarnation"),
        )
    metadata = client.stat(request["workbench"], "logs/effects.log")
    print(json.dumps({"status": "success", "result": result, "metadata": metadata, "identity": identity}))
except Exception as error:
    print(json.dumps({
        "status": "error", "type": type(error).__name__, "message": str(error),
        "operation_id": getattr(error, "operation_id", None), "state": getattr(error, "state", None),
        "code": getattr(error, "code", None), "cause_code": getattr(error, "cause_code", None),
        "retryable": getattr(error, "retryable", None), "expected": getattr(error, "expected", None),
        "is_append_error": isinstance(error, nokv.AppendError),
        "is_incarnation_mismatch": isinstance(error, nokv.WorkspaceIncarnationMismatch),
        "identity": identity,
    }))
"""


def python_call(stack, request, label):
    config = {
        "root_id": stack.root_id,
        "etcd_endpoint": stack.etcd_endpoint,
        "etcd_prefix": stack.etcd_prefix,
        "bucket": stack.bucket,
        "object_root": stack.object_root,
        "object_endpoint": stack.object_endpoint,
        "access_key": stack.access_key,
        "secret_key": stack.secret_key,
        "workbench_root": stack.config.workbench_root,
        "request": request,
    }
    process = stack.run(
        [stack.python_executable, "-c", PYTHON_INTEROP_PROGRAM],
        label=label,
        input_text=json.dumps(config),
    )
    result = json.loads(process.stdout)
    stack.record(
        "python-interop-transcript.jsonl",
        {"label": label, "request": request, "response": result},
    )
    return result


def python_interoperability(stack):
    wb, operation_id = "python-interoperability", "88" * 16
    stack.cli("workbench_create", {"id": wb})
    seed = python_call(
        stack,
        {"action": "seed", "workbench": wb, "text": "seed\n"},
        "python-publish-metadata-seed",
    )
    assert seed["status"] == "success", seed
    expected_metadata = {
        "producer": "downstream-harness",
        "manifest_identity": "run-42",
        "index_fields": [{"field_id": "run.step", "kind": "signed", "value": 7}],
    }
    for field, expected_value in expected_metadata.items():
        assert seed["metadata"][field] == expected_value, (field, seed)
    original = native_append(
        stack, wb, operation_id, "cli\n", label="interop-cli-original"
    )
    replay = python_call(
        stack,
        {
            "action": "append",
            "workbench": wb,
            "text": "cli\n",
            "operation_id": operation_id,
        },
        "interop-python-replays-cli",
    )
    assert replay["status"] == "success" and replay["result"]["replayed"] is True, (
        replay
    )
    assert receipt(replay["result"]) == receipt(original), (replay, original)
    for field in ("producer", "manifest_identity", "index_fields"):
        assert replay["metadata"][field] == expected_metadata[field], (
            field,
            seed,
            replay,
        )
    python_original = python_call(
        stack,
        {
            "action": "append",
            "workbench": wb,
            "text": "python\n",
            "operation_id": "89" * 16,
        },
        "interop-python-original",
    )
    assert python_original["status"] == "success", python_original
    cli_replay = native_append(
        stack, wb, "89" * 16, "python\n", label="interop-cli-replays-python"
    )
    assert cli_replay["replayed"] is True and receipt(cli_replay) == receipt(
        python_original["result"]
    ), cli_replay
    mismatch = python_call(
        stack,
        {
            "action": "append",
            "workbench": wb,
            "text": "different\n",
            "operation_id": operation_id,
        },
        "interop-python-intent-mismatch",
    )
    assert (
        mismatch["status"] == "error"
        and mismatch["is_append_error"] is True
        and mismatch["operation_id"] == operation_id
        and mismatch["code"] == "AppendUnresolved"
        and mismatch["cause_code"] == "RequestReplayMismatch"
        and mismatch["retryable"] is False
    ), mismatch
    fence = python_call(
        stack,
        {
            "action": "append",
            "workbench": wb,
            "text": "fenced\n",
            "operation_id": "8a" * 16,
            "incarnation": "fe" * 16,
        },
        "interop-python-incarnation-fence",
    )
    assert (
        fence["status"] == "error"
        and fence["is_incarnation_mismatch"] is True
        and fence["operation_id"] == "8a" * 16
    ), fence
    expected = b"seed\ncli\npython\n"
    assert (
        materialize(stack, wb, "logs", "effects.log", "interop-final-bytes") == expected
    )
    stack.restart()
    reopened = python_call(
        stack,
        {
            "action": "append",
            "workbench": wb,
            "text": "cli\n",
            "operation_id": operation_id,
        },
        "interop-python-replay-after-owner-reopen",
    )
    assert reopened["status"] == "success" and receipt(reopened["result"]) == receipt(
        original
    ), reopened
    assert (
        materialize(stack, wb, "logs", "effects.log", "interop-reopened-bytes")
        == expected
    )
    for field, expected_value in expected_metadata.items():
        assert reopened["metadata"][field] == expected_value, (field, reopened)
    inheritance = default_type_inheritance(stack, wb + "-default-type")
    return {
        "status": "PASS",
        "default_content_type_inheritance": inheritance,
        "python_identity": seed["identity"],
        "cli_original": original,
        "python_replay": replay["result"],
        "python_original": python_original["result"],
        "cli_replay": cli_replay,
        "typed_mismatch": mismatch,
        "typed_incarnation_fence": fence,
        "metadata_inheritance_verified": True,
        "owner_reopen_verified": True,
        "exact_bytes": expected.decode(),
    }


def pack_rpc_value(value):
    """Encode the small JSON-shaped MessagePack request used by this gate only."""
    if value is None:
        return b"\xc0"
    if isinstance(value, bool):
        return b"\xc3" if value else b"\xc2"
    if isinstance(value, int) and value >= 0:
        if value < 128:
            return bytes([value])
        for limit, marker, width in (
            (256, 0xCC, 1),
            (65536, 0xCD, 2),
            (2**32, 0xCE, 4),
            (2**64, 0xCF, 8),
        ):
            if value < limit:
                return bytes([marker]) + value.to_bytes(width, "big")
    if isinstance(value, str):
        body = value.encode()
        length = len(body)
        if length < 32:
            return bytes([0xA0 | length]) + body
        for limit, marker, width in (
            (256, 0xD9, 1),
            (65536, 0xDA, 2),
            (2**32, 0xDB, 4),
        ):
            if length < limit:
                return bytes([marker]) + length.to_bytes(width, "big") + body
    if isinstance(value, list):
        count = len(value)
        prefix = (
            bytes([0x90 | count]) if count < 16 else b"\xdc" + count.to_bytes(2, "big")
        )
        return prefix + b"".join(pack_rpc_value(item) for item in value)
    if isinstance(value, dict):
        count = len(value)
        prefix = (
            bytes([0x80 | count]) if count < 16 else b"\xde" + count.to_bytes(2, "big")
        )
        return prefix + b"".join(
            pack_rpc_value(key) + pack_rpc_value(item) for key, item in value.items()
        )
    raise ValueError(f"unsupported gate request value: {type(value).__name__}")


def raw_rpc(stack, operation, label):
    """Use a captured valid native handshake and route for one public RPC."""
    with stack.proxy._lock:
        handshake = stack.proxy.handshake_request
        request = {
            "schema": stack.proxy.wire_schema,
            "payload": {
                "request_id": list(uuid.uuid4().bytes),
                "route": dict(stack.proxy.latest_route),
                "operation": operation,
            },
        }
    assert handshake is not None and request["schema"], (
        "native CLI must establish the wire profile first"
    )
    encoded = pack_rpc_value(request)
    assert unpack(encoded) == request
    with socket.create_connection(
        ("127.0.0.1", stack.proxy_port), timeout=stack.timeout
    ) as client:
        client.settimeout(stack.timeout)
        client.sendall(len(handshake).to_bytes(4, "big") + handshake)
        response = _recv_exact(client, int.from_bytes(_recv_exact(client, 4), "big"))
        assert is_handshake_frame(response), response.hex()
        client.sendall(len(encoded).to_bytes(4, "big") + encoded)
        response = unpack(
            _recv_exact(client, int.from_bytes(_recv_exact(client, 4), "big"))
        )
    stack.record(
        "native-rpc-transcript.jsonl",
        {"label": label, "request": request, "response": response},
    )
    return response["payload"]["outcome"]


def cross_kind_identity_collision(stack):
    wb, operation_id, text = "cross-kind-collision", "99" * 16, "published-once\n"
    stack.cli("workbench_create", {"id": wb})
    original = native_append(
        stack, wb, operation_id, text, label="cross-kind-original-append"
    )
    query = {
        "operation": "get_operation",
        "request": {"operation_id": list(bytes.fromhex(operation_id))},
    }
    before = raw_rpc(stack, query, "cross-kind-operation-before")
    assert (
        before["status"] == "success"
        and before["body"]["value"]["kind"] == "artifact_publish"
    ), before
    collision = raw_rpc(
        stack,
        {
            "operation": "commit",
            "request": {
                "operation_id": list(bytes.fromhex(operation_id)),
                "workbench": wb,
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
        "cross-kind-commit-reuses-append-id",
    )
    assert collision["status"] == "failure", collision
    assert collision["body"]["code"] in ("conflict", "request_replay_mismatch"), (
        collision
    )
    after = raw_rpc(stack, query, "cross-kind-operation-after")
    assert after["status"] == "success", after
    before_value, after_value = before["body"]["value"], after["body"]["value"]
    assert (
        after_value["kind"] == "artifact_publish"
        and after_value["state"] == "succeeded"
    ), after
    assert after_value["result"] == before_value["result"], (before, after)
    replay = native_append(
        stack, wb, operation_id, text, label="cross-kind-original-still-replays"
    )
    assert receipt(replay) == receipt(original), (original, replay)
    assert (
        materialize(stack, wb, "logs", "effects.log", "cross-kind-bytes")
        == text.encode()
    )
    stack.restart()
    reopened = native_append(
        stack, wb, operation_id, text, label="cross-kind-after-owner-reopen"
    )
    assert receipt(reopened) == receipt(original), (original, reopened)
    assert (
        materialize(stack, wb, "logs", "effects.log", "cross-kind-reopened-bytes")
        == text.encode()
    )
    return {
        "status": "PASS",
        "colliding_commit": collision,
        "original_receipt": original,
        "after_collision_receipt": replay,
        "operation_kind_preserved": True,
        "owner_reopen_verified": True,
    }


def default_type_inheritance(stack, wb):
    """Both surfaces preserve an existing nondefault type when override is absent."""
    stack.cli("workbench_create", {"id": wb})
    custom_type = "application/x-harness-events"
    seed = python_call(
        stack,
        {
            "action": "seed",
            "workbench": wb,
            "text": "seed\n",
            "content_type": custom_type,
        },
        "inherit-custom-type-seed",
    )
    expected_metadata = {
        "content_type": custom_type,
        "producer": "downstream-harness",
        "manifest_identity": "run-42",
        "index_fields": [{"field_id": "run.step", "kind": "signed", "value": 7}],
    }
    assert seed["status"] == "success", seed
    for field, expected in expected_metadata.items():
        assert seed["metadata"][field] == expected, (field, seed)
    request = {
        "action": "append",
        "workbench": wb,
        "text": "python-inherit\n",
        "operation_id": "aa" * 16,
        "content_type": None,
    }
    original = python_call(stack, request, "inherit-python-original")
    replay = python_call(stack, request, "inherit-python-same-none-replay")
    assert original["status"] == replay["status"] == "success", (original, replay)
    assert replay["result"]["replayed"] is True and receipt(
        original["result"]
    ) == receipt(replay["result"]), (original, replay)
    changed = python_call(
        stack,
        {**request, "content_type": custom_type},
        "inherit-none-to-explicit-mismatch",
    )
    assert (
        changed["status"] == "error"
        and changed["cause_code"] == "RequestReplayMismatch"
    ), changed
    cli_original = native_append(
        stack,
        wb,
        "ac" * 16,
        "cli-inherit\n",
        label="inherit-cli-original",
        content_type=None,
    )
    cli_replay = native_append(
        stack,
        wb,
        "ac" * 16,
        "cli-inherit\n",
        label="inherit-cli-replay",
        content_type=None,
    )
    assert cli_replay["replayed"] is True and receipt(cli_original) == receipt(
        cli_replay
    ), cli_replay
    stack.restart()
    reopened = python_call(stack, request, "inherit-python-after-reopen")
    assert reopened["status"] == "success" and receipt(reopened["result"]) == receipt(
        original["result"]
    ), reopened
    for field, expected in expected_metadata.items():
        assert (
            replay["metadata"][field] == expected
            and reopened["metadata"][field] == expected
        ), (field, replay, reopened)
    body = materialize(stack, wb, "logs", "effects.log", "inherit-reopened-bytes")
    assert body == b"seed\npython-inherit\ncli-inherit\n", body
    return {
        "status": "PASS",
        "expected_metadata": expected_metadata,
        "python_original": original["result"],
        "cli_original": cli_original,
        "none_to_explicit_mismatch": changed,
        "exact_bytes": body.decode(),
        "owner_reopen_verified": True,
    }


class IsolatedStack:
    def __init__(
        self,
        evidence_dir: str | Path,
        *,
        binary: Path,
        source: Path,
        label="append",
        timeout=60,
    ):
        self.source = source.resolve()
        self.evidence_dir = Path(evidence_dir).resolve()
        self.evidence = live.Evidence(self.evidence_dir)
        self.evidence.prepare()
        self.label = "nokv-audit-" + label + "-" + uuid.uuid4().hex[:10]
        self.timeout = timeout
        self.head = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=self.source, text=True
        ).strip()

        def ident(name):
            return hashlib.sha256((self.label + name).encode()).hexdigest()[:32]

        self.root_id, self.agent_id, self.shard_id = [
            ident(x) for x in ("root", "agent", "shard")
        ]
        (
            self.etcd_port,
            self.peer_port,
            self.s3_port,
            self.console_port,
            self.owner_port,
            self.proxy_port,
        ) = [infra.free_port() for _ in range(6)]
        self.etcd_endpoint = f"http://127.0.0.1:{self.etcd_port}"
        self.peer_endpoint = f"http://127.0.0.1:{self.peer_port}"
        self.object_endpoint = f"http://127.0.0.1:{self.s3_port}"
        self.etcd_prefix = "/nokv/" + self.label
        self.bucket = self.label
        self.object_root = "append-identity-gate"
        self.object_region = "us-east-1"
        self.access_key = self.secret_key = "rustfsadmin"
        self.binary = binary.resolve()
        self.container = self.label + "-rustfs"
        self.volume = self.container + "-data"
        executables = {}
        for name in ("aws", "docker", "etcd", "etcdctl"):
            executable = shutil.which(name)
            if executable is None:
                raise infra.NotQualified(f"required executable is unavailable: {name}")
            executables[name] = Path(executable)
        if not self.binary.is_file():
            raise infra.NotQualified(f"NoKV binary is unavailable: {self.binary}")
        self.aws = executables["aws"]
        self.docker = executables["docker"]
        self.etcdctl = executables["etcdctl"]
        self.etcd_binary = executables["etcd"]
        self.aws_env = infra.aws_environment(self.access_key, self.secret_key)
        self.proxy = EvidenceDropProxy(self.proxy_port, timeout, self)
        self.advertise = f"127.0.0.1:{self.proxy_port}"
        self.config = live.parse_args(
            [
                "--nokv-bin",
                str(self.binary),
                "--evidence-dir",
                str(self.evidence_dir),
                "--metadata-dir",
                str(self.evidence_dir / "metadata"),
                "--root-id",
                self.root_id,
                "--agent-id",
                self.agent_id,
                "--logical-shard-id",
                self.shard_id,
                "--agent-name",
                self.agent_id,
                "--etcd-endpoint",
                self.etcd_endpoint,
                "--etcd-key-prefix",
                self.etcd_prefix,
                "--server-bind",
                f"127.0.0.1:{self.owner_port}",
                "--advertise-endpoint",
                self.advertise,
                "--object-endpoint",
                self.object_endpoint,
                "--object-bucket",
                self.bucket,
                "--object-root",
                self.object_root,
                "--object-access-key-id",
                self.access_key,
                "--object-secret-access-key",
                self.secret_key,
                "--command-timeout-seconds",
                str(timeout),
            ]
        )
        self.processes = []
        self.logs = []
        self.lock = threading.Lock()
        self.owner = None
        self.epoch = 0
        self.closed = False

    def record(self, name, value):
        with self.lock:
            self.evidence.line(name, value)

    def json(self, name, value):
        self.evidence.json(name, value)

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
        started = live.now()
        result = subprocess.run(
            command,
            cwd=self.source,
            text=True,
            capture_output=True,
            input=input_text,
            env=env,
            timeout=timeout or self.timeout,
        )
        redacted = live.redact_argv(command)
        redacted = [
            a.split("=")[0] + "=<redacted>"
            if a.startswith(("RUSTFS_ACCESS_KEY=", "RUSTFS_SECRET_KEY="))
            else a
            for a in redacted
        ]
        self.record(
            "processes.jsonl",
            {
                "label": label,
                "argv": redacted,
                "started_at": started,
                "ended_at": live.now(),
                "returncode": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
            },
        )
        if check and result.returncode:
            raise RuntimeError(f"{label}: {result.stderr or result.stdout}")
        return result

    def start(self, label, command):
        f = (self.evidence_dir / f"{label}.log").open("w")
        self.logs.append(f)
        p = subprocess.Popen(
            list(map(str, command)),
            cwd=self.source,
            stdout=f,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=True,
        )
        self.processes.append(p)
        self.record(
            "processes.jsonl",
            {
                "label": label,
                "pid": p.pid,
                "argv": live.redact_argv(list(map(str, command))),
                "started_at": live.now(),
            },
        )
        return p

    @property
    def client_args(self):
        return live.client_args(self.config)

    def cli(
        self,
        tool,
        arguments,
        *,
        label=None,
        extra=(),
        expect_error=None,
        check=True,
        config=None,
    ):
        cfg = config or self.config
        p = self.run(
            [
                *live.client_args(cfg),
                *extra,
                "workbench",
                tool,
                live.canonical_json(arguments),
            ],
            label=label or tool,
            check=False,
        )
        raw = p.stdout if not p.returncode else p.stderr.strip().removeprefix("nokv: ")
        try:
            value = json.loads(raw)
        except json.JSONDecodeError:
            value = {"unparsed": raw, "returncode": p.returncode}
        self.record(
            "native-cli-transcript.jsonl",
            {
                "label": label or tool,
                "tool": tool,
                "arguments": arguments,
                "extra": list(extra),
                "returncode": p.returncode,
                "result": value,
            },
        )
        if expect_error is not None:
            assert p.returncode and value.get("code") == expect_error, value
        elif check:
            assert p.returncode == 0 and value.get("status") == "success", value
        return value

    def restart(self, *, kill=True):
        if self.owner is not None and self.owner.poll() is None:
            if kill:
                self.owner.kill()
                self.owner.wait(timeout=10)
            else:
                live.stop(self.owner)
        infra.wait_session_absent(
            self.etcdctl,
            self.etcd_endpoint,
            f"{self.etcd_prefix}/sessions/{self.shard_id}",
            self.source,
            self.timeout,
        )
        self.epoch += 1
        cfg = dataclasses.replace(self.config, metadata_mode="reopen")
        self.owner = self.start(f"owner-{self.epoch}", live.server_command(cfg))
        infra.wait_tcp(self.owner, self.owner_port, self.timeout)
        self.record(
            "faults.jsonl",
            {
                "kind": "owner-kill-reopen" if kill else "owner-clean-reopen",
                "at": live.now(),
                "owner_number": self.epoch,
            },
        )

    def source_snapshot(self):
        """Fingerprint tracked and newly added product sources without equating them to build input."""
        paths = set()
        for options in ((), ("--others", "--exclude-standard")):
            raw = subprocess.check_output(
                [
                    "git",
                    "ls-files",
                    "-z",
                    *options,
                    "--",
                    "Cargo.toml",
                    "Cargo.lock",
                    "crates",
                ],
                cwd=self.source,
            )
            paths.update(path.decode() for path in raw.split(b"\0") if path)
        files = {
            path: live.digest_file(self.source / path)
            for path in sorted(paths)
            if (self.source / path).is_file()
        }
        manifest = {
            "files": files,
            "sha256": hashlib.sha256(live.canonical_json(files).encode()).hexdigest(),
            "qualification": "current source snapshot; binary hash and embedded build identity are independent evidence",
        }
        self.json("source-files.json", manifest)
        return {"file": "source-files.json", "sha256": manifest["sha256"]}

    def __enter__(self):
        try:
            self.run(
                [self.docker, "volume", "create", self.volume], label="create-volume"
            )
            self.run(
                infra.rustfs_container_command(
                    self.docker,
                    container=self.container,
                    volume=self.volume,
                    s3_port=self.s3_port,
                    console_port=self.console_port,
                    image=PINNED_RUSTFS_IMAGE,
                    access_key=self.access_key,
                    secret_key=self.secret_key,
                ),
                label="start-rustfs",
                timeout=180,
            )
            infra.wait_rustfs(
                self.aws,
                self.object_endpoint,
                self.aws_env,
                self.source,
                self.timeout,
                docker=self.docker,
                container=self.container,
            )
            self.run(
                infra.aws_command(
                    self.aws,
                    self.object_endpoint,
                    "s3api",
                    "create-bucket",
                    "--bucket",
                    self.bucket,
                ),
                label="create-bucket",
                env=self.aws_env,
            )
            peer = self.peer_endpoint
            self.etcd = self.start(
                "etcd",
                [
                    self.etcd_binary,
                    "--name",
                    self.label,
                    "--data-dir",
                    self.evidence_dir / "etcd-data",
                    "--listen-client-urls",
                    self.etcd_endpoint,
                    "--advertise-client-urls",
                    self.etcd_endpoint,
                    "--listen-peer-urls",
                    peer,
                    "--initial-advertise-peer-urls",
                    peer,
                    "--initial-cluster",
                    f"{self.label}={peer}",
                    "--initial-cluster-state",
                    "new",
                    "--log-level",
                    "warn",
                ],
            )
            infra.wait_etcd(
                self.etcdctl,
                self.etcd_endpoint,
                self.etcd,
                self.source,
                self.timeout,
            )
            identity = json.loads(
                self.run(
                    [self.binary, "version", "--json"], label="binary-identity"
                ).stdout
            )
            assert identity["holt"]["version"] == "0.8.6", identity
            self.json(
                "environment.json",
                {
                    "source_head": self.head,
                    "source_directory": str(self.source),
                    "harness_path": str(Path(__file__).resolve()),
                    "harness_sha256": live.digest_file(Path(__file__)),
                    "binary_path": str(self.binary),
                    "binary_build_head": identity["git_commit"],
                    "build_head_matches_source_head": identity["git_commit"]
                    == self.head,
                    "source_snapshot": self.source_snapshot(),
                    "identity": identity,
                    "source_diff_sha256": hashlib.sha256(
                        subprocess.check_output(
                            ["git", "diff", "HEAD"], cwd=self.source
                        )
                    ).hexdigest(),
                    "binary_sha256": live.digest_file(self.binary),
                    "provider_image": PINNED_RUSTFS_IMAGE,
                    "schema_sha256": live.digest_file(
                        self.source / "crates/nokv-agent/workbench_contract_schema.json"
                    ),
                    "started_at": live.now(),
                    "root_id": self.root_id,
                    "shard_id": self.shard_id,
                    "etcd_endpoint": self.etcd_endpoint,
                    "object_endpoint": self.object_endpoint,
                    "object_root": self.object_root,
                    "bucket": self.bucket,
                    "profile": "Holt local sync, isolated etcd, real S3/RustFS, native CLI",
                },
            )
            self.run(live.provision_command(self.config), label="provision")
            if self.proxy:
                self.proxy.set_target(self.owner_port)
                self.proxy.start()
            self.epoch = 1
            self.owner = self.start("owner-1", live.server_command(self.config))
            infra.wait_tcp(self.owner, self.owner_port, self.timeout)
            return self
        except BaseException:
            self.close()
            raise

    def close(self):
        if self.closed:
            return
        self.closed = True
        cleanup = []
        for p in reversed(self.processes):
            if p.poll() is None:
                live.stop(p)
        if self.proxy:
            try:
                self.proxy.close()
            except Exception as e:
                cleanup.append(str(e))
        for f in self.logs:
            f.close()
        self.run(
            [self.docker, "logs", "--tail", "100", self.container],
            label="rustfs-logs",
            check=False,
        )
        for args, label in (
            ([self.docker, "rm", "-f", self.container], "remove-rustfs"),
            ([self.docker, "volume", "rm", self.volume], "remove-volume"),
        ):
            p = self.run(args, label=label, check=False)
            cleanup.append({"step": label, "exit_code": p.returncode})
        self.json("cleanup.json", {"at": live.now(), "results": cleanup})

    def __exit__(self, kind, value, tb):
        self.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--nokv-bin", type=Path, required=True)
    parser.add_argument("--source-dir", type=Path, required=True)
    parser.add_argument(
        "--mode", choices=("baseline", "qualified"), default="qualified"
    )
    parser.add_argument("--timeout-seconds", type=int, default=60)
    parser.add_argument(
        "--python-executable",
        type=Path,
        help="Isolated Python environment containing the candidate NoKV extension; enables cross-surface E2E.",
    )
    parser.add_argument(
        "--scenario",
        default="all",
        choices=(
            "all",
            "legacy_response_loss",
            "terminal_response_loss",
            "identity_binding_and_history",
            "concurrent_same_identity",
            "begin_response_loss",
            "begin_response_loss_with_drift",
            "distinct_identity_contention",
            "multiblock_replay",
            "python_interoperability",
            "cross_kind_identity_collision",
        ),
        help="Run one acceptance scenario; all is required for complete qualification.",
    )
    args = parser.parse_args()
    results = {
        "schema": "nokv.append_identity_recovery_gate.v1",
        "command": sys.argv,
        "started_at": live.now(),
        "mode": args.mode,
        "selection": args.scenario,
        "full_matrix": False,
        "qualified_scopes": [],
        "python_e2e_enabled": args.python_executable is not None,
        "scenarios": {},
    }
    stack = IsolatedStack(
        args.evidence_dir,
        binary=args.nokv_bin,
        source=args.source_dir,
        timeout=args.timeout_seconds,
    )
    stack.python_executable = args.python_executable
    try:
        with stack:
            if args.mode == "baseline":
                scenarios = [("legacy_response_loss", append_loss)]
            else:
                stack.proxy.capture_rpc = True
                scenarios = [
                    ("terminal_response_loss", terminal_response_loss),
                    ("identity_binding_and_history", identity_binding_and_history),
                    ("concurrent_same_identity", concurrent_same_identity),
                    (
                        "begin_response_loss",
                        lambda value: begin_response_loss(value, change_live=False),
                    ),
                    (
                        "begin_response_loss_with_drift",
                        lambda value: begin_response_loss(value, change_live=True),
                    ),
                    ("distinct_identity_contention", distinct_identity_contention),
                    ("multiblock_replay", multiblock_replay),
                    ("cross_kind_identity_collision", cross_kind_identity_collision),
                ]
                if args.python_executable is not None:
                    scenarios.append(
                        ("python_interoperability", python_interoperability)
                    )
            if args.scenario != "all":
                scenarios = [
                    (name, scenario)
                    for name, scenario in scenarios
                    if name == args.scenario
                ]
                if not scenarios:
                    raise ValueError(
                        f"scenario {args.scenario!r} is unavailable in {args.mode!r} mode"
                    )
            for name, scenario in scenarios:
                print(f"START {name}", flush=True)
                started = time.monotonic()
                try:
                    result = scenario(stack)
                except Exception as error:
                    results["scenarios"][name] = {
                        "status": "FAIL",
                        "error": repr(error),
                        "wall_seconds": time.monotonic() - started,
                    }
                    stack.json("results.json", results)
                    raise
                result["wall_seconds"] = time.monotonic() - started
                results["scenarios"][name] = result
                stack.json("results.json", results)
                print(f"{name}: {result['status']}", flush=True)
            results["qualified_scopes"] = [
                name
                for name, result in results["scenarios"].items()
                if result["status"] == "PASS"
            ]
            results["full_matrix"] = (
                args.mode == "qualified"
                and args.scenario == "all"
                and args.python_executable is not None
                and "python_interoperability" in results["qualified_scopes"]
            )
            results["status"] = (
                "RED_REPRODUCED"
                if args.mode == "baseline"
                else "GREEN"
                if results["full_matrix"]
                else "GREEN_PARTIAL"
            )
        results["completed_at"] = live.now()
        stack.json("results.json", results)
        print(
            json.dumps(
                {"status": results["status"], "evidence_dir": str(stack.evidence_dir)}
            ),
            flush=True,
        )
        return 0
    except Exception:
        results["status"] = "GATE_ERROR"
        results["traceback"] = traceback.format_exc()
        stack.json("results.json", results)
        print(results["traceback"], file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
