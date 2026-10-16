#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0
"""Public, ID-based append cleanup inspection and recovery acceptance.

The fault proxy proves real transport boundaries. Operational recovery obtains
its ledger only from public CLI/Python inspection, never a captured Stage RPC.
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
import sys
import time
import traceback

import append_product_acceptance_gate as product

base = product.base
require = product.require
BLOCK_SIZE = 4096
BODY_LIMIT = 4 * 1024 * 1024


def operation(
    stack,
    action,
    operation_id,
    label,
    *,
    limit=None,
    cursor=None,
    expected_state_digest=None,
    config=None,
):
    cfg = config or stack.config
    command = [
        str(stack.binary),
        *base.live.control_args(cfg),
        "--agent-id",
        cfg.agent_id,
        "operation",
        action,
        operation_id,
    ]
    if limit is not None:
        command += ["--limit", str(limit)]
    if cursor is not None:
        command += ["--cursor", cursor]
    if expected_state_digest is not None:
        command += ["--expected-state-digest", expected_state_digest]
    response = stack.run(command, label=label, check=False)
    if response.returncode == -9:
        result = {"status": "caller_killed", "operation_id": operation_id}
    else:
        raw = (
            response.stdout
            if response.returncode == 0
            else response.stderr.strip().removeprefix("nokv: ")
        )
        try:
            result = json.loads(raw)
        except json.JSONDecodeError as error:
            raise product.ContractViolation(
                f"Public operation {action} returned no structured JSON: {raw}"
            ) from error
        require(
            (response.returncode == 0) == (result.get("status") == "success"),
            "operation exit and structured receipt agree",
            result,
        )
    stack.record(
        "public-operation-transcript.jsonl",
        {
            "at": base.live.now(),
            "action": action,
            "operation_id": operation_id,
            "label": label,
            "cursor": cursor,
            "expected_state_digest": expected_state_digest,
            "object_configuration_supplied": False,
            "result": result,
        },
    )
    return result


PYTHON_OPERATIONS_PROGRAM = r"""
import base64, hashlib, json, sys
from pathlib import Path
import nokv
from nokv import _native
c = json.load(sys.stdin)
client = nokv.Client(c["root_id"], nokv.RoutingConfig.etcd([c["etcd_endpoint"]], key_prefix=c["etcd_prefix"]), object_store=None, workbench_root=c["workbench_root"])
try:
    if c["action"] == "inspect":
        cursor = None if c["cursor"] is None else base64.b64decode(c["cursor"], validate=True)
        result = client.operation_inspect(c["operation_id"], cursor=cursor, limit=c["limit"])
        if result.get("next_cursor") is not None:
            result["next_cursor"] = base64.b64encode(result["next_cursor"]).decode()
    else:
        result = client.operation_recover(c["operation_id"], expected_state_digest=c["expected_state_digest"])
    print(json.dumps({"result":result,"native_sha256":hashlib.sha256(Path(_native.__file__).read_bytes()).hexdigest(),"object_store":None}))
except Exception as error:
    print(json.dumps({"result":{"status":"error","type":type(error).__name__,"message":str(error),**{field:getattr(error,field,None) for field in ("code","cause_code","operation_id","expected_state_digest","recovery_receipt","next_action","retryable")}}}))
"""


def python_operation(
    stack,
    action,
    operation_id,
    label,
    *,
    limit=32,
    cursor=None,
    expected_state_digest=None,
):
    if stack.python_executable is None:
        raise base.infra.NotQualified(
            "An installed matching Python extension is required for public operations interop."
        )
    config = {
        "root_id": stack.root_id,
        "etcd_endpoint": stack.etcd_endpoint,
        "etcd_prefix": stack.etcd_prefix,
        "workbench_root": stack.config.workbench_root,
        "action": action,
        "operation_id": operation_id,
        "limit": limit,
        "cursor": cursor,
        "expected_state_digest": expected_state_digest,
    }
    response = stack.run(
        [stack.python_executable, "-c", PYTHON_OPERATIONS_PROGRAM],
        label=label,
        input_text=json.dumps(config),
    )
    value = json.loads(response.stdout)
    stack.record("python-operation-transcript.jsonl", {"label": label, "result": value})
    return value["result"]


def await_state(stack, operation_id, predicate, label, deadline):
    until, observations = time.monotonic() + deadline, []
    while True:
        status = product.operation_status(
            stack, operation_id, f"{label}-{len(observations):03d}"
        )
        observations.append(status)
        if predicate(status):
            stack.json(label + ".json", observations)
            return status
        require(
            time.monotonic() < until,
            "operation reaches declared state within the recovery window",
            observations,
        )
        time.sleep(0.5)


def fixture_body(count):
    return b"".join(
        sequence.to_bytes(4, "big") + bytes([sequence % 251]) * (BLOCK_SIZE - 4)
        for sequence in range(count)
    )


def create_quarantine(stack, *, label, count, fault_sequence, deadline):
    workbench = "operations-" + label
    operation_id = hashlib.sha256(workbench.encode()).hexdigest()[:32]
    payload = fixture_body(count)
    stack.cli("workbench_create", {"id": workbench})
    owner_pid = stack.owner.pid
    with stack.object_proxy.lock:
        require(stack.object_proxy.rule is None, "no previous provider fault is armed")
        first_fault = len(stack.object_proxy.fired)
        stack.object_proxy.rule = {
            "method": "PUT",
            "action": "seal_ack_loss_head_failure",
            "path_suffix": f"/blocks/{fault_sequence:016x}",
        }
    caller_label = label + "-uploaded-caller"
    stack.proxy.arm("stage_artifact_manifest", lambda: stack.kill_caller(caller_label))
    first = product.append(
        stack,
        workbench,
        operation_id,
        payload,
        label=caller_label,
        max_size=BODY_LIMIT,
        block_size=BLOCK_SIZE,
    )
    stack.proxy.wait_dropped()
    require(
        first.get("status") == "caller_killed",
        "the real uploaded caller was killed",
        first,
    )
    active_noop = operation(stack, "recover", operation_id, label + "-active-noop")
    require(
        stack.owner.pid == owner_pid and stack.owner.poll() is None,
        "the same healthy owner survives the real caller SIGKILL",
    )
    require(
        active_noop.get("status") == "success"
        and active_noop.get("state") == "pending"
        and active_noop.get("requested") is False
        and active_noop.get("recovery_receipt") is None
        and active_noop.get("cleanup_retry_count") == 0,
        "fresh recovery of an active publication is a metadata-only no-op",
        active_noop,
    )
    status = await_state(
        stack,
        operation_id,
        lambda s: s.get("state") == "quarantined",
        label + "-quarantine",
        deadline,
    )
    faults = stack.object_proxy.fired[first_fault:]
    require(
        len(faults) == 1
        and faults[0]["fault"] == "seal_ack_loss_head_failure"
        and faults[0]["request_bytes"] == 0
        and faults[0]["if_match"]
        and 200 <= faults[0]["upstream_status"] < 300,
        "real empty conditional seal persisted before ACK loss",
        faults,
    )
    require(
        any(
            item["method"] == "HEAD" and item["path"] == faults[0]["path"]
            for item in stack.object_proxy.rejected_requests
        ),
        "the exact sealed key's follow-up HEAD was rejected",
    )
    require(
        status.get("receipt") is None
        and status.get("cleanup_retry_count") == 0
        and status.get("cause_code"),
        "initial quarantine has no append receipt or retry generation",
        status,
    )
    return {
        "workbench": workbench,
        "operation_id": operation_id,
        "payload": payload,
        "registered_count": count,
        "cleanup_cursor": fault_sequence // 32 * 32,
        "quarantine": status,
        "fault": faults[0],
        "active_noop": active_noop,
    }


def inspect_all(
    stack, fixture, label, *, limit=32, python_pages=False, reopen_after_first=False
):
    operation_id, cursor, pages, entries = fixture["operation_id"], None, [], []
    while True:
        caller = python_operation if python_pages and len(pages) % 2 else operation
        page = caller(
            stack,
            "inspect",
            operation_id,
            f"{label}-page-{len(pages):03d}",
            limit=limit,
            cursor=cursor,
        )
        require(
            page.get("status") == "success" and page.get("action") == "inspect",
            "public ledger page succeeds",
            page,
        )
        require(
            page.get("operation_id") == operation_id,
            "public page belongs to the requested logical operation",
            page,
        )
        require(
            page["operation_token"]["operation_id"] == operation_id
            and page["publication_token"]["operation_id"]
            == fixture["quarantine"]["publication_operation_id"],
            "public tokens bind the logical parent and admitted publication child",
            page,
        )
        if pages:
            require(
                all(
                    page[field] == pages[0][field]
                    for field in (
                        "operation_token",
                        "publication_token",
                        "registered_count",
                        "cleanup_cursor",
                        "remaining_count",
                        "object_namespace_id",
                    )
                ),
                "all pages bind the same exact logical/child state and ledger interval",
                page,
            )
        require(
            len(page["entries"]) <= limit,
            "public page respects its requested bound",
            page,
        )
        pages.append(page)
        entries.extend(page["entries"])
        cursor = page.get("next_cursor")
        if cursor is None:
            break
        require(
            page["entries"] and len(pages) <= fixture["registered_count"] + 1,
            "pagination advances and remains bounded",
            page,
        )
        if reopen_after_first and len(pages) == 1:
            stack.restart()
    first = pages[0]
    require(
        first["registered_count"] == fixture["registered_count"]
        and first["cleanup_cursor"] == fixture["cleanup_cursor"]
        and first["remaining_count"]
        == first["registered_count"] - first["cleanup_cursor"],
        "public interval describes actual registered rows and cleaned prefix",
        first,
    )
    require(
        [row["sequence"] for row in entries]
        == list(range(first["cleanup_cursor"], first["registered_count"])),
        "every remaining sequence occurs exactly once across public pages",
        entries,
    )
    for row in entries:
        block = fixture["payload"][
            row["sequence"] * BLOCK_SIZE : (row["sequence"] + 1) * BLOCK_SIZE
        ]
        require(
            row["expected_length"] == len(block)
            and row["expected_digest"] == "sha256:" + hashlib.sha256(block).hexdigest()
            and row["object_identity"].endswith(f"/blocks/{row['sequence']:016x}")
            and row["multipart_token"] is None,
            "public ledger preserves exact expected object identity, bytes, and digest",
            row,
        )
    result = {"pages": pages, "entries": entries}
    stack.json(label + ".json", result)
    return result


def release_provider(stack):
    with stack.object_proxy.lock:
        stack.object_proxy.reject_matching = None
        stack.object_proxy.rule = None


def cleanup_receipt(value, fixture, digest, count):
    receipt = value.get("recovery_receipt")
    expected = {
        "operation_id": fixture["operation_id"],
        "publication_operation_id": fixture["quarantine"]["publication_operation_id"],
        "cleanup_retry_count": count,
        "expected_state_digest": digest,
    }
    require(
        value.get("status") == "success"
        and value.get("requested") is True
        and receipt == expected,
        "complete cleanup acceptance receipt binds its original exact state",
        value,
    )
    return receipt


def reject_trusted_verdict(stack, fixture, inspected, label):
    # This deliberate raw request is only a negative test of the surviving
    # generic verdict API; all inspection/recovery inputs come from public CLI.
    token = inspected["publication_token"]
    wrong = base.raw_rpc(
        stack,
        {
            "operation": "reconcile_quarantined_artifact_publish",
            "request": {
                "token": {
                    "operation_id": list(bytes.fromhex(token["operation_id"])),
                    "state_digest": list(bytes.fromhex(token["state_digest"])),
                },
                "resolution": "provider_objects_absent",
                "reason": "Acceptance verifies that append cleanup cannot trust a caller verdict.",
                "evidence_digest": list(hashlib.sha256(fixture["payload"]).digest()),
            },
        },
        label,
    )
    require(
        wrong.get("status") == "failure"
        and wrong.get("body", {}).get("code") == "precondition_failed"
        and wrong.get("body", {}).get("conflict") == "operation_state"
        and wrong.get("body", {}).get("retryable") is False,
        "trusted generic provider verdict cannot reconcile an append",
        wrong,
    )
    after = operation(stack, "inspect", fixture["operation_id"], label + "-unchanged")
    require(
        all(
            after.get(field) == inspected[field]
            for field in (
                "operation_token",
                "publication_token",
                "registered_count",
                "cleanup_cursor",
                "remaining_count",
            )
        ),
        "rejected caller verdict leaves the inspected durable state unchanged",
        after,
    )
    return wrong


def exact_input_error(
    value,
    operation_id,
    *,
    code="InvalidArgument",
    cause="InvalidArgument",
    conflict=None,
):
    details = value.get("details", {})
    require(
        value.get("status") == "error"
        and value.get("code") == code
        and details.get("operation_id") == operation_id
        and details.get("cause_code") == cause
        and details.get("next_action") == "query_same"
        and (conflict is None or details.get("conflict") == conflict),
        "public operation error has the precise actionable classification",
        value,
    )


def inspection_input_rejections(stack, fixture, first_page, label):
    operation_id, cursor = fixture["operation_id"], first_page["next_cursor"]
    require(
        cursor is not None, "negative pagination tests require a real non-final cursor"
    )
    results = []
    for limit in (0, 193):
        result = operation(
            stack, "inspect", operation_id, f"{label}-limit-{limit}", limit=limit
        )
        exact_input_error(result, operation_id, cause=None)
        results.append(result)
    malformed = operation(
        stack, "inspect", operation_id, label + "-malformed-base64", cursor="%%%"
    )
    exact_input_error(malformed, operation_id, cause=None)
    results.append(malformed)
    decoded = bytearray(base64.b64decode(cursor, validate=True))
    decoded[-1] ^= 1
    corrupted = operation(
        stack,
        "inspect",
        operation_id,
        label + "-corrupt-cursor",
        cursor=base64.b64encode(decoded).decode(),
    )
    exact_input_error(corrupted, operation_id)
    results.append(corrupted)
    other_id = hashlib.sha256((operation_id + "other-operation").encode()).hexdigest()[
        :32
    ]
    other = operation(
        stack, "inspect", other_id, label + "-wrong-logical-id", cursor=cursor
    )
    exact_input_error(other, other_id)
    results.append(other)
    root_b = hashlib.sha256(
        (stack.root_id + "inspection-other-root").encode()
    ).hexdigest()[:32]
    config_b = dataclasses.replace(stack.config, root_id=root_b)
    stack.run(
        base.live.provision_command(config_b), label=label + "-provision-other-root"
    )
    stack.restart()
    other_root = operation(
        stack,
        "inspect",
        operation_id,
        label + "-wrong-root",
        cursor=cursor,
        config=config_b,
    )
    exact_input_error(other_root, operation_id)
    require(
        "entries" not in other_root,
        "wrong-root error exposes no ledger entries",
        other_root,
    )
    results.append(other_root)
    for action in ("inspect", "recover"):
        missing = operation(stack, action, other_id, label + "-missing-" + action)
        exact_input_error(missing, other_id, code="NotFound", cause="NotFound")
        results.append(missing)
    unchanged = operation(
        stack, "inspect", operation_id, label + "-first-page-after-errors"
    )
    require(
        all(
            unchanged.get(field) == first_page[field]
            for field in (
                "operation_token",
                "publication_token",
                "registered_count",
                "cleanup_cursor",
                "remaining_count",
            )
        ),
        "invalid scope, cursors and limits do not mutate the original ledger",
        unchanged,
    )
    return results


def finish_append(stack, fixture, label):
    operation_id = fixture["operation_id"]
    result = product.append(
        stack,
        fixture["workbench"],
        operation_id,
        fixture["payload"],
        label=label,
        max_size=BODY_LIMIT,
        block_size=BLOCK_SIZE,
    )
    require(
        result.get("status") == "success",
        "same logical append completes after public cleanup",
        result,
    )
    require(
        result["publication_operation_id"]
        != fixture["quarantine"]["publication_operation_id"],
        "append completion uses a successor after the failed child was cleaned",
        result,
    )
    product.exact_body(
        stack, fixture["workbench"], fixture["payload"], label + "-bytes"
    )
    replay = product.replay_unchanged(
        stack,
        fixture["workbench"],
        operation_id,
        fixture["payload"],
        result,
        label + "-replay",
        max_size=BODY_LIMIT,
        block_size=BLOCK_SIZE,
    )
    return {"receipt": result, "replay": replay}


def public_quarantine_recovery(
    stack,
    deadline,
    *,
    label="public-quarantine",
    count=1,
    fault_sequence=0,
    python_pages=False,
):
    fixture = create_quarantine(
        stack,
        label=label,
        count=count,
        fault_sequence=fault_sequence,
        deadline=deadline,
    )
    try:
        stack.restart()
        before = product.inventory(stack, label + "-inventory-before-inspect")
        inspected = inspect_all(
            stack, fixture, label + "-inspection", python_pages=python_pages
        )
        obsolete = reject_trusted_verdict(
            stack, fixture, inspected["pages"][0], label + "-trusted-verdict-rejected"
        )
        blocked = product.append(
            stack,
            fixture["workbench"],
            fixture["operation_id"],
            fixture["payload"],
            label=label + "-quarantined-append-blocked",
            max_size=BODY_LIMIT,
            block_size=BLOCK_SIZE,
        )
        require(
            blocked.get("status") == "error"
            and blocked.get("code") == "AppendUnresolved"
            and blocked.get("details", {}).get("operation_id")
            == fixture["operation_id"],
            "append cannot silently advance a quarantined child",
            blocked,
        )
        still_quarantined = product.operation_status(
            stack, fixture["operation_id"], label + "-still-quarantined"
        )
        require(
            still_quarantined.get("state") == "quarantined"
            and still_quarantined.get("publication_operation_id")
            == fixture["quarantine"]["publication_operation_id"]
            and still_quarantined.get("cleanup_retry_count") == 0,
            "quarantined append replay leaves the same child and retry generation",
            still_quarantined,
        )
        require(
            product.inventory(stack, label + "-inventory-after-inspect") == before,
            "metadata-only inspection leaves the real provider inventory unchanged",
        )
        release_provider(stack)
        digest = inspected["pages"][0]["operation_token"]["state_digest"]
        accepted = operation(
            stack,
            "recover",
            fixture["operation_id"],
            label + "-recover",
            expected_state_digest=digest,
        )
        cleanup_receipt(accepted, fixture, digest, 1)
        ready = await_state(
            stack,
            fixture["operation_id"],
            lambda s: s.get("state") == "ready_to_retry"
            and s.get("next_action") == "resubmit_same",
            label + "-ready",
            deadline,
        )
        require(
            ready["cleanup_retry_count"] == 1,
            "one cleanup request advances one retry generation",
            ready,
        )
        ready_noop = operation(
            stack, "recover", fixture["operation_id"], label + "-ready-noop"
        )
        require(
            ready_noop.get("status") == "success"
            and ready_noop.get("requested") is False
            and ready_noop.get("recovery_receipt") is None
            and ready_noop.get("state") == "ready_to_retry"
            and ready_noop.get("cleanup_retry_count") == 1,
            "fresh recovery of a cleaned attempt is a metadata-only no-op",
            ready_noop,
        )
        sealed = product.inventory(stack, label + "-sealed-inventory")
        require(
            all(
                sealed.get(
                    stack.object_root.rstrip("/") + "/" + row["object_identity"], [None]
                )[0]
                == 0
                for row in inspected["entries"]
            ),
            "owner retained a zero guard for every publicly inspected remaining key",
            sealed,
        )
        provider_reads = []
        for row in inspected["entries"]:
            key = stack.object_root.rstrip("/") + "/" + row["object_identity"]
            target = stack.evidence_dir / f"{label}-sealed-key-{row['sequence']}.data"
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
                    str(target),
                ),
                label=f"{label}-sealed-key-{row['sequence']}-read",
                env=stack.aws_env,
            )
            metadata = json.loads(read.stdout)
            require(
                target.read_bytes() == b""
                and metadata["ContentLength"] == 0
                and metadata.get("ETag"),
                "independent provider GET proves every inspected guard is empty",
                metadata,
            )
            provider_reads.append(
                {"key": key, "content_length": 0, "etag": metadata["ETag"]}
            )
        finished = finish_append(stack, fixture, label + "-same-id-completion")
        replay = operation(
            stack,
            "recover",
            fixture["operation_id"],
            label + "-same-token-replay",
            expected_state_digest=digest,
        )
        cleanup_receipt(replay, fixture, digest, 1)
        require(
            replay.get("replayed") is True,
            "historical cleanup acceptance is marked as replay",
            replay,
        )
        return product.case_result(
            safety="PASS",
            completion="PASS",
            fault=fixture["fault"],
            initial_quarantine=fixture["quarantine"],
            inspection=inspected,
            recovery=accepted,
            ready=ready,
            active_noop=fixture["active_noop"],
            ready_noop=ready_noop,
            historical_cleanup_replay=replay,
            obsolete_trusted_verdict=obsolete,
            blocked_append=blocked,
            verified_provider_reads=provider_reads,
            **finished,
        )
    finally:
        release_provider(stack)


def paginated_prefix_recovery(stack, deadline):
    label = "public-paged-prefix"
    fixture = create_quarantine(
        stack, label=label, count=257, fault_sequence=32, deadline=deadline
    )
    try:
        inspected = inspect_all(
            stack,
            fixture,
            label + "-mixed-sdk-pages",
            python_pages=True,
            reopen_after_first=True,
        )
        require(
            len(inspected["pages"]) > 1, "more than one actual CLI/Python page was read"
        )
        widest = inspect_all(stack, fixture, label + "-maximum-pages", limit=192)
        require(
            len(widest["pages"]) == 2 and widest["entries"] == inspected["entries"],
            "maximum-size pagination agrees with small mixed-SDK pages",
        )
        rejected_inputs = inspection_input_rejections(
            stack, fixture, inspected["pages"][0], label + "-inputs"
        )
        # The public first remaining key locates this controlled fixture's older
        # keys; they are checked as provider guards, never fabricated ledger rows.
        prefix = inspected["entries"][0]["object_identity"].rsplit("/", 1)[0]
        objects = product.inventory(stack, label + "-cleaned-prefix-guards")
        require(
            all(
                objects.get(
                    stack.object_root.rstrip("/") + "/" + prefix + f"/{index:016x}",
                    [None],
                )[0]
                == 0
                for index in range(32)
            ),
            "the durably retired prefix remains sealed",
            objects,
        )
        release_provider(stack)
        digest = inspected["pages"][0]["operation_token"]["state_digest"]
        stack.proxy.begin_rendezvous(2, operation_name="retry_append_cleanup")
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                calls = [
                    pool.submit(
                        caller,
                        stack,
                        "recover",
                        fixture["operation_id"],
                        label + "-" + name + "-recover",
                        expected_state_digest=digest,
                    )
                    for name, caller in (
                        ("python", python_operation),
                        ("cli", operation),
                    )
                ]
                concurrent_results = [
                    call.result(timeout=stack.timeout) for call in calls
                ]
            require(
                len(stack.proxy.rendezvous_entries) == 2,
                "two real recovery RPCs arrived before either was forwarded",
            )
        finally:
            stack.proxy.rendezvous = None
        for value in concurrent_results:
            cleanup_receipt(value, fixture, digest, 1)
        require(
            any(value.get("replayed") is True for value in concurrent_results),
            "one concurrent exact-token recovery is durably replayed",
            concurrent_results,
        )
        accepted = concurrent_results[0]
        ready = await_state(
            stack,
            fixture["operation_id"],
            lambda s: s.get("state") == "ready_to_retry",
            label + "-ready",
            deadline,
        )
        guards = product.inventory(stack, label + "-all-guards")
        require(
            all(
                guards.get(
                    stack.object_root.rstrip("/") + "/" + prefix + f"/{index:016x}",
                    [None],
                )[0]
                == 0
                for index in range(257)
            ),
            "owner seals and retains every old key across all pages",
            guards,
        )
        finished = finish_append(stack, fixture, label + "-complete")
        replay = operation(
            stack,
            "recover",
            fixture["operation_id"],
            label + "-cli-replay",
            expected_state_digest=digest,
        )
        cleanup_receipt(replay, fixture, digest, 1)
        require(
            replay.get("replayed") is True,
            "Python cleanup acceptance replays through CLI",
            replay,
        )
        return product.case_result(
            safety="PASS",
            completion="PASS",
            fault=fixture["fault"],
            cleaned_prefix=32,
            registered_count=257,
            mixed_pages=inspected,
            maximum_pages=widest,
            recovery=accepted,
            ready=ready,
            recovery_replay=replay,
            concurrent_recoveries=concurrent_results,
            recovery_requests_held_before_forward=2,
            rejected_inputs=rejected_inputs,
            **finished,
        )
    finally:
        release_provider(stack)


def recovery_ack_loss_and_aba(stack, deadline):
    label = "public-recovery-aba"
    fixture = create_quarantine(
        stack, label=label, count=3, fault_sequence=0, deadline=deadline
    )
    operation_id = fixture["operation_id"]
    try:
        first_pages = inspect_all(stack, fixture, label + "-first-inspection", limit=1)
        first_digest = first_pages["pages"][0]["operation_token"]["state_digest"]
        first_cursor = first_pages["pages"][0]["next_cursor"]
        require(
            isinstance(first_cursor, str) and bool(first_cursor),
            "the original attempt supplies a real non-final inspection cursor",
            first_pages["pages"][0],
        )
        caller = label + "-recover-caller"
        stack.proxy.arm("retry_append_cleanup", lambda: stack.kill_caller(caller))
        lost = operation(
            stack, "recover", operation_id, caller, expected_state_digest=first_digest
        )
        stack.proxy.wait_dropped()
        require(
            lost.get("status") == "caller_killed",
            "real recovery acceptance ACK was lost with caller SIGKILL",
            lost,
        )
        dropped = stack.proxy.last_drop
        require(
            dropped is not None
            and dropped.get("operation") == "retry_append_cleanup"
            and dropped.get("delivered_to_client") is False,
            "the original cleanup acceptance is captured before response loss",
            dropped,
        )
        original = dropped["response"]["payload"]
        original_body = original["outcome"]["body"]
        require(
            original["outcome"]["status"] == "success"
            and original_body["result"] == "append_cleanup_retried"
            and original.get("replayed") is False
            and isinstance(original.get("commit_version"), int)
            and original["commit_version"] > 0,
            "the dropped response contains the first durable cleanup acceptance",
            original,
        )
        original_receipt = {
            field: bytes(value).hex()
            if field
            in (
                "operation_id",
                "publication_operation_id",
                "expected_state_digest",
            )
            else value
            for field, value in original_body["value"].items()
        }
        cleanup_receipt(
            {
                "status": "success",
                "requested": True,
                "recovery_receipt": original_receipt,
            },
            fixture,
            first_digest,
            1,
        )
        again = await_state(
            stack,
            operation_id,
            lambda s: s.get("state") == "quarantined"
            and s.get("cleanup_retry_count") == 1,
            label + "-quarantine-count-one",
            deadline,
        )
        stack.restart()
        replay = operation(
            stack,
            "recover",
            operation_id,
            label + "-old-token-after-reopen",
            expected_state_digest=first_digest,
        )
        cleanup_receipt(replay, fixture, first_digest, 1)
        require(
            replay.get("replayed") is True
            and replay.get("commit_version") == original["commit_version"]
            and replay.get("cleanup_retry_count") == 1
            and replay.get("state") == "quarantined",
            "same token cannot start another cleanup round after ABA",
            replay,
        )
        python_replay = python_operation(
            stack,
            "recover",
            operation_id,
            label + "-python-old-token",
            expected_state_digest=first_digest,
        )
        cleanup_receipt(python_replay, fixture, first_digest, 1)
        require(
            python_replay.get("replayed") is True
            and python_replay.get("commit_version") == original["commit_version"]
            and python_replay.get("cleanup_retry_count") == 1,
            "Python repeats the original cleanup command receipt without advancing its counter",
            python_replay,
        )
        next_pages = inspect_all(stack, fixture, label + "-next-inspection", limit=1)
        next_digest = next_pages["pages"][0]["operation_token"]["state_digest"]
        require(
            next_digest != first_digest,
            "retry generation makes repeated quarantine tokens distinct",
        )
        stale = operation(
            stack,
            "inspect",
            operation_id,
            label + "-stale-first-round-cursor",
            cursor=first_cursor,
        )
        exact_input_error(
            stale,
            operation_id,
            code="Conflict",
            cause="Conflict",
            conflict="OperationState",
        )
        release_provider(stack)
        old_again = operation(
            stack,
            "recover",
            operation_id,
            label + "-old-token-provider-restored",
            expected_state_digest=first_digest,
        )
        cleanup_receipt(old_again, fixture, first_digest, 1)
        require(
            old_again.get("state") == "quarantined"
            and old_again.get("replayed") is True
            and old_again.get("commit_version") == original["commit_version"]
            and old_again.get("cleanup_retry_count") == 1,
            "restoring the provider does not turn an old token replay into a new request",
            old_again,
        )
        accepted = operation(
            stack,
            "recover",
            operation_id,
            label + "-new-token-recovery",
            expected_state_digest=next_digest,
        )
        cleanup_receipt(accepted, fixture, next_digest, 2)
        ready = await_state(
            stack,
            operation_id,
            lambda s: s.get("state") == "ready_to_retry"
            and s.get("cleanup_retry_count") == 2,
            label + "-ready-count-two",
            deadline,
        )
        require(
            ready.get("publication_operation_id")
            == fixture["quarantine"]["publication_operation_id"]
            and ready.get("attempt") == fixture["quarantine"]["attempt"]
            and ready.get("attempt_phase") == "cleaned"
            and ready.get("cleanup_retry_count") == 2
            and ready.get("receipt") is None
            and ready.get("next_action") == "resubmit_same",
            "both cleanup rounds belong to the original child before successor admission",
            ready,
        )
        finished = finish_append(stack, fixture, label + "-complete")

        def successor_observation(stage):
            status = product.operation_status(
                stack, operation_id, label + "-successor-cursor-status-" + stage
            )
            objects = product.inventory(
                stack, label + "-successor-cursor-objects-" + stage
            )
            # This independent public read includes the root metadata clock,
            # so even an unintended write that preserves visible fields fails.
            paths = base.raw_rpc(
                stack,
                {
                    "operation": "list_paths",
                    "request": {
                        "workbench": fixture["workbench"],
                        "prefix": None,
                        "recursive": True,
                        "view": "live",
                        "expected_read_version": None,
                        "workspace_continuation_fence": None,
                        "page": {"cursor": None, "limit": 32},
                    },
                },
                label + "-successor-cursor-paths-" + stage,
            )
            require(
                paths.get("status") == "success"
                and paths["body"]["result"] == "paths"
                and paths["body"]["value"]["read_version"] > 0
                and len(paths["body"]["value"]["entries"]) == 1
                and paths["body"]["value"]["next_cursor"] is None,
                "a complete public path listing supplies the metadata read version",
                paths,
            )
            return {"status": status, "objects": objects, "paths": paths["body"]}

        before_rejections = successor_observation("before")
        require(
            before_rejections["status"]["state"] == "committed"
            and before_rejections["status"]["attempt"] == ready["attempt"] + 1
            and before_rejections["status"]["publication_operation_id"]
            == finished["receipt"]["publication_operation_id"],
            "old-cursor rejection is exercised after the successor has published",
            before_rejections["status"],
        )
        successor_cli_rejection = operation(
            stack,
            "inspect",
            operation_id,
            label + "-old-cursor-after-successor-cli",
            limit=1,
            cursor=first_cursor,
        )
        exact_input_error(
            successor_cli_rejection,
            operation_id,
            code="Conflict",
            cause="Conflict",
            conflict="OperationState",
        )
        require(
            successor_cli_rejection.get("retryable") is False
            and "entries" not in successor_cli_rejection,
            "CLI rejects the old attempt cursor without returning successor ledger rows",
            successor_cli_rejection,
        )
        successor_python_rejection = python_operation(
            stack,
            "inspect",
            operation_id,
            label + "-old-cursor-after-successor-python",
            limit=1,
            cursor=first_cursor,
        )
        require(
            successor_python_rejection.get("status") == "error"
            and successor_python_rejection.get("type") == "AppendError"
            and successor_python_rejection.get("code") == "Conflict"
            and successor_python_rejection.get("cause_code") == "Conflict"
            and successor_python_rejection.get("operation_id") == operation_id
            and successor_python_rejection.get("next_action") == "query_same"
            and successor_python_rejection.get("retryable") is False
            and "entries" not in successor_python_rejection,
            "Python rejects the same old attempt cursor with the exact typed conflict",
            successor_python_rejection,
        )
        after_rejections = successor_observation("after")
        require(
            after_rejections == before_rejections,
            "old-cursor rejections preserve the metadata clock, append status, paths and object inventory",
            {"before": before_rejections, "after": after_rejections},
        )
        historical = operation(
            stack,
            "recover",
            operation_id,
            label + "-first-receipt-after-second-round",
            expected_state_digest=first_digest,
        )
        cleanup_receipt(historical, fixture, first_digest, 1)
        require(
            historical.get("state") == "committed"
            and historical.get("publication_operation_id")
            == finished["receipt"]["publication_operation_id"]
            and historical.get("publication_operation_id")
            != ready["publication_operation_id"]
            and historical.get("attempt") == ready["attempt"] + 1
            and historical.get("attempt_phase") == "published"
            and historical.get("cleanup_retry_count") == 0
            and historical.get("replayed") is True
            and historical.get("commit_version") == original["commit_version"]
            and historical.get("recovery_receipt") == original_receipt
            and product.canonical_receipt(historical["receipt"])
            == product.canonical_receipt(finished["receipt"]),
            "the published successor has its own zero cleanup count while the original recovery receipt replays unchanged",
            historical,
        )
        no_op = operation(stack, "recover", operation_id, label + "-committed-noop")
        require(
            no_op.get("status") == "success"
            and no_op.get("requested") is False
            and no_op.get("recovery_receipt") is None
            and product.canonical_receipt(no_op["receipt"])
            == product.canonical_receipt(finished["receipt"]),
            "recover of committed append is a metadata-only no-op",
            no_op,
        )
        return product.case_result(
            safety="PASS",
            completion="PASS",
            initial_fault=fixture["fault"],
            lost_recovery=lost,
            original_cleanup_receipt=original_receipt,
            original_cleanup_commit_version=original["commit_version"],
            repeated_quarantine=again,
            first_replay=replay,
            python_replay=python_replay,
            second_recovery=accepted,
            ready=ready,
            historical_first_receipt=historical,
            committed_noop=no_op,
            stale_cursor_rejected=stale,
            successor_cursor_rejections={
                "cursor": first_cursor,
                "cli": successor_cli_rejection,
                "python": successor_python_rejection,
                "before": before_rejections,
                "after": after_rejections,
            },
            **finished,
        )
    finally:
        release_provider(stack)


def owner_during_cleanup_seal(stack, deadline):
    label = "public-owner-mid-seal"
    fixture = create_quarantine(
        stack, label=label, count=3, fault_sequence=0, deadline=deadline
    )
    operation_id = fixture["operation_id"]
    try:
        inspected = inspect_all(stack, fixture, label + "-inspection")
        digest = inspected["pages"][0]["operation_token"]["state_digest"]
        release_provider(stack)
        with stack.object_proxy.lock:
            stack.object_proxy.seal_held.clear()
            stack.object_proxy.seal_release.clear()
            previous_faults = len(stack.object_proxy.fired)
            stack.object_proxy.rule = {
                "method": "PUT",
                "action": "hold_seal_success",
                "path_suffix": "/blocks/0000000000000001",
            }
        owner_pid = stack.owner.pid
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(
                operation,
                stack,
                "recover",
                operation_id,
                label + "-recover-before-owner-kill",
                expected_state_digest=digest,
            )
            try:
                require(
                    stack.object_proxy.seal_held.wait(stack.timeout),
                    "the owner reached a real successful conditional seal before its response was released",
                )
                faults = stack.object_proxy.fired[previous_faults:]
                require(
                    len(faults) == 1
                    and faults[0]["fault"] == "hold_seal_success"
                    and faults[0]["request_bytes"] == 0
                    and faults[0]["if_match"]
                    and 200 <= faults[0]["upstream_status"] < 300,
                    "real provider seal success is held at the owner crash boundary",
                    faults,
                )
                require(
                    stack.owner.pid == owner_pid and stack.owner.poll() is None,
                    "SIGKILL targets the owner currently waiting for the seal response",
                )
                stack.record(
                    "owner-cleanup-seal-fault.jsonl",
                    {
                        "at": base.live.now(),
                        "owner_pid": owner_pid,
                        "operation_id": operation_id,
                        "provider_response": faults[0],
                        "delivered_to_old_owner": False,
                    },
                )
                stack.restart()
            finally:
                stack.object_proxy.seal_release.set()
            first = future.result(timeout=stack.timeout)
        if first.get("status") == "success":
            cleanup_receipt(first, fixture, digest, 1)
        else:
            details = first.get("details", {})
            require(
                first.get("code") == "AppendCleanupUnresolved"
                and details.get("operation_id") == operation_id
                and details.get("expected_state_digest") == digest
                and details.get("next_action") == "retry_same_cleanup",
                "unknown recovery observation preserves the exact cleanup command identity",
                first,
            )
            if details.get("recovery_receipt") is not None:
                cleanup_receipt(
                    {
                        "status": "success",
                        "requested": True,
                        "recovery_receipt": details["recovery_receipt"],
                    },
                    fixture,
                    digest,
                    1,
                )
        replay = operation(
            stack,
            "recover",
            operation_id,
            label + "-same-token-after-owner-reopen",
            expected_state_digest=digest,
        )
        cleanup_receipt(replay, fixture, digest, 1)
        require(
            replay.get("replayed") is True,
            "same cleanup command replays after an owner died mid-seal",
            replay,
        )
        ready = await_state(
            stack,
            operation_id,
            lambda s: s.get("state") == "ready_to_retry"
            and s.get("cleanup_retry_count") == 1,
            label + "-ready",
            deadline,
        )
        guards = product.inventory(stack, label + "-all-old-guards")
        require(
            all(
                guards.get(
                    stack.object_root.rstrip("/") + "/" + row["object_identity"], [None]
                )[0]
                == 0
                for row in inspected["entries"]
            ),
            "new owner finishes sealing all old keys without reopening the completed key",
            guards,
        )
        finished = finish_append(stack, fixture, label + "-same-id-completion")
        return product.case_result(
            safety="PASS",
            completion="PASS",
            inspected=inspected,
            killed_owner_pid=owner_pid,
            reopened_owner_pid=stack.owner.pid,
            held_provider_success=faults[0],
            first_recovery_observation=first,
            recovery_replay=replay,
            ready=ready,
            **finished,
        )
    finally:
        stack.object_proxy.seal_release.set()
        release_provider(stack)


def main():
    cases = {
        "public_quarantine_recovery": public_quarantine_recovery,
        "paginated_prefix_recovery": paginated_prefix_recovery,
        "recovery_ack_loss_and_aba": recovery_ack_loss_and_aba,
        "owner_during_cleanup_seal": owner_during_cleanup_seal,
    }
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nokv-bin", type=Path, required=True)
    parser.add_argument("--source-dir", type=Path, required=True)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--python-executable", type=Path)
    parser.add_argument("--append-activity-lease-ms", type=int, default=1000)
    parser.add_argument("--completion-timeout-seconds", type=float, default=90)
    parser.add_argument("--timeout-seconds", type=int, default=120)
    parser.add_argument("--scenario", choices=("all", *cases), default="all")
    args = parser.parse_args()
    result = {
        "schema": "nokv.append_operations_acceptance.v1",
        "status": "NOT QUALIFIED",
        "started_at": base.live.now(),
        "command": sys.argv,
        "harness_path": str(Path(__file__).resolve()),
        "harness_sha256": base.live.digest_file(Path(__file__)),
        "scenarios": {},
        "qualification_scope": "Public CLI/Python inspection and explicit same-token cleanup retries on a real local Holt/RustFS owner.",
        "safety": "NOT QUALIFIED",
        "completion": "NOT QUALIFIED",
        "performance": "NOT QUALIFIED",
        "workspace_release": "NOT QUALIFIED",
        "unqualified_scopes": [
            "cross-host HA",
            "real demo queue redelivery (separate integration)",
            "public workspace deletion/reincarnation (no public lifecycle endpoint)",
            "long-term retention and high-cardinality GC",
            "workload-matched performance SLO",
        ],
    }
    stack, exit_code = None, 2
    try:
        require(
            sys.flags.optimize == 0, "qualification requires active Python assertions"
        )
        stack = product.ProductStack(
            args.evidence_dir,
            binary=args.nokv_bin,
            source=args.source_dir,
            label="append-operations",
            timeout=args.timeout_seconds,
            publication_lease_ms=args.append_activity_lease_ms,
        )
        stack.python_executable = args.python_executable
        with stack:
            shutil.copyfile(
                Path(__file__), args.evidence_dir / "executed-operations-harness.py"
            )
            result["machine_profile"] = product.record_machine_profile(stack)
            selected = (
                cases
                if args.scenario == "all"
                else {args.scenario: cases[args.scenario]}
            )
            for name, case in selected.items():
                print("START " + name, flush=True)
                started = time.monotonic()
                try:
                    value = case(stack, args.completion_timeout_seconds)
                except Exception as error:
                    value = product.case_result(
                        safety="FAIL"
                        if isinstance(error, product.ContractViolation)
                        else "NOT QUALIFIED",
                        completion="NOT QUALIFIED",
                        failure=repr(error),
                        traceback=traceback.format_exc(),
                    )
                value["elapsed_seconds"] = time.monotonic() - started
                result["scenarios"][name] = value
                stack.json("operations-results.json", result)
                print(
                    json.dumps(
                        {
                            "scenario": name,
                            "safety": value["safety"],
                            "completion": value["completion"],
                        }
                    ),
                    flush=True,
                )
                if value["safety"] != "PASS" or value["completion"] != "PASS":
                    break
            for dimension in ("safety", "completion"):
                statuses = [value[dimension] for value in result["scenarios"].values()]
                result[dimension] = (
                    "FAIL"
                    if "FAIL" in statuses
                    else "PASS"
                    if statuses and all(status == "PASS" for status in statuses)
                    else "NOT QUALIFIED"
                )
            result["all_listed_scenarios_executed"] = set(result["scenarios"]) == set(
                cases
            )
            result["status"] = (
                "FAIL"
                if "FAIL" in (result["safety"], result["completion"])
                else "PASS"
                if result["safety"] == result["completion"] == "PASS"
                else "NOT QUALIFIED"
            )
            result["qualified_scopes"] = [
                name
                for name, value in result["scenarios"].items()
                if value["safety"] == value["completion"] == "PASS"
            ]
            exit_code = {"PASS": 0, "FAIL": 1, "NOT QUALIFIED": 2}[result["status"]]
    except Exception as error:
        result.update(
            status="FAIL"
            if isinstance(error, product.ContractViolation)
            else "NOT QUALIFIED",
            error=repr(error),
            traceback=traceback.format_exc(),
        )
        exit_code = 1 if isinstance(error, product.ContractViolation) else 2
    finally:
        result["completed_at"] = base.live.now()
        args.evidence_dir.mkdir(parents=True, exist_ok=True)
        cleanup_file = args.evidence_dir / "cleanup.json"
        if cleanup_file.is_file():
            cleanup = json.loads(cleanup_file.read_text())
            if any(item.get("exit_code") != 0 for item in cleanup["results"]):
                result["cleanup_failure"] = cleanup
                result["status"], exit_code = "NOT QUALIFIED", 2
        (args.evidence_dir / "operations-results.json").write_text(
            json.dumps(result, indent=2) + "\n"
        )
        print(
            json.dumps(
                {"status": result["status"], "evidence": str(args.evidence_dir)}
            ),
            flush=True,
        )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
