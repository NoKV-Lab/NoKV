#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Black-box Workbench evidence through the primary native ``nokv`` CLI."""

from __future__ import annotations

import dataclasses
import json
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import Any, Iterable

import live_workbench as common
from source_bound_producer import ProducerError
from typed_live_qualification import gap_record, load_live_context, publish_live_result
from workbench_contract import (
    CONTRACT_SNAPSHOT_SCHEMA,
    WORKBENCH_TOOLS,
    WorkbenchContractError,
    contract_evidence,
    validate_tool_contract,
)


SCHEMA = "nokv.workbench.native_cli_evidence.v1"
CLI_TRANSCRIPT = "cli-transcript.jsonl"

Config = common.Config
Evidence = common.Evidence
ToolStep = common.ToolStep
TYPED_SCENARIOS = common.TYPED_SCENARIOS
TYPED_UNSUPPORTED_SCENARIOS = common.TYPED_UNSUPPORTED_SCENARIOS
TYPED_EVIDENCE_ROLES = ("producer-result", "qualification", "cli-transcript")


@dataclass(frozen=True)
class CliInvocation:
    """One direct ``nokv workbench`` process and its raw terminal streams."""

    label: str
    tool: str
    arguments: dict[str, Any]
    command: tuple[str, ...]
    started_at: str
    finished_at: str
    returncode: int
    stdout: str
    stderr: str


@dataclass
class CliTranscript:
    """One monotonic sequence shared by every direct CLI subprocess."""

    next_sequence: int = 1

    def allocate(self) -> int:
        sequence = self.next_sequence
        self.next_sequence += 1
        return sequence


def schema_command(config: Config) -> list[str]:
    return [str(config.binary), "schema"]


def workbench_command(config: Config, step: ToolStep) -> list[str]:
    """Build one argv-only direct CLI tool invocation without a shell."""

    return [
        *common.client_args(config),
        "workbench",
        step.name,
        common.canonical_json(step.arguments),
    ]


def _stream_text(value: str | bytes | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def _json_object(value: str, field: str) -> dict[str, Any]:
    if not value.strip():
        raise common.WorkflowFailure(f"{field} is empty")
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError as error:
        raise common.WorkflowFailure(f"{field} is not valid JSON") from error
    if not isinstance(decoded, dict):
        raise common.WorkflowFailure(f"{field} must be one JSON object")
    return decoded


def verify_native_cli_schema(config: Config, evidence: Evidence) -> None:
    """Verify that this exact executable exports the frozen 18-tool schemas."""

    completed = common.completed_process(
        evidence, "native-cli-schema", schema_command(config), config
    )
    payload = _json_object(completed.stdout, "native CLI schema stdout")
    if set(payload) != {"schema", "tools"}:
        raise common.WorkflowFailure("native CLI schema response has unexpected fields")
    if payload["schema"] != CONTRACT_SNAPSHOT_SCHEMA:
        raise common.WorkflowFailure("native CLI schema marker differs from the contract")
    tools = payload["tools"]
    if not isinstance(tools, list) or not all(
        isinstance(tool, dict) for tool in tools
    ):
        raise common.WorkflowFailure("native CLI schema response lacks a tool array")
    try:
        validate_tool_contract(tools, schema_key="input_schema")
        contract = contract_evidence(tools, schema_key="input_schema")
    except WorkbenchContractError as error:
        raise common.WorkflowFailure(
            f"native CLI schema differs from the contract: {error}"
        ) from error
    contract["transport"] = "native-cli"
    evidence.json("contract.json", contract)


def _error_json(stderr: str, label: str) -> dict[str, Any]:
    """Decode the exact JSON error envelope emitted by ``nokv`` main."""

    text = stderr.strip()
    prefix = "nokv: "
    if not text.startswith(prefix) or "\n" in text:
        raise common.WorkflowFailure(
            f"{label} did not return one native CLI JSON error envelope"
        )
    return _json_object(text.removeprefix(prefix), f"{label} stderr error envelope")


class NativeCli:
    """Invoke tools over the native CLI and retain a reviewable transcript."""

    def __init__(
        self,
        config: Config,
        evidence: Evidence,
        transcript: CliTranscript | None = None,
    ) -> None:
        self.config = config
        self.evidence = evidence
        self.transcript = transcript or CliTranscript()

    def _record(self, invocation: CliInvocation) -> None:
        sequence = self.transcript.allocate()
        raw_arguments = common.canonical_json(invocation.arguments)
        response: dict[str, Any] | None = None
        response_source: str | None = None
        if invocation.returncode == 0:
            try:
                response = _json_object(invocation.stdout, f"{invocation.label} stdout")
                response_source = "stdout"
            except common.WorkflowFailure:
                pass
        else:
            try:
                response = _error_json(invocation.stderr, invocation.label)
                response_source = "stderr"
            except common.WorkflowFailure:
                pass
        record = {
            "schema": SCHEMA,
            "transport": "native-cli",
            "sequence": sequence,
            "label": invocation.label,
            "tool": invocation.tool,
            "argv": common.redact_argv(invocation.command),
            "arguments_raw": raw_arguments,
            "arguments": json.loads(raw_arguments),
            "started_at": invocation.started_at,
            "finished_at": invocation.finished_at,
            "returncode": invocation.returncode,
            "stdout_raw": invocation.stdout,
            "stderr_raw": invocation.stderr,
            "response_source": response_source,
            "response": response,
        }
        self.evidence.line(CLI_TRANSCRIPT, record)
        self.evidence.line(
            "processes.jsonl",
            {
                "schema": SCHEMA,
                "label": f"native-cli:{invocation.label}",
                "tool": invocation.tool,
                "argv": common.redact_argv(invocation.command),
                "started_at": invocation.started_at,
                "finished_at": invocation.finished_at,
                "returncode": invocation.returncode,
                "stdout": invocation.stdout,
                "stderr": invocation.stderr,
            },
        )

    def execute(self, label: str, step: ToolStep) -> CliInvocation:
        command = workbench_command(self.config, step)
        started = common.now()
        try:
            completed = subprocess.run(
                command,
                cwd=self.config.repo,
                text=True,
                capture_output=True,
                timeout=self.config.timeout,
                check=False,
            )
        except subprocess.TimeoutExpired as error:
            finished = common.now()
            stdout, stderr = _stream_text(error.stdout), _stream_text(error.stderr)
            self.evidence.line(
                CLI_TRANSCRIPT,
                {
                    "schema": SCHEMA,
                    "transport": "native-cli",
                    "sequence": self.transcript.allocate(),
                    "label": label,
                    "tool": step.name,
                    "argv": common.redact_argv(command),
                    "arguments_raw": common.canonical_json(step.arguments),
                    "arguments": json.loads(common.canonical_json(step.arguments)),
                    "started_at": started,
                    "finished_at": finished,
                    "timed_out": True,
                    "stdout_raw": stdout,
                    "stderr_raw": stderr,
                },
            )
            self.evidence.line(
                "processes.jsonl",
                {
                    "schema": SCHEMA,
                    "label": f"native-cli:{label}",
                    "tool": step.name,
                    "argv": common.redact_argv(command),
                    "started_at": started,
                    "finished_at": finished,
                    "timed_out": True,
                    "stdout": stdout,
                    "stderr": stderr,
                },
            )
            raise common.WorkflowFailure(f"{label} timed out") from error
        invocation = CliInvocation(
            label=label,
            tool=step.name,
            arguments=step.arguments,
            command=tuple(command),
            started_at=started,
            finished_at=common.now(),
            returncode=completed.returncode,
            stdout=completed.stdout,
            stderr=completed.stderr,
        )
        self._record(invocation)
        return invocation

    def call(self, step: ToolStep) -> dict[str, Any]:
        invocation = self.execute(step.label, step)
        if step.error_code is not None:
            if invocation.returncode == 0:
                raise common.WorkflowFailure(f"{step.label} unexpectedly succeeded")
            if invocation.stdout.strip():
                raise common.WorkflowFailure(
                    f"{step.label} wrote stdout instead of a native CLI error"
                )
            result = _error_json(invocation.stderr, step.label)
            if (
                result.get("status") != "error"
                or result.get("code") != step.error_code
            ):
                raise common.WorkflowFailure(
                    f"{step.label} did not return {step.error_code}"
                )
            return result

        if invocation.returncode != 0:
            detail = invocation.stderr.strip() or invocation.stdout.strip() or "no output"
            raise common.WorkflowFailure(
                f"{step.label} failed ({invocation.returncode}): {detail}"
            )
        if invocation.stderr.strip():
            raise common.WorkflowFailure(f"{step.label} wrote unexpected stderr")
        result = _json_object(invocation.stdout, f"{step.label} stdout")
        if result.get("status") != "success":
            raise common.WorkflowFailure(f"{step.label} failed: {result}")
        common.reject_internal_keys(result, step.label)
        return result


def wait_for_server(
    config: Config,
    evidence: Evidence,
    server: subprocess.Popen[str],
    transcript: CliTranscript,
) -> NativeCli:
    """Wait through a valid direct CLI request, never an invalid raw socket."""

    probe_config = dataclasses.replace(config, timeout=min(config.timeout, 5))
    cli = NativeCli(probe_config, evidence, transcript)
    probe = ToolStep(
        "native-cli-readiness",
        "workbench_find",
        {"committed": True, "limit": 1},
    )
    deadline = time.monotonic() + config.timeout
    last_error = ""
    while time.monotonic() < deadline:
        if server.poll() is not None:
            raise common.WorkflowFailure(
                f"serve exited during native CLI startup ({server.returncode})"
            )
        invocation = cli.execute(probe.label, probe)
        if invocation.returncode == 0 and not invocation.stderr.strip():
            try:
                result = _json_object(invocation.stdout, f"{probe.label} stdout")
            except common.WorkflowFailure as error:
                last_error = str(error)
            else:
                if result.get("status") == "success":
                    evidence.line(
                        "processes.jsonl",
                        {
                            "schema": SCHEMA,
                            "label": "native-cli-server-ready",
                            "tool": probe.name,
                            "finished_at": common.now(),
                        },
                    )
                    return NativeCli(config, evidence, transcript)
                last_error = f"readiness result was not success: {result}"
        else:
            last_error = (
                invocation.stderr.strip()
                or invocation.stdout.strip()
                or f"exit {invocation.returncode}"
            )
        time.sleep(0.25)
    raise common.WorkflowFailure(
        "serve did not accept a native CLI request before timeout: " + last_error
    )


def assert_native_authority_results(
    results: dict[str, dict[str, Any]],
    mismatch: CliInvocation,
    config: Config,
    peer: Config,
) -> dict[str, Any]:
    if results["peer-read-before-create"].get("code") != "NotFound":
        raise common.WorkflowFailure(
            "peer RootId observed the primary Workbench before creation"
        )
    peer_put = results["peer-put"]
    if (
        peer_put.get("workbench_id") != config.workbench
        or peer_put.get("generation") != 1
        or peer_put.get("replace") is not False
    ):
        raise common.WorkflowFailure(
            "peer RootId did not create an independent same-name Workbench"
        )
    peer_document = common.document(results["peer-read"], "peer authority read")
    reconnect_document = common.document(
        results["peer-reconnect-read"], "peer authority reconnect read"
    )
    primary_document = common.document(
        results["primary-read-after-peer-write"], "primary authority read"
    )
    if peer_document != {"authority": "peer"} or reconnect_document != peer_document:
        raise common.WorkflowFailure("peer Agent binding did not survive an exact reconnect")
    if (
        primary_document.get("state") != "post-snapshot"
        or "authority" in primary_document
    ):
        raise common.WorkflowFailure(
            "RootId isolation allowed a peer write into the primary Workbench"
        )

    combined_error = f"{mismatch.stdout}\n{mismatch.stderr}"
    if (
        mismatch.returncode == 0
        or mismatch.stdout.strip()
        or "already bound to another Agent" not in combined_error
        or "jsonrpc" in combined_error.lower()
    ):
        raise common.WorkflowFailure(
            "wrong AgentId was not rejected before native CLI tool dispatch"
        )
    if config.agent_id in combined_error or peer.agent_id in combined_error:
        raise common.WorkflowFailure(
            "Agent binding mismatch disclosed a stable Agent identity"
        )
    return {
        "schema": SCHEMA,
        "status": "PASS",
        "contract_id": "C06",
        "workbench_id": config.workbench,
        "distinct_root_count": 2,
        "same_logical_shard": True,
        "peer_reconnect": "PASS",
        "wrong_agent_admission": "rejected-before-native-cli-dispatch",
    }


def run_authority_probe(
    config: Config,
    evidence: Evidence,
    server: subprocess.Popen[str],
    primary: NativeCli,
) -> dict[str, Any]:
    peer, mismatch_config = common.authority_configs(config)
    peer_cli = NativeCli(peer, evidence, primary.transcript)
    mismatch_cli = NativeCli(mismatch_config, evidence, primary.transcript)
    results: dict[str, dict[str, Any]] = {}
    peer_payload = common.canonical_json({"authority": "peer"}) + "\n"

    common.require_running("serve", server)
    results["peer-read-before-create"] = peer_cli.call(
        ToolStep(
            "peer-read-before-create",
            "workbench_read",
            {"id": config.workbench, "section": "input", "path": "scan.json"},
            "NotFound",
        )
    )
    results["peer-put"] = peer_cli.call(
        ToolStep(
            "peer-put",
            "workbench_put_file",
            {
                "id": config.workbench,
                "section": "input",
                "path": "scan.json",
                "text": peer_payload,
                "content_type": "application/json",
                "replace": False,
            },
        )
    )
    results["peer-read"] = peer_cli.call(
        ToolStep(
            "peer-read",
            "workbench_read",
            {"id": config.workbench, "section": "input", "path": "scan.json"},
        )
    )
    # Every NativeCli.call starts a new subprocess, so this is a real direct
    # CLI reconnect rather than a reuse of process-local client state.
    results["peer-reconnect-read"] = peer_cli.call(
        ToolStep(
            "peer-reconnect-read",
            "workbench_read",
            {"id": config.workbench, "section": "input", "path": "scan.json"},
        )
    )
    results["primary-read-after-peer-write"] = primary.call(
        ToolStep(
            "primary-read-after-peer-write",
            "workbench_read",
            {"id": config.workbench, "section": "input", "path": "scan.json"},
        )
    )
    mismatch = mismatch_cli.execute(
        "native-cli-authority-mismatch",
        ToolStep(
            "native-cli-authority-mismatch",
            "workbench_read",
            {"id": config.workbench, "section": "input", "path": "scan.json"},
        ),
    )
    return assert_native_authority_results(results, mismatch, config, peer)


def run_live(config: Config, evidence: Evidence, steps: list[ToolStep]) -> None:
    verify_native_cli_schema(config, evidence)
    provision = common.completed_process(
        evidence, "provision", common.provision_command(config), config
    )
    if json.loads(provision.stdout).get("lifecycle") != "active":
        raise common.WorkflowFailure("provision did not activate root placement")
    peer, _ = common.authority_configs(config)
    peer_provision = common.completed_process(
        evidence, "provision-authority-peer", common.provision_command(peer), peer
    )
    if json.loads(peer_provision.stdout).get("lifecycle") != "active":
        raise common.WorkflowFailure("peer provision did not activate root placement")

    serve_out = (evidence.root / "serve.stdout.log").open("w")
    serve_err = (evidence.root / "serve.stderr.log").open("w")
    server = subprocess.Popen(
        common.server_command(config),
        cwd=config.repo,
        stdin=subprocess.DEVNULL,
        stdout=serve_out,
        stderr=serve_err,
        text=True,
        start_new_session=True,
    )
    evidence.line(
        "processes.jsonl",
        {
            "schema": SCHEMA,
            "label": "serve",
            "argv": common.redact_argv(common.server_command(config)),
            "pid": server.pid,
            "started_at": common.now(),
        },
    )
    try:
        transcript = CliTranscript()
        cli = wait_for_server(config, evidence, server, transcript)
        results: dict[str, dict[str, Any]] = {}
        for step in steps:
            results[step.label] = cli.call(step)
            if step.label == "grep-phase1-page-1":
                continuation = common.grep_continuation_step(step, results[step.label])
                results[continuation.label] = cli.call(continuation)
            if step.label == "edit-input":
                common.transfer(config, evidence)
        phase_one_evidence = common.assert_results(results, config)
        authority_evidence = run_authority_probe(config, evidence, server, cli)
        common.require_running("serve", server)
        evidence.json("phase1-contracts.json", phase_one_evidence)
        evidence.json("authority-contracts.json", authority_evidence)
    finally:
        try:
            evidence.line(
                "processes.jsonl",
                {
                    "schema": SCHEMA,
                    "label": "serve-exit",
                    "returncode": common.stop(server),
                    "finished_at": common.now(),
                },
            )
        finally:
            serve_out.close()
            serve_err.close()


def environment(config: Config) -> dict[str, Any]:
    value = common.environment(config)
    value["schema"] = SCHEMA
    value["runner"] = {"transport": "native-cli", "entrypoint": __file__}
    return value


def plan(config: Config, steps: Iterable[ToolStep]) -> dict[str, Any]:
    steps = list(steps)
    sandbox = config.evidence / "sandbox"
    peer, mismatch = common.authority_configs(config)
    coverage = common.planned_tool_coverage(steps)
    return {
        "schema": SCHEMA,
        "mode": "dry-run" if config.dry_run else "live",
        "transport": "native-cli",
        "commands": {
            "build": ["cargo", "build", "-p", "nokv", "--bin", "nokv"]
            if config.build
            else None,
            "native_cli_schema": common.redact_argv(schema_command(config)),
            "provision": common.redact_argv(common.provision_command(config)),
            "provision_authority_peer": common.redact_argv(
                common.provision_command(peer)
            ),
            "serve": common.redact_argv(common.server_command(config)),
            "native_cli_readiness": common.redact_argv(
                workbench_command(
                    config,
                    ToolStep(
                        "native-cli-readiness",
                        "workbench_find",
                        {"committed": True, "limit": 1},
                    ),
                )
            ),
            "native_cli_authority_peer": common.redact_argv(
                [*common.client_args(peer), "workbench", "<tool>", "<json>"]
            ),
            "native_cli_authority_mismatch": common.redact_argv(
                [*common.client_args(mismatch), "workbench", "<tool>", "<json>"]
            ),
            "materialize": common.redact_argv(
                common.materialize_command(config, sandbox / "scan.json")
            ),
            "collect": common.redact_argv(
                common.collect_command(config, sandbox / "reconstruction.json")
            ),
        },
        "tool_commands": [
            {
                "label": step.label,
                "tool": step.name,
                "arguments": json.loads(common.canonical_json(step.arguments)),
                "argv": common.redact_argv(workbench_command(config, step)),
            }
            for step in steps
        ],
        "dynamic_tool_steps": [
            {
                "label": "grep-phase1-page-2",
                "cursor_from": "grep-phase1-page-1.next_cursor",
                "transport": "native-cli",
            }
        ],
        "tool_coverage": {
            "expected": sorted(WORKBENCH_TOOLS),
            "planned": sorted(coverage),
            "count": len(coverage),
            "complete": coverage == WORKBENCH_TOOLS,
        },
    }


def qualification(
    state: str, reason: str, workflow: str, transcript: str | None = None
) -> dict[str, Any]:
    gate_zero = "FAIL" if state == "FAIL" else "NOT QUALIFIED"
    gate_reason = reason
    if workflow == "PASS":
        gate_reason = (
            "The direct native CLI 18-tool workflow passed, but the one-day "
            "snapshot lease did not expire and reach reaped state; Gate 0 is "
            "partial evidence."
        )
    return {
        "schema": SCHEMA,
        "recorded_at": common.now(),
        "overall_status": state,
        "reason": reason,
        "workbench_workflow": {
            "status": workflow,
            "transport": "native-cli",
            "transcript_sha256": transcript,
        },
        "acceptance_gates": {
            str(index): {
                "status": gate_zero if index == 0 else "NOT QUALIFIED",
                "reason": gate_reason
                if index == 0
                else "This native CLI harness does not qualify this gate.",
            }
            for index in range(9)
        },
    }


def parse_args(argv: list[str] | None = None) -> Config:
    return common.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    config = parse_args(argv)
    evidence, steps, prepared = Evidence(config.evidence), common.tool_plan(config), False
    typed_context = None

    def finish(code: int, record: dict[str, Any]) -> int:
        if typed_context is None or config.qualification_result is None:
            return code
        outcome = "PASS" if code == 0 else "NQ" if code == 3 else "FAIL"
        transcript_path = evidence.root / CLI_TRANSCRIPT
        transcript = transcript_path.read_bytes() if transcript_path.is_file() else None
        try:
            publish_live_result(
                result_path=config.qualification_result,
                context=typed_context,
                outcome=outcome,
                qualification=record,
                evidence_roles=TYPED_EVIDENCE_ROLES,
                transcript=transcript,
            )
        except (OSError, ProducerError) as error:
            print(f"FAIL: {error}", file=sys.stderr)
            return 2
        return code

    try:
        if config.qualification_result is not None:
            if config.evidence == config.qualification_result.parent:
                raise ProducerError(
                    "native CLI workflow evidence must not overlap typed direct-child evidence"
                )
            typed_context = load_live_context(
                producer_id="live-workbench",
                scenarios=TYPED_SCENARIOS,
                dependency_names=("etcd", "object-store"),
                product_binary=config.binary,
                evidence_roles=TYPED_EVIDENCE_ROLES,
            )
            unsupported = sorted(
                set(typed_context.scenarios).intersection(TYPED_UNSUPPORTED_SCENARIOS)
            )
            if unsupported:
                reason = (
                    "The direct native CLI 18-tool harness does not execute the generic "
                    "seven-tool MCP profile, direct RootId WorkspaceClient, or current "
                    f"operational CLI surfaces required by {unsupported}."
                )
                record = gap_record(producer="live-workbench", reason=reason)
                print(json.dumps(record, indent=2, sort_keys=True))
                return finish(3, record)
        evidence.prepare()
        prepared = True
        evidence.json("plan.json", plan(config, steps))
        if common.planned_tool_coverage(steps) != WORKBENCH_TOOLS:
            raise common.WorkflowFailure("tool plan does not cover exactly 18 tools")
        common.validate(config, live=False)
        if config.dry_run:
            record = qualification(
                "NOT QUALIFIED",
                "Dry-run validated direct native CLI commands and exact 18-tool "
                "coverage; no live dependency ran.",
                "NOT QUALIFIED",
            )
            evidence.json("qualification.json", record)
            print(json.dumps(record, indent=2, sort_keys=True))
            return finish(0, record)
        if config.build:
            if shutil.which("cargo") is None:
                raise common.NotQualified("cargo is unavailable for --build")
            old_timeout = config.timeout
            config = dataclasses.replace(config, timeout=max(old_timeout, 900))
            common.completed_process(
                evidence,
                "build",
                ["cargo", "build", "-p", "nokv", "--bin", "nokv"],
                config,
            )
            config = dataclasses.replace(config, timeout=old_timeout)
        common.validate(config, live=True)
        evidence.json("environment.json", environment(config))
        run_live(config, evidence, steps)
        transcript = common.digest_file(evidence.root / CLI_TRANSCRIPT)
        record = qualification(
            "NOT QUALIFIED",
            "Direct native CLI Workbench workflow passed; full system acceptance "
            "requires the remaining gates.",
            "PASS",
            transcript,
        )
        evidence.json("qualification.json", record)
        print(json.dumps(record, indent=2, sort_keys=True))
        return finish(0, record)
    except common.NotQualified as error:
        record = qualification("NOT QUALIFIED", str(error), "NOT QUALIFIED")
        if prepared:
            evidence.json("qualification.json", record)
        print(json.dumps(record, indent=2, sort_keys=True))
        return finish(3, record)
    except (
        common.WorkflowFailure,
        OSError,
        ProducerError,
        ValueError,
        json.JSONDecodeError,
    ) as error:
        record = qualification("FAIL", str(error), "FAIL")
        if prepared:
            evidence.json("qualification.json", record)
        print(json.dumps(record, indent=2, sort_keys=True))
        return finish(2, record)


if __name__ == "__main__":
    raise SystemExit(main())
