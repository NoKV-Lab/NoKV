#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the primary native CLI Workbench evidence runner."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import native_cli_workbench as harness
from workbench_contract import CONTRACT_SNAPSHOT_SCHEMA, FROZEN_INPUT_SCHEMAS


REPO = Path(__file__).resolve().parents[2]


def config(evidence_dir: Path) -> harness.Config:
    return harness.parse_args(["--dry-run", "--evidence-dir", str(evidence_dir)])


class NativeCliWorkbenchTest(unittest.TestCase):
    def test_direct_command_uses_one_argv_json_argument(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            current = config(Path(directory) / "evidence")
        step = harness.ToolStep(
            "put",
            "workbench_put_file",
            {
                "id": "run-1",
                "section": "input",
                "path": "nested/雪.json",
                "text": "{\"ok\":true}",
                "replace": False,
            },
        )
        command = harness.workbench_command(current, step)
        self.assertEqual(command[0], str(current.binary))
        self.assertEqual(command[-3:-1], ["workbench", "workbench_put_file"])
        self.assertEqual(command[-1], harness.common.canonical_json(step.arguments))
        self.assertNotIn("mcp", command)

    def test_plan_has_exact_tool_coverage_and_no_sidecar_command(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            current = config(Path(directory) / "evidence")
            plan = harness.plan(current, harness.common.tool_plan(current))
        self.assertEqual(plan["transport"], "native-cli")
        self.assertEqual(plan["tool_coverage"]["count"], 18)
        self.assertTrue(plan["tool_coverage"]["complete"])
        self.assertTrue(plan["tool_commands"])
        self.assertTrue(
            all(
                command["argv"][-3:-1] == ["workbench", command["tool"]]
                for command in plan["tool_commands"]
            )
        )
        self.assertNotIn("mcp", harness.common.canonical_json(plan).lower())
        self.assertEqual(plan["commands"]["native_cli_schema"][-1], "schema")

    def test_native_binary_schema_is_frozen_before_tool_execution(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            current = config(root / "evidence")
            evidence = harness.Evidence(current.evidence)
            evidence.prepare()
            payload = {
                "schema": CONTRACT_SNAPSHOT_SCHEMA,
                "tools": [
                    {"name": name, "input_schema": schema}
                    for name, schema in FROZEN_INPUT_SCHEMAS.items()
                ],
            }
            completed = subprocess.CompletedProcess(
                args=[], returncode=0, stdout=json.dumps(payload), stderr=""
            )
            with mock.patch.object(
                harness.common, "completed_process", return_value=completed
            ) as process:
                harness.verify_native_cli_schema(current, evidence)
            contract = json.loads(
                (current.evidence / "contract.json").read_text(encoding="utf-8")
            )

        self.assertEqual(process.call_args.args[1], "native-cli-schema")
        self.assertEqual(process.call_args.args[2], harness.schema_command(current))
        self.assertEqual(contract["transport"], "native-cli")
        self.assertEqual(contract["tool_count"], 18)

    def test_successful_direct_call_records_exact_cli_transcript(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            current = config(root / "evidence")
            evidence = harness.Evidence(current.evidence)
            evidence.prepare()
            step = harness.ToolStep("create", "workbench_create", {"id": "run-1"})
            completed = subprocess.CompletedProcess(
                args=[],
                returncode=0,
                stdout='{"status":"success","workbench_id":"run-1"}\n',
                stderr="",
            )
            with mock.patch.object(harness.subprocess, "run", return_value=completed):
                result = harness.NativeCli(current, evidence).call(step)
            transcript = [
                json.loads(line)
                for line in (current.evidence / harness.CLI_TRANSCRIPT)
                .read_text(encoding="utf-8")
                .splitlines()
            ]

        self.assertEqual(result["workbench_id"], "run-1")
        self.assertEqual(len(transcript), 1)
        record = transcript[0]
        self.assertEqual(record["transport"], "native-cli")
        self.assertEqual(record["sequence"], 1)
        self.assertEqual(record["tool"], "workbench_create")
        self.assertEqual(record["arguments_raw"], '{"id":"run-1"}')
        self.assertEqual(record["response_source"], "stdout")
        self.assertEqual(record["response"], result)
        self.assertIsInstance(record["started_at"], str)
        self.assertIsInstance(record["finished_at"], str)

    def test_transcript_redacts_object_secret_from_direct_cli_argv(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            current = harness.dataclasses.replace(
                config(root / "evidence"), secret_key="do-not-record-this-secret"
            )
            evidence = harness.Evidence(current.evidence)
            evidence.prepare()
            completed = subprocess.CompletedProcess(
                args=[], returncode=0, stdout='{"status":"success"}\n', stderr=""
            )
            with mock.patch.object(harness.subprocess, "run", return_value=completed):
                harness.NativeCli(current, evidence).call(
                    harness.ToolStep("find", "workbench_find", {"limit": 1})
                )
            encoded = (current.evidence / harness.CLI_TRANSCRIPT).read_text(
                encoding="utf-8"
            )

        self.assertNotIn("do-not-record-this-secret", encoded)
        self.assertIn("<redacted>", encoded)

    def test_expected_tool_error_is_decoded_from_native_cli_stderr(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            current = config(root / "evidence")
            evidence = harness.Evidence(current.evidence)
            evidence.prepare()
            step = harness.ToolStep(
                "already-exists",
                "workbench_put_file",
                {"id": "run-1", "section": "input", "path": "a.txt", "text": "x"},
                "AlreadyExists",
            )
            completed = subprocess.CompletedProcess(
                args=[],
                returncode=1,
                stdout="",
                stderr=(
                    "nokv: {\"status\":\"error\",\"code\":\"AlreadyExists\","
                    "\"message\":\"exists\",\"retryable\":false,\"details\":{}}\n"
                ),
            )
            with mock.patch.object(harness.subprocess, "run", return_value=completed):
                result = harness.NativeCli(current, evidence).call(step)

        self.assertEqual(result["code"], "AlreadyExists")
        self.assertEqual(result["status"], "error")

    def test_malformed_native_cli_error_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            current = config(root / "evidence")
            evidence = harness.Evidence(current.evidence)
            evidence.prepare()
            step = harness.ToolStep(
                "already-exists",
                "workbench_put_file",
                {"id": "run-1", "section": "input", "path": "a.txt", "text": "x"},
                "AlreadyExists",
            )
            completed = subprocess.CompletedProcess(
                args=[], returncode=1, stdout="", stderr="nokv: not-json\n"
            )
            with (
                mock.patch.object(harness.subprocess, "run", return_value=completed),
                self.assertRaisesRegex(harness.common.WorkflowFailure, "not valid JSON"),
            ):
                harness.NativeCli(current, evidence).call(step)

    def test_server_readiness_uses_a_valid_direct_cli_probe(self) -> None:
        class Server:
            returncode = None

            @staticmethod
            def poll() -> None:
                return None

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            current = config(root / "evidence")
            evidence = harness.Evidence(current.evidence)
            evidence.prepare()
            completed = subprocess.CompletedProcess(
                args=[], returncode=0, stdout='{"status":"success"}\n', stderr=""
            )
            with mock.patch.object(harness.subprocess, "run", return_value=completed):
                client = harness.wait_for_server(
                    current, evidence, Server(), harness.CliTranscript()
                )
            transcript = [
                json.loads(line)
                for line in (current.evidence / harness.CLI_TRANSCRIPT)
                .read_text(encoding="utf-8")
                .splitlines()
            ]

        self.assertEqual(client.config, current)
        self.assertEqual(len(transcript), 1)
        self.assertEqual(transcript[0]["label"], "native-cli-readiness")
        self.assertEqual(transcript[0]["tool"], "workbench_find")
        self.assertEqual(transcript[0]["response_source"], "stdout")

    def test_transcript_sequence_is_shared_across_root_authority_clients(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            current = config(root / "evidence")
            peer, _ = harness.common.authority_configs(current)
            evidence = harness.Evidence(current.evidence)
            evidence.prepare()
            transcript = harness.CliTranscript()
            completed = subprocess.CompletedProcess(
                args=[], returncode=0, stdout='{"status":"success"}\n', stderr=""
            )
            with mock.patch.object(harness.subprocess, "run", return_value=completed):
                harness.NativeCli(current, evidence, transcript).call(
                    harness.ToolStep("primary", "workbench_find", {"limit": 1})
                )
                harness.NativeCli(peer, evidence, transcript).call(
                    harness.ToolStep("peer", "workbench_find", {"limit": 1})
                )
            sequences = [
                json.loads(line)["sequence"]
                for line in (current.evidence / harness.CLI_TRANSCRIPT)
                .read_text(encoding="utf-8")
                .splitlines()
            ]

        self.assertEqual(sequences, [1, 2])

    def test_source_bound_unsupported_claims_publish_a_cli_transcript_gap(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = root / "typed" / "producer-result.json"
            unsupported = next(iter(harness.TYPED_UNSUPPORTED_SCENARIOS))
            context = SimpleNamespace(scenarios=(unsupported,))
            with (
                mock.patch.object(harness, "load_live_context", return_value=context),
                mock.patch.object(harness, "publish_live_result") as publish,
                redirect_stdout(StringIO()),
            ):
                status = harness.main(
                    [
                        "--nokv-bin",
                        str(root / "nokv"),
                        "--qualification-result",
                        str(result),
                        "--evidence-dir",
                        str(root / "workflow"),
                    ]
                )

        self.assertEqual(status, 3)
        self.assertEqual(publish.call_args.kwargs["outcome"], "NQ")
        self.assertEqual(
            publish.call_args.kwargs["evidence_roles"],
            ("producer-result", "qualification", "cli-transcript"),
        )
        self.assertIn(
            unsupported, publish.call_args.kwargs["qualification"]["reason"]
        )

    def test_qualification_identifies_the_primary_transport(self) -> None:
        record = harness.qualification(
            "NOT QUALIFIED", "bounded live workflow passed", "PASS", "ab" * 32
        )
        self.assertEqual(record["workbench_workflow"]["status"], "PASS")
        self.assertEqual(record["workbench_workflow"]["transport"], "native-cli")
        self.assertEqual(record["acceptance_gates"]["0"]["status"], "NOT QUALIFIED")
        self.assertIn("direct native CLI", record["acceptance_gates"]["0"]["reason"])

    def test_dry_run_writes_a_native_cli_plan(self) -> None:
        script = Path(__file__).with_name("native_cli_workbench.py")
        with tempfile.TemporaryDirectory() as directory:
            evidence = Path(directory) / "evidence"
            completed = subprocess.run(
                [
                    sys.executable,
                    str(script),
                    "--dry-run",
                    "--evidence-dir",
                    str(evidence),
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            plan = json.loads((evidence / "plan.json").read_text(encoding="utf-8"))
            qualification = json.loads(
                (evidence / "qualification.json").read_text(encoding="utf-8")
            )

        self.assertEqual(plan["transport"], "native-cli")
        self.assertTrue(plan["tool_coverage"]["complete"])
        self.assertEqual(qualification["workbench_workflow"]["transport"], "native-cli")
        self.assertEqual(qualification["overall_status"], "NOT QUALIFIED")

    def test_required_rust_job_runs_and_retains_native_cli_evidence(self) -> None:
        workflow = (REPO / ".github/workflows/rust.yml").read_text(encoding="utf-8")
        for required in (
            "id: native_cli_workbench",
            "python3 scripts/workbench/native_cli_workbench.py",
            "--agent-name ci-native-cli-agent",
            "--server-bind 127.0.0.1:17751",
            'jq -e \'.workbench_workflow.transport == "native-cli"\'',
            "native-cli-workbench-${{ github.sha }}",
            "if: ${{ always() && steps.native_cli_workbench.outcome != 'skipped' }}",
        ):
            with self.subTest(required=required):
                self.assertIn(required, workflow)


if __name__ == "__main__":
    unittest.main()
