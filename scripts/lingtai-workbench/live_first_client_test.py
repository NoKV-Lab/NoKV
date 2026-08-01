#!/usr/bin/env python3
"""Unit tests for the first-client live evidence harness."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import live_first_client as harness


REQUIRED_FIRST_OCCURRENCE_ORDER = [
    "workbench_create",
    "workbench_put_file",
    "workbench_append",
    "workbench_read",
    "workbench_stat",
    "workbench_list",
    "workbench_grep",
    "workbench_edit",
    "workbench_search",
    "workbench_aggregate",
    "workbench_catalog",
    "workbench_commit",
    "workbench_snapshot",
    "workbench_snapshot_list",
    "workbench_snapshot_renew",
    "workbench_restore",
    "workbench_find",
    "workbench_snapshot_retire",
]


def config(evidence_dir: Path) -> harness.Config:
    return harness.parse_args(
        [
            "--dry-run",
            "--evidence-dir",
            str(evidence_dir),
        ]
    )


class LiveFirstClientTest(unittest.TestCase):
    def test_plan_covers_exact_eighteen_tools_in_dependency_order(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            steps = harness.tool_plan(config(Path(directory) / "evidence"))
        self.assertEqual(harness.planned_tool_coverage(steps), harness.WORKBENCH_TOOLS)
        first_occurrences = list(dict.fromkeys(step.name for step in steps))
        self.assertEqual(first_occurrences, REQUIRED_FIRST_OCCURRENCE_ORDER)
        self.assertEqual(len(first_occurrences), 18)

    def test_flat_commands_and_plan_have_no_superseded_surface(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            current = config(Path(directory) / "evidence")
            commands = [
                harness.provision_command(current),
                harness.server_command(current),
                harness.mcp_command(current),
                harness.materialize_command(current, Path(directory) / "input.json"),
                harness.collect_command(current, Path(directory) / "output.json"),
            ]
            encoded = harness.canonical_json(
                {
                    "commands": commands,
                    "steps": [step.arguments for step in harness.tool_plan(current)],
                }
            ).lower()
        for command in commands:
            self.assertEqual(command[0], str(current.binary))
        for forbidden in ("fuse", "posix", "fsspec", "inode", "dentry", "yanex"):
            self.assertNotIn(forbidden, encoded)

    def test_secret_values_are_hashed_and_not_retained(self) -> None:
        secret = "do-not-record-this-secret"
        redacted = harness.redact_argv(
            ["nokv", "--object-secret-access-key", secret, "mcp"]
        )
        self.assertNotIn(secret, redacted)
        self.assertTrue(redacted[2].startswith("<redacted:sha256:"))

    def test_nested_internal_storage_identity_is_rejected(self) -> None:
        with self.assertRaises(harness.WorkflowFailure):
            harness.reject_internal_keys(
                {"result": [{"workspace_incarnation_id": "internal"}]},
                "fixture",
            )

    def test_dry_run_writes_not_qualified_evidence_and_complete_coverage(self) -> None:
        script = Path(__file__).with_name("live_first_client.py")
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
        self.assertEqual(plan["tool_coverage"]["count"], 18)
        self.assertTrue(plan["tool_coverage"]["complete"])
        self.assertEqual(qualification["overall_status"], "NOT QUALIFIED")
        self.assertEqual(
            qualification["first_client_workflow"]["status"], "NOT QUALIFIED"
        )

    def test_missing_live_binary_is_not_qualified_not_pass(self) -> None:
        script = Path(__file__).with_name("live_first_client.py")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            evidence = root / "evidence"
            completed = subprocess.run(
                [
                    sys.executable,
                    str(script),
                    "--nokv-bin",
                    str(root / "missing-nokv"),
                    "--evidence-dir",
                    str(evidence),
                    "--metadata-dir",
                    str(root / "metadata"),
                    "--etcd-endpoint",
                    "http://127.0.0.1:1",
                    "--object-bucket",
                    "test",
                    "--object-endpoint",
                    "http://127.0.0.1:1",
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(completed.returncode, 3, completed.stderr)
            qualification = json.loads(
                (evidence / "qualification.json").read_text(encoding="utf-8")
            )
        self.assertEqual(qualification["overall_status"], "NOT QUALIFIED")
        self.assertNotEqual(qualification["first_client_workflow"]["status"], "PASS")


if __name__ == "__main__":
    unittest.main()
