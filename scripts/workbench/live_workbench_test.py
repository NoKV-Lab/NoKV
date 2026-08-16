#!/usr/bin/env python3
"""Unit tests for the generic live Workbench evidence harness."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import live_workbench as harness


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


class LiveWorkbenchTest(unittest.TestCase):
    def test_default_workflow_evidence_is_runtime_neutral(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            current = config(Path(directory) / "evidence")
            encoded = harness.canonical_json(
                {
                    "agent": current.agent,
                    "node": current.node,
                    "object_root": current.object_root,
                    "plan": harness.plan(current, harness.tool_plan(current)),
                    "qualification": harness.qualification(
                        "NOT QUALIFIED", "test", "NOT QUALIFIED"
                    ),
                }
            ).lower()

        self.assertNotIn("lingtai", encoded)

    def test_plan_covers_exact_eighteen_tools_in_dependency_order(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            steps = harness.tool_plan(config(Path(directory) / "evidence"))
        self.assertEqual(harness.planned_tool_coverage(steps), harness.WORKBENCH_TOOLS)
        first_occurrences = list(dict.fromkeys(step.name for step in steps))
        self.assertEqual(first_occurrences, REQUIRED_FIRST_OCCURRENCE_ORDER)
        self.assertEqual(len(first_occurrences), 18)

    def test_phase_one_plan_restores_old_contracts_without_explicit_create(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            current = config(Path(directory) / "evidence")
            steps = harness.tool_plan(current)
            plan = harness.plan(current, steps)
        by_label = {step.label: step for step in steps}
        phase_one_ids = harness.phase_one_workbench_ids(current)
        self.assertEqual(len(set(phase_one_ids.values())), 3)
        self.assertTrue(all(
            harness.WORKBENCH_ID.fullmatch(workbench_id)
            for workbench_id in phase_one_ids.values()
        ))
        created_ids = {
            step.arguments["id"]
            for step in steps
            if step.name == "workbench_create"
        }
        self.assertTrue(set(phase_one_ids.values()).isdisjoint(created_ids))
        self.assertEqual(by_label["implicit-put"].arguments["id"], phase_one_ids["put"])
        self.assertEqual(by_label["implicit-put-replay"].error_code, "AlreadyExists")
        self.assertEqual(
            by_label["implicit-append"].arguments["id"], phase_one_ids["append"]
        )
        self.assertEqual(
            by_label["implicit-commit"].arguments["id"], phase_one_ids["commit"]
        )
        self.assertEqual(
            by_label["find"].arguments["manifest_pattern"], "PtYcHoGrApHy"
        )

        for omitted, explicit_empty in harness.OPTIONAL_SCOPE_RESULT_PAIRS:
            omitted_arguments = by_label[omitted].arguments
            empty_arguments = dict(by_label[explicit_empty].arguments)
            self.assertEqual(empty_arguments.pop("path"), "")
            self.assertEqual(empty_arguments, omitted_arguments)

        self.assertEqual(by_label["grep-phase1-page-1"].arguments["limit"], 1)
        self.assertEqual(
            plan["dynamic_tool_steps"],
            [{"label": "grep-phase1-page-2", "cursor_from": "grep-phase1-page-1.next_cursor"}],
        )

    def test_phase_one_fixture_ids_cannot_alias_configured_workbenches(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            current = config(Path(directory) / "evidence")
        fixture = harness.phase_one_workbench_ids(current)["put"]
        collided = harness.dataclasses.replace(current, workbench=fixture)
        with self.assertRaisesRegex(harness.NotQualified, "Phase 1 fixture"):
            harness.validate(collided, live=False)

    def test_grep_continuation_copies_the_exact_scope_and_opaque_cursor(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            current = config(Path(directory) / "evidence")
            first = next(
                step for step in harness.tool_plan(current)
                if step.label == "grep-phase1-page-1"
            )
        second = harness.grep_continuation_step(
            first, {"truncated": True, "next_cursor": "opaque-cursor"}
        )
        expected = dict(first.arguments)
        expected["cursor"] = "opaque-cursor"
        self.assertEqual(second.label, "grep-phase1-page-2")
        self.assertEqual(second.name, first.name)
        self.assertEqual(second.arguments, expected)
        with self.assertRaisesRegex(harness.WorkflowFailure, "continuation cursor"):
            harness.grep_continuation_step(
                first, {"truncated": False, "next_cursor": None}
            )

    def test_workflow_pass_keeps_one_day_reap_gate_not_qualified(self) -> None:
        record = harness.qualification(
            "NOT QUALIFIED", "bounded live workflow passed", "PASS", "ab" * 32
        )
        self.assertEqual(record["workbench_workflow"]["status"], "PASS")
        self.assertEqual(record["acceptance_gates"]["0"]["status"], "NOT QUALIFIED")
        self.assertIn("one-day snapshot lease", record["acceptance_gates"]["0"]["reason"])

    def test_phase_one_assertions_emit_reviewable_ledger_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            current = config(Path(directory) / "evidence")
        results = self.phase_one_results(current)
        evidence = harness.assert_phase_one_results(results, current)
        self.assertEqual(evidence["schema"], harness.SCHEMA)
        self.assertEqual(set(evidence["checks"]), {"C04", "C05", "C15", "T08"})
        self.assertTrue(all(
            check["status"] == "PASS" for check in evidence["checks"].values()
        ))
        self.assertNotIn(
            results["grep-phase1-page-1"]["next_cursor"],
            harness.canonical_json(evidence),
        )

    def test_phase_one_assertions_reject_overlapping_grep_pages(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            current = config(Path(directory) / "evidence")
        results = self.phase_one_results(current)
        results["grep-phase1-page-2"]["matches"] = list(
            results["grep-phase1-page-1"]["matches"]
        )
        with self.assertRaisesRegex(harness.WorkflowFailure, "grep continuation"):
            harness.assert_phase_one_results(results, current)

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
        script = Path(__file__).with_name("live_workbench.py")
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
            qualification["workbench_workflow"]["status"], "NOT QUALIFIED"
        )

    def test_missing_live_binary_is_not_qualified_not_pass(self) -> None:
        script = Path(__file__).with_name("live_workbench.py")
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
        self.assertNotEqual(qualification["workbench_workflow"]["status"], "PASS")

    @staticmethod
    def phase_one_results(current: harness.Config) -> dict[str, dict[str, object]]:
        phase_one_ids = harness.phase_one_workbench_ids(current)
        results: dict[str, dict[str, object]] = {}
        for omitted, explicit_empty in harness.OPTIONAL_SCOPE_RESULT_PAIRS:
            result = {"status": "success", "path": f"scope/{omitted}"}
            results[omitted] = result
            results[explicit_empty] = dict(result)
        results.update(
            {
                "implicit-put": {
                    "status": "success", "workbench_id": phase_one_ids["put"],
                    "generation": 1, "replace": False,
                },
                "implicit-put-replay": {"code": "AlreadyExists"},
                "implicit-put-second": {
                    "status": "success", "workbench_id": phase_one_ids["put"],
                    "generation": 1, "replace": False,
                },
                "implicit-append": {
                    "status": "success", "workbench_id": phase_one_ids["append"],
                    "generation": 1, "created": True,
                },
                "implicit-commit": {
                    "status": "success", "workbench_id": phase_one_ids["commit"],
                    "commit_identity": "ab" * 32, "idempotent_replay": False,
                },
                "implicit-commit-replay": {
                    "status": "success", "workbench_id": phase_one_ids["commit"],
                    "commit_identity": "ab" * 32, "idempotent_replay": True,
                },
                "find": {
                    "status": "success",
                    "matches": [{
                        "workbench_id": current.workbench, "committed": True,
                        "commit_identity_verified": True,
                    }],
                },
                "grep-phase1-page-1": {
                    "status": "success", "truncated": True,
                    "next_cursor": "opaque-secret-cursor",
                    "matches": [{
                        "path": (
                            f"{current.workbench_root}/{phase_one_ids['put']}"
                            "/outputs/a.txt"
                        )
                    }],
                },
                "grep-phase1-page-2": {
                    "status": "success", "truncated": False, "next_cursor": None,
                    "matches": [{
                        "path": (
                            f"{current.workbench_root}/{phase_one_ids['put']}"
                            "/outputs/b.txt"
                        )
                    }],
                },
            }
        )
        return results


if __name__ == "__main__":
    unittest.main()
