#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Closed-manifest tests for Phase 1 typed qualification invocations."""

from __future__ import annotations

import json
import re
import subprocess
import unittest
from pathlib import Path

import api_absence_qualification
import api_decision_qualification
import commit_replay_qualification
import cursor_differential_qualification
import lingtai_mcp_qualification
import native_cli_workbench
import nokv_agent_qualification
import object_namespace_recovery_gate
import pre423_contract_ledger
import python_sdk_qualification
import restore_composition_gate
import snapshot_lifecycle_qualification


SCRIPT_DIR = Path(__file__).resolve().parent
REPO = SCRIPT_DIR.parents[1]
MANIFEST_PATH = SCRIPT_DIR / "qualification_invocation_manifest.json"
WORKFLOW_PATH = REPO / ".github" / "workflows" / "rust.yml"
PRODUCER_MODULES = {
    "api-absence": api_absence_qualification,
    "api-decision": api_decision_qualification,
    "commit-replay": commit_replay_qualification,
    "cursor-differential": cursor_differential_qualification,
    "nokv-agent-unit": nokv_agent_qualification,
    "lingtai-mcp": lingtai_mcp_qualification,
    "live-workbench": native_cli_workbench,
    "object-namespace-recovery": object_namespace_recovery_gate,
    "python-sdk": python_sdk_qualification,
    "restore-composition": restore_composition_gate,
    "snapshot-lifecycle": snapshot_lifecycle_qualification,
}
PRODUCER_SCENARIOS = {
    producer: getattr(module, "SCENARIOS", getattr(module, "TYPED_SCENARIOS", None))
    for producer, module in PRODUCER_MODULES.items()
}
INVOCATION_IDS = {
    "api-absence-pass",
    "api-decision-pass",
    "api-decision-nq",
    "commit-replay-pass",
    "commit-replay-nq",
    "cursor-differential-pass",
    "cursor-differential-nq",
    "nokv-agent-unit-pass",
    "nokv-agent-unit-nq",
    "lingtai-mcp-nq",
    "live-workbench-nq",
    "object-namespace-recovery-nq",
    "python-sdk-nq",
    "restore-composition-nq",
    "snapshot-lifecycle-pass",
    "snapshot-lifecycle-nq",
}
INVOCATION_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]{0,63}$")


def _load_manifest() -> dict[str, object]:
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


class QualificationInvocationManifestTests(unittest.TestCase):
    def test_manifest_is_closed_and_names_every_typed_producer(self) -> None:
        manifest = _load_manifest()
        self.assertEqual(
            set(manifest),
            {
                "schema",
                "typed_producers",
                "invocations",
                "aggregate_expectation",
            },
        )
        self.assertEqual(manifest["schema"], "nokv.pre423.qualification_invocations.v1")
        self.assertEqual(set(manifest["typed_producers"]), set(PRODUCER_MODULES))

        ledger = pre423_contract_ledger.load_ledger()
        invocations = manifest["invocations"]
        self.assertIsInstance(invocations, list)
        self.assertEqual(
            {invocation["producer"] for invocation in invocations},
            set(PRODUCER_MODULES),
        )
        invocation_ids = {invocation["id"] for invocation in invocations}
        self.assertEqual(len(invocation_ids), len(invocations))
        self.assertEqual(len(invocations), 16)
        self.assertEqual(invocation_ids, INVOCATION_IDS)
        for invocation in invocations:
            self.assertEqual(
                set(invocation),
                {
                    "id",
                    "producer",
                    "evidence_kind",
                    "script",
                    "expected_outcome",
                    "expected_exit_code",
                    "uses_rust_target",
                    "claims",
                },
            )
            self.assertRegex(invocation["id"], INVOCATION_ID_PATTERN)
            self.assertEqual(Path(invocation["id"]).name, invocation["id"])
            producer = invocation["producer"]
            contract = ledger["producer_catalog"][producer]
            self.assertEqual(invocation["script"], contract["command"]["entrypoint"])
            self.assertIn(invocation["evidence_kind"], contract["evidence_kinds"])
            self.assertEqual(
                invocation["expected_exit_code"],
                {"PASS": 0, "NQ": 3}[invocation["expected_outcome"]],
            )
            self.assertEqual(
                invocation["uses_rust_target"],
                "rust_toolchain" in contract["required_subjects"],
            )
            self.assertTrue(invocation["claims"])

    def test_pass_and_nq_scenarios_are_complete_disjoint_invocations(self) -> None:
        manifest = _load_manifest()
        invocations = manifest["invocations"]
        all_claims: list[str] = []
        pass_count = 0
        nq_count = 0
        for invocation in invocations:
            producer = invocation["producer"]
            scenarios = PRODUCER_SCENARIOS[producer]
            expected_outcome = invocation["expected_outcome"]
            for claim in invocation["claims"]:
                stable_id, gate, scenario = claim.split(":", 2)
                specification = scenarios[scenario]
                contract = getattr(specification, "contract", specification)
                self.assertEqual(stable_id, contract.stable_id)
                self.assertEqual(gate, contract.gate)
                declared_outcome = (
                    "NQ"
                    if getattr(specification, "not_qualified_reason", None) is not None
                    else expected_outcome
                )
                self.assertEqual(declared_outcome, expected_outcome)
                if expected_outcome == "PASS":
                    pass_count += 1
                else:
                    nq_count += 1
            all_claims.extend(invocation["claims"])

        self.assertEqual(len(all_claims), len(set(all_claims)))
        self.assertEqual((pass_count, nq_count), (74, 98))

        ledger = pre423_contract_ledger.load_ledger()
        typed_producers = set(manifest["typed_producers"])
        typed_eligible_scenarios = {
            (item["id"], gate, scenario)
            for item in ledger["items"]
            for gate in item["required_gates"]
            for expectation in (
                pre423_contract_ledger.resolve_gate_expectation(
                    ledger, item["id"], gate
                ),
            )
            if typed_producers.intersection(expectation["allowed_producers"])
            for scenario in expectation["scenarios"]
        }
        manifest_scenarios = {
            tuple(claim.split(":", 2))
            for invocation in invocations
            for claim in invocation["claims"]
        }
        self.assertEqual(len(typed_eligible_scenarios), 172)
        self.assertEqual(manifest_scenarios, typed_eligible_scenarios)

        outcomes_by_producer: dict[str, set[str]] = {}
        for invocation in invocations:
            outcomes_by_producer.setdefault(invocation["producer"], set()).add(
                invocation["expected_outcome"]
            )
        self.assertEqual(
            outcomes_by_producer,
            {
                "api-absence": {"PASS"},
                "api-decision": {"PASS", "NQ"},
                "commit-replay": {"PASS", "NQ"},
                "cursor-differential": {"PASS", "NQ"},
                "lingtai-mcp": {"NQ"},
                "live-workbench": {"NQ"},
                "nokv-agent-unit": {"PASS", "NQ"},
                "object-namespace-recovery": {"NQ"},
                "python-sdk": {"NQ"},
                "restore-composition": {"NQ"},
                "snapshot-lifecycle": {"PASS", "NQ"},
            },
        )

    def test_aggregate_expectation_has_no_omitted_producers_but_remains_nq(
        self,
    ) -> None:
        manifest = _load_manifest()
        expectation = manifest["aggregate_expectation"]
        self.assertEqual(
            set(expectation),
            {
                "expected_status",
                "expected_exit_code",
                "omitted_producers",
                "product_artifact_manifest",
                "receipt_counts",
            },
        )
        self.assertEqual(expectation["expected_status"], "NQ")
        self.assertEqual(expectation["expected_exit_code"], 3)
        self.assertEqual(expectation["omitted_producers"], [])
        self.assertIs(expectation["product_artifact_manifest"], True)
        expected_receipt_count = sum(
            len({":".join(claim.split(":", 2)[:2]) for claim in invocation["claims"]})
            for invocation in manifest["invocations"]
        )
        self.assertEqual(
            expectation["receipt_counts"],
            {
                "discovered": expected_receipt_count,
                "accepted": expected_receipt_count,
                "selected": expected_receipt_count,
                "superseded": 0,
                "rejected": 0,
                "invalid": 0,
            },
        )

        ledger = pre423_contract_ledger.load_ledger()
        self.assertEqual(
            set(ledger["producer_catalog"]) - set(manifest["typed_producers"]),
            set(),
        )

    def test_required_workflow_executes_and_uploads_fail_closed_qualification(
        self,
    ) -> None:
        workflow = WORKFLOW_PATH.read_text(encoding="utf-8")
        required_tokens = (
            "scripts/workbench/qualification_invocation_manifest.json",
            "scripts/workbench/qualification_invocation_check.py",
            "scripts/workbench/qualification_invocation_check_test.py",
            "scripts/workbench/source_bound_producer.py",
            "scripts/workbench/source_bound_producer_test.py",
            "scripts/workbench/typed_live_qualification.py",
            "scripts/workbench/typed_live_qualification_test.py",
            "scripts/workbench/qualification_receipt.py",
            "scripts/workbench/qualification_receipt_test.py",
            "scripts/workbench/qualification_aggregate.py",
            "scripts/workbench/qualification_aggregate_test.py",
            "scripts/workbench/api_absence_qualification.py",
            "scripts/workbench/api_absence_qualification_test.py",
            "scripts/workbench/api_decision_qualification.py",
            "scripts/workbench/api_decision_qualification_test.py",
            "scripts/workbench/commit_replay_qualification.py",
            "scripts/workbench/commit_replay_qualification_test.py",
            "scripts/workbench/cursor_differential_qualification.py",
            "scripts/workbench/cursor_differential_qualification_test.py",
            "scripts/workbench/nokv_agent_qualification.py",
            "scripts/workbench/nokv_agent_qualification_test.py",
            "scripts/workbench/lingtai_mcp_qualification.py",
            "scripts/workbench/python_sdk_qualification.py",
            "scripts/workbench/live_gap_qualification_test.py",
            "scripts/workbench/snapshot_lifecycle_qualification.py",
            "scripts/workbench/snapshot_lifecycle_qualification_test.py",
            "runner_python=$(python3 -c 'import os,sys; print(os.path.realpath(sys.executable))')",
            '[[ -x "$runner_python" && "$runner_python" = /* ]]',
            '"$runner_python" scripts/workbench/qualification_receipt.py',
            '"$runner_python" "$script"',
            "cargo build --locked -p nokv --bin nokv --target-dir target/phase1-qualification",
            '--evidence "qualification=$evidence_root/qualification.json"',
            '--evidence "mcp-transcript=$evidence_root/mcp-transcript.jsonl"',
            '--evidence "cli-transcript=$evidence_root/cli-transcript.jsonl"',
            "lingtai-kernel=git:834274df1304488d3e6b5b2cde4a3b481a81e38b",
            "python_sdk_sha256=$(git ls-files -s crates/nokv-python | sha256sum",
            "python3 scripts/workbench/qualification_aggregate.py",
            "python3 scripts/workbench/qualification_invocation_check.py",
            '[[ "$aggregate_status" -eq 3 ]]',
            "'.status == \"NQ\"'",
            "'.receipt_counts == $expected'",
            "phase1-qualification-${{ github.sha }}",
            "id: qualification_product",
            "phase1-product-${{ github.sha }}",
            "steps.qualification_product.outputs.artifact-id",
            "steps.qualification_product.outputs.artifact-digest",
            "nokv.pre423.product_artifact_manifest.v1",
            "workbench-contract:\n    runs-on: ubuntu-latest\n    timeout-minutes: 45",
            "if: ${{ always() }}",
            "if-no-files-found: warn",
            "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1",
            "snapshot-lifecycle)",
        )
        for token in required_tokens:
            with self.subTest(token=token):
                self.assertIn(token, workflow)
        self.assertIn("--product-artifact-manifest", workflow)

    def test_workflow_runner_python_matches_receipt_executable_identity(self) -> None:
        workflow = WORKFLOW_PATH.read_text(encoding="utf-8")
        assignments = [
            line.strip()
            for line in workflow.splitlines()
            if line.strip().startswith("runner_python=")
        ]
        self.assertEqual(len(assignments), 1)
        completed = subprocess.run(
            [
                "bash",
                "-c",
                "\n".join(
                    (
                        "set -euo pipefail",
                        assignments[0],
                        '[[ -x "$runner_python" && "$runner_python" = /* ]]',
                        'printf "argv0=%s\\n" "$runner_python"',
                        '"$runner_python" -c \'import os,sys; print("executable=" + os.path.realpath(sys.executable))\'',
                    )
                ),
            ],
            cwd=REPO,
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        identity = dict(line.split("=", 1) for line in completed.stdout.splitlines())
        self.assertEqual(identity["argv0"], identity["executable"])


if __name__ == "__main__":
    unittest.main()
