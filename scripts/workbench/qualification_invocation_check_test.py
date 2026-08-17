#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Tests for closed Phase 1 invocation-to-aggregate verification."""

from __future__ import annotations

import copy
import json
import tempfile
import unittest
from pathlib import Path

import pre423_contract_ledger
import qualification_invocation_check as checker


SCRIPT_DIR = Path(__file__).resolve().parent
MANIFEST_PATH = SCRIPT_DIR / "qualification_invocation_manifest.json"


def _status_worst(statuses: list[str]) -> str:
    priority = {"PASS": 0, "NQ": 1, "FAIL": 2}
    return max(statuses, key=priority.__getitem__)


def _valid_report() -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
    ledger = pre423_contract_ledger.load_ledger()
    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    expected = checker.derive_expected_scenarios(manifest=manifest, ledger=ledger)
    items = []
    for item in ledger["items"]:
        gates = []
        for gate_name in item["required_gates"]:
            expectation = pre423_contract_ledger.resolve_gate_expectation(
                ledger, item["id"], gate_name
            )
            scenarios = []
            for scenario_name in expectation["scenarios"]:
                key = (item["id"], gate_name, scenario_name)
                specification = expected.get(key)
                if specification is None:
                    status = "NQ"
                    selected_receipts = []
                else:
                    status = specification.outcome
                    selected_receipts = [
                        {
                            "producer": specification.producer,
                            "job": "workbench-contract",
                            "attempt": 1,
                            "operation_id": f"operation-{scenario_name}",
                            "outcome": specification.outcome,
                            "receipt": f"receipts/{scenario_name}.json",
                        }
                    ]
                scenarios.append(
                    {
                        "scenario": scenario_name,
                        "status": status,
                        "selected_receipts": selected_receipts,
                    }
                )
            gates.append(
                {
                    "gate": gate_name,
                    "status": _status_worst(
                        [scenario["status"] for scenario in scenarios]
                    ),
                    "allowed_evidence_kinds": expectation["allowed_evidence_kinds"],
                    "allowed_producers": expectation["allowed_producers"],
                    "scenarios": scenarios,
                }
            )
        items.append(
            {
                "stable_id": item["id"],
                "class": item["class"],
                "disposition": item["current_disposition"],
                "status": _status_worst([gate["status"] for gate in gates]),
                "gates": gates,
            }
        )
    report = {
        "schema": "nokv.pre423.qualification_aggregate.v1",
        "status": manifest["aggregate_expectation"]["expected_status"],
        "source_sha": "1" * 40,
        "workflow_run_id": "run-1",
        "product_artifact_manifest": {
            "provider": "github-actions",
            "workflow_run_id": "run-1",
            "workflow_attempt": 1,
            "head_sha": "1" * 40,
            "manifest_sha256": "2" * 64,
            "artifact_mapping_count": 5,
        },
        "receipt_counts": manifest["aggregate_expectation"]["receipt_counts"],
        "item_status_counts": {
            status: sum(item["status"] == status for item in items)
            for status in ("PASS", "NQ", "FAIL")
        },
        "items": items,
        "rejected_receipts": [],
        "invalid_receipts": [],
        "receipt_conflicts": [],
        "latest_attempts": [{"workflow_run_id": "run-1", "attempt": 1}],
        "rejected_latest_bundles": [],
    }
    return manifest, ledger, report


def _scenario_entry(
    report: dict[str, object], key: tuple[str, str, str]
) -> dict[str, object]:
    stable_id, gate_name, scenario_name = key
    item = next(item for item in report["items"] if item["stable_id"] == stable_id)
    gate = next(gate for gate in item["gates"] if gate["gate"] == gate_name)
    return next(
        scenario
        for scenario in gate["scenarios"]
        if scenario["scenario"] == scenario_name
    )


def _recompute_status_summaries(report: dict[str, object]) -> None:
    for item in report["items"]:
        for gate in item["gates"]:
            gate["status"] = _status_worst(
                [scenario["status"] for scenario in gate["scenarios"]]
            )
        item["status"] = _status_worst([gate["status"] for gate in item["gates"]])
    report["item_status_counts"] = {
        status: sum(item["status"] == status for item in report["items"])
        for status in ("PASS", "NQ", "FAIL")
    }
    report["status"] = _status_worst([item["status"] for item in report["items"]])


class QualificationInvocationCheckTests(unittest.TestCase):
    def test_valid_report_exactly_covers_all_typed_manifest_scenarios(self) -> None:
        manifest, ledger, report = _valid_report()

        summary = checker.validate_aggregate_report(
            manifest=manifest, ledger=ledger, report=report
        )

        self.assertEqual(summary.typed_scenarios, 172)
        self.assertEqual(summary.pass_scenarios, 74)
        self.assertEqual(summary.nq_scenarios, 98)

    def test_deleted_pass_receipt_and_duplicate_keeps_count_but_fails(self) -> None:
        manifest, ledger, report = _valid_report()
        expected = checker.derive_expected_scenarios(manifest=manifest, ledger=ledger)
        pass_keys = [
            key
            for key, specification in expected.items()
            if specification.outcome == "PASS"
        ]
        missing = _scenario_entry(report, pass_keys[0])
        duplicated = _scenario_entry(report, pass_keys[1])
        missing["selected_receipts"] = []
        duplicated["selected_receipts"].append(
            copy.deepcopy(duplicated["selected_receipts"][0])
        )
        self.assertEqual(report["receipt_counts"]["discovered"], 137)

        with self.assertRaisesRegex(checker.InvocationCheckError, "selected receipt"):
            checker.validate_aggregate_report(
                manifest=manifest, ledger=ledger, report=report
            )

    def test_manifest_cannot_omit_any_typed_scenario(self) -> None:
        manifest, ledger, _ = _valid_report()
        manifest["invocations"][0]["claims"].pop()

        with self.assertRaisesRegex(
            checker.InvocationCheckError, "do not exactly cover"
        ):
            checker.derive_expected_scenarios(manifest=manifest, ledger=ledger)

    def test_item_status_counts_must_match_recomputed_items(self) -> None:
        manifest, ledger, report = _valid_report()
        report["item_status_counts"]["PASS"] += 1
        report["item_status_counts"]["NQ"] -= 1

        with self.assertRaisesRegex(checker.InvocationCheckError, "item_status_counts"):
            checker.validate_aggregate_report(
                manifest=manifest, ledger=ledger, report=report
            )

    def test_product_artifact_binding_must_cover_every_live_product_producer(
        self,
    ) -> None:
        manifest, ledger, report = _valid_report()
        report["product_artifact_manifest"]["artifact_mapping_count"] = 4

        with self.assertRaisesRegex(
            checker.InvocationCheckError, "product artifact binding"
        ):
            checker.validate_aggregate_report(
                manifest=manifest, ledger=ledger, report=report
            )

    def test_overall_status_must_match_worst_item(self) -> None:
        manifest, ledger, report = _valid_report()
        report["status"] = "PASS"

        with self.assertRaisesRegex(checker.InvocationCheckError, "overall status"):
            checker.validate_aggregate_report(
                manifest=manifest, ledger=ledger, report=report
            )

    def test_cli_is_the_production_checker_entrypoint(self) -> None:
        manifest, _, report = _valid_report()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest_path = root / "manifest.json"
            report_path = root / "aggregate.json"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            report_path.write_text(json.dumps(report), encoding="utf-8")

            exit_code = checker.main(
                [
                    "--manifest",
                    str(manifest_path),
                    "--ledger",
                    str(pre423_contract_ledger.LEDGER_PATH),
                    "--report",
                    str(report_path),
                ]
            )

        self.assertEqual(exit_code, 0)


if __name__ == "__main__":
    unittest.main()
