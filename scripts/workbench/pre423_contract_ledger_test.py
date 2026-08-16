#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Tests for the pre-#423 Workbench contract ledger gate."""

from __future__ import annotations

import copy
import subprocess
import sys
import unittest
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

import pre423_contract_ledger as ledger  # noqa: E402


class Pre423ContractLedgerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.value = ledger.load_ledger()

    def test_checked_in_ledger_has_the_canonical_counts(self) -> None:
        summary = ledger.validate_ledger(self.value)
        self.assertEqual(summary.items, 47)
        self.assertEqual(summary.core, 39)
        self.assertEqual(summary.legacy, 8)

    def test_duplicate_stable_id_is_rejected(self) -> None:
        changed = copy.deepcopy(self.value)
        changed["items"][1]["id"] = changed["items"][0]["id"]
        with self.assertRaisesRegex(ledger.LedgerError, "duplicate ids"):
            ledger.validate_ledger(changed)

    def test_missing_item_is_rejected_before_counts_can_drift(self) -> None:
        changed = copy.deepcopy(self.value)
        changed["items"].pop()
        with self.assertRaisesRegex(ledger.LedgerError, "exactly 47"):
            ledger.validate_ledger(changed)

    def test_invalid_class_and_disposition_are_rejected(self) -> None:
        invalid_class = copy.deepcopy(self.value)
        invalid_class["items"][0]["class"] = "E"
        with self.assertRaisesRegex(ledger.LedgerError, "class must be"):
            ledger.validate_ledger(invalid_class)

        invalid_disposition = copy.deepcopy(self.value)
        invalid_disposition["items"][0]["current_disposition"] = "ignore"
        with self.assertRaisesRegex(ledger.LedgerError, "current_disposition"):
            ledger.validate_ledger(invalid_disposition)

    def test_class_a_or_b_cannot_be_silently_retired(self) -> None:
        for item_class, disposition in (
            ("A", "retire"),
            ("B", "do-not-restore"),
        ):
            with self.subTest(item_class=item_class, disposition=disposition):
                changed = copy.deepcopy(self.value)
                changed["items"][0]["class"] = item_class
                changed["items"][0]["current_disposition"] = disposition
                with self.assertRaisesRegex(ledger.LedgerError, "cannot be silently"):
                    ledger.validate_ledger(changed)

    def test_core_and_legacy_scope_cannot_be_relabelled(self) -> None:
        changed = copy.deepcopy(self.value)
        changed["items"][0]["scope"] = "legacy"
        with self.assertRaisesRegex(ledger.LedgerError, "must be core"):
            ledger.validate_ledger(changed)

    def test_evidence_and_gate_references_are_required(self) -> None:
        missing_evidence = copy.deepcopy(self.value)
        missing_evidence["items"][0]["source_evidence"] = []
        with self.assertRaisesRegex(ledger.LedgerError, "source_evidence"):
            ledger.validate_ledger(missing_evidence)

        unknown_gate = copy.deepcopy(self.value)
        unknown_gate["items"][0]["required_gates"] = ["missing-gate"]
        with self.assertRaisesRegex(ledger.LedgerError, "unknown gates"):
            ledger.validate_ledger(unknown_gate)

    def test_every_required_gate_has_an_item_specific_expectation(self) -> None:
        ledger.validate_ledger(self.value)
        self.assertEqual(
            sum(len(item["required_gates"]) for item in self.value["items"]),
            137,
        )
        self.assertEqual(
            sum(len(item["gate_expectations"]) for item in self.value["items"]),
            137,
        )
        for item in self.value["items"]:
            with self.subTest(item_id=item["id"]):
                self.assertEqual(
                    set(item["required_gates"]), set(item["gate_expectations"])
                )
                for gate in item["required_gates"]:
                    expectation = ledger.resolve_gate_expectation(
                        self.value, item["id"], gate
                    )
                    self.assertTrue(expectation["scenarios"])
                    self.assertTrue(expectation["allowed_evidence_kinds"])
                    self.assertTrue(expectation["allowed_producers"])

    def test_missing_or_extra_gate_expectation_is_rejected(self) -> None:
        missing = copy.deepcopy(self.value)
        del missing["items"][0]["gate_expectations"]["schema-surface"]
        with self.assertRaisesRegex(ledger.LedgerError, "must exactly match"):
            ledger.validate_ledger(missing)

        extra = copy.deepcopy(self.value)
        extra["items"][0]["gate_expectations"]["not-required"] = {
            "profile": "schema-surface",
            "scenarios": ["t01.not-required"],
        }
        with self.assertRaisesRegex(ledger.LedgerError, "must exactly match"):
            ledger.validate_ledger(extra)

    def test_expectation_profile_must_bind_known_kinds_and_producers(self) -> None:
        unknown_kind = copy.deepcopy(self.value)
        unknown_kind["expectation_profiles"]["schema-surface"][
            "allowed_evidence_kinds"
        ] = ["claimed-live"]
        with self.assertRaisesRegex(ledger.LedgerError, "evidence kinds"):
            ledger.validate_ledger(unknown_kind)

        unknown_producer = copy.deepcopy(self.value)
        unknown_producer["expectation_profiles"]["schema-surface"][
            "allowed_producers"
        ] = ["unknown-producer"]
        with self.assertRaisesRegex(ledger.LedgerError, "unknown producers"):
            ledger.validate_ledger(unknown_producer)

    def test_scenarios_are_nonempty_and_unique_within_an_item(self) -> None:
        changed = copy.deepcopy(self.value)
        changed["items"][0]["gate_expectations"]["schema-surface"]["scenarios"] = [
            "t01.same",
            "t01.same",
        ]
        with self.assertRaisesRegex(ledger.LedgerError, "must not contain duplicates"):
            ledger.validate_ledger(changed)

    def test_scenario_inventory_cannot_be_silently_reduced(self) -> None:
        changed = copy.deepcopy(self.value)
        provider = changed["items"][17]["gate_expectations"]["provider-recovery"]
        provider["scenarios"] = provider["scenarios"][:1]
        with self.assertRaisesRegex(ledger.LedgerError, "exactly 172 scenarios"):
            ledger.validate_ledger(changed)

    def test_gate_policy_cannot_be_swapped_while_preserving_counts(self) -> None:
        changed = copy.deepcopy(self.value)
        item = changed["items"][0]
        item["required_gates"][-1] = "api-decision"
        item["gate_expectations"]["api-decision"] = {
            "profile": "api-decision",
            "scenarios": item["gate_expectations"].pop("native-workbench-e2e")[
                "scenarios"
            ],
        }
        with self.assertRaisesRegex(ledger.LedgerError, "qualification policy digest"):
            ledger.validate_ledger(changed)

    def test_lingtai_profile_cannot_be_broadened_to_static_evidence(self) -> None:
        changed = copy.deepcopy(self.value)
        profile = changed["expectation_profiles"]["lingtai-mcp-e2e"]
        profile["allowed_evidence_kinds"].append("static")
        profile["allowed_producers"].append("api-decision")
        with self.assertRaisesRegex(ledger.LedgerError, "qualification policy digest"):
            ledger.validate_ledger(changed)

    def test_live_producer_binds_binary_argument_and_dependency_policy(self) -> None:
        for producer_id, producer in self.value["producer_catalog"].items():
            with self.subTest(producer=producer_id):
                command = producer["command"]
                if "live" in producer["evidence_kinds"]:
                    self.assertTrue(command["binary_argument"].startswith("--"))
                    self.assertTrue(producer["required_dependencies"])
                else:
                    self.assertIsNone(command["binary_argument"])
                    self.assertEqual(producer["required_dependencies"], {})

    def test_live_dependency_policy_cannot_become_arbitrary_self_report(self) -> None:
        changed = copy.deepcopy(self.value)
        changed["producer_catalog"]["live-workbench"]["required_dependencies"] = {
            "fake-provider": ["free-form"]
        }
        with self.assertRaisesRegex(ledger.LedgerError, "dependency identity kinds"):
            ledger.validate_ledger(changed)

    def test_rust_test_producers_require_the_runner_bound_toolchain(self) -> None:
        for producer_id in (
            "commit-replay",
            "cursor-differential",
            "nokv-agent-unit",
            "snapshot-lifecycle",
        ):
            with self.subTest(producer=producer_id):
                self.assertEqual(
                    self.value["producer_catalog"][producer_id]["required_subjects"],
                    ["rust_toolchain"],
                )

        changed = copy.deepcopy(self.value)
        changed["producer_catalog"]["commit-replay"]["required_subjects"] = []
        with self.assertRaisesRegex(ledger.LedgerError, "qualification policy digest"):
            ledger.validate_ledger(changed)

    def test_live_binary_argument_cannot_be_removed(self) -> None:
        changed = copy.deepcopy(self.value)
        changed["producer_catalog"]["live-workbench"]["command"]["binary_argument"] = (
            None
        )
        with self.assertRaisesRegex(ledger.LedgerError, "binary_argument"):
            ledger.validate_ledger(changed)

    def test_pre_revision_citation_cannot_exceed_recorded_file_length(self) -> None:
        changed = copy.deepcopy(self.value)
        item = next(item for item in changed["items"] if item["id"] == "L04")
        item["source_evidence"][0] = item["source_evidence"][0].replace(
            "62-336", "62-430"
        )
        with self.assertRaisesRegex(ledger.LedgerError, "exceeds pre-revision"):
            ledger.validate_ledger(changed)

    def test_fuse_posix_cannot_become_product_acceptance(self) -> None:
        changed = copy.deepcopy(self.value)
        fuse = next(item for item in changed["items"] if item["id"] == "L07")
        fuse["class"] = "C"
        fuse["current_disposition"] = "restore"
        with self.assertRaisesRegex(ledger.LedgerError, "FUSE/POSIX"):
            ledger.validate_ledger(changed)

    def test_generic_agent_profile_cannot_be_replaced_by_absence(self) -> None:
        changed = copy.deepcopy(self.value)
        item = next(item for item in changed["items"] if item["id"] == "L01")
        item["current_disposition"] = "replace"
        item["required_gates"][-1] = "api-absence"
        item["gate_expectations"]["api-absence"] = {
            "profile": "api-absence",
            "scenarios": ["l01.generic-profile-api-absence"],
        }
        del item["gate_expectations"]["native-workbench-e2e"]
        with self.assertRaisesRegex(ledger.LedgerError, "generic Agent MCP profile"):
            ledger.validate_ledger(changed)

    def test_operational_recovery_oracle_cannot_be_collapsed_to_a_decision(
        self,
    ) -> None:
        changed = copy.deepcopy(self.value)
        item = next(item for item in changed["items"] if item["id"] == "L08")
        item["gate_expectations"]["provider-recovery"]["scenarios"] = [
            "l08.retained-backup-restore-fsck-gc-provider-operations",
            *(f"l08.decision-only-{index}" for index in range(1, 9)),
        ]
        with self.assertRaisesRegex(ledger.LedgerError, "operational recovery"):
            ledger.validate_ledger(changed)

    def test_non_posix_python_compatibility_cannot_be_retired(self) -> None:
        for item_id in ("L03", "L04", "L05", "L06"):
            with self.subTest(item_id=item_id):
                changed = copy.deepcopy(self.value)
                item = next(
                    entry for entry in changed["items"] if entry["id"] == item_id
                )
                item["current_disposition"] = "retire"
                with self.assertRaisesRegex(
                    ledger.LedgerError, "must be replaced, not retired"
                ):
                    ledger.validate_ledger(changed)

    def test_python_compatibility_live_oracle_cannot_be_reduced(self) -> None:
        changed = copy.deepcopy(self.value)
        item = next(entry for entry in changed["items"] if entry["id"] == "L05")
        item["gate_expectations"]["python-sdk-e2e"]["scenarios"].pop()
        with self.assertRaisesRegex(ledger.LedgerError, "exactly 172 scenarios"):
            ledger.validate_ledger(changed)

    def test_create_and_path_contracts_require_all_five_sections(self) -> None:
        for item_id in ("T01", "C03"):
            with self.subTest(item_id=item_id):
                changed = copy.deepcopy(self.value)
                item = next(item for item in changed["items"] if item["id"] == item_id)
                item["requirement"] = item["requirement"].replace(
                    "five virtual sections", "three virtual sections"
                )
                with self.assertRaisesRegex(
                    ledger.LedgerError, "canonical five Workbench sections"
                ):
                    ledger.validate_ledger(changed)

    def test_cli_validates_the_checked_in_ledger(self) -> None:
        completed = subprocess.run(
            [sys.executable, str(SCRIPT_DIR / "pre423_contract_ledger.py")],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("items=47 core=39 legacy=8", completed.stdout)


if __name__ == "__main__":
    unittest.main()
