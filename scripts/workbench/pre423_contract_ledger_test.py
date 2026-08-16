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

    def test_fuse_posix_cannot_become_product_acceptance(self) -> None:
        changed = copy.deepcopy(self.value)
        fuse = next(item for item in changed["items"] if item["id"] == "L07")
        fuse["class"] = "C"
        fuse["current_disposition"] = "restore"
        with self.assertRaisesRegex(ledger.LedgerError, "FUSE/POSIX"):
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
