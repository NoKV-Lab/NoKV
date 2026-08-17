#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Policy tests for deliberately unqualified live producer entrypoints."""

from __future__ import annotations

import unittest

import lingtai_mcp_qualification
import pre423_contract_ledger
import python_sdk_qualification


PRODUCERS = {
    "lingtai-mcp": lingtai_mcp_qualification,
    "python-sdk": python_sdk_qualification,
}


class LiveGapQualificationTests(unittest.TestCase):
    def test_gap_entrypoints_cover_every_owned_ledger_scenario(self) -> None:
        ledger = pre423_contract_ledger.load_ledger()
        for producer, module in PRODUCERS.items():
            expected = {
                scenario
                for item in ledger["items"]
                for gate in item["required_gates"]
                for expectation in (
                    pre423_contract_ledger.resolve_gate_expectation(
                        ledger, item["id"], gate
                    ),
                )
                if producer in expectation["allowed_producers"]
                for scenario in expectation["scenarios"]
            }
            with self.subTest(producer=producer):
                self.assertEqual(set(module.SCENARIOS), expected)
                self.assertTrue(module.REASON)

    def test_gap_reasons_cannot_claim_live_success(self) -> None:
        for producer, module in PRODUCERS.items():
            with self.subTest(producer=producer):
                reason = module.REASON.lower()
                self.assertIn("no ", reason)
                self.assertNotIn(" passed", reason)


if __name__ == "__main__":
    unittest.main()
