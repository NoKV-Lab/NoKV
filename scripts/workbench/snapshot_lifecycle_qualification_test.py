#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Mapping tests for exact snapshot-lifecycle qualification."""

from __future__ import annotations

import unittest
from unittest.mock import patch

import pre423_contract_ledger
import snapshot_lifecycle_qualification as qualification


class SnapshotLifecycleQualificationTests(unittest.TestCase):
    def test_scenarios_exactly_cover_the_ledger_profile(self) -> None:
        ledger = pre423_contract_ledger.load_ledger()
        expected = {
            scenario
            for item in ledger["items"]
            for gate in item["required_gates"]
            for expectation in (
                pre423_contract_ledger.resolve_gate_expectation(
                    ledger, item["id"], gate
                ),
            )
            if "snapshot-lifecycle" in expectation["allowed_producers"]
            for scenario in expectation["scenarios"]
        }
        self.assertEqual(set(qualification.SCENARIOS), expected)

    def test_checkpoint_composition_remains_explicitly_not_qualified(self) -> None:
        gaps = {
            "c17.snapshot-commit-alias-ttl-metadata-warnings",
            "c19.frozen-renew-retire-reap-expire-foreign",
            "l05.checkpoint-snapshot-lifecycle",
        }
        for scenario, specification in qualification.SCENARIOS.items():
            if scenario in gaps:
                self.assertIsNotNone(specification.not_qualified_reason)
            else:
                self.assertTrue(specification.assertions, scenario)

    def test_assertions_are_exact_single_targets(self) -> None:
        assertions = {
            assertion
            for specification in qualification.SCENARIOS.values()
            for assertion in specification.assertions
        }
        self.assertEqual(
            {assertion.package for assertion in assertions},
            {"nokv-client", "nokv-meta", "nokv-server"},
        )
        self.assertTrue(
            all(assertion.target_args == ("--lib",) for assertion in assertions)
        )

    def test_entrypoint_binds_the_catalogued_qualification_role(self) -> None:
        with patch.object(qualification, "rust_main", return_value=3) as runner:
            self.assertEqual(qualification.main(), 3)
        self.assertEqual(
            runner.call_args.kwargs["evidence_roles"],
            ("producer-result", "qualification"),
        )


if __name__ == "__main__":
    unittest.main()
