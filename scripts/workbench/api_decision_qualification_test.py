#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Mapping tests for checked-in API-decision qualification."""

import unittest
from pathlib import Path

import api_decision_qualification as qualification
import source_bound_producer as producer


class ApiDecisionQualificationTests(unittest.TestCase):
    def test_scenario_inventory_and_explicit_replacement_decisions(self) -> None:
        self.assertEqual(
            set(qualification.SCENARIOS),
            {
                "t17.snapshot-projection-decision",
                "c18.no-checkpoints-jsonl-decision",
                "l01.generic-profile-restoration-contract",
                "l02.workspace-client-replacement-decision",
                "l03.path-native-python-compatibility-contract",
                "l04.workbench-scoped-fsspec-contract",
                "l05.workbench-checkpoint-compatibility-contract",
                "l06.workbench-dcp-adapter-contract",
                "l08.operational-command-restoration-contract",
            },
        )
        self.assertIsNone(
            qualification.SCENARIOS[
                "l01.generic-profile-restoration-contract"
            ].not_qualified_reason
        )
        self.assertTrue(
            qualification.SCENARIOS[
                "l01.generic-profile-restoration-contract"
            ].assertions
        )
        self.assertIsNotNone(
            qualification.SCENARIOS[
                "l08.operational-command-restoration-contract"
            ].not_qualified_reason
        )
        for scenario in (
            "t17.snapshot-projection-decision",
            "c18.no-checkpoints-jsonl-decision",
            "l01.generic-profile-restoration-contract",
            "l02.workspace-client-replacement-decision",
            "l03.path-native-python-compatibility-contract",
            "l04.workbench-scoped-fsspec-contract",
            "l05.workbench-checkpoint-compatibility-contract",
            "l06.workbench-dcp-adapter-contract",
        ):
            self.assertTrue(qualification.SCENARIOS[scenario].assertions)

    def test_generic_profile_decision_binds_current_contract_and_cli_selection(
        self,
    ) -> None:
        specification = qualification.SCENARIOS[
            "l01.generic-profile-restoration-contract"
        ]
        self.assertEqual(
            [assertion.assertion_id for assertion in specification.assertions],
            [
                "l01-generic-agent-contract-is-checked-in",
                "l01-generic-agent-profile-is-explicitly-selectable",
            ],
        )

    def test_every_decision_predicate_matches_a_tracked_source(self) -> None:
        repo = Path(__file__).resolve().parents[2]
        for scenario, specification in qualification.SCENARIOS.items():
            for assertion in specification.assertions:
                with self.subTest(scenario=scenario, assertion=assertion.assertion_id):
                    result = producer.execute_source_assertion(assertion, repo=repo)
                    self.assertTrue(result.passed, result.record)


if __name__ == "__main__":
    unittest.main()
