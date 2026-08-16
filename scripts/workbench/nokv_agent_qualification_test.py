#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Mapping tests for the exact nokv-agent qualification producer."""

import unittest

import nokv_agent_qualification as qualification


class NokvAgentQualificationTests(unittest.TestCase):
    def test_scenario_inventory_is_frozen(self) -> None:
        self.assertEqual(len(qualification.SCENARIOS), 46)
        self.assertEqual(
            set(qualification.SCENARIOS),
            {
                "t01.create-schema",
                "t01.create-result",
                "t02.put-schema",
                "t02.put-create-replace-contract",
                "t02.put-path-native-output",
                "t03.append-schema",
                "t03.append-result-contract",
                "t03.append-delta-generation-digest-output",
                "t04.edit-schema",
                "t04.edit-noop-and-conflict-contract",
                "t05.list-root-equivalence-contract",
                "t06.stat-metadata-only-contract",
                "t07.get-range-format-conditional-contract",
                "t07.get-text-base64-page-output",
                "t08.grep-literal-or-glob-contract",
                "t09.search-query-controls-contract",
                "t10.aggregate-query-controls-contract",
                "t10.aggregate-output",
                "t11.describe-query-capabilities-contract",
                "t11.describe-output",
                "t12.find-commit-manifest-contract",
                "t12.find-optional-manifest-output",
                "t13.commit-provenance-contract",
                "t13.commit-path-native-output",
                "t17.snapshot-projection-output",
                "t18.restore-destination-owned-output",
                "c01.exact-18-tool-schema",
                "c02.closed-input-schema",
                "c02.typed-validation-error-contract",
                "c02.typed-validation-error-output",
                "c03.id-section-and-path-normalization-contract",
                "c04.implicit-workbench-create-contract",
                "c05.empty-path-equivalence-contract",
                "c07.exclusive-payload-schema",
                "c07.payload-decode-content-type-size-contract",
                "c08.create-replace-precondition-contract",
                "c09.append-result-contract",
                "c10.edit-match-noop-conflict-contract",
                "c12.stat-read-range-conditional-contract",
                "c13.grep-literal-or-bounds-contract",
                "c14.query-scope-predicate-control-contract",
                "c14.query-defined-empty-output",
                "c15.find-filter-projection-contract",
                "c17.snapshot-validation-contract",
                "c18.snapshot-lifecycle-output",
                "l01.generic-seven-tool-profile-schema",
            },
        )

    def test_every_qualified_scenario_uses_exact_tests(self) -> None:
        for scenario, specification in qualification.SCENARIOS.items():
            with self.subTest(scenario=scenario):
                if specification.not_qualified_reason is not None:
                    self.assertIn(
                        scenario,
                        {
                            "c04.implicit-workbench-create-contract",
                            "c07.payload-decode-content-type-size-contract",
                            "c13.grep-literal-or-bounds-contract",
                        },
                    )
                    continue
                self.assertTrue(specification.assertions)
                for assertion in specification.assertions:
                    self.assertTrue(assertion.test_name)
                    self.assertIn(
                        "--lib", assertion.target_args
                    ) if assertion.target_args == ("--lib",) else self.assertEqual(
                        assertion.target_args[:1], ("--test",)
                    )

    def test_generic_profile_schema_uses_the_exact_seven_tool_oracle(self) -> None:
        specification = qualification.SCENARIOS["l01.generic-seven-tool-profile-schema"]
        self.assertIsNone(specification.not_qualified_reason)
        self.assertEqual(
            [
                (assertion.package, assertion.target_args, assertion.test_name)
                for assertion in specification.assertions
            ],
            [
                (
                    "nokv-agent",
                    ("--test", "generic_profile"),
                    "generic_agent_profile_preserves_the_exact_seven_tool_contract",
                )
            ],
        )


if __name__ == "__main__":
    unittest.main()
