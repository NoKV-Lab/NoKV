#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Mapping tests for opaque-cursor differential qualification."""

import unittest

import cursor_differential_qualification as qualification


class CursorDifferentialQualificationTests(unittest.TestCase):
    def test_scenario_inventory_is_frozen_and_read_scope_gap_is_visible(self) -> None:
        self.assertEqual(
            set(qualification.SCENARIOS),
            {
                "t05.list-cursor-progress-and-scope",
                "t07.get-page-progress-and-scope",
                "t08.grep-case-insensitive-pagination",
                "t09.search-scope-bound-cursor",
                "t12.find-case-insensitive-pagination",
                "c11.all-pageable-progress-scope-and-stale-rejection",
                "c13.grep-case-glob-cursor",
                "c15.find-case-insensitive-pagination",
            },
        )
        self.assertIsNotNone(
            qualification.SCENARIOS[
                "t07.get-page-progress-and-scope"
            ].not_qualified_reason
        )
        for scenario, specification in qualification.SCENARIOS.items():
            if scenario != "t07.get-page-progress-and-scope":
                self.assertTrue(specification.assertions, scenario)


if __name__ == "__main__":
    unittest.main()
