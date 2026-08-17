#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Mapping tests for exact commit-replay qualification."""

import unittest

import commit_replay_qualification as qualification


class CommitReplayQualificationTests(unittest.TestCase):
    def test_scenario_inventory_is_frozen_and_checkpoint_is_not_false_green(
        self,
    ) -> None:
        self.assertEqual(
            set(qualification.SCENARIOS),
            {
                "t13.commit-exact-replay",
                "t13.commit-conflict-and-head-authority",
                "c04.implicit-create-race-and-replay",
                "c16.canonical-identity-conflict-replay",
                "c21.restored-destination-owned-provenance",
                "l05.checkpoint-commit-replay",
            },
        )
        self.assertIsNotNone(
            qualification.SCENARIOS["l05.checkpoint-commit-replay"].not_qualified_reason
        )
        for scenario, specification in qualification.SCENARIOS.items():
            if scenario != "l05.checkpoint-commit-replay":
                self.assertTrue(specification.assertions, scenario)


if __name__ == "__main__":
    unittest.main()
