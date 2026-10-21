#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Contract tests for the native CLI contract gate's grading."""

from __future__ import annotations

import json
import types
import unittest

from cli_contract_gate import (
    REPOSITORY,
    Call,
    ContractFailure,
    Stack,
    contract_tables,
    hex_of,
    verdict,
)


def grader() -> types.SimpleNamespace:
    exits, actions = contract_tables((REPOSITORY / "docs" / "cli-contract.md").read_text())
    return types.SimpleNamespace(exits=exits, actions=actions)


def failed(exit: int, envelope: object, stdout: str = "") -> Call:
    line = envelope if isinstance(envelope, str) else json.dumps(envelope)
    return Call("probe", exit, stdout, line + "\n", 0, [])


def envelope(klass: str, next_action: str) -> dict:
    return {
        "status": "error",
        "code": "Conflict",
        "message": "path generation mismatch",
        "retryable": False,
        "details": {},
        "class": klass,
        "next_action": next_action,
    }


class ContractTablesTest(unittest.TestCase):
    def test_reads_every_class_and_action_the_contract_publishes(self) -> None:
        exits, actions = contract_tables((REPOSITORY / "docs" / "cli-contract.md").read_text())
        self.assertEqual(exits["invalid_input"], 2)
        self.assertEqual(exits["outcome_unknown"], exits["in_progress"])
        self.assertEqual(len(exits), 10)
        self.assertNotIn(0, exits.values())
        self.assertIn("query_same", actions)
        self.assertEqual(len(actions), 10)


class FailureGradingTest(unittest.TestCase):
    def test_accepts_exactly_the_published_envelope(self) -> None:
        call = failed(4, envelope("precondition_failed", "reread_then_retry"))
        graded = Stack.failure(grader(), call, "precondition_failed", "reread_then_retry")
        self.assertEqual(graded["class"], "precondition_failed")

    def test_rejects_each_contract_violation(self) -> None:
        good = envelope("precondition_failed", "reread_then_retry")
        violations = {
            "wrong exit code": failed(1, good),
            "stdout output": failed(4, good, stdout="{}"),
            "prefixed text": failed(4, "nokv: " + json.dumps(good)),
            "two lines": Call("probe", 4, "", json.dumps(good) + "\nmore\n", 0, []),
            "other class": failed(5, envelope("transient", "resubmit_same")),
            "other action": failed(4, envelope("precondition_failed", "fix_input")),
            "unpublished class": failed(4, envelope("conflict", "reread_then_retry")),
            "missing details": failed(4, {key: value for key, value in good.items() if key != "details"}),
            "success exit": failed(0, good),
        }
        for name, call in violations.items():
            with self.subTest(name), self.assertRaises(ContractFailure):
                Stack.failure(grader(), call, "precondition_failed", "reread_then_retry")


class VerdictTest(unittest.TestCase):
    def test_red_requires_every_scenario_to_fail(self) -> None:
        passed, failed_, gap = {"status": "PASS"}, {"status": "FAIL"}, {"status": "NOT QUALIFIED"}
        self.assertEqual(verdict({"a": passed, "b": passed}, "pass"), "PASS")
        self.assertEqual(verdict({"a": passed, "b": failed_}, "pass"), "FAIL")
        self.assertEqual(verdict({"a": failed_, "b": failed_}, "red"), "PASS")
        self.assertEqual(verdict({"a": failed_, "b": passed}, "red"), "FAIL")
        # A scenario whose fault never fired qualifies nothing either way.
        self.assertEqual(verdict({"a": gap}, "pass"), "FAIL")
        self.assertEqual(verdict({"a": gap}, "red"), "FAIL")
        self.assertEqual(verdict({}, "pass"), "FAIL")


class WireIdTest(unittest.TestCase):
    def test_decodes_array_and_binary_ids(self) -> None:
        self.assertEqual(hex_of([0, 1, 255]), "0001ff")
        self.assertEqual(hex_of({"binary_hex": "0001ff"}), "0001ff")


if __name__ == "__main__":
    unittest.main()
