#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Deterministic unit tests for the live checkpoint/restore harness."""

from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import sys
import unittest
from pathlib import Path


sys.dont_write_bytecode = True
MODULE_PATH = Path(__file__).with_name("checkpoint_restore_live_e2e.py")


def load_module():
    spec = importlib.util.spec_from_file_location(
        "checkpoint_restore_live_e2e", MODULE_PATH
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class CheckpointRestoreLiveE2ETest(unittest.TestCase):
    def setUp(self):
        self.module = load_module()

    def test_profiles_keep_quick_and_full_workloads_explicit(self):
        quick = self.module.workload_profile("quick")
        full = self.module.workload_profile("full")

        self.assertEqual(quick.page_limit, 7)
        self.assertEqual(full.page_limit, 7)
        self.assertLess(quick.renew_rounds, full.renew_rounds)
        self.assertEqual(full.renew_rounds, 100)
        self.assertLess(quick.reaper_rounds, full.reaper_rounds)
        self.assertEqual(full.reaper_rounds, 200)
        self.assertEqual(full.history_entries, 101)

    def test_snapshot_cli_id_parser_is_stable_across_extra_fields(self):
        self.assertEqual(
            self.module.parse_snapshot_id(
                "snapshot /agents/a/wb id=184 version=99 lease_expires_at=123\n"
            ),
            184,
        )
        with self.assertRaisesRegex(self.module.AcceptanceError, "lacks id"):
            self.module.parse_snapshot_id("snapshot 184 version=99")
        self.assertEqual(
            self.module.parse_snapshot_expiry(
                "snapshot /agents/a/wb id=184 version=99 "
                "lease_expires_unix_ms=123\n"
            ),
            123,
        )
        with self.assertRaisesRegex(self.module.AcceptanceError, "lacks lease"):
            self.module.parse_snapshot_expiry("snapshot /x id=1 version=2")

    def test_decode_tool_error_preserves_typed_contract(self):
        error = self.module.decode_tool_error(
            {
                "status": "error",
                "message": json.dumps(
                    {
                        "code": "SnapshotRootMismatch",
                        "message": "foreign snapshot",
                        "retryable": False,
                        "details": {"snapshot_id": 42},
                    }
                ),
            }
        )

        self.assertEqual(error.code, "SnapshotRootMismatch")
        self.assertEqual(error.details["snapshot_id"], 42)
        self.assertFalse(error.retryable)

    def test_decode_tool_error_rejects_success(self):
        with self.assertRaisesRegex(
            self.module.AcceptanceError, "not an MCP tool error"
        ):
            self.module.decode_tool_error({"status": "success"})

    def test_page_accumulator_rejects_duplicates_and_cursor_stalls(self):
        pages = self.module.PageAccumulator(2)
        pages.add(["a", "b"], "62", True)

        with self.assertRaisesRegex(self.module.AcceptanceError, "duplicate entry"):
            pages.add(["b", "c"], "63", True)

        pages = self.module.PageAccumulator(2)
        pages.add(["a"], "61", True)
        with self.assertRaisesRegex(self.module.AcceptanceError, "cursor repeated"):
            pages.add(["b"], "61", True)

    def test_page_accumulator_requires_global_order_and_terminal_cursor(self):
        pages = self.module.PageAccumulator(2)
        pages.add(["a", "b"], "62", True)
        pages.add(["c"], None, False)
        self.assertEqual(pages.names, ["a", "b", "c"])
        self.assertEqual(pages.page_count, 2)
        self.assertEqual(pages.page_sizes, [2, 1])

        with self.assertRaisesRegex(self.module.AcceptanceError, "not sorted"):
            self.module.PageAccumulator(2).add(["b", "a"], None, False)

        with self.assertRaisesRegex(self.module.AcceptanceError, "next_cursor"):
            self.module.PageAccumulator(2).add(["a"], "61", False)

    def test_page_accumulator_enforces_limit_and_nonempty_truncated_page(self):
        with self.assertRaisesRegex(self.module.AcceptanceError, "above limit"):
            self.module.PageAccumulator(2).add(["a", "b", "c"], None, False)
        with self.assertRaisesRegex(self.module.AcceptanceError, "truncated page is empty"):
            self.module.PageAccumulator(2).add([], "61", True)

    def test_tool_contract_requires_restore_schema(self):
        base = [
            {"name": name, "schema": {"type": "object"}}
            for name in self.module.BASE_WORKBENCH_TOOLS
        ]
        restore = {
            "name": "workbench_restore",
            "schema": {
                "type": "object",
                "required": ["id", "at_snapshot", "destination_id"],
                "properties": {
                    "id": {"type": "string"},
                    "at_snapshot": {
                        "anyOf": [
                            {"type": "integer", "minimum": 0},
                            {"type": "string", "minLength": 1},
                        ]
                    },
                    "destination_id": {"type": "string"},
                },
                "additionalProperties": False,
            },
        }

        self.assertTrue(self.module.validate_tool_contract(base + [restore], True))
        self.assertFalse(self.module.validate_tool_contract(base, False))
        with self.assertRaisesRegex(self.module.AcceptanceError, "workbench_restore"):
            self.module.validate_tool_contract(base, True)

    def test_tool_contract_rejects_extra_restore_arguments(self):
        tools = [
            {"name": name, "schema": {"type": "object"}}
            for name in self.module.BASE_WORKBENCH_TOOLS
        ]
        tools.append(
            {
                "name": "workbench_restore",
                "schema": {
                    "type": "object",
                    "required": ["id", "at_snapshot", "destination_id"],
                    "properties": {
                        "id": {},
                        "at_snapshot": {},
                        "destination_id": {},
                        "in_place": {},
                    },
                    "additionalProperties": False,
                },
            }
        )

        with self.assertRaisesRegex(self.module.AcceptanceError, "exactly"):
            self.module.validate_tool_contract(tools, True)

    def test_tool_contract_rejects_loose_restore_snapshot_alternatives(self):
        base = [
            {"name": name, "schema": {"type": "object"}}
            for name in self.module.BASE_WORKBENCH_TOOLS
        ]

        def restore_schema(alternatives):
            return {
                "name": "workbench_restore",
                "schema": {
                    "type": "object",
                    "required": ["id", "at_snapshot", "destination_id"],
                    "properties": {
                        "id": {"type": "string"},
                        "at_snapshot": {"anyOf": alternatives},
                        "destination_id": {"type": "string"},
                    },
                    "additionalProperties": False,
                },
            }

        with self.assertRaisesRegex(self.module.AcceptanceError, "exactly two"):
            self.module.validate_tool_contract(
                base
                + [
                    restore_schema(
                        [
                            {"type": "integer", "minimum": 0},
                            {"type": "string", "minLength": 1},
                            {"type": "null"},
                        ]
                    )
                ],
                True,
            )
        with self.assertRaisesRegex(self.module.AcceptanceError, "non-empty"):
            self.module.validate_tool_contract(
                base
                + [
                    restore_schema(
                        [
                            {"type": "integer", "minimum": 0},
                            {"type": "string"},
                        ]
                    )
                ],
                True,
            )

    def test_reaper_counts_reject_inconsistent_accounting(self):
        valid = {
            "object_gc": {
                "snapshot_reap": {
                    "expired_candidates": 3,
                    "reaped": 2,
                    "conflicted": 1,
                }
            }
        }
        self.assertEqual(
            self.module.AcceptanceSuite._reap_counts(valid),
            (3, 2, 1),
        )
        valid["object_gc"]["snapshot_reap"]["conflicted"] = 0
        with self.assertRaisesRegex(self.module.AcceptanceError, "accounting violated"):
            self.module.AcceptanceSuite._reap_counts(valid)

    def test_require_all_rejects_quick_profile(self):
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as raised:
                self.module.parse_args(["--require-all"])
        self.assertEqual(raised.exception.code, 2)
        args = self.module.parse_args(["--profile", "full", "--require-all"])
        self.assertEqual(args.profile, "full")
        self.assertTrue(args.require_all)


if __name__ == "__main__":
    unittest.main()
