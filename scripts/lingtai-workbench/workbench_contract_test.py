#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import copy
import sys
import unittest
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

import workbench_contract as contract  # noqa: E402


def frozen_tools(*, schema_key: str = "inputSchema") -> list[dict]:
    return [
        {
            "name": name,
            "description": f"description for {name}",
            schema_key: copy.deepcopy(schema),
        }
        for name, schema in contract.FROZEN_INPUT_SCHEMAS.items()
    ]


def reverse_unordered_arrays(value: object) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            if key in {
                "allOf",
                "anyOf",
                "enum",
                "oneOf",
                "required",
                "type",
            } and isinstance(item, list):
                item.reverse()
            reverse_unordered_arrays(item)
    elif isinstance(value, list):
        for item in value:
            reverse_unordered_arrays(item)


class WorkbenchContractTest(unittest.TestCase):
    def test_frozen_surface_is_the_exact_seventeen_tools(self):
        self.assertEqual(len(contract.FROZEN_INPUT_SCHEMAS), 17)
        self.assertEqual(set(contract.FROZEN_INPUT_SCHEMAS), contract.WORKBENCH_TOOLS)
        contract.validate_tool_contract(frozen_tools())

    def test_every_tool_schema_is_compared(self):
        for name in sorted(contract.WORKBENCH_TOOLS):
            with self.subTest(tool=name):
                tools = frozen_tools()
                tool = next(item for item in tools if item["name"] == name)
                tool["inputSchema"]["maxProperties"] = 999
                with self.assertRaisesRegex(
                    contract.WorkbenchContractError,
                    rf"^{name} inputSchema differs",
                ):
                    contract.validate_tool_contract(tools)

    def test_missing_field_and_extra_restriction_fail(self):
        missing = frozen_tools()
        create = next(tool for tool in missing if tool["name"] == "workbench_create")
        del create["inputSchema"]["properties"]["id"]
        with self.assertRaises(contract.WorkbenchContractError):
            contract.validate_tool_contract(missing)

        restricted = frozen_tools()
        create = next(tool for tool in restricted if tool["name"] == "workbench_create")
        create["inputSchema"]["properties"]["id"]["maxLength"] = 128
        with self.assertRaises(contract.WorkbenchContractError):
            contract.validate_tool_contract(restricted)

    def test_annotations_are_recursively_ignored(self):
        tools = frozen_tools()
        for tool in tools:
            schema = tool["inputSchema"]
            schema.update(
                {
                    "$comment": "generated from Rust",
                    "default": {},
                    "deprecated": False,
                    "description": "updated wording",
                    "example": {},
                    "examples": [],
                    "readOnly": False,
                    "title": "Workbench input",
                    "writeOnly": False,
                }
            )
        restore = next(tool for tool in tools if tool["name"] == contract.RESTORE_TOOL)
        restore["inputSchema"]["properties"]["at_snapshot"]["anyOf"][0][
            "description"
        ] = "nested wording"

        contract.validate_tool_contract(tools)
        self.assertEqual(
            contract.contract_evidence(tools), contract.expected_contract_evidence()
        )

    def test_unordered_schema_arrays_do_not_change_evidence(self):
        tools = frozen_tools()
        reverse_unordered_arrays(tools)

        contract.validate_tool_contract(tools)
        self.assertEqual(
            contract.contract_evidence(tools), contract.expected_contract_evidence()
        )

    def test_one_of_and_all_of_order_is_semantic(self):
        first = {
            "allOf": [{"type": "string"}, {"minLength": 1}],
            "oneOf": [{"const": "a"}, {"const": "b"}],
        }
        second = {
            "allOf": list(reversed(first["allOf"])),
            "oneOf": list(reversed(first["oneOf"])),
        }
        self.assertEqual(
            contract.normalize_schema(first), contract.normalize_schema(second)
        )

    def test_annotation_named_property_and_enum_data_are_preserved(self):
        schema = {
            "description": "outer annotation",
            "properties": {
                "description": {
                    "type": "string",
                    "description": "inner annotation",
                }
            },
            "enum": [{"description": "literal data"}],
        }
        self.assertEqual(
            contract.normalize_schema(schema),
            {
                "properties": {"description": {"type": "string"}},
                "enum": [{"description": "literal data"}],
            },
        )

    def test_alternate_schema_key_uses_the_same_frozen_contract(self):
        tools = frozen_tools(schema_key="input_schema")
        contract.validate_tool_contract(tools, schema_key="input_schema")
        self.assertEqual(
            contract.contract_evidence(tools, schema_key="input_schema"),
            contract.expected_contract_evidence(),
        )


if __name__ == "__main__":
    unittest.main()
