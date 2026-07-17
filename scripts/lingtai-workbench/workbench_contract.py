#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Frozen semantic contract for LingTai's NoKV workbench MCP surface."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


RESTORE_TOOL = "workbench_restore"
REQUIRED_CAPABILITY = "restore_to_fork_v1"
BASE_WORKBENCH_TOOLS = {
    "workbench_create",
    "workbench_put_file",
    "workbench_append",
    "workbench_edit",
    "workbench_list",
    "workbench_stat",
    "workbench_read",
    "workbench_grep",
    "workbench_search",
    "workbench_aggregate",
    "workbench_catalog",
    "workbench_find",
    "workbench_commit",
    "workbench_snapshot",
    "workbench_snapshot_renew",
    "workbench_snapshot_list",
}
WORKBENCH_TOOLS = BASE_WORKBENCH_TOOLS | {RESTORE_TOOL}
CONTRACT_SNAPSHOT_SCHEMA = "nokv.workbench.mcp_input_schemas.v1"
CONTRACT_SNAPSHOT_PATH = Path(__file__).with_name("workbench_contract_schema.json")

# JSON Schema annotations never change which tool arguments are accepted. Keep
# them out of the deployment digest so wording-only releases do not require a
# contract override. Validation keywords, including format and content*, stay
# in the comparison because clients may enforce them.
ANNOTATION_KEYWORDS = frozenset(
    {
        "$comment",
        "default",
        "deprecated",
        "description",
        "example",
        "examples",
        "readOnly",
        "title",
        "writeOnly",
    }
)
UNORDERED_SCHEMA_ARRAY_KEYWORDS = frozenset({"allOf", "anyOf", "oneOf"})
UNORDERED_LITERAL_ARRAY_KEYWORDS = frozenset({"enum", "required", "type"})
SCHEMA_MAP_KEYWORDS = frozenset(
    {"$defs", "definitions", "dependentSchemas", "patternProperties", "properties"}
)
SCHEMA_VALUE_KEYWORDS = frozenset(
    {
        "additionalProperties",
        "contains",
        "contentSchema",
        "else",
        "if",
        "items",
        "not",
        "propertyNames",
        "then",
        "unevaluatedItems",
        "unevaluatedProperties",
    }
)


class WorkbenchContractError(ValueError):
    """A live MCP surface cannot satisfy the LingTai workbench contract."""


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def json_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _normalize_literal(value: Any) -> Any:
    """Canonicalize JSON data without treating its keys as schema keywords."""
    if isinstance(value, dict):
        return {key: _normalize_literal(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalize_literal(item) for item in value]
    return value


def _normalize_schema_value(value: Any) -> Any:
    if isinstance(value, list):
        return [normalize_schema(item) for item in value]
    return normalize_schema(value)


def normalize_schema(value: Any) -> Any:
    """Return a semantic JSON Schema form used for exact contract comparison.

    The normalizer deliberately does not resolve references or simplify schema
    logic. It only removes standard annotations and canonicalizes keywords whose
    array order has no validation meaning. Every remaining keyword must match
    the Rust-owned frozen contract exactly.
    """
    if isinstance(value, bool):
        return value
    if not isinstance(value, dict):
        return _normalize_literal(value)

    normalized: dict[str, Any] = {}
    for key, item in value.items():
        if key in ANNOTATION_KEYWORDS:
            continue
        if key in UNORDERED_SCHEMA_ARRAY_KEYWORDS:
            branches = [normalize_schema(branch) for branch in item]
            normalized[key] = sorted(branches, key=canonical_json)
        elif key in UNORDERED_LITERAL_ARRAY_KEYWORDS and isinstance(item, list):
            values = [_normalize_literal(element) for element in item]
            normalized[key] = sorted(values, key=canonical_json)
        elif key in SCHEMA_MAP_KEYWORDS and isinstance(item, dict):
            # Map keys are user property/definition names. A property literally
            # named "description" must not be mistaken for an annotation.
            normalized[key] = {
                name: normalize_schema(schema) for name, schema in item.items()
            }
        elif key in SCHEMA_VALUE_KEYWORDS:
            normalized[key] = _normalize_schema_value(item)
        else:
            normalized[key] = _normalize_literal(item)
    return normalized


def _load_frozen_input_schemas() -> dict[str, dict[str, Any]]:
    try:
        snapshot = json.loads(CONTRACT_SNAPSHOT_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as err:
        raise RuntimeError(
            f"cannot load frozen workbench contract {CONTRACT_SNAPSHOT_PATH}: {err}"
        ) from err
    if not isinstance(snapshot, dict):
        raise RuntimeError("frozen workbench contract must be a JSON object")
    if snapshot.get("schema") != CONTRACT_SNAPSHOT_SCHEMA:
        raise RuntimeError("frozen workbench contract has the wrong schema marker")
    schemas = snapshot.get("inputSchemas")
    if not isinstance(schemas, dict) or not all(
        isinstance(name, str) and isinstance(schema, dict)
        for name, schema in schemas.items()
    ):
        raise RuntimeError("frozen workbench contract has invalid inputSchemas")
    if set(schemas) != WORKBENCH_TOOLS:
        raise RuntimeError(
            "frozen workbench contract tool names differ from WORKBENCH_TOOLS"
        )
    return {name: normalize_schema(schema) for name, schema in schemas.items()}


FROZEN_INPUT_SCHEMAS = _load_frozen_input_schemas()


def extract_raw_tools(response: Any) -> list[dict[str, Any]]:
    if not isinstance(response, dict):
        raise WorkbenchContractError("tools/list response must be a JSON object")
    if response.get("jsonrpc") != "2.0" or response.get("id") != 1:
        raise WorkbenchContractError(
            "tools/list response has the wrong JSON-RPC envelope"
        )
    result = response.get("result")
    if not isinstance(result, dict):
        raise WorkbenchContractError("tools/list response lacks a result object")
    tools = result.get("tools")
    if not isinstance(tools, list):
        raise WorkbenchContractError("tools/list result lacks a tools array")
    if not all(isinstance(tool, dict) for tool in tools):
        raise WorkbenchContractError("tools/list contains a non-object tool")
    return tools


def _schema(tool: dict[str, Any], schema_key: str) -> dict[str, Any]:
    schema = tool.get(schema_key)
    if not isinstance(schema, dict):
        name = tool.get("name", "<unknown>")
        raise WorkbenchContractError(f"{name} lacks {schema_key}")
    return schema


def validate_tool_contract(
    tools: list[dict[str, Any]],
    *,
    schema_key: str = "inputSchema",
) -> None:
    names = [tool.get("name") for tool in tools]
    if any(not isinstance(name, str) or not name for name in names):
        raise WorkbenchContractError("tools/list contains a tool without a string name")
    if len(set(names)) != len(names):
        raise WorkbenchContractError("tools/list contains duplicate tool names")

    actual = set(names)
    if actual != WORKBENCH_TOOLS:
        raise WorkbenchContractError(
            "unexpected workbench tool surface; "
            f"missing={sorted(WORKBENCH_TOOLS - actual)}, "
            f"extra={sorted(actual - WORKBENCH_TOOLS)}"
        )

    by_name = {tool["name"]: tool for tool in tools}
    for name in sorted(WORKBENCH_TOOLS):
        actual_schema = normalize_schema(_schema(by_name[name], schema_key))
        expected_schema = FROZEN_INPUT_SCHEMAS[name]
        if actual_schema != expected_schema:
            raise WorkbenchContractError(
                f"{name} inputSchema differs from the frozen Rust contract; "
                f"expected_sha256={json_sha256(expected_schema)}, "
                f"actual_sha256={json_sha256(actual_schema)}"
            )


def contract_payload(
    tools: list[dict[str, Any]],
    *,
    schema_key: str = "inputSchema",
) -> list[dict[str, Any]]:
    """Return the semantic invocation contract, excluding annotations and order."""
    validate_tool_contract(tools, schema_key=schema_key)
    return sorted(
        (
            {
                "name": tool["name"],
                "inputSchema": normalize_schema(_schema(tool, schema_key)),
            }
            for tool in tools
        ),
        key=lambda item: item["name"],
    )


def contract_evidence(
    tools: list[dict[str, Any]],
    *,
    schema_key: str = "inputSchema",
) -> dict[str, Any]:
    payload = contract_payload(tools, schema_key=schema_key)
    restore = next(item for item in payload if item["name"] == RESTORE_TOOL)
    return {
        "required_capabilities": [REQUIRED_CAPABILITY],
        "tool_count": len(payload),
        "tool_names": [item["name"] for item in payload],
        "tools_schema_sha256": json_sha256(payload),
        "restore_schema_sha256": json_sha256(restore["inputSchema"]),
    }


def expected_contract_evidence() -> dict[str, Any]:
    """Return evidence for the checked-in Rust-owned 17-tool contract."""
    tools = [
        {"name": name, "inputSchema": schema}
        for name, schema in FROZEN_INPUT_SCHEMAS.items()
    ]
    return contract_evidence(tools)
