#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0
"""The native CLI failure contract of docs/cli-contract.md as a grader.

A gate that drives the native `nokv` binary records each finished command as a
`Call` and grades it here. A success prints one JSON result and exits 0. A
failure prints nothing to stdout, exactly one JSON envelope line to stderr that
names a published `class` and `next_action`, and exits with the code of its
class. This module imports no gate, so every gate can import it.
"""

from __future__ import annotations

import dataclasses
import json
from pathlib import Path
from typing import Any

# The published exit codes by class and the published next actions.
Contract = tuple[dict[str, int], set[str]]


class ContractFailure(Exception):
    """The binary under test broke the published contract."""


class NotQualified(Exception):
    """The gate could not establish a scenario's precondition."""


def require(condition: bool, claim: str, observed: Any = None) -> None:
    if not condition:
        detail = json.dumps(observed, sort_keys=True, default=str)[:900]
        raise ContractFailure(f"{claim}: {detail}")


def contract_tables(document: str) -> tuple[dict[str, int], set[str]]:
    """The class-to-exit-code map and the next actions the contract publishes."""

    def rows(name: str) -> list[list[str]]:
        begin, end = f"<!-- {name}:begin -->", f"<!-- {name}:end -->"
        start = document.index(begin) + len(begin)
        table = document[start : document.index(end, start)]
        cells = [
            [cell.strip() for cell in line.strip().strip("|").split("|")]
            for line in table.splitlines()
            if line.startswith("| ") and not line.startswith("| ---")
        ]
        return cells[1:]

    exits = {
        row[1].strip("`"): int(row[0]) for row in rows("exit-codes") if row[1]
    }
    actions = {row[0].strip("`") for row in rows("next-actions")}
    if not exits or not actions:
        raise NotQualified("docs/cli-contract.md publishes no contract tables")
    return exits, actions


def published_contract(repository: Path) -> Contract:
    """The contract tables of the repository's docs/cli-contract.md."""
    return contract_tables((repository / "docs" / "cli-contract.md").read_text())


@dataclasses.dataclass
class Call:
    """One finished `nokv` command. The owner connection and request counts
    come from the proxy; a gate without one leaves them at their defaults."""

    label: str
    exit: int
    stdout: str
    stderr: str
    connections: int = 0
    requests: list[str] = dataclasses.field(default_factory=list)


def contract_success(call: Call) -> dict[str, Any]:
    """The call exited 0 and printed one JSON success result."""
    require(call.exit == 0, f"{call.label} succeeds", [call.exit, call.stderr[-600:]])
    try:
        result = json.loads(call.stdout)
    except json.JSONDecodeError:
        raise ContractFailure(f"{call.label} prints one JSON result: {call.stdout[:300]}")
    require(result.get("status") == "success", f"{call.label} reports success", result)
    return result


def contract_failure(call: Call, exits: dict[str, int], actions: set[str], klass: str,
                     next_action: str) -> dict[str, Any]:
    """The call failed with exactly the contract envelope of `klass`: nothing on
    stdout, one JSON line on stderr, and the exit code of its class."""
    observed = [call.exit, call.stdout[:200], call.stderr[-600:]]
    require(call.exit != 0, f"{call.label} fails", observed)
    require(call.stdout == "", f"{call.label} writes nothing to stdout", observed)
    lines = call.stderr.splitlines()
    require(len(lines) == 1, f"{call.label} writes exactly one stderr line", observed)
    try:
        envelope = json.loads(lines[0])
    except json.JSONDecodeError:
        raise ContractFailure(f"{call.label} writes a JSON failure line: {lines[0][:300]}")
    require(
        isinstance(envelope, dict)
        and envelope.get("status") == "error"
        and {"code", "message", "retryable", "details"} <= envelope.keys(),
        f"{call.label} keeps the Agent error envelope",
        envelope,
    )
    require(envelope.get("class") in exits, f"{call.label} names a published class", envelope)
    require(envelope.get("next_action") in actions, f"{call.label} names a published next action", envelope)
    require(
        [envelope["class"], envelope["next_action"]] == [klass, next_action],
        f"{call.label} is {klass} with {next_action}",
        envelope,
    )
    require(
        call.exit == exits[klass],
        f"{call.label} exits with the code of {klass}",
        [call.exit, exits[klass]],
    )
    return envelope


def owner_serves(call: Call, exits: dict[str, int], actions: set[str]) -> bool:
    """An `operation status` probe of an unknown id shows that the owner serves
    metadata reads once it answers not_found rather than a transport failure."""
    try:
        envelope = contract_failure(call, exits, actions, "not_found", "resubmit_same")
    except ContractFailure:
        return False
    return envelope.get("code") == "NotFound"
