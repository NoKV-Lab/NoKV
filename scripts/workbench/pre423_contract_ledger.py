#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Validate the machine-readable pre-#423 Workbench contract ledger."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


LEDGER_SCHEMA = "nokv.workbench.pre423_contract_ledger.v1"
LEDGER_PATH = Path(__file__).with_name("pre423_contract_ledger.json")
EXPECTED_IDS = {
    *(f"T{index:02d}" for index in range(1, 19)),
    *(f"C{index:02d}" for index in range(1, 22)),
    *(f"L{index:02d}" for index in range(1, 9)),
}
ALLOWED_CLASSES = frozenset({"A", "B", "C", "D"})
ALLOWED_DISPOSITIONS = frozenset({"restore", "replace", "retire", "do-not-restore"})
REQUIRED_ITEM_KEYS = frozenset(
    {
        "id",
        "scope",
        "class",
        "current_disposition",
        "owner",
        "boundary",
        "requirement",
        "source_evidence",
        "required_gates",
    }
)


class LedgerError(ValueError):
    """The checked-in ledger is incomplete or violates its policy."""


@dataclass(frozen=True)
class LedgerSummary:
    items: int
    core: int
    legacy: int
    classes: dict[str, int]
    dispositions: dict[str, int]


def load_ledger(path: Path = LEDGER_PATH) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as err:
        raise LedgerError(f"cannot load contract ledger {path}: {err}") from err
    if not isinstance(value, dict):
        raise LedgerError("contract ledger must be a JSON object")
    return value


def _nonempty_string(value: Any, field: str, item_id: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise LedgerError(f"{item_id}.{field} must be a non-empty string")


def _nonempty_string_list(value: Any, field: str, item_id: str) -> list[str]:
    if (
        not isinstance(value, list)
        or not value
        or any(not isinstance(element, str) or not element.strip() for element in value)
    ):
        raise LedgerError(f"{item_id}.{field} must be a non-empty string array")
    if len(value) != len(set(value)):
        raise LedgerError(f"{item_id}.{field} must not contain duplicates")
    return value


def validate_ledger(ledger: dict[str, Any]) -> LedgerSummary:
    if ledger.get("schema") != LEDGER_SCHEMA:
        raise LedgerError(f"contract ledger must use schema {LEDGER_SCHEMA}")

    for revision_field in ("pre_revision", "recovery_baseline_revision"):
        revision = ledger.get(revision_field)
        if (
            not isinstance(revision, str)
            or len(revision) != 40
            or any(character not in "0123456789abcdef" for character in revision)
        ):
            raise LedgerError(f"{revision_field} must be a lowercase 40-byte git SHA")

    gate_catalog = ledger.get("gate_catalog")
    if not isinstance(gate_catalog, dict) or not gate_catalog:
        raise LedgerError("gate_catalog must be a non-empty object")
    for gate_id, description in gate_catalog.items():
        _nonempty_string(gate_id, "gate id", "gate_catalog")
        _nonempty_string(description, "description", f"gate_catalog.{gate_id}")

    items = ledger.get("items")
    if not isinstance(items, list):
        raise LedgerError("items must be an array")
    if len(items) != 47:
        raise LedgerError(f"items must contain exactly 47 entries, got {len(items)}")

    ids: list[str] = []
    scopes: list[str] = []
    classes: list[str] = []
    dispositions: list[str] = []
    for position, item in enumerate(items):
        if not isinstance(item, dict):
            raise LedgerError(f"items[{position}] must be an object")
        missing = REQUIRED_ITEM_KEYS - set(item)
        if missing:
            raise LedgerError(
                f"items[{position}] is missing required keys {sorted(missing)}"
            )

        item_id = item.get("id")
        _nonempty_string(item_id, "id", f"items[{position}]")
        ids.append(item_id)

        scope = item.get("scope")
        if scope not in {"core", "legacy"}:
            raise LedgerError(f"{item_id}.scope must be core or legacy")
        scopes.append(scope)

        expected_scope = "legacy" if item_id.startswith("L") else "core"
        if scope != expected_scope:
            raise LedgerError(
                f"{item_id}.scope must be {expected_scope} for its stable id"
            )

        item_class = item.get("class")
        if item_class not in ALLOWED_CLASSES:
            raise LedgerError(
                f"{item_id}.class must be one of {sorted(ALLOWED_CLASSES)}"
            )
        classes.append(item_class)

        disposition = item.get("current_disposition")
        if disposition not in ALLOWED_DISPOSITIONS:
            raise LedgerError(
                f"{item_id}.current_disposition must be one of "
                f"{sorted(ALLOWED_DISPOSITIONS)}"
            )
        dispositions.append(disposition)

        if item_class in {"A", "B"} and disposition in {
            "retire",
            "do-not-restore",
        }:
            raise LedgerError(
                f"{item_id}: class {item_class} cannot be silently {disposition}"
            )
        if item_class == "D" and disposition != "do-not-restore":
            raise LedgerError(
                f"{item_id}: class D must have do-not-restore disposition"
            )
        if disposition == "do-not-restore" and item_class != "D":
            raise LedgerError(
                f"{item_id}: do-not-restore disposition is reserved for class D"
            )

        for field in ("owner", "boundary", "requirement"):
            _nonempty_string(item.get(field), field, item_id)
        evidence = _nonempty_string_list(
            item.get("source_evidence"), "source_evidence", item_id
        )
        if not any(source.startswith("pre:") for source in evidence):
            raise LedgerError(f"{item_id}.source_evidence must cite a pre: source")
        required_gates = _nonempty_string_list(
            item.get("required_gates"), "required_gates", item_id
        )
        unknown_gates = set(required_gates) - set(gate_catalog)
        if unknown_gates:
            raise LedgerError(
                f"{item_id}.required_gates references unknown gates "
                f"{sorted(unknown_gates)}"
            )

    if len(ids) != len(set(ids)):
        duplicates = sorted({item_id for item_id in ids if ids.count(item_id) > 1})
        raise LedgerError(f"contract ledger contains duplicate ids {duplicates}")
    actual_ids = set(ids)
    if actual_ids != EXPECTED_IDS:
        raise LedgerError(
            "contract ledger stable ids differ from the canonical 47-item set; "
            f"missing={sorted(EXPECTED_IDS - actual_ids)}, "
            f"extra={sorted(actual_ids - EXPECTED_IDS)}"
        )

    core_count = scopes.count("core")
    legacy_count = scopes.count("legacy")
    if core_count != 39 or legacy_count != 8:
        raise LedgerError(
            "contract ledger must contain exactly 39 core and 8 legacy entries; "
            f"got core={core_count}, legacy={legacy_count}"
        )

    by_id = {item["id"]: item for item in items}
    fuse_boundary = by_id["L07"]
    if (
        fuse_boundary["class"] != "D"
        or fuse_boundary["current_disposition"] != "do-not-restore"
        or "api-absence" not in fuse_boundary["required_gates"]
    ):
        raise LedgerError(
            "L07 FUSE/POSIX must remain class D, do-not-restore, and API-absence gated"
        )

    return LedgerSummary(
        items=len(items),
        core=core_count,
        legacy=legacy_count,
        classes={value: classes.count(value) for value in sorted(ALLOWED_CLASSES)},
        dispositions={
            value: dispositions.count(value) for value in sorted(ALLOWED_DISPOSITIONS)
        },
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate the pre-#423 Workbench contract recovery ledger."
    )
    parser.add_argument(
        "--ledger",
        type=Path,
        default=LEDGER_PATH,
        help="ledger JSON path",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        summary = validate_ledger(load_ledger(args.ledger))
    except LedgerError as err:
        print(f"FAIL: {err}")
        return 2
    print(
        "PASS: pre-#423 contract ledger "
        f"items={summary.items} core={summary.core} legacy={summary.legacy} "
        f"classes={json.dumps(summary.classes, sort_keys=True)} "
        f"dispositions={json.dumps(summary.dispositions, sort_keys=True)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
