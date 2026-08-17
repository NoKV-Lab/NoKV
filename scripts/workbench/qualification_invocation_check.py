#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Check that a Phase 1 aggregate exactly matches its closed invocation manifest."""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

import pre423_contract_ledger as ledger_module
from qualification_aggregate import AGGREGATE_SCHEMA


MANIFEST_SCHEMA = "nokv.pre423.qualification_invocations.v1"
MANIFEST_PATH = Path(__file__).with_name("qualification_invocation_manifest.json")
INVOCATION_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]{0,63}$")
HEX_40 = re.compile(r"^[0-9a-f]{40}$")
HEX_64 = re.compile(r"^[0-9a-f]{64}$")
OUTCOME_EXIT_CODES = {"PASS": 0, "NQ": 3}
STATUS_PRIORITY = {"PASS": 0, "NQ": 1, "FAIL": 2}
RECEIPT_COUNT_FIELDS = {
    "discovered",
    "accepted",
    "selected",
    "superseded",
    "rejected",
    "invalid",
}


class InvocationCheckError(ValueError):
    """The invocation manifest or aggregate report is not a closed exact match."""


@dataclass(frozen=True)
class ExpectedScenario:
    producer: str
    outcome: str


@dataclass(frozen=True)
class CheckSummary:
    typed_scenarios: int
    pass_scenarios: int
    nq_scenarios: int


def _load_json(path: Path, field: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise InvocationCheckError(f"cannot load {field} {path}: {error}") from error
    if not isinstance(value, dict):
        raise InvocationCheckError(f"{field} must be a JSON object")
    return value


def _closed_object(value: Any, fields: set[str], name: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != fields:
        raise InvocationCheckError(f"{name} fields must be exactly {sorted(fields)}")
    return value


def _string_list(value: Any, name: str, *, allow_empty: bool = False) -> list[str]:
    if (
        not isinstance(value, list)
        or any(not isinstance(entry, str) or not entry for entry in value)
        or len(value) != len(set(value))
        or (not allow_empty and not value)
    ):
        qualifier = (
            "a unique string list" if allow_empty else "a non-empty unique string list"
        )
        raise InvocationCheckError(f"{name} must be {qualifier}")
    return value


def _status_worst(statuses: list[str], name: str) -> str:
    if not statuses or any(status not in STATUS_PRIORITY for status in statuses):
        raise InvocationCheckError(f"{name} contains an invalid or empty status set")
    return max(statuses, key=STATUS_PRIORITY.__getitem__)


def _typed_eligible_scenarios(
    ledger: dict[str, Any], typed_producers: set[str]
) -> set[tuple[str, str, str]]:
    return {
        (item["id"], gate, scenario)
        for item in ledger["items"]
        for gate in item["required_gates"]
        for expectation in (
            ledger_module.resolve_gate_expectation(ledger, item["id"], gate),
        )
        if typed_producers.intersection(expectation["allowed_producers"])
        for scenario in expectation["scenarios"]
    }


def derive_expected_scenarios(
    *, manifest: dict[str, Any], ledger: dict[str, Any]
) -> dict[tuple[str, str, str], ExpectedScenario]:
    """Validate the closed manifest and derive every typed scenario expectation."""

    ledger_module.validate_ledger(ledger)
    _closed_object(
        manifest,
        {"schema", "typed_producers", "invocations", "aggregate_expectation"},
        "manifest",
    )
    if manifest["schema"] != MANIFEST_SCHEMA:
        raise InvocationCheckError(f"manifest must use schema {MANIFEST_SCHEMA}")
    typed_producer_values = _string_list(
        manifest["typed_producers"], "manifest.typed_producers"
    )
    typed_producers = set(typed_producer_values)
    unknown_producers = typed_producers - set(ledger["producer_catalog"])
    if unknown_producers:
        raise InvocationCheckError(
            f"manifest has unknown typed producers {sorted(unknown_producers)}"
        )

    invocations = manifest["invocations"]
    if not isinstance(invocations, list) or not invocations:
        raise InvocationCheckError("manifest.invocations must be a non-empty array")
    invocation_ids: set[str] = set()
    invoked_producers: set[str] = set()
    expected: dict[tuple[str, str, str], ExpectedScenario] = {}
    expected_receipts = 0
    for index, raw_invocation in enumerate(invocations):
        name = f"manifest.invocations[{index}]"
        invocation = _closed_object(
            raw_invocation,
            {
                "id",
                "producer",
                "evidence_kind",
                "script",
                "expected_outcome",
                "expected_exit_code",
                "uses_rust_target",
                "claims",
            },
            name,
        )
        invocation_id = invocation["id"]
        if (
            not isinstance(invocation_id, str)
            or not INVOCATION_ID_PATTERN.fullmatch(invocation_id)
            or Path(invocation_id).name != invocation_id
        ):
            raise InvocationCheckError(f"{name}.id is not a safe invocation id")
        if invocation_id in invocation_ids:
            raise InvocationCheckError(f"duplicate invocation id {invocation_id!r}")
        invocation_ids.add(invocation_id)

        producer = invocation["producer"]
        if not isinstance(producer, str) or producer not in typed_producers:
            raise InvocationCheckError(f"{name}.producer is not a typed producer")
        invoked_producers.add(producer)
        producer_contract = ledger["producer_catalog"][producer]
        if invocation["script"] != producer_contract["command"]["entrypoint"]:
            raise InvocationCheckError(f"{name}.script does not match the ledger")
        evidence_kind = invocation["evidence_kind"]
        if evidence_kind not in producer_contract["evidence_kinds"]:
            raise InvocationCheckError(
                f"{name}.evidence_kind is not allowed for {producer!r}"
            )
        outcome = invocation["expected_outcome"]
        if outcome not in OUTCOME_EXIT_CODES or invocation["expected_exit_code"] != (
            OUTCOME_EXIT_CODES.get(outcome)
        ):
            raise InvocationCheckError(f"{name} has an invalid outcome/exit pair")
        uses_rust_target = invocation["uses_rust_target"]
        expected_rust_target = (
            "rust_toolchain" in producer_contract["required_subjects"]
        )
        if (
            not isinstance(uses_rust_target, bool)
            or uses_rust_target != expected_rust_target
        ):
            raise InvocationCheckError(
                f"{name}.uses_rust_target does not match the producer contract"
            )
        claims = _string_list(invocation["claims"], f"{name}.claims")
        receipt_groups: set[tuple[str, str]] = set()
        for claim in claims:
            parts = claim.split(":", 2)
            if len(parts) != 3 or any(not part for part in parts):
                raise InvocationCheckError(f"{name} has malformed claim {claim!r}")
            stable_id, gate, scenario = parts
            try:
                expectation = ledger_module.resolve_gate_expectation(
                    ledger, stable_id, gate
                )
            except ledger_module.LedgerError as error:
                raise InvocationCheckError(
                    f"{name} has invalid claim {claim!r}: {error}"
                ) from error
            if scenario not in expectation["scenarios"]:
                raise InvocationCheckError(f"{name} has undeclared claim {claim!r}")
            if producer not in expectation["allowed_producers"]:
                raise InvocationCheckError(
                    f"{name} producer is not allowed for claim {claim!r}"
                )
            if evidence_kind not in expectation["allowed_evidence_kinds"]:
                raise InvocationCheckError(
                    f"{name} evidence kind is not allowed for claim {claim!r}"
                )
            key = (stable_id, gate, scenario)
            if key in expected:
                raise InvocationCheckError(
                    f"typed scenario {stable_id}:{gate}:{scenario} is duplicated"
                )
            expected[key] = ExpectedScenario(producer, outcome)
            receipt_groups.add((stable_id, gate))
        expected_receipts += len(receipt_groups)

    if invoked_producers != typed_producers:
        raise InvocationCheckError(
            "manifest invocations do not exactly cover typed producers; "
            f"missing={sorted(typed_producers - invoked_producers)}"
        )
    eligible = _typed_eligible_scenarios(ledger, typed_producers)
    if set(expected) != eligible:
        raise InvocationCheckError(
            "manifest claims do not exactly cover typed-eligible ledger scenarios; "
            f"missing={sorted(eligible - set(expected))} "
            f"extra={sorted(set(expected) - eligible)}"
        )

    aggregate_expectation = _closed_object(
        manifest["aggregate_expectation"],
        {
            "expected_status",
            "expected_exit_code",
            "omitted_producers",
            "product_artifact_manifest",
            "receipt_counts",
        },
        "manifest.aggregate_expectation",
    )
    if (
        aggregate_expectation["expected_status"] != "NQ"
        or aggregate_expectation["expected_exit_code"] != 3
        or aggregate_expectation["product_artifact_manifest"] is not True
    ):
        raise InvocationCheckError(
            "qualification aggregate must remain NQ/exit 3 and require a product manifest"
        )
    omitted = set(
        _string_list(
            aggregate_expectation["omitted_producers"],
            "manifest.aggregate_expectation.omitted_producers",
            allow_empty=True,
        )
    )
    expected_omitted = set(ledger["producer_catalog"]) - typed_producers
    if omitted != expected_omitted:
        raise InvocationCheckError(
            "manifest omitted producers do not match the ledger; "
            f"missing={sorted(expected_omitted - omitted)} "
            f"extra={sorted(omitted - expected_omitted)}"
        )
    receipt_counts = _closed_object(
        aggregate_expectation["receipt_counts"],
        RECEIPT_COUNT_FIELDS,
        "manifest.aggregate_expectation.receipt_counts",
    )
    if any(
        not isinstance(value, int) or isinstance(value, bool) or value < 0
        for value in receipt_counts.values()
    ) or receipt_counts != {
        "discovered": expected_receipts,
        "accepted": expected_receipts,
        "selected": expected_receipts,
        "superseded": 0,
        "rejected": 0,
        "invalid": 0,
    }:
        raise InvocationCheckError(
            "manifest receipt counts do not match its invocation grouping"
        )
    return expected


def _report_scenarios(
    *, report: dict[str, Any], ledger: dict[str, Any]
) -> dict[tuple[str, str, str], dict[str, Any]]:
    items = report.get("items")
    if not isinstance(items, list):
        raise InvocationCheckError("aggregate.items must be an array")
    ledger_items = {item["id"]: item for item in ledger["items"]}
    reported: dict[tuple[str, str, str], dict[str, Any]] = {}
    seen_items: set[str] = set()
    for raw_item in items:
        if not isinstance(raw_item, dict):
            raise InvocationCheckError("aggregate item must be an object")
        stable_id = raw_item.get("stable_id")
        if not isinstance(stable_id, str) or stable_id not in ledger_items:
            raise InvocationCheckError(f"aggregate has unknown stable id {stable_id!r}")
        if stable_id in seen_items:
            raise InvocationCheckError(f"aggregate duplicates stable id {stable_id!r}")
        seen_items.add(stable_id)
        ledger_item = ledger_items[stable_id]
        if (
            raw_item.get("class") != ledger_item["class"]
            or raw_item.get("disposition") != ledger_item["current_disposition"]
        ):
            raise InvocationCheckError(
                f"aggregate item {stable_id} does not match ledger policy"
            )
        gates = raw_item.get("gates")
        if not isinstance(gates, list):
            raise InvocationCheckError(
                f"aggregate item {stable_id} gates must be an array"
            )
        seen_gates: set[str] = set()
        gate_statuses: list[str] = []
        for raw_gate in gates:
            if not isinstance(raw_gate, dict):
                raise InvocationCheckError(
                    f"aggregate item {stable_id} gate is invalid"
                )
            gate = raw_gate.get("gate")
            if not isinstance(gate, str) or gate not in ledger_item["required_gates"]:
                raise InvocationCheckError(
                    f"aggregate item {stable_id} has unknown gate {gate!r}"
                )
            if gate in seen_gates:
                raise InvocationCheckError(
                    f"aggregate duplicates gate {stable_id}:{gate}"
                )
            seen_gates.add(gate)
            expectation = ledger_module.resolve_gate_expectation(
                ledger, stable_id, gate
            )
            if (
                raw_gate.get("allowed_evidence_kinds")
                != expectation["allowed_evidence_kinds"]
                or raw_gate.get("allowed_producers") != expectation["allowed_producers"]
            ):
                raise InvocationCheckError(
                    f"aggregate gate {stable_id}:{gate} does not match ledger policy"
                )
            scenarios = raw_gate.get("scenarios")
            if not isinstance(scenarios, list):
                raise InvocationCheckError(
                    f"aggregate gate {stable_id}:{gate} scenarios must be an array"
                )
            scenario_statuses: list[str] = []
            seen_scenarios: set[str] = set()
            for raw_scenario in scenarios:
                if not isinstance(raw_scenario, dict):
                    raise InvocationCheckError(
                        f"aggregate gate {stable_id}:{gate} scenario is invalid"
                    )
                scenario = raw_scenario.get("scenario")
                if (
                    not isinstance(scenario, str)
                    or scenario not in expectation["scenarios"]
                ):
                    raise InvocationCheckError(
                        f"aggregate gate {stable_id}:{gate} has unknown scenario {scenario!r}"
                    )
                if scenario in seen_scenarios:
                    raise InvocationCheckError(
                        f"aggregate duplicates scenario {stable_id}:{gate}:{scenario}"
                    )
                seen_scenarios.add(scenario)
                status = raw_scenario.get("status")
                if status not in STATUS_PRIORITY:
                    raise InvocationCheckError(
                        f"aggregate scenario {stable_id}:{gate}:{scenario} has invalid status"
                    )
                if not isinstance(raw_scenario.get("selected_receipts"), list):
                    raise InvocationCheckError(
                        f"aggregate scenario {stable_id}:{gate}:{scenario} selected_receipts must be an array"
                    )
                key = (stable_id, gate, scenario)
                if key in reported:
                    raise InvocationCheckError(
                        f"aggregate duplicates scenario {stable_id}:{gate}:{scenario}"
                    )
                reported[key] = raw_scenario
                scenario_statuses.append(status)
            if seen_scenarios != set(expectation["scenarios"]):
                raise InvocationCheckError(
                    f"aggregate gate {stable_id}:{gate} has missing or extra scenarios"
                )
            expected_gate_status = _status_worst(
                scenario_statuses, f"aggregate gate {stable_id}:{gate}"
            )
            if raw_gate.get("status") != expected_gate_status:
                raise InvocationCheckError(
                    f"aggregate gate {stable_id}:{gate} status is inconsistent"
                )
            gate_statuses.append(expected_gate_status)
        if seen_gates != set(ledger_item["required_gates"]):
            raise InvocationCheckError(
                f"aggregate item {stable_id} has missing or extra gates"
            )
        expected_item_status = _status_worst(
            gate_statuses, f"aggregate item {stable_id}"
        )
        if raw_item.get("status") != expected_item_status:
            raise InvocationCheckError(
                f"aggregate item {stable_id} status is inconsistent"
            )
    if seen_items != set(ledger_items):
        raise InvocationCheckError("aggregate has missing or extra ledger items")
    return reported


def validate_aggregate_report(
    *, manifest: dict[str, Any], ledger: dict[str, Any], report: dict[str, Any]
) -> CheckSummary:
    """Validate exact manifest coverage and its aggregate scenario contributions."""

    expected = derive_expected_scenarios(manifest=manifest, ledger=ledger)
    aggregate_expectation = manifest["aggregate_expectation"]
    if report.get("schema") != AGGREGATE_SCHEMA:
        raise InvocationCheckError(f"aggregate must use schema {AGGREGATE_SCHEMA}")
    product_manifest = report.get("product_artifact_manifest")
    product_fields = {
        "provider",
        "workflow_run_id",
        "workflow_attempt",
        "head_sha",
        "manifest_sha256",
        "artifact_mapping_count",
    }
    expected_product_producers = {
        producer
        for producer in manifest["typed_producers"]
        if "product_binary" in ledger["producer_catalog"][producer]["required_subjects"]
    }
    if (
        not isinstance(product_manifest, dict)
        or set(product_manifest) != product_fields
        or product_manifest.get("provider") != "github-actions"
        or product_manifest.get("artifact_mapping_count")
        != len(expected_product_producers)
        or not isinstance(product_manifest.get("workflow_run_id"), str)
        or not product_manifest["workflow_run_id"]
        or not isinstance(product_manifest.get("workflow_attempt"), int)
        or isinstance(product_manifest["workflow_attempt"], bool)
        or product_manifest["workflow_attempt"] < 1
        or not isinstance(product_manifest.get("head_sha"), str)
        or not HEX_40.fullmatch(product_manifest["head_sha"])
        or not isinstance(product_manifest.get("manifest_sha256"), str)
        or not HEX_64.fullmatch(product_manifest["manifest_sha256"])
    ):
        raise InvocationCheckError(
            "qualification aggregate lacks the exact external product artifact binding"
        )
    if report.get("receipt_counts") != aggregate_expectation["receipt_counts"]:
        raise InvocationCheckError("aggregate receipt counts do not match the manifest")
    for field in (
        "rejected_receipts",
        "invalid_receipts",
        "receipt_conflicts",
        "rejected_latest_bundles",
    ):
        if report.get(field) != []:
            raise InvocationCheckError(f"aggregate {field} must be empty")

    reported = _report_scenarios(report=report, ledger=ledger)
    item_statuses = [item["status"] for item in report["items"]]
    computed_item_counts = {
        status: sum(item_status == status for item_status in item_statuses)
        for status in STATUS_PRIORITY
    }
    item_status_counts = report.get("item_status_counts")
    if (
        not isinstance(item_status_counts, dict)
        or set(item_status_counts) != set(STATUS_PRIORITY)
        or any(type(value) is not int for value in item_status_counts.values())
        or item_status_counts != computed_item_counts
    ):
        raise InvocationCheckError(
            "aggregate item_status_counts do not match recomputed item statuses"
        )
    overall_status = _status_worst(item_statuses, "aggregate items")
    if report.get("status") != overall_status:
        raise InvocationCheckError(
            "aggregate overall status does not match the worst item status"
        )
    if overall_status != aggregate_expectation["expected_status"]:
        raise InvocationCheckError("aggregate status does not match the manifest")

    errors: list[str] = []
    for key, scenario_report in reported.items():
        selected = scenario_report["selected_receipts"]
        specification = expected.get(key)
        label = ":".join(key)
        if specification is None:
            if scenario_report["status"] != "NQ":
                errors.append(f"uncovered scenario {label} must remain NQ")
            if selected:
                errors.append(f"unexpected selected receipt for {label}")
            continue
        if scenario_report["status"] != specification.outcome:
            errors.append(
                f"scenario {label} expected {specification.outcome} but got "
                f"{scenario_report['status']}"
            )
        if len(selected) != 1:
            errors.append(
                f"scenario {label} requires exactly one selected receipt, got "
                f"{len(selected)}"
            )
            continue
        receipt = selected[0]
        if not isinstance(receipt, dict):
            errors.append(f"scenario {label} selected receipt is not an object")
            continue
        if receipt.get("producer") != specification.producer:
            errors.append(
                f"scenario {label} selected receipt producer does not match manifest"
            )
        if receipt.get("outcome") != specification.outcome:
            errors.append(
                f"scenario {label} selected receipt outcome does not match manifest"
            )
    for key in set(expected) - set(reported):
        errors.append(f"missing aggregate scenario {':'.join(key)}")
    if errors:
        raise InvocationCheckError("; ".join(errors))

    pass_scenarios = sum(
        specification.outcome == "PASS" for specification in expected.values()
    )
    return CheckSummary(
        typed_scenarios=len(expected),
        pass_scenarios=pass_scenarios,
        nq_scenarios=len(expected) - pass_scenarios,
    )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check Phase 1 typed qualification aggregate coverage."
    )
    parser.add_argument("--manifest", type=Path, default=MANIFEST_PATH)
    parser.add_argument("--ledger", type=Path, default=ledger_module.LEDGER_PATH)
    parser.add_argument("--report", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        summary = validate_aggregate_report(
            manifest=_load_json(args.manifest, "invocation manifest"),
            ledger=ledger_module.load_ledger(args.ledger),
            report=_load_json(args.report, "aggregate report"),
        )
    except (ledger_module.LedgerError, InvocationCheckError) as error:
        print(f"FAIL: {error}", file=sys.stderr)
        return 2
    print(
        "PASS: Phase 1 aggregate exactly covers "
        f"{summary.typed_scenarios} typed scenarios "
        f"({summary.pass_scenarios} PASS, {summary.nq_scenarios} NQ)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
