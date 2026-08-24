#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Validate the machine-readable pre-#423 Workbench contract ledger."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
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
ALLOWED_EVIDENCE_KINDS = frozenset({"static", "unit", "integration", "live"})
EXPECTED_GATE_REFERENCES = 137
EXPECTED_SCENARIOS = 172
QUALIFICATION_POLICY_SHA256 = (
    "396f3559cdce040d4d9f4997b331033f2c6f8753385536e2721c22102b56235c"
)
ALLOWED_PRODUCER_SUBJECTS = frozenset(
    {"product_binary", "dependencies", "rust_toolchain"}
)
LIVE_PRODUCER_SUBJECTS = frozenset({"product_binary", "dependencies"})
ALLOWED_DEPENDENCY_IDENTITY_KINDS = frozenset({"git", "oci", "sha256"})
EVIDENCE_ROLE_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._-]*$")
SOURCE_RANGE_PATTERN = re.compile(r"^[0-9]+(?:-[0-9]+)?(?:,[0-9]+(?:-[0-9]+)?)*$")
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
        "gate_expectations",
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
    gate_references: int
    scenarios: int


def load_ledger(path: Path = LEDGER_PATH) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as err:
        raise LedgerError(f"cannot load contract ledger {path}: {err}") from err
    if not isinstance(value, dict):
        raise LedgerError("contract ledger must be a JSON object")
    return value


def canonical_json(value: Any) -> bytes:
    """Encode qualification policy data for stable source-bound hashing."""

    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def json_sha256(value: Any) -> str:
    """Return the canonical JSON SHA-256 for a ledger item or expectation."""

    return hashlib.sha256(canonical_json(value)).hexdigest()


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


def _string_list(
    value: Any, field: str, item_id: str, *, allow_empty: bool = False
) -> list[str]:
    if (
        not isinstance(value, list)
        or (not allow_empty and not value)
        or any(not isinstance(element, str) or not element.strip() for element in value)
    ):
        qualifier = "a string array" if allow_empty else "a non-empty string array"
        raise LedgerError(f"{item_id}.{field} must be {qualifier}")
    if len(value) != len(set(value)):
        raise LedgerError(f"{item_id}.{field} must not contain duplicates")
    return value


def _validate_source_evidence(
    evidence: list[str], line_counts: dict[str, int], item_id: str
) -> None:
    for citation in evidence:
        if not citation.startswith("pre:"):
            continue
        location = citation.removeprefix("pre:")
        path = location
        ranges: str | None = None
        if ":" in location:
            candidate_path, candidate_ranges = location.rsplit(":", 1)
            if SOURCE_RANGE_PATTERN.fullmatch(candidate_ranges):
                path, ranges = candidate_path, candidate_ranges
        if path not in line_counts:
            raise LedgerError(
                f"{item_id}.source_evidence references unrecorded pre-revision "
                f"path {path!r}"
            )
        if ranges is None:
            continue
        maximum = line_counts[path]
        for source_range in ranges.split(","):
            if "-" in source_range:
                start_text, end_text = source_range.split("-", 1)
            else:
                start_text = end_text = source_range
            start, end = int(start_text), int(end_text)
            if start < 1 or end < start:
                raise LedgerError(
                    f"{item_id}.source_evidence has invalid range {source_range!r}"
                )
            if end > maximum:
                raise LedgerError(
                    f"{item_id}.source_evidence range {source_range} exceeds "
                    f"pre-revision {path} length {maximum}"
                )


def _find_item(ledger: dict[str, Any], item_id: str) -> dict[str, Any]:
    items = ledger.get("items")
    if not isinstance(items, list):
        raise LedgerError("items must be an array")
    matching = [
        item for item in items if isinstance(item, dict) and item.get("id") == item_id
    ]
    if len(matching) != 1:
        raise LedgerError(f"unknown or duplicate stable id {item_id}")
    return matching[0]


def resolve_gate_expectation(
    ledger: dict[str, Any], item_id: str, gate: str
) -> dict[str, Any]:
    """Resolve an item's profile-backed gate expectation into a hashable object."""

    item = _find_item(ledger, item_id)
    expectations = item.get("gate_expectations")
    if not isinstance(expectations, dict) or gate not in expectations:
        raise LedgerError(f"{item_id} has no qualification expectation for {gate}")
    raw = expectations[gate]
    if not isinstance(raw, dict):
        raise LedgerError(f"{item_id}.gate_expectations.{gate} must be an object")
    profile_id = raw.get("profile")
    profiles = ledger.get("expectation_profiles")
    if not isinstance(profiles, dict) or profile_id not in profiles:
        raise LedgerError(
            f"{item_id}.gate_expectations.{gate} references unknown profile "
            f"{profile_id!r}"
        )
    profile = profiles[profile_id]
    if not isinstance(profile, dict):
        raise LedgerError(f"expectation profile {profile_id} must be an object")
    return {
        "profile": profile_id,
        "gate": gate,
        "scenarios": list(raw.get("scenarios", [])),
        "allowed_evidence_kinds": list(profile.get("allowed_evidence_kinds", [])),
        "allowed_producers": list(profile.get("allowed_producers", [])),
    }


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

    producer_catalog = ledger.get("producer_catalog")
    if not isinstance(producer_catalog, dict) or not producer_catalog:
        raise LedgerError("producer_catalog must be a non-empty object")
    for producer_id, producer in producer_catalog.items():
        _nonempty_string(producer_id, "producer id", "producer_catalog")
        producer_field = f"producer_catalog.{producer_id}"
        if not isinstance(producer, dict):
            raise LedgerError(f"{producer_field} must be an object")
        expected_producer_keys = {
            "description",
            "evidence_kinds",
            "command",
            "required_evidence_roles",
            "required_subjects",
            "required_dependencies",
        }
        if set(producer) != expected_producer_keys:
            raise LedgerError(
                f"{producer_field} keys must be exactly "
                f"{sorted(expected_producer_keys)}"
            )
        _nonempty_string(producer.get("description"), "description", producer_field)
        producer_evidence_kinds = _nonempty_string_list(
            producer.get("evidence_kinds"), "evidence_kinds", producer_field
        )
        unknown_producer_kinds = set(producer_evidence_kinds) - ALLOWED_EVIDENCE_KINDS
        if unknown_producer_kinds:
            raise LedgerError(
                f"{producer_field} references unknown evidence kinds "
                f"{sorted(unknown_producer_kinds)}"
            )
        command = producer.get("command")
        if not isinstance(command, dict):
            raise LedgerError(f"{producer_field}.command must be an object")
        expected_command_keys = {
            "kind",
            "entrypoint",
            "result_argument",
            "binary_argument",
            "forbidden_arguments",
        }
        if set(command) != expected_command_keys:
            raise LedgerError(
                f"{producer_field}.command keys must be exactly "
                f"{sorted(expected_command_keys)}"
            )
        if command.get("kind") != "python-script":
            raise LedgerError(f"{producer_field}.command.kind must be python-script")
        entrypoint = command.get("entrypoint")
        _nonempty_string(entrypoint, "entrypoint", f"{producer_field}.command")
        entrypoint_path = Path(entrypoint)
        if (
            entrypoint_path.is_absolute()
            or ".." in entrypoint_path.parts
            or not entrypoint.startswith("scripts/workbench/")
            or entrypoint_path.suffix != ".py"
        ):
            raise LedgerError(
                f"{producer_field}.command.entrypoint must be a source-bound "
                "scripts/workbench Python path"
            )
        result_argument = command.get("result_argument")
        _nonempty_string(
            result_argument, "result_argument", f"{producer_field}.command"
        )
        if not result_argument.startswith("--"):
            raise LedgerError(
                f"{producer_field}.command.result_argument must be an option"
            )
        binary_argument = command.get("binary_argument")
        if binary_argument is not None and (
            not isinstance(binary_argument, str)
            or not binary_argument.startswith("--")
            or binary_argument == result_argument
        ):
            raise LedgerError(
                f"{producer_field}.command.binary_argument must be null or a "
                "distinct command option"
            )
        forbidden = _nonempty_string_list(
            command.get("forbidden_arguments"),
            "forbidden_arguments",
            f"{producer_field}.command",
        )
        if "--dry-run" not in forbidden:
            raise LedgerError(
                f"{producer_field}.command must forbid --dry-run qualification"
            )
        roles = _nonempty_string_list(
            producer.get("required_evidence_roles"),
            "required_evidence_roles",
            producer_field,
        )
        if "producer-result" not in roles or any(
            not EVIDENCE_ROLE_PATTERN.fullmatch(role) for role in roles
        ):
            raise LedgerError(
                f"{producer_field}.required_evidence_roles must contain valid "
                "producer-result role"
            )
        subjects = _string_list(
            producer.get("required_subjects"),
            "required_subjects",
            producer_field,
            allow_empty=True,
        )
        unknown_subjects = set(subjects) - ALLOWED_PRODUCER_SUBJECTS
        if unknown_subjects:
            raise LedgerError(
                f"{producer_field} references unknown required subjects "
                f"{sorted(unknown_subjects)}"
            )
        if "live" in producer_evidence_kinds and set(subjects) != (
            LIVE_PRODUCER_SUBJECTS
        ):
            raise LedgerError(
                f"live producer {producer_id} must require product_binary and "
                "dependencies subjects"
            )
        if "rust_toolchain" in subjects and "live" in producer_evidence_kinds:
            raise LedgerError(
                f"live producer {producer_id} must bind product runtime subjects, "
                "not the qualification host Rust toolchain"
            )
        required_dependencies = producer.get("required_dependencies")
        if not isinstance(required_dependencies, dict):
            raise LedgerError(
                f"{producer_field}.required_dependencies must be an object"
            )
        for dependency_name, identity_kinds in required_dependencies.items():
            _nonempty_string(
                dependency_name,
                "dependency name",
                f"{producer_field}.required_dependencies",
            )
            kinds = _nonempty_string_list(
                identity_kinds,
                "dependency identity kinds",
                f"{producer_field}.required_dependencies.{dependency_name}",
            )
            unknown_identity_kinds = set(kinds) - ALLOWED_DEPENDENCY_IDENTITY_KINDS
            if unknown_identity_kinds:
                raise LedgerError(
                    f"{producer_field}.required_dependencies.{dependency_name} "
                    "references unknown dependency identity kinds "
                    f"{sorted(unknown_identity_kinds)}"
                )
        if "live" in producer_evidence_kinds:
            if binary_argument is None:
                raise LedgerError(
                    f"live producer {producer_id} requires command.binary_argument"
                )
            if not required_dependencies:
                raise LedgerError(
                    f"live producer {producer_id} requires pinned dependencies"
                )
        elif binary_argument is not None or required_dependencies:
            raise LedgerError(
                f"non-live producer {producer_id} cannot claim binary or dependency "
                "subjects"
            )

    pre_source_line_counts = ledger.get("pre_source_line_counts")
    if not isinstance(pre_source_line_counts, dict) or not pre_source_line_counts:
        raise LedgerError("pre_source_line_counts must be a non-empty object")
    for source_path, line_count in pre_source_line_counts.items():
        _nonempty_string(source_path, "source path", "pre_source_line_counts")
        if (
            not isinstance(line_count, int)
            or isinstance(line_count, bool)
            or line_count < 1
        ):
            raise LedgerError(
                f"pre_source_line_counts.{source_path} must be a positive integer"
            )

    expectation_profiles = ledger.get("expectation_profiles")
    if not isinstance(expectation_profiles, dict) or not expectation_profiles:
        raise LedgerError("expectation_profiles must be a non-empty object")
    profiled_gates: set[str] = set()
    for profile_id, profile in expectation_profiles.items():
        _nonempty_string(profile_id, "profile id", "expectation_profiles")
        if not isinstance(profile, dict):
            raise LedgerError(f"expectation profile {profile_id} must be an object")
        profile_gate = profile.get("gate")
        _nonempty_string(profile_gate, "gate", f"expectation_profiles.{profile_id}")
        if profile_gate not in gate_catalog:
            raise LedgerError(
                f"expectation profile {profile_id} references unknown gate "
                f"{profile_gate!r}"
            )
        profiled_gates.add(profile_gate)
        evidence_kinds = _nonempty_string_list(
            profile.get("allowed_evidence_kinds"),
            "allowed_evidence_kinds",
            f"expectation_profiles.{profile_id}",
        )
        unknown_kinds = set(evidence_kinds) - ALLOWED_EVIDENCE_KINDS
        if unknown_kinds:
            raise LedgerError(
                f"expectation profile {profile_id} references unknown evidence kinds "
                f"{sorted(unknown_kinds)}"
            )
        producers = _nonempty_string_list(
            profile.get("allowed_producers"),
            "allowed_producers",
            f"expectation_profiles.{profile_id}",
        )
        unknown_producers = set(producers) - set(producer_catalog)
        if unknown_producers:
            raise LedgerError(
                f"expectation profile {profile_id} references unknown producers "
                f"{sorted(unknown_producers)}"
            )
        kinds_without_producer = {
            evidence_kind
            for evidence_kind in evidence_kinds
            if not any(
                evidence_kind in producer_catalog[producer_id]["evidence_kinds"]
                for producer_id in producers
            )
        }
        producers_without_kind = {
            producer_id
            for producer_id in producers
            if not set(producer_catalog[producer_id]["evidence_kinds"]).intersection(
                evidence_kinds
            )
        }
        if kinds_without_producer or producers_without_kind:
            raise LedgerError(
                f"expectation profile {profile_id} has incompatible producer and "
                f"evidence kinds; kinds_without_producer={sorted(kinds_without_producer)}, "
                f"producers_without_kind={sorted(producers_without_kind)}"
            )
    if profiled_gates != set(gate_catalog):
        raise LedgerError(
            "expectation profiles must cover every catalogued gate; "
            f"missing={sorted(set(gate_catalog) - profiled_gates)}, "
            f"extra={sorted(profiled_gates - set(gate_catalog))}"
        )

    items = ledger.get("items")
    if not isinstance(items, list):
        raise LedgerError("items must be an array")
    if len(items) != 47:
        raise LedgerError(f"items must contain exactly 47 entries, got {len(items)}")
    candidate_ids = [item.get("id") for item in items if isinstance(item, dict)]
    duplicate_ids = sorted(
        {
            item_id
            for item_id in candidate_ids
            if isinstance(item_id, str) and candidate_ids.count(item_id) > 1
        }
    )
    if duplicate_ids:
        raise LedgerError(f"contract ledger contains duplicate ids {duplicate_ids}")

    ids: list[str] = []
    scopes: list[str] = []
    classes: list[str] = []
    dispositions: list[str] = []
    all_scenarios: list[str] = []
    gate_reference_count = 0
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
        _validate_source_evidence(evidence, pre_source_line_counts, item_id)
        required_gates = _nonempty_string_list(
            item.get("required_gates"), "required_gates", item_id
        )
        unknown_gates = set(required_gates) - set(gate_catalog)
        if unknown_gates:
            raise LedgerError(
                f"{item_id}.required_gates references unknown gates "
                f"{sorted(unknown_gates)}"
            )
        gate_reference_count += len(required_gates)

        expectations = item.get("gate_expectations")
        if not isinstance(expectations, dict):
            raise LedgerError(f"{item_id}.gate_expectations must be an object")
        if set(expectations) != set(required_gates):
            raise LedgerError(
                f"{item_id}.gate_expectations keys must exactly match required_gates; "
                f"missing={sorted(set(required_gates) - set(expectations))}, "
                f"extra={sorted(set(expectations) - set(required_gates))}"
            )
        for gate in required_gates:
            raw_expectation = expectations[gate]
            if not isinstance(raw_expectation, dict):
                raise LedgerError(
                    f"{item_id}.gate_expectations.{gate} must be an object"
                )
            profile_id = raw_expectation.get("profile")
            _nonempty_string(
                profile_id, "profile", f"{item_id}.gate_expectations.{gate}"
            )
            if profile_id not in expectation_profiles:
                raise LedgerError(
                    f"{item_id}.gate_expectations.{gate} references unknown profile "
                    f"{profile_id!r}"
                )
            if expectation_profiles[profile_id]["gate"] != gate:
                raise LedgerError(
                    f"{item_id}.gate_expectations.{gate} profile {profile_id} "
                    f"belongs to gate {expectation_profiles[profile_id]['gate']}"
                )
            scenarios = _nonempty_string_list(
                raw_expectation.get("scenarios"),
                "scenarios",
                f"{item_id}.gate_expectations.{gate}",
            )
            expected_prefix = f"{item_id.lower()}."
            if any(not scenario.startswith(expected_prefix) for scenario in scenarios):
                raise LedgerError(
                    f"{item_id}.gate_expectations.{gate}.scenarios must use "
                    f"item-specific prefix {expected_prefix}"
                )
            all_scenarios.extend(scenarios)

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

    if gate_reference_count != EXPECTED_GATE_REFERENCES:
        raise LedgerError(
            "contract ledger must preserve exactly "
            f"{EXPECTED_GATE_REFERENCES} required gate references, got "
            f"{gate_reference_count}"
        )
    if len(all_scenarios) != len(set(all_scenarios)):
        duplicates = sorted(
            {
                scenario
                for scenario in all_scenarios
                if all_scenarios.count(scenario) > 1
            }
        )
        raise LedgerError(
            f"qualification scenarios must be globally item-specific; duplicates={duplicates}"
        )
    if len(all_scenarios) != EXPECTED_SCENARIOS:
        raise LedgerError(
            f"contract ledger must preserve exactly {EXPECTED_SCENARIOS} scenarios, "
            f"got {len(all_scenarios)}"
        )

    cited_paths = {
        citation.removeprefix("pre:").rsplit(":", 1)[0]
        if ":" in citation.removeprefix("pre:")
        and SOURCE_RANGE_PATTERN.fullmatch(
            citation.removeprefix("pre:").rsplit(":", 1)[1]
        )
        else citation.removeprefix("pre:")
        for item in items
        for citation in item["source_evidence"]
        if citation.startswith("pre:")
    }
    if cited_paths != set(pre_source_line_counts):
        raise LedgerError(
            "pre_source_line_counts must exactly cover cited pre-revision paths; "
            f"missing={sorted(cited_paths - set(pre_source_line_counts))}, "
            f"extra={sorted(set(pre_source_line_counts) - cited_paths)}"
        )

    core_count = scopes.count("core")
    legacy_count = scopes.count("legacy")
    if core_count != 39 or legacy_count != 8:
        raise LedgerError(
            "contract ledger must contain exactly 39 core and 8 legacy entries; "
            f"got core={core_count}, legacy={legacy_count}"
        )

    by_id = {item["id"]: item for item in items}
    canonical_sections = ("input", "scripts", "outputs", "logs", "metadata")
    for item_id in ("T01", "C03"):
        requirement = by_id[item_id]["requirement"]
        if "five virtual sections" not in requirement or any(
            section not in requirement for section in canonical_sections
        ):
            raise LedgerError(
                f"{item_id} must preserve the canonical five Workbench sections"
            )
    fuse_boundary = by_id["L07"]
    if (
        fuse_boundary["class"] != "D"
        or fuse_boundary["current_disposition"] != "do-not-restore"
        or "api-absence" not in fuse_boundary["required_gates"]
    ):
        raise LedgerError(
            "L07 FUSE/POSIX must remain class D, do-not-restore, and API-absence gated"
        )

    generic_agent = by_id["L01"]
    generic_agent_expectations = generic_agent["gate_expectations"]
    if (
        generic_agent["current_disposition"] != "restore"
        or set(generic_agent["required_gates"])
        != {"api-decision", "schema-surface", "native-workbench-e2e"}
        or generic_agent_expectations["schema-surface"]["scenarios"]
        != ["l01.generic-seven-tool-profile-schema"]
        or generic_agent_expectations["native-workbench-e2e"]["scenarios"]
        != ["l01.generic-seven-tool-profile-live"]
    ):
        raise LedgerError(
            "L01 generic Agent MCP profile must be restored and schema/live gated"
        )

    operational_scenarios = {
        "l08.single-node-no-external-etcd-live",
        "l08.http-health-readiness-stats-live",
        "l08.metadata-backup-fresh-node-restore-live",
        "l08.metadata-fsck-clean-and-corruption-detection-live",
        "l08.manual-reconciliable-gc-live",
        "l08.checkpoint-log-replay-live",
        "l08.cross-owner-failover-live",
        "l08.multishard-routing-failover-live",
        "l08.v010-upgrade-recovery-matrix-live",
    }
    operations = by_id["L08"]
    if (
        operations["current_disposition"] != "replace"
        or set(operations["gate_expectations"]["provider-recovery"]["scenarios"])
        != operational_scenarios
    ):
        raise LedgerError(
            "L08 operational recovery scenarios are not the closed oracle"
        )

    python_compatibility_scenarios = {
        "L03": {
            "l03.python-create-publish-read-stat-list-rename-remove-live",
            "l03.python-replace-generation-and-frozen-snapshot-live",
            "l03.python-range-batch-ordering-and-bounds-live",
            "l03.python-retry-concurrency-and-query-live",
        },
        "L04": {
            "l04.fsspec-open-mode-policy-live",
            "l04.fsspec-virtual-prefix-namespace-live",
            "l04.fsspec-move-remove-live",
            "l04.fsspec-write-read-range-live",
            "l04.fsspec-create-only-and-replace-live",
            "l04.fsspec-retry-and-concurrency-live",
        },
        "L05": {
            "l05.checkpoint-publish-resolve-load-exact-live",
            "l05.checkpoint-highest-committed-step-live",
            "l05.checkpoint-partial-step-invisible-live",
            "l05.checkpoint-distributed-shard-commit-live",
            "l05.checkpoint-invalid-shard-name-rejected-live",
        },
        "L06": {
            "l06.dcp-rank-semantics-live",
            "l06.dcp-rank-failure-propagation-live",
            "l06.dcp-short-range-batch-fails-live",
            "l06.dcp-multirank-exact-roundtrip-live",
        },
    }
    for item_id, expected_scenarios in python_compatibility_scenarios.items():
        item = by_id[item_id]
        if item["current_disposition"] != "replace":
            raise LedgerError(
                f"{item_id} Python compatibility behavior must be replaced, not retired"
            )
        if "python-sdk-e2e" not in item["required_gates"]:
            raise LedgerError(
                f"{item_id} Python compatibility behavior requires python-sdk-e2e"
            )
        actual_scenarios = set(item["gate_expectations"]["python-sdk-e2e"]["scenarios"])
        if actual_scenarios != expected_scenarios:
            raise LedgerError(
                f"{item_id} Python compatibility scenarios are not the closed oracle; "
                f"missing={sorted(expected_scenarios - actual_scenarios)}, "
                f"extra={sorted(actual_scenarios - expected_scenarios)}"
            )

    excluded = [
        item["id"]
        for item in items
        if item["current_disposition"] in {"retire", "do-not-restore"}
    ]
    if excluded != ["L07"]:
        raise LedgerError(
            "only L07 FUSE/POSIX may be retired or excluded from this recovery ledger; "
            f"got {excluded}"
        )

    actual_policy_digest = json_sha256(ledger)
    if actual_policy_digest != QUALIFICATION_POLICY_SHA256:
        raise LedgerError(
            "qualification policy digest differs from the reviewed golden; "
            f"expected={QUALIFICATION_POLICY_SHA256} actual={actual_policy_digest}"
        )

    return LedgerSummary(
        items=len(items),
        core=core_count,
        legacy=legacy_count,
        classes={value: classes.count(value) for value in sorted(ALLOWED_CLASSES)},
        dispositions={
            value: dispositions.count(value) for value in sorted(ALLOWED_DISPOSITIONS)
        },
        gate_references=gate_reference_count,
        scenarios=len(all_scenarios),
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
        f"dispositions={json.dumps(summary.dispositions, sort_keys=True)} "
        f"gate_references={summary.gate_references} scenarios={summary.scenarios}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
