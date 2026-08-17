#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Run exact transport-free Workbench schema, facade, and golden tests."""

from __future__ import annotations

from source_bound_producer import (
    RustScenario,
    RustTestAssertion,
    ScenarioContract,
    rust_main,
)


def _test(
    assertion_id: str,
    package: str,
    target_args: tuple[str, ...],
    test_name: str,
) -> RustTestAssertion:
    return RustTestAssertion(assertion_id, package, target_args, test_name)


AGENT_ALL = _test(
    "agent-all-tools-golden",
    "nokv-agent",
    ("--test", "sdk_facade"),
    "all_eighteen_tools_execute_against_typed_backend_primitives",
)
AGENT_CONTRACT = _test(
    "agent-exact-tool-inventory",
    "nokv-agent",
    ("--lib",),
    "tests::contract_contains_exactly_the_eighteen_workbench_tools",
)
AGENT_SCHEMA = _test(
    "agent-closed-schema-validation",
    "nokv-agent",
    ("--lib",),
    "tests::validator_rejects_missing_unknown_and_wrong_enum_fields",
)
AGENT_SCHEMA_BOUNDS = _test(
    "agent-schema-bound-validation",
    "nokv-agent",
    ("--lib",),
    "tests::validator_enforces_digest_pattern_and_snapshot_selector_exclusivity",
)
AGENT_ERROR = _test(
    "agent-stable-error-envelope",
    "nokv-agent",
    ("--lib",),
    "tests::errors_have_the_stable_agent_envelope",
)
EMPTY_SCOPE = _test(
    "agent-empty-optional-scope",
    "nokv-agent",
    ("--test", "sdk_facade"),
    "empty_optional_scope_path_is_equivalent_to_omission",
)
PATH_PUT = _test(
    "agent-path-jail-and-put-modes",
    "nokv-agent",
    ("--test", "sdk_facade"),
    "path_jail_and_put_modes_fail_closed",
)
APPEND_EDIT = _test(
    "agent-append-and-edit-conflict-semantics",
    "nokv-agent",
    ("--test", "sdk_facade"),
    "append_is_one_backend_operation_and_edit_revalidates_generation_conflicts",
)
APPEND_LIMIT = _test(
    "agent-append-delta-limit",
    "nokv-agent",
    ("--test", "sdk_facade"),
    "append_limits_the_delta_without_capping_the_existing_logical_artifact",
)
READ = _test(
    "agent-structured-and-byte-reads",
    "nokv-agent",
    ("--test", "sdk_facade"),
    "structured_and_byte_reads_preserve_explicit_cursor_semantics",
)
GREP = _test(
    "agent-grep-case-literal-and-glob",
    "nokv-agent",
    ("--test", "sdk_facade"),
    "grep_treats_patterns_as_case_insensitive_literals_and_globs_as_basenames",
)
WORKBENCH_GREP_PIPE_BOUNDS = _test(
    "agent-workbench-grep-pipe-seventeen-pattern-bound",
    "nokv-agent",
    ("--test", "sdk_facade"),
    "workbench_grep_pipe_compatibility_is_not_enabled_for_generic_grep",
)
COMMIT = _test(
    "agent-canonical-commit-identity",
    "nokv-agent",
    ("--test", "sdk_facade"),
    "commit_identity_uses_recursively_canonical_manifest_json",
)
COMMIT_CLOCK = _test(
    "agent-clock-free-commit-request",
    "nokv-agent",
    ("--test", "sdk_facade"),
    "commit_request_is_clock_free_across_handler_reconstruction",
)
SNAPSHOT_RESTORE = _test(
    "agent-snapshot-restore-projection",
    "nokv-agent",
    ("--test", "sdk_facade"),
    "snapshots_shape_annotations_and_restore_without_internal_roots",
)
ROOT_PRESENTATION = _test(
    "agent-root-is-presentation-only",
    "nokv-agent",
    ("--test", "sdk_facade"),
    "logical_workbench_root_is_presentation_not_storage_identity",
)
GENERIC_PROFILE_CONTRACT = _test(
    "agent-generic-seven-tool-contract",
    "nokv-agent",
    ("--test", "generic_profile"),
    "generic_agent_profile_preserves_the_exact_seven_tool_contract",
)
WORKBENCH_ID = _test(
    "types-workbench-id-grammar",
    "nokv-types",
    ("--lib",),
    "workspace::tests::workbench_id_enforces_frozen_ascii_contract",
)
RELATIVE_PATH = _test(
    "types-relative-path-jail",
    "nokv-types",
    ("--lib",),
    "workspace::tests::relative_path_rejects_invalid_components_without_cleanup",
)


def _scenario(
    stable_id: str,
    gate: str,
    *assertions: RustTestAssertion,
    nq: str | None = None,
) -> RustScenario:
    return RustScenario(ScenarioContract(stable_id, gate), tuple(assertions), nq)


SCENARIOS = {
    "t01.create-schema": _scenario(
        "T01", "schema-surface", AGENT_CONTRACT, AGENT_SCHEMA
    ),
    "t01.create-result": _scenario("T01", "facade-contract", AGENT_ALL),
    "t02.put-schema": _scenario("T02", "schema-surface", AGENT_SCHEMA, PATH_PUT),
    "t02.put-create-replace-contract": _scenario("T02", "facade-contract", PATH_PUT),
    "t02.put-path-native-output": _scenario("T02", "output-golden", AGENT_ALL),
    "t03.append-schema": _scenario("T03", "schema-surface", AGENT_CONTRACT, AGENT_ALL),
    "t03.append-result-contract": _scenario("T03", "facade-contract", APPEND_EDIT),
    "t03.append-delta-generation-digest-output": _scenario(
        "T03", "output-golden", APPEND_EDIT
    ),
    "t04.edit-schema": _scenario("T04", "schema-surface", AGENT_CONTRACT, AGENT_ALL),
    "t04.edit-noop-and-conflict-contract": _scenario(
        "T04", "facade-contract", APPEND_EDIT
    ),
    "t05.list-root-equivalence-contract": _scenario(
        "T05", "facade-contract", EMPTY_SCOPE
    ),
    "t06.stat-metadata-only-contract": _scenario("T06", "facade-contract", AGENT_ALL),
    "t07.get-range-format-conditional-contract": _scenario(
        "T07", "facade-contract", READ
    ),
    "t07.get-text-base64-page-output": _scenario(
        "T07", "output-golden", AGENT_ALL, READ
    ),
    "t08.grep-literal-or-glob-contract": _scenario("T08", "facade-contract", GREP),
    "t09.search-query-controls-contract": _scenario(
        "T09", "facade-contract", AGENT_ALL
    ),
    "t10.aggregate-query-controls-contract": _scenario(
        "T10", "facade-contract", AGENT_ALL
    ),
    "t10.aggregate-output": _scenario("T10", "output-golden", AGENT_ALL),
    "t11.describe-query-capabilities-contract": _scenario(
        "T11", "facade-contract", AGENT_ALL
    ),
    "t11.describe-output": _scenario("T11", "output-golden", AGENT_ALL),
    "t12.find-commit-manifest-contract": _scenario("T12", "facade-contract", AGENT_ALL),
    "t12.find-optional-manifest-output": _scenario("T12", "output-golden", AGENT_ALL),
    "t13.commit-provenance-contract": _scenario(
        "T13", "facade-contract", COMMIT, COMMIT_CLOCK
    ),
    "t13.commit-path-native-output": _scenario("T13", "output-golden", AGENT_ALL),
    "t17.snapshot-projection-output": _scenario(
        "T17", "output-golden", AGENT_ALL, SNAPSHOT_RESTORE
    ),
    "t18.restore-destination-owned-output": _scenario(
        "T18", "output-golden", AGENT_ALL, SNAPSHOT_RESTORE
    ),
    "c01.exact-18-tool-schema": _scenario(
        "C01", "schema-surface", AGENT_CONTRACT, AGENT_ALL
    ),
    "c02.closed-input-schema": _scenario(
        "C02", "schema-surface", AGENT_SCHEMA, AGENT_SCHEMA_BOUNDS
    ),
    "c02.typed-validation-error-contract": _scenario(
        "C02", "facade-contract", AGENT_ERROR, AGENT_SCHEMA
    ),
    "c02.typed-validation-error-output": _scenario(
        "C02", "output-golden", AGENT_ALL, AGENT_ERROR
    ),
    "c03.id-section-and-path-normalization-contract": _scenario(
        "C03",
        "facade-contract",
        WORKBENCH_ID,
        RELATIVE_PATH,
        PATH_PUT,
        AGENT_ALL,
        ROOT_PRESENTATION,
    ),
    "c04.implicit-workbench-create-contract": _scenario(
        "C04",
        "facade-contract",
        nq="implicit creation is implemented below the transport-free facade, but no exact nokv-agent unit oracle proves absent-id put, append, and commit",
    ),
    "c05.empty-path-equivalence-contract": _scenario(
        "C05", "facade-contract", EMPTY_SCOPE
    ),
    "c07.exclusive-payload-schema": _scenario(
        "C07", "schema-surface", PATH_PUT, AGENT_SCHEMA
    ),
    "c07.payload-decode-content-type-size-contract": _scenario(
        "C07",
        "facade-contract",
        nq="no exact facade test jointly proves invalid base64, content-type defaulting, and payload size rejection",
    ),
    "c08.create-replace-precondition-contract": _scenario(
        "C08", "facade-contract", PATH_PUT
    ),
    "c09.append-result-contract": _scenario(
        "C09", "facade-contract", APPEND_EDIT, APPEND_LIMIT
    ),
    "c10.edit-match-noop-conflict-contract": _scenario(
        "C10", "facade-contract", APPEND_EDIT
    ),
    "c12.stat-read-range-conditional-contract": _scenario(
        "C12", "facade-contract", AGENT_ALL, READ
    ),
    "c13.grep-literal-or-bounds-contract": _scenario(
        "C13",
        "facade-contract",
        GREP,
        WORKBENCH_GREP_PIPE_BOUNDS,
    ),
    "c14.query-scope-predicate-control-contract": _scenario(
        "C14", "facade-contract", AGENT_ALL, EMPTY_SCOPE
    ),
    "c14.query-defined-empty-output": _scenario("C14", "output-golden", AGENT_ALL),
    "c15.find-filter-projection-contract": _scenario(
        "C15", "facade-contract", AGENT_ALL
    ),
    "c17.snapshot-validation-contract": _scenario(
        "C17", "facade-contract", AGENT_SCHEMA_BOUNDS, SNAPSHOT_RESTORE
    ),
    "c18.snapshot-lifecycle-output": _scenario(
        "C18", "output-golden", AGENT_ALL, SNAPSHOT_RESTORE
    ),
    "l01.generic-seven-tool-profile-schema": _scenario(
        "L01",
        "schema-surface",
        GENERIC_PROFILE_CONTRACT,
    ),
}


def main() -> int:
    return rust_main(
        producer_id="nokv-agent-unit",
        evidence_kinds=("unit",),
        scenarios=SCENARIOS,
        description="Run exact source-bound nokv-agent Workbench tests.",
    )


if __name__ == "__main__":
    raise SystemExit(main())
