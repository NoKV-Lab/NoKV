#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Run exact opaque cursor, scope, staleness, and casefold tests."""

from source_bound_producer import (
    RustScenario,
    RustTestAssertion,
    ScenarioContract,
    rust_main,
)


def _test(
    assertion_id: str, package: str, test_name: str, *target: str
) -> RustTestAssertion:
    return RustTestAssertion(assertion_id, package, tuple(target), test_name)


LIST_SCOPE = _test(
    "cli-list-cursor-scope-and-v4-schema",
    "nokv",
    "backend::tests::list_cursor_is_bound_to_workbench_prefix_view_and_breaks_the_old_schema",
    "--bin",
    "nokv",
)
LIST_ROOT_SCOPE = _test(
    "cli-public-cursor-root-binding",
    "nokv",
    "backend::tests::public_cursor_scopes_are_bound_to_the_storage_root",
    "--bin",
    "nokv",
)
LIST_STALE = _test(
    "cli-list-staleness-fails-closed",
    "nokv",
    "backend::tests::user_list_cursor_staleness_fails_without_an_automatic_restart",
    "--bin",
    "nokv",
)
LIST_PROGRESS = _test(
    "server-live-list-progress-and-revision-fence",
    "nokv-server",
    "executor::tests::live_list_workspace_fence_ignores_unrelated_writes_but_rejects_target_changes",
    "--lib",
)
GREP_CASE = _test(
    "agent-grep-case-literal-glob",
    "nokv-agent",
    "grep_treats_patterns_as_case_insensitive_literals_and_globs_as_basenames",
    "--test",
    "sdk_facade",
)
GREP_SCOPE = _test(
    "cli-grep-cursor-scope-and-revision",
    "nokv",
    "backend::tests::grep_cursor_is_bound_to_workbench_prefix_recursion_and_workspace_revision",
    "--bin",
    "nokv",
)
GREP_PROGRESS = _test(
    "cli-grep-page-two-after-unrelated-write",
    "nokv",
    "backend::tests::grep_page_two_survives_an_unrelated_read_version_advance",
    "--bin",
    "nokv",
)
SEARCH_SCOPE = _test(
    "meta-search-stable-order-and-query-binding",
    "nokv-meta",
    "workspace::query::tests::suffix_is_strictly_typed_and_pages_in_stable_sort_order",
    "--lib",
)
SEARCH_STALE = _test(
    "meta-search-cross-version-rejection",
    "nokv-meta",
    "workspace::query::tests::historical_context_freezes_rows_and_rejects_cross_version_cursor",
    "--lib",
)
FIND_CASE = _test(
    "cli-find-ascii-casefold",
    "nokv",
    "backend::tests::manifest_literal_matching_is_ascii_case_insensitive_for_nested_json",
    "--bin",
    "nokv",
)
FIND_CURSOR = _test(
    "cli-find-casefold-independent-of-cursor",
    "nokv",
    "backend::tests::manifest_literal_matching_is_independent_of_projection_and_page_cursor",
    "--bin",
    "nokv",
)
CATALOG_PAGES = _test(
    "meta-catalog-stable-pages",
    "nokv-meta",
    "workspace::query::tests::catalog_advertises_only_executable_operator_sets_and_pages_stably",
    "--lib",
)


def _scenario(
    stable_id: str,
    *assertions: RustTestAssertion,
    nq: str | None = None,
) -> RustScenario:
    return RustScenario(
        ScenarioContract(stable_id, "cursor-differential"), tuple(assertions), nq
    )


SCENARIOS = {
    "t05.list-cursor-progress-and-scope": _scenario(
        "T05", LIST_SCOPE, LIST_ROOT_SCOPE, LIST_STALE, LIST_PROGRESS
    ),
    "t07.get-page-progress-and-scope": _scenario(
        "T07",
        nq="structured and byte read cursors have a progress oracle, but no exact test binds a cursor to the selected artifact and Workbench scope",
    ),
    "t08.grep-case-insensitive-pagination": _scenario(
        "T08", GREP_CASE, GREP_SCOPE, GREP_PROGRESS
    ),
    "t09.search-scope-bound-cursor": _scenario("T09", SEARCH_SCOPE, SEARCH_STALE),
    "t12.find-case-insensitive-pagination": _scenario("T12", FIND_CASE, FIND_CURSOR),
    "c11.all-pageable-progress-scope-and-stale-rejection": _scenario(
        "C11",
        LIST_SCOPE,
        LIST_ROOT_SCOPE,
        LIST_STALE,
        LIST_PROGRESS,
        GREP_SCOPE,
        GREP_PROGRESS,
        SEARCH_SCOPE,
        SEARCH_STALE,
        FIND_CURSOR,
        CATALOG_PAGES,
    ),
    "c13.grep-case-glob-cursor": _scenario("C13", GREP_CASE, GREP_SCOPE, GREP_PROGRESS),
    "c15.find-case-insensitive-pagination": _scenario("C15", FIND_CASE, FIND_CURSOR),
}


def main() -> int:
    return rust_main(
        producer_id="cursor-differential",
        evidence_kinds=("unit", "integration"),
        scenarios=SCENARIOS,
        description="Run exact source-bound Workbench cursor differential tests.",
    )


if __name__ == "__main__":
    raise SystemExit(main())
