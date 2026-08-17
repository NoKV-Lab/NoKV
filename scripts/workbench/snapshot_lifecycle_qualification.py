#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Run exact cross-package snapshot lifecycle and retention integration tests."""

from __future__ import annotations

from source_bound_producer import (
    RustScenario,
    RustTestAssertion,
    ScenarioContract,
    rust_main,
)


SERVER_LIFECYCLE = RustTestAssertion(
    assertion_id="server-snapshot-lifecycle",
    package="nokv-server",
    target_args=("--lib",),
    test_name="executor::tests::snapshot_lifecycle_uses_visible_incarnation_and_lists_terminal_states",
)
SERVER_ALIAS = RustTestAssertion(
    assertion_id="server-snapshot-alias-authority",
    package="nokv-server",
    target_args=("--lib",),
    test_name="executor::tests::snapshot_alias_point_get_uses_alias_generation_not_numeric_id_order",
)
CLIENT_RENEW = RustTestAssertion(
    assertion_id="client-snapshot-renew-alias",
    package="nokv-client",
    target_args=("--lib",),
    test_name="snapshot_workflow::tests::expired_snapshot_is_revived_with_the_original_alias_selector",
)
CLIENT_RETIRE = RustTestAssertion(
    assertion_id="client-snapshot-retire-replay",
    package="nokv-client",
    target_args=("--lib",),
    test_name="snapshot_workflow::tests::repeated_retire_returns_false_and_preserves_the_first_reason_without_a_scan",
)
META_RETENTION = RustTestAssertion(
    assertion_id="metadata-snapshot-retention-release",
    package="nokv-meta",
    target_args=("--lib",),
    test_name="workspace::snapshot::tests::mint_freezes_commit_and_retire_releases_it_once",
)
SCENARIOS = {
    "t14.snapshot-validation-and-lease": RustScenario(
        ScenarioContract("T14", "snapshot-lifecycle"),
        assertions=(SERVER_LIFECYCLE,),
    ),
    "t15.renew-id-alias-and-frozen-content": RustScenario(
        ScenarioContract("T15", "snapshot-lifecycle"),
        assertions=(CLIENT_RENEW, SERVER_ALIAS, META_RETENTION),
    ),
    "t16.retire-terminal-idempotent-replay": RustScenario(
        ScenarioContract("T16", "snapshot-lifecycle"),
        assertions=(META_RETENTION, CLIENT_RETIRE),
    ),
    "t17.snapshot-count-and-list-projection": RustScenario(
        ScenarioContract("T17", "snapshot-lifecycle"),
        assertions=(SERVER_LIFECYCLE,),
    ),
    "c17.snapshot-commit-alias-ttl-metadata-warnings": RustScenario(
        ScenarioContract("C17", "snapshot-lifecycle"),
        not_qualified_reason=(
            "The exact tests bind commit, alias, and TTL behavior, but do not "
            "exercise the complete public metadata-warning projection."
        ),
    ),
    "c18.snapshot-renew-list-retire-terminal-replay": RustScenario(
        ScenarioContract("C18", "snapshot-lifecycle"),
        assertions=(SERVER_LIFECYCLE, CLIENT_RENEW, CLIENT_RETIRE),
    ),
    "c19.frozen-renew-retire-reap-expire-foreign": RustScenario(
        ScenarioContract("C19", "snapshot-lifecycle"),
        not_qualified_reason=(
            "Reap and reopen tests do not exercise frozen reads plus foreign-root "
            "rejection in one cross-package integration boundary."
        ),
    ),
    "l05.checkpoint-snapshot-lifecycle": RustScenario(
        ScenarioContract("L05", "snapshot-lifecycle"),
        not_qualified_reason=(
            "Snapshot lifecycle tests do not yet execute the installed Python "
            "checkpoint adapter against the same live Workbench and therefore "
            "cannot qualify checkpoint-to-snapshot composition."
        ),
    ),
}


def main() -> int:
    return rust_main(
        producer_id="snapshot-lifecycle",
        evidence_kinds=("integration",),
        scenarios=SCENARIOS,
        description=__doc__ or "snapshot lifecycle qualification",
        evidence_roles=("producer-result", "qualification"),
    )


if __name__ == "__main__":
    raise SystemExit(main())
