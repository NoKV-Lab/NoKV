#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Run exact commit identity, conflict, response-loss, and replay tests."""

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


AGENT_CANONICAL = _test(
    "agent-canonical-commit-identity",
    "nokv-agent",
    "commit_identity_uses_recursively_canonical_manifest_json",
    "--test",
    "sdk_facade",
)
CLIENT_TERMINAL_REPLAY = _test(
    "client-terminal-commit-replay",
    "nokv-client",
    "workbench_workflow::tests::terminal_commit_replay_resubmits_the_durable_exact_request_without_live_path",
    "--lib",
)
CLIENT_CONFLICT = _test(
    "client-commit-conflict-exact-resubmit",
    "nokv-client",
    "workbench_workflow::tests::commit_conflict_resubmits_the_exact_dto_once",
    "--lib",
)
CLIENT_REPLAY_MISMATCH = _test(
    "client-replay-mismatch-fails-closed",
    "nokv-client",
    "workbench_workflow::tests::commit_conflict_then_replay_mismatch_stops_without_lookup_fallback",
    "--lib",
)
META_HEAD_REPLAY = _test(
    "meta-head-generation-and-response-loss",
    "nokv-meta",
    "workspace::commit::tests::begin_build_enforces_exact_head_generation_and_replays_response_loss",
    "--lib",
)
META_REPLACED_HEAD_REPLAY = _test(
    "meta-old-commit-replays-after-head-replacement",
    "nokv-meta",
    "workspace::commit::tests::completed_commit_replays_after_a_replacement_advances_the_live_head",
    "--lib",
)
IMPLICIT_PUT = _test(
    "cli-backend-implicit-put",
    "nokv",
    "backend::tests::publish_implicitly_admits_a_missing_workbench_before_provider_use",
    "--bin",
    "nokv",
)
IMPLICIT_APPEND = _test(
    "cli-backend-implicit-append",
    "nokv",
    "backend::tests::append_implicitly_admits_a_missing_workbench_before_provider_use",
    "--bin",
    "nokv",
)
IMPLICIT_COMMIT = _test(
    "cli-backend-implicit-commit",
    "nokv",
    "backend::tests::commit_recovers_identity_before_implicitly_admitting_a_missing_workbench",
    "--bin",
    "nokv",
)
CREATE_REPLAY = _test(
    "meta-workspace-create-exact-replay",
    "nokv-meta",
    "workspace::namespace::tests::visible_workspace_create_replays_and_request_mismatch_fails",
    "--lib",
)
RESTORED_CHAIN = _test(
    "meta-restored-destination-composes",
    "nokv-meta",
    "workspace::restore::tests::snapshot_restore_chains_from_a_restored_workbench",
    "--lib",
)
RESTORE_TERMINAL_REPLAY = _test(
    "client-terminal-restore-receipt-replay",
    "nokv-client",
    "workbench_workflow::tests::terminal_restore_replay_uses_receipt_without_source_or_projection",
    "--lib",
)


def _scenario(
    stable_id: str,
    *assertions: RustTestAssertion,
    nq: str | None = None,
) -> RustScenario:
    return RustScenario(
        ScenarioContract(stable_id, "commit-replay"), tuple(assertions), nq
    )


SCENARIOS = {
    "t13.commit-exact-replay": _scenario(
        "T13", AGENT_CANONICAL, CLIENT_TERMINAL_REPLAY, META_REPLACED_HEAD_REPLAY
    ),
    "t13.commit-conflict-and-head-authority": _scenario(
        "T13", CLIENT_CONFLICT, CLIENT_REPLAY_MISMATCH, META_HEAD_REPLAY
    ),
    "c04.implicit-create-race-and-replay": _scenario(
        "C04", IMPLICIT_PUT, IMPLICIT_APPEND, IMPLICIT_COMMIT, CREATE_REPLAY
    ),
    "c16.canonical-identity-conflict-replay": _scenario(
        "C16",
        AGENT_CANONICAL,
        CLIENT_CONFLICT,
        CLIENT_REPLAY_MISMATCH,
        META_REPLACED_HEAD_REPLAY,
    ),
    "c21.restored-destination-owned-provenance": _scenario(
        "C21", RESTORED_CHAIN, RESTORE_TERMINAL_REPLAY
    ),
    "l05.checkpoint-commit-replay": _scenario(
        "L05",
        nq="the old Python checkpoint helper is absent and no approved current replacement drives commit replay",
    ),
}


def main() -> int:
    return rust_main(
        producer_id="commit-replay",
        evidence_kinds=("unit", "integration"),
        scenarios=SCENARIOS,
        description="Run exact source-bound Workbench commit replay tests.",
    )


if __name__ == "__main__":
    raise SystemExit(main())
