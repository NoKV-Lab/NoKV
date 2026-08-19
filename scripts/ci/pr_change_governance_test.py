#!/usr/bin/env python3
"""Tests for the pull request change-governance policy."""

from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from typing import Any


sys.path.insert(0, str(Path(__file__).resolve().parent))

from pr_change_governance import (  # noqa: E402
    GitHubApi,
    GovernanceInputError,
    evaluate_policy,
    review_trigger_summary,
)


HEAD = "a" * 40
OLD_HEAD = "b" * 40
CORE_ONE = "wchwawa"
CORE_TWO = "feichai0017"
REPO = Path(__file__).resolve().parents[2]
CODEOWNERS_PATH = Path(
    os.environ.get("NOKV_CODEOWNERS_PATH", REPO / ".github/CODEOWNERS")
)
WORKFLOW_PATH = Path(
    os.environ.get(
        "NOKV_CHANGE_GOVERNANCE_WORKFLOW_PATH",
        REPO / ".github/workflows/change-governance.yml",
    )
)


def pull_request(
    *,
    additions: int = 0,
    deletions: int = 0,
    changed_files: int = 1,
    author: str = "author",
) -> dict[str, Any]:
    return {
        "additions": additions,
        "deletions": deletions,
        "changed_files": changed_files,
        "head": {"sha": HEAD},
        "user": {"login": author, "type": "User"},
    }


def review(
    login: str,
    *,
    state: str = "APPROVED",
    commit_id: str = HEAD,
    reviewer_type: str = "User",
    review_id: int = 1,
) -> dict[str, Any]:
    return {
        "id": review_id,
        "submitted_at": f"2026-08-16T00:00:{review_id:02d}Z",
        "state": state,
        "commit_id": commit_id,
        "user": {"login": login, "type": reviewer_type},
    }


def workflow_run(
    login: str, *, created_at: str, run_id: int, head_sha: str = HEAD,
    pull_number: int | None = 17, head_repository_id: int = 101,
    head_branch: str = "feature",
) -> dict[str, Any]:
    return {
        "actor": {"login": login},
        "created_at": created_at,
        "head_branch": head_branch,
        "head_repository": {"id": head_repository_id},
        "head_sha": head_sha,
        "id": run_id,
        "pull_requests": [] if pull_number is None else [{"number": pull_number}],
    }


class FakeGitHubApi(GitHubApi):
    def __init__(self, payload: dict[str, Any]) -> None:
        super().__init__("https://api.github.invalid", "test-token")
        self.payload = payload

    def get_json(self, path: str) -> Any:
        del path
        return self.payload


class ChangeGovernancePolicyTest(unittest.TestCase):
    def test_codeowners_protects_only_ci_trust_roots(self) -> None:
        lines = {
            line.strip()
            for line in CODEOWNERS_PATH.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        }
        self.assertNotIn("* @wchwawa @feichai0017", lines)
        self.assertEqual(
            lines,
            {
                "/.github/CODEOWNERS @wchwawa @feichai0017",
                "/.github/actions/ @wchwawa @feichai0017",
                "/.github/workflows/ @wchwawa @feichai0017",
                "/scripts/ci/ @wchwawa @feichai0017",
                "/scripts/workbench/ @wchwawa @feichai0017",
                "/scripts/release/test_homebrew_source_release.py @wchwawa @feichai0017",
                "/scripts/release/test_python_sdk_release.py @wchwawa @feichai0017",
            },
        )

    def test_base_owned_workflow_fetches_codeowners_before_policy_tests(self) -> None:
        workflow = WORKFLOW_PATH.read_text(encoding="utf-8")
        self.assertIn(".github/CODEOWNERS", workflow)
        self.assertIn("NOKV_CODEOWNERS_PATH", workflow)

    def test_workflow_avoids_redundant_review_request_events(self) -> None:
        workflow = WORKFLOW_PATH.read_text(encoding="utf-8")

        self.assertIn("      - synchronize", workflow)
        self.assertIn("  pull_request_review:", workflow)
        self.assertIn("    types: [submitted, edited, dismissed]", workflow)
        self.assertNotIn("      - review_requested", workflow)
        self.assertNotIn("      - review_request_removed", workflow)
        self.assertIn("    name: governed-change-review", workflow)
        self.assertIn(
            "STATUS_CONTEXT: change-governance/large-change-review", workflow
        )

    def test_exact_threshold_does_not_trigger_large_change_rule(self) -> None:
        decision = evaluate_policy(pull_request(additions=3_000, deletions=2_000), [])

        self.assertFalse(decision.is_large_change)
        self.assertTrue(decision.allowed)
        self.assertEqual(decision.required_approvals, 0)

    def test_small_ordinary_change_keeps_fast_path_without_review(self) -> None:
        decision = evaluate_policy(
            pull_request(additions=10), [], changed_paths=("crates/nokv/src/main.rs",)
        )

        self.assertFalse(decision.is_large_change)
        self.assertFalse(decision.is_governance_sensitive)
        self.assertEqual(decision.required_approvals, 0)
        self.assertTrue(decision.allowed)

    def test_small_governance_change_requires_non_pusher_core_review(self) -> None:
        decision = evaluate_policy(
            pull_request(additions=10),
            [review(CORE_TWO)],
            head_introducer_logins=("contributor",),
            changed_paths=(".github/workflows/rust.yml",),
        )

        self.assertFalse(decision.is_large_change)
        self.assertTrue(decision.is_governance_sensitive)
        self.assertEqual(decision.required_approvals, 1)
        self.assertEqual(decision.current_approval_logins, (CORE_TWO,))
        self.assertTrue(decision.allowed)

    def test_review_trigger_summary_names_the_independent_policy_reasons(self) -> None:
        ordinary = evaluate_policy(
            pull_request(additions=10),
            [],
            changed_paths=("crates/nokv/src/main.rs",),
        )
        large = evaluate_policy(
            pull_request(additions=5_001),
            [],
            head_introducer_logins=("contributor",),
        )
        sensitive = evaluate_policy(
            pull_request(additions=10),
            [],
            head_introducer_logins=("contributor",),
            changed_paths=(".github/workflows/rust.yml",),
        )
        both = evaluate_policy(
            pull_request(additions=5_001),
            [],
            head_introducer_logins=("contributor",),
            changed_paths=(".github/workflows/rust.yml",),
        )

        self.assertEqual(review_trigger_summary(ordinary), "none")
        self.assertEqual(
            review_trigger_summary(large), "large change (5,001 > 5,000 lines)"
        )
        self.assertEqual(
            review_trigger_summary(sensitive),
            "protected CI trust-root change (1 path)",
        )
        self.assertEqual(
            review_trigger_summary(both),
            "large change (5,001 > 5,000 lines) and "
            "protected CI trust-root change (1 path)",
        )

    def test_sensitive_workbench_gate_change_rejects_pusher_approval(self) -> None:
        decision = evaluate_policy(
            pull_request(additions=10),
            [review(CORE_ONE)],
            head_introducer_logins=(CORE_ONE,),
            changed_paths=("scripts/workbench/live_workbench.py",),
        )

        self.assertTrue(decision.is_governance_sensitive)
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.current_approval_logins, ())

    def test_all_trust_root_paths_are_sensitive_but_docs_are_not(self) -> None:
        sensitive = (
            ".github/CODEOWNERS",
            ".github/actions/verify/action.yml",
            ".github/workflows/rust.yml",
            "scripts/ci/pr_change_governance.py",
            "scripts/workbench/pre423_contract_ledger.json",
            "scripts/release/test_homebrew_source_release.py",
            "scripts/release/test_python_sdk_release.py",
        )
        for path in sensitive:
            with self.subTest(path=path):
                decision = evaluate_policy(
                    pull_request(additions=1),
                    [review(CORE_TWO)],
                    head_introducer_logins=("contributor",),
                    changed_paths=(path,),
                )
                self.assertTrue(decision.is_governance_sensitive)
        decision = evaluate_policy(
            pull_request(additions=1),
            [],
            changed_paths=("docs/development/code_contract.md",),
        )
        self.assertFalse(decision.is_governance_sensitive)

    def test_one_line_over_threshold_requires_one_core_approval(self) -> None:
        decision = evaluate_policy(
            pull_request(additions=5_001), [], head_introducer_logins=("contributor",)
        )

        self.assertTrue(decision.is_large_change)
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.required_approvals, 1)

    def test_current_core_maintainer_approval_passes(self) -> None:
        decision = evaluate_policy(
            pull_request(additions=3_000, deletions=2_001),
            [review(CORE_ONE, review_id=1)],
            head_introducer_logins=("contributor",),
        )

        self.assertTrue(decision.allowed)
        self.assertEqual(decision.current_approval_logins, (CORE_ONE,))

    def test_duplicate_approvals_count_once(self) -> None:
        decision = evaluate_policy(
            pull_request(additions=5_001),
            [review(CORE_ONE, review_id=1), review(CORE_ONE.upper(), review_id=2)],
            head_introducer_logins=("contributor",),
        )

        self.assertTrue(decision.allowed)
        self.assertEqual(decision.current_approval_logins, (CORE_ONE.upper(),))

    def test_author_bots_and_non_core_reviewers_do_not_count(self) -> None:
        decision = evaluate_policy(
            pull_request(additions=5_001, author=CORE_ONE),
            [
                review(CORE_ONE, review_id=1),
                review("automation[bot]", reviewer_type="Bot", review_id=2),
                review("non-core-human", review_id=3),
            ],
            head_introducer_logins=(CORE_ONE,),
        )

        self.assertFalse(decision.allowed)
        self.assertEqual(decision.current_approval_logins, ())

    def test_approval_on_an_old_head_does_not_count(self) -> None:
        decision = evaluate_policy(
            pull_request(additions=5_001),
            [review(CORE_ONE, commit_id=OLD_HEAD, review_id=1)],
            head_introducer_logins=("contributor",),
        )

        self.assertFalse(decision.allowed)
        self.assertEqual(decision.current_approval_logins, ())

    def test_later_changes_requested_overrides_approval(self) -> None:
        decision = evaluate_policy(
            pull_request(additions=5_001),
            [
                review(CORE_ONE, review_id=1),
                review(CORE_ONE, state="CHANGES_REQUESTED", review_id=2),
            ],
            head_introducer_logins=("contributor",),
        )

        self.assertFalse(decision.allowed)
        self.assertEqual(decision.current_approval_logins, ())

    def test_comment_does_not_erase_an_approval(self) -> None:
        decision = evaluate_policy(
            pull_request(additions=5_001),
            [
                review(CORE_TWO, review_id=1),
                review(CORE_TWO, state="COMMENTED", review_id=2),
            ],
            head_introducer_logins=("contributor",),
        )

        self.assertTrue(decision.allowed)

    def test_dismissed_review_does_not_count(self) -> None:
        decision = evaluate_policy(
            pull_request(additions=5_001),
            [review(CORE_ONE, state="DISMISSED", review_id=1)],
            head_introducer_logins=("contributor",),
        )

        self.assertFalse(decision.allowed)
        self.assertEqual(decision.current_approval_logins, ())

    def test_malformed_pull_request_data_fails_closed(self) -> None:
        malformed = pull_request(additions=5_001)
        malformed["deletions"] = "0"

        with self.assertRaises(GovernanceInputError):
            evaluate_policy(malformed, [], head_introducer_logins=("contributor",))

    def test_malformed_decisive_review_fails_closed(self) -> None:
        malformed_review = review(CORE_ONE)
        del malformed_review["commit_id"]

        with self.assertRaises(GovernanceInputError):
            evaluate_policy(
                pull_request(additions=5_001),
                [malformed_review],
                head_introducer_logins=("contributor",),
            )

    def test_current_head_pusher_cannot_supply_approval(self) -> None:
        decision = evaluate_policy(
            pull_request(additions=5_001),
            [review(CORE_ONE)],
            head_introducer_logins=(CORE_ONE,),
        )

        self.assertFalse(decision.allowed)
        self.assertEqual(decision.current_approval_logins, ())

    def test_large_change_without_pusher_identity_fails_closed(self) -> None:
        with self.assertRaises(GovernanceInputError):
            evaluate_policy(pull_request(additions=5_001), [review(CORE_ONE)])

    def test_earliest_current_head_run_identifies_pusher(self) -> None:
        api = FakeGitHubApi(
            {
                "workflow_runs": [
                    workflow_run(
                        CORE_TWO,
                        created_at="2026-08-16T00:10:00Z",
                        run_id=2,
                    ),
                    workflow_run(
                        CORE_ONE,
                        created_at="2026-08-16T00:00:00Z",
                        run_id=1,
                    ),
                ]
            }
        )

        self.assertEqual(
            api.get_current_head_introducers(
                "NoKV-Lab/NoKV", HEAD, 17, 101, "feature", "2026-08-15T00:00:00Z"
            ),
            (CORE_ONE,),
        )

    def test_current_head_pusher_ignores_runs_for_another_pull_request(self) -> None:
        api = FakeGitHubApi(
            {
                "workflow_runs": [
                    workflow_run(
                        CORE_TWO,
                        created_at="2026-08-16T00:00:00Z",
                        run_id=1,
                        pull_number=99,
                    ),
                    workflow_run(
                        CORE_ONE,
                        created_at="2026-08-16T00:01:00Z",
                        run_id=2,
                        pull_number=17,
                    ),
                ]
            }
        )

        self.assertEqual(
            api.get_current_head_introducers(
                "NoKV-Lab/NoKV", HEAD, 17, 101, "feature", "2026-08-15T00:00:00Z"
            ),
            (CORE_ONE,),
        )

    def test_empty_pull_array_uses_exact_head_repository_ref_and_time_fallback(self) -> None:
        api = FakeGitHubApi(
            {
                "workflow_runs": [
                    workflow_run(
                        CORE_ONE,
                        created_at="2026-08-16T00:01:00Z",
                        run_id=1,
                        pull_number=None,
                    )
                ]
            }
        )

        self.assertEqual(
            api.get_current_head_introducers(
                "NoKV-Lab/NoKV", HEAD, 17, 101, "feature", "2026-08-16T00:00:00Z"
            ),
            (CORE_ONE,),
        )

    def test_empty_pull_array_with_wrong_head_identity_fails_closed(self) -> None:
        api = FakeGitHubApi(
            {
                "workflow_runs": [
                    workflow_run(
                        CORE_ONE,
                        created_at="2026-08-16T00:01:00Z",
                        run_id=1,
                        pull_number=None,
                        head_repository_id=999,
                    )
                ]
            }
        )

        with self.assertRaises(GovernanceInputError):
            api.get_current_head_introducers(
                "NoKV-Lab/NoKV", HEAD, 17, 101, "feature", "2026-08-16T00:00:00Z"
            )

    def test_missing_current_head_run_fails_closed(self) -> None:
        api = FakeGitHubApi({"workflow_runs": []})

        with self.assertRaises(GovernanceInputError):
            api.get_current_head_introducers(
                "NoKV-Lab/NoKV", HEAD, 17, 101, "feature", "2026-08-15T00:00:00Z"
            )

    def test_workflow_run_for_another_head_fails_closed(self) -> None:
        api = FakeGitHubApi(
            {
                "workflow_runs": [
                    workflow_run(
                        CORE_ONE,
                        created_at="2026-08-16T00:00:00Z",
                        run_id=1,
                        head_sha=OLD_HEAD,
                    )
                ]
            }
        )

        with self.assertRaises(GovernanceInputError):
            api.get_current_head_introducers(
                "NoKV-Lab/NoKV", HEAD, 17, 101, "feature", "2026-08-15T00:00:00Z"
            )

    def test_changed_paths_are_complete_canonical_and_sorted(self) -> None:
        api = FakeGitHubApi(
            [
                {
                    "filename": "docs/live_workbench.py",
                    "previous_filename": "scripts/workbench/live_workbench.py",
                },
                {"filename": ".github/workflows/rust.yml"},
            ]
        )

        self.assertEqual(
            api.get_changed_paths("NoKV-Lab/NoKV", 17, 2),
            (
                ".github/workflows/rust.yml",
                "docs/live_workbench.py",
                "scripts/workbench/live_workbench.py",
            ),
        )

    def test_incomplete_or_noncanonical_changed_paths_fail_closed(self) -> None:
        incomplete = FakeGitHubApi([{"filename": ".github/workflows/rust.yml"}])
        with self.assertRaisesRegex(GovernanceInputError, "incomplete"):
            incomplete.get_changed_paths("NoKV-Lab/NoKV", 17, 2)

        noncanonical = FakeGitHubApi([{"filename": "../rust.yml"}])
        with self.assertRaisesRegex(GovernanceInputError, "canonical"):
            noncanonical.get_changed_paths("NoKV-Lab/NoKV", 17, 1)


if __name__ == "__main__":
    unittest.main()
