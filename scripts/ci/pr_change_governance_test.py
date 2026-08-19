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
    def test_base_owned_workflow_fetches_policy_and_tests(self) -> None:
        workflow = WORKFLOW_PATH.read_text(encoding="utf-8")
        self.assertIn("scripts/ci/pr_change_governance.py", workflow)
        self.assertIn("scripts/ci/pr_change_governance_test.py", workflow)
        self.assertIn(".github/CODEOWNERS", workflow)
        self.assertIn("NOKV_CODEOWNERS_PATH", workflow)
        self.assertNotIn("actions/checkout", workflow)

    def test_workflow_avoids_redundant_review_request_events(self) -> None:
        workflow = WORKFLOW_PATH.read_text(encoding="utf-8")

        self.assertIn("      - synchronize", workflow)
        self.assertIn("  pull_request_review:", workflow)
        self.assertIn("    types: [submitted, edited, dismissed]", workflow)
        self.assertNotIn("      - review_requested", workflow)
        self.assertNotIn("      - review_request_removed", workflow)
        self.assertIn("    name: large-change-review", workflow)
        self.assertNotIn("statuses: write", workflow)
        self.assertNotIn("STATUS_CONTEXT", workflow)
        self.assertNotIn("post_status", workflow)

    def test_exact_threshold_does_not_trigger_large_change_rule(self) -> None:
        decision = evaluate_policy(pull_request(additions=3_000, deletions=2_000), [])

        self.assertFalse(decision.is_large_change)
        self.assertTrue(decision.allowed)
        self.assertEqual(decision.required_approvals, 0)

    def test_small_ordinary_change_keeps_fast_path_without_review(self) -> None:
        decision = evaluate_policy(pull_request(additions=10), [])

        self.assertFalse(decision.is_large_change)
        self.assertEqual(decision.required_approvals, 0)
        self.assertTrue(decision.allowed)

    def test_small_ci_trust_root_change_does_not_require_review(self) -> None:
        decision = evaluate_policy(
            pull_request(additions=10),
            [],
        )

        self.assertFalse(decision.is_large_change)
        self.assertEqual(decision.required_approvals, 0)
        self.assertEqual(decision.current_approval_logins, ())
        self.assertTrue(decision.allowed)

    def test_review_trigger_summary_names_only_the_size_threshold(self) -> None:
        ordinary = evaluate_policy(pull_request(additions=10), [])
        large = evaluate_policy(
            pull_request(additions=5_001),
            [],
            head_introducer_logins=("contributor",),
        )

        self.assertEqual(review_trigger_summary(ordinary), "none")
        self.assertEqual(
            review_trigger_summary(large), "large change (5,001 > 5,000 lines)"
        )

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

if __name__ == "__main__":
    unittest.main()
