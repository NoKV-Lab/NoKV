#!/usr/bin/env python3
"""Tests for the pull request change-governance policy."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from typing import Any


sys.path.insert(0, str(Path(__file__).resolve().parent))

from pr_change_governance import (  # noqa: E402
    GitHubApi,
    GovernanceInputError,
    evaluate_policy,
)


HEAD = "a" * 40
OLD_HEAD = "b" * 40
CORE_ONE = "wchwawa"
CORE_TWO = "feichai0017"


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
    login: str, *, created_at: str, run_id: int, head_sha: str = HEAD
) -> dict[str, Any]:
    return {
        "actor": {"login": login},
        "created_at": created_at,
        "head_sha": head_sha,
        "id": run_id,
    }


class FakeGitHubApi(GitHubApi):
    def __init__(self, payload: dict[str, Any]) -> None:
        super().__init__("https://api.github.invalid", "test-token")
        self.payload = payload

    def get_json(self, path: str) -> Any:
        del path
        return self.payload


class ChangeGovernancePolicyTest(unittest.TestCase):
    def test_exact_threshold_does_not_trigger_large_change_rule(self) -> None:
        decision = evaluate_policy(pull_request(additions=3_000, deletions=2_000), [])

        self.assertFalse(decision.is_large_change)
        self.assertTrue(decision.allowed)
        self.assertEqual(decision.required_approvals, 0)

    def test_one_line_over_threshold_requires_one_core_approval(self) -> None:
        decision = evaluate_policy(
            pull_request(additions=5_001), [], last_push_logins=("contributor",)
        )

        self.assertTrue(decision.is_large_change)
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.required_approvals, 1)

    def test_current_core_maintainer_approval_passes(self) -> None:
        decision = evaluate_policy(
            pull_request(additions=3_000, deletions=2_001),
            [review(CORE_ONE, review_id=1)],
            last_push_logins=("contributor",),
        )

        self.assertTrue(decision.allowed)
        self.assertEqual(decision.current_approval_logins, (CORE_ONE,))

    def test_duplicate_approvals_count_once(self) -> None:
        decision = evaluate_policy(
            pull_request(additions=5_001),
            [review(CORE_ONE, review_id=1), review(CORE_ONE.upper(), review_id=2)],
            last_push_logins=("contributor",),
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
            last_push_logins=(CORE_ONE,),
        )

        self.assertFalse(decision.allowed)
        self.assertEqual(decision.current_approval_logins, ())

    def test_approval_on_an_old_head_does_not_count(self) -> None:
        decision = evaluate_policy(
            pull_request(additions=5_001),
            [review(CORE_ONE, commit_id=OLD_HEAD, review_id=1)],
            last_push_logins=("contributor",),
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
            last_push_logins=("contributor",),
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
            last_push_logins=("contributor",),
        )

        self.assertTrue(decision.allowed)

    def test_dismissed_review_does_not_count(self) -> None:
        decision = evaluate_policy(
            pull_request(additions=5_001),
            [review(CORE_ONE, state="DISMISSED", review_id=1)],
            last_push_logins=("contributor",),
        )

        self.assertFalse(decision.allowed)
        self.assertEqual(decision.current_approval_logins, ())

    def test_malformed_pull_request_data_fails_closed(self) -> None:
        malformed = pull_request(additions=5_001)
        malformed["deletions"] = "0"

        with self.assertRaises(GovernanceInputError):
            evaluate_policy(malformed, [], last_push_logins=("contributor",))

    def test_malformed_decisive_review_fails_closed(self) -> None:
        malformed_review = review(CORE_ONE)
        del malformed_review["commit_id"]

        with self.assertRaises(GovernanceInputError):
            evaluate_policy(
                pull_request(additions=5_001),
                [malformed_review],
                last_push_logins=("contributor",),
            )

    def test_current_head_pusher_cannot_supply_approval(self) -> None:
        decision = evaluate_policy(
            pull_request(additions=5_001),
            [review(CORE_ONE)],
            last_push_logins=(CORE_ONE,),
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
            api.get_current_head_push_actors("NoKV-Lab/NoKV", HEAD),
            (CORE_ONE,),
        )

    def test_missing_current_head_run_fails_closed(self) -> None:
        api = FakeGitHubApi({"workflow_runs": []})

        with self.assertRaises(GovernanceInputError):
            api.get_current_head_push_actors("NoKV-Lab/NoKV", HEAD)

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
            api.get_current_head_push_actors("NoKV-Lab/NoKV", HEAD)


if __name__ == "__main__":
    unittest.main()
