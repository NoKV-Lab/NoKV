#!/usr/bin/env python3
"""Tests for the pull request change-governance policy."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from typing import Any


sys.path.insert(0, str(Path(__file__).resolve().parent))

from pr_change_governance import GovernanceInputError, evaluate_policy  # noqa: E402


HEAD = "a" * 40
OLD_HEAD = "b" * 40


def pull_request(
    *, additions: int = 0, deletions: int = 0, changed_files: int = 1
) -> dict[str, Any]:
    return {
        "additions": additions,
        "deletions": deletions,
        "changed_files": changed_files,
        "head": {"sha": HEAD},
        "user": {"login": "author", "type": "User"},
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


class ChangeGovernancePolicyTest(unittest.TestCase):
    def test_exact_threshold_does_not_trigger_large_change_rule(self) -> None:
        decision = evaluate_policy(pull_request(additions=6_000, deletions=4_000), [])

        self.assertFalse(decision.is_large_change)
        self.assertTrue(decision.allowed)
        self.assertEqual(decision.required_approvals, 0)

    def test_one_line_over_threshold_requires_two_approvals(self) -> None:
        decision = evaluate_policy(pull_request(additions=10_001), [])

        self.assertTrue(decision.is_large_change)
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.required_approvals, 2)

    def test_two_distinct_current_human_approvals_pass(self) -> None:
        decision = evaluate_policy(
            pull_request(additions=8_000, deletions=3_000),
            [review("reviewer-one", review_id=1), review("reviewer-two", review_id=2)],
        )

        self.assertTrue(decision.allowed)
        self.assertEqual(
            decision.current_approval_logins, ("reviewer-one", "reviewer-two")
        )

    def test_duplicate_approvals_count_once(self) -> None:
        decision = evaluate_policy(
            pull_request(additions=10_001),
            [review("reviewer", review_id=1), review("Reviewer", review_id=2)],
        )

        self.assertFalse(decision.allowed)
        self.assertEqual(decision.current_approval_logins, ("Reviewer",))

    def test_author_and_bots_do_not_count(self) -> None:
        decision = evaluate_policy(
            pull_request(additions=10_001),
            [
                review("author", review_id=1),
                review("automation[bot]", reviewer_type="Bot", review_id=2),
                review("human", review_id=3),
            ],
        )

        self.assertFalse(decision.allowed)
        self.assertEqual(decision.current_approval_logins, ("human",))

    def test_approval_on_an_old_head_does_not_count(self) -> None:
        decision = evaluate_policy(
            pull_request(additions=10_001),
            [
                review("stale", commit_id=OLD_HEAD, review_id=1),
                review("current", review_id=2),
            ],
        )

        self.assertFalse(decision.allowed)
        self.assertEqual(decision.current_approval_logins, ("current",))

    def test_later_changes_requested_overrides_approval(self) -> None:
        decision = evaluate_policy(
            pull_request(additions=10_001),
            [
                review("reviewer-one", review_id=1),
                review("reviewer-two", review_id=2),
                review(
                    "reviewer-two", state="CHANGES_REQUESTED", review_id=3
                ),
            ],
        )

        self.assertFalse(decision.allowed)
        self.assertEqual(decision.current_approval_logins, ("reviewer-one",))

    def test_comment_does_not_erase_an_approval(self) -> None:
        decision = evaluate_policy(
            pull_request(additions=10_001),
            [
                review("reviewer-one", review_id=1),
                review("reviewer-one", state="COMMENTED", review_id=2),
                review("reviewer-two", review_id=3),
            ],
        )

        self.assertTrue(decision.allowed)

    def test_dismissed_review_does_not_count(self) -> None:
        decision = evaluate_policy(
            pull_request(additions=10_001),
            [
                review("dismissed", state="DISMISSED", review_id=1),
                review("current", review_id=2),
            ],
        )

        self.assertFalse(decision.allowed)
        self.assertEqual(decision.current_approval_logins, ("current",))

    def test_malformed_pull_request_data_fails_closed(self) -> None:
        malformed = pull_request(additions=10_001)
        malformed["deletions"] = "0"

        with self.assertRaises(GovernanceInputError):
            evaluate_policy(malformed, [])

    def test_malformed_decisive_review_fails_closed(self) -> None:
        malformed_review = review("reviewer")
        del malformed_review["commit_id"]

        with self.assertRaises(GovernanceInputError):
            evaluate_policy(pull_request(additions=10_001), [malformed_review])


if __name__ == "__main__":
    unittest.main()
