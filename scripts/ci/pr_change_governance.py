#!/usr/bin/env python3
"""Fail closed when a large pull request lacks core-maintainer approval."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


LARGE_CHANGE_LINE_THRESHOLD = 5_000
LARGE_CHANGE_REQUIRED_APPROVALS = 1
CORE_MAINTAINER_LOGINS = frozenset({"feichai0017", "wchwawa"})
MAX_API_PAGES = 100
REPOSITORY_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")


class GovernanceInputError(RuntimeError):
    """The policy could not evaluate trustworthy GitHub data."""


@dataclass(frozen=True)
class GovernanceDecision:
    additions: int
    deletions: int
    changed_files: int
    changed_lines: int
    threshold: int
    required_approvals: int
    current_approval_logins: tuple[str, ...]
    current_head_push_logins: tuple[str, ...]
    head_sha: str

    @property
    def is_large_change(self) -> bool:
        return self.changed_lines > self.threshold

    @property
    def allowed(self) -> bool:
        return len(self.current_approval_logins) >= self.required_approvals


class GitHubApi:
    """Small read-only REST client with bounded pagination."""

    def __init__(self, api_url: str, token: str) -> None:
        if not token:
            raise GovernanceInputError("GitHub token is required")
        self.api_url = api_url.rstrip("/")
        self.token = token

    def get_json(self, path: str) -> Any:
        request = urllib.request.Request(
            f"{self.api_url}/{path.lstrip('/')}",
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self.token}",
                "User-Agent": "nokv-change-governance",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                payload = response.read()
        except urllib.error.HTTPError as error:
            raise GovernanceInputError(
                f"GitHub API returned HTTP {error.code} for {path}"
            ) from None
        except urllib.error.URLError as error:
            raise GovernanceInputError(
                f"GitHub API request failed for {path}: {error.reason}"
            ) from None

        try:
            return json.loads(payload)
        except json.JSONDecodeError as error:
            raise GovernanceInputError(
                f"GitHub API returned invalid JSON for {path}: {error.msg}"
            ) from None

    def get_all(self, path: str) -> list[dict[str, Any]]:
        separator = "&" if "?" in path else "?"
        collected: list[dict[str, Any]] = []
        for page in range(1, MAX_API_PAGES + 1):
            payload = self.get_json(f"{path}{separator}per_page=100&page={page}")
            if not isinstance(payload, list) or not all(
                isinstance(item, dict) for item in payload
            ):
                raise GovernanceInputError(
                    f"GitHub API returned a non-object page for {path}"
                )
            collected.extend(payload)
            if len(payload) < 100:
                return collected
        raise GovernanceInputError(
            f"GitHub API pagination exceeded {MAX_API_PAGES} pages for {path}"
        )

    def get_current_head_push_actors(
        self, repository: str, head_sha: str
    ) -> tuple[str, ...]:
        """Return the actor that introduced the current head to pull-request CI.

        The earliest pull_request workflow run for a head is created by the
        opened or synchronize event that made that head reviewable. Reruns keep
        the original actor. Selecting the earliest run avoids mistaking a later
        reopen event for a push.
        """

        runs: list[tuple[str, int, str]] = []
        for page in range(1, MAX_API_PAGES + 1):
            query = urllib.parse.urlencode(
                {
                    "event": "pull_request",
                    "head_sha": head_sha,
                    "per_page": 100,
                    "page": page,
                }
            )
            payload = self.get_json(f"repos/{repository}/actions/runs?{query}")
            if not isinstance(payload, dict):
                raise GovernanceInputError(
                    "GitHub Actions API returned a non-object workflow-run page"
                )
            page_runs = payload.get("workflow_runs")
            if not isinstance(page_runs, list) or not all(
                isinstance(run, dict) for run in page_runs
            ):
                raise GovernanceInputError(
                    "GitHub Actions API returned malformed workflow runs"
                )

            for run in page_runs:
                run_head_sha = _required_string(run, "head_sha", "workflow run")
                if run_head_sha != head_sha:
                    raise GovernanceInputError(
                        "GitHub Actions API returned a workflow run for another head"
                    )
                created_at = _required_string(run, "created_at", "workflow run")
                run_id = run.get("id")
                if (
                    isinstance(run_id, bool)
                    or not isinstance(run_id, int)
                    or run_id < 0
                ):
                    raise GovernanceInputError("workflow run id is not valid")
                actor = run.get("actor")
                if not isinstance(actor, dict):
                    raise GovernanceInputError("workflow run actor is missing")
                login = _required_string(actor, "login", "workflow run actor")
                runs.append((created_at, run_id, login))

            if len(page_runs) < 100:
                if not runs:
                    raise GovernanceInputError(
                        "no pull-request workflow run identifies the current-head pusher"
                    )
                earliest = min(runs)
                return (earliest[2],)

        raise GovernanceInputError(
            "GitHub Actions workflow-run pagination exceeded "
            f"{MAX_API_PAGES} pages"
        )


def _nonnegative_int(payload: dict[str, Any], field: str) -> int:
    value = payload.get(field)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise GovernanceInputError(f"pull request field {field!r} is not a nonnegative integer")
    return value


def _required_string(payload: dict[str, Any], field: str, owner: str) -> str:
    value = payload.get(field)
    if not isinstance(value, str) or not value:
        raise GovernanceInputError(f"{owner} field {field!r} is not a nonempty string")
    return value


def _review_order(review: dict[str, Any]) -> tuple[str, int]:
    submitted_at = review.get("submitted_at")
    review_id = review.get("id")
    return (
        submitted_at if isinstance(submitted_at, str) else "",
        review_id if isinstance(review_id, int) and not isinstance(review_id, bool) else -1,
    )


def current_core_maintainer_approvals(
    pull_request: dict[str, Any],
    reviews: list[dict[str, Any]],
    last_push_logins: tuple[str, ...],
) -> tuple[str, ...]:
    user = pull_request.get("user")
    head = pull_request.get("head")
    if not isinstance(user, dict) or not isinstance(head, dict):
        raise GovernanceInputError("pull request author or head identity is missing")

    author_login = _required_string(user, "login", "pull request author").casefold()
    head_sha = _required_string(head, "sha", "pull request head")
    excluded_logins = {author_login}
    excluded_logins.update(login.casefold() for login in last_push_logins)
    decisive_state: dict[str, tuple[str, bool]] = {}

    for review in sorted(reviews, key=_review_order):
        raw_state = review.get("state")
        if not isinstance(raw_state, str):
            raise GovernanceInputError("review state is missing")
        state = raw_state.upper()
        if state not in {"APPROVED", "CHANGES_REQUESTED", "DISMISSED"}:
            continue

        reviewer = review.get("user")
        if not isinstance(reviewer, dict):
            raise GovernanceInputError("decisive review has no reviewer identity")
        login = _required_string(reviewer, "login", "reviewer")
        reviewer_type = _required_string(reviewer, "type", "reviewer")
        normalized_login = login.casefold()
        if (
            reviewer_type != "User"
            or normalized_login in excluded_logins
            or normalized_login not in CORE_MAINTAINER_LOGINS
        ):
            continue

        commit_id = _required_string(review, "commit_id", "review")
        if commit_id != head_sha:
            continue

        key = normalized_login
        if state == "APPROVED":
            decisive_state[key] = (login, True)
        else:
            decisive_state[key] = (login, False)

    return tuple(
        sorted(login for login, approved in decisive_state.values() if approved)
    )


def evaluate_policy(
    pull_request: dict[str, Any],
    reviews: list[dict[str, Any]],
    last_push_logins: tuple[str, ...] = (),
) -> GovernanceDecision:
    additions = _nonnegative_int(pull_request, "additions")
    deletions = _nonnegative_int(pull_request, "deletions")
    changed_files = _nonnegative_int(pull_request, "changed_files")
    head = pull_request.get("head")
    if not isinstance(head, dict):
        raise GovernanceInputError("pull request head identity is missing")
    head_sha = _required_string(head, "sha", "pull request head")
    changed_lines = additions + deletions
    required_approvals = (
        LARGE_CHANGE_REQUIRED_APPROVALS
        if changed_lines > LARGE_CHANGE_LINE_THRESHOLD
        else 0
    )
    if required_approvals and not last_push_logins:
        raise GovernanceInputError(
            "current-head pusher identity is required for a large change"
        )
    approvals = current_core_maintainer_approvals(
        pull_request, reviews, last_push_logins
    )
    return GovernanceDecision(
        additions=additions,
        deletions=deletions,
        changed_files=changed_files,
        changed_lines=changed_lines,
        threshold=LARGE_CHANGE_LINE_THRESHOLD,
        required_approvals=required_approvals,
        current_approval_logins=approvals,
        current_head_push_logins=last_push_logins,
        head_sha=head_sha,
    )


def _annotation(value: str) -> str:
    return value.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def _write_step_summary(decision: GovernanceDecision) -> None:
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not path:
        return
    approval_text = ", ".join(decision.current_approval_logins) or "none"
    pusher_text = ", ".join(decision.current_head_push_logins) or "not required"
    outcome = "PASS" if decision.allowed else "FAIL"
    with Path(path).open("a", encoding="utf-8") as summary:
        summary.write("## Pull request change governance\n\n")
        summary.write(f"- Result: **{outcome}**\n")
        summary.write(
            f"- Changed lines: **{decision.changed_lines:,}** "
            f"({decision.additions:,} additions + {decision.deletions:,} deletions)\n"
        )
        summary.write(f"- Changed files: **{decision.changed_files:,}**\n")
        summary.write(f"- Current-head pusher: **{pusher_text}**\n")
        summary.write(
            "- Current-head core maintainer approvals: "
            f"**{len(decision.current_approval_logins)}** "
            f"({approval_text})\n"
        )
        summary.write(f"- Required by this gate: **{decision.required_approvals}**\n")


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--repository", default=os.environ.get("GITHUB_REPOSITORY", "")
    )
    parser.add_argument("--pull-request", type=int, required=True)
    parser.add_argument(
        "--api-url", default=os.environ.get("GITHUB_API_URL", "https://api.github.com")
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN") or ""
    try:
        if not REPOSITORY_PATTERN.fullmatch(args.repository):
            raise GovernanceInputError("repository must have owner/name form")
        if args.pull_request <= 0:
            raise GovernanceInputError("pull request number must be positive")

        api = GitHubApi(args.api_url, token)
        pull_request = api.get_json(
            f"repos/{args.repository}/pulls/{args.pull_request}"
        )
        if not isinstance(pull_request, dict):
            raise GovernanceInputError("GitHub API returned a non-object pull request")
        additions = _nonnegative_int(pull_request, "additions")
        deletions = _nonnegative_int(pull_request, "deletions")
        is_large_change = additions + deletions > LARGE_CHANGE_LINE_THRESHOLD
        if is_large_change:
            reviews = api.get_all(
                f"repos/{args.repository}/pulls/{args.pull_request}/reviews"
            )
            head = pull_request.get("head")
            if not isinstance(head, dict):
                raise GovernanceInputError("pull request head identity is missing")
            head_sha = _required_string(head, "sha", "pull request head")
            last_push_logins = api.get_current_head_push_actors(
                args.repository, head_sha
            )
        else:
            reviews = []
            last_push_logins = ()
        decision = evaluate_policy(pull_request, reviews, last_push_logins)
    except GovernanceInputError as error:
        print(
            "::error title=Change governance failed closed::"
            + _annotation(str(error)),
            file=sys.stderr,
        )
        return 2

    print(json.dumps(asdict(decision), sort_keys=True))
    _write_step_summary(decision)
    if decision.allowed:
        print(
            f"Change governance passed: {decision.changed_lines} changed lines, "
            f"{len(decision.current_approval_logins)} current core maintainer approvals."
        )
        return 0

    print(
        "::error title=Large change lacks current core maintainer approval::"
        + _annotation(
            f"PR #{args.pull_request} changes {decision.changed_lines} lines, above "
            f"the {decision.threshold} line threshold, but has only "
            f"{len(decision.current_approval_logins)} eligible approvals on head "
            f"{decision.head_sha}. The author, current-head pusher, bots, non-core "
            "reviewers, dismissed reviews, duplicate reviewers, and approvals on "
            "older commits do not count."
        ),
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
