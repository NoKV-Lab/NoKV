#!/usr/bin/env python3
"""Fail closed when a governed pull request lacks core-maintainer approval."""

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
GOVERNANCE_SENSITIVE_EXACT_PATHS = frozenset(
    {
        ".github/CODEOWNERS",
        "scripts/release/test_homebrew_source_release.py",
    }
)
GOVERNANCE_SENSITIVE_PREFIXES = (
    ".github/actions/",
    ".github/workflows/",
    "scripts/ci/",
    "scripts/workbench/",
)
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
    changed_paths: tuple[str, ...]
    governance_sensitive_paths: tuple[str, ...]
    threshold: int
    required_approvals: int
    current_approval_logins: tuple[str, ...]
    current_head_introducer_logins: tuple[str, ...]
    head_sha: str

    @property
    def is_large_change(self) -> bool:
        return self.changed_lines > self.threshold

    @property
    def is_governance_sensitive(self) -> bool:
        return bool(self.governance_sensitive_paths)

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

    def get_changed_paths(
        self, repository: str, pull_request_number: int, expected_count: int
    ) -> tuple[str, ...]:
        files = self.get_all(
            f"repos/{repository}/pulls/{pull_request_number}/files"
        )
        if len(files) != expected_count:
            raise GovernanceInputError(
                "pull request changed-file list is incomplete: "
                f"expected {expected_count}, received {len(files)}"
            )
        paths = tuple(
            _required_string(item, "filename", "pull request file")
            for item in files
        )
        previous_paths = tuple(
            previous
            for item in files
            if isinstance((previous := item.get("previous_filename")), str)
            and previous
        )
        all_paths = paths + previous_paths
        if len(set(paths)) != len(paths):
            raise GovernanceInputError("pull request changed-file list contains duplicates")
        for path in all_paths:
            if path.startswith("/") or path.endswith("/") or ".." in path.split("/"):
                raise GovernanceInputError(
                    f"pull request file path is not canonical: {path!r}"
                )
        return tuple(sorted(set(all_paths)))

    def get_current_head_introducers(
        self,
        repository: str,
        head_sha: str,
        pull_request_number: int,
        head_repository_id: int,
        head_ref: str,
        pull_request_created_at: str,
    ) -> tuple[str, ...]:
        """Return the actor that first introduced the exact head to PR CI.

        GitHub does not expose an authoritative conditional last-pusher API.
        The earliest original ``pull_request`` workflow run is a conservative
        proxy for the actor that made this exact head reviewable. Runs with an
        empty pull-request array are bound by head repository, ref, SHA, and PR
        creation time. Conflicting earliest actors fail closed.
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
                head_branch = _required_string(run, "head_branch", "workflow run")
                run_head_repository = run.get("head_repository")
                if not isinstance(run_head_repository, dict):
                    raise GovernanceInputError("workflow run head repository is missing")
                run_head_repository_id = run_head_repository.get("id")
                if (
                    isinstance(run_head_repository_id, bool)
                    or not isinstance(run_head_repository_id, int)
                    or run_head_repository_id <= 0
                ):
                    raise GovernanceInputError(
                        "workflow run head repository id is not valid"
                    )
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
                pull_requests = run.get("pull_requests")
                if not isinstance(pull_requests, list) or not all(
                    isinstance(item, dict) for item in pull_requests
                ):
                    raise GovernanceInputError(
                        "workflow run pull-request identity is missing"
                    )
                run_pull_numbers = []
                for item in pull_requests:
                    number = item.get("number")
                    if isinstance(number, bool) or not isinstance(number, int) or number <= 0:
                        raise GovernanceInputError(
                            "workflow run pull-request number is not valid"
                        )
                    run_pull_numbers.append(number)
                if run_pull_numbers and pull_request_number not in run_pull_numbers:
                    continue
                if (
                    run_head_repository_id != head_repository_id
                    or head_branch != head_ref
                    or created_at < pull_request_created_at
                ):
                    continue
                runs.append((created_at, run_id, login))

            if len(page_runs) < 100:
                if not runs:
                    raise GovernanceInputError(
                        "no pull-request workflow run identifies the current-head introducer"
                    )
                earliest_time = min(created_at for created_at, _, _ in runs)
                earliest_actors = {
                    login for created_at, _, login in runs if created_at == earliest_time
                }
                if len(earliest_actors) != 1:
                    raise GovernanceInputError(
                        "current-head introducer actors conflict at the earliest event"
                    )
                return tuple(sorted(earliest_actors))

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
    head_introducer_logins: tuple[str, ...],
) -> tuple[str, ...]:
    user = pull_request.get("user")
    head = pull_request.get("head")
    if not isinstance(user, dict) or not isinstance(head, dict):
        raise GovernanceInputError("pull request author or head identity is missing")

    author_login = _required_string(user, "login", "pull request author").casefold()
    head_sha = _required_string(head, "sha", "pull request head")
    excluded_logins = {author_login}
    excluded_logins.update(login.casefold() for login in head_introducer_logins)
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


def governance_sensitive_paths(changed_paths: tuple[str, ...]) -> tuple[str, ...]:
    sensitive = []
    for path in changed_paths:
        if path in GOVERNANCE_SENSITIVE_EXACT_PATHS or path.startswith(
            GOVERNANCE_SENSITIVE_PREFIXES
        ):
            sensitive.append(path)
    return tuple(sorted(sensitive))


def review_trigger_summary(decision: GovernanceDecision) -> str:
    triggers = []
    if decision.is_large_change:
        triggers.append(
            f"large change ({decision.changed_lines:,} > "
            f"{decision.threshold:,} lines)"
        )
    if decision.is_governance_sensitive:
        path_count = len(decision.governance_sensitive_paths)
        noun = "path" if path_count == 1 else "paths"
        triggers.append(
            f"protected CI trust-root change ({path_count:,} {noun})"
        )
    return " and ".join(triggers) or "none"


def evaluate_policy(
    pull_request: dict[str, Any],
    reviews: list[dict[str, Any]],
    head_introducer_logins: tuple[str, ...] = (),
    changed_paths: tuple[str, ...] = (),
) -> GovernanceDecision:
    additions = _nonnegative_int(pull_request, "additions")
    deletions = _nonnegative_int(pull_request, "deletions")
    changed_files = _nonnegative_int(pull_request, "changed_files")
    head = pull_request.get("head")
    if not isinstance(head, dict):
        raise GovernanceInputError("pull request head identity is missing")
    head_sha = _required_string(head, "sha", "pull request head")
    changed_lines = additions + deletions
    sensitive_paths = governance_sensitive_paths(changed_paths)
    required_approvals = (
        LARGE_CHANGE_REQUIRED_APPROVALS
        if changed_lines > LARGE_CHANGE_LINE_THRESHOLD or sensitive_paths
        else 0
    )
    if required_approvals and not head_introducer_logins:
        raise GovernanceInputError(
            "current-head introducer identity is required for a governed change"
        )
    approvals = current_core_maintainer_approvals(
        pull_request, reviews, head_introducer_logins
    )
    return GovernanceDecision(
        additions=additions,
        deletions=deletions,
        changed_files=changed_files,
        changed_lines=changed_lines,
        changed_paths=tuple(sorted(changed_paths)),
        governance_sensitive_paths=sensitive_paths,
        threshold=LARGE_CHANGE_LINE_THRESHOLD,
        required_approvals=required_approvals,
        current_approval_logins=approvals,
        current_head_introducer_logins=head_introducer_logins,
        head_sha=head_sha,
    )


def _annotation(value: str) -> str:
    return value.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def _write_step_summary(decision: GovernanceDecision) -> None:
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not path:
        return
    approval_text = ", ".join(decision.current_approval_logins) or "none"
    introducer_text = ", ".join(decision.current_head_introducer_logins) or "not required"
    outcome = "PASS" if decision.allowed else "FAIL"
    with Path(path).open("a", encoding="utf-8") as summary:
        summary.write("## Pull request change governance\n\n")
        summary.write(f"- Result: **{outcome}**\n")
        summary.write(
            f"- Changed lines: **{decision.changed_lines:,}** "
            f"({decision.additions:,} additions + {decision.deletions:,} deletions)\n"
        )
        summary.write(f"- Changed files: **{decision.changed_files:,}**\n")
        summary.write(
            f"- Independent-review trigger: **{review_trigger_summary(decision)}**\n"
        )
        sensitive_text = ", ".join(decision.governance_sensitive_paths) or "none"
        summary.write(f"- Governance-sensitive paths: **{sensitive_text}**\n")
        summary.write(f"- Current-head introducer: **{introducer_text}**\n")
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
        changed_files = _nonnegative_int(pull_request, "changed_files")
        changed_paths = api.get_changed_paths(
            args.repository, args.pull_request, changed_files
        )
        requires_review = (
            additions + deletions > LARGE_CHANGE_LINE_THRESHOLD
            or bool(governance_sensitive_paths(changed_paths))
        )
        if requires_review:
            reviews = api.get_all(
                f"repos/{args.repository}/pulls/{args.pull_request}/reviews"
            )
            head = pull_request.get("head")
            if not isinstance(head, dict):
                raise GovernanceInputError("pull request head identity is missing")
            head_sha = _required_string(head, "sha", "pull request head")
            head_ref = _required_string(head, "ref", "pull request head")
            head_repository = head.get("repo")
            if not isinstance(head_repository, dict):
                raise GovernanceInputError("pull request head repository is missing")
            head_repository_id = head_repository.get("id")
            if (
                isinstance(head_repository_id, bool)
                or not isinstance(head_repository_id, int)
                or head_repository_id <= 0
            ):
                raise GovernanceInputError("pull request head repository id is not valid")
            pull_request_created_at = _required_string(
                pull_request, "created_at", "pull request"
            )
            head_introducer_logins = api.get_current_head_introducers(
                args.repository,
                head_sha,
                args.pull_request,
                head_repository_id,
                head_ref,
                pull_request_created_at,
            )
        else:
            reviews = []
            head_introducer_logins = ()
        decision = evaluate_policy(
            pull_request, reviews, head_introducer_logins, changed_paths
        )
    except GovernanceInputError as error:
        print(
            "::error title=Change governance failed closed::"
            + _annotation(str(error)),
            file=sys.stderr,
        )
        return 2

    decision_payload = asdict(decision)
    decision_payload["review_trigger"] = review_trigger_summary(decision)
    print(json.dumps(decision_payload, sort_keys=True))
    _write_step_summary(decision)
    if decision.allowed:
        print(
            "Change governance passed: "
            f"review trigger {review_trigger_summary(decision)}, "
            f"{decision.changed_lines} changed lines, "
            f"{len(decision.current_approval_logins)} current core maintainer approvals."
        )
        return 0

    print(
        "::error title=Governed change lacks current core maintainer approval::"
        + _annotation(
            f"PR #{args.pull_request} requires review because of "
            f"{review_trigger_summary(decision)}. It changes "
            f"{decision.changed_lines:,} lines; the large-change threshold is more than "
            f"{decision.threshold:,}. It has only "
            f"{len(decision.current_approval_logins)} eligible approvals on head "
            f"{decision.head_sha}. The author, current-head introducer, bots, non-core "
            "reviewers, dismissed reviews, duplicate reviewers, and approvals on "
            "older commits do not count."
        ),
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
