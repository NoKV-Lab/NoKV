#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0
"""Regression tests for PR description enforcement."""

from __future__ import annotations

import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest

from pr_description import DEPENDABOT, SECTIONS, validate_description


def description() -> str:
    return "\n\n".join(f"## {section}\n\nConcrete explanation for {section}." for section in SECTIONS)


class DescriptionTest(unittest.TestCase):
    def test_complete_description_passes(self) -> None:
        self.assertEqual(validate_description("chore: enforce PR descriptions", description()), [])

    def test_title_requires_type_and_outcome(self) -> None:
        for title in (None, "", "update docs", "docs:", "docs: ", "docs(scope): update docs"):
            with self.subTest(title=title):
                self.assertTrue(validate_description(title, description()))

    def test_missing_duplicate_reordered_and_extra_sections_fail(self) -> None:
        for body in (
            description().replace("## Example", "### Example"),
            description() + "\n\n## Gain\nDuplicate.",
            description().replace("## Problem", "## Temporary").replace("## Gain", "## Problem").replace("## Temporary", "## Gain"),
            description() + "\n\n## Files Changed\nFile list.",
        ):
            with self.subTest(body=body):
                self.assertTrue(validate_description("docs: update guidance", body))

    def test_empty_and_comment_only_sections_fail(self) -> None:
        for content in ("", "<!-- Fill this in. -->", "- [ ]", "___", "TODO", "_TODO_", "N/A", "None.", "```markdown\n```", "~~~~\n~~~~"):
            body = description().replace("Concrete explanation for Tradeoffs.", content)
            with self.subTest(content=content):
                self.assertTrue(validate_description("docs: update guidance", body))

    def test_not_applicable_with_reason_passes(self) -> None:
        body = description().replace("Concrete explanation for Tradeoffs.", "Not applicable: no runtime behavior changes.")
        self.assertEqual(validate_description("docs: update guidance", body), [])

    def test_comments_cannot_supply_headings_or_content(self) -> None:
        self.assertTrue(validate_description("docs: update guidance", "<!--\n" + description() + "\n-->"))
        body = description().replace("Concrete explanation for Tradeoffs.", "<!-- hidden")
        self.assertTrue(validate_description("docs: update guidance", body))

    def test_fenced_example_headings_do_not_count_as_sections(self) -> None:
        for fence in ("```", "~~~~"):
            body = description().replace("Concrete explanation for Example.", f"{fence}markdown\n## Summary\nBefore.\n{fence}")
            with self.subTest(fence=fence):
                self.assertEqual(validate_description("docs: update guidance", body), [])
        self.assertTrue(validate_description("docs: update guidance", "```markdown\n" + description() + "\n```"))

    def test_unclosed_code_fence_and_preamble_fail(self) -> None:
        for body in (description() + "\n```", "Closes #123\n\n" + description()):
            self.assertTrue(validate_description("docs: update guidance", body))

    def test_dependabot_version_updates_are_checked_for_the_title_only(self) -> None:
        notes = "Bumps [libc](https://github.com/rust-lang/libc) from 0.2.190 to 0.2.191.\n\n<details>Release notes</details>"
        self.assertEqual(DEPENDABOT, "dependabot[bot]")
        self.assertEqual(validate_description("deps: bump libc from 0.2.190 to 0.2.191", notes, DEPENDABOT), [])
        self.assertEqual(validate_description("ci: bump actions/setup-python from 7.0.0 to 7.1.0", notes, DEPENDABOT), [])
        # The title rule still holds, including for the scope Dependabot adds
        # when the commit-message configuration asks for one.
        self.assertTrue(validate_description("deps(deps): bump libc from 0.2.190 to 0.2.191", notes, DEPENDABOT))
        # Nobody else is exempt: the same body from a person or another bot fails.
        for author in (None, "", "wchwawa", "github-actions[bot]", "dependabot", "Dependabot[bot]"):
            with self.subTest(author=author):
                self.assertTrue(validate_description("deps: bump libc from 0.2.190 to 0.2.191", notes, author))

    def test_invalid_payload_and_empty_body_fail(self) -> None:
        for body in (None, "", 123, [], {}):
            self.assertTrue(validate_description("docs: update guidance", body))

    def test_cli_passes_valid_and_rejects_invalid_json(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "pr.json"
            command = [sys.executable, str(Path(__file__).with_name("pr_description.py")), "--pull-request-json", str(path)]
            for payload, expected in (
                ({"title": "docs: update guidance", "body": description()}, 0),
                ({"title": "docs: update guidance"}, 1),
                ([], 1),
                ({"title": "deps: bump libc from 0.2.190 to 0.2.191", "body": "Bumps libc.", "author": DEPENDABOT}, 0),
                ({"title": "deps: bump libc from 0.2.190 to 0.2.191", "body": "Bumps libc.", "author": "wchwawa"}, 1),
            ):
                path.write_text(json.dumps(payload), encoding="utf-8")
                self.assertEqual(subprocess.run(command, capture_output=True).returncode, expected)
            path.write_text("not JSON", encoding="utf-8")
            self.assertEqual(subprocess.run(command, capture_output=True).returncode, 1)
            path.unlink()
            self.assertEqual(subprocess.run(command, capture_output=True).returncode, 1)

    def test_dependabot_titles_follow_the_title_rule(self) -> None:
        repo = Path(__file__).resolve().parents[2]
        config = (repo / ".github/dependabot.yml").read_text()
        # Dependabot titles a PR "<prefix>(<scope>): ..." when the commit
        # message includes the scope, which the title rule refuses.
        self.assertNotIn("include:", config)
        prefixes = re.findall(r'(?m)^\s+prefix:\s*"([^"]+)"', config)
        self.assertTrue(prefixes)
        for prefix in prefixes:
            with self.subTest(prefix=prefix):
                self.assertEqual(validate_description(f"{prefix}: bump example from 1.0.0 to 1.0.1", "Bumps example.", DEPENDABOT), [])

    def test_template_and_contributor_example_match_the_policy(self) -> None:
        repo = Path(__file__).resolve().parents[2]
        template = (repo / ".github/pull_request_template.md").read_text()
        self.assertEqual(re.findall(r"^## (.+)$", template, re.M), list(SECTIONS))
        self.assertTrue(validate_description("docs: update guidance", template))
        guide = (repo / "CONTRIBUTING.md").read_text()
        example = re.search(r"```markdown\n(.*?)```", guide, re.S).group(1)
        self.assertEqual(validate_description("docs: update guidance", example), [])

    def test_metadata_workflow_uses_only_the_protected_policy(self) -> None:
        repo = Path(__file__).resolve().parents[2]
        workflow = (repo / ".github/workflows/pr-description.yml").read_text()
        self.assertIn("  pull_request_target:", workflow)
        self.assertIn("types: [opened, reopened, synchronize, edited, ready_for_review]", workflow)
        self.assertIn("github.event.pull_request.base.sha", workflow)
        self.assertIn("?ref=$POLICY_SHA", workflow)
        # The author comes from the API, which a PR cannot set; only the
        # policy's exact login is exempt from the body sections.
        self.assertIn("--jq '{title, body, author: .user.login}'", workflow)
        self.assertNotIn("actions/checkout", workflow)
        self.assertNotRegex(workflow, r"(?m)^  pull_request:")
        self.assertNotIn("github.event.pull_request.body", workflow)
        self.assertNotIn("github.event.pull_request.title", workflow)


if __name__ == "__main__":
    unittest.main()
