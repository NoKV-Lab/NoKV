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

from pr_description import SECTIONS, validate_description


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

    def test_invalid_payload_and_empty_body_fail(self) -> None:
        for body in (None, "", 123, [], {}):
            self.assertTrue(validate_description("docs: update guidance", body))

    def test_cli_passes_valid_and_rejects_invalid_json(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "pr.json"
            command = [sys.executable, str(Path(__file__).with_name("pr_description.py")), "--pull-request-json", str(path)]
            for payload, expected in (({"title": "docs: update guidance", "body": description()}, 0), ({"title": "docs: update guidance"}, 1), ([], 1)):
                path.write_text(json.dumps(payload), encoding="utf-8")
                self.assertEqual(subprocess.run(command, capture_output=True).returncode, expected)
            path.write_text("not JSON", encoding="utf-8")
            self.assertEqual(subprocess.run(command, capture_output=True).returncode, 1)
            path.unlink()
            self.assertEqual(subprocess.run(command, capture_output=True).returncode, 1)

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
        self.assertIn("--jq '{title, body}'", workflow)
        self.assertNotIn("actions/checkout", workflow)
        self.assertNotRegex(workflow, r"(?m)^  pull_request:")
        self.assertNotIn("github.event.pull_request.body", workflow)
        self.assertNotIn("github.event.pull_request.title", workflow)


if __name__ == "__main__":
    unittest.main()
