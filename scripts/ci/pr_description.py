#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0
"""Validate the repository's PR title and description structure."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path


SECTIONS = (
    "TLDR", "Problem", "Consequences", "Example", "Solution", "Gain",
    "Tradeoffs", "Validation",
)
TITLE = re.compile(r"[a-z][a-z0-9-]*: \S.*")
PLACEHOLDERS = {"todo", "tbd", "na", "none", "placeholder"}
# Dependabot writes its version-update PR bodies itself (release notes and
# commits) and cannot be asked for the eight sections. The login comes from
# the pull request API, which a PR author cannot set.
DEPENDABOT = "dependabot[bot]"


def validate_description(title: object, body: object, author: object = None) -> list[str]:
    """Return actionable errors without echoing untrusted PR content."""
    errors = []
    if not isinstance(title, str) or not TITLE.fullmatch(title.strip()):
        errors.append("Title must use <type>: <outcome>, for example chore: enforce PR descriptions.")
    if author == DEPENDABOT:
        return errors
    if not isinstance(body, str) or not body.strip():
        return errors + ["PR body must contain all required sections with content."]

    body = re.sub(r"<!--.*?(?:-->|\Z)", "", body, flags=re.S)
    sections: list[tuple[str, list[str]]] = []
    preamble = []
    fence_character = ""
    fence_length = 0
    for line in body.splitlines():
        fence = re.match(r"^ {0,3}(`{3,}|~{3,})(.*)$", line)
        if fence:
            marker, suffix = fence.groups()
            if not fence_character:
                fence_character, fence_length = marker[0], len(marker)
            elif marker[0] == fence_character and len(marker) >= fence_length and not suffix.strip():
                fence_character = ""
            (sections[-1][1] if sections else preamble).append(line)
            continue
        heading = re.match(r"^ {0,3}##\s+(.+?)\s*#*\s*$", line) if not fence_character else None
        if heading:
            sections.append((heading.group(1), []))
        else:
            (sections[-1][1] if sections else preamble).append(line)

    if [name for name, _ in sections] != list(SECTIONS):
        errors.append("Use exactly these ## headings in order: " + ", ".join(SECTIONS) + ".")
    if any(line.strip() for line in preamble):
        errors.append("Start the PR body with ## TLDR; put issue links under Problem.")
    if fence_character:
        errors.append("Close every Markdown code fence.")
    for name, lines in sections:
        if name not in SECTIONS:
            continue
        content = "\n".join(line for line in lines if not re.match(r"^ {0,3}(`{3,}|~{3,})", line))
        words = re.sub(r"[\W_]+", "", content.casefold())
        if not words or words in PLACEHOLDERS:
            errors.append(f"{name} needs content; explain briefly when it does not apply.")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pull-request-json", required=True, type=Path,
                        help="JSON object containing title, body and optionally the author's login, "
                             "obtained with gh pr view or gh api")
    args = parser.parse_args()
    try:
        payload = json.loads(args.pull_request_json.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("expected a JSON object")
    except (OSError, ValueError) as error:
        print(f"PR description input could not be read: {error}", file=sys.stderr)
        return 1
    errors = validate_description(payload.get("title"), payload.get("body"), payload.get("author"))
    for error in errors:
        print(error, file=sys.stderr)
    if errors:
        return 1
    print("PR title and all eight description sections are valid.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
