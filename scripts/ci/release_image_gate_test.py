#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Local, credential-free regression tests for the image release gate."""

from __future__ import annotations

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


GATE = Path(__file__).with_name("release_image_gate.py")
WORKFLOW = Path(__file__).resolve().parents[2] / ".github/workflows/release-images.yml"


def git(*arguments: str, cwd: Path | None = None) -> str:
    result = subprocess.run(
        ["git", *arguments], cwd=cwd, check=True, capture_output=True, text=True
    )
    return result.stdout.strip()


class ReleaseImageGateTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="nokv-image-release-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.remote = self.root / "remote.git"
        self.author = self.root / "author"
        self.tooling = self.root / "tooling"
        self.source = self.root / "source"
        self.output = self.root / "outputs"

        git("init", "--bare", str(self.remote))
        git("init", "--initial-branch=main", str(self.author))
        git("config", "user.name", "NoKV Release Test", cwd=self.author)
        git(
            "config", "user.email", "nokv-release-test@example.invalid", cwd=self.author
        )
        package = self.author / "crates/nokv/Cargo.toml"
        package.parent.mkdir(parents=True)
        package.write_text('[package]\nname = "nokv"\nversion = "1.2.3"\n')
        git("add", ".", cwd=self.author)
        git("commit", "-m", "Test release source", cwd=self.author)
        self.commit = git("rev-parse", "HEAD", cwd=self.author)
        git("tag", "-a", "v1.2.3", "-m", "Test release v1.2.3", cwd=self.author)
        git("remote", "add", "origin", str(self.remote), cwd=self.author)
        git("push", "origin", "main", "v1.2.3", cwd=self.author)
        git("clone", "--branch", "main", str(self.remote), str(self.tooling))
        git("clone", "--branch", "main", str(self.remote), str(self.source))

    def gate(self, command: str, *arguments: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                sys.executable,
                str(GATE),
                command,
                "--repository",
                str(self.tooling),
                "--remote",
                str(self.remote),
                *arguments,
            ],
            capture_output=True,
            text=True,
        )

    def resolve(
        self,
        *,
        tag: str = "v1.2.3",
        rust: str = "1.88",
        debian: str = "bookworm",
    ) -> subprocess.CompletedProcess[str]:
        return self.gate(
            "resolve",
            "--tag",
            tag,
            "--rust-version",
            rust,
            "--debian-version",
            debian,
            "--output",
            str(self.output),
        )

    def verify(self, expected: str | None = None) -> subprocess.CompletedProcess[str]:
        return self.gate(
            "verify",
            "--source",
            str(self.source),
            "--tag",
            "v1.2.3",
            "--expected-commit",
            expected or self.commit,
        )

    def test_valid_release_inputs_bind_to_exact_commit(self) -> None:
        resolved = self.resolve()
        self.assertEqual(resolved.returncode, 0, resolved.stderr)
        outputs = dict(
            line.split("=", 1) for line in self.output.read_text().splitlines()
        )
        self.assertEqual(outputs["tag"], "v1.2.3")
        self.assertEqual(outputs["version"], "1.2.3")
        self.assertEqual(outputs["commit"], self.commit)
        self.assertEqual(outputs["rust_version"], "1.88")
        self.assertEqual(outputs["debian_version"], "bookworm")
        verified = self.verify()
        self.assertEqual(verified.returncode, 0, verified.stderr)

    def test_shell_payloads_do_not_execute_or_resolve(self) -> None:
        marker = self.root / "executed"
        payload = f'v1.2.3"; touch "{marker}"; #'
        for tag, rust, debian in (
            (payload, "1.88", "bookworm"),
            ("v01.2.3", "1.88", "bookworm"),
            ("v1.2.3-rc1", "1.88", "bookworm"),
            ("main", "1.88", "bookworm"),
            ("v1.2.3", f"1.88\nNOKV_BINARY=$(touch {marker})", "bookworm"),
            ("v1.2.3", "1.88", f"bookworm\n$(touch {marker})"),
        ):
            with self.subTest(tag=tag, rust=rust, debian=debian):
                result = self.resolve(tag=tag, rust=rust, debian=debian)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(marker.exists())
                self.assertFalse(self.output.exists())

    def test_tag_must_be_on_canonical_main(self) -> None:
        git("switch", "-c", "unreviewed", cwd=self.author)
        (self.author / "unreviewed").write_text("unreviewed")
        git("add", "unreviewed", cwd=self.author)
        git("commit", "-m", "Unreviewed change", cwd=self.author)
        git("tag", "v1.2.4", cwd=self.author)
        git("push", "origin", "v1.2.4", cwd=self.author)
        result = self.resolve(tag="v1.2.4")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("not on canonical main", result.stderr)

    def test_tag_move_fails_revalidation(self) -> None:
        self.assertEqual(self.resolve().returncode, 0)
        package = self.author / "crates/nokv/Cargo.toml"
        package.write_text('[package]\nname = "nokv"\nversion = "1.2.3"\n# moved\n')
        git("add", ".", cwd=self.author)
        git("commit", "-m", "Move release tag", cwd=self.author)
        git("tag", "--force", "v1.2.3", cwd=self.author)
        git("push", "origin", "main", cwd=self.author)
        git("push", "--force", "origin", "v1.2.3", cwd=self.author)
        result = self.verify()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("release tag moved", result.stderr)

    def test_checkout_must_match_validated_commit(self) -> None:
        self.assertEqual(self.resolve().returncode, 0)
        git("config", "user.name", "NoKV Release Test", cwd=self.source)
        git(
            "config", "user.email", "nokv-release-test@example.invalid", cwd=self.source
        )
        (self.source / "changed").write_text("unexpected checkout")
        git("add", "changed", cwd=self.source)
        git("commit", "-m", "Unexpected source", cwd=self.source)
        result = self.verify()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("source checkout is", result.stderr)

    def test_source_version_must_match_tag(self) -> None:
        self.assertEqual(self.resolve().returncode, 0)
        package = self.author / "crates/nokv/Cargo.toml"
        package.write_text('[package]\nname = "nokv"\nversion = "9.9.9"\n')
        git("add", ".", cwd=self.author)
        git("commit", "-m", "Wrong package version", cwd=self.author)
        changed = git("rev-parse", "HEAD", cwd=self.author)
        git("tag", "--force", "v1.2.3", cwd=self.author)
        git("push", "origin", "main", cwd=self.author)
        git("push", "--force", "origin", "v1.2.3", cwd=self.author)
        git("fetch", "origin", "main", cwd=self.source)
        git("checkout", "--detach", changed, cwd=self.source)
        result = self.verify(expected=changed)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("package version", result.stderr)

    def test_workflow_routes_inputs_only_through_environment(self) -> None:
        workflow = WORKFLOW.read_text()
        input_lines = [
            line.strip() for line in workflow.splitlines() if "${{ inputs." in line
        ]
        self.assertEqual(
            input_lines,
            [
                "RELEASE_TAG: ${{ inputs.tag }}",
                "RUST_VERSION: ${{ inputs.rust_version }}",
                "DEBIAN_VERSION: ${{ inputs.debian_version }}",
            ],
        )
        self.assertIn("ref: ${{ needs.validate.outputs.commit }}", workflow)
        self.assertEqual(workflow.count("persist-credentials: false"), 4)


if __name__ == "__main__":
    unittest.main()
