#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Tests for the Python SDK wheel release boundary."""

from __future__ import annotations

import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
REPOSITORY_ROOT = SCRIPT_DIR.parents[1]
sys.path.insert(0, str(SCRIPT_DIR))

import python_sdk_release as release  # noqa: E402

COMMIT = "0123456789abcdef0123456789abcdef01234567"


def _write_repository(root: Path, *, cli: str, python: str, static_version: bool = False) -> None:
    (root / "crates/nokv").mkdir(parents=True)
    (root / "crates/nokv-python").mkdir(parents=True)
    (root / "crates/nokv/Cargo.toml").write_text(
        f'[package]\nname = "nokv"\nversion = "{cli}"\n'
    )
    (root / "crates/nokv-python/Cargo.toml").write_text(
        f'[package]\nname = "nokv-python"\nversion = "{python}"\npublish = false\n'
    )
    project = (
        '[project]\nname = "nokv"\nversion = "9.9.9"\n'
        if static_version
        else '[project]\nname = "nokv"\ndynamic = ["version"]\n'
    )
    (root / "crates/nokv-python/pyproject.toml").write_text(project)


def _write_wheels(dist: Path, version: str, platforms: list[str]) -> None:
    dist.mkdir(parents=True, exist_ok=True)
    for platform in platforms:
        (dist / f"nokv-{version}-cp39-abi3-{platform}.whl").write_bytes(
            f"wheel-{platform}".encode()
        )


FULL_SET = [
    "manylinux_2_28_x86_64",
    "manylinux_2_28_aarch64",
    "macosx_11_0_arm64",
    "macosx_10_12_x86_64",
]


class StableTagTest(unittest.TestCase):
    def test_accepts_only_canonical_stable_tags(self) -> None:
        self.assertEqual(release.validate_stable_tag("v0.11.0"), "0.11.0")
        for tag in ("0.11.0", "v0.11", "v01.2.3", "v1.0.0-rc.1", "latest", "v1.0.0\n"):
            with self.subTest(tag=tag), self.assertRaises(release.ReleaseError):
                release.validate_stable_tag(tag)


class VersionAgreementTest(unittest.TestCase):
    def test_python_version_must_follow_the_cli_release_line(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _write_repository(root, cli="0.11.0", python="0.11.0")
            self.assertEqual(release.validate_version(root, "v0.11.0"), "0.11.0")
            with self.assertRaises(release.ReleaseError):
                release.validate_version(root, "v0.11.1")

    def test_diverged_python_version_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _write_repository(root, cli="0.11.0", python="0.1.0")
            with self.assertRaises(release.ReleaseError):
                release.sdk_version(root)

    def test_static_pyproject_version_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _write_repository(root, cli="0.11.0", python="0.11.0", static_version=True)
            with self.assertRaises(release.ReleaseError):
                release.sdk_version(root)

    def test_checked_in_repository_declares_one_release_line(self) -> None:
        version = release.sdk_version(REPOSITORY_ROOT)
        self.assertRegex(version, r"^\d+\.\d+\.\d+$")


class WheelSetTest(unittest.TestCase):
    def test_exact_platform_set_is_required(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            dist = Path(directory) / "dist"
            _write_wheels(dist, "0.11.0", FULL_SET)
            wheels = release.classify_wheels(dist, "0.11.0")
            self.assertEqual(sorted(wheels), sorted(release.EXPECTED_PLATFORMS))

            (dist / "nokv-0.11.0-cp39-abi3-macosx_10_12_x86_64.whl").unlink()
            with self.assertRaisesRegex(release.ReleaseError, "missing wheels for: macos-x86_64"):
                release.classify_wheels(dist, "0.11.0")

    def test_foreign_duplicate_and_wrong_version_wheels_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            dist = Path(directory) / "dist"
            _write_wheels(dist, "0.11.0", FULL_SET)
            (dist / "notes.txt").write_text("stray")
            with self.assertRaisesRegex(release.ReleaseError, "unexpected file"):
                release.classify_wheels(dist, "0.11.0")
            (dist / "notes.txt").unlink()

            _write_wheels(dist, "0.11.0", ["macosx_12_0_arm64"])
            with self.assertRaisesRegex(release.ReleaseError, "duplicate wheel"):
                release.classify_wheels(dist, "0.11.0")
            (dist / "nokv-0.11.0-cp39-abi3-macosx_12_0_arm64.whl").unlink()

            _write_wheels(dist, "0.11.1", ["manylinux_2_28_x86_64"])
            with self.assertRaisesRegex(release.ReleaseError, "is version '0.11.1'"):
                release.classify_wheels(dist, "0.11.0")
            (dist / "nokv-0.11.1-cp39-abi3-manylinux_2_28_x86_64.whl").unlink()

            (dist / "nokv-0.11.0-cp312-cp312-manylinux_2_28_x86_64.whl").write_bytes(b"x")
            with self.assertRaisesRegex(release.ReleaseError, "unexpected file"):
                release.classify_wheels(dist, "0.11.0")
            (dist / "nokv-0.11.0-cp312-cp312-manylinux_2_28_x86_64.whl").unlink()

            (dist / "nokv-0.11.0-cp39-abi3-win_amd64.whl").write_bytes(b"x")
            with self.assertRaisesRegex(release.ReleaseError, "not a release target"):
                release.classify_wheels(dist, "0.11.0")

    def test_manifest_and_checksums_bind_exact_wheel_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            dist = root / "dist"
            _write_wheels(dist, "0.11.0", FULL_SET)
            manifest_path, checksums_path = release.write_release_assets(
                dist=dist, version="0.11.0", tag="v0.11.0", commit=COMMIT, output=root / "out"
            )
            manifest = json.loads(manifest_path.read_text())
            self.assertEqual(manifest["schema"], release.MANIFEST_SCHEMA)
            self.assertEqual(manifest["commit"], COMMIT)
            self.assertEqual(manifest["api_version"], 1)
            self.assertEqual(sorted(manifest["wheels"]), sorted(release.EXPECTED_PLATFORMS))
            expected = hashlib.sha256(b"wheel-macosx_11_0_arm64").hexdigest()
            self.assertEqual(manifest["wheels"]["macos-arm64"]["sha256"], expected)
            self.assertIn(f"{expected}  nokv-0.11.0-cp39-abi3-macosx_11_0_arm64.whl", checksums_path.read_text())
            release.verify_manifest_wheels(manifest_path, dist)

            (dist / "nokv-0.11.0-cp39-abi3-macosx_11_0_arm64.whl").write_bytes(b"tampered")
            with self.assertRaisesRegex(release.ReleaseError, "differs from manifest"):
                release.verify_manifest_wheels(manifest_path, dist)

    def test_manifest_rejects_non_canonical_commit_or_tag(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            dist = Path(directory) / "dist"
            _write_wheels(dist, "0.11.0", FULL_SET)
            wheels = release.classify_wheels(dist, "0.11.0")
            with self.assertRaises(release.ReleaseError):
                release.build_manifest(version="0.11.0", tag="v0.11.0", commit="abc", wheels=wheels)
            with self.assertRaises(release.ReleaseError):
                release.build_manifest(version="0.11.0", tag="v0.11.1", commit=COMMIT, wheels=wheels)


class InstalledSdkTest(unittest.TestCase):
    def test_wrong_version_or_missing_distribution_fails_closed(self) -> None:
        # Whether or not the running interpreter can import `nokv`, it cannot
        # be an installed wheel of this impossible version.
        with self.assertRaises(release.ReleaseError):
            release.verify_installed_sdk(Path(sys.executable), "0.0.0")

    def test_missing_interpreter_is_a_release_error(self) -> None:
        with self.assertRaises(release.ReleaseError):
            release.verify_installed_sdk(Path("/nonexistent/python-interpreter"), "0.11.0")


class WorkflowTest(unittest.TestCase):
    def _run_blocks(self, workflow: str) -> str:
        lines = workflow.splitlines()
        run_blocks: list[str] = []
        index = 0
        while index < len(lines):
            line = lines[index]
            if line.lstrip().startswith("run: |"):
                indent = len(line) - len(line.lstrip())
                block: list[str] = []
                index += 1
                while index < len(lines):
                    candidate = lines[index]
                    if candidate.strip() and len(candidate) - len(candidate.lstrip()) <= indent:
                        break
                    block.append(candidate)
                    index += 1
                run_blocks.append("\n".join(block))
                continue
            index += 1
        return "\n".join(run_blocks)

    def test_release_workflow_keeps_untrusted_tag_away_from_shell_and_pins_actions(self) -> None:
        workflow = (
            REPOSITORY_ROOT / ".github/workflows/release-python-sdk.yml"
        ).read_text(encoding="utf-8")
        lines = workflow.splitlines()
        self.assertNotIn("${{ inputs.tag }}", self._run_blocks(workflow))
        self.assertIn("persist-credentials: false", workflow)
        for runner in ("ubuntu-latest", "ubuntu-24.04-arm", "macos-15", "macos-15-intel"):
            with self.subTest(runner=runner):
                self.assertIn(runner, workflow)
        self.assertIn('manylinux: "2_28"', workflow)
        self.assertIn("before-script-linux", workflow)
        self.assertIn("python_sdk_release.py validate-version", workflow)
        self.assertIn("python_sdk_release.py write-assets", workflow)
        self.assertIn("python_sdk_release.py verify-install", workflow)
        self.assertIn("--verify-tag", workflow)
        # The publish step runs outside the checkout (cd "$RELEASE_DIR"), so gh
        # must learn the repository from the environment, not from git context.
        self.assertIn("GH_REPO: ${{ github.repository }}", workflow)
        self.assertIn("Release tag moved", workflow)
        self.assertIn("release asset already exists with different bytes", workflow)
        self.assertNotIn("archive/refs/tags", workflow)
        self.assertNotIn("--clobber", workflow)

        action_refs = [
            line.split("@", 1)[1].strip()
            for line in lines
            if line.strip().startswith("uses:")
        ]
        self.assertTrue(action_refs)
        for action_ref in action_refs:
            with self.subTest(action_ref=action_ref):
                self.assertRegex(action_ref, r"^[0-9a-f]{40}$")

    def test_pull_request_workflow_installs_the_built_wheel_before_testing(self) -> None:
        workflow = (REPOSITORY_ROOT / ".github/workflows/python-sdk.yml").read_text(
            encoding="utf-8"
        )
        self.assertIn("pull_request", workflow)
        self.assertIn("manylinux: 2_28", workflow)
        self.assertIn("python_sdk_release.py verify-install", workflow)
        self.assertIn("crates/nokv-python/tests", workflow)
        self.assertNotIn("maturin develop", workflow)
        for line in workflow.splitlines():
            if line.strip().startswith("uses:"):
                with self.subTest(line=line.strip()):
                    self.assertRegex(line.split("@", 1)[1].strip(), r"^[0-9a-f]{40}$")


if __name__ == "__main__":
    unittest.main()
