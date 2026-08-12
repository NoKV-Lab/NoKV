#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Tests for the source-built Homebrew release boundary."""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
REPOSITORY_ROOT = SCRIPT_DIR.parents[1]
sys.path.insert(0, str(SCRIPT_DIR))

import homebrew_source_release as release  # noqa: E402


HOLT_CHECKSUM = "c0e62dad7ce341d1e1995cc0034cb347d0e562e444b2354f8418410e2ba770e4"
REGISTRY = "registry+https://github.com/rust-lang/crates.io-index"


def run(*arguments: str, cwd: Path) -> str:
    completed = subprocess.run(
        arguments,
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def write(path: Path, contents: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(contents, encoding="utf-8")


def repository_fixture(root: Path) -> Path:
    write(
        root / "Cargo.toml",
        """\
[workspace]
members = ["crates/nokv"]
resolver = "2"

[workspace.dependencies]
nokv = { path = "crates/nokv", version = "1.0.0" }
holt = { version = "=0.8.4", default-features = false }
""",
    )
    write(
        root / "crates/nokv/Cargo.toml",
        """\
[package]
name = "nokv"
version = "1.0.0"
edition = "2021"
""",
    )
    write(root / "crates/nokv/src/main.rs", "fn main() {}\n")
    write(
        root / "Cargo.lock",
        f"""\
version = 4

[[package]]
name = "holt"
version = "0.8.4"
source = "{REGISTRY}"
checksum = "{HOLT_CHECKSUM}"

[[package]]
name = "nokv"
version = "1.0.0"
dependencies = [
 "holt",
]
""",
    )
    write(
        root / "crates/nokv-agent/workbench_contract_schema.json",
        json.dumps(
            {
                "schema": "nokv.workbench.mcp_input_schemas.v1",
                "inputSchemas": {
                    f"workbench_tool_{index:02d}": {"type": "object"}
                    for index in range(18)
                },
            }
        ),
    )
    write(root / "README.md", "fixture\n")
    run("git", "init", "-q", cwd=root)
    run("git", "config", "user.name", "NoKV Test", cwd=root)
    run("git", "config", "user.email", "nokv-test@example.invalid", cwd=root)
    run("git", "add", ".", cwd=root)
    run("git", "commit", "-q", "-m", "fixture", cwd=root)
    run("git", "tag", "v1.0.0", cwd=root)
    return root


class StableTagTest(unittest.TestCase):
    def test_accepts_only_canonical_stable_tags(self) -> None:
        for tag, version in {
            "v0.1.0": "0.1.0",
            "v1.0.0": "1.0.0",
            "v12.345.6789": "12.345.6789",
        }.items():
            with self.subTest(tag=tag):
                self.assertEqual(release.validate_stable_tag(tag), version)

    def test_rejects_ambiguous_or_executable_refs(self) -> None:
        invalid = [
            "1.0.0",
            "v1.0",
            "v1.0.0-alpha.1",
            "v1.0.0+build",
            "v01.0.0",
            "v1.00.0",
            "v1.0.00",
            "v1.0.0^{}",
            "refs/tags/v1.0.0",
            "v1.0.0/../main",
            "v1.0.0;echo owned",
            "v1.0.0$(id)",
            "v1.0.0`id`",
            "v1.0.0\nmain",
            "ｖ1.0.0",
            "v١.٠.٠",
            "",
        ]
        for tag in invalid:
            with self.subTest(tag=tag):
                with self.assertRaises(release.ReleaseError):
                    release.validate_stable_tag(tag)


class RepositoryValidationTest(unittest.TestCase):
    def test_tag_head_package_lock_and_holt_are_one_release(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = repository_fixture(Path(directory))
            metadata = release.collect_release(root, "v1.0.0")

        self.assertEqual(metadata.version, "1.0.0")
        self.assertEqual(metadata.tag, "v1.0.0")
        self.assertRegex(metadata.commit, r"^[0-9a-f]{40}$")
        self.assertRegex(metadata.tree, r"^[0-9a-f]{40}$")
        self.assertRegex(metadata.cargo_lock_sha256, r"^[0-9a-f]{64}$")
        self.assertEqual(metadata.holt.version, "0.8.4")
        self.assertEqual(metadata.holt.source, REGISTRY)
        self.assertEqual(metadata.holt.checksum, HOLT_CHECKSUM)
        self.assertEqual(metadata.workbench.tool_count, 18)

    def test_tag_must_resolve_to_head(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = repository_fixture(Path(directory))
            write(root / "after-tag", "drift\n")
            run("git", "add", "after-tag", cwd=root)
            run("git", "commit", "-q", "-m", "drift", cwd=root)
            with self.assertRaisesRegex(release.ReleaseError, "does not point to HEAD"):
                release.collect_release(root, "v1.0.0")

    def test_worktree_must_be_clean(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = repository_fixture(Path(directory))
            write(root / "untracked", "not released\n")
            with self.assertRaisesRegex(release.ReleaseError, "worktree is not clean"):
                release.collect_release(root, "v1.0.0")

    def test_package_and_lock_versions_must_match_tag(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = repository_fixture(Path(directory))
            package = root / "crates/nokv/Cargo.toml"
            package.write_text(
                package.read_text(encoding="utf-8").replace("1.0.0", "1.0.1"),
                encoding="utf-8",
            )
            run("git", "add", ".", cwd=root)
            run("git", "commit", "-q", "-m", "package drift", cwd=root)
            run("git", "tag", "v1.0.1", cwd=root)
            with self.assertRaisesRegex(release.ReleaseError, "workspace nokv dependency"):
                release.collect_release(root, "v1.0.1")

    def test_holt_manifest_pin_must_match_exact_lock_entry(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = repository_fixture(Path(directory))
            manifest = root / "Cargo.toml"
            manifest.write_text(
                manifest.read_text(encoding="utf-8").replace("=0.8.4", "=0.8.3"),
                encoding="utf-8",
            )
            run("git", "add", ".", cwd=root)
            run("git", "commit", "-q", "-m", "holt drift", cwd=root)
            run("git", "tag", "-f", "v1.0.0", cwd=root)
            with self.assertRaisesRegex(release.ReleaseError, "Holt manifest pin"):
                release.collect_release(root, "v1.0.0")

    def test_holt_lock_entry_must_be_unique_and_checksummed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = repository_fixture(Path(directory))
            lock = root / "Cargo.lock"
            lock.write_text(
                lock.read_text(encoding="utf-8")
                + "\n[[package]]\nname = \"holt\"\nversion = \"9.9.9\"\n",
                encoding="utf-8",
            )
            run("git", "add", ".", cwd=root)
            run("git", "commit", "-q", "-m", "duplicate holt", cwd=root)
            run("git", "tag", "-f", "v1.0.0", cwd=root)
            with self.assertRaisesRegex(release.ReleaseError, "exactly one Holt"):
                release.collect_release(root, "v1.0.0")


class SourceArchiveTest(unittest.TestCase):
    def test_archive_is_deterministic_complete_and_path_safe(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = repository_fixture(Path(directory) / "repo")
            output = Path(directory) / "dist"
            metadata = release.collect_release(root, "v1.0.0")
            first = release.create_source_archive(root, metadata, output / "first")
            second = release.create_source_archive(root, metadata, output / "second")

            first_bytes = first.read_bytes()
            second_bytes = second.read_bytes()
            self.assertEqual(first_bytes, second_bytes)
            self.assertEqual(
                hashlib.sha256(first_bytes).hexdigest(),
                hashlib.sha256(second_bytes).hexdigest(),
            )
            with tarfile.open(first, "r:gz") as archive:
                names = [member.name for member in archive.getmembers()]

        self.assertIn("nokv-1.0.0/Cargo.lock", names)
        self.assertIn("nokv-1.0.0/crates/nokv/Cargo.toml", names)
        self.assertTrue(
            all(name == "nokv-1.0.0" or name.startswith("nokv-1.0.0/") for name in names)
        )
        self.assertFalse(any(".git" in Path(name).parts for name in names))

    def test_manifest_and_formula_bind_exact_source_and_runtime_identity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = repository_fixture(Path(directory) / "repo")
            output = Path(directory) / "dist"
            metadata = release.collect_release(root, "v1.0.0")
            archive = release.create_source_archive(root, metadata, output)
            manifest = release.build_manifest(metadata, archive)
            formula = release.render_formula(
                manifest,
                "https://github.com/NoKV-Lab/NoKV/releases/download/"
                "v1.0.0/nokv-1.0.0-source.tar.gz",
            )
            archive_sha256 = hashlib.sha256(archive.read_bytes()).hexdigest()

        self.assertEqual(manifest["source"]["sha256"], archive_sha256)
        self.assertIn('class Nokv < Formula', formula)
        self.assertNotIn("Cask", formula)
        self.assertNotIn("on_arm", formula)
        self.assertNotIn("on_intel", formula)
        self.assertNotIn('  version "1.0.0"', formula)
        self.assertIn('depends_on "rust" => :build', formula)
        self.assertIn('depends_on "protobuf" => :build', formula)
        self.assertIn('"cargo", "install", *std_cargo_args(path: "crates/nokv")', formula)
        self.assertIn(metadata.commit, formula)
        self.assertIn(metadata.cargo_lock_sha256, formula)
        self.assertIn(HOLT_CHECKSUM, formula)
        self.assertIn('assert_equal 18, schema.fetch("tools").length', formula)

    def test_formula_rejects_github_generated_or_mismatched_sources(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = repository_fixture(Path(directory) / "repo")
            output = Path(directory) / "dist"
            metadata = release.collect_release(root, "v1.0.0")
            archive = release.create_source_archive(root, metadata, output)
            manifest = release.build_manifest(metadata, archive)

            invalid_urls = [
                "https://github.com/NoKV-Lab/NoKV/archive/refs/tags/v1.0.0.tar.gz",
                "https://github.com/NoKV-Lab/NoKV/releases/latest/download/"
                "nokv-1.0.0-source.tar.gz",
                "https://github.com/NoKV-Lab/NoKV/releases/download/v1.0.1/"
                "nokv-1.0.0-source.tar.gz",
            ]
            for url in invalid_urls:
                with self.subTest(url=url):
                    with self.assertRaises(release.ReleaseError):
                        release.render_formula(manifest, url)


class InstalledIdentityTest(unittest.TestCase):
    def test_installed_binary_must_match_every_release_identity(self) -> None:
        manifest = {
            "schema": release.MANIFEST_SCHEMA,
            "version": "1.0.0",
            "tag": "v1.0.0",
            "commit": "a" * 40,
            "cargo_lock_sha256": "b" * 64,
            "holt": {
                "version": "0.8.4",
                "source": REGISTRY,
                "checksum": HOLT_CHECKSUM,
            },
            "workbench": {
                "schema": "nokv.workbench.mcp_input_schemas.v1",
                "tool_count": 18,
            },
        }
        version = {
            "version": "1.0.0",
            "git_commit": "a" * 40,
            "cargo_lock_sha256": "b" * 64,
            "holt": manifest["holt"],
            "workbench_contract_schema": "nokv.workbench.mcp_input_schemas.v1",
            "workbench_tool_count": 18,
        }
        schema = {
            "schema": "nokv.workbench.mcp_input_schemas.v1",
            "tools": [{} for _ in range(18)],
        }

        release.verify_installed_identity(manifest, version, schema)
        for path, replacement in [
            (("version",), "1.0.1"),
            (("git_commit",), "c" * 40),
            (("cargo_lock_sha256",), "d" * 64),
            (("holt", "checksum"), "e" * 64),
            (("workbench_tool_count",), 17),
        ]:
            drifted = json.loads(json.dumps(version))
            target = drifted
            for key in path[:-1]:
                target = target[key]
            target[path[-1]] = replacement
            with self.subTest(path=path):
                with self.assertRaises(release.ReleaseError):
                    release.verify_installed_identity(manifest, drifted, schema)


class PrivateTapAndWorkflowTest(unittest.TestCase):
    def test_private_tap_marker_is_exact(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / ".nokv-tap.json"
            marker.write_text(
                json.dumps(
                    {
                        "schema": release.TAP_MARKER_SCHEMA,
                        "repository": "NoKV-Lab/homebrew-tap",
                        "visibility": "private",
                        "source_repository": "NoKV-Lab/NoKV",
                    }
                ),
                encoding="utf-8",
            )
            release.verify_tap_marker(marker)
            marker.write_text(
                marker.read_text(encoding="utf-8").replace("private", "public"),
                encoding="utf-8",
            )
            with self.assertRaises(release.ReleaseError):
                release.verify_tap_marker(marker)

    def test_workflow_keeps_untrusted_tag_away_from_shell_and_tokens(self) -> None:
        workflow = (
            REPOSITORY_ROOT / ".github/workflows/release-homebrew.yml"
        ).read_text(encoding="utf-8")
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

        self.assertNotIn("${{ inputs.tag }}", "\n".join(run_blocks))
        self.assertIn("persist-credentials: false", workflow)
        self.assertIn("macos-15-intel", workflow)
        self.assertIn("macos-15", workflow)
        self.assertIn("NoKV-Lab/homebrew-tap", workflow)
        self.assertIn("create-github-app-token", workflow)
        self.assertIn("gh pr create", workflow)
        self.assertNotIn("HOMEBREW_TAP_PAT", workflow)
        self.assertNotIn("archive/refs/tags", workflow)

        action_refs = [
            line.split("@", 1)[1].strip()
            for line in lines
            if line.strip().startswith("uses:")
        ]
        self.assertTrue(action_refs)
        for action_ref in action_refs:
            with self.subTest(action_ref=action_ref):
                self.assertRegex(action_ref, r"^[0-9a-f]{40}$")


if __name__ == "__main__":
    unittest.main()
