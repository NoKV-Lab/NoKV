#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import hashlib
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

import nokv_runtime as runtime  # noqa: E402


NOKV_REVISION = "a" * 40
HOLT_REVISION = "b" * 40
IDENTITY = runtime.SourceIdentity(
    schema=runtime.BUILD_INFO_SCHEMA,
    nokv_version="0.1.0",
    nokv_git_commit=NOKV_REVISION,
    source_dirty=False,
    cargo_lock_sha256="c" * 64,
    holt_crate_version="0.8.2",
    holt_git_commit=HOLT_REVISION,
)


class NokvRuntimeTest(unittest.TestCase):
    def make_project(self, root: Path) -> Path:
        project = root / "project"
        (project / ".lingtai").mkdir(parents=True)
        return project

    def make_binary(self, root: Path, content: bytes = b"candidate nokv\n") -> Path:
        binary = root / "nokv"
        binary.write_bytes(content)
        os.chmod(binary, 0o755)
        return binary

    def test_stage_runtime_is_content_addressed_and_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            project = self.make_project(root)
            binary = self.make_binary(root)

            first = runtime.stage_runtime(project, binary, IDENTITY)
            command_inode = first.command.stat().st_ino
            build_info = first.command.parent / "build-info.json"
            build_info_bytes = build_info.read_bytes()

            second = runtime.stage_runtime(project, binary, IDENTITY)

            self.assertEqual(second, first)
            self.assertEqual(second.command.stat().st_ino, command_inode)
            self.assertEqual(second.command.read_bytes(), binary.read_bytes())
            self.assertEqual(build_info.read_bytes(), build_info_bytes)
            self.assertFalse(list(first.command.parent.parent.glob(".nokv.*")))

    def test_candidate_change_between_hash_and_copy_is_rejected_cleanly(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            project = self.make_project(root)
            original = b"original candidate bytes\n"
            replacement = b"replacement candidate bytes\n"
            binary = self.make_binary(root, original)
            resolved_binary = binary.resolve()
            original_digest = hashlib.sha256(original).hexdigest()
            real_sha256_file = runtime.sha256_file
            changed = False

            def hash_then_change(path: Path) -> str:
                nonlocal changed
                digest = real_sha256_file(path)
                if Path(path) == resolved_binary and not changed:
                    binary.write_bytes(replacement)
                    changed = True
                return digest

            with mock.patch.object(
                runtime, "sha256_file", side_effect=hash_then_change
            ):
                with self.assertRaisesRegex(ValueError, "changed while staging"):
                    runtime.stage_runtime(project, binary, IDENTITY)

            revision_dir = project / ".lingtai" / "runtime" / "nokv" / NOKV_REVISION
            self.assertTrue(changed)
            self.assertFalse((revision_dir / original_digest).exists())
            self.assertFalse(list(revision_dir.glob(".nokv.*")))
            self.assertFalse(list(revision_dir.rglob("build-info.json")))

    def test_existing_symlink_in_managed_runtime_path_is_rejected(self):
        components = (".lingtai", "runtime", "nokv", "revision", "digest")
        for component in components:
            with (
                self.subTest(component=component),
                tempfile.TemporaryDirectory() as tmp,
            ):
                root = Path(tmp)
                project = root / "project"
                project.mkdir()
                external = root / "external"
                external.mkdir()
                binary = self.make_binary(root)
                digest = runtime.sha256_file(binary)

                if component == ".lingtai":
                    (project / ".lingtai").symlink_to(
                        external, target_is_directory=True
                    )
                else:
                    lingtai_root = project / ".lingtai"
                    lingtai_root.mkdir()
                    if component == "runtime":
                        (lingtai_root / "runtime").symlink_to(
                            external, target_is_directory=True
                        )
                    else:
                        runtime_root = lingtai_root / "runtime"
                        runtime_root.mkdir()
                        if component == "nokv":
                            (runtime_root / "nokv").symlink_to(
                                external, target_is_directory=True
                            )
                        else:
                            nokv_root = runtime_root / "nokv"
                            nokv_root.mkdir()
                            if component == "revision":
                                (nokv_root / NOKV_REVISION).symlink_to(
                                    external, target_is_directory=True
                                )
                            else:
                                revision_dir = nokv_root / NOKV_REVISION
                                revision_dir.mkdir()
                                (revision_dir / digest).symlink_to(
                                    external, target_is_directory=True
                                )

                with self.assertRaisesRegex(ValueError, "symlink component"):
                    runtime.stage_runtime(project, binary, IDENTITY)

                self.assertEqual(list(external.iterdir()), [])

    def test_existing_runtime_file_symlink_is_rejected_without_following_it(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            project = self.make_project(root)
            binary = self.make_binary(root)
            digest = runtime.sha256_file(binary)
            runtime_dir = (
                project / ".lingtai" / "runtime" / "nokv" / NOKV_REVISION / digest
            )
            runtime_dir.mkdir(parents=True)
            external = root / "external-nokv"
            external.write_bytes(b"do not replace\n")
            (runtime_dir / "nokv").symlink_to(external)

            with self.assertRaisesRegex(ValueError, "not a regular file"):
                runtime.stage_runtime(project, binary, IDENTITY)

            self.assertEqual(external.read_bytes(), b"do not replace\n")
            self.assertTrue((runtime_dir / "nokv").is_symlink())
            self.assertFalse(list((runtime_dir.parent).glob(".nokv.*")))

    def test_existing_build_info_symlink_is_rejected_without_following_it(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            project = self.make_project(root)
            binary = self.make_binary(root)
            staged = runtime.stage_runtime(project, binary, IDENTITY)
            build_info = staged.command.parent / "build-info.json"
            external = root / "external-build-info.json"
            external.write_text("do not replace\n", encoding="utf-8")
            build_info.unlink()
            build_info.symlink_to(external)

            with self.assertRaisesRegex(ValueError, "not a regular file"):
                runtime.stage_runtime(project, binary, IDENTITY)

            self.assertEqual(external.read_text(encoding="utf-8"), "do not replace\n")
            self.assertTrue(build_info.is_symlink())

    def test_managed_directory_swap_cannot_redirect_staging_writes(self):
        components = (".lingtai", "runtime", "nokv", "revision", "digest")
        for component in components:
            with (
                self.subTest(component=component),
                tempfile.TemporaryDirectory() as tmp,
            ):
                root = Path(tmp)
                project = self.make_project(root)
                binary = self.make_binary(root)
                digest = runtime.sha256_file(binary)
                names = {
                    ".lingtai": ".lingtai",
                    "runtime": "runtime",
                    "nokv": "nokv",
                    "revision": NOKV_REVISION,
                    "digest": digest,
                }
                external = root / f"external-{component}"
                external.mkdir()
                held = root / f"held-{component}"
                real_open_directory_at = runtime._open_directory_at
                swapped = False

                def open_then_swap(
                    directory_fd: int,
                    name: str,
                    path: Path,
                    *,
                    create: bool,
                ) -> int:
                    nonlocal swapped
                    descriptor = real_open_directory_at(
                        directory_fd, name, path, create=create
                    )
                    if name == names[component] and not swapped:
                        path.rename(held)
                        path.symlink_to(external, target_is_directory=True)
                        swapped = True
                    return descriptor

                with mock.patch.object(
                    runtime, "_open_directory_at", side_effect=open_then_swap
                ):
                    with self.assertRaisesRegex(
                        ValueError, "symlink component|changed while staging"
                    ):
                        runtime.stage_runtime(project, binary, IDENTITY)

                self.assertTrue(swapped)
                self.assertEqual(list(external.iterdir()), [])
                self.assertFalse(list(external.rglob("nokv")))
                self.assertFalse(list(external.rglob("build-info.json")))

    def test_holt_lock_source_with_deceptive_host_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            source_root = Path(tmp)
            (source_root / "Cargo.toml").write_text(
                "[workspace.dependencies]\n"
                f'holt = {{ git = "https://github.com/NoKV-Lab/holt.git", '
                f'rev = "{HOLT_REVISION}" }}\n',
                encoding="utf-8",
            )
            (source_root / "Cargo.lock").write_text(
                "[[package]]\n"
                'name = "holt"\n'
                'version = "0.8.2"\n'
                'source = "git+https://evil.invalid/'
                f'github.com/NoKV-Lab/holt.git#{HOLT_REVISION}"\n',
                encoding="utf-8",
            )
            subprocess.run(["git", "init", "-q"], cwd=source_root, check=True)
            subprocess.run(["git", "add", "."], cwd=source_root, check=True)
            subprocess.run(
                [
                    "git",
                    "-c",
                    "user.name=NoKV Test",
                    "-c",
                    "user.email=nokv-test@example.invalid",
                    "commit",
                    "-q",
                    "-m",
                    "test fixture",
                ],
                cwd=source_root,
                check=True,
            )
            revision = subprocess.check_output(
                ["git", "rev-parse", "HEAD"], cwd=source_root, text=True
            ).strip()

            with self.assertRaisesRegex(ValueError, "not pinned to NoKV-Lab/holt"):
                runtime.source_identity(source_root, revision=revision)

    def test_source_identity_requires_a_real_git_checkout(self):
        with tempfile.TemporaryDirectory() as tmp:
            source_root = Path(tmp)
            (source_root / "crates/nokv").mkdir(parents=True)
            (source_root / "Cargo.toml").write_text(
                "[workspace.dependencies]\n"
                f'holt = {{ git = "https://github.com/NoKV-Lab/holt.git", '
                f'rev = "{HOLT_REVISION}" }}\n',
                encoding="utf-8",
            )
            (source_root / "Cargo.lock").write_text(
                "[[package]]\n"
                'name = "holt"\n'
                'version = "0.8.2"\n'
                'source = "git+https://github.com/NoKV-Lab/holt.git'
                f'?rev={HOLT_REVISION}#{HOLT_REVISION}"\n',
                encoding="utf-8",
            )
            (source_root / "crates/nokv/Cargo.toml").write_text(
                '[package]\nname = "nokv"\nversion = "0.1.0"\n',
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "requires a git checkout"):
                runtime.source_identity(source_root, revision=NOKV_REVISION)


if __name__ == "__main__":
    unittest.main()
