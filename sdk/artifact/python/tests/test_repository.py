# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from mlflow.exceptions import MlflowException

from nokv_mlflow_artifact import LocalArtifactStore, NoKVArtifactRepository

_CONFIGURED_STORE_ROOT: Path | None = None


def _configured_store_factory(
    *,
    artifact_uri: str,
    tracking_uri: str | None = None,
    registry_uri: str | None = None,
) -> LocalArtifactStore:
    if _CONFIGURED_STORE_ROOT is None:
        raise AssertionError("test store root is not configured")
    if artifact_uri != "nokv://tenant/run":
        raise AssertionError(f"unexpected artifact URI: {artifact_uri}")
    if tracking_uri != "tracking://local":
        raise AssertionError(f"unexpected tracking URI: {tracking_uri}")
    if registry_uri != "registry://local":
        raise AssertionError(f"unexpected registry URI: {registry_uri}")
    return LocalArtifactStore(_CONFIGURED_STORE_ROOT)


class NoKVArtifactRepositoryTest(unittest.TestCase):
    def test_log_list_download_and_delete_file(self) -> None:
        with tempfile.TemporaryDirectory() as workspace:
            workspace_path = Path(workspace)
            store = LocalArtifactStore(workspace_path / "store")
            repo = NoKVArtifactRepository("nokv://tenant/run", store=store)

            local_file = workspace_path / "metrics.json"
            local_file.write_text('{"accuracy": 0.98}', encoding="utf-8")

            repo.log_artifact(str(local_file), "runs/run-1")

            root_entries = repo.list_artifacts()
            self.assertEqual([(entry.path, entry.is_dir) for entry in root_entries], [("runs", True)])

            run_entries = repo.list_artifacts("runs/run-1")
            self.assertEqual(len(run_entries), 1)
            self.assertEqual(run_entries[0].path, "runs/run-1/metrics.json")
            self.assertFalse(run_entries[0].is_dir)
            self.assertEqual(run_entries[0].file_size, local_file.stat().st_size)

            download_dir = workspace_path / "downloads"
            download_dir.mkdir()
            downloaded = repo.download_artifacts("runs/run-1/metrics.json", dst_path=str(download_dir))
            self.assertEqual(Path(downloaded).read_text(encoding="utf-8"), '{"accuracy": 0.98}')

            repo.delete_artifacts("runs/run-1/metrics.json")
            self.assertEqual(repo.list_artifacts("runs/run-1"), [])

    def test_log_artifacts_preserves_relative_paths(self) -> None:
        with tempfile.TemporaryDirectory() as workspace:
            workspace_path = Path(workspace)
            source = workspace_path / "source"
            nested = source / "nested"
            nested.mkdir(parents=True)
            (source / "root.txt").write_text("root", encoding="utf-8")
            (nested / "child.txt").write_text("child", encoding="utf-8")

            repo = NoKVArtifactRepository(
                "nokv://tenant/run",
                store=LocalArtifactStore(workspace_path / "store"),
            )

            repo.log_artifacts(str(source), "bundle")

            self.assertEqual(
                [(entry.path, entry.is_dir) for entry in repo.list_artifacts("bundle")],
                [("bundle/nested", True), ("bundle/root.txt", False)],
            )
            self.assertEqual(
                [(entry.path, entry.is_dir) for entry in repo.list_artifacts("bundle/nested")],
                [("bundle/nested/child.txt", False)],
            )

            download_dir = workspace_path / "downloads"
            download_dir.mkdir()
            downloaded = Path(repo.download_artifacts("bundle", dst_path=str(download_dir)))
            self.assertEqual((downloaded / "root.txt").read_text(encoding="utf-8"), "root")
            self.assertEqual((downloaded / "nested" / "child.txt").read_text(encoding="utf-8"), "child")

    def test_delete_directory_recursively_removes_children(self) -> None:
        with tempfile.TemporaryDirectory() as workspace:
            workspace_path = Path(workspace)
            repo = NoKVArtifactRepository(
                "nokv://tenant/run",
                store=LocalArtifactStore(workspace_path / "store"),
            )
            local_file = workspace_path / "model.bin"
            local_file.write_bytes(b"weights")
            repo.log_artifact(str(local_file), "models/latest")
            nested_file = workspace_path / "metadata.txt"
            nested_file.write_text("metadata", encoding="utf-8")
            repo.log_artifact(str(nested_file), "models/latest/nested")

            repo.delete_artifacts("models/latest")

            self.assertEqual(repo.list_artifacts("models/latest"), [])
            self.assertEqual(repo.list_artifacts("models"), [])

    def test_delete_root_clears_children(self) -> None:
        with tempfile.TemporaryDirectory() as workspace:
            workspace_path = Path(workspace)
            repo = NoKVArtifactRepository(
                "nokv://tenant/run",
                store=LocalArtifactStore(workspace_path / "store"),
            )
            first = workspace_path / "first.txt"
            first.write_text("first", encoding="utf-8")
            second = workspace_path / "second.txt"
            second.write_text("second", encoding="utf-8")
            repo.log_artifact(str(first), "root")
            repo.log_artifact(str(second), "root/nested")

            repo.delete_artifacts()

            self.assertEqual(repo.list_artifacts(), [])

    def test_nokv_file_uri_uses_local_development_store(self) -> None:
        with tempfile.TemporaryDirectory() as workspace:
            workspace_path = Path(workspace)
            store_root = workspace_path / "store"
            repo = NoKVArtifactRepository(f"nokv+file://{store_root}")
            local_file = workspace_path / "params.txt"
            local_file.write_text("lr=0.1", encoding="utf-8")

            repo.log_artifact(str(local_file), "trial")

            self.assertEqual(
                [(entry.path, entry.is_dir, entry.file_size) for entry in repo.list_artifacts("trial")],
                [("trial/params.txt", False, len("lr=0.1"))],
            )

    def test_nokv_uri_uses_configured_store_factory(self) -> None:
        global _CONFIGURED_STORE_ROOT

        old_factory = os.environ.get("NOKV_ARTIFACT_STORE_FACTORY")
        with tempfile.TemporaryDirectory() as workspace:
            workspace_path = Path(workspace)
            _CONFIGURED_STORE_ROOT = workspace_path / "store"
            os.environ["NOKV_ARTIFACT_STORE_FACTORY"] = f"{__name__}:_configured_store_factory"
            try:
                repo = NoKVArtifactRepository(
                    "nokv://tenant/run",
                    tracking_uri="tracking://local",
                    registry_uri="registry://local",
                )
                local_file = workspace_path / "artifact.txt"
                local_file.write_text("configured", encoding="utf-8")

                repo.log_artifact(str(local_file), "factory")

                self.assertEqual(
                    [(entry.path, entry.is_dir) for entry in repo.list_artifacts("factory")],
                    [("factory/artifact.txt", False)],
                )
            finally:
                _CONFIGURED_STORE_ROOT = None
                if old_factory is None:
                    os.environ.pop("NOKV_ARTIFACT_STORE_FACTORY", None)
                else:
                    os.environ["NOKV_ARTIFACT_STORE_FACTORY"] = old_factory

    def test_rejects_non_canonical_artifact_paths(self) -> None:
        with tempfile.TemporaryDirectory() as workspace:
            repo = NoKVArtifactRepository(
                "nokv://tenant/run",
                store=LocalArtifactStore(Path(workspace) / "store"),
            )

            for path in ("/absolute", "../escape", "nested//child", "nested/"):
                with self.subTest(path=path):
                    with self.assertRaises(MlflowException):
                        repo.list_artifacts(path)


if __name__ == "__main__":
    unittest.main()
