# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from pathlib import Path

import pytest
from mlflow.exceptions import MlflowException
from mlflow.store.artifact.artifact_repository_registry import get_artifact_repository

from nokv_mlflow_artifact import NoKVArtifactRepository


@pytest.fixture
def nokv_artifact_repo(tmp_path: Path) -> NoKVArtifactRepository:
    return get_artifact_repository(f"nokv+file://{tmp_path / 'store'}")


def test_nokv_file_entrypoint_resolves_repository(tmp_path: Path) -> None:
    repo = get_artifact_repository(f"nokv+file://{tmp_path / 'store'}")

    assert isinstance(repo, NoKVArtifactRepository)


def test_log_list_and_download_artifacts(nokv_artifact_repo: NoKVArtifactRepository, tmp_path: Path) -> None:
    source = tmp_path / "source"
    nested = source / "nested"
    nested.mkdir(parents=True)
    (source / "a.txt").write_text("A", encoding="utf-8")
    (source / "b.txt").write_text("B", encoding="utf-8")
    (nested / "c.txt").write_text("C", encoding="utf-8")

    nokv_artifact_repo.log_artifacts(str(source))

    assert [(entry.path, entry.is_dir) for entry in nokv_artifact_repo.list_artifacts()] == [
        ("a.txt", False),
        ("b.txt", False),
        ("nested", True),
    ]
    assert [
        (entry.path, entry.is_dir) for entry in nokv_artifact_repo.list_artifacts("nested")
    ] == [("nested/c.txt", False)]

    with open(nokv_artifact_repo.download_artifacts("a.txt"), encoding="utf-8") as file:
        assert file.read() == "A"

    downloaded_nested = Path(nokv_artifact_repo.download_artifacts("nested"))
    assert downloaded_nested.name == "nested"
    assert (downloaded_nested / "c.txt").read_text(encoding="utf-8") == "C"

    downloaded_root = Path(nokv_artifact_repo.download_artifacts(""))
    assert (downloaded_root / "a.txt").read_text(encoding="utf-8") == "A"
    assert (downloaded_root / "b.txt").read_text(encoding="utf-8") == "B"
    assert (downloaded_root / "nested" / "c.txt").read_text(encoding="utf-8") == "C"


def test_log_artifact_to_subdirectory(nokv_artifact_repo: NoKVArtifactRepository, tmp_path: Path) -> None:
    local_file = tmp_path / "model.bin"
    local_file.write_bytes(b"weights")

    nokv_artifact_repo.log_artifact(str(local_file), artifact_path="models/latest")

    assert [(entry.path, entry.is_dir) for entry in nokv_artifact_repo.list_artifacts()] == [
        ("models", True)
    ]
    assert [
        (entry.path, entry.is_dir, entry.file_size)
        for entry in nokv_artifact_repo.list_artifacts("models/latest")
    ] == [("models/latest/model.bin", False, len(b"weights"))]


def test_download_artifacts_returns_absolute_path(
    nokv_artifact_repo: NoKVArtifactRepository, tmp_path: Path
) -> None:
    local_file = tmp_path / "metrics.json"
    local_file.write_text("{}", encoding="utf-8")
    destination = tmp_path / "downloads"
    destination.mkdir()

    nokv_artifact_repo.log_artifact(str(local_file))

    downloaded = nokv_artifact_repo.download_artifacts("metrics.json", dst_path=str(destination))

    assert downloaded == str(Path(downloaded).resolve())
    assert downloaded.startswith(str(destination.resolve()))


def test_list_artifacts_on_file_returns_empty(
    nokv_artifact_repo: NoKVArtifactRepository, tmp_path: Path
) -> None:
    local_file = tmp_path / "file.txt"
    local_file.write_text("payload", encoding="utf-8")
    nokv_artifact_repo.log_artifact(str(local_file))

    assert nokv_artifact_repo.list_artifacts("file.txt") == []


def test_delete_artifacts_file_directory_root_and_missing_path(
    nokv_artifact_repo: NoKVArtifactRepository, tmp_path: Path
) -> None:
    source = tmp_path / "source"
    nested = source / "nested"
    nested.mkdir(parents=True)
    (source / "a.txt").write_text("A", encoding="utf-8")
    (nested / "c.txt").write_text("C", encoding="utf-8")
    nokv_artifact_repo.log_artifacts(str(source))

    nokv_artifact_repo.delete_artifacts("nested/c.txt")
    assert nokv_artifact_repo.list_artifacts("nested") == []
    assert [(entry.path, entry.is_dir) for entry in nokv_artifact_repo.list_artifacts()] == [
        ("a.txt", False),
        ("nested", True),
    ]

    nokv_artifact_repo.delete_artifacts("nested")
    assert [(entry.path, entry.is_dir) for entry in nokv_artifact_repo.list_artifacts()] == [
        ("a.txt", False)
    ]

    nokv_artifact_repo.delete_artifacts("nonexistent")
    nokv_artifact_repo.delete_artifacts()
    assert nokv_artifact_repo.list_artifacts() == []


def test_invalid_paths_raise_mlflow_exception(
    nokv_artifact_repo: NoKVArtifactRepository, tmp_path: Path
) -> None:
    local_file = tmp_path / "file.txt"
    local_file.write_text("payload", encoding="utf-8")

    for artifact_path in ["/absolute", "../escape", "nested//child", "nested/"]:
        with pytest.raises(MlflowException):
            nokv_artifact_repo.log_artifact(str(local_file), artifact_path=artifact_path)

    with pytest.raises(MlflowException):
        nokv_artifact_repo.download_artifacts("/absolute/path/to/file")


def test_download_missing_artifact_raises_mlflow_exception(
    nokv_artifact_repo: NoKVArtifactRepository, tmp_path: Path
) -> None:
    destination = tmp_path / "downloads"
    destination.mkdir()

    with pytest.raises(MlflowException):
        nokv_artifact_repo.download_artifacts("missing.txt", dst_path=str(destination))
