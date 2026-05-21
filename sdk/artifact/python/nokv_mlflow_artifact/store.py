# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import os
import posixpath
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol


@dataclass(frozen=True)
class ArtifactInfo:
    path: str
    is_dir: bool
    size: int | None


class ArtifactStoreError(Exception):
    pass


class ArtifactNotFound(ArtifactStoreError):
    pass


class ArtifactIsDirectory(ArtifactStoreError):
    pass


class ArtifactStore(Protocol):
    def put_file(self, local_path: str, artifact_path: str) -> ArtifactInfo:
        ...

    def list(self, artifact_path: str | None = None) -> list[ArtifactInfo]:
        ...

    def download_file(self, artifact_path: str, local_path: str) -> ArtifactInfo:
        ...

    def delete_file(self, artifact_path: str) -> None:
        ...

    def delete_directory(self, artifact_path: str) -> None:
        ...

    def stat(self, artifact_path: str) -> ArtifactInfo:
        ...


class LocalArtifactStore:
    """A filesystem ArtifactStore used for adapter tests and local development."""

    def __init__(self, root: str | os.PathLike[str]) -> None:
        self._root = Path(root).resolve()
        self._root.mkdir(parents=True, exist_ok=True)

    @property
    def root(self) -> Path:
        return self._root

    def put_file(self, local_path: str, artifact_path: str) -> ArtifactInfo:
        source = Path(local_path)
        if not source.is_file():
            raise ArtifactNotFound(f"local file does not exist: {local_path}")

        normalized = normalize_artifact_path(artifact_path, allow_empty=False)
        destination = self._resolve(normalized)
        destination.parent.mkdir(parents=True, exist_ok=True)
        _copy_file_atomic(source, destination)
        return _file_info(self._root, destination)

    def list(self, artifact_path: str | None = None) -> list[ArtifactInfo]:
        normalized = normalize_artifact_path(artifact_path, allow_empty=True)
        target = self._resolve(normalized)
        if not target.exists():
            return []
        if target.is_file():
            return []
        if not target.is_dir():
            raise ArtifactStoreError(f"artifact path is not a file or directory: {normalized}")

        entries = [_file_info(self._root, child) for child in target.iterdir()]
        return sorted(entries, key=lambda entry: entry.path)

    def download_file(self, artifact_path: str, local_path: str) -> ArtifactInfo:
        normalized = normalize_artifact_path(artifact_path, allow_empty=False)
        source = self._resolve(normalized)
        if not source.exists():
            raise ArtifactNotFound(f"artifact does not exist: {normalized}")
        if source.is_dir():
            raise ArtifactIsDirectory(f"artifact is a directory: {normalized}")

        destination = Path(local_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        _copy_file_atomic(source, destination)
        return _file_info(self._root, source)

    def delete_file(self, artifact_path: str) -> None:
        normalized = normalize_artifact_path(artifact_path, allow_empty=False)
        target = self._resolve(normalized)
        if not target.exists():
            raise ArtifactNotFound(f"artifact does not exist: {normalized}")
        if target.is_dir():
            raise ArtifactIsDirectory(f"artifact is a directory: {normalized}")
        target.unlink()

    def delete_directory(self, artifact_path: str) -> None:
        normalized = normalize_artifact_path(artifact_path, allow_empty=False)
        target = self._resolve(normalized)
        if not target.exists():
            raise ArtifactNotFound(f"artifact does not exist: {normalized}")
        if not target.is_dir():
            raise ValueError(f"artifact is not a directory: {normalized}")
        target.rmdir()

    def stat(self, artifact_path: str) -> ArtifactInfo:
        normalized = normalize_artifact_path(artifact_path, allow_empty=True)
        target = self._resolve(normalized)
        if not target.exists():
            raise ArtifactNotFound(f"artifact does not exist: {normalized}")
        return _file_info(self._root, target)

    def _resolve(self, normalized_path: str) -> Path:
        if normalized_path:
            target = (self._root / Path(*normalized_path.split("/"))).resolve()
        else:
            target = self._root
        if target != self._root and self._root not in target.parents:
            raise ValueError(f"artifact path escapes store root: {normalized_path}")
        return target


def normalize_artifact_path(path: str | None, *, allow_empty: bool) -> str:
    if path is None:
        if allow_empty:
            return ""
        raise ValueError("artifact path is required")
    if not isinstance(path, str):
        raise TypeError("artifact path must be a string")
    if "\x00" in path:
        raise ValueError("artifact path contains a NUL byte")
    if "\\" in path:
        raise ValueError("artifact path must use POSIX separators")

    if path == "":
        if allow_empty:
            return ""
        raise ValueError("artifact path is required")

    normalized = posixpath.normpath(path)
    if (
        normalized != path
        or normalized == "."
        or normalized == ".."
        or normalized.startswith("../")
        or normalized.startswith("/")
    ):
        raise ValueError(f"artifact path escapes namespace: {path}")
    return normalized


def join_artifact_path(prefix: str | None, name: str) -> str:
    base = normalize_artifact_path(prefix, allow_empty=True)
    filename = normalize_artifact_path(name, allow_empty=False)
    if "/" in filename:
        raise ValueError("artifact name must be a single path segment")
    return f"{base}/{filename}" if base else filename


def _file_info(root: Path, path: Path) -> ArtifactInfo:
    rel_path = path.relative_to(root).as_posix()
    if rel_path == ".":
        rel_path = ""
    if path.is_dir():
        return ArtifactInfo(path=rel_path, is_dir=True, size=None)
    return ArtifactInfo(path=rel_path, is_dir=False, size=path.stat().st_size)


def _copy_file_atomic(source: Path, destination: Path) -> None:
    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.",
        suffix=".tmp",
        dir=str(destination.parent),
    )
    try:
        with os.fdopen(fd, "wb") as temporary:
            with source.open("rb") as input_file:
                shutil.copyfileobj(input_file, temporary)
            temporary.flush()
            os.fsync(temporary.fileno())
        os.replace(temporary_name, destination)
    except BaseException:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise
