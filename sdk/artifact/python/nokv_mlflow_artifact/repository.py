# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import os
import posixpath
import tempfile
import urllib.parse
from importlib import import_module
from pathlib import Path

from mlflow.entities.file_info import FileInfo
from mlflow.exceptions import MlflowException
from mlflow.protos.databricks_pb2 import (
    INVALID_PARAMETER_VALUE,
    RESOURCE_DOES_NOT_EXIST,
)
from mlflow.store.artifact.artifact_repo import ArtifactRepository, verify_artifact_path

from nokv_mlflow_artifact.store import (
    ArtifactInfo,
    ArtifactIsDirectory,
    ArtifactNotFound,
    ArtifactStore,
    LocalArtifactStore,
    join_artifact_path,
    normalize_artifact_path,
)


_STORE_FACTORY_ENV = "NOKV_ARTIFACT_STORE_FACTORY"


class NoKVArtifactRepository(ArtifactRepository):
    """MLflow artifact repository backed by the NoKV artifact namespace SDK."""

    def __init__(
        self,
        artifact_uri: str,
        tracking_uri: str | None = None,
        registry_uri: str | None = None,
        store: ArtifactStore | None = None,
    ) -> None:
        super().__init__(artifact_uri, tracking_uri, registry_uri)
        self._store = (
            store
            if store is not None
            else store_from_artifact_uri(artifact_uri, tracking_uri, registry_uri)
        )

    def log_artifact(self, local_file: str, artifact_path: str | None = None) -> None:
        verify_artifact_path(artifact_path)
        if not os.path.isfile(local_file):
            raise MlflowException(
                f"Local artifact does not exist or is not a file: '{local_file}'",
                error_code=RESOURCE_DOES_NOT_EXIST,
            )

        destination = join_artifact_path(artifact_path, os.path.basename(local_file))
        self._put_file(local_file, destination)

    def log_artifacts(self, local_dir: str, artifact_path: str | None = None) -> None:
        verify_artifact_path(artifact_path)
        if not os.path.isdir(local_dir):
            raise MlflowException(
                f"Local artifact directory does not exist: '{local_dir}'",
                error_code=RESOURCE_DOES_NOT_EXIST,
            )

        base = Path(local_dir)
        for root, dirs, files in os.walk(local_dir):
            dirs.sort()
            for filename in sorted(files):
                local_file = Path(root) / filename
                relative_path = local_file.relative_to(base).as_posix()
                destination = _join_optional_prefix(artifact_path, relative_path)
                self._put_file(str(local_file), destination)

    def list_artifacts(self, path: str | None = None) -> list[FileInfo]:
        normalized = _normalize_for_mlflow(path, allow_empty=True)
        try:
            entries = self._store.list(normalized)
        except ValueError as exc:
            raise _invalid_parameter(str(exc)) from exc
        return [_to_file_info(entry) for entry in sorted(entries, key=lambda entry: entry.path)]

    def _download_file(self, remote_file_path: str, local_path: str) -> None:
        normalized = _normalize_for_mlflow(remote_file_path, allow_empty=False)
        destination = Path(local_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary_name = tempfile.mkstemp(
            prefix=f".{destination.name}.",
            suffix=".tmp",
            dir=str(destination.parent),
        )
        os.close(fd)
        try:
            self._store.download_file(normalized, temporary_name)
            os.replace(temporary_name, destination)
        except ArtifactNotFound as exc:
            _unlink_if_exists(temporary_name)
            raise _not_found(normalized) from exc
        except ArtifactIsDirectory as exc:
            _unlink_if_exists(temporary_name)
            raise _invalid_parameter(f"Artifact is a directory: '{normalized}'") from exc
        except ValueError as exc:
            _unlink_if_exists(temporary_name)
            raise _invalid_parameter(str(exc)) from exc
        except BaseException:
            _unlink_if_exists(temporary_name)
            raise

    def delete_artifacts(self, artifact_path: str | None = None) -> None:
        normalized = _normalize_for_mlflow(artifact_path, allow_empty=True)
        if normalized == "":
            if self.list_artifacts(None):
                raise _directory_delete_unsupported(normalized)
            return

        listing = self.list_artifacts(normalized)
        if listing:
            raise _directory_delete_unsupported(normalized)

        try:
            self._store.delete_file(normalized)
        except ArtifactNotFound as exc:
            raise _not_found(normalized) from exc
        except ArtifactIsDirectory as exc:
            raise _directory_delete_unsupported(normalized) from exc
        except ValueError as exc:
            raise _invalid_parameter(str(exc)) from exc

    def _put_file(self, local_file: str, destination: str) -> None:
        try:
            self._store.put_file(local_file, destination)
        except ValueError as exc:
            raise _invalid_parameter(str(exc)) from exc


def store_from_artifact_uri(
    artifact_uri: str,
    tracking_uri: str | None = None,
    registry_uri: str | None = None,
) -> ArtifactStore:
    parsed = urllib.parse.urlparse(artifact_uri)
    if parsed.scheme == "nokv+file":
        return LocalArtifactStore(_local_path_from_nokv_file_uri(parsed))
    if parsed.scheme == "nokv":
        factory_name = os.environ.get(_STORE_FACTORY_ENV)
        if factory_name:
            return _load_store_factory(factory_name)(
                artifact_uri=artifact_uri,
                tracking_uri=tracking_uri,
                registry_uri=registry_uri,
            )
    raise MlflowException(
        "NoKV MLflow artifacts require a Python NoKV ArtifactStore implementation. "
        f"Set {_STORE_FACTORY_ENV}, pass a store to NoKVArtifactRepository, or use the "
        "nokv+file scheme for local adapter tests.",
        error_code=INVALID_PARAMETER_VALUE,
    )


def _load_store_factory(factory_name: str):
    module_name, separator, function_name = factory_name.partition(":")
    if not separator or not module_name or not function_name:
        raise MlflowException(
            f"{_STORE_FACTORY_ENV} must use 'module:function' format",
            error_code=INVALID_PARAMETER_VALUE,
        )
    try:
        module = import_module(module_name)
        factory = getattr(module, function_name)
    except (ImportError, AttributeError) as exc:
        raise MlflowException(
            f"Failed to load NoKV artifact store factory '{factory_name}': {exc}",
            error_code=INVALID_PARAMETER_VALUE,
        ) from exc
    if not callable(factory):
        raise MlflowException(
            f"NoKV artifact store factory '{factory_name}' is not callable",
            error_code=INVALID_PARAMETER_VALUE,
        )
    return factory


def _local_path_from_nokv_file_uri(parsed: urllib.parse.ParseResult) -> str:
    if parsed.netloc not in ("", "localhost"):
        raise MlflowException(
            f"nokv+file URI must use a local path, got host '{parsed.netloc}'",
            error_code=INVALID_PARAMETER_VALUE,
        )
    path = urllib.parse.unquote(parsed.path)
    if not path:
        raise MlflowException(
            "nokv+file URI requires an absolute filesystem path",
            error_code=INVALID_PARAMETER_VALUE,
        )
    return path


def _normalize_for_mlflow(path: str | None, *, allow_empty: bool) -> str:
    try:
        return normalize_artifact_path(path, allow_empty=allow_empty)
    except (TypeError, ValueError) as exc:
        raise _invalid_parameter(str(exc)) from exc


def _join_optional_prefix(prefix: str | None, relative_path: str) -> str:
    base = normalize_artifact_path(prefix, allow_empty=True)
    relative = normalize_artifact_path(relative_path, allow_empty=False)
    return posixpath.join(base, relative) if base else relative


def _to_file_info(info: ArtifactInfo) -> FileInfo:
    return FileInfo(info.path, info.is_dir, None if info.is_dir else info.size)


def _not_found(path: str) -> MlflowException:
    return MlflowException(
        f"No such NoKV artifact: '{path}'",
        error_code=RESOURCE_DOES_NOT_EXIST,
    )


def _invalid_parameter(message: str) -> MlflowException:
    return MlflowException(message, error_code=INVALID_PARAMETER_VALUE)


def _directory_delete_unsupported(path: str) -> MlflowException:
    display_path = path or "<root>"
    return MlflowException(
        f"NoKV artifact directory delete is not supported yet: '{display_path}'. "
        "The fsmeta rmdir primitive is required for recursive MLflow deletes.",
        error_code=INVALID_PARAMETER_VALUE,
    )


def _unlink_if_exists(path: str) -> None:
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass
