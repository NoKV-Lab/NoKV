"""Bounded fsspec compatibility inside one explicit NoKV Workbench.

The adapter projects the five virtual Workbench sections and implicit artifact
prefixes as directory-shaped fsspec results. It never creates directory rows,
permissions, inodes, mounts, or an arbitrary root filesystem. File writes are
whole-object immutable publications fenced by the current generation.
"""

from __future__ import annotations

import io
from typing import Iterable, Optional, Sequence

from ._native import Client

try:
    from fsspec.spec import AbstractFileSystem
except ImportError as exc:  # pragma: no cover - exercised by installed extras.
    raise ImportError(
        "WorkbenchFileSystem requires fsspec; install nokv with its fsspec dependency"
    ) from exc


SECTIONS = ("input", "scripts", "outputs", "logs", "metadata")
Range = tuple[int, int]
RangeRequest = tuple[str, Sequence[Range], Optional[int], Optional[int]]


def _validate_open_mode(mode: str) -> None:
    flags = str(mode)
    if "+" in flags:
        raise ValueError(
            f"unsupported open mode {mode!r}: immutable Workbench artifacts do not support update"
        )
    if "t" in flags:
        raise ValueError(
            f"unsupported open mode {mode!r}: WorkbenchFileSystem is byte-oriented"
        )
    base = flags.replace("b", "")
    if base == "a":
        raise ValueError(
            f"unsupported open mode {mode!r}: immutable Workbench artifacts do not support append"
        )
    if flags not in ("r", "rb", "w", "wb", "x", "xb"):
        raise ValueError(f"unsupported open mode {mode!r}")


def _workbench_path(path: str, *, require_artifact: bool = False) -> str:
    if not isinstance(path, str):
        raise ValueError("Workbench path must be a string")
    if path.startswith("nokv://"):
        path = path[len("nokv://") :]
    if not path or path.startswith("/") or path.endswith("/"):
        raise ValueError("Workbench paths are section-prefixed relative paths")
    if "\\" in path or "\x00" in path:
        raise ValueError("Workbench paths cannot contain backslashes or NUL")
    parts = path.split("/")
    if any(part in ("", ".", "..") for part in parts):
        raise ValueError("Workbench paths cannot contain empty, '.' or '..' components")
    if parts[0] not in SECTIONS:
        raise ValueError(f"Workbench path must start with one of {SECTIONS!r}")
    if len(parts) > 1 and parts[1] == parts[0]:
        raise ValueError("Workbench path duplicates its section prefix")
    if require_artifact and len(parts) == 1:
        raise ValueError("file operations require a path below one virtual section")
    return "/".join(parts)


def _file_info(path: str, metadata: dict) -> dict:
    return {
        "name": path,
        "size": int(metadata["logical_size"]),
        "type": "file",
        "generation": int(metadata["generation"]),
        "body_digest": metadata.get("body_digest"),
    }


def _prefix_info(path: str) -> dict:
    return {"name": path, "size": 0, "type": "directory"}


class _WorkbenchFile(io.BytesIO):
    def __init__(
        self,
        filesystem: "WorkbenchFileSystem",
        path: str,
        mode: str,
        expected_generation: Optional[int],
        publish_options: dict,
    ):
        self._filesystem = filesystem
        self._path = path
        self._mode = mode.replace("b", "")
        self._expected_generation = expected_generation
        self._publish_options = publish_options
        self._publication_attempted = False
        if self._mode == "r":
            body = filesystem.client.read(
                filesystem.workbench, path, filesystem.snapshot_id
            )["bytes"]
            super().__init__(body)
        else:
            super().__init__()

    def writable(self) -> bool:
        return self._mode in ("w", "x")

    def readable(self) -> bool:
        return self._mode == "r"

    def close(self) -> None:
        if self.closed:
            return
        failure = None
        if self.writable() and not self._publication_attempted:
            self._publication_attempted = True
            try:
                self._filesystem.client.publish_bytes(
                    self._filesystem.workbench,
                    self._path,
                    self.getvalue(),
                    expected_generation=self._expected_generation,
                    **self._publish_options,
                )
            except BaseException as error:  # close must propagate the exact SDK failure.
                failure = error
        super().close()
        if failure is not None:
            raise failure


class WorkbenchFileSystem(AbstractFileSystem):
    """fsspec adapter jailed to one Workbench and its five virtual sections."""

    protocol = "nokv"
    root_marker = ""

    def __init__(
        self,
        *args,
        client: Client,
        workbench: str,
        snapshot_id: Optional[int] = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        if not workbench:
            raise ValueError("workbench must be non-empty")
        if snapshot_id is not None and int(snapshot_id) <= 0:
            raise ValueError("snapshot_id must be greater than zero")
        self.client = client
        self.workbench = str(workbench)
        self.snapshot_id = None if snapshot_id is None else int(snapshot_id)

    def _require_writable(self) -> None:
        if self.snapshot_id is not None:
            raise ValueError("snapshot-scoped WorkbenchFileSystem is read-only")

    @classmethod
    def _strip_protocol(cls, path: str) -> str:
        return _workbench_path(path)

    def _open(self, path, mode="rb", block_size=None, **kwargs):
        del block_size
        autocommit = bool(kwargs.pop("autocommit", True))
        kwargs.pop("cache_options", None)
        if not autocommit:
            raise ValueError("WorkbenchFileSystem commits whole objects on close")
        _validate_open_mode(mode)
        path = _workbench_path(path, require_artifact=True)
        if mode.replace("b", "") != "r":
            self._require_writable()
        base = mode.replace("b", "")
        expected_generation = None
        if base == "w" and self.client.exists(self.workbench, path, self.snapshot_id):
            expected_generation = int(
                self.client.stat(self.workbench, path, self.snapshot_id)["generation"]
            )
        publish_options = {
            "content_type": kwargs.pop("content_type", "application/octet-stream"),
            "producer": kwargs.pop("producer", "nokv-workbench-fsspec"),
        }
        for key in ("operation_id", "artifact_revision_id", "block_size"):
            if key in kwargs:
                publish_options[key] = kwargs.pop(key)
        if kwargs:
            raise ValueError(f"unsupported Workbench file options: {sorted(kwargs)!r}")
        return _WorkbenchFile(
            self, path, mode, expected_generation, publish_options
        )

    def cat_file(self, path, start=None, end=None, **kwargs):
        path = _workbench_path(path, require_artifact=True)
        expected_generation = kwargs.pop("expected_generation", None)
        max_gap_bytes = kwargs.pop("max_gap_bytes", None)
        if max_gap_bytes is not None:
            max_gap_bytes = int(max_gap_bytes)
            if max_gap_bytes < 0:
                raise ValueError("max_gap_bytes must be non-negative")
        if kwargs:
            raise ValueError(f"unsupported range options: {sorted(kwargs)!r}")
        if start is None and end is None:
            outcome = self.client.read(self.workbench, path, self.snapshot_id)
            if expected_generation is not None and int(expected_generation) != int(
                outcome["metadata"]["generation"]
            ):
                raise RuntimeError("artifact generation changed before full read")
        else:
            start = 0 if start is None else int(start)
            if start < 0:
                raise ValueError("start must be non-negative")
            if end is None:
                metadata = self.client.stat(self.workbench, path, self.snapshot_id)
                if expected_generation is not None and int(expected_generation) != int(
                    metadata["generation"]
                ):
                    raise RuntimeError("artifact generation changed before range read")
                expected_generation = int(metadata["generation"])
                end = int(metadata["logical_size"])
            else:
                end = int(end)
            if end < start:
                raise ValueError("end must be greater than or equal to start")
            if end == start:
                return b""
            batches = self.client.read_ranges_batch(
                self.workbench,
                [(path, [(start, end - start)], expected_generation, max_gap_bytes)],
                self.snapshot_id,
            )
            if len(batches) != 1 or len(batches[0]) != 1:
                raise RuntimeError("read_ranges_batch omitted the requested fsspec range")
            return bytes(batches[0][0])
        return bytes(outcome["bytes"])

    def read_ranges_batch(
        self, requests: Iterable[RangeRequest]
    ) -> list[list[bytes]]:
        normalized = []
        for path, ranges, expected_generation, max_gap_bytes in requests:
            normalized.append(
                (
                    _workbench_path(path, require_artifact=True),
                    [(int(offset), int(length)) for offset, length in ranges],
                    expected_generation,
                    max_gap_bytes,
                )
            )
        return self.client.read_ranges_batch(
            self.workbench, normalized, self.snapshot_id
        )

    def exists(self, path, **kwargs):
        del kwargs
        try:
            self.info(path)
            return True
        except (FileNotFoundError, ValueError):
            return False

    def info(self, path, **kwargs):
        del kwargs
        path = _workbench_path(path)
        if path in SECTIONS:
            return _prefix_info(path)
        if self.client.exists(self.workbench, path, self.snapshot_id):
            return _file_info(
                path, self.client.stat(self.workbench, path, self.snapshot_id)
            )
        page = self.client.list(
            self.workbench,
            path,
            True,
            None,
            1,
            None,
            self.snapshot_id,
        )
        if page["entries"]:
            return _prefix_info(path)
        raise FileNotFoundError(path)

    def ls(self, path, detail=True, **kwargs):
        del kwargs
        path = _workbench_path(path)
        if self.client.exists(self.workbench, path, self.snapshot_id):
            entries = [self.info(path)]
        else:
            entries = []
            cursor = None
            read_version = None
            while True:
                page = self.client.list(
                    self.workbench,
                    path,
                    False,
                    cursor,
                    1_000,
                    read_version,
                    self.snapshot_id,
                )
                read_version = int(page["read_version"])
                for entry in page["entries"]:
                    child = entry["path"]
                    entries.append(
                        _prefix_info(child)
                        if entry["kind"] == "prefix"
                        else _file_info(child, entry)
                    )
                cursor = page["next_cursor"]
                if cursor is None:
                    break
            if not entries and path not in SECTIONS:
                raise FileNotFoundError(path)
        return entries if detail else [entry["name"] for entry in entries]

    def makedirs(self, path, exist_ok=False, **kwargs):
        self._require_writable()
        if kwargs:
            raise ValueError("Workbench prefixes have no mode, owner, or permission fields")
        path = _workbench_path(path)
        if self.exists(path):
            if not exist_ok:
                raise FileExistsError(path)
            return
        # Prefixes are implicit. This intentionally writes no durable metadata.

    def mkdir(self, path, create_parents=True, **kwargs):
        del create_parents
        return self.makedirs(path, **kwargs)

    def rmdir(self, path):
        self._require_writable()
        path = _workbench_path(path)
        if path in SECTIONS:
            raise OSError("virtual Workbench sections cannot be removed")
        if self.client.exists(self.workbench, path, self.snapshot_id):
            raise NotADirectoryError(path)
        page = self.client.list(
            self.workbench,
            path,
            True,
            None,
            1,
            None,
            self.snapshot_id,
        )
        if page["entries"]:
            raise OSError(f"prefix is not empty: {path}")
        # An empty implicit prefix has no durable row to delete.

    def mv(self, path1, path2, recursive=False, maxdepth=None, **kwargs):
        self._require_writable()
        del maxdepth
        if recursive:
            raise ValueError("WorkbenchFileSystem moves artifacts, not directory trees")
        if kwargs.pop("replace", False):
            raise ValueError("atomic Workbench rename is create-only")
        if kwargs:
            raise ValueError(f"unsupported move options: {sorted(kwargs)!r}")
        source = _workbench_path(path1, require_artifact=True)
        destination = _workbench_path(path2, require_artifact=True)
        metadata = self.client.stat(self.workbench, source, self.snapshot_id)
        return self.client.rename(
            self.workbench, source, destination, int(metadata["generation"])
        )

    def rm_file(self, path):
        self._require_writable()
        path = _workbench_path(path, require_artifact=True)
        metadata = self.client.stat(self.workbench, path, self.snapshot_id)
        return self.client.remove(
            self.workbench, path, int(metadata["generation"])
        )

    def rm(self, path, recursive=False, maxdepth=None):
        del maxdepth
        if recursive:
            raise ValueError("recursive prefix deletion is outside the Workbench adapter")
        return self.rm_file(path)
