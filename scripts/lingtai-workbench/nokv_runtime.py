#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Content-addressed NoKV runtime identity and staging helpers."""

from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import shutil
import stat
import subprocess
import tempfile
import urllib.parse
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


BUILD_INFO_SCHEMA = "nokv.build_info.v1"
REVISION_RE = re.compile(r"^[0-9a-f]{40}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True)
class SourceIdentity:
    schema: str
    nokv_version: str
    nokv_git_commit: str
    source_dirty: bool
    cargo_lock_sha256: str
    holt_crate_version: str
    holt_git_commit: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class StagedRuntime:
    command: Path
    sha256: str
    size_bytes: int
    identity: SourceIdentity


@dataclass(frozen=True)
class BuildInfo:
    identity: SourceIdentity
    binary_sha256: str
    binary_size_bytes: int

    def as_dict(self) -> dict[str, Any]:
        return {
            **self.identity.as_dict(),
            "binary_sha256": self.binary_sha256,
            "binary_size_bytes": self.binary_size_bytes,
        }


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1 << 20):
            digest.update(chunk)
    return digest.hexdigest()


def validate_revision(value: str) -> str:
    normalized = value.lower()
    if not REVISION_RE.fullmatch(normalized):
        raise ValueError("NoKV revision must be a full 40-character git commit")
    return normalized


def validate_sha256(value: str) -> str:
    normalized = value.lower()
    if not SHA256_RE.fullmatch(normalized):
        raise ValueError("binary SHA-256 must contain exactly 64 hex characters")
    return normalized


def _run(command: list[str], *, cwd: Path | None = None) -> str:
    completed = subprocess.run(
        command,
        cwd=cwd,
        check=False,
        capture_output=True,
        text=True,
        timeout=15,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise ValueError(f"{' '.join(command)} failed: {detail}")
    return completed.stdout.strip()


def discover_nokv_binary(explicit: str | None = None) -> Path:
    candidate = explicit or os.environ.get("NOKV_BIN") or shutil.which("nokv")
    if candidate is None and shutil.which("brew"):
        completed = subprocess.run(
            ["brew", "--prefix", "nokv"],
            check=False,
            capture_output=True,
            text=True,
            timeout=15,
        )
        if completed.returncode == 0 and completed.stdout.strip():
            candidate = str(Path(completed.stdout.strip()) / "bin" / "nokv")
    if candidate is None:
        raise FileNotFoundError(
            "cannot find nokv; pass --nokv-bin, set NOKV_BIN, or install it on PATH"
        )
    path = Path(candidate).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(f"nokv binary does not exist: {path}")
    if not os.access(path, os.X_OK):
        raise PermissionError(f"nokv binary is not executable: {path}")
    return path


def _package_block(lock_text: str, name: str) -> dict[str, str]:
    for block in re.split(r"(?m)^\[\[package\]\]\s*$", lock_text):
        values = dict(re.findall(r'(?m)^(name|version|source) = "([^"]+)"$', block))
        if values.get("name") == name:
            return values
    raise ValueError(f"Cargo.lock does not contain package {name}")


def _nokv_version(source_root: Path) -> str:
    manifest = source_root / "crates" / "nokv" / "Cargo.toml"
    text = manifest.read_text(encoding="utf-8")
    match = re.search(r'(?m)^version = "([^"]+)"$', text)
    if match is None:
        raise ValueError(f"cannot resolve nokv package version from {manifest}")
    return match.group(1)


def source_identity(source_root: Path, revision: str | None = None) -> SourceIdentity:
    root = source_root.expanduser().resolve()
    cargo_toml = root / "Cargo.toml"
    cargo_lock = root / "Cargo.lock"
    if not cargo_toml.is_file() or not cargo_lock.is_file():
        raise FileNotFoundError(f"not a NoKV source root: {root}")

    git_dir = root / ".git"
    if not git_dir.exists():
        raise ValueError(f"NoKV source identity requires a git checkout: {root}")
    head = validate_revision(_run(["git", "rev-parse", "HEAD"], cwd=root))
    if revision is None:
        revision = head
    revision = validate_revision(revision)
    if head != revision:
        raise ValueError(f"source HEAD {head} does not match revision {revision}")
    dirty = bool(
        _run(
            ["git", "status", "--porcelain", "--untracked-files=normal"],
            cwd=root,
        )
    )

    lock_text = cargo_lock.read_text(encoding="utf-8")
    holt = _package_block(lock_text, "holt")
    source = holt.get("source", "")
    parsed_source = urllib.parse.urlsplit(source.removeprefix("git+").split("#", 1)[0])
    if (
        parsed_source.scheme != "https"
        or (parsed_source.hostname or "").lower() != "github.com"
        or parsed_source.path.rstrip("/").lower() != "/nokv-lab/holt.git"
    ):
        raise ValueError("Cargo.lock Holt package is not pinned to NoKV-Lab/holt")
    source_commit = source.rsplit("#", 1)[-1]
    holt_commit = validate_revision(source_commit)
    holt_version = holt.get("version")
    if not holt_version:
        raise ValueError("Cargo.lock Holt package has no version")
    manifest_text = cargo_toml.read_text(encoding="utf-8")
    manifest_match = re.search(
        r'(?m)^holt\s*=\s*\{[^\n]*\brev\s*=\s*"([0-9a-fA-F]{40})"',
        manifest_text,
    )
    if manifest_match is None:
        raise ValueError("Cargo.toml must pin Holt with a full git rev")
    manifest_commit = validate_revision(manifest_match.group(1))
    if manifest_commit != holt_commit:
        raise ValueError(
            "Holt revision differs between Cargo.toml and Cargo.lock: "
            f"{manifest_commit} != {holt_commit}"
        )

    return SourceIdentity(
        schema=BUILD_INFO_SCHEMA,
        nokv_version=_nokv_version(root),
        nokv_git_commit=revision,
        source_dirty=dirty,
        cargo_lock_sha256=sha256_file(cargo_lock),
        holt_crate_version=holt_version,
        holt_git_commit=holt_commit,
    )


def identity_from_mapping(data: Any, *, context: str) -> SourceIdentity:
    if not isinstance(data, dict) or data.get("schema") != BUILD_INFO_SCHEMA:
        raise ValueError(f"{context} is not a {BUILD_INFO_SCHEMA} object")
    required_strings = (
        "nokv_version",
        "nokv_git_commit",
        "cargo_lock_sha256",
        "holt_crate_version",
        "holt_git_commit",
    )
    for field in required_strings:
        if not isinstance(data.get(field), str) or not data[field]:
            raise ValueError(f"{context}: {field} must be a non-empty string")
    if not isinstance(data.get("source_dirty"), bool):
        raise ValueError(f"{context}: source_dirty must be a boolean")
    validate_revision(data["nokv_git_commit"])
    validate_revision(data["holt_git_commit"])
    validate_sha256(data["cargo_lock_sha256"])
    return SourceIdentity(
        **{field: data[field] for field in SourceIdentity.__annotations__}
    )


def load_build_info(path: Path) -> BuildInfo:
    data = json.loads(path.expanduser().read_text(encoding="utf-8"))
    return build_info_from_mapping(data, context=str(path))


def build_info_from_mapping(data: Any, *, context: str) -> BuildInfo:
    identity = identity_from_mapping(data, context=context)
    binary_sha256 = data.get("binary_sha256")
    binary_size_bytes = data.get("binary_size_bytes")
    if not isinstance(binary_sha256, str):
        raise ValueError(f"{context}: binary_sha256 must be a string")
    validate_sha256(binary_sha256)
    if not isinstance(binary_size_bytes, int) or isinstance(binary_size_bytes, bool):
        raise ValueError(f"{context}: binary_size_bytes must be an integer")
    if binary_size_bytes < 1:
        raise ValueError(f"{context}: binary_size_bytes must be positive")
    return BuildInfo(identity, binary_sha256, binary_size_bytes)


def infer_distribution(binary: Path) -> str:
    parts = {part.lower() for part in binary.parts}
    if "cellar" in parts or "homebrew" in parts:
        return "brew"
    return "path"


def _lstat(path: Path) -> os.stat_result | None:
    try:
        return path.lstat()
    except FileNotFoundError:
        return None


def _directory_open_flags() -> int:
    return os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)


def _regular_file_open_flags() -> int:
    return (
        os.O_RDONLY
        | os.O_NOFOLLOW
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )


def _entry_metadata_at(directory_fd: int, name: str) -> os.stat_result | None:
    try:
        return os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
    except FileNotFoundError:
        return None


def _raise_directory_open_error(
    directory_fd: int, name: str, path: Path, error: OSError
) -> None:
    metadata = _entry_metadata_at(directory_fd, name)
    if metadata is not None and stat.S_ISLNK(metadata.st_mode):
        raise ValueError(
            f"managed runtime path contains a symlink component: {path}"
        ) from error
    if metadata is not None and not stat.S_ISDIR(metadata.st_mode):
        raise ValueError(
            f"managed runtime path component is not a directory: {path}"
        ) from error
    raise error


def _open_directory_at(
    directory_fd: int, name: str, path: Path, *, create: bool
) -> int:
    try:
        return os.open(name, _directory_open_flags(), dir_fd=directory_fd)
    except FileNotFoundError:
        if not create:
            raise
        try:
            os.mkdir(name, 0o755, dir_fd=directory_fd)
            os.fsync(directory_fd)
        except FileExistsError:
            pass
    except OSError as error:
        _raise_directory_open_error(directory_fd, name, path, error)

    try:
        return os.open(name, _directory_open_flags(), dir_fd=directory_fd)
    except OSError as error:
        _raise_directory_open_error(directory_fd, name, path, error)
    raise AssertionError("unreachable")


def _open_project_directory(path: Path) -> int:
    try:
        descriptor = os.open(path, _directory_open_flags())
    except OSError as error:
        metadata = _lstat(path)
        if metadata is not None and stat.S_ISLNK(metadata.st_mode):
            raise ValueError(f"project path is a symlink: {path}") from error
        if metadata is not None and not stat.S_ISDIR(metadata.st_mode):
            raise ValueError(f"project path is not a directory: {path}") from error
        raise
    if not stat.S_ISDIR(os.fstat(descriptor).st_mode):
        os.close(descriptor)
        raise ValueError(f"project path is not a directory: {path}")
    return descriptor


def _create_temp_file_at(directory_fd: int, prefix: str) -> tuple[int, str]:
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | os.O_NOFOLLOW
        | getattr(os, "O_CLOEXEC", 0)
    )
    for _ in range(128):
        name = f"{prefix}{secrets.token_hex(12)}"
        try:
            return os.open(name, flags, 0o600, dir_fd=directory_fd), name
        except FileExistsError:
            continue
    raise FileExistsError("cannot allocate a unique managed runtime temporary file")


def _unlink_at(directory_fd: int, name: str) -> None:
    try:
        os.unlink(name, dir_fd=directory_fd)
    except FileNotFoundError:
        pass


def _open_regular_file_at(directory_fd: int, name: str, path: Path) -> int:
    try:
        descriptor = os.open(name, _regular_file_open_flags(), dir_fd=directory_fd)
    except OSError as error:
        metadata = _entry_metadata_at(directory_fd, name)
        if metadata is not None and (
            stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode)
        ):
            raise ValueError(
                f"content-addressed runtime is not a regular file: {path}"
            ) from error
        raise
    if not stat.S_ISREG(os.fstat(descriptor).st_mode):
        os.close(descriptor)
        raise ValueError(f"content-addressed runtime is not a regular file: {path}")
    return descriptor


def _sha256_descriptor(descriptor: int) -> tuple[str, int]:
    digest = hashlib.sha256()
    size_bytes = 0
    os.lseek(descriptor, 0, os.SEEK_SET)
    while chunk := os.read(descriptor, 1 << 20):
        digest.update(chunk)
        size_bytes += len(chunk)
    return digest.hexdigest(), size_bytes


def _read_descriptor(descriptor: int) -> bytes:
    chunks = []
    os.lseek(descriptor, 0, os.SEEK_SET)
    while chunk := os.read(descriptor, 1 << 20):
        chunks.append(chunk)
    return b"".join(chunks)


def _load_build_info_descriptor(descriptor: int, *, context: str) -> BuildInfo:
    try:
        data = json.loads(_read_descriptor(descriptor).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"{context} is not valid UTF-8 JSON") from error
    return build_info_from_mapping(data, context=context)


def _build_info_text(build_info: BuildInfo) -> str:
    return (
        json.dumps(build_info.as_dict(), ensure_ascii=False, indent=2, sort_keys=True)
        + "\n"
    )


def _verify_directory_binding(path: Path, descriptor: int) -> None:
    metadata = _lstat(path)
    if metadata is None:
        raise ValueError(f"managed runtime path changed while staging: {path}")
    if stat.S_ISLNK(metadata.st_mode):
        raise ValueError(f"managed runtime path contains a symlink component: {path}")
    if not stat.S_ISDIR(metadata.st_mode):
        raise ValueError(f"managed runtime path component is not a directory: {path}")
    opened_metadata = os.fstat(descriptor)
    if (metadata.st_dev, metadata.st_ino) != (
        opened_metadata.st_dev,
        opened_metadata.st_ino,
    ):
        raise ValueError(f"managed runtime path changed while staging: {path}")


def _verify_regular_binding_at(
    directory_fd: int, name: str, descriptor: int, path: Path
) -> None:
    metadata = _entry_metadata_at(directory_fd, name)
    opened_metadata = os.fstat(descriptor)
    if metadata is None or not stat.S_ISREG(metadata.st_mode):
        raise ValueError(f"content-addressed runtime changed while staging: {path}")
    if (metadata.st_dev, metadata.st_ino) != (
        opened_metadata.st_dev,
        opened_metadata.st_ino,
    ):
        raise ValueError(f"content-addressed runtime changed while staging: {path}")


def _copy_with_sha256(source: Path, target: Any) -> tuple[str, int]:
    digest = hashlib.sha256()
    size_bytes = 0
    with source.open("rb") as source_handle:
        while chunk := source_handle.read(1 << 20):
            target.write(chunk)
            digest.update(chunk)
            size_bytes += len(chunk)
    return digest.hexdigest(), size_bytes


def stage_runtime(
    project: Path,
    binary: Path,
    identity: SourceIdentity,
    *,
    expected_sha256: str | None = None,
) -> StagedRuntime:
    project = project.expanduser().resolve()
    lingtai_root = project / ".lingtai"
    binary = binary.expanduser().resolve()
    digest = sha256_file(binary)
    if expected_sha256 is not None and digest != validate_sha256(expected_sha256):
        raise ValueError(
            f"nokv binary SHA-256 mismatch: expected {expected_sha256}, got {digest}"
        )

    revision = validate_revision(identity.nokv_git_commit)
    runtime_root = lingtai_root / "runtime"
    nokv_root = runtime_root / "nokv"
    revision_dir = nokv_root / revision
    runtime_dir = revision_dir / digest
    command = runtime_dir / "nokv"
    build_info = runtime_dir / "build-info.json"

    directory_descriptors: list[tuple[Path, int]] = []
    revision_descriptor: int | None = None
    digest_descriptor: int | None = None
    command_descriptor: int | None = None
    build_info_descriptor: int | None = None
    command_temp_name = ""
    build_info_temp_name = ""
    try:
        project_descriptor = _open_project_directory(project)
        directory_descriptors.append((project, project_descriptor))
        try:
            lingtai_descriptor = _open_directory_at(
                project_descriptor, ".lingtai", lingtai_root, create=False
            )
        except FileNotFoundError as error:
            raise FileNotFoundError(
                f"LingTai project has no .lingtai directory: {project}"
            ) from error
        directory_descriptors.append((lingtai_root, lingtai_descriptor))
        runtime_descriptor = _open_directory_at(
            lingtai_descriptor, "runtime", runtime_root, create=True
        )
        directory_descriptors.append((runtime_root, runtime_descriptor))
        nokv_descriptor = _open_directory_at(
            runtime_descriptor, "nokv", nokv_root, create=True
        )
        directory_descriptors.append((nokv_root, nokv_descriptor))
        revision_descriptor = _open_directory_at(
            nokv_descriptor, revision, revision_dir, create=True
        )
        directory_descriptors.append((revision_dir, revision_descriptor))

        temp_descriptor, command_temp_name = _create_temp_file_at(
            revision_descriptor, ".nokv."
        )
        with os.fdopen(temp_descriptor, "wb") as target:
            copied_digest, copied_size = _copy_with_sha256(binary, target)
            os.fchmod(
                target.fileno(),
                stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP,
            )
            target.flush()
            os.fsync(target.fileno())
        if copied_digest != digest:
            raise ValueError(
                "nokv binary changed while staging: "
                f"initial SHA-256 {digest}, copied SHA-256 {copied_digest}"
            )

        digest_descriptor = _open_directory_at(
            revision_descriptor, digest, runtime_dir, create=True
        )
        directory_descriptors.append((runtime_dir, digest_descriptor))

        if _entry_metadata_at(digest_descriptor, "nokv") is None:
            try:
                os.link(
                    command_temp_name,
                    "nokv",
                    src_dir_fd=revision_descriptor,
                    dst_dir_fd=digest_descriptor,
                    follow_symlinks=False,
                )
            except FileExistsError:
                pass
            else:
                os.fsync(digest_descriptor)
        command_descriptor = _open_regular_file_at(digest_descriptor, "nokv", command)
        staged_digest, staged_size = _sha256_descriptor(command_descriptor)
        if staged_digest != digest:
            raise ValueError(
                f"content-addressed runtime was modified in place: {command}"
            )
        if staged_size != copied_size:
            raise ValueError(
                f"content-addressed runtime size differs from copied binary: {command}"
            )

        expected_build_info = BuildInfo(identity, digest, staged_size)
        if _entry_metadata_at(digest_descriptor, "build-info.json") is None:
            build_info_temp_descriptor, build_info_temp_name = _create_temp_file_at(
                digest_descriptor, ".build-info.json."
            )
            with os.fdopen(build_info_temp_descriptor, "w", encoding="utf-8") as handle:
                handle.write(_build_info_text(expected_build_info))
                handle.flush()
                os.fsync(handle.fileno())
            try:
                os.link(
                    build_info_temp_name,
                    "build-info.json",
                    src_dir_fd=digest_descriptor,
                    dst_dir_fd=digest_descriptor,
                    follow_symlinks=False,
                )
            except FileExistsError:
                pass
            else:
                os.fsync(digest_descriptor)

        build_info_descriptor = _open_regular_file_at(
            digest_descriptor, "build-info.json", build_info
        )
        if (
            _load_build_info_descriptor(build_info_descriptor, context=str(build_info))
            != expected_build_info
        ):
            raise ValueError(
                f"content-addressed build identity conflicts with {build_info}"
            )

        for path, descriptor in directory_descriptors:
            _verify_directory_binding(path, descriptor)
        _verify_regular_binding_at(
            digest_descriptor, "nokv", command_descriptor, command
        )
        _verify_regular_binding_at(
            digest_descriptor,
            "build-info.json",
            build_info_descriptor,
            build_info,
        )
    finally:
        if build_info_temp_name and digest_descriptor is not None:
            _unlink_at(digest_descriptor, build_info_temp_name)
        if command_temp_name and revision_descriptor is not None:
            _unlink_at(revision_descriptor, command_temp_name)
        if build_info_descriptor is not None:
            os.close(build_info_descriptor)
        if command_descriptor is not None:
            os.close(command_descriptor)
        for _, descriptor in reversed(directory_descriptors):
            os.close(descriptor)

    return StagedRuntime(
        command=command,
        sha256=digest,
        size_bytes=staged_size,
        identity=identity,
    )


def write_build_info(path: Path, identity: SourceIdentity, binary: Path) -> bool:
    path = path.expanduser().resolve()
    binary = binary.expanduser().resolve()
    build_info = BuildInfo(
        identity=identity,
        binary_sha256=sha256_file(binary),
        binary_size_bytes=binary.stat().st_size,
    )
    text = _build_info_text(build_info)
    if path.exists() and path.read_text(encoding="utf-8") == text:
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
        os.replace(tmp_name, path)
    finally:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)
    return True
