#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Build and verify a source-only NoKV Homebrew release."""

from __future__ import annotations

import argparse
import dataclasses
import gzip
import hashlib
import json
import re
import subprocess
import sys
import tarfile
import tomllib
import urllib.parse
from pathlib import Path, PurePosixPath
from typing import Any, NoReturn


MANIFEST_SCHEMA = "nokv.homebrew.source_release.v1"
TAP_MARKER_SCHEMA = "nokv.homebrew.public_tap.v1"
SOURCE_REPOSITORY = "NoKV-Lab/NoKV"
TAP_REPOSITORY = "NoKV-Lab/homebrew-tap"
WORKBENCH_TOOL_COUNT = 18
STABLE_TAG = re.compile(
    r"v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)",
    re.ASCII,
)
HEX_40 = re.compile(r"[0-9a-f]{40}", re.ASCII)
HEX_64 = re.compile(r"[0-9a-f]{64}", re.ASCII)
CRATES_IO_SOURCE = "registry+https://github.com/rust-lang/crates.io-index"


class ReleaseError(ValueError):
    """The requested release is not safe or internally consistent."""


@dataclasses.dataclass(frozen=True)
class HoltLock:
    version: str
    source: str
    checksum: str


@dataclasses.dataclass(frozen=True)
class WorkbenchContract:
    schema: str
    tool_count: int
    schemas_sha256: str


@dataclasses.dataclass(frozen=True)
class ReleaseMetadata:
    version: str
    tag: str
    commit: str
    tree: str
    cargo_lock_sha256: str
    holt: HoltLock
    workbench: WorkbenchContract


def validate_stable_tag(tag: str) -> str:
    """Return the plain version for one canonical vMAJOR.MINOR.PATCH tag."""
    if not isinstance(tag, str) or STABLE_TAG.fullmatch(tag) is None:
        raise ReleaseError(
            f"invalid release tag {tag!r}; expected canonical vMAJOR.MINOR.PATCH"
        )
    return tag[1:]


def _run(
    arguments: list[str],
    *,
    cwd: Path,
    capture_bytes: bool = False,
) -> str | bytes:
    try:
        completed = subprocess.run(
            arguments,
            cwd=cwd,
            check=True,
            capture_output=True,
            text=not capture_bytes,
        )
    except (OSError, subprocess.CalledProcessError) as error:
        detail = ""
        if isinstance(error, subprocess.CalledProcessError):
            stderr = error.stderr
            if isinstance(stderr, bytes):
                stderr = stderr.decode("utf-8", errors="replace")
            detail = f": {stderr.strip()}" if stderr else ""
        raise ReleaseError(f"command failed: {arguments!r}{detail}") from error
    if capture_bytes:
        return completed.stdout
    return completed.stdout.strip()


def _git(repository: Path, *arguments: str) -> str:
    output = _run(["git", *arguments], cwd=repository)
    if not isinstance(output, str):
        raise AssertionError("text git command returned bytes")
    return output


def _load_toml(path: Path) -> dict[str, Any]:
    try:
        with path.open("rb") as source:
            value = tomllib.load(source)
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise ReleaseError(f"cannot read TOML file {path}: {error}") from error
    if not isinstance(value, dict):
        raise ReleaseError(f"TOML root must be a table: {path}")
    return value


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ReleaseError(f"cannot read JSON file {path}: {error}") from error
    if not isinstance(value, dict):
        raise ReleaseError(f"JSON root must be an object: {path}")
    return value


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as error:
        raise ReleaseError(f"cannot hash {path}: {error}") from error
    return digest.hexdigest()


def _package_entries(lock: dict[str, Any], name: str) -> list[dict[str, Any]]:
    packages = lock.get("package")
    if not isinstance(packages, list):
        raise ReleaseError("Cargo.lock lacks a package array")
    return [
        package
        for package in packages
        if isinstance(package, dict) and package.get("name") == name
    ]


def read_holt_lock(lock_path: Path) -> HoltLock:
    entries = _package_entries(_load_toml(lock_path), "holt")
    if len(entries) != 1:
        raise ReleaseError(
            f"Cargo.lock must contain exactly one Holt package; found {len(entries)}"
        )
    entry = entries[0]
    version = entry.get("version")
    source = entry.get("source")
    checksum = entry.get("checksum")
    if not isinstance(version, str) or not version:
        raise ReleaseError("Holt lock entry lacks a version")
    if source != CRATES_IO_SOURCE:
        raise ReleaseError(f"Holt lock source is not canonical crates.io: {source!r}")
    if not isinstance(checksum, str) or HEX_64.fullmatch(checksum) is None:
        raise ReleaseError("Holt lock entry lacks a canonical SHA-256 checksum")
    return HoltLock(version=version, source=source, checksum=checksum)


def read_workbench_contract(repository: Path) -> WorkbenchContract:
    path = repository / "crates/nokv-agent/workbench_contract_schema.json"
    snapshot = _load_json(path)
    schema = snapshot.get("schema")
    schemas = snapshot.get("inputSchemas")
    if not isinstance(schema, str) or not schema:
        raise ReleaseError("Workbench contract snapshot lacks its schema marker")
    if not isinstance(schemas, dict):
        raise ReleaseError("Workbench contract snapshot lacks inputSchemas")
    if len(schemas) != WORKBENCH_TOOL_COUNT:
        raise ReleaseError(
            "Workbench contract must contain exactly "
            f"{WORKBENCH_TOOL_COUNT} tools; found {len(schemas)}"
        )
    encoded = json.dumps(
        schemas,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return WorkbenchContract(
        schema=schema,
        tool_count=len(schemas),
        schemas_sha256=_sha256_bytes(encoded),
    )


def _dependency_version(value: Any, name: str) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, dict) and isinstance(value.get("version"), str):
        return value["version"]
    raise ReleaseError(f"workspace dependency {name} lacks an explicit version")


def collect_release(
    repository: Path,
    tag: str,
    *,
    expected_commit: str | None = None,
    require_tag: bool = True,
    require_clean: bool = True,
) -> ReleaseMetadata:
    """Validate and collect all identities that define a NoKV source release."""
    version = validate_stable_tag(tag)
    repository = repository.resolve()
    if require_clean:
        dirty = _git(repository, "status", "--porcelain", "--untracked-files=all")
        if dirty:
            raise ReleaseError("release worktree is not clean")

    commit = _git(repository, "rev-parse", "HEAD")
    if HEX_40.fullmatch(commit) is None:
        raise ReleaseError(f"HEAD is not a canonical SHA-1 commit: {commit!r}")
    if expected_commit is not None:
        if HEX_40.fullmatch(expected_commit) is None:
            raise ReleaseError("expected commit is not a canonical SHA-1")
        if commit != expected_commit:
            raise ReleaseError(
                f"HEAD {commit} does not match validated commit {expected_commit}"
            )
    if require_tag:
        tag_commit = _git(repository, "rev-parse", f"refs/tags/{tag}^{{commit}}")
        if tag_commit != commit:
            raise ReleaseError(
                f"release tag {tag} does not point to HEAD; tag={tag_commit}, HEAD={commit}"
            )

    tree = _git(repository, "rev-parse", "HEAD^{tree}")
    if HEX_40.fullmatch(tree) is None:
        raise ReleaseError(f"HEAD tree is not a canonical SHA-1: {tree!r}")

    package = _load_toml(repository / "crates/nokv/Cargo.toml")
    package_version = package.get("package", {}).get("version")
    if package_version != version:
        raise ReleaseError(
            f"nokv package version {package_version!r} does not match tag {tag}"
        )

    workspace = _load_toml(repository / "Cargo.toml")
    dependencies = workspace.get("workspace", {}).get("dependencies")
    if not isinstance(dependencies, dict):
        raise ReleaseError("workspace.dependencies is missing")
    nokv_dependency_version = _dependency_version(dependencies.get("nokv"), "nokv")
    if nokv_dependency_version != version:
        raise ReleaseError(
            "workspace nokv dependency version "
            f"{nokv_dependency_version!r} does not match tag {tag}"
        )

    lock_path = repository / "Cargo.lock"
    lock = _load_toml(lock_path)
    nokv_entries = _package_entries(lock, "nokv")
    if len(nokv_entries) != 1 or nokv_entries[0].get("version") != version:
        versions = [entry.get("version") for entry in nokv_entries]
        raise ReleaseError(
            f"Cargo.lock nokv package versions {versions!r} do not match tag {tag}"
        )

    holt = read_holt_lock(lock_path)
    holt_pin = _dependency_version(dependencies.get("holt"), "holt")
    if holt_pin != f"={holt.version}":
        raise ReleaseError(
            f"Holt manifest pin {holt_pin!r} does not match locked ={holt.version}"
        )

    return ReleaseMetadata(
        version=version,
        tag=tag,
        commit=commit,
        tree=tree,
        cargo_lock_sha256=_sha256_file(lock_path),
        holt=holt,
        workbench=read_workbench_contract(repository),
    )


def _validate_archive(path: Path, metadata: ReleaseMetadata) -> None:
    expected_prefix = f"nokv-{metadata.version}"
    required = {
        f"{expected_prefix}/Cargo.lock",
        f"{expected_prefix}/Cargo.toml",
        f"{expected_prefix}/crates/nokv/Cargo.toml",
        f"{expected_prefix}/crates/nokv/src/main.rs",
        f"{expected_prefix}/crates/nokv-agent/workbench_contract_schema.json",
    }
    names: set[str] = set()
    try:
        with tarfile.open(path, "r:gz") as archive:
            for member in archive.getmembers():
                candidate = PurePosixPath(member.name)
                if candidate.is_absolute() or ".." in candidate.parts:
                    raise ReleaseError(f"source archive contains unsafe path {member.name!r}")
                if not candidate.parts or candidate.parts[0] != expected_prefix:
                    raise ReleaseError(
                        f"source archive path escapes {expected_prefix!r}: {member.name!r}"
                    )
                if ".git" in candidate.parts:
                    raise ReleaseError(f"source archive contains Git metadata: {member.name!r}")
                if member.issym() or member.islnk():
                    link = PurePosixPath(member.linkname)
                    if link.is_absolute() or ".." in link.parts:
                        raise ReleaseError(
                            f"source archive contains unsafe link {member.name!r}"
                        )
                names.add(member.name.rstrip("/"))
    except (OSError, tarfile.TarError) as error:
        raise ReleaseError(f"cannot validate source archive {path}: {error}") from error
    missing = required - names
    if missing:
        raise ReleaseError(f"source archive lacks required files: {sorted(missing)}")


def create_source_archive(
    repository: Path,
    metadata: ReleaseMetadata,
    output_directory: Path,
) -> Path:
    """Create a deterministic gzip-compressed archive from the validated commit."""
    repository = repository.resolve()
    output_directory.mkdir(parents=True, exist_ok=True)
    output = output_directory / f"nokv-{metadata.version}-source.tar.gz"
    tar_bytes = _run(
        [
            "git",
            "archive",
            "--format=tar",
            f"--prefix=nokv-{metadata.version}/",
            metadata.commit,
        ],
        cwd=repository,
        capture_bytes=True,
    )
    if not isinstance(tar_bytes, bytes):
        raise AssertionError("binary git archive command returned text")
    try:
        with output.open("wb") as raw:
            with gzip.GzipFile(
                filename="",
                mode="wb",
                fileobj=raw,
                compresslevel=9,
                mtime=0,
            ) as compressed:
                compressed.write(tar_bytes)
    except OSError as error:
        raise ReleaseError(f"cannot write source archive {output}: {error}") from error
    _validate_archive(output, metadata)
    return output


def build_manifest(metadata: ReleaseMetadata, archive: Path) -> dict[str, Any]:
    expected_name = f"nokv-{metadata.version}-source.tar.gz"
    if archive.name != expected_name:
        raise ReleaseError(
            f"source archive must be named {expected_name!r}, got {archive.name!r}"
        )
    _validate_archive(archive, metadata)
    return {
        "schema": MANIFEST_SCHEMA,
        "version": metadata.version,
        "tag": metadata.tag,
        "commit": metadata.commit,
        "tree": metadata.tree,
        "cargo_lock_sha256": metadata.cargo_lock_sha256,
        "holt": dataclasses.asdict(metadata.holt),
        "source": {
            "name": archive.name,
            "prefix": f"nokv-{metadata.version}",
            "sha256": _sha256_file(archive),
        },
        "workbench": dataclasses.asdict(metadata.workbench),
    }


def write_manifest(manifest: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def load_manifest(path: Path) -> dict[str, Any]:
    manifest = _load_json(path)
    if manifest.get("schema") != MANIFEST_SCHEMA:
        raise ReleaseError(f"manifest {path} has the wrong schema")
    version = manifest.get("version")
    tag = manifest.get("tag")
    if not isinstance(tag, str) or validate_stable_tag(tag) != version:
        raise ReleaseError("manifest tag and version do not match")
    if HEX_40.fullmatch(str(manifest.get("commit", ""))) is None:
        raise ReleaseError("manifest commit is not canonical")
    if HEX_64.fullmatch(str(manifest.get("cargo_lock_sha256", ""))) is None:
        raise ReleaseError("manifest Cargo.lock checksum is not canonical")
    source = manifest.get("source")
    if not isinstance(source, dict):
        raise ReleaseError("manifest lacks source identity")
    if source.get("name") != f"nokv-{version}-source.tar.gz":
        raise ReleaseError("manifest source name does not match version")
    if HEX_64.fullmatch(str(source.get("sha256", ""))) is None:
        raise ReleaseError("manifest source checksum is not canonical")
    return manifest


def verify_manifest_archive(manifest: dict[str, Any], archive: Path) -> None:
    source = manifest["source"]
    if archive.name != source["name"]:
        raise ReleaseError(
            f"archive name {archive.name!r} does not match manifest {source['name']!r}"
        )
    actual = _sha256_file(archive)
    if actual != source["sha256"]:
        raise ReleaseError(
            f"archive checksum {actual} does not match manifest {source['sha256']}"
        )


def _validate_source_url(manifest: dict[str, Any], source_url: str) -> None:
    parsed = urllib.parse.urlparse(source_url)
    if parsed.query or parsed.fragment or parsed.params:
        raise ReleaseError("source URL cannot contain query, fragment, or parameters")
    expected_name = manifest["source"]["name"]
    if parsed.scheme == "file":
        if not parsed.path.startswith("/") or PurePosixPath(parsed.path).name != expected_name:
            raise ReleaseError("local source URL must be absolute and use the exact archive name")
        return
    expected_path = (
        f"/{SOURCE_REPOSITORY}/releases/download/"
        f"{manifest['tag']}/{expected_name}"
    )
    if (
        parsed.scheme != "https"
        or parsed.netloc != "github.com"
        or parsed.path != expected_path
    ):
        raise ReleaseError(
            "release Formula must use the exact versioned GitHub release asset URL"
        )


def _ruby_string(value: str) -> str:
    if "\n" in value or "\r" in value or "\x00" in value:
        raise ReleaseError("Formula string contains a forbidden control character")
    return json.dumps(value, ensure_ascii=True)


def render_formula(manifest: dict[str, Any], source_url: str) -> str:
    if manifest.get("schema") != MANIFEST_SCHEMA:
        raise ReleaseError("cannot render Formula from an unknown manifest schema")
    _validate_source_url(manifest, source_url)
    holt = manifest.get("holt")
    workbench = manifest.get("workbench")
    source = manifest.get("source")
    if not all(isinstance(value, dict) for value in (holt, workbench, source)):
        raise ReleaseError("release manifest is incomplete")

    values = {
        "version": str(manifest["version"]),
        "url": source_url,
        "source_sha": str(source["sha256"]),
        "commit": str(manifest["commit"]),
        "lock_sha": str(manifest["cargo_lock_sha256"]),
        "holt_version": str(holt["version"]),
        "holt_source": str(holt["source"]),
        "holt_checksum": str(holt["checksum"]),
        "workbench_schema": str(workbench["schema"]),
    }
    return f'''# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

class Nokv < Formula
  desc "Metadata control plane and Workbench MCP server for agent workspaces"
  homepage "https://nokv.io/"
  url {_ruby_string(values["url"])}
  sha256 {_ruby_string(values["source_sha"])}
  license "Apache-2.0"

  depends_on "protobuf" => :build
  depends_on "rust" => :build

  def install
    ENV["NOKV_BUILD_GIT_COMMIT"] = {_ruby_string(values["commit"])}
    ENV["NOKV_BUILD_CARGO_LOCK_SHA256"] = {_ruby_string(values["lock_sha"])}
    ENV["NOKV_BUILD_HOLT_VERSION"] = {_ruby_string(values["holt_version"])}
    ENV["NOKV_BUILD_HOLT_SOURCE"] = {_ruby_string(values["holt_source"])}
    ENV["NOKV_BUILD_HOLT_CHECKSUM"] = {_ruby_string(values["holt_checksum"])}

    system "cargo", "install", *std_cargo_args(path: "crates/nokv")
  end

  test do
    assert_equal "nokv {values['version']}", shell_output("#{{bin}}/nokv --version").strip

    identity = JSON.parse(shell_output("#{{bin}}/nokv version --json"))
    assert_equal {_ruby_string(values["version"])}, identity.fetch("version")
    assert_equal {_ruby_string(values["commit"])}, identity.fetch("git_commit")
    assert_equal {_ruby_string(values["lock_sha"])},
                 identity.fetch("cargo_lock_sha256")
    assert_equal {_ruby_string(values["holt_version"])}, identity.dig("holt", "version")
    assert_equal {_ruby_string(values["holt_source"])}, identity.dig("holt", "source")
    assert_equal {_ruby_string(values["holt_checksum"])}, identity.dig("holt", "checksum")
    assert_equal {_ruby_string(values["workbench_schema"])}, identity.fetch("workbench_contract_schema")
    assert_equal {WORKBENCH_TOOL_COUNT}, identity.fetch("workbench_tool_count")

    schema = JSON.parse(shell_output("#{{bin}}/nokv schema"))
    assert_equal {_ruby_string(values["workbench_schema"])}, schema.fetch("schema")
    assert_equal {WORKBENCH_TOOL_COUNT}, schema.fetch("tools").length
  end
end
'''


def _same(actual: Any, expected: Any, field: str) -> None:
    if actual != expected:
        raise ReleaseError(
            f"installed {field} {actual!r} does not match release {expected!r}"
        )


def verify_installed_identity(
    manifest: dict[str, Any],
    version_payload: dict[str, Any],
    schema_payload: dict[str, Any],
) -> None:
    if manifest.get("schema") != MANIFEST_SCHEMA:
        raise ReleaseError("release manifest has the wrong schema")
    holt = manifest["holt"]
    workbench = manifest["workbench"]
    _same(version_payload.get("version"), manifest["version"], "version")
    _same(version_payload.get("git_commit"), manifest["commit"], "commit")
    _same(
        version_payload.get("cargo_lock_sha256"),
        manifest["cargo_lock_sha256"],
        "Cargo.lock checksum",
    )
    _same(version_payload.get("holt"), holt, "Holt identity")
    _same(
        version_payload.get("workbench_contract_schema"),
        workbench["schema"],
        "Workbench contract schema",
    )
    _same(
        version_payload.get("workbench_tool_count"),
        workbench["tool_count"],
        "Workbench tool count",
    )
    _same(schema_payload.get("schema"), workbench["schema"], "live schema")
    tools = schema_payload.get("tools")
    if not isinstance(tools, list):
        raise ReleaseError("installed schema does not contain a tools array")
    _same(len(tools), workbench["tool_count"], "live Workbench tool count")


def verify_tap_marker(marker_path: Path) -> None:
    marker = _load_json(marker_path)
    expected = {
        "schema": TAP_MARKER_SCHEMA,
        "repository": TAP_REPOSITORY,
        "visibility": "public",
        "source_repository": SOURCE_REPOSITORY,
    }
    if marker != expected:
        raise ReleaseError(
            f"tap marker does not identify the exact public repository: {marker!r}"
        )


def _read_command_json(arguments: list[str]) -> dict[str, Any]:
    try:
        completed = subprocess.run(
            arguments,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as error:
        stderr = getattr(error, "stderr", "") or ""
        raise ReleaseError(f"installed command failed: {arguments!r}: {stderr.strip()}") from error
    try:
        value = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise ReleaseError(f"installed command returned invalid JSON: {error}") from error
    if not isinstance(value, dict):
        raise ReleaseError("installed command JSON must be an object")
    return value


def _command_prepare(arguments: argparse.Namespace) -> None:
    metadata = collect_release(
        arguments.repository,
        arguments.tag,
        expected_commit=arguments.expected_commit,
        require_tag=not arguments.no_local_tag,
    )
    archive = create_source_archive(arguments.repository, metadata, arguments.output)
    manifest = build_manifest(metadata, archive)
    manifest_path = arguments.output / f"nokv-{metadata.version}-homebrew.json"
    write_manifest(manifest, manifest_path)
    checksums = arguments.output / "SHA256SUMS"
    checksums.write_text(
        f"{_sha256_file(archive)}  {archive.name}\n"
        f"{_sha256_file(manifest_path)}  {manifest_path.name}\n",
        encoding="utf-8",
    )
    print(manifest_path)


def _command_render(arguments: argparse.Namespace) -> None:
    manifest = load_manifest(arguments.manifest)
    formula = render_formula(manifest, arguments.source_url)
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(formula, encoding="utf-8")


def _command_verify_archive(arguments: argparse.Namespace) -> None:
    manifest = load_manifest(arguments.manifest)
    verify_manifest_archive(manifest, arguments.archive)


def _command_verify_install(arguments: argparse.Namespace) -> None:
    manifest = load_manifest(arguments.manifest)
    version = _read_command_json([str(arguments.binary), "version", "--json"])
    schema = _read_command_json([str(arguments.binary), "schema"])
    verify_installed_identity(manifest, version, schema)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subcommands = parser.add_subparsers(dest="command", required=True)

    validate = subcommands.add_parser("validate-tag")
    validate.add_argument("tag")

    prepare = subcommands.add_parser("prepare")
    prepare.add_argument("--repository", type=Path, default=Path.cwd())
    prepare.add_argument("--tag", required=True)
    prepare.add_argument("--expected-commit")
    prepare.add_argument("--output", type=Path, required=True)
    prepare.add_argument("--no-local-tag", action="store_true")
    prepare.set_defaults(handler=_command_prepare)

    render = subcommands.add_parser("render-formula")
    render.add_argument("--manifest", type=Path, required=True)
    render.add_argument("--source-url", required=True)
    render.add_argument("--output", type=Path, required=True)
    render.set_defaults(handler=_command_render)

    archive = subcommands.add_parser("verify-archive")
    archive.add_argument("--manifest", type=Path, required=True)
    archive.add_argument("--archive", type=Path, required=True)
    archive.set_defaults(handler=_command_verify_archive)

    install = subcommands.add_parser("verify-install")
    install.add_argument("--manifest", type=Path, required=True)
    install.add_argument("--binary", type=Path, required=True)
    install.set_defaults(handler=_command_verify_install)

    tap = subcommands.add_parser("verify-tap")
    tap.add_argument("--marker", type=Path, required=True)
    tap.set_defaults(handler=lambda args: verify_tap_marker(args.marker))
    return parser


def _die(message: str) -> NoReturn:
    print(f"homebrew-source-release: {message}", file=sys.stderr)
    raise SystemExit(2)


def main() -> None:
    parser = _parser()
    arguments = parser.parse_args()
    try:
        if arguments.command == "validate-tag":
            print(validate_stable_tag(arguments.tag))
            return
        arguments.handler(arguments)
    except ReleaseError as error:
        _die(str(error))


if __name__ == "__main__":
    main()
