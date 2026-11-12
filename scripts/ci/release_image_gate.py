#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Resolve and recheck the immutable source identity for image releases."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import tomllib
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "release"))
from homebrew_source_release import ReleaseError, validate_stable_tag  # noqa: E402


SOURCE_REMOTE = "https://github.com/NoKV-Lab/NoKV.git"
HEX_40 = re.compile(r"[0-9a-f]{40}", re.ASCII)
RUST_VERSION = re.compile(
    r"[1-9][0-9]*\.(0|[1-9][0-9]*)(?:\.(0|[1-9][0-9]*))?", re.ASCII
)
DEBIAN_VERSIONS = frozenset({"bookworm", "trixie"})


def _git(repository: Path, *arguments: str) -> str:
    try:
        result = subprocess.run(
            ["git", "-C", str(repository), *arguments],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as error:
        raise ReleaseError(
            f"git {arguments[0]} failed: {error.stderr.strip()}"
        ) from error
    return result.stdout.strip()


def validate_inputs(tag: str, rust_version: str, debian_version: str) -> str:
    version = validate_stable_tag(tag)
    if RUST_VERSION.fullmatch(rust_version) is None:
        raise ReleaseError("Rust image version must be a numeric major.minor[.patch]")
    if debian_version not in DEBIAN_VERSIONS:
        raise ReleaseError("Debian image version must be bookworm or trixie")
    return version


def resolve_commit(repository: Path, tag: str, remote: str = SOURCE_REMOTE) -> str:
    validate_stable_tag(tag)
    _git(
        repository,
        "fetch",
        "--no-tags",
        "--force",
        remote,
        f"refs/tags/{tag}:refs/nokv-release/tag",
    )
    _git(
        repository,
        "fetch",
        "--no-tags",
        "--force",
        remote,
        "refs/heads/main:refs/nokv-release/main",
    )
    commit = _git(repository, "rev-parse", "refs/nokv-release/tag^{commit}")
    if HEX_40.fullmatch(commit) is None:
        raise ReleaseError("release tag did not resolve to a canonical SHA-1 commit")
    try:
        subprocess.run(
            [
                "git",
                "-C",
                str(repository),
                "merge-base",
                "--is-ancestor",
                commit,
                "refs/nokv-release/main",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as error:
        raise ReleaseError("release tag commit is not on canonical main") from error
    return commit


def verify_source(source: Path, tag: str, expected_commit: str) -> None:
    version = validate_stable_tag(tag)
    if HEX_40.fullmatch(expected_commit) is None:
        raise ReleaseError("expected release commit is not a canonical SHA-1")
    actual = _git(source, "rev-parse", "HEAD")
    if actual != expected_commit:
        raise ReleaseError(f"source checkout is {actual}, expected {expected_commit}")
    try:
        with (source / "crates/nokv/Cargo.toml").open("rb") as manifest:
            declared = tomllib.load(manifest).get("package", {}).get("version")
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise ReleaseError(f"cannot read nokv package version: {error}") from error
    if declared != version:
        raise ReleaseError(f"nokv package version {declared!r} differs from {tag}")


def resolve(arguments: argparse.Namespace) -> None:
    version = validate_inputs(
        arguments.tag, arguments.rust_version, arguments.debian_version
    )
    commit = resolve_commit(arguments.repository, arguments.tag, arguments.remote)
    with arguments.output.open("a", encoding="utf-8") as output:
        for key, value in (
            ("tag", arguments.tag),
            ("version", version),
            ("commit", commit),
            ("rust_version", arguments.rust_version),
            ("debian_version", arguments.debian_version),
        ):
            print(f"{key}={value}", file=output)


def verify(arguments: argparse.Namespace) -> None:
    if HEX_40.fullmatch(arguments.expected_commit) is None:
        raise ReleaseError("expected release commit is not a canonical SHA-1")
    actual = resolve_commit(arguments.repository, arguments.tag, arguments.remote)
    if actual != arguments.expected_commit:
        raise ReleaseError(
            f"release tag moved: expected {arguments.expected_commit}, found {actual}"
        )
    verify_source(arguments.source, arguments.tag, arguments.expected_commit)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    resolve_parser = commands.add_parser("resolve")
    resolve_parser.add_argument("--repository", type=Path, required=True)
    resolve_parser.add_argument("--remote", default=SOURCE_REMOTE)
    resolve_parser.add_argument("--tag", required=True)
    resolve_parser.add_argument("--rust-version", required=True)
    resolve_parser.add_argument("--debian-version", required=True)
    resolve_parser.add_argument("--output", type=Path, required=True)
    resolve_parser.set_defaults(handler=resolve)
    verify_parser = commands.add_parser("verify")
    verify_parser.add_argument("--repository", type=Path, required=True)
    verify_parser.add_argument("--remote", default=SOURCE_REMOTE)
    verify_parser.add_argument("--source", type=Path, required=True)
    verify_parser.add_argument("--tag", required=True)
    verify_parser.add_argument("--expected-commit", required=True)
    verify_parser.set_defaults(handler=verify)
    arguments = parser.parse_args()
    try:
        arguments.handler(arguments)
    except ReleaseError as error:
        print(f"release image gate: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
