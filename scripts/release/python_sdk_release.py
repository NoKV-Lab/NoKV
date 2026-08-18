#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Validate and describe the NoKV Python SDK wheels attached to a GitHub release.

The wheels are built by `.github/workflows/release-python-sdk.yml` from one
validated tag commit. This module owns the release identities that must agree
before any wheel is published:

- the canonical `vMAJOR.MINOR.PATCH` tag, the `crates/nokv` package version,
  and the `crates/nokv-python` package version (the wheel version);
- the exact wheel set: one abi3 wheel per supported platform, nothing else;
- the manifest and checksum assets that let a consumer verify a downloaded
  wheel without trusting the download path.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
import tomllib
from pathlib import Path
from typing import Any, NoReturn


MANIFEST_SCHEMA = "nokv.python_sdk.release.v1"
DISTRIBUTION = "nokv"
API_VERSION = 1
PYTHON_TAG = "cp39"
ABI_TAG = "abi3"
STABLE_TAG = re.compile(
    r"v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)",
    re.ASCII,
)
HEX_40 = re.compile(r"[0-9a-f]{40}", re.ASCII)
WHEEL_NAME = re.compile(
    r"nokv-(?P<version>[0-9][0-9A-Za-z.]*)-cp39-abi3-(?P<platform>[A-Za-z0-9_.]+)\.whl",
    re.ASCII,
)
# One wheel per supported platform. Linux wheels are built inside the official
# manylinux_2_28 images (glibc >= 2.28); macOS wheels are built natively on
# Apple Silicon and Intel runners. The exact macOS deployment target in the tag
# is owned by maturin, so it is matched by architecture.
EXPECTED_PLATFORMS: dict[str, re.Pattern[str]] = {
    "linux-x86_64": re.compile(r"manylinux_2_28_x86_64", re.ASCII),
    "linux-aarch64": re.compile(r"manylinux_2_28_aarch64", re.ASCII),
    "macos-arm64": re.compile(r"macosx_[0-9]+_[0-9]+_arm64", re.ASCII),
    "macos-x86_64": re.compile(r"macosx_[0-9]+_[0-9]+_x86_64", re.ASCII),
}


class ReleaseError(ValueError):
    """The requested release is not safe or internally consistent."""


def validate_stable_tag(tag: str) -> str:
    """Return the plain version for one canonical vMAJOR.MINOR.PATCH tag."""
    if not isinstance(tag, str) or STABLE_TAG.fullmatch(tag) is None:
        raise ReleaseError(
            f"invalid release tag {tag!r}; expected canonical vMAJOR.MINOR.PATCH"
        )
    return tag[1:]


def _load_toml(path: Path) -> dict[str, Any]:
    try:
        with path.open("rb") as source:
            value = tomllib.load(source)
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise ReleaseError(f"cannot read TOML file {path}: {error}") from error
    if not isinstance(value, dict):
        raise ReleaseError(f"TOML root must be a table: {path}")
    return value


def _package_version(path: Path, name: str) -> str:
    package = _load_toml(path).get("package", {})
    version = package.get("version")
    if not isinstance(version, str) or not version:
        raise ReleaseError(f"{name} package version is missing in {path}")
    return version


def sdk_version(repository: Path) -> str:
    """Return the wheel version declared by the repository, after checking it
    agrees with the `crates/nokv` release line and that the Python project
    reads it from Cargo (not from a second hand-maintained copy)."""

    cli_version = _package_version(repository / "crates/nokv/Cargo.toml", "nokv")
    python_version = _package_version(
        repository / "crates/nokv-python/Cargo.toml", "nokv-python"
    )
    if python_version != cli_version:
        raise ReleaseError(
            f"crates/nokv-python version {python_version!r} does not match "
            f"crates/nokv version {cli_version!r}; bump both in the release change"
        )
    project = _load_toml(repository / "crates/nokv-python/pyproject.toml").get(
        "project", {}
    )
    if "version" in project or "version" not in project.get("dynamic", []):
        raise ReleaseError(
            "crates/nokv-python/pyproject.toml must declare `dynamic = [\"version\"]` "
            "and no static version so the wheel version comes from Cargo.toml"
        )
    return python_version


def validate_version(repository: Path, tag: str) -> str:
    version = validate_stable_tag(tag)
    declared = sdk_version(repository)
    if declared != version:
        raise ReleaseError(
            f"Python SDK version {declared!r} does not match tag {tag}"
        )
    return version


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def classify_wheels(dist: Path, version: str) -> dict[str, Path]:
    """Map every expected platform to exactly one wheel in `dist`.

    Fails closed on a missing platform, a duplicate platform, a foreign file,
    a wheel of another version, or a non-abi3 wheel.
    """

    if not dist.is_dir():
        raise ReleaseError(f"wheel directory does not exist: {dist}")
    found: dict[str, Path] = {}
    for entry in sorted(dist.iterdir()):
        if entry.name.startswith("."):
            continue
        match = WHEEL_NAME.fullmatch(entry.name)
        if match is None or not entry.is_file():
            raise ReleaseError(f"unexpected file in wheel directory: {entry.name}")
        if match.group("version") != version:
            raise ReleaseError(
                f"wheel {entry.name} is version {match.group('version')!r}, "
                f"expected {version!r}"
            )
        platform = match.group("platform")
        owners = [
            name
            for name, pattern in EXPECTED_PLATFORMS.items()
            if pattern.fullmatch(platform)
        ]
        if len(owners) != 1:
            raise ReleaseError(f"wheel platform tag {platform!r} is not a release target")
        if owners[0] in found:
            raise ReleaseError(f"duplicate wheel for {owners[0]}: {entry.name}")
        found[owners[0]] = entry
    missing = sorted(set(EXPECTED_PLATFORMS) - set(found))
    if missing:
        raise ReleaseError(f"missing wheels for: {', '.join(missing)}")
    return found


def build_manifest(
    *, version: str, tag: str, commit: str, wheels: dict[str, Path]
) -> dict[str, Any]:
    if HEX_40.fullmatch(commit) is None:
        raise ReleaseError(f"commit is not a canonical SHA-1: {commit!r}")
    if validate_stable_tag(tag) != version:
        raise ReleaseError(f"tag {tag} does not match version {version}")
    return {
        "schema": MANIFEST_SCHEMA,
        "distribution": DISTRIBUTION,
        "version": version,
        "api_version": API_VERSION,
        "tag": tag,
        "commit": commit,
        "python_tag": PYTHON_TAG,
        "abi_tag": ABI_TAG,
        "requires_python": ">=3.9",
        "wheels": {
            platform: {
                "file": path.name,
                "sha256": _sha256_file(path),
                "bytes": path.stat().st_size,
            }
            for platform, path in sorted(wheels.items())
        },
    }


def render_checksums(manifest: dict[str, Any]) -> str:
    lines = [
        f"{entry['sha256']}  {entry['file']}"
        for _platform, entry in sorted(manifest["wheels"].items())
    ]
    return "\n".join(lines) + "\n"


def write_release_assets(
    *, dist: Path, version: str, tag: str, commit: str, output: Path
) -> tuple[Path, Path]:
    wheels = classify_wheels(dist, version)
    manifest = build_manifest(version=version, tag=tag, commit=commit, wheels=wheels)
    output.mkdir(parents=True, exist_ok=True)
    manifest_path = output / f"nokv-{version}-python-sdk.json"
    checksums_path = output / f"nokv-{version}-python-sdk-SHA256SUMS"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    checksums_path.write_text(render_checksums(manifest))
    return manifest_path, checksums_path


def verify_manifest_wheels(manifest_path: Path, dist: Path) -> None:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("schema") != MANIFEST_SCHEMA:
        raise ReleaseError(f"unexpected manifest schema {manifest.get('schema')!r}")
    wheels = classify_wheels(dist, str(manifest["version"]))
    for platform, entry in manifest["wheels"].items():
        path = wheels.get(platform)
        if path is None or path.name != entry["file"]:
            raise ReleaseError(f"manifest wheel for {platform} is not in {dist}")
        actual = _sha256_file(path)
        if actual != entry["sha256"]:
            raise ReleaseError(
                f"wheel {path.name} sha256 {actual} differs from manifest {entry['sha256']}"
            )


def verify_installed_sdk(python: Path, version: str) -> None:
    """Import the installed distribution with the given interpreter and check
    that it is the release version with the frozen API version."""

    script = (
        "import json, nokv, importlib.metadata as m;"
        "print(json.dumps({'version': nokv.__version__, 'api': nokv.API_VERSION,"
        " 'dist': m.version('nokv'), 'file': nokv.__file__}))"
    )
    completed = subprocess.run(
        [str(python), "-c", script],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        raise ReleaseError(
            f"installed SDK import failed: {completed.stderr.strip()[-2000:]}"
        )
    report = json.loads(completed.stdout)
    if report["version"] != version or report["dist"] != version:
        raise ReleaseError(
            f"installed SDK reports version {report['version']!r} "
            f"(distribution {report['dist']!r}), expected {version!r}"
        )
    if report["api"] != API_VERSION:
        raise ReleaseError(f"installed SDK API version {report['api']!r} != {API_VERSION}")
    if "site-packages" not in report["file"] and "dist-packages" not in report["file"]:
        raise ReleaseError(
            f"installed SDK was imported from a source tree, not a wheel: {report['file']}"
        )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subcommands = parser.add_subparsers(dest="command", required=True)

    validate = subcommands.add_parser("validate-tag")
    validate.add_argument("tag")

    version = subcommands.add_parser("validate-version")
    version.add_argument("--repository", type=Path, default=Path.cwd())
    version.add_argument("--tag", required=True)
    version.set_defaults(
        handler=lambda args: print(validate_version(args.repository, args.tag))
    )

    declared = subcommands.add_parser("sdk-version")
    declared.add_argument("--repository", type=Path, default=Path.cwd())
    declared.set_defaults(handler=lambda args: print(sdk_version(args.repository)))

    assets = subcommands.add_parser("write-assets")
    assets.add_argument("--dist", type=Path, required=True)
    assets.add_argument("--version", required=True)
    assets.add_argument("--tag", required=True)
    assets.add_argument("--commit", required=True)
    assets.add_argument("--output", type=Path, required=True)
    assets.set_defaults(
        handler=lambda args: print(
            "\n".join(
                str(path)
                for path in write_release_assets(
                    dist=args.dist,
                    version=args.version,
                    tag=args.tag,
                    commit=args.commit,
                    output=args.output,
                )
            )
        )
    )

    verify_wheels = subcommands.add_parser("verify-wheels")
    verify_wheels.add_argument("--manifest", type=Path, required=True)
    verify_wheels.add_argument("--dist", type=Path, required=True)
    verify_wheels.set_defaults(
        handler=lambda args: verify_manifest_wheels(args.manifest, args.dist)
    )

    install = subcommands.add_parser("verify-install")
    install.add_argument("--python", type=Path, required=True)
    install.add_argument("--version", required=True)
    install.set_defaults(
        handler=lambda args: verify_installed_sdk(args.python, args.version)
    )
    return parser


def _die(message: str) -> NoReturn:
    print(f"python-sdk-release: {message}", file=sys.stderr)
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
