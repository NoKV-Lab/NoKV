#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Container image metadata for NoKV images, and a check of a built image.

`emit` prints the OCI labels and annotations every NoKV image carries, as the
multi-line outputs `labels` and `annotations` of a GitHub Actions step.
Annotations go on the image index and on each platform manifest: Artifact Hub
reads a multi-architecture image's metadata from its index.

`check` opens an OCI image layout (a directory or a tar file, as `docker buildx
build --output type=oci` writes it) and fails unless the image index carries
every key Artifact Hub requires, with the values `emit` would give.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import tarfile
from datetime import datetime
from pathlib import Path

REPOSITORY = "https://github.com/NoKV-Lab/NoKV"
RAW = "https://raw.githubusercontent.com/NoKV-Lab/NoKV"
# The package page Artifact Hub renders: plain Markdown with absolute links.
README_PATH = "docs/container-image.md"

STATIC = {
    "org.opencontainers.image.title": "NoKV",
    "org.opencontainers.image.description": (
        "Durable, versioned workspaces for disposable agents: the nokv owner and CLI, "
        "with a transactional metadata control plane over S3-compatible artifact bytes."
    ),
    "org.opencontainers.image.url": "https://nokv.io",
    "org.opencontainers.image.source": REPOSITORY,
    "org.opencontainers.image.vendor": "NoKV-Lab",
    "org.opencontainers.image.licenses": "Apache-2.0",
    "io.artifacthub.package.license": "Apache-2.0",
    "io.artifacthub.package.category": "storage",
    "io.artifacthub.package.keywords": "agents,workspaces,metadata,object-storage,s3,versioning,rust",
    "io.artifacthub.package.logo-url": f"{RAW}/main/docs/public/img/logo.png",
}

# Keys Artifact Hub requires on a container image.
REQUIRED = (
    "io.artifacthub.package.readme-url",
    "org.opencontainers.image.created",
    "org.opencontainers.image.description",
)

STABLE_VERSION = re.compile(r"v?[0-9]+\.[0-9]+\.[0-9]+", re.ASCII)
GIT_REF = re.compile(r"[0-9A-Za-z][0-9A-Za-z._/-]*", re.ASCII)
INDEX_MEDIA_TYPES = (
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
)


class MetadataError(Exception):
    pass


def metadata(ref: str, version: str, revision: str, created: str) -> dict[str, str]:
    """Every label and annotation value of an image built from `ref`."""
    if not GIT_REF.fullmatch(ref) or ".." in ref:
        raise MetadataError(f"not a git ref: {ref!r}")
    if not version or any(character.isspace() for character in version):
        raise MetadataError(f"not a version: {version!r}")
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise MetadataError(f"not a commit id: {revision!r}")
    try:
        datetime.fromisoformat(created.replace("Z", "+00:00"))
    except ValueError as error:
        raise MetadataError(f"not an RFC 3339 time: {created!r}") from error
    values = dict(STATIC)
    values.update({
        "org.opencontainers.image.version": version,
        "org.opencontainers.image.revision": revision,
        "org.opencontainers.image.created": created,
        "org.opencontainers.image.documentation": f"{REPOSITORY}/tree/{ref}/docs",
        "io.artifacthub.package.readme-url": f"{RAW}/{ref}/{README_PATH}",
        "io.artifacthub.package.prerelease": "false" if STABLE_VERSION.fullmatch(version) else "true",
    })
    return values


def github_outputs(values: dict[str, str]) -> str:
    """The `labels` and `annotations` step outputs, one `key=value` per line.
    Annotations target the index and every platform manifest."""
    for key, value in values.items():
        if "\n" in value or "=" in key:
            raise MetadataError(f"metadata cannot carry {key!r}")
    labels = "\n".join(f"{key}={value}" for key, value in values.items())
    annotations = "\n".join(f"index,manifest:{key}={value}" for key, value in values.items())
    return f"labels<<EOF\n{labels}\nEOF\nannotations<<EOF\n{annotations}\nEOF\n"


class Layout:
    """Read access to an OCI image layout in a directory or a tar file."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.tar = tarfile.open(path) if path.is_file() else None
        # Exporters write members as "index.json" or "./index.json".
        self.members = {} if self.tar is None else {
            member.name.removeprefix("./"): member for member in self.tar.getmembers() if member.isfile()
        }

    def read(self, name: str) -> bytes:
        if self.tar is None:
            return (self.path / name).read_bytes()
        member = self.members.get(name)
        if member is None:
            raise MetadataError(f"{name} is missing from {self.path}")
        return self.tar.extractfile(member).read()

    def blob(self, digest: str) -> dict:
        algorithm, _, encoded = digest.partition(":")
        return json.loads(self.read(f"blobs/{algorithm}/{encoded}"))


def image_index(layout: Layout) -> dict:
    """The image index the layout holds: the layout's own index when it
    annotates the image, otherwise the one index it references."""
    top = json.loads(layout.read("index.json"))
    nested = [entry for entry in top.get("manifests", []) if entry.get("mediaType") in INDEX_MEDIA_TYPES]
    if len(nested) == 1:
        return layout.blob(nested[0]["digest"])
    if nested:
        raise MetadataError(f"the layout holds {len(nested)} image indexes, expected one")
    return top


def check(path: Path, expected: dict[str, str] | None) -> list[str]:
    """Problems with the image index in the layout at `path`, if any."""
    layout = Layout(path)
    index = image_index(layout)
    annotations = index.get("annotations") or {}
    problems = []
    for key in REQUIRED:
        value = annotations.get(key)
        if not isinstance(value, str) or not value:
            problems.append(f"the image index has no {key} annotation")
    readme = annotations.get("io.artifacthub.package.readme-url", "")
    if readme and not (readme.startswith(f"{RAW}/") and readme.endswith(".md")):
        problems.append(f"readme-url is not a raw Markdown URL: {readme}")
    for key, value in (expected or {}).items():
        if annotations.get(key) != value:
            problems.append(f"index annotation {key} is {annotations.get(key)!r}, expected {value!r}")
    # Attestation manifests are listed with an unknown platform and carry no
    # image metadata.
    images = [entry for entry in index.get("manifests", []) if entry.get("platform", {}).get("os") != "unknown"]
    if len(images) < 2:
        problems.append("the image index lists fewer than two platform manifests")
    for entry in images:
        manifest = layout.blob(entry["digest"])
        platform = entry.get("platform", {})
        missing = [key for key in REQUIRED if not (manifest.get("annotations") or {}).get(key)]
        if missing:
            problems.append(f"the {platform.get('architecture', '?')} manifest lacks {', '.join(missing)}")
    return problems


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("emit", "check"):
        command = commands.add_parser(name)
        command.add_argument("--ref", required=name == "emit", help="git ref the image is built from")
        command.add_argument("--version", required=name == "emit")
        command.add_argument("--revision", required=name == "emit", help="40-hex commit id")
        command.add_argument("--created", required=name == "emit", help="RFC 3339 build time")
        if name == "check":
            command.add_argument("layout", type=Path, help="OCI layout directory or tar")
    args = parser.parse_args(argv)
    try:
        if args.command == "emit":
            sys.stdout.write(github_outputs(metadata(args.ref, args.version, args.revision, args.created)))
            return 0
        expected = None
        if args.ref:
            expected = metadata(args.ref, args.version, args.revision, args.created)
        problems = check(args.layout, expected)
    except (MetadataError, OSError, KeyError, ValueError, tarfile.TarError) as error:
        print(f"image metadata: {error}", file=sys.stderr)
        return 1
    for problem in problems:
        print(f"image metadata: {problem}", file=sys.stderr)
    if not problems:
        print("image metadata: the image index carries the Artifact Hub metadata")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
