#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0
"""Regression tests for container image metadata."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import tarfile
import tempfile
import unittest

from image_metadata import REQUIRED, MetadataError, check, github_outputs, metadata

REVISION = "5c383d4a90" + "0" * 30
CREATED = "2026-10-31T05:45:59Z"


def values(version: str = "v0.13.0", ref: str = "v0.13.0") -> dict[str, str]:
    return metadata(ref, version, REVISION, CREATED)


class Blobs:
    """An OCI image layout written the way buildx exports one."""

    def __init__(self, root: Path) -> None:
        self.root = root
        (root / "blobs" / "sha256").mkdir(parents=True)

    def put(self, document: dict) -> str:
        data = json.dumps(document).encode()
        digest = hashlib.sha256(data).hexdigest()
        (self.root / "blobs" / "sha256" / digest).write_bytes(data)
        return f"sha256:{digest}"


def layout(root: Path, index_annotations: dict[str, str], manifest_annotations: dict[str, str],
           platforms: tuple[str, ...] = ("amd64", "arm64")) -> Path:
    blobs = Blobs(root)
    entries = []
    for architecture in platforms:
        digest = blobs.put({"schemaVersion": 2, "annotations": manifest_annotations})
        entries.append({"mediaType": "application/vnd.oci.image.manifest.v1+json", "digest": digest,
                        "platform": {"architecture": architecture, "os": "linux"}})
    attestation = blobs.put({"schemaVersion": 2})
    entries.append({"mediaType": "application/vnd.oci.image.manifest.v1+json", "digest": attestation,
                    "platform": {"architecture": "unknown", "os": "unknown"}})
    index = blobs.put({"schemaVersion": 2, "mediaType": "application/vnd.oci.image.index.v1+json",
                       "manifests": entries, "annotations": index_annotations})
    (root / "index.json").write_text(json.dumps({
        "schemaVersion": 2,
        "manifests": [{"mediaType": "application/vnd.oci.image.index.v1+json", "digest": index}],
    }))
    return root


class MetadataTest(unittest.TestCase):
    def test_every_required_key_is_emitted_with_a_raw_readme_pinned_to_the_ref(self) -> None:
        emitted = values()
        for key in REQUIRED:
            self.assertTrue(emitted[key], key)
        self.assertEqual(
            emitted["io.artifacthub.package.readme-url"],
            "https://raw.githubusercontent.com/NoKV-Lab/NoKV/v0.13.0/docs/container-image.md",
        )
        self.assertEqual(emitted["org.opencontainers.image.version"], "v0.13.0")
        self.assertEqual(emitted["org.opencontainers.image.created"], CREATED)
        self.assertTrue(Path(__file__).resolve().parents[2].joinpath("docs/container-image.md").is_file())

    def test_only_a_stable_version_is_not_a_prerelease(self) -> None:
        self.assertEqual(values("v0.13.0")["io.artifacthub.package.prerelease"], "false")
        self.assertEqual(values("0.13.0")["io.artifacthub.package.prerelease"], "false")
        for version in ("v0.13.0-rc.1", "dev-5c383d4a90"):
            self.assertEqual(values(version)["io.artifacthub.package.prerelease"], "true", version)

    def test_inputs_that_cannot_name_a_build_are_refused(self) -> None:
        for ref, version, revision, created in (
            ("v0.13.0;rm", "v0.13.0", REVISION, CREATED),
            ("../main", "v0.13.0", REVISION, CREATED),
            ("v0.13.0", "", REVISION, CREATED),
            ("v0.13.0", "v0.13.0", "abc", CREATED),
            ("v0.13.0", "v0.13.0", REVISION, "yesterday"),
        ):
            with self.subTest(ref=ref, version=version, revision=revision, created=created):
                with self.assertRaises(MetadataError):
                    metadata(ref, version, revision, created)

    def test_outputs_put_every_annotation_on_the_index_and_the_manifests(self) -> None:
        outputs = github_outputs(values())
        labels, annotations = outputs.split("annotations<<EOF\n")
        self.assertIn("org.opencontainers.image.created=2026-10-31T05:45:59Z\n", labels)
        lines = annotations.removesuffix("EOF\n").splitlines()
        self.assertEqual(len(lines), len(values()))
        self.assertTrue(all(line.startswith("index,manifest:") for line in lines))


class CheckTest(unittest.TestCase):
    def test_a_layout_with_the_metadata_passes_as_a_directory_and_as_a_tar(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = layout(Path(directory) / "layout", values(), values())
            self.assertEqual(check(root, values()), [])
            archive = Path(directory) / "image.tar"
            with tarfile.open(archive, "w") as tar:
                tar.add(root, arcname=".")
            self.assertEqual(check(archive, values()), [])

    def test_an_index_without_the_required_keys_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            missing = {key: value for key, value in values().items() if key not in REQUIRED}
            problems = check(layout(Path(directory), missing, values()), None)
        self.assertEqual(len(problems), len(REQUIRED), problems)

    def test_manifest_annotations_and_both_platforms_are_required(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            problems = check(layout(Path(directory), values(), {}, platforms=("amd64",)), None)
        self.assertTrue(any("fewer than two platform manifests" in problem for problem in problems), problems)
        self.assertTrue(any("amd64 manifest lacks" in problem for problem in problems), problems)

    def test_values_other_than_the_expected_ones_fail(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            built = values(ref="main")
            problems = check(layout(Path(directory), built, built), values())
        self.assertTrue(any("readme-url" in problem for problem in problems), problems)


if __name__ == "__main__":
    unittest.main()
