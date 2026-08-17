#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Tests for closed live producer context and evidence publication."""

from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from source_bound_producer import ProducerError, ScenarioContract
from typed_live_qualification import load_live_context, publish_live_result


def canonical_sha256(value: object) -> str:
    payload = json.dumps(
        value, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


class TypedLiveQualificationTests(unittest.TestCase):
    def context_environment(self, binary: Path) -> dict[str, str]:
        subjects = {
            "dependencies": [
                {"name": "etcd", "identity": "sha256:" + "11" * 32},
                {"name": "object-store", "identity": "oci:example@sha256:" + "22" * 32},
            ],
            "product_binary": {
                "path": str(binary),
                "sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
            },
        }
        return {
            "NOKV_QUALIFICATION_PRODUCER": "live-workbench",
            "NOKV_QUALIFICATION_EVIDENCE_KIND": "live",
            "NOKV_QUALIFICATION_OPERATION_ID": "33" * 16,
            "NOKV_QUALIFICATION_SOURCE_SHA": "44" * 20,
            "NOKV_QUALIFICATION_COMMAND_ARGV_SHA256": "55" * 32,
            "NOKV_QUALIFICATION_SUBJECTS": json.dumps(
                subjects, separators=(",", ":"), sort_keys=True
            ),
            "NOKV_QUALIFICATION_SUBJECTS_SHA256": canonical_sha256(subjects),
            "NOKV_QUALIFICATION_CLAIMS": json.dumps(
                [
                    {
                        "stable_id": "T01",
                        "gate": "native-workbench-e2e",
                        "scenario": "t01.create-live",
                    }
                ],
                separators=(",", ":"),
                sort_keys=True,
            ),
            "NOKV_QUALIFICATION_REQUIRED_EVIDENCE_ROLES": json.dumps(
                ["producer-result", "qualification", "mcp-transcript"],
                separators=(",", ":"),
            ),
        }

    def test_live_context_binds_binary_dependencies_and_multiple_roles(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "nokv"
            binary.write_bytes(b"release identity")
            context = load_live_context(
                producer_id="live-workbench",
                scenarios={
                    "t01.create-live": ScenarioContract("T01", "native-workbench-e2e")
                },
                dependency_names=("etcd", "object-store"),
                product_binary=binary,
                evidence_roles=(
                    "producer-result",
                    "qualification",
                    "mcp-transcript",
                ),
                environ=self.context_environment(binary),
            )

        self.assertEqual(context.scenarios, ("t01.create-live",))

    def test_live_result_publishes_all_required_direct_children(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "nokv"
            binary.write_bytes(b"release identity")
            context = load_live_context(
                producer_id="live-workbench",
                scenarios={
                    "t01.create-live": ScenarioContract("T01", "native-workbench-e2e")
                },
                dependency_names=("etcd", "object-store"),
                product_binary=binary,
                evidence_roles=(
                    "producer-result",
                    "qualification",
                    "mcp-transcript",
                ),
                environ=self.context_environment(binary),
            )
            evidence = root / "evidence"
            evidence.mkdir()
            result = evidence / "producer-result.json"
            publish_live_result(
                result_path=result,
                context=context,
                outcome="NQ",
                qualification={"status": "NOT QUALIFIED", "reason": "no service"},
                evidence_roles=(
                    "producer-result",
                    "qualification",
                    "mcp-transcript",
                ),
            )

            value = json.loads(result.read_text(encoding="utf-8"))
            self.assertEqual(
                value["scenarios"]["t01.create-live"]["evidence_roles"],
                ["producer-result", "qualification", "mcp-transcript"],
            )
            self.assertTrue((evidence / "qualification.json").is_file())
            self.assertTrue((evidence / "mcp-transcript.jsonl").is_file())

    def test_live_context_rejects_a_different_binary_argument(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "nokv"
            other = root / "other"
            binary.write_bytes(b"release identity")
            other.write_bytes(b"different")
            with self.assertRaisesRegex(ProducerError, "binary argument"):
                load_live_context(
                    producer_id="live-workbench",
                    scenarios={
                        "t01.create-live": ScenarioContract(
                            "T01", "native-workbench-e2e"
                        )
                    },
                    dependency_names=("etcd", "object-store"),
                    product_binary=other,
                    evidence_roles=(
                        "producer-result",
                        "qualification",
                        "mcp-transcript",
                    ),
                    environ=self.context_environment(binary),
                )

    def test_live_pass_requires_a_real_transcript(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "nokv"
            binary.write_bytes(b"release identity")
            context = load_live_context(
                producer_id="live-workbench",
                scenarios={
                    "t01.create-live": ScenarioContract("T01", "native-workbench-e2e")
                },
                dependency_names=("etcd", "object-store"),
                product_binary=binary,
                evidence_roles=(
                    "producer-result",
                    "qualification",
                    "mcp-transcript",
                ),
                environ=self.context_environment(binary),
            )
            evidence = root / "evidence"
            evidence.mkdir()
            with self.assertRaisesRegex(ProducerError, "real MCP transcript"):
                publish_live_result(
                    result_path=evidence / "producer-result.json",
                    context=context,
                    outcome="PASS",
                    qualification={"status": "PASS"},
                    evidence_roles=(
                        "producer-result",
                        "qualification",
                        "mcp-transcript",
                    ),
                )
            self.assertEqual(list(evidence.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
