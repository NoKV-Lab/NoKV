#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Tests for fail-closed pre-#423 qualification aggregation."""

from __future__ import annotations

import copy
import hashlib
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from typing import Callable
from unittest import mock


SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

import pre423_contract_ledger as ledger_module  # noqa: E402
import qualification_aggregate as aggregate_module  # noqa: E402
import qualification_receipt as receipt_module  # noqa: E402


PRODUCER_FIXTURE = r"""#!/usr/bin/env python3
import argparse
import json
import os
import pathlib

parser = argparse.ArgumentParser()
parser.add_argument("--qualification-result", required=True)
args = parser.parse_args()
claims = json.loads(os.environ["NOKV_QUALIFICATION_CLAIMS"])
roles = json.loads(os.environ["NOKV_QUALIFICATION_REQUIRED_EVIDENCE_ROLES"])
result = {
    "schema": "nokv.pre423.producer_result.v1",
    "producer": os.environ["NOKV_QUALIFICATION_PRODUCER"],
    "evidence_kind": os.environ["NOKV_QUALIFICATION_EVIDENCE_KIND"],
    "operation_id": os.environ["NOKV_QUALIFICATION_OPERATION_ID"],
    "source_sha": os.environ["NOKV_QUALIFICATION_SOURCE_SHA"],
    "command_argv_sha256": os.environ["NOKV_QUALIFICATION_COMMAND_ARGV_SHA256"],
    "subjects": json.loads(os.environ["NOKV_QUALIFICATION_SUBJECTS"]),
    "subjects_sha256": os.environ["NOKV_QUALIFICATION_SUBJECTS_SHA256"],
    "scenarios": {
        claim["scenario"]: {"outcome": "PASS", "evidence_roles": roles}
        for claim in claims
    },
}
path = pathlib.Path(args.qualification_result)
path.parent.mkdir(parents=True, exist_ok=True)
path.write_text(json.dumps(result), encoding="utf-8")
"""


def _dependency_identity(kinds: list[str], seed: str) -> str:
    kind = kinds[0]
    digest = hashlib.sha256(seed.encode()).hexdigest()
    if kind == "git":
        return f"git:{digest[:40]}"
    if kind == "oci":
        return f"oci:example/{seed}@sha256:{digest}"
    return f"sha256:{digest}"


class QualificationAggregateTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.repo = Path(self.temporary.name) / "source-repo"
        self.repo.mkdir()
        subprocess.run(["git", "init"], cwd=self.repo, check=True, capture_output=True)
        subprocess.run(
            ["git", "config", "user.name", "Qualification Test"],
            cwd=self.repo,
            check=True,
        )
        subprocess.run(
            ["git", "config", "user.email", "qualification@example.invalid"],
            cwd=self.repo,
            check=True,
        )
        self.bundle = Path(self.temporary.name) / "bundle"
        self.receipt_dir = self.bundle / "receipts"
        self.receipt_dir.mkdir(parents=True)
        self.product_artifact_manifest = (
            Path(self.temporary.name) / "trusted-provenance" / "product-artifacts.json"
        )
        self.ledger = ledger_module.load_ledger()
        ledger_module.validate_ledger(self.ledger)
        self.rust_toolchain = receipt_module.derive_rust_toolchain_subject(
            repo=self.repo
        )
        for producer_contract in self.ledger["producer_catalog"].values():
            entrypoint = self.repo / producer_contract["command"]["entrypoint"]
            entrypoint.parent.mkdir(parents=True, exist_ok=True)
            entrypoint.write_text(PRODUCER_FIXTURE, encoding="utf-8")
        self.product_binary = self.repo / "target" / "release" / "nokv"
        self.product_binary.parent.mkdir(parents=True)
        self.product_binary.write_bytes(b"qualification binary\x00")
        self.product_binary = self.product_binary.resolve()
        subprocess.run(["git", "add", "."], cwd=self.repo, check=True)
        subprocess.run(
            ["git", "commit", "-m", "source"],
            cwd=self.repo,
            check=True,
            capture_output=True,
        )
        self.source_sha = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=self.repo,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _write_complete_receipts(self) -> list[Path]:
        paths = []
        for item in self.ledger["items"]:
            for gate in item["required_gates"]:
                expectation = ledger_module.resolve_gate_expectation(
                    self.ledger, item["id"], gate
                )
                producer = expectation["allowed_producers"][0]
                producer_contract = self.ledger["producer_catalog"][producer]
                evidence_kind = expectation["allowed_evidence_kinds"][0]
                run_name = f"{item['id']}-{gate}"
                run_dir = self.bundle / "runs" / run_name
                run_dir.mkdir(parents=True, exist_ok=True)
                operation_id = f"operation-{run_name}"
                subjects: dict[str, object] = {
                    "dependencies": [
                        {
                            "name": name,
                            "identity": _dependency_identity(kinds, name),
                        }
                        for name, kinds in sorted(
                            producer_contract["required_dependencies"].items()
                        )
                    ]
                }
                if "product_binary" in producer_contract["required_subjects"]:
                    subjects["product_binary"] = {
                        "path": str(self.product_binary),
                        "sha256": hashlib.sha256(
                            self.product_binary.read_bytes()
                        ).hexdigest(),
                    }
                if "rust_toolchain" in producer_contract["required_subjects"]:
                    subjects["rust_toolchain"] = self.rust_toolchain
                result_source = str(
                    (self.bundle / "producer-evidence" / f"{run_name}.json").resolve()
                )
                executable = str(Path(sys.executable).resolve())
                entrypoint_path = self.repo / producer_contract["command"]["entrypoint"]
                argv = [
                    executable,
                    str(entrypoint_path),
                    producer_contract["command"]["result_argument"],
                    result_source,
                ]
                binary_argument = producer_contract["command"]["binary_argument"]
                if binary_argument is not None:
                    argv.extend([binary_argument, subjects["product_binary"]["path"]])
                argv_sha = ledger_module.json_sha256(argv)
                producer_result = {
                    "schema": receipt_module.PRODUCER_RESULT_SCHEMA,
                    "producer": producer,
                    "evidence_kind": evidence_kind,
                    "operation_id": operation_id,
                    "source_sha": self.source_sha,
                    "command_argv_sha256": argv_sha,
                    "subjects": subjects,
                    "subjects_sha256": ledger_module.json_sha256(subjects),
                    "scenarios": {
                        scenario: {
                            "outcome": "PASS",
                            "evidence_roles": producer_contract[
                                "required_evidence_roles"
                            ],
                        }
                        for scenario in expectation["scenarios"]
                    },
                }
                evidence_payloads = {
                    "stdout": f"{item['id']}:{gate}\n".encode(),
                    "stderr": b"",
                    "producer-result": json.dumps(
                        producer_result, sort_keys=True
                    ).encode(),
                }
                for role in producer_contract["required_evidence_roles"]:
                    evidence_payloads.setdefault(
                        role, f"{role}:{item['id']}:{gate}\n".encode()
                    )
                evidence_entries = []
                for index, (role, payload) in enumerate(evidence_payloads.items()):
                    evidence_path = run_dir / f"{index:02d}-{role}.bin"
                    evidence_path.write_bytes(payload)
                    evidence_entries.append(
                        {
                            "role": role,
                            "path": evidence_path.relative_to(self.bundle).as_posix(),
                            "sha256": hashlib.sha256(payload).hexdigest(),
                            "size_bytes": len(payload),
                            "media_type": "application/octet-stream",
                        }
                    )
                receipt = {
                    "schema": "nokv.pre423.qualification_receipt.v1",
                    "stable_id": item["id"],
                    "gate": gate,
                    "scenario_ids": expectation["scenarios"],
                    "evidence_kind": evidence_kind,
                    "outcome": "PASS",
                    "source": {
                        "repository": "NoKV",
                        "sha": self.source_sha,
                        "dirty": False,
                        "ledger_item_sha256": ledger_module.json_sha256(item),
                        "gate_expectation_sha256": ledger_module.json_sha256(
                            expectation
                        ),
                        "qualification_policy_sha256": (
                            ledger_module.QUALIFICATION_POLICY_SHA256
                        ),
                    },
                    "execution": {
                        "producer": producer,
                        "workflow_run_id": "run-1",
                        "job": f"{producer}-qualification",
                        "attempt": 1,
                        "operation_id": operation_id,
                        "argv": argv,
                        "command_argv_sha256": argv_sha,
                        "command_contract_sha256": ledger_module.json_sha256(
                            producer_contract
                        ),
                        "entrypoint": producer_contract["command"]["entrypoint"],
                        "entrypoint_sha256": hashlib.sha256(
                            entrypoint_path.read_bytes()
                        ).hexdigest(),
                        "executable": executable,
                        "executable_sha256": hashlib.sha256(
                            Path(executable).read_bytes()
                        ).hexdigest(),
                        "producer_result_source_path": result_source,
                        "cwd": str(self.repo),
                        "started_at": "2026-08-16T00:00:00Z",
                        "finished_at": "2026-08-16T00:00:01Z",
                        "exit_code": 0,
                        "producer_result_sha256": ledger_module.json_sha256(
                            producer_result
                        ),
                    },
                    "subjects": subjects,
                    "evidence": evidence_entries,
                }
                path = self.receipt_dir / f"{item['id']}-{gate}.json"
                path.write_text(json.dumps(receipt), encoding="utf-8")
                paths.append(path)
        return paths

    def _write_product_artifact_manifest(
        self, *, mutate: Callable[[dict[str, object]], None] | None = None
    ) -> Path:
        binary_sha256 = hashlib.sha256(self.product_binary.read_bytes()).hexdigest()
        artifacts = []
        for index, (producer, contract) in enumerate(
            sorted(self.ledger["producer_catalog"].items()), start=1
        ):
            if "product_binary" not in contract["required_subjects"]:
                continue
            artifact_seed = hashlib.sha256(producer.encode()).hexdigest()
            artifacts.append(
                {
                    "producer": producer,
                    "job": f"{producer}-qualification",
                    "artifact_id": str(10_000 + index),
                    "artifact_digest": f"sha256:{artifact_seed}",
                    "binary_path": "nokv",
                    "binary_sha256": f"sha256:{binary_sha256}",
                }
            )
        value = {
            "schema": "nokv.pre423.product_artifact_manifest.v1",
            "provider": "github-actions",
            "workflow_run_id": "run-1",
            "workflow_attempt": 1,
            "head_sha": self.source_sha,
            "artifacts": artifacts,
        }
        if mutate is not None:
            mutate(value)
        self.product_artifact_manifest.parent.mkdir(parents=True, exist_ok=True)
        self.product_artifact_manifest.write_text(json.dumps(value), encoding="utf-8")
        return self.product_artifact_manifest

    def _aggregate(
        self, *, product_artifact_manifest: Path | None = None
    ) -> aggregate_module.AggregationResult:
        manifest = (
            self._write_product_artifact_manifest()
            if product_artifact_manifest is None
            else product_artifact_manifest
        )
        return aggregate_module.aggregate_receipts(
            ledger=self.ledger,
            receipt_dir=self.receipt_dir,
            source_sha=self.source_sha,
            repo=self.repo,
            workflow_run_id="run-1",
            product_artifact_manifest=manifest,
        )

    def _set_receipt_outcome(self, path: Path, outcome: str) -> None:
        receipt = json.loads(path.read_text(encoding="utf-8"))
        exit_code = {"PASS": 0, "NQ": 3, "FAIL": 7}[outcome]
        receipt["outcome"] = outcome
        receipt["execution"]["exit_code"] = exit_code
        result_entry = next(
            entry for entry in receipt["evidence"] if entry["role"] == "producer-result"
        )
        result_path = self.bundle / result_entry["path"]
        producer_result = json.loads(result_path.read_text(encoding="utf-8"))
        for scenario in producer_result["scenarios"].values():
            scenario["outcome"] = outcome
        payload = json.dumps(producer_result, sort_keys=True).encode()
        result_path.write_bytes(payload)
        result_entry["sha256"] = hashlib.sha256(payload).hexdigest()
        result_entry["size_bytes"] = len(payload)
        receipt["execution"]["producer_result_sha256"] = ledger_module.json_sha256(
            producer_result
        )
        path.write_text(json.dumps(receipt), encoding="utf-8")

    def test_no_receipts_is_not_qualified(self) -> None:
        result = self._aggregate()
        self.assertEqual(result.status, "NQ")
        self.assertEqual(result.exit_code, 3)
        self.assertEqual(len(result.report["items"]), 47)

    def test_rust_toolchain_subject_is_revalidated_against_the_host(self) -> None:
        paths = self._write_complete_receipts()
        path = next(
            candidate
            for candidate in paths
            if "rust_toolchain"
            in json.loads(candidate.read_text(encoding="utf-8"))["subjects"]
        )
        receipt = json.loads(path.read_text(encoding="utf-8"))
        receipt["subjects"]["rust_toolchain"]["cargo"]["resolved_sha256"] = "0" * 64
        path.write_text(json.dumps(receipt), encoding="utf-8")

        result = self._aggregate()

        self.assertEqual(result.status, "FAIL")
        self.assertTrue(
            any(
                "rust_toolchain identity does not match" in entry["reason"]
                for entry in result.report["invalid_receipts"]
            )
        )

    def test_every_declared_scenario_can_aggregate_to_pass(self) -> None:
        self._write_complete_receipts()
        result = self._aggregate()
        self.assertEqual(result.status, "PASS", result.report)
        self.assertEqual(result.exit_code, 0)

    def test_complete_live_receipts_without_external_artifact_manifest_are_nq(
        self,
    ) -> None:
        self._write_complete_receipts()

        result = aggregate_module.aggregate_receipts(
            ledger=self.ledger,
            receipt_dir=self.receipt_dir,
            source_sha=self.source_sha,
            repo=self.repo,
            workflow_run_id="run-1",
        )

        self.assertEqual(result.status, "NQ", result.report)
        self.assertTrue(
            any(
                "external product artifact manifest is required" in entry["reason"]
                for entry in result.report["rejected_receipts"]
            ),
            result.report["rejected_receipts"],
        )

    def test_missing_manifest_does_not_hide_a_malformed_live_receipt(self) -> None:
        paths = self._write_complete_receipts()
        path = next(
            candidate
            for candidate in paths
            if "product_binary"
            in json.loads(candidate.read_text(encoding="utf-8"))["subjects"]
        )
        receipt = json.loads(path.read_text(encoding="utf-8"))
        receipt["execution"]["command_argv_sha256"] = "0" * 64
        path.write_text(json.dumps(receipt), encoding="utf-8")

        result = aggregate_module.aggregate_receipts(
            ledger=self.ledger,
            receipt_dir=self.receipt_dir,
            source_sha=self.source_sha,
            repo=self.repo,
            workflow_run_id="run-1",
        )

        self.assertEqual(result.status, "FAIL", result.report)
        self.assertTrue(result.report["invalid_receipts"])

    def test_live_receipt_requires_exact_external_artifact_mapping(self) -> None:
        self._write_complete_receipts()

        def change_binary_digest(value: dict[str, object]) -> None:
            artifacts = value["artifacts"]
            assert isinstance(artifacts, list)
            artifact = artifacts[0]
            assert isinstance(artifact, dict)
            artifact["binary_sha256"] = f"sha256:{'0' * 64}"

        manifest = self._write_product_artifact_manifest(mutate=change_binary_digest)
        result = self._aggregate(product_artifact_manifest=manifest)

        self.assertEqual(result.status, "FAIL", result.report)
        self.assertTrue(
            any(
                "product binary digest disagrees" in entry["reason"]
                for entry in result.report["invalid_receipts"]
            ),
            result.report["invalid_receipts"],
        )

    def test_external_artifact_manifest_rejects_duplicate_mapping(self) -> None:
        self._write_complete_receipts()

        def duplicate_mapping(value: dict[str, object]) -> None:
            artifacts = value["artifacts"]
            assert isinstance(artifacts, list)
            artifacts.append(copy.deepcopy(artifacts[0]))

        manifest = self._write_product_artifact_manifest(mutate=duplicate_mapping)
        with self.assertRaisesRegex(
            aggregate_module.AggregateError, "duplicate producer/job mapping"
        ):
            self._aggregate(product_artifact_manifest=manifest)

    def test_external_artifact_manifest_binds_run_head_id_and_digest(self) -> None:
        self._write_complete_receipts()

        def set_field(
            value: dict[str, object], field: str, replacement: object
        ) -> None:
            if field.startswith("artifact."):
                artifacts = value["artifacts"]
                assert isinstance(artifacts, list)
                artifact = artifacts[0]
                assert isinstance(artifact, dict)
                artifact[field.removeprefix("artifact.")] = replacement
            else:
                value[field] = replacement

        cases = (
            ("workflow_run_id", "other-run", "run does not match"),
            ("head_sha", "1" * 40, "head does not match"),
            ("artifact.artifact_id", "not-an-id", "positive decimal string"),
            ("artifact.artifact_digest", "sha256:short", "sha256:<64"),
            ("artifact.producer", "unknown-producer", "catalogued live"),
        )
        for field, replacement, expected_error in cases:
            with self.subTest(field=field):
                manifest = self._write_product_artifact_manifest(
                    mutate=lambda value,
                    field=field,
                    replacement=replacement: set_field(value, field, replacement)
                )
                with self.assertRaisesRegex(
                    aggregate_module.AggregateError, expected_error
                ):
                    self._aggregate(product_artifact_manifest=manifest)

    def test_external_artifact_manifest_requires_exact_live_job_mapping(self) -> None:
        self._write_complete_receipts()

        def change_job(value: dict[str, object]) -> None:
            artifacts = value["artifacts"]
            assert isinstance(artifacts, list)
            artifact = artifacts[0]
            assert isinstance(artifact, dict)
            artifact["job"] = "different-live-job"

        manifest = self._write_product_artifact_manifest(mutate=change_job)
        result = self._aggregate(product_artifact_manifest=manifest)

        self.assertEqual(result.status, "NQ", result.report)
        self.assertTrue(
            any(
                "no mapping for live producer" in entry["reason"]
                for entry in result.report["rejected_receipts"]
            ),
            result.report["rejected_receipts"],
        )

    def test_product_artifact_manifest_cannot_come_from_receipt_bundle(self) -> None:
        self._write_complete_receipts()
        source = self._write_product_artifact_manifest()
        bundled_manifest = self.bundle / "product-artifacts.json"
        bundled_manifest.write_bytes(source.read_bytes())

        with self.assertRaisesRegex(
            aggregate_module.AggregateError, "must not be inside the receipt bundle"
        ):
            self._aggregate(product_artifact_manifest=bundled_manifest)

    def test_missing_receipt_is_not_qualified(self) -> None:
        paths = self._write_complete_receipts()
        paths[0].unlink()
        result = self._aggregate()
        self.assertEqual(result.status, "NQ")
        self.assertEqual(result.exit_code, 3)

    def test_partial_latest_attempt_cannot_reuse_old_scenario_passes(self) -> None:
        paths = self._write_complete_receipts()
        old_path = paths[0]
        old_receipt = json.loads(old_path.read_text(encoding="utf-8"))

        new_receipt = copy.deepcopy(old_receipt)
        new_receipt["execution"]["attempt"] = 2
        (self.receipt_dir / "new-attempt.json").write_text(
            json.dumps(new_receipt), encoding="utf-8"
        )
        result = self._aggregate()
        self.assertEqual(result.status, "NQ", result.report)
        self.assertEqual(result.report["receipt_counts"]["selected"], 1)
        self.assertGreater(result.report["receipt_counts"]["superseded"], 0)

    def test_rejected_latest_attempt_cannot_fall_back_to_old_clean_pass(self) -> None:
        paths = self._write_complete_receipts()
        latest = json.loads(paths[0].read_text(encoding="utf-8"))
        latest["execution"]["attempt"] = 2
        latest["source"]["dirty"] = True
        (self.receipt_dir / "dirty-latest-attempt.json").write_text(
            json.dumps(latest), encoding="utf-8"
        )
        result = self._aggregate()
        self.assertNotEqual(result.status, "PASS", result.report)
        self.assertTrue(result.report["rejected_receipts"])

    def test_cross_job_same_producer_equivocation_is_framework_fail(self) -> None:
        paths = self._write_complete_receipts()
        duplicate = json.loads(paths[0].read_text(encoding="utf-8"))
        duplicate["execution"]["job"] = "second-job-same-producer"
        duplicate["execution"]["started_at"] = "2026-08-16T00:00:00.500Z"
        (self.receipt_dir / "equivocation.json").write_text(
            json.dumps(duplicate), encoding="utf-8"
        )
        result = self._aggregate()
        self.assertEqual(result.status, "FAIL")
        self.assertTrue(result.report["receipt_conflicts"])

    def test_same_job_same_producer_identical_duplicate_is_framework_fail(self) -> None:
        paths = self._write_complete_receipts()
        duplicate = json.loads(paths[0].read_text(encoding="utf-8"))
        (self.receipt_dir / "identical-duplicate.json").write_text(
            json.dumps(duplicate), encoding="utf-8"
        )

        result = self._aggregate()

        self.assertEqual(result.status, "FAIL")
        self.assertTrue(result.report["receipt_conflicts"])

    def test_runner_receipt_is_accepted_without_shape_translation(self) -> None:
        repo = Path(self.temporary.name) / "runner-source-repo"
        repo.mkdir()
        subprocess.run(["git", "init"], cwd=repo, check=True, capture_output=True)
        subprocess.run(
            ["git", "config", "user.name", "Qualification Test"],
            cwd=repo,
            check=True,
        )
        subprocess.run(
            ["git", "config", "user.email", "qualification@example.invalid"],
            cwd=repo,
            check=True,
        )
        (repo / "source.txt").write_text("source\n", encoding="utf-8")
        producer_script = repo / "scripts" / "workbench" / "nokv_agent_qualification.py"
        producer_script.parent.mkdir(parents=True)
        producer_script.write_text(PRODUCER_FIXTURE, encoding="utf-8")
        subprocess.run(["git", "add", "."], cwd=repo, check=True)
        subprocess.run(
            ["git", "commit", "-m", "source"],
            cwd=repo,
            check=True,
            capture_output=True,
        )
        sha = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        evidence_root = Path(self.temporary.name) / "runner-evidence"
        producer_result = evidence_root / "producer-result.json"
        receipt_module.execute_qualification(
            ledger=self.ledger,
            repo=repo,
            output_dir=self.bundle,
            evidence_root=evidence_root,
            producer="nokv-agent-unit",
            evidence_kind="unit",
            claim_values=["T01:schema-surface:t01.create-schema"],
            evidence_values=[f"producer-result={producer_result}"],
            argv=[
                sys.executable,
                str(producer_script),
                "--qualification-result",
                str(producer_result),
            ],
        )
        result = aggregate_module.aggregate_receipts(
            ledger=self.ledger,
            receipt_dir=self.receipt_dir,
            source_sha=sha,
            repo=repo,
        )
        self.assertEqual(result.status, "NQ")
        self.assertEqual(result.report["receipt_counts"]["accepted"], 1)
        self.assertFalse(result.report["invalid_receipts"])
        t01 = next(
            item for item in result.report["items"] if item["stable_id"] == "T01"
        )
        schema_gate = next(
            gate for gate in t01["gates"] if gate["gate"] == "schema-surface"
        )
        self.assertEqual(schema_gate["status"], "PASS")

    def test_latest_nq_and_fail_outcomes_fail_closed(self) -> None:
        for outcome, expected_status, expected_exit in (
            ("NQ", "NQ", 3),
            ("FAIL", "FAIL", 2),
        ):
            with self.subTest(outcome=outcome):
                for path in self.receipt_dir.glob("*.json"):
                    path.unlink()
                paths = self._write_complete_receipts()
                self._set_receipt_outcome(paths[0], outcome)
                result = self._aggregate()
                self.assertEqual(result.status, expected_status)
                self.assertEqual(result.exit_code, expected_exit)

    def test_wrong_source_or_dirty_source_cannot_qualify_current_sha(self) -> None:
        for mutation in ("sha", "dirty"):
            with self.subTest(mutation=mutation):
                for path in self.receipt_dir.glob("*.json"):
                    path.unlink()
                paths = self._write_complete_receipts()
                receipt = json.loads(paths[0].read_text(encoding="utf-8"))
                if mutation == "sha":
                    receipt["source"]["sha"] = "2" * 40
                else:
                    receipt["source"]["dirty"] = True
                paths[0].write_text(json.dumps(receipt), encoding="utf-8")
                result = self._aggregate()
                self.assertEqual(result.status, "NQ")
                self.assertTrue(result.report["rejected_receipts"])

    def test_tampered_evidence_is_framework_fail(self) -> None:
        paths = self._write_complete_receipts()
        receipt = json.loads(paths[0].read_text(encoding="utf-8"))
        evidence_path = self.bundle / receipt["evidence"][0]["path"]
        evidence_path.write_text("tampered\n", encoding="utf-8")
        result = self._aggregate()
        self.assertEqual(result.status, "FAIL")
        self.assertEqual(result.exit_code, 2)
        self.assertTrue(result.report["invalid_receipts"])

    def test_symlinked_bundle_evidence_is_framework_fail(self) -> None:
        paths = self._write_complete_receipts()
        receipt = json.loads(paths[0].read_text(encoding="utf-8"))
        producer_result = next(
            entry for entry in receipt["evidence"] if entry["role"] == "producer-result"
        )
        evidence_path = self.bundle / producer_result["path"]
        payload = evidence_path.read_bytes()
        external = Path(self.temporary.name) / "external-producer-result.json"
        external.write_bytes(payload)
        evidence_path.unlink()
        evidence_path.symlink_to(external)
        result = self._aggregate()
        self.assertEqual(result.status, "FAIL")
        self.assertTrue(result.report["invalid_receipts"])

    def test_tracked_symlink_entrypoint_is_independently_rejected(self) -> None:
        outside = Path(self.temporary.name) / "outside-producer.py"
        outside.write_text(PRODUCER_FIXTURE, encoding="utf-8")
        entrypoint = self.repo / "scripts/workbench/nokv_agent_qualification.py"
        entrypoint.unlink()
        entrypoint.symlink_to(outside)
        subprocess.run(
            ["git", "add", "scripts/workbench/nokv_agent_qualification.py"],
            cwd=self.repo,
            check=True,
        )
        subprocess.run(
            ["git", "commit", "-m", "replace producer with symlink"],
            cwd=self.repo,
            check=True,
            capture_output=True,
        )
        self.source_sha = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=self.repo,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        self._write_complete_receipts()

        result = self._aggregate()

        self.assertEqual(result.status, "FAIL")
        self.assertTrue(
            any(
                "regular tracked file" in invalid["reason"]
                for invalid in result.report["invalid_receipts"]
            ),
            result.report["invalid_receipts"],
        )

    def test_empty_or_nul_bundle_evidence_path_is_framework_fail(self) -> None:
        for malformed in (".", "runs/\x00evidence"):
            with self.subTest(path=repr(malformed)):
                for path in self.receipt_dir.glob("*.json"):
                    path.unlink()
                paths = self._write_complete_receipts()
                receipt = json.loads(paths[0].read_text(encoding="utf-8"))
                receipt["evidence"][0]["path"] = malformed
                paths[0].write_text(json.dumps(receipt), encoding="utf-8")

                result = self._aggregate()

                self.assertEqual(result.status, "FAIL")
                self.assertEqual(result.exit_code, 2)
                self.assertTrue(result.report["invalid_receipts"])

    def test_true_command_manifest_cannot_impersonate_a_producer(self) -> None:
        paths = self._write_complete_receipts()
        receipt = json.loads(paths[0].read_text(encoding="utf-8"))
        receipt["execution"]["argv"] = ["/usr/bin/true"]
        receipt["execution"]["command_argv_sha256"] = ledger_module.json_sha256(
            receipt["execution"]["argv"]
        )
        paths[0].write_text(json.dumps(receipt), encoding="utf-8")
        result = self._aggregate()
        self.assertEqual(result.status, "FAIL")
        self.assertTrue(result.report["invalid_receipts"])

    def test_structured_nq_cannot_be_promoted_by_zero_exit_receipt(self) -> None:
        paths = self._write_complete_receipts()
        receipt = json.loads(paths[0].read_text(encoding="utf-8"))
        result_entry = next(
            entry for entry in receipt["evidence"] if entry["role"] == "producer-result"
        )
        result_path = self.bundle / result_entry["path"]
        producer_result = json.loads(result_path.read_text(encoding="utf-8"))
        for scenario in producer_result["scenarios"].values():
            scenario["outcome"] = "NQ"
        payload = json.dumps(producer_result, sort_keys=True).encode()
        result_path.write_bytes(payload)
        result_entry["sha256"] = hashlib.sha256(payload).hexdigest()
        result_entry["size_bytes"] = len(payload)
        receipt["execution"]["producer_result_sha256"] = ledger_module.json_sha256(
            producer_result
        )
        paths[0].write_text(json.dumps(receipt), encoding="utf-8")
        result = self._aggregate()
        self.assertEqual(result.status, "FAIL")
        self.assertTrue(result.report["invalid_receipts"])

    def test_disallowed_evidence_kind_is_framework_fail(self) -> None:
        paths = self._write_complete_receipts()
        target = next(path for path in paths if path.name == "T18-lingtai-mcp-e2e.json")
        receipt = json.loads(target.read_text(encoding="utf-8"))
        receipt["evidence_kind"] = "static"
        target.write_text(json.dumps(receipt), encoding="utf-8")
        result = self._aggregate()
        self.assertEqual(result.status, "FAIL")
        self.assertTrue(result.report["invalid_receipts"])

    def test_expected_workflow_attempt_rejects_older_receipts(self) -> None:
        self._write_complete_receipts()
        result = aggregate_module.aggregate_receipts(
            ledger=self.ledger,
            receipt_dir=self.receipt_dir,
            source_sha=self.source_sha,
            repo=self.repo,
            workflow_run_id="run-1",
            workflow_attempt=2,
        )
        self.assertEqual(result.status, "NQ")
        self.assertEqual(result.report["receipt_counts"]["accepted"], 0)
        self.assertTrue(result.report["rejected_receipts"])

    def test_cli_cannot_override_github_run_identity(self) -> None:
        error = io.StringIO()
        with (
            mock.patch.dict(
                os.environ,
                {"GITHUB_RUN_ID": "trusted-run", "GITHUB_RUN_ATTEMPT": "2"},
            ),
            redirect_stderr(error),
        ):
            return_code = aggregate_module.main(
                [
                    "--receipt-dir",
                    str(self.receipt_dir),
                    "--output",
                    str(self.bundle / "aggregate.json"),
                    "--repo",
                    str(Path(self.temporary.name)),
                    "--workflow-run-id",
                    "attacker-run",
                    "--workflow-attempt",
                    "2",
                ]
            )
        self.assertEqual(return_code, 2)
        self.assertIn("cannot override GITHUB_RUN_ID", error.getvalue())

    def test_ledger_hash_change_invalidates_old_receipt(self) -> None:
        self._write_complete_receipts()
        changed = copy.deepcopy(self.ledger)
        changed["items"][0]["requirement"] += " Changed."
        with self.assertRaisesRegex(
            ledger_module.LedgerError, "qualification policy digest"
        ):
            aggregate_module.aggregate_receipts(
                ledger=changed,
                receipt_dir=self.receipt_dir,
                source_sha=self.source_sha,
                repo=self.repo,
                workflow_run_id="run-1",
            )


if __name__ == "__main__":
    unittest.main()
