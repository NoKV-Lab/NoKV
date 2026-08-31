#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Tests for command-bound pre-#423 qualification receipts."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
RUNNER = SCRIPT_DIR / "qualification_receipt.py"
AGGREGATOR = SCRIPT_DIR / "qualification_aggregate.py"
PRODUCER_FIXTURE = r"""#!/usr/bin/env python3
import argparse
import json
import os
import pathlib
import sys

parser = argparse.ArgumentParser()
parser.add_argument("--qualification-result", required=True)
parser.add_argument(
    "--mode",
    choices=(
        "pass",
        "nq",
        "fail",
        "dry-nq",
        "mutate",
        "symlink",
        "wrong-subjects",
    ),
    default="pass",
)
parser.add_argument("--artifact")
parser.add_argument("--external-source")
parser.add_argument("--nokv-bin")
args = parser.parse_args()

outcome = {"nq": "NQ", "dry-nq": "NQ", "fail": "FAIL"}.get(
    args.mode, "PASS"
)
if args.artifact:
    artifact = pathlib.Path(args.artifact)
    artifact.parent.mkdir(parents=True, exist_ok=True)
    if args.mode == "symlink":
        artifact.symlink_to(pathlib.Path(args.external_source))
    else:
        artifact.write_bytes(b"actual runtime evidence\x00")
if args.mode == "mutate":
    pathlib.Path("source.txt").write_text("mutated\n", encoding="utf-8")

claims = json.loads(os.environ["NOKV_QUALIFICATION_CLAIMS"])
roles = json.loads(os.environ["NOKV_QUALIFICATION_REQUIRED_EVIDENCE_ROLES"])
if args.artifact:
    roles.append("artifact")
result = {
    "schema": "nokv.pre423.producer_result.v1",
    "producer": os.environ["NOKV_QUALIFICATION_PRODUCER"],
    "evidence_kind": os.environ["NOKV_QUALIFICATION_EVIDENCE_KIND"],
    "operation_id": os.environ["NOKV_QUALIFICATION_OPERATION_ID"],
    "source_sha": os.environ["NOKV_QUALIFICATION_SOURCE_SHA"],
    "command_argv_sha256": os.environ[
        "NOKV_QUALIFICATION_COMMAND_ARGV_SHA256"
    ],
    "subjects": (
        {"dependencies": [{"name": "self-reported", "identity": "free-form"}]}
        if args.mode == "wrong-subjects"
        else json.loads(os.environ["NOKV_QUALIFICATION_SUBJECTS"])
    ),
    "subjects_sha256": os.environ["NOKV_QUALIFICATION_SUBJECTS_SHA256"],
    "scenarios": {
        claim["scenario"]: {"outcome": outcome, "evidence_roles": roles}
        for claim in claims
    },
}
result_path = pathlib.Path(args.qualification_result)
result_path.parent.mkdir(parents=True, exist_ok=True)
result_path.write_text(json.dumps(result), encoding="utf-8")
print("qualified-out")
print("qualified-err", file=sys.stderr)
raise SystemExit({"nq": 3, "fail": 7}.get(args.mode, 0))
"""


class QualificationReceiptTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.repo = self.root / "repo"
        self.repo.mkdir()
        self._git("init")
        self._git("config", "user.name", "Qualification Test")
        self._git("config", "user.email", "qualification@example.invalid")
        (self.repo / "source.txt").write_text("source\n", encoding="utf-8")
        self.producer_script = (
            self.repo / "scripts" / "workbench" / "nokv_agent_qualification.py"
        )
        self.producer_script.parent.mkdir(parents=True)
        self.producer_script.write_text(PRODUCER_FIXTURE, encoding="utf-8")
        self.live_producer_script = (
            self.repo / "scripts" / "workbench" / "live_workbench.py"
        )
        self.live_producer_script.write_text(PRODUCER_FIXTURE, encoding="utf-8")
        self._git("add", ".")
        self._git("commit", "-m", "source")
        self.sha = self._git("rev-parse", "HEAD").stdout.strip()
        self.bundle = self.root / "bundle"
        self.evidence_root = self.root / "producer-evidence"
        self.producer_result = self.evidence_root / "producer-result.json"

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _git(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["git", *args],
            cwd=self.repo,
            check=True,
            capture_output=True,
            text=True,
        )

    def _run(
        self,
        *command: str,
        claim: str = "T01:schema-surface:t01.create-schema",
        producer: str = "nokv-agent-unit",
        evidence_kind: str = "unit",
        extra: tuple[str, ...] = (),
        environment: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                sys.executable,
                str(RUNNER),
                "--repo",
                str(self.repo),
                "--output-dir",
                str(self.bundle),
                "--evidence-root",
                str(self.evidence_root),
                "--producer",
                producer,
                "--evidence-kind",
                evidence_kind,
                "--claim",
                claim,
                "--evidence",
                f"producer-result={self.producer_result}",
                *extra,
                "--",
                *command,
            ],
            check=False,
            capture_output=True,
            text=True,
            env=environment,
        )

    def _producer_command(self, mode: str = "pass", *extra: str) -> tuple[str, ...]:
        return (
            sys.executable,
            str(self.producer_script),
            "--qualification-result",
            str(self.producer_result),
            "--mode",
            mode,
            *extra,
        )

    def _only_receipt(self) -> dict[str, object]:
        paths = list((self.bundle / "receipts").glob("*.json"))
        self.assertEqual(len(paths), 1, paths)
        return json.loads(paths[0].read_text(encoding="utf-8"))

    def test_executes_argv_and_binds_source_exit_and_stream_hashes(self) -> None:
        command = self._producer_command()
        completed = self._run(*command)
        self.assertEqual(completed.returncode, 0, completed.stderr)

        receipt = self._only_receipt()
        self.assertEqual(receipt["outcome"], "PASS")
        self.assertEqual(receipt["source"]["sha"], self.sha)
        self.assertFalse(receipt["source"]["dirty"])
        self.assertEqual(receipt["execution"]["argv"], list(command))
        self.assertEqual(receipt["execution"]["exit_code"], 0)
        evidence = {entry["role"]: entry for entry in receipt["evidence"]}
        self.assertEqual(
            evidence["stdout"]["sha256"],
            hashlib.sha256(b"qualified-out\n").hexdigest(),
        )
        self.assertEqual(
            evidence["stderr"]["sha256"],
            hashlib.sha256(b"qualified-err\n").hexdigest(),
        )

    def test_exit_three_records_nq_and_preserves_exit_code(self) -> None:
        completed = self._run(*self._producer_command("nq"))
        self.assertEqual(completed.returncode, 3)
        receipt = self._only_receipt()
        self.assertEqual(receipt["outcome"], "NQ")
        self.assertEqual(receipt["execution"]["exit_code"], 3)

    def test_nonzero_exit_records_fail(self) -> None:
        completed = self._run(*self._producer_command("fail"))
        self.assertEqual(completed.returncode, 7)
        receipt = self._only_receipt()
        self.assertEqual(receipt["outcome"], "FAIL")
        self.assertEqual(receipt["execution"]["exit_code"], 7)

    def test_invalid_claim_is_rejected_before_command_runs(self) -> None:
        completed = self._run(
            *self._producer_command(),
            claim="T01:schema-surface:t01.not-declared",
        )
        self.assertEqual(completed.returncode, 2)
        self.assertFalse((self.bundle / "receipts").exists())

    def test_single_shell_string_is_never_interpreted(self) -> None:
        sentinel = self.root / "shell-string-must-not-run"
        shell_string = (
            f'{sys.executable} -c "from pathlib import Path; '
            f'Path({str(sentinel)!r}).touch()"'
        )
        completed = self._run(shell_string)
        self.assertNotEqual(completed.returncode, 0)
        self.assertFalse(sentinel.exists())
        self.assertFalse((self.bundle / "receipts").exists())

    def test_live_claim_requires_binary_and_dependency_identity(self) -> None:
        sentinel = self.root / "must-not-run"
        completed = self._run(
            sys.executable,
            "-c",
            f"from pathlib import Path; Path({str(sentinel)!r}).touch()",
            claim="T01:native-workbench-e2e:t01.create-live",
            producer="live-workbench",
            evidence_kind="live",
        )
        self.assertEqual(completed.returncode, 2)
        self.assertFalse(sentinel.exists())

    def test_true_cannot_impersonate_a_live_producer(self) -> None:
        completed = self._run(
            "/usr/bin/true",
            claim="T01:native-workbench-e2e:t01.create-live",
            producer="live-workbench",
            evidence_kind="live",
            extra=(
                "--binary",
                "/usr/bin/true",
                "--dependency",
                f"object-store=oci:rustfs/rustfs@sha256:{'2' * 64}",
                "--evidence",
                f"qualification={self.evidence_root / 'qualification.json'}",
                "--evidence",
                f"mcp-transcript={self.evidence_root / 'mcp-transcript.jsonl'}",
            ),
        )
        self.assertEqual(completed.returncode, 2)
        self.assertIn("source-bound Python entrypoint", completed.stderr)
        if (self.bundle / "receipts").exists():
            self.assertNotEqual(self._only_receipt()["outcome"], "PASS")

    def test_product_binary_subject_must_match_producer_argv(self) -> None:
        qualification = self.evidence_root / "qualification.json"
        transcript = self.evidence_root / "mcp-transcript.jsonl"
        completed = self._run(
            sys.executable,
            str(self.live_producer_script),
            "--qualification-result",
            str(self.producer_result),
            "--nokv-bin",
            "/usr/bin/false",
            claim="T01:native-workbench-e2e:t01.create-live",
            producer="live-workbench",
            evidence_kind="live",
            extra=(
                "--binary",
                "/usr/bin/true",
                "--dependency",
                f"object-store=oci:rustfs/rustfs@sha256:{'2' * 64}",
                "--evidence",
                f"qualification={qualification}",
                "--evidence",
                f"mcp-transcript={transcript}",
            ),
        )
        self.assertEqual(completed.returncode, 2)
        self.assertIn("product binary argument", completed.stderr)
        self.assertFalse((self.bundle / "receipts").exists())

    def test_dependency_names_and_identities_are_not_arbitrary(self) -> None:
        completed = self._run(
            sys.executable,
            str(self.live_producer_script),
            "--qualification-result",
            str(self.producer_result),
            "--nokv-bin",
            "/usr/bin/true",
            claim="T01:native-workbench-e2e:t01.create-live",
            producer="live-workbench",
            evidence_kind="live",
            extra=(
                "--binary",
                "/usr/bin/true",
                "--dependency",
                "fake-provider=self-reported",
                "--evidence",
                f"qualification={self.evidence_root / 'qualification.json'}",
                "--evidence",
                f"mcp-transcript={self.evidence_root / 'mcp-transcript.jsonl'}",
            ),
        )
        self.assertEqual(completed.returncode, 2)
        self.assertIn("dependency names", completed.stderr)
        self.assertFalse((self.bundle / "receipts").exists())

    def test_runner_requires_its_own_python_interpreter(self) -> None:
        sentinel = self.root / "python-evil-ran"
        python_evil = self.root / "python-evil"
        python_evil.write_text(f"#!/bin/sh\ntouch {sentinel}\n", encoding="utf-8")
        python_evil.chmod(0o755)
        completed = self._run(
            str(python_evil),
            str(self.producer_script),
            "--qualification-result",
            str(self.producer_result),
        )
        self.assertEqual(completed.returncode, 2)
        self.assertIn("runner Python executable", completed.stderr)
        self.assertFalse(sentinel.exists())

    def test_tracked_symlink_entrypoint_cannot_escape_the_checkout(self) -> None:
        sentinel = self.root / "outside-entrypoint-ran"
        outside = self.root / "outside.py"
        outside.write_text(
            f"from pathlib import Path\nPath({str(sentinel)!r}).touch()\n",
            encoding="utf-8",
        )
        self.producer_script.unlink()
        self.producer_script.symlink_to(outside)
        self._git("add", "scripts/workbench/nokv_agent_qualification.py")
        self._git("commit", "-m", "replace producer with symlink")

        completed = self._run(*self._producer_command())

        self.assertEqual(completed.returncode, 2)
        self.assertIn("regular tracked file", completed.stderr)
        self.assertFalse(sentinel.exists())
        self.assertFalse((self.bundle / "receipts").exists())

    def test_cli_cannot_override_github_job_identity(self) -> None:
        environment = os.environ.copy()
        environment["GITHUB_JOB"] = "trusted-producer-job"
        completed = subprocess.run(
            [
                sys.executable,
                str(RUNNER),
                "--repo",
                str(self.repo),
                "--output-dir",
                str(self.bundle),
                "--evidence-root",
                str(self.evidence_root),
                "--producer",
                "nokv-agent-unit",
                "--evidence-kind",
                "unit",
                "--claim",
                "T01:schema-surface:t01.create-schema",
                "--job",
                "attacker-job",
                "--",
                *self._producer_command(),
            ],
            check=False,
            capture_output=True,
            text=True,
            env=environment,
        )
        self.assertEqual(completed.returncode, 2)
        self.assertIn("cannot override GITHUB_JOB", completed.stderr)

    def test_dirty_source_is_recorded_not_hidden(self) -> None:
        (self.repo / "source.txt").write_text("dirty\n", encoding="utf-8")
        completed = self._run(*self._producer_command())
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertTrue(self._only_receipt()["source"]["dirty"])

    def test_command_that_mutates_checkout_cannot_pass(self) -> None:
        completed = self._run(
            *self._producer_command("mutate"),
        )
        self.assertEqual(completed.returncode, 2)
        receipt = self._only_receipt()
        self.assertEqual(receipt["outcome"], "FAIL")
        self.assertIn("source identity changed", receipt["qualification_errors"][0])

    def test_declared_evidence_is_copied_and_hashed_after_execution(self) -> None:
        artifact = self.evidence_root / "artifact.bin"
        payload = b"actual runtime evidence\x00"
        completed = self._run(
            *self._producer_command("pass", "--artifact", str(artifact)),
            extra=("--evidence", f"artifact={artifact}"),
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        receipt = self._only_receipt()
        artifact_entry = next(
            entry for entry in receipt["evidence"] if entry["role"] == "artifact"
        )
        self.assertEqual(artifact_entry["sha256"], hashlib.sha256(payload).hexdigest())
        copied = self.bundle / artifact_entry["path"]
        self.assertEqual(copied.read_bytes(), payload)

    def test_preexisting_external_file_cannot_be_adopted_as_evidence(self) -> None:
        arbitrary = self.root / "preexisting-secret.txt"
        arbitrary.write_text("not produced by qualification\n", encoding="utf-8")
        completed = self._run(
            *self._producer_command(),
            extra=("--evidence", f"artifact={arbitrary}"),
        )
        self.assertEqual(completed.returncode, 2)
        self.assertFalse((self.bundle / "receipts").exists())

    def test_command_created_symlink_cannot_escape_evidence_root(self) -> None:
        arbitrary = self.root / "preexisting-secret.txt"
        arbitrary.write_text("not produced by qualification\n", encoding="utf-8")
        artifact = self.evidence_root / "artifact.bin"
        completed = self._run(
            *self._producer_command(
                "symlink",
                "--artifact",
                str(artifact),
                "--external-source",
                str(arbitrary),
            ),
            extra=("--evidence", f"artifact={artifact}"),
        )
        self.assertEqual(completed.returncode, 2)
        receipt = self._only_receipt()
        self.assertEqual(receipt["outcome"], "FAIL")
        self.assertTrue(
            any("symlink" in error for error in receipt["qualification_errors"])
        )
        copied_roles = {entry["role"] for entry in receipt["evidence"]}
        self.assertNotIn("artifact", copied_roles)

    def test_structured_nq_with_zero_exit_cannot_be_promoted_to_pass(self) -> None:
        completed = self._run(*self._producer_command("dry-nq"))
        self.assertEqual(completed.returncode, 2)
        receipt = self._only_receipt()
        self.assertEqual(receipt["outcome"], "FAIL")
        self.assertTrue(
            any(
                "disagrees with command outcome" in error
                for error in receipt["qualification_errors"]
            )
        )

    def test_producer_must_derive_subjects_not_only_echo_their_hash(self) -> None:
        completed = self._run(*self._producer_command("wrong-subjects"))
        self.assertEqual(completed.returncode, 2)
        receipt = self._only_receipt()
        self.assertEqual(receipt["outcome"], "FAIL")
        self.assertTrue(
            any(
                "subjects do not match" in error
                for error in receipt["qualification_errors"]
            )
        )

    def test_runner_bound_toolchain_defeats_fake_cargo_end_to_end(self) -> None:
        (self.repo / "scripts" / "workbench" / "source_bound_producer.py").write_text(
            (SCRIPT_DIR / "source_bound_producer.py").read_text(encoding="utf-8"),
            encoding="utf-8",
        )
        self.producer_script.write_text(
            """#!/usr/bin/env python3
from source_bound_producer import RustScenario, RustTestAssertion, ScenarioContract, rust_main

SCENARIOS = {
    "t01.create-schema": RustScenario(
        ScenarioContract("T01", "schema-surface"),
        (RustTestAssertion("fixture", "qualification-fixture", ("--lib",), "tests::qualification_fixture"),),
    )
}

if __name__ == "__main__":
    raise SystemExit(rust_main(
        producer_id="nokv-agent-unit",
        evidence_kinds=("unit",),
        scenarios=SCENARIOS,
        description="qualification fixture",
    ))
""",
            encoding="utf-8",
        )
        (self.repo / "Cargo.toml").write_text(
            """[package]
name = "qualification-fixture"
version = "0.1.0"
edition = "2021"
""",
            encoding="utf-8",
        )
        (self.repo / "Cargo.lock").write_text(
            """# This file is automatically @generated by Cargo.
version = 4

[[package]]
name = "qualification-fixture"
version = "0.1.0"
""",
            encoding="utf-8",
        )
        (self.repo / ".gitignore").write_text("__pycache__/\n", encoding="utf-8")
        source_dir = self.repo / "src"
        source_dir.mkdir()
        (source_dir / "lib.rs").write_text(
            """#[cfg(test)]
mod tests {
    #[test]
    fn qualification_fixture() {}
}
""",
            encoding="utf-8",
        )
        self._git("add", ".")
        self._git("commit", "-m", "add source-bound Rust producer")

        sentinel = self.root / "fake-cargo-ran"
        fake_cargo = self.root / "fake-bin" / "cargo"
        fake_cargo.parent.mkdir()
        fake_cargo.write_text(
            f"#!/bin/sh\ntouch {sentinel}\nexit 0\n",
            encoding="utf-8",
        )
        fake_cargo.chmod(0o755)
        environment = os.environ.copy()
        environment["CARGO"] = str(fake_cargo)
        environment["PATH"] = f"{fake_cargo.parent}:{environment['PATH']}"
        fake_home = self.root / "attacker-home"
        fake_home.mkdir()
        environment["HOME"] = str(fake_home)

        completed = self._run(
            sys.executable,
            str(self.producer_script),
            "--qualification-result",
            str(self.producer_result),
            "--target-dir",
            str(self.root / "cargo-target"),
            environment=environment,
        )

        receipt = self._only_receipt()
        self.assertEqual(
            completed.returncode,
            0,
            json.dumps(receipt.get("qualification_errors", []), indent=2),
        )
        self.assertFalse(sentinel.exists())
        toolchain = receipt["subjects"]["rust_toolchain"]
        self.assertNotEqual(toolchain["cargo"]["launcher_path"], str(fake_cargo))
        self.assertEqual(receipt["outcome"], "PASS")

        aggregate_output = self.root / "aggregate.json"
        aggregated = subprocess.run(
            [
                sys.executable,
                str(AGGREGATOR),
                "--repo",
                str(self.repo),
                "--receipt-dir",
                str(self.bundle / "receipts"),
                "--output",
                str(aggregate_output),
            ],
            check=False,
            capture_output=True,
            text=True,
            env=environment,
        )
        self.assertEqual(aggregated.returncode, 3, aggregated.stderr)
        report = json.loads(aggregate_output.read_text(encoding="utf-8"))
        self.assertEqual(report["receipt_counts"]["accepted"], 1)
        self.assertEqual(report["receipt_counts"]["invalid"], 0)


if __name__ == "__main__":
    unittest.main()
