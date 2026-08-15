#!/usr/bin/env python3
"""Contract tests for the real etcd/RustFS object-namespace recovery gate."""

from __future__ import annotations

import copy
import sys
import tempfile
import unittest
from pathlib import Path

from object_namespace_recovery_gate import (
    PINNED_RUSTFS_IMAGE,
    Evidence,
    WorkflowFailure,
    aws_command,
    aws_environment,
    phymat_documents,
    run,
    validate_gate_evidence,
)


REPO = Path(__file__).resolve().parents[2]


def valid_evidence() -> dict[str, object]:
    structure, relaxation = phymat_documents()
    return {
        "status": "PASS",
        "namespace": {
            "bound_id": "11" * 16,
            "marker_id": "11" * 16,
            "mismatch_rejected": True,
            "control_record_unchanged": True,
            "metadata_mutation_blocked": True,
            "wrong_profile_objects": 1,
        },
        "restart": {
            "signal": "SIGKILL",
            "session_absent_before_retry": True,
            "before_owner_epoch": 1,
            "after_owner_epoch": 2,
            "structure": structure,
            "relaxation": relaxation,
        },
        "outage": {
            "error": {
                "status": "error",
                "code": "ObjectUnavailable",
                "retryable": True,
                "details": {"attempts": 3, "source": "nokv-client"},
                "message": "workspace request exhausted 3 attempts: artifact object backend is unavailable",
            },
            "failed_request_digest": "22" * 32,
            "recovered_request_digest": "22" * 32,
            "recovered": relaxation,
        },
        "phymat": {
            "structure": structure,
            "relaxation": relaxation,
            "commit_replay_converged": True,
        },
    }


class ObjectNamespaceRecoveryGateTest(unittest.TestCase):
    def test_rustfs_image_is_digest_pinned(self) -> None:
        self.assertIn("@sha256:", PINNED_RUSTFS_IMAGE)
        self.assertNotIn(":latest", PINNED_RUSTFS_IMAGE)

    def test_rustfs_launcher_has_one_bounded_retry_authority(self) -> None:
        launcher = (REPO / "scripts/workbench/start_rustfs.sh").read_text()
        self.assertIn(PINNED_RUSTFS_IMAGE, launcher)
        self.assertIn("AWS_MAX_ATTEMPTS=1", launcher)
        self.assertIn("--cli-connect-timeout 1", launcher)
        self.assertIn("--cli-read-timeout 2", launcher)

    def test_live_gate_bounds_each_aws_readiness_attempt(self) -> None:
        environment = aws_environment("access", "secret")
        self.assertEqual(environment["AWS_MAX_ATTEMPTS"], "1")
        command = aws_command(Path("/usr/bin/aws"), "http://127.0.0.1:9000", "s3api", "list-buckets")
        self.assertIn("--cli-connect-timeout", command)
        self.assertEqual(command[command.index("--cli-connect-timeout") + 1], "1")
        self.assertIn("--cli-read-timeout", command)
        self.assertEqual(command[command.index("--cli-read-timeout") + 1], "2")

    def test_rust_ci_runs_an_independent_live_gate_and_retains_evidence(self) -> None:
        workflow = (REPO / ".github/workflows/rust.yml").read_text()
        for required in (
            "object-namespace-recovery:",
            "id: object_namespace_recovery",
            "python3 scripts/workbench/object_namespace_recovery_gate.py",
            "python3 scripts/workbench/object_namespace_recovery_gate_test.py",
            "--evidence-dir \"$OBJECT_RECOVERY_EVIDENCE_DIR\"",
            "object-namespace-recovery-${{ github.sha }}",
            "if-no-files-found: error",
        ):
            with self.subTest(required=required):
                self.assertIn(required, workflow)
        self.assertIn(PINNED_RUSTFS_IMAGE, workflow)

    def test_valid_evidence_covers_namespace_restart_outage_and_phymat(self) -> None:
        validate_gate_evidence(valid_evidence())

    def test_retryable_false_is_a_release_failure(self) -> None:
        evidence = valid_evidence()
        evidence["outage"]["error"]["retryable"] = False  # type: ignore[index]
        with self.assertRaisesRegex(WorkflowFailure, "retryable"):
            validate_gate_evidence(evidence)

    def test_provider_identity_must_not_leak_in_public_error(self) -> None:
        evidence = valid_evidence()
        evidence["outage"]["error"]["message"] += (  # type: ignore[index]
            " endpoint=http://127.0.0.1:9000 bucket=secret nokv/artifacts/key"
        )
        with self.assertRaisesRegex(WorkflowFailure, "physical object identity"):
            validate_gate_evidence(evidence)

    def test_timed_out_process_never_persists_secret_arguments(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            evidence = Evidence(Path(temporary) / "evidence")
            evidence.prepare()
            with self.assertRaisesRegex(WorkflowFailure, "timed out"):
                run(
                    [
                        sys.executable,
                        "-c",
                        "import time; time.sleep(1)",
                        "--object-secret-access-key",
                        "must-not-leak",
                    ],
                    cwd=REPO,
                    timeout=0.01,
                    evidence=evidence,
                    label="timeout-probe",
                )
            recorded = (evidence.root / "processes.jsonl").read_text()
            self.assertNotIn("must-not-leak", recorded)
            self.assertIn("<redacted:sha256:", recorded)

    def test_outage_and_recovery_must_use_the_same_logical_request(self) -> None:
        evidence = valid_evidence()
        evidence["outage"]["recovered_request_digest"] = "33" * 32  # type: ignore[index]
        with self.assertRaisesRegex(WorkflowFailure, "logical request"):
            validate_gate_evidence(evidence)

    def test_wrong_profile_must_not_mutate_control_or_store_payloads(self) -> None:
        for field, value in (
            ("control_record_unchanged", False),
            ("metadata_mutation_blocked", False),
            ("wrong_profile_objects", 2),
        ):
            evidence = valid_evidence()
            evidence["namespace"][field] = value  # type: ignore[index]
            with self.assertRaises(WorkflowFailure):
                validate_gate_evidence(evidence)

    def test_restart_must_wait_for_lease_and_advance_once(self) -> None:
        evidence = valid_evidence()
        evidence["restart"]["after_owner_epoch"] = 3  # type: ignore[index]
        with self.assertRaisesRegex(WorkflowFailure, "exactly once"):
            validate_gate_evidence(evidence)

        evidence = valid_evidence()
        evidence["restart"]["session_absent_before_retry"] = False  # type: ignore[index]
        with self.assertRaisesRegex(WorkflowFailure, "session"):
            validate_gate_evidence(evidence)

    def test_phymat_payload_must_survive_restart_and_outage_exactly(self) -> None:
        evidence = valid_evidence()
        evidence["outage"]["recovered"] = copy.deepcopy(  # type: ignore[index]
            evidence["outage"]["recovered"]  # type: ignore[index]
        )
        evidence["outage"]["recovered"]["energy_ev"] = -1.0  # type: ignore[index]
        with self.assertRaisesRegex(WorkflowFailure, "PhyMat"):
            validate_gate_evidence(evidence)


if __name__ == "__main__":
    unittest.main()
