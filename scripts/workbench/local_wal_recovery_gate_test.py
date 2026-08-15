#!/usr/bin/env python3
"""Contract tests for the real-etcd local-WAL recovery gate."""

from __future__ import annotations

import copy
import contextlib
import io
import json
import signal
import tempfile
import unittest
from pathlib import Path

from local_wal_recovery_gate import (
    CRASH_STAGES,
    WorkflowFailure,
    main,
    validate_stage_evidence,
)


REPO = Path(__file__).resolve().parents[2]


def valid_evidence(stage: str) -> dict[str, object]:
    local_epoch = 1 if stage == "before-local-fence" else 2
    return {
        "stage": stage,
        "previous_owner_epoch": 1,
        "recovery_owner_epoch": 2,
        "local_epoch_at_crash": local_epoch,
        "fault_exit_code": -signal.SIGKILL,
        "session_absent_before_retry": True,
        "final_owner_epoch": 2,
        "final_state": "Serving",
        "metadata_probe": {
            "status": "success",
            "path": "/agents/issue450/wb/restart-proof",
        },
    }


class LocalWalRecoveryGateContractTests(unittest.TestCase):
    def test_rust_ci_runs_the_real_gate_and_retains_failure_evidence(self) -> None:
        workflow = (REPO / ".github/workflows/rust.yml").read_text()

        for required in (
            "python3 scripts/workbench/local_wal_recovery_gate_test.py",
            "id: local_wal_recovery",
            "python3 scripts/workbench/local_wal_recovery_gate.py",
            "--build",
            "--evidence-dir \"$RECOVERY_EVIDENCE_DIR\"",
            "sha256sum --check",
            "if: ${{ always() && steps.local_wal_recovery.outcome != 'skipped' }}",
            "if-no-files-found: error",
        ):
            with self.subTest(required=required):
                self.assertIn(required, workflow)
        self.assertRegex(
            workflow,
            r"ETCD_LINUX_AMD64_SHA256: [0-9a-f]{64}",
        )

    def test_gate_has_exactly_the_two_epoch_two_crash_boundaries(self) -> None:
        self.assertEqual(
            CRASH_STAGES,
            ("before-local-fence", "after-local-fence"),
        )

    def test_each_stage_converges_to_the_same_recovery_epoch(self) -> None:
        for stage in CRASH_STAGES:
            with self.subTest(stage=stage):
                validate_stage_evidence(valid_evidence(stage))

    def test_epoch_three_is_a_release_failure(self) -> None:
        evidence = valid_evidence("before-local-fence")
        evidence["final_owner_epoch"] = 3

        with self.assertRaisesRegex(WorkflowFailure, "advanced from recovery epoch 2 to 3"):
            validate_stage_evidence(evidence)

    def test_pre_fence_crash_must_leave_local_epoch_one(self) -> None:
        evidence = valid_evidence("before-local-fence")
        evidence["local_epoch_at_crash"] = 2

        with self.assertRaisesRegex(WorkflowFailure, "before-local-fence.*local epoch 1"):
            validate_stage_evidence(evidence)

    def test_post_fence_crash_must_persist_local_epoch_two(self) -> None:
        evidence = valid_evidence("after-local-fence")
        evidence["local_epoch_at_crash"] = 1

        with self.assertRaisesRegex(WorkflowFailure, "after-local-fence.*local epoch 2"):
            validate_stage_evidence(evidence)

    def test_retry_waits_for_the_killed_session_to_disappear(self) -> None:
        evidence = valid_evidence("after-local-fence")
        evidence["session_absent_before_retry"] = False

        with self.assertRaisesRegex(WorkflowFailure, "owner session remained live"):
            validate_stage_evidence(evidence)

    def test_boundary_process_must_be_killed_not_cleanly_released(self) -> None:
        evidence = valid_evidence("after-local-fence")
        evidence["fault_exit_code"] = 0

        with self.assertRaisesRegex(WorkflowFailure, "was not terminated by SIGKILL"):
            validate_stage_evidence(evidence)

    def test_terminal_metadata_probe_is_required(self) -> None:
        evidence = copy.deepcopy(valid_evidence("after-local-fence"))
        evidence["metadata_probe"]["status"] = "error"  # type: ignore[index]

        with self.assertRaisesRegex(WorkflowFailure, "metadata probe did not succeed"):
            validate_stage_evidence(evidence)

    def test_not_qualified_dependency_failure_retains_terminal_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            evidence = Path(temporary) / "evidence"
            missing = Path(temporary) / "missing-etcd"

            with contextlib.redirect_stderr(io.StringIO()):
                code = main(
                    [
                        "--evidence-dir",
                        str(evidence),
                        "--etcd-bin",
                        str(missing),
                        "--etcdctl-bin",
                        str(missing),
                    ]
                )

            self.assertEqual(code, 3)
            terminal = json.loads((evidence / "qualification.json").read_text())
            self.assertEqual(terminal["status"], "NOT QUALIFIED")
            self.assertIn("real etcd binary", terminal["error"])


if __name__ == "__main__":
    unittest.main()
