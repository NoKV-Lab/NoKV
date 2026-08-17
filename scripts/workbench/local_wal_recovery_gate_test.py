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
from unittest import mock

from local_wal_recovery_gate import (
    CONCURRENT_STAGE,
    CRASH_STAGES,
    GRACEFUL_RELEASE_DEADLINE_SECONDS,
    GRACEFUL_PAYLOAD,
    GRACEFUL_STAGE,
    GRACEFUL_TTL_SECONDS,
    WorkflowFailure,
    control_args,
    fixed_id,
    main,
    object_args,
    run_concurrent_stage,
    server_command,
    validate_concurrent_evidence,
    validate_graceful_evidence,
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


def valid_concurrent_evidence() -> dict[str, object]:
    record = {"owner_epoch": 1, "state": 3, "owner": "gate-incumbent"}
    return {
        "stage": CONCURRENT_STAGE,
        "loser_exit_code": 1,
        "loser_stderr": "nokv: metadata store failed: ... is held by another owner",
        "incumbent_alive_after": True,
        "control_record_before": dict(record),
        "control_record_after": dict(record),
        "metadata_probe": {
            "status": "success",
            "path": "/agents/issue450/wb/exclusion-proof",
        },
    }


def valid_graceful_evidence() -> dict[str, object]:
    payload = "Z3JhY2VmdWwtc2h1dGRvd24tcHJvb2YK"
    return {
        "stage": GRACEFUL_STAGE,
        "signal": "SIGTERM",
        "lease_ttl_seconds": GRACEFUL_TTL_SECONDS,
        "release_deadline_seconds": GRACEFUL_RELEASE_DEADLINE_SECONDS,
        "owner_exit_code": 0,
        "shutdown_elapsed_seconds": 0.25,
        "session_removed_elapsed_seconds": 0.3,
        "session_absent_before_reopen": True,
        "reopen_started_after_session_absent": True,
        "reopen_owner_epoch": 2,
        "payload_expected_base64": payload,
        "payload_observed_base64": payload,
        "metadata_probe": {
            "status": "success",
            "bytes": payload,
            "bytes_encoding": "base64",
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
            '--evidence-dir "$RECOVERY_EVIDENCE_DIR"',
            "--object-endpoint http://127.0.0.1:9000",
            "--object-bucket nokv-local-wal-recovery-gate",
            "bash scripts/workbench/start_rustfs.sh",
            "NOKV_WORKBENCH_RUSTFS_VOLUME: nokv-local-wal-recovery-rustfs-data",
            "docker volume rm -f nokv-local-wal-recovery-rustfs-data",
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

    def test_graceful_shutdown_is_a_distinct_long_ttl_stage(self) -> None:
        self.assertEqual(GRACEFUL_STAGE, "graceful-owner-shutdown")
        self.assertEqual(GRACEFUL_TTL_SECONDS, 10)
        self.assertLess(GRACEFUL_RELEASE_DEADLINE_SECONDS, GRACEFUL_TTL_SECONDS / 2)
        validate_graceful_evidence(valid_graceful_evidence())

    def test_graceful_stage_uses_exact_session_read_not_the_ttl_wait_helper(
        self,
    ) -> None:
        source = (REPO / "scripts/workbench/local_wal_recovery_gate.py").read_text()
        start = source.index("def run_graceful_stage(")
        end = source.index("def run_concurrent_stage(", start)
        graceful = source[start:end]

        self.assertNotIn("wait_session_absent(", graceful)
        self.assertIn("etcd_value(", graceful)
        self.assertIn("owner.wait(timeout=GRACEFUL_RELEASE_DEADLINE_SECONDS)", graceful)
        self.assertTrue(GRACEFUL_PAYLOAD)
        arguments = control_args(
            Path("/tmp/nokv"),
            "11" * 16,
            "http://127.0.0.1:2379",
            "/nokv/local-wal-recovery/graceful",
            GRACEFUL_TTL_SECONDS,
        )
        self.assertEqual(arguments[-1], str(GRACEFUL_TTL_SECONDS))

    def test_async_safe_signal_flags_are_installed_before_owner_acquisition(
        self,
    ) -> None:
        source = (REPO / "crates/nokv/src/main.rs").read_text()

        self.assertIn("signal_hook::flag::register", source)
        self.assertIn('("SIGINT", signal_hook::consts::SIGINT)', source)
        self.assertIn('("SIGTERM", signal_hook::consts::SIGTERM)', source)
        self.assertLess(
            source.index("let shutdown = install_shutdown_signal()?;"),
            source.index("let owner = bootstrap_shard("),
        )
        manifest = (REPO / "Cargo.toml").read_text()
        self.assertIn(
            'signal-hook = { version = "=0.3.18", default-features = false }',
            manifest,
        )

    def test_shutdown_joins_lifecycle_before_exact_release_and_fails_release_errors(
        self,
    ) -> None:
        source = (REPO / "crates/nokv/src/main.rs").read_text()
        run = source.index("let server_result = server.run_until_shutdown")
        join = source.index("join_lifecycle_workers(lifecycle_workers", run)
        release = source.index("let release_result = server.release_ownership()", join)
        release_failure = source.index(
            'failures.push(format!("owner release failed:', release
        )

        self.assertLess(run, join)
        self.assertLess(join, release)
        self.assertLess(release, release_failure)

    def test_graceful_owner_must_exit_zero(self) -> None:
        evidence = valid_graceful_evidence()
        evidence["owner_exit_code"] = -signal.SIGTERM

        with self.assertRaisesRegex(WorkflowFailure, "did not exit cleanly"):
            validate_graceful_evidence(evidence)

    def test_graceful_elapsed_evidence_must_be_finite(self) -> None:
        evidence = valid_graceful_evidence()
        evidence["session_removed_elapsed_seconds"] = float("nan")

        with self.assertRaisesRegex(WorkflowFailure, "release deadline"):
            validate_graceful_evidence(evidence)

    def test_graceful_session_removal_cannot_be_ttl_expiry(self) -> None:
        evidence = valid_graceful_evidence()
        evidence["session_removed_elapsed_seconds"] = GRACEFUL_TTL_SECONDS

        with self.assertRaisesRegex(WorkflowFailure, "release deadline"):
            validate_graceful_evidence(evidence)

    def test_graceful_reopen_must_start_after_exact_session_removal(self) -> None:
        evidence = valid_graceful_evidence()
        evidence["reopen_started_after_session_absent"] = False

        with self.assertRaisesRegex(WorkflowFailure, "before session removal"):
            validate_graceful_evidence(evidence)

    def test_graceful_reopen_must_read_the_exact_old_bytes(self) -> None:
        evidence = valid_graceful_evidence()
        evidence["payload_observed_base64"] = "bmV3LWJ5dGVzCg=="

        with self.assertRaisesRegex(WorkflowFailure, "payload bytes changed"):
            validate_graceful_evidence(evidence)

    def test_every_root_has_a_distinct_canonical_agent_fixture(self) -> None:
        seed = "agent-admission-contract"
        agent_ids: set[str] = set()
        for stage in (*CRASH_STAGES, GRACEFUL_STAGE, CONCURRENT_STAGE):
            agent_id = fixed_id(seed, f"{stage}:agent")
            self.assertRegex(agent_id, r"^[0-9a-f]{32}$")
            self.assertNotEqual(agent_id, fixed_id(seed, f"{stage}:root"))
            agent_ids.add(agent_id)
        self.assertEqual(len(agent_ids), len(CRASH_STAGES) + 2)

    def test_shard_server_control_args_have_no_single_agent_identity(self) -> None:
        arguments = control_args(
            Path("/tmp/nokv"),
            "11" * 16,
            "http://127.0.0.1:2379",
            "/nokv/local-wal-recovery/test",
        )
        self.assertNotIn("--agent-id", arguments)
        command = server_command(
            arguments,
            ["--object-root", "gate/root"],
            19000,
            "gate-owner",
            "--metadata-create",
            Path("/tmp/metadata"),
        )
        self.assertNotIn("--agent-id", command)
        with self.assertRaisesRegex(WorkflowFailure, "must not carry"):
            server_command(
                [*arguments, "--agent-id", "22" * 16],
                ["--object-root", "gate/root"],
                19000,
                "gate-owner",
                "--metadata-create",
                Path("/tmp/metadata"),
            )

    def test_a_live_owner_keeps_a_second_process_out(self) -> None:
        validate_concurrent_evidence(valid_concurrent_evidence())

    def test_the_second_owner_must_actually_fail(self) -> None:
        evidence = valid_concurrent_evidence()
        evidence["loser_exit_code"] = 0

        with self.assertRaisesRegex(WorkflowFailure, "second owner did not fail"):
            validate_concurrent_evidence(evidence)

    def test_a_refused_takeover_must_not_touch_the_control_record(self) -> None:
        evidence = valid_concurrent_evidence()
        evidence["control_record_after"] = {
            "owner_epoch": 2,
            "state": 2,
            "owner": "gate-challenger",
        }

        with self.assertRaisesRegex(WorkflowFailure, "mutated the control record"):
            validate_concurrent_evidence(evidence)

    def test_the_incumbent_must_survive_the_refused_takeover(self) -> None:
        evidence = valid_concurrent_evidence()
        evidence["incumbent_alive_after"] = False

        with self.assertRaisesRegex(WorkflowFailure, "incumbent did not survive"):
            validate_concurrent_evidence(evidence)

    def test_the_incumbent_must_still_serve_after_the_refusal(self) -> None:
        evidence = valid_concurrent_evidence()
        evidence["metadata_probe"] = {"status": "error"}

        with self.assertRaisesRegex(WorkflowFailure, "stopped serving"):
            validate_concurrent_evidence(evidence)

    def test_the_refusal_must_carry_a_diagnostic(self) -> None:
        evidence = valid_concurrent_evidence()
        evidence["loser_stderr"] = "   "

        with self.assertRaisesRegex(WorkflowFailure, "without a diagnostic"):
            validate_concurrent_evidence(evidence)

    def test_gate_uses_a_real_object_profile_for_namespace_admission(self) -> None:
        arguments = object_args(
            "before-local-fence",
            "http://127.0.0.1:9000",
            "nokv-recovery",
            "release-gate",
        )
        self.assertIn("http://127.0.0.1:9000", arguments)
        self.assertIn("nokv-recovery", arguments)
        self.assertIn("release-gate/before-local-fence", arguments)
        self.assertNotIn("http://127.0.0.1:1", arguments)

    def test_concurrent_stage_threads_object_profile_into_provision(self) -> None:
        seed = "object-profile-contract"
        expected_agent_id = fixed_id(seed, f"{CONCURRENT_STAGE}:agent")
        self.assertRegex(expected_agent_id, r"^[0-9a-f]{32}$")
        object_arguments = [
            "--object-bucket",
            "nokv-recovery",
            "--object-endpoint",
            "http://127.0.0.1:9000",
            "--object-root",
            "release-gate/concurrent-takeover",
        ]
        with tempfile.TemporaryDirectory() as directory:
            with (
                mock.patch(
                    "local_wal_recovery_gate.object_args",
                    return_value=object_arguments,
                ) as object_args_mock,
                mock.patch(
                    "local_wal_recovery_gate.run",
                    side_effect=WorkflowFailure("stop after provision"),
                ) as run_mock,
                self.assertRaisesRegex(WorkflowFailure, "stop after provision"),
            ):
                run_concurrent_stage(
                    repo=REPO,
                    binary=Path("/tmp/nokv"),
                    etcdctl=Path("/tmp/etcdctl"),
                    etcd_endpoint="http://127.0.0.1:2379",
                    evidence=Path(directory),
                    seed=seed,
                    object_endpoint="http://127.0.0.1:9000",
                    object_bucket="nokv-recovery",
                    object_root="release-gate",
                    timeout=1,
                )

        object_args_mock.assert_called_once_with(
            CONCURRENT_STAGE,
            "http://127.0.0.1:9000",
            "nokv-recovery",
            "release-gate",
        )
        provision_command = run_mock.call_args.args[0]
        self.assertNotEqual(provision_command[2], expected_agent_id)
        self.assertEqual(
            provision_command,
            [
                "/tmp/nokv",
                "--root-id",
                provision_command[2],
                "--etcd-endpoint",
                "http://127.0.0.1:2379",
                "--etcd-key-prefix",
                provision_command[6],
                "--etcd-lease-ttl-seconds",
                "2",
                "--agent-id",
                expected_agent_id,
                *object_arguments,
                "provision",
                provision_command[-1],
            ],
        )

    def test_each_stage_converges_to_the_same_recovery_epoch(self) -> None:
        for stage in CRASH_STAGES:
            with self.subTest(stage=stage):
                validate_stage_evidence(valid_evidence(stage))

    def test_epoch_three_is_a_release_failure(self) -> None:
        evidence = valid_evidence("before-local-fence")
        evidence["final_owner_epoch"] = 3

        with self.assertRaisesRegex(
            WorkflowFailure, "advanced from recovery epoch 2 to 3"
        ):
            validate_stage_evidence(evidence)

    def test_pre_fence_crash_must_leave_local_epoch_one(self) -> None:
        evidence = valid_evidence("before-local-fence")
        evidence["local_epoch_at_crash"] = 2

        with self.assertRaisesRegex(
            WorkflowFailure, "before-local-fence.*local epoch 1"
        ):
            validate_stage_evidence(evidence)

    def test_post_fence_crash_must_persist_local_epoch_two(self) -> None:
        evidence = valid_evidence("after-local-fence")
        evidence["local_epoch_at_crash"] = 1

        with self.assertRaisesRegex(
            WorkflowFailure, "after-local-fence.*local epoch 2"
        ):
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
