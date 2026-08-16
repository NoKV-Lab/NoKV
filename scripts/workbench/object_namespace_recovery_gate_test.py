#!/usr/bin/env python3
"""Contract tests for the real etcd/RustFS object-namespace recovery gate."""

from __future__ import annotations

import copy
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
from pathlib import Path

from object_namespace_recovery_gate import (
    PINNED_RUSTFS_IMAGE,
    Evidence,
    WorkflowFailure,
    aws_command,
    aws_environment,
    control_args,
    execute,
    fixed_id,
    parse_args,
    phymat_documents,
    run,
    rustfs_container_command,
    server_command,
    validate_gate_evidence,
    wait_rustfs,
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
            "agent_identity_redacted": True,
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
    def test_wrong_root_has_its_own_canonical_agent_fixture(self) -> None:
        seed = "agent-admission-contract"
        root_agent = fixed_id(seed, "agent")
        wrong_root_agent = fixed_id(seed, "wrong-agent")
        self.assertRegex(root_agent, r"^[0-9a-f]{32}$")
        self.assertRegex(wrong_root_agent, r"^[0-9a-f]{32}$")
        self.assertNotEqual(root_agent, wrong_root_agent)
        self.assertNotIn(
            root_agent,
            {fixed_id(seed, "root"), fixed_id(seed, "wrong-root")},
        )
        self.assertNotIn(
            wrong_root_agent,
            {fixed_id(seed, "root"), fixed_id(seed, "wrong-root")},
        )

    def test_shard_server_command_has_no_single_agent_identity(self) -> None:
        common = control_args(
            Path("/tmp/nokv"),
            "11" * 16,
            "http://127.0.0.1:2379",
            "/nokv/object-namespace-gate/test",
        )
        command = server_command(
            common,
            ["--object-root", "gate/root"],
            19000,
            "gate-owner",
            "--metadata-create",
            Path("/tmp/metadata"),
        )
        self.assertNotIn("--agent-id", command)
        with self.assertRaisesRegex(WorkflowFailure, "must not carry"):
            server_command(
                [*common, "--agent-id", "22" * 16],
                ["--object-root", "gate/root"],
                19000,
                "gate-owner",
                "--metadata-create",
                Path("/tmp/metadata"),
            )

    def test_primary_root_provision_uses_a_distinct_stable_agent_id(self) -> None:
        seed = "agent-admission-contract"
        with tempfile.TemporaryDirectory() as temporary:
            temporary_path = Path(temporary)
            executable = temporary_path / "executable"
            executable.touch()
            args = parse_args(
                [
                    "--repo",
                    str(REPO),
                    "--evidence-dir",
                    str(temporary_path / "evidence"),
                    "--binary",
                    str(executable),
                    "--etcd-bin",
                    str(executable),
                    "--etcdctl-bin",
                    str(executable),
                    "--docker-bin",
                    str(executable),
                    "--aws-bin",
                    str(executable),
                    "--seed",
                    seed,
                ]
            )
            recorded: list[tuple[list[str], str | None]] = []

            def fake_run(
                argv: list[object], **kwargs: object
            ) -> subprocess.CompletedProcess[str]:
                command = [str(item) for item in argv]
                label = kwargs.get("label")
                recorded.append((command, label if isinstance(label, str) else None))
                if label == "provision":
                    raise WorkflowFailure("stop after provision")
                return subprocess.CompletedProcess(command, 0, "", "")

            process = mock.Mock()
            process.poll.return_value = None
            with (
                mock.patch(
                    "object_namespace_recovery_gate.run", side_effect=fake_run
                ),
                mock.patch(
                    "object_namespace_recovery_gate.start_process",
                    return_value=process,
                ),
                mock.patch(
                    "object_namespace_recovery_gate.free_port",
                    side_effect=range(19000, 19006),
                ),
                mock.patch("object_namespace_recovery_gate.wait_etcd"),
                mock.patch("object_namespace_recovery_gate.wait_rustfs"),
                mock.patch("object_namespace_recovery_gate.stop_process"),
                mock.patch("object_namespace_recovery_gate.capture_rustfs_log"),
                self.assertRaisesRegex(WorkflowFailure, "stop after provision"),
            ):
                execute(args)

        provision = next(command for command, label in recorded if label == "provision")
        expected_agent_id = fixed_id(seed, "agent")
        self.assertRegex(expected_agent_id, r"^[0-9a-f]{32}$")
        self.assertNotEqual(expected_agent_id, fixed_id(seed, "root"))
        self.assertEqual(
            provision[provision.index("--agent-id") + 1], expected_agent_id
        )

    def test_rustfs_image_is_digest_pinned(self) -> None:
        self.assertIn("@sha256:", PINNED_RUSTFS_IMAGE)
        self.assertNotIn(":latest", PINNED_RUSTFS_IMAGE)

    def test_rustfs_launcher_has_one_bounded_retry_authority(self) -> None:
        launcher = (REPO / "scripts/workbench/start_rustfs.sh").read_text()
        self.assertIn(PINNED_RUSTFS_IMAGE, launcher)
        self.assertIn("AWS_MAX_ATTEMPTS=1", launcher)
        self.assertIn("--cli-connect-timeout 1", launcher)
        self.assertIn("--cli-read-timeout 2", launcher)
        self.assertIn("NOKV_WORKBENCH_RUSTFS_VOLUME", launcher)
        self.assertIn("docker volume create", launcher)

    def test_live_gate_uses_a_docker_volume_for_non_root_rustfs(self) -> None:
        command = rustfs_container_command(
            Path("/usr/bin/docker"),
            container="gate",
            volume="gate-data",
            s3_port=19000,
            console_port=19001,
            image=PINNED_RUSTFS_IMAGE,
            access_key="access",
            secret_key="secret",
        )
        self.assertIn("type=volume,source=gate-data,target=/data", command)
        self.assertNotIn("--secret-key", command)
        self.assertNotIn("--access-key", command)

    @mock.patch("object_namespace_recovery_gate.run")
    def test_readiness_fails_fast_when_rustfs_exits(self, run_mock: mock.Mock) -> None:
        run_mock.side_effect = [
            subprocess.CompletedProcess(["aws"], 1, "", "not ready"),
            subprocess.CompletedProcess(["docker"], 0, "exited 13\n", ""),
        ]
        with self.assertRaisesRegex(
            WorkflowFailure, "container exited before readiness.*exit code 13"
        ):
            wait_rustfs(
                Path("/usr/bin/aws"),
                "http://127.0.0.1:9000",
                {},
                REPO,
                60,
                docker=Path("/usr/bin/docker"),
                container="gate",
            )
        self.assertEqual(run_mock.call_count, 2)

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

    def test_wrong_profile_error_must_not_leak_either_agent_identity(self) -> None:
        evidence = valid_evidence()
        evidence["namespace"]["agent_identity_redacted"] = False  # type: ignore[index]
        with self.assertRaisesRegex(WorkflowFailure, "AgentId"):
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
