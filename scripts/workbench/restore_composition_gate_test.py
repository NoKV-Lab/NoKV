#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Contract tests for the pre-#423 restore-composition acceptance gate."""

from __future__ import annotations

import copy
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stderr
from io import StringIO
from pathlib import Path

from restore_composition_gate import (
    BUILD_TIMEOUT_SECONDS,
    Evidence,
    PINNED_RUSTFS_IMAGE,
    PRE423_ORACLE_REVISION,
    Config,
    WorkflowFailure,
    build_timeout,
    matching_mcp_structured_content,
    mutation_request_id,
    mutation_command,
    oracle_plan,
    qualification,
    redact_argv,
    start_process,
    validate_composition_evidence,
    validate_fault_barrier_evidence,
    validate_environment_evidence,
    validate_mutation_result,
    validate_owner_loss_error,
    TYPED_SCENARIOS,
)
import pre423_contract_ledger


REPO = Path(__file__).resolve().parents[2]


def config(root: Path) -> Config:
    return Config(
        repo=REPO,
        binary=root / "nokv",
        evidence=root / "evidence",
        target=root / "target",
        fault_binary=root / "fault-target" / "debug" / "nokv-restore-crash-owner",
        fault_target=root / "fault-target",
        etcd=Path("/usr/local/bin/etcd"),
        etcdctl=Path("/usr/local/bin/etcdctl"),
        docker=Path("/usr/local/bin/docker"),
        aws=Path("/usr/local/bin/aws"),
        rustfs_image=PINNED_RUSTFS_IMAGE,
        seed="restore-composition-test",
        build=False,
        dry_run=True,
        timeout=60.0,
        keep_resources=False,
    )


def manifest(
    *, workbench: str, identity: str, content_digest: str, source: str | None = None
) -> dict[str, object]:
    value: dict[str, object] = {
        "schema": "nokv.workbench.run_manifest.v1",
        "workbench_id": workbench,
        "workbench_path": f"/agents/composition/wb/{workbench}",
        "content_digest_uri": content_digest,
        "manifest_digest_uri": "sha256:" + "7a" * 32,
        "commit_identity": identity,
        "committed_at_unix_seconds": 1_800_000_000,
        "manifest": {"task": "restore-composition"},
    }
    if source is not None:
        value["restored_source"] = source
    return value


def valid_evidence() -> dict[str, object]:
    a_identity = "11" * 32
    b_identity = "22" * 32
    c_identity = "33" * 32
    clean_digest = "sha256:" + "44" * 32
    dirty_digest = "sha256:" + "55" * 32
    return {
        "schema": "nokv.restore_composition_gate.v1",
        "status": "PASS",
        "oracle_revision": PRE423_ORACLE_REVISION,
        "excluded_surfaces": [
            "FUSE",
            "POSIX",
            "Yanex",
            "inode",
            "dentry",
            "physical layout",
        ],
        "call_labels": [
            "create-a",
            "put-a-rename-source",
            "put-a-delete-source",
            "put-a-payload",
            "commit-a",
            "snapshot-a",
            "mutate-a-after-snapshot",
            "restore-b",
            "find-b-committed",
            "rename-b",
            "remove-b",
            "publish-b",
            "snapshot-b-no-recommit",
            "restore-c",
            "find-c-committed",
            "mutate-b-after-snapshot",
            "retire-snapshot-b",
            "read-c-after-retire",
            "restore-c-terminal-replay",
        ],
        "workbenches": {
            "a": {
                "id": "composition-a",
                "commit_generation": 1,
                "committed": True,
                "run_manifest": manifest(
                    workbench="composition-a",
                    identity=a_identity,
                    content_digest=clean_digest,
                ),
            },
            "b": {
                "id": "composition-b",
                "destination_generation": 1,
                "committed": True,
                "run_manifest": manifest(
                    workbench="composition-b",
                    identity=b_identity,
                    content_digest=clean_digest,
                    source="composition-a",
                ),
                "restore_manifest": {
                    "schema": "nokv.workbench.restore_manifest.v2",
                    "source_workbench_id": "composition-a",
                    "destination_workbench_id": "composition-b",
                    "operation_id": "66" * 16,
                    "snapshot_id": 1,
                },
                "destination_owned_manifest_objects": 2,
                "old_rename_path_absent": True,
                "deleted_path_absent": True,
                "renamed_bytes_sha256": "77" * 32,
                "published_bytes_sha256": "88" * 32,
            },
            "c": {
                "id": "composition-c",
                "destination_generation": 1,
                "committed": True,
                "run_manifest": manifest(
                    workbench="composition-c",
                    identity=c_identity,
                    content_digest=dirty_digest,
                    source="composition-b",
                ),
                "restore_manifest": {
                    "schema": "nokv.workbench.restore_manifest.v2",
                    "source_workbench_id": "composition-b",
                    "destination_workbench_id": "composition-c",
                    "operation_id": "99" * 16,
                    "snapshot_id": 2,
                },
                "destination_owned_manifest_objects": 2,
                "old_rename_path_absent": True,
                "deleted_path_absent": True,
                "renamed_bytes_sha256": "77" * 32,
                "published_bytes_sha256": "88" * 32,
                "readable_after_snapshot_retire": True,
            },
        },
        "snapshots": {
            "a": {"snapshot_id": 1, "source_workbench_id": "composition-a"},
            "b": {
                "snapshot_id": 2,
                "source_workbench_id": "composition-b",
                "minted_without_recommit": True,
                "retired": True,
            },
        },
        "independence": {
            "a_post_snapshot_bytes_excluded_from_b": True,
            "b_post_snapshot_bytes_excluded_from_c": True,
            "a_b_c_distinct": True,
        },
        "terminal_replay": {
            "operation_id_stable": True,
            "destination_commit_identity_stable": True,
            "idempotent_replay": True,
        },
        "fault_injection": {
            "status": "PASS",
            "reason": "Qualified exact pre-Complete owner-loss recovery.",
            "arm_schema": "nokv.restore-crash.arm.v1",
            "evidence_schema": "nokv.restore-crash.evidence.v1",
            "run_id": "10" * 16,
            "root_id": "20" * 16,
            "destination_workspace_incarnation_id": "30" * 16,
            "operation_id": "66" * 16,
            "replay_operation_id": "66" * 16,
            "destination_commit_id": "aa" * 32,
            "replay_destination_commit_id": "aa" * 32,
            "phase": "destination_building",
            "durable_read_version": 7,
            "owner_exit_code": 86,
            "initial_owner_session_absent_before_fault": True,
            "fault_owner_session_absent_before_reopen": True,
            "destination_hidden_before_replay": True,
            "operation_state_before_replay": "running",
            "publication_states_before_replay": ["succeeded", "succeeded"],
            "manifest_publication_operation_ids": ["ab" * 16, "cd" * 16],
            "manifest_artifact_revision_ids": ["bc" * 16, "dc" * 16],
            "manifest_bindings_exact": True,
            "built_commit_members": 0,
            "sealed_revisions": 0,
            "manifest_objects_published_before_crash": 2,
            "destination_generation": 1,
            "interruption_label": "restore-b-pre-complete-crash",
            "interrupted_oracle_label": "restore-b",
            "replay_label": "restore-b",
            "fault_owner_socket_ready": True,
            "successor_owner_socket_ready": True,
            "mcp_survived_fault_owner_exit": True,
            "pre_replay_object_inventory_sha256": "de" * 32,
            "post_replay_object_inventory_sha256": "de" * 32,
            "object_inventory_stable_across_replay": True,
            "client_failure": {
                "status": "error",
                "code": "ClientFailure",
                "retryable": True,
                "details": {"source": "nokv-client", "attempts": 3},
            },
            "idempotent_replay": True,
        },
    }


def barrier_fixture() -> tuple[dict[str, object], dict[str, object]]:
    def raw(value: str) -> list[int]:
        return list(bytes.fromhex(value))

    arm: dict[str, object] = {
        "schema": "nokv.restore-crash.arm.v1",
        "run_id": "10" * 16,
        "root_id": raw("20" * 16),
        "source_workbench": "composition-a",
        "source_workspace_incarnation_id": raw("30" * 16),
        "snapshot_id": 7,
        "destination_workbench": "composition-b",
        "destination_workspace_incarnation_id": raw("40" * 16),
        "operation_id": raw("50" * 16),
    }

    def binding(operation: str, revision: str) -> dict[str, object]:
        identity = {
            "publication_operation_id": raw(operation),
            "artifact_revision_id": raw(revision),
        }
        return {
            "expected": identity,
            "actual": {
                "identity": copy.deepcopy(identity),
                "workspace_incarnation_id": arm["destination_workspace_incarnation_id"],
                "body_digest_uri": "sha256:" + "60" * 32,
                "manifest_digest_uri": "sha256:" + "70" * 32,
                "logical_size": 12,
                "content_type": "application/json",
            },
        }

    envelope: dict[str, object] = {
        "schema": "nokv.restore-crash.evidence.v1",
        "run_id": arm["run_id"],
        "root_id": arm["root_id"],
        "operation_id": arm["operation_id"],
        "evidence": {
            "route": {
                "root_id": arm["root_id"],
                "logical_shard_id": raw("80" * 16),
                "object_namespace_id": raw("90" * 16),
                "placement_generation": 1,
                "owner_epoch": 2,
            },
            "operation_id": arm["operation_id"],
            "durable_read_version": 9,
            "phase": "destination_building",
            "initialization_digest": raw("a0" * 32),
            "destination_workspace_incarnation_id": arm[
                "destination_workspace_incarnation_id"
            ],
            "destination_commit_id": raw("b0" * 32),
            "run_manifest": binding("c0" * 16, "d0" * 16),
            "restore_manifest": binding("e0" * 16, "f0" * 16),
            "built_commit_members": 0,
            "sealed_revisions": 0,
        },
    }
    return arm, envelope


class RestoreCompositionGateTest(unittest.TestCase):
    def test_typed_scenarios_exactly_cover_restore_composition_profile(self) -> None:
        ledger = pre423_contract_ledger.load_ledger()
        expected = {
            scenario
            for item in ledger["items"]
            for gate in item["required_gates"]
            for expectation in (
                pre423_contract_ledger.resolve_gate_expectation(
                    ledger, item["id"], gate
                ),
            )
            if "restore-composition" in expectation["allowed_producers"]
            for scenario in expectation["scenarios"]
        }
        self.assertEqual(set(TYPED_SCENARIOS), expected)

    def test_typed_mode_forbids_dry_run(self) -> None:
        with self.assertRaises(SystemExit), redirect_stderr(StringIO()):
            from restore_composition_gate import parse_args

            parse_args(
                [
                    "--dry-run",
                    "--qualification-result",
                    "/tmp/producer-result.json",
                ]
            )

    def test_oracle_plan_preserves_dirty_nested_restore_without_recommit(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            plan = oracle_plan(config(Path(temporary)))
        labels = [step.label for step in plan]
        self.assertEqual(
            labels,
            [
                "create-a",
                "put-a-rename-source",
                "put-a-delete-source",
                "put-a-payload",
                "commit-a",
                "snapshot-a",
                "mutate-a-after-snapshot",
                "restore-b",
                "find-b-committed",
                "rename-b",
                "remove-b",
                "publish-b",
                "snapshot-b-no-recommit",
                "restore-c",
                "find-c-committed",
                "mutate-b-after-snapshot",
                "retire-snapshot-b",
                "read-c-after-retire",
                "restore-c-terminal-replay",
            ],
        )
        between = labels[
            labels.index("restore-b") + 1 : labels.index("snapshot-b-no-recommit")
        ]
        self.assertNotIn("commit-b", between)
        self.assertEqual(
            plan[labels.index("snapshot-b-no-recommit")].arguments["id"],
            "composition-b",
        )
        self.assertEqual(
            plan[labels.index("restore-c")].arguments["id"], "composition-b"
        )

    def test_mutation_commands_are_workbench_scoped_not_absolute_paths(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            cfg = config(Path(temporary))
            rename = mutation_command(
                cfg,
                "rename",
                workbench="composition-b",
                section="outputs",
                path="rename-source.txt",
                destination_path="renamed.txt",
                expected_generation=1,
            )
            remove = mutation_command(
                cfg,
                "remove",
                workbench="composition-b",
                section="outputs",
                path="delete-source.txt",
                expected_generation=1,
            )
        for command in (rename, remove):
            self.assertIn("workspace-path", command)
            self.assertIn("--expected-generation", command)
            self.assertIn("--request-id", command)
            self.assertNotIn("/agents/composition/wb", " ".join(command))
        rename_start = rename.index("rename")
        self.assertEqual(
            rename[rename_start + 1 : rename_start + 6],
            [
                "composition-b",
                "outputs",
                "rename-source.txt",
                "renamed.txt",
                "--expected-generation",
            ],
        )
        remove_start = remove.index("remove")
        self.assertEqual(
            remove[remove_start + 1 : remove_start + 5],
            [
                "composition-b",
                "outputs",
                "delete-source.txt",
                "--expected-generation",
            ],
        )
        request_id = rename[rename.index("--request-id") + 1]
        self.assertRegex(request_id, r"^[0-9a-f]{32}$")
        self.assertEqual(
            request_id,
            mutation_request_id(
                cfg,
                "rename",
                "composition-b",
                "outputs",
                "rename-source.txt",
                "renamed.txt",
                1,
            ),
        )

    def test_mutation_result_requires_the_exact_public_request_receipt(self) -> None:
        result = {
            "status": "success",
            "workbench_id": "composition-b",
            "generation": 1,
        }
        with self.assertRaisesRegex(WorkflowFailure, "request identity"):
            validate_mutation_result(
                result,
                "rename",
                "composition-b",
                "11" * 16,
            )
        result["request_id"] = "11" * 16
        validate_mutation_result(
            result,
            "rename",
            "composition-b",
            "11" * 16,
        )

    def test_valid_evidence_proves_destination_owned_dirty_composition(self) -> None:
        validate_composition_evidence(valid_evidence())

    def test_validation_rejects_a_recommit_between_restore_and_snapshot(self) -> None:
        evidence = valid_evidence()
        labels = evidence["call_labels"]
        assert isinstance(labels, list)
        labels.insert(labels.index("snapshot-b-no-recommit"), "commit-b")
        with self.assertRaisesRegex(WorkflowFailure, "recommit"):
            validate_composition_evidence(evidence)

    def test_validation_rejects_stale_source_identity_or_one_manifest_restore(
        self,
    ) -> None:
        stale = valid_evidence()
        stale["workbenches"]["c"]["restore_manifest"]["source_workbench_id"] = (
            "composition-a"
        )
        with self.assertRaisesRegex(WorkflowFailure, "B as its source"):
            validate_composition_evidence(stale)

        one_manifest = valid_evidence()
        one_manifest["workbenches"]["b"]["destination_owned_manifest_objects"] = 1
        with self.assertRaisesRegex(WorkflowFailure, "exactly two"):
            validate_composition_evidence(one_manifest)

    def test_validation_rejects_clean_digest_or_retention_regression(self) -> None:
        clean = valid_evidence()
        clean["workbenches"]["c"]["run_manifest"]["content_digest_uri"] = clean[
            "workbenches"
        ]["b"]["run_manifest"]["content_digest_uri"]
        with self.assertRaisesRegex(WorkflowFailure, "dirty"):
            validate_composition_evidence(clean)

        lost = valid_evidence()
        lost["workbenches"]["c"]["readable_after_snapshot_retire"] = False
        with self.assertRaisesRegex(WorkflowFailure, "retirement"):
            validate_composition_evidence(lost)

    def test_fault_boundary_must_pass_before_the_gate_can_pass(self) -> None:
        record = qualification(
            composition="PASS",
            fault="NOT QUALIFIED",
            reason="Exact pre-Complete crash evidence was not executed.",
        )
        self.assertEqual(record["overall_status"], "NOT QUALIFIED")
        self.assertEqual(record["restore_composition"]["status"], "PASS")
        self.assertEqual(
            record["partial_publication_recovery"]["status"], "NOT QUALIFIED"
        )
        self.assertNotEqual(record["partial_publication_recovery"]["status"], "PASS")

    def test_environment_evidence_binds_both_independent_executables(self) -> None:
        default_digest = "11" * 32
        fault_digest = "22" * 32
        value = {
            "schema": "nokv.restore_composition_gate.v1",
            "binary": {"path": "/tmp/nokv", "sha256": default_digest},
            "fault_binary": {
                "path": "/tmp/nokv-restore-crash-owner",
                "sha256": fault_digest,
            },
        }
        validate_environment_evidence(
            value,
            expected_binary_sha256=default_digest,
            expected_fault_binary_sha256=fault_digest,
        )

        missing = copy.deepcopy(value)
        del missing["fault_binary"]
        with self.assertRaisesRegex(WorkflowFailure, "both executable identities"):
            validate_environment_evidence(
                missing,
                expected_binary_sha256=default_digest,
                expected_fault_binary_sha256=fault_digest,
            )

        drifted = copy.deepcopy(value)
        drifted["fault_binary"]["sha256"] = "33" * 32
        with self.assertRaisesRegex(WorkflowFailure, "fault owner digest drifted"):
            validate_environment_evidence(
                drifted,
                expected_binary_sha256=default_digest,
                expected_fault_binary_sha256=fault_digest,
            )

        reused = copy.deepcopy(value)
        reused["fault_binary"]["sha256"] = default_digest
        with self.assertRaisesRegex(WorkflowFailure, "independently built"):
            validate_environment_evidence(
                reused,
                expected_binary_sha256=default_digest,
                expected_fault_binary_sha256=default_digest,
            )

    def test_validation_rejects_fault_replay_drift_or_nonzero_closure(self) -> None:
        drifted = valid_evidence()
        drifted["fault_injection"]["replay_operation_id"] = "ef" * 16
        with self.assertRaisesRegex(WorkflowFailure, "fault operation identity"):
            validate_composition_evidence(drifted)

        progressed = valid_evidence()
        progressed["fault_injection"]["built_commit_members"] = 1
        with self.assertRaisesRegex(WorkflowFailure, "zero closure progress"):
            validate_composition_evidence(progressed)

    def test_validation_rejects_fault_inventory_or_publication_regression(self) -> None:
        drifted = valid_evidence()
        drifted["fault_injection"]["post_replay_object_inventory_sha256"] = "ff" * 32
        with self.assertRaisesRegex(WorkflowFailure, "object inventory"):
            validate_composition_evidence(drifted)

        unpublished = valid_evidence()
        unpublished["fault_injection"]["publication_states_before_replay"] = [
            "succeeded",
            "running",
        ]
        with self.assertRaisesRegex(WorkflowFailure, "manifest publications"):
            validate_composition_evidence(unpublished)

        missing_failure = valid_evidence()
        del missing_failure["fault_injection"]["client_failure"]
        with self.assertRaisesRegex(WorkflowFailure, "bounded client failure"):
            validate_composition_evidence(missing_failure)

    def test_started_process_records_redacted_argv_and_pid(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            cfg = config(root)
            evidence = Evidence(root / "evidence")
            evidence.prepare()
            with (root / "process.log").open("w", encoding="utf-8") as log:
                process = start_process(
                    [
                        sys.executable,
                        "-c",
                        "pass",
                        "--object-access-key-id",
                        "sentinel-access-key",
                    ],
                    cfg,
                    log,
                    evidence=evidence,
                    label="test-owner",
                )
                self.assertEqual(process.wait(timeout=10), 0)
            record = json.loads(
                (evidence.root / "processes.jsonl").read_text(encoding="utf-8")
            )
        self.assertEqual(record["label"], "test-owner")
        self.assertEqual(record["pid"], process.pid)
        self.assertIn("started_at", record)
        self.assertNotIn("sentinel-access-key", json.dumps(record))
        self.assertEqual(record["argv"][-1], "<redacted>")

    def test_required_workflow_runs_feature_tests_and_uploads_only_evidence(
        self,
    ) -> None:
        workflow = (REPO / ".github" / "workflows" / "rust.yml").read_text(
            encoding="utf-8"
        )
        self.assertIn(
            "cargo test -p nokv-server --features restore-crash-test-support",
            workflow,
        )
        self.assertIn(
            "cargo test -p nokv-bench --bin nokv-restore-crash-owner "
            "--features restore-crash-test-support",
            workflow,
        )
        self.assertIn(
            '--fault-target-dir "${RUNNER_TEMP}/restore-composition-fault-target"',
            workflow,
        )
        self.assertIn("path: ${{ runner.temp }}/restore-composition/evidence", workflow)
        self.assertNotIn("path: ${{ runner.temp }}/restore-composition\n", workflow)

    def test_fault_barrier_requires_exact_dual_manifest_binding(self) -> None:
        arm, envelope = barrier_fixture()
        summary = validate_fault_barrier_evidence(envelope, arm)
        self.assertEqual(summary["phase"], "destination_building")
        self.assertEqual(summary["built_commit_members"], 0)
        self.assertEqual(summary["sealed_revisions"], 0)
        self.assertEqual(
            summary["manifest_publication_operation_ids"],
            ["c0" * 16, "e0" * 16],
        )

        drifted = copy.deepcopy(envelope)
        drifted["evidence"]["run_manifest"]["actual"]["identity"][
            "artifact_revision_id"
        ] = list(bytes.fromhex("01" * 16))
        with self.assertRaisesRegex(WorkflowFailure, "binding is not exact"):
            validate_fault_barrier_evidence(drifted, arm)

    def test_only_bounded_client_failure_qualifies_owner_loss(self) -> None:
        error = {
            "status": "error",
            "code": "ClientFailure",
            "retryable": True,
            "details": {"source": "nokv-client", "attempts": 3},
        }
        validate_owner_loss_error(error, "restore-b")
        for field, value in (
            ("code", "Transport"),
            ("retryable", False),
            ("details", {"source": "nokv-client", "attempts": 2}),
        ):
            invalid = copy.deepcopy(error)
            invalid[field] = value
            with self.assertRaisesRegex(WorkflowFailure, "bounded client failure"):
                validate_owner_loss_error(invalid, "restore-b")

    def test_owner_loss_requires_matching_text_and_structured_content(self) -> None:
        error = {
            "status": "error",
            "code": "ClientFailure",
            "retryable": True,
            "details": {"source": "nokv-client", "attempts": 3},
        }
        result = {
            "isError": True,
            "content": [{"type": "text", "text": json.dumps(error)}],
            "structuredContent": error,
        }
        self.assertEqual(matching_mcp_structured_content(result, "restore-b"), error)
        drifted = copy.deepcopy(result)
        drifted["structuredContent"]["retryable"] = False
        with self.assertRaisesRegex(WorkflowFailure, "text and structured"):
            matching_mcp_structured_content(drifted, "restore-b")

    def test_build_timeout_is_independent_from_the_fault_deadline(self) -> None:
        self.assertEqual(build_timeout(60.0), BUILD_TIMEOUT_SECONDS)
        self.assertEqual(build_timeout(BUILD_TIMEOUT_SECONDS + 1), 1_201.0)
        self.assertEqual(config(Path("/tmp/restore-gate-test")).timeout, 60.0)

    def test_validation_requires_controlled_owner_and_socket_evidence(self) -> None:
        for field in (
            "fault_owner_socket_ready",
            "successor_owner_socket_ready",
            "mcp_survived_fault_owner_exit",
        ):
            missing = valid_evidence()
            missing["fault_injection"][field] = False
            with self.assertRaisesRegex(WorkflowFailure, "successor/replay boundary"):
                validate_composition_evidence(missing)

        unbound = valid_evidence()
        unbound["fault_injection"]["interrupted_oracle_label"] = "restore-c"
        with self.assertRaisesRegex(WorkflowFailure, "fault arm"):
            validate_composition_evidence(unbound)

    def test_malformed_fault_publication_ids_fail_closed(self) -> None:
        malformed = valid_evidence()
        malformed["fault_injection"]["manifest_publication_operation_ids"] = [
            [],
            "cd" * 16,
        ]
        with self.assertRaisesRegex(WorkflowFailure, "manifest publications"):
            validate_composition_evidence(malformed)

    def test_validation_rejects_excluded_filesystem_or_layout_surfaces(self) -> None:
        evidence = valid_evidence()
        exclusions = evidence["excluded_surfaces"]
        assert isinstance(exclusions, list)
        exclusions.remove("inode")
        with self.assertRaisesRegex(WorkflowFailure, "excluded surfaces"):
            validate_composition_evidence(evidence)

    def test_rustfs_image_is_digest_pinned(self) -> None:
        self.assertIn("@sha256:", PINNED_RUSTFS_IMAGE)
        self.assertNotIn(":latest", PINNED_RUSTFS_IMAGE)

    def test_process_evidence_redacts_all_object_credential_identifiers(self) -> None:
        redacted = redact_argv(
            [
                "docker",
                "-e",
                "RUSTFS_ACCESS_KEY=access-identifier",
                "-e",
                "RUSTFS_SECRET_KEY=secret-value",
                "nokv",
                "--object-access-key-id",
                "access-identifier",
                "--object-secret-access-key",
                "secret-value",
            ]
        )
        serialized = " ".join(redacted)
        self.assertNotIn("access-identifier", serialized)
        self.assertNotIn("secret-value", serialized)
        self.assertIn("RUSTFS_ACCESS_KEY=<redacted>", redacted)
        self.assertIn("RUSTFS_SECRET_KEY=<redacted>", redacted)
        self.assertEqual(redacted[-2:], ["--object-secret-access-key", "<redacted>"])

    def test_rust_ci_freezes_the_gate_without_claiming_a_live_pass(self) -> None:
        workflow = (REPO / ".github/workflows/rust.yml").read_text(encoding="utf-8")
        self.assertIn("scripts/workbench/restore_composition_gate.py", workflow)
        self.assertIn("scripts/workbench/restore_composition_gate_test.py", workflow)
        self.assertIn("--fault-target-dir", workflow)
        self.assertIn('.partial_publication_recovery.status == "PASS"', workflow)
        self.assertIn('.overall_status == "PASS"', workflow)

    def test_evidence_fixtures_are_independent(self) -> None:
        first = valid_evidence()
        second = copy.deepcopy(first)
        second["workbenches"]["c"]["id"] = "changed"
        self.assertEqual(first["workbenches"]["c"]["id"], "composition-c")


if __name__ == "__main__":
    unittest.main()
