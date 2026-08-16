#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Contract tests for the pre-#423 restore-composition acceptance gate."""

from __future__ import annotations

import copy
import tempfile
import unittest
from pathlib import Path

from restore_composition_gate import (
    PINNED_RUSTFS_IMAGE,
    PRE423_ORACLE_REVISION,
    Config,
    WorkflowFailure,
    mutation_request_id,
    mutation_command,
    oracle_plan,
    qualification,
    validate_composition_evidence,
    validate_mutation_result,
)


REPO = Path(__file__).resolve().parents[2]


def config(root: Path) -> Config:
    return Config(
        repo=REPO,
        binary=root / "nokv",
        evidence=root / "evidence",
        target=root / "target",
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
                    "schema": "nokv.workbench.restore_manifest.v1",
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
                    "schema": "nokv.workbench.restore_manifest.v1",
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
            "status": "NOT QUALIFIED",
            "reason": "No public object-first/pre-Complete crash boundary exists.",
        },
    }


class RestoreCompositionGateTest(unittest.TestCase):
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

    def test_fault_boundary_is_honestly_not_qualified_not_a_pass(self) -> None:
        record = qualification(
            composition="PASS",
            fault="NOT QUALIFIED",
            reason="No public object-first/pre-Complete crash boundary exists.",
        )
        self.assertEqual(record["overall_status"], "PASS")
        self.assertEqual(record["restore_composition"]["status"], "PASS")
        self.assertEqual(
            record["partial_publication_recovery"]["status"], "NOT QUALIFIED"
        )
        self.assertNotEqual(record["partial_publication_recovery"]["status"], "PASS")

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

    def test_rust_ci_freezes_the_gate_without_claiming_a_live_pass(self) -> None:
        workflow = (REPO / ".github/workflows/rust.yml").read_text(encoding="utf-8")
        self.assertIn("scripts/workbench/restore_composition_gate.py", workflow)
        self.assertIn("scripts/workbench/restore_composition_gate_test.py", workflow)

    def test_evidence_fixtures_are_independent(self) -> None:
        first = valid_evidence()
        second = copy.deepcopy(first)
        second["workbenches"]["c"]["id"] = "changed"
        self.assertEqual(first["workbenches"]["c"]["id"], "composition-c")


if __name__ == "__main__":
    unittest.main()
