#!/usr/bin/env python3
"""Contract tests for the path-native fork-to-restore live gate."""

from __future__ import annotations

import copy
import json
import socket
import threading
import unittest
from pathlib import Path

from fork_restore_recovery_gate import (
    proxy_server_command,
    BLOCK_BYTES,
    CONCURRENT_RESTORES,
    FULL_PROFILE_FILES,
    RESTORE_MANIFEST_OBJECTS,
    WORKSPACE_HANDSHAKE_MAGIC,
    ResponseDropProxy,
    WorkflowFailure,
    classify_manifest_objects,
    decode_cli_error,
    fixture_block_marker,
    fixture_logical_bytes,
    is_handshake_frame,
    is_retryable_restore_race,
    request_operation,
    validate_gate_evidence,
    validate_restore_manifest,
    validate_run_manifest,
)


REPO = Path(__file__).resolve().parents[2]


def valid_evidence() -> dict[str, object]:
    return {
        "status": "PASS",
        "fixture": {
            "files": 16,
            "block_bytes": BLOCK_BYTES,
            "logical_bytes": fixture_logical_bytes(16),
            "full_profile": False,
            "object_count": 16,
            "object_bytes": fixture_logical_bytes(16),
            "distinct_etags": 16,
            "marker_digest": "ab" * 32,
        },
        "restarts": [
            {
                "phase": "prepare_restore",
                "signal": "SIGKILL",
                "request_failed_before_retry": True,
                "before_owner_epoch": 1,
                "after_owner_epoch": 2,
            },
            {
                "phase": "finalize_restore",
                "signal": "SIGKILL",
                "request_failed_before_retry": True,
                "before_owner_epoch": 2,
                "after_owner_epoch": 3,
            },
        ],
        "forks": {
            "prepare_ack_loss": {
                "manifest_objects_added": RESTORE_MANIFEST_OBJECTS,
                "payload_objects_overwritten": 0,
            },
            "terminal_ack_loss": {
                "manifest_objects_added": RESTORE_MANIFEST_OBJECTS,
                "payload_objects_overwritten": 0,
                "retry_replayed": True,
            },
            "concurrent": {
                "manifest_objects_added": RESTORE_MANIFEST_OBJECTS,
                "payload_objects_overwritten": 0,
                "calls": CONCURRENT_RESTORES,
                "unique_operation_ids": 1,
                "failures": 0,
                "retry_attempts": [1] * CONCURRENT_RESTORES,
            },
            "nested": {
                "manifest_objects_added": RESTORE_MANIFEST_OBJECTS,
                "payload_objects_overwritten": 0,
                "content_matches_parent": True,
                "source_recommit_replace": True,
                "source_commit_objects_added": 1,
            },
        },
        "retention": {
            "source_snapshot_retired": True,
            "source_diverged": True,
            "forks_preserved_snapshot": True,
        },
    }


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def handshake_frame() -> bytes:
    """One canonical 48-byte ClientHello frame with a placeholder schema."""
    payload = bytearray(48)
    payload[:8] = WORKSPACE_HANDSHAKE_MAGIC
    payload[8] = 1  # HandshakeKind::ClientHello
    schema = b"nokv.workspace.test"
    payload[9] = len(schema)
    payload[16 : 16 + len(schema)] = schema
    return len(payload).to_bytes(4, "big") + bytes(payload)


def read_exact(stream: socket.socket, length: int) -> bytes:
    value = bytearray()
    while len(value) < length:
        chunk = stream.recv(length - len(value))
        if not chunk:
            break
        value.extend(chunk)
    return bytes(value)


class ForkRestoreRecoveryGateTest(unittest.TestCase):
    def test_full_profile_is_exactly_one_gibibyte(self) -> None:
        self.assertEqual(FULL_PROFILE_FILES, 256)
        self.assertEqual(fixture_logical_bytes(FULL_PROFILE_FILES), 1 << 30)
        self.assertEqual(CONCURRENT_RESTORES, 16)

    def test_each_cow_file_has_a_deterministic_distinct_marker(self) -> None:
        first = fixture_block_marker(0)
        self.assertEqual(first, fixture_block_marker(0))
        self.assertNotEqual(first, fixture_block_marker(1))
        self.assertLess(len(first), BLOCK_BYTES)
        with self.assertRaisesRegex(WorkflowFailure, "non-negative"):
            fixture_block_marker(-1)

    def test_restore_manifest_requires_the_complete_path_native_identity(self) -> None:
        manifest = {
            "schema": "nokv.workbench.restore_manifest.v2",
            "operation_id": "11" * 16,
            "source_workbench_id": "source",
            "source_path": "/agents/materials/wb/source",
            "destination_workbench_id": "destination",
            "destination_path": "/agents/materials/wb/destination",
            "restored_from": {
                "workbench_id": "source",
                "path": "/agents/materials/wb/source",
                "source": {"kind": "snapshot", "snapshot_id": 7},
            },
        }
        validate_restore_manifest(manifest, "source", "destination")
        # v1 carried the snapshot id twice and the gate cross-checked the two
        # copies. v2 carries it once, so that particular mutation is no longer
        # detectable from inside the document -- and does not need to be: the
        # manifest is content-addressed, so an altered byte changes its digest
        # and therefore the revision the restore is bound to. What remains
        # worth asserting is everything the document can still contradict.
        for mutate, why in (
            (lambda m: m["restored_from"].__setitem__("workbench_id", "other"),
             "restored_from names a different source workbench"),
            (lambda m: m["restored_from"].__setitem__("path", "/agents/materials/wb/other"),
             "restored_from names a different source path"),
            (lambda m: m["restored_from"]["source"].__setitem__("snapshot_id", 0),
             "a zero snapshot id is not a snapshot"),
            (lambda m: m["restored_from"].__setitem__("source", {"kind": "commit", "commit_id": "ab" * 32}),
             "a commit source is different provenance, not another spelling"),
            (lambda m: m["restored_from"].__setitem__("source", {"kind": "snapshot"}),
             "a snapshot source without its id"),
        ):
            malformed = copy.deepcopy(manifest)
            mutate(malformed)
            with self.assertRaisesRegex(WorkflowFailure, "exact fork identities", msg=why):
                validate_restore_manifest(malformed, "source", "destination")

    def test_every_restore_adds_exactly_two_destination_manifests(self) -> None:
        self.assertEqual(RESTORE_MANIFEST_OBJECTS, 2)
        restore = {"schema": "nokv.workbench.restore_manifest.v2"}
        run_manifest = {"schema": "nokv.workbench.run_manifest.v1"}
        self.assertEqual(
            classify_manifest_objects({"k/a": run_manifest, "k/b": restore}, "fork"),
            ("k/b", "k/a"),
        )
        with self.assertRaisesRegex(WorkflowFailure, "one restore manifest plus one run"):
            classify_manifest_objects({"k/a": restore, "k/b": restore}, "fork")
        with self.assertRaisesRegex(WorkflowFailure, "one restore manifest plus one run"):
            classify_manifest_objects({"k/a": restore, "k/b": b"payload"}, "fork")
        evidence = valid_evidence()
        evidence["forks"]["concurrent"]["manifest_objects_added"] = 1  # type: ignore[index]
        with self.assertRaisesRegex(WorkflowFailure, "exactly two destination-owned"):
            validate_gate_evidence(evidence)

    def test_run_manifest_must_be_owned_by_the_destination(self) -> None:
        manifest = {
            "schema": "nokv.workbench.run_manifest.v1",
            "workbench_id": "destination",
            "workbench_path": "/agents/materials/wb/destination",
            "commit_identity": "cd" * 32,
            "content_digest_uri": "sha256:" + "ab" * 32,
        }
        validate_run_manifest(manifest, "destination")
        with self.assertRaisesRegex(WorkflowFailure, "not owned by workbench source"):
            validate_run_manifest(manifest, "source")

    def test_nested_source_must_be_recommitted_with_explicit_replace(self) -> None:
        evidence = valid_evidence()
        evidence["forks"]["nested"]["source_recommit_replace"] = False  # type: ignore[index]
        with self.assertRaisesRegex(WorkflowFailure, "nested fork"):
            validate_gate_evidence(evidence)

    def test_nonpositive_fixture_is_rejected(self) -> None:
        with self.assertRaisesRegex(WorkflowFailure, "positive"):
            fixture_logical_bytes(0)

    def test_request_classifier_reads_the_wire_operation_tag(self) -> None:
        request = {
            "route": {},
            "request_id": "00" * 16,
            "operation": {"operation": "finalize_restore", "request": {}},
        }
        self.assertEqual(
            request_operation(json.dumps(request).encode()), "finalize_restore"
        )
        self.assertIsNone(request_operation(b"not-json"))

    def test_only_structured_retryable_restore_races_are_redriven(self) -> None:
        stderr = (
            'nokv: {"code":"Conflict","details":{"conflict":"OperationState"},'
            '"message":"operation progressed","retryable":true,"status":"error"}\n'
        )
        error = decode_cli_error(stderr)
        self.assertTrue(is_retryable_restore_race(error))

        stale_nonretryable = copy.deepcopy(error)
        stale_nonretryable["retryable"] = False
        self.assertFalse(is_retryable_restore_race(stale_nonretryable))
        predicate_race = copy.deepcopy(error)
        predicate_race["details"]["conflict"] = None
        self.assertTrue(is_retryable_restore_race(predicate_race))
        unrelated = copy.deepcopy(error)
        unrelated["details"]["conflict"] = "PathGeneration"
        self.assertFalse(is_retryable_restore_race(unrelated))

    def test_response_drop_happens_only_after_the_owner_response(self) -> None:
        try:
            backend_port = free_port()
            proxy_port = free_port()
        except PermissionError:
            self.skipTest("the current sandbox forbids loopback listeners")
        backend_ready = threading.Event()
        owner_responded = threading.Event()

        def backend() -> None:
            with socket.socket() as listener:
                listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                listener.bind(("127.0.0.1", backend_port))
                listener.listen(1)
                backend_ready.set()
                connection, _ = listener.accept()
                with connection:
                    length = int.from_bytes(read_exact(connection, 4), "big")
                    hello = read_exact(connection, length)
                    if not is_handshake_frame(hello):
                        return
                    accepted = bytearray(hello)
                    accepted[8] = 2  # HandshakeKind::Accepted
                    connection.sendall(len(accepted).to_bytes(4, "big") + accepted)
                    length = int.from_bytes(read_exact(connection, 4), "big")
                    request = read_exact(connection, length)
                    if is_handshake_frame(request):
                        return
                    response = b'{"status":"committed"}'
                    connection.sendall(len(response).to_bytes(4, "big") + response)
                    owner_responded.set()

        backend_thread = threading.Thread(target=backend, daemon=True)
        backend_thread.start()
        self.assertTrue(backend_ready.wait(2))
        proxy = ResponseDropProxy(proxy_port, 2)
        proxy.start()
        proxy.set_target(backend_port)
        callback = threading.Event()
        proxy.arm("prepare_restore", callback.set)
        request = json.dumps(
            {"operation": {"operation": "prepare_restore", "request": {}}}
        ).encode()
        with socket.create_connection(("127.0.0.1", proxy_port), timeout=2) as client:
            client.sendall(handshake_frame())
            self.assertEqual(read_exact(client, len(handshake_frame()))[4:12], WORKSPACE_HANDSHAKE_MAGIC)
            client.sendall(len(request).to_bytes(4, "big") + request)
            self.assertEqual(client.recv(1), b"")
        proxy.wait_dropped()
        proxy.close()
        backend_thread.join(timeout=2)
        self.assertTrue(owner_responded.is_set())
        self.assertTrue(callback.is_set())

    def test_valid_evidence_covers_ack_loss_concurrency_nested_and_retention(self) -> None:
        validate_gate_evidence(valid_evidence())

    def test_owner_epochs_must_advance_once_at_each_dropped_response(self) -> None:
        evidence = valid_evidence()
        evidence["restarts"][1]["after_owner_epoch"] = 4  # type: ignore[index]
        with self.assertRaisesRegex(WorkflowFailure, "finalize_restore"):
            validate_gate_evidence(evidence)

    def test_concurrent_calls_must_converge_to_one_operation(self) -> None:
        evidence = valid_evidence()
        evidence["forks"]["concurrent"]["unique_operation_ids"] = 2  # type: ignore[index]
        with self.assertRaisesRegex(WorkflowFailure, "concurrent"):
            validate_gate_evidence(evidence)

    def test_fixture_requires_one_distinct_object_per_marker(self) -> None:
        evidence = valid_evidence()
        evidence["fixture"]["distinct_etags"] = 15  # type: ignore[index]
        with self.assertRaisesRegex(WorkflowFailure, "object closure"):
            validate_gate_evidence(evidence)

    def test_zero_copy_evidence_rejects_overwritten_payloads(self) -> None:
        evidence = valid_evidence()
        evidence["forks"]["nested"]["payload_objects_overwritten"] = 1  # type: ignore[index]
        with self.assertRaisesRegex(WorkflowFailure, "overwrote"):
            validate_gate_evidence(evidence)

    def test_terminal_ack_loss_requires_replay(self) -> None:
        evidence = valid_evidence()
        evidence["forks"]["terminal_ack_loss"]["retry_replayed"] = False  # type: ignore[index]
        with self.assertRaisesRegex(WorkflowFailure, "did not replay"):
            validate_gate_evidence(evidence)

    def test_retired_source_snapshot_must_not_change_fork_content(self) -> None:
        evidence = copy.deepcopy(valid_evidence())
        evidence["retention"]["forks_preserved_snapshot"] = False  # type: ignore[index]
        with self.assertRaisesRegex(WorkflowFailure, "retirement"):
            validate_gate_evidence(evidence)

    def test_rust_ci_runs_and_uploads_the_fork_restore_gate(self) -> None:
        workflow = (REPO / ".github/workflows/rust.yml").read_text(encoding="utf-8")
        for expected in (
            "fork-restore-recovery:",
            "python3 scripts/workbench/fork_restore_recovery_gate.py",
            "python3 scripts/workbench/fork_restore_recovery_gate_test.py",
            "--fixture-files 64",
            "fork-restore-recovery-${{ github.sha }}",
            "if-no-files-found: error",
        ):
            with self.subTest(expected=expected):
                self.assertIn(expected, workflow)



class ProxyServerCommandTest(unittest.TestCase):
    def test_proxied_owner_keeps_local_wal_as_recovery_authority(self) -> None:
        command = proxy_server_command(
            ["nokv", "--root-id", "11" * 16],
            ["--object-bucket", "b"],
            7801,
            7802,
            "fork-gate-e1",
            "--metadata-reopen",
            Path("/tmp/meta"),
        )
        serve = command.index("serve")
        self.assertEqual(command[serve - 2 : serve], ["--recovery-publication", "local-only"])
        self.assertEqual(command[command.index("--advertise-endpoint") + 1], "127.0.0.1:7802")
        self.assertNotIn("--agent-id", command)


if __name__ == "__main__":
    unittest.main()
