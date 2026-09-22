#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the wire-compatibility matrix gate helpers."""

import io
import struct
import sys
import unittest
from contextlib import redirect_stdout

sys.path.insert(0, __import__("os").path.dirname(__file__))
import wire_compat_gate as gate  # noqa: E402


class FrameEncodingTests(unittest.TestCase):
    def test_handshake_round_trips_exact_fields(self) -> None:
        frame = gate.encode_handshake(gate.KIND_CLIENT_HELLO, "nokv.workspace.rpc.v9")
        self.assertEqual(len(frame), 4 + gate.HANDSHAKE_PAYLOAD_BYTES)
        self.assertEqual(struct.unpack(">I", frame[:4])[0], gate.HANDSHAKE_PAYLOAD_BYTES)
        kind, schema = gate.decode_handshake(frame)
        self.assertEqual(kind, gate.KIND_CLIENT_HELLO)
        self.assertEqual(schema, "nokv.workspace.rpc.v9")

    def test_handshake_rejects_oversized_schema(self) -> None:
        with self.assertRaises(AssertionError):
            gate.encode_handshake(gate.KIND_CLIENT_HELLO, "x" * 33)

    def test_incompatible_handshake_decodes_kind_and_advertised_schema(self) -> None:
        frame = gate.encode_handshake(gate.KIND_INCOMPATIBLE, "nokv.workspace.rpc.v9")
        kind, schema = gate.decode_handshake(frame)
        self.assertEqual(kind, gate.KIND_INCOMPATIBLE)
        self.assertEqual(schema, "nokv.workspace.rpc.v9")

    def test_legacy_v2_frame_is_a_complete_public_request(self) -> None:
        root_id = bytes([1]) * 16
        shard_id = bytes([2]) * 16
        request_id = bytes([4]) * 16
        frame = gate.encode_legacy_v2_business_frame(
            root_id,
            shard_id,
            placement_generation=7,
            owner_epoch=11,
            request_id=request_id,
        )
        length = struct.unpack(">I", frame[:4])[0]
        self.assertEqual(len(frame), 4 + length)
        body = frame[4:]
        # Top-level schema/payload map, with a nonempty payload carrying the
        # complete v2 route, request id, and a real CreateWorkspace operation.
        self.assertEqual(body[0], 0x82)
        self.assertIn(b"schema", body)
        self.assertIn(gate.LEGACY_V2_SCHEMA.encode(), body)
        self.assertIn(b"route", body)
        self.assertIn(b"request_id", body)
        self.assertIn(b"create_workspace", body)
        self.assertIn(b"\xDC\x00\x10" + request_id, body)

    def test_legacy_v2_upgrade_response_is_exactly_framed(self) -> None:
        root_id = bytes([1]) * 16
        shard_id = bytes([2]) * 16
        request_id = bytes([4]) * 16
        response = gate.encode_legacy_v2_upgrade_response(
            root_id,
            shard_id,
            placement_generation=7,
            owner_epoch=11,
            request_id=request_id,
        )
        self.assertEqual(len(response), 4 + struct.unpack(">I", response[:4])[0])
        body = response[4:]
        self.assertEqual(body[0], 0x82)
        self.assertIn(gate.LEGACY_V2_SCHEMA.encode(), body)
        self.assertIn(b"precondition_failed", body)
        self.assertIn(gate.LEGACY_CLIENT_UPGRADE_MESSAGE.encode(), body)
        self.assertIn(b"\xDC\x00\x10" + request_id, body)


class EvidenceTests(unittest.TestCase):
    def test_evidence_records_and_writes_all_scenarios(self) -> None:
        import json
        import os
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "evidence.json")
            evidence = gate.Evidence(path)
            evidence.record("a", True, "ok")
            evidence.record("b", False, "boom")
            evidence.record_artifact("source", "/tmp/source.tar.gz", "11" * 32, "11" * 32)
            evidence.write()
            with open(path, encoding="utf-8") as handle:
                data = json.load(handle)
            self.assertEqual(len(data["wire_compat_matrix"]), 2)
            self.assertEqual(data["wire_compat_matrix"][0]["scenario"], "a")
            self.assertTrue(data["wire_compat_matrix"][0]["passed"])
            self.assertFalse(data["wire_compat_matrix"][1]["passed"])
            self.assertEqual(data["legacy_artifacts"]["source"]["path"], "source.tar.gz")
            self.assertEqual(data["legacy_artifacts"]["source"]["sha256"], "11" * 32)


class DigestTests(unittest.TestCase):
    def test_verify_pinned_digest_accepts_matching_file(self) -> None:
        import hashlib
        import os
        import tempfile

        with tempfile.NamedTemporaryFile(delete=False) as handle:
            handle.write(b"legacy source")
            path = handle.name
        try:
            expected = hashlib.sha256(b"legacy source").hexdigest()
            self.assertEqual(
                gate.verify_pinned_digest(path, expected, "legacy source"),
                expected,
            )
        finally:
            os.unlink(path)

    def test_verify_pinned_digest_rejects_mismatch_and_invalid_pin(self) -> None:
        import os
        import tempfile

        with tempfile.NamedTemporaryFile(delete=False) as handle:
            handle.write(b"legacy binary")
            path = handle.name
        try:
            with self.assertRaisesRegex(ValueError, "digest mismatch"):
                gate.verify_pinned_digest(path, "00" * 32, "legacy binary")
            with self.assertRaisesRegex(ValueError, "lowercase hexadecimal"):
                gate.verify_pinned_digest(path, "not-a-digest", "legacy binary")
        finally:
            os.unlink(path)


class SocketHelperTests(unittest.TestCase):
    def test_read_exact_surfaces_early_close_as_connection_error(self) -> None:
        import socket

        left, right = socket.socketpair()
        try:
            right.sendall(b"ab")
            right.close()
            with self.assertRaises(ConnectionError):
                gate.read_exact(left, 4, timeout=1.0)
        finally:
            left.close()

    def test_read_frame_round_trips_a_length_prefixed_frame(self) -> None:
        import socket

        payload = gate.encode_handshake(gate.KIND_ACCEPTED, "nokv.workspace.rpc.v9")
        left, right = socket.socketpair()
        try:
            right.sendall(payload)
            self.assertEqual(gate.read_frame(left, timeout=1.0), payload)
        finally:
            left.close()
            right.close()


if __name__ == "__main__":
    unittest.main()
