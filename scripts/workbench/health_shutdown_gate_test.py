#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the health/shutdown gate helpers."""

from __future__ import annotations

import argparse
import unittest
from pathlib import Path

import health_shutdown_gate as gate


class HealthShutdownGateTest(unittest.TestCase):
    def test_fixed_id_is_stable_hex32(self) -> None:
        first = gate.fixed_id("seed-a", "root")
        second = gate.fixed_id("seed-a", "root")
        self.assertEqual(first, second)
        self.assertEqual(len(first), 32)
        int(first, 16)
        self.assertNotEqual(first, gate.fixed_id("seed-b", "root"))
        self.assertNotEqual(first, gate.fixed_id("seed-a", "shard"))

    def test_control_args_require_positive_ttl_and_carry_identity(self) -> None:
        argv = gate.control_args(
            Path("/bin/nokv"),
            "a" * 32,
            "http://127.0.0.1:22379",
            "/nokv/test",
        )
        self.assertEqual(argv[0], "/bin/nokv")
        self.assertIn("--root-id", argv)
        self.assertIn("a" * 32, argv)
        self.assertIn("--etcd-endpoint", argv)
        self.assertIn("--etcd-lease-ttl-seconds", argv)
        self.assertIn(str(gate.GRACEFUL_TTL_SECONDS), argv)

    def test_server_command_includes_the_health_endpoint(self) -> None:
        command = gate.server_command(
            ["nokv", "--root-id", "b" * 32],
            ["--object-bucket", "bucket"],
            17750,
            17751,
            "node-a",
            "--metadata-create",
            Path("/tmp/meta"),
        )
        self.assertEqual(command[-1], "serve")
        self.assertIn("--health-endpoint", command)
        self.assertIn("127.0.0.1:17751", command)
        self.assertIn("--bind", command)
        self.assertIn("127.0.0.1:17750", command)
        self.assertIn("--advertise-endpoint", command)
        self.assertIn("--metadata-create", command)

    def test_object_args_scope_every_stage_under_one_prefix(self) -> None:
        argv = gate.object_args(
            "stage-a", "http://127.0.0.1:9000", "bucket", "root", "key", "secret"
        )
        self.assertIn("--object-bucket", argv)
        self.assertIn("bucket", argv)
        self.assertIn("--object-root", argv)
        self.assertEqual(argv[argv.index("--object-root") + 1], "root/stage-a")
        self.assertIn("--object-access-key-id", argv)
        self.assertIn("key", argv)
        self.assertIn("--object-secret-access-key", argv)

    def test_clean_environment_strips_proxy_variables(self) -> None:
        import os

        os.environ["HTTP_PROXY"] = "http://127.0.0.1:7892"
        os.environ["https_proxy"] = "http://127.0.0.1:7892"
        cleaned = gate.clean_environment()
        for name in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
            self.assertNotIn(name, cleaned)
        self.assertIn("PATH", cleaned)
        del os.environ["HTTP_PROXY"]
        del os.environ["https_proxy"]

    def test_validate_stats_accepts_the_serving_shape(self) -> None:
        gate.validate_stats(
            {
                "pid": 1234,
                "uptime_seconds": 1.5,
                "protocol_schema": "nokv.workspace.rpc.v9",
                "installed_roots": 1,
                "owner_loss": False,
                "draining": False,
                "ready": True,
                "connections_total": 1,
                "requests_total": 2,
                "inflight_connections": 0,
            },
            "sample",
        )

    def test_validate_stats_rejects_missing_or_wrong_fields(self) -> None:
        base = {
            "pid": 1234,
            "uptime_seconds": 1.5,
            "protocol_schema": "nokv.workspace.rpc.v9",
            "installed_roots": 1,
            "owner_loss": False,
            "draining": False,
            "ready": True,
            "connections_total": 1,
            "requests_total": 2,
            "inflight_connections": 0,
        }
        for field in list(base):
            damaged = dict(base)
            del damaged[field]
            with self.assertRaises(gate.WorkflowFailure):
                gate.validate_stats(damaged, "missing-field")
        with self.assertRaises(gate.WorkflowFailure):
            gate.validate_stats({**base, "ready": False}, "owner-loss")
        with self.assertRaises(gate.WorkflowFailure):
            gate.validate_stats({**base, "owner_loss": True}, "owner-loss")
        with self.assertRaises(gate.WorkflowFailure):
            gate.validate_stats({**base, "installed_roots": 0}, "no-roots")
        with self.assertRaises(gate.WorkflowFailure):
            gate.validate_stats({**base, "pid": 0}, "bad-pid")
        with self.assertRaises(gate.WorkflowFailure):
            gate.validate_stats({**base, "protocol_schema": "other.rpc"}, "bad-schema")

    def test_parser_requires_evidence_dir(self) -> None:
        with self.assertRaises(SystemExit):
            gate.parser().parse_args([])

    def test_parser_requires_a_real_etcd_and_object_target(self) -> None:
        parsed = gate.parser().parse_args(
            [
                "--evidence-dir",
                "/tmp/evidence",
                "--object-endpoint",
                "http://127.0.0.1:9000",
                "--object-bucket",
                "bucket",
                "--etcd-bin",
                "/usr/bin/etcd",
                "--etcdctl-bin",
                "/usr/bin/etcdctl",
                "--binary",
                "/tmp/nokv",
            ]
        )
        self.assertEqual(parsed.evidence_dir, Path("/tmp/evidence"))
        self.assertEqual(parsed.object_endpoint, "http://127.0.0.1:9000")
        self.assertEqual(parsed.etcd_bin, Path("/usr/bin/etcd"))

    def test_wait_exit_kills_a_stubborn_process_and_reports(self) -> None:
        import subprocess
        import sys

        proc = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(60)"]
        )
        with self.assertRaises(gate.WorkflowFailure):
            gate.wait_exit(proc, 0.2)
        self.assertIsNotNone(proc.poll())


if __name__ == "__main__":
    unittest.main()
