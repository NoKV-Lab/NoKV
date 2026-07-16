#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import contextlib
import concurrent.futures
import copy
import hashlib
import http.client
import http.server
import importlib.util
import io
import sys
import tempfile
import threading
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("durable_restore_live_e2e.py")
SPEC = importlib.util.spec_from_file_location("durable_restore_live_e2e", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def restore_tool(schema: dict) -> list[dict]:
    tools = [
        {
            "name": name,
            "description": "",
            "schema": (
                valid_retire_schema()
                if name == "workbench_snapshot_retire"
                else {}
            ),
        }
        for name in sorted(MODULE.BASE_WORKBENCH_TOOLS)
    ]
    tools.append({"name": MODULE.RESTORE_TOOL, "description": "", "schema": schema})
    return tools


def valid_retire_schema() -> dict:
    return {
        "type": "object",
        "required": ["id"],
        "properties": {
            "id": {"type": "string", "minLength": 1},
            "snapshot_id": {"type": "integer", "minimum": 0},
            "name": {"type": "string", "minLength": 1},
            "reason": {
                "type": ["string", "null"],
                "minLength": 1,
                "maxLength": 256,
            },
        },
        "oneOf": [
            {"required": ["snapshot_id"]},
            {"required": ["name"]},
        ],
        "additionalProperties": False,
    }


def valid_schema() -> dict:
    return {
        "type": "object",
        "required": ["id", "at_snapshot", "destination_id"],
        "properties": {
            "id": {"type": "string", "minLength": 1},
            "at_snapshot": {
                "anyOf": [
                    {"type": "integer", "minimum": 0},
                    {"type": "string", "minLength": 1},
                ]
            },
            "destination_id": {"type": "string", "minLength": 1},
        },
        "additionalProperties": False,
    }


class ToolContractTests(unittest.TestCase):
    def test_accepts_exact_restore_contract(self):
        MODULE.validate_tool_contract(restore_tool(valid_schema()))

    def test_rejects_null_restore_snapshot(self):
        schema = valid_schema()
        schema["properties"]["at_snapshot"]["anyOf"].append({"type": "null"})
        with self.assertRaisesRegex(MODULE.AcceptanceError, "exactly two"):
            MODULE.validate_tool_contract(restore_tool(schema))

    def test_rejects_additional_properties(self):
        schema = valid_schema()
        schema["additionalProperties"] = True
        with self.assertRaisesRegex(MODULE.AcceptanceError, "additional"):
            MODULE.validate_tool_contract(restore_tool(schema))

    def test_rejects_empty_destination_identifier_schema(self):
        schema = valid_schema()
        del schema["properties"]["destination_id"]["minLength"]
        with self.assertRaisesRegex(MODULE.AcceptanceError, "non-empty"):
            MODULE.validate_tool_contract(restore_tool(schema))

    def test_rejects_missing_capability_gated_tool(self):
        tools = restore_tool(valid_schema())[:-1]
        with self.assertRaisesRegex(MODULE.AcceptanceError, "missing"):
            MODULE.validate_tool_contract(tools)

    def test_rejects_retire_schema_without_exact_target_union(self):
        tools = restore_tool(valid_schema())
        retire = next(
            tool
            for tool in tools
            if tool["name"] == "workbench_snapshot_retire"
        )
        retire["schema"]["oneOf"] = [{"required": ["snapshot_id", "name"]}]
        with self.assertRaisesRegex(MODULE.AcceptanceError, "exactly one target"):
            MODULE.validate_tool_contract(tools)


class HelpersTests(unittest.TestCase):
    def test_manual_gc_defaults_to_production_sized_pages(self):
        environment = object.__new__(MODULE.LiveEnvironment)
        requests = []

        def http_text(path, *, timeout, method):
            requests.append((path, timeout, method))
            return "{}"

        environment.http_text = http_text

        self.assertEqual(environment.manual_gc(), {})
        self.assertEqual(requests, [("/gc?limit=64", 60, "POST")])

    def test_index_gate_uses_a_valid_multi_page_search_limit(self):
        self.assertGreaterEqual(MODULE.INDEX_PAGE_LIMIT, 1)
        self.assertLessEqual(MODULE.INDEX_PAGE_LIMIT, 10)
        self.assertLess(
            MODULE.INDEX_PAGE_LIMIT,
            MODULE.workload_profile("quick").indexed_files,
        )

    def test_full_profile_is_exactly_one_gibibyte(self):
        profile = MODULE.workload_profile("full")
        self.assertEqual(profile.large_bytes, 1 << 30)
        self.assertGreater(profile.indexed_files, 64)
        self.assertEqual(MODULE.fixture_block_count(profile.large_bytes), 256)

    def test_each_cow_block_has_a_deterministic_distinct_marker(self):
        markers = [MODULE.fixture_block_marker(index) for index in range(256)]
        self.assertEqual(len(set(markers)), 256)
        self.assertTrue(
            all(len(marker) <= MODULE.COW_BLOCK_BYTES for marker in markers)
        )
        self.assertEqual(markers[0], MODULE.fixture_block_marker(0))
        with self.assertRaisesRegex(MODULE.AcceptanceError, "4 MiB"):
            MODULE.fixture_block_count(MODULE.COW_BLOCK_BYTES + 1)

    def test_binary_digest_hashes_the_exact_file(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "nokv"
            path.write_bytes(b"current binary bytes")
            self.assertEqual(
                MODULE.sha256_file(path),
                hashlib.sha256(b"current binary bytes").hexdigest(),
            )

    def test_object_diff_detects_put_overwrite_and_delete(self):
        first = MODULE.ObjectFingerprint(10, "one", "t1")
        overwritten = MODULE.ObjectFingerprint(10, "one", "t2")
        before = {"stable": first, "overwritten": first, "deleted": first}
        after = {"stable": first, "overwritten": overwritten, "new": first}
        self.assertEqual(
            MODULE.changed_objects(before, after),
            {"overwritten", "deleted", "new"},
        )

    def test_structured_error_preserves_retry_contract(self):
        error = MODULE.decode_tool_error(
            {
                "status": "error",
                "code": "RestoreInProgress",
                "message": "retry",
                "retryable": True,
                "details": {"operation_id": "restore-1"},
            }
        )
        self.assertEqual(error.code, "RestoreInProgress")
        self.assertTrue(error.retryable)

    def test_native_structured_error_requires_top_level_fields(self):
        native = {
            "status": "error",
            "code": "RestoreDestinationConflict",
            "message": "destination is already claimed",
            "retryable": False,
            "details": {"destination": "/workbenches/restored"},
        }
        error = MODULE.assert_native_tool_error(
            native, "RestoreDestinationConflict"
        )
        self.assertEqual(error.details["destination"], "/workbenches/restored")
        stringified = {
            "status": "error",
            "message": (
                '{"code":"RestoreDestinationConflict",'
                '"message":"destination is already claimed",'
                '"retryable":false,"details":{}}'
            ),
        }
        with self.assertRaisesRegex(MODULE.AcceptanceError, "top-level code"):
            MODULE.assert_native_tool_error(
                stringified, "RestoreDestinationConflict"
            )

    def test_restore_manifest_requires_all_source_and_destination_identity_fields(self):
        manifest = {
            "schema": "nokv.workbench.restore_manifest.v1",
            "operation_id": "restore-1",
            "restored_from": {
                "workbench_id": "source",
                "path": "/workbenches/source",
                "snapshot_id": 7,
            },
            "source_workbench_id": "source",
            "source_path": "/workbenches/source",
            "destination_workbench_id": "restored",
            "destination_path": "/workbenches/restored",
            "snapshot_id": 7,
        }

        def validate(candidate):
            MODULE.validate_restore_manifest(
                candidate,
                operation_id="restore-1",
                source_workbench_id="source",
                source_path="/workbenches/source",
                destination_workbench_id="restored",
                destination_path="/workbenches/restored",
                snapshot_id=7,
            )

        validate(manifest)
        for field_path in (
            ("restored_from", "workbench_id"),
            ("restored_from", "path"),
            ("source_path",),
            ("destination_path",),
        ):
            with self.subTest(field_path=field_path):
                malformed = copy.deepcopy(manifest)
                target = malformed
                for component in field_path[:-1]:
                    target = target[component]
                target[field_path[-1]] = "wrong"
                with self.assertRaisesRegex(
                    MODULE.AcceptanceError, "restore manifest is malformed"
                ):
                    validate(malformed)

    def test_require_all_rejects_quick_profile(self):
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                MODULE.parse_args(["--profile", "quick", "--require-all"])

    def test_require_all_rejects_stale_no_build_mode(self):
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                MODULE.parse_args(["--profile", "full", "--require-all", "--no-build"])

    def test_restore_operation_id_matches_rust_codec_vector(self):
        operation_id = MODULE.restore_operation_id(
            1, "/agents/a/wb/source/", 123, "/agents/a/wb/dest"
        )
        self.assertEqual(
            operation_id,
            "restore-6cf0e554391351b42e765d841664687bc0c5fc99af34a84bcdef22f71c27245c",
        )
        self.assertNotEqual(
            operation_id,
            MODULE.restore_operation_id(
                2, "/agents/a/wb/source", 123, "/agents/a/wb/dest"
            ),
        )

    def test_restore_barrier_controller_owns_marker_lifecycle(self):
        with tempfile.TemporaryDirectory() as temporary:
            controller = MODULE.RestoreBarrierController(Path(temporary))
            handle = controller.handle(
                "restore-0123456789abcdef", "materialize-batch-000000"
            )
            handle.arm()
            self.assertTrue(handle.arm_path.exists())
            handle.ready_path.write_text("ready\n", encoding="utf-8")
            handle.wait_ready(0.1)
            handle.release()
            self.assertTrue(handle.continue_path.exists())
            handle.disarm_after_crash()
            self.assertFalse(handle.arm_path.exists())
            self.assertFalse(handle.ready_path.exists())
            self.assertFalse(handle.continue_path.exists())

    def test_optional_barrier_discovery_stops_when_operation_finishes(self):
        with tempfile.TemporaryDirectory() as temporary:
            controller = MODULE.RestoreBarrierController(Path(temporary))
            handle = controller.handle(
                "restore-0123456789abcdef", "reference-batch-000099"
            )
            handle.arm()
            future = concurrent.futures.Future()
            future.set_result({"status": "success"})
            self.assertFalse(handle.wait_ready_or_done(future, 0.1))
            handle.ready_path.write_text("ready\n", encoding="utf-8")
            self.assertTrue(handle.wait_ready_or_done(future, 0.1))

    def test_restore_crash_matrix_covers_cleanup_and_release(self):
        phases = MODULE.create_crash_phases(3, 4)
        self.assertEqual(
            [phase for phase in phases if phase.startswith("materialize-batch-")],
            [
                "materialize-batch-000000",
                "materialize-batch-000001",
                "materialize-batch-000002",
            ],
        )
        self.assertEqual(
            [phase for phase in phases if phase.startswith("reference-batch-")],
            [
                "reference-batch-000000",
                "reference-batch-000001",
                "reference-batch-000002",
                "reference-batch-000003",
            ],
        )
        self.assertEqual(phases[0], "hold-applied")
        self.assertEqual(phases[-1], "attach-applied")
        self.assertTrue(
            MODULE.restore_phase_requires_manifest_rebuild(
                "initialization-put-after-000000"
            )
        )
        self.assertTrue(
            MODULE.restore_phase_requires_manifest_rebuild("reference-batch-000012")
        )
        self.assertTrue(MODULE.restore_phase_requires_manifest_rebuild("index-sealed"))
        self.assertFalse(
            MODULE.restore_phase_requires_manifest_rebuild("attach-applied")
        )
        self.assertEqual(MODULE.CLEANUP_CRASH_PHASE, "cleanup-batch-000000")
        self.assertEqual(MODULE.RELEASE_CRASH_PHASE, "release-batch-000000")

    def test_restore_stats_and_fsck_validation_fail_closed(self):
        metrics = {
            "available": True,
            "active_marker": True,
            "allocator_v2_fenced": True,
            "operations": {
                "preparing": 0,
                "ready_to_attach": 0,
                "complete": 1,
                "cleaning": 0,
                "discarding": 0,
                "releasing": 0,
            },
            "staging_rows": 8,
            "exact_reference_rows": 9,
            "index_rows": 10,
            "cleanup_backlog": 0,
            "release_backlog": 0,
            "quarantine_rows": 0,
            "control_rows": {"operation": 1},
        }
        stats = {"restore": metrics}
        MODULE.validate_restore_metrics(stats, expected_complete=1, expect_empty=False)
        report = {
            "consistent": True,
            "dangling_count": 0,
            "dangling": [],
            "size_mismatch_count": 0,
            "size_mismatches": [],
            "snapshot_pins_scanned": 0,
            "fork_bindings_scanned": 0,
            "restore_shards": [
                {
                    "mount_id": 1,
                    "report": {
                        "consistent": True,
                        "metrics": {
                            key: value
                            for key, value in metrics.items()
                            if key != "available"
                        },
                        "borrowed_objects_checked": 3,
                        "dangling_borrowed_objects": [],
                        "borrowed_object_size_mismatches": [],
                        "issues": [],
                    },
                }
            ],
        }
        MODULE.validate_fsck_report(
            report,
            expected_complete=1,
            expected_snapshot_pins=0,
            expected_fork_bindings=0,
        )
        report["restore_shards"][0]["report"]["issues"] = [{"code": "bad"}]
        with self.assertRaisesRegex(MODULE.AcceptanceError, "issues"):
            MODULE.validate_fsck_report(
                report,
                expected_complete=1,
                expected_snapshot_pins=0,
                expected_fork_bindings=0,
            )

    def test_live_fsck_retries_only_the_declared_metadata_race(self):
        environment = object.__new__(MODULE.LiveEnvironment)
        environment.config = type(
            "Config",
            (),
            {"command_timeout": 1.0, "tool_timeout": 1.0, "gc_deadline": 1.0},
        )()
        responses = iter(
            [
                MODULE.AcceptanceError(
                    "NoKV GET /fsck returned HTTP 500: "
                    + MODULE.RETRYABLE_FSCK_CONFLICT
                ),
                '{"consistent":true}',
            ]
        )

        def retryable_http_text(*_args, **_kwargs):
            response = next(responses)
            if isinstance(response, Exception):
                raise response
            return response

        environment.http_text = retryable_http_text
        self.assertEqual(environment.fsck(), {"consistent": True})

        def terminal_http_text(*_args, **_kwargs):
            raise MODULE.AcceptanceError(
                "NoKV GET /fsck returned HTTP 500: durable corruption"
            )

        environment.http_text = terminal_http_text
        with self.assertRaisesRegex(MODULE.AcceptanceError, "durable corruption"):
            environment.fsck()

    def test_release_terminal_wait_drives_bounded_gc_pages(self):
        suite = object.__new__(MODULE.AcceptanceSuite)
        suite.source = "source"
        suite.snapshot_id = 7
        suite.env = type(
            "Environment",
            (),
            {
                "config": type("Config", (), {"tool_timeout": 1.0})(),
            },
        )()
        suite.env.gc_limits = []
        release_backlogs = iter([1, 0])

        def manual_gc(*, limit):
            suite.env.gc_limits.append(limit)
            return {
                "object_gc": {
                    "restore_release_jobs_processed": 1,
                    "restore_release_backlog": next(release_backlogs),
                    "restore_release_quarantine": 0,
                    "restore_release_mount_wide_quarantine": 0,
                }
            }

        suite.env.manual_gc = manual_gc
        outcomes = iter(
            [
                {
                    "status": "error",
                    "code": "RestoreInProgress",
                    "message": "release is pending",
                    "retryable": True,
                    "details": {},
                },
                {
                    "status": "error",
                    "code": "RestoreInProgress",
                    "message": "release is pending",
                    "retryable": True,
                    "details": {},
                },
                {
                    "status": "error",
                    "code": "RestoreDestinationConflict",
                    "message": "destination now belongs to another workbench",
                    "retryable": False,
                    "details": {},
                },
            ]
        )
        suite.clients = [
            type(
                "Client",
                (),
                {"raw_call": staticmethod(lambda _name, _arguments: next(outcomes))},
            )()
        ]

        error, attempts = suite.wait_for_restore_error(
            "destination", "RestoreDestinationConflict"
        )

        self.assertEqual(error.code, "RestoreDestinationConflict")
        self.assertEqual(attempts, 3)
        self.assertEqual(suite.env.gc_limits, [64, 64])

    def test_release_terminal_wait_fails_closed_on_quarantine(self):
        suite = object.__new__(MODULE.AcceptanceSuite)
        suite.source = "source"
        suite.snapshot_id = 7
        suite.env = type(
            "Environment",
            (),
            {
                "config": type("Config", (), {"tool_timeout": 1.0})(),
            },
        )()
        suite.env.manual_gc = lambda *, limit: {
            "object_gc": {
                "restore_release_jobs_processed": 1,
                "restore_release_backlog": 1,
                "restore_release_quarantine": 1,
                "restore_release_mount_wide_quarantine": 0,
            }
        }
        suite.clients = [
            type(
                "Client",
                (),
                {
                    "raw_call": staticmethod(
                        lambda _name, _arguments: {
                            "status": "error",
                            "code": "RestoreInProgress",
                            "message": "release is pending",
                            "retryable": True,
                            "details": {},
                        }
                    )
                },
            )()
        ]

        with self.assertRaisesRegex(MODULE.AcceptanceError, "quarantined"):
            suite.wait_for_restore_error(
                "destination", "RestoreDestinationConflict"
            )

    def test_released_restore_metrics_allow_only_durable_ledger_rows(self):
        metrics = {
            "active_marker": True,
            "allocator_v2_fenced": True,
            "operations": {
                "preparing": 0,
                "ready_to_attach": 0,
                "complete": 0,
                "cleaning": 0,
                "discarding": 0,
                "releasing": 0,
            },
            "staging_rows": 0,
            "exact_reference_rows": 0,
            "index_rows": 0,
            "cleanup_backlog": 0,
            "release_backlog": 0,
            "quarantine_rows": 0,
            "control_rows": {
                "operation": 0,
                "init_upload_tombstone": 2,
                "init_upload_tombstone_cursor": 1,
                "release_cursor": 1,
            },
        }
        MODULE.validate_restore_metrics_object(
            metrics, expected_complete=0, expect_empty=True
        )
        self.assertTrue(MODULE.restore_release_graph_drained(metrics))
        releasing = copy.deepcopy(metrics)
        releasing["operations"]["releasing"] = 1
        releasing["control_rows"]["operation"] = 1
        releasing["release_backlog"] = 1
        self.assertFalse(MODULE.restore_release_graph_drained(releasing))
        for leaked_row in ("base_owner", "release_job"):
            with self.subTest(leaked_row=leaked_row):
                leaked = {
                    **metrics,
                    "control_rows": {
                        **metrics["control_rows"],
                        leaked_row: 1,
                    },
                }
                with self.assertRaisesRegex(MODULE.AcceptanceError, "did not drain"):
                    MODULE.validate_restore_metrics_object(
                        leaked, expected_complete=0, expect_empty=True
                    )

    def test_counting_proxy_forwards_and_counts_successful_put(self):
        class BackendHandler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, _format, *_args):
                return

            def do_PUT(self):
                length = int(self.headers["Content-Length"])
                self.server.payload = self.rfile.read(length)
                self.send_response(200)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def do_DELETE(self):
                self.send_response(204)
                self.send_header("Content-Length", "0")
                self.end_headers()

        backend = http.server.ThreadingHTTPServer(
            ("127.0.0.1", MODULE.free_port()), BackendHandler
        )
        proxy = MODULE.CountingProxyServer(
            ("127.0.0.1", MODULE.free_port()), backend.server_port
        )
        backend_thread = threading.Thread(target=backend.serve_forever, daemon=True)
        proxy_thread = threading.Thread(target=proxy.serve_forever, daemon=True)
        backend_thread.start()
        proxy_thread.start()
        try:
            connection = http.client.HTTPConnection(
                "127.0.0.1", proxy.server_port, timeout=5
            )
            connection.request("PUT", "/bucket/object", body=b"payload")
            response = connection.getresponse()
            response.read()
            connection.close()
            self.assertEqual(response.status, 200)
            self.assertEqual(backend.payload, b"payload")
            self.assertEqual(proxy.put_records(), ["/bucket/object"])
            connection = http.client.HTTPConnection(
                "127.0.0.1", proxy.server_port, timeout=5
            )
            connection.request("DELETE", "/bucket/object")
            delete_response = connection.getresponse()
            delete_response.read()
            connection.close()
            self.assertEqual(delete_response.status, 204)
            self.assertEqual(
                proxy.mutation_records(),
                [
                    MODULE.ObjectMutation("PUT", "/bucket/object"),
                    MODULE.ObjectMutation("DELETE", "/bucket/object"),
                ],
            )
        finally:
            proxy.shutdown()
            backend.shutdown()
            proxy.server_close()
            backend.server_close()
            proxy_thread.join(timeout=5)
            backend_thread.join(timeout=5)


if __name__ == "__main__":
    unittest.main()
