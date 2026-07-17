#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import contextlib
import io
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

import managed_nokv_server as managed  # noqa: E402


PID = 4242
SERVER_BIND = "127.0.0.1:7799"
START_IDENTITY = "Fri Jul 17 10:00:00 2026"


class ManagedNokvServerTest(unittest.TestCase):
    def make_binary(self, root: Path, content: bytes = b"nokv-v1") -> Path:
        binary = root / "runtime" / "nokv"
        binary.parent.mkdir(parents=True)
        binary.write_bytes(content)
        os.chmod(binary, 0o755)
        return binary.resolve()

    def launch_argv(self, binary: Path, meta: Path) -> list[str]:
        return [
            str(binary),
            "--server-bind",
            SERVER_BIND,
            "--object-backend",
            "rustfs",
            "--s3-endpoint",
            "http://127.0.0.1:9000",
            "--s3-bucket",
            "nokv-lingtai-workbench",
            "--meta",
            str(meta),
            "serve",
        ]

    def observation(self, binary: Path, meta: Path) -> managed.ProcessObservation:
        return managed.ProcessObservation(
            START_IDENTITY,
            " ".join(self.launch_argv(binary, meta)),
        )

    def record(self, root: Path) -> tuple[Path, Path, Path, managed.ManagedServerState]:
        binary = self.make_binary(root)
        meta = (root / "meta").resolve()
        meta.mkdir()
        state_path = root / "managed-server.json"
        observation = self.observation(binary, meta)
        with (
            mock.patch.object(managed, "capture_process", return_value=observation),
            mock.patch.object(managed, "listener_pids", return_value={PID}),
            mock.patch.object(
                managed,
                "read_process_argv",
                return_value=self.launch_argv(binary, meta),
            ),
        ):
            state, changed = managed.record_server_state(
                state_path,
                pid=PID,
                binary=binary,
                argv=self.launch_argv(binary, meta),
                server_bind=SERVER_BIND,
                meta=meta,
                object_backend="rustfs",
                s3_endpoint="http://127.0.0.1:9000",
                s3_bucket="nokv-lingtai-workbench",
            )
        self.assertTrue(changed)
        return state_path, binary, meta, state

    def verify_with(
        self,
        state_path: Path,
        observation: managed.ProcessObservation,
        *,
        listeners: set[int] | None = None,
        expectation: managed.LaunchExpectation | None = None,
    ) -> managed.ManagedServerState:
        if listeners is None:
            listeners = {PID}
        recorded_argv = managed.load_state(state_path).argv
        with (
            mock.patch.object(managed, "capture_process", return_value=observation),
            mock.patch.object(managed, "listener_pids", return_value=listeners),
            mock.patch.object(managed, "read_process_argv", return_value=recorded_argv),
        ):
            return managed.verify_server_state(state_path, expectation)

    def test_write_is_atomic_and_idempotent(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            state_path, binary, meta, first_state = self.record(root)
            inode_before = state_path.stat().st_ino
            bytes_before = state_path.read_bytes()
            observation = self.observation(binary, meta)

            with (
                mock.patch.object(managed, "capture_process", return_value=observation),
                mock.patch.object(managed, "listener_pids", return_value={PID}),
                mock.patch.object(
                    managed,
                    "read_process_argv",
                    return_value=self.launch_argv(binary, meta),
                ),
                mock.patch.object(
                    managed.os, "replace", wraps=managed.os.replace
                ) as replace,
            ):
                second_state, changed = managed.record_server_state(
                    state_path,
                    pid=PID,
                    binary=binary,
                    argv=self.launch_argv(binary, meta),
                    server_bind=SERVER_BIND,
                    meta=meta,
                    object_backend="rustfs",
                    s3_endpoint="http://127.0.0.1:9000",
                    s3_bucket="nokv-lingtai-workbench",
                )

            self.assertFalse(changed)
            self.assertEqual(second_state, first_state)
            self.assertEqual(state_path.stat().st_ino, inode_before)
            self.assertEqual(state_path.read_bytes(), bytes_before)
            replace.assert_not_called()
            self.assertEqual(set(json.loads(bytes_before)), managed.STATE_FIELDS)

    def test_write_fsyncs_the_parent_directory(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            real_fsync_directory = managed._fsync_directory
            with mock.patch.object(
                managed,
                "_fsync_directory",
                wraps=real_fsync_directory,
            ) as fsync_directory:
                state_path, _, _, _ = self.record(root)

            fsync_directory.assert_called_once_with(state_path.parent)

    def test_launch_persists_full_state_before_exec_and_cleans_up_exec_failure(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            binary = self.make_binary(root)
            meta = (root / "meta").resolve()
            meta.mkdir()
            state_path = root / "managed-server.json"
            argv = self.launch_argv(binary, meta)
            observed_states: list[managed.ManagedServerState] = []

            def observe_then_fail(exec_binary: str, exec_argv: list[str]):
                self.assertEqual(exec_binary, str(binary))
                self.assertEqual(exec_argv, argv)
                observed_states.append(managed.load_state(state_path))
                raise OSError("injected exec failure")

            with (
                mock.patch.object(managed.os, "getpid", return_value=PID),
                mock.patch.object(
                    managed, "_ps_field", return_value=START_IDENTITY
                ) as ps_field,
                mock.patch.object(
                    managed.os, "execv", side_effect=observe_then_fail
                ) as execv,
                self.assertRaisesRegex(OSError, "injected exec failure"),
            ):
                managed.launch_server(
                    state_path,
                    binary=binary,
                    argv=argv,
                    server_bind=SERVER_BIND,
                    meta=meta,
                    object_backend="rustfs",
                    s3_endpoint="http://127.0.0.1:9000",
                    s3_bucket="nokv-lingtai-workbench",
                )

            self.assertEqual(len(observed_states), 1)
            state = observed_states[0]
            self.assertEqual(state.pid, PID)
            self.assertEqual(state.process_start_identity, START_IDENTITY)
            self.assertEqual(state.process_command, " ".join(argv))
            self.assertEqual(state.argv, argv)
            self.assertEqual(state.binary, str(binary))
            self.assertFalse(state_path.exists())
            self.assertEqual(ps_field.call_count, 2)
            execv.assert_called_once_with(str(binary), argv)

    def test_launch_never_overwrites_existing_managed_state(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            state_path, binary, meta, original = self.record(root)
            with (
                mock.patch.object(managed.os, "getpid", return_value=9999),
                mock.patch.object(managed, "_ps_field", return_value=START_IDENTITY),
                mock.patch.object(managed.os, "execv") as execv,
                self.assertRaisesRegex(
                    managed.ManagedServerError, "state already exists"
                ),
            ):
                managed.launch_server(
                    state_path,
                    binary=binary,
                    argv=self.launch_argv(binary, meta),
                    server_bind=SERVER_BIND,
                    meta=meta,
                    object_backend="rustfs",
                    s3_endpoint="http://127.0.0.1:9000",
                    s3_bucket="nokv-lingtai-workbench",
                )

            self.assertEqual(managed.load_state(state_path), original)
            execv.assert_not_called()

    def test_write_rejects_a_different_running_process_argv(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            binary = self.make_binary(root)
            meta = (root / "meta").resolve()
            meta.mkdir()
            observation = self.observation(binary, meta)
            different_argv = [str(binary), "--server-bind", SERVER_BIND, "serve"]

            with (
                mock.patch.object(managed, "capture_process", return_value=observation),
                mock.patch.object(managed, "listener_pids", return_value={PID}),
                mock.patch.object(
                    managed, "read_process_argv", return_value=different_argv
                ),
                self.assertRaisesRegex(
                    managed.ManagedServerError, "running process argv differs"
                ),
            ):
                managed.record_server_state(
                    root / "state.json",
                    pid=PID,
                    binary=binary,
                    argv=self.launch_argv(binary, meta),
                    server_bind=SERVER_BIND,
                    meta=meta,
                    object_backend="rustfs",
                    s3_endpoint="http://127.0.0.1:9000",
                    s3_bucket="nokv-lingtai-workbench",
                )

    def test_ps_fallback_is_exact_and_fails_closed_for_whitespace(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            binary = self.make_binary(root)
            meta = (root / "meta").resolve()
            argv = self.launch_argv(binary, meta)
            observation = managed.ProcessObservation(START_IDENTITY, " ".join(argv))
            with mock.patch.object(managed, "read_process_argv", return_value=None):
                managed.ensure_recorded_launch_matches_process(
                    PID, binary, argv, observation
                )
                ambiguous = [*argv[:-1], "value with spaces", "serve"]
                with self.assertRaisesRegex(
                    managed.ManagedServerError, "cannot prove exact process argv"
                ):
                    managed.ensure_recorded_launch_matches_process(
                        PID,
                        binary,
                        ambiguous,
                        managed.ProcessObservation(START_IDENTITY, " ".join(ambiguous)),
                    )

    def test_verify_rejects_pid_reuse(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            state_path, binary, meta, _ = self.record(root)
            reused = managed.ProcessObservation(
                "Fri Jul 17 11:00:00 2026",
                self.observation(binary, meta).command,
            )

            with self.assertRaisesRegex(
                managed.ManagedServerError, "PID may have been reused"
            ):
                self.verify_with(state_path, reused)

    def test_verify_rejects_launch_drift(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            state_path, binary, meta, _ = self.record(root)
            drifted = managed.ProcessObservation(
                START_IDENTITY,
                self.observation(binary, meta).command + " --unexpected",
            )

            with self.assertRaisesRegex(
                managed.ManagedServerError, "refusing launch drift"
            ):
                self.verify_with(state_path, drifted)

    def test_verify_rejects_listener_mismatch(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            state_path, binary, meta, _ = self.record(root)

            with self.assertRaisesRegex(
                managed.ManagedServerError, "listener PID mismatch"
            ):
                self.verify_with(
                    state_path,
                    self.observation(binary, meta),
                    listeners={9999},
                )

    def test_verify_rejects_binary_replacement(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            state_path, binary, meta, _ = self.record(root)
            binary.write_bytes(b"nokv-v2")
            os.chmod(binary, 0o755)

            with self.assertRaisesRegex(
                managed.ManagedServerError, "binary was replaced in place"
            ):
                self.verify_with(state_path, self.observation(binary, meta))

    def test_verify_expected_launch_is_an_exact_reuse_gate(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            state_path, binary, meta, state = self.record(root)
            matching = managed.LaunchExpectation(
                argv=state.argv,
                binary=state.binary,
                server_bind=state.server_bind,
                meta=state.meta,
                object_backend=state.object_backend,
                s3_endpoint=state.s3_endpoint,
                s3_bucket=state.s3_bucket,
            )
            verified = self.verify_with(
                state_path,
                self.observation(binary, meta),
                expectation=matching,
            )
            self.assertEqual(verified, state)

            drifted = managed.LaunchExpectation(
                **{**matching.__dict__, "s3_bucket": "different-bucket"}
            )
            with self.assertRaisesRegex(
                managed.ManagedServerError, "differing=\\['s3_bucket'\\]"
            ):
                self.verify_with(
                    state_path,
                    self.observation(binary, meta),
                    expectation=drifted,
                )

    def test_terminate_uses_pidfd_to_anchor_verify_signal_and_wait(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            state_path, _, _, state = self.record(root)
            events: list[str] = []

            def verify(path: Path):
                self.assertEqual(path, state_path)
                events.append("verify")
                return state

            def send_signal(pidfd: int, signal_number: int, info, flags: int):
                self.assertEqual((pidfd, signal_number, info, flags), (77, 15, None, 0))
                events.append("signal")

            with (
                mock.patch.object(managed.sys, "platform", "linux"),
                mock.patch.object(
                    managed.os, "pidfd_open", return_value=77, create=True
                ) as pidfd_open,
                mock.patch.object(
                    managed.signal,
                    "pidfd_send_signal",
                    side_effect=send_signal,
                    create=True,
                ) as pidfd_send_signal,
                mock.patch.object(
                    managed, "verify_server_state", side_effect=verify
                ) as verify_state,
                mock.patch.object(
                    managed,
                    "_wait_for_pidfd_exit",
                    side_effect=lambda pidfd, timeout: events.append("wait") or True,
                ) as wait_for_exit,
                mock.patch.object(managed.os, "close") as close,
            ):
                terminated = managed.terminate_server(state_path, 5.0)

            self.assertEqual(terminated, state)
            self.assertEqual(events, ["verify", "signal", "wait"])
            self.assertFalse(state_path.exists())
            pidfd_open.assert_called_once_with(PID, 0)
            pidfd_send_signal.assert_called_once()
            verify_state.assert_called_once_with(state_path)
            wait_for_exit.assert_called_once_with(77, 5.0)
            self.assertEqual(close.call_args_list[0], mock.call(77))

    def test_terminate_fallback_revalidates_immediately_before_signal(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            state_path, binary, meta, state = self.record(root)
            observation = self.observation(binary, meta)
            events: list[str] = []

            with (
                mock.patch.object(managed, "_try_open_pidfd", return_value=None),
                mock.patch.object(
                    managed,
                    "verify_server_state",
                    side_effect=lambda path: events.append("verify") or state,
                ),
                mock.patch.object(
                    managed,
                    "capture_process",
                    side_effect=lambda pid: events.append("identity") or observation,
                ),
                mock.patch.object(
                    managed,
                    "ensure_recorded_launch_matches_process",
                    side_effect=lambda *args: events.append("argv"),
                ),
                mock.patch.object(
                    managed,
                    "ensure_listener_owner",
                    side_effect=lambda *args: events.append("listener"),
                ),
                mock.patch.object(
                    managed.os,
                    "kill",
                    side_effect=lambda *args: events.append("signal"),
                ) as kill,
                mock.patch.object(
                    managed,
                    "_wait_for_matching_process_exit",
                    side_effect=lambda *args: events.append("wait") or True,
                ),
            ):
                terminated = managed.terminate_server(state_path, 5.0)

            self.assertEqual(terminated, state)
            self.assertEqual(
                events,
                ["verify", "identity", "argv", "listener", "signal", "wait"],
            )
            kill.assert_called_once_with(PID, managed.signal.SIGTERM)
            self.assertFalse(state_path.exists())

    def test_terminate_timeout_retains_state_for_retry(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            state_path, _, _, state = self.record(root)
            with (
                mock.patch.object(managed, "_try_open_pidfd", return_value=77),
                mock.patch.object(managed, "verify_server_state", return_value=state),
                mock.patch.object(managed.signal, "pidfd_send_signal", create=True),
                mock.patch.object(managed, "_wait_for_pidfd_exit", return_value=False),
                mock.patch.object(managed.os, "close"),
                self.assertRaisesRegex(
                    managed.ManagedServerError, "did not stop within 0.25s"
                ),
            ):
                managed.terminate_server(state_path, 0.25)

            self.assertEqual(managed.load_state(state_path), state)

    def test_pid_mode_validates_schema_and_refuses_state_symlink(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            state_path, _, _, state = self.record(root)
            stdout = io.StringIO()
            stderr = io.StringIO()
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                code = managed.main(["pid", "--state", str(state_path)])
            self.assertEqual(code, 0, stderr.getvalue())
            self.assertEqual(stdout.getvalue(), f"{PID}\n")

            symlink = root / "state-link.json"
            symlink.symlink_to(state_path)
            stdout = io.StringIO()
            stderr = io.StringIO()
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                code = managed.main(["pid", "--state", str(symlink)])
            self.assertEqual(code, 1)
            self.assertEqual(stdout.getvalue(), "")
            self.assertIn("must not be a symlink", stderr.getvalue())
            with self.assertRaisesRegex(
                managed.ManagedServerError, "must not be a symlink"
            ):
                managed.write_state(symlink, state)

            invalid = root / "invalid-state.json"
            mapping = state.as_dict()
            mapping["schema"] = "wrong.schema"
            invalid.write_text(json.dumps(mapping), encoding="utf-8")
            stdout = io.StringIO()
            stderr = io.StringIO()
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                code = managed.main(["pid", "--state", str(invalid)])
            self.assertEqual(code, 1)
            self.assertEqual(stdout.getvalue(), "")
            self.assertIn("state schema must be", stderr.getvalue())

    def test_write_cli_accepts_full_argv_after_separator(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            binary = self.make_binary(root)
            meta = (root / "meta").resolve()
            argv = self.launch_argv(binary, meta)
            args = managed.parse_args(
                [
                    "write",
                    "--state",
                    str(root / "state.json"),
                    "--pid",
                    str(PID),
                    "--binary",
                    str(binary),
                    "--server-bind",
                    SERVER_BIND,
                    "--meta",
                    str(meta),
                    "--object-backend",
                    "rustfs",
                    "--s3-endpoint",
                    "http://127.0.0.1:9000",
                    "--s3-bucket",
                    "nokv-lingtai-workbench",
                    "--",
                    *argv,
                ]
            )
            self.assertEqual(args.launch_argv, argv)

            launch_args = managed.parse_args(
                [
                    "launch",
                    "--state",
                    str(root / "launch-state.json"),
                    "--binary",
                    str(binary),
                    "--server-bind",
                    SERVER_BIND,
                    "--meta",
                    str(meta),
                    "--object-backend",
                    "rustfs",
                    "--s3-endpoint",
                    "http://127.0.0.1:9000",
                    "--s3-bucket",
                    "nokv-lingtai-workbench",
                    "--",
                    *argv,
                ]
            )
            self.assertEqual(launch_args.launch_argv, argv)

            terminate_args = managed.parse_args(
                [
                    "terminate",
                    "--state",
                    str(root / "launch-state.json"),
                    "--timeout-seconds",
                    "5",
                ]
            )
            self.assertEqual(terminate_args.timeout_seconds, 5.0)


if __name__ == "__main__":
    unittest.main()
