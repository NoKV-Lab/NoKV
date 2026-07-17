#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Record and verify the identity of a helper-managed NoKV server."""

from __future__ import annotations

import argparse
import errno
import json
import math
import os
import select
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from nokv_runtime import sha256_file, validate_sha256


STATE_SCHEMA = "nokv.lingtai.managed_server.v1"
STATE_FIELDS = {
    "schema",
    "pid",
    "process_start_identity",
    "process_command",
    "argv",
    "binary",
    "binary_sha256",
    "server_bind",
    "meta",
    "object_backend",
    "s3_endpoint",
    "s3_bucket",
}


class ManagedServerError(RuntimeError):
    """A managed server state cannot be trusted or reused."""


@dataclass(frozen=True)
class ProcessObservation:
    start_identity: str
    command: str


@dataclass(frozen=True)
class ManagedServerState:
    schema: str
    pid: int
    process_start_identity: str
    process_command: str
    argv: list[str]
    binary: str
    binary_sha256: str
    server_bind: str
    meta: str
    object_backend: str
    s3_endpoint: str
    s3_bucket: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class LaunchExpectation:
    argv: list[str]
    binary: str
    server_bind: str
    meta: str
    object_backend: str
    s3_endpoint: str
    s3_bucket: str


def absolute_path(path: Path) -> Path:
    expanded = path.expanduser()
    if expanded.is_absolute():
        return expanded
    return Path.cwd() / expanded


def canonical_binary(path: Path) -> Path:
    try:
        resolved = path.expanduser().resolve(strict=True)
    except FileNotFoundError as err:
        raise ManagedServerError(f"NoKV binary does not exist: {path}") from err
    if not resolved.is_file():
        raise ManagedServerError(f"NoKV binary is not a regular file: {resolved}")
    if not os.access(resolved, os.X_OK):
        raise ManagedServerError(f"NoKV binary is not executable: {resolved}")
    return resolved


def canonical_meta(path: Path) -> Path:
    return path.expanduser().resolve()


def validate_server_bind(value: str) -> str:
    host, separator, port_text = value.rpartition(":")
    if not separator or not host or not port_text.isdigit():
        raise ManagedServerError(f"server bind must be HOST:PORT, got {value!r}")
    port = int(port_text)
    if port < 1 or port > 65535:
        raise ManagedServerError(f"server bind port is out of range: {port}")
    return value


def _required_string(data: dict[str, Any], field: str) -> str:
    value = data.get(field)
    if not isinstance(value, str) or not value:
        raise ManagedServerError(f"managed server state {field} must be non-empty")
    return value


def state_from_mapping(data: Any) -> ManagedServerState:
    if not isinstance(data, dict):
        raise ManagedServerError("managed server state must be a JSON object")
    actual_fields = set(data)
    if actual_fields != STATE_FIELDS:
        raise ManagedServerError(
            "managed server state fields differ from the v1 schema; "
            f"missing={sorted(STATE_FIELDS - actual_fields)}, "
            f"extra={sorted(actual_fields - STATE_FIELDS)}"
        )
    if data.get("schema") != STATE_SCHEMA:
        raise ManagedServerError(f"managed server state schema must be {STATE_SCHEMA}")
    pid = data.get("pid")
    if not isinstance(pid, int) or isinstance(pid, bool) or pid < 1:
        raise ManagedServerError("managed server state pid must be a positive integer")
    argv = data.get("argv")
    if (
        not isinstance(argv, list)
        or not argv
        or any(not isinstance(argument, str) for argument in argv)
    ):
        raise ManagedServerError(
            "managed server state argv must be a non-empty string array"
        )
    if argv[-1] != "serve":
        raise ManagedServerError(
            "managed server state argv must describe a NoKV serve launch"
        )
    binary = _required_string(data, "binary")
    if not Path(binary).is_absolute():
        raise ManagedServerError("managed server state binary must be absolute")
    if not Path(argv[0]).is_absolute():
        raise ManagedServerError("managed server state argv[0] must be absolute")
    if Path(argv[0]).expanduser().resolve() != Path(binary):
        raise ManagedServerError(
            "managed server state argv[0] does not resolve to its binary"
        )
    meta = _required_string(data, "meta")
    if not Path(meta).is_absolute():
        raise ManagedServerError("managed server state meta must be absolute")
    digest = _required_string(data, "binary_sha256")
    try:
        validate_sha256(digest)
    except ValueError as err:
        raise ManagedServerError(
            "managed server state binary_sha256 is invalid"
        ) from err
    state = ManagedServerState(
        schema=STATE_SCHEMA,
        pid=pid,
        process_start_identity=_required_string(data, "process_start_identity"),
        process_command=_required_string(data, "process_command"),
        argv=list(argv),
        binary=binary,
        binary_sha256=digest,
        server_bind=validate_server_bind(_required_string(data, "server_bind")),
        meta=meta,
        object_backend=_required_string(data, "object_backend"),
        s3_endpoint=_required_string(data, "s3_endpoint"),
        s3_bucket=_required_string(data, "s3_bucket"),
    )
    return state


def _state_text(state: ManagedServerState) -> str:
    return (
        json.dumps(
            state.as_dict(),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_DIRECTORY", 0)
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _read_regular_file(path: Path) -> str:
    path = absolute_path(path)
    if path.is_symlink():
        raise ManagedServerError(f"managed server state must not be a symlink: {path}")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags | nofollow)
    except FileNotFoundError as err:
        raise ManagedServerError(
            f"managed server state does not exist: {path}"
        ) from err
    except OSError as err:
        if err.errno == errno.ELOOP:
            raise ManagedServerError(
                f"managed server state must not be a symlink: {path}"
            ) from err
        raise ManagedServerError(
            f"cannot open managed server state {path}: {err}"
        ) from err
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ManagedServerError(
                f"managed server state is not a regular file: {path}"
            )
        with os.fdopen(descriptor, encoding="utf-8") as handle:
            descriptor = -1
            return handle.read()
    except UnicodeDecodeError as err:
        raise ManagedServerError(
            f"managed server state is not valid UTF-8: {path}"
        ) from err
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def load_state(path: Path) -> ManagedServerState:
    text = _read_regular_file(path)
    try:
        data = json.loads(text)
    except json.JSONDecodeError as err:
        raise ManagedServerError(
            f"managed server state contains invalid JSON: {path}: {err}"
        ) from err
    return state_from_mapping(data)


def write_state(
    path: Path,
    state: ManagedServerState,
    *,
    require_absent: bool = False,
) -> bool:
    state = state_from_mapping(state.as_dict())
    path = absolute_path(path)
    if path.is_symlink():
        raise ManagedServerError(f"managed server state must not be a symlink: {path}")
    text = _state_text(state)
    if require_absent and (path.exists() or path.is_symlink()):
        raise ManagedServerError(f"managed server state already exists: {path}")
    if path.exists():
        if load_state(path) == state and _read_regular_file(path) == text:
            return False
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", dir=str(path.parent)
    )
    try:
        os.fchmod(descriptor, stat.S_IRUSR | stat.S_IWUSR)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            descriptor = -1
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        if require_absent:
            try:
                os.link(temporary_name, path, follow_symlinks=False)
            except FileExistsError as err:
                raise ManagedServerError(
                    f"managed server state appeared before launch: {path}"
                ) from err
            os.unlink(temporary_name)
            temporary_name = ""
        else:
            if path.is_symlink():
                raise ManagedServerError(
                    f"managed server state became a symlink before replace: {path}"
                )
            os.replace(temporary_name, path)
            temporary_name = ""
        _fsync_directory(path.parent)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if os.path.exists(temporary_name):
            os.unlink(temporary_name)
    return True


def remove_state_if_matches(path: Path, expected: ManagedServerState) -> None:
    path = absolute_path(path)
    current = load_state(path)
    if current != expected:
        raise ManagedServerError(f"managed server state changed before cleanup: {path}")
    path.unlink()
    _fsync_directory(path.parent)


def ensure_process_alive(pid: int) -> None:
    try:
        os.kill(pid, 0)
    except ProcessLookupError as err:
        raise ManagedServerError(f"managed NoKV server pid {pid} is not alive") from err
    except PermissionError:
        return
    except OSError as err:
        raise ManagedServerError(
            f"cannot inspect managed NoKV server pid {pid}: {err}"
        ) from err


def _ps_field(pid: int, field: str) -> str:
    ps = shutil.which("ps")
    if ps is None:
        raise ManagedServerError("ps is required to inspect the managed server")
    environment = os.environ.copy()
    environment["LC_ALL"] = "C"
    completed = subprocess.run(
        [ps, "-ww", "-p", str(pid), "-o", f"{field}="],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
        env=environment,
    )
    value = completed.stdout.strip()
    if completed.returncode != 0 or not value:
        detail = completed.stderr.strip() or value or "process not found"
        raise ManagedServerError(f"cannot read ps {field} for pid {pid}: {detail}")
    if "\n" in value:
        raise ManagedServerError(f"ps returned multiple {field} rows for pid {pid}")
    return value


def capture_process(pid: int) -> ProcessObservation:
    ensure_process_alive(pid)
    observation = ProcessObservation(
        start_identity=_ps_field(pid, "lstart"),
        command=_ps_field(pid, "command"),
    )
    ensure_process_alive(pid)
    return observation


def read_process_argv(pid: int) -> list[str] | None:
    """Read exact argv where the platform exposes a stable process interface."""
    if not sys.platform.startswith("linux"):
        return None
    path = Path("/proc") / str(pid) / "cmdline"
    try:
        encoded = path.read_bytes()
    except OSError as err:
        raise ManagedServerError(
            f"cannot read exact argv for managed NoKV server pid {pid}: {err}"
        ) from err
    if not encoded or not encoded.endswith(b"\0"):
        raise ManagedServerError(f"process argv for pid {pid} is empty or incomplete")
    return [os.fsdecode(argument) for argument in encoded[:-1].split(b"\0")]


def ensure_recorded_launch_matches_process(
    pid: int,
    binary: Path,
    argv: list[str],
    observation: ProcessObservation,
) -> None:
    if not argv or argv[-1] != "serve":
        raise ManagedServerError("full launch argv must end with the serve command")
    if Path(argv[0]).expanduser().resolve() != binary:
        raise ManagedServerError("launch argv[0] does not resolve to --binary")
    actual_argv = read_process_argv(pid)
    if actual_argv is not None:
        if actual_argv != argv:
            raise ManagedServerError(
                "running process argv differs from the NoKV serve launch being recorded"
            )
        return

    # macOS ps exposes the complete command text but not an unambiguous argv
    # vector. Whitespace-free arguments have one safe representation, so fail
    # closed for paths or values that cannot be proved without guessing.
    if any(
        not argument or any(character.isspace() for character in argument)
        for argument in argv
    ):
        raise ManagedServerError(
            "cannot prove exact process argv on this platform when an argument "
            "is empty or contains whitespace"
        )
    expected_command = " ".join(argv)
    if observation.command != expected_command:
        raise ManagedServerError(
            "running process command differs from the NoKV serve launch being recorded"
        )


def listener_pids(server_bind: str) -> set[int]:
    validate_server_bind(server_bind)
    lsof = shutil.which("lsof")
    if lsof is None:
        raise ManagedServerError("lsof is required to verify the managed listener")
    completed = subprocess.run(
        [
            lsof,
            "-nP",
            "-t",
            f"-iTCP@{server_bind}",
            "-sTCP:LISTEN",
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    if completed.returncode not in (0, 1):
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise ManagedServerError(f"cannot inspect listener at {server_bind}: {detail}")
    result: set[int] = set()
    for line in completed.stdout.splitlines():
        value = line.strip()
        if not value:
            continue
        if not value.isdigit() or int(value) < 1:
            raise ManagedServerError(
                f"lsof returned an invalid listener pid at {server_bind}: {value!r}"
            )
        result.add(int(value))
    return result


def ensure_listener_owner(server_bind: str, pid: int) -> None:
    actual = listener_pids(server_bind)
    if actual != {pid}:
        raise ManagedServerError(
            f"listener PID mismatch for {server_bind}: "
            f"expected {pid}, got {sorted(actual)}"
        )


def _ensure_same_process(
    state: ManagedServerState, observation: ProcessObservation
) -> None:
    if observation.start_identity != state.process_start_identity:
        raise ManagedServerError(
            f"process launch identity changed for pid {state.pid}; "
            "the PID may have been reused"
        )
    if observation.command != state.process_command:
        raise ManagedServerError(
            f"process command changed for pid {state.pid}; refusing launch drift"
        )


def record_server_state(
    path: Path,
    *,
    pid: int,
    binary: Path,
    argv: list[str],
    server_bind: str,
    meta: Path,
    object_backend: str,
    s3_endpoint: str,
    s3_bucket: str,
) -> tuple[ManagedServerState, bool]:
    binary = canonical_binary(binary)
    meta = canonical_meta(meta)
    server_bind = validate_server_bind(server_bind)
    if not argv:
        raise ManagedServerError("full NoKV launch argv is required after --")
    first = capture_process(pid)
    ensure_recorded_launch_matches_process(pid, binary, argv, first)
    ensure_listener_owner(server_bind, pid)
    digest = sha256_file(binary)
    second = capture_process(pid)
    if first != second:
        raise ManagedServerError(
            f"process identity changed while recording pid {pid}; retry safely"
        )
    ensure_recorded_launch_matches_process(pid, binary, argv, second)
    ensure_listener_owner(server_bind, pid)
    second_digest = sha256_file(binary)
    if second_digest != digest:
        raise ManagedServerError(
            "NoKV binary changed while recording managed server state"
        )
    state = ManagedServerState(
        schema=STATE_SCHEMA,
        pid=pid,
        process_start_identity=first.start_identity,
        process_command=first.command,
        argv=list(argv),
        binary=str(binary),
        binary_sha256=digest,
        server_bind=server_bind,
        meta=str(meta),
        object_backend=object_backend,
        s3_endpoint=s3_endpoint,
        s3_bucket=s3_bucket,
    )
    return state, write_state(path, state)


def verify_server_state(
    path: Path,
    expectation: LaunchExpectation | None = None,
) -> ManagedServerState:
    state = load_state(path)
    first = capture_process(state.pid)
    _ensure_same_process(state, first)
    binary = Path(state.binary)
    ensure_recorded_launch_matches_process(state.pid, binary, state.argv, first)
    ensure_listener_owner(state.server_bind, state.pid)
    if not binary.is_file():
        raise ManagedServerError(f"managed NoKV binary does not exist: {binary}")
    digest = sha256_file(binary)
    if digest != state.binary_sha256:
        raise ManagedServerError(
            "managed NoKV binary was replaced in place: "
            f"expected {state.binary_sha256}, got {digest}"
        )
    second = capture_process(state.pid)
    _ensure_same_process(state, second)
    ensure_recorded_launch_matches_process(state.pid, binary, state.argv, second)
    if first != second:
        raise ManagedServerError(
            f"process identity changed while verifying pid {state.pid}"
        )
    ensure_listener_owner(state.server_bind, state.pid)
    if sha256_file(binary) != digest:
        raise ManagedServerError("managed NoKV binary changed during verification")
    if expectation is not None:
        actual_launch = LaunchExpectation(
            argv=state.argv,
            binary=state.binary,
            server_bind=state.server_bind,
            meta=state.meta,
            object_backend=state.object_backend,
            s3_endpoint=state.s3_endpoint,
            s3_bucket=state.s3_bucket,
        )
        if actual_launch != expectation:
            differing = [
                field
                for field in LaunchExpectation.__dataclass_fields__
                if getattr(actual_launch, field) != getattr(expectation, field)
            ]
            raise ManagedServerError(
                "managed server launch does not match the expected configuration; "
                f"differing={differing}"
            )
    return state


def expected_process_command(argv: list[str]) -> str:
    if not argv:
        raise ManagedServerError("full NoKV launch argv is required after --")
    if any(
        "\0" in argument or "\n" in argument or "\r" in argument for argument in argv
    ):
        raise ManagedServerError(
            "NoKV launch arguments must not contain NUL or newlines"
        )
    if not sys.platform.startswith("linux") and any(
        not argument or any(character.isspace() for character in argument)
        for argument in argv
    ):
        raise ManagedServerError(
            "cannot prove exact process argv on this platform when an argument "
            "is empty or contains whitespace"
        )
    return " ".join(argv)


def launch_server(
    path: Path,
    *,
    binary: Path,
    argv: list[str],
    server_bind: str,
    meta: Path,
    object_backend: str,
    s3_endpoint: str,
    s3_bucket: str,
) -> None:
    binary = canonical_binary(binary)
    meta = canonical_meta(meta)
    server_bind = validate_server_bind(server_bind)
    if not argv or argv[-1] != "serve":
        raise ManagedServerError("full launch argv must end with the serve command")
    if Path(argv[0]).expanduser().resolve() != binary:
        raise ManagedServerError("launch argv[0] does not resolve to --binary")
    process_command = expected_process_command(argv)
    pid = os.getpid()
    first_start_identity = _ps_field(pid, "lstart")
    digest = sha256_file(binary)
    second_start_identity = _ps_field(pid, "lstart")
    if first_start_identity != second_start_identity:
        raise ManagedServerError(
            f"launch identity changed while preparing pid {pid}; retry safely"
        )
    state = ManagedServerState(
        schema=STATE_SCHEMA,
        pid=pid,
        process_start_identity=first_start_identity,
        process_command=process_command,
        argv=list(argv),
        binary=str(binary),
        binary_sha256=digest,
        server_bind=server_bind,
        meta=str(meta),
        object_backend=object_backend,
        s3_endpoint=s3_endpoint,
        s3_bucket=s3_bucket,
    )
    write_state(path, state, require_absent=True)
    try:
        if sha256_file(binary) != digest:
            raise ManagedServerError(
                "NoKV binary changed after managed launch state was persisted"
            )
        os.execv(str(binary), argv)
        raise ManagedServerError("exec returned without replacing the launch helper")
    except BaseException as launch_error:
        try:
            remove_state_if_matches(path, state)
        except Exception as cleanup_error:
            raise ManagedServerError(
                f"NoKV exec failed ({launch_error}); managed state cleanup also "
                f"failed: {cleanup_error}"
            ) from launch_error
        raise


def _verified_state_matches(path: Path, expected: ManagedServerState) -> None:
    verified = verify_server_state(path)
    if verified != expected:
        raise ManagedServerError(
            f"managed server state changed during termination: {absolute_path(path)}"
        )


def _pidfd_api_available() -> bool:
    return (
        sys.platform.startswith("linux")
        and callable(getattr(os, "pidfd_open", None))
        and callable(getattr(signal, "pidfd_send_signal", None))
    )


def _try_open_pidfd(pid: int) -> int | None:
    if not _pidfd_api_available():
        return None
    try:
        return os.pidfd_open(pid, 0)
    except OSError as err:
        if err.errno in {errno.ENOSYS, errno.EINVAL, errno.EOPNOTSUPP}:
            return None
        raise ManagedServerError(
            f"cannot open pidfd for managed pid {pid}: {err}"
        ) from err


def _wait_for_pidfd_exit(pidfd: int, timeout_seconds: float) -> bool:
    deadline = time.monotonic() + timeout_seconds
    while True:
        remaining = max(0.0, deadline - time.monotonic())
        try:
            readable, _, _ = select.select([pidfd], [], [], remaining)
        except InterruptedError:
            if remaining == 0.0:
                return False
            continue
        return bool(readable)


def _final_revalidate_before_signal(state: ManagedServerState) -> None:
    observation = capture_process(state.pid)
    _ensure_same_process(state, observation)
    ensure_recorded_launch_matches_process(
        state.pid, Path(state.binary), state.argv, observation
    )
    ensure_listener_owner(state.server_bind, state.pid)


def _matching_process_is_alive(state: ManagedServerState) -> bool:
    try:
        observation = capture_process(state.pid)
    except ManagedServerError as inspection_error:
        try:
            os.kill(state.pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            raise inspection_error
        raise inspection_error
    return (
        observation.start_identity == state.process_start_identity
        and observation.command == state.process_command
    )


def _wait_for_matching_process_exit(
    state: ManagedServerState, timeout_seconds: float
) -> bool:
    deadline = time.monotonic() + timeout_seconds
    while _matching_process_is_alive(state):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        time.sleep(min(0.05, remaining))
    return True


def terminate_server(path: Path, timeout_seconds: float) -> ManagedServerState:
    if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise ManagedServerError("termination timeout must be a positive finite number")
    state = load_state(path)
    pidfd = _try_open_pidfd(state.pid)
    if pidfd is not None:
        try:
            _verified_state_matches(path, state)
            try:
                signal.pidfd_send_signal(pidfd, signal.SIGTERM, None, 0)
            except ProcessLookupError:
                exited = True
            else:
                exited = _wait_for_pidfd_exit(pidfd, timeout_seconds)
        finally:
            os.close(pidfd)
    else:
        _verified_state_matches(path, state)
        _final_revalidate_before_signal(state)
        try:
            os.kill(state.pid, signal.SIGTERM)
        except ProcessLookupError:
            exited = True
        else:
            exited = _wait_for_matching_process_exit(state, timeout_seconds)
    if not exited:
        raise ManagedServerError(
            f"managed NoKV server pid {state.pid} did not stop within "
            f"{timeout_seconds:g}s"
        )
    remove_state_if_matches(path, state)
    return state


def _positive_pid(value: str) -> int:
    try:
        pid = int(value)
    except ValueError as err:
        raise argparse.ArgumentTypeError("pid must be an integer") from err
    if pid < 1:
        raise argparse.ArgumentTypeError("pid must be positive")
    return pid


def _positive_timeout(value: str) -> float:
    try:
        timeout = float(value)
    except ValueError as err:
        raise argparse.ArgumentTypeError("timeout must be a number") from err
    if not math.isfinite(timeout) or timeout <= 0:
        raise argparse.ArgumentTypeError("timeout must be a positive finite number")
    return timeout


def _launch_argv(values: list[str]) -> list[str]:
    if values and values[0] == "--":
        return values[1:]
    return values


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_subparsers(dest="mode", required=True)

    write = modes.add_parser("write", help="Atomically record a running server.")
    write.add_argument("--state", type=Path, required=True)
    write.add_argument("--pid", type=_positive_pid, required=True)
    write.add_argument("--binary", type=Path, required=True)
    write.add_argument("--server-bind", required=True)
    write.add_argument("--meta", type=Path, required=True)
    write.add_argument("--object-backend", required=True)
    write.add_argument("--s3-endpoint", required=True)
    write.add_argument("--s3-bucket", required=True)
    write.add_argument("launch_argv", nargs=argparse.REMAINDER)

    launch = modes.add_parser(
        "launch", help="Persist launch identity, then exec the NoKV server."
    )
    launch.add_argument("--state", type=Path, required=True)
    launch.add_argument("--binary", type=Path, required=True)
    launch.add_argument("--server-bind", required=True)
    launch.add_argument("--meta", type=Path, required=True)
    launch.add_argument("--object-backend", required=True)
    launch.add_argument("--s3-endpoint", required=True)
    launch.add_argument("--s3-bucket", required=True)
    launch.add_argument("launch_argv", nargs=argparse.REMAINDER)

    verify = modes.add_parser("verify", help="Verify process and launch identity.")
    verify.add_argument("--state", type=Path, required=True)
    verify.add_argument("--expect-binary", type=Path)
    verify.add_argument("--expect-server-bind")
    verify.add_argument("--expect-meta", type=Path)
    verify.add_argument("--expect-object-backend")
    verify.add_argument("--expect-s3-endpoint")
    verify.add_argument("--expect-s3-bucket")
    verify.add_argument("expected_argv", nargs=argparse.REMAINDER)

    pid = modes.add_parser("pid", help="Print the PID from a valid state file.")
    pid.add_argument("--state", type=Path, required=True)

    terminate = modes.add_parser(
        "terminate", help="Verify and terminate exactly one managed server."
    )
    terminate.add_argument("--state", type=Path, required=True)
    terminate.add_argument("--timeout-seconds", type=_positive_timeout, default=5.0)

    args = parser.parse_args(argv)
    if args.mode in {"write", "launch"}:
        args.launch_argv = _launch_argv(args.launch_argv)
        if not args.launch_argv:
            parser.error(f"{args.mode} requires the full NoKV argv after --")
    elif args.mode == "verify":
        args.expected_argv = _launch_argv(args.expected_argv)
        expectation_fields = (
            args.expect_binary,
            args.expect_server_bind,
            args.expect_meta,
            args.expect_object_backend,
            args.expect_s3_endpoint,
            args.expect_s3_bucket,
        )
        has_expectation = any(
            value is not None for value in expectation_fields
        ) or bool(args.expected_argv)
        if has_expectation and (
            any(value is None for value in expectation_fields) or not args.expected_argv
        ):
            parser.error(
                "verify launch matching requires every --expect-* option and the "
                "full expected argv after --"
            )
        args.has_expectation = has_expectation
    return args


def _expectation(args: argparse.Namespace) -> LaunchExpectation:
    binary = canonical_binary(args.expect_binary)
    argv = list(args.expected_argv)
    if Path(argv[0]).expanduser().resolve() != binary:
        raise ManagedServerError("expected argv[0] does not resolve to --expect-binary")
    if argv[-1] != "serve":
        raise ManagedServerError("full expected argv must end with the serve command")
    string_values = {
        "--expect-object-backend": args.expect_object_backend,
        "--expect-s3-endpoint": args.expect_s3_endpoint,
        "--expect-s3-bucket": args.expect_s3_bucket,
    }
    for option, value in string_values.items():
        if not value:
            raise ManagedServerError(f"{option} must be non-empty")
    return LaunchExpectation(
        argv=argv,
        binary=str(binary),
        server_bind=validate_server_bind(args.expect_server_bind),
        meta=str(canonical_meta(args.expect_meta)),
        object_backend=args.expect_object_backend,
        s3_endpoint=args.expect_s3_endpoint,
        s3_bucket=args.expect_s3_bucket,
    )


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    try:
        if args.mode == "write":
            state, changed = record_server_state(
                args.state,
                pid=args.pid,
                binary=args.binary,
                argv=args.launch_argv,
                server_bind=args.server_bind,
                meta=args.meta,
                object_backend=args.object_backend,
                s3_endpoint=args.s3_endpoint,
                s3_bucket=args.s3_bucket,
            )
            print(f"state: {absolute_path(args.state)}")
            print(f"pid: {state.pid}")
            print(f"changed: {str(changed).lower()}")
        elif args.mode == "launch":
            launch_server(
                args.state,
                binary=args.binary,
                argv=args.launch_argv,
                server_bind=args.server_bind,
                meta=args.meta,
                object_backend=args.object_backend,
                s3_endpoint=args.s3_endpoint,
                s3_bucket=args.s3_bucket,
            )
        elif args.mode == "verify":
            expectation = _expectation(args) if args.has_expectation else None
            state = verify_server_state(args.state, expectation)
            print(f"pid: {state.pid}")
            print("verified: true")
            if expectation is not None:
                print("reusable: true")
        elif args.mode == "terminate":
            state = terminate_server(args.state, args.timeout_seconds)
            print(f"pid: {state.pid}")
            print("terminated: true")
            print("state_removed: true")
        else:
            state = load_state(args.state)
            print(state.pid)
    except Exception as err:
        print(f"error: {err}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
