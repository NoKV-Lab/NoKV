#!/usr/bin/env python3
"""Bring up the single-node NoKV stack that LoopX's Stage 2A rows expect.

LoopX qualifies its NoKV authority candidate against a live owner with two
environment-gated ladder rows (`s0.nokv_live_matrix`, `s2a.nokv_live_qualification`
in `loopx/control_plane/testing/authority_e2e_ladder.py`). Those rows need an
etcd control path, an S3-compatible object store, one serving `nokv` owner, one
existing workbench, an ignored client configuration file in the exact key shape
of LoopX's `nokv_jsonl_helper.py`, and a handful of environment variables. This
script produces all of that from one command so the LoopX maintainer can run
the gate without reconstructing the recipe by hand.

It reuses the parts NoKV CI already qualifies: an isolated etcd member, the
digest-pinned RustFS container from `start_rustfs.sh` (or a `moto` S3 server
when Docker is unavailable), and the `provision` / `serve` argument shape of
`live_workbench.py`. It is a test stack: one owner, one node, no HA, no
production hardening. Credentials it generates are random, live only in the
0600 files it writes, and never appear on stdout.
"""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import os
import secrets
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable, Sequence

REPO = Path(__file__).resolve().parents[2]
START_RUSTFS = REPO / "scripts" / "workbench" / "start_rustfs.sh"
STATE_FILE = "stack.json"
CLIENT_CONFIG_FILE = "nokv-client.json"
LIVE_ENV_FILE = "live.env"
NEXT_STEPS_FILE = "next-steps.txt"
OBJECT_REGION = "us-east-1"
WORKBENCH_ROOT = "/agents/stage2a/wb"
ETCD_LEASE_TTL_SECONDS = 10
# Every string leaf of the client configuration and every value of the live
# environment becomes a forbidden token in LoopX's ladder privacy scan, which
# fails a report when any token appears in the evidence. Generated names are
# therefore prefixed and random so none can collide with ladder vocabulary.
LADDER_VOCABULARY = (
    "s0.nokv_live_matrix",
    "s2a.nokv_live_qualification",
    "stage_2a_single_node_store_conformance",
    "loopx_nokv_authority_live_qualification_v0",
    "loopx_shared_goal_authority_e2e_report_v0",
    "nokv_sdk_version",
    "nokv_api_version",
    "privacy_violations",
    "unverified",
    "pending",
    "pass",
    "fail",
)
SECRET_FLAGS = {"--object-secret-access-key"}


class StackError(RuntimeError):
    """The stack could not be prepared; nothing partially prepared is trusted."""


@dataclasses.dataclass(frozen=True)
class StackIdentity:
    root_id: str
    shard_id: str
    agent_id: str
    etcd_prefix: str
    bucket: str
    object_root: str
    access_key: str
    secret_key: str
    workbench: str
    node: str


@dataclasses.dataclass(frozen=True)
class Endpoints:
    etcd_client_port: int
    etcd_peer_port: int
    object_port: int
    object_console_port: int
    owner_port: int

    @property
    def etcd(self) -> str:
        return f"http://127.0.0.1:{self.etcd_client_port}"

    @property
    def etcd_peer(self) -> str:
        return f"http://127.0.0.1:{self.etcd_peer_port}"

    @property
    def object_store(self) -> str:
        return f"http://127.0.0.1:{self.object_port}"

    @property
    def owner(self) -> str:
        return f"127.0.0.1:{self.owner_port}"


def fresh_identity(token_hex: Callable[[int], str] = secrets.token_hex) -> StackIdentity:
    """Random ids and credentials whose prefixes cannot collide with ladder vocabulary."""

    return StackIdentity(
        root_id=token_hex(16),
        shard_id=token_hex(16),
        agent_id=token_hex(16),
        etcd_prefix=f"/lx-{token_hex(6)}",
        bucket=f"lx-{token_hex(8)}",
        object_root=f"rt-{token_hex(6)}",
        access_key=f"ak{token_hex(12)}",
        secret_key=f"sk{token_hex(24)}",
        workbench=f"wb{token_hex(5)}",
        node=f"nd-{token_hex(4)}",
    )


def digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def fresh_endpoints(port: Callable[[], int] = free_port) -> Endpoints:
    return Endpoints(
        etcd_client_port=port(),
        etcd_peer_port=port(),
        object_port=port(),
        object_console_port=port(),
        owner_port=port(),
    )


# --- command construction (pure; frozen by loopx_stage2a_stack_test.py) ------


def etcd_command(
    etcd: Path, data_dir: Path, name: str, endpoints: Endpoints
) -> list[str]:
    return [
        str(etcd),
        "--name",
        name,
        "--data-dir",
        str(data_dir),
        "--listen-client-urls",
        endpoints.etcd,
        "--advertise-client-urls",
        endpoints.etcd,
        "--listen-peer-urls",
        endpoints.etcd_peer,
        "--initial-advertise-peer-urls",
        endpoints.etcd_peer,
        "--initial-cluster",
        f"{name}={endpoints.etcd_peer}",
        "--initial-cluster-state",
        "new",
    ]


def moto_command(python: Path, endpoints: Endpoints) -> list[str]:
    return [
        str(python),
        "-m",
        "moto.server",
        "-H",
        "127.0.0.1",
        "-p",
        str(endpoints.object_port),
    ]


def control_args(binary: Path, identity: StackIdentity, endpoints: Endpoints) -> list[str]:
    return [
        str(binary),
        "--root-id",
        identity.root_id,
        "--etcd-endpoint",
        endpoints.etcd,
        "--etcd-key-prefix",
        identity.etcd_prefix,
        "--etcd-lease-ttl-seconds",
        str(ETCD_LEASE_TTL_SECONDS),
    ]


def object_args(identity: StackIdentity, endpoints: Endpoints) -> list[str]:
    return [
        "--object-bucket",
        identity.bucket,
        "--object-endpoint",
        endpoints.object_store,
        "--object-root",
        identity.object_root,
        "--object-region",
        OBJECT_REGION,
        "--object-access-key-id",
        identity.access_key,
        "--object-secret-access-key",
        identity.secret_key,
    ]


def provision_command(binary: Path, identity: StackIdentity, endpoints: Endpoints) -> list[str]:
    return [
        *control_args(binary, identity, endpoints),
        "--agent-id",
        identity.agent_id,
        *object_args(identity, endpoints),
        "provision",
        identity.shard_id,
    ]


def server_command(
    binary: Path, identity: StackIdentity, endpoints: Endpoints, metadata: Path
) -> list[str]:
    return [
        *control_args(binary, identity, endpoints),
        *object_args(identity, endpoints),
        "--bind",
        endpoints.owner,
        "--advertise-endpoint",
        endpoints.owner,
        "--node-id",
        identity.node,
        "--metadata-create",
        str(metadata),
        "--lifecycle-interval-millis",
        "100",
        "serve",
    ]


def client_config(identity: StackIdentity, endpoints: Endpoints) -> dict[str, Any]:
    """The exact key shape `loopx/control_plane/coordination/nokv_jsonl_helper.py` admits."""

    return {
        "root_id": identity.root_id,
        "routing": {
            "kind": "etcd",
            "endpoints": [endpoints.etcd],
            "key_prefix": identity.etcd_prefix,
            "lease_ttl_seconds": ETCD_LEASE_TTL_SECONDS,
        },
        "object_store": {
            "kind": "s3",
            "bucket": identity.bucket,
            "region": OBJECT_REGION,
            "root": identity.object_root,
            "endpoint": endpoints.object_store,
            "access_key_id": identity.access_key,
            "secret_access_key": identity.secret_key,
        },
        "workbench_root": WORKBENCH_ROOT,
    }


def live_env(
    identity: StackIdentity,
    endpoints: Endpoints,
    *,
    config_path: Path,
    python: Path,
) -> dict[str, str]:
    """Variables for the ladder gates `env:nokv_legacy` and `env:nokv_authority`."""

    return {
        "NOKV_COORDINATION_LIVE": "1",
        "NOKV_ETCD": endpoints.etcd,
        "NOKV_ETCD_PREFIX": identity.etcd_prefix,
        "NOKV_ROOT_ID": identity.root_id,
        "NOKV_BUCKET": identity.bucket,
        "NOKV_OBJECT_ENDPOINT": endpoints.object_store,
        "NOKV_OBJECT_ROOT": identity.object_root,
        "NOKV_OBJECT_KEY": identity.access_key,
        "NOKV_OBJECT_SECRET": identity.secret_key,
        "LOOPX_NOKV_AUTHORITY_LIVE": "1",
        "LOOPX_NOKV_AUTHORITY_CONFIG_JSON": str(config_path),
        "LOOPX_NOKV_AUTHORITY_PYTHON": str(python),
        "LOOPX_NOKV_AUTHORITY_WORKBENCH": identity.workbench,
    }


def render_env(values: dict[str, str]) -> str:
    return "".join(f"export {key}={shlex.quote(value)}\n" for key, value in values.items())


def string_leaves(value: Any) -> list[str]:
    if isinstance(value, dict):
        return [leaf for item in value.values() for leaf in string_leaves(item)]
    if isinstance(value, list):
        return [leaf for item in value for leaf in string_leaves(item)]
    if isinstance(value, str):
        return [value]
    return []


def privacy_collisions(config: dict[str, Any], env: dict[str, str]) -> list[str]:
    """Generated leaves that LoopX's ladder privacy scan would find inside its own report."""

    collisions: list[str] = []
    for leaf in {*string_leaves(config), *env.values()}:
        if len(leaf) < 4:
            continue
        for word in LADDER_VOCABULARY:
            if leaf in word:
                collisions.append(f"{leaf!r} is a substring of ladder vocabulary {word!r}")
    return sorted(collisions)


def ladder_commands(stack_dir: Path, loopx_python: str) -> list[str]:
    env = stack_dir / LIVE_ENV_FILE
    ladder = "-m loopx.control_plane.testing.authority_e2e_ladder"
    return [
        f"set -a && source {shlex.quote(str(env))} && set +a",
        f"{loopx_python} {ladder} --row s0.nokv_live_matrix --row s2a.nokv_live_qualification"
        f" --report-json {shlex.quote(str(stack_dir / 'ladder-stage2a.json'))}",
        f"{loopx_python} {ladder} --allow-unverified --report-json"
        f" {shlex.quote(str(stack_dir / 'ladder-full.json'))}",
    ]


def redact_argv(argv: Sequence[str]) -> list[str]:
    output: list[str] = []
    redact_next = False
    for argument in argv:
        if redact_next:
            output.append(f"<redacted:sha256:{digest(argument)[:12]}>")
            redact_next = False
        else:
            output.append(argument)
            redact_next = argument in SECRET_FLAGS
    return output


def plan(
    *,
    binary: Path,
    python: Path,
    stack_dir: Path,
    identity: StackIdentity,
    endpoints: Endpoints,
    object_store: str,
) -> dict[str, Any]:
    """Everything `up` will do, with secrets redacted; `plan` prints this without starting anything."""

    config = client_config(identity, endpoints)
    env = live_env(identity, endpoints, config_path=stack_dir / CLIENT_CONFIG_FILE, python=python)
    return {
        "stack_dir": str(stack_dir),
        "object_store": object_store,
        "endpoints": dataclasses.asdict(endpoints),
        "commands": {
            "etcd": redact_argv(etcd_command(Path("etcd"), stack_dir / "etcd", identity.node, endpoints)),
            "provision": redact_argv(provision_command(binary, identity, endpoints)),
            "serve": redact_argv(server_command(binary, identity, endpoints, stack_dir / "metadata")),
        },
        "client_config_sha256": digest(canonical_json(config)),
        "workbench_sha256_prefix": digest(identity.workbench)[:12],
        "privacy_collisions": privacy_collisions(config, env),
        "files": [CLIENT_CONFIG_FILE, LIVE_ENV_FILE, STATE_FILE, NEXT_STEPS_FILE],
    }


# --- process helpers ---------------------------------------------------------


def start_process(argv: Sequence[str], log: Path, env: dict[str, str] | None = None) -> subprocess.Popen[str]:
    handle = log.open("a", encoding="utf-8")
    return subprocess.Popen(
        list(argv),
        stdin=subprocess.DEVNULL,
        stdout=handle,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
        env=env,
    )


def stop_pid(pid: int) -> None:
    """Terminate a process started with its own session; escalate to SIGKILL after 10 s."""

    for sig in (signal.SIGTERM, signal.SIGKILL):
        try:
            os.kill(pid, sig)
        except ProcessLookupError:
            return
        except PermissionError:
            pass
        try:
            os.killpg(pid, sig)
        except (ProcessLookupError, PermissionError):
            pass
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                return
            time.sleep(0.1)


def wait_tcp(process: subprocess.Popen[str], port: int, timeout: float, what: str) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise StackError(f"{what} exited before listening (code {process.returncode})")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise StackError(f"{what} did not listen on 127.0.0.1:{port} within {timeout:.0f}s")


def wait_etcd(etcdctl: Path, endpoint: str, process: subprocess.Popen[str], timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise StackError(f"etcd exited before readiness (code {process.returncode})")
        result = subprocess.run(
            [str(etcdctl), f"--endpoints={endpoint}", "endpoint", "health"],
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
        if result.returncode == 0:
            return
        time.sleep(0.1)
    raise StackError(f"etcd did not become healthy at {endpoint}")


def run_checked(argv: Sequence[str], *, timeout: float, what: str, log: Path | None = None) -> str:
    result = subprocess.run(list(argv), capture_output=True, text=True, timeout=timeout, check=False)
    if log is not None:
        with log.open("a", encoding="utf-8") as handle:
            handle.write(f"$ {' '.join(redact_argv(argv))}\n{result.stdout}{result.stderr}\n")
    if result.returncode != 0:
        raise StackError(f"{what} failed (code {result.returncode}); see {log or 'stderr'}")
    return result.stdout


def python_json(python: Path, snippet: str, payload: dict[str, Any], *, timeout: float, what: str) -> dict[str, Any]:
    """Run a snippet under the SDK interpreter; the snippet reads JSON on stdin and prints JSON."""

    result = subprocess.run(
        [str(python), "-I", "-c", snippet],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    if result.returncode != 0:
        raise StackError(f"{what} failed under {python}: {result.stderr.strip().splitlines()[-1:] or 'no output'}")
    try:
        value = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise StackError(f"{what} printed invalid JSON") from error
    if not isinstance(value, dict):
        raise StackError(f"{what} printed a non-object")
    return value


SDK_VERSION_SNIPPET = """
import json, sys
import nokv
print(json.dumps({"version": nokv.__version__, "api_version": nokv.API_VERSION,
    "fence": hasattr(nokv, "WorkspaceIncarnationMismatch")}))
"""

CREATE_WORKBENCH_SNIPPET = """
import json, sys
import nokv
p = json.load(sys.stdin)
client = nokv.Client(
    root_id=p["root_id"],
    routing=nokv.RoutingConfig.etcd(p["etcd_endpoints"], p["etcd_prefix"], p["lease_ttl"]),
    object_store=nokv.ObjectStoreConfig.s3(p["bucket"], region=p["region"], root=p["object_root"],
        endpoint=p["object_endpoint"], access_key_id=p["access_key"], secret_access_key=p["secret_key"]),
    workbench_root=p["workbench_root"],
)
client.create_workspace(p["workbench"])
found = []
cursor = None
while True:
    page = client.find_workspaces(cursor=cursor, limit=100)
    for entry in page["workspaces"]:
        workspace = entry["workspace"]
        if workspace["workbench"] == p["workbench"]:
            found.append(workspace["workspace_incarnation_id"])
    cursor = page.get("next_cursor")
    if not cursor:
        break
print(json.dumps({"incarnations": found}))
"""

MOTO_BUCKET_SNIPPET = """
import json, sys
import boto3
p = json.load(sys.stdin)
s3 = boto3.client("s3", endpoint_url=p["endpoint"], region_name=p["region"],
    aws_access_key_id=p["access_key"], aws_secret_access_key=p["secret_key"])
s3.create_bucket(Bucket=p["bucket"])
print(json.dumps({"buckets": [b["Name"] for b in s3.list_buckets()["Buckets"]]}))
"""


# --- commands -----------------------------------------------------------------


def require_executable(path: str, hint: str) -> Path:
    resolved = path if os.path.sep in path else shutil.which(path)
    if resolved is None or not os.access(resolved, os.X_OK):
        raise StackError(f"{path} is not executable; {hint}")
    # Keep the caller's path (absolute, symlinks intact): a venv's bin/python is
    # a symlink to the base interpreter and must stay the venv's entry point.
    return Path(os.path.abspath(resolved))


def write_private(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    os.chmod(path, 0o600)


def command_up(args: argparse.Namespace) -> int:
    stack_dir = Path(args.stack_dir).resolve()
    if stack_dir.exists() and any(stack_dir.iterdir()):
        raise StackError(f"stack directory is not empty: {stack_dir}")
    python = require_executable(args.python, "pass --python from a venv that has the nokv wheel installed")
    if not python.is_absolute():
        raise StackError("--python must be an absolute path")
    binary = Path(args.nokv_binary).resolve() if args.nokv_binary else REPO / "target" / "release" / "nokv"
    if args.build:
        cargo = require_executable("cargo", "install the Rust toolchain or pass --nokv-binary")
        run_checked(
            [str(cargo), "build", "--release", "--locked", "-p", "nokv", "--bin", "nokv"],
            timeout=3600,
            what="cargo build",
        )
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise StackError(f"nokv binary is missing: {binary} (pass --build or --nokv-binary)")
    etcd = require_executable(args.etcd_bin, "install etcd (for example `brew install etcd`) or pass --etcd-bin")
    etcdctl = require_executable(args.etcdctl_bin, "install etcd or pass --etcdctl-bin")

    sdk = python_json(python, SDK_VERSION_SNIPPET, {}, timeout=60, what="NoKV SDK import")
    binary_version = run_checked([str(binary), "--version"], timeout=30, what="nokv --version").split()[-1]
    if sdk["version"] != binary_version:
        raise StackError(
            f"the nokv binary reports {binary_version} but the SDK under --python reports {sdk['version']};"
            " LoopX's helper requires wheel and owner from the same release"
        )
    if not sdk["fence"]:
        raise StackError("the SDK under --python lacks WorkspaceIncarnationMismatch; LoopX pins a fenced wheel")

    object_store = args.object_store
    if object_store == "auto":
        object_store = "rustfs" if shutil.which("docker") and shutil.which("aws") else "moto"
    if object_store == "moto":
        probe = subprocess.run([str(python), "-c", "import moto, boto3"], capture_output=True, text=True, check=False)
        if probe.returncode != 0:
            raise StackError("--object-store moto needs `pip install 'moto[server]' boto3` in the --python venv")
    elif not START_RUSTFS.is_file():
        raise StackError(f"missing {START_RUSTFS}")

    identity = fresh_identity()
    endpoints = fresh_endpoints()
    logs = stack_dir / "logs"
    logs.mkdir(parents=True)
    (stack_dir / "etcd").mkdir()
    state: dict[str, Any] = {
        "schema": "nokv.loopx_stage2a_stack.v1",
        "stack_dir": str(stack_dir),
        "object_store": object_store,
        "endpoints": dataclasses.asdict(endpoints),
        "pids": {},
        "binary": str(binary),
        "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "sdk": sdk,
        "workbench_sha256_prefix": digest(identity.workbench)[:12],
    }
    started: list[subprocess.Popen[str]] = []

    def persist() -> None:
        write_private(stack_dir / STATE_FILE, json.dumps(state, indent=2, sort_keys=True) + "\n")

    try:
        etcd_process = start_process(
            etcd_command(etcd, stack_dir / "etcd", identity.node, endpoints), logs / "etcd.log"
        )
        started.append(etcd_process)
        state["pids"]["etcd"] = etcd_process.pid
        persist()
        wait_etcd(etcdctl, endpoints.etcd, etcd_process, timeout=30)

        if object_store == "moto":
            moto_process = start_process(moto_command(python, endpoints), logs / "moto.log")
            started.append(moto_process)
            state["pids"]["moto"] = moto_process.pid
            persist()
            wait_tcp(moto_process, endpoints.object_port, 60, "moto")
            python_json(
                python,
                MOTO_BUCKET_SNIPPET,
                {
                    "endpoint": endpoints.object_store,
                    "region": OBJECT_REGION,
                    "access_key": identity.access_key,
                    "secret_key": identity.secret_key,
                    "bucket": identity.bucket,
                },
                timeout=60,
                what="moto bucket creation",
            )
        else:
            container = f"lx-stage2a-rustfs-{identity.node[3:]}"
            state["rustfs_container"] = container
            persist()
            env = os.environ.copy()
            env.update(
                {
                    "NOKV_WORKBENCH_RUSTFS_CONTAINER": container,
                    "NOKV_WORKBENCH_RUSTFS_PORT": str(endpoints.object_port),
                    "NOKV_WORKBENCH_RUSTFS_CONSOLE_PORT": str(endpoints.object_console_port),
                    "NOKV_WORKBENCH_RUSTFS_DATA_DIR": str(stack_dir / "rustfs"),
                    "NOKV_WORKBENCH_S3_ACCESS_KEY_ID": identity.access_key,
                    "NOKV_WORKBENCH_S3_SECRET_ACCESS_KEY": identity.secret_key,
                    "NOKV_WORKBENCH_S3_BUCKET": identity.bucket,
                }
            )
            env.pop("NOKV_WORKBENCH_RUSTFS_VOLUME", None)
            result = subprocess.run(
                ["bash", str(START_RUSTFS)], env=env, capture_output=True, text=True, timeout=600, check=False
            )
            with (logs / "rustfs.log").open("a", encoding="utf-8") as handle:
                handle.write(result.stdout + result.stderr)
            if result.returncode != 0:
                raise StackError(f"start_rustfs.sh failed (code {result.returncode}); see {logs / 'rustfs.log'}")

        run_checked(
            provision_command(binary, identity, endpoints),
            timeout=300,
            what="nokv provision",
            log=logs / "provision.log",
        )
        owner = start_process(
            server_command(binary, identity, endpoints, stack_dir / "metadata"), logs / "owner.log"
        )
        started.append(owner)
        state["pids"]["owner"] = owner.pid
        persist()
        wait_tcp(owner, endpoints.owner_port, 120, "nokv owner")

        created = python_json(
            python,
            CREATE_WORKBENCH_SNIPPET,
            {
                "root_id": identity.root_id,
                "etcd_endpoints": [endpoints.etcd],
                "etcd_prefix": identity.etcd_prefix,
                "lease_ttl": ETCD_LEASE_TTL_SECONDS,
                "bucket": identity.bucket,
                "region": OBJECT_REGION,
                "object_root": identity.object_root,
                "object_endpoint": endpoints.object_store,
                "access_key": identity.access_key,
                "secret_key": identity.secret_key,
                "workbench_root": WORKBENCH_ROOT,
                "workbench": identity.workbench,
            },
            timeout=180,
            what="workbench creation through the SDK",
        )
        incarnations = created.get("incarnations")
        if not isinstance(incarnations, list) or len(incarnations) != 1:
            raise StackError(f"expected exactly one incarnation for the new workbench, found {incarnations!r}")
        state["workbench_incarnation_sha256_prefix"] = digest(str(incarnations[0]))[:12]

        config = client_config(identity, endpoints)
        env_values = live_env(identity, endpoints, config_path=stack_dir / CLIENT_CONFIG_FILE, python=python)
        collisions = privacy_collisions(config, env_values)
        if collisions:
            raise StackError("generated identity collides with ladder vocabulary: " + "; ".join(collisions))
        write_private(stack_dir / CLIENT_CONFIG_FILE, json.dumps(config, indent=2) + "\n")
        write_private(stack_dir / LIVE_ENV_FILE, render_env(env_values))
        state["client_config_sha256"] = digest(canonical_json(config))
        commands = ladder_commands(stack_dir, args.loopx_python)
        write_private(
            stack_dir / NEXT_STEPS_FILE,
            "# Run from a LoopX checkout whose Python has loopx importable.\n" + "\n".join(commands) + "\n",
        )
        state["ready"] = True
        persist()
    except BaseException as error:
        exit_codes = {process.pid: process.poll() for process in started}
        try:
            for process in reversed(started):
                stop_pid(process.pid)
            if state.get("rustfs_container"):
                subprocess.run(
                    ["docker", "rm", "-f", state["rustfs_container"]], capture_output=True, check=False
                )
        finally:
            state["ready"] = False
            state["failure"] = {"error": str(error), "exit_codes_before_cleanup": exit_codes}
            persist()
        raise

    summary = {
        "stack_dir": str(stack_dir),
        "object_store": object_store,
        "owner": endpoints.owner,
        "pids": state["pids"],
        "binary_sha256": state["binary_sha256"],
        "sdk": sdk,
        "client_config_sha256": state["client_config_sha256"],
        "workbench_sha256_prefix": state["workbench_sha256_prefix"],
        "files": {name: str(stack_dir / name) for name in (CLIENT_CONFIG_FILE, LIVE_ENV_FILE, NEXT_STEPS_FILE)},
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    print("\nnext steps (also in next-steps.txt):", file=sys.stderr)
    for line in commands:
        print(f"  {line}", file=sys.stderr)
    return 0


def load_state(stack_dir: Path) -> dict[str, Any]:
    path = stack_dir / STATE_FILE
    if not path.is_file():
        raise StackError(f"no {STATE_FILE} in {stack_dir}; nothing to act on")
    state = json.loads(path.read_text(encoding="utf-8"))
    if state.get("schema") != "nokv.loopx_stage2a_stack.v1":
        raise StackError(f"unknown state schema in {path}")
    return state


def pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def command_down(args: argparse.Namespace) -> int:
    stack_dir = Path(args.stack_dir).resolve()
    state = load_state(stack_dir)
    for name in ("owner", "moto", "etcd"):
        pid = state.get("pids", {}).get(name)
        if isinstance(pid, int):
            stop_pid(pid)
    container = state.get("rustfs_container")
    if container:
        subprocess.run(["docker", "rm", "-f", container], capture_output=True, check=False)
    if args.purge:
        shutil.rmtree(stack_dir)
    else:
        state["ready"] = False
        write_private(stack_dir / STATE_FILE, json.dumps(state, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"stack_dir": str(stack_dir), "stopped": True, "purged": bool(args.purge)}))
    return 0


def command_status(args: argparse.Namespace) -> int:
    stack_dir = Path(args.stack_dir).resolve()
    state = load_state(stack_dir)
    report = {
        "stack_dir": str(stack_dir),
        "ready": bool(state.get("ready")),
        "object_store": state.get("object_store"),
        "owner": f"127.0.0.1:{state['endpoints']['owner_port']}",
        "processes": {name: pid_alive(pid) for name, pid in state.get("pids", {}).items()},
        "client_config_sha256": state.get("client_config_sha256"),
        "workbench_sha256_prefix": state.get("workbench_sha256_prefix"),
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if all(report["processes"].values()) and report["ready"] else 1


def command_plan(args: argparse.Namespace) -> int:
    stack_dir = Path(args.stack_dir).resolve()
    binary = Path(args.nokv_binary).resolve() if args.nokv_binary else REPO / "target" / "release" / "nokv"
    object_store = args.object_store
    if object_store == "auto":
        object_store = "rustfs" if shutil.which("docker") and shutil.which("aws") else "moto"
    print(
        json.dumps(
            plan(
                binary=binary,
                python=Path(args.python),
                stack_dir=stack_dir,
                identity=fresh_identity(),
                endpoints=fresh_endpoints(),
                object_store=object_store,
            ),
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)

    def common(sub: argparse.ArgumentParser, *, needs_python: bool) -> None:
        sub.add_argument("--stack-dir", required=True, help="empty directory that receives data, logs and the two private files")
        if needs_python:
            sub.add_argument("--python", required=True, help="absolute path to a Python whose venv has the matching nokv wheel")
            sub.add_argument("--nokv-binary", help="serving binary (default: target/release/nokv of this checkout)")
            sub.add_argument("--object-store", choices=("auto", "rustfs", "moto"), default="auto")
            sub.add_argument("--loopx-python", default="python", help="interpreter name to print in the LoopX ladder commands")

    up = commands.add_parser("up", help="start etcd, the object store and one owner; create one workbench")
    common(up, needs_python=True)
    up.add_argument("--build", action="store_true", help="cargo build --release -p nokv --bin nokv first")
    up.add_argument("--etcd-bin", default="etcd")
    up.add_argument("--etcdctl-bin", default="etcdctl")
    up.set_defaults(run=command_up)

    planned = commands.add_parser("plan", help="print the redacted plan without starting anything")
    common(planned, needs_python=True)
    planned.set_defaults(run=command_plan)

    down = commands.add_parser("down", help="stop every process and container of a stack")
    common(down, needs_python=False)
    down.add_argument("--purge", action="store_true", help="also delete the stack directory")
    down.set_defaults(run=command_down)

    status = commands.add_parser("status", help="report whether the stack's processes are alive")
    common(status, needs_python=False)
    status.set_defaults(run=command_status)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return int(args.run(args))
    except StackError as error:
        print(f"loopx_stage2a_stack: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
