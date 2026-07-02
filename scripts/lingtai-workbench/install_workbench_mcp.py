#!/usr/bin/env python3
import argparse
import json
import os
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_MCP_NAME = "nokv-workbench"
DEFAULT_BUCKET = "nokv-lingtai-workbench"
DEFAULT_ENDPOINT = "http://127.0.0.1:9000"
DEFAULT_SERVER_BIND = "127.0.0.1:7799"
# Per-agent tenant isolation: the {agent_id} placeholder is written verbatim
# and expanded by lingtai-kernel at MCP launch (Agent._expand_agent_placeholders).
# Must stay identical to the kernel's bundled nokv-workbench skill assets.
DEFAULT_WORKBENCH_ROOT = "/agents/{agent_id}/wb"


@dataclass(frozen=True)
class InstallConfig:
    nokv_bin: str
    server_bind: str
    object_backend: str
    s3_endpoint: str | None
    s3_bucket: str
    workbench_root: str
    mcp_name: str = DEFAULT_MCP_NAME


@dataclass(frozen=True)
class InstallResult:
    agent_dir: Path
    registry_changed: bool
    init_changed: bool


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def default_nokv_bin() -> str:
    return str(repo_root() / "target" / "debug" / "nokv")


def mcp_args(config: InstallConfig) -> list[str]:
    args = [
        "--server-bind",
        config.server_bind,
        "--object-backend",
        config.object_backend,
    ]
    if config.s3_endpoint:
        args.extend(["--s3-endpoint", config.s3_endpoint])
    args.extend(
        [
            "--s3-bucket",
            config.s3_bucket,
            "mcp",
            "--profile",
            "workbench",
            "--workbench-root",
            config.workbench_root,
        ]
    )
    return args


def registry_record(config: InstallConfig) -> dict[str, Any]:
    return {
        "name": config.mcp_name,
        "summary": "NoKV LingTai workbench.",
        "transport": "stdio",
        "command": config.nokv_bin,
        "args": mcp_args(config),
        "source": "local-nokv",
    }


def init_spec(config: InstallConfig) -> dict[str, Any]:
    return {
        "type": "stdio",
        "command": config.nokv_bin,
        "args": mcp_args(config),
    }


def read_registry(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    records = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        stripped = line.strip()
        if not stripped:
            continue
        try:
            record = json.loads(stripped)
        except json.JSONDecodeError as err:
            raise ValueError(f"{path}:{line_number}: invalid JSONL entry: {err}") from err
        if not isinstance(record, dict):
            raise ValueError(f"{path}:{line_number}: registry entry must be a JSON object")
        name = record.get("name")
        if not isinstance(name, str) or not name:
            raise ValueError(f"{path}:{line_number}: registry entry must have a string name")
        records.append(record)
    return records


def write_text_if_changed(path: Path, text: str) -> bool:
    if path.exists() and path.read_text(encoding="utf-8") == text:
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
        os.replace(tmp_name, path)
    finally:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)
    return True


def upsert_registry(agent_dir: Path, config: InstallConfig) -> bool:
    path = agent_dir / "mcp_registry.jsonl"
    desired = registry_record(config)
    records = read_registry(path)
    output = [desired]
    output.extend(record for record in records if record.get("name") != config.mcp_name)
    text = "".join(
        json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"
        for record in output
    )
    return write_text_if_changed(path, text)


def read_init(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"{path} does not exist")
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return data


def upsert_init(agent_dir: Path, config: InstallConfig) -> bool:
    path = agent_dir / "init.json"
    data = read_init(path)
    mcp = data.setdefault("mcp", {})
    if not isinstance(mcp, dict):
        raise ValueError(f"{path}: mcp must be a JSON object when present")
    mcp[config.mcp_name] = init_spec(config)
    text = json.dumps(data, ensure_ascii=False, indent=2) + "\n"
    return write_text_if_changed(path, text)


def configure_agent(agent_dir: Path | str, config: InstallConfig) -> InstallResult:
    resolved = Path(agent_dir).expanduser().resolve()
    if not resolved.is_dir():
        raise FileNotFoundError(f"agent directory does not exist: {resolved}")
    registry_changed = upsert_registry(resolved, config)
    init_changed = upsert_init(resolved, config)
    return InstallResult(
        agent_dir=resolved,
        registry_changed=registry_changed,
        init_changed=init_changed,
    )


def agent_candidates(project: Path) -> list[Path]:
    agents_root = project.expanduser() / ".lingtai"
    if not agents_root.is_dir():
        raise FileNotFoundError(f"LingTai project has no .lingtai directory: {project}")
    return sorted(
        path
        for path in agents_root.iterdir()
        if path.is_dir() and not path.name.startswith(".") and (path / "init.json").is_file()
    )


def agent_is_running(agent_dir: Path) -> bool:
    status_path = agent_dir / ".status.json"
    if not status_path.exists():
        return False
    try:
        status = json.loads(status_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return False
    runtime = status.get("runtime")
    return isinstance(runtime, dict) and runtime.get("running") is True


def is_coordinator(agent_dir: Path) -> bool:
    return agent_dir.name.startswith("coordinator")


def choose_default_agent(project: Path) -> Path:
    candidates = agent_candidates(project)
    if not candidates:
        raise ValueError(f"no LingTai agents with init.json found under {project / '.lingtai'}")

    running_coordinators = [
        agent_dir for agent_dir in candidates if is_coordinator(agent_dir) and agent_is_running(agent_dir)
    ]
    if len(running_coordinators) == 1:
        return running_coordinators[0]
    if len(running_coordinators) > 1:
        names = ", ".join(agent.name for agent in running_coordinators)
        raise ValueError(f"multiple running coordinator agents found: {names}")

    coordinators = [agent_dir for agent_dir in candidates if is_coordinator(agent_dir)]
    if len(coordinators) == 1:
        return coordinators[0]
    if len(coordinators) > 1:
        names = ", ".join(agent.name for agent in coordinators)
        raise ValueError(f"multiple coordinator agents found: {names}")

    if len(candidates) == 1:
        return candidates[0]

    names = ", ".join(agent.name for agent in candidates)
    raise ValueError(f"multiple LingTai agents found; pass --agent explicitly: {names}")


def resolve_agent_dir(project: Path, agent: str | None, agent_dir: str | None) -> Path:
    if agent_dir:
        return Path(agent_dir).expanduser()
    if agent:
        return project.expanduser() / ".lingtai" / agent
    return choose_default_agent(project)


def describe_agent_selection(agent: str | None, agent_dir: str | None, resolved: Path) -> str:
    if agent_dir:
        return "explicit --agent-dir"
    if agent:
        return f"explicit --agent {agent}"
    return f"default {resolved.name}"


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Idempotently register the NoKV workbench MCP for one LingTai agent.",
    )
    parser.add_argument("--project", default=".", help="LingTai project directory.")
    parser.add_argument("--agent", help="Agent directory name under PROJECT/.lingtai.")
    parser.add_argument("--agent-dir", help="Explicit LingTai agent directory.")
    parser.add_argument("--nokv-bin", default=default_nokv_bin(), help="Path to nokv binary.")
    parser.add_argument("--server-bind", default=DEFAULT_SERVER_BIND)
    parser.add_argument("--object-backend", default="rustfs")
    parser.add_argument("--s3-endpoint", default=DEFAULT_ENDPOINT)
    parser.add_argument("--s3-bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--workbench-root", default=DEFAULT_WORKBENCH_ROOT)
    parser.add_argument("--mcp-name", default=DEFAULT_MCP_NAME)
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    try:
        agent_dir = resolve_agent_dir(Path(args.project), args.agent, args.agent_dir)
        config = InstallConfig(
            nokv_bin=str(Path(args.nokv_bin).expanduser()),
            server_bind=args.server_bind,
            object_backend=args.object_backend,
            s3_endpoint=args.s3_endpoint or None,
            s3_bucket=args.s3_bucket,
            workbench_root=args.workbench_root,
            mcp_name=args.mcp_name,
        )
        result = configure_agent(agent_dir, config)
    except Exception as err:
        print(f"error: {err}", file=sys.stderr)
        return 1

    print(f"agent_dir: {result.agent_dir}")
    print(f"agent_selection: {describe_agent_selection(args.agent, args.agent_dir, result.agent_dir)}")
    print(f"registry_changed: {str(result.registry_changed).lower()}")
    print(f"init_changed: {str(result.init_changed).lower()}")
    if result.registry_changed or result.init_changed:
        print("next: run /refresh in the target LingTai agent")
    else:
        print("already configured")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
