<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# LingTai Workbench Scripts

This directory contains local preflight helpers for the LingTai workbench MCP
demo path. These scripts are not benchmark harnesses and do not depend on
benchmark data directories.

## One-command Setup

Run the full local preflight and MCP install path with:

```bash
./scripts/lingtai-workbench/up.sh
```

`up.sh` does the following with the defaults below:

- builds `target/debug/nokv`
- starts or verifies RustFS and the `nokv-lingtai-workbench` bucket
- verifies or starts the NoKV server at `127.0.0.1:7799`
- checks that the LingTai TUI runtime can see the `nokv-workbench` skill
- checks that the workbench MCP exposes `workbench_*` tools
- idempotently installs the MCP registration into the selected LingTai agent

Defaults:

```text
RustFS endpoint:  http://127.0.0.1:9000
NoKV server bind: 127.0.0.1:7799
bucket:           nokv-lingtai-workbench
workbench root:   /agents/{agent_id}/wb (kernel expands {agent_id} per agent)
state dir:        target/lingtai-workbench
```

Project selection:

1. `LINGTAI_WORKBENCH_PROJECT`
2. the current directory when it contains `.lingtai/`
3. `~/lingtai-demo`

Agent selection is automatic: explicit `--agent-dir` or `--agent` in the lower
level installer wins, otherwise the installer chooses one running coordinator,
then one coordinator, then the only agent. Ambiguous multi-agent projects fail
with a list of candidates.

After `up.sh` finishes, refresh the selected agent inside LingTai:

```text
/refresh
```

`/refresh` restarts the MCP stdio child process. The NoKV server does not need
to be restarted for MCP-only changes because request argument parsing and tool
definitions live in `nokv mcp --profile workbench`.

## Start RustFS

Start or reuse the dedicated LingTai workbench RustFS endpoint:

```bash
./scripts/lingtai-workbench/start_rustfs.sh
```

Defaults:

```text
endpoint: http://127.0.0.1:9000
bucket:   nokv-lingtai-workbench
data:     target/lingtai-workbench/rustfs
```

Override the endpoint only when the default ports are already occupied:

```bash
LINGTAI_WORKBENCH_RUSTFS_PORT=9010 \
LINGTAI_WORKBENCH_RUSTFS_CONSOLE_PORT=9011 \
LINGTAI_WORKBENCH_S3_ENDPOINT=http://127.0.0.1:9010 \
./scripts/lingtai-workbench/start_rustfs.sh
```

Use the same endpoint and bucket in the NoKV server, CLI checks, and MCP
registration.

## Start NoKV

Build the CLI binary:

```bash
cargo build -p nokv --bin nokv
```

Start the metadata server in a separate terminal:

```bash
mkdir -p ~/nokv-workbench-meta

./target/debug/nokv \
  --server-bind 127.0.0.1:7799 \
  --object-backend rustfs \
  --s3-endpoint http://127.0.0.1:9000 \
  --s3-bucket nokv-lingtai-workbench \
  --meta ~/nokv-workbench-meta \
  serve
```

Check that the client path can reach the server and object store:

```bash
./target/debug/nokv \
  --server-bind 127.0.0.1:7799 \
  --object-backend rustfs \
  --s3-endpoint http://127.0.0.1:9000 \
  --s3-bucket nokv-lingtai-workbench \
  ls /
```

An empty root with exit status 0 is a successful preflight check.

## Install MCP Into One LingTai Agent

`install_workbench_mcp.py` idempotently writes the target agent's two LingTai
MCP files:

- `<agent>/mcp_registry.jsonl`
- `<agent>/init.json`

Example:

```bash
python3 ./scripts/lingtai-workbench/install_workbench_mcp.py \
  --project /Users/wangchanghao/lingtai-demo \
  --agent 'coordinator(codex-gpt-5.4)' \
  --nokv-bin /Users/wangchanghao/NoKV/target/debug/nokv \
  --server-bind 127.0.0.1:7799 \
  --object-backend rustfs \
  --s3-endpoint http://127.0.0.1:9000 \
  --s3-bucket nokv-lingtai-workbench \
  --workbench-root '/agents/{agent_id}/wb'
```

The installer upserts the `nokv-workbench` MCP server registration. Re-running
the same command does not duplicate registry lines or rewrite files when the
desired state is already present.

If you already know the agent directory, pass it directly:

```bash
python3 ./scripts/lingtai-workbench/install_workbench_mcp.py \
  --agent-dir '/Users/wangchanghao/lingtai-demo/.lingtai/coordinator(codex-gpt-5.4)' \
  --nokv-bin /Users/wangchanghao/NoKV/target/debug/nokv
```

## Runtime Skill Check

The TUI runtime must be able to see the `nokv-workbench` skill:

```bash
~/.lingtai-tui/runtime/venv/bin/python - <<'PY'
from pathlib import Path
import importlib.metadata as md
import lingtai.intrinsic_skills as skills

print("lingtai:", md.version("lingtai"))
root = Path(skills.__file__).parent
print("nokv-workbench skill:", (root / "nokv-workbench" / "SKILL.md").exists())
PY
```

The installer does not replace or patch the TUI runtime. Install a
workbench-enabled LingTai runtime separately, then run this installer for each
agent that should receive the MCP tools.

## Tool Names

The MCP server exposes workbench tools with the `workbench_` prefix:

```text
workbench_create
workbench_put_file
workbench_list
workbench_stat
workbench_read
workbench_grep
workbench_find
workbench_commit
workbench_snapshot
```

The server registration name remains `nokv-workbench`; that is the MCP server
id used by LingTai, not the public tool-name prefix.

## Tests

Run the installer tests with:

```bash
python3 ./scripts/lingtai-workbench/install_workbench_mcp_test.py
bash -n ./scripts/lingtai-workbench/up.sh
```
