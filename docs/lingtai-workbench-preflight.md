<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# LingTai Workbench Preflight

This page is the handoff checklist for testing the NoKV workbench MCP with a
LingTai TUI runtime.

## Start RustFS

Use the LingTai workbench script, not the Yanex benchmark script:

```bash
./scripts/lingtai-workbench/start_rustfs.sh
```

The script defaults the bucket to `nokv-lingtai-workbench`. Override the bucket
explicitly when needed:

```bash
LINGTAI_WORKBENCH_S3_BUCKET=nokv-lingtai-workbench \
  ./scripts/lingtai-workbench/start_rustfs.sh
```

## Start NoKV

Build and run the NoKV server against the same metadata address and bucket that
the LingTai MCP registration will use:

```bash
cargo build -p nokv --bin nokv
mkdir -p ~/nokv-workbench-meta
target/debug/nokv \
  --server-bind 127.0.0.1:7799 \
  --object-backend rustfs \
  --s3-bucket nokv-lingtai-workbench \
  --meta ~/nokv-workbench-meta \
  serve
```

Check client connectivity from a second terminal:

```bash
target/debug/nokv \
  --server-bind 127.0.0.1:7799 \
  --object-backend rustfs \
  --s3-bucket nokv-lingtai-workbench \
  ls /
```

## Prepare LingTai

Do not install a source tree that is older than the LingTai package already in
the TUI runtime. First inspect the runtime version:

```bash
~/.lingtai-tui/runtime/venv/bin/python - <<'PY'
import importlib.metadata as md
print(md.version("lingtai"))
PY
```

The LingTai workbench branch should be based on the matching or newer upstream
LingTai release before it is installed into the TUI runtime. Build or install
that rebased branch, then verify that the runtime has the workbench skill:

```bash
~/.lingtai-tui/runtime/venv/bin/python - <<'PY'
from pathlib import Path
import lingtai.intrinsic_skills as skills
root = Path(skills.__file__).parent
print((root / "nokv-workbench" / "SKILL.md").exists())
PY
```

## Register MCP

The MCP registration must include NoKV global connection flags before the `mcp`
subcommand:

```json
{"name":"nokv-workbench","summary":"NoKV workbench.","transport":"stdio","command":"/path/to/nokv","args":["--server-bind","127.0.0.1:7799","--object-backend","rustfs","--s3-endpoint","http://127.0.0.1:9000","--s3-bucket","nokv-lingtai-workbench","mcp","--profile","workbench","--workbench-root","/agents/{agent_id}/wb"],"source":"local-nokv"}
```

Use the same command and args in the agent's `init.json` `mcp` block, then
refresh or restart the agent.
