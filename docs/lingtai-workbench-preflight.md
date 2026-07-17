<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Configure and Update NoKV for LingTai Workbench

This is the user guide for connecting one existing LingTai Agent to NoKV and
keeping that connection current. The supported user path is a clean NoKV source
checkout plus `scripts/lingtai-workbench/up.sh`. Brew distribution is not yet
available.

## Know Which MCP Surface You Need

NoKV has two MCP profiles:

- The generic Agent profile exposes seven read-only namespace tools: `ls`,
  `stat`, `catalog`, `read`, `aggregate`, `find`, and `grep`. It is suitable for
  a general MCP client, but it is not the LingTai workbench integration.
- The Workbench profile exposes exactly 18 `workbench_*` tools, including
  writes, checkpoints, indexed queries, and durable `workbench_restore`. It is
  jailed below `/agents/{agent_id}/wb` and is registered in LingTai as
  `nokv-workbench`.

The setup below requires the complete 18-tool Workbench profile. Restore is
advertised only when every metadata owner that can serve the selected Agent
root supports `restore_to_fork_v1`; the setup fails closed if the fleet is
mixed or the schema differs.

## Before the First Configuration

Prepare all of the following:

1. A LingTai project that already contains at least one Agent directory and
   `init.json` below `/path/to/project/.lingtai/`. The NoKV helper registers MCP
   for an Agent; it does not create the Agent.
2. A LingTai TUI runtime that already contains the `nokv-workbench` intrinsic
   skill.
3. A clean NoKV source checkout on the revision you intend to deploy.
4. `python3`, `git`, Cargo/Rust, `lsof`, and the AWS CLI. Docker is also needed
   when the configured RustFS endpoint is not already running.
5. A persistent metadata directory. Do not use a disposable checkout, `/tmp`,
   or a directory removed by `cargo clean`. Keep using the same directory on
   every update.

Verify the LingTai skill before changing NoKV:

```bash
~/.lingtai-tui/runtime/venv/bin/python - <<'PY'
from pathlib import Path
import lingtai.intrinsic_skills as skills

root = Path(skills.__file__).parent
print((root / "nokv-workbench" / "SKILL.md").exists())
PY
```

The command must print `True`. Install a workbench-enabled LingTai release
before continuing if it does not.

Choose stable local storage once. This example keeps both Holt metadata and
local RustFS objects outside the source checkout:

```bash
export LINGTAI_WORKBENCH_META_DIR="$HOME/.local/share/nokv/lingtai-workbench/meta"
export LINGTAI_WORKBENCH_RUSTFS_DATA_DIR="$HOME/.local/share/nokv/lingtai-workbench/rustfs"
```

The guarded one-command path intentionally uses its dedicated local RustFS
credentials. Custom credential and secret-manager integration is outside this
helper and requires a separately reviewed deployment; it is not a supported
`up.sh` override. The metadata directory and object-store identity are durable
deployment state, so do not silently point an update at a new empty location.

## First Configuration

From the NoKV checkout:

```bash
git switch main
git pull --ff-only

LINGTAI_WORKBENCH_PROJECT=/path/to/lingtai-project \
LINGTAI_WORKBENCH_META_DIR="$HOME/.local/share/nokv/lingtai-workbench/meta" \
LINGTAI_WORKBENCH_RUSTFS_DATA_DIR="$HOME/.local/share/nokv/lingtai-workbench/rustfs" \
./scripts/lingtai-workbench/up.sh
```

`up.sh` accepts no command-line arguments. Configure it only with environment
variables. It performs the following guarded handoff:

- validates the existing LingTai Agent files and Workbench skill;
- builds NoKV with the locked `Cargo.lock` and records the exact Holt revision;
- stages an immutable binary under the LingTai project by NoKV commit and
  binary SHA-256;
- probes the candidate's exact 18-tool contract against the current metadata
  endpoint before replacing a running server, then starts or verifies RustFS
  and the helper-managed NoKV metadata server;
- rechecks the selected Agent's concrete root after the server handoff;
- updates `mcp_registry.jsonl`, `init.json`, and the NoKV lock under a per-Agent
  lock and recovery journal.

By default the helper selects, in order, the only running coordinator, the only
coordinator, or the only Agent. Do not set an Agent name unless selection is
ambiguous. If the error lists multiple candidates, rerun with the complete
directory name exactly as printed:

```bash
LINGTAI_WORKBENCH_PROJECT=/path/to/lingtai-project \
LINGTAI_WORKBENCH_AGENT='coordinator(codex-gpt-5.4)' \
LINGTAI_WORKBENCH_META_DIR="$HOME/.local/share/nokv/lingtai-workbench/meta" \
LINGTAI_WORKBENCH_RUSTFS_DATA_DIR="$HOME/.local/share/nokv/lingtai-workbench/rustfs" \
./scripts/lingtai-workbench/up.sh
```

After a successful handoff, run this command in the selected LingTai Agent:

```text
/refresh
```

This restarts the MCP stdio child with the newly locked runtime.

## Daily Update

Use the same path for every NoKV update. Preserve the metadata directory,
object store, project, and Agent selection used during the first configuration:

```bash
cd /path/to/NoKV
git switch main
git pull --ff-only

LINGTAI_WORKBENCH_PROJECT=/path/to/lingtai-project \
LINGTAI_WORKBENCH_META_DIR="$HOME/.local/share/nokv/lingtai-workbench/meta" \
LINGTAI_WORKBENCH_RUSTFS_DATA_DIR="$HOME/.local/share/nokv/lingtai-workbench/rustfs" \
./scripts/lingtai-workbench/up.sh
```

Run `/refresh` only after the script reports success. The Agent registration
uses the immutable staged copy, not mutable `target/release/nokv`, so a later
Cargo build cannot silently change the launched executable.

## Read-Only Check

Check the installed state without writing any file:

```bash
python3 ./scripts/lingtai-workbench/sync_workbench_mcp.py \
  --project /path/to/lingtai-project \
  --check
```

The check validates the lock, immutable binary and build identity, LingTai
registry and `init.json`, launch arguments, and the live 18-tool contract. It
uses the same automatic Agent selection as setup. Add the exact `--agent`
directory name only when the project is ambiguous:

```bash
python3 ./scripts/lingtai-workbench/sync_workbench_mcp.py \
  --project /path/to/lingtai-project \
  --agent 'coordinator(codex-gpt-5.4)' \
  --check
```

## Handle a Failed Update

Do not hand-edit the LingTai MCP files and do not run `/refresh` after a failed
handoff. Fix the reported cause and rerun the same `up.sh` command. The sync
transaction recovers an interrupted Agent-file update on the next normal run.

Common failures:

- **No Agent or ambiguous Agent:** create the Agent first, or set
  `LINGTAI_WORKBENCH_AGENT` to one complete directory name from the error.
- **Missing `nokv-workbench` skill:** update the LingTai runtime; registering
  NoKV cannot install or patch the skill.
- **Dirty NoKV checkout:** commit or stash the changes. Dirty builds are for
  explicit maintainer testing, not a downstream update.
- **Port already owned by an unknown process:** inspect it with
  `lsof -nP -iTCP@127.0.0.1:7799 -sTCP:LISTEN`. The helper deliberately refuses
  to stop a server it cannot prove it owns.
- **macOS reports that exact process argv cannot be proved:** keep the NoKV,
  LingTai project, metadata, and local RustFS paths free of whitespace. The
  managed-server gate fails closed when `ps` cannot represent an argument
  unambiguously.
- **RustFS or bucket failure:** verify the configured endpoint with the AWS CLI
  and inspect `docker logs lingtai-workbench-rustfs` for the default local
  container.
- **Workbench schema changed:** review the reported canonical contract change.
  The error prints the exact new SHA-256. Accept only that reviewed digest:

  ```bash
  LINGTAI_WORKBENCH_ACCEPT_CONTRACT_SHA256=<new-digest-from-error> \
  LINGTAI_WORKBENCH_PROJECT=/path/to/lingtai-project \
  LINGTAI_WORKBENCH_META_DIR="$HOME/.local/share/nokv/lingtai-workbench/meta" \
  LINGTAI_WORKBENCH_RUSTFS_DATA_DIR="$HOME/.local/share/nokv/lingtai-workbench/rustfs" \
  ./scripts/lingtai-workbench/up.sh
  ```

  This is not a Boolean bypass: any other or later digest still fails.

Use these files when diagnosing a failure:

- server log, by default:
  `/path/to/NoKV/target/lingtai-workbench/nokv-server.log`;
- selected Agent lock:
  `/path/to/lingtai-project/.lingtai/<agent>/nokv-workbench.lock.json`;
- interrupted transaction marker, when present:
  `/path/to/lingtai-project/.lingtai/<agent>/.nokv-workbench.transaction.json`.

Do not delete an interrupted transaction marker by hand; rerun the normal
update so the helper can recover it.

## Downgrade and Recovery Boundary

The immutable runtime directory and lock protect identity; they are not a
general rollback manager. Prefer fixing the issue and moving forward to a
known-good NoKV main revision.

After the first durable restore operation activates `restore_to_fork_v1`, the
persistent metadata contains an active marker and allocator downgrade fence.
Never start a pre-restore NoKV metadata binary against that metadata directory.
A safe downgrade requires disabling restore routing, globally stopping or
fencing restore writers, using the typed drain procedure, running a clean full
fsck, and creating a fresh metadata checkpoint. This is an operator procedure,
not a normal LingTai update; see [Architecture](architecture.md) and involve the
NoKV maintainers.

For script internals, Release artifact identity, and manual layer-by-layer
diagnostics, use the
[LingTai Workbench maintainer reference](../scripts/lingtai-workbench/README.md).
