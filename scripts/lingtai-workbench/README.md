<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# LingTai Workbench Maintainer Reference

The single user-facing setup and update path is
[`docs/lingtai-workbench-preflight.md`](../../docs/lingtai-workbench-preflight.md).
Do not duplicate that tutorial here. This page documents the scripts, artifact
contract, manual diagnostics, and acceptance gates used to maintain the
NoKV-to-LingTai Workbench handoff.

These helpers target LingTai. Historical benchmark or Yanex paths must not be
used to configure the Workbench MCP.

## Script Responsibilities

| Script | Responsibility |
| --- | --- |
| `up.sh` | Environment-only orchestration for one guarded update. It performs offline Agent preflight, prepares an immutable runtime, probes a candidate before replacing a running server, starts or verifies RustFS and the helper-owned metadata server, rechecks the live contract, and switches the Agent registration. It accepts no CLI arguments. |
| `sync_workbench_mcp.py` | Lower-level source build or artifact staging, content-addressed runtime selection, offline/live preflight, per-Agent locking, journaled registry update, and read-only lock verification. |
| `nokv_runtime.py` | NoKV/Holt/Cargo.lock identity, artifact-bound build-info parsing, SHA-256 verification, and symlink-safe immutable runtime staging. |
| `managed_nokv_server.py` | Records and verifies the helper-owned server PID, process start identity, listener ownership, binary digest, complete argv, metadata path, and object-store configuration before reuse or termination. |
| `workbench_contract.py` | Semantic validation and evidence for the exact 17-tool Workbench surface. |
| `workbench_contract_schema.json` | Checked-in canonical `inputSchema` snapshot owned by `workbench_mcp.rs`. |
| `generate_nokv_build_info.py` | Produces `nokv.build_info.v1` for a Release or future Brew artifact. |
| `install_workbench_mcp.py` | Raw idempotent registry primitive. It intentionally performs no binary, owner, capability, or schema gate. |
| `start_rustfs.sh` | Starts or reuses the dedicated local RustFS container and creates the selected bucket. |
| `durable_restore_live_e2e.py` | Real RustFS, NoKV, LingTai reconnect, crash/replay, COW, index, and lifecycle merge gate. |

The adjacent `*_test.py` files cover the corresponding Python module or
script. They are maintainer tests, not downstream installation steps.

## Runtime and Lock Layout

The selected binary is copied before registration to:

```text
<project>/.lingtai/runtime/nokv/<nokv-commit>/<binary-sha256>/nokv
```

Its artifact-bound `build-info.json` is stored beside it. The selected Agent
then owns:

```text
<agent>/mcp_registry.jsonl
<agent>/init.json
<agent>/nokv-workbench.lock.json
<agent>/.nokv-workbench.sync.lock
<agent>/.nokv-workbench.transaction.json   # present only during/recovering a write
```

The lock records the binary digest and size, NoKV commit, `Cargo.lock` digest,
Holt commit, launch arguments, concrete Agent root, and canonical MCP contract
evidence. A rebuild or package upgrade cannot replace the registered binary in
place.

The helper's default process state is below
`<NoKV checkout>/target/lingtai-workbench`. Metadata is durable product state,
not process scratch: deployments must override `LINGTAI_WORKBENCH_META_DIR` to
a persistent location and keep that location stable across updates.

## Environment Reference

`up.sh` accepts no positional or option arguments. Its primary environment is:

| Variable | Default | Meaning |
| --- | --- | --- |
| `LINGTAI_WORKBENCH_PROJECT` | current project with `.lingtai`, then `~/lingtai-demo` | LingTai project to update. |
| `LINGTAI_WORKBENCH_AGENT` | automatic selection | Exact directory name below `.lingtai`; set only after an ambiguity error. |
| `LINGTAI_TUI_PYTHON` | `~/.lingtai-tui/runtime/venv/bin/python` | Python used to verify the intrinsic skill. |
| `LINGTAI_WORKBENCH_META_DIR` | `target/lingtai-workbench/meta` | Holt metadata directory. Production/downstream use must override this with persistent storage. |
| `LINGTAI_WORKBENCH_SERVER_BIND` | `127.0.0.1:7799` | Metadata RPC listen/client address. |
| `LINGTAI_WORKBENCH_SERVER_LOG` | `target/lingtai-workbench/nokv-server.log` | Helper-managed server log. |
| `LINGTAI_WORKBENCH_SERVER_PID` | `target/lingtai-workbench/nokv-server.pid` | Helper-managed process id. |
| `LINGTAI_WORKBENCH_SERVER_STATE` | `target/lingtai-workbench/nokv-server.json` | Managed server identity and launch state. |
| `LINGTAI_WORKBENCH_ROOT` | `/agents/{agent_id}/wb` | Per-Agent Workbench root template. |
| `LINGTAI_WORKBENCH_OBJECT_BACKEND` | `rustfs` | NoKV object backend. |
| `LINGTAI_WORKBENCH_S3_ENDPOINT` | `http://127.0.0.1:9000` | S3-compatible endpoint. |
| `LINGTAI_WORKBENCH_S3_BUCKET` | `nokv-lingtai-workbench` | Object bucket. |
| `LINGTAI_WORKBENCH_S3_ACCESS_KEY_ID` | `rustfsadmin` | Lower-level RustFS bootstrap credential. `up.sh` rejects a non-default value because custom credentials are not propagated into the LingTai MCP registration. |
| `LINGTAI_WORKBENCH_S3_SECRET_ACCESS_KEY` | `rustfsadmin` | Lower-level RustFS bootstrap credential. `up.sh` rejects a non-default value because custom credentials are not propagated into the LingTai MCP registration. |
| `LINGTAI_WORKBENCH_ACCEPT_CONTRACT_SHA256` | unset | Accept exactly one reviewed new canonical schema digest. It is not a Boolean bypass. |
| `LINGTAI_WORKBENCH_ALLOW_DIRTY` | `0` | Set to `1` only for an explicitly dirty local maintainer build. |

Local RustFS-specific controls are:

| Variable | Default |
| --- | --- |
| `LINGTAI_WORKBENCH_DATA_ROOT` | `target/lingtai-workbench` |
| `LINGTAI_WORKBENCH_RUSTFS_DATA_DIR` | `<data-root>/rustfs` |
| `LINGTAI_WORKBENCH_RUSTFS_CONTAINER` | `lingtai-workbench-rustfs` |
| `LINGTAI_WORKBENCH_RUSTFS_IMAGE` | `rustfs/rustfs:latest` |
| `LINGTAI_WORKBENCH_RUSTFS_HOST` | `127.0.0.1` |
| `LINGTAI_WORKBENCH_RUSTFS_PORT` | `9000` |
| `LINGTAI_WORKBENCH_RUSTFS_CONSOLE_PORT` | `9001` |

An external Release or future Brew artifact is passed through `up.sh` with:

| Variable | Meaning |
| --- | --- |
| `NOKV_BIN` | Exact packaged native executable. |
| `NOKV_BUILD_INFO` | Matching artifact-bound `nokv.build_info.v1`; mandatory with `NOKV_BIN`. |
| `NOKV_REVISION` | Optional expected full 40-character NoKV commit. |
| `NOKV_DISTRIBUTION` | Optional `release`, `brew`, `source`, or `path` label recorded in the lock. |
| `NOKV_EXPECTED_SHA256` | Optional checksum from an independently trusted release channel. |

## Lower-Level Source Handoff

`--build-source` is the only trusted lower-level source build path. It runs
`cargo build --locked --release`, checks that source identity did not change
during the build, creates build-info for the exact output bytes, and stages the
binary. It is mutually exclusive with `--nokv-bin` and `--build-info`.

When a compatible metadata server and object store are already running, build,
gate, and switch one Agent directly with:

```bash
python3 ./scripts/lingtai-workbench/sync_workbench_mcp.py \
  --project /path/to/lingtai-project \
  --build-source . \
  --distribution source \
  --server-bind 127.0.0.1:7799 \
  --object-backend rustfs \
  --s3-endpoint http://127.0.0.1:9000 \
  --s3-bucket nokv-lingtai-workbench \
  --workbench-root '/agents/{agent_id}/wb'
```

Omit `--agent` for normal automatic selection. If selection is ambiguous, pass
one exact directory name with `--agent`.

To build and stage without probing or changing the Agent:

```bash
python3 ./scripts/lingtai-workbench/sync_workbench_mcp.py \
  --project /path/to/lingtai-project \
  --build-source . \
  --distribution source \
  --stage-only
```

The only stdout line is the immutable executable path. The sibling
`build-info.json` must travel with that staged executable.

To validate a staged candidate against the current metadata endpoint without
changing Agent files, replace `--stage-only` with `--probe-only` and include the
same server/object-store/workbench-root options as the deployment. `up.sh` runs
this read-only live probe automatically before it replaces an existing server.

## Release Artifact Contract

A Release must build from a clean checkout of the exact advertised commit and
ship both the native executable and matching `nokv.build_info.v1`. Generate the
identity only after the final binary exists:

```bash
cargo build --locked --release -p nokv --bin nokv

python3 ./scripts/lingtai-workbench/generate_nokv_build_info.py \
  --source-root . \
  --revision "$(git rev-parse HEAD)" \
  --nokv-bin ./target/release/nokv \
  --output ./dist/build-info.json
```

The build-info binds the exact binary SHA-256 and size to the NoKV commit,
`Cargo.lock`, and Holt commit. Release installation should place it at
`share/nokv/build-info.json` or otherwise provide its path explicitly.

Exercise a packaged artifact through the same orchestration and live gate:

```bash
LINGTAI_WORKBENCH_PROJECT=/path/to/lingtai-project \
LINGTAI_WORKBENCH_META_DIR=/persistent/path/to/nokv-meta \
NOKV_BIN=/opt/nokv/bin/nokv \
NOKV_BUILD_INFO=/opt/nokv/share/nokv/build-info.json \
NOKV_DISTRIBUTION=release \
./scripts/lingtai-workbench/up.sh
```

Do not call the raw installer as a package post-install hook: owner capability
and live schema still have to be checked at deployment time.

The Brew tap is not published yet. When it is available, the formula must
install the same binary/build-info pair, and downstream activation must still
pass `NOKV_BIN` plus `NOKV_BUILD_INFO` to `up.sh`. npm and pip wrappers are not
the distribution boundary for the native NoKV server.

## Manual Layer-by-Layer Diagnostics

Use these commands to isolate one layer. They are not an alternative user
installation path.

### 1. LingTai Skill and Agent Files

```bash
~/.lingtai-tui/runtime/venv/bin/python - <<'PY'
from pathlib import Path
import lingtai.intrinsic_skills as skills

root = Path(skills.__file__).parent
print((root / "nokv-workbench" / "SKILL.md").exists())
PY

python3 ./scripts/lingtai-workbench/sync_workbench_mcp.py \
  --project /path/to/lingtai-project \
  --preflight-only
```

`--preflight-only` recovers a valid interrupted local transaction when present,
then parses the Agent files and compares an old lock to the checked-in canonical
contract without building or probing.

### 2. RustFS and Bucket

```bash
LINGTAI_WORKBENCH_RUSTFS_DATA_DIR=/persistent/path/to/rustfs \
./scripts/lingtai-workbench/start_rustfs.sh

AWS_ACCESS_KEY_ID=rustfsadmin \
AWS_SECRET_ACCESS_KEY=rustfsadmin \
aws --endpoint-url http://127.0.0.1:9000 s3api head-bucket \
  --bucket nokv-lingtai-workbench
```

### 3. Immutable Source Candidate

```bash
STAGED_NOKV="$(python3 ./scripts/lingtai-workbench/sync_workbench_mcp.py \
  --project /path/to/lingtai-project \
  --build-source . \
  --distribution source \
  --stage-only)"

test -x "$STAGED_NOKV"
test -f "$(dirname "$STAGED_NOKV")/build-info.json"
```

### 4. Metadata Server and Connectivity

Use the exact staged binary and the same durable metadata directory and object
store as the deployment:

```bash
"$STAGED_NOKV" \
  --server-bind 127.0.0.1:7799 \
  --object-backend rustfs \
  --s3-endpoint http://127.0.0.1:9000 \
  --s3-bucket nokv-lingtai-workbench \
  --meta /persistent/path/to/nokv-meta \
  serve
```

From another terminal:

```bash
"$STAGED_NOKV" \
  --server-bind 127.0.0.1:7799 \
  --object-backend rustfs \
  --s3-endpoint http://127.0.0.1:9000 \
  --s3-bucket nokv-lingtai-workbench \
  ls /
```

Exit status zero proves the client can reach both metadata and object storage.

### 5. Raw Workbench Contract

Probe a concrete Agent root, not the literal `{agent_id}` template:

```bash
printf '%s\n' '{"jsonrpc":"2.0","id":1,"method":"tools/list"}' | \
  "$STAGED_NOKV" \
    --server-bind 127.0.0.1:7799 \
    --object-backend rustfs \
    --s3-endpoint http://127.0.0.1:9000 \
    --s3-bucket nokv-lingtai-workbench \
    mcp --profile workbench \
    --workbench-root '/agents/coordinator(codex-gpt-5.4)/wb'
```

The result must contain exactly 17 tools. Compare `inputSchema` semantically to
`workbench_contract_schema.json`; descriptions and JSON Schema annotations do
not affect the contract digest, while missing fields or added restrictions do.

### 6. Gated Registration and Read-Only Verification

After the staged binary is serving successfully:

```bash
python3 ./scripts/lingtai-workbench/sync_workbench_mcp.py \
  --project /path/to/lingtai-project \
  --nokv-bin "$STAGED_NOKV" \
  --build-info "$(dirname "$STAGED_NOKV")/build-info.json" \
  --distribution source \
  --server-bind 127.0.0.1:7799 \
  --object-backend rustfs \
  --s3-endpoint http://127.0.0.1:9000 \
  --s3-bucket nokv-lingtai-workbench \
  --workbench-root '/agents/{agent_id}/wb'

python3 ./scripts/lingtai-workbench/sync_workbench_mcp.py \
  --project /path/to/lingtai-project \
  --check
```

When a reviewed contract transition is intentional, pass the exact digest from
the error as `--accept-contract-sha256 <digest>`. Never add a Boolean force
flag.

### Raw Registration Repair

`install_workbench_mcp.py` only renders and upserts the two LingTai MCP files.
It does not create a lock or validate the runtime. Reserve it for tests or
manual repair where all gates have already been performed:

```bash
python3 ./scripts/lingtai-workbench/install_workbench_mcp.py \
  --agent-dir '/path/to/project/.lingtai/coordinator(codex-gpt-5.4)' \
  --nokv-bin /immutable/path/to/nokv \
  --server-bind 127.0.0.1:7799 \
  --object-backend rustfs \
  --s3-endpoint http://127.0.0.1:9000 \
  --s3-bucket nokv-lingtai-workbench \
  --workbench-root '/agents/{agent_id}/wb'
```

## Operational Failure State

Inspect these files before deleting or restarting anything:

```text
target/lingtai-workbench/nokv-server.log
target/lingtai-workbench/nokv-server.pid
target/lingtai-workbench/nokv-server.json
target/lingtai-workbench/up.lock/
<agent>/.nokv-workbench.sync.lock
<agent>/.nokv-workbench.transaction.json
<agent>/nokv-workbench.lock.json
```

The normal sync and `--preflight-only` take the exclusive per-Agent lock and
recover a valid interrupted transaction. The read-only `--check` refuses while
a transaction is pending; rerun the normal update rather than removing the
marker.

If port `7799` is occupied, identify the listener first:

```bash
lsof -nP -iTCP@127.0.0.1:7799 -sTCP:LISTEN
```

The helper must not terminate an unverified process. For the default local
object store, inspect `docker logs lingtai-workbench-rustfs` and verify the
bucket independently with the AWS CLI.

Once `restore_to_fork_v1_active` exists in metadata, never test an older,
pre-restore metadata server against that directory. The typed global drain,
full fsck, and post-drain metadata checkpoint are a separate controlled
downgrade procedure.

## Tests

Run the focused script suites from the NoKV repository root:

```bash
python3 ./scripts/lingtai-workbench/install_workbench_mcp_test.py
python3 ./scripts/lingtai-workbench/workbench_contract_test.py
python3 ./scripts/lingtai-workbench/nokv_runtime_test.py
python3 ./scripts/lingtai-workbench/managed_nokv_server_test.py
python3 ./scripts/lingtai-workbench/sync_workbench_mcp_test.py
python3 ./scripts/lingtai-workbench/up_test.py
python3 ./scripts/lingtai-workbench/durable_restore_live_e2e_test.py

python3 -m py_compile \
  ./scripts/lingtai-workbench/install_workbench_mcp.py \
  ./scripts/lingtai-workbench/workbench_contract.py \
  ./scripts/lingtai-workbench/nokv_runtime.py \
  ./scripts/lingtai-workbench/managed_nokv_server.py \
  ./scripts/lingtai-workbench/generate_nokv_build_info.py \
  ./scripts/lingtai-workbench/sync_workbench_mcp.py \
  ./scripts/lingtai-workbench/durable_restore_live_e2e.py

ruff check ./scripts/lingtai-workbench
ruff format --check ./scripts/lingtai-workbench
bash -n ./scripts/lingtai-workbench/up.sh
bash -n ./scripts/lingtai-workbench/start_rustfs.sh
```

## Durable Restore Live E2E

The merge gate must run from the LingTai companion checkout's environment:

```bash
uv run --project /path/to/lingtai-kernel \
  python /path/to/NoKV/scripts/lingtai-workbench/durable_restore_live_e2e.py \
  --lingtai-kernel-dir /path/to/lingtai-kernel \
  --profile full \
  --require-all
```

The full profile uses a real 1 GiB sparse fixture and validates the exact raw
MCP contract, LingTai registration and reconnect, 16-way restore idempotency,
COW object PUT counts, crash barriers across materialization/reference/index
and attach phases, metadata checkpoint plus log replay, source retirement,
borrower object lifetime, indexed queries, nested restore, rename/delete/
release cleanup, fsck, and final object inventory. `--require-all` has no skip
path for missing Docker, AWS CLI, LingTai dependencies, capability, or a stale
binary.

For local iteration only:

```bash
uv run --project /path/to/lingtai-kernel \
  python /path/to/NoKV/scripts/lingtai-workbench/durable_restore_live_e2e.py \
  --lingtai-kernel-dir /path/to/lingtai-kernel \
  --profile quick \
  --keep-state
```

The quick profile keeps the non-crash contract, indexing, restart, and object
lifecycle checks with a smaller fixture; it is not merge evidence.

The metadata HA companion gate remains:

```bash
NOKV_HA_STALE_OWNER_CHAOS=1 ./scripts/run-metadata-ha-smoke.sh
```
