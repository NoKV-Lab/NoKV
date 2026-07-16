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
workbench_append
workbench_edit
workbench_list
workbench_stat
workbench_read
workbench_grep
workbench_search
workbench_aggregate
workbench_catalog
workbench_find
workbench_commit
workbench_snapshot
workbench_snapshot_renew
workbench_snapshot_retire
workbench_snapshot_list
workbench_restore
```

The server registration name remains `nokv-workbench`; that is the MCP server
id used by LingTai, not the public tool-name prefix.

## File Publication And Structured Read Contract

`workbench_put_file` has two exclusive modes; it is not upsert:

- `replace=false` (the default) is create-only and fails when the target
  already exists.
- `replace=true` is replace-only and fails when the target is missing. Use it
  only when replacing an existing whole file intentionally.

`workbench_append` is a separate operation and creates its target when missing.
Do not use `replace=true` as a create fallback after a speculative write: an
exists/not-found race is a coordination conflict that the caller must observe.

Live `workbench_read` with `format="structured"` parses JSON, YAML, and UTF-8
text records. It does not natively parse `application/x-ndjson`, and there is
no NDJSON record-pagination contract. A `.jsonl` suffix alone does not select a
parser: store the file with a `text/*` content type to receive raw
`record_type="text_lines"` records and parse each `value.text` yourself. For
`application/x-ndjson` or any other unsupported content type, use
`format="bytes"`. At a snapshot, non-bytes reads expose UTF-8 text lines as a
snapshot-specific raw-text mode; that is still not an NDJSON parser.

## Commit Identity Contract

`workbench_commit` publishes `metadata/run_manifest.json` with schema
`nokv.workbench.run_manifest.v1`. The call requires `content_digest_uri` in the
exact form `sha256:<64 lowercase hex>`. LingTai must compute this digest before
the call from the job outputs or another stable, application-owned content
description; a phase label alone is not content identity.

NoKV separately hashes the compact canonical JSON manifest (recursively sorted
object keys, array order preserved) as `manifest_digest_uri`, then derives
`commit_identity` from the workbench id, content digest, and manifest digest
under the `nokv.workbench.commit_identity.v1` domain. The server timestamp is
not part of either identity.

The identity byte stream is unambiguous and portable: start with
`b"nokv.workbench.commit_identity.v1\0"`, then append the workbench id,
`content_digest_uri`, and `manifest_digest_uri` in that order, each prefixed by
its unsigned 64-bit big-endian UTF-8 byte length. `commit_identity` is the
lowercase `sha256:` URI of that complete stream.

An exact retry returns the existing commit with `idempotent_replay=true`,
including after a committed response was lost. A different identity returns
`WorkbenchCommitConflict`, even when both manifests have the same phase.
Replacing a different commit or upgrading a legacy v0 manifest requires an
explicit `replace=true`; a concurrent identity change still fails closed.
Legacy v0 manifests remain readable by `workbench_find`, but they never count
as an identity match.

## Snapshot Annotation Contract

`workbench_snapshot` accepts optional `reason` and `metadata` fields. `reason`
is a non-empty human-readable string bounded to 256 Unicode characters and
1024 UTF-8 bytes. `metadata` is a JSON object bounded to 4096 canonical bytes,
8 container levels, and 64 object keys across the complete value. The returned
`annotation` is also preserved by checkpoint list and renew responses; these
fields are not encoded into the 64-character checkpoint name.

Annotations live in the workbench checkpoint registry, which is appended after
the authoritative snapshot pin is created. If that append fails, the MCP call
returns typed `SnapshotRegistryWritePartial` with the created snapshot id,
lease, annotation, and explicit retry/retire compensation. It does not report a
success that falsely claims the annotation is discoverable.

## Snapshot Retirement Contract

`workbench_snapshot_retire` is the MCP lifecycle endpoint for releasing a
checkpoint. Pass `id` and exactly one of `snapshot_id` or `name`; an optional
bounded `reason` records why retirement was requested. The operation calls the
existing path-bound metadata retirement API, so a foreign-root snapshot or a
snapshot whose fork retention is still active remains a typed error.

The operation is idempotent. The call that removes the pin returns
`retired=true`; an exact retry after the pin is already absent succeeds with
`retired=false`. NoKV never upgrades that false outcome into a fabricated
deletion. The checkpoint registry records retire lifecycle events, and
`workbench_snapshot_list` reports `state=retired` only when it has an
acknowledged `retired=true` event. An absent pin without that proof remains
`state=reaped`. The base surface therefore has 17 tools, or 18 when
`workbench_restore` is capability-enabled.

## Tests

Run the installer tests with:

```bash
python3 ./scripts/lingtai-workbench/install_workbench_mcp_test.py
python3 ./scripts/lingtai-workbench/durable_restore_live_e2e_test.py
python3 -m py_compile ./scripts/lingtai-workbench/durable_restore_live_e2e.py
bash -n ./scripts/lingtai-workbench/up.sh
```

## Durable Restore Live Gate

`durable_restore_live_e2e.py` is the merge gate for durable workbench
restore-to-fork. It starts an isolated RustFS container, a request-counting S3
proxy, `nokv serve`, and disposable LingTai Agent registrations. The Agent MCP
is deliberately disconnected and recovered before use. The concurrency gate
then launches 16 independent MCP processes plus a seventeenth observer from the
same Agent-resolved launch contract.

Run the complete gate from the LingTai companion checkout's uv environment:

```bash
uv run --project /path/to/lingtai-kernel \
  python /path/to/NoKV/scripts/lingtai-workbench/durable_restore_live_e2e.py \
  --lingtai-kernel-dir /path/to/lingtai-kernel \
  --profile full \
  --require-all
```

The full gate always uses a 1 GiB sparse-but-real binary fixture. Every 4 MiB
block contains a distinct deterministic marker; the upload must expose exactly
256 independent 4 MiB RustFS objects and the final remote digest must match. It validates
the capability-gated raw restore schema, numeric exact retry, first-visible
manifest and checkpoint-registry removal, 16-way idempotency, COW PUT counts,
durable crash recovery after hold, every dynamically discovered materialization
and exact-reference batch (including the first absent phase as a bounded
termination proof), initialization PUT-before and PUT-after, reference seal,
index seal, and attach apply-before-ACK. Every crash after the initialization
PUT but before attach must observe the old incarnation,
publish its durable cleanup tombstone, delete the old-incarnation object, and
only then PUT the rebuilt manifest under a fresh incarnation key. The permanent
tombstone stays eligible for repeated sweeps so an arbitrarily late old-owner
PUT cannot become untracked or delete the rebuilt manifest. It also crashes
during bounded cleanup and paged release,
then validates server
kill/replay, Agent reconnect, `search`/`aggregate`/`catalog`, nested restore,
source pin retirement and deletion, root move and rename-replace, and
escaped-borrower retention followed by final exact-reference object release.
Search is consumed through every cursor page; rename/delete/publish and final
release must leave no query ghosts. A deterministic pre-attach barrier proves
that stat and all three query surfaces remain hidden before the visibility
pointer flips. The live Agent's actual restore handler resends the same numeric
request after MCP reconnect. Restore metrics and strict object/restore fsck must
show one private Complete graph after the 16-way call; after release its
operation/member/exact-reference/index/release rows must return to the measured
durable-ledger baseline with no backlog or quarantine. All transient graph rows
must be zero; permanent initialization tombstones and their round-robin cursor,
plus the release cursor, must retain exactly their pre-restore row counts. The durable
`restore_to_fork_v1_active` marker and allocator-v2 downgrade fence must remain
present (they are removed only by the explicit downgrade-drain protocol and are
not leaks). Pin retirement races explicit object and history GC, demonstrates
zero remaining pins/ForkBindings, and preserves the borrower. The final RustFS
inventory must equal the exact initial inventory.
`--require-all` has no skip path: missing Docker, AWS CLI, LingTai dependencies,
the restore capability, a stale/unbuilt binary, a changed binary hash, or any
scenario fails the command. The JSON summary records NoKV/LingTai revisions and
the exact launched NoKV binary SHA-256.

The barrier protocol is explicitly test-only. The gate sets
`NOKV_TEST_RESTORE_BARRIER_DIR` on `nokv serve`, arms
`<operation-id>.<phase>.arm`, waits for the server's `.ready` marker, sends
SIGKILL, removes all markers, reopens the same metadata WAL and RustFS bucket,
and resends the same numeric request. Every completed create must expose exactly
one root and one live manifest object (a post-PUT crash necessarily records one
discarded old-incarnation PUT as well); released operations must leave no object
references behind.

The metadata HA smoke also holds a post-checkpoint restore at `index-sealed`,
fails owner A over through etcd, and explicitly redrives the same operation on
owner B. It uses a per-run RustFS bucket (including with external RustFS),
requires strict JSON fsck for both object references and the unique two-Complete
restore graph, and removes the bucket on exit:

```bash
NOKV_HA_STALE_OWNER_CHAOS=1 ./scripts/run-metadata-ha-smoke.sh
```

The live public surface exercises the canonical path index and built-in query
fields. No MCP or CLI API currently registers custom `PathIndexCatalog` rows,
so custom-row overlay and replay coverage belongs in the `nokv-meta`
integration suite; this live gate does not claim to exercise that private
registration interface.

For local iteration only, use `--profile quick`; it keeps the non-crash
contract, idempotency, restart, indexing, and object-lifecycle assertions with
a 16 MiB binary fixture, but omits the expensive crash matrix. Add
`--keep-state` when diagnosing a failure.
