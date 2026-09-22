<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# LoopX Stage 2A Stack

Status: test-only bring-up for LoopX's NoKV authority qualification. One owner,
one node, loopback only. It is not a deployment profile.

LoopX qualifies its NoKV `AuthorityStore` candidate with two environment-gated
rows of its shared-goal-authority ladder, `s0.nokv_live_matrix` and
`s2a.nokv_live_qualification` (`loopx/control_plane/testing/authority_e2e_ladder.py`).
Both rows need a serving NoKV owner, an etcd control path, an S3-compatible
object store, an existing workbench, an ignored client configuration in the
exact key shape LoopX's `nokv_jsonl_helper.py` admits, and a fixed set of
environment variables. `scripts/workbench/loopx_stage2a_stack.py` produces all
of that from one command so a LoopX maintainer can run the gate without
reconstructing the recipe by hand.

## Prerequisites

- A `nokv` binary of the release under test: `target/release/nokv` of this
  checkout (`--build` compiles it) or `--nokv-binary PATH`.
- A Python virtual environment with the matching `nokv` wheel installed, for
  example:

  ```bash
  python3 -m venv stage2a-venv
  stage2a-venv/bin/pip install "nokv==0.11.1" \
    --find-links https://github.com/NoKV-Lab/NoKV/releases/expanded_assets/v0.11.1
  ```

  The script refuses a wheel whose `__version__` differs from `nokv --version`,
  and a wheel without `WorkspaceIncarnationMismatch`, because LoopX's helper
  admits only a fenced SDK from the same release as the owner.
- `etcd` and `etcdctl` on `PATH` (`brew install etcd`, or the pinned archive
  NoKV CI installs) or `--etcd-bin` / `--etcdctl-bin`.
- An object store. The default is the digest-pinned RustFS container from
  `scripts/workbench/start_rustfs.sh`, which needs `docker` and the `aws` CLI.
  Without Docker, `--object-store moto` runs a `moto` S3 server from the same
  venv (`pip install "moto[server]" boto3`). A single-disk MinIO does not pass
  NoKV's object admission probe and is not supported here.

## Commands

```bash
# Start everything and create one workbench. The directory must be empty.
python3 scripts/workbench/loopx_stage2a_stack.py up \
  --stack-dir /path/to/stage2a \
  --python /path/to/stage2a-venv/bin/python \
  --loopx-python /path/to/loopx-venv/bin/python

# Print the redacted plan (identities, ports, commands) without starting anything.
python3 scripts/workbench/loopx_stage2a_stack.py plan --stack-dir /path/to/stage2a --python /path/to/stage2a-venv/bin/python

# Liveness of the recorded processes; exit 1 when any is gone.
python3 scripts/workbench/loopx_stage2a_stack.py status --stack-dir /path/to/stage2a

# Stop the owner, etcd, moto or the RustFS container; --purge also deletes the directory.
python3 scripts/workbench/loopx_stage2a_stack.py down --stack-dir /path/to/stage2a --purge
```

`up` writes into the stack directory:

| File | Content |
| --- | --- |
| `nokv-client.json` (0600) | `root_id`, `routing` (`etcd` kind: `endpoints`, `key_prefix`, `lease_ttl_seconds`), `object_store` (`s3` kind: `bucket`, `region`, `root`, `endpoint`, `access_key_id`, `secret_access_key`) and `workbench_root`, exactly the keys the LoopX helper accepts |
| `live.env` (0600) | the `env:nokv_legacy` variables (`NOKV_COORDINATION_LIVE`, `NOKV_ETCD`, `NOKV_ETCD_PREFIX`, `NOKV_ROOT_ID`, `NOKV_BUCKET`, `NOKV_OBJECT_ENDPOINT`, `NOKV_OBJECT_ROOT`, `NOKV_OBJECT_KEY`, `NOKV_OBJECT_SECRET`) and the `env:nokv_authority` variables (`LOOPX_NOKV_AUTHORITY_LIVE`, `LOOPX_NOKV_AUTHORITY_CONFIG_JSON`, `LOOPX_NOKV_AUTHORITY_PYTHON`, `LOOPX_NOKV_AUTHORITY_WORKBENCH`) |
| `stack.json` (0600) | process ids, ports, the binary's SHA-256, the SDK version and digests of the workbench name and configuration; no credentials |
| `next-steps.txt` | the three LoopX commands below |
| `etcd/`, `metadata/`, `rustfs/`, `logs/` | member data, the owner's local WAL, object data, process logs |

stdout carries only digests: the workbench name, the credentials and the etcd
prefix never leave the two 0600 files. Generated names are random with fixed
prefixes (`lx-`, `rt-`, `ak`, `sk`, `wb`, `nd-`), because LoopX's ladder turns
every configuration leaf and environment value into a forbidden token and fails
a report that contains one; `up` refuses to write files whose leaves collide
with ladder vocabulary.

## Running the LoopX gate

From a LoopX checkout whose Python can import `loopx`:

```bash
set -a && source /path/to/stage2a/live.env && set +a
python -m loopx.control_plane.testing.authority_e2e_ladder \
  --row s0.nokv_live_matrix --row s2a.nokv_live_qualification \
  --report-json /path/to/stage2a/ladder-stage2a.json
# The complete ladder; rows whose environment is absent report unverified.
python -m loopx.control_plane.testing.authority_e2e_ladder --allow-unverified \
  --report-json /path/to/stage2a/ladder-full.json
```

A green `s2a.nokv_live_qualification` row proves single-node store conformance
for the SDK release the helper pins, including the stale-incarnation
publication fence (`stale_incarnation_fence_rejected`,
`stale_incarnation_fence_left_generation_unchanged`). The same stack with a
wheel from an older release is the negative pairing: the helper refuses it at
admission and the row fails typed, which is the expected outcome, not a stack
defect.

## What this does not prove

Availability, failover, restart or restore recovery, capacity, multi-owner
operation, production object stores or authenticated transport. Those remain
LoopX qualification holds on the NoKV profile and NoKV's own acceptance gates
(see [Workspace Acceptance](./workspace-acceptance.md)); this script only
removes the manual reconstruction of the single-node stack the two Stage 2A
rows require.
