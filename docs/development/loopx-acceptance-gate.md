<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# LoopX acceptance gate for `experimental/metadata-runtimes`

`experimental/metadata-runtimes` removes the etcd control path and replaces it
with the `holt:///` and `fdb:///` metadata runtimes ([#500](https://github.com/NoKV-Lab/NoKV/pull/500)).
`main` keeps the etcd control path because it is the qualified backend of a
downstream partner: LoopX's shared-goal-authority work pins NoKV SDK `0.11.0`
/ `API_VERSION 1` and keys its environment gates and client configuration on
the etcd control path. This page records the promotion gate that was stated
on #500 so that it is enforced by review, not remembered from a comment.

## The gate

1. The series is integrated into `experimental/metadata-runtimes` first, never
   directly into `main`.
2. The one mandatory condition for merging the branch into `main` is
   production-grade acceptance by LoopX:
   - LoopX's NoKV live rows, `s0.nokv_live_matrix` (Stage 0 NoKV matrix) and
     `s2a.nokv_live_qualification` (Stage 2A live qualification) under
     `examples/shared-goal-authority-e2e`, pass against a build of this branch;
   - the LoopX-side configuration change that makes those rows run against the
     branch is reviewed and merged by the LoopX owner;
   - there is no regression in the SDK `0.11.0` / `API_VERSION 1` wire contract
     LoopX pins.
3. Green internal FDB gates are necessary, not sufficient.

## What the branch changes underneath the pinned contract

The wheel built from this branch still reports `__version__ == "0.11.0"` and
`API_VERSION == 1`, but it is not the same contract:

| Surface | `main` (0.11.0 release) | this branch |
| --- | --- | --- |
| `nokv.RoutingConfig` | `etcd(...)`, `static(...)` | `seeds([...])` only; numeric `IP:port`, no hostnames |
| `WORKSPACE_PROTOCOL_SCHEMA` | `nokv.workspace.rpc.v9` | `nokv.workspace.rpc.v10`; a mismatched handshake fails closed |
| CLI routing | `--etcd-endpoint`, `--etcd-key-prefix`, `--metadata-create` | `--seed IP:PORT`, `--meta-url holt:///...` or `fdb:///...` |
| `Client`, `publish_bytes`, `read`, `find_workspaces`, `ObjectStoreConfig` | unchanged | unchanged |

Consequences the gate has to account for:

- A `0.11.0` wheel cannot talk to a branch server, and a branch wheel cannot
  talk to a `main` server. Wheel and binary must be upgraded together.
- A version guard that compares only `__version__` and `API_VERSION` cannot see
  the break. The wheel exports `nokv.WORKSPACE_PROTOCOL_SCHEMA` and
  `nokv version --json` reports `workspace_protocol_schema` so a deployment can
  compare the two strings before connecting.
- The LoopX helper builds its client from `RoutingConfig.etcd(...)`. Until it
  accepts a seeds routing kind, the rows cannot run against this branch at all;
  that helper change is the "LoopX-side configuration change" in the gate.
- Whether the Python surface change (`etcd`/`static` removed) warrants
  `API_VERSION 2` and a `0.12.0` release is decided in the promotion PR, not
  here. Until then every evidence record must say that `0.11.0` / `API 1` on
  this branch is nominal and only the wheel SHA-256 and the schema string
  distinguish it from the release.

## Evidence procedure

Build the candidate from the exact branch commit under test:

```bash
cargo build --release -p nokv --bin nokv
(cd crates/nokv-python && maturin build --release --out ../../dist)
python3.12 -m venv /tmp/nokv-candidate && /tmp/nokv-candidate/bin/pip install dist/nokv-*.whl
```

Bring up a disposable Holt-backed owner with an admitted S3-compatible object
store (the same object store that qualified `main` may be reused with a fresh
bucket or prefix):

```bash
nokv format --meta-url holt:///abs/path/meta
nokv --root-id "$NOKV_ROOT_ID" --agent-id "$NOKV_AGENT_ID" \
  --object-bucket "$NOKV_BUCKET" --object-endpoint "$NOKV_OBJECT_ENDPOINT" \
  provision --meta-url holt:///abs/path/meta
nokv --advertise-endpoint 127.0.0.1:7750 \
  --object-bucket "$NOKV_BUCKET" --object-endpoint "$NOKV_OBJECT_ENDPOINT" \
  serve --meta-url holt:///abs/path/meta
```

Run the LoopX rows three times and keep all three reports:

1. Baseline: LoopX `main` with the `0.11.0` release wheel against a `main`
   NoKV owner (etcd routing). Proves the rows themselves are green.
2. Candidate: the LoopX change under review with the branch wheel against the
   branch owner (seed routing). This is the gate.
3. Regression: the same LoopX change with the `0.11.0` release wheel against
   the `main` owner. Proves the LoopX change did not break the pinned path.

Also record one negative pairing: the `0.11.0` release wheel pointed at the
branch owner must fail the handshake closed, and the row must report that as
unavailable rather than as a pass.

## Evidence record

An acceptance record is posted on #500 and linked from the LoopX pull request.
It carries, with no credentials or endpoint values:

- branch commit, `nokv` binary SHA-256, `nokv version --json` output;
- wheel filename and SHA-256, `nokv.WORKSPACE_PROTOCOL_SCHEMA`;
- number of seeds and the object store identity class (provider, bucket count);
- the three LoopX ladder JSON reports and their `summary` lines;
- the negative-pairing transcript;
- the "not proven" list below, restated for that record.

Result classes are the ones in [Workspace acceptance](workspace-acceptance.md):
`PASS` only when every row ran and passed with the retained evidence above;
`FAIL` when a row ran and reported a contract violation; `NOT QUALIFIED` when a
row could not run.

## Roles

- The NoKV maintainer who holds the LoopX-side review scope runs the rows,
  posts the record and owns the LoopX helper/ladder change.
- The LoopX owner reviews and merges the LoopX-side change; that merge is part
  of the gate, not a formality after it.
- The branch author keeps the internal FDB and Holt gates green; those gates
  are inputs to the record, not substitutes for the LoopX rows.

## Not proven by this gate

- The FDB runtime. The LoopX rows exercise one Holt-backed owner; `fdb:///`
  stays `NOT QUALIFIED` by this gate.
- Seed failover. The rows use one seed; multi-seed discovery and failover are
  covered only by the branch's own qualification documents.
- Availability, restart, takeover, capacity and retention of the authority
  envelope. Those are separate LoopX qualification lanes.
- Real cloud S3. The rows run against an S3-compatible local provider.
- Pre-connect detection of a wire mismatch by the LoopX version guard. The
  schema export makes detection possible; adopting it is the LoopX change.

## Status

Defined on 2026-09-19. No acceptance record exists yet; the LoopX-side change
and the first candidate run are tracked from #500.
