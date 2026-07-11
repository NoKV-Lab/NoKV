<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Checkpoint/Restore Live E2E

`checkpoint_restore_live_e2e.py` is the checked-in acceptance path for the
NoKV to LingTai workbench checkpoint/restore contract. It starts an isolated
real stack:

```text
RustFS Docker
-> nokv serve (100ms history/object GC workers)
-> nokv mcp --profile workbench
-> LingTai Agent registry/init registration, placeholder expansion, retry, and handler
-> three real LingTai MCPClient sessions from the resolved per-agent launch configs
```

The harness owns unique ports, a unique bucket and container, and a temporary
metadata directory. It always stops the server and removes its RustFS container.
Pass `--keep-state` only when preserving the metadata and logs is useful for
debugging. Every process wait, tool call, HTTP poll, and GC loop has a deadline.

## Run

Use the Python environment from the exact LingTai checkout under acceptance.
For each agent the harness writes real `mcp_registry.jsonl` and `init.json`
inputs, boots `Agent`, captures the registered client's resolved launch config,
closes that client, calls `Agent._retry_failed_mcps()`, and verifies the root is
unchanged. It then invokes the registered `workbench_create` handler before
opening the three independent MCP sessions used by the remaining scenarios.

```bash
~/lingtai-kernel/.venv/bin/python \
  ./scripts/lingtai-workbench/checkpoint_restore_live_e2e.py \
  --lingtai-kernel-dir ~/lingtai-kernel \
  --profile quick
```

The full acceptance counts are:

```bash
~/lingtai-kernel/.venv/bin/python \
  ./scripts/lingtai-workbench/checkpoint_restore_live_e2e.py \
  --lingtai-kernel-dir ~/lingtai-kernel \
  --profile full \
  --require-all
```

`full` runs 100 concurrent 1-day/90-day renew rounds per client, 200
short-lease renew/reaper races, and 101 historical entries with `limit=7`.
`quick` keeps the same assertions with smaller local iteration counts.

Required local commands are `docker`, `aws`, and Cargo. The RustFS image,
ports, NoKV binary, deadlines, and state directory are configurable in
`--help`. Use `--no-build` only with a binary built from the checkout under
test.

While the restore track is being stacked on top of the lease/root-binding and
historical-index tracks, this command validates A+B without claiming restore
coverage:

```bash
~/lingtai-kernel/.venv/bin/python \
  ./scripts/lingtai-workbench/checkpoint_restore_live_e2e.py \
  --lingtai-kernel-dir ~/lingtai-kernel \
  --profile quick \
  --allow-missing-restore
```

The JSON summary marks every unavailable C-only scenario `skipped`. This is a
development-only A+B command. The final integrated acceptance command is
exactly the `--profile full --require-all` command above: `--require-all` is
valid only with `full` and turns any skipped scenario into a failing exit.

## Assertions

The live harness verifies:

- LingTai reads the real registry/init inputs, expands each agent root, recovers
  a deliberately closed registered MCP through `_retry_failed_mcps()`, invokes
  the registered handler, and preserves the resolved roots across restart;
- two concurrent LingTai MCP clients cannot shorten an acknowledged renewal;
- every manual-GC and server-health snapshot-reaper payload satisfies
  `reaped + conflicted == expired_candidates`;
- 100-300ms renew/reaper races include a deterministic
  `scan -> renew -> conditional delete` interleaving, require the background
  reaper health counter's `conflicted` delta to be positive, and retain
  snapshot readability after every successful renew;
- two expanded agent roots using the same workbench id reject foreign numeric
  ids and forged checkpoint names for stat, list, read, and renew with
  `SnapshotRootMismatch`, before and after restart;
- a just-acknowledged checkpoint registry update and its pinned point-read
  survive immediate `SIGKILL` and reopen without relying on destructors or a
  final checkpoint;
- delete, rename, and delete/recreate historical listings agree with point
  reads; every page respects `limit`, truncated pages are non-empty, cursors
  advance, and the full 101-entry/`limit=7` run produces exactly 15 pages with
  no omission or duplicate;
- destination first visibility already has no `metadata/checkpoints.jsonl`,
  already has a structured `metadata/restore_manifest.json`, and leaves the
  source registry unchanged;
- restored nested, renamed, deleted, and current-only paths exactly match the
  checkpoint while the source remains at its current state, and the terminal
  restore response carries the complete typed outcome;
- restore is COW, source-preserving, idempotent across a second MCP client and
  server restart, performs no body PUT beyond the one permitted manifest PUT,
  and rejects another snapshot at the same destination with
  `RestoreDestinationConflict`;
- retiring source checkpoints and deleting the source cannot reclaim
  fork-shared RustFS objects, while deleting the fork eventually releases
  those exact objects.

## Deterministic Live-Test Surfaces and Coverage Boundary

The short-lease race probes `snapshot PATH 200`. An older A+B binary reports a
skip in the development-only mode; the final breaking CLI enables the real
race, and `--require-all` rejects that skip.

Operator in-place rollback, FUSE mount/cache coherence, FUSE writeback journal
replay, and FUSE object-GC restaging are deliberately outside this Workbench
acceptance boundary. They belong to separate operator/FUSE correctness tracks
and must not become implicit dependencies of `workbench_restore`.

The live harness cannot directly enumerate durable fork-base references. Exact
RustFS object-key retention and reclamation provide live, indirect leak
evidence; internal base-reference invariants remain deterministic Rust
recovery/TDD acceptance.

## Static Tests

```bash
python3 ./scripts/lingtai-workbench/checkpoint_restore_live_e2e_test.py
ruff check ./scripts/lingtai-workbench/checkpoint_restore_live_e2e.py \
  ./scripts/lingtai-workbench/checkpoint_restore_live_e2e_test.py
git diff --check
```
