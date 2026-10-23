<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Durable Append Qualification Record

Status: feature-scoped safety and completion evidence. This record covers the
local qualification finalized on 2026-09-23 (Australia/Sydney) and remote CI
on 2026-09-24. It is not a whole-platform production or performance approval.
The [user guide](../append.md) explains the API; the
[product specification](./append-product-spec.md) defines its contract.

## Source And Profile

The tested NoKV source is
[`13590c1cc12300d79396ea902a06b2dbb010a75b`](https://github.com/NoKV-Lab/NoKV/commit/13590c1cc12300d79396ea902a06b2dbb010a75b).
The feature-branch commit that integrates main,
[`3b83f8a216edb8ae09f79b77a22f746a386dd79f`](https://github.com/NoKV-Lab/NoKV/commit/3b83f8a216edb8ae09f79b77a22f746a386dd79f),
has the identical Git tree, `2ce0d3728d50a767da314528daac53cbfac10d04`.
This is source equivalence, not a claim that the frozen binary was built from
the later commit.

The local gates used a clean source tree, matching CLI and installed Python
extension, macOS arm64, one local Holt authority, isolated etcd 3.7.1, and real
RustFS. The linked dependency was registry **Holt 0.8.6**, not a local Holt
checkout. The RustFS image was pinned to
`rustfs/rustfs@sha256:e620d37756fff072b10bf648c7bb9d370d7e91a928b7e6a5e1ac85bdfb4e4dab`.
The tested schema is RPC v12, system format 13, publication value format 7.

Fault tests explicitly used the supported 1,000 ms append activity lease plus
the normal 30-second clock grace, 90-second completion and 120-second command
budgets, and 100 independent CLI replays. These budgets and measured durations
are not the default 30-minute lease or a recovery SLA.

## Local Fault Qualification

| Execution | Result | Evidence scope |
| --- | --- | --- |
| [Core product gate](../../scripts/workbench/append_product_acceptance_gate.py), 21 scenarios | Safety and completion `PASS`; all listed scenarios executed. | Same/different logical identities, complete intent conflicts, cross-root isolation, deterministic admission races, caller death at five observed publication boundaries, owner death/reopen, provider PUT response loss and read corruption, late PUT blocked by a zero-byte seal, size/block/batch boundaries, 100 replays, metadata-only history, CLI/Python parity, and real incompatible wire/store rejection without mutation. |
| [Public operations gate](../../scripts/workbench/append_operations_acceptance_gate.py), four scenarios | Safety and completion `PASS`; all listed scenarios executed. | Public inspection and owner recovery; 257 registered keys with a 32-row cleaned prefix; CLI/Python pages and concurrent recover; recovery ACK loss, owner reopen, repeated quarantine, successor attempts and stale former-child cursors; owner death after a real seal PUT succeeded but before its response returned. |

The core gate's public quarantine scenario also runs in the operations gate.
These are **21 + 4 executions, not 25 distinct requirements**. Required
completion is checked alongside at-most-once effect, exact ordered bytes,
generation changes, stable receipt fields, and relevant provider inventory.
After a successor is published, an old inspection cursor is rejected while
the old saved recovery digest still replays its original admission receipt.

Workspace deletion or incarnation replacement during old-child cleanup is
covered by metadata integration tests. It is not claimed as public lifecycle
E2E: there is no corresponding public deletion/rebinding endpoint in this
qualification. The historical
[identity gate](../../scripts/workbench/append_identity_recovery_gate.py)
reproduces the earlier duplicate-effect defect and preserves the narrower
single-attempt design's evidence; it cannot replace the current completion
gates. Commands and prerequisites are in the
[runner guide](../../scripts/workbench/README.md#stable-append-gates).

The local raw logs, databases, frozen artifacts, and JSON receipts are retained
by the project but are **not published in this repository or as public CI
artifacts**. Their recorded identities are:

| Local receipt or artifact | SHA-256 |
| --- | --- |
| `product-final-13590c1/product-results.json` | `b458994291406dc001f63d9a1fff39edac81d3c2f9663dd981273201d9a2958c` |
| `operations-final-13590c1/operations-results.json` | `44aa7777ed156473f216236ccf23de1d850b98b8eee38082d675643ead5695da` |
| Frozen CLI | `19b6c785ad9fe3a3c78d4409bc76f56dbf4a69f76b6bc2a9e5346bc10eb7b774` |
| Installed Python native extension | `43778cbc080ef099ac545dec4a804f7c2a8e53db1f61858a46d17378ce45ac21` |

Each gate's result identifies its executed script and source hash separately
from `environment.json`, which identifies the shared identity-gate stack
helper. These hashes locate retained evidence; they do not substitute for
access to the raw records or an independent run.

## Remote NoKV CI

The workflows below tested the same `13590c1` source tree. Their test merge
checkout and branch tree were verified equal.

| Run | Result | Scope |
| --- | --- | --- |
| [Rust](https://github.com/NoKV-Lab/NoKV/actions/runs/35992447765) | `PASS` | Default workspace: 1,158 passed, 11 ignored. Diagnostics feature: 429 test executions passed; restore-crash feature: 188 passed. Formatting, clippy, contract checks, and existing local-WAL, live Workbench, object namespace, restore composition, and fork/restore gates passed. |
| [Python SDK](https://github.com/NoKV-Lab/NoKV/actions/runs/35992447625) | `PASS` | Release `manylinux_2_28_x86_64` wheel built and installed into a clean Python 3.12 environment; 34 SDK tests passed. |
| [Docker](https://github.com/NoKV-Lab/NoKV/actions/runs/35992447760) | `PASS` | amd64 and arm64 image builds; this is not arm64 runtime fault qualification. |

Feature test counts overlap and must not be added as unique tests. The 11
ignored tests are not counted as executed successes. These NoKV workflows
**did not execute the new complete append 21 + 4 fault matrix**. Existing
Workbench runs through the deprecated sidecar do not qualify the corresponding
native CLI/Python 18-tool path. The pre-#423 aggregator's deliberate
`NOT QUALIFIED` result is also not erased by a successful CI job.

## Real Downstream Queue Redelivery

The demo's
[push CI](https://github.com/NoKV-Lab/nokv-demo/actions/runs/35993046954) and
[PR CI](https://github.com/NoKV-Lab/nokv-demo/actions/runs/35993047249)
tested demo commit `13cc33cd733b024591620f8e69a35c153bb97aba`, pinned to NoKV
`13590c1`. **The demo repository is private; these links and its retained CI
evidence require repository access.** They are not public download links.

Each run passed 434 Python tests plus three subtests, 75 Node tests, TypeScript
checks, and a real Linux consumer redelivery gate. The gate killed the actual
consumer after NoKV committed but before its local queue acknowledgement:

- The legacy invocation was redelivered and grew the body from **13 to 26
  bytes**, reproducing the duplicate effect.
- A new process read the stable event from SQLite outbox and redelivered the
  same complete intent and id. The body remained **13 bytes**, with all ten
  immutable append receipt fields equal. Separate ids produced independent
  effects, and the same id with changed intent was rejected.

The test exercised the real Runtime/Storage/native CLI integration and
independent byte verification, without paid model calls or a cloud sandbox.
Its source and runner are documented in the access-restricted
[demo redelivery guide](https://github.com/NoKV-Lab/nokv-demo/blob/13cc33cd733b024591620f8e69a35c153bb97aba/docs/APPEND_REDELIVERY.md).
This demonstrates the tested consumer's durable queue boundary, not every
agent framework or execution environment.

## Remaining Qualification Boundaries

- Full [Workspace Acceptance](./workspace-acceptance.md), including Gate 0's
  complete native/Python 18-tool and snapshot-expiry evidence, remains separate.
- Process SIGKILL and same-directory Holt reopen do not qualify cross-host HA,
  disk loss, physical power failure, or every internal WAL/fsync boundary.
- Rejection of previous wire/store formats is qualified; migration, mixed
  writers, and cross-format rollback are not.
- Receipt, failed-revision reservation, zero-byte guard, and outbox retention
  have no long-term capacity or high-cardinality GC qualification. Guards must
  survive external object deletion/overwrite/expiration; versioned-bucket
  historical payload retention needs its own evidence.
- The tested RustFS endpoint does not qualify all S3-compatible providers.
  Throughput, long-running load, tail latency, and workload SLOs need separate
  measurements; both local append gates report performance `NOT QUALIFIED`.

Original red evidence and intermediate harness/setup failures remain retained.
This dated record does not rewrite them, change the frozen qualification
ledger, or assert a current PR approval or merge status.
