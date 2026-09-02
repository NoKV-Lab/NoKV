<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# FDB Unknown-Outcome Qualification Design

Date: 2026-09-01

Status: approved design; not implemented or qualified.

## Decision

Qualify Gate 2 with deterministic lost-ack injection around the real
FoundationDB C API. Every target transaction must commit successfully to the
real FDB cluster before an external Linux `LD_PRELOAD` shim substitutes one
`commit_unknown_result` (`1021`) for that successful acknowledgement.

The live workload uses the exact production `nokv-fdb` candidate, an isolated
qualification child built from the same clean source where an internal store
method has no CLI entry point, a real FDB cluster, pinned RustFS, fresh
physical prefixes, and independent exact readback. It covers every distinct
NoKV commit and reconciliation path rather than sampling one metadata command
and extrapolating from it. Store-only child cases cannot produce Gate 2 PASS
without the end-to-end candidate bootstrap and ordinary-command cases.

This design follows the package boundaries in
[`code_contract.md`](code_contract.md), the storage-safety checks in
[`pr_review_checklist.md`](pr_review_checklist.md), and the normative Gate 2
boundary in [`metadata-store-interface.md`](metadata-store-interface.md).
Mocks, dry runs, an observed production `1021`, and fail-closed behavior
without deterministic applied-commit proof cannot qualify this gate.

## Scope

This work owns four outcomes:

1. deterministic substitution of one FDB commit acknowledgement after the
   underlying commit has definitely succeeded;
2. exact readback or fail-closed handling for every metadata and distributed-
   control mutation family;
3. retained evidence that an ambiguous acknowledgement never double-applies a
   logical mutation or publishes `Serving` from unproved state; and
4. a clean-candidate live matrix whose complete success is sufficient to move
   Gate 2 from `NOT QUALIFIED` to `PASS`.

The term *mutation family* means a distinct raw-commit, acknowledgement, or
high-level reconciliation path. Metadata command variants that all use the
same sealed `MetadataCommand` transaction and `CommandDedupe` acknowledgement
path are one family. Root-fence install and activation are retained as
separate subcases because bootstrap gives them special high-level readback
semantics. If implementation discovery finds another distinct commit path,
the live matrix must grow before Gate 2 can pass.

## Safety Property

For a logical operation `O`, let `T` be its one raw FDB transaction and `S` the
expected durable state. The qualification path is:

```text
NoKV submits T
  -> real fdb_transaction_commit(T) completes with success
  -> injector reports 1021 exactly once
  -> NoKV receives a typed unknown outcome
  -> NoKV either proves S through exact readback or fails closed
  -> independent readback proves S and the absence of a second apply
```

Returning success is legal only after the exact operation-specific state has
been proved. Returning an ownership error and transferring responsibility to
a successor is also legal when local admission closes first and the successor
replays the same logical request through its durable identity. Blindly
resubmitting the raw transaction is never legal.

The qualification controller additionally proves that:

- the real commit succeeded once;
- the shim substituted exactly one acknowledgement;
- the caller observed the typed unknown outcome;
- the expected durable state exists under the same fence and identity;
- commit clocks, generations, heartbeat sequences, and dedupe records advanced
  only as required by one logical apply; and
- no unproved catalog, owner, or route state became discoverable as `Serving`.

## Injector Architecture

The fault injector is an independent Linux shared object owned by `bench/`.
It is not a Rust crate, Cargo dependency, production feature, server option, or
NoKV protocol surface.

```text
qualification controller
  -> starts exact candidate or one-operation child with LD_PRELOAD=<shim>
       -> process calls libfdb_c
       -> matching transaction really commits
       -> shim changes only the successful commit future result to 1021
  -> reads product-visible outcome
  -> performs independent FDB and NoKV readback
```

The shim resolves the real FDB symbols with `dlsym(RTLD_NEXT, ...)`. It
observes exact binary mutation keys passed to:

- `fdb_transaction_set`;
- `fdb_transaction_clear`;
- `fdb_transaction_clear_range`; and
- `fdb_transaction_atomic_op`.

A scenario supplies one exact target key as hexadecimal bytes, the expected
mutation kind, and one selection rule. Startup operations use the exact
ordinal matching mutation in that fresh process. Later lifecycle operations
use an inherited control descriptor: the shim starts disarmed, the controller
observes the required precondition, and a run-nonce-bound one-shot arm message
selects the next exact key-and-kind match. This distinction is necessary
because acquisition, activation, renewal, fail-close, and release can mutate
the same route or heartbeat key at different times.

An ordinal or armed match marks only the associated `FDBTransaction` as the
target. A clear range matches only when the exact key is inside its half-open
interval. Reads never select a transaction, pre-arm matching mutations remain
transparent, and the shim never changes transaction contents, predicates,
timing, or retry options. The control descriptor is inherited by the shim;
the candidate neither opens nor interprets it.

The shim tracks the commit future returned by `fdb_transaction_commit`. When
`fdb_future_get_error` reports real success for the one target future, the
shim returns FDB error `1021` once and records both values. A real nonzero FDB
error is passed through unchanged. A second target transaction, a second
substitution, an unobserved target commit, or destruction of the target future
without the required observation invalidates the scenario.

The target selector, control descriptor, and evidence channel are configured
only for the preloaded shim. Product code does not parse those environment
variables. Retained evidence stores the target-key SHA-256, mutation kind,
selection rule, observed match count, and arm event, but not the raw key. Each
scenario uses a fresh process, physical FDB prefix, target key, and injector
event stream so pointer reuse or prior state cannot satisfy a later case.

FDB client buggify is not used because it is global and nondeterministic.
FoundationDB automatic idempotency is not used because it changes the
candidate transaction behavior and prevents the unknown outcome this gate is
required to exercise.

## Metadata Mutation Matrix

All metadata cases use an owner-session-fenced `FdbStore`. The live controller
retains the exact typed error, durable rows, and operation-specific counters.

| Family | Injected transaction | Required product behavior | Exact PASS oracle |
| --- | --- | --- | --- |
| Initialization | Creation of schema, shard identity, and system rows | The provisioning runtime may reconcile by `MetaShard::open`; it must not issue a second initialization commit. | Every bootstrap row decodes to the expected schema and shard, and the injector saw one target commit. |
| Owner epoch | Advance from the durable predecessor to the acquired session epoch | Read the durable epoch after an unknown result. Continue only when it equals the exact acquired epoch; an ahead, missing, or mismatched value fails closed. | Owner fence and recovery state encode the expected epoch once. |
| Root-fence install | Deterministic install `MetadataCommand` | Read back the exact root identity, logical shard, object namespace, placement generation, and activation state. | `Installing` is accepted; an already `Active` record is accepted only when every immutable field and the command identity match. |
| Root-fence activation | Deterministic transition command | Read back the exact fence before catalog readiness can continue. | The fence is exactly `Active`; no Ready catalog or Serving route came from an unproved fence. |
| Ordinary command | One externally visible workspace mutation with a unique request ID | Owner A returns a retryable ownership result and closes local admission. Owner B acquires a strictly newer session and replays the exact same sealed request. | B returns the durable `CommandDedupe` result with `replayed=true`; the deterministic result and commit version match, and the commit clock advances once. |
| Lease-clock high-water | Advance the monotonic high-water under one root and owner fence | Under the same fence, exact readback may accept a durable value at least as high as requested; otherwise the operation fails closed. | The isolated scenario has no concurrent clock writer, so the retained value must equal the requested value and its recovery effect occurs once. |

`initialize`, `advance_owner_epoch`, `observe_lease_clock`, and sealed command
execution are separate acknowledgement paths. Different ordinary command
mutation variants do not require redundant live cases unless they introduce a
different transaction or reconciliation boundary.

## Distributed-Control Mutation Matrix

The control matrix is derived from the actual write methods of
`FdbControlStore`. It includes `fail_closed`; treating local admission closure
as proof of the shared route mutation would leave a real control commit family
untested.

| Family | Required exact readback or fail-closed oracle |
| --- | --- |
| Store manifest format | The complete immutable `StoreManifest` equals the requested value. A mismatch remains an error. |
| Shard catalog create | The `Provisioning` shard and initial `Unassigned` route both exist with the exact logical-shard identity; no session or heartbeat was fabricated. |
| Root catalog create | The complete root, shard, object namespace, placement generation, and `Provisioning` state match. |
| Root catalog CAS | The complete next record is present. Observing only `Ready` is insufficient. |
| Shard catalog CAS | The complete next record is present, including the exact logical-shard identity and state. |
| Provisioning-owner acquisition | Route, session, owner epoch, session generation, endpoint, and initial heartbeat are the exact planned update for the observed predecessor. |
| Serving-owner acquisition | The same complete tuple is proved under a Ready catalog; an unrelated newer session is not accepted as reconciliation. |
| Owner renewal | The durable heartbeat names the exact session and has the exact planned next sequence. No wall-clock lease deadline is persisted. If exact proof is unavailable, local admission closes and the caller does not retry the raw renewal. |
| Route activation | The route is `Serving` with the exact session, owner epoch, session generation, owner, and endpoint. |
| Route fail-close | Local admission closes first. The shared route is either proved as exact `FailClosed` for the same session or the cleanup remains an error; the process never resumes admission from an ambiguous result. |
| Owner release | The route is exact `Unassigned`, the stable session is absent, and the retained owner epoch/session generation and next heartbeat sequence are correct. A successor tuple must never be accepted as the released state. |

Create and CAS helpers may reconcile conflict and unknown-outcome results by
reading the complete expected record. Owner acquisition, activation, and
release retain their existing exact-session predicates. Renewal and
fail-close may return a typed failure rather than continue when readback
cannot prove the exact update; safety does not require availability under an
ambiguous control result.

## Ordinary-Command Failover Flow

The workspace-command case proves the end-to-end server and client behavior,
not merely the `TxnStore` adapter:

```text
client sends request R to owner A
  -> A commits mutation + CommandDedupe(R)
  -> shim turns successful acknowledgement into 1021
  -> A returns retryable NotOwner and closes shard admission
  -> shared route cannot be republished by A
  -> owner B acquires a strictly newer exact session
  -> client refreshes through a NoKV seed
  -> client sends the byte-identical sealed request R to B
  -> B returns the durable dedupe result without another mutation
```

The controller retains the request digest, command digest, request ID,
original dedupe row, successor response, commit version before and after, both
owner tuples, client route-refresh transcript, and independent data readback.
A newly generated request ID is not a reconciliation test.

## Product-Code Boundary

The raw adapters continue to perform one raw commit attempt. In particular,
`nokv-meta-fdb` maps FDB `1021` to typed `StoreError::OutcomeUnknown` and must
not grow an automatic retry loop. `nokv-control-fdb` retains the same rule for
ordinary control mutations; its manifest create readback remains a narrowly
owned operation-specific exception.

Metadata-state reconciliation belongs beside the metadata operation in
`nokv-meta/src/workspace/engine.rs`. The expected product changes are local
unknown-outcome readbacks for owner-epoch and lease-clock advancement. They
reuse the existing durable owner and high-water accessors and return success
only for the exact legal state. They do not introduce a generic commit wrapper.

FDB composition and admission ordering remain in
`nokv-server/src/fdb_runtime.rs`. Initialization, root-fence commands, exact
control records, fail-close, takeover, and release are reconciled there. If
live injection exposes a missing exact control readback, the fix belongs in
this operation-specific orchestration or in the control domain method that
owns the record. It does not belong in a provider-generic utility module.

The server executor retains the existing ordinary-command rule: an
unresolved metadata acknowledgement becomes a retryable ownership response,
then the registry fails the shard closed. The successor reconciles only by the
same sealed request identity and durable dedupe row.

## Qualification Code Layout

No workspace crate is added. The qualification remains in `nokv-bench`:

```text
bench/
├── fault_injection/
│   ├── fdb_commit_unknown.c
│   └── build_fdb_commit_unknown_shim.sh
└── src/
    ├── bin/
    │   └── nokv-fdb-unknown-outcome-qualification.rs
    └── fdb_unknown_outcome/
        ├── mod.rs
        ├── evidence.rs
        ├── metadata.rs
        └── control.rs
```

The binary entry point owns only explicit live-gate enablement, option
parsing, environment qualification, and terminal exit status.
`metadata.rs` and `control.rs` own responsibility-named scenarios.
`evidence.rs` owns only Gate 2-specific schemas and validation; it reuses
`qualification_runtime::EvidenceBundle`, hashing, process supervision, and
atomic finalization instead of reimplementing those helpers. Tests stay next
to the owning module. No `utils/`, generic fault framework, or public
production API is introduced.

The controller invokes the production candidate for format/provision/serve
phases and the end-to-end workspace request. When a control-store method has
no independently triggerable production CLI or RPC boundary, the same
qualification binary starts a one-operation child mode under the shim. That
child calls the real production crate method against the real cluster, then
exits; it contains no alternate transaction implementation. Its digest and
the qualification feature set are retained beside the candidate digest. This
is the same separation already used when live qualification needs direct
control or fenced-metadata inspection.

The shim is built explicitly for the Linux qualification container. It is not
built by a workspace `build.rs` and is not linked into normal Cargo targets.
The controller accepts an absolute shim path and verifies both source and
binary digests before launching a candidate or qualification child.

## Execution Isolation

Each case and each repetition receives:

- a new preloaded candidate or one-operation qualification-child process, plus
  the exact successor candidate processes required by the ordinary-command
  case;
- a unique FDB prefix below the run's approved prefix base;
- a unique root, logical shard, owner identities, endpoints, and request IDs;
- one exact target-key digest;
- a fresh shim event stream; and
- pre-case and post-case FDB and RustFS health observations.

The selected executable SHA-256 is checked before every process launch. A
scenario cannot inherit a previous prefix or use a newly rebuilt candidate or
qualification binary. The controller performs cleanup only after terminal
evidence for that scenario is durable and records cleanup results. Cleanup
failure cannot rewrite the scenario's semantic result, but it prevents the
overall qualification run from passing.

The live matrix runs on Linux. A macOS developer may start it through the
repository's Docker qualification environment, but `DYLD_INSERT_LIBRARIES` is
not a second supported qualification path.

## Evidence Contract

Every invocation creates one non-existing directory:

```text
target/qualification/fdb-unknown-outcome-<source>-<timestamp>/
├── environment.json
├── candidate.json
├── injector.json
├── scenarios/
│   └── <family>-<repetition>/
│       ├── input.json
│       ├── injector-events.jsonl
│       ├── product-outcome.json
│       ├── readback-before.json
│       ├── readback-after.json
│       └── result.json
└── result.json
```

`environment.json` records the clean source revision and dirty state, OS and
architecture, Rust toolchain, FDB API/client/server versions, cluster-file
digest, FDB prefix-base digest, pinned RustFS image identity and health URL,
object-namespace binding digest, and qualification binary digest.
`candidate.json` records the exact `nokv-fdb` path, SHA-256, version output,
and redacted production invocation. `injector.json` records shim source and
binary digests, compiler identity, the exported contract version, target
selector contract, and the permitted substitution count.

Each scenario retains:

- the operation identity and complete expected state;
- target-key SHA-256;
- target mutation kind, ordinal or arm event, and observed match count;
- real commit result and substituted result;
- process ID, transaction sequence, commit-future sequence, and substitution
  count from the shim;
- the typed product-visible outcome;
- independent before/after FDB state;
- NoKV protocol evidence where the operation is externally visible;
- exact counters or dedupe identity proving one logical apply; and
- process exits and cleanup status.

Raw object-store credentials, raw cluster connection strings, and raw binary
target keys are not retained. A credential-redaction scan is part of bundle
validation.

The top-level `result.json` is created once, atomically, after all required
files pass schema and digest validation. An incomplete process cannot leave a
stale PASS marker. The result also carries an inventory and SHA-256 for every
retained evidence file other than the terminal file itself.

## Result Semantics

The only terminal statuses are:

- `PASS`: every required family and subcase completed three clean repetitions
  from a clean source against one exact candidate and qualification-binary
  digest pair, with one valid substitution per case;
- `FAIL`: a real target commit failed, the target matched zero or multiple
  transactions, substitution count differed from one, exact state mismatched,
  a logical mutation applied twice, an unproved route became `Serving`, or any
  other semantic assertion or required cleanup failed; and
- `NOT QUALIFIED`: the live gate was not explicitly enabled, the real FDB or
  pinned RustFS service, required Linux loader, or selector control channel was
  unavailable, the source was dirty, the candidate identity was ineligible,
  or environment preflight could not establish the qualification conditions
  before a scenario began.

After a scenario begins, injector contract violations are `FAIL`, not
`NOT QUALIFIED`. Timeouts retain enough state to distinguish an environment
loss from a product semantic failure; an ambiguous timeout cannot become
`PASS`.

Unit tests, mock stores, an injector self-test, a no-injection control, or a
dry run may report their own success but cannot emit a Gate 2 `PASS` result.

## Validation Layers

### Injector contract tests

A small fake FDB C-API fixture proves:

- non-target transactions and futures are transparent;
- exact set, clear, range-clear, and atomic-key matching;
- ordinal selection, pre-arm transparency, run-nonce-bound one-shot arming, and
  duplicate-arm rejection;
- one real-success target becomes one `1021`;
- real FDB errors are passed through unchanged;
- multiple transactions and threads do not cross-associate pointers;
- a repeated target or substitution fails; and
- future destruction and injector shutdown report incomplete contracts.

### Deterministic Rust tests

The Rust workload tests:

- option and live-gate validation;
- compile-time inventory of all required families and repetitions;
- scenario state transitions and one-shot target allocation;
- exact per-family readback predicates;
- ordinary-command request identity and dedupe assertions;
- evidence paths, schemas, redaction, digests, and atomic finalization;
- `PASS`, `FAIL`, and `NOT QUALIFIED` classification; and
- refusal to publish PASS for a mock, dry run, missing file, or wrong candidate
  digest.

Existing lost-ack unit tests remain useful regression evidence, including the
store-authority `CommandDedupe` replay test. They do not replace the live FDB
matrix.

### Live FDB sequence

The environment-gated qualification performs:

1. a clean environment, exact-candidate, qualification-child, FDB, and pinned
   RustFS preflight;
2. a no-injection negative control proving the shim is transparent;
3. one targeted smoke case proving the real-success-to-`1021` chain;
4. the complete metadata and control matrix, three fresh repetitions each;
5. FDB health and exact prefix inspection before and after every case;
6. bundle schema, digest, redaction, and scenario-inventory audit; and
7. a final clean-worktree build and repository gate run for the exact source.

The smoke case and negative control are prerequisites, not counted among the
three qualifying repetitions.

## Acceptance

Implementation is acceptable only when all of the following hold:

- the shim contract tests and Rust qualification tests pass;
- `cargo fmt --all -- --check` passes;
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
  passes in the supported FDB builder composition;
- the repository-supported workspace test split passes;
- `python3 scripts/workbench/workbench_contract_test.py` passes;
- `git diff --check` passes;
- every new non-merge commit has a `Signed-off-by` trailer;
- Cargo metadata contains no new workspace crate for this work;
- the candidate has no dynamic dependency on the shim and production code has
  no fault-injection option, environment parser, or feature;
- the live bundle contains every required case and three clean repetitions
  with the exact same candidate and qualification-binary SHA-256 values;
- the bundle audit finds no credentials or missing evidence; and
- the qualification report changes Gate 2 to `PASS` only in a commit that
  cites the retained clean-head bundle and its terminal digests.

The full distributed FDB serving profile remains `NOT QUALIFIED` after Gate 2
until Gates 8, 9, and 10 independently pass.

## Non-Goals

This work does not:

- add automatic retries for ambiguous raw commits;
- add a production fault-injection feature, CLI, environment variable, or
  compatibility shim;
- use FDB buggify or automatic idempotency as qualification evidence;
- link or package the injector with `nokv-fdb`;
- create a new crate or reorganize the existing workspace;
- qualify lifecycle, maximum physical transaction limits, or performance;
- restore etcd, the retired bootstrap composition, Yanex workflows, or
  historical benchmark compatibility; or
- change Gate 2 status before the complete retained live evidence passes.
