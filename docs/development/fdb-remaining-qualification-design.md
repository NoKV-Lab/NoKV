<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# FDB Remaining Qualification Design

Date: 2026-09-01

Status: approved continuation design; Gates 8 through 10 remain not qualified
until their live evidence bundles pass.

## Decision

Complete the remaining FoundationDB serving qualification as three independent
live workloads in `nokv-bench`:

1. lifecycle safety and recovery;
2. measured transaction limits and object-payload separation; and
3. controlled client-visible latency and throughput.

The workloads share retained-evidence and process-supervision code, but they do
not share terminal results. A Gate 8 failure cannot be hidden by a Gate 9 or
Gate 10 result, and a performance run cannot qualify durability.

The exact production `nokv-fdb` candidate remains the only serving process.
Qualification code may inspect or construct a legal precondition through the
same production Rust crates when no public request can create that state, but
it must not implement another metadata transaction, object lifecycle, or
routing path. No fault selector, admin endpoint, environment-controlled branch,
or qualification feature enters a production binary.

This design follows the package contract in
[`code_contract.md`](code_contract.md), the storage review rules in
[`pr_review_checklist.md`](pr_review_checklist.md), and Gates 8 through 10 in
[`metadata-store-interface.md`](metadata-store-interface.md).

## Rejected Alternatives

One combined Gate 8-10 executable was rejected because it would mix safety and
performance evidence, make partial failures difficult to audit, and grow into
another orchestration framework.

Production qualification hooks were rejected because a test-only route, delete
mode, or transaction-observer option would expand the product surface and could
change the exact behavior being measured.

Duplicating the full Workbench workflow in a new client was rejected. The
qualification workloads call the public CLI or the existing Rust SDK and use
the production protocol, publication, commit, snapshot, and restore state
machines.

## Gate 8: Lifecycle

The live lifecycle workload uses a fresh FDB prefix, a real FDB cluster, a
pinned RustFS service, and one exact candidate. It retains the active route and
session tuple before and after every destructive phase.

The public-path scenarios are:

- publish an immutable artifact and read it back;
- commit a Workbench and retain the sealed commit identity;
- mint, renew, retire, and list a snapshot;
- restore the snapshot into an absent destination and read the frozen body;
- publish a disposable artifact, remove its exact generation, and wait for the
  resulting revision GC to finish; and
- repeat disposable publication/removal while an external proxy loses exactly
  one successful RustFS `DELETE` response, then require durable GC quarantine.

A Workbench head intentionally retains its parent commits. There is no public
operation that creates a sealed zero-consumer commit solely for retirement.
For the commit-retirement case, the qualification executable opens the current
session-fenced `FdbStore`, constructs one codec-valid sealed commit plus its
exact revision reference through `nokv-meta`, and commits that precondition as
one normal `MetadataCommand`. The production candidate must discover, claim,
release, and complete retirement. The qualification process only observes the
terminal records; it never calls `LifecycleRunner`.

The ambiguous-delete injector is a separate TCP proxy owned by the Gate 8
workload. Before it is armed, it transparently forwards the candidate's S3
traffic. Once armed, it selects the next `DELETE`, forwards the complete
request to RustFS, waits for a successful RustFS response, retains its status
and digest, drops every response byte, and closes the client connection. PASS
requires the production candidate to persist:

- `GcOperation.phase = Quarantined`;
- `GcCandidate.claim_state = Quarantined`;
- `ArtifactRevision.state = Quarantined`; and
- non-empty bounded ambiguity evidence.

The proxy seeing no target, more than one target, a non-success RustFS result,
or forwarding any selected response byte invalidates the run. Ordinary provider
unavailability is not accepted as this scenario.

## Gate 9: Limits

Gate 9 has two complementary cases.

The transaction-envelope case opens a real session-fenced `FdbStore` against a
fresh qualification root and submits progressively larger valid transactions.
The adapter records FoundationDB's `get_approximate_size` result in adapter-
owned read-only diagnostics. The diagnostics are general FDB store statistics,
not an injection hook: they cannot change a transaction, a limit, or a result.
The workload retains logical request bytes, conservative affected bytes,
observed approximate physical bytes, mutations, checks, outcomes, and the
largest successful point below the 9,500,000-byte physical guard.

The required planner assertion is conservative:

```text
maximum observed physical bytes for a valid 900,000-byte logical plan
  < measured rejecting or configured physical envelope
```

The object-separation case publishes and materializes an artifact larger than
the 2,900,000-byte logical transaction limit through the exact candidate. PASS
requires the full object digest and length to round-trip while every observed
metadata transaction remains below both the logical hard limit and the physical
guard. The payload itself must not appear in any FDB mutation value or retained
metadata row.

The measured envelope is specific to the recorded FDB client/server versions,
API version, cluster configuration, and candidate source. It is not promoted to
a universal FoundationDB limit.

## Gate 10: Performance

Gate 10 measures the exact candidate over the NoKV seed and workspace wire
protocol. It has separate uncontended and contended profiles rather than
combining them into one percentile distribution.

Each profile records:

- warmup count, measured operations, payload size, concurrency, and key
  distribution;
- successful, conflicted, retried, and failed operation counts;
- p50, p95, p99, and maximum client-visible latency using nearest-rank
  percentiles;
- elapsed time and completed operations per second;
- route/session identity, FDB topology and status, candidate and dependency
  digests, host/CPU identity, logical and observed physical transaction sizes;
  and
- CPU governor, container CPU quota/affinity, and available thermal/frequency
  observations, or an explicit statement that a control is unavailable.

The uncontended profile uses independent workspace identities so its conflict
rate is expected to be zero. The contended profile creates one generation-1
source path per group, then races renames to distinct destinations while every
request pins that source generation. This isolates metadata contention from
object upload while making the winner and losing preconditions auditable. A
failed operation is retained and prevents PASS; retries are counted at the
layer that actually performs them.

Performance evidence describes this topology and workload only. It does not set
a product SLO, compare Holt and FDB as equivalent durability profiles, or turn a
correctness stress result into a performance claim.

## Code Layout

No workspace crate is added. The intended layout is responsibility based:

```text
bench/src/
├── fdb_live_runtime.rs
├── fdb_lifecycle_qualification/
│   ├── mod.rs
│   ├── evidence.rs
│   ├── inspection.rs
│   └── lost_delete_proxy.rs
├── fdb_limits_qualification.rs
├── fdb_performance_qualification.rs
└── bin/
    ├── nokv-fdb-lifecycle-qualification.rs
    ├── nokv-fdb-limits-qualification.rs
    └── nokv-fdb-performance-qualification.rs
```

`fdb_live_runtime.rs` owns only the configuration, redacted command execution,
candidate supervision, and FDB/RustFS environment capture shared by these three
gates. Gate-specific records, assertions, and fault behavior stay with their
own modules. Existing Gate 2, Gate 6, and Gate 7 workloads are not mechanically
rewritten as part of this work.

## Evidence And Status

Every gate creates a non-existing evidence directory, hashes the candidate and
qualification executable, retains redacted command transcripts, captures FDB
and RustFS health before and after the run, and atomically publishes one
`result.json`. Credentials, raw authorization headers, and environment dumps
are forbidden.

`PASS` requires all scenarios for that gate. A missing dependency, unavailable
control, unobserved fault, incomplete identity, or missing evidence role is
`NOT QUALIFIED`; a completed semantic or safety violation is `FAIL`.

The user-facing FDB runtime remains **NOT QUALIFIED** until Gate 8, Gate 9, and
Gate 10 have each produced a complete PASS bundle from the same accepted source
revision, and the aggregate repository gates also pass.
