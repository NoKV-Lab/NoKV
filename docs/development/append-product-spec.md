<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Durable append product contract

This is the normative implementation contract. The
[durable append guide](../append.md) provides CLI/Python examples; the
[qualification record](append-qualification.md) separates executed evidence
from requirements below.

A queue worker saves an action ID and its event before appending the event to a
workspace file. If its process dies, its replacement submits the same action
and receives one durable result. Concurrent writers may change the file while
that action is recovering. NoKV must either finish the action exactly once or
report a durable, actionable recovery state without allowing two effects.

This document specifies that feature. Execution receipts establish which
requirements passed on a particular build and deployment. The broader release
gates in [workspace acceptance](workspace-acceptance.md) remain independent.

## Consumer responsibilities and supported interfaces

The native `nokv workspace-path append` command is the primary interface, followed by Python
`Client.append_bytes` and Rust `WorkspaceClient::append_artifact_idempotent`.
They share the Rust client state machine. Owner preflight advertises
`artifact_append_v1` so downstream clients can require the logical append
contract explicitly. `nokv operation status` and Python
`Client.operation_status` query the logical operation using metadata only.
`nokv operation inspect` / `Client.operation_inspect` inspect the current child's
retained staged ledger. `nokv operation recover` / `Client.operation_recover`
request owner-executed cleanup through `artifact_append_recovery_v1`. Neither
operation requires the delta, a current workspace binding, object credentials
on the caller, or a transcript captured before the failure.
Metadata routing and caller configuration are still required; metadata-only
does not mean an offline lookup or that the serving owner needs no object
provider for cleanup.
The frozen Workbench tool schemas remain unchanged.

The caller must durably retain the following before its first submission:

- A root-scoped 128-bit logical operation ID, unique across append, publication,
  commit build, and restore operations.
- The normalized workspace/path and intended workspace incarnation, once known.
- The exact delta bytes, create content type, optional replacement content type,
  block size, and maximum resulting size.

The caller must reuse those inputs when redelivering an action. Independent
actions need independent IDs even when their bytes match. A fork of a run uses
its own identity; retargeting an admitted ID to the fork is an intent mismatch.
Callers must not allocate a new ID merely because an RPC timed out. A digest is
not a substitute for retaining the delta: NoKV does not reconstruct payloads
from a hash or execute the harness's external actions.

When a caller omits the incarnation, the shared client first queries the
logical operation. An admitted operation resolves to its original incarnation
even if its workspace name has since been removed or rebound. Only an absent
operation resolves through the live workspace name. An explicit incarnation
must match and is never silently replaced. The server checks the fence during
admission and publication.

## Identity, intent, and immutable attempts

The logical operation owns a full SHA-256 commitment to the canonical intent.
It includes the root, logical ID, incarnation, length-delimited target and
content-type fields, explicit option tags, block size, effective result-size
limit, delta length, and full delta SHA-256. Omitted result-size limits normalize
to the documented default before hashing. Distinct optional content-type
policies remain distinct even when their strings happen to match the current
file. Scheduling retry budgets and transport configuration are not intent.

The metadata service binds this commitment exactly. Metadata RPCs do not carry
the original delta, so the service does not claim to independently recompute
the caller's full intent hash. Object upload verification and manifest/body
validation retain their existing responsibilities.

Each logical operation has one current numbered publication attempt, starting
at zero. Child publication and artifact revision identities are SHA-256-derived
from the root, logical ID, attempt number, and separate versioned domains. The
server verifies this derivation. The logical ID is distinct from the child ID;
a consumer must persist and retry the logical ID.

The parent contains the canonical intent, original target/incarnation, current
attempt identity, and eventual compact receipt. Each child publication carries
its parent/attempt binding and its immutable generation-dependent plan. An
active child cannot be replaced by a newly planned child.

The identities and counters have different lifetimes:

| Field | Scope and meaning |
| --- | --- |
| `operation_id` | Caller-owned logical action; unchanged across all redeliveries and safe successors. |
| `attempt` | Current child number, starting at zero; advances only with atomic predecessor-cleaned admission. |
| `publication_operation_id` / `artifact_revision_id` | Derived immutable identities of that numbered child. |
| `cleanup_retry_count` | Number of accepted operator cleanup retries for this child; a new child starts at zero. |
| `operation_token.state_digest` | Exact observed logical parent/current-child state; retaining it identifies one operator recovery request. |

## Atomicity and recovery invariants

1. Creating or advancing the parent and admitting its child happen in one
   metadata command. There is no externally admitted parent-to-missing-child
   interval and no separate advance RPC.
2. The first child requires an absent parent. A successor requires the exact
   current predecessor to be durably `Cleaned`. `Failed`, timeout, `NotFound`,
   stale routing, `Finalizing`, and `Quarantined` do not prove cleanup.
3. A child's final publication verifies that it is still the parent's current
   attempt. Finalizing the child, committing the file/manifest/references/indexes
   and change event, and persisting the parent's successful receipt happen in
   the same metadata transaction.
4. A successful parent never advances. Replaying success returns the original
   receipt without consulting the live path or reading object bodies.
5. Operation-kind collision checks execute atomically during admission. A
   preliminary lookup alone does not establish uniqueness under concurrency.
6. Owner epochs, workspace incarnations, row digests, and publication-absence
   proofs continue to fence recovery. A failed read or uncertain commit cannot
   authorize cleanup, a successor, or an inferred success.
7. Cleanup permanently seals a failed append child's staged object keys before
   retiring their staging rows. Its revision claim remains reserved so that
   neither delayed uploads nor later reuse of that revision can revive its
   payload. Parent receipts do not pin historical artifact bytes.
8. Retrying quarantined cleanup increments the child's durable
   `cleanup_retry_count` exactly once. The exact expected logical state token
   identifies a recovery request, whose original admission receipt remains
   replayable after another quarantine, owner restart, or logical successor.
   The counter prevents a cleanup failure from returning the child to the same
   state bytes and accepting a delayed old request as a new recovery round.

A definitive generation conflict during completion can put the exact losing
child into fenced abort/cleanup after proving it did not publish. The client
can then re-read the current head and admit a successor under the same logical
ID. Concurrent successful appends have a generation order; NoKV does not promise
that this order matches caller start times or external queue order.

## Observable outcomes and caller actions

| State | Meaning | Required next action |
| --- | --- | --- |
| `committed` | Parent and child have an atomic successful receipt. | `none`; acknowledge the queued action. |
| `pending` | The current child is uploading, finalizing, aborting, or cleaning. | `poll`; keep the same action and payload. |
| `ready_to_retry` | The predecessor is durably cleaned and cannot publish. | `resubmit_same`; send the original logical ID, delta, and options. |
| `quarantined` | Automatic recovery cannot prove a safe transition. | `retry_cleanup`; inspect the operation, resolve the dependency failure, and request owner cleanup using the saved state token. |
| Query error or unknown transport outcome | No authoritative recovery observation was obtained. | Query the same ID again; do not infer absence or allocate a replacement action. |
| Intent mismatch | The ID belongs to a different intent or lifecycle. | Correct the caller's persisted action mapping; do not mutate that ID's intent. |

Errors carry the logical operation ID where available, the structured cause
code, and observed wire state where known. An error envelope's conservative
`query_same` action does not claim knowledge of the current child. The metadata
status result supplies authoritative attempt, child ID, phase, activity
deadline, original incarnation/target, current attempt failure when present,
cleanup retry count, and receipt. An attempt failure describes its child, not a terminal failure of
the logical action. Applications must use
structured fields, not parse human-readable messages.

The compact successful append receipt binds the logical and publication IDs,
target/incarnation, artifact revision, workspace revision, path generation,
whole-body size and digest. These historical fields are immutable.
`commit_version` and `replayed` are call-envelope metadata, not fields of the
append receipt: a later metadata-only lookup may have no commit version, and a
replay may report different call metadata. Do not compare entire response
envelopes to decide whether an append happened twice.

A missing logical operation is a point-in-time observation. Another caller may
admit it immediately afterward. Resubmitting the same ID is safe because atomic
admission handles that race; changing to a fresh ID is not justified.

The synchronous client performs bounded local submissions and cleanup polling.
It may return `pending` while owner recovery continues. Eventual completion
requires redelivery with the same payload, a reachable owner/object provider,
and progress by the fenced cleanup lifecycle. Perpetual contention, exhausted
capacity, unavailable dependencies, or quarantine cannot be converted into a
success promise by retrying more aggressively.

## Failed-object sealing and delayed uploads

A successful DELETE proves absence at one instant. It cannot prevent an old
PUT from arriving afterward. A cleanup implementation that then forgets its
staged keys can leak an unreachable payload even while logical append
idempotency remains correct. Product acceptance reproduces this window by
holding a real conditional PUT before it reaches the provider, killing its
caller, allowing cleanup, and then releasing that same request.

Stable append therefore closes failed object keys monotonically. If a key is
absent, cleanup creates a zero-byte seal with `If-None-Match: *`. If payload
already exists, it replaces it with the same seal using `If-Match` against the
observed provider ETag. Aborted append cleanup never DELETEs these keys. This
also prevents an old cleanup request from deleting a newer seal and reopening
the late-upload window. A delayed nonempty upload cannot replace a seal. An
exact empty-body replay still cannot authorize publication by a terminal child.

`Sealed` is distinct from physical deletion or absence. The failed child's
revision claim remains permanently associated with its terminal operation,
including after a logical successor succeeds. The ordinary collector for
published revisions cannot claim that unpublished reserved revision. The
generic publication reconciliation API cannot resolve a stable append by
accepting a caller's claim of absence or publication. The earlier unreleased
append-specific sealed-object verdict is removed. An ambiguous provider result
cannot be converted into an unproved successful cleanup.

Object-provider admission for stable append includes sealing conformance in
addition to immutable creation. Provider receipt verification is bound to the
actual handle and block-size profile. Generic publication admission retains
its existing requirements; the native owner that advertises append support
must qualify sealing. A local evictable cache cannot supply the permanent
proof; a tiered store seals its durable provider and invalidates its hot copy.

These guarantees depend on the admitted provider honoring the conditional
writes. The real acceptance receipt identifies the tested RustFS build and
configuration. It does not certify every product describing itself as
S3-compatible. The artifact prefix must have no lifecycle expiration rule or
external delete/overwrite process that can remove live seals. A provider delete
marker is not a live empty seal: [AWS conditional-write semantics](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html)
allow `If-None-Match` creation when the current version is a delete marker. With versioned buckets, replacement clears the
current payload only; old object versions may retain bytes and require a
separately qualified retention policy. The acceptance configuration and this
feature do not certify deletion of historical provider versions. Ordinary
publication cleanup remains a separate lifecycle contract; this feature's seal
semantics apply to stable append attempts.

## Public inspection and recovery

Inspection starts with the logical operation ID and returns its exact logical
token, current publication token, object namespace, phase, and counters. The
`registered_count` is the actual staged cursor, not the publication plan's
intended object count. The `cleanup_cursor` counts rows retired after proven
sealing; `remaining_count = registered_count - cleanup_cursor`. Each entry
contains its sequence, complete object identity, expected length and digest,
and multipart token when applicable. These entries describe currently retained
staging metadata, not a history of every object ever used by the logical action.
Published attempts can still retain their staging rows: an inspection result
alone does not authorize cleanup of those live objects.

The default page limit is 32 and the maximum is 192. A page has exactly the
bounded contiguous portion of `[cleanup_cursor, registered_count)` selected by
its cursor. The opaque continuation binds the root, logical ID, exact state
token and last ordinal. Every page checks the current parent, current child
and ledger rows. Unpublished children also require their exact revision
reservation; successful publication has already released that reservation.
State or child changes invalidate the
continuation; the caller must restart from the first page. Missing or mismatched
rows fail closed instead of producing an incomplete page described as complete.
Cursor checksums detect accidental damage; server-side fencing supplies the
authority. Inspection performs no provider I/O or metadata mutation.

For example, an operator investigates action `ab...` after a provider outage:

```text
nokv <routing arguments> operation inspect ab... --limit 32
nokv <routing arguments> operation recover ab... --expected-state-digest <saved digest>
nokv <routing arguments> operation status ab...
```

Use full 32-character operation IDs and the full 64-character state digest.
Persist `operation_token.state_digest` from inspection before requesting
recovery. Repeating `recover` with that same digest replays the same durable
cleanup admission, even if the first response was lost and the owner has since
failed cleanup again. The response's `recovery_receipt` identifies that original
round, while its operation fields report the current observation. `requested`
means the request has a durable admission receipt, including replay;
`replayed` distinguishes replay from fresh admission. Neither field means the
append committed or cleanup finished.

For example, cleanup token T can admit round 1 for child A. If that cleanup
quarantines again, resending T returns the same round-1 receipt; it does not
start round 2. A deliberate request using a new observation can admit round 2.
After A reaches `Cleaned` and the same logical append publishes child B,
replaying T still returns A's original recovery receipt while the response's
current operation describes B with `cleanup_retry_count=0`. This is not a
counter regression. The recovery call's commit version belongs to the original
cleanup-admission command and is preserved on exact-token replay.

Omitting the expected digest explicitly requests recovery against the currently
observed state. Active, cleaned and committed attempts return an observation
with `requested=false` and no recovery receipt. A quarantined attempt can start
one new round. One invocation never silently selects another state token after
a race or uncertain response. For queue-driven operator automation, retain the
explicit digest so retrying a request cannot accidentally start a later round.
An unresolved request error preserves the logical ID, expected digest and any
known admission receipt, with `retry_same_cleanup` as the next action.

The owner verifies the exact parent/child binding, permanent revision claim,
revision absence and root/owner fences before atomically re-enqueuing cleanup.
The durable command receipt retains the original failure for audit. The
existing lifecycle worker performs and verifies conditional seals, advances
cleanup cursors, and records durable proof. Operator clients do not write seals
or supply a provider verdict. Cleanup of an abandoned old incarnation uses
these operation/revision fences even after the workspace name disappears or
is rebound; it cannot publish into the replacement workspace. Generic
publication cleanup retains its existing authorization rules.

After the owner reports `ready_to_retry`, the caller redelivers the original
append ID, delta and complete intent. Recovery without the payload only makes
that safe redelivery possible; it cannot synthesize the logical append result.
Repeated provider ambiguity returns the same child to quarantine with a new
retry count. A new inspection and a deliberate new token can request another
round after the underlying problem is corrected.

## Example recovery timelines

| Failure | Durable observation | Recovery |
| --- | --- | --- |
| Worker dies before admission. | No parent. | Replacement submits the same ID; attempt zero is admitted once. |
| Begin succeeds but its response is lost; worker or owner dies. | Parent and child already exist. | Wait for fenced cleanup, then submit the same ID to create the next child atomically. |
| An object PUT succeeds but the client loses its response. | The child may be pending; publication is not inferred. | Query and wait for a safe child outcome. Unknown upload results never authorize an unfenced successor. |
| A different action wins a generation CAS. | Losing child is safely aborted and cleaned. | The same logical append replans against the new head and completes once. |
| Complete commits but its response is lost. | Parent contains the committed receipt. | Replacement returns that receipt; it uploads and appends nothing. |
| File is later appended, removed, or replaced. | Parent retains its original success. | Status and replay still return the historical result without changing the new live head. |
| Workspace name is deleted and recreated. | Parent retains the original incarnation. | Old success remains queryable; an unfinished old intent cannot write into the new instance. |

A zero-byte action uses the same contract. It creates an empty file when absent.
A new ID may advance an existing file's generation even though its bytes remain
unchanged. Replaying that ID cannot advance the generation again.

## Size, resource, and retention contract

The maximum delta is 16 MiB. The default maximum resulting artifact is also
16 MiB, including the existing body and the new delta. A caller may explicitly
increase the result-size limit; that limit becomes part of the immutable
intent. Raising it does not raise the delta limit. Compaction/rematerialization
must enforce the effective bound before allocating or reading the full result.

CLI file reads are capped even if a file grows after its initial metadata check.
Text and encoded input bounds are checked before unnecessary allocation; Python
checks byte input length before cloning it into a Rust-owned buffer. Rust
callers own their input allocation but are checked before publication I/O.
Block sizes must remain within the admitted object provider's capabilities.
CLI `--block-size` preserves complete intent parity with Python and Rust.

Append activity leases are an operator setting, exposed by
`nokv serve --append-activity-lease-ms`; the allowed range is 1 second to 24 hours and the default is 30 minutes. Recovery also
obeys the existing clock-skew safety allowance and lifecycle scheduling. The
lease must cover the deployment's maximum interval between durable upload
progress updates. There is no independent client upload heartbeat. A short
lease can be used for controlled acceptance; its measured recovery time is not
a promise for a deployment using the default lease.

Published and cleaned child records and logical receipts are currently retained
without automatic expiration. This is an explicit identity-retention cost. Failed attempts also retain their
revision reservations and zero-byte object seals; the live sealed object has
no former payload bytes but still incurs provider key/metadata overhead.
Historical provider versions, if enabled, are a separate retention concern.
Removing these records without an equivalent tombstone/replay protocol would
allow forgotten IDs to execute again. Receipts preserve metadata, not permanent
access to the old revision's object bytes. Ordinary snapshot/hold/GC policies
control body retention.

The lifecycle runner first performs a read-only consistency check over retained
publications, parents, and activity markers. It rejects corruption without
automatic repair. This initial validation cost grows with retained history.
Subsequent recovery passes scan an active-publication index, updated atomically
with child lifecycle transitions, so every routine pass does not rescan all
past receipts before finding abandoned active work. Retained row count and
storage growth still require deployment capacity planning; this change does
not introduce an unbounded-throughput or unlimited-retention SLA.

## Downstream demand behind the contract

The following are documented downstream requirements, not claims of existing
NoKV integrations or partnerships. Sources were checked on 2026-09-23.

| Downstream evidence | Consequence for append |
| --- | --- |
| [Temporal AI engineering patterns](https://go.temporal.io/platform-hub/ai-engineering/ai-patterns) describes retryable tool Activities and caller-supplied idempotency keys. | An action ID must survive worker replacement and distinguish deliberate repeated actions from redelivery. |
| [AWS Durable Execution idempotency guidance](https://docs.aws.amazon.com/durable-execution/patterns/best-practices/idempotency/) requires repeat-safe external work. | A recovered append must return its original outcome; merely refusing all further attempts leaves durable workflows unable to finish. |
| [LangGraph persistence](https://docs.langchain.com/oss/python/langgraph/persistence) separates thread checkpoints from shared durable state. | A harness checkpoint alone cannot prove whether an independently committed file append happened; the state layer needs its own receipt and queryable recovery state. |

The last column states the design inference for NoKV. These sources establish
functional demand, not a benchmark target or a promised commercial SLA.

## Architecture decisions

| Candidate | Decision and reason |
| --- | --- |
| One caller ID bound to one publication forever | Protects duplicate effects but strands an admitted action after safe cleanup or a lost CAS. Insufficient for redelivery that must finish. |
| Reuse exact request IDs | Useful RPC deduplication, but cannot represent a logical append across changed routing, process state, or generation-dependent plans. |
| Client-only deterministic attempt counters | Unsafe: an absent predecessor can be admitted by a delayed caller after another caller skips it. Durable predecessor fencing is required. |
| Persist the entire plan and resume that frozen upload | Requires additional descriptor/payload retention and holds; the frozen generation can still lose its CAS permanently. It does not remove the need for safe logical successors. |
| Delete failed objects and forget their keys | Rejected for stable append: a delayed PUT can recreate an untracked payload after deletion. |
| Periodically rescan all failed keys forever | Can eventually reclaim finite delayed writes, but retains an ongoing history-sized scan and leaves a reopening window after every DELETE. |
| Monotonic zero-byte seals plus revision reservations | Selected for failed append objects. Requires qualified conditional replacement and permanent small reservations; avoids delayed DELETE/PUT reopening races. |
| Caller seals keys and submits a trusted provider verdict | Rejected for the public append recovery workflow. Requires callers to execute storage repair correctly and cannot independently verify their claimed evidence. |
| Owner-executed cleanup with exact-token retry receipts | Selected. Reuses fenced lifecycle proofs, requires no caller payload or provider configuration, and distinguishes replaying one recovery request from starting another. |
| Separate parent advance and child admission | Creates a missing-child interval and requires another recovery protocol. Atomic Begin keeps the predecessor proof and successor admission together. |
| Logical parent plus fenced immutable attempts | Selected. Reuses publication durability, cleanup, reference lifetime, and owner fencing while adding durable redelivery completion. |
| Content hash as the identity | Suppresses two intentional actions with the same bytes. Hashes authenticate intent; they do not identify business actions. |
| One immutable event file per action | A useful downstream event model, but requires its own ordering/projection policy and changes the byte-append interface. |

## Version and deployment boundary

The workspace RPC schema is `nokv.workspace.rpc.v12`, the system format is 13,
and the publication value format is 7. A parent row is a new operation kind;
the child includes the monotonic cleanup retry counter. These version gates
also reject the preceding unreleased append candidate's v11/system-12 layout.
Older stores and protocol clients are rejected explicitly. The serving backend
is the published Holt 0.8.6 dependency pinned in Cargo. Existing Holt stores
are inspected through its read-only open path before writable recovery, so a
format rejection does not rewrite their files. There is no automatic
system-format migration, marker-only upgrade, or mixed-version write mode.
This feature provides no qualified migration/export-import procedure for an
incompatible store. Preserve that namespace intact; do not edit its marker or
reuse it as a fresh current-format store. Object-namespace adoption is a
separate binding operation and cannot upgrade the system format.

Red and green fault tests initialize separate version-appropriate stores. A
green restart test must reopen the exact same green Holt directory. NoKV's
locked Holt dependency, operating system, object provider, owner binary, client
binary, Python extension, source commit, and configuration belong in the
acceptance receipt. A locally checked-out Holt head is not evidence for the
backend linked into the tested NoKV binary.

## Functional acceptance requirements

The core product gate is
[`append_product_acceptance_gate.py`](../../scripts/workbench/append_product_acceptance_gate.py).
Public operator workflows are covered by
[`append_operations_acceptance_gate.py`](../../scripts/workbench/append_operations_acceptance_gate.py).
Run its `--help` for the exact invocation. Preserve the previous frozen binary
for red tests and use a separately frozen candidate binary and matching Python
wheel for green tests. Do not replace a red failure oracle with a weaker green
one. The earlier identity gate is retained as historical evidence of the
narrower single-attempt contract.

The functional matrix must include:

- Same-ID concurrent fresh callers synchronized before admission; distinct IDs
  contending on one file; identical bytes under independent IDs; root isolation.
- Successful-response loss at admission and publication, owner and caller
  SIGKILL, same-store reopen, and caller death while the owner stays healthy.
- Staged-object and manifest boundaries, object-provider faults, actual delayed
  PUT arrival after cleanup, monotonic zero-byte seals, and recovery from proven
  failed attempts without an additional user action ID. A seal with an
  unprovable outcome must quarantine, reject caller-supplied provider verdicts,
  and complete only after exact-token recovery through the fenced owner.
- Metadata-only status with the delta file gone; historical append replay
  with its original bytes while the object provider is unavailable, after a
  newer live generation, removal, and workspace name reuse.
- Exact typed intent and incarnation rejection, zero bytes, block boundaries,
  size-limit rejection before admission, and explicit larger result bounds.
- CLI/Python/Rust semantic parity, incompatible-client/store rejection without
  mutation, and deterministic metadata tests for predecessor holes, parent
  receipt atomicity, cross-kind collisions, and maximum legal command shape.
- Public metadata-only inspection across multiple pages, a genuinely sealed
  prefix, missing-row rejection, stale/cross-root/cross-child cursors, and
  inspection without access to earlier transport transcripts.
- Cleanup admission response loss, fresh processes, owner reopen, concurrent
  requests and repeated quarantine. The same saved token must retain its
  original receipt and counter; a deliberate new token may start another round.
  Active and committed attempts must remain untouched.
- A durable downstream queue retains the complete intent before dispatch and
  acknowledges only the stored NoKV receipt. Kill its actual consumer after
  append commit but before queue acknowledgement, then redeliver through its
  actual storage adapter. Identify the harness and integration actually run.
- Old-incarnation cleanup after workspace deletion/rebinding, using metadata
  integration tests where no corresponding public lifecycle entry point exists.
  Do not mislabel fixture-only transitions as public black-box coverage.

Each scenario checks both safety and completion. Compare the entire ordered
content, expected generation increments, stable historical receipt fields,
unchanged live metadata on replay, and relevant object inventory. A safe failure
cannot make a required completion scenario green. Tests must not use Python
`assert` for acceptance conditions that disappear under optimized execution.

Record latency distributions and failures for the measured workload separately
from correctness. Report unexecuted applicable release gates as `NOT QUALIFIED`.
A command-boundary SIGKILL proves that boundary; it does not prove every internal
WAL/fsync interruption point. Internal commit barriers and machine-crash tests
must identify their exact scope and use independent normal-binary recovery.
