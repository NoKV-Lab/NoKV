<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Native CLI Contract

Contract `nokv.cli.contract.v2`, reported by `nokv version --json` as
`cli_contract` beside the wire `rpc_schema`. Version 2 adds the
`unauthenticated` and `permission_denied` classes for an owner that
authenticates its clients over RPC TLS, narrows `operator_required` to what
only an operator can change, and replaces the one-Agent-per-root scope with
the authenticated principal.

This is the contract an Agent harness or script relies on when it drives the
native `nokv` CLI: how a command names its effect so a retry is safe, how it
pins the version it changes, how it reports a failure, and how its outcome is
queried later. The 18 Workbench tools keep their frozen input schemas and
results ([Workbench Contract](./workbench-contract.md)); this contract adds the
CLI's own verbs and its failure envelope around them.

## Failures

A failed client command writes exactly one JSON line to stderr, nothing to
stdout, and exits with the code of its failure class:

```json
{"status":"error","code":"Conflict","message":"…","retryable":false,
 "details":{…},"class":"precondition_failed","next_action":"reread_then_retry"}
```

`code`, `message`, `retryable` and `details` are the stable Agent envelope.
`class` says what the failure means for durable state; `next_action` says
what the caller may do next. Branch on `class` and `next_action`, never on
`message`. `retryable` is legacy: it is kept unchanged until the wire error
model carries a class itself, and no new caller should read it.

<!-- exit-codes:begin -->
| Exit | `class` | Meaning |
| --- | --- | --- |
| 0 | | Success, including a replayed result. |
| 1 | `internal` | An unexpected failure. |
| 2 | `invalid_input` | The command or its arguments are invalid; nothing ran. |
| 3 | `not_found` | The named workbench, path, snapshot or operation does not exist. |
| 4 | `precondition_failed` | A create-only, generation, incarnation, head or lifecycle precondition no longer holds; nothing was applied. |
| 5 | `transient` | The request failed before it could take effect; repeating it is safe. |
| 6 | `outcome_unknown` | The request may or may not have taken effect. |
| 6 | `in_progress` | The identified operation is still running. |
| 7 | `identity_conflict` | The identity was already used for other inputs; nothing was applied. |
| 8 | `incompatible` | The client and owner do not share a protocol version or capability. |
| 9 | `operator_required` | Durable or control state needs operator action, or the client's TLS settings do not fit the owner. |
| 10 | `unauthenticated` | The owner does not accept this client's identity: the connection has no TLS where the owner requires it, or its certificate is not a current credential of the owner's tenant. Nothing was applied. |
| 11 | `permission_denied` | The authenticated principal holds no role that allows the request, or it tried to continue an operation another principal began. Nothing was applied. |
<!-- exit-codes:end -->

A refusal of the principal keeps its class when it ends an append or a multi-stage publish: the owner refuses before it applies anything, so an append refused as `permission_denied` or `unauthenticated` is not an unknown outcome, and a publish whose next stage and whose abort are both refused after a role change is `permission_denied`, not `outcome_unknown`. `next_action` is `operator` in both cases.

A Rust panic exits `101`, outside this table. A `nokv workbench-session`
answers each failed line with `{"id","error","class","next_action"}` and keeps
running; the session process itself exits `0` at end of input.

<!-- next-actions:begin -->
| `next_action` | Do this |
| --- | --- |
| `none` | Nothing remains to do. |
| `poll` | Wait, then observe the same identity again. |
| `query_same` | Run `nokv operation status <id>` before deciding. |
| `resubmit_same` | Run the same command with the same identity and arguments. |
| `retry_cleanup` | Run `nokv operation recover <id>` for a quarantined append. |
| `retry_same_cleanup` | Repeat `operation recover` with the saved `--expected-state-digest`. |
| `reread_then_retry` | Read current state, then decide again under a new identity. |
| `fix_input` | Change the command; repeating it unchanged fails the same way. |
| `do_not_retry` | Stop; repeating the command cannot succeed. |
| `operator` | Stop and hand the state to an operator. |
<!-- next-actions:end -->

An unknown outcome is resolved through the command's identity. With a caller
operation id, query it. With a caller request id or a content-derived identity,
repeat the same command: it replays or applies once. A command that mints a
fresh identity per call (`workbench_put_file`, `workbench_append`, and
`workbench_edit` without `operation_id`, `workbench_snapshot`, and `collect`
without `--operation-id`) may apply twice if repeated, so its unknown outcome
says `reread_then_retry`.
A read never reports an unknown outcome.

## Retries

A command repeats an attempt that failed before it could take effect: a
refused or dropped connection, or an owner answer marked `retryable`. A
request that reached a changed owner is re-routed and repeated at once; every
other repeat waits first, `--retry-backoff-ms` before the second attempt,
doubling per further attempt and never longer than `--max-retry-backoff-ms`.
The defaults are three attempts (`--max-attempts`, 1 to 16) with no pause,
which an owner restart of a few seconds exhausts. A `workbench-session` that
must outlive such a restart starts with `--max-attempts 16
--retry-backoff-ms 250`: it waits about 26 seconds in all before giving up. A
backoff above its bound is `invalid_input`. The Python SDK takes the same
options as `Client(..., max_attempts=3, retry_backoff_ms=0,
max_retry_backoff_ms=2000)`.

Exhausted attempts fail with class `transient` when no attempt can have
taken effect and `outcome_unknown` when the last one may have reached the
owner; both are resolved as described above. A caller that repeats a
create-only `workbench_put_file` after an unknown outcome gives it an
`operation_id` so an applied first attempt replays its receipt; without one,
`precondition_failed` (`AlreadyExists`) on the repeat may be its own earlier
write, and the caller reads the path back before deciding. An identified
write that was in flight when the owner died is re-observed with the same
budget when a backoff is set: the restarted owner aborts the attempt it did
not admit, and the command then publishes the next attempt under the same
id. `in_progress` is answered when the budget runs out, or at once without a
backoff, and the caller then follows `poll`.

## Identity

| Command | Identity | Replay |
| --- | --- | --- |
| `workspace-path put`, `collect --operation-id` | `--operation-id HEX32` | Same id and inputs from any process return the original receipt with `replayed: true`. |
| `workspace-path append` | `--operation-id HEX32` | As above, over fenced attempts. |
| `workbench_put_file`, `workbench_append`, `workbench_edit` | optional `operation_id` argument (HEX32) | Same id and inputs from any process return the original receipt with `idempotent_replay: true`; other inputs are `identity_conflict` ([Workbench Contract](./workbench-contract.md#operation-identity)). |
| `workspace-path rename`, `remove` | `--request-id HEX32` | Same id and arguments replay, including after an owner restart. |
| `workbench_commit`, `workbench_restore` | Derived from the content | Repeating the same content replays. |
| `workbench_create`, `snapshot_renew`, `snapshot_retire` | None | Repeating converges. |

The same id with other inputs is `identity_conflict` and changes nothing. A
request id and an operation id are separate namespaces. Use a fresh,
time-ordered 128-bit id per intent (for example a UUIDv7 without hyphens) and
keep it until the outcome is known; ids are retained without expiry. A failed
attempt of a `put` admitted nothing, so repeating the same command starts its
next attempt under the same caller id.

## Version Constraints

`nokv workspace-path stat <workbench> <section> <path>` reports a path's
tokens in one metadata read: `workspace_incarnation_id`, which is never
reused, `workspace_revision`, the path `generation`, and the immutable
`artifact_revision_id`. A missing path reports `exists: false` with its
workspace's incarnation.

`put` without `--expected-generation` is create-only; with it, the path must
still be at that generation. `--expected-workspace-incarnation-id` fences a
`put` or an `append` on the workspace instance. A refusal is
`precondition_failed` and, for a path conflict, carries
`details.current_generation`.

A path generation restarts at `1` after the path is removed and recreated, so
a generation alone cannot tell two lineages apart. Compare
`artifact_revision_id` from `stat` or a receipt when that matters.

## Operation Results

`nokv operation status <HEX32>` observes an append, publish, commit or restore
without a mutation. `nokv operation status --commit-identity <HEX64>` names a
commit by the `commit_identity` that `workbench_commit` reports. A result
reports `state` (`running`, `succeeded`, `aborting`, `failed`, `quarantined`,
or the append states) with its `next_action`, and a succeeded operation
carries its stored `receipt`: the original result, not a re-evaluation of
current state. Over RPC TLS a result also names `initiator`, the principal
that began the operation, as 32 lowercase hex characters; it is `null` on a
plaintext deployment. The Python SDK's `operation_status` reports the same
fields. An absent operation is `not_found` with `resubmit_same`: absence
is a point-in-time observation, and resubmitting the same command admits it at
most once.

## Python SDK

The direct Python SDK reports this contract on its exceptions: every
exception a `Client` call raises carries `error_class` and `next_action` with
the names above, including `unauthenticated` and `permission_denied` from an
owner that authenticates the client's `TlsConfig` certificate. `Client.put_bytes` is `workspace-path put` and
`Client.operation_status`, whose `commit_identity` is `--commit-identity`, is
`operation status`; both return the fields this CLI prints, and a put or an
append made through one surface replays through the other. See
[the Python SDK](../crates/nokv-python/README.md).

## Scope

Over RPC TLS the owner authenticates each client as a principal of the
root's tenant and authorizes each request by the principal's role, so many
Agents share one root. A request id and an operation id belong to the
principal that claimed them: another principal presenting the same id with
other inputs is `identity_conflict`, and one presenting a durable
operation it did not begin is `permission_denied`, except that an operator
continues any operation. The owner that cannot refresh its tenant policy
answers `transient` with `resubmit_same`. Without RPC TLS the owner
authenticates nobody: every client of a root shares its identity namespace,
and the `unauthenticated` and `permission_denied` classes do not occur.
