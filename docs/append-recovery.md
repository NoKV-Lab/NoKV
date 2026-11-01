<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Append Recovery with the Native CLI

How a harness appends under one durable identity, queries the outcome, and
recovers a quarantined append with `nokv workspace-path append`,
`nokv operation status`, `nokv operation inspect`, and
`nokv operation recover`. The [CLI contract](cli-contract.md) defines the
failure envelope and retries.

For an append that must survive a lost reply or a harness restart, save one
operation id before sending the request, then reuse it with the same inputs:

```shell
nokv [route/agent/object options] workspace-path append run-42 logs events.jsonl \
  --operation-id aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --expected-workspace-incarnation-id bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb \
  --text 'step-7-done' --content-type text/plain
```

Replace the example ids with the caller's stable logical operation id and the
intended workspace's incarnation. On the first call, omitting the incarnation
observes and fences the existing workspace. On recovery, omission resolves the
original durable operation first, even if the workspace was deleted or recreated.
Append never creates a missing workspace.

Use exactly one of `--text`, `--base64`, or `--file`. Text defaults to
`text/plain; charset=utf-8` on creation; binary and file input default to
`application/octet-stream`. Existing content type is inherited unless
`--content-type` overrides it. Explicitly use the same type when replaying across
text, file, and Python inputs. The delta limit is 16 MiB; `--max-artifact-bytes`
may lower it. The resulting body also defaults to a 16 MiB limit;
`--max-logical-size` raises that bound and is part of the stable intent.
`--block-size` defaults to 4 MiB and matches Python's `block_size` argument;
retain the same value across retries and surface changes.

The receipt identifies both the logical `operation_id` and its successful
`publication_operation_id`. Its revision, generation, workspace revision, size,
digest, and incarnation describe the original publication, even if another
writer has advanced or removed the path. `replayed` and nullable `commit_version`
describe this SDK call and are not stable receipt fields.

Query a result without resending the delta or contacting the object store:

```shell
nokv [route/agent options] operation status aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
```

The response supplies a machine-readable recovery action:

| `state` | `next_action` | Caller action |
| --- | --- | --- |
| `committed` | `none` | Consume the original `receipt`. |
| `pending` | `poll` | Query the same logical identity again after waiting. |
| `ready_to_retry` | `resubmit_same` | Resubmit the original append inputs under the same logical identity. |
| `quarantined` | `retry_cleanup` | Inspect the failure, restore provider availability, then ask the owner to retry cleanup. |

Status also retains the current physical attempt's `cause_code`,
`failure_message`, and structured `attempt_failure`, including when the logical
operation is `ready_to_retry`.

Inspect and recover without object credentials:

```shell
nokv [route/agent options] operation inspect aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa --limit 32
nokv [route/agent options] operation recover aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --expected-state-digest <saved-operation_token.state_digest>
```

Inspection returns `operation_token` and `publication_token`, each with
`operation_id` and `state_digest`, plus the object namespace, `registered_count`,
`cleanup_cursor`, `remaining_count`, and `entries`. Entries describe retained
staged objects, not a historical inventory of retired keys. Each entry includes
its sequence, identity, expected length, digest, and optional multipart token.
The page limit defaults to 32 and may range from 1 to 192. Continue with the
opaque base64 `next_cursor` using `--cursor`; a stale cursor returns
`Conflict/OperationState`, requiring a fresh inspection instead of mixing pages.

Save the logical token before requesting recovery. Reuse its state digest after
a lost reply or process restart. The owner performs sealing; this command never
resends the append delta. `requested=true` means a cleanup request was durably
accepted, including a replay. Its historical `recovery_receipt` contains the
logical id, publication id, cleanup retry count, and expected state digest.
The top-level status is a fresh observation and may be newer than that receipt;
acceptance is not cleanup completion. Omitting the digest targets the current
state once and is a no-op for pending, cleaned, or committed operations. A later
quarantine needs an intentional new request with a newly inspected token.
`AppendCleanupUnresolved` preserves the saved digest, any known recovery receipt,
and `next_action=retry_same_cleanup`; generic retry handlers must not choose a
new token. After `ready_to_retry`, resubmit the original append inputs.

An interrupted publication must be fenced and cleaned before a successor
attempt can start. Status queries only observe this lifecycle. Persist the delta
or a way to reproduce the exact bytes until the operation commits. An unknown
result or `NotFound` observation is not proof that a concurrent request cannot
commit; errors preserve the logical id, and an unknown outcome uses
`next_action=query_same`.
`cause_code=RequestReplayMismatch` means the identity belongs to different
inputs or a different lifecycle: recover the original intent before retrying.
Never generate a replacement id merely to bypass an uncertain outcome.

The older `workbench_append` tool retains per-invocation identities and cannot
recover one action across separate processes. Use `workspace-path append` for
new retrying harness integrations. The fixed 18-tool contract remains unchanged.
The [append product specification](development/append-product-spec.md)
defines the supported recovery, size, retention, and release boundaries.
