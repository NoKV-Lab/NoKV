<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Durable append and recovery

A queue worker appends an event, NoKV commits it, and the worker dies before
acknowledging its queue message. With a saved logical operation ID, the next
worker can recover the original receipt instead of appending the event twice.
NoKV also lets that same logical action make progress after an unsuccessful
publication attempt has been safely cleaned up.

Use the native `nokv workspace-path append` command for this contract. Embedded
Python callers use `Client.append_bytes`; both delegate recovery to the same
Rust SDK. The 18-tool `workbench_append` operation retains its existing
per-invocation identity and generation-CAS behavior. Repeating that tool in a
new process does not recover one durable logical action. The deprecated MCP
sidecar and fsspec append mode are not integration paths for this feature.

This guide describes the checked-in implementation. A merged change is not a
published release: use a binary and, when applicable, a Python wheel built
from a matching qualified commit or a release that explicitly includes this
feature. `nokv version --json` reports the binary's source identity. A package
version string alone does not distinguish builds from unreleased commits.

## Before sending an append

Use an existing, provisioned deployment as described in
[deployment preflight](workbench-preflight.md). The CLI requires its real
`RootId`, bound `AgentId`, and etcd routing; a static owner address alone is
insufficient for these Agent-facing commands. The first append requires an
existing workspace. It may create a missing file, but never the workspace.
New publications need the deployment's matching S3-compatible object namespace
and a provider admitted for append sealing. Status, inspection, and cleanup
recovery need metadata connectivity but no caller-side object configuration.

Persist an outbox or queue record **before dispatch**, containing:

- The root and a 128-bit logical `operation_id`, encoded as 32 lowercase hex
  characters. Keep it unique across append, publish, commit-build, and restore
  operations in that root.
- The workbench, normalized section/path, and intended workspace incarnation
  when known. Keep the root with the ID: the same ID in a different root does
  not identify the original action.
- The exact delta bytes, or durable storage from which those exact bytes can
  be recovered. A content digest alone is insufficient.
- The content-type policy, block size, and effective maximum resulting size.
  Preserve all these options across retries.

Generate an ID once when accepting a business action, then commit the record
using the application's durable queue/database transaction. In-memory state,
a freshly generated UUID on each delivery, or an unflushed JSON file is not a
durable outbox. Separate actions receive separate IDs even if their bytes match.
Retargeting an admitted action to a fork or another path is an intent mismatch.

The incarnation identifies a particular lifetime of a workspace name. If it
is omitted, the SDK first looks for an admitted operation and uses its original
incarnation; only an unknown operation resolves the current workspace name.
Thus a committed action remains recoverable after deletion/recreation, while
an unfinished old action cannot write into the replacement workspace. An
explicit `expected_workspace_incarnation_id` must match; it is never silently
updated. Save a known incarnation when accepting work for a particular instance.

## CLI: submit the saved action

The following Bash examples require `jq`, an existing workspace, and the
deployment values in `AGENT_ID`, `ETCD_ENDPOINT`, `OBJECT_BUCKET`, `OBJECT_ROOT`,
`OBJECT_REGION`, and `OBJECT_ENDPOINT`. Configure the same provider credentials
as your other NoKV object operations. An optional `ETCD_KEY_PREFIX` defaults to
`/nokv/control`. Connection settings belong to deployment configuration, not
to the immutable append intent.

For illustration, a retained application record named `append-intent.json`
has this shape. Replace the example root/ID/workbench with values from your
durable queue; this JSON shape is an application example, not a NoKV wire API.
The base64 value encodes `{"step":7}` followed by a newline.

```json
{
  "root_id": "11111111111111111111111111111111",
  "operation_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "workbench": "run-42",
  "section": "logs",
  "path": "events.jsonl",
  "expected_workspace_incarnation_id": null,
  "content_type": "application/x-ndjson",
  "block_size": 4194304,
  "max_logical_size": 16777216,
  "data_base64": "eyJzdGVwIjo3fQo="
}
```

Read the saved values on every delivery:

```bash
set -euo pipefail
job=append-intent.json
operation_id=$(jq -er '.operation_id' "$job")
route=(--root-id "$(jq -er '.root_id' "$job")"
       --agent-id "${AGENT_ID:?}"
       --etcd-endpoint "${ETCD_ENDPOINT:?}"
       --etcd-key-prefix "${ETCD_KEY_PREFIX:-/nokv/control}")
objects=(--object-bucket "${OBJECT_BUCKET:?}"
         --object-root "${OBJECT_ROOT:?}"
         --object-region "${OBJECT_REGION:?}"
         --object-endpoint "${OBJECT_ENDPOINT:?}")
incarnation=$(jq -r '.expected_workspace_incarnation_id // empty' "$job")
fence=()
if [[ -n "$incarnation" ]]; then
  fence=(--expected-workspace-incarnation-id "$incarnation")
fi

nokv "${route[@]}" "${objects[@]}" workspace-path append \
  "$(jq -er '.workbench' "$job")" "$(jq -er '.section' "$job")" \
  "$(jq -er '.path' "$job")" \
  --operation-id "$operation_id" "${fence[@]}" \
  --base64 "$(jq -er '.data_base64' "$job")" \
  --content-type "$(jq -er '.content_type' "$job")" \
  --block-size "$(jq -er '.block_size' "$job")" \
  --max-logical-size "$(jq -er '.max_logical_size' "$job")"
```

CLI paths are relative to the selected section: use `logs events.jsonl`, not
`logs logs/events.jsonl`. Valid sections are `input`, `scripts`, `outputs`,
`logs`, and `metadata`. Paths reject absolute paths, empty components, `.` and
`..`, backslashes, and NULs. Append accepts exactly one of `--text`, `--base64`,
or `--file`; it does not add a newline. For larger deltas, use `--file` with a
retained regular file to avoid OS command-line length limits. The reader rejects
symlinks and non-regular inputs and caps bytes read even if the file grows.

All options that select routing or objects precede `workspace-path`; append
options follow the target. `--operation-id` is required. The incarnation,
content type, block size, and resulting-size bound are optional with the
defaults below. There is no caller-supplied generation for this command: the
SDK handles publication CAS and fenced successor attempts.

On success, stdout contains JSON with `status=success`, `operation=append`,
`state=committed`, and `next_action=none`. The stable receipt fields are:

| Fields | Meaning |
| --- | --- |
| `operation_id` | The caller's logical action. |
| `publication_operation_id`, `artifact_revision_id` | Its successful publication attempt and immutable revision. |
| `workbench_id`, `path`, `workspace_incarnation_id` | The original target; `path` includes the section. |
| `workspace_revision`, `generation` | Versions at that publication. |
| `logical_size`, `body_digest` | Complete resulting body length and SHA-256 digest URI. |

`replayed` and nullable `commit_version` describe this SDK call; exclude them
when comparing or retaining the stable receipt. A replay can return generation
1 even if the current path is at generation 3 or has been removed.

Durably save the stable receipt before acknowledging the business queue
message. If the consumer crashes between those steps, redelivery reads the
saved action and either its saved receipt or NoKV's original result. Keep
completed IDs deduplicated according to the application's delivery contract.
NoKV cannot atomically acknowledge an external queue for the caller.

## Query first, then follow the recovery action

The following commands reuse the `route` array and `operation_id` above and
do not use the `objects` array:

```bash
nokv "${route[@]}" operation status "$operation_id"
```

| `state` | `next_action` | What to do |
| --- | --- | --- |
| `committed` | `none` | Persist the original `receipt`, then acknowledge the action. |
| `pending` | `poll` | Wait and query this ID again. An active/abandoned child has not reached a safe retry point. |
| `ready_to_retry` | `resubmit_same` | Redeliver the same bytes and options under the same logical ID. |
| `quarantined` | `retry_cleanup` | Inspect the failure, correct the dependency problem, then request owner cleanup with the saved token. |

Status queries do not advance an attempt. An unsuccessful physical attempt
does not mean the logical action has failed permanently. Only completed
cleanup permits a successor; repeated queries do not reconstruct the delta.
Schedule polling with the caller's backoff and deadline. Owner activity leases
and cleanup scheduling can make recovery take longer than one client call.
The owner setting `--append-activity-lease-ms` defaults to 30 minutes, accepts
1 second to 24 hours, and has an additional clock-skew allowance. There is no
independent client upload heartbeat; operators must budget for upload progress
before shortening the lease.

Status includes the current `publication_operation_id`, zero-based `attempt`,
`attempt_phase`, `cleanup_retry_count`, `activity_deadline_ms`, original target,
`observed_state`, `progress`, `cause_code`, `failure_message`, `attempt_failure`,
and nullable `receipt`. `progress` contains completed/total rows and bytes;
totals may be null. `attempt_failure` retains the physical attempt's code,
message, retryability, conflict kind, and current generation when present.
Use the public `state`/`next_action` pair for decisions; an exception's raw
`state` may instead be `Running`, `Quarantined`, or unknown.

### Inspect pages and save a recovery token

```bash
nokv "${route[@]}" operation inspect "$operation_id" --limit 32 > inspection.json
cursor=$(jq -r '.next_cursor // empty' inspection.json)
if [[ -n "$cursor" ]]; then
  nokv "${route[@]}" operation inspect "$operation_id" \
    --limit 32 --cursor "$cursor" > next-page.json
fi
```

Continue until `next_cursor` is null. Each page includes the status fields,
`action=inspect`, `operation_token`, `publication_token`, `object_namespace_id`,
`registered_count`, `cleanup_cursor`, `remaining_count`, and `entries`.
Each entry has `sequence`, `object_identity`, `expected_length`,
`expected_digest`, and nullable `multipart_token`.

The page contains retained staging rows for the current child, not every key
ever used by the logical action. `remaining_count` is the retained interval
`registered_count - cleanup_cursor`, not a count of unpublished bytes or proof
that a published object should be cleaned. Do not delete inspected keys.

The base64 continuation cursor binds the root, logical ID, exact observation,
and last sequence. A changed phase, child, or cleanup state invalidates it with
`Conflict/OperationState`; discard the partial inspection and start again.
Malformed cursors and cursors used with another root/ID are `InvalidArgument`.
The default page limit is 32, with an allowed range of 1–192.

`operation_token.state_digest` is different: it is a 64-character lowercase
hex token for one cleanup recovery request. Persist that token in the durable
operator task before sending `recover`; a plain `inspection.json` redirection
in this example is not the durable handoff. After correcting the reported
dependency failure, use the token saved while the operation was quarantined:

```bash
if [[ "$(jq -er '.state' inspection.json)" == quarantined ]]; then
  recovery_digest=$(jq -er '.operation_token.state_digest' inspection.json)
  nokv "${route[@]}" operation recover "$operation_id" \
    --expected-state-digest "$recovery_digest"
fi
nokv "${route[@]}" operation status "$operation_id"
```

The owner seals the failed child's staged keys; the caller needs neither
payload nor object credentials for this request. `requested=true` means the
cleanup request was durably accepted, including a replay. Its
`recovery_receipt` contains `operation_id`, `publication_operation_id`,
`cleanup_retry_count`, and `expected_state_digest`. The top-level status and
`operation_token` are a fresh observation and can be newer than that historical
recovery receipt. Neither acceptance nor `replayed=true` proves cleanup or
append completion.

Repeat an uncertain recovery request with **the same saved digest**, even if
the operation has since progressed or become quarantined again. If that request
was durably accepted, it replays that round's receipt. Otherwise, a now-stale
token can be rejected without starting a new round. A new round requires a
deliberate new inspection/token. Without `--expected-state-digest`, each
call targets the current observation once: pending, cleaned, and committed
operations return `requested=false` with no recovery receipt. Once status is
`ready_to_retry`, submit the original append again; `recover` never publishes
the delta or starts its successor on the caller's behalf.

## Python: use the same retained intent

The Python path combines section and relative path into `logs/events.jsonl`.
This example reads the same durable record as the CLI example, using the same
explicit content type and bounds. The installed wheel must include the feature
and match the deployment; see the [SDK installation guide](../crates/nokv-python/README.md).

```python
import base64
import json
import os
from pathlib import Path

from nokv import Client, ObjectStoreConfig, RoutingConfig

job = json.loads(Path("append-intent.json").read_text())
routing = RoutingConfig.etcd(
    [os.environ["ETCD_ENDPOINT"]],
    key_prefix=os.environ.get("ETCD_KEY_PREFIX", "/nokv/control"),
)
objects = ObjectStoreConfig.s3(
    bucket=os.environ["OBJECT_BUCKET"],
    root=os.environ["OBJECT_ROOT"],
    region=os.environ["OBJECT_REGION"],
    endpoint=os.environ["OBJECT_ENDPOINT"],
)
client = Client(job["root_id"], routing, object_store=objects)
result = client.append_bytes(
    job["workbench"],
    f'{job["section"]}/{job["path"]}',
    base64.b64decode(job["data_base64"], validate=True),
    job["operation_id"],
    content_type=job["content_type"],
    block_size=job["block_size"],
    max_logical_size=job["max_logical_size"],
    expected_workspace_incarnation_id=job["expected_workspace_incarnation_id"],
)
print(result)  # Persist the stable receipt before acknowledging the queue.

# A separate metadata-only client needs no ObjectStoreConfig.
metadata = Client(job["root_id"], routing)
status = metadata.operation_status(job["operation_id"])
page = metadata.operation_inspect(job["operation_id"], limit=32)
```

`append_bytes` accepts `bytes`; there is no separate Python `append_file`
method. Preserve input bounds when reading files in application code. Its
success/status fields match the CLI. `operation_inspect(operation_id, *,
cursor=None, limit=32)` returns `next_cursor` as opaque **bytes**, not a base64
string; pass it unchanged to the next Python call. For a cross-language
handoff, base64-encode/decode it exactly once.

`operation_recover(operation_id, expected_state_digest=None)` takes the saved
lowercase hex **string** from `page["operation_token"]["state_digest"]`.
The same quarantine check, durable token persistence, and status decision
table apply. A completed append can also be replayed through a metadata-only
client when the original payload/options are available: receipt validation
happens before object-store initialization. Querying status needs no payload.

## Defaults, errors, and boundaries

| Option | CLI | Python | Default / rule |
| --- | --- | --- | --- |
| Input | Exactly one of `--text`, `--base64`, `--file` | `data: bytes` | At most 16 MiB per delta; CLI `--max-artifact-bytes` can lower this. |
| Result size | `--max-logical-size` | `max_logical_size` | 16 MiB including existing body; explicit larger bounds do not raise the delta limit. |
| Block size | `--block-size` | `block_size` | 4 MiB; positive and within provider admission limits. |
| Content type | `--content-type` | `content_type` | Omission inherits an existing type; creation defaults differ by input form. |
| Incarnation | `--expected-workspace-incarnation-id` | `expected_workspace_incarnation_id` | Omission resolves original admission first, otherwise the live workspace. |

CLI text creation defaults to `text/plain; charset=utf-8`; CLI file/base64 and
Python default to `application/octet-stream`. An explicit type applies on both
creation and append. The full intent binds the creation default **and whether
an override was supplied**, even when the current file already has that type.
Do not switch from omitted type to explicit type on retry. For interchangeable
CLI/Python delivery, choose one explicit type on the first submission and keep
it. A default/explicit 16 MiB result limit normalizes to the same intent; a
different bound or block size does not. Existing producer, manifest identity,
and typed index fields are inherited; these append interfaces do not accept
replacement metadata fields.

CLI success JSON goes to stdout. Runtime errors exit nonzero and are written
to stderr as `nokv: { ... }`, with top-level `status`, `code`, `message`, and
`retryable`; recovery attributes are under `details`. Early argument-parsing
errors can be plain text. Python exposes runtime append failures as
`nokv.AppendError`; workspace-incarnation conflicts use
`nokv.WorkspaceIncarnationMismatch`, a separate `RuntimeError` subtype with
append recovery attributes. Invalid Python argument types or identity encoding
may raise `TypeError`/`ValueError` before a request is constructed.

| Observation | Response |
| --- | --- |
| `AppendUnresolved`, timeout, or unknown outcome | Keep the root, ID, and intent; query the same operation. Unknown state is not proof of no effect. |
| `NotFound` | Verify root/ID and query or redeliver the saved action as appropriate; a concurrent admission may still occur. Never infer permission for a replacement ID. |
| `cause_code=RequestReplayMismatch` | Restore the original intent or correct the caller's identity assignment; changing inputs cannot recover this action. |
| `Conflict` with workspace-incarnation conflict | Investigate the original workspace lifetime. Do not silently target its replacement. |
| `InvalidArgument` | Fix invalid input before a new admission; preserve an already admitted action's original intent. |
| `AppendFailed` with `ProviderAdmissionRejected`, `ProviderAdmissionUnavailable`, or `ProviderAdmissionInconclusive` | Inspect capability/configuration or provider availability. Admission failures are not cached; a repaired provider can be checked again using the same client. |
| `AppendCleanupUnresolved` | Retry `recover` with its saved `expected_state_digest`; retain any known `recovery_receipt`. |

Append errors preserve `operation_id`, optional raw `state`, `cause_code`, and
`next_action=query_same`. Cleanup uncertainty instead uses
`next_action=retry_same_cleanup`; it preserves the expected digest and any
already observed historical cleanup receipt/publication ID. Generic retry
handlers must not interpret `retryable=false` as permission to discard the ID
or allocate a fresh one. Follow the explicit action rather than parsing text.

A zero-byte delta is still an action: a new ID can create an empty file or
advance an existing generation, while replaying that ID cannot do so again.
Concurrent successful actions have a publication order; their caller start
times do not establish FIFO order. This is bounded byte append, not a
transaction across a queue, an external API, and NoKV, nor a guarantee of
unlimited log size or sustained throughput.

Logical receipts and child records currently have no automatic expiration.
Failed attempts retain small permanent revision reservations and zero-byte
seals so delayed PUTs cannot reopen abandoned keys. A historical receipt proves
the original result; it does **not** retain its body forever. Use the documented
commit/tag/retention lifecycle when historical bytes must remain readable.
Capacity-plan this retained metadata and provider-key overhead. Owner lease,
schema upgrade, provider qualification, and acceptance details are in the
[append product specification](development/append-product-spec.md). The
[qualification record](development/append-qualification.md) identifies the
executed local and remote tests and their remaining boundaries.

Rust integrations use `IdempotentAppendOptions` and
`WorkspaceClient::append_artifact_idempotent`, with the shared
`get_append_operation`, `inspect_append_operation`, and
`recover_append_operation` methods. Reuse those compositions instead of
implementing another append or cleanup state machine.
