<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Append identity and response-loss recovery

A harness can persist a caller operation ID before sending an append. Retrying
that ID means one publication attempt, even after the harness or shard owner
restarts. A different operation ID represents a different action, including
when its bytes are identical. The native CLI is the primary integration
surface; the Python SDK uses the same client implementation.

## Contract and scope

The stable append request binds the complete caller intent with a full SHA-256
commitment in the existing publish operation record. The intent includes the
normalized target and explicit append inputs and policies. It is distinct from
the generation-dependent immutable publication plan. The client computes this
commitment from its typed inputs; the server exact-binds it as opaque input and
does not claim to verify delta bytes that the metadata RPC does not carry.

One operation identity admits at most one publication attempt. It never derives
a second identity after a timeout, read conflict, or path-generation conflict.
Before consulting the live path, a retry authenticates the operation's intent,
target, workspace incarnation, and artifact revision. Identity reuse with a
changed request fails closed.

Callers must allocate operation IDs uniquely across the public lifecycle kinds
within one root: append/publication, commit build, and restore. Domain-separated
IDs persisted by the harness are one way to meet this requirement. The server
also enforces this rule atomically: each initial admission checks that both
other kinds' operation keys are absent in the same metadata command that
creates its own row. A preliminary GetOperation lookup alone cannot close this
race. No separate registry or new key family is needed. GetOperation remains
fail-closed if corrupt or externally supplied state contains multiple kinds for
one identity. Internal commit and restore manifest publications use distinct,
domain-separated identities and keep their existing lifecycle authority.

A published operation returns its original compact receipt: operation ID,
target, workspace revision, generation, artifact revision, logical size, and
body digest. This is a historical result. A later append, removal, or replacement
of the live path does not revoke that success and does not trigger another
append. The receipt does not claim that its revision is still the live head or
that its bytes remain retained forever.

The response envelope's `commit_version` and `replayed` are metadata about the
current call, not fields of the stable historical result. The first publication
may carry a metadata commit version; recovery through GetOperation carries no
new commit version and marks the client result as replayed. Consumers should
compare the receipt fields listed above, rather than requiring byte-identical
JSON envelopes across the first call and retries.

An admitted operation without a terminal success returns a queryable pending or
terminal failure result. A new Begin RPC cannot replan, take over, or clean up
that operation. Byte-identical retries of the original RPC continue through the
existing request ledger. The existing fenced publish lifecycle owns recovery,
including the publication-absence proof required before Finalizing cleanup.
The caller must not interpret timeout, NotFound, or Finalizing as proof that an
append did not happen, nor automatically retry with a fresh operation ID.

For example, a queue-driven harness persists an action ID for task 42, step 7
before appending its completion event. If its worker dies after publication,
the replacement worker sends the same ID and event and recovers that receipt.
Two independently scheduled steps may emit identical events and still need two
different IDs. A branch of a run also needs its own action identity: changing
the target workspace under the parent's ID is an intent mismatch.

A worker that dies during upload has a different recovery boundary. Its new
process can discover the admitted operation, but this API does not resume that
upload or create another attempt. The existing owner lifecycle can finish its
fenced cleanup. Workloads requiring automatic completion despite admitted CAS
conflicts or abandoned uploads need the durable attempt mapping described below.

The workspace incarnation fence is mandatory for stable append. It prevents a
request for an old workspace instance from writing into a later instance with
the same name. Root routing and owner fencing continue to apply to lookups,
request deduplication, and mutation.

Terminal operations currently have no automatic GC. Their retained records
provide the identity tombstone and historical receipt. Any future retention
policy must preserve an equivalent replay contract before removing them. This
change does not add a hidden artifact reference or pin every old revision just
to recover metadata. PublishPreparation projects existing target, incarnation,
revision, and intent fields; it does not store a full descriptor, whose legal
index fields can exceed the 16 KiB operation-record budget.

## Architecture alternatives

| Alternative | Reason for the decision |
| --- | --- |
| Stable caller ID alone | Re-reading the live head builds a different append plan after success; an ID flag alone cannot authenticate or recover the original intent. |
| Selected: one attempt through the existing publish lifecycle | Keeps publication, cleanup, fencing, and durability in one state machine while making response-loss recovery exact and bounded. |
| Logical append with durable attempt mapping | Appropriate if automatic rebase after concurrent-writer conflicts becomes a required product contract. It needs a durable parent or predecessor fence proving every earlier attempt cannot publish. It is outside this single-attempt change. |
| RPC request-ID reuse | Protects an exact encoded command, including route-dependent fields. It cannot alone represent a logical append across process and owner changes. |
| Content-hash deduplication | Incorrectly suppresses two intentional actions carrying identical bytes. |
| Immutable event files keyed by action ID | A useful harness design for event-oriented workloads, but changes the byte-append contract and needs an ordering/projection policy. It does not repair append. |

Deterministic attempt numbering without durable predecessor records is unsafe:
one caller may skip an unadmitted attempt after a read conflict while another
caller later publishes that same attempt. Both it and the first caller's next
attempt can then succeed. This change deliberately has no such attempt chain.

## Storage and protocol versions

The strict workspace system format advances from 10 to 11; the publication
record family advances from value format 4 to 5; the RPC schema advances from
`nokv.workspace.rpc.v9` to `nokv.workspace.rpc.v10`. Older stores and protocol
schemas are rejected. No migration, compatibility decoder, or marker-only
upgrade is introduced. Red and green acceptance therefore use independently
initialized stores. Green recovery tests reopen the same green-version Holt
directory after owner termination.

## Acceptance requirements

Tests must retain actual owner/object-provider execution evidence for response
loss followed by a new CLI process and owner restart; one logical append must
produce one event. Additional cases cover a newer live head, changed intent,
concurrent callers with the same ID, distinct IDs with identical bytes,
incarnation changes, and admitted pending operations. Unit tests protect exact
binding, codec versions, server request deduplication, historical receipts,
owner fencing, and refusal to replan or clean up an admitted operation.

The executable gate requires Docker, AWS CLI, etcd, and etcdctl. Build the CLI
with `cargo build -p nokv --features etcd`, freeze a copy of that binary, and
install a wheel built from the same checkout into an isolated Python environment.
Run the full qualification from the repository root:

```shell
python3 scripts/workbench/append_identity_recovery_gate.py \
  --mode qualified --scenario all \
  --source-dir "$PWD" --nokv-bin /absolute/path/to/frozen-nokv \
  --python-executable /absolute/path/to/venv/bin/python \
  --evidence-dir /absolute/path/to/new-evidence-directory
```

Use `--mode baseline` with a frozen pre-fix binary and a separate evidence
directory to reproduce the duplicate append. The gate retains the dropped
successful RPC response, process/owner restart evidence, materialized bytes,
binary and Python-extension identities, current source hashes, and cleanup
results. `GREEN_PARTIAL` identifies a selected subset or a run without Python;
pending-operation cases qualify safety separately from completion.

This contract covers NoKV publication. It does not make email, browser actions,
payments, or other external effects execute once. A downstream harness must
assign and persist identities for those effects at their own authority.
