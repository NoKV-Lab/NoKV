<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Workbench Contract

Status: normative Agent-facing contract.

The Workbench profile is NoKV's stable Agent-facing product surface. The
workspace architecture implements this profile while keeping physical namespace
records, object keys, operation journals, and routing state outside the
Workbench contract.

The contract sources are:

- this document for behavior, results, errors, and lifecycle semantics;
- the Rust tool definitions and result shaping owned by `crates/nokv-agent/`;
- the frozen normalized input-schema snapshot in
  `crates/nokv-agent/workbench_contract_schema.json`;
- `scripts/workbench/workbench_contract.py` for checking tool names and
  normalized input schemas.

CLI and MCP wiring are consumers of this contract, not schema authorities.

A supported Workbench deployment exposes exactly all 18 tools. Registration
fails closed unless every possible destination owner supports the durable
restore contract and the complete schema.

## Product Boundary

Workbench is a logical workspace API, not a host-filesystem API.

It guarantees:

- a jailed workspace root;
- path-shaped discovery and artifact access;
- conditional publication and deterministic retry;
- indexed metadata queries;
- durable run commits;
- leased point-in-time snapshots;
- durable restore into a new workbench.

It does not guarantee:

- a FUSE mount or host-filesystem path;
- file descriptors, `open`/`close`, `fsync`, `mmap`, or advisory locks;
- uid, gid, mode, ACL, xattr, hardlink, symlink, or special-node semantics;
- empty-directory identity;
- arbitrary directory rename;
- cross-shard atomic filesystem operations.

Programs that require local paths use an explicit materialize/collect adapter.
Those local sandbox paths are not NoKV namespace identities.

## Jail And Logical Paths

The deployment root is `/agents/{agent_id}/wb` and must not be `/`.

This root is durable presentation configuration. It shapes returned paths and
the presentation-path fields in canonical run/restore manifest v1 envelopes,
so a deployment must retain the same value across MCP/CLI restarts and exact
operation replay. It is not a namespace, routing, Holt-key, object-key, or
sharding identity; `RootId` remains the storage and routing authority.

A workbench id:

- starts with an ASCII letter or digit;
- contains only ASCII letters, digits, `_`, and `-`;
- is at most 128 UTF-8 bytes.

The five standard sections are:

```text
input
scripts
outputs
logs
metadata
```

Tool paths are relative to the selected section. The adapter rejects absolute
paths, empty components, `.`, `..`, backslashes, NUL, and a duplicated section
prefix. The metadata core synthesizes the five sections as virtual prefixes.

Entries written by another approved NoKV client outside the standard sections
remain discoverable by the read/query tools with `section: null`. The jailed
write tools cannot address them.

## Tool Surface

| Tool | Stable behavior | Required core capability |
| --- | --- | --- |
| `workbench_create` | Create one workbench and expose the five standard sections. Exact retries converge. | Atomic workspace marker create. |
| `workbench_put_file` | `replace=false` is create-only; `replace=true` is replace-only. It is never upsert. | Publish create-only or replace-if-generation. |
| `workbench_append` | Create when missing, otherwise append after generation CAS; retry write conflicts; return the new size and generation. The returned `digest` identifies the appended delta, not the whole resulting body. | Immutable segment publish plus conditional head advance. |
| `workbench_edit` | UTF-8 exact-string replacement; require one match unless `replace_all=true`; revalidate after a conflict; a byte-identical result does not publish a new generation. | Read with generation plus replace-if-generation. |
| `workbench_list` | Non-recursive, cursor-paginated listing at live state or a snapshot id/name. | Delimited path scan at one exact snapshot version or one stable live workspace incarnation/revision. |
| `workbench_stat` | Compact metadata card without reading the body, at live state or a snapshot. | Exact path read at one version. |
| `workbench_read` | Structured JSON/YAML/text shaping or base64 byte ranges; `if_none_match` uses generation; snapshot reads remain frozen. | Versioned stat and range read. |
| `workbench_grep` | Case-insensitive literal matching, at most 16 OR patterns, optional basename glob; not regex. | Candidate enumeration plus body range reads. |
| `workbench_search` | Metadata predicates, sort, projection, and facets, within one workbench or across the Agent root. | Version-consistent secondary-index query. |
| `workbench_aggregate` | Bounded count/sum/avg/min/max/group/filter/sort over metadata. | Version-consistent aggregate query. |
| `workbench_catalog` | Discover stable field ids and supported query operators. | Index catalog introspection. |
| `workbench_find` | Find workbenches by committed state and run-manifest literal match. | Workspace and commit query. |
| `workbench_commit` | Publish the versioned run manifest with deterministic identity, exact replay, explicit replace, and conflict detection. | Commit-if-head plus durable manifest hold. |
| `workbench_snapshot` | Snapshot a committed workbench, optionally name and annotate it, with a default seven-day and maximum 90-day lease. | Workspace MVCC snapshot plus lifecycle record. |
| `workbench_snapshot_renew` | Resolve id/name and extend only; never shorten; fail loudly after reap. | Conditional live-snapshot renewal. |
| `workbench_snapshot_retire` | Root-bound retirement; first success reports `retired=true`; an exact absent retry reports `retired=false`. | Conditional snapshot retirement. |
| `workbench_snapshot_list` | Report `alive`, `expired`, `retired`, or `reaped` with aliases, annotations, and lifecycle evidence. | Snapshot lifecycle query. |
| `workbench_restore` | Keep source unchanged, require an absent destination, hide staging, restore into a new workbench, and make exact retries converge. | Durable same-shard restore operation. |

Helper behavior such as structured result shaping, base64 encoding, exact-string
editing, grep matching, section projection, and friendly error text belongs in
the Workbench adapter. It must not force corresponding record types into the
metadata core.

`workbench_list` cursors are opaque, scope-bound continuation tokens. They bind
the RootId, workbench, normalized prefix, live-or-snapshot selector,
continuation fence, and last returned child. Snapshot continuations remain at
one exact retained root read version. Live continuations may resolve a newer
root read version only while the target workspace incarnation and revision are
unchanged; target drift fails closed. When the caller did not supply a cursor,
the adapter may discard a partially collected attempt and restart the whole
bounded scan, but it never combines target workspace revisions. Grep cursors
additionally bind the exact pattern set, basename glob, and recursion mode, so
changing query semantics cannot skip earlier candidates. `workbench_catalog`
applies the same whole-result retry rule to its internal
query-digest/read-version cursor. Neither surface turns a naked read version
into permission to read unretained history; durable historical reads still
require a snapshot or another typed `HistoryHold`.

## Generations And Conditional Writes

`generation` is the caller-visible conditional-write token.

- A successful body publication changes generation.
- A failed or byte-identical edit does not.
- `if_none_match` skips the body when generation is unchanged.
- replace/edit/append validate the generation they observed.
- a snapshot freezes the generation visible at its read version.

The core stores a whole-body digest on the resulting immutable revision.
`workbench_append.digest` remains the adapter-computed SHA-256 of only the
appended bytes.

Workbench responses do not contain `inode`, `source_root`, or
`destination_root`. Those names are not Workbench identities, routing inputs,
conditional-write tokens, provenance fields, or result projections.

## Commit Identity

`workbench_commit` continues to publish
`metadata/run_manifest.json` with schema
`nokv.workbench.run_manifest.v1`.

The caller supplies:

```text
content_digest_uri = "sha256:" + 64 lowercase hex characters
```

NoKV computes:

```text
manifest_digest_uri =
  sha256(canonical compact JSON:
         object keys recursively sorted,
         array order preserved)
```

The stable commit identity is:

```text
sha256(
  "nokv.workbench.commit_identity.v1\0"
  || len64be(workbench_id)       || workbench_id
  || len64be(content_digest_uri) || content_digest_uri
  || len64be(manifest_digest_uri)|| manifest_digest_uri
)
```

Server timestamps are excluded. An exact retry returns the existing commit with
`idempotent_replay=true`. A different identity conflicts unless
`replace=true`, and explicit replacement still loses to a concurrent head
change. In particular, after commit A succeeds and commit B explicitly replaces
it, an exact retry of A still returns A's original result; it does not reinterpret
A against B's current head or run-manifest path. This guarantee relies on the
terminal build and manifest-publication operations remaining durable. NoKV does
not currently garbage-collect those rows; a future operation-retention policy
must preserve an equivalent replay tombstone before deleting them.

The lower layer prepares this projection without weakening that facade
contract. The first commit request freezes the source read version, expected
head, explicit replace bit, exact run-manifest condition, and first
owner-observed commit time in one durable build operation. A commit status
returns the full exact request, its opaque digest of every Agent projection
input other than that time, and the immutable staged-manifest binding as durable
preparation. Before a staged manifest exists, recovery compares the current
Agent projection digest with the durable one and fails closed on a different
presentation path or canonical manifest. The CLI/SDK constructs the canonical
envelope only after that check and with the durable time, so a later process
never regenerates its bytes from a local clock or unbound mutable request.
The Agent adapter must recompute that projection digest from the six typed
facade inputs on every fresh commit and recovery attempt; callers cannot supply
or override it. The metadata server exact-binds the opaque digest in the durable
operation but cannot reconstruct or semantically validate facade-only fields
that are absent from the wire request. A raw protocol `CommitRequest` is
therefore an internal trusted boundary, like its caller-supplied content digest,
and is not evidence by itself of a canonically constructed Agent projection.
The canonical envelope is then published under `CommitStaging`: its immutable
revision and commit-owned strong reference become durable, but no
`PathCurrent(metadata/run_manifest.json)` exists yet. One final owner-fenced
metadata command publishes that reserved path and its path reference, advances
`WorkspaceCurrent`, creates the sealed commit, moves the typed Workbench head
and its consumers, completes the build operation, and emits the change event.
Readers therefore observe either the old manifest and head or the new manifest
and head at one commit version. Generic put/remove cannot mutate either
canonical manifest path; restore owns `metadata/restore_manifest.json` through
its distinct `RestoreStaging` authority.

The metadata core stores a typed commit record as the authority and derives
the actual workspace tree digest only after freezing the canonical member
closure:

```text
tree_digest_uri = "sha256:" + lowercase_hex(member_digest)
```

The caller cannot supply or override that digest. This internal binding and
the exact revision holds do not alter the facade identity above. The manifest
path and bytes remain a stable Workbench projection. The persisted body is the
canonical `nokv.workbench.run_manifest.v1` envelope, including the Workbench id
and presentation path, the two caller-visible digests, commit identity, commit
time, and the canonical caller manifest. The artifact descriptor digest and
size describe that full envelope; the typed commit record remains the
authority. Reads and discovery accept the projection only when its canonical
bytes, descriptor, commit identity, and typed commit head agree.
Commit recovery verifies those bytes against the commit-owned binding and the
durable manifest publish-operation result, rather than reading the currently
named run-manifest path.

## Snapshot And Restore

A Workbench snapshot is a leased MVCC recovery point, not a permanent archive.
Its name is an alias for the leased snapshot and does not turn it into a durable
tag.

`snapshot_id` is the non-negative numeric id accepted and returned by the
Workbench schemas. The metadata core stores that id as unsigned 64-bit
big-endian; it does not expose an internal UUID through the facade. Snapshot
names resolve through an exact, unique alias record.
Minting the same snapshot name again preserves the existing latest-mint-wins
behavior: the name resolves to the newest minted snapshot, while older
snapshots remain addressable by numeric id. Renew and retire events do not
move the name, and a terminal newest snapshot does not fall back to an older
mint.

Long-lived dataset and run reuse must use an immutable commit or durable tag.
That distinction lets snapshot expiry release metadata history without deleting
committed artifacts.

While a restore/fork still consumes a snapshot, retirement retains the current
typed `ForkRetentionActive` behavior. Successful publication or terminal abort
releases that consumer exactly once; completed destinations are then
protected by their own immutable revision references rather than by the source
snapshot lease.

Snapshot lifecycle truth lives in typed metadata records.
`metadata/checkpoints.jsonl` does not exist in the Workbench namespace, response
schema, or contract state.

Restore is:

- same Agent root and logical shard;
- source-preserving;
- destination-creating, never in-place;
- invisible until the final workspace marker commit;
- zero-copy for immutable artifact revisions inside one shard;
- idempotent through a deterministic operation identity;
- recoverable after process or owner failure.

The core derives that identity from the exact root, source incarnation and
snapshot/commit identity, and destination workbench id. Initialization has a
separate canonical digest because the stable restore manifest contains the
operation id; including that manifest in the id would be circular. The core
stores the full identity and initialization digests beside the shortened
operation id, so either a hash-prefix collision or a mismatched retry fails
closed instead of resuming the wrong restore.

`metadata/restore_manifest.json` is the canonical
`nokv.workbench.restore_manifest.v1` provenance envelope. It records the
operation id, source and destination Workbench ids and presentation paths, and
the selected snapshot id. Its exact digest, size, and JSON content type are
bound durably when restore preparation begins and are checked again when the
staging workspace is published. Restore member count and member digest remain
typed metadata fields only. `MetaShard` does not generate a second JSON
manifest schema.

## Contract Conformance

A Workbench release is qualified only when it passes both the normalized
input-schema validator and boundary-level result/error tests. Schema validation
alone proves only the tool names and input shapes. Conformance evidence covers:

- all 18 tool names and normalized input schemas;
- typed error code, retryability, and conflict classification;
- jail/path/section projection;
- generation and digest relationships;
- commit identities and exact replay;
- snapshot state transitions and frozen reads;
- restore operation identity, staging invisibility, and terminal replay;
- paginated result membership and ordering.

Internal object keys, Holt keys, owner addresses, retry timing, and snapshot
format versions are not Workbench contracts.
