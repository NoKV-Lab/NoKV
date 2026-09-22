# NoKV Python SDK

The native full `nokv` CLI is NoKV's primary product and integration surface.
This direct Python SDK is the secondary choice for callers that need an
in-process API. Agent frameworks should normally expose skills over the CLI.

This package exposes API version `1` for path-native Workbenches and immutable
artifacts. The version is available as `nokv.API_VERSION`; the stable package
exports are listed by `nokv.__all__`. `nokv.__version__` is the NoKV release
line the installed wheel was built from (for example `0.11.0`); it is not the
API version and it changes with every release.

## Install

Every stable NoKV release publishes one abi3 wheel per supported platform as
GitHub release assets, next to a manifest and a checksum file:

| Asset | Platform |
| --- | --- |
| `nokv-<version>-cp39-abi3-manylinux_2_28_x86_64.whl` | Linux x86_64, glibc >= 2.28 |
| `nokv-<version>-cp39-abi3-manylinux_2_28_aarch64.whl` | Linux aarch64, glibc >= 2.28 |
| `nokv-<version>-cp39-abi3-macosx_*_arm64.whl` | macOS Apple Silicon |
| `nokv-<version>-cp39-abi3-macosx_*_x86_64.whl` | macOS Intel |
| `nokv-<version>-python-sdk.json` | manifest: tag, commit, per-wheel SHA-256 |
| `nokv-<version>-python-sdk-SHA256SUMS` | `sha256sum -c` input for the wheels |

The wheels target CPython 3.9 and newer through the stable ABI. Install the
release you pinned by pointing `pip` at that release's assets, and verify the
download against the published checksum:

```shell
version=0.11.0
pip install "nokv==$version" \
  --find-links "https://github.com/NoKV-Lab/NoKV/releases/expanded_assets/v$version"
python -c 'import nokv; print(nokv.__version__, nokv.API_VERSION)'
```

To pin one exact file instead, download the wheel and its checksum file from
`https://github.com/NoKV-Lab/NoKV/releases/download/v<version>/`, run
`sha256sum -c nokv-<version>-python-sdk-SHA256SUMS --ignore-missing`, then
`pip install ./nokv-<version>-cp39-abi3-<platform>.whl`.

The wheel version, the `crates/nokv` package version, and the release tag are
one identity; the release workflow refuses to publish otherwise. A wheel built
from an unreleased commit reports that commit's declared version, so pin by
release tag, not by version string alone, when qualifying a deployment.

Building from source (`maturin build --release` in `crates/nokv-python`)
requires a Rust toolchain and a `protoc` binary; that path is for development,
not for installing the SDK.

## Version 1 surface

- `Client` provides Workbench-scoped create, generation-fenced replace, read,
  stat, list, atomic rename, remove, frozen-snapshot reads, bounded range batch,
  query, materialize, and collect operations. `publish_bytes` and
  `publish_file` accept an optional `expected_workspace_incarnation_id` (32
  lowercase hex, the value `find_workspaces` and `read` metadata report): the
  owner checks it atomically with `expected_generation` before any durable row
  or object exists, and a workbench bound to a different incarnation raises
  `nokv.WorkspaceIncarnationMismatch` (a `RuntimeError` subclass carrying
  `expected`) with nothing written. Callers that omit the argument keep the
  0.11.0 behaviour; a server older than 0.11.1 rejects a fenced request as an
  invalid argument instead of ignoring the fence.
- `Client.append_bytes(workbench, path, data, operation_id, ...)` accepts Python
  `bytes` under a caller-owned stable logical identity. The first admission
  requires an existing workspace. An omitted `expected_workspace_incarnation_id`
  first resolves a recorded operation's original incarnation, then observes the
  live workspace only when no operation was found. Explicit incarnation fences
  are always checked. Save the identity before the first call and reuse the
  exact inputs after a lost reply.
  The result matches native `workspace-path append`: `status`, `operation`,
  `state=committed`, `next_action=none`, logical `operation_id`, successful
  `publication_operation_id`, `workbench_id`, `path`, `artifact_revision_id`,
  `generation`, `workspace_revision`, `logical_size`, `body_digest`, and
  `workspace_incarnation_id`. These describe the original publication.
  `replayed` and nullable `commit_version` are call metadata.
  `content_type=None` inherits an existing artifact's type and uses
  `application/octet-stream` on creation; an explicit type applies to both.
  Use an explicit matching type when replaying between Python and CLI text.
  The delta limit is 16 MiB, checked before copying the Python bytes.
  `max_logical_size=None` means a 16 MiB resulting-body limit; an explicit larger
  bound is part of the intent and must be retained across retries. The default
  block size is 4 MiB and maps to CLI `--block-size`. A retry may start a successor publication only after the
  predecessor is fenced and cleaned; the logical identity never changes.
- `Client.operation_status(operation_id)` queries an append without resending
  its payload or depending on a current workspace. `Client(root_id, routing)`
  needs no object-store configuration; configured object stores are initialized
  lazily on object operations, so a fresh status client works during an S3
  outage. The result reports `state` and `next_action` from the shared Rust SDK:
  `committed/none` with the original `receipt`, `pending/poll`,
  `ready_to_retry/resubmit_same`, or `quarantined/retry_cleanup`. It also
  contains the logical and publication identities, attempt number and phase,
  activity deadline, original target and incarnation, progress, `cause_code`,
  `failure_message`, and `attempt_failure` for a failed physical attempt. The
  logical operation may still be `ready_to_retry` while retaining that failure.
  A query never advances an attempt. Retain or reproduce the original delta
  until commitment; follow `resubmit_same` using `append_bytes`.
- `Client.operation_inspect(operation_id, *, cursor=None, limit=32)` returns the
  same status plus logical `operation_token`, child `publication_token`, object
  namespace, `registered_count`, `cleanup_cursor`, `remaining_count`, and a page
  of retained staged `entries`. Each entry has `sequence`, `object_identity`,
  `expected_length`, `expected_digest`, and optional `multipart_token`. Retired
  keys are not a historical inventory. The limit is 1 to 192; `next_cursor` is
  opaque `bytes` or `None`. Continue with the unchanged cursor. A changed state
  returns `Conflict`; restart inspection rather than combining different pages.
- `Client.operation_recover(operation_id, expected_state_digest=None)` asks the
  owner to retry quarantined cleanup. Both inspection and recovery are metadata
  only and work with `Client(root_id, routing)` without S3 credentials. Persist
  the inspected `operation_token["state_digest"]` (a lowercase hex string) and
  pass it on every retry of one recovery request. `requested=True` means durable
  acceptance, including replay; `recovery_receipt` preserves that round's
  logical id, publication id, cleanup retry count, and expected state digest.
  Top-level status is a fresh observation and may be newer than the receipt.
  Acceptance is not cleanup completion. Without a digest, the method targets
  the current state once and is a no-op for pending, cleaned, or committed
  operations. A later quarantine requires a new intentional recovery request.
  When status reaches `ready_to_retry`, redeliver the original append inputs;
  recovery does not reconstruct or publish the delta.
- Append and status failures carry `operation_id`, observed `state`, `code`,
  expected incarnation in `expected`, `cause_code`, and `next_action=query_same`.
  `publication_operation_id` is `None` until status supplies an observed
  publication. `AppendError` is a `RuntimeError`; incarnation conflicts retain
  the `WorkspaceIncarnationMismatch` subtype with the same recovery attributes.
  `cause_code=RequestReplayMismatch` identifies changed inputs or another
  lifecycle, so recover the original intent instead of blindly resubmitting.
  `retryable` is false for generic retry handlers. An unknown result or a
  `NotFound` observation never authorizes a replacement logical identity.
  Cleanup errors use `code=AppendCleanupUnresolved` and
  `next_action=retry_same_cleanup`, retain `expected_state_digest`, and expose
  any known `recovery_receipt` and its publication id. Retry using that exact
  digest; do not automatically choose a fresh token after a lost reply.
- `WorkbenchFileSystem` is an fsspec compatibility adapter bound to one explicit
  Workbench. Paths must be one of `input`, `scripts`, `outputs`, `logs`, or
  `metadata`, optionally followed by an artifact-relative path. Sections and
  artifact prefixes are virtual directory-shaped projections; no directory
  records are created.
- `nokv.checkpoint` publishes immutable shards before a create-only manifest.
  The manifest is the checkpoint commit point, so incomplete shard sets are not
  discoverable as committed checkpoints.
- `nokv.torch` is an optional `torch.distributed.checkpoint` adapter. Import it
  explicitly after installing the `torch` extra.

The fsspec adapter supports byte modes `r`, `rb`, `w`, `wb`, `x`, and `xb`.
Append, update, text, permissions, inode/dentry identity, mounts, recursive
directory mutation, and an arbitrary root filesystem are intentionally outside
this API. Historical `NoKVFileSystem`, `ReadBuffer`, range-plan, and epoch-reader
types are not part of version 1.

Snapshots require a committed Workbench. Commit and restore must be driven by
the canonical Workbench lifecycle facade; clients must not synthesize a
run-manifest or duplicate the durable workflow locally.

Stable append's size, retention, owner recovery, and release contract is specified
in [the append product specification](../../docs/development/append-product-spec.md).
The fsspec adapter's append mode remains outside this contract; retrying harnesses
should use native `nokv workspace-path append` first, or `Client.append_bytes`
when embedded Python execution is required.
