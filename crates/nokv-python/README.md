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
- `Client.append_bytes(workbench, path, data, operation_id, ...)` appends Python
  `bytes` under a caller-persisted logical identity. `operation_status`,
  `operation_inspect`, and `operation_recover` query its outcome and request
  owner-executed cleanup without payloads or object credentials. These methods
  share the native CLI's durable append state machine; see
  [Durable append](../../docs/append.md) for the CLI-first guide, a shared
  persisted-input example, all defaults and result fields, and recovery steps.
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

## Durable append in Python

These methods describe the checked-in API. Do not assume the example `0.11.0`
release wheel above includes them. A main-branch merge does not publish a new
wheel. Use a matching qualified source build, or a release that explicitly
includes durable append; record the exact commit/asset identity rather than
only `nokv.__version__`. Source builds and the owner must satisfy the same
[format and release boundary](../../docs/development/append-product-spec.md).

| Method | Input and result |
| --- | --- |
| `append_bytes(workbench, path, data, operation_id, content_type=None, block_size=4194304, max_logical_size=None, expected_workspace_incarnation_id=None)` | `data` is `bytes`; `path` includes the section, such as `logs/events.jsonl`. Returns the original committed receipt. |
| `operation_status(operation_id)` | Metadata-only observation with `state`, `next_action`, and nullable historical `receipt`. |
| `operation_inspect(operation_id, *, cursor=None, limit=32)` | Metadata-only retained staging page. `next_cursor` is opaque `bytes`; limits are 1–192. |
| `operation_recover(operation_id, expected_state_digest=None)` | Ask the owner to retry quarantined cleanup. The digest is a saved 64-character lowercase hex string. Acceptance is not completion. |

The following reads a previously persisted application intent. It needs a
reachable, provisioned root and etcd endpoint, but no `ObjectStoreConfig`:

```python
import json
import os
from pathlib import Path

from nokv import Client, RoutingConfig

job = json.loads(Path("append-intent.json").read_text())
routing = RoutingConfig.etcd(
    [os.environ["ETCD_ENDPOINT"]],
    key_prefix=os.environ.get("ETCD_KEY_PREFIX", "/nokv/control"),
)
client = Client(job["root_id"], routing)
status = client.operation_status(job["operation_id"])
print(status["state"], status["next_action"], status["receipt"])
```

See the [complete input record and CLI/Python submission example](../../docs/append.md)
for a new append or redelivery. Save the root, logical ID, exact bytes, target,
content-type policy, block size, and resulting-size limit before dispatch.
The first admission needs an existing workspace. If incarnation is omitted,
the SDK resolves an admitted operation's original incarnation before consulting
the live workspace name. Explicit fences are never replaced silently.

The delta limit and default total-body limit are each 16 MiB; increasing
`max_logical_size` does not increase the delta limit. `content_type=None`
inherits an existing type and defaults to `application/octet-stream` on
creation. An explicit type applies to both and is a different intent policy
from omission, even when the strings match the current file. For CLI/Python
redelivery, use the same explicit type from the first call. Keep block size and
all other options identical. Existing producer, manifest identity, and index
fields are inherited.

Follow `committed/none`, `pending/poll`, `ready_to_retry/resubmit_same`, or
`quarantined/retry_cleanup`. Queries do not advance an attempt. Inspection's
cursor continues one exact observation; its logical
`operation_token["state_digest"]` identifies a cleanup recovery request.
Persist that digest before `operation_recover` and reuse it after an uncertain
reply. A later quarantine needs an intentional new recovery round. Once cleanup
finishes, redeliver the original append input; cleanup cannot reconstruct it.

Runtime append errors expose `operation_id`, optional raw `state`, `code`,
`cause_code`, `next_action`, and `retryable=False`. `AppendError` and
`WorkspaceIncarnationMismatch` are separate `RuntimeError` subtypes; catch both
when handling fenced append errors. Local argument type/hex errors may instead
be `TypeError` or `ValueError`. `RequestReplayMismatch` as a cause means the
original intent or lifecycle differs, not a transient retry. A timeout or
`NotFound` observation does not authorize a replacement ID.
`AppendCleanupUnresolved` additionally retains `expected_state_digest`, any
known `recovery_receipt` and publication ID, and
`next_action="retry_same_cleanup"`. Reuse that digest instead of selecting a
new token automatically.

Persist the stable receipt before acknowledging an external queue. Its ten
fields identify the original logical action, successful publication/revision,
target/incarnation, versions, complete body size, and digest. `replayed` and
nullable `commit_version` are call metadata, not stable receipt fields. A
retained receipt does not permanently retain historical body bytes.

The [product specification](../../docs/development/append-product-spec.md)
defines owner leases, provider sealing, retention, and qualification.
`WorkbenchFileSystem` has no append mode, and the legacy 18-tool
`workbench_append` operation is not the durable identity API. Use native
`nokv workspace-path append` first, or `Client.append_bytes` for embedded work.
