# NoKV Python SDK

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
  query, materialize, and collect operations.
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
