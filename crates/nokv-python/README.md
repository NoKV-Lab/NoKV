# NoKV Python SDK

This package exposes API version `1` for path-native Workbenches and immutable
artifacts. The version is available as `nokv.API_VERSION`; the stable package
exports are listed by `nokv.__all__`.

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
