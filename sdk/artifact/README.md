# NoKV Artifact SDK Notes

This directory tracks artifact SDK work that sits above `fsmeta` and below
framework adapters such as MLflow. The current artifact work is intentionally
narrow: provide a namespace-oriented artifact API first, then adapt MLflow's
`ArtifactRepository` interface on top of it.

## Current Task

Build an Artifact Namespace SDK for MLflow artifacts.

The SDK owns artifact namespace behavior:

- `Put` a file artifact at a logical artifact path.
- `Get` or download a file artifact by path.
- `List` direct children under a logical artifact directory.
- `Stat` artifact metadata.
- `Delete` a file artifact.

The SDK does not embed artifact bytes in `fsmeta`. `fsmeta` stores namespace
metadata and a compact opaque body reference. The artifact body itself belongs
to a separate `BodyStore`, which may later be backed by local files, S3, R2, or
another object store.

The distributed fsmeta layer is not part of the current artifact path. The
active path is direct fsmeta execution against the local single-engine runtime.
The gRPC surface has the same primitive so existing tools keep compiling, but
distributed behavior is not the development target for this phase.

## fsmeta Primitives Available

The artifact SDK currently depends on this fsmeta namespace surface:

- `Create`: creates a file or directory dentry plus inode.
- `LookupPlus`: resolves a dentry and inode for exact metadata.
- `ReadDirPlus`: lists directory children with inode metadata.
- `Rename`: moves a staged dentry to a final path only when the destination
  does not exist.
- `RenameReplace`: atomically publishes a staged file dentry at the final path,
  replacing an existing non-directory target when present.
- `Unlink`: removes one non-directory dentry and deletes or decrements its inode
  record according to link count.

`RenameReplace` is the native primitive added for artifact overwrite semantics.
It commits the namespace replacement in one KV transaction:

- delete the staged source dentry;
- put the destination dentry;
- delete the old destination inode when its link count reaches zero, otherwise
  decrement its link count;
- apply quota deltas in the same mutation group when quota accounting is
  enabled.

`RenameReplace` is deliberately a visible-only fsmeta operation with no subtree
handoff and no durability barrier. It compiles as a slow path operation because
the write set depends on whether the destination exists and on the old inode
link count.

## Existing SDK Implementation

This directory contains a Go artifact SDK package with these internal
interfaces:

```go
type NamespaceClient interface {
    Create(context.Context, fsmeta.CreateRequest) (fsmeta.CreateResult, error)
    LookupPlus(context.Context, fsmeta.LookupRequest) (fsmeta.DentryAttrPair, error)
    ReadDirPlus(context.Context, fsmeta.ReadDirRequest) ([]fsmeta.DentryAttrPair, error)
    Rename(context.Context, fsmeta.RenameRequest) error
    RenameReplace(context.Context, fsmeta.RenameReplaceRequest) (fsmeta.RenameReplaceResult, error)
    Unlink(context.Context, fsmeta.UnlinkRequest) error
}

type BodyStore interface {
    Put(context.Context, io.Reader) (BodyRef, error)
    Get(context.Context, BodyRef, io.Writer) error
    Delete(context.Context, BodyRef) error
}
```

`Store.Put` writes the body first, creates a hidden staged fsmeta file entry,
then publishes it with `RenameReplace`. Readers observe either the old body
reference or the new body reference, never a half-published artifact path.

`FileBodyStore` is the current local body-store implementation. It is suitable
for local integration tests and for proving the SDK contract, not as the final
cloud object-store strategy.

`python` contains the first Python MLflow adapter increment:

- `NoKVArtifactRepository` subclasses MLflow's `ArtifactRepository`.
- `ArtifactStore` is the narrow Python protocol the future NoKV/fsmeta client
  must implement.
- `LocalArtifactStore` is a local development and adapter-test store. It is not
  the production NoKV namespace client.
- The adapter implements file upload, recursive upload, direct-child listing,
  inherited MLflow recursive download, exact file download, and file deletion.
- Directory delete returns a clear MLflow error until the fsmeta `rmdir`
  primitive is available.

The package registers these MLflow artifact URI schemes:

- `nokv`: production NoKV artifact namespace. This currently requires an
  injected Python `ArtifactStore` implementation or a configured
  `NOKV_ARTIFACT_STORE_FACTORY=module:function` factory. MLflow registry
  construction can use that factory because it cannot pass an injected store.
- `nokv+file`: local development and adapter-test store rooted at a filesystem
  path.

## MLflow Target Interface

The upstream behavior target is `mlflow/store/artifact/artifact_repo.py`.
The MLflow adapter should subclass `ArtifactRepository` and implement the
artifact methods expected by MLflow:

- `log_artifact(local_file, artifact_path=None)`: upload one local file.
- `log_artifacts(local_dir, artifact_path=None)`: recursively upload a local
  directory.
- `list_artifacts(path=None) -> list[FileInfo]`: return direct children. MLflow
  expects `FileInfo(path, is_dir, file_size)` records and treats listing a file
  as an empty child list.
- `download_artifacts(artifact_path, dst_path=None) -> str`: download one file
  or a directory tree to a local destination and return the local path.
- `_download_file(remote_file_path, local_path)`: copy one remote artifact file
  to an exact local path.
- `delete_artifacts(artifact_path=None)`: delete artifacts when supported.

URI registration should follow `mlflow/utils/uri.py` and
`mlflow/store/artifact/artifact_repository_registry.py`. The adapter needs a
stable NoKV artifact URI scheme and a repository factory that maps that scheme
to the NoKV-backed `ArtifactRepository` implementation.

## Gaps Before Full MLflow Support

Known gaps at the bottom of the stack:

- Recursive directory delete is not available in this SDK path yet. `rmdir` is
  being developed separately. Until then, `delete_artifacts` can safely support
  file deletion only, or return a clear unsupported error for directories.
- Body garbage collection is not complete. `RenameReplace` returns
  `OldInodeDeleted`, `OldDentry`, and `OldInode`, which is enough to identify
  when an overwritten artifact body may be eligible for cleanup. The SDK should
  not eagerly delete old body data until the body store has a safe ownership or
  reference-counting rule.
- The production Python `ArtifactStore` binding is still missing. The adapter
  is now in place, but the default `nokv` scheme must be wired to a real Python
  NoKV/fsmeta client before it can serve production traffic without injection.
- Object-store-backed `BodyStore` implementations are still pending. S3/R2/GCS
  style stores need content-addressing or atomic object publish rules, retry
  behavior, credentials configuration, and cleanup policy.
- Directory listing pagination is handled through `ReadDirPlusAll` in the Go
  SDK, but the Python adapter still needs matching pagination behavior.
- Cross-process and crash recovery behavior for staged entries needs a policy:
  stale staged entries can be ignored by normal listing, retried, or collected
  by a maintenance job.
- MLflow async logging paths should be checked after the synchronous adapter is
  working. The namespace semantics should stay the same, but retry and error
  surfacing need explicit tests.

## Test Expectations

Artifact SDK changes should be test-driven against the local single-engine
fsmeta runtime. Required coverage includes:

- overwrite through `RenameReplace`;
- target missing path behaves like rename;
- directory source and directory target rejection;
- hard-link destination replacement decrements the old inode link count instead
  of deleting the shared inode;
- MLflow-compatible list behavior for files and directories;
- local download writes through a temporary file before final rename.

The fsmeta primitive tests live under `fsmeta/runtime/local`. SDK behavior tests
live under `sdk/artifact`. The Python MLflow adapter tests live under
`sdk/artifact/python/tests` and can be run against the local MLflow checkout
with a Python environment that satisfies MLflow's dependencies:

```sh
PYTHONPATH=/Users/wangchanghao/mlflow-demo:/Users/wangchanghao/NoKV/sdk/artifact/python \
  python -m unittest discover -s sdk/artifact/python/tests -v
```
