<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

<div align="center">
  <img src="./docs/public/img/logo.svg" width="200" alt="NoKV" />

  <p>
    <strong>A Rust filesystem for AI training and agent workspaces.</strong>
  </p>

  <p>
    <a href="https://github.com/feichai0017/NoKV/actions"><img alt="CI" src="https://img.shields.io/github/actions/workflow/status/feichai0017/NoKV/rust.yml?branch=main&label=ci" /></a>
    <a href="https://crates.io/crates/nokvfs-cli"><img alt="crates.io" src="https://img.shields.io/crates/v/nokvfs-cli?logo=rust" /></a>
    <a href="https://docs.rs/nokvfs-cli"><img alt="docs.rs" src="https://img.shields.io/docsrs/nokvfs-cli?logo=docs.rs" /></a>
    <img alt="Rust Version" src="https://img.shields.io/badge/rust-1.88%2B-f74c00?logo=rust&logoColor=white" />
  </p>

  <p>
    <img alt="License" src="https://img.shields.io/badge/license-Apache--2.0-yellow" />
    <a href="https://landscape.cncf.io/?group=projects-and-products&item=runtime--cloud-native-storage--nokv"><img alt="CNCF Landscape" src="https://img.shields.io/badge/CNCF%20Landscape-listed-5699C6?logo=cncf&logoColor=white" /></a>
    <a href="https://dbdb.io/db/nokv"><img alt="DBDB.io" src="https://img.shields.io/badge/DBDB.io-profiled-244A64" /></a>
    <a href="https://deepwiki.com/feichai0017/NoKV"><img alt="DeepWiki" src="https://img.shields.io/badge/DeepWiki-Ask-6f42c1" /></a>
  </p>

  <h3>Recognized In The AI-Native Storage Ecosystem</h3>

  <table>
    <tr>
      <td align="center" width="360">
        <a href="https://landscape.cncf.io/?group=projects-and-products&item=runtime--cloud-native-storage--nokv">
          <img src="./docs/public/img/recognition/cncf.svg" width="128" alt="Cloud Native Computing Foundation" />
        </a>
        <br />
        <strong>Linux Foundation CNCF Landscape</strong>
        <br />
        <sub>Listed in AI Native Infra / Storage and Cloud Native Storage.</sub>
      </td>
      <td align="center" width="360">
        <a href="https://dbdb.io/db/nokv">
          <img src="./docs/public/img/recognition/dbdb.svg" width="128" alt="DBDB.io Database of Databases" />
        </a>
        <br />
        <strong>DBDB.io Database of Databases</strong>
        <br />
        <sub>Historical database profile; current NoKV is the Rust filesystem product line.</sub>
      </td>
    </tr>
  </table>
</div>

<br/>

## What Is NoKV?

NoKV is a Rust filesystem for AI training and agent workspaces. It keeps
filesystem metadata in [Holt](https://crates.io/crates/holt) and stores file
bodies in S3-compatible object storage such as RustFS, MinIO, Ceph RGW, or AWS
S3.

The core split is deliberate:

```text
FUSE / SDK / CLI
  -> NoKV metadata service
  -> Holt-backed inode/dentry metadata
  -> S3-compatible object storage for file bodies
```

NoKV owns namespace truth, metadata transactions, body descriptors, snapshots,
watches, and object-reference garbage collection. Object stores own byte
durability and replication.

## Why This Exists

AI training and agent systems often need a normal file interface while their
real data lives in object storage. The hard part is not only moving bytes. The
hard part is metadata:

- datasets need fast directory scans and stable snapshot views;
- checkpoints need atomic publish and overwrite semantics;
- artifact stores need body references, digest metadata, and cleanup of failed
  staged uploads;
- agent workspaces need scoped namespace views and typed change events;
- training nodes need FUSE compatibility, but SDK paths should avoid kernel
  overhead when possible.

NoKV turns those needs into a filesystem-shaped metadata service instead of
forcing each application to stitch together SQL rows, object-store prefixes,
ad hoc locks, and polling loops.

## Current Status

This repository is now the Rust NoKV-FS product line.

Implemented today:

- low-level FUSE frontend for lookup, getattr, readdir, readdirplus, create,
  mkdir, symlink/readlink, rename, unlink, rmdir, read, write, flush, release,
  fsync, and basic setattr/truncate;
- Holt-backed local metadata service with inode/dentry canonical metadata,
  dentry projection, command predicates, command dedupe, and history records;
- chunked object data path where file bodies are split into immutable object
  blocks and published by metadata manifest;
- S3-compatible object backend, with RustFS as the local development default;
- Rust SDK and `nokv-fs` CLI for namespace operations, artifact publish,
  metadata server access, and object range reads;
- long-running `nokvfs-server` with health, readiness, stats, manual GC, and
  framed binary metadata RPC;
- read-only snapshot mounts, snapshot-version reads, typed watch replay, and
  FUSE cache invalidation from watch events;
- pending-object GC and metadata history GC tied to snapshot retention.

Not implemented yet:

- distributed metadata replication and high availability;
- FUSE over the metadata server;
- Python/fsspec and Kubernetes CSI packages;
- full POSIX coverage such as hardlinks, xattrs, locks, special files,
  `statfs`, ACLs, and mature multi-client cache coherence.

So NoKV is currently a usable single-node object-backed filesystem prototype,
not yet a JuiceFS/3FS-class distributed filesystem.

## Repository Layout

```text
crates/
  nokvfs-types     storage-neutral namespace model types
  nokvfs-protocol  framed metadata RPC DTOs and binary codec
  nokvfs-meta      schema, MetadataCommand, Holt store, service core
  nokvfs-object    S3-compatible object body storage helpers
  nokvfs-client    Rust SDK over metadata service and object backend
  nokvfs-fuse      low-level FUSE frontend
  nokvfs-server    long-running metad process and framed RPC service
  nokvfs-cli       nokv-fs CLI binary

bench/             system workload benchmark harness
docs/              product, architecture, layout, RustFS, and benchmark docs
examples/          PyTorch and Kubernetes examples
```

## Quick Start

Build and test:

```bash
cargo test --workspace
```

Build the CLI:

```bash
cargo build --release -p nokvfs-cli --bin nokv-fs
```

Initialize local metadata:

```bash
cargo run --release -p nokvfs-cli --bin nokv-fs -- init
```

Start a metadata server:

```bash
cargo run --release -p nokvfs-cli --bin nokv-fs -- serve
```

By default NoKV expects a local RustFS-compatible S3 endpoint at
`http://127.0.0.1:9000`, bucket `nokv`, with development credentials
`rustfsadmin` / `rustfsadmin`. See [docs/rustfs.md](docs/rustfs.md) for the
RustFS setup commands.

Publish and read an artifact:

```bash
cargo run --release -p nokvfs-cli --bin nokv-fs -- \
  put-artifact /runs/1/checkpoint.bin ./checkpoint.bin

cargo run --release -p nokvfs-cli --bin nokv-fs -- \
  cat /runs/1/checkpoint.bin > restored.bin
```

Mount with FUSE:

```bash
mkdir -p /tmp/nokv-mount

cargo run --release -p nokvfs-cli --bin nokv-fs -- \
  mount /tmp/nokv-mount
```

On macOS this requires macFUSE. NoKV passes `noappledouble` and `noapplexattr`
mount options because xattr persistence is not implemented yet.

## Benchmarks

The root `bench/` crate contains system workload harnesses:

```bash
cargo run --release -p nokvfs-bench --bin nokv-fs-bench -- \
  --profile smoke \
  --workload all
```

Covered workload shapes include:

- `mdtest-easy` and `mdtest-hard` metadata smoke workloads;
- `checkpoint-publish` object-backed checkpoint publish/read;
- `training-read` dataset-shaped object reads;
- `mlperf-dlio` generated MLPerf Storage/DLIO-style training and checkpoint
  shape.

These are local single-node service benchmarks. Distributed and training-cluster
claims need separate runs that report replication, cache, object-store, and
durability settings.

## Design Notes

NoKV follows the same high-level separation used by systems like JuiceFS and
3FS: metadata is separate from file body storage. The current implementation is
still narrower:

- like JuiceFS, NoKV uses inode/dentry semantics and stores file bodies outside
  metadata;
- like 3FS, NoKV is aimed at AI training workloads and a metadata/data split;
- unlike those mature systems, NoKV does not yet provide distributed metadata
  HA, production cache coherence, or full POSIX coverage.

The next major engineering steps are POSIX completion, client/cache work, and a
distributed metadata layer with Holt as the shard-local state machine.

## Documentation

- [Architecture](docs/architecture.md)
- [Product Design](docs/product-design.md)
- [Metadata Schema](docs/metadata-schema.md)
- [Object Layout](docs/object-layout.md)
- [AI Training](docs/ai-training.md)
- [Checkpointing](docs/checkpointing.md)
- [RustFS Backend](docs/rustfs.md)
- [Benchmarks](docs/benchmarks.md)

## License

Apache-2.0. See [LICENSE](LICENSE).
