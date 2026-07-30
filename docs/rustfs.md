---
title: RustFS Backend
---

<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# RustFS Backend

NoKV can use RustFS through its S3-compatible object interface. In this role,
RustFS stores immutable file-body blocks and, when experimental metadata
recovery is enabled, checkpoint images and logical-log segments.

RustFS is a data and recovery-object provider, not NoKV's metadata engine. A
multi-node RustFS deployment can improve the durability and availability of
those objects, but it does not replicate the local Holt metadata store or turn
NoKV's experimental metadata owner-handoff path into a production-ready
service.

## Local Development Setup

Install the RustFS binary with Homebrew:

```bash
brew tap rustfs/homebrew-tap
brew install rustfs
rustfs --version
```

Start a disposable single-node process:

```bash
mkdir -p /tmp/rustfs-data
RUSTFS_ACCESS_KEY=rustfsadmin \
RUSTFS_SECRET_KEY=rustfsadmin \
rustfs server \
  --address 127.0.0.1:9000 \
  --console-enable \
  --console-address 127.0.0.1:9001 \
  /tmp/rustfs-data
```

The credentials above are intentionally simple development credentials. Never
reuse them on a network-reachable or persistent deployment. Configure access
keys, transport security, bucket policy, encryption, replication, backup, and
failure domains according to the object-provider deployment.

Create the default bucket with an S3-compatible client:

```bash
AWS_ACCESS_KEY_ID=rustfsadmin \
AWS_SECRET_ACCESS_KEY=rustfsadmin \
aws --endpoint-url http://127.0.0.1:9000 \
  s3api create-bucket --bucket nokv
```

The repository can provision a temporary local RustFS directory and run the
NoKV end-to-end harness:

```bash
scripts/run-rustfs-e2e.sh
```

Use `NOKV_E2E_PROFILE`, `NOKV_E2E_WORKLOAD`, and
`NOKV_E2E_OBJECT_CONCURRENCY` to change that run. Set
`NOKV_E2E_CARGO_TARGET_DIR` when the build needs an isolated target directory.
These scripts are local validation helpers, not a multi-machine availability or
throughput qualification.

## Configure NoKV

The repository's local defaults target bucket `nokv`, endpoint
`http://127.0.0.1:9000`, and the development credentials shown above:

```bash
cargo run --release -p nokv --bin nokv -- init
cargo run --release -p nokv --bin nokv -- mkdir /workspaces
cargo run --release -p nokv --bin nokv -- mkdir /workspaces/1
```

Pass explicit object settings for artifact publication, reads, and FUSE:

```bash
cargo run --release -p nokv --bin nokv -- \
  --object-backend rustfs \
  --s3-bucket nokv \
  --s3-endpoint http://127.0.0.1:9000 \
  --s3-access-key-id rustfsadmin \
  --s3-secret-access-key rustfsadmin \
  put-artifact /workspaces/1/artifact.bin ./artifact.bin
```

NoKV derives immutable block identities and atomically publishes their metadata
in the owning shard. RustFS determines the physical durability and availability
of the bytes. See [Object Layout](./object-layout.md) for that boundary.

## Benchmark Object-Backed Workloads

```bash
cargo run --release -p nokv-bench --bin nokv-bench -- \
  --profile smoke \
  --workload checkpoint-publish \
  --object-backend rustfs \
  --object-concurrency 4 \
  --checkpoint-bytes 1048576 \
  --s3-bucket nokv \
  --s3-endpoint http://127.0.0.1:9000 \
  --s3-access-key-id rustfsadmin \
  --s3-secret-access-key rustfsadmin
```

Workload scope matters:

- `mdtest-easy` and `mdtest-hard` are metadata-only and do not measure RustFS.
- `checkpoint-publish` exercises object upload plus metadata publication.
- `training-read` is an optional packed-artifact/range-read workload; despite
  its historical name, it can represent any reader of packed immutable data.
- `metadata-durability-batch` uses metadata-only file bodies, but its controlled
  sync-log phase writes grouped recovery-log segments to the object provider.
- `--block-cache off` is a useful control when measuring provider latency rather
  than local cache reuse.

For the optional packed-range matrix:

```bash
scripts/run-ai-shard-range-matrix.sh
```

The script covers exact sparse reads, gap-coalesced reads, and MB-scale
read-ahead admission against disposable RustFS instances. Use
`NOKV_AI_SHARD_MATRIX_OUTPUT_DIR` or `NOKV_AI_SHARD_MATRIX_CSV` to choose its
output location. Its results do not establish fleet-level small-file throughput.

## Deployment Checks

Before treating RustFS as a shared provider for a NoKV fleet, validate:

- every metadata owner can reach the same bucket and object-key namespace;
- credentials and bucket policy allow only the required operations;
- body blocks, metadata checkpoints, and logical logs have the intended
  replication and backup policy;
- provider lifecycle rules do not delete objects protected by NoKV snapshots or
  awaiting metadata recovery;
- PUT/GET/range throughput and tail latency meet the workload target;
- the provider's failure behavior matches the recovery assumptions documented
  in [Metadata Sharding And Recovery](./metadata-sharding-and-recovery.md).

References:

- [RustFS Linux installation](https://docs.rustfs.com/installation/linux/index.html)
- [RustFS Docker installation](https://docs.rustfs.com/installation/docker/index.html)
