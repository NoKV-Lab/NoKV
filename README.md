<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

<div align="center">
  <img src="./docs/public/img/logo.png" width="320" alt="NoKV" />

  <h3>Durable agent workspaces.</h3>

  <p>
    NoKV is a metadata control plane for agent workspaces. It publishes
    crash-consistent, versioned workspace state over object storage, with
    path-sharded metadata for small-file scale-out.
  </p>

  <p>
    <a href="https://nokv.io/"><strong>Website</strong></a> ·
    <a href="./docs/index.md"><strong>Documentation</strong></a> ·
    <a href="#quick-start"><strong>Quick Start</strong></a> ·
    <a href="https://github.com/orgs/NoKV-Lab/discussions"><strong>Discussions</strong></a> ·
    <a href="#contributing"><strong>Contributing</strong></a>
  </p>
</div>

## Our building partners include:

| Partner | Project |
| --- | --- |
| **OpenViking** | [Website](https://openviking.ai/) · [GitHub](https://github.com/volcengine/OpenViking) · ![OpenViking stars](https://img.shields.io/github/stars/volcengine/OpenViking?style=flat&label=stars) |
| **Hermes Agent** | [GitHub](https://github.com/NousResearch/hermes-agent) · ![Hermes Agent stars](https://img.shields.io/github/stars/NousResearch/hermes-agent?style=flat&label=stars) |
| **Lingtai AI** | [Website](https://lingtai.ai/en/) · [GitHub](https://github.com/Lingtai-AI/lingtai) · ![Lingtai stars](https://img.shields.io/github/stars/Lingtai-AI/lingtai?style=flat&label=stars) |
| **LoopX** | [GitHub](https://github.com/huangruiteng/loopx) · ![LoopX stars](https://img.shields.io/github/stars/huangruiteng/loopx?style=flat&label=stars) |
| **heima** | [GitHub](https://github.com/litentry/heima) · ![heima stars](https://img.shields.io/github/stars/litentry/heima?style=flat&label=stars) |

Building-partner status denotes an active collaboration. It does not by itself
imply a production deployment, support SLA, or completed enterprise qualification.

## Recognition

<table>
  <tr>
    <td align="center" width="120">
      <a href="https://landscape.cncf.io/?group=projects-and-products&item=runtime--cloud-native-storage--nokv">
        <img src="./docs/public/img/recognition/cncf.svg" width="56" alt="CNCF Landscape" />
      </a>
    </td>
    <td>
      <strong>CNCF Landscape</strong><br/>
      Listed under AI Native Infra / Storage and Cloud Native Storage.
    </td>
  </tr>
  <tr>
    <td align="center">
      <a href="https://dbdb.io/db/nokv">
        <img src="./docs/public/img/recognition/dbdb.svg" width="56" alt="DBDB.io Database of Databases" />
      </a>
    </td>
    <td>
      <strong>DBDB.io</strong><br/>
      Catalogued by the CMU Database Group. The profile is historical; this
      repository is the current Rust product line.
    </td>
  </tr>
</table>

## What NoKV Owns

Agent runs produce configs, logs, checkpoints, reports, and evidence across
many files and object keys. NoKV gives that state one filesystem-shaped address
and owns the metadata required to publish, inspect, snapshot, fork, and recover
it.

```text
Agents / RAGFS / SDK / CLI / FUSE / Python
                    |
                    v
          NoKV metadata service
  namespace, versions, snapshots, watches,
      CoW bindings, provenance metadata, GC
                    |
          +---------+---------+
          |                   |
          v                   v
  embedded Holt         S3-compatible store
  metadata truth        immutable file bodies
```

NoKV owns namespace truth, shard-local metadata transactions, versioned body
descriptors, snapshots, typed watches, CoW workspace bindings, and
object-reference GC policy. The object provider owns the physical durability,
replication, availability, and access policy of file bodies.

NoKV is not a semantic-memory database or an agent orchestrator. Context
retrieval, ranking, planning, validation, and runtime policy stay above the
storage layer.

## Workspace Guarantees

- **Crash-consistent publication.** One owning shard publishes a new artifact
  generation as one durable metadata command.
- **Stable historical reads.** Reuse a pinned snapshot when a workflow needs a
  consistent view across multiple reads.
- **CoW fork-to-restore.** Restore into a new same-shard workspace, validate it,
  then switch consumers without mutating the source workspace.
- **Typed change streams.** Namespace changes are recorded as replayable events
  with cursors and retention rules.
- **Immutable body generations.** Published metadata references immutable
  object blocks; local placement can change without changing namespace truth.

These guarantees are shard-local. NoKV does not currently provide cross-shard
transactions. Snapshot protection is leased, and path/CoW scoping is not an
authentication or RBAC boundary.

## Distributed Status

| Status | Capabilities |
| --- | --- |
| **Current default** | One `nokv-server`, embedded Holt, S3-compatible bodies, Rust SDK, CLI, FUSE, Python/fsspec, snapshots, watches, same-shard CoW, object-reference GC |
| **Experimental on `main`** | Longest-prefix path sharding, fleet-aware Rust/CLI/FUSE routing, one active Holt-backed owner per shard, etcd lease/epoch fencing, checkpoint and logical shared-log recovery |
| **Next / hardening** | Multi-machine fault qualification, enterprise small-file throughput validation, online resharding, Python fleet routing, production metadata HA, tenant identity/policy, live-workspace freeze, broader POSIX coverage |

Holt stays embedded and shard-local. NoKV owns horizontal routing above it. The
experimental fleet is not consensus replication and is not yet a
JuiceFS/3FS-class production-HA filesystem.

Read the [architecture](docs/architecture.md),
[product boundary](docs/product-design.md), and
[experimental fleet runbook](docs/multishard-fleet-runbook.md) for the exact
contracts and known limits.

## Interfaces

- Rust path/file SDK through `nokv-client`.
- Low-level FUSE frontend through `nokv-fuse`.
- Python/fsspec binding over one metadata endpoint.
- `nokv` CLI for local operation and fleet-aware Rust paths.
- Native MCP over stdio:
  - generic `agent` profile: seven read-only namespace tools;
  - Workbench profile: 17 base tools and a conditional restore tool when the
    configured owners advertise that capability.

The Agent tool contracts are transport-free and documented in
[docs/development/nokv-agent.md](docs/development/nokv-agent.md). LingTai users
should follow the
[Workbench setup and preflight guide](docs/lingtai-workbench-preflight.md).

## Quick Start

Prerequisites for the commands below: Rust 1.88+, the RustFS binary, AWS CLI,
and `curl`. Mounting on macOS additionally requires macFUSE.

Build NoKV:

```bash
cargo build --release -p nokv --bin nokv
```

Start a local RustFS endpoint and create the default bucket:

```bash
mkdir -p /tmp/rustfs-data
RUSTFS_ACCESS_KEY=rustfsadmin \
RUSTFS_SECRET_KEY=rustfsadmin \
rustfs server --address 127.0.0.1:9000 /tmp/rustfs-data &

until AWS_ACCESS_KEY_ID=rustfsadmin \
  AWS_SECRET_ACCESS_KEY=rustfsadmin \
  aws --endpoint-url http://127.0.0.1:9000 \
    s3api list-buckets >/dev/null 2>&1; do sleep 1; done

AWS_ACCESS_KEY_ID=rustfsadmin \
AWS_SECRET_ACCESS_KEY=rustfsadmin \
aws --endpoint-url http://127.0.0.1:9000 \
  s3api create-bucket --bucket nokv
```

Start the metadata service and initialize the namespace:

```bash
cargo run --release -p nokv --bin nokv -- serve &
until curl -fsS http://127.0.0.1:7777/readyz >/dev/null 2>&1; do sleep 1; done
cargo run --release -p nokv --bin nokv -- init
cargo run --release -p nokv --bin nokv -- mkdir /runs
cargo run --release -p nokv --bin nokv -- mkdir /runs/1
```

Publish and read an artifact:

```bash
cargo run --release -p nokv --bin nokv -- \
  put-artifact /runs/1/checkpoint.bin ./checkpoint.bin

cargo run --release -p nokv --bin nokv -- \
  cat /runs/1/checkpoint.bin > restored.bin
```

The default RustFS credentials above are for localhost development only. See
[docs/rustfs.md](docs/rustfs.md) for configuration details.

## Documentation

- [Documentation Index](docs/index.md)
- [Architecture](docs/architecture.md)
- [Product Design](docs/product-design.md)
- [Metadata Sharding and Recovery](docs/metadata-sharding-and-recovery.md)
- [Metadata Schema](docs/metadata-schema.md)
- [Object Layout](docs/object-layout.md)
- [Checkpointing](docs/checkpointing.md)
- [CoW Workspaces](docs/cow-workspaces.md)
- [LingTai Workbench Setup](docs/lingtai-workbench-preflight.md)
- [Benchmarks and Evidence](docs/benchmarks.md)

## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md) and the
[code contract](docs/development/code_contract.md) before changing package
boundaries or storage semantics. Open work suitable for newcomers is listed
under the dynamic
[good first issue query](https://github.com/NoKV-Lab/NoKV/issues?q=is%3Aissue%20state%3Aopen%20label%3A%22good%20first%20issue%22).

All commits must include a DCO `Signed-off-by` trailer.

## Crates

| Crate | Role |
| --- | --- |
| [`nokv-types`](https://crates.io/crates/nokv-types) | Storage-neutral namespace and shard types |
| [`nokv-protocol`](https://crates.io/crates/nokv-protocol) | Framed metadata RPC DTOs and codec |
| [`nokv-meta`](https://crates.io/crates/nokv-meta) | Schema, commands, Holt store, snapshots, CoW, and GC |
| [`nokv-control`](https://crates.io/crates/nokv-control) | Shard map, leases, epochs, and recovery pointers |
| [`nokv-object`](https://crates.io/crates/nokv-object) | S3-compatible immutable body storage and local hot tier |
| [`nokv-agent`](https://crates.io/crates/nokv-agent) | Transport-free read-only agent tool contracts |
| [`nokv-client`](https://crates.io/crates/nokv-client) | Rust path/file SDK and fleet routing |
| [`nokv-fuse`](https://crates.io/crates/nokv-fuse) | Low-level FUSE frontend |
| [`nokv-python`](https://crates.io/crates/nokv-python) | Python SDK and fsspec binding |
| [`nokv-server`](https://crates.io/crates/nokv-server) | Long-running metadata service and shard owners |
| [`nokv`](https://crates.io/crates/nokv) | CLI and MCP/workbench transport wiring |

## License

Apache-2.0. See [LICENSE](LICENSE).
