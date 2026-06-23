<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

<div align="center">
  <img src="./docs/public/img/logo.png" width="360" alt="NoKV" />

  <p>
    <strong>Metadata control plane for object-backed agent artifacts.</strong>
  </p>

  <p>
    <a href="https://github.com/feichai0017/NoKV/actions"><img alt="CI" src="https://img.shields.io/github/actions/workflow/status/feichai0017/NoKV/rust.yml?branch=main&label=ci" /></a>
    <img alt="Rust Version" src="https://img.shields.io/badge/rust-1.88%2B-f74c00?logo=rust&logoColor=white" />
  </p>

  <p>
    <img alt="License" src="https://img.shields.io/badge/license-Apache--2.0-yellow" />
    <a href="https://landscape.cncf.io/?group=projects-and-products&item=runtime--cloud-native-storage--nokv"><img alt="CNCF Landscape" src="https://img.shields.io/badge/CNCF%20Landscape-listed-5699C6?logo=cncf&logoColor=white" /></a>
    <a href="https://dbdb.io/db/nokv"><img alt="DBDB.io" src="https://img.shields.io/badge/DBDB.io-profiled-244A64" /></a>
    <a href="https://deepwiki.com/feichai0017/NoKV"><img alt="DeepWiki" src="https://img.shields.io/badge/DeepWiki-Ask-6f42c1" /></a>
  </p>

  <p>
    <a href="https://nokv.io/architecture">Docs</a> ·
    <a href="https://nokv.io/blog/agents-want-filesystems">Why Filesystems</a> ·
    <a href="#-quick-start">Quick Start</a> ·
    <a href="#-measured-evidence">Benchmarks</a> ·
    <a href="https://github.com/feichai0017/NoKV/discussions">Discussions</a> ·
    <a href="#-contributing">Contributing</a>
  </p>

  <h3>Listed In The AI-Native Storage Ecosystem</h3>

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

NoKV is a metadata control plane for object-backed agent artifacts: run
outputs, log files, checkpoints, reports, and citable evidence in one
filesystem-shaped namespace. For the longer interface argument, see
[Agents Want Filesystems](https://nokv.io/blog/agents-want-filesystems).

It is not a trace database. Keep JSONL, SQLite, or Postgres as the source of
truth for runtime events; use NoKV as the agent-facing namespace over the
artifacts and evidence those systems produce.

NoKV keeps namespace metadata in its own path-native engine
([Holt](https://crates.io/crates/holt)) and stores file bodies as immutable
blocks in S3-compatible object storage such as RustFS, MinIO, Ceph RGW, or AWS
S3.

```text
FUSE / SDK / CLI
  -> NoKV metadata service     (self-contained; no separate metadata DB to run)
  -> Holt inode/dentry metadata
  -> S3-compatible object store for file bodies
```

NoKV owns namespace truth, metadata transactions, snapshots, watches, and
object-reference GC. The object store owns byte durability and replication. The
metadata engine is built in, so local deployments operate a filesystem rather
than a filesystem plus a separate Redis, MySQL, or TiKV cluster.

## Why NoKV

Agent workflows are artifact-heavy; their workspaces aren't. Every run leaves
behind configs, metrics, logs, checkpoints — and that state scatters across
folders, JSON files, object-store keys, and database rows. Agents pay a
navigation tax in tokens every time they go looking. NoKV gives that state one
address, with the metadata guarantees the workload actually needs:

- **Checkpoints publish atomically.** Readers see the complete new checkpoint
  or the previous one — never a half-written file, even across a crash.
- **Snapshots are time travel.** Pin a frozen view of any subtree and keep
  reading it while jobs write; GC never deletes what a snapshot still needs.
- **Changes are events, not polls.** Every create, rename, and publish lands as
  a typed, replayable event with a cursor.
- **Artifacts carry body references and digests**, with cleanup of failed
  staged uploads.
- **Bodies are immutable, versioned blocks.** Replacement publishes a new
  generation, so node-local caches never invalidate object bytes after publish.

The primary write model is write-once publish, matching how datasets,
checkpoints, and artifacts are commonly written.

## 🤝 Contributing

Contributions are welcome, from first-timers to seasoned Rustaceans. Read [CONTRIBUTING.md](CONTRIBUTING.md) to get started: it covers setup, conventions, and the review gate. Pick up a [good first issue](https://github.com/NoKV-Lab/NoKV/issues?q=is%3Aissue%20state%3Aopen%20label%3A%22good%20first%20issue%22), or follow the recommended newcomer track in [#354](https://github.com/NoKV-Lab/NoKV/issues/354) (MCP server for the agent namespace surface).

To understand the project first, read [Why filesystems](https://nokv.io/blog/agents-want-filesystems), [the metadata engine](https://nokv.io/blog/holt-in-nokv), and [the benchmark](https://nokv.io/benchmark).

## 📚 Documentation

- [Architecture](docs/architecture.md)
- [Product Design](docs/product-design.md)
- [AI-Native Metadata HA And Fast Path](docs/metadata-ha-fast-path.md)
- [Metadata Schema](docs/metadata-schema.md)
- [Object Layout](docs/object-layout.md)
- [Checkpointing](docs/checkpointing.md)
- [RustFS Backend](docs/rustfs.md)
- [Benchmarks](docs/benchmarks.md)

## 🤖 The Agent Interface

`ls` · `stat` · `catalog` · `find` · `aggregate` · `read` · `grep`

Seven verbs, one progressive-disclosure surface: an agent discovers what
exists, learns what is queryable, and pays to read only what it needs.
Predicates, sort, and projection are pushed into the engine, so a "top-5 runs
by val_loss" report costs two calls — one `catalog`, one `find`. `grep` sweeps
a subtree and returns line-numbered matches with citable evidence URIs
(`nokv-native://path@generation:N#L3`).

The verbs live in [`nokv-agent`](crates/nokv-agent/src/lib.rs): the tool
definitions are LLM-ready JSON schemas, and `execute_agent_tool` routes calls
over the same `AgentNamespace` trait whether the namespace is embedded
(in-process) or remote (metadata RPC via `nokv-client`). The crate is
transport-free — it depends only on `nokv-meta`, `nokv-object`, and
`nokv-types`. See the [contributor handbook](docs/development/nokv-agent.md).

**Today** the agent verbs ship in the Rust SDK; filesystem operations ship in
the `nokv` CLI and FUSE mount. An **MCP server is in development** — follow
[#354](https://github.com/feichai0017/NoKV/issues/354).

## 📊 Measured Evidence

**Agent interface.** We gave the same agent (`gpt-5.4-mini`) the same 875-run
experiment corpus through two surfaces — raw SQL over SQLite, and the NoKV
namespace — across five tasks, 10 repeats per arm and task (100 fully stateless
runs), judged against deterministic gold facts neither arm can see:

| Set mean (per 5-task pass) | Raw SQLite | NoKV namespace |
| --- | --- | --- |
| Tasks solved correctly | 4.40 / 5 | **4.50 / 5** |
| Prompt tokens (incl. cached) | 151,572 | **82,827 (−45%)** |
| Cost (USD, list rates) | $0.0708 | **$0.0433 (−39%)** |

In this 10-repeat sample, the token gap widens to ~2.4× on the
compound-exploration subset, and SQL won the single-shot analytics task —
per-task results, wins and losses both, are in the report. Harness, tasks,
judge, and the raw telemetry of all 100 runs are committed, so every published
number is recomputable: see
[`bench/agent-interface/`](bench/agent-interface/BENCHMARK_REPORT.md).

**Storage engine.** Local engineering baselines, not official MLPerf results.
Single-node service numbers are release builds through the NoKV server and Holt
metadata path. FUSE comparison numbers depend on kernel/FUSE, object backend,
cache settings, and workload shape.

| Workload | Result |
| --- | --- |
| Metadata create (`mdtest`, 65k records) | **~127K ops/s** (single-writer, batched service path) |
| Same, one directory of 65k entries | Same order of throughput; path-native ART does not degrade on large directories |
| Checkpoint publish (1 MiB blocks, concurrency 16) | **~1.1 GiB/s** in the service/object benchmark |
| Dataset read (16 KiB samples, concurrency 16) | **~3,000 samples/s** in the service/object benchmark |
| Resident metadata | **~1.5 KiB / file** in the measured shape |
| Atomic checkpoint | Object bytes land first; metadata publishes a new generation atomically |

Same-machine FUSE-vs-FUSE smoke against one RustFS endpoint currently shows
NoKV behind JuiceFS on the end-to-end mounted path. That gap is expected to come
from FUSE/RPC fixed costs and data-plane cache/writeback maturity, not from the
Holt metadata engine alone.

## NoKV vs JuiceFS

NoKV follows the same high-level separation used by systems like JuiceFS and
3FS: metadata is separate from file body storage. The difference is that NoKV
ships its metadata engine as part of the filesystem and optimizes for
agent-workspace and artifact publish/read patterns first.

| | JuiceFS | NoKV |
| --- | --- | --- |
| Metadata engine | External DB such as Redis, MySQL, or TiKV | Built-in, path-native Holt engine |
| Atomic checkpoint publish | POSIX rename/write semantics over the metadata engine | First-class publish-by-generation primitive |
| Block model | Slice/block model supporting broad POSIX behavior | Immutable object blocks plus new-generation manifests |
| Workspace-native primitives | Layered on top of the filesystem | Snapshots, typed watch, body descriptors, and GC floors are core metadata concepts |
| Agent query surface | None | `ls`/`stat`/`catalog`/`find`/`aggregate`/`read`/`grep` with push-down and line-numbered evidence |
| POSIX completeness | Mature production filesystem | P0 subset implemented; still hardening compatibility gates |
| Maturity | Production, large deployments | Young Rust implementation; single-node local mode is usable, replication is roadmap |

NoKV is an object-backed filesystem with a sharded Holt metadata plane: multiple
metadata shards (one Holt engine each) behind long-running metadata servers,
routed through an etcd control plane, with cross-shard grafts presenting a single
FUSE namespace across shards. Metadata HA today is single-writer-per-shard with
checkpoint-image + shared-log, epoch-fenced failover — not yet consensus
replication — so it is not yet a JuiceFS/3FS class production-HA distributed
filesystem.



## 🚦 Quick Start

Build and test:

```bash
cargo test --workspace
cargo build --release -p nokv --bin nokv
```

Start a local RustFS-compatible S3 endpoint and create the default bucket:

```bash
mkdir -p /tmp/rustfs-data
RUSTFS_ACCESS_KEY=rustfsadmin \
RUSTFS_SECRET_KEY=rustfsadmin \
rustfs server --address 127.0.0.1:9000 /tmp/rustfs-data &

AWS_ACCESS_KEY_ID=rustfsadmin \
AWS_SECRET_ACCESS_KEY=rustfsadmin \
aws --endpoint-url http://127.0.0.1:9000 \
  s3api create-bucket --bucket nokv
```

By default NoKV expects bucket `nokv` at `http://127.0.0.1:9000` with
development credentials `rustfsadmin` / `rustfsadmin`. See
[docs/rustfs.md](docs/rustfs.md) for other deployment modes.

Start the metadata server, then initialize the namespace. Every other command
talks to the server on `127.0.0.1:7777`, so keep it running:

```bash
cargo run --release -p nokv --bin nokv -- serve &

cargo run --release -p nokv --bin nokv -- init
```

Publish and read an artifact:

```bash
cargo run --release -p nokv --bin nokv -- \
  put-artifact /runs/1/checkpoint.bin ./checkpoint.bin

cargo run --release -p nokv --bin nokv -- \
  cat /runs/1/checkpoint.bin > restored.bin
```

Mount with FUSE:

```bash
mkdir -p /tmp/nokv-mount

cargo run --release -p nokv --bin nokv -- \
  mount /tmp/nokv-mount
```

On macOS this requires macFUSE. NoKV passes the `noappledouble` mount option to
avoid Finder/resource-fork AppleDouble sidecars; user xattr roundtrip is
covered by the FUSE smoke test.

## 🧩 Crates

| Crate | Role |
| --- | --- |
| [`nokv-types`](https://crates.io/crates/nokv-types) | Storage-neutral namespace model |
| [`nokv-protocol`](https://crates.io/crates/nokv-protocol) | Framed metadata RPC DTOs and binary codec |
| [`nokv-object`](https://crates.io/crates/nokv-object) | S3-compatible object body storage |
| [`nokv-meta`](https://crates.io/crates/nokv-meta) | Schema, `MetadataCommand`, Holt store, service core |
| [`nokv-control`](https://crates.io/crates/nokv-control) | Shard ownership, epochs, and failover coordination |
| [`nokv-agent`](https://crates.io/crates/nokv-agent) | Transport-free agent tool surface (the seven verbs) |
| [`nokv-client`](https://crates.io/crates/nokv-client) | Rust SDK over the metadata service |
| [`nokv-fuse`](https://crates.io/crates/nokv-fuse) | Low-level FUSE frontend |
| [`nokv-server`](https://crates.io/crates/nokv-server) | Long-running metad process and framed RPC |
| [`nokv`](https://crates.io/crates/nokv) | `nokv` CLI binary |


## 📄 License

Apache-2.0. See [LICENSE](LICENSE).
