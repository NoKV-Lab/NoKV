<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# NoKV FoundationDB Metadata Adapter

`nokv-meta-fdb` implements the storage-neutral `TxnStore` contract over
FoundationDB. It is selected only by the non-default, feature-gated FDB
runtime in `nokv-server` and remains **NOT QUALIFIED** for NoKV serving until
the live serving gates are retained as evidence.

The adapter uses `nokv-fdb` for the explicit 7.3 API selection, shared network
lifetime, database/transaction handles, physical store envelope, common
options, and error classification. A caller starts one `FdbRuntime` and passes
that guard to every `FdbStore::open`; reopen tests keep the guard alive while
individual stores are dropped. Every open also requires one
`FdbMetadataSessionFence` containing the exact stable session key/value plus
the expected owner epoch and session generation. Metadata transactions never
read the independently renewed heartbeat key.

The default build tests options, session-fence validation, physical key
encoding, limits, and error classification without importing or linking the
FoundationDB client:

```bash
cargo test -p nokv-meta-fdb
```

Compile the binding-specific implementation with the selected FoundationDB 7.3
API and embedded headers:

```bash
cargo check -p nokv-meta-fdb --features fdb --all-targets
```

The live suite also requires a compatible `libfdb_c` and a disposable cluster
namespace. The cluster file must be an absolute path. The optional namespace
base must be at most 31 UTF-8 bytes; the test appends a random child name and
clears only that exact generated child namespace.

```bash
NOKV_TEST_FDB_CLUSTER_FILE=/absolute/path/to/fdb.cluster \
NOKV_TEST_FDB_NAMESPACE=nokv-test \
cargo test -p nokv-meta-fdb --features fdb \
  --test fdb_conformance -- --ignored --nocapture
```

The live suite proves that heartbeat changes do not fence metadata and that a
session replacement fences an already-open store's next read and write.
Passing it characterizes the adapter contract. It does not qualify workspace
behavior, unknown outcomes, process or network loss, failover, or performance.
