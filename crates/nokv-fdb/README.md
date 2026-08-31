<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# NoKV FoundationDB Runtime

`nokv-fdb` owns NoKV's shared FoundationDB boundary: explicit 7.3 API
selection, the one process-global and non-restartable network runtime,
database/transaction handles, common connection options, versioned physical
store subspaces, and error classification.

The default build does not compile or link `libfdb_c`:

```bash
cargo test -p nokv-fdb
```

Compile the binding boundary with:

```bash
cargo check -p nokv-fdb --features fdb --all-targets
```

The runtime performs no automatic transaction retry. Dropping the final
runtime, database, and transaction handle stops the FoundationDB network; that
process cannot restart it. This package does not qualify FoundationDB serving
by itself.
