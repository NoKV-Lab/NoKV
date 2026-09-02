<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# NoKV FoundationDB Control Store

`nokv-control-fdb` persists the provider-neutral store manifest, root and
logical-shard catalogs, routes, stable owner sessions, and independent
heartbeats below one `nokv-fdb` store prefix. It uses create-only provisioning,
exact catalog CAS, a local monotonic heartbeat observer, and one explicit raw
commit attempt per mutation.

The default build tests physical keys, frozen record codecs, ownership
transitions, prefix admission, and deterministic TTL observation without
loading `libfdb_c`:

```bash
cargo test -p nokv-control-fdb
```

Compile the live adapter with:

```bash
cargo check -p nokv-control-fdb --features fdb --all-targets
```

The concurrent-contender and takeover test requires a disposable FoundationDB
7.3 cluster:

```bash
NOKV_TEST_FDB_CLUSTER_FILE=/absolute/fdb.cluster \
  cargo test -p nokv-control-fdb --features fdb \
  --test fdb_control -- --ignored --nocapture
```

`nokv-server` composes this package only behind its non-default `fdb` feature.
This package and its unit tests do not qualify FDB serving by themselves.
