<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Legacy MemTable Design

The old self-managed Go LSM memtable path has been removed from the mainline
architecture. `local.DB` now uses a raw ordered-KV backend through
`storage/kv`: Pebble is the default implementation and Holt is the owned
backend target. NoKV keeps MVCC internal-key encoding in `txn/storage`.

This page is retained only to keep older links stable. New storage-backend work
should start from [`architecture.md`](architecture.md) and
[`development/code_contract.md`](development/code_contract.md).
