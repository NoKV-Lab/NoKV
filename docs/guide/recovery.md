<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Recovery Model

NoKV recovery is intentionally strict: authoritative state is either recovered
from its owner or startup returns an error. The current mainline separates
recovery ownership by layer.

| Layer | Recovery owner |
| --- | --- |
| Ordered KV | Concrete `storage/kv` backend: Pebble today, Holt target |
| NoKV MVCC keys and versions | `txn/storage`, `txn/mvcc`, and `local.DB` |
| Raft logs and peer snapshots | `raftstore/raftlog` and `raftstore/snapshot` |
| Store-local region catalog | `raftstore/localmeta` |
| Rooted topology / authority truth | `meta/root` |
| fsmeta namespace model | `fsmeta/exec` over `fsmeta/backend` |

The removed self-managed LSM path had manifest/SST/WAL recovery invariants.
Those files are no longer mainline product state. Concrete backends own their
own physical recovery formats, and this version does not provide an online
migration path from old self-managed LSM workdirs.

Useful focused checks:

```bash
go test ./local/... ./txn/... ./raftstore/raftlog ./raftstore/store ./raftstore/server ./fsmeta/contract ./fsmeta/integration -count=1
```
