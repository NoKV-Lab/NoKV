<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# File and mmap primitives

The `storage/file` package provides low-level file and mmap helpers for storage
runtime internals.

---

## 1. Components

| Type | Role |
| --- | --- |
| `file.Options` | Common open parameters: FID, path, flags, max size, and VFS |
| `MmapFile` | Portable mmap-backed file with append, sync, truncate, delete, and remap helpers |
| `vfs.FS` | Filesystem abstraction used by tests for deterministic fault injection |

---

## 2. Sync and Truncation

- `MmapFile.Sync` flushes dirty pages.
- `MmapFile.Truncate` changes the physical file size and remaps when needed.
- `MmapFile.Delete` unmaps, closes, and removes the file.
- Directory sync is handled through `vfs.SyncDir` when callers need rename
  publication safety.

---

## 3. Current Users

| Layer | Usage |
| --- | --- |
| WAL | Segment files and fsync/rotation tests |
| Storage backend support | file and mmap helpers when a concrete backend needs them |
| slab | append-only sidecar segments for namespace-derived caches |

The file layer intentionally does not encode storage semantics such as WAL
record headers, backend table/block formats, or slab consumer payloads.
