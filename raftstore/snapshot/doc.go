// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package snapshot defines raftstore-internal region snapshot payloads.
//
// The payload stores canonical NoKV MVCC internal entries plus region metadata.
// It is used for raft peer bootstrap and catch-up only; operator migration,
// SST ingest/export, physical table manifests, and file IDs are intentionally
// outside the generic storage-backend contract.
package snapshot
