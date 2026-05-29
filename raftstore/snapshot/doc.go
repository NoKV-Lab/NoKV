// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package snapshot defines the backend-neutral region snapshot protocol used
// by raftstore migration and peer bootstrap flows.
//
// Concrete payload formats belong in subpackages. The current LSM external-SST
// implementation lives in raftstore/snapshot/sst, so SST manifests, table file
// IDs, compatibility checks, and rollback hooks do not become a generic
// storage-backend contract.
package snapshot
