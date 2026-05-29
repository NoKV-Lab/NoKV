// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package local binds fsmeta execution to the embedded single-node database.
//
// The package owns no distributed authority. It provides one local mount
// admission record, a local inode allocator, a durable backend.Store over
// local.DB, and local watch/snapshot adapters so fsmeta/exec can run without
// coordinator, root, or raftstore.
package local
