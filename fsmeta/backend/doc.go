// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package backend defines the storage-engine-neutral MVCC surface consumed by
// fsmeta execution.
//
// It owns no namespace semantics and no concrete storage runtime. Implementors
// adapt local, raftstore, or future metadata engines to this contract; fsmeta
// semantic execution remains in fsmeta/exec.
package backend
