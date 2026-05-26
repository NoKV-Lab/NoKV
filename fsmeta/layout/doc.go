// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package layout owns fsmeta's ordered-key backend layout.
//
// It maps fsmeta/model objects and operation requests onto byte keys, byte
// values, affinity buckets, placement ranges, and operation key plans. It does
// not own namespace semantics, storage runtimes, protobufs, or backend clients.
package layout
