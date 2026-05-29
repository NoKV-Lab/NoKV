// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package storage

// Store is the narrow internal storage surface used by Percolator and
// raftstore MVCC maintenance code.
type Store interface {
	// ApplyInternalEntries persists one caller-provided batch of internal
	// entries. Implementations used by raft apply must preserve the batch as
	// one atomic storage group; maintenance callers retry whole batches.
	ApplyInternalEntries(entries []*Entry) error
	GetInternalEntry(cf ColumnFamily, key []byte, version uint64) (*Entry, error)
	NewInternalIterator(opt *Options) Iterator
}

// AtomicInternalApplyPlanner reports whether a batch of internal entries will
// be persisted as one indivisible local apply group. Region-local 1PC fast
// paths must fall back to Percolator 2PC when this cannot be proven.
type AtomicInternalApplyPlanner interface {
	CanApplyInternalEntriesAtomically(entries []*Entry) bool
}
