// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package kv defines the raw ordered key/value engine contract below NoKV's
// MVCC and fsmeta execution layers.
package kv

// Store is the physical ordered KV boundary. It deliberately exposes raw
// byte keys and values only; MVCC timestamps, column families, raftstore
// regions, fsmeta layout, operator data movement, and table-file details live
// above this package.
type Store interface {
	Get(key []byte) ([]byte, bool, error)
	Put(key, value []byte) error
	Delete(key []byte) error
	DeleteRange(start, end []byte) error
	NewIterator(opts IteratorOptions) (Iterator, error)
	// ApplyBatch persists all mutations atomically. Implementations must make
	// every op visible together or return an error without exposing a partial
	// batch.
	ApplyBatch(batch Batch) error
	Snapshot() (Snapshot, error)
	Sync() error
	Close() error
	Stats() Stats
}

// Batch is one atomic group of raw KV mutations.
type Batch struct {
	Ops []Mutation
}

// MutationOp identifies one raw KV batch operation.
type MutationOp uint8

const (
	PutOp MutationOp = iota + 1
	DeleteOp
	DeleteRangeOp
)

// Mutation is one raw KV mutation. For DeleteRangeOp, Key is the inclusive
// start and End is the exclusive end.
type Mutation struct {
	Op    MutationOp
	Key   []byte
	End   []byte
	Value []byte
}

// IteratorOptions controls raw ordered scans.
type IteratorOptions struct {
	LowerBound []byte
	UpperBound []byte
	Reverse    bool
}

// Iterator is a raw ordered iterator over one store or snapshot view.
type Iterator interface {
	First() bool
	Last() bool
	Seek(key []byte) bool
	Next() bool
	Prev() bool
	Valid() bool
	Key() []byte
	Value() ([]byte, error)
	Close() error
}

// Snapshot is a read-only point-in-time raw KV view.
type Snapshot interface {
	Get(key []byte) ([]byte, bool, error)
	NewIterator(opts IteratorOptions) (Iterator, error)
	Close() error
}

// Stats is intentionally small and backend-neutral.
type Stats struct {
	KeysEstimate uint64 `json:"keys_estimate,omitempty"`
	SizeBytes    uint64 `json:"size_bytes,omitempty"`
}
