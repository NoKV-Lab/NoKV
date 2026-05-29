// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package backend

import "context"

// KV is the minimal ordered key/value tuple fsmeta consumes from backend scans.
type KV struct {
	Key   []byte
	Value []byte
}

// MutationOp names a versioned metadata write operation.
type MutationOp uint8

const (
	MutationPut MutationOp = iota
	MutationDelete
)

// Mutation describes one versioned metadata key mutation.
type Mutation struct {
	Op                MutationOp
	Key               []byte
	Value             []byte
	AssertionNotExist bool
	ExpiresAt         uint64
}

// PredicateKind names a storage predicate a backend must validate under the
// same read/order boundary as its one-phase mutation.
type PredicateKind uint8

const (
	PredicateNotExists PredicateKind = iota
	PredicateExists
	PredicateValueEquals
)

// Predicate describes a backend-validated read predicate for one-phase
// metadata mutations.
type Predicate struct {
	Key           []byte
	Kind          PredicateKind
	ReadVersion   uint64
	ExpectedValue []byte
}

// Store is the minimum MVCC metadata backend required by fsmeta execution.
//
// Mutation atomicity is defined over the supplied mutation group. Implementors
// may use a local one-phase write, Percolator 2PC, or another equivalent
// protocol, but operational data movement, physical ingest/export, and engine
// diagnostics are intentionally outside this contract.
type Store interface {
	ReserveTimestamp(ctx context.Context, count uint64) (uint64, error)
	Get(ctx context.Context, key []byte, version uint64) ([]byte, bool, error)
	BatchGet(ctx context.Context, keys [][]byte, version uint64) (map[string][]byte, error)
	Scan(ctx context.Context, startKey []byte, limit uint32, version uint64) ([]KV, error)
	Mutate(ctx context.Context, primary []byte, mutations []*Mutation, startVersion, commitVersion, lockTTL uint64) (uint64, error)
	MutateAtCommit(ctx context.Context, primary []byte, mutations []*Mutation, startVersion, commitVersion, lockTTL uint64) (uint64, error)
}

// AtomicMutator is an optional one-phase mutation capability.
type AtomicMutator interface {
	TryAtomicMutate(ctx context.Context, primary []byte, predicates []*Predicate, mutations []*Mutation, startVersion, commitVersion uint64) (handled bool, err error)
}

// ReadOrderedAtomicMutator is the one-phase mutation contract fsmeta may
// consume. Implementations must guarantee that a read at version T cannot miss
// any successful one-phase write whose commit version is <= T merely because
// the write had not reached the storage apply boundary yet.
type ReadOrderedAtomicMutator interface {
	AtomicMutator
	AtomicMutatePreservesReadOrder() bool
}

// StatsProvider is implemented by lower runtime layers that expose diagnostics
// without making observability part of Store.
type StatsProvider interface {
	Stats() map[string]any
}
