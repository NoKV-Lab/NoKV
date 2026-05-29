// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"context"
	"sync/atomic"

	"github.com/feichai0017/NoKV/fsmeta/backend"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
)

// KVClient is the raftstore client surface required by Runner.
type KVClient interface {
	Get(ctx context.Context, key []byte, version uint64) (*kvrpcpb.GetResponse, error)
	BatchGet(ctx context.Context, keys [][]byte, version uint64) (map[string]*kvrpcpb.GetResponse, error)
	Scan(ctx context.Context, startKey []byte, limit uint32, version uint64) ([]*kvrpcpb.KV, error)
	Mutate(ctx context.Context, primary []byte, mutations []*kvrpcpb.Mutation, startVersion, commitVersion, lockTTL uint64) error
}

type atomicMutateOnePhase interface {
	TryAtomicMutate(ctx context.Context, primary []byte, predicates []*kvrpcpb.AtomicPredicate, mutations []*kvrpcpb.Mutation, startVersion, commitVersion uint64) (bool, error)
}

type commitTimestampMutator interface {
	MutateWithCommitTimestamp(ctx context.Context, primary []byte, mutations []*kvrpcpb.Mutation, startVersion, lockTTL uint64, allocateCommitVersion func(context.Context) (uint64, error)) (uint64, error)
}

type statsProvider interface {
	Stats() map[string]any
}

// TSOClient is the coordinator timestamp surface required by Runner.
type TSOClient interface {
	Tso(ctx context.Context, req *coordpb.TsoRequest) (*coordpb.TsoResponse, error)
}

// Runner adapts NoKV's real raftstore client and coordinator TSO into the
// fsmeta/backend.Store contract. It intentionally contains no filesystem
// semantics; fsmeta/exec remains the only interpreter of OperationPlan.
type Runner struct {
	kv                           KVClient
	tso                          *tsoCoalescer
	atomicRunnerUnsupportedTotal atomic.Uint64
}

// NewRunner constructs a backend.Store backed by raftstore KV RPCs and
// coordinator TSO. Both dependencies are explicit because timestamps do not
// belong to the KV data path.
func NewRunner(kv KVClient, tso TSOClient) (*Runner, error) {
	if kv == nil {
		return nil, errKVClientRequired
	}
	if tso == nil {
		return nil, errTSOClientRequired
	}
	return &Runner{kv: kv, tso: newTSOCoalescer(tso, defaultTSOCoalescerConfig())}, nil
}

// ReserveTimestamp reserves count consecutive timestamps from coordinator TSO.
func (r *Runner) ReserveTimestamp(ctx context.Context, count uint64) (uint64, error) {
	if count == 0 {
		return 0, errTimestampCountRequired
	}
	return r.tso.Reserve(ctx, count)
}

// Get returns the value visible at version.
func (r *Runner) Get(ctx context.Context, key []byte, version uint64) ([]byte, bool, error) {
	resp, err := r.kv.Get(ctx, key, version)
	if err != nil {
		return nil, false, err
	}
	if resp == nil || resp.GetNotFound() {
		return nil, false, nil
	}
	if keyErr := resp.GetError(); keyErr != nil {
		return nil, false, runnerKeyError("kv get", keyErr)
	}
	return append([]byte(nil), resp.GetValue()...), true, nil
}

// BatchGet returns found values visible at version, keyed by string(key).
func (r *Runner) BatchGet(ctx context.Context, keys [][]byte, version uint64) (map[string][]byte, error) {
	responses, err := r.kv.BatchGet(ctx, keys, version)
	if err != nil {
		return nil, err
	}
	out := make(map[string][]byte, len(responses))
	for keyID, resp := range responses {
		if resp == nil || resp.GetNotFound() {
			continue
		}
		if keyErr := resp.GetError(); keyErr != nil {
			return nil, runnerKeyError("kv batch get", keyErr)
		}
		out[keyID] = append([]byte(nil), resp.GetValue()...)
	}
	return out, nil
}

// Scan returns up to limit key/value pairs starting at startKey.
func (r *Runner) Scan(ctx context.Context, startKey []byte, limit uint32, version uint64) ([]backend.KV, error) {
	kvs, err := r.kv.Scan(ctx, startKey, limit, version)
	if err != nil {
		return nil, err
	}
	out := make([]backend.KV, 0, len(kvs))
	for _, kv := range kvs {
		if kv == nil {
			continue
		}
		out = append(out, backend.KV{
			Key:   append([]byte(nil), kv.GetKey()...),
			Value: append([]byte(nil), kv.GetValue()...),
		})
	}
	return out, nil
}

// Mutate delegates to raftstore's two-phase commit path and returns the commit
// timestamp that actually published the mutation. Real raftstore clients may
// allocate commit_ts after prewrite; callers that publish root frontiers must
// use the returned timestamp instead of the speculative one they passed in.
func (r *Runner) Mutate(ctx context.Context, primary []byte, mutations []*backend.Mutation, startVersion, commitVersion, lockTTL uint64) (uint64, error) {
	wireMutations, err := mutationsToProto(mutations)
	if err != nil {
		return 0, err
	}
	if kv, ok := r.kv.(commitTimestampMutator); ok {
		// The executor still passes a preallocated commitVersion for in-memory
		// test runners. Real raftstore clients can allocate commit_ts after
		// prewrite, which avoids repeated CommitTsExpired under mixed reads.
		return kv.MutateWithCommitTimestamp(ctx, primary, wireMutations, startVersion, lockTTL, func(ctx context.Context) (uint64, error) {
			// Commit timestamp allocation happens while Percolator locks are
			// live, so it bypasses the short coalescing window to keep lock
			// tenure tight and reduce reader-pushed min_commit_ts churn.
			return r.tso.ReserveImmediate(ctx, 1)
		})
	}
	if err := r.kv.Mutate(ctx, primary, wireMutations, startVersion, commitVersion, lockTTL); err != nil {
		return 0, err
	}
	return commitVersion, nil
}

// MutateAtCommit uses the caller-provided commitVersion exactly. fsmeta uses
// this for root-authority protocols that publish the commit frontier outside the
// KV data path before the transaction can safely allocate a later timestamp.
func (r *Runner) MutateAtCommit(ctx context.Context, primary []byte, mutations []*backend.Mutation, startVersion, commitVersion, lockTTL uint64) (uint64, error) {
	wireMutations, err := mutationsToProto(mutations)
	if err != nil {
		return commitVersion, err
	}
	if err := r.kv.Mutate(ctx, primary, wireMutations, startVersion, commitVersion, lockTTL); err != nil {
		return commitVersion, err
	}
	return commitVersion, nil
}

// Stats returns runtime counters. Nested KV stats come from the real
// raftstore client when available, keeping fsmeta expvar useful without making
// optional observability part of KVClient.
func (r *Runner) Stats() map[string]any {
	if r == nil {
		return map[string]any{
			"atomic_runner_unsupported_total": uint64(0),
		}
	}
	out := map[string]any{
		"atomic_runner_unsupported_total": r.atomicRunnerUnsupportedTotal.Load(),
	}
	if r.tso != nil {
		out["tso"] = r.tso.Stats()
	}
	if stats, ok := r.kv.(statsProvider); ok {
		out["kv"] = stats.Stats()
	}
	return out
}

// TryAtomicMutate delegates to the region-local one-phase mutation path when the
// underlying KV client supports it. handled=false means callers should keep
// the regular Percolator 2PC path.
func (r *Runner) TryAtomicMutate(ctx context.Context, primary []byte, predicates []*backend.Predicate, mutations []*backend.Mutation, startVersion, commitVersion uint64) (bool, error) {
	onePhase, ok := r.kv.(atomicMutateOnePhase)
	if !ok {
		r.atomicRunnerUnsupportedTotal.Add(1)
		return false, nil
	}
	wirePredicates, err := predicatesToProto(predicates)
	if err != nil {
		return false, err
	}
	wireMutations, err := mutationsToProto(mutations)
	if err != nil {
		return false, err
	}
	return onePhase.TryAtomicMutate(ctx, primary, wirePredicates, wireMutations, startVersion, commitVersion)
}

func mutationsToProto(mutations []*backend.Mutation) ([]*kvrpcpb.Mutation, error) {
	out := make([]*kvrpcpb.Mutation, 0, len(mutations))
	for _, mutation := range mutations {
		if mutation == nil {
			out = append(out, nil)
			continue
		}
		op, err := mutationOpToProto(mutation.Op)
		if err != nil {
			return nil, err
		}
		out = append(out, &kvrpcpb.Mutation{
			Op:                op,
			Key:               append([]byte(nil), mutation.Key...),
			Value:             append([]byte(nil), mutation.Value...),
			AssertionNotExist: mutation.AssertionNotExist,
			ExpiresAt:         mutation.ExpiresAt,
		})
	}
	return out, nil
}

func mutationOpToProto(op backend.MutationOp) (kvrpcpb.Mutation_Op, error) {
	switch op {
	case backend.MutationPut:
		return kvrpcpb.Mutation_Put, nil
	case backend.MutationDelete:
		return kvrpcpb.Mutation_Delete, nil
	default:
		return kvrpcpb.Mutation_Put, errUnsupportedMutationOp
	}
}

func predicatesToProto(predicates []*backend.Predicate) ([]*kvrpcpb.AtomicPredicate, error) {
	out := make([]*kvrpcpb.AtomicPredicate, 0, len(predicates))
	for _, predicate := range predicates {
		if predicate == nil {
			out = append(out, nil)
			continue
		}
		kind, err := predicateKindToProto(predicate.Kind)
		if err != nil {
			return nil, err
		}
		out = append(out, &kvrpcpb.AtomicPredicate{
			Key:           append([]byte(nil), predicate.Key...),
			Kind:          kind,
			ReadVersion:   predicate.ReadVersion,
			ExpectedValue: append([]byte(nil), predicate.ExpectedValue...),
		})
	}
	return out, nil
}

func predicateKindToProto(kind backend.PredicateKind) (kvrpcpb.AtomicPredicateKind, error) {
	switch kind {
	case backend.PredicateNotExists:
		return kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_NOT_EXISTS, nil
	case backend.PredicateExists:
		return kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_EXISTS, nil
	case backend.PredicateValueEquals:
		return kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_VALUE_EQUALS, nil
	default:
		return kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_NOT_EXISTS, errUnsupportedPredicateKind
	}
}
