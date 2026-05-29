// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package percolator implements Google-Percolator-style distributed
// MVCC two-phase commit over NoKV's key/value substrate.
//
// Protocol ops: Prewrite, Commit, Rollback, ResolveLock, CheckTxnStatus,
// TxnHeartBeat.
// Concurrency is controlled by a striped-mutex latch manager
// (txn/latch) shared per raftstore/kv service instance.
// The timestamp oracle is provided by coordinator/tso.
//
// This package is used in distributed mode only. Embedded DB APIs use the
// local MVCC path without percolator's 2PC.
package percolator

import (
	"bytes"
	"errors"
	"sync/atomic"
	"time"

	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"

	"github.com/feichai0017/NoKV/txn/latch"
	"github.com/feichai0017/NoKV/txn/mvcc"
	txnstore "github.com/feichai0017/NoKV/txn/storage"
	"github.com/feichai0017/NoKV/utils"
)

// Prewrite applies mutation prewrites for a single region transaction.
func Prewrite(db txnstore.Store, latches *latch.Manager, req *kvrpcpb.PrewriteRequest) []*kvrpcpb.KeyError {
	results := PrewriteBatch(db, latches, []*kvrpcpb.PrewriteRequest{req})
	if len(results) == 0 {
		return nil
	}
	return results[0]
}

// PrewriteBatch applies a raft-entry batch of prewrite requests. It keeps
// request-level errors independent while fusing local storage apply for
// non-overlapping requests.
func PrewriteBatch(db txnstore.Store, latches *latch.Manager, reqs []*kvrpcpb.PrewriteRequest) [][]*kvrpcpb.KeyError {
	results := make([][]*kvrpcpb.KeyError, len(reqs))
	keys := make([][]byte, 0)
	for _, req := range reqs {
		keys = append(keys, prewriteRequestKeys(req)...)
	}
	guard := latches.Acquire(keys)
	defer guard.Release()

	reader := NewReader(db)
	group := twoPCApplyGroup{}
	for i, req := range reqs {
		if req == nil {
			continue
		}
		requestKeys := prewriteRequestKeys(req)
		if group.conflicts(requestKeys) {
			group.flush(db, func(item twoPCApplyRequest, err *kvrpcpb.KeyError) {
				if err != nil {
					results[item.index] = []*kvrpcpb.KeyError{err}
				}
			})
		}
		var errs []*kvrpcpb.KeyError
		ops := make([]versionedOp, 0, len(req.Mutations)*3)
		for _, mut := range req.Mutations {
			if mut == nil {
				continue
			}
			planned, err := planPrewriteMutation(reader, req, mut)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			ops = append(ops, planned...)
		}
		if len(errs) > 0 {
			results[i] = errs
			continue
		}
		group.add(i, requestKeys, ops, 0)
	}
	group.flush(db, func(item twoPCApplyRequest, err *kvrpcpb.KeyError) {
		if err != nil {
			results[item.index] = []*kvrpcpb.KeyError{err}
		}
	})
	return results
}

func planPrewriteMutation(reader *Reader, req *kvrpcpb.PrewriteRequest, mut *kvrpcpb.Mutation) ([]versionedOp, *kvrpcpb.KeyError) {
	key := mut.GetKey()
	if len(key) == 0 {
		return nil, keyErrorAbort(errEmptyMutationKey)
	}
	lock, err := reader.GetLock(key)
	if err != nil {
		return nil, keyErrorRetryable(err)
	}
	if lock != nil && lock.Ts != req.StartVersion {
		return nil, keyErrorLocked(key, lock)
	}
	if write, commitTs, err := reader.MostRecentWrite(key); err != nil {
		return nil, keyErrorRetryable(err)
	} else if write != nil && commitTs >= req.StartVersion {
		return nil, keyErrorWriteConflict(key, req.PrimaryLock, commitTs, write.StartTs, req.StartVersion)
	}
	if mut.GetAssertionNotExist() {
		exists, err := keyExistsAt(reader, key, req.StartVersion)
		if err != nil {
			return nil, keyErrorRetryable(err)
		}
		if exists {
			return nil, keyErrorAlreadyExists(key)
		}
	}
	ops := make([]versionedOp, 0, 3)
	var shortValue []byte
	var shortValueExpiresAt uint64
	switch mut.Op {
	case kvrpcpb.Mutation_Put:
		if mvcc.CanInlineShortValue(mut.GetOp(), mut.GetValue()) {
			// Commit sees only the lock, not the original mutation request.
			// Keep small values in the lock so commit can materialize the
			// committed write without writing a default-CF record.
			shortValue = txnstore.SafeCopy(nil, mut.GetValue())
			shortValueExpiresAt = mut.GetExpiresAt()
			percolatorStats.shortValuePrewriteTotal.Add(1)
		} else {
			ops = append(ops,
				versionedOp{cf: txnstore.CFDefault, key: key, version: req.StartVersion, meta: txnstore.BitDelete},
				versionedOp{cf: txnstore.CFDefault, key: key, version: req.StartVersion, value: mut.Value, expires: mut.GetExpiresAt()},
			)
		}
	case kvrpcpb.Mutation_Delete, kvrpcpb.Mutation_Lock:
		ops = append(ops,
			versionedOp{cf: txnstore.CFDefault, key: key, version: req.StartVersion, meta: txnstore.BitDelete},
		)
	default:
		return nil, keyErrorAbortf(errUnsupportedMutationOp, "%v", mut.Op)
	}
	newLock := mvcc.Lock{
		Primary:     txnstore.SafeCopy(nil, req.PrimaryLock),
		Ts:          req.StartVersion,
		StartTime:   currentPhysicalTimeMillis(),
		TTL:         req.LockTtl,
		Kind:        mut.Op,
		MinCommitTs: req.MinCommitTs,
		ShortValue:  shortValue,
		ExpiresAt:   shortValueExpiresAt,
	}
	encoded := mvcc.EncodeLock(newLock)
	ops = append(ops, versionedOp{cf: txnstore.CFLock, key: key, version: lockColumnTs, value: encoded})
	return ops, nil
}

func prewriteRequestKeys(req *kvrpcpb.PrewriteRequest) [][]byte {
	if req == nil {
		return nil
	}
	keys := make([][]byte, 0, len(req.Mutations))
	for _, mut := range req.Mutations {
		if mut != nil && len(mut.Key) > 0 {
			keys = append(keys, mut.Key)
		}
	}
	return keys
}

// validateCommitVersion rejects commits that would violate MVCC ordering.
func validateCommitVersion(StartVersion uint64, CommitVersion uint64) *kvrpcpb.KeyError {
	if CommitVersion <= StartVersion {
		return keyErrorAbort(errCommitVersionNotAfterStart)
	}
	return nil
}

// Commit finalises earlier prewrites by removing locks and writing commit
// records. A non-nil KeyError is returned when commit should abort.
func Commit(db txnstore.Store, latches *latch.Manager, req *kvrpcpb.CommitRequest) *kvrpcpb.KeyError {
	results := CommitBatch(db, latches, []*kvrpcpb.CommitRequest{req})
	if len(results) == 0 {
		return nil
	}
	return results[0]
}

// CommitBatch applies a raft-entry batch of commit requests. Percolator still
// decides each transaction at its primary key; this only reduces local apply
// fragmentation after raft has already ordered the commit requests.
func CommitBatch(db txnstore.Store, latches *latch.Manager, reqs []*kvrpcpb.CommitRequest) []*kvrpcpb.KeyError {
	results := make([]*kvrpcpb.KeyError, len(reqs))
	keys := make([][]byte, 0)
	for _, req := range reqs {
		if req != nil {
			keys = append(keys, req.Keys...)
		}
	}
	guard := latches.Acquire(keys)
	defer guard.Release()

	reader := NewReader(db)
	group := twoPCApplyGroup{}
	for i, req := range reqs {
		if req == nil {
			continue
		}
		if err := validateCommitVersion(req.StartVersion, req.CommitVersion); err != nil {
			results[i] = err
			continue
		}
		if group.conflicts(req.Keys) {
			group.flush(db, func(item twoPCApplyRequest, err *kvrpcpb.KeyError) {
				results[item.index] = err
			})
		}
		ops := make([]versionedOp, 0, len(req.Keys)*2)
		for _, key := range req.Keys {
			planned, err := planCommitKey(reader, key, req.StartVersion, req.CommitVersion)
			if err != nil {
				results[i] = err
				break
			}
			ops = append(ops, planned...)
		}
		if results[i] != nil {
			continue
		}
		group.add(i, req.Keys, ops, 0)
	}
	group.flush(db, func(item twoPCApplyRequest, err *kvrpcpb.KeyError) {
		results[item.index] = err
	})
	return results
}

// AtomicMutateResult is the local outcome for a region-local 1PC attempt.
type AtomicMutateResult struct {
	Error       *kvrpcpb.KeyError
	AppliedKeys uint64
	Fallback    bool
}

type statsRegistry struct {
	applyCalledTotal        atomic.Uint64
	fusedApplyBatchesTotal  atomic.Uint64
	fusedApplyRequestsTotal atomic.Uint64
	fusedApplyEntriesTotal  atomic.Uint64
	twoPCFusedBatchesTotal  atomic.Uint64
	twoPCFusedRequestsTotal atomic.Uint64
	twoPCFusedEntriesTotal  atomic.Uint64
	shortValuePrewriteTotal atomic.Uint64
	shortValueCommitTotal   atomic.Uint64
	shortValueAtomicTotal   atomic.Uint64
}

var percolatorStats statsRegistry

// Stats returns package-wide server-side Percolator counters. These are
// deliberately protocol-level counters, so callers can tell whether requests
// reached Percolator after client-side region admission.
func Stats() map[string]any {
	return map[string]any{
		"atomic_apply_called_total":         percolatorStats.applyCalledTotal.Load(),
		"atomic_fused_apply_batches_total":  percolatorStats.fusedApplyBatchesTotal.Load(),
		"atomic_fused_apply_requests_total": percolatorStats.fusedApplyRequestsTotal.Load(),
		"atomic_fused_apply_entries_total":  percolatorStats.fusedApplyEntriesTotal.Load(),
		"two_pc_fused_apply_batches_total":  percolatorStats.twoPCFusedBatchesTotal.Load(),
		"two_pc_fused_apply_requests_total": percolatorStats.twoPCFusedRequestsTotal.Load(),
		"two_pc_fused_apply_entries_total":  percolatorStats.twoPCFusedEntriesTotal.Load(),
		"short_value_prewrite_total":        percolatorStats.shortValuePrewriteTotal.Load(),
		"short_value_commit_total":          percolatorStats.shortValueCommitTotal.Load(),
		"short_value_atomic_total":          percolatorStats.shortValueAtomicTotal.Load(),
	}
}

// ApplyAtomicMutate tries to materialize a region-local MVCC mutation without
// exposing Percolator locks. The selected storage backend owns the physical
// atomicity of the internal entry batch.
func ApplyAtomicMutate(db txnstore.Store, latches *latch.Manager, req *kvrpcpb.TryAtomicMutateRequest) AtomicMutateResult {
	if req == nil {
		return AtomicMutateResult{}
	}
	results := ApplyAtomicMutateBatch(db, latches, []*kvrpcpb.TryAtomicMutateRequest{req})
	if len(results) == 0 {
		return AtomicMutateResult{}
	}
	return results[0]
}

// ApplyAtomicMutateBatch applies a raft-entry batch of independent 1PC
// attempts. It preserves per-request responses and fuses non-overlapping
// requests into one storage-backed atomic apply group.
func ApplyAtomicMutateBatch(db txnstore.Store, latches *latch.Manager, reqs []*kvrpcpb.TryAtomicMutateRequest) []AtomicMutateResult {
	results := make([]AtomicMutateResult, len(reqs))
	if len(reqs) == 0 {
		return results
	}
	prepared := make([]atomicMutatePrepared, len(reqs))
	var allKeys [][]byte
	var called uint64
	for i, req := range reqs {
		item := prepareAtomicMutate(req)
		prepared[i] = item
		results[i] = item.result
		if !item.eligible {
			continue
		}
		called++
		allKeys = append(allKeys, item.keys...)
	}
	if called == 0 {
		return results
	}
	percolatorStats.applyCalledTotal.Add(called)

	guard := latches.Acquire(allKeys)
	defer guard.Release()

	group := atomicMutateApplyGroup{}
	for i, item := range prepared {
		if !item.eligible {
			continue
		}
		// Overlapping requests must observe raft-log order. Flush before
		// planning the later request so its predicates see earlier writes.
		if group.conflicts(item.keys) {
			group.flush(db, results)
		}
		plan, result, ok := planAtomicMutateUnlocked(db, item.req, item.keys)
		if !ok {
			results[i] = result
			continue
		}
		if len(plan.entries) == 0 {
			results[i] = AtomicMutateResult{AppliedKeys: plan.appliedKeys}
			continue
		}
		group.add(i, plan)
	}
	group.flush(db, results)
	return results
}

type atomicMutatePrepared struct {
	req      *kvrpcpb.TryAtomicMutateRequest
	keys     [][]byte
	eligible bool
	result   AtomicMutateResult
}

func prepareAtomicMutate(req *kvrpcpb.TryAtomicMutateRequest) atomicMutatePrepared {
	if req == nil {
		return atomicMutatePrepared{}
	}
	if err := validateCommitVersion(req.StartVersion, req.CommitVersion); err != nil {
		return atomicMutatePrepared{result: AtomicMutateResult{Error: err}}
	}
	mutations := req.GetMutations()
	if len(mutations) == 0 {
		return atomicMutatePrepared{}
	}
	keys := make([][]byte, 0, len(mutations)+len(req.GetPredicates()))
	for _, mut := range mutations {
		if mut != nil && len(mut.Key) > 0 {
			keys = append(keys, mut.Key)
		}
	}
	for _, pred := range req.GetPredicates() {
		if pred != nil && len(pred.Key) > 0 {
			keys = append(keys, pred.Key)
		}
	}
	return atomicMutatePrepared{req: req, keys: keys, eligible: true}
}

type atomicMutatePlan struct {
	entries     []*txnstore.Entry
	appliedKeys uint64
	keys        [][]byte
}

func planAtomicMutateUnlocked(db txnstore.Store, req *kvrpcpb.TryAtomicMutateRequest, keys [][]byte) (atomicMutatePlan, AtomicMutateResult, bool) {
	mutations := req.GetMutations()
	reader := NewReader(db)
	if applied, err := atomicMutateAlreadyApplied(db, reader, req); err != nil {
		return atomicMutatePlan{}, AtomicMutateResult{Error: keyErrorRetryable(err)}, false
	} else if applied {
		return atomicMutatePlan{}, AtomicMutateResult{AppliedKeys: uint64(len(mutations))}, false
	}

	primary := mutations[0].GetKey()
	ops := make([]versionedOp, 0, len(mutations)*3)
	for _, pred := range req.GetPredicates() {
		if err := validateAtomicPredicate(reader, pred, req.StartVersion); err != nil {
			return atomicMutatePlan{}, AtomicMutateResult{Error: err}, false
		}
	}
	for _, mut := range mutations {
		if mut == nil {
			continue
		}
		if err := validateAtomicMutation(mut); err != nil {
			return atomicMutatePlan{}, AtomicMutateResult{Error: err}, false
		}
		key := mut.GetKey()
		lock, err := reader.GetLock(key)
		if err != nil {
			return atomicMutatePlan{}, AtomicMutateResult{Error: keyErrorRetryable(err)}, false
		}
		if lock != nil {
			return atomicMutatePlan{}, AtomicMutateResult{Error: keyErrorLocked(key, lock)}, false
		}
		if write, commitTs, err := reader.MostRecentWrite(key); err != nil {
			return atomicMutatePlan{}, AtomicMutateResult{Error: keyErrorRetryable(err)}, false
		} else if write != nil && commitTs >= req.StartVersion {
			return atomicMutatePlan{}, AtomicMutateResult{Error: keyErrorWriteConflict(key, primary, commitTs, write.StartTs, req.StartVersion)}, false
		}
		if mut.GetAssertionNotExist() {
			exists, err := keyExistsAt(reader, key, req.StartVersion)
			if err != nil {
				return atomicMutatePlan{}, AtomicMutateResult{Error: keyErrorRetryable(err)}, false
			}
			if exists {
				return atomicMutatePlan{}, AtomicMutateResult{Error: keyErrorAlreadyExists(key)}, false
			}
		}
		ops = append(ops, committedMutationOps(mut, req.StartVersion, req.CommitVersion)...)
	}
	entries := versionedOpsToEntries(ops...)
	return atomicMutatePlan{entries: entries, appliedKeys: uint64(len(mutations)), keys: keys}, AtomicMutateResult{}, true
}

type atomicMutateApplyGroup struct {
	requests []atomicMutateGroupRequest
	entries  []*txnstore.Entry
	keys     map[string]struct{}
}

type atomicMutateGroupRequest struct {
	index       int
	appliedKeys uint64
}

func (g *atomicMutateApplyGroup) conflicts(keys [][]byte) bool {
	if g == nil || len(g.keys) == 0 {
		return false
	}
	for _, key := range keys {
		if _, ok := g.keys[string(key)]; ok {
			return true
		}
	}
	return false
}

func (g *atomicMutateApplyGroup) add(index int, plan atomicMutatePlan) {
	g.requests = append(g.requests, atomicMutateGroupRequest{index: index, appliedKeys: plan.appliedKeys})
	g.entries = append(g.entries, plan.entries...)
	if g.keys == nil {
		g.keys = make(map[string]struct{}, len(plan.keys))
	}
	for _, key := range plan.keys {
		g.keys[string(key)] = struct{}{}
	}
}

func (g *atomicMutateApplyGroup) flush(db txnstore.Store, results []AtomicMutateResult) {
	if g == nil || len(g.requests) == 0 {
		return
	}
	defer releaseEntries(g.entries)
	if len(g.requests) > 1 {
		percolatorStats.fusedApplyBatchesTotal.Add(1)
		percolatorStats.fusedApplyRequestsTotal.Add(uint64(len(g.requests)))
		percolatorStats.fusedApplyEntriesTotal.Add(uint64(len(g.entries)))
	}
	err := applyVersionedEntries(db, g.entries)
	for _, req := range g.requests {
		if err != nil {
			results[req.index] = AtomicMutateResult{Error: keyErrorRetryable(err)}
			continue
		}
		results[req.index] = AtomicMutateResult{AppliedKeys: req.appliedKeys}
	}
	g.requests = nil
	g.entries = nil
	g.keys = nil
}

func validateAtomicPredicate(reader *Reader, pred *kvrpcpb.AtomicPredicate, startVersion uint64) *kvrpcpb.KeyError {
	if pred == nil {
		return keyErrorAbort(errInvalidAtomicMutate)
	}
	key := pred.GetKey()
	if len(key) == 0 {
		return keyErrorAbort(errEmptyMutationKey)
	}
	lock, err := reader.GetLock(key)
	if err != nil {
		return keyErrorRetryable(err)
	}
	if lock != nil {
		// Predicates can reference keys outside the mutation set. They still
		// participate in transaction conflict detection; otherwise a 1PC
		// compare could observe a committed snapshot while ignoring a live
		// Percolator writer on the same key.
		return keyErrorLocked(key, lock)
	}
	readVersion := pred.GetReadVersion()
	if readVersion == 0 {
		readVersion = startVersion
	}
	exists, err := keyExistsAt(reader, key, readVersion)
	if err != nil {
		return keyErrorRetryable(err)
	}
	switch pred.GetKind() {
	case kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_NOT_EXISTS:
		if exists {
			return keyErrorAlreadyExists(key)
		}
	case kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_EXISTS:
		if !exists {
			return keyErrorAbort(errInvalidAtomicMutate)
		}
	case kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_VALUE_EQUALS:
		value, _, err := reader.GetValue(key, readVersion)
		if err != nil {
			if errors.Is(err, utils.ErrKeyNotFound) {
				return keyErrorRetryable(errAtomicPredicateMismatch)
			}
			return keyErrorRetryable(err)
		}
		if !bytes.Equal(value, pred.GetExpectedValue()) {
			return keyErrorRetryable(errAtomicPredicateMismatch)
		}
	default:
		return keyErrorAbort(errInvalidAtomicMutate)
	}
	return nil
}

func validateAtomicMutation(mut *kvrpcpb.Mutation) *kvrpcpb.KeyError {
	if len(mut.GetKey()) == 0 {
		return keyErrorAbort(errEmptyMutationKey)
	}
	switch mut.GetOp() {
	case kvrpcpb.Mutation_Put, kvrpcpb.Mutation_Delete:
	default:
		return keyErrorAbortf(errUnsupportedMutationOp, "%v", mut.GetOp())
	}
	return nil
}

func atomicMutateAlreadyApplied(db txnstore.Store, reader *Reader, req *kvrpcpb.TryAtomicMutateRequest) (bool, error) {
	anyPresent := false
	allPresent := true
	for _, mut := range req.GetMutations() {
		if mut == nil {
			continue
		}
		write, commitTs, err := reader.GetWriteByStartTs(mut.GetKey(), req.StartVersion)
		if err != nil {
			return false, err
		}
		if write == nil {
			allPresent = false
			continue
		}
		anyPresent = true
		if commitTs != req.CommitVersion || write.Kind != mut.GetOp() {
			return false, nil
		}
		if mut.GetOp() == kvrpcpb.Mutation_Put {
			matches, err := committedPutMatches(db, write, mut, req.StartVersion)
			if err != nil || !matches {
				return false, err
			}
		}
	}
	return anyPresent && allPresent, nil
}

func committedPutMatches(db txnstore.Store, write *mvcc.Write, mut *kvrpcpb.Mutation, startVersion uint64) (bool, error) {
	if len(write.ShortValue) > 0 {
		return bytes.Equal(write.ShortValue, mut.GetValue()) && write.ExpiresAt == mut.GetExpiresAt(), nil
	}
	return defaultRecordMatches(db, mut, startVersion)
}

func defaultRecordMatches(db txnstore.Store, mut *kvrpcpb.Mutation, startVersion uint64) (bool, error) {
	entry, err := db.GetInternalEntry(txnstore.CFDefault, mut.GetKey(), startVersion)
	if err != nil {
		return false, err
	}
	defer entry.DecrRef()
	if entry.Meta&txnstore.BitDelete > 0 {
		return false, nil
	}
	return bytes.Equal(entry.Value, mut.GetValue()) && entry.ExpiresAt == mut.GetExpiresAt(), nil
}

func committedMutationOps(mut *kvrpcpb.Mutation, startVersion, commitVersion uint64) []versionedOp {
	switch mut.GetOp() {
	case kvrpcpb.Mutation_Put:
		write := committedWriteForMutation(mut, startVersion)
		if len(write.ShortValue) > 0 {
			percolatorStats.shortValueAtomicTotal.Add(1)
			return []versionedOp{
				{cf: txnstore.CFWrite, key: mut.GetKey(), version: commitVersion, value: mvcc.EncodeWrite(write)},
			}
		}
		return []versionedOp{
			{cf: txnstore.CFDefault, key: mut.GetKey(), version: startVersion, meta: txnstore.BitDelete},
			{cf: txnstore.CFDefault, key: mut.GetKey(), version: startVersion, value: mut.GetValue(), expires: mut.GetExpiresAt()},
			{cf: txnstore.CFWrite, key: mut.GetKey(), version: commitVersion, value: mvcc.EncodeWrite(write)},
		}
	case kvrpcpb.Mutation_Delete:
		write := mvcc.EncodeWrite(mvcc.Write{Kind: mut.GetOp(), StartTs: startVersion})
		return []versionedOp{
			{cf: txnstore.CFDefault, key: mut.GetKey(), version: startVersion, meta: txnstore.BitDelete},
			{cf: txnstore.CFWrite, key: mut.GetKey(), version: commitVersion, value: write},
		}
	default:
		return nil
	}
}

func committedWriteForMutation(mut *kvrpcpb.Mutation, startVersion uint64) mvcc.Write {
	write := mvcc.Write{Kind: mut.GetOp(), StartTs: startVersion}
	if mvcc.CanInlineShortValue(mut.GetOp(), mut.GetValue()) {
		write.ShortValue = txnstore.SafeCopy(nil, mut.GetValue())
		write.ExpiresAt = mut.GetExpiresAt()
	}
	return write
}

// BatchRollback rolls back the provided keys for the given start version.
func BatchRollback(db txnstore.Store, latches *latch.Manager, req *kvrpcpb.BatchRollbackRequest) *kvrpcpb.KeyError {
	results := BatchRollbackBatch(db, latches, []*kvrpcpb.BatchRollbackRequest{req})
	if len(results) == 0 {
		return nil
	}
	return results[0]
}

// BatchRollbackBatch applies a raft-entry batch of rollback requests. Rollback
// remains idempotent per start_ts; this only shares local storage apply for
// independent rollback records already ordered by raft.
func BatchRollbackBatch(db txnstore.Store, latches *latch.Manager, reqs []*kvrpcpb.BatchRollbackRequest) []*kvrpcpb.KeyError {
	results := make([]*kvrpcpb.KeyError, len(reqs))
	keys := make([][]byte, 0)
	for _, req := range reqs {
		if req != nil {
			keys = append(keys, req.Keys...)
		}
	}
	guard := latches.Acquire(keys)
	defer guard.Release()

	reader := NewReader(db)
	group := twoPCApplyGroup{}
	for i, req := range reqs {
		if req == nil {
			continue
		}
		if group.conflicts(req.Keys) {
			group.flush(db, func(item twoPCApplyRequest, err *kvrpcpb.KeyError) {
				results[item.index] = err
			})
		}
		ops := make([]versionedOp, 0, len(req.Keys)*3)
		for _, key := range req.Keys {
			planned, err := planRollbackKey(reader, key, req.StartVersion)
			if err != nil {
				results[i] = err
				break
			}
			ops = append(ops, planned...)
		}
		if results[i] != nil {
			continue
		}
		group.add(i, req.Keys, ops, 0)
	}
	group.flush(db, func(item twoPCApplyRequest, err *kvrpcpb.KeyError) {
		results[item.index] = err
	})
	return results
}

type ResolveLockResult struct {
	ResolvedLocks uint64
	Error         *kvrpcpb.KeyError
}

// ResolveLock resolves locks for the given transaction. commitVersion == 0
// performs a rollback; otherwise the keys are committed.
func ResolveLock(db txnstore.Store, latches *latch.Manager, req *kvrpcpb.ResolveLockRequest) (uint64, *kvrpcpb.KeyError) {
	results := ResolveLockBatch(db, latches, []*kvrpcpb.ResolveLockRequest{req})
	if len(results) == 0 {
		return 0, nil
	}
	return results[0].ResolvedLocks, results[0].Error
}

// ResolveLockBatch applies a raft-entry batch of lock resolution requests. It
// preserves resolved-count semantics for every logical request while sharing the
// local apply when the resolved key sets do not overlap.
func ResolveLockBatch(db txnstore.Store, latches *latch.Manager, reqs []*kvrpcpb.ResolveLockRequest) []ResolveLockResult {
	results := make([]ResolveLockResult, len(reqs))
	keys := make([][]byte, 0)
	for _, req := range reqs {
		if req != nil {
			keys = append(keys, req.Keys...)
		}
	}
	guard := latches.Acquire(keys)
	defer guard.Release()

	reader := NewReader(db)
	group := twoPCApplyGroup{}
	for i, req := range reqs {
		if req == nil {
			continue
		}
		if req.CommitVersion != 0 {
			if err := validateCommitVersion(req.StartVersion, req.CommitVersion); err != nil {
				results[i].Error = err
				continue
			}
		}
		if group.conflicts(req.Keys) {
			group.flush(db, func(item twoPCApplyRequest, err *kvrpcpb.KeyError) {
				if err != nil {
					results[item.index].Error = err
					return
				}
				results[item.index].ResolvedLocks = item.resolvedLocks
			})
		}
		var resolved uint64
		ops := make([]versionedOp, 0, len(req.Keys)*3)
		seen := make(map[string]struct{}, len(req.Keys))
		for _, key := range req.Keys {
			if len(key) == 0 {
				continue
			}
			keyID := string(key)
			if _, ok := seen[keyID]; ok {
				continue
			}
			seen[keyID] = struct{}{}
			lock, err := reader.GetLock(key)
			if err != nil {
				results[i].Error = keyErrorRetryable(err)
				break
			}
			if lock == nil || lock.Ts != req.StartVersion {
				continue
			}
			if req.CommitVersion == 0 {
				planned, err := planRollbackKey(reader, key, req.StartVersion)
				if err != nil {
					results[i].Error = err
					break
				}
				ops = append(ops, planned...)
			} else {
				planned, err := planCommitKeyWithLock(reader, key, lock, req.CommitVersion)
				if err != nil {
					results[i].Error = err
					break
				}
				ops = append(ops, planned...)
			}
			resolved++
		}
		if results[i].Error != nil {
			continue
		}
		if len(ops) == 0 {
			results[i].ResolvedLocks = resolved
			continue
		}
		group.add(i, req.Keys, ops, resolved)
	}
	group.flush(db, func(item twoPCApplyRequest, err *kvrpcpb.KeyError) {
		if err != nil {
			results[item.index].Error = err
			return
		}
		results[item.index].ResolvedLocks = item.resolvedLocks
	})
	return results
}

// CheckTxnStatus inspects the primary lock state and optionally rolls back
// expired transactions.
func CheckTxnStatus(db txnstore.Store, latches *latch.Manager, req *kvrpcpb.CheckTxnStatusRequest) *kvrpcpb.CheckTxnStatusResponse {
	resp := &kvrpcpb.CheckTxnStatusResponse{}
	if req == nil {
		return resp
	}
	keys := [][]byte{req.PrimaryKey}
	guard := latches.Acquire(keys)
	defer guard.Release()

	reader := NewReader(db)
	lock, err := reader.GetLock(req.PrimaryKey)
	if err != nil {
		resp.Error = keyErrorRetryable(err)
		return resp
	}
	if lock != nil {
		if lock.Ts != req.LockTs {
			resp.Error = keyErrorLocked(req.PrimaryKey, lock)
			return resp
		}
		if isLockExpired(lock, req.CurrentTime) {
			if err := rollbackKey(db, reader, req.PrimaryKey, req.LockTs); err != nil {
				resp.Error = err
				return resp
			}
			resp.Action = kvrpcpb.CheckTxnStatusAction_CheckTxnStatusTTLExpireRollback
			return resp
		}
		if req.CallerStartTs > 0 && lock.MinCommitTs < req.CallerStartTs+1 {
			lock.MinCommitTs = req.CallerStartTs + 1
			if err := applyVersionedOps(db, versionedOp{
				cf:      txnstore.CFLock,
				key:     req.PrimaryKey,
				version: lockColumnTs,
				value:   mvcc.EncodeLock(*lock),
			}); err != nil {
				resp.Error = keyErrorRetryable(err)
				return resp
			}
			resp.Action = kvrpcpb.CheckTxnStatusAction_CheckTxnStatusMinCommitTsPushed
		}
		resp.LockTtl = lock.TTL
		return resp
	}

	write, commitTs, err := reader.GetWriteByStartTs(req.PrimaryKey, req.LockTs)
	if err != nil {
		resp.Error = keyErrorRetryable(err)
		return resp
	}
	if write != nil {
		if write.Kind == kvrpcpb.Mutation_Rollback {
			resp.Action = kvrpcpb.CheckTxnStatusAction_CheckTxnStatusLockNotExistRollback
			return resp
		}
		resp.CommitVersion = commitTs
		return resp
	}

	if req.RollbackIfNotExist {
		if err := rollbackKey(db, reader, req.PrimaryKey, req.LockTs); err != nil {
			resp.Error = err
		} else {
			resp.Action = kvrpcpb.CheckTxnStatusAction_CheckTxnStatusLockNotExistRollback
		}
	}
	return resp
}

// TxnHeartBeat extends the primary lock TTL for a live transaction. It never
// resurrects an expired or already-resolved primary lock.
func TxnHeartBeat(db txnstore.Store, latches *latch.Manager, req *kvrpcpb.TxnHeartBeatRequest) *kvrpcpb.TxnHeartBeatResponse {
	resp := &kvrpcpb.TxnHeartBeatResponse{}
	if req == nil {
		return resp
	}
	if len(req.PrimaryKey) == 0 {
		resp.Error = keyErrorAbort(errTxnHeartbeatPrimaryRequired)
		return resp
	}
	if req.StartVersion == 0 {
		resp.Error = keyErrorAbort(errTxnHeartbeatStartRequired)
		return resp
	}
	if req.TtlExtension == 0 {
		resp.Error = keyErrorAbort(errTxnHeartbeatTTLRequired)
		return resp
	}
	if req.CurrentTime == 0 {
		resp.Error = keyErrorAbort(errTxnHeartbeatTimeRequired)
		return resp
	}

	guard := latches.Acquire([][]byte{req.PrimaryKey})
	defer guard.Release()

	reader := NewReader(db)
	lock, err := reader.GetLock(req.PrimaryKey)
	if err != nil {
		resp.Error = keyErrorRetryable(err)
		return resp
	}
	if lock != nil {
		if lock.Ts != req.StartVersion {
			resp.Error = keyErrorLocked(req.PrimaryKey, lock)
			return resp
		}
		if !bytes.Equal(lock.Primary, req.PrimaryKey) {
			resp.Error = keyErrorAbort(errTxnHeartbeatPrimaryMismatch)
			return resp
		}
		if isLockExpired(lock, req.CurrentTime) {
			if err := rollbackKey(db, reader, req.PrimaryKey, req.StartVersion); err != nil {
				resp.Error = err
				return resp
			}
			resp.Action = kvrpcpb.TxnHeartBeatAction_TxnHeartBeatTTLExpireRollback
			return resp
		}
		desiredTTL := req.TtlExtension
		if req.CurrentTime > lock.StartTime {
			desiredTTL = req.CurrentTime - lock.StartTime + req.TtlExtension
		}
		if desiredTTL > lock.TTL {
			lock.TTL = desiredTTL
			if err := applyVersionedOps(db, versionedOp{
				cf:      txnstore.CFLock,
				key:     req.PrimaryKey,
				version: lockColumnTs,
				value:   mvcc.EncodeLock(*lock),
			}); err != nil {
				resp.Error = keyErrorRetryable(err)
				return resp
			}
			resp.Action = kvrpcpb.TxnHeartBeatAction_TxnHeartBeatTTLExtended
		}
		resp.LockTtl = lock.TTL
		resp.LockExpireTime = lockExpireTime(lock)
		return resp
	}

	write, commitTs, err := reader.GetWriteByStartTs(req.PrimaryKey, req.StartVersion)
	if err != nil {
		resp.Error = keyErrorRetryable(err)
		return resp
	}
	if write != nil && write.Kind != kvrpcpb.Mutation_Rollback {
		resp.CommitVersion = commitTs
		return resp
	}
	if err := rollbackKey(db, reader, req.PrimaryKey, req.StartVersion); err != nil {
		resp.Error = err
		return resp
	}
	resp.Action = kvrpcpb.TxnHeartBeatAction_TxnHeartBeatLockNotExistRollback
	return resp
}

func keyExistsAt(reader *Reader, key []byte, readTs uint64) (bool, error) {
	write, _, err := reader.getWriteForRead(key, readTs)
	if err != nil {
		return false, err
	}
	if write == nil {
		return false, nil
	}
	switch write.Kind {
	case kvrpcpb.Mutation_Delete, kvrpcpb.Mutation_Rollback:
		return false, nil
	default:
		return true, nil
	}
}

func planCommitKey(reader *Reader, key []byte, startVersion, commitVersion uint64) ([]versionedOp, *kvrpcpb.KeyError) {
	if len(key) == 0 {
		return nil, keyErrorAbort(errEmptyCommitKey)
	}
	lock, err := reader.GetLock(key)
	if err != nil {
		return nil, keyErrorRetryable(err)
	}
	if lock == nil {
		write, _, err := reader.GetWriteByStartTs(key, startVersion)
		if err != nil {
			return nil, keyErrorRetryable(err)
		}
		if write != nil {
			if write.Kind == kvrpcpb.Mutation_Rollback {
				return nil, keyErrorTxnAlreadyRolledBack()
			}
			return nil, nil
		}
		return nil, keyErrorTxnLockLost()
	}
	if lock.Ts != startVersion {
		return nil, keyErrorLocked(key, lock)
	}
	return planCommitKeyWithLock(reader, key, lock, commitVersion)
}

func planCommitKeyWithLock(reader *Reader, key []byte, lock *mvcc.Lock, commitVersion uint64) ([]versionedOp, *kvrpcpb.KeyError) {
	committed, _, err := reader.GetWriteByStartTs(key, lock.Ts)
	if err != nil {
		return nil, keyErrorRetryable(err)
	}
	if committed != nil {
		if committed.Kind == kvrpcpb.Mutation_Rollback {
			return nil, keyErrorTxnAlreadyRolledBack()
		}
		return []versionedOp{{
			cf:      txnstore.CFLock,
			key:     key,
			version: lockColumnTs,
			meta:    txnstore.BitDelete,
		}}, nil
	}

	if lock.MinCommitTs > commitVersion {
		return nil, keyErrorCommitTsExpired(key, commitVersion, lock.MinCommitTs)
	}

	write := mvcc.Write{Kind: lock.Kind, StartTs: lock.Ts}
	if len(lock.ShortValue) > 0 {
		write.ShortValue = txnstore.SafeCopy(nil, lock.ShortValue)
		write.ExpiresAt = lock.ExpiresAt
		percolatorStats.shortValueCommitTotal.Add(1)
	}
	entry := mvcc.EncodeWrite(write)
	return []versionedOp{
		{cf: txnstore.CFWrite, key: key, version: commitVersion, value: entry},
		{cf: txnstore.CFLock, key: key, version: lockColumnTs, meta: txnstore.BitDelete},
	}, nil
}

func commitKey(db txnstore.Store, reader *Reader, key []byte, lock *mvcc.Lock, commitVersion uint64) *kvrpcpb.KeyError {
	ops, keyErr := planCommitKeyWithLock(reader, key, lock, commitVersion)
	if keyErr != nil {
		return keyErr
	}
	if err := applyVersionedOps(db, ops...); err != nil {
		return keyErrorRetryable(err)
	}
	return nil
}

func rollbackKey(db txnstore.Store, reader *Reader, key []byte, startTs uint64) *kvrpcpb.KeyError {
	ops, keyErr := planRollbackKey(reader, key, startTs)
	if keyErr != nil {
		return keyErr
	}
	if err := applyVersionedOps(db, ops...); err != nil {
		return keyErrorRetryable(err)
	}
	return nil
}

func planRollbackKey(reader *Reader, key []byte, startTs uint64) ([]versionedOp, *kvrpcpb.KeyError) {
	if len(key) == 0 {
		return nil, keyErrorAbort(errEmptyRollbackKey)
	}
	write, _, err := reader.GetWriteByStartTs(key, startTs)
	if err != nil {
		return nil, keyErrorRetryable(err)
	}
	if write != nil {
		return nil, nil
	}

	lock, err := reader.GetLock(key)
	if err != nil {
		return nil, keyErrorRetryable(err)
	}

	rollback := mvcc.EncodeWrite(mvcc.Write{Kind: kvrpcpb.Mutation_Rollback, StartTs: startTs})
	ops := make([]versionedOp, 0, 3)
	// A short-value prewrite never created a default-CF record, so rollback
	// only needs the rollback marker and lock tombstone for that case.
	if lock == nil || lock.Ts != startTs || len(lock.ShortValue) == 0 {
		ops = append(ops, versionedOp{cf: txnstore.CFDefault, key: key, version: startTs, meta: txnstore.BitDelete})
	}
	ops = append(ops, versionedOp{cf: txnstore.CFWrite, key: key, version: startTs, value: rollback})
	if lock != nil && lock.Ts == startTs {
		ops = append(ops, versionedOp{cf: txnstore.CFLock, key: key, version: lockColumnTs, meta: txnstore.BitDelete})
	}
	return ops, nil
}

type versionedOp struct {
	cf      txnstore.ColumnFamily
	key     []byte
	version uint64
	value   []byte
	meta    byte
	expires uint64
}

type twoPCApplyGroup struct {
	requests []twoPCApplyRequest
	entries  []*txnstore.Entry
	keys     map[string]struct{}
}

type twoPCApplyRequest struct {
	index         int
	resolvedLocks uint64
}

func (g *twoPCApplyGroup) conflicts(keys [][]byte) bool {
	if g == nil || len(g.keys) == 0 {
		return false
	}
	for _, key := range keys {
		if _, ok := g.keys[string(key)]; ok {
			return true
		}
	}
	return false
}

func (g *twoPCApplyGroup) add(index int, keys [][]byte, ops []versionedOp, resolvedLocks uint64) {
	if len(ops) == 0 {
		return
	}
	g.requests = append(g.requests, twoPCApplyRequest{index: index, resolvedLocks: resolvedLocks})
	g.entries = append(g.entries, versionedOpsToEntries(ops...)...)
	if g.keys == nil {
		g.keys = make(map[string]struct{}, len(keys))
	}
	for _, key := range keys {
		g.keys[string(key)] = struct{}{}
	}
}

func (g *twoPCApplyGroup) flush(db txnstore.Store, setResult func(twoPCApplyRequest, *kvrpcpb.KeyError)) {
	if g == nil || len(g.requests) == 0 {
		return
	}
	defer releaseEntries(g.entries)
	if len(g.requests) > 1 {
		percolatorStats.twoPCFusedBatchesTotal.Add(1)
		percolatorStats.twoPCFusedRequestsTotal.Add(uint64(len(g.requests)))
		percolatorStats.twoPCFusedEntriesTotal.Add(uint64(len(g.entries)))
	}
	var keyErr *kvrpcpb.KeyError
	if err := applyVersionedEntries(db, g.entries); err != nil {
		keyErr = keyErrorRetryable(err)
	}
	for _, req := range g.requests {
		setResult(req, keyErr)
	}
	g.requests = nil
	g.entries = nil
	g.keys = nil
}

func applyVersionedOps(db txnstore.Store, ops ...versionedOp) error {
	if len(ops) == 0 {
		return nil
	}
	entries := versionedOpsToEntries(ops...)
	// NoKV's DB regroups these internal entries by commit-pipeline shard before
	// they reach the raw storage backend. Percolator batches at the protocol phase
	// boundary; storage keeps the per-key placement invariant.
	defer releaseEntries(entries)
	return applyVersionedEntries(db, entries)
}

func versionedOpsToEntries(ops ...versionedOp) []*txnstore.Entry {
	entries := make([]*txnstore.Entry, 0, len(ops))
	for _, op := range ops {
		entries = append(entries, txnstore.NewInternalEntry(op.cf, op.key, op.version, op.value, op.meta, op.expires))
	}
	return entries
}

func applyVersionedEntries(db txnstore.Store, entries []*txnstore.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	return db.ApplyInternalEntries(entries)
}

func releaseEntries(entries []*txnstore.Entry) {
	for _, entry := range entries {
		if entry != nil {
			entry.DecrRef()
		}
	}
}

func currentPhysicalTimeMillis() uint64 {
	return uint64(time.Now().UnixMilli())
}

func isLockExpired(lock *mvcc.Lock, currentTime uint64) bool {
	if lock == nil {
		return false
	}
	if lock.TTL == 0 || lock.StartTime == 0 || currentTime == 0 {
		return false
	}
	return currentTime >= lock.StartTime && currentTime-lock.StartTime >= lock.TTL
}

func lockExpireTime(lock *mvcc.Lock) uint64 {
	if lock == nil || lock.StartTime == 0 || lock.TTL == 0 {
		return 0
	}
	if ^uint64(0)-lock.StartTime < lock.TTL {
		return ^uint64(0)
	}
	return lock.StartTime + lock.TTL
}
