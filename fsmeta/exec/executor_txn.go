// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/fsmeta/backend"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
)

func (e *Executor) mutateWithAtomicOnePhase(ctx context.Context, kind model.OperationKind, primary []byte, predicates []*backend.Predicate, mutations []*backend.Mutation, startVersion, commitVersion uint64) error {
	stats := e.atomicOnePhaseCounters(kind)
	onePhase, ok := e.runner.(backend.ReadOrderedAtomicMutator)
	if !ok || !onePhase.AtomicMutatePreservesReadOrder() {
		if stats != nil {
			stats.runnerUnsupportedTotal.Add(1)
		}
		_, err := e.runner.Mutate(ctx, primary, mutations, startVersion, commitVersion, e.lockTTL)
		return err
	}
	affinity := atomicOnePhaseAffinity(primary, mutations)
	if stats != nil && !stats.allowAttempt(affinity) {
		stats.skipTotal.Add(1)
		_, err := e.runner.Mutate(ctx, primary, mutations, startVersion, commitVersion, e.lockTTL)
		return err
	}
	if stats != nil {
		stats.attemptTotal.Add(1)
	}
	handled, err := onePhase.TryAtomicMutate(ctx, primary, cloneAtomicPredicates(predicates), cloneMutations(mutations), startVersion, commitVersion)
	if err != nil || handled {
		if err == nil && stats != nil {
			stats.successTotal.Add(1)
			stats.recordSuccess(affinity)
		}
		return err
	}
	if stats != nil {
		stats.fallbackTotal.Add(1)
		stats.recordFallback(affinity)
	}
	_, err = e.runner.Mutate(ctx, primary, mutations, startVersion, commitVersion, e.lockTTL)
	return err
}

const (
	atomicOnePhaseBackoffAfter = 16
	atomicOnePhaseProbeEvery   = 128
)

func (s *atomicOnePhaseCounters) allowAttempt(affinity string) bool {
	if s == nil {
		return true
	}
	if s.affinityFallbacks(affinity) < atomicOnePhaseBackoffAfter {
		return true
	}
	// Some plans are only conditionally co-located. Back off after repeated
	// admission misses for the same placement pattern, but do not let unrelated
	// patterns force all operations of this kind back onto 2PC.
	return s.backoffSkipTotal.Add(1)%atomicOnePhaseProbeEvery == 0
}

func (s *atomicOnePhaseCounters) affinityFallbacks(affinity string) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fallbacksByAffinity[affinity]
}

func atomicOnePhaseAffinity(primary []byte, mutations []*backend.Mutation) string {
	const virtualShards = 64
	shards := make([]int, 0, 1+len(mutations))
	if len(primary) > 0 {
		shards = append(shards, layout.ShardForUserKey(primary, virtualShards))
	}
	for _, mutation := range mutations {
		if mutation == nil || len(mutation.Key) == 0 {
			continue
		}
		shards = append(shards, layout.ShardForUserKey(mutation.Key, virtualShards))
	}
	if len(shards) == 0 {
		return "empty"
	}
	sort.Ints(shards)
	out := make([]byte, 0, len(shards)*3)
	for i, shard := range shards {
		if i > 0 {
			out = append(out, ',')
		}
		out = fmt.Appendf(out, "%02d", shard)
	}
	return string(out)
}

func (e *Executor) mutateWithoutAtomicOnePhase(ctx context.Context, kind model.OperationKind, primary []byte, mutations []*backend.Mutation, startVersion, commitVersion uint64) error {
	if stats := e.atomicOnePhaseCounters(kind); stats != nil {
		stats.skipTotal.Add(1)
	}
	_, err := e.runner.Mutate(ctx, primary, mutations, startVersion, commitVersion, e.lockTTL)
	return err
}

func (e *Executor) atomicOnePhaseCounters(kind model.OperationKind) *atomicOnePhaseCounters {
	if e == nil {
		return nil
	}
	return e.atomicOnePhase[kind]
}

// GetReadVersion returns an ephemeral MVCC read version. It is intentionally
// cheaper than SnapshotSubtree: no root event is published and no GC-retention
// promise is made.
func (e *Executor) GetReadVersion(ctx context.Context, req model.ReadVersionRequest) (uint64, error) {
	if req.Mount == "" {
		return 0, model.ErrInvalidMountID
	}
	if err := e.requireActiveMount(ctx, req.Mount); err != nil {
		return 0, err
	}
	return e.reserveReadVersion(ctx)
}

func (e *Executor) reserveReadVersion(ctx context.Context) (uint64, error) {
	return e.reserveTimestampWithRetry(ctx, 1, maxReadContentionRetries, &e.readRetriesTotal, &e.readRetryExhaustedTotal)
}

func (e *Executor) readVersion(ctx context.Context, snapshotVersion uint64) (uint64, error) {
	if snapshotVersion != 0 {
		return snapshotVersion, nil
	}
	return e.reserveReadVersion(ctx)
}

// reserveTxnVersions reserves start_ts plus a speculative commit_ts in one TSO
// hop. AtomicMutate and in-memory runners use the speculative commit version.
// The real raftstore runner obtains commit_ts after prewrite for regular 2PC,
// which is the strict Percolator boundary under read/write contention.
//
// When a path does use the speculative commit_ts, two server-side safety nets
// keep pre-allocation from silently violating snapshot isolation:
//
//  1. When a concurrent reader at start_ts > our commit_ts encounters our
//     prewrite lock, it pushes lock.MinCommitTs = reader_start_ts + 1 via
//     CheckTxnStatus (see txn/percolator/txn.go: CallerStartTs handling).
//  2. commitKey rejects the commit with keyErrorCommitTsExpired when
//     lock.MinCommitTs > commitVersion (see txn/percolator/txn.go:373-375).
//
// Together these force a retry-with-fresh-ts under contention: incorrect
// speculative commit_ts is detected at commit time, never silently accepted.
// CommitTsExpired is retried transparently by withTxnRetry below.
func (e *Executor) reserveTxnVersions(ctx context.Context) (uint64, uint64, error) {
	startVersion, err := e.reserveTimestampWithRetry(ctx, 2, maxTxnContentionRetries, &e.txnRetriesTotal, &e.txnRetryExhaustedTotal)
	if err != nil {
		return 0, 0, err
	}
	return startVersion, startVersion + 1, nil
}

func (e *Executor) reserveTimestampWithRetry(ctx context.Context, count uint64, maxRetries int, retryTotal, exhaustedTotal *atomic.Uint64) (uint64, error) {
	var last error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		version, err := e.runner.ReserveTimestamp(ctx, count)
		if err == nil {
			return version, nil
		}
		if !nokverrors.Retryable(err) {
			return 0, err
		}
		last = err
		if attempt == maxRetries {
			if exhaustedTotal != nil {
				exhaustedTotal.Add(1)
			}
			break
		}
		if retryTotal != nil {
			retryTotal.Add(1)
		}
		// Coordinator duty handoff can reject a timestamp request with stale
		// evidence before any fsmeta mutation has started. Retrying here keeps
		// transient authority churn below the namespace API boundary.
		if err := waitTxnContentionRetryDelay(ctx, txnContentionRetryDelay(attempt)); err != nil {
			return 0, err
		}
	}
	return 0, last
}

func (e *Executor) withTxnRetry(ctx context.Context, run func(startVersion, commitVersion uint64) error, scopes ...compile.AuthorityScope) error {
	if err := e.drainVisibleAuthority(ctx, scopes...); err != nil {
		return err
	}
	return e.withTxnRetryNoVisibleFlush(ctx, run)
}

func (e *Executor) withTxnRetryNoVisibleFlush(ctx context.Context, run func(startVersion, commitVersion uint64) error) error {
	var last error
	started := time.Now()
	for attempt := 0; ; attempt++ {
		startVersion, commitVersion, err := e.reserveTxnVersions(ctx)
		if err != nil {
			return err
		}
		err = run(startVersion, commitVersion)
		if err == nil {
			return nil
		}
		if !isRetryableTxnAttempt(err) {
			return translateMutateError(err)
		}
		last = err
		if !canRetryTxnAttempt(attempt, started, err, e.lockTTL) {
			e.txnRetryExhaustedTotal.Add(1)
			break
		}
		// A live Percolator lock or a coordinator/region route refresh can race
		// with the same semantic fsmeta operation. Retrying at this boundary
		// keeps transient MVCC and route churn below the API contract.
		e.txnRetriesTotal.Add(1)
		delay := txnContentionRetryDelay(attempt)
		if budget := txnRetryBudget(err, e.lockTTL); budget > 0 {
			remaining := budget - time.Since(started)
			if remaining <= 0 {
				e.txnRetryExhaustedTotal.Add(1)
				break
			}
			delay = min(delay, remaining)
		}
		if err := waitTxnContentionRetryDelay(ctx, delay); err != nil {
			return err
		}
	}
	return translateMutateError(last)
}

func (e *Executor) withReadRetry(ctx context.Context, snapshotVersion uint64, run func(version uint64) error) error {
	var last error
	for attempt := 0; attempt <= maxReadContentionRetries; attempt++ {
		version, err := e.readVersion(ctx, snapshotVersion)
		if err != nil {
			return err
		}
		err = run(version)
		if err == nil {
			return nil
		}
		if !isRetryableReadAttempt(err) {
			return err
		}
		last = err
		if attempt == maxReadContentionRetries {
			e.readRetryExhaustedTotal.Add(1)
			break
		}
		// ReadDir and ReadDirPlus may race with live Percolator locks or region
		// route refresh. Retrying keeps the external API at the fsmeta level
		// instead of leaking transient storage details to callers.
		e.readRetriesTotal.Add(1)
		if err := waitTxnContentionRetryDelay(ctx, txnContentionRetryDelay(attempt)); err != nil {
			return err
		}
	}
	return last
}

func canRetryTxnAttempt(attempt int, started time.Time, err error, fallbackLockTTL uint64) bool {
	if budget := txnRetryBudget(err, fallbackLockTTL); budget > 0 {
		return time.Since(started) < budget
	}
	return attempt < maxTxnContentionRetries
}

func txnRetryBudget(err error, fallbackLockTTL uint64) time.Duration {
	switch {
	case nokverrors.IsKind(err, nokverrors.KindLockConflict):
	case nokverrors.IsKind(err, nokverrors.KindRetryable):
		// Percolator uses Retryable for a dead start_ts, for example when
		// commit finds that the prewrite lock was already rolled back. The
		// fsmeta semantic operation can safely re-read and re-plan, but under
		// raft/store congestion it needs the same bounded liveness window as a
		// visible live-lock wait.
	default:
		return 0
	}
	ttlMillis := txnLockTTLMillis(err)
	if ttlMillis == 0 {
		ttlMillis = fallbackLockTTL
	}
	if ttlMillis == 0 {
		return txnContentionRetryMaxBackoff
	}
	budget := time.Duration(ttlMillis) * time.Millisecond
	if budget <= 0 || budget > maxTxnLockRetryBudget {
		return maxTxnLockRetryBudget
	}
	return budget + txnContentionRetryMaxBackoff
}

func txnLockTTLMillis(err error) uint64 {
	var carrier nokverrors.KeyErrorCarrier
	if !errors.As(err, &carrier) {
		return 0
	}
	var maxTTL uint64
	for _, keyErr := range carrier.KeyErrors() {
		if ttl := keyErr.GetLocked().GetLockTtl(); ttl > maxTTL {
			maxTTL = ttl
		}
	}
	return maxTTL
}

func txnContentionRetryDelay(attempt int) time.Duration {
	if attempt <= 0 {
		return txnContentionRetryBaseBackoff
	}
	if attempt >= 7 {
		return txnContentionRetryMaxBackoff
	}
	return min(txnContentionRetryBaseBackoff<<attempt, txnContentionRetryMaxBackoff)
}

func waitTxnContentionRetryDelay(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func cloneBytes(in []byte) []byte {
	if in == nil {
		return nil
	}
	return append([]byte(nil), in...)
}

func cloneMutations(in []*backend.Mutation) []*backend.Mutation {
	out := make([]*backend.Mutation, 0, len(in))
	for _, mut := range in {
		if mut == nil {
			out = append(out, nil)
			continue
		}
		out = append(out, &backend.Mutation{
			Op:                mut.Op,
			Key:               cloneBytes(mut.Key),
			Value:             cloneBytes(mut.Value),
			AssertionNotExist: mut.AssertionNotExist,
			ExpiresAt:         mut.ExpiresAt,
		})
	}
	return out
}

func cloneAtomicPredicates(in []*backend.Predicate) []*backend.Predicate {
	out := make([]*backend.Predicate, 0, len(in))
	for _, pred := range in {
		if pred == nil {
			out = append(out, nil)
			continue
		}
		out = append(out, &backend.Predicate{
			Key:           cloneBytes(pred.Key),
			Kind:          pred.Kind,
			ReadVersion:   pred.ReadVersion,
			ExpectedValue: cloneBytes(pred.ExpectedValue),
		})
	}
	return out
}

func translateMutateError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, model.ErrExists) {
		return err
	}
	if nokverrors.HasKeyErrorKind(err, nokverrors.KindAlreadyExists) {
		return fmt.Errorf("%w: %v", model.ErrExists, err)
	}
	return err
}

func isRetryableTxnContention(err error) bool {
	return nokverrors.IsTxnContention(err)
}

func isRetryableTxnAttempt(err error) bool {
	return isRetryableTxnContention(err) || isRetryableRouteRefresh(err)
}

func isRetryableReadAttempt(err error) bool {
	return isRetryableTxnContention(err) || isRetryableRouteRefresh(err)
}

func isRetryableRouteRefresh(err error) bool {
	switch nokverrors.KindOf(err) {
	case nokverrors.KindRouteUnavailable,
		nokverrors.KindRegionRouting,
		nokverrors.KindStaleEpoch:
		return true
	default:
		return false
	}
}

func atomicNotExists(key []byte) *backend.Predicate {
	return &backend.Predicate{Key: cloneBytes(key), Kind: backend.PredicateNotExists}
}

func atomicValueEquals(key, value []byte) *backend.Predicate {
	return &backend.Predicate{
		Key:           cloneBytes(key),
		Kind:          backend.PredicateValueEquals,
		ExpectedValue: cloneBytes(value),
	}
}
