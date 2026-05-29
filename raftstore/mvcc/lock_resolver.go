// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package mvcc

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	txnmvcc "github.com/feichai0017/NoKV/txn/mvcc"
	txnstore "github.com/feichai0017/NoKV/txn/storage"
	"github.com/feichai0017/NoKV/utils"
)

const defaultResolveLockBatch = 4096

// ResolveLocksOptions bounds one local expired-lock resolution pass.
type ResolveLocksOptions struct {
	// CurrentTs is the logical timestamp used for CheckTxnStatus and
	// min-commit-ts pushes. Zero disables resolution.
	CurrentTs uint64
	// CurrentTime is the physical Unix millisecond time used for TTL checks.
	// Zero disables resolution.
	CurrentTime uint64
	// BatchLocks limits how many expired locks are resolved in one semantic
	// ResolveLock proposal batch. A non-positive value uses the default.
	BatchLocks int
	// MaxLocks stops one pass after scanning this many non-tombstone lock
	// records. Zero means unlimited.
	MaxLocks uint64
}

func (o ResolveLocksOptions) batchLocks() int {
	if o.BatchLocks <= 0 {
		return defaultResolveLockBatch
	}
	return o.BatchLocks
}

// ResolveLocksStats summarizes one expired-lock resolution pass.
type ResolveLocksStats struct {
	ScannedLocks       uint64
	DeletedLockMarkers uint64
	RetainedLocks      uint64
	ExpiredLocks       uint64
	ResolvedLocks      uint64
	CommittedLocks     uint64
	RolledBackLocks    uint64
}

// ResolveExpiredLocksReplicated resolves expired locks through primary-authority
// CheckTxnStatus and semantic ResolveLock commands. Use this path for
// cluster-mode stores so apply observers and watch streams see normal
// transaction-resolution events.
func ResolveExpiredLocksReplicated(ctx context.Context, db txnstore.Store, resolver LockResolver, opt ResolveLocksOptions) (ResolveLocksStats, error) {
	var stats ResolveLocksStats
	if opt.CurrentTs == 0 || opt.CurrentTime == 0 {
		return stats, nil
	}
	var afterUserKey []byte
	for {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		var maxLocks uint64
		if opt.MaxLocks > 0 {
			if stats.ScannedLocks >= opt.MaxLocks {
				return stats, nil
			}
			maxLocks = opt.MaxLocks - stats.ScannedLocks
		}
		batch, err := collectResolveLockBatch(ctx, db, opt.CurrentTime, afterUserKey, opt.batchLocks(), maxLocks)
		if err != nil {
			return stats, err
		}
		stats.add(batch.scan)
		if len(batch.locks) > 0 {
			decisions, resolved, err := planResolveLocks(ctx, resolver, opt.CurrentTs, opt.CurrentTime, batch.locks)
			if err != nil {
				return stats, err
			}
			commands := groupResolveLockCommands(decisions)
			for _, cmd := range commands {
				applied, err := proposeResolveLocks(ctx, resolver, cmd.startTs, cmd.commitTs, cmd.keys)
				if err != nil {
					return stats, err
				}
				resolved.ResolvedLocks += applied
				if cmd.commitTs == 0 {
					resolved.RolledBackLocks += applied
				} else {
					resolved.CommittedLocks += applied
				}
			}
			stats.add(resolved)
		}
		if batch.done {
			return stats, nil
		}
		if opt.MaxLocks > 0 && stats.ScannedLocks >= opt.MaxLocks {
			return stats, nil
		}
		afterUserKey = batch.lastUserKey
	}
}

type lockRecord struct {
	key  []byte
	lock txnmvcc.Lock
}

type resolveLockBatch struct {
	scan        ResolveLocksStats
	locks       []lockRecord
	lastUserKey []byte
	done        bool
}

func collectResolveLockBatch(ctx context.Context, db txnstore.Store, currentTime uint64, afterUserKey []byte, maxExpiredLocks int, maxLocks uint64) (resolveLockBatch, error) {
	var batch resolveLockBatch
	if db == nil {
		return batch, errNilMVCCStore
	}
	iter := db.NewInternalIterator(&txnstore.Options{IsAsc: true})
	if iter == nil {
		batch.done = true
		return batch, nil
	}
	defer func() { _ = iter.Close() }()

	seekLockStart(iter, afterUserKey)
	for iter.Valid() {
		if err := ctx.Err(); err != nil {
			return batch, err
		}
		item := iter.Item()
		if item == nil || item.Entry() == nil {
			iter.Next()
			continue
		}
		entry := item.Entry()
		cf, userKey, _, ok := txnstore.SplitInternalKey(entry.Key)
		if !ok {
			return batch, fmt.Errorf("raftstore/mvcc: expected internal lock key, got %x", entry.Key)
		}
		if cf != txnstore.CFLock {
			batch.done = true
			return batch, nil
		}
		if maxLocks > 0 && batch.scan.ScannedLocks >= maxLocks {
			return batch, nil
		}
		batch.lastUserKey = txnstore.SafeCopy(batch.lastUserKey, userKey)
		if entry.Meta&txnstore.BitDelete > 0 {
			batch.scan.DeletedLockMarkers++
			iter.Next()
			continue
		}
		lock, err := txnmvcc.DecodeLock(entry.Value)
		if err != nil {
			return batch, errDecodeCFLock(userKey, err)
		}
		batch.scan.ScannedLocks++
		if !lockExpired(lock, currentTime) {
			batch.scan.RetainedLocks++
			iter.Next()
			continue
		}
		batch.scan.ExpiredLocks++
		batch.locks = append(batch.locks, lockRecord{
			key:  txnstore.SafeCopy(nil, userKey),
			lock: lock,
		})
		iter.Next()
		if len(batch.locks) >= maxExpiredLocks {
			return batch, nil
		}
	}
	batch.done = true
	return batch, nil
}

func seekLockStart(iter txnstore.Iterator, afterUserKey []byte) {
	if len(afterUserKey) == 0 {
		iter.Seek(txnstore.InternalKey(txnstore.CFLock, nil, txnstore.MaxVersion))
		return
	}
	iter.Seek(txnstore.InternalKey(txnstore.CFLock, afterUserKey, 0))
	for iter.Valid() {
		item := iter.Item()
		if item == nil || item.Entry() == nil {
			iter.Next()
			continue
		}
		cf, userKey, _, ok := txnstore.SplitInternalKey(item.Entry().Key)
		if !ok || cf != txnstore.CFLock || !bytes.Equal(userKey, afterUserKey) {
			return
		}
		iter.Next()
	}
}

type resolveLockDecision struct {
	key             []byte
	startTs         uint64
	commitTs        uint64
	alreadyResolved bool
}

type resolveLockCommand struct {
	startTs  uint64
	commitTs uint64
	keys     [][]byte
}

func planResolveLocks(ctx context.Context, resolver LockResolver, currentTs, currentTime uint64, locks []lockRecord) ([]resolveLockDecision, ResolveLocksStats, error) {
	var stats ResolveLocksStats
	decisions := make([]resolveLockDecision, 0, len(locks))
	for _, rec := range locks {
		if err := ctx.Err(); err != nil {
			return nil, stats, err
		}
		decision, err := resolveOneLock(ctx, resolver, currentTs, currentTime, rec)
		if err != nil {
			return nil, stats, err
		}
		if decision == nil {
			stats.RetainedLocks++
			continue
		}
		if decision.alreadyResolved {
			stats.ResolvedLocks++
			if decision.commitTs == 0 {
				stats.RolledBackLocks++
			} else {
				stats.CommittedLocks++
			}
			continue
		}
		decisions = append(decisions, *decision)
	}
	return decisions, stats, nil
}

func resolveOneLock(ctx context.Context, resolver LockResolver, currentTs, currentTime uint64, rec lockRecord) (*resolveLockDecision, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	status, err := checkTxnStatus(ctx, resolver, rec.lock.Primary, rec.lock.Ts, currentTs, currentTime)
	if err != nil {
		return nil, err
	}
	if status == nil {
		return nil, fmt.Errorf("%w for primary %x", errNilCheckTxnStatusResult, rec.lock.Primary)
	}
	if keyErr := status.GetError(); keyErr != nil {
		return nil, errCheckTxnStatusKeyError(rec.lock.Primary, keyErr)
	}
	if commitTs := status.GetCommitVersion(); commitTs > 0 {
		return &resolveLockDecision{
			key:      txnstore.SafeCopy(nil, rec.key),
			startTs:  rec.lock.Ts,
			commitTs: commitTs,
		}, nil
	}
	switch status.GetAction() {
	case kvrpcpb.CheckTxnStatusAction_CheckTxnStatusTTLExpireRollback,
		kvrpcpb.CheckTxnStatusAction_CheckTxnStatusLockNotExistRollback:
		decision := &resolveLockDecision{
			key:     txnstore.SafeCopy(nil, rec.key),
			startTs: rec.lock.Ts,
		}
		if bytes.Equal(rec.key, rec.lock.Primary) {
			decision.alreadyResolved = true
		}
		return decision, nil
	default:
		return nil, nil
	}
}

func lockForKey(db txnstore.Store, key []byte) (*txnmvcc.Lock, error) {
	entry, err := db.GetInternalEntry(txnstore.CFLock, key, txnstore.MaxVersion)
	if err != nil {
		if errors.Is(err, utils.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, err
	}
	if entry == nil {
		return nil, nil
	}
	defer entry.DecrRef()
	if entry.Meta&txnstore.BitDelete > 0 || entry.Value == nil {
		return nil, nil
	}
	lock, err := txnmvcc.DecodeLock(entry.Value)
	if err != nil {
		return nil, errDecodeCFLock(key, err)
	}
	return &lock, nil
}

func writeByStartTs(db txnstore.Store, key []byte, startTs uint64) (txnmvcc.Write, uint64, bool, error) {
	iter := db.NewInternalIterator(&txnstore.Options{IsAsc: true})
	if iter == nil {
		return txnmvcc.Write{}, 0, false, nil
	}
	defer func() { _ = iter.Close() }()

	iter.Seek(txnstore.InternalKey(txnstore.CFWrite, key, txnstore.MaxVersion))
	for iter.Valid() {
		item := iter.Item()
		if item == nil || item.Entry() == nil {
			iter.Next()
			continue
		}
		entry := item.Entry()
		cf, userKey, commitTs, ok := txnstore.SplitInternalKey(entry.Key)
		if !ok {
			return txnmvcc.Write{}, 0, false, fmt.Errorf("raftstore/mvcc: expected internal write key, got %x", entry.Key)
		}
		if cf != txnstore.CFWrite || !bytes.Equal(userKey, key) {
			break
		}
		if entry.Meta&txnstore.BitDelete > 0 {
			iter.Next()
			continue
		}
		write, err := txnmvcc.DecodeWrite(entry.Value)
		if err != nil {
			return txnmvcc.Write{}, 0, false, fmt.Errorf("raftstore/mvcc: decode CFWrite %x@%d: %w", userKey, commitTs, err)
		}
		if write.StartTs == startTs {
			return write, commitTs, true, nil
		}
		if commitTs < startTs {
			break
		}
		iter.Next()
	}
	return txnmvcc.Write{}, 0, false, nil
}

func lockExpired(lock txnmvcc.Lock, currentTime uint64) bool {
	if lock.TTL == 0 || lock.StartTime == 0 || currentTime == 0 {
		return false
	}
	return currentTime >= lock.StartTime && currentTime-lock.StartTime >= lock.TTL
}

func groupResolveLockCommands(decisions []resolveLockDecision) []resolveLockCommand {
	type key struct {
		startTs  uint64
		commitTs uint64
	}
	groups := make([]resolveLockCommand, 0, len(decisions))
	index := make(map[key]int, len(decisions))
	for _, decision := range decisions {
		k := key{startTs: decision.startTs, commitTs: decision.commitTs}
		idx, ok := index[k]
		if !ok {
			idx = len(groups)
			index[k] = idx
			groups = append(groups, resolveLockCommand{startTs: decision.startTs, commitTs: decision.commitTs})
		}
		groups[idx].keys = append(groups[idx].keys, txnstore.SafeCopy(nil, decision.key))
	}
	return groups
}

func (s *ResolveLocksStats) add(other ResolveLocksStats) {
	s.ScannedLocks += other.ScannedLocks
	s.DeletedLockMarkers += other.DeletedLockMarkers
	s.RetainedLocks += other.RetainedLocks
	s.ExpiredLocks += other.ExpiredLocks
	s.ResolvedLocks += other.ResolvedLocks
	s.CommittedLocks += other.CommittedLocks
	s.RolledBackLocks += other.RolledBackLocks
}
