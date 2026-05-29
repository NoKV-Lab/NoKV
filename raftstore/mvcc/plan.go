// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package mvcc

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	txnmvcc "github.com/feichai0017/NoKV/txn/mvcc"
	txnstore "github.com/feichai0017/NoKV/txn/storage"
)

const defaultApplyBatchEntries = 4096

// PlanStats summarizes a non-destructive MVCC GC planning pass.
type PlanStats struct {
	ScannedKeys           uint64
	DroppableKeys         uint64
	WriteVersions         uint64
	RetainedWrites        uint64
	DroppableWrites       uint64
	AnchorWrites          uint64
	RetainedDefaultRefs   uint64
	DeletedWriteMarkers   uint64
	SafePointClampedKeys  uint64
	MaxVersionsPerKey     uint64
	MinEffectiveSafePoint uint64
	MaxEffectiveSafePoint uint64
}

// ApplyStats summarizes a destructive MVCC GC pass. The embedded plan counters
// describe what the scanner decided; Applied* counters describe tombstones
// acknowledged by replicated maintenance proposals.
type ApplyStats struct {
	PlanStats
	AppliedWriteDeletes   uint64
	AppliedDefaultDeletes uint64
}

// ApplyOptions configures a destructive MVCC GC pass.
type ApplyOptions struct {
	// BatchEntries limits the number of tombstones submitted in one replicated
	// maintenance proposal batch. A non-positive value uses the default.
	BatchEntries int
	// MaxKeys stops one destructive pass after scanning this many user keys.
	// Zero means unlimited.
	MaxKeys uint64
}

func (o ApplyOptions) batchEntries() int {
	if o.BatchEntries <= 0 {
		return defaultApplyBatchEntries
	}
	return o.BatchEntries
}

// Plan scans CFWrite and applies MVCC GC policy without deleting data.
func Plan(ctx context.Context, db txnstore.Store, policy SafePointPolicy) (PlanStats, error) {
	var stats PlanStats
	_, err := walk(ctx, db, policy, nil, &stats, nil)
	return stats, err
}

// ApplyReplicated applies MVCC GC through a MaintenanceProposer. Each tombstone
// batch is submitted as a replicated raft command; there is intentionally no
// local direct-apply path for production GC.
func ApplyReplicated(ctx context.Context, db txnstore.Store, proposer MaintenanceProposer, policy SafePointPolicy, opt ApplyOptions) (ApplyStats, error) {
	return applyWith(ctx, db, policy, opt, func(ctx context.Context, entries []*txnstore.Entry) (maintenanceSubmitResult, error) {
		return proposeMaintenanceEntries(ctx, proposer, entries)
	})
}

type applySubmitFn func(context.Context, []*txnstore.Entry) (maintenanceSubmitResult, error)

func applyWith(ctx context.Context, db txnstore.Store, policy SafePointPolicy, opt ApplyOptions, submit applySubmitFn) (ApplyStats, error) {
	var stats ApplyStats
	var afterUserKey []byte
	for {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		var maxKeys uint64
		if opt.MaxKeys > 0 {
			if stats.ScannedKeys >= opt.MaxKeys {
				return stats, nil
			}
			maxKeys = opt.MaxKeys - stats.ScannedKeys
		}
		batch, err := collectApplyBatch(ctx, db, policy, afterUserKey, opt.batchEntries(), maxKeys)
		if err != nil {
			return stats, err
		}
		stats.add(batch.plan)
		applied, err := submit(ctx, batch.entries)
		stats.AppliedWriteDeletes += applied.writeDeletes
		stats.AppliedDefaultDeletes += applied.defaultDeletes
		if err != nil {
			releaseEntries(batch.entries)
			return stats, err
		}
		releaseEntries(batch.entries)
		if batch.done {
			return stats, nil
		}
		if opt.MaxKeys > 0 && stats.ScannedKeys >= opt.MaxKeys {
			return stats, nil
		}
		afterUserKey = batch.lastUserKey
	}
}

type groupFn func(userKey []byte, decisions []txnmvcc.GCWriteDecision) error

type walkResult struct {
	lastUserKey []byte
	stopped     bool
}

func walk(ctx context.Context, db txnstore.Store, policy SafePointPolicy, afterUserKey []byte, stats *PlanStats, fn groupFn) (walkResult, error) {
	var result walkResult
	if db == nil {
		return result, errNilMVCCStore
	}
	iter := db.NewInternalIterator(&txnstore.Options{IsAsc: true})
	if iter == nil {
		return result, nil
	}
	defer func() { _ = iter.Close() }()

	var (
		currentKey []byte
		versions   []txnmvcc.GCWriteVersion
		decisions  []txnmvcc.GCWriteDecision
	)
	flush := func() error {
		if len(currentKey) == 0 {
			return nil
		}
		result.lastUserKey = txnstore.SafeCopy(result.lastUserKey, currentKey)
		var safePoint uint64
		safePoint, decisions = policy.AppendPlanWritesForKey(decisions[:0], currentKey, versions)
		stats.recordGroup(policy.RequestedSafePoint, safePoint, decisions)
		if fn != nil {
			if err := fn(currentKey, decisions); err != nil {
				if errors.Is(err, errStop) {
					result.stopped = true
					currentKey = nil
					versions = versions[:0]
					return nil
				}
				return err
			}
		}
		currentKey = nil
		versions = versions[:0]
		return nil
	}

	seekStart(iter, afterUserKey)
	for iter.Valid() {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		item := iter.Item()
		if item == nil || item.Entry() == nil {
			iter.Next()
			continue
		}
		entry := item.Entry()
		cf, userKey, commitTs, ok := txnstore.SplitInternalKey(entry.Key)
		if !ok {
			return result, fmt.Errorf("raftstore/mvcc: expected internal key, got %x", entry.Key)
		}
		if cf != txnstore.CFWrite {
			break
		}
		if len(currentKey) > 0 && !bytes.Equal(currentKey, userKey) {
			if err := flush(); err != nil {
				return result, err
			}
			if result.stopped {
				return result, nil
			}
		}
		if len(currentKey) == 0 {
			currentKey = txnstore.SafeCopy(nil, userKey)
		}
		if entry.Meta&txnstore.BitDelete > 0 {
			stats.DeletedWriteMarkers++
			iter.Next()
			continue
		}
		write, err := txnmvcc.DecodeWrite(entry.Value)
		if err != nil {
			return result, fmt.Errorf("raftstore/mvcc: decode CFWrite %x@%d: %w", userKey, commitTs, err)
		}
		versions = append(versions, txnmvcc.GCWriteVersion{CommitTs: commitTs, Write: write})
		if uint64(len(versions)) > policy.maxVersionsPerKey() {
			return result, fmt.Errorf("raftstore/mvcc: key %x has more than %d buffered write versions; split the pass or raise MaxVersionsPerKey", currentKey, policy.maxVersionsPerKey())
		}
		iter.Next()
	}
	return result, flush()
}

func seekStart(iter txnstore.Iterator, afterUserKey []byte) {
	if len(afterUserKey) == 0 {
		iter.Seek(txnstore.InternalKey(txnstore.CFWrite, nil, txnstore.MaxVersion))
		return
	}
	iter.Seek(txnstore.InternalKey(txnstore.CFWrite, afterUserKey, 0))
	for iter.Valid() {
		item := iter.Item()
		if item == nil || item.Entry() == nil {
			iter.Next()
			continue
		}
		cf, userKey, _, ok := txnstore.SplitInternalKey(item.Entry().Key)
		if !ok || cf != txnstore.CFWrite || !bytes.Equal(userKey, afterUserKey) {
			return
		}
		iter.Next()
	}
}

type applyBatch struct {
	plan        PlanStats
	entries     []*txnstore.Entry
	lastUserKey []byte
	done        bool
}

func collectApplyBatch(ctx context.Context, db txnstore.Store, policy SafePointPolicy, afterUserKey []byte, maxEntries int, maxKeys uint64) (applyBatch, error) {
	var batch applyBatch
	result, err := walk(ctx, db, policy, afterUserKey, &batch.plan, func(userKey []byte, decisions []txnmvcc.GCWriteDecision) error {
		writes, defaults := buildDeletes(userKey, decisions)
		batch.entries = append(batch.entries, writes...)
		batch.entries = append(batch.entries, defaults...)
		if maxKeys > 0 && batch.plan.ScannedKeys >= maxKeys {
			return errStop
		}
		if len(batch.entries) >= maxEntries {
			return errStop
		}
		return nil
	})
	if err != nil {
		releaseEntries(batch.entries)
		return applyBatch{}, err
	}
	batch.lastUserKey = result.lastUserKey
	batch.done = !result.stopped
	return batch, nil
}

func buildDeletes(userKey []byte, decisions []txnmvcc.GCWriteDecision) (writes, defaults []*txnstore.Entry) {
	retainedDefault := make(map[uint64]struct{})
	for _, decision := range decisions {
		if decision.RetainDefaultStartTs != 0 {
			retainedDefault[decision.RetainDefaultStartTs] = struct{}{}
		}
	}
	for _, decision := range decisions {
		if decision.Keep {
			continue
		}
		writes = append(writes, txnstore.NewInternalEntry(txnstore.CFWrite, userKey, decision.CommitTs, nil, txnstore.BitDelete, 0))
		if !txnmvcc.WriteNeedsDefaultRecord(decision.Write) {
			continue
		}
		if _, ok := retainedDefault[decision.Write.StartTs]; ok {
			continue
		}
		defaults = append(defaults, txnstore.NewInternalEntry(txnstore.CFDefault, userKey, decision.Write.StartTs, nil, txnstore.BitDelete, 0))
	}
	return writes, defaults
}

func (s *PlanStats) recordGroup(requestedSafePoint, safePoint uint64, plan []txnmvcc.GCWriteDecision) {
	s.ScannedKeys++
	if safePoint != 0 && safePoint < requestedSafePoint {
		s.SafePointClampedKeys++
	}
	if versions := uint64(len(plan)); versions > s.MaxVersionsPerKey {
		s.MaxVersionsPerKey = versions
	}
	if safePoint != 0 && (s.MinEffectiveSafePoint == 0 || safePoint < s.MinEffectiveSafePoint) {
		s.MinEffectiveSafePoint = safePoint
	}
	if safePoint > s.MaxEffectiveSafePoint {
		s.MaxEffectiveSafePoint = safePoint
	}
	hasDrop := false
	for _, decision := range plan {
		s.WriteVersions++
		if decision.Keep {
			s.RetainedWrites++
		} else {
			s.DroppableWrites++
			hasDrop = true
		}
		if decision.Anchor {
			s.AnchorWrites++
		}
		if decision.RetainDefaultStartTs != 0 {
			s.RetainedDefaultRefs++
		}
	}
	if hasDrop {
		s.DroppableKeys++
	}
}

func (s *ApplyStats) add(other PlanStats) {
	s.ScannedKeys += other.ScannedKeys
	s.DroppableKeys += other.DroppableKeys
	s.WriteVersions += other.WriteVersions
	s.RetainedWrites += other.RetainedWrites
	s.DroppableWrites += other.DroppableWrites
	s.AnchorWrites += other.AnchorWrites
	s.RetainedDefaultRefs += other.RetainedDefaultRefs
	s.DeletedWriteMarkers += other.DeletedWriteMarkers
	s.SafePointClampedKeys += other.SafePointClampedKeys
	if other.MaxVersionsPerKey > s.MaxVersionsPerKey {
		s.MaxVersionsPerKey = other.MaxVersionsPerKey
	}
	if other.MinEffectiveSafePoint != 0 && (s.MinEffectiveSafePoint == 0 || other.MinEffectiveSafePoint < s.MinEffectiveSafePoint) {
		s.MinEffectiveSafePoint = other.MinEffectiveSafePoint
	}
	if other.MaxEffectiveSafePoint > s.MaxEffectiveSafePoint {
		s.MaxEffectiveSafePoint = other.MaxEffectiveSafePoint
	}
}

func releaseEntries(entries []*txnstore.Entry) {
	for _, entry := range entries {
		if entry != nil {
			entry.DecrRef()
		}
	}
}
