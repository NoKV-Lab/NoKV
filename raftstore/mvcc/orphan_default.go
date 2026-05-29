// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package mvcc

import (
	"bytes"
	"context"
	"fmt"
	txnstore "github.com/feichai0017/NoKV/txn/storage"
)

// OrphanDefaultOptions bounds one CFDefault orphan cleanup pass.
type OrphanDefaultOptions struct {
	// BatchEntries limits default tombstones submitted in one batch. A
	// non-positive value uses the normal apply batch default.
	BatchEntries int
}

func (o OrphanDefaultOptions) batchEntries() int {
	if o.BatchEntries <= 0 {
		return defaultApplyBatchEntries
	}
	return o.BatchEntries
}

// OrphanDefaultStats summarizes CFDefault records with no live CFWrite or
// CFLock owner.
type OrphanDefaultStats struct {
	ScannedDefaults       uint64
	DeletedDefaultMarkers uint64
	RetainedDefaults      uint64
	OrphanDefaults        uint64
	AppliedDefaultDeletes uint64
}

// ApplyOrphanDefaultsReplicated deletes orphan CFDefault records through a
// replicated maintenance command. Use this path for cluster-mode stores.
func ApplyOrphanDefaultsReplicated(ctx context.Context, db txnstore.Store, proposer MaintenanceProposer, opt OrphanDefaultOptions) (OrphanDefaultStats, error) {
	return applyOrphanDefaultsWith(ctx, db, opt, func(ctx context.Context, entries []*txnstore.Entry) (maintenanceSubmitResult, error) {
		return proposeMaintenanceEntries(ctx, proposer, entries)
	})
}

type orphanDefaultSubmitFn func(context.Context, []*txnstore.Entry) (maintenanceSubmitResult, error)

func applyOrphanDefaultsWith(ctx context.Context, db txnstore.Store, opt OrphanDefaultOptions, submit orphanDefaultSubmitFn) (OrphanDefaultStats, error) {
	var stats OrphanDefaultStats
	var afterUserKey []byte
	var afterVersion uint64
	for {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		batch, err := collectOrphanDefaultBatch(ctx, db, afterUserKey, afterVersion, opt.batchEntries())
		if err != nil {
			return stats, err
		}
		stats.add(batch.scan)
		applied, err := submit(ctx, batch.entries)
		stats.AppliedDefaultDeletes += applied.defaultDeletes
		if err != nil {
			releaseEntries(batch.entries)
			return stats, err
		}
		releaseEntries(batch.entries)
		if batch.done {
			return stats, nil
		}
		afterUserKey = batch.lastUserKey
		afterVersion = batch.lastVersion
	}
}

type orphanDefaultBatch struct {
	scan        OrphanDefaultStats
	entries     []*txnstore.Entry
	lastUserKey []byte
	lastVersion uint64
	done        bool
}

func collectOrphanDefaultBatch(ctx context.Context, db txnstore.Store, afterUserKey []byte, afterVersion uint64, maxEntries int) (orphanDefaultBatch, error) {
	var batch orphanDefaultBatch
	if db == nil {
		return batch, errNilMVCCStore
	}
	iter := db.NewInternalIterator(&txnstore.Options{IsAsc: true})
	if iter == nil {
		batch.done = true
		return batch, nil
	}
	defer func() { _ = iter.Close() }()

	seekDefaultStart(iter, afterUserKey, afterVersion)
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
		cf, userKey, startTs, ok := txnstore.SplitInternalKey(entry.Key)
		if !ok {
			return batch, fmt.Errorf("raftstore/mvcc: expected internal default key, got %x", entry.Key)
		}
		if cf != txnstore.CFDefault {
			batch.done = true
			return batch, nil
		}
		batch.lastUserKey = txnstore.SafeCopy(batch.lastUserKey, userKey)
		batch.lastVersion = startTs
		if entry.Meta&txnstore.BitDelete > 0 {
			batch.scan.DeletedDefaultMarkers++
			iter.Next()
			continue
		}
		batch.scan.ScannedDefaults++
		retained, err := defaultHasOwner(db, userKey, startTs)
		if err != nil {
			return batch, err
		}
		if retained {
			batch.scan.RetainedDefaults++
			iter.Next()
			continue
		}
		batch.scan.OrphanDefaults++
		batch.entries = append(batch.entries, txnstore.NewInternalEntry(txnstore.CFDefault, userKey, startTs, nil, txnstore.BitDelete, 0))
		iter.Next()
		if len(batch.entries) >= maxEntries {
			return batch, nil
		}
	}
	batch.done = true
	return batch, nil
}

func defaultHasOwner(db txnstore.Store, userKey []byte, startTs uint64) (bool, error) {
	if _, _, ok, err := writeByStartTs(db, userKey, startTs); err != nil {
		return false, err
	} else if ok {
		return true, nil
	}
	lock, err := lockForKey(db, userKey)
	if err != nil {
		return false, err
	}
	return lock != nil && lock.Ts == startTs, nil
}

func seekDefaultStart(iter txnstore.Iterator, afterUserKey []byte, afterVersion uint64) {
	if len(afterUserKey) == 0 {
		iter.Seek(txnstore.InternalKey(txnstore.CFDefault, nil, txnstore.MaxVersion))
		return
	}
	iter.Seek(txnstore.InternalKey(txnstore.CFDefault, afterUserKey, afterVersion))
	for iter.Valid() {
		item := iter.Item()
		if item == nil || item.Entry() == nil {
			iter.Next()
			continue
		}
		cf, userKey, version, ok := txnstore.SplitInternalKey(item.Entry().Key)
		if !ok || cf != txnstore.CFDefault || !bytes.Equal(userKey, afterUserKey) || version != afterVersion {
			return
		}
		iter.Next()
	}
}

func (s *OrphanDefaultStats) add(other OrphanDefaultStats) {
	s.ScannedDefaults += other.ScannedDefaults
	s.DeletedDefaultMarkers += other.DeletedDefaultMarkers
	s.RetainedDefaults += other.RetainedDefaults
	s.OrphanDefaults += other.OrphanDefaults
	s.AppliedDefaultDeletes += other.AppliedDefaultDeletes
}
