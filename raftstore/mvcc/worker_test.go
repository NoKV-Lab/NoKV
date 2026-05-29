// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package mvcc_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	local "github.com/feichai0017/NoKV/local"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	storemvcc "github.com/feichai0017/NoKV/raftstore/mvcc"
	entrykv "github.com/feichai0017/NoKV/txn/storage"
	"github.com/stretchr/testify/require"
)

func TestMaintenanceWorkerRunOnceUsesReplicatedPaths(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	gcKey := []byte("vol/gc")
	applyMVCCGCPutVersion(t, db, gcKey, 150, 140, "new")
	applyMVCCGCPutVersion(t, db, gcKey, 90, 80, "anchor")
	applyMVCCGCPutVersion(t, db, gcKey, 40, 30, "old")
	lockKey := []byte("vol/lock")
	applyVersionedEntryForApplyTest(t, db, entrykv.CFDefault, lockKey, 10, []byte("value"), 0, 0)
	applyMVCCGCLockRecord(t, db, lockKey, lockKey, 10, 5, kvrpcpb.Mutation_Put)
	orphanKey := []byte("vol/orphan")
	applyVersionedEntryForApplyTest(t, db, entrykv.CFDefault, orphanKey, 7, []byte("orphan"), 0, 0)

	maintenance := &testMaintenanceProposer{db: db}
	locks := &testLockResolver{db: db}
	worker, ok := storemvcc.NewMaintenanceWorker(storemvcc.MaintenanceWorkerConfig{
		MVCCStore:           db,
		MaintenanceProposer: maintenance,
		LockResolver:        locks,
		Interval:            time.Hour,
		SafePoint:           func() uint64 { return 100 },
		CurrentTs:           func() uint64 { return 20 },
		CurrentTime:         func() uint64 { return 20 },
		Apply:               storemvcc.ApplyOptions{BatchEntries: 2},
		ResolveLocks:        storemvcc.ResolveLocksOptions{BatchLocks: 1},
		RunOrphanDefaults:   true,
		OrphanDefaults:      storemvcc.OrphanDefaultOptions{BatchEntries: 1},
	})
	require.True(t, ok)

	require.NoError(t, worker.RunOnce(context.Background()))
	snap := worker.Snapshot()
	require.Equal(t, uint64(1), snap.Runs)
	require.Empty(t, snap.LastError)
	require.Equal(t, uint64(1), snap.LastResolveLocks.ResolvedLocks)
	require.Equal(t, uint64(2), snap.LastApply.AppliedWriteDeletes)
	require.Equal(t, uint64(1), snap.LastApply.AppliedDefaultDeletes)
	require.Equal(t, uint64(1), snap.LastOrphanDefaults.AppliedDefaultDeletes)
	require.GreaterOrEqual(t, maintenance.calls, 2)
	require.Equal(t, 1, locks.statusCalls)
	require.Zero(t, locks.calls)
}

func TestMaintenanceWorkerContinuesAfterResolveError(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	gcKey := []byte("vol/resolve-error-gc")
	applyMVCCGCPutVersion(t, db, gcKey, 150, 140, "new")
	applyMVCCGCPutVersion(t, db, gcKey, 90, 80, "anchor")
	applyMVCCGCPutVersion(t, db, gcKey, 40, 30, "old")
	lockKey := []byte("vol/resolve-error-lock")
	applyVersionedEntryForApplyTest(t, db, entrykv.CFDefault, lockKey, 200, []byte("value"), 0, 0)
	applyMVCCGCLockRecord(t, db, lockKey, lockKey, 200, 5, kvrpcpb.Mutation_Put)

	worker, ok := storemvcc.NewMaintenanceWorker(storemvcc.MaintenanceWorkerConfig{
		MVCCStore:           db,
		MaintenanceProposer: &testMaintenanceProposer{db: db},
		LockResolver:        &failingLockResolver{err: errors.New("resolve failed")},
		Interval:            time.Hour,
		SafePoint:           func() uint64 { return 100 },
		CurrentTs:           func() uint64 { return 300 },
		CurrentTime:         func() uint64 { return 300 },
		Apply:               storemvcc.ApplyOptions{BatchEntries: 8},
		ResolveLocks:        storemvcc.ResolveLocksOptions{BatchLocks: 1},
	})
	require.True(t, ok)

	err := worker.RunOnce(context.Background())
	require.ErrorContains(t, err, "resolve failed")
	snap := worker.Snapshot()
	require.Contains(t, snap.LastResolveError, "resolve failed")
	require.Empty(t, snap.LastApplyError)
	require.Equal(t, uint64(1), snap.LastApply.AppliedWriteDeletes)
	require.Equal(t, uint64(1), snap.LastApply.AppliedDefaultDeletes)
}

func TestMaintenanceWorkerRunsOrphanCleanupAfterApplyError(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	gcKey := []byte("vol/apply-error-gc")
	applyMVCCGCPutVersion(t, db, gcKey, 150, 140, "new")
	applyMVCCGCPutVersion(t, db, gcKey, 90, 80, "anchor")
	applyMVCCGCPutVersion(t, db, gcKey, 40, 30, "old")
	orphanKey := []byte("vol/apply-error-orphan")
	applyVersionedEntryForApplyTest(t, db, entrykv.CFDefault, orphanKey, 7, []byte("orphan"), 0, 0)

	maintenance := &sequencedMaintenanceProposer{
		db:   db,
		errs: []error{errors.New("apply failed")},
	}
	worker, ok := storemvcc.NewMaintenanceWorker(storemvcc.MaintenanceWorkerConfig{
		MVCCStore:           db,
		MaintenanceProposer: maintenance,
		Interval:            time.Hour,
		SafePoint:           func() uint64 { return 100 },
		Apply:               storemvcc.ApplyOptions{BatchEntries: 8},
		RunOrphanDefaults:   true,
		OrphanDefaults:      storemvcc.OrphanDefaultOptions{BatchEntries: 8},
	})
	require.True(t, ok)

	err := worker.RunOnce(context.Background())
	require.ErrorContains(t, err, "apply failed")
	snap := worker.Snapshot()
	require.Contains(t, snap.LastApplyError, "apply failed")
	require.Empty(t, snap.LastOrphanError)
	require.Equal(t, uint64(1), snap.LastOrphanDefaults.AppliedDefaultDeletes)
	require.Equal(t, 2, maintenance.calls)
}

func TestMaintenanceWorkerReportsSafePointSkip(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	worker, ok := storemvcc.NewMaintenanceWorker(storemvcc.MaintenanceWorkerConfig{
		MVCCStore:           db,
		MaintenanceProposer: &testMaintenanceProposer{db: db},
		Interval:            time.Hour,
		SafePoint:           func() uint64 { return 0 },
	})
	require.True(t, ok)
	require.NoError(t, worker.RunOnce(context.Background()))

	snap := worker.Snapshot()
	require.True(t, snap.LastSafePointSkipped)
	require.Zero(t, snap.LastApply.ScannedKeys)
}

func TestMaintenanceWorkerDisabledWithoutReplicatedSubmitter(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	_, ok := storemvcc.NewMaintenanceWorker(storemvcc.MaintenanceWorkerConfig{
		MVCCStore: db,
		Interval:  time.Second,
		SafePoint: func() uint64 { return 100 },
	})
	require.False(t, ok)
}

func TestMaintenanceWorkerAllowsLockResolutionOnly(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	key := []byte("vol/lock-only")
	applyVersionedEntryForApplyTest(t, db, entrykv.CFDefault, key, 10, []byte("value"), 0, 0)
	applyMVCCGCLockRecord(t, db, key, key, 10, 5, kvrpcpb.Mutation_Put)

	worker, ok := storemvcc.NewMaintenanceWorker(storemvcc.MaintenanceWorkerConfig{
		MVCCStore:    db,
		LockResolver: &testLockResolver{db: db},
		Interval:     time.Hour,
		CurrentTs:    func() uint64 { return 20 },
		CurrentTime:  func() uint64 { return 20 },
		ResolveLocks: storemvcc.ResolveLocksOptions{BatchLocks: 1},
	})
	require.True(t, ok)
	require.NoError(t, worker.RunOnce(context.Background()))

	snap := worker.Snapshot()
	require.Equal(t, uint64(1), snap.LastResolveLocks.ResolvedLocks)
	require.Zero(t, snap.LastApply.AppliedWriteDeletes)
}

func TestMaintenanceWorkerCloseCancelsRunningPass(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	key := []byte("vol/cancel")
	applyMVCCGCPutVersion(t, db, key, 90, 80, "anchor")
	applyMVCCGCPutVersion(t, db, key, 40, 30, "old")

	proposer := &blockingMaintenanceProposer{entered: make(chan struct{})}
	worker, ok := storemvcc.NewMaintenanceWorker(storemvcc.MaintenanceWorkerConfig{
		MVCCStore:           db,
		MaintenanceProposer: proposer,
		Interval:            time.Millisecond,
		SafePoint:           func() uint64 { return 100 },
	})
	require.True(t, ok)

	worker.Start()
	require.Eventually(t, func() bool {
		select {
		case <-proposer.entered:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)

	done := make(chan struct{})
	go func() {
		worker.Close()
		close(done)
	}()
	require.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
}

func TestMaintenanceWorkerRestartConvergesAfterPartialPass(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	keyA := []byte("vol/restart-a")
	keyB := []byte("vol/restart-b")
	for _, key := range [][]byte{keyA, keyB} {
		applyMVCCGCPutVersion(t, db, key, 150, 140, "new")
		applyMVCCGCPutVersion(t, db, key, 90, 80, "anchor")
		applyMVCCGCPutVersion(t, db, key, 40, 30, "old")
	}

	firstWorker, ok := storemvcc.NewMaintenanceWorker(storemvcc.MaintenanceWorkerConfig{
		MVCCStore: db,
		MaintenanceProposer: &sequencedMaintenanceProposer{
			db:   db,
			errs: []error{nil, errors.New("node restarted during gc")},
		},
		Interval:  time.Hour,
		SafePoint: func() uint64 { return 100 },
		Apply:     storemvcc.ApplyOptions{BatchEntries: 2},
	})
	require.True(t, ok)
	err := firstWorker.RunOnce(context.Background())
	require.ErrorContains(t, err, "node restarted during gc")
	require.Equal(t, uint64(1), firstWorker.Snapshot().LastApply.AppliedWriteDeletes)

	secondWorker, ok := storemvcc.NewMaintenanceWorker(storemvcc.MaintenanceWorkerConfig{
		MVCCStore:           db,
		MaintenanceProposer: &testMaintenanceProposer{db: db},
		Interval:            time.Hour,
		SafePoint:           func() uint64 { return 100 },
		Apply:               storemvcc.ApplyOptions{BatchEntries: 2},
	})
	require.True(t, ok)
	require.NoError(t, secondWorker.RunOnce(context.Background()))

	assertWriteTombstoned(t, db, keyA, 40)
	assertWriteTombstoned(t, db, keyB, 40)
}

func assertWriteTombstoned(t *testing.T, db *local.DB, key []byte, commitTs uint64) {
	t.Helper()
	entry, err := db.GetInternalEntry(entrykv.CFWrite, key, commitTs)
	require.NoError(t, err)
	defer entry.DecrRef()
	require.NotZero(t, entry.Meta&entrykv.BitDelete)
}

type blockingMaintenanceProposer struct {
	entered chan struct{}
	once    sync.Once
}

func (p *blockingMaintenanceProposer) ProposeMVCCMaintenance(ctx context.Context, entries []*entrykv.Entry) (uint64, uint64, uint64, error) {
	if len(entries) > 0 {
		p.once.Do(func() {
			close(p.entered)
		})
	}
	<-ctx.Done()
	return 0, 0, 0, ctx.Err()
}

type failingLockResolver struct {
	err error
}

func (p *failingLockResolver) ResolveLocks(context.Context, uint64, uint64, [][]byte) (uint64, error) {
	return 0, p.err
}

func (p *failingLockResolver) CheckTxnStatus(context.Context, []byte, uint64, uint64, uint64) (*kvrpcpb.CheckTxnStatusResponse, error) {
	return &kvrpcpb.CheckTxnStatusResponse{CommitVersion: 301}, nil
}

type sequencedMaintenanceProposer struct {
	db    *local.DB
	mu    sync.Mutex
	calls int
	errs  []error
}

func (p *sequencedMaintenanceProposer) ProposeMVCCMaintenance(_ context.Context, entries []*entrykv.Entry) (uint64, uint64, uint64, error) {
	p.mu.Lock()
	call := p.calls
	p.calls++
	var err error
	if call < len(p.errs) {
		err = p.errs[call]
	}
	p.mu.Unlock()
	if err != nil {
		return 0, 0, 0, err
	}
	if p.db != nil {
		if err := p.db.ApplyInternalEntries(entries); err != nil {
			return 0, 0, 0, err
		}
	}
	writes, defaults := countTestMaintenanceEntries(entries)
	return uint64(len(entries)), writes, defaults, nil
}
