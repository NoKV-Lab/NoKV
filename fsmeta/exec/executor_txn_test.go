// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"context"
	"fmt"
	"testing"
	"time"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/fsmeta/model"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/stretchr/testify/require"
)

func TestExecutorGetReadVersionReservesEphemeralTimestamp(t *testing.T) {
	runner := newFakeRunner()
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	version, err := executor.GetReadVersion(context.Background(), model.ReadVersionRequest{Mount: "vol"})
	require.NoError(t, err)
	require.Equal(t, uint64(1), version)
	require.Equal(t, uint64(2), runner.nextTS)
}

func TestExecutorRetriesTimestampAuthorityRefreshBeforeMutate(t *testing.T) {
	runner := newFakeRunner()
	runner.timestampErrs = []error{nokverrors.New(nokverrors.KindStaleEpoch, "coordinator client: stale witness era")}
	executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{22}}))
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	})

	require.NoError(t, err)
	require.Empty(t, runner.timestampErrs)
	require.Len(t, runner.mutations, 1)
	requireStatUint(t, executor.Stats(), "txn_retries_total", 1)
}

func TestExecutorRetriesReadTimestampAuthorityRefresh(t *testing.T) {
	runner := newFakeRunner()
	runner.timestampErrs = []error{nokverrors.New(nokverrors.KindStaleEpoch, "coordinator client: stale witness era")}
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	version, err := executor.GetReadVersion(context.Background(), model.ReadVersionRequest{Mount: "vol"})

	require.NoError(t, err)
	require.NotZero(t, version)
	require.Empty(t, runner.timestampErrs)
	requireStatUint(t, executor.Stats(), "read_retries_total", 1)
}

func TestExecutorRetriesLostTxnLock(t *testing.T) {
	runner := newFakeRunner()
	runner.mutateErrs = []error{
		fakeTxnKeyError{errors: []*kvrpcpb.KeyError{{
			Retryable: "percolator: lock not found",
		}}},
		nil,
	}
	allocator := &fakeInodeAllocator{ids: []model.InodeID{22}}
	executor, err := newTestExecutor(runner, WithInodeAllocator(allocator))
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	})
	require.NoError(t, err)
	require.Len(t, runner.mutations, 1)
	require.Equal(t, 1, allocator.calls)
	require.Equal(t, uint64(1), executor.Stats()["txn_retries_total"])
	require.Equal(t, uint64(0), executor.Stats()["txn_retry_exhausted_total"])
}

func TestExecutorRetriesCommitTsExpired(t *testing.T) {
	runner := newFakeRunner()
	runner.mutateErrs = []error{
		fakeTxnKeyError{errors: []*kvrpcpb.KeyError{{
			CommitTsExpired: &kvrpcpb.CommitTsExpired{
				Key:         []byte("dentry"),
				CommitTs:    2,
				MinCommitTs: 5,
			},
		}}},
		nil,
	}
	allocator := &fakeInodeAllocator{ids: []model.InodeID{22}}
	executor, err := newTestExecutor(runner, WithInodeAllocator(allocator))
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	})
	require.NoError(t, err)
	require.Len(t, runner.mutations, 1)
	require.Equal(t, 1, allocator.calls)
	require.Equal(t, uint64(5), runner.nextTS)
	require.Equal(t, uint64(1), executor.Stats()["txn_retries_total"])
	require.Equal(t, uint64(0), executor.Stats()["txn_retry_exhausted_total"])
}

func TestExecutorRetriesRouteUnavailable(t *testing.T) {
	runner := newFakeRunner()
	runner.mutateErrs = []error{
		nokverrors.New(nokverrors.KindRouteUnavailable, "route lookup refreshing"),
		nil,
	}
	allocator := &fakeInodeAllocator{ids: []model.InodeID{22}}
	executor, err := newTestExecutor(runner, WithInodeAllocator(allocator))
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	})
	require.NoError(t, err)
	require.Len(t, runner.mutations, 1)
	require.Equal(t, 1, allocator.calls)
	require.Equal(t, uint64(5), runner.nextTS)
	require.Equal(t, uint64(1), executor.Stats()["txn_retries_total"])
	require.Equal(t, uint64(0), executor.Stats()["txn_retry_exhausted_total"])
}

func TestExecutorRetriesWriteConflict(t *testing.T) {
	runner := newFakeRunner()
	runner.mutateErrs = []error{
		fakeTxnKeyError{errors: []*kvrpcpb.KeyError{{
			WriteConflict: &kvrpcpb.WriteConflict{
				Key:        []byte("dentry"),
				ConflictTs: 4,
				StartTs:    2,
			},
		}}},
		nil,
	}
	allocator := &fakeInodeAllocator{ids: []model.InodeID{22}}
	executor, err := newTestExecutor(runner, WithInodeAllocator(allocator))
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	})
	require.NoError(t, err)
	require.Len(t, runner.mutations, 1)
	require.Equal(t, 1, allocator.calls)
	require.Equal(t, uint64(1), executor.Stats()["txn_retries_total"])
	require.Equal(t, uint64(0), executor.Stats()["txn_retry_exhausted_total"])
}

func TestExecutorRetriesLockedTxnContention(t *testing.T) {
	runner := newFakeRunner()
	runner.mutateErrs = []error{
		fakeTxnKeyError{errors: []*kvrpcpb.KeyError{{
			Locked: &kvrpcpb.Locked{
				PrimaryLock: []byte("dentry"),
				Key:         []byte("dentry"),
				LockVersion: 2,
				LockTtl:     defaultLockTTL,
			},
		}}},
		nil,
	}
	allocator := &fakeInodeAllocator{ids: []model.InodeID{22}}
	executor, err := newTestExecutor(runner, WithInodeAllocator(allocator))
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	})
	require.NoError(t, err)
	require.Len(t, runner.mutations, 1)
	require.Equal(t, 1, allocator.calls)
	require.Equal(t, uint64(5), runner.nextTS)
	require.Equal(t, uint64(1), executor.Stats()["txn_retries_total"])
	require.Equal(t, uint64(0), executor.Stats()["txn_retry_exhausted_total"])
}

func TestExecutorRetriesSustainedLiveTxnContention(t *testing.T) {
	runner := newFakeRunner()
	for range 9 {
		runner.mutateErrs = append(runner.mutateErrs, fakeTxnKeyError{errors: []*kvrpcpb.KeyError{{
			Locked: &kvrpcpb.Locked{
				PrimaryLock: []byte("dentry"),
				Key:         []byte("dentry"),
				LockVersion: 2,
				LockTtl:     defaultLockTTL,
			},
		}}})
	}
	runner.mutateErrs = append(runner.mutateErrs, nil)
	allocator := &fakeInodeAllocator{ids: []model.InodeID{22}}
	executor, err := newTestExecutor(runner, WithInodeAllocator(allocator))
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	})
	require.NoError(t, err)
	require.Len(t, runner.mutations, 1)
	require.Equal(t, 1, allocator.calls)
	require.Equal(t, uint64(21), runner.nextTS)
	require.Equal(t, uint64(9), executor.Stats()["txn_retries_total"])
	require.Equal(t, uint64(0), executor.Stats()["txn_retry_exhausted_total"])
}

func TestTxnContentionRetryPolicyUsesLockTTLAfterFixedAttempts(t *testing.T) {
	lockErr := fakeTxnKeyError{errors: []*kvrpcpb.KeyError{{
		Locked: &kvrpcpb.Locked{
			PrimaryLock: []byte("dentry"),
			Key:         []byte("dentry"),
			LockVersion: 2,
			LockTtl:     uint64(5 * time.Second / time.Millisecond),
		},
	}}}
	budget := txnRetryBudget(lockErr, defaultLockTTL)

	require.Equal(t, 5*time.Second+txnContentionRetryMaxBackoff, budget)
	require.True(t, canRetryTxnAttempt(maxTxnContentionRetries+8, time.Now(), lockErr, defaultLockTTL))
	require.False(t, canRetryTxnAttempt(maxTxnContentionRetries+8, time.Now().Add(-budget-time.Millisecond), lockErr, defaultLockTTL))
}

func TestTxnContentionRetryPolicyKeepsCountBoundForNonLockConflicts(t *testing.T) {
	writeConflictErr := fakeTxnKeyError{errors: []*kvrpcpb.KeyError{{
		WriteConflict: &kvrpcpb.WriteConflict{
			Key:        []byte("dentry"),
			ConflictTs: 4,
			StartTs:    2,
		},
	}}}

	require.Zero(t, txnRetryBudget(writeConflictErr, defaultLockTTL))
	require.True(t, canRetryTxnAttempt(maxTxnContentionRetries-1, time.Now(), writeConflictErr, defaultLockTTL))
	require.False(t, canRetryTxnAttempt(maxTxnContentionRetries, time.Now(), writeConflictErr, defaultLockTTL))
}

func TestTxnRetryBudgetFallsBackWhenLockDetailsAreUnavailable(t *testing.T) {
	err := nokverrors.New(nokverrors.KindLockConflict, "lock conflict translated across rpc boundary")

	require.Equal(t, 25*time.Millisecond+txnContentionRetryMaxBackoff, txnRetryBudget(err, 25))
}

func TestTxnRetryBudgetCoversPercolatorRetryableStartTSLoss(t *testing.T) {
	err := fakeTxnKeyError{errors: []*kvrpcpb.KeyError{{
		Retryable: "percolator: lock not found",
	}}}

	require.Equal(t, 50*time.Millisecond+txnContentionRetryMaxBackoff, txnRetryBudget(err, 50))
	require.True(t, canRetryTxnAttempt(maxTxnContentionRetries+1, time.Now(), err, 50))
}

func TestExecutorAtomicOnePhaseBacksOffAfterRepeatedFallback(t *testing.T) {
	base := newFakeRunner()
	runner := &fakeAtomicRunner{fakeRunner: base, handled: false}
	authority := &fakeAuthorityResolver{same: true}
	resolver := &fakeMountResolver{records: map[model.MountID]MountAdmission{
		"vol": {MountID: "vol", MountKeyID: 1, RootInode: model.RootInode, SchemaVersion: 1},
	}}
	executor, err := newTestExecutor(runner, WithMountResolver(resolver), WithSubtreeAuthorityResolver(authority))
	require.NoError(t, err)

	seedDirectory(t, runner.fakeRunner, "vol", 8)
	total := atomicOnePhaseBackoffAfter + 3
	for i := range total {
		oldName := fmt.Sprintf("old-%d", i)
		newName := fmt.Sprintf("new-%d", i)
		seedDentry(t, runner.fakeRunner, "vol", 7, oldName, model.InodeID(100+i))
		err := executor.Rename(context.Background(), model.RenameRequest{
			Mount:      "vol",
			FromParent: 7,
			FromName:   oldName,
			ToParent:   8,
			ToName:     newName,
		})
		require.NoError(t, err)
	}

	require.Len(t, runner.atomicCalls, atomicOnePhaseBackoffAfter)
	stats := executor.Stats()
	requireAtomicStatUint(t, stats, model.OperationRename, "fallback_total", atomicOnePhaseBackoffAfter)
	requireAtomicStatUint(t, stats, model.OperationRename, "skip_total", 3)
	requireAtomicStatUint(t, stats, model.OperationRename, "backoff_skip_total", 3)
}

func TestExecutorAtomicOnePhaseBackoffIsAffinityScoped(t *testing.T) {
	base := newFakeRunner()
	runner := &fakeAtomicRunner{fakeRunner: base, handled: false}
	authority := &fakeAuthorityResolver{same: true}
	resolver := &fakeMountResolver{records: map[model.MountID]MountAdmission{
		"vol": {MountID: "vol", MountKeyID: 1, RootInode: model.RootInode, SchemaVersion: 1},
	}}
	executor, err := newTestExecutor(runner, WithMountResolver(resolver), WithSubtreeAuthorityResolver(authority))
	require.NoError(t, err)

	seedDirectory(t, runner.fakeRunner, "vol", 8)
	for i := range atomicOnePhaseBackoffAfter {
		oldName := fmt.Sprintf("old-%d", i)
		newName := fmt.Sprintf("new-%d", i)
		seedDentry(t, runner.fakeRunner, "vol", 7, oldName, model.InodeID(100+i))
		err := executor.Rename(context.Background(), model.RenameRequest{
			Mount:      "vol",
			FromParent: 7,
			FromName:   oldName,
			ToParent:   8,
			ToName:     newName,
		})
		require.NoError(t, err)
	}

	from, to := findDifferentRenameAffinity(t, 7, 8)
	seedDentry(t, runner.fakeRunner, "vol", from, "other-old", 999)
	seedDirectory(t, runner.fakeRunner, "vol", to)
	err = executor.Rename(context.Background(), model.RenameRequest{
		Mount:      "vol",
		FromParent: from,
		FromName:   "other-old",
		ToParent:   to,
		ToName:     "other-new",
	})
	require.NoError(t, err)

	require.Len(t, runner.atomicCalls, atomicOnePhaseBackoffAfter+1)
	requireAtomicStatUint(t, executor.Stats(), model.OperationRename, "skip_total", 0)
}
