// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package percolator

import (
	"errors"
	"path/filepath"
	"testing"

	local "github.com/feichai0017/NoKV/local"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/feichai0017/NoKV/txn/latch"
	"github.com/feichai0017/NoKV/utils"
	"github.com/stretchr/testify/require"
)

func TestPercolatorCrashMatrixBatchPrewriteApplyFailureRetries(t *testing.T) {
	db := openTestDB(t)
	store := newCountingStore(db)
	latches := latch.NewManager(32)
	primary := []byte("crash-prewrite-primary")
	secondary := []byte("crash-prewrite-secondary")
	startTs := uint64(90)
	injected := errors.New("prewrite batch apply failed")
	store.failNextApply(injected)

	errs := Prewrite(store, latches, &kvrpcpb.PrewriteRequest{
		PrimaryLock:  primary,
		StartVersion: startTs,
		LockTtl:      3000,
		Mutations: []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Mutation_Put, Key: primary, Value: []byte("primary-value")},
			{Op: kvrpcpb.Mutation_Put, Key: secondary, Value: []byte("secondary-value")},
		},
	})
	require.Len(t, errs, 1)
	require.Contains(t, errs[0].GetRetryable(), injected.Error())

	reader := NewReader(db)
	for _, key := range [][]byte{primary, secondary} {
		lock, err := reader.GetLock(key)
		require.NoError(t, err)
		require.Nil(t, lock, "key=%s", key)
	}

	require.Empty(t, Prewrite(store, latches, &kvrpcpb.PrewriteRequest{
		PrimaryLock:  primary,
		StartVersion: startTs,
		LockTtl:      3000,
		Mutations: []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Mutation_Put, Key: primary, Value: []byte("primary-value")},
			{Op: kvrpcpb.Mutation_Put, Key: secondary, Value: []byte("secondary-value")},
		},
	}))
	for _, key := range [][]byte{primary, secondary} {
		lock, err := reader.GetLock(key)
		require.NoError(t, err)
		require.NotNil(t, lock, "key=%s", key)
	}
}

func TestPercolatorCrashMatrixAtomicMutateUsesPebbleBatch(t *testing.T) {
	workDir := t.TempDir()
	startTs := uint64(300)
	commitTs := uint64(320)
	dentryKey, inodeKey := []byte("crash-dentry-value"), []byte("crash-inode-value")

	opt := testOptionsForDir(workDir)
	db, err := local.Open(opt)
	require.NoError(t, err)

	latches := latch.NewManager(32)
	req := atomicPutIfAbsentRequest(startTs, commitTs, dentryKey, []byte("dentry-value"), inodeKey, []byte("inode-value"))

	result := ApplyAtomicMutate(db, latches, req)
	require.Nil(t, result.Error)
	require.False(t, result.Fallback)
	require.Equal(t, uint64(2), result.AppliedKeys)
	_ = db.Close()

	reopenedOpt := testOptionsForDir(workDir)
	db, err = local.Open(reopenedOpt)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	reader := NewReader(db)
	value, _, err := reader.GetValue(dentryKey, commitTs+1)
	require.NoError(t, err)
	require.Equal(t, []byte("dentry-value"), value)
	value, _, err = reader.GetValue(inodeKey, commitTs+1)
	require.NoError(t, err)
	require.Equal(t, []byte("inode-value"), value)
}

func TestPercolatorCrashMatrixPrimaryCommittedSecondaryRecovered(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(32)
	primary := []byte("crash-primary-commit")
	secondary := []byte("crash-secondary-commit")
	startTs := uint64(100)
	commitTs := uint64(120)

	require.Empty(t, Prewrite(db, latches, &kvrpcpb.PrewriteRequest{
		PrimaryLock:  primary,
		StartVersion: startTs,
		LockTtl:      3000,
		Mutations: []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Mutation_Put, Key: primary, Value: []byte("primary-value")},
			{Op: kvrpcpb.Mutation_Put, Key: secondary, Value: []byte("secondary-value")},
		},
	}))
	require.Nil(t, Commit(db, latches, &kvrpcpb.CommitRequest{
		Keys:          [][]byte{primary},
		StartVersion:  startTs,
		CommitVersion: commitTs,
	}))

	status := CheckTxnStatus(db, latches, &kvrpcpb.CheckTxnStatusRequest{
		PrimaryKey: primary,
		LockTs:     startTs,
	})
	require.Nil(t, status.GetError())
	require.Equal(t, commitTs, status.GetCommitVersion())

	resolved, keyErr := ResolveLock(db, latches, &kvrpcpb.ResolveLockRequest{
		StartVersion:  startTs,
		CommitVersion: status.GetCommitVersion(),
		Keys:          [][]byte{secondary},
	})
	require.Nil(t, keyErr)
	require.Equal(t, uint64(1), resolved)

	reader := NewReader(db)
	value, _, err := reader.GetValue(primary, commitTs+10)
	require.NoError(t, err)
	require.Equal(t, []byte("primary-value"), value)
	value, _, err = reader.GetValue(secondary, commitTs+10)
	require.NoError(t, err)
	require.Equal(t, []byte("secondary-value"), value)
	lock, err := reader.GetLock(secondary)
	require.NoError(t, err)
	require.Nil(t, lock)
}

func TestPercolatorCrashMatrixSecondaryResolveApplyFailureRetries(t *testing.T) {
	db := openTestDB(t)
	store := newCountingStore(db)
	latches := latch.NewManager(32)
	primary := []byte("crash-resolve-primary")
	secondary := []byte("crash-resolve-secondary")
	startTs := uint64(130)
	commitTs := uint64(150)

	require.Empty(t, Prewrite(db, latches, &kvrpcpb.PrewriteRequest{
		PrimaryLock:  primary,
		StartVersion: startTs,
		LockTtl:      3000,
		Mutations: []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Mutation_Put, Key: primary, Value: []byte("primary-value")},
			{Op: kvrpcpb.Mutation_Put, Key: secondary, Value: []byte("secondary-value")},
		},
	}))
	require.Nil(t, Commit(db, latches, &kvrpcpb.CommitRequest{
		Keys:          [][]byte{primary},
		StartVersion:  startTs,
		CommitVersion: commitTs,
	}))

	injected := errors.New("resolve batch apply failed")
	store.failNextApply(injected)
	resolved, keyErr := ResolveLock(store, latches, &kvrpcpb.ResolveLockRequest{
		StartVersion:  startTs,
		CommitVersion: commitTs,
		Keys:          [][]byte{secondary},
	})
	require.Zero(t, resolved)
	require.NotNil(t, keyErr)
	require.Contains(t, keyErr.GetRetryable(), injected.Error())

	reader := NewReader(db)
	lock, err := reader.GetLock(secondary)
	require.NoError(t, err)
	require.NotNil(t, lock)

	resolved, keyErr = ResolveLock(store, latches, &kvrpcpb.ResolveLockRequest{
		StartVersion:  startTs,
		CommitVersion: commitTs,
		Keys:          [][]byte{secondary},
	})
	require.Nil(t, keyErr)
	require.Equal(t, uint64(1), resolved)
	value, _, err := reader.GetValue(secondary, commitTs+10)
	require.NoError(t, err)
	require.Equal(t, []byte("secondary-value"), value)
}

func TestPercolatorCrashMatrixPrimaryRollbackSecondaryRecovered(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(32)
	primary := []byte("crash-primary-rollback")
	secondary := []byte("crash-secondary-rollback")
	startTs := uint64(200)

	require.Empty(t, Prewrite(db, latches, &kvrpcpb.PrewriteRequest{
		PrimaryLock:  primary,
		StartVersion: startTs,
		LockTtl:      3000,
		Mutations: []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Mutation_Put, Key: primary, Value: []byte("primary-value")},
			{Op: kvrpcpb.Mutation_Put, Key: secondary, Value: []byte("secondary-value")},
		},
	}))
	require.Nil(t, BatchRollback(db, latches, &kvrpcpb.BatchRollbackRequest{
		Keys:         [][]byte{primary},
		StartVersion: startTs,
	}))

	status := CheckTxnStatus(db, latches, &kvrpcpb.CheckTxnStatusRequest{
		PrimaryKey:         primary,
		LockTs:             startTs,
		RollbackIfNotExist: true,
	})
	require.Nil(t, status.GetError())
	require.Equal(t, kvrpcpb.CheckTxnStatusAction_CheckTxnStatusLockNotExistRollback, status.GetAction())

	resolved, keyErr := ResolveLock(db, latches, &kvrpcpb.ResolveLockRequest{
		StartVersion: startTs,
		Keys:         [][]byte{secondary},
	})
	require.Nil(t, keyErr)
	require.Equal(t, uint64(1), resolved)

	reader := NewReader(db)
	_, _, err := reader.GetValue(primary, startTs+100)
	require.ErrorIs(t, err, utils.ErrKeyNotFound)
	_, _, err = reader.GetValue(secondary, startTs+100)
	require.ErrorIs(t, err, utils.ErrKeyNotFound)
	lock, err := reader.GetLock(secondary)
	require.NoError(t, err)
	require.Nil(t, lock)
}

func TestPercolatorCrashMatrixCommitAndRollbackIdempotentAfterRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	db, err := local.Open(testOptionsForDir(dir))
	require.NoError(t, err)
	latches := latch.NewManager(32)

	committedKey := []byte("crash-restart-committed")
	require.Empty(t, Prewrite(db, latches, &kvrpcpb.PrewriteRequest{
		PrimaryLock:  committedKey,
		StartVersion: 300,
		LockTtl:      3000,
		Mutations: []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Mutation_Put, Key: committedKey, Value: []byte("committed-value")},
		},
	}))
	require.Nil(t, Commit(db, latches, &kvrpcpb.CommitRequest{
		Keys:          [][]byte{committedKey},
		StartVersion:  300,
		CommitVersion: 320,
	}))

	rolledBackKey := []byte("crash-restart-rolled-back")
	require.Empty(t, Prewrite(db, latches, &kvrpcpb.PrewriteRequest{
		PrimaryLock:  rolledBackKey,
		StartVersion: 400,
		LockTtl:      3000,
		Mutations: []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Mutation_Put, Key: rolledBackKey, Value: []byte("rolled-back-value")},
		},
	}))
	require.Nil(t, BatchRollback(db, latches, &kvrpcpb.BatchRollbackRequest{
		Keys:         [][]byte{rolledBackKey},
		StartVersion: 400,
	}))
	require.NoError(t, db.Close())

	db, err = local.Open(testOptionsForDir(dir))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	require.Nil(t, Commit(db, latches, &kvrpcpb.CommitRequest{
		Keys:          [][]byte{committedKey},
		StartVersion:  300,
		CommitVersion: 320,
	}))
	require.Nil(t, BatchRollback(db, latches, &kvrpcpb.BatchRollbackRequest{
		Keys:         [][]byte{committedKey},
		StartVersion: 300,
	}))
	require.Nil(t, BatchRollback(db, latches, &kvrpcpb.BatchRollbackRequest{
		Keys:         [][]byte{rolledBackKey},
		StartVersion: 400,
	}))

	reader := NewReader(db)
	value, _, err := reader.GetValue(committedKey, 500)
	require.NoError(t, err)
	require.Equal(t, []byte("committed-value"), value)
	_, _, err = reader.GetValue(rolledBackKey, 500)
	require.ErrorIs(t, err, utils.ErrKeyNotFound)
}
