// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package mvcc_test

import (
	"context"
	"testing"

	local "github.com/feichai0017/NoKV/local"
	storemvcc "github.com/feichai0017/NoKV/raftstore/mvcc"
	txnmvcc "github.com/feichai0017/NoKV/txn/mvcc"
	entrykv "github.com/feichai0017/NoKV/txn/storage"
	"github.com/stretchr/testify/require"
)

func applyMVCCGCLock(t *testing.T, db *local.DB, key []byte, startTs uint64) {
	t.Helper()
	lock := txnmvcc.EncodeLock(txnmvcc.Lock{
		Primary: key,
		Ts:      startTs,
	})
	applyVersionedEntryForApplyTest(t, db, entrykv.CFLock, key, entrykv.MaxVersion, lock, 0, 0)
}

func TestPlanMVCCGCTxnFloorScansActiveLocks(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	applyMVCCGCLock(t, db, []byte("a"), 80)
	applyMVCCGCLock(t, db, []byte("b"), 30)
	applyVersionedEntryForApplyTest(t, db, entrykv.CFLock, []byte("c"), entrykv.MaxVersion, nil, entrykv.BitDelete, 0)

	floor, err := storemvcc.PlanTxnFloor(context.Background(), db)
	require.NoError(t, err)
	require.True(t, floor.Active())
	require.Equal(t, uint64(2), floor.ActiveLocks)
	require.Equal(t, uint64(30), floor.OldestStartTs)
	require.Equal(t, uint64(80), floor.MaxStartTs)
}

func TestPlanMVCCGCTxnFloorRejectsCorruptLock(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	applyVersionedEntryForApplyTest(t, db, entrykv.CFLock, []byte("bad"), entrykv.MaxVersion, []byte{0xff}, 0, 0)

	_, err := storemvcc.PlanTxnFloor(context.Background(), db)
	require.ErrorContains(t, err, "decode CFLock")
}

func TestPlanMVCCGCTxnFloorHonorsContextCancellation(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	applyMVCCGCLock(t, db, []byte("a"), 80)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := storemvcc.PlanTxnFloor(ctx, db)
	require.ErrorIs(t, err, context.Canceled)
}
