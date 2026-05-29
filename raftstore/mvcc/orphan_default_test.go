// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package mvcc_test

import (
	"context"
	"testing"

	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	storemvcc "github.com/feichai0017/NoKV/raftstore/mvcc"
	entrykv "github.com/feichai0017/NoKV/txn/storage"
	"github.com/stretchr/testify/require"
)

func TestApplyOrphanDefaultsDeletesUnownedDefaultRecord(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	key := []byte("orphan")
	applyVersionedEntryForApplyTest(t, db, entrykv.CFDefault, key, 10, []byte("value"), 0, 0)

	stats, err := storemvcc.ApplyOrphanDefaultsReplicated(context.Background(), db, &testMaintenanceProposer{db: db}, storemvcc.OrphanDefaultOptions{BatchEntries: 1})
	require.NoError(t, err)
	require.Equal(t, uint64(1), stats.ScannedDefaults)
	require.Equal(t, uint64(1), stats.OrphanDefaults)
	require.Equal(t, uint64(1), stats.AppliedDefaultDeletes)

	payload, err := db.GetInternalEntry(entrykv.CFDefault, key, 10)
	require.NoError(t, err)
	defer payload.DecrRef()
	require.NotZero(t, payload.Meta&entrykv.BitDelete)
}

func TestApplyOrphanDefaultsReplaySkipsAppliedDefaultTombstone(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	key := []byte("orphan-replay")
	applyVersionedEntryForApplyTest(t, db, entrykv.CFDefault, key, 10, []byte("value"), 0, 0)

	first, err := storemvcc.ApplyOrphanDefaultsReplicated(context.Background(), db, &testMaintenanceProposer{db: db}, storemvcc.OrphanDefaultOptions{BatchEntries: 1})
	require.NoError(t, err)
	require.Equal(t, uint64(1), first.AppliedDefaultDeletes)

	second, err := storemvcc.ApplyOrphanDefaultsReplicated(context.Background(), db, &testMaintenanceProposer{db: db}, storemvcc.OrphanDefaultOptions{BatchEntries: 1})
	require.NoError(t, err)
	require.Zero(t, second.OrphanDefaults)
	require.Zero(t, second.AppliedDefaultDeletes)
	require.Equal(t, uint64(1), second.DeletedDefaultMarkers)

	payload, err := db.GetInternalEntry(entrykv.CFDefault, key, 10)
	require.NoError(t, err)
	defer payload.DecrRef()
	require.NotZero(t, payload.Meta&entrykv.BitDelete)
}

func TestApplyOrphanDefaultsReplicatedUsesMaintenanceProposer(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	key := []byte("orphan")
	applyVersionedEntryForApplyTest(t, db, entrykv.CFDefault, key, 10, []byte("value"), 0, 0)

	proposer := &testMaintenanceProposer{db: db}
	stats, err := storemvcc.ApplyOrphanDefaultsReplicated(context.Background(), db, proposer, storemvcc.OrphanDefaultOptions{BatchEntries: 1})
	require.NoError(t, err)
	require.Equal(t, uint64(1), stats.OrphanDefaults)
	require.Equal(t, uint64(1), stats.AppliedDefaultDeletes)
	require.Equal(t, 1, proposer.calls)

	payload, err := db.GetInternalEntry(entrykv.CFDefault, key, 10)
	require.NoError(t, err)
	defer payload.DecrRef()
	require.NotZero(t, payload.Meta&entrykv.BitDelete)
}

func TestApplyOrphanDefaultsRetainsWriteOwnedDefaultRecord(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	key := []byte("owned")
	applyMVCCGCPutVersion(t, db, key, 20, 10, "value")

	stats, err := storemvcc.ApplyOrphanDefaultsReplicated(context.Background(), db, &testMaintenanceProposer{db: db}, storemvcc.OrphanDefaultOptions{})
	require.NoError(t, err)
	require.Equal(t, uint64(1), stats.ScannedDefaults)
	require.Equal(t, uint64(1), stats.RetainedDefaults)
	require.Zero(t, stats.AppliedDefaultDeletes)
}

func TestApplyOrphanDefaultsRetainsLockOwnedDefaultRecord(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	key := []byte("locked")
	applyVersionedEntryForApplyTest(t, db, entrykv.CFDefault, key, 10, []byte("value"), 0, 0)
	applyMVCCGCLockRecord(t, db, key, key, 10, 100, kvrpcpb.Mutation_Put)

	stats, err := storemvcc.ApplyOrphanDefaultsReplicated(context.Background(), db, &testMaintenanceProposer{db: db}, storemvcc.OrphanDefaultOptions{})
	require.NoError(t, err)
	require.Equal(t, uint64(1), stats.ScannedDefaults)
	require.Equal(t, uint64(1), stats.RetainedDefaults)
	require.Zero(t, stats.AppliedDefaultDeletes)
}
