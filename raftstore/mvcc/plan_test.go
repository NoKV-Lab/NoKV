// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package mvcc_test

import (
	"context"
	"errors"
	"testing"

	local "github.com/feichai0017/NoKV/local"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	storemvcc "github.com/feichai0017/NoKV/raftstore/mvcc"
	txnmvcc "github.com/feichai0017/NoKV/txn/mvcc"
	entrykv "github.com/feichai0017/NoKV/txn/storage"
	"github.com/stretchr/testify/require"
)

func openMVCCGCPlanTestDB(t *testing.T) *local.DB {
	t.Helper()
	opt := local.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	db, err := local.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func applyVersionedEntryForApplyTest(t *testing.T, db *local.DB, cf entrykv.ColumnFamily, key []byte, version uint64, value []byte, meta byte, expiresAt uint64) {
	t.Helper()
	entry := entrykv.NewInternalEntry(cf, key, version, entrykv.SafeCopy(nil, value), meta, expiresAt)
	defer entry.DecrRef()
	require.NoError(t, db.ApplyInternalEntries([]*entrykv.Entry{entry}))
}

func applyMVCCGCWrite(t *testing.T, db *local.DB, key []byte, commitTs, startTs uint64) {
	t.Helper()
	write := txnmvcc.EncodeWrite(txnmvcc.Write{Kind: kvrpcpb.Mutation_Put, StartTs: startTs})
	applyVersionedEntryForApplyTest(t, db, entrykv.CFWrite, key, commitTs, write, 0, 0)
}

func applyMVCCGCPutVersion(t *testing.T, db *local.DB, key []byte, commitTs, startTs uint64, value string) {
	t.Helper()
	applyVersionedEntryForApplyTest(t, db, entrykv.CFDefault, key, startTs, []byte(value), 0, 0)
	applyMVCCGCWrite(t, db, key, commitTs, startTs)
}

func TestPlanMVCCGCReportsMountScopedPlan(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	volKey := []byte("vol/key")
	otherKey := []byte("other/key")
	for _, key := range [][]byte{volKey, otherKey} {
		applyMVCCGCWrite(t, db, key, 150, 140)
		applyMVCCGCWrite(t, db, key, 90, 80)
		applyMVCCGCWrite(t, db, key, 40, 30)
	}

	stats, err := storemvcc.Plan(context.Background(), db, storemvcc.SafePointPolicy{
		RequestedSafePoint: 100,
		SnapshotRetention: rootstate.SnapshotRetentionIndex{
			GlobalFloor: 50,
			MountFloors: map[uint64]uint64{
				1: 50,
			},
		},
		Mount: testMountResolver,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(2), stats.ScannedKeys)
	require.Equal(t, uint64(6), stats.WriteVersions)
	require.Equal(t, uint64(5), stats.RetainedWrites)
	require.Equal(t, uint64(1), stats.DroppableWrites)
	require.Equal(t, uint64(2), stats.AnchorWrites)
	require.Equal(t, uint64(5), stats.RetainedDefaultRefs)
	require.Equal(t, uint64(1), stats.SafePointClampedKeys)
	require.Equal(t, uint64(3), stats.MaxVersionsPerKey)
	require.Equal(t, uint64(50), stats.MinEffectiveSafePoint)
	require.Equal(t, uint64(100), stats.MaxEffectiveSafePoint)
}

func TestPlanMVCCGCDoesNotDeleteData(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	key := []byte("vol/key")
	applyMVCCGCWrite(t, db, key, 90, 80)
	applyMVCCGCWrite(t, db, key, 40, 30)

	_, err := storemvcc.Plan(context.Background(), db, storemvcc.SafePointPolicy{RequestedSafePoint: 100})
	require.NoError(t, err)

	entry, err := db.GetInternalEntry(entrykv.CFWrite, key, 40)
	require.NoError(t, err)
	defer entry.DecrRef()
	require.NotNil(t, entry.Value)
}

func TestPlanMVCCGCRejectsCorruptWritePayload(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	key := []byte("vol/key")
	applyVersionedEntryForApplyTest(t, db, entrykv.CFWrite, key, 90, []byte{0xff}, 0, 0)

	_, err := storemvcc.Plan(context.Background(), db, storemvcc.SafePointPolicy{RequestedSafePoint: 100})
	require.ErrorContains(t, err, "decode CFWrite")
}

func TestPlanMVCCGCRejectsOverlongVersionChain(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	key := []byte("vol/hot")
	applyMVCCGCWrite(t, db, key, 150, 140)
	applyMVCCGCWrite(t, db, key, 90, 80)
	applyMVCCGCWrite(t, db, key, 40, 30)

	_, err := storemvcc.Plan(context.Background(), db, storemvcc.SafePointPolicy{
		RequestedSafePoint: 100,
		MaxVersionsPerKey:  2,
	})
	require.ErrorContains(t, err, "buffered write versions")
}

func TestPlanMVCCGCHonorsContextCancellation(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	key := []byte("vol/key")
	applyMVCCGCWrite(t, db, key, 90, 80)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := storemvcc.Plan(ctx, db, storemvcc.SafePointPolicy{RequestedSafePoint: 100})
	require.ErrorIs(t, err, context.Canceled)
}

func TestApplyMVCCGCDeletesDroppableWriteAndDefault(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	key := []byte("vol/key")
	applyMVCCGCPutVersion(t, db, key, 150, 140, "new")
	applyMVCCGCPutVersion(t, db, key, 90, 80, "anchor")
	applyMVCCGCPutVersion(t, db, key, 40, 30, "old")

	stats, err := storemvcc.ApplyReplicated(
		context.Background(),
		db,
		&testMaintenanceProposer{db: db},
		storemvcc.SafePointPolicy{RequestedSafePoint: 100},
		storemvcc.ApplyOptions{},
	)
	require.NoError(t, err)
	require.Equal(t, uint64(1), stats.AppliedWriteDeletes)
	require.Equal(t, uint64(1), stats.AppliedDefaultDeletes)
	require.Equal(t, uint64(1), stats.DroppableWrites)
	require.Equal(t, uint64(1), stats.AnchorWrites)

	droppedWrite, err := db.GetInternalEntry(entrykv.CFWrite, key, 40)
	require.NoError(t, err)
	defer droppedWrite.DecrRef()
	require.NotZero(t, droppedWrite.Meta&entrykv.BitDelete)

	droppedDefault, err := db.GetInternalEntry(entrykv.CFDefault, key, 30)
	require.NoError(t, err)
	defer droppedDefault.DecrRef()
	require.NotZero(t, droppedDefault.Meta&entrykv.BitDelete)

	anchorDefault, err := db.GetInternalEntry(entrykv.CFDefault, key, 80)
	require.NoError(t, err)
	defer anchorDefault.DecrRef()
	require.Zero(t, anchorDefault.Meta&entrykv.BitDelete)
	require.Equal(t, []byte("anchor"), anchorDefault.Value)
}

func TestApplyMVCCGCReplaySkipsAlreadyAppliedTombstones(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	key := []byte("vol/replay")
	applyMVCCGCPutVersion(t, db, key, 150, 140, "new")
	applyMVCCGCPutVersion(t, db, key, 90, 80, "anchor")
	applyMVCCGCPutVersion(t, db, key, 40, 30, "old")

	first, err := storemvcc.ApplyReplicated(
		context.Background(),
		db,
		&testMaintenanceProposer{db: db},
		storemvcc.SafePointPolicy{RequestedSafePoint: 100},
		storemvcc.ApplyOptions{},
	)
	require.NoError(t, err)
	require.Equal(t, uint64(1), first.AppliedWriteDeletes)
	require.Equal(t, uint64(1), first.AppliedDefaultDeletes)

	second, err := storemvcc.ApplyReplicated(
		context.Background(),
		db,
		&testMaintenanceProposer{db: db},
		storemvcc.SafePointPolicy{RequestedSafePoint: 100},
		storemvcc.ApplyOptions{},
	)
	require.NoError(t, err)
	require.Zero(t, second.AppliedWriteDeletes)
	require.Zero(t, second.AppliedDefaultDeletes)
	require.Zero(t, second.DroppableWrites)
	require.Equal(t, uint64(1), second.DeletedWriteMarkers)

	droppedWrite, err := db.GetInternalEntry(entrykv.CFWrite, key, 40)
	require.NoError(t, err)
	defer droppedWrite.DecrRef()
	require.NotZero(t, droppedWrite.Meta&entrykv.BitDelete)

	droppedDefault, err := db.GetInternalEntry(entrykv.CFDefault, key, 30)
	require.NoError(t, err)
	defer droppedDefault.DecrRef()
	require.NotZero(t, droppedDefault.Meta&entrykv.BitDelete)
}

func TestApplyMVCCGCReplicatedUsesMaintenanceProposer(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	key := []byte("vol/key")
	applyMVCCGCPutVersion(t, db, key, 150, 140, "new")
	applyMVCCGCPutVersion(t, db, key, 90, 80, "anchor")
	applyMVCCGCPutVersion(t, db, key, 40, 30, "old")

	proposer := &testMaintenanceProposer{db: db}
	stats, err := storemvcc.ApplyReplicated(
		context.Background(),
		db,
		proposer,
		storemvcc.SafePointPolicy{RequestedSafePoint: 100},
		storemvcc.ApplyOptions{BatchEntries: 1},
	)
	require.NoError(t, err)
	require.Equal(t, uint64(1), stats.AppliedWriteDeletes)
	require.Equal(t, uint64(1), stats.AppliedDefaultDeletes)
	require.Equal(t, 1, proposer.calls)

	droppedWrite, err := db.GetInternalEntry(entrykv.CFWrite, key, 40)
	require.NoError(t, err)
	defer droppedWrite.DecrRef()
	require.NotZero(t, droppedWrite.Meta&entrykv.BitDelete)
}

func TestApplyMVCCGCReplicatedRejectsPartialProposerAck(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	key := []byte("vol/key")
	applyMVCCGCPutVersion(t, db, key, 150, 140, "new")
	applyMVCCGCPutVersion(t, db, key, 90, 80, "anchor")
	applyMVCCGCPutVersion(t, db, key, 40, 30, "old")

	_, err := storemvcc.ApplyReplicated(
		context.Background(),
		db,
		&testMaintenanceProposer{applied: 1},
		storemvcc.SafePointPolicy{RequestedSafePoint: 100},
		storemvcc.ApplyOptions{},
	)
	require.ErrorContains(t, err, "applied 1 entries")
}

func TestApplyMVCCGCReplicatedReportsPartialSubmitStats(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	key := []byte("vol/key")
	applyMVCCGCPutVersion(t, db, key, 150, 140, "new")
	applyMVCCGCPutVersion(t, db, key, 90, 80, "anchor")
	applyMVCCGCPutVersion(t, db, key, 40, 30, "old")

	stats, err := storemvcc.ApplyReplicated(
		context.Background(),
		db,
		&partialMaintenanceProposer{err: errors.New("region failed"), writeDeletes: 1},
		storemvcc.SafePointPolicy{RequestedSafePoint: 100},
		storemvcc.ApplyOptions{},
	)
	require.ErrorContains(t, err, "region failed")
	require.Equal(t, uint64(1), stats.AppliedWriteDeletes)
	require.Zero(t, stats.AppliedDefaultDeletes)
}

func TestApplyMVCCGCHonorsMountScopedRetention(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	volKey := []byte("vol/key")
	otherKey := []byte("other/key")
	for _, key := range [][]byte{volKey, otherKey} {
		applyMVCCGCPutVersion(t, db, key, 150, 140, "new")
		applyMVCCGCPutVersion(t, db, key, 90, 80, "mid")
		applyMVCCGCPutVersion(t, db, key, 40, 30, "old")
	}

	stats, err := storemvcc.ApplyReplicated(
		context.Background(),
		db,
		&testMaintenanceProposer{db: db},
		storemvcc.SafePointPolicy{
			RequestedSafePoint: 100,
			SnapshotRetention: rootstate.SnapshotRetentionIndex{
				GlobalFloor: 50,
				MountFloors: map[uint64]uint64{
					1: 50,
				},
			},
			Mount: testMountResolver,
		},
		storemvcc.ApplyOptions{},
	)
	require.NoError(t, err)
	require.Equal(t, uint64(1), stats.AppliedWriteDeletes)
	require.Equal(t, uint64(1), stats.AppliedDefaultDeletes)

	retainedVolWrite, err := db.GetInternalEntry(entrykv.CFWrite, volKey, 40)
	require.NoError(t, err)
	defer retainedVolWrite.DecrRef()
	require.Zero(t, retainedVolWrite.Meta&entrykv.BitDelete)

	droppedOtherWrite, err := db.GetInternalEntry(entrykv.CFWrite, otherKey, 40)
	require.NoError(t, err)
	defer droppedOtherWrite.DecrRef()
	require.NotZero(t, droppedOtherWrite.Meta&entrykv.BitDelete)
}

func TestApplyMVCCGCBatchesWithoutRescanningDeletedKeys(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	for i := range byte(4) {
		key := []byte{'v', 'o', 'l', '/', '0' + i}
		applyMVCCGCPutVersion(t, db, key, 150, 140, "new")
		applyMVCCGCPutVersion(t, db, key, 90, 80, "anchor")
		applyMVCCGCPutVersion(t, db, key, 40, 30, "old")
	}

	stats, err := storemvcc.ApplyReplicated(
		context.Background(),
		db,
		&testMaintenanceProposer{db: db},
		storemvcc.SafePointPolicy{RequestedSafePoint: 100},
		storemvcc.ApplyOptions{BatchEntries: 2},
	)
	require.NoError(t, err)
	require.Equal(t, uint64(4), stats.ScannedKeys)
	require.Equal(t, uint64(4), stats.AppliedWriteDeletes)
	require.Equal(t, uint64(4), stats.AppliedDefaultDeletes)
	require.Equal(t, uint64(4), stats.DroppableWrites)

	for i := range byte(4) {
		key := []byte{'v', 'o', 'l', '/', '0' + i}
		droppedWrite, err := db.GetInternalEntry(entrykv.CFWrite, key, 40)
		require.NoError(t, err)
		require.NotZero(t, droppedWrite.Meta&entrykv.BitDelete)
		droppedWrite.DecrRef()
	}
}

func TestApplyMVCCGCStopsAtMaxKeys(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	for i := range byte(4) {
		key := []byte{'v', 'o', 'l', '/', '0' + i}
		applyMVCCGCPutVersion(t, db, key, 150, 140, "new")
		applyMVCCGCPutVersion(t, db, key, 90, 80, "anchor")
		applyMVCCGCPutVersion(t, db, key, 40, 30, "old")
	}

	stats, err := storemvcc.ApplyReplicated(
		context.Background(),
		db,
		&testMaintenanceProposer{db: db},
		storemvcc.SafePointPolicy{RequestedSafePoint: 100},
		storemvcc.ApplyOptions{BatchEntries: 100, MaxKeys: 2},
	)
	require.NoError(t, err)
	require.Equal(t, uint64(2), stats.ScannedKeys)
	require.Equal(t, uint64(2), stats.AppliedWriteDeletes)
	require.Equal(t, uint64(2), stats.AppliedDefaultDeletes)
}

type testMaintenanceProposer struct {
	db      *local.DB
	calls   int
	applied uint64
}

func (p *testMaintenanceProposer) ProposeMVCCMaintenance(_ context.Context, entries []*entrykv.Entry) (uint64, uint64, uint64, error) {
	p.calls++
	if p.db != nil {
		if err := p.db.ApplyInternalEntries(entries); err != nil {
			return 0, 0, 0, err
		}
	}
	if p.applied != 0 {
		limit := min(int(p.applied), len(entries))
		writes, defaults := countTestMaintenanceEntries(entries[:limit])
		return p.applied, writes, defaults, nil
	}
	writes, defaults := countTestMaintenanceEntries(entries)
	return uint64(len(entries)), writes, defaults, nil
}

type partialMaintenanceProposer struct {
	err            error
	writeDeletes   uint64
	defaultDeletes uint64
}

func (p *partialMaintenanceProposer) ProposeMVCCMaintenance(context.Context, []*entrykv.Entry) (uint64, uint64, uint64, error) {
	return p.writeDeletes + p.defaultDeletes, p.writeDeletes, p.defaultDeletes, p.err
}

func countTestMaintenanceEntries(entries []*entrykv.Entry) (uint64, uint64) {
	var writes, defaults uint64
	for _, entry := range entries {
		if entry == nil {
			continue
		}
		cf, _, _, ok := entrykv.SplitInternalKey(entry.Key)
		if !ok {
			continue
		}
		switch cf {
		case entrykv.CFWrite:
			writes++
		case entrykv.CFDefault:
			defaults++
		}
	}
	return writes, defaults
}
