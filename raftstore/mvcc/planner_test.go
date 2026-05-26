// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package mvcc_test

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
	local "github.com/feichai0017/NoKV/local"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	storemvcc "github.com/feichai0017/NoKV/raftstore/mvcc"
	"github.com/feichai0017/NoKV/txn/mvcc"
	"github.com/feichai0017/NoKV/utils"
	"github.com/stretchr/testify/require"
)

type mvccGCPlannerHarness struct {
	task    *utils.PeriodicTask
	planner *storemvcc.GCPlanner
}

func newMVCCGCPlannerTestOptions(t *testing.T) *local.Options {
	t.Helper()
	opt := local.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	opt.MemTableSize = 1 << 12
	opt.SSTableMaxSz = 1 << 20
	return opt
}

func openMVCCGCPlannerTestDB(t *testing.T, opt *local.Options) *local.DB {
	t.Helper()
	db, err := local.Open(opt)
	require.NoError(t, err)
	return db
}

func applyMVCCGCPlannerEntry(t *testing.T, db *local.DB, cf kv.ColumnFamily, key []byte, version uint64, value []byte, meta byte) {
	t.Helper()
	entry := kv.NewInternalEntry(cf, key, version, kv.SafeCopy(nil, value), meta, 0)
	defer entry.DecrRef()
	require.NoError(t, db.ApplyInternalEntries([]*kv.Entry{entry}))
}

func applyMVCCGCPlannerWrite(t *testing.T, db *local.DB, key []byte, commitTs, startTs uint64) {
	t.Helper()
	write := mvcc.EncodeWrite(mvcc.Write{Kind: kvrpcpb.Mutation_Put, StartTs: startTs})
	applyMVCCGCPlannerEntry(t, db, kv.CFWrite, key, commitTs, write, 0)
}

func applyMVCCGCPlannerLock(t *testing.T, db *local.DB, key []byte, startTs uint64) {
	t.Helper()
	lock := mvcc.EncodeLock(mvcc.Lock{
		Primary: key,
		Ts:      startTs,
	})
	applyMVCCGCPlannerEntry(t, db, kv.CFLock, key, kv.MaxVersion, lock, 0)
}

func startMVCCGCPlannerHarness(t *testing.T, cfg storemvcc.GCPlanConfig) *mvccGCPlannerHarness {
	t.Helper()
	taskCfg, planner, ok := storemvcc.NewGCPlanTask(cfg)
	require.True(t, ok)
	task := utils.NewPeriodicTask(taskCfg)
	require.NotNil(t, task)
	task.Start()
	return &mvccGCPlannerHarness{task: task, planner: planner}
}

func (h *mvccGCPlannerHarness) Close() {
	if h != nil && h.task != nil {
		h.task.Close()
	}
}

func (h *mvccGCPlannerHarness) Snapshot() storemvcc.GCPlanSnapshot {
	if h == nil || h.planner == nil || h.task == nil {
		return storemvcc.GCPlanSnapshot{}
	}
	return h.planner.Snapshot(h.task.Snapshot())
}

func waitMVCCGCPlannerSnapshot(t *testing.T, planner *mvccGCPlannerHarness, fn func(storemvcc.GCPlanSnapshot) bool) storemvcc.GCPlanSnapshot {
	t.Helper()
	var snap storemvcc.GCPlanSnapshot
	require.Eventually(t, func() bool {
		snap = planner.Snapshot()
		return fn(snap)
	}, time.Second, 10*time.Millisecond)
	return snap
}

func TestMVCCGCPlannerDisabledByDefault(t *testing.T) {
	opt := newMVCCGCPlannerTestOptions(t)
	db := openMVCCGCPlannerTestDB(t, opt)
	defer func() { _ = db.Close() }()

	_, _, ok := storemvcc.NewGCPlanTask(storemvcc.GCPlanConfig{MVCCStore: db})
	require.False(t, ok)
}

func TestMVCCGCPlannerRunsReadOnlyPlan(t *testing.T) {
	opt := newMVCCGCPlannerTestOptions(t)
	db := openMVCCGCPlannerTestDB(t, opt)
	defer func() { _ = db.Close() }()
	planner := startMVCCGCPlannerHarness(t, storemvcc.GCPlanConfig{
		MVCCStore: db,
		Interval:  5 * time.Millisecond,
		SafePoint: func() uint64 { return 100 },
	})
	defer planner.Close()

	key := []byte("planner-key")
	applyMVCCGCPlannerWrite(t, db, key, 150, 140)
	applyMVCCGCPlannerWrite(t, db, key, 90, 80)
	applyMVCCGCPlannerWrite(t, db, key, 40, 30)

	snap := waitMVCCGCPlannerSnapshot(t, planner, func(s storemvcc.GCPlanSnapshot) bool {
		return s.LastError == "" && s.LastPlan.DroppableWrites == 1
	})
	require.True(t, snap.Enabled)
	require.Greater(t, snap.Runs, uint64(0))
	require.Equal(t, uint64(1), snap.LastPlan.ScannedKeys)

	entry, err := db.GetInternalEntry(kv.CFWrite, key, 40)
	require.NoError(t, err)
	defer entry.DecrRef()
	require.Zero(t, entry.Meta&kv.BitDelete)
}

func TestMVCCGCPlannerReadsSafePointEachRun(t *testing.T) {
	var safePoint atomic.Uint64
	opt := newMVCCGCPlannerTestOptions(t)
	db := openMVCCGCPlannerTestDB(t, opt)
	defer func() { _ = db.Close() }()
	planner := startMVCCGCPlannerHarness(t, storemvcc.GCPlanConfig{
		MVCCStore: db,
		Interval:  5 * time.Millisecond,
		SafePoint: safePoint.Load,
	})
	defer planner.Close()

	key := []byte("dynamic-safe-point-key")
	applyMVCCGCPlannerWrite(t, db, key, 150, 140)
	applyMVCCGCPlannerWrite(t, db, key, 90, 80)
	applyMVCCGCPlannerWrite(t, db, key, 40, 30)
	waitMVCCGCPlannerSnapshot(t, planner, func(s storemvcc.GCPlanSnapshot) bool {
		return s.Runs > 0 && s.SkippedRuns > 0 && s.LastPlan.ScannedKeys == 0
	})

	safePoint.Store(100)
	waitMVCCGCPlannerSnapshot(t, planner, func(s storemvcc.GCPlanSnapshot) bool {
		return s.LastError == "" && s.LastPlan.DroppableWrites == 1
	})
}

func TestMVCCGCPlannerRetainsLastPlanWhenSafePointDisabled(t *testing.T) {
	var safePoint atomic.Uint64
	safePoint.Store(100)
	opt := newMVCCGCPlannerTestOptions(t)
	db := openMVCCGCPlannerTestDB(t, opt)
	defer func() { _ = db.Close() }()
	planner := startMVCCGCPlannerHarness(t, storemvcc.GCPlanConfig{
		MVCCStore: db,
		Interval:  5 * time.Millisecond,
		SafePoint: safePoint.Load,
	})
	defer planner.Close()

	key := []byte("disabled-safe-point-key")
	applyMVCCGCPlannerWrite(t, db, key, 150, 140)
	applyMVCCGCPlannerWrite(t, db, key, 90, 80)
	applyMVCCGCPlannerWrite(t, db, key, 40, 30)
	waitMVCCGCPlannerSnapshot(t, planner, func(s storemvcc.GCPlanSnapshot) bool {
		return s.LastError == "" && s.LastPlan.DroppableWrites == 1
	})

	safePoint.Store(0)
	time.Sleep(20 * time.Millisecond)
	snap := planner.Snapshot()
	require.Equal(t, uint64(1), snap.LastPlan.DroppableWrites)
	require.Greater(t, snap.SkippedRuns, uint64(0))
}

func TestMVCCGCPlannerHonorsSnapshotRetentionAndTxnFloor(t *testing.T) {
	opt := newMVCCGCPlannerTestOptions(t)
	db := openMVCCGCPlannerTestDB(t, opt)
	defer func() { _ = db.Close() }()
	planner := startMVCCGCPlannerHarness(t, storemvcc.GCPlanConfig{
		MVCCStore: db,
		Interval:  5 * time.Millisecond,
		SafePoint: func() uint64 { return 100 },
		Retention: func() rootstate.SnapshotRetentionIndex {
			return rootstate.SnapshotRetentionIndex{
				MountFloors: map[uint64]uint64{
					1: 50,
				},
			}
		},
		Mount: layout.MountKeyResolver,
	})
	defer planner.Close()

	volKey, err := layout.EncodeInodeKey(model.MountIdentity{MountID: "vol", MountKeyID: 1}, 10)
	require.NoError(t, err)
	otherKey, err := layout.EncodeInodeKey(model.MountIdentity{MountID: "other", MountKeyID: 2}, 10)
	require.NoError(t, err)
	for _, key := range [][]byte{volKey, otherKey} {
		applyMVCCGCPlannerWrite(t, db, key, 150, 140)
		applyMVCCGCPlannerWrite(t, db, key, 90, 80)
		applyMVCCGCPlannerWrite(t, db, key, 40, 30)
	}
	applyMVCCGCPlannerLock(t, db, []byte("active-lock"), 60)

	snap := waitMVCCGCPlannerSnapshot(t, planner, func(s storemvcc.GCPlanSnapshot) bool {
		return s.LastError == "" &&
			s.LastTxnFloor.OldestStartTs == 60 &&
			s.LastPlan.ScannedKeys == 2 &&
			s.LastPlan.SafePointClampedKeys == 2
	})
	require.Equal(t, uint64(1), snap.LastTxnFloor.ActiveLocks)
	require.Equal(t, uint64(0), snap.LastPlan.DroppableWrites)
	require.Equal(t, uint64(50), snap.LastPlan.MinEffectiveSafePoint)
	require.Equal(t, uint64(60), snap.LastPlan.MaxEffectiveSafePoint)
}

func TestMVCCGCPlannerRecordsTxnFloorErrors(t *testing.T) {
	opt := newMVCCGCPlannerTestOptions(t)
	db := openMVCCGCPlannerTestDB(t, opt)
	defer func() { _ = db.Close() }()
	planner := startMVCCGCPlannerHarness(t, storemvcc.GCPlanConfig{
		MVCCStore: db,
		Interval:  5 * time.Millisecond,
		SafePoint: func() uint64 { return 100 },
	})
	defer planner.Close()

	applyMVCCGCPlannerEntry(t, db, kv.CFLock, []byte("bad-lock"), kv.MaxVersion, []byte{0xff}, 0)

	snap := waitMVCCGCPlannerSnapshot(t, planner, func(s storemvcc.GCPlanSnapshot) bool {
		return strings.Contains(s.LastError, "decode CFLock")
	})
	require.Equal(t, uint64(0), snap.LastPlan.ScannedKeys)
}
