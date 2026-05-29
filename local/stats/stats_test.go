// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package stats_test

// External-test integration tests for the Stats subsystem, exercised
// through the local.DB facade. Helpers are intentionally inlined:
// shared db_test.go fixtures live in the local package and an external
// test package can't see unexported identifiers.

import (
	"encoding/json"
	"expvar"
	"testing"

	local "github.com/feichai0017/NoKV/local"
	"github.com/feichai0017/NoKV/local/internal/commit"
	"github.com/feichai0017/NoKV/local/stats"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	"github.com/feichai0017/NoKV/metrics"
	"github.com/stretchr/testify/require"
)

func openTestDB(t testing.TB, opt *local.Options) *local.DB {
	t.Helper()
	db, err := local.Open(opt)
	require.NoError(t, err)
	return db
}

func newTestOptions(t *testing.T) *local.Options {
	t.Helper()
	opt := local.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	opt.MemTableSize = 1 << 12
	opt.MaxBatchCount = 10
	opt.MaxBatchSize = 1 << 20
	opt.DetectConflicts = true
	return opt
}

func TestStatsCollectSnapshots(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	rm := metrics.NewRegionMetrics()
	db.SetRegionMetrics(rm)
	rm.RecordState(1, metaregion.ReplicaStateRunning)
	rm.RecordState(2, metaregion.ReplicaStateRemoving)
	mmapBefore := metrics.MmapAdviceStats()
	prefetchBefore := metrics.TablePrefetchStats()
	metrics.RecordMmapMadvise(true)
	metrics.RecordMmapMadvise(false)
	metrics.RecordTablePrefetchLaunched()
	metrics.RecordTablePrefetchCompleted()
	metrics.RecordTablePrefetchAborted()

	require.NoError(t, db.Set([]byte("stats-key"), []byte("stats-value")))
	entry, err := db.Get([]byte("stats-key"))
	require.NoError(t, err)
	require.Equal(t, []byte("stats-value"), entry.Value)

	snap := db.Info().Snapshot()
	require.Empty(t, snap.Hot.WriteKeys)
	require.False(t, snap.WAL.TypedRecordWarning)
	require.Equal(t, uint64(0), snap.WAL.AutoGCRuns)
	require.Equal(t, uint64(0), snap.WAL.AutoGCRemoved)
	require.Greater(t, snap.Write.BatchesTotal, int64(0))
	require.False(t, snap.Write.ThrottleActive)
	require.Equal(t, db.IteratorReused(), snap.Cache.IteratorReused)
	require.GreaterOrEqual(t, snap.Storage.Mmap.Madvise, mmapBefore.Madvise+1)
	require.GreaterOrEqual(t, snap.Storage.Mmap.MadviseFailed, mmapBefore.MadviseFailed+1)
	require.GreaterOrEqual(t, snap.Storage.Prefetch.Launched, prefetchBefore.Launched+1)
	require.GreaterOrEqual(t, snap.Storage.Prefetch.Completed, prefetchBefore.Completed+1)
	require.GreaterOrEqual(t, snap.Storage.Prefetch.Aborted, prefetchBefore.Aborted+1)

	require.Equal(t, int64(2), snap.Region.Total)
	require.Equal(t, int64(1), snap.Region.Running)
	require.Equal(t, int64(1), snap.Region.Removing)

	db.Info().Collect()
	exported := loadExpvarStatsSnapshot(t)
	require.Equal(t, snap.Storage, exported.Storage)
	require.Equal(t, snap.Write.BatchesTotal, exported.Write.BatchesTotal)
	require.Equal(t, snap.Region.Total, exported.Region.Total)

	// Legacy scalar keys are intentionally removed.
	require.Nil(t, expvar.Get("NoKV.Local.Stats.Flush.Pending"))
	require.Nil(t, expvar.Get("NoKV.Local.Stats.WAL.ActiveSegment"))
	require.Nil(t, expvar.Get("NoKV.Txns.Active"))
	require.Nil(t, expvar.Get("NoKV.Stats"))
	require.Nil(t, expvar.Get("NoKV.Mmap.Madvise"))
	require.Nil(t, expvar.Get("NoKV.Mmap.MadviseFailed"))
	require.Nil(t, expvar.Get("NoKV.Prefetch.Launched"))
	require.Nil(t, expvar.Get("NoKV.Prefetch.Aborted"))
	require.Nil(t, expvar.Get("NoKV.Prefetch.Completed"))
}

func TestStatsSnapshotTracksThrottleAndWalRemovals(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	require.NoError(t, db.Set([]byte("wal-metrics"), []byte("value")))
	db.ApplyThrottle(commit.WriteThrottleStop)
	defer db.ApplyThrottle(commit.WriteThrottleNone)

	snap := db.Info().Snapshot()
	require.True(t, snap.Write.ThrottleActive)
	require.Equal(t, "stop", snap.Write.ThrottleMode)
	require.Equal(t, uint32(1000), snap.Write.ThrottlePressure)
	require.Equal(t, uint64(0), snap.Write.ThrottleRate)

	db.Info().Collect()
	exported := loadExpvarStatsSnapshot(t)
	require.True(t, exported.Write.ThrottleActive)
	require.Equal(t, "stop", exported.Write.ThrottleMode)
	require.Equal(t, uint32(1000), exported.Write.ThrottlePressure)
	require.Equal(t, uint64(0), exported.Write.ThrottleRate)

	db.ApplyThrottle(commit.WriteThrottleNone)
	snapAfter := db.Info().Snapshot()
	require.False(t, snapAfter.Write.ThrottleActive)
	require.Equal(t, "none", snapAfter.Write.ThrottleMode)
	require.Equal(t, uint32(0), snapAfter.Write.ThrottlePressure)
	require.Equal(t, uint64(0), snapAfter.Write.ThrottleRate)

	db.Info().Collect()
	exportedAfter := loadExpvarStatsSnapshot(t)
	require.False(t, exportedAfter.Write.ThrottleActive)

	// Legacy scalar key should remain absent.
	require.Nil(t, expvar.Get("NoKV.Local.Stats.Write.Throttle"))
}

func TestStatsSnapshotIncludesMVCCGCSource(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()
	db.SetMVCCGCStatsSnapshotSource(func() stats.MVCCGCStatsSnapshot {
		return stats.MVCCGCStatsSnapshot{
			Enabled:                     true,
			Runs:                        1,
			ScannedKeys:                 1,
			DroppableWrites:             1,
			WriteVersions:               3,
			AnchorWrites:                1,
			MaxVersionsPerKey:           3,
			MaxEffectiveSafePoint:       100,
			MaintenanceEnabled:          true,
			MaintenanceRuns:             2,
			MaintenanceResolveError:     "resolve warn",
			MaintenanceSafePointSkipped: true,
			ResolvedLocks:               3,
			AppliedWriteDeletes:         7,
			AppliedOrphanDefaults:       10,
		}
	})

	snap := db.Info().Snapshot()
	require.True(t, snap.MVCCGC.Enabled)
	require.Equal(t, uint64(1), snap.MVCCGC.Runs)
	require.Equal(t, uint64(1), snap.MVCCGC.ScannedKeys)
	require.Equal(t, uint64(3), snap.MVCCGC.WriteVersions)
	require.Equal(t, uint64(1), snap.MVCCGC.AnchorWrites)
	require.Equal(t, uint64(3), snap.MVCCGC.MaxVersionsPerKey)
	require.Equal(t, uint64(100), snap.MVCCGC.MaxEffectiveSafePoint)
	require.True(t, snap.MVCCGC.MaintenanceEnabled)
	require.Equal(t, uint64(2), snap.MVCCGC.MaintenanceRuns)
	require.Equal(t, "resolve warn", snap.MVCCGC.MaintenanceResolveError)
	require.True(t, snap.MVCCGC.MaintenanceSafePointSkipped)
	require.Equal(t, uint64(3), snap.MVCCGC.ResolvedLocks)
	require.Equal(t, uint64(7), snap.MVCCGC.AppliedWriteDeletes)
	require.Equal(t, uint64(10), snap.MVCCGC.AppliedOrphanDefaults)

	db.Info().Collect()
	exported := loadExpvarStatsSnapshot(t)
	require.Equal(t, snap.MVCCGC.Enabled, exported.MVCCGC.Enabled)
	require.Equal(t, snap.MVCCGC.Runs, exported.MVCCGC.Runs)
	require.Equal(t, snap.MVCCGC.DroppableWrites, exported.MVCCGC.DroppableWrites)
	require.Equal(t, snap.MVCCGC.MaxVersionsPerKey, exported.MVCCGC.MaxVersionsPerKey)
	require.Equal(t, snap.MVCCGC.MaintenanceRuns, exported.MVCCGC.MaintenanceRuns)
	require.Equal(t, snap.MVCCGC.MaintenanceResolveError, exported.MVCCGC.MaintenanceResolveError)
	require.Equal(t, snap.MVCCGC.MaintenanceSafePointSkipped, exported.MVCCGC.MaintenanceSafePointSkipped)
	require.Equal(t, snap.MVCCGC.ResolvedLocks, exported.MVCCGC.ResolvedLocks)
	require.Equal(t, snap.MVCCGC.AppliedWriteDeletes, exported.MVCCGC.AppliedWriteDeletes)
}

func loadExpvarStatsSnapshot(t *testing.T) stats.StatsSnapshot {
	t.Helper()

	v := expvar.Get("NoKV.Local.Stats")
	require.NotNil(t, v)

	var snap stats.StatsSnapshot
	require.NoError(t, json.Unmarshal([]byte(v.String()), &snap))
	return snap
}
