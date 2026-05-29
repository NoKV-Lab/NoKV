// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package stats owns periodic runtime metric collection and snapshot
// publication for local.DB. The Stats type runs a small ticker that builds a
// StatsSnapshot from its Host and republishes it through the stable product
// expvar name "NoKV.Local.Stats". The local package provides Stats with a Host
// implementation; tests construct mock Hosts directly.
package stats

import (
	"expvar"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/experimental/thermos"
	"github.com/feichai0017/NoKV/metrics"
	rawkv "github.com/feichai0017/NoKV/storage/kv"
	"github.com/feichai0017/NoKV/storage/wal"
	"github.com/feichai0017/NoKV/utils"
)

// Host wires the Stats subsystem back into its DB host. Every accessor
// is read-only; Stats never mutates host state.
type Host interface {
	// ControlWALsLocked invokes fn while holding the host's control-WAL mutex.
	// Stats only iterates the slice while the lock is held.
	ControlWALsLocked(fn func(wals []*wal.Manager))
	BackgroundWatchdogs() []*wal.Watchdog
	StorageStats() rawkv.Stats
	HotWrite() *thermos.RotatingThermos
	IteratorReused() uint64
	WriteMetrics() *metrics.WriteMetrics

	// Atomic indicators of write throttling state.
	BlockWritesActive() bool
	SlowWritesActive() bool
	HotWriteLimited() uint64

	// Options-snapshot accessors.
	ControlLogLagWarnSegments() int64
	WALTypedRecordWarnRatio() float64
	WALTypedRecordWarnSegments() int64
	ThermosTopK() int
	ControlLogPointerSnapshot() func() map[uint64]ControlLogPointer
	MVCCGCStatsSnapshot() MVCCGCStatsSnapshot
	TransportMetrics() metrics.GRPCTransportMetrics
}

// Stats owns periodic runtime metric collection and snapshot publication.
type Stats struct {
	host     Host
	closer   *utils.Closer
	once     sync.Once
	interval time.Duration

	regionMetrics atomic.Pointer[metrics.RegionMetrics]
}

var (
	statsExpvarOnce       sync.Once
	exportedStatsSnapshot atomic.Pointer[StatsSnapshot]
)

// HotKeyStat represents one hot key and its observed touch count.
type HotKeyStat struct {
	Key   string `json:"key"`
	Count int32  `json:"count"`
}

// ControlLogPointer is the local stats view of a replicated control-log checkpoint.
type ControlLogPointer struct {
	Segment      uint32
	SegmentIndex uint64
}

// LSMLevelStats captures aggregated metrics per LSM level.
type LSMLevelStats struct {
	Level                       int     `json:"level"`
	TableCount                  int     `json:"tables"`
	SizeBytes                   int64   `json:"size_bytes"`
	ValueBytes                  int64   `json:"value_bytes"`
	StaleBytes                  int64   `json:"stale_bytes"`
	LandingTables               int     `json:"landing_tables"`
	LandingSizeBytes            int64   `json:"landing_size_bytes"`
	LandingValueBytes           int64   `json:"landing_value_bytes"`
	ValueDensity                float64 `json:"value_density"`
	LandingValueDensity         float64 `json:"landing_value_density"`
	LandingRuns                 int64   `json:"landing_runs"`
	LandingMs                   float64 `json:"landing_ms"`
	LandingTablesCompactedCount int64   `json:"landing_tables_compacted"`
	MergeRuns                   int64   `json:"landing_merge_runs"`
	MergeMs                     float64 `json:"landing_merge_ms"`
	MergeTables                 int64   `json:"landing_merge_tables"`
}

// StatsSnapshot captures a point-in-time view of internal backlog metrics.
type StatsSnapshot struct {
	Entries    int64                        `json:"entries"`
	Flush      FlushStatsSnapshot           `json:"flush"`
	Compaction CompactionStatsSnapshot      `json:"compaction"`
	WAL        WALStatsSnapshot             `json:"wal"`
	Raft       RaftStatsSnapshot            `json:"raft"`
	Write      WriteStatsSnapshot           `json:"write"`
	Region     RegionStatsSnapshot          `json:"region"`
	MVCCGC     MVCCGCStatsSnapshot          `json:"mvcc_gc"`
	Hot        HotStatsSnapshot             `json:"hot"`
	Cache      CacheStatsSnapshot           `json:"cache"`
	LSM        LSMStatsSnapshot             `json:"lsm"`
	Transport  metrics.GRPCTransportMetrics `json:"transport"`
}

// FlushStatsSnapshot summarizes flush queue depth and stage timing.
type FlushStatsSnapshot struct {
	Pending       int64   `json:"pending"`
	QueueLength   int64   `json:"queue_length"`
	Active        int64   `json:"active"`
	WaitMs        float64 `json:"wait_ms"`
	LastWaitMs    float64 `json:"last_wait_ms"`
	MaxWaitMs     float64 `json:"max_wait_ms"`
	BuildMs       float64 `json:"build_ms"`
	LastBuildMs   float64 `json:"last_build_ms"`
	MaxBuildMs    float64 `json:"max_build_ms"`
	ReleaseMs     float64 `json:"release_ms"`
	LastReleaseMs float64 `json:"last_release_ms"`
	MaxReleaseMs  float64 `json:"max_release_ms"`
	Completed     int64   `json:"completed"`
}

// CompactionStatsSnapshot summarizes compaction backlog, runtime, and landing behavior.
type CompactionStatsSnapshot struct {
	Backlog              int64   `json:"backlog"`
	MaxScore             float64 `json:"max_score"`
	LastDurationMs       float64 `json:"last_duration_ms"`
	MaxDurationMs        float64 `json:"max_duration_ms"`
	Runs                 uint64  `json:"runs"`
	LandingRuns          int64   `json:"landing_runs"`
	MergeRuns            int64   `json:"landing_merge_runs"`
	LandingMs            float64 `json:"landing_ms"`
	MergeMs              float64 `json:"landing_merge_ms"`
	LandingTables        int64   `json:"landing_tables"`
	MergeTables          int64   `json:"landing_merge_tables"`
	ValueWeight          float64 `json:"value_weight"`
	ValueWeightSuggested float64 `json:"value_weight_suggested,omitempty"`
}

// WALStatsSnapshot captures WAL head position, record mix, and watchdog status.
type WALStatsSnapshot struct {
	ActiveSegment           int64             `json:"active_segment"`
	SegmentCount            int64             `json:"segment_count"`
	ActiveSize              int64             `json:"active_size"`
	SegmentsRemoved         uint64            `json:"segments_removed"`
	RecordCounts            wal.RecordMetrics `json:"record_counts"`
	SegmentsWithRaftRecords int               `json:"segments_with_raft_records"`
	RemovableRaftSegments   int               `json:"removable_raft_segments"`
	TypedRecordRatio        float64           `json:"typed_record_ratio"`
	TypedRecordWarning      bool              `json:"typed_record_warning"`
	TypedRecordReason       string            `json:"typed_record_reason,omitempty"`
	AutoGCRuns              uint64            `json:"auto_gc_runs"`
	AutoGCRemoved           uint64            `json:"auto_gc_removed"`
	AutoGCLastUnix          int64             `json:"auto_gc_last_unix"`
}

// RaftStatsSnapshot summarizes raft log lag across tracked groups.
type RaftStatsSnapshot struct {
	GroupCount       int    `json:"group_count"`
	LaggingGroups    int    `json:"lagging_groups"`
	MinLogSegment    uint32 `json:"min_log_segment"`
	MaxLogSegment    uint32 `json:"max_log_segment"`
	MaxLagSegments   int64  `json:"max_lag_segments"`
	LagWarnThreshold int64  `json:"lag_warn_threshold"`
	LagWarning       bool   `json:"lag_warning"`
}

// WriteStatsSnapshot tracks write-path queue pressure, latency, and throttling.
type WriteStatsSnapshot struct {
	QueueDepth       int64   `json:"queue_depth"`
	QueueEntries     int64   `json:"queue_entries"`
	QueueBytes       int64   `json:"queue_bytes"`
	AvgBatchEntries  float64 `json:"avg_batch_entries"`
	AvgBatchBytes    float64 `json:"avg_batch_bytes"`
	AvgRequestWaitMs float64 `json:"avg_request_wait_ms"`
	AvgApplyMs       float64 `json:"avg_apply_ms"`
	AvgSyncMs        float64 `json:"avg_sync_ms"`
	AvgSyncBatch     float64 `json:"avg_sync_batch"`
	SyncCount        int64   `json:"sync_count"`
	BatchesTotal     int64   `json:"batches_total"`
	ThrottleActive   bool    `json:"throttle_active"`
	SlowdownActive   bool    `json:"slowdown_active"`
	ThrottleMode     string  `json:"throttle_mode"`
	ThrottlePressure uint32  `json:"throttle_pressure_permille"`
	ThrottleRate     uint64  `json:"throttle_rate_bytes_per_sec"`
	HotKeyLimited    uint64  `json:"hot_key_limited"`
}

// RegionStatsSnapshot reports region counts grouped by region state.
type RegionStatsSnapshot struct {
	Total     int64 `json:"total"`
	New       int64 `json:"new"`
	Running   int64 `json:"running"`
	Removing  int64 `json:"removing"`
	Tombstone int64 `json:"tombstone"`
	Other     int64 `json:"other"`
}

// MVCCGCStatsSnapshot reports read-only MVCC GC planning and replicated
// maintenance state.
type MVCCGCStatsSnapshot struct {
	Enabled               bool    `json:"enabled"`
	Runs                  uint64  `json:"runs"`
	SkippedRuns           uint64  `json:"skipped_runs,omitempty"`
	LastUnix              int64   `json:"last_unix"`
	LastDurationMs        float64 `json:"last_duration_ms"`
	LastError             string  `json:"last_error,omitempty"`
	ActiveLocks           uint64  `json:"active_locks"`
	OldestStartTs         uint64  `json:"oldest_start_ts"`
	MaxStartTs            uint64  `json:"max_start_ts"`
	ScannedKeys           uint64  `json:"scanned_keys"`
	DroppableKeys         uint64  `json:"droppable_keys"`
	WriteVersions         uint64  `json:"write_versions"`
	RetainedWrites        uint64  `json:"retained_writes"`
	DroppableWrites       uint64  `json:"droppable_writes"`
	AnchorWrites          uint64  `json:"anchor_writes"`
	RetainedDefaultRefs   uint64  `json:"retained_default_refs"`
	DeletedWriteMarkers   uint64  `json:"deleted_write_markers"`
	SafePointClampedKeys  uint64  `json:"safe_point_clamped_keys"`
	MaxVersionsPerKey     uint64  `json:"max_versions_per_key"`
	MinEffectiveSafePoint uint64  `json:"min_effective_safe_point"`
	MaxEffectiveSafePoint uint64  `json:"max_effective_safe_point"`

	MaintenanceEnabled          bool    `json:"maintenance_enabled"`
	MaintenanceRuns             uint64  `json:"maintenance_runs"`
	MaintenanceLastUnix         int64   `json:"maintenance_last_unix"`
	MaintenanceLastDurationMs   float64 `json:"maintenance_last_duration_ms"`
	MaintenanceLastError        string  `json:"maintenance_last_error,omitempty"`
	MaintenanceResolveError     string  `json:"maintenance_resolve_error,omitempty"`
	MaintenanceApplyError       string  `json:"maintenance_apply_error,omitempty"`
	MaintenanceOrphanError      string  `json:"maintenance_orphan_error,omitempty"`
	MaintenanceSafePointSkipped bool    `json:"maintenance_safe_point_skipped,omitempty"`
	ScannedLocks                uint64  `json:"scanned_locks"`
	ExpiredLocks                uint64  `json:"expired_locks"`
	ResolvedLocks               uint64  `json:"resolved_locks"`
	CommittedLocks              uint64  `json:"committed_locks"`
	RolledBackLocks             uint64  `json:"rolled_back_locks"`
	DeletedLockMarkers          uint64  `json:"deleted_lock_markers"`
	AppliedWriteDeletes         uint64  `json:"applied_write_deletes"`
	AppliedDefaultDeletes       uint64  `json:"applied_default_deletes"`
	OrphanDefaults              uint64  `json:"orphan_defaults"`
	AppliedOrphanDefaults       uint64  `json:"applied_orphan_defaults"`
}

// HotStatsSnapshot contains write-hot keys and optional ring internals.
type HotStatsSnapshot struct {
	WriteKeys []HotKeyStat   `json:"write_keys,omitempty"`
	WriteRing *thermos.Stats `json:"write_ring,omitempty"`
}

// CacheStatsSnapshot captures block/index/bloom hit-rate indicators.
type CacheStatsSnapshot struct {
	BlockL0HitRate float64 `json:"block_l0_hit_rate"`
	BlockL1HitRate float64 `json:"block_l1_hit_rate"`
	IndexHitRate   float64 `json:"index_hit_rate"`
	IteratorReused uint64  `json:"iterator_reused"`
}

// LSMStatsSnapshot summarizes per-level storage shape and value-density signals.
type LSMStatsSnapshot struct {
	Levels            []LSMLevelStats               `json:"levels,omitempty"`
	ValueBytesTotal   int64                         `json:"value_bytes_total"`
	ValueDensityMax   float64                       `json:"value_density_max"`
	ValueDensityAlert bool                          `json:"value_density_alert"`
	RangeFilter       RangeFilterStatsSnapshot      `json:"range_filter"`
	Mmap              metrics.MmapAdviceSnapshot    `json:"mmap"`
	Prefetch          metrics.TablePrefetchSnapshot `json:"prefetch"`
}

// RangeFilterStatsSnapshot summarizes range-filter pruning activity on read paths.
type RangeFilterStatsSnapshot struct {
	PointCandidates   uint64 `json:"point_candidates"`
	PointPruned       uint64 `json:"point_pruned"`
	BoundedCandidates uint64 `json:"bounded_candidates"`
	BoundedPruned     uint64 `json:"bounded_pruned"`
	Fallbacks         uint64 `json:"fallbacks"`
}

// New constructs a Stats wired to host. interval defaults to 5s when 0.
func New(host Host, interval time.Duration) *Stats {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	s := &Stats{
		host:     host,
		closer:   utils.NewCloser(),
		interval: interval,
	}
	statsExpvarOnce.Do(func() {
		expvar.Publish("NoKV.Local.Stats", expvar.Func(func() any {
			if ptr := exportedStatsSnapshot.Load(); ptr != nil {
				return *ptr
			}
			return StatsSnapshot{}
		}))
	})
	return s
}

// StartStats runs periodic collection of internal backlog metrics.
func (s *Stats) StartStats() {
	if s == nil {
		return
	}
	s.once.Do(func() {
		s.closer.Add(1)
		go s.run()
	})
}

// Close stops the stats loop.
func (s *Stats) Close() error {
	if s == nil {
		return nil
	}
	s.closer.Close()
	return nil
}

// SetRegionMetrics attaches region metrics recorder used in snapshots.
func (s *Stats) SetRegionMetrics(rm *metrics.RegionMetrics) {
	if s == nil {
		return
	}
	s.regionMetrics.Store(rm)
}

func (s *Stats) run() {
	defer s.closer.Done()

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	// Collect once at startup so expvar has values immediately.
	s.Collect()

	for {
		select {
		case <-ticker.C:
			s.Collect()
		case <-s.closer.Closed():
			return
		}
	}
}

// Collect snapshots background queues and propagates them to expvar.
func (s *Stats) Collect() {
	if s == nil {
		return
	}
	snap := s.Snapshot()
	exportedStatsSnapshot.Store(&snap)
}

// Snapshot returns a point-in-time metrics snapshot without mutating state.
func (s *Stats) Snapshot() StatsSnapshot {
	var snap StatsSnapshot
	if s == nil || s.host == nil {
		return snap
	}

	if thresh := s.host.ControlLogLagWarnSegments(); thresh > 0 {
		snap.Raft.LagWarnThreshold = thresh
	}
	snap.Entries = int64(s.host.StorageStats().KeysEstimate)

	snap.LSM.Mmap = metrics.MmapAdviceStats()
	snap.LSM.Prefetch = metrics.TablePrefetchStats()

	if wm := s.host.WriteMetrics(); wm != nil {
		wsnap := wm.Snapshot()
		snap.Write.QueueDepth = wsnap.QueueLen
		snap.Write.QueueEntries = wsnap.QueueEntries
		snap.Write.QueueBytes = wsnap.QueueBytes
		snap.Write.AvgBatchEntries = wsnap.AvgBatchEntries
		snap.Write.AvgBatchBytes = wsnap.AvgBatchBytes
		snap.Write.AvgRequestWaitMs = wsnap.AvgRequestWaitMs
		snap.Write.AvgApplyMs = wsnap.AvgApplyMs
		snap.Write.AvgSyncMs = wsnap.AvgSyncMs
		snap.Write.AvgSyncBatch = wsnap.AvgSyncBatch
		snap.Write.SyncCount = wsnap.SyncSamples
		snap.Write.BatchesTotal = wsnap.Batches
	}
	stopActive := s.host.BlockWritesActive()
	slowActive := s.host.SlowWritesActive()
	snap.Write.ThrottleActive = stopActive || slowActive
	snap.Write.SlowdownActive = slowActive
	switch {
	case stopActive:
		snap.Write.ThrottleMode = "stop"
	case slowActive:
		snap.Write.ThrottleMode = "slowdown"
	default:
		snap.Write.ThrottleMode = "none"
	}
	if stopActive && snap.Write.ThrottlePressure == 0 {
		snap.Write.ThrottlePressure = 1000
	} else if slowActive && snap.Write.ThrottlePressure == 0 {
		snap.Write.ThrottlePressure = 1
	}
	snap.Write.HotKeyLimited = s.host.HotWriteLimited()

	if rm := s.regionMetrics.Load(); rm != nil {
		rms := rm.Snapshot()
		snap.Region.Total = int64(rms.Total)
		snap.Region.New = int64(rms.New)
		snap.Region.Running = int64(rms.Running)
		snap.Region.Removing = int64(rms.Removing)
		snap.Region.Tombstone = int64(rms.Tombstone)
		snap.Region.Other = int64(rms.Other)
	}

	snap.MVCCGC = s.host.MVCCGCStatsSnapshot()

	var ptrs map[uint64]ControlLogPointer
	if ptrFn := s.host.ControlLogPointerSnapshot(); ptrFn != nil {
		ptrs = ptrFn()
		snap.Raft.GroupCount = len(ptrs)
	}

	removableRaftSegments := 0
	s.host.ControlWALsLocked(func(wals []*wal.Manager) {
		for _, mgr := range wals {
			if mgr == nil {
				continue
			}
			shardStats := mgr.Metrics()
			shardSegments := mgr.SegmentMetrics()
			shardAnalysis := metrics.AnalyzeWALBacklog(shardStats, shardSegments)
			snap.WAL.RecordCounts.Entries += shardAnalysis.RecordCounts.Entries
			snap.WAL.RecordCounts.RaftEntries += shardAnalysis.RecordCounts.RaftEntries
			snap.WAL.RecordCounts.RaftStates += shardAnalysis.RecordCounts.RaftStates
			snap.WAL.RecordCounts.RaftSnapshots += shardAnalysis.RecordCounts.RaftSnapshots
			snap.WAL.RecordCounts.Other += shardAnalysis.RecordCounts.Other
			snap.WAL.SegmentsWithRaftRecords += shardAnalysis.SegmentsWithRaft
			if shardStats != nil {
				snap.WAL.SegmentCount += int64(shardStats.SegmentCount)
				snap.WAL.SegmentsRemoved += shardStats.RemovedSegments
			}
			for _, id := range shardAnalysis.RemovableSegments {
				if shardSegments[id].RaftRecords() > 0 && shardStats != nil && id < shardStats.ActiveSegment {
					removableRaftSegments++
				}
			}
		}
	})
	snap.WAL.RemovableRaftSegments = removableRaftSegments
	if total := snap.WAL.RecordCounts.Total(); total > 0 {
		raftRecords := snap.WAL.RecordCounts.RaftRecords()
		snap.WAL.TypedRecordRatio = float64(raftRecords) / float64(total)
	}

	if len(ptrs) > 0 {
		var minSeg uint32
		var maxSeg uint32
		var maxLag int64
		lagging := 0
		effectiveActive := snap.WAL.ActiveSegment
		if snap.WAL.ActiveSize == 0 && effectiveActive > 0 {
			effectiveActive--
		}
		for _, ptr := range ptrs {
			if ptr.Segment == 0 {
				lagging++
				if effectiveActive > maxLag {
					maxLag = effectiveActive
				}
				continue
			}
			if minSeg == 0 || ptr.Segment < minSeg {
				minSeg = ptr.Segment
			}
			if ptr.Segment > maxSeg {
				maxSeg = ptr.Segment
			}
			if effectiveActive > 0 {
				lag := max(effectiveActive-int64(ptr.Segment), 0)
				if lag > 0 {
					lagging++
				}
				if lag > maxLag {
					maxLag = lag
				}
			}
		}
		snap.Raft.MinLogSegment = minSeg
		snap.Raft.MaxLogSegment = maxSeg
		snap.Raft.MaxLagSegments = maxLag
		snap.Raft.LaggingGroups = lagging
	}
	threshold := max(s.host.ControlLogLagWarnSegments(), 0)
	snap.Raft.LagWarnThreshold = threshold
	if threshold > 0 && snap.Raft.MaxLagSegments >= threshold && snap.Raft.LaggingGroups > 0 {
		snap.Raft.LagWarning = true
	}

	warning, reason := metrics.WALTypedWarning(snap.WAL.TypedRecordRatio, snap.WAL.SegmentsWithRaftRecords, s.host.WALTypedRecordWarnRatio(), s.host.WALTypedRecordWarnSegments())
	watchdogs := s.host.BackgroundWatchdogs()
	if len(watchdogs) > 0 {
		var anyWarn bool
		var warnReason string
		for _, watchdog := range watchdogs {
			if watchdog == nil {
				continue
			}
			wsnap := watchdog.Snapshot()
			snap.WAL.AutoGCRuns += wsnap.AutoRuns
			snap.WAL.AutoGCRemoved += wsnap.SegmentsRemoved
			if wsnap.LastAutoUnix > snap.WAL.AutoGCLastUnix {
				snap.WAL.AutoGCLastUnix = wsnap.LastAutoUnix
			}
			if wsnap.Warning && !anyWarn {
				anyWarn = true
				warnReason = wsnap.WarningReason
			}
		}
		if anyWarn {
			snap.WAL.TypedRecordWarning = true
			snap.WAL.TypedRecordReason = warnReason
		} else if warning {
			snap.WAL.TypedRecordWarning = true
			snap.WAL.TypedRecordReason = reason
		}
	} else if warning {
		snap.WAL.TypedRecordWarning = true
		snap.WAL.TypedRecordReason = reason
	}

	if hot := s.host.HotWrite(); hot != nil {
		topK := s.host.ThermosTopK()
		for _, item := range hot.TopN(topK) {
			snap.Hot.WriteKeys = append(snap.Hot.WriteKeys, HotKeyStat{Key: item.Key, Count: item.Count})
		}
		hotStats := hot.Stats()
		snap.Hot.WriteRing = &hotStats
	}
	snap.Cache.IteratorReused = s.host.IteratorReused()
	snap.Transport = s.host.TransportMetrics()
	return snap
}
