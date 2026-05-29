// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package local

import (
	"time"

	"github.com/feichai0017/NoKV/local/stats"
	workdirmode "github.com/feichai0017/NoKV/local/workdir"
	storekv "github.com/feichai0017/NoKV/storage/kv"
	"github.com/feichai0017/NoKV/storage/vfs"
	"github.com/feichai0017/NoKV/storage/wal"
)

const (
	defaultWriteBatchMaxCount         = 64
	defaultWriteBatchMaxSize    int64 = 1 << 20
	defaultThermosTopK                = 16
	defaultBlockCacheBytes      int64 = 256 << 20
	defaultWriteThrottleMinRate int64 = 32 << 20
	defaultWriteThrottleMaxRate int64 = 128 << 20
)

// StorageBackendConfig is the local DB view of the physical ordered-KV backend.
// It is intentionally storage-engine only; NoKV MVCC keys, fsmeta layout, and
// raftstore semantics remain above the returned storage/kv.Store.
type StorageBackendConfig struct {
	Dir              string
	SyncWrites       bool
	CacheBytes       int64
	WriteBufferBytes uint64
}

// StorageBackendFactory opens one physical ordered-KV store for local.DB.
type StorageBackendFactory func(StorageBackendConfig) (storekv.Store, error)

// Options holds the top-level database configuration.
type Options struct {
	// FS provides the filesystem implementation used by DB runtime components.
	// Nil defaults to vfs.OSFS.
	FS vfs.FS

	// AllowedModes limits which workdir runtime modes Open accepts. An empty
	// allow-list means standalone-only. Cluster runtime and offline diagnostics
	// must opt into seeded/cluster directories explicitly.
	AllowedModes []workdirmode.Mode

	WorkDir string
	// StorageBackendFactory overrides the physical ordered-KV backend. Nil uses
	// the built-in Pebble adapter. Future Holt wiring should enter through this
	// hook or a storage/holt adapter that implements the same contract.
	StorageBackendFactory StorageBackendFactory
	// StorageWriteBufferBytes is the write-buffer budget passed to the selected
	// ordered-KV backend. It is not a NoKV memtable or MVCC setting.
	StorageWriteBufferBytes int64
	// MaxBatchCount bounds the number of entries grouped into one internal
	// write batch. NewDefaultOptions exposes a concrete default; Open resolves
	// zero to the constructor default when left unset.
	MaxBatchCount int64
	// MaxBatchSize bounds the size in bytes of one internal write batch.
	// NewDefaultOptions exposes a concrete default; Open resolves zero to the
	// constructor default when left unset.
	MaxBatchSize int64

	// WriteBatchMaxCount bounds how many requests the commit worker coalesces in
	// one pass. NewDefaultOptions exposes a concrete default; Open resolves zero
	// to the constructor default when left unset.
	WriteBatchMaxCount int
	// WriteBatchMaxSize bounds the byte size the commit worker coalesces in one
	// pass. NewDefaultOptions exposes a concrete default; Open resolves zero to
	// the constructor default when left unset.
	WriteBatchMaxSize int64

	DetectConflicts bool
	ThermosEnabled  bool
	ThermosBits     uint8
	ThermosTopK     int
	// ThermosRotationInterval enables dual-ring rotation for hotness tracking.
	// Zero disables rotation.
	ThermosRotationInterval time.Duration
	// ThermosNodeCap caps the number of tracked keys per ring. Zero disables the cap.
	ThermosNodeCap uint64
	// ThermosNodeSampleBits controls stable sampling once the cap is reached.
	// A value of 0 enforces a strict cap; larger values sample 1/2^N keys.
	ThermosNodeSampleBits uint8
	// ThermosDecayInterval controls how often Thermos halves its global counters.
	// Zero disables periodic decay.
	ThermosDecayInterval time.Duration
	// ThermosDecayShift determines how aggressively counters decay (count >>= shift).
	ThermosDecayShift uint32
	// ThermosWindowSlots controls the number of sliding-window buckets tracked per key.
	// Zero disables the sliding window.
	ThermosWindowSlots int
	// ThermosWindowSlotDuration sets the duration of each sliding-window bucket.
	ThermosWindowSlotDuration time.Duration
	SyncWrites                bool
	// SyncPipeline enables a dedicated sync worker goroutine that decouples
	// WAL fsync from the commit pipeline. When false (the default), the commit
	// worker performs fsync inline. Only effective when SyncWrites is true.
	SyncPipeline bool
	// WriteHotKeyLimit caps how many consecutive writes a single key can issue
	// before the DB returns utils.ErrHotKeyWriteThrottle. Zero disables write-path
	// throttling.
	WriteHotKeyLimit int32
	// WriteBatchWait adds an optional coalescing delay when the commit queue is
	// momentarily empty, letting small bursts share one WAL fsync/apply pass.
	// Zero disables the delay.
	WriteBatchWait time.Duration
	// WriteThrottleMinRate is the target write admission rate in bytes/sec when
	// slowdown pressure approaches the stop threshold. NewDefaultOptions
	// exposes a concrete default; zero lets Open resolve the constructor
	// default.
	WriteThrottleMinRate int64
	// WriteThrottleMaxRate is the target write admission rate in bytes/sec when
	// slowdown first becomes active. NewDefaultOptions exposes a concrete
	// default; zero lets Open resolve the constructor default.
	WriteThrottleMaxRate int64

	// BlockCacheBytes bounds the in-memory cache budget passed to the selected
	// ordered-KV backend.
	BlockCacheBytes int64

	// ControlLogLagWarnSegments determines how many WAL segments one replicated
	// control-log consumer can lag behind the active segment before stats
	// surfaces a warning. Zero disables the alert.
	ControlLogLagWarnSegments int64

	// EnableControlWALWatchdog enables the background WAL backlog watchdog which
	// surfaces control-log typed-record warnings and optionally runs automated
	// segment GC.
	EnableControlWALWatchdog bool
	// ControlWALBufferSize controls the size of the in-memory write buffer used by
	// control-log WAL managers. Larger buffers reduce syscall frequency at the
	// cost of memory. NewDefaultOptions exposes a concrete default; Open
	// resolves zero to the constructor default when left unset.
	ControlWALBufferSize int
	// ControlWALAutoGCInterval controls how frequently the watchdog evaluates WAL
	// backlog for automated garbage collection.
	ControlWALAutoGCInterval time.Duration
	// ControlWALAutoGCMinRemovable is the minimum number of removable WAL segments
	// required before an automated GC pass will run.
	ControlWALAutoGCMinRemovable int
	// ControlWALAutoGCMaxBatch bounds how many WAL segments are removed during a single
	// automated GC pass.
	ControlWALAutoGCMaxBatch int
	// ControlWALTypedRecordWarnRatio triggers a typed-record warning when raft records
	// constitute at least this fraction of WAL writes. Zero disables ratio-based
	// warnings.
	ControlWALTypedRecordWarnRatio float64
	// ControlWALTypedRecordWarnSegments triggers a typed-record warning when the number
	// of WAL segments containing raft records exceeds this threshold. Zero
	// disables segment-count warnings.
	ControlWALTypedRecordWarnSegments int64
	// ControlLogPointerSnapshot returns durable control-log checkpoints used by
	// control-log watchdogs, GC policy, and diagnostics. It must return a
	// detached snapshot. Nil disables control-log backlog accounting.
	ControlLogPointerSnapshot func() map[uint64]stats.ControlLogPointer
}

// NewDefaultOptions returns the default option set.
func NewDefaultOptions() *Options {
	opt := &Options{
		WorkDir:                   "./work_test",
		StorageWriteBufferBytes:   64 << 20,
		ThermosEnabled:            false,
		ThermosBits:               12,
		ThermosTopK:               defaultThermosTopK,
		ThermosRotationInterval:   30 * time.Minute,
		ThermosNodeCap:            250_000,
		ThermosNodeSampleBits:     0,
		ThermosDecayInterval:      0,
		ThermosDecayShift:         0,
		ThermosWindowSlots:        8,
		ThermosWindowSlotDuration: 250 * time.Millisecond,
		// Conservative defaults to avoid long batch-induced pauses.
		WriteBatchMaxCount:                defaultWriteBatchMaxCount,
		WriteBatchMaxSize:                 defaultWriteBatchMaxSize,
		MaxBatchCount:                     defaultWriteBatchMaxCount,
		MaxBatchSize:                      defaultWriteBatchMaxSize,
		BlockCacheBytes:                   defaultBlockCacheBytes,
		SyncWrites:                        false,
		WriteHotKeyLimit:                  128,
		WriteBatchWait:                    200 * time.Microsecond,
		WriteThrottleMinRate:              defaultWriteThrottleMinRate,
		WriteThrottleMaxRate:              defaultWriteThrottleMaxRate,
		ControlLogLagWarnSegments:         8,
		EnableControlWALWatchdog:          true,
		ControlWALBufferSize:              wal.DefaultBufferSize,
		ControlWALAutoGCInterval:          15 * time.Second,
		ControlWALAutoGCMinRemovable:      1,
		ControlWALAutoGCMaxBatch:          4,
		ControlWALTypedRecordWarnRatio:    0.35,
		ControlWALTypedRecordWarnSegments: 6,
	}
	return opt
}

// resolveOpenDefaults resolves constructor-owned defaults once at the DB
// boundary. Zero remains meaningful for settings that explicitly use zero to
// disable a feature.
func (opt *Options) resolveOpenDefaults() {
	if opt == nil {
		return
	}
	if opt.ThermosTopK <= 0 {
		opt.ThermosTopK = defaultThermosTopK
	}
	if opt.WriteBatchMaxCount <= 0 {
		opt.WriteBatchMaxCount = defaultWriteBatchMaxCount
	}
	if opt.WriteBatchMaxSize <= 0 {
		opt.WriteBatchMaxSize = defaultWriteBatchMaxSize
	}
	if opt.MaxBatchCount <= 0 {
		opt.MaxBatchCount = int64(opt.WriteBatchMaxCount)
	}
	if opt.MaxBatchSize <= 0 {
		opt.MaxBatchSize = opt.WriteBatchMaxSize
	}
	if opt.WriteBatchWait < 0 {
		opt.WriteBatchWait = 0
	}
	opt.normalizeStorageOptions()
	if opt.ControlWALBufferSize <= 0 {
		opt.ControlWALBufferSize = wal.DefaultBufferSize
	}
}

func (opt *Options) normalizeStorageOptions() {
	if opt == nil {
		return
	}
	if opt.BlockCacheBytes <= 0 {
		opt.BlockCacheBytes = defaultBlockCacheBytes
	}
	if opt.WriteThrottleMinRate <= 0 {
		opt.WriteThrottleMinRate = defaultWriteThrottleMinRate
	}
	if opt.WriteThrottleMaxRate <= 0 || opt.WriteThrottleMaxRate < opt.WriteThrottleMinRate {
		opt.WriteThrottleMaxRate = max(opt.WriteThrottleMinRate, defaultWriteThrottleMaxRate)
	}
}
