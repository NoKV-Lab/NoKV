// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package ycsb

import (
	"os"
	"runtime"
	"strconv"

	"github.com/cockroachdb/pebble"
	badger "github.com/dgraph-io/badger/v4"
	badgeropts "github.com/dgraph-io/badger/v4/options"
	local "github.com/feichai0017/NoKV/local"
)

const (
	ycsbDefaultTotalCacheMB = 512

	ycsbNoKVWriteBatchMaxCount = 10_000
	ycsbNoKVWriteBatchMaxSize  = 128 << 20

	ycsbBadgerNumVersionsToKeep       = 1
	ycsbBadgerNumMemtables            = 5
	ycsbBadgerNumLevelZeroTables      = 5
	ycsbBadgerNumLevelZeroTablesStall = 15
	ycsbBadgerNumCompactors           = 4
	ycsbBadgerLevelSizeMultiplier     = 10
	ycsbBadgerTableSizeMultiplier     = 2
	ycsbBadgerMaxLevels               = 7
	ycsbBadgerBlockSize               = 4 << 10
	ycsbBadgerBloomFalsePositive      = 0.01
	ycsbBadgerValueLogMaxEntries      = 1_000_000
	ycsbBadgerZSTDCompressionLevel    = 1
	ycsbBadgerNamespaceOffset         = -1
	ycsbBadgerBaseLevelMultiplier     = 5
	ycsbBadgerDefaultTableSize        = 2 << 20
	ycsbBadgerDefaultValueLogSize     = (1 << 30) - 1
)

func cacheBudgetBytes(mb int) int64 {
	if mb <= 0 {
		return 0
	}
	return int64(mb) << 20
}

func normalizeTotalCacheMB(totalMB int) int {
	if totalMB <= 0 {
		return ycsbDefaultTotalCacheMB
	}
	return totalMB
}

func defaultNoKVCacheBudgetMB(totalMB int) (blockMB, indexMB int) {
	totalMB = normalizeTotalCacheMB(totalMB)
	indexMB = totalMB / 4
	blockMB = totalMB - indexMB
	return blockMB, indexMB
}

func resolveNoKVCacheBudgetMB(totalMB, explicitIndexMB int) (blockMB, indexMB int) {
	blockMB, indexMB = defaultNoKVCacheBudgetMB(totalMB)
	if explicitIndexMB > 0 {
		indexMB = explicitIndexMB
	}
	if explicitIndexMB > 0 {
		blockMB = normalizeTotalCacheMB(totalMB) - indexMB
		if blockMB < 0 {
			blockMB = 0
		}
	}
	return blockMB, indexMB
}

func defaultBadgerCacheBudgetMB(totalMB int) (blockMB, indexMB int) {
	totalMB = normalizeTotalCacheMB(totalMB)
	blockMB = totalMB / 2
	indexMB = totalMB - blockMB
	return blockMB, indexMB
}

// buildNoKVBenchmarkOptions starts from local.NewDefaultOptions() and
// overrides only the workload-specific sizing plus the background helpers
// (WAL watchdog, batch coalescing wait, hot-key throttle) that
// would otherwise muddy benchmark numbers. The shape is intentionally
// production-like so that perf changes in NewDefaultOptions are picked
// up by YCSB on the next run without manual re-sync.
func buildNoKVBenchmarkOptions(dir string, opts ycsbEngineOptions) *local.Options {
	cfg := local.NewDefaultOptions()
	cfg.WorkDir = dir

	cfg.MemTableSize = int64(opts.MemtableMB) << 20
	cfg.WriteBatchMaxCount = ycsbNoKVWriteBatchMaxCount
	cfg.WriteBatchMaxSize = ycsbNoKVWriteBatchMaxSize
	cfg.MaxBatchCount = ycsbNoKVWriteBatchMaxCount
	cfg.MaxBatchSize = ycsbNoKVWriteBatchMaxSize
	cfg.DetectConflicts = false
	cfg.SyncWrites = opts.SyncWrites

	totalCacheMB := normalizeTotalCacheMB(opts.BlockCacheMB)
	blockCacheMB, indexCacheMB := resolveNoKVCacheBudgetMB(totalCacheMB, opts.NoKVIndexCacheMB)
	cfg.BlockCacheBytes = cacheBudgetBytes(blockCacheMB)
	cfg.IndexCacheBytes = cacheBudgetBytes(indexCacheMB)

	// Per-test sweep override; otherwise inherit NewDefaultOptions().
	if n := ycsbNoKVWriteShards(); n > 0 {
		cfg.WriteShardCount = n
	}

	// Disable background helpers and timing-sensitive heuristics so
	// benchmark numbers reflect the steady-state storage path only.
	cfg.WriteBatchWait = 0
	cfg.WriteHotKeyLimit = 0
	cfg.EnableWALWatchdog = false
	cfg.ManifestSync = false

	// Maximum-throughput profile: skip the production defaults that trade
	// CPU for IO benefits. YCSB uses generic keys, so it deliberately does
	// not provide a semantic key shape for local shard or prefix-bloom hints.
	cfg.BlockCompression = local.BlockCompressionNone
	cfg.UserKeyShapeExtractor = nil

	return cfg
}

// ycsbNoKVWriteShards reads NOKV_TEST_WRITE_SHARDS for ad-hoc sweeps. Zero or
// invalid falls back to the constructor default (4).
func ycsbNoKVWriteShards() int {
	if v := os.Getenv("NOKV_TEST_WRITE_SHARDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 0
}

// buildBadgerBenchmarkOptions returns an explicit benchmark profile for
// Badger, avoiding badger.DefaultOptions so benchmark behavior stays stable
// even if upstream defaults change in a future release.
func buildBadgerBenchmarkOptions(dir string, opts ycsbEngineOptions) badger.Options {
	totalCacheMB := normalizeTotalCacheMB(opts.BlockCacheMB)
	blockCacheMB, indexCacheMB := defaultBadgerCacheBudgetMB(totalCacheMB)
	if opts.BadgerBlockCacheMB > 0 {
		blockCacheMB = opts.BadgerBlockCacheMB
	}
	if opts.BadgerIndexCacheMB > 0 {
		indexCacheMB = opts.BadgerIndexCacheMB
	}
	memtableSize := int64(opts.MemtableMB) << 20
	if memtableSize <= 0 {
		memtableSize = 64 << 20
	}
	baseTableSize := int64(opts.SSTableMB) << 20
	if baseTableSize <= 0 {
		baseTableSize = ycsbBadgerDefaultTableSize
	}
	valueLogFileSize := int64(opts.VlogFileMB) << 20
	if valueLogFileSize <= 0 {
		valueLogFileSize = ycsbBadgerDefaultValueLogSize
	}
	numGo := runtime.GOMAXPROCS(0)
	if numGo > 8 {
		numGo = 8
	}
	if numGo <= 0 {
		numGo = 1
	}
	return badger.Options{
		Dir:                     dir,
		ValueDir:                dir,
		SyncWrites:              opts.SyncWrites,
		NumVersionsToKeep:       ycsbBadgerNumVersionsToKeep,
		Logger:                  nil,
		Compression:             parseBadgerCompression(opts.BadgerCompression),
		MetricsEnabled:          false,
		NumGoroutines:           numGo,
		MemTableSize:            memtableSize,
		BaseTableSize:           baseTableSize,
		BaseLevelSize:           baseTableSize * ycsbBadgerBaseLevelMultiplier,
		LevelSizeMultiplier:     ycsbBadgerLevelSizeMultiplier,
		TableSizeMultiplier:     ycsbBadgerTableSizeMultiplier,
		MaxLevels:               ycsbBadgerMaxLevels,
		ValueThreshold:          int64(opts.ValueThreshold),
		NumMemtables:            ycsbBadgerNumMemtables,
		BlockSize:               ycsbBadgerBlockSize,
		BloomFalsePositive:      ycsbBadgerBloomFalsePositive,
		BlockCacheSize:          int64(blockCacheMB) << 20,
		IndexCacheSize:          int64(indexCacheMB) << 20,
		NumLevelZeroTables:      ycsbBadgerNumLevelZeroTables,
		NumLevelZeroTablesStall: ycsbBadgerNumLevelZeroTablesStall,
		ValueLogFileSize:        valueLogFileSize,
		ValueLogMaxEntries:      ycsbBadgerValueLogMaxEntries,
		NumCompactors:           ycsbBadgerNumCompactors,
		CompactL0OnClose:        false,
		ZSTDCompressionLevel:    ycsbBadgerZSTDCompressionLevel,
		VerifyValueChecksum:     false,
		DetectConflicts:         false,
		NamespaceOffset:         ycsbBadgerNamespaceOffset,
	}
}

// buildPebbleBenchmarkOptions returns the benchmark profile for Pebble. Pebble
// still validates remaining invariants during Open(), but all benchmark-facing
// knobs are set explicitly here rather than being inherited from defaults.
func buildPebbleBenchmarkOptions(opts ycsbEngineOptions) *pebble.Options {
	pebbleOpts := &pebble.Options{
		BytesPerSync: 0,
		DisableWAL:   false,
	}
	if mb := normalizeTotalCacheMB(opts.BlockCacheMB); mb > 0 {
		pebbleOpts.Cache = pebble.NewCache(int64(mb) << 20)
	}
	if mb := opts.MemtableMB; mb > 0 {
		pebbleOpts.MemTableSize = uint64(mb) << 20
	}
	level0 := pebble.LevelOptions{
		Compression: parsePebbleCompression(opts.PebbleCompression),
		BlockSize:   4 << 10,
	}
	if mb := opts.SSTableMB; mb > 0 {
		level0.TargetFileSize = int64(mb) << 20
	}
	pebbleOpts.Levels = []pebble.LevelOptions{level0}
	return pebbleOpts
}

func parseBadgerCompression(codec string) badgeropts.CompressionType {
	switch codec {
	case "snappy":
		return badgeropts.Snappy
	case "zstd":
		return badgeropts.ZSTD
	default:
		return badgeropts.None
	}
}
