// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package ycsb

import (
	"bytes"
	"path/filepath"
	"testing"
	"text/tabwriter"

	"github.com/cockroachdb/pebble"
	badgeropts "github.com/dgraph-io/badger/v4/options"
	"github.com/stretchr/testify/require"
)

func TestEngineDir(t *testing.T) {
	opts := ycsbEngineOptions{BaseDir: "/tmp/ycsb"}
	require.Equal(t, filepath.Join("/tmp/ycsb", "nokv_ycsb"), opts.engineDir("nokv"))
}

func TestLatencyRecorder(t *testing.T) {
	rec := newLatencyRecorder(4)
	rec.Record(10)
	rec.Record(20)
	rec.Record(30)

	require.InDelta(t, 0.0, rec.Percentile(0), 0.001)
	require.InDelta(t, 10.0, rec.Percentile(1), 0.001)
	require.InDelta(t, 20.0, rec.Percentile(50), 0.001)
	require.InDelta(t, 30.0, rec.Percentile(100), 0.001)

	samples := rec.Samples()
	require.ElementsMatch(t, []int64{10, 20, 30}, samples)
}

func TestIntRecorder(t *testing.T) {
	rec := newIntRecorder(3)
	rec.Record(5)
	rec.Record(15)
	rec.Record(25)

	require.InDelta(t, 5.0, rec.Percentile(10), 0.001)
	require.InDelta(t, 15.0, rec.Percentile(50), 0.001)
	require.InDelta(t, 25.0, rec.Percentile(100), 0.001)
	require.InDelta(t, (5+15+25)/3.0, rec.Average(), 0.001)

	var nilRecorder *intRecorder
	require.Zero(t, nilRecorder.Average())
	require.Zero(t, nilRecorder.Percentile(50))
}

func TestWriteSummaryTableOutputsRows(t *testing.T) {
	results := []BenchmarkResult{{
		Engine:          "nokv",
		Operation:       "YCSB-A",
		Mode:            "standalone",
		TotalOperations: 0, // verify zero guard paths
		TotalDuration:   0, // nanoseconds
	}}
	results[0].Finalize()

	buf := &bytes.Buffer{}
	tw := tabwriter.NewWriter(buf, 0, 4, 2, ' ', 0)
	writeSummaryTable(tw, results)

	require.Contains(t, buf.String(), "nokv")
	require.Contains(t, buf.String(), "YCSB-A")
}

func TestLatencyRecorderEdgeCases(t *testing.T) {
	rec := newLatencyRecorder(2)
	require.Equal(t, 0.0, rec.Percentile(0))
	require.Equal(t, 0.0, rec.Percentile(200)) // >100 clamps

	rec.Record(5)
	require.Equal(t, float64(5), rec.Percentile(100))

	rec.Record(10)
	require.InDelta(t, 5.0, rec.Percentile(1), 0.001)
	require.InDelta(t, 10.0, rec.Percentile(100), 0.001)
}

func TestBuildNoKVBenchmarkOptions(t *testing.T) {
	opts := buildNoKVBenchmarkOptions(t.TempDir(), ycsbEngineOptions{
		BlockCacheMB: 512,
		MemtableMB:   64,
		SSTableMB:    512,
		SyncWrites:   false,
	})

	require.False(t, opts.ThermosEnabled)
	require.Zero(t, opts.WriteBatchWait)
	require.Zero(t, opts.WriteHotKeyLimit)
	require.False(t, opts.EnableWALWatchdog)
	require.Nil(t, opts.UserKeyShapeExtractor)
	require.Equal(t, ycsbNoKVWriteBatchMaxCount, opts.WriteBatchMaxCount)
	require.Equal(t, int64(ycsbNoKVWriteBatchMaxCount), opts.MaxBatchCount)
	require.Equal(t, int64(384)<<20, opts.BlockCacheBytes)
	require.Equal(t, int64(128)<<20, opts.IndexCacheBytes)
}

func TestBuildNoKVBenchmarkOptionsExplicitCacheOverrides(t *testing.T) {
	opts := buildNoKVBenchmarkOptions(t.TempDir(), ycsbEngineOptions{
		BlockCacheMB:     512,
		NoKVIndexCacheMB: 64,
		MemtableMB:       64,
		SSTableMB:        512,
		SyncWrites:       false,
	})

	require.Equal(t, int64(448)<<20, opts.BlockCacheBytes)
	require.Equal(t, int64(64)<<20, opts.IndexCacheBytes)
}

func TestBuildBadgerBenchmarkOptions(t *testing.T) {
	opts := buildBadgerBenchmarkOptions(t.TempDir(), ycsbEngineOptions{
		BlockCacheMB:      512,
		BadgerCompression: "none",
		ValueThreshold:    1024,
		MemtableMB:        64,
		SSTableMB:         512,
		VlogFileMB:        512,
		SyncWrites:        false,
	})

	require.Nil(t, opts.Logger)
	require.False(t, opts.MetricsEnabled)
	require.False(t, opts.DetectConflicts)
	require.Equal(t, badgeropts.None, opts.Compression)
	require.Equal(t, int64(256)<<20, opts.BlockCacheSize)
	require.Equal(t, int64(256)<<20, opts.IndexCacheSize)
	require.Equal(t, int64(512)<<20, opts.BaseTableSize)
	require.Equal(t, int64(512)<<20, opts.ValueLogFileSize)
}

func TestBuildBadgerBenchmarkOptionsExplicitCacheOverrides(t *testing.T) {
	opts := buildBadgerBenchmarkOptions(t.TempDir(), ycsbEngineOptions{
		BlockCacheMB:       512,
		BadgerBlockCacheMB: 128,
		BadgerIndexCacheMB: 64,
		BadgerCompression:  "none",
		ValueThreshold:     1024,
		MemtableMB:         64,
		SSTableMB:          512,
		VlogFileMB:         512,
		SyncWrites:         false,
	})

	require.Equal(t, int64(128)<<20, opts.BlockCacheSize)
	require.Equal(t, int64(64)<<20, opts.IndexCacheSize)
}

func TestBuildPebbleBenchmarkOptions(t *testing.T) {
	opts := buildPebbleBenchmarkOptions(ycsbEngineOptions{
		BlockCacheMB:      512,
		MemtableMB:        64,
		SSTableMB:         512,
		PebbleCompression: "none",
	})

	require.NotNil(t, opts.Cache)
	require.Equal(t, int64(512)<<20, opts.Cache.MaxSize())
	require.Equal(t, uint64(64)<<20, opts.MemTableSize)
	require.Len(t, opts.Levels, 1)
	require.Equal(t, pebble.NoCompression, opts.Levels[0].Compression)
	require.Equal(t, int64(512)<<20, opts.Levels[0].TargetFileSize)
	require.Equal(t, 4<<10, opts.Levels[0].BlockSize)
}
