// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package ycsb

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"text/tabwriter"
)

func TestBenchmarkYCSB(t *testing.T) {
	if os.Getenv(benchmarkEnvKey) != "1" {
		t.Skipf("set %s=1 to run YCSB benchmarks", benchmarkEnvKey)
	}

	workloads, err := parseYCSBWorkloads(*ycsbWorkloads)
	if err != nil {
		t.Fatalf("parse workloads: %v", err)
	}
	engines := parseYCSBEngines(*ycsbEngines)
	if len(engines) == 0 {
		t.Fatalf("no YCSB engines selected (use -ycsb_engines)")
	}

	baseDir := filepath.Join(*fBenchDir, "ycsb")
	cfg := ycsbConfig{
		BaseDir:     baseDir,
		Seed:        *fSeed,
		RecordCount: *ycsbRecords,
		Operations:  *ycsbOperations,
		WarmUpOps:   *ycsbWarmOperations,
		ValueSize:   *ycsbValueSize,
		ValueDist:   *ycsbValueDist,
		ValueMin:    *ycsbValueMin,
		ValueMax:    *ycsbValueMax,
		ValueStd:    *ycsbValueStd,
		ValuePct:    *ycsbValuePercentiles,
		Concurrency: *ycsbConcurrency,
		ScanLength:  *ycsbScanLength,
		TargetOps:   *ycsbTargetOps,
		StatusEvery: *ycsbStatusInterval,
		Workloads:   workloads,
		Engines:     engines,
	}

	opts := ycsbEngineOptions{
		BaseDir:            baseDir,
		ValueSize:          cfg.ValueSize,
		ValueThreshold:     *fValueThreshold,
		SyncWrites:         *fSyncWrites,
		BlockCacheMB:       *ycsbBlockCacheMB,
		NoKVIndexCacheMB:   *ycsbNoKVIndexCacheMB,
		MemtableMB:         *ycsbMemtableMB,
		SSTableMB:          *ycsbSSTableMB,
		VlogFileMB:         *ycsbVlogFileMB,
		BadgerBlockCacheMB: *fBadgerBlockMB,
		BadgerIndexCacheMB: *fBadgerIndexMB,
		BadgerCompression:  strings.ToLower(*fBadgerCompression),
		PebbleCompression:  strings.ToLower(*ycsbPebbleCompression),
		RocksDBCompression: strings.ToLower(*ycsbRocksCompression),
	}

	results, err := runYCSBBenchmarks(cfg, opts)
	if err != nil {
		t.Fatalf("run YCSB benchmarks: %v", err)
	}

	if len(results) == 0 {
		t.Fatalf("no YCSB results generated")
	}

	tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	writeSummaryTable(tw, results)

	if err := writeYCSBSummary(results, filepath.Join(baseDir, "results")); err != nil {
		t.Fatalf("write YCSB summary: %v", err)
	}
	if err := WriteResults(results); err != nil {
		t.Fatalf("write YCSB text summary: %v", err)
	}
}
