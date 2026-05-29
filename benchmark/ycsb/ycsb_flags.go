// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package ycsb

import (
	"flag"
	"time"
)

var (
	fBenchDir   = flag.String("benchdir", "data", "benchmark working directory root")
	fSeed       = flag.Int64("seed", 42, "random seed for data generation")
	fSyncWrites = flag.Bool("sync", false, "force fsync on every write")

	fValueThreshold = flag.Int("value_threshold", 2048, "Badger value size threshold (bytes)")

	fBadgerBlockMB     = flag.Int("badger_block_cache_mb", -1, "Badger block cache size (MB); <=0 uses the default fair split from the total cache budget")
	fBadgerIndexMB     = flag.Int("badger_index_cache_mb", -1, "Badger index cache size (MB); <=0 uses the default fair split from the total cache budget")
	fBadgerCompression = flag.String("badger_compression", "none", "Badger compression codec: none|snappy|zstd")

	ycsbWorkloads         = flag.String("ycsb_workloads", "A,B,C,D,E,F", "comma-separated YCSB workloads (A-F)")
	ycsbEngines           = flag.String("ycsb_engines", "nokv,badger", "comma-separated engines to benchmark (nokv,badger,pebble,rocksdb)")
	ycsbRecords           = flag.Int("ycsb_records", 1000000, "number of records to preload during YCSB load phase")
	ycsbOperations        = flag.Int("ycsb_ops", 1000000, "number of transactional operations per workload")
	ycsbConcurrency       = flag.Int("ycsb_conc", 16, "worker goroutine count for YCSB transactional phase")
	ycsbScanLength        = flag.Int("ycsb_scan_len", 100, "maximum scan length used by YCSB workload E (maxscanlength)")
	ycsbValueSize         = flag.Int("ycsb_value_size", 1024, "value size (bytes) for YCSB records; 1 KiB matches the metadata-service profile (HDFS / CephFS / fsmeta inode + dentry size envelope)")
	ycsbWarmOperations    = flag.Int("ycsb_warm_ops", 0, "warm-up operations executed per workload before measuring (0 disables, matches official flow)")
	ycsbTargetOps         = flag.Int("ycsb_target_ops", 0, "optional target ops/sec (overall) during run; 0 disables throttling")
	ycsbStatusInterval    = flag.Duration("ycsb_status_interval", 0*time.Second, "interval for progress/status reporting; 0 disables")
	ycsbPebbleCompression = flag.String("ycsb_pebble_compression", "none", "Pebble compression codec: none|snappy|zstd")
	ycsbRocksCompression  = flag.String("ycsb_rocks_compression", "none", "RocksDB compression codec: none|snappy|zstd")
	ycsbBlockCacheMB      = flag.Int("ycsb_block_cache_mb", 512, "Total cache budget (MB) used as the default fair baseline across engines")
	ycsbNoKVIndexCacheMB  = flag.Int("ycsb_nokv_index_cache_mb", -1, "NoKV index cache budget (MB); <=0 uses the default fair split from the total cache budget")
	ycsbMemtableMB        = flag.Int("ycsb_memtable_mb", 64, "Memtable size (MB) for ordered-KV engines where applicable")
	ycsbSSTableMB         = flag.Int("ycsb_sstable_mb", 512, "Target SST size (MB) for RocksDB/Pebble where applicable")
	ycsbVlogFileMB        = flag.Int("ycsb_vlog_mb", 512, "Badger value log file size (MB)")
	ycsbValueDist         = flag.String("ycsb_value_dist", "fixed", "Value size distribution: fixed|uniform|normal|percentile")
	ycsbValueMin          = flag.Int("ycsb_value_min", 0, "Min value size for uniform/normal distributions (bytes); 0 defaults to value_size")
	ycsbValueMax          = flag.Int("ycsb_value_max", 0, "Max value size for uniform/normal distributions (bytes); 0 defaults to value_size")
	ycsbValueStd          = flag.Int("ycsb_value_std", 0, "Stddev for normal distribution (bytes); 0 defaults to value_size/4")
	ycsbValuePercentiles  = flag.String("ycsb_value_percentiles", "", "Percentile map for percentile dist, e.g. \"50:256,90:512,99:1024,100:2048\"")
)

const benchmarkEnvKey = "NOKV_RUN_BENCHMARKS"
