// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package local

import (
	"encoding/binary"
	"testing"
	"time"

	kv "github.com/feichai0017/NoKV/txn/storage"
)

func newBenchDB(b *testing.B, optFn func(*Options)) *DB {
	b.Helper()
	opt := NewDefaultOptions()
	opt.WorkDir = b.TempDir()
	opt.EnableWALWatchdog = false
	opt.SyncWrites = false
	opt.ManifestSync = false
	opt.WriteBatchWait = 0
	if optFn != nil {
		optFn(opt)
	}
	db := openTestDB(b, opt)
	b.Cleanup(func() {
		_ = db.Close()
	})
	return db
}

func benchKeyBuffer() []byte {
	key := make([]byte, 16)
	copy(key, "benchkey")
	return key
}

func setBenchKey(buf []byte, i uint64) {
	binary.LittleEndian.PutUint64(buf[len(buf)-8:], i)
}

func makeBenchKey(i int) []byte {
	key := benchKeyBuffer()
	setBenchKey(key, uint64(i))
	return key
}

func loadBenchKeys(b *testing.B, db *DB, n int, value []byte) [][]byte {
	b.Helper()
	keys := make([][]byte, n)
	for i := range n {
		key := makeBenchKey(i)
		if err := db.Set(key, value); err != nil {
			b.Fatalf("preload key %d: %v", i, err)
		}
		keys[i] = key
	}
	time.Sleep(10 * time.Millisecond)
	return keys
}

func BenchmarkDBSetSmall(b *testing.B) {
	db := newBenchDB(b, nil)
	value := make([]byte, 32)
	key := benchKeyBuffer()
	b.ReportAllocs()
	b.SetBytes(int64(len(value)))

	for i := 0; b.Loop(); i++ {
		setBenchKey(key, uint64(i))
		if err := db.Set(key, value); err != nil {
			b.Fatalf("set: %v", err)
		}
	}
}

func BenchmarkDBSetLarge(b *testing.B) {
	db := newBenchDB(b, nil)
	value := make([]byte, 4<<10)
	key := benchKeyBuffer()
	b.ReportAllocs()
	b.SetBytes(int64(len(value)))

	for i := 0; b.Loop(); i++ {
		setBenchKey(key, uint64(i))
		if err := db.Set(key, value); err != nil {
			b.Fatalf("set: %v", err)
		}
	}
}

func BenchmarkDBGetSmall(b *testing.B) {
	db := newBenchDB(b, nil)
	value := make([]byte, 64)
	keys := loadBenchKeys(b, db, 10_000, value)
	b.ReportAllocs()
	b.SetBytes(int64(len(value)))

	for i := 0; b.Loop(); i++ {
		if _, err := db.Get(keys[i%len(keys)]); err != nil {
			b.Fatalf("get: %v", err)
		}
	}
}

func BenchmarkDBGetLarge(b *testing.B) {
	db := newBenchDB(b, nil)
	value := make([]byte, 4<<10)
	keys := loadBenchKeys(b, db, 10_000, value)
	b.ReportAllocs()
	b.SetBytes(int64(len(value)))

	for i := 0; b.Loop(); i++ {
		if _, err := db.Get(keys[i%len(keys)]); err != nil {
			b.Fatalf("get: %v", err)
		}
	}
}

// BenchmarkDBBatchSet compares end-to-end batch write throughput under three sync modes:
//
//	NoSync        – SyncWrites=false
//	SyncInline    – SyncWrites=true, SyncPipeline=false
//	SyncPipeline  – SyncWrites=true, SyncPipeline=true
func BenchmarkDBBatchSet(b *testing.B) {
	type syncMode struct {
		name     string
		sync     bool
		pipeline bool
	}
	modes := []syncMode{
		{"NoSync", false, false},
		{"SyncInline", true, false},
		{"SyncPipeline", true, true},
	}

	value := make([]byte, 256)
	batchSize := 64

	for _, mode := range modes {
		b.Run(mode.name, func(b *testing.B) {
			db := newBenchDB(b, func(opt *Options) {
				opt.WriteBatchMaxCount = 128
				opt.MaxBatchCount = 128
				opt.SyncWrites = mode.sync
				opt.SyncPipeline = mode.pipeline
			})
			b.ReportAllocs()
			b.SetBytes(int64(batchSize * len(value)))
			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				entries := make([]*kv.Entry, batchSize)
				for j := range batchSize {
					key := makeBenchKey(i*batchSize + j)
					entries[j] = kv.NewInternalEntry(kv.CFDefault, key, nonTxnMaxVersion, value, 0, 0)
				}
				req, err := db.sendToWriteCh(entries, true)
				if err != nil {
					b.Fatalf("batchSet: %v", err)
				}
				if err := req.Wait(); err != nil {
					b.Fatalf("wait batchSet: %v", err)
				}
			}
		})
	}
}

// BenchmarkDBCommitInlineValueSizes measures commit pipeline throughput across
// metadata-sized and larger inline values. NoKV no longer routes user values
// through a separate blob log, so these cases exercise the same WAL + memtable path.
func BenchmarkDBCommitInlineValueSizes(b *testing.B) {
	type profile struct {
		name      string
		valueSize int
	}
	profiles := []profile{
		{"Inline_64B", 64},
		{"Inline_256B", 256},
		{"Inline_1KB", 1024},
		{"Inline_4KB", 4 << 10},
		{"Inline_8KB", 8 << 10},
	}
	const batchSize = 64

	for _, p := range profiles {
		b.Run(p.name, func(b *testing.B) {
			value := make([]byte, p.valueSize)
			db := newBenchDB(b, func(opt *Options) {
				opt.WriteBatchMaxCount = 128
				opt.MaxBatchCount = 128
			})
			b.ReportAllocs()
			b.SetBytes(int64(batchSize * len(value)))
			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				entries := make([]*kv.Entry, batchSize)
				for j := range batchSize {
					key := makeBenchKey(i*batchSize + j)
					entries[j] = kv.NewInternalEntry(kv.CFDefault, key, nonTxnMaxVersion, value, 0, 0)
				}
				req, err := db.sendToWriteCh(entries, true)
				if err != nil {
					b.Fatalf("send: %v", err)
				}
				if err := req.Wait(); err != nil {
					b.Fatalf("wait: %v", err)
				}
			}
		})
	}
}
