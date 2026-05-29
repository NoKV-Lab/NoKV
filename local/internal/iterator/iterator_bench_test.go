// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package iterator_test

// External-test benchmarks for the iterator state machine, exercised
// through the root local.DB facade so we measure the same hot path
// end-to-end users hit. Helpers are intentionally inlined: shared
// db_test.go fixtures live in the root package and an external test
// package can't see unexported identifiers.

import (
	"encoding/binary"
	"testing"
	"time"

	local "github.com/feichai0017/NoKV/local"
	index "github.com/feichai0017/NoKV/txn/storage"
)

func newBenchDB(b *testing.B, optFn func(*local.Options)) *local.DB {
	b.Helper()
	opt := local.NewDefaultOptions()
	opt.WorkDir = b.TempDir()
	opt.EnableWALWatchdog = false
	opt.SyncWrites = false
	opt.WriteBatchWait = 0
	if optFn != nil {
		optFn(opt)
	}
	db, err := local.Open(opt)
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	b.Cleanup(func() {
		_ = db.Close()
	})
	return db
}

func makeBenchKey(i int) []byte {
	key := make([]byte, 16)
	copy(key, "benchkey")
	binary.LittleEndian.PutUint64(key[len(key)-8:], uint64(i))
	return key
}

func loadBenchKeys(b *testing.B, db *local.DB, n int, value []byte) [][]byte {
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

func BenchmarkDBIteratorScan(b *testing.B) {
	db := newBenchDB(b, nil)
	value := make([]byte, 128)
	_ = loadBenchKeys(b, db, 20_000, value)
	it := db.NewIterator(&index.Options{IsAsc: true})
	defer func() { _ = it.Close() }()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it.Rewind()
		for it.Valid() {
			it.Next()
		}
	}
}

func BenchmarkDBIteratorSeek(b *testing.B) {
	db := newBenchDB(b, nil)
	value := make([]byte, 128)
	keys := loadBenchKeys(b, db, 20_000, value)
	it := db.NewIterator(&index.Options{IsAsc: true})
	defer func() { _ = it.Close() }()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := keys[i%len(keys)]
		it.Seek(key)
		if it.Valid() {
			_ = it.Item()
		}
	}
}
