// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package wal

import (
	"testing"

	kv "github.com/feichai0017/NoKV/txn/storage"
)

func newBenchManager(b *testing.B) *Manager {
	b.Helper()
	mgr, err := Open(Config{
		Dir:         b.TempDir(),
		SegmentSize: 256 << 20,
	})
	if err != nil {
		b.Fatalf("open wal: %v", err)
	}
	b.Cleanup(func() {
		_ = mgr.Close()
	})
	return mgr
}

var benchDurabilities = []struct {
	name   string
	policy DurabilityPolicy
}{
	{"buffered", DurabilityBuffered},
	{"flushed", DurabilityFlushed},
	{"fsync_batched", DurabilityFsyncBatched},
}

func BenchmarkWALAppend(b *testing.B) {
	for _, d := range benchDurabilities {
		b.Run(d.name, func(b *testing.B) {
			mgr := newBenchManager(b)
			payload := make([]byte, 256)
			entry := kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("bench-key"), 1), payload)
			defer entry.DecrRef()
			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))
			b.ResetTimer()
			for b.Loop() {
				if _, err := mgr.AppendEntry(d.policy, entry); err != nil {
					b.Fatalf("append: %v", err)
				}
			}
		})
	}
}

func BenchmarkWALAppendParallel(b *testing.B) {
	for _, d := range benchDurabilities {
		b.Run(d.name, func(b *testing.B) {
			mgr := newBenchManager(b)
			payload := make([]byte, 256)
			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				entry := kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("bench-key"), 1), payload)
				defer entry.DecrRef()
				for pb.Next() {
					if _, err := mgr.AppendEntry(d.policy, entry); err != nil {
						b.Fatalf("append: %v", err)
					}
				}
			})
		})
	}
}

func BenchmarkWALReplay(b *testing.B) {
	mgr := newBenchManager(b)
	payload := make([]byte, 128)
	entry := kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("bench-key"), 1), payload)
	defer entry.DecrRef()
	for range 10_000 {
		if _, err := mgr.AppendEntry(DurabilityBuffered, entry); err != nil {
			b.Fatalf("append preload: %v", err)
		}
	}
	if err := mgr.Sync(); err != nil {
		b.Fatalf("sync: %v", err)
	}
	seg := mgr.ActiveSegment()
	b.ReportAllocs()

	for b.Loop() {
		if err := mgr.ReplaySegment(seg, func(EntryInfo, []byte) error { return nil }); err != nil {
			b.Fatalf("replay: %v", err)
		}
	}
}
