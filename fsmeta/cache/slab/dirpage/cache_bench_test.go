// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package dirpage

import (
	"crypto/rand"
	"fmt"
	"path/filepath"
	"testing"
)

// Phase 5f benches measure the dirpage cache substrate cost in
// isolation. End-to-end "DirPage fast path vs full LSM Scan + N inode
// BatchGet" comparisons live with the fsmeta integration once a real
// DB benchmark fixture is wired in (Phase 5g).
//
// Numbers here answer two specific questions:
//   - How fast is a warm-cache Lookup on a 1K / 10K / 100K-entry dir?
//   - How fast is a MaterializeAsync write of the same?
//
// Run:
//   go test -bench BenchmarkDirPageLookupWarm ./fsmeta/cache/slab/dirpage
//   go test -bench BenchmarkDirPageMaterializeAsync ./fsmeta/cache/slab/dirpage

func newBenchCache(b *testing.B, maxPageBytes int) *Cache {
	b.Helper()
	dir := b.TempDir()
	c, err := Open(Config{
		Dir:            filepath.Join(dir, "dirpage"),
		MaxSegmentSize: 1 << 28, // 256 MiB so even 100K-entry dirs fit in one segment
		MaxPageBytes:   maxPageBytes,
	})
	if err != nil {
		b.Fatalf("open dirpage cache: %v", err)
	}
	b.Cleanup(func() { _ = c.Close() })
	return c
}

func makeBenchEntries(count, blobSize int) []Entry {
	out := make([]Entry, count)
	for i := range out {
		blob := make([]byte, blobSize)
		_, _ = rand.Read(blob)
		out[i] = Entry{
			Name:     fmt.Appendf(nil, "entry-%07d", i),
			Inode:    uint64(i + 1),
			AttrBlob: blob,
		}
	}
	return out
}

// BenchmarkDirPageLookupWarm measures the steady-state Lookup hit cost
// for directories of varying size. The cache is pre-populated; each
// iteration is a single Lookup round-trip including in-memory index
// probe + N mmap page reads + decode.
func BenchmarkDirPageLookupWarm(b *testing.B) {
	const blobSize = 96 // typical fsmeta InodeRecord size after encoding
	for _, n := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("entries=%d", n), func(b *testing.B) {
			c := newBenchCache(b, 16<<10)
			key := PageKey{Mount: 1, Parent: uint64(n)}
			entries := makeBenchEntries(n, blobSize)
			if err := c.MaterializeAsync(key, 1, entries); err != nil {
				b.Fatalf("materialize: %v", err)
			}
			b.ResetTimer()
			b.ReportAllocs()
			for b.Loop() {
				out, ok := c.Lookup(key, 1)
				if !ok || len(out) != n {
					b.Fatalf("lookup miss or wrong size: ok=%v got=%d want=%d", ok, len(out), n)
				}
			}
		})
	}
}

// BenchmarkDirPageLookupMiss measures the cost of a cache miss (the
// fsmeta caller will then fall back to LSM scan + materialize). The
// in-memory index probe is the only work; no page read or decode.
func BenchmarkDirPageLookupMiss(b *testing.B) {
	c := newBenchCache(b, 16<<10)
	key := PageKey{Mount: 99, Parent: 99}
	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		if _, ok := c.Lookup(key, 0); ok {
			b.Fatalf("expected miss")
		}
	}
}

// BenchmarkDirPageMaterializeAsync measures the cost of writing a fresh
// page set for directories of varying size. This is the cost the fsmeta
// caller pays on the cold path (after an LSM Scan + N inode BatchGet
// produces the assembled DentryAttrPair slice).
func BenchmarkDirPageMaterializeAsync(b *testing.B) {
	const blobSize = 96
	for _, n := range []int{1_000, 10_000} {
		b.Run(fmt.Sprintf("entries=%d", n), func(b *testing.B) {
			c := newBenchCache(b, 16<<10)
			entries := makeBenchEntries(n, blobSize)
			b.ResetTimer()
			b.ReportAllocs()
			i := 0
			for b.Loop() {
				key := PageKey{Mount: 1, Parent: uint64(i)}
				i++
				if err := c.MaterializeAsync(key, 1, entries); err != nil {
					b.Fatalf("materialize: %v", err)
				}
			}
		})
	}
}

// BenchmarkDirPageInvalidate measures the cost of bumping the
// invalidation epoch — what every fsmeta Create/Unlink/Link/Rename pays after
// a successful commit. The work is one sync.Map lookup + atomic.Add plus a
// small in-memory page-index sweep for cached pages under that directory.
func BenchmarkDirPageInvalidate(b *testing.B) {
	c := newBenchCache(b, 16<<10)
	keys := make([]PageKey, 1024)
	for i := range keys {
		keys[i] = PageKey{Mount: 1, Parent: uint64(i)}
		if err := c.MaterializeAsync(keys[i], 0, makeEntries(1, 16)); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		c.Invalidate(keys[i&1023].Directory())
		i++
	}
}
