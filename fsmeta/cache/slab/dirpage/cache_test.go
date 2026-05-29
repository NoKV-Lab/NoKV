// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package dirpage

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func newTestCache(t *testing.T, opts ...func(*Config)) (*Cache, string) {
	t.Helper()
	dir := t.TempDir()
	cfg := Config{
		Dir:            filepath.Join(dir, "dirpage"),
		MaxSegmentSize: 1 << 22, // 4 MiB to stress rotation in tests
		MaxPageBytes:   2 << 10, // small pages to exercise multi-page splits
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	c, err := Open(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	return c, cfg.Dir
}

func makeEntries(n int, blobSize int) []Entry {
	out := make([]Entry, n)
	for i := range out {
		blob := make([]byte, blobSize)
		_, _ = rand.Read(blob)
		out[i] = Entry{
			Name:     fmt.Appendf(nil, "entry-%05d", i),
			Inode:    uint64(i + 1),
			AttrBlob: blob,
		}
	}
	return out
}

// TestCacheLookupMissAndHit covers the basic store / lookup contract.
func TestCacheLookupMissAndHit(t *testing.T) {
	c, _ := newTestCache(t)
	key := PageKey{Mount: 1, Parent: 42}

	// Lookup before any materialize must miss.
	got, ok := c.Lookup(key, 1)
	require.False(t, ok)
	require.Nil(t, got)

	entries := makeEntries(8, 32)
	require.NoError(t, c.MaterializeAsync(key, 1, entries))

	got, ok = c.Lookup(key, 1)
	require.True(t, ok)
	require.Equal(t, len(entries), len(got))
	for i := range entries {
		require.Equal(t, entries[i].Name, got[i].Name)
		require.Equal(t, entries[i].Inode, got[i].Inode)
		require.Equal(t, entries[i].AttrBlob, got[i].AttrBlob)
	}

	stats := c.Stats()
	require.Equal(t, uint64(1), stats.Hits)
	require.Equal(t, uint64(1), stats.Misses)
	require.Equal(t, uint64(1), stats.StoreOK)
}

func TestCachePageKeyIncludesPagination(t *testing.T) {
	c, _ := newTestCache(t)
	afterA := PageKey{Mount: 1, Parent: 42, StartAfter: "a", Limit: 1}
	firstPage := PageKey{Mount: 1, Parent: 42, Limit: 1}

	require.NoError(t, c.MaterializeAsync(afterA, 1, makeEntries(1, 16)))
	_, ok := c.Lookup(firstPage, 1)
	require.False(t, ok, "a page materialized for one cursor must not satisfy a different cursor")
	_, ok = c.Lookup(afterA, 1)
	require.True(t, ok)
}

func TestCacheInvalidateDropsAllPagesForDirectory(t *testing.T) {
	c, _ := newTestCache(t)
	firstPage := PageKey{Mount: 1, Parent: 42, Limit: 1}
	nextPage := PageKey{Mount: 1, Parent: 42, StartAfter: "a", Limit: 1}

	require.NoError(t, c.MaterializeAsync(firstPage, 1, makeEntries(1, 16)))
	require.NoError(t, c.MaterializeAsync(nextPage, 1, makeEntries(1, 16)))
	require.Len(t, c.dirs[firstPage.Directory()], 2)
	c.Invalidate(firstPage.Directory())

	_, ok := c.Lookup(firstPage, 1)
	require.False(t, ok)
	_, ok = c.Lookup(nextPage, 1)
	require.False(t, ok)
	require.Empty(t, c.dirs[firstPage.Directory()])
}

// TestCacheStaleByFrontier confirms Lookup with the wrong frontier
// returns a stale miss without surfacing the cached entries.
func TestCacheStaleByFrontier(t *testing.T) {
	c, _ := newTestCache(t)
	key := PageKey{Mount: 2, Parent: 7}
	require.NoError(t, c.MaterializeAsync(key, 5, makeEntries(4, 16)))

	// Same frontier => fresh.
	_, ok := c.Lookup(key, 5)
	require.True(t, ok)

	// Different frontier => stale.
	_, ok = c.Lookup(key, 6)
	require.False(t, ok)
	_, ok = c.Lookup(key, 4)
	require.False(t, ok)

	require.Equal(t, uint64(2), c.Stats().Stale)
}

// TestCacheInvalidateBumpsEpoch checks that Invalidate returns a fresh
// epoch and drops the in-memory page set.
func TestCacheInvalidateBumpsEpoch(t *testing.T) {
	c, _ := newTestCache(t)
	key := PageKey{Mount: 3, Parent: 11}

	require.Equal(t, uint64(0), c.CurrentEpoch(key.Directory()))
	next := c.Invalidate(key.Directory())
	require.Equal(t, uint64(1), next)
	require.Equal(t, uint64(1), c.CurrentEpoch(key.Directory()))

	require.NoError(t, c.MaterializeAsync(key, next, makeEntries(2, 8)))
	_, ok := c.Lookup(key, next)
	require.True(t, ok)

	bumped := c.Invalidate(key.Directory())
	require.Equal(t, uint64(2), bumped)
	_, ok = c.Lookup(key, next)
	require.False(t, ok, "Invalidate must drop the prior page set")
}

// TestCacheMaterializeDropsAfterInvalidate covers the publish-discipline
// race: an Invalidate that fires after the caller decided to materialize
// at frontier=N must cause the materialize to be dropped, not committed.
func TestCacheMaterializeDropsAfterInvalidate(t *testing.T) {
	c, _ := newTestCache(t)
	key := PageKey{Mount: 4, Parent: 22}

	// Bump epoch to 5 first.
	for range 5 {
		c.Invalidate(key.Directory())
	}
	require.Equal(t, uint64(5), c.CurrentEpoch(key.Directory()))

	// Materialize against a stale frontier (4) while current epoch is 5.
	// Cache must drop the write.
	require.NoError(t, c.MaterializeAsync(key, 4, makeEntries(3, 8)))

	stats := c.Stats()
	require.Equal(t, uint64(1), stats.Dropped)
	require.Equal(t, uint64(0), stats.StoreOK)

	// Lookup against either frontier still misses.
	_, ok := c.Lookup(key, 4)
	require.False(t, ok)
	_, ok = c.Lookup(key, 5)
	require.False(t, ok)
}

// TestCacheMultiPage exercises the splitter: a directory big enough to
// span multiple pages should round-trip without entry loss or reorder.
func TestCacheMultiPage(t *testing.T) {
	c, _ := newTestCache(t)
	key := PageKey{Mount: 5, Parent: 100}

	// 200 entries × ~50B name + 64B blob ~= 25KB total → multiple
	// 2KB pages.
	entries := makeEntries(200, 64)
	require.NoError(t, c.MaterializeAsync(key, 1, entries))

	got, ok := c.Lookup(key, 1)
	require.True(t, ok)
	require.Equal(t, len(entries), len(got))
	for i := range entries {
		require.Equal(t, entries[i].Name, got[i].Name, "entry %d name mismatch", i)
		require.Equal(t, entries[i].Inode, got[i].Inode, "entry %d inode mismatch", i)
	}
}

// TestCacheReloadAcrossOpen confirms pages survive Close / re-Open. The
// caller's frontier must match what was stored — reload does not reset
// the per-key epoch, so the consumer is responsible for choosing
// frontier values that survive a process restart (in fsmeta this is the
// WatchSubtree event cursor or a deterministic recompute).
func TestCacheReloadAcrossOpen(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		Dir:          filepath.Join(dir, "dirpage"),
		MaxPageBytes: 2 << 10,
	}
	key := PageKey{Mount: 6, Parent: 9}
	entries := makeEntries(10, 32)

	{
		c, err := Open(cfg)
		require.NoError(t, err)
		require.NoError(t, c.MaterializeAsync(key, 7, entries))
		require.NoError(t, c.Close())
	}
	{
		c, err := Open(cfg)
		require.NoError(t, err)
		defer func() { _ = c.Close() }()

		got, ok := c.Lookup(key, 7)
		require.True(t, ok, "reload must re-index materialized pages")
		require.Equal(t, len(entries), len(got))
		for i := range entries {
			require.Equal(t, entries[i].Name, got[i].Name)
			require.Equal(t, entries[i].AttrBlob, got[i].AttrBlob)
		}
	}
}

// TestCacheRotation forces enough pages into the cache to overflow the
// active segment and trigger rotation; subsequent Lookups must still
// work because the locator carries fid.
func TestCacheRotation(t *testing.T) {
	c, _ := newTestCache(t, func(c *Config) {
		c.MaxSegmentSize = 32 << 10 // 32 KiB to force rotation fast
		c.MaxPageBytes = 4 << 10
	})

	type instance struct {
		key     PageKey
		entries []Entry
	}
	const nDirs = 64
	dirs := make([]instance, nDirs)
	for i := range dirs {
		dirs[i] = instance{
			key:     PageKey{Mount: 7, Parent: uint64(1000 + i)},
			entries: makeEntries(40, 128),
		}
		require.NoError(t, c.MaterializeAsync(dirs[i].key, 1, dirs[i].entries))
	}

	// Rotation must have occurred — at least 2 segment files exist.
	require.GreaterOrEqual(t, len(c.mgr.ListFIDs()), 2,
		"expected the small-segment cache to have rotated to multiple fids")

	// Every directory still resolves correctly.
	for _, d := range dirs {
		got, ok := c.Lookup(d.key, 1)
		require.True(t, ok, "lookup must hit for %v", d.key)
		require.Equal(t, len(d.entries), len(got))
	}
}

// TestCacheConcurrentAccess fans out reads / writes / invalidations to
// surface any locking bugs under -race.
func TestCacheConcurrentAccess(t *testing.T) {
	c, _ := newTestCache(t)
	const (
		writers = 4
		readers = 8
		ops     = 200
	)

	keyOf := func(i int) PageKey { return PageKey{Mount: 8, Parent: uint64(i & 7)} }

	var wg sync.WaitGroup
	wg.Add(writers + readers)
	for w := range writers {
		go func(seed int) {
			defer wg.Done()
			for i := range ops {
				key := keyOf(seed + i)
				ep := c.CurrentEpoch(key.Directory())
				_ = c.MaterializeAsync(key, ep, makeEntries(4, 16))
				if i%17 == 0 {
					c.Invalidate(key.Directory())
				}
			}
		}(w)
	}
	for r := range readers {
		go func(seed int) {
			defer wg.Done()
			for i := range ops {
				key := keyOf(seed + i)
				ep := c.CurrentEpoch(key.Directory())
				if entries, ok := c.Lookup(key, ep); ok {
					require.NotEmpty(t, entries)
				}
			}
		}(r)
	}
	wg.Wait()
}

// TestEncodeDecodeRoundTrip pins the wire format: a page goes through
// encode + decode and comes back byte-identical.
func TestEncodeDecodeRoundTrip(t *testing.T) {
	hdr := pageHeader{
		Mount:      9,
		Parent:     0xdeadbeefcafebabe,
		PageNo:     3,
		Frontier:   42,
		EntryCount: 4,
	}
	entries := []Entry{
		{Name: []byte("a"), Inode: 100, AttrBlob: []byte("attr-a")},
		{Name: []byte("longer-name"), Inode: 200, AttrBlob: []byte{}},
		{Name: []byte("zero-inode"), Inode: 0, AttrBlob: []byte("z")},
		{Name: []byte("emoji-📁"), Inode: 1<<63 - 1, AttrBlob: []byte("blob")},
	}
	encoded := encodePage(nil, hdr, entries)

	gotHdr, gotEntries, n, err := decodePage(encoded)
	require.NoError(t, err)
	require.Equal(t, len(encoded), n)
	require.Equal(t, hdr, gotHdr)
	require.Equal(t, len(entries), len(gotEntries))
	for i := range entries {
		require.Equal(t, entries[i].Name, gotEntries[i].Name)
		require.Equal(t, entries[i].Inode, gotEntries[i].Inode)
		require.Equal(t, entries[i].AttrBlob, gotEntries[i].AttrBlob)
	}
}

// TestDecodeRejectsCorruption ensures any single-byte flip past the
// magic+version preamble fails the CRC check rather than silently
// returning bad data.
func TestDecodeRejectsCorruption(t *testing.T) {
	hdr := pageHeader{Mount: 1, Parent: 1, PageNo: 0, Frontier: 1, EntryCount: 1}
	entries := []Entry{{Name: []byte("x"), Inode: 1, AttrBlob: []byte("y")}}
	encoded := encodePage(nil, hdr, entries)

	// Flip one byte in the raw payload region just before the checksum
	// tail. Picking a byte inside attr_blob keeps the varint framing
	// intact, so the failure mode under test is specifically the CRC
	// check rather than a truncation-style decode error.
	corrupt := append([]byte(nil), encoded...)
	corrupt[len(corrupt)-5] ^= 0xff
	_, _, _, err := decodePage(corrupt)
	require.ErrorIs(t, err, errPageBadChecksum)

	// Corrupting the magic should be caught earlier.
	bad := append([]byte(nil), encoded...)
	bad[0] ^= 0xff
	_, _, _, err = decodePage(bad)
	require.ErrorIs(t, err, errPageBadMagic)
}

func TestDecodeRejectsOversizedStartAfterLength(t *testing.T) {
	var encoded []byte
	var fixed [recordHeaderFixed]byte
	binary.LittleEndian.PutUint32(fixed[0:4], dirPageMagic)
	binary.LittleEndian.PutUint16(fixed[4:6], dirPageVersion)
	encoded = append(encoded, fixed[:]...)
	encoded = binary.AppendUvarint(encoded, 1)     // mount
	encoded = binary.AppendUvarint(encoded, 1)     // parent
	encoded = binary.AppendUvarint(encoded, 1<<62) // impossible start_after length
	encoded = binary.AppendUvarint(encoded, 0)     // limit, unreachable
	encoded = append(encoded, make([]byte, 4)...)  // checksum tail placeholder

	_, _, _, err := decodePage(encoded)
	require.ErrorIs(t, err, errPageTruncated)
}
