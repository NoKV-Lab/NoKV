// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package slab

import (
	"io"
	"path/filepath"
	"sync"
	"testing"

	"github.com/feichai0017/NoKV/storage/file"
	"github.com/feichai0017/NoKV/storage/vfs"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

const segmentTestHeaderSize = 20

func TestSegmentBootstrapReadWrite(t *testing.T) {
	dir := t.TempDir()
	opt := &file.Options{
		FID:      1,
		FileName: filepath.Join(dir, "seg"),
		MaxSz:    1 << 20,
	}

	var s Segment
	require.NoError(t, s.Open(opt))
	defer func() { _ = s.Close() }()

	require.NoError(t, s.Bootstrap(segmentTestHeaderSize))

	payload := []byte("hello-world")
	offset := segmentTestHeaderSize
	require.NoError(t, s.Write(uint32(offset), payload))

	read, err := s.Read(uint32(offset), uint32(len(payload)))
	require.NoError(t, err)
	require.Equal(t, payload, read)

	require.NoError(t, s.SetReadOnly())
	require.True(t, s.ro)
	require.NoError(t, s.SetWritable())
	require.False(t, s.ro)
}

func TestSegmentLifecycleHelpers(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "seg2")
	opt := &file.Options{
		FID:      9,
		FileName: path,
		MaxSz:    1 << 20,
	}

	var s Segment
	require.NoError(t, s.Open(opt))
	defer func() { _ = s.Close() }()

	require.NoError(t, s.Bootstrap(segmentTestHeaderSize))

	payload := []byte("seg-data")
	offset := segmentTestHeaderSize
	require.NoError(t, s.Write(uint32(offset), payload))
	require.Greater(t, s.Size(), int64(0))

	require.NoError(t, s.Sync())
	pos, err := s.Seek(0, 0)
	require.NoError(t, err)
	require.Equal(t, int64(0), pos)

	fd, ok := s.FileFD()
	require.True(t, ok)
	require.NotZero(t, fd)
	require.NotNil(t, s.File())
	require.Equal(t, path, s.FileName())

	end := uint32(offset + len(payload))
	require.NoError(t, s.DoneWriting(end))
	require.Equal(t, int64(end), s.Size())

	require.NoError(t, s.Truncate(int64(offset)))
	require.Equal(t, int64(offset), s.Size())

	require.NoError(t, s.Init())
}

func TestSegmentDoneWritingTruncateFailure(t *testing.T) {
	dir := t.TempDir()
	filename := filepath.Join(dir, "seg")

	truncateErr := errors.New("truncate failure")
	policy := vfs.NewFaultPolicy(
		vfs.FailOnNthRule(vfs.OpFileTrunc, filename, 2, truncateErr),
	)
	fs := vfs.NewFaultFSWithPolicy(vfs.OSFS{}, policy)

	opt := &file.Options{
		FID:      1,
		FileName: filename,
		MaxSz:    1 << 10,
		FS:       fs,
	}
	var s Segment
	require.NoError(t, s.Open(opt))
	defer func() { _ = s.Close() }()

	require.NoError(t, s.SetWritable())
	data := []byte("TestSegmentDoneWritingTruncateFailure")
	require.NoError(t, s.Write(0, data))
	require.GreaterOrEqual(t, s.Size(), int64(len(data)))

	err := s.DoneWriting(uint32(len(data)))
	require.Error(t, err)
	require.Contains(t, err.Error(), "truncate failure")
	require.Contains(t, err.Error(), "Unable to truncate segment")
}

func TestSegmentDoneWritingFileSyncFailure(t *testing.T) {
	dir := t.TempDir()
	filename := filepath.Join(dir, "seg")

	syncErr := errors.New("sync failure")
	policy := vfs.NewFaultPolicy(
		vfs.FailOnceRule(vfs.OpFileSync, filename, syncErr),
	)
	fs := vfs.NewFaultFSWithPolicy(vfs.OSFS{}, policy)

	opt := &file.Options{
		FID:      1,
		FileName: filename,
		MaxSz:    1 << 10,
		FS:       fs,
	}
	var s Segment
	require.NoError(t, s.Open(opt))
	defer func() { _ = s.Close() }()

	require.NoError(t, s.SetWritable())
	data := []byte("TestSegmentDoneWritingFileSyncFailure")
	require.NoError(t, s.Write(0, data))
	require.GreaterOrEqual(t, s.Size(), int64(len(data)))

	err := s.DoneWriting(uint32(len(data)))
	require.Error(t, err)
	require.Contains(t, err.Error(), "sync failure")
	require.Contains(t, err.Error(), "Unable to sync file descriptor (metadata) after truncate")
}

// TestSegmentWriteSizeMonotonicOutOfOrder reproduces the production race
// where a slab consumer hands out non-overlapping offset ranges without
// serializing the subsequent store.Write calls. If a Write that
// landed at a larger offset finishes first, a plain size.Store from a
// later Write at a smaller offset would shrink the high-water back below
// already-published data, producing spurious EOF on Read. This
// test drives that exact pattern under a serializing lock (matching how
// store.Lock guards Write in production) and asserts the high-water
// only ever advances.
func TestSegmentWriteSizeMonotonicOutOfOrder(t *testing.T) {
	dir := t.TempDir()
	opt := &file.Options{
		FID:      42,
		FileName: filepath.Join(dir, "seg-mono"),
		MaxSz:    16 << 20,
	}

	var s Segment
	require.NoError(t, s.Open(opt))
	defer func() { _ = s.Close() }()

	// Truncate down to the header high-water so subsequent writes extend
	// the live region. Mirrors a freshly-rotated active segment where a
	// consumer cursor starts at the header.
	require.NoError(t, s.Truncate(int64(segmentTestHeaderSize)))

	const (
		entries     = 1024
		payloadSize = 1024
	)
	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte(i & 0xff)
	}

	// Reserve N disjoint ranges in ascending order (matches reserve()).
	starts := make([]uint32, entries)
	cursor := uint32(segmentTestHeaderSize)
	for i := range starts {
		starts[i] = cursor
		cursor += uint32(payloadSize)
	}

	// Replay the writes in REVERSE order under a single lock — same
	// invariant as production where store.Lock serializes Writes but
	// reserve() decouples reservation order from execution order. The
	// largest-offset Write lands first; without monotonic CAS, the
	// smallest-offset Write would then shrink the high-water to
	// (start + len) of the smallest reservation.
	var lock sync.Mutex
	highWater := uint32(0)
	for i := entries - 1; i >= 0; i-- {
		start := starts[i]
		lock.Lock()
		require.NoError(t, s.Write(start, payload))
		end := start + uint32(payloadSize)
		if end > highWater {
			highWater = end
		}
		hw := uint32(s.Size())
		require.GreaterOrEqual(t, hw, highWater,
			"high-water=%d shrunk below %d after Write at start=%d", hw, highWater, start)
		lock.Unlock()
	}

	// Final high-water must cover the largest reservation.
	require.Equal(t, int64(cursor), s.Size(), "final high-water must cover all reservations")
}

// TestSegmentFreshOpenSizeIsZero locks the new high-water invariant
// introduced when Segment was made a generic substrate: Open MUST initialize
// the logical high-water to 0 even though the on-disk file may have been
// preallocated to MaxSz. Pre-fix this test would observe Size == MaxSz and a
// Read at offset 0 of length MaxSz would return capacity-bounded mmap bytes
// (junk) instead of io.EOF.
func TestSegmentFreshOpenSizeIsZero(t *testing.T) {
	dir := t.TempDir()
	const maxSz = 4 << 20
	opt := &file.Options{
		FID:      7,
		FileName: filepath.Join(dir, "fresh.seg"),
		MaxSz:    maxSz,
	}
	var s Segment
	require.NoError(t, s.Open(opt))
	defer func() { _ = s.Close() }()

	require.Equal(t, int64(0), s.Size(), "fresh open must have logical high-water = 0")
	require.Equal(t, int64(maxSz), s.Capacity(), "fresh open exposes mapped capacity")

	// Reading any byte before logical writes must EOF, even though the
	// mmap region has capacity backing it.
	_, err := s.Read(0, 1)
	require.ErrorIs(t, err, io.EOF, "Read before any Write must EOF on logical bound")

	// After a Write the high-water advances; a Read inside the new
	// high-water succeeds.
	payload := []byte("hello-fresh")
	require.NoError(t, s.Write(0, payload))
	require.Equal(t, int64(len(payload)), s.Size())
	got, err := s.Read(0, uint32(len(payload)))
	require.NoError(t, err)
	require.Equal(t, payload, got)

	// A Read past the high-water but inside Capacity must still EOF.
	_, err = s.Read(uint32(len(payload)), 1)
	require.ErrorIs(t, err, io.EOF, "Read past high-water must EOF even within Capacity")
}

// TestSegmentLoadSizeFromFile verifies the reload path for sealed segments:
// after Open (size = 0) the caller restores the logical
// high-water from the file's on-disk size, which equals the logical extent
// after DoneWriting / VerifyDir truncation.
func TestSegmentLoadSizeFromFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "reload.seg")

	// Phase 1: write some data, seal the segment, close.
	{
		opt := &file.Options{FID: 1, FileName: path, MaxSz: 4 << 20}
		var s Segment
		require.NoError(t, s.Open(opt))
		require.NoError(t, s.Bootstrap(segmentTestHeaderSize))
		body := []byte("reload-body")
		require.NoError(t, s.Write(uint32(segmentTestHeaderSize), body))
		end := uint32(segmentTestHeaderSize + len(body))
		require.NoError(t, s.DoneWriting(end))
		require.Equal(t, int64(end), s.Size())
		require.NoError(t, s.Close())
	}

	// Phase 2: reopen. Open alone leaves size = 0; LoadSizeFromFile
	// restores it to the sealed file's truncated extent.
	{
		opt := &file.Options{FID: 1, FileName: path, MaxSz: 4 << 20}
		var s Segment
		require.NoError(t, s.Open(opt))
		defer func() { _ = s.Close() }()
		require.Equal(t, int64(0), s.Size(), "Open must not infer size from file stat")
		require.NoError(t, s.LoadSizeFromFile())
		require.Equal(t, int64(segmentTestHeaderSize+len("reload-body")), s.Size())
	}
}
