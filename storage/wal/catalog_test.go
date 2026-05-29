// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package wal_test

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/feichai0017/NoKV/storage/wal"
	"github.com/stretchr/testify/require"
)

// TestManagerRotateWritesIncrementalCatalog asserts that the catalog
// persisted on rotation describes every record appended to the prior
// segment, without relying on a post-hoc rescan.
func TestManagerRotateWritesIncrementalCatalog(t *testing.T) {
	dir := t.TempDir()
	mgr, err := wal.Open(wal.Config{Dir: dir})
	require.NoError(t, err)
	defer func() { _ = mgr.Close() }()

	// Use distinct payloads + raft group ids so we exercise the
	// recordGroupID branch alongside plain record types.
	groupPayload := func(id uint64, suffix byte) []byte {
		var buf [binary.MaxVarintLen64 + 1]byte
		n := binary.PutUvarint(buf[:], id)
		out := append([]byte(nil), buf[:n]...)
		return append(out, suffix)
	}
	for i := range byte(5) {
		_, err := mgr.AppendRecords(wal.DurabilityBuffered,
			wal.Record{Type: wal.RecordTypeRaftEntry, Payload: groupPayload(uint64(i+1), 'a'+i)},
		)
		require.NoError(t, err)
	}
	require.NoError(t, mgr.Rotate())

	// .idx file must exist immediately after rotation, with non-trivial size.
	info, err := os.Stat(filepath.Join(dir, "00001.wal.idx"))
	require.NoError(t, err)
	require.Greater(t, info.Size(), int64(16))

	// Filtered replay walks the on-disk catalog. If the incremental
	// catalog were missing or malformed we'd see fewer than 5 hits.
	var seen []uint64
	require.NoError(t, mgr.ReplayFiltered(
		func(info wal.EntryInfo) bool { return info.Type == wal.RecordTypeRaftEntry },
		func(info wal.EntryInfo, _ []byte) error {
			seen = append(seen, info.GroupID)
			return nil
		},
	))
	require.Equal(t, []uint64{1, 2, 3, 4, 5}, seen)
}

// TestManagerResumePreservesIncrementalCatalog ensures that reopening
// a WAL with an existing active segment loads the in-memory catalog
// from disk (not just from a future rescan) and that subsequent appends
// extend it correctly.
func TestManagerResumePreservesIncrementalCatalog(t *testing.T) {
	dir := t.TempDir()
	mgr, err := wal.Open(wal.Config{Dir: dir})
	require.NoError(t, err)

	first := []byte("before-restart")
	_, err = mgr.AppendRecords(wal.DurabilityFlushed,
		wal.Record{Type: wal.RecordTypeEntry, Payload: first},
	)
	require.NoError(t, err)
	// Close persists the active segment's catalog via persistActiveCatalogLocked.
	require.NoError(t, mgr.Close())

	mgr2, err := wal.Open(wal.Config{Dir: dir})
	require.NoError(t, err)
	defer func() { _ = mgr2.Close() }()

	second := []byte("after-restart")
	_, err = mgr2.AppendRecords(wal.DurabilityFlushed,
		wal.Record{Type: wal.RecordTypeEntry, Payload: second},
	)
	require.NoError(t, err)
	require.NoError(t, mgr2.Rotate())

	// Both records must be visible: the resumed segment carried the prior
	// catalog forward, the new append extended it, and rotation persisted
	// the combined view.
	got := visiblePayloads(t, dir, wal.RecordTypeEntry)
	require.Equal(t, [][]byte{first, second}, got)
}

// TestManagerResumeRebuildsBrokenCatalog confirms the recovery path:
// when the on-disk .idx is corrupt, resume falls back to a one-time
// rescan that both populates the in-memory catalog and rewrites the
// .idx so future opens stay fast.
func TestManagerResumeRebuildsBrokenCatalog(t *testing.T) {
	dir := t.TempDir()
	mgr, err := wal.Open(wal.Config{Dir: dir})
	require.NoError(t, err)
	_, err = mgr.AppendRecords(wal.DurabilityFlushed,
		wal.Record{Type: wal.RecordTypeEntry, Payload: []byte("survives-rebuild")},
	)
	require.NoError(t, err)
	require.NoError(t, mgr.Close())

	idxPath := filepath.Join(dir, "00001.wal.idx")
	require.NoError(t, os.WriteFile(idxPath, []byte("garbage"), 0o644))

	mgr2, err := wal.Open(wal.Config{Dir: dir})
	require.NoError(t, err)
	defer func() { _ = mgr2.Close() }()

	// Append to confirm the in-memory catalog was rebuilt and is now
	// being extended; rotation persists the combined catalog.
	_, err = mgr2.AppendRecords(wal.DurabilityFlushed,
		wal.Record{Type: wal.RecordTypeEntry, Payload: []byte("post-rebuild")},
	)
	require.NoError(t, err)
	require.NoError(t, mgr2.Rotate())

	got := visiblePayloads(t, dir, wal.RecordTypeEntry)
	require.Equal(t, [][]byte{[]byte("survives-rebuild"), []byte("post-rebuild")}, got)

	info, err := os.Stat(idxPath)
	require.NoError(t, err)
	require.Greater(t, info.Size(), int64(len("garbage")))
}
