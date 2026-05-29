// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package wal_test

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/storage/wal"
	"github.com/stretchr/testify/require"
)

func TestManagerRetentionBlocksSegmentRemoval(t *testing.T) {
	dir := t.TempDir()
	mgr, err := wal.Open(wal.Config{Dir: dir})
	require.NoError(t, err)
	defer func() { _ = mgr.Close() }()

	appendRotatedRaftRecords(t, mgr, 3)
	require.NoError(t, mgr.RegisterRetention("test", func() wal.RetentionMark {
		return wal.RetentionMark{FirstSegment: 2}
	}))

	require.NoError(t, mgr.RemoveSegment(1))
	err = mgr.RemoveSegment(2)
	require.ErrorIs(t, err, wal.ErrSegmentRetained)
}

func TestWatchdogFiltersRetainedSegments(t *testing.T) {
	dir := t.TempDir()
	mgr, err := wal.Open(wal.Config{Dir: filepath.Join(dir, "wal")})
	require.NoError(t, err)
	defer func() { _ = mgr.Close() }()

	appendRotatedRaftRecords(t, mgr, 4)
	require.NoError(t, mgr.RegisterRetention("test", func() wal.RetentionMark {
		return wal.RetentionMark{FirstSegment: 2}
	}))

	wd := wal.NewWatchdog(wal.WatchdogConfig{
		Manager:      mgr,
		Interval:     time.Hour,
		MinRemovable: 1,
		MaxBatch:     4,
		WarnRatio:    0,
		WarnSegments: 0,
	})

	wd.RunOnce()

	files, err := mgr.ListSegments()
	require.NoError(t, err)
	var names []string
	for _, f := range files {
		names = append(names, filepath.Base(f))
	}
	joined := strings.Join(names, ",")
	require.NotContains(t, joined, "00001.wal")
	require.Contains(t, joined, "00002.wal")
	require.Contains(t, joined, "00003.wal")

	err = mgr.RemoveSegment(2)
	require.True(t, errors.Is(err, wal.ErrSegmentRetained))
}

func appendRotatedRaftRecords(t *testing.T, mgr *wal.Manager, count int) {
	t.Helper()
	record := wal.Record{Type: wal.RecordTypeRaftEntry, Payload: []byte("raft-entry")}
	for i := range count {
		_, err := mgr.AppendRecords(wal.DurabilityBuffered, record)
		require.NoError(t, err)
		if i < count-1 {
			require.NoError(t, mgr.Rotate())
		}
	}
	require.NoError(t, mgr.Sync())
}
