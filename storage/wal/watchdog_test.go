// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package wal

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWatchdogAutoGC(t *testing.T) {
	dir := t.TempDir()
	mgr, err := Open(Config{Dir: filepath.Join(dir, "wal")})
	require.NoError(t, err)
	defer func() { _ = mgr.Close() }()

	record := Record{Type: RecordTypeRaftEntry, Payload: []byte("raft-entry")}
	for i := range 4 {
		_, err := mgr.AppendRecords(DurabilityBuffered, record)
		require.NoError(t, err)
		if i < 3 {
			require.NoError(t, mgr.Rotate())
		}
	}
	require.NoError(t, mgr.Sync())

	wd := NewWatchdog(WatchdogConfig{
		Manager:      mgr,
		Interval:     time.Hour,
		MinRemovable: 1,
		MaxBatch:     2,
		WarnRatio:    0,
		WarnSegments: 0,
	})

	wd.RunOnce()

	snap := wd.Snapshot()
	require.GreaterOrEqual(t, snap.AutoRuns, uint64(1))
	require.GreaterOrEqual(t, snap.SegmentsRemoved, uint64(1))
	require.False(t, snap.Warning)

	files, err := mgr.ListSegments()
	require.NoError(t, err)
	require.NotEmpty(t, files)
	var ids []string
	for _, f := range files {
		ids = append(ids, filepath.Base(f))
	}
	joined := strings.Join(ids, ",")
	require.NotContains(t, joined, "00001.wal")
	require.NotContains(t, joined, "00002.wal")
}

func TestWatchdogConfigResolveDefaults(t *testing.T) {
	cfg := (WatchdogConfig{Manager: &Manager{}}).resolveDefaults()
	require.Equal(t, defaultWatchdogInterval, cfg.Interval)
	require.Equal(t, defaultWatchdogMinRemovable, cfg.MinRemovable)
	require.Equal(t, defaultWatchdogMaxBatch, cfg.MaxBatch)
}

func TestWatchdogTypedWarning(t *testing.T) {
	dir := t.TempDir()
	mgr, err := Open(Config{Dir: filepath.Join(dir, "wal")})
	require.NoError(t, err)
	defer func() { _ = mgr.Close() }()

	record := Record{Type: RecordTypeRaftEntry, Payload: []byte("raft-entry")}
	_, err = mgr.AppendRecords(DurabilityBuffered, record)
	require.NoError(t, err)
	require.NoError(t, mgr.Sync())

	wd := NewWatchdog(WatchdogConfig{
		Manager:      mgr,
		Interval:     time.Hour,
		MinRemovable: 10,
		MaxBatch:     0,
		WarnRatio:    0.1,
		WarnSegments: 0,
	})
	wd.RunOnce()

	snap := wd.Snapshot()
	require.True(t, snap.Warning)
	require.NotEmpty(t, snap.WarningReason)
	require.Contains(t, snap.WarningReason, "typed record ratio")
	require.Equal(t, uint64(0), snap.AutoRuns)
	require.Greater(t, snap.TypedRatio, 0.0)
}
