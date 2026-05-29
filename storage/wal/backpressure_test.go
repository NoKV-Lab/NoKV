// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package wal_test

import (
	"errors"
	"testing"

	"github.com/feichai0017/NoKV/storage/wal"
	"github.com/stretchr/testify/require"
)

func TestManagerBackpressureRejectsSegmentGrowthPastHardCap(t *testing.T) {
	mgr, err := wal.Open(wal.Config{
		Dir:         t.TempDir(),
		SegmentSize: 64 << 10,
		MaxSegments: 1,
	})
	require.NoError(t, err)
	defer func() { _ = mgr.Close() }()

	payload := make([]byte, 60<<10)
	_, err = mgr.AppendRecords(wal.DurabilityBuffered, wal.Record{Type: wal.RecordTypeEntry, Payload: payload})
	require.NoError(t, err)

	_, err = mgr.AppendRecords(wal.DurabilityBuffered, wal.Record{Type: wal.RecordTypeEntry, Payload: make([]byte, 8<<10)})
	require.ErrorIs(t, err, wal.ErrWALBackpressure)
	require.Equal(t, uint32(1), mgr.ActiveSegment())
}

func TestManagerBackpressureAllowsGrowthAfterSegmentRemoval(t *testing.T) {
	mgr, err := wal.Open(wal.Config{
		Dir:         t.TempDir(),
		SegmentSize: 64 << 10,
		MaxSegments: 2,
	})
	require.NoError(t, err)
	defer func() { _ = mgr.Close() }()

	payload := make([]byte, 60<<10)
	_, err = mgr.AppendRecords(wal.DurabilityBuffered, wal.Record{Type: wal.RecordTypeEntry, Payload: payload})
	require.NoError(t, err)
	_, err = mgr.AppendRecords(wal.DurabilityBuffered, wal.Record{Type: wal.RecordTypeEntry, Payload: make([]byte, 8<<10)})
	require.NoError(t, err)
	require.Equal(t, uint32(2), mgr.ActiveSegment())

	err = mgr.RemoveSegment(1)
	require.NoError(t, err)
	require.NoError(t, mgr.Rotate())
	require.Equal(t, uint32(3), mgr.ActiveSegment())

	err = mgr.RemoveSegment(3)
	require.True(t, errors.Is(err, wal.ErrSegmentRetained))
}
