// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package file

import (
	"os"
	"path/filepath"
	"testing"

	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	"github.com/feichai0017/NoKV/storage/vfs"
	"github.com/stretchr/testify/require"
)

func TestEventLogReadCommittedIgnoresTornTail(t *testing.T) {
	dir := t.TempDir()
	log := fileEventLog{fs: vfs.Ensure(nil), workdir: dir}
	records := []rootstorage.CommittedEvent{
		{Cursor: rootstate.Cursor{Term: 1, Index: 1}, Event: rootevent.StoreJoined(1)},
		{Cursor: rootstate.Cursor{Term: 1, Index: 2}, Event: rootevent.StoreJoined(2)},
	}
	cleanEnd, err := log.AppendCommitted(records...)
	require.NoError(t, err)

	f, err := os.OpenFile(filepath.Join(dir, LogFileName), os.O_WRONLY|os.O_APPEND, 0)
	require.NoError(t, err)
	_, err = f.Write([]byte{0xaa, 0xbb, 0xcc})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	tail, err := log.ReadCommitted(0)
	require.NoError(t, err)
	require.Len(t, tail.Records, 2)
	require.Equal(t, rootstate.Cursor{Term: 1, Index: 1}, tail.Records[0].Cursor)
	require.Equal(t, rootstate.Cursor{Term: 1, Index: 2}, tail.TailCursor(rootstate.Cursor{}))
	require.Equal(t, int64(0), tail.StartOffset)
	require.Greater(t, tail.EndOffset, cleanEnd)
}

func TestEventLogReadCommittedRejectsCorruptFrame(t *testing.T) {
	dir := t.TempDir()
	log := fileEventLog{fs: vfs.Ensure(nil), workdir: dir}
	record := rootstorage.CommittedEvent{
		Cursor: rootstate.Cursor{Term: 1, Index: 1},
		Event:  rootevent.StoreJoined(1),
	}
	_, err := log.AppendCommitted(record)
	require.NoError(t, err)

	f, err := os.OpenFile(filepath.Join(dir, LogFileName), os.O_RDWR, 0)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte{0x01}, recordHeaderSize)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	_, err = log.ReadCommitted(0)
	require.ErrorContains(t, err, "checksum mismatch")
}
