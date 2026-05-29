// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package file

import (
	"context"
	"testing"

	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	"github.com/feichai0017/NoKV/storage/vfs"
	"github.com/stretchr/testify/require"
)

func TestStoreReadCommittedReportsFellBehindCompaction(t *testing.T) {
	dir := t.TempDir()
	store := NewStore(vfs.Ensure(nil), dir)

	rec1 := rootstorage.CommittedEvent{
		Cursor: rootstate.Cursor{Term: 1, Index: 1},
		Event:  rootevent.StoreJoined(1),
	}
	rec2 := rootstorage.CommittedEvent{
		Cursor: rootstate.Cursor{Term: 1, Index: 2},
		Event:  rootevent.StoreJoined(2),
	}
	rec3 := rootstorage.CommittedEvent{
		Cursor: rootstate.Cursor{Term: 1, Index: 3},
		Event:  rootevent.StoreJoined(3),
	}

	offsetAfterFirst, err := store.AppendCommitted(context.Background(), rec1)
	require.NoError(t, err)
	_, err = store.AppendCommitted(context.Background(), rec2, rec3)
	require.NoError(t, err)

	require.NoError(t, store.SaveCheckpoint(rootstorage.Checkpoint{
		Snapshot:   rootstate.Snapshot{},
		TailOffset: offsetAfterFirst,
	}))

	tail, err := store.ReadCommitted(0)
	require.NoError(t, err)
	require.True(t, tail.FellBehind())
	require.Equal(t, int64(0), tail.RequestedOffset)
	require.Equal(t, offsetAfterFirst, tail.StartOffset)
	require.GreaterOrEqual(t, tail.EndOffset, tail.StartOffset)
	require.Len(t, tail.Records, 2)
	require.Equal(t, rootstate.Cursor{Term: 1, Index: 2}, tail.Records[0].Cursor)
	require.Equal(t, rootstate.Cursor{Term: 1, Index: 3}, tail.TailCursor(rootstate.Cursor{}))
}

func TestStoreInstallBootstrapNormalizesTailOrigin(t *testing.T) {
	dir := t.TempDir()
	store := NewStore(vfs.Ensure(nil), dir)

	observed := rootstorage.ObservedCommitted{
		Checkpoint: rootstorage.Checkpoint{
			Snapshot:   rootstate.Snapshot{State: rootstate.State{LastCommitted: rootstate.Cursor{Term: 1, Index: 2}}},
			TailOffset: 64,
		},
		Tail: rootstorage.CommittedTail{
			RequestedOffset: 32,
			StartOffset:     64,
			EndOffset:       96,
			Records: []rootstorage.CommittedEvent{
				{Cursor: rootstate.Cursor{Term: 1, Index: 2}, Event: rootevent.StoreJoined(2)},
			},
		},
	}
	require.NoError(t, store.InstallBootstrap(observed))

	checkpoint, err := store.LoadCheckpoint()
	require.NoError(t, err)
	require.Equal(t, int64(0), checkpoint.TailOffset)

	tail, err := store.ReadCommitted(0)
	require.NoError(t, err)
	require.Len(t, tail.Records, 1)
	require.Equal(t, rootstate.Cursor{Term: 1, Index: 2}, tail.Records[0].Cursor)
}

func TestStoreInstallBootstrapReplaysAfterReopen(t *testing.T) {
	dir := t.TempDir()
	store := NewStore(vfs.Ensure(nil), dir)

	observed := rootstorage.ObservedCommitted{
		Checkpoint: rootstorage.Checkpoint{
			Snapshot:   rootstate.Snapshot{State: rootstate.State{LastCommitted: rootstate.Cursor{Term: 3, Index: 7}}},
			TailOffset: 128,
		},
		Tail: rootstorage.CommittedTail{
			RequestedOffset: 96,
			StartOffset:     128,
			EndOffset:       192,
			Records: []rootstorage.CommittedEvent{
				{Cursor: rootstate.Cursor{Term: 3, Index: 7}, Event: rootevent.StoreJoined(7)},
				{Cursor: rootstate.Cursor{Term: 3, Index: 8}, Event: rootevent.StoreJoined(8)},
			},
		},
	}
	require.NoError(t, store.InstallBootstrap(observed))

	reopened := NewStore(vfs.Ensure(nil), dir)
	checkpoint, err := reopened.LoadCheckpoint()
	require.NoError(t, err)
	require.Equal(t, rootstate.Cursor{Term: 3, Index: 7}, checkpoint.Snapshot.State.LastCommitted)
	require.Equal(t, int64(0), checkpoint.TailOffset)

	tail, err := reopened.ReadCommitted(0)
	require.NoError(t, err)
	require.False(t, tail.FellBehind())
	require.Equal(t, int64(0), tail.StartOffset)
	require.Len(t, tail.Records, 2)
	require.Equal(t, rootstate.Cursor{Term: 3, Index: 7}, tail.Records[0].Cursor)
	require.Equal(t, rootstate.Cursor{Term: 3, Index: 8}, tail.TailCursor(rootstate.Cursor{}))
}
