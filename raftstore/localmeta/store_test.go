// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package localmeta

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/feichai0017/NoKV/meta/topology"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/feichai0017/NoKV/storage/vfs"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func withPendingRootEventLimit(t *testing.T, limit int, fn func()) {
	t.Helper()
	prev := maxPendingRootEvents
	maxPendingRootEvents = limit
	t.Cleanup(func() {
		maxPendingRootEvents = prev
	})
	fn()
}

func withBlockedRootEventLimit(t *testing.T, limit int, fn func()) {
	t.Helper()
	prev := maxBlockedRootEvents
	maxBlockedRootEvents = limit
	t.Cleanup(func() {
		maxBlockedRootEvents = prev
	})
	fn()
}

func TestLocalStorePersistsRegions(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenLocalStore(dir, nil)
	require.NoError(t, err)

	meta := RegionMeta{
		ID:       11,
		StartKey: []byte("a"),
		EndKey:   []byte("m"),
		Epoch: metaregion.Epoch{
			Version:     1,
			ConfVersion: 1,
		},
	}
	require.NoError(t, store.SaveRegion(meta))
	require.NoError(t, store.Close())

	reopened, err := OpenLocalStore(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })

	snapshot := reopened.Snapshot()
	got, ok := snapshot[meta.ID]
	require.True(t, ok)
	require.Equal(t, meta.ID, got.ID)
	require.Equal(t, meta.StartKey, got.StartKey)
	require.Equal(t, meta.EndKey, got.EndKey)
	require.FileExists(t, filepath.Join(dir, ReplicaStateFileName))
}

func TestLocalStoreDeleteRegion(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenLocalStore(dir, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()

	meta := RegionMeta{ID: 7}
	require.NoError(t, store.SaveRegion(meta))
	require.NoError(t, store.DeleteRegion(meta.ID))
	_, ok := store.Snapshot()[meta.ID]
	require.False(t, ok)
}

func TestLocalStorePersistsRaftPointers(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenLocalStore(dir, nil)
	require.NoError(t, err)

	ptr := RaftLogPointer{
		GroupID:        7,
		Segment:        3,
		Offset:         2048,
		AppliedIndex:   42,
		AppliedTerm:    5,
		Committed:      41,
		SnapshotIndex:  64,
		SnapshotTerm:   7,
		TruncatedIndex: 11,
		TruncatedTerm:  2,
	}
	require.NoError(t, store.SaveRaftPointer(ptr))
	require.NoError(t, store.Close())

	reopened, err := OpenLocalStore(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })

	got, ok := reopened.RaftPointer(ptr.GroupID)
	require.True(t, ok)
	require.Equal(t, ptr, got)
	require.FileExists(t, filepath.Join(dir, RaftProgressFileName))
}

func TestLocalStoreRaftPointerDoesNotPersistUnrelatedCatalogs(t *testing.T) {
	dir := t.TempDir()
	injected := errors.New("unexpected replica catalog write")
	fs := vfs.NewFaultFSWithPolicy(vfs.OSFS{}, vfs.NewFaultPolicy(
		vfs.FailOnceRule(vfs.OpOpenFile, filepath.Join(dir, ReplicaStateFileName)+".tmp", injected),
	))
	store, err := OpenLocalStore(dir, fs)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()

	require.NoError(t, store.SaveRaftPointer(RaftLogPointer{
		GroupID:      7,
		Segment:      3,
		Offset:       2048,
		AppliedIndex: 42,
		AppliedTerm:  5,
	}))
	require.FileExists(t, filepath.Join(dir, RaftProgressFileName))
	_, statErr := os.Stat(filepath.Join(dir, ReplicaStateFileName))
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestLocalStoreCoalescesRaftPointerWithinSegment(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenLocalStore(dir, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()

	first := RaftLogPointer{
		GroupID:      9,
		Segment:      2,
		Offset:       128,
		AppliedIndex: 11,
		AppliedTerm:  2,
	}
	require.NoError(t, store.SaveRaftPointer(first))

	injected := errors.New("unexpected progress checkpoint")
	store.fs = vfs.NewFaultFSWithPolicy(vfs.OSFS{}, vfs.NewFaultPolicy(
		vfs.FailOnceRule(vfs.OpOpenFile, filepath.Join(dir, RaftProgressFileName)+".tmp", injected),
	))
	advanced := first
	advanced.Offset = 256
	advanced.AppliedIndex = 12
	require.NoError(t, store.SaveRaftPointer(advanced))

	got, ok := store.RaftPointer(first.GroupID)
	require.True(t, ok)
	require.Equal(t, advanced, got)

	reopened, err := OpenLocalStore(dir, nil)
	require.NoError(t, err)
	disk, ok := reopened.RaftPointer(first.GroupID)
	require.True(t, ok)
	require.Equal(t, first, disk)
	require.NoError(t, reopened.Close())

	store.fs = vfs.OSFS{}
	nextSegment := advanced
	nextSegment.Segment = 3
	nextSegment.Offset = 64
	nextSegment.AppliedIndex = 13
	require.NoError(t, store.SaveRaftPointer(nextSegment))

	reopened, err = OpenLocalStore(dir, nil)
	require.NoError(t, err)
	disk, ok = reopened.RaftPointer(first.GroupID)
	require.True(t, ok)
	require.Equal(t, nextSegment, disk)
	require.NoError(t, reopened.Close())
}

func TestLocalStorePersistsFirstCommittedRaftPointerBoundary(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenLocalStore(dir, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()

	snapshotPtr := RaftLogPointer{
		GroupID:       9,
		Segment:       2,
		Offset:        128,
		SnapshotIndex: 1,
		SnapshotTerm:  1,
	}
	require.NoError(t, store.SaveRaftPointer(snapshotPtr))

	committedPtr := snapshotPtr
	committedPtr.Offset = 256
	committedPtr.Committed = 1
	require.NoError(t, store.SaveRaftPointer(committedPtr))

	reopened, err := OpenLocalStore(dir, nil)
	require.NoError(t, err)
	disk, ok := reopened.RaftPointer(snapshotPtr.GroupID)
	require.True(t, ok)
	require.Equal(t, committedPtr, disk)
	require.NoError(t, reopened.Close())
}

func TestLocalStoreRetriesFailedRaftPointerCheckpoint(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenLocalStore(dir, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()

	first := RaftLogPointer{
		GroupID:      9,
		Segment:      2,
		Offset:       128,
		AppliedIndex: 11,
		AppliedTerm:  2,
	}
	require.NoError(t, store.SaveRaftPointer(first))

	injected := errors.New("progress checkpoint failed")
	store.fs = vfs.NewFaultFSWithPolicy(vfs.OSFS{}, vfs.NewFaultPolicy(
		vfs.FailOnceRule(vfs.OpOpenFile, filepath.Join(dir, RaftProgressFileName)+".tmp", injected),
	))
	nextSegment := first
	nextSegment.Segment = 3
	nextSegment.Offset = 64
	nextSegment.AppliedIndex = 12
	require.ErrorIs(t, store.SaveRaftPointer(nextSegment), injected)

	got, ok := store.RaftPointer(first.GroupID)
	require.True(t, ok)
	require.Equal(t, nextSegment, got)

	reopened, err := OpenLocalStore(dir, nil)
	require.NoError(t, err)
	disk, ok := reopened.RaftPointer(first.GroupID)
	require.True(t, ok)
	require.Equal(t, first, disk)
	require.NoError(t, reopened.Close())

	require.NoError(t, store.SaveRaftPointer(nextSegment))
	reopened, err = OpenLocalStore(dir, nil)
	require.NoError(t, err)
	disk, ok = reopened.RaftPointer(first.GroupID)
	require.True(t, ok)
	require.Equal(t, nextSegment, disk)
	require.NoError(t, reopened.Close())
}

func TestLocalStorePersistsPendingRootEvents(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenLocalStore(dir, nil)
	require.NoError(t, err)

	event := rootevent.PeerAdded(11, 2, 22, topology.Descriptor{
		RegionID: 11,
		StartKey: []byte("a"),
		EndKey:   []byte("m"),
	})
	require.NoError(t, store.SavePendingRootEvent(PendingRootEvent{
		Sequence: 7,
		Event:    event,
	}))
	require.NoError(t, store.Close())

	reopened, err := OpenLocalStore(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })

	pending := reopened.PendingRootEvents()
	require.Len(t, pending, 1)
	require.Equal(t, uint64(7), pending[0].Sequence)
	require.Equal(t, rootstate.TransitionIDFromEvent(event), rootstate.TransitionIDFromEvent(pending[0].Event))
	require.FileExists(t, filepath.Join(dir, PendingRootEventsFileName))
}

func TestLocalStoreDeletesPendingRootEvents(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenLocalStore(dir, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()

	event := rootevent.RegionTombstoned(9)
	require.NoError(t, store.SavePendingRootEvent(PendingRootEvent{
		Sequence: 3,
		Event:    event,
	}))
	require.NoError(t, store.DeletePendingRootEvent(3))
	require.Empty(t, store.PendingRootEvents())
}

func TestLocalStoreRejectsPendingRootEventOverflow(t *testing.T) {
	withPendingRootEventLimit(t, 8, func() {
		dir := t.TempDir()
		store, err := OpenLocalStore(dir, nil)
		require.NoError(t, err)
		defer func() { require.NoError(t, store.Close()) }()

		for seq := uint64(1); seq <= uint64(maxPendingRootEvents); seq++ {
			require.NoError(t, store.SavePendingRootEvent(PendingRootEvent{
				Sequence: seq,
				Event:    rootevent.RegionTombstoned(seq),
			}))
		}
		err = store.SavePendingRootEvent(PendingRootEvent{
			Sequence: uint64(maxPendingRootEvents + 1),
			Event:    rootevent.RegionTombstoned(uint64(maxPendingRootEvents + 1)),
		})
		require.ErrorContains(t, err, "pending rooted event limit exceeded")
		require.Len(t, store.PendingRootEvents(), maxPendingRootEvents)
	})
}

func TestLocalStoreAllowsPendingRootEventOverwriteAtLimit(t *testing.T) {
	withPendingRootEventLimit(t, 8, func() {
		dir := t.TempDir()
		store, err := OpenLocalStore(dir, nil)
		require.NoError(t, err)
		defer func() { require.NoError(t, store.Close()) }()

		for seq := uint64(1); seq <= uint64(maxPendingRootEvents); seq++ {
			require.NoError(t, store.SavePendingRootEvent(PendingRootEvent{
				Sequence: seq,
				Event:    rootevent.RegionTombstoned(seq),
			}))
		}
		require.NoError(t, store.SavePendingRootEvent(PendingRootEvent{
			Sequence: uint64(maxPendingRootEvents),
			Event:    rootevent.StoreJoined(uint64(maxPendingRootEvents)),
		}))
		pending := store.PendingRootEvents()
		require.Len(t, pending, maxPendingRootEvents)
		require.Equal(t, rootevent.KindStoreJoined, pending[len(pending)-1].Event.Kind)
	})
}

func TestLocalStorePersistsPendingSchedulerOperations(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenLocalStore(dir, nil)
	require.NoError(t, err)

	require.NoError(t, store.SavePendingSchedulerOperation(PendingSchedulerOperation{
		Kind:         PendingSchedulerOperationLeaderTransfer,
		RegionID:     17,
		SourcePeerID: 101,
		TargetPeerID: 202,
		Attempts:     3,
	}))
	require.NoError(t, store.Close())

	reopened, err := OpenLocalStore(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })

	ops := reopened.PendingSchedulerOperations()
	require.Len(t, ops, 1)
	require.Equal(t, PendingSchedulerOperationLeaderTransfer, ops[0].Kind)
	require.Equal(t, uint64(17), ops[0].RegionID)
	require.Equal(t, uint64(101), ops[0].SourcePeerID)
	require.Equal(t, uint64(202), ops[0].TargetPeerID)
	require.Equal(t, uint32(3), ops[0].Attempts)
	require.FileExists(t, filepath.Join(dir, PendingSchedulerOperationsFileName))
}

func TestLocalStorePersistsPendingSplitAndMergeSchedulerOperations(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenLocalStore(dir, nil)
	require.NoError(t, err)

	child := RegionMeta{
		ID:       18,
		StartKey: []byte("m"),
		EndKey:   []byte("z"),
		Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 101}, {StoreID: 2, PeerID: 201}},
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
	}
	require.NoError(t, store.SavePendingSchedulerOperation(PendingSchedulerOperation{
		Kind:       PendingSchedulerOperationSplitRegion,
		RegionID:   17,
		SplitKey:   []byte("m"),
		SplitChild: child,
		Attempts:   2,
	}))
	require.NoError(t, store.SavePendingSchedulerOperation(PendingSchedulerOperation{
		Kind:           PendingSchedulerOperationMergeRegion,
		RegionID:       17,
		SourceRegionID: 18,
		Attempts:       4,
	}))
	require.NoError(t, store.Close())

	reopened, err := OpenLocalStore(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })

	ops := reopened.PendingSchedulerOperations()
	require.Len(t, ops, 2)
	require.Equal(t, PendingSchedulerOperationMergeRegion, ops[0].Kind)
	require.Equal(t, uint64(18), ops[0].SourceRegionID)
	require.Equal(t, uint32(4), ops[0].Attempts)
	require.Equal(t, PendingSchedulerOperationSplitRegion, ops[1].Kind)
	require.Equal(t, []byte("m"), ops[1].SplitKey)
	require.Equal(t, uint64(18), ops[1].SplitChild.ID)
	require.Equal(t, []metaregion.Peer{{StoreID: 1, PeerID: 101}, {StoreID: 2, PeerID: 201}}, ops[1].SplitChild.Peers)
	require.Equal(t, uint32(2), ops[1].Attempts)
}

func TestLocalStoreMovesPendingRootEventToBlocked(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenLocalStore(dir, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()

	event := rootevent.RegionTombstoned(23)
	require.NoError(t, store.SavePendingRootEvent(PendingRootEvent{
		Sequence: 4,
		Event:    event,
	}))
	require.NoError(t, store.MovePendingRootEventToBlocked(4, BlockedRootEvent{
		Sequence:     4,
		Event:        event,
		TransitionID: rootstate.TransitionIDFromEvent(event),
		LastError:    "permanent reject",
	}))

	require.Empty(t, store.PendingRootEvents())
	blocked := store.BlockedRootEvents()
	require.Len(t, blocked, 1)
	require.Equal(t, uint64(4), blocked[0].Sequence)
	require.Equal(t, "permanent reject", blocked[0].LastError)
	require.Equal(t, rootstate.TransitionIDFromEvent(event), blocked[0].TransitionID)
	require.FileExists(t, filepath.Join(dir, BlockedRootEventsFileName))
}

func TestLocalStoreRejectsMismatchedBlockedRootEventSequence(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenLocalStore(dir, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()

	event := rootevent.RegionTombstoned(23)
	require.NoError(t, store.SavePendingRootEvent(PendingRootEvent{
		Sequence: 4,
		Event:    event,
	}))
	err = store.MovePendingRootEventToBlocked(4, BlockedRootEvent{
		Sequence:     5,
		Event:        event,
		TransitionID: rootstate.TransitionIDFromEvent(event),
		LastError:    "permanent reject",
	})
	require.ErrorContains(t, err, "sequence mismatch")
	require.Len(t, store.PendingRootEvents(), 1)
	require.Empty(t, store.BlockedRootEvents())
}

func TestLocalStoreHelpersAndSnapshots(t *testing.T) {
	var nilStore *Store
	require.Empty(t, nilStore.WorkDir())
	require.Nil(t, nilStore.RaftPointerSnapshot())
	require.Nil(t, nilStore.DurableRaftPointerSnapshot())
	require.True(t, nilStore.Empty())

	dir := t.TempDir()
	store, err := OpenLocalStore(dir, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()

	require.Equal(t, dir, store.WorkDir())
	require.True(t, store.Empty())

	meta := RegionMeta{
		ID:       33,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    metaregion.Epoch{Version: 2, ConfVersion: 3},
	}
	require.NoError(t, store.SaveRegion(meta))
	require.False(t, store.Empty())

	snapshot := store.Snapshot()
	require.Len(t, snapshot, 1)
	snapshot[meta.ID] = RegionMeta{ID: meta.ID, StartKey: []byte("mutated")}
	require.Equal(t, []byte("a"), store.Snapshot()[meta.ID].StartKey)

	ptr := RaftLogPointer{GroupID: 9, Segment: 2, Offset: 128, AppliedIndex: 11}
	require.NoError(t, store.SaveRaftPointer(ptr))
	ptrs := store.RaftPointerSnapshot()
	require.Equal(t, ptr, ptrs[ptr.GroupID])
	ptrs[ptr.GroupID] = RaftLogPointer{}
	got, ok := store.RaftPointer(ptr.GroupID)
	require.True(t, ok)
	require.Equal(t, ptr, got)

	replayable := ptr
	replayable.Offset = 256
	replayable.AppliedIndex = 12
	require.NoError(t, store.SaveRaftPointer(replayable))
	require.Equal(t, replayable, store.RaftPointerSnapshot()[ptr.GroupID])
	require.Equal(t, ptr, store.DurableRaftPointerSnapshot()[ptr.GroupID])

	boundary := replayable
	boundary.Segment = 3
	boundary.Offset = 64
	require.NoError(t, store.SaveRaftPointer(boundary))
	require.Equal(t, boundary, store.DurableRaftPointerSnapshot()[ptr.GroupID])
}

func TestLocalStoreDeletePendingSchedulerOperation(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenLocalStore(dir, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()

	require.NoError(t, store.DeletePendingSchedulerOperation(PendingSchedulerOperationUnknown, 0))

	op := PendingSchedulerOperation{
		Kind:         PendingSchedulerOperationLeaderTransfer,
		RegionID:     19,
		SourcePeerID: 301,
		TargetPeerID: 302,
	}
	require.NoError(t, store.SavePendingSchedulerOperation(op))
	require.Len(t, store.PendingSchedulerOperations(), 1)

	require.NoError(t, store.DeletePendingSchedulerOperation(op.Kind, op.RegionID))
	require.Empty(t, store.PendingSchedulerOperations())
}

func TestLoadBlockedRootEventCatalogLimit(t *testing.T) {
	withBlockedRootEventLimit(t, 1, func() {
		dir := t.TempDir()
		path := filepath.Join(dir, BlockedRootEventsFileName)
		catalog := &metapb.BlockedRootEventCatalog{
			Entries: []*metapb.BlockedRootEvent{
				{
					Sequence:     1,
					Event:        metawire.RootEventToProto(rootevent.RegionTombstoned(1)),
					TransitionId: "t1",
					LastError:    "stale",
				},
				{
					Sequence:     2,
					Event:        metawire.RootEventToProto(rootevent.RegionTombstoned(2)),
					TransitionId: "t2",
					LastError:    "stale",
				},
			},
		}
		payload, err := proto.Marshal(catalog)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(path, payload, 0o644))

		_, err = loadBlockedRootEventCatalog(vfs.OSFS{}, dir)
		require.ErrorContains(t, err, "blocked rooted event catalog exceeds limit")
	})
}

func TestPersistProtoFileRemovesTmpOnFileFailures(t *testing.T) {
	t.Run("write", func(t *testing.T) {
		dir := t.TempDir()
		name := "persist-write.binpb"
		tmp := filepath.Join(dir, name) + ".tmp"
		injected := errors.New("write injected")
		fs := vfs.NewFaultFSWithPolicy(vfs.OSFS{}, vfs.NewFaultPolicy(
			vfs.FailOnceRule(vfs.OpFileWrite, tmp, injected),
		))

		err := persistProtoFile(fs, dir, name, []byte("payload"))
		require.ErrorIs(t, err, injected)
		_, statErr := os.Stat(tmp)
		require.ErrorIs(t, statErr, os.ErrNotExist)
	})

	t.Run("sync", func(t *testing.T) {
		dir := t.TempDir()
		name := "persist-sync.binpb"
		tmp := filepath.Join(dir, name) + ".tmp"
		injected := errors.New("sync injected")
		fs := vfs.NewFaultFSWithPolicy(vfs.OSFS{}, vfs.NewFaultPolicy(
			vfs.FailOnceRule(vfs.OpFileSync, tmp, injected),
		))

		err := persistProtoFile(fs, dir, name, []byte("payload"))
		require.ErrorIs(t, err, injected)
		_, statErr := os.Stat(tmp)
		require.ErrorIs(t, statErr, os.ErrNotExist)
	})

	t.Run("close", func(t *testing.T) {
		dir := t.TempDir()
		name := "persist-close.binpb"
		tmp := filepath.Join(dir, name) + ".tmp"
		injected := errors.New("close injected")
		fs := vfs.NewFaultFSWithPolicy(vfs.OSFS{}, vfs.NewFaultPolicy(
			vfs.FailOnceRule(vfs.OpFileClose, tmp, injected),
		))

		err := persistProtoFile(fs, dir, name, []byte("payload"))
		require.ErrorIs(t, err, injected)
		_, statErr := os.Stat(tmp)
		require.ErrorIs(t, statErr, os.ErrNotExist)
	})
}
