// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package contract

import (
	"fmt"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/stretchr/testify/require"
)

func TestModelUnlinkKeepsSessionIndexesUntilSessionLifecycleRuns(t *testing.T) {
	model := NewModel("vol")
	require.NoError(t, model.Apply(Operation{
		Kind:   OpCreate,
		Mount:  "vol",
		Parent: model.Root,
		Name:   "file",
		Inode:  10,
		Type:   fsmeta.InodeTypeFile,
		Mode:   0o600,
	}).Err)
	require.NoError(t, model.Apply(Operation{
		Kind:      OpOpenWriteSession,
		Mount:     "vol",
		Inode:     10,
		Session:   "writer-1",
		ExpiresNs: model.NowUnixNs + int64(time.Minute),
	}).Err)

	require.NoError(t, model.Apply(Operation{
		Kind:   OpUnlink,
		Mount:  "vol",
		Parent: model.Root,
		Name:   "file",
	}).Err)

	require.NoError(t, model.CheckInvariants())
	require.NotContains(t, model.inodes, fsmeta.InodeID(10))
	require.Contains(t, model.sessions, sessionKey{inode: 10, session: "writer-1"})
	require.Contains(t, model.owners, fsmeta.InodeID(10))

	require.NoError(t, model.Apply(Operation{
		Kind:    OpCloseSession,
		Mount:   "vol",
		Inode:   10,
		Session: "writer-1",
	}).Err)
	require.NoError(t, model.CheckInvariants())
	require.Empty(t, model.sessions)
	require.Empty(t, model.owners)
}

func TestModelExpiresSessionsAfterTimeAdvance(t *testing.T) {
	model := NewModel("vol")
	require.NoError(t, model.Apply(Operation{
		Kind:   OpCreate,
		Mount:  "vol",
		Parent: model.Root,
		Name:   "file",
		Inode:  10,
		Type:   fsmeta.InodeTypeFile,
		Mode:   0o600,
	}).Err)
	require.NoError(t, model.Apply(Operation{
		Kind:      OpOpenWriteSession,
		Mount:     "vol",
		Inode:     10,
		Session:   "writer-1",
		ExpiresNs: model.NowUnixNs + int64(time.Second),
	}).Err)

	require.NoError(t, model.Apply(Operation{
		Kind:      OpAdvanceTime,
		Mount:     "vol",
		AdvanceNs: int64(2 * time.Second),
	}).Err)
	result := model.Apply(Operation{Kind: OpExpireSessions, Mount: "vol", Limit: 16})

	require.NoError(t, result.Err)
	require.Equal(t, uint64(1), result.Expired)
	require.Empty(t, model.sessions)
	require.Empty(t, model.owners)
	require.NoError(t, model.CheckInvariants())
}

func TestModelExpireStaleOwnerDoesNotRemoveReusedLiveSession(t *testing.T) {
	model := NewModel("vol")
	for _, inode := range []fsmeta.InodeID{10, 11} {
		require.NoError(t, model.Apply(Operation{
			Kind:   OpCreate,
			Mount:  "vol",
			Parent: model.Root,
			Name:   fmt.Sprintf("file-%d", inode),
			Inode:  inode,
			Type:   fsmeta.InodeTypeFile,
			Mode:   0o600,
		}).Err)
	}
	require.NoError(t, model.Apply(Operation{
		Kind:      OpOpenWriteSession,
		Mount:     "vol",
		Inode:     10,
		Session:   "writer-1",
		ExpiresNs: model.NowUnixNs + int64(time.Second),
	}).Err)
	require.NoError(t, model.Apply(Operation{
		Kind:      OpAdvanceTime,
		Mount:     "vol",
		AdvanceNs: int64(2 * time.Second),
	}).Err)
	require.NoError(t, model.Apply(Operation{
		Kind:      OpOpenWriteSession,
		Mount:     "vol",
		Inode:     11,
		Session:   "writer-1",
		ExpiresNs: model.NowUnixNs + int64(time.Minute),
	}).Err)

	result := model.Apply(Operation{Kind: OpExpireSessions, Mount: "vol", Limit: 16})

	require.NoError(t, result.Err)
	require.Equal(t, uint64(1), result.Expired)
	require.Equal(t, fsmeta.InodeID(11), model.sessions[sessionKey{inode: 11, session: "writer-1"}].Inode)
	require.NotContains(t, model.owners, fsmeta.InodeID(10))
	require.Contains(t, model.owners, fsmeta.InodeID(11))
	require.NoError(t, model.CheckInvariants())
}

func TestModelOpenWithStaleOwnerDoesNotRemoveReusedLiveSession(t *testing.T) {
	model := NewModel("vol")
	for _, inode := range []fsmeta.InodeID{10, 11} {
		require.NoError(t, model.Apply(Operation{
			Kind:   OpCreate,
			Mount:  "vol",
			Parent: model.Root,
			Name:   fmt.Sprintf("file-%d", inode),
			Inode:  inode,
			Type:   fsmeta.InodeTypeFile,
			Mode:   0o600,
		}).Err)
	}
	require.NoError(t, model.Apply(Operation{
		Kind:      OpOpenWriteSession,
		Mount:     "vol",
		Inode:     10,
		Session:   "writer-1",
		ExpiresNs: model.NowUnixNs + int64(time.Second),
	}).Err)
	require.NoError(t, model.Apply(Operation{
		Kind:      OpAdvanceTime,
		Mount:     "vol",
		AdvanceNs: int64(2 * time.Second),
	}).Err)
	require.NoError(t, model.Apply(Operation{
		Kind:      OpOpenWriteSession,
		Mount:     "vol",
		Inode:     11,
		Session:   "writer-1",
		ExpiresNs: model.NowUnixNs + int64(time.Minute),
	}).Err)

	require.NoError(t, model.Apply(Operation{
		Kind:      OpOpenWriteSession,
		Mount:     "vol",
		Inode:     10,
		Session:   "writer-2",
		ExpiresNs: model.NowUnixNs + int64(time.Minute),
	}).Err)

	require.Equal(t, fsmeta.InodeID(11), model.sessions[sessionKey{inode: 11, session: "writer-1"}].Inode)
	require.Equal(t, fsmeta.InodeID(10), model.sessions[sessionKey{inode: 10, session: "writer-2"}].Inode)
	require.Equal(t, fsmeta.SessionID("writer-1"), model.owners[11].Session)
	require.Equal(t, fsmeta.SessionID("writer-2"), model.owners[10].Session)
	require.NoError(t, model.CheckInvariants())
}

func TestModelLinkRenameUnlinkMaintainsLinkCounts(t *testing.T) {
	model := NewModel("vol")
	require.NoError(t, model.Apply(Operation{
		Kind:   OpCreate,
		Mount:  "vol",
		Parent: model.Root,
		Name:   "file",
		Inode:  10,
		Type:   fsmeta.InodeTypeFile,
		Mode:   0o600,
	}).Err)
	require.NoError(t, model.Apply(Operation{
		Kind:       OpLink,
		Mount:      "vol",
		FromParent: model.Root,
		FromName:   "file",
		ToParent:   model.Root,
		ToName:     "alias",
	}).Err)
	require.Equal(t, uint32(2), model.inodes[10].LinkCount)
	require.NoError(t, model.CheckInvariants())

	require.NoError(t, model.Apply(Operation{
		Kind:       OpRenameSubtree,
		Mount:      "vol",
		FromParent: model.Root,
		FromName:   "alias",
		ToParent:   model.Root,
		ToName:     "moved",
	}).Err)
	require.Equal(t, uint32(2), model.inodes[10].LinkCount)
	require.NotContains(t, model.dentries, dentryKey{parent: model.Root, name: "alias"})
	require.Contains(t, model.dentries, dentryKey{parent: model.Root, name: "moved"})
	require.NoError(t, model.CheckInvariants())

	require.NoError(t, model.Apply(Operation{
		Kind:   OpUnlink,
		Mount:  "vol",
		Parent: model.Root,
		Name:   "file",
	}).Err)
	require.Equal(t, uint32(1), model.inodes[10].LinkCount)
	require.Contains(t, model.dentries, dentryKey{parent: model.Root, name: "moved"})
	require.NoError(t, model.CheckInvariants())

	require.NoError(t, model.Apply(Operation{
		Kind:   OpUnlink,
		Mount:  "vol",
		Parent: model.Root,
		Name:   "moved",
	}).Err)
	require.NotContains(t, model.inodes, fsmeta.InodeID(10))
	require.NoError(t, model.CheckInvariants())
}

func TestModelRenameReplaceMaintainsReplacedLinkCounts(t *testing.T) {
	model := NewModel("vol")
	require.NoError(t, model.Apply(Operation{
		Kind:   OpCreate,
		Mount:  "vol",
		Parent: model.Root,
		Name:   "old",
		Inode:  10,
		Type:   fsmeta.InodeTypeFile,
		Mode:   0o600,
	}).Err)
	require.NoError(t, model.Apply(Operation{
		Kind:       OpLink,
		Mount:      "vol",
		FromParent: model.Root,
		FromName:   "old",
		ToParent:   model.Root,
		ToName:     "old-alias",
	}).Err)
	require.NoError(t, model.Apply(Operation{
		Kind:   OpCreate,
		Mount:  "vol",
		Parent: model.Root,
		Name:   "stage",
		Inode:  11,
		Type:   fsmeta.InodeTypeFile,
		Mode:   0o600,
	}).Err)

	result := model.Apply(Operation{
		Kind:       OpRenameReplace,
		Mount:      "vol",
		FromParent: model.Root,
		FromName:   "stage",
		ToParent:   model.Root,
		ToName:     "old",
	})
	require.NoError(t, result.Err)

	require.True(t, result.RenameReplace.Replaced)
	require.False(t, result.RenameReplace.OldInodeDeleted)
	require.Equal(t, fsmeta.InodeID(10), result.RenameReplace.OldDentry.Inode)
	require.Equal(t, uint32(2), result.RenameReplace.OldInode.LinkCount)
	require.Equal(t, fsmeta.InodeID(11), model.dentries[dentryKey{parent: model.Root, name: "old"}].Inode)
	require.Equal(t, uint32(1), model.inodes[10].LinkCount)
	require.Contains(t, model.dentries, dentryKey{parent: model.Root, name: "old-alias"})
	require.NotContains(t, model.dentries, dentryKey{parent: model.Root, name: "stage"})
	require.NoError(t, model.CheckInvariants())
}

func TestModelRenameReplaceDeletesSingleLinkReplacedInode(t *testing.T) {
	model := NewModel("vol")
	require.NoError(t, model.Apply(Operation{
		Kind:   OpCreate,
		Mount:  "vol",
		Parent: model.Root,
		Name:   "old",
		Inode:  10,
		Type:   fsmeta.InodeTypeFile,
		Mode:   0o600,
	}).Err)
	require.NoError(t, model.Apply(Operation{
		Kind:   OpCreate,
		Mount:  "vol",
		Parent: model.Root,
		Name:   "stage",
		Inode:  11,
		Type:   fsmeta.InodeTypeFile,
		Mode:   0o600,
	}).Err)

	result := model.Apply(Operation{
		Kind:       OpRenameReplace,
		Mount:      "vol",
		FromParent: model.Root,
		FromName:   "stage",
		ToParent:   model.Root,
		ToName:     "old",
	})
	require.NoError(t, result.Err)

	require.True(t, result.RenameReplace.Replaced)
	require.True(t, result.RenameReplace.OldInodeDeleted)
	require.Equal(t, fsmeta.InodeID(10), result.RenameReplace.OldDentry.Inode)
	require.Equal(t, uint32(1), result.RenameReplace.OldInode.LinkCount)
	require.Equal(t, fsmeta.InodeID(11), model.dentries[dentryKey{parent: model.Root, name: "old"}].Inode)
	require.NotContains(t, model.inodes, fsmeta.InodeID(10))
	require.NotContains(t, model.dentries, dentryKey{parent: model.Root, name: "stage"})
	require.NoError(t, model.CheckInvariants())
}

func TestModelSnapshotReadDirStaysStableAcrossNamespaceMutation(t *testing.T) {
	model := NewModel("vol")
	require.NoError(t, model.Apply(Operation{
		Kind:   OpCreate,
		Mount:  "vol",
		Parent: model.Root,
		Name:   "alpha",
		Inode:  10,
		Type:   fsmeta.InodeTypeFile,
		Mode:   0o600,
	}).Err)
	require.NoError(t, model.ApplySnapshot(Operation{
		Kind:        OpSnapshotSubtree,
		Mount:       "vol",
		Parent:      model.Root,
		SnapshotRef: 0,
	}, fsmeta.SnapshotSubtreeToken{
		Mount:       "vol",
		RootInode:   model.Root,
		ReadVersion: 100,
	}).Err)

	require.NoError(t, model.Apply(Operation{
		Kind:       OpRenameSubtree,
		Mount:      "vol",
		FromParent: model.Root,
		FromName:   "alpha",
		ToParent:   model.Root,
		ToName:     "beta",
	}).Err)

	snapshot := model.Apply(Operation{
		Kind:        OpReadDirPlus,
		Mount:       "vol",
		Parent:      model.Root,
		Limit:       10,
		SnapshotRef: 0,
	})
	require.NoError(t, snapshot.Err)
	requireDentryNames(t, snapshot.Pairs, "alpha")

	latest := model.Apply(Operation{
		Kind:        OpReadDirPlus,
		Mount:       "vol",
		Parent:      model.Root,
		Limit:       10,
		SnapshotRef: -1,
	})
	require.NoError(t, latest.Err)
	requireDentryNames(t, latest.Pairs, "beta")
	require.NoError(t, model.CheckInvariants())
}

func TestEquivalentErrorMatchesWrappedSentinel(t *testing.T) {
	require.True(t, EquivalentError(fmt.Errorf("wrapped: %w", fsmeta.ErrNotFound), fsmeta.ErrNotFound))
	require.False(t, EquivalentError(fmt.Errorf("wrapped: %w", fsmeta.ErrNotFound), fsmeta.ErrExists))
}

func requireDentryNames(t *testing.T, pairs []fsmeta.DentryAttrPair, names ...string) {
	t.Helper()
	got := make([]string, 0, len(pairs))
	for _, pair := range pairs {
		got = append(got, pair.Dentry.Name)
	}
	require.Equal(t, names, got)
}
