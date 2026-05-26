// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package fsmeta

import (
	"bytes"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/stretchr/testify/require"
)

func TestPlanCreateTouchesDentryAndInode(t *testing.T) {
	plan, err := PlanCreate(model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "file",
	}, testMount, 22)
	require.NoError(t, err)

	dentry, err := EncodeDentryKey(testMount, model.RootInode, "file")
	require.NoError(t, err)
	parent, err := EncodeInodeKey(testMount, model.RootInode)
	require.NoError(t, err)
	inode, err := EncodeInodeKey(testMount, 22)
	require.NoError(t, err)

	require.Equal(t, model.OperationCreate, plan.Kind)
	require.Equal(t, dentry, plan.PrimaryKey)
	require.Equal(t, [][]byte{parent, dentry, inode}, plan.ReadKeys)
	require.Equal(t, [][]byte{parent, dentry, inode}, plan.MutateKeys)
}

func TestPlanReadDirScansOnlyDirectoryPrefix(t *testing.T) {
	plan, err := PlanReadDir(model.ReadDirRequest{
		Mount:  "vol",
		Parent: 7,
		Limit:  128,
	}, testMount)
	require.NoError(t, err)

	prefix, err := EncodeDentryPrefix(testMount, 7)
	require.NoError(t, err)
	require.Equal(t, model.OperationReadDir, plan.Kind)
	require.Equal(t, prefix, plan.PrimaryKey)
	require.Equal(t, prefix, plan.StartKey)
	require.Equal(t, uint32(128), plan.Limit)
	require.Equal(t, [][]byte{prefix}, plan.ReadPrefixes)
	require.Empty(t, plan.ReadKeys)
	require.Empty(t, plan.MutateKeys)
}

func TestPlanReadDirDefaultsLimit(t *testing.T) {
	plan, err := PlanReadDir(model.ReadDirRequest{
		Mount:  "vol",
		Parent: 7,
	}, testMount)
	require.NoError(t, err)
	require.Equal(t, model.DefaultReadDirLimit, plan.Limit)
}

func TestPlanExpireWriteSessionsScansSessionBuckets(t *testing.T) {
	plan, err := PlanExpireWriteSessions(model.ExpireWriteSessionsRequest{
		Mount: "vol",
		Limit: 32,
	}, testMount)
	require.NoError(t, err)

	first, err := EncodeSessionBucketPrefix(testMount, 0)
	require.NoError(t, err)
	last, err := EncodeSessionBucketPrefix(testMount, AffinityBucket(DefaultAffinityBucketCount-1))
	require.NoError(t, err)
	mountPrefix, err := EncodeMountPrefix(testMount)
	require.NoError(t, err)

	require.Equal(t, model.OperationExpireSessions, plan.Kind)
	require.Equal(t, first, plan.PrimaryKey)
	require.Equal(t, first, plan.StartKey)
	require.Equal(t, uint32(32), plan.Limit)
	require.Len(t, plan.ReadPrefixes, DefaultAffinityBucketCount)
	require.Equal(t, first, plan.ReadPrefixes[0])
	require.Equal(t, last, plan.ReadPrefixes[len(plan.ReadPrefixes)-1])
	for _, prefix := range plan.ReadPrefixes {
		require.True(t, bytes.HasPrefix(prefix, mountPrefix))
		kind, err := KeyKindOf(prefix)
		require.NoError(t, err)
		require.Equal(t, KeyKindSession, kind)
	}
}

func TestPlanReadDirStartAfterBecomesInclusiveSeekKey(t *testing.T) {
	plan, err := PlanReadDir(model.ReadDirRequest{
		Mount:      "vol",
		Parent:     7,
		StartAfter: "a",
		Limit:      64,
	}, testMount)
	require.NoError(t, err)

	cursor, err := EncodeDentryKey(testMount, 7, "a")
	require.NoError(t, err)
	nextName, err := EncodeDentryKey(testMount, 7, "aa")
	require.NoError(t, err)

	require.Equal(t, append(cursor, 0), plan.StartKey)
	require.Positive(t, bytes.Compare(plan.StartKey, cursor))
	require.Negative(t, bytes.Compare(plan.StartKey, nextName))
	require.Equal(t, uint32(64), plan.Limit)
}

func TestPlanReadDirRejectsOversizedPage(t *testing.T) {
	_, err := PlanReadDir(model.ReadDirRequest{
		Mount:  "vol",
		Parent: 7,
		Limit:  model.MaxReadDirLimit + 1,
	}, testMount)
	require.ErrorIs(t, err, model.ErrInvalidPageSize)
}

func TestPlanRenameTouchesSourceAndDestinationDentries(t *testing.T) {
	plan, err := PlanRename(model.RenameRequest{
		Mount:      "vol",
		FromParent: 2,
		FromName:   "old",
		ToParent:   3,
		ToName:     "new",
	}, testMount)
	require.NoError(t, err)

	from, err := EncodeDentryKey(testMount, 2, "old")
	require.NoError(t, err)
	to, err := EncodeDentryKey(testMount, 3, "new")
	require.NoError(t, err)
	fromParent, err := EncodeInodeKey(testMount, 2)
	require.NoError(t, err)
	toParent, err := EncodeInodeKey(testMount, 3)
	require.NoError(t, err)

	require.Equal(t, model.OperationRename, plan.Kind)
	require.Equal(t, from, plan.PrimaryKey)
	require.Equal(t, [][]byte{from, to, fromParent, toParent}, plan.ReadKeys)
	require.Equal(t, [][]byte{from, to, fromParent, toParent}, plan.MutateKeys)
}

func TestPlanRenameRejectsNoop(t *testing.T) {
	_, err := PlanRename(model.RenameRequest{
		Mount:      "vol",
		FromParent: 2,
		FromName:   "same",
		ToParent:   2,
		ToName:     "same",
	}, testMount)
	require.ErrorIs(t, err, model.ErrInvalidRequest)
}

func TestPlanRenameSubtreeTouchesSourceAndDestinationDentries(t *testing.T) {
	plan, err := PlanRenameSubtree(model.RenameSubtreeRequest{
		Mount:      "vol",
		FromParent: 2,
		FromName:   "old",
		ToParent:   3,
		ToName:     "new",
	}, testMount)
	require.NoError(t, err)

	from, err := EncodeDentryKey(testMount, 2, "old")
	require.NoError(t, err)
	to, err := EncodeDentryKey(testMount, 3, "new")
	require.NoError(t, err)
	fromParent, err := EncodeInodeKey(testMount, 2)
	require.NoError(t, err)
	toParent, err := EncodeInodeKey(testMount, 3)
	require.NoError(t, err)

	require.Equal(t, model.OperationRenameSubtree, plan.Kind)
	require.Equal(t, from, plan.PrimaryKey)
	require.Equal(t, [][]byte{from, to, fromParent, toParent}, plan.ReadKeys)
	require.Equal(t, [][]byte{from, to, fromParent, toParent}, plan.MutateKeys)
}

func TestPlanRenameSubtreeRejectsNoop(t *testing.T) {
	_, err := PlanRenameSubtree(model.RenameSubtreeRequest{
		Mount:      "vol",
		FromParent: 2,
		FromName:   "same",
		ToParent:   2,
		ToName:     "same",
	}, testMount)
	require.ErrorIs(t, err, model.ErrInvalidRequest)
}

func TestPlanSnapshotSubtreeScansRootPrefix(t *testing.T) {
	plan, err := PlanSnapshotSubtree(model.SnapshotSubtreeRequest{
		Mount:     "vol",
		RootInode: 7,
	}, testMount)
	require.NoError(t, err)

	prefix, err := EncodeDentryPrefix(testMount, 7)
	require.NoError(t, err)
	require.Equal(t, model.OperationSnapshotSubtree, plan.Kind)
	require.Equal(t, "vol", string(plan.Mount))
	require.Equal(t, prefix, plan.PrimaryKey)
	require.Equal(t, [][]byte{prefix}, plan.ReadPrefixes)
	require.Empty(t, plan.ReadKeys)
	require.Empty(t, plan.MutateKeys)
}

func TestPlanLinkTouchesSourceDestinationAndRejectsNoop(t *testing.T) {
	plan, err := PlanLink(model.LinkRequest{
		Mount:      "vol",
		FromParent: 2,
		FromName:   "old",
		ToParent:   3,
		ToName:     "new",
	}, testMount)
	require.NoError(t, err)

	from, err := EncodeDentryKey(testMount, 2, "old")
	require.NoError(t, err)
	to, err := EncodeDentryKey(testMount, 3, "new")
	require.NoError(t, err)
	toParent, err := EncodeInodeKey(testMount, 3)
	require.NoError(t, err)

	require.Equal(t, model.OperationLink, plan.Kind)
	require.Equal(t, to, plan.PrimaryKey)
	require.Equal(t, [][]byte{from, to, toParent}, plan.ReadKeys)
	require.Equal(t, [][]byte{to, toParent}, plan.MutateKeys)

	_, err = PlanLink(model.LinkRequest{
		Mount:      "vol",
		FromParent: 2,
		FromName:   "same",
		ToParent:   2,
		ToName:     "same",
	}, testMount)
	require.ErrorIs(t, err, model.ErrInvalidRequest)
}

func TestPlanUnlinkTouchesDentry(t *testing.T) {
	plan, err := PlanUnlink(model.UnlinkRequest{
		Mount:  "vol",
		Parent: 7,
		Name:   "file",
	}, testMount)
	require.NoError(t, err)

	dentry, err := EncodeDentryKey(testMount, 7, "file")
	require.NoError(t, err)
	parent, err := EncodeInodeKey(testMount, 7)
	require.NoError(t, err)
	require.Equal(t, model.OperationUnlink, plan.Kind)
	require.Equal(t, dentry, plan.PrimaryKey)
	require.Equal(t, [][]byte{dentry, parent}, plan.ReadKeys)
	require.Equal(t, [][]byte{dentry, parent}, plan.MutateKeys)
}

func TestPlanRemoveTouchesDentry(t *testing.T) {
	plan, err := PlanRemove(model.RemoveRequest{
		Mount:  "vol",
		Parent: 7,
		Name:   "file",
	}, testMount)
	require.NoError(t, err)

	dentry, err := EncodeDentryKey(testMount, 7, "file")
	require.NoError(t, err)
	parent, err := EncodeInodeKey(testMount, 7)
	require.NoError(t, err)
	require.Equal(t, model.OperationRemove, plan.Kind)
	require.Equal(t, dentry, plan.PrimaryKey)
	require.Equal(t, [][]byte{dentry, parent}, plan.ReadKeys)
	require.Equal(t, [][]byte{dentry, parent}, plan.MutateKeys)
}

func TestPlanRemoveDirectoryTouchesParentAndDentry(t *testing.T) {
	plan, err := PlanRemoveDirectory(model.RemoveDirectoryRequest{
		Mount:  "vol",
		Parent: 7,
		Name:   "dir",
	}, testMount)
	require.NoError(t, err)

	parent, err := EncodeInodeKey(testMount, 7)
	require.NoError(t, err)
	dentry, err := EncodeDentryKey(testMount, 7, "dir")
	require.NoError(t, err)
	require.Equal(t, model.OperationRemoveDirectory, plan.Kind)
	require.Equal(t, dentry, plan.PrimaryKey)
	require.Equal(t, [][]byte{parent, dentry}, plan.ReadKeys)
	require.Equal(t, [][]byte{parent, dentry}, plan.MutateKeys)
}

func TestPlansCloneKeys(t *testing.T) {
	plan, err := PlanLookup(model.LookupRequest{Mount: "vol", Parent: model.RootInode, Name: "file"}, testMount)
	require.NoError(t, err)
	original := append([]byte(nil), plan.PrimaryKey...)

	plan.PrimaryKey[0] ^= 0xff
	require.True(t, bytes.Equal(original, plan.ReadKeys[0]))
}

func TestPlanOpenWriteSessionTouchesInodeAndSession(t *testing.T) {
	plan, err := PlanOpenWriteSession(model.OpenWriteSessionRequest{
		Mount:   "vol",
		Inode:   44,
		Session: "client-1",
	}, testMount)
	require.NoError(t, err)

	inode, err := EncodeInodeKey(testMount, 44)
	require.NoError(t, err)
	session, err := EncodeSessionKey(testMount, 44, "client-1")
	require.NoError(t, err)
	owner, err := EncodeInodeSessionKey(testMount, 44)
	require.NoError(t, err)

	require.Equal(t, model.OperationOpenWriteSession, plan.Kind)
	require.Equal(t, session, plan.PrimaryKey)
	require.Equal(t, [][]byte{inode, session, owner}, plan.ReadKeys)
	require.Equal(t, [][]byte{session, owner}, plan.MutateKeys)
}
