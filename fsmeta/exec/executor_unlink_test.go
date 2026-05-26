// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"context"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta/model"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/stretchr/testify/require"
)

func TestExecutorUnlinkReservesNegativeQuotaWhenInodeExists(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "file", 22)
	seedInode(t, runner, "vol", model.InodeRecord{Inode: 22, Type: model.InodeTypeFile, Size: 4096, LinkCount: 1})
	quota := &fakeQuotaResolver{}
	executor, err := newTestExecutor(runner, WithQuotaResolver(quota))
	require.NoError(t, err)

	err = executor.Unlink(context.Background(), model.UnlinkRequest{Mount: "vol", Parent: 7, Name: "file"})
	require.NoError(t, err)
	require.Equal(t, [][]QuotaChange{{{Mount: "vol", MountKeyID: 1, Scope: 7, Bytes: -4096, Inodes: -1}}}, quota.changes)
}

func TestExecutorUnlinkRemovesDentry(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "file", 22)
	seedInode(t, runner, "vol", model.InodeRecord{Inode: 22, Type: model.InodeTypeFile, LinkCount: 1})
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	err = executor.Unlink(context.Background(), model.UnlinkRequest{
		Mount:  "vol",
		Parent: 7,
		Name:   "file",
	})
	require.NoError(t, err)

	_, err = executor.Lookup(context.Background(), model.LookupRequest{
		Mount:  "vol",
		Parent: 7,
		Name:   "file",
	})
	require.ErrorIs(t, err, model.ErrNotFound)
	require.Len(t, runner.mutations, 1)
	require.Len(t, runner.mutations[0], 3)
	require.Equal(t, kvrpcpb.Mutation_Delete, runner.mutations[0][0].GetOp())
	require.Equal(t, kvrpcpb.Mutation_Delete, runner.mutations[0][1].GetOp())
	_, ok, err := executor.readInode(context.Background(), testMountIdentity, 22, 99)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestExecutorRemoveRemovesDentry(t *testing.T) {
	base := newFakeRunner()
	runner := &fakeAtomicRunner{fakeRunner: base, handled: true}
	seedDentry(t, runner.fakeRunner, "vol", 7, "file", 22)
	seedInode(t, runner.fakeRunner, "vol", model.InodeRecord{Inode: 22, Type: model.InodeTypeFile, LinkCount: 1})
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	result, err := executor.Remove(context.Background(), model.RemoveRequest{
		Mount:  "vol",
		Parent: 7,
		Name:   "file",
	})
	require.NoError(t, err)
	require.Equal(t, model.DentryRecord{Parent: 7, Name: "file", Inode: 22, Type: model.InodeTypeFile}, result.RemovedDentry)
	require.Equal(t, model.InodeID(22), result.OldInode.Inode)
	require.True(t, result.InodeDeleted)

	_, err = executor.Lookup(context.Background(), model.LookupRequest{
		Mount:  "vol",
		Parent: 7,
		Name:   "file",
	})
	require.ErrorIs(t, err, model.ErrNotFound)
	require.Empty(t, base.mutations)
	require.Len(t, runner.atomicCalls, 1)
	require.Len(t, runner.atomicCalls[0].mutations, 3)
	require.Equal(t, kvrpcpb.Mutation_Delete, runner.atomicCalls[0].mutations[0].GetOp())
	require.Equal(t, kvrpcpb.Mutation_Delete, runner.atomicCalls[0].mutations[1].GetOp())
	requireAtomicStatUint(t, executor.Stats(), model.OperationRemove, "success_total", 1)
}

func TestExecutorUnlinkUsesAtomicMutateWithValuePredicates(t *testing.T) {
	base := newFakeRunner()
	runner := &fakeAtomicRunner{fakeRunner: base, handled: true}
	seedDentry(t, runner.fakeRunner, "vol", 7, "file", 22)
	seedInode(t, runner.fakeRunner, "vol", model.InodeRecord{Inode: 22, Type: model.InodeTypeFile, LinkCount: 1})
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	err = executor.Unlink(context.Background(), model.UnlinkRequest{Mount: "vol", Parent: 7, Name: "file"})
	require.NoError(t, err)

	require.Len(t, runner.atomicCalls, 1)
	require.Empty(t, base.mutations)
	requireAtomicStatUint(t, executor.Stats(), model.OperationUnlink, "success_total", 1)
	require.Equal(t, kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_VALUE_EQUALS, runner.atomicCalls[0].predicates[0].GetKind())
	require.Equal(t, kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_VALUE_EQUALS, runner.atomicCalls[0].predicates[1].GetKind())
	_, ok, err := executor.readInode(context.Background(), testMountIdentity, 22, 99)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestExecutorUnlinkDecrementsMultiLinkInode(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "file", 22)
	seedInode(t, runner, "vol", model.InodeRecord{Inode: 22, Type: model.InodeTypeFile, LinkCount: 2})
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	err = executor.Unlink(context.Background(), model.UnlinkRequest{Mount: "vol", Parent: 7, Name: "file"})
	require.NoError(t, err)

	inode, ok, err := executor.readInode(context.Background(), testMountIdentity, 22, 99)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint32(1), inode.LinkCount)
}

func TestExecutorUnlinkVisibleCommitServesOverlay(t *testing.T) {
	runner := newFakeRunner()
	inode := testInodeForParentBucket(t, 7, 7)
	seedDentry(t, runner, "vol", 7, "file", inode)
	seedInode(t, runner, "vol", model.InodeRecord{Inode: inode, Type: model.InodeTypeFile, Size: 4096, LinkCount: 2})
	committer := newTestVisibleCommitter(t, runner)
	executor, err := newTestExecutor(
		runner,
		WithVisibleAuthorityAdmitter(ownedVisibleAdmitter{}),
		WithVisibleCommitter(committer),
	)
	require.NoError(t, err)

	err = executor.Unlink(context.Background(), model.UnlinkRequest{Mount: "vol", Parent: 7, Name: "file"})
	require.NoError(t, err)

	require.Empty(t, runner.mutations)
	_, err = executor.Lookup(context.Background(), model.LookupRequest{Mount: "vol", Parent: 7, Name: "file"})
	require.ErrorIs(t, err, model.ErrNotFound)
	stored, ok, err := executor.readInode(context.Background(), testMountIdentity, inode, 99)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint32(1), stored.LinkCount)
	require.Equal(t, uint64(1), committer.Stats()["commit_total"])
}

func TestExecutorUnlinkLastReferenceVisibleCommitDeletesInode(t *testing.T) {
	runner := newFakeRunner()
	inode := testInodeForParentBucket(t, 7, 7)
	seedDentry(t, runner, "vol", 7, "file", inode)
	seedInode(t, runner, "vol", model.InodeRecord{Inode: inode, Type: model.InodeTypeFile, Size: 4096, LinkCount: 1})
	committer := newTestVisibleCommitter(t, runner)
	executor, err := newTestExecutor(
		runner,
		WithVisibleAuthorityAdmitter(ownedVisibleAdmitter{}),
		WithVisibleCommitter(committer),
	)
	require.NoError(t, err)

	err = executor.Unlink(context.Background(), model.UnlinkRequest{Mount: "vol", Parent: 7, Name: "file"})
	require.NoError(t, err)

	require.Empty(t, runner.mutations)
	_, err = executor.Lookup(context.Background(), model.LookupRequest{Mount: "vol", Parent: 7, Name: "file"})
	require.ErrorIs(t, err, model.ErrNotFound)
	_, ok, err := executor.readInode(context.Background(), testMountIdentity, inode, 99)
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, uint64(1), committer.Stats()["commit_total"])
}

func TestExecutorUnlinkRejectsDirectoryBeforeVisibleCommit(t *testing.T) {
	runner := newFakeRunner()
	inode := testInodeForParentBucket(t, 7, 7)
	seedDentryType(t, runner, "vol", 7, "dir", inode, model.InodeTypeDirectory)
	seedInode(t, runner, "vol", model.InodeRecord{Inode: inode, Type: model.InodeTypeDirectory, Mode: 0o755, LinkCount: 1})
	committer := newTestVisibleCommitter(t, runner)
	executor, err := newTestExecutor(
		runner,
		WithVisibleAuthorityAdmitter(ownedVisibleAdmitter{}),
		WithVisibleCommitter(committer),
	)
	require.NoError(t, err)

	err = executor.Unlink(context.Background(), model.UnlinkRequest{Mount: "vol", Parent: 7, Name: "dir"})
	require.ErrorIs(t, err, model.ErrInvalidRequest)

	require.Empty(t, runner.mutations)
	require.Equal(t, uint64(0), committer.Stats()["commit_total"])
}

func TestExecutorUnlinkRejectsDirectoryOnTxnPath(t *testing.T) {
	runner := newFakeRunner()
	inode := testInodeForParentBucket(t, 7, 7)
	seedDentryType(t, runner, "vol", 7, "dir", inode, model.InodeTypeDirectory)
	seedInode(t, runner, "vol", model.InodeRecord{Inode: inode, Type: model.InodeTypeDirectory, Mode: 0o755, LinkCount: 1})
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	err = executor.Unlink(context.Background(), model.UnlinkRequest{Mount: "vol", Parent: 7, Name: "dir"})
	require.ErrorIs(t, err, model.ErrInvalidRequest)

	require.Empty(t, runner.mutations)
}

func TestExecutorRemoveRejectsDirectoryOnTxnPath(t *testing.T) {
	runner := newFakeRunner()
	inode := testInodeForParentBucket(t, 7, 7)
	seedDentryType(t, runner, "vol", 7, "dir", inode, model.InodeTypeDirectory)
	seedInode(t, runner, "vol", model.InodeRecord{Inode: inode, Type: model.InodeTypeDirectory, Mode: 0o755, LinkCount: 1})
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	_, err = executor.Remove(context.Background(), model.RemoveRequest{Mount: "vol", Parent: 7, Name: "dir"})
	require.ErrorIs(t, err, model.ErrInvalidRequest)

	require.Empty(t, runner.mutations)
}

func TestExecutorRemoveDirectoryRemovesEmptyDirectory(t *testing.T) {
	runner := newFakeRunner()
	inode := testInodeForParentBucket(t, 7, 7)
	seedDentryType(t, runner, "vol", 7, "dir", inode, model.InodeTypeDirectory)
	seedInode(t, runner, "vol", model.InodeRecord{
		Inode:     inode,
		Type:      model.InodeTypeDirectory,
		Mode:      0o755,
		LinkCount: 1,
	})
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	err = executor.RemoveDirectory(context.Background(), model.RemoveDirectoryRequest{Mount: "vol", Parent: 7, Name: "dir"})
	require.NoError(t, err)

	_, err = executor.Lookup(context.Background(), model.LookupRequest{Mount: "vol", Parent: 7, Name: "dir"})
	require.ErrorIs(t, err, model.ErrNotFound)
	_, ok, err := executor.readInode(context.Background(), testMountIdentity, inode, 99)
	require.NoError(t, err)
	require.False(t, ok)
	parent, ok, err := executor.readInode(context.Background(), testMountIdentity, 7, 99)
	require.NoError(t, err)
	require.True(t, ok)
	require.Zero(t, parent.ChildCount)
	require.Len(t, runner.mutations, 1)
	require.Len(t, runner.mutations[0], 3)
	require.Equal(t, kvrpcpb.Mutation_Put, runner.mutations[0][0].GetOp())
	require.Equal(t, kvrpcpb.Mutation_Delete, runner.mutations[0][1].GetOp())
	require.Equal(t, kvrpcpb.Mutation_Delete, runner.mutations[0][2].GetOp())
}

func TestExecutorRemoveDirectoryReservesNegativeQuota(t *testing.T) {
	runner := newFakeRunner()
	inode := testInodeForParentBucket(t, 7, 7)
	seedDentryType(t, runner, "vol", 7, "dir", inode, model.InodeTypeDirectory)
	seedInode(t, runner, "vol", model.InodeRecord{
		Inode:     inode,
		Type:      model.InodeTypeDirectory,
		Size:      4096,
		Mode:      0o755,
		LinkCount: 1,
	})
	quota := &fakeQuotaResolver{}
	executor, err := newTestExecutor(runner, WithQuotaResolver(quota))
	require.NoError(t, err)

	err = executor.RemoveDirectory(context.Background(), model.RemoveDirectoryRequest{Mount: "vol", Parent: 7, Name: "dir"})
	require.NoError(t, err)
	require.Equal(t, [][]QuotaChange{{{Mount: "vol", MountKeyID: 1, Scope: 7, Bytes: -4096, Inodes: -1}}}, quota.changes)
}

func TestExecutorRemoveDirectoryRejectsNonEmptyDirectory(t *testing.T) {
	runner := newFakeRunner()
	inode := testInodeForParentBucket(t, 7, 7)
	seedDentryType(t, runner, "vol", 7, "dir", inode, model.InodeTypeDirectory)
	seedInode(t, runner, "vol", model.InodeRecord{
		Inode:      inode,
		Type:       model.InodeTypeDirectory,
		Mode:       0o755,
		LinkCount:  1,
		ChildCount: 1,
	})
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	err = executor.RemoveDirectory(context.Background(), model.RemoveDirectoryRequest{Mount: "vol", Parent: 7, Name: "dir"})
	require.ErrorIs(t, err, model.ErrInvalidRequest)
	require.Empty(t, runner.mutations)
	record, err := executor.Lookup(context.Background(), model.LookupRequest{Mount: "vol", Parent: 7, Name: "dir"})
	require.NoError(t, err)
	require.Equal(t, inode, record.Inode)
}

func TestExecutorRemoveDirectoryVisibleCommitServesOverlay(t *testing.T) {
	runner := newFakeRunner()
	inode := testInodeForParentBucket(t, 7, 7)
	seedDentryType(t, runner, "vol", 7, "dir", inode, model.InodeTypeDirectory)
	seedInode(t, runner, "vol", model.InodeRecord{
		Inode:     inode,
		Type:      model.InodeTypeDirectory,
		Mode:      0o755,
		LinkCount: 1,
	})
	committer := newTestVisibleCommitter(t, runner)
	executor, err := newTestExecutor(
		runner,
		WithVisibleAuthorityAdmitter(ownedVisibleAdmitter{}),
		WithVisibleCommitter(committer),
	)
	require.NoError(t, err)

	err = executor.RemoveDirectory(context.Background(), model.RemoveDirectoryRequest{Mount: "vol", Parent: 7, Name: "dir"})
	require.NoError(t, err)

	require.Empty(t, runner.mutations)
	_, err = executor.Lookup(context.Background(), model.LookupRequest{Mount: "vol", Parent: 7, Name: "dir"})
	require.ErrorIs(t, err, model.ErrNotFound)
	_, ok, err := executor.readInode(context.Background(), testMountIdentity, inode, 99)
	require.NoError(t, err)
	require.False(t, ok)
	parent, ok, err := executor.readInode(context.Background(), testMountIdentity, 7, 99)
	require.NoError(t, err)
	require.True(t, ok)
	require.Zero(t, parent.ChildCount)
	require.Equal(t, uint64(1), committer.Stats()["commit_total"])
}

func TestExecutorUnlinkMissingDentry(t *testing.T) {
	runner := newFakeRunner()
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	err = executor.Unlink(context.Background(), model.UnlinkRequest{
		Mount:  "vol",
		Parent: 7,
		Name:   "missing",
	})
	require.ErrorIs(t, err, model.ErrNotFound)
	require.Empty(t, runner.mutations)
}

func BenchmarkExecutorUnlinkDefaultPath(b *testing.B) {
	runner := newFakeRunner()
	executor, err := newTestExecutor(runner)
	if err != nil {
		b.Fatal(err)
	}
	benchmarkExecutorUnlink(b, runner, executor)
}

func BenchmarkExecutorUnlinkVisibleCommit(b *testing.B) {
	runner := newFakeRunner()
	executor, err := newTestExecutor(
		runner,
		WithVisibleAuthorityAdmitter(ownedVisibleAdmitter{}),
		WithVisibleCommitter(noopVisibleCommitter{}),
	)
	if err != nil {
		b.Fatal(err)
	}
	benchmarkExecutorUnlink(b, runner, executor)
}
