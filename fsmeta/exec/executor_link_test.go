// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"context"
	"github.com/feichai0017/NoKV/fsmeta"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestExecutorLinkCreatesDentryAndIncrementsLinkCount(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "file", 22)
	seedDirectory(t, runner, "vol", 8)
	seedInode(t, runner, "vol", fsmeta.InodeRecord{Inode: 22, Type: fsmeta.InodeTypeFile, Size: 4096, LinkCount: 1})
	quota := &fakeQuotaResolver{}
	executor, err := newTestExecutor(runner, WithQuotaResolver(quota))
	require.NoError(t, err)

	err = executor.Link(context.Background(), fsmeta.LinkRequest{
		Mount:      "vol",
		FromParent: 7,
		FromName:   "file",
		ToParent:   8,
		ToName:     "alias",
	})
	require.NoError(t, err)

	record, err := executor.Lookup(context.Background(), fsmeta.LookupRequest{Mount: "vol", Parent: 8, Name: "alias"})
	require.NoError(t, err)
	require.Equal(t, fsmeta.InodeID(22), record.Inode)
	inode, ok, err := executor.readInode(context.Background(), testMountIdentity, 22, 99)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint32(2), inode.LinkCount)
	require.Equal(t, [][]QuotaChange{{{Mount: "vol", MountKeyID: 1, Scope: 8, Bytes: 4096, Inodes: 1}}}, quota.changes)
}

func TestExecutorLinkVisibleCommitServesOverlay(t *testing.T) {
	runner := newFakeRunner()
	inode := testInodeForParentBucket(t, 8, 8)
	seedDentry(t, runner, "vol", 7, "file", inode)
	seedDirectory(t, runner, "vol", 8)
	seedInode(t, runner, "vol", fsmeta.InodeRecord{Inode: inode, Type: fsmeta.InodeTypeFile, Size: 4096, LinkCount: 1})
	committer := newTestVisibleCommitter(t, runner)
	executor, err := newTestExecutor(
		runner,
		WithVisibleAuthorityAdmitter(ownedVisibleAdmitter{}),
		WithVisibleCommitter(committer),
	)
	require.NoError(t, err)

	err = executor.Link(context.Background(), fsmeta.LinkRequest{
		Mount:      "vol",
		FromParent: 7,
		FromName:   "file",
		ToParent:   8,
		ToName:     "alias",
	})
	require.NoError(t, err)

	require.Empty(t, runner.mutations)
	record, err := executor.Lookup(context.Background(), fsmeta.LookupRequest{Mount: "vol", Parent: 8, Name: "alias"})
	require.NoError(t, err)
	require.Equal(t, inode, record.Inode)
	stored, ok, err := executor.readInode(context.Background(), testMountIdentity, inode, 99)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint32(2), stored.LinkCount)
	require.Equal(t, uint64(1), committer.Stats()["commit_total"])
}

func TestExecutorLinkUsesAtomicMutateWithValuePredicates(t *testing.T) {
	base := newFakeRunner()
	runner := &fakeAtomicRunner{fakeRunner: base, handled: true}
	seedDentry(t, runner.fakeRunner, "vol", 7, "file", 22)
	seedDirectory(t, runner.fakeRunner, "vol", 8)
	seedInode(t, runner.fakeRunner, "vol", fsmeta.InodeRecord{Inode: 22, Type: fsmeta.InodeTypeFile, Size: 4096, LinkCount: 1})
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	err = executor.Link(context.Background(), fsmeta.LinkRequest{
		Mount:      "vol",
		FromParent: 7,
		FromName:   "file",
		ToParent:   8,
		ToName:     "alias",
	})
	require.NoError(t, err)

	require.Len(t, runner.atomicCalls, 1)
	require.Empty(t, base.mutations)
	requireAtomicStatUint(t, executor.Stats(), fsmeta.OperationLink, "success_total", 1)
	require.Equal(t, kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_VALUE_EQUALS, runner.atomicCalls[0].predicates[0].GetKind())
	require.Equal(t, kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_NOT_EXISTS, runner.atomicCalls[0].predicates[1].GetKind())
	require.Equal(t, kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_VALUE_EQUALS, runner.atomicCalls[0].predicates[2].GetKind())
	inode, ok, err := executor.readInode(context.Background(), testMountIdentity, 22, 99)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint32(2), inode.LinkCount)
}

func TestExecutorLinkRejectsDirectory(t *testing.T) {
	runner := newFakeRunner()
	seedDentryType(t, runner, "vol", 7, "dir", 22, fsmeta.InodeTypeDirectory)
	seedDirectory(t, runner, "vol", 8)
	seedInode(t, runner, "vol", fsmeta.InodeRecord{Inode: 22, Type: fsmeta.InodeTypeDirectory, LinkCount: 1})
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	err = executor.Link(context.Background(), fsmeta.LinkRequest{
		Mount:      "vol",
		FromParent: 7,
		FromName:   "dir",
		ToParent:   8,
		ToName:     "alias",
	})
	require.ErrorIs(t, err, fsmeta.ErrInvalidRequest)
	require.Empty(t, runner.mutations)
}

func BenchmarkExecutorLinkDefaultPath(b *testing.B) {
	runner := newFakeRunner()
	executor, err := newTestExecutor(runner)
	if err != nil {
		b.Fatal(err)
	}
	benchmarkExecutorLink(b, runner, executor)
}

func BenchmarkExecutorLinkVisibleCommit(b *testing.B) {
	runner := newFakeRunner()
	executor, err := newTestExecutor(
		runner,
		WithVisibleAuthorityAdmitter(ownedVisibleAdmitter{}),
		WithVisibleCommitter(noopVisibleCommitter{}),
	)
	if err != nil {
		b.Fatal(err)
	}
	benchmarkExecutorLink(b, runner, executor)
}
