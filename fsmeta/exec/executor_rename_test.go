// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"context"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/model"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/stretchr/testify/require"
)

func TestExecutorCreateVisibleCommitAcceptsCrossBucketCatalogInstall(t *testing.T) {
	runner := newFakeRunner()
	committer := &fakeVisibleCommitter{}
	inode := testInodeForDifferentBucket(t, model.RootInode)
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{inode}}),
		WithVisibleAuthorityAdmitter(&fakeVisibleAdmitter{owned: true}),
		WithVisibleCommitter(committer),
	)
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	})
	require.NoError(t, err)
	require.Equal(t, 1, committer.calls)
	require.Empty(t, runner.mutations)

	stats := executor.Stats()
	requireVisibleCommitStatUint(t, stats, "success_total", 1)
}

func TestExecutorRenameSubtreeMovesDentry(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "old", 22)
	seedDirectory(t, runner, "vol", 8)
	publisher := &fakeSubtreePublisher{}
	resolver := &fakeMountResolver{records: map[model.MountID]MountAdmission{
		"vol": {MountID: "vol", MountKeyID: 1, RootInode: model.RootInode, SchemaVersion: 1},
	}}
	executor, err := newTestExecutor(runner, WithMountResolver(resolver), WithSubtreeHandoffPublisher(publisher))
	require.NoError(t, err)

	err = executor.RenameSubtree(context.Background(), model.RenameSubtreeRequest{
		Mount:      "vol",
		FromParent: 7,
		FromName:   "old",
		ToParent:   8,
		ToName:     "new",
	})
	require.NoError(t, err)

	_, err = executor.Lookup(context.Background(), model.LookupRequest{Mount: "vol", Parent: 7, Name: "old"})
	require.ErrorIs(t, err, model.ErrNotFound)
	record, err := executor.Lookup(context.Background(), model.LookupRequest{Mount: "vol", Parent: 8, Name: "new"})
	require.NoError(t, err)
	require.Equal(t, model.DentryRecord{
		Parent: 8,
		Name:   "new",
		Inode:  22,
		Type:   model.InodeTypeFile,
	}, record)
	require.Len(t, runner.mutations, 1)
	require.Len(t, runner.mutations[0], 4)
	require.Equal(t, kvrpcpb.Mutation_Delete, runner.mutations[0][0].GetOp())
	require.Equal(t, kvrpcpb.Mutation_Put, runner.mutations[0][1].GetOp())
	require.True(t, runner.mutations[0][1].GetAssertionNotExist())
	require.Equal(t, []subtreePublishCall{{mount: "vol", root: model.RootInode, frontier: 2}}, publisher.starts)
	require.Equal(t, []subtreePublishCall{{mount: "vol", root: model.RootInode, frontier: 2}}, publisher.completes)
}

func TestExecutorRenameVisibleCommitServesOverlay(t *testing.T) {
	runner := newFakeRunner()
	seedDirectory(t, runner, "vol", 7)
	committer := newTestVisibleCommitter(t, runner)
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{22}}),
		WithVisibleAuthorityAdmitter(ownedVisibleAdmitter{}),
		WithVisibleCommitter(committer),
	)
	require.NoError(t, err)
	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: 7,
		Name:   "old",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	})
	require.NoError(t, err)

	err = executor.Rename(context.Background(), model.RenameRequest{
		Mount:      "vol",
		FromParent: 7,
		FromName:   "old",
		ToParent:   7,
		ToName:     "new",
	})
	require.NoError(t, err)

	_, err = executor.Lookup(context.Background(), model.LookupRequest{Mount: "vol", Parent: 7, Name: "old"})
	require.ErrorIs(t, err, model.ErrNotFound)
	record, err := executor.Lookup(context.Background(), model.LookupRequest{Mount: "vol", Parent: 7, Name: "new"})
	require.NoError(t, err)
	require.Equal(t, model.DentryRecord{Parent: 7, Name: "new", Inode: 22, Type: model.InodeTypeFile}, record)
	require.Empty(t, runner.mutations, "same-parent rename should stay inside visible overlay")

	stats := executor.Stats()
	requireVisibleCommitStatUint(t, stats, "attempt_total", 2)
	requireVisibleCommitStatUint(t, stats, "success_total", 2)
}

func TestExecutorRenameDoesNotFallbackAfterVisibleOverlayReadAdmissionMiss(t *testing.T) {
	runner := newFakeRunner()
	seedDirectory(t, runner, "vol", 7)
	committer := newTestVisibleCommitter(t, runner)
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{22}}),
		WithVisibleAuthorityAdmitter(ownedVisibleAdmitter{}),
		WithVisibleCommitter(committer),
	)
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: 7,
		Name:   "theta",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	})
	require.NoError(t, err)
	require.Empty(t, runner.mutations)

	committer.submitErr = ErrVisibleAdmissionRejected
	err = executor.Rename(context.Background(), model.RenameRequest{
		Mount:      "vol",
		FromParent: 7,
		FromName:   "theta",
		ToParent:   7,
		ToName:     "eta",
	})
	require.ErrorIs(t, err, errVisibleOverlayFallbackUnsafe)
	require.Empty(t, runner.mutations, "overlay-backed rename must not fall back to base mutation after admission changes")

	_, err = executor.Lookup(context.Background(), model.LookupRequest{Mount: "vol", Parent: 7, Name: "theta"})
	require.NoError(t, err)
	_, err = executor.Lookup(context.Background(), model.LookupRequest{Mount: "vol", Parent: 7, Name: "eta"})
	require.ErrorIs(t, err, model.ErrNotFound)
}

func TestExecutorRenameBaseSourceUsesDurablePath(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "old", 22)
	committer := newTestVisibleCommitter(t, runner)
	executor, err := newTestExecutor(
		runner,
		WithVisibleAuthorityAdmitter(ownedVisibleAdmitter{}),
		WithVisibleCommitter(committer),
	)
	require.NoError(t, err)

	err = executor.Rename(context.Background(), model.RenameRequest{
		Mount:      "vol",
		FromParent: 7,
		FromName:   "old",
		ToParent:   7,
		ToName:     "new",
	})
	require.NoError(t, err)
	require.Len(t, runner.mutations, 1)
	require.Equal(t, uint64(0), committer.Stats()["commit_total"])
}

func TestExecutorCrossParentSameBucketRenameUsesVisibleCommit(t *testing.T) {
	runner := newFakeRunner()
	fromParent := model.InodeID(7)
	toParent := testInodeForParentBucket(t, fromParent, fromParent)
	require.NotEqual(t, fromParent, toParent)
	require.Equal(t, fsmeta.BucketForInodeID(fromParent), fsmeta.BucketForInodeID(toParent))
	seedDirectory(t, runner, "vol", fromParent)
	seedDirectory(t, runner, "vol", toParent)
	committer := newTestVisibleCommitter(t, runner)
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{22}}),
		WithSubtreeAuthorityResolver(&fakeAuthorityResolver{same: true}),
		WithVisibleAuthorityAdmitter(ownedVisibleAdmitter{}),
		WithVisibleCommitter(committer),
	)
	require.NoError(t, err)
	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: fromParent,
		Name:   "old",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile, Size: 4096},
	})
	require.NoError(t, err)

	err = executor.Rename(context.Background(), model.RenameRequest{
		Mount:      "vol",
		FromParent: fromParent,
		FromName:   "old",
		ToParent:   toParent,
		ToName:     "new",
	})
	require.NoError(t, err)

	_, err = executor.Lookup(context.Background(), model.LookupRequest{Mount: "vol", Parent: fromParent, Name: "old"})
	require.ErrorIs(t, err, model.ErrNotFound)
	record, err := executor.Lookup(context.Background(), model.LookupRequest{Mount: "vol", Parent: toParent, Name: "new"})
	require.NoError(t, err)
	require.Equal(t, model.DentryRecord{Parent: toParent, Name: "new", Inode: 22, Type: model.InodeTypeFile}, record)
	require.Empty(t, runner.mutations, "bucket-local cross-parent rename should stay inside visible overlay")
	require.Equal(t, uint64(2), committer.Stats()["commit_total"])
}

func TestExecutorRenameVisibleUsesEmptyDirectoryFactForDestination(t *testing.T) {
	runner := newFakeRunner()
	fromParent := model.InodeID(7)
	toParent := testInodeForParentBucket(t, fromParent, fromParent)
	seedDirectory(t, runner, "vol", fromParent)
	seedDirectory(t, runner, "vol", toParent)
	committer := newTestVisibleCommitter(t, runner)
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{22}}),
		WithSubtreeAuthorityResolver(&fakeAuthorityResolver{same: true}),
		WithVisibleAuthorityAdmitter(ownedVisibleAdmitter{}),
		WithVisibleCommitter(committer),
	)
	require.NoError(t, err)
	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: fromParent,
		Name:   "old",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile, Size: 4096},
	})
	require.NoError(t, err)
	committer.RememberEmptyDirectory(testMountIdentity, toParent)

	runner.getCalls = 0
	err = executor.Rename(context.Background(), model.RenameRequest{
		Mount:      "vol",
		FromParent: fromParent,
		FromName:   "old",
		ToParent:   toParent,
		ToName:     "new",
	})
	require.NoError(t, err)

	require.Equal(t, 2, runner.getCalls, "rename should still prove parent child-count values while using overlay source values and the destination directory fact")
	require.Empty(t, runner.mutations)
}

func TestExecutorCrossBucketRenameUsesDurablePath(t *testing.T) {
	runner := newFakeRunner()
	fromParent := model.InodeID(7)
	toParent := testInodeForDifferentBucket(t, fromParent, fromParent)
	require.NotEqual(t, fromParent, toParent)
	require.NotEqual(t, fsmeta.BucketForInodeID(fromParent), fsmeta.BucketForInodeID(toParent))
	seedDentry(t, runner, "vol", fromParent, "old", 22)
	seedDirectory(t, runner, "vol", toParent)
	seedInode(t, runner, "vol", model.InodeRecord{Inode: 22, Type: model.InodeTypeFile, Size: 4096, LinkCount: 1})
	committer := newTestVisibleCommitter(t, runner)
	executor, err := newTestExecutor(
		runner,
		WithSubtreeAuthorityResolver(&fakeAuthorityResolver{same: true}),
		WithVisibleAuthorityAdmitter(ownedVisibleAdmitter{}),
		WithVisibleCommitter(committer),
	)
	require.NoError(t, err)

	err = executor.Rename(context.Background(), model.RenameRequest{
		Mount:      "vol",
		FromParent: fromParent,
		FromName:   "old",
		ToParent:   toParent,
		ToName:     "new",
	})
	require.NoError(t, err)

	_, err = executor.Lookup(context.Background(), model.LookupRequest{Mount: "vol", Parent: fromParent, Name: "old"})
	require.ErrorIs(t, err, model.ErrNotFound)
	record, err := executor.Lookup(context.Background(), model.LookupRequest{Mount: "vol", Parent: toParent, Name: "new"})
	require.NoError(t, err)
	require.Equal(t, model.DentryRecord{Parent: toParent, Name: "new", Inode: 22, Type: model.InodeTypeFile}, record)
	require.Len(t, runner.mutations, 1, "cross-bucket rename remains on the ordinary durable path until multi-bucket segment install is atomic")
	require.Equal(t, uint64(0), committer.Stats()["commit_total"])
}

func TestExecutorRenameRejectsCrossAuthority(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "old", 22)
	authority := &fakeAuthorityResolver{same: false}
	resolver := &fakeMountResolver{records: map[model.MountID]MountAdmission{
		"vol": {MountID: "vol", MountKeyID: 1, RootInode: model.RootInode, SchemaVersion: 1},
	}}
	executor, err := newTestExecutor(runner, WithMountResolver(resolver), WithSubtreeAuthorityResolver(authority))
	require.NoError(t, err)

	err = executor.Rename(context.Background(), model.RenameRequest{
		Mount:      "vol",
		FromParent: 7,
		FromName:   "old",
		ToParent:   8,
		ToName:     "new",
	})
	require.ErrorIs(t, err, model.ErrCrossAuthorityRename)
	require.Empty(t, runner.mutations)
	require.Equal(t, 1, authority.calls)
}

func TestExecutorRenameSubtreeRejectsMissingSource(t *testing.T) {
	runner := newFakeRunner()
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	err = executor.RenameSubtree(context.Background(), model.RenameSubtreeRequest{
		Mount:      "vol",
		FromParent: 7,
		FromName:   "missing",
		ToParent:   8,
		ToName:     "new",
	})
	require.ErrorIs(t, err, model.ErrNotFound)
	require.Empty(t, runner.mutations)
}

func TestExecutorRenameSubtreeRejectsExistingDestination(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "old", 22)
	seedDentry(t, runner, "vol", 8, "existing", 23)
	publisher := &fakeSubtreePublisher{}
	resolver := &fakeMountResolver{records: map[model.MountID]MountAdmission{
		"vol": {MountID: "vol", MountKeyID: 1, RootInode: model.RootInode, SchemaVersion: 1},
	}}
	executor, err := newTestExecutor(runner, WithMountResolver(resolver), WithSubtreeHandoffPublisher(publisher))
	require.NoError(t, err)

	err = executor.RenameSubtree(context.Background(), model.RenameSubtreeRequest{
		Mount:      "vol",
		FromParent: 7,
		FromName:   "old",
		ToParent:   8,
		ToName:     "existing",
	})
	require.ErrorIs(t, err, model.ErrExists)
	require.Empty(t, runner.mutations)
	require.Empty(t, publisher.starts)
	require.Empty(t, publisher.completes)
}

func BenchmarkExecutorRenameDefaultPath(b *testing.B) {
	runner := newFakeRunner()
	executor, err := newTestExecutor(runner)
	if err != nil {
		b.Fatal(err)
	}
	benchmarkExecutorRename(b, runner, executor)
}

func BenchmarkExecutorRenameVisibleCommit(b *testing.B) {
	runner := newFakeRunner()
	executor, err := newTestExecutor(
		runner,
		WithVisibleAuthorityAdmitter(ownedVisibleAdmitter{}),
		WithVisibleCommitter(noopVisibleCommitter{}),
	)
	if err != nil {
		b.Fatal(err)
	}
	benchmarkExecutorRename(b, runner, executor)
}
