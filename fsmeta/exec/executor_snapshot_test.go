// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"context"
	"errors"
	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestExecutorSnapshotSubtreeTokenDrivesReadVersion(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "a", 21)
	seedInode(t, runner, "vol", fsmeta.InodeRecord{Inode: 21, Type: fsmeta.InodeTypeFile, LinkCount: 1})
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	token, err := executor.SnapshotSubtree(context.Background(), fsmeta.SnapshotSubtreeRequest{
		Mount:     "vol",
		RootInode: 7,
	})
	require.NoError(t, err)
	require.Equal(t, fsmeta.SnapshotSubtreeToken{Mount: "vol", MountKeyID: 1, RootInode: 7, ReadVersion: 1}, token)

	_, err = executor.ReadDirPlus(context.Background(), fsmeta.ReadDirRequest{
		Mount:           "vol",
		Parent:          7,
		Limit:           8,
		SnapshotVersion: token.ReadVersion,
	})
	require.NoError(t, err)
	require.Equal(t, []uint64{token.ReadVersion}, runner.scanVersions)
	require.Equal(t, []uint64{token.ReadVersion}, runner.batchVersions)
}

func TestExecutorSnapshotSubtreeFlushesVisibleAuthorityBeforeToken(t *testing.T) {
	runner := newFakeRunner()
	flusher := &fakeVisibleAuthorityFlusher{}
	executor, err := newTestExecutor(runner,
		WithVisibleCommitter(flusher),
		WithVisibleAuthorityAdmitter(&fakeVisibleAdmitter{owned: true}),
	)
	require.NoError(t, err)

	token, err := executor.SnapshotSubtree(context.Background(), fsmeta.SnapshotSubtreeRequest{
		Mount:     "vol",
		RootInode: 7,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), token.ReadVersion)
	require.Equal(t, 1, flusher.flushCalls)
	require.Len(t, flusher.flushScopes, 1)
	require.Equal(t, fsmeta.MountID("vol"), flusher.flushScopes[0].Mount)
	require.Equal(t, fsmeta.MountKeyID(1), flusher.flushScopes[0].MountKeyID)
	require.Equal(t, []fsmeta.InodeID{7}, flusher.flushScopes[0].Parents)
}

func TestExecutorSnapshotSubtreeUsesVisibleCaptureWhenAvailable(t *testing.T) {
	runner := newFakeRunner()
	ref := testSnapshotEvidenceRef(3, 0xaa)
	capturer := &fakeVisibleSnapshotCapturer{capture: true, segmentRefs: []fsmeta.SnapshotEvidenceRef{ref}}
	executor, err := newTestExecutor(runner,
		WithVisibleCommitter(capturer),
		WithVisibleAuthorityAdmitter(&fakeVisibleAdmitter{owned: true}),
	)
	require.NoError(t, err)

	token, err := executor.SnapshotSubtree(context.Background(), fsmeta.SnapshotSubtreeRequest{
		Mount:     "vol",
		RootInode: 7,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), token.ReadVersion)
	require.Equal(t, []uint64{1}, capturer.captureVersions)
	require.Len(t, capturer.captureScopes, 1)
	require.Equal(t, []fsmeta.InodeID{7}, capturer.captureScopes[0].Parents)
	require.Equal(t, []fsmeta.SnapshotEvidenceRef{ref}, token.RuntimeEvidence)
	require.Equal(t, 0, capturer.flushCalls)
}

func TestExecutorResolveSnapshotSubtreeTokenAllowsRetiredMount(t *testing.T) {
	runner := newFakeRunner()
	resolver := &fakeMountResolver{records: map[fsmeta.MountID]MountAdmission{
		"vol": {MountID: "vol", MountKeyID: 9, RootInode: fsmeta.RootInode, SchemaVersion: 1, Retired: true},
	}}
	executor, err := newTestExecutor(runner, WithMountResolver(resolver))
	require.NoError(t, err)

	token, err := executor.ResolveSnapshotSubtreeToken(context.Background(), fsmeta.SnapshotSubtreeToken{
		Mount:       "vol",
		RootInode:   7,
		ReadVersion: 42,
	})
	require.NoError(t, err)
	require.Equal(t, fsmeta.SnapshotSubtreeToken{Mount: "vol", MountKeyID: 9, RootInode: 7, ReadVersion: 42}, token)
}

func TestExecutorResolveSnapshotSubtreeTokenRejectsInvalidVisibleRef(t *testing.T) {
	runner := newFakeRunner()
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	_, err = executor.ResolveSnapshotSubtreeToken(context.Background(), fsmeta.SnapshotSubtreeToken{
		Mount:           "vol",
		RootInode:       7,
		ReadVersion:     42,
		RuntimeEvidence: []fsmeta.SnapshotEvidenceRef{{EpochID: 1}},
	})
	require.ErrorIs(t, err, fsmeta.ErrInvalidRequest)
}

func testSnapshotEvidenceRef(epoch uint64, seed byte) fsmeta.SnapshotEvidenceRef {
	var root [32]byte
	var digest [32]byte
	root[0] = seed
	digest[0] = seed + 1
	return fsmeta.SnapshotEvidenceRef{
		EpochID:       epoch,
		EvidenceRoot:  root,
		PayloadDigest: digest,
	}
}

func TestExecutorReadDirPlusRetriesLiveLockAtSnapshotVersion(t *testing.T) {
	runner := newFakeRunner()
	runner.scanErrs = []error{txnLockedError("vol", 7, "a")}
	seedDentry(t, runner, "vol", 7, "a", 21)
	seedInode(t, runner, "vol", fsmeta.InodeRecord{
		Inode:     21,
		Type:      fsmeta.InodeTypeFile,
		LinkCount: 1,
	})
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	pairs, err := executor.ReadDirPlus(context.Background(), fsmeta.ReadDirRequest{
		Mount:           "vol",
		Parent:          7,
		Limit:           8,
		SnapshotVersion: 100,
	})
	require.NoError(t, err)
	require.Len(t, pairs, 1)
	require.Equal(t, []uint64{100, 100}, runner.scanVersions)
	requireStatUint(t, executor.Stats(), "read_retries_total", 1)
	requireStatUint(t, executor.Stats(), "read_retry_exhausted_total", 0)
}

func TestExecutorRenameMovesDentryWithoutSubtreeHandoff(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "old", 22)
	seedDirectory(t, runner, "vol", 8)
	publisher := &fakeSubtreePublisher{}
	authority := &fakeAuthorityResolver{same: true}
	resolver := &fakeMountResolver{records: map[fsmeta.MountID]MountAdmission{
		"vol": {MountID: "vol", MountKeyID: 1, RootInode: fsmeta.RootInode, SchemaVersion: 1},
	}}
	executor, err := newTestExecutor(
		runner,
		WithMountResolver(resolver),
		WithSubtreeAuthorityResolver(authority),
		WithSubtreeHandoffPublisher(publisher),
	)
	require.NoError(t, err)

	err = executor.Rename(context.Background(), fsmeta.RenameRequest{
		Mount:      "vol",
		FromParent: 7,
		FromName:   "old",
		ToParent:   8,
		ToName:     "new",
	})
	require.NoError(t, err)

	_, err = executor.Lookup(context.Background(), fsmeta.LookupRequest{Mount: "vol", Parent: 7, Name: "old"})
	require.ErrorIs(t, err, fsmeta.ErrNotFound)
	record, err := executor.Lookup(context.Background(), fsmeta.LookupRequest{Mount: "vol", Parent: 8, Name: "new"})
	require.NoError(t, err)
	require.Equal(t, fsmeta.InodeID(22), record.Inode)
	require.Len(t, runner.mutations, 1)
	require.Empty(t, publisher.starts)
	require.Empty(t, publisher.completes)
	require.Equal(t, 1, authority.calls)
}

func TestExecutorRenameUsesAtomicMutateWithoutSubtreeHandoff(t *testing.T) {
	base := newFakeRunner()
	runner := &fakeAtomicRunner{fakeRunner: base, handled: true}
	seedDentry(t, runner.fakeRunner, "vol", 7, "old", 22)
	seedDirectory(t, runner.fakeRunner, "vol", 8)
	seedInode(t, runner.fakeRunner, "vol", fsmeta.InodeRecord{Inode: 22, Type: fsmeta.InodeTypeFile, LinkCount: 1})
	publisher := &fakeSubtreePublisher{}
	authority := &fakeAuthorityResolver{same: true}
	resolver := &fakeMountResolver{records: map[fsmeta.MountID]MountAdmission{
		"vol": {MountID: "vol", MountKeyID: 1, RootInode: fsmeta.RootInode, SchemaVersion: 1},
	}}
	executor, err := newTestExecutor(
		runner,
		WithMountResolver(resolver),
		WithSubtreeAuthorityResolver(authority),
		WithSubtreeHandoffPublisher(publisher),
	)
	require.NoError(t, err)

	err = executor.Rename(context.Background(), fsmeta.RenameRequest{
		Mount:      "vol",
		FromParent: 7,
		FromName:   "old",
		ToParent:   8,
		ToName:     "new",
	})
	require.NoError(t, err)

	require.Len(t, runner.atomicCalls, 1)
	require.Empty(t, base.mutations)
	require.Empty(t, publisher.starts)
	require.Empty(t, publisher.completes)
	requireAtomicStatUint(t, executor.Stats(), fsmeta.OperationRename, "success_total", 1)
	record, err := executor.Lookup(context.Background(), fsmeta.LookupRequest{Mount: "vol", Parent: 8, Name: "new"})
	require.NoError(t, err)
	require.Equal(t, fsmeta.InodeID(22), record.Inode)
}

func TestExecutorRenameSubtreePinsCommitVersionToHandoffFrontier(t *testing.T) {
	runner := newFakeRunner()
	runner.actualCommitVersion = 99
	seedDentry(t, runner, "vol", 7, "old", 22)
	seedDirectory(t, runner, "vol", 8)
	publisher := &fakeSubtreePublisher{}
	resolver := &fakeMountResolver{records: map[fsmeta.MountID]MountAdmission{
		"vol": {MountID: "vol", MountKeyID: 1, RootInode: fsmeta.RootInode, SchemaVersion: 1},
	}}
	executor, err := newTestExecutor(runner, WithMountResolver(resolver), WithSubtreeHandoffPublisher(publisher))
	require.NoError(t, err)

	err = executor.RenameSubtree(context.Background(), fsmeta.RenameSubtreeRequest{
		Mount:      "vol",
		FromParent: 7,
		FromName:   "old",
		ToParent:   8,
		ToName:     "new",
	})
	require.NoError(t, err)

	require.Equal(t, []subtreePublishCall{{mount: "vol", root: fsmeta.RootInode, frontier: 2}}, publisher.starts)
	require.Equal(t, []subtreePublishCall{{mount: "vol", root: fsmeta.RootInode, frontier: 2}}, publisher.completes)
}

func TestExecutorRenameSubtreeBlocksMutationWhenStartHandoffFails(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "old", 22)
	seedDirectory(t, runner, "vol", 8)
	publisher := &fakeSubtreePublisher{startErr: errors.New("publish failed")}
	resolver := &fakeMountResolver{records: map[fsmeta.MountID]MountAdmission{
		"vol": {MountID: "vol", MountKeyID: 1, RootInode: fsmeta.RootInode, SchemaVersion: 1},
	}}
	executor, err := newTestExecutor(runner, WithMountResolver(resolver), WithSubtreeHandoffPublisher(publisher))
	require.NoError(t, err)

	err = executor.RenameSubtree(context.Background(), fsmeta.RenameSubtreeRequest{
		Mount:      "vol",
		FromParent: 7,
		FromName:   "old",
		ToParent:   8,
		ToName:     "new",
	})
	require.ErrorContains(t, err, "publish failed")
	require.Empty(t, runner.mutations)

	record, err := executor.Lookup(context.Background(), fsmeta.LookupRequest{Mount: "vol", Parent: 7, Name: "old"})
	require.NoError(t, err)
	require.Equal(t, fsmeta.InodeID(22), record.Inode)
}

func TestExecutorRenameSubtreeReportsCompleteHandoffFailureAfterMutation(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "old", 22)
	seedDirectory(t, runner, "vol", 8)
	publisher := &fakeSubtreePublisher{completeErr: errors.New("complete failed")}
	resolver := &fakeMountResolver{records: map[fsmeta.MountID]MountAdmission{
		"vol": {MountID: "vol", MountKeyID: 1, RootInode: fsmeta.RootInode, SchemaVersion: 1},
	}}
	executor, err := newTestExecutor(runner, WithMountResolver(resolver), WithSubtreeHandoffPublisher(publisher))
	require.NoError(t, err)

	err = executor.RenameSubtree(context.Background(), fsmeta.RenameSubtreeRequest{
		Mount:      "vol",
		FromParent: 7,
		FromName:   "old",
		ToParent:   8,
		ToName:     "new",
	})
	require.ErrorContains(t, err, "complete failed")
	require.Len(t, runner.mutations, 1)
	require.Equal(t, []subtreePublishCall{{mount: "vol", root: fsmeta.RootInode, frontier: 2}}, publisher.starts)

	_, err = executor.Lookup(context.Background(), fsmeta.LookupRequest{Mount: "vol", Parent: 7, Name: "old"})
	require.ErrorIs(t, err, fsmeta.ErrNotFound)
	record, err := executor.Lookup(context.Background(), fsmeta.LookupRequest{Mount: "vol", Parent: 8, Name: "new"})
	require.NoError(t, err)
	require.Equal(t, fsmeta.InodeID(22), record.Inode)
}
