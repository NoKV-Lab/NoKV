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

func TestExecutorUpdateInodeUsesAtomicMutateWithValuePredicates(t *testing.T) {
	base := newFakeRunner()
	runner := &fakeAtomicRunner{fakeRunner: base, handled: true}
	seedDentry(t, runner.fakeRunner, "vol", 7, "file", 22)
	seedInode(t, runner.fakeRunner, "vol", fsmeta.InodeRecord{Inode: 22, Type: fsmeta.InodeTypeFile, Mode: 0o644, LinkCount: 1})
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	updated, err := executor.UpdateInode(context.Background(), fsmeta.UpdateInodeRequest{
		Mount:   "vol",
		Parent:  7,
		Inode:   22,
		Name:    "file",
		SetMode: true,
		Mode:    0o600,
	})
	require.NoError(t, err)
	require.Equal(t, uint32(0o600), updated.Mode)
	require.Len(t, runner.atomicCalls, 1)
	require.Empty(t, base.mutations)
	requireAtomicStatUint(t, executor.Stats(), fsmeta.OperationUpdateInode, "success_total", 1)
	require.Equal(t, kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_VALUE_EQUALS, runner.atomicCalls[0].predicates[0].GetKind())
	require.Equal(t, kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_VALUE_EQUALS, runner.atomicCalls[0].predicates[1].GetKind())

	stored, ok, err := executor.readInode(context.Background(), testMountIdentity, 22, 99)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint32(0o600), stored.Mode)
}

func TestExecutorUpdateInodeSkipsAtomicMutateWhenQuotaMutates(t *testing.T) {
	base := newFakeRunner()
	runner := &fakeAtomicRunner{fakeRunner: base, handled: true}
	seedDentry(t, runner.fakeRunner, "vol", 7, "file", 22)
	seedInode(t, runner.fakeRunner, "vol", fsmeta.InodeRecord{Inode: 22, Type: fsmeta.InodeTypeFile, Size: 1024, LinkCount: 1})
	quotaKey, err := fsmeta.EncodeUsageKey(testMountIdentity, 7)
	require.NoError(t, err)
	quota := &fakeQuotaResolver{mutation: &kvrpcpb.Mutation{Op: kvrpcpb.Mutation_Put, Key: quotaKey, Value: []byte("usage")}}
	executor, err := newTestExecutor(runner, WithQuotaResolver(quota))
	require.NoError(t, err)

	_, err = executor.UpdateInode(context.Background(), fsmeta.UpdateInodeRequest{
		Mount:   "vol",
		Parent:  7,
		Inode:   22,
		Name:    "file",
		SetSize: true,
		Size:    2048,
	})
	require.NoError(t, err)
	require.Empty(t, runner.atomicCalls)
	require.Len(t, base.mutations, 1)
	requireAtomicStatUint(t, executor.Stats(), fsmeta.OperationUpdateInode, "skip_total", 1)
}

func TestExecutorUpdateInodeVisibleCommitReadsCreateOverlay(t *testing.T) {
	runner := newFakeRunner()
	seedDirectory(t, runner, "vol", 7)
	committer := newTestVisibleCommitter(t, runner)
	inode := testInodeForParentBucket(t, 7, 7)
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{ids: []fsmeta.InodeID{inode}}),
		WithVisibleAuthorityAdmitter(ownedVisibleAdmitter{}),
		WithVisibleCommitter(committer),
	)
	require.NoError(t, err)

	created, err := executor.Create(context.Background(), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: 7,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile, Mode: 0o644},
	})
	require.NoError(t, err)

	updated, err := executor.UpdateInode(context.Background(), fsmeta.UpdateInodeRequest{
		Mount:            "vol",
		Parent:           7,
		Inode:            created.Inode.Inode,
		Name:             "file",
		SetSize:          true,
		Size:             8192,
		SetMode:          true,
		Mode:             0o600,
		SetUpdatedUnixNs: true,
		UpdatedUnixNs:    42,
	})
	require.NoError(t, err)

	require.Equal(t, uint32(0o600), updated.Mode)
	require.Equal(t, uint64(8192), updated.Size)
	require.Equal(t, int64(42), updated.UpdatedUnixNs)
	stored, ok, err := executor.readInode(context.Background(), testMountIdentity, created.Inode.Inode, 99)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, updated, stored)
	require.Empty(t, runner.mutations, "create+update should stay inside visible overlay")

	stats := executor.Stats()
	requireVisibleCommitStatUint(t, stats, "attempt_total", 2)
	requireVisibleCommitStatUint(t, stats, "success_total", 2)
}

func TestExecutorUpdateInodeVisibleRechecksObservedValue(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "file", 22)
	seedInode(t, runner, "vol", fsmeta.InodeRecord{
		Inode:     22,
		Type:      fsmeta.InodeTypeFile,
		LinkCount: 1,
		Size:      1024,
	})
	changed := false
	committer := &fakeVisibleCommitter{
		beforeAdmission: func() {
			if changed {
				return
			}
			changed = true
			seedInode(t, runner, "vol", fsmeta.InodeRecord{
				Inode:     22,
				Type:      fsmeta.InodeTypeFile,
				LinkCount: 1,
				Size:      4096,
			})
		},
	}
	executor, err := newTestExecutor(
		runner,
		WithVisibleAuthorityAdmitter(ownedVisibleAdmitter{}),
		WithVisibleCommitter(committer),
	)
	require.NoError(t, err)

	updated, err := executor.UpdateInode(context.Background(), fsmeta.UpdateInodeRequest{
		Mount:   "vol",
		Parent:  7,
		Inode:   22,
		Name:    "file",
		SetSize: true,
		Size:    2048,
	})
	require.NoError(t, err)

	require.Equal(t, uint64(2048), updated.Size)
	require.Zero(t, committer.calls, "stale observed value must reject the visible admission before commit")
	require.Len(t, runner.mutations, 1, "rejected visible admission should fall back to the ordinary transaction path")
	requireVisibleCommitStatUint(t, executor.Stats(), "skip_predicate_total", 1)
}

func TestExecutorUpdateInodeUpdatesMutableFieldsAndQuota(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "file", 22)
	seedInode(t, runner, "vol", fsmeta.InodeRecord{
		Inode:         22,
		Type:          fsmeta.InodeTypeFile,
		Size:          4096,
		Mode:          0o644,
		LinkCount:     1,
		CreatedUnixNs: 10,
		UpdatedUnixNs: 20,
	})
	quotaKey, err := fsmeta.EncodeUsageKey(testMountIdentity, 7)
	require.NoError(t, err)
	quota := &fakeQuotaResolver{mutation: &kvrpcpb.Mutation{Op: kvrpcpb.Mutation_Put, Key: quotaKey, Value: []byte("usage")}}
	executor, err := newTestExecutor(runner, WithQuotaResolver(quota))
	require.NoError(t, err)

	updated, err := executor.UpdateInode(context.Background(), fsmeta.UpdateInodeRequest{
		Mount:            "vol",
		Parent:           7,
		Inode:            22,
		Name:             "file",
		SetSize:          true,
		Size:             8192,
		SetMode:          true,
		Mode:             0o600,
		SetUpdatedUnixNs: true,
		UpdatedUnixNs:    30,
		SetOpaqueAttrs:   true,
		OpaqueAttrs:      []byte("body=cas://1"),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(8192), updated.Size)
	require.Equal(t, uint32(0o600), updated.Mode)
	require.Equal(t, int64(30), updated.UpdatedUnixNs)
	require.Equal(t, []byte("body=cas://1"), updated.OpaqueAttrs)
	require.Equal(t, int64(10), updated.CreatedUnixNs)
	require.Equal(t, [][]QuotaChange{{{Mount: "vol", MountKeyID: 1, Scope: 7, Bytes: 4096}}}, quota.changes)
	require.Len(t, runner.mutations, 1)
	require.Equal(t, quotaKey, runner.mutations[0][1].GetKey())

	stored, ok, err := executor.readInode(context.Background(), testMountIdentity, 22, 99)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, updated, stored)
}

func TestExecutorUpdateInodeRejectsHardLinkedInode(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "file", 22)
	seedInode(t, runner, "vol", fsmeta.InodeRecord{Inode: 22, Type: fsmeta.InodeTypeFile, Size: 4096, LinkCount: 2})
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	_, err = executor.UpdateInode(context.Background(), fsmeta.UpdateInodeRequest{
		Mount:   "vol",
		Parent:  7,
		Inode:   22,
		Name:    "file",
		SetSize: true,
		Size:    8192,
	})
	require.ErrorIs(t, err, fsmeta.ErrInvalidRequest)
	require.Empty(t, runner.mutations)
}

func TestExecutorUpdateInodeRejectsDentryTypeMismatch(t *testing.T) {
	runner := newFakeRunner()
	seedDentryType(t, runner, "vol", 7, "file", 22, fsmeta.InodeTypeDirectory)
	seedInode(t, runner, "vol", fsmeta.InodeRecord{Inode: 22, Type: fsmeta.InodeTypeFile, Size: 4096, LinkCount: 1})
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	_, err = executor.UpdateInode(context.Background(), fsmeta.UpdateInodeRequest{
		Mount:   "vol",
		Parent:  7,
		Inode:   22,
		Name:    "file",
		SetMode: true,
		Mode:    0o600,
	})
	require.ErrorIs(t, err, fsmeta.ErrInvalidValue)
	require.Empty(t, runner.mutations)
}

func BenchmarkExecutorUpdateInodeDefaultPath(b *testing.B) {
	runner := newFakeRunner()
	executor, err := newTestExecutor(runner)
	if err != nil {
		b.Fatal(err)
	}
	benchmarkExecutorUpdateInode(b, runner, executor)
}

func BenchmarkExecutorUpdateInodeVisibleCommit(b *testing.B) {
	runner := newFakeRunner()
	executor, err := newTestExecutor(
		runner,
		WithVisibleAuthorityAdmitter(ownedVisibleAdmitter{}),
		WithVisibleCommitter(noopVisibleCommitter{}),
	)
	if err != nil {
		b.Fatal(err)
	}
	benchmarkExecutorUpdateInode(b, runner, executor)
}
