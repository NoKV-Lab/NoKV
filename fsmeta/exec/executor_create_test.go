// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"context"
	"errors"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta/backend"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/stretchr/testify/require"
)

func TestExecutorCreateAdmitsVisibleAuthority(t *testing.T) {
	runner := newFakeRunner()
	admitter := &fakeVisibleAdmitter{owned: true}
	inode := testInodeForParentBucket(t, model.RootInode)
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{inode}}),
		WithVisibleAuthorityAdmitter(admitter),
	)
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	})
	require.NoError(t, err)

	require.Equal(t, 1, admitter.calls)
	require.Len(t, admitter.scopes, 1)
	require.Equal(t, model.MountID("vol"), admitter.scopes[0].Mount)
	require.Equal(t, model.MountKeyID(1), admitter.scopes[0].MountKeyID)
	require.Equal(t, []model.InodeID{model.RootInode}, admitter.scopes[0].Parents)
	require.Equal(t, []model.InodeID{inode}, admitter.scopes[0].Inodes)
	require.Len(t, runner.mutations, 1)

	stats := executor.Stats()
	requireVisibleStatBool(t, stats, "enabled", true)
	requireVisibleStatUint(t, stats, "eligible_total", 1)
	requireVisibleStatUint(t, stats, "acquire_total", 1)
	requireVisibleStatUint(t, stats, "owned_total", 1)
	requireVisibleStatUint(t, stats, "held_total", 0)
	requireVisibleStatUint(t, stats, "slow_total", 0)
}

func TestExecutorCreateVisibleCommitBypassesRaftCommit(t *testing.T) {
	runner := newFakeRunner()
	committer := &fakeVisibleCommitter{}
	inode := testInodeForParentBucket(t, model.RootInode)
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{inode}}),
		WithVisibleAuthorityAdmitter(&fakeVisibleAdmitter{owned: true}),
		WithVisibleCommitter(committer),
	)
	require.NoError(t, err)

	result, err := executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	})
	require.NoError(t, err)

	require.Equal(t, inode, result.Inode.Inode)
	require.Equal(t, inode, result.Dentry.Inode)
	require.Equal(t, 1, committer.calls)
	require.Len(t, committer.ids, 1)
	require.Contains(t, committer.ids[0].ClientID, "fsmeta-exec/create")
	require.Equal(t, uint64(1), committer.ids[0].Seq)
	require.Len(t, committer.deltas, 1)
	require.Equal(t, compile.EligibilityVisibleCommit, committer.deltas[0].Eligibility)
	require.Empty(t, runner.mutations, "visible commit success must bypass the current Raft commit")

	stats := executor.Stats()
	requireVisibleCommitStatBool(t, stats, "enabled", true)
	requireVisibleCommitStatUint(t, stats, "attempt_total", 1)
	requireVisibleCommitStatUint(t, stats, "success_total", 1)
	requireVisibleCommitStatUint(t, stats, "error_total", 0)
	requireVisibleCommitStatUint(t, stats, "skip_no_authority_total", 0)
}

func TestExecutorCreateVisibleCommitRejectsExistingDentry(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", model.RootInode, "file", 21)
	committer := &fakeVisibleCommitter{}
	inode := testInodeForParentBucket(t, model.RootInode)
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
	require.ErrorIs(t, err, model.ErrExists)
	require.Zero(t, committer.calls, "failed not-exists predicate must not enter visible commit")
	require.Empty(t, runner.mutations)

	stats := executor.Stats()
	requireVisibleCommitStatUint(t, stats, "attempt_total", 1)
	requireVisibleCommitStatUint(t, stats, "skip_predicate_total", 1)
}

func TestExecutorCreateVisibleCommitErrorDoesNotFallback(t *testing.T) {
	runner := newFakeRunner()
	commitErr := errors.New("overlay commit failed")
	committer := &fakeVisibleCommitter{err: commitErr}
	inode := testInodeForParentBucket(t, model.RootInode)
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
	require.ErrorIs(t, err, commitErr)
	require.Equal(t, 1, committer.calls)
	require.Empty(t, runner.mutations, "ambiguous Visible evidence must not fall back into a second commit path")

	stats := executor.Stats()
	requireVisibleCommitStatUint(t, stats, "attempt_total", 1)
	requireVisibleCommitStatUint(t, stats, "success_total", 0)
	requireVisibleCommitStatUint(t, stats, "error_total", 1)
}

func TestExecutorCreateVisibleCommitRequiresAuthorityAdmission(t *testing.T) {
	runner := newFakeRunner()
	committer := &fakeVisibleCommitter{}
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{22}}),
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
	require.Zero(t, committer.calls)
	require.Len(t, runner.mutations, 1)

	stats := executor.Stats()
	requireVisibleCommitStatUint(t, stats, "attempt_total", 0)
	requireVisibleCommitStatUint(t, stats, "skip_no_authority_total", 0)
}

func TestExecutorCreateVisibleCommitSkipsSharedQuota(t *testing.T) {
	runner := newFakeRunner()
	seedDirectory(t, runner, "vol", 7)
	quotaKey, err := layout.EncodeUsageKey(testMountIdentity, 7)
	require.NoError(t, err)
	quota := &fakeQuotaResolver{mutation: &backend.Mutation{Op: backend.MutationPut, Key: quotaKey, Value: []byte("usage")}}
	committer := &fakeVisibleCommitter{}
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{22}}),
		WithQuotaResolver(quota),
		WithVisibleAuthorityAdmitter(&fakeVisibleAdmitter{owned: true}),
		WithVisibleCommitter(committer),
	)
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: 7,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile, Size: 4096},
	})
	require.NoError(t, err)

	require.Zero(t, committer.calls, "shared quota must remain on the transaction runner until quota credits exist")
	require.Equal(t, [][]QuotaChange{{{Mount: "vol", MountKeyID: 1, Scope: 7, Bytes: 4096, Inodes: 1}}}, quota.changes)
	require.Len(t, runner.mutations, 1)
	require.Len(t, runner.mutations[0], 4)

	stats := executor.Stats()
	requireVisibleCommitStatUint(t, stats, "attempt_total", 0)
	require.Equal(t, [][]QuotaChange{{{Mount: "vol", MountKeyID: 1, Scope: 7, Bytes: 4096, Inodes: 1}}}, quota.perasChecks)
}

func TestExecutorCreateVisibleCommitAllowsQuotaResolverWithoutFence(t *testing.T) {
	runner := newFakeRunner()
	seedDirectory(t, runner, "vol", 7)
	quota := &fakeQuotaResolver{allowVisibleCommit: true}
	committer := &fakeVisibleCommitter{}
	inode := testInodeForParentBucket(t, 7, 7)
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{inode}}),
		WithQuotaResolver(quota),
		WithVisibleAuthorityAdmitter(&fakeVisibleAdmitter{owned: true}),
		WithVisibleCommitter(committer),
	)
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: 7,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile, Size: 4096},
	})
	require.NoError(t, err)

	require.Equal(t, 1, committer.calls)
	require.Empty(t, quota.changes)
	require.Empty(t, runner.mutations)
	require.Equal(t, [][]QuotaChange{{{Mount: "vol", MountKeyID: 1, Scope: 7, Bytes: 4096, Inodes: 1}}}, quota.perasChecks)

	stats := executor.Stats()
	requireVisibleCommitStatUint(t, stats, "attempt_total", 1)
	requireVisibleCommitStatUint(t, stats, "success_total", 1)
}

func TestExecutorCreateVisibleCommitRejectsOverlayDuplicate(t *testing.T) {
	runner := newFakeRunner()
	committer := newTestVisibleCommitter(t, runner)
	firstInode := testInodeForParentBucket(t, model.RootInode)
	secondInode := testInodeForParentBucket(t, model.RootInode, firstInode)
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{firstInode, secondInode}}),
		WithVisibleAuthorityAdmitter(ownedVisibleAdmitter{}),
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

	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	})
	require.ErrorIs(t, err, model.ErrExists)
	require.Empty(t, runner.mutations, "overlay predicate failure must not fall back into ordinary mutation")
	require.Equal(t, uint64(1), committer.Stats()["commit_total"])

	stats := executor.Stats()
	requireVisibleCommitStatUint(t, stats, "attempt_total", 2)
	requireVisibleCommitStatUint(t, stats, "success_total", 1)
	requireVisibleCommitStatUint(t, stats, "skip_predicate_total", 1)
	requireVisibleCommitStatUint(t, stats, "error_total", 0)
}

func TestExecutorCreateVisibleCommitUsesEmptyDirectoryFact(t *testing.T) {
	runner := newFakeRunner()
	committer := newTestVisibleCommitter(t, runner)
	dirInode := testInodeForParentBucket(t, model.RootInode)
	childInode := testInodeForParentBucket(t, dirInode, dirInode)
	nextChildInode := testInodeForParentBucket(t, dirInode, dirInode, childInode)
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{dirInode, childInode, nextChildInode}}),
		WithVisibleAuthorityAdmitter(ownedVisibleAdmitter{}),
		WithVisibleCommitter(committer),
	)
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "run",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeDirectory},
	})
	require.NoError(t, err)
	getsAfterDir := runner.getCalls

	created, err := executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: dirInode,
		Name:   "part-000",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	})
	require.NoError(t, err)

	require.Equal(t, childInode, created.Inode.Inode)
	require.Equal(t, getsAfterDir, runner.getCalls, "empty-directory admission should avoid per-child predicate reads")

	nextCreated, err := executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: dirInode,
		Name:   "part-001",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	})
	require.NoError(t, err)

	require.Equal(t, nextChildInode, nextCreated.Inode.Inode)
	require.Equal(t, getsAfterDir, runner.getCalls, "base-empty directory coverage should avoid later child predicate reads")
	require.Empty(t, runner.mutations)
	require.Equal(t, uint64(3), committer.Stats()["commit_total"])
}

func TestExecutorCreateFallsBackWhenVisibleAuthorityHeldElsewhere(t *testing.T) {
	runner := newFakeRunner()
	admitter := &fakeVisibleAdmitter{owned: false}
	inode := testInodeForParentBucket(t, model.RootInode)
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{inode}}),
		WithVisibleAuthorityAdmitter(admitter),
	)
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	})
	require.NoError(t, err)
	require.Equal(t, 1, admitter.calls)
	require.NotEmpty(t, runner.mutations)

	stats := executor.Stats()
	requireVisibleStatUint(t, stats, "eligible_total", 1)
	requireVisibleStatUint(t, stats, "acquire_total", 1)
	requireVisibleStatUint(t, stats, "owned_total", 0)
	requireVisibleStatUint(t, stats, "held_total", 1)
}

func TestExecutorCreateWithSharedQuotaSkipsVisibleAuthorityAdmission(t *testing.T) {
	runner := newFakeRunner()
	admitter := &fakeVisibleAdmitter{owned: true}
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{22}}),
		WithQuotaResolver(&fakeQuotaResolver{}),
		WithVisibleAuthorityAdmitter(admitter),
	)
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile, Size: 4096},
	})
	require.NoError(t, err)
	require.Zero(t, admitter.calls)
	require.Len(t, runner.mutations, 1)

	stats := executor.Stats()
	requireVisibleStatUint(t, stats, "eligible_total", 0)
	requireVisibleStatUint(t, stats, "acquire_total", 0)
	requireVisibleStatUint(t, stats, "owned_total", 0)
	requireVisibleStatUint(t, stats, "slow_total", 1)
	requireVisibleSlowReasonStatUint(t, stats, compile.SlowReasonSharedQuota, 1)
}

func TestExecutorCreateRequiresInodeAllocator(t *testing.T) {
	executor, err := newTestExecutor(newFakeRunner())
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	})
	require.ErrorIs(t, err, errInodeAllocatorRequired)
}

func TestExecutorCreateUsesAtomicMutateOnePhaseWhenHandled(t *testing.T) {
	base := newFakeRunner()
	runner := &fakeAtomicRunner{fakeRunner: base, handled: true}
	executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{22}}))
	require.NoError(t, err)

	req := model.CreateRequest{Mount: "vol", Parent: model.RootInode, Name: "file", Attrs: model.CreateAttrs{Type: model.InodeTypeFile}}
	_, err = executor.Create(context.Background(), req)
	require.NoError(t, err)

	plan, err := layout.PlanCreate(req, testMountIdentity, 22)
	require.NoError(t, err)
	stats := executor.Stats()
	requireStatUint(t, stats, "create_total", 1)
	requireAtomicStatUint(t, stats, model.OperationCreate, "attempt_total", 1)
	requireAtomicStatUint(t, stats, model.OperationCreate, "success_total", 1)
	requireAtomicStatUint(t, stats, model.OperationCreate, "fallback_total", 0)
	requireAtomicStatUint(t, stats, model.OperationCreate, "skip_total", 0)
	requireAtomicStatUint(t, stats, model.OperationCreate, "runner_unsupported_total", 0)
	require.Len(t, runner.atomicCalls, 1)
	call := runner.atomicCalls[0]
	require.Equal(t, plan.PrimaryKey, call.primary)
	require.Equal(t, uint64(1), call.startVersion)
	require.Equal(t, uint64(2), call.commitVersion)
	require.Len(t, call.predicates, 3)
	require.Equal(t, plan.ReadKeys[0], call.predicates[0].Key)
	require.Equal(t, backend.PredicateValueEquals, call.predicates[0].Kind)
	require.Equal(t, plan.MutateKeys[1], call.predicates[1].Key)
	require.Equal(t, backend.PredicateNotExists, call.predicates[1].Kind)
	require.Equal(t, plan.MutateKeys[2], call.predicates[2].Key)
	require.Equal(t, backend.PredicateNotExists, call.predicates[2].Kind)
	require.Len(t, call.mutations, 3)
	require.True(t, call.mutations[1].AssertionNotExist)
	require.True(t, call.mutations[2].AssertionNotExist)
	require.Empty(t, base.mutations)

	record, err := executor.Lookup(context.Background(), model.LookupRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "file",
	})
	require.NoError(t, err)
	require.Equal(t, model.InodeID(22), record.Inode)
}

func TestExecutorCreateSkipsSpeculativeAtomicMutateWithoutReadOrdering(t *testing.T) {
	base := newFakeRunner()
	runner := &fakeSpeculativeAtomicRunner{fakeRunner: base}
	executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{22}}))
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	})
	require.NoError(t, err)

	require.Empty(t, runner.atomicCalls)
	require.Len(t, base.mutations, 1)
	stats := executor.Stats()
	requireAtomicStatUint(t, stats, model.OperationCreate, "attempt_total", 0)
	requireAtomicStatUint(t, stats, model.OperationCreate, "success_total", 0)
	requireAtomicStatUint(t, stats, model.OperationCreate, "runner_unsupported_total", 1)
}

func TestExecutorCreateFallsBackWhenAtomicMutateNotHandled(t *testing.T) {
	base := newFakeRunner()
	runner := &fakeAtomicRunner{fakeRunner: base, handled: false}
	executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{22}}))
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	})
	require.NoError(t, err)

	require.Len(t, runner.atomicCalls, 1)
	stats := executor.Stats()
	requireStatUint(t, stats, "create_total", 1)
	requireAtomicStatUint(t, stats, model.OperationCreate, "attempt_total", 1)
	requireAtomicStatUint(t, stats, model.OperationCreate, "success_total", 0)
	requireAtomicStatUint(t, stats, model.OperationCreate, "fallback_total", 1)
	requireAtomicStatUint(t, stats, model.OperationCreate, "skip_total", 0)
	requireAtomicStatUint(t, stats, model.OperationCreate, "runner_unsupported_total", 0)
	require.Len(t, base.mutations, 1)
	require.Len(t, base.mutations[0], 3)
}

func TestExecutorCreateRecordsUnsupportedAtomicRunner(t *testing.T) {
	runner := newFakeRunner()
	executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{22}}))
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	})
	require.NoError(t, err)

	stats := executor.Stats()
	requireStatUint(t, stats, "create_total", 1)
	requireAtomicStatUint(t, stats, model.OperationCreate, "attempt_total", 0)
	requireAtomicStatUint(t, stats, model.OperationCreate, "success_total", 0)
	requireAtomicStatUint(t, stats, model.OperationCreate, "fallback_total", 0)
	requireAtomicStatUint(t, stats, model.OperationCreate, "skip_total", 0)
	requireAtomicStatUint(t, stats, model.OperationCreate, "runner_unsupported_total", 1)
	require.Len(t, runner.mutations, 1)
}

func TestExecutorCreateSkipsAtomicMutateWhenQuotaMutates(t *testing.T) {
	base := newFakeRunner()
	seedDirectory(t, base, "vol", 7)
	runner := &fakeAtomicRunner{fakeRunner: base, handled: true}
	quotaKey, err := layout.EncodeUsageKey(testMountIdentity, 0)
	require.NoError(t, err)
	quota := &fakeQuotaResolver{mutation: &backend.Mutation{Op: backend.MutationPut, Key: quotaKey, Value: []byte("usage")}}
	executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{22}}), WithQuotaResolver(quota))
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: 7,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile, Size: 4096},
	})
	require.NoError(t, err)

	// Quota reservation adds an extra key, so Create must use the full 2PC
	// path until AtomicMutate can prove all fsmeta and quota keys share one
	// atomic local apply group.
	stats := executor.Stats()
	requireStatUint(t, stats, "create_total", 1)
	requireAtomicStatUint(t, stats, model.OperationCreate, "attempt_total", 0)
	requireAtomicStatUint(t, stats, model.OperationCreate, "success_total", 0)
	requireAtomicStatUint(t, stats, model.OperationCreate, "fallback_total", 0)
	requireAtomicStatUint(t, stats, model.OperationCreate, "skip_total", 1)
	requireAtomicStatUint(t, stats, model.OperationCreate, "runner_unsupported_total", 0)
	require.Empty(t, runner.atomicCalls)
	require.Len(t, base.mutations, 1)
	require.Len(t, base.mutations[0], 4)
	require.Equal(t, quotaKey, base.mutations[0][3].Key)
}

func TestExecutorCreateRejectsExistingDentry(t *testing.T) {
	runner := newFakeRunner()
	executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{22, 23}}))
	require.NoError(t, err)

	req := model.CreateRequest{Mount: "vol", Parent: model.RootInode, Name: "file", Attrs: model.CreateAttrs{Type: model.InodeTypeFile}}
	_, err = executor.Create(context.Background(), req)
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), req)
	require.ErrorIs(t, err, model.ErrExists)
	require.Len(t, runner.mutations, 1)
	require.Equal(t, 2, runner.getCalls)
}

func TestExecutorCreateRequiresActiveMountWhenResolverConfigured(t *testing.T) {
	t.Run("active mount", func(t *testing.T) {
		runner := newFakeRunner()
		resolver := &fakeMountResolver{records: map[model.MountID]MountAdmission{
			"vol": {MountID: "vol", MountKeyID: 1, RootInode: model.RootInode, SchemaVersion: 1},
		}}
		executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{22}}), WithMountResolver(resolver))
		require.NoError(t, err)

		_, err = executor.Create(context.Background(), model.CreateRequest{
			Mount:  "vol",
			Parent: model.RootInode,
			Name:   "file",
			Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
		})
		require.NoError(t, err)
		require.Equal(t, 1, resolver.calls)
		require.Len(t, runner.mutations, 1)
	})

	t.Run("missing mount", func(t *testing.T) {
		runner := newFakeRunner()
		resolver := &fakeMountResolver{records: map[model.MountID]MountAdmission{}}
		executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{22}}), WithMountResolver(resolver))
		require.NoError(t, err)

		_, err = executor.Create(context.Background(), model.CreateRequest{
			Mount:  "missing",
			Parent: model.RootInode,
			Name:   "file",
			Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
		})
		require.ErrorIs(t, err, model.ErrMountNotRegistered)
		require.Equal(t, 1, resolver.calls)
		require.Empty(t, runner.mutations)
	})

	t.Run("retired mount", func(t *testing.T) {
		runner := newFakeRunner()
		resolver := &fakeMountResolver{records: map[model.MountID]MountAdmission{
			"vol": {MountID: "vol", MountKeyID: 1, RootInode: model.RootInode, SchemaVersion: 1, Retired: true},
		}}
		executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{22}}), WithMountResolver(resolver))
		require.NoError(t, err)

		_, err = executor.Create(context.Background(), model.CreateRequest{
			Mount:  "vol",
			Parent: model.RootInode,
			Name:   "file",
			Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
		})
		require.ErrorIs(t, err, model.ErrMountRetired)
		require.Equal(t, 1, resolver.calls)
		require.Empty(t, runner.mutations)
	})
}

func TestExecutorCreateReservesQuotaInsideMutation(t *testing.T) {
	runner := newFakeRunner()
	seedDirectory(t, runner, "vol", 7)
	quotaKey, err := layout.EncodeUsageKey(testMountIdentity, 0)
	require.NoError(t, err)
	quota := &fakeQuotaResolver{mutation: &backend.Mutation{Op: backend.MutationPut, Key: quotaKey, Value: []byte("usage")}}
	executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{22}}), WithQuotaResolver(quota))
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: 7,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile, Size: 4096},
	})
	require.NoError(t, err)
	require.Equal(t, [][]QuotaChange{{{Mount: "vol", MountKeyID: 1, Scope: 7, Bytes: 4096, Inodes: 1}}}, quota.changes)
	require.Len(t, runner.mutations, 1)
	require.Equal(t, quotaKey, runner.mutations[0][3].Key)
}

func TestExecutorCreateRejectsQuotaExceededBeforeMutation(t *testing.T) {
	runner := newFakeRunner()
	seedDirectory(t, runner, "vol", 7)
	quota := &fakeQuotaResolver{err: model.ErrQuotaExceeded}
	executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{22}}), WithQuotaResolver(quota))
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: 7,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile, Size: 4096},
	})
	require.ErrorIs(t, err, model.ErrQuotaExceeded)
	require.Empty(t, runner.mutations)
	require.Equal(t, [][]QuotaChange{{{Mount: "vol", MountKeyID: 1, Scope: 7, Bytes: 4096, Inodes: 1}}}, quota.changes)
}

func TestExecutorCreateTranslatesAlreadyExistsConflict(t *testing.T) {
	runner := newFakeRunner()
	runner.mutateErr = fakeTxnKeyError{errors: []*kvrpcpb.KeyError{{
		AlreadyExists: &kvrpcpb.KeyAlreadyExists{Key: []byte("dentry")},
	}}}
	executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{22}}))
	require.NoError(t, err)

	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	})
	require.ErrorIs(t, err, model.ErrExists)
	require.Equal(t, 1, runner.getCalls)
}

func BenchmarkExecutorCreateDefaultPath(b *testing.B) {
	executor, err := newTestExecutor(newFakeRunner(), WithInodeAllocator(&fakeInodeAllocator{next: 22}))
	if err != nil {
		b.Fatal(err)
	}
	benchmarkExecutorCreate(b, executor)
}

func BenchmarkExecutorCreateVisibleCommit(b *testing.B) {
	executor, err := newTestExecutor(
		newFakeRunner(),
		WithInodeAllocator(&fakeInodeAllocator{next: 22}),
		WithVisibleAuthorityAdmitter(ownedVisibleAdmitter{}),
		WithVisibleCommitter(noopVisibleCommitter{}),
	)
	if err != nil {
		b.Fatal(err)
	}
	benchmarkExecutorCreate(b, executor)
}

func BenchmarkExecutorCheckpointStormDefaultPath100(b *testing.B) {
	executor, err := newTestExecutor(newFakeRunner(), WithInodeAllocator(&fakeInodeAllocator{next: 22}))
	if err != nil {
		b.Fatal(err)
	}
	benchmarkExecutorCheckpointStorm(b, executor, nil, 100)
}

func BenchmarkExecutorCheckpointStormVisibleSegment100(b *testing.B) {
	runner := newFakeRunner()
	committer := newTestVisibleCommitter(b, runner)
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{next: 22}),
		WithVisibleAuthorityAdmitter(ownedVisibleAdmitter{}),
		WithVisibleCommitter(committer),
	)
	if err != nil {
		b.Fatal(err)
	}
	benchmarkExecutorCheckpointStorm(b, executor, committer, 100)
}

func BenchmarkExecutorCheckpointStormVisibleCommit100(b *testing.B) {
	runner := newFakeRunner()
	committer := newTestVisibleCommitter(b, runner)
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{next: 22}),
		WithVisibleAuthorityAdmitter(ownedVisibleAdmitter{}),
		WithVisibleCommitter(committer),
	)
	if err != nil {
		b.Fatal(err)
	}
	benchmarkExecutorCheckpointStorm(b, executor, committer, 100)
}
