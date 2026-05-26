// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"context"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/feichai0017/NoKV/fsmeta/proof"
	"github.com/stretchr/testify/require"
)

func TestExecutorVisiblePredicateReadsOverlayBeforeTimestamp(t *testing.T) {
	runner := newFakeRunner()
	key := dentryKeyForTest(t, "vol", model.RootInode, "file")
	value := dentryValueForTest(t, model.RootInode, "file", 21, model.InodeTypeFile)
	committer := scanOverlayCommitter{
		values: overlayMapForTest(overlayValueForTest(key, value)),
	}
	executor, err := newTestExecutor(runner, WithVisibleCommitter(committer))
	require.NoError(t, err)

	_, ok, err := executor.visiblePredicatesHold(context.Background(), compile.MaterializedOp{CompiledOp: compile.CompiledOp{Delta: compile.SemanticDelta{
		ReadPredicates: []compile.Predicate{{
			Kind:             compile.PredicateObservedValue,
			Key:              key,
			ExpectedValue:    value,
			HasExpectedValue: true,
		}},
	}}}, VisibleAdmissionContext{ProofFrontier: proof.ProofFrontier{EpochID: 1, Sequence: 1}})
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(1), runner.nextTS, "overlay predicate admission must not reserve a read timestamp")
}

func TestExecutorVisibleObservedPredicateRechecksExpectedValue(t *testing.T) {
	runner := newFakeRunner()
	key := dentryKeyForTest(t, "vol", model.RootInode, "file")
	oldValue := dentryValueForTest(t, model.RootInode, "file", 21, model.InodeTypeFile)
	newValue := dentryValueForTest(t, model.RootInode, "file", 22, model.InodeTypeFile)
	runner.data[string(key)] = newValue
	committer := newTestVisibleCommitter(t, runner)
	committer.RememberKey(key, true)
	executor, err := newTestExecutor(runner, WithVisibleCommitter(committer))
	require.NoError(t, err)

	_, ok, err := executor.visiblePredicatesHold(context.Background(), compile.MaterializedOp{CompiledOp: compile.CompiledOp{Delta: compile.SemanticDelta{
		ReadPredicates: []compile.Predicate{{
			Kind:             compile.PredicateObservedValue,
			Key:              key,
			ExpectedValue:    oldValue,
			HasExpectedValue: true,
			RuntimeChecked:   true,
		}},
	}}}, VisibleAdmissionContext{ProofFrontier: proof.ProofFrontier{EpochID: 1, Sequence: 1}})
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, 1, runner.getCalls, "known-present facts cannot replace byte-level observed-value recheck")
}

func TestExecutorVisibleNotExistsKnownUsesCurrentDirectoryEmptiness(t *testing.T) {
	runner := newFakeRunner()
	committer := newTestVisibleCommitter(t, runner)
	committer.RememberEmptyDirectory(testMountIdentity, model.RootInode)
	committer.ForgetEmptyDirectory(testMountIdentity, model.RootInode)
	executor, err := newTestExecutor(runner, WithVisibleCommitter(committer))
	require.NoError(t, err)
	key := dentryKeyForTest(t, "vol", model.RootInode, "eta")

	knownAbsent := executor.visibleNotExistsKnown(compile.AuthorityScope{
		Mount:      testMountIdentity.MountID,
		MountKeyID: testMountIdentity.MountKeyID,
		Parents:    []model.InodeID{model.RootInode},
	}, key, committer)

	require.False(t, knownAbsent, "base-empty directory facts are not enough once visible writes made the current directory non-empty")
}

func TestExecutorVisiblePredicateRejectsCorruptProof(t *testing.T) {
	runner := newFakeRunner()
	key := dentryKeyForTest(t, "vol", model.RootInode, "file")
	value := dentryValueForTest(t, model.RootInode, "file", 21, model.InodeTypeFile)
	runner.data[string(key)] = value
	executor, err := newTestExecutor(runner, WithVisibleCommitter(newTestVisibleCommitter(t, runner)))
	require.NoError(t, err)

	predicateProof := proof.NewPredicateProof(key, value, true, 7, proof.ReadSourceBase, proof.ProofFrontier{})
	predicateProof.Digest[0] ^= 0xff
	_, ok, err := executor.visiblePredicatesHold(context.Background(), compile.MaterializedOp{
		CompiledOp: compile.CompiledOp{Delta: compile.SemanticDelta{
			ReadPredicates: []compile.Predicate{{
				Kind:             compile.PredicateObservedValue,
				Key:              key,
				ExpectedValue:    value,
				HasExpectedValue: true,
			}},
		}},
		PredicateProofs: []proof.PredicateProof{predicateProof},
	}, VisibleAdmissionContext{ProofFrontier: proof.ProofFrontier{EpochID: 1, Sequence: 1}})

	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, 0, runner.getCalls)
}

func TestExecutorMergeVisibleOverlayScanUsesOrderedMerge(t *testing.T) {
	executor := &Executor{visibleCommitter: scanOverlayCommitter{rows: []VisibleOverlayKV{
		{Key: []byte("k/b"), Delete: true},
		{Key: []byte("k/c"), Value: []byte("overlay-c")},
		{Key: []byte("k/e"), Value: []byte("overlay-e")},
	}}}
	base := []KV{
		{Key: []byte("k/a"), Value: []byte("base-a")},
		{Key: []byte("k/b"), Value: []byte("base-b")},
		{Key: []byte("k/d"), Value: []byte("base-d")},
	}

	merged := executor.mergeVisibleOverlayScan(base, []byte("k/"), 4)

	require.Equal(t, []KV{
		{Key: []byte("k/a"), Value: []byte("base-a")},
		{Key: []byte("k/c"), Value: []byte("overlay-c")},
		{Key: []byte("k/d"), Value: []byte("base-d")},
		{Key: []byte("k/e"), Value: []byte("overlay-e")},
	}, merged)
}

func TestExecutorVisibleOperationIDIsExecutorScoped(t *testing.T) {
	first, err := New(newFakeRunner())
	require.NoError(t, err)
	second, err := New(newFakeRunner())
	require.NoError(t, err)

	firstID := first.nextVisibleOperationID(model.OperationCreate)
	secondID := second.nextVisibleOperationID(model.OperationCreate)

	require.Equal(t, uint64(1), firstID.Seq)
	require.Equal(t, uint64(1), secondID.Seq)
	require.Contains(t, firstID.ClientID, "fsmeta-exec/create")
	require.Contains(t, secondID.ClientID, "fsmeta-exec/create")
	require.NotEqual(t, firstID.ClientID, secondID.ClientID)
}

func BenchmarkExecutorAdmitVisibleAuthorityOwned(b *testing.B) {
	executor, err := New(newFakeRunner(), WithVisibleAuthorityAdmitter(ownedVisibleAdmitter{}))
	if err != nil {
		b.Fatal(err)
	}
	delta := compile.SemanticDelta{
		Eligibility: compile.EligibilityVisibleCommit,
		Authority: compile.AuthorityScope{
			Mount:      "vol",
			MountKeyID: 1,
			Buckets:    []fsmeta.AffinityBucket{fsmeta.BucketForInodeID(model.RootInode)},
			Parents:    []model.InodeID{model.RootInode},
			Inodes:     []model.InodeID{22},
		},
	}
	ctx := context.Background()

	b.ReportAllocs()
	for b.Loop() {
		if err := executor.admitVisibleAuthority(ctx, delta); err != nil {
			b.Fatal(err)
		}
	}
}
