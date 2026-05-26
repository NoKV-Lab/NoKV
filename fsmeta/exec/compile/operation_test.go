// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package compile

import (
	"crypto/sha256"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/feichai0017/NoKV/fsmeta/proof"
	"github.com/stretchr/testify/require"
)

func TestDerivedOperationRequiresRuntimeMaterialization(t *testing.T) {
	delta, err := testUpdateInodeDelta(t, model.UpdateInodeRequest{
		Mount:   "vol",
		Parent:  3,
		Inode:   44,
		Name:    "file",
		SetMode: true,
		Mode:    0o600,
	}, testMount)
	require.NoError(t, err)

	op := testCompileAOT(t, delta)
	require.True(t, op.Authority.Required)
	require.Equal(t, FenceActiveAuthority, op.Authority.Fence)
	require.False(t, op.Placement.CanSegment)
	require.True(t, op.Placement.RequiresMaterialize)
	require.Equal(t, DurabilityVisibleOnly, op.Durability)
	require.Len(t, op.Predicates, 2)
	require.True(t, op.Predicates[0].NeedValue)
	require.True(t, op.Predicates[1].NeedValue)
	require.Len(t, op.Effects, 1)
	require.Equal(t, DerivationRuntimeValue, op.Effects[0].Derivation)
	require.False(t, op.Effects[0].Concrete)
}

func TestMaterializedOpRecompilesConcreteEffectsAndCarriesProofs(t *testing.T) {
	delta, err := testUpdateInodeDelta(t, model.UpdateInodeRequest{
		Mount:   "vol",
		Parent:  3,
		Inode:   44,
		Name:    "file",
		SetMode: true,
		Mode:    0o600,
	}, testMount)
	require.NoError(t, err)
	compiled := testCompileAOT(t, delta)
	require.True(t, compiled.Placement.RequiresMaterialize)

	key := mustInodeKey(t, 44)
	value := []byte("new-inode")
	predicateProof := proof.NewPredicateProof(key, []byte("old-inode"), true, 9, proof.ReadSourceBase, proof.ProofFrontier{})
	materialized := testMaterializeAOTWithEffects(t, compiled, []WriteEffect{{Kind: EffectPut, Key: key, Value: value}}, []proof.PredicateProof{predicateProof})

	require.True(t, materialized.Placement.CanSegment)
	require.False(t, materialized.Placement.RequiresMaterialize)
	require.Equal(t, compiled.IntentDigest, materialized.IntentDigest)
	require.NotEqual(t, compiled.DescriptorDigest, materialized.ReplayDigest)
	require.Equal(t, materialized.DescriptorDigest, materialized.ReplayDigest)
	require.Len(t, materialized.Effects, 1)
	require.True(t, materialized.Effects[0].Concrete)
	require.Equal(t, DerivationNone, materialized.Effects[0].Derivation)
	require.Equal(t, sha256.Sum256(value), materialized.Effects[0].ValueHash)
	require.Len(t, materialized.PredicateProofs, 1)
	require.Equal(t, predicateProof.Digest, materialized.PredicateProofs[0].Digest)

	predicateProof.Value[0] ^= 0xff
	value[0] ^= 0xff
	require.NotEqual(t, predicateProof.Value, materialized.PredicateProofs[0].Value)
	require.NotEqual(t, value, materialized.Effects[0].Value)
}

func TestCompiledDigestSemanticsAreStableAcrossMaterialization(t *testing.T) {
	createDelta, err := testCreateDelta(t, model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	}, testMount, 44)
	require.NoError(t, err)
	create := testCompileAOT(t, createDelta)
	require.Equal(t, create.DescriptorDigest, create.IntentDigest)
	require.Equal(t, create.DescriptorDigest, create.ReplayDigest)

	updateDelta, err := testUpdateInodeDelta(t, model.UpdateInodeRequest{
		Mount:   "vol",
		Parent:  3,
		Inode:   44,
		Name:    "file",
		SetMode: true,
		Mode:    0o600,
	}, testMount)
	require.NoError(t, err)
	compiled := testCompileAOT(t, updateDelta)
	key := mustInodeKey(t, 44)
	predicateProof := testPredicateProof(key, []byte("old-inode"), true, 9, proof.ReadSourceBase)
	materialized := testMaterializeAOTWithEffects(t, compiled, []WriteEffect{{Kind: EffectPut, Key: key, Value: []byte("new-inode")}}, []proof.PredicateProof{predicateProof})
	require.Equal(t, compiled.IntentDigest, materialized.IntentDigest)
	require.NotEqual(t, compiled.DescriptorDigest, materialized.DescriptorDigest)
	require.Equal(t, materialized.DescriptorDigest, materialized.ReplayDigest)
}

func TestSegmentMergeDecisionUsesCompilerPlans(t *testing.T) {
	left, err := testCreateDelta(t, model.CreateRequest{
		Mount:  "vol",
		Parent: 8,
		Name:   "a",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	}, testMount, testParentInSameBucket(t, 8))
	require.NoError(t, err)
	right, err := testCreateDelta(t, model.CreateRequest{
		Mount:  "vol",
		Parent: 8,
		Name:   "b",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	}, testMount, testParentInDifferentBucketAfter(t, 8, 128))
	require.NoError(t, err)

	decision := CanAppendSegment(testCompileAOT(t, left), testCompileAOT(t, right), SegmentBudget{
		MaxOperations:   2,
		MaxMutations:    6,
		MaxPayloadBytes: 1 << 20,
	})
	require.Equal(t, SegmentDecisionAppend, decision.Kind)

	decision = CanAppendSegment(testCompileAOT(t, left), testCompileAOT(t, right), SegmentBudget{MaxMutations: 3})
	require.Equal(t, SegmentDecisionCut, decision.Kind)

	snapshot, err := testSnapshotSubtreeDelta(t, model.SnapshotSubtreeRequest{Mount: "vol", RootInode: 8}, testMount)
	require.NoError(t, err)
	decision = CanAppendSegment(testCompileAOT(t, left), testCompileAOT(t, snapshot), SegmentBudget{})
	require.Equal(t, SegmentDecisionFlushBeforeAndAfter, decision.Kind)
	require.Equal(t, SlowReasonDurabilityBarrier, decision.Reason)

	closeLeft := testMaterializedCloseWriteSession(t, "writer-left", 44)
	closeRight := testMaterializedCloseWriteSession(t, "writer-right", 45)
	decision = CanAppendSegment(closeLeft.CompiledOp, closeRight.CompiledOp, SegmentBudget{MaxMutations: 4})
	require.Equal(t, SegmentDecisionAppend, decision.Kind)
	closeInstallPlan, ok := SegmentPlanForInstall(closeLeft.Segment, true)
	require.True(t, ok)
	require.Equal(t, SegmentInstallSingleBucket, closeInstallPlan.Install)
	require.Equal(t, closeLeft.Segment.MaterializeMergeKey, closeInstallPlan.MergeKey)

	decision = CanAppendSegment(testCompileAOT(t, left), closeLeft.CompiledOp, SegmentBudget{})
	require.Equal(t, SegmentDecisionFlushBeforeAndAfter, decision.Kind)
	require.Equal(t, SlowReasonDurabilityBarrier, decision.Reason)
}

func TestSegmentPlanAPIPreservesCompilerBoundary(t *testing.T) {
	left, err := testCreateDelta(t, model.CreateRequest{
		Mount:  "vol",
		Parent: 8,
		Name:   "a",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	}, testMount, testParentInSameBucket(t, 8))
	require.NoError(t, err)
	right, err := testCreateDelta(t, model.CreateRequest{
		Mount:  "vol",
		Parent: 8,
		Name:   "b",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	}, testMount, testParentInDifferentBucketAfter(t, 8, 128))
	require.NoError(t, err)

	leftPlan := testCompileAOT(t, left).Segment
	rightPlan := testCompileAOT(t, right).Segment
	decision := CanAppendSegmentPlans(leftPlan, rightPlan, DurabilityVisibleOnly, SegmentBudget{MaxMutations: 6})
	require.Equal(t, SegmentDecisionAppend, decision.Kind)

	merged := MergeSegmentPlans(leftPlan, rightPlan)
	require.Equal(t, uint32(2), merged.OperationCount)
	require.Equal(t, uint32(6), merged.MutationCount)
	require.Greater(t, merged.EstimatedPayloadBytes, leftPlan.EstimatedPayloadBytes)

	installPlan, ok := SegmentPlanForInstall(leftPlan, false)
	require.True(t, ok)
	require.Equal(t, SegmentInstallCatalog, installPlan.Install)

	materializePlan, ok := SegmentPlanForInstall(leftPlan, true)
	require.True(t, ok)
	require.Equal(t, SegmentInstallSingleBucket, materializePlan.Install)
	require.True(t, materializePlan.MergeKey.HasPrimaryBucket)
}

func TestSegmentMergeKeyDistinguishesBucketZeroFromNoPrimaryBucket(t *testing.T) {
	singleBucketZero := SegmentMergeKey{
		MountKeyID:       testMount.MountKeyID,
		HasPrimaryBucket: true,
		PrimaryBucket:    0,
		Install:          SegmentInstallSingleBucket,
		Durability:       DurabilityVisibleOnly,
		FormatVersion:    segmentFormatVersion,
	}
	multiBucket := SegmentMergeKey{
		MountKeyID:    testMount.MountKeyID,
		Install:       SegmentInstallSingleBucket,
		Durability:    DurabilityVisibleOnly,
		FormatVersion: segmentFormatVersion,
	}
	require.NotEqual(t, singleBucketZero, multiBucket)
}

func testMaterializedCloseWriteSession(t *testing.T, session model.SessionID, inode model.InodeID) MaterializedOp {
	t.Helper()
	req := model.CloseWriteSessionRequest{
		Mount:   "vol",
		Inode:   inode,
		Session: session,
	}
	program, err := CompileCloseWriteSessionProgram(req, testMount)
	require.NoError(t, err)
	sessionValue, err := fsmeta.EncodeSessionValue(model.SessionRecord{
		Session:       session,
		Inode:         inode,
		ExpiresUnixNs: 100,
	})
	require.NoError(t, err)
	ownerValue, err := fsmeta.EncodeSessionValue(model.SessionRecord{
		Session:       session,
		Inode:         inode,
		ExpiresUnixNs: 100,
	})
	require.NoError(t, err)
	sessionKey := program.Compiled.Delta.ReadPredicates[0].Key
	ownerKey := program.Compiled.Delta.ReadPredicates[1].Key
	proofs := []proof.PredicateProof{
		testPredicateProof(sessionKey, sessionValue, true, 12, proof.ReadSourceBase),
		testPredicateProof(ownerKey, ownerValue, true, 12, proof.ReadSourceBase),
	}
	materialized, err := MaterializeCloseWriteSession(program, CloseWriteSessionValues{
		DeleteOwner:     true,
		PredicateProofs: proofs,
	})
	require.NoError(t, err)
	return materialized
}
