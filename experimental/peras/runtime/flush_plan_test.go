// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"testing"

	fsperas "github.com/feichai0017/NoKV/experimental/peras/exec"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/stretchr/testify/require"
)

func TestSplitReplayPlanByCompilerBudgetRequiresStoredSegmentPlan(t *testing.T) {
	key, err := layout.EncodeInodeKey(model.MountIdentity{MountID: "vol", MountKeyID: 1}, 8)
	require.NoError(t, err)

	_, err = splitReplayPlanByCompilerBudget(fsperas.ReplayPlan{
		EpochID: 1,
		Operations: []fsperas.ReplayOperation{{
			OpID:      fsperas.OperationID{ClientID: "client", Seq: 1},
			Kind:      model.OperationUpdateInode,
			Mutations: []fsperas.ReplayMutation{{Key: key, Value: []byte("inode")}},
		}},
	}, false, compile.SegmentBudget{MaxMutations: 16}, 0)
	require.ErrorIs(t, err, fsperas.ErrInvalidPerasSegment)
}

func TestSplitReplayPlanByCompilerBudgetUsesStoredSegmentPlan(t *testing.T) {
	mount := model.MountIdentity{MountID: "vol", MountKeyID: 1}
	leftKey := fsmetaKeyForBucket(t, mount, 1)
	rightKey := fsmetaKeyForBucket(t, mount, 2)
	plan := compile.SegmentPlan{
		MergeKey: compile.SegmentMergeKey{
			MountKeyID:    mount.MountKeyID,
			Install:       compile.SegmentInstallCatalog,
			Durability:    compile.DurabilityVisibleOnly,
			FormatVersion: 1,
		},
		Install:               compile.SegmentInstallCatalog,
		CanAppend:             true,
		EstimatedPayloadBytes: 16,
		OperationCount:        1,
		MutationCount:         1,
	}

	out, err := splitReplayPlanByCompilerBudget(fsperas.ReplayPlan{
		EpochID: 1,
		Operations: []fsperas.ReplayOperation{
			replayOperationWithSegmentPlan("client", 1, leftKey, plan),
			replayOperationWithSegmentPlan("client", 2, rightKey, plan),
		},
	}, false, compile.SegmentBudget{MaxMutations: 16, MaxPayloadBytes: 64}, 0)
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Len(t, out[0].Operations, 2)
}

func TestSplitReplayPlanByCompilerBudgetCutsByCatalogRouteBudget(t *testing.T) {
	mount := model.MountIdentity{MountID: "vol", MountKeyID: 1}
	leftKey := fsmetaKeyForBucket(t, mount, 1)
	rightKey := fsmetaKeyForBucket(t, mount, 2)
	plan := compile.SegmentPlan{
		MergeKey: compile.SegmentMergeKey{
			MountKeyID:    mount.MountKeyID,
			Install:       compile.SegmentInstallCatalog,
			Durability:    compile.DurabilityVisibleOnly,
			FormatVersion: 1,
		},
		Install:               compile.SegmentInstallCatalog,
		CanAppend:             true,
		EstimatedPayloadBytes: 16,
		OperationCount:        1,
		MutationCount:         1,
	}

	out, err := splitReplayPlanByCompilerBudget(fsperas.ReplayPlan{
		EpochID: 1,
		Operations: []fsperas.ReplayOperation{
			replayOperationWithSegmentPlan("client", 1, leftKey, plan),
			replayOperationWithSegmentPlan("client", 2, rightKey, plan),
		},
	}, false, compile.SegmentBudget{MaxMutations: 16, MaxPayloadBytes: 64}, 1)
	require.NoError(t, err)
	require.Len(t, out, 2)
	require.Len(t, out[0].Operations, 1)
	require.Len(t, out[1].Operations, 1)
}

func TestSplitReplayPlanByCompilerBudgetCutsByPayload(t *testing.T) {
	mount := model.MountIdentity{MountID: "vol", MountKeyID: 1}
	key := fsmetaKeyForBucket(t, mount, 1)
	plan := compile.SegmentPlan{
		MergeKey: compile.SegmentMergeKey{
			MountKeyID:    mount.MountKeyID,
			Install:       compile.SegmentInstallCatalog,
			Durability:    compile.DurabilityVisibleOnly,
			FormatVersion: 1,
		},
		Install:               compile.SegmentInstallCatalog,
		CanAppend:             true,
		EstimatedPayloadBytes: 40,
		OperationCount:        1,
		MutationCount:         1,
	}

	out, err := splitReplayPlanByCompilerBudget(fsperas.ReplayPlan{
		EpochID: 1,
		Operations: []fsperas.ReplayOperation{
			replayOperationWithSegmentPlan("client", 1, key, plan),
			replayOperationWithSegmentPlan("client", 2, key, plan),
		},
	}, false, compile.SegmentBudget{MaxPayloadBytes: 64}, 0)
	require.NoError(t, err)
	require.Len(t, out, 2)
	require.Len(t, out[0].Operations, 1)
	require.Len(t, out[1].Operations, 1)
}

func TestSplitReplayPlanByCompilerBudgetRejectsNonMaterializablePlan(t *testing.T) {
	mount := model.MountIdentity{MountID: "vol", MountKeyID: 1}
	key := fsmetaKeyForBucket(t, mount, 1)
	plan := compile.SegmentPlan{
		MergeKey: compile.SegmentMergeKey{
			MountKeyID:    mount.MountKeyID,
			Install:       compile.SegmentInstallCatalog,
			Durability:    compile.DurabilityVisibleOnly,
			FormatVersion: 1,
		},
		Install:               compile.SegmentInstallCatalog,
		CanAppend:             true,
		EstimatedPayloadBytes: 16,
		OperationCount:        1,
		MutationCount:         1,
	}

	_, err := splitReplayPlanByCompilerBudget(fsperas.ReplayPlan{
		EpochID:    1,
		Operations: []fsperas.ReplayOperation{replayOperationWithSegmentPlan("client", 1, key, plan)},
	}, true, compile.SegmentBudget{MaxMutations: 16}, 0)
	require.ErrorIs(t, err, fsperas.ErrInvalidPerasSegment)
}

func replayOperationWithSegmentPlan(client string, seq uint64, key []byte, segment compile.SegmentPlan) fsperas.ReplayOperation {
	return fsperas.ReplayOperation{
		OpID:       fsperas.OperationID{ClientID: client, Seq: seq},
		Kind:       model.OperationUpdateInode,
		Segment:    segment,
		Durability: compile.DurabilityVisibleOnly,
		Atomicity: compile.AtomicityGroup{
			Members:  []compile.MutationID{0},
			Recovery: compile.RecoveryReplayAllOrNothing,
		},
		Mutations: []fsperas.ReplayMutation{{Key: key, Value: []byte("value")}},
	}
}

func fsmetaKeyForBucket(t *testing.T, mount model.MountIdentity, bucket layout.AffinityBucket) []byte {
	t.Helper()
	for inode := model.InodeID(2); inode < 100_000; inode++ {
		if layout.BucketForInodeID(inode) != bucket {
			continue
		}
		key, err := layout.EncodeInodeKey(mount, inode)
		require.NoError(t, err)
		return key
	}
	t.Fatalf("no inode found for bucket %d", bucket)
	return nil
}
