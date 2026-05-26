// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package compile

import (
	"strings"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile/specdsl"
	compilespecs "github.com/feichai0017/NoKV/fsmeta/exec/compile/specs"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/stretchr/testify/require"
)

func TestGeneratedProgramsMatchSemanticSpecs(t *testing.T) {
	renameToParent := testParentInSameBucket(t, model.RootInode)
	crossBucketParent := testParentInDifferentBucket(t, model.RootInode)
	for _, tc := range []struct {
		spec    specdsl.OpSpec
		compile func(*testing.T) CompiledOp
	}{
		{spec: compilespecs.Create, compile: func(t *testing.T) CompiledOp {
			program, err := CompileCreateProgram(model.CreateRequest{Mount: "vol", Parent: model.RootInode, Name: "file", Attrs: model.CreateAttrs{Type: model.InodeTypeFile}}, testMount, 44)
			require.NoError(t, err)
			return program.Compiled
		}},
		{spec: compilespecs.UpdateInode, compile: func(t *testing.T) CompiledOp {
			program, err := CompileUpdateInodeProgram(model.UpdateInodeRequest{Mount: "vol", Parent: model.RootInode, Inode: 44, Name: "file", SetMode: true, Mode: 0o600}, testMount)
			require.NoError(t, err)
			return program.Compiled
		}},
		{spec: compilespecs.Lookup, compile: func(t *testing.T) CompiledOp {
			program, err := CompileLookupProgram(model.LookupRequest{Mount: "vol", Parent: model.RootInode, Name: "file"}, testMount)
			require.NoError(t, err)
			return program.Compiled
		}},
		{spec: compilespecs.ReadDir, compile: func(t *testing.T) CompiledOp {
			program, err := CompileReadDirProgram(model.ReadDirRequest{Mount: "vol", Parent: model.RootInode, Limit: 32}, testMount)
			require.NoError(t, err)
			return program.Compiled
		}},
		{spec: compilespecs.SnapshotSubtree, compile: func(t *testing.T) CompiledOp {
			program, err := CompileSnapshotSubtreeProgram(model.SnapshotSubtreeRequest{Mount: "vol", RootInode: model.RootInode}, testMount)
			require.NoError(t, err)
			return program.Compiled
		}},
		{spec: compilespecs.Rename, compile: func(t *testing.T) CompiledOp {
			program, err := CompileRenameProgram(model.RenameRequest{Mount: "vol", FromParent: model.RootInode, FromName: "old", ToParent: renameToParent, ToName: "new"}, testMount)
			require.NoError(t, err)
			return program.Compiled
		}},
		{spec: compilespecs.RenameReplace, compile: func(t *testing.T) CompiledOp {
			program, err := CompileRenameReplaceProgram(model.RenameReplaceRequest{Mount: "vol", FromParent: model.RootInode, FromName: "old", ToParent: renameToParent, ToName: "new"}, testMount)
			require.NoError(t, err)
			return program.Compiled
		}},
		{spec: compilespecs.RenameSubtree, compile: func(t *testing.T) CompiledOp {
			program, err := CompileRenameSubtreeProgram(model.RenameSubtreeRequest{Mount: "vol", FromParent: model.RootInode, FromName: "old", ToParent: crossBucketParent, ToName: "new"}, testMount)
			require.NoError(t, err)
			return program.Compiled
		}},
		{spec: compilespecs.Link, compile: func(t *testing.T) CompiledOp {
			program, err := CompileLinkProgram(model.LinkRequest{Mount: "vol", FromParent: model.RootInode, FromName: "old", ToParent: renameToParent, ToName: "new"}, testMount)
			require.NoError(t, err)
			return program.Compiled
		}},
		{spec: compilespecs.Unlink, compile: func(t *testing.T) CompiledOp {
			program, err := CompileUnlinkProgram(model.UnlinkRequest{Mount: "vol", Parent: model.RootInode, Name: "old"}, testMount)
			require.NoError(t, err)
			return program.Compiled
		}},
		{spec: compilespecs.Remove, compile: func(t *testing.T) CompiledOp {
			program, err := CompileRemoveProgram(model.RemoveRequest{Mount: "vol", Parent: model.RootInode, Name: "old"}, testMount)
			require.NoError(t, err)
			return program.Compiled
		}},
		{spec: compilespecs.RemoveDirectory, compile: func(t *testing.T) CompiledOp {
			program, err := CompileRemoveDirectoryProgram(model.RemoveDirectoryRequest{Mount: "vol", Parent: model.RootInode, Name: "old-dir"}, testMount)
			require.NoError(t, err)
			return program.Compiled
		}},
		{spec: compilespecs.OpenWriteSession, compile: func(t *testing.T) CompiledOp {
			program, err := CompileOpenWriteSessionProgram(model.OpenWriteSessionRequest{Mount: "vol", Inode: 44, Session: "writer-1", TTL: time.Second}, testMount)
			require.NoError(t, err)
			return program.Compiled
		}},
		{spec: compilespecs.HeartbeatWriteSession, compile: func(t *testing.T) CompiledOp {
			program, err := CompileHeartbeatWriteSessionProgram(model.HeartbeatWriteSessionRequest{Mount: "vol", Inode: 44, Session: "writer-1", TTL: time.Second}, testMount)
			require.NoError(t, err)
			return program.Compiled
		}},
		{spec: compilespecs.CloseWriteSession, compile: func(t *testing.T) CompiledOp {
			program, err := CompileCloseWriteSessionProgram(model.CloseWriteSessionRequest{Mount: "vol", Inode: 44, Session: "writer-1"}, testMount)
			require.NoError(t, err)
			return program.Compiled
		}},
		{spec: compilespecs.ExpireWriteSessions, compile: func(t *testing.T) CompiledOp {
			program, err := CompileExpireWriteSessionsProgram(model.ExpireWriteSessionsRequest{Mount: "vol", Limit: 16}, testMount)
			require.NoError(t, err)
			return program.Compiled
		}},
	} {
		t.Run(tc.spec.Name, func(t *testing.T) {
			requireProgramMatchesSemanticSpec(t, tc.compile(t), tc.spec)
		})
	}
}

func requireProgramMatchesSemanticSpec(t *testing.T, op CompiledOp, spec specdsl.OpSpec) {
	t.Helper()
	require.Equal(t, spec.OperationKind, operationKindSpecName(op.Delta.Kind), spec.Name)
	require.Equal(t, spec.Durability, durabilitySpecName(op.Durability), spec.Name)
	require.Equal(t, spec.Eligibility, eligibilitySpecName(op.Delta.Eligibility), spec.Name)
	if spec.SlowReason != "" {
		require.Equal(t, spec.SlowReason, slowReasonSpecName(op.Delta.SlowReason), spec.Name)
	}
	require.Equal(t, spec.DurabilityBarrier, op.Delta.DurabilityBarrier, spec.Name)
	require.Equal(t, spec.WatchAtSeal, op.Delta.WatchAtSeal, spec.Name)
	require.Len(t, op.Delta.Authority.Parents, len(spec.Authority.Parents), spec.Name)
	require.Len(t, op.Delta.Authority.Inodes, len(spec.Authority.Inodes), spec.Name)
	require.Equal(t, spec.Authority.Broad, op.Delta.Authority.Broad, spec.Name)
	require.Equal(t, spec.Authority.AllowOpaqueKeys, op.Delta.Authority.AllowOpaqueKeys, spec.Name)

	requirePredicatesMatchSemanticSpec(t, op, spec)

	require.Len(t, op.Delta.WriteEffects, len(spec.Effects), spec.Name)
	require.Len(t, op.Effects, len(spec.Effects), spec.Name)
	for i, effect := range spec.Effects {
		require.Equal(t, effect.Kind, effectKindSpecName(op.Delta.WriteEffects[i].Kind), spec.Name)
		require.Equal(t, effect.Kind, effectKindSpecName(op.Effects[i].Kind), spec.Name)
		require.True(t, semanticKeyBindingMatches(op.Delta, op.Delta.WriteEffects[i].Key, effect.Key), spec.Name)
		require.True(t, semanticKeyBindingMatches(op.Delta, op.Effects[i].Key, effect.Key), spec.Name)
	}

	require.Len(t, op.Delta.RuntimeGuards, len(spec.Guards), spec.Name)
	require.Len(t, op.Guards, len(spec.Guards), spec.Name)
	for i, guard := range spec.Guards {
		require.Equal(t, guard.Guard, guardSpecName(op.Delta.RuntimeGuards[i]), spec.Name)
		require.Equal(t, guard.Guard, guardSpecName(op.Guards[i].Guard), spec.Name)
	}
}

func requirePredicatesMatchSemanticSpec(t *testing.T, op CompiledOp, spec specdsl.OpSpec) {
	t.Helper()
	if len(spec.Predicates) == 1 && spec.Predicates[0].Repeatable {
		require.NotEmpty(t, op.Delta.ReadPredicates, spec.Name)
		require.Len(t, op.Predicates, len(op.Delta.ReadPredicates), spec.Name)
		for i := range op.Delta.ReadPredicates {
			require.Equal(t, spec.Predicates[0].Kind, predicateKindSpecName(op.Delta.ReadPredicates[i].Kind), spec.Name)
			require.Equal(t, spec.Predicates[0].Kind, predicateKindSpecName(op.Predicates[i].Kind), spec.Name)
			require.True(t, semanticIndexedKeyBindingMatches(op.Delta, op.Delta.ReadPredicates[i].Key, repeatableSpecBindingFamily(spec.Predicates[0].Key), i), spec.Name)
			require.True(t, semanticIndexedKeyBindingMatches(op.Delta, op.Predicates[i].Key, repeatableSpecBindingFamily(spec.Predicates[0].Key), i), spec.Name)
		}
		return
	}
	require.Len(t, op.Delta.ReadPredicates, len(spec.Predicates), spec.Name)
	require.Len(t, op.Predicates, len(spec.Predicates), spec.Name)
	for i, predicate := range spec.Predicates {
		require.Equal(t, predicate.Kind, predicateKindSpecName(op.Delta.ReadPredicates[i].Kind), spec.Name)
		require.Equal(t, predicate.Kind, predicateKindSpecName(op.Predicates[i].Kind), spec.Name)
		require.True(t, semanticKeyBindingMatches(op.Delta, op.Delta.ReadPredicates[i].Key, predicate.Key), spec.Name)
		require.True(t, semanticKeyBindingMatches(op.Delta, op.Predicates[i].Key, predicate.Key), spec.Name)
	}
}

func repeatableSpecBindingFamily(binding string) string {
	if before, _, ok := strings.Cut(binding, "["); ok {
		return before
	}
	return binding
}

func operationKindSpecName(kind model.OperationKind) string {
	switch kind {
	case model.OperationCreate:
		return "model.OperationCreate"
	case model.OperationUpdateInode:
		return "model.OperationUpdateInode"
	case model.OperationLookup:
		return "model.OperationLookup"
	case model.OperationReadDir:
		return "model.OperationReadDir"
	case model.OperationSnapshotSubtree:
		return "model.OperationSnapshotSubtree"
	case model.OperationRename:
		return "model.OperationRename"
	case model.OperationRenameReplace:
		return "model.OperationRenameReplace"
	case model.OperationRenameSubtree:
		return "model.OperationRenameSubtree"
	case model.OperationLink:
		return "model.OperationLink"
	case model.OperationUnlink:
		return "model.OperationUnlink"
	case model.OperationRemove:
		return "model.OperationRemove"
	case model.OperationRemoveDirectory:
		return "model.OperationRemoveDirectory"
	case model.OperationOpenWriteSession:
		return "model.OperationOpenWriteSession"
	case model.OperationHeartbeatSession:
		return "model.OperationHeartbeatSession"
	case model.OperationCloseSession:
		return "model.OperationCloseSession"
	case model.OperationExpireSessions:
		return "model.OperationExpireSessions"
	default:
		return ""
	}
}

func durabilitySpecName(durability DurabilityClass) string {
	switch durability {
	case DurabilityVisibleOnly:
		return "DurabilityVisibleOnly"
	case DurabilityNeedsFsyncDir:
		return "DurabilityNeedsFsyncDir"
	case DurabilityNeedsCloseSession:
		return "DurabilityNeedsCloseSession"
	case DurabilityNeedsPublishCheckpoint:
		return "DurabilityNeedsPublishCheckpoint"
	default:
		return ""
	}
}

func eligibilitySpecName(eligibility Eligibility) string {
	switch eligibility {
	case EligibilityVisibleCommit:
		return "EligibilityVisibleCommit"
	case EligibilitySlowPath:
		return "EligibilitySlowPath"
	default:
		return ""
	}
}

func slowReasonSpecName(reason SlowReason) string {
	switch reason {
	case SlowReasonNone:
		return "SlowReasonNone"
	case SlowReasonReadOnly:
		return "SlowReasonReadOnly"
	case SlowReasonRangeRead:
		return "SlowReasonRangeRead"
	case SlowReasonDurabilityBarrier:
		return "SlowReasonDurabilityBarrier"
	case SlowReasonCrossBucket:
		return "SlowReasonCrossBucket"
	case SlowReasonSharedQuota:
		return "SlowReasonSharedQuota"
	case SlowReasonDynamicWriteSet:
		return "SlowReasonDynamicWriteSet"
	case SlowReasonMaintenanceScan:
		return "SlowReasonMaintenanceScan"
	default:
		return ""
	}
}

func predicateKindSpecName(kind PredicateKind) string {
	switch kind {
	case PredicateExists:
		return "PredicateExists"
	case PredicateNotExists:
		return "PredicateNotExists"
	case PredicateObservedValue:
		return "PredicateObservedValue"
	case PredicatePrefixScan:
		return "PredicatePrefixScan"
	default:
		return ""
	}
}

func effectKindSpecName(kind EffectKind) string {
	switch kind {
	case EffectPut:
		return "EffectPut"
	case EffectDelete:
		return "EffectDelete"
	case EffectDerivedPut:
		return "EffectDerivedPut"
	case EffectDerivedDelete:
		return "EffectDerivedDelete"
	default:
		return ""
	}
}

func guardSpecName(guard RuntimeGuard) string {
	switch guard {
	case GuardSingleLinkInode:
		return "GuardSingleLinkInode"
	case GuardSameAuthority:
		return "GuardSameAuthority"
	case GuardNonDirectoryInode:
		return "GuardNonDirectoryInode"
	case GuardEmptyDirectory:
		return "GuardEmptyDirectory"
	case GuardLiveSession:
		return "GuardLiveSession"
	case GuardExpiredSessionOwner:
		return "GuardExpiredSessionOwner"
	case GuardQuotaCredit:
		return "GuardQuotaCredit"
	default:
		return ""
	}
}
