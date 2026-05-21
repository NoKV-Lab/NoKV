// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package compile

import (
	"slices"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/proof"
	"github.com/stretchr/testify/require"
)

var testMount = fsmeta.MountIdentity{MountID: "vol", MountKeyID: 7}
var benchmarkCompiledOp CompiledOp

func testParentInSameBucket(t *testing.T, base fsmeta.InodeID) fsmeta.InodeID {
	t.Helper()
	want := fsmeta.BucketForInodeID(base)
	for candidate := base + 1; candidate < base+4096; candidate++ {
		if fsmeta.BucketForInodeID(candidate) == want {
			return candidate
		}
	}
	t.Fatalf("no same-bucket parent found for inode %d", base)
	return 0
}

func testParentInDifferentBucket(t *testing.T, base fsmeta.InodeID) fsmeta.InodeID {
	t.Helper()
	want := fsmeta.BucketForInodeID(base)
	for candidate := base + 1; candidate < base+4096; candidate++ {
		if fsmeta.BucketForInodeID(candidate) != want {
			return candidate
		}
	}
	t.Fatalf("no different-bucket parent found for inode %d", base)
	return 0
}

func testParentInDifferentBucketAfter(t *testing.T, base, start fsmeta.InodeID) fsmeta.InodeID {
	t.Helper()
	want := fsmeta.BucketForInodeID(base)
	for candidate := start; candidate < start+4096; candidate++ {
		if fsmeta.BucketForInodeID(candidate) != want {
			return candidate
		}
	}
	t.Fatalf("no different-bucket parent found for inode %d after %d", base, start)
	return 0
}

func mustInodeKey(t *testing.T, inode fsmeta.InodeID) []byte {
	t.Helper()
	key, err := fsmeta.EncodeInodeKey(testMount, inode)
	require.NoError(t, err)
	return key
}

func testPredicateProof(key, value []byte, present bool, version uint64, source proof.ReadSource) proof.PredicateProof {
	return proof.NewPredicateProof(key, value, present, version, source, proof.ProofFrontier{})
}

func testCompileAOT(tb testing.TB, delta SemanticDelta) CompiledOp {
	tb.Helper()
	compiled, err := compileAOTDelta(delta)
	require.NoError(tb, err)
	return compiled
}

func testMaterializeAOT(tb testing.TB, delta SemanticDelta, proofs []proof.PredicateProof) MaterializedOp {
	tb.Helper()
	compiled := testCompileAOT(tb, delta)
	return MaterializedOp{
		CompiledOp:      compiled,
		PredicateProofs: clonePredicateProofs(proofs),
	}
}

func testMaterializeAOTWithEffects(tb testing.TB, op CompiledOp, effects []WriteEffect, proofs []proof.PredicateProof) MaterializedOp {
	tb.Helper()
	materialized, err := MaterializeCompiledOpWithEvidence(op, effects, PredicateEvidence{Proofs: proofs}, nil)
	require.NoError(tb, err)
	return materialized
}

func testGuardProofsFor(op MaterializedOp) []proof.GuardProof {
	proofs, err := GuardProofsFor(op.CompiledOp, op.PredicateProofs, op.Delta.RuntimeGuards)
	if err != nil {
		panic(err)
	}
	return proofs
}

func testCreateDelta(tb testing.TB, req fsmeta.CreateRequest, mount fsmeta.MountIdentity, inodeID fsmeta.InodeID, opts ...Option) (SemanticDelta, error) {
	tb.Helper()
	program, err := CompileCreateProgram(req, mount, inodeID, opts...)
	if err != nil {
		return SemanticDelta{}, err
	}
	return cloneDelta(program.Compiled.Delta), nil
}

func testUpdateInodeDelta(tb testing.TB, req fsmeta.UpdateInodeRequest, mount fsmeta.MountIdentity, opts ...Option) (SemanticDelta, error) {
	tb.Helper()
	program, err := CompileUpdateInodeProgram(req, mount, opts...)
	if err != nil {
		return SemanticDelta{}, err
	}
	return cloneDelta(program.Compiled.Delta), nil
}

func testRenameDelta(tb testing.TB, req fsmeta.RenameRequest, mount fsmeta.MountIdentity) (SemanticDelta, error) {
	tb.Helper()
	program, err := CompileRenameProgram(req, mount)
	if err != nil {
		return SemanticDelta{}, err
	}
	return cloneDelta(program.Compiled.Delta), nil
}

func testRenameReplaceDelta(tb testing.TB, req fsmeta.RenameReplaceRequest, mount fsmeta.MountIdentity) (SemanticDelta, error) {
	tb.Helper()
	program, err := CompileRenameReplaceProgram(req, mount)
	if err != nil {
		return SemanticDelta{}, err
	}
	return cloneDelta(program.Compiled.Delta), nil
}

func testSnapshotSubtreeDelta(tb testing.TB, req fsmeta.SnapshotSubtreeRequest, mount fsmeta.MountIdentity) (SemanticDelta, error) {
	tb.Helper()
	program, err := CompileSnapshotSubtreeProgram(req, mount)
	if err != nil {
		return SemanticDelta{}, err
	}
	return cloneDelta(program.Compiled.Delta), nil
}

func testLinkDelta(tb testing.TB, req fsmeta.LinkRequest, mount fsmeta.MountIdentity, opts ...Option) (SemanticDelta, error) {
	tb.Helper()
	program, err := CompileLinkProgram(req, mount, opts...)
	if err != nil {
		return SemanticDelta{}, err
	}
	return cloneDelta(program.Compiled.Delta), nil
}

func testUnlinkDelta(tb testing.TB, req fsmeta.UnlinkRequest, mount fsmeta.MountIdentity, opts ...Option) (SemanticDelta, error) {
	tb.Helper()
	program, err := CompileUnlinkProgram(req, mount, opts...)
	if err != nil {
		return SemanticDelta{}, err
	}
	return cloneDelta(program.Compiled.Delta), nil
}

func testRemoveDelta(tb testing.TB, req fsmeta.RemoveRequest, mount fsmeta.MountIdentity, opts ...Option) (SemanticDelta, error) {
	tb.Helper()
	program, err := CompileRemoveProgram(req, mount, opts...)
	if err != nil {
		return SemanticDelta{}, err
	}
	return cloneDelta(program.Compiled.Delta), nil
}

func testOpenWriteSessionDelta(tb testing.TB, req fsmeta.OpenWriteSessionRequest, mount fsmeta.MountIdentity) (SemanticDelta, error) {
	tb.Helper()
	program, err := CompileOpenWriteSessionProgram(req, mount)
	if err != nil {
		return SemanticDelta{}, err
	}
	return cloneDelta(program.Compiled.Delta), nil
}

func testHeartbeatWriteSessionDelta(tb testing.TB, req fsmeta.HeartbeatWriteSessionRequest, mount fsmeta.MountIdentity) (SemanticDelta, error) {
	tb.Helper()
	program, err := CompileHeartbeatWriteSessionProgram(req, mount)
	if err != nil {
		return SemanticDelta{}, err
	}
	return cloneDelta(program.Compiled.Delta), nil
}

func testCloseWriteSessionDelta(tb testing.TB, req fsmeta.CloseWriteSessionRequest, mount fsmeta.MountIdentity) (SemanticDelta, error) {
	tb.Helper()
	program, err := CompileCloseWriteSessionProgram(req, mount)
	if err != nil {
		return SemanticDelta{}, err
	}
	return cloneDelta(program.Compiled.Delta), nil
}

func testExpireWriteSessionsDelta(tb testing.TB, req fsmeta.ExpireWriteSessionsRequest, mount fsmeta.MountIdentity) (SemanticDelta, error) {
	tb.Helper()
	program, err := CompileExpireWriteSessionsProgram(req, mount)
	if err != nil {
		return SemanticDelta{}, err
	}
	return cloneDelta(program.Compiled.Delta), nil
}

func testConcreteUpdateInodeDelta(tb testing.TB, expected []byte) (SemanticDelta, []proof.PredicateProof) {
	tb.Helper()
	delta, err := testUpdateInodeDelta(tb, fsmeta.UpdateInodeRequest{
		Mount:   "vol",
		Parent:  3,
		Inode:   44,
		Name:    "file",
		SetMode: true,
		Mode:    0o600,
	}, testMount)
	require.NoError(tb, err)
	dentryValue, err := fsmeta.EncodeDentryValue(fsmeta.DentryRecord{
		Parent: 3,
		Name:   "file",
		Inode:  44,
		Type:   fsmeta.InodeTypeFile,
	})
	require.NoError(tb, err)
	dentryKey := delta.ReadPredicates[0].Key
	inodeKey := delta.ReadPredicates[1].Key
	if expected == nil {
		expected, err = fsmeta.EncodeInodeValue(fsmeta.InodeRecord{Inode: 44, Type: fsmeta.InodeTypeFile, LinkCount: 1})
		require.NoError(tb, err)
	}
	delta.ReadPredicates = []Predicate{
		{Kind: PredicateObservedValue, Key: dentryKey, ExpectedValue: dentryValue, HasExpectedValue: true, RuntimeChecked: true},
		{Kind: PredicateObservedValue, Key: inodeKey, ExpectedValue: expected, HasExpectedValue: true, RuntimeChecked: true},
	}
	delta.WriteEffects = []WriteEffect{{Kind: EffectPut, Key: inodeKey, Value: []byte("new-inode")}}
	return delta, []proof.PredicateProof{
		testPredicateProof(dentryKey, dentryValue, true, 9, proof.ReadSourceBase),
		testPredicateProof(inodeKey, expected, true, 9, proof.ReadSourceBase),
	}
}

func TestCreateCompilesVisibleCommitDelta(t *testing.T) {
	req := fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile, Size: 128, Mode: 0o644},
	}
	delta, err := testCreateDelta(t, req, testMount, 22)
	require.NoError(t, err)

	dentryKey, err := fsmeta.EncodeDentryKey(testMount, fsmeta.RootInode, "file")
	require.NoError(t, err)
	parentKey, err := fsmeta.EncodeInodeKey(testMount, fsmeta.RootInode)
	require.NoError(t, err)
	inodeKey, err := fsmeta.EncodeInodeKey(testMount, 22)
	require.NoError(t, err)

	require.Equal(t, fsmeta.OperationCreate, delta.Kind)
	require.Equal(t, EligibilityVisibleCommit, delta.Eligibility)
	require.Equal(t, SlowReasonNone, delta.SlowReason)
	require.Equal(t, []Predicate{
		{Kind: PredicateObservedValue, Key: parentKey},
		{Kind: PredicateNotExists, Key: dentryKey},
		{Kind: PredicateNotExists, Key: inodeKey},
	}, delta.ReadPredicates)
	require.Len(t, delta.WriteEffects, 3)
	require.Equal(t, EffectDerivedPut, delta.WriteEffects[0].Kind)
	require.Equal(t, parentKey, delta.WriteEffects[0].Key)
	require.Equal(t, EffectPut, delta.WriteEffects[1].Kind)
	require.Equal(t, dentryKey, delta.WriteEffects[1].Key)
	require.Equal(t, EffectPut, delta.WriteEffects[2].Kind)
	require.Equal(t, inodeKey, delta.WriteEffects[2].Key)

	dentry, err := fsmeta.DecodeDentryValue(delta.WriteEffects[1].Value)
	require.NoError(t, err)
	require.Equal(t, fsmeta.DentryRecord{Parent: fsmeta.RootInode, Name: "file", Inode: 22, Type: fsmeta.InodeTypeFile}, dentry)
	inode, err := fsmeta.DecodeInodeValue(delta.WriteEffects[2].Value)
	require.NoError(t, err)
	require.Equal(t, fsmeta.InodeID(22), inode.Inode)
	require.Equal(t, uint64(128), inode.Size)

	require.Equal(t, []fsmeta.InodeID{fsmeta.RootInode}, delta.Authority.Parents)
	require.Equal(t, []fsmeta.InodeID{22}, delta.Authority.Inodes)
	require.True(t, slices.Contains(delta.Authority.Buckets, fsmeta.BucketForInodeID(fsmeta.RootInode)))
	require.True(t, slices.Contains(delta.Authority.Buckets, fsmeta.BucketForInodeID(22)))
}

func TestCreateCompilesSegmentInstallableOperation(t *testing.T) {
	inodeID := testParentInDifferentBucket(t, fsmeta.RootInode)
	delta, err := testCreateDelta(t, fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	}, testMount, inodeID)
	require.NoError(t, err)
	dentryKey, err := fsmeta.EncodeDentryKey(testMount, fsmeta.RootInode, "file")
	require.NoError(t, err)

	op := testCompileAOT(t, delta)
	require.Equal(t, delta.Kind, op.Delta.Kind)
	require.True(t, op.Authority.Required)
	require.Equal(t, FenceActiveAuthority, op.Authority.Fence)
	require.True(t, op.Placement.CanSegment)
	require.False(t, op.Placement.RequiresMaterialize)
	require.Equal(t, testMount.MountKeyID, op.Placement.MountKeyID)
	require.Equal(t, DurabilityVisibleOnly, op.Durability)
	require.Equal(t, SegmentInstallCatalog, op.Placement.Install)
	require.Equal(t, SegmentInstallCatalog, op.Placement.MergeKey.Install)
	require.Equal(t, segmentFormatVersion, op.Placement.MergeKey.FormatVersion)
	require.Equal(t, []MutationID{0, 1, 2}, op.Atomicity.Members)
	require.False(t, op.Atomicity.Splittable)
	require.Equal(t, RecoveryReplayAllOrNothing, op.Atomicity.Recovery)
	require.Equal(t, op.DescriptorDigest, op.Atomicity.Digest)

	require.Len(t, op.Predicates, 3)
	require.True(t, op.Predicates[0].NeedValue)
	require.False(t, op.Predicates[0].NeedAbsent)
	require.True(t, op.Predicates[1].NeedAbsent)
	require.True(t, op.Predicates[2].NeedAbsent)
	require.Len(t, op.Effects, 3)
	require.Equal(t, MutationID(0), op.Effects[0].ID)
	require.Equal(t, DerivationRuntimeValue, op.Effects[0].Derivation)
	require.False(t, op.Effects[0].Concrete)
	require.Equal(t, testMount.MountKeyID, op.Effects[0].MountKeyID)
	require.Equal(t, fsmeta.KeyKindInode, op.Effects[0].RecordKind)
	require.Len(t, op.Watch, 1)
	require.Equal(t, WatchEventCreate, op.Watch[0].EventKind)
	require.Equal(t, WatchEmitVisible, op.Watch[0].EmitAt)
	require.Equal(t, dentryKey, op.Watch[0].Key)
	require.Equal(t, fsmeta.RootInode, op.Watch[0].Parent)
	require.Equal(t, "file", op.Watch[0].Name)
	require.Equal(t, inodeID, op.Watch[0].Inode)
	require.Len(t, op.Footprint.Reads, 3)
	require.Len(t, op.Footprint.Writes, 3)
	require.Len(t, op.Footprint.ConflictKeys, 6)
	require.False(t, op.Footprint.HasPrefixRead)
	require.False(t, op.Footprint.HasOpaqueKeys)
	require.True(t, op.Footprint.EstimatedBytes > 0)
	require.True(t, op.Completion.RetainCompletion)
	require.Equal(t, CompletionVisible, op.Completion.Kind)
	require.Equal(t, uint32(3), op.Completion.MutationCount)
	require.Equal(t, op.DescriptorDigest, op.Completion.DescriptorDigest)
	require.Equal(t, op.Placement.MergeKey, op.Segment.MergeKey)
	require.True(t, op.Segment.CanAppend)
	require.Equal(t, uint32(1), op.Segment.OperationCount)
	require.Equal(t, uint32(3), op.Segment.MutationCount)
}

func TestGeneratedCreateProgramMatchesCurrentCompiler(t *testing.T) {
	req := fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile, Size: 128, Mode: 0o644},
	}
	inodeID := testParentInDifferentBucket(t, fsmeta.RootInode)
	for _, tc := range []struct {
		name string
		opts []Option
	}{
		{name: "default"},
		{name: "escrow", opts: []Option{WithQuotaMode(QuotaModeEscrow)}},
		{name: "shared", opts: []Option{WithQuotaMode(QuotaModeShared)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			delta, err := testCreateDelta(t, req, testMount, inodeID, tc.opts...)
			require.NoError(t, err)

			program, err := CompileCreateProgram(req, testMount, inodeID, tc.opts...)
			require.NoError(t, err)
			require.Equal(t, testCompileAOT(t, delta), program.Compiled)

			defaultMaterialized, err := MaterializeCreate(program, CreateValues{})
			require.NoError(t, err)
			expectedDefault, err := MaterializeCompiledOpWithEvidence(program.Compiled, nil, PredicateEvidence{}, nil)
			require.NoError(t, err)
			require.Equal(t, expectedDefault, defaultMaterialized)

			parentValue, err := fsmeta.EncodeInodeValue(fsmeta.InodeRecord{
				Inode:      req.Parent,
				Type:       fsmeta.InodeTypeDirectory,
				LinkCount:  1,
				ChildCount: 1,
			})
			require.NoError(t, err)
			values := CreateValues{
				ParentInodeValue: parentValue,
				DentryValue:      append([]byte(nil), program.Compiled.Delta.WriteEffects[1].Value...),
				InodeValue:       append([]byte(nil), program.Compiled.Delta.WriteEffects[2].Value...),
			}
			valueMaterialized, err := MaterializeCreate(program, values)
			require.NoError(t, err)
			expectedDelta := cloneDelta(delta)
			expectedDelta.WriteEffects = []WriteEffect{
				{Kind: EffectPut, Key: expectedDelta.WriteEffects[0].Key, Value: parentValue},
				expectedDelta.WriteEffects[1],
				expectedDelta.WriteEffects[2],
			}
			expected, err := MaterializeCompiledOpWithEvidence(program.Compiled, expectedDelta.WriteEffects, PredicateEvidence{}, nil)
			require.NoError(t, err)
			require.Equal(t, expected, valueMaterialized)
		})
	}
}

func TestGeneratedCreateMaterializerRejectsMalformedProgram(t *testing.T) {
	_, err := MaterializeCreate(CreateProgram{}, CreateValues{})
	require.ErrorIs(t, err, fsmeta.ErrInvalidRequest)

	req := fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	}
	program, err := CompileCreateProgram(req, testMount, 22)
	require.NoError(t, err)
	_, err = MaterializeCreate(program, CreateValues{DentryValue: program.Compiled.Delta.WriteEffects[1].Value})
	require.ErrorIs(t, err, fsmeta.ErrInvalidRequest)
}

func TestSessionProgramsMatchCurrentCompiler(t *testing.T) {
	openReq := fsmeta.OpenWriteSessionRequest{
		Mount:   "vol",
		Inode:   44,
		Session: "writer-1",
		TTL:     time.Second,
	}
	openDelta, err := testOpenWriteSessionDelta(t, openReq, testMount)
	require.NoError(t, err)
	openProgram, err := CompileOpenWriteSessionProgram(openReq, testMount)
	require.NoError(t, err)
	require.Equal(t, testCompileAOT(t, openDelta), openProgram.Compiled)

	heartbeatReq := fsmeta.HeartbeatWriteSessionRequest{
		Mount:   "vol",
		Inode:   44,
		Session: "writer-1",
		TTL:     time.Second,
	}
	heartbeatDelta, err := testHeartbeatWriteSessionDelta(t, heartbeatReq, testMount)
	require.NoError(t, err)
	heartbeatProgram, err := CompileHeartbeatWriteSessionProgram(heartbeatReq, testMount)
	require.NoError(t, err)
	require.Equal(t, testCompileAOT(t, heartbeatDelta), heartbeatProgram.Compiled)

	closeReq := fsmeta.CloseWriteSessionRequest{
		Mount:   "vol",
		Inode:   44,
		Session: "writer-1",
	}
	closeDelta, err := testCloseWriteSessionDelta(t, closeReq, testMount)
	require.NoError(t, err)
	closeProgram, err := CompileCloseWriteSessionProgram(closeReq, testMount)
	require.NoError(t, err)
	require.Equal(t, testCompileAOT(t, closeDelta), closeProgram.Compiled)
}

func TestGeneratedProgramEntriesAreCanonical(t *testing.T) {
	renameToParent := testParentInSameBucket(t, fsmeta.RootInode)
	crossBucketParent := testParentInDifferentBucket(t, fsmeta.RootInode)
	for _, tc := range []struct {
		name    string
		compile func() (CompiledOp, error)
	}{
		{
			name: "update_inode",
			compile: func() (CompiledOp, error) {
				program, err := CompileUpdateInodeProgram(fsmeta.UpdateInodeRequest{Mount: "vol", Parent: fsmeta.RootInode, Inode: 44, Name: "file", SetMode: true, Mode: 0o600}, testMount, WithQuotaMode(QuotaModeEscrow))
				return program.Compiled, err
			},
		},
		{
			name: "lookup",
			compile: func() (CompiledOp, error) {
				program, err := CompileLookupProgram(fsmeta.LookupRequest{Mount: "vol", Parent: fsmeta.RootInode, Name: "file"}, testMount)
				return program.Compiled, err
			},
		},
		{
			name: "readdir",
			compile: func() (CompiledOp, error) {
				program, err := CompileReadDirProgram(fsmeta.ReadDirRequest{Mount: "vol", Parent: fsmeta.RootInode, Limit: 32}, testMount)
				return program.Compiled, err
			},
		},
		{
			name: "snapshot_subtree",
			compile: func() (CompiledOp, error) {
				program, err := CompileSnapshotSubtreeProgram(fsmeta.SnapshotSubtreeRequest{Mount: "vol", RootInode: fsmeta.RootInode}, testMount)
				return program.Compiled, err
			},
		},
		{
			name: "rename",
			compile: func() (CompiledOp, error) {
				program, err := CompileRenameProgram(fsmeta.RenameRequest{Mount: "vol", FromParent: fsmeta.RootInode, FromName: "old", ToParent: renameToParent, ToName: "new"}, testMount)
				return program.Compiled, err
			},
		},
		{
			name: "rename_replace",
			compile: func() (CompiledOp, error) {
				program, err := CompileRenameReplaceProgram(fsmeta.RenameReplaceRequest{Mount: "vol", FromParent: fsmeta.RootInode, FromName: "old", ToParent: renameToParent, ToName: "new"}, testMount)
				return program.Compiled, err
			},
		},
		{
			name: "rename_subtree",
			compile: func() (CompiledOp, error) {
				program, err := CompileRenameSubtreeProgram(fsmeta.RenameSubtreeRequest{Mount: "vol", FromParent: fsmeta.RootInode, FromName: "old", ToParent: crossBucketParent, ToName: "new"}, testMount)
				return program.Compiled, err
			},
		},
		{
			name: "link",
			compile: func() (CompiledOp, error) {
				program, err := CompileLinkProgram(fsmeta.LinkRequest{Mount: "vol", FromParent: fsmeta.RootInode, FromName: "old", ToParent: renameToParent, ToName: "new"}, testMount, WithQuotaMode(QuotaModeEscrow))
				return program.Compiled, err
			},
		},
		{
			name: "unlink",
			compile: func() (CompiledOp, error) {
				program, err := CompileUnlinkProgram(fsmeta.UnlinkRequest{Mount: "vol", Parent: fsmeta.RootInode, Name: "old"}, testMount, WithQuotaMode(QuotaModeEscrow))
				return program.Compiled, err
			},
		},
		{
			name: "remove",
			compile: func() (CompiledOp, error) {
				program, err := CompileRemoveProgram(fsmeta.RemoveRequest{Mount: "vol", Parent: fsmeta.RootInode, Name: "old"}, testMount, WithQuotaMode(QuotaModeEscrow))
				return program.Compiled, err
			},
		},
		{
			name: "open_write_session",
			compile: func() (CompiledOp, error) {
				program, err := CompileOpenWriteSessionProgram(fsmeta.OpenWriteSessionRequest{Mount: "vol", Inode: 44, Session: "writer-1", TTL: time.Second}, testMount)
				return program.Compiled, err
			},
		},
		{
			name: "heartbeat_write_session",
			compile: func() (CompiledOp, error) {
				program, err := CompileHeartbeatWriteSessionProgram(fsmeta.HeartbeatWriteSessionRequest{Mount: "vol", Inode: 44, Session: "writer-1", TTL: time.Second}, testMount)
				return program.Compiled, err
			},
		},
		{
			name: "close_write_session",
			compile: func() (CompiledOp, error) {
				program, err := CompileCloseWriteSessionProgram(fsmeta.CloseWriteSessionRequest{Mount: "vol", Inode: 44, Session: "writer-1"}, testMount)
				return program.Compiled, err
			},
		},
		{
			name: "expire_write_sessions",
			compile: func() (CompiledOp, error) {
				program, err := CompileExpireWriteSessionsProgram(fsmeta.ExpireWriteSessionsRequest{Mount: "vol", Limit: 16}, testMount)
				return program.Compiled, err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			compiled, err := tc.compile()
			require.NoError(t, err)
			require.Equal(t, testCompileAOT(t, compiled.Delta), compiled)
		})
	}
}

func TestSessionProgramsRejectInvalidTTL(t *testing.T) {
	_, err := CompileOpenWriteSessionProgram(fsmeta.OpenWriteSessionRequest{
		Mount:   "vol",
		Inode:   44,
		Session: "writer-1",
	}, testMount)
	require.ErrorIs(t, err, fsmeta.ErrInvalidRequest)

	_, err = CompileHeartbeatWriteSessionProgram(fsmeta.HeartbeatWriteSessionRequest{
		Mount:   "vol",
		Inode:   44,
		Session: "writer-1",
	}, testMount)
	require.ErrorIs(t, err, fsmeta.ErrInvalidRequest)
}

func TestOpenWriteSessionMaterializerMatchesGenericMaterialization(t *testing.T) {
	req := fsmeta.OpenWriteSessionRequest{
		Mount:   "vol",
		Inode:   44,
		Session: "writer-1",
		TTL:     time.Second,
	}
	program, err := CompileOpenWriteSessionProgram(req, testMount)
	require.NoError(t, err)
	sessionValue, err := fsmeta.EncodeSessionValue(fsmeta.SessionRecord{Session: req.Session, Inode: req.Inode, ExpiresUnixNs: 200})
	require.NoError(t, err)
	inodeValue, err := fsmeta.EncodeInodeValue(fsmeta.InodeRecord{Inode: req.Inode, Type: fsmeta.InodeTypeFile})
	require.NoError(t, err)
	inodeKey := program.Compiled.Delta.ReadPredicates[0].Key
	sessionKey := program.Compiled.Delta.ReadPredicates[1].Key
	ownerKey := program.Compiled.Delta.ReadPredicates[2].Key
	proofs := []proof.PredicateProof{
		testPredicateProof(inodeKey, inodeValue, true, 9, proof.ReadSourceBase),
		testPredicateProof(sessionKey, nil, false, 9, proof.ReadSourceBase),
		testPredicateProof(ownerKey, nil, false, 9, proof.ReadSourceBase),
	}

	materialized, err := MaterializeOpenWriteSession(program, OpenWriteSessionValues{
		SessionValue:    sessionValue,
		PredicateProofs: proofs,
	})
	require.NoError(t, err)

	expectedDelta := cloneDelta(program.Compiled.Delta)
	expectedDelta.ReadPredicates = []Predicate{
		{Kind: PredicateObservedValue, Key: inodeKey, ExpectedValue: inodeValue, HasExpectedValue: true, RuntimeChecked: true},
		{Kind: PredicateNotExists, Key: sessionKey, RuntimeChecked: true},
		{Kind: PredicateNotExists, Key: ownerKey, RuntimeChecked: true},
	}
	expectedDelta.WriteEffects = []WriteEffect{
		{Kind: EffectPut, Key: sessionKey, Value: sessionValue},
		{Kind: EffectPut, Key: ownerKey, Value: sessionValue},
	}
	expected := testMaterializeAOT(t, expectedDelta, proofs)
	expected.IntentDigest = program.Compiled.IntentDigest
	require.Equal(t, expected, materialized)
}

func TestHeartbeatWriteSessionMaterializerMatchesGenericMaterialization(t *testing.T) {
	req := fsmeta.HeartbeatWriteSessionRequest{
		Mount:   "vol",
		Inode:   44,
		Session: "writer-1",
		TTL:     time.Second,
	}
	program, err := CompileHeartbeatWriteSessionProgram(req, testMount)
	require.NoError(t, err)
	oldValue, err := fsmeta.EncodeSessionValue(fsmeta.SessionRecord{Session: req.Session, Inode: req.Inode, ExpiresUnixNs: 100})
	require.NoError(t, err)
	newValue, err := fsmeta.EncodeSessionValue(fsmeta.SessionRecord{Session: req.Session, Inode: req.Inode, ExpiresUnixNs: 200})
	require.NoError(t, err)
	sessionKey := program.Compiled.Delta.ReadPredicates[0].Key
	ownerKey := program.Compiled.Delta.ReadPredicates[1].Key
	proofs := []proof.PredicateProof{
		testPredicateProof(sessionKey, oldValue, true, 11, proof.ReadSourceBase),
		testPredicateProof(ownerKey, oldValue, true, 11, proof.ReadSourceBase),
	}

	materialized, err := MaterializeHeartbeatWriteSession(program, HeartbeatWriteSessionValues{
		SessionValue:    newValue,
		PredicateProofs: proofs,
	})
	require.NoError(t, err)

	expectedDelta := cloneDelta(program.Compiled.Delta)
	expectedDelta.ReadPredicates = []Predicate{
		{Kind: PredicateObservedValue, Key: sessionKey, ExpectedValue: oldValue, HasExpectedValue: true, RuntimeChecked: true},
		{Kind: PredicateObservedValue, Key: ownerKey, ExpectedValue: oldValue, HasExpectedValue: true, RuntimeChecked: true},
	}
	expectedDelta.WriteEffects = []WriteEffect{
		{Kind: EffectPut, Key: sessionKey, Value: newValue},
		{Kind: EffectPut, Key: ownerKey, Value: newValue},
	}
	expected := testMaterializeAOT(t, expectedDelta, proofs)
	expected.IntentDigest = program.Compiled.IntentDigest
	require.Equal(t, expected, materialized)
}

func TestCloseWriteSessionMaterializerMatchesGenericMaterialization(t *testing.T) {
	req := fsmeta.CloseWriteSessionRequest{
		Mount:   "vol",
		Inode:   44,
		Session: "writer-1",
	}
	program, err := CompileCloseWriteSessionProgram(req, testMount)
	require.NoError(t, err)
	sessionValue, err := fsmeta.EncodeSessionValue(fsmeta.SessionRecord{Session: req.Session, Inode: req.Inode, ExpiresUnixNs: 100})
	require.NoError(t, err)
	ownerValue, err := fsmeta.EncodeSessionValue(fsmeta.SessionRecord{Session: "other", Inode: req.Inode, ExpiresUnixNs: 100})
	require.NoError(t, err)
	sessionKey := program.Compiled.Delta.ReadPredicates[0].Key
	ownerKey := program.Compiled.Delta.ReadPredicates[1].Key
	proofs := []proof.PredicateProof{
		testPredicateProof(sessionKey, sessionValue, true, 12, proof.ReadSourceBase),
		testPredicateProof(ownerKey, ownerValue, true, 12, proof.ReadSourceBase),
	}

	materialized, err := MaterializeCloseWriteSession(program, CloseWriteSessionValues{
		PredicateProofs: proofs,
	})
	require.NoError(t, err)

	expectedDelta := cloneDelta(program.Compiled.Delta)
	expectedDelta.ReadPredicates = []Predicate{
		{Kind: PredicateObservedValue, Key: sessionKey, ExpectedValue: sessionValue, HasExpectedValue: true, RuntimeChecked: true},
		{Kind: PredicateObservedValue, Key: ownerKey, ExpectedValue: ownerValue, HasExpectedValue: true, RuntimeChecked: true},
	}
	expectedDelta.WriteEffects = []WriteEffect{{Kind: EffectDelete, Key: sessionKey}}
	expected := testMaterializeAOT(t, expectedDelta, proofs)
	expected.IntentDigest = program.Compiled.IntentDigest
	require.Equal(t, expected, materialized)

	withOwnerDelete, err := MaterializeCloseWriteSession(program, CloseWriteSessionValues{
		DeleteOwner:     true,
		PredicateProofs: proofs,
	})
	require.NoError(t, err)
	expectedDelta.WriteEffects = []WriteEffect{
		{Kind: EffectDelete, Key: sessionKey},
		{Kind: EffectDelete, Key: ownerKey},
	}
	expected = testMaterializeAOT(t, expectedDelta, proofs)
	expected.IntentDigest = program.Compiled.IntentDigest
	require.Equal(t, expected, withOwnerDelete)
}

func TestSessionMaterializersRejectMalformedInput(t *testing.T) {
	openReq := fsmeta.OpenWriteSessionRequest{Mount: "vol", Inode: 44, Session: "writer-1", TTL: time.Second}
	openProgram, err := CompileOpenWriteSessionProgram(openReq, testMount)
	require.NoError(t, err)
	_, err = MaterializeOpenWriteSession(openProgram, OpenWriteSessionValues{})
	require.ErrorIs(t, err, fsmeta.ErrInvalidRequest)

	sessionValue, err := fsmeta.EncodeSessionValue(fsmeta.SessionRecord{Session: openReq.Session, Inode: openReq.Inode, ExpiresUnixNs: 100})
	require.NoError(t, err)
	badProof := proof.PredicateProof{Key: openProgram.Compiled.Delta.ReadPredicates[0].Key, Present: true, Value: []byte("bad"), Version: 1, Source: proof.ReadSourceBase}
	_, err = MaterializeOpenWriteSession(openProgram, OpenWriteSessionValues{
		SessionValue:    sessionValue,
		PredicateProofs: []proof.PredicateProof{badProof},
	})
	require.Error(t, err)
}

func TestCreateRespectsQuotaMode(t *testing.T) {
	req := fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: 3,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	}

	shared, err := testCreateDelta(t, req, testMount, 44, WithQuotaMode(QuotaModeShared))
	require.NoError(t, err)
	require.Equal(t, EligibilitySlowPath, shared.Eligibility)
	require.Equal(t, SlowReasonSharedQuota, shared.SlowReason)

	escrow, err := testCreateDelta(t, req, testMount, 44, WithQuotaMode(QuotaModeEscrow))
	require.NoError(t, err)
	require.Equal(t, EligibilityVisibleCommit, escrow.Eligibility)
	require.Contains(t, escrow.RuntimeGuards, GuardQuotaCredit)
}

func BenchmarkCompileCreateProgram(b *testing.B) {
	req := fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile, Size: 128, Mode: 0o644},
	}
	inodeID := fsmeta.InodeID(22)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		program, err := CompileCreateProgram(req, testMount, inodeID, WithQuotaMode(QuotaModeEscrow))
		if err != nil {
			b.Fatal(err)
		}
		benchmarkCompiledOp = program.Compiled
	}
}

func BenchmarkCompileOpenWriteSessionProgram(b *testing.B) {
	req := fsmeta.OpenWriteSessionRequest{
		Mount:   "vol",
		Inode:   44,
		Session: "writer-1",
		TTL:     time.Second,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		program, err := CompileOpenWriteSessionProgram(req, testMount)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkCompiledOp = program.Compiled
	}
}

func TestRenameBucketLocalVisibleCrossBucketSlow(t *testing.T) {
	sameParent, err := testRenameDelta(t, fsmeta.RenameRequest{
		Mount:      "vol",
		FromParent: 8,
		FromName:   "old",
		ToParent:   8,
		ToName:     "new",
	}, testMount)
	require.NoError(t, err)
	require.Equal(t, EligibilityVisibleCommit, sameParent.Eligibility)
	require.Equal(t, []fsmeta.InodeID{8}, sameParent.Authority.Parents)
	require.Equal(t, []WriteEffect{
		{Kind: EffectDelete, Key: sameParent.Plan.MutateKeys[0]},
		{Kind: EffectDerivedPut, Key: sameParent.Plan.MutateKeys[1]},
		{Kind: EffectDerivedPut, Key: sameParent.Plan.MutateKeys[2]},
		{Kind: EffectDerivedPut, Key: sameParent.Plan.MutateKeys[3]},
	}, sameParent.WriteEffects)

	sameBucketParent := testParentInSameBucket(t, 8)
	crossParentSameBucket, err := testRenameDelta(t, fsmeta.RenameRequest{
		Mount:      "vol",
		FromParent: 8,
		FromName:   "old",
		ToParent:   sameBucketParent,
		ToName:     "new",
	}, testMount)
	require.NoError(t, err)
	require.Equal(t, EligibilityVisibleCommit, crossParentSameBucket.Eligibility)
	require.Equal(t, SlowReasonNone, crossParentSameBucket.SlowReason)
	require.Equal(t, []fsmeta.InodeID{8, sameBucketParent}, crossParentSameBucket.Authority.Parents)
	require.Len(t, crossParentSameBucket.Authority.Buckets, 1)

	differentBucketParent := testParentInDifferentBucket(t, 8)
	crossBucket, err := testRenameDelta(t, fsmeta.RenameRequest{
		Mount:      "vol",
		FromParent: 8,
		FromName:   "old",
		ToParent:   differentBucketParent,
		ToName:     "new",
	}, testMount)
	require.NoError(t, err)
	require.Equal(t, EligibilitySlowPath, crossBucket.Eligibility)
	require.Equal(t, SlowReasonCrossBucket, crossBucket.SlowReason)
	require.Equal(t, []fsmeta.InodeID{8, differentBucketParent}, crossBucket.Authority.Parents)
	require.Len(t, crossBucket.Authority.Buckets, 2)
}

func TestRenameReplaceCompilesSlowPathWithoutBarrier(t *testing.T) {
	delta, err := testRenameReplaceDelta(t, fsmeta.RenameReplaceRequest{
		Mount:      "vol",
		FromParent: 8,
		FromName:   ".stage-artifact",
		ToParent:   8,
		ToName:     "artifact",
	}, testMount)
	require.NoError(t, err)

	require.Equal(t, fsmeta.OperationRenameReplace, delta.Kind)
	require.Equal(t, EligibilitySlowPath, delta.Eligibility)
	require.Equal(t, SlowReasonDynamicWriteSet, delta.SlowReason)
	require.False(t, delta.DurabilityBarrier)
	require.False(t, delta.WatchAtSeal)
	require.Equal(t, []fsmeta.InodeID{8}, delta.Authority.Parents)
	require.Len(t, delta.ReadPredicates, 4)
	for _, predicate := range delta.ReadPredicates {
		require.Equal(t, PredicateObservedValue, predicate.Kind)
	}
	require.Len(t, delta.WriteEffects, 4)
	require.Equal(t, EffectDelete, delta.WriteEffects[0].Kind)
	require.Equal(t, EffectDerivedPut, delta.WriteEffects[1].Kind)
	require.Equal(t, EffectDerivedPut, delta.WriteEffects[2].Kind)
	require.Equal(t, EffectDerivedPut, delta.WriteEffects[3].Kind)

	compiled := testCompileAOT(t, delta)
	require.Equal(t, DurabilityVisibleOnly, compiled.Durability)
	require.False(t, compiled.Authority.Required)
	require.Equal(t, FenceNone, compiled.Authority.Fence)
	require.False(t, compiled.Placement.CanSegment)
}

func TestSlowPathBoundariesStayExplicit(t *testing.T) {
	snapshot, err := testSnapshotSubtreeDelta(t, fsmeta.SnapshotSubtreeRequest{
		Mount:     "vol",
		RootInode: 11,
	}, testMount)
	require.NoError(t, err)
	require.Equal(t, EligibilitySlowPath, snapshot.Eligibility)
	require.Equal(t, SlowReasonDurabilityBarrier, snapshot.SlowReason)
	require.True(t, snapshot.DurabilityBarrier)
	compiledSnapshot := testCompileAOT(t, snapshot)
	require.False(t, compiledSnapshot.Authority.Required)
	require.Equal(t, FenceNone, compiledSnapshot.Authority.Fence)
	require.Equal(t, DurabilityNeedsPublishCheckpoint, compiledSnapshot.Durability)
	require.False(t, compiledSnapshot.Placement.CanSegment)

	expire, err := testExpireWriteSessionsDelta(t, fsmeta.ExpireWriteSessionsRequest{
		Mount: "vol",
		Limit: 8,
	}, testMount)
	require.NoError(t, err)
	require.Equal(t, EligibilitySlowPath, expire.Eligibility)
	require.Equal(t, SlowReasonMaintenanceScan, expire.SlowReason)
	require.Len(t, expire.ReadPredicates, fsmeta.DefaultAffinityBucketCount)
}

func TestLinkAndUnlinkCompileRuntimeConcreteVisibleCommitDeltas(t *testing.T) {
	link, err := testLinkDelta(t, fsmeta.LinkRequest{
		Mount:      "vol",
		FromParent: 4,
		FromName:   "src",
		ToParent:   4,
		ToName:     "dst",
	}, testMount)
	require.NoError(t, err)
	require.Equal(t, EligibilityVisibleCommit, link.Eligibility)
	require.Empty(t, link.SlowReason)
	require.Contains(t, link.RuntimeGuards, GuardSameAuthority)
	require.Len(t, link.WriteEffects, 3)
	require.Equal(t, EffectDerivedPut, link.WriteEffects[0].Kind)
	require.Equal(t, EffectDerivedPut, link.WriteEffects[1].Kind)
	require.Equal(t, EffectDerivedPut, link.WriteEffects[2].Kind)

	delta, err := testUnlinkDelta(t, fsmeta.UnlinkRequest{
		Mount:  "vol",
		Parent: 5,
		Name:   "file",
	}, testMount)
	require.NoError(t, err)
	require.Equal(t, EligibilityVisibleCommit, delta.Eligibility)
	require.Empty(t, delta.SlowReason)
	require.Equal(t, EffectDelete, delta.WriteEffects[0].Kind)
	require.Equal(t, EffectDerivedPut, delta.WriteEffects[1].Kind)
	require.Equal(t, EffectDerivedPut, delta.WriteEffects[2].Kind)

	remove, err := testRemoveDelta(t, fsmeta.RemoveRequest{
		Mount:  "vol",
		Parent: 5,
		Name:   "file",
	}, testMount)
	require.NoError(t, err)
	require.Equal(t, EligibilityVisibleCommit, remove.Eligibility)
	require.Empty(t, remove.SlowReason)
	require.Equal(t, EffectDelete, remove.WriteEffects[0].Kind)
	require.Equal(t, EffectDerivedPut, remove.WriteEffects[1].Kind)
	require.Equal(t, EffectDerivedPut, remove.WriteEffects[2].Kind)
}

func TestSessionOperationsCompileVisibleCommitDeltas(t *testing.T) {
	open, err := testOpenWriteSessionDelta(t, fsmeta.OpenWriteSessionRequest{
		Mount:   "vol",
		Inode:   33,
		Session: "writer-1",
		TTL:     time.Minute,
	}, testMount)
	require.NoError(t, err)
	require.Equal(t, EligibilityVisibleCommit, open.Eligibility)
	require.Contains(t, open.RuntimeGuards, GuardExpiredSessionOwner)

	heartbeat, err := testHeartbeatWriteSessionDelta(t, fsmeta.HeartbeatWriteSessionRequest{
		Mount:   "vol",
		Inode:   33,
		Session: "writer-1",
		TTL:     time.Minute,
	}, testMount)
	require.NoError(t, err)
	require.Equal(t, EligibilityVisibleCommit, heartbeat.Eligibility)
	require.Contains(t, heartbeat.RuntimeGuards, GuardLiveSession)

	closeDelta, err := testCloseWriteSessionDelta(t, fsmeta.CloseWriteSessionRequest{
		Mount:   "vol",
		Inode:   33,
		Session: "writer-1",
	}, testMount)
	require.NoError(t, err)
	require.Equal(t, EligibilityVisibleCommit, closeDelta.Eligibility)
	require.Equal(t, DurabilityNeedsCloseSession, testCompileAOT(t, closeDelta).Durability)
	require.Contains(t, closeDelta.RuntimeGuards, GuardLiveSession)
	require.Len(t, closeDelta.ReadPredicates, 2)
	require.Len(t, closeDelta.WriteEffects, 2)
}

func TestDeltasCloneReturnedBytes(t *testing.T) {
	delta, err := testCreateDelta(t, fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "file",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	}, testMount, 22)
	require.NoError(t, err)
	primary := append([]byte(nil), delta.Plan.PrimaryKey...)
	predicate := append([]byte(nil), delta.ReadPredicates[1].Key...)
	value := append([]byte(nil), delta.WriteEffects[1].Value...)

	delta.Plan.PrimaryKey[0] ^= 0xff
	delta.ReadPredicates[1].Key[0] ^= 0xff
	delta.WriteEffects[1].Value[0] ^= 0xff

	require.Equal(t, primary, delta.Plan.MutateKeys[1])
	require.NotEqual(t, delta.Plan.PrimaryKey, delta.Plan.MutateKeys[1])
	require.Equal(t, predicate, delta.Plan.MutateKeys[1])
	require.NotEqual(t, predicate, delta.ReadPredicates[1].Key)
	require.NotEqual(t, value, delta.WriteEffects[1].Value)
}
