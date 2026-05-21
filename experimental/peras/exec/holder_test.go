// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/proof"
	"github.com/stretchr/testify/require"
)

func TestHolderSubmitReturnsVisibleAckWithoutWitnessIO(t *testing.T) {
	witness := newFakeWitnessReplica("store-1")
	holder := newTestHolder(t)

	ack, _, err := holder.Submit(context.Background(), opID("client-a", 1), opWithValueWrites("a", "v1"))
	require.NoError(t, err)

	require.Equal(t, uint64(1), ack.EpochID)
	require.Equal(t, opID("client-a", 1), ack.OpID)
	require.Equal(t, "holder-a", ack.HolderID)
	require.Equal(t, 1, holder.Pending())
	require.Empty(t, witness.snapshot().Segments)
}

func TestHolderSubmitReturnsPendingAckForSameOperationID(t *testing.T) {
	holder := newTestHolder(t)
	id := opID("client-a", 1)
	delta := opWithValueWrites("a", "v1")

	first, _, err := holder.Submit(context.Background(), id, delta)
	require.NoError(t, err)
	second, _, err := holder.Submit(context.Background(), id, delta)
	require.NoError(t, err)

	require.Equal(t, first, second)
	require.Equal(t, 1, holder.Pending())
	plan, _, err := holder.BuildPendingReplayPlan(100)
	require.NoError(t, err)
	require.Equal(t, []OperationID{id}, []OperationID{plan.Operations[0].OpID})
}

func TestHolderPendingAckUsesIntentBeforeGuardProofs(t *testing.T) {
	holder := newTestHolder(t)
	id := opID("client-a", 1)
	unsealed, err := generatedCreateIntentOp("a", "v1", compile.WithQuotaMode(compile.QuotaModeEscrow))
	require.NoError(t, err)
	sealed := sealTestMaterializedOp(unsealed)

	first, _, err := holder.Submit(context.Background(), id, sealed)
	require.NoError(t, err)
	second, ok, err := holder.PendingAck(id, unsealed)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, first, second)
}

func TestHolderSubmitRejectsSameOperationIDDifferentEffects(t *testing.T) {
	holder := newTestHolder(t)
	id := opID("client-a", 1)

	_, _, err := holder.Submit(context.Background(), id, opWithValueWrites("a", "v1"))
	require.NoError(t, err)
	_, _, err = holder.Submit(context.Background(), id, opWithValueWrites("a", "v2"))
	require.ErrorIs(t, err, ErrDuplicateOperation)
	require.Equal(t, 1, holder.Pending())
}

func TestHolderBuildPendingReplayPlanUsesAdmissionOrder(t *testing.T) {
	holder := newTestHolder(t)
	first := opID("client-a", 1)
	second := opID("client-b", 1)

	_, _, err := holder.Submit(context.Background(), first, opWithValueWrites("a", "v1"))
	require.NoError(t, err)
	_, _, err = holder.Submit(context.Background(), second, opWithValueWrites("a", "v2"))
	require.NoError(t, err)

	plan, _, err := holder.BuildPendingReplayPlan(100)
	require.NoError(t, err)
	require.Equal(t, []OperationID{first, second}, []OperationID{plan.Operations[0].OpID, plan.Operations[1].OpID})
	holder.MarkAppliedIDs(first)
	require.Equal(t, 1, holder.Pending())
}

func TestHolderBuildPendingReplayPlanLimitKeepsLaterOperationsPending(t *testing.T) {
	holder := newTestHolder(t)
	first := opID("client-a", 1)
	second := opID("client-b", 1)
	third := opID("client-c", 1)

	_, _, err := holder.Submit(context.Background(), first, opWithValueWrites("a", "v1"))
	require.NoError(t, err)
	_, _, err = holder.Submit(context.Background(), second, opWithValueWrites("b", "v2"))
	require.NoError(t, err)
	_, _, err = holder.Submit(context.Background(), third, opWithValueWrites("c", "v3"))
	require.NoError(t, err)

	plan, _, err := holder.BuildPendingReplayPlanLimit(100, 2)
	require.NoError(t, err)
	require.Equal(t, []OperationID{first, second}, []OperationID{plan.Operations[0].OpID, plan.Operations[1].OpID})
	require.Equal(t, ReplayVersionRange{First: 100, Count: 2}, plan.Versions)

	require.NoError(t, holder.MarkReplayPlanApplied(plan))
	require.Equal(t, 1, holder.Pending())
	remaining := holder.PendingIDs()
	require.Equal(t, []OperationID{third}, remaining)
}

func TestHolderBuildPendingReplayPlanForScopeFiltersDisjointAuthority(t *testing.T) {
	holder := newTestHolder(t)
	first := opID("client-a", 1)
	second := opID("client-b", 1)
	firstOp := testGeneratedCreateOpForInodes(t, 10, 20, "a")
	secondOp := testGeneratedCreateOpForInodes(t, 30, 40, "b")
	firstScope := firstOp.Delta.Authority

	_, _, err := holder.Submit(context.Background(), first, firstOp)
	require.NoError(t, err)
	_, _, err = holder.Submit(context.Background(), second, secondOp)
	require.NoError(t, err)

	plan, scope, ok, err := holder.BuildPendingReplayPlanForScope(100, firstScope)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, firstScope, scope)
	require.Equal(t, []OperationID{first}, []OperationID{plan.Operations[0].OpID})
	require.Equal(t, ReplayVersionRange{First: 100, Count: 1}, plan.Versions)

	require.NoError(t, holder.MarkReplayPlanApplied(plan))
	require.Equal(t, 1, holder.Pending())

	_, _, ok, err = holder.BuildPendingReplayPlanForScope(0, compile.AuthorityScope{
		Mount:      "vol",
		MountKeyID: 1,
		Parents:    []fsmeta.InodeID{99},
	})
	require.NoError(t, err)
	require.False(t, ok)
}

func TestHolderBuildReplayPlanAndMarkApplied(t *testing.T) {
	holder, err := NewHolder(HolderConfig{
		EpochID:  1,
		HolderID: "holder-a",
	})
	require.NoError(t, err)

	first := opID("client-a", 1)
	second := opID("client-b", 1)
	_, _, err = holder.Submit(context.Background(), first, opWithValueWrites("a", "v1"))
	require.NoError(t, err)
	_, _, err = holder.Submit(context.Background(), second, opWithValueWrites("a", "v2"))
	require.NoError(t, err)
	require.Equal(t, 2, holder.Pending())

	plan, _, err := holder.BuildPendingReplayPlan(100)
	require.NoError(t, err)
	require.Equal(t, uint64(1), plan.EpochID)
	require.Equal(t, ReplayVersionRange{First: 100, Count: 2}, plan.Versions)
	require.Equal(t, 2, len(plan.Operations))
	require.Equal(t, 2, holder.Pending(), "building a segment plan must not release the fence before apply")

	require.NoError(t, holder.MarkReplayPlanApplied(plan))
	require.Zero(t, holder.Pending())
}

func TestHolderRejectsIneligibleOperation(t *testing.T) {
	holder := newTestHolder(t)
	op := opWithValueWrites("a", "v1")
	op.Delta.Eligibility = compile.EligibilitySlowPath

	_, _, err := holder.Submit(context.Background(), opID("client-a", 1), op)
	require.ErrorIs(t, err, ErrIneligibleOperation)
}

func TestHolderAcceptsCrossBucketDelta(t *testing.T) {
	holder := newTestHolder(t)
	mount := fsmeta.MountIdentity{MountID: "vol", MountKeyID: 1}
	parent := inodeForBucket(t, 1)
	inode := inodeForBucket(t, 2)
	op := testGeneratedCreateOpForInodes(t, parent, inode, "cross-bucket")
	require.Equal(t, mount.MountKeyID, op.Authority.Scope.MountKeyID)
	require.Len(t, op.Authority.Scope.Buckets, 2)

	_, _, err := holder.Submit(context.Background(), opID("client-a", 1), op)
	require.NoError(t, err)
	require.Equal(t, 1, holder.Pending())
}

func BenchmarkHolderSubmitDisjoint(b *testing.B) {
	holder := mustHolderForBench(b)
	ctx := context.Background()
	delta := opWithValueWrites("bench-key", "value")

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		id := OperationID{ClientID: "bench", Seq: uint64(i + 1)}
		if _, _, err := holder.Submit(ctx, id, delta); err != nil {
			b.Fatal(err)
		}
		holder.MarkAppliedIDs(id)
	}
}

type fakeWitnessReplica struct {
	mu       sync.Mutex
	id       string
	segments []SegmentWitnessRecord
}

func newFakeWitnessReplica(id string) *fakeWitnessReplica {
	return &fakeWitnessReplica{id: id}
}

func (r *fakeWitnessReplica) ID() string {
	return r.id
}

func (r *fakeWitnessReplica) AppendSegments(_ context.Context, _ compile.AuthorityScope, records []SegmentWitnessRecord) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.segments = append(r.segments, records...)
	return nil
}

func (r *fakeWitnessReplica) Probe(context.Context, uint64) (WitnessSnapshot, error) {
	return r.snapshot(), nil
}

func (r *fakeWitnessReplica) snapshot() WitnessSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	segments := make([]SegmentWitnessRecord, 0, len(r.segments))
	segments = append(segments, r.segments...)
	return WitnessSnapshot{Segments: segments}
}

func newTestHolder(t *testing.T) *Holder {
	t.Helper()
	holder, err := NewHolder(HolderConfig{
		EpochID:  1,
		HolderID: "holder-a",
	})
	require.NoError(t, err)
	return holder
}

var testMount = fsmeta.MountIdentity{MountID: "vol", MountKeyID: 1}

func testGeneratedCreateOp(tb testing.TB, name, value string, opts ...compile.Option) compile.MaterializedOp {
	tb.Helper()
	op, err := generatedCreateOp(name, value, opts...)
	if err != nil {
		tb.Fatal(err)
	}
	return op
}

func generatedCreateOp(name, value string, opts ...compile.Option) (compile.MaterializedOp, error) {
	op, err := generatedCreateIntentOp(name, value, opts...)
	if err != nil {
		return compile.MaterializedOp{}, err
	}
	return sealTestMaterializedOp(op), nil
}

func generatedCreateIntentOp(name, value string, opts ...compile.Option) (compile.MaterializedOp, error) {
	name = strings.NewReplacer("/", "-", "\x00", "-").Replace(name)
	if name == "" || name == "." || name == ".." {
		name = "entry"
	}
	inodeID := fsmeta.InodeID(100)
	for _, ch := range name + value {
		inodeID += fsmeta.InodeID(ch)
	}
	program, err := compile.CompileCreateProgram(fsmeta.CreateRequest{
		Mount:  testMount.MountID,
		Parent: fsmeta.RootInode,
		Name:   name,
		Attrs: fsmeta.CreateAttrs{
			Type: fsmeta.InodeTypeFile,
			Size: uint64(len(value)),
			Mode: 0o644,
		},
	}, testMount, inodeID, opts...)
	if err != nil {
		return compile.MaterializedOp{}, err
	}
	op, err := materializeGeneratedCreate(program, fsmeta.RootInode)
	if err != nil {
		return compile.MaterializedOp{}, err
	}
	return compile.WithPredicateProofs(op, testPredicateProofsForMaterializedOp(op)), nil
}

func testGeneratedCreateOpForInodes(tb testing.TB, parent, inode fsmeta.InodeID, name string) compile.MaterializedOp {
	tb.Helper()
	program, err := compile.CompileCreateProgram(fsmeta.CreateRequest{
		Mount:  testMount.MountID,
		Parent: parent,
		Name:   name,
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile, Mode: 0o644},
	}, testMount, inode)
	if err != nil {
		tb.Fatal(err)
	}
	op, err := materializeGeneratedCreate(program, parent)
	if err != nil {
		tb.Fatal(err)
	}
	return sealTestMaterializedOp(op)
}

func materializeGeneratedCreate(program compile.CreateProgram, parent fsmeta.InodeID) (compile.MaterializedOp, error) {
	parentValue, err := fsmeta.EncodeInodeValue(fsmeta.InodeRecord{
		Inode:      parent,
		Type:       fsmeta.InodeTypeDirectory,
		LinkCount:  1,
		ChildCount: 1,
	})
	if err != nil {
		return compile.MaterializedOp{}, err
	}
	return compile.MaterializeCreate(program, compile.CreateValues{
		ParentInodeValue: parentValue,
		DentryValue:      program.Compiled.Delta.WriteEffects[1].Value,
		InodeValue:       program.Compiled.Delta.WriteEffects[2].Value,
	})
}

func opWithValueWrites(key, value string) compile.MaterializedOp {
	op, err := generatedCreateOp(key, value)
	if err != nil {
		panic(err)
	}
	return op
}

func sealTestMaterializedOp(op compile.MaterializedOp) compile.MaterializedOp {
	proofs := testPredicateProofsForMaterializedOp(op)
	guardProofs, err := compile.GuardProofsFor(op.CompiledOp, proofs, op.Delta.RuntimeGuards)
	if err != nil {
		panic(err)
	}
	return compile.WithAdmissionProofs(op, proofs, guardProofs)
}

func testPredicateProofsForMaterializedOp(op compile.MaterializedOp) []proof.PredicateProof {
	if len(op.Delta.ReadPredicates) == 0 {
		return nil
	}
	proofs := make([]proof.PredicateProof, 0, len(op.Delta.ReadPredicates))
	seen := make(map[string]struct{}, len(op.Delta.ReadPredicates))
	for _, predicate := range op.Delta.ReadPredicates {
		if _, ok := seen[string(predicate.Key)]; ok {
			continue
		}
		seen[string(predicate.Key)] = struct{}{}
		frontier := proof.ProofFrontier{EpochID: 1, Sequence: 1}
		switch predicate.Kind {
		case compile.PredicateExists:
			proofs = append(proofs, proof.NewPredicateProof(predicate.Key, nil, true, 0, proof.ReadSourceOverlay, frontier))
		case compile.PredicateNotExists:
			proofs = append(proofs, proof.NewPredicateProof(predicate.Key, nil, false, 0, proof.ReadSourceOverlay, frontier))
		case compile.PredicateObservedValue:
			proofs = append(proofs, proof.NewPredicateProof(predicate.Key, predicate.ExpectedValue, true, 0, proof.ReadSourceOverlay, frontier))
		}
	}
	return proofs
}

func inodeForBucket(t *testing.T, bucket fsmeta.AffinityBucket) fsmeta.InodeID {
	t.Helper()
	for inode := fsmeta.InodeID(2); inode < 100_000; inode++ {
		if fsmeta.BucketForInodeID(inode) == bucket {
			return inode
		}
	}
	t.Fatalf("no inode found for bucket %d", bucket)
	return 0
}

func mustHolderForBench(b *testing.B) *Holder {
	b.Helper()
	holder, err := NewHolder(HolderConfig{
		EpochID:  1,
		HolderID: "holder-a",
	})
	if err != nil {
		b.Fatal(err)
	}
	return holder
}
