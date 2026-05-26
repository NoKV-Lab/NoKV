// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package compile

import (
	"crypto/sha256"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/feichai0017/NoKV/fsmeta/proof"
	"github.com/stretchr/testify/require"
)

func TestMaterializedOpValidationRequiresAbsentProof(t *testing.T) {
	op := materializedCreateForValidation(t, 44)
	var validationErr ValidationError
	err := op.ValidateForAdmissionIntent()
	require.ErrorAs(t, err, &validationErr)
	require.Equal(t, ValidationPredicateProofMissing, validationErr.Kind)

	parentProof := proof.NewPredicateProof(op.Delta.ReadPredicates[0].Key, op.Delta.WriteEffects[0].Value, true, 0, proof.ReadSourceOverlay, proof.ProofFrontier{EpochID: 1, Sequence: 1})
	op = WithPredicateProofs(op, []proof.PredicateProof{parentProof})
	require.NoError(t, op.ValidateForAdmissionIntent())

	err = op.ValidateForAdmission()
	require.ErrorAs(t, err, &validationErr)
	require.Equal(t, ValidationPredicateProofMissing, validationErr.Kind)

	proofs := []proof.PredicateProof{
		parentProof,
		proof.NewPredicateProof(op.Delta.ReadPredicates[1].Key, nil, false, 0, proof.ReadSourceOverlay, proof.ProofFrontier{EpochID: 1, Sequence: 1}),
		proof.NewPredicateProof(op.Delta.ReadPredicates[2].Key, nil, false, 0, proof.ReadSourceOverlay, proof.ProofFrontier{EpochID: 1, Sequence: 1}),
	}
	op = WithPredicateProofs(op, proofs)
	require.NoError(t, op.ValidateForAdmission())
}

func TestPredicateProofCarriesAbsenceProofClass(t *testing.T) {
	op := materializedCreateForValidation(t, 44)

	parent := proof.NewPredicateProof(op.Delta.ReadPredicates[0].Key, op.Delta.WriteEffects[0].Value, true, 0, proof.ReadSourceOverlay, proof.ProofFrontier{EpochID: 7, Sequence: 9})
	overlay := proof.NewPredicateProof(op.Delta.ReadPredicates[1].Key, nil, false, 0, proof.ReadSourceOverlay, proof.ProofFrontier{EpochID: 7, Sequence: 9})
	require.Equal(t, proof.PredicateProofOverlayFrontierAbsence, overlay.ProofKind)
	require.NotEqual(t, [32]byte{}, overlay.ScopeDigest)

	base := proof.NewPredicateProof(op.Delta.ReadPredicates[2].Key, nil, false, 11, proof.ReadSourceBase, proof.ProofFrontier{})
	require.Equal(t, proof.PredicateProofPointAbsence, base.ProofKind)
	require.NotEqual(t, [32]byte{}, base.ScopeDigest)

	op = WithPredicateProofs(op, []proof.PredicateProof{parent, overlay, base})
	require.NoError(t, op.ValidateForAdmission())

	base.ScopeDigest[0] ^= 0xff
	op = WithPredicateProofs(op, []proof.PredicateProof{parent, overlay, base})
	var validationErr ValidationError
	err := op.ValidateForAdmission()
	require.ErrorAs(t, err, &validationErr)
	require.Equal(t, ValidationPredicateProofMismatch, validationErr.Kind)
}

func TestMaterializedOpValidationBindsGuardProofEvidence(t *testing.T) {
	delta, proofs := testConcreteUpdateInodeDelta(t, nil)
	op := testMaterializeAOT(t, delta, proofs)
	wrongEvidence := proof.GuardEvidence{SchemaVersion: proof.Version1}
	op = WithGuardProofs(op, []proof.GuardProof{GuardProofFor(op.Delta.RuntimeGuards[0], true, wrongEvidence)})

	var validationErr ValidationError
	err := op.ValidateForAdmission()
	require.ErrorAs(t, err, &validationErr)
	require.Equal(t, ValidationGuardProofMismatch, validationErr.Kind)

	op = testMaterializeAOT(t, delta, proofs)
	op = WithGuardProofs(op, testGuardProofsFor(op))
	require.NoError(t, VerifyGuardProof(op.CompiledOp, op.PredicateProofs, op.Guards[0], op.GuardProofs[0]))
	require.NoError(t, op.ValidateForAdmission())
}

func TestMaterializedOpValidationRejectsReplayDigestDrift(t *testing.T) {
	op := materializedCreateForValidation(t, 44)
	op.ReplayDigest[0] ^= 0xff

	var validationErr ValidationError
	err := op.ValidateForAdmission()
	require.ErrorAs(t, err, &validationErr)
	require.Equal(t, ValidationCanonicalMismatch, validationErr.Kind)
}

func TestObservedValuePredicateCompilesExactProofObligation(t *testing.T) {
	expected, err := layout.EncodeInodeValue(model.InodeRecord{Inode: 44, Type: model.InodeTypeFile, LinkCount: 1})
	require.NoError(t, err)
	delta, _ := testConcreteUpdateInodeDelta(t, expected)

	op := testCompileAOT(t, delta)
	require.Len(t, op.Predicates, 2)
	require.True(t, op.Predicates[1].NeedValue)
	require.True(t, op.Predicates[1].HasExpectedValue)
	require.Equal(t, sha256.Sum256(expected), op.Predicates[1].ExpectHash)
}

func TestMaterializedOpValidationRejectsUncoveredWrite(t *testing.T) {
	op := materializedCreateForValidation(t, 44)
	badScope := op.Authority.Scope
	badScope.Inodes = []model.InodeID{55}
	op.Delta.Authority = badScope
	op.Authority.Scope = badScope

	var validationErr ValidationError
	err := op.ValidateForAdmission()
	require.ErrorAs(t, err, &validationErr)
	require.Equal(t, ValidationAuthorityMismatch, validationErr.Kind)
}

func TestMaterializedOpValidationRejectsNonCanonicalDescriptor(t *testing.T) {
	op := materializedCreateForValidation(t, 44)
	op.Placement.CanSegment = false

	var validationErr ValidationError
	err := op.ValidateForAdmission()
	require.ErrorAs(t, err, &validationErr)
	require.Equal(t, ValidationCanonicalMismatch, validationErr.Kind)
}

func materializedCreateForValidation(t *testing.T, inode model.InodeID) MaterializedOp {
	t.Helper()
	req := model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	}
	program, err := CompileCreateProgram(req, testMount, inode)
	require.NoError(t, err)
	parentValue, err := layout.EncodeInodeValue(model.InodeRecord{
		Inode:      model.RootInode,
		Type:       model.InodeTypeDirectory,
		LinkCount:  1,
		ChildCount: 1,
	})
	require.NoError(t, err)
	op, err := MaterializeCreate(program, CreateValues{
		ParentInodeValue: parentValue,
		DentryValue:      program.Compiled.Delta.WriteEffects[1].Value,
		InodeValue:       program.Compiled.Delta.WriteEffects[2].Value,
	})
	require.NoError(t, err)
	return op
}

func TestMaterializedOpValidationRequiresObservedValueProof(t *testing.T) {
	delta, proofs := testConcreteUpdateInodeDelta(t, nil)

	var validationErr ValidationError
	err := testMaterializeAOT(t, delta, nil).ValidateForAdmission()
	require.ErrorAs(t, err, &validationErr)
	require.Equal(t, ValidationPredicateProofMissing, validationErr.Kind)

	op := testMaterializeAOT(t, delta, proofs)
	op = WithGuardProofs(op, testGuardProofsFor(op))
	require.NoError(t, op.ValidateForAdmission())
}

func TestMaterializedOpValidationRejectsBadPredicateProofContract(t *testing.T) {
	delta, proofs := testConcreteUpdateInodeDelta(t, nil)
	badProof := proof.PredicateProof{
		Key:     proofs[1].Key,
		Present: proofs[1].Present,
		Value:   proofs[1].Value,
		Source:  proof.ReadSourceUnknown,
	}
	badProof.Digest = proof.PredicateProofDigest(badProof.Key, badProof.Value, badProof.Present, badProof.Version, badProof.Source, badProof.ProofFrontier)

	var validationErr ValidationError
	err := testMaterializeAOT(t, delta, []proof.PredicateProof{proofs[0], badProof}).ValidateForAdmission()
	require.ErrorAs(t, err, &validationErr)
	require.Equal(t, ValidationPredicateProofMismatch, validationErr.Kind)

	duplicateProof := proofs[1]
	err = testMaterializeAOT(t, delta, []proof.PredicateProof{proofs[0], duplicateProof, duplicateProof}).ValidateForAdmission()
	require.ErrorAs(t, err, &validationErr)
	require.Equal(t, ValidationPredicateProofMismatch, validationErr.Kind)
}

func TestMaterializedOpValidationRequiresGuardProof(t *testing.T) {
	delta, proofs := testConcreteUpdateInodeDelta(t, nil)

	var validationErr ValidationError
	err := testMaterializeAOT(t, delta, proofs).ValidateForAdmission()
	require.ErrorAs(t, err, &validationErr)
	require.Equal(t, ValidationGuardProofMissing, validationErr.Kind)

	op := testMaterializeAOT(t, delta, proofs)
	op = WithGuardProofs(op, testGuardProofsFor(op))
	require.NoError(t, op.ValidateForAdmission())
}
