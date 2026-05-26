// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package compile

import (
	"bytes"
	"crypto/sha256"
	"slices"

	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/proof"
)

type ValidationErrorKind uint8

const (
	ValidationOK ValidationErrorKind = iota
	ValidationMissingConcreteEffects
	ValidationAuthorityMismatch
	ValidationPredicateProofMissing
	ValidationPredicateProofMismatch
	ValidationGuardProofMissing
	ValidationGuardProofMismatch
	ValidationPlacementMismatch
	ValidationCanonicalMismatch
)

func (k ValidationErrorKind) String() string {
	switch k {
	case ValidationMissingConcreteEffects:
		return "missing concrete effects"
	case ValidationAuthorityMismatch:
		return "authority mismatch"
	case ValidationPredicateProofMissing:
		return "predicate proof missing"
	case ValidationPredicateProofMismatch:
		return "predicate proof mismatch"
	case ValidationGuardProofMissing:
		return "guard proof missing"
	case ValidationGuardProofMismatch:
		return "guard proof mismatch"
	case ValidationPlacementMismatch:
		return "placement mismatch"
	case ValidationCanonicalMismatch:
		return "canonical mismatch"
	default:
		return "ok"
	}
}

type ValidationError struct {
	Kind ValidationErrorKind
	Key  []byte
}

func (e ValidationError) Error() string {
	return "invalid materialized metadata operation: " + e.Kind.String()
}

func (op MaterializedOp) ValidateForAdmissionIntent() error {
	return op.validateForAdmission(false)
}

func (op MaterializedOp) ValidateForAdmission() error {
	return op.validateForAdmission(true)
}

func (op MaterializedOp) validateForAdmission(requireGuardProofs bool) error {
	if !materializedOpIsCanonical(op) {
		return ValidationError{Kind: ValidationCanonicalMismatch}
	}
	if op.Delta.Eligibility != EligibilityVisibleCommit ||
		!op.Placement.CanSegment ||
		op.Placement.RequiresMaterialize ||
		op.Footprint.HasPrefixRead {
		return ValidationError{Kind: ValidationPlacementMismatch}
	}
	if len(op.Effects) == 0 {
		return ValidationError{Kind: ValidationMissingConcreteEffects}
	}
	for _, effect := range op.Effects {
		if !effect.Concrete || len(effect.Key) == 0 {
			return ValidationError{Kind: ValidationMissingConcreteEffects, Key: effect.Key}
		}
		switch effect.Kind {
		case EffectPut:
			if effect.Value == nil {
				return ValidationError{Kind: ValidationMissingConcreteEffects, Key: effect.Key}
			}
		case EffectDelete:
		default:
			return ValidationError{Kind: ValidationMissingConcreteEffects, Key: effect.Key}
		}
		if !authorityScopeCoversKey(op.Authority.Scope, keyRef(KeyAccessWrite, effect.Key)) {
			return ValidationError{Kind: ValidationAuthorityMismatch, Key: effect.Key}
		}
	}
	for _, ref := range op.Footprint.Reads {
		if !authorityScopeCoversKey(op.Authority.Scope, ref) {
			return ValidationError{Kind: ValidationAuthorityMismatch, Key: ref.Key}
		}
	}
	if !authorityScopeCoversBuckets(op.Authority.Scope, op.Placement.Buckets) {
		return ValidationError{Kind: ValidationPlacementMismatch}
	}
	proofs, err := predicateProofMap(op.PredicateProofs)
	if err != nil {
		return err
	}
	for _, obligation := range op.Predicates {
		requireProof := obligation.Kind == PredicateObservedValue
		if requireGuardProofs && (obligation.NeedValue || obligation.NeedAbsent) {
			requireProof = true
		}
		if !requireProof {
			continue
		}
		proof, ok := proofs[string(obligation.Key)]
		if !ok {
			return ValidationError{Kind: ValidationPredicateProofMissing, Key: obligation.Key}
		}
		if !predicateProofMatches(obligation, proof) {
			return ValidationError{Kind: ValidationPredicateProofMismatch, Key: obligation.Key}
		}
		if requireGuardProofs && obligation.NeedAbsent && !durableAbsenceProof(proof) {
			return ValidationError{Kind: ValidationPredicateProofMismatch, Key: obligation.Key}
		}
	}
	guardProofs, err := guardProofMap(op.GuardProofs)
	if err != nil {
		return err
	}
	for _, obligation := range op.Guards {
		proof, ok := guardProofs[obligation.Guard]
		if !ok {
			if !requireGuardProofs {
				continue
			}
			return ValidationError{Kind: ValidationGuardProofMissing}
		}
		if err := VerifyGuardProof(op.CompiledOp, op.PredicateProofs, obligation, proof); err != nil {
			return ValidationError{Kind: ValidationGuardProofMismatch}
		}
	}
	return nil
}

func materializedOpIsCanonical(op MaterializedOp) bool {
	canonical, err := compileAOTDelta(op.Delta)
	if err != nil {
		return false
	}
	if op.DescriptorDigest != canonical.DescriptorDigest ||
		op.ReplayDigest != canonical.DescriptorDigest {
		return false
	}
	return authorityPlanEqual(op.Authority, canonical.Authority) &&
		placementPlanEqual(op.Placement, canonical.Placement) &&
		keyFootprintEqual(op.Footprint, canonical.Footprint) &&
		predicateObligationsEqual(op.Predicates, canonical.Predicates) &&
		guardObligationsEqual(op.Guards, canonical.Guards) &&
		effectPlansEqual(op.Effects, canonical.Effects) &&
		atomicityGroupEqual(op.Atomicity, canonical.Atomicity) &&
		op.Durability == canonical.Durability &&
		watchProjectionsEqual(op.Watch, canonical.Watch) &&
		op.Completion == canonical.Completion &&
		op.Segment == canonical.Segment
}

func authorityPlanEqual(left, right AuthorityPlan) bool {
	return left.Required == right.Required &&
		left.Fence == right.Fence &&
		authorityScopeEqual(left.Scope, right.Scope)
}

func authorityScopeEqual(left, right AuthorityScope) bool {
	return left.Mount == right.Mount &&
		left.MountKeyID == right.MountKeyID &&
		left.Broad == right.Broad &&
		left.AllowOpaqueKeys == right.AllowOpaqueKeys &&
		slices.Equal(left.Buckets, right.Buckets) &&
		slices.Equal(left.Parents, right.Parents) &&
		slices.Equal(left.Inodes, right.Inodes)
}

func placementPlanEqual(left, right PlacementPlan) bool {
	return left.MountKeyID == right.MountKeyID &&
		left.SingleBucket == right.SingleBucket &&
		left.Install == right.Install &&
		left.CanSegment == right.CanSegment &&
		left.RequiresMaterialize == right.RequiresMaterialize &&
		left.SlowReason == right.SlowReason &&
		left.MergeKey == right.MergeKey &&
		slices.Equal(left.Buckets, right.Buckets)
}

func keyFootprintEqual(left, right KeyFootprint) bool {
	return left.HasPrefixRead == right.HasPrefixRead &&
		left.HasOpaqueKeys == right.HasOpaqueKeys &&
		left.EstimatedBytes == right.EstimatedBytes &&
		keyRefsEqual(left.Reads, right.Reads) &&
		keyRefsEqual(left.Writes, right.Writes) &&
		keyRefsEqual(left.ConflictKeys, right.ConflictKeys)
}

func keyRefsEqual(left, right []KeyRef) bool {
	return slices.EqualFunc(left, right, func(a, b KeyRef) bool {
		return a.Mode == b.Mode &&
			a.Opaque == b.Opaque &&
			a.MountKeyID == b.MountKeyID &&
			a.Bucket == b.Bucket &&
			a.Kind == b.Kind &&
			a.Parent == b.Parent &&
			a.Inode == b.Inode &&
			bytes.Equal(a.Key, b.Key)
	})
}

func predicateObligationsEqual(left, right []PredicateObligation) bool {
	return slices.EqualFunc(left, right, func(a, b PredicateObligation) bool {
		return a.Kind == b.Kind &&
			a.NeedValue == b.NeedValue &&
			a.NeedAbsent == b.NeedAbsent &&
			a.Guard == b.Guard &&
			a.HasExpectedValue == b.HasExpectedValue &&
			a.ExpectHash == b.ExpectHash &&
			bytes.Equal(a.Key, b.Key)
	})
}

func guardObligationsEqual(left, right []GuardObligation) bool {
	return slices.Equal(left, right)
}

func effectPlansEqual(left, right []EffectPlan) bool {
	return slices.EqualFunc(left, right, func(a, b EffectPlan) bool {
		return a.ID == b.ID &&
			a.Kind == b.Kind &&
			a.Concrete == b.Concrete &&
			a.Opaque == b.Opaque &&
			a.MountKeyID == b.MountKeyID &&
			a.Bucket == b.Bucket &&
			a.RecordKind == b.RecordKind &&
			a.ValueHash == b.ValueHash &&
			a.Derivation == b.Derivation &&
			bytes.Equal(a.Key, b.Key) &&
			bytes.Equal(a.Value, b.Value)
	})
}

func atomicityGroupEqual(left, right AtomicityGroup) bool {
	return left.Splittable == right.Splittable &&
		left.Recovery == right.Recovery &&
		left.Digest == right.Digest &&
		slices.Equal(left.Members, right.Members)
}

func watchProjectionsEqual(left, right []WatchProjection) bool {
	return slices.EqualFunc(left, right, func(a, b WatchProjection) bool {
		return a.EventKind == b.EventKind &&
			a.Parent == b.Parent &&
			a.Name == b.Name &&
			a.Inode == b.Inode &&
			a.EmitAt == b.EmitAt &&
			bytes.Equal(a.Key, b.Key)
	})
}

func predicateProofMap(proofs []proof.PredicateProof) (map[string]proof.PredicateProof, error) {
	out := make(map[string]proof.PredicateProof, len(proofs))
	for _, predicateProof := range proofs {
		if len(predicateProof.Key) == 0 {
			return nil, ValidationError{Kind: ValidationPredicateProofMismatch}
		}
		if err := proof.VerifyPredicateProof(predicateProof); err != nil {
			return nil, ValidationError{Kind: ValidationPredicateProofMismatch, Key: predicateProof.Key}
		}
		if _, ok := out[string(predicateProof.Key)]; ok {
			return nil, ValidationError{Kind: ValidationPredicateProofMismatch, Key: predicateProof.Key}
		}
		out[string(predicateProof.Key)] = predicateProof
	}
	return out, nil
}

func durableAbsenceProof(predicateProof proof.PredicateProof) bool {
	if predicateProof.Present {
		return true
	}
	switch predicateProof.Source {
	case proof.ReadSourceBase, proof.ReadSourceSegment:
		return predicateProof.ProofKind == proof.PredicateProofPointAbsence && predicateProof.Version != 0
	case proof.ReadSourceOverlay:
		return predicateProof.ProofKind == proof.PredicateProofOverlayFrontierAbsence && predicateProof.ProofFrontier.Valid()
	default:
		return false
	}
}

func predicateProofMatches(obligation PredicateObligation, predicateProof proof.PredicateProof) bool {
	if !bytes.Equal(obligation.Key, predicateProof.Key) {
		return false
	}
	if obligation.NeedValue && !predicateProof.Present {
		return false
	}
	if obligation.NeedAbsent && predicateProof.Present {
		return false
	}
	if obligation.HasExpectedValue {
		if !predicateProof.Present {
			return false
		}
		if sha256.Sum256(predicateProof.Value) != obligation.ExpectHash {
			return false
		}
	}
	return true
}

func authorityScopeCoversKey(scope AuthorityScope, ref KeyRef) bool {
	if len(ref.Key) == 0 || ref.Mode == KeyAccessReadPrefix {
		return false
	}
	if ref.Opaque {
		return scope.AllowOpaqueKeys
	}
	if scope.MountKeyID == 0 || ref.MountKeyID != scope.MountKeyID {
		return false
	}
	if !slices.Contains(scope.Buckets, ref.Bucket) {
		return false
	}
	switch ref.Kind {
	case layout.KeyKindMount:
		return true
	case layout.KeyKindDentry:
		return scope.Broad || slices.Contains(scope.Parents, ref.Parent)
	case layout.KeyKindInode, layout.KeyKindChunk, layout.KeyKindSession:
		return scope.Broad || slices.Contains(scope.Inodes, ref.Inode)
	case layout.KeyKindUsage:
		return true
	default:
		return false
	}
}

func authorityScopeCoversBuckets(scope AuthorityScope, buckets []layout.AffinityBucket) bool {
	if len(buckets) == 0 {
		return true
	}
	if scope.MountKeyID == 0 || len(scope.Buckets) == 0 {
		return scope.Broad
	}
	for _, bucket := range buckets {
		if !slices.Contains(scope.Buckets, bucket) {
			return false
		}
	}
	return true
}

func guardProofMap(proofs []proof.GuardProof) (map[RuntimeGuard]proof.GuardProof, error) {
	out := make(map[RuntimeGuard]proof.GuardProof, len(proofs))
	for _, guardProof := range proofs {
		if guardProof.SchemaVersion != proof.Version1 || guardProof.Evidence.SchemaVersion != proof.Version1 || guardProof.Guard == "" || !guardProof.Passed {
			return nil, ValidationError{Kind: ValidationGuardProofMismatch}
		}
		guard, ok := RuntimeGuardForProofRule(guardProof.Guard)
		if !ok {
			return nil, ValidationError{Kind: ValidationGuardProofMismatch}
		}
		if guardProof.Evidence.Guard != guardProof.Guard {
			return nil, ValidationError{Kind: ValidationGuardProofMismatch}
		}
		if guardProof.Digest != proof.GuardProofDigest(guardProof.Guard, guardProof.Passed, guardProof.Evidence) {
			return nil, ValidationError{Kind: ValidationGuardProofMismatch}
		}
		if _, ok := out[guard]; ok {
			return nil, ValidationError{Kind: ValidationGuardProofMismatch}
		}
		out[guard] = guardProof
	}
	return out, nil
}
