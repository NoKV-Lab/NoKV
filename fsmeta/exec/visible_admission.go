// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"time"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/feichai0017/NoKV/fsmeta/proof"
)

func (e *Executor) admitVisibleAuthority(ctx context.Context, delta compile.SemanticDelta) error {
	if e == nil || e.visibleAuthority == nil {
		return nil
	}
	if delta.Eligibility != compile.EligibilityVisibleCommit {
		e.visibleAdmission.recordSlow(delta.SlowReason)
		return nil
	}
	e.visibleAdmission.eligibleTotal.Add(1)
	if e.visibleCommitter != nil {
		return nil
	}
	e.visibleAdmission.acquireTotal.Add(1)
	owned, err := e.visibleAuthority.AcquireVisibleAuthority(ctx, delta.Authority)
	if err != nil {
		e.visibleAdmission.errorTotal.Add(1)
		return nil
	}
	if !owned {
		e.visibleAdmission.heldTotal.Add(1)
		return nil
	}
	e.visibleAdmission.ownedTotal.Add(1)
	return nil
}

func (e *Executor) tryVisibleCommit(ctx context.Context, op compile.MaterializedOp) (bool, error) {
	if e == nil || e.visibleCommitter == nil {
		return false, nil
	}
	if e.visibleAuthority == nil {
		e.visibleCommit.skipNoAuthorityTotal.Add(1)
		return false, nil
	}
	delta := op.Delta
	if delta.Eligibility != compile.EligibilityVisibleCommit {
		e.visibleCommit.skipIneligibleTotal.Add(1)
		return false, nil
	}
	if op.Placement.RequiresMaterialize {
		e.visibleCommit.skipNonConcreteTotal.Add(1)
		return false, nil
	}
	if !op.Placement.CanSegment {
		e.visibleCommit.skipPlacementTotal.Add(1)
		return false, nil
	}
	id := e.nextVisibleOperationID(delta.Kind)
	e.visibleCommit.attemptTotal.Add(1)
	start := time.Now()
	_, err := e.visibleCommitter.SubmitVisible(ctx, id, op, e.visiblePredicatesHold)
	latency := uint64(time.Since(start).Nanoseconds())
	e.visibleCommit.latencyTotalNanosecond.Add(latency)
	recordUint64Max(&e.visibleCommit.latencyMaxNanosecond, latency)
	if err != nil {
		if errors.Is(err, ErrVisibleAdmissionRejected) ||
			errors.Is(err, ErrVisibleIneligibleOperation) ||
			errors.Is(err, errVisibleAuthorityNotHeld) ||
			nokverrors.KindOf(err) == nokverrors.KindNotLeader {
			e.visibleCommit.skipPredicateTotal.Add(1)
			return false, nil
		}
		if isVisibleAdmissionTerminalError(err) {
			e.visibleCommit.skipPredicateTotal.Add(1)
			return true, err
		}
		e.visibleCommit.errorTotal.Add(1)
		return true, err
	}
	e.visibleCommit.successTotal.Add(1)
	return true, nil
}

func (e *Executor) tryVisibleCommitAfterRead(ctx context.Context, view *visibleReadView, op compile.MaterializedOp) (bool, error) {
	committed, err := e.tryVisibleCommit(ctx, op)
	if err != nil || committed {
		return committed, err
	}
	if view.observedVisibleOverlay() {
		return false, errVisibleOverlayFallbackUnsafe
	}
	return false, nil
}

func (e *Executor) visiblePredicatesHold(ctx context.Context, op compile.MaterializedOp, admissionCtx VisibleAdmissionContext) (VisibleAdmissionResult, bool, error) {
	delta := op.Delta
	if !visiblePredicateProofsValid(op.PredicateProofs) {
		return VisibleAdmissionResult{}, false, nil
	}
	frontier := admissionCtx.ProofFrontier
	proofs := make([]proof.PredicateProof, 0, len(delta.ReadPredicates))
	proofsByKey := make(map[string]int, len(delta.ReadPredicates))
	appendProof := func(predicateProof proof.PredicateProof) {
		key := string(predicateProof.Key)
		if index, ok := proofsByKey[key]; ok {
			proofs[index] = predicateProof
			return
		}
		proofsByKey[key] = len(proofs)
		proofs = append(proofs, predicateProof)
	}
	if len(delta.ReadPredicates) == 0 {
		return e.visibleAdmissionResult(op, proofs)
	}
	index := e.visiblePredicateIndex()
	var version uint64
	var haveVersion bool
	read := func(key []byte) ([]byte, bool, proof.ReadSource, uint64, error) {
		if value, deleted, ok := e.visibleOverlayGet(key); ok {
			if deleted {
				return nil, false, proof.ReadSourceOverlay, 0, nil
			}
			return value, true, proof.ReadSourceOverlay, 0, nil
		}
		if !haveVersion {
			var err error
			version, err = e.reserveReadVersion(ctx)
			if err != nil {
				return nil, false, proof.ReadSourceUnknown, 0, err
			}
			haveVersion = true
		}
		value, ok, err := e.runner.Get(ctx, key, version)
		return value, ok, proof.ReadSourceBase, version, err
	}
	for _, predicate := range delta.ReadPredicates {
		switch predicate.Kind {
		case compile.PredicateExists:
			if index != nil {
				present, known := index.KeyState(predicate.Key)
				if known {
					if !present {
						return VisibleAdmissionResult{}, false, model.ErrNotFound
					}
					appendProof(proof.NewPredicateProof(predicate.Key, nil, true, 0, proof.ReadSourceOverlay, frontier))
					continue
				}
			}
			value, ok, source, proofVersion, err := read(predicate.Key)
			if err != nil {
				return VisibleAdmissionResult{}, false, err
			}
			if !ok {
				return VisibleAdmissionResult{}, false, model.ErrNotFound
			}
			appendProof(proof.NewPredicateProof(predicate.Key, value, true, proofVersion, source, proofFrontierForSource(source, frontier)))
		case compile.PredicateNotExists:
			if index != nil {
				present, known := index.KeyState(predicate.Key)
				if known {
					if present {
						return VisibleAdmissionResult{}, false, model.ErrExists
					}
					appendProof(proof.NewPredicateProof(predicate.Key, nil, false, 0, proof.ReadSourceOverlay, frontier))
					continue
				}
				if e.visibleNotExistsKnown(delta.Authority, predicate.Key, index) ||
					visibleNotExistsDerivedFromDelta(delta, predicate, index) {
					appendProof(proof.NewPredicateProof(predicate.Key, nil, false, 0, proof.ReadSourceOverlay, frontier))
					continue
				}
			}
			_, ok, source, proofVersion, err := read(predicate.Key)
			if err != nil {
				return VisibleAdmissionResult{}, false, err
			}
			if ok {
				return VisibleAdmissionResult{}, false, model.ErrExists
			}
			appendProof(proof.NewPredicateProof(predicate.Key, nil, false, proofVersion, source, proofFrontierForSource(source, frontier)))
		case compile.PredicateObservedValue:
			if !predicate.HasExpectedValue {
				return VisibleAdmissionResult{}, false, nil
			}
			if value, deleted, ok := e.visibleOverlayGet(predicate.Key); ok {
				if deleted || !bytes.Equal(value, predicate.ExpectedValue) {
					return VisibleAdmissionResult{}, false, nil
				}
				appendProof(proof.NewPredicateProof(predicate.Key, value, true, 0, proof.ReadSourceOverlay, frontier))
				continue
			}
			value, ok, source, proofVersion, err := read(predicate.Key)
			if err != nil {
				return VisibleAdmissionResult{}, false, err
			}
			if !ok || !bytes.Equal(value, predicate.ExpectedValue) {
				return VisibleAdmissionResult{}, false, nil
			}
			appendProof(proof.NewPredicateProof(predicate.Key, value, true, proofVersion, source, proofFrontierForSource(source, frontier)))
		case compile.PredicatePrefixScan:
			return VisibleAdmissionResult{}, false, nil
		default:
			return VisibleAdmissionResult{}, false, nil
		}
	}
	return e.visibleAdmissionResult(op, proofs)
}

func (e *Executor) visibleAdmissionResult(op compile.MaterializedOp, proofs []proof.PredicateProof) (VisibleAdmissionResult, bool, error) {
	guardProofs, err := compile.GuardProofsFor(op.CompiledOp, proofs, op.Delta.RuntimeGuards)
	if err != nil {
		return VisibleAdmissionResult{}, false, nil
	}
	return VisibleAdmissionResult{
		PredicateProofs: proofs,
		GuardProofs:     guardProofs,
	}, true, nil
}

func proofFrontierForSource(source proof.ReadSource, frontier proof.ProofFrontier) proof.ProofFrontier {
	if source == proof.ReadSourceOverlay {
		return frontier
	}
	return proof.ProofFrontier{}
}

func visiblePredicateProofsValid(proofs []proof.PredicateProof) bool {
	for _, predicateProof := range proofs {
		if err := proof.VerifyPredicateProof(predicateProof); err != nil {
			return false
		}
	}
	return true
}

func (e *Executor) visiblePredicateIndex() VisiblePredicateIndex {
	if e == nil || e.visibleCommitter == nil {
		return nil
	}
	index, ok := e.visibleCommitter.(VisiblePredicateIndex)
	if !ok {
		return nil
	}
	return index
}

func (e *Executor) rememberVisibleCreate(mount model.MountIdentity, plan layout.OperationPlan, inode model.InodeRecord) {
	index := e.visiblePredicateIndex()
	if index == nil {
		return
	}
	if len(plan.MutateKeys) > 1 {
		index.RememberKey(plan.MutateKeys[1], true)
	}
	if len(plan.MutateKeys) > 2 {
		index.RememberKey(plan.MutateKeys[2], true)
	}
	if inode.Type == model.InodeTypeDirectory {
		index.RememberEmptyDirectory(mount, inode.Inode)
		return
	}
	ownerKey, err := layout.EncodeInodeSessionKey(mount, inode.Inode)
	if err == nil {
		index.RememberKey(ownerKey, false)
	}
	index.RememberEmptySessionNamespace(mount, inode.Inode)
}

type visibleEmptyDirectoryForgetter interface {
	ForgetEmptyDirectory(mount model.MountIdentity, inode model.InodeID)
}

func (e *Executor) forgetVisibleEmptyDirectory(mount model.MountIdentity, inode model.InodeID) {
	index := e.visiblePredicateIndex()
	if index == nil {
		return
	}
	forgetter, ok := index.(visibleEmptyDirectoryForgetter)
	if !ok {
		return
	}
	forgetter.ForgetEmptyDirectory(mount, inode)
}

func visibleDeltaAllowsAbsentObservedValue(delta compile.SemanticDelta) bool {
	return slices.Contains(delta.RuntimeGuards, compile.GuardExpiredSessionOwner)
}

func visibleNotExistsDerivedFromDelta(delta compile.SemanticDelta, predicate compile.Predicate, index VisiblePredicateIndex) bool {
	if delta.Kind != model.OperationCreate || len(delta.Plan.MutateKeys) < 3 {
		return false
	}
	if bytes.Equal(predicate.Key, delta.Plan.MutateKeys[2]) {
		return true
	}
	if !bytes.Equal(predicate.Key, delta.Plan.MutateKeys[1]) || len(delta.Authority.Parents) != 1 {
		return false
	}
	return index.DirectoryBaseEmpty(model.MountIdentity{
		MountID:    delta.Authority.Mount,
		MountKeyID: delta.Authority.MountKeyID,
	}, delta.Authority.Parents[0])
}

func (e *Executor) visibleNotExistsKnown(scope compile.AuthorityScope, key []byte, index VisiblePredicateIndex) bool {
	if index == nil || len(key) == 0 || scope.Mount == "" || scope.MountKeyID == 0 {
		return false
	}
	present, known := index.KeyState(key)
	if known {
		return !present
	}
	parts, ok := layout.InspectKey(key)
	if !ok || parts.MountKeyID != scope.MountKeyID {
		return false
	}
	if parts.Kind == layout.KeyKindSession {
		return index.SessionNamespaceEmpty(model.MountIdentity{
			MountID:    scope.Mount,
			MountKeyID: scope.MountKeyID,
		}, parts.Inode)
	}
	if parts.Kind != layout.KeyKindDentry {
		return false
	}
	return index.DirectoryEmpty(model.MountIdentity{
		MountID:    scope.Mount,
		MountKeyID: scope.MountKeyID,
	}, parts.Parent)
}

func isVisibleAdmissionTerminalError(err error) bool {
	return errors.Is(err, model.ErrExists) ||
		errors.Is(err, model.ErrNotFound) ||
		errors.Is(err, model.ErrInvalidRequest) ||
		errors.Is(err, model.ErrInvalidValue)
}
