// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"sync"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/feichai0017/NoKV/fsmeta/proof"
)

type HolderConfig struct {
	EpochID  uint64
	HolderID string
}

type Holder struct {
	epochID  uint64
	holderID string
	detector *ConflictDetector

	submitMu sync.Mutex
	mu       sync.Mutex
	pending  map[OperationID]holderPendingOperation
}

func NewHolder(cfg HolderConfig) (*Holder, error) {
	if cfg.EpochID == 0 || cfg.HolderID == "" {
		return nil, ErrHolderConfigInvalid
	}
	return &Holder{
		epochID:  cfg.EpochID,
		holderID: cfg.HolderID,
		detector: NewConflictDetector(),
		pending:  make(map[OperationID]holderPendingOperation),
	}, nil
}

type VisibleAck struct {
	EpochID  uint64
	OpID     OperationID
	HolderID string
}

type holderPendingOperation struct {
	scope        compile.AuthorityScope
	intentDigest [32]byte
	op           ReplayOperation
}

func (h *Holder) Submit(ctx context.Context, id OperationID, op compile.MaterializedOp) (VisibleAck, ReplayOperation, error) {
	if h == nil || h.detector == nil {
		return VisibleAck{}, ReplayOperation{}, ErrHolderConfigInvalid
	}
	if err := ctxErr(ctx); err != nil {
		return VisibleAck{}, ReplayOperation{}, err
	}
	delta := op.Delta
	if delta.Eligibility != compile.EligibilityVisibleCommit {
		return VisibleAck{}, ReplayOperation{}, ErrIneligibleOperation
	}
	replay, err := replayOperationFromMaterialized(id, op)
	if err != nil {
		return VisibleAck{}, ReplayOperation{}, err
	}
	h.submitMu.Lock()
	defer h.submitMu.Unlock()
	if ack, ok, err := h.pendingAckForOperation(id, replay); ok || err != nil {
		return ack, replay, err
	}
	if _, err := h.detector.Admit(id, op); err != nil {
		if errors.Is(err, ErrDuplicateOperation) {
			if ack, ok, pendingErr := h.pendingAckForOperation(id, replay); ok || pendingErr != nil {
				return ack, replay, pendingErr
			}
		}
		return VisibleAck{}, ReplayOperation{}, err
	}
	h.mu.Lock()
	h.pending[id] = holderPendingOperation{
		scope:        cloneAuthorityScope(delta.Authority),
		intentDigest: op.IntentDigest,
		op:           replay,
	}
	h.mu.Unlock()
	return VisibleAck{EpochID: h.epochID, OpID: id, HolderID: h.holderID}, replay, nil
}

func (h *Holder) RestoreVisible(scope compile.AuthorityScope, op ReplayOperation) error {
	if h == nil || h.detector == nil {
		return ErrHolderConfigInvalid
	}
	if scope.Mount == "" || scope.MountKeyID == 0 {
		return ErrInvalidWitnessRecord
	}
	if err := validateVisibleReplayOperation(op); err != nil {
		return err
	}
	h.submitMu.Lock()
	defer h.submitMu.Unlock()
	if _, ok, err := h.pendingAckForOperation(op.OpID, op); ok || err != nil {
		return err
	}
	if err := h.detector.AdmitReplay(op); err != nil {
		if errors.Is(err, ErrDuplicateOperation) {
			if _, ok, pendingErr := h.pendingAckForOperation(op.OpID, op); ok || pendingErr != nil {
				return pendingErr
			}
		}
		return err
	}
	h.mu.Lock()
	h.pending[op.OpID] = holderPendingOperation{
		scope: cloneAuthorityScope(scope),
		op:    cloneReplayOperation(op),
	}
	h.mu.Unlock()
	return nil
}

func (h *Holder) PendingAck(id OperationID, op compile.MaterializedOp) (VisibleAck, bool, error) {
	if h == nil || h.detector == nil {
		return VisibleAck{}, false, ErrHolderConfigInvalid
	}
	if !id.Valid() {
		return VisibleAck{}, false, ErrInvalidOperationID
	}
	h.mu.Lock()
	pending, ok := h.pending[id]
	h.mu.Unlock()
	if !ok {
		return VisibleAck{}, false, nil
	}
	if op.IntentDigest != ([32]byte{}) {
		if pending.intentDigest == op.IntentDigest {
			return VisibleAck{EpochID: h.epochID, OpID: id, HolderID: h.holderID}, true, nil
		}
		return VisibleAck{}, false, ErrDuplicateOperation
	}
	replay, err := replayOperationFromMaterialized(id, op)
	if err != nil {
		return VisibleAck{}, false, err
	}
	if !replayOperationsEqual(pending.op, replay) {
		return VisibleAck{}, false, ErrDuplicateOperation
	}
	return VisibleAck{EpochID: h.epochID, OpID: id, HolderID: h.holderID}, true, nil
}

func (h *Holder) pendingAckForOperation(id OperationID, op ReplayOperation) (VisibleAck, bool, error) {
	if !id.Valid() {
		return VisibleAck{}, false, ErrInvalidOperationID
	}
	h.mu.Lock()
	pending, ok := h.pending[id]
	h.mu.Unlock()
	if !ok {
		return VisibleAck{}, false, nil
	}
	if !replayOperationsEqual(pending.op, op) {
		return VisibleAck{}, false, ErrDuplicateOperation
	}
	return VisibleAck{EpochID: h.epochID, OpID: id, HolderID: h.holderID}, true, nil
}

func (h *Holder) MarkAppliedIDs(ids ...OperationID) {
	if h == nil || h.detector == nil {
		return
	}
	h.detector.RemoveMany(ids...)
	h.mu.Lock()
	for _, id := range ids {
		delete(h.pending, id)
	}
	h.mu.Unlock()
}

func (h *Holder) BuildPendingReplayPlan(firstVersion uint64) (ReplayPlan, compile.AuthorityScope, error) {
	return h.BuildPendingReplayPlanLimit(firstVersion, 0)
}

func (h *Holder) BuildPendingReplayPlanLimit(firstVersion uint64, maxOps int) (ReplayPlan, compile.AuthorityScope, error) {
	if h == nil || h.detector == nil {
		return ReplayPlan{}, compile.AuthorityScope{}, ErrHolderConfigInvalid
	}
	plan, scope, ok, err := h.buildPendingReplayPlan(firstVersion, nil, maxOps)
	if err != nil {
		return ReplayPlan{}, compile.AuthorityScope{}, err
	}
	if !ok {
		return ReplayPlan{}, compile.AuthorityScope{}, ErrInvalidPerasSegment
	}
	return plan, scope, nil
}

func (h *Holder) BuildPendingReplayPlanForScope(firstVersion uint64, target compile.AuthorityScope) (ReplayPlan, compile.AuthorityScope, bool, error) {
	if h == nil || h.detector == nil {
		return ReplayPlan{}, compile.AuthorityScope{}, false, ErrHolderConfigInvalid
	}
	return h.buildPendingReplayPlan(firstVersion, func(scope compile.AuthorityScope) bool {
		return authorityScopesOverlap(scope, target)
	}, 0)
}

func (h *Holder) buildPendingReplayPlan(firstVersion uint64, include func(compile.AuthorityScope) bool, maxOps int) (ReplayPlan, compile.AuthorityScope, bool, error) {
	ids := h.detector.IDsLimit(maxOps)
	if len(ids) == 0 {
		return ReplayPlan{}, compile.AuthorityScope{}, false, nil
	}
	plan := ReplayPlan{
		EpochID:    h.epochID,
		Operations: make([]ReplayOperation, 0, len(ids)),
	}
	var scope compile.AuthorityScope
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, id := range ids {
		pending, ok := h.pending[id]
		if !ok {
			return ReplayPlan{}, compile.AuthorityScope{}, false, ErrInvalidPerasSegment
		}
		if include != nil && !include(pending.scope) {
			continue
		}
		if len(plan.Operations) == 0 {
			scope = cloneAuthorityScope(pending.scope)
		} else {
			scope = unionAuthorityScopes(scope, pending.scope)
		}
		plan.Operations = append(plan.Operations, cloneReplayOperation(pending.op))
	}
	if len(plan.Operations) == 0 {
		return ReplayPlan{}, compile.AuthorityScope{}, false, nil
	}
	if firstVersion != 0 {
		plan.Versions = ReplayVersionRange{First: firstVersion, Count: uint64(len(plan.Operations))}
	}
	return plan, scope, true, nil
}

func (h *Holder) MarkReplayPlanApplied(plan ReplayPlan) error {
	if h == nil || h.detector == nil {
		return ErrHolderConfigInvalid
	}
	if plan.EpochID != h.epochID || len(plan.Operations) == 0 {
		return ErrInvalidPerasSegment
	}
	ids := make([]OperationID, 0, len(plan.Operations))
	for _, op := range plan.Operations {
		ids = append(ids, op.OpID)
	}
	h.MarkAppliedIDs(ids...)
	return nil
}

func (h *Holder) Pending() int {
	if h == nil || h.detector == nil {
		return 0
	}
	return h.detector.Len()
}

func (h *Holder) PendingIDs() []OperationID {
	if h == nil || h.detector == nil {
		return nil
	}
	return h.detector.IDs()
}

func (h *Holder) EpochID() uint64 {
	if h == nil {
		return 0
	}
	return h.epochID
}

func (h *Holder) HolderID() string {
	if h == nil {
		return ""
	}
	return h.holderID
}

func cloneAuthorityScope(scope compile.AuthorityScope) compile.AuthorityScope {
	out := scope
	out.Buckets = append([]fsmeta.AffinityBucket(nil), scope.Buckets...)
	out.Parents = append([]model.InodeID(nil), scope.Parents...)
	out.Inodes = append([]model.InodeID(nil), scope.Inodes...)
	return out
}

func unionAuthorityScopes(left, right compile.AuthorityScope) compile.AuthorityScope {
	if left.Mount == "" {
		return cloneAuthorityScope(right)
	}
	out := cloneAuthorityScope(left)
	out.Buckets = unionBuckets(out.Buckets, right.Buckets)
	out.Parents = unionInodes(out.Parents, right.Parents)
	out.Inodes = unionInodes(out.Inodes, right.Inodes)
	return out
}

func authorityScopesOverlap(left, right compile.AuthorityScope) bool {
	if left.Mount != right.Mount || left.MountKeyID != right.MountKeyID {
		return false
	}
	return bucketsOverlap(left.Buckets, right.Buckets) &&
		inodesOverlap(left.Parents, right.Parents) &&
		inodesOverlap(left.Inodes, right.Inodes)
}

// AuthorityScopesOverlap reports whether two authority scopes may cover the
// same fsmeta write.
func AuthorityScopesOverlap(left, right compile.AuthorityScope) bool {
	return authorityScopesOverlap(left, right)
}

func bucketsOverlap(left, right []fsmeta.AffinityBucket) bool {
	if len(left) == 0 || len(right) == 0 {
		return true
	}
	for _, l := range left {
		if slices.Contains(right, l) {
			return true
		}
	}
	return false
}

func inodesOverlap(left, right []model.InodeID) bool {
	if len(left) == 0 || len(right) == 0 {
		return true
	}
	for _, l := range left {
		if slices.Contains(right, l) {
			return true
		}
	}
	return false
}

func unionBuckets(left, right []fsmeta.AffinityBucket) []fsmeta.AffinityBucket {
	out := append([]fsmeta.AffinityBucket(nil), left...)
	for _, candidate := range right {
		seen := slices.Contains(out, candidate)
		if !seen {
			out = append(out, candidate)
		}
	}
	return out
}

func unionInodes(left, right []model.InodeID) []model.InodeID {
	out := append([]model.InodeID(nil), left...)
	for _, candidate := range right {
		seen := slices.Contains(out, candidate)
		if !seen {
			out = append(out, candidate)
		}
	}
	return out
}

func replayOperationsEqual(left, right ReplayOperation) bool {
	if left.OpID != right.OpID ||
		left.Kind != right.Kind ||
		left.DescriptorDigest != right.DescriptorDigest ||
		left.PredicateProofDigest != right.PredicateProofDigest ||
		left.ExecutionPlanDigest != right.ExecutionPlanDigest ||
		!predicateProofsEqual(left.PredicateProofs, right.PredicateProofs) ||
		!guardProofsEqual(left.GuardProofs, right.GuardProofs) ||
		left.Segment != right.Segment ||
		left.Durability != right.Durability ||
		left.Atomicity.Splittable != right.Atomicity.Splittable ||
		left.Atomicity.Recovery != right.Atomicity.Recovery ||
		left.Atomicity.Digest != right.Atomicity.Digest ||
		len(left.Atomicity.Members) != len(right.Atomicity.Members) ||
		len(left.Mutations) != len(right.Mutations) {
		return false
	}
	for i := range left.Atomicity.Members {
		if left.Atomicity.Members[i] != right.Atomicity.Members[i] {
			return false
		}
	}
	for i := range left.Mutations {
		l := left.Mutations[i]
		r := right.Mutations[i]
		if l.Delete != r.Delete || !bytes.Equal(l.Key, r.Key) || !bytes.Equal(l.Value, r.Value) {
			return false
		}
	}
	return true
}

func predicateProofsEqual(left, right []proof.PredicateProof) bool {
	return slices.EqualFunc(left, right, func(l, r proof.PredicateProof) bool {
		return l.SchemaVersion == r.SchemaVersion &&
			l.Rule == r.Rule &&
			l.Present == r.Present &&
			l.Version == r.Version &&
			l.Source == r.Source &&
			l.ProofFrontier == r.ProofFrontier &&
			l.ProofKind == r.ProofKind &&
			l.ScopeDigest == r.ScopeDigest &&
			l.Digest == r.Digest &&
			bytes.Equal(l.Key, r.Key) &&
			bytes.Equal(l.Value, r.Value)
	})
}

func guardProofsEqual(left, right []proof.GuardProof) bool {
	return slices.Equal(left, right)
}
