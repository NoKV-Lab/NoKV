// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	fsperas "github.com/feichai0017/NoKV/experimental/peras/exec"
	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/feichai0017/NoKV/fsmeta/proof"
)

func (c *Runtime) freezeFlushBatchesLocked(target *compile.AuthorityScope, materialize bool, maxOpsPerHolder int) ([]perasFlushBatch, error) {
	plans, err := c.freezeReplayPlansLocked(target, maxOpsPerHolder)
	if err != nil {
		return nil, err
	}
	return c.buildFlushBatches(plans, materialize)
}

func (c *Runtime) buildFlushBatches(plans []perasFrozenPlan, materialize bool) ([]perasFlushBatch, error) {
	batches := make([]perasFlushBatch, 0, len(plans))
	needsPayload := c.flushNeedsSegmentPayload()
	for _, frozen := range plans {
		sized, err := splitReplayPlanByCompilerBudget(frozen.plan, materialize, c.replaySegmentBudget(materialize), c.replaySegmentCatalogRouteBudget(materialize))
		if err != nil {
			return nil, c.recordErrorf("split peras replay plan by install budget: %w", err)
		}
		jobs := make([]perasFlushJob, 0, len(sized))
		for _, plan := range sized {
			segment, err := fsperas.BuildPerasSegmentFromReplayPlan(plan)
			if err != nil {
				return nil, c.recordErrorf("build peras segment: %w", err)
			}
			var payload []byte
			var digest [32]byte
			if needsPayload {
				var err error
				payload, err = fsperas.EncodePerasSegment(segment)
				if err != nil {
					return nil, c.recordErrorf("encode peras segment: %w", err)
				}
				digest, err = fsperas.PerasSegmentPayloadDigest(payload)
				if err != nil {
					return nil, c.recordErrorf("digest peras segment: %w", err)
				}
			}
			install, err := fsperas.PerasSegmentInstallPlan(segment, materialize)
			if err != nil {
				return nil, c.recordErrorf("plan peras segment install: %w", err)
			}
			job := perasFlushJob{
				scope:       frozen.scope,
				plan:        plan,
				segment:     segment,
				payload:     payload,
				digest:      digest,
				install:     install,
				materialize: materialize,
			}
			jobs = append(jobs, job)
		}
		batchLimit := c.flushBatchJobLimit(materialize)
		for start := 0; start < len(jobs); start += batchLimit {
			end := min(start+batchLimit, len(jobs))
			batchPlan, err := joinReplayPlansForBatch(sized[start:end])
			if err != nil {
				return nil, c.recordErrorf("build peras batch replay plan: %w", err)
			}
			batches = append(batches, perasFlushBatch{
				holder:          frozen.holder,
				scope:           frozen.scope,
				plan:            batchPlan,
				jobs:            append([]perasFlushJob(nil), jobs[start:end]...),
				witnessUnixNano: c.nextWitnessUnixNano(),
			})
		}
	}
	return batches, nil
}

func (c *Runtime) flushBatchJobLimit(materialize bool) int {
	if !materialize {
		return int(^uint(0) >> 1)
	}
	if c == nil || c.installN <= 0 {
		return 1
	}
	return c.installN
}

func (c *Runtime) flushNeedsSegmentPayload() bool {
	if c == nil {
		return true
	}
	if c.usesSegmentWitness() {
		return true
	}
	return segmentInstallerNeedsPayload(c.installer)
}

func joinReplayPlansForBatch(plans []fsperas.ReplayPlan) (fsperas.ReplayPlan, error) {
	if len(plans) == 0 || plans[0].EpochID == 0 {
		return fsperas.ReplayPlan{}, fsperas.ErrInvalidPerasSegment
	}
	out := fsperas.ReplayPlan{EpochID: plans[0].EpochID}
	for _, plan := range plans {
		if plan.EpochID != out.EpochID || !plan.Versions.Empty() || len(plan.Operations) == 0 {
			return fsperas.ReplayPlan{}, fsperas.ErrInvalidPerasSegment
		}
		out.Operations = append(out.Operations, cloneRuntimeReplayOperations(plan.Operations)...)
	}
	return out, nil
}

func (c *Runtime) replaySegmentBudget(materialize bool) compile.SegmentBudget {
	budget := compile.SegmentBudget{
		MaxOperations:   uint32(c.maxOps),
		MaxMutations:    uint32(c.maxReplay),
		MaxPayloadBytes: c.maxPayload,
	}
	if !materialize {
		return budget
	}
	cap := c.materializeMaxReplay
	if cap <= 0 {
		cap = defaultPerasMaterializeMaxReplayMutations
	}
	if c.maxReplay > 0 && c.maxReplay < cap {
		budget.MaxMutations = uint32(c.maxReplay)
	} else {
		budget.MaxMutations = uint32(cap)
	}
	return budget
}

func (c *Runtime) replaySegmentCatalogRouteBudget(materialize bool) int {
	if materialize {
		return 0
	}
	if c.routeBudget > 0 {
		return c.routeBudget
	}
	return 1
}

func splitReplayPlanByCompilerBudget(plan fsperas.ReplayPlan, materialize bool, budget compile.SegmentBudget, catalogRouteBudget int) ([]fsperas.ReplayPlan, error) {
	if plan.EpochID == 0 || len(plan.Operations) == 0 {
		return nil, fsperas.ErrInvalidPerasSegment
	}
	if !plan.Versions.Empty() {
		return nil, fsperas.ErrReplayVersionRequired
	}
	out := make([]fsperas.ReplayPlan, 0, len(plan.Operations))
	current := fsperas.ReplayPlan{EpochID: plan.EpochID}
	var currentPlan compile.SegmentPlan
	currentCatalogBuckets := make(map[catalogRouteBucket]struct{})
	flush := func() {
		if len(current.Operations) == 0 {
			return
		}
		out = append(out, fsperas.ReplayPlan{
			EpochID:    current.EpochID,
			Operations: cloneRuntimeReplayOperations(current.Operations),
		})
		current.Operations = current.Operations[:0]
		currentPlan = compile.SegmentPlan{}
		clear(currentCatalogBuckets)
	}
	for _, op := range plan.Operations {
		if !op.OpID.Valid() || len(op.Mutations) == 0 {
			return nil, fsperas.ErrInvalidPerasSegment
		}
		nextPlan, ok := compile.SegmentPlanForInstall(op.Segment, materialize)
		if !ok {
			return nil, fsperas.ErrInvalidPerasSegment
		}
		nextCatalogBuckets, nextCatalog, err := replayOperationCatalogRouteBuckets(op, materialize, nextPlan)
		if err != nil {
			return nil, err
		}
		if len(current.Operations) > 0 {
			decision := compile.CanAppendSegmentPlans(currentPlan, nextPlan, op.Durability, budget)
			if decision.Kind != compile.SegmentDecisionAppend {
				flush()
			}
		}
		if len(current.Operations) > 0 && nextCatalog && catalogRouteBudget > 0 && catalogRouteBucketUnionCount(currentCatalogBuckets, nextCatalogBuckets) > catalogRouteBudget {
			flush()
		}
		current.Operations = append(current.Operations, cloneRuntimeReplayOperation(op))
		if len(current.Operations) == 1 {
			currentPlan = nextPlan
		} else {
			currentPlan = compile.MergeSegmentPlans(currentPlan, nextPlan)
		}
		if nextCatalog {
			for _, bucket := range nextCatalogBuckets {
				currentCatalogBuckets[bucket] = struct{}{}
			}
		}
	}
	flush()
	return out, nil
}

type catalogRouteBucket struct {
	mount  model.MountKeyID
	bucket fsmeta.AffinityBucket
}

func replayOperationCatalogRouteBuckets(op fsperas.ReplayOperation, materialize bool, plan compile.SegmentPlan) ([]catalogRouteBucket, bool, error) {
	if materialize || plan.Install != compile.SegmentInstallCatalog {
		return nil, false, nil
	}
	seen := make(map[catalogRouteBucket]struct{})
	out := make([]catalogRouteBucket, 0, len(op.Mutations))
	for _, mutation := range op.Mutations {
		parts, ok := fsmeta.InspectKey(mutation.Key)
		if !ok {
			return nil, false, fsperas.ErrInvalidPerasSegment
		}
		key := catalogRouteBucket{mount: parts.MountKeyID, bucket: parts.Bucket}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, key)
	}
	if len(out) == 0 {
		return nil, false, fsperas.ErrInvalidPerasSegment
	}
	return out, true, nil
}

func catalogRouteBucketUnionCount(current map[catalogRouteBucket]struct{}, next []catalogRouteBucket) int {
	total := len(current)
	for _, bucket := range next {
		if _, ok := current[bucket]; !ok {
			total++
		}
	}
	return total
}

func cloneRuntimeReplayOperations(ops []fsperas.ReplayOperation) []fsperas.ReplayOperation {
	out := make([]fsperas.ReplayOperation, 0, len(ops))
	for _, op := range ops {
		out = append(out, cloneRuntimeReplayOperation(op))
	}
	return out
}

func cloneRuntimeReplayOperation(op fsperas.ReplayOperation) fsperas.ReplayOperation {
	mutations := make([]fsperas.ReplayMutation, 0, len(op.Mutations))
	for _, mutation := range op.Mutations {
		mutations = append(mutations, fsperas.ReplayMutation{
			Key:    append([]byte(nil), mutation.Key...),
			Value:  append([]byte(nil), mutation.Value...),
			Delete: mutation.Delete,
		})
	}
	return fsperas.ReplayOperation{
		OpID:                 op.OpID,
		Kind:                 op.Kind,
		DescriptorDigest:     op.DescriptorDigest,
		PredicateProofDigest: op.PredicateProofDigest,
		ExecutionPlanDigest:  op.ExecutionPlanDigest,
		PredicateProofs:      cloneRuntimePredicateProofs(op.PredicateProofs),
		GuardProofs:          cloneRuntimeGuardProofs(op.GuardProofs),
		Segment:              op.Segment,
		Atomicity:            cloneRuntimeReplayAtomicity(op.Atomicity),
		Durability:           op.Durability,
		Mutations:            mutations,
	}
}

func cloneRuntimePredicateProofs(proofs []proof.PredicateProof) []proof.PredicateProof {
	if len(proofs) == 0 {
		return nil
	}
	out := make([]proof.PredicateProof, len(proofs))
	for i, predicateProof := range proofs {
		out[i] = proof.PredicateProof{
			SchemaVersion: predicateProof.SchemaVersion,
			Rule:          predicateProof.Rule,
			Key:           append([]byte(nil), predicateProof.Key...),
			Present:       predicateProof.Present,
			Value:         append([]byte(nil), predicateProof.Value...),
			Version:       predicateProof.Version,
			Source:        predicateProof.Source,
			ProofFrontier: predicateProof.ProofFrontier,
			ProofKind:     predicateProof.ProofKind,
			ScopeDigest:   predicateProof.ScopeDigest,
			Digest:        predicateProof.Digest,
		}
	}
	return out
}

func cloneRuntimeGuardProofs(proofs []proof.GuardProof) []proof.GuardProof {
	if len(proofs) == 0 {
		return nil
	}
	out := make([]proof.GuardProof, len(proofs))
	copy(out, proofs)
	return out
}

func cloneRuntimeReplayAtomicity(group compile.AtomicityGroup) compile.AtomicityGroup {
	group.Members = append([]compile.MutationID(nil), group.Members...)
	return group
}

func (c *Runtime) freezeReplayPlansLocked(target *compile.AuthorityScope, maxOpsPerHolder int) ([]perasFrozenPlan, error) {
	holders := c.holderSnapshot()
	plans := make([]perasFrozenPlan, 0, len(holders))
	for _, holder := range holders {
		plan, scope, ok, err := c.buildFlushPlan(holder, target, maxOpsPerHolder)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		plans = append(plans, perasFrozenPlan{
			holder: holder,
			scope:  scope,
			plan:   plan,
		})
	}
	return plans, nil
}

func (c *Runtime) buildFlushPlan(holder *fsperas.Holder, target *compile.AuthorityScope, maxOps int) (fsperas.ReplayPlan, compile.AuthorityScope, bool, error) {
	if target != nil {
		plan, scope, ok, err := holder.BuildPendingReplayPlanForScope(0, *target)
		if err != nil {
			return fsperas.ReplayPlan{}, compile.AuthorityScope{}, false, c.recordErrorf("build peras replay plan: %w", err)
		}
		return plan, scope, ok, nil
	}
	pending := holder.Pending()
	if pending == 0 {
		return fsperas.ReplayPlan{}, compile.AuthorityScope{}, false, nil
	}
	plan, scope, err := holder.BuildPendingReplayPlanLimit(0, maxOps)
	if err != nil {
		return fsperas.ReplayPlan{}, compile.AuthorityScope{}, false, c.recordErrorf("build peras replay plan: %w", err)
	}
	if maxOps <= 0 && len(plan.Operations) != pending {
		return fsperas.ReplayPlan{}, compile.AuthorityScope{}, false, c.recordError(fsperas.ErrInvalidPerasSegment)
	}
	return plan, scope, true, nil
}

func (c *Runtime) holderSnapshot() []*fsperas.Holder {
	if c == nil || c.epochs == nil {
		return nil
	}
	return c.epochs.holderSnapshot()
}
