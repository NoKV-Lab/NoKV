// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package compile

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"slices"
	"strconv"
	"strings"
	"unsafe"

	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/feichai0017/NoKV/fsmeta/proof"
)

const segmentFormatVersion uint16 = 1

// CompiledOp is the segment-installable semantic descriptor for one metadata
// operation. SemanticDelta is the executor-facing mutation contract; this
// descriptor is the compiler boundary optimized runtimes use for admission,
// packing, recovery, completion, and watch projection.
type CompiledOp struct {
	Delta SemanticDelta
	// DescriptorDigest is the canonical digest of this exact generated
	// descriptor, including any materialized observed values and effects.
	DescriptorDigest [32]byte
	// IntentDigest stays pinned to the original request intent across
	// materialization so completion/watch code can identify the user's op.
	IntentDigest [32]byte
	// ReplayDigest is the digest that segment replay must install. After
	// materialization it must equal DescriptorDigest.
	ReplayDigest [32]byte
	Authority    AuthorityPlan
	Placement    PlacementPlan
	Footprint    KeyFootprint
	Predicates   []PredicateObligation
	Guards       []GuardObligation
	Effects      []EffectPlan
	Atomicity    AtomicityGroup
	Durability   DurabilityClass
	Watch        []WatchProjection
	Completion   CompletionPlan
	Segment      SegmentPlan
}

// MaterializedOp is the closed semantic program admitted by an optimized
// runtime. It has the static generated descriptor plus any runtime predicate
// proofs and concrete effects needed to install the operation inside a segment.
type MaterializedOp struct {
	CompiledOp
	PredicateProofs []proof.PredicateProof
	GuardProofs     []proof.GuardProof
}

// PredicateEvidence carries runtime reads that turn a generated program with
// symbolic predicates into the proof-carrying descriptor admitted by an
// optimized runtime.
type PredicateEvidence struct {
	Proofs []proof.PredicateProof
}

type FenceMode uint8

const (
	FenceNone FenceMode = iota
	FenceActiveAuthority
)

type AuthorityPlan struct {
	Scope    AuthorityScope
	Required bool
	Fence    FenceMode
}

type SegmentInstallMode uint8

const (
	SegmentInstallNone SegmentInstallMode = iota
	SegmentInstallSingleBucket
	SegmentInstallCatalog
)

type SegmentMergeKey struct {
	MountKeyID       model.MountKeyID
	HasPrimaryBucket bool
	PrimaryBucket    layout.AffinityBucket
	Install          SegmentInstallMode
	Durability       DurabilityClass
	FormatVersion    uint16
}

type PlacementPlan struct {
	MountKeyID          model.MountKeyID
	Buckets             []layout.AffinityBucket
	SingleBucket        bool
	Install             SegmentInstallMode
	CanSegment          bool
	RequiresMaterialize bool
	SlowReason          SlowReason
	MergeKey            SegmentMergeKey
}

type KeyAccessMode uint8

const (
	KeyAccessRead KeyAccessMode = iota
	KeyAccessReadPrefix
	KeyAccessWrite
)

type KeyRef struct {
	Mode       KeyAccessMode
	Key        []byte
	Opaque     bool
	MountKeyID model.MountKeyID
	Bucket     layout.AffinityBucket
	Kind       layout.KeyKind
	Parent     model.InodeID
	Inode      model.InodeID
}

type KeyFootprint struct {
	Reads          []KeyRef
	Writes         []KeyRef
	ConflictKeys   []KeyRef
	HasPrefixRead  bool
	HasOpaqueKeys  bool
	EstimatedBytes uint64
}

type PredicateObligation struct {
	Kind             PredicateKind
	Key              []byte
	NeedValue        bool
	NeedAbsent       bool
	Guard            RuntimeGuard
	HasExpectedValue bool
	ExpectHash       [32]byte
}

type GuardObligation struct {
	Guard  RuntimeGuard
	Digest [32]byte
}

type DerivationKind uint8

const (
	DerivationNone DerivationKind = iota
	DerivationRuntimeValue
)

type EffectPlan struct {
	ID         MutationID
	Kind       EffectKind
	Key        []byte
	Value      []byte
	Concrete   bool
	Opaque     bool
	MountKeyID model.MountKeyID
	Bucket     layout.AffinityBucket
	RecordKind layout.KeyKind
	ValueHash  [32]byte
	Derivation DerivationKind
}

type MutationID uint16

type RecoveryRule uint8

const (
	RecoveryReplayAllOrNothing RecoveryRule = iota
)

type AtomicityGroup struct {
	Members    []MutationID
	Splittable bool
	Recovery   RecoveryRule
	Digest     [32]byte
}

type DurabilityClass uint8

const (
	DurabilityVisibleOnly DurabilityClass = iota
	DurabilityNeedsFsyncDir
	DurabilityNeedsCloseSession
	DurabilityNeedsPublishCheckpoint
)

type WatchEmitPoint uint8

const (
	WatchEmitVisible WatchEmitPoint = iota
	WatchEmitSeal
)

type WatchEventKind uint8

const (
	WatchEventUnknown WatchEventKind = iota
	WatchEventCreate
	WatchEventDelete
	WatchEventRename
	WatchEventUpdate
)

type WatchProjection struct {
	EventKind WatchEventKind
	Key       []byte
	Parent    model.InodeID
	Name      string
	Inode     model.InodeID
	EmitAt    WatchEmitPoint
}

type CompletionKind uint8

const (
	CompletionNone CompletionKind = iota
	CompletionVisible
	CompletionDurable
)

type CompletionPlan struct {
	RetainCompletion bool
	Kind             CompletionKind
	MutationCount    uint32
	DescriptorDigest [32]byte
}

type SegmentPlan struct {
	MergeKey              SegmentMergeKey
	Install               SegmentInstallMode
	MaterializeMergeKey   SegmentMergeKey
	MaterializeInstall    SegmentInstallMode
	CanAppend             bool
	CanMaterialize        bool
	RequiresMaterialize   bool
	EstimatedPayloadBytes uint64
	OperationCount        uint32
	MutationCount         uint32
}

// InstallPlan is the segment-install command header produced after a compiled
// replay plan has been sealed into a concrete segment. Routing and apply
// scheduling consume this metadata without decoding the segment payload.
type InstallPlan struct {
	Mode               SegmentInstallMode
	Materialize        bool
	RoutingKeys        [][]byte
	DependencyKeys     [][]byte
	CatalogKeys        [][]byte
	MaterializedKeys   [][]byte
	CanonicalObjectKey []byte
}

type SegmentBudget struct {
	MaxOperations   uint32
	MaxMutations    uint32
	MaxPayloadBytes uint64
}

type SegmentDecisionKind uint8

const (
	SegmentDecisionAppend SegmentDecisionKind = iota
	SegmentDecisionCut
	SegmentDecisionReject
	SegmentDecisionFlushBeforeAndAfter
)

type SegmentDecision struct {
	Kind   SegmentDecisionKind
	Reason SlowReason
}

const smallPredicateEvidenceLimit = 4

// MaterializeCompiledOpWithEvidence recompiles a generated descriptor with
// concrete runtime effects and predicate evidence, without reinterpreting the
// operation semantics at admission time.
func MaterializeCompiledOpWithEvidence(op CompiledOp, effects []WriteEffect, evidence PredicateEvidence, guardProofs []proof.GuardProof) (MaterializedOp, error) {
	delta := op.Delta
	if effects != nil {
		delta.WriteEffects = cloneEffects(effects)
	}
	var err error
	delta, err = applyPredicateEvidence(delta, evidence)
	if err != nil {
		return MaterializedOp{}, err
	}
	delta.Authority = authorityScopeWithDeltaKeys(delta.Authority, delta)
	compiled, err := compileAOTDelta(delta)
	if err != nil {
		return MaterializedOp{}, err
	}
	compiled.IntentDigest = op.IntentDigest
	compiled.ReplayDigest = compiled.DescriptorDigest
	return MaterializedOp{
		CompiledOp:      compiled,
		PredicateProofs: clonePredicateProofs(evidence.Proofs),
		GuardProofs:     cloneGuardProofs(guardProofs),
	}, nil
}

func applyPredicateEvidence(delta SemanticDelta, evidence PredicateEvidence) (SemanticDelta, error) {
	delta = cloneDelta(delta)
	if len(evidence.Proofs) <= smallPredicateEvidenceLimit {
		return applySmallPredicateEvidence(delta, evidence.Proofs)
	}
	proofs, err := predicateProofMap(evidence.Proofs)
	if err != nil {
		return SemanticDelta{}, err
	}
	seen := make(map[string]struct{}, len(delta.ReadPredicates)+len(proofs))
	for i := range delta.ReadPredicates {
		predicate := &delta.ReadPredicates[i]
		if predicate.Kind == PredicatePrefixScan {
			continue
		}
		if len(predicate.Key) == 0 {
			return SemanticDelta{}, model.ErrInvalidRequest
		}
		key := string(predicate.Key)
		seen[key] = struct{}{}
		if proof, ok := proofs[key]; ok {
			applyPredicateProof(predicate, proof)
			continue
		}
	}
	extraKeys := make([]string, 0, len(proofs))
	for key := range proofs {
		if _, ok := seen[key]; !ok {
			extraKeys = append(extraKeys, key)
		}
	}
	slices.Sort(extraKeys)
	for _, key := range extraKeys {
		proof := proofs[key]
		predicate := Predicate{Key: cloneBytes(proof.Key)}
		applyPredicateProof(&predicate, proof)
		delta.ReadPredicates = append(delta.ReadPredicates, predicate)
	}
	return delta, nil
}

func applySmallPredicateEvidence(delta SemanticDelta, proofs []proof.PredicateProof) (SemanticDelta, error) {
	var usedStack [smallPredicateEvidenceLimit]bool
	used := usedStack[:len(proofs)]
	for i, predicateProof := range proofs {
		if err := validatePredicateProof(predicateProof); err != nil {
			return SemanticDelta{}, err
		}
		for _, previous := range proofs[:i] {
			if bytes.Equal(previous.Key, predicateProof.Key) {
				return SemanticDelta{}, ValidationError{Kind: ValidationPredicateProofMismatch, Key: predicateProof.Key}
			}
		}
	}
	for i := range delta.ReadPredicates {
		predicate := &delta.ReadPredicates[i]
		if predicate.Kind == PredicatePrefixScan {
			continue
		}
		if len(predicate.Key) == 0 {
			return SemanticDelta{}, model.ErrInvalidRequest
		}
		for proofIndex, predicateProof := range proofs {
			if !bytes.Equal(predicateProof.Key, predicate.Key) {
				continue
			}
			applyPredicateProof(predicate, predicateProof)
			used[proofIndex] = true
			break
		}
	}
	var extraStack [smallPredicateEvidenceLimit]proof.PredicateProof
	extra := extraStack[:0]
	for i, predicateProof := range proofs {
		if used[i] {
			continue
		}
		extra = append(extra, predicateProof)
	}
	slices.SortFunc(extra, func(left, right proof.PredicateProof) int {
		return bytes.Compare(left.Key, right.Key)
	})
	for _, predicateProof := range extra {
		predicate := Predicate{Key: cloneBytes(predicateProof.Key)}
		applyPredicateProof(&predicate, predicateProof)
		delta.ReadPredicates = append(delta.ReadPredicates, predicate)
	}
	return delta, nil
}

func validatePredicateProof(predicateProof proof.PredicateProof) error {
	if len(predicateProof.Key) == 0 {
		return ValidationError{Kind: ValidationPredicateProofMismatch}
	}
	if err := proof.VerifyPredicateProof(predicateProof); err != nil {
		return ValidationError{Kind: ValidationPredicateProofMismatch, Key: predicateProof.Key}
	}
	return nil
}

func applyPredicateProof(predicate *Predicate, proof proof.PredicateProof) {
	if !proof.Present {
		predicate.Kind = PredicateNotExists
		predicate.ExpectedValue = nil
		predicate.HasExpectedValue = false
		predicate.RuntimeChecked = true
		return
	}
	predicate.Kind = PredicateObservedValue
	predicate.ExpectedValue = cloneBytes(proof.Value)
	predicate.HasExpectedValue = true
	predicate.RuntimeChecked = true
}

func authorityScopeWithDeltaKeys(scope AuthorityScope, delta SemanticDelta) AuthorityScope {
	out := cloneScope(scope)
	for _, predicate := range delta.ReadPredicates {
		out = authorityScopeWithKey(out, predicate.Key)
	}
	for _, effect := range delta.WriteEffects {
		out = authorityScopeWithKey(out, effect.Key)
	}
	return out
}

func authorityScopeWithKey(scope AuthorityScope, key []byte) AuthorityScope {
	parts, ok := layout.InspectKey(key)
	if !ok {
		return scope
	}
	if scope.MountKeyID == 0 {
		scope.MountKeyID = parts.MountKeyID
	}
	scope.Buckets = uniqueBuckets(append(scope.Buckets, parts.Bucket))
	switch parts.Kind {
	case layout.KeyKindDentry:
		scope.Parents = uniqueInodes(append(scope.Parents, parts.Parent))
	case layout.KeyKindInode, layout.KeyKindChunk, layout.KeyKindSession:
		scope.Inodes = uniqueInodes(append(scope.Inodes, parts.Inode))
	case layout.KeyKindUsage:
		scope.Parents = uniqueInodes(append(scope.Parents, model.InodeID(parts.UsageScope)))
	}
	return scope
}

func WithGuardProofs(op MaterializedOp, proofs []proof.GuardProof) MaterializedOp {
	op.GuardProofs = cloneGuardProofs(proofs)
	return op
}

func WithPredicateProofs(op MaterializedOp, proofs []proof.PredicateProof) MaterializedOp {
	op.PredicateProofs = clonePredicateProofs(proofs)
	return op
}

func WithAdmissionProofs(op MaterializedOp, predicateProofs []proof.PredicateProof, guardProofs []proof.GuardProof) MaterializedOp {
	op.PredicateProofs = clonePredicateProofs(predicateProofs)
	op.GuardProofs = cloneGuardProofs(guardProofs)
	return op
}

func CanAppendSegment(current, next CompiledOp, budget SegmentBudget) SegmentDecision {
	return CanAppendSegmentPlans(current.Segment, next.Segment, next.Durability, budget)
}

func CanAppendSegmentPlans(current, next SegmentPlan, nextDurability DurabilityClass, budget SegmentBudget) SegmentDecision {
	if nextDurability != DurabilityVisibleOnly && current.MergeKey.Durability != nextDurability {
		return SegmentDecision{Kind: SegmentDecisionFlushBeforeAndAfter, Reason: SlowReasonDurabilityBarrier}
	}
	if !current.CanAppend || !next.CanAppend {
		return SegmentDecision{Kind: SegmentDecisionReject, Reason: SlowReasonDynamicWriteSet}
	}
	if current.MergeKey != next.MergeKey {
		return SegmentDecision{Kind: SegmentDecisionCut, Reason: SlowReasonCrossBucket}
	}
	if budget.MaxOperations > 0 && current.OperationCount+next.OperationCount > budget.MaxOperations {
		return SegmentDecision{Kind: SegmentDecisionCut}
	}
	if budget.MaxMutations > 0 && current.MutationCount+next.MutationCount > budget.MaxMutations {
		return SegmentDecision{Kind: SegmentDecisionCut}
	}
	if budget.MaxPayloadBytes > 0 && current.EstimatedPayloadBytes+next.EstimatedPayloadBytes > budget.MaxPayloadBytes {
		return SegmentDecision{Kind: SegmentDecisionCut}
	}
	return SegmentDecision{Kind: SegmentDecisionAppend}
}

func MergeSegmentPlans(current, next SegmentPlan) SegmentPlan {
	out := current
	out.OperationCount += next.OperationCount
	out.MutationCount += next.MutationCount
	out.EstimatedPayloadBytes += next.EstimatedPayloadBytes
	return out
}

func SegmentPlanForInstall(plan SegmentPlan, materialize bool) (SegmentPlan, bool) {
	if !materialize {
		return plan, plan.CanAppend && plan.Install != SegmentInstallNone
	}
	if !plan.CanMaterialize || plan.MaterializeInstall == SegmentInstallNone {
		return SegmentPlan{}, false
	}
	out := plan
	out.Install = plan.MaterializeInstall
	out.MergeKey = plan.MaterializeMergeKey
	out.CanAppend = true
	return out, true
}

func fenceMode(delta SemanticDelta) FenceMode {
	if delta.Eligibility != EligibilityVisibleCommit {
		return FenceNone
	}
	return FenceActiveAuthority
}

func keyRef(mode KeyAccessMode, key []byte) KeyRef {
	out := KeyRef{
		Mode: mode,
		Key:  key,
	}
	parts, ok := layout.InspectKey(key)
	if !ok {
		out.Opaque = len(key) > 0
		return out
	}
	out.MountKeyID = parts.MountKeyID
	out.Bucket = parts.Bucket
	out.Kind = parts.Kind
	out.Parent = parts.Parent
	out.Inode = parts.Inode
	return out
}

func semanticKeyBindingMatches(delta SemanticDelta, actual []byte, binding string) bool {
	if binding == "" || binding == "runtime" {
		return true
	}
	expected, ok := semanticKeyBinding(delta, binding)
	if !ok {
		return false
	}
	return bytes.Equal(actual, expected)
}

func semanticIndexedKeyBindingMatches(delta SemanticDelta, actual []byte, family string, index int) bool {
	if family == "" || family == "runtime" || index < 0 {
		return true
	}
	var expected []byte
	switch family {
	case "read":
		if index >= len(delta.Plan.ReadKeys) {
			return false
		}
		expected = delta.Plan.ReadKeys[index]
	case "read_prefix":
		if index >= len(delta.Plan.ReadPrefixes) {
			return false
		}
		expected = delta.Plan.ReadPrefixes[index]
	case "mutate":
		if index >= len(delta.Plan.MutateKeys) {
			return false
		}
		expected = delta.Plan.MutateKeys[index]
	default:
		return false
	}
	return len(expected) != 0 && bytes.Equal(actual, expected)
}

func semanticKeyBinding(delta SemanticDelta, binding string) ([]byte, bool) {
	switch binding {
	case "primary":
		return delta.Plan.PrimaryKey, len(delta.Plan.PrimaryKey) != 0
	case "owner":
		return semanticOwnerKey(delta)
	}
	if prefix, ok := strings.CutSuffix(binding, "]"); ok {
		name, indexText, ok := strings.Cut(prefix, "[")
		if !ok {
			return nil, false
		}
		index, err := strconv.Atoi(indexText)
		if err != nil || index < 0 {
			return nil, false
		}
		switch name {
		case "read":
			if index >= len(delta.Plan.ReadKeys) {
				return nil, false
			}
			return delta.Plan.ReadKeys[index], len(delta.Plan.ReadKeys[index]) != 0
		case "read_prefix":
			if index >= len(delta.Plan.ReadPrefixes) {
				return nil, false
			}
			return delta.Plan.ReadPrefixes[index], len(delta.Plan.ReadPrefixes[index]) != 0
		case "mutate":
			if index >= len(delta.Plan.MutateKeys) {
				return nil, false
			}
			return delta.Plan.MutateKeys[index], len(delta.Plan.MutateKeys[index]) != 0
		case "predicate":
			if index >= len(delta.ReadPredicates) {
				return nil, false
			}
			return delta.ReadPredicates[index].Key, len(delta.ReadPredicates[index].Key) != 0
		}
	}
	return nil, false
}

func semanticOwnerKey(delta SemanticDelta) ([]byte, bool) {
	var sessionKey []byte
	switch {
	case len(delta.Plan.ReadKeys) > 0:
		sessionKey = delta.Plan.ReadKeys[0]
	case len(delta.Plan.PrimaryKey) > 0:
		sessionKey = delta.Plan.PrimaryKey
	default:
		return nil, false
	}
	parts, ok := layout.InspectKey(sessionKey)
	if !ok || parts.Kind != layout.KeyKindSession || parts.Inode == 0 || delta.Authority.Mount == "" {
		return nil, false
	}
	key, err := layout.EncodeInodeSessionKey(model.MountIdentity{
		MountID:    delta.Authority.Mount,
		MountKeyID: parts.MountKeyID,
	}, parts.Inode)
	return key, err == nil
}

func watchEventKind(delta SemanticDelta, effect WriteEffect) WatchEventKind {
	switch delta.Kind {
	case model.OperationCreate:
		return WatchEventCreate
	case model.OperationRename, model.OperationRenameReplace, model.OperationRenameSubtree:
		return WatchEventRename
	case model.OperationUnlink, model.OperationRemove:
		return WatchEventDelete
	}
	switch effect.Kind {
	case EffectDelete, EffectDerivedDelete:
		return WatchEventDelete
	case EffectPut, EffectDerivedPut:
		return WatchEventUpdate
	default:
		return WatchEventUnknown
	}
}

func dentryName(key []byte) string {
	name, ok := layout.DentryNameBytesOfKey(key)
	if !ok || len(name) == 0 {
		return ""
	}
	return unsafe.String(&name[0], len(name))
}

func PredicateProofSetDigest(proofs []proof.PredicateProof) [32]byte {
	if len(proofs) == 0 {
		return [32]byte{}
	}
	h := newDigestBuilder()
	h.writeUint64(uint64(len(proofs)))
	for _, predicateProof := range proofs {
		h.writeUint64(uint64(predicateProof.SchemaVersion))
		h.writeString(string(predicateProof.Rule))
		h.writeBytes(predicateProof.Key)
		h.writeBool(predicateProof.Present)
		h.writeBytes(predicateProof.Value)
		h.writeUint64(predicateProof.Version)
		h.writeUint64(uint64(predicateProof.Source))
		h.writeUint64(predicateProof.ProofFrontier.EpochID)
		h.writeUint64(predicateProof.ProofFrontier.Sequence)
		h.writeUint64(uint64(predicateProof.ProofKind))
		h.writeBytes(predicateProof.ScopeDigest[:])
		h.writeBytes(predicateProof.Digest[:])
	}
	return h.sum()
}

func ProofRuleForGuard(guard RuntimeGuard) (proof.RuleID, bool) {
	switch guard {
	case GuardSingleLinkInode:
		return proof.RuleGuardSingleLinkInode, true
	case GuardSameAuthority:
		return proof.RuleGuardSameAuthority, true
	case GuardNonDirectoryInode:
		return proof.RuleGuardNonDirectoryInode, true
	case GuardEmptyDirectory:
		return proof.RuleGuardEmptyDirectory, true
	case GuardLiveSession:
		return proof.RuleGuardLiveSession, true
	case GuardExpiredSessionOwner:
		return proof.RuleGuardExpiredSessionOwner, true
	case GuardQuotaCredit:
		return proof.RuleGuardQuotaCredit, true
	default:
		return "", false
	}
}

func RuntimeGuardForProofRule(rule proof.RuleID) (RuntimeGuard, bool) {
	switch rule {
	case proof.RuleGuardSingleLinkInode:
		return GuardSingleLinkInode, true
	case proof.RuleGuardSameAuthority:
		return GuardSameAuthority, true
	case proof.RuleGuardNonDirectoryInode:
		return GuardNonDirectoryInode, true
	case proof.RuleGuardEmptyDirectory:
		return GuardEmptyDirectory, true
	case proof.RuleGuardLiveSession:
		return GuardLiveSession, true
	case proof.RuleGuardExpiredSessionOwner:
		return GuardExpiredSessionOwner, true
	case proof.RuleGuardQuotaCredit:
		return GuardQuotaCredit, true
	default:
		return "", false
	}
}

func guardProofRule(guard RuntimeGuard) proof.RuleID {
	rule, _ := ProofRuleForGuard(guard)
	return rule
}

func GuardObligationDigest(guard RuntimeGuard) [32]byte {
	return proof.GuardObligationDigest(guardProofRule(guard))
}

func KeyFootprintDigest(footprint KeyFootprint) [32]byte {
	h := newDigestBuilder()
	h.writeUint64(uint64(len(footprint.Reads)))
	for _, ref := range footprint.Reads {
		h.writeKeyRef(ref)
	}
	h.writeUint64(uint64(len(footprint.Writes)))
	for _, ref := range footprint.Writes {
		h.writeKeyRef(ref)
	}
	h.writeUint64(uint64(len(footprint.ConflictKeys)))
	for _, ref := range footprint.ConflictKeys {
		h.writeKeyRef(ref)
	}
	h.writeBool(footprint.HasPrefixRead)
	h.writeBool(footprint.HasOpaqueKeys)
	h.writeUint64(footprint.EstimatedBytes)
	return h.sum()
}

func GuardProofFor(guard RuntimeGuard, passed bool, evidence proof.GuardEvidence) proof.GuardProof {
	rule := guardProofRule(guard)
	return proof.GuardProofFor(rule, passed, evidence)
}

func GuardProofsFor(op CompiledOp, predicateProofs []proof.PredicateProof, guards []RuntimeGuard) ([]proof.GuardProof, error) {
	if len(guards) == 0 {
		return nil, nil
	}
	out := make([]proof.GuardProof, 0, len(guards))
	for _, guard := range guards {
		evidence, err := GuardEvidenceForGuard(op, predicateProofs, guard)
		if err != nil {
			return nil, err
		}
		out = append(out, GuardProofFor(guard, true, evidence))
	}
	return out, nil
}

func VerifyGuardProof(op CompiledOp, predicateProofs []proof.PredicateProof, obligation GuardObligation, guardProof proof.GuardProof) error {
	rule, ok := ProofRuleForGuard(obligation.Guard)
	if !ok {
		return model.ErrInvalidRequest
	}
	proofObligation := proof.GuardObligation{
		Guard:  rule,
		Digest: GuardObligationDigest(obligation.Guard),
	}
	evidence, err := GuardEvidenceForGuard(op, predicateProofs, obligation.Guard)
	if err != nil {
		return err
	}
	if err := proof.VerifyGuardProof(proofObligation, evidence, guardProof); err != nil {
		return model.ErrInvalidRequest
	}
	return nil
}

func GuardEvidenceForGuard(op CompiledOp, predicateProofs []proof.PredicateProof, guard RuntimeGuard) (proof.GuardEvidence, error) {
	rule, ok := ProofRuleForGuard(guard)
	if !ok {
		return proof.GuardEvidence{}, model.ErrInvalidRequest
	}
	evidence := proof.GuardEvidence{
		SchemaVersion:        proof.Version1,
		Guard:                rule,
		DescriptorDigest:     op.DescriptorDigest,
		PredicateProofDigest: PredicateProofSetDigest(predicateProofs),
		FootprintDigest:      KeyFootprintDigest(op.Footprint),
		EffectDigest:         EffectPlanDigest(op.Effects),
		ProofFrontier:        proofSetFrontier(predicateProofs),
	}
	var subject [32]byte
	var subjectOK bool
	switch guard {
	case GuardSingleLinkInode:
		evidence.Kind = proof.GuardEvidenceSingleLinkInode
		subject, subjectOK = singleLinkInodeGuardSubject(predicateProofs)
	case GuardSameAuthority:
		evidence.Kind = proof.GuardEvidenceSameAuthority
		subject, subjectOK = sameAuthorityGuardSubject(op)
	case GuardNonDirectoryInode:
		evidence.Kind = proof.GuardEvidenceNonDirectoryInode
		subject, subjectOK = nonDirectoryGuardSubject(op, predicateProofs)
	case GuardEmptyDirectory:
		evidence.Kind = proof.GuardEvidenceEmptyDirectory
		subject, subjectOK = emptyDirectoryGuardSubject(op, predicateProofs)
	case GuardLiveSession:
		evidence.Kind = proof.GuardEvidenceLiveSession
		subject, subjectOK = liveSessionGuardSubject(predicateProofs)
	case GuardExpiredSessionOwner:
		evidence.Kind = proof.GuardEvidenceExpiredSessionOwner
		subject, subjectOK = expiredSessionOwnerGuardSubject(predicateProofs)
	case GuardQuotaCredit:
		evidence.Kind = proof.GuardEvidenceQuotaCredit
		subject, subjectOK = quotaCreditGuardSubject(op)
	default:
		return proof.GuardEvidence{}, model.ErrInvalidRequest
	}
	if !subjectOK {
		return proof.GuardEvidence{}, model.ErrInvalidRequest
	}
	evidence.SubjectDigest = subject
	return evidence, nil
}

func EffectPlanDigest(effects []EffectPlan) [32]byte {
	if len(effects) == 0 {
		return [32]byte{}
	}
	h := newDigestBuilder()
	h.writeUint64(uint64(len(effects)))
	for _, effect := range effects {
		h.writeUint64(uint64(effect.ID))
		h.writeUint64(uint64(effect.Kind))
		h.writeBytes(effect.Key)
		h.writeBytes(effect.Value)
		h.writeBool(effect.Concrete)
		h.writeBool(effect.Opaque)
		h.writeUint64(uint64(effect.MountKeyID))
		h.writeUint64(uint64(effect.Bucket))
		h.writeUint64(uint64(effect.RecordKind))
		h.writeBytes(effect.ValueHash[:])
		h.writeUint64(uint64(effect.Derivation))
	}
	return h.sum()
}

func proofSetFrontier(proofs []proof.PredicateProof) proof.ProofFrontier {
	for _, predicateProof := range proofs {
		if predicateProof.ProofFrontier.Valid() {
			return predicateProof.ProofFrontier
		}
	}
	return proof.ProofFrontier{}
}

func singleLinkInodeGuardSubject(proofs []proof.PredicateProof) ([32]byte, bool) {
	for _, proof := range proofs {
		if !proof.Present {
			continue
		}
		parts, ok := layout.InspectKey(proof.Key)
		if !ok || parts.Kind != layout.KeyKindInode {
			continue
		}
		inode, err := layout.DecodeInodeValue(proof.Value)
		if err != nil || inode.LinkCount != 1 {
			return [32]byte{}, false
		}
		return inodeGuardSubjectDigest(GuardSingleLinkInode, proof, inode), true
	}
	return [32]byte{}, false
}

func nonDirectoryGuardSubject(op CompiledOp, proofs []proof.PredicateProof) ([32]byte, bool) {
	for _, key := range nonDirectoryGuardCandidateKeys(op) {
		if subject, ok := nonDirectoryGuardSubjectForKey(proofs, key); ok {
			return subject, true
		}
	}
	for _, proof := range proofs {
		if !proof.Present {
			continue
		}
		parts, ok := layout.InspectKey(proof.Key)
		if !ok {
			continue
		}
		switch parts.Kind {
		case layout.KeyKindInode:
			inode, err := layout.DecodeInodeValue(proof.Value)
			if err != nil {
				return [32]byte{}, false
			}
			if inode.Type == model.InodeTypeDirectory {
				continue
			}
			return inodeGuardSubjectDigest(GuardNonDirectoryInode, proof, inode), true
		case layout.KeyKindDentry:
			dentry, err := layout.DecodeDentryValue(proof.Value)
			if err != nil {
				return [32]byte{}, false
			}
			if dentry.Type == model.InodeTypeDirectory {
				continue
			}
			return dentryGuardSubjectDigest(GuardNonDirectoryInode, proof, dentry), true
		}
	}
	return [32]byte{}, false
}

func nonDirectoryGuardCandidateKeys(op CompiledOp) [][]byte {
	plan := op.Delta.Plan
	switch op.Delta.Kind {
	case model.OperationLink:
		if len(plan.ReadKeys) > 0 {
			return [][]byte{plan.ReadKeys[0]}
		}
	case model.OperationOpenWriteSession:
		if len(plan.ReadKeys) > 0 {
			return [][]byte{plan.ReadKeys[0]}
		}
	}
	return nil
}

func nonDirectoryGuardSubjectForKey(proofs []proof.PredicateProof, key []byte) ([32]byte, bool) {
	if len(key) == 0 {
		return [32]byte{}, false
	}
	for _, proof := range proofs {
		if !proof.Present || !bytes.Equal(proof.Key, key) {
			continue
		}
		parts, ok := layout.InspectKey(proof.Key)
		if !ok {
			return [32]byte{}, false
		}
		switch parts.Kind {
		case layout.KeyKindInode:
			inode, err := layout.DecodeInodeValue(proof.Value)
			if err != nil || inode.Type == model.InodeTypeDirectory {
				return [32]byte{}, false
			}
			return inodeGuardSubjectDigest(GuardNonDirectoryInode, proof, inode), true
		case layout.KeyKindDentry:
			dentry, err := layout.DecodeDentryValue(proof.Value)
			if err != nil || dentry.Type == model.InodeTypeDirectory {
				return [32]byte{}, false
			}
			return dentryGuardSubjectDigest(GuardNonDirectoryInode, proof, dentry), true
		default:
			return [32]byte{}, false
		}
	}
	return [32]byte{}, false
}

func liveSessionGuardSubject(proofs []proof.PredicateProof) ([32]byte, bool) {
	for _, proof := range proofs {
		if !proof.Present {
			continue
		}
		parts, ok := layout.InspectKey(proof.Key)
		if !ok || parts.Kind != layout.KeyKindSession {
			continue
		}
		session, err := layout.DecodeSessionValue(proof.Value)
		if err != nil || session.Session == "" || session.Inode == 0 {
			return [32]byte{}, false
		}
		return sessionGuardSubjectDigest(GuardLiveSession, proof, session), true
	}
	return [32]byte{}, false
}

func expiredSessionOwnerGuardSubject(proofs []proof.PredicateProof) ([32]byte, bool) {
	h := newDigestBuilder()
	h.writeString(string(GuardExpiredSessionOwner))
	count := uint64(0)
	for _, proof := range proofs {
		parts, ok := layout.InspectKey(proof.Key)
		if !ok || parts.Kind != layout.KeyKindSession {
			continue
		}
		if proof.Present {
			return [32]byte{}, false
		}
		h.writeBytes(proof.Key)
		h.writeBytes(proof.Digest[:])
		count++
	}
	if count == 0 {
		return [32]byte{}, false
	}
	h.writeUint64(count)
	return h.sum(), true
}

func sameAuthorityGuardSubject(op CompiledOp) ([32]byte, bool) {
	if op.Footprint.HasPrefixRead || op.Footprint.HasOpaqueKeys {
		return [32]byte{}, false
	}
	scope := op.Authority.Scope
	if scope.MountKeyID == 0 || !authorityScopeCoversBuckets(scope, op.Placement.Buckets) {
		return [32]byte{}, false
	}
	for _, ref := range op.Footprint.Reads {
		if !authorityScopeCoversKey(scope, ref) {
			return [32]byte{}, false
		}
	}
	for _, ref := range op.Footprint.Writes {
		if !authorityScopeCoversKey(scope, ref) {
			return [32]byte{}, false
		}
	}
	h := newDigestBuilder()
	h.writeString(string(GuardSameAuthority))
	h.writeAuthorityScope(scope)
	footprintDigest := KeyFootprintDigest(op.Footprint)
	h.writeBytes(footprintDigest[:])
	return h.sum(), true
}

func quotaCreditGuardSubject(op CompiledOp) ([32]byte, bool) {
	if len(op.Effects) == 0 || op.Footprint.HasPrefixRead || !op.Placement.CanSegment {
		return [32]byte{}, false
	}
	h := newDigestBuilder()
	h.writeString(string(GuardQuotaCredit))
	h.writeAuthorityScope(op.Authority.Scope)
	effectDigest := EffectPlanDigest(op.Effects)
	h.writeBytes(effectDigest[:])
	return h.sum(), true
}

func emptyDirectoryGuardSubject(op CompiledOp, proofs []proof.PredicateProof) ([32]byte, bool) {
	if op.Delta.Kind == model.OperationRemoveDirectory {
		if subject, ok := removeDirectoryGuardSubject(op, proofs); ok {
			return subject, true
		}
		return [32]byte{}, false
	}
	for _, proof := range proofs {
		if !proof.Present {
			continue
		}
		parts, ok := layout.InspectKey(proof.Key)
		if !ok || parts.Kind != layout.KeyKindInode {
			continue
		}
		inode, err := layout.DecodeInodeValue(proof.Value)
		if err != nil || inode.Type != model.InodeTypeDirectory || inode.ChildCount != 0 {
			return [32]byte{}, false
		}
		return inodeGuardSubjectDigest(GuardEmptyDirectory, proof, inode), true
	}
	return [32]byte{}, false
}

func removeDirectoryGuardSubject(op CompiledOp, proofs []proof.PredicateProof) ([32]byte, bool) {
	var dentry model.DentryRecord
	var haveDentry bool
	for _, predicateProof := range proofs {
		if !predicateProof.Present || !bytes.Equal(predicateProof.Key, op.Delta.Plan.PrimaryKey) {
			continue
		}
		record, err := layout.DecodeDentryValue(predicateProof.Value)
		if err != nil || record.Type != model.InodeTypeDirectory {
			return [32]byte{}, false
		}
		dentry = record
		haveDentry = true
		break
	}
	if !haveDentry {
		return [32]byte{}, false
	}
	for _, predicateProof := range proofs {
		if !predicateProof.Present {
			continue
		}
		parts, ok := layout.InspectKey(predicateProof.Key)
		if !ok || parts.Kind != layout.KeyKindInode || parts.Inode != dentry.Inode {
			continue
		}
		inode, err := layout.DecodeInodeValue(predicateProof.Value)
		if err != nil || inode.Type != model.InodeTypeDirectory || inode.ChildCount != 0 || inode.Inode == model.RootInode {
			return [32]byte{}, false
		}
		return inodeGuardSubjectDigest(GuardEmptyDirectory, predicateProof, inode), true
	}
	return [32]byte{}, false
}

func inodeGuardSubjectDigest(guard RuntimeGuard, proof proof.PredicateProof, inode model.InodeRecord) [32]byte {
	h := newDigestBuilder()
	h.writeString(string(guard))
	h.writeBytes(proof.Key)
	h.writeBytes(proof.Digest[:])
	h.writeUint64(uint64(inode.Inode))
	h.writeString(string(inode.Type))
	h.writeUint64(uint64(inode.LinkCount))
	h.writeUint64(inode.ChildCount)
	return h.sum()
}

func dentryGuardSubjectDigest(guard RuntimeGuard, proof proof.PredicateProof, dentry model.DentryRecord) [32]byte {
	h := newDigestBuilder()
	h.writeString(string(guard))
	h.writeBytes(proof.Key)
	h.writeBytes(proof.Digest[:])
	h.writeUint64(uint64(dentry.Parent))
	h.writeString(dentry.Name)
	h.writeUint64(uint64(dentry.Inode))
	h.writeString(string(dentry.Type))
	return h.sum()
}

func sessionGuardSubjectDigest(guard RuntimeGuard, proof proof.PredicateProof, session model.SessionRecord) [32]byte {
	h := newDigestBuilder()
	h.writeString(string(guard))
	h.writeBytes(proof.Key)
	h.writeBytes(proof.Digest[:])
	h.writeString(string(session.Session))
	h.writeUint64(uint64(session.Inode))
	h.writeUint64(uint64(session.ExpiresUnixNs))
	return h.sum()
}

func ExecutionPlanDigest(segment SegmentPlan, atomicity AtomicityGroup, durability DurabilityClass) [32]byte {
	h := newDigestBuilder()
	h.writeSegmentMergeKey(segment.MergeKey)
	h.writeUint64(uint64(segment.Install))
	h.writeSegmentMergeKey(segment.MaterializeMergeKey)
	h.writeUint64(uint64(segment.MaterializeInstall))
	h.writeBool(segment.CanAppend)
	h.writeBool(segment.CanMaterialize)
	h.writeBool(segment.RequiresMaterialize)
	h.writeUint64(segment.EstimatedPayloadBytes)
	h.writeUint64(uint64(segment.OperationCount))
	h.writeUint64(uint64(segment.MutationCount))
	h.writeUint64(uint64(atomicity.Recovery))
	h.writeBool(atomicity.Splittable)
	h.writeBytes(atomicity.Digest[:])
	h.writeUint64(uint64(len(atomicity.Members)))
	for _, member := range atomicity.Members {
		h.writeUint64(uint64(member))
	}
	h.writeUint64(uint64(durability))
	return h.sum()
}

func GuardProofSetDigest(proofs []proof.GuardProof) [32]byte {
	if len(proofs) == 0 {
		return [32]byte{}
	}
	h := newDigestBuilder()
	h.writeUint64(uint64(len(proofs)))
	for _, proof := range proofs {
		h.writeUint64(uint64(proof.SchemaVersion))
		h.writeString(string(proof.Guard))
		h.writeBool(proof.Passed)
		h.writeUint64(uint64(proof.Evidence.SchemaVersion))
		h.writeString(string(proof.Evidence.Guard))
		h.writeUint64(uint64(proof.Evidence.Kind))
		h.writeBytes(proof.Evidence.DescriptorDigest[:])
		h.writeBytes(proof.Evidence.PredicateProofDigest[:])
		h.writeBytes(proof.Evidence.FootprintDigest[:])
		h.writeBytes(proof.Evidence.EffectDigest[:])
		h.writeBytes(proof.Evidence.SubjectDigest[:])
		h.writeUint64(proof.Evidence.ProofFrontier.EpochID)
		h.writeUint64(proof.Evidence.ProofFrontier.Sequence)
		h.writeBytes(proof.Digest[:])
	}
	return h.sum()
}

func AdmissionProofSetDigest(predicates []proof.PredicateProof, guards []proof.GuardProof) [32]byte {
	h := newDigestBuilder()
	predicateDigest := PredicateProofSetDigest(predicates)
	guardDigest := GuardProofSetDigest(guards)
	h.writeBytes(predicateDigest[:])
	h.writeBytes(guardDigest[:])
	return h.sum()
}

func descriptorDigest(delta SemanticDelta) [32]byte {
	h := newDigestBuilder()
	h.writeString(string(delta.Kind))
	h.writeString(delta.Eligibility.String())
	h.writeString(string(delta.SlowReason))
	if delta.DurabilityBarrier {
		h.writeUint64(1)
	}
	if delta.WatchAtSeal {
		h.writeUint64(1)
	}
	h.writeUint64(uint64(delta.Authority.MountKeyID))
	for _, bucket := range delta.Authority.Buckets {
		h.writeUint64(uint64(bucket))
	}
	for _, predicate := range delta.ReadPredicates {
		h.writeUint64(uint64(predicate.Kind))
		h.writeBytes(predicate.Key)
		if predicate.HasExpectedValue {
			h.writeBytes(predicate.ExpectedValue)
		}
	}
	for _, effect := range delta.WriteEffects {
		h.writeUint64(uint64(effect.Kind))
		h.writeBytes(effect.Key)
		h.writeBytes(effect.Value)
	}
	for _, guard := range delta.RuntimeGuards {
		h.writeString(string(guard))
	}
	return h.sum()
}

type digestBuilder struct {
	stack [512]byte
	off   int
	heap  []byte
}

func newDigestBuilder() digestBuilder {
	return digestBuilder{}
}

func (b *digestBuilder) writeSegmentMergeKey(key SegmentMergeKey) {
	b.writeUint64(uint64(key.MountKeyID))
	b.writeBool(key.HasPrimaryBucket)
	b.writeUint64(uint64(key.PrimaryBucket))
	b.writeUint64(uint64(key.Install))
	b.writeUint64(uint64(key.Durability))
	b.writeUint64(uint64(key.FormatVersion))
}

func (b *digestBuilder) writeKeyRef(ref KeyRef) {
	b.writeUint64(uint64(ref.Mode))
	b.writeBytes(ref.Key)
	b.writeBool(ref.Opaque)
	b.writeUint64(uint64(ref.MountKeyID))
	b.writeUint64(uint64(ref.Bucket))
	b.writeUint64(uint64(ref.Kind))
	b.writeUint64(uint64(ref.Parent))
	b.writeUint64(uint64(ref.Inode))
}

func (b *digestBuilder) writeAuthorityScope(scope AuthorityScope) {
	b.writeString(string(scope.Mount))
	b.writeUint64(uint64(scope.MountKeyID))
	b.writeBool(scope.Broad)
	b.writeBool(scope.AllowOpaqueKeys)
	b.writeUint64(uint64(len(scope.Buckets)))
	for _, bucket := range scope.Buckets {
		b.writeUint64(uint64(bucket))
	}
	b.writeUint64(uint64(len(scope.Parents)))
	for _, parent := range scope.Parents {
		b.writeUint64(uint64(parent))
	}
	b.writeUint64(uint64(len(scope.Inodes)))
	for _, inode := range scope.Inodes {
		b.writeUint64(uint64(inode))
	}
}

func (b *digestBuilder) writeString(value string) {
	b.writeUint64(uint64(len(value)))
	b.writeRawString(value)
}

func (b *digestBuilder) writeBytes(value []byte) {
	b.writeUint64(uint64(len(value)))
	b.writeRaw(value)
}

func (b *digestBuilder) writeRaw(value []byte) {
	if len(value) == 0 {
		return
	}
	if b.heap != nil {
		b.heap = append(b.heap, value...)
		return
	}
	if len(value) <= len(b.stack)-b.off {
		copy(b.stack[b.off:], value)
		b.off += len(value)
		return
	}
	b.spill(len(value))
	b.heap = append(b.heap, value...)
}

func (b *digestBuilder) writeUint64(value uint64) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], value)
	b.writeRaw(buf[:])
}

func (b *digestBuilder) writeBool(value bool) {
	if value {
		b.writeUint64(1)
		return
	}
	b.writeUint64(0)
}

func (b *digestBuilder) sum() [32]byte {
	if b.heap != nil {
		return sha256.Sum256(b.heap)
	}
	return sha256.Sum256(b.stack[:b.off])
}

func (b *digestBuilder) writeRawString(value string) {
	if len(value) == 0 {
		return
	}
	if b.heap != nil {
		b.heap = append(b.heap, value...)
		return
	}
	if len(value) <= len(b.stack)-b.off {
		b.off += copy(b.stack[b.off:], value)
		return
	}
	b.spill(len(value))
	b.heap = append(b.heap, value...)
}

func (b *digestBuilder) spill(extra int) {
	needed := b.off + extra
	capacity := max(needed, len(b.stack)*2)
	b.heap = make([]byte, b.off, capacity)
	copy(b.heap, b.stack[:b.off])
}

func cloneDelta(delta SemanticDelta) SemanticDelta {
	return SemanticDelta{
		Kind:              delta.Kind,
		Plan:              clonePlan(delta.Plan),
		Authority:         cloneScope(delta.Authority),
		ReadPredicates:    clonePredicates(delta.ReadPredicates),
		WriteEffects:      cloneEffects(delta.WriteEffects),
		RuntimeGuards:     append([]RuntimeGuard(nil), delta.RuntimeGuards...),
		Eligibility:       delta.Eligibility,
		SlowReason:        delta.SlowReason,
		DurabilityBarrier: delta.DurabilityBarrier,
		WatchAtSeal:       delta.WatchAtSeal,
	}
}

func clonePredicateProofs(proofs []proof.PredicateProof) []proof.PredicateProof {
	if len(proofs) == 0 {
		return nil
	}
	out := make([]proof.PredicateProof, len(proofs))
	for i, predicateProof := range proofs {
		out[i] = proof.PredicateProof{
			SchemaVersion: predicateProof.SchemaVersion,
			Rule:          predicateProof.Rule,
			Key:           cloneBytes(predicateProof.Key),
			Present:       predicateProof.Present,
			Value:         cloneBytes(predicateProof.Value),
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

func cloneGuardProofs(proofs []proof.GuardProof) []proof.GuardProof {
	if len(proofs) == 0 {
		return nil
	}
	out := make([]proof.GuardProof, len(proofs))
	copy(out, proofs)
	return out
}
