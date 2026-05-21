// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package compile turns fsmeta requests into generated semantic programs.
//
// The compiler is deliberately conservative. Generated programs describe the
// static key footprint, concrete or symbolic effects, and the guards an
// optimized runtime must prove before bypassing the ordinary Percolator/Raft
// path. The compiler does not execute reads, does not allocate timestamps, and
// does not weaken the current fsmeta executor.
package compile

import (
	"slices"

	"github.com/feichai0017/NoKV/fsmeta"
)

// Eligibility describes whether a request can enter the visible optimized write
// path after the listed runtime guards have been checked by the holder.
type Eligibility uint8

const (
	EligibilityVisibleCommit Eligibility = iota
	EligibilitySlowPath
)

func (e Eligibility) String() string {
	switch e {
	case EligibilityVisibleCommit:
		return "visible_commit"
	case EligibilitySlowPath:
		return "slow_path"
	default:
		return "unknown"
	}
}

// SlowReason records why a request must remain on the current Percolator/Raft
// path.
type SlowReason string

const (
	SlowReasonNone              SlowReason = ""
	SlowReasonReadOnly          SlowReason = "read_only"
	SlowReasonRangeRead         SlowReason = "range_read"
	SlowReasonDurabilityBarrier SlowReason = "durability_barrier"
	SlowReasonCrossBucket       SlowReason = "cross_bucket"
	SlowReasonSharedQuota       SlowReason = "shared_quota"
	SlowReasonDynamicWriteSet   SlowReason = "dynamic_write_set"
	SlowReasonMaintenanceScan   SlowReason = "maintenance_scan"
)

// RuntimeGuard is a condition an optimized runtime must verify against its
// merged holder view before it can admit the operation.
type RuntimeGuard string

const (
	GuardSingleLinkInode     RuntimeGuard = "single_link_inode"
	GuardSameAuthority       RuntimeGuard = "same_authority"
	GuardNonDirectoryInode   RuntimeGuard = "non_directory_inode"
	GuardEmptyDirectory      RuntimeGuard = "empty_directory"
	GuardLiveSession         RuntimeGuard = "live_session"
	GuardExpiredSessionOwner RuntimeGuard = "expired_session_owner"
	GuardQuotaCredit         RuntimeGuard = "quota_credit"
)

// PredicateKind is the static predicate class carried by a semantic delta.
type PredicateKind uint8

const (
	PredicateExists PredicateKind = iota
	PredicateNotExists
	PredicateObservedValue
	PredicatePrefixScan
)

// Predicate describes one key or prefix condition. ExpectedValue is set only
// for PredicateObservedValue after the executor has observed a concrete value.
type Predicate struct {
	Kind             PredicateKind
	Key              []byte
	ExpectedValue    []byte
	HasExpectedValue bool
	RuntimeChecked   bool
}

// EffectKind is the mutation class an optimized runtime would eventually
// replay.
type EffectKind uint8

const (
	EffectPut EffectKind = iota
	EffectDelete
	EffectDerivedPut
	EffectDerivedDelete
)

// WriteEffect describes a static write target. Value is present only when the
// request itself fully determines the bytes, such as Create.
type WriteEffect struct {
	Kind  EffectKind
	Key   []byte
	Value []byte
}

// AuthorityScope is the mount-local scope a holder grant must cover. It is a
// runtime contract, not persisted root truth.
type AuthorityScope struct {
	Mount           fsmeta.MountID
	MountKeyID      fsmeta.MountKeyID
	Buckets         []fsmeta.AffinityBucket
	Parents         []fsmeta.InodeID
	Inodes          []fsmeta.InodeID
	Broad           bool
	AllowOpaqueKeys bool
}

// SemanticDelta is the request-time semantic program produced by generated
// compile entries. It carries the authority scope, predicate obligations,
// symbolic/concrete effects, and slow-path decision before runtime evidence is
// attached. Runtime code must materialize it into MaterializedOp before holder
// admission.
type SemanticDelta struct {
	Kind              fsmeta.OperationKind
	Plan              fsmeta.OperationPlan
	Authority         AuthorityScope
	ReadPredicates    []Predicate
	WriteEffects      []WriteEffect
	RuntimeGuards     []RuntimeGuard
	Eligibility       Eligibility
	SlowReason        SlowReason
	DurabilityBarrier bool
	WatchAtSeal       bool
}

var emptyKeySet = [][]byte{}

type QuotaMode uint8

const (
	QuotaModeNone QuotaMode = iota
	QuotaModeEscrow
	QuotaModeShared
)

type Options struct {
	QuotaMode QuotaMode
}

type Option struct {
	quotaMode    QuotaMode
	setQuotaMode bool
}

func WithQuotaMode(mode QuotaMode) Option {
	return Option{quotaMode: mode, setQuotaMode: true}
}

func canonicalPlan(plan fsmeta.OperationPlan) fsmeta.OperationPlan {
	if plan.ReadKeys == nil {
		plan.ReadKeys = emptyKeySet
	}
	if plan.ReadPrefixes == nil {
		plan.ReadPrefixes = emptyKeySet
	}
	if plan.MutateKeys == nil {
		plan.MutateKeys = emptyKeySet
	}
	return plan
}

func applyQuotaPolicy(delta SemanticDelta, opts Options, guard RuntimeGuard) SemanticDelta {
	switch opts.QuotaMode {
	case QuotaModeShared:
		delta.Eligibility = EligibilitySlowPath
		delta.SlowReason = SlowReasonSharedQuota
	case QuotaModeEscrow:
		delta.RuntimeGuards = append(delta.RuntimeGuards, guard)
	}
	return delta
}

func collectOptions(opts ...Option) Options {
	var out Options
	for _, opt := range opts {
		if opt.setQuotaMode {
			out.QuotaMode = opt.quotaMode
		}
	}
	return out
}

func scopeFor(mount fsmeta.MountIdentity, parents, inodes []fsmeta.InodeID) AuthorityScope {
	scope := AuthorityScope{
		Mount:      mount.MountID,
		MountKeyID: mount.MountKeyID,
		Parents:    uniqueInodes(parents),
		Inodes:     uniqueInodes(inodes),
	}
	buckets := make([]fsmeta.AffinityBucket, 0, len(scope.Parents)+len(scope.Inodes))
	for _, parent := range scope.Parents {
		buckets = append(buckets, fsmeta.BucketForInodeID(parent))
	}
	for _, inode := range scope.Inodes {
		buckets = append(buckets, fsmeta.BucketForInodeID(inode))
	}
	scope.Buckets = uniqueBuckets(buckets)
	return scope
}

func uniqueInodes(in []fsmeta.InodeID) []fsmeta.InodeID {
	switch len(in) {
	case 0:
		return nil
	case 1:
		if in[0] == 0 {
			return nil
		}
		return []fsmeta.InodeID{in[0]}
	case 2:
		left, right := in[0], in[1]
		switch {
		case left == 0 && right == 0:
			return nil
		case left == 0:
			return []fsmeta.InodeID{right}
		case right == 0 || left == right:
			return []fsmeta.InodeID{left}
		case left < right:
			return []fsmeta.InodeID{left, right}
		default:
			return []fsmeta.InodeID{right, left}
		}
	}
	out := make([]fsmeta.InodeID, 0, len(in))
	seen := make(map[fsmeta.InodeID]struct{}, len(in))
	for _, id := range in {
		if id == 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	slices.Sort(out)
	return out
}

func uniqueBuckets(in []fsmeta.AffinityBucket) []fsmeta.AffinityBucket {
	switch len(in) {
	case 0:
		return nil
	case 1:
		return []fsmeta.AffinityBucket{in[0]}
	case 2:
		left, right := in[0], in[1]
		switch {
		case left == right:
			return []fsmeta.AffinityBucket{left}
		case left < right:
			return []fsmeta.AffinityBucket{left, right}
		default:
			return []fsmeta.AffinityBucket{right, left}
		}
	}
	out := make([]fsmeta.AffinityBucket, 0, len(in))
	seen := make(map[fsmeta.AffinityBucket]struct{}, len(in))
	for _, bucket := range in {
		if _, ok := seen[bucket]; ok {
			continue
		}
		seen[bucket] = struct{}{}
		out = append(out, bucket)
	}
	slices.Sort(out)
	return out
}

func clonePlan(plan fsmeta.OperationPlan) fsmeta.OperationPlan {
	return fsmeta.OperationPlan{
		Kind:         plan.Kind,
		Mount:        plan.Mount,
		PrimaryKey:   cloneBytes(plan.PrimaryKey),
		StartKey:     cloneBytes(plan.StartKey),
		Limit:        plan.Limit,
		ReadKeys:     cloneKeySet(plan.ReadKeys),
		ReadPrefixes: cloneKeySet(plan.ReadPrefixes),
		MutateKeys:   cloneKeySet(plan.MutateKeys),
	}
}

func cloneScope(scope AuthorityScope) AuthorityScope {
	return AuthorityScope{
		Mount:           scope.Mount,
		MountKeyID:      scope.MountKeyID,
		Buckets:         append([]fsmeta.AffinityBucket(nil), scope.Buckets...),
		Parents:         append([]fsmeta.InodeID(nil), scope.Parents...),
		Inodes:          append([]fsmeta.InodeID(nil), scope.Inodes...),
		Broad:           scope.Broad,
		AllowOpaqueKeys: scope.AllowOpaqueKeys,
	}
}

func clonePredicates(in []Predicate) []Predicate {
	out := make([]Predicate, 0, len(in))
	for _, predicate := range in {
		out = append(out, Predicate{
			Kind:             predicate.Kind,
			Key:              cloneBytes(predicate.Key),
			ExpectedValue:    cloneBytes(predicate.ExpectedValue),
			HasExpectedValue: predicate.HasExpectedValue,
			RuntimeChecked:   predicate.RuntimeChecked,
		})
	}
	return out
}

func cloneEffects(in []WriteEffect) []WriteEffect {
	out := make([]WriteEffect, 0, len(in))
	for _, effect := range in {
		out = append(out, WriteEffect{
			Kind:  effect.Kind,
			Key:   cloneBytes(effect.Key),
			Value: cloneBytes(effect.Value),
		})
	}
	return out
}

func cloneKeySet(in [][]byte) [][]byte {
	out := make([][]byte, 0, len(in))
	for _, key := range in {
		out = append(out, cloneBytes(key))
	}
	return out
}

func cloneBytes(in []byte) []byte {
	if in == nil {
		return nil
	}
	return append([]byte(nil), in...)
}
