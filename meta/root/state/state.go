// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package state holds the compact applied root state of the metadata
// kernel (State, grant lifecycle, pending peer/range changes) and the
// ApplyEventToState / ApplyEventToSnapshot functions
// that drive a rooted event log into that state.
//
// This package is the only place where the meaning of a typed rooted
// event is codified. meta/root/replicated persists the events through a
// 3-peer raft quorum; callers under meta/root/server, meta/root/client,
// coordinator/, and raftstore/ consume the resulting State as truth.
package state

import (
	"fmt"
	"maps"

	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	"github.com/feichai0017/NoKV/meta/topology"
)

type Cursor = rootproto.Cursor

// AllocatorKind identifies one globally fenced allocator domain inside rooted
// metadata state.
type AllocatorKind uint8

const (
	AllocatorKindUnknown AllocatorKind = iota
	AllocatorKindID
	AllocatorKindTSO
)

// State is the compact checkpointed state of the metadata root.
type State struct {
	ClusterEpoch          uint64
	MembershipEpoch       uint64
	LastCommitted         Cursor
	IDFence               uint64
	TSOFence              uint64
	ActiveGrants          []rootproto.AuthorityGrant
	RetiredGrants         []rootproto.GrantRetirement
	GrantInheritances     []rootproto.GrantInheritance
	RetiredEraFloors      []rootproto.AuthorityRetiredEraFloor
	ActiveVisibleGrants   []rootproto.VisibleAuthorityGrant
	VisibleAuthorityEpoch uint64
	VisibleAuthoritySeals []rootproto.VisibleAuthoritySeal
}

func (s State) ActiveGrantFor(duty rootproto.DutyID, scope rootproto.DutyScope) (rootproto.AuthorityGrant, bool) {
	for _, grant := range s.ActiveGrants {
		if grant.CoversDutyKey(rootproto.DutyKey{DutyID: duty, Scope: scope}) {
			return cloneAuthorityGrant(grant), true
		}
	}
	return rootproto.AuthorityGrant{}, false
}

func (s State) ActiveGrantByID(grantID string) (rootproto.AuthorityGrant, bool) {
	for _, grant := range s.ActiveGrants {
		if grant.GrantID == grantID {
			return cloneAuthorityGrant(grant), true
		}
	}
	return rootproto.AuthorityGrant{}, false
}

type StoreMembershipState uint8

const (
	StoreMembershipUnknown StoreMembershipState = iota
	StoreMembershipActive
	StoreMembershipRetired
)

// StoreMembership is the rooted membership truth for one store ID. Runtime
// liveness and network addresses are intentionally kept out of this record.
type StoreMembership struct {
	StoreID   uint64
	State     StoreMembershipState
	JoinedAt  Cursor
	RetiredAt Cursor
}

type MountState uint8

const (
	MountStateUnknown MountState = iota
	MountStateActive
	MountStateRetired
)

// MountRecord is rooted truth for one fsmeta mount. Runtime fsmeta sessions
// and cache state are intentionally excluded.
type MountRecord struct {
	MountID       string
	MountKeyID    uint64
	RootInode     uint64
	SchemaVersion uint32
	State         MountState
	RegisteredAt  Cursor
	RetiredAt     Cursor
}

type SubtreeAuthorityState uint8

const (
	SubtreeAuthorityUnknown SubtreeAuthorityState = iota
	SubtreeAuthorityActive
	SubtreeAuthorityHandoff
)

// SubtreeAuthority is rooted truth for one subtree authority era. The record is
// keyed by (mount, root inode); dentry mutations remain data-plane writes.
type SubtreeAuthority struct {
	SubtreeID              string
	Mount                  string
	RootInode              uint64
	AuthorityID            string
	Era                    uint64
	Frontier               uint64
	State                  SubtreeAuthorityState
	DeclaredAt             Cursor
	HandoffStartedAt       Cursor
	HandoffCompletedAt     Cursor
	PredecessorAuthorityID string
	PredecessorEra         uint64
	PredecessorFrontier    uint64
	SuccessorAuthorityID   string
	SuccessorEra           uint64
	InheritedFrontier      uint64
}

// QuotaFence is rooted quota truth for one mount or subtree. RootInode 0 means
// mount-wide.
type QuotaFence struct {
	SubjectID   string
	Mount       string
	RootInode   uint64
	LimitBytes  uint64
	LimitInodes uint64
	Era         uint64
	Frontier    uint64
	UpdatedAt   Cursor
}

func SubtreeAuthorityKey(mount string, rootInode uint64) string {
	if mount == "" || rootInode == 0 {
		return ""
	}
	return fmt.Sprintf("%s/%d", mount, rootInode)
}

func QuotaFenceKey(mount string, rootInode uint64) string {
	return rootevent.QuotaFenceID(mount, rootInode)
}

func SubtreeAuthorityID(mount string, rootInode, era uint64) string {
	if mount == "" || rootInode == 0 {
		return ""
	}
	return fmt.Sprintf("%s/%d#%d", mount, rootInode, era)
}

type PendingPeerChangeKind uint8

const (
	PendingPeerChangeUnknown PendingPeerChangeKind = iota
	PendingPeerChangeAddition
	PendingPeerChangeRemoval
)

type PendingPeerChange struct {
	Kind    PendingPeerChangeKind
	StoreID uint64
	PeerID  uint64
	Base    topology.Descriptor
	Target  topology.Descriptor
}

type PendingRangeChangeKind uint8

const (
	PendingRangeChangeUnknown PendingRangeChangeKind = iota
	PendingRangeChangeSplit
	PendingRangeChangeMerge
)

type PendingRangeChange struct {
	Kind           PendingRangeChangeKind
	ParentRegionID uint64
	LeftRegionID   uint64
	RightRegionID  uint64
	BaseParent     topology.Descriptor
	BaseLeft       topology.Descriptor
	BaseRight      topology.Descriptor
	Left           topology.Descriptor
	Right          topology.Descriptor
	Merged         topology.Descriptor
}

// Snapshot is the compact materialized rooted metadata state used for bounded bootstrap and recovery.
type Snapshot struct {
	State               State
	Stores              map[uint64]StoreMembership
	SnapshotEpochs      map[string]SnapshotEpoch
	Mounts              map[string]MountRecord
	Subtrees            map[string]SubtreeAuthority
	Quotas              map[string]QuotaFence
	Descriptors         map[uint64]topology.Descriptor
	PendingPeerChanges  map[uint64]PendingPeerChange
	PendingRangeChanges map[uint64]PendingRangeChange
}

// CommitInfo reports one successful root append together with the resulting compact root state.
type CommitInfo struct {
	Cursor Cursor
	State  State
}

func CloneSnapshot(snapshot Snapshot) Snapshot {
	out := Snapshot{
		State:               CloneState(snapshot.State),
		Stores:              CloneStoreMemberships(snapshot.Stores),
		SnapshotEpochs:      CloneSnapshotEpochs(snapshot.SnapshotEpochs),
		Mounts:              CloneMounts(snapshot.Mounts),
		Subtrees:            CloneSubtreeAuthorities(snapshot.Subtrees),
		Quotas:              CloneQuotaFences(snapshot.Quotas),
		Descriptors:         CloneDescriptors(snapshot.Descriptors),
		PendingPeerChanges:  ClonePendingPeerChanges(snapshot.PendingPeerChanges),
		PendingRangeChanges: ClonePendingRangeChanges(snapshot.PendingRangeChanges),
	}
	return out
}

func CloneState(state State) State {
	state.ActiveGrants = cloneAuthorityGrants(state.ActiveGrants)
	state.RetiredGrants = append([]rootproto.GrantRetirement(nil), state.RetiredGrants...)
	state.GrantInheritances = append([]rootproto.GrantInheritance(nil), state.GrantInheritances...)
	state.RetiredEraFloors = rootproto.CloneAuthorityRetiredEraFloors(state.RetiredEraFloors)
	state.ActiveVisibleGrants = cloneVisibleAuthorityGrants(state.ActiveVisibleGrants)
	state.VisibleAuthoritySeals = cloneVisibleAuthoritySeals(state.VisibleAuthoritySeals)
	return state
}

func CloneMounts(in map[string]MountRecord) map[string]MountRecord {
	if len(in) == 0 {
		return make(map[string]MountRecord)
	}
	out := make(map[string]MountRecord, len(in))
	maps.Copy(out, in)
	return out
}

func CloneSubtreeAuthorities(in map[string]SubtreeAuthority) map[string]SubtreeAuthority {
	if in == nil {
		return nil
	}
	if len(in) == 0 {
		return make(map[string]SubtreeAuthority)
	}
	out := make(map[string]SubtreeAuthority, len(in))
	maps.Copy(out, in)
	return out
}

func CloneQuotaFences(in map[string]QuotaFence) map[string]QuotaFence {
	if in == nil {
		return nil
	}
	if len(in) == 0 {
		return make(map[string]QuotaFence)
	}
	out := make(map[string]QuotaFence, len(in))
	maps.Copy(out, in)
	return out
}

type SnapshotEpoch struct {
	SnapshotID      string
	Mount           string
	MountKeyID      uint64
	RootInode       uint64
	ReadVersion     uint64
	PublishedAt     Cursor
	RuntimeEvidence []rootproto.SnapshotEvidenceRef
}

// SnapshotRetentionIndex summarizes active snapshot read-version floors.
// GlobalFloor is the oldest active snapshot across all mounts. MountFloors
// narrows the same retention pressure to one numeric fsmeta storage mount.
type SnapshotRetentionIndex struct {
	GlobalFloor uint64
	MountFloors map[uint64]uint64
}

// Active reports whether at least one snapshot epoch contributes a retention
// floor.
func (i SnapshotRetentionIndex) Active() bool {
	return i.GlobalFloor != 0
}

// FloorForMount returns the oldest active snapshot read version for one
// numeric storage mount.
func (i SnapshotRetentionIndex) FloorForMount(mountKeyID uint64) (uint64, bool) {
	if mountKeyID == 0 || len(i.MountFloors) == 0 {
		return 0, false
	}
	floor, ok := i.MountFloors[mountKeyID]
	return floor, ok
}

// SnapshotRetentionFloor returns the oldest active snapshot read version.
// Data-plane MVCC GC must not discard versions needed by any active snapshot
// below this floor. The bool is false when no snapshot epoch is active.
func SnapshotRetentionFloor(epochs map[string]SnapshotEpoch) (uint64, bool) {
	index := SnapshotRetentionIndexFor(epochs)
	return index.GlobalFloor, index.Active()
}

// SnapshotRetentionIndexFor returns both global and mount-scoped retention
// floors for active snapshot epochs.
func SnapshotRetentionIndexFor(epochs map[string]SnapshotEpoch) SnapshotRetentionIndex {
	index := SnapshotRetentionIndex{MountFloors: make(map[uint64]uint64)}
	for _, epoch := range epochs {
		if epoch.ReadVersion == 0 {
			continue
		}
		if index.GlobalFloor == 0 || epoch.ReadVersion < index.GlobalFloor {
			index.GlobalFloor = epoch.ReadVersion
		}
		if epoch.MountKeyID == 0 {
			continue
		}
		if current := index.MountFloors[epoch.MountKeyID]; current == 0 || epoch.ReadVersion < current {
			index.MountFloors[epoch.MountKeyID] = epoch.ReadVersion
		}
	}
	return index
}

// SnapshotRetentionFloor returns the oldest active snapshot read version in s.
func (s Snapshot) SnapshotRetentionFloor() (uint64, bool) {
	return SnapshotRetentionFloor(s.SnapshotEpochs)
}

// SnapshotRetentionIndex returns global and per-mount active snapshot floors in
// s.
func (s Snapshot) SnapshotRetentionIndex() SnapshotRetentionIndex {
	return SnapshotRetentionIndexFor(s.SnapshotEpochs)
}

func CloneSnapshotEpochs(in map[string]SnapshotEpoch) map[string]SnapshotEpoch {
	if len(in) == 0 {
		return make(map[string]SnapshotEpoch)
	}
	out := make(map[string]SnapshotEpoch, len(in))
	for id, epoch := range in {
		epoch.RuntimeEvidence = rootproto.CloneSnapshotEvidenceRefs(epoch.RuntimeEvidence)
		out[id] = epoch
	}
	return out
}

func CloneStoreMemberships(in map[uint64]StoreMembership) map[uint64]StoreMembership {
	if len(in) == 0 {
		return make(map[uint64]StoreMembership)
	}
	out := make(map[uint64]StoreMembership, len(in))
	maps.Copy(out, in)
	return out
}

func CloneDescriptors(in map[uint64]topology.Descriptor) map[uint64]topology.Descriptor {
	if len(in) == 0 {
		return make(map[uint64]topology.Descriptor)
	}
	out := make(map[uint64]topology.Descriptor, len(in))
	for id, desc := range in {
		out[id] = desc.Clone()
	}
	return out
}

func MaxDescriptorRevision(descriptors map[uint64]topology.Descriptor) uint64 {
	var maxEpoch uint64
	for _, desc := range descriptors {
		if desc.RootEpoch > maxEpoch {
			maxEpoch = desc.RootEpoch
		}
	}
	return maxEpoch
}

func ClonePendingPeerChanges(in map[uint64]PendingPeerChange) map[uint64]PendingPeerChange {
	if len(in) == 0 {
		return make(map[uint64]PendingPeerChange)
	}
	out := make(map[uint64]PendingPeerChange, len(in))
	for id, change := range in {
		out[id] = PendingPeerChange{
			Kind:    change.Kind,
			StoreID: change.StoreID,
			PeerID:  change.PeerID,
			Base:    change.Base.Clone(),
			Target:  change.Target.Clone(),
		}
	}
	return out
}

func ClonePendingRangeChanges(in map[uint64]PendingRangeChange) map[uint64]PendingRangeChange {
	if len(in) == 0 {
		return make(map[uint64]PendingRangeChange)
	}
	out := make(map[uint64]PendingRangeChange, len(in))
	for id, change := range in {
		out[id] = PendingRangeChange{
			Kind:           change.Kind,
			ParentRegionID: change.ParentRegionID,
			LeftRegionID:   change.LeftRegionID,
			RightRegionID:  change.RightRegionID,
			BaseParent:     change.BaseParent.Clone(),
			BaseLeft:       change.BaseLeft.Clone(),
			BaseRight:      change.BaseRight.Clone(),
			Left:           change.Left.Clone(),
			Right:          change.Right.Clone(),
			Merged:         change.Merged.Clone(),
		}
	}
	return out
}

// ApplyEventToState applies one rooted metadata event into compact root state.
func ApplyEventToState(state *State, cursor Cursor, event rootevent.Event) {
	if state == nil {
		return
	}
	switch event.Kind {
	case rootevent.KindStoreJoined, rootevent.KindStoreRetired:
		if event.StoreMembership != nil && event.StoreMembership.StoreID != 0 {
			state.MembershipEpoch++
		}
	case rootevent.KindIDAllocatorFenced:
		if event.AllocatorFence != nil && event.AllocatorFence.Minimum > state.IDFence {
			state.IDFence = event.AllocatorFence.Minimum
		}
	case rootevent.KindTSOAllocatorFenced:
		if event.AllocatorFence != nil && event.AllocatorFence.Minimum > state.TSOFence {
			state.TSOFence = event.AllocatorFence.Minimum
		}
	case rootevent.KindSnapshotEpochPublished,
		rootevent.KindSnapshotEpochRetired,
		rootevent.KindMountRetired,
		rootevent.KindSubtreeAuthorityDeclared,
		rootevent.KindSubtreeHandoffStarted,
		rootevent.KindSubtreeHandoffCompleted,
		rootevent.KindQuotaFenceUpdated:
		// Filesystem namespace authority events advance the root cursor but do
		// not mutate cluster topology or store membership epochs.
	case rootevent.KindMountRegistered:
		// mount_key_id is allocated from the same global ID space as region and
		// peer IDs. Static bootstrap events therefore act as allocator fences.
		if event.Mount != nil && event.Mount.MountKeyID > state.IDFence {
			state.IDFence = event.Mount.MountKeyID
		}
	case rootevent.KindGrantIssued:
		applyGrantIssuedToState(state, cursor, event)
	case rootevent.KindGrantSealed, rootevent.KindGrantRetired:
		applyGrantRetirementToState(state, cursor, event)
	case rootevent.KindGrantInherited:
		applyGrantInheritanceToState(state, cursor, event)
	case rootevent.KindVisibleAuthorityGranted:
		applyVisibleAuthorityGrantedToState(state, cursor, event)
	case rootevent.KindVisibleAuthoritySealed:
		applyVisibleAuthoritySealedToState(state, event)
	case rootevent.KindVisibleAuthorityRetired:
		applyVisibleAuthorityRetiredToState(state, event)
	case rootevent.KindRegionBootstrap,
		rootevent.KindRegionDescriptorPublished,
		rootevent.KindRegionTombstoned,
		rootevent.KindRegionSplitPlanned,
		rootevent.KindRegionSplitCommitted,
		rootevent.KindRegionSplitCancelled,
		rootevent.KindRegionMergePlanned,
		rootevent.KindRegionMerged,
		rootevent.KindRegionMergeCancelled,
		rootevent.KindPeerAdditionPlanned,
		rootevent.KindPeerRemovalPlanned,
		rootevent.KindPeerAdded,
		rootevent.KindPeerRemoved,
		rootevent.KindPeerAdditionCancelled,
		rootevent.KindPeerRemovalCancelled:
		state.ClusterEpoch++
	}
	state.LastCommitted = cursor
}

func applyGrantIssuedToState(state *State, cursor Cursor, event rootevent.Event) {
	if state == nil || event.Grant == nil {
		return
	}
	grant := cloneAuthorityGrant(*event.Grant)
	if grant.IssuedAt.Term == 0 && grant.IssuedAt.Index == 0 {
		grant.IssuedAt = cursor
	}
	if grant.IssuedRootToken.Term == 0 && grant.IssuedRootToken.Index == 0 && grant.IssuedRootToken.Revision == 0 {
		grant.IssuedRootToken = rootproto.AuthorityRootToken{Term: cursor.Term, Index: cursor.Index}
	}
	replaced := false
	for i := range state.ActiveGrants {
		if state.ActiveGrants[i].GrantID == grant.GrantID {
			state.ActiveGrants[i] = grant
			replaced = true
			break
		}
	}
	if !replaced {
		state.ActiveGrants = append(state.ActiveGrants, grant)
	}
	for _, duty := range grant.Duties {
		if duty.Bound.Kind != rootproto.DutyBoundMonotone {
			continue
		}
		switch duty.DutyID {
		case rootproto.DutyAllocID:
			if duty.Bound.MonotoneUpper > state.IDFence {
				state.IDFence = duty.Bound.MonotoneUpper
			}
		case rootproto.DutyTSO:
			if duty.Bound.MonotoneUpper > state.TSOFence {
				state.TSOFence = duty.Bound.MonotoneUpper
			}
		}
	}
}

func applyGrantRetirementToState(state *State, cursor Cursor, event rootevent.Event) {
	if state == nil || event.GrantRetirement == nil {
		return
	}
	retirement := *event.GrantRetirement
	retirement.Bounds = append([]rootproto.DutyGrant(nil), event.GrantRetirement.Bounds...)
	if retirement.RetiredAt.Term == 0 && retirement.RetiredAt.Index == 0 {
		retirement.RetiredAt = cursor
	}
	if retirement.Mode == rootproto.GrantRetirementUnspecified && event.Kind == rootevent.KindGrantSealed {
		retirement.Mode = rootproto.GrantRetirementSealedExact
	}
	if retirement.Mode == rootproto.GrantRetirementUnspecified && event.Kind == rootevent.KindGrantRetired {
		retirement.Mode = rootproto.GrantRetirementExpiredBound
	}
	replaced := false
	for i := range state.RetiredGrants {
		if state.RetiredGrants[i].GrantID == retirement.GrantID {
			state.RetiredGrants[i] = retirement
			replaced = true
			break
		}
	}
	if !replaced {
		state.RetiredGrants = append(state.RetiredGrants, retirement)
	}
	for i := 0; i < len(state.ActiveGrants); i++ {
		if state.ActiveGrants[i].GrantID == retirement.GrantID {
			state.ActiveGrants = append(state.ActiveGrants[:i], state.ActiveGrants[i+1:]...)
			i--
		}
	}
}

func applyGrantInheritanceToState(state *State, cursor Cursor, event rootevent.Event) {
	if state == nil || event.GrantInheritance == nil {
		return
	}
	inheritance := *event.GrantInheritance
	if inheritance.InheritedAt.Term == 0 && inheritance.InheritedAt.Index == 0 {
		inheritance.InheritedAt = cursor
	}
	state.GrantInheritances = append(state.GrantInheritances, inheritance)
	for i := range state.RetiredGrants {
		if state.RetiredGrants[i].GrantID == inheritance.PredecessorGrantID {
			state.RetiredGrants[i].InheritedByGrantID = inheritance.SuccessorGrantID
			state.RetiredEraFloors = rootproto.AdvanceAuthorityRetiredEraFloorsForBounds(
				state.RetiredEraFloors,
				state.RetiredGrants[i].Bounds,
				state.RetiredGrants[i].Era,
			)
		}
	}
}

// CompactEunomiaState removes inherited retirement history that is already
// represented by compact finality floors. It only drops a retirement after every
// duty/scope in that retirement has reached the same era, preserving the evidence
// needed by unrelated duties until their own floors advance.
func CompactEunomiaState(state State) State {
	if len(state.RetiredEraFloors) == 0 {
		return state
	}
	originalRetirements := append([]rootproto.GrantRetirement(nil), state.RetiredGrants...)
	activePredecessors := make(map[string]struct{})
	for _, grant := range state.ActiveGrants {
		for _, retirement := range grant.PredecessorRetirements {
			if retirement.GrantID != "" {
				activePredecessors[retirement.GrantID] = struct{}{}
			}
		}
	}
	retirements := make([]rootproto.GrantRetirement, 0, len(originalRetirements))
	for _, retirement := range originalRetirements {
		if retirement.InheritedByGrantID != "" && retirementCoveredByRetiredEraFloor(retirement, state.RetiredEraFloors) {
			if _, active := activePredecessors[retirement.GrantID]; !active {
				continue
			}
		}
		retirements = append(retirements, retirement)
	}
	inheritances := make([]rootproto.GrantInheritance, 0, len(state.GrantInheritances))
	for _, inheritance := range state.GrantInheritances {
		keep := true
		for _, retirement := range originalRetirements {
			if retirement.GrantID == inheritance.PredecessorGrantID &&
				retirement.InheritedByGrantID != "" &&
				retirementCoveredByRetiredEraFloor(retirement, state.RetiredEraFloors) {
				if _, active := activePredecessors[retirement.GrantID]; !active {
					keep = false
				}
				break
			}
		}
		if keep {
			inheritances = append(inheritances, inheritance)
		}
	}
	state.RetiredGrants = retirements
	state.GrantInheritances = inheritances
	return state
}

// retirementCoveredByRetiredEraFloor reports whether compact scoped floors fully
// cover one inherited retirement.
func retirementCoveredByRetiredEraFloor(retirement rootproto.GrantRetirement, floors []rootproto.AuthorityRetiredEraFloor) bool {
	if retirement.Era == 0 {
		return false
	}
	if len(retirement.Bounds) == 0 {
		return false
	}
	for _, bound := range retirement.Bounds {
		if rootproto.AuthorityRetiredEraFloorFor(floors, bound.DutyID, bound.Scope) < retirement.Era {
			return false
		}
	}
	return true
}

func cloneAuthorityGrant(grant rootproto.AuthorityGrant) rootproto.AuthorityGrant {
	grant.Duties = append([]rootproto.DutyGrant(nil), grant.Duties...)
	grant.PredecessorRetirements = append([]rootproto.GrantRetirement(nil), grant.PredecessorRetirements...)
	return grant
}

func cloneAuthorityGrants(grants []rootproto.AuthorityGrant) []rootproto.AuthorityGrant {
	if len(grants) == 0 {
		return nil
	}
	out := make([]rootproto.AuthorityGrant, len(grants))
	for i, grant := range grants {
		out[i] = cloneAuthorityGrant(grant)
	}
	return out
}

// NextCursor returns the next ordered root cursor.
func NextCursor(prev Cursor) Cursor {
	term := prev.Term
	if term == 0 {
		term = 1
	}
	return Cursor{Term: term, Index: prev.Index + 1}
}

// CursorAfter reports whether a is ordered strictly after b.
func CursorAfter(a, b Cursor) bool {
	if a.Term != b.Term {
		return a.Term > b.Term
	}
	return a.Index > b.Index
}
