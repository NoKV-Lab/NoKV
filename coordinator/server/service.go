// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package server implements the Coordinator gRPC service — the control-plane
// entry point for route lookup, TSO, ID allocation, grant management, and
// rooted topology mutations.
//
// This package owns the SERVICE layer. It consumes rooted truth from
// meta/root/ but never owns durable cluster state. The execution plane
// (raftstore/) applies and publishes, and coordinator reconstructs its
// view by tailing rooted commits.
//
// Heavy logic is deliberately split into sibling packages:
// catalog (region/event validation), view (directory + store health),
// storage (rooted adapter), audit (snapshot + trace audit), and the root
// authority protocol consumed from meta/root/protocol.
package server

import (
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/coordinator/catalog"
	"github.com/feichai0017/NoKV/coordinator/idalloc"
	"github.com/feichai0017/NoKV/coordinator/rootview"
	"github.com/feichai0017/NoKV/coordinator/scheduling"
	"github.com/feichai0017/NoKV/coordinator/tso"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
)

// Service implements the Coordinator gRPC API.
//
// Lock order:
//  1. writeMu
//  2. allocMu
//  3. grantMu
//
// Never acquire these locks in reverse order.
type Service struct {
	coordpb.UnimplementedCoordinatorServer

	cluster         *catalog.Cluster
	ids             *idalloc.IDAllocator
	tso             *tso.Allocator
	storage         rootview.RootStorage
	idWindowHigh    uint64
	idWindowSize    uint64
	tsoWindowHigh   uint64
	tsoWindowSize   uint64
	allocMu         sync.Mutex
	writeMu         sync.Mutex
	grantMu         sync.RWMutex
	coordinatorID   string
	grantTTL        time.Duration
	grantRenewIn    time.Duration
	grantClockSkew  time.Duration
	grantCandidates []string
	grantDuties     []rootproto.DutyID
	now             func() time.Time
	grantView       coordinatorGrantView
	authorityMu     sync.Mutex
	authorityDuties map[rootproto.DutyID]authorityDutyServing
	rootViewMu      sync.RWMutex
	rootView        coordinatorRootSnapshotView
	rootViewTTL     time.Duration
	// storeHeartbeatTTL holds the time.Duration value as an int64 so callers
	// (storeState reads, ConfigureStoreHeartbeatTTL writes) avoid a data race
	// without taking a lock on the read path.
	storeHeartbeatTTL atomic.Int64
	statusMu          sync.RWMutex
	lastRootReload    int64
	lastRootError     string
	eunomiaMetrics    eunomiaMetrics
	scheduler         *scheduling.Planner
}

type authorityServingState uint8

const (
	authorityServing authorityServingState = iota
	authorityDraining
	authoritySealed
)

func (s authorityServingState) String() string {
	switch s {
	case authorityServing:
		return "serving"
	case authorityDraining:
		return "draining"
	case authoritySealed:
		return "sealed"
	default:
		return "unknown"
	}
}

type authorityDutyServing struct {
	state    authorityServingState
	inflight uint64
}

type coordinatorGrantView struct {
	grants           []rootproto.AuthorityGrant
	certificates     map[string]rootproto.GrantCertificate
	retirements      []rootproto.GrantRetirement
	inheritances     []rootproto.GrantInheritance
	retiredEraFloors []rootproto.AuthorityRetiredEraFloor
}

type coordinatorRootSnapshotView struct {
	snapshot    rootview.Snapshot
	loaded      bool
	refreshing  bool
	refreshedAt time.Time
}

func (v *coordinatorGrantView) Reset() {
	if v == nil {
		return
	}
	v.grants = nil
	v.certificates = nil
	v.retirements = nil
	v.inheritances = nil
	v.retiredEraFloors = nil
}

func (v *coordinatorGrantView) Refresh(snapshot rootview.Snapshot) {
	if v == nil {
		return
	}
	certs := v.certificates
	nextCerts := make(map[string]rootproto.GrantCertificate)
	v.grants = make([]rootproto.AuthorityGrant, 0, len(snapshot.ActiveGrants))
	for _, grant := range snapshot.ActiveGrants {
		if cert, ok := certs[grant.GrantID]; ok && grantCertificateCoversGrant(cert, grant) {
			v.grants = append(v.grants, cert.Grant)
			nextCerts[grant.GrantID] = cert
			continue
		}
		v.grants = append(v.grants, cloneGrantForView(grant))
	}
	v.certificates = nextCerts
	v.retirements = append([]rootproto.GrantRetirement(nil), snapshot.RetiredGrants...)
	v.inheritances = append([]rootproto.GrantInheritance(nil), snapshot.GrantInheritances...)
	v.retiredEraFloors = rootproto.CloneAuthorityRetiredEraFloors(snapshot.RetiredEraFloors)
}

func (v coordinatorGrantView) Grants() []rootproto.AuthorityGrant {
	out := make([]rootproto.AuthorityGrant, len(v.grants))
	for i, grant := range v.grants {
		out[i] = cloneGrantForView(grant)
	}
	return out
}

func (v coordinatorGrantView) GrantFor(duty rootproto.DutyID, scope rootproto.DutyScope) (rootproto.AuthorityGrant, bool) {
	for _, grant := range v.grants {
		if grant.CoversDutyKey(rootproto.DutyKey{DutyID: duty, Scope: scope}) {
			return cloneGrantForView(grant), true
		}
	}
	return rootproto.AuthorityGrant{}, false
}

func (v coordinatorGrantView) GrantByID(grantID string) (rootproto.AuthorityGrant, bool) {
	for _, grant := range v.grants {
		if grant.GrantID == grantID {
			return cloneGrantForView(grant), true
		}
	}
	return rootproto.AuthorityGrant{}, false
}

func (v coordinatorGrantView) CertificateFor(grant rootproto.AuthorityGrant) rootproto.GrantCertificate {
	if v.certificates == nil {
		return rootproto.GrantCertificate{}
	}
	return v.certificates[grant.GrantID]
}

func (v coordinatorGrantView) Retirements() []rootproto.GrantRetirement {
	return append([]rootproto.GrantRetirement(nil), v.retirements...)
}

// RetiredEraFloorFor reads the finality floor for one served duty.
func (v coordinatorGrantView) RetiredEraFloorFor(duty rootproto.DutyID, scope rootproto.DutyScope) uint64 {
	return rootproto.AuthorityRetiredEraFloorFor(v.retiredEraFloors, duty, scope)
}

func cloneGrantForView(grant rootproto.AuthorityGrant) rootproto.AuthorityGrant {
	grant.Duties = append([]rootproto.DutyGrant(nil), grant.Duties...)
	grant.PredecessorRetirements = append([]rootproto.GrantRetirement(nil), grant.PredecessorRetirements...)
	return grant
}

const defaultAllocatorWindowSize uint64 = 10_000
const defaultGrantTTL = 10 * time.Second
const defaultGrantRenewIn = 3 * time.Second
const defaultGrantClockSkew = 500 * time.Millisecond
const defaultGrantRetryMin = 200 * time.Millisecond
const maxGrantRetry = 60 * time.Second
const defaultGrantReleaseTimeout = defaultGrantTTL
const defaultRootSnapshotRefreshInterval = 250 * time.Millisecond
const defaultStoreHeartbeatTTL = 10 * time.Second

// NewService constructs a Coordinator service. The optional root storage fixes
// durable rooted persistence at construction time; omitting it keeps the service
// in explicit in-memory mode.
func NewService(cluster *catalog.Cluster, ids *idalloc.IDAllocator, tsAlloc *tso.Allocator, root ...rootview.RootStorage) *Service {
	if cluster == nil {
		cluster = catalog.NewCluster()
	}
	if ids == nil {
		ids = idalloc.NewIDAllocator(1)
	}
	if tsAlloc == nil {
		tsAlloc = tso.NewAllocator(1)
	}
	var storage rootview.RootStorage
	if len(root) > 0 {
		storage = root[0]
	}
	svc := &Service{
		cluster:       cluster,
		ids:           ids,
		tso:           tsAlloc,
		storage:       storage,
		idWindowSize:  defaultAllocatorWindowSize,
		tsoWindowSize: defaultAllocatorWindowSize,
		now:           time.Now,
		scheduler:     scheduling.NewPlanner(scheduling.PlanOptions{}),
	}
	svc.storeHeartbeatTTL.Store(int64(defaultStoreHeartbeatTTL))
	return svc
}

// ConfigureAuthorityGrant enables the explicit coordinator owner grant gate.
// Empty holderID disables the gate and keeps the current in-memory-only behavior.
func (s *Service) ConfigureAuthorityGrant(holderID string, ttl, renewIn time.Duration) {
	s.ConfigureAuthorityGrantDuties(holderID, nil, nil, ttl, renewIn)
}

func (s *Service) ConfigureAuthorityGrantDuties(holderID string, candidates []string, duties []rootproto.DutyID, ttl, renewIn time.Duration) {
	if s == nil {
		return
	}
	s.grantMu.Lock()
	defer s.grantMu.Unlock()
	s.coordinatorID = holderID
	s.resetAuthorityServing()
	if holderID == "" {
		s.grantTTL = 0
		s.grantRenewIn = 0
		s.grantClockSkew = 0
		s.grantCandidates = nil
		s.grantDuties = nil
		s.grantView.Reset()
		return
	}
	if ttl <= 0 {
		ttl = defaultGrantTTL
	}
	if renewIn <= 0 || renewIn >= ttl {
		renewIn = defaultGrantRenewIn
		if renewIn >= ttl {
			renewIn = ttl / 2
		}
	}
	clockSkew := defaultGrantClockSkew
	if clockSkew >= renewIn && renewIn > 0 {
		clockSkew = renewIn / 2
	}
	if clockSkew <= 0 {
		clockSkew = time.Millisecond
	}
	s.grantTTL = ttl
	s.grantRenewIn = renewIn
	s.grantClockSkew = clockSkew
	s.grantCandidates = normalizeGrantCandidates(holderID, candidates)
	s.grantDuties = normalizeGrantDuties(duties)
}

func normalizeGrantCandidates(holderID string, candidates []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(candidates)+1)
	add := func(candidate string) {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			return
		}
		if _, ok := seen[candidate]; ok {
			return
		}
		seen[candidate] = struct{}{}
		out = append(out, candidate)
	}
	for _, candidate := range candidates {
		add(candidate)
	}
	add(holderID)
	sort.Strings(out)
	return out
}

func normalizeGrantDuties(duties []rootproto.DutyID) []rootproto.DutyID {
	if len(duties) == 0 {
		return nil
	}
	seen := map[rootproto.DutyID]struct{}{}
	out := make([]rootproto.DutyID, 0, len(duties))
	for _, duty := range duties {
		if duty == "" {
			continue
		}
		if _, ok := seen[duty]; ok {
			continue
		}
		seen[duty] = struct{}{}
		out = append(out, duty)
	}
	return out
}

// ConfigureAllocatorWindows overrides the rooted allocator refill window sizes.
// Zero values keep the default window behavior.
func (s *Service) ConfigureAllocatorWindows(idWindowSize, tsoWindowSize uint64) {
	if s == nil {
		return
	}
	if idWindowSize != 0 {
		s.idWindowSize = idWindowSize
	}
	if tsoWindowSize != 0 {
		s.tsoWindowSize = tsoWindowSize
	}
}

// ConfigureStoreHeartbeatTTL controls when the runtime store registry marks a
// store as down after its last heartbeat. Non-positive values keep the default.
// Safe to call concurrently with RPC handlers that read storeHeartbeatTTL via
// atomic load (see storeState in service_gateway.go).
func (s *Service) ConfigureStoreHeartbeatTTL(ttl time.Duration) {
	if s == nil {
		return
	}
	if ttl <= 0 {
		ttl = defaultStoreHeartbeatTTL
	}
	s.storeHeartbeatTTL.Store(int64(ttl))
}

// ConfigureRootSnapshotRefresh controls how long GetRegionByKey keeps one
// cached rooted snapshot before refreshing it asynchronously.
func (s *Service) ConfigureRootSnapshotRefresh(interval time.Duration) {
	if s == nil {
		return
	}
	s.rootViewTTL = interval
}

// RefreshFromStorage refreshes rooted durable state into the in-memory service
// view and fences allocator state so a future leader cannot allocate stale ids.
func (s *Service) RefreshFromStorage() error {
	if s == nil || s.storage == nil {
		return nil
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.reloadAndFenceAllocators(true)
}

// ReloadFromStorage reloads the in-memory rooted view from the storage cache
// without forcing the underlying rooted backend to refresh first.
func (s *Service) ReloadFromStorage() error {
	if s == nil || s.storage == nil {
		return nil
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.reloadAndFenceAllocators(false)
}
