// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package peras adapts root-issued Peras authority grants to fsmeta runtime scopes.
package peras

import (
	"slices"
	"sync"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/model"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
)

type ActiveAuthorities struct {
	mu     sync.RWMutex
	grants map[string]rootproto.VisibleAuthorityGrant
	ready  bool
}

func NewActiveAuthorities() *ActiveAuthorities {
	return &ActiveAuthorities{grants: make(map[string]rootproto.VisibleAuthorityGrant)}
}

// Replace installs one root-observed active grant snapshot. Invalid entries are
// rejected instead of being partially installed, so callers never mix two root
// views in one table.
func (a *ActiveAuthorities) Replace(grants []rootproto.VisibleAuthorityGrant) error {
	next := make(map[string]rootproto.VisibleAuthorityGrant, len(grants))
	ordered := make([]rootproto.VisibleAuthorityGrant, 0, len(grants))
	for _, grant := range grants {
		if !grant.Valid() {
			return ErrInvalidGrant
		}
		if _, ok := next[grant.GrantID]; ok {
			return ErrInvalidGrant
		}
		if slices.ContainsFunc(ordered, grant.Overlaps) {
			return ErrConflictingGrant
		}
		next[grant.GrantID] = cloneGrant(grant)
		ordered = append(ordered, grant)
	}
	a.mu.Lock()
	a.grants = next
	a.ready = true
	a.mu.Unlock()
	return nil
}

func (a *ActiveAuthorities) Snapshot() []rootproto.VisibleAuthorityGrant {
	if a == nil {
		return nil
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	out := make([]rootproto.VisibleAuthorityGrant, 0, len(a.grants))
	for _, grant := range a.grants {
		out = append(out, cloneGrant(grant))
	}
	slices.SortFunc(out, func(left, right rootproto.VisibleAuthorityGrant) int {
		if left.GrantID < right.GrantID {
			return -1
		}
		if left.GrantID > right.GrantID {
			return 1
		}
		return 0
	})
	return out
}

func (a *ActiveAuthorities) ApplyRootEvent(event rootevent.Event) error {
	if a == nil || event.VisibleGrant == nil {
		return nil
	}
	switch event.Kind {
	case rootevent.KindVisibleAuthorityGranted:
		return a.applyGranted(*event.VisibleGrant)
	case rootevent.KindVisibleAuthorityRetired:
		next := a.Snapshot()
		for i := 0; i < len(next); i++ {
			if next[i].GrantID == event.VisibleGrant.GrantID {
				next = append(next[:i], next[i+1:]...)
				i--
			}
		}
		return a.Replace(next)
	default:
		return nil
	}
}

func (a *ActiveAuthorities) applyGranted(grant rootproto.VisibleAuthorityGrant) error {
	if !grant.Valid() {
		return ErrInvalidGrant
	}
	next := a.Snapshot()
	for i := 0; i < len(next); i++ {
		current := next[i]
		if current.GrantID == grant.GrantID {
			next[i] = cloneGrant(grant)
			return a.Replace(next)
		}
		if !current.Overlaps(grant) {
			continue
		}
		if current.EpochID > grant.EpochID {
			return nil
		}
		if current.EpochID == grant.EpochID {
			return ErrConflictingGrant
		}
		next = append(next[:i], next[i+1:]...)
		i--
	}
	next = append(next, cloneGrant(grant))
	return a.Replace(next)
}

func (a *ActiveAuthorities) Find(scope compile.AuthorityScope, now time.Time) (rootproto.VisibleAuthorityGrant, bool, error) {
	if a == nil {
		return rootproto.VisibleAuthorityGrant{}, false, nil
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	var found rootproto.VisibleAuthorityGrant
	for _, grant := range a.grants {
		if !GrantCoversDelta(grant, scope, now) {
			continue
		}
		if found.Valid() {
			return rootproto.VisibleAuthorityGrant{}, false, ErrAmbiguousAuthority
		}
		found = grant
	}
	if !found.Valid() {
		return rootproto.VisibleAuthorityGrant{}, false, nil
	}
	return cloneGrant(found), true, nil
}

// FencesKey reports whether a concrete fsmeta key is currently covered by one
// active Peras authority. Non-fsmeta keys are ignored so generic KV traffic is
// not accidentally fenced.
func (a *ActiveAuthorities) FencesKey(key []byte, now time.Time) (rootproto.VisibleAuthorityGrant, bool, error) {
	if a == nil {
		return rootproto.VisibleAuthorityGrant{}, false, nil
	}
	parts, ok := fsmeta.InspectKey(key)
	if !ok {
		return rootproto.VisibleAuthorityGrant{}, false, nil
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	if !a.ready {
		return rootproto.VisibleAuthorityGrant{}, false, ErrAuthorityViewStale
	}
	var found rootproto.VisibleAuthorityGrant
	for _, grant := range a.grants {
		if !grantCoversKey(grant, parts, now) {
			continue
		}
		if found.Valid() {
			return rootproto.VisibleAuthorityGrant{}, false, ErrAmbiguousAuthority
		}
		found = grant
	}
	if !found.Valid() {
		return rootproto.VisibleAuthorityGrant{}, false, nil
	}
	return cloneGrant(found), true, nil
}

func GrantCoversDelta(grant rootproto.VisibleAuthorityGrant, scope compile.AuthorityScope, now time.Time) bool {
	if !grant.Valid() || !grant.ActiveAt(now.UnixNano()) {
		return false
	}
	if scope.MountKeyID == 0 || grant.Scope.MountKeyID != uint64(scope.MountKeyID) {
		return false
	}
	if !grantCoversBuckets(grant.Scope.Buckets, scope.Buckets, false) {
		return false
	}
	if !grantCoversInodes(grant.Scope.Parents, scope.Parents, true) {
		return false
	}
	return grantCoversInodes(grant.Scope.Inodes, scope.Inodes, true)
}

func grantCoversBuckets(grant []uint16, requested []fsmeta.AffinityBucket, emptyRequestCovered bool) bool {
	if len(grant) == 0 {
		return true
	}
	if len(requested) == 0 {
		return emptyRequestCovered
	}
	for _, value := range requested {
		if !slices.Contains(grant, uint16(value)) {
			return false
		}
	}
	return true
}

func grantCoversKey(grant rootproto.VisibleAuthorityGrant, parts fsmeta.KeyParts, now time.Time) bool {
	if !grant.Valid() || !grant.ActiveAt(now.UnixNano()) {
		return false
	}
	if grant.Scope.MountKeyID != uint64(parts.MountKeyID) {
		return false
	}
	if !grantCoversBuckets(grant.Scope.Buckets, []fsmeta.AffinityBucket{parts.Bucket}, false) {
		return false
	}
	switch parts.Kind {
	case fsmeta.KeyKindMount:
		return true
	case fsmeta.KeyKindDentry:
		return grantCoversInodes(grant.Scope.Parents, []model.InodeID{parts.Parent}, false)
	case fsmeta.KeyKindInode, fsmeta.KeyKindChunk, fsmeta.KeyKindSession:
		return grantCoversInodes(grant.Scope.Inodes, []model.InodeID{parts.Inode}, false)
	case fsmeta.KeyKindUsage:
		if len(grant.Scope.Parents) == 0 && len(grant.Scope.Inodes) == 0 {
			return true
		}
		scope := uint64(parts.UsageScope)
		return slices.Contains(grant.Scope.Parents, scope) ||
			slices.Contains(grant.Scope.Inodes, scope)
	default:
		return false
	}
}

func grantCoversInodes(grant []uint64, requested []model.InodeID, emptyRequestCovered bool) bool {
	if len(grant) == 0 {
		return true
	}
	if len(requested) == 0 {
		return emptyRequestCovered
	}
	for _, value := range requested {
		if !slices.Contains(grant, uint64(value)) {
			return false
		}
	}
	return true
}

func (a *ActiveAuthorities) HolderFor(scope compile.AuthorityScope, now time.Time) (string, bool, error) {
	grant, ok, err := a.Find(scope, now)
	if err != nil || !ok {
		return "", ok, err
	}
	return grant.HolderID, true, nil
}

func (a *ActiveAuthorities) HeldBy(holderID string, scope compile.AuthorityScope, now time.Time) (bool, error) {
	if holderID == "" {
		return false, nil
	}
	grant, ok, err := a.Find(scope, now)
	if err != nil || !ok {
		return false, err
	}
	return grant.HolderID == holderID, nil
}

func cloneGrant(grant rootproto.VisibleAuthorityGrant) rootproto.VisibleAuthorityGrant {
	return rootproto.CloneVisibleAuthorityGrant(grant)
}

func AuthorityScopeFromDelta(scope compile.AuthorityScope) rootproto.VisibleAuthorityScope {
	return rootproto.VisibleAuthorityScope{
		MountID:    string(scope.Mount),
		MountKeyID: uint64(scope.MountKeyID),
		Buckets:    perasBucketsFromDelta(scope.Buckets),
		Parents:    perasInodesFromDelta(scope.Parents),
		Inodes:     perasInodesFromDelta(scope.Inodes),
	}
}

func ScopeFromGrant(grant rootproto.VisibleAuthorityGrant) compile.AuthorityScope {
	return scopeFromRootAuthority(grant.Scope)
}

func ScopeFromSeal(seal rootproto.VisibleAuthoritySeal) compile.AuthorityScope {
	return scopeFromRootAuthority(seal.Scope)
}

func scopeFromRootAuthority(rootScope rootproto.VisibleAuthorityScope) compile.AuthorityScope {
	scope := compile.AuthorityScope{
		Mount:      model.MountID(rootScope.MountID),
		MountKeyID: model.MountKeyID(rootScope.MountKeyID),
		Parents:    fsmetaInodesFromRoot(rootScope.Parents),
		Inodes:     fsmetaInodesFromRoot(rootScope.Inodes),
	}
	if len(rootScope.Buckets) > 0 {
		scope.Buckets = make([]fsmeta.AffinityBucket, len(rootScope.Buckets))
		for i, bucket := range rootScope.Buckets {
			scope.Buckets[i] = fsmeta.AffinityBucket(bucket)
		}
	}
	return scope
}

func ScopeEmpty(scope compile.AuthorityScope) bool {
	return scope.Mount == "" || scope.MountKeyID == 0
}

func perasBucketsFromDelta(buckets []fsmeta.AffinityBucket) []uint16 {
	out := make([]uint16, len(buckets))
	for i, bucket := range buckets {
		out[i] = uint16(bucket)
	}
	return out
}

func perasInodesFromDelta(inodes []model.InodeID) []uint64 {
	out := make([]uint64, len(inodes))
	for i, inode := range inodes {
		out[i] = uint64(inode)
	}
	return out
}

func fsmetaInodesFromRoot(inodes []uint64) []model.InodeID {
	out := make([]model.InodeID, len(inodes))
	for i, inode := range inodes {
		out[i] = model.InodeID(inode)
	}
	return out
}
