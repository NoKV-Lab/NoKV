// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"slices"

	fsperas "github.com/feichai0017/NoKV/experimental/peras/exec"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
)

func GrantHasPredecessor(grant rootproto.VisibleAuthorityGrant) bool {
	var zero [32]byte
	return grant.EpochID > 1 && grant.PredecessorDigest != zero
}

func SegmentWithinScope(segment fsperas.PerasSegment, scope compile.AuthorityScope) bool {
	if ScopeEmpty(scope) {
		return true
	}
	checked := false
	for _, entry := range segment.EntriesView() {
		parts, ok := layout.InspectKey(entry.Key)
		if !ok {
			if checked {
				return false
			}
			continue
		}
		checked = true
		if !scopeCoversKeyParts(scope, parts) {
			return false
		}
	}
	return true
}

func CatalogBuckets(scope compile.AuthorityScope) []layout.AffinityBucket {
	if scope.MountKeyID == 0 {
		return nil
	}
	if len(scope.Buckets) > 0 {
		buckets := append([]layout.AffinityBucket(nil), scope.Buckets...)
		slices.Sort(buckets)
		return slices.Compact(buckets)
	}
	buckets := make([]layout.AffinityBucket, layout.DefaultAffinityBucketCount)
	for idx := range buckets {
		buckets[idx] = layout.AffinityBucket(idx)
	}
	return buckets
}

func NormalizeScopes(scopes []compile.AuthorityScope) []compile.AuthorityScope {
	if len(scopes) == 0 {
		return []compile.AuthorityScope{{}}
	}
	out := make([]compile.AuthorityScope, 0, len(scopes))
	for _, scope := range scopes {
		if ScopeEmpty(scope) {
			return []compile.AuthorityScope{{}}
		}
		out = append(out, CloneScope(scope))
	}
	return out
}

func ScopesOverlap(left, right compile.AuthorityScope) bool {
	if ScopeEmpty(left) || ScopeEmpty(right) {
		return true
	}
	return fsperas.AuthorityScopesOverlap(left, right)
}

func ScopesEqual(left, right compile.AuthorityScope) bool {
	return left.Mount == right.Mount &&
		left.MountKeyID == right.MountKeyID &&
		slices.Equal(left.Buckets, right.Buckets) &&
		slices.Equal(left.Parents, right.Parents) &&
		slices.Equal(left.Inodes, right.Inodes)
}

func CloneScope(scope compile.AuthorityScope) compile.AuthorityScope {
	scope.Buckets = append([]layout.AffinityBucket(nil), scope.Buckets...)
	scope.Parents = append([]model.InodeID(nil), scope.Parents...)
	scope.Inodes = append([]model.InodeID(nil), scope.Inodes...)
	return scope
}

func CloneScopes(scopes []compile.AuthorityScope) []compile.AuthorityScope {
	out := make([]compile.AuthorityScope, len(scopes))
	for idx, scope := range scopes {
		out[idx] = CloneScope(scope)
	}
	return out
}

func scopeCoversKeyParts(scope compile.AuthorityScope, parts layout.KeyParts) bool {
	if scope.MountKeyID == 0 || parts.MountKeyID != scope.MountKeyID {
		return false
	}
	if len(scope.Buckets) > 0 && !slices.Contains(scope.Buckets, parts.Bucket) {
		return false
	}
	switch parts.Kind {
	case layout.KeyKindDentry:
		return len(scope.Parents) == 0 || slices.Contains(scope.Parents, parts.Parent)
	case layout.KeyKindInode, layout.KeyKindChunk, layout.KeyKindSession:
		return len(scope.Inodes) == 0 || slices.Contains(scope.Inodes, parts.Inode)
	case layout.KeyKindUsage:
		return len(scope.Parents) == 0 || slices.Contains(scope.Parents, parts.UsageScope)
	default:
		return true
	}
}
