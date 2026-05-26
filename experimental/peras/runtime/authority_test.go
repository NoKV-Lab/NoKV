// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"errors"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/model"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	"github.com/stretchr/testify/require"
)

var (
	testNow   = time.Unix(100, 0)
	testMount = model.MountIdentity{MountID: "vol", MountKeyID: 7}
)

func TestActiveAuthoritiesFindsCoveringGrant(t *testing.T) {
	table := NewActiveAuthorities()
	request := compile.AuthorityScope{
		Mount:      testMount.MountID,
		MountKeyID: testMount.MountKeyID,
		Buckets:    []fsmeta.AffinityBucket{2},
		Parents:    []model.InodeID{10},
	}
	grant := testGrant("g1", "holder-a", compile.AuthorityScope{
		Mount:      testMount.MountID,
		MountKeyID: testMount.MountKeyID,
		Buckets:    []fsmeta.AffinityBucket{1, 2},
		Parents:    []model.InodeID{10},
	})
	require.NoError(t, table.Replace([]rootproto.VisibleAuthorityGrant{grant}))

	found, ok, err := table.Find(request, testNow)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "holder-a", found.HolderID)

	holder, ok, err := table.HolderFor(request, testNow)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "holder-a", holder)

	held, err := table.HeldBy("holder-a", request, testNow)
	require.NoError(t, err)
	require.True(t, held)
	held, err = table.HeldBy("holder-b", request, testNow)
	require.NoError(t, err)
	require.False(t, held)
}

func TestActiveAuthoritiesRejectsExpiredAndWrongMount(t *testing.T) {
	table := NewActiveAuthorities()
	require.NoError(t, table.Replace([]rootproto.VisibleAuthorityGrant{
		testGrant("g1", "holder-a", compile.AuthorityScope{
			Mount:      testMount.MountID,
			MountKeyID: testMount.MountKeyID,
			Buckets:    []fsmeta.AffinityBucket{1},
		}),
	}))

	_, ok, err := table.Find(compile.AuthorityScope{
		Mount:      testMount.MountID,
		MountKeyID: testMount.MountKeyID,
		Buckets:    []fsmeta.AffinityBucket{1},
	}, testNow.Add(2*time.Hour))
	require.NoError(t, err)
	require.False(t, ok)

	_, ok, err = table.Find(compile.AuthorityScope{
		Mount:      "other",
		MountKeyID: 9,
		Buckets:    []fsmeta.AffinityBucket{1},
	}, testNow)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestActiveAuthoritiesTreatsEmptyGrantDimensionsAsWildcard(t *testing.T) {
	table := NewActiveAuthorities()
	require.NoError(t, table.Replace([]rootproto.VisibleAuthorityGrant{
		testGrant("g1", "holder-a", compile.AuthorityScope{
			Mount:      testMount.MountID,
			MountKeyID: testMount.MountKeyID,
		}),
	}))

	_, ok, err := table.Find(compile.AuthorityScope{
		Mount:      testMount.MountID,
		MountKeyID: testMount.MountKeyID,
		Buckets:    []fsmeta.AffinityBucket{15},
		Parents:    []model.InodeID{100},
		Inodes:     []model.InodeID{200},
	}, testNow)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestActiveAuthoritiesDoesNotLetSpecificBucketCoverMountWideScope(t *testing.T) {
	table := NewActiveAuthorities()
	require.NoError(t, table.Replace([]rootproto.VisibleAuthorityGrant{
		testGrant("g1", "holder-a", compile.AuthorityScope{
			Mount:      testMount.MountID,
			MountKeyID: testMount.MountKeyID,
			Buckets:    []fsmeta.AffinityBucket{1},
		}),
	}))

	_, ok, err := table.Find(compile.AuthorityScope{
		Mount:      testMount.MountID,
		MountKeyID: testMount.MountKeyID,
	}, testNow)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestActiveAuthoritiesRejectsInvalidDuplicateAndConflictingGrants(t *testing.T) {
	table := NewActiveAuthorities()

	require.ErrorIs(t, table.Replace([]rootproto.VisibleAuthorityGrant{{GrantID: "missing-fields"}}), ErrInvalidGrant)

	left := testGrant("g1", "holder-a", compile.AuthorityScope{
		Mount:      testMount.MountID,
		MountKeyID: testMount.MountKeyID,
		Buckets:    []fsmeta.AffinityBucket{1},
	})
	duplicate := left
	require.ErrorIs(t, table.Replace([]rootproto.VisibleAuthorityGrant{left, duplicate}), ErrInvalidGrant)

	right := testGrant("g2", "holder-b", compile.AuthorityScope{
		Mount:      testMount.MountID,
		MountKeyID: testMount.MountKeyID,
		Buckets:    []fsmeta.AffinityBucket{1},
	})
	require.ErrorIs(t, table.Replace([]rootproto.VisibleAuthorityGrant{left, right}), ErrConflictingGrant)
}

func TestActiveAuthoritiesAllowsDisjointBuckets(t *testing.T) {
	table := NewActiveAuthorities()
	left := testGrant("g1", "holder-a", compile.AuthorityScope{
		Mount:      testMount.MountID,
		MountKeyID: testMount.MountKeyID,
		Buckets:    []fsmeta.AffinityBucket{1},
	})
	right := testGrant("g2", "holder-b", compile.AuthorityScope{
		Mount:      testMount.MountID,
		MountKeyID: testMount.MountKeyID,
		Buckets:    []fsmeta.AffinityBucket{2},
	})
	require.NoError(t, table.Replace([]rootproto.VisibleAuthorityGrant{left, right}))

	found, ok, err := table.Find(compile.AuthorityScope{
		Mount:      testMount.MountID,
		MountKeyID: testMount.MountKeyID,
		Buckets:    []fsmeta.AffinityBucket{2},
	}, testNow)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "holder-b", found.HolderID)
}

func TestActiveAuthoritiesSnapshotIsIsolated(t *testing.T) {
	table := NewActiveAuthorities()
	require.NoError(t, table.Replace([]rootproto.VisibleAuthorityGrant{
		testGrant("g1", "holder-a", compile.AuthorityScope{
			Mount:      testMount.MountID,
			MountKeyID: testMount.MountKeyID,
			Buckets:    []fsmeta.AffinityBucket{1},
		}),
	}))

	snapshot := table.Snapshot()
	require.Len(t, snapshot, 1)
	snapshot[0].Scope.Buckets[0] = 9

	found, ok, err := table.Find(compile.AuthorityScope{
		Mount:      testMount.MountID,
		MountKeyID: testMount.MountKeyID,
		Buckets:    []fsmeta.AffinityBucket{1},
	}, testNow)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []uint16{1}, found.Scope.Buckets)
}

func TestActiveAuthoritiesDetectsAmbiguousTableState(t *testing.T) {
	table := &ActiveAuthorities{grants: map[string]rootproto.VisibleAuthorityGrant{
		"g1": testGrant("g1", "holder-a", compile.AuthorityScope{Mount: "vol", MountKeyID: 7}),
		"g2": testGrant("g2", "holder-b", compile.AuthorityScope{Mount: "vol", MountKeyID: 7}),
	}}
	_, ok, err := table.Find(compile.AuthorityScope{
		Mount:      "vol",
		MountKeyID: 7,
		Buckets:    []fsmeta.AffinityBucket{3},
	}, testNow)
	require.False(t, ok)
	require.True(t, errors.Is(err, ErrAmbiguousAuthority))
}

func TestActiveAuthoritiesFencesMountWideFsmetaKeys(t *testing.T) {
	table := NewActiveAuthorities()
	require.NoError(t, table.Replace([]rootproto.VisibleAuthorityGrant{
		testGrant("g1", "holder-a", compile.AuthorityScope{
			Mount:      testMount.MountID,
			MountKeyID: testMount.MountKeyID,
		}),
	}))

	keys := make([][]byte, 0, 5)
	dentry, err := fsmeta.EncodeDentryKey(testMount, 10, "name")
	require.NoError(t, err)
	keys = append(keys, dentry)
	inode, err := fsmeta.EncodeInodeKey(testMount, 20)
	require.NoError(t, err)
	keys = append(keys, inode)
	session, err := fsmeta.EncodeSessionKey(testMount, 20, "writer")
	require.NoError(t, err)
	keys = append(keys, session)
	usage, err := fsmeta.EncodeUsageKey(testMount, 30)
	require.NoError(t, err)
	keys = append(keys, usage)
	mount, err := fsmeta.EncodeMountKey(testMount)
	require.NoError(t, err)
	keys = append(keys, mount)

	for _, key := range keys {
		found, ok, err := table.FencesKey(key, testNow)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, "holder-a", found.HolderID)
	}
}

func TestActiveAuthoritiesFencesFsmetaKeysFailClosedBeforeSnapshot(t *testing.T) {
	table := NewActiveAuthorities()
	key, err := fsmeta.EncodeDentryKey(testMount, model.RootInode, "artifact")
	require.NoError(t, err)

	_, _, err = table.FencesKey(key, testNow)
	require.ErrorIs(t, err, ErrAuthorityViewStale)

	_, ok, err := table.FencesKey([]byte("plain-user-key"), testNow)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestActiveAuthoritiesFencesSpecificParentAndBucket(t *testing.T) {
	table := NewActiveAuthorities()
	parent := model.InodeID(10)
	require.NoError(t, table.Replace([]rootproto.VisibleAuthorityGrant{
		testGrant("g1", "holder-a", compile.AuthorityScope{
			Mount:      testMount.MountID,
			MountKeyID: testMount.MountKeyID,
			Buckets:    []fsmeta.AffinityBucket{fsmeta.BucketForInodeID(parent)},
			Parents:    []model.InodeID{parent},
		}),
	}))

	matching, err := fsmeta.EncodeDentryKey(testMount, parent, "name")
	require.NoError(t, err)
	_, ok, err := table.FencesKey(matching, testNow)
	require.NoError(t, err)
	require.True(t, ok)

	otherParent, err := fsmeta.EncodeDentryKey(testMount, parent+1, "name")
	require.NoError(t, err)
	_, ok, err = table.FencesKey(otherParent, testNow)
	require.NoError(t, err)
	require.False(t, ok)

	otherMount, err := fsmeta.EncodeDentryKey(model.MountIdentity{MountID: "other", MountKeyID: 9}, parent, "name")
	require.NoError(t, err)
	_, ok, err = table.FencesKey(otherMount, testNow)
	require.NoError(t, err)
	require.False(t, ok)

	_, ok, err = table.FencesKey([]byte("not-fsmeta"), testNow)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestActiveAuthoritiesFencesUsageOnlyWhenScopeMatches(t *testing.T) {
	table := NewActiveAuthorities()
	require.NoError(t, table.Replace([]rootproto.VisibleAuthorityGrant{
		testGrant("g1", "holder-a", compile.AuthorityScope{
			Mount:      testMount.MountID,
			MountKeyID: testMount.MountKeyID,
			Inodes:     []model.InodeID{20},
		}),
	}))

	matching, err := fsmeta.EncodeUsageKey(testMount, 20)
	require.NoError(t, err)
	_, ok, err := table.FencesKey(matching, testNow)
	require.NoError(t, err)
	require.True(t, ok)

	other, err := fsmeta.EncodeUsageKey(testMount, 21)
	require.NoError(t, err)
	_, ok, err = table.FencesKey(other, testNow)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestActiveAuthoritiesAppliesRootEvents(t *testing.T) {
	table := NewActiveAuthorities()
	grant := testGrant("g1", "holder-a", compile.AuthorityScope{
		Mount:      testMount.MountID,
		MountKeyID: testMount.MountKeyID,
		Buckets:    []fsmeta.AffinityBucket{1},
	})

	require.NoError(t, table.ApplyRootEvent(rootevent.VisibleAuthorityGranted(grant)))
	require.Equal(t, grant.GrantID, table.Snapshot()[0].GrantID)

	replacement := grant
	replacement.HolderID = "holder-b"
	replacement.EpochID = 2
	require.NoError(t, table.ApplyRootEvent(rootevent.VisibleAuthorityGranted(replacement)))
	require.Equal(t, replacement.HolderID, table.Snapshot()[0].HolderID)

	require.NoError(t, table.ApplyRootEvent(rootevent.VisibleAuthorityRetired(replacement)))
	require.Empty(t, table.Snapshot())
}

func TestActiveAuthoritiesRejectsConflictingRootEvent(t *testing.T) {
	table := NewActiveAuthorities()
	left := testGrant("g1", "holder-a", compile.AuthorityScope{
		Mount:      testMount.MountID,
		MountKeyID: testMount.MountKeyID,
		Buckets:    []fsmeta.AffinityBucket{1},
	})
	right := testGrant("g2", "holder-b", compile.AuthorityScope{
		Mount:      testMount.MountID,
		MountKeyID: testMount.MountKeyID,
		Buckets:    []fsmeta.AffinityBucket{1},
	})

	require.NoError(t, table.ApplyRootEvent(rootevent.VisibleAuthorityGranted(left)))
	require.ErrorIs(t, table.ApplyRootEvent(rootevent.VisibleAuthorityGranted(right)), ErrConflictingGrant)
	require.Equal(t, left.GrantID, table.Snapshot()[0].GrantID)
}

func TestActiveAuthoritiesRootGrantReplacesOlderOverlap(t *testing.T) {
	table := NewActiveAuthorities()
	old := testGrant("g1", "holder-a", compile.AuthorityScope{
		Mount:      testMount.MountID,
		MountKeyID: testMount.MountKeyID,
		Buckets:    []fsmeta.AffinityBucket{1},
	})
	next := testGrant("g2", "holder-b", compile.AuthorityScope{
		Mount:      testMount.MountID,
		MountKeyID: testMount.MountKeyID,
		Buckets:    []fsmeta.AffinityBucket{1},
	})
	next.EpochID = old.EpochID + 1

	require.NoError(t, table.ApplyRootEvent(rootevent.VisibleAuthorityGranted(old)))
	require.NoError(t, table.ApplyRootEvent(rootevent.VisibleAuthorityGranted(next)))

	snapshot := table.Snapshot()
	require.Len(t, snapshot, 1)
	require.Equal(t, next.GrantID, snapshot[0].GrantID)
	require.Equal(t, next.HolderID, snapshot[0].HolderID)
}

func TestActiveAuthoritiesRootGrantIgnoresOlderOverlap(t *testing.T) {
	table := NewActiveAuthorities()
	current := testGrant("g2", "holder-b", compile.AuthorityScope{
		Mount:      testMount.MountID,
		MountKeyID: testMount.MountKeyID,
		Buckets:    []fsmeta.AffinityBucket{1},
	})
	current.EpochID = 2
	old := testGrant("g1", "holder-a", compile.AuthorityScope{
		Mount:      testMount.MountID,
		MountKeyID: testMount.MountKeyID,
		Buckets:    []fsmeta.AffinityBucket{1},
	})

	require.NoError(t, table.ApplyRootEvent(rootevent.VisibleAuthorityGranted(current)))
	require.NoError(t, table.ApplyRootEvent(rootevent.VisibleAuthorityGranted(old)))

	snapshot := table.Snapshot()
	require.Len(t, snapshot, 1)
	require.Equal(t, current.GrantID, snapshot[0].GrantID)
}

func TestAuthorityScopeFromDeltaConvertsRootTypes(t *testing.T) {
	scope := AuthorityScopeFromDelta(compile.AuthorityScope{
		Mount:      testMount.MountID,
		MountKeyID: testMount.MountKeyID,
		Buckets:    []fsmeta.AffinityBucket{1, 2},
		Parents:    []model.InodeID{10},
		Inodes:     []model.InodeID{20},
	})

	require.Equal(t, "vol", scope.MountID)
	require.Equal(t, uint64(7), scope.MountKeyID)
	require.Equal(t, []uint16{1, 2}, scope.Buckets)
	require.Equal(t, []uint64{10}, scope.Parents)
	require.Equal(t, []uint64{20}, scope.Inodes)
}

func BenchmarkActiveAuthoritiesApplyRootEvent(b *testing.B) {
	b.ReportAllocs()
	table := NewActiveAuthorities()
	grants := make([]rootproto.VisibleAuthorityGrant, 0, 16)
	for bucket := range 16 {
		grants = append(grants, testGrant("g"+string(rune('a'+bucket)), "holder-a", compile.AuthorityScope{
			Mount:      testMount.MountID,
			MountKeyID: testMount.MountKeyID,
			Buckets:    []fsmeta.AffinityBucket{fsmeta.AffinityBucket(bucket)},
		}))
	}
	require.NoError(b, table.Replace(grants))
	event := rootevent.VisibleAuthorityRetired(grants[11])

	for b.Loop() {
		require.NoError(b, table.ApplyRootEvent(rootevent.VisibleAuthorityGranted(grants[11])))
		require.NoError(b, table.ApplyRootEvent(event))
	}
}

func BenchmarkActiveAuthoritiesFind(b *testing.B) {
	b.ReportAllocs()
	table := NewActiveAuthorities()
	grants := make([]rootproto.VisibleAuthorityGrant, 0, 16)
	for bucket := range 16 {
		grants = append(grants, testGrant("g"+string(rune('a'+bucket)), "holder-a", compile.AuthorityScope{
			Mount:      testMount.MountID,
			MountKeyID: testMount.MountKeyID,
			Buckets:    []fsmeta.AffinityBucket{fsmeta.AffinityBucket(bucket)},
		}))
	}
	require.NoError(b, table.Replace(grants))
	scope := compile.AuthorityScope{
		Mount:      testMount.MountID,
		MountKeyID: testMount.MountKeyID,
		Buckets:    []fsmeta.AffinityBucket{11},
	}

	for b.Loop() {
		grant, ok, err := table.Find(scope, testNow)
		if err != nil || !ok || grant.HolderID != "holder-a" {
			b.Fatalf("unexpected lookup result: grant=%+v ok=%v err=%v", grant, ok, err)
		}
	}
}

func testGrant(id, holder string, scope compile.AuthorityScope) rootproto.VisibleAuthorityGrant {
	return rootproto.VisibleAuthorityGrant{
		GrantID:         id,
		EpochID:         1,
		HolderID:        holder,
		Scope:           AuthorityScopeFromDelta(scope),
		ExpiresUnixNano: testNow.Add(time.Hour).UnixNano(),
	}
}
