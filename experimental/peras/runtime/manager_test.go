// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"context"
	"testing"
	"time"

	fsperas "github.com/feichai0017/NoKV/experimental/peras/exec"
	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/model"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/stretchr/testify/require"
)

type fakeVisibleAuthorityClient struct {
	calls     int
	sealCalls int
	last      rootproto.VisibleAuthorityCommand
	resp      *coordpb.ApplyVisibleAuthorityResponse
	seals     []*metapb.RootVisibleAuthoritySeal
	err       error
}

func (f *fakeVisibleAuthorityClient) ApplyVisibleAuthority(_ context.Context, req *coordpb.ApplyVisibleAuthorityRequest) (*coordpb.ApplyVisibleAuthorityResponse, error) {
	f.calls++
	f.last = metawire.RootVisibleAuthorityCommandFromProto(req.GetCommand())
	return f.resp, f.err
}

func (f *fakeVisibleAuthorityClient) ListVisibleAuthoritySeals(context.Context, *coordpb.ListVisibleAuthoritySealsRequest) (*coordpb.ListVisibleAuthoritySealsResponse, error) {
	f.sealCalls++
	return &coordpb.ListVisibleAuthoritySealsResponse{Seals: f.seals}, f.err
}

func TestManagerAcquireInstallsGrantedAuthority(t *testing.T) {
	now := time.Unix(10, 0)
	scope := testRuntimePerasScope(1)
	grant := testRuntimeVisibleGrant("holder-a/1", "holder-a", scope, now.Add(time.Minute))
	client := &fakeVisibleAuthorityClient{
		resp: &coordpb.ApplyVisibleAuthorityResponse{
			Status:       metapb.RootVisibleAuthorityApplyStatus_ROOT_VISIBLE_AUTHORITY_APPLY_STATUS_GRANTED,
			Grant:        metawire.RootVisibleAuthorityGrantToProto(grant),
			ActiveGrants: []*metapb.RootVisibleAuthorityGrant{metawire.RootVisibleAuthorityGrantToProto(grant)},
		},
	}
	table := NewActiveAuthorities()
	manager, err := NewAuthorityManager(client, table, "holder-a", time.Minute, func() time.Time { return now })
	require.NoError(t, err)

	got, owned, err := manager.Acquire(context.Background(), scope)
	require.NoError(t, err)
	require.True(t, owned)
	require.Equal(t, grant, got)
	require.Equal(t, 1, client.calls)
	require.Equal(t, rootproto.VisibleAuthorityActAcquire, client.last.Kind)
	require.Equal(t, "holder-a", client.last.HolderID)
	require.Equal(t, now.Add(time.Minute).UnixNano(), client.last.ExpiresUnixNano)
	require.Empty(t, client.last.Scope.Buckets)
	require.Empty(t, client.last.Scope.Parents)
	require.Empty(t, client.last.Scope.Inodes)
	require.Equal(t, []rootproto.VisibleAuthorityGrant{grant}, table.Snapshot())
}

func TestManagerAcquireMountWideGrantCoversBucket(t *testing.T) {
	now := time.Unix(10, 0)
	scope := testRuntimePerasScope(1)
	wideScope := scope
	wideScope.Buckets = nil
	wideScope.Parents = nil
	wideScope.Inodes = nil
	grant := testRuntimeVisibleGrant("holder-a/1", "holder-a", wideScope, now.Add(time.Minute))
	table := NewActiveAuthorities()
	require.NoError(t, table.Replace([]rootproto.VisibleAuthorityGrant{grant}))
	client := &fakeVisibleAuthorityClient{}
	manager, err := NewAuthorityManager(client, table, "holder-a", time.Minute, func() time.Time { return now })
	require.NoError(t, err)

	got, owned, err := manager.Acquire(context.Background(), scope)
	require.NoError(t, err)
	require.True(t, owned)
	require.Equal(t, grant.GrantID, got.GrantID)
	require.Equal(t, grant.HolderID, got.HolderID)
	require.Equal(t, grant.Scope.MountKeyID, got.Scope.MountKeyID)
	require.Empty(t, got.Scope.Buckets)
	require.Empty(t, got.Scope.Parents)
	require.Empty(t, got.Scope.Inodes)
	require.Zero(t, client.calls)
}

func TestManagerAcquireUsesLocalHeldGrant(t *testing.T) {
	now := time.Unix(10, 0)
	scope := testRuntimePerasScope(1)
	grant := testRuntimeVisibleGrant("holder-a/1", "holder-a", scope, now.Add(time.Minute))
	table := NewActiveAuthorities()
	require.NoError(t, table.Replace([]rootproto.VisibleAuthorityGrant{grant}))
	client := &fakeVisibleAuthorityClient{}
	manager, err := NewAuthorityManager(client, table, "holder-a", time.Minute, func() time.Time { return now })
	require.NoError(t, err)

	got, owned, err := manager.Acquire(context.Background(), scope)
	require.NoError(t, err)
	require.True(t, owned)
	require.Equal(t, grant, got)
	require.Zero(t, client.calls)
}

func TestManagerAcquireHeldUpdatesActiveAuthorityView(t *testing.T) {
	now := time.Unix(10, 0)
	scope := testRuntimePerasScope(1)
	held := testRuntimeVisibleGrant("holder-b/1", "holder-b", scope, now.Add(time.Minute))
	client := &fakeVisibleAuthorityClient{
		resp: &coordpb.ApplyVisibleAuthorityResponse{
			Status:       metapb.RootVisibleAuthorityApplyStatus_ROOT_VISIBLE_AUTHORITY_APPLY_STATUS_HELD,
			ActiveGrants: []*metapb.RootVisibleAuthorityGrant{metawire.RootVisibleAuthorityGrantToProto(held)},
		},
	}
	table := NewActiveAuthorities()
	manager, err := NewAuthorityManager(client, table, "holder-a", time.Minute, func() time.Time { return now })
	require.NoError(t, err)

	got, owned, err := manager.Acquire(context.Background(), scope)
	require.NoError(t, err)
	require.False(t, owned)
	require.Equal(t, held, got)
	require.Equal(t, []rootproto.VisibleAuthorityGrant{held}, table.Snapshot())
}

func TestManagerRetireAuthority(t *testing.T) {
	now := time.Unix(10, 0)
	scope := testRuntimePerasScope(1)
	grant := testRuntimeVisibleGrant("holder-a/1", "holder-a", scope, now.Add(time.Minute))
	client := &fakeVisibleAuthorityClient{
		resp: &coordpb.ApplyVisibleAuthorityResponse{
			Status: metapb.RootVisibleAuthorityApplyStatus_ROOT_VISIBLE_AUTHORITY_APPLY_STATUS_RETIRED,
		},
	}
	table := NewActiveAuthorities()
	require.NoError(t, table.Replace([]rootproto.VisibleAuthorityGrant{grant}))
	manager, err := NewAuthorityManager(client, table, "holder-a", time.Minute, func() time.Time { return now })
	require.NoError(t, err)

	require.NoError(t, manager.Retire(context.Background(), grant))
	require.Equal(t, rootproto.VisibleAuthorityActRetire, client.last.Kind)
	require.Equal(t, grant.GrantID, client.last.GrantID)
	require.Empty(t, table.Snapshot())
}

func TestManagerPublishesSegmentSeal(t *testing.T) {
	now := time.Unix(10, 0)
	scope := testRuntimePerasScope(1)
	grant := testRuntimeVisibleGrant("holder-a/1", "holder-a", scope, now.Add(time.Minute))
	client := &fakeVisibleAuthorityClient{
		resp: &coordpb.ApplyVisibleAuthorityResponse{
			Status:       metapb.RootVisibleAuthorityApplyStatus_ROOT_VISIBLE_AUTHORITY_APPLY_STATUS_SEALED,
			ActiveGrants: []*metapb.RootVisibleAuthorityGrant{metawire.RootVisibleAuthorityGrantToProto(grant)},
		},
	}
	table := NewActiveAuthorities()
	require.NoError(t, table.Replace([]rootproto.VisibleAuthorityGrant{grant}))
	manager, err := NewAuthorityManager(client, table, "holder-a", time.Minute, func() time.Time { return now })
	require.NoError(t, err)
	segment := testRuntimePerasSegment(t)
	var digest [32]byte
	digest[0] = 99
	cursor := InstallCursor{RegionID: 7, Term: 3, Index: 99, InstallVersion: 1234}

	require.NoError(t, manager.PublishSegmentSeal(context.Background(), grant, segment, digest, cursor))
	require.Equal(t, rootproto.VisibleAuthorityActSeal, client.last.Kind)
	require.Equal(t, grant.GrantID, client.last.GrantID)
	require.Equal(t, segment.Root, client.last.SegmentRoot)
	require.Equal(t, digest, client.last.SegmentPayloadDigest)
	require.Equal(t, uint64(1), client.last.OperationCount)
	require.Equal(t, uint64(2), client.last.EntryCount)
	require.Equal(t, cursor.RegionID, client.last.InstallRegionID)
	require.Equal(t, cursor.Term, client.last.InstallTerm)
	require.Equal(t, cursor.Index, client.last.InstallIndex)
	require.Equal(t, cursor.InstallVersion, client.last.InstallVersion)
}

func TestManagerListsMatchingRootSeals(t *testing.T) {
	now := time.Unix(10, 0)
	scope := testRuntimePerasScope(1)
	matching := testRuntimeVisibleSeal("grant-a", "holder-a", scope, now)
	otherScope := testRuntimePerasScope(2)
	other := testRuntimeVisibleSeal("grant-b", "holder-a", otherScope, now)
	client := &fakeVisibleAuthorityClient{
		seals: []*metapb.RootVisibleAuthoritySeal{
			metawire.RootVisibleAuthoritySealToProto(matching),
			metawire.RootVisibleAuthoritySealToProto(other),
		},
	}
	manager, err := NewAuthorityManager(client, NewActiveAuthorities(), "holder-a", time.Minute, func() time.Time { return now })
	require.NoError(t, err)

	seals, err := manager.ListVisibleAuthoritySeals(context.Background(), scope)
	require.NoError(t, err)
	require.Equal(t, []rootproto.VisibleAuthoritySeal{matching}, seals)
	require.Equal(t, 1, client.sealCalls)
}

func TestManagerRetireVisibleAuthorityFiltersScope(t *testing.T) {
	now := time.Unix(10, 0)
	scopeA := testRuntimePerasScope(1)
	scopeB := testRuntimePerasScope(2)
	grantA := testRuntimeVisibleGrant("holder-a/1", "holder-a", scopeA, now.Add(time.Minute))
	grantB := testRuntimeVisibleGrant("holder-a/2", "holder-a", scopeB, now.Add(time.Minute))
	client := &fakeVisibleAuthorityClient{
		resp: &coordpb.ApplyVisibleAuthorityResponse{
			Status:       metapb.RootVisibleAuthorityApplyStatus_ROOT_VISIBLE_AUTHORITY_APPLY_STATUS_RETIRED,
			ActiveGrants: []*metapb.RootVisibleAuthorityGrant{metawire.RootVisibleAuthorityGrantToProto(grantB)},
		},
	}
	table := NewActiveAuthorities()
	require.NoError(t, table.Replace([]rootproto.VisibleAuthorityGrant{grantA, grantB}))
	manager, err := NewAuthorityManager(client, table, "holder-a", time.Minute, func() time.Time { return now })
	require.NoError(t, err)

	require.NoError(t, manager.RetireVisibleAuthority(context.Background(), scopeA))
	require.Equal(t, 1, client.calls)
	require.Equal(t, grantA.GrantID, client.last.GrantID)
	require.Equal(t, []rootproto.VisibleAuthorityGrant{grantB}, table.Snapshot())
}

func TestManagerMountRetireScopeMatchesBucketGrants(t *testing.T) {
	now := time.Unix(10, 0)
	scopeA := testRuntimePerasScope(1)
	scopeB := testRuntimePerasScope(2)
	grantA := testRuntimeVisibleGrant("holder-a/1", "holder-a", scopeA, now.Add(time.Minute))
	grantB := testRuntimeVisibleGrant("holder-a/2", "holder-a", scopeB, now.Add(time.Minute))
	table := NewActiveAuthorities()
	require.NoError(t, table.Replace([]rootproto.VisibleAuthorityGrant{grantA, grantB}))
	manager, err := NewAuthorityManager(&fakeVisibleAuthorityClient{}, table, "holder-a", time.Minute, func() time.Time { return now })
	require.NoError(t, err)

	grants := manager.ownedGrantsForScopes(compile.AuthorityScope{Mount: "vol", MountKeyID: 7})
	require.ElementsMatch(t, []rootproto.VisibleAuthorityGrant{grantA, grantB}, grants)
}

func TestManagerRetireVisibleAuthorityIgnoresForeignGrant(t *testing.T) {
	now := time.Unix(10, 0)
	scope := testRuntimePerasScope(1)
	grant := testRuntimeVisibleGrant("holder-b/1", "holder-b", scope, now.Add(time.Minute))
	client := &fakeVisibleAuthorityClient{}
	table := NewActiveAuthorities()
	require.NoError(t, table.Replace([]rootproto.VisibleAuthorityGrant{grant}))
	manager, err := NewAuthorityManager(client, table, "holder-a", time.Minute, func() time.Time { return now })
	require.NoError(t, err)

	require.NoError(t, manager.RetireVisibleAuthority(context.Background(), scope))
	require.Zero(t, client.calls)
	require.Equal(t, []rootproto.VisibleAuthorityGrant{grant}, table.Snapshot())
}

func TestManagerRejectsInvalidConfigAndResponses(t *testing.T) {
	_, err := NewAuthorityManager(nil, NewActiveAuthorities(), "holder-a", time.Minute, nil)
	require.ErrorIs(t, err, ErrClientRequired)
	_, err = NewAuthorityManager(&fakeVisibleAuthorityClient{}, nil, "holder-a", time.Minute, nil)
	require.ErrorIs(t, err, ErrTableRequired)
	_, err = NewAuthorityManager(&fakeVisibleAuthorityClient{}, NewActiveAuthorities(), "", time.Minute, nil)
	require.ErrorIs(t, err, ErrHolderRequired)
	_, err = NewAuthorityManager(&fakeVisibleAuthorityClient{}, NewActiveAuthorities(), "holder-a", -time.Second, nil)
	require.ErrorIs(t, err, ErrTTLInvalid)

	manager, err := NewAuthorityManager(&fakeVisibleAuthorityClient{
		resp: &coordpb.ApplyVisibleAuthorityResponse{Status: metapb.RootVisibleAuthorityApplyStatus_ROOT_VISIBLE_AUTHORITY_APPLY_STATUS_GRANTED},
	}, NewActiveAuthorities(), "holder-a", time.Minute, func() time.Time { return time.Unix(10, 0) })
	require.NoError(t, err)
	_, _, err = manager.Acquire(context.Background(), testRuntimePerasScope(1))
	require.ErrorIs(t, err, ErrInvalidResponse)
}

func TestManagerRetireRejectsForeignGrant(t *testing.T) {
	now := time.Unix(10, 0)
	scope := testRuntimePerasScope(1)
	grant := testRuntimeVisibleGrant("holder-b/1", "holder-b", scope, now.Add(time.Minute))
	manager, err := NewAuthorityManager(&fakeVisibleAuthorityClient{}, NewActiveAuthorities(), "holder-a", time.Minute, func() time.Time { return now })
	require.NoError(t, err)

	err = manager.Retire(context.Background(), grant)
	require.ErrorIs(t, err, ErrNotHeld)
	require.True(t, IsNotHeld(err))
}

func BenchmarkManagerAcquireLocalHeld(b *testing.B) {
	now := time.Unix(10, 0)
	scope := testRuntimePerasScope(1)
	grant := testRuntimeVisibleGrant("holder-a/1", "holder-a", scope, now.Add(time.Minute))
	table := NewActiveAuthorities()
	if err := table.Replace([]rootproto.VisibleAuthorityGrant{grant}); err != nil {
		b.Fatal(err)
	}
	manager, err := NewAuthorityManager(&fakeVisibleAuthorityClient{}, table, "holder-a", time.Minute, func() time.Time { return now })
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	for b.Loop() {
		got, owned, err := manager.Acquire(context.Background(), scope)
		if err != nil || !owned || got.GrantID == "" {
			b.Fatalf("owned=%v grant=%q err=%v", owned, got.GrantID, err)
		}
	}
}

func testRuntimePerasScope(bucket fsmeta.AffinityBucket) compile.AuthorityScope {
	return compile.AuthorityScope{
		Mount:      "vol",
		MountKeyID: 7,
		Buckets:    []fsmeta.AffinityBucket{bucket},
		Parents:    []model.InodeID{99},
		Inodes:     []model.InodeID{100},
	}
}

func testRuntimeVisibleGrant(id, holder string, scope compile.AuthorityScope, expires time.Time) rootproto.VisibleAuthorityGrant {
	return rootproto.VisibleAuthorityGrant{
		GrantID:         id,
		EpochID:         1,
		HolderID:        holder,
		Scope:           AuthorityScopeFromDelta(scope),
		ExpiresUnixNano: expires.UnixNano(),
	}
}

func testRuntimeVisibleSeal(id, holder string, scope compile.AuthorityScope, sealed time.Time) rootproto.VisibleAuthoritySeal {
	return rootproto.VisibleAuthoritySeal{
		GrantID:              id,
		EpochID:              1,
		HolderID:             holder,
		Scope:                AuthorityScopeFromDelta(scope),
		SegmentRoot:          [32]byte{1},
		SegmentPayloadDigest: [32]byte{2},
		OperationCount:       3,
		EntryCount:           4,
		SealedUnixNano:       sealed.UnixNano(),
		InstallRegionID:      5,
		InstallTerm:          6,
		InstallIndex:         7,
		InstallVersion:       8,
	}
}

func testRuntimePerasSegment(t *testing.T) fsperas.PerasSegment {
	t.Helper()
	mount := model.MountIdentity{MountID: "vol", MountKeyID: 7}
	dentry, err := fsmeta.EncodeDentryKey(mount, 99, "a")
	require.NoError(t, err)
	inode, err := fsmeta.EncodeInodeKey(mount, 100)
	require.NoError(t, err)
	segment, err := fsperas.BuildPerasSegmentFromReplayPlan(fsperas.ReplayPlan{
		EpochID: 1,
		Operations: []fsperas.ReplayOperation{
			{
				OpID: fsperas.OperationID{ClientID: "client", Seq: 1},
				Mutations: []fsperas.ReplayMutation{
					{Key: dentry, Value: []byte("dentry-value")},
					{Key: inode, Value: []byte("inode-value")},
				},
			},
		},
	})
	require.NoError(t, err)
	return segment
}
