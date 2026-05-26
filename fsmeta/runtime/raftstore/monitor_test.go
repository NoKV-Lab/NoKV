// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"context"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/fsmeta/model"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type fakeMountList struct {
	mountCalls   int
	quotaCalls   int
	subtreeCalls int
	mounts       []*coordpb.MountInfo
	quotas       []*coordpb.QuotaFenceInfo
	subtrees     []*coordpb.SubtreeAuthorityInfo
	err          error
}

func (c *fakeMountList) ListMounts(context.Context, *coordpb.ListMountsRequest) (*coordpb.ListMountsResponse, error) {
	c.mountCalls++
	return &coordpb.ListMountsResponse{Mounts: c.mounts}, c.err
}

func (c *fakeMountList) ListQuotaFences(context.Context, *coordpb.ListQuotaFencesRequest) (*coordpb.ListQuotaFencesResponse, error) {
	c.quotaCalls++
	return &coordpb.ListQuotaFencesResponse{Fences: c.quotas}, c.err
}

func (c *fakeMountList) ListSubtreeAuthorities(context.Context, *coordpb.ListSubtreeAuthoritiesRequest) (*coordpb.ListSubtreeAuthoritiesResponse, error) {
	c.subtreeCalls++
	return &coordpb.ListSubtreeAuthoritiesResponse{Subtrees: c.subtrees}, c.err
}

func (c *fakeMountList) WatchRootEvents(context.Context, *coordpb.WatchRootEventsRequest, ...grpc.CallOption) (coordpb.Coordinator_WatchRootEventsClient, error) {
	return nil, c.err
}

type fakeRetireRouter struct {
	retired []model.MountID
}

func (r *fakeRetireRouter) RetireMount(m model.MountID) int {
	r.retired = append(r.retired, m)
	return 1
}

type subtreePublishCall struct {
	mount    model.MountID
	root     model.InodeID
	frontier uint64
}

type fakeSubtreePublisher struct {
	starts    []subtreePublishCall
	completes []subtreePublishCall
}

func (p *fakeSubtreePublisher) StartSubtreeHandoff(_ context.Context, mount model.MountID, root model.InodeID, frontier uint64) error {
	p.starts = append(p.starts, subtreePublishCall{mount: mount, root: root, frontier: frontier})
	return nil
}

func (p *fakeSubtreePublisher) CompleteSubtreeHandoff(_ context.Context, mount model.MountID, root model.InodeID, frontier uint64) error {
	p.completes = append(p.completes, subtreePublishCall{mount: mount, root: root, frontier: frontier})
	return nil
}

func TestMonitorRetiresWatchersAndCache(t *testing.T) {
	list := &fakeMountList{
		mounts: []*coordpb.MountInfo{{
			MountId:       "vol",
			RootInode:     1,
			SchemaVersion: 1,
			State:         coordpb.MountState_MOUNT_STATE_RETIRED,
		}},
	}
	cache := &mountCache{ttl: time.Minute}
	router := &fakeRetireRouter{}

	quotas := &quotaCache{ttl: time.Minute}
	mon := &monitor{coord: list, router: router, cache: cache, quotas: quotas}
	require.NoError(t, mon.bootstrap(context.Background()))

	require.Equal(t, 1, list.mountCalls)
	require.Equal(t, 1, list.quotaCalls)
	require.Equal(t, 1, list.subtreeCalls)
	require.Equal(t, []model.MountID{"vol"}, router.retired)

	entry, ok := cache.entries["vol"]
	require.True(t, ok)
	require.True(t, entry.record.Retired)
}

func TestMonitorCompletesPendingSubtreeHandoffs(t *testing.T) {
	list := &fakeMountList{
		subtrees: []*coordpb.SubtreeAuthorityInfo{{
			MountId:             "vol",
			RootInode:           1,
			State:               coordpb.SubtreeAuthorityState_SUBTREE_AUTHORITY_STATE_HANDOFF,
			PredecessorFrontier: 42,
		}},
	}
	router := &fakeRetireRouter{}
	pub := &fakeSubtreePublisher{}

	mon := &monitor{coord: list, router: router, subtrees: pub}
	require.NoError(t, mon.bootstrap(context.Background()))

	require.Equal(t, []subtreePublishCall{{mount: "vol", root: 1, frontier: 42}}, pub.completes)
}

func TestMonitorRefreshesQuotaFences(t *testing.T) {
	list := &fakeMountList{
		quotas: []*coordpb.QuotaFenceInfo{{
			Subject:     &coordpb.QuotaSubject{MountId: "vol", SubtreeRoot: 7},
			LimitBytes:  4096,
			LimitInodes: 12,
			Era:         3,
		}},
	}
	quotas := &quotaCache{ttl: time.Minute}
	router := &fakeRetireRouter{}

	mon := &monitor{coord: list, router: router, quotas: quotas}
	require.NoError(t, mon.bootstrap(context.Background()))

	fence, ok, found := quotas.lookup(quotaSubject{mount: "vol", scope: 7}, time.Now())
	require.False(t, found)
	require.False(t, ok)
	require.Equal(t, quotaFence{}, fence)
}
