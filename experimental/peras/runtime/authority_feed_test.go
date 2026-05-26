// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	"github.com/feichai0017/NoKV/meta/wire"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestRootAuthorityFeedPollsGrantSnapshotsWhileWatchIsQuiet(t *testing.T) {
	source := &pollingAuthoritySource{}
	table := NewActiveAuthorities()
	ctx := t.Context()
	feed := StartRootAuthorityFeed(ctx, source, table, 5*time.Millisecond)
	require.NotNil(t, feed)
	defer func() { require.NoError(t, feed.Close()) }()

	scope := compile.AuthorityScope{
		Mount:      testMount.MountID,
		MountKeyID: testMount.MountKeyID,
		Buckets:    []layout.AffinityBucket{7},
	}
	source.replace(testGrant("g1", "holder-a", scope))

	require.Eventually(t, func() bool {
		grant, ok, err := table.Find(scope, testNow)
		return err == nil && ok && grant.HolderID == "holder-a"
	}, time.Second, 10*time.Millisecond)
}

func TestRootAuthorityFeedAppliesGrantEvents(t *testing.T) {
	source := &pollingAuthoritySource{}
	table := NewActiveAuthorities()
	feed := &RootAuthorityFeed{source: source, table: table}

	scope := compile.AuthorityScope{
		Mount:      testMount.MountID,
		MountKeyID: testMount.MountKeyID,
		Buckets:    []layout.AffinityBucket{11},
	}
	grant := testGrant("g-event", "holder-event", scope)

	feed.applyRootEvent(rootevent.VisibleAuthorityGranted(grant))
	grant, ok, err := table.Find(scope, testNow)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "holder-event", grant.HolderID)
}

type pollingAuthoritySource struct {
	mu     sync.Mutex
	grants []rootproto.VisibleAuthorityGrant
}

func (s *pollingAuthoritySource) replace(grants ...rootproto.VisibleAuthorityGrant) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.grants = append([]rootproto.VisibleAuthorityGrant(nil), grants...)
}

func (s *pollingAuthoritySource) ListVisibleAuthorityGrants(context.Context, *coordpb.ListVisibleAuthorityGrantsRequest) (*coordpb.ListVisibleAuthorityGrantsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*metapb.RootVisibleAuthorityGrant, 0, len(s.grants))
	for _, grant := range s.grants {
		out = append(out, wire.RootVisibleAuthorityGrantToProto(grant))
	}
	return &coordpb.ListVisibleAuthorityGrantsResponse{Grants: out}, nil
}

func (s *pollingAuthoritySource) WatchRootEvents(ctx context.Context, _ *coordpb.WatchRootEventsRequest, _ ...grpc.CallOption) (coordpb.Coordinator_WatchRootEventsClient, error) {
	return pollingWatchStream{ctx: ctx}, nil
}

type pollingWatchStream struct {
	grpc.ClientStream
	ctx context.Context
}

func (s pollingWatchStream) Recv() (*coordpb.WatchRootEventsResponse, error) {
	<-s.ctx.Done()
	return nil, s.ctx.Err()
}
