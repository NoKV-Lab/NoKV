// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/engine/wal"
	fsperas "github.com/feichai0017/NoKV/experimental/peras/exec"
	runtimeperas "github.com/feichai0017/NoKV/experimental/peras/runtime"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestStartStoreWitnessUsesRootAuthorityFeed(t *testing.T) {
	scope := testStoreWitnessAuthorityScope()
	source := &testStoreWitnessAuthoritySource{
		grants: []rootproto.VisibleAuthorityGrant{testStoreWitnessAuthorityGrant(scope)},
	}
	opener := &testStoreWitnessWALOpener{dir: t.TempDir()}
	defer opener.close(t)

	runtime, err := StartStoreWitness(t.Context(), 1, source, opener, wal.DurabilityFsync)
	require.NoError(t, err)
	require.NotNil(t, runtime)
	require.NotNil(t, runtime.Witness)
	require.NotNil(t, runtime.Authorities)
	require.NotNil(t, runtime.Feed)
	defer func() { require.NoError(t, runtime.Close()) }()

	record := testStoreWitnessSegmentRecord()
	require.Eventually(t, func() bool {
		return runtime.Witness.AppendSegments(t.Context(), scope, []fsperas.SegmentWitnessRecord{record}) == nil
	}, time.Second, 10*time.Millisecond)
}

type testStoreWitnessWALOpener struct {
	dir      string
	managers []*wal.Manager
}

func (o *testStoreWitnessWALOpener) OpenControlWAL(groupID uint64) (*wal.Manager, error) {
	manager, err := wal.Open(wal.Config{Dir: filepath.Join(o.dir, "wal", fmt.Sprintf("group-%d", groupID))})
	if err != nil {
		return nil, err
	}
	o.managers = append(o.managers, manager)
	return manager, nil
}

func (o *testStoreWitnessWALOpener) close(t *testing.T) {
	t.Helper()
	for _, manager := range o.managers {
		require.NoError(t, manager.Close())
	}
}

type testStoreWitnessAuthoritySource struct {
	grants []rootproto.VisibleAuthorityGrant
}

func (s *testStoreWitnessAuthoritySource) ListVisibleAuthorityGrants(context.Context, *coordpb.ListVisibleAuthorityGrantsRequest) (*coordpb.ListVisibleAuthorityGrantsResponse, error) {
	out := make([]*metapb.RootVisibleAuthorityGrant, 0, len(s.grants))
	for _, grant := range s.grants {
		out = append(out, metawire.RootVisibleAuthorityGrantToProto(grant))
	}
	return &coordpb.ListVisibleAuthorityGrantsResponse{Grants: out}, nil
}

func (s *testStoreWitnessAuthoritySource) WatchRootEvents(ctx context.Context, _ *coordpb.WatchRootEventsRequest, _ ...grpc.CallOption) (coordpb.Coordinator_WatchRootEventsClient, error) {
	return testStoreWitnessWatchStream{ctx: ctx}, nil
}

type testStoreWitnessWatchStream struct {
	grpc.ClientStream
	ctx context.Context
}

func (s testStoreWitnessWatchStream) Recv() (*coordpb.WatchRootEventsResponse, error) {
	<-s.ctx.Done()
	return nil, s.ctx.Err()
}

func testStoreWitnessAuthorityScope() compile.AuthorityScope {
	return compile.AuthorityScope{
		Mount:      model.MountID("vol"),
		MountKeyID: model.MountKeyID(7),
		Buckets:    []layout.AffinityBucket{3},
		Parents:    []model.InodeID{11},
		Inodes:     []model.InodeID{29},
	}
}

func testStoreWitnessAuthorityGrant(scope compile.AuthorityScope) rootproto.VisibleAuthorityGrant {
	return rootproto.VisibleAuthorityGrant{
		GrantID:         "grant-1",
		EpochID:         1,
		HolderID:        "holder-a",
		Scope:           runtimeperas.AuthorityScopeFromDelta(scope),
		ExpiresUnixNano: time.Now().Add(time.Hour).UnixNano(),
	}
}

func testStoreWitnessSegmentRecord() fsperas.SegmentWitnessRecord {
	var root [32]byte
	root[0] = 1
	var digest [32]byte
	digest[0] = 2
	return fsperas.SegmentWitnessRecord{
		EpochID:              1,
		SegmentRoot:          root,
		SegmentPayloadDigest: digest,
		SegmentPayloadSize:   4096,
		SegmentPointer:       "inline",
		OperationCount:       64,
		EntryCount:           128,
		TimestampUnixNano:    time.Now().UnixNano(),
		HolderID:             "holder-a",
	}
}
