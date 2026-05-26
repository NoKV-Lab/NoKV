// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/model"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"github.com/stretchr/testify/require"
)

var allocatorTestMount = model.MountIdentity{MountID: "vol", MountKeyID: 1}

type fakeAllocIDClient struct {
	mu      sync.Mutex
	next    uint64
	counts  []uint64
	err     error
	fixed   []*coordpb.AllocIDResponse
	reqs    []*coordpb.AllocIDRequest
	returns []*coordpb.AllocIDResponse
}

func (c *fakeAllocIDClient) AllocID(_ context.Context, req *coordpb.AllocIDRequest) (*coordpb.AllocIDResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reqs = append(c.reqs, req)
	if c.err != nil {
		return nil, c.err
	}
	if len(c.fixed) > 0 {
		resp := c.fixed[0]
		c.fixed = c.fixed[1:]
		c.returns = append(c.returns, resp)
		return resp, nil
	}
	if c.next == 0 {
		c.next = 1
	}
	count := req.GetCount()
	if count == 0 {
		count = 1
	}
	resp := &coordpb.AllocIDResponse{FirstId: c.next, Count: count}
	c.next += count
	c.counts = append(c.counts, count)
	c.returns = append(c.returns, resp)
	return resp, nil
}

func TestShardAffineInodeAllocatorSkipsRootInode(t *testing.T) {
	client := &fakeAllocIDClient{next: 1}
	alloc, err := NewShardAffineInodeAllocatorWithBatch(client, 1, 4)
	require.NoError(t, err)

	inode, err := alloc.AllocateCreateInode(context.Background(), allocatorTestMount, model.RootInode, "file")
	require.NoError(t, err)
	require.Greater(t, inode, model.RootInode)
	require.Equal(t, []uint64{4}, client.counts)
	require.Equal(t, uint64(3), alloc.Stats()["inode_alloc_reserved_total"])
}

func TestShardAffineInodeAllocatorChoosesWorkspaceBucket(t *testing.T) {
	client := &fakeAllocIDClient{next: 2}
	alloc, err := NewShardAffineInodeAllocatorWithBatch(client, fsmeta.DefaultAffinityBucketCount, 64)
	require.NoError(t, err)

	inode, err := alloc.AllocateCreateInode(context.Background(), allocatorTestMount, model.RootInode, "aligned")
	require.NoError(t, err)
	want, err := createDentryBucket(allocatorTestMount, model.RootInode, "aligned")
	require.NoError(t, err)
	got, err := createInodeBucket(allocatorTestMount, inode)
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Equal(t, uint64(1), alloc.Stats()["inode_alloc_affinity_hit_total"])
	require.Equal(t, uint64(0), alloc.Stats()["inode_alloc_affinity_miss_total"])
}

func TestShardAffineInodeAllocatorReturnsUniqueConcurrentIDs(t *testing.T) {
	client := &fakeAllocIDClient{next: 2}
	alloc, err := NewShardAffineInodeAllocatorWithBatch(client, fsmeta.DefaultAffinityBucketCount, 16)
	require.NoError(t, err)

	const workers = 64
	ids := make(chan model.InodeID, workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			inode, err := alloc.AllocateCreateInode(context.Background(), allocatorTestMount, model.RootInode, "hot")
			errs <- err
			ids <- inode
		})
	}
	wg.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	seen := make(map[model.InodeID]struct{}, workers)
	for id := range ids {
		require.NotContains(t, seen, id)
		seen[id] = struct{}{}
	}
	require.Len(t, seen, workers)
}

func TestShardAffineInodeAllocatorPropagatesAllocIDError(t *testing.T) {
	want := errors.New("root unavailable")
	alloc, err := NewShardAffineInodeAllocatorWithBatch(&fakeAllocIDClient{err: want}, fsmeta.DefaultAffinityBucketCount, 8)
	require.NoError(t, err)

	_, err = alloc.AllocateCreateInode(context.Background(), allocatorTestMount, model.RootInode, "file")
	require.ErrorIs(t, err, want)
}

func TestShardAffineInodeAllocatorMissStillReturnsUsableID(t *testing.T) {
	target, err := createDentryBucket(allocatorTestMount, model.RootInode, "file")
	require.NoError(t, err)
	var candidate model.InodeID
	for id := model.InodeID(2); id < 10_000; id++ {
		bucket, err := createInodeBucket(allocatorTestMount, id)
		require.NoError(t, err)
		if bucket != target {
			candidate = id
			break
		}
	}
	require.NotZero(t, candidate)
	client := &fakeAllocIDClient{fixed: []*coordpb.AllocIDResponse{{FirstId: uint64(candidate), Count: 1}}}
	alloc, err := NewShardAffineInodeAllocatorWithBatch(client, fsmeta.DefaultAffinityBucketCount, 1)
	require.NoError(t, err)

	inode, err := alloc.AllocateCreateInode(context.Background(), allocatorTestMount, model.RootInode, "file")
	require.NoError(t, err)
	require.Equal(t, candidate, inode)
	require.Equal(t, uint64(0), alloc.Stats()["inode_alloc_affinity_hit_total"])
	require.Equal(t, uint64(1), alloc.Stats()["inode_alloc_affinity_miss_total"])
}
