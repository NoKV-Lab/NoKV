// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/model"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"github.com/feichai0017/NoKV/utils"
)

const (
	defaultInodeAllocBatchSize  = fsmeta.DefaultAffinityBucketCount * 256
	defaultInodeAffinityBuckets = fsmeta.DefaultAffinityBucketCount
)

// IDAllocatorClient is the rooted coordinator ID surface used by fsmeta. The
// allocator deliberately depends on the RPC contract, not coordinator/idalloc,
// so coordinator remains the only production authority for globally unique IDs.
type IDAllocatorClient interface {
	AllocID(ctx context.Context, req *coordpb.AllocIDRequest) (*coordpb.AllocIDResponse, error)
}

// ShardAffineInodeAllocator prefetches coordinator IDs and returns an inode ID
// whose fsmeta affinity bucket matches the target workspace when possible. A
// miss is still correct: Create keeps the existing 1PC safety gate and falls
// back to Percolator 2PC when local atomicity cannot be proven.
type ShardAffineInodeAllocator struct {
	client    IDAllocatorClient
	buckets   int
	batchSize uint64

	mu    sync.Mutex
	pools map[model.MountKeyID][][]model.InodeID

	total         atomic.Uint64
	affinityHit   atomic.Uint64
	affinityMiss  atomic.Uint64
	refillTotal   atomic.Uint64
	reservedTotal atomic.Uint64
}

func NewShardAffineInodeAllocator(client IDAllocatorClient, shardCount int) (*ShardAffineInodeAllocator, error) {
	return NewShardAffineInodeAllocatorWithBatch(client, shardCount, defaultInodeAllocBatchSize)
}

func NewShardAffineInodeAllocatorWithBatch(client IDAllocatorClient, shardCount int, batchSize uint64) (*ShardAffineInodeAllocator, error) {
	if client == nil {
		return nil, errIDAllocatorClientRequired
	}
	if batchSize == 0 {
		return nil, errInodeAllocBatchRequired
	}
	buckets := max(utils.NormalizeShardCount(shardCount), fsmeta.DefaultAffinityBucketCount)
	return &ShardAffineInodeAllocator{
		client:    client,
		buckets:   buckets,
		batchSize: batchSize,
		pools:     make(map[model.MountKeyID][][]model.InodeID),
	}, nil
}

func (a *ShardAffineInodeAllocator) AllocateCreateInode(ctx context.Context, mount model.MountIdentity, parent model.InodeID, name string) (model.InodeID, error) {
	if a == nil {
		return 0, errIDAllocatorClientRequired
	}
	target, err := createDentryBucket(mount, parent, name)
	if err != nil {
		return 0, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if inode, ok := a.popBucketLocked(mount, target); ok {
		a.total.Add(1)
		a.affinityHit.Add(1)
		return inode, nil
	}
	if err := a.refillLocked(ctx, mount); err != nil {
		return 0, err
	}
	if inode, ok := a.popBucketLocked(mount, target); ok {
		a.total.Add(1)
		a.affinityHit.Add(1)
		return inode, nil
	}
	if inode, ok := a.popAnyLocked(mount); ok {
		a.total.Add(1)
		a.affinityMiss.Add(1)
		return inode, nil
	}
	return 0, errNoUsableInodeID
}

func (a *ShardAffineInodeAllocator) Stats() map[string]any {
	if a == nil {
		return map[string]any{
			"inode_alloc_total":               uint64(0),
			"inode_alloc_affinity_hit_total":  uint64(0),
			"inode_alloc_affinity_miss_total": uint64(0),
			"inode_alloc_refill_total":        uint64(0),
			"inode_alloc_reserved_total":      uint64(0),
		}
	}
	return map[string]any{
		"inode_alloc_total":               a.total.Load(),
		"inode_alloc_affinity_hit_total":  a.affinityHit.Load(),
		"inode_alloc_affinity_miss_total": a.affinityMiss.Load(),
		"inode_alloc_refill_total":        a.refillTotal.Load(),
		"inode_alloc_reserved_total":      a.reservedTotal.Load(),
	}
}

func (a *ShardAffineInodeAllocator) refillLocked(ctx context.Context, mount model.MountIdentity) error {
	resp, err := a.client.AllocID(ctx, &coordpb.AllocIDRequest{Count: a.batchSize})
	if err != nil {
		return err
	}
	if resp == nil {
		return errNilAllocIDResponse
	}
	if resp.GetCount() == 0 {
		return errEmptyAllocIDResponse
	}
	a.refillTotal.Add(1)
	pool := a.ensurePoolsLocked(mount)
	first := resp.GetFirstId()
	for i := uint64(0); i < resp.GetCount(); i++ {
		id := first + i
		if id < first || model.InodeID(id) <= model.RootInode {
			continue
		}
		bucket, err := createInodeBucket(mount, model.InodeID(id))
		if err != nil {
			return err
		}
		pool[bucket] = append(pool[bucket], model.InodeID(id))
		a.reservedTotal.Add(1)
	}
	return nil
}

func (a *ShardAffineInodeAllocator) ensurePoolsLocked(mount model.MountIdentity) [][]model.InodeID {
	if pool := a.pools[mount.MountKeyID]; len(pool) == a.buckets {
		return pool
	}
	pool := make([][]model.InodeID, a.buckets)
	a.pools[mount.MountKeyID] = pool
	return pool
}

func (a *ShardAffineInodeAllocator) popBucketLocked(mount model.MountIdentity, bucket fsmeta.AffinityBucket) (model.InodeID, bool) {
	pool := a.ensurePoolsLocked(mount)
	idx := int(bucket)
	if idx < 0 || idx >= len(pool) || len(pool[idx]) == 0 {
		return 0, false
	}
	last := len(pool[idx]) - 1
	inode := pool[idx][last]
	pool[idx] = pool[idx][:last]
	return inode, true
}

func (a *ShardAffineInodeAllocator) popAnyLocked(mount model.MountIdentity) (model.InodeID, bool) {
	pool := a.ensurePoolsLocked(mount)
	for bucket := range pool {
		if inode, ok := a.popBucketLocked(mount, fsmeta.AffinityBucket(bucket)); ok {
			return inode, true
		}
	}
	return 0, false
}

func createDentryBucket(mount model.MountIdentity, parent model.InodeID, name string) (fsmeta.AffinityBucket, error) {
	if parent == model.RootInode {
		return fsmeta.ChooseWorkspaceBucket(mount, name), nil
	}
	key, err := fsmeta.EncodeDentryKey(mount, parent, name)
	if err != nil {
		return 0, err
	}
	bucket, ok := fsmeta.BucketOfKey(key)
	if !ok {
		return 0, fsmeta.ErrInvalidKey
	}
	return bucket, nil
}

func createInodeBucket(mount model.MountIdentity, inode model.InodeID) (fsmeta.AffinityBucket, error) {
	key, err := fsmeta.EncodeInodeKey(mount, inode)
	if err != nil {
		return 0, err
	}
	bucket, ok := fsmeta.BucketOfKey(key)
	if !ok {
		return 0, fsmeta.ErrInvalidKey
	}
	return bucket, nil
}
