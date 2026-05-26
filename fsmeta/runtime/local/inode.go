// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package local

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/feichai0017/NoKV/engine/index"
	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/model"
	localdb "github.com/feichai0017/NoKV/local"
)

// InodeAllocator assigns monotonically increasing local inode IDs.
type InodeAllocator struct {
	mount model.MountIdentity

	mu   sync.Mutex
	next uint64

	total       atomic.Uint64
	affineHit   atomic.Uint64
	affineProbe atomic.Uint64
}

// NewInodeAllocator initializes allocation above every inode key already
// present for mount.
func NewInodeAllocator(db *localdb.DB, mount model.MountIdentity) (*InodeAllocator, error) {
	maxInode, err := maxInodeInStore(db, mount)
	if err != nil {
		return nil, err
	}
	if maxInode < model.RootInode {
		maxInode = model.RootInode
	}
	alloc := &InodeAllocator{mount: mount}
	alloc.next = uint64(maxInode) + 1
	return alloc, nil
}

// AllocateCreateInode implements fsmetaexec.InodeAllocator.
func (a *InodeAllocator) AllocateCreateInode(_ context.Context, mount model.MountIdentity, parent model.InodeID, name string) (model.InodeID, error) {
	if a == nil {
		return 0, errMountRequired
	}
	if mount != a.mount {
		return 0, model.ErrMountNotRegistered
	}
	target, err := localCreateDentryBucket(mount, parent, name)
	if err != nil {
		return 0, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for {
		id := model.InodeID(a.next)
		a.next++
		if id <= model.RootInode {
			continue
		}
		a.affineProbe.Add(1)
		if fsmeta.BucketForInodeID(id) != target {
			continue
		}
		a.total.Add(1)
		a.affineHit.Add(1)
		return id, nil
	}
}

// Stats returns local allocator diagnostics.
func (a *InodeAllocator) Stats() map[string]any {
	if a == nil {
		return map[string]any{
			"next_inode":                       uint64(0),
			"inode_alloc_total":                uint64(0),
			"inode_alloc_affinity_hit_total":   uint64(0),
			"inode_alloc_affinity_probe_total": uint64(0),
		}
	}
	a.mu.Lock()
	next := a.next
	a.mu.Unlock()
	return map[string]any{
		"next_inode":                       next,
		"inode_alloc_total":                a.total.Load(),
		"inode_alloc_affinity_hit_total":   a.affineHit.Load(),
		"inode_alloc_affinity_probe_total": a.affineProbe.Load(),
	}
}

func localCreateDentryBucket(mount model.MountIdentity, parent model.InodeID, name string) (fsmeta.AffinityBucket, error) {
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

func maxInodeInStore(db *localdb.DB, mount model.MountIdentity) (model.InodeID, error) {
	if db == nil {
		return 0, nil
	}
	iter := db.NewInternalIterator(&index.Options{IsAsc: true})
	if iter == nil {
		return 0, nil
	}
	defer func() { _ = iter.Close() }()
	var maxInode model.InodeID
	iter.Seek(kv.InternalKey(kv.CFWrite, nil, kv.MaxVersion))
	for iter.Valid() {
		item := iter.Item()
		if item == nil || item.Entry() == nil {
			iter.Next()
			continue
		}
		cf, userKey, _, ok := kv.SplitInternalKey(item.Entry().Key)
		if !ok {
			return 0, errInvalidInternalEntry
		}
		if cf != kv.CFWrite {
			break
		}
		parts, ok := fsmeta.InspectKey(userKey)
		if ok && parts.MountKeyID == mount.MountKeyID && parts.Kind == fsmeta.KeyKindInode && parts.Inode > maxInode {
			maxInode = parts.Inode
		}
		iter.Next()
	}
	return maxInode, nil
}
