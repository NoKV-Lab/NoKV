// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	fsmetaexec "github.com/feichai0017/NoKV/fsmeta/exec"
	"github.com/feichai0017/NoKV/fsmeta/model"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
)

const defaultMountTTL = time.Second

type mountLookup interface {
	GetMount(context.Context, *coordpb.GetMountRequest) (*coordpb.GetMountResponse, error)
}

// mountCache implements MountResolver against the coordinator GetMount RPC
// with a small TTL cache. Retire events bypass the TTL via markRetired so
// admission flips immediately.
type mountCache struct {
	coord mountLookup
	ttl   time.Duration
	now   func() time.Time

	mu      sync.Mutex
	entries map[model.MountID]mountEntry

	cacheHitsTotal        atomic.Uint64
	cacheMissesTotal      atomic.Uint64
	admissionRejectsTotal atomic.Uint64
}

type mountEntry struct {
	record    fsmetaexec.MountAdmission
	err       error
	expiresAt time.Time
}

func (c *mountCache) ResolveMount(ctx context.Context, mount model.MountID) (fsmetaexec.MountAdmission, error) {
	if c.coord == nil {
		return fsmetaexec.MountAdmission{}, errMountCacheNotConfigured
	}
	now := c.clock()
	if record, err, ok := c.lookup(mount, now); ok {
		c.cacheHitsTotal.Add(1)
		c.countAdmissionReject(record, err)
		return record, err
	}
	c.cacheMissesTotal.Add(1)
	resp, err := c.coord.GetMount(ctx, &coordpb.GetMountRequest{MountId: string(mount)})
	if err != nil {
		return fsmetaexec.MountAdmission{}, err
	}
	record, err := mountFromProto(resp)
	if err == nil {
		c.put(mount, now, record, nil)
	}
	c.countAdmissionReject(record, err)
	return record, err
}

// SameAuthority reflects the current fsmeta deployment model: a registered
// mount owns one rooted namespace authority. The executor calls this before
// ordinary Rename so a future nested-authority resolver can reject
// cross-authority moves without changing the data-plane rename path.
func (c *mountCache) SameAuthority(ctx context.Context, mount model.MountID, fromParent, toParent model.InodeID) (bool, error) {
	if fromParent == 0 || toParent == 0 {
		return false, model.ErrInvalidInodeID
	}
	if _, err := c.ResolveMount(ctx, mount); err != nil {
		return false, err
	}
	return true, nil
}

// markRetired forces the cached view of mount to retired. The monitor calls
// this when it observes a rooted MountRetired event so admission flips before
// the cached TTL expires.
func (c *mountCache) markRetired(mount model.MountID) {
	if mount == "" {
		return
	}
	c.put(mount, c.clock(), fsmetaexec.MountAdmission{MountID: mount, Retired: true}, nil)
}

func (c *mountCache) clock() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

func (c *mountCache) lookup(mount model.MountID, now time.Time) (fsmetaexec.MountAdmission, error, bool) {
	if c.ttl <= 0 {
		return fsmetaexec.MountAdmission{}, nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[mount]
	if !ok || !now.Before(entry.expiresAt) {
		return fsmetaexec.MountAdmission{}, nil, false
	}
	return entry.record, entry.err, true
}

func (c *mountCache) put(mount model.MountID, now time.Time, record fsmetaexec.MountAdmission, err error) {
	if c.ttl <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = make(map[model.MountID]mountEntry)
	}
	c.entries[mount] = mountEntry{record: record, err: err, expiresAt: now.Add(c.ttl)}
}

func (c *mountCache) countAdmissionReject(record fsmetaexec.MountAdmission, err error) {
	if c == nil {
		return
	}
	if err != nil || record.MountID == "" || record.Retired {
		c.admissionRejectsTotal.Add(1)
	}
}

// Stats returns mount-admission counters suitable for expvar export.
func (c *mountCache) Stats() map[string]any {
	if c == nil {
		return map[string]any{}
	}
	return map[string]any{
		"cache_hits_total":        c.cacheHitsTotal.Load(),
		"cache_misses_total":      c.cacheMissesTotal.Load(),
		"admission_rejects_total": c.admissionRejectsTotal.Load(),
	}
}

func mountFromProto(resp *coordpb.GetMountResponse) (fsmetaexec.MountAdmission, error) {
	if resp == nil || resp.GetNotFound() {
		return fsmetaexec.MountAdmission{}, model.ErrMountNotRegistered
	}
	info := resp.GetMount()
	if info == nil {
		return fsmetaexec.MountAdmission{}, model.ErrMountNotRegistered
	}
	if info.GetMountKeyId() == 0 {
		return fsmetaexec.MountAdmission{}, model.ErrMountNotRegistered
	}
	return fsmetaexec.MountAdmission{
		MountID:       model.MountID(info.GetMountId()),
		MountKeyID:    model.MountKeyID(info.GetMountKeyId()),
		RootInode:     model.InodeID(info.GetRootInode()),
		SchemaVersion: info.GetSchemaVersion(),
		Retired:       info.GetState() == coordpb.MountState_MOUNT_STATE_RETIRED,
	}, nil
}
