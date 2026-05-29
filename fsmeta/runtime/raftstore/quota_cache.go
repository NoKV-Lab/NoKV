// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/fsmeta/backend"
	fsmetaexec "github.com/feichai0017/NoKV/fsmeta/exec"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
)

const defaultQuotaTTL = time.Second

type quotaLookup interface {
	GetQuotaFence(context.Context, *coordpb.GetQuotaFenceRequest) (*coordpb.GetQuotaFenceResponse, error)
}

type quotaCache struct {
	coord quotaLookup
	ttl   time.Duration
	now   func() time.Time

	mu     sync.RWMutex
	fences map[quotaSubject]quotaEntry
	misses map[quotaSubject]time.Time

	checksTotal         atomic.Uint64
	rejectsTotal        atomic.Uint64
	cacheHitsTotal      atomic.Uint64
	cacheMissesTotal    atomic.Uint64
	fenceUpdatesTotal   atomic.Uint64
	usageMutationsTotal atomic.Uint64
}

type quotaSubject struct {
	mount      model.MountID
	mountKeyID model.MountKeyID
	scope      model.InodeID
}

type quotaEntry struct {
	fence     quotaFence
	expiresAt time.Time
}

type quotaFence struct {
	limitBytes  uint64
	limitInodes uint64
	era         uint64
}

type quotaDelta struct {
	bytes  int64
	inodes int64
}

// ReserveQuota returns usage-counter mutations for the provided quota deltas.
// The returned mutations are intentionally plain Put/Delete mutations; the
// Percolator conflict check on the shared usage keys serializes concurrent
// writers across fsmeta gateway processes.
func (c *quotaCache) ReserveQuota(ctx context.Context, runner backend.Store, changes []fsmetaexec.QuotaChange, startVersion uint64) ([]*backend.Mutation, error) {
	if c == nil || c.coord == nil || runner == nil || len(changes) == 0 {
		return nil, nil
	}
	deltas, err := c.aggregate(changes)
	if err != nil {
		return nil, err
	}
	mutations := make([]*backend.Mutation, 0, len(deltas))
	for subject, delta := range deltas {
		mut, err := c.reserveSubject(ctx, runner, subject, delta, startVersion)
		if err != nil {
			return nil, err
		}
		if mut != nil {
			mutations = append(mutations, mut)
		}
	}
	return mutations, nil
}

func (c *quotaCache) AllowVisibleQuota(ctx context.Context, changes []fsmetaexec.QuotaChange) (bool, error) {
	if c == nil || c.coord == nil || len(changes) == 0 {
		return true, nil
	}
	for _, change := range changes {
		if change.Mount == "" || change.MountKeyID == 0 {
			return false, model.ErrInvalidMountID
		}
		if change.Bytes == 0 && change.Inodes == 0 {
			continue
		}
		ok, err := c.allowVisibleQuotaSubject(ctx, quotaSubject{mount: change.Mount, mountKeyID: change.MountKeyID})
		if err != nil || !ok {
			return ok, err
		}
		if change.Scope != 0 {
			ok, err = c.allowVisibleQuotaSubject(ctx, quotaSubject{mount: change.Mount, mountKeyID: change.MountKeyID, scope: change.Scope})
			if err != nil || !ok {
				return ok, err
			}
		}
	}
	return true, nil
}

func (c *quotaCache) allowVisibleQuotaSubject(ctx context.Context, subject quotaSubject) (bool, error) {
	_, ok, err := c.resolve(ctx, subject)
	if err != nil {
		return false, err
	}
	return !ok, nil
}

func (c *quotaCache) aggregate(changes []fsmetaexec.QuotaChange) (map[quotaSubject]quotaDelta, error) {
	out := make(map[quotaSubject]quotaDelta)
	for _, change := range changes {
		if change.Mount == "" || change.MountKeyID == 0 {
			return nil, model.ErrInvalidMountID
		}
		addDelta(out, quotaSubject{mount: change.Mount, mountKeyID: change.MountKeyID}, change.Bytes, change.Inodes)
		if change.Scope != 0 {
			addDelta(out, quotaSubject{mount: change.Mount, mountKeyID: change.MountKeyID, scope: change.Scope}, change.Bytes, change.Inodes)
		}
	}
	for subject, delta := range out {
		if delta.bytes == 0 && delta.inodes == 0 {
			delete(out, subject)
		}
	}
	return out, nil
}

func addDelta(out map[quotaSubject]quotaDelta, subject quotaSubject, bytesDelta, inodesDelta int64) {
	delta := out[subject]
	delta.bytes = saturatingAddInt64(delta.bytes, bytesDelta)
	delta.inodes = saturatingAddInt64(delta.inodes, inodesDelta)
	out[subject] = delta
}

func (c *quotaCache) reserveSubject(ctx context.Context, runner backend.Store, subject quotaSubject, delta quotaDelta, startVersion uint64) (*backend.Mutation, error) {
	c.checksTotal.Add(1)
	fence, ok, err := c.resolve(ctx, subject)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	key, err := layout.EncodeUsageKey(model.MountIdentity{MountID: subject.mount, MountKeyID: subject.mountKeyID}, subject.scope)
	if err != nil {
		return nil, err
	}
	current, err := readUsage(ctx, runner, key, startVersion)
	if err != nil {
		return nil, err
	}
	next := applyUsageDelta(current, delta)
	if delta.bytes > 0 && fence.limitBytes > 0 && next.Bytes > fence.limitBytes {
		c.rejectsTotal.Add(1)
		return nil, fmt.Errorf("%w: mount=%s scope=%d bytes=%d add=%d limit=%d era=%d",
			model.ErrQuotaExceeded, subject.mount, subject.scope, current.Bytes, delta.bytes, fence.limitBytes, fence.era)
	}
	if delta.inodes > 0 && fence.limitInodes > 0 && next.Inodes > fence.limitInodes {
		c.rejectsTotal.Add(1)
		return nil, fmt.Errorf("%w: mount=%s scope=%d inodes=%d add=%d limit=%d era=%d",
			model.ErrQuotaExceeded, subject.mount, subject.scope, current.Inodes, delta.inodes, fence.limitInodes, fence.era)
	}
	c.usageMutationsTotal.Add(1)
	if next.Bytes == 0 && next.Inodes == 0 {
		return &backend.Mutation{Op: backend.MutationDelete, Key: key}, nil
	}
	value, err := layout.EncodeUsageValue(next)
	if err != nil {
		return nil, err
	}
	return &backend.Mutation{Op: backend.MutationPut, Key: key, Value: value}, nil
}

func readUsage(ctx context.Context, runner backend.Store, key []byte, version uint64) (model.UsageRecord, error) {
	value, ok, err := runner.Get(ctx, key, version)
	if err != nil {
		return model.UsageRecord{}, err
	}
	if !ok {
		return model.UsageRecord{}, nil
	}
	return layout.DecodeUsageValue(value)
}

func applyUsageDelta(current model.UsageRecord, delta quotaDelta) model.UsageRecord {
	return model.UsageRecord{
		Bytes:  applyDelta(current.Bytes, delta.bytes),
		Inodes: applyDelta(current.Inodes, delta.inodes),
	}
}

func (c *quotaCache) resolve(ctx context.Context, subject quotaSubject) (quotaFence, bool, error) {
	now := c.clock()
	if fence, ok, found := c.lookup(subject, now); found {
		c.cacheHitsTotal.Add(1)
		return fence, ok, nil
	}
	c.cacheMissesTotal.Add(1)
	resp, err := c.coord.GetQuotaFence(ctx, &coordpb.GetQuotaFenceRequest{
		Subject: &coordpb.QuotaSubject{
			MountId:     string(subject.mount),
			SubtreeRoot: uint64(subject.scope),
		},
	})
	if err != nil {
		return quotaFence{}, false, err
	}
	if resp == nil || resp.GetNotFound() || resp.GetFence() == nil {
		c.putMiss(subject, now)
		return quotaFence{}, false, nil
	}
	fence := quotaFenceFromProto(resp.GetFence())
	c.putFence(subject, now, fence)
	return fence, true, nil
}

func (c *quotaCache) lookup(subject quotaSubject, now time.Time) (quotaFence, bool, bool) {
	if c.ttl <= 0 {
		return quotaFence{}, false, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if entry, ok := c.fences[subject]; ok && now.Before(entry.expiresAt) {
		return entry.fence, true, true
	}
	if expiresAt, ok := c.misses[subject]; ok && now.Before(expiresAt) {
		return quotaFence{}, false, true
	}
	return quotaFence{}, false, false
}

func (c *quotaCache) markFenceUpdated(info *coordpb.QuotaFenceInfo) {
	if c == nil || info == nil || info.GetSubject() == nil || info.GetSubject().GetMountId() == "" {
		return
	}
	// QuotaFenceInfo currently carries the public mount id but not mount_key_id.
	// ReserveQuota keys the cache by both identities, so caching this update here
	// would create an entry that no reserve path can safely hit.
}

func (c *quotaCache) purgeMount(mount model.MountID) {
	if c == nil || mount == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for subject := range c.fences {
		if subject.mount == mount {
			delete(c.fences, subject)
		}
	}
	for subject := range c.misses {
		if subject.mount == mount {
			delete(c.misses, subject)
		}
	}
}

func (c *quotaCache) putFence(subject quotaSubject, now time.Time, fence quotaFence) {
	if c.ttl <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fences == nil {
		c.fences = make(map[quotaSubject]quotaEntry)
	}
	if c.misses != nil {
		delete(c.misses, subject)
	}
	c.fences[subject] = quotaEntry{fence: fence, expiresAt: now.Add(c.ttl)}
}

func (c *quotaCache) putMiss(subject quotaSubject, now time.Time) {
	if c.ttl <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.misses == nil {
		c.misses = make(map[quotaSubject]time.Time)
	}
	if c.fences != nil {
		delete(c.fences, subject)
	}
	c.misses[subject] = now.Add(c.ttl)
}

func (c *quotaCache) Stats() map[string]any {
	if c == nil {
		return map[string]any{}
	}
	return map[string]any{
		"checks_total":          c.checksTotal.Load(),
		"rejects_total":         c.rejectsTotal.Load(),
		"cache_hits_total":      c.cacheHitsTotal.Load(),
		"cache_misses_total":    c.cacheMissesTotal.Load(),
		"fence_updates_total":   c.fenceUpdatesTotal.Load(),
		"usage_mutations_total": c.usageMutationsTotal.Load(),
	}
}

func (c *quotaCache) clock() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

func quotaFenceFromProto(info *coordpb.QuotaFenceInfo) quotaFence {
	if info == nil {
		return quotaFence{}
	}
	return quotaFence{limitBytes: info.GetLimitBytes(), limitInodes: info.GetLimitInodes(), era: info.GetEra()}
}

func saturatingAddInt64(current, delta int64) int64 {
	if delta > 0 && current > math.MaxInt64-delta {
		return math.MaxInt64
	}
	if delta < 0 && current < math.MinInt64-delta {
		return math.MinInt64
	}
	return current + delta
}

func applyDelta(current uint64, delta int64) uint64 {
	if delta < 0 {
		sub := uint64(-delta)
		if sub >= current {
			return 0
		}
		return current - sub
	}
	add := uint64(delta)
	if add > math.MaxUint64-current {
		return math.MaxUint64
	}
	return current + add
}
