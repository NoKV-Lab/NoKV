// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"container/list"
	"sync"
	"time"

	"github.com/feichai0017/NoKV/fsmeta/model"
)

const (
	defaultLookupCacheEntries = 4096
	defaultLookupCacheTTL     = time.Second
)

// LookupCacheConfig controls the optional client-side positive dentry cache.
//
// The cache is deliberately bounded and TTL-based. It is an optimization for
// hot path resolution, not an authority source: callers that need externally
// synchronized freshness should disable it or pair it with watch-based
// invalidation at a higher layer.
type LookupCacheConfig struct {
	MaxEntries int
	TTL        time.Duration
}

// LookupCacheStats is a low-cardinality snapshot of client cache behavior.
type LookupCacheStats struct {
	Hits          uint64
	Misses        uint64
	Inserts       uint64
	Invalidations uint64
	Evictions     uint64
	Expired       uint64
}

// LookupCache is a pure positive dentry cache. It knows nothing about RPC,
// retries, or fsmeta mutation semantics; the client assembly layer decides
// when to consult, fill, or invalidate it.
type LookupCache struct {
	mu         sync.Mutex
	maxEntries int
	ttl        time.Duration
	now        func() time.Time
	items      map[lookupCacheKey]*list.Element
	lru        *list.List
	stats      LookupCacheStats
}

// NewLookupCache builds a bounded positive dentry cache. A zero config uses
// conservative defaults; negative capacity or TTL values are rejected.
func NewLookupCache(cfg LookupCacheConfig) (*LookupCache, error) {
	normalized, err := normalizeLookupCacheConfig(cfg)
	if err != nil {
		return nil, err
	}
	return newLookupCache(normalized), nil
}

func normalizeLookupCacheConfig(cfg LookupCacheConfig) (LookupCacheConfig, error) {
	if cfg.MaxEntries < 0 || cfg.TTL < 0 {
		return LookupCacheConfig{}, errLookupCacheInvalidConfig
	}
	if cfg.MaxEntries == 0 {
		cfg.MaxEntries = defaultLookupCacheEntries
	}
	if cfg.TTL == 0 {
		cfg.TTL = defaultLookupCacheTTL
	}
	return cfg, nil
}

type lookupCacheKey struct {
	mount  model.MountID
	parent model.InodeID
	name   string
}

type lookupCacheEntry struct {
	key       lookupCacheKey
	record    model.DentryRecord
	expiresAt time.Time
}

func newLookupCache(cfg LookupCacheConfig) *LookupCache {
	return &LookupCache{
		maxEntries: cfg.MaxEntries,
		ttl:        cfg.TTL,
		now:        time.Now,
		items:      make(map[lookupCacheKey]*list.Element, cfg.MaxEntries),
		lru:        list.New(),
	}
}

func (c *LookupCache) Stats() LookupCacheStats {
	if c == nil {
		return LookupCacheStats{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stats
}

// Get returns a fresh cached dentry for mount/parent/name when present.
func (c *LookupCache) Get(mount model.MountID, parent model.InodeID, name string) (model.DentryRecord, bool) {
	return c.get(lookupCacheKey{mount: mount, parent: parent, name: name})
}

func (c *LookupCache) get(key lookupCacheKey) (model.DentryRecord, bool) {
	if c == nil {
		return model.DentryRecord{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	elem := c.items[key]
	if elem == nil {
		c.stats.Misses++
		return model.DentryRecord{}, false
	}
	entry := elem.Value.(*lookupCacheEntry)
	if !entry.expiresAt.After(c.now()) {
		c.removeElement(elem)
		c.stats.Expired++
		c.stats.Misses++
		return model.DentryRecord{}, false
	}
	c.lru.MoveToFront(elem)
	c.stats.Hits++
	return entry.record, true
}

func (c *LookupCache) peek(key lookupCacheKey) (model.DentryRecord, bool) {
	if c == nil {
		return model.DentryRecord{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	elem := c.items[key]
	if elem == nil {
		return model.DentryRecord{}, false
	}
	entry := elem.Value.(*lookupCacheEntry)
	if !entry.expiresAt.After(c.now()) {
		c.removeElement(elem)
		c.stats.Expired++
		return model.DentryRecord{}, false
	}
	return entry.record, true
}

// Put stores one dentry record using the supplied mount identity.
func (c *LookupCache) Put(mount model.MountID, record model.DentryRecord) {
	if c == nil || c.maxEntries == 0 {
		return
	}
	key := lookupCacheKey{mount: mount, parent: record.Parent, name: record.Name}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.putLocked(key, record)
}

// PutMany stores a batch of dentry records for the same mount.
func (c *LookupCache) PutMany(mount model.MountID, records []model.DentryRecord) {
	for _, record := range records {
		c.Put(mount, record)
	}
}

// Invalidate drops the exact mount/parent/name cache entry.
func (c *LookupCache) Invalidate(mount model.MountID, parent model.InodeID, name string) {
	c.invalidate(lookupCacheKey{mount: mount, parent: parent, name: name})
}

func (c *LookupCache) invalidate(key lookupCacheKey) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem := c.items[key]; elem != nil {
		c.removeElement(elem)
		c.stats.Invalidations++
	}
}

// Clear drops all cached entries and leaves cumulative counters intact.
func (c *LookupCache) Clear() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items = make(map[lookupCacheKey]*list.Element, c.maxEntries)
	c.lru.Init()
}

func (c *LookupCache) putLocked(key lookupCacheKey, record model.DentryRecord) {
	if elem := c.items[key]; elem != nil {
		entry := elem.Value.(*lookupCacheEntry)
		entry.record = record
		entry.expiresAt = c.now().Add(c.ttl)
		c.lru.MoveToFront(elem)
		c.stats.Inserts++
		return
	}
	entry := &lookupCacheEntry{
		key:       key,
		record:    record,
		expiresAt: c.now().Add(c.ttl),
	}
	c.items[key] = c.lru.PushFront(entry)
	c.stats.Inserts++
	for len(c.items) > c.maxEntries {
		back := c.lru.Back()
		if back == nil {
			break
		}
		c.removeElement(back)
		c.stats.Evictions++
	}
}

func (c *LookupCache) removeElement(elem *list.Element) {
	c.lru.Remove(elem)
	entry := elem.Value.(*lookupCacheEntry)
	delete(c.items, entry.key)
}
