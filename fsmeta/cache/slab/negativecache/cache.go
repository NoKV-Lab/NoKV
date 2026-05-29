// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package negativecache is the slab-backed Derived consumer that
// remembers "this key does not exist". Lookups against keys that have
// been recorded as missing return Has(true), letting the caller skip
// the expensive authoritative probe.
//
// The cache is intentionally generic — it is keyed by an opaque
// "full key" and uses a caller-supplied GroupKeyFn to extract the
// invalidation group key. When Invalidate is called for any full key, the
// per-group generation is bumped so every previously-cached entry whose group
// matches becomes stale on the next Has lookup.
//
// Persistence is optional and best-effort (Derived consistency class):
//
//   - Snapshot writes every still-fresh entry into a slab segment.
//   - Reload re-issues Remember for each restored entry, restoring
//     the in-memory cuckoo-style hash table.
//   - A corrupt or missing snapshot is silently ignored — the only
//     consequence is a re-warm window after restart.
//
// A single typed consumer owns the in-memory cache, the slab persistence, and
// the wire format.
package negativecache

import (
	"bytes"
	"sync"

	xxhash "github.com/cespare/xxhash/v2"
)

// DefaultBucketCount is the table size used when Config.BucketCount is
// zero. Power-of-two so we can mask instead of mod. ~64K buckets ×
// 2 (entries + generations) ≈ a few MB at steady state.
const DefaultBucketCount = 1 << 16

// GroupKeyFn extracts the invalidation group key from a full key. The
// returned slice need not be a copy; the cache takes its own copies
// only when it stores the key for later comparison.
type GroupKeyFn func(fullKey []byte) []byte

// Config controls Cache construction. Persistence is opt-in via Dir.
type Config struct {
	// GroupKeyFn extracts the group key from a full key. Required; an
	// identity function (return fullKey unchanged) collapses the cache
	// into a non-versioned key set.
	GroupKeyFn GroupKeyFn
	// BucketCount must be a power of two, or zero for the default. Larger
	// reduces hash collisions; smaller saves memory.
	BucketCount int
}

// Cache is a generation-tagged "key not found" cache. Safe for
// concurrent use.
type Cache struct {
	mask        uint64
	groupKeyFn  GroupKeyFn
	entries     []entryBucket
	generations []generationBucket
}

type entryBucket struct {
	mu    sync.RWMutex
	hash  uint64
	key   []byte
	gen   uint64
	valid bool
}

type generationBucket struct {
	mu    sync.RWMutex
	hash  uint64
	key   []byte
	gen   uint64
	valid bool
}

// New constructs an in-memory Cache without persistence. Callers that
// also want snapshot-on-close + restore-on-open should use Open and
// the persistence helpers in this package.
func New(cfg Config) *Cache {
	if cfg.GroupKeyFn == nil {
		cfg.GroupKeyFn = func(k []byte) []byte { return k }
	}
	bucketCount := cfg.BucketCount
	if bucketCount <= 0 {
		bucketCount = DefaultBucketCount
	}
	// Round up to next power of two if not already.
	bucketCount = roundUpPowerOf2(bucketCount)
	return &Cache{
		mask:        uint64(bucketCount - 1),
		groupKeyFn:  cfg.GroupKeyFn,
		entries:     make([]entryBucket, bucketCount),
		generations: make([]generationBucket, bucketCount),
	}
}

func roundUpPowerOf2(v int) int {
	if v <= 1 {
		return 1
	}
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v |= v >> 32
	return v + 1
}

// Has reports whether fullKey was previously recorded as missing and
// has not since been invalidated. It is safe to call concurrently.
func (c *Cache) Has(fullKey []byte) bool {
	if c == nil || len(c.entries) == 0 || len(c.generations) == 0 {
		return false
	}
	groupKey, groupHash, fullHash, ok := c.split(fullKey)
	if !ok {
		return false
	}
	gen, ok := c.generation(groupKey, groupHash)
	if !ok {
		return false
	}
	b := &c.entries[fullHash&c.mask]
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.valid && b.hash == fullHash && b.gen == gen && bytes.Equal(b.key, fullKey)
}

// Remember records fullKey as missing under the current generation for
// its group.
func (c *Cache) Remember(fullKey []byte) {
	if c == nil || len(c.entries) == 0 || len(c.generations) == 0 {
		return
	}
	groupKey, groupHash, fullHash, ok := c.split(fullKey)
	if !ok {
		return
	}
	gen := c.ensureGeneration(groupKey, groupHash)
	b := &c.entries[fullHash&c.mask]
	b.mu.Lock()
	b.hash = fullHash
	b.key = append(b.key[:0], fullKey...)
	b.gen = gen
	b.valid = true
	b.mu.Unlock()
}

// Invalidate bumps the generation for the group containing fullKey,
// rendering every previously-cached entry in that group stale.
func (c *Cache) Invalidate(fullKey []byte) {
	if c == nil || len(c.generations) == 0 {
		return
	}
	groupKey, groupHash, _, ok := c.split(fullKey)
	if !ok {
		return
	}
	b := &c.generations[groupHash&c.mask]
	b.mu.Lock()
	if !b.valid || b.hash != groupHash || !bytes.Equal(b.key, groupKey) {
		b.hash = groupHash
		b.key = append(b.key[:0], groupKey...)
		b.gen = 1
		b.valid = true
		b.mu.Unlock()
		return
	}
	b.gen++
	if b.gen == 0 {
		b.gen = 1
	}
	b.mu.Unlock()
}

// Clear drops every entry and generation. After Clear, Has returns
// false for every key until the next Remember repopulates it.
func (c *Cache) Clear() {
	if c == nil {
		return
	}
	for i := range c.entries {
		b := &c.entries[i]
		b.mu.Lock()
		b.valid = false
		b.key = b.key[:0]
		b.hash = 0
		b.gen = 0
		b.mu.Unlock()
	}
	for i := range c.generations {
		b := &c.generations[i]
		b.mu.Lock()
		b.valid = false
		b.key = b.key[:0]
		b.hash = 0
		b.gen = 0
		b.mu.Unlock()
	}
}

// SnapshotKeys returns a copy of every still-fresh full key. Stale
// entries (those whose stored generation no longer matches the current
// group generation, e.g. after an Invalidate) are dropped. Caller owns
// the returned slices and the slice header.
func (c *Cache) SnapshotKeys() [][]byte {
	if c == nil {
		return nil
	}
	out := make([][]byte, 0, len(c.entries)/8)
	for i := range c.entries {
		b := &c.entries[i]
		b.mu.RLock()
		if b.valid && len(b.key) > 0 {
			groupKey := c.groupKeyFn(b.key)
			if len(groupKey) > 0 {
				groupHash := xxhash.Sum64(groupKey)
				if gen, ok := c.generation(groupKey, groupHash); ok && gen == b.gen {
					cp := make([]byte, len(b.key))
					copy(cp, b.key)
					out = append(out, cp)
				}
			}
		}
		b.mu.RUnlock()
	}
	return out
}

// split derives the (group_key, group_hash, full_hash) triple a Has /
// Remember call needs.
func (c *Cache) split(fullKey []byte) ([]byte, uint64, uint64, bool) {
	if len(fullKey) == 0 {
		return nil, 0, 0, false
	}
	groupKey := c.groupKeyFn(fullKey)
	if len(groupKey) == 0 {
		return nil, 0, 0, false
	}
	return groupKey, xxhash.Sum64(groupKey), xxhash.Sum64(fullKey), true
}

func (c *Cache) generation(groupKey []byte, groupHash uint64) (uint64, bool) {
	b := &c.generations[groupHash&c.mask]
	b.mu.RLock()
	defer b.mu.RUnlock()
	if !b.valid || b.hash != groupHash || !bytes.Equal(b.key, groupKey) {
		return 0, false
	}
	return b.gen, true
}

func (c *Cache) ensureGeneration(groupKey []byte, groupHash uint64) uint64 {
	b := &c.generations[groupHash&c.mask]
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.valid || b.hash != groupHash || !bytes.Equal(b.key, groupKey) {
		b.hash = groupHash
		b.key = append(b.key[:0], groupKey...)
		b.gen = 1
		b.valid = true
		return b.gen
	}
	if b.gen == 0 {
		b.gen = 1
	}
	return b.gen
}
