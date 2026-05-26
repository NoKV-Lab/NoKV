// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"testing"
	"time"

	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/stretchr/testify/require"
)

func TestLookupCacheHit(t *testing.T) {
	cache, err := NewLookupCache(LookupCacheConfig{MaxEntries: 8, TTL: time.Minute})
	require.NoError(t, err)

	record := model.DentryRecord{Parent: 7, Name: "run", Inode: 42, Type: model.InodeTypeDirectory}
	cache.Put("vol", record)
	got, ok := cache.Get("vol", 7, "run")

	require.True(t, ok)
	require.Equal(t, record, got)
	require.Equal(t, LookupCacheStats{Hits: 1, Inserts: 1}, cache.Stats())
}

func TestLookupCacheMissAndExpire(t *testing.T) {
	cache, err := NewLookupCache(LookupCacheConfig{MaxEntries: 8, TTL: time.Nanosecond})
	require.NoError(t, err)

	_, ok := cache.Get("vol", 7, "missing")
	require.False(t, ok)

	cache.Put("vol", model.DentryRecord{Parent: 7, Name: "run", Inode: 42, Type: model.InodeTypeDirectory})
	time.Sleep(time.Millisecond)
	_, ok = cache.Get("vol", 7, "run")
	require.False(t, ok)

	stats := cache.Stats()
	require.Equal(t, uint64(2), stats.Misses)
	require.Equal(t, uint64(1), stats.Expired)
}

func TestLookupCachePutManyAndInvalidate(t *testing.T) {
	cache, err := NewLookupCache(LookupCacheConfig{MaxEntries: 8, TTL: time.Minute})
	require.NoError(t, err)

	cache.PutMany("vol", []model.DentryRecord{
		{Parent: 7, Name: "run-a", Inode: 42, Type: model.InodeTypeDirectory},
		{Parent: 7, Name: "run-b", Inode: 43, Type: model.InodeTypeDirectory},
	})
	_, ok := cache.Get("vol", 7, "run-a")
	require.True(t, ok)

	cache.Invalidate("vol", 7, "run-a")
	_, ok = cache.Get("vol", 7, "run-a")
	require.False(t, ok)

	stats := cache.Stats()
	require.Equal(t, uint64(2), stats.Inserts)
	require.Equal(t, uint64(1), stats.Invalidations)
}

func TestLookupCacheEvictsLRU(t *testing.T) {
	cache, err := NewLookupCache(LookupCacheConfig{MaxEntries: 1, TTL: time.Minute})
	require.NoError(t, err)

	cache.Put("vol", model.DentryRecord{Parent: 7, Name: "a", Inode: 42, Type: model.InodeTypeFile})
	cache.Put("vol", model.DentryRecord{Parent: 7, Name: "b", Inode: 43, Type: model.InodeTypeFile})

	_, ok := cache.Get("vol", 7, "a")
	require.False(t, ok)
	_, ok = cache.Get("vol", 7, "b")
	require.True(t, ok)
	require.Equal(t, uint64(1), cache.Stats().Evictions)
}

func TestLookupCacheRejectsInvalidConfig(t *testing.T) {
	_, err := NewLookupCache(LookupCacheConfig{MaxEntries: -1})
	require.ErrorIs(t, err, errLookupCacheInvalidConfig)

	_, err = NewLookupCache(LookupCacheConfig{TTL: -time.Second})
	require.ErrorIs(t, err, errLookupCacheInvalidConfig)
}
