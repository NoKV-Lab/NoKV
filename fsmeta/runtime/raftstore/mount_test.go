// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"context"
	"testing"
	"time"

	fsmetawatch "github.com/feichai0017/NoKV/fsmeta/exec/watch"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/feichai0017/NoKV/fsmeta/observe"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"github.com/stretchr/testify/require"
)

type fakeMountLookup struct {
	calls int
	resp  *coordpb.GetMountResponse
	err   error
}

func (c *fakeMountLookup) GetMount(context.Context, *coordpb.GetMountRequest) (*coordpb.GetMountResponse, error) {
	c.calls++
	return c.resp, c.err
}

func TestMountCacheReturnsActiveMount(t *testing.T) {
	lookup := &fakeMountLookup{
		resp: &coordpb.GetMountResponse{Mount: &coordpb.MountInfo{
			MountId:       "vol",
			MountKeyId:    1,
			RootInode:     1,
			SchemaVersion: 1,
			State:         coordpb.MountState_MOUNT_STATE_ACTIVE,
		}},
	}
	now := time.Unix(100, 0)
	cache := &mountCache{
		coord: lookup,
		ttl:   time.Minute,
		now:   func() time.Time { return now },
	}

	first, err := cache.ResolveMount(context.Background(), model.MountID("vol"))
	require.NoError(t, err)
	second, err := cache.ResolveMount(context.Background(), model.MountID("vol"))
	require.NoError(t, err)

	require.Equal(t, 1, lookup.calls)
	require.Equal(t, first, second)
	require.Equal(t, model.MountID("vol"), first.MountID)
	require.False(t, first.Retired)
}

func TestMountCacheRefreshesAfterTTL(t *testing.T) {
	lookup := &fakeMountLookup{
		resp: &coordpb.GetMountResponse{Mount: &coordpb.MountInfo{
			MountId:       "vol",
			MountKeyId:    1,
			RootInode:     1,
			SchemaVersion: 1,
			State:         coordpb.MountState_MOUNT_STATE_ACTIVE,
		}},
	}
	now := time.Unix(100, 0)
	cache := &mountCache{
		coord: lookup,
		ttl:   time.Second,
		now:   func() time.Time { return now },
	}

	first, err := cache.ResolveMount(context.Background(), model.MountID("vol"))
	require.NoError(t, err)
	require.Equal(t, model.InodeID(1), first.RootInode)

	now = now.Add(2 * time.Second)
	lookup.resp = &coordpb.GetMountResponse{Mount: &coordpb.MountInfo{
		MountId:       "vol",
		MountKeyId:    1,
		RootInode:     9,
		SchemaVersion: 2,
		State:         coordpb.MountState_MOUNT_STATE_RETIRED,
	}}
	second, err := cache.ResolveMount(context.Background(), model.MountID("vol"))
	require.NoError(t, err)

	require.Equal(t, 2, lookup.calls)
	require.Equal(t, model.InodeID(9), second.RootInode)
	require.True(t, second.Retired)
}

func TestMountCacheDoesNotCacheNotFound(t *testing.T) {
	lookup := &fakeMountLookup{resp: &coordpb.GetMountResponse{NotFound: true}}
	now := time.Unix(100, 0)
	cache := &mountCache{
		coord: lookup,
		ttl:   time.Minute,
		now:   func() time.Time { return now },
	}

	_, err := cache.ResolveMount(context.Background(), model.MountID("missing"))
	require.ErrorIs(t, err, model.ErrMountNotRegistered)
	_, err = cache.ResolveMount(context.Background(), model.MountID("missing"))
	require.ErrorIs(t, err, model.ErrMountNotRegistered)
	require.Equal(t, 2, lookup.calls)
	require.Equal(t, map[string]any{
		"cache_hits_total":        uint64(0),
		"cache_misses_total":      uint64(2),
		"admission_rejects_total": uint64(2),
	}, cache.Stats())
}

func TestWatcherRejectsRetiredMount(t *testing.T) {
	lookup := &fakeMountLookup{
		resp: &coordpb.GetMountResponse{Mount: &coordpb.MountInfo{
			MountId:       "vol",
			MountKeyId:    1,
			RootInode:     1,
			SchemaVersion: 1,
			State:         coordpb.MountState_MOUNT_STATE_RETIRED,
		}},
	}
	cache := &mountCache{coord: lookup, ttl: time.Minute}
	w := watcher{Router: fsmetawatch.NewRouter(), mounts: cache}

	_, err := w.Subscribe(context.Background(), observe.WatchRequest{
		Mount:     "vol",
		RootInode: model.RootInode,
	})
	require.ErrorIs(t, err, model.ErrMountRetired)
	require.Equal(t, 1, lookup.calls)
}

func TestWatcherStatsIncludesRouterSnapshot(t *testing.T) {
	router := fsmetawatch.NewRouter()
	w := watcher{Router: router}

	stats := w.Stats()
	require.Equal(t, 0, stats["subscribers"])
	require.Equal(t, uint64(0), stats["events_total"])

	empty := watcher{}
	require.Empty(t, empty.Stats())
}
