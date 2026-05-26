// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package watch

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/stretchr/testify/require"
)

func TestRouterFiltersByPrefixAndAcksBudget(t *testing.T) {
	router := NewRouter()
	prefix := []byte("fsm\x00prefix/")
	sub, err := router.Subscribe(context.Background(), fsmeta.WatchRequest{
		KeyPrefix:          prefix,
		BackPressureWindow: 1,
	})
	require.NoError(t, err)
	defer sub.Close()

	router.Publish(fsmeta.WatchEvent{
		Cursor:        fsmeta.WatchCursor{RegionID: 1, Term: 2, Index: 3},
		CommitVersion: 10,
		Source:        fsmeta.WatchEventSourceCommit,
		Key:           []byte("other/key"),
	})
	select {
	case <-sub.Events():
		t.Fatal("unexpected event for non-matching prefix")
	default:
	}

	want := fsmeta.WatchEvent{
		Cursor:        fsmeta.WatchCursor{RegionID: 1, Term: 2, Index: 4},
		CommitVersion: 11,
		Source:        fsmeta.WatchEventSourceCommit,
		Key:           []byte("fsm\x00prefix/a"),
	}
	router.Publish(want)
	require.Equal(t, want, <-sub.Events())
	sub.Ack(want.Cursor)

	next := want
	next.Cursor.Index = 5
	next.CommitVersion = 12
	next.Key = []byte("fsm\x00prefix/b")
	router.Publish(next)
	require.Equal(t, next, <-sub.Events())
	require.NoError(t, sub.Err())
}

func TestRouterClosesSlowSubscriberOnOverflow(t *testing.T) {
	router := NewRouter()
	sub, err := router.Subscribe(context.Background(), fsmeta.WatchRequest{
		KeyPrefix:          []byte("k/"),
		BackPressureWindow: 1,
	})
	require.NoError(t, err)

	router.Publish(fsmeta.WatchEvent{Cursor: fsmeta.WatchCursor{RegionID: 1, Term: 1, Index: 1}, Key: []byte("k/a")})
	router.Publish(fsmeta.WatchEvent{Cursor: fsmeta.WatchCursor{RegionID: 1, Term: 1, Index: 2}, Key: []byte("k/b")})

	require.Eventually(t, func() bool {
		return errors.Is(sub.Err(), model.ErrWatchOverflow)
	}, time.Second, 10*time.Millisecond)
	require.GreaterOrEqual(t, router.Dropped(), uint64(1))
}

func TestRouterConsumesApplyEvents(t *testing.T) {
	router := NewRouter()
	sub, err := router.Subscribe(context.Background(), fsmeta.WatchRequest{
		KeyPrefix: []byte("dentry/"),
	})
	require.NoError(t, err)
	defer sub.Close()

	router.OnApply(fsmeta.ApplyEvent{
		RegionID:      7,
		Term:          3,
		Index:         9,
		Source:        fsmeta.WatchEventSourceResolveLock,
		CommitVersion: 44,
		Keys:          [][]byte{[]byte("dentry/a"), []byte("inode/1")},
	})

	got := <-sub.Events()
	require.Equal(t, fsmeta.WatchEvent{
		Cursor:        fsmeta.WatchCursor{RegionID: 7, Term: 3, Index: 9},
		CommitVersion: 44,
		Source:        fsmeta.WatchEventSourceResolveLock,
		Key:           []byte("dentry/a"),
	}, got)
}

func TestRouterDeduplicatesReplicatedApplyEvents(t *testing.T) {
	router := NewRouter()
	sub, err := router.Subscribe(context.Background(), fsmeta.WatchRequest{
		KeyPrefix: []byte("k/"),
	})
	require.NoError(t, err)
	defer sub.Close()

	evt := fsmeta.WatchEvent{
		Cursor:        fsmeta.WatchCursor{RegionID: 1, Term: 2, Index: 3},
		CommitVersion: 4,
		Source:        fsmeta.WatchEventSourceCommit,
		Key:           []byte("k/a"),
	}
	router.Publish(evt)
	router.Publish(evt)

	require.Equal(t, evt, <-sub.Events())
	select {
	case got := <-sub.Events():
		t.Fatalf("unexpected duplicate event: %+v", got)
	default:
	}
}

func TestRouterPublishesPerasVisibleEventsLiveOnly(t *testing.T) {
	router := NewRouter()
	sub, err := router.Subscribe(context.Background(), fsmeta.WatchRequest{
		KeyPrefix:          []byte("k/"),
		BackPressureWindow: 1,
	})
	require.NoError(t, err)
	defer sub.Close()

	visible := fsmeta.WatchEvent{
		Cursor: fsmeta.WatchCursor{
			Term:  9,
			Index: 1,
		},
		Source: fsmeta.WatchEventSourceRuntimeVisible,
		Key:    []byte("k/a"),
	}
	router.Publish(visible)
	require.Equal(t, visible, <-sub.Events())

	next := visible
	next.Cursor.Index = 2
	next.Key = []byte("k/b")
	router.Publish(next)
	require.Equal(t, next, <-sub.Events())
	require.NoError(t, sub.Err())

	stats := router.Stats()
	require.Equal(t, 0, stats["regions"])
	require.Equal(t, 0, stats["recent_events"])

	replay, err := router.Subscribe(context.Background(), fsmeta.WatchRequest{
		KeyPrefix:    []byte("k/"),
		ResumeCursor: fsmeta.WatchCursor{RegionID: 1, Term: 1, Index: 1},
	})
	require.ErrorIs(t, err, model.ErrWatchCursorExpired)
	require.Nil(t, replay)
}

func TestRouterStatsTracksPublishedAndSubscribers(t *testing.T) {
	router := NewRouter()
	require.Equal(t, map[string]any{
		"subscribers":     0,
		"regions":         0,
		"recent_events":   0,
		"events_total":    uint64(0),
		"delivered_total": uint64(0),
		"dropped_total":   uint64(0),
		"overflow_total":  uint64(0),
	}, router.Stats())

	sub, err := router.Subscribe(context.Background(), fsmeta.WatchRequest{KeyPrefix: []byte("k/")})
	require.NoError(t, err)
	defer sub.Close()
	router.Publish(fsmeta.WatchEvent{
		Cursor:        fsmeta.WatchCursor{RegionID: 1, Term: 1, Index: 1},
		CommitVersion: 10,
		Source:        fsmeta.WatchEventSourceCommit,
		Key:           []byte("k/a"),
	})

	stats := router.Stats()
	require.Equal(t, 1, stats["subscribers"])
	require.Equal(t, 1, stats["regions"])
	require.Equal(t, 1, stats["recent_events"])
	require.Equal(t, uint64(1), stats["events_total"])
	require.Equal(t, uint64(1), stats["delivered_total"])
}

func TestRouterReplaysEventsAfterResumeCursor(t *testing.T) {
	router := NewRouter()
	prefix := []byte("k/")
	first := fsmeta.WatchEvent{
		Cursor:        fsmeta.WatchCursor{RegionID: 1, Term: 1, Index: 1},
		CommitVersion: 10,
		Source:        fsmeta.WatchEventSourceCommit,
		Key:           []byte("k/a"),
	}
	second := fsmeta.WatchEvent{
		Cursor:        fsmeta.WatchCursor{RegionID: 1, Term: 1, Index: 2},
		CommitVersion: 11,
		Source:        fsmeta.WatchEventSourceCommit,
		Key:           []byte("k/b"),
	}
	thirdOtherPrefix := fsmeta.WatchEvent{
		Cursor:        fsmeta.WatchCursor{RegionID: 1, Term: 1, Index: 3},
		CommitVersion: 12,
		Source:        fsmeta.WatchEventSourceCommit,
		Key:           []byte("other/c"),
	}
	router.Publish(first)
	router.Publish(second)
	router.Publish(thirdOtherPrefix)

	sub, err := router.Subscribe(context.Background(), fsmeta.WatchRequest{
		KeyPrefix:    prefix,
		ResumeCursor: first.Cursor,
	})
	require.NoError(t, err)
	defer sub.Close()
	require.Equal(t, thirdOtherPrefix.Cursor, sub.ReadyCursor())
	require.Equal(t, second, <-sub.Events())
	select {
	case got := <-sub.Events():
		t.Fatalf("unexpected extra replay event: %+v", got)
	default:
	}

	live := fsmeta.WatchEvent{
		Cursor:        fsmeta.WatchCursor{RegionID: 1, Term: 1, Index: 4},
		CommitVersion: 13,
		Source:        fsmeta.WatchEventSourceCommit,
		Key:           []byte("k/d"),
	}
	router.Publish(live)
	require.Equal(t, live, <-sub.Events())
}

func TestRouterRejectsExpiredResumeCursor(t *testing.T) {
	router := NewRouter()
	router.Publish(fsmeta.WatchEvent{
		Cursor:        fsmeta.WatchCursor{RegionID: 1, Term: 1, Index: 10},
		CommitVersion: 20,
		Source:        fsmeta.WatchEventSourceCommit,
		Key:           []byte("k/current"),
	})

	_, err := router.Subscribe(context.Background(), fsmeta.WatchRequest{
		KeyPrefix:    []byte("k/"),
		ResumeCursor: fsmeta.WatchCursor{RegionID: 1, Term: 1, Index: 9},
	})
	require.ErrorIs(t, err, model.ErrWatchCursorExpired)
}

func TestRouterRetiresMountSubscriptions(t *testing.T) {
	router := NewRouter()
	volPrefix, err := fsmeta.EncodeDentryPrefix(model.MountIdentity{MountID: "vol", MountKeyID: 1}, model.RootInode)
	require.NoError(t, err)
	otherPrefix, err := fsmeta.EncodeDentryPrefix(model.MountIdentity{MountID: "other", MountKeyID: 2}, model.RootInode)
	require.NoError(t, err)
	volSub, err := router.Subscribe(context.Background(), fsmeta.WatchRequest{
		Mount:     "vol",
		KeyPrefix: volPrefix,
	})
	require.NoError(t, err)
	otherSub, err := router.Subscribe(context.Background(), fsmeta.WatchRequest{
		KeyPrefix: otherPrefix,
	})
	require.NoError(t, err)
	defer otherSub.Close()

	require.Equal(t, 1, router.RetireMount("vol"))
	_, ok := <-volSub.Events()
	require.False(t, ok)
	require.ErrorIs(t, volSub.Err(), model.ErrMountRetired)

	evt := fsmeta.WatchEvent{
		Cursor:        fsmeta.WatchCursor{RegionID: 1, Term: 1, Index: 10},
		CommitVersion: 20,
		Source:        fsmeta.WatchEventSourceCommit,
		Key:           append(otherPrefix, []byte("entry")...),
	}
	router.Publish(fsmeta.WatchEvent{
		Cursor:        fsmeta.WatchCursor{RegionID: 1, Term: 1, Index: 9},
		CommitVersion: 19,
		Source:        fsmeta.WatchEventSourceCommit,
		Key:           append(volPrefix, []byte("entry")...),
	})
	router.Publish(evt)
	require.Equal(t, evt, <-otherSub.Events())
}

func TestWatchPrefixRejectsRecursiveInodeSubtree(t *testing.T) {
	_, err := fsmeta.WatchPrefix(fsmeta.WatchRequest{
		Mount:              "vol",
		RootInode:          model.RootInode,
		DescendRecursively: true,
	})
	require.ErrorIs(t, err, model.ErrInvalidRequest)
}
