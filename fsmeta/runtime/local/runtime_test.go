// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package local

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/contract"
	storemvcc "github.com/feichai0017/NoKV/raftstore/mvcc"
	"github.com/stretchr/testify/require"
)

func TestOpenCreateLookupSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	rt, err := Open(ctx, Options{WorkDir: dir, Mount: testMount()})
	require.NoError(t, err)

	created, err := rt.Executor.Create(ctx, fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "agent-state",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile, Mode: 0o644},
	})
	require.NoError(t, err)
	require.NoError(t, rt.Close())

	rt, err = Open(ctx, Options{WorkDir: dir, Mount: testMount()})
	require.NoError(t, err)
	defer func() { require.NoError(t, rt.Close()) }()

	got, err := rt.Executor.Lookup(ctx, fsmeta.LookupRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "agent-state",
	})
	require.NoError(t, err)
	require.Equal(t, created.Dentry, got)

	next, err := rt.Executor.Create(ctx, fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "next",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	})
	require.NoError(t, err)
	require.Greater(t, next.Inode.Inode, created.Inode.Inode)
}

func TestOpenUsesShardedLocalDBByDefault(t *testing.T) {
	ctx := context.Background()
	rt, err := Open(ctx, Options{WorkDir: t.TempDir(), Mount: testMount()})
	require.NoError(t, err)
	defer func() { require.NoError(t, rt.Close()) }()

	require.Len(t, rt.DB.LSMWALs(), 4)
}

func TestLocalInodeAllocatorChoosesWorkspaceShard(t *testing.T) {
	ctx := context.Background()
	rt, err := Open(ctx, Options{WorkDir: t.TempDir(), Mount: testMount()})
	require.NoError(t, err)
	defer func() { require.NoError(t, rt.Close()) }()

	created, err := rt.Executor.Create(ctx, fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "workspace-a",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeDirectory},
	})
	require.NoError(t, err)
	require.Equal(t, fsmeta.ChooseWorkspaceBucket(testMount(), "workspace-a"), fsmeta.BucketForInodeID(created.Inode.Inode))
}

func TestOpenUsesDirectMVCC(t *testing.T) {
	ctx := context.Background()
	rt, err := Open(ctx, Options{
		WorkDir: t.TempDir(),
		Mount:   testMount(),
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, rt.Close()) }()

	created, err := rt.Executor.Create(ctx, fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "direct",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	})
	require.NoError(t, err)
	got, err := rt.Executor.Lookup(ctx, fsmeta.LookupRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "direct",
	})
	require.NoError(t, err)
	require.Equal(t, created.Dentry, got)
}

func TestLocalRuntimeConcurrentDirectoryChildCount(t *testing.T) {
	ctx := context.Background()
	rt, err := Open(ctx, Options{
		WorkDir: t.TempDir(),
		Mount:   testMount(),
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, rt.Close()) }()

	dir, err := rt.Executor.Create(ctx, fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "hot-dir",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeDirectory},
	})
	require.NoError(t, err)

	const files = 32
	runConcurrentFSOps(t, files, func(i int) error {
		_, err := rt.Executor.Create(ctx, fsmeta.CreateRequest{
			Mount:  "vol",
			Parent: dir.Inode.Inode,
			Name:   fmt.Sprintf("file-%03d", i),
			Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
		})
		return err
	})

	rows, err := rt.Executor.ReadDirPlus(ctx, fsmeta.ReadDirRequest{
		Mount:  "vol",
		Parent: dir.Inode.Inode,
		Limit:  files,
	})
	require.NoError(t, err)
	require.Len(t, rows, files)

	runConcurrentFSOps(t, files, func(i int) error {
		return rt.Executor.Unlink(ctx, fsmeta.UnlinkRequest{
			Mount:  "vol",
			Parent: dir.Inode.Inode,
			Name:   fmt.Sprintf("file-%03d", i),
		})
	})
	require.NoError(t, rt.Executor.RemoveDirectory(ctx, fsmeta.RemoveDirectoryRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "hot-dir",
	}))
}

func TestLocalRuntimePassesFSMetaContract(t *testing.T) {
	ctx := context.Background()
	model := contract.NewModel("vol")
	rt, err := Open(ctx, Options{
		WorkDir: t.TempDir(),
		Mount:   testMount(),
		Clock: func() time.Time {
			return time.Unix(0, model.NowUnixNs)
		},
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, rt.Close()) }()

	mapped, err := contract.NewInodeMappingExecutor(rt.Executor)
	require.NoError(t, err)
	err = contract.Run(ctx, mapped, model, contract.GenerateScript(3, 60))
	require.NoError(t, err)
}

func TestLocalRuntimePublishesWatchEvents(t *testing.T) {
	ctx := context.Background()
	rt, err := Open(ctx, Options{WorkDir: t.TempDir(), Mount: testMount()})
	require.NoError(t, err)
	defer func() { require.NoError(t, rt.Close()) }()

	sub, err := rt.Watcher.Subscribe(ctx, fsmeta.WatchRequest{
		Mount:              "vol",
		RootInode:          fsmeta.RootInode,
		BackPressureWindow: 4,
	})
	require.NoError(t, err)
	defer sub.Close()

	_, err = rt.Executor.Create(ctx, fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "watched",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	})
	require.NoError(t, err)

	wantKey, err := fsmeta.EncodeDentryKey(testMount(), fsmeta.RootInode, "watched")
	require.NoError(t, err)
	evt := requireWatchEvent(t, sub)
	require.Equal(t, wantKey, evt.Key)
	require.Equal(t, fsmeta.WatchEventSourceCommit, evt.Source)
	require.Equal(t, localWatchTerm, evt.Cursor.Term)
	require.NotZero(t, evt.Cursor.Index)

	stats := rt.Watcher.Stats()
	require.GreaterOrEqual(t, stats["events_total"].(uint64), uint64(1))
	require.Equal(t, uint64(1), stats["delivered_total"])
}

func TestLocalRuntimeWatchReplaysAfterResumeCursor(t *testing.T) {
	ctx := context.Background()
	rt, err := Open(ctx, Options{
		WorkDir: t.TempDir(),
		Mount:   testMount(),
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, rt.Close()) }()

	firstSub, err := rt.Watcher.Subscribe(ctx, fsmeta.WatchRequest{
		Mount:     "vol",
		RootInode: fsmeta.RootInode,
	})
	require.NoError(t, err)
	_, err = rt.Executor.Create(ctx, fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "first",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	})
	require.NoError(t, err)
	first := requireWatchEvent(t, firstSub)
	firstSub.Close()

	_, err = rt.Executor.Create(ctx, fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "second",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	})
	require.NoError(t, err)

	resumed, err := rt.Watcher.Subscribe(ctx, fsmeta.WatchRequest{
		Mount:        "vol",
		RootInode:    fsmeta.RootInode,
		ResumeCursor: first.Cursor,
	})
	require.NoError(t, err)
	defer resumed.Close()

	wantKey, err := fsmeta.EncodeDentryKey(testMount(), fsmeta.RootInode, "second")
	require.NoError(t, err)
	replayed := requireWatchEvent(t, resumed)
	require.Equal(t, wantKey, replayed.Key)
	require.Greater(t, replayed.Cursor.Index, first.Cursor.Index)
}

func TestLocalRuntimePublishesAndRetiresSnapshots(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	rt, err := Open(ctx, Options{WorkDir: dir, Mount: testMount()})
	require.NoError(t, err)

	token, err := rt.Executor.SnapshotSubtree(ctx, fsmeta.SnapshotSubtreeRequest{
		Mount:     "vol",
		RootInode: fsmeta.RootInode,
	})
	require.NoError(t, err)
	require.NoError(t, rt.Snapshots.PublishSnapshotSubtree(ctx, token))
	stats := rt.Snapshots.Stats()
	require.Equal(t, 1, stats["active_snapshots"])
	require.Equal(t, token.ReadVersion, stats["retention_floor"])
	require.Equal(t, true, stats["persistent"])
	floor, ok := rt.Snapshots.SnapshotRetentionFloor()
	require.True(t, ok)
	require.Equal(t, token.ReadVersion, floor)

	gcKey, err := fsmeta.EncodeDentryKey(testMount(), fsmeta.RootInode, "pinned")
	require.NoError(t, err)
	policy := storemvcc.SafePointPolicy{
		RequestedSafePoint: token.ReadVersion + 100,
		SnapshotRetention:  rt.Snapshots.SnapshotRetentionIndex(),
		Mount:              fsmeta.MountKeyResolver,
	}
	require.Equal(t, token.ReadVersion, policy.EffectiveForKey(gcKey))

	require.NoError(t, rt.Close())

	rt, err = Open(ctx, Options{WorkDir: dir, Mount: testMount()})
	require.NoError(t, err)
	stats = rt.Snapshots.Stats()
	require.Equal(t, 1, stats["active_snapshots"])
	require.Equal(t, token.ReadVersion, stats["retention_floor"])
	require.Equal(t, uint64(1), stats["recovered_total"])

	resolved, err := rt.Executor.ResolveSnapshotSubtreeToken(ctx, token)
	require.NoError(t, err)
	require.NoError(t, rt.Snapshots.RetireSnapshotSubtree(ctx, resolved))
	require.Equal(t, 0, rt.Snapshots.Stats()["active_snapshots"])
	require.NoError(t, rt.Close())

	rt, err = Open(ctx, Options{WorkDir: dir, Mount: testMount()})
	require.NoError(t, err)
	defer func() { require.NoError(t, rt.Close()) }()
	stats = rt.Snapshots.Stats()
	require.Equal(t, 0, stats["active_snapshots"])
	require.Equal(t, uint64(0), stats["retention_floor"])
}

func TestLocalRuntimeMaintainsQuotaUsage(t *testing.T) {
	ctx := context.Background()
	rt, err := Open(ctx, Options{WorkDir: t.TempDir(), Mount: testMount()})
	require.NoError(t, err)
	defer func() { require.NoError(t, rt.Close()) }()

	_, err = rt.Executor.Create(ctx, fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "quota-file",
		Attrs: fsmeta.CreateAttrs{
			Type: fsmeta.InodeTypeFile,
			Size: 4096,
		},
	})
	require.NoError(t, err)

	rootUsage, err := rt.Executor.GetQuotaUsage(ctx, fsmeta.QuotaUsageRequest{
		Mount: "vol",
		Scope: fsmeta.RootInode,
	})
	require.NoError(t, err)
	require.Equal(t, fsmeta.UsageRecord{Bytes: 4096, Inodes: 1}, rootUsage)

	mountUsage, err := rt.Executor.GetQuotaUsage(ctx, fsmeta.QuotaUsageRequest{Mount: "vol"})
	require.NoError(t, err)
	require.Equal(t, rootUsage, mountUsage)
}

func TestLocalRuntimeReadDirPlusSeesCreateAfterEmptyRead(t *testing.T) {
	ctx := context.Background()
	rt, err := Open(ctx, Options{
		WorkDir: t.TempDir(),
		Mount:   testMount(),
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, rt.Close()) }()

	pairs, err := rt.Executor.ReadDirPlus(ctx, fsmeta.ReadDirRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Limit:  8,
	})
	require.NoError(t, err)
	require.Empty(t, pairs)

	_, err = rt.Executor.Create(ctx, fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "after-empty-read",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	})
	require.NoError(t, err)

	pairs, err = rt.Executor.ReadDirPlus(ctx, fsmeta.ReadDirRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Limit:  8,
	})
	require.NoError(t, err)
	require.Len(t, pairs, 1)
	require.Equal(t, "after-empty-read", pairs[0].Dentry.Name)
}

func runConcurrentFSOps(t *testing.T, count int, fn func(int) error) {
	t.Helper()
	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make(chan error, count)
	for i := range count {
		wg.Go(func() {
			<-start
			if err := fn(i); err != nil {
				errs <- err
			}
		})
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}

func requireWatchEvent(t *testing.T, sub fsmeta.WatchSubscription) fsmeta.WatchEvent {
	t.Helper()
	select {
	case evt, ok := <-sub.Events():
		require.True(t, ok, "watch subscription closed: %v", sub.Err())
		return evt
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for watch event")
		return fsmeta.WatchEvent{}
	}
}
