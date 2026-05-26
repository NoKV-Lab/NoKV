// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"bytes"
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/engine/slab/dirpage"
	"github.com/feichai0017/NoKV/engine/slab/negativecache"
	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/stretchr/testify/require"
)

func TestExecutorCreateAndLookup(t *testing.T) {
	runner := newFakeRunner()
	executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{22}}))
	require.NoError(t, err)

	result, err := executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	})
	require.NoError(t, err)
	require.Equal(t, model.InodeID(22), result.Inode.Inode)

	record, err := executor.Lookup(context.Background(), model.LookupRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "file",
	})
	require.NoError(t, err)
	require.Equal(t, model.DentryRecord{
		Parent: model.RootInode,
		Name:   "file",
		Inode:  22,
		Type:   model.InodeTypeFile,
	}, record)

	require.Len(t, runner.mutations, 1)
	require.Len(t, runner.mutations[0], 3)
	require.True(t, runner.mutations[0][1].GetAssertionNotExist())
	require.True(t, runner.mutations[0][2].GetAssertionNotExist())
}

func TestExecutorCreateVisibleCommitServesLookupOverlay(t *testing.T) {
	runner := newFakeRunner()
	committer := newTestVisibleCommitter(t, runner)
	inode := testInodeForParentBucket(t, model.RootInode)
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{inode}}),
		WithVisibleAuthorityAdmitter(ownedVisibleAdmitter{}),
		WithVisibleCommitter(committer),
	)
	require.NoError(t, err)

	created, err := executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	})
	require.NoError(t, err)
	require.Empty(t, runner.mutations)

	lookedUp, err := executor.Lookup(context.Background(), model.LookupRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "file",
	})
	require.NoError(t, err)
	require.Equal(t, created.Dentry, lookedUp)
}

func TestExecutorCreateVisibleCommitServesReadDirPlusOverlay(t *testing.T) {
	runner := newFakeRunner()
	committer := newTestVisibleCommitter(t, runner)
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{22}}),
		WithVisibleAuthorityAdmitter(ownedVisibleAdmitter{}),
		WithVisibleCommitter(committer),
	)
	require.NoError(t, err)

	created, err := executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	})
	require.NoError(t, err)

	pairs, err := executor.ReadDirPlus(context.Background(), model.ReadDirRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Limit:  16,
	})
	require.NoError(t, err)
	require.Equal(t, []model.DentryAttrPair{{
		Dentry: created.Dentry,
		Inode:  created.Inode,
	}}, pairs)
}

func TestExecutorReadDirPlusOverlayOnlyPathBypassesDirPageCache(t *testing.T) {
	runner := newFakeRunner()
	cache, err := dirpage.Open(dirpage.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	defer func() { _ = cache.Close() }()
	committer := newTestVisibleCommitter(t, runner)
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{22}}),
		WithVisibleAuthorityAdmitter(ownedVisibleAdmitter{}),
		WithVisibleCommitter(committer),
		WithDirPageCache(cache),
	)
	require.NoError(t, err)

	created, err := executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "file",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	})
	require.NoError(t, err)
	req := model.ReadDirRequest{Mount: "vol", Parent: model.RootInode, Limit: 16}

	first, err := executor.ReadDirPlus(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, []model.DentryAttrPair{{Dentry: created.Dentry, Inode: created.Inode}}, first)
	require.Equal(t, uint64(0), cache.Stats().StoreOK)

	second, err := executor.ReadDirPlus(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, first, second)
	stats := cache.Stats()
	require.Equal(t, uint64(0), stats.Hits)
	require.Equal(t, uint64(0), stats.StoreOK)
}

func TestExecutorReadDirPlusVisibleOverlayPathBypassesDirPageCache(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", model.RootInode, "base", 22)
	seedInode(t, runner, "vol", model.InodeRecord{
		Inode:     22,
		Type:      model.InodeTypeFile,
		LinkCount: 1,
	})
	cache, err := dirpage.Open(dirpage.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	defer func() { _ = cache.Close() }()
	executor, err := newTestExecutor(
		runner,
		WithVisibleCommitter(scanOverlayCommitter{directoryPresent: true}),
		WithDirPageCache(cache),
	)
	require.NoError(t, err)
	req := model.ReadDirRequest{Mount: "vol", Parent: model.RootInode, Limit: 16}

	first, err := executor.ReadDirPlus(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, []model.DentryAttrPair{{
		Dentry: model.DentryRecord{Parent: model.RootInode, Name: "base", Inode: 22, Type: model.InodeTypeFile},
		Inode:  model.InodeRecord{Inode: 22, Type: model.InodeTypeFile, LinkCount: 1},
	}}, first)
	require.Equal(t, uint64(0), cache.Stats().StoreOK)

	second, err := executor.ReadDirPlus(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, first, second)
	stats := cache.Stats()
	require.Equal(t, uint64(0), stats.Hits)
	require.Equal(t, uint64(0), stats.StoreOK)
	require.Len(t, runner.scanVersions, 2, "visible-backed ReadDirPlus must not materialize the persistent dirpage cache")
}

func TestExecutorReadDirPlusPinsVisibleOverlayAcrossDentryAndInodeReads(t *testing.T) {
	runner := newFakeRunner()
	parent := model.InodeID(7)
	seedDentry(t, runner, "vol", parent, "eta", 22)
	seedInode(t, runner, "vol", model.InodeRecord{Inode: 22, Type: model.InodeTypeFile, LinkCount: 1})
	committer := newTestVisibleCommitter(t, runner)
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{23}}),
		WithVisibleAuthorityAdmitter(ownedVisibleAdmitter{}),
		WithVisibleCommitter(committer),
	)
	require.NoError(t, err)
	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: parent,
		Name:   "omega",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	})
	require.NoError(t, err)

	var once sync.Once
	runner.beforeBatchGet = func() {
		once.Do(func() {
			require.NoError(t, executor.Link(context.Background(), model.LinkRequest{
				Mount:      "vol",
				FromParent: parent,
				FromName:   "eta",
				ToParent:   parent,
				ToName:     "zeta",
			}))
		})
	}

	pairs, err := executor.ReadDirPlus(context.Background(), model.ReadDirRequest{
		Mount:  "vol",
		Parent: parent,
		Limit:  1,
	})
	require.NoError(t, err)
	require.Len(t, pairs, 1)
	require.Equal(t, "eta", pairs[0].Dentry.Name)
	require.Equal(t, uint32(1), pairs[0].Inode.LinkCount, "ReadDirPlus must not combine a pre-link dentry page with a post-link inode overlay")

	record, err := executor.Lookup(context.Background(), model.LookupRequest{Mount: "vol", Parent: parent, Name: "zeta"})
	require.NoError(t, err)
	require.Equal(t, model.InodeID(22), record.Inode)
}

func TestExecutorReadDirPlusCachesSealedVisibleDirectory(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", model.RootInode, "base", 22)
	seedInode(t, runner, "vol", model.InodeRecord{
		Inode:     22,
		Type:      model.InodeTypeFile,
		LinkCount: 1,
	})
	sealedDentryKey := dentryKeyForTest(t, "vol", model.RootInode, "sealed")
	sealedInodeKey := inodeKeyForTest(t, "vol", 33)
	sealedRows := []VisibleOverlayKV{
		overlayValueForTest(sealedDentryKey, dentryValueForTest(t, model.RootInode, "sealed", 33, model.InodeTypeFile)),
		overlayValueForTest(sealedInodeKey, inodeValueForTest(t, model.InodeRecord{Inode: 33, Type: model.InodeTypeFile, LinkCount: 1})),
	}
	cache, err := dirpage.Open(dirpage.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	defer func() { _ = cache.Close() }()
	executor, err := newTestExecutor(
		runner,
		WithVisibleCommitter(sealedDirectoryCommitter{
			rows:     sealedRows,
			values:   overlayMapForTest(sealedRows...),
			frontier: 7,
		}),
		WithDirPageCache(cache),
	)
	require.NoError(t, err)
	req := model.ReadDirRequest{Mount: "vol", Parent: model.RootInode, Limit: 16}

	first, err := executor.ReadDirPlus(context.Background(), req)
	require.NoError(t, err)
	require.Len(t, first, 2)
	scansAfterFirst := len(runner.scanVersions)
	require.Eventually(t, func() bool {
		return cache.Stats().StoreOK > 0
	}, time.Second, 10*time.Millisecond)

	second, err := executor.ReadDirPlus(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.Equal(t, scansAfterFirst, len(runner.scanVersions), "sealed Visible directory should use frontier-aware dirpage cache")
	require.Greater(t, cache.Stats().Hits, uint64(0))
}

func TestExecutorReadDirPlusInvalidatesCacheWhenSealedVisibleFrontierChanges(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", model.RootInode, "base", 22)
	seedInode(t, runner, "vol", model.InodeRecord{
		Inode:     22,
		Type:      model.InodeTypeFile,
		LinkCount: 1,
	})
	sealedDentryKey := dentryKeyForTest(t, "vol", model.RootInode, "sealed")
	sealedInodeKey := inodeKeyForTest(t, "vol", 33)
	sealedRows := []VisibleOverlayKV{
		overlayValueForTest(sealedDentryKey, dentryValueForTest(t, model.RootInode, "sealed", 33, model.InodeTypeFile)),
		overlayValueForTest(sealedInodeKey, inodeValueForTest(t, model.InodeRecord{Inode: 33, Type: model.InodeTypeFile, LinkCount: 1})),
	}
	cache, err := dirpage.Open(dirpage.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	defer func() { _ = cache.Close() }()
	committer := &sealedDirectoryCommitter{
		rows:     sealedRows,
		values:   overlayMapForTest(sealedRows...),
		frontier: 7,
	}
	executor, err := newTestExecutor(runner, WithVisibleCommitter(committer), WithDirPageCache(cache))
	require.NoError(t, err)
	req := model.ReadDirRequest{Mount: "vol", Parent: model.RootInode, Limit: 16}

	first, err := executor.ReadDirPlus(context.Background(), req)
	require.NoError(t, err)
	require.Len(t, first, 2)
	require.Eventually(t, func() bool {
		return cache.Stats().StoreOK > 0
	}, time.Second, 10*time.Millisecond)
	scansAfterFirst := len(runner.scanVersions)

	newDentryKey := dentryKeyForTest(t, "vol", model.RootInode, "sealed-new")
	newInodeKey := inodeKeyForTest(t, "vol", 44)
	newRows := []VisibleOverlayKV{
		overlayValueForTest(newDentryKey, dentryValueForTest(t, model.RootInode, "sealed-new", 44, model.InodeTypeFile)),
		overlayValueForTest(newInodeKey, inodeValueForTest(t, model.InodeRecord{Inode: 44, Type: model.InodeTypeFile, LinkCount: 1})),
	}
	committer.rows = append(committer.rows, newRows...)
	committer.values = overlayMapForTest(committer.rows...)
	committer.frontier = 8

	second, err := executor.ReadDirPlus(context.Background(), req)
	require.NoError(t, err)
	require.Len(t, second, 3)
	require.Greater(t, len(runner.scanVersions), scansAfterFirst, "sealed frontier change must invalidate stale dirpage rows")
}

func TestExecutorReadDirPlusUsesDirPageCacheWhenVisibleHasNoDirectoryOverlay(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", model.RootInode, "base", 22)
	seedInode(t, runner, "vol", model.InodeRecord{
		Inode:     22,
		Type:      model.InodeTypeFile,
		LinkCount: 1,
	})
	cache, err := dirpage.Open(dirpage.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	defer func() { _ = cache.Close() }()
	committer := newTestVisibleCommitter(t, runner)
	executor, err := newTestExecutor(
		runner,
		WithVisibleCommitter(committer),
		WithDirPageCache(cache),
	)
	require.NoError(t, err)
	req := model.ReadDirRequest{Mount: "vol", Parent: model.RootInode, Limit: 16}

	first, err := executor.ReadDirPlus(context.Background(), req)
	require.NoError(t, err)
	require.Len(t, first, 1)
	scansAfterFirst := len(runner.scanVersions)
	require.Eventually(t, func() bool {
		return cache.Stats().StoreOK > 0
	}, time.Second, 10*time.Millisecond)

	second, err := executor.ReadDirPlus(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.Equal(t, scansAfterFirst, len(runner.scanVersions), "directory without Visible rows should still use dirpage cache")
}

func TestExecutorReadDirPlusUsesDirPageCacheWhenOverlayRowsAreInAnotherDirectory(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", model.RootInode, "base", 22)
	seedInode(t, runner, "vol", model.InodeRecord{
		Inode:     22,
		Type:      model.InodeTypeFile,
		LinkCount: 1,
	})
	seedInode(t, runner, "vol", model.InodeRecord{
		Inode:     33,
		Type:      model.InodeTypeDirectory,
		LinkCount: 1,
	})
	cache, err := dirpage.Open(dirpage.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	defer func() { _ = cache.Close() }()
	committer := newTestVisibleCommitter(t, runner)
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{next: 44}),
		WithVisibleAuthorityAdmitter(ownedVisibleAdmitter{}),
		WithVisibleCommitter(committer),
		WithDirPageCache(cache),
	)
	require.NoError(t, err)
	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: 33,
		Name:   "overlay",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	})
	require.NoError(t, err)

	req := model.ReadDirRequest{Mount: "vol", Parent: model.RootInode, Limit: 16}
	first, err := executor.ReadDirPlus(context.Background(), req)
	require.NoError(t, err)
	require.Len(t, first, 1)
	scansAfterFirst := len(runner.scanVersions)
	require.Eventually(t, func() bool {
		return cache.Stats().StoreOK > 0
	}, time.Second, 10*time.Millisecond)

	second, err := executor.ReadDirPlus(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.Equal(t, scansAfterFirst, len(runner.scanVersions), "Visible rows in a different directory must not disable this directory's cache")
}

func TestExecutorReadDirVisibleCreatedDirectorySkipsBaseScan(t *testing.T) {
	runner := newFakeRunner()
	dirInode := testInodeForParentBucket(t, model.RootInode)
	childInode := testInodeForParentBucket(t, dirInode, dirInode)
	committer := newTestVisibleCommitter(t, runner)
	executor, err := newTestExecutor(
		runner,
		WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{dirInode, childInode}}),
		WithVisibleAuthorityAdmitter(ownedVisibleAdmitter{}),
		WithVisibleCommitter(committer),
	)
	require.NoError(t, err)

	dir, err := executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "run",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeDirectory},
	})
	require.NoError(t, err)
	file, err := executor.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: dir.Inode.Inode,
		Name:   "artifact",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	})
	require.NoError(t, err)
	runner.scanVersions = nil
	runner.batchVersions = nil

	records, err := executor.ReadDir(context.Background(), model.ReadDirRequest{
		Mount:  "vol",
		Parent: dir.Inode.Inode,
		Limit:  16,
	})
	require.NoError(t, err)
	require.Equal(t, []model.DentryRecord{file.Dentry}, records)
	require.Empty(t, runner.scanVersions, "Visible-created directory has a covered base namespace")

	pairs, err := executor.ReadDirPlus(context.Background(), model.ReadDirRequest{
		Mount:  "vol",
		Parent: dir.Inode.Inode,
		Limit:  16,
	})
	require.NoError(t, err)
	require.Equal(t, []model.DentryAttrPair{{Dentry: file.Dentry, Inode: file.Inode}}, pairs)
	require.Empty(t, runner.scanVersions)
	require.Empty(t, runner.batchVersions)
}

func TestExecutorLookupReturnsNotFound(t *testing.T) {
	executor, err := newTestExecutor(newFakeRunner())
	require.NoError(t, err)

	_, err = executor.Lookup(context.Background(), model.LookupRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "missing",
	})
	require.ErrorIs(t, err, model.ErrNotFound)
}

func TestExecutorReadDirConsumesPlanCursorAndLimit(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "a", 21)
	seedDentry(t, runner, "vol", 7, "b", 22)
	seedDentry(t, runner, "vol", 7, "c", 23)
	seedDentry(t, runner, "vol", 8, "outside", 99)

	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	records, err := executor.ReadDir(context.Background(), model.ReadDirRequest{
		Mount:      "vol",
		Parent:     7,
		StartAfter: "a",
		Limit:      1,
	})
	require.NoError(t, err)
	require.Equal(t, []model.DentryRecord{{
		Parent: 7,
		Name:   "b",
		Inode:  22,
		Type:   model.InodeTypeFile,
	}}, records)
}

func TestExecutorReadDirRetriesLiveLock(t *testing.T) {
	runner := newFakeRunner()
	runner.scanErrs = []error{txnLockedError("vol", 7, "a")}
	seedDentry(t, runner, "vol", 7, "a", 21)
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	records, err := executor.ReadDir(context.Background(), model.ReadDirRequest{
		Mount:  "vol",
		Parent: 7,
		Limit:  8,
	})
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, []uint64{1, 2}, runner.scanVersions)
	requireStatUint(t, executor.Stats(), "read_retries_total", 1)
	requireStatUint(t, executor.Stats(), "read_retry_exhausted_total", 0)
}

func TestExecutorReadDirExhaustsRetriesOnLiveLock(t *testing.T) {
	runner := newFakeRunner()
	for range maxReadContentionRetries + 1 {
		runner.scanErrs = append(runner.scanErrs, txnLockedError("vol", 7, "a"))
	}
	seedDentry(t, runner, "vol", 7, "a", 21)
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	_, err = executor.ReadDir(context.Background(), model.ReadDirRequest{
		Mount:  "vol",
		Parent: 7,
		Limit:  8,
	})
	require.Error(t, err)
	wantVersions := make([]uint64, maxReadContentionRetries+1)
	for i := range wantVersions {
		wantVersions[i] = uint64(i + 1)
	}
	require.Equal(t, wantVersions, runner.scanVersions)
	requireStatUint(t, executor.Stats(), "read_retries_total", uint64(maxReadContentionRetries))
	requireStatUint(t, executor.Stats(), "read_retry_exhausted_total", 1)
}

func TestExecutorReadDirPlusReturnsDentriesAndAttrs(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "a", 21)
	seedInode(t, runner, "vol", model.InodeRecord{
		Inode:     21,
		Type:      model.InodeTypeFile,
		Size:      4096,
		Mode:      0o644,
		LinkCount: 1,
	})
	seedDentryType(t, runner, "vol", 7, "b", 22, model.InodeTypeDirectory)
	seedInode(t, runner, "vol", model.InodeRecord{
		Inode:     22,
		Type:      model.InodeTypeDirectory,
		Mode:      0o755,
		LinkCount: 2,
	})

	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	pairs, err := executor.ReadDirPlus(context.Background(), model.ReadDirRequest{
		Mount:  "vol",
		Parent: 7,
		Limit:  8,
	})
	require.NoError(t, err)
	require.Equal(t, []model.DentryAttrPair{
		{
			Dentry: model.DentryRecord{Parent: 7, Name: "a", Inode: 21, Type: model.InodeTypeFile},
			Inode: model.InodeRecord{
				Inode:     21,
				Type:      model.InodeTypeFile,
				Size:      4096,
				Mode:      0o644,
				LinkCount: 1,
			},
		},
		{
			Dentry: model.DentryRecord{Parent: 7, Name: "b", Inode: 22, Type: model.InodeTypeDirectory},
			Inode: model.InodeRecord{
				Inode:     22,
				Type:      model.InodeTypeDirectory,
				Mode:      0o755,
				LinkCount: 2,
			},
		},
	}, pairs)
}

func TestExecutorLookupPlusReturnsDentryAndAttrs(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "a", 21)
	seedInode(t, runner, "vol", model.InodeRecord{
		Inode:     21,
		Type:      model.InodeTypeFile,
		Size:      4096,
		Mode:      0o644,
		LinkCount: 1,
	})
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	pair, err := executor.LookupPlus(context.Background(), model.LookupRequest{
		Mount:  "vol",
		Parent: 7,
		Name:   "a",
	})
	require.NoError(t, err)
	require.Equal(t, model.DentryAttrPair{
		Dentry: model.DentryRecord{Parent: 7, Name: "a", Inode: 21, Type: model.InodeTypeFile},
		Inode: model.InodeRecord{
			Inode:     21,
			Type:      model.InodeTypeFile,
			Size:      4096,
			Mode:      0o644,
			LinkCount: 1,
		},
	}, pair)
}

func TestExecutorReadDirPlusMissingInodeReturnsNotFound(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "a", 21)
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	_, err = executor.ReadDirPlus(context.Background(), model.ReadDirRequest{
		Mount:  "vol",
		Parent: 7,
		Limit:  8,
	})
	require.ErrorIs(t, err, model.ErrNotFound)
}

func TestExecutorLookupUsesVisibleOverlayWithoutTimestamp(t *testing.T) {
	runner := newFakeRunner()
	key := dentryKeyForTest(t, "vol", model.RootInode, "visible")
	value := dentryValueForTest(t, model.RootInode, "visible", 22, model.InodeTypeFile)
	committer := scanOverlayCommitter{
		values: overlayMapForTest(overlayValueForTest(key, value)),
	}
	executor, err := newTestExecutor(runner, WithVisibleCommitter(committer))
	require.NoError(t, err)

	record, err := executor.Lookup(context.Background(), model.LookupRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "visible",
	})
	require.NoError(t, err)
	require.Equal(t, model.InodeID(22), record.Inode)
	require.Zero(t, runner.getCalls)
	require.Equal(t, uint64(1), runner.nextTS, "overlay lookup must not reserve a read timestamp")
}

func TestExecutorLookupUsesMergedDirectoryWhenVisibleTombstoneHidesBase(t *testing.T) {
	runner := newFakeRunner()
	parent := model.InodeID(7)
	seedDentry(t, runner, "vol", parent, "alpha", 22)
	alphaKey := dentryKeyForTest(t, "vol", parent, "alpha")
	etaKey := dentryKeyForTest(t, "vol", parent, "eta")
	rows := []VisibleOverlayKV{
		overlayDeleteForTest(alphaKey),
		overlayValueForTest(etaKey, dentryValueForTest(t, parent, "eta", 22, model.InodeTypeFile)),
	}
	executor, err := newTestExecutor(runner, WithVisibleCommitter(scanOverlayCommitter{
		rows:   rows,
		values: overlayMapForTest(rows...),
	}))
	require.NoError(t, err)

	_, err = executor.Lookup(context.Background(), model.LookupRequest{Mount: "vol", Parent: parent, Name: "alpha"})
	require.ErrorIs(t, err, model.ErrNotFound)
	record, err := executor.Lookup(context.Background(), model.LookupRequest{Mount: "vol", Parent: parent, Name: "eta"})
	require.NoError(t, err)
	require.Equal(t, model.DentryRecord{Parent: parent, Name: "eta", Inode: 22, Type: model.InodeTypeFile}, record)
}

func TestExecutorLookupUsesVisibleOverlayDeleteWithoutRunner(t *testing.T) {
	runner := newFakeRunner()
	key := dentryKeyForTest(t, "vol", model.RootInode, "deleted")
	committer := scanOverlayCommitter{
		values: overlayMapForTest(overlayDeleteForTest(key)),
	}
	executor, err := newTestExecutor(runner, WithVisibleCommitter(committer))
	require.NoError(t, err)

	_, err = executor.Lookup(context.Background(), model.LookupRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "deleted",
	})
	require.ErrorIs(t, err, model.ErrNotFound)
	require.Zero(t, runner.getCalls)
	require.Equal(t, uint64(1), runner.nextTS, "overlay tombstone lookup must not reserve a read timestamp")
}

func TestExecutorLookupChecksVisibleOverlayBeforeNegativeCache(t *testing.T) {
	runner := newFakeRunner()
	cache := negativecache.New(negativecache.Config{
		GroupKeyFn: func(k []byte) []byte { return k },
	})
	key := dentryKeyForTest(t, "vol", model.RootInode, "visible")
	value := dentryValueForTest(t, model.RootInode, "visible", 22, model.InodeTypeFile)
	committer := scanOverlayCommitter{
		values: overlayMapForTest(overlayValueForTest(key, value)),
	}
	executor, err := newTestExecutor(runner, WithNegativeCache(cache), WithVisibleCommitter(committer))
	require.NoError(t, err)
	cache.Remember(key)

	record, err := executor.Lookup(context.Background(), model.LookupRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "visible",
	})
	require.NoError(t, err)
	require.Equal(t, model.InodeID(22), record.Inode)
	require.Zero(t, runner.getCalls)
	require.False(t, cache.Has(key), "overlay hit must invalidate stale negative memo")
}

func TestExecutorClearsNegativeCacheWhenVisibleOverlayIsEnabled(t *testing.T) {
	runner := newFakeRunner()
	cache := negativecache.New(negativecache.Config{
		GroupKeyFn: func(k []byte) []byte { return k },
	})
	key := dentryKeyForTest(t, "vol", model.RootInode, "stale")
	cache.Remember(key)

	_, err := newTestExecutor(runner, WithNegativeCache(cache), WithVisibleCommitter(scanOverlayCommitter{}))
	require.NoError(t, err)
	require.False(t, cache.Has(key), "startup Visible replay can make persisted negative memos stale")
}

func TestExecutorReadDirRefillsBaseAfterOverlayTombstone(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "a", 21)
	seedDentry(t, runner, "vol", 7, "b", 22)
	seedDentry(t, runner, "vol", 7, "c", 23)
	deleteKey := dentryKeyForTest(t, "vol", 7, "a")
	committer := scanOverlayCommitter{
		rows: []VisibleOverlayKV{overlayDeleteForTest(deleteKey)},
	}
	executor, err := newTestExecutor(runner, WithVisibleCommitter(committer))
	require.NoError(t, err)

	records, err := executor.ReadDir(context.Background(), model.ReadDirRequest{
		Mount:  "vol",
		Parent: 7,
		Limit:  2,
	})
	require.NoError(t, err)
	require.Equal(t, []model.DentryRecord{
		{Parent: 7, Name: "b", Inode: 22, Type: model.InodeTypeFile},
		{Parent: 7, Name: "c", Inode: 23, Type: model.InodeTypeFile},
	}, records)
	require.Len(t, runner.scanVersions, 2, "base scan must refill after overlay tombstone removes a row")
}

func TestExecutorReadDirRefillsOverlayTombstonesBeforeBaseRows(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", 7, "a", 21)
	seedDentry(t, runner, "vol", 7, "b", 22)
	seedDentry(t, runner, "vol", 7, "c", 23)
	seedDentry(t, runner, "vol", 7, "d", 24)
	seedDentry(t, runner, "vol", 7, "e", 25)
	committer := scanOverlayCommitter{
		rows: []VisibleOverlayKV{
			overlayDeleteForTest(dentryKeyForTest(t, "vol", 7, "a")),
			overlayDeleteForTest(dentryKeyForTest(t, "vol", 7, "b")),
			overlayDeleteForTest(dentryKeyForTest(t, "vol", 7, "c")),
		},
	}
	executor, err := newTestExecutor(runner, WithVisibleCommitter(committer))
	require.NoError(t, err)

	records, err := executor.ReadDir(context.Background(), model.ReadDirRequest{
		Mount:  "vol",
		Parent: 7,
		Limit:  2,
	})
	require.NoError(t, err)
	require.Equal(t, []model.DentryRecord{
		{Parent: 7, Name: "d", Inode: 24, Type: model.InodeTypeFile},
		{Parent: 7, Name: "e", Inode: 25, Type: model.InodeTypeFile},
	}, records)
}

func TestExecutorReadDirOverlayOnlyRefillsOverlayAfterTombstone(t *testing.T) {
	prefix, err := fsmeta.EncodeDentryPrefix(testMountIdentityFor("vol"), 7)
	require.NoError(t, err)
	committer := scanOverlayCommitter{
		rows: []VisibleOverlayKV{
			overlayDeleteForTest(dentryKeyForTest(t, "vol", 7, "a")),
			overlayDeleteForTest(dentryKeyForTest(t, "vol", 7, "b")),
			overlayValueForTest(
				dentryKeyForTest(t, "vol", 7, "c"),
				dentryValueForTest(t, 7, "c", 23, model.InodeTypeFile),
			),
			overlayValueForTest(
				dentryKeyForTest(t, "vol", 7, "d"),
				dentryValueForTest(t, 7, "d", 24, model.InodeTypeFile),
			),
		},
	}
	executor := &Executor{visibleCommitter: committer}

	kvs, rows, _ := executor.mergeVisibleDirectoryOverlayScan(nil, prefix, prefix, 2)
	require.Equal(t, uint32(4), rows)
	require.Len(t, kvs, 2)
	first, err := fsmeta.DecodeDentryValue(kvs[0].Value)
	require.NoError(t, err)
	second, err := fsmeta.DecodeDentryValue(kvs[1].Value)
	require.NoError(t, err)
	require.Equal(t, "c", first.Name)
	require.Equal(t, "d", second.Name)
}

func TestExecutorReadDirPlusUsesVisibleOverlayInodesWithoutBatchGet(t *testing.T) {
	runner := newFakeRunner()
	seedDentry(t, runner, "vol", model.RootInode, "visible", 22)
	inodeKey := inodeKeyForTest(t, "vol", 22)
	inodeValue := inodeValueForTest(t, model.InodeRecord{
		Inode:     22,
		Type:      model.InodeTypeFile,
		LinkCount: 1,
	})
	committer := scanOverlayCommitter{
		directoryPresent: true,
		values:           overlayMapForTest(overlayValueForTest(inodeKey, inodeValue)),
	}
	executor, err := newTestExecutor(runner, WithVisibleCommitter(committer))
	require.NoError(t, err)

	pairs, err := executor.ReadDirPlus(context.Background(), model.ReadDirRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Limit:  8,
	})
	require.NoError(t, err)
	require.Len(t, pairs, 1)
	require.Equal(t, "visible", pairs[0].Dentry.Name)
	require.Equal(t, model.InodeID(22), pairs[0].Inode.Inode)
	require.NotEmpty(t, runner.scanVersions)
	require.Empty(t, runner.batchVersions, "inode attributes supplied by overlay should skip runner.BatchGet")
}

func TestExecutorNegativeCacheLookupShortCircuit(t *testing.T) {
	runner := newFakeRunner()
	cache := negativecache.New(negativecache.Config{
		GroupKeyFn: func(k []byte) []byte { return k },
	})
	executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{100}}), WithNegativeCache(cache))
	require.NoError(t, err)

	req := model.LookupRequest{Mount: "vol", Parent: model.RootInode, Name: "missing"}

	// First lookup: real LSM probe (runner.Get), records the miss.
	_, err = executor.Lookup(context.Background(), req)
	require.ErrorIs(t, err, model.ErrNotFound)
	firstGetCalls := runner.getCalls

	// Second lookup: served by cache, no runner round-trip.
	_, err = executor.Lookup(context.Background(), req)
	require.ErrorIs(t, err, model.ErrNotFound)
	require.Equal(t, firstGetCalls, runner.getCalls,
		"runner.Get must not be called when negative cache memo is fresh")
}

func TestExecutorDirPageReadDirPlusCacheHit(t *testing.T) {
	runner := newFakeRunner()
	cache, err := dirpage.Open(dirpage.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	defer func() { _ = cache.Close() }()
	executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{10, 11, 12}}), WithDirPageCache(cache))
	require.NoError(t, err)

	mount := model.MountID("vol")
	parent := model.RootInode
	for _, name := range []string{"a", "b", "c"} {
		_, err := executor.Create(context.Background(), model.CreateRequest{
			Mount: mount, Parent: parent, Name: name, Attrs: model.CreateAttrs{Type: model.InodeTypeFile},
		})
		require.NoError(t, err)
	}

	req := model.ReadDirRequest{Mount: mount, Parent: parent, Limit: 100}

	// First call: runner Scan + BatchGet, then async materialize.
	first, err := executor.ReadDirPlus(context.Background(), req)
	require.NoError(t, err)
	require.Len(t, first, 3)
	scansAfterFirst := len(runner.scanVersions)

	// Second call: cache hit → no new Scan / BatchGet against the runner.
	second, err := executor.ReadDirPlus(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.Equal(t, scansAfterFirst, len(runner.scanVersions),
		"runner.Scan must not be called when dirpage cache hits")
}

func TestExecutorDirPageReadDirPlusCacheKeysPagination(t *testing.T) {
	runner := newFakeRunner()
	cache, err := dirpage.Open(dirpage.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	defer func() { _ = cache.Close() }()
	executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{10, 11, 12}}), WithDirPageCache(cache))
	require.NoError(t, err)

	mount := model.MountID("vol")
	parent := model.RootInode
	for _, name := range []string{"a", "b", "c"} {
		_, err := executor.Create(context.Background(), model.CreateRequest{
			Mount: mount, Parent: parent, Name: name, Attrs: model.CreateAttrs{Type: model.InodeTypeFile},
		})
		require.NoError(t, err)
	}

	// Materialize a non-leading page first. A later first-page request must not
	// reuse it just because both requests target the same parent directory.
	pageAfterA, err := executor.ReadDirPlus(context.Background(), model.ReadDirRequest{
		Mount: mount, Parent: parent, StartAfter: "a", Limit: 1,
	})
	require.NoError(t, err)
	require.Equal(t, "b", pageAfterA[0].Dentry.Name)

	firstPage, err := executor.ReadDirPlus(context.Background(), model.ReadDirRequest{
		Mount: mount, Parent: parent, Limit: 1,
	})
	require.NoError(t, err)
	require.Equal(t, "a", firstPage[0].Dentry.Name)
	require.Equal(t, parent, firstPage[0].Dentry.Parent)
}

func TestExecutorDirPageDecodeFailureFallsBackToRunner(t *testing.T) {
	runner := newFakeRunner()
	cache := &corruptDirPageCache{}
	executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{10}}), WithDirPageCache(cache))
	require.NoError(t, err)

	mount := model.MountID("vol")
	parent := model.RootInode
	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount: mount, Parent: parent, Name: "a", Attrs: model.CreateAttrs{Type: model.InodeTypeFile},
	})
	require.NoError(t, err)

	out, err := executor.ReadDirPlus(context.Background(), model.ReadDirRequest{
		Mount: mount, Parent: parent, Limit: 1,
	})
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Equal(t, "a", out[0].Dentry.Name)
	require.NotEmpty(t, runner.scanVersions, "corrupt derived cache must fall back to the runner")
}

func TestEncodeDirPageEntriesRejectsPartialMaterialization(t *testing.T) {
	_, err := encodeDirPageEntries([]model.DentryAttrPair{{
		Dentry: model.DentryRecord{Parent: 1, Name: "bad", Inode: 10, Type: model.InodeTypeFile},
		Inode: model.InodeRecord{
			Inode:       10,
			Type:        model.InodeTypeFile,
			OpaqueAttrs: make([]byte, model.MaxInodeOpaqueAttrsBytes+1),
		},
	}})
	require.Error(t, err)
}

func TestExecutorDirPageInvalidatedByCreate(t *testing.T) {
	runner := newFakeRunner()
	cache, err := dirpage.Open(dirpage.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	defer func() { _ = cache.Close() }()
	executor, err := newTestExecutor(runner, WithInodeAllocator(&fakeInodeAllocator{ids: []model.InodeID{99}}), WithDirPageCache(cache))
	require.NoError(t, err)

	mount := model.MountID("vol")
	parent := model.RootInode

	// Materialize an initial empty page set under frontier 0.
	_, err = executor.ReadDirPlus(context.Background(), model.ReadDirRequest{
		Mount: mount, Parent: parent, Limit: 10,
	})
	require.NoError(t, err)

	// Create a dentry; this must bump the dirpage epoch.
	_, err = executor.Create(context.Background(), model.CreateRequest{
		Mount: mount, Parent: parent, Name: "fresh", Attrs: model.CreateAttrs{Type: model.InodeTypeFile},
	})
	require.NoError(t, err)

	// Next ReadDirPlus must miss the cache (epoch advanced) and re-scan.
	scansBefore := len(runner.scanVersions)
	out, err := executor.ReadDirPlus(context.Background(), model.ReadDirRequest{
		Mount: mount, Parent: parent, Limit: 10,
	})
	require.NoError(t, err)
	require.Len(t, out, 1, "create must invalidate the cached empty page set")
	require.Greater(t, len(runner.scanVersions), scansBefore,
		"epoch bump must force a runner scan on the next ReadDirPlus")
}

func BenchmarkExecutorReadDirPlusFromVisibleView100(b *testing.B) {
	const entries = 100
	mount := testMountIdentityFor("vol")
	dentries := make([]model.DentryRecord, entries)
	overlayRows := make([]VisibleOverlayKV, 0, entries)
	for i := range entries {
		inode := model.InodeID(1_000 + i)
		dentries[i] = model.DentryRecord{
			Parent: model.RootInode,
			Name:   "file-" + strconv.Itoa(i),
			Inode:  inode,
			Type:   model.InodeTypeFile,
		}
		key, err := fsmeta.EncodeInodeKey(mount, inode)
		require.NoError(b, err)
		value, err := fsmeta.EncodeInodeValue(model.InodeRecord{
			Inode:     inode,
			Type:      model.InodeTypeFile,
			LinkCount: 1,
		})
		require.NoError(b, err)
		overlayRows = append(overlayRows, overlayValueForTest(key, value))
	}
	executor := &Executor{visibleCommitter: scanOverlayCommitter{
		values: overlayMapForTest(overlayRows...),
	}}

	b.ReportAllocs()
	for b.Loop() {
		pairs, ok, err := executor.readDirPlusFromVisibleView(mount, dentries)
		if err != nil {
			b.Fatal(err)
		}
		if !ok || len(pairs) != entries {
			b.Fatalf("unexpected Visible view result: ok=%v entries=%d", ok, len(pairs))
		}
	}
}

type sealedDirectoryCommitter struct {
	noopVisibleCommitter
	rows     []VisibleOverlayKV
	values   map[string]VisibleOverlayKV
	frontier uint64
}

func (c sealedDirectoryCommitter) GetVisibleOverlay(key []byte) ([]byte, bool, bool) {
	value, deleted, ok := c.GetVisibleOverlayView(key)
	if !ok {
		return nil, false, false
	}
	return append([]byte(nil), value...), deleted, true
}

func (c sealedDirectoryCommitter) GetVisibleOverlayView(key []byte) ([]byte, bool, bool) {
	row, ok := c.values[string(key)]
	if !ok {
		return nil, false, false
	}
	return row.Value, row.Delete, true
}

func (c sealedDirectoryCommitter) ScanVisibleOverlay(start []byte, limit uint32) []VisibleOverlayKV {
	return scanOverlayRowsForTest(c.rows, start, limit)
}

func (c sealedDirectoryCommitter) ScanVisibleDirectory(prefix, start []byte, limit uint32) []VisibleOverlayKV {
	rows := scanOverlayRowsForTest(c.rows, start, limit)
	out := rows[:0]
	for _, row := range rows {
		if !bytes.HasPrefix(row.Key, prefix) {
			continue
		}
		out = append(out, row)
	}
	return out
}

func (c sealedDirectoryCommitter) HasVisibleDirectoryOverlay(prefix []byte) bool {
	for _, row := range c.rows {
		if bytes.HasPrefix(row.Key, prefix) {
			return true
		}
	}
	return false
}

func (c sealedDirectoryCommitter) HasPendingVisibleDirectory([]byte) bool {
	return false
}

func (c sealedDirectoryCommitter) VisibleDirectoryCacheFrontier(prefix []byte) uint64 {
	if c.HasVisibleDirectoryOverlay(prefix) {
		return c.frontier
	}
	return 0
}

func scanOverlayRowsForTest(rows []VisibleOverlayKV, start []byte, limit uint32) []VisibleOverlayKV {
	out := make([]VisibleOverlayKV, 0, len(rows))
	for _, row := range rows {
		if bytes.Compare(row.Key, start) < 0 {
			continue
		}
		out = append(out, row)
		if uint32(len(out)) == limit {
			break
		}
	}
	return out
}
