// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"

	fsperas "github.com/feichai0017/NoKV/experimental/peras/exec"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/feichai0017/NoKV/local"
	entrykv "github.com/feichai0017/NoKV/txn/storage"
	"github.com/stretchr/testify/require"
)

func TestBuildMVCCSegmentInstallEntriesUsesOneInstallVersion(t *testing.T) {
	segment := fsmetaSegmentForTest(t)
	payload, err := fsperas.EncodePerasSegment(segment)
	require.NoError(t, err)
	digest, err := fsperas.PerasSegmentPayloadDigest(payload)
	require.NoError(t, err)

	entries, err := BuildMVCCSegmentInstallEntries(segment, 99)
	require.NoError(t, err)
	defer releaseMVCCReplayEntries(entries)

	objectKey, err := fsperas.PerasSegmentObjectKey(segment)
	require.NoError(t, err)
	indexKeys, err := fsperas.PerasSegmentCatalogIndexKeys(segment)
	require.NoError(t, err)
	indexFound := make(map[string]bool, len(indexKeys))
	var catalogFound bool
	for _, entry := range entries {
		require.Equal(t, uint64(99), entry.Version)
		cf, userKey, _, ok := entrykv.SplitInternalKey(entry.Key)
		require.True(t, ok)
		if cf != entrykv.CFDefault {
			continue
		}
		if bytes.Equal(userKey, objectKey) {
			catalogFound = true
			catalog, err := fsperas.DecodePerasSegmentCatalogRecord(entry.Value)
			require.NoError(t, err)
			require.Equal(t, segment.Root, catalog.Root)
			require.Equal(t, uint64(99), catalog.InstallVersion)
			require.Equal(t, uint64(len(segment.Completions)), catalog.CompletionCount)
			require.Equal(t, digest, catalog.SegmentPayloadDigest)
			require.Equal(t, uint64(len(payload)), catalog.SegmentPayloadSize)
			require.Equal(t, payload, catalog.SegmentPayload)
			continue
		}
		for _, indexKey := range indexKeys {
			if !bytes.Equal(userKey, indexKey) {
				continue
			}
			indexFound[string(indexKey)] = true
			index, err := fsperas.DecodePerasSegmentCatalogIndexRecord(entry.Value)
			require.NoError(t, err)
			require.Equal(t, segment.Root, index.Root)
			require.Equal(t, objectKey, index.ObjectKey)
		}
	}
	require.True(t, catalogFound)
	require.Len(t, indexFound, len(indexKeys))
}

func TestBuildMVCCSegmentCatalogInstallEntriesStoresObjectAndBucketIndexes(t *testing.T) {
	segment := fsmetaSegmentForTest(t)
	payload, err := fsperas.EncodePerasSegment(segment)
	require.NoError(t, err)
	digest, err := fsperas.PerasSegmentPayloadDigest(payload)
	require.NoError(t, err)

	entries, err := BuildMVCCSegmentCatalogInstallEntries(segment, 99)
	require.NoError(t, err)
	defer releaseMVCCReplayEntries(entries)
	indexKeys, err := fsperas.PerasSegmentCatalogIndexKeys(segment)
	require.NoError(t, err)
	objectKey, err := fsperas.PerasSegmentObjectKey(segment)
	require.NoError(t, err)
	require.Len(t, entries, len(indexKeys)+1)

	var objectFound bool
	indexFound := make(map[string]bool, len(indexKeys))
	for _, entry := range entries {
		cf, userKey, _, ok := entrykv.SplitInternalKey(entry.Key)
		require.True(t, ok)
		require.Equal(t, entrykv.CFDefault, cf)
		if bytes.Equal(userKey, objectKey) {
			objectFound = true
			catalog, err := fsperas.DecodePerasSegmentCatalogRecord(entry.Value)
			require.NoError(t, err)
			require.Equal(t, segment.Root, catalog.Root)
			require.Equal(t, digest, catalog.SegmentPayloadDigest)
			require.Equal(t, uint64(len(payload)), catalog.SegmentPayloadSize)
			decoded, err := fsperas.VerifyPerasSegmentPayload(catalog.SegmentPayload, segment.Root, digest)
			require.NoError(t, err)
			require.Equal(t, segment.Stats(), decoded.Stats())
			continue
		}
		for _, indexKey := range indexKeys {
			if !bytes.Equal(userKey, indexKey) {
				continue
			}
			indexFound[string(indexKey)] = true
			index, err := fsperas.DecodePerasSegmentCatalogIndexRecord(entry.Value)
			require.NoError(t, err)
			require.Equal(t, segment.Root, index.Root)
			require.Equal(t, objectKey, index.ObjectKey)
		}
	}
	require.True(t, objectFound)
	require.Len(t, indexFound, len(indexKeys))
}

func TestBuildMVCCSegmentCatalogInstallEntriesForObjectKeyStoresPayloadOnce(t *testing.T) {
	segment := fsmetaMultiBucketSegmentForTest(t)
	payload, err := fsperas.EncodePerasSegment(segment)
	require.NoError(t, err)
	digest, err := fsperas.PerasSegmentPayloadDigest(payload)
	require.NoError(t, err)
	objectKeys, err := fsperas.PerasSegmentCatalogObjectKeys(segment)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(objectKeys), 2)
	canonicalObjectKey, err := fsperas.PerasSegmentObjectKey(segment)
	require.NoError(t, err)
	require.Equal(t, objectKeys[0], canonicalObjectKey)

	canonicalEntries, err := BuildMVCCSegmentCatalogInstallEntriesWithPayloadForObjectKey(segment, 99, payload, digest, canonicalObjectKey)
	require.NoError(t, err)
	defer releaseMVCCReplayEntries(canonicalEntries)
	require.Len(t, canonicalEntries, 2)
	require.True(t, entriesContainUserKey(canonicalEntries, canonicalObjectKey))

	nonCanonicalEntries, err := BuildMVCCSegmentCatalogInstallEntriesWithPayloadForObjectKey(segment, 99, payload, digest, objectKeys[1])
	require.NoError(t, err)
	defer releaseMVCCReplayEntries(nonCanonicalEntries)
	require.Len(t, nonCanonicalEntries, 1)
	require.False(t, entriesContainUserKey(nonCanonicalEntries, objectKeys[1]))

	canonicalIndex := decodeOnlyCatalogIndex(t, canonicalEntries)
	require.Equal(t, canonicalObjectKey, canonicalIndex.ObjectKey)
	nonCanonicalIndex := decodeOnlyCatalogIndex(t, nonCanonicalEntries)
	require.Equal(t, canonicalObjectKey, nonCanonicalIndex.ObjectKey)
}

func TestBuildMVCCSegmentInstallEntriesRequiresFSMetaKeys(t *testing.T) {
	segment, err := fsperas.BuildPerasSegmentFromReplayPlan(fsperas.ReplayPlan{
		EpochID: 1,
		Operations: []fsperas.ReplayOperation{{
			OpID: testPerasInstallOpID("client-a", 1),
			Mutations: []fsperas.ReplayMutation{{
				Key:   []byte("raw-key"),
				Value: []byte("value"),
			}},
		}},
	})
	require.NoError(t, err)

	entries, err := BuildMVCCSegmentInstallEntries(segment, 99)
	require.ErrorIs(t, err, fsperas.ErrInvalidPerasSegment)
	require.Empty(t, entries)
}

func BenchmarkBuildMVCCSegmentCatalogInstallEntries1000(b *testing.B) {
	segment, err := fsperas.BuildPerasSegmentFromReplayPlan(workspaceInstallReplayPlan(b, 1000))
	if err != nil {
		b.Fatal(err)
	}
	payload, err := fsperas.EncodePerasSegment(segment)
	if err != nil {
		b.Fatal(err)
	}
	digest, err := fsperas.PerasSegmentPayloadDigest(payload)
	if err != nil {
		b.Fatal(err)
	}
	catalogKeys, err := fsperas.PerasSegmentCatalogIndexKeys(segment)
	if err != nil {
		b.Fatal(err)
	}
	wantEntries := len(catalogKeys) + 1

	b.ReportAllocs()
	for b.Loop() {
		entries, err := BuildMVCCSegmentCatalogInstallEntriesWithPayload(segment, 99, payload, digest)
		if err != nil {
			b.Fatal(err)
		}
		if len(entries) != wantEntries {
			b.Fatalf("unexpected catalog entry count %d", len(entries))
		}
		releaseMVCCReplayEntries(entries)
	}
}

func BenchmarkBuildMVCCSegmentCatalogInstallEntriesForObjectKey1000(b *testing.B) {
	segment, err := fsperas.BuildPerasSegmentFromReplayPlan(workspaceInstallReplayPlan(b, 1000))
	if err != nil {
		b.Fatal(err)
	}
	payload, err := fsperas.EncodePerasSegment(segment)
	if err != nil {
		b.Fatal(err)
	}
	digest, err := fsperas.PerasSegmentPayloadDigest(payload)
	if err != nil {
		b.Fatal(err)
	}
	objectKey, err := fsperas.PerasSegmentObjectKey(segment)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	for b.Loop() {
		entries, err := BuildMVCCSegmentCatalogInstallEntriesWithPayloadForObjectKey(segment, 99, payload, digest, objectKey)
		if err != nil {
			b.Fatal(err)
		}
		if len(entries) != 2 {
			b.Fatalf("unexpected route entry count %d", len(entries))
		}
		releaseMVCCReplayEntries(entries)
	}
}

func BenchmarkBuildMVCCSegmentCatalogInstallEntriesForNonCanonicalObjectKey1000(b *testing.B) {
	segment, err := fsperas.BuildPerasSegmentFromReplayPlan(workspaceInstallReplayPlan(b, 1000))
	if err != nil {
		b.Fatal(err)
	}
	payload, err := fsperas.EncodePerasSegment(segment)
	if err != nil {
		b.Fatal(err)
	}
	digest, err := fsperas.PerasSegmentPayloadDigest(payload)
	if err != nil {
		b.Fatal(err)
	}
	objectKeys, err := fsperas.PerasSegmentCatalogObjectKeys(segment)
	if err != nil {
		b.Fatal(err)
	}
	if len(objectKeys) < 2 {
		b.Skip("segment did not span multiple buckets")
	}
	objectKey := objectKeys[1]

	b.ReportAllocs()
	for b.Loop() {
		entries, err := BuildMVCCSegmentCatalogInstallEntriesWithPayloadForObjectKey(segment, 99, payload, digest, objectKey)
		if err != nil {
			b.Fatal(err)
		}
		if len(entries) != 1 {
			b.Fatalf("unexpected route entry count %d", len(entries))
		}
		releaseMVCCReplayEntries(entries)
	}
}

func BenchmarkBuildMVCCSegmentCatalogIndexInstallEntries1000(b *testing.B) {
	segment, err := fsperas.BuildPerasSegmentFromReplayPlan(workspaceInstallReplayPlan(b, 1000))
	if err != nil {
		b.Fatal(err)
	}
	payload, err := fsperas.EncodePerasSegment(segment)
	if err != nil {
		b.Fatal(err)
	}
	digest, err := fsperas.PerasSegmentPayloadDigest(payload)
	if err != nil {
		b.Fatal(err)
	}
	objectKeys, err := fsperas.PerasSegmentCatalogObjectKeys(segment)
	if err != nil {
		b.Fatal(err)
	}
	if len(objectKeys) < 2 {
		b.Skip("segment did not span multiple buckets")
	}
	canonicalObjectKey, err := fsperas.PerasSegmentObjectKey(segment)
	if err != nil {
		b.Fatal(err)
	}
	routeKey := objectKeys[1]

	b.ReportAllocs()
	for b.Loop() {
		entries, err := buildMVCCSegmentCatalogIndexInstallEntries(segment.Root, digest, segment.EpochID, 99, uint64(len(payload)), routeKey, canonicalObjectKey)
		if err != nil {
			b.Fatal(err)
		}
		if len(entries) != 1 {
			b.Fatalf("unexpected route entry count %d", len(entries))
		}
		releaseMVCCReplayEntries(entries)
	}
}

func BenchmarkBuildMVCCSegmentMaterializationEntries1000(b *testing.B) {
	segment, err := fsperas.BuildPerasSegmentFromReplayPlan(workspaceInstallReplayPlan(b, 1000))
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	for b.Loop() {
		entries, err := BuildMVCCSegmentInstallEntries(segment, 99)
		if err != nil {
			b.Fatal(err)
		}
		if len(entries) <= 1 {
			b.Fatalf("unexpected materialized entry count %d", len(entries))
		}
		releaseMVCCReplayEntries(entries)
	}
}

func openPerasReplayDB(t *testing.T) *local.DB {
	t.Helper()
	opt := local.NewDefaultOptions()
	opt.WorkDir = filepath.Join(t.TempDir(), "db")
	opt.MemTableSize = 1 << 12
	db, err := local.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func fsmetaSegmentForTest(t *testing.T) fsperas.PerasSegment {
	t.Helper()
	mount := model.MountIdentity{MountID: "vol", MountKeyID: 1}
	dentryA, err := layout.EncodeDentryKey(mount, model.RootInode, "a")
	require.NoError(t, err)
	dentryB, err := layout.EncodeDentryKey(mount, model.RootInode, "b")
	require.NoError(t, err)
	inodeA, err := layout.EncodeInodeKey(mount, 7)
	require.NoError(t, err)
	segment, err := fsperas.BuildPerasSegmentFromReplayPlan(fsperas.ReplayPlan{
		EpochID: 1,
		Operations: []fsperas.ReplayOperation{
			{
				OpID: testPerasInstallOpID("client-a", 1),
				Kind: model.OperationCreate,
				Mutations: []fsperas.ReplayMutation{
					{Key: dentryA, Value: []byte("inode=7")},
					{Key: inodeA, Value: []byte("attrs")},
				},
			},
			{
				OpID: testPerasInstallOpID("client-b", 1),
				Kind: model.OperationCreate,
				Mutations: []fsperas.ReplayMutation{
					{Key: dentryB, Value: []byte("inode=8")},
				},
			},
		},
	})
	require.NoError(t, err)
	return segment
}

func fsmetaMultiBucketSegmentForTest(t *testing.T) fsperas.PerasSegment {
	t.Helper()
	mount := model.MountIdentity{MountID: "vol", MountKeyID: 1}
	var inode model.InodeID
	for candidate := model.InodeID(2); candidate < 1_000_000; candidate++ {
		if layout.BucketForInodeID(candidate) != layout.RootAffinityBucket {
			inode = candidate
			break
		}
	}
	require.NotZero(t, inode)
	dentry, err := layout.EncodeDentryKey(mount, model.RootInode, "artifact")
	require.NoError(t, err)
	inodeKey, err := layout.EncodeInodeKey(mount, inode)
	require.NoError(t, err)
	segment, err := fsperas.BuildPerasSegmentFromReplayPlan(fsperas.ReplayPlan{
		EpochID: 1,
		Operations: []fsperas.ReplayOperation{{
			OpID: testPerasInstallOpID("client-a", 1),
			Kind: model.OperationCreate,
			Mutations: []fsperas.ReplayMutation{
				{Key: dentry, Value: []byte("inode")},
				{Key: inodeKey, Value: []byte("attrs")},
			},
		}},
	})
	require.NoError(t, err)
	return segment
}

func entriesContainUserKey(entries []*entrykv.Entry, want []byte) bool {
	for _, entry := range entries {
		cf, userKey, _, ok := entrykv.SplitInternalKey(entry.Key)
		if ok && cf == entrykv.CFDefault && bytes.Equal(userKey, want) {
			return true
		}
	}
	return false
}

func decodeOnlyCatalogIndex(t *testing.T, entries []*entrykv.Entry) fsperas.SegmentCatalogIndexRecord {
	t.Helper()
	var record fsperas.SegmentCatalogIndexRecord
	var found bool
	for _, entry := range entries {
		cf, userKey, _, ok := entrykv.SplitInternalKey(entry.Key)
		require.True(t, ok)
		if cf != entrykv.CFDefault {
			continue
		}
		parts, ok := layout.InspectKey(userKey)
		if !ok || parts.Kind != layout.KeyKindSegment || parts.SegmentRecord != layout.SegmentRecordIndex {
			continue
		}
		next, err := fsperas.DecodePerasSegmentCatalogIndexRecord(entry.Value)
		require.NoError(t, err)
		require.False(t, found)
		record = next
		found = true
	}
	require.True(t, found)
	return record
}

func testPerasInstallOpID(client string, seq uint64) fsperas.OperationID {
	return fsperas.OperationID{ClientID: client, Seq: seq}
}

func workspaceInstallReplayPlan(tb testing.TB, count int) fsperas.ReplayPlan {
	tb.Helper()
	mount := model.MountIdentity{MountID: "workspace", MountKeyID: 42}
	ops := make([]fsperas.ReplayOperation, 0, count)
	for i := range count {
		name := fmt.Sprintf("checkpoint-%06d", i)
		dentry, err := layout.EncodeDentryKey(mount, model.RootInode, name)
		require.NoError(tb, err)
		inodeKey, err := layout.EncodeInodeKey(mount, model.InodeID(1000+i))
		require.NoError(tb, err)
		ops = append(ops, fsperas.ReplayOperation{
			OpID: testPerasInstallOpID("workspace-writer", uint64(i+1)),
			Kind: model.OperationCreate,
			Mutations: []fsperas.ReplayMutation{
				{Key: dentry, Value: []byte("inode")},
				{Key: inodeKey, Value: []byte("attrs")},
			},
		})
	}
	return fsperas.ReplayPlan{
		EpochID:    11,
		Versions:   fsperas.ReplayVersionRange{First: 1000, Count: uint64(count)},
		Operations: ops,
	}
}
