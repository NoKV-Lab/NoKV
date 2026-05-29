// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package layout

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/feichai0017/NoKV/utils"
	"github.com/stretchr/testify/require"
)

var (
	testMount      = model.MountIdentity{MountID: "vol", MountKeyID: 1}
	testOtherMount = model.MountIdentity{MountID: "volume", MountKeyID: 2}
)

func TestKeyLayoutStable(t *testing.T) {
	key, err := EncodeInodeKey(testMount, 42)
	require.NoError(t, err)

	require.Equal(t, "66736d00010000000000000001000769000000000000002a", hex.EncodeToString(key))

	kind, err := KeyKindOf(key)
	require.NoError(t, err)
	require.Equal(t, KeyKindInode, kind)
}

func TestUsageKeyAllowsMountWideScope(t *testing.T) {
	key, err := EncodeUsageKey(testMount, 0)
	require.NoError(t, err)

	kind, err := KeyKindOf(key)
	require.NoError(t, err)
	require.Equal(t, KeyKindUsage, kind)
}

func TestSegmentCatalogIndexKeyUsesRootedMountAndBucket(t *testing.T) {
	var root [32]byte
	root[0] = 7
	key, err := EncodeSegmentCatalogIndexKey(testMount.MountKeyID, 3, root)
	require.NoError(t, err)

	kind, err := KeyKindOf(key)
	require.NoError(t, err)
	require.Equal(t, KeyKindSegment, kind)
	require.Equal(t, "segment", kind.String())

	parts, ok := InspectKey(key)
	require.True(t, ok)
	require.Equal(t, testMount.MountKeyID, parts.MountKeyID)
	require.Equal(t, AffinityBucket(3), parts.Bucket)
	require.Equal(t, SegmentRecordIndex, parts.SegmentRecord)
	require.Equal(t, root, parts.SegmentRoot)

	object, err := EncodeSegmentObjectKey(testMount.MountKeyID, 3, root)
	require.NoError(t, err)
	parts, ok = InspectKey(object)
	require.True(t, ok)
	require.Equal(t, SegmentRecordObject, parts.SegmentRecord)
	require.Equal(t, root, parts.SegmentRoot)
}

func TestSegmentCatalogIndexPrefixCoversCatalogKeys(t *testing.T) {
	var root [32]byte
	root[0] = 7
	key, err := EncodeSegmentCatalogIndexKey(testMount.MountKeyID, 3, root)
	require.NoError(t, err)
	prefix, err := EncodeSegmentCatalogIndexPrefix(testMount.MountKeyID, 3)
	require.NoError(t, err)

	require.True(t, bytes.HasPrefix(key, prefix))
	object, err := EncodeSegmentObjectKey(testMount.MountKeyID, 3, root)
	require.NoError(t, err)
	require.False(t, bytes.HasPrefix(object, prefix))
	otherBucketPrefix, err := EncodeSegmentCatalogIndexPrefix(testMount.MountKeyID, 4)
	require.NoError(t, err)
	require.False(t, bytes.HasPrefix(key, otherBucketPrefix))
}

func TestSnapshotKeyRoundTrip(t *testing.T) {
	key, err := EncodeSnapshotKey(testMount, model.RootInode, 42)
	require.NoError(t, err)

	kind, err := KeyKindOf(key)
	require.NoError(t, err)
	require.Equal(t, KeyKindSnapshot, kind)
	require.Equal(t, "snapshot", kind.String())

	parts, ok := InspectKey(key)
	require.True(t, ok)
	require.Equal(t, testMount.MountKeyID, parts.MountKeyID)
	require.Equal(t, RootAffinityBucket, parts.Bucket)
	require.Equal(t, KeyKindSnapshot, parts.Kind)
	require.Equal(t, model.RootInode, parts.SnapshotRoot)
	require.Equal(t, uint64(42), parts.SnapshotReadVersion)

	prefix, err := EncodeSnapshotPrefix(testMount)
	require.NoError(t, err)
	require.True(t, bytes.HasPrefix(key, prefix))
}

func TestAuxiliaryKeyEncoders(t *testing.T) {
	mount, err := EncodeMountKey(testMount)
	require.NoError(t, err)
	kind, err := KeyKindOf(mount)
	require.NoError(t, err)
	require.Equal(t, KeyKindMount, kind)
	require.Equal(t, "mount", kind.String())
	require.Equal(t, "unknown(122)", KeyKind('z').String())

	chunk, err := EncodeChunkKey(testMount, 22, 3)
	require.NoError(t, err)
	kind, err = KeyKindOf(chunk)
	require.NoError(t, err)
	require.Equal(t, KeyKindChunk, kind)
	require.Equal(t, "chunk", kind.String())
}

func TestBucketPrefixAndRangeCoverOneAffinityBucket(t *testing.T) {
	start, end, err := EncodeBucketRange(testMount, 3)
	require.NoError(t, err)
	require.NotEmpty(t, end)

	var inBucket model.InodeID
	for inode := model.InodeID(2); inode < 10_000; inode++ {
		if BucketForInodeID(inode) == 3 {
			inBucket = inode
			break
		}
	}
	require.NotZero(t, inBucket)
	key, err := EncodeInodeKey(testMount, inBucket)
	require.NoError(t, err)
	require.GreaterOrEqual(t, bytes.Compare(key, start), 0)
	require.Less(t, bytes.Compare(key, end), 0)
	bucket, ok := BucketOfKey(key)
	require.True(t, ok)
	require.Equal(t, AffinityBucket(3), bucket)
}

func TestMountPrefixAndRangeCoverOnlyOneMount(t *testing.T) {
	start, end, err := EncodeMountKeyRange(testMount)
	require.NoError(t, err)
	require.NotEmpty(t, end)

	inode, err := EncodeInodeKey(testMount, 42)
	require.NoError(t, err)
	dentry, err := EncodeDentryKey(testMount, 7, "name")
	require.NoError(t, err)
	other, err := EncodeInodeKey(testOtherMount, 42)
	require.NoError(t, err)

	require.True(t, bytes.HasPrefix(inode, start))
	require.True(t, bytes.HasPrefix(dentry, start))
	require.GreaterOrEqual(t, bytes.Compare(inode, start), 0)
	require.Less(t, bytes.Compare(inode, end), 0)
	require.GreaterOrEqual(t, bytes.Compare(dentry, start), 0)
	require.Less(t, bytes.Compare(dentry, end), 0)
	require.False(t, bytes.HasPrefix(other, start))
	require.GreaterOrEqual(t, bytes.Compare(other, end), 0)

	mountKey, ok := MountKeyIDOfKey(dentry)
	require.True(t, ok)
	require.Equal(t, model.MountKeyID(1), mountKey)
	name, ok := DentryNameOfKey(dentry)
	require.True(t, ok)
	require.Equal(t, "name", name)

	_, ok = MountKeyIDOfKey(start)
	require.False(t, ok)
}

func TestInspectKeyExtractsAuthorityParts(t *testing.T) {
	dentry, err := EncodeDentryKey(testMount, 7, "file")
	require.NoError(t, err)
	parts, ok := InspectKey(dentry)
	require.True(t, ok)
	require.Equal(t, testMount.MountKeyID, parts.MountKeyID)
	require.Equal(t, BucketForInodeID(7), parts.Bucket)
	require.Equal(t, KeyKindDentry, parts.Kind)
	require.Equal(t, model.InodeID(7), parts.Parent)

	inode, err := EncodeInodeKey(testMount, 42)
	require.NoError(t, err)
	parts, ok = InspectKey(inode)
	require.True(t, ok)
	require.Equal(t, KeyKindInode, parts.Kind)
	require.Equal(t, model.InodeID(42), parts.Inode)

	chunk, err := EncodeChunkKey(testMount, 42, 3)
	require.NoError(t, err)
	parts, ok = InspectKey(chunk)
	require.True(t, ok)
	require.Equal(t, KeyKindChunk, parts.Kind)
	require.Equal(t, model.InodeID(42), parts.Inode)

	session, err := EncodeSessionKey(testMount, 42, "writer")
	require.NoError(t, err)
	parts, ok = InspectKey(session)
	require.True(t, ok)
	require.Equal(t, KeyKindSession, parts.Kind)
	require.Equal(t, model.InodeID(42), parts.Inode)

	usage, err := EncodeUsageKey(testMount, 9)
	require.NoError(t, err)
	parts, ok = InspectKey(usage)
	require.True(t, ok)
	require.Equal(t, KeyKindUsage, parts.Kind)
	require.Equal(t, model.InodeID(9), parts.UsageScope)

	_, ok = InspectKey([]byte("bad"))
	require.False(t, ok)
}

func TestDentryPrefixGroupsDirectoryEntries(t *testing.T) {
	prefix, err := EncodeDentryPrefix(testMount, 9)
	require.NoError(t, err)
	a, err := EncodeDentryKey(testMount, 9, "a")
	require.NoError(t, err)
	b, err := EncodeDentryKey(testMount, 9, "b")
	require.NoError(t, err)
	otherParent, err := EncodeDentryKey(testMount, 10, "a")
	require.NoError(t, err)
	otherMountKey, err := EncodeDentryKey(testOtherMount, 9, "a")
	require.NoError(t, err)

	require.True(t, bytes.HasPrefix(a, prefix))
	require.True(t, bytes.HasPrefix(b, prefix))
	require.False(t, bytes.HasPrefix(otherParent, prefix))
	require.False(t, bytes.HasPrefix(otherMountKey, prefix))
}

func TestShardForUserKeyUsesGenericHash(t *testing.T) {
	const shards = 8
	key, err := EncodeInodeKey(testMount, 42)
	require.NoError(t, err)

	require.Equal(t, utils.ShardForUserKey(key, shards), ShardForUserKey(key, shards))
	require.Equal(t, 0, ShardForUserKey(key, 1))
}

func TestNameValidationRejectsUnsafePathComponents(t *testing.T) {
	for _, name := range []string{"", ".", "..", "a/b", "a\x00b"} {
		_, err := EncodeDentryKey(testMount, model.RootInode, name)
		require.ErrorIs(t, err, model.ErrInvalidName)
	}
}

func TestKeyKindOfRejectsInvalidKeys(t *testing.T) {
	_, err := KeyKindOf([]byte("not-fsmeta"))
	require.ErrorIs(t, err, ErrInvalidKey)

	key, err := encodeKey(testMount, RootAffinityBucket, KeyKind('z'), []byte("body"))
	require.NoError(t, err)
	_, err = KeyKindOf(key)
	require.ErrorIs(t, err, ErrInvalidKeyKind)
}

func findInodeOnBucket(t *testing.T, mount model.MountIdentity, bucket AffinityBucket) model.InodeID {
	t.Helper()
	for inode := model.InodeID(2); inode < 10_000; inode++ {
		key, err := EncodeInodeKey(mount, inode)
		require.NoError(t, err)
		got, ok := BucketOfKey(key)
		require.True(t, ok)
		if got == bucket {
			return inode
		}
	}
	t.Fatalf("no inode found for bucket %d", bucket)
	return 0
}
