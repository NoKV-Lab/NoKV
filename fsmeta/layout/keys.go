// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package layout

import (
	"encoding/binary"
	"fmt"

	"github.com/feichai0017/NoKV/fsmeta/model"
)

// fsmeta key layout:
//
//	common prefix:
//	  magic[4] = "fsm\0"
//	  version  = 0x01
//	  mount_key_id be64
//	  affinity_bucket be16
//	  kind byte
//
//	kind bodies:
//	  mount   'm' : empty
//	  inode   'i' : inode be64
//	  dentry  'd' : parent inode be64 | name bytes
//	  chunk   'c' : inode be64 | chunk index be64
//	  session 's' : inode be64 | 0x01 | session bytes, or inode be64 | 0x00 for writer ownership
//	  usage   'u' : quota scope inode be64; scope 0 is mount-wide usage
//	  segment 'p' : record-kind byte | sealed segment root sha256
//	  snapshot 'x' : root inode be64 | read version be64
//
// Big-endian integer fields preserve numeric order inside each key family.
var keyMagic = []byte{'f', 's', 'm', 0}

const keySchemaVersion byte = 1

const (
	DefaultAffinityBucketCount = 16
	RootAffinityBucket         = AffinityBucket(0)
	encodedMountKeyBytes       = 8
	encodedBucketBytes         = 2
)

type AffinityBucket uint16

// KeyKind classifies one fsmeta key family.
type KeyKind byte

const (
	KeyKindMount    KeyKind = 'm'
	KeyKindInode    KeyKind = 'i'
	KeyKindDentry   KeyKind = 'd'
	KeyKindChunk    KeyKind = 'c'
	KeyKindSession  KeyKind = 's'
	KeyKindUsage    KeyKind = 'u'
	KeyKindSegment  KeyKind = 'p'
	KeyKindSnapshot KeyKind = 'x'
)

const (
	SegmentRecordObject byte = 'o'
	SegmentRecordIndex  byte = 'i'
)

type KeyParts struct {
	MountKeyID          model.MountKeyID
	Bucket              AffinityBucket
	Kind                KeyKind
	Parent              model.InodeID
	Inode               model.InodeID
	UsageScope          model.InodeID
	SegmentRecord       byte
	SegmentRoot         [32]byte
	SnapshotRoot        model.InodeID
	SnapshotReadVersion uint64
}

func (k KeyKind) String() string {
	switch k {
	case KeyKindMount:
		return "mount"
	case KeyKindInode:
		return "inode"
	case KeyKindDentry:
		return "dentry"
	case KeyKindChunk:
		return "chunk"
	case KeyKindSession:
		return "session"
	case KeyKindUsage:
		return "usage"
	case KeyKindSegment:
		return "segment"
	case KeyKindSnapshot:
		return "snapshot"
	default:
		return fmt.Sprintf("unknown(%d)", byte(k))
	}
}

// EncodeMountKey returns the mount-level metadata record key.
func EncodeMountKey(mount model.MountIdentity) ([]byte, error) {
	return encodeKey(mount, RootAffinityBucket, KeyKindMount, nil)
}

// EncodeMountPrefix returns the common key prefix for one mount. All fsmeta
// records under the mount share this prefix regardless of key kind.
func EncodeMountPrefix(mount model.MountIdentity) ([]byte, error) {
	return encodeMountPrefix(mount, 0)
}

// EncodeMountKeyRange returns the half-open key range covering all fsmeta
// records under one mount.
func EncodeMountKeyRange(mount model.MountIdentity) (start, end []byte, err error) {
	start, err = EncodeMountPrefix(mount)
	if err != nil {
		return nil, nil, err
	}
	return start, prefixUpperBound(start), nil
}

// EncodeBucketPrefix returns the key prefix for one mount-local affinity
// bucket. Coordinator placement can split these byte ranges without learning
// fsmeta's record families.
func EncodeBucketPrefix(mount model.MountIdentity, bucket AffinityBucket) ([]byte, error) {
	prefix, err := encodeMountPrefix(mount, encodedBucketBytes)
	if err != nil {
		return nil, err
	}
	return appendBucket(prefix, bucket), nil
}

func EncodeBucketRange(mount model.MountIdentity, bucket AffinityBucket) (start, end []byte, err error) {
	start, err = EncodeBucketPrefix(mount, bucket)
	if err != nil {
		return nil, nil, err
	}
	return start, prefixUpperBound(start), nil
}

// EncodeInodeKey returns the inode attribute record key.
func EncodeInodeKey(mount model.MountIdentity, inode model.InodeID) ([]byte, error) {
	if err := model.ValidateInodeID(inode); err != nil {
		return nil, err
	}
	if err := model.ValidateMountIdentity(mount); err != nil {
		return nil, err
	}
	out := encodeKeyPrefixForMountKeyID(mount.MountKeyID, BucketForInodeID(inode), KeyKindInode, 8)
	return binary.BigEndian.AppendUint64(out, uint64(inode)), nil
}

// EncodeDentryPrefix returns the scan prefix for entries directly under parent.
func EncodeDentryPrefix(mount model.MountIdentity, parent model.InodeID) ([]byte, error) {
	if err := model.ValidateInodeID(parent); err != nil {
		return nil, err
	}
	var body [8]byte
	binary.BigEndian.PutUint64(body[:], uint64(parent))
	return encodeKey(mount, BucketForInodeID(parent), KeyKindDentry, body[:])
}

// EncodeDentryKey returns the dentry record key for parent/name.
func EncodeDentryKey(mount model.MountIdentity, parent model.InodeID, name string) ([]byte, error) {
	if err := model.ValidateName(name); err != nil {
		return nil, err
	}
	if err := model.ValidateInodeID(parent); err != nil {
		return nil, err
	}
	if err := model.ValidateMountIdentity(mount); err != nil {
		return nil, err
	}
	out := encodeKeyPrefixForMountKeyID(mount.MountKeyID, BucketForInodeID(parent), KeyKindDentry, 8+len(name))
	out = binary.BigEndian.AppendUint64(out, uint64(parent))
	return append(out, name...), nil
}

// EncodeChunkKey returns the chunk mapping record key for inode/chunk.
func EncodeChunkKey(mount model.MountIdentity, inode model.InodeID, chunk model.ChunkIndex) ([]byte, error) {
	if err := model.ValidateInodeID(inode); err != nil {
		return nil, err
	}
	var body [16]byte
	binary.BigEndian.PutUint64(body[:8], uint64(inode))
	binary.BigEndian.PutUint64(body[8:], uint64(chunk))
	return encodeKey(mount, BucketForInodeID(inode), KeyKindChunk, body[:])
}

// EncodeSessionKey returns the client/session state key.
func EncodeSessionKey(mount model.MountIdentity, inode model.InodeID, session model.SessionID) ([]byte, error) {
	if err := model.ValidateInodeID(inode); err != nil {
		return nil, err
	}
	if err := model.ValidateSessionID(session); err != nil {
		return nil, err
	}
	body := make([]byte, 9, 9+len(session))
	binary.BigEndian.PutUint64(body[:8], uint64(inode))
	body[8] = 0x01
	body = append(body, string(session)...)
	return encodeKey(mount, BucketForInodeID(inode), KeyKindSession, body)
}

// EncodeSessionBucketPrefix returns the prefix for all session records in one
// mount-local affinity bucket. Maintenance scanners use this bucket-local
// prefix instead of the mount prefix so routed scans stay inside real fsmeta
// data ranges.
func EncodeSessionBucketPrefix(mount model.MountIdentity, bucket AffinityBucket) ([]byte, error) {
	return encodeKey(mount, bucket, KeyKindSession, nil)
}

// EncodeInodeSessionKey returns the exclusive writer-owner key for one inode.
func EncodeInodeSessionKey(mount model.MountIdentity, inode model.InodeID) ([]byte, error) {
	if err := model.ValidateInodeID(inode); err != nil {
		return nil, err
	}
	var body [9]byte
	binary.BigEndian.PutUint64(body[:8], uint64(inode))
	body[8] = 0x00
	return encodeKey(mount, BucketForInodeID(inode), KeyKindSession, body[:])
}

// EncodeUsageKey returns a quota usage/counter key. Scope 0 is mount-wide;
// non-zero scopes are direct quota accounting roots.
func EncodeUsageKey(mount model.MountIdentity, scope model.InodeID) ([]byte, error) {
	var body [8]byte
	binary.BigEndian.PutUint64(body[:], uint64(scope))
	return encodeKey(mount, BucketForInodeID(scope), KeyKindUsage, body[:])
}

// EncodeSnapshotKey returns the hidden local snapshot-retention record key.
// Snapshot records intentionally live in the root affinity bucket so one local
// runtime can recover all active snapshot tokens without scanning every fsmeta
// affinity bucket.
func EncodeSnapshotKey(mount model.MountIdentity, root model.InodeID, readVersion uint64) ([]byte, error) {
	if err := model.ValidateInodeID(root); err != nil {
		return nil, err
	}
	if readVersion == 0 {
		return nil, model.ErrInvalidRequest
	}
	var body [16]byte
	binary.BigEndian.PutUint64(body[:8], uint64(root))
	binary.BigEndian.PutUint64(body[8:], readVersion)
	return encodeKey(mount, RootAffinityBucket, KeyKindSnapshot, body[:])
}

// EncodeSnapshotPrefix returns the hidden local snapshot-retention key prefix.
func EncodeSnapshotPrefix(mount model.MountIdentity) ([]byte, error) {
	return encodeKey(mount, RootAffinityBucket, KeyKindSnapshot, nil)
}

// EncodeSegmentCatalogIndexKey returns the hidden per-bucket catalog key that
// points reads at one durable segment object. It deliberately takes the rooted
// mount key id rather than a human mount name because recovery reconstructs it
// from already-encoded fsmeta keys.
func EncodeSegmentCatalogIndexKey(mountKeyID model.MountKeyID, bucket AffinityBucket, root [32]byte) ([]byte, error) {
	return encodeSegmentKey(mountKeyID, bucket, SegmentRecordIndex, root)
}

// EncodeSegmentObjectKey returns the hidden segment-object key that stores
// one copy of a sealed segment payload. Per-bucket catalog keys point at this
// object so multi-bucket segments do not duplicate payload bytes.
func EncodeSegmentObjectKey(mountKeyID model.MountKeyID, bucket AffinityBucket, root [32]byte) ([]byte, error) {
	return encodeSegmentKey(mountKeyID, bucket, SegmentRecordObject, root)
}

func encodeSegmentKey(mountKeyID model.MountKeyID, bucket AffinityBucket, record byte, root [32]byte) ([]byte, error) {
	if err := model.ValidateMountKeyID(mountKeyID); err != nil {
		return nil, err
	}
	if root == ([32]byte{}) {
		return nil, ErrInvalidKey
	}
	switch record {
	case SegmentRecordObject, SegmentRecordIndex:
	default:
		return nil, ErrInvalidKey
	}
	body := make([]byte, 1+len(root))
	body[0] = record
	copy(body[1:], root[:])
	return encodeKeyForMountKeyID(mountKeyID, bucket, KeyKindSegment, body), nil
}

// EncodeSegmentCatalogIndexPrefix returns the hidden catalog scan prefix for
// durable segment indexes in one mount-local affinity bucket.
func EncodeSegmentCatalogIndexPrefix(mountKeyID model.MountKeyID, bucket AffinityBucket) ([]byte, error) {
	if err := model.ValidateMountKeyID(mountKeyID); err != nil {
		return nil, err
	}
	return encodeKeyForMountKeyID(mountKeyID, bucket, KeyKindSegment, []byte{SegmentRecordIndex}), nil
}

// KeyKindOf returns the kind byte encoded in a fsmeta key.
func KeyKindOf(key []byte) (KeyKind, error) {
	_, pos, err := decodeHeaderParts(key)
	if err != nil {
		return 0, err
	}
	if pos >= len(key) {
		return 0, ErrInvalidKey
	}
	kind := KeyKind(key[pos])
	switch kind {
	case KeyKindMount, KeyKindInode, KeyKindDentry, KeyKindChunk, KeyKindSession, KeyKindUsage, KeyKindSegment, KeyKindSnapshot:
		return kind, nil
	default:
		return 0, ErrInvalidKeyKind
	}
}

// DentryNameOfKey extracts the final name component from a concrete dentry
// storage key. It is intended for watch diagnostics that receive committed
// storage keys but must not know the mount_key_id that produced them.
func DentryNameOfKey(key []byte) (string, bool) {
	name, ok := DentryNameBytesOfKey(key)
	if !ok {
		return "", false
	}
	return string(name), true
}

// DentryNameBytesOfKey extracts the final name component from a concrete
// dentry key without allocating. The returned bytes alias key and must be
// treated as immutable by callers that keep derived views.
func DentryNameBytesOfKey(key []byte) ([]byte, bool) {
	_, kindPos, err := decodeHeaderParts(key)
	if err != nil || kindPos >= len(key) || KeyKind(key[kindPos]) != KeyKindDentry {
		return nil, false
	}
	body := key[kindPos+1:]
	if len(body) <= 8 {
		return nil, false
	}
	return body[8:], true
}

// MountKeyIDOfKey returns the rooted storage mount identity encoded in a full
// fsmeta key.
func MountKeyIDOfKey(key []byte) (model.MountKeyID, bool) {
	mount, _, err := decodeHeaderParts(key)
	if err != nil {
		return 0, false
	}
	return mount, true
}

func BucketOfKey(key []byte) (AffinityBucket, bool) {
	_, pos, err := decodeMountPrefix(key)
	if err != nil || len(key)-pos < encodedBucketBytes+1 {
		return 0, false
	}
	return AffinityBucket(binary.BigEndian.Uint16(key[pos : pos+encodedBucketBytes])), true
}

func InspectKey(key []byte) (KeyParts, bool) {
	mount, bucketPos, err := decodeMountPrefix(key)
	if err != nil || len(key)-bucketPos < encodedBucketBytes+1 {
		return KeyParts{}, false
	}
	kindPos := bucketPos + encodedBucketBytes
	kind := KeyKind(key[kindPos])
	parts := KeyParts{
		MountKeyID: mount,
		Bucket:     AffinityBucket(binary.BigEndian.Uint16(key[bucketPos:kindPos])),
		Kind:       kind,
	}
	body := key[kindPos+1:]
	switch kind {
	case KeyKindMount:
		return parts, len(body) == 0
	case KeyKindDentry:
		if len(body) <= 8 {
			return KeyParts{}, false
		}
		parts.Parent = model.InodeID(binary.BigEndian.Uint64(body[:8]))
		return parts, true
	case KeyKindInode:
		if len(body) != 8 {
			return KeyParts{}, false
		}
		parts.Inode = model.InodeID(binary.BigEndian.Uint64(body[:8]))
		return parts, true
	case KeyKindChunk:
		if len(body) != 16 {
			return KeyParts{}, false
		}
		parts.Inode = model.InodeID(binary.BigEndian.Uint64(body[:8]))
		return parts, true
	case KeyKindSession:
		if len(body) < 9 {
			return KeyParts{}, false
		}
		parts.Inode = model.InodeID(binary.BigEndian.Uint64(body[:8]))
		return parts, true
	case KeyKindUsage:
		if len(body) != 8 {
			return KeyParts{}, false
		}
		parts.UsageScope = model.InodeID(binary.BigEndian.Uint64(body[:8]))
		return parts, true
	case KeyKindSegment:
		if len(body) != 1+len(parts.SegmentRoot) {
			return KeyParts{}, false
		}
		switch body[0] {
		case SegmentRecordObject, SegmentRecordIndex:
			parts.SegmentRecord = body[0]
		default:
			return KeyParts{}, false
		}
		copy(parts.SegmentRoot[:], body[1:])
		return parts, true
	case KeyKindSnapshot:
		if len(body) != 16 {
			return KeyParts{}, false
		}
		parts.SnapshotRoot = model.InodeID(binary.BigEndian.Uint64(body[:8]))
		parts.SnapshotReadVersion = binary.BigEndian.Uint64(body[8:])
		return parts, parts.SnapshotRoot != 0 && parts.SnapshotReadVersion != 0
	default:
		return KeyParts{}, false
	}
}

// MountKeyResolver adapts fsmeta keys to raftstore MVCC GC mount-scoped
// retention policy.
func MountKeyResolver(key []byte) (uint64, bool) {
	mount, ok := MountKeyIDOfKey(key)
	return uint64(mount), ok
}

// HashBucketForKey returns the hash affinity bucket for one encoded fsmeta key.
// It is used for diagnostics and synthetic placement tests only; physical
// atomicity is owned by storage/kv.ApplyBatch.
func HashBucketForKey(key []byte, bucketCount int) int {
	bucketCount = NormalizeAffinityBucketCount(bucketCount)
	if bucketCount <= 1 {
		return 0
	}
	if len(key) == 0 {
		return 0
	}
	return int(fnv1a32(key)) & (bucketCount - 1)
}

// NormalizeAffinityBucketCount returns a power-of-two fsmeta affinity bucket count.
// Non-positive and single-bucket configurations collapse to 1.
func NormalizeAffinityBucketCount(buckets int) int {
	if buckets <= 1 {
		return 1
	}
	out := 1
	for out*2 <= buckets {
		out *= 2
	}
	return out
}

func BucketForInodeID(inode model.InodeID) AffinityBucket {
	if inode == model.RootInode {
		return RootAffinityBucket
	}
	var body [8]byte
	binary.BigEndian.PutUint64(body[:], uint64(inode))
	return AffinityBucket(HashBucketForKey(body[:], DefaultAffinityBucketCount))
}

func ChooseWorkspaceBucket(mount model.MountIdentity, name string) AffinityBucket {
	var mountBuf [8]byte
	binary.BigEndian.PutUint64(mountBuf[:], uint64(mount.MountKeyID))
	key := make([]byte, 0, len(mountBuf)+1+len(name))
	key = append(key, mountBuf[:]...)
	key = append(key, 0)
	key = append(key, name...)
	return AffinityBucket(HashBucketForKey(key, DefaultAffinityBucketCount))
}

func fnv1a32(b []byte) uint32 {
	var h uint32 = 2166136261
	for _, c := range b {
		h ^= uint32(c)
		h *= 16777619
	}
	return h
}

func encodeKey(mount model.MountIdentity, bucket AffinityBucket, kind KeyKind, body []byte) ([]byte, error) {
	if err := model.ValidateMountIdentity(mount); err != nil {
		return nil, err
	}
	return encodeKeyForMountKeyID(mount.MountKeyID, bucket, kind, body), nil
}

func encodeKeyForMountKeyID(mountKeyID model.MountKeyID, bucket AffinityBucket, kind KeyKind, body []byte) []byte {
	out := encodeKeyPrefixForMountKeyID(mountKeyID, bucket, kind, len(body))
	out = append(out, body...)
	return out
}

func encodeKeyPrefixForMountKeyID(mountKeyID model.MountKeyID, bucket AffinityBucket, kind KeyKind, bodyLen int) []byte {
	out := make([]byte, 0, len(keyMagic)+1+encodedMountKeyBytes+encodedBucketBytes+1+bodyLen)
	out = append(out, keyMagic...)
	out = append(out, keySchemaVersion)
	var mountBuf [encodedMountKeyBytes]byte
	binary.BigEndian.PutUint64(mountBuf[:], uint64(mountKeyID))
	out = append(out, mountBuf[:]...)
	out = appendBucket(out, bucket)
	out = append(out, byte(kind))
	return out
}

func encodeMountPrefix(mount model.MountIdentity, suffixLen int) ([]byte, error) {
	if err := model.ValidateMountIdentity(mount); err != nil {
		return nil, err
	}
	out := make([]byte, 0, len(keyMagic)+1+encodedMountKeyBytes+suffixLen)
	out = append(out, keyMagic...)
	out = append(out, keySchemaVersion)
	var buf [encodedMountKeyBytes]byte
	binary.BigEndian.PutUint64(buf[:], uint64(mount.MountKeyID))
	out = append(out, buf[:]...)
	return out, nil
}

func appendBucket(out []byte, bucket AffinityBucket) []byte {
	var buf [encodedBucketBytes]byte
	binary.BigEndian.PutUint16(buf[:], uint16(bucket))
	return append(out, buf[:]...)
}

func prefixUpperBound(prefix []byte) []byte {
	out := append([]byte(nil), prefix...)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] != 0xff {
			out[i]++
			return out[:i+1]
		}
	}
	return nil
}

func decodeHeaderParts(key []byte) (model.MountKeyID, int, error) {
	mount, pos, err := decodeMountPrefix(key)
	if err != nil {
		return 0, 0, err
	}
	if len(key)-pos < encodedBucketBytes+1 {
		return 0, 0, ErrInvalidKey
	}
	return mount, pos + encodedBucketBytes, nil
}

func decodeMountPrefix(key []byte) (model.MountKeyID, int, error) {
	if len(key) < len(keyMagic)+1+encodedMountKeyBytes {
		return 0, 0, ErrInvalidKey
	}
	for i := range keyMagic {
		if key[i] != keyMagic[i] {
			return 0, 0, ErrInvalidKey
		}
	}
	pos := len(keyMagic)
	if key[pos] != keySchemaVersion {
		return 0, 0, ErrInvalidKey
	}
	pos++
	if len(key)-pos < encodedMountKeyBytes {
		return 0, 0, ErrInvalidKey
	}
	mount := model.MountKeyID(binary.BigEndian.Uint64(key[pos : pos+encodedMountKeyBytes]))
	if mount == 0 {
		return 0, 0, ErrInvalidKey
	}
	pos += encodedMountKeyBytes
	return mount, pos, nil
}
