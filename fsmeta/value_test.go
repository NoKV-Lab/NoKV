// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package fsmeta

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDentryValueRoundTrip(t *testing.T) {
	value, err := EncodeDentryValue(DentryRecord{
		Parent: RootInode,
		Name:   "file",
		Inode:  22,
		Type:   InodeTypeFile,
	})
	require.NoError(t, err)
	require.Equal(t, "66737600016400000000000000010466696c65000000000000001601", hex.EncodeToString(value))

	kind, err := ValueKindOf(value)
	require.NoError(t, err)
	require.Equal(t, ValueKindDentry, kind)

	record, err := DecodeDentryValue(value)
	require.NoError(t, err)
	require.Equal(t, DentryRecord{
		Parent: RootInode,
		Name:   "file",
		Inode:  22,
		Type:   InodeTypeFile,
	}, record)
}

func TestInodeValueRoundTrip(t *testing.T) {
	value, err := EncodeInodeValue(InodeRecord{
		Inode:       22,
		Type:        InodeTypeFile,
		Size:        4096,
		LinkCount:   1,
		OpaqueAttrs: []byte(`{"body_ref":"s3://bucket/checkpoint"}`),
	})
	require.NoError(t, err)
	require.Equal(t, "6673760001690000000000000016010000000000001000000000000000000100000000000000000000000000000000257b22626f64795f726566223a2273333a2f2f6275636b65742f636865636b706f696e74227d00", hex.EncodeToString(value))

	record, err := DecodeInodeValue(value)
	require.NoError(t, err)
	require.Equal(t, InodeRecord{
		Inode:       22,
		Type:        InodeTypeFile,
		Size:        4096,
		LinkCount:   1,
		OpaqueAttrs: []byte(`{"body_ref":"s3://bucket/checkpoint"}`),
	}, record)
}

func TestSessionValueRoundTrip(t *testing.T) {
	value, err := EncodeSessionValue(SessionRecord{
		Session:       "writer-1",
		Inode:         22,
		ExpiresUnixNs: 99,
	})
	require.NoError(t, err)

	record, err := DecodeSessionValue(value)
	require.NoError(t, err)
	require.Equal(t, SessionRecord{
		Session:       "writer-1",
		Inode:         22,
		ExpiresUnixNs: 99,
	}, record)
}

func TestUsageValueRoundTrip(t *testing.T) {
	value, err := EncodeUsageValue(UsageRecord{
		Bytes:  4096,
		Inodes: 12,
	})
	require.NoError(t, err)
	require.Equal(t, "usage", ValueKindUsage.String())
	require.Equal(t, "unknown(122)", ValueKind('z').String())

	kind, err := ValueKindOf(value)
	require.NoError(t, err)
	require.Equal(t, ValueKindUsage, kind)

	record, err := DecodeUsageValue(value)
	require.NoError(t, err)
	require.Equal(t, UsageRecord{Bytes: 4096, Inodes: 12}, record)
}

func TestSnapshotValueRoundTrip(t *testing.T) {
	var ref SnapshotEvidenceRef
	ref.EpochID = 7
	ref.EvidenceRoot[0] = 1
	ref.PayloadDigest[0] = 2
	token := SnapshotSubtreeToken{
		Mount:           "vol",
		MountKeyID:      9,
		RootInode:       RootInode,
		ReadVersion:     42,
		RuntimeEvidence: []SnapshotEvidenceRef{ref},
	}
	value, err := EncodeSnapshotValue(token)
	require.NoError(t, err)
	require.Equal(t, "snapshot", ValueKindSnapshot.String())

	kind, err := ValueKindOf(value)
	require.NoError(t, err)
	require.Equal(t, ValueKindSnapshot, kind)

	decoded, err := DecodeSnapshotValue(value)
	require.NoError(t, err)
	require.Equal(t, token, decoded)
}

func TestValueDecodersRejectWrongKind(t *testing.T) {
	value, err := EncodeDentryValue(DentryRecord{Parent: RootInode, Name: "file", Inode: 22})
	require.NoError(t, err)

	_, err = DecodeInodeValue(value)
	require.ErrorIs(t, err, ErrInvalidValueKind)
}

func TestValueKindOfRejectsInvalidValues(t *testing.T) {
	_, err := ValueKindOf([]byte("not-fsmeta-value"))
	require.ErrorIs(t, err, ErrInvalidValue)

	value := encodeValue(ValueKind('z'), []byte("body"))
	_, err = ValueKindOf(value)
	require.ErrorIs(t, err, ErrInvalidValueKind)
}

func TestValueCodecsRejectInvalidType(t *testing.T) {
	_, err := EncodeInodeValue(InodeRecord{Inode: 22, Type: InodeType("symlink")})
	require.ErrorIs(t, err, ErrInvalidValue)

	_, err = EncodeInodeValue(InodeRecord{
		Inode:       22,
		Type:        InodeTypeFile,
		OpaqueAttrs: make([]byte, MaxInodeOpaqueAttrsBytes+1),
	})
	require.ErrorIs(t, err, ErrInvalidValue)

	body := append([]byte{
		0, 0, 0, 0, 0, 0, 0, 22,
		99,
	}, make([]byte, 32)...)
	body = append(body, 0)
	value := encodeValue(ValueKindInode, body)
	_, err = DecodeInodeValue(value)
	require.ErrorIs(t, err, ErrInvalidValue)

	value = encodeValue(ValueKindInode, append([]byte{
		0, 0, 0, 0, 0, 0, 0, 22,
		1,
	}, make([]byte, 32)...))
	_, err = DecodeInodeValue(value)
	require.ErrorIs(t, err, ErrInvalidValue)
}
