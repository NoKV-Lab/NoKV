// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"testing"

	fsperas "github.com/feichai0017/NoKV/experimental/peras/exec"
	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/stretchr/testify/require"
)

func TestPerasWireRoundTrip(t *testing.T) {
	scope := compile.AuthorityScope{
		Mount:      model.MountID("m1"),
		MountKeyID: model.MountKeyID(9),
		Buckets:    []fsmeta.AffinityBucket{1, 3},
		Parents:    []model.InodeID{11, 12},
		Inodes:     []model.InodeID{21},
	}
	decodedScope, err := ScopeFromProto(ScopeToProto(scope))
	require.NoError(t, err)
	require.Equal(t, scope, decodedScope)

	record := wireTestSegmentRecord()
	decodedRecord, err := SegmentWitnessRecordFromProto(SegmentWitnessRecordToProto(record))
	require.NoError(t, err)
	require.Equal(t, record, decodedRecord)

	snapshot := fsperas.WitnessSnapshot{Segments: []fsperas.SegmentWitnessRecord{record}}
	decoded, err := SnapshotFromProto(SnapshotToProto(snapshot))
	require.NoError(t, err)
	require.Equal(t, snapshot, decoded)
}

func TestPerasWireRejectsWrongFixedDigestLength(t *testing.T) {
	record := SegmentWitnessRecordToProto(wireTestSegmentRecord())
	record.SegmentRoot = record.SegmentRoot[:31]
	_, err := SegmentWitnessRecordFromProto(record)
	require.ErrorContains(t, err, "segment_root length")
}

func wireTestSegmentRecord() fsperas.SegmentWitnessRecord {
	var root [32]byte
	root[0] = 9
	payload := []byte("wire-segment-payload")
	digest, err := fsperas.PerasSegmentPayloadDigest(payload)
	if err != nil {
		panic(err)
	}
	return fsperas.SegmentWitnessRecord{
		EpochID:              7,
		SegmentRoot:          root,
		SegmentPayloadDigest: digest,
		SegmentPayloadSize:   uint64(len(payload)),
		SegmentPayload:       payload,
		OperationCount:       128,
		EntryCount:           256,
		TimestampUnixNano:    1234,
		HolderID:             "holder-a",
	}
}
