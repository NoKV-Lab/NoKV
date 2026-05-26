// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"fmt"

	fsperas "github.com/feichai0017/NoKV/experimental/peras/exec"
	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/model"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
)

// ScopeToProto keeps the StoreKV wire shape transport-only. The authority
// itself remains rooted in meta/root and compile.AuthorityScope.
func ScopeToProto(scope compile.AuthorityScope) *kvrpcpb.VisibleAuthorityScope {
	out := &kvrpcpb.VisibleAuthorityScope{
		Mount:      string(scope.Mount),
		MountKeyId: uint64(scope.MountKeyID),
		Buckets:    make([]uint32, 0, len(scope.Buckets)),
		Parents:    make([]uint64, 0, len(scope.Parents)),
		Inodes:     make([]uint64, 0, len(scope.Inodes)),
	}
	for _, bucket := range scope.Buckets {
		out.Buckets = append(out.Buckets, uint32(bucket))
	}
	for _, parent := range scope.Parents {
		out.Parents = append(out.Parents, uint64(parent))
	}
	for _, inode := range scope.Inodes {
		out.Inodes = append(out.Inodes, uint64(inode))
	}
	return out
}

func ScopeFromProto(in *kvrpcpb.VisibleAuthorityScope) (compile.AuthorityScope, error) {
	if in == nil {
		return compile.AuthorityScope{}, fmt.Errorf("peras wire: authority scope missing")
	}
	out := compile.AuthorityScope{
		Mount:      model.MountID(in.GetMount()),
		MountKeyID: model.MountKeyID(in.GetMountKeyId()),
		Buckets:    make([]fsmeta.AffinityBucket, 0, len(in.GetBuckets())),
		Parents:    make([]model.InodeID, 0, len(in.GetParents())),
		Inodes:     make([]model.InodeID, 0, len(in.GetInodes())),
	}
	for _, bucket := range in.GetBuckets() {
		out.Buckets = append(out.Buckets, fsmeta.AffinityBucket(bucket))
	}
	for _, parent := range in.GetParents() {
		out.Parents = append(out.Parents, model.InodeID(parent))
	}
	for _, inode := range in.GetInodes() {
		out.Inodes = append(out.Inodes, model.InodeID(inode))
	}
	return out, nil
}

func SegmentWitnessRecordToProto(record fsperas.SegmentWitnessRecord) *kvrpcpb.SegmentWitnessRecord {
	return &kvrpcpb.SegmentWitnessRecord{
		EpochId:              record.EpochID,
		SegmentRoot:          append([]byte(nil), record.SegmentRoot[:]...),
		SegmentPayloadDigest: append([]byte(nil), record.SegmentPayloadDigest[:]...),
		PredecessorDigest:    append([]byte(nil), record.PredecessorDigest[:]...),
		SegmentPayloadSize:   record.SegmentPayloadSize,
		SegmentPointer:       record.SegmentPointer,
		SegmentPayload:       append([]byte(nil), record.SegmentPayload...),
		OperationCount:       record.OperationCount,
		EntryCount:           record.EntryCount,
		TimestampUnixNano:    record.TimestampUnixNano,
		HolderId:             record.HolderID,
	}
}

func SegmentWitnessRecordsToProto(records []fsperas.SegmentWitnessRecord) []*kvrpcpb.SegmentWitnessRecord {
	out := make([]*kvrpcpb.SegmentWitnessRecord, 0, len(records))
	for _, record := range records {
		out = append(out, SegmentWitnessRecordToProto(record))
	}
	return out
}

func SegmentWitnessRecordFromProto(in *kvrpcpb.SegmentWitnessRecord) (fsperas.SegmentWitnessRecord, error) {
	if in == nil {
		return fsperas.SegmentWitnessRecord{}, fmt.Errorf("peras wire: segment witness record missing")
	}
	out := fsperas.SegmentWitnessRecord{
		EpochID:            in.GetEpochId(),
		SegmentPayloadSize: in.GetSegmentPayloadSize(),
		SegmentPointer:     in.GetSegmentPointer(),
		SegmentPayload:     append([]byte(nil), in.GetSegmentPayload()...),
		OperationCount:     in.GetOperationCount(),
		EntryCount:         in.GetEntryCount(),
		TimestampUnixNano:  in.GetTimestampUnixNano(),
		HolderID:           in.GetHolderId(),
	}
	if err := copyFixed(out.SegmentRoot[:], in.GetSegmentRoot(), "segment_root"); err != nil {
		return fsperas.SegmentWitnessRecord{}, err
	}
	if err := copyFixed(out.SegmentPayloadDigest[:], in.GetSegmentPayloadDigest(), "segment_payload_digest"); err != nil {
		return fsperas.SegmentWitnessRecord{}, err
	}
	if err := copyFixed(out.PredecessorDigest[:], in.GetPredecessorDigest(), "predecessor_digest"); err != nil {
		return fsperas.SegmentWitnessRecord{}, err
	}
	return out, nil
}

func SegmentWitnessRecordsFromProto(in []*kvrpcpb.SegmentWitnessRecord) ([]fsperas.SegmentWitnessRecord, error) {
	out := make([]fsperas.SegmentWitnessRecord, 0, len(in))
	for _, segment := range in {
		record, err := SegmentWitnessRecordFromProto(segment)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	return out, nil
}

func SnapshotToProto(snapshot fsperas.WitnessSnapshot) *kvrpcpb.ProbeSegmentWitnessResponse {
	out := &kvrpcpb.ProbeSegmentWitnessResponse{
		Segments: make([]*kvrpcpb.SegmentWitnessRecord, 0, len(snapshot.Segments)),
	}
	for _, segment := range snapshot.Segments {
		out.Segments = append(out.Segments, SegmentWitnessRecordToProto(segment))
	}
	return out
}

func SnapshotFromProto(in *kvrpcpb.ProbeSegmentWitnessResponse) (fsperas.WitnessSnapshot, error) {
	if in == nil {
		return fsperas.WitnessSnapshot{}, fmt.Errorf("peras wire: witness snapshot missing")
	}
	out := fsperas.WitnessSnapshot{
		Segments: make([]fsperas.SegmentWitnessRecord, 0, len(in.GetSegments())),
	}
	for _, segment := range in.GetSegments() {
		record, err := SegmentWitnessRecordFromProto(segment)
		if err != nil {
			return fsperas.WitnessSnapshot{}, err
		}
		out.Segments = append(out.Segments, record)
	}
	return out, nil
}

func copyFixed(dst []byte, src []byte, field string) error {
	if len(src) != len(dst) {
		return fmt.Errorf("peras wire: %s length %d != %d", field, len(src), len(dst))
	}
	copy(dst, src)
	return nil
}
