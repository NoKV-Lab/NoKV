// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"bytes"
	"slices"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/model"
)

var (
	perasSegmentCatalogMagic = [4]byte{'N', 'P', 'C', 2}
	perasSegmentIndexMagic   = [4]byte{'N', 'P', 'I', 1}
)

// SegmentCatalogRecord is the hidden segment object written with a durable
// segment install. Bucket-local index records point to this object so restart
// can rebuild the installed segment frontier and operation completion table
// without replaying request-level evidence.
type SegmentCatalogRecord struct {
	EpochID        uint64
	InstallVersion uint64
	Root           [32]byte

	SegmentPayloadDigest [32]byte
	SegmentPayloadSize   uint64
	SegmentPayload       []byte

	OperationCount     uint64
	EntryCount         uint64
	CompletionCount    uint64
	InputMutationCount uint64
	CoalescedMutations uint64

	Completions []SegmentCompletion
}

// SegmentCatalogIndexRecord is the per-bucket discovery record for one sealed
// segment object.
type SegmentCatalogIndexRecord struct {
	EpochID              uint64
	InstallVersion       uint64
	Root                 [32]byte
	SegmentPayloadDigest [32]byte
	SegmentPayloadSize   uint64
	ObjectKey            []byte
}

func PerasSegmentCatalogIndexKeys(segment PerasSegment) ([][]byte, error) {
	buckets, err := perasSegmentCatalogBuckets(segment)
	if err != nil {
		return nil, err
	}
	keys := make([][]byte, 0, len(buckets))
	for _, bucket := range buckets {
		key, err := fsmeta.EncodeSegmentCatalogIndexKey(bucket.mount, bucket.bucket, segment.Root)
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, nil
}

func PerasSegmentObjectKey(segment PerasSegment) ([]byte, error) {
	bucket, err := perasSegmentCanonicalCatalogBucket(segment)
	if err != nil {
		return nil, err
	}
	return fsmeta.EncodeSegmentObjectKey(bucket.mount, bucket.bucket, segment.Root)
}

func perasSegmentCanonicalCatalogBucket(segment PerasSegment) (perasSegmentCatalogBucket, error) {
	if err := validatePerasSegmentPayload(segment); err != nil {
		return perasSegmentCatalogBucket{}, err
	}
	if len(segment.entries) == 0 {
		return perasSegmentCatalogBucket{}, ErrInvalidPerasSegment
	}
	var out perasSegmentCatalogBucket
	for idx, entry := range segment.entries {
		parts, ok := fsmeta.InspectKey(entry.Key)
		if !ok {
			return perasSegmentCatalogBucket{}, ErrInvalidPerasSegment
		}
		key := perasSegmentCatalogBucket{mount: parts.MountKeyID, bucket: parts.Bucket}
		if idx == 0 || key.mount < out.mount || (key.mount == out.mount && key.bucket < out.bucket) {
			out = key
		}
	}
	if out.mount == 0 {
		return perasSegmentCatalogBucket{}, ErrInvalidPerasSegment
	}
	return out, nil
}

// PerasSegmentCatalogObjectKeys returns one bucket-local object key per
// fsmeta bucket touched by the segment. These keys are routing markers for
// raftstore install: only the canonical object key stores the segment payload;
// every bucket writes a local index record pointing at that canonical object.
func PerasSegmentCatalogObjectKeys(segment PerasSegment) ([][]byte, error) {
	buckets, err := perasSegmentCatalogBuckets(segment)
	if err != nil {
		return nil, err
	}
	keys := make([][]byte, 0, len(buckets))
	for _, bucket := range buckets {
		key, err := fsmeta.EncodeSegmentObjectKey(bucket.mount, bucket.bucket, segment.Root)
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, nil
}

type perasSegmentCatalogBucket struct {
	mount  model.MountKeyID
	bucket fsmeta.AffinityBucket
}

func perasSegmentCatalogBuckets(segment PerasSegment) ([]perasSegmentCatalogBucket, error) {
	if err := validatePerasSegmentPayload(segment); err != nil {
		return nil, err
	}
	if len(segment.entries) == 0 {
		return nil, ErrInvalidPerasSegment
	}
	seen := make(map[perasSegmentCatalogBucket]struct{})
	out := make([]perasSegmentCatalogBucket, 0)
	for _, entry := range segment.entries {
		parts, ok := fsmeta.InspectKey(entry.Key)
		if !ok {
			return nil, ErrInvalidPerasSegment
		}
		key := perasSegmentCatalogBucket{mount: parts.MountKeyID, bucket: parts.Bucket}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, key)
	}
	slices.SortFunc(out, func(a, b perasSegmentCatalogBucket) int {
		if a.mount < b.mount {
			return -1
		}
		if a.mount > b.mount {
			return 1
		}
		if a.bucket < b.bucket {
			return -1
		}
		if a.bucket > b.bucket {
			return 1
		}
		return 0
	})
	return out, nil
}

func EncodePerasSegmentCatalogRecord(segment PerasSegment, installVersion uint64) ([]byte, error) {
	payload, err := EncodePerasSegment(segment)
	if err != nil {
		return nil, err
	}
	digest, err := PerasSegmentPayloadDigest(payload)
	if err != nil {
		return nil, err
	}
	return EncodePerasSegmentCatalogRecordWithPayload(segment, installVersion, payload, digest)
}

// EncodePerasSegmentCatalogRecordWithPayload writes a catalog record using a
// segment payload the caller has already verified against segment.Root and
// digest. This avoids re-encoding the payload on the raftstore install path.
func EncodePerasSegmentCatalogRecordWithPayload(segment PerasSegment, installVersion uint64, payload []byte, digest [32]byte) ([]byte, error) {
	if installVersion == 0 {
		return nil, ErrReplayVersionRequired
	}
	if err := validatePerasSegmentPayload(segment); err != nil {
		return nil, err
	}
	if len(payload) == 0 || digest == ([32]byte{}) {
		return nil, ErrInvalidPerasSegment
	}
	stats := segment.Stats()
	var out bytes.Buffer
	out.Grow(segmentCatalogRecordEncodedSize(segment, len(payload)))
	writeFixed(&out, perasSegmentCatalogMagic[:])
	writeUint64(&out, segment.EpochID)
	writeUint64(&out, installVersion)
	writeFixed(&out, segment.Root[:])
	writeFixed(&out, digest[:])
	writeUint64(&out, uint64(len(payload)))
	writeBytes(&out, payload)
	writeUint64(&out, stats.OperationCount)
	writeUint64(&out, stats.EntryCount)
	writeUint64(&out, stats.CompletionCount)
	writeUint64(&out, stats.InputMutationCount)
	writeUint64(&out, stats.CoalescedMutations)
	writeUint64(&out, uint64(len(segment.Completions)))
	for _, completion := range segment.Completions {
		writeSegmentCompletion(&out, completion)
	}
	return out.Bytes(), nil
}

func segmentCatalogRecordEncodedSize(segment PerasSegment, payloadSize int) int {
	size := len(perasSegmentCatalogMagic) +
		8 + 8 + 32 + 32 + 8 +
		4 + payloadSize +
		8 + 8 + 8 + 8 + 8 + 8
	for _, completion := range segment.Completions {
		size += segmentCompletionEncodedSize(completion)
	}
	return size
}

func EncodePerasSegmentCatalogIndexRecord(record SegmentCatalogRecord, objectKey []byte) ([]byte, error) {
	if err := validateSegmentCatalogPayload(record); err != nil {
		return nil, err
	}
	return encodePerasSegmentCatalogIndexRecord(record.EpochID, record.InstallVersion, record.Root, record.SegmentPayloadDigest, record.SegmentPayloadSize, objectKey)
}

// EncodePerasSegmentCatalogIndexRecordFields writes an index record from a
// caller-verified segment identity. It validates the identity fields and object
// key shape, but it does not rescan the full segment payload.
func EncodePerasSegmentCatalogIndexRecordFields(epochID, installVersion uint64, root, payloadDigest [32]byte, payloadSize uint64, objectKey []byte) ([]byte, error) {
	return encodePerasSegmentCatalogIndexRecord(epochID, installVersion, root, payloadDigest, payloadSize, objectKey)
}

func encodePerasSegmentCatalogIndexRecord(epochID, installVersion uint64, root, digest [32]byte, payloadSize uint64, objectKey []byte) ([]byte, error) {
	if epochID == 0 || installVersion == 0 || root == ([32]byte{}) || digest == ([32]byte{}) || payloadSize == 0 || len(objectKey) == 0 {
		return nil, ErrInvalidPerasSegment
	}
	parts, ok := fsmeta.InspectKey(objectKey)
	if !ok || parts.Kind != fsmeta.KeyKindSegment || parts.SegmentRecord != fsmeta.SegmentRecordObject || parts.SegmentRoot != root {
		return nil, ErrInvalidPerasSegment
	}
	var out bytes.Buffer
	out.Grow(len(perasSegmentIndexMagic) + 8 + 8 + 32 + 32 + 8 + 4 + len(objectKey))
	writeFixed(&out, perasSegmentIndexMagic[:])
	writeUint64(&out, epochID)
	writeUint64(&out, installVersion)
	writeFixed(&out, root[:])
	writeFixed(&out, digest[:])
	writeUint64(&out, payloadSize)
	writeBytes(&out, objectKey)
	return out.Bytes(), nil
}

func DecodePerasSegmentCatalogIndexRecord(payload []byte) (SegmentCatalogIndexRecord, error) {
	r := witnessReader{buf: payload}
	if err := r.readMagic(perasSegmentIndexMagic); err != nil {
		return SegmentCatalogIndexRecord{}, ErrInvalidPerasSegment
	}
	epochID, err := r.readUint64()
	if err != nil {
		return SegmentCatalogIndexRecord{}, ErrInvalidPerasSegment
	}
	installVersion, err := r.readUint64()
	if err != nil || installVersion == 0 {
		return SegmentCatalogIndexRecord{}, ErrInvalidPerasSegment
	}
	var root [32]byte
	if err := r.readFixed(root[:]); err != nil {
		return SegmentCatalogIndexRecord{}, ErrInvalidPerasSegment
	}
	var payloadDigest [32]byte
	if err := r.readFixed(payloadDigest[:]); err != nil {
		return SegmentCatalogIndexRecord{}, ErrInvalidPerasSegment
	}
	payloadSize, err := r.readUint64()
	if err != nil || payloadSize == 0 {
		return SegmentCatalogIndexRecord{}, ErrInvalidPerasSegment
	}
	objectKey, err := r.readBytes()
	if err != nil || len(objectKey) == 0 {
		return SegmentCatalogIndexRecord{}, ErrInvalidPerasSegment
	}
	if !r.done() || epochID == 0 || root == ([32]byte{}) || payloadDigest == ([32]byte{}) {
		return SegmentCatalogIndexRecord{}, ErrInvalidPerasSegment
	}
	parts, ok := fsmeta.InspectKey(objectKey)
	if !ok || parts.Kind != fsmeta.KeyKindSegment || parts.SegmentRecord != fsmeta.SegmentRecordObject || parts.SegmentRoot != root {
		return SegmentCatalogIndexRecord{}, ErrInvalidPerasSegment
	}
	return SegmentCatalogIndexRecord{
		EpochID:              epochID,
		InstallVersion:       installVersion,
		Root:                 root,
		SegmentPayloadDigest: payloadDigest,
		SegmentPayloadSize:   payloadSize,
		ObjectKey:            objectKey,
	}, nil
}

func DecodePerasSegmentCatalogRecord(payload []byte) (SegmentCatalogRecord, error) {
	r := witnessReader{buf: payload}
	if err := r.readMagic(perasSegmentCatalogMagic); err != nil {
		return SegmentCatalogRecord{}, ErrInvalidPerasSegment
	}
	epochID, err := r.readUint64()
	if err != nil {
		return SegmentCatalogRecord{}, ErrInvalidPerasSegment
	}
	installVersion, err := r.readUint64()
	if err != nil || installVersion == 0 {
		return SegmentCatalogRecord{}, ErrInvalidPerasSegment
	}
	var root [32]byte
	if err := r.readFixed(root[:]); err != nil {
		return SegmentCatalogRecord{}, ErrInvalidPerasSegment
	}
	var payloadDigest [32]byte
	if err := r.readFixed(payloadDigest[:]); err != nil {
		return SegmentCatalogRecord{}, ErrInvalidPerasSegment
	}
	payloadSize, err := r.readUint64()
	if err != nil {
		return SegmentCatalogRecord{}, ErrInvalidPerasSegment
	}
	segmentPayload, err := r.readBytes()
	if err != nil {
		return SegmentCatalogRecord{}, ErrInvalidPerasSegment
	}
	operationCount, err := r.readUint64()
	if err != nil {
		return SegmentCatalogRecord{}, ErrInvalidPerasSegment
	}
	entryCount, err := r.readUint64()
	if err != nil {
		return SegmentCatalogRecord{}, ErrInvalidPerasSegment
	}
	completionCount, err := r.readUint64()
	if err != nil {
		return SegmentCatalogRecord{}, ErrInvalidPerasSegment
	}
	inputMutationCount, err := r.readUint64()
	if err != nil {
		return SegmentCatalogRecord{}, ErrInvalidPerasSegment
	}
	coalescedMutations, err := r.readUint64()
	if err != nil {
		return SegmentCatalogRecord{}, ErrInvalidPerasSegment
	}
	encodedCompletionCount, err := r.readUint64()
	if err != nil || encodedCompletionCount != completionCount || encodedCompletionCount > uint64(maxSegmentSliceLen()) {
		return SegmentCatalogRecord{}, ErrInvalidPerasSegment
	}
	completions := make([]SegmentCompletion, 0, encodedCompletionCount)
	for range encodedCompletionCount {
		completion, err := readSegmentCompletion(&r)
		if err != nil {
			return SegmentCatalogRecord{}, ErrInvalidPerasSegment
		}
		completions = append(completions, completion)
	}
	if !r.done() || epochID == 0 || root == ([32]byte{}) || operationCount == 0 || completionCount != uint64(len(completions)) {
		return SegmentCatalogRecord{}, ErrInvalidPerasSegment
	}
	record := SegmentCatalogRecord{
		EpochID:              epochID,
		InstallVersion:       installVersion,
		Root:                 root,
		SegmentPayloadDigest: payloadDigest,
		SegmentPayloadSize:   payloadSize,
		SegmentPayload:       segmentPayload,
		OperationCount:       operationCount,
		EntryCount:           entryCount,
		CompletionCount:      completionCount,
		InputMutationCount:   inputMutationCount,
		CoalescedMutations:   coalescedMutations,
		Completions:          completions,
	}
	if err := validateSegmentCatalogPayload(record); err != nil {
		return SegmentCatalogRecord{}, err
	}
	return record, nil
}

func validateSegmentCatalogPayload(record SegmentCatalogRecord) error {
	if record.Root == ([32]byte{}) || record.SegmentPayloadDigest == ([32]byte{}) {
		return ErrInvalidPerasSegment
	}
	if record.SegmentPayloadSize == 0 || uint64(len(record.SegmentPayload)) != record.SegmentPayloadSize {
		return ErrInvalidPerasSegment
	}
	segment, err := VerifyPerasSegmentPayload(record.SegmentPayload, record.Root, record.SegmentPayloadDigest)
	if err != nil {
		return err
	}
	stats := segment.Stats()
	if stats.OperationCount != record.OperationCount ||
		stats.EntryCount != record.EntryCount ||
		stats.CompletionCount != record.CompletionCount ||
		stats.InputMutationCount != record.InputMutationCount ||
		stats.CoalescedMutations != record.CoalescedMutations ||
		len(segment.Completions) != len(record.Completions) {
		return ErrInvalidPerasSegment
	}
	for i, completion := range segment.Completions {
		if !segmentCompletionEqual(completion, record.Completions[i]) {
			return ErrInvalidPerasSegment
		}
	}
	return nil
}

func segmentCompletionEqual(left, right SegmentCompletion) bool {
	if left.OpID != right.OpID ||
		left.Kind != right.Kind ||
		left.Version != right.Version ||
		left.MutationCount != right.MutationCount ||
		left.DescriptorDigest != right.DescriptorDigest ||
		left.PredicateProofDigest != right.PredicateProofDigest ||
		left.ExecutionPlanDigest != right.ExecutionPlanDigest ||
		!predicateProofsEqual(left.PredicateProofs, right.PredicateProofs) ||
		!guardProofsEqual(left.GuardProofs, right.GuardProofs) {
		return false
	}
	return true
}
