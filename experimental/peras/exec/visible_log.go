// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"bytes"
	"context"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/model"
)

var visibleRecordMagic = [4]byte{'N', 'P', 'V', 3}
var visibleAppliedMagic = [4]byte{'N', 'P', 'A', 2}

type VisibleRootLineage struct {
	ClusterEpoch uint64
	Term         uint64
	Index        uint64
	Revision     uint64
}

func (l VisibleRootLineage) Valid() bool {
	return l.ClusterEpoch != 0 && l.Term != 0 && l.Index != 0 && l.Revision != 0
}

type VisibleOperationRecord struct {
	EpochID           uint64
	HolderID          string
	GrantID           string
	GrantExpiresNanos int64
	PredecessorDigest [32]byte
	RootLineage       VisibleRootLineage
	Scope             compile.AuthorityScope
	Operation         ReplayOperation
	TimestampUnixNano int64
}

type VisibleOperationReference struct {
	OpID                 OperationID
	DescriptorDigest     [32]byte
	PredicateProofDigest [32]byte
	ExecutionPlanDigest  [32]byte
}

type VisibleAppliedRange struct {
	SegmentID   uint32
	StartOffset uint64
	EndOffset   uint64
}

type VisibleAppliedRecord struct {
	EpochID  uint64
	HolderID string
	Ranges   []VisibleAppliedRange
}

type VisibleLog interface {
	AppendVisible(context.Context, VisibleOperationRecord) error
}

type VisibleLogReplayer interface {
	ReplayVisible(context.Context) ([]VisibleOperationRecord, error)
}

type VisibleLogApplier interface {
	AppendVisibleApplied(context.Context, VisibleAppliedRecord) error
}

func EncodeVisibleOperationRecord(record VisibleOperationRecord) ([]byte, error) {
	return EncodeVisibleOperationRecordTo(nil, record)
}

// EncodeVisibleOperationRecordTo encodes record into dst[:0] when capacity allows.
func EncodeVisibleOperationRecordTo(dst []byte, record VisibleOperationRecord) ([]byte, error) {
	if err := validateVisibleOperationRecord(record); err != nil {
		return nil, err
	}
	out := bytes.NewBuffer(dst[:0])
	out.Grow(visibleOperationRecordEncodedSize(record))
	writeFixed(out, visibleRecordMagic[:])
	writeUint64(out, record.EpochID)
	writeString(out, record.HolderID)
	writeString(out, record.GrantID)
	writeInt64(out, record.GrantExpiresNanos)
	writeFixed(out, record.PredecessorDigest[:])
	writeVisibleRootLineage(out, record.RootLineage)
	writeAuthorityScope(out, record.Scope)
	writeInt64(out, record.TimestampUnixNano)
	writeVisibleReplayOperation(out, record.Operation)
	return out.Bytes(), nil
}

func EncodeVisibleAppliedRecord(record VisibleAppliedRecord) ([]byte, error) {
	if err := validateVisibleAppliedRecord(record); err != nil {
		return nil, err
	}
	var out bytes.Buffer
	out.Grow(len(visibleAppliedMagic) + 8 + stringEncodedSize(record.HolderID) + 8 + len(record.Ranges)*(8+8+8))
	writeFixed(&out, visibleAppliedMagic[:])
	writeUint64(&out, record.EpochID)
	writeString(&out, record.HolderID)
	writeUint64(&out, uint64(len(record.Ranges)))
	for _, applied := range record.Ranges {
		writeVisibleAppliedRange(&out, applied)
	}
	return out.Bytes(), nil
}

func DecodeVisibleOperationRecord(payload []byte) (VisibleOperationRecord, error) {
	r := witnessReader{buf: payload}
	var magic [4]byte
	if err := r.readFixed(magic[:]); err != nil {
		return VisibleOperationRecord{}, ErrInvalidWitnessRecord
	}
	if !bytes.Equal(magic[:], visibleRecordMagic[:]) {
		return VisibleOperationRecord{}, ErrInvalidWitnessRecord
	}
	record := VisibleOperationRecord{}
	var err error
	if record.EpochID, err = r.readUint64(); err != nil {
		return VisibleOperationRecord{}, ErrInvalidWitnessRecord
	}
	if record.HolderID, err = r.readString(); err != nil {
		return VisibleOperationRecord{}, ErrInvalidWitnessRecord
	}
	if record.GrantID, err = r.readString(); err != nil {
		return VisibleOperationRecord{}, ErrInvalidWitnessRecord
	}
	var expires uint64
	if expires, err = r.readUint64(); err != nil {
		return VisibleOperationRecord{}, ErrInvalidWitnessRecord
	}
	record.GrantExpiresNanos = int64(expires)
	if err := r.readFixed(record.PredecessorDigest[:]); err != nil {
		return VisibleOperationRecord{}, ErrInvalidWitnessRecord
	}
	if record.RootLineage, err = readVisibleRootLineage(&r); err != nil {
		return VisibleOperationRecord{}, ErrInvalidWitnessRecord
	}
	if record.Scope, err = readAuthorityScope(&r); err != nil {
		return VisibleOperationRecord{}, ErrInvalidWitnessRecord
	}
	var ts uint64
	if ts, err = r.readUint64(); err != nil {
		return VisibleOperationRecord{}, ErrInvalidWitnessRecord
	}
	record.TimestampUnixNano = int64(ts)
	if record.Operation, err = readVisibleReplayOperation(&r); err != nil {
		return VisibleOperationRecord{}, ErrInvalidWitnessRecord
	}
	if !r.done() {
		return VisibleOperationRecord{}, ErrInvalidWitnessRecord
	}
	if err := validateVisibleOperationRecord(record); err != nil {
		return VisibleOperationRecord{}, err
	}
	return record, nil
}

func DecodeVisibleAppliedRecord(payload []byte) (VisibleAppliedRecord, error) {
	r := witnessReader{buf: payload}
	if err := r.readMagic(visibleAppliedMagic); err != nil {
		return VisibleAppliedRecord{}, ErrInvalidWitnessRecord
	}
	record := VisibleAppliedRecord{}
	var err error
	if record.EpochID, err = r.readUint64(); err != nil {
		return VisibleAppliedRecord{}, ErrInvalidWitnessRecord
	}
	if record.HolderID, err = r.readString(); err != nil {
		return VisibleAppliedRecord{}, ErrInvalidWitnessRecord
	}
	count, err := r.readUint64()
	if err != nil || count > uint64(maxSegmentSliceLen()) {
		return VisibleAppliedRecord{}, ErrInvalidWitnessRecord
	}
	record.Ranges = make([]VisibleAppliedRange, 0, count)
	for range count {
		applied, err := readVisibleAppliedRange(&r)
		if err != nil {
			return VisibleAppliedRecord{}, ErrInvalidWitnessRecord
		}
		record.Ranges = append(record.Ranges, applied)
	}
	if !r.done() {
		return VisibleAppliedRecord{}, ErrInvalidWitnessRecord
	}
	if err := validateVisibleAppliedRecord(record); err != nil {
		return VisibleAppliedRecord{}, err
	}
	return record, nil
}

func validateVisibleOperationRecord(record VisibleOperationRecord) error {
	if record.EpochID == 0 || record.HolderID == "" || record.GrantID == "" || record.GrantExpiresNanos <= 0 {
		return ErrInvalidWitnessRecord
	}
	if !record.RootLineage.Valid() {
		return ErrInvalidWitnessRecord
	}
	if record.Scope.Mount == "" || record.Scope.MountKeyID == 0 {
		return ErrInvalidWitnessRecord
	}
	return validateVisibleReplayOperation(record.Operation)
}

func validateVisibleAppliedRecord(record VisibleAppliedRecord) error {
	if record.EpochID == 0 || record.HolderID == "" || len(record.Ranges) == 0 {
		return ErrInvalidWitnessRecord
	}
	for _, applied := range record.Ranges {
		if err := validateVisibleAppliedRange(applied); err != nil {
			return ErrInvalidWitnessRecord
		}
	}
	return nil
}

func validateVisibleAppliedRange(applied VisibleAppliedRange) error {
	if applied.SegmentID == 0 || applied.EndOffset <= applied.StartOffset {
		return ErrInvalidWitnessRecord
	}
	return nil
}

func validateVisibleReplayOperation(op ReplayOperation) error {
	if !op.OpID.Valid() || op.Kind == "" || len(op.Mutations) == 0 {
		return ErrInvalidWitnessRecord
	}
	if op.DescriptorDigest == ([32]byte{}) || op.PredicateProofDigest == ([32]byte{}) || op.ExecutionPlanDigest == ([32]byte{}) {
		return ErrInvalidWitnessRecord
	}
	if compile.AdmissionProofSetDigest(op.PredicateProofs, op.GuardProofs) != op.PredicateProofDigest {
		return ErrInvalidWitnessRecord
	}
	if compile.ExecutionPlanDigest(op.Segment, op.Atomicity, op.Durability) != op.ExecutionPlanDigest {
		return ErrInvalidWitnessRecord
	}
	for _, mutation := range op.Mutations {
		if len(mutation.Key) == 0 || (!mutation.Delete && mutation.Value == nil) {
			return ErrInvalidWitnessRecord
		}
	}
	return nil
}

func visibleOperationRecordEncodedSize(record VisibleOperationRecord) int {
	size := len(visibleRecordMagic) + 8 + stringEncodedSize(record.HolderID) + stringEncodedSize(record.GrantID) + 8 + 32 + 32 + 8
	size += authorityScopeEncodedSize(record.Scope)
	size += stringEncodedSize(record.Operation.OpID.ClientID) + 8 + stringEncodedSize(string(record.Operation.Kind)) + 32 + 32 + 32
	size += segmentPlanEncodedSize(record.Operation.Segment)
	size += atomicityGroupEncodedSize(record.Operation.Atomicity)
	size += 8
	size += 8
	for _, mutation := range record.Operation.Mutations {
		size += 4 + len(mutation.Key) + 1 + 4 + len(mutation.Value)
	}
	return size
}

func writeVisibleRootLineage(out *bytes.Buffer, lineage VisibleRootLineage) {
	writeUint64(out, lineage.ClusterEpoch)
	writeUint64(out, lineage.Term)
	writeUint64(out, lineage.Index)
	writeUint64(out, lineage.Revision)
}

func readVisibleRootLineage(r *witnessReader) (VisibleRootLineage, error) {
	clusterEpoch, err := r.readUint64()
	if err != nil {
		return VisibleRootLineage{}, err
	}
	term, err := r.readUint64()
	if err != nil {
		return VisibleRootLineage{}, err
	}
	index, err := r.readUint64()
	if err != nil {
		return VisibleRootLineage{}, err
	}
	revision, err := r.readUint64()
	if err != nil {
		return VisibleRootLineage{}, err
	}
	return VisibleRootLineage{
		ClusterEpoch: clusterEpoch,
		Term:         term,
		Index:        index,
		Revision:     revision,
	}, nil
}

func writeVisibleReplayOperation(out *bytes.Buffer, op ReplayOperation) {
	writeOperationID(out, op.OpID)
	writeString(out, string(op.Kind))
	writeFixed(out, op.DescriptorDigest[:])
	writeFixed(out, op.PredicateProofDigest[:])
	executionPlanDigest := replayOperationExecutionPlanDigest(op)
	writeFixed(out, executionPlanDigest[:])
	writeSegmentPlan(out, op.Segment)
	writeAtomicityGroup(out, op.Atomicity)
	writeUint64(out, uint64(op.Durability))
	writePredicateProofs(out, op.PredicateProofs)
	writeGuardProofs(out, op.GuardProofs)
	writeUint64(out, uint64(len(op.Mutations)))
	for _, mutation := range op.Mutations {
		writeBytes(out, mutation.Key)
		writeBool(out, mutation.Delete)
		writeBytes(out, mutation.Value)
	}
}

func VisibleOperationReferenceFromReplay(op ReplayOperation) (VisibleOperationReference, error) {
	if err := validateVisibleReplayOperation(op); err != nil {
		return VisibleOperationReference{}, err
	}
	return VisibleOperationReference{
		OpID:                 op.OpID,
		DescriptorDigest:     op.DescriptorDigest,
		PredicateProofDigest: op.PredicateProofDigest,
		ExecutionPlanDigest:  replayOperationExecutionPlanDigest(op),
	}, nil
}

func writeVisibleAppliedRange(out *bytes.Buffer, applied VisibleAppliedRange) {
	writeUint64(out, uint64(applied.SegmentID))
	writeUint64(out, applied.StartOffset)
	writeUint64(out, applied.EndOffset)
}

func writeAuthorityScope(out *bytes.Buffer, scope compile.AuthorityScope) {
	writeString(out, string(scope.Mount))
	writeUint64(out, uint64(scope.MountKeyID))
	writeUint64(out, uint64(len(scope.Buckets)))
	for _, bucket := range scope.Buckets {
		writeUint64(out, uint64(bucket))
	}
	writeUint64(out, uint64(len(scope.Parents)))
	for _, parent := range scope.Parents {
		writeUint64(out, uint64(parent))
	}
	writeUint64(out, uint64(len(scope.Inodes)))
	for _, inode := range scope.Inodes {
		writeUint64(out, uint64(inode))
	}
	writeBool(out, scope.Broad)
	writeBool(out, scope.AllowOpaqueKeys)
}

func readAuthorityScope(r *witnessReader) (compile.AuthorityScope, error) {
	mount, err := r.readString()
	if err != nil {
		return compile.AuthorityScope{}, err
	}
	mountKeyID, err := r.readUint64()
	if err != nil {
		return compile.AuthorityScope{}, err
	}
	buckets, err := readUint64Slice(r)
	if err != nil {
		return compile.AuthorityScope{}, err
	}
	parents, err := readUint64Slice(r)
	if err != nil {
		return compile.AuthorityScope{}, err
	}
	inodes, err := readUint64Slice(r)
	if err != nil {
		return compile.AuthorityScope{}, err
	}
	broad, err := r.readBool()
	if err != nil {
		return compile.AuthorityScope{}, err
	}
	allowOpaque, err := r.readBool()
	if err != nil {
		return compile.AuthorityScope{}, err
	}
	scope := compile.AuthorityScope{
		Mount:           model.MountID(mount),
		MountKeyID:      model.MountKeyID(mountKeyID),
		Buckets:         make([]fsmeta.AffinityBucket, len(buckets)),
		Parents:         make([]model.InodeID, len(parents)),
		Inodes:          make([]model.InodeID, len(inodes)),
		Broad:           broad,
		AllowOpaqueKeys: allowOpaque,
	}
	for i, bucket := range buckets {
		scope.Buckets[i] = fsmeta.AffinityBucket(bucket)
	}
	for i, parent := range parents {
		scope.Parents[i] = model.InodeID(parent)
	}
	for i, inode := range inodes {
		scope.Inodes[i] = model.InodeID(inode)
	}
	return scope, nil
}

func readUint64Slice(r *witnessReader) ([]uint64, error) {
	count, err := r.readUint64()
	if err != nil || count > uint64(maxSegmentSliceLen()) {
		return nil, ErrInvalidWitnessRecord
	}
	out := make([]uint64, 0, count)
	for range count {
		value, err := r.readUint64()
		if err != nil {
			return nil, err
		}
		out = append(out, value)
	}
	return out, nil
}

func writeSegmentPlan(out *bytes.Buffer, plan compile.SegmentPlan) {
	writeSegmentMergeKey(out, plan.MergeKey)
	writeUint64(out, uint64(plan.Install))
	writeSegmentMergeKey(out, plan.MaterializeMergeKey)
	writeUint64(out, uint64(plan.MaterializeInstall))
	writeBool(out, plan.CanAppend)
	writeBool(out, plan.CanMaterialize)
	writeBool(out, plan.RequiresMaterialize)
	writeUint64(out, plan.EstimatedPayloadBytes)
	writeUint64(out, uint64(plan.OperationCount))
	writeUint64(out, uint64(plan.MutationCount))
}

func readSegmentPlan(r *witnessReader) (compile.SegmentPlan, error) {
	mergeKey, err := readSegmentMergeKey(r)
	if err != nil {
		return compile.SegmentPlan{}, err
	}
	install, err := r.readUint64()
	if err != nil {
		return compile.SegmentPlan{}, err
	}
	materializeMergeKey, err := readSegmentMergeKey(r)
	if err != nil {
		return compile.SegmentPlan{}, err
	}
	materializeInstall, err := r.readUint64()
	if err != nil {
		return compile.SegmentPlan{}, err
	}
	canAppend, err := r.readBool()
	if err != nil {
		return compile.SegmentPlan{}, err
	}
	canMaterialize, err := r.readBool()
	if err != nil {
		return compile.SegmentPlan{}, err
	}
	requiresMaterialize, err := r.readBool()
	if err != nil {
		return compile.SegmentPlan{}, err
	}
	estimatedPayloadBytes, err := r.readUint64()
	if err != nil {
		return compile.SegmentPlan{}, err
	}
	operationCount, err := r.readUint64()
	if err != nil {
		return compile.SegmentPlan{}, err
	}
	mutationCount, err := r.readUint64()
	if err != nil {
		return compile.SegmentPlan{}, err
	}
	return compile.SegmentPlan{
		MergeKey:              mergeKey,
		Install:               compile.SegmentInstallMode(install),
		MaterializeMergeKey:   materializeMergeKey,
		MaterializeInstall:    compile.SegmentInstallMode(materializeInstall),
		CanAppend:             canAppend,
		CanMaterialize:        canMaterialize,
		RequiresMaterialize:   requiresMaterialize,
		EstimatedPayloadBytes: estimatedPayloadBytes,
		OperationCount:        uint32(operationCount),
		MutationCount:         uint32(mutationCount),
	}, nil
}

func writeSegmentMergeKey(out *bytes.Buffer, key compile.SegmentMergeKey) {
	writeUint64(out, uint64(key.MountKeyID))
	writeBool(out, key.HasPrimaryBucket)
	writeUint64(out, uint64(key.PrimaryBucket))
	writeUint64(out, uint64(key.Install))
	writeUint64(out, uint64(key.Durability))
	writeUint64(out, uint64(key.FormatVersion))
}

func readSegmentMergeKey(r *witnessReader) (compile.SegmentMergeKey, error) {
	mountKeyID, err := r.readUint64()
	if err != nil {
		return compile.SegmentMergeKey{}, err
	}
	hasPrimaryBucket, err := r.readBool()
	if err != nil {
		return compile.SegmentMergeKey{}, err
	}
	primaryBucket, err := r.readUint64()
	if err != nil {
		return compile.SegmentMergeKey{}, err
	}
	install, err := r.readUint64()
	if err != nil {
		return compile.SegmentMergeKey{}, err
	}
	durability, err := r.readUint64()
	if err != nil {
		return compile.SegmentMergeKey{}, err
	}
	formatVersion, err := r.readUint64()
	if err != nil {
		return compile.SegmentMergeKey{}, err
	}
	return compile.SegmentMergeKey{
		MountKeyID:       model.MountKeyID(mountKeyID),
		HasPrimaryBucket: hasPrimaryBucket,
		PrimaryBucket:    fsmeta.AffinityBucket(primaryBucket),
		Install:          compile.SegmentInstallMode(install),
		Durability:       compile.DurabilityClass(durability),
		FormatVersion:    uint16(formatVersion),
	}, nil
}

func writeAtomicityGroup(out *bytes.Buffer, group compile.AtomicityGroup) {
	writeUint64(out, uint64(len(group.Members)))
	for _, member := range group.Members {
		writeUint64(out, uint64(member))
	}
	writeBool(out, group.Splittable)
	writeUint64(out, uint64(group.Recovery))
	writeFixed(out, group.Digest[:])
}

func readAtomicityGroup(r *witnessReader) (compile.AtomicityGroup, error) {
	count, err := r.readUint64()
	if err != nil || count > uint64(maxSegmentSliceLen()) {
		return compile.AtomicityGroup{}, ErrInvalidWitnessRecord
	}
	members := make([]compile.MutationID, 0, count)
	for range count {
		member, err := r.readUint64()
		if err != nil {
			return compile.AtomicityGroup{}, err
		}
		members = append(members, compile.MutationID(member))
	}
	splittable, err := r.readBool()
	if err != nil {
		return compile.AtomicityGroup{}, err
	}
	recovery, err := r.readUint64()
	if err != nil {
		return compile.AtomicityGroup{}, err
	}
	var digest [32]byte
	if err := r.readFixed(digest[:]); err != nil {
		return compile.AtomicityGroup{}, err
	}
	return compile.AtomicityGroup{
		Members:    members,
		Splittable: splittable,
		Recovery:   compile.RecoveryRule(recovery),
		Digest:     digest,
	}, nil
}

func authorityScopeEncodedSize(scope compile.AuthorityScope) int {
	return stringEncodedSize(string(scope.Mount)) + 8 +
		8 + len(scope.Buckets)*8 +
		8 + len(scope.Parents)*8 +
		8 + len(scope.Inodes)*8 +
		2
}

func segmentPlanEncodedSize(compile.SegmentPlan) int {
	return 5*8 + 8 + 5*8 + 8 + 3 + 8 + 8 + 8
}

func atomicityGroupEncodedSize(group compile.AtomicityGroup) int {
	return 8 + len(group.Members)*8 + 1 + 8 + 32
}

func readVisibleAppliedRange(r *witnessReader) (VisibleAppliedRange, error) {
	segmentID, err := r.readUint64()
	if err != nil {
		return VisibleAppliedRange{}, err
	}
	startOffset, err := r.readUint64()
	if err != nil {
		return VisibleAppliedRange{}, err
	}
	endOffset, err := r.readUint64()
	if err != nil {
		return VisibleAppliedRange{}, err
	}
	if segmentID == 0 || segmentID > uint64(^uint32(0)) {
		return VisibleAppliedRange{}, ErrInvalidWitnessRecord
	}
	applied := VisibleAppliedRange{
		SegmentID:   uint32(segmentID),
		StartOffset: startOffset,
		EndOffset:   endOffset,
	}
	return applied, validateVisibleAppliedRange(applied)
}

func readVisibleReplayOperation(r *witnessReader) (ReplayOperation, error) {
	opID, err := r.readOperationID()
	if err != nil {
		return ReplayOperation{}, err
	}
	kind, err := r.readString()
	if err != nil {
		return ReplayOperation{}, err
	}
	var descriptorDigest [32]byte
	if err := r.readFixed(descriptorDigest[:]); err != nil {
		return ReplayOperation{}, err
	}
	var predicateProofDigest [32]byte
	if err := r.readFixed(predicateProofDigest[:]); err != nil {
		return ReplayOperation{}, err
	}
	var executionPlanDigest [32]byte
	if err := r.readFixed(executionPlanDigest[:]); err != nil {
		return ReplayOperation{}, err
	}
	segmentPlan, err := readSegmentPlan(r)
	if err != nil {
		return ReplayOperation{}, err
	}
	atomicity, err := readAtomicityGroup(r)
	if err != nil {
		return ReplayOperation{}, err
	}
	durability, err := r.readUint64()
	if err != nil {
		return ReplayOperation{}, err
	}
	predicateProofs, err := readPredicateProofs(r)
	if err != nil {
		return ReplayOperation{}, err
	}
	guardProofs, err := readGuardProofs(r)
	if err != nil {
		return ReplayOperation{}, err
	}
	count, err := r.readUint64()
	if err != nil || count > uint64(maxSegmentSliceLen()) {
		return ReplayOperation{}, ErrInvalidWitnessRecord
	}
	mutations := make([]ReplayMutation, 0, count)
	for range count {
		key, err := r.readBytes()
		if err != nil {
			return ReplayOperation{}, err
		}
		deleted, err := r.readBool()
		if err != nil {
			return ReplayOperation{}, err
		}
		value, err := r.readBytes()
		if err != nil {
			return ReplayOperation{}, err
		}
		mutations = append(mutations, ReplayMutation{Key: key, Value: value, Delete: deleted})
	}
	op := ReplayOperation{
		OpID:                 opID,
		Kind:                 model.OperationKind(kind),
		DescriptorDigest:     descriptorDigest,
		PredicateProofDigest: predicateProofDigest,
		ExecutionPlanDigest:  executionPlanDigest,
		PredicateProofs:      predicateProofs,
		GuardProofs:          guardProofs,
		Segment:              segmentPlan,
		Atomicity:            atomicity,
		Durability:           compile.DurabilityClass(durability),
		Mutations:            mutations,
	}
	return op, validateVisibleReplayOperation(op)
}
