// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"bytes"
	"crypto/sha256"
	"io"
	"slices"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/feichai0017/NoKV/fsmeta/proof"
)

var perasSegmentMagic = [4]byte{'N', 'P', 'S', 2}

// SegmentRecordClass is the fsmeta key family stored in one sealed Peras
// segment. The class is diagnostic and query-planning metadata; the storage
// key remains the source of truth.
type SegmentRecordClass uint8

const (
	SegmentRecordOther SegmentRecordClass = iota
	SegmentRecordDentry
	SegmentRecordInode
	SegmentRecordChunk
	SegmentRecordSession
	SegmentRecordUsage
)

func (c SegmentRecordClass) String() string {
	switch c {
	case SegmentRecordDentry:
		return "dentry"
	case SegmentRecordInode:
		return "inode"
	case SegmentRecordChunk:
		return "chunk"
	case SegmentRecordSession:
		return "session"
	case SegmentRecordUsage:
		return "usage"
	default:
		return "other"
	}
}

type SegmentKV struct {
	Class  SegmentRecordClass
	Key    []byte
	Value  []byte
	Delete bool
}

type SegmentCompletion struct {
	OpID                 OperationID
	Kind                 model.OperationKind
	Version              uint64
	MutationCount        uint32
	DescriptorDigest     [32]byte
	PredicateProofDigest [32]byte
	ExecutionPlanDigest  [32]byte
	PredicateProofs      []proof.PredicateProof
	GuardProofs          []proof.GuardProof
}

type SegmentStats struct {
	OperationCount       uint64
	InputMutationCount   uint64
	EntryCount           uint64
	CompletionCount      uint64
	CoalescedMutations   uint64
	CompressionRatio     float64
	OperationsPerSegment float64
}

type SegmentReadHeader struct {
	FirstKey       []byte
	LastKey        []byte
	EntryCount     uint64
	DentryCount    uint64
	InodeCount     uint64
	SessionCount   uint64
	TombstoneCount uint64
	DirectoryCount uint64
}

// PerasSegment is the authority-local install unit produced by sealing a
// batch of Peras operations. It is queryable before later materialization into
// the ordinary MVCC layout.
type PerasSegment struct {
	EpochID  uint64
	Versions ReplayVersionRange
	Root     [32]byte

	Dentries   []SegmentKV
	Inodes     []SegmentKV
	Chunks     []SegmentKV
	Sessions   []SegmentKV
	Usage      []SegmentKV
	Other      []SegmentKV
	Tombstones []SegmentKV

	Completions []SegmentCompletion
	ReadHeader  SegmentReadHeader

	entries            []SegmentKV
	completionIndex    map[OperationID]int
	inputMutationCount uint64
}

func (s PerasSegment) ForEachEntry(fn func(SegmentKV) error) error {
	if fn == nil {
		return nil
	}
	for _, entry := range s.entries {
		if err := fn(entry); err != nil {
			return err
		}
	}
	return nil
}

func EncodePerasSegment(segment PerasSegment) ([]byte, error) {
	if err := validatePerasSegmentPayload(segment); err != nil {
		return nil, err
	}
	var out bytes.Buffer
	out.Grow(perasSegmentPayloadEncodedSize(segment))
	writeFixed(&out, perasSegmentMagic[:])
	writeUint64(&out, segment.EpochID)
	writeUint64(&out, segment.Versions.First)
	writeUint64(&out, segment.Versions.Count)
	writeFixed(&out, segment.Root[:])
	writeUint64(&out, segment.inputMutationCount)
	writeUint64(&out, uint64(len(segment.entries)))
	for _, entry := range segment.entries {
		writeUint64(&out, uint64(entry.Class))
		writeBytes(&out, entry.Key)
		writeBool(&out, entry.Delete)
		writeBytes(&out, entry.Value)
	}
	writeUint64(&out, uint64(len(segment.Completions)))
	for _, completion := range segment.Completions {
		writeSegmentCompletion(&out, completion)
	}
	return out.Bytes(), nil
}

func DecodePerasSegment(payload []byte) (PerasSegment, error) {
	r := witnessReader{buf: payload}
	if err := r.readMagic(perasSegmentMagic); err != nil {
		return PerasSegment{}, ErrInvalidPerasSegment
	}
	epochID, err := r.readUint64()
	if err != nil {
		return PerasSegment{}, ErrInvalidPerasSegment
	}
	firstVersion, err := r.readUint64()
	if err != nil {
		return PerasSegment{}, ErrInvalidPerasSegment
	}
	versionCount, err := r.readUint64()
	if err != nil {
		return PerasSegment{}, ErrInvalidPerasSegment
	}
	var root [32]byte
	if err := r.readFixed(root[:]); err != nil {
		return PerasSegment{}, ErrInvalidPerasSegment
	}
	inputMutationCount, err := r.readUint64()
	if err != nil {
		return PerasSegment{}, ErrInvalidPerasSegment
	}
	entryCount, err := r.readUint64()
	if err != nil {
		return PerasSegment{}, ErrInvalidPerasSegment
	}
	if entryCount > uint64(maxSegmentSliceLen()) {
		return PerasSegment{}, ErrInvalidPerasSegment
	}
	entries := make([]SegmentKV, 0, entryCount)
	for range entryCount {
		class, err := r.readUint64()
		if err != nil {
			return PerasSegment{}, ErrInvalidPerasSegment
		}
		key, err := r.readBytes()
		if err != nil {
			return PerasSegment{}, ErrInvalidPerasSegment
		}
		deleted, err := r.readBool()
		if err != nil {
			return PerasSegment{}, ErrInvalidPerasSegment
		}
		value, err := r.readBytes()
		if err != nil {
			return PerasSegment{}, ErrInvalidPerasSegment
		}
		entries = append(entries, SegmentKV{
			Class:  SegmentRecordClass(class),
			Key:    key,
			Value:  value,
			Delete: deleted,
		})
	}
	completionCount, err := r.readUint64()
	if err != nil {
		return PerasSegment{}, ErrInvalidPerasSegment
	}
	if completionCount > uint64(maxSegmentSliceLen()) {
		return PerasSegment{}, ErrInvalidPerasSegment
	}
	completions := make([]SegmentCompletion, 0, completionCount)
	for range completionCount {
		completion, err := readSegmentCompletion(&r)
		if err != nil {
			return PerasSegment{}, ErrInvalidPerasSegment
		}
		completions = append(completions, completion)
	}
	if !r.done() {
		return PerasSegment{}, ErrInvalidPerasSegment
	}
	segment := PerasSegment{
		EpochID:            epochID,
		Versions:           ReplayVersionRange{First: firstVersion, Count: versionCount},
		Root:               root,
		Completions:        completions,
		entries:            entries,
		completionIndex:    make(map[OperationID]int, len(completions)),
		inputMutationCount: inputMutationCount,
	}
	for i, completion := range segment.Completions {
		segment.completionIndex[completion.OpID] = i
	}
	segment.assignRuns(entries)
	segment.ReadHeader = buildSegmentReadHeader(entries)
	if err := validatePerasSegmentPayload(segment); err != nil {
		return PerasSegment{}, err
	}
	return segment, nil
}

func PerasSegmentPayloadDigest(payload []byte) ([32]byte, error) {
	if len(payload) == 0 {
		return [32]byte{}, ErrInvalidPerasSegment
	}
	return sha256.Sum256(payload), nil
}

func VerifyPerasSegmentPayload(payload []byte, expectedRoot, expectedDigest [32]byte) (PerasSegment, error) {
	if expectedRoot == ([32]byte{}) || expectedDigest == ([32]byte{}) {
		return PerasSegment{}, ErrInvalidPerasSegment
	}
	actualDigest, err := PerasSegmentPayloadDigest(payload)
	if err != nil {
		return PerasSegment{}, err
	}
	if actualDigest != expectedDigest {
		return PerasSegment{}, ErrInvalidPerasSegment
	}
	segment, err := DecodePerasSegment(payload)
	if err != nil {
		return PerasSegment{}, err
	}
	if segment.Root != expectedRoot {
		return PerasSegment{}, ErrInvalidPerasSegment
	}
	return segment, nil
}

func BuildPerasSegmentFromReplayPlan(plan ReplayPlan) (PerasSegment, error) {
	if plan.EpochID == 0 || len(plan.Operations) == 0 {
		return PerasSegment{}, ErrInvalidPerasSegment
	}
	if !plan.Versions.Empty() {
		if err := plan.Versions.ValidateForOperationCount(uint64(len(plan.Operations))); err != nil {
			return PerasSegment{}, err
		}
	}

	totalMutations := replayPlanMutationCount(plan)
	latest := make(map[string]SegmentKV, totalMutations)
	completions := make([]SegmentCompletion, 0, len(plan.Operations))
	var mutationCount uint64
	for opOffset, op := range plan.Operations {
		if !op.OpID.Valid() || len(op.Mutations) == 0 {
			return PerasSegment{}, ErrInvalidPerasSegment
		}
		proofDigest := compile.AdmissionProofSetDigest(op.PredicateProofs, op.GuardProofs)
		if op.PredicateProofDigest != ([32]byte{}) {
			if op.PredicateProofDigest != proofDigest {
				return PerasSegment{}, ErrInvalidPerasSegment
			}
			proofDigest = op.PredicateProofDigest
		}
		version := uint64(0)
		if !plan.Versions.Empty() {
			v, err := plan.Versions.VersionAt(uint64(opOffset))
			if err != nil {
				return PerasSegment{}, err
			}
			version = v
		}
		completions = append(completions, SegmentCompletion{
			OpID:                 op.OpID,
			Kind:                 op.Kind,
			Version:              version,
			MutationCount:        uint32(len(op.Mutations)),
			DescriptorDigest:     op.DescriptorDigest,
			PredicateProofDigest: proofDigest,
			ExecutionPlanDigest:  replayOperationExecutionPlanDigest(op),
			PredicateProofs:      clonePredicateProofs(op.PredicateProofs),
			GuardProofs:          cloneGuardProofs(op.GuardProofs),
		})
		for _, mutation := range op.Mutations {
			if len(mutation.Key) == 0 || (!mutation.Delete && mutation.Value == nil) {
				return PerasSegment{}, ErrInvalidPerasSegment
			}
			kv := SegmentKV{
				Class:  classifySegmentKey(mutation.Key),
				Key:    cloneBytes(mutation.Key),
				Value:  cloneBytes(mutation.Value),
				Delete: mutation.Delete,
			}
			latest[string(mutation.Key)] = kv
			mutationCount++
		}
	}

	entries := make([]SegmentKV, 0, len(latest))
	for _, kv := range latest {
		entries = append(entries, kv)
	}
	slices.SortFunc(entries, compareSegmentKV)

	segment := PerasSegment{
		EpochID:            plan.EpochID,
		Versions:           plan.Versions,
		Completions:        completions,
		entries:            entries,
		completionIndex:    make(map[OperationID]int, len(completions)),
		inputMutationCount: mutationCount,
	}
	for i, completion := range segment.Completions {
		segment.completionIndex[completion.OpID] = i
	}
	segment.assignRuns(entries)
	segment.ReadHeader = buildSegmentReadHeader(entries)
	segment.Root = segmentRoot(segment)
	return segment, nil
}

func replayPlanMutationCount(plan ReplayPlan) int {
	count := 0
	for _, op := range plan.Operations {
		count += len(op.Mutations)
	}
	return count
}

func (s PerasSegment) Get(key []byte) (value []byte, deleted bool, ok bool) {
	value, deleted, ok = s.GetView(key)
	if !ok {
		return nil, false, false
	}
	return cloneBytes(value), deleted, true
}

// GetView returns the segment-owned value bytes. Callers must not mutate the
// returned slice.
func (s PerasSegment) GetView(key []byte) (value []byte, deleted bool, ok bool) {
	i, found := s.find(key)
	if !found {
		return nil, false, false
	}
	entry := s.entries[i]
	return entry.Value, entry.Delete, true
}

func (s PerasSegment) Scan(start []byte, limit uint32) []SegmentKV {
	return cloneSegmentKVs(s.ScanView(start, limit))
}

// ScanView returns a segment-owned sorted suffix. Callers must not mutate the
// returned slice or the bytes inside it.
func (s PerasSegment) ScanView(start []byte, limit uint32) []SegmentKV {
	if limit == 0 || len(s.entries) == 0 {
		return nil
	}
	i, _ := s.lowerBound(start)
	if i >= len(s.entries) {
		return nil
	}
	end := min(i+int(limit), len(s.entries))
	return s.entries[i:end]
}

func (s PerasSegment) Completion(id OperationID) (SegmentCompletion, bool) {
	if !id.Valid() {
		return SegmentCompletion{}, false
	}
	if s.completionIndex != nil {
		i, ok := s.completionIndex[id]
		if !ok {
			return SegmentCompletion{}, false
		}
		return s.Completions[i], true
	}
	for _, completion := range s.Completions {
		if completion.OpID == id {
			return completion, true
		}
	}
	return SegmentCompletion{}, false
}

func (s PerasSegment) Entries() []SegmentKV {
	return cloneSegmentKVs(s.entries)
}

// EntriesView returns the segment-owned sorted entries. Callers must not mutate
// the returned slice or any nested byte slice.
func (s PerasSegment) EntriesView() []SegmentKV {
	return s.entries
}

func (s PerasSegment) FirstKey() ([]byte, error) {
	if err := validatePerasSegmentPayload(s); err != nil {
		return nil, err
	}
	return cloneBytes(s.entries[0].Key), nil
}

func (s PerasSegment) Stats() SegmentStats {
	entryCount := uint64(len(s.entries))
	stats := SegmentStats{
		OperationCount:       uint64(len(s.Completions)),
		InputMutationCount:   s.inputMutationCount,
		EntryCount:           entryCount,
		CompletionCount:      uint64(len(s.Completions)),
		OperationsPerSegment: float64(len(s.Completions)),
	}
	if s.inputMutationCount > entryCount {
		stats.CoalescedMutations = s.inputMutationCount - entryCount
	}
	if entryCount == 0 {
		stats.CompressionRatio = 1
	} else {
		stats.CompressionRatio = float64(s.inputMutationCount) / float64(entryCount)
	}
	return stats
}

func (s PerasSegment) ReadHeaderView() SegmentReadHeader {
	header := s.ReadHeader
	header.FirstKey = cloneBytes(header.FirstKey)
	header.LastKey = cloneBytes(header.LastKey)
	return header
}

func validatePerasSegmentPayload(segment PerasSegment) error {
	if segment.EpochID == 0 || segment.Root == ([32]byte{}) || len(segment.entries) == 0 || len(segment.Completions) == 0 {
		return ErrInvalidPerasSegment
	}
	if segment.Versions.Count != 0 && segment.Versions.Count != uint64(len(segment.Completions)) {
		return ErrInvalidPerasSegment
	}
	if segment.inputMutationCount < uint64(len(segment.entries)) {
		return ErrInvalidPerasSegment
	}
	for i, entry := range segment.entries {
		if len(entry.Key) == 0 || (!entry.Delete && entry.Value == nil) {
			return ErrInvalidPerasSegment
		}
		if uint64(len(entry.Key)) > uint64(^uint32(0)) || uint64(len(entry.Value)) > uint64(^uint32(0)) {
			return ErrInvalidPerasSegment
		}
		if i > 0 && bytes.Compare(segment.entries[i-1].Key, entry.Key) >= 0 {
			return ErrInvalidPerasSegment
		}
	}
	seen := make(map[OperationID]struct{}, len(segment.Completions))
	var completionMutationCount uint64
	for _, completion := range segment.Completions {
		if !completion.OpID.Valid() || completion.MutationCount == 0 {
			return ErrInvalidPerasSegment
		}
		if _, ok := seen[completion.OpID]; ok {
			return ErrInvalidPerasSegment
		}
		seen[completion.OpID] = struct{}{}
		completionMutationCount += uint64(completion.MutationCount)
		if uint64(len(completion.OpID.ClientID)) > uint64(^uint32(0)) || uint64(len(completion.Kind)) > uint64(^uint32(0)) {
			return ErrInvalidPerasSegment
		}
		if compile.AdmissionProofSetDigest(completion.PredicateProofs, completion.GuardProofs) != completion.PredicateProofDigest {
			return ErrInvalidPerasSegment
		}
	}
	if completionMutationCount != segment.inputMutationCount {
		return ErrInvalidPerasSegment
	}
	if segmentRoot(segment) != segment.Root {
		return ErrInvalidPerasSegment
	}
	return nil
}

func perasSegmentPayloadEncodedSize(segment PerasSegment) int {
	size := len(perasSegmentMagic) + 8 + 8 + 8 + 32 + 8 + 8 + 8
	for _, entry := range segment.entries {
		size += 8 + 4 + len(entry.Key) + 1 + 4 + len(entry.Value)
	}
	for _, completion := range segment.Completions {
		size += segmentCompletionEncodedSize(completion)
	}
	return size
}

func writeSegmentCompletion(out io.Writer, completion SegmentCompletion) {
	writeOperationID(out, completion.OpID)
	writeString(out, string(completion.Kind))
	writeUint64(out, completion.Version)
	writeUint64(out, uint64(completion.MutationCount))
	writeFixed(out, completion.DescriptorDigest[:])
	writeFixed(out, completion.PredicateProofDigest[:])
	writeFixed(out, completion.ExecutionPlanDigest[:])
	writePredicateProofs(out, completion.PredicateProofs)
	writeGuardProofs(out, completion.GuardProofs)
}

func readSegmentCompletion(r *witnessReader) (SegmentCompletion, error) {
	opID, err := r.readOperationID()
	if err != nil {
		return SegmentCompletion{}, err
	}
	kind, err := r.readString()
	if err != nil {
		return SegmentCompletion{}, err
	}
	version, err := r.readUint64()
	if err != nil {
		return SegmentCompletion{}, err
	}
	mutationCount, err := r.readUint64()
	if err != nil || mutationCount > uint64(^uint32(0)) {
		return SegmentCompletion{}, ErrInvalidPerasSegment
	}
	var descriptorDigest [32]byte
	if err := r.readFixed(descriptorDigest[:]); err != nil {
		return SegmentCompletion{}, err
	}
	var predicateProofDigest [32]byte
	if err := r.readFixed(predicateProofDigest[:]); err != nil {
		return SegmentCompletion{}, err
	}
	var executionPlanDigest [32]byte
	if err := r.readFixed(executionPlanDigest[:]); err != nil {
		return SegmentCompletion{}, err
	}
	predicateProofs, err := readPredicateProofs(r)
	if err != nil {
		return SegmentCompletion{}, err
	}
	guardProofs, err := readGuardProofs(r)
	if err != nil {
		return SegmentCompletion{}, err
	}
	return SegmentCompletion{
		OpID:                 opID,
		Kind:                 model.OperationKind(kind),
		Version:              version,
		MutationCount:        uint32(mutationCount),
		DescriptorDigest:     descriptorDigest,
		PredicateProofDigest: predicateProofDigest,
		ExecutionPlanDigest:  executionPlanDigest,
		PredicateProofs:      predicateProofs,
		GuardProofs:          guardProofs,
	}, nil
}

func writePredicateProofs(out io.Writer, proofs []proof.PredicateProof) {
	writeUint64(out, uint64(len(proofs)))
	for _, proof := range proofs {
		writeUint64(out, uint64(proof.SchemaVersion))
		writeString(out, string(proof.Rule))
		writeBytes(out, proof.Key)
		writeBool(out, proof.Present)
		writeBytes(out, proof.Value)
		writeUint64(out, proof.Version)
		writeUint64(out, uint64(proof.Source))
		writeUint64(out, proof.ProofFrontier.EpochID)
		writeUint64(out, proof.ProofFrontier.Sequence)
		writeUint64(out, uint64(proof.ProofKind))
		writeFixed(out, proof.ScopeDigest[:])
		writeFixed(out, proof.Digest[:])
	}
}

func readPredicateProofs(r *witnessReader) ([]proof.PredicateProof, error) {
	count, err := r.readUint64()
	if err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, nil
	}
	if count > uint64(maxSegmentSliceLen()) {
		return nil, ErrInvalidPerasSegment
	}
	proofs := make([]proof.PredicateProof, 0, count)
	for range count {
		schemaVersion, err := r.readUint64()
		if err != nil {
			return nil, err
		}
		rule, err := r.readString()
		if err != nil {
			return nil, err
		}
		key, err := r.readBytes()
		if err != nil {
			return nil, err
		}
		present, err := r.readBool()
		if err != nil {
			return nil, err
		}
		value, err := r.readBytes()
		if err != nil {
			return nil, err
		}
		version, err := r.readUint64()
		if err != nil {
			return nil, err
		}
		source, err := r.readUint64()
		if err != nil {
			return nil, err
		}
		epochID, err := r.readUint64()
		if err != nil {
			return nil, err
		}
		sequence, err := r.readUint64()
		if err != nil {
			return nil, err
		}
		proofKind, err := r.readUint64()
		if err != nil {
			return nil, err
		}
		var scopeDigest [32]byte
		if err := r.readFixed(scopeDigest[:]); err != nil {
			return nil, err
		}
		var digest [32]byte
		if err := r.readFixed(digest[:]); err != nil {
			return nil, err
		}
		predicateProof := proof.PredicateProof{
			SchemaVersion: proof.Version(schemaVersion),
			Rule:          proof.RuleID(rule),
			Key:           key,
			Present:       present,
			Value:         value,
			Version:       version,
			Source:        proof.ReadSource(source),
			ProofFrontier: proof.ProofFrontier{EpochID: epochID, Sequence: sequence},
			ProofKind:     proof.PredicateProofKind(proofKind),
			ScopeDigest:   scopeDigest,
			Digest:        digest,
		}
		if err := proof.VerifyPredicateProof(predicateProof); err != nil {
			return nil, ErrInvalidPerasSegment
		}
		proofs = append(proofs, predicateProof)
	}
	return proofs, nil
}

func writeGuardProofs(out io.Writer, proofs []proof.GuardProof) {
	writeUint64(out, uint64(len(proofs)))
	for _, proof := range proofs {
		writeUint64(out, uint64(proof.SchemaVersion))
		writeString(out, string(proof.Guard))
		writeBool(out, proof.Passed)
		writeGuardEvidence(out, proof.Evidence)
		writeFixed(out, proof.Digest[:])
	}
}

func readGuardProofs(r *witnessReader) ([]proof.GuardProof, error) {
	count, err := r.readUint64()
	if err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, nil
	}
	if count > uint64(maxSegmentSliceLen()) {
		return nil, ErrInvalidPerasSegment
	}
	proofs := make([]proof.GuardProof, 0, count)
	for range count {
		schemaVersion, err := r.readUint64()
		if err != nil {
			return nil, err
		}
		guard, err := r.readString()
		if err != nil {
			return nil, err
		}
		passed, err := r.readBool()
		if err != nil {
			return nil, err
		}
		evidence, err := readGuardEvidence(r)
		if err != nil {
			return nil, err
		}
		var digest [32]byte
		if err := r.readFixed(digest[:]); err != nil {
			return nil, err
		}
		guardProof := proof.GuardProof{
			SchemaVersion: proof.Version(schemaVersion),
			Guard:         proof.RuleID(guard),
			Passed:        passed,
			Evidence:      evidence,
			Digest:        digest,
		}
		if guardProof.SchemaVersion != proof.Version1 || guardProof.Evidence.SchemaVersion != proof.Version1 {
			return nil, ErrInvalidPerasSegment
		}
		if proof.GuardProofDigest(guardProof.Guard, guardProof.Passed, guardProof.Evidence) != guardProof.Digest {
			return nil, ErrInvalidPerasSegment
		}
		proofs = append(proofs, guardProof)
	}
	return proofs, nil
}

func writeGuardEvidence(out io.Writer, evidence proof.GuardEvidence) {
	writeUint64(out, uint64(evidence.SchemaVersion))
	writeString(out, string(evidence.Guard))
	writeUint64(out, uint64(evidence.Kind))
	writeFixed(out, evidence.DescriptorDigest[:])
	writeFixed(out, evidence.PredicateProofDigest[:])
	writeFixed(out, evidence.FootprintDigest[:])
	writeFixed(out, evidence.EffectDigest[:])
	writeFixed(out, evidence.SubjectDigest[:])
	writeUint64(out, evidence.ProofFrontier.EpochID)
	writeUint64(out, evidence.ProofFrontier.Sequence)
}

func readGuardEvidence(r *witnessReader) (proof.GuardEvidence, error) {
	schemaVersion, err := r.readUint64()
	if err != nil {
		return proof.GuardEvidence{}, err
	}
	guard, err := r.readString()
	if err != nil {
		return proof.GuardEvidence{}, err
	}
	kind, err := r.readUint64()
	if err != nil {
		return proof.GuardEvidence{}, err
	}
	var descriptorDigest [32]byte
	if err := r.readFixed(descriptorDigest[:]); err != nil {
		return proof.GuardEvidence{}, err
	}
	var predicateProofDigest [32]byte
	if err := r.readFixed(predicateProofDigest[:]); err != nil {
		return proof.GuardEvidence{}, err
	}
	var footprintDigest [32]byte
	if err := r.readFixed(footprintDigest[:]); err != nil {
		return proof.GuardEvidence{}, err
	}
	var effectDigest [32]byte
	if err := r.readFixed(effectDigest[:]); err != nil {
		return proof.GuardEvidence{}, err
	}
	var subjectDigest [32]byte
	if err := r.readFixed(subjectDigest[:]); err != nil {
		return proof.GuardEvidence{}, err
	}
	epochID, err := r.readUint64()
	if err != nil {
		return proof.GuardEvidence{}, err
	}
	sequence, err := r.readUint64()
	if err != nil {
		return proof.GuardEvidence{}, err
	}
	return proof.GuardEvidence{
		SchemaVersion:        proof.Version(schemaVersion),
		Guard:                proof.RuleID(guard),
		Kind:                 proof.GuardEvidenceKind(kind),
		DescriptorDigest:     descriptorDigest,
		PredicateProofDigest: predicateProofDigest,
		FootprintDigest:      footprintDigest,
		EffectDigest:         effectDigest,
		SubjectDigest:        subjectDigest,
		ProofFrontier:        proof.ProofFrontier{EpochID: epochID, Sequence: sequence},
	}, nil
}

func segmentCompletionEncodedSize(completion SegmentCompletion) int {
	size := stringEncodedSize(completion.OpID.ClientID) + 8 + stringEncodedSize(string(completion.Kind)) + 8 + 8 + 32 + 32 + 32
	size += 8
	for _, proof := range completion.PredicateProofs {
		size += 8 + stringEncodedSize(string(proof.Rule)) + 4 + len(proof.Key) + 1 + 4 + len(proof.Value) + 8 + 8 + 8 + 8 + 8 + 32 + 32
	}
	size += 8
	for _, proof := range completion.GuardProofs {
		size += 8 + stringEncodedSize(string(proof.Guard)) + 1 + guardEvidenceEncodedSize(proof.Evidence) + 32
	}
	return size
}

func guardEvidenceEncodedSize(evidence proof.GuardEvidence) int {
	return 8 + stringEncodedSize(string(evidence.Guard)) + 8 + 32 + 32 + 32 + 32 + 32 + 8 + 8
}

func maxSegmentSliceLen() int {
	return int(^uint(0) >> 1)
}

func (s *PerasSegment) assignRuns(entries []SegmentKV) {
	for _, entry := range entries {
		if entry.Delete {
			s.Tombstones = append(s.Tombstones, entry)
			continue
		}
		switch entry.Class {
		case SegmentRecordDentry:
			s.Dentries = append(s.Dentries, entry)
		case SegmentRecordInode:
			s.Inodes = append(s.Inodes, entry)
		case SegmentRecordChunk:
			s.Chunks = append(s.Chunks, entry)
		case SegmentRecordSession:
			s.Sessions = append(s.Sessions, entry)
		case SegmentRecordUsage:
			s.Usage = append(s.Usage, entry)
		default:
			s.Other = append(s.Other, entry)
		}
	}
}

func buildSegmentReadHeader(entries []SegmentKV) SegmentReadHeader {
	if len(entries) == 0 {
		return SegmentReadHeader{}
	}
	header := SegmentReadHeader{
		FirstKey:   cloneBytes(entries[0].Key),
		LastKey:    cloneBytes(entries[len(entries)-1].Key),
		EntryCount: uint64(len(entries)),
	}
	directories := make(map[string]struct{})
	for _, entry := range entries {
		if entry.Delete {
			header.TombstoneCount++
		}
		switch entry.Class {
		case SegmentRecordDentry:
			header.DentryCount++
			if prefix, ok := dentryDirectoryPrefix(entry.Key); ok {
				directories[prefix] = struct{}{}
			}
		case SegmentRecordInode:
			header.InodeCount++
		case SegmentRecordSession:
			header.SessionCount++
		}
	}
	header.DirectoryCount = uint64(len(directories))
	return header
}

func (s PerasSegment) find(key []byte) (int, bool) {
	i, equal := s.lowerBound(key)
	return i, equal
}

func (s PerasSegment) lowerBound(key []byte) (int, bool) {
	i, j := 0, len(s.entries)
	for i < j {
		h := int(uint(i+j) >> 1)
		if bytes.Compare(s.entries[h].Key, key) < 0 {
			i = h + 1
		} else {
			j = h
		}
	}
	return i, i < len(s.entries) && bytes.Equal(s.entries[i].Key, key)
}

func compareSegmentKV(left, right SegmentKV) int {
	if cmp := bytes.Compare(left.Key, right.Key); cmp != 0 {
		return cmp
	}
	if left.Delete == right.Delete {
		return 0
	}
	if left.Delete {
		return 1
	}
	return -1
}

func classifySegmentKey(key []byte) SegmentRecordClass {
	kind, err := layout.KeyKindOf(key)
	if err != nil {
		return SegmentRecordOther
	}
	switch kind {
	case layout.KeyKindDentry:
		return SegmentRecordDentry
	case layout.KeyKindInode:
		return SegmentRecordInode
	case layout.KeyKindChunk:
		return SegmentRecordChunk
	case layout.KeyKindSession:
		return SegmentRecordSession
	case layout.KeyKindUsage:
		return SegmentRecordUsage
	default:
		return SegmentRecordOther
	}
}

func segmentRoot(segment PerasSegment) [32]byte {
	h := sha256.New()
	writeFixed(h, perasSegmentMagic[:])
	writeUint64(h, segment.EpochID)
	writeUint64(h, segment.Versions.First)
	writeUint64(h, segment.Versions.Count)
	writeUint64(h, uint64(len(segment.entries)))
	for _, entry := range segment.entries {
		writeUint64(h, uint64(entry.Class))
		writeBytesHash(h, entry.Key)
		writeBool(h, entry.Delete)
		writeBytesHash(h, entry.Value)
	}
	writeUint64(h, uint64(len(segment.Completions)))
	for _, completion := range segment.Completions {
		writeSegmentCompletion(h, completion)
	}
	return digestFromHash(h.Sum(nil))
}

func cloneSegmentKV(in SegmentKV) SegmentKV {
	return SegmentKV{
		Class:  in.Class,
		Key:    cloneBytes(in.Key),
		Value:  cloneBytes(in.Value),
		Delete: in.Delete,
	}
}

func cloneSegmentKVs(in []SegmentKV) []SegmentKV {
	if len(in) == 0 {
		return nil
	}
	out := make([]SegmentKV, 0, len(in))
	for _, kv := range in {
		out = append(out, cloneSegmentKV(kv))
	}
	return out
}
