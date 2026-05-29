// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	fsperas "github.com/feichai0017/NoKV/experimental/peras/exec"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/proof"
	"github.com/feichai0017/NoKV/storage/wal"
)

type WALVisibleLog struct {
	wal        *wal.Manager
	durability wal.DurabilityPolicy
	mu         sync.RWMutex
	records    []fsperas.VisibleOperationRecord
	pending    map[visibleOperationLogKey]visibleOperationLogEntry
	replayed   bool

	appendMu    sync.Mutex
	appendQ     chan visibleAppendRequest
	closed      chan struct{}
	appendDone  chan struct{}
	closeOnce   sync.Once
	batchAppend bool

	retainAppliedRecords bool
}

const (
	visibleLogAppendQueueSize = 4096
	visibleLogAppendBatchSize = 256
	visibleLogPayloadPoolCap  = 256 << 10
)

var visibleLogPayloadPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 0, 4<<10)
		return &buf
	},
}

func encodeVisibleOperationPayload(record fsperas.VisibleOperationRecord) ([]byte, func(), error) {
	bufRef := visibleLogPayloadPool.Get().(*[]byte)
	payload, err := fsperas.EncodeVisibleOperationRecordTo((*bufRef)[:0], record)
	if err != nil {
		releaseVisibleOperationPayload((*bufRef)[:0])
		return nil, nil, err
	}
	return payload, func() { releaseVisibleOperationPayload(payload) }, nil
}

func releaseVisibleOperationPayload(payload []byte) {
	if cap(payload) > visibleLogPayloadPoolCap {
		return
	}
	buf := payload[:0]
	visibleLogPayloadPool.Put(&buf)
}

type visibleOperationLogKey struct {
	epoch  uint64
	holder string
	id     fsperas.OperationID
}

type visibleAppliedLogKey struct {
	epoch  uint64
	holder string
}

type visibleOperationLogEntry struct {
	reference fsperas.VisibleOperationReference
	position  visibleOperationLogPosition
}

type visibleAppendRequest struct {
	record  fsperas.VisibleOperationRecord
	payload []byte
	release func()
	result  chan error
}

type visibleOperationLogPosition struct {
	segmentID   uint32
	startOffset uint64
	endOffset   uint64
}

func NewWALVisibleLog(manager *wal.Manager, durability wal.DurabilityPolicy) (*WALVisibleLog, error) {
	if manager == nil {
		return nil, fsperas.ErrVisibleLogRequired
	}
	log := &WALVisibleLog{
		wal:        manager,
		durability: durability,
		pending:    make(map[visibleOperationLogKey]visibleOperationLogEntry),
	}
	switch durability {
	case wal.DurabilityFsync, wal.DurabilityFsyncBatched:
		log.appendQ = make(chan visibleAppendRequest, visibleLogAppendQueueSize)
		log.closed = make(chan struct{})
		log.appendDone = make(chan struct{})
		log.batchAppend = true
		go log.runVisibleAppendLoop()
	}
	return log, nil
}

func (l *WALVisibleLog) VisibleLogPolicy() string {
	if l == nil {
		return "disabled"
	}
	return walDurabilityPolicyName(l.durability)
}

// SetRetainAppliedRecords keeps visible operation records physically present
// after applied markers are written. Local materialized Peras uses those
// records as its durable completion index after it opts out of catalog records.
func (l *WALVisibleLog) SetRetainAppliedRecords(retain bool) {
	if l == nil {
		return
	}
	l.mu.Lock()
	l.retainAppliedRecords = retain
	l.mu.Unlock()
}

func (l *WALVisibleLog) AppendVisible(ctx context.Context, record fsperas.VisibleOperationRecord) error {
	if err := visibleLogContextErr(ctx); err != nil {
		return err
	}
	var appendErr error
	if l != nil && l.batchAppend {
		payload, release, err := encodeVisibleOperationPayload(record)
		if err != nil {
			return fmt.Errorf("encode peras visible record: %w", err)
		}
		appendErr = l.enqueueVisibleRecord(ctx, record, payload, release)
	} else {
		payload, release, err := encodeVisibleOperationPayload(record)
		if err != nil {
			return fmt.Errorf("encode peras visible record: %w", err)
		}
		defer release()
		appendErr = l.appendVisibleRecord(record, payload)
	}
	if appendErr != nil {
		return fmt.Errorf("append peras visible WAL: %w", appendErr)
	}
	return nil
}

func (l *WALVisibleLog) appendVisibleRecord(record fsperas.VisibleOperationRecord, payload []byte) error {
	info, err := l.appendPayload(payload)
	if err != nil {
		return err
	}
	position, err := visibleOperationLogPositionFromEntry(info)
	if err != nil {
		return err
	}
	reference := visibleOperationReferenceForRecord(record)
	l.mu.Lock()
	l.rememberVisibleRecordLocked(record, reference, position)
	l.mu.Unlock()
	return nil
}

func (l *WALVisibleLog) enqueueVisibleRecord(ctx context.Context, record fsperas.VisibleOperationRecord, payload []byte, release func()) error {
	if l == nil || l.wal == nil {
		if release != nil {
			release()
		}
		return fsperas.ErrVisibleLogRequired
	}
	if ctx == nil {
		ctx = context.Background()
	}
	result := make(chan error, 1)
	req := visibleAppendRequest{
		record:  record,
		payload: payload,
		release: release,
		result:  result,
	}
	l.appendMu.Lock()
	select {
	case <-l.closed:
		l.appendMu.Unlock()
		if release != nil {
			release()
		}
		return ErrVisibleLogClosed
	default:
	}
	select {
	case l.appendQ <- req:
		l.appendMu.Unlock()
	case <-ctx.Done():
		l.appendMu.Unlock()
		if release != nil {
			release()
		}
		return ctx.Err()
	}
	return <-result
}

func (l *WALVisibleLog) runVisibleAppendLoop() {
	defer close(l.appendDone)
	for {
		select {
		case req := <-l.appendQ:
			l.appendVisibleBatch(req)
		case <-l.closed:
			for {
				select {
				case req := <-l.appendQ:
					l.appendVisibleBatch(req)
				default:
					return
				}
			}
		}
	}
}

func (l *WALVisibleLog) appendVisibleBatch(first visibleAppendRequest) {
	batch := make([]visibleAppendRequest, 0, visibleLogAppendBatchSize)
	batch = append(batch, first)
	for len(batch) < visibleLogAppendBatchSize {
		select {
		case req := <-l.appendQ:
			batch = append(batch, req)
		default:
			l.appendVisibleBatchNow(batch)
			return
		}
	}
	l.appendVisibleBatchNow(batch)
}

func (l *WALVisibleLog) appendVisibleBatchNow(batch []visibleAppendRequest) {
	payloads := make([][]byte, len(batch))
	for i, req := range batch {
		payloads[i] = req.payload
	}
	infos, err := l.appendPayloads(payloads)
	if err != nil {
		completeVisibleAppendBatch(batch, err)
		return
	}
	if len(infos) != len(batch) {
		completeVisibleAppendBatch(batch, fsperas.ErrInvalidWitnessRecord)
		return
	}
	positions := make([]visibleOperationLogPosition, len(infos))
	references := make([]fsperas.VisibleOperationReference, len(batch))
	for i, info := range infos {
		position, err := visibleOperationLogPositionFromEntry(info)
		if err != nil {
			completeVisibleAppendBatch(batch, err)
			return
		}
		positions[i] = position
		references[i] = visibleOperationReferenceForRecord(batch[i].record)
	}
	l.mu.Lock()
	for i, req := range batch {
		l.rememberVisibleRecordLocked(req.record, references[i], positions[i])
	}
	l.mu.Unlock()
	completeVisibleAppendBatch(batch, nil)
}

func completeVisibleAppendBatch(batch []visibleAppendRequest, err error) {
	for _, req := range batch {
		if req.release != nil {
			req.release()
		}
		req.result <- err
	}
}

func (l *WALVisibleLog) rememberVisibleRecordLocked(record fsperas.VisibleOperationRecord, reference fsperas.VisibleOperationReference, position visibleOperationLogPosition) {
	l.initPendingLocked()
	l.records = append(l.records, record)
	l.pending[visibleOperationLogKeyForRecord(record)] = visibleOperationLogEntry{
		reference: reference,
		position:  position,
	}
}

func (l *WALVisibleLog) AppendVisibleApplied(ctx context.Context, record fsperas.VisibleAppliedRecord) error {
	if err := visibleLogContextErr(ctx); err != nil {
		return err
	}
	payload, err := fsperas.EncodeVisibleAppliedRecord(record)
	if err != nil {
		return err
	}
	if _, err := l.appendPayload(payload); err != nil {
		return err
	}
	l.mu.Lock()
	l.records = removeVisibleAppliedRecords(l.records, l.pending, record)
	l.removeAppliedPendingLocked(record)
	canCompact := l.replayed && !l.retainAppliedRecords
	l.mu.Unlock()
	if canCompact {
		_ = l.CompactApplied()
	}
	return nil
}

func (l *WALVisibleLog) AppendVisibleReplayPlanApplied(ctx context.Context, epochID uint64, holderID string, plan fsperas.ReplayPlan) error {
	if err := visibleLogContextErr(ctx); err != nil {
		return err
	}
	record, err := l.visibleAppliedRecordForReplayPlan(epochID, holderID, plan)
	if err != nil {
		return err
	}
	return l.AppendVisibleApplied(ctx, record)
}

func (l *WALVisibleLog) visibleAppliedRecordForReplayPlan(epochID uint64, holderID string, plan fsperas.ReplayPlan) (fsperas.VisibleAppliedRecord, error) {
	if l == nil || l.wal == nil {
		return fsperas.VisibleAppliedRecord{}, fsperas.ErrVisibleLogRequired
	}
	if epochID == 0 || holderID == "" || len(plan.Operations) == 0 {
		return fsperas.VisibleAppliedRecord{}, fsperas.ErrInvalidWitnessRecord
	}
	positions := make([]visibleOperationLogPosition, 0, len(plan.Operations))
	l.mu.RLock()
	defer l.mu.RUnlock()
	for _, op := range plan.Operations {
		if !op.OpID.Valid() {
			return fsperas.VisibleAppliedRecord{}, fsperas.ErrInvalidOperationID
		}
		key := visibleOperationLogKey{epoch: epochID, holder: holderID, id: op.OpID}
		entry, ok := l.pending[key]
		if !ok {
			return fsperas.VisibleAppliedRecord{}, fsperas.ErrInvalidWitnessRecord
		}
		if err := visibleOperationMatchesReplay(entry.reference, op); err != nil {
			return fsperas.VisibleAppliedRecord{}, err
		}
		positions = append(positions, entry.position)
	}
	return fsperas.VisibleAppliedRecord{
		EpochID:  epochID,
		HolderID: holderID,
		Ranges:   coalesceVisibleAppliedRanges(positions),
	}, nil
}

func (l *WALVisibleLog) ReplayVisible(ctx context.Context) ([]fsperas.VisibleOperationRecord, error) {
	states, err := l.ReplayVisibleState(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]fsperas.VisibleOperationRecord, 0, len(states))
	for _, state := range states {
		if state.Applied {
			continue
		}
		out = append(out, state.Record)
	}
	return out, nil
}

func (l *WALVisibleLog) ReplayVisibleState(ctx context.Context) ([]VisibleLogStateRecord, error) {
	if l == nil || l.wal == nil {
		return nil, fsperas.ErrVisibleLogRequired
	}
	records := make([]VisibleLogStateRecord, 0)
	latest := make(map[visibleOperationLogKey]int)
	pending := make(map[visibleOperationLogKey]visibleOperationLogEntry)
	applied := make(map[visibleAppliedLogKey][]fsperas.VisibleAppliedRange)
	err := l.wal.ReplayFiltered(func(info wal.EntryInfo) bool {
		return info.Type == wal.RecordTypeVisibleCommit
	}, func(info wal.EntryInfo, payload []byte) error {
		if err := visibleLogContextErr(ctx); err != nil {
			return err
		}
		if record, err := fsperas.DecodeVisibleAppliedRecord(payload); err == nil {
			key := visibleAppliedLogKey{epoch: record.EpochID, holder: record.HolderID}
			applied[key] = append(applied[key], record.Ranges...)
			return nil
		}
		record, err := fsperas.DecodeVisibleOperationRecord(payload)
		if err != nil {
			return err
		}
		key := visibleOperationLogKeyForRecord(record)
		if idx, ok := latest[key]; ok {
			records[idx].Record = record
			records[idx].Applied = false
		} else {
			latest[key] = len(records)
			records = append(records, VisibleLogStateRecord{Record: record})
		}
		position, err := visibleOperationLogPositionFromEntry(info)
		if err != nil {
			return err
		}
		reference, err := fsperas.VisibleOperationReferenceFromReplay(record.Operation)
		if err != nil {
			return err
		}
		pending[key] = visibleOperationLogEntry{reference: reference, position: position}
		return nil
	})
	if err != nil {
		return nil, err
	}
	for idx, state := range records {
		record := state.Record
		key := visibleOperationLogKeyForRecord(record)
		entry, ok := pending[key]
		if ok && visibleOperationPositionApplied(entry.position, applied[visibleAppliedLogKey{epoch: key.epoch, holder: key.holder}]) {
			delete(pending, key)
			records[idx].Applied = true
		}
	}
	l.mu.Lock()
	l.records = clonePendingVisibleStateRecords(records)
	l.pending = pending
	l.replayed = true
	l.mu.Unlock()
	return cloneVisibleLogStateRecords(records), nil
}

func (l *WALVisibleLog) Records() []fsperas.VisibleOperationRecord {
	if l == nil {
		return nil
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	return cloneVisibleOperationRecords(l.records)
}

func (l *WALVisibleLog) CompactApplied() error {
	if l == nil || l.wal == nil {
		return fsperas.ErrVisibleLogRequired
	}
	l.mu.RLock()
	retainApplied := l.retainAppliedRecords
	l.mu.RUnlock()
	if retainApplied {
		return nil
	}
	retain := l.firstRetainedSegment()
	if retain <= 1 {
		return nil
	}
	files, err := l.wal.ListSegments()
	if err != nil {
		return err
	}
	for _, file := range files {
		id, ok := visibleWALSegmentID(file)
		if !ok || id >= retain {
			continue
		}
		if err := l.wal.RemoveSegment(id); err != nil && !errors.Is(err, wal.ErrSegmentRetained) {
			return err
		}
	}
	return nil
}

func (l *WALVisibleLog) firstRetainedSegment() uint32 {
	active := l.wal.ActiveSegment()
	retain := active
	l.mu.RLock()
	defer l.mu.RUnlock()
	for _, entry := range l.pending {
		if entry.position.segmentID == 0 {
			continue
		}
		if retain == 0 || entry.position.segmentID < retain {
			retain = entry.position.segmentID
		}
	}
	return retain
}

func (l *WALVisibleLog) appendPayload(payload []byte) (wal.EntryInfo, error) {
	infos, err := l.appendPayloads([][]byte{payload})
	if err != nil {
		return wal.EntryInfo{}, err
	}
	if len(infos) != 1 {
		return wal.EntryInfo{}, fsperas.ErrInvalidWitnessRecord
	}
	return infos[0], nil
}

func (l *WALVisibleLog) appendPayloads(payloads [][]byte) ([]wal.EntryInfo, error) {
	if l == nil || l.wal == nil {
		return nil, fsperas.ErrVisibleLogRequired
	}
	if len(payloads) == 0 {
		return nil, nil
	}
	records := make([]wal.Record, len(payloads))
	for i, payload := range payloads {
		records[i] = wal.Record{
			Type:    wal.RecordTypeVisibleCommit,
			Payload: payload,
		}
	}
	return l.wal.AppendRecords(l.durability, records...)
}

func visibleLogContextErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func visibleOperationMatchesReplay(current fsperas.VisibleOperationReference, applied fsperas.ReplayOperation) error {
	appliedRef, err := fsperas.VisibleOperationReferenceFromReplay(applied)
	if err != nil {
		return err
	}
	if current != appliedRef {
		return fsperas.ErrInvalidWitnessRecord
	}
	return nil
}

func visibleOperationReferenceForRecord(record fsperas.VisibleOperationRecord) fsperas.VisibleOperationReference {
	op := record.Operation
	return fsperas.VisibleOperationReference{
		OpID:                 op.OpID,
		DescriptorDigest:     op.DescriptorDigest,
		PredicateProofDigest: op.PredicateProofDigest,
		ExecutionPlanDigest:  op.ExecutionPlanDigest,
	}
}

func visibleOperationLogPositionFromEntry(info wal.EntryInfo) (visibleOperationLogPosition, error) {
	if info.SegmentID == 0 || info.Offset < 0 || info.Length == 0 {
		return visibleOperationLogPosition{}, fsperas.ErrInvalidWitnessRecord
	}
	start := uint64(info.Offset)
	end := start + uint64(info.Length)
	if end <= start {
		return visibleOperationLogPosition{}, fsperas.ErrInvalidWitnessRecord
	}
	return visibleOperationLogPosition{
		segmentID:   info.SegmentID,
		startOffset: start,
		endOffset:   end,
	}, nil
}

func coalesceVisibleAppliedRanges(positions []visibleOperationLogPosition) []fsperas.VisibleAppliedRange {
	if len(positions) == 0 {
		return nil
	}
	sort.Slice(positions, func(i, j int) bool {
		if positions[i].segmentID != positions[j].segmentID {
			return positions[i].segmentID < positions[j].segmentID
		}
		if positions[i].startOffset != positions[j].startOffset {
			return positions[i].startOffset < positions[j].startOffset
		}
		return positions[i].endOffset < positions[j].endOffset
	})
	ranges := make([]fsperas.VisibleAppliedRange, 0, len(positions))
	for _, position := range positions {
		if len(ranges) > 0 {
			last := &ranges[len(ranges)-1]
			if last.SegmentID == position.segmentID && last.EndOffset == position.startOffset {
				last.EndOffset = position.endOffset
				continue
			}
			if last.SegmentID == position.segmentID && last.StartOffset == position.startOffset && last.EndOffset == position.endOffset {
				continue
			}
		}
		ranges = append(ranges, fsperas.VisibleAppliedRange{
			SegmentID:   position.segmentID,
			StartOffset: position.startOffset,
			EndOffset:   position.endOffset,
		})
	}
	return ranges
}

func visibleOperationPositionApplied(position visibleOperationLogPosition, ranges []fsperas.VisibleAppliedRange) bool {
	if position.segmentID == 0 || len(ranges) == 0 {
		return false
	}
	for _, applied := range ranges {
		if applied.SegmentID == position.segmentID &&
			applied.StartOffset <= position.startOffset &&
			applied.EndOffset >= position.endOffset {
			return true
		}
	}
	return false
}

func removeVisibleAppliedRecords(records []fsperas.VisibleOperationRecord, pending map[visibleOperationLogKey]visibleOperationLogEntry, applied fsperas.VisibleAppliedRecord) []fsperas.VisibleOperationRecord {
	if len(records) == 0 || len(applied.Ranges) == 0 {
		return records
	}
	out := records[:0]
	for _, record := range records {
		if record.EpochID == applied.EpochID && record.HolderID == applied.HolderID {
			key := visibleOperationLogKeyForRecord(record)
			entry, ok := pending[key]
			if ok && visibleOperationPositionApplied(entry.position, applied.Ranges) {
				continue
			}
		}
		out = append(out, record)
	}
	return out
}

func (l *WALVisibleLog) removeAppliedPendingLocked(applied fsperas.VisibleAppliedRecord) {
	l.initPendingLocked()
	for key, entry := range l.pending {
		if key.epoch == applied.EpochID && key.holder == applied.HolderID && visibleOperationPositionApplied(entry.position, applied.Ranges) {
			delete(l.pending, key)
		}
	}
}

func (l *WALVisibleLog) initPendingLocked() {
	if l.pending == nil {
		l.pending = make(map[visibleOperationLogKey]visibleOperationLogEntry)
	}
}

func visibleOperationLogKeyForRecord(record fsperas.VisibleOperationRecord) visibleOperationLogKey {
	return visibleOperationLogKey{
		epoch:  record.EpochID,
		holder: record.HolderID,
		id:     record.Operation.OpID,
	}
}

func visibleWALSegmentID(path string) (uint32, bool) {
	name := filepath.Base(path)
	name = strings.TrimSuffix(name, ".wal")
	id, err := strconv.ParseUint(name, 10, 32)
	if err != nil || id == 0 {
		return 0, false
	}
	return uint32(id), true
}

func walDurabilityPolicyName(policy wal.DurabilityPolicy) string {
	switch policy {
	case wal.DurabilityBuffered:
		return "buffered"
	case wal.DurabilityFlushed:
		return "flushed"
	case wal.DurabilityFsync:
		return "fsync"
	case wal.DurabilityFsyncBatched:
		return "fsync-batched"
	default:
		return "unknown"
	}
}

func (l *WALVisibleLog) Close() {
	if l == nil {
		return
	}
	l.closeOnce.Do(func() {
		if !l.batchAppend {
			return
		}
		l.appendMu.Lock()
		close(l.closed)
		l.appendMu.Unlock()
		<-l.appendDone
	})
}

func cloneVisibleOperationRecords(in []fsperas.VisibleOperationRecord) []fsperas.VisibleOperationRecord {
	if len(in) == 0 {
		return nil
	}
	out := make([]fsperas.VisibleOperationRecord, len(in))
	for i, record := range in {
		out[i] = cloneVisibleOperationRecord(record)
	}
	return out
}

func clonePendingVisibleStateRecords(in []VisibleLogStateRecord) []fsperas.VisibleOperationRecord {
	if len(in) == 0 {
		return nil
	}
	out := make([]fsperas.VisibleOperationRecord, 0, len(in))
	for _, record := range in {
		if record.Applied {
			continue
		}
		out = append(out, cloneVisibleOperationRecord(record.Record))
	}
	return out
}

func cloneVisibleLogStateRecords(in []VisibleLogStateRecord) []VisibleLogStateRecord {
	if len(in) == 0 {
		return nil
	}
	out := make([]VisibleLogStateRecord, len(in))
	for i, record := range in {
		out[i] = VisibleLogStateRecord{
			Record:  cloneVisibleOperationRecord(record.Record),
			Applied: record.Applied,
		}
	}
	return out
}

func cloneVisibleOperationRecord(record fsperas.VisibleOperationRecord) fsperas.VisibleOperationRecord {
	record.Scope = CloneScope(record.Scope)
	record.Operation = cloneReplayOperation(record.Operation)
	return record
}

func cloneReplayOperation(op fsperas.ReplayOperation) fsperas.ReplayOperation {
	op.PredicateProofs = clonePredicateProofs(op.PredicateProofs)
	op.GuardProofs = append([]proof.GuardProof(nil), op.GuardProofs...)
	op.Atomicity.Members = append([]compile.MutationID(nil), op.Atomicity.Members...)
	op.Mutations = cloneReplayMutations(op.Mutations)
	return op
}

func clonePredicateProofs(in []proof.PredicateProof) []proof.PredicateProof {
	if len(in) == 0 {
		return nil
	}
	out := make([]proof.PredicateProof, len(in))
	for i, predicate := range in {
		out[i] = predicate
		out[i].Key = cloneBytes(predicate.Key)
		out[i].Value = cloneBytes(predicate.Value)
	}
	return out
}

func cloneReplayMutations(in []fsperas.ReplayMutation) []fsperas.ReplayMutation {
	if len(in) == 0 {
		return nil
	}
	out := make([]fsperas.ReplayMutation, len(in))
	for i, mutation := range in {
		out[i] = fsperas.ReplayMutation{
			Key:    cloneBytes(mutation.Key),
			Value:  cloneBytes(mutation.Value),
			Delete: mutation.Delete,
		}
	}
	return out
}

var (
	_ fsperas.VisibleLog         = (*WALVisibleLog)(nil)
	_ fsperas.VisibleLogApplier  = (*WALVisibleLog)(nil)
	_ fsperas.VisibleLogReplayer = (*WALVisibleLog)(nil)
	_ visibleReplayPlanApplier   = (*WALVisibleLog)(nil)
)
