// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"context"
	"sync"
	"time"

	fsperas "github.com/feichai0017/NoKV/experimental/peras/exec"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/storage/wal"
)

type LocalWitnessReplica struct {
	id  string
	log *WALWitnessLog
}

func NewLocalWitnessReplica(id string, log *WALWitnessLog) (*LocalWitnessReplica, error) {
	if id == "" || log == nil {
		return nil, fsperas.ErrWitnessReplicaInvalid
	}
	return &LocalWitnessReplica{id: id, log: log}, nil
}

func (r *LocalWitnessReplica) ID() string {
	if r == nil {
		return ""
	}
	return r.id
}

func (r *LocalWitnessReplica) AppendSegments(ctx context.Context, _ compile.AuthorityScope, records []fsperas.SegmentWitnessRecord) error {
	if r == nil || r.log == nil {
		return fsperas.ErrWitnessLogRequired
	}
	_, err := r.log.AppendSegments(ctx, records)
	return err
}

func (r *LocalWitnessReplica) Probe(ctx context.Context, epochID uint64) (fsperas.WitnessSnapshot, error) {
	if r == nil || r.log == nil {
		return fsperas.WitnessSnapshot{}, fsperas.ErrWitnessLogRequired
	}
	return r.log.Probe(ctx, epochID)
}

func (r *LocalWitnessReplica) ProbeSegment(ctx context.Context, ref fsperas.WitnessSegmentRef) (fsperas.SegmentWitnessRecord, bool, error) {
	if r == nil || r.log == nil {
		return fsperas.SegmentWitnessRecord{}, false, fsperas.ErrWitnessLogRequired
	}
	return r.log.ProbeSegment(ctx, ref)
}

type WALWitnessLog struct {
	wal        *wal.Manager
	durability wal.DurabilityPolicy
	metrics    witnessLogMetrics
	mu         sync.RWMutex
	segments   []fsperas.SegmentWitnessRecord
}

func NewWALWitnessLog(manager *wal.Manager, durability wal.DurabilityPolicy) (*WALWitnessLog, error) {
	if manager == nil {
		return nil, fsperas.ErrWitnessLogRequired
	}
	return &WALWitnessLog{wal: manager, durability: durability}, nil
}

func (l *WALWitnessLog) Stats() map[string]any {
	if l == nil {
		return emptyWitnessLogStats()
	}
	return l.metrics.Stats()
}

func (l *WALWitnessLog) AppendSegments(ctx context.Context, records []fsperas.SegmentWitnessRecord) (infos []wal.EntryInfo, err error) {
	recordCount := len(records)
	payloadBytes := 0
	defer func() {
		if l != nil {
			l.metrics.recordAppend(recordCount, payloadBytes, err)
		}
	}()
	if err = ctxErr(ctx); err != nil {
		return nil, err
	}
	if l == nil || l.wal == nil {
		return nil, fsperas.ErrWitnessLogRequired
	}
	if len(records) == 0 {
		return nil, nil
	}
	walRecords := make([]wal.Record, 0, len(records))
	encodeStart := time.Now()
	for _, record := range records {
		payload, err := fsperas.EncodeSegmentWitnessRecord(record)
		if err != nil {
			l.metrics.recordEncode(time.Since(encodeStart))
			return nil, err
		}
		payloadBytes += len(payload)
		walRecords = append(walRecords, wal.Record{
			Type:    wal.RecordTypeWitnessEvidence,
			Payload: payload,
		})
	}
	l.metrics.recordEncode(time.Since(encodeStart))
	appendStart := time.Now()
	infos, err = l.wal.AppendRecords(l.durability, walRecords...)
	l.metrics.recordWALAppend(time.Since(appendStart))
	if err != nil {
		return nil, err
	}
	if len(infos) != len(records) {
		return nil, fsperas.ErrInvalidWitnessRecord
	}
	l.mu.Lock()
	l.segments = append(l.segments, records...)
	l.mu.Unlock()
	return infos, nil
}

func (l *WALWitnessLog) Probe(ctx context.Context, epochID uint64) (fsperas.WitnessSnapshot, error) {
	if l == nil || l.wal == nil {
		return fsperas.WitnessSnapshot{}, fsperas.ErrWitnessLogRequired
	}
	segments := make(map[witnessSegmentKey]fsperas.SegmentWitnessRecord)
	err := l.wal.ReplayFiltered(
		func(info wal.EntryInfo) bool {
			return info.Type == wal.RecordTypeWitnessEvidence
		},
		func(_ wal.EntryInfo, payload []byte) error {
			if err := ctxErr(ctx); err != nil {
				return err
			}
			frame, err := fsperas.DecodeWitnessFrame(payload)
			if err != nil {
				return err
			}
			if frame.Kind != fsperas.WitnessRecordSegment {
				return fsperas.ErrInvalidWitnessRecord
			}
			if frame.Segment.EpochID == epochID {
				key := witnessSegmentKey{epochID: frame.Segment.EpochID, root: frame.Segment.SegmentRoot, digest: frame.Segment.SegmentPayloadDigest}
				segments[key] = frame.Segment
			}
			return nil
		},
	)
	if err != nil {
		return fsperas.WitnessSnapshot{}, err
	}
	l.mu.RLock()
	for _, segment := range l.segments {
		if segment.EpochID == epochID {
			key := witnessSegmentKey{epochID: segment.EpochID, root: segment.SegmentRoot, digest: segment.SegmentPayloadDigest}
			segments[key] = segment
		}
	}
	l.mu.RUnlock()
	out := fsperas.WitnessSnapshot{Segments: make([]fsperas.SegmentWitnessRecord, 0, len(segments))}
	for _, segment := range segments {
		out.Segments = append(out.Segments, segment)
	}
	return out, nil
}

func (l *WALWitnessLog) ProbeSegment(ctx context.Context, ref fsperas.WitnessSegmentRef) (fsperas.SegmentWitnessRecord, bool, error) {
	if l == nil || l.wal == nil {
		return fsperas.SegmentWitnessRecord{}, false, fsperas.ErrWitnessLogRequired
	}
	if !ref.Valid() {
		return fsperas.SegmentWitnessRecord{}, false, fsperas.ErrInvalidWitnessRecord
	}
	var out fsperas.SegmentWitnessRecord
	found := false
	err := l.wal.ReplayFiltered(
		func(info wal.EntryInfo) bool {
			return info.Type == wal.RecordTypeWitnessEvidence
		},
		func(_ wal.EntryInfo, payload []byte) error {
			if err := ctxErr(ctx); err != nil {
				return err
			}
			frame, err := fsperas.DecodeWitnessFrame(payload)
			if err != nil {
				return err
			}
			if frame.Kind != fsperas.WitnessRecordSegment {
				return fsperas.ErrInvalidWitnessRecord
			}
			if witnessRecordMatchesRef(frame.Segment, ref) {
				out = frame.Segment
				found = true
			}
			return nil
		},
	)
	if err != nil {
		return fsperas.SegmentWitnessRecord{}, false, err
	}
	l.mu.RLock()
	for _, segment := range l.segments {
		if witnessRecordMatchesRef(segment, ref) {
			out = segment
			found = true
		}
	}
	l.mu.RUnlock()
	return out, found, nil
}

func witnessRecordMatchesRef(record fsperas.SegmentWitnessRecord, ref fsperas.WitnessSegmentRef) bool {
	return record.EpochID == ref.EpochID && record.SegmentRoot == ref.SegmentRoot && record.SegmentPayloadDigest == ref.SegmentPayloadDigest
}

func ctxErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}
