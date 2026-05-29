// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftlog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"

	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/failpoints"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"github.com/feichai0017/NoKV/storage/wal"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

const walRecordOverhead = 8 // length (4 bytes) + checksum (4 bytes)

// WALStorageConfig configures WAL-backed raft storage.
type WALStorageConfig struct {
	GroupID   uint64
	WAL       *wal.Manager
	LocalMeta *localmeta.Store
}

type entrySpan struct {
	firstIndex uint64
	lastIndex  uint64
	segmentID  uint32
	offsets    []uint64
	recordEnd  uint64
}

// WALStorage implements the PeerStorage interface using the shared WAL manager
// plus store-local raft replay metadata. It mirrors the storage layout used in
// TinyKV/TiKV, tracking segments so WAL GC can coordinate across raft groups.
type WALStorage struct {
	mu         sync.Mutex
	groupID    uint64
	wal        *wal.Manager
	localMeta  *localmeta.Store
	mem        *myraft.MemoryStorage
	pointer    localmeta.RaftLogPointer
	entrySpans []entrySpan
}

// OpenWALStorage constructs a WAL-backed raft storage.
func OpenWALStorage(cfg WALStorageConfig) (*WALStorage, error) {
	if cfg.GroupID == 0 {
		return nil, errWALStorageRequiresGroupID
	}
	if cfg.WAL == nil {
		return nil, errWALStorageRequiresManager
	}
	ws := &WALStorage{
		groupID:    cfg.GroupID,
		wal:        cfg.WAL,
		localMeta:  cfg.LocalMeta,
		mem:        myraft.NewMemoryStorage(),
		entrySpans: make([]entrySpan, 0, 16),
	}
	if cfg.LocalMeta != nil {
		if ptr, ok := cfg.LocalMeta.RaftPointer(cfg.GroupID); ok {
			if err := validateRaftPointer(cfg.WAL, ptr); err != nil {
				return nil, err
			}
			ws.pointer = ptr
			if err := ws.restoreCompactedPrefix(ptr); err != nil {
				return nil, err
			}
		}
	}

	replayPtr := ws.pointer

	if err := cfg.WAL.ReplayFiltered(func(info wal.EntryInfo) bool {
		switch info.Type {
		case wal.RecordTypeRaftEntry, wal.RecordTypeRaftState, wal.RecordTypeRaftSnapshot:
			return info.GroupID == cfg.GroupID
		default:
			return false
		}
	}, func(info wal.EntryInfo, payload []byte) error {
		switch info.Type {
		case wal.RecordTypeRaftEntry:
			gid, entries, err := decodeRaftEntries(payload)
			if err != nil {
				return err
			}
			if gid != cfg.GroupID || len(entries) == 0 {
				return nil
			}
			if err := ws.appendReplayEntries(trimCompactedEntries(entries, ws.pointer.TruncatedIndex)); err != nil {
				return err
			}
			ws.recordEntrySpan(info, payload, entries)
			last := entries[len(entries)-1]
			replayPtr.GroupID = cfg.GroupID
			replayPtr.Segment = info.SegmentID
			replayPtr.Offset = recordEnd(info)
			replayPtr.AppliedIndex = last.Index
			replayPtr.AppliedTerm = last.Term
		case wal.RecordTypeRaftState:
			gid, st, err := decodeRaftHardState(payload)
			if err != nil {
				return err
			}
			if gid != cfg.GroupID {
				return nil
			}
			if err := ws.mem.SetHardState(st); err != nil {
				return err
			}
			replayPtr.GroupID = cfg.GroupID
			replayPtr.Segment = info.SegmentID
			replayPtr.Offset = recordEnd(info)
			if st.Term > replayPtr.AppliedTerm {
				replayPtr.AppliedTerm = st.Term
			}
			if st.Commit > replayPtr.Committed {
				replayPtr.Committed = st.Commit
			}
		case wal.RecordTypeRaftSnapshot:
			gid, snap, err := decodeRaftSnapshot(payload)
			if err != nil {
				return err
			}
			if gid != cfg.GroupID || myraft.IsEmptySnap(snap) {
				return nil
			}
			meta := snap.Metadata
			if ws.pointer.TruncatedIndex > 0 && meta.Index < ws.pointer.TruncatedIndex {
				return nil
			}
			if ws.pointer.TruncatedIndex > 0 && meta.Index == ws.pointer.TruncatedIndex {
				if err := ws.hydrateCompactedPrefixSnapshot(snap); err != nil {
					return err
				}
			} else {
				if err := ws.mem.ApplySnapshot(snap); err != nil {
					return err
				}
			}
			replayPtr.GroupID = cfg.GroupID
			replayPtr.Segment = info.SegmentID
			replayPtr.Offset = recordEnd(info)
			replayPtr.SnapshotIndex = meta.Index
			replayPtr.SnapshotTerm = meta.Term
			if meta.Index > replayPtr.AppliedIndex {
				replayPtr.AppliedIndex = meta.Index
				replayPtr.AppliedTerm = meta.Term
			}
			replayPtr.TruncatedIndex = meta.Index
			replayPtr.TruncatedTerm = meta.Term
			if meta.Index > 0 {
				if seg, ok := ws.segmentForIndex(meta.Index); ok {
					replayPtr.SegmentIndex = uint64(seg)
				} else {
					replayPtr.SegmentIndex = uint64(info.SegmentID)
				}
				ws.pruneEntrySpans(meta.Index)
			}
		default:
			return nil
		}
		return nil
	}); err != nil {
		return nil, err
	}

	if isPointerAhead(replayPtr, ws.pointer) {
		if err := ws.updatePointer(replayPtr); err != nil {
			return nil, err
		}
	}

	if ws.pointer.TruncatedIndex > 0 {
		ws.pruneEntrySpans(ws.pointer.TruncatedIndex)
	}

	return ws, nil
}

func (ws *WALStorage) restoreCompactedPrefix(ptr localmeta.RaftLogPointer) error {
	if ptr.TruncatedIndex == 0 {
		return nil
	}
	term := ptr.TruncatedTerm
	if term == 0 && ptr.SnapshotIndex == ptr.TruncatedIndex {
		term = ptr.SnapshotTerm
	}
	return ws.mem.ApplySnapshot(myraft.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index:     ptr.TruncatedIndex,
			Term:      term,
			ConfState: ws.compactedPrefixConfState(),
		},
	})
}

func (ws *WALStorage) hydrateCompactedPrefixSnapshot(snap myraft.Snapshot) error {
	index := snap.Metadata.Index
	hs, _, err := ws.mem.InitialState()
	if err != nil {
		return err
	}
	last, err := ws.mem.LastIndex()
	if err != nil {
		return err
	}
	var suffix []myraft.Entry
	if last > index {
		suffix, err = ws.mem.Entries(index+1, last+1, ^uint64(0))
		if err != nil {
			return err
		}
	}
	mem := myraft.NewMemoryStorage()
	if err := mem.ApplySnapshot(snap); err != nil {
		return err
	}
	if err := mem.SetHardState(hs); err != nil {
		return err
	}
	if len(suffix) > 0 {
		if err := mem.Append(suffix); err != nil {
			return err
		}
	}
	ws.mem = mem
	return nil
}

func (ws *WALStorage) compactedPrefixConfState() raftpb.ConfState {
	if ws == nil || ws.localMeta == nil || ws.groupID == 0 {
		return raftpb.ConfState{}
	}
	meta := ws.localMeta.Snapshot()[ws.groupID]
	if len(meta.Peers) == 0 {
		return raftpb.ConfState{}
	}
	voters := make([]uint64, 0, len(meta.Peers))
	for _, peer := range meta.Peers {
		if peer.PeerID != 0 {
			voters = append(voters, peer.PeerID)
		}
	}
	return raftpb.ConfState{Voters: voters}
}

func (ws *WALStorage) appendReplayEntries(entries []myraft.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	last, err := ws.mem.LastIndex()
	if err != nil {
		return err
	}
	if first := entries[0].Index; first > last+1 {
		return fmt.Errorf("raftstore: raft WAL replay gap for group %d: first entry %d after last %d",
			ws.groupID, first, last)
	}
	return ws.mem.Append(entries)
}

func trimCompactedEntries(entries []myraft.Entry, truncated uint64) []myraft.Entry {
	if truncated == 0 {
		return entries
	}
	for idx, entry := range entries {
		if entry.Index > truncated {
			return entries[idx:]
		}
	}
	return nil
}

// Append persists raft entries via WAL.
func (ws *WALStorage) Append(entries []myraft.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	payload, err := encodeRaftEntries(ws.groupID, entries)
	if err != nil {
		return err
	}

	ws.mu.Lock()
	defer ws.mu.Unlock()

	infos, err := ws.wal.AppendRecords(wal.DurabilityFsyncBatched, wal.Record{
		Type:    wal.RecordTypeRaftEntry,
		Payload: payload,
	})
	if err != nil {
		return err
	}
	if len(infos) != 1 {
		return errExpectedSingleEntryRecord(len(infos))
	}
	if err := ws.mem.Append(entries); err != nil {
		return err
	}
	ws.recordEntrySpan(infos[0], payload, entries)
	last := entries[len(entries)-1]
	ptr := ws.pointer
	ptr.GroupID = ws.groupID
	ptr.Segment = infos[0].SegmentID
	ptr.Offset = recordEnd(infos[0])
	ptr.AppliedIndex = last.Index
	ptr.AppliedTerm = last.Term
	return ws.updatePointer(ptr)
}

// AppendWithHardState persists raft entries and HardState in one WAL batch.
func (ws *WALStorage) AppendWithHardState(entries []myraft.Entry, st myraft.HardState) error {
	if len(entries) == 0 {
		return ws.SetHardState(st)
	}
	if myraft.IsEmptyHardState(st) {
		return ws.Append(entries)
	}
	statePayload, err := encodeRaftHardState(ws.groupID, st)
	if err != nil {
		return err
	}
	entryPayload, err := encodeRaftEntries(ws.groupID, entries)
	if err != nil {
		return err
	}

	ws.mu.Lock()
	defer ws.mu.Unlock()

	infos, err := ws.wal.AppendRecords(wal.DurabilityFsyncBatched,
		wal.Record{Type: wal.RecordTypeRaftState, Payload: statePayload},
		wal.Record{Type: wal.RecordTypeRaftEntry, Payload: entryPayload},
	)
	if err != nil {
		return err
	}
	if len(infos) != 2 {
		return fmt.Errorf("raftstore: expected two coalesced WAL records, got %d", len(infos))
	}
	if err := ws.mem.SetHardState(st); err != nil {
		return err
	}
	if err := ws.mem.Append(entries); err != nil {
		return err
	}
	ws.recordEntrySpan(infos[1], entryPayload, entries)
	last := entries[len(entries)-1]
	ptr := ws.pointer
	ptr.GroupID = ws.groupID
	ptr.Segment = infos[1].SegmentID
	ptr.Offset = recordEnd(infos[1])
	ptr.AppliedIndex = last.Index
	ptr.AppliedTerm = last.Term
	if st.Commit > ptr.Committed {
		ptr.Committed = st.Commit
	}
	if st.Term > ptr.AppliedTerm {
		ptr.AppliedTerm = st.Term
	}
	return ws.updatePointer(ptr)
}

// ApplySnapshot persists and applies a raft snapshot.
func (ws *WALStorage) ApplySnapshot(snap myraft.Snapshot) error {
	if myraft.IsEmptySnap(snap) {
		return nil
	}
	payload, err := encodeRaftSnapshot(ws.groupID, snap)
	if err != nil {
		return err
	}

	ws.mu.Lock()
	defer ws.mu.Unlock()

	infos, err := ws.wal.AppendRecords(wal.DurabilityFsyncBatched, wal.Record{
		Type:    wal.RecordTypeRaftSnapshot,
		Payload: payload,
	})
	if err != nil {
		return err
	}
	if len(infos) != 1 {
		return errExpectedSingleSnapshotRecord(len(infos))
	}
	if err := ws.mem.ApplySnapshot(snap); err != nil {
		return err
	}
	meta := snap.Metadata
	ptr := ws.pointer
	ptr.GroupID = ws.groupID
	ptr.Segment = infos[0].SegmentID
	ptr.Offset = recordEnd(infos[0])
	ptr.SnapshotIndex = meta.Index
	ptr.SnapshotTerm = meta.Term
	if meta.Index > 0 {
		ptr.AppliedIndex = meta.Index
		ptr.AppliedTerm = meta.Term
	}
	ptr.TruncatedIndex = meta.Index
	ptr.TruncatedTerm = meta.Term
	var (
		truncSegment uint32
		truncOffset  uint64
	)
	if meta.Index > 0 {
		if seg, off, ok := ws.offsetAfterIndex(meta.Index); ok {
			truncSegment = seg
			truncOffset = off
		} else if seg, ok := ws.segmentForIndex(meta.Index); ok {
			truncSegment = seg
		} else {
			truncSegment = infos[0].SegmentID
			truncOffset = recordEnd(infos[0])
		}
	}
	ptr.SegmentIndex = uint64(truncSegment)
	ptr.TruncatedOffset = truncOffset
	if err := ws.updatePointer(ptr); err != nil {
		return err
	}
	ws.pruneEntrySpans(meta.Index)
	return nil
}

// SetHardState persists the raft hard state.
func (ws *WALStorage) SetHardState(st myraft.HardState) error {
	if myraft.IsEmptyHardState(st) {
		ws.mu.Lock()
		defer ws.mu.Unlock()
		return ws.mem.SetHardState(st)
	}

	payload, err := encodeRaftHardState(ws.groupID, st)
	if err != nil {
		return err
	}

	ws.mu.Lock()
	defer ws.mu.Unlock()

	infos, err := ws.wal.AppendRecords(wal.DurabilityFsyncBatched, wal.Record{
		Type:    wal.RecordTypeRaftState,
		Payload: payload,
	})
	if err != nil {
		return err
	}
	if len(infos) != 1 {
		return errExpectedSingleHardStateRecord(len(infos))
	}
	if err := ws.mem.SetHardState(st); err != nil {
		return err
	}
	ptr := ws.pointer
	ptr.GroupID = ws.groupID
	ptr.Segment = infos[0].SegmentID
	ptr.Offset = recordEnd(infos[0])
	if st.Commit > ptr.Committed {
		ptr.Committed = st.Commit
	}
	if st.Term > ptr.AppliedTerm {
		ptr.AppliedTerm = st.Term
	}
	return ws.updatePointer(ptr)
}

// MaybeCompact retains only the newest portion of the WAL, mirroring TinyKV's
// behaviour of compacting applied raft log entries.
func (ws *WALStorage) MaybeCompact(applied, retain uint64) error {
	if retain == 0 || applied == 0 || applied <= retain {
		return nil
	}
	target := applied - retain
	if target <= ws.pointer.TruncatedIndex {
		return nil
	}
	return ws.compactTo(target)
}

// Delegated Storage interface methods. WALStorage owns durability, but the
// etcd/raft Storage contract reads from the in-memory mirror; each method
// below is a deliberate adapter and not a one-off accidental wrapper.

// forwarding-ok: WALStorage satisfies raft.Storage; durable WAL ownership lives elsewhere.
func (ws *WALStorage) InitialState() (myraft.HardState, myraft.ConfState, error) {
	return ws.mem.InitialState()
}

// forwarding-ok: WALStorage satisfies raft.Storage; durable WAL ownership lives elsewhere.
func (ws *WALStorage) Entries(lo, hi, maxSize uint64) ([]myraft.Entry, error) {
	return ws.mem.Entries(lo, hi, maxSize)
}

// forwarding-ok: WALStorage satisfies raft.Storage; durable WAL ownership lives elsewhere.
func (ws *WALStorage) Term(i uint64) (uint64, error) {
	return ws.mem.Term(i)
}

// forwarding-ok: WALStorage satisfies raft.Storage; durable WAL ownership lives elsewhere.
func (ws *WALStorage) LastIndex() (uint64, error) {
	return ws.mem.LastIndex()
}

// forwarding-ok: WALStorage satisfies raft.Storage; durable WAL ownership lives elsewhere.
func (ws *WALStorage) FirstIndex() (uint64, error) {
	return ws.mem.FirstIndex()
}

// forwarding-ok: WALStorage satisfies raft.Storage; durable WAL ownership lives elsewhere.
func (ws *WALStorage) Snapshot() (myraft.Snapshot, error) {
	return ws.mem.Snapshot()
}

// Internal helpers ----------------------------------------------------------

func (ws *WALStorage) updatePointer(ptr localmeta.RaftLogPointer) error {
	if ptr.Segment == 0 {
		return nil
	}
	ptr.GroupID = ws.groupID
	if ws.pointer == ptr {
		return nil
	}
	if failpoints.ShouldSkipLocalMetaUpdate() {
		return nil
	}
	if ws.localMeta != nil {
		if err := ws.localMeta.SaveRaftPointer(ptr); err != nil {
			return err
		}
	}
	ws.pointer = ptr
	return nil
}

func (ws *WALStorage) compactTo(index uint64) error {
	if index == 0 {
		return nil
	}
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if index <= ws.pointer.TruncatedIndex {
		return nil
	}
	term, err := ws.mem.Term(index)
	if err != nil && !errors.Is(err, myraft.ErrCompacted) {
		return err
	}
	if err := ws.mem.Compact(index); err != nil && !errors.Is(err, myraft.ErrCompacted) {
		return err
	}
	segment, truncOffset, ok := ws.offsetAfterIndex(index)
	if !ok {
		if ws.pointer.SegmentIndex > 0 {
			segment = uint32(ws.pointer.SegmentIndex)
		} else {
			segment = ws.pointer.Segment
		}
		truncOffset = ws.pointer.TruncatedOffset
	}
	ptr := ws.pointer
	ptr.TruncatedIndex = index
	if term != 0 {
		ptr.TruncatedTerm = term
	}
	ptr.SegmentIndex = uint64(segment)
	ptr.TruncatedOffset = truncOffset
	if err := ws.updatePointer(ptr); err != nil {
		return err
	}
	ws.pruneEntrySpans(index)
	return nil
}

func (ws *WALStorage) recordEntrySpan(info wal.EntryInfo, payload []byte, entries []myraft.Entry) {
	if len(entries) == 0 {
		return
	}
	first := entries[0].Index
	last := entries[len(entries)-1].Index
	if first == 0 || last == 0 {
		return
	}
	offsets := computeEntryOffsets(info, payload, entries)
	trimmed := ws.entrySpans[:0]
	for _, span := range ws.entrySpans {
		if span.lastIndex < first {
			trimmed = append(trimmed, span)
			continue
		}
		if span.firstIndex < first {
			cut := int(first - span.firstIndex)
			if cut > 0 && cut < len(span.offsets) {
				span.recordEnd = span.offsets[cut]
				span.offsets = span.offsets[:cut]
			}
			span.lastIndex = first - 1
			if span.lastIndex >= span.firstIndex {
				trimmed = append(trimmed, span)
			}
		}
		break
	}
	ws.entrySpans = append(trimmed, entrySpan{
		firstIndex: first,
		lastIndex:  last,
		segmentID:  info.SegmentID,
		offsets:    offsets,
		recordEnd:  recordEnd(info),
	})
}

func (ws *WALStorage) segmentForIndex(index uint64) (uint32, bool) {
	if index == 0 {
		return 0, false
	}
	for _, span := range ws.entrySpans {
		if index < span.firstIndex {
			break
		}
		if index <= span.lastIndex {
			return span.segmentID, true
		}
	}
	return 0, false
}

func (ws *WALStorage) pruneEntrySpans(index uint64) {
	if index == 0 {
		return
	}
	n := 0
	for _, span := range ws.entrySpans {
		if span.lastIndex <= index {
			continue
		}
		if span.firstIndex <= index {
			trim := int(index - span.firstIndex + 1)
			if trim >= len(span.offsets) {
				continue
			}
			span.firstIndex = index + 1
			span.offsets = span.offsets[trim:]
		}
		ws.entrySpans[n] = span
		n++
	}
	ws.entrySpans = ws.entrySpans[:n]
}

func (ws *WALStorage) offsetAfterIndex(index uint64) (uint32, uint64, bool) {
	if index == 0 {
		return 0, 0, false
	}
	for _, span := range ws.entrySpans {
		if index < span.firstIndex {
			break
		}
		if index <= span.lastIndex {
			if len(span.offsets) == 0 {
				return span.segmentID, span.recordEnd, true
			}
			pos := int(index - span.firstIndex)
			if pos+1 < len(span.offsets) {
				return span.segmentID, span.offsets[pos+1], true
			}
			return span.segmentID, span.recordEnd, true
		}
	}
	return 0, 0, false
}

func computeEntryOffsets(info wal.EntryInfo, payload []byte, entries []myraft.Entry) []uint64 {
	if len(entries) == 0 || len(payload) == 0 {
		return nil
	}
	base := uint64(info.Offset) + 4 + 1 // length prefix + record type
	idx := 0
	if _, err := readUvarint(payload, &idx); err != nil {
		return nil
	}
	if _, err := readUvarint(payload, &idx); err != nil {
		return nil
	}
	offsets := make([]uint64, 0, len(entries))
	for range entries {
		offsets = append(offsets, base+uint64(idx))
		size, err := readUvarint(payload, &idx)
		if err != nil {
			return offsets
		}
		if idx+int(size) > len(payload) {
			return offsets
		}
		idx += int(size)
	}
	return offsets
}

func recordEnd(info wal.EntryInfo) uint64 {
	return uint64(info.Offset) + uint64(info.Length) + walRecordOverhead
}

func isPointerAhead(newPtr, oldPtr localmeta.RaftLogPointer) bool {
	if newPtr.Segment == 0 {
		return false
	}
	if oldPtr.Segment == 0 {
		return true
	}
	if newPtr.Segment != oldPtr.Segment {
		return newPtr.Segment > oldPtr.Segment
	}
	return newPtr.Offset > oldPtr.Offset
}

func validateRaftPointer(walMgr *wal.Manager, ptr localmeta.RaftLogPointer) error {
	if walMgr == nil {
		return nil
	}
	if ptr.Segment == 0 || ptr.Offset == 0 {
		return nil
	}
	var (
		found     bool
		recType   wal.RecordType
		ptrOffset = ptr.Offset
	)
	err := walMgr.ReplaySegment(ptr.Segment, func(info wal.EntryInfo, _ []byte) error {
		end := recordEnd(info)
		if end == ptrOffset {
			found = true
			recType = info.Type
			return errStopPointerValidation
		}
		if end > ptrOffset {
			return fmt.Errorf("raftstore: local raft pointer offset %d falls within record ending at %d (segment=%d)",
				ptrOffset, end, ptr.Segment)
		}
		return nil
	})
	if errors.Is(err, errStopPointerValidation) {
		err = nil
	}
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("raftstore: local raft pointer offset %d not found in segment %d", ptrOffset, ptr.Segment)
	}
	switch recType {
	case wal.RecordTypeRaftEntry, wal.RecordTypeRaftState, wal.RecordTypeRaftSnapshot:
		return nil
	default:
		return fmt.Errorf("raftstore: local raft pointer references non-raft record type %d in segment %d",
			recType, ptr.Segment)
	}
}

func encodeRaftEntries(groupID uint64, entries []myraft.Entry) ([]byte, error) {
	var buf bytes.Buffer
	writeUvarint(&buf, groupID)
	writeUvarint(&buf, uint64(len(entries)))
	for _, entry := range entries {
		data, err := entry.Marshal()
		if err != nil {
			return nil, err
		}
		writeUvarint(&buf, uint64(len(data)))
		if _, err := buf.Write(data); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func decodeRaftEntries(data []byte) (uint64, []myraft.Entry, error) {
	idx := 0
	groupID, err := readUvarint(data, &idx)
	if err != nil {
		return 0, nil, err
	}
	count64, err := readUvarint(data, &idx)
	if err != nil {
		return 0, nil, err
	}
	if count64 > uint64(len(data)) {
		return 0, nil, io.ErrUnexpectedEOF
	}
	count := int(count64)
	entries := make([]myraft.Entry, 0, count)
	for range count {
		size, err := readUvarint(data, &idx)
		if err != nil {
			return 0, nil, err
		}
		if idx+int(size) > len(data) {
			return 0, nil, io.ErrUnexpectedEOF
		}
		var entry myraft.Entry
		if err := entry.Unmarshal(data[idx : idx+int(size)]); err != nil {
			return 0, nil, err
		}
		idx += int(size)
		entries = append(entries, entry)
	}
	return groupID, entries, nil
}

func encodeRaftHardState(groupID uint64, st myraft.HardState) ([]byte, error) {
	data, err := st.Marshal()
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	writeUvarint(&buf, groupID)
	writeUvarint(&buf, uint64(len(data)))
	if _, err := buf.Write(data); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeRaftHardState(data []byte) (uint64, myraft.HardState, error) {
	var st myraft.HardState
	idx := 0
	groupID, err := readUvarint(data, &idx)
	if err != nil {
		return 0, st, err
	}
	size, err := readUvarint(data, &idx)
	if err != nil {
		return 0, st, err
	}
	if idx+int(size) > len(data) {
		return 0, st, io.ErrUnexpectedEOF
	}
	if err := st.Unmarshal(data[idx : idx+int(size)]); err != nil {
		return 0, st, err
	}
	return groupID, st, nil
}

func encodeRaftSnapshot(groupID uint64, snap myraft.Snapshot) ([]byte, error) {
	pbSnap := raftpb.Snapshot(snap)
	data, err := pbSnap.Marshal()
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	writeUvarint(&buf, groupID)
	writeUvarint(&buf, uint64(len(data)))
	if _, err := buf.Write(data); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeRaftSnapshot(data []byte) (uint64, myraft.Snapshot, error) {
	var snap myraft.Snapshot
	idx := 0
	groupID, err := readUvarint(data, &idx)
	if err != nil {
		return 0, snap, err
	}
	size, err := readUvarint(data, &idx)
	if err != nil {
		return 0, snap, err
	}
	if idx+int(size) > len(data) {
		return 0, snap, io.ErrUnexpectedEOF
	}
	if err := (*raftpb.Snapshot)(&snap).Unmarshal(data[idx : idx+int(size)]); err != nil {
		return 0, snap, err
	}
	return groupID, snap, nil
}

func writeUvarint(buf *bytes.Buffer, value uint64) {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], value)
	buf.Write(tmp[:n])
}

func readUvarint(data []byte, idx *int) (uint64, error) {
	if *idx >= len(data) {
		return 0, io.ErrUnexpectedEOF
	}
	val, n := binary.Uvarint(data[*idx:])
	if n <= 0 {
		return 0, io.ErrUnexpectedEOF
	}
	*idx += n
	return val, nil
}
