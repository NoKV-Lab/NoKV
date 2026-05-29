// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package localmeta

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	metaregion "github.com/feichai0017/NoKV/meta/region"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/feichai0017/NoKV/storage/vfs"
	"google.golang.org/protobuf/proto"
)

const (
	// ReplicaStateFileName is the durable local replica catalog file used by one
	// store for restart recovery. The payload is one binary protobuf blob encoded
	// as metapb.ReplicaLocalCatalog.
	ReplicaStateFileName = "replicas.binpb"
	// RaftProgressFileName is the durable local raft progress file used by one
	// store for WAL/apply recovery. The payload is one binary protobuf blob
	// encoded as metapb.RaftProgressCatalog.
	RaftProgressFileName = "raft-progress.binpb"
	// PendingRootEventsFileName stores locally applied rooted events that still
	// need a successful publish acknowledgement from the coordinator.
	PendingRootEventsFileName = "pending-root-events.binpb"
	// PendingSchedulerOperationsFileName stores scheduler decisions that must
	// survive restart until the local executor applies them.
	PendingSchedulerOperationsFileName = "pending-scheduler-operations.binpb"
	// BlockedRootEventsFileName stores locally applied rooted events that were
	// permanently rejected by the coordinator and therefore require repair
	// instead of automatic retry.
	BlockedRootEventsFileName = "blocked-root-events.binpb"
)

var (
	// maxPendingRootEvents bounds the locally durable rooted-event backlog so a
	// disconnected coordinator cannot grow restart recovery state without limit.
	maxPendingRootEvents = 10_000
	// maxBlockedRootEvents bounds the durable reconciliation backlog after
	// permanent rooted publish rejection.
	maxBlockedRootEvents = 10_000
	// maxPendingSchedulerOperations bounds the locally durable scheduler
	// decision backlog.
	maxPendingSchedulerOperations = 10_000
)

type diskState struct {
	Regions                    map[uint64]RegionMeta
	RaftPointers               map[uint64]RaftLogPointer
	PendingRootEvents          map[uint64]PendingRootEvent
	PendingSchedulerOperations map[string]PendingSchedulerOperation
	BlockedRootEvents          map[uint64]BlockedRootEvent
}

// Store persists store-local region metadata used only for local recovery.
//
// Files:
//   - replicas.binpb      -> metapb.ReplicaLocalCatalog
//   - raft-progress.binpb -> metapb.RaftProgressCatalog
//
// This store is not cluster authority and must not be treated as routing truth.
type Store struct {
	fs      vfs.FS
	workdir string

	mu    sync.RWMutex
	state diskState

	// persistedRaftPointers mirrors the last successfully written raft-progress
	// catalog. SaveRaftPointer may keep fresher in-memory progress for runtime
	// retention, so checkpoint decisions must use this durable baseline to make a
	// failed checkpoint retryable.
	persistedRaftPointers map[uint64]RaftLogPointer
}

// WorkDir returns the local metadata directory backing this store.
func (s *Store) WorkDir() string {
	if s == nil {
		return ""
	}
	return s.workdir
}

// OpenLocalStore opens the local raftstore metadata store in workdir.
func OpenLocalStore(workdir string, fs vfs.FS) (*Store, error) {
	workdir = strings.TrimSpace(workdir)
	if workdir == "" {
		return nil, fmt.Errorf("raftstore/localmeta: workdir is required")
	}
	fs = vfs.Ensure(fs)
	if err := fs.MkdirAll(workdir, 0o755); err != nil {
		return nil, err
	}
	state, err := loadState(fs, workdir)
	if err != nil {
		return nil, err
	}
	return &Store{
		fs:                    fs,
		workdir:               workdir,
		state:                 state,
		persistedRaftPointers: CloneRaftPointers(state.RaftPointers),
	}, nil
}

// Snapshot returns a deep copy of the local peer catalog.
func (s *Store) Snapshot() map[uint64]RegionMeta {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return CloneRegionMetas(s.state.Regions)
}

// RaftPointer returns the latest locally recorded WAL pointer for one raft group.
func (s *Store) RaftPointer(groupID uint64) (RaftLogPointer, bool) {
	if s == nil || groupID == 0 {
		return RaftLogPointer{}, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	ptr, ok := s.state.RaftPointers[groupID]
	return ptr, ok
}

// RaftPointerSnapshot returns a copy of all locally recorded WAL pointers.
func (s *Store) RaftPointerSnapshot() map[uint64]RaftLogPointer {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return CloneRaftPointers(s.state.RaftPointers)
}

// DurableRaftPointerSnapshot returns the raft pointers known to be present in
// the store-local raft progress catalog. WAL retention must use this lower
// bound instead of the hotter in-memory pointers, otherwise a crash after WAL GC
// can leave durable metadata behind the retained raft log.
func (s *Store) DurableRaftPointerSnapshot() map[uint64]RaftLogPointer {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return CloneRaftPointers(s.persistedRaftPointers)
}

// PendingRootEvents returns locally durable rooted events that still require a
// successful publish acknowledgement. Results are sorted by sequence.
func (s *Store) PendingRootEvents() []PendingRootEvent {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.state.PendingRootEvents) == 0 {
		return nil
	}
	keys := sortedPendingSequences(s.state.PendingRootEvents)
	out := make([]PendingRootEvent, 0, len(keys))
	for _, seq := range keys {
		out = append(out, ClonePendingRootEvent(s.state.PendingRootEvents[seq]))
	}
	return out
}

// PendingSchedulerOperations returns locally durable scheduler decisions that
// still need to be applied by the store runtime.
func (s *Store) PendingSchedulerOperations() []PendingSchedulerOperation {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.state.PendingSchedulerOperations) == 0 {
		return nil
	}
	keys := sortedPendingSchedulerOperationKeys(s.state.PendingSchedulerOperations)
	out := make([]PendingSchedulerOperation, 0, len(keys))
	for _, key := range keys {
		out = append(out, ClonePendingSchedulerOperation(s.state.PendingSchedulerOperations[key]))
	}
	return out
}

// BlockedRootEvents returns the durable rooted publish backlog that requires
// explicit repair instead of automatic retry.
func (s *Store) BlockedRootEvents() []BlockedRootEvent {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.state.BlockedRootEvents) == 0 {
		return nil
	}
	keys := sortedBlockedSequences(s.state.BlockedRootEvents)
	out := make([]BlockedRootEvent, 0, len(keys))
	for _, seq := range keys {
		out = append(out, CloneBlockedRootEvent(s.state.BlockedRootEvents[seq]))
	}
	return out
}

// Empty reports whether the local peer catalog is empty.
func (s *Store) Empty() bool {
	if s == nil {
		return true
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.state.Regions) == 0
}

// SaveRegion persists one local region metadata update.
func (s *Store) SaveRegion(meta RegionMeta) error {
	if s == nil {
		return nil
	}
	if meta.ID == 0 {
		return fmt.Errorf("raftstore/localmeta: region id is zero")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state.Regions == nil {
		s.state.Regions = make(map[uint64]RegionMeta)
	}
	s.state.Regions[meta.ID] = CloneRegionMeta(meta)
	return s.persistReplicaCatalogLocked()
}

// DeleteRegion removes one region metadata entry from the local catalog.
func (s *Store) DeleteRegion(regionID uint64) error {
	if s == nil || regionID == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.state.Regions, regionID)
	return s.persistReplicaCatalogLocked()
}

// SaveRaftPointer records the local WAL pointer for one raft group.
//
// The in-memory pointer is always current so runtime WAL retention can see the
// latest raft progress. The on-disk checkpoint is only forced when it advances a
// restart or retention boundary; same-segment offset/index churn is replayable
// from the raft WAL after a crash and must not add a metadata fsync to every
// raft append. The first committed index is a boundary because seeded peers use
// it as durable restart evidence before normal raft replay has run.
func (s *Store) SaveRaftPointer(ptr RaftLogPointer) error {
	if s == nil {
		return nil
	}
	if ptr.GroupID == 0 {
		return fmt.Errorf("raftstore/localmeta: raft pointer group id is zero")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state.RaftPointers == nil {
		s.state.RaftPointers = make(map[uint64]RaftLogPointer)
	}
	prev, existed := s.persistedRaftPointers[ptr.GroupID]
	s.state.RaftPointers[ptr.GroupID] = ptr
	if !raftPointerCheckpointRequired(prev, ptr, existed) {
		return nil
	}
	if err := s.persistRaftProgressCatalogLocked(); err != nil {
		return err
	}
	s.persistedRaftPointers = CloneRaftPointers(s.state.RaftPointers)
	return nil
}

// SavePendingRootEvent persists one locally applied rooted event until
// Coordinator publish acknowledgement succeeds. The caller must invoke this
// before enqueueing the event for publish so a process crash cannot drop
// already-applied topology truth.
func (s *Store) SavePendingRootEvent(event PendingRootEvent) error {
	if s == nil {
		return nil
	}
	if event.Sequence == 0 {
		return fmt.Errorf("raftstore/localmeta: pending rooted event sequence is zero")
	}
	if event.Event.Kind == 0 {
		return fmt.Errorf("raftstore/localmeta: pending rooted event kind is unknown")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state.PendingRootEvents == nil {
		s.state.PendingRootEvents = make(map[uint64]PendingRootEvent)
	}
	if _, exists := s.state.PendingRootEvents[event.Sequence]; !exists && len(s.state.PendingRootEvents) >= maxPendingRootEvents {
		return fmt.Errorf("raftstore/localmeta: pending rooted event limit exceeded (max=%d)", maxPendingRootEvents)
	}
	s.state.PendingRootEvents[event.Sequence] = ClonePendingRootEvent(event)
	return s.persistPendingRootEventCatalogLocked()
}

// DeletePendingRootEvent removes one publish-acknowledged rooted event from the
// local durable pending catalog. Until this returns successfully, restart must
// treat the event as needing republish.
func (s *Store) DeletePendingRootEvent(sequence uint64) error {
	if s == nil || sequence == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.state.PendingRootEvents, sequence)
	return s.persistPendingRootEventCatalogLocked()
}

// SavePendingSchedulerOperation persists one scheduler decision until local
// execution successfully consumes it.
func (s *Store) SavePendingSchedulerOperation(op PendingSchedulerOperation) error {
	if s == nil {
		return nil
	}
	if op.Kind == PendingSchedulerOperationUnknown {
		return fmt.Errorf("raftstore/localmeta: pending scheduler operation kind is unknown")
	}
	if op.RegionID == 0 {
		return fmt.Errorf("raftstore/localmeta: pending scheduler operation region id is zero")
	}
	key := PendingSchedulerOperationKey(op)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state.PendingSchedulerOperations == nil {
		s.state.PendingSchedulerOperations = make(map[string]PendingSchedulerOperation)
	}
	if _, exists := s.state.PendingSchedulerOperations[key]; !exists && len(s.state.PendingSchedulerOperations) >= maxPendingSchedulerOperations {
		return fmt.Errorf("raftstore/localmeta: pending scheduler operation limit exceeded (max=%d)", maxPendingSchedulerOperations)
	}
	s.state.PendingSchedulerOperations[key] = ClonePendingSchedulerOperation(op)
	return s.persistPendingSchedulerOperationCatalogLocked()
}

// DeletePendingSchedulerOperation removes one scheduler decision after local
// execution finishes applying it.
func (s *Store) DeletePendingSchedulerOperation(kind PendingSchedulerOperationKind, regionID uint64) error {
	if s == nil || kind == PendingSchedulerOperationUnknown || regionID == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.state.PendingSchedulerOperations, pendingSchedulerOperationKey(kind, regionID))
	return s.persistPendingSchedulerOperationCatalogLocked()
}

// MovePendingRootEventToBlocked removes one retryable rooted event from the
// pending catalog and records it as a blocked reconciliation item.
func (s *Store) MovePendingRootEventToBlocked(sequence uint64, blocked BlockedRootEvent) error {
	if s == nil {
		return nil
	}
	if sequence == 0 || blocked.Sequence == 0 {
		return fmt.Errorf("raftstore/localmeta: blocked rooted event sequence is zero")
	}
	if blocked.Sequence != sequence {
		return fmt.Errorf("raftstore/localmeta: blocked rooted event sequence mismatch (pending=%d blocked=%d)", sequence, blocked.Sequence)
	}
	if blocked.Event.Kind == 0 {
		return fmt.Errorf("raftstore/localmeta: blocked rooted event kind is unknown")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.state.PendingRootEvents, sequence)
	if s.state.BlockedRootEvents == nil {
		s.state.BlockedRootEvents = make(map[uint64]BlockedRootEvent)
	}
	if _, exists := s.state.BlockedRootEvents[blocked.Sequence]; !exists && len(s.state.BlockedRootEvents) >= maxBlockedRootEvents {
		return fmt.Errorf("raftstore/localmeta: blocked rooted event limit exceeded (max=%d)", maxBlockedRootEvents)
	}
	s.state.BlockedRootEvents[blocked.Sequence] = CloneBlockedRootEvent(blocked)
	return s.persistRootEventCatalogsLocked()
}

// Close releases resources associated with the metadata store.
func (s *Store) Close() error {
	return nil
}

func loadState(fs vfs.FS, workdir string) (diskState, error) {
	regions, err := loadReplicaCatalog(fs, workdir)
	if err != nil {
		return diskState{}, err
	}
	progress, err := loadRaftProgressCatalog(fs, workdir)
	if err != nil {
		return diskState{}, err
	}
	pending, err := loadPendingRootEventCatalog(fs, workdir)
	if err != nil {
		return diskState{}, err
	}
	schedulerOps, err := loadPendingSchedulerOperationCatalog(fs, workdir)
	if err != nil {
		return diskState{}, err
	}
	blocked, err := loadBlockedRootEventCatalog(fs, workdir)
	if err != nil {
		return diskState{}, err
	}
	return diskState{
		Regions:                    regions,
		RaftPointers:               progress,
		PendingRootEvents:          pending,
		PendingSchedulerOperations: schedulerOps,
		BlockedRootEvents:          blocked,
	}, nil
}

func (s *Store) persistReplicaCatalogLocked() error {
	return persistReplicaCatalog(s.fs, s.workdir, s.state.Regions)
}

func (s *Store) persistRaftProgressCatalogLocked() error {
	return persistRaftProgressCatalog(s.fs, s.workdir, s.state.RaftPointers)
}

func (s *Store) persistPendingRootEventCatalogLocked() error {
	return persistPendingRootEventCatalog(s.fs, s.workdir, s.state.PendingRootEvents)
}

func (s *Store) persistPendingSchedulerOperationCatalogLocked() error {
	return persistPendingSchedulerOperationCatalog(s.fs, s.workdir, s.state.PendingSchedulerOperations)
}

func (s *Store) persistRootEventCatalogsLocked() error {
	// Permanent publish rejection must not disappear behind a successful pending
	// deletion. Persist the blocked catalog first so an interrupted move leaves
	// repair evidence on disk even if the pending catalog still needs cleanup.
	if err := persistBlockedRootEventCatalog(s.fs, s.workdir, s.state.BlockedRootEvents); err != nil {
		return err
	}
	return persistPendingRootEventCatalog(s.fs, s.workdir, s.state.PendingRootEvents)
}

func raftPointerCheckpointRequired(prev, next RaftLogPointer, existed bool) bool {
	if !existed {
		return true
	}
	if prev == next {
		return false
	}
	if prev.Segment == 0 || next.Segment == 0 {
		return true
	}
	if prev.Segment != next.Segment {
		return true
	}
	if next.Offset < prev.Offset {
		return true
	}
	if prev.Committed == 0 && next.Committed != 0 {
		return true
	}
	if prev.SnapshotIndex != next.SnapshotIndex || prev.SnapshotTerm != next.SnapshotTerm {
		return true
	}
	if prev.TruncatedIndex != next.TruncatedIndex || prev.TruncatedTerm != next.TruncatedTerm {
		return true
	}
	if prev.SegmentIndex != next.SegmentIndex || prev.TruncatedOffset != next.TruncatedOffset {
		return true
	}
	return false
}

func loadReplicaCatalog(fs vfs.FS, workdir string) (map[uint64]RegionMeta, error) {
	path := filepath.Join(workdir, ReplicaStateFileName)
	data, err := fs.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return make(map[uint64]RegionMeta), nil
		}
		return nil, err
	}
	if len(data) == 0 {
		return make(map[uint64]RegionMeta), nil
	}
	var pbCatalog metapb.ReplicaLocalCatalog
	if err := proto.Unmarshal(data, &pbCatalog); err != nil {
		return nil, err
	}
	out := make(map[uint64]RegionMeta, len(pbCatalog.Regions))
	for _, item := range pbCatalog.Regions {
		if item == nil || item.RegionId == 0 {
			continue
		}
		out[item.RegionId] = regionMetaFromPB(item)
	}
	return out, nil
}

func loadRaftProgressCatalog(fs vfs.FS, workdir string) (map[uint64]RaftLogPointer, error) {
	path := filepath.Join(workdir, RaftProgressFileName)
	data, err := fs.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return make(map[uint64]RaftLogPointer), nil
		}
		return nil, err
	}
	if len(data) == 0 {
		return make(map[uint64]RaftLogPointer), nil
	}
	var pbCatalog metapb.RaftProgressCatalog
	if err := proto.Unmarshal(data, &pbCatalog); err != nil {
		return nil, err
	}
	out := make(map[uint64]RaftLogPointer, len(pbCatalog.Entries))
	for _, item := range pbCatalog.Entries {
		if item == nil || item.GroupId == 0 {
			continue
		}
		out[item.GroupId] = raftProgressFromPB(item)
	}
	return out, nil
}

func loadPendingRootEventCatalog(fs vfs.FS, workdir string) (map[uint64]PendingRootEvent, error) {
	path := filepath.Join(workdir, PendingRootEventsFileName)
	data, err := fs.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return make(map[uint64]PendingRootEvent), nil
		}
		return nil, err
	}
	if len(data) == 0 {
		return make(map[uint64]PendingRootEvent), nil
	}
	var pbCatalog metapb.PendingRootEventCatalog
	if err := proto.Unmarshal(data, &pbCatalog); err != nil {
		return nil, err
	}
	out := make(map[uint64]PendingRootEvent, len(pbCatalog.Entries))
	for _, item := range pbCatalog.Entries {
		if item == nil || item.GetSequence() == 0 || item.GetEvent() == nil {
			continue
		}
		out[item.GetSequence()] = PendingRootEvent{
			Sequence: item.GetSequence(),
			Event:    metawire.RootEventFromProto(item.GetEvent()),
		}
	}
	if len(out) > maxPendingRootEvents {
		return nil, fmt.Errorf("raftstore/localmeta: pending rooted event catalog exceeds limit (entries=%d max=%d)", len(out), maxPendingRootEvents)
	}
	return out, nil
}

func loadPendingSchedulerOperationCatalog(fs vfs.FS, workdir string) (map[string]PendingSchedulerOperation, error) {
	path := filepath.Join(workdir, PendingSchedulerOperationsFileName)
	data, err := fs.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return make(map[string]PendingSchedulerOperation), nil
		}
		return nil, err
	}
	if len(data) == 0 {
		return make(map[string]PendingSchedulerOperation), nil
	}
	var pbCatalog metapb.PendingSchedulerOperationCatalog
	if err := proto.Unmarshal(data, &pbCatalog); err != nil {
		return nil, err
	}
	out := make(map[string]PendingSchedulerOperation, len(pbCatalog.Entries))
	for _, item := range pbCatalog.Entries {
		if item == nil || item.GetRegionId() == 0 {
			continue
		}
		op := PendingSchedulerOperation{
			Kind:           pendingSchedulerOperationKindFromPB(item.GetOperationType()),
			RegionID:       item.GetRegionId(),
			SourcePeerID:   item.GetSourcePeerId(),
			TargetPeerID:   item.GetTargetPeerId(),
			Attempts:       item.GetAttempts(),
			SplitKey:       append([]byte(nil), item.GetSplitKey()...),
			SourceRegionID: item.GetSourceRegionId(),
		}
		if item.GetSplitChild() != nil {
			child, err := FromDescriptorProto(item.GetSplitChild())
			if err != nil {
				return nil, err
			}
			op.SplitChild = child
		}
		if op.Kind == PendingSchedulerOperationUnknown {
			continue
		}
		out[PendingSchedulerOperationKey(op)] = op
	}
	if len(out) > maxPendingSchedulerOperations {
		return nil, fmt.Errorf("raftstore/localmeta: pending scheduler operation catalog exceeds limit (entries=%d max=%d)", len(out), maxPendingSchedulerOperations)
	}
	return out, nil
}

func loadBlockedRootEventCatalog(fs vfs.FS, workdir string) (map[uint64]BlockedRootEvent, error) {
	path := filepath.Join(workdir, BlockedRootEventsFileName)
	data, err := fs.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return make(map[uint64]BlockedRootEvent), nil
		}
		return nil, err
	}
	if len(data) == 0 {
		return make(map[uint64]BlockedRootEvent), nil
	}
	var pbCatalog metapb.BlockedRootEventCatalog
	if err := proto.Unmarshal(data, &pbCatalog); err != nil {
		return nil, err
	}
	out := make(map[uint64]BlockedRootEvent, len(pbCatalog.Entries))
	for _, item := range pbCatalog.Entries {
		if item == nil || item.GetSequence() == 0 || item.GetEvent() == nil {
			continue
		}
		out[item.GetSequence()] = BlockedRootEvent{
			Sequence:     item.GetSequence(),
			Event:        metawire.RootEventFromProto(item.GetEvent()),
			TransitionID: item.GetTransitionId(),
			LastError:    item.GetLastError(),
		}
	}
	if len(out) > maxBlockedRootEvents {
		return nil, fmt.Errorf("raftstore/localmeta: blocked rooted event catalog exceeds limit (entries=%d max=%d)", len(out), maxBlockedRootEvents)
	}
	return out, nil
}

func persistReplicaCatalog(fs vfs.FS, workdir string, regions map[uint64]RegionMeta) error {
	keys := sortedRegionIDs(regions)
	pbCatalog := &metapb.ReplicaLocalCatalog{
		Regions: make([]*metapb.LocalRegionMeta, 0, len(keys)),
	}
	for _, id := range keys {
		pbCatalog.Regions = append(pbCatalog.Regions, regionMetaToPB(regions[id]))
	}
	payload, err := proto.Marshal(pbCatalog)
	if err != nil {
		return err
	}
	return persistProtoFile(fs, workdir, ReplicaStateFileName, payload)
}

func persistRaftProgressCatalog(fs vfs.FS, workdir string, progress map[uint64]RaftLogPointer) error {
	keys := sortedRaftGroupIDs(progress)
	pbCatalog := &metapb.RaftProgressCatalog{
		Entries: make([]*metapb.RaftProgress, 0, len(keys)),
	}
	for _, id := range keys {
		pbCatalog.Entries = append(pbCatalog.Entries, raftProgressToPB(progress[id]))
	}
	payload, err := proto.Marshal(pbCatalog)
	if err != nil {
		return err
	}
	return persistProtoFile(fs, workdir, RaftProgressFileName, payload)
}

func persistPendingRootEventCatalog(fs vfs.FS, workdir string, pending map[uint64]PendingRootEvent) error {
	keys := sortedPendingSequences(pending)
	pbCatalog := &metapb.PendingRootEventCatalog{
		Entries: make([]*metapb.PendingRootEvent, 0, len(keys)),
	}
	for _, seq := range keys {
		item := pending[seq]
		pbCatalog.Entries = append(pbCatalog.Entries, &metapb.PendingRootEvent{
			Sequence: item.Sequence,
			Event:    metawire.RootEventToProto(item.Event),
		})
	}
	payload, err := proto.Marshal(pbCatalog)
	if err != nil {
		return err
	}
	return persistProtoFile(fs, workdir, PendingRootEventsFileName, payload)
}

func persistPendingSchedulerOperationCatalog(fs vfs.FS, workdir string, ops map[string]PendingSchedulerOperation) error {
	keys := sortedPendingSchedulerOperationKeys(ops)
	pbCatalog := &metapb.PendingSchedulerOperationCatalog{
		Entries: make([]*metapb.PendingSchedulerOperation, 0, len(keys)),
	}
	for _, key := range keys {
		op := ops[key]
		pbCatalog.Entries = append(pbCatalog.Entries, &metapb.PendingSchedulerOperation{
			OperationType:  pendingSchedulerOperationKindToPB(op.Kind),
			RegionId:       op.RegionID,
			SourcePeerId:   op.SourcePeerID,
			TargetPeerId:   op.TargetPeerID,
			Attempts:       op.Attempts,
			SplitKey:       append([]byte(nil), op.SplitKey...),
			SplitChild:     pendingSplitChildToPB(op.SplitChild),
			SourceRegionId: op.SourceRegionID,
		})
	}
	payload, err := proto.Marshal(pbCatalog)
	if err != nil {
		return err
	}
	return persistProtoFile(fs, workdir, PendingSchedulerOperationsFileName, payload)
}

func pendingSplitChildToPB(meta RegionMeta) *metapb.RegionDescriptor {
	if meta.ID == 0 {
		return nil
	}
	return DescriptorToProto(meta)
}

func persistBlockedRootEventCatalog(fs vfs.FS, workdir string, blocked map[uint64]BlockedRootEvent) error {
	keys := sortedBlockedSequences(blocked)
	pbCatalog := &metapb.BlockedRootEventCatalog{
		Entries: make([]*metapb.BlockedRootEvent, 0, len(keys)),
	}
	for _, seq := range keys {
		item := blocked[seq]
		pbCatalog.Entries = append(pbCatalog.Entries, &metapb.BlockedRootEvent{
			Sequence:     item.Sequence,
			Event:        metawire.RootEventToProto(item.Event),
			TransitionId: item.TransitionID,
			LastError:    item.LastError,
		})
	}
	payload, err := proto.Marshal(pbCatalog)
	if err != nil {
		return err
	}
	return persistProtoFile(fs, workdir, BlockedRootEventsFileName, payload)
}

func persistProtoFile(fs vfs.FS, workdir, name string, payload []byte) error {
	path := filepath.Join(workdir, name)
	tmp := path + ".tmp"
	f, err := fs.OpenFileHandle(tmp, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(payload); err != nil {
		_ = f.Close()
		_ = fs.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = fs.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = fs.Remove(tmp)
		return err
	}
	if err := fs.Rename(tmp, path); err != nil {
		return err
	}
	return vfs.SyncDir(fs, workdir)
}

func regionMetaToPB(meta RegionMeta) *metapb.LocalRegionMeta {
	out := &metapb.LocalRegionMeta{
		RegionId: meta.ID,
		StartKey: append([]byte(nil), meta.StartKey...),
		EndKey:   append([]byte(nil), meta.EndKey...),
		Epoch: &metapb.RegionEpoch{
			Version:     meta.Epoch.Version,
			ConfVersion: meta.Epoch.ConfVersion,
		},
		State: regionStateToPB(meta.State),
	}
	if len(meta.Peers) > 0 {
		out.Peers = make([]*metapb.RegionPeer, 0, len(meta.Peers))
		for _, peer := range meta.Peers {
			out.Peers = append(out.Peers, &metapb.RegionPeer{
				StoreId: peer.StoreID,
				PeerId:  peer.PeerID,
			})
		}
	}
	return out
}

func regionMetaFromPB(meta *metapb.LocalRegionMeta) RegionMeta {
	if meta == nil {
		return RegionMeta{}
	}
	out := RegionMeta{
		ID:       meta.RegionId,
		StartKey: append([]byte(nil), meta.StartKey...),
		EndKey:   append([]byte(nil), meta.EndKey...),
		State:    regionStateFromPB(meta.State),
	}
	if meta.Epoch != nil {
		out.Epoch = metaregion.Epoch{
			Version:     meta.Epoch.Version,
			ConfVersion: meta.Epoch.ConfVersion,
		}
	}
	if len(meta.Peers) > 0 {
		out.Peers = make([]metaregion.Peer, 0, len(meta.Peers))
		for _, peer := range meta.Peers {
			if peer == nil {
				continue
			}
			out.Peers = append(out.Peers, metaregion.Peer{
				StoreID: peer.StoreId,
				PeerID:  peer.PeerId,
			})
		}
	}
	return out
}

func raftProgressToPB(ptr RaftLogPointer) *metapb.RaftProgress {
	return &metapb.RaftProgress{
		GroupId:         ptr.GroupID,
		Segment:         ptr.Segment,
		Offset:          ptr.Offset,
		AppliedIndex:    ptr.AppliedIndex,
		AppliedTerm:     ptr.AppliedTerm,
		Committed:       ptr.Committed,
		SnapshotIndex:   ptr.SnapshotIndex,
		SnapshotTerm:    ptr.SnapshotTerm,
		TruncatedIndex:  ptr.TruncatedIndex,
		TruncatedTerm:   ptr.TruncatedTerm,
		SegmentIndex:    ptr.SegmentIndex,
		TruncatedOffset: ptr.TruncatedOffset,
	}
}

func raftProgressFromPB(ptr *metapb.RaftProgress) RaftLogPointer {
	if ptr == nil {
		return RaftLogPointer{}
	}
	return RaftLogPointer{
		GroupID:         ptr.GroupId,
		Segment:         ptr.Segment,
		Offset:          ptr.Offset,
		AppliedIndex:    ptr.AppliedIndex,
		AppliedTerm:     ptr.AppliedTerm,
		Committed:       ptr.Committed,
		SnapshotIndex:   ptr.SnapshotIndex,
		SnapshotTerm:    ptr.SnapshotTerm,
		TruncatedIndex:  ptr.TruncatedIndex,
		TruncatedTerm:   ptr.TruncatedTerm,
		SegmentIndex:    ptr.SegmentIndex,
		TruncatedOffset: ptr.TruncatedOffset,
	}
}

func regionStateToPB(state metaregion.ReplicaState) metapb.RegionReplicaState {
	switch state {
	case RegionStateNew:
		return metapb.RegionReplicaState_REGION_REPLICA_STATE_NEW
	case RegionStateRunning:
		return metapb.RegionReplicaState_REGION_REPLICA_STATE_RUNNING
	case RegionStateRemoving:
		return metapb.RegionReplicaState_REGION_REPLICA_STATE_REMOVING
	case RegionStateTombstone:
		return metapb.RegionReplicaState_REGION_REPLICA_STATE_TOMBSTONE
	default:
		return metapb.RegionReplicaState_REGION_REPLICA_STATE_UNSPECIFIED
	}
}

func regionStateFromPB(state metapb.RegionReplicaState) metaregion.ReplicaState {
	switch state {
	case metapb.RegionReplicaState_REGION_REPLICA_STATE_NEW:
		return RegionStateNew
	case metapb.RegionReplicaState_REGION_REPLICA_STATE_RUNNING:
		return RegionStateRunning
	case metapb.RegionReplicaState_REGION_REPLICA_STATE_REMOVING:
		return RegionStateRemoving
	case metapb.RegionReplicaState_REGION_REPLICA_STATE_TOMBSTONE:
		return RegionStateTombstone
	default:
		return RegionStateNew
	}
}

func pendingSchedulerOperationKindToPB(kind PendingSchedulerOperationKind) uint32 {
	return uint32(kind)
}

func pendingSchedulerOperationKindFromPB(kind uint32) PendingSchedulerOperationKind {
	switch PendingSchedulerOperationKind(kind) {
	case PendingSchedulerOperationLeaderTransfer, PendingSchedulerOperationSplitRegion, PendingSchedulerOperationMergeRegion:
		return PendingSchedulerOperationKind(kind)
	default:
		return PendingSchedulerOperationUnknown
	}
}

func sortedRegionIDs(regions map[uint64]RegionMeta) []uint64 {
	keys := make([]uint64, 0, len(regions))
	for id := range regions {
		keys = append(keys, id)
	}
	slices.Sort(keys)
	return keys
}

func sortedRaftGroupIDs(progress map[uint64]RaftLogPointer) []uint64 {
	keys := make([]uint64, 0, len(progress))
	for id := range progress {
		keys = append(keys, id)
	}
	slices.Sort(keys)
	return keys
}

func sortedPendingSequences(pending map[uint64]PendingRootEvent) []uint64 {
	keys := make([]uint64, 0, len(pending))
	for seq := range pending {
		keys = append(keys, seq)
	}
	slices.Sort(keys)
	return keys
}

func sortedPendingSchedulerOperationKeys(ops map[string]PendingSchedulerOperation) []string {
	keys := make([]string, 0, len(ops))
	for key := range ops {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

func sortedBlockedSequences(blocked map[uint64]BlockedRootEvent) []uint64 {
	keys := make([]uint64, 0, len(blocked))
	for seq := range blocked {
		keys = append(keys, seq)
	}
	slices.Sort(keys)
	return keys
}
