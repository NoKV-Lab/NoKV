// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"bytes"
	"maps"
	"sort"
	"strings"
	"sync"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/model"
)

// OverlayKV is one visible Peras overlay row.
type OverlayKV struct {
	Key    []byte
	Value  []byte
	Delete bool
}

type overlayEntry struct {
	opID       OperationID
	key        []byte
	value      []byte
	delete     bool
	generation uint64
	retired    uint64
}

// OverlaySnapshot is a generation-pinned directory view over an OverlayView.
// It owns only the directory prefix and referenced inode-key set; values stay
// in the overlay's multi-version entry history until the snapshot is retired.
type OverlaySnapshot struct {
	view       *OverlayView
	generation uint64
	prefix     string
	inodeKeys  map[string]struct{}
}

// OverlayView is the authority-local visible read plane before a Peras segment
// is sealed and installed. It owns only semantic overlay state; durable
// segment catalog discovery stays in the runtime layer.
type OverlayView struct {
	mu             sync.RWMutex
	entries        map[string]overlayEntry
	history        map[string][]overlayEntry
	sortedKeys     []string
	sortedDirty    bool
	directoryKeys  map[string]map[string]struct{}
	directoryRuns  map[string][]string
	directoryDirty map[string]bool
	directoryEpoch map[string]uint64
	epoch          uint64
	known          map[string]bool
	emptyDirs      map[string]struct{}
	baseEmptyDirs  map[string]struct{}
	emptySessions  map[string]struct{}
}

func NewOverlayView() *OverlayView {
	return &OverlayView{
		entries:        make(map[string]overlayEntry),
		history:        make(map[string][]overlayEntry),
		directoryKeys:  make(map[string]map[string]struct{}),
		directoryRuns:  make(map[string][]string),
		directoryDirty: make(map[string]bool),
		directoryEpoch: make(map[string]uint64),
		known:          make(map[string]bool),
		emptyDirs:      make(map[string]struct{}),
		baseEmptyDirs:  make(map[string]struct{}),
		emptySessions:  make(map[string]struct{}),
	}
}

func (v *OverlayView) Add(id OperationID, op compile.MaterializedOp) error {
	if v == nil {
		return ErrInvalidPerasSegment
	}
	if err := op.ValidateForAdmission(); err != nil {
		return ErrInvalidPerasSegment
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	v.initLocked()
	generation := v.nextGenerationLocked()
	for _, effect := range op.Effects {
		if len(effect.Key) == 0 {
			return ErrInvalidPerasSegment
		}
		entry := overlayEntry{
			opID:       id,
			key:        cloneBytes(effect.Key),
			generation: generation,
		}
		switch effect.Kind {
		case compile.EffectPut:
			if effect.Value == nil {
				return ErrInvalidPerasSegment
			}
			entry.value = cloneBytes(effect.Value)
		case compile.EffectDelete:
			entry.delete = true
		default:
			return ErrInvalidPerasSegment
		}
		v.putEntryLocked(entry)
		v.sortedDirty = true
		v.indexDirectoryKeyLocked(effect.Key, generation)
	}
	return RememberOperationFacts(v.known, v.emptyDirs, v.baseEmptyDirs, v.emptySessions, op)
}

func (v *OverlayView) AddReplayOperation(op ReplayOperation) error {
	if v == nil {
		return ErrInvalidPerasSegment
	}
	if err := validateVisibleReplayOperation(op); err != nil {
		return err
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	v.initLocked()
	generation := v.nextGenerationLocked()
	for _, mutation := range op.Mutations {
		if len(mutation.Key) == 0 || (!mutation.Delete && mutation.Value == nil) {
			return ErrInvalidPerasSegment
		}
		entry := overlayEntry{
			opID:       op.OpID,
			key:        cloneBytes(mutation.Key),
			value:      cloneBytes(mutation.Value),
			delete:     mutation.Delete,
			generation: generation,
		}
		v.putEntryLocked(entry)
		v.known[string(mutation.Key)] = !mutation.Delete
		if !mutation.Delete {
			ForgetEmptySessionNamespaceForKey(v.emptySessions, mutation.Key)
		}
		v.sortedDirty = true
		v.indexDirectoryKeyLocked(mutation.Key, generation)
	}
	return nil
}

func (v *OverlayView) AddSegment(segment PerasSegment) error {
	if v == nil {
		return ErrInvalidPerasSegment
	}
	if len(segment.EntriesView()) == 0 {
		return ErrInvalidPerasSegment
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	v.initLocked()
	generation := v.nextGenerationLocked()
	for _, kv := range segment.EntriesView() {
		if len(kv.Key) == 0 || (!kv.Delete && kv.Value == nil) {
			return ErrInvalidPerasSegment
		}
		v.putEntryLocked(overlayEntry{
			key:        cloneBytes(kv.Key),
			value:      cloneBytes(kv.Value),
			delete:     kv.Delete,
			generation: generation,
		})
		v.sortedDirty = true
		v.indexDirectoryKeyLocked(kv.Key, generation)
	}
	return nil
}

// SnapshotDirectory returns a generation-pinned direct directory view. It does
// not clone row values; it records only the dentry prefix and inode keys needed
// for ReadDirPlus over that directory.
func (v *OverlayView) SnapshotDirectory(mount model.MountIdentity, prefix []byte) *OverlaySnapshot {
	if v == nil || len(prefix) == 0 {
		return nil
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	v.initLocked()
	generation := v.epoch
	prefixKey := string(prefix)
	snapshot := &OverlaySnapshot{
		view:       v,
		generation: generation,
		prefix:     prefixKey,
		inodeKeys:  make(map[string]struct{}),
	}
	rows := v.scanDirectoryAtLocked(generation, prefixKey, string(prefix), 0)
	for _, row := range rows {
		if row.Delete {
			continue
		}
		dentry, err := fsmeta.DecodeDentryValue(row.Value)
		if err != nil {
			continue
		}
		inodeKey, err := fsmeta.EncodeInodeKey(mount, dentry.Inode)
		if err != nil {
			continue
		}
		snapshot.inodeKeys[string(inodeKey)] = struct{}{}
	}
	return snapshot
}

// Clone returns a point-in-time copy of the overlay read plane.
func (v *OverlayView) Clone() *OverlayView {
	out := NewOverlayView()
	if v == nil {
		return out
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	out.entries = make(map[string]overlayEntry, len(v.entries))
	for key, entry := range v.entries {
		out.entries[key] = overlayEntry{
			opID:       entry.opID,
			key:        cloneBytes(entry.key),
			value:      cloneBytes(entry.value),
			delete:     entry.delete,
			generation: entry.generation,
			retired:    entry.retired,
		}
	}
	out.history = cloneOverlayHistory(v.history)
	out.sortedKeys = append(out.sortedKeys, v.sortedKeys...)
	out.sortedDirty = v.sortedDirty
	out.directoryKeys = cloneOverlayStringSetMap(v.directoryKeys)
	out.directoryRuns = cloneOverlayStringSliceMap(v.directoryRuns)
	out.directoryDirty = make(map[string]bool, len(v.directoryDirty))
	maps.Copy(out.directoryDirty, v.directoryDirty)
	out.directoryEpoch = make(map[string]uint64, len(v.directoryEpoch))
	maps.Copy(out.directoryEpoch, v.directoryEpoch)
	out.epoch = v.epoch
	out.known = make(map[string]bool, len(v.known))
	maps.Copy(out.known, v.known)
	out.emptyDirs = cloneOverlayStringSet(v.emptyDirs)
	out.baseEmptyDirs = cloneOverlayStringSet(v.baseEmptyDirs)
	out.emptySessions = cloneOverlayStringSet(v.emptySessions)
	return out
}

// CloneForSnapshotDirectory copies the pending rows needed to serve a direct
// directory snapshot: matching dentries plus pending inode rows referenced by
// those dentries.
func (v *OverlayView) CloneForSnapshotDirectory(mount model.MountIdentity, prefix []byte) *OverlayView {
	out := NewOverlayView()
	if v == nil || len(prefix) == 0 {
		return out
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	out.initLocked()
	keys := v.directoryKeys[string(prefix)]
	for key := range keys {
		entry, ok := v.entries[key]
		if !ok {
			continue
		}
		out.addClonedEntryLocked(entry)
		if entry.delete {
			continue
		}
		dentry, err := fsmeta.DecodeDentryValue(entry.value)
		if err != nil {
			continue
		}
		inodeKey, err := fsmeta.EncodeInodeKey(mount, dentry.Inode)
		if err != nil {
			continue
		}
		if inodeEntry, ok := v.entries[string(inodeKey)]; ok {
			out.addClonedEntryLocked(inodeEntry)
		}
	}
	return out
}

func (s *OverlaySnapshot) GetView(key []byte) (value []byte, deleted bool, ok bool) {
	if s == nil || s.view == nil || len(key) == 0 {
		return nil, false, false
	}
	keyString := string(key)
	if !strings.HasPrefix(keyString, s.prefix) {
		if _, ok := s.inodeKeys[keyString]; !ok {
			return nil, false, false
		}
	}
	return s.view.GetViewAt(s.generation, key)
}

func (s *OverlaySnapshot) ScanDirectory(prefix, start []byte, limit uint32) []OverlayKV {
	if s == nil || s.view == nil || limit == 0 || string(prefix) != s.prefix {
		return nil
	}
	return s.view.ScanDirectoryAt(s.generation, prefix, start, limit)
}

func (s *OverlaySnapshot) HasDirectory(prefix []byte) bool {
	if s == nil || s.view == nil || string(prefix) != s.prefix {
		return false
	}
	return s.view.HasDirectoryAt(s.generation, prefix)
}

func (s *OverlaySnapshot) Generation() uint64 {
	if s == nil {
		return 0
	}
	return s.generation
}

func (v *OverlayView) Generation() uint64 {
	if v == nil {
		return 0
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.epoch
}

func (v *OverlayView) Get(key []byte) (value []byte, deleted bool, ok bool) {
	value, deleted, ok = v.GetView(key)
	if !ok {
		return nil, false, false
	}
	return cloneBytes(value), deleted, true
}

// GetView returns overlay-owned value bytes. Callers must not mutate the
// returned slice.
func (v *OverlayView) GetView(key []byte) (value []byte, deleted bool, ok bool) {
	if v == nil {
		return nil, false, false
	}
	v.mu.RLock()
	entry, ok := v.entries[string(key)]
	v.mu.RUnlock()
	if !ok {
		return nil, false, false
	}
	return entry.value, entry.delete, true
}

// GetViewAt returns the visible value for key at a captured overlay generation.
func (v *OverlayView) GetViewAt(generation uint64, key []byte) (value []byte, deleted bool, ok bool) {
	if v == nil || generation == 0 {
		return nil, false, false
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.getViewAtLocked(generation, string(key))
}

func (v *OverlayView) KeyState(key []byte) (present bool, known bool) {
	if v == nil {
		return false, false
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	if entry, ok := v.entries[string(key)]; ok {
		return !entry.delete, true
	}
	present, ok := v.known[string(key)]
	if ok {
		return present, true
	}
	if SessionNamespaceEmptyForKey(v.emptySessions, key) {
		return false, true
	}
	return present, ok
}

func (v *OverlayView) DirectoryEmpty(mount model.MountIdentity, inode model.InodeID) bool {
	if v == nil {
		return false
	}
	v.mu.RLock()
	_, ok := v.emptyDirs[DirectoryFactKey(mount, inode)]
	v.mu.RUnlock()
	return ok
}

func (v *OverlayView) DirectoryBaseEmpty(mount model.MountIdentity, inode model.InodeID) bool {
	if v == nil {
		return false
	}
	v.mu.RLock()
	_, ok := v.baseEmptyDirs[DirectoryBaseFactKey(mount, inode)]
	v.mu.RUnlock()
	return ok
}

func (v *OverlayView) SessionNamespaceEmpty(mount model.MountIdentity, inode model.InodeID) bool {
	if v == nil {
		return false
	}
	v.mu.RLock()
	_, ok := v.emptySessions[SessionNamespaceFactKey(mount, inode)]
	v.mu.RUnlock()
	return ok
}

func (v *OverlayView) RememberKey(key []byte, present bool) {
	if v == nil || len(key) == 0 {
		return
	}
	v.mu.Lock()
	v.initLocked()
	v.known[string(key)] = present
	if present {
		ForgetEmptySessionNamespaceForKey(v.emptySessions, key)
	}
	v.mu.Unlock()
}

func (v *OverlayView) RememberEmptyDirectory(mount model.MountIdentity, inode model.InodeID) {
	if v == nil {
		return
	}
	v.mu.Lock()
	v.initLocked()
	RememberEmptyDirectoryFact(v.emptyDirs, mount, inode)
	RememberBaseEmptyDirectoryFact(v.baseEmptyDirs, mount, inode)
	v.mu.Unlock()
}

func (v *OverlayView) ForgetEmptyDirectory(mount model.MountIdentity, inode model.InodeID) {
	if v == nil {
		return
	}
	v.mu.Lock()
	ForgetEmptyDirectoryFact(v.emptyDirs, mount, inode)
	v.mu.Unlock()
}

func (v *OverlayView) RememberEmptySessionNamespace(mount model.MountIdentity, inode model.InodeID) {
	if v == nil {
		return
	}
	v.mu.Lock()
	v.initLocked()
	RememberEmptySessionNamespaceFact(v.emptySessions, mount, inode)
	v.mu.Unlock()
}

func (v *OverlayView) Scan(start []byte, limit uint32) []OverlayKV {
	if v == nil || limit == 0 {
		return nil
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	v.rebuildSortedKeysLocked()
	startKey := string(start)
	idx := sort.SearchStrings(v.sortedKeys, startKey)
	end := min(idx+int(limit), len(v.sortedKeys))
	out := make([]OverlayKV, 0, end-idx)
	for _, key := range v.sortedKeys[idx:end] {
		entry := v.entries[key]
		out = append(out, OverlayKV{
			Key:    cloneBytes(entry.key),
			Value:  cloneBytes(entry.value),
			Delete: entry.delete,
		})
	}
	return out
}

func (v *OverlayView) ScanDirectory(prefix, start []byte, limit uint32) []OverlayKV {
	if v == nil || limit == 0 || len(prefix) == 0 {
		return nil
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	v.rebuildDirectoryRunLocked(string(prefix))
	keys := v.directoryRuns[string(prefix)]
	if len(keys) == 0 {
		return nil
	}
	startKey := string(start)
	idx := sort.SearchStrings(keys, startKey)
	end := min(idx+int(limit), len(keys))
	out := make([]OverlayKV, 0, end-idx)
	for _, key := range keys[idx:end] {
		entry := v.entries[key]
		out = append(out, OverlayKV{
			Key:    cloneBytes(entry.key),
			Value:  cloneBytes(entry.value),
			Delete: entry.delete,
		})
	}
	return out
}

func (v *OverlayView) ScanDirectoryAt(generation uint64, prefix, start []byte, limit uint32) []OverlayKV {
	if v == nil || generation == 0 || limit == 0 || len(prefix) == 0 {
		return nil
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.scanDirectoryAtLocked(generation, string(prefix), string(start), limit)
}

func (v *OverlayView) HasDirectory(prefix []byte) bool {
	if v == nil || len(prefix) == 0 {
		return false
	}
	v.mu.RLock()
	keys := v.directoryKeys[string(prefix)]
	ok := len(keys) > 0
	v.mu.RUnlock()
	return ok
}

func (v *OverlayView) HasDirectoryAt(generation uint64, prefix []byte) bool {
	if v == nil || generation == 0 || len(prefix) == 0 {
		return false
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	rows := v.scanDirectoryAtLocked(generation, string(prefix), string(prefix), 1)
	return len(rows) > 0
}

func (v *OverlayView) DirectoryFrontier(prefix []byte) uint64 {
	if v == nil || len(prefix) == 0 {
		return 0
	}
	v.mu.RLock()
	frontier := v.directoryEpoch[string(prefix)]
	if len(v.directoryKeys[string(prefix)]) == 0 {
		frontier = 0
	}
	v.mu.RUnlock()
	return frontier
}

func MergeOverlayScans(base, overlay []OverlayKV, limit uint32) []OverlayKV {
	if limit == 0 {
		return nil
	}
	out := make([]OverlayKV, 0, limit)
	i, j := 0, 0
	for len(out) < int(limit) && (i < len(base) || j < len(overlay)) {
		switch {
		case i >= len(base):
			out = append(out, cloneOverlayKV(overlay[j]))
			j++
		case j >= len(overlay):
			out = append(out, cloneOverlayKV(base[i]))
			i++
		default:
			cmp := bytes.Compare(base[i].Key, overlay[j].Key)
			switch {
			case cmp < 0:
				out = append(out, cloneOverlayKV(base[i]))
				i++
			case cmp > 0:
				out = append(out, cloneOverlayKV(overlay[j]))
				j++
			default:
				out = append(out, cloneOverlayKV(overlay[j]))
				i++
				j++
			}
		}
	}
	return out
}

func (v *OverlayView) RemovePlan(plan ReplayPlan) {
	if v == nil {
		return
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	generation := v.nextGenerationLocked()
	for _, op := range plan.Operations {
		for _, mutation := range op.Mutations {
			entry, ok := v.entries[string(mutation.Key)]
			if ok && entry.opID == op.OpID {
				v.retireEntryLocked(entry, generation)
				delete(v.entries, string(mutation.Key))
				v.sortedDirty = true
				v.removeDirectoryKeyLocked(mutation.Key, generation)
			}
		}
	}
}

func (v *OverlayView) Stats() (overlayKeys, knownKeys, emptyDirs, baseEmptyDirs, emptySessions int) {
	if v == nil {
		return 0, 0, 0, 0, 0
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	return len(v.entries), len(v.known), len(v.emptyDirs), len(v.baseEmptyDirs), len(v.emptySessions)
}

func (v *OverlayView) PruneHistoryBefore(minGeneration uint64) {
	if v == nil || minGeneration == 0 {
		return
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	for key, entries := range v.history {
		kept := entries[:0]
		for _, entry := range entries {
			if entry.retired == 0 || entry.retired > minGeneration {
				kept = append(kept, entry)
			}
		}
		if len(kept) == 0 {
			delete(v.history, key)
			continue
		}
		v.history[key] = kept
	}
}

func (v *OverlayView) HistoryLen() int {
	if v == nil {
		return 0
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	total := 0
	for _, entries := range v.history {
		total += len(entries)
	}
	return total
}

func (v *OverlayView) ReadIndexStats() (directories, dirty int) {
	if v == nil {
		return 0, 0
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	for _, keys := range v.directoryKeys {
		if len(keys) > 0 {
			directories++
		}
	}
	for _, isDirty := range v.directoryDirty {
		if isDirty {
			dirty++
		}
	}
	return directories, dirty
}

func (v *OverlayView) initLocked() {
	if v.entries == nil {
		v.entries = make(map[string]overlayEntry)
	}
	if v.history == nil {
		v.history = make(map[string][]overlayEntry)
	}
	if v.directoryKeys == nil {
		v.directoryKeys = make(map[string]map[string]struct{})
	}
	if v.directoryRuns == nil {
		v.directoryRuns = make(map[string][]string)
	}
	if v.directoryDirty == nil {
		v.directoryDirty = make(map[string]bool)
	}
	if v.directoryEpoch == nil {
		v.directoryEpoch = make(map[string]uint64)
	}
	if v.known == nil {
		v.known = make(map[string]bool)
	}
	if v.emptyDirs == nil {
		v.emptyDirs = make(map[string]struct{})
	}
	if v.baseEmptyDirs == nil {
		v.baseEmptyDirs = make(map[string]struct{})
	}
	if v.emptySessions == nil {
		v.emptySessions = make(map[string]struct{})
	}
	if v.sortedKeys == nil {
		v.sortedDirty = true
	}
}

func (v *OverlayView) addClonedEntryLocked(entry overlayEntry) {
	v.initLocked()
	cloned := overlayEntry{
		opID:       entry.opID,
		key:        cloneBytes(entry.key),
		value:      cloneBytes(entry.value),
		delete:     entry.delete,
		generation: entry.generation,
		retired:    entry.retired,
	}
	v.entries[string(cloned.key)] = cloned
	v.sortedDirty = true
	v.indexDirectoryKeyLocked(cloned.key, cloned.generation)
}

func (v *OverlayView) putEntryLocked(entry overlayEntry) {
	key := string(entry.key)
	if current, ok := v.entries[key]; ok {
		v.retireEntryLocked(current, entry.generation)
	}
	v.entries[key] = entry
}

func (v *OverlayView) retireEntryLocked(entry overlayEntry, generation uint64) {
	if entry.generation == 0 || generation == 0 {
		return
	}
	entry.retired = generation
	key := string(entry.key)
	v.history[key] = append(v.history[key], entry)
}

func (v *OverlayView) getViewAtLocked(generation uint64, key string) ([]byte, bool, bool) {
	if entry, ok := v.entries[key]; ok && overlayEntryVisibleAt(entry, generation) {
		return entry.value, entry.delete, true
	}
	history := v.history[key]
	for idx := len(history) - 1; idx >= 0; idx-- {
		entry := history[idx]
		if overlayEntryVisibleAt(entry, generation) {
			return entry.value, entry.delete, true
		}
	}
	return nil, false, false
}

func (v *OverlayView) scanDirectoryAtLocked(generation uint64, prefix, start string, limit uint32) []OverlayKV {
	keys := make(map[string]struct{})
	addKey := func(key string) {
		if key < start || !strings.HasPrefix(key, prefix) {
			return
		}
		keys[key] = struct{}{}
	}
	if current := v.directoryKeys[prefix]; len(current) > 0 {
		for key := range current {
			addKey(key)
		}
	}
	for key := range v.history {
		addKey(key)
	}
	sorted := make([]string, 0, len(keys))
	for key := range keys {
		sorted = append(sorted, key)
	}
	sort.Strings(sorted)
	rows := make([]OverlayKV, 0)
	for _, key := range sorted {
		if limit != 0 && len(rows) >= int(limit) {
			break
		}
		value, deleted, ok := v.getViewAtLocked(generation, key)
		if !ok {
			continue
		}
		rows = append(rows, OverlayKV{
			Key:    []byte(key),
			Value:  cloneBytes(value),
			Delete: deleted,
		})
	}
	return rows
}

func (v *OverlayView) nextGenerationLocked() uint64 {
	v.epoch++
	if v.epoch == 0 {
		v.epoch = 1
	}
	return v.epoch
}

func (v *OverlayView) rebuildSortedKeysLocked() {
	if !v.sortedDirty && len(v.sortedKeys) == len(v.entries) {
		return
	}
	v.sortedKeys = v.sortedKeys[:0]
	for key := range v.entries {
		v.sortedKeys = append(v.sortedKeys, key)
	}
	sort.Strings(v.sortedKeys)
	v.sortedDirty = false
}

func (v *OverlayView) rebuildDirectoryRunLocked(prefix string) {
	v.initLocked()
	keys := v.directoryKeys[prefix]
	if len(keys) == 0 {
		delete(v.directoryRuns, prefix)
		delete(v.directoryDirty, prefix)
		return
	}
	if !v.directoryDirty[prefix] && len(v.directoryRuns[prefix]) == len(keys) {
		return
	}
	run := v.directoryRuns[prefix][:0]
	for key := range keys {
		if _, ok := v.entries[key]; ok {
			run = append(run, key)
		}
	}
	sort.Strings(run)
	v.directoryRuns[prefix] = run
	v.directoryDirty[prefix] = false
}

func (v *OverlayView) indexDirectoryKeyLocked(key []byte, generation uint64) {
	prefix, ok := dentryDirectoryPrefix(key)
	if !ok {
		return
	}
	v.initLocked()
	keys := v.directoryKeys[prefix]
	if keys == nil {
		keys = make(map[string]struct{})
		v.directoryKeys[prefix] = keys
	}
	keys[string(key)] = struct{}{}
	v.directoryDirty[prefix] = true
	v.directoryEpoch[prefix] = generation
}

func (v *OverlayView) removeDirectoryKeyLocked(key []byte, generation uint64) {
	prefix, ok := dentryDirectoryPrefix(key)
	if !ok {
		return
	}
	keyString := string(key)
	if keys := v.directoryKeys[prefix]; keys != nil {
		delete(keys, keyString)
		if len(keys) == 0 {
			delete(v.directoryKeys, prefix)
			delete(v.directoryRuns, prefix)
			delete(v.directoryDirty, prefix)
			delete(v.directoryEpoch, prefix)
			return
		}
	}
	v.directoryDirty[prefix] = true
	v.directoryEpoch[prefix] = generation
}

func dentryDirectoryPrefix(key []byte) (string, bool) {
	name, ok := fsmeta.DentryNameBytesOfKey(key)
	if !ok || len(name) == 0 || len(name) > len(key) {
		return "", false
	}
	return string(key[:len(key)-len(name)]), true
}

func cloneOverlayKV(in OverlayKV) OverlayKV {
	return OverlayKV{
		Key:    cloneBytes(in.Key),
		Value:  cloneBytes(in.Value),
		Delete: in.Delete,
	}
}

func cloneOverlayStringSet(in map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{}, len(in))
	for key := range in {
		out[key] = struct{}{}
	}
	return out
}

func cloneOverlayStringSetMap(in map[string]map[string]struct{}) map[string]map[string]struct{} {
	out := make(map[string]map[string]struct{}, len(in))
	for key, set := range in {
		out[key] = cloneOverlayStringSet(set)
	}
	return out
}

func cloneOverlayHistory(in map[string][]overlayEntry) map[string][]overlayEntry {
	out := make(map[string][]overlayEntry, len(in))
	for key, entries := range in {
		copied := make([]overlayEntry, len(entries))
		for i, entry := range entries {
			copied[i] = overlayEntry{
				opID:       entry.opID,
				key:        cloneBytes(entry.key),
				value:      cloneBytes(entry.value),
				delete:     entry.delete,
				generation: entry.generation,
				retired:    entry.retired,
			}
		}
		out[key] = copied
	}
	return out
}

func overlayEntryVisibleAt(entry overlayEntry, generation uint64) bool {
	return entry.generation != 0 &&
		entry.generation <= generation &&
		(entry.retired == 0 || generation < entry.retired)
}

func cloneOverlayStringSliceMap(in map[string][]string) map[string][]string {
	out := make(map[string][]string, len(in))
	for key, values := range in {
		out[key] = append([]string(nil), values...)
	}
	return out
}
