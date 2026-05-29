// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package slab

import (
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/feichai0017/NoKV/storage/file"
	"github.com/feichai0017/NoKV/storage/vfs"
	"github.com/feichai0017/NoKV/utils"
	pkgerrors "github.com/pkg/errors"
)

// DefaultMaxSize is the default per-segment size cap when the consumer
// leaves Config.MaxSize unset.
const DefaultMaxSize int64 = 1 << 29

// Config controls how a slab Manager opens a directory of segment files
// and rotates between them. Each slab consumer supplies its own Config so the
// substrate can stay generic across authoritative and derived sidecar data.
type Config struct {
	// Dir is the directory holding segment files.
	Dir string
	// FileSuffix is the filename suffix for segment files (e.g. ".dirpage").
	// Required: enforces per-consumer isolation when multiple slab managers
	// share a parent directory.
	FileSuffix string
	// FileMode is the os.FileMode applied when creating new segment files.
	// Zero falls back to utils.DefaultFileMode.
	FileMode os.FileMode
	// MaxSize is the per-segment capacity in bytes (mmap mapping size).
	// Zero falls back to DefaultMaxSize.
	MaxSize int64
	// HeaderSize is the number of bytes reserved at offset 0 of each fresh
	// segment (zero means no header).
	HeaderSize int
	// FS provides the filesystem abstraction (vfs.OSFS in production,
	// fault-injection FS in tests).
	FS vfs.FS
}

func (cfg Config) resolve() (Config, error) {
	if cfg.Dir == "" {
		return Config{}, fmt.Errorf("slab manager: dir required")
	}
	if cfg.FileSuffix == "" {
		return Config{}, fmt.Errorf("slab manager: file suffix required")
	}
	cfg.FS = vfs.Ensure(cfg.FS)
	if cfg.FileMode == 0 {
		cfg.FileMode = utils.DefaultFileMode
	}
	if cfg.MaxSize == 0 {
		cfg.MaxSize = DefaultMaxSize
	}
	return cfg, nil
}

// Manager owns a directory of mmap-backed Segment files and the rotation
// lifecycle between them. It does not understand what the bytes mean —
// no value pointer, no bucket routing, no GC sample logic, no product index
// integration. Those are consumer concerns layered on top.
//
// Concurrency model:
//   - filesLock guards files / index / maxFid / active / activeID;
//     callers that need to compose multiple lifecycle ops use WithLock.
//   - Per-segment writes serialize on Segment.Lock (sync.RWMutex). Reads
//     hold the same lock as readers; the manager's AcquireRead helper
//     handles the sealed/active distinction automatically.
//   - index is updated copy-on-write so readers can take a lock-free
//     snapshot via loadIndex.
type Manager struct {
	cfg       Config
	filesLock sync.RWMutex
	files     map[uint32]*managedSegment
	index     atomic.Value // *segmentIndex
	maxFid    uint32
	active    *managedSegment
	activeID  uint32
}

type segmentIndex struct {
	files map[uint32]*managedSegment
}

// Open opens (or creates if missing) the directory and inventories every
// existing segment file. The active segment is the one with the highest
// fid; sealed segments are loaded read-only. If no segments exist a fresh
// active segment with fid 0 is created and bootstrapped with the configured
// header.
func Open(cfg Config) (*Manager, error) {
	cfg, err := cfg.resolve()
	if err != nil {
		return nil, err
	}
	if err := cfg.FS.MkdirAll(cfg.Dir, os.ModePerm); err != nil {
		return nil, err
	}
	m := &Manager{
		cfg:   cfg,
		files: make(map[uint32]*managedSegment),
	}
	if err := m.populate(); err != nil {
		return nil, err
	}
	if len(m.files) == 0 {
		if _, err := m.createLocked(0); err != nil {
			return nil, err
		}
		m.active = m.files[0]
		m.activeID = 0
	} else {
		m.activeID = m.maxFid
		m.active = m.files[m.activeID]
	}
	m.filesLock.Lock()
	m.refreshIndexLocked()
	m.filesLock.Unlock()
	return m, nil
}

// SetMaxSize updates the per-segment size cap. Existing segments keep
// their current mmap; only future Rotate operations honor the new cap.
func (m *Manager) SetMaxSize(maxSize int64) {
	if maxSize <= 0 {
		return
	}
	m.filesLock.Lock()
	m.cfg.MaxSize = maxSize
	m.filesLock.Unlock()
}

// MaxSize returns the configured per-segment cap.
func (m *Manager) MaxSize() int64 {
	m.filesLock.RLock()
	defer m.filesLock.RUnlock()
	return m.cfg.MaxSize
}

// HeaderSize returns the configured per-segment header reservation.
func (m *Manager) HeaderSize() int { return m.cfg.HeaderSize }

// MaxFID returns the highest segment fid currently tracked.
func (m *Manager) MaxFID() uint32 {
	m.filesLock.RLock()
	defer m.filesLock.RUnlock()
	return m.maxFid
}

// ActiveFID returns the fid of the segment currently accepting writes.
func (m *Manager) ActiveFID() uint32 {
	m.filesLock.RLock()
	defer m.filesLock.RUnlock()
	return m.activeID
}

// Active returns the active segment and its fid for writers. Returns
// (nil, 0, false) when the manager has no segments (post-Close).
func (m *Manager) Active() (*Segment, uint32, bool) {
	m.filesLock.RLock()
	defer m.filesLock.RUnlock()
	if m.active == nil {
		return nil, 0, false
	}
	return m.active.store, m.activeID, true
}

// ListFIDs returns a sorted snapshot of currently-tracked segment fids.
func (m *Manager) ListFIDs() []uint32 {
	idx := m.loadIndex()
	fids := make([]uint32, 0, len(idx.files))
	for fid := range idx.files {
		fids = append(fids, fid)
	}
	slices.Sort(fids)
	return fids
}

// WithLock runs fn while holding the manager's write lock. Used by
// consumers that need to compose lookup + Rotate + write reservation as a
// single atomic operation. fn must not call other
// Manager methods that re-acquire the lock.
func (m *Manager) WithLock(fn func() error) error {
	m.filesLock.Lock()
	defer m.filesLock.Unlock()
	return fn()
}

// EnsureActiveLocked promotes the active segment to maxFid and returns it.
// Caller must hold the lock acquired via WithLock.
func (m *Manager) EnsureActiveLocked() (*Segment, uint32, error) {
	if m.active != nil {
		return m.active.store, m.activeID, nil
	}
	if len(m.files) == 0 {
		if _, err := m.createLocked(0); err != nil {
			return nil, 0, err
		}
		m.active = m.files[0]
		m.activeID = 0
		m.refreshIndexLocked()
		return m.active.store, m.activeID, nil
	}
	m.active = m.files[m.maxFid]
	m.activeID = m.maxFid
	m.refreshIndexLocked()
	return m.active.store, m.activeID, nil
}

// RotateLocked seals the current active segment at currentOffset, creates
// the next fid, and bootstraps its header. Returns the new active segment
// and fid. Caller must hold WithLock.
func (m *Manager) RotateLocked(currentOffset uint32) (*Segment, uint32, error) {
	if m.active != nil {
		if err := m.active.store.DoneWriting(currentOffset); err != nil {
			return nil, 0, err
		}
		_ = m.active.store.SetReadOnly()
		m.active.seal()
	}
	nextID := m.maxFid + 1
	if _, err := m.createLocked(nextID); err != nil {
		return nil, 0, err
	}
	m.active = m.files[nextID]
	m.activeID = nextID
	m.refreshIndexLocked()
	return m.active.store, m.activeID, nil
}

// Rotate is the convenience wrapper that takes the lock for callers that
// don't need to compose with reserve. currentOffset is the writer's
// logical write cursor for the segment being sealed.
func (m *Manager) Rotate(currentOffset uint32) (*Segment, uint32, error) {
	m.filesLock.Lock()
	defer m.filesLock.Unlock()
	return m.RotateLocked(currentOffset)
}

// AcquireRead returns a segment for reading along with a release callback.
// Sealed segments are pinned with a reader counter so concurrent removal
// can wait for in-flight reads to drain; active segments take the
// segment's read lock for the duration.
func (m *Manager) AcquireRead(fid uint32) (*Segment, func(), error) {
	seg, ok := m.loadIndex().files[fid]
	if !ok {
		return nil, nil, pkgerrors.Errorf("slab manager: segment %d not found", fid)
	}
	if seg.isClosing() {
		return nil, nil, pkgerrors.Errorf("slab manager: segment %d closing", fid)
	}
	if seg.isSealed() {
		if !seg.pinRead() {
			return nil, nil, pkgerrors.Errorf("slab manager: segment %d closing", fid)
		}
		return seg.store, seg.unpinRead, nil
	}
	seg.store.Lock.RLock()
	return seg.store, seg.store.Lock.RUnlock, nil
}

// GetSegment returns the underlying Segment for direct access. Caller is
// responsible for holding the appropriate lock; prefer AcquireRead unless
// performing one-shot metadata calls (Size, FileName, Sync).
func (m *Manager) GetSegment(fid uint32) (*Segment, error) {
	seg, ok := m.loadIndex().files[fid]
	if !ok {
		return nil, pkgerrors.Errorf("slab manager: segment %d not found", fid)
	}
	return seg.store, nil
}

// Remove drops the segment with fid: deletes it from the index, closes
// the underlying file, and removes the on-disk file. Active segments are
// recovered to the next-highest fid so the manager always has an active
// (or, if there are no segments left, sets active to nil).
//
// activeOffsetReset is invoked while holding the manager lock right after
// the new active is chosen, so consumers can re-derive their logical write
// cursor from the new active segment's Size. Pass nil when the consumer
// doesn't track a write cursor.
func (m *Manager) Remove(fid uint32, activeOffsetReset func(active *Segment)) error {
	m.filesLock.Lock()
	seg, ok := m.files[fid]
	if !ok {
		m.filesLock.Unlock()
		return nil
	}
	delete(m.files, fid)

	var maxID uint32
	for id := range m.files {
		if id > maxID {
			maxID = id
		}
	}
	m.maxFid = maxID

	if fid == m.activeID {
		if len(m.files) == 0 {
			m.active = nil
			m.activeID = 0
			if activeOffsetReset != nil {
				activeOffsetReset(nil)
			}
		} else {
			m.activeID = maxID
			m.active = m.files[maxID]
			if activeOffsetReset != nil && m.active != nil {
				activeOffsetReset(m.active.store)
			}
		}
	}
	m.refreshIndexLocked()
	m.filesLock.Unlock()

	seg.beginClose()
	if seg.isSealed() {
		seg.waitForNoPins()
	}
	seg.store.Lock.Lock()
	defer seg.store.Lock.Unlock()
	if err := seg.store.Close(); err != nil {
		return err
	}
	return m.cfg.FS.Remove(seg.store.FileName())
}

// SyncFIDs fsyncs the listed segments. Duplicate fids are deduped.
func (m *Manager) SyncFIDs(fids []uint32) error {
	if m == nil || len(fids) == 0 {
		return nil
	}
	seen := make(map[uint32]struct{}, len(fids))
	for _, fid := range fids {
		if _, ok := seen[fid]; ok {
			continue
		}
		seen[fid] = struct{}{}
		m.filesLock.RLock()
		seg := m.files[fid]
		if seg != nil && seg.store != nil {
			seg.store.Lock.Lock()
		}
		m.filesLock.RUnlock()
		if seg == nil || seg.store == nil {
			continue
		}
		err := seg.store.Sync()
		seg.store.Lock.Unlock()
		if err != nil {
			return err
		}
	}
	return nil
}

// SyncActive fsyncs the active segment. Mostly used in tests; the write
// path generally syncs all touched segments via SyncFIDs.
func (m *Manager) SyncActive() error {
	if m == nil {
		return nil
	}
	m.filesLock.RLock()
	seg := m.active
	m.filesLock.RUnlock()
	if seg == nil || seg.store == nil {
		return nil
	}
	seg.store.Lock.Lock()
	defer seg.store.Lock.Unlock()
	return seg.store.Sync()
}

// SegmentSize is a convenience for one-shot metadata reads.
func (m *Manager) SegmentSize(fid uint32) (int64, error) {
	store, err := m.GetSegment(fid)
	if err != nil {
		return 0, err
	}
	return store.Size(), nil
}

// SegmentInit refreshes the high-water from the on-disk file size. Used
// after external truncation (rewind / sanitize) has resized the file.
func (m *Manager) SegmentInit(fid uint32) error {
	store, err := m.GetSegment(fid)
	if err != nil {
		return err
	}
	return store.Init()
}

// SegmentBootstrap rewrites the header on the segment, treating it as
// freshly empty. Used by recovery flows that truncate a segment to size 0
// and need to re-initialize the header bytes.
func (m *Manager) SegmentBootstrap(fid uint32) error {
	store, err := m.GetSegment(fid)
	if err != nil {
		return err
	}
	store.Lock.Lock()
	defer store.Lock.Unlock()
	return store.Bootstrap(m.cfg.HeaderSize)
}

// SegmentTruncate shrinks the segment to offset.
func (m *Manager) SegmentTruncate(fid uint32, offset uint32) error {
	store, err := m.GetSegment(fid)
	if err != nil {
		return err
	}
	store.Lock.Lock()
	defer store.Lock.Unlock()
	return store.Truncate(int64(offset))
}

// RewindSpec describes a rewind target: keep segment fid as the new active
// at offset, drop every higher-fid segment from the manager and disk.
type RewindSpec struct {
	Fid    uint32
	Offset uint32
}

// Rewind rolls the manager back to spec: drops segments with fid > spec.Fid,
// truncates spec.Fid's segment to spec.Offset, and re-activates it. Used by
// a consumer after a write failure to undo partially-published state.
//
// activeOffsetReset is invoked under the manager lock once the new active
// is chosen, so consumers can re-derive their write cursor from spec.Offset.
func (m *Manager) Rewind(spec RewindSpec, activeOffsetReset func(active *Segment, offset uint32)) error {
	var (
		extra []struct {
			seg  *managedSegment
			name string
		}
		active *managedSegment
	)
	m.filesLock.Lock()
	for fid, seg := range m.files {
		if fid > spec.Fid {
			extra = append(extra, struct {
				seg  *managedSegment
				name string
			}{seg: seg, name: seg.store.FileName()})
			delete(m.files, fid)
		}
	}
	seg, ok := m.files[spec.Fid]
	if ok {
		active = seg
		m.active = seg
		m.activeID = spec.Fid
		m.maxFid = spec.Fid
		if activeOffsetReset != nil {
			activeOffsetReset(seg.store, spec.Offset)
		}
	}
	m.refreshIndexLocked()
	m.filesLock.Unlock()
	if !ok {
		return pkgerrors.Errorf("slab manager: rewind segment %d not found", spec.Fid)
	}
	var firstErr error
	active.beginClose()
	active.waitForNoPins()
	if err := active.store.SetWritable(); err != nil {
		firstErr = err
	}
	active.activate()
	for _, item := range extra {
		item.seg.beginClose()
		if item.seg.isSealed() {
			item.seg.waitForNoPins()
		}
		item.seg.store.Lock.Lock()
		if err := item.seg.store.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		item.seg.store.Lock.Unlock()
		if err := m.cfg.FS.Remove(item.name); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	active.store.Lock.Lock()
	if err := active.store.Truncate(int64(spec.Offset)); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := active.store.Init(); err != nil && firstErr == nil {
		firstErr = err
	}
	active.store.Lock.Unlock()
	return firstErr
}

// Close closes every tracked segment and resets the manager. Returns the
// first error encountered while closing; subsequent close errors are
// swallowed since callers can no longer recover.
func (m *Manager) Close() error {
	m.filesLock.Lock()
	defer m.filesLock.Unlock()
	var firstErr error
	for fid, seg := range m.files {
		seg.beginClose()
		if seg.isSealed() {
			seg.waitForNoPins()
		}
		if err := seg.store.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(m.files, fid)
	}
	m.active = nil
	m.activeID = 0
	m.refreshIndexLocked()
	return firstErr
}

// loadIndex returns a copy-on-write snapshot of the segment index for
// lock-free reads.
func (m *Manager) loadIndex() *segmentIndex {
	if idx := m.index.Load(); idx != nil {
		return idx.(*segmentIndex)
	}
	return &segmentIndex{files: make(map[uint32]*managedSegment)}
}

func (m *Manager) refreshIndexLocked() {
	next := make(map[uint32]*managedSegment, len(m.files))
	maps.Copy(next, m.files)
	m.index.Store(&segmentIndex{files: next})
}

// populate inventories existing segment files in cfg.Dir.
func (m *Manager) populate() error {
	pattern := filepath.Join(m.cfg.Dir, "*"+m.cfg.FileSuffix)
	files, err := m.cfg.FS.Glob(pattern)
	if err != nil {
		return err
	}
	sort.Strings(files)
	var max uint32
	for _, path := range files {
		fid, ok := parseFID(filepath.Base(path), m.cfg.FileSuffix)
		if !ok {
			continue
		}
		if fid > max {
			max = fid
		}
	}
	m.maxFid = max
	for _, path := range files {
		fid, ok := parseFID(filepath.Base(path), m.cfg.FileSuffix)
		if !ok {
			continue
		}
		readonly := fid != max
		store, err := openSegmentFile(m.cfg.FS, fid, path, m.cfg.Dir, m.cfg.MaxSize, readonly)
		if err != nil {
			return err
		}
		// Reload sealed segments' high-water from the on-disk size; the
		// truncate at DoneWriting / VerifyDir made stat == logical extent.
		if err := store.LoadSizeFromFile(); err != nil {
			return err
		}
		m.files[fid] = newManagedSegment(store, readonly)
	}
	return nil
}

// createLocked creates a new segment file with fid and bootstraps the
// configured header. Caller holds filesLock.
func (m *Manager) createLocked(fid uint32) (*Segment, error) {
	path := filepath.Join(m.cfg.Dir, fmt.Sprintf("%05d%s", fid, m.cfg.FileSuffix))
	store, err := createSegmentFile(m.cfg.FS, fid, path, m.cfg.Dir, m.cfg.MaxSize, m.cfg.HeaderSize)
	if err != nil {
		return nil, err
	}
	m.files[fid] = newManagedSegment(store, false)
	if fid > m.maxFid {
		m.maxFid = fid
	}
	return store, nil
}

func openSegmentFile(fs vfs.FS, fid uint32, path, dir string, maxSize int64, readOnly bool) (*Segment, error) {
	flag := os.O_CREATE | os.O_RDWR
	if readOnly {
		flag = os.O_RDONLY
	}
	seg := &Segment{}
	if err := seg.Open(&file.Options{
		FID:      uint64(fid),
		FileName: path,
		Dir:      dir,
		Flag:     flag,
		MaxSz:    int(maxSize),
		FS:       fs,
	}); err != nil {
		return nil, err
	}
	return seg, nil
}

func createSegmentFile(fs vfs.FS, fid uint32, path, dir string, maxSize int64, headerSize int) (*Segment, error) {
	seg, err := openSegmentFile(fs, fid, path, dir, maxSize, false)
	if err != nil {
		return nil, err
	}
	if err := seg.Bootstrap(headerSize); err != nil {
		_ = seg.Close()
		return nil, err
	}
	return seg, nil
}

func parseFID(base, suffix string) (uint32, bool) {
	stem := base
	if len(stem) > len(suffix) && stem[len(stem)-len(suffix):] == suffix {
		stem = stem[:len(stem)-len(suffix)]
	}
	var fid uint32
	if _, err := fmt.Sscanf(stem, "%05d", &fid); err != nil {
		return 0, false
	}
	return fid, true
}
