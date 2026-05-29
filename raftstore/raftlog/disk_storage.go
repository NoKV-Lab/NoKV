// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftlog

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"

	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/storage/vfs"
)

const (
	logFileName  = "raft.log"
	hardFileName = "raft.hard"
	snapFileName = "raft.snap"
)

// DiskStorage persists raft entries, hard state, and snapshot to disk while
// delegating the Storage interface to an in-memory MemoryStorage.
type DiskStorage struct {
	mu        sync.Mutex
	dir       string
	fs        vfs.FS
	mem       *myraft.MemoryStorage
	entries   []myraft.Entry
	hardState myraft.HardState
	snapshot  myraft.Snapshot
}

// OpenDiskStorage loads or initialises raft storage with the provided filesystem.
// Nil fs defaults to OSFS.
func OpenDiskStorage(dir string, fs vfs.FS) (*DiskStorage, error) {
	if dir == "" {
		return nil, errStorageDirRequired
	}
	fs = vfs.Ensure(fs)
	if err := fs.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	ds := &DiskStorage{
		dir: dir,
		fs:  fs,
		mem: myraft.NewMemoryStorage(),
	}
	if err := ds.load(); err != nil {
		return nil, err
	}
	return ds, nil
}

func (ds *DiskStorage) load() error {
	if err := ds.loadSnapshot(); err != nil {
		return err
	}
	if err := ds.loadEntries(); err != nil {
		return err
	}
	if err := ds.loadHardState(); err != nil {
		return err
	}
	return nil
}

func (ds *DiskStorage) loadSnapshot() error {
	path := filepath.Join(ds.dir, snapFileName)
	data, err := ds.fs.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	var snap myraft.Snapshot
	if err := snap.Unmarshal(data); err != nil {
		return errDecodeSnapshot(err)
	}
	if myraft.IsEmptySnap(snap) {
		return nil
	}
	if err := ds.mem.ApplySnapshot(snap); err != nil {
		return err
	}
	ds.snapshot = snap
	return nil
}

func (ds *DiskStorage) loadEntries() error {
	path := filepath.Join(ds.dir, logFileName)
	f, err := ds.fs.OpenHandle(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer func() { _ = f.Close() }()

	var entries []myraft.Entry
	for {
		var length uint32
		if err := binary.Read(f, binary.LittleEndian, &length); err != nil {
			if err == io.EOF {
				break
			}
			return errReadLogLength(err)
		}
		buf := make([]byte, length)
		if _, err := io.ReadFull(f, buf); err != nil {
			return errReadLogPayload(err)
		}
		var entry myraft.Entry
		if err := entry.Unmarshal(buf); err != nil {
			return errDecodeEntry(err)
		}
		entries = append(entries, entry)
	}
	if len(entries) > 0 {
		if err := ds.mem.Append(entries); err != nil {
			return err
		}
	}
	ds.entries = entries
	return nil
}

func (ds *DiskStorage) loadHardState() error {
	path := filepath.Join(ds.dir, hardFileName)
	data, err := ds.fs.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	var st myraft.HardState
	if err := st.Unmarshal(data); err != nil {
		return errDecodeHardState(err)
	}
	if err := ds.mem.SetHardState(st); err != nil {
		return err
	}
	ds.hardState = st
	return nil
}

// saveEntriesLocked appends entries and persists; caller must hold ds.mu.
func (ds *DiskStorage) saveEntriesLocked(entries []myraft.Entry) error {
	if err := ds.mem.Append(entries); err != nil {
		return err
	}
	ds.entries = append(ds.entries, entries...)
	return ds.persistEntriesLocked()
}

func (ds *DiskStorage) SaveReadyState(rd myraft.Ready) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	if !myraft.IsEmptyHardState(rd.HardState) {
		if err := ds.mem.SetHardState(rd.HardState); err != nil {
			return err
		}
		ds.hardState = rd.HardState
		if err := ds.persistHardStateLocked(); err != nil {
			return err
		}
	}
	if !myraft.IsEmptySnap(rd.Snapshot) {
		if err := ds.mem.ApplySnapshot(rd.Snapshot); err != nil {
			return err
		}
		ds.snapshot = rd.Snapshot
		if err := ds.persistSnapshotLocked(); err != nil {
			return err
		}
		if err := ds.refreshEntriesLocked(); err != nil {
			return err
		}
	}
	if len(rd.Entries) > 0 {
		if err := ds.saveEntriesLocked(rd.Entries); err != nil {
			return err
		}
	}
	return nil
}

func (ds *DiskStorage) Append(entries []myraft.Entry) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	return ds.saveEntriesLocked(entries)
}

func (ds *DiskStorage) ApplySnapshot(snap myraft.Snapshot) error {
	if myraft.IsEmptySnap(snap) {
		return nil
	}
	ds.mu.Lock()
	defer ds.mu.Unlock()
	if err := ds.mem.ApplySnapshot(snap); err != nil {
		return err
	}
	ds.snapshot = snap
	if err := ds.persistSnapshotLocked(); err != nil {
		return err
	}
	if err := ds.refreshEntriesLocked(); err != nil {
		return err
	}
	return nil
}

func (ds *DiskStorage) SetHardState(st myraft.HardState) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	if err := ds.mem.SetHardState(st); err != nil {
		return err
	}
	ds.hardState = st
	return ds.persistHardStateLocked()
}

// Snapshot delegates to the underlying storage.
//
// forwarding-ok: DiskStorage satisfies raft.Storage; the in-memory mirror owns the read view.
func (ds *DiskStorage) Snapshot() (myraft.Snapshot, error) {
	return ds.mem.Snapshot()
}

// Entries returns a slice of log entries between [lo,hi).
//
// forwarding-ok: DiskStorage satisfies raft.Storage; the in-memory mirror owns the read view.
func (ds *DiskStorage) Entries(lo, hi, maxSize uint64) ([]myraft.Entry, error) {
	return ds.mem.Entries(lo, hi, maxSize)
}

// Term returns the term of entry i, which must be in the log.
//
// forwarding-ok: DiskStorage satisfies raft.Storage; the in-memory mirror owns the read view.
func (ds *DiskStorage) Term(i uint64) (uint64, error) {
	return ds.mem.Term(i)
}

// LastIndex returns the last index of the log entries.
//
// forwarding-ok: DiskStorage satisfies raft.Storage; the in-memory mirror owns the read view.
func (ds *DiskStorage) LastIndex() (uint64, error) {
	return ds.mem.LastIndex()
}

// FirstIndex returns the index of the first log entry.
//
// forwarding-ok: DiskStorage satisfies raft.Storage; the in-memory mirror owns the read view.
func (ds *DiskStorage) FirstIndex() (uint64, error) {
	return ds.mem.FirstIndex()
}

// InitialState returns the HardState and ConfState information.
//
// forwarding-ok: DiskStorage satisfies raft.Storage; the in-memory mirror owns the read view.
func (ds *DiskStorage) InitialState() (myraft.HardState, myraft.ConfState, error) {
	return ds.mem.InitialState()
}

// refreshEntriesLocked reloads entries after snapshot; caller must hold ds.mu.
func (ds *DiskStorage) refreshEntriesLocked() error {
	first, err := ds.mem.FirstIndex()
	if err != nil {
		ds.entries = nil
		return nil
	}
	last, err := ds.mem.LastIndex()
	if err != nil {
		return err
	}
	if last < first {
		ds.entries = nil
		return nil
	}
	ents, err := ds.mem.Entries(first, last+1, math.MaxUint64)
	if err != nil {
		return err
	}
	ds.entries = ents
	return nil
}

// persistEntriesLocked flushes entry log state; caller must hold ds.mu.
func (ds *DiskStorage) persistEntriesLocked() error {
	path := filepath.Join(ds.dir, logFileName)
	if len(ds.entries) == 0 {
		return ds.removeFile(path)
	}
	tmp := path + ".tmp"
	f, err := ds.fs.OpenFileHandle(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	for _, entry := range ds.entries {
		data, err := entry.Marshal()
		if err != nil {
			_ = f.Close()
			return err
		}
		if err := binary.Write(f, binary.LittleEndian, uint32(len(data))); err != nil {
			_ = f.Close()
			return err
		}
		if _, err := f.Write(data); err != nil {
			_ = f.Close()
			return err
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	return ds.fs.Rename(tmp, path)
}

// persistHardStateLocked writes hard state to disk; caller must hold ds.mu.
func (ds *DiskStorage) persistHardStateLocked() error {
	path := filepath.Join(ds.dir, hardFileName)
	if myraft.IsEmptyHardState(ds.hardState) {
		return ds.removeFile(path)
	}
	data, err := ds.hardState.Marshal()
	if err != nil {
		return err
	}
	return ds.atomicWriteFile(path, data)
}

// persistSnapshotLocked writes snapshot metadata to disk; caller must hold ds.mu.
func (ds *DiskStorage) persistSnapshotLocked() error {
	path := filepath.Join(ds.dir, snapFileName)
	if myraft.IsEmptySnap(ds.snapshot) {
		return ds.removeFile(path)
	}
	data, err := ds.snapshot.Marshal()
	if err != nil {
		return err
	}
	return ds.atomicWriteFile(path, data)
}

func (ds *DiskStorage) removeFile(path string) error {
	if err := ds.fs.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (ds *DiskStorage) atomicWriteFile(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := ds.fs.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return ds.fs.Rename(tmp, path)
}
