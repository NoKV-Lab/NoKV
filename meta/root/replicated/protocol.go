// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package replicated

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"

	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/storage/vfs"
)

// protocolStateFileName stores replicated metadata protocol recovery state.
//
// The file is backend-private and not cluster truth. It only exists so one
// rooted replicated node can restart its raft protocol state without having to
// reconstruct hard state, raft snapshot, and retained raft entries from peers.
const protocolStateFileName = "root.raft.bin"

type persistedProtocolState struct {
	HardState myraft.HardState
	Snapshot  myraft.Snapshot
	Entries   []myraft.Entry
}

// loadProtocolState reads root.raft.bin.
//
// On-disk format:
//   - uint32 length + HardState marshaled bytes
//   - uint32 length + Snapshot marshaled bytes
//   - uint32 entry count
//   - repeated: uint32 length + Entry marshaled bytes
func loadProtocolState(workdir string) (persistedProtocolState, error) {
	path := filepath.Join(workdir, protocolStateFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return persistedProtocolState{}, nil
		}
		return persistedProtocolState{}, err
	}
	if len(data) == 0 {
		return persistedProtocolState{}, nil
	}
	var (
		state persistedProtocolState
		off   int
	)
	read := func() ([]byte, error) {
		if off+4 > len(data) {
			return nil, io.ErrUnexpectedEOF
		}
		n := int(binary.LittleEndian.Uint32(data[off : off+4]))
		off += 4
		if off+n > len(data) {
			return nil, io.ErrUnexpectedEOF
		}
		out := data[off : off+n]
		off += n
		return out, nil
	}
	if payload, err := read(); err != nil {
		return persistedProtocolState{}, err
	} else if len(payload) > 0 {
		if err := state.HardState.Unmarshal(payload); err != nil {
			return persistedProtocolState{}, err
		}
	}
	if payload, err := read(); err != nil {
		return persistedProtocolState{}, err
	} else if len(payload) > 0 {
		if err := state.Snapshot.Unmarshal(payload); err != nil {
			return persistedProtocolState{}, err
		}
	}
	if off+4 > len(data) {
		return persistedProtocolState{}, io.ErrUnexpectedEOF
	}
	count := int(binary.LittleEndian.Uint32(data[off : off+4]))
	off += 4
	state.Entries = make([]myraft.Entry, 0, count)
	for range count {
		payload, err := read()
		if err != nil {
			return persistedProtocolState{}, err
		}
		var entry myraft.Entry
		if len(payload) > 0 {
			if err := entry.Unmarshal(payload); err != nil {
				return persistedProtocolState{}, err
			}
		}
		state.Entries = append(state.Entries, entry)
	}
	return state, nil
}

// saveProtocolState publishes root.raft.bin atomically via temp-file replace.
func saveProtocolState(workdir string, state persistedProtocolState) error {
	if err := os.MkdirAll(workdir, 0o755); err != nil {
		return err
	}
	payload := make([]byte, 0, 4096)
	appendMessage := func(data []byte, err error) error {
		if err != nil {
			return err
		}
		var hdr [4]byte
		binary.LittleEndian.PutUint32(hdr[:], uint32(len(data)))
		payload = append(payload, hdr[:]...)
		payload = append(payload, data...)
		return nil
	}
	if err := appendMessage(state.HardState.Marshal()); err != nil {
		return err
	}
	if err := appendMessage(state.Snapshot.Marshal()); err != nil {
		return err
	}
	var count [4]byte
	binary.LittleEndian.PutUint32(count[:], uint32(len(state.Entries)))
	payload = append(payload, count[:]...)
	for i := range state.Entries {
		if err := appendMessage(state.Entries[i].Marshal()); err != nil {
			return err
		}
	}
	path := filepath.Join(workdir, protocolStateFileName)
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	renamed := false
	defer func() {
		_ = f.Close()
		if !renamed {
			_ = os.Remove(tmp)
		}
	}()
	if _, err := f.Write(payload); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	renamed = true
	return vfs.SyncDir(vfs.Ensure(nil), workdir)
}

// captureProtocolState snapshots the in-memory raft storage into the durable
// backend-private recovery image persisted as root.raft.bin.
func captureProtocolState(storage *myraft.MemoryStorage) (persistedProtocolState, error) {
	if storage == nil {
		return persistedProtocolState{}, nil
	}
	hs, cs, err := storage.InitialState()
	if err != nil {
		return persistedProtocolState{}, err
	}
	snap, err := storage.Snapshot()
	if err != nil {
		return persistedProtocolState{}, err
	}
	first, err := storage.FirstIndex()
	if err != nil {
		return persistedProtocolState{}, err
	}
	last, err := storage.LastIndex()
	if err != nil {
		return persistedProtocolState{}, err
	}
	if len(cs.Voters) > 0 {
		snapshotIndex := hs.Commit
		if snapshotIndex == 0 {
			snapshotIndex = last
		}
		if snapshotIndex > 0 && (myraft.IsEmptySnap(snap) || len(snap.Metadata.ConfState.Voters) == 0 || snap.Metadata.Index < snapshotIndex) {
			snap, err = storage.CreateSnapshot(snapshotIndex, &cs, nil)
			if err != nil {
				return persistedProtocolState{}, err
			}
		}
	}
	var entries []myraft.Entry
	start := first
	if !myraft.IsEmptySnap(snap) && snap.Metadata.Index+1 > start {
		start = snap.Metadata.Index + 1
	}
	if last+1 > start {
		entries, err = storage.Entries(start, last+1, math.MaxUint64)
		if err != nil {
			return persistedProtocolState{}, err
		}
	}
	return persistedProtocolState{
		HardState: hs,
		Snapshot:  snap,
		Entries:   entries,
	}, nil
}
