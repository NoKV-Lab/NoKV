// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftlog

import (
	"errors"
	"fmt"
	"os"

	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/storage/vfs"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

// ExportSnapshot persists the current raft snapshot from storage to the supplied file path.
// If the snapshot is empty the file is removed (best-effort) and the function returns without error.
func ExportSnapshot(storage PeerStorage, path string, fs vfs.FS) error {
	if storage == nil {
		return fmt.Errorf("engine: snapshot export requires storage")
	}
	fs = vfs.Ensure(fs)
	snap, err := storage.Snapshot()
	if err != nil {
		return err
	}
	if myraft.IsEmptySnap(snap) {
		if err := fs.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("engine: remove empty snapshot %s: %w", path, err)
		}
		return nil
	}
	pbSnap := raftpb.Snapshot(snap)
	data, err := (&pbSnap).Marshal()
	if err != nil {
		return fmt.Errorf("engine: marshal snapshot: %w", err)
	}
	if err := fs.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("engine: write snapshot %s: %w", path, err)
	}
	return nil
}

// ImportSnapshot loads a raft snapshot from the provided file and applies it to storage.
// Missing files are treated as no-op (no snapshot to import).
func ImportSnapshot(storage PeerStorage, path string, fs vfs.FS) error {
	if storage == nil {
		return fmt.Errorf("engine: snapshot import requires storage")
	}
	fs = vfs.Ensure(fs)
	data, err := fs.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("engine: read snapshot %s: %w", path, err)
	}
	if len(data) == 0 {
		return fmt.Errorf("engine: snapshot file %s is empty", path)
	}
	var snap raftpb.Snapshot
	if err := snap.Unmarshal(data); err != nil {
		return fmt.Errorf("engine: unmarshal snapshot %s: %w", path, err)
	}
	return storage.ApplySnapshot(myraft.Snapshot(snap))
}
