// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package sst

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/feichai0017/NoKV/engine/vfs"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
)

const (
	sstVersion = 1
	// sstSnapshotName stores one region snapshot manifest as JSON metadata.
	sstSnapshotName = "snapshot.json"
	// sstTablesDirName stores the referenced SST payload files for one imported
	// or exported region snapshot.
	sstTablesDirName = "tables"
)

// TableMeta describes one SST file inside a region snapshot.
type TableMeta struct {
	RelativePath string `json:"relative_path"`
	SmallestKey  []byte `json:"smallest_key"`
	LargestKey   []byte `json:"largest_key"`
	EntryCount   uint64 `json:"entry_count"`
	SizeBytes    uint64 `json:"size_bytes"`
	ValueBytes   uint64 `json:"value_bytes"`
}

// Compatibility records the minimum table-format settings that must match
// before an SST snapshot can be imported safely.
type Compatibility struct {
	BlockSize          int     `json:"block_size"`
	BloomFalsePositive float64 `json:"bloom_false_positive"`
}

// Meta describes one region-scoped SST snapshot.
//
// On disk, snapshot.json is a JSON document and tables/ contains the material
// SST payload files referenced by Meta.Tables.
type Meta struct {
	Version       uint32               `json:"version"`
	Region        localmeta.RegionMeta `json:"region"`
	EntryCount    uint64               `json:"entry_count"`
	TableCount    uint64               `json:"table_count"`
	InlineValues  bool                 `json:"inline_values"`
	Compatibility Compatibility        `json:"compatibility"`
	Tables        []TableMeta          `json:"tables"`
	CreatedAt     time.Time            `json:"created_at"`
}

// ExportResult reports the persisted snapshot metadata after a successful export.
type ExportResult struct {
	Meta Meta
}

// SnapshotStore is the high-level region-snapshot bridge exposed by the storage
// engine to raftstore wiring.
//
// ImportSnapshot returns the full staged-import result so callers that need a
// simple region metadata view can read result.Meta.Region, while install paths
// can still roll back imported SST files before peer publish completes.
type SnapshotStore interface {
	ExportSnapshot(region localmeta.RegionMeta) ([]byte, error)
	ImportSnapshot(payload []byte) (*ImportResult, error)
	ExportSnapshotTo(w io.Writer, region localmeta.RegionMeta) (Meta, error)
	ImportSnapshotFrom(r io.Reader) (*ImportResult, error)
}

// ImportResult reports one successful staged SST snapshot import.
// The caller may still need to publish higher-level peer metadata before the
// install becomes visible as one hosted raft peer.
type ImportResult struct {
	Meta            Meta
	ImportedTables  uint64
	ImportedBytes   uint64
	ImportedFileIDs []uint64
	rollback        func() error
}

// ReadMeta loads one SST snapshot metadata file from dir.
func ReadMeta(dir string, fs vfs.FS) (Meta, error) {
	fs = vfs.Ensure(fs)
	data, err := fs.ReadFile(filepath.Join(dir, sstSnapshotName))
	if err != nil {
		return Meta{}, fmt.Errorf("snapshot: read sst meta %s: %w", filepath.Join(dir, sstSnapshotName), err)
	}
	var meta Meta
	if err := json.Unmarshal(data, &meta); err != nil {
		return Meta{}, fmt.Errorf("snapshot: decode sst meta %s: %w", filepath.Join(dir, sstSnapshotName), err)
	}
	if meta.Version != sstVersion {
		return Meta{}, fmt.Errorf("snapshot: unsupported sst version %d", meta.Version)
	}
	return meta, nil
}

func writeMeta(path string, meta *Meta, fs vfs.FS) error {
	fs = vfs.Ensure(fs)
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("snapshot: encode sst meta %s: %w", path, err)
	}
	parent := filepath.Dir(path)
	tmpPath := fmt.Sprintf("%s.tmp.%d.%d", path, os.Getpid(), time.Now().UnixNano())
	f, err := fs.OpenFileHandle(tmpPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("snapshot: create sst meta temp %s: %w", tmpPath, err)
	}
	published := false
	defer func() {
		if !published {
			_ = fs.Remove(tmpPath)
		}
	}()
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return fmt.Errorf("snapshot: write sst meta %s: %w", tmpPath, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("snapshot: sync sst meta %s: %w", tmpPath, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("snapshot: close sst meta %s: %w", tmpPath, err)
	}
	if err := fs.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("snapshot: publish sst meta %s: %w", path, err)
	}
	if err := vfs.SyncDir(fs, parent); err != nil {
		return fmt.Errorf("snapshot: sync sst meta parent %s: %w", parent, err)
	}
	published = true
	return nil
}
