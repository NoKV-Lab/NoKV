// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package sst

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/feichai0017/NoKV/engine/index"
	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/engine/lsm"
	"github.com/feichai0017/NoKV/engine/vfs"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
)

// exportSource is the minimal data-source view needed to build one region
// snapshot on disk. It lets the snapshot code:
// 1. scan the region's internal entries,
// 2. materialize separated values back into inline values, and
// 3. reuse the live engine's external-SST-compatible table options.
type exportSource interface {
	NewInternalIterator(opt *index.Options) index.Iterator
	MaterializeInternalEntry(src *kv.Entry) (*kv.Entry, error)
	ExternalSSTOptions() *lsm.Options
}

// installSink is the minimal install target needed to ingest one prepared
// snapshot directory. Export is intentionally absent here because import and
// rollback are the only destination-side actions.
type installSink interface {
	ExternalSSTOptions() *lsm.Options
	ImportExternalSST(paths []string) (*lsm.ExternalSSTImportResult, error)
	RollbackExternalSST(fileIDs []uint64) error
}

// ExportDir persists one region snapshot as one or more self-contained SST
// files. Values are materialized inline so the snapshot is self-contained.
func ExportDir(src exportSource, dir string, region localmeta.RegionMeta, fs vfs.FS) (*ExportResult, error) {
	if src == nil {
		return nil, fmt.Errorf("snapshot: export sst requires source")
	}
	if dir == "" {
		return nil, fmt.Errorf("snapshot: export sst requires dir")
	}
	opt := src.ExternalSSTOptions()
	if opt == nil {
		return nil, fmt.Errorf("snapshot: export sst requires lsm options")
	}
	fs = vfs.Ensure(fs)
	if _, err := fs.Stat(dir); err == nil {
		return nil, fmt.Errorf("snapshot: export sst target %s already exists", dir)
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("snapshot: stat export sst target %s: %w", dir, err)
	}

	parent := filepath.Dir(dir)
	if err := fs.MkdirAll(parent, 0o755); err != nil {
		return nil, fmt.Errorf("snapshot: create export sst parent %s: %w", parent, err)
	}
	tmpDir := fmt.Sprintf("%s.tmp.%d.%d", dir, os.Getpid(), time.Now().UnixNano())
	if err := fs.MkdirAll(tmpDir, 0o755); err != nil {
		return nil, fmt.Errorf("snapshot: create export sst temp dir %s: %w", tmpDir, err)
	}
	success := false
	defer func() {
		if !success {
			_ = fs.RemoveAll(tmpDir)
		}
	}()

	entries, err := collectMaterializedEntries(src, region)
	if err != nil {
		return nil, err
	}
	meta := Meta{
		Version:       sstVersion,
		Region:        localmeta.CloneRegionMeta(region),
		InlineValues:  true,
		Compatibility: snapshotCompatibility(opt),
		CreatedAt:     time.Now().UTC(),
	}

	if len(entries) > 0 {
		tablesDir := filepath.Join(tmpDir, sstTablesDirName)
		if err := fs.MkdirAll(tablesDir, 0o755); err != nil {
			return nil, fmt.Errorf("snapshot: create sst tables dir %s: %w", tablesDir, err)
		}
		chunks := splitSnapshotEntries(entries, opt.SSTableMaxSz)
		meta.Tables = make([]TableMeta, 0, len(chunks))
		for i, chunk := range chunks {
			tablePath := filepath.Join(tablesDir, fmt.Sprintf("%06d.sst", i+1))
			tableMeta, err := lsm.ExportExternalSST(tablePath, chunk, opt)
			if err != nil {
				return nil, fmt.Errorf("snapshot: build export sst table %d: %w", i+1, err)
			}
			meta.EntryCount += tableMeta.EntryCount
			meta.Tables = append(meta.Tables, TableMeta{
				RelativePath: filepath.Join(sstTablesDirName, filepath.Base(tableMeta.Path)),
				SmallestKey:  kv.SafeCopy(nil, tableMeta.SmallestKey),
				LargestKey:   kv.SafeCopy(nil, tableMeta.LargestKey),
				EntryCount:   tableMeta.EntryCount,
				SizeBytes:    tableMeta.SizeBytes,
				ValueBytes:   tableMeta.ValueBytes,
			})
		}
		meta.TableCount = uint64(len(meta.Tables))
	}

	if err := writeMeta(filepath.Join(tmpDir, sstSnapshotName), &meta, fs); err != nil {
		return nil, err
	}
	if err := vfs.SyncDir(fs, tmpDir); err != nil {
		return nil, fmt.Errorf("snapshot: sync export sst temp dir %s: %w", tmpDir, err)
	}
	if err := fs.Rename(tmpDir, dir); err != nil {
		return nil, fmt.Errorf("snapshot: publish export sst %s: %w", dir, err)
	}
	if err := vfs.SyncDir(fs, parent); err != nil {
		return nil, fmt.Errorf("snapshot: sync export sst parent %s: %w", parent, err)
	}
	success = true
	return &ExportResult{Meta: meta}, nil
}

// ImportDir installs one snapshot directory through the engine's external table
// ingest path.
func ImportDir(dst installSink, dir string, fs vfs.FS) (*ImportResult, error) {
	if dst == nil {
		return nil, fmt.Errorf("snapshot: import sst requires sink")
	}
	fs = vfs.Ensure(fs)
	meta, err := ReadMeta(dir, fs)
	if err != nil {
		return nil, err
	}
	if err := validateSnapshotCompatibility(meta, dst.ExternalSSTOptions()); err != nil {
		return nil, err
	}
	paths := make([]string, 0, len(meta.Tables))
	var importedBytes uint64
	for _, table := range meta.Tables {
		if table.RelativePath == "" {
			return nil, fmt.Errorf("snapshot: sst manifest contains empty table path")
		}
		path := filepath.Join(dir, table.RelativePath)
		stat, err := fs.Stat(path)
		if err != nil {
			return nil, fmt.Errorf("snapshot: stat sst table %s: %w", path, err)
		}
		paths = append(paths, path)
		importedBytes += uint64(stat.Size())
	}
	if len(paths) == 0 {
		return &ImportResult{Meta: meta}, nil
	}
	result := &ImportResult{
		Meta:           meta,
		ImportedTables: uint64(len(paths)),
		ImportedBytes:  importedBytes,
	}
	imported, err := dst.ImportExternalSST(paths)
	if err != nil {
		return nil, fmt.Errorf("snapshot: import sst snapshot dir %s: %w", dir, err)
	}
	if imported != nil {
		result.ImportedFileIDs = append(result.ImportedFileIDs, imported.FileIDs...)
		if imported.ImportedBytes != 0 {
			result.ImportedBytes = imported.ImportedBytes
		}
	}
	if len(result.ImportedFileIDs) > 0 {
		result.rollback = func() error {
			return dst.RollbackExternalSST(result.ImportedFileIDs)
		}
	}
	return result, nil
}

// Rollback removes previously imported SST tables through the rollback
// capability captured when the snapshot was imported.
func (r *ImportResult) Rollback() error {
	if r == nil || len(r.ImportedFileIDs) == 0 {
		return nil
	}
	if r.rollback == nil {
		return fmt.Errorf("snapshot: rollback is unavailable for imported files")
	}
	return r.rollback()
}

func collectMaterializedEntries(src exportSource, region localmeta.RegionMeta) ([]*kv.Entry, error) {
	bounds := localmeta.CloneRegionMeta(region)
	iter := src.NewInternalIterator(&index.Options{
		IsAsc:      true,
		LowerBound: bounds.StartKey,
		UpperBound: bounds.EndKey,
	})
	if iter == nil {
		return nil, fmt.Errorf("snapshot: nil internal iterator")
	}
	defer func() { _ = iter.Close() }()

	var entries []*kv.Entry
	for iter.Rewind(); iter.Valid(); iter.Next() {
		item := iter.Item()
		if item == nil || item.Entry() == nil {
			continue
		}
		_, userKey, _, ok := kv.SplitInternalKey(item.Entry().Key)
		if !ok {
			return nil, fmt.Errorf("snapshot: invalid internal key")
		}
		if !keyInRegion(bounds, userKey) {
			continue
		}
		materialized, err := src.MaterializeInternalEntry(item.Entry())
		if err != nil {
			return nil, fmt.Errorf("snapshot: materialize entry: %w", err)
		}
		entries = append(entries, materialized)
	}
	return entries, nil
}

func snapshotCompatibility(opt *lsm.Options) Compatibility {
	if opt == nil {
		return Compatibility{}
	}
	return Compatibility{
		BlockSize:          opt.BlockSize,
		BloomFalsePositive: opt.BloomFalsePositive,
	}
}

func validateSnapshotCompatibility(meta Meta, opt *lsm.Options) error {
	expected := snapshotCompatibility(opt)
	if meta.Compatibility.BlockSize != expected.BlockSize {
		return fmt.Errorf(
			"snapshot: incompatible block size: payload=%d target=%d",
			meta.Compatibility.BlockSize,
			expected.BlockSize,
		)
	}
	if meta.Compatibility.BloomFalsePositive != expected.BloomFalsePositive {
		return fmt.Errorf(
			"snapshot: incompatible bloom false positive: payload=%g target=%g",
			meta.Compatibility.BloomFalsePositive,
			expected.BloomFalsePositive,
		)
	}
	return nil
}

func splitSnapshotEntries(entries []*kv.Entry, targetTableBytes int64) [][]*kv.Entry {
	if len(entries) == 0 {
		return nil
	}
	if targetTableBytes <= 0 {
		return [][]*kv.Entry{entries}
	}

	var (
		chunks      [][]*kv.Entry
		current     []*kv.Entry
		currentSize int64
	)
	flush := func() {
		if len(current) == 0 {
			return
		}
		chunks = append(chunks, current)
		current = nil
		currentSize = 0
	}
	for _, entry := range entries {
		estimated := int64(kv.EstimateEncodeSize(entry))
		if len(current) > 0 && currentSize+estimated > targetTableBytes {
			flush()
		}
		current = append(current, entry)
		currentSize += estimated
	}
	flush()
	return chunks
}

func keyInRegion(region localmeta.RegionMeta, userKey []byte) bool {
	if len(region.StartKey) > 0 && bytes.Compare(userKey, region.StartKey) < 0 {
		return false
	}
	if len(region.EndKey) > 0 && bytes.Compare(userKey, region.EndKey) >= 0 {
		return false
	}
	return true
}

func prepareSnapshotTempDir(workDir, pattern string, fs vfs.FS) (string, func(), error) {
	fs = vfs.Ensure(fs)
	base := workDir
	if base == "" {
		base = os.TempDir()
	}
	if err := fs.MkdirAll(base, 0o755); err != nil {
		return "", nil, fmt.Errorf("snapshot: create temp base %s: %w", base, err)
	}
	dir, err := os.MkdirTemp(base, pattern)
	if err != nil {
		return "", nil, fmt.Errorf("snapshot: create temp dir in %s: %w", base, err)
	}
	cleanup := func() { _ = fs.RemoveAll(dir) }
	return dir, cleanup, nil
}
