// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package sst_test

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/engine/lsm"
	"github.com/feichai0017/NoKV/engine/wal"
	local "github.com/feichai0017/NoKV/local"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	snapshot "github.com/feichai0017/NoKV/raftstore/snapshot/sst"
	"github.com/feichai0017/NoKV/utils"
	"github.com/stretchr/testify/require"
)

func TestExportDir(t *testing.T) {
	srcDB := openSnapshotDB(t)
	defer func() { _ = srcDB.Close() }()

	valueBacked := bytes.Repeat([]byte("v"), 4096)
	entries := []*kv.Entry{
		kv.NewInternalEntry(kv.CFDefault, []byte("alpha"), 3, []byte("a"), 0, 0),
		kv.NewInternalEntry(kv.CFWrite, []byte("alpha"), 3, []byte("aw"), 0, 0),
		kv.NewInternalEntry(kv.CFDefault, []byte("beta"), 2, valueBacked, 0, 0),
	}
	for _, entry := range entries {
		defer entry.DecrRef()
	}
	require.NoError(t, srcDB.ApplyInternalEntries(entries))

	region := localmeta.RegionMeta{
		ID:       7,
		StartKey: []byte("alpha"),
		EndKey:   []byte("z"),
		State:    localmeta.RegionStateRunning,
	}
	snapshotDir := filepath.Join(t.TempDir(), "region.sst.snapshot")
	result, err := snapshot.NewDBStore(srcDB).ExportSnapshotDir(snapshotDir, region)
	require.NoError(t, err)
	require.Equal(t, uint64(3), result.Meta.EntryCount)
	require.Equal(t, uint64(1), result.Meta.TableCount)
	require.True(t, result.Meta.InlineValues)

	meta, err := snapshot.ReadMeta(snapshotDir, nil)
	require.NoError(t, err)
	require.Equal(t, result.Meta.EntryCount, meta.EntryCount)
	require.Len(t, meta.Tables, 1)
	require.FileExists(t, filepath.Join(snapshotDir, meta.Tables[0].RelativePath))

	dstLSM := openSnapshotLSM(t)
	defer func() { require.NoError(t, dstLSM.Close()) }()
	imported, err := snapshot.ImportDir(dstLSM, snapshotDir, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1), imported.ImportedTables)
	require.NotZero(t, imported.ImportedBytes)

	for _, entry := range entries {
		expected, err := srcDB.MaterializeInternalEntry(entry)
		require.NoError(t, err)
		got, err := dstLSM.Get(entry.Key)
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Equal(t, expected.Value, got.Value)
		got.DecrRef()
	}
}

func TestExportDirSplitsLargeSnapshotIntoMultipleTables(t *testing.T) {
	srcDB := openSnapshotDBWithTweak(t, func(opt *local.Options) {
		opt.SSTableMaxSz = 512
	})
	defer func() { _ = srcDB.Close() }()

	var entries []*kv.Entry
	for i := range 12 {
		entry := kv.NewInternalEntry(
			kv.CFDefault,
			fmt.Appendf(nil, "key-%02d", i),
			uint64(100-i),
			bytes.Repeat([]byte{byte('a' + i)}, 256),
			0,
			0,
		)
		entries = append(entries, entry)
		defer entry.DecrRef()
	}
	require.NoError(t, srcDB.ApplyInternalEntries(entries))

	region := localmeta.RegionMeta{
		ID:       8,
		StartKey: []byte("key-"),
		EndKey:   []byte("zzz"),
		State:    localmeta.RegionStateRunning,
	}
	snapshotDir := filepath.Join(t.TempDir(), "region.multi.snapshot")
	result, err := snapshot.NewDBStore(srcDB).ExportSnapshotDir(snapshotDir, region)
	require.NoError(t, err)
	require.Greater(t, result.Meta.TableCount, uint64(1))
	require.Len(t, result.Meta.Tables, int(result.Meta.TableCount))
	for i, table := range result.Meta.Tables {
		require.Equal(t, filepath.Join("tables", fmt.Sprintf("%06d.sst", i+1)), table.RelativePath)
		require.FileExists(t, filepath.Join(snapshotDir, table.RelativePath))
	}
}

func TestExportPayloadRoundTrip(t *testing.T) {
	srcDB := openSnapshotDB(t)
	defer func() { _ = srcDB.Close() }()

	valueBacked := bytes.Repeat([]byte("w"), 4096)
	entries := []*kv.Entry{
		kv.NewInternalEntry(kv.CFDefault, []byte("alpha"), 3, []byte("a"), 0, 0),
		kv.NewInternalEntry(kv.CFDefault, []byte("beta"), 2, valueBacked, 0, 0),
	}
	for _, entry := range entries {
		defer entry.DecrRef()
	}
	require.NoError(t, srcDB.ApplyInternalEntries(entries))

	region := localmeta.RegionMeta{
		ID:       19,
		StartKey: []byte("alpha"),
		EndKey:   []byte("z"),
		State:    localmeta.RegionStateRunning,
	}
	payload, err := snapshot.NewDBStore(srcDB).ExportSnapshot(region)
	require.NoError(t, err)
	require.NotEmpty(t, payload)

	readMeta, err := snapshot.ReadPayloadMeta(payload)
	require.NoError(t, err)
	require.Equal(t, uint64(2), readMeta.EntryCount)
	require.Equal(t, region.ID, readMeta.Region.ID)

	dstLSM := openSnapshotLSM(t)
	defer func() { require.NoError(t, dstLSM.Close()) }()
	imported, err := snapshot.ImportPayload(dstLSM, t.TempDir(), payload, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1), imported.ImportedTables)
	for _, entry := range entries {
		expected, err := srcDB.MaterializeInternalEntry(entry)
		require.NoError(t, err)
		got, err := dstLSM.Get(entry.Key)
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Equal(t, expected.Value, got.Value)
		got.DecrRef()
	}
}

func TestExportPayloadToAndImportPayloadFromRoundTrip(t *testing.T) {
	srcDB := openSnapshotDB(t)
	defer func() { _ = srcDB.Close() }()

	entries := []*kv.Entry{
		kv.NewInternalEntry(kv.CFDefault, []byte("alpha"), 3, []byte("a"), 0, 0),
		kv.NewInternalEntry(kv.CFDefault, []byte("beta"), 2, bytes.Repeat([]byte("x"), 2048), 0, 0),
	}
	for _, entry := range entries {
		defer entry.DecrRef()
	}
	require.NoError(t, srcDB.ApplyInternalEntries(entries))

	region := localmeta.RegionMeta{
		ID:       23,
		StartKey: []byte("alpha"),
		EndKey:   []byte("z"),
		State:    localmeta.RegionStateRunning,
	}

	var payload bytes.Buffer
	meta, err := snapshot.NewDBStore(srcDB).ExportSnapshotTo(&payload, region)
	require.NoError(t, err)
	require.Equal(t, region.ID, meta.Region.ID)

	readMeta, err := snapshot.ReadPayloadMetaFrom(bytes.NewReader(payload.Bytes()))
	require.NoError(t, err)
	require.Equal(t, meta.EntryCount, readMeta.EntryCount)

	dstLSM := openSnapshotLSM(t)
	defer func() { require.NoError(t, dstLSM.Close()) }()
	imported, err := snapshot.ImportPayloadFrom(dstLSM, t.TempDir(), bytes.NewReader(payload.Bytes()), nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1), imported.ImportedTables)

	for _, entry := range entries {
		expected, err := srcDB.MaterializeInternalEntry(entry)
		require.NoError(t, err)
		got, err := dstLSM.Get(entry.Key)
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Equal(t, expected.Value, got.Value)
		got.DecrRef()
	}
}

func TestImportPayloadRollback(t *testing.T) {
	srcDB := openSnapshotDB(t)
	defer func() { _ = srcDB.Close() }()

	entries := []*kv.Entry{
		kv.NewInternalEntry(kv.CFDefault, []byte("alpha"), 3, []byte("a"), 0, 0),
		kv.NewInternalEntry(kv.CFDefault, []byte("beta"), 2, bytes.Repeat([]byte("r"), 1024), 0, 0),
	}
	for _, entry := range entries {
		defer entry.DecrRef()
	}
	require.NoError(t, srcDB.ApplyInternalEntries(entries))

	region := localmeta.RegionMeta{
		ID:       29,
		StartKey: []byte("alpha"),
		EndKey:   []byte("z"),
		State:    localmeta.RegionStateRunning,
	}
	payload, err := snapshot.NewDBStore(srcDB).ExportSnapshot(region)
	require.NoError(t, err)

	dstLSM := openSnapshotLSM(t)
	defer func() { require.NoError(t, dstLSM.Close()) }()
	imported, err := snapshot.ImportPayload(dstLSM, t.TempDir(), payload, nil)
	require.NoError(t, err)
	require.NotEmpty(t, imported.ImportedFileIDs)

	got, err := dstLSM.Get(entries[0].Key)
	require.NoError(t, err)
	require.NotNil(t, got)
	got.DecrRef()

	require.NoError(t, imported.Rollback())

	got, err = dstLSM.Get(entries[0].Key)
	require.ErrorIs(t, err, utils.ErrKeyNotFound)
	require.Nil(t, got)
}

func TestReadPayloadMetaRejectsMissingMeta(t *testing.T) {
	var payload bytes.Buffer
	tw := tar.NewWriter(&payload)
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Name: "tables/000001.sst",
		Mode: 0o600,
		Size: 4,
	}))
	_, err := tw.Write([]byte("fake"))
	require.NoError(t, err)
	require.NoError(t, tw.Close())

	_, err = snapshot.ReadPayloadMeta(payload.Bytes())
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing snapshot.json")
}

func TestImportPayloadRejectsMissingTableFile(t *testing.T) {
	meta := snapshot.Meta{
		Version: 1,
		Region: localmeta.RegionMeta{
			ID:       41,
			StartKey: []byte("a"),
			EndKey:   []byte("z"),
			State:    localmeta.RegionStateRunning,
		},
		EntryCount:   1,
		TableCount:   1,
		InlineValues: true,
		Compatibility: snapshot.Compatibility{
			BlockSize:          lsm.DefaultBlockSize,
			BloomFalsePositive: lsm.DefaultBloomFalsePositive,
		},
		Tables: []snapshot.TableMeta{{
			RelativePath: "tables/000001.sst",
			SmallestKey:  []byte("a"),
			LargestKey:   []byte("z"),
			EntryCount:   1,
			SizeBytes:    4,
			ValueBytes:   1,
		}},
	}
	metaBytes, err := json.Marshal(meta)
	require.NoError(t, err)

	var payload bytes.Buffer
	tw := tar.NewWriter(&payload)
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Name: "snapshot.json",
		Mode: 0o600,
		Size: int64(len(metaBytes)),
	}))
	_, err = tw.Write(metaBytes)
	require.NoError(t, err)
	require.NoError(t, tw.Close())

	dstLSM := openSnapshotLSM(t)
	defer func() { require.NoError(t, dstLSM.Close()) }()

	_, err = snapshot.ImportPayload(dstLSM, t.TempDir(), payload.Bytes(), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing table")
}

func TestImportPayloadRejectsAbsolutePath(t *testing.T) {
	meta := snapshot.Meta{
		Version: 1,
		Region: localmeta.RegionMeta{
			ID:       42,
			StartKey: []byte("a"),
			EndKey:   []byte("z"),
			State:    localmeta.RegionStateRunning,
		},
		Compatibility: snapshot.Compatibility{
			BlockSize:          lsm.DefaultBlockSize,
			BloomFalsePositive: lsm.DefaultBloomFalsePositive,
		},
	}
	metaBytes, err := json.Marshal(meta)
	require.NoError(t, err)

	var payload bytes.Buffer
	tw := tar.NewWriter(&payload)
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Name: "snapshot.json",
		Mode: 0o600,
		Size: int64(len(metaBytes)),
	}))
	_, err = tw.Write(metaBytes)
	require.NoError(t, err)
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Name: "/tmp/escape.sst",
		Mode: 0o600,
		Size: 4,
	}))
	_, err = tw.Write([]byte("fake"))
	require.NoError(t, err)
	require.NoError(t, tw.Close())

	dstLSM := openSnapshotLSM(t)
	defer func() { require.NoError(t, dstLSM.Close()) }()

	_, err = snapshot.ImportPayload(dstLSM, t.TempDir(), payload.Bytes(), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid sst snapshot path")
}

func TestImportPayloadRejectsParentTraversalPath(t *testing.T) {
	meta := snapshot.Meta{
		Version: 1,
		Region: localmeta.RegionMeta{
			ID:       46,
			StartKey: []byte("a"),
			EndKey:   []byte("z"),
			State:    localmeta.RegionStateRunning,
		},
		Compatibility: snapshot.Compatibility{
			BlockSize:          lsm.DefaultBlockSize,
			BloomFalsePositive: lsm.DefaultBloomFalsePositive,
		},
	}
	metaBytes, err := json.Marshal(meta)
	require.NoError(t, err)

	var payload bytes.Buffer
	tw := tar.NewWriter(&payload)
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Name: "snapshot.json",
		Mode: 0o600,
		Size: int64(len(metaBytes)),
	}))
	_, err = tw.Write(metaBytes)
	require.NoError(t, err)
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Name: "tables/../../escape.sst",
		Mode: 0o600,
		Size: 4,
	}))
	_, err = tw.Write([]byte("fake"))
	require.NoError(t, err)
	require.NoError(t, tw.Close())

	dstLSM := openSnapshotLSM(t)
	defer func() { require.NoError(t, dstLSM.Close()) }()

	_, err = snapshot.ImportPayload(dstLSM, t.TempDir(), payload.Bytes(), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid sst snapshot path")
}

func TestClosedDBSnapshotCallsReturnError(t *testing.T) {
	db := openSnapshotDB(t)
	region := localmeta.RegionMeta{
		ID:       43,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		State:    localmeta.RegionStateRunning,
	}
	require.NoError(t, db.Close())

	_, err := snapshot.NewDBStore(db).ExportSnapshot(region)
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires open db")

	_, err = snapshot.NewDBStore(db).ImportSnapshot([]byte("not-a-real-payload"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires open db")
}

func TestImportDirRejectsIncompatibleTableFormat(t *testing.T) {
	srcDB := openSnapshotDBWithTweak(t, func(opt *local.Options) {
		opt.SSTableMaxSz = 1 << 20
	})
	defer func() { _ = srcDB.Close() }()

	entry := kv.NewInternalEntry(kv.CFDefault, []byte("alpha"), 3, []byte("a"), 0, 0)
	defer entry.DecrRef()
	require.NoError(t, srcDB.ApplyInternalEntries([]*kv.Entry{entry}))

	region := localmeta.RegionMeta{
		ID:       44,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		State:    localmeta.RegionStateRunning,
	}
	snapshotDir := filepath.Join(t.TempDir(), "region.incompatible.snapshot")
	_, err := snapshot.NewDBStore(srcDB).ExportSnapshotDir(snapshotDir, region)
	require.NoError(t, err)

	dstLSM := openSnapshotLSMWithTweak(t, func(opt *lsm.Options) {
		opt.BlockSize = 2 << 10
	})
	defer func() { require.NoError(t, dstLSM.Close()) }()

	_, err = snapshot.ImportDir(dstLSM, snapshotDir, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "incompatible block size")
}

func TestImportDirRejectsIncompatibleBloomFalsePositive(t *testing.T) {
	srcDB := openSnapshotDBWithTweak(t, func(opt *local.Options) {
		opt.SSTableMaxSz = 1 << 20
	})
	defer func() { _ = srcDB.Close() }()

	entry := kv.NewInternalEntry(kv.CFDefault, []byte("alpha"), 3, []byte("a"), 0, 0)
	defer entry.DecrRef()
	require.NoError(t, srcDB.ApplyInternalEntries([]*kv.Entry{entry}))

	region := localmeta.RegionMeta{
		ID:       45,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		State:    localmeta.RegionStateRunning,
	}
	snapshotDir := filepath.Join(t.TempDir(), "region.bloom.incompatible.snapshot")
	_, err := snapshot.NewDBStore(srcDB).ExportSnapshotDir(snapshotDir, region)
	require.NoError(t, err)

	dstLSM := openSnapshotLSMWithTweak(t, func(opt *lsm.Options) {
		opt.BloomFalsePositive = 0.02
	})
	defer func() { require.NoError(t, dstLSM.Close()) }()

	_, err = snapshot.ImportDir(dstLSM, snapshotDir, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "incompatible bloom false positive")
}

func openSnapshotDB(t testing.TB) *local.DB {
	t.Helper()
	return openSnapshotDBWithTweak(t, nil)
}

func openSnapshotDBWithTweak(t testing.TB, tweak func(*local.Options)) *local.DB {
	t.Helper()
	opt := local.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	opt.SyncWrites = false
	if tweak != nil {
		tweak(opt)
	}
	db, err := local.Open(opt)
	require.NoError(t, err)
	return db
}

func testSnapshotLSMOptionsWithTweak(t testing.TB, tweak func(*lsm.Options)) *lsm.Options {
	t.Helper()
	opt := &lsm.Options{
		WorkDir:             t.TempDir(),
		MemTableSize:        1 << 20,
		SSTableMaxSz:        1 << 20,
		BlockSize:           lsm.DefaultBlockSize,
		BloomFalsePositive:  lsm.DefaultBloomFalsePositive,
		BaseLevelSize:       10 << 20,
		LevelSizeMultiplier: 10,
		BaseTableSize:       2 << 20,
		TableSizeMultiplier: 2,
		NumLevelZeroTables:  15,
		MaxLevelNum:         7,
		NumCompactors:       1,
	}
	if tweak != nil {
		tweak(opt)
	}
	return opt
}

func openSnapshotLSM(t testing.TB) *lsm.LSM {
	t.Helper()
	return openSnapshotLSMWithTweak(t, nil)
}

func openSnapshotLSMWithTweak(t testing.TB, tweak func(*lsm.Options)) *lsm.LSM {
	t.Helper()
	opt := testSnapshotLSMOptionsWithTweak(t, tweak)
	wlog, err := wal.Open(wal.Config{Dir: opt.WorkDir})
	require.NoError(t, err)
	lsmInst, err := lsm.NewLSM(opt, []*wal.Manager{wlog})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, wlog.Close()) })
	return lsmInst
}
