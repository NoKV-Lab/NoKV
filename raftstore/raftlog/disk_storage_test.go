// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftlog

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/storage/vfs"
	"github.com/stretchr/testify/require"
)

func TestDiskStorageAppendAndReload(t *testing.T) {
	dir := t.TempDir()
	ds, err := OpenDiskStorage(dir, nil)
	require.NoError(t, err)

	entries := []myraft.Entry{
		{Index: 1, Term: 1, Data: []byte("a")},
		{Index: 2, Term: 1, Data: []byte("b")},
	}
	require.NoError(t, ds.Append(entries))

	hard := myraft.HardState{Term: 2, Vote: 3, Commit: 2}
	require.NoError(t, ds.SetHardState(hard))

	reloaded, err := OpenDiskStorage(dir, nil)
	require.NoError(t, err)

	last, err := reloaded.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(2), last)

	first, err := reloaded.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(1), first)

	term, err := reloaded.Term(2)
	require.NoError(t, err)
	require.Equal(t, uint64(1), term)

	got, err := reloaded.Entries(1, 3, 1024)
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Equal(t, []byte("a"), got[0].Data)
	require.Equal(t, []byte("b"), got[1].Data)

	hs, _, err := reloaded.InitialState()
	require.NoError(t, err)
	require.Equal(t, hard, hs)
}

func TestDiskStorageSaveReadyState(t *testing.T) {
	dir := t.TempDir()
	ds, err := OpenDiskStorage(dir, nil)
	require.NoError(t, err)

	rd := myraft.Ready{
		HardState: myraft.HardState{Term: 5, Vote: 2, Commit: 4},
		Entries: []myraft.Entry{
			{Index: 1, Term: 1, Data: []byte("a")},
			{Index: 2, Term: 2, Data: []byte("b")},
		},
		Snapshot: myraft.Snapshot{Metadata: myraft.SnapshotMetadata{Index: 2, Term: 2}},
	}

	require.NoError(t, ds.SaveReadyState(rd))

	hs, _, err := ds.InitialState()
	require.NoError(t, err)
	require.Equal(t, rd.HardState, hs)

	snap, err := ds.Snapshot()
	require.NoError(t, err)
	require.Equal(t, rd.Snapshot.Metadata.Index, snap.Metadata.Index)
	require.Equal(t, rd.Snapshot.Metadata.Term, snap.Metadata.Term)
}

func TestSnapshotExportImport(t *testing.T) {
	dir := t.TempDir()
	ds, err := OpenDiskStorage(dir, nil)
	require.NoError(t, err)

	snap := myraft.Snapshot{Metadata: myraft.SnapshotMetadata{Index: 10, Term: 3}}
	require.NoError(t, ds.ApplySnapshot(snap))

	path := filepath.Join(dir, "snap.out")
	require.NoError(t, ExportSnapshot(ds, path, nil))

	ds2, err := OpenDiskStorage(t.TempDir(), nil)
	require.NoError(t, err)
	require.NoError(t, ImportSnapshot(ds2, path, nil))

	loaded, err := ds2.Snapshot()
	require.NoError(t, err)
	require.Equal(t, snap.Metadata.Index, loaded.Metadata.Index)
	require.Equal(t, snap.Metadata.Term, loaded.Metadata.Term)
}

func TestImportSnapshotErrors(t *testing.T) {
	ds, err := OpenDiskStorage(t.TempDir(), nil)
	require.NoError(t, err)

	require.NoError(t, ImportSnapshot(ds, filepath.Join(t.TempDir(), "missing.snap"), nil))

	path := filepath.Join(t.TempDir(), "empty.snap")
	require.NoError(t, os.WriteFile(path, nil, 0o600))
	require.Error(t, ImportSnapshot(ds, path, nil))
}

func TestDiskStorageRemovesEmptyHardState(t *testing.T) {
	dir := t.TempDir()
	ds, err := OpenDiskStorage(dir, nil)
	require.NoError(t, err)

	require.NoError(t, ds.SetHardState(myraft.HardState{Term: 1, Vote: 1, Commit: 1}))
	hardPath := filepath.Join(dir, hardFileName)
	_, err = os.Stat(hardPath)
	require.NoError(t, err)

	require.NoError(t, ds.SetHardState(myraft.HardState{}))
	_, err = os.Stat(hardPath)
	require.True(t, os.IsNotExist(err))
}

func TestOpenDiskStorageRequiresDir(t *testing.T) {
	_, err := OpenDiskStorage("", nil)
	require.Error(t, err)
}

func TestOpenDiskStorageInjectedFailure(t *testing.T) {
	injected := errors.New("mkdir injected")
	fs := vfs.NewFaultFSWithPolicy(vfs.OSFS{}, vfs.NewFaultPolicy(
		vfs.FailOnceRule(vfs.OpMkdirAll, "", injected),
	))
	_, err := OpenDiskStorage(t.TempDir(), fs)
	require.ErrorIs(t, err, injected)
}

func TestDiskStorageLoadSnapshotUnreadable(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, snapFileName)
	require.NoError(t, os.WriteFile(path, []byte("snap"), 0o600))
	require.NoError(t, os.Chmod(path, 0))

	_, err := OpenDiskStorage(dir, nil)
	require.Error(t, err)
}

func TestDiskStorageLoadEntriesCorrupt(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, logFileName)
	f, err := os.Create(logPath)
	require.NoError(t, err)
	require.NoError(t, binary.Write(f, binary.LittleEndian, uint32(10)))
	_, err = f.Write([]byte{0x01, 0x02})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	_, err = OpenDiskStorage(dir, nil)
	require.Error(t, err)
}

func TestSnapshotExportImportInjectedFailure(t *testing.T) {
	dir := t.TempDir()
	ds, err := OpenDiskStorage(dir, nil)
	require.NoError(t, err)
	require.NoError(t, ds.ApplySnapshot(myraft.Snapshot{Metadata: myraft.SnapshotMetadata{Index: 2, Term: 1}}))

	path := filepath.Join(dir, "snap.injected")
	writeErr := errors.New("write injected")
	wfs := vfs.NewFaultFSWithPolicy(vfs.OSFS{}, vfs.NewFaultPolicy(
		vfs.FailOnceRule(vfs.OpWriteFile, path, writeErr),
	))
	err = ExportSnapshot(ds, path, wfs)
	require.ErrorIs(t, err, writeErr)

	readErr := errors.New("read injected")
	rfs := vfs.NewFaultFSWithPolicy(vfs.OSFS{}, vfs.NewFaultPolicy(
		vfs.FailOnceRule(vfs.OpReadFile, path, readErr),
	))
	ds2, err := OpenDiskStorage(t.TempDir(), nil)
	require.NoError(t, err)
	err = ImportSnapshot(ds2, path, rfs)
	require.ErrorIs(t, err, readErr)
}
