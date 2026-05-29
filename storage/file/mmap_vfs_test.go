// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package file

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/feichai0017/NoKV/storage/vfs"
	"github.com/stretchr/testify/require"
)

func TestMmapFileDeleteUsesInjectedFS(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mmap-vfs-delete.dat")
	injected := errors.New("remove injected")
	policy := vfs.NewFaultPolicy(vfs.FailOnceRule(vfs.OpRemove, path, injected))
	fs := vfs.NewFaultFSWithPolicy(vfs.OSFS{}, policy)

	mf, err := OpenMmapFile(fs, path, os.O_CREATE|os.O_RDWR, 64)
	require.NoError(t, err)
	require.NotNil(t, mf)

	err = mf.Delete()
	require.ErrorIs(t, err, injected)
}

func TestSyncDirInjectsOpenFailure(t *testing.T) {
	dir := t.TempDir()
	injected := errors.New("open dir injected")
	policy := vfs.NewFaultPolicy(vfs.FailOnceRule(vfs.OpOpen, dir, injected))
	fs := vfs.NewFaultFSWithPolicy(vfs.OSFS{}, policy)

	err := vfs.SyncDir(fs, dir)
	require.ErrorIs(t, err, injected)
}
