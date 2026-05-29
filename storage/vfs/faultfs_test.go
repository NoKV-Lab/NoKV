// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package vfs

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFaultFSInjectOpenFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "x.data")
	injected := errors.New("open injected")
	policy := NewFaultPolicy(FailOnceRule(OpOpen, path, injected))
	fs := NewFaultFSWithPolicy(OSFS{}, policy)

	require.NoError(t, fs.WriteFile(path, []byte("ok"), 0o644))
	_, err := fs.OpenHandle(path)
	require.ErrorIs(t, err, injected)
}

func TestFaultFSInjectOpenFileHandle(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "x.handle")
	injected := errors.New("openfile handle injected")
	policy := NewFaultPolicy(FailOnceRule(OpOpenFile, path, injected))
	fs := NewFaultFSWithPolicy(OSFS{}, policy)

	_, err := fs.OpenFileHandle(path, os.O_CREATE|os.O_RDWR, 0o644)
	require.ErrorIs(t, err, injected)
}

func TestFaultFSInjectLock(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "LOCK")
	injected := errors.New("lock injected")
	policy := NewFaultPolicy(FailOnceRule(OpLock, path, injected))
	fs := NewFaultFSWithPolicy(OSFS{}, policy)

	lock, err := fs.Lock(path)
	require.ErrorIs(t, err, injected)
	require.Nil(t, lock)
}

func TestFaultFileFailOnNthWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "x.write")
	injected := errors.New("file write injected")
	policy := NewFaultPolicy(FailOnNthRule(OpFileWrite, path, 2, injected))
	fs := NewFaultFSWithPolicy(OSFS{}, policy)

	f, err := fs.OpenFileHandle(path, os.O_CREATE|os.O_RDWR, 0o644)
	require.NoError(t, err)
	_, err = f.Write([]byte("ok"))
	require.NoError(t, err)
	_, err = f.Write([]byte("boom"))
	require.ErrorIs(t, err, injected)
	require.NoError(t, f.Close())
}

func TestFaultFileFailOnceSyncCloseAndTruncate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "x.sync")
	syncErr := errors.New("file sync injected")
	closeErr := errors.New("file close injected")
	truncErr := errors.New("file truncate injected")
	policy := NewFaultPolicy(
		FailOnceRule(OpFileSync, path, syncErr),
		FailOnceRule(OpFileClose, path, closeErr),
		FailOnceRule(OpFileTrunc, path, truncErr),
	)
	fs := NewFaultFSWithPolicy(OSFS{}, policy)

	f, err := fs.OpenFileHandle(path, os.O_CREATE|os.O_RDWR, 0o644)
	require.NoError(t, err)
	_, err = f.Write([]byte("payload"))
	require.NoError(t, err)
	err = f.Sync()
	require.ErrorIs(t, err, syncErr)
	err = f.Truncate(0)
	require.ErrorIs(t, err, truncErr)
	err = f.Close()
	require.ErrorIs(t, err, closeErr)
}

func TestFaultPolicyFailOnNth(t *testing.T) {
	injected := errors.New("stat injected")
	policy := NewFaultPolicy(FailOnNthRule(OpStat, "", 3, injected))
	fs := NewFaultFSWithPolicy(OSFS{}, policy)
	path := filepath.Join(t.TempDir(), "x.data")
	require.NoError(t, fs.WriteFile(path, []byte("ok"), 0o644))

	_, err := fs.Stat(path)
	require.NoError(t, err)
	_, err = fs.Stat(path)
	require.NoError(t, err)
	_, err = fs.Stat(path)
	require.ErrorIs(t, err, injected)
	_, err = fs.Stat(path)
	require.NoError(t, err)
}

func TestFaultPolicyFailAfterNth(t *testing.T) {
	injected := errors.New("read injected")
	policy := NewFaultPolicy(FailAfterNthRule(OpReadFile, "", 2, injected))
	fs := NewFaultFSWithPolicy(OSFS{}, policy)
	path := filepath.Join(t.TempDir(), "x.data")
	require.NoError(t, fs.WriteFile(path, []byte("ok"), 0o644))

	_, err := fs.ReadFile(path)
	require.NoError(t, err)
	_, err = fs.ReadFile(path)
	require.ErrorIs(t, err, injected)
	_, err = fs.ReadFile(path)
	require.ErrorIs(t, err, injected)
}

func TestFaultPolicyRenameRule(t *testing.T) {
	injected := errors.New("rename injected")
	dir := t.TempDir()
	src := filepath.Join(dir, "from.data")
	dst := filepath.Join(dir, "to.data")
	policy := NewFaultPolicy(FailOnceRenameRule(src, dst, injected))
	fs := NewFaultFSWithPolicy(OSFS{}, policy)
	require.NoError(t, fs.WriteFile(src, []byte("v"), 0o644))

	err := fs.Rename(src, dst)
	require.ErrorIs(t, err, injected)
}

func TestFaultPolicyRenameIgnoresGenericPathRule(t *testing.T) {
	injected := errors.New("rename injected")
	dir := t.TempDir()
	src := filepath.Join(dir, "from.data")
	dst := filepath.Join(dir, "to.data")
	policy := NewFaultPolicy(FailOnceRule(OpRename, src, injected))
	fs := NewFaultFSWithPolicy(OSFS{}, policy)
	require.NoError(t, fs.WriteFile(src, []byte("v"), 0o644))

	require.NoError(t, fs.Rename(src, dst))
}

func TestFaultPolicyRenameRuleOnRenameNoReplace(t *testing.T) {
	injected := errors.New("rename no replace injected")
	dir := t.TempDir()
	src := filepath.Join(dir, "from.data")
	dst := filepath.Join(dir, "to.data")
	policy := NewFaultPolicy(FailOnceRenameRule(src, dst, injected))
	fs := NewFaultFSWithPolicy(OSFS{}, policy)
	require.NoError(t, fs.WriteFile(src, []byte("v"), 0o644))

	err := fs.RenameNoReplace(src, dst)
	require.ErrorIs(t, err, injected)
}

func TestFaultFSInjectReadDirAndRemoveAll(t *testing.T) {
	dir := t.TempDir()
	readErr := errors.New("readdir injected")
	removeErr := errors.New("removeall injected")
	policy := NewFaultPolicy(
		FailOnceRule(OpReadDir, dir, readErr),
		FailOnceRule(OpRemoveAll, dir, removeErr),
	)
	fs := NewFaultFSWithPolicy(OSFS{}, policy)
	require.NoError(t, fs.MkdirAll(filepath.Join(dir, "nested"), 0o755))

	_, err := fs.ReadDir(dir)
	require.ErrorIs(t, err, readErr)
	err = fs.RemoveAll(dir)
	require.ErrorIs(t, err, removeErr)
}

func TestFaultFSPassThrough(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "x.data")
	fs := NewFaultFS(OSFS{}, nil)

	require.NoError(t, fs.MkdirAll(dir, 0o755))
	require.NoError(t, fs.WriteFile(path, []byte("ok"), 0o644))
	got, err := fs.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "ok", string(got))
}

func TestFaultFSInjectWithHookCompatibility(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "x.hook")
	injected := errors.New("hook injected")
	fs := NewFaultFS(OSFS{}, func(op Op, p string) error {
		if op == OpOpenFile && p == path {
			return injected
		}
		return nil
	})

	_, err := fs.OpenFileHandle(path, os.O_CREATE|os.O_RDWR, 0o644)
	require.ErrorIs(t, err, injected)
}
