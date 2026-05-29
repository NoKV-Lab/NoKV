// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package vfs

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

type noFDFile struct {
	file *os.File
}

func (f *noFDFile) Read(p []byte) (int, error) {
	return f.file.Read(p)
}

func (f *noFDFile) ReadAt(p []byte, off int64) (int, error) {
	return f.file.ReadAt(p, off)
}

func (f *noFDFile) Write(p []byte) (int, error) {
	return f.file.Write(p)
}

func (f *noFDFile) WriteAt(p []byte, off int64) (int, error) {
	return f.file.WriteAt(p, off)
}

func (f *noFDFile) Seek(offset int64, whence int) (int64, error) {
	return f.file.Seek(offset, whence)
}

func (f *noFDFile) Close() error {
	return f.file.Close()
}

func (f *noFDFile) Stat() (os.FileInfo, error) {
	return f.file.Stat()
}

func (f *noFDFile) Sync() error {
	return f.file.Sync()
}

func (f *noFDFile) Truncate(size int64) error {
	return f.file.Truncate(size)
}

func (f *noFDFile) Name() string {
	return f.file.Name()
}

func TestFileFDExtractsFromOSFile(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "fd-*")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	t.Cleanup(func() {
		if err := f.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			t.Fatalf("close temp file: %v", err)
		}
	})

	fd, ok := FileFD(f)
	if !ok {
		t.Fatalf("expected descriptor from os file")
	}
	if fd == 0 {
		t.Fatalf("expected non-zero descriptor")
	}
}

func TestFileFDReturnsFalseWhenDescriptorUnsupported(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "nofd-*")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	t.Cleanup(func() {
		if err := f.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			t.Fatalf("close temp file: %v", err)
		}
	})

	noFD := &noFDFile{file: f}
	fd, ok := FileFD(noFD)
	if ok {
		t.Fatalf("expected descriptor to be unavailable, got %d", fd)
	}
	if _, err := noFD.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("seek nofd file: %v", err)
	}
}

func TestSyncDirSuccess(t *testing.T) {
	dir := t.TempDir()
	if err := SyncDir(nil, dir); err != nil {
		t.Fatalf("sync dir: %v", err)
	}
}

func TestSyncDirOpenFailure(t *testing.T) {
	dir := t.TempDir()
	injected := errors.New("open dir injected")
	fs := NewFaultFSWithPolicy(OSFS{}, NewFaultPolicy(FailOnceRule(OpOpen, dir, injected)))

	err := SyncDir(fs, dir)
	if !errors.Is(err, injected) {
		t.Fatalf("expected injected error, got %v", err)
	}
}

func TestLockPrimacy(t *testing.T) {
	lockPath := filepath.Join(t.TempDir(), "LOCK")

	lock, err := (OSFS{}).Lock(lockPath)
	if err != nil {
		t.Fatalf("lock path: %v", err)
	}
	defer func() {
		if err := lock.Close(); err != nil {
			t.Fatalf("close lock: %v", err)
		}
	}()

	if _, err := os.Stat(lockPath); err != nil {
		t.Fatalf("lock file missing: %v", err)
	}

	other, err := (OSFS{}).Lock(lockPath)
	if err == nil {
		_ = other.Close()
		t.Fatalf("expected second lock acquisition to fail")
	}
}

func TestRenameNoReplaceSuccess(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "from.data")
	dst := filepath.Join(dir, "to.data")
	if err := os.WriteFile(src, []byte("value"), 0o644); err != nil {
		t.Fatalf("write src: %v", err)
	}
	if err := (OSFS{}).RenameNoReplace(src, dst); errors.Is(err, ErrRenameNoReplaceUnsupported) {
		t.Skipf("rename no-replace unsupported on this platform/filesystem: %v", err)
	} else if err != nil {
		t.Fatalf("rename no replace: %v", err)
	}
	if _, err := os.Stat(src); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected src missing, got: %v", err)
	}
	if _, err := os.Stat(dst); err != nil {
		t.Fatalf("expected dst present, got: %v", err)
	}
}

func TestRenameNoReplaceExistingTarget(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "from.data")
	dst := filepath.Join(dir, "to.data")
	if err := os.WriteFile(src, []byte("from"), 0o644); err != nil {
		t.Fatalf("write src: %v", err)
	}
	if err := os.WriteFile(dst, []byte("to"), 0o644); err != nil {
		t.Fatalf("write dst: %v", err)
	}
	err := (OSFS{}).RenameNoReplace(src, dst)
	if errors.Is(err, ErrRenameNoReplaceUnsupported) {
		t.Skipf("rename no-replace unsupported on this platform/filesystem: %v", err)
	}
	if !errors.Is(err, os.ErrExist) {
		t.Fatalf("expected os.ErrExist, got: %v", err)
	}
	if _, statErr := os.Stat(src); statErr != nil {
		t.Fatalf("expected src retained, got: %v", statErr)
	}
}
