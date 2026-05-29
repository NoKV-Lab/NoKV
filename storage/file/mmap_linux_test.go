// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package file

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenMmapFile(t *testing.T) {
	dir, err := os.MkdirTemp("", "nokv-mmap-test")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, os.RemoveAll(dir))
	})

	filePath := filepath.Join(dir, "test.mmap")

	// Create a new file.
	mf, err := OpenMmapFile(nil, filePath, os.O_CREATE|os.O_RDWR, 1024)
	require.NoError(t, err)
	require.NotNil(t, mf)
	require.Equal(t, 1024, len(mf.Data))
	require.NoError(t, mf.Close())

	// Open an existing file.
	mf, err = OpenMmapFile(nil, filePath, os.O_RDWR, 0)
	require.NoError(t, err)
	require.NotNil(t, mf)
	require.Equal(t, 1024, len(mf.Data))

	// Write something and sync.
	copy(mf.Data[10:20], []byte("helloworld"))
	require.NoError(t, mf.Sync())
	require.NoError(t, mf.Close())

	// Reopen as read-only and verify content.
	mf, err = OpenMmapFile(nil, filePath, os.O_RDONLY, 0)
	require.NoError(t, err)
	require.NotNil(t, mf)
	require.Equal(t, 1024, len(mf.Data))
	require.Equal(t, "helloworld", string(mf.Data[10:20]))
	require.NoError(t, mf.Close())
}

func TestMmapFile_ReadWrite(t *testing.T) {
	dir, err := os.MkdirTemp("", "nokv-mmap-rw-test")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, os.RemoveAll(dir))
	})

	filePath := filepath.Join(dir, "test.mmap")

	mf, err := OpenMmapFile(nil, filePath, os.O_CREATE|os.O_RDWR, 256)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, mf.Close())
	})

	// Use AllocateSlice to write data.
	slice, next, err := mf.AllocateSlice(10, 0)
	require.NoError(t, err)
	copy(slice, []byte("0123456789"))
	require.Equal(t, 14, next) // 4 bytes for size + 10 bytes for data

	// Use AppendBuffer to write data.
	err = mf.AppendBuffer(uint32(next), []byte("abcdef"))
	require.NoError(t, err)

	// Read back using Bytes.
	data, err := mf.Bytes(4, 10)
	require.NoError(t, err)
	require.Equal(t, "0123456789", string(data))

	// Read back using Slice.
	readSlice := mf.Slice(0)
	require.Equal(t, "0123456789", string(readSlice))

	// Read the second piece of data.
	data, err = mf.Bytes(next, 6)
	require.NoError(t, err)
	require.Equal(t, "abcdef", string(data))
}

func TestMmapFile_Resize(t *testing.T) {
	dir, err := os.MkdirTemp("", "nokv-mmap-resize-test")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, os.RemoveAll(dir))
	})

	filePath := filepath.Join(dir, "test.mmap")

	mf, err := OpenMmapFile(nil, filePath, os.O_CREATE|os.O_RDWR, 100)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, mf.Close())
	})
	require.Equal(t, 100, len(mf.Data))

	// Grow the file.
	err = mf.Truncate(200)
	require.NoError(t, err)
	require.Equal(t, 200, len(mf.Data))

	// Test auto-growth with AllocateSlice.
	_, _, err = mf.AllocateSlice(150, 50)
	require.NoError(t, err)
	require.True(t, len(mf.Data) >= 204) // 50 + 4 + 150

	// Test auto-growth with AppendBuffer.
	err = mf.AppendBuffer(uint32(len(mf.Data)-10), make([]byte, 20))
	require.NoError(t, err)
	require.True(t, len(mf.Data) > 204)
}

func TestMmapFile_Delete(t *testing.T) {
	dir, err := os.MkdirTemp("", "nokv-mmap-delete-test")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, os.RemoveAll(dir))
	})

	filePath := filepath.Join(dir, "test.mmap")

	mf, err := OpenMmapFile(nil, filePath, os.O_CREATE|os.O_RDWR, 128)
	require.NoError(t, err)

	err = mf.Delete()
	require.NoError(t, err)

	_, err = os.Stat(filePath)
	require.True(t, os.IsNotExist(err))
}
