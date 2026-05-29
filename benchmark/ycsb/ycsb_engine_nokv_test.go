// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package ycsb

import (
	"bytes"
	"io"
	"os"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNoKVEngineCRUD(t *testing.T) {
	engine := newNoKVEngine(ycsbEngineOptions{
		BaseDir:    t.TempDir(),
		ValueSize:  8,
		MemtableMB: 1,
		SSTableMB:  4,
	})

	require.Equal(t, "NoKV", engine.Name())
	_, ok := engine.(*nokvEngine)
	require.True(t, ok)
	require.NoError(t, engine.Open(true))
	defer func() {
		require.NoError(t, engine.Close())
	}()

	key := []byte("user000000000001")
	val := []byte("value")
	require.NoError(t, engine.Insert(key, val))

	out, err := engine.Read(key, nil)
	require.NoError(t, err)
	require.Equal(t, val, out)

	newVal := []byte("value2")
	require.NoError(t, engine.Update(key, newVal))
	out, err = engine.Read(key, nil)
	require.NoError(t, err)
	require.Equal(t, newVal, out)

	count, err := engine.Scan(key, 10)
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

func TestNoKVEnginePrintStats(t *testing.T) {
	eng := newNoKVEngine(ycsbEngineOptions{
		BaseDir:    t.TempDir(),
		ValueSize:  8,
		MemtableMB: 1,
		SSTableMB:  4,
	})
	engine, ok := eng.(*nokvEngine)
	require.True(t, ok)

	require.NoError(t, engine.Open(true))
	defer func() {
		require.NoError(t, engine.Close())
	}()
	require.NoError(t, engine.Insert([]byte("key"), []byte("val")))

	old := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w
	t.Cleanup(func() {
		os.Stdout = old
	})

	engine.printStats()
	require.NoError(t, w.Close())
	out, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Contains(t, string(out), "NoKV Stats")
}

func TestNoKVEngineScanWithLargeInlineEntries(t *testing.T) {
	engine := newNoKVEngine(ycsbEngineOptions{
		BaseDir:    t.TempDir(),
		ValueSize:  256,
		MemtableMB: 1,
		SSTableMB:  4,
	})
	require.NoError(t, engine.Open(true))
	defer func() {
		require.NoError(t, engine.Close())
	}()

	val := bytes.Repeat([]byte("v"), 256)
	keys := make([][]byte, 0, 5)
	for i := 0; i < 5; i++ {
		key := formatYCSBKey(int64(i), 12)
		keys = append(keys, key)
		require.NoError(t, engine.Insert(key, val))
	}
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})

	count, err := engine.Scan(keys[0], 3)
	require.NoError(t, err)
	require.Equal(t, 3, count)
}
