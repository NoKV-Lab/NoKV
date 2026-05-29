// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package pebble

import (
	"testing"

	storekv "github.com/feichai0017/NoKV/storage/kv"
	"github.com/stretchr/testify/require"
)

func TestPebbleStoreBatchSnapshotAndIterator(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()

	require.NoError(t, store.ApplyBatch(storekv.Batch{Ops: []storekv.Mutation{
		{Op: storekv.PutOp, Key: []byte("a"), Value: []byte("1")},
		{Op: storekv.PutOp, Key: []byte("b"), Value: []byte("2")},
	}}))
	snap, err := store.Snapshot()
	require.NoError(t, err)
	defer func() { require.NoError(t, snap.Close()) }()

	require.NoError(t, store.Delete([]byte("a")))
	value, ok, err := snap.Get([]byte("a"))
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("1"), value)

	it, err := store.NewIterator(storekv.IteratorOptions{})
	require.NoError(t, err)
	defer func() { require.NoError(t, it.Close()) }()
	require.True(t, it.First())
	require.Equal(t, []byte("b"), it.Key())
}
