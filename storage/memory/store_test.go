// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package memory

import (
	"testing"

	rawkv "github.com/feichai0017/NoKV/storage/kv"
	"github.com/stretchr/testify/require"
)

func TestMemoryStoreBatchAndIterator(t *testing.T) {
	store := New()
	require.NoError(t, store.ApplyBatch(rawkv.Batch{Ops: []rawkv.Mutation{
		{Op: rawkv.PutOp, Key: []byte("b"), Value: []byte("2")},
		{Op: rawkv.PutOp, Key: []byte("a"), Value: []byte("1")},
	}}))
	value, ok, err := store.Get([]byte("a"))
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("1"), value)

	it, err := store.NewIterator(rawkv.IteratorOptions{})
	require.NoError(t, err)
	defer func() { require.NoError(t, it.Close()) }()
	require.True(t, it.First())
	require.Equal(t, []byte("a"), it.Key())
	require.True(t, it.Next())
	require.Equal(t, []byte("b"), it.Key())
	require.False(t, it.Next())
}
