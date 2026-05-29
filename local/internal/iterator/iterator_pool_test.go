// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package iterator

import (
	"testing"

	index "github.com/feichai0017/NoKV/txn/storage"
	"github.com/stretchr/testify/require"
)

type fakeIterator struct{}

func (f *fakeIterator) Next()            {}
func (f *fakeIterator) Valid() bool      { return false }
func (f *fakeIterator) Rewind()          {}
func (f *fakeIterator) Item() index.Item { return nil }
func (f *fakeIterator) Close() error     { return nil }
func (f *fakeIterator) Seek([]byte)      {}

func TestIteratorContextAndPool(t *testing.T) {
	var nilCtx *IteratorContext
	nilCtx.Append(&fakeIterator{})
	require.Nil(t, nilCtx.Iterators())
	nilCtx.reset()

	var nilPool *IteratorPool
	ctx := nilPool.Get()
	require.NotNil(t, ctx)
	require.Equal(t, uint64(0), nilPool.Reused())
	nilPool.Put(ctx)

	pool := NewIteratorPool()
	require.NotNil(t, pool)

	first := pool.Get()
	require.NotNil(t, first)
	first.Append(&fakeIterator{}, &fakeIterator{})
	require.Len(t, first.Iterators(), 2)
	pool.Put(first)

	second := pool.Get()
	require.NotNil(t, second)
	require.Empty(t, second.Iterators())
	require.LessOrEqual(t, pool.Reused(), uint64(1))
}
