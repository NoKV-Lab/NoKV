// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package iterator_test

// External-test integration tests for the user-facing iterator,
// exercised through the root local.DB facade. Helpers are intentionally
// inlined: shared db_test.go fixtures live in the root package and an
// external test package can't see unexported identifiers.

import (
	"testing"
	"time"

	local "github.com/feichai0017/NoKV/local"
	iterpkg "github.com/feichai0017/NoKV/local/internal/iterator"
	index "github.com/feichai0017/NoKV/txn/storage"
	"github.com/stretchr/testify/require"
)

func openTestDB(t testing.TB, opt *local.Options) *local.DB {
	t.Helper()
	db, err := local.Open(opt)
	require.NoError(t, err)
	return db
}

func newTestOptions(t *testing.T) *local.Options {
	t.Helper()
	opt := local.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	opt.StorageWriteBufferBytes = 1 << 12
	opt.MaxBatchCount = 10
	opt.MaxBatchSize = 1 << 20
	return opt
}

// TestDBIteratorLegitimateFilteringNoError verifies that legitimate filtering
// (deleted entries, expired entries) does NOT set an error.
func TestDBIteratorLegitimateFilteringNoError(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	require.NotNil(t, db)
	defer func() { _ = db.Close() }()

	require.NoError(t, db.Set([]byte("key1"), []byte("val1")))
	require.NoError(t, db.Del([]byte("key1")))

	require.NoError(t, db.SetWithTTL([]byte("key2"), []byte("val2"), 1*time.Millisecond))
	time.Sleep(10 * time.Millisecond)

	iter := db.NewIterator(&index.Options{IsAsc: true})
	defer func() { _ = iter.Close() }()
	dbIter := iter.(*iterpkg.DBIterator)

	iter.Rewind()
	require.False(t, iter.Valid(), "no visible entries")

	require.Nil(t, dbIter.Err(), "legitimate filtering should not set error")
}

// TestDBIteratorEmptyDatabaseNoError verifies empty database returns no error.
func TestDBIteratorEmptyDatabaseNoError(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	require.NotNil(t, db)
	defer func() { _ = db.Close() }()

	iter := db.NewIterator(&index.Options{IsAsc: true})
	defer func() { _ = iter.Close() }()
	dbIter := iter.(*iterpkg.DBIterator)

	iter.Rewind()
	require.False(t, iter.Valid(), "empty database")
	require.Nil(t, dbIter.Err(), "no error on empty database")
}
