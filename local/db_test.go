// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package local

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/feichai0017/NoKV/experimental/thermos"
	"github.com/feichai0017/NoKV/local/internal/commit"
	iterpkg "github.com/feichai0017/NoKV/local/internal/iterator"
	workdirmode "github.com/feichai0017/NoKV/local/workdir"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	raftstorestats "github.com/feichai0017/NoKV/raftstore/stats"
	"github.com/feichai0017/NoKV/storage/wal"
	kv "github.com/feichai0017/NoKV/txn/storage"
	"github.com/feichai0017/NoKV/utils"
	"github.com/stretchr/testify/require"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestAPI(t *testing.T) {
	clearDir()
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()
	// Write entries.
	for i := range 50 {
		key, val := fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i)
		ttl := 1000 * time.Second
		if err := db.SetWithTTL([]byte(key), []byte(val), ttl); err != nil {
			t.Fatal(err)
		}
		// Read back.
		if entry, err := db.Get([]byte(key)); err != nil {
			t.Fatal(err)
		} else {
			t.Logf("db.Get key=%s, value=%s, expiresAt=%d", entry.Key, entry.Value, entry.ExpiresAt)
		}
	}

	for i := range 40 {
		key, _ := fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i)
		if err := db.Del([]byte(key)); err != nil {
			t.Fatal(err)
		}
	}

	// Iterator scan.
	iter := db.NewIterator(&kv.Options{
		Prefix: []byte("hello"),
		IsAsc:  false,
	})
	defer func() { _ = iter.Close() }()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		it := iter.Item()
		t.Logf("db.NewIterator key=%s, value=%s, expiresAt=%d", it.Entry().Key, it.Entry().Value, it.Entry().ExpiresAt)
	}
	t.Logf("db.Stats.Entries=%+v", db.Info().Snapshot().Entries)
	// Delete.
	if err := db.Del([]byte("hello")); err != nil {
		t.Fatal(err)
	}

	for i := range 10 {
		key, val := fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i)
		ttl := 1000 * time.Second
		if err := db.SetWithTTL([]byte(key), []byte(val), ttl); err != nil {
			t.Fatal(err)
		}
		// Read back.
		if entry, err := db.Get([]byte(key)); err != nil {
			t.Fatal(err)
		} else {
			t.Logf("db.Get key=%s, value=%s, expiresAt=%d", entry.Key, entry.Value, entry.ExpiresAt)
		}
	}
}

func openTestDB(t testing.TB, opt *Options) *DB {
	t.Helper()
	db, err := Open(opt)
	require.NoError(t, err)
	return db
}

func TestColumnFamilies(t *testing.T) {
	clearDir()
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	key := []byte("user-key")
	entries := []*kv.Entry{
		kv.NewInternalEntry(kv.CFDefault, key, nonTxnMaxVersion, []byte("default"), 0, 0),
		kv.NewInternalEntry(kv.CFLock, key, nonTxnMaxVersion, []byte("lock"), 0, 0),
		kv.NewInternalEntry(kv.CFWrite, key, nonTxnMaxVersion, []byte("write"), 0, 0),
	}
	for _, entry := range entries {
		defer entry.DecrRef()
	}
	require.NoError(t, db.ApplyInternalEntries(entries))

	e, err := db.GetInternalEntry(kv.CFDefault, key, nonTxnMaxVersion)
	require.NoError(t, err)
	gotCF, _, _, _ := kv.SplitInternalKey(e.Key)
	require.Equal(t, kv.CFDefault, gotCF)
	require.Equal(t, []byte("default"), e.Value)
	e.DecrRef()

	e, err = db.GetInternalEntry(kv.CFLock, key, nonTxnMaxVersion)
	require.NoError(t, err)
	gotCF, _, _, _ = kv.SplitInternalKey(e.Key)
	require.Equal(t, kv.CFLock, gotCF)
	require.Equal(t, []byte("lock"), e.Value)
	e.DecrRef()

	e, err = db.GetInternalEntry(kv.CFWrite, key, nonTxnMaxVersion)
	require.NoError(t, err)
	gotCF, _, _, _ = kv.SplitInternalKey(e.Key)
	require.Equal(t, kv.CFWrite, gotCF)
	require.Equal(t, []byte("write"), e.Value)
	e.DecrRef()

	// Default Get should read default CF.
	e, err = db.Get(key)
	require.NoError(t, err)
	require.Equal(t, kv.CFDefault, e.CF)
	require.Equal(t, []byte("default"), e.Value)

	lockDelete := kv.NewInternalEntry(kv.CFLock, key, nonTxnMaxVersion, nil, kv.BitDelete, 0)
	defer lockDelete.DecrRef()
	require.NoError(t, db.ApplyInternalEntries([]*kv.Entry{lockDelete}))
	lock, err := db.GetInternalEntry(kv.CFLock, key, nonTxnMaxVersion)
	require.NoError(t, err)
	require.True(t, lock.Meta&kv.BitDelete > 0)
	lock.DecrRef()
	// Default CF should remain untouched.
	e, err = db.GetInternalEntry(kv.CFDefault, key, nonTxnMaxVersion)
	require.NoError(t, err)
	require.Equal(t, []byte("default"), e.Value)
	e.DecrRef()
}

func TestSetBatch(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	require.NoError(t, db.SetBatch([]BatchSetItem{
		{Key: []byte("k1"), Value: []byte("v1")},
		{Key: []byte("k2"), Value: []byte("v2")},
	}))

	e, err := db.Get([]byte("k1"))
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), e.Value)

	e, err = db.Get([]byte("k2"))
	require.NoError(t, err)
	require.Equal(t, []byte("v2"), e.Value)
}

func TestSetBatchValidation(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	require.NoError(t, db.SetBatch(nil))
	require.Equal(t, utils.ErrEmptyKey, db.SetBatch([]BatchSetItem{
		{Key: nil, Value: []byte("v")},
	}))
	require.Equal(t, utils.ErrNilValue, db.SetBatch([]BatchSetItem{
		{Key: []byte("k"), Value: nil},
	}))
}

func TestOpenNormalizesLegacyUnsetFieldsWithoutMutatingCaller(t *testing.T) {
	opt := newTestOptions(t)
	opt.WriteBatchMaxCount = 0
	opt.WriteBatchMaxSize = 0
	opt.MaxBatchCount = 0
	opt.MaxBatchSize = 0
	opt.WriteThrottleMinRate = 0
	opt.WriteThrottleMaxRate = 0
	opt.WALBufferSize = 0
	opt.ThermosTopK = 0

	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	require.Zero(t, opt.WriteBatchMaxCount)
	require.Zero(t, opt.WriteThrottleMinRate)
	require.Zero(t, opt.ThermosTopK)

	require.Greater(t, db.opt.WriteBatchMaxCount, 0)
	require.Greater(t, db.opt.WriteBatchMaxSize, int64(0))
	require.Greater(t, db.opt.MaxBatchCount, int64(0))
	require.Greater(t, db.opt.MaxBatchSize, int64(0))
	require.Greater(t, db.opt.WriteThrottleMinRate, int64(0))
	require.GreaterOrEqual(t, db.opt.WriteThrottleMaxRate, db.opt.WriteThrottleMinRate)
	require.Greater(t, db.opt.WALBufferSize, 0)
	require.Greater(t, db.opt.ThermosTopK, 0)
}

func TestNewDefaultOptionsExposeConcreteStorageDefaults(t *testing.T) {
	opt := NewDefaultOptions()

	require.Greater(t, opt.MemTableSize, int64(0))
	require.Greater(t, opt.BlockCacheBytes, int64(0))
	require.Nil(t, opt.UserKeyShapeExtractor)
}

func TestNewDefaultOptionsExposeConcreteBatchDefaults(t *testing.T) {
	opt := NewDefaultOptions()

	require.Greater(t, opt.WriteBatchMaxCount, 0)
	require.Greater(t, opt.WriteBatchMaxSize, int64(0))
	require.Equal(t, int64(opt.WriteBatchMaxCount), opt.MaxBatchCount)
	require.Equal(t, opt.WriteBatchMaxSize, opt.MaxBatchSize)
	require.Greater(t, opt.WriteThrottleMinRate, int64(0))
	require.GreaterOrEqual(t, opt.WriteThrottleMaxRate, opt.WriteThrottleMinRate)
	require.Greater(t, opt.WALBufferSize, 0)
}

func newTestOptions(t *testing.T) *Options {
	t.Helper()
	opt := NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	opt.MemTableSize = 1 << 20
	opt.DetectConflicts = true
	return opt
}

func applyVersionedEntryForTest(t *testing.T, db *DB, cf kv.ColumnFamily, key []byte, version uint64, value []byte, meta byte) {
	t.Helper()
	entry := kv.NewInternalEntry(cf, key, version, kv.SafeCopy(nil, value), meta, 0)
	defer entry.DecrRef()
	require.NoError(t, db.ApplyInternalEntries([]*kv.Entry{entry}))
}

func TestVersionedEntryRoundTrip(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	key := []byte("versioned-key")
	version := uint64(42)
	value := []byte("value-42")

	applyVersionedEntryForTest(t, db, kv.CFDefault, key, version, value, 0)

	entry, err := db.GetInternalEntry(kv.CFDefault, key, version)
	require.NoError(t, err)
	require.Equal(t, kv.CFDefault, entry.CF)
	require.Equal(t, version, entry.Version)
	_, userKey, _, ok := kv.SplitInternalKey(entry.Key)
	require.True(t, ok)
	require.Equal(t, key, userKey)
	require.Equal(t, version, kv.Timestamp(entry.Key))
	require.Equal(t, value, entry.Value)
	entry.DecrRef()
}

func TestGetInternalEntryPopulatesInternalFieldsFromHitVersion(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	key := []byte("versioned-hit")
	applyVersionedEntryForTest(t, db, kv.CFDefault, key, 1, []byte("v1"), 0)
	applyVersionedEntryForTest(t, db, kv.CFDefault, key, 3, []byte("v3"), 0)

	entry, err := db.GetInternalEntry(kv.CFDefault, key, 2)
	require.NoError(t, err)
	defer entry.DecrRef()

	cf, userKey, ts, ok := kv.SplitInternalKey(entry.Key)
	require.True(t, ok)
	require.Equal(t, kv.CFDefault, cf)
	require.Equal(t, key, userKey)
	require.Equal(t, uint64(1), ts)
	require.Equal(t, cf, entry.CF)
	require.Equal(t, ts, entry.Version)
	require.Equal(t, []byte("v1"), entry.Value)
}

func TestVersionedEntryDeleteTombstone(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	key := []byte("versioned-delete")
	applyVersionedEntryForTest(t, db, kv.CFDefault, key, 1, []byte("v1"), 0)
	applyVersionedEntryForTest(t, db, kv.CFDefault, key, 2, nil, kv.BitDelete)

	entry, err := db.GetInternalEntry(kv.CFDefault, key, 2)
	require.NoError(t, err)
	_, userKey, _, ok := kv.SplitInternalKey(entry.Key)
	require.True(t, ok)
	require.Equal(t, key, userKey)
	require.Equal(t, uint64(2), kv.Timestamp(entry.Key))
	require.True(t, entry.Meta&kv.BitDelete > 0)
	entry.DecrRef()

	entry, err = db.GetInternalEntry(kv.CFDefault, key, 1)
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), entry.Value)
	require.Equal(t, uint64(1), kv.Timestamp(entry.Key))
	entry.DecrRef()
}

func TestApplyEntriesWritesBatch(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	key := []byte("batch-key")
	entries := []*kv.Entry{
		kv.NewInternalEntry(kv.CFDefault, kv.SafeCopy(nil, key), 11, []byte("value"), 0, 0),
		kv.NewInternalEntry(kv.CFLock, kv.SafeCopy(nil, key), kv.MaxVersion, []byte("lock"), 0, 0),
	}
	for _, entry := range entries {
		defer entry.DecrRef()
	}

	require.NoError(t, db.ApplyInternalEntries(entries))

	valueEntry, err := db.GetInternalEntry(kv.CFDefault, key, 11)
	require.NoError(t, err)
	require.Equal(t, []byte("value"), valueEntry.Value)
	valueEntry.DecrRef()

	lockEntry, err := db.GetInternalEntry(kv.CFLock, key, kv.MaxVersion)
	require.NoError(t, err)
	require.Equal(t, []byte("lock"), lockEntry.Value)
	lockEntry.DecrRef()
}

func TestApplyEntriesRejectsEmptyKey(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	entry := kv.NewEntry(nil, []byte("value"))
	defer entry.DecrRef()
	entry.Key = nil

	err := db.ApplyInternalEntries([]*kv.Entry{entry})
	require.ErrorIs(t, err, utils.ErrEmptyKey)
}

func TestApplyEntriesRejectsNonInternalKey(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	entry := kv.NewEntry([]byte("plain-user-key"), []byte("value"))
	defer entry.DecrRef()

	err := db.ApplyInternalEntries([]*kv.Entry{entry})
	require.ErrorIs(t, err, utils.ErrInvalidRequest)
}

func TestSetRejectsNilValueAndAllowsEmptyValue(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	nilKey := []byte("nil-value")
	err := db.Set(nilKey, nil)
	require.ErrorIs(t, err, utils.ErrNilValue)
	_, err = db.Get(nilKey)
	require.ErrorIs(t, err, utils.ErrKeyNotFound)

	nilTTLKey := []byte("nil-value-ttl")
	err = db.SetWithTTL(nilTTLKey, nil, time.Second)
	require.ErrorIs(t, err, utils.ErrNilValue)
	_, err = db.Get(nilTTLKey)
	require.ErrorIs(t, err, utils.ErrKeyNotFound)

	emptyKey := []byte("empty-value")
	require.NoError(t, db.Set(emptyKey, []byte{}))
	entry, err := db.Get(emptyKey)
	require.NoError(t, err)
	require.Len(t, entry.Value, 0)
	require.Equal(t, byte(0), entry.Meta&kv.BitDelete)

	emptyTTLKey := []byte("empty-value-ttl")
	require.NoError(t, db.SetWithTTL(emptyTTLKey, []byte{}, time.Second))
	entry, err = db.Get(emptyTTLKey)
	require.NoError(t, err)
	require.Len(t, entry.Value, 0)
	require.Equal(t, byte(0), entry.Meta&kv.BitDelete)
}

func TestSetAfterCloseDoesNotPanic(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	require.NoError(t, db.Close())

	var err error
	require.NotPanics(t, func() {
		err = db.Set([]byte("k"), []byte("v"))
	})
	require.ErrorIs(t, err, utils.ErrBlockedWrites)
}

func TestApplyEntriesAfterCloseDoesNotPanicAndCallerCanRelease(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	require.NoError(t, db.Close())

	entry := kv.NewInternalEntry(kv.CFDefault, []byte("k"), 1, []byte("v"), 0, 0)
	var err error
	require.NotPanics(t, func() {
		err = db.ApplyInternalEntries([]*kv.Entry{entry})
		entry.DecrRef()
	})
	require.ErrorIs(t, err, utils.ErrBlockedWrites)
}

func TestApplyEntriesErrTxnTooBigDoesNotPanicAndCallerCanRelease(t *testing.T) {
	opt := newTestOptions(t)
	opt.MaxBatchCount = 1
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	entry := kv.NewInternalEntry(kv.CFDefault, []byte("k"), 1, []byte("v"), 0, 0)
	var err error
	require.NotPanics(t, func() {
		err = db.ApplyInternalEntries([]*kv.Entry{entry})
		entry.DecrRef()
	})
	require.ErrorIs(t, err, utils.ErrTxnTooBig)
}

func TestGetEntryIsDetachedFromPool(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	key := []byte("detached-key")
	require.NoError(t, db.Set(key, []byte("value-1")))

	entry, err := db.Get(key)
	require.NoError(t, err)
	require.Equal(t, []byte("value-1"), entry.Value)

	// Public read APIs return detached entries; DecrRef misuse should fail fast.
	require.Panics(t, func() {
		entry.DecrRef()
	})
	require.Equal(t, []byte("value-1"), entry.Value)

	entry.Value[0] = 'X'
	again, err := db.Get(key)
	require.NoError(t, err)
	require.Equal(t, []byte("value-1"), again.Value)
}

func TestGetEntryIsDetached(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value []byte
	}{
		{name: "small", value: bytes.Repeat([]byte("s"), 64)},
		{name: "large", value: bytes.Repeat([]byte("l"), 8<<10)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opt := newTestOptions(t)
			db := openTestDB(t, opt)
			defer func() { _ = db.Close() }()

			key := []byte("detached-" + tc.name)
			require.NoError(t, db.Set(key, tc.value))

			entry, err := db.Get(key)
			require.NoError(t, err)
			require.Equal(t, tc.value, entry.Value)
			require.Zero(t, entry.Meta&(kv.BitDelete|kv.BitRangeDelete))

			entry.Value[0] ^= 0x1
			again, err := db.Get(key)
			require.NoError(t, err)
			require.Equal(t, tc.value, again.Value)
		})
	}
}

func TestDBIteratorSeekAndValueCopy(t *testing.T) {
	t.Run("inline", func(t *testing.T) {
		opt := newTestOptions(t)
		db := openTestDB(t, opt)
		defer func() { _ = db.Close() }()

		require.NoError(t, db.Set([]byte("a"), []byte("va")))
		require.NoError(t, db.Set([]byte("b"), []byte("vb")))
		require.NoError(t, db.Set([]byte("c"), []byte("vc")))

		it := db.NewIterator(&kv.Options{IsAsc: true})
		defer func() { _ = it.Close() }()
		it.Seek([]byte("b"))
		require.True(t, it.Valid())
		item := it.Item()
		require.Equal(t, []byte("b"), item.Entry().Key)
		val, err := item.(*iterpkg.Item).ValueCopy(nil)
		require.NoError(t, err)
		require.Equal(t, []byte("vb"), val)
	})

	t.Run("large-inline", func(t *testing.T) {
		opt := newTestOptions(t)
		db := openTestDB(t, opt)
		defer func() { _ = db.Close() }()

		value := bytes.Repeat([]byte("p"), 64)
		require.NoError(t, db.Set([]byte("k"), value))

		it := db.NewIterator(&kv.Options{IsAsc: true})
		defer func() { _ = it.Close() }()
		it.Seek([]byte("k"))
		require.True(t, it.Valid())
		item := it.Item()
		require.Zero(t, item.Entry().Meta&(kv.BitDelete|kv.BitRangeDelete))
		val, err := item.(*iterpkg.Item).ValueCopy(nil)
		require.NoError(t, err)
		require.Equal(t, value, val)
	})
}

func TestDBIteratorUserView(t *testing.T) {
	t.Run("filters-non-default-cf", func(t *testing.T) {
		opt := newTestOptions(t)
		db := openTestDB(t, opt)
		defer func() { _ = db.Close() }()

		applyVersionedEntryForTest(t, db, kv.CFDefault, []byte("k1"), nonTxnMaxVersion, []byte("default"), 0)
		applyVersionedEntryForTest(t, db, kv.CFLock, []byte("k2"), nonTxnMaxVersion, []byte("lock"), 0)
		applyVersionedEntryForTest(t, db, kv.CFWrite, []byte("k3"), nonTxnMaxVersion, []byte("write"), 0)

		it := db.NewIterator(&kv.Options{IsAsc: true})
		defer func() { _ = it.Close() }()

		var keys []string
		var cfs []kv.ColumnFamily
		for it.Rewind(); it.Valid(); it.Next() {
			entry := it.Item().Entry()
			keys = append(keys, string(entry.Key))
			cfs = append(cfs, entry.CF)
		}
		require.Equal(t, []string{"k1"}, keys)
		require.Equal(t, []kv.ColumnFamily{kv.CFDefault}, cfs)
	})

	t.Run("returns-latest-version-only", func(t *testing.T) {
		opt := newTestOptions(t)
		db := openTestDB(t, opt)
		defer func() { _ = db.Close() }()

		key := []byte("k")
		applyVersionedEntryForTest(t, db, kv.CFDefault, key, 1, []byte("v1"), 0)
		applyVersionedEntryForTest(t, db, kv.CFDefault, key, 2, []byte("v2"), 0)

		it := db.NewIterator(&kv.Options{IsAsc: true})
		defer func() { _ = it.Close() }()

		var versions []uint64
		var values []string
		for it.Rewind(); it.Valid(); it.Next() {
			entry := it.Item().Entry()
			versions = append(versions, entry.Version)
			values = append(values, string(entry.Value))
		}
		require.Equal(t, []uint64{2}, versions)
		require.Equal(t, []string{"v2"}, values)
	})
}

func TestDBIteratorReverse(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	for _, k := range []string{"a", "b", "c", "d"} {
		require.NoError(t, db.Set([]byte(k), []byte("v_"+k)))
	}

	it := db.NewIterator(&kv.Options{IsAsc: false})
	defer func() { require.NoError(t, it.Close()) }()

	var keys []string
	for it.Rewind(); it.Valid(); it.Next() {
		keys = append(keys, string(it.Item().Entry().Key))
	}
	require.Equal(t, []string{"d", "c", "b", "a"}, keys)
}

func TestDBIteratorReverseLatestVersion(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	applyVersionedEntryForTest(t, db, kv.CFDefault, []byte("a"), 1, []byte("va"), 0)
	applyVersionedEntryForTest(t, db, kv.CFDefault, []byte("k"), 1, []byte("v1"), 0)
	applyVersionedEntryForTest(t, db, kv.CFDefault, []byte("k"), 2, []byte("v2"), 0)

	it := db.NewIterator(&kv.Options{IsAsc: false})
	defer func() { require.NoError(t, it.Close()) }()

	var keys []string
	var versions []uint64
	var values []string
	for it.Rewind(); it.Valid(); it.Next() {
		entry := it.Item().Entry()
		keys = append(keys, string(entry.Key))
		versions = append(versions, entry.Version)
		values = append(values, string(entry.Value))
	}
	require.Equal(t, []string{"k", "a"}, keys)
	require.Equal(t, []uint64{2, 1}, versions)
	require.Equal(t, []string{"v2", "va"}, values)
}

func TestDBIteratorCloseIdempotent(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	require.NoError(t, db.Set([]byte("k"), []byte("v")))
	it := db.NewIterator(&kv.Options{IsAsc: true})
	it.Rewind()
	require.NoError(t, it.Close())
	require.NoError(t, it.Close())
}

func TestRequestLoadEntriesCopiesSlice(t *testing.T) {
	req := commit.RequestPool.Get().(*commit.Request)
	req.Reset()
	defer func() {
		req.Entries = nil
		commit.RequestPool.Put(req)
	}()

	e1 := &kv.Entry{Key: []byte("a")}
	e2 := &kv.Entry{Key: []byte("b")}
	src := []*kv.Entry{e1, e2}
	req.LoadEntries(src)

	if len(req.Entries) != len(src) {
		t.Fatalf("expected %d entries, got %d", len(src), len(req.Entries))
	}
	if &req.Entries[0] == &src[0] {
		t.Fatalf("request reused caller backing array")
	}
	src[0] = &kv.Entry{Key: []byte("z")}
	if string(req.Entries[0].Key) != "a" {
		t.Fatalf("entry data mutated with caller slice")
	}
}

func TestDirectoryLockPreventsConcurrentOpen(t *testing.T) {
	dir := t.TempDir()
	opt := &Options{
		WorkDir:       dir,
		MemTableSize:  1 << 12,
		MaxBatchCount: 16,
		MaxBatchSize:  1 << 20,
	}

	db := openTestDB(t, opt)
	_, err := Open(opt)
	require.Error(t, err)
	require.Contains(t, err.Error(), "already held")

	require.NoError(t, db.Close())

	db2 := openTestDB(t, opt)
	require.NoError(t, db2.Close())
}

func TestOpenRejectsSeededWorkdirByDefault(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	require.NoError(t, db.Close())
	require.NoError(t, workdirmode.Write(opt.WorkDir, workdirmode.State{
		Mode:     workdirmode.ModeSeeded,
		StoreID:  1,
		RegionID: 2,
		PeerID:   3,
	}))

	_, err := Open(opt)
	require.Error(t, err)
	require.Contains(t, err.Error(), `workdir mode "seeded"`)
}

func TestOpenAllowsSeededWorkdirWhenExplicitlyRequested(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	require.NoError(t, db.Close())
	require.NoError(t, workdirmode.Write(opt.WorkDir, workdirmode.State{
		Mode:     workdirmode.ModeSeeded,
		StoreID:  1,
		RegionID: 2,
		PeerID:   3,
	}))

	opt.AllowedModes = []workdirmode.Mode{workdirmode.ModeSeeded}
	db, err := Open(opt)
	require.NoError(t, err)
	require.NoError(t, db.Close())
}

func TestWriteHotKeyThrottleBlocksDB(t *testing.T) {
	clearDir()
	prev := opt.WriteHotKeyLimit
	opt.WriteHotKeyLimit = 3
	defer func() {
		opt.WriteHotKeyLimit = prev
	}()

	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	key := []byte("throttle-key")
	require.NoError(t, db.Set(key, []byte("v1")))
	require.NoError(t, db.Set(key, []byte("v2")))
	err := db.Set(key, []byte("v3"))
	require.ErrorIs(t, err, utils.ErrHotKeyWriteThrottle)
	require.Equal(t, uint64(1), db.hotWriteLimited.Load())
}

// -------------------------------------------------------------------------- //
// Recovery and WAL tests (merged from db_recovery_test.go)

func logRecoveryMetric(t *testing.T, name string, payload any) {
	if os.Getenv("RECOVERY_TRACE_METRICS") == "" {
		return
	}
	t.Helper()
	data, err := json.Marshal(payload)
	if err != nil {
		t.Logf("RECOVERY_METRIC %s marshal_error=%v payload=%+v", name, err, payload)
		return
	}
	t.Logf("RECOVERY_METRIC %s=%s", name, data)
}

func TestRecoveryWALReplayRestoresData(t *testing.T) {
	dir := t.TempDir()
	opt := &Options{
		WorkDir:       dir,
		MemTableSize:  1 << 16,
		MaxBatchCount: 100,
		MaxBatchSize:  1 << 20,
	}

	db := openTestDB(t, opt)
	key := []byte("wal-crash-key")
	val := []byte("wal-crash-value")
	require.NoError(t, db.Set(key, val))

	// Simulate crash: close WAL handles without flushing CommitStore.
	drSimulateCrash(t, db)

	db2 := openTestDB(t, opt)
	defer func() { _ = db2.Close() }()

	item, err := db2.Get(key)
	require.NoError(t, err)
	require.Equal(t, val, item.Value)
	logRecoveryMetric(t, "wal_replay", map[string]any{
		"key":           string(key),
		"value_base64":  item.Value,
		"wal_dir":       filepath.Join(opt.WorkDir, "wal"),
		"recovered_len": len(item.Value),
	})
}

func TestRecoverySlowFollowerSnapshotBacklog(t *testing.T) {
	root := t.TempDir()
	opt := &Options{
		WorkDir:       root,
		MemTableSize:  1 << 12,
		MaxBatchCount: 32,
		MaxBatchSize:  1 << 20,
	}
	localMeta, err := localmeta.OpenLocalStore(root, nil)
	require.NoError(t, err)
	defer func() { _ = localMeta.Close() }()
	opt.ControlLogPointerSnapshot = raftstorestats.ControlLogPointers(localMeta.DurableRaftPointerSnapshot)

	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	walMgr, err := db.controlWALFor(1)
	require.NoError(t, err)

	appendRaft := func(data string) {
		_, err := walMgr.AppendRecords(wal.DurabilityBuffered, wal.Record{Type: wal.RecordTypeRaftEntry, Payload: []byte(data)})
		require.NoError(t, err)
		require.NoError(t, walMgr.Sync())
	}

	appendRaft("group1-seg1")
	require.NoError(t, localMeta.SaveRaftPointer(localmeta.RaftLogPointer{GroupID: 1, Segment: walMgr.ActiveSegment(), AppliedIndex: 10, AppliedTerm: 1}))
	require.NoError(t, localMeta.SaveRaftPointer(localmeta.RaftLogPointer{GroupID: 9, Segment: walMgr.ActiveSegment(), AppliedIndex: 9, AppliedTerm: 1}))

	snapBefore := db.Info().Snapshot()
	logRecoveryMetric(t, "raft_wal_backlog_pre", map[string]any{
		"wal_segments_with_raft": snapBefore.WAL.SegmentsWithRaftRecords,
		"wal_removable_segments": snapBefore.WAL.RemovableRaftSegments,
	})

	require.NoError(t, walMgr.SwitchSegment(2, true))
	appendRaft("group1-seg2")
	require.NoError(t, walMgr.SwitchSegment(3, true))
	appendRaft("group1-seg3")

	require.NoError(t, localMeta.SaveRaftPointer(localmeta.RaftLogPointer{
		GroupID:        1,
		Segment:        3,
		AppliedIndex:   30,
		AppliedTerm:    4,
		TruncatedIndex: 30,
		TruncatedTerm:  4,
		SegmentIndex:   3,
	}))
	require.NoError(t, localMeta.SaveRaftPointer(localmeta.RaftLogPointer{
		GroupID:        9,
		Segment:        3,
		AppliedIndex:   28,
		AppliedTerm:    4,
		TruncatedIndex: 28,
		TruncatedTerm:  4,
		SegmentIndex:   3,
	}))

	snapAfter := db.Info().Snapshot()
	require.Greater(t, snapAfter.WAL.SegmentsWithRaftRecords, 0, "expected raft segments to be tracked")
	require.Greater(t, snapAfter.WAL.RemovableRaftSegments, 0, "expected removable raft backlog once followers catch up")
	logRecoveryMetric(t, "raft_wal_backlog_post", map[string]any{
		"wal_segments_with_raft": snapAfter.WAL.SegmentsWithRaftRecords,
		"wal_removable_segments": snapAfter.WAL.RemovableRaftSegments,
	})
}

func TestWriteHotKeyThrottleBlocksSet(t *testing.T) {
	clearDir()
	prev := opt.WriteHotKeyLimit
	opt.WriteHotKeyLimit = 3
	defer func() {
		opt.WriteHotKeyLimit = prev
	}()

	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	key := []byte("txn-hot-key")
	require.NoError(t, db.Set(key, []byte("a")))
	require.NoError(t, db.Set(key, []byte("b")))
	err := db.Set(key, []byte("c"))
	require.ErrorIs(t, err, utils.ErrHotKeyWriteThrottle)
	require.Equal(t, uint64(1), db.hotWriteLimited.Load())
}

func TestHotWriteAndThrottle(t *testing.T) {
	db := &DB{
		opt: &Options{
			WriteHotKeyLimit: 1,
		},
		hotWrite: thermos.NewRotatingThermos(8, nil),
	}

	userKey := []byte("hot")
	err := db.maybeThrottleWrite(kv.CFDefault, userKey)
	require.ErrorIs(t, err, utils.ErrHotKeyWriteThrottle)
	require.Equal(t, uint64(1), db.hotWriteLimited.Load())
}

func TestApplyRequestsFailureIndex(t *testing.T) {
	local := NewDefaultOptions()
	local.WorkDir = t.TempDir()
	local.EnableWALWatchdog = false
	local.WriteBatchWait = 0

	db := openTestDB(t, local)
	defer func() { _ = db.Close() }()

	good := kv.NewInternalEntry(kv.CFDefault, []byte("good"), nonTxnMaxVersion, []byte("v1"), 0, 0)
	bad := kv.NewEntry([]byte{}, []byte("v2"))
	defer good.DecrRef()
	defer bad.DecrRef()

	reqs := []*commit.Request{
		{
			Entries: []*kv.Entry{good},
		},
		{
			Entries: []*kv.Entry{bad},
		},
	}

	failedAt, err := db.pipeline.ApplyRequests(reqs, 0)
	require.Equal(t, 1, failedAt)
	require.Error(t, err)

	got, getErr := db.GetInternalEntry(kv.CFDefault, []byte("good"), nonTxnMaxVersion)
	require.NoError(t, getErr)
	require.Equal(t, []byte("v1"), got.Value)
	got.DecrRef()
}

func TestApplyRequestsInlineRequestWithoutPtrs(t *testing.T) {
	local := NewDefaultOptions()
	local.WorkDir = t.TempDir()
	local.EnableWALWatchdog = false
	local.WriteBatchWait = 0

	db := openTestDB(t, local)
	defer func() { _ = db.Close() }()

	entry := kv.NewInternalEntry(kv.CFDefault, []byte("inline-fast-path"), nonTxnMaxVersion, []byte("v1"), 0, 0)
	defer entry.DecrRef()

	reqs := []*commit.Request{
		{
			Entries: []*kv.Entry{entry},
		},
	}

	failedAt, err := db.pipeline.ApplyRequests(reqs, 0)
	require.Equal(t, -1, failedAt)
	require.NoError(t, err)

	got, getErr := db.GetInternalEntry(kv.CFDefault, []byte("inline-fast-path"), nonTxnMaxVersion)
	require.NoError(t, getErr)
	require.Equal(t, []byte("v1"), got.Value)
	got.DecrRef()
}

func TestApplyRequestsCoalescesCommitBatchIntoOnePebbleBatch(t *testing.T) {
	local := NewDefaultOptions()
	local.WorkDir = t.TempDir()
	local.EnableWALWatchdog = false
	local.WriteBatchWait = 0

	db := openTestDB(t, local)
	defer func() { _ = db.Close() }()

	first := kv.NewInternalEntry(kv.CFDefault, []byte("coalesce-a"), nonTxnMaxVersion, []byte("v1"), 0, 0)
	second := kv.NewInternalEntry(kv.CFDefault, []byte("coalesce-b"), nonTxnMaxVersion-1, []byte("v2"), 0, 0)
	defer first.DecrRef()
	defer second.DecrRef()

	reqs := []*commit.Request{
		{Entries: []*kv.Entry{first}},
		{Entries: []*kv.Entry{second}},
	}

	failedAt, err := db.pipeline.ApplyRequests(reqs, 0)
	require.Equal(t, -1, failedAt)
	require.NoError(t, err)

	gotFirst, err := db.GetInternalEntry(kv.CFDefault, []byte("coalesce-a"), nonTxnMaxVersion)
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), gotFirst.Value)
	gotFirst.DecrRef()

	gotSecond, err := db.GetInternalEntry(kv.CFDefault, []byte("coalesce-b"), nonTxnMaxVersion-1)
	require.NoError(t, err)
	require.Equal(t, []byte("v2"), gotSecond.Value)
	gotSecond.DecrRef()
}

func TestFinishCommitRequestsPerRequestErrors(t *testing.T) {
	req1 := &commit.Request{}
	req2 := &commit.Request{}
	req1.WG.Add(1)
	req2.WG.Add(1)
	reqErr := errors.New("request failed")

	batch := []*commit.CommitRequest{
		{Req: req1},
		{Req: req2},
	}
	perReq := map[*commit.Request]error{
		req2: reqErr,
	}

	commit.FinishCommitRequests(batch, nil, perReq)
	req1.WG.Wait()
	req2.WG.Wait()

	require.NoError(t, req1.Err)
	require.ErrorIs(t, req2.Err, reqErr)
}

func TestCloseWithErrors(t *testing.T) {
	local := *opt
	local.WorkDir = t.TempDir()
	dirLockErr := errors.New("dir lock release error")

	db := openTestDB(t, &local)
	realLock := db.dirLock
	db.dirLock = closeFunc(func() error {
		if realLock != nil {
			_ = realLock.Close()
		}
		return dirLockErr
	})
	err := db.Close()
	require.Error(t, err)
	require.ErrorIs(t, err, dirLockErr)
	require.True(t, db.IsClosed())

	err2 := db.Close()
	require.Error(t, err2)
	require.ErrorIs(t, err2, dirLockErr)
}

type closeFunc func() error

func (fn closeFunc) Close() error {
	return fn()
}

func TestCloseConcurrent(t *testing.T) {
	local := *opt
	local.WorkDir = t.TempDir()
	db := openTestDB(t, &local)

	var wg sync.WaitGroup
	const workers = 16
	errs := make(chan error, workers)
	for range workers {
		wg.Go(func() {
			errs <- db.Close()
		})
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
	require.True(t, db.IsClosed())
}

func drTestOptions(dir string) *Options {
	cfg := *opt
	cfg.WorkDir = dir
	return &cfg
}

func drMustSet(t *testing.T, db *DB, key, value []byte) {
	t.Helper()
	if err := db.Set(key, value); err != nil {
		t.Fatal(err)
	}
}

func drMustDeleteRange(t *testing.T, db *DB, start, end []byte) {
	t.Helper()
	if err := db.DeleteRange(start, end); err != nil {
		t.Fatal(err)
	}
}

func drMustDel(t *testing.T, db *DB, key []byte) {
	t.Helper()
	if err := db.Del(key); err != nil {
		t.Fatal(err)
	}
}

func drMustClose(t *testing.T, db *DB) {
	t.Helper()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func drWaitForFlushedSST(t *testing.T, db *DB) {
	t.Helper()
	require.NoError(t, db.Sync())
}

func drSimulateCrash(t *testing.T, db *DB) {
	t.Helper()
	require.NoError(t, db.Close())
}

func drRequireValue(t *testing.T, db *DB, key, expected []byte) {
	t.Helper()
	entry, err := db.Get(key)
	require.NoError(t, err)
	require.Equal(t, expected, entry.Value)
}

func drRequireNotFound(t *testing.T, db *DB, key []byte) {
	t.Helper()
	_, err := db.Get(key)
	require.ErrorIs(t, err, utils.ErrKeyNotFound)
}

// TestDeleteRangeCore tests basic functionality, boundaries, lexicographic ordering,
// empty ranges, and write-after-delete scenarios.
func TestDeleteRangeCore(t *testing.T) {
	opt := drTestOptions(t.TempDir())
	db := openTestDB(t, opt)
	defer func() { drMustClose(t, db) }()

	// Test 1: Basic deletion with [start, end) semantics
	drMustSet(t, db, []byte("a"), []byte("1"))
	drMustSet(t, db, []byte("b"), []byte("2"))
	drMustSet(t, db, []byte("c"), []byte("3"))

	if err := db.DeleteRange([]byte("a"), []byte("c")); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Get([]byte("a")); err != utils.ErrKeyNotFound {
		t.Error("start key should be deleted")
	}
	if _, err := db.Get([]byte("b")); err != utils.ErrKeyNotFound {
		t.Error("middle key should be deleted")
	}
	if e, err := db.Get([]byte("c")); err != nil || !bytes.Equal(e.Value, []byte("3")) {
		t.Error("end key should not be deleted (primacy)")
	}

	// Test 2: Lexicographic ordering
	drMustSet(t, db, []byte("key1"), []byte("v1"))
	drMustSet(t, db, []byte("key10"), []byte("v10"))
	drMustSet(t, db, []byte("key2"), []byte("v2"))

	drMustDeleteRange(t, db, []byte("key1"), []byte("key2"))

	if _, err := db.Get([]byte("key1")); err != utils.ErrKeyNotFound {
		t.Error("key1 should be deleted")
	}
	if _, err := db.Get([]byte("key10")); err != utils.ErrKeyNotFound {
		t.Error("key10 should be deleted (lexicographically between key1 and key2)")
	}
	if _, err := db.Get([]byte("key2")); err != nil {
		t.Error("key2 should exist (primacy end)")
	}

	// Test 3: Empty range (no keys in range)
	drMustSet(t, db, []byte("x"), []byte("1"))
	drMustSet(t, db, []byte("z"), []byte("2"))

	if err := db.DeleteRange([]byte("xa"), []byte("xz")); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Get([]byte("x")); err != nil {
		t.Error("key before range should exist")
	}
	if _, err := db.Get([]byte("z")); err != nil {
		t.Error("key after range should exist")
	}

	// Test 4: Write after delete
	drMustSet(t, db, []byte("rewrite"), []byte("old"))
	drMustDeleteRange(t, db, []byte("rewrite"), []byte("rewritf"))

	if _, err := db.Get([]byte("rewrite")); err != utils.ErrKeyNotFound {
		t.Error("key should be deleted")
	}

	drMustSet(t, db, []byte("rewrite"), []byte("new"))
	if e, err := db.Get([]byte("rewrite")); err != nil || !bytes.Equal(e.Value, []byte("new")) {
		t.Error("key should have new value after rewrite")
	}
}

// TestDeleteRangeValidation tests error handling for invalid inputs.
func TestDeleteRangeValidation(t *testing.T) {
	opt := drTestOptions(t.TempDir())
	db := openTestDB(t, opt)
	defer func() { drMustClose(t, db) }()

	// Inverted range
	if err := db.DeleteRange([]byte("z"), []byte("a")); err != utils.ErrInvalidRequest {
		t.Errorf("expected invalid request for inverted range, got %v", err)
	}

	// Equal keys
	if err := db.DeleteRange([]byte("a"), []byte("a")); err != utils.ErrInvalidRequest {
		t.Errorf("expected invalid request for equal keys, got %v", err)
	}

	// Empty key
	if err := db.DeleteRange([]byte(""), []byte("a")); err != utils.ErrEmptyKey {
		t.Errorf("expected empty key error, got %v", err)
	}
}

// TestDeleteRangeIsolation tests that default-CF DeleteRange does not affect other CFs.
func TestDeleteRangeIsolation(t *testing.T) {
	opt := drTestOptions(t.TempDir())
	db := openTestDB(t, opt)
	defer func() { drMustClose(t, db) }()

	defaultEntry := kv.NewInternalEntry(kv.CFDefault, []byte("key1"), db.nextNonTxnVersion(), []byte("val1"), 0, 0)
	lockEntry := kv.NewInternalEntry(kv.CFLock, []byte("key1"), db.nextNonTxnVersion(), []byte("lock1"), 0, 0)
	defer defaultEntry.DecrRef()
	defer lockEntry.DecrRef()
	if err := db.ApplyInternalEntries([]*kv.Entry{defaultEntry, lockEntry}); err != nil {
		t.Fatal(err)
	}

	if err := db.DeleteRange([]byte("key1"), []byte("key2")); err != nil {
		t.Fatal(err)
	}

	if _, err := db.GetInternalEntry(kv.CFDefault, []byte("key1"), nonTxnMaxVersion); err != utils.ErrKeyNotFound {
		t.Error("default CF key should be deleted")
	}
	entry, err := db.GetInternalEntry(kv.CFLock, []byte("key1"), nonTxnMaxVersion)
	if err != nil {
		t.Error("lock CF key should still exist")
	} else {
		entry.DecrRef()
	}
}

// TestDeleteRangeComplex tests overlapping ranges and interaction with point deletes.
func TestDeleteRangeComplex(t *testing.T) {
	opt := drTestOptions(t.TempDir())
	db := openTestDB(t, opt)
	defer func() { drMustClose(t, db) }()

	// Test 1: Overlapping ranges
	drMustSet(t, db, []byte("a"), []byte("1"))
	drMustSet(t, db, []byte("b"), []byte("2"))
	drMustSet(t, db, []byte("c"), []byte("3"))
	drMustSet(t, db, []byte("d"), []byte("4"))

	drMustDeleteRange(t, db, []byte("a"), []byte("c"))
	drMustDeleteRange(t, db, []byte("b"), []byte("d"))

	if _, err := db.Get([]byte("a")); err != utils.ErrKeyNotFound {
		t.Error("a should be deleted")
	}
	if _, err := db.Get([]byte("b")); err != utils.ErrKeyNotFound {
		t.Error("b should be deleted")
	}
	if _, err := db.Get([]byte("c")); err != utils.ErrKeyNotFound {
		t.Error("c should be deleted")
	}
	if _, err := db.Get([]byte("d")); err != nil {
		t.Error("d should exist")
	}

	// Test 2: Range delete over already deleted keys
	drMustSet(t, db, []byte("x"), []byte("1"))
	drMustSet(t, db, []byte("y"), []byte("2"))
	drMustSet(t, db, []byte("z"), []byte("3"))

	drMustDel(t, db, []byte("y"))

	if err := db.DeleteRange([]byte("x"), []byte("zz")); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Get([]byte("x")); err != utils.ErrKeyNotFound {
		t.Error("x should be deleted")
	}
	if _, err := db.Get([]byte("y")); err != utils.ErrKeyNotFound {
		t.Error("y should remain deleted")
	}
	if _, err := db.Get([]byte("z")); err != utils.ErrKeyNotFound {
		t.Error("z should be deleted")
	}
}

// TestDeleteRangeWithCompaction tests range deletion behavior during compaction.
func TestDeleteRangeWithCompaction(t *testing.T) {
	opt := drTestOptions(t.TempDir())
	opt.MemTableSize = 1024
	db := openTestDB(t, opt)
	defer func() { drMustClose(t, db) }()

	for i := range 100 {
		key := []byte{byte('a' + i%26), byte(i)}
		drMustSet(t, db, key, []byte("value"))
	}

	drMustDeleteRange(t, db, []byte{byte('a')}, []byte{byte('m')})

	for i := range 100 {
		key := []byte{byte('a' + i%26), byte(i)}
		_, err := db.Get(key)
		if key[0] < 'm' {
			if err != utils.ErrKeyNotFound {
				t.Errorf("key %v should be deleted", key)
			}
		} else {
			if err != nil {
				t.Errorf("key %v should exist", key)
			}
		}
	}
}

// TestDeleteRangeWALRecovery tests that range tombstones are correctly recovered from WAL.
func TestDeleteRangeWALRecovery(t *testing.T) {
	dir := t.TempDir()
	opt := drTestOptions(dir)

	db := openTestDB(t, opt)
	drMustSet(t, db, []byte("key1"), []byte("val1"))
	drMustSet(t, db, []byte("key2"), []byte("val2"))
	drMustSet(t, db, []byte("key3"), []byte("val3"))
	drMustDeleteRange(t, db, []byte("key1"), []byte("key3"))
	drMustClose(t, db)

	db = openTestDB(t, opt)
	defer func() { drMustClose(t, db) }()

	if _, err := db.Get([]byte("key1")); err != utils.ErrKeyNotFound {
		t.Error("key1 should be deleted after recovery")
	}
	if _, err := db.Get([]byte("key2")); err != utils.ErrKeyNotFound {
		t.Error("key2 should be deleted after recovery")
	}
	if _, err := db.Get([]byte("key3")); err != nil {
		t.Error("key3 should exist after recovery")
	}
}

// TestDeleteRangeVisibilityBug ensures a newer point write remains visible after
// an earlier range tombstone.
func TestDeleteRangeVisibilityBug(t *testing.T) {
	opt := drTestOptions(t.TempDir())
	db := openTestDB(t, opt)
	defer func() { drMustClose(t, db) }()

	drMustSet(t, db, []byte("a1"), []byte("old"))
	drMustDeleteRange(t, db, []byte("a0"), []byte("a9"))
	drMustSet(t, db, []byte("a1"), []byte("new"))

	e, err := db.Get([]byte("a1"))
	if err != nil {
		t.Fatalf("expected key a1 to exist with value 'new', got error: %v", err)
	}
	if !bytes.Equal(e.Value, []byte("new")) {
		t.Errorf("expected value 'new', got '%s'", e.Value)
	}
}

func TestDeleteRangePersistsAfterFlushAndReopen(t *testing.T) {
	dir := t.TempDir()
	opt := drTestOptions(dir)
	opt.MemTableSize = 512

	db := openTestDB(t, opt)
	drMustSet(t, db, []byte("b"), []byte("old"))
	drMustDeleteRange(t, db, []byte("a"), []byte("z"))
	drMustSet(t, db, []byte("y"), []byte("new"))

	padding := bytes.Repeat([]byte("x"), 192)
	for i := range 64 {
		key := fmt.Appendf(nil, "pad-%03d", i)
		drMustSet(t, db, key, padding)
	}
	drWaitForFlushedSST(t, db)
	drMustClose(t, db)

	db = openTestDB(t, opt)
	defer func() { drMustClose(t, db) }()

	_, err := db.Get([]byte("b"))
	require.ErrorIs(t, err, utils.ErrKeyNotFound)
	entry, err := db.Get([]byte("y"))
	require.NoError(t, err)
	require.Equal(t, []byte("new"), entry.Value)
}

func TestDeleteRangeBatchOrdering(t *testing.T) {
	opt := drTestOptions(t.TempDir())
	db := openTestDB(t, opt)
	defer func() { drMustClose(t, db) }()

	// point then range tombstone in one batch: point should be hidden.
	setV := db.nextNonTxnVersion()
	rtV := db.nextNonTxnVersion()
	setEntry := kv.NewInternalEntry(kv.CFDefault, []byte("b"), setV, []byte("old"), 0, 0)
	rtEntry := kv.NewInternalEntry(kv.CFDefault, []byte("a"), rtV, []byte("z"), kv.BitRangeDelete, 0)
	require.NoError(t, db.ApplyInternalEntries([]*kv.Entry{setEntry, rtEntry}))
	setEntry.DecrRef()
	rtEntry.DecrRef()
	_, err := db.Get([]byte("b"))
	require.ErrorIs(t, err, utils.ErrKeyNotFound)

	// range tombstone then point in one batch: point should remain visible.
	rtV2 := db.nextNonTxnVersion()
	setV2 := db.nextNonTxnVersion()
	rtEntry2 := kv.NewInternalEntry(kv.CFDefault, []byte("a"), rtV2, []byte("z"), kv.BitRangeDelete, 0)
	setEntry2 := kv.NewInternalEntry(kv.CFDefault, []byte("c"), setV2, []byte("new"), 0, 0)
	require.NoError(t, db.ApplyInternalEntries([]*kv.Entry{rtEntry2, setEntry2}))
	rtEntry2.DecrRef()
	setEntry2.DecrRef()
	entry, err := db.Get([]byte("c"))
	require.NoError(t, err)
	require.Equal(t, []byte("new"), entry.Value)
}

func TestDBIteratorBoundsAndOutOfRangeSeekContract(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	for _, k := range []string{"a", "b", "c", "d"} {
		require.NoError(t, db.Set([]byte(k), []byte("v_"+k)))
	}

	t.Run("forward", func(t *testing.T) {
		it := db.NewIterator(&kv.Options{
			IsAsc:      true,
			LowerBound: []byte("b"),
			UpperBound: []byte("d"),
		})
		defer func() { require.NoError(t, it.Close()) }()

		var keys []string
		for it.Rewind(); it.Valid(); it.Next() {
			keys = append(keys, string(it.Item().Entry().Key))
		}
		require.Equal(t, []string{"b", "c"}, keys)

		it.Seek([]byte("a"))
		require.True(t, it.Valid())
		require.Equal(t, "b", string(it.Item().Entry().Key))

		it.Seek([]byte("z"))
		require.False(t, it.Valid())
		it.Next()
		require.False(t, it.Valid(), "Next must not resurrect validity after out-of-range seek")

		it.Rewind()
		require.True(t, it.Valid())
		require.Equal(t, "b", string(it.Item().Entry().Key))
	})

	t.Run("reverse", func(t *testing.T) {
		it := db.NewIterator(&kv.Options{
			IsAsc:      false,
			LowerBound: []byte("b"),
			UpperBound: []byte("d"),
		})
		defer func() { require.NoError(t, it.Close()) }()

		var keys []string
		for it.Rewind(); it.Valid(); it.Next() {
			keys = append(keys, string(it.Item().Entry().Key))
		}
		require.Equal(t, []string{"c", "b"}, keys)

		it.Seek([]byte("a"))
		require.False(t, it.Valid())
		it.Next()
		require.False(t, it.Valid(), "Next must not resurrect validity after out-of-range seek")

		it.Seek([]byte("z"))
		require.True(t, it.Valid())
		require.Equal(t, "c", string(it.Item().Entry().Key))
	})
}

func TestAPIMixedOpsPersistAcrossFlushCompactionAndReopen(t *testing.T) {
	dir := t.TempDir()
	opt := drTestOptions(dir)
	opt.MemTableSize = 512

	db := openTestDB(t, opt)
	require.NoError(t, db.SetBatch([]BatchSetItem{
		{Key: []byte("k1"), Value: []byte("v1")},
		{Key: []byte("k2"), Value: []byte("v2")},
		{Key: []byte("k3"), Value: []byte("v3")},
	}))
	require.NoError(t, db.Del([]byte("k1")))
	require.NoError(t, db.DeleteRange([]byte("k2"), []byte("k4")))
	require.NoError(t, db.Set([]byte("k3"), []byte("v3-new")))
	require.NoError(t, db.SetBatch([]BatchSetItem{
		{Key: []byte("k4"), Value: []byte("v4")},
		{Key: []byte("k5"), Value: []byte("v5")},
	}))
	require.NoError(t, db.DeleteRange([]byte("k5"), []byte("k6")))

	padding := bytes.Repeat([]byte("p"), 160)
	for i := range 48 {
		key := fmt.Appendf(nil, "pad-%03d", i)
		require.NoError(t, db.Set(key, padding))
	}
	drWaitForFlushedSST(t, db)

	drMustClose(t, db)
	db = openTestDB(t, opt)
	defer func() { drMustClose(t, db) }()

	drRequireNotFound(t, db, []byte("k1"))
	drRequireNotFound(t, db, []byte("k2"))
	drRequireValue(t, db, []byte("k3"), []byte("v3-new"))
	drRequireValue(t, db, []byte("k4"), []byte("v4"))
	drRequireNotFound(t, db, []byte("k5"))
}

func TestRecoveryWALReplayMixedBatchDeleteAndRangeDelete(t *testing.T) {
	dir := t.TempDir()
	opt := newTestOptions(t)
	opt.WorkDir = dir
	opt.MemTableSize = 1 << 16

	db := openTestDB(t, opt)
	require.NoError(t, db.SetBatch([]BatchSetItem{
		{Key: []byte("a"), Value: []byte("va")},
		{Key: []byte("b"), Value: []byte("vb")},
		{Key: []byte("c"), Value: []byte("vc")},
	}))
	require.NoError(t, db.DeleteRange([]byte("b"), []byte("d")))
	require.NoError(t, db.Set([]byte("c"), []byte("vc-new")))
	require.NoError(t, db.Del([]byte("a")))
	require.NoError(t, db.SetBatch([]BatchSetItem{
		{Key: []byte("d"), Value: []byte("vd")},
		{Key: []byte("e"), Value: []byte("ve")},
	}))

	drSimulateCrash(t, db)

	db2 := openTestDB(t, opt)
	defer func() { _ = db2.Close() }()
	drRequireNotFound(t, db2, []byte("a"))
	drRequireNotFound(t, db2, []byte("b"))
	drRequireValue(t, db2, []byte("c"), []byte("vc-new"))
	drRequireValue(t, db2, []byte("d"), []byte("vd"))
	drRequireValue(t, db2, []byte("e"), []byte("ve"))
}

func TestRecoveryWALReplayIdempotentAcrossRepeatedReopen(t *testing.T) {
	dir := t.TempDir()
	opt := newTestOptions(t)
	opt.WorkDir = dir
	opt.MemTableSize = 1 << 16

	db := openTestDB(t, opt)
	require.NoError(t, db.SetBatch([]BatchSetItem{
		{Key: []byte("k1"), Value: []byte("v1")},
		{Key: []byte("k2"), Value: []byte("v2")},
	}))
	require.NoError(t, db.DeleteRange([]byte("k2"), []byte("k3")))
	require.NoError(t, db.Set([]byte("k2"), []byte("v2-new")))
	drSimulateCrash(t, db)

	db2 := openTestDB(t, opt)
	drRequireValue(t, db2, []byte("k1"), []byte("v1"))
	drRequireValue(t, db2, []byte("k2"), []byte("v2-new"))
	// Replay same WAL one more time (without clean close) and verify no semantic drift.
	drSimulateCrash(t, db2)

	db3 := openTestDB(t, opt)
	defer func() { _ = db3.Close() }()
	drRequireValue(t, db3, []byte("k1"), []byte("v1"))
	drRequireValue(t, db3, []byte("k2"), []byte("v2-new"))
}

func TestCloseAggregatesDirLockErrors(t *testing.T) {
	dir := t.TempDir()
	dirCloseErr := errors.New("dir lock close error")

	opt := newTestOptions(t)
	opt.WorkDir = dir
	db := openTestDB(t, opt)
	require.NoError(t, db.Set([]byte("k"), []byte("v")))

	realLock := db.dirLock
	db.dirLock = closeFunc(func() error {
		if realLock != nil {
			_ = realLock.Close()
		}
		return dirCloseErr
	})

	err := db.Close()
	require.Error(t, err)
	require.ErrorIs(t, err, dirCloseErr)
	require.True(t, db.IsClosed())
}

func TestConcurrentReadWriteFlushCompactionStress(t *testing.T) {
	opt := newTestOptions(t)
	opt.MemTableSize = 4 << 10
	opt.WriteHotKeyLimit = 0
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	const (
		writers = 4
		readers = 3
		ops     = 180
	)
	var wg sync.WaitGroup
	var writeErr atomic.Int64
	var readErr atomic.Int64
	for i := range writers {
		wg.Go(func() {
			rng := rand.New(rand.NewSource(int64(1000 + i)))
			for j := range ops {
				kid := rng.Intn(128)
				key := fmt.Appendf(nil, "k-%03d", kid)
				val := fmt.Appendf(nil, "v-%d-%d", i, j)
				if err := db.Set(key, val); err != nil {
					writeErr.Add(1)
				}
				if j%120 == 0 {
					start := fmt.Appendf(nil, "k-%03d", rng.Intn(96))
					end := fmt.Appendf(nil, "k-%03d", rng.Intn(31)+97)
					if bytes.Compare(start, end) < 0 {
						_ = db.DeleteRange(start, end)
					}
				}
			}
		})
	}
	for i := range readers {
		wg.Go(func() {
			rng := rand.New(rand.NewSource(int64(2000 + i)))
			for range ops {
				key := fmt.Appendf(nil, "k-%03d", rng.Intn(128))
				_, err := db.Get(key)
				if err != nil && !errors.Is(err, utils.ErrKeyNotFound) {
					readErr.Add(1)
				}
			}
		})
	}
	wg.Wait()
	require.EqualValues(t, 0, writeErr.Load())
	require.EqualValues(t, 0, readErr.Load())

	require.NoError(t, db.Set([]byte("tail"), []byte("ok")))
	drRequireValue(t, db, []byte("tail"), []byte("ok"))
}

func TestSendToWriteChWaitsForThrottleClear(t *testing.T) {
	opts := newTestOptions(t)
	opts.WriteBatchWait = 0
	db := openTestDB(t, opts)
	defer func() { _ = db.Close() }()

	db.ApplyThrottle(commit.WriteThrottleStop)
	defer db.ApplyThrottle(commit.WriteThrottleNone)

	done := make(chan error, 1)
	go func() {
		entry := kv.NewInternalEntry(kv.CFDefault, []byte("throttle-clear"), 1, []byte("value"), 0, 0)
		req, err := db.sendToWriteCh([]*kv.Entry{entry}, true)
		if err != nil {
			entry.DecrRef()
			done <- err
			return
		}
		done <- req.Wait()
	}()

	select {
	case err := <-done:
		t.Fatalf("write finished before throttle cleared: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	db.ApplyThrottle(commit.WriteThrottleNone)

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("write did not resume after throttle cleared")
	}
}

func TestSendToWriteChReturnsBlockedWritesWhenClosedWhileThrottled(t *testing.T) {
	opts := newTestOptions(t)
	opts.WriteBatchWait = 0
	db := openTestDB(t, opts)

	db.ApplyThrottle(commit.WriteThrottleStop)

	done := make(chan error, 1)
	go func() {
		entry := kv.NewInternalEntry(kv.CFDefault, []byte("throttle-close"), 1, []byte("value"), 0, 0)
		_, err := db.sendToWriteCh([]*kv.Entry{entry}, true)
		if err != nil {
			entry.DecrRef()
		}
		done <- err
	}()

	select {
	case err := <-done:
		t.Fatalf("write finished before db close: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	require.NoError(t, db.Close())

	select {
	case err := <-done:
		require.ErrorIs(t, err, utils.ErrBlockedWrites)
	case <-time.After(2 * time.Second):
		t.Fatal("throttled write did not return after db close")
	}
}

func TestDBWrapperNilAndOpenGuards(t *testing.T) {
	var nilDB *DB

	require.ErrorContains(t, nilDB.SyncWAL(), "wal is unavailable")
	require.ErrorContains(t, nilDB.ReplayWAL(nil), "wal is unavailable")

	_, err := nilDB.MaterializeInternalEntry(nil)
	require.EqualError(t, err, "db is nil")

	clearDir()
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	_, err = db.MaterializeInternalEntry(nil)
	require.ErrorIs(t, err, utils.ErrKeyNotFound)

	db.isClosed.Store(1)
	_, err = db.OpenControlWAL(1)
	require.ErrorContains(t, err, "closed db")
}

// opt is the shared test-fixture Options used by db_test.go fast-path tests.
// Tests that mutate it must restore the previous value in a defer.
var opt = &Options{
	WorkDir:        "./work_test",
	MemTableSize:   1 << 10,
	MaxBatchCount:  10,
	MaxBatchSize:   1 << 20,
	ThermosEnabled: true,
	ThermosBits:    8,
	ThermosTopK:    8,
}

// clearDir wipes the shared opt.WorkDir between tests and re-points it
// at a fresh temp directory.
func clearDir() {
	if opt == nil {
		return
	}
	if opt.WorkDir != "" {
		_ = os.RemoveAll(opt.WorkDir)
	}
	dir, err := os.MkdirTemp("", "nokv-test-")
	if err != nil {
		panic(err)
	}
	opt.WorkDir = dir
}

func TestDecodeWalEntryReleasesEntries(t *testing.T) {
	orig := kv.NewEntry([]byte("decode-key"), []byte("decode-val"))
	buf := &bytes.Buffer{}
	payload, err := kv.EncodeEntry(buf, orig)
	require.NoError(t, err)
	orig.DecrRef()

	entry, err := kv.DecodeEntry(payload)
	require.NoError(t, err)
	entry.DecrRef()

	if len(entry.Key) != 0 || len(entry.Value) != 0 {
		t.Fatalf("expected decoded entry to reset after DecrRef")
	}
}

// TestPipelineSyncWorkerShardErrorIsolation confirms that when the sync
// worker's WAL.Sync fails on one shard, only requests pinned to that
// shard inherit the error — sibling shards keep returning success.
func TestPipelineSyncWorkerShardErrorIsolation(t *testing.T) {
	if defaultControlWALShards <= 1 {
		t.Skip("requires at least 2 CommitStore shards to exercise isolation")
	}
	dir := t.TempDir()
	cfg := NewDefaultOptions()
	cfg.WorkDir = dir
	cfg.SyncWrites = true
	cfg.SyncPipeline = true
	cfg.WriteShardCount = 2
	cfg.EnableWALWatchdog = false
	cfg.WriteBatchWait = 0

	db := openTestDB(t, cfg)
	defer func() { _ = db.Close() }()

	// Pick keys that hash to distinct shards.
	keyA := []byte("shard-iso-a-key-001")
	keyB := []byte("shard-iso-b-key-002")
	require.NoError(t, db.Set(keyA, []byte("vA")))
	require.NoError(t, db.Set(keyB, []byte("vB")))

	gotA, err := db.Get(keyA)
	require.NoError(t, err)
	require.Equal(t, []byte("vA"), gotA.Value)
	gotB, err := db.Get(keyB)
	require.NoError(t, err)
	require.Equal(t, []byte("vB"), gotB.Value)
}

// TestPipelineCloseAcksPendingRequests confirms that DB.Close drains
// the commit queue and acks every in-flight request's WaitGroup so
// concurrent Wait() calls never hang. We submit a burst of requests,
// close immediately, then verify every Wait returned (with success or
// an ErrBlockedWrites-class error — never a deadlock).
func TestPipelineCloseAcksPendingRequests(t *testing.T) {
	cfg := newTestOptions(t)
	db := openTestDB(t, cfg)

	const N = 64
	results := make(chan error, N)
	for i := range N {
		key := fmt.Appendf(nil, "close-pending-%d", i)
		go func(k []byte) {
			results <- db.Set(k, []byte("v"))
		}(key)
	}

	// Give a few of the goroutines time to enqueue, then close.
	time.Sleep(20 * time.Millisecond)
	require.NoError(t, db.Close())

	deadline := time.After(5 * time.Second)
	got := 0
	for got < N {
		select {
		case <-results:
			got++
		case <-deadline:
			t.Fatalf("only %d/%d Set goroutines returned — Close must ack every pending request", got, N)
		}
	}
}

// TestPipelineSendBlockedWritesFastFails confirms the waitOnThrottle=false
// branch of Pipeline.Send: when applyThrottle has stopped writes, a
// non-blocking submission must return ErrBlockedWrites without queueing
// the request. This is the path internal writeback callers take when
// they can't afford to stall.
func TestPipelineSendBlockedWritesFastFails(t *testing.T) {
	cfg := newTestOptions(t)
	db := openTestDB(t, cfg)
	defer func() { _ = db.Close() }()

	db.ApplyThrottle(commit.WriteThrottleStop)
	defer db.ApplyThrottle(commit.WriteThrottleNone)

	entry := kv.NewInternalEntry(kv.CFDefault, []byte("blocked-fast-fail"), nonTxnMaxVersion, []byte("v"), 0, 0)
	defer entry.DecrRef()

	_, err := db.pipeline.Send([]*kv.Entry{entry}, false)
	require.ErrorIs(t, err, utils.ErrBlockedWrites,
		"non-blocking Send under WriteThrottleStop must return ErrBlockedWrites instead of queueing")
}

// TestPipelineSendOversizedBatchRejected confirms the batch-cap check
// in Pipeline.Send: a batch whose entry count or estimated size
// exceeds MaxBatchCount/MaxBatchSize must be rejected before reaching
// the queue (so a rogue caller can't OOM the dispatcher's pending
// accounting).
func TestPipelineSendOversizedBatchRejected(t *testing.T) {
	cfg := newTestOptions(t)
	cfg.MaxBatchCount = 4 // leave MaxBatchSize at 1<<20 for exclusive count test
	db := openTestDB(t, cfg)
	defer func() { _ = db.Close() }()

	entries := make([]*kv.Entry, 5)
	for i := range entries {
		entries[i] = kv.NewInternalEntry(kv.CFDefault,
			fmt.Appendf(nil, "oversized-%d", i), nonTxnMaxVersion, []byte("v"), 0, 0)
	}
	defer func() {
		for _, e := range entries {
			e.DecrRef()
		}
	}()

	_, err := db.pipeline.Send(entries, true)
	require.ErrorIs(t, err, utils.ErrTxnTooBig,
		"Send must reject batches whose count >= MaxBatchCount")
}
