// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package percolator

import (
	"bytes"
	"errors"
	"fmt"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	local "github.com/feichai0017/NoKV/local"
	"github.com/feichai0017/NoKV/txn/latch"
	"github.com/feichai0017/NoKV/txn/mvcc"
	kv "github.com/feichai0017/NoKV/txn/storage"
	"github.com/feichai0017/NoKV/utils"
)

func testOptionsForDir(dir string) *local.Options {
	opt := local.NewDefaultOptions()
	opt.WorkDir = dir
	opt.MemTableSize = 1 << 12
	return opt
}

func openTestDB(t *testing.T) *local.DB {
	opt := testOptionsForDir(filepath.Join(t.TempDir(), "db"))
	db, err := local.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func applyVersionedEntryForTxnTest(t *testing.T, db *local.DB, cf kv.ColumnFamily, key []byte, version uint64, value []byte, meta byte) {
	t.Helper()
	entry := kv.NewInternalEntry(cf, key, version, kv.SafeCopy(nil, value), meta, 0)
	defer entry.DecrRef()
	require.NoError(t, db.ApplyInternalEntries([]*kv.Entry{entry}))
}

func atomicPutIfAbsentRequest(startVersion, commitVersion uint64, firstKey, firstValue, secondKey, secondValue []byte) *kvrpcpb.TryAtomicMutateRequest {
	return &kvrpcpb.TryAtomicMutateRequest{
		StartVersion:  startVersion,
		CommitVersion: commitVersion,
		Predicates: []*kvrpcpb.AtomicPredicate{
			{Key: kv.SafeCopy(nil, firstKey), Kind: kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_NOT_EXISTS},
			{Key: kv.SafeCopy(nil, secondKey), Kind: kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_NOT_EXISTS},
		},
		Mutations: []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Mutation_Put, Key: kv.SafeCopy(nil, firstKey), Value: kv.SafeCopy(nil, firstValue), AssertionNotExist: true},
			{Op: kvrpcpb.Mutation_Put, Key: kv.SafeCopy(nil, secondKey), Value: kv.SafeCopy(nil, secondValue), AssertionNotExist: true},
		},
	}
}

func atomicPutRequest(startVersion, commitVersion uint64, key, value []byte) *kvrpcpb.TryAtomicMutateRequest {
	return &kvrpcpb.TryAtomicMutateRequest{
		StartVersion:  startVersion,
		CommitVersion: commitVersion,
		Predicates: []*kvrpcpb.AtomicPredicate{
			{Key: kv.SafeCopy(nil, key), Kind: kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_NOT_EXISTS},
		},
		Mutations: []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Mutation_Put, Key: kv.SafeCopy(nil, key), Value: kv.SafeCopy(nil, value), AssertionNotExist: true},
		},
	}
}

func atomicMutateStat(t *testing.T, stats map[string]any, key string) uint64 {
	t.Helper()
	raw, ok := stats[key]
	require.Truef(t, ok, "missing stat %s", key)
	got, ok := raw.(uint64)
	require.Truef(t, ok, "stat %s has type %T", key, raw)
	return got
}

type rollbackTestStore struct {
	applyInternalEntries func(entries []*kv.Entry) error
	getInternalEntry     func(cf kv.ColumnFamily, key []byte, version uint64) (*kv.Entry, error)
	newInternalIterator  func(opt *kv.Options) kv.Iterator
}

func (s rollbackTestStore) ApplyInternalEntries(entries []*kv.Entry) error {
	if s.applyInternalEntries != nil {
		return s.applyInternalEntries(entries)
	}
	return nil
}

func (s rollbackTestStore) GetInternalEntry(cf kv.ColumnFamily, key []byte, version uint64) (*kv.Entry, error) {
	if s.getInternalEntry != nil {
		return s.getInternalEntry(cf, key, version)
	}
	return nil, utils.ErrKeyNotFound
}

func (s rollbackTestStore) NewInternalIterator(opt *kv.Options) kv.Iterator {
	if s.newInternalIterator != nil {
		return s.newInternalIterator(opt)
	}
	return &testIterator{}
}

type countingStore struct {
	base               *local.DB
	applyCalls         int
	appliedEntryCounts []int
	failApplyErr       error
	failApplyRemaining int
}

func newCountingStore(base *local.DB) *countingStore {
	return &countingStore{base: base}
}

func (s *countingStore) ApplyInternalEntries(entries []*kv.Entry) error {
	s.applyCalls++
	s.appliedEntryCounts = append(s.appliedEntryCounts, len(entries))
	if s.failApplyRemaining > 0 {
		s.failApplyRemaining--
		return s.failApplyErr
	}
	return s.base.ApplyInternalEntries(entries)
}

func (s *countingStore) GetInternalEntry(cf kv.ColumnFamily, key []byte, version uint64) (*kv.Entry, error) {
	return s.base.GetInternalEntry(cf, key, version)
}

func (s *countingStore) NewInternalIterator(opt *kv.Options) kv.Iterator {
	return s.base.NewInternalIterator(opt)
}

func (s *countingStore) failNextApply(err error) {
	s.failApplyErr = err
	s.failApplyRemaining = 1
}

type countingAtomicStore struct {
	*countingStore
}

func newCountingAtomicStore(base *local.DB) *countingAtomicStore {
	return &countingAtomicStore{countingStore: newCountingStore(base)}
}

type testIterator struct {
	items []kv.Item
	idx   int
}

func (it *testIterator) Next() {
	it.idx++
}

func (it *testIterator) Valid() bool {
	return it != nil && it.idx < len(it.items)
}

func (it *testIterator) Rewind() {
	it.idx = 0
}

func (it *testIterator) Item() kv.Item {
	if !it.Valid() {
		return nil
	}
	return it.items[it.idx]
}

func (it *testIterator) Close() error {
	return nil
}

func (it *testIterator) Seek(key []byte) {
	it.idx = 0
}

func TestPrewriteAndCommitPut(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(32)
	req := &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{{
			Op:    kvrpcpb.Mutation_Put,
			Key:   []byte("k1"),
			Value: []byte("value1"),
		}},
		PrimaryLock:  []byte("k1"),
		StartVersion: 10,
		LockTtl:      3000,
	}
	errs := Prewrite(db, latches, req)
	require.Empty(t, errs)

	reader := NewReader(db)
	lock, err := reader.GetLock([]byte("k1"))
	require.NoError(t, err)
	require.NotNil(t, lock)
	require.Equal(t, req.StartVersion, lock.Ts)
	require.NotZero(t, lock.StartTime)
	require.Equal(t, req.LockTtl, lock.TTL)

	commitReq := &kvrpcpb.CommitRequest{
		Keys:          [][]byte{[]byte("k1")},
		StartVersion:  req.StartVersion,
		CommitVersion: 20,
	}
	require.Nil(t, Commit(db, latches, commitReq))

	write, commitTs, err := reader.MostRecentWrite([]byte("k1"))
	require.NoError(t, err)
	require.NotNil(t, write)
	require.Equal(t, uint64(20), commitTs)
	require.Equal(t, req.StartVersion, write.StartTs)

	val, _, err := reader.GetValue([]byte("k1"), 30)
	require.NoError(t, err)
	require.Equal(t, []byte("value1"), val)
	lock, err = reader.GetLock([]byte("k1"))
	require.NoError(t, err)
	require.Nil(t, lock)
}

func TestApplyAtomicMutateMaterializesCommittedKeys(t *testing.T) {
	opt := testOptionsForDir(filepath.Join(t.TempDir(), "db"))
	opt.WriteShardCount = 1
	db, err := local.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	latches := latch.NewManager(16)
	req := atomicPutIfAbsentRequest(10, 11, []byte("dentry"), []byte("ino=42"), []byte("inode"), []byte("attrs"))

	result := ApplyAtomicMutate(db, latches, req)
	require.Nil(t, result.Error)
	require.False(t, result.Fallback)
	require.Equal(t, uint64(2), result.AppliedKeys)

	reader := NewReader(db)
	for _, mut := range req.GetMutations() {
		value, _, err := reader.GetValue(mut.GetKey(), req.GetCommitVersion())
		require.NoError(t, err)
		require.Equal(t, mut.GetValue(), value)
		lock, err := reader.GetLock(mut.GetKey())
		require.NoError(t, err)
		require.Nil(t, lock)
	}
	_, _, err = reader.GetValue([]byte("dentry"), req.GetStartVersion())
	require.ErrorIs(t, err, utils.ErrKeyNotFound)

	result = ApplyAtomicMutate(db, latches, req)
	require.Nil(t, result.Error)
	require.False(t, result.Fallback)
	require.Equal(t, uint64(2), result.AppliedKeys)
}

func TestApplyAtomicMutateBatchFusesIndependentRequests(t *testing.T) {
	opt := testOptionsForDir(t.TempDir())
	opt.WriteShardCount = 1
	db, err := local.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	store := newCountingAtomicStore(db)
	latches := latch.NewManager(16)
	before := Stats()
	results := ApplyAtomicMutateBatch(store, latches, []*kvrpcpb.TryAtomicMutateRequest{
		atomicPutRequest(40, 41, []byte("atomic-batch-a"), []byte("va")),
		atomicPutRequest(42, 43, []byte("atomic-batch-b"), []byte("vb")),
	})
	require.Len(t, results, 2)
	for _, result := range results {
		require.Nil(t, result.Error)
		require.False(t, result.Fallback)
		require.Equal(t, uint64(1), result.AppliedKeys)
	}
	require.Equal(t, 1, store.applyCalls)
	require.Equal(t, []int{2}, store.appliedEntryCounts)

	reader := NewReader(db)
	value, _, err := reader.GetValue([]byte("atomic-batch-a"), 50)
	require.NoError(t, err)
	require.Equal(t, []byte("va"), value)
	value, _, err = reader.GetValue([]byte("atomic-batch-b"), 50)
	require.NoError(t, err)
	require.Equal(t, []byte("vb"), value)

	after := Stats()
	require.Equal(t, atomicMutateStat(t, before, "atomic_fused_apply_batches_total")+1, atomicMutateStat(t, after, "atomic_fused_apply_batches_total"))
	require.Equal(t, atomicMutateStat(t, before, "atomic_fused_apply_requests_total")+2, atomicMutateStat(t, after, "atomic_fused_apply_requests_total"))
	require.Equal(t, atomicMutateStat(t, before, "atomic_fused_apply_entries_total")+2, atomicMutateStat(t, after, "atomic_fused_apply_entries_total"))
}

func TestApplyAtomicMutateBatchUsesSinglePebbleApplyGroup(t *testing.T) {
	const shardCount = 4

	opt := testOptionsForDir(t.TempDir())
	opt.WriteShardCount = shardCount
	db, err := local.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	first, second := keysWithAscendingCommitShards(t, shardCount)
	firstShard := testShardForInternalKey(kv.InternalKey(kv.CFDefault, first, 50), shardCount)
	secondShard := testShardForInternalKey(kv.InternalKey(kv.CFDefault, second, 52), shardCount)
	require.NotEqual(t, firstShard, secondShard)

	store := newCountingAtomicStore(db)
	results := ApplyAtomicMutateBatch(store, latch.NewManager(16), []*kvrpcpb.TryAtomicMutateRequest{
		atomicPutRequest(50, 51, first, []byte("first")),
		atomicPutRequest(52, 53, second, []byte("second")),
	})
	require.Len(t, results, 2)
	for _, result := range results {
		require.Nil(t, result.Error)
		require.False(t, result.Fallback)
		require.Equal(t, uint64(1), result.AppliedKeys)
	}
	require.Equal(t, 1, store.applyCalls)
	require.Equal(t, []int{2}, store.appliedEntryCounts)
}

func TestApplyAtomicMutateBatchPreservesOverlappingRequestOrder(t *testing.T) {
	opt := testOptionsForDir(t.TempDir())
	opt.WriteShardCount = 1
	db, err := local.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	store := newCountingAtomicStore(db)
	key := []byte("atomic-overlap")
	results := ApplyAtomicMutateBatch(store, latch.NewManager(16), []*kvrpcpb.TryAtomicMutateRequest{
		atomicPutRequest(60, 61, key, []byte("first")),
		atomicPutRequest(62, 63, key, []byte("second")),
	})
	require.Len(t, results, 2)
	require.Nil(t, results[0].Error)
	require.Equal(t, uint64(1), results[0].AppliedKeys)
	require.NotNil(t, results[1].Error)
	require.NotNil(t, results[1].Error.GetAlreadyExists())
	require.Equal(t, 1, store.applyCalls)

	value, _, err := NewReader(db).GetValue(key, 70)
	require.NoError(t, err)
	require.Equal(t, []byte("first"), value)
}

func TestApplyAtomicMutateBatchApplyFailureMarksWholeFusedGroup(t *testing.T) {
	opt := testOptionsForDir(t.TempDir())
	opt.WriteShardCount = 1
	db, err := local.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	injected := errors.New("fused atomic apply failed")
	store := newCountingAtomicStore(db)
	store.failNextApply(injected)
	results := ApplyAtomicMutateBatch(store, latch.NewManager(16), []*kvrpcpb.TryAtomicMutateRequest{
		atomicPutRequest(70, 71, []byte("atomic-fail-a"), []byte("va")),
		atomicPutRequest(72, 73, []byte("atomic-fail-b"), []byte("vb")),
	})
	require.Len(t, results, 2)
	for _, result := range results {
		require.NotNil(t, result.Error)
		require.Contains(t, result.Error.GetRetryable(), injected.Error())
	}
	require.Equal(t, 1, store.applyCalls)

	reader := NewReader(db)
	_, _, err = reader.GetValue([]byte("atomic-fail-a"), 80)
	require.ErrorIs(t, err, utils.ErrKeyNotFound)
	_, _, err = reader.GetValue([]byte("atomic-fail-b"), 80)
	require.ErrorIs(t, err, utils.ErrKeyNotFound)
}

func TestApplyAtomicMutateRejectsExistingDentry(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(16)
	require.Empty(t, Prewrite(db, latches, &kvrpcpb.PrewriteRequest{
		StartVersion: 1,
		PrimaryLock:  []byte("dentry"),
		Mutations: []*kvrpcpb.Mutation{{
			Op:    kvrpcpb.Mutation_Put,
			Key:   []byte("dentry"),
			Value: []byte("old"),
		}},
	}))
	require.Nil(t, Commit(db, latches, &kvrpcpb.CommitRequest{
		Keys:          [][]byte{[]byte("dentry")},
		StartVersion:  1,
		CommitVersion: 5,
	}))

	result := ApplyAtomicMutate(db, latches, atomicPutIfAbsentRequest(6, 7, []byte("dentry"), []byte("new"), []byte("inode"), []byte("attrs")))
	keyErr := result.Error
	require.NotNil(t, keyErr.GetAlreadyExists())
	require.Equal(t, []byte("dentry"), keyErr.GetAlreadyExists().GetKey())
}

func TestApplyAtomicMutateValueEqualsPredicate(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(16)
	key := []byte("rmw-key")
	require.Empty(t, Prewrite(db, latches, &kvrpcpb.PrewriteRequest{
		StartVersion: 1,
		PrimaryLock:  key,
		Mutations: []*kvrpcpb.Mutation{{
			Op:    kvrpcpb.Mutation_Put,
			Key:   key,
			Value: []byte("old"),
		}},
	}))
	require.Nil(t, Commit(db, latches, &kvrpcpb.CommitRequest{
		Keys:          [][]byte{key},
		StartVersion:  1,
		CommitVersion: 5,
	}))

	result := ApplyAtomicMutate(db, latches, &kvrpcpb.TryAtomicMutateRequest{
		StartVersion:  6,
		CommitVersion: 7,
		Predicates: []*kvrpcpb.AtomicPredicate{{
			Key:           kv.SafeCopy(nil, key),
			Kind:          kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_VALUE_EQUALS,
			ExpectedValue: []byte("old"),
		}},
		Mutations: []*kvrpcpb.Mutation{{
			Op:    kvrpcpb.Mutation_Put,
			Key:   kv.SafeCopy(nil, key),
			Value: []byte("new"),
		}},
	})
	require.Nil(t, result.Error)
	value, _, err := NewReader(db).GetValue(key, 7)
	require.NoError(t, err)
	require.Equal(t, []byte("new"), value)

	result = ApplyAtomicMutate(db, latches, &kvrpcpb.TryAtomicMutateRequest{
		StartVersion:  8,
		CommitVersion: 9,
		Predicates: []*kvrpcpb.AtomicPredicate{{
			Key:           kv.SafeCopy(nil, key),
			Kind:          kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_VALUE_EQUALS,
			ExpectedValue: []byte("old"),
		}},
		Mutations: []*kvrpcpb.Mutation{{
			Op:    kvrpcpb.Mutation_Put,
			Key:   kv.SafeCopy(nil, key),
			Value: []byte("bad"),
		}},
	})
	require.NotNil(t, result.Error)
	require.Contains(t, result.Error.GetRetryable(), errAtomicPredicateMismatch.Error())
	value, _, err = NewReader(db).GetValue(key, 9)
	require.NoError(t, err)
	require.Equal(t, []byte("new"), value)
}

func TestApplyAtomicMutateRejectsLockedPredicate(t *testing.T) {
	opt := testOptionsForDir(filepath.Join(t.TempDir(), "db"))
	opt.WriteShardCount = 1
	db, err := local.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	latches := latch.NewManager(16)
	predicateKey := []byte("atomic-predicate-lock")
	targetKey := []byte("atomic-target")

	require.Empty(t, Prewrite(db, latches, &kvrpcpb.PrewriteRequest{
		StartVersion: 1,
		PrimaryLock:  predicateKey,
		LockTtl:      3000,
		Mutations: []*kvrpcpb.Mutation{{
			Op:    kvrpcpb.Mutation_Put,
			Key:   predicateKey,
			Value: []byte("pending"),
		}},
	}))

	result := ApplyAtomicMutate(db, latches, &kvrpcpb.TryAtomicMutateRequest{
		StartVersion:  10,
		CommitVersion: 11,
		Predicates: []*kvrpcpb.AtomicPredicate{{
			Key:  predicateKey,
			Kind: kvrpcpb.AtomicPredicateKind_ATOMIC_PREDICATE_KIND_NOT_EXISTS,
		}},
		Mutations: []*kvrpcpb.Mutation{{
			Op:    kvrpcpb.Mutation_Put,
			Key:   targetKey,
			Value: []byte("value"),
		}},
	})
	require.NotNil(t, result.Error)
	require.NotNil(t, result.Error.GetLocked())
	require.Equal(t, predicateKey, result.Error.GetLocked().GetKey())

	_, _, err = NewReader(db).GetValue(targetKey, 11)
	require.ErrorIs(t, err, utils.ErrKeyNotFound)
}

func TestCommittedLockDoesNotHideVisiblePut(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(32)
	key := []byte("lock-visible-put")

	put := &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{{
			Op:    kvrpcpb.Mutation_Put,
			Key:   key,
			Value: []byte("value1"),
		}},
		PrimaryLock:  key,
		StartVersion: 10,
		LockTtl:      3000,
	}
	require.Empty(t, Prewrite(db, latches, put))
	require.Nil(t, Commit(db, latches, &kvrpcpb.CommitRequest{
		Keys:          [][]byte{key},
		StartVersion:  put.StartVersion,
		CommitVersion: 20,
	}))

	lock := &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{{
			Op:  kvrpcpb.Mutation_Lock,
			Key: key,
		}},
		PrimaryLock:  key,
		StartVersion: 30,
		LockTtl:      3000,
	}
	require.Empty(t, Prewrite(db, latches, lock))
	require.Nil(t, Commit(db, latches, &kvrpcpb.CommitRequest{
		Keys:          [][]byte{key},
		StartVersion:  lock.StartVersion,
		CommitVersion: 40,
	}))

	reader := NewReader(db)
	val, _, err := reader.GetValue(key, 50)
	require.NoError(t, err)
	require.Equal(t, []byte("value1"), val)

	exists, err := keyExistsAt(reader, key, 50)
	require.NoError(t, err)
	require.True(t, exists)
}

func TestCommittedLockDoesNotCreateVisibleKey(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(32)
	key := []byte("lock-only")

	lock := &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{{
			Op:  kvrpcpb.Mutation_Lock,
			Key: key,
		}},
		PrimaryLock:  key,
		StartVersion: 10,
		LockTtl:      3000,
	}
	require.Empty(t, Prewrite(db, latches, lock))
	require.Nil(t, Commit(db, latches, &kvrpcpb.CommitRequest{
		Keys:          [][]byte{key},
		StartVersion:  lock.StartVersion,
		CommitVersion: 20,
	}))

	reader := NewReader(db)
	_, _, err := reader.GetValue(key, 30)
	require.ErrorIs(t, err, utils.ErrKeyNotFound)

	exists, err := keyExistsAt(reader, key, 30)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestRollbackMarkerDoesNotHideVisiblePut(t *testing.T) {
	db := openTestDB(t)
	key := []byte("rollback-visible-put")

	applyVersionedEntryForTxnTest(t, db, kv.CFDefault, key, 10, []byte("value1"), 0)
	applyVersionedEntryForTxnTest(t, db, kv.CFWrite, key, 20, mvcc.EncodeWrite(mvcc.Write{
		Kind:    kvrpcpb.Mutation_Put,
		StartTs: 10,
	}), 0)
	applyVersionedEntryForTxnTest(t, db, kv.CFDefault, key, 30, nil, kv.BitDelete)
	applyVersionedEntryForTxnTest(t, db, kv.CFWrite, key, 30, mvcc.EncodeWrite(mvcc.Write{
		Kind:    kvrpcpb.Mutation_Rollback,
		StartTs: 30,
	}), 0)

	reader := NewReader(db)
	val, _, err := reader.GetValue(key, 40)
	require.NoError(t, err)
	require.Equal(t, []byte("value1"), val)

	exists, err := keyExistsAt(reader, key, 40)
	require.NoError(t, err)
	require.True(t, exists)
}

func TestPrewriteAssertionNotExistRejectsVisibleValue(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(32)
	key := []byte("assert-new")

	first := &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{{
			Op:    kvrpcpb.Mutation_Put,
			Key:   key,
			Value: []byte("v1"),
		}},
		PrimaryLock:  key,
		StartVersion: 10,
		LockTtl:      3000,
	}
	require.Empty(t, Prewrite(db, latches, first))
	require.Nil(t, Commit(db, latches, &kvrpcpb.CommitRequest{
		Keys:          [][]byte{key},
		StartVersion:  first.StartVersion,
		CommitVersion: 20,
	}))

	second := &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{{
			Op:                kvrpcpb.Mutation_Put,
			Key:               key,
			Value:             []byte("v2"),
			AssertionNotExist: true,
		}},
		PrimaryLock:  key,
		StartVersion: 30,
		LockTtl:      3000,
	}
	errs := Prewrite(db, latches, second)
	require.Len(t, errs, 1)
	require.NotNil(t, errs[0].GetAlreadyExists())
	require.Equal(t, key, errs[0].GetAlreadyExists().GetKey())
}

func TestPrewriteAssertionNotExistAllowsDeletedValue(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(32)
	key := []byte("assert-after-delete")

	put := &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{{
			Op:    kvrpcpb.Mutation_Put,
			Key:   key,
			Value: []byte("v1"),
		}},
		PrimaryLock:  key,
		StartVersion: 10,
		LockTtl:      3000,
	}
	require.Empty(t, Prewrite(db, latches, put))
	require.Nil(t, Commit(db, latches, &kvrpcpb.CommitRequest{
		Keys:          [][]byte{key},
		StartVersion:  put.StartVersion,
		CommitVersion: 20,
	}))

	del := &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{{
			Op:  kvrpcpb.Mutation_Delete,
			Key: key,
		}},
		PrimaryLock:  key,
		StartVersion: 30,
		LockTtl:      3000,
	}
	require.Empty(t, Prewrite(db, latches, del))
	require.Nil(t, Commit(db, latches, &kvrpcpb.CommitRequest{
		Keys:          [][]byte{key},
		StartVersion:  del.StartVersion,
		CommitVersion: 40,
	}))

	create := &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{{
			Op:                kvrpcpb.Mutation_Put,
			Key:               key,
			Value:             []byte("v2"),
			AssertionNotExist: true,
		}},
		PrimaryLock:  key,
		StartVersion: 50,
		LockTtl:      3000,
	}
	require.Empty(t, Prewrite(db, latches, create))
}

func TestPrewriteConflictingLock(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(32)
	first := &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{{
			Op:    kvrpcpb.Mutation_Put,
			Key:   []byte("conflict"),
			Value: []byte("v1"),
		}},
		PrimaryLock:  []byte("conflict"),
		StartVersion: 5,
		LockTtl:      1000,
	}
	require.Empty(t, Prewrite(db, latches, first))

	second := &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{{
			Op:    kvrpcpb.Mutation_Put,
			Key:   []byte("conflict"),
			Value: []byte("v2"),
		}},
		PrimaryLock:  []byte("conflict"),
		StartVersion: 8,
		LockTtl:      1000,
	}
	errs := Prewrite(db, latches, second)
	require.Len(t, errs, 1)
	require.NotNil(t, errs[0].GetLocked())
}

func TestPrewriteBatchConflictDoesNotApplyPartialLocks(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(32)
	conflictKey := []byte("batch-prewrite-conflict")
	require.Empty(t, Prewrite(db, latches, &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{{
			Op:    kvrpcpb.Mutation_Put,
			Key:   conflictKey,
			Value: []byte("existing"),
		}},
		PrimaryLock:  conflictKey,
		StartVersion: 5,
		LockTtl:      1000,
	}))

	store := newCountingStore(db)
	keys := [][]byte{[]byte("batch-prewrite-a"), []byte("batch-prewrite-b"), conflictKey}
	errs := Prewrite(store, latches, &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Mutation_Put, Key: keys[0], Value: []byte("va")},
			{Op: kvrpcpb.Mutation_Put, Key: keys[1], Value: []byte("vb")},
			{Op: kvrpcpb.Mutation_Put, Key: keys[2], Value: []byte("vc")},
		},
		PrimaryLock:  keys[0],
		StartVersion: 8,
		LockTtl:      1000,
	})
	require.Len(t, errs, 1)
	require.NotNil(t, errs[0].GetLocked())
	require.Zero(t, store.applyCalls)

	reader := NewReader(db)
	for _, key := range keys[:2] {
		lock, err := reader.GetLock(key)
		require.NoError(t, err)
		require.Nil(t, lock, "key=%s", key)
		_, err = db.GetInternalEntry(kv.CFDefault, key, 8)
		require.ErrorIs(t, err, utils.ErrKeyNotFound, "key=%s", key)
	}
}

func TestPrewriteBatchAppliesAllMutationsOnce(t *testing.T) {
	db := openTestDB(t)
	store := newCountingStore(db)
	latches := latch.NewManager(32)
	keys := [][]byte{[]byte("batch-prewrite-ok-a"), []byte("batch-prewrite-ok-b"), []byte("batch-prewrite-ok-c")}

	require.Empty(t, Prewrite(store, latches, &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Mutation_Put, Key: keys[0], Value: []byte("va")},
			{Op: kvrpcpb.Mutation_Put, Key: keys[1], Value: []byte("vb")},
			{Op: kvrpcpb.Mutation_Put, Key: keys[2], Value: []byte("vc")},
		},
		PrimaryLock:  keys[0],
		StartVersion: 8,
		LockTtl:      1000,
	}))
	require.Equal(t, 1, store.applyCalls)
	require.Equal(t, []int{3}, store.appliedEntryCounts)

	reader := NewReader(db)
	for _, key := range keys {
		lock, err := reader.GetLock(key)
		require.NoError(t, err)
		require.NotNil(t, lock, "key=%s", key)
		require.Equal(t, uint64(8), lock.Ts)
	}
}

func TestPrewriteRequestBatchFusesIndependentRequests(t *testing.T) {
	db := openTestDB(t)
	store := newCountingStore(db)
	latches := latch.NewManager(32)
	before := Stats()

	results := PrewriteBatch(store, latches, []*kvrpcpb.PrewriteRequest{
		{
			Mutations:    []*kvrpcpb.Mutation{{Op: kvrpcpb.Mutation_Put, Key: []byte("prewrite-fused-a"), Value: []byte("va")}},
			PrimaryLock:  []byte("prewrite-fused-a"),
			StartVersion: 20,
			LockTtl:      1000,
		},
		{
			Mutations:    []*kvrpcpb.Mutation{{Op: kvrpcpb.Mutation_Put, Key: []byte("prewrite-fused-b"), Value: []byte("vb")}},
			PrimaryLock:  []byte("prewrite-fused-b"),
			StartVersion: 22,
			LockTtl:      1000,
		},
	})
	require.Len(t, results, 2)
	require.Empty(t, results[0])
	require.Empty(t, results[1])
	require.Equal(t, 1, store.applyCalls)
	require.Equal(t, []int{2}, store.appliedEntryCounts)

	after := Stats()
	require.Equal(t, atomicMutateStat(t, before, "two_pc_fused_apply_batches_total")+1, atomicMutateStat(t, after, "two_pc_fused_apply_batches_total"))
	require.Equal(t, atomicMutateStat(t, before, "two_pc_fused_apply_requests_total")+2, atomicMutateStat(t, after, "two_pc_fused_apply_requests_total"))
	require.Equal(t, atomicMutateStat(t, before, "two_pc_fused_apply_entries_total")+2, atomicMutateStat(t, after, "two_pc_fused_apply_entries_total"))
}

func TestPrewriteRequestBatchPreservesOverlappingOrder(t *testing.T) {
	db := openTestDB(t)
	store := newCountingStore(db)
	latches := latch.NewManager(32)
	key := []byte("prewrite-overlap")

	results := PrewriteBatch(store, latches, []*kvrpcpb.PrewriteRequest{
		{
			Mutations:    []*kvrpcpb.Mutation{{Op: kvrpcpb.Mutation_Put, Key: key, Value: []byte("first")}},
			PrimaryLock:  key,
			StartVersion: 30,
			LockTtl:      1000,
		},
		{
			Mutations:    []*kvrpcpb.Mutation{{Op: kvrpcpb.Mutation_Put, Key: key, Value: []byte("second")}},
			PrimaryLock:  key,
			StartVersion: 32,
			LockTtl:      1000,
		},
	})
	require.Len(t, results, 2)
	require.Empty(t, results[0])
	require.Len(t, results[1], 1)
	require.NotNil(t, results[1][0].GetLocked())
	require.Equal(t, 1, store.applyCalls)

	lock, err := NewReader(db).GetLock(key)
	require.NoError(t, err)
	require.NotNil(t, lock)
	require.Equal(t, uint64(30), lock.Ts)
}

func TestPrewriteRequestBatchApplyFailureMarksFusedRequests(t *testing.T) {
	db := openTestDB(t)
	store := newCountingStore(db)
	injected := errors.New("fused prewrite apply failed")
	store.failNextApply(injected)
	latches := latch.NewManager(32)

	results := PrewriteBatch(store, latches, []*kvrpcpb.PrewriteRequest{
		{
			Mutations:    []*kvrpcpb.Mutation{{Op: kvrpcpb.Mutation_Put, Key: []byte("prewrite-fail-a"), Value: []byte("va")}},
			PrimaryLock:  []byte("prewrite-fail-a"),
			StartVersion: 40,
			LockTtl:      1000,
		},
		{
			Mutations:    []*kvrpcpb.Mutation{{Op: kvrpcpb.Mutation_Put, Key: []byte("prewrite-fail-b"), Value: []byte("vb")}},
			PrimaryLock:  []byte("prewrite-fail-b"),
			StartVersion: 42,
			LockTtl:      1000,
		},
	})
	require.Len(t, results, 2)
	for _, errs := range results {
		require.Len(t, errs, 1)
		require.Contains(t, errs[0].GetRetryable(), injected.Error())
	}
	require.Equal(t, 1, store.applyCalls)

	reader := NewReader(db)
	for _, key := range [][]byte{[]byte("prewrite-fail-a"), []byte("prewrite-fail-b")} {
		lock, err := reader.GetLock(key)
		require.NoError(t, err)
		require.Nil(t, lock)
	}
}

func TestCommitRequestBatchFusesIndependentRequests(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(32)
	keys := [][]byte{[]byte("commit-fused-a"), []byte("commit-fused-b")}
	require.Empty(t, Prewrite(db, latches, &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Mutation_Put, Key: keys[0], Value: []byte("va")},
			{Op: kvrpcpb.Mutation_Put, Key: keys[1], Value: []byte("vb")},
		},
		PrimaryLock:  keys[0],
		StartVersion: 50,
		LockTtl:      1000,
	}))

	store := newCountingStore(db)
	results := CommitBatch(store, latches, []*kvrpcpb.CommitRequest{
		{Keys: [][]byte{keys[0]}, StartVersion: 50, CommitVersion: 60},
		{Keys: [][]byte{keys[1]}, StartVersion: 50, CommitVersion: 60},
	})
	require.Len(t, results, 2)
	require.Nil(t, results[0])
	require.Nil(t, results[1])
	require.Equal(t, 1, store.applyCalls)
	require.Equal(t, []int{4}, store.appliedEntryCounts)
}

func TestBatchRollbackRequestBatchFusesIndependentRequests(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(32)
	keys := [][]byte{[]byte("rollback-fused-a"), []byte("rollback-fused-b")}
	require.Empty(t, Prewrite(db, latches, &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Mutation_Put, Key: keys[0], Value: []byte("va")},
			{Op: kvrpcpb.Mutation_Put, Key: keys[1], Value: []byte("vb")},
		},
		PrimaryLock:  keys[0],
		StartVersion: 70,
		LockTtl:      1000,
	}))

	store := newCountingStore(db)
	results := BatchRollbackBatch(store, latches, []*kvrpcpb.BatchRollbackRequest{
		{Keys: [][]byte{keys[0]}, StartVersion: 70},
		{Keys: [][]byte{keys[1]}, StartVersion: 70},
	})
	require.Len(t, results, 2)
	require.Nil(t, results[0])
	require.Nil(t, results[1])
	require.Equal(t, 1, store.applyCalls)
	require.Equal(t, []int{4}, store.appliedEntryCounts)
}

func TestResolveLockRequestBatchFusesIndependentRequests(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(32)
	keys := [][]byte{[]byte("resolve-fused-a"), []byte("resolve-fused-b")}
	require.Empty(t, Prewrite(db, latches, &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Mutation_Put, Key: keys[0], Value: []byte("va")},
			{Op: kvrpcpb.Mutation_Put, Key: keys[1], Value: []byte("vb")},
		},
		PrimaryLock:  keys[0],
		StartVersion: 80,
		LockTtl:      1000,
	}))

	store := newCountingStore(db)
	results := ResolveLockBatch(store, latches, []*kvrpcpb.ResolveLockRequest{
		{Keys: [][]byte{keys[0]}, StartVersion: 80, CommitVersion: 90},
		{Keys: [][]byte{keys[1]}, StartVersion: 80, CommitVersion: 90},
	})
	require.Len(t, results, 2)
	for _, result := range results {
		require.Nil(t, result.Error)
		require.Equal(t, uint64(1), result.ResolvedLocks)
	}
	require.Equal(t, 1, store.applyCalls)
	require.Equal(t, []int{4}, store.appliedEntryCounts)
}

func TestCommitMissingLock(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(32)
	commit := &kvrpcpb.CommitRequest{
		Keys:          [][]byte{[]byte("missing")},
		StartVersion:  3,
		CommitVersion: 6,
	}
	err := Commit(db, latches, commit)
	require.NotNil(t, err)
	require.Contains(t, err.GetRetryable(), "lock not found")
}

// TestCommitNilRequestReturnsNil verifies Commit ignores nil requests.
func TestCommitNilRequestReturnsNil(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(32)
	require.Nil(t, Commit(db, latches, nil))
}

// TestCommitRejectsEmptyKey verifies Commit rejects empty keys.
func TestCommitRejectsEmptyKey(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(32)

	err := Commit(db, latches, &kvrpcpb.CommitRequest{
		Keys:          [][]byte{nil},
		StartVersion:  3,
		CommitVersion: 6,
	})
	require.NotNil(t, err)
	require.Contains(t, err.GetAbort(), "empty key in commit")
}

// TestCommitRejectsCommitVersionNotAfterStartVersion preserves MVCC ordering.
func TestCommitRejectsCommitVersionNotAfterStartVersion(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(32)
	key := []byte("mvcc-order")
	startTs := uint64(20)
	readTs := uint64(15)

	prewrite := &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{{
			Op:    kvrpcpb.Mutation_Put,
			Key:   key,
			Value: []byte("value"),
		}},
		PrimaryLock:  key,
		StartVersion: startTs,
		LockTtl:      3000,
	}
	require.Empty(t, Prewrite(db, latches, prewrite))

	for _, commitTs := range []uint64{10, startTs} {
		keyErr := Commit(db, latches, &kvrpcpb.CommitRequest{
			Keys:          [][]byte{key},
			StartVersion:  startTs,
			CommitVersion: commitTs,
		})
		if keyErr == nil {
			t.Errorf("Commit accepted commitVersion=%d with startVersion=%d", commitTs, startTs)
			continue
		}
		require.Contains(t, keyErr.GetAbort(), "greater than start version")
	}

	reader := NewReader(db)
	val, _, err := reader.GetValue(key, readTs)
	if !errors.Is(err, utils.ErrKeyNotFound) {
		t.Fatalf("read at ts=%d unexpectedly observed value %q, err=%v", readTs, val, err)
	}
}

// TestCommitMissingLockWithRollbackWriteRequiresFreshTxn rejects this start_ts
// while telling higher layers that re-running with a fresh transaction is safe.
func TestCommitMissingLockWithRollbackWriteRequiresFreshTxn(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(32)
	key := []byte("rolled-back")
	startTs := uint64(18)

	applyVersionedEntryForTxnTest(t, db, kv.CFWrite, key, startTs, mvcc.EncodeWrite(mvcc.Write{
		Kind:    kvrpcpb.Mutation_Rollback,
		StartTs: startTs,
	}), 0)

	err := Commit(db, latches, &kvrpcpb.CommitRequest{
		Keys:          [][]byte{key},
		StartVersion:  startTs,
		CommitVersion: 30,
	})
	require.NotNil(t, err)
	require.Contains(t, err.GetRetryable(), "transaction already rolled back")
}

// TestCommitReturnsRetryableOnLockLookupError surfaces lock read failures.
func TestCommitReturnsRetryableOnLockLookupError(t *testing.T) {
	latches := latch.NewManager(32)
	store := rollbackTestStore{
		getInternalEntry: func(cf kv.ColumnFamily, key []byte, version uint64) (*kv.Entry, error) {
			return nil, errors.New("lock lookup failed")
		},
	}

	err := Commit(store, latches, &kvrpcpb.CommitRequest{
		Keys:          [][]byte{[]byte("retry-lock")},
		StartVersion:  9,
		CommitVersion: 18,
	})
	require.NotNil(t, err)
	require.Contains(t, err.GetRetryable(), "lock lookup failed")
}

// TestCommitReturnsRetryableOnWriteLookupErrorWhenLockMissing surfaces write scan failures.
func TestCommitReturnsRetryableOnWriteLookupErrorWhenLockMissing(t *testing.T) {
	badEntry := kv.NewEntry([]byte("bad-key"), nil)
	t.Cleanup(badEntry.DecrRef)

	latches := latch.NewManager(32)
	store := rollbackTestStore{
		newInternalIterator: func(opt *kv.Options) kv.Iterator {
			return &testIterator{items: []kv.Item{badEntry}}
		},
	}

	err := Commit(store, latches, &kvrpcpb.CommitRequest{
		Keys:          [][]byte{[]byte("retry-write")},
		StartVersion:  9,
		CommitVersion: 18,
	})
	require.NotNil(t, err)
	require.Contains(t, err.GetRetryable(), "scanWrites expects internal key")
}

// TestCommitMissingLockWithExistingWriteIsIdempotent preserves idempotent commits.
func TestCommitMissingLockWithExistingWriteIsIdempotent(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(32)
	key := []byte("already-committed")
	startTs := uint64(12)
	commitTs := uint64(22)

	applyVersionedEntryForTxnTest(t, db, kv.CFWrite, key, commitTs, mvcc.EncodeWrite(mvcc.Write{
		Kind:    kvrpcpb.Mutation_Put,
		StartTs: startTs,
	}), 0)

	err := Commit(db, latches, &kvrpcpb.CommitRequest{
		Keys:          [][]byte{key},
		StartVersion:  startTs,
		CommitVersion: commitTs,
	})
	require.Nil(t, err)
}

func TestCommitRemovesAllLocks(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(16)
	keys := [][]byte{[]byte("commit-a"), []byte("commit-b"), []byte("commit-c")}
	require.Empty(t, Prewrite(db, latches, &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Mutation_Put, Key: keys[0], Value: []byte("va")},
			{Op: kvrpcpb.Mutation_Delete, Key: keys[1]},
			{Op: kvrpcpb.Mutation_Put, Key: keys[2], Value: []byte("vc")},
		},
		PrimaryLock:  keys[0],
		StartVersion: 18,
		LockTtl:      1000,
	}))
	require.Nil(t, Commit(db, latches, &kvrpcpb.CommitRequest{
		Keys:          keys,
		StartVersion:  18,
		CommitVersion: 25,
	}))

	reader := NewReader(db)
	for _, key := range keys {
		lock, err := reader.GetLock(key)
		require.NoError(t, err)
		require.Nil(t, lock, "key=%s", key)
		write, commitTs, err := reader.GetWriteByStartTs(key, 18)
		require.NoError(t, err)
		require.NotNil(t, write, "key=%s", key)
		require.Equal(t, uint64(25), commitTs, "key=%s", key)
	}
}

func TestCommitBatchAppliesAllKeysOnce(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(16)
	keys := [][]byte{[]byte("commit-once-a"), []byte("commit-once-b"), []byte("commit-once-c")}
	require.Empty(t, Prewrite(db, latches, &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Mutation_Put, Key: keys[0], Value: []byte("va")},
			{Op: kvrpcpb.Mutation_Put, Key: keys[1], Value: []byte("vb")},
			{Op: kvrpcpb.Mutation_Put, Key: keys[2], Value: []byte("vc")},
		},
		PrimaryLock:  keys[0],
		StartVersion: 18,
		LockTtl:      1000,
	}))

	store := newCountingStore(db)
	require.Nil(t, Commit(store, latches, &kvrpcpb.CommitRequest{
		Keys:          keys,
		StartVersion:  18,
		CommitVersion: 25,
	}))
	require.Equal(t, 1, store.applyCalls)
	require.Equal(t, []int{6}, store.appliedEntryCounts)
}

func TestCommitAfterBatchPrewritePreservesShardAffinityAcrossRestart(t *testing.T) {
	const shardCount = 4

	dir := filepath.Join(t.TempDir(), "db")
	opt := testOptionsForDir(dir)
	opt.WriteShardCount = shardCount
	db, err := local.Open(opt)
	require.NoError(t, err)

	first, second := keysWithAscendingCommitShards(t, shardCount)
	latches := latch.NewManager(32)
	errs := Prewrite(db, latches, &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Mutation_Put, Key: first, Value: []byte("first")},
			{Op: kvrpcpb.Mutation_Put, Key: second, Value: []byte("second")},
		},
		PrimaryLock:  first,
		StartVersion: 10,
		LockTtl:      3000,
	})
	require.Empty(t, errs)
	require.Nil(t, Commit(db, latches, &kvrpcpb.CommitRequest{
		Keys:          [][]byte{second},
		StartVersion:  10,
		CommitVersion: 20,
	}))
	require.NoError(t, db.Close())

	db, err = local.Open(opt)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// This reproduces the sharded-apply failure mode behind the review comment:
	// if a batch prewrite routes every key by the first key's shard, then a
	// later single-key commit can put the second key's lock tombstone on its
	// real shard while the stale lock remains on the first shard. After restart
	// shard hints are cold, so equal-version lock records are resolved by shard
	// scan order and the stale lock can become visible again.
	lock, err := NewReader(db).GetLock(second)
	require.NoError(t, err)
	require.Nil(t, lock)
}

func keysWithAscendingCommitShards(t *testing.T, shardCount int) ([]byte, []byte) {
	t.Helper()
	keysByShard := make([][]byte, shardCount)
	for i := range 10000 {
		key := fmt.Appendf(nil, "affinity-%d", i)
		shardID := testShardForInternalKey(kv.InternalKey(kv.CFLock, key, lockColumnTs), shardCount)
		if shardID >= 0 && shardID < shardCount && keysByShard[shardID] == nil {
			keysByShard[shardID] = key
		}
	}
	for low := range shardCount {
		for high := low + 1; high < shardCount; high++ {
			if keysByShard[low] != nil && keysByShard[high] != nil {
				return keysByShard[low], keysByShard[high]
			}
		}
	}
	t.Fatalf("could not find keys on ascending shards for shardCount=%d", shardCount)
	return nil, nil
}

func testShardForInternalKey(internalKey []byte, shardCount int) int {
	if shardCount <= 1 {
		return 0
	}
	_, userKey, _, ok := kv.SplitInternalKey(internalKey)
	if !ok || len(userKey) == 0 {
		return 0
	}
	return utils.ShardForUserKey(userKey, shardCount)
}

func TestCommitBatchCommitTsExpiredDoesNotApplyPartialCommits(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(16)
	keys := [][]byte{[]byte("commit-expired-a"), []byte("commit-expired-b")}
	require.Empty(t, Prewrite(db, latches, &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Mutation_Put, Key: keys[0], Value: []byte("va")},
			{Op: kvrpcpb.Mutation_Put, Key: keys[1], Value: []byte("vb")},
		},
		PrimaryLock:  keys[0],
		StartVersion: 18,
		LockTtl:      1000,
		MinCommitTs:  30,
	}))

	store := newCountingStore(db)
	err := Commit(store, latches, &kvrpcpb.CommitRequest{
		Keys:          keys,
		StartVersion:  18,
		CommitVersion: 25,
	})
	require.NotNil(t, err)
	require.NotNil(t, err.GetCommitTsExpired())
	require.Zero(t, store.applyCalls)

	reader := NewReader(db)
	for _, key := range keys {
		write, _, readErr := reader.GetWriteByStartTs(key, 18)
		require.NoError(t, readErr)
		require.Nil(t, write, "key=%s", key)
		lock, readErr := reader.GetLock(key)
		require.NoError(t, readErr)
		require.NotNil(t, lock, "key=%s", key)
	}
}

// TestCommitReturnsLockedOnDifferentTransactionLock rejects foreign locks.
func TestCommitReturnsLockedOnDifferentTransactionLock(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(32)
	key := []byte("locked-by-other")

	applyVersionedEntryForTxnTest(t, db, kv.CFLock, key, lockColumnTs, mvcc.EncodeLock(mvcc.Lock{
		Primary: key,
		Ts:      30,
		Kind:    kvrpcpb.Mutation_Put,
	}), 0)

	err := Commit(db, latches, &kvrpcpb.CommitRequest{
		Keys:          [][]byte{key},
		StartVersion:  20,
		CommitVersion: 40,
	})
	require.NotNil(t, err)
	require.NotNil(t, err.GetLocked())
	require.Equal(t, uint64(30), err.GetLocked().GetLockVersion())
}

func TestReaderMostRecentWriteSkipsOtherCF(t *testing.T) {
	db := openTestDB(t)
	require.NoError(t, db.Set([]byte("b"), []byte("vb")))

	entry := mvcc.EncodeWrite(mvcc.Write{Kind: kvrpcpb.Mutation_Put, StartTs: 1})
	applyVersionedEntryForTxnTest(t, db, kv.CFWrite, []byte("a"), 10, entry, 0)

	reader := NewReader(db)
	write, commitTs, err := reader.MostRecentWrite([]byte("a"))
	require.NoError(t, err)
	require.NotNil(t, write)
	require.Equal(t, uint64(10), commitTs)
	require.Equal(t, uint64(1), write.StartTs)
}

func TestBatchRollbackRemovesLock(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(16)
	req := &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{{
			Op:    kvrpcpb.Mutation_Put,
			Key:   []byte("rk"),
			Value: []byte("v"),
		}},
		PrimaryLock:  []byte("rk"),
		StartVersion: 8,
		LockTtl:      1000,
	}
	require.Empty(t, Prewrite(db, latches, req))

	rollback := &kvrpcpb.BatchRollbackRequest{Keys: [][]byte{[]byte("rk")}, StartVersion: 8}
	require.Nil(t, BatchRollback(db, latches, rollback))

	reader := NewReader(db)
	lock, err := reader.GetLock([]byte("rk"))
	require.NoError(t, err)
	require.Nil(t, lock)
	write, commitTs, err := reader.GetWriteByStartTs([]byte("rk"), 8)
	require.NoError(t, err)
	require.NotNil(t, write)
	require.Equal(t, kvrpcpb.Mutation_Rollback, write.Kind)
	require.Equal(t, uint64(8), commitTs)
}

func TestBatchRollbackRemovesAllLocks(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(16)
	keys := [][]byte{[]byte("rk-a"), []byte("rk-b"), []byte("rk-c")}
	req := &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Mutation_Put, Key: keys[0], Value: []byte("va")},
			{Op: kvrpcpb.Mutation_Delete, Key: keys[1]},
			{Op: kvrpcpb.Mutation_Put, Key: keys[2], Value: []byte("vc")},
		},
		PrimaryLock:  keys[0],
		StartVersion: 18,
		LockTtl:      1000,
	}
	require.Empty(t, Prewrite(db, latches, req))
	require.Nil(t, BatchRollback(db, latches, &kvrpcpb.BatchRollbackRequest{
		Keys:         keys,
		StartVersion: 18,
	}))

	reader := NewReader(db)
	for _, key := range keys {
		lock, err := reader.GetLock(key)
		require.NoError(t, err)
		require.Nil(t, lock, "key=%s", key)
		write, commitTs, err := reader.GetWriteByStartTs(key, 18)
		require.NoError(t, err)
		require.NotNil(t, write, "key=%s", key)
		require.Equal(t, kvrpcpb.Mutation_Rollback, write.Kind, "key=%s", key)
		require.Equal(t, uint64(18), commitTs, "key=%s", key)
	}
}

func TestBatchRollbackAppliesAllKeysOnce(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(16)
	keys := [][]byte{[]byte("rollback-once-a"), []byte("rollback-once-b"), []byte("rollback-once-c")}
	require.Empty(t, Prewrite(db, latches, &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Mutation_Put, Key: keys[0], Value: []byte("va")},
			{Op: kvrpcpb.Mutation_Delete, Key: keys[1]},
			{Op: kvrpcpb.Mutation_Put, Key: keys[2], Value: []byte("vc")},
		},
		PrimaryLock:  keys[0],
		StartVersion: 18,
		LockTtl:      1000,
	}))

	store := newCountingStore(db)
	require.Nil(t, BatchRollback(store, latches, &kvrpcpb.BatchRollbackRequest{
		Keys:         keys,
		StartVersion: 18,
	}))
	require.Equal(t, 1, store.applyCalls)
	require.Equal(t, []int{7}, store.appliedEntryCounts)
}

func TestBatchRollbackDoesNotDeleteOtherTransactionLock(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(16)
	key := []byte("rk-other-lock")

	prewrite := &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{{
			Op:    kvrpcpb.Mutation_Put,
			Key:   key,
			Value: []byte("value"),
		}},
		PrimaryLock:  key,
		StartVersion: 20,
		LockTtl:      1000,
	}
	require.Empty(t, Prewrite(db, latches, prewrite))

	reader := NewReader(db)
	lock, err := reader.GetLock(key)
	require.NoError(t, err)
	require.NotNil(t, lock)
	require.Equal(t, uint64(20), lock.Ts)

	rollback := &kvrpcpb.BatchRollbackRequest{Keys: [][]byte{key}, StartVersion: 10}
	require.Nil(t, BatchRollback(db, latches, rollback))

	lock, err = reader.GetLock(key)
	require.NoError(t, err)
	require.NotNil(t, lock)
	require.Equal(t, uint64(20), lock.Ts)

	write, commitTs, err := reader.GetWriteByStartTs(key, 10)
	require.NoError(t, err)
	require.NotNil(t, write)
	require.Equal(t, kvrpcpb.Mutation_Rollback, write.Kind)
	require.Equal(t, uint64(10), commitTs)

	commit := &kvrpcpb.CommitRequest{
		Keys:          [][]byte{key},
		StartVersion:  20,
		CommitVersion: 30,
	}
	require.Nil(t, Commit(db, latches, commit))

	value, _, err := reader.GetValue(key, 40)
	require.NoError(t, err)
	require.Equal(t, []byte("value"), value)
}

func TestLateBatchRollbackAfterCommitPreservesCommittedWrite(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(16)
	key := []byte("late-rollback")
	startTs := uint64(20)
	commitTs := uint64(30)

	require.Empty(t, Prewrite(db, latches, &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{{
			Op:    kvrpcpb.Mutation_Put,
			Key:   key,
			Value: []byte("value"),
		}},
		PrimaryLock:  key,
		StartVersion: startTs,
		LockTtl:      1000,
	}))
	require.Nil(t, Commit(db, latches, &kvrpcpb.CommitRequest{
		Keys:          [][]byte{key},
		StartVersion:  startTs,
		CommitVersion: commitTs,
	}))
	require.Nil(t, BatchRollback(db, latches, &kvrpcpb.BatchRollbackRequest{
		Keys:         [][]byte{key},
		StartVersion: startTs,
	}))

	reader := NewReader(db)
	write, gotCommitTs, err := reader.GetWriteByStartTs(key, startTs)
	require.NoError(t, err)
	require.NotNil(t, write)
	require.Equal(t, kvrpcpb.Mutation_Put, write.Kind)
	require.Equal(t, commitTs, gotCommitTs)
	value, _, err := reader.GetValue(key, commitTs+1)
	require.NoError(t, err)
	require.Equal(t, []byte("value"), value)
}

func TestResolveLockCommit(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(16)
	pre := &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{{
			Op:    kvrpcpb.Mutation_Put,
			Key:   []byte("res"),
			Value: []byte("val"),
		}},
		PrimaryLock:  []byte("res"),
		StartVersion: 40,
		LockTtl:      1000,
	}
	require.Empty(t, Prewrite(db, latches, pre))
	count, keyErr := ResolveLock(db, latches, &kvrpcpb.ResolveLockRequest{
		Keys:          [][]byte{[]byte("res")},
		StartVersion:  40,
		CommitVersion: 50,
	})
	require.Nil(t, keyErr)
	require.Equal(t, uint64(1), count)
	reader := NewReader(db)
	val, _, err := reader.GetValue([]byte("res"), 60)
	require.NoError(t, err)
	require.Equal(t, []byte("val"), val)
}

func TestResolveLockCommitAppliesAllLocksOnce(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(16)
	keys := [][]byte{[]byte("resolve-once-a"), []byte("resolve-once-b")}
	require.Empty(t, Prewrite(db, latches, &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Mutation_Put, Key: keys[0], Value: []byte("va")},
			{Op: kvrpcpb.Mutation_Put, Key: keys[1], Value: []byte("vb")},
		},
		PrimaryLock:  keys[0],
		StartVersion: 40,
		LockTtl:      1000,
	}))

	store := newCountingStore(db)
	count, keyErr := ResolveLock(store, latches, &kvrpcpb.ResolveLockRequest{
		Keys:          [][]byte{keys[0], keys[1], keys[1]},
		StartVersion:  40,
		CommitVersion: 50,
	})
	require.Nil(t, keyErr)
	require.Equal(t, uint64(2), count)
	require.Equal(t, 1, store.applyCalls)
	require.Equal(t, []int{4}, store.appliedEntryCounts)
}

func TestResolveLockCommitsSecondaryAfterPrimaryCommitCrashWindow(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(16)
	primary := []byte("crash-primary")
	secondary := []byte("crash-secondary")
	startTs := uint64(40)
	commitTs := uint64(50)

	require.Empty(t, Prewrite(db, latches, &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Mutation_Put, Key: primary, Value: []byte("primary-value")},
			{Op: kvrpcpb.Mutation_Put, Key: secondary, Value: []byte("secondary-value")},
		},
		PrimaryLock:  primary,
		StartVersion: startTs,
		LockTtl:      1000,
	}))
	require.Nil(t, Commit(db, latches, &kvrpcpb.CommitRequest{
		Keys:          [][]byte{primary},
		StartVersion:  startTs,
		CommitVersion: commitTs,
	}))

	count, keyErr := ResolveLock(db, latches, &kvrpcpb.ResolveLockRequest{
		Keys:          [][]byte{secondary},
		StartVersion:  startTs,
		CommitVersion: commitTs,
	})
	require.Nil(t, keyErr)
	require.Equal(t, uint64(1), count)

	reader := NewReader(db)
	value, _, err := reader.GetValue(secondary, commitTs+1)
	require.NoError(t, err)
	require.Equal(t, []byte("secondary-value"), value)
	lock, err := reader.GetLock(secondary)
	require.NoError(t, err)
	require.Nil(t, lock)

	count, keyErr = ResolveLock(db, latches, &kvrpcpb.ResolveLockRequest{
		Keys:          [][]byte{secondary},
		StartVersion:  startTs,
		CommitVersion: commitTs,
	})
	require.Nil(t, keyErr)
	require.Zero(t, count)
}

// TestResolveLockNilRequest verifies ResolveLock ignores nil requests.
func TestResolveLockNilRequest(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(16)

	var (
		count  uint64
		keyErr *kvrpcpb.KeyError
	)
	require.NotPanics(t, func() {
		count, keyErr = ResolveLock(db, latches, nil)
	})
	require.Zero(t, count)
	require.Nil(t, keyErr)
}

// TestResolveLockSkipsEmptyAndMismatchedKeys ignores non-matching locks.
func TestResolveLockSkipsEmptyAndMismatchedKeys(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(16)
	key := []byte("other-lock")

	applyVersionedEntryForTxnTest(t, db, kv.CFLock, key, lockColumnTs, mvcc.EncodeLock(mvcc.Lock{
		Primary: key,
		Ts:      55,
		Kind:    kvrpcpb.Mutation_Put,
	}), 0)

	count, keyErr := ResolveLock(db, latches, &kvrpcpb.ResolveLockRequest{
		Keys:          [][]byte{nil, key},
		StartVersion:  40,
		CommitVersion: 60,
	})
	require.Zero(t, count)
	require.Nil(t, keyErr)
}

// TestResolveLockReturnsRetryableOnLockLookupError surfaces lock read failures.
func TestResolveLockReturnsRetryableOnLockLookupError(t *testing.T) {
	latches := latch.NewManager(16)
	store := rollbackTestStore{
		getInternalEntry: func(cf kv.ColumnFamily, key []byte, version uint64) (*kv.Entry, error) {
			return nil, errors.New("lock lookup failed")
		},
	}

	count, keyErr := ResolveLock(store, latches, &kvrpcpb.ResolveLockRequest{
		Keys:          [][]byte{[]byte("retry-lock")},
		StartVersion:  40,
		CommitVersion: 50,
	})
	require.Zero(t, count)
	require.NotNil(t, keyErr)
	require.Contains(t, keyErr.GetRetryable(), "lock lookup failed")
}

// TestResolveLockRollback keeps commitVersion==0 on the rollback path.
func TestResolveLockRollback(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(16)
	key := []byte("resolve-rollback")
	startTs := uint64(40)

	pre := &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{{
			Op:    kvrpcpb.Mutation_Put,
			Key:   key,
			Value: []byte("val"),
		}},
		PrimaryLock:  key,
		StartVersion: startTs,
		LockTtl:      1000,
	}
	require.Empty(t, Prewrite(db, latches, pre))

	count, keyErr := ResolveLock(db, latches, &kvrpcpb.ResolveLockRequest{
		Keys:         [][]byte{key},
		StartVersion: startTs,
	})
	require.Equal(t, uint64(1), count)
	require.Nil(t, keyErr)

	reader := NewReader(db)
	lock, err := reader.GetLock(key)
	require.NoError(t, err)
	require.Nil(t, lock)

	write, commitTs, err := reader.GetWriteByStartTs(key, startTs)
	require.NoError(t, err)
	require.NotNil(t, write)
	require.Equal(t, kvrpcpb.Mutation_Rollback, write.Kind)
	require.Equal(t, startTs, commitTs)
}

// TestResolveLockRollbackReturnsError propagates rollback apply failures.
func TestResolveLockRollbackReturnsError(t *testing.T) {
	key := []byte("resolve-rollback-error")
	startTs := uint64(40)
	latches := latch.NewManager(16)
	store := rollbackTestStore{
		getInternalEntry: func(cf kv.ColumnFamily, gotKey []byte, version uint64) (*kv.Entry, error) {
			if cf == kv.CFLock && string(gotKey) == string(key) && version == lockColumnTs {
				entry := kv.NewInternalEntry(cf, gotKey, version, mvcc.EncodeLock(mvcc.Lock{
					Primary: key,
					Ts:      startTs,
					Kind:    kvrpcpb.Mutation_Put,
				}), 0, 0)
				return entry, nil
			}
			return nil, utils.ErrKeyNotFound
		},
		applyInternalEntries: func(entries []*kv.Entry) error {
			return errors.New("apply failed")
		},
	}

	count, keyErr := ResolveLock(store, latches, &kvrpcpb.ResolveLockRequest{
		Keys:         [][]byte{key},
		StartVersion: startTs,
	})
	require.Zero(t, count)
	require.NotNil(t, keyErr)
	require.Contains(t, keyErr.GetRetryable(), "apply failed")
}

// TestResolveLockRejectsCommitVersionNotAfterStartVersion preserves MVCC ordering.
func TestResolveLockRejectsCommitVersionNotAfterStartVersion(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(16)
	key := []byte("res-order")
	startTs := uint64(40)
	readTs := uint64(35)

	pre := &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{{
			Op:    kvrpcpb.Mutation_Put,
			Key:   key,
			Value: []byte("val"),
		}},
		PrimaryLock:  key,
		StartVersion: startTs,
		LockTtl:      1000,
	}
	require.Empty(t, Prewrite(db, latches, pre))

	for _, commitTs := range []uint64{30, startTs} {
		count, keyErr := ResolveLock(db, latches, &kvrpcpb.ResolveLockRequest{
			Keys:          [][]byte{key},
			StartVersion:  startTs,
			CommitVersion: commitTs,
		})
		if keyErr == nil {
			t.Errorf("ResolveLock accepted commitVersion=%d with startVersion=%d", commitTs, startTs)
			continue
		}
		if count != 0 {
			t.Errorf("ResolveLock resolved %d locks with invalid commitVersion=%d", count, commitTs)
		}
		require.Contains(t, keyErr.GetAbort(), "greater than start version")
	}

	reader := NewReader(db)
	val, _, err := reader.GetValue(key, readTs)
	if !errors.Is(err, utils.ErrKeyNotFound) {
		t.Fatalf("read at ts=%d unexpectedly observed value %q, err=%v", readTs, val, err)
	}
}

func TestResolveLockCommitTsExpired(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(16)
	key := []byte("res-expired")
	startTs := uint64(40)
	minCommitTs := uint64(60)
	commitTs := uint64(50)

	pre := &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{{
			Op:    kvrpcpb.Mutation_Put,
			Key:   key,
			Value: []byte("val"),
		}},
		PrimaryLock:  key,
		StartVersion: startTs,
		LockTtl:      1000,
		MinCommitTs:  minCommitTs,
	}
	require.Empty(t, Prewrite(db, latches, pre))

	count, keyErr := ResolveLock(db, latches, &kvrpcpb.ResolveLockRequest{
		Keys:          [][]byte{key},
		StartVersion:  startTs,
		CommitVersion: commitTs,
	})
	require.Equal(t, uint64(0), count)
	require.NotNil(t, keyErr)
	require.NotNil(t, keyErr.GetCommitTsExpired())
	require.Equal(t, commitTs, keyErr.GetCommitTsExpired().GetCommitTs())
	require.Equal(t, minCommitTs, keyErr.GetCommitTsExpired().GetMinCommitTs())

	reader := NewReader(db)
	lock, err := reader.GetLock(key)
	require.NoError(t, err)
	require.NotNil(t, lock)
	write, _, err := reader.GetWriteByStartTs(key, startTs)
	require.NoError(t, err)
	require.Nil(t, write)
}

func TestCheckTxnStatusTTLExpire(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(16)
	startTs := uint64(100)
	pre := &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{{
			Op:    kvrpcpb.Mutation_Put,
			Key:   []byte("primary"),
			Value: []byte("value"),
		}},
		PrimaryLock:  []byte("primary"),
		StartVersion: startTs,
		LockTtl:      5,
	}
	require.Empty(t, Prewrite(db, latches, pre))
	reader := NewReader(db)
	lock, err := reader.GetLock([]byte("primary"))
	require.NoError(t, err)
	require.NotNil(t, lock)
	resp := CheckTxnStatus(db, latches, &kvrpcpb.CheckTxnStatusRequest{
		PrimaryKey:         []byte("primary"),
		LockTs:             startTs,
		CurrentTs:          startTs,
		RollbackIfNotExist: true,
		CurrentTime:        lock.StartTime + lock.TTL,
	})
	require.Equal(t, kvrpcpb.CheckTxnStatusAction_CheckTxnStatusTTLExpireRollback, resp.GetAction())
	lock, err = reader.GetLock([]byte("primary"))
	require.NoError(t, err)
	require.Nil(t, lock)
}

func TestCheckTxnStatusTTLRollbackMakesForegroundCommitAbort(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(16)
	key := []byte("foreground-primary")
	startTs := uint64(100)
	commitTs := uint64(101)
	pre := &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{{
			Op:    kvrpcpb.Mutation_Put,
			Key:   key,
			Value: []byte("value"),
		}},
		PrimaryLock:  key,
		StartVersion: startTs,
		LockTtl:      5,
	}
	require.Empty(t, Prewrite(db, latches, pre))

	reader := NewReader(db)
	lock, err := reader.GetLock(key)
	require.NoError(t, err)
	require.NotNil(t, lock)

	// This models a read-side lock resolver racing with the original writer.
	// Once physical time passes the fixed TTL, CheckTxnStatus is allowed to
	// rollback the primary even if the foreground client is merely delayed in
	// the commit queue. The following commit must then fail, matching the class
	// of fsmeta UpdateInode failures seen under the semantic benchmark.
	status := CheckTxnStatus(db, latches, &kvrpcpb.CheckTxnStatusRequest{
		PrimaryKey:         key,
		LockTs:             startTs,
		CurrentTs:          startTs + 10,
		CallerStartTs:      startTs + 10,
		RollbackIfNotExist: true,
		CurrentTime:        lock.StartTime + lock.TTL,
	})
	require.Nil(t, status.GetError())
	require.Equal(t, kvrpcpb.CheckTxnStatusAction_CheckTxnStatusTTLExpireRollback, status.GetAction())

	commitErr := Commit(db, latches, &kvrpcpb.CommitRequest{
		Keys:          [][]byte{key},
		StartVersion:  startTs,
		CommitVersion: commitTs,
	})
	require.NotNil(t, commitErr)
	require.Contains(t, commitErr.GetRetryable(), "transaction already rolled back")
}

func TestCheckTxnStatusDoesNotExpireFromLogicalTSODistance(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(16)
	key := []byte("logical-live")
	startTs := uint64(100)
	pre := &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{{
			Op:    kvrpcpb.Mutation_Put,
			Key:   key,
			Value: []byte("value"),
		}},
		PrimaryLock:  key,
		StartVersion: startTs,
		LockTtl:      5000,
	}
	require.Empty(t, Prewrite(db, latches, pre))

	reader := NewReader(db)
	lock, err := reader.GetLock(key)
	require.NoError(t, err)
	require.NotNil(t, lock)

	resp := CheckTxnStatus(db, latches, &kvrpcpb.CheckTxnStatusRequest{
		PrimaryKey:         key,
		LockTs:             startTs,
		CurrentTs:          startTs + 1_000_000,
		RollbackIfNotExist: true,
		CurrentTime:        lock.StartTime + lock.TTL - 1,
	})
	require.Nil(t, resp.GetError())
	require.Equal(t, kvrpcpb.CheckTxnStatusAction_CheckTxnStatusNoAction, resp.GetAction())
	require.Equal(t, lock.TTL, resp.GetLockTtl())

	lock, err = reader.GetLock(key)
	require.NoError(t, err)
	require.NotNil(t, lock)
}

func TestCheckTxnStatusExpiresFromPhysicalTimeWithoutLogicalTSODistance(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(16)
	key := []byte("physical-expired")
	startTs := uint64(100)
	pre := &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{{
			Op:    kvrpcpb.Mutation_Put,
			Key:   key,
			Value: []byte("value"),
		}},
		PrimaryLock:  key,
		StartVersion: startTs,
		LockTtl:      5,
	}
	require.Empty(t, Prewrite(db, latches, pre))

	reader := NewReader(db)
	lock, err := reader.GetLock(key)
	require.NoError(t, err)
	require.NotNil(t, lock)

	resp := CheckTxnStatus(db, latches, &kvrpcpb.CheckTxnStatusRequest{
		PrimaryKey:         key,
		LockTs:             startTs,
		CurrentTs:          startTs,
		RollbackIfNotExist: true,
		CurrentTime:        lock.StartTime + lock.TTL,
	})
	require.Nil(t, resp.GetError())
	require.Equal(t, kvrpcpb.CheckTxnStatusAction_CheckTxnStatusTTLExpireRollback, resp.GetAction())
}

func TestTxnHeartBeatExtendsPrimaryLockTTL(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(16)
	key := []byte("hb-primary")
	startTs := uint64(100)
	require.Empty(t, Prewrite(db, latches, &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{{
			Op:    kvrpcpb.Mutation_Put,
			Key:   key,
			Value: []byte("value"),
		}},
		PrimaryLock:  key,
		StartVersion: startTs,
		LockTtl:      100,
	}))

	reader := NewReader(db)
	lock, err := reader.GetLock(key)
	require.NoError(t, err)
	require.NotNil(t, lock)
	originalStart := lock.StartTime

	hb := TxnHeartBeat(db, latches, &kvrpcpb.TxnHeartBeatRequest{
		PrimaryKey:   key,
		StartVersion: startTs,
		TtlExtension: 200,
		CurrentTime:  originalStart + 50,
	})
	require.Nil(t, hb.GetError())
	require.Equal(t, kvrpcpb.TxnHeartBeatAction_TxnHeartBeatTTLExtended, hb.GetAction())
	require.Equal(t, uint64(250), hb.GetLockTtl())
	require.Equal(t, originalStart+250, hb.GetLockExpireTime())

	status := CheckTxnStatus(db, latches, &kvrpcpb.CheckTxnStatusRequest{
		PrimaryKey:         key,
		LockTs:             startTs,
		CurrentTs:          startTs + 1_000_000,
		RollbackIfNotExist: true,
		CurrentTime:        originalStart + 150,
	})
	require.Nil(t, status.GetError())
	require.Equal(t, kvrpcpb.CheckTxnStatusAction_CheckTxnStatusNoAction, status.GetAction())

	lock, err = reader.GetLock(key)
	require.NoError(t, err)
	require.NotNil(t, lock)
	require.Equal(t, uint64(250), lock.TTL)
}

func TestTxnHeartBeatExtensionSurvivesRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	db, err := local.Open(testOptionsForDir(dir))
	require.NoError(t, err)
	latches := latch.NewManager(16)
	key := []byte("hb-restart-primary")
	startTs := uint64(100)
	require.Empty(t, Prewrite(db, latches, &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{{
			Op:    kvrpcpb.Mutation_Put,
			Key:   key,
			Value: []byte("value"),
		}},
		PrimaryLock:  key,
		StartVersion: startTs,
		LockTtl:      100,
	}))

	reader := NewReader(db)
	lock, err := reader.GetLock(key)
	require.NoError(t, err)
	require.NotNil(t, lock)
	originalStart := lock.StartTime

	hb := TxnHeartBeat(db, latches, &kvrpcpb.TxnHeartBeatRequest{
		PrimaryKey:   key,
		StartVersion: startTs,
		TtlExtension: 500,
		CurrentTime:  originalStart + 50,
	})
	require.Nil(t, hb.GetError())
	require.Equal(t, kvrpcpb.TxnHeartBeatAction_TxnHeartBeatTTLExtended, hb.GetAction())
	require.Equal(t, uint64(550), hb.GetLockTtl())
	require.NoError(t, db.Close())

	db, err = local.Open(testOptionsForDir(dir))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	latches = latch.NewManager(16)
	reader = NewReader(db)
	lock, err = reader.GetLock(key)
	require.NoError(t, err)
	require.NotNil(t, lock)
	require.Equal(t, originalStart, lock.StartTime)
	require.Equal(t, uint64(550), lock.TTL)

	status := CheckTxnStatus(db, latches, &kvrpcpb.CheckTxnStatusRequest{
		PrimaryKey:         key,
		LockTs:             startTs,
		CurrentTs:          startTs + 1_000_000,
		RollbackIfNotExist: true,
		CurrentTime:        originalStart + 500,
	})
	require.Nil(t, status.GetError())
	require.Equal(t, kvrpcpb.CheckTxnStatusAction_CheckTxnStatusNoAction, status.GetAction())
}

func TestTxnHeartBeatDoesNotResurrectExpiredPrimary(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(16)
	key := []byte("hb-expired")
	startTs := uint64(100)
	require.Empty(t, Prewrite(db, latches, &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{{
			Op:    kvrpcpb.Mutation_Put,
			Key:   key,
			Value: []byte("value"),
		}},
		PrimaryLock:  key,
		StartVersion: startTs,
		LockTtl:      5,
	}))

	reader := NewReader(db)
	lock, err := reader.GetLock(key)
	require.NoError(t, err)
	require.NotNil(t, lock)

	hb := TxnHeartBeat(db, latches, &kvrpcpb.TxnHeartBeatRequest{
		PrimaryKey:   key,
		StartVersion: startTs,
		TtlExtension: 100,
		CurrentTime:  lock.StartTime + lock.TTL,
	})
	require.Nil(t, hb.GetError())
	require.Equal(t, kvrpcpb.TxnHeartBeatAction_TxnHeartBeatTTLExpireRollback, hb.GetAction())

	lock, err = reader.GetLock(key)
	require.NoError(t, err)
	require.Nil(t, lock)
	write, _, err := reader.GetWriteByStartTs(key, startTs)
	require.NoError(t, err)
	require.NotNil(t, write)
	require.Equal(t, kvrpcpb.Mutation_Rollback, write.Kind)
}

func TestTxnHeartBeatReportsCommittedPrimary(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(16)
	key := []byte("hb-committed")
	startTs := uint64(100)
	commitTs := uint64(120)
	require.Empty(t, Prewrite(db, latches, &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{{
			Op:    kvrpcpb.Mutation_Put,
			Key:   key,
			Value: []byte("value"),
		}},
		PrimaryLock:  key,
		StartVersion: startTs,
		LockTtl:      100,
	}))
	require.Nil(t, Commit(db, latches, &kvrpcpb.CommitRequest{
		Keys:          [][]byte{key},
		StartVersion:  startTs,
		CommitVersion: commitTs,
	}))

	hb := TxnHeartBeat(db, latches, &kvrpcpb.TxnHeartBeatRequest{
		PrimaryKey:   key,
		StartVersion: startTs,
		TtlExtension: 100,
		CurrentTime:  1,
	})
	require.Nil(t, hb.GetError())
	require.Equal(t, commitTs, hb.GetCommitVersion())
}

func TestTxnHeartBeatFencesMissingPrimaryWithRollback(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(16)
	key := []byte("hb-missing")
	startTs := uint64(100)

	hb := TxnHeartBeat(db, latches, &kvrpcpb.TxnHeartBeatRequest{
		PrimaryKey:   key,
		StartVersion: startTs,
		TtlExtension: 100,
		CurrentTime:  1,
	})
	require.Nil(t, hb.GetError())
	require.Equal(t, kvrpcpb.TxnHeartBeatAction_TxnHeartBeatLockNotExistRollback, hb.GetAction())

	reader := NewReader(db)
	write, _, err := reader.GetWriteByStartTs(key, startTs)
	require.NoError(t, err)
	require.NotNil(t, write)
	require.Equal(t, kvrpcpb.Mutation_Rollback, write.Kind)
}

func TestReaderGetValueFromShortValueWithExpiresAt(t *testing.T) {
	db := openTestDB(t)
	reader := NewReader(db)
	key := []byte("short-value")
	write := mvcc.Write{
		Kind:       kvrpcpb.Mutation_Put,
		StartTs:    11,
		ShortValue: []byte("v-short"),
		ExpiresAt:  ^uint64(0),
	}
	applyVersionedEntryForTxnTest(t, db, kv.CFWrite, key, 20, mvcc.EncodeWrite(write), 0)

	val, expiresAt, err := reader.GetValue(key, 30)
	require.NoError(t, err)
	require.Equal(t, []byte("v-short"), val)
	require.Equal(t, write.ExpiresAt, expiresAt)
}

func TestReaderGetValueFromExpiredShortValue(t *testing.T) {
	db := openTestDB(t)
	reader := NewReader(db)
	key := []byte("short-expired")
	write := mvcc.Write{
		Kind:       kvrpcpb.Mutation_Put,
		StartTs:    11,
		ShortValue: []byte("v-short"),
		ExpiresAt:  1, // definitely expired
	}
	applyVersionedEntryForTxnTest(t, db, kv.CFWrite, key, 20, mvcc.EncodeWrite(write), 0)

	_, _, err := reader.GetValue(key, 30)
	require.ErrorIs(t, err, utils.ErrKeyNotFound)
}

func TestPrewriteCommitShortValueSkipsDefaultCF(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(16)
	key := []byte("short-prewrite")
	value := []byte("inline")
	expiresAt := uint64(^uint64(0))

	require.Empty(t, Prewrite(db, latches, &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{{
			Op:        kvrpcpb.Mutation_Put,
			Key:       key,
			Value:     value,
			ExpiresAt: expiresAt,
		}},
		PrimaryLock:  key,
		StartVersion: 10,
		LockTtl:      1000,
	}))

	_, err := db.GetInternalEntry(kv.CFDefault, key, 10)
	require.ErrorIs(t, err, utils.ErrKeyNotFound)

	reader := NewReader(db)
	lock, err := reader.GetLock(key)
	require.NoError(t, err)
	require.NotNil(t, lock)
	require.Equal(t, value, lock.ShortValue)
	require.Equal(t, expiresAt, lock.ExpiresAt)

	require.Nil(t, Commit(db, latches, &kvrpcpb.CommitRequest{
		Keys:          [][]byte{key},
		StartVersion:  10,
		CommitVersion: 20,
	}))

	write, commitTs, err := reader.GetWriteByStartTs(key, 10)
	require.NoError(t, err)
	require.NotNil(t, write)
	require.Equal(t, uint64(20), commitTs)
	require.Equal(t, value, write.ShortValue)
	require.Equal(t, expiresAt, write.ExpiresAt)

	got, gotExpiresAt, err := reader.GetValue(key, 30)
	require.NoError(t, err)
	require.Equal(t, value, got)
	require.Equal(t, expiresAt, gotExpiresAt)
}

func TestPrewriteLargeValueKeepsDefaultCF(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(16)
	key := []byte("large-prewrite")
	value := bytes.Repeat([]byte("x"), mvcc.DefaultShortValueMaxBytes+1)

	require.Empty(t, Prewrite(db, latches, &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{{
			Op:    kvrpcpb.Mutation_Put,
			Key:   key,
			Value: value,
		}},
		PrimaryLock:  key,
		StartVersion: 11,
		LockTtl:      1000,
	}))

	entry, err := db.GetInternalEntry(kv.CFDefault, key, 11)
	require.NoError(t, err)
	require.Equal(t, value, entry.Value)
	entry.DecrRef()

	require.Nil(t, Commit(db, latches, &kvrpcpb.CommitRequest{
		Keys:          [][]byte{key},
		StartVersion:  11,
		CommitVersion: 21,
	}))

	write, _, err := NewReader(db).GetWriteByStartTs(key, 11)
	require.NoError(t, err)
	require.NotNil(t, write)
	require.Empty(t, write.ShortValue)
}

func TestAtomicMutateShortValueSkipsDefaultCF(t *testing.T) {
	opt := testOptionsForDir(filepath.Join(t.TempDir(), "db"))
	opt.WriteShardCount = 1
	db, err := local.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	key := []byte("atomic-short")
	value := []byte("inline")
	req := atomicPutRequest(30, 31, key, value)
	result := ApplyAtomicMutate(db, latch.NewManager(16), req)
	require.Nil(t, result.Error)
	require.False(t, result.Fallback)
	require.Equal(t, uint64(1), result.AppliedKeys)

	_, err = db.GetInternalEntry(kv.CFDefault, key, 30)
	require.ErrorIs(t, err, utils.ErrKeyNotFound)

	reader := NewReader(db)
	write, commitTs, err := reader.GetWriteByStartTs(key, 30)
	require.NoError(t, err)
	require.NotNil(t, write)
	require.Equal(t, uint64(31), commitTs)
	require.Equal(t, value, write.ShortValue)

	got, _, err := reader.GetValue(key, 40)
	require.NoError(t, err)
	require.Equal(t, value, got)
}

func TestBatchRollbackShortValueLockDoesNotWriteDefaultTombstone(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(16)
	key := []byte("short-rollback")

	require.Empty(t, Prewrite(db, latches, &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{{
			Op:    kvrpcpb.Mutation_Put,
			Key:   key,
			Value: []byte("inline"),
		}},
		PrimaryLock:  key,
		StartVersion: 40,
		LockTtl:      1000,
	}))
	require.Nil(t, BatchRollback(db, latches, &kvrpcpb.BatchRollbackRequest{
		Keys:         [][]byte{key},
		StartVersion: 40,
	}))

	_, err := db.GetInternalEntry(kv.CFDefault, key, 40)
	require.ErrorIs(t, err, utils.ErrKeyNotFound)
	write, commitTs, err := NewReader(db).GetWriteByStartTs(key, 40)
	require.NoError(t, err)
	require.NotNil(t, write)
	require.Equal(t, kvrpcpb.Mutation_Rollback, write.Kind)
	require.Equal(t, uint64(40), commitTs)
}

func TestKeyErrorHelpers(t *testing.T) {
	conflict := keyErrorWriteConflict([]byte("k"), []byte("p"), 3, 4, 5)
	require.NotNil(t, conflict.GetWriteConflict())
	require.Equal(t, uint64(3), conflict.GetWriteConflict().GetConflictTs())

	retry := keyErrorRetryable(errors.New("retry"))
	require.Equal(t, "retry", retry.GetRetryable())
}

func TestCommitKeyAlreadyRolledBack(t *testing.T) {
	db := openTestDB(t)
	reader := NewReader(db)
	key := []byte("rb")
	lock := &mvcc.Lock{Primary: key, Ts: 10, Kind: kvrpcpb.Mutation_Put}

	rollback := mvcc.EncodeWrite(mvcc.Write{Kind: kvrpcpb.Mutation_Rollback, StartTs: lock.Ts})
	applyVersionedEntryForTxnTest(t, db, kv.CFWrite, key, 15, rollback, 0)

	err := commitKey(db, reader, key, lock, 20)
	require.NotNil(t, err)
	require.Contains(t, err.GetRetryable(), "transaction already rolled back")
}

func TestCommitKeyWritesAndCleansLock(t *testing.T) {
	db := openTestDB(t)
	reader := NewReader(db)
	key := []byte("commit")
	lock := &mvcc.Lock{Primary: key, Ts: 11, Kind: kvrpcpb.Mutation_Put}

	applyVersionedEntryForTxnTest(t, db, kv.CFLock, key, lockColumnTs, mvcc.EncodeLock(*lock), 0)
	commitErr := commitKey(db, reader, key, lock, 22)
	require.Nil(t, commitErr)

	write, commitTs, readErr := reader.GetWriteByStartTs(key, lock.Ts)
	require.NoError(t, readErr)
	require.NotNil(t, write)
	require.Equal(t, uint64(22), commitTs)
}

func TestCommitKeyAlreadyCommittedDifferentVersion(t *testing.T) {
	db := openTestDB(t)
	reader := NewReader(db)
	key := []byte("dup")
	lock := &mvcc.Lock{Primary: key, Ts: 12, Kind: kvrpcpb.Mutation_Put}

	applyVersionedEntryForTxnTest(t, db, kv.CFWrite, key, 30, mvcc.EncodeWrite(mvcc.Write{Kind: lock.Kind, StartTs: lock.Ts}), 0)
	applyVersionedEntryForTxnTest(t, db, kv.CFLock, key, lockColumnTs, mvcc.EncodeLock(*lock), 0)

	commitErr := commitKey(db, reader, key, lock, 40)
	require.Nil(t, commitErr)
}

func TestRollbackKeyCreatesRollbackWrite(t *testing.T) {
	db := openTestDB(t)
	reader := NewReader(db)
	key := []byte("rb2")
	startTs := uint64(17)

	applyVersionedEntryForTxnTest(t, db, kv.CFLock, key, lockColumnTs, mvcc.EncodeLock(mvcc.Lock{
		Primary: key,
		Ts:      startTs,
		Kind:    kvrpcpb.Mutation_Put,
	}), 0)
	applyVersionedEntryForTxnTest(t, db, kv.CFDefault, key, startTs, []byte("val"), 0)

	rollbackErr := rollbackKey(db, reader, key, startTs)
	require.Nil(t, rollbackErr)

	write, commitTs, readErr := reader.GetWriteByStartTs(key, startTs)
	require.NoError(t, readErr)
	require.NotNil(t, write)
	require.Equal(t, kvrpcpb.Mutation_Rollback, write.Kind)
	require.Equal(t, startTs, commitTs)
}

func TestRollbackKeyReturnsNilWhenWriteAlreadyExists(t *testing.T) {
	db := openTestDB(t)
	reader := NewReader(db)
	key := []byte("rb-existing")
	startTs := uint64(17)

	applyVersionedEntryForTxnTest(t, db, kv.CFWrite, key, 25, mvcc.EncodeWrite(mvcc.Write{
		Kind:    kvrpcpb.Mutation_Put,
		StartTs: startTs,
	}), 0)

	rollbackErr := rollbackKey(db, reader, key, startTs)
	require.Nil(t, rollbackErr)

	write, commitTs, readErr := reader.GetWriteByStartTs(key, startTs)
	require.NoError(t, readErr)
	require.NotNil(t, write)
	require.Equal(t, kvrpcpb.Mutation_Put, write.Kind)
	require.Equal(t, uint64(25), commitTs)
}

func TestRollbackKeyReturnsRetryableWhenWriteLookupFails(t *testing.T) {
	badEntry := kv.NewEntry([]byte("bad-key"), nil)
	t.Cleanup(badEntry.DecrRef)

	store := rollbackTestStore{
		newInternalIterator: func(opt *kv.Options) kv.Iterator {
			return &testIterator{items: []kv.Item{badEntry}}
		},
	}

	rollbackErr := rollbackKey(store, NewReader(store), []byte("rb-write-err"), 9)
	require.NotNil(t, rollbackErr)
	require.Contains(t, rollbackErr.GetRetryable(), "scanWrites expects internal key")
}

func TestRollbackKeyReturnsRetryableWhenLockLookupFails(t *testing.T) {
	store := rollbackTestStore{
		getInternalEntry: func(cf kv.ColumnFamily, key []byte, version uint64) (*kv.Entry, error) {
			return nil, errors.New("lock lookup failed")
		},
	}

	rollbackErr := rollbackKey(store, NewReader(store), []byte("rb-lock-err"), 9)
	require.NotNil(t, rollbackErr)
	require.Contains(t, rollbackErr.GetRetryable(), "lock lookup failed")
}

func TestRollbackKeyReturnsRetryableWhenApplyFails(t *testing.T) {
	store := rollbackTestStore{
		applyInternalEntries: func(entries []*kv.Entry) error {
			return errors.New("apply failed")
		},
	}

	rollbackErr := rollbackKey(store, NewReader(store), []byte("rb-apply-err"), 9)
	require.NotNil(t, rollbackErr)
	require.Contains(t, rollbackErr.GetRetryable(), "apply failed")
}

func TestIsLockExpired(t *testing.T) {
	require.False(t, isLockExpired(nil, 10))
	require.False(t, isLockExpired(&mvcc.Lock{Ts: 5, StartTime: 100, TTL: 0}, 110))
	require.False(t, isLockExpired(&mvcc.Lock{Ts: 5, StartTime: 0, TTL: 5}, 110))
	require.False(t, isLockExpired(&mvcc.Lock{Ts: 5, StartTime: 100, TTL: 5}, 104))
	require.True(t, isLockExpired(&mvcc.Lock{Ts: 5, StartTime: 100, TTL: 5}, 105))
}

func TestCommitTsExpired(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(32)
	key := []byte("expired")

	prewriteReq := &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{{
			Op:    kvrpcpb.Mutation_Put,
			Key:   key,
			Value: []byte("v1"),
		}},
		PrimaryLock:  key,
		StartVersion: 10,
		LockTtl:      1000,
		// TODO: Manually set a large MinCommitTs to simulate that a concurrent
		// reader has pushed the lock's min_commit_ts.
		// This hardcoded value should be removed once the Read
		// path for pushing min_commit_ts is implemented.
		MinCommitTs: 30,
	}
	require.Empty(t, Prewrite(db, latches, prewriteReq))

	commitReq := &kvrpcpb.CommitRequest{
		Keys:          [][]byte{key},
		StartVersion:  10,
		CommitVersion: 20,
	}
	err := Commit(db, latches, commitReq)

	require.NotNil(t, err)
	require.NotNil(t, err.GetCommitTsExpired())
	require.Equal(t, uint64(20), err.GetCommitTsExpired().CommitTs)
	require.Equal(t, uint64(30), err.GetCommitTsExpired().MinCommitTs)

	// Mock retry commit with new commit ts
	commitReq = &kvrpcpb.CommitRequest{
		Keys:          [][]byte{key},
		StartVersion:  10,
		CommitVersion: 30,
	}
	require.Nil(t, Commit(db, latches, commitReq))
}

// TestIdempotentCommitCleansUpLock verifies that a duplicate commit with the same startTs and commitVersion deletes the lock.
func TestIdempotentCommitCleansUpLock(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(32)
	key := []byte("k")

	lockVal := mvcc.EncodeLock(mvcc.Lock{
		Primary:   key,
		Ts:        11,
		StartTime: 1100,
		TTL:       3000,
		Kind:      kvrpcpb.Mutation_Put,
	})
	applyVersionedEntryForTxnTest(t, db, kv.CFLock, key, lockColumnTs, lockVal, 0)

	writeVal := mvcc.EncodeWrite(mvcc.Write{Kind: kvrpcpb.Mutation_Put, StartTs: 11})
	applyVersionedEntryForTxnTest(t, db, kv.CFWrite, key, 22, writeVal, 0)

	commitReq := &kvrpcpb.CommitRequest{
		Keys:          [][]byte{key},
		StartVersion:  11,
		CommitVersion: 22,
	}
	require.Nil(t, Commit(db, latches, commitReq))

	reader := NewReader(db)
	lock, err := reader.GetLock(key)
	require.NoError(t, err)
	require.Nil(t, lock, "idempotent commit must not leave a stale lock")
}

// TestIdempotentCommitWithPushedMinCommitTs verifies that an idempotent commit cleans up the lock even when MinCommitTs has been pushed past the original commitVersion.
func TestIdempotentCommitWithPushedMinCommitTs(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(32)
	key := []byte("k")

	lockVal := mvcc.EncodeLock(mvcc.Lock{
		Primary:     key,
		Ts:          11,
		StartTime:   1100,
		TTL:         3000,
		Kind:        kvrpcpb.Mutation_Put,
		MinCommitTs: 30,
	})
	applyVersionedEntryForTxnTest(t, db, kv.CFLock, key, lockColumnTs, lockVal, 0)

	writeVal := mvcc.EncodeWrite(mvcc.Write{Kind: kvrpcpb.Mutation_Put, StartTs: 11})
	applyVersionedEntryForTxnTest(t, db, kv.CFWrite, key, 22, writeVal, 0)

	commitReq := &kvrpcpb.CommitRequest{
		Keys:          [][]byte{key},
		StartVersion:  11,
		CommitVersion: 22,
	}
	require.Nil(t, Commit(db, latches, commitReq))

	reader := NewReader(db)
	lock, err := reader.GetLock(key)
	require.NoError(t, err)
	require.Nil(t, lock, "idempotent commit must clean lock even when MinCommitTs was pushed")
}
