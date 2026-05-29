// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package percolator

import (
	"testing"

	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/feichai0017/NoKV/txn/mvcc"
	kv "github.com/feichai0017/NoKV/txn/storage"
	"github.com/stretchr/testify/require"
)

func TestReaderGetLockReturnsLiveLockWithoutWriteRecord(t *testing.T) {
	db := openTestDB(t)
	key := []byte("live-lock")

	applyVersionedEntryForTxnTest(t, db, kv.CFLock, key, lockColumnTs, mvcc.EncodeLock(mvcc.Lock{
		Primary:   key,
		Ts:        10,
		StartTime: 1000,
		TTL:       3000,
		Kind:      kvrpcpb.Mutation_Put,
	}), 0)

	lock, err := NewReader(db).GetLock(key)
	require.NoError(t, err)
	require.NotNil(t, lock)
	require.Equal(t, uint64(10), lock.Ts)
}

func TestReaderGetLockIgnoresCommittedStaleLock(t *testing.T) {
	db := openTestDB(t)
	key := []byte("committed-stale-lock")

	applyVersionedEntryForTxnTest(t, db, kv.CFLock, key, lockColumnTs, mvcc.EncodeLock(mvcc.Lock{
		Primary:   key,
		Ts:        10,
		StartTime: 1000,
		TTL:       3000,
		Kind:      kvrpcpb.Mutation_Put,
	}), 0)
	applyVersionedEntryForTxnTest(t, db, kv.CFWrite, key, 11, mvcc.EncodeWrite(mvcc.Write{
		Kind:    kvrpcpb.Mutation_Put,
		StartTs: 10,
	}), 0)

	lock, err := NewReader(db).GetLock(key)
	require.NoError(t, err)
	require.Nil(t, lock)
}

func TestReaderGetLockIgnoresRolledBackStaleLock(t *testing.T) {
	db := openTestDB(t)
	key := []byte("rolled-back-stale-lock")

	applyVersionedEntryForTxnTest(t, db, kv.CFLock, key, lockColumnTs, mvcc.EncodeLock(mvcc.Lock{
		Primary:   key,
		Ts:        10,
		StartTime: 1000,
		TTL:       3000,
		Kind:      kvrpcpb.Mutation_Delete,
	}), 0)
	applyVersionedEntryForTxnTest(t, db, kv.CFWrite, key, 10, mvcc.EncodeWrite(mvcc.Write{
		Kind:    kvrpcpb.Mutation_Rollback,
		StartTs: 10,
	}), 0)

	lock, err := NewReader(db).GetLock(key)
	require.NoError(t, err)
	require.Nil(t, lock)
}
