// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package percolator

import (
	"bytes"
	"fmt"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"time"

	"github.com/feichai0017/NoKV/txn/mvcc"
	txnstore "github.com/feichai0017/NoKV/txn/storage"
	"github.com/feichai0017/NoKV/utils"
)

const lockColumnTs = txnstore.MaxVersion

// Reader provides helper methods to inspect MVCC state within a DB instance.
type Reader struct {
	db txnstore.Store
}

// NewReader constructs a Reader.
func NewReader(db txnstore.Store) *Reader {
	return &Reader{db: db}
}

// GetLock returns the lock stored for the provided key, if any.
func (r *Reader) GetLock(key []byte) (*mvcc.Lock, error) {
	entry, err := r.db.GetInternalEntry(txnstore.CFLock, key, lockColumnTs)
	if err != nil {
		if err == utils.ErrKeyNotFound {
			return nil, nil
		}
		return nil, err
	}
	defer entry.DecrRef()
	if entry.Meta&txnstore.BitDelete > 0 || entry.Value == nil {
		return nil, nil
	}
	lock, err := mvcc.DecodeLock(entry.Value)
	if err != nil {
		return nil, err
	}
	write, _, err := r.GetWriteByStartTs(key, lock.Ts)
	if err != nil {
		return nil, err
	}
	if write != nil {
		return nil, nil
	}
	return &lock, nil
}

// MostRecentWrite returns the latest committed write for the specified key.
func (r *Reader) MostRecentWrite(key []byte) (*mvcc.Write, uint64, error) {
	var result *mvcc.Write
	var commitTs uint64
	if err := r.scanWrites(key, func(w mvcc.Write, ts uint64) bool {
		if result == nil || ts > commitTs {
			copy := w
			result = &copy
			commitTs = ts
		}
		return true
	}); err != nil {
		return nil, 0, err
	}
	if result == nil {
		return nil, 0, nil
	}
	return result, commitTs, nil
}

// GetWriteByStartTs returns the commit record for the provided key/startTs pair.
func (r *Reader) GetWriteByStartTs(key []byte, startTs uint64) (*mvcc.Write, uint64, error) {
	var result *mvcc.Write
	var commitTs uint64
	if err := r.scanWrites(key, func(w mvcc.Write, ts uint64) bool {
		if w.StartTs == startTs {
			copy := w
			result = &copy
			commitTs = ts
			return false
		}
		if ts < startTs {
			return false
		}
		return true
	}); err != nil {
		return nil, 0, err
	}
	if result == nil {
		return nil, 0, nil
	}
	return result, commitTs, nil
}

// GetValue reads the value visible at readTs and returns its expiry metadata
// from the default CF record.
func (r *Reader) GetValue(key []byte, readTs uint64) ([]byte, uint64, error) {
	write, _, err := r.getWriteForRead(key, readTs)
	if err != nil {
		return nil, 0, err
	}
	if write == nil {
		return nil, 0, utils.ErrKeyNotFound
	}
	if write.Kind == kvrpcpb.Mutation_Delete || write.Kind == kvrpcpb.Mutation_Rollback {
		return nil, 0, utils.ErrKeyNotFound
	}
	if len(write.ShortValue) > 0 {
		if write.ExpiresAt > 0 && write.ExpiresAt <= uint64(time.Now().Unix()) {
			return nil, 0, utils.ErrKeyNotFound
		}
		return txnstore.SafeCopy(nil, write.ShortValue), write.ExpiresAt, nil
	}
	entry, err := r.db.GetInternalEntry(txnstore.CFDefault, key, write.StartTs)
	if err != nil {
		return nil, 0, err
	}
	defer entry.DecrRef()
	if entry.IsDeletedOrExpired() {
		return nil, 0, utils.ErrKeyNotFound
	}
	return txnstore.SafeCopy(nil, entry.Value), entry.ExpiresAt, nil
}

func (r *Reader) getWriteForRead(key []byte, readTs uint64) (*mvcc.Write, uint64, error) {
	var result *mvcc.Write
	var commitTs uint64
	if err := r.scanWrites(key, func(w mvcc.Write, ts uint64) bool {
		if ts <= readTs && (w.Kind == kvrpcpb.Mutation_Lock || w.Kind == kvrpcpb.Mutation_Rollback) {
			return true
		}
		if ts <= readTs && (result == nil || ts > commitTs) {
			copy := w
			result = &copy
			commitTs = ts
		}
		return true
	}); err != nil {
		return nil, 0, err
	}
	if result == nil {
		return nil, 0, nil
	}
	return result, commitTs, nil
}

func (r *Reader) scanWrites(key []byte, fn func(mvcc.Write, uint64) bool) error {
	iter := r.db.NewInternalIterator(&txnstore.Options{IsAsc: true})
	defer func() { _ = iter.Close() }()
	if iter == nil {
		return nil
	}
	iter.Seek(txnstore.InternalKey(txnstore.CFWrite, key, txnstore.MaxVersion))
	for iter.Valid() {
		item := iter.Item()
		if item == nil {
			iter.Next()
			continue
		}
		entry := item.Entry()
		if entry == nil {
			iter.Next()
			continue
		}
		cf, userKey, ts, ok := txnstore.SplitInternalKey(entry.Key)
		if !ok {
			return fmt.Errorf("percolator: scanWrites expects internal key, got %x", entry.Key)
		}
		if cf != txnstore.CFWrite {
			break
		}
		if !bytes.Equal(userKey, key) {
			iter.Next()
			break
		}
		if entry.Meta&txnstore.BitDelete > 0 {
			iter.Next()
			continue
		}
		write, err := mvcc.DecodeWrite(entry.Value)
		if err != nil {
			return err
		}
		if !fn(write, ts) {
			break
		}
		iter.Next()
	}
	return nil
}
