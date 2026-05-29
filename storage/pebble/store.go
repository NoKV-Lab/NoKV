// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package pebble adapts CockroachDB Pebble to NoKV's raw ordered KV boundary.
package pebble

import (
	"errors"

	cpebble "github.com/cockroachdb/pebble"

	rawkv "github.com/feichai0017/NoKV/storage/kv"
)

// Options configures a Pebble-backed raw KV store.
type Options struct {
	Dir           string
	SyncWrites    bool
	CacheBytes    int64
	MemTableBytes uint64
}

// Store wraps a Pebble DB.
type Store struct {
	db        *cpebble.DB
	cache     *cpebble.Cache
	writeOpts *cpebble.WriteOptions
}

// Open opens a Pebble-backed raw KV store.
func Open(opts Options) (*Store, error) {
	pebbleOpts := &cpebble.Options{}
	if opts.CacheBytes > 0 {
		pebbleOpts.Cache = cpebble.NewCache(opts.CacheBytes)
	}
	if opts.MemTableBytes > 0 {
		pebbleOpts.MemTableSize = max(opts.MemTableBytes, 64<<10)
	}
	db, err := cpebble.Open(opts.Dir, pebbleOpts)
	if err != nil {
		if pebbleOpts.Cache != nil {
			pebbleOpts.Cache.Unref()
		}
		return nil, err
	}
	writeOpts := cpebble.NoSync
	if opts.SyncWrites {
		writeOpts = cpebble.Sync
	}
	return &Store{db: db, cache: pebbleOpts.Cache, writeOpts: writeOpts}, nil
}

func (s *Store) Get(key []byte) ([]byte, bool, error) {
	value, closer, err := s.db.Get(key)
	if errors.Is(err, cpebble.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = closer.Close() }()
	return append([]byte(nil), value...), true, nil
}

func (s *Store) Put(key, value []byte) error {
	return s.db.Set(key, value, s.writeOpts)
}

func (s *Store) Delete(key []byte) error {
	return s.db.Delete(key, s.writeOpts)
}

func (s *Store) DeleteRange(start, end []byte) error {
	return s.db.DeleteRange(start, end, s.writeOpts)
}

func (s *Store) ApplyBatch(batch rawkv.Batch) error {
	if len(batch.Ops) == 0 {
		return nil
	}
	b := s.db.NewBatch()
	defer func() { _ = b.Close() }()
	for _, op := range batch.Ops {
		switch op.Op {
		case rawkv.PutOp:
			if err := b.Set(op.Key, op.Value, nil); err != nil {
				return err
			}
		case rawkv.DeleteOp:
			if err := b.Delete(op.Key, nil); err != nil {
				return err
			}
		case rawkv.DeleteRangeOp:
			if err := b.DeleteRange(op.Key, op.End, nil); err != nil {
				return err
			}
		}
	}
	return b.Commit(s.writeOpts)
}

func (s *Store) NewIterator(opts rawkv.IteratorOptions) (rawkv.Iterator, error) {
	it, err := s.db.NewIter(toPebbleIterOptions(opts))
	if err != nil {
		return nil, err
	}
	return &iterator{it: it}, nil
}

func (s *Store) Snapshot() (rawkv.Snapshot, error) {
	return &snapshot{snap: s.db.NewSnapshot()}, nil
}

func (s *Store) Sync() error {
	return s.db.Flush()
}

func (s *Store) Close() error {
	var err error
	if s.db != nil {
		err = s.db.Close()
		s.db = nil
	}
	if s.cache != nil {
		s.cache.Unref()
		s.cache = nil
	}
	return err
}

func (s *Store) Stats() rawkv.Stats {
	if s == nil || s.db == nil {
		return rawkv.Stats{}
	}
	metrics := s.db.Metrics()
	return rawkv.Stats{
		SizeBytes: uint64(metrics.DiskSpaceUsage()) + metrics.MemTable.Size,
	}
}

type snapshot struct {
	snap *cpebble.Snapshot
}

func (s *snapshot) Get(key []byte) ([]byte, bool, error) {
	value, closer, err := s.snap.Get(key)
	if errors.Is(err, cpebble.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = closer.Close() }()
	return append([]byte(nil), value...), true, nil
}

func (s *snapshot) NewIterator(opts rawkv.IteratorOptions) (rawkv.Iterator, error) {
	it, err := s.snap.NewIter(toPebbleIterOptions(opts))
	if err != nil {
		return nil, err
	}
	return &iterator{it: it}, nil
}

func (s *snapshot) Close() error {
	return s.snap.Close()
}

type iterator struct {
	it *cpebble.Iterator
}

// forwarding-ok: iterator adapts Pebble's concrete iterator to storage/kv.Iterator.
func (it *iterator) First() bool { return it.it.First() }

// forwarding-ok: iterator adapts Pebble's concrete iterator to storage/kv.Iterator.
func (it *iterator) Last() bool { return it.it.Last() }
func (it *iterator) Seek(key []byte) bool {
	return it.it.SeekGE(key)
}
func (it *iterator) Next() bool  { return it.it.Next() }
func (it *iterator) Prev() bool  { return it.it.Prev() }
func (it *iterator) Valid() bool { return it.it.Valid() }
func (it *iterator) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return append([]byte(nil), it.it.Key()...)
}
func (it *iterator) Value() ([]byte, error) {
	if !it.Valid() {
		return nil, nil
	}
	value, err := it.it.ValueAndErr()
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), value...), nil
}
func (it *iterator) Close() error {
	err := it.it.Error()
	if closeErr := it.it.Close(); err == nil {
		err = closeErr
	}
	return err
}

func toPebbleIterOptions(opts rawkv.IteratorOptions) *cpebble.IterOptions {
	return &cpebble.IterOptions{
		LowerBound: opts.LowerBound,
		UpperBound: opts.UpperBound,
	}
}
