// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package memory provides an in-memory ordered KV implementation for tests.
package memory

import (
	"bytes"
	"sort"
	"sync"

	rawkv "github.com/feichai0017/NoKV/storage/kv"
	"github.com/feichai0017/NoKV/utils"
)

// Store is an in-memory raw ordered KV store.
type Store struct {
	mu     sync.RWMutex
	closed bool
	data   map[string][]byte
}

// New constructs an empty memory store.
func New() *Store {
	return &Store{data: make(map[string][]byte)}
}

func (s *Store) Get(key []byte) ([]byte, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, false, utils.ErrBlockedWrites
	}
	value, ok := s.data[string(key)]
	if !ok {
		return nil, false, nil
	}
	return append([]byte(nil), value...), true, nil
}

func (s *Store) Put(key, value []byte) error {
	return s.ApplyBatch(rawkv.Batch{Ops: []rawkv.Mutation{{Op: rawkv.PutOp, Key: key, Value: value}}})
}

func (s *Store) Delete(key []byte) error {
	return s.ApplyBatch(rawkv.Batch{Ops: []rawkv.Mutation{{Op: rawkv.DeleteOp, Key: key}}})
}

func (s *Store) DeleteRange(start, end []byte) error {
	return s.ApplyBatch(rawkv.Batch{Ops: []rawkv.Mutation{{Op: rawkv.DeleteRangeOp, Key: start, End: end}}})
}

func (s *Store) ApplyBatch(batch rawkv.Batch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return utils.ErrBlockedWrites
	}
	for _, op := range batch.Ops {
		switch op.Op {
		case rawkv.PutOp:
			s.data[string(op.Key)] = append([]byte(nil), op.Value...)
		case rawkv.DeleteOp:
			delete(s.data, string(op.Key))
		case rawkv.DeleteRangeOp:
			for key := range s.data {
				k := []byte(key)
				if bytes.Compare(k, op.Key) >= 0 && bytes.Compare(k, op.End) < 0 {
					delete(s.data, key)
				}
			}
		}
	}
	return nil
}

func (s *Store) NewIterator(opts rawkv.IteratorOptions) (rawkv.Iterator, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return newIterator(snapshotData(s.data), opts), nil
}

func (s *Store) Snapshot() (rawkv.Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return &snapshot{data: snapshotData(s.data)}, nil
}

func (s *Store) Sync() error { return nil }

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func (s *Store) Stats() rawkv.Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var size uint64
	for k, v := range s.data {
		size += uint64(len(k) + len(v))
	}
	return rawkv.Stats{KeysEstimate: uint64(len(s.data)), SizeBytes: size}
}

type snapshot struct {
	data map[string][]byte
}

func (s *snapshot) Get(key []byte) ([]byte, bool, error) {
	value, ok := s.data[string(key)]
	if !ok {
		return nil, false, nil
	}
	return append([]byte(nil), value...), true, nil
}

func (s *snapshot) NewIterator(opts rawkv.IteratorOptions) (rawkv.Iterator, error) {
	return newIterator(snapshotData(s.data), opts), nil
}

func (s *snapshot) Close() error { return nil }

type iterator struct {
	keys   [][]byte
	values [][]byte
	idx    int
	opts   rawkv.IteratorOptions
}

func newIterator(data map[string][]byte, opts rawkv.IteratorOptions) *iterator {
	keys := make([][]byte, 0, len(data))
	for key := range data {
		k := []byte(key)
		if len(opts.LowerBound) > 0 && bytes.Compare(k, opts.LowerBound) < 0 {
			continue
		}
		if len(opts.UpperBound) > 0 && bytes.Compare(k, opts.UpperBound) >= 0 {
			continue
		}
		keys = append(keys, append([]byte(nil), k...))
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 })
	values := make([][]byte, len(keys))
	for i, key := range keys {
		values[i] = data[string(key)]
	}
	return &iterator{keys: keys, values: values, idx: -1, opts: opts}
}

func (it *iterator) First() bool {
	if len(it.keys) == 0 {
		it.idx = -1
		return false
	}
	it.idx = 0
	return true
}

func (it *iterator) Last() bool {
	if len(it.keys) == 0 {
		it.idx = -1
		return false
	}
	it.idx = len(it.keys) - 1
	return true
}

func (it *iterator) Seek(key []byte) bool {
	pos := sort.Search(len(it.keys), func(i int) bool {
		return bytes.Compare(it.keys[i], key) >= 0
	})
	if pos >= len(it.keys) {
		it.idx = -1
		return false
	}
	it.idx = pos
	return true
}

func (it *iterator) Next() bool {
	if it.idx < 0 {
		return it.First()
	}
	it.idx++
	return it.Valid()
}

func (it *iterator) Prev() bool {
	if it.idx < 0 {
		return it.Last()
	}
	it.idx--
	return it.Valid()
}

func (it *iterator) Valid() bool {
	return it != nil && it.idx >= 0 && it.idx < len(it.keys)
}

func (it *iterator) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return it.keys[it.idx]
}

func (it *iterator) Value() ([]byte, error) {
	if !it.Valid() {
		return nil, nil
	}
	return append([]byte(nil), it.values[it.idx]...), nil
}

func (it *iterator) Close() error { return nil }

func snapshotData(data map[string][]byte) map[string][]byte {
	out := make(map[string][]byte, len(data))
	for key, value := range data {
		out[key] = append([]byte(nil), value...)
	}
	return out
}
