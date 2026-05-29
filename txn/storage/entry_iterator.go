// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"bufio"
	"io"
)

// EntryIterator sequentially decodes entries stored using NoKV's entry-stream
// record layout:
//
//	| header(varint fields) | key bytes | value bytes | crc32 (4B) |
//
// The header encodes key length, value length, meta, and expiresAt via
// uvarint encoding (see EntryHeader). Entry.Key is returned as raw bytes; the
// caller is responsible for knowing whether a given stream stores user keys,
// base keys, or internal keys.
type EntryIterator struct {
	reader    *bufio.Reader
	current   *Entry
	recordLen uint32
	err       error
}

// NewEntryIterator constructs an iterator over the provided reader. The reader
// must contain records encoded via EncodeEntryTo in sequential order. Callers
// must Close the iterator to return pooled entries.
func NewEntryIterator(r io.Reader) *EntryIterator {
	if r == nil {
		return &EntryIterator{err: io.EOF}
	}
	if br, ok := r.(*bufio.Reader); ok {
		return &EntryIterator{reader: br}
	}
	return &EntryIterator{reader: bufio.NewReader(r)}
}

// Next advances to the next entry, returning true when an entry was decoded
// successfully. The returned entry maintains a reference count that must be
// released via Close or by advancing to the next record.
func (it *EntryIterator) Next() bool {
	if it == nil || it.err != nil {
		return false
	}
	it.releaseCurrent()

	// Delegate the complex decoding logic to DecodeEntryFrom
	entry, recordLen, err := DecodeEntryFrom(it.reader)
	if err != nil {
		it.err = err // Store the terminal error (e.g., io.EOF)
		return false
	}

	it.current = entry
	it.recordLen = recordLen
	return true
}

// Entry returns the current entry. It remains valid until the next call to Next
// or Close.
func (it *EntryIterator) Entry() *Entry {
	if it == nil {
		return nil
	}
	return it.current
}

// RecordLen reports the encoded length of the current record.
func (it *EntryIterator) RecordLen() uint32 {
	if it == nil {
		return 0
	}
	return it.recordLen
}

// Err returns the terminal error that stopped iteration. A nil or io.EOF error
// indicates successful exhaustion.
func (it *EntryIterator) Err() error {
	if it == nil {
		return nil
	}
	return it.err
}

// Close releases the current entry to the pool.
func (it *EntryIterator) Close() error {
	if it == nil {
		return nil
	}
	it.releaseCurrent()
	return nil
}

func (it *EntryIterator) releaseCurrent() {
	if it == nil || it.current == nil {
		return
	}
	it.current.DecrRef()
	it.current = nil
}
