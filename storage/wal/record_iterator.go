// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package wal

import (
	"bufio"
	"io"
)

// RecordIterator provides iterator-style access over WAL records stored in the
// | length (4B) | type (1B) | payload | crc32 (4B) | layout.
type RecordIterator struct {
	reader  *bufio.Reader
	buffer  []byte
	recType RecordType
	length  uint32
	err     error
}

// NewRecordIterator constructs an iterator over WAL records using the provided reader.
func NewRecordIterator(r io.Reader, bufSize int) *RecordIterator {
	if r == nil {
		return &RecordIterator{err: io.EOF}
	}
	br, ok := r.(*bufio.Reader)
	if !ok {
		if bufSize <= 0 {
			bufSize = defaultBufferSize
		}
		br = bufio.NewReaderSize(r, bufSize)
	}
	return &RecordIterator{reader: br}
}

// Next advances the stream to the next record. The payload excludes the type byte.
func (rs *RecordIterator) Next() bool {
	if rs == nil || rs.err != nil {
		return false
	}

	recType, payload, length, err := DecodeRecord(rs.reader)
	if err != nil {
		rs.err = err
		return false
	}

	rs.recType = recType
	// payload already excludes the type byte.
	rs.buffer = payload
	rs.length = length
	return true
}

// Record returns a copy of the record payload (type byte excluded).
func (rs *RecordIterator) Record() []byte {
	if rs == nil {
		return nil
	}
	if rs.length <= 1 {
		return nil
	}
	// rs.buffer now contains just the payload, so we return a copy of it directly.
	return append([]byte(nil), rs.buffer...)
}

// Type returns the record type of the current record.
func (rs *RecordIterator) Type() RecordType {
	if rs == nil {
		return RecordTypeEntry
	}
	return rs.recType
}

// Length returns the encoded payload length (type byte + payload bytes).
func (rs *RecordIterator) Length() uint32 {
	if rs == nil {
		return 0
	}
	return rs.length
}

// Err returns the terminal error for the stream.
func (rs *RecordIterator) Err() error {
	if rs == nil {
		return nil
	}
	return rs.err
}

// Close releases resources held by the stream.
func (rs *RecordIterator) Close() error {
	if rs == nil {
		return nil
	}
	rs.buffer = nil
	return nil
}
