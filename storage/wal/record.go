// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"

	kv "github.com/feichai0017/NoKV/txn/storage"
)

// RecordType identifies the kind of payload stored in the WAL.
type RecordType uint8

const (
	// RecordTypeEntry represents an LSM mutation (default behaviour).
	RecordTypeEntry RecordType = iota
	// RecordTypeRaftEntry encodes a batch of raft log entries.
	RecordTypeRaftEntry
	// RecordTypeRaftState encodes a raft HardState update.
	RecordTypeRaftState
	// RecordTypeRaftSnapshot encodes a raft snapshot payload.
	RecordTypeRaftSnapshot
	// RecordTypeEntryBatch encodes a batch of LSM mutations in one WAL record.
	RecordTypeEntryBatch
	// RecordTypeWitnessEvidence encodes subsystem witness evidence.
	RecordTypeWitnessEvidence
	// RecordTypeVisibleCommit encodes holder-local visible acknowledgements.
	RecordTypeVisibleCommit
)

// Record describes a typed WAL payload.
//
// The WAL record is stored on disk in the following format:
//
//	+--------+-----------+-----------+---------+
//	| Length | Type      | Payload   | CRC32   |
//	| [4]byte| [1]byte   | [N]byte   | [4]byte |
//	+--------+-----------+-----------+---------+
//
// - Length: The length of the Type and Payload fields.
// - Type: The type of the record, as defined by RecordType.
// - Payload: The record's data.
// - CRC32: A CRC32 checksum of the Type and Payload.
type Record struct {
	Type    RecordType
	Payload []byte
}

func DecodeRecord(r io.Reader) (RecordType, []byte, uint32, error) {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return 0, nil, 0, io.EOF
		}
		return 0, nil, 0, err
	}

	length := binary.BigEndian.Uint32(header[:])
	if length == 0 {
		return 0, nil, 0, ErrEmptyRecord
	}

	// Allocate buffer for type byte + payload.
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return 0, nil, 0, ErrPartialRecord
		}
		return 0, nil, 0, err
	}

	var crcBuf [4]byte
	if _, err := io.ReadFull(r, crcBuf[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return 0, nil, 0, ErrPartialRecord
		}
		return 0, nil, 0, err
	}

	expected := binary.BigEndian.Uint32(crcBuf[:])
	hasher := kv.CRC32()
	// Calculate CRC over type byte + payload.
	if _, err := hasher.Write(buf); err != nil {
		kv.PutCRC32(hasher)
		return 0, nil, 0, err
	}
	sum := hasher.Sum32()
	kv.PutCRC32(hasher)
	if expected != sum {
		return 0, nil, 0, kv.ErrBadChecksum
	}

	recType := RecordType(buf[0])
	payload := buf[1:] // Payload is the rest after the type byte.

	return recType, payload, length, nil
}

// EncodeRecordTo appends one WAL record's on-disk encoding to dst and returns
// the extended slice. The encoded layout matches EncodeRecord:
//
//	[4 BE length][1 type][payload bytes][4 BE CRC32]
//
// where length covers (type + payload) and CRC32 covers (type + payload).
//
// This decouples the CPU work (CRC + serialization) from any io.Writer so that
// callers in AppendRecords can encode records outside m.mu in parallel and
// then briefly hold the lock only for the bufio copy step.
func EncodeRecordTo(dst []byte, recType RecordType, payload []byte) []byte {
	typeByte := byte(recType)
	payloadLen := uint32(len(payload) + 1) // type byte + payload

	// 4-byte length header.
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], payloadLen)
	dst = append(dst, hdr[:]...)

	// 1-byte type.
	dst = append(dst, typeByte)

	// payload bytes.
	dst = append(dst, payload...)

	// CRC32 over (type + payload).
	h := kv.CRC32()
	_, _ = h.Write([]byte{typeByte})
	_, _ = h.Write(payload)
	crc := h.Sum32()
	kv.PutCRC32(h)

	binary.BigEndian.PutUint32(hdr[:], crc)
	dst = append(dst, hdr[:]...)
	return dst
}

// recordBufPool reuses scratch byte slices for AppendRecords' lock-free
// encoding phase. The slice is filled outside m.mu and copied into the WAL
// bufio writer under the lock, so the pool is safe to reuse.
var recordBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 4<<10) // 4 KiB initial cap
		return &b
	},
}

func acquireRecordBuf() *[]byte {
	bp := recordBufPool.Get().(*[]byte)
	*bp = (*bp)[:0]
	return bp
}

func releaseRecordBuf(bp *[]byte) {
	if bp == nil {
		return
	}
	const maxPooledCap = 1 << 20 // 1 MiB
	if cap(*bp) > maxPooledCap {
		return
	}
	recordBufPool.Put(bp)
}

// entryBufPool reuses the short-lived per-entry encoding buffer used by
// EncodeEntryBatch. Each entry's encoded bytes are copied into the outer
// output buffer immediately, so the per-entry buffer can be safely returned
// to the pool after the batch encode completes. The outer output buffer is
// NOT pooled because its slice escapes to the caller; reusing it would race
// with the next EncodeEntryBatch on a different goroutine.
var entryBufPool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
}

func acquireEntryBuf() *bytes.Buffer {
	buf := entryBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	return buf
}

func releaseEntryBuf(buf *bytes.Buffer) {
	if buf == nil {
		return
	}
	// Drop oversized buffers so a single huge entry does not pin a large
	// allocation forever in the pool.
	const maxPooledCap = 1 << 20 // 1 MiB
	if buf.Cap() > maxPooledCap {
		return
	}
	entryBufPool.Put(buf)
}

// EncodeEntryBatch encodes entries into a single payload suitable for RecordTypeEntryBatch.
//
// Payload layout:
//
//	+--------------+------------------------------+
//	| entry_count  | repeated(entry_len + entry) |
//	| uint32 (BE)  | uint32 (BE) + raw bytes     |
//	+--------------+------------------------------+
func EncodeEntryBatch(entries []*kv.Entry) ([]byte, error) {
	if len(entries) == 0 {
		return nil, fmt.Errorf("wal: empty entry batch")
	}
	// Outer out buffer is fresh per-call because its byte slice escapes.
	// The per-entry scratch buffer is pooled (its bytes get copied into out
	// immediately so reuse is safe).
	var out bytes.Buffer
	entryBuf := acquireEntryBuf()
	defer releaseEntryBuf(entryBuf)

	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(entries)))
	if _, err := out.Write(header[:]); err != nil {
		return nil, err
	}
	for _, e := range entries {
		if e == nil || len(e.Key) == 0 {
			return nil, fmt.Errorf("wal: invalid entry in batch")
		}
		payload, err := kv.EncodeEntry(entryBuf, e)
		if err != nil {
			return nil, err
		}
		binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
		if _, err := out.Write(header[:]); err != nil {
			return nil, err
		}
		if _, err := out.Write(payload); err != nil {
			return nil, err
		}
	}
	return out.Bytes(), nil
}

// DecodeEntryBatch decodes a RecordTypeEntryBatch payload.
// Returned entries are pooled objects with refcount 1; caller must DecrRef().
func DecodeEntryBatch(payload []byte) ([]*kv.Entry, error) {
	if len(payload) < 4 {
		return nil, fmt.Errorf("wal: malformed entry batch payload")
	}
	count := binary.BigEndian.Uint32(payload[:4])
	rest := payload[4:]
	if count == 0 {
		return nil, fmt.Errorf("wal: malformed entry batch payload")
	}
	// Each batch element must at least contain a 4-byte length field and
	// a non-empty encoded entry payload.
	if uint64(count) > uint64(len(rest))/5 {
		return nil, fmt.Errorf("wal: malformed entry batch payload")
	}
	maxInt := int(^uint(0) >> 1)
	if uint64(count) > uint64(maxInt) {
		return nil, fmt.Errorf("wal: malformed entry batch payload")
	}
	prealloc := min(int(count), 1024)
	entries := make([]*kv.Entry, 0, prealloc)
	defer func() {
		if rest != nil {
			for _, e := range entries {
				e.DecrRef()
			}
		}
	}()
	for range count {
		if len(rest) < 4 {
			return nil, fmt.Errorf("wal: malformed entry batch payload")
		}
		ln := binary.BigEndian.Uint32(rest[:4])
		rest = rest[4:]
		if ln == 0 || uint32(len(rest)) < ln {
			return nil, fmt.Errorf("wal: malformed entry batch payload")
		}
		entry, err := kv.DecodeEntry(rest[:ln])
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
		rest = rest[ln:]
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("wal: malformed entry batch payload")
	}
	decoded := entries
	rest = nil
	return decoded, nil
}
