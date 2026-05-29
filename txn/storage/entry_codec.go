// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"math"
	"sync"
)

var crc32Pool = sync.Pool{
	New: func() any {
		return crc32.New(CastagnoliCrcTable)
	},
}

var headerPool = sync.Pool{
	New: func() any {
		buf := make([]byte, MaxEntryHeaderSize)
		return &buf
	},
}

// CRC32 returns a Castagnoli hash from the shared pool.
func CRC32() hash.Hash32 {
	h := crc32Pool.Get().(hash.Hash32)
	h.Reset()
	return h
}

// PutCRC32 returns a hash to the pool.
func PutCRC32(h hash.Hash32) {
	if h == nil {
		return
	}
	h.Reset()
	crc32Pool.Put(h)
}

// EntryHeader is the unified entry header used by WAL encodings.
// Binary layout (Encode):
//
//	varint(KeyLen) | varint(ValueLen) | varint(Meta) | varint(ExpiresAt)
//
// Meta is constrained to 1 byte; lengths are stored as unsigned varints.
type EntryHeader struct {
	KeyLen    uint32
	ValueLen  uint32
	Meta      byte
	ExpiresAt uint64
}

func (h *EntryHeader) decode(readVarint func() (uint64, error)) error {
	klen, err := readVarint()
	if err != nil {
		return err
	}
	h.KeyLen = uint32(klen)

	vlen, err := readVarint()
	if err != nil {
		return err
	}
	h.ValueLen = uint32(vlen)

	meta, err := readVarint()
	if err != nil {
		return err
	}
	if meta > math.MaxUint8 {
		return fmt.Errorf("entry header meta overflow: %d", meta)
	}
	h.Meta = byte(meta)

	expiresAt, err := readVarint()
	if err != nil {
		return err
	}
	h.ExpiresAt = expiresAt
	return nil
}

// Encode serializes the header using uvarint encoding for each field.
func (h EntryHeader) Encode(out []byte) int {
	idx := 0
	idx = binary.PutUvarint(out[idx:], uint64(h.KeyLen))
	idx += binary.PutUvarint(out[idx:], uint64(h.ValueLen))
	idx += binary.PutUvarint(out[idx:], uint64(h.Meta))
	idx += binary.PutUvarint(out[idx:], h.ExpiresAt)
	return idx
}

// DecodeFrom consumes the header from a HashReader and reports the number of bytes read.
func (h *EntryHeader) DecodeFrom(reader *HashReader) (int, error) {
	start := reader.BytesRead
	readVarint := func() (uint64, error) {
		return binary.ReadUvarint(reader)
	}
	if err := h.decode(readVarint); err != nil {
		return reader.BytesRead - start, err
	}
	return reader.BytesRead - start, nil
}

// Decode parses the header directly from the provided byte slice.
func (h *EntryHeader) Decode(buf []byte) (int, error) {
	idx := 0
	readVarint := func() (uint64, error) {
		if idx >= len(buf) {
			return 0, io.ErrUnexpectedEOF
		}
		val, n := binary.Uvarint(buf[idx:])
		if n <= 0 {
			return 0, io.ErrUnexpectedEOF
		}
		idx += n
		return val, nil
	}
	if err := h.decode(readVarint); err != nil {
		return 0, err
	}
	return idx, nil
}

// EncodeEntry is a convenience wrapper around EncodeEntryTo that returns the
// encoded record bytes from a buffer.
func EncodeEntry(buf *bytes.Buffer, e *Entry) ([]byte, error) {
	if buf == nil {
		buf = &bytes.Buffer{}
	}
	buf.Reset()
	n, err := EncodeEntryTo(buf, e)
	if err != nil {
		return nil, err
	}
	out := buf.Bytes()
	if len(out) >= n {
		return out[:n], nil
	}
	dup := make([]byte, n)
	copy(dup, out)
	return dup, nil
}

// EncodeEntryTo is the core streaming encoder.
// It serializes the entry fields currently stored in e and writes them directly
// to w. The encoder treats e.Key and e.Value as raw bytes and does not validate
// whether e.Key is a user key, base key, or internal key. Callers that persist
// entries are responsible for supplying bytes in the correct internal format.
//
// The encoded layout is: | header | key | value | crc32 |
// - header: Varint-encoded, contains Key/Value lengths, Meta, and ExpiresAt.
// - crc32: 4 bytes, BigEndian, checksums the header, key, and value.
func EncodeEntryTo(w io.Writer, entry *Entry) (int, error) {
	header := EntryHeader{
		KeyLen:    uint32(len(entry.Key)),
		ValueLen:  uint32(len(entry.Value)),
		Meta:      entry.Meta,
		ExpiresAt: entry.ExpiresAt,
	}

	baseHeaderBuf := headerPool.Get().(*[]byte)
	headerBuf := (*baseHeaderBuf)[:MaxEntryHeaderSize]
	sz := header.Encode(headerBuf)
	if sz > len(headerBuf) {
		headerPool.Put(baseHeaderBuf)
		return 0, fmt.Errorf("entry header overflow: sz=%d cap=%d key=%d val=%d meta=%d expires=%d", sz, len(headerBuf), len(entry.Key), len(entry.Value), header.Meta, header.ExpiresAt)
	}
	headerBuf = headerBuf[:sz]

	crc := CRC32()
	defer PutCRC32(crc)
	if _, err := crc.Write(headerBuf); err != nil {
		headerPool.Put(baseHeaderBuf)
		return 0, err
	}

	total := 0
	write := func(b []byte) error {
		if len(b) == 0 {
			return nil
		}
		n, err := w.Write(b)
		if err != nil {
			return err
		}
		if n != len(b) {
			return io.ErrShortWrite
		}
		total += n
		return nil
	}
	writeSection := func(b []byte) error {
		if err := write(b); err != nil {
			return err
		}
		if _, err := crc.Write(b); err != nil {
			return err
		}
		return nil
	}

	if err := write(headerBuf); err != nil {
		headerPool.Put(baseHeaderBuf)
		return 0, err
	}
	headerPool.Put(baseHeaderBuf)
	if err := writeSection(entry.Key); err != nil {
		return 0, err
	}
	if err := writeSection(entry.Value); err != nil {
		return 0, err
	}

	var crcBuf [crc32.Size]byte
	binary.BigEndian.PutUint32(crcBuf[:], crc.Sum32())
	if err := write(crcBuf[:]); err != nil {
		return 0, err
	}

	return total, nil
}

// DecodeEntryFrom is the core streaming decoder.
// It reads one encoded entry record from r into an Entry object and performs a
// full CRC32 checksum verification. The decoder restores raw Key/Value bytes;
// callers that require richer semantics must interpret whether Entry.Key is a
// user key, base key, or internal key separately.
//
// It expects the data stream to have the layout: | header | key | value | crc32 |
//
// The returned Entry is sourced from an object pool and has a reference count of 1.
// The caller MUST call DecrRef() on the entry when it is no longer needed to return it
// to the pool and avoid memory leaks.
//
// In addition to the Entry, it also returns the total length of the record in the stream.
func DecodeEntryFrom(r io.Reader) (*Entry, uint32, error) {
	if r == nil {
		return nil, 0, errors.New("kv: decode entry from nil reader")
	}

	hashReader := NewHashReader(r)

	var header EntryHeader
	headerBytes, err := header.DecodeFrom(hashReader)
	if err != nil {
		switch {
		case errors.Is(err, io.EOF) && headerBytes == 0:
			return nil, 0, io.EOF
		case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
			return nil, 0, ErrPartialEntry
		default:
			return nil, 0, err
		}
	}

	keyLen := int(header.KeyLen)
	if keyLen < 0 || uint32(keyLen) != header.KeyLen {
		return nil, 0, ErrPartialEntry
	}
	valueLen := int(header.ValueLen)
	if valueLen < 0 || uint32(valueLen) != header.ValueLen {
		return nil, 0, ErrPartialEntry
	}

	entry := acquireEntry()
	entry.CF = CFDefault
	entry.Version = 0
	entry.ValThreshold = 0
	entry.Hlen = headerBytes
	entry.Offset = 0
	entry.Meta = header.Meta
	entry.ExpiresAt = header.ExpiresAt

	if cap(entry.Key) < keyLen {
		entry.Key = make([]byte, keyLen)
	} else {
		entry.Key = entry.Key[:keyLen]
	}
	if _, err := io.ReadFull(hashReader, entry.Key); err != nil {
		entry.DecrRef()
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, 0, ErrPartialEntry
		}
		return nil, 0, err
	}
	_ = entry.PopulateInternalMeta()

	if cap(entry.Value) < valueLen {
		entry.Value = make([]byte, valueLen)
	} else {
		entry.Value = entry.Value[:valueLen]
	}
	if _, err := io.ReadFull(hashReader, entry.Value); err != nil {
		entry.DecrRef()
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, 0, ErrPartialEntry
		}
		return nil, 0, err
	}

	var crcBuf [crc32.Size]byte
	if _, err := io.ReadFull(r, crcBuf[:]); err != nil {
		entry.DecrRef()
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, 0, ErrPartialEntry
		}
		return nil, 0, err
	}
	if BytesToU32(crcBuf[:]) != hashReader.Sum32() {
		entry.DecrRef()
		return nil, 0, ErrBadChecksum
	}

	recordLen := uint32(headerBytes) + uint32(keyLen) + uint32(valueLen) + crc32.Size
	return entry, recordLen, nil
}

// EstimateEncodeSize estimates the encoded size of an entry in WAL/SST streams.
func EstimateEncodeSize(e *Entry) int {
	return len(e.Key) + len(e.Value) + 8 /* ExpiresAt uint64 */ +
		crc32.Size + MaxEntryHeaderSize
}

// DecodeEntry is a convenience function that decodes a byte slice.
// It takes a byte slice containing a single record and decodes it into an Entry object.
//
// Internally, it wraps the []byte with a bytes.NewReader and calls the core
// DecodeEntryFrom function to perform the actual decoding and validation.
// This design follows the DRY principle by reusing the core decoding logic.
func DecodeEntry(data []byte) (*Entry, error) {
	reader := bytes.NewReader(data)
	entry, _, err := DecodeEntryFrom(reader) // Directly call, reuse core logic
	return entry, err
}

// DecodeValueSlice parses an encoded entry record and returns a slice referencing
// the encoded value.
// The returned slice aliases the provided data.
func DecodeValueSlice(data []byte) ([]byte, EntryHeader, error) {
	var header EntryHeader
	idx, err := header.Decode(data)
	if err != nil {
		return nil, EntryHeader{}, err
	}

	totalLen := int(header.KeyLen) + int(header.ValueLen)
	if totalLen < 0 {
		return nil, EntryHeader{}, io.ErrUnexpectedEOF
	}
	payloadEnd := idx + totalLen
	checksumEnd := payloadEnd + crc32.Size
	if payloadEnd < idx || checksumEnd > len(data) {
		return nil, EntryHeader{}, io.ErrUnexpectedEOF
	}

	expected := binary.BigEndian.Uint32(data[payloadEnd:checksumEnd])
	actual := crc32.Checksum(data[:payloadEnd], CastagnoliCrcTable)
	if expected != actual {
		return nil, EntryHeader{}, ErrBadChecksum
	}

	valueStart := idx + int(header.KeyLen)
	return data[valueStart:payloadEnd], header, nil
}
