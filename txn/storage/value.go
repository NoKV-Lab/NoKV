// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"encoding/binary"
	"unsafe"
)

// ValueStruct is the serialized form of an inline value used inside Entry.Value
// and WAL payloads.
//
// Binary layout (EncodeValue):
//
//	+------+-------------+-------------+
//	| Meta | ExpiresAt   | Value bytes |
//	+------+-------------+-------------+
//	| 1B   | varint(u64) | raw payload |
//	+------+-------------+-------------+
//
// Meta bits include deletion and range tombstone flags. User data is stored
// inline in the value payload.
type ValueStruct struct {
	Meta      byte
	Value     []byte
	ExpiresAt uint64
}

// EncodedSize returns the size of the encoded value structure.
func (vs *ValueStruct) EncodedSize() uint32 {
	sz := len(vs.Value) + 1 // meta
	enc := sizeVarint(vs.ExpiresAt)
	return uint32(sz + enc)
}

// DecodeValue decodes the provided buffer into the value structure.
func (vs *ValueStruct) DecodeValue(buf []byte) {
	vs.Meta = buf[0]
	var sz int
	vs.ExpiresAt, sz = binary.Uvarint(buf[1:])
	vs.Value = buf[1+sz:]
}

// EncodeValue encodes the value structure into the provided buffer and returns the bytes written.
func (vs *ValueStruct) EncodeValue(b []byte) uint32 {
	b[0] = vs.Meta
	sz := binary.PutUvarint(b[1:], vs.ExpiresAt)
	n := copy(b[1+sz:], vs.Value)
	return uint32(1 + sz + n)
}

func sizeVarint(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}

// BytesToU32 converts the given byte slice to uint32
func BytesToU32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}

// BytesToU64 converts the given byte slice to uint64
func BytesToU64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// U32SliceToBytes converts the given Uint32 slice to byte slice
func U32SliceToBytes(u32s []uint32) []byte {
	if len(u32s) == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(&u32s[0])), len(u32s)*4)
}

// U32ToBytes converts the given Uint32 to bytes
func U32ToBytes(v uint32) []byte {
	var uBuf [4]byte
	binary.BigEndian.PutUint32(uBuf[:], v)
	return uBuf[:]
}

// U64ToBytes converts the given Uint64 to bytes
func U64ToBytes(v uint64) []byte {
	var uBuf [8]byte
	binary.BigEndian.PutUint64(uBuf[:], v)
	return uBuf[:]
}

// BytesToU32Slice converts the given byte slice to uint32 slice
func BytesToU32Slice(b []byte) []uint32 {
	if len(b) == 0 {
		return nil
	}
	return unsafe.Slice((*uint32)(unsafe.Pointer(&b[0])), len(b)/4)
}
