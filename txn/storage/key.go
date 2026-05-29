// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"unsafe"
)

type stringStruct struct {
	str unsafe.Pointer
	len int
}

// MaxVersion is the canonical upper-bound MVCC version sentinel.
const MaxVersion uint64 = math.MaxUint64

//go:noescape
//go:linkname memhash runtime.memhash
func memhash(p unsafe.Pointer, h, s uintptr) uintptr

// InternalToBaseKey removes the trailing 8-byte MVCC timestamp suffix from an
// canonical internal key and returns the corresponding base key (CF+userKey).
// For non-internal keys it returns the input unchanged.
func InternalToBaseKey(internalKey []byte) []byte {
	if len(internalKey) < 8 {
		return internalKey
	}
	base := internalKey[:len(internalKey)-8]
	if _, _, ok := SplitBaseKey(base); ok {
		return base
	}
	return internalKey
}

// BaseKey assembles a base key from (column family, user key) without an MVCC
// timestamp suffix.
func BaseKey(cf ColumnFamily, userKey []byte) []byte {
	if !cf.Valid() {
		cf = CFDefault
	}
	out := make([]byte, cfHeaderSize+len(userKey))
	out[0] = cfMarker0
	out[1] = cfMarker1
	out[2] = cfMarker2
	out[3] = byte(cf)
	copy(out[cfHeaderSize:], userKey)
	return out
}

// SplitBaseKey returns the column family and user key from a base key
// (CF marker + user key, without MVCC timestamp).
func SplitBaseKey(baseKey []byte) (ColumnFamily, []byte, bool) {
	if len(baseKey) >= cfHeaderSize &&
		baseKey[0] == cfMarker0 &&
		baseKey[1] == cfMarker1 &&
		baseKey[2] == cfMarker2 {
		cf := ColumnFamily(baseKey[3])
		if cf.Valid() {
			return cf, baseKey[cfHeaderSize:], true
		}
	}
	return CFDefault, baseKey, false
}

// Timestamp decodes the MVCC timestamp from the trailing 8-byte suffix.
// It returns 0 when the key is too short to carry a timestamp.
func Timestamp(internalKey []byte) uint64 {
	if len(internalKey) <= 8 {
		return 0
	}
	return math.MaxUint64 - binary.BigEndian.Uint64(internalKey[len(internalKey)-8:])
}

// SameBaseKey checks for key equality ignoring the MVCC timestamp suffix.
func SameBaseKey(src, dst []byte) bool {
	return bytes.Equal(InternalToBaseKey(src), InternalToBaseKey(dst))
}

// CompareInternalKeys compares two canonical internal keys.
func CompareInternalKeys(key1, key2 []byte) int {
	if len(key1) <= 8 || len(key2) <= 8 {
		panic(fmt.Sprintf("%s,%s < 8", string(key1), string(key2)))
	}
	if cmp := bytes.Compare(key1[:len(key1)-8], key2[:len(key2)-8]); cmp != 0 {
		return cmp
	}
	return bytes.Compare(key1[len(key1)-8:], key2[len(key2)-8:])
}

// CompareBaseKeys compares the CF+user-key portions of two keys, ignoring MVCC timestamp.
func CompareBaseKeys(key1, key2 []byte) int {
	return bytes.Compare(InternalToBaseKey(key1), InternalToBaseKey(key2))
}

// CompareBaseKeysAssumeValid compares the CF+user-key portions of two
// internal keys without validating the CF marker. Both arguments MUST be
// canonical internal keys (length > 8, produced by InternalKey or decoded from
// a trusted entry stream). Use CompareBaseKeys for untrusted input. Hot paths
// that have already established the input is internal should prefer this
// variant: profiles show the validation overhead in InternalToBaseKey +
// SplitBaseKey can dominate the actual memcmp it wraps.
func CompareBaseKeysAssumeValid(key1, key2 []byte) int {
	return bytes.Compare(key1[:len(key1)-8], key2[:len(key2)-8])
}

// CompareUserKeys compares pure user-key portions of two internal keys.
// Both inputs must use the InternalKey layout.
func CompareUserKeys(key1, key2 []byte) int {
	if len(key1) == 0 || len(key2) == 0 {
		return bytes.Compare(key1, key2)
	}
	_, uk1, _, ok1 := SplitInternalKey(key1)
	_, uk2, _, ok2 := SplitInternalKey(key2)
	if !ok1 || !ok2 {
		panic(fmt.Sprintf("CompareUserKeys requires internal keys (ok1=%t ok2=%t)", ok1, ok2))
	}
	return bytes.Compare(uk1, uk2)
}

// InternalKey encodes (column family, user key, version) into the canonical
// internal layout used above ordered-KV backends:
//
//	+------------+----------+----------------------+
//	| CF marker  | User key | Timestamp (uint64 BE)|
//	+------------+----------+----------------------+
//	| 4 bytes    | raw      | 8 bytes (descending) |
//	+------------+----------+----------------------+
//
// CF marker uses 3 fixed bytes (0xFF,'C','F') plus the CF byte. Timestamp is
// bitwise inverted (^ts) so that newer versions sort before older ones.
func InternalKey(cf ColumnFamily, userKey []byte, version uint64) []byte {
	if !cf.Valid() {
		cf = CFDefault
	}
	out := make([]byte, cfHeaderSize+len(userKey)+8)
	out[0] = cfMarker0
	out[1] = cfMarker1
	out[2] = cfMarker2
	out[3] = byte(cf)
	copy(out[cfHeaderSize:], userKey)
	binary.BigEndian.PutUint64(out[len(out)-8:], math.MaxUint64-version)
	return out
}

// SplitInternalKey decodes an internal key into (cf, userKey, ts).
// It returns ok=false when the key does not carry a valid CF marker + timestamp layout.
func SplitInternalKey(internalKey []byte) (ColumnFamily, []byte, uint64, bool) {
	if len(internalKey) <= 8 {
		return CFDefault, nil, 0, false
	}
	baseKey := InternalToBaseKey(internalKey)
	ts := Timestamp(internalKey)
	cf, userKey, ok := SplitBaseKey(baseKey)
	if !ok {
		return CFDefault, nil, 0, false
	}
	return cf, userKey, ts, true
}

// MemHash is the hash function used by go map, it utilizes available hardware instructions.
// NOTE: The hash seed changes for every process. So, this cannot be used as a persistent hash.
func MemHash(data []byte) uint64 {
	ss := (*stringStruct)(unsafe.Pointer(&data))
	return uint64(memhash(ss.str, 0, uintptr(ss.len)))
}

// SafeCopy does append(a[:0], src...).
func SafeCopy(a, src []byte) []byte {
	return append(a[:0], src...)
}
