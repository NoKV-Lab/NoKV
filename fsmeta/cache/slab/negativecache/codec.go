// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package negativecache

import (
	"encoding/binary"
	"hash/crc32"
)

// Wire format for the slab snapshot.
//
//	magic      uint32 ("NCSL", little-endian)
//	version    uint16
//	entries    repeated of:
//	    len      uvarint   (length of full key in bytes)
//	    key      [len]byte
//	terminator zero-length record (uvarint 0)
//	crc32      uint32 little-endian over entries + terminator
//
// Reload tolerates missing, truncated, or bad snapshots by restoring no more
// than the valid prefix. Negative cache entries are derived hints, so the safe
// failure mode is a cold cache and an authoritative re-probe.
const (
	snapshotMagic   uint32 = 0x4e43534c // "NCSL"
	snapshotVersion uint16 = 1
	snapshotHeader         = 4 + 2 // magic + version
	snapshotTrailer        = 1 + 4 // zero terminator + crc32
	snapshotFile           = "negative.slab"
)

func encodeSnapshotKeys(keys [][]byte, maxSize int64) ([]byte, int) {
	minSize := int64(snapshotHeader + snapshotTrailer)
	if maxSize < minSize {
		maxSize = minSize
	}
	buf := make([]byte, snapshotHeader, min(int(maxSize), snapshotHeader+len(keys)*32))
	binary.LittleEndian.PutUint32(buf[0:4], snapshotMagic)
	binary.LittleEndian.PutUint16(buf[4:6], snapshotVersion)

	hasher := crc32.NewIEEE()
	written := 0
	var lenBuf [binary.MaxVarintLen64]byte
	for _, key := range keys {
		n := binary.PutUvarint(lenBuf[:], uint64(len(key)))
		if int64(len(buf))+int64(n)+int64(len(key))+int64(snapshotTrailer) > maxSize {
			break
		}
		buf = append(buf, lenBuf[:n]...)
		hasher.Write(lenBuf[:n])
		buf = append(buf, key...)
		hasher.Write(key)
		written++
	}

	buf = append(buf, 0)
	hasher.Write([]byte{0})
	var crcBuf [4]byte
	binary.LittleEndian.PutUint32(crcBuf[:], hasher.Sum32())
	buf = append(buf, crcBuf[:]...)
	return buf, written
}

func decodeSnapshotKeys(body []byte) ([][]byte, bool) {
	if len(body) < snapshotHeader {
		return nil, false
	}
	if magic := binary.LittleEndian.Uint32(body[0:4]); magic != snapshotMagic {
		return nil, false
	}
	if ver := binary.LittleEndian.Uint16(body[4:6]); ver != snapshotVersion {
		return nil, false
	}

	hasher := crc32.NewIEEE()
	cursor := snapshotHeader
	var keys [][]byte
	for cursor < len(body) {
		klen, n := binary.Uvarint(body[cursor:])
		if n <= 0 {
			return keys, true
		}
		hasher.Write(body[cursor : cursor+n])
		cursor += n
		if klen == 0 {
			if cursor+4 > len(body) {
				return keys, true
			}
			want := binary.LittleEndian.Uint32(body[cursor : cursor+4])
			if want != hasher.Sum32() {
				return nil, false
			}
			return keys, true
		}
		if klen > uint64(len(body)-cursor) {
			return keys, true
		}
		end := cursor + int(klen)
		key := make([]byte, int(klen))
		copy(key, body[cursor:end])
		hasher.Write(body[cursor:end])
		keys = append(keys, key)
		cursor = end
	}
	return keys, true
}
