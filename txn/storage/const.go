// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"encoding/binary"
	"hash/crc32"
)

const (
	// BitDelete marks an entry as a deletion tombstone.
	BitDelete byte = 1 << 0
	// BitRangeDelete marks an entry as a range tombstone.
	BitRangeDelete byte = 1 << 1

	// MaxEntryHeaderSize defines the maximum number of bytes required to encode an EntryHeader.
	MaxEntryHeaderSize int = 4 * binary.MaxVarintLen64
)

var (
	// CastagnoliCrcTable is the shared CRC32 polynomial used across entry streams.
	CastagnoliCrcTable = crc32.MakeTable(crc32.Castagnoli)
)
