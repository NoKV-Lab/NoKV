// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"hash/crc32"
	"io"
)

// HashReader wraps an io.Reader and keeps track of the CRC32 checksum and bytes read.
type HashReader struct {
	R         io.Reader
	H         hash32
	BytesRead int // Number of bytes read.
	tmp       [1]byte
}

type hash32 interface {
	Write(p []byte) (int, error)
	Sum32() uint32
}

// NewHashReader creates a HashReader backed by Castagnoli CRC32.
func NewHashReader(r io.Reader) *HashReader {
	hash := crc32.New(CastagnoliCrcTable)
	return &HashReader{
		R: r,
		H: hash,
	}
}

// Read reads len(p) bytes from the reader. Returns the number of bytes read, error on failure.
func (t *HashReader) Read(p []byte) (int, error) {
	n, err := t.R.Read(p)
	if err != nil {
		return n, err
	}
	t.BytesRead += n
	return t.H.Write(p[:n])
}

// ReadByte reads exactly one byte from the reader. Returns error on failure.
func (t *HashReader) ReadByte() (byte, error) {
	_, err := t.Read(t.tmp[:])
	return t.tmp[0], err
}

// Sum32 returns the sum32 of the underlying hash.
func (t *HashReader) Sum32() uint32 {
	return t.H.Sum32()
}
