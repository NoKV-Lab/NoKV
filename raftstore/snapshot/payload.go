// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"time"

	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	txnstore "github.com/feichai0017/NoKV/txn/storage"
)

var payloadMagic = []byte{'N', 'o', 'K', 'V', 'R', 'S', 'N', 'P'}

const payloadVersion uint32 = 1

type payloadHeader struct {
	Version    uint32               `json:"version"`
	Format     Format               `json:"format"`
	Region     localmeta.RegionMeta `json:"region"`
	EntryCount uint64               `json:"entry_count"`
	CreatedAt  time.Time            `json:"created_at"`
}

// WritePayloadTo writes one backend-neutral region snapshot payload.
func WritePayloadTo(w io.Writer, region localmeta.RegionMeta, entries []*txnstore.Entry) (Descriptor, error) {
	if w == nil {
		return Descriptor{}, fmt.Errorf("snapshot: payload writer is nil")
	}
	descriptor := Descriptor{
		Format:     FormatEntries,
		Region:     localmeta.CloneRegionMeta(region),
		EntryCount: uint64(len(entries)),
		CreatedAt:  time.Now().UTC(),
	}
	header := payloadHeader{
		Version:    payloadVersion,
		Format:     descriptor.Format,
		Region:     descriptor.Region,
		EntryCount: descriptor.EntryCount,
		CreatedAt:  descriptor.CreatedAt,
	}
	headerBytes, err := json.Marshal(header)
	if err != nil {
		return Descriptor{}, fmt.Errorf("snapshot: encode payload header: %w", err)
	}
	if _, err := w.Write(payloadMagic); err != nil {
		return Descriptor{}, fmt.Errorf("snapshot: write payload magic: %w", err)
	}
	var fixed [4]byte
	binary.BigEndian.PutUint32(fixed[:], payloadVersion)
	if _, err := w.Write(fixed[:]); err != nil {
		return Descriptor{}, fmt.Errorf("snapshot: write payload version: %w", err)
	}
	var lenBuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lenBuf[:], uint64(len(headerBytes)))
	if _, err := w.Write(lenBuf[:n]); err != nil {
		return Descriptor{}, fmt.Errorf("snapshot: write payload header length: %w", err)
	}
	if _, err := w.Write(headerBytes); err != nil {
		return Descriptor{}, fmt.Errorf("snapshot: write payload header: %w", err)
	}
	for _, entry := range entries {
		if entry == nil {
			continue
		}
		if _, err := txnstore.EncodeEntryTo(w, entry); err != nil {
			return Descriptor{}, fmt.Errorf("snapshot: encode payload entry: %w", err)
		}
	}
	return descriptor, nil
}

// ReadPayloadMeta decodes only the region metadata from a snapshot payload.
func ReadPayloadMeta(payload []byte) (Descriptor, error) {
	if len(payload) == 0 {
		return Descriptor{}, fmt.Errorf("snapshot: empty entry payload")
	}
	return ReadPayloadMetaFrom(bytes.NewReader(payload))
}

// ReadPayloadMetaFrom decodes only the region metadata from a snapshot payload.
func ReadPayloadMetaFrom(r io.Reader) (Descriptor, error) {
	header, _, err := readPayloadHeader(r)
	if err != nil {
		return Descriptor{}, err
	}
	return header.descriptor(), nil
}

// ReadPayloadFrom decodes a full snapshot payload into detached internal
// entries. The caller owns the returned entries and must release them.
func ReadPayloadFrom(r io.Reader) (Descriptor, []*txnstore.Entry, error) {
	header, br, err := readPayloadHeader(r)
	if err != nil {
		return Descriptor{}, nil, err
	}
	iter := txnstore.NewEntryIterator(br)
	defer func() { _ = iter.Close() }()
	entries := make([]*txnstore.Entry, 0, header.EntryCount)
	for iter.Next() {
		src := iter.Entry()
		if src == nil {
			continue
		}
		cf, userKey, version, ok := txnstore.SplitInternalKey(src.Key)
		if !ok {
			releaseEntries(entries)
			return Descriptor{}, nil, fmt.Errorf("snapshot: payload contains invalid internal key")
		}
		entry := txnstore.NewInternalEntry(
			cf,
			txnstore.SafeCopy(nil, userKey),
			version,
			txnstore.SafeCopy(nil, src.Value),
			src.Meta,
			src.ExpiresAt,
		)
		entries = append(entries, entry)
	}
	if err := iter.Err(); err != nil && err != io.EOF {
		releaseEntries(entries)
		return Descriptor{}, nil, fmt.Errorf("snapshot: decode payload entries: %w", err)
	}
	if uint64(len(entries)) != header.EntryCount {
		releaseEntries(entries)
		return Descriptor{}, nil, fmt.Errorf("snapshot: entry count mismatch header=%d decoded=%d", header.EntryCount, len(entries))
	}
	return header.descriptor(), entries, nil
}

func readPayloadHeader(r io.Reader) (payloadHeader, *bufio.Reader, error) {
	if r == nil {
		return payloadHeader{}, nil, fmt.Errorf("snapshot: payload reader is nil")
	}
	br := bufio.NewReader(r)
	magic := make([]byte, len(payloadMagic))
	if _, err := io.ReadFull(br, magic); err != nil {
		return payloadHeader{}, nil, fmt.Errorf("snapshot: read payload magic: %w", err)
	}
	if !bytes.Equal(magic, payloadMagic) {
		return payloadHeader{}, nil, fmt.Errorf("snapshot: invalid payload magic")
	}
	var fixed [4]byte
	if _, err := io.ReadFull(br, fixed[:]); err != nil {
		return payloadHeader{}, nil, fmt.Errorf("snapshot: read payload version: %w", err)
	}
	version := binary.BigEndian.Uint32(fixed[:])
	if version != payloadVersion {
		return payloadHeader{}, nil, fmt.Errorf("snapshot: unsupported payload version %d", version)
	}
	headerLen, err := binary.ReadUvarint(br)
	if err != nil {
		return payloadHeader{}, nil, fmt.Errorf("snapshot: read payload header length: %w", err)
	}
	if headerLen == 0 || headerLen > 4<<20 {
		return payloadHeader{}, nil, fmt.Errorf("snapshot: invalid payload header length %d", headerLen)
	}
	headerBytes := make([]byte, headerLen)
	if _, err := io.ReadFull(br, headerBytes); err != nil {
		return payloadHeader{}, nil, fmt.Errorf("snapshot: read payload header: %w", err)
	}
	var header payloadHeader
	if err := json.Unmarshal(headerBytes, &header); err != nil {
		return payloadHeader{}, nil, fmt.Errorf("snapshot: decode payload header: %w", err)
	}
	if header.Version != payloadVersion {
		return payloadHeader{}, nil, fmt.Errorf("snapshot: unsupported header version %d", header.Version)
	}
	if header.Format != FormatEntries {
		return payloadHeader{}, nil, fmt.Errorf("snapshot: unsupported payload format %q", header.Format)
	}
	if header.Region.ID == 0 {
		return payloadHeader{}, nil, fmt.Errorf("snapshot: payload missing region metadata")
	}
	return header, br, nil
}

func (h payloadHeader) descriptor() Descriptor {
	return Descriptor{
		Format:     h.Format,
		Region:     localmeta.CloneRegionMeta(h.Region),
		EntryCount: h.EntryCount,
		CreatedAt:  h.CreatedAt,
	}
}
