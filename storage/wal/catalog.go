// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"

	kv "github.com/feichai0017/NoKV/txn/storage"
)

var catalogMagic = [8]byte{'N', 'O', 'K', 'V', 'I', 'D', 'X', 1}

type catalogEntry struct {
	Type   RecordType
	Group  uint64
	Offset int64
	Length uint32
}

func (m *Manager) catalogPath(id uint32) string {
	return m.segmentPath(id) + ".idx"
}

func (m *Manager) loadSegmentCatalog(id uint32) ([]catalogEntry, error) {
	data, err := m.cfg.FS.ReadFile(m.catalogPath(id))
	if err != nil {
		return nil, err
	}
	entries, err := decodeSegmentCatalog(data)
	if err != nil {
		return nil, err
	}
	info, err := m.cfg.FS.Stat(m.segmentPath(id))
	if err != nil {
		return nil, err
	}
	if catalogEnd(entries) != info.Size() {
		return nil, fmt.Errorf("wal: stale catalog for segment %d", id)
	}
	return entries, nil
}

func (m *Manager) writeSegmentCatalog(id uint32, entries []catalogEntry) error {
	data := encodeSegmentCatalog(entries)
	return m.cfg.FS.WriteFile(m.catalogPath(id), data, m.cfg.FileMode)
}

func (m *Manager) rebuildSegmentCatalog(id uint32, path string) ([]catalogEntry, error) {
	f, err := m.cfg.FS.OpenHandle(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	return m.scanAndWriteSegmentCatalog(id, f)
}

func (m *Manager) scanAndWriteSegmentCatalog(id uint32, r io.Reader) ([]catalogEntry, error) {
	reIter := NewRecordIterator(r, m.bufferSize)
	defer func() { _ = reIter.Close() }()

	var (
		offset  int64
		entries []catalogEntry
	)
	for reIter.Next() {
		length := reIter.Length()
		groupID, err := recordGroupID(reIter.Type(), reIter.Record())
		if err != nil {
			return nil, err
		}
		entries = append(entries, catalogEntry{
			Type:   reIter.Type(),
			Group:  groupID,
			Offset: offset,
			Length: length,
		})
		offset += int64(length) + 8
	}

	switch err := reIter.Err(); err {
	case nil, io.EOF, ErrPartialRecord:
	case kv.ErrBadChecksum:
		return nil, fmt.Errorf("wal: checksum mismatch segment=%d offset=%d", id, offset)
	default:
		return nil, err
	}
	if err := m.writeSegmentCatalog(id, entries); err != nil {
		return nil, err
	}
	return entries, nil
}

func (m *Manager) replayIndexedFile(path string, entries []catalogEntry, filter func(EntryInfo) bool, fn func(info EntryInfo, payload []byte) error) error {
	f, err := m.cfg.FS.OpenHandle(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	for _, entry := range entries {
		info := EntryInfo{
			SegmentID: segmentIDFromPath(path),
			Offset:    entry.Offset,
			Length:    entry.Length,
			Type:      entry.Type,
			GroupID:   entry.Group,
		}
		if filter != nil && !filter(info) {
			continue
		}
		if _, err := f.Seek(entry.Offset, io.SeekStart); err != nil {
			return err
		}
		recType, payload, length, err := DecodeRecord(f)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, ErrPartialRecord) {
				return errStaleCatalog
			}
			return err
		}
		groupID, err := recordGroupID(recType, payload)
		if err != nil {
			return err
		}
		if recType != entry.Type || length != entry.Length || groupID != entry.Group {
			return errStaleCatalog
		}
		info.Length = length
		if err := fn(info, payload); err != nil {
			return err
		}
	}
	return nil
}

func encodeSegmentCatalog(entries []catalogEntry) []byte {
	var buf bytes.Buffer
	_, _ = buf.Write(catalogMagic[:])
	var body bytes.Buffer
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(entries)))
	_, _ = body.Write(header[:])
	for _, entry := range entries {
		_ = body.WriteByte(byte(entry.Type))
		var tmp [20]byte
		binary.BigEndian.PutUint64(tmp[:8], entry.Group)
		binary.BigEndian.PutUint64(tmp[8:16], uint64(entry.Offset))
		binary.BigEndian.PutUint32(tmp[16:], entry.Length)
		_, _ = body.Write(tmp[:])
	}
	bodyBytes := body.Bytes()
	_, _ = buf.Write(bodyBytes)
	var crc [4]byte
	binary.BigEndian.PutUint32(crc[:], crc32.ChecksumIEEE(bodyBytes))
	_, _ = buf.Write(crc[:])
	return buf.Bytes()
}

func decodeSegmentCatalog(data []byte) ([]catalogEntry, error) {
	if len(data) < len(catalogMagic)+4+4 {
		return nil, io.ErrUnexpectedEOF
	}
	if !bytes.Equal(data[:len(catalogMagic)], catalogMagic[:]) {
		return nil, fmt.Errorf("wal: invalid catalog magic")
	}
	body := data[len(catalogMagic) : len(data)-4]
	gotCRC := binary.BigEndian.Uint32(data[len(data)-4:])
	if crc32.ChecksumIEEE(body) != gotCRC {
		return nil, fmt.Errorf("wal: catalog checksum mismatch")
	}
	count := binary.BigEndian.Uint32(body[:4])
	rest := body[4:]
	if uint64(count) > uint64(len(rest))/21 {
		return nil, io.ErrUnexpectedEOF
	}
	if len(rest) != int(count)*21 {
		return nil, io.ErrUnexpectedEOF
	}
	entries := make([]catalogEntry, 0, count)
	for range count {
		entry := catalogEntry{
			Type:   RecordType(rest[0]),
			Group:  binary.BigEndian.Uint64(rest[1:9]),
			Offset: int64(binary.BigEndian.Uint64(rest[9:17])),
			Length: binary.BigEndian.Uint32(rest[17:21]),
		}
		if entry.Offset < 0 || entry.Length == 0 {
			return nil, fmt.Errorf("wal: invalid catalog entry")
		}
		entries = append(entries, entry)
		rest = rest[21:]
	}
	return entries, nil
}

func catalogEnd(entries []catalogEntry) int64 {
	if len(entries) == 0 {
		return 0
	}
	last := entries[len(entries)-1]
	return last.Offset + int64(last.Length) + 8
}

func segmentIDFromPath(path string) uint32 {
	var id uint32
	_, _ = fmt.Sscanf(filepath.Base(path), "%05d.wal", &id)
	return id
}

func recordGroupID(recType RecordType, payload []byte) (uint64, error) {
	switch recType {
	case RecordTypeRaftEntry, RecordTypeRaftState, RecordTypeRaftSnapshot:
		groupID, n := binary.Uvarint(payload)
		if n <= 0 {
			return 0, fmt.Errorf("wal: malformed raft record group id")
		}
		return groupID, nil
	default:
		return 0, nil
	}
}
