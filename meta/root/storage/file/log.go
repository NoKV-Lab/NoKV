// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package file

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/feichai0017/NoKV/storage/vfs"
	"google.golang.org/protobuf/proto"
)

const recordHeaderSize = 24

type fileEventLog struct {
	fs      vfs.FS
	workdir string
}

// ReadCommitted reads the retained rooted event WAL from root.events.wal.
//
// On-disk record format:
//   - term        uint64 little-endian
//   - index       uint64 little-endian
//   - payload_len uint32 little-endian
//   - payload_crc uint32 little-endian
//   - payload     []byte protobuf(metapb.RootEvent)
//
// A short trailing record is treated as a torn tail and ignored so recovery can
// continue from the last fully published committed record.
func (l fileEventLog) ReadCommitted(offset int64) (rootstorage.CommittedTail, error) {
	path := filepath.Join(l.workdir, LogFileName)
	f, err := l.fs.OpenHandle(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return rootstorage.CommittedTail{RequestedOffset: offset, StartOffset: offset, EndOffset: offset}, nil
		}
		return rootstorage.CommittedTail{}, err
	}
	defer func() { _ = f.Close() }()
	if offset > 0 {
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			return rootstorage.CommittedTail{}, err
		}
	}
	var out []rootstorage.CommittedEvent
	for {
		rec, ok, err := readRecord(f)
		if err != nil {
			return rootstorage.CommittedTail{}, err
		}
		if !ok {
			end, err := fileSize(f)
			if err != nil {
				return rootstorage.CommittedTail{}, err
			}
			return rootstorage.CommittedTail{
				RequestedOffset: offset,
				StartOffset:     offset,
				EndOffset:       end,
				Records:         out,
			}, nil
		}
		out = append(out, rec)
	}
}

// AppendCommitted appends one or more committed rooted events to root.events.wal
// and fsyncs the file before reporting the new end offset.
func (l fileEventLog) AppendCommitted(records ...rootstorage.CommittedEvent) (int64, error) {
	path := filepath.Join(l.workdir, LogFileName)
	f, err := l.fs.OpenFileHandle(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return 0, err
	}
	for _, rec := range records {
		if err := writeRecord(f, rec); err != nil {
			_ = f.Close()
			return 0, err
		}
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return 0, err
	}
	logEnd, err := fileSize(f)
	if err != nil {
		_ = f.Close()
		return 0, err
	}
	if err := f.Close(); err != nil {
		return 0, err
	}
	return logEnd, nil
}

// CompactCommitted rewrites the retained committed window into a fresh WAL file
// and atomically publishes it. This is how the file backend materializes
// retention/compaction after old committed records have fallen behind the
// checkpoint.
func (l fileEventLog) CompactCommitted(stream rootstorage.CommittedTail) error {
	path := filepath.Join(l.workdir, LogFileName)
	tmp := path + ".tmp"
	f, err := l.fs.OpenFileHandle(tmp, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	for _, rec := range stream.Records {
		if err := writeRecord(f, rec); err != nil {
			_ = f.Close()
			_ = l.fs.Remove(tmp)
			return err
		}
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = l.fs.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = l.fs.Remove(tmp)
		return err
	}
	if err := l.fs.Rename(tmp, path); err != nil {
		return err
	}
	return vfs.SyncDir(l.fs, l.workdir)
}

func (l fileEventLog) Size() (int64, error) {
	info, err := l.fs.Stat(filepath.Join(l.workdir, LogFileName))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	return info.Size(), nil
}

// writeRecord writes one framed committed rooted event to the WAL.
func writeRecord(w io.Writer, rec rootstorage.CommittedEvent) error {
	payload, err := proto.Marshal(metawire.RootEventToProto(rec.Event))
	if err != nil {
		return err
	}
	hdr := make([]byte, recordHeaderSize)
	binary.LittleEndian.PutUint64(hdr[0:8], rec.Cursor.Term)
	binary.LittleEndian.PutUint64(hdr[8:16], rec.Cursor.Index)
	binary.LittleEndian.PutUint32(hdr[16:20], uint32(len(payload)))
	binary.LittleEndian.PutUint32(hdr[20:24], crc32.ChecksumIEEE(payload))
	if err := writeAll(w, hdr); err != nil {
		return err
	}
	return writeAll(w, payload)
}

// readRecord decodes one framed committed rooted event from the WAL.
func readRecord(r io.Reader) (rootstorage.CommittedEvent, bool, error) {
	hdr := make([]byte, recordHeaderSize)
	n, err := io.ReadFull(r, hdr)
	if err != nil {
		if errors.Is(err, io.EOF) && n == 0 {
			return rootstorage.CommittedEvent{}, false, nil
		}
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return rootstorage.CommittedEvent{}, false, nil
		}
		return rootstorage.CommittedEvent{}, false, err
	}
	payloadLen := binary.LittleEndian.Uint32(hdr[16:20])
	expectedCRC := binary.LittleEndian.Uint32(hdr[20:24])
	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return rootstorage.CommittedEvent{}, false, nil
		}
		return rootstorage.CommittedEvent{}, false, err
	}
	if crc32.ChecksumIEEE(payload) != expectedCRC {
		return rootstorage.CommittedEvent{}, false, fmt.Errorf("meta/root/storage/file: root log checksum mismatch")
	}
	var pbEvent metapb.RootEvent
	if err := proto.Unmarshal(payload, &pbEvent); err != nil {
		return rootstorage.CommittedEvent{}, false, err
	}
	return rootstorage.CommittedEvent{
		Cursor: rootstate.Cursor{
			Term:  binary.LittleEndian.Uint64(hdr[0:8]),
			Index: binary.LittleEndian.Uint64(hdr[8:16]),
		},
		Event: metawire.RootEventFromProto(&pbEvent),
	}, true, nil
}

func writeAll(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}
