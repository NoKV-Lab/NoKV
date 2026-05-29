// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package wal_test

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/feichai0017/NoKV/storage/vfs"
	"github.com/feichai0017/NoKV/storage/wal"
	kv "github.com/feichai0017/NoKV/txn/storage"
)

func appendEntryValue(t *testing.T, m *wal.Manager, key, value string) wal.EntryInfo {
	t.Helper()
	entry := kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte(key), 1), []byte(value))
	defer entry.DecrRef()
	info, err := m.AppendEntry(wal.DurabilityBuffered, entry)
	if err != nil {
		t.Fatalf("append entry: %v", err)
	}
	return info
}

func TestManagerOpenWithFaultFS(t *testing.T) {
	dir := t.TempDir()
	injected := errors.New("mkdir fail")
	policy := vfs.NewFaultPolicy(vfs.FailOnceRule(vfs.OpMkdirAll, "", injected))
	fs := vfs.NewFaultFSWithPolicy(vfs.OSFS{}, policy)

	_, err := wal.Open(wal.Config{Dir: dir, FS: fs})
	if !errors.Is(err, injected) {
		t.Fatalf("expected injected error, got %v", err)
	}
}

func TestManagerOpenRequiresDir(t *testing.T) {
	if _, err := wal.Open(wal.Config{}); err == nil {
		t.Fatalf("expected missing dir to return error")
	}
}

func TestManagerOpenClampsSmallSegmentSize(t *testing.T) {
	dir := t.TempDir()
	m, err := wal.Open(wal.Config{Dir: dir, SegmentSize: 1})
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer func() { _ = m.Close() }()

	appendEntryValue(t, m, "k1", "v1")
	appendEntryValue(t, m, "k2", "v2")
	if err := m.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}

	files, err := m.ListSegments()
	if err != nil {
		t.Fatalf("list segments: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("expected normalized segment size to keep two small entries in one segment, got %d segments", len(files))
	}
	if got := m.ActiveSegment(); got != 1 {
		t.Fatalf("expected active segment 1, got %d", got)
	}
}

func TestManagerCloseReturnsSyncFailure(t *testing.T) {
	dir := t.TempDir()
	segmentPath := filepath.Join(dir, "00001.wal")
	injected := errors.New("sync fail")
	policy := vfs.NewFaultPolicy(vfs.FailOnceRule(vfs.OpFileSync, segmentPath, injected))
	fs := vfs.NewFaultFSWithPolicy(vfs.OSFS{}, policy)

	m, err := wal.Open(wal.Config{Dir: dir, FS: fs})
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	appendEntryValue(t, m, "k", "x")

	err = m.Close()
	if !errors.Is(err, injected) {
		t.Fatalf("expected sync failure, got %v", err)
	}
}

func TestManagerCloseRetriesAfterCloseFailure(t *testing.T) {
	dir := t.TempDir()
	injected := errors.New("close fail")
	failClose := false
	policy := vfs.NewFaultPolicy()
	policy.SetHook(func(op vfs.Op, _ string) error {
		if op == vfs.OpFileClose && failClose {
			failClose = false
			return injected
		}
		return nil
	})
	fs := vfs.NewFaultFSWithPolicy(vfs.OSFS{}, policy)

	m, err := wal.Open(wal.Config{Dir: dir, FS: fs})
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	appendEntryValue(t, m, "k", "x")

	failClose = true
	err = m.Close()
	if !errors.Is(err, injected) {
		t.Fatalf("expected close failure, got %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("retry close: %v", err)
	}
}

func TestManagerAppendAndReplay(t *testing.T) {
	dir := t.TempDir()
	m, err := wal.Open(wal.Config{Dir: dir})
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}

	entries := []string{
		"hello",
		"world",
		"zoom",
	}
	for _, value := range entries {
		appendEntryValue(t, m, "k", value)
	}
	if err := m.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// reopen and replay
	m, err = wal.Open(wal.Config{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = m.Close() }()

	var got []string
	if err := m.Replay(func(_ wal.EntryInfo, payload []byte) error {
		entry, err := kv.DecodeEntry(payload)
		if err != nil {
			return err
		}
		got = append(got, string(entry.Value))
		entry.DecrRef()
		return nil
	}); err != nil {
		t.Fatalf("replay: %v", err)
	}

	if len(got) != len(entries) {
		t.Fatalf("entries mismatch: want %d got %d", len(entries), len(got))
	}
	for i := range entries {
		if entries[i] != got[i] {
			t.Fatalf("entry %d mismatch: want %q got %q", i, entries[i], got[i])
		}
	}
}

func TestManagerReplayRebuildsStaleCatalog(t *testing.T) {
	dir := t.TempDir()
	m, err := wal.Open(wal.Config{Dir: dir})
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer func() { _ = m.Close() }()

	appendEntryValue(t, m, "k", "catalog")
	if err := m.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}

	catalogPath := filepath.Join(dir, "00001.wal.idx")
	if err := os.WriteFile(catalogPath, []byte("stale"), 0o644); err != nil {
		t.Fatalf("write stale catalog: %v", err)
	}

	var got []string
	if err := m.Replay(func(_ wal.EntryInfo, payload []byte) error {
		entry, err := kv.DecodeEntry(payload)
		if err != nil {
			return err
		}
		got = append(got, string(entry.Value))
		entry.DecrRef()
		return nil
	}); err != nil {
		t.Fatalf("replay with stale catalog: %v", err)
	}
	if len(got) != 1 || got[0] != "catalog" {
		t.Fatalf("unexpected replayed values: %v", got)
	}
	info, err := os.Stat(catalogPath)
	if err != nil {
		t.Fatalf("stat rebuilt catalog: %v", err)
	}
	if info.Size() <= int64(len("stale")) {
		t.Fatalf("expected catalog to be rebuilt, size=%d", info.Size())
	}
}

func TestManagerReplayFilteredSkipsUnmatchedCatalogRecords(t *testing.T) {
	dir := t.TempDir()
	m, err := wal.Open(wal.Config{Dir: dir})
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer func() { _ = m.Close() }()

	groupPayload := func(groupID uint64, suffix byte) []byte {
		var buf [binary.MaxVarintLen64 + 1]byte
		n := binary.PutUvarint(buf[:], groupID)
		out := append([]byte(nil), buf[:n]...)
		return append(out, suffix)
	}
	if _, err := m.AppendRecords(wal.DurabilityBuffered,
		wal.Record{Type: wal.RecordTypeRaftEntry, Payload: groupPayload(1, 'a')},
		wal.Record{Type: wal.RecordTypeRaftEntry, Payload: groupPayload(2, 'b')},
	); err != nil {
		t.Fatalf("append records: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	m, err = wal.Open(wal.Config{Dir: dir})
	if err != nil {
		t.Fatalf("reopen wal: %v", err)
	}
	defer func() { _ = m.Close() }()

	var got []uint64
	if err := m.ReplayFiltered(func(info wal.EntryInfo) bool {
		return info.Type == wal.RecordTypeRaftEntry && info.GroupID == 2
	}, func(info wal.EntryInfo, _ []byte) error {
		got = append(got, info.GroupID)
		return nil
	}); err != nil {
		t.Fatalf("filtered replay: %v", err)
	}
	if len(got) != 1 || got[0] != 2 {
		t.Fatalf("unexpected filtered groups: %v", got)
	}
}

func TestManagerRemoveSegmentRemovesCatalog(t *testing.T) {
	dir := t.TempDir()
	m, err := wal.Open(wal.Config{Dir: dir})
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer func() { _ = m.Close() }()

	appendEntryValue(t, m, "k1", "v1")
	if err := m.Rotate(); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	appendEntryValue(t, m, "k2", "v2")

	catalogPath := filepath.Join(dir, "00001.wal.idx")
	if _, err := os.Stat(catalogPath); err != nil {
		t.Fatalf("expected rotated segment catalog: %v", err)
	}
	if err := m.RemoveSegment(1); err != nil {
		t.Fatalf("remove segment: %v", err)
	}
	if _, err := os.Stat(catalogPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected catalog removal, got %v", err)
	}
}

func TestManagerRotate(t *testing.T) {
	dir := t.TempDir()
	m, err := wal.Open(wal.Config{Dir: dir})
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer func() { _ = m.Close() }()

	appendEntryValue(t, m, "k", "record")
	if err := m.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if err := m.Rotate(); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	appendEntryValue(t, m, "k", "record")
	if err := m.Sync(); err != nil {
		t.Fatalf("sync after rotate: %v", err)
	}
	files, err := m.ListSegments()
	if err != nil {
		t.Fatalf("list segments: %v", err)
	}
	if len(files) != 2 {
		t.Fatalf("expected 2 segments, got %d (%v)", len(files), files)
	}
}

func TestManagerReplayHandlesTruncate(t *testing.T) {
	dir := t.TempDir()
	m, err := wal.Open(wal.Config{Dir: dir})
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer func() { _ = m.Close() }()

	appendEntryValue(t, m, "k1", "alpha")
	appendEntryValue(t, m, "k2", "beta")
	if err := m.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}

	files, err := m.ListSegments()
	if err != nil || len(files) == 0 {
		t.Fatalf("list segments err=%v files=%v", err, files)
	}
	active := files[len(files)-1]
	if err := m.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Truncate the last two bytes to leave at least one full record.
	info, err := os.Stat(active)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if err := os.Truncate(active, info.Size()-2); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	m, err = wal.Open(wal.Config{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = m.Close() }()

	var count int
	if err := m.Replay(func(_ wal.EntryInfo, payload []byte) error {
		count++
		return nil
	}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if count == 0 {
		t.Fatalf("expected at least one record before truncation")
	}
}

func TestManagerReplayChecksumError(t *testing.T) {
	dir := t.TempDir()
	m, err := wal.Open(wal.Config{Dir: dir})
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer func() { _ = m.Close() }()

	appendEntryValue(t, m, "k", "gamma")
	if err := m.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	files, err := m.ListSegments()
	if err != nil || len(files) == 0 {
		t.Fatalf("list segments err=%v files=%v", err, files)
	}
	path := files[0]
	if err := m.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Corrupt checksum
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open segment: %v", err)
	}
	if _, err := f.Seek(-2, io.SeekEnd); err != nil {
		t.Fatalf("seek: %v", err)
	}
	if _, err := f.Write([]byte{0x12, 0x34}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close segment: %v", err)
	}

	m, err = wal.Open(wal.Config{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = m.Close() }()

	err = m.Replay(func(_ wal.EntryInfo, payload []byte) error { return nil })
	if err == nil {
		t.Fatalf("expected checksum error")
	}
}

func TestManagerCloseIdempotent(t *testing.T) {
	dir := t.TempDir()
	m, err := wal.Open(wal.Config{Dir: dir})
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	appendEntryValue(t, m, "k", "x")
	if err := m.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("close again: %v", err)
	}
}

// Helper verifying segments sorted order.
func TestManagerListSegmentsSorted(t *testing.T) {
	dir := t.TempDir()
	m, err := wal.Open(wal.Config{Dir: dir, SegmentSize: 128})
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer func() { _ = m.Close() }()

	for i := range 5 {
		payload := []byte("payload-" + string(rune('a'+i)))
		appendEntryValue(t, m, "k", string(payload))
		if i%2 == 1 {
			if err := m.Rotate(); err != nil {
				t.Fatalf("rotate: %v", err)
			}
		}
	}
	if err := m.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	files, err := m.ListSegments()
	if err != nil {
		t.Fatalf("list segments: %v", err)
	}
	if !sort.StringsAreSorted(files) {
		t.Fatalf("files not sorted: %v", files)
	}
}

func TestVerifyDirTruncatesPartialSegment(t *testing.T) {
	dir := t.TempDir()
	m, err := wal.Open(wal.Config{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	payloads := []string{"alpha", "beta"}
	for i, payload := range payloads {
		appendEntryValue(t, m, string(rune('a'+i)), payload)
	}
	if err := m.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	files, err := filepath.Glob(filepath.Join(dir, "*.wal"))
	if err != nil || len(files) == 0 {
		t.Fatalf("list segments err=%v files=%v", err, files)
	}
	f, err := os.OpenFile(files[0], os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("open segment: %v", err)
	}
	if err := binary.Write(f, binary.BigEndian, uint32(16)); err != nil {
		t.Fatalf("write length: %v", err)
	}
	if _, err := f.Write([]byte("bad")); err != nil {
		t.Fatalf("write partial: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close segment: %v", err)
	}

	if err := wal.VerifyDir(dir, nil); err != nil {
		t.Fatalf("verify dir: %v", err)
	}

	m, err = wal.Open(wal.Config{Dir: dir})
	if err != nil {
		t.Fatalf("reopen after verify: %v", err)
	}
	defer func() { _ = m.Close() }()

	count := 0
	if err := m.Replay(func(_ wal.EntryInfo, payload []byte) error {
		count++
		return nil
	}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if count != len(payloads) {
		t.Fatalf("expected %d entries after verify, got %d", len(payloads), count)
	}
}

func TestVerifyDirPropagatesTruncateFailure(t *testing.T) {
	dir := t.TempDir()
	m, err := wal.Open(wal.Config{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	appendEntryValue(t, m, "k", "alpha")
	if err := m.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	files, err := filepath.Glob(filepath.Join(dir, "*.wal"))
	if err != nil || len(files) == 0 {
		t.Fatalf("list segments err=%v files=%v", err, files)
	}
	path := files[0]

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("open segment: %v", err)
	}
	if err := binary.Write(f, binary.BigEndian, uint32(16)); err != nil {
		t.Fatalf("write length: %v", err)
	}
	if _, err := f.Write([]byte("bad")); err != nil {
		t.Fatalf("write partial: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close segment: %v", err)
	}

	injected := errors.New("truncate fail")
	policy := vfs.NewFaultPolicy(vfs.FailOnceRule(vfs.OpFileTrunc, path, injected))
	fs := vfs.NewFaultFSWithPolicy(vfs.OSFS{}, policy)

	err = wal.VerifyDir(dir, fs)
	if !errors.Is(err, injected) {
		t.Fatalf("expected truncate failure, got %v", err)
	}
}

func TestManagerAppendRecordsTyped(t *testing.T) {
	dir := t.TempDir()
	m, err := wal.Open(wal.Config{Dir: dir})
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer func() { _ = m.Close() }()

	rec := wal.Record{
		Type:    wal.RecordTypeRaftEntry,
		Payload: []byte("raft-data"),
	}
	infos, err := m.AppendRecords(wal.DurabilityBuffered, rec)
	if err != nil {
		t.Fatalf("append records: %v", err)
	}
	if len(infos) != 1 {
		t.Fatalf("expected 1 record info, got %d", len(infos))
	}
	if infos[0].Type != wal.RecordTypeRaftEntry {
		t.Fatalf("expected record type raft entry, got %v", infos[0].Type)
	}
	if err := m.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}

	var seenType wal.RecordType
	if err := m.Replay(func(info wal.EntryInfo, payload []byte) error {
		seenType = info.Type
		if string(payload) != "raft-data" {
			t.Fatalf("unexpected payload: %q", payload)
		}
		return nil
	}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if seenType != wal.RecordTypeRaftEntry {
		t.Fatalf("expected replay type raft entry, got %v", seenType)
	}

	metrics := m.Metrics()
	if metrics == nil {
		t.Fatalf("expected wal metrics to be available")
		return
	}
	if metrics.RecordCounts.RaftEntries != 1 {
		t.Fatalf("expected raft entry count to be 1, got %+v", metrics.RecordCounts)
	}
	if metrics.RecordCounts.Entries != 0 || metrics.RecordCounts.RaftStates != 0 || metrics.RecordCounts.RaftSnapshots != 0 {
		t.Fatalf("unexpected record counts %+v", metrics.RecordCounts)
	}
	if metrics.RecordCounts.Total() != 1 {
		t.Fatalf("expected total record count to equal 1, got %d", metrics.RecordCounts.Total())
	}
	if metrics.SegmentsWithRaftRecords != 1 {
		t.Fatalf("expected raft segment count to be 1, got %d", metrics.SegmentsWithRaftRecords)
	}
	segMetrics := m.SegmentRecordMetrics(infos[0].SegmentID)
	if segMetrics.RaftEntries != 1 {
		t.Fatalf("expected segment raft entry count to be 1, got %+v", segMetrics)
	}

	if err := m.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	m, err = wal.Open(wal.Config{Dir: dir})
	if err != nil {
		t.Fatalf("reopen wal: %v", err)
	}
	defer func() { _ = m.Close() }()

	metrics = m.Metrics()
	if metrics.RecordCounts.RaftEntries != 1 {
		t.Fatalf("expected raft entry count to persist after reopen, got %+v", metrics.RecordCounts)
	}
	if metrics.SegmentsWithRaftRecords != 1 {
		t.Fatalf("expected segment count with raft records to persist after reopen, got %d", metrics.SegmentsWithRaftRecords)
	}
}

func TestManagerAppendEntryBatchAndReplay(t *testing.T) {
	dir := t.TempDir()
	m, err := wal.Open(wal.Config{Dir: dir})
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer func() { _ = m.Close() }()

	e1 := kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("k1"), 10), []byte("v1"))
	e2 := kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("k2"), 11), []byte("v2"))
	defer e1.DecrRef()
	defer e2.DecrRef()

	info, err := m.AppendEntryBatch(wal.DurabilityBuffered, []*kv.Entry{e1, e2})
	if err != nil {
		t.Fatalf("append entry batch: %v", err)
	}
	if info.Type != wal.RecordTypeEntryBatch {
		t.Fatalf("expected entry batch type, got %v", info.Type)
	}
	if err := m.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}

	replayed := 0
	if err := m.Replay(func(info wal.EntryInfo, payload []byte) error {
		if info.Type != wal.RecordTypeEntryBatch {
			return nil
		}
		replayed++
		entries, err := wal.DecodeEntryBatch(payload)
		if err != nil {
			t.Fatalf("decode entry batch: %v", err)
		}
		defer func() {
			for _, e := range entries {
				e.DecrRef()
			}
		}()
		if len(entries) != 2 {
			t.Fatalf("expected 2 entries, got %d", len(entries))
		}
		_, key0, _, ok0 := kv.SplitInternalKey(entries[0].Key)
		if !ok0 || string(key0) != "k1" || string(entries[0].Value) != "v1" {
			t.Fatalf("unexpected first entry: key=%q value=%q", key0, entries[0].Value)
		}
		_, key1, _, ok1 := kv.SplitInternalKey(entries[1].Key)
		if !ok1 || string(key1) != "k2" || string(entries[1].Value) != "v2" {
			t.Fatalf("unexpected second entry: key=%q value=%q", key1, entries[1].Value)
		}
		return nil
	}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if replayed != 1 {
		t.Fatalf("expected exactly 1 batch record, got %d", replayed)
	}
}

func TestManagerAppendEntryRejectsInvalidInput(t *testing.T) {
	dir := t.TempDir()
	m, err := wal.Open(wal.Config{Dir: dir})
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer func() { _ = m.Close() }()

	if _, err := m.AppendEntry(wal.DurabilityBuffered, nil); err == nil {
		t.Fatalf("expected error for nil entry")
	}

	e := kv.NewEntry(nil, []byte("v"))
	defer e.DecrRef()
	if _, err := m.AppendEntry(wal.DurabilityBuffered, e); err == nil {
		t.Fatalf("expected error for empty key entry")
	}
}

func TestDecodeEntryBatchRejectsMalformedCount(t *testing.T) {
	t.Run("zero count", func(t *testing.T) {
		payload := make([]byte, 4)
		if _, err := wal.DecodeEntryBatch(payload); err == nil {
			t.Fatalf("expected malformed entry batch error")
		}
	})

	t.Run("count exceeds payload minimum footprint", func(t *testing.T) {
		payload := make([]byte, 9)
		binary.BigEndian.PutUint32(payload[:4], ^uint32(0))
		binary.BigEndian.PutUint32(payload[4:8], 1)
		payload[8] = 0
		if _, err := wal.DecodeEntryBatch(payload); err == nil {
			t.Fatalf("expected malformed entry batch error")
		}
	})
}
