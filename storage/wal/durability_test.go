// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package wal_test

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/feichai0017/NoKV/storage/wal"
)

func TestManagerDurabilityVisibilityMatrix(t *testing.T) {
	tests := []struct {
		name        string
		durability  wal.DurabilityPolicy
		afterAppend func(t *testing.T, m *wal.Manager) bool
		wantVisible bool
	}{
		{
			name:        "buffered_append_without_sync",
			durability:  wal.DurabilityBuffered,
			wantVisible: false,
		},
		{
			name:       "explicit_sync",
			durability: wal.DurabilityBuffered,
			afterAppend: func(t *testing.T, m *wal.Manager) bool {
				t.Helper()
				if err := m.Sync(); err != nil {
					t.Fatalf("sync: %v", err)
				}
				return false
			},
			wantVisible: true,
		},
		{
			name:        "fsync_durability",
			durability:  wal.DurabilityFsync,
			wantVisible: true,
		},
		{
			name:       "close",
			durability: wal.DurabilityBuffered,
			afterAppend: func(t *testing.T, m *wal.Manager) bool {
				t.Helper()
				if err := m.Close(); err != nil {
					t.Fatalf("close: %v", err)
				}
				return true
			},
			wantVisible: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			m, err := wal.Open(wal.Config{Dir: dir})
			if err != nil {
				t.Fatalf("open wal: %v", err)
			}
			closed := false
			defer func() {
				if !closed {
					_ = m.Close()
				}
			}()

			payload := []byte("durability-" + tt.name)
			if _, err := m.AppendRecords(tt.durability, wal.Record{
				Type:    wal.RecordTypeRaftEntry,
				Payload: payload,
			}); err != nil {
				t.Fatalf("append record: %v", err)
			}
			if tt.afterAppend != nil {
				closed = tt.afterAppend(t, m)
			}

			got := visiblePayloads(t, dir, wal.RecordTypeRaftEntry)
			if tt.wantVisible {
				if len(got) != 1 || string(got[0]) != string(payload) {
					t.Fatalf("expected durable payload %q, got %q", payload, got)
				}
				return
			}
			if len(got) != 0 {
				t.Fatalf("expected buffered record to be invisible before sync, got %q", got)
			}
		})
	}
}

func TestManagerAppendPolicyControlsVisibility(t *testing.T) {
	dir := t.TempDir()
	m, err := wal.Open(wal.Config{Dir: dir})
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer func() { _ = m.Close() }()

	payload := []byte("option-flushed")
	if _, err := m.AppendRecords(wal.DurabilityFlushed, wal.Record{
		Type:    wal.RecordTypeRaftEntry,
		Payload: payload,
	}); err != nil {
		t.Fatalf("append with options: %v", err)
	}

	got := visiblePayloads(t, dir, wal.RecordTypeRaftEntry)
	if len(got) != 1 || string(got[0]) != string(payload) {
		t.Fatalf("expected flushed append to be visible, got %q", got)
	}
}

func TestManagerRejectsInvalidDurabilityPolicy(t *testing.T) {
	m, err := wal.Open(wal.Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer func() { _ = m.Close() }()
	if _, err := m.AppendRecords(wal.DurabilityPolicy(255), wal.Record{
		Type:    wal.RecordTypeRaftEntry,
		Payload: []byte("bad-policy"),
	}); err == nil {
		t.Fatalf("expected invalid append durability to fail")
	}
}

func visiblePayloads(t *testing.T, dir string, typ wal.RecordType) [][]byte {
	t.Helper()
	segments, err := filepath.Glob(filepath.Join(dir, "*.wal"))
	if err != nil {
		t.Fatalf("glob segments: %v", err)
	}
	sort.Strings(segments)

	var out [][]byte
	for _, path := range segments {
		f, err := os.Open(path)
		if err != nil {
			t.Fatalf("open segment %s: %v", path, err)
		}
		iter := wal.NewRecordIterator(f, wal.DefaultBufferSize)
		for iter.Next() {
			if iter.Type() == typ {
				out = append(out, iter.Record())
			}
		}
		if err := iter.Err(); err != nil && !errors.Is(err, io.EOF) {
			_ = iter.Close()
			_ = f.Close()
			t.Fatalf("iterate segment %s: %v", path, err)
		}
		if err := iter.Close(); err != nil {
			_ = f.Close()
			t.Fatalf("close iterator: %v", err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("close segment %s: %v", path, err)
		}
	}
	return out
}
