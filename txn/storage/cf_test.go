// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"testing"
)

func TestEncodeDecodeColumnFamilyKey(t *testing.T) {
	key := []byte("alpha")
	encoded := append([]byte{cfMarker0, cfMarker1, cfMarker2, byte(CFWrite)}, key...)
	cf, userKey, ok := SplitBaseKey(encoded)
	if !ok {
		t.Fatalf("expected encoded key to carry marker")
	}
	if cf != CFWrite {
		t.Fatalf("expected CFWrite, got %v", cf)
	}
	if string(userKey) != string(key) {
		t.Fatalf("expected user key %q, got %q", key, userKey)
	}

	cf2, userKey2, ts, ok := SplitInternalKey(InternalKey(CFLock, key, 42))
	if !ok {
		t.Fatalf("expected strict split success")
	}
	if cf2 != CFLock {
		t.Fatalf("expected CFLock, got %v", cf2)
	}
	if string(userKey2) != string(key) {
		t.Fatalf("unexpected user key %q", userKey2)
	}
	if ts != 42 {
		t.Fatalf("expected ts 42, got %d", ts)
	}
}

func TestColumnFamilyStringAndValid(t *testing.T) {
	if CFDefault.String() != "default" {
		t.Fatalf("expected default, got %s", CFDefault.String())
	}
	if CFLock.String() != "lock" {
		t.Fatalf("expected lock, got %s", CFLock.String())
	}
	if CFWrite.String() != "write" {
		t.Fatalf("expected write, got %s", CFWrite.String())
	}
	if ColumnFamily(99).String() == "" {
		t.Fatalf("expected unknown CF string to be non-empty")
	}
	if !CFDefault.Valid() || !CFLock.Valid() || !CFWrite.Valid() {
		t.Fatalf("expected built-in column families to be valid")
	}
	if ColumnFamily(99).Valid() {
		t.Fatalf("unexpected valid status for unknown column family")
	}
}
