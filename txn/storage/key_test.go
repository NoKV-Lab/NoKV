// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKeyHelpers(t *testing.T) {
	key := []byte("alpha")
	k1 := InternalKey(CFDefault, key, 1)
	k2 := InternalKey(CFDefault, key, 2)

	if !SameBaseKey(k1, k2) {
		t.Fatalf("expected SameBaseKey for different versions")
	}
	if SameBaseKey(k1, InternalKey(CFDefault, []byte("beta"), 1)) {
		t.Fatalf("unexpected SameBaseKey for different user keys")
	}

	if Timestamp(k1) != 1 {
		t.Fatalf("expected ts=1, got %d", Timestamp(k1))
	}

	h1 := MemHash(k1)
	h2 := MemHash(k1)
	if h1 != h2 {
		t.Fatalf("expected stable MemHash for same data")
	}
}

func TestSplitInternalKeyStrict(t *testing.T) {
	internal := InternalKey(CFWrite, []byte("alpha"), 42)
	cf, userKey, ts, ok := SplitInternalKey(internal)
	if !ok {
		t.Fatalf("expected strict decode success")
	}
	if cf != CFWrite {
		t.Fatalf("expected CFWrite, got %v", cf)
	}
	if string(userKey) != "alpha" {
		t.Fatalf("expected user key alpha, got %q", userKey)
	}
	if ts != 42 {
		t.Fatalf("expected ts=42, got %d", ts)
	}

	if _, _, _, ok := SplitInternalKey([]byte("raw-key")); ok {
		t.Fatalf("expected strict decode failure for raw key")
	}

	nonCanonical := append([]byte("plain"), make([]byte, 8)...)
	if _, _, _, ok := SplitInternalKey(nonCanonical); ok {
		t.Fatalf("expected strict decode failure without CF marker")
	}
}

func TestBaseKeySplitInternalKeyAndSafeCopy(t *testing.T) {
	if got := InternalToBaseKey([]byte("raw")); string(got) != "raw" {
		t.Fatalf("expected raw key unchanged, got %q", got)
	}
	internal := InternalKey(CFLock, []byte("hello"), 5)
	base := InternalToBaseKey(internal)
	cf, decodedUserKey, ok := SplitBaseKey(base)
	if !ok || cf != CFLock || string(decodedUserKey) != "hello" {
		t.Fatalf("expected base key for CFLock/hello, got ok=%v cf=%v key=%q", ok, cf, decodedUserKey)
	}
	_, userKey, _, ok := SplitInternalKey(internal)
	if !ok || string(userKey) != "hello" {
		t.Fatalf("expected split user key hello, got ok=%v key=%q", ok, userKey)
	}
	encoded := BaseKey(CFWrite, []byte("world"))
	cf, decodedUserKey, ok = SplitBaseKey(encoded)
	if !ok || cf != CFWrite || string(decodedUserKey) != "world" {
		t.Fatalf("expected encoded base key for CFWrite/world, got ok=%v cf=%v key=%q", ok, cf, decodedUserKey)
	}
	orig := []byte("copy")
	out := SafeCopy(nil, orig)
	if string(out) != "copy" {
		t.Fatalf("expected copy, got %q", out)
	}
	orig[0] = 'C'
	if string(out) != "copy" {
		t.Fatalf("expected SafeCopy to detach from source")
	}
}

func TestCompareBaseAndUserKeys(t *testing.T) {
	k1 := InternalKey(CFDefault, []byte("a"), 1)
	k2 := InternalKey(CFDefault, []byte("b"), 1)
	k3 := InternalKey(CFWrite, []byte("a"), 1)
	require.Less(t, CompareBaseKeys(k1, k2), 0)
	require.Less(t, CompareBaseKeys(k1, k3), 0)
	require.Equal(t, 0, CompareBaseKeys(
		InternalKey(CFDefault, []byte("c"), 10),
		InternalKey(CFDefault, []byte("c"), 1),
	))
	require.Less(t, CompareUserKeys(k1, k2), 0)
	require.Equal(t, 0, CompareUserKeys(
		InternalKey(CFDefault, []byte("c"), 10),
		InternalKey(CFDefault, []byte("c"), 1),
	))
}

// TestCompareBaseKeysAssumeValidMatchesSafe verifies the fast variant
// agrees with the safe variant for every well-formed internal key the
// hot path can produce. Cross-CF, identical-key-different-version, and
// equal-key cases are all covered.
func TestCompareBaseKeysAssumeValidMatchesSafe(t *testing.T) {
	cases := []struct{ k1, k2 []byte }{
		{InternalKey(CFDefault, []byte("a"), 1), InternalKey(CFDefault, []byte("b"), 1)},
		{InternalKey(CFDefault, []byte("a"), 1), InternalKey(CFWrite, []byte("a"), 1)},
		{InternalKey(CFDefault, []byte("c"), 10), InternalKey(CFDefault, []byte("c"), 1)},
		{InternalKey(CFDefault, []byte(""), 1), InternalKey(CFDefault, []byte("a"), 1)},
		{InternalKey(CFWrite, []byte("zzzz"), 99), InternalKey(CFWrite, []byte("zzzz"), 1)},
	}
	for _, tc := range cases {
		safe := CompareBaseKeys(tc.k1, tc.k2)
		fast := CompareBaseKeysAssumeValid(tc.k1, tc.k2)
		require.Equal(t, sign(safe), sign(fast),
			"safe=%d fast=%d k1=%x k2=%x", safe, fast, tc.k1, tc.k2)
	}
}

func sign(x int) int {
	switch {
	case x < 0:
		return -1
	case x > 0:
		return 1
	default:
		return 0
	}
}
