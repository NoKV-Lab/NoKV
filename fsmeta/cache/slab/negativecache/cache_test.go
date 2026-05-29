// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package negativecache

import (
	"bytes"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// versionedKey simulates a versioned full key: user_key + 0x00 separator
// + 1-byte version. The "group key" is the user_key portion (everything
// up to the separator).
func versionedKey(user string, version byte) []byte {
	out := make([]byte, 0, len(user)+2)
	out = append(out, user...)
	out = append(out, 0x00)
	out = append(out, version)
	return out
}

func userOf(fullKey []byte) []byte {
	before, _, ok := bytes.Cut(fullKey, []byte{0x00})
	if !ok {
		return fullKey
	}
	return before
}

func TestCacheRememberHasInvalidate(t *testing.T) {
	c := New(Config{GroupKeyFn: userOf})
	k := versionedKey("alpha", 1)

	require.False(t, c.Has(k), "Has must be false before Remember")
	c.Remember(k)
	require.True(t, c.Has(k))

	// A different version of the same user key was never explicitly
	// remembered, so it must miss.
	require.False(t, c.Has(versionedKey("alpha", 2)))
	// A different user key likewise misses.
	require.False(t, c.Has(versionedKey("beta", 1)))

	// Invalidating any version of "alpha" bumps the group's generation,
	// so the previously-remembered alpha entry becomes stale.
	c.Invalidate(versionedKey("alpha", 5))
	require.False(t, c.Has(k), "Invalidate must stale every entry in the group")

	// Re-Remember at the new generation makes it fresh again.
	c.Remember(k)
	require.True(t, c.Has(k))
}

func TestCacheClearWipesEverything(t *testing.T) {
	c := New(Config{GroupKeyFn: userOf})
	for i := byte(1); i <= 10; i++ {
		c.Remember(versionedKey("k", i))
	}
	for i := byte(1); i <= 10; i++ {
		require.True(t, c.Has(versionedKey("k", i)))
	}
	c.Clear()
	for i := byte(1); i <= 10; i++ {
		require.False(t, c.Has(versionedKey("k", i)))
	}
}

func TestCacheSnapshotKeysDropsStaleEntries(t *testing.T) {
	c := New(Config{GroupKeyFn: userOf})
	c.Remember(versionedKey("alive", 1))
	c.Remember(versionedKey("alive", 2))
	c.Remember(versionedKey("doomed", 1))

	// Invalidate the doomed group; its entries should be excluded.
	c.Invalidate(versionedKey("doomed", 99))

	keys := c.SnapshotKeys()
	require.Len(t, keys, 2, "doomed entry must be filtered out")
	for _, k := range keys {
		require.True(t, bytes.HasPrefix(k, []byte("alive")),
			"SnapshotKeys must only return still-fresh entries; got %q", k)
	}
}

func TestCacheConcurrentRememberHas(t *testing.T) {
	c := New(Config{GroupKeyFn: userOf})
	const writers, readers, ops = 4, 4, 500
	var wg sync.WaitGroup
	wg.Add(writers + readers)
	for w := range writers {
		go func(seed int) {
			defer wg.Done()
			for i := range ops {
				c.Remember(versionedKey("g", byte((seed*31+i)&0xff)))
			}
		}(w)
	}
	for r := range readers {
		go func(seed int) {
			defer wg.Done()
			for i := range ops {
				_ = c.Has(versionedKey("g", byte((seed*17+i)&0xff)))
				if i%23 == 0 {
					c.Invalidate(versionedKey("g", 0))
				}
			}
		}(r)
	}
	wg.Wait()
}

func TestPersistenceRoundTrip(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "negcache")

	// Phase 1: open with persistence, remember a few keys, snapshot.
	{
		_, p, err := OpenWithPersistence(
			Config{GroupKeyFn: userOf},
			PersistConfig{Dir: dir},
		)
		require.NoError(t, err)
		// Reload of an empty dir succeeds with zero restored.
		// Now populate.
		c := p.cache
		c.Remember(versionedKey("k1", 1))
		c.Remember(versionedKey("k2", 1))
		c.Remember(versionedKey("k3", 1))
		// Invalidate one group so its entry is dropped from the snapshot.
		c.Invalidate(versionedKey("k2", 99))

		n, err := p.Snapshot()
		require.NoError(t, err)
		require.Equal(t, 2, n, "stale k2 entry must not be snapshotted")
	}

	// Phase 2: reopen — restored cache must contain k1 and k3 only.
	{
		c, _, err := OpenWithPersistence(
			Config{GroupKeyFn: userOf},
			PersistConfig{Dir: dir},
		)
		require.NoError(t, err)
		require.True(t, c.Has(versionedKey("k1", 1)), "k1 must be warm after reload")
		require.True(t, c.Has(versionedKey("k3", 1)), "k3 must be warm after reload")
		require.False(t, c.Has(versionedKey("k2", 1)), "k2 was stale at snapshot time and must stay missing")
	}
}

func TestPersistenceMissingFileIsClean(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "negcache")
	c, p, err := OpenWithPersistence(
		Config{GroupKeyFn: userOf},
		PersistConfig{Dir: dir},
	)
	require.NoError(t, err)
	// No prior snapshot; cache must be empty and Has returns false.
	require.False(t, c.Has(versionedKey("anything", 1)))
	// Calling Reload again must continue to succeed silently.
	n, err := p.Reload()
	require.NoError(t, err)
	require.Equal(t, 0, n)
}

func TestSnapshotCodecRejectsBadChecksum(t *testing.T) {
	body, written := encodeSnapshotKeys([][]byte{[]byte("alpha"), []byte("beta")}, 1<<20)
	require.Equal(t, 2, written)
	body[len(body)-1] ^= 0xff

	keys, ok := decodeSnapshotKeys(body)
	require.False(t, ok)
	require.Nil(t, keys)
}

func TestSnapshotCodecRestoresValidPrefixFromTruncatedTail(t *testing.T) {
	body, written := encodeSnapshotKeys([][]byte{[]byte("alpha"), []byte("beta")}, 1<<20)
	require.Equal(t, 2, written)
	body = body[:len(body)-2]

	keys, ok := decodeSnapshotKeys(body)
	require.True(t, ok)
	require.Len(t, keys, 2)
	require.Equal(t, []byte("alpha"), keys[0])
	require.Equal(t, []byte("beta"), keys[1])
}
