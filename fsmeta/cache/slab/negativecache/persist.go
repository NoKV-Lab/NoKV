// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package negativecache

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/feichai0017/NoKV/fsmeta/cache/slab"
	"github.com/feichai0017/NoKV/storage/file"
)

// PersistConfig opens a Persistence sidecar for a Cache. Persistence is
// optional — callers that want pure in-memory negative caches can use
// New instead of OpenWithPersistence.
type PersistConfig struct {
	// Dir is the directory the slab segment lives in. Must be writable.
	Dir string
	// MaxSize bounds the on-disk snapshot. Zero falls back to 64 MiB.
	// Snapshots stop appending once the limit is hit; the dropped tail
	// re-warms via the Cache's normal Remember path.
	MaxSize int64
	// FS lets tests inject a fault FS; defaults to the OS via slab.
	FS any
}

// Persistence pairs a Cache with its slab-backed snapshot file.
// Snapshot writes the cache to disk; Reload reads it back. Snapshot
// truncates and rewrites — this is a snapshot, not an append-only log.
type Persistence struct {
	dir     string
	maxSize int64
	cache   *Cache
}

// OpenWithPersistence returns a Cache plus a Persistence helper. If a
// snapshot already exists at Dir/negative.slab it is restored into the
// cache before returning, so the caller observes a warm cache from the
// first Has call.
func OpenWithPersistence(cacheCfg Config, persistCfg PersistConfig) (*Cache, *Persistence, error) {
	if persistCfg.Dir == "" {
		return nil, nil, errPersistenceDirRequired
	}
	if persistCfg.MaxSize <= 0 {
		persistCfg.MaxSize = 64 << 20
	}
	cache := New(cacheCfg)
	p := &Persistence{
		dir:     persistCfg.Dir,
		maxSize: persistCfg.MaxSize,
		cache:   cache,
	}
	if _, err := p.Reload(); err != nil {
		return cache, p, err
	}
	return cache, p, nil
}

// Snapshot writes every still-fresh entry from the cache to the
// snapshot file, replacing any prior contents. Returns the number of
// entries written. Truncation past MaxSize silently drops the tail.
func (p *Persistence) Snapshot() (int, error) {
	if p == nil || p.cache == nil {
		return 0, nil
	}
	keys := p.cache.SnapshotKeys()
	body, written := encodeSnapshotKeys(keys, p.maxSize)
	if err := os.MkdirAll(p.dir, 0o755); err != nil {
		return 0, fmt.Errorf("negativecache snapshot dir: %w", err)
	}
	path := filepath.Join(p.dir, snapshotFile)

	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return 0, fmt.Errorf("negativecache snapshot unlink: %w", err)
	}

	var seg slab.Segment
	if err := seg.Open(&file.Options{
		FID:      0,
		FileName: path,
		MaxSz:    int(p.maxSize),
	}); err != nil {
		return 0, fmt.Errorf("negativecache snapshot open: %w", err)
	}
	defer func() { _ = seg.Close() }()

	if err := seg.Write(0, body); err != nil {
		return 0, fmt.Errorf("negativecache snapshot write: %w", err)
	}
	if err := seg.DoneWriting(uint32(len(body))); err != nil {
		return written, fmt.Errorf("negativecache snapshot seal: %w", err)
	}
	return written, nil
}

// Reload reads the snapshot file (if present) and replays Remember for
// each restored full key. Returns the number of restored keys.
// Missing/truncated/corrupt snapshots produce a zero count and a nil
// error — Derived class tolerates loss.
func (p *Persistence) Reload() (int, error) {
	if p == nil || p.cache == nil {
		return 0, nil
	}
	path := filepath.Join(p.dir, snapshotFile)
	body, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	keys, ok := decodeSnapshotKeys(body)
	if !ok {
		return 0, nil
	}
	for _, key := range keys {
		p.cache.Remember(key)
	}
	return len(keys), nil
}
