// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package dirpage

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/feichai0017/NoKV/fsmeta/cache/slab"
	"github.com/feichai0017/NoKV/storage/vfs"
)

// Config controls how a Cache opens its backing slab and shapes its
// pages. Sensible defaults apply when zero.
type Config struct {
	// Dir is the directory the underlying slab.Manager will own.
	Dir string
	// FS provides the filesystem abstraction (defaults to vfs.OSFS via
	// vfs.Ensure inside slab.Open).
	FS vfs.FS
	// MaxSegmentSize bounds each backing slab segment. Zero falls back
	// to slab.DefaultMaxSize.
	MaxSegmentSize int64
	// MaxPageBytes targets the per-page on-disk size. Pages roll over
	// when adding the next entry would exceed this. Zero falls back to
	// 16 KiB (one mmap page on most systems and aligned with the RFC's
	// "4-16KB page" guidance — picking the upper end favors fewer
	// pages and bigger sequential reads at Lookup time).
	MaxPageBytes int
}

func (c Config) resolve() Config {
	if c.MaxPageBytes <= 0 {
		c.MaxPageBytes = 16 << 10
	}
	return c
}

// Cache is the DirPageSlab consumer of slab.Manager. It is safe for
// concurrent use and exposes a small Lookup / Materialize / Invalidate
// surface.
//
// Consistency model: Derived. LSM (the consumer's backing store, owned
// outside this package) is authoritative; pages here can be lost / stale
// without affecting correctness — Lookup returns false on stale and the
// caller falls back to its source of truth.
type Cache struct {
	cfg    Config
	mgr    *slab.Manager
	pageMu sync.RWMutex
	pages  map[PageKey]*pageSet              // in-memory index of materialized pages
	dirs   map[DirectoryKey]map[PageKey]bool // reverse index for directory-scoped invalidation
	epochs sync.Map                          // DirectoryKey -> *atomic.Uint64 (current invalidation epoch)
	stats  stats
}

// pageSet is the cache's view of one directory's materialized pages.
// frontier is shared across all pages of one materialization round and
// decides Lookup freshness vs the caller's supplied frontier.
type pageSet struct {
	frontier uint64
	pages    []pageLoc // sorted by PageNo at materialize time
}

// pageLoc is the slab address of one page record.
type pageLoc struct {
	Fid    uint32
	Offset uint32
	Length uint32
	PageNo uint32
}

type stats struct {
	hits    atomic.Uint64
	misses  atomic.Uint64
	stale   atomic.Uint64
	storeOK atomic.Uint64
	dropped atomic.Uint64
}

// Stats is a snapshot of the counters; useful for tests and metrics.
type Stats struct {
	Hits, Misses, Stale, StoreOK, Dropped uint64
}

// Stats returns a snapshot of the cache counters.
func (c *Cache) Stats() Stats {
	return Stats{
		Hits:    c.stats.hits.Load(),
		Misses:  c.stats.misses.Load(),
		Stale:   c.stats.stale.Load(),
		StoreOK: c.stats.storeOK.Load(),
		Dropped: c.stats.dropped.Load(),
	}
}

// Open opens (or creates) the backing slab and returns a ready Cache.
//
// On reopen the existing slab segments are scanned and any well-formed
// page records are inserted into the in-memory index. Because the
// invalidation epoch is not persisted (Derived consistency, RFC §4.1
// option C), restored pages survive only until the consumer's next
// Invalidate for that key.
func Open(cfg Config) (*Cache, error) {
	cfg = cfg.resolve()
	if cfg.Dir == "" {
		return nil, errors.New("dirpage cache: Dir required")
	}
	mgr, err := slab.Open(slab.Config{
		Dir:        cfg.Dir,
		FS:         cfg.FS,
		FileSuffix: ".dirpage",
		MaxSize:    cfg.MaxSegmentSize,
		HeaderSize: 0, // dirpage frames are self-describing; no preamble needed
	})
	if err != nil {
		return nil, err
	}
	c := &Cache{
		cfg:   cfg,
		mgr:   mgr,
		pages: make(map[PageKey]*pageSet),
		dirs:  make(map[DirectoryKey]map[PageKey]bool),
	}
	if err := c.reload(); err != nil {
		_ = mgr.Close()
		return nil, err
	}
	return c, nil
}

// Close releases the backing slab.
func (c *Cache) Close() error {
	return c.mgr.Close()
}

// Lookup returns the materialized entries for key if their stored frontier and
// page identity match. Returns (nil, false) on miss, stale, or any decode
// error encountered along the way.
//
// frontier is opaque: in fsmeta wiring this is the WatchSubtree event
// cursor. The cache only requires that callers pass the same value for
// matching MaterializeAsync / Lookup pairs.
func (c *Cache) Lookup(key PageKey, frontier uint64) ([]Entry, bool) {
	c.pageMu.RLock()
	set, ok := c.pages[key]
	c.pageMu.RUnlock()
	if !ok {
		c.stats.misses.Add(1)
		return nil, false
	}
	if set.frontier != frontier {
		c.stats.stale.Add(1)
		return nil, false
	}
	out := make([]Entry, 0, 16)
	for _, loc := range set.pages {
		seg, release, err := c.mgr.AcquireRead(loc.Fid)
		if err != nil {
			c.stats.misses.Add(1)
			return nil, false
		}
		buf, err := seg.Read(loc.Offset, loc.Length)
		if err != nil {
			release()
			c.stats.misses.Add(1)
			return nil, false
		}
		// Copy the bytes out so we can release the read pin before
		// continuing to the next page (which may share the same segment
		// lock and would deadlock with another writer otherwise).
		copyBuf := make([]byte, len(buf))
		copy(copyBuf, buf)
		release()

		hdr, entries, _, err := decodePage(copyBuf)
		if err != nil {
			c.stats.misses.Add(1)
			return nil, false
		}
		// Defense in depth: if a segment was truncated under us and the
		// in-memory locator now points at a different page, drop the
		// whole set rather than return inconsistent data.
		if hdr.Mount != key.Mount || hdr.Parent != key.Parent ||
			hdr.StartAfter != key.StartAfter || hdr.Limit != key.Limit ||
			hdr.Frontier != frontier {
			c.stats.misses.Add(1)
			return nil, false
		}
		out = append(out, entries...)
	}
	c.stats.hits.Add(1)
	return out, true
}

// MaterializeAsync stores entries as one or more pages, recording
// frontier with each. The store is idempotent across crashes — pages
// from earlier materializations remain on disk until reclaimed by a
// future GC pass; on Lookup the in-memory index points only at the
// latest set so older versions are effectively orphaned.
//
// "Async" in the name reflects the call shape we expect from the fsmeta
// integration (MaterializeAsync runs off the read fast path), not any
// queueing inside the cache itself: this method writes to slab
// synchronously. A consumer that wants true async behavior should call
// from a goroutine.
//
// If a concurrent Invalidate has already advanced the cache's notion of
// the key's frontier past the supplied frontier, the materialization is
// dropped (counter Dropped++) — the caller's view was stale by the time
// the write happened.
func (c *Cache) MaterializeAsync(key PageKey, frontier uint64, entries []Entry) error {
	dir := key.Directory()
	if cur := c.currentEpoch(dir); cur > frontier {
		c.stats.dropped.Add(1)
		return nil
	}
	pageGroups := splitIntoPages(key.StartAfter, entries, c.cfg.MaxPageBytes)

	locs := make([]pageLoc, 0, len(pageGroups))
	for pageNo, group := range pageGroups {
		hdr := pageHeader{
			Mount:      key.Mount,
			Parent:     key.Parent,
			StartAfter: key.StartAfter,
			Limit:      key.Limit,
			PageNo:     uint32(pageNo),
			Frontier:   frontier,
			EntryCount: uint32(len(group)),
		}
		encoded := encodePage(nil, hdr, group)

		seg, fid, offset, err := c.appendBytes(encoded)
		if err != nil {
			return fmt.Errorf("dirpage materialize %s: %w", key.keyString(), err)
		}
		_ = seg // segment locked / unlocked inside appendBytes
		locs = append(locs, pageLoc{
			Fid:    fid,
			Offset: offset,
			Length: uint32(len(encoded)),
			PageNo: uint32(pageNo),
		})
	}

	c.pageMu.Lock()
	// Race: if Invalidate fired after we passed the entry check but
	// before we got here, we may overwrite the just-bumped epoch's
	// fresh state. Re-check under the lock.
	if cur := c.currentEpoch(dir); cur > frontier {
		c.pageMu.Unlock()
		c.stats.dropped.Add(1)
		return nil
	}
	sort.Slice(locs, func(i, j int) bool { return locs[i].PageNo < locs[j].PageNo })
	c.pages[key] = &pageSet{frontier: frontier, pages: locs}
	c.indexPageLocked(key)
	c.pageMu.Unlock()
	c.stats.storeOK.Add(1)
	return nil
}

// Invalidate marks all cached pages for a directory as stale and bumps the
// directory epoch. Returns the new epoch value so callers that synthesize
// frontiers (instead of pulling from WatchSubtree) can use it as the next
// "current" frontier.
//
// Invalidate is sound regardless of in-flight MaterializeAsync calls: a
// MaterializeAsync that races sees the bumped epoch on its second check
// and drops its store rather than publishing stale pages.
func (c *Cache) Invalidate(key DirectoryKey) uint64 {
	ep := c.epochFor(key)
	next := ep.Add(1)
	c.pageMu.Lock()
	for pageKey := range c.dirs[key] {
		delete(c.pages, pageKey)
	}
	delete(c.dirs, key)
	c.pageMu.Unlock()
	return next
}

// CurrentEpoch returns the cache's current invalidation epoch for a directory.
// Useful for tests and for the consumer's "current frontier" supply when
// WatchSubtree is not in scope.
func (c *Cache) CurrentEpoch(key DirectoryKey) uint64 {
	return c.currentEpoch(key)
}

func (c *Cache) currentEpoch(key DirectoryKey) uint64 {
	if v, ok := c.epochs.Load(key); ok {
		return v.(*atomic.Uint64).Load()
	}
	return 0
}

func (c *Cache) epochFor(key DirectoryKey) *atomic.Uint64 {
	if v, ok := c.epochs.Load(key); ok {
		return v.(*atomic.Uint64)
	}
	fresh := new(atomic.Uint64)
	if v, loaded := c.epochs.LoadOrStore(key, fresh); loaded {
		return v.(*atomic.Uint64)
	}
	return fresh
}

// appendBytes commits one record into the active slab segment, rotating
// when the active fills.
func (c *Cache) appendBytes(buf []byte) (*slab.Segment, uint32, uint32, error) {
	var (
		seg    *slab.Segment
		fid    uint32
		offset uint32
	)
	maxSize := c.mgr.MaxSize()
	if int64(len(buf)) > maxSize {
		return nil, 0, 0, fmt.Errorf("dirpage page (%d bytes) exceeds segment cap (%d)", len(buf), maxSize)
	}
	err := c.mgr.WithLock(func() error {
		active, activeFid, err := c.mgr.EnsureActiveLocked()
		if err != nil {
			return err
		}
		// dirpage segments have HeaderSize=0 so cur starts at 0 for fresh
		// segments; reload populated Size from on-disk extent.
		cur := uint32(active.Size())
		if int64(cur)+int64(len(buf)) > maxSize {
			active, activeFid, err = c.mgr.RotateLocked(cur)
			if err != nil {
				return err
			}
			cur = 0
		}
		seg = active
		fid = activeFid
		offset = cur
		return nil
	})
	if err != nil {
		return nil, 0, 0, err
	}
	seg.Lock.Lock()
	if err := seg.Write(offset, buf); err != nil {
		seg.Lock.Unlock()
		return nil, 0, 0, err
	}
	seg.Lock.Unlock()
	return seg, fid, offset, nil
}

// reload scans the slab and rebuilds the in-memory page index. Called
// from Open. Bad/truncated trailing records stop the scan for that
// segment but don't fail Open — Derived semantics tolerate partial loss.
func (c *Cache) reload() error {
	for _, fid := range c.mgr.ListFIDs() {
		if err := c.reloadSegment(fid); err != nil {
			return err
		}
	}
	return nil
}

func (c *Cache) reloadSegment(fid uint32) error {
	seg, release, err := c.mgr.AcquireRead(fid)
	if err != nil {
		return err
	}
	size := uint32(seg.Size())
	if size == 0 {
		release()
		return nil
	}
	buf, err := seg.Read(0, size)
	if err != nil {
		release()
		return err
	}
	// Local copy so we drop the read pin before mutating the index. The
	// underlying mmap stays alive after release, but copying here avoids
	// holding the segment read lock through index mutation (which itself
	// is purely in-memory but kept short for clarity).
	body := make([]byte, len(buf))
	copy(body, buf)
	release()

	cursor := 0
	type loaded struct {
		hdr pageHeader
		loc pageLoc
	}
	groups := make(map[PageKey][]loaded)
	for cursor < len(body) {
		hdr, _, n, err := decodePage(body[cursor:])
		if err != nil {
			// Stop at first bad/truncated record; remaining bytes are
			// either unfinished writes or future writers' space.
			break
		}
		key := PageKey{Mount: hdr.Mount, Parent: hdr.Parent, StartAfter: hdr.StartAfter, Limit: hdr.Limit}
		groups[key] = append(groups[key], loaded{
			hdr: hdr,
			loc: pageLoc{
				Fid:    fid,
				Offset: uint32(cursor),
				Length: uint32(n),
				PageNo: hdr.PageNo,
			},
		})
		cursor += n
	}

	c.pageMu.Lock()
	defer c.pageMu.Unlock()
	for key, items := range groups {
		// Existing entry from another segment (or a later materialization
		// in the same segment): keep the latest frontier we've seen.
		latest := items[0].hdr.Frontier
		for _, it := range items[1:] {
			if it.hdr.Frontier > latest {
				latest = it.hdr.Frontier
			}
		}
		var locs []pageLoc
		for _, it := range items {
			if it.hdr.Frontier == latest {
				locs = append(locs, it.loc)
			}
		}
		sort.Slice(locs, func(i, j int) bool { return locs[i].PageNo < locs[j].PageNo })
		if cur, ok := c.pages[key]; !ok || latest > cur.frontier {
			c.pages[key] = &pageSet{frontier: latest, pages: locs}
			c.indexPageLocked(key)
		}
	}
	return nil
}

func (c *Cache) indexPageLocked(key PageKey) {
	dir := key.Directory()
	pages := c.dirs[dir]
	if pages == nil {
		pages = make(map[PageKey]bool)
		c.dirs[dir] = pages
	}
	pages[key] = true
}
