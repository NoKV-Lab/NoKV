// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package slab provides the append-only mmap-backed segment substrate used by
// sideband physical storage consumers such as DirPage and NegativeCache. The
// substrate is intentionally minimal: it knows how to
// append bytes, read a (offset, length) range, seal/truncate/sync the file,
// and remap between read-only and writable. It does NOT know what the bytes
// mean — there is no value pointer, no bucket routing, no business GC, no
// product-level indexing. Those are consumer concerns.
package slab

import (
	"fmt"
	"io"
	"math"
	"os"
	"sync"
	"sync/atomic"

	"github.com/feichai0017/NoKV/storage/file"
	"github.com/feichai0017/NoKV/storage/vfs"
	"github.com/feichai0017/NoKV/utils"
	"github.com/pkg/errors"
)

// Segment is an append-only mmap-backed file. It tracks a high-water mark
// (the largest end offset ever written) and serializes Write callers via the
// embedded RWMutex. Read callers must hold a read lock while dereferencing
// the returned slice (it is backed by the live mmap region).
type Segment struct {
	Lock sync.RWMutex
	FID  uint32
	size atomic.Uint32
	f    *file.MmapFile
	ro   bool
}

// Open creates or attaches the mmap region described by opt. The mapped
// region is sized to opt.MaxSz (fresh files are truncated up to that size).
//
// IMPORTANT — size invariant: Open initializes the *logical high-water mark*
// to zero, NOT to the file's on-disk byte count. For freshly-allocated files
// the on-disk size equals the preallocated mmap capacity, which has no
// meaningful relationship to "how many bytes have been logically written".
// Treating capacity as high-water made Read's EOF check (offset+len > size)
// meaningless and risked returning trailing junk to a consumer that didn't
// maintain its own logical cursor.
//
// Callers reopening an existing sealed segment (whose on-disk size has been
// truncated to its logical extent by DoneWriting or VerifyDir/sanitize) must
// follow Open with LoadSizeFromFile() to restore the high-water from stat.
// Callers creating a fresh segment do not need to: Bootstrap (or the first
// Write) advances the high-water via the monotonic CAS in Write.
func (s *Segment) Open(opt *file.Options) error {
	var err error
	s.FID = uint32(opt.FID)
	s.Lock = sync.RWMutex{}
	flag := opt.Flag
	if flag == 0 {
		flag = os.O_CREATE | os.O_RDWR
	}
	s.f, err = file.OpenMmapFile(opt.FS, opt.FileName, flag, opt.MaxSz)
	if err != nil {
		return errors.Wrapf(err, "unable to open segment file")
	}
	s.size.Store(0)
	s.ro = flag == os.O_RDONLY
	return nil
}

// Read returns a slice over the mmap region for [offset, offset+length).
// Caller must hold s.Lock for read for the lifetime of the returned slice.
//
// Returns io.EOF when the range falls outside the mapped region or beyond
// the high-water mark. The high-water guarantee depends on the upper-layer
// publish discipline: callers must not surface an offset to readers until
// the corresponding Write has returned (slab-substrate publish invariant in
// §4.4). Within the high-water mark there can be holes (reserved-but-not-
// yet-written ranges); the publish discipline is what keeps readers from
// observing them.
func (s *Segment) Read(offset, length uint32) (buf []byte, err error) {
	// Do not convert size to uint32, because s.f.Data can be 4GB which
	// would overflow uint32 to 0 and turn every Read into an EOF.
	mmapSize := int64(len(s.f.Data))
	hi := s.size.Load()
	end64 := int64(offset) + int64(length)
	if int64(offset) >= mmapSize || end64 > mmapSize || end64 > int64(hi) {
		return nil, io.EOF
	}
	return s.f.Bytes(int(offset), int(length))
}

// DoneWriting seals the active region at offset: flushes pages, truncates
// the file to the actual written size, and re-mmaps. After DoneWriting the
// segment is still writable (callers can SetReadOnly to harden it).
func (s *Segment) DoneWriting(offset uint32) error {
	// Async flush + sync before acquiring the writer lock. We hold no lock
	// here because callers seal segments after publishing all writes.
	_ = s.f.SyncAsyncRange(0, int64(offset))
	if err := s.f.Sync(); err != nil {
		return errors.Wrapf(err, "Unable to sync segment: %q", s.FileName())
	}

	s.Lock.Lock()
	defer s.Lock.Unlock()

	if err := s.f.Truncate(int64(offset)); err != nil {
		return errors.Wrapf(err, "Unable to truncate segment: %q", s.FileName())
	}
	s.size.Store(offset)

	if err := s.File().Sync(); err != nil {
		return errors.Wrapf(err, "Unable to sync file descriptor (metadata) after truncate: %q", s.FileName())
	}

	if err := s.Init(); err != nil {
		return errors.Wrapf(err, "failed to initialize file %s", s.FileName())
	}

	// Drop freshly written pages from page cache; cold segments rely on OS
	// cache rather than user cache.
	_ = s.f.Advise(utils.AccessPatternDontNeed)
	return nil
}

// Write appends buf at offset. The high-water mark is advanced to
// offset+len(buf) only if it would grow — see the monotonic CAS comment
// below for why this matters.
func (s *Segment) Write(offset uint32, buf []byte) (err error) {
	if s.ro {
		return fmt.Errorf("segment %s is read-only", s.FileName())
	}
	err = s.f.AppendBuffer(offset, buf)
	if err == nil {
		end := offset + uint32(len(buf))
		// Some consumers hand out non-overlapping offset ranges without
		// serializing the subsequent Write calls. Two concurrent writes can
		// therefore race on s.size: if the writer with the larger
		// reservation finishes first, a plain Store from the later writer
		// would shrink the high-water back below already-published data.
		// Use a monotonic CAS so the high-water only ever advances.
		for {
			cur := s.size.Load()
			if end <= cur || s.size.CompareAndSwap(cur, end) {
				break
			}
		}
	}
	return err
}

// Truncate explicitly sets the high-water (and underlying file) to offset.
// Unlike Write this can shrink the high-water; consumers use it for failed
// append rollback and ad-hoc segment rebuild.
func (s *Segment) Truncate(offset int64) error {
	if err := s.f.Truncate(offset); err != nil {
		return err
	}
	if offset < 0 {
		offset = 0
	}
	if offset > math.MaxUint32 {
		offset = math.MaxUint32
	}
	s.size.Store(uint32(offset))
	return nil
}

func (s *Segment) Close() error { return s.f.Close() }

// Size returns the logical high-water mark (the largest offset+length ever
// committed by Write or set explicitly by Truncate / LoadSizeFromFile).
// This is the upper bound that Read enforces; reads beyond it return io.EOF.
func (s *Segment) Size() int64 { return int64(s.size.Load()) }

// Capacity returns the mapped region size in bytes. This is the physical
// upper bound on Read addresses; consumers should not reach past Capacity
// even with explicit Truncate(>Capacity) since the underlying mmap is fixed.
// Capacity is set at Open time from opt.MaxSz; it changes only when an
// AppendBuffer-induced grow re-mmaps the file.
func (s *Segment) Capacity() int64 { return int64(len(s.f.Data)) }

// LoadSizeFromFile sets the logical high-water mark from the file's current
// on-disk size. This is the right thing for reopening a sealed segment whose
// physical size already equals its logical extent (sealed via DoneWriting or
// truncated by VerifyDir/sanitize). It is the wrong thing for a fresh,
// preallocated file whose on-disk size is its capacity.
func (s *Segment) LoadSizeFromFile() error {
	fstat, err := s.f.File.Stat()
	if err != nil {
		return errors.Wrapf(err, "Unable to check stat for %q", s.FileName())
	}
	sz := max(fstat.Size(), 0)
	if sz > math.MaxUint32 {
		panic(fmt.Errorf("[Segment.LoadSizeFromFile] sz > math.MaxUint32"))
	}
	s.size.Store(uint32(sz))
	return nil
}

// Bootstrap reserves and zeroes a header region of the given size. headerSize
// of 0 is a no-op. Used by consumers that need a small fixed-format header
// at the start of every segment.
func (s *Segment) Bootstrap(headerSize int) error {
	if s == nil {
		return fmt.Errorf("segment bootstrap: nil receiver")
	}
	if s.ro {
		return fmt.Errorf("segment bootstrap: read-only file %s", s.FileName())
	}
	if headerSize <= 0 {
		return nil
	}
	header := make([]byte, headerSize)
	return s.Write(0, header)
}

// Init resyncs the high-water from the on-disk file size. Used after
// DoneWriting (when the file has been truncated to its logical size) to
// reflect the post-truncate extent. Equivalent to LoadSizeFromFile but
// silently no-ops when the file is empty (preserving older behavior used
// by DoneWriting → Init re-init flow).
func (s *Segment) Init() error {
	fstat, err := s.f.File.Stat()
	if err != nil {
		return errors.Wrapf(err, "Unable to check stat for %q", s.FileName())
	}
	sz := fstat.Size()
	if sz == 0 {
		return nil
	}
	if sz > math.MaxUint32 {
		panic(fmt.Errorf("[Segment.Init] sz > math.MaxUint32"))
	}
	s.size.Store(uint32(sz))
	return nil
}

func (s *Segment) FileName() string { return s.f.File.Name() }

func (s *Segment) Seek(offset int64, whence int) (int64, error) {
	return s.f.File.Seek(offset, whence)
}

// FileFD exposes the OS descriptor capability when the segment is backed
// by a real OS handle.
func (s *Segment) FileFD() (uintptr, bool) {
	if s == nil || s.f == nil {
		return 0, false
	}
	return vfs.FileFD(s.f.File)
}

func (s *Segment) File() vfs.File { return s.f.File }

// Sync flushes the mmap region. Caller must hold s.Lock.
func (s *Segment) Sync() error { return s.f.Sync() }

// SetReadOnly remaps the file as read-only and advises the OS to drop pages.
func (s *Segment) SetReadOnly() error {
	s.Lock.Lock()
	defer s.Lock.Unlock()
	if s.ro {
		return nil
	}
	if err := s.f.Remap(false); err != nil {
		return err
	}
	_ = s.f.Advise(utils.AccessPatternDontNeed)
	s.ro = true
	return nil
}

// SetWritable remaps the file back to writable mode.
func (s *Segment) SetWritable() error {
	s.Lock.Lock()
	defer s.Lock.Unlock()
	if !s.ro {
		return nil
	}
	if err := s.f.Remap(true); err != nil {
		return err
	}
	s.ro = false
	return nil
}
