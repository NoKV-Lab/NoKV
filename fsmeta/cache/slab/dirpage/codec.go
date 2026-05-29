// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package dirpage implements the DirPageSlab Derived consumer of
// fsmeta/cache/slab. It materializes individual (mount, parent_inode, cursor,
// limit) directory pages as packed records in a slab so that ReadDirPlus
// can short-circuit a fan-out of one LSM prefix scan + N inode Gets into
// a single sequential page read.
//
// Wire format and consistency model in brief:
//
//   - LSM is authoritative; pages here are best-effort cache (Derived
//     consistency class).
//   - Each materialized directory becomes one or more page records in a
//     slab segment, sequenced by page_no.
//   - Each page carries a frontier (an opaque uint64 supplied by the
//     caller; in fsmeta this is the WatchSubtree event cursor). Lookup
//     succeeds only if every page's frontier matches the caller's
//     supplied current frontier.
//   - Invalidate marks a key so that any future Lookup against the
//     current frontier-set returns a miss until MaterializeAsync writes
//     a new page set with a fresh frontier.
//
// dirpage does not know about fsmeta types. The caller maps its own
// MountID/InodeID/InodeRecord onto (uint32 mount, uint64 parent,
// uint64 inode, []byte attrBlob). This keeps fsmeta/cache/slab/dirpage free
// of upper-layer dependencies.
package dirpage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// dirPageMagic identifies a dirpage record at offset 0 of its on-slab
// frame. The four ASCII bytes are "DPSL" little-endian.
const dirPageMagic uint32 = 0x4c535044

// dirPageVersion is the current dirpage slab format. NoKV has not shipped a
// stable dirpage cache format yet, so development-time incompatible changes
// update this format directly instead of carrying migration branches.
const dirPageVersion uint16 = 1

// recordHeaderFixed is the size of the magic + version prefix we read
// before the variable-length body to discriminate dirpage records from
// surrounding bytes (matters because slab segments may legitimately hold
// other consumers' records in the future).
const recordHeaderFixed = 4 + 2 // magic + version

// DirectoryKey identifies the mutation invalidation scope for one directory.
// A directory can have many cached PageKeys, but all of them become stale when
// any child dentry changes.
type DirectoryKey struct {
	Mount  uint64
	Parent uint64
}

// PageKey identifies one cached directory page. Mount and Parent are opaque to
// dirpage; the consumer maps its own typed identifiers to these uint widths.
// StartAfter and Limit are part of the identity because the cache stores one
// caller-visible page, not a complete logical directory.
type PageKey struct {
	Mount      uint64
	Parent     uint64
	StartAfter string
	Limit      uint32
}

func (k PageKey) Directory() DirectoryKey {
	return DirectoryKey{Mount: k.Mount, Parent: k.Parent}
}

// Entry is one materialized directory entry. AttrBlob is the consumer-
// chosen serialization of the inode attributes (e.g. fsmeta/model.InodeRecord
// encoded via layout.EncodeInodeValue); dirpage never inspects it.
type Entry struct {
	Name     []byte
	Inode    uint64
	AttrBlob []byte
}

// pageHeader is the decoded prefix of a dirpage record (everything before
// the entries).
type pageHeader struct {
	Mount      uint64
	Parent     uint64
	StartAfter string
	Limit      uint32
	PageNo     uint32
	Frontier   uint64
	EntryCount uint32
}

// estimatePageSize gives the on-disk footprint of a page with the listed
// entries. Used by the splitter to decide when to roll over to the next
// page. Slightly pessimistic (assumes max varint width).
func estimatePageSize(startAfter string, entries []Entry) int {
	const overhead = recordHeaderFixed +
		binary.MaxVarintLen64*3 + // mount + parent + frontier
		binary.MaxVarintLen64 + // start_after_len
		binary.MaxVarintLen32*3 + // limit + page_no + entry_count
		4 // crc32
	total := overhead
	total += len(startAfter)
	for _, e := range entries {
		total += binary.MaxVarintLen64 // name_len
		total += len(e.Name)
		total += binary.MaxVarintLen64 // inode
		total += binary.MaxVarintLen64 // attr_blob_len
		total += len(e.AttrBlob)
	}
	return total
}

// encodePage serializes one page into dst (appending). Returns dst with
// the encoded bytes appended.
func encodePage(dst []byte, hdr pageHeader, entries []Entry) []byte {
	start := len(dst)

	// magic + version
	var fixed [recordHeaderFixed]byte
	binary.LittleEndian.PutUint32(fixed[0:4], dirPageMagic)
	binary.LittleEndian.PutUint16(fixed[4:6], dirPageVersion)
	dst = append(dst, fixed[:]...)

	// header (varint)
	dst = binary.AppendUvarint(dst, hdr.Mount)
	dst = binary.AppendUvarint(dst, hdr.Parent)
	dst = binary.AppendUvarint(dst, uint64(len(hdr.StartAfter)))
	dst = append(dst, hdr.StartAfter...)
	dst = binary.AppendUvarint(dst, uint64(hdr.Limit))
	dst = binary.AppendUvarint(dst, uint64(hdr.PageNo))
	dst = binary.AppendUvarint(dst, hdr.Frontier)
	dst = binary.AppendUvarint(dst, uint64(hdr.EntryCount))

	// entries
	for _, e := range entries {
		dst = binary.AppendUvarint(dst, uint64(len(e.Name)))
		dst = append(dst, e.Name...)
		dst = binary.AppendUvarint(dst, e.Inode)
		dst = binary.AppendUvarint(dst, uint64(len(e.AttrBlob)))
		dst = append(dst, e.AttrBlob...)
	}

	// CRC32 over everything from `start` to here
	crc := crc32.ChecksumIEEE(dst[start:])
	var crcBuf [4]byte
	binary.LittleEndian.PutUint32(crcBuf[:], crc)
	dst = append(dst, crcBuf[:]...)
	return dst
}

// decodePage parses one page from buf. Returns the header, decoded
// entries, and the number of bytes consumed (so callers iterating over
// a multi-record buffer can advance their cursor).
func decodePage(buf []byte) (pageHeader, []Entry, int, error) {
	if len(buf) < recordHeaderFixed+4 { // need at least header prefix + crc
		return pageHeader{}, nil, 0, errPageTruncated
	}
	magic := binary.LittleEndian.Uint32(buf[0:4])
	if magic != dirPageMagic {
		return pageHeader{}, nil, 0, errPageBadMagic
	}
	ver := binary.LittleEndian.Uint16(buf[4:6])
	if ver != dirPageVersion {
		return pageHeader{}, nil, 0, errPageBadVersion
	}

	cursor := recordHeaderFixed
	read := func() (uint64, error) {
		v, n := binary.Uvarint(buf[cursor:])
		if n <= 0 {
			return 0, errPageTruncated
		}
		cursor += n
		return v, nil
	}
	readBytes := func(n uint64) ([]byte, error) {
		// Keep corrupted length prefixes from overflowing int conversion or
		// slicing past the decoded frame. The cache is derived, so corrupt
		// frames are treated as misses by callers.
		if n > uint64(len(buf)-cursor) {
			return nil, errPageTruncated
		}
		out := buf[cursor : cursor+int(n)]
		cursor += int(n)
		return out, nil
	}

	mount, err := read()
	if err != nil {
		return pageHeader{}, nil, 0, err
	}
	parent, err := read()
	if err != nil {
		return pageHeader{}, nil, 0, err
	}
	startAfterLen, err := read()
	if err != nil {
		return pageHeader{}, nil, 0, err
	}
	startAfterBytes, err := readBytes(startAfterLen)
	if err != nil {
		return pageHeader{}, nil, 0, err
	}
	startAfter := string(startAfterBytes)
	limit, err := read()
	if err != nil {
		return pageHeader{}, nil, 0, err
	}
	pageNo, err := read()
	if err != nil {
		return pageHeader{}, nil, 0, err
	}
	frontier, err := read()
	if err != nil {
		return pageHeader{}, nil, 0, err
	}
	entryCount, err := read()
	if err != nil {
		return pageHeader{}, nil, 0, err
	}

	entries := make([]Entry, 0, entryCount)
	for range entryCount {
		nameLen, err := read()
		if err != nil {
			return pageHeader{}, nil, 0, err
		}
		nameBytes, err := readBytes(nameLen)
		if err != nil {
			return pageHeader{}, nil, 0, err
		}
		name := make([]byte, nameLen)
		copy(name, nameBytes)

		inode, err := read()
		if err != nil {
			return pageHeader{}, nil, 0, err
		}
		blobLen, err := read()
		if err != nil {
			return pageHeader{}, nil, 0, err
		}
		blobBytes, err := readBytes(blobLen)
		if err != nil {
			return pageHeader{}, nil, 0, err
		}
		blob := make([]byte, blobLen)
		copy(blob, blobBytes)

		entries = append(entries, Entry{Name: name, Inode: inode, AttrBlob: blob})
	}

	if cursor+4 > len(buf) {
		return pageHeader{}, nil, 0, errPageTruncated
	}
	want := binary.LittleEndian.Uint32(buf[cursor : cursor+4])
	got := crc32.ChecksumIEEE(buf[:cursor])
	if want != got {
		return pageHeader{}, nil, 0, errPageBadChecksum
	}
	cursor += 4

	hdr := pageHeader{
		Mount:      mount,
		Parent:     parent,
		StartAfter: startAfter,
		Limit:      uint32(limit),
		PageNo:     uint32(pageNo),
		Frontier:   frontier,
		EntryCount: uint32(entryCount),
	}
	return hdr, entries, cursor, nil
}

// splitIntoPages partitions entries into groups, each fitting under
// maxPageBytes when encoded. Empty entries returns one empty page so the
// caller still records "this directory is materialized as empty".
func splitIntoPages(startAfter string, entries []Entry, maxPageBytes int) [][]Entry {
	if len(entries) == 0 {
		return [][]Entry{nil}
	}
	if maxPageBytes <= 0 {
		return [][]Entry{entries}
	}
	var pages [][]Entry
	cur := []Entry{}
	curSize := estimatePageSize(startAfter, nil)
	for _, e := range entries {
		entrySize := binary.MaxVarintLen64 + len(e.Name) + binary.MaxVarintLen64 + binary.MaxVarintLen64 + len(e.AttrBlob)
		if len(cur) > 0 && curSize+entrySize > maxPageBytes {
			pages = append(pages, cur)
			cur = []Entry{}
			curSize = estimatePageSize(startAfter, nil)
		}
		cur = append(cur, e)
		curSize += entrySize
	}
	if len(cur) > 0 {
		pages = append(pages, cur)
	}
	return pages
}

// keyString is a debug helper for error messages.
func (k PageKey) keyString() string {
	return fmt.Sprintf("(mount=%d parent=%d start_after=%q limit=%d)", k.Mount, k.Parent, k.StartAfter, k.Limit)
}
