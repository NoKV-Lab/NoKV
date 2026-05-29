// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"sync"
	"time"

	"github.com/feichai0017/NoKV/utils"
)

var entryPool = sync.Pool{
	New: func() any {
		return &Entry{}
	},
}

// Entry is the generic key/value mutation container used across NoKV's MVCC
// layer.
//
// It can hold:
//   - fully encoded internal key/value payloads destined for WAL or raw KV apply
//   - borrowed or detached results produced by iterators and table lookups
//   - scratch entries used by tests and internal buffering
//
// When an Entry is serialized with EncodeEntryTo, the binary layout is:
//
//	+---------+------+--------+--------+
//	| Header  | Key  | Value  | CRC32  |
//	+---------+------+--------+--------+
//	| varints | raw  | raw    | 4 bytes|
//	+---------+------+--------+--------+
//
// Entry itself does not validate key/value semantics. Callers that persist
// entries are responsible for ensuring Key and Value already use the expected
// bytes for that path:
//   - public APIs usually start from user keys
//   - engine-internal storage paths usually carry internal keys
//   - range/filter helpers often operate on base keys
type Entry struct {
	Key       []byte
	Value     []byte
	ExpiresAt uint64

	CF ColumnFamily

	Meta         byte
	Version      uint64
	Offset       uint32
	Hlen         int // Length of the header.
	ValThreshold int64

	utils.RefCount
}

// IncrRef increments the entry reference count.
// It wraps RefCount.Incr to add nil-receiver safety.
func (e *Entry) IncrRef() {
	if e == nil {
		return
	}
	e.Incr()
}

// DecrRef decrements the entry reference count and releases it to the pool when it reaches zero.
// It panics on refcount underflow (decrement past zero), which indicates a
// lifecycle bug in the caller.
func (e *Entry) DecrRef() {
	if e == nil {
		return
	}
	if e.Decr() == 0 {
		e.reset()
		entryPool.Put(e)
	}
}

func (e *Entry) reset() {
	e.Key = nil
	e.Value = nil
	e.ExpiresAt = 0
	e.CF = CFDefault
	e.Meta = 0
	e.Version = 0
	e.Offset = 0
	e.Hlen = 0
	e.ValThreshold = 0
	e.Reset()
}

func acquireEntry() *Entry {
	e := entryPool.Get().(*Entry)
	e.IncrRef()
	return e
}

// NewEntry creates a lightweight entry from arbitrary key/value bytes.
//
// It does not parse or validate key layout; keyBytes may be a user key, base
// key, internal key, or path-local scratch bytes. CF is initialized to
// CFDefault and Version remains unset (0) until filled by a caller that knows
// MVCC context.
func NewEntry(keyBytes, valueBytes []byte) *Entry {
	e := acquireEntry()
	e.Key = keyBytes
	e.Value = valueBytes
	e.CF = CFDefault
	return e
}

// NewInternalEntry creates an Entry whose Key is encoded from the supplied
// userKey into canonical internal-key layout.
//
// Ownership note: userKey is encoded into a newly allocated internal-key buffer,
// while value is referenced directly (no deep copy). Callers must keep value
// immutable until the entry is no longer used.
//
// This helper also sets CF and Version to the supplied MVCC context.
func NewInternalEntry(cf ColumnFamily, userKey []byte, version uint64, valueBytes []byte, meta byte, expiresAt uint64) *Entry {
	if !cf.Valid() {
		cf = CFDefault
	}
	e := acquireEntry()
	e.Key = InternalKey(cf, userKey, version)
	e.Value = valueBytes
	e.CF = cf
	e.Version = version
	e.Meta = meta
	e.ExpiresAt = expiresAt
	return e
}

// NewValueStructEntry wraps an internal-key/value-struct lookup result in a
// pooled Entry. internalKey is expected to already use canonical internal-key
// layout; CF and Version are derived when parsing succeeds and left at defaults
// otherwise.
func NewValueStructEntry(internalKey []byte, vs ValueStruct) *Entry {
	e := acquireEntry()
	e.Key = internalKey
	e.Value = vs.Value
	e.ExpiresAt = vs.ExpiresAt
	e.Meta = vs.Meta
	e.CF = CFDefault
	e.Version = 0
	if cf, _, version, ok := SplitInternalKey(internalKey); ok {
		e.CF = cf
		e.Version = version
	}
	return e
}

// Entry returns itself to satisfy iterator item interfaces.
func (e *Entry) Entry() *Entry {
	return e
}

// PopulateInternalMeta parses e.Key as an internal key and fills CF/Version.
// It returns false when the key is not in canonical internal-key format.
func (e *Entry) PopulateInternalMeta() bool {
	if e == nil {
		return false
	}
	cf, _, ts, ok := SplitInternalKey(e.Key)
	if !ok {
		e.CF = CFDefault
		e.Version = 0
		return false
	}
	e.CF = cf
	e.Version = ts
	return true
}

// IsDeletedOrExpired reports whether the entry is a tombstone or has passed its expiry.
func (e *Entry) IsDeletedOrExpired() bool {
	if e == nil || e.Value == nil {
		return true
	}
	if e.Meta&BitDelete > 0 {
		return true
	}
	if e.ExpiresAt == 0 {
		return false
	}
	return e.ExpiresAt <= uint64(time.Now().Unix())
}

// WithTTL sets the TTL for the entry.
func (e *Entry) WithTTL(dur time.Duration) *Entry {
	e.ExpiresAt = uint64(time.Now().Add(dur).Unix())
	return e
}

// EncodedValueSize reports the encoded size of the value payload stored in the
// entry. It does not include entry header bytes, key bytes, or CRC32.
func (e *Entry) EncodedValueSize() uint32 {
	sz := len(e.Value)
	enc := sizeVarint(uint64(e.Meta))
	enc += sizeVarint(e.ExpiresAt)
	return uint32(sz + enc)
}

// EstimateSize estimates the inline memory footprint used by write admission.
func (e *Entry) EstimateSize() int {
	return len(e.Key) + len(e.Value) + sizeVarint(uint64(e.Meta)) + sizeVarint(e.ExpiresAt)
}

// IsRangeDelete reports whether the entry is a range tombstone.
func (e *Entry) IsRangeDelete() bool {
	return e.Meta&BitRangeDelete != 0
}

// RangeEnd returns the end key from the Value field for range tombstones.
func (e *Entry) RangeEnd() []byte {
	return e.Value
}
