// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValueStruct(t *testing.T) {
	v := ValueStruct{
		Value:     []byte("feichai's kv"),
		Meta:      2,
		ExpiresAt: 213123123123,
	}
	data := make([]byte, v.EncodedSize())
	v.EncodeValue(data)
	var decoded ValueStruct
	decoded.DecodeValue(data)
	assert.Equal(t, decoded, v)
}

func TestEntryEncodeDecodeRoundTrip(t *testing.T) {
	val := ValueStruct{
		Meta:      0x2,
		Value:     []byte("inline-value"),
		ExpiresAt: 42,
	}
	entry := &Entry{
		Key:       InternalKey(CFWrite, []byte("foo"), 99),
		Value:     encodeValueStruct(val),
		Meta:      val.Meta,
		ExpiresAt: val.ExpiresAt,
	}
	var buf bytes.Buffer
	n, err := EncodeEntryTo(&buf, entry)
	require.NoError(t, err)
	require.Equal(t, buf.Len(), n)

	decoded, recordLen, err := DecodeEntryFrom(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	t.Cleanup(func() { decoded.DecrRef() })
	require.Equal(t, uint32(n), recordLen)

	assert.Equal(t, entry.Meta, decoded.Meta)
	assert.Equal(t, entry.ExpiresAt, decoded.ExpiresAt)
	assert.Equal(t, entry.Key, decoded.Key)
	assert.Equal(t, entry.Value, decoded.Value)

	cf, userKey, ts, ok := SplitInternalKey(decoded.Key)
	require.True(t, ok)
	assert.Equal(t, CFWrite, cf)
	assert.Equal(t, []byte("foo"), userKey)
	assert.Equal(t, uint64(99), ts)
	assert.Equal(t, cf, decoded.CF)
	assert.Equal(t, ts, decoded.Version)
}

func TestDecodeEntryFromEOFAndPartial(t *testing.T) {
	e := &Entry{
		Key:       InternalKey(CFDefault, []byte("bar"), 1),
		Value:     encodeValueStruct(ValueStruct{Value: []byte("baz")}),
		ExpiresAt: 5,
	}
	var buf bytes.Buffer
	_, err := EncodeEntryTo(&buf, e)
	require.NoError(t, err)

	decoded, _, err := DecodeEntryFrom(bytes.NewReader(nil))
	assert.Nil(t, decoded)
	assert.ErrorIs(t, err, io.EOF)

	truncated := buf.Bytes()[:buf.Len()-2]
	decoded, _, err = DecodeEntryFrom(bytes.NewReader(truncated))
	assert.Nil(t, decoded)
	assert.ErrorIs(t, err, ErrPartialEntry)
}

func TestEntryIterator(t *testing.T) {
	var buf bytes.Buffer
	for ts := uint64(1); ts <= 2; ts++ {
		e := &Entry{
			Key:       InternalKey(CFDefault, []byte("iter"), ts),
			Value:     encodeValueStruct(ValueStruct{Value: []byte{byte('a' + ts)}}),
			ExpiresAt: ts,
		}
		_, err := EncodeEntryTo(&buf, e)
		require.NoError(t, err)
	}

	it := NewEntryIterator(bytes.NewReader(buf.Bytes()))
	defer func() { require.NoError(t, it.Close()) }()

	count := 0
	for it.Next() {
		count++
		assert.Greater(t, it.RecordLen(), uint32(0))
		entry := it.Entry()
		require.NotNil(t, entry)
		cf, userKey, ts, ok := SplitInternalKey(entry.Key)
		require.True(t, ok)
		assert.Equal(t, CFDefault, cf)
		assert.Equal(t, []byte("iter"), userKey)
		assert.Equal(t, uint64(count), ts)
	}
	assert.Equal(t, 2, count)
	assert.ErrorIs(t, it.Err(), io.EOF)
}

func TestEntryHelpers(t *testing.T) {
	e := NewEntry([]byte("k"), []byte("v"))
	defer e.DecrRef()
	if e.Entry() != e {
		t.Fatalf("expected Entry() to return receiver")
	}
	if e.CF != CFDefault {
		t.Fatalf("expected default CF, got %v", e.CF)
	}

	e2 := NewEntry([]byte("lk"), []byte("lv"))
	e2.CF = CFLock
	defer e2.DecrRef()
	if e2.CF != CFLock {
		t.Fatalf("expected CFLock, got %v", e2.CF)
	}

	e2.CF = CFWrite
	if !e2.CF.Valid() {
		t.Fatalf("expected valid CF after setting column family")
	}

	if e2.IsDeletedOrExpired() {
		t.Fatalf("unexpected deleted/expired for live entry")
	}

	e2.Value = nil
	if !e2.IsDeletedOrExpired() {
		t.Fatalf("expected deleted entry")
	}

	e2.Value = []byte("lv")
	e2.WithTTL(1)
	if e2.ExpiresAt == 0 {
		t.Fatalf("expected ttl to set expiresAt")
	}

	if e2.EncodedValueSize() == 0 {
		t.Fatalf("expected encoded size > 0")
	}

	sz := e2.EstimateSize()
	expected := len(e2.Key) + len(e2.Value) + sizeVarint(uint64(e2.Meta)) + sizeVarint(e2.ExpiresAt)
	if sz != expected {
		t.Fatalf("unexpected estimate: got %d want %d", sz, expected)
	}
}

func TestNewInternalEntry(t *testing.T) {
	e := NewInternalEntry(CFWrite, []byte("uk"), 42, []byte("val"), BitDelete, 99)
	defer e.DecrRef()

	cf, userKey, ts, ok := SplitInternalKey(e.Key)
	require.True(t, ok)
	require.Equal(t, CFWrite, cf)
	require.Equal(t, []byte("uk"), userKey)
	require.Equal(t, uint64(42), ts)
	require.Equal(t, []byte("val"), e.Value)
	require.Equal(t, byte(BitDelete), e.Meta)
	require.Equal(t, uint64(99), e.ExpiresAt)
}

func TestEntryDecrRefUnderflowPanics(t *testing.T) {
	e := &Entry{Key: []byte("k"), Value: []byte("v")}
	require.Panics(t, func() {
		e.DecrRef()
	})
}

func TestValueHelpers(t *testing.T) {
	u32 := uint32(0xAABBCCDD)
	if got := BytesToU32(U32ToBytes(u32)); got != u32 {
		t.Fatalf("expected round-trip u32, got %x", got)
	}
	u64 := uint64(0x1122334455667788)
	if got := BytesToU64(U64ToBytes(u64)); got != u64 {
		t.Fatalf("expected round-trip u64, got %x", got)
	}
	if BytesToU32Slice(nil) != nil {
		t.Fatalf("expected nil slice for empty input")
	}
	if U32SliceToBytes(nil) != nil {
		t.Fatalf("expected nil bytes for empty input")
	}
	u32s := []uint32{1, 2, 3}
	raw := U32SliceToBytes(u32s)
	back := BytesToU32Slice(raw)
	require.Equal(t, u32s, back)

	var nilEntry *Entry
	if !nilEntry.IsDeletedOrExpired() {
		t.Fatalf("expected nil entry to be treated as deleted")
	}
	if !(&Entry{Value: nil}).IsDeletedOrExpired() {
		t.Fatalf("expected nil value entry to be treated as deleted")
	}
	if (&Entry{Value: []byte("v"), ExpiresAt: uint64(time.Now().Add(time.Hour).Unix())}).IsDeletedOrExpired() {
		t.Fatalf("expected live entry to be retained")
	}
	if !(&Entry{Meta: BitDelete, Value: []byte("v")}).IsDeletedOrExpired() {
		t.Fatalf("expected delete-marked entry to be deleted")
	}
	if !(&Entry{Value: []byte("v"), ExpiresAt: uint64(time.Now().Add(-time.Second).Unix())}).IsDeletedOrExpired() {
		t.Fatalf("expected expired entry to be deleted")
	}
	if (&Entry{Value: []byte("v"), ExpiresAt: uint64(time.Now().Add(time.Hour).Unix())}).IsDeletedOrExpired() {
		t.Fatalf("expected future expiry entry to be live")
	}
}

func TestNewValueStructEntry(t *testing.T) {
	key := InternalKey(CFWrite, []byte("user-key"), 42)
	vs := ValueStruct{
		Meta:      BitRangeDelete,
		Value:     []byte("value"),
		ExpiresAt: 123,
	}
	e := NewValueStructEntry(key, vs)
	t.Cleanup(func() { e.DecrRef() })
	require.Equal(t, key, e.Key)
	require.Equal(t, vs.Value, e.Value)
	require.Equal(t, vs.Meta, e.Meta)
	require.Equal(t, vs.ExpiresAt, e.ExpiresAt)
	require.Equal(t, CFWrite, e.CF)
	require.Equal(t, uint64(42), e.Version)
}

func encodeValueStruct(v ValueStruct) []byte {
	buf := make([]byte, v.EncodedSize())
	v.EncodeValue(buf)
	return buf
}
