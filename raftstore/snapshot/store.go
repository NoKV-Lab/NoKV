// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"bytes"
	"fmt"
	"io"

	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	txnstore "github.com/feichai0017/NoKV/txn/storage"
)

type entryStore interface {
	ApplyInternalEntries(entries []*txnstore.Entry) error
	NewInternalIterator(opt *txnstore.Options) txnstore.Iterator
}

type entryMaterializer interface {
	MaterializeInternalEntry(src *txnstore.Entry) (*txnstore.Entry, error)
}

// MVCCStore adapts the transaction storage contract into raftstore snapshot
// payloads. It owns no storage engine details; entries remain in canonical
// NoKV internal-key format.
type MVCCStore struct {
	store entryStore
}

// NewStore constructs a snapshot store over an MVCC internal-entry store.
func NewStore(store entryStore) MVCCStore {
	return MVCCStore{store: store}
}

func (s MVCCStore) ExportSnapshot(region localmeta.RegionMeta) ([]byte, error) {
	var buf bytes.Buffer
	if _, err := s.ExportSnapshotTo(&buf, region); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (s MVCCStore) ImportSnapshot(payload []byte) (*ImportResult, error) {
	if len(payload) == 0 {
		return nil, fmt.Errorf("snapshot: empty entry payload")
	}
	return s.ImportSnapshotFrom(bytes.NewReader(payload))
}

func (s MVCCStore) ExportSnapshotTo(w io.Writer, region localmeta.RegionMeta) (Descriptor, error) {
	if s.store == nil {
		return Descriptor{}, fmt.Errorf("snapshot: export requires MVCC store")
	}
	if w == nil {
		return Descriptor{}, fmt.Errorf("snapshot: export requires writer")
	}
	entries, err := collectRegionEntries(s.store, region)
	if err != nil {
		return Descriptor{}, err
	}
	defer releaseEntries(entries)
	return WritePayloadTo(w, region, entries)
}

func (s MVCCStore) ImportSnapshotFrom(r io.Reader) (*ImportResult, error) {
	if s.store == nil {
		return nil, fmt.Errorf("snapshot: import requires MVCC store")
	}
	descriptor, entries, err := ReadPayloadFrom(r)
	if err != nil {
		return nil, err
	}
	defer releaseEntries(entries)
	if len(entries) > 0 {
		if err := s.store.ApplyInternalEntries(entries); err != nil {
			return nil, fmt.Errorf("snapshot: apply region entries: %w", err)
		}
	}
	return &ImportResult{Descriptor: descriptor}, nil
}

func collectRegionEntries(store entryStore, region localmeta.RegionMeta) ([]*txnstore.Entry, error) {
	if store == nil {
		return nil, fmt.Errorf("snapshot: nil MVCC store")
	}
	var entries []*txnstore.Entry
	for cf := txnstore.CFDefault; cf <= txnstore.CFWrite; cf++ {
		cfEntries, err := collectRegionCFEntries(store, region, cf)
		if err != nil {
			releaseEntries(entries)
			return nil, err
		}
		entries = append(entries, cfEntries...)
	}
	return entries, nil
}

func collectRegionCFEntries(store entryStore, region localmeta.RegionMeta, cf txnstore.ColumnFamily) ([]*txnstore.Entry, error) {
	opt := &txnstore.Options{
		IsAsc:      true,
		LowerBound: snapshotLowerBound(cf, region.StartKey),
		UpperBound: snapshotUpperBound(cf, region.EndKey),
	}
	iter := store.NewInternalIterator(opt)
	if iter == nil {
		return nil, fmt.Errorf("snapshot: nil internal iterator")
	}
	defer func() { _ = iter.Close() }()

	var entries []*txnstore.Entry
	for iter.Rewind(); iter.Valid(); iter.Next() {
		item := iter.Item()
		if item == nil || item.Entry() == nil {
			continue
		}
		src := item.Entry()
		entryCF, userKey, _, ok := txnstore.SplitInternalKey(src.Key)
		if !ok {
			releaseEntries(entries)
			return nil, fmt.Errorf("snapshot: invalid internal key")
		}
		if entryCF != cf || !keyInRegion(region, userKey) {
			continue
		}
		entry, err := materializeEntry(store, src)
		if err != nil {
			releaseEntries(entries)
			return nil, fmt.Errorf("snapshot: materialize entry: %w", err)
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func materializeEntry(store entryStore, src *txnstore.Entry) (*txnstore.Entry, error) {
	if m, ok := store.(entryMaterializer); ok {
		return m.MaterializeInternalEntry(src)
	}
	cf, userKey, version, ok := txnstore.SplitInternalKey(src.Key)
	if !ok {
		return nil, fmt.Errorf("invalid internal key")
	}
	return txnstore.NewInternalEntry(
		cf,
		txnstore.SafeCopy(nil, userKey),
		version,
		txnstore.SafeCopy(nil, src.Value),
		src.Meta,
		src.ExpiresAt,
	), nil
}

func snapshotLowerBound(cf txnstore.ColumnFamily, start []byte) []byte {
	if len(start) == 0 {
		return txnstore.InternalKey(cf, nil, txnstore.MaxVersion)
	}
	return txnstore.InternalKey(cf, start, txnstore.MaxVersion)
}

func snapshotUpperBound(cf txnstore.ColumnFamily, end []byte) []byte {
	if len(end) == 0 {
		if cf == txnstore.CFWrite {
			return nil
		}
		return txnstore.InternalKey(cf+1, nil, txnstore.MaxVersion)
	}
	return txnstore.InternalKey(cf, end, txnstore.MaxVersion)
}

func releaseEntries(entries []*txnstore.Entry) {
	for _, entry := range entries {
		if entry != nil {
			entry.DecrRef()
		}
	}
}

func keyInRegion(region localmeta.RegionMeta, userKey []byte) bool {
	if len(region.StartKey) > 0 && bytes.Compare(userKey, region.StartKey) < 0 {
		return false
	}
	if len(region.EndKey) > 0 && bytes.Compare(userKey, region.EndKey) >= 0 {
		return false
	}
	return true
}

var _ Store = MVCCStore{}
