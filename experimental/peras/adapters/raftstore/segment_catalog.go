// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"bytes"
	"slices"

	fsperas "github.com/feichai0017/NoKV/experimental/peras/exec"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	entrykv "github.com/feichai0017/NoKV/txn/storage"
)

type SegmentCatalogStore interface {
	NewInternalIterator(*entrykv.Options) entrykv.Iterator
}

func LoadPerasSegmentCatalogs(store SegmentCatalogStore) ([]fsperas.SegmentCatalogRecord, error) {
	if store == nil {
		return nil, fsperas.ErrSegmentCatalogStoreRequired
	}
	it := store.NewInternalIterator(&entrykv.Options{IsAsc: true})
	if it == nil {
		return nil, fsperas.ErrSegmentCatalogStoreRequired
	}
	var records []fsperas.SegmentCatalogRecord
	it.Rewind()
	for it.Valid() {
		item := it.Item()
		if item == nil || item.Entry() == nil {
			it.Next()
			continue
		}
		entry := item.Entry()
		cf, userKey, _, ok := entrykv.SplitInternalKey(entry.Key)
		if !ok || cf != entrykv.CFDefault {
			it.Next()
			continue
		}
		parts, ok := layout.InspectKey(userKey)
		if !ok || parts.Kind != layout.KeyKindSegment || parts.SegmentRecord != layout.SegmentRecordObject {
			it.Next()
			continue
		}
		record, err := fsperas.DecodePerasSegmentCatalogRecord(entry.Value)
		if err != nil {
			_ = it.Close()
			return nil, err
		}
		if record.Root != parts.SegmentRoot {
			_ = it.Close()
			return nil, fsperas.ErrInvalidPerasSegment
		}
		records = append(records, record)
		it.Next()
	}
	if err := it.Close(); err != nil {
		return nil, err
	}
	slices.SortFunc(records, func(a, b fsperas.SegmentCatalogRecord) int {
		if a.InstallVersion < b.InstallVersion {
			return -1
		}
		if a.InstallVersion > b.InstallVersion {
			return 1
		}
		return bytes.Compare(a.Root[:], b.Root[:])
	})
	return records, nil
}

func LoadPerasSegmentCatalog(store SegmentCatalogStore, segment fsperas.PerasSegment) (fsperas.SegmentCatalogRecord, bool, error) {
	catalogKey, err := fsperas.PerasSegmentObjectKey(segment)
	if err != nil {
		return fsperas.SegmentCatalogRecord{}, false, err
	}
	return LoadPerasSegmentCatalogForObjectKey(store, segment, catalogKey)
}

func LoadPerasSegmentCatalogInstallForObjectKey(store SegmentCatalogStore, segment fsperas.PerasSegment, objectKey []byte) (bool, error) {
	if store == nil {
		return false, fsperas.ErrSegmentCatalogStoreRequired
	}
	bucket, err := perasSegmentObjectBucket(segment, objectKey)
	if err != nil {
		return false, err
	}
	canonicalObjectKey, err := fsperas.PerasSegmentObjectKey(segment)
	if err != nil {
		return false, err
	}
	indexKey, err := layout.EncodeSegmentCatalogIndexKey(bucket.mount, bucket.bucket, segment.Root)
	if err != nil {
		return false, err
	}
	it := store.NewInternalIterator(&entrykv.Options{IsAsc: true})
	if it == nil {
		return false, fsperas.ErrSegmentCatalogStoreRequired
	}
	defer func() { _ = it.Close() }()

	it.Seek(entrykv.InternalKey(entrykv.CFDefault, indexKey, entrykv.MaxVersion))
	if !it.Valid() {
		return false, nil
	}
	item := it.Item()
	if item == nil || item.Entry() == nil {
		return false, nil
	}
	entry := item.Entry()
	cf, userKey, _, ok := entrykv.SplitInternalKey(entry.Key)
	if !ok || cf != entrykv.CFDefault || !bytes.Equal(userKey, indexKey) {
		return false, nil
	}
	record, err := fsperas.DecodePerasSegmentCatalogIndexRecord(entry.Value)
	if err != nil {
		return false, err
	}
	if record.Root != segment.Root || !bytes.Equal(record.ObjectKey, canonicalObjectKey) {
		return false, fsperas.ErrInvalidPerasSegment
	}
	return true, nil
}

func LoadPerasSegmentCatalogIndexInstall(store SegmentCatalogStore, root [32]byte, routingKey, canonicalObjectKey []byte) (bool, error) {
	if store == nil {
		return false, fsperas.ErrSegmentCatalogStoreRequired
	}
	route, err := inspectPerasSegmentObjectKey(root, routingKey)
	if err != nil {
		return false, err
	}
	if _, err := inspectPerasSegmentObjectKey(root, canonicalObjectKey); err != nil {
		return false, fsperas.ErrInvalidPerasSegment
	}
	indexKey, err := layout.EncodeSegmentCatalogIndexKey(route.mount, route.bucket, root)
	if err != nil {
		return false, err
	}
	it := store.NewInternalIterator(&entrykv.Options{IsAsc: true})
	if it == nil {
		return false, fsperas.ErrSegmentCatalogStoreRequired
	}
	defer func() { _ = it.Close() }()

	it.Seek(entrykv.InternalKey(entrykv.CFDefault, indexKey, entrykv.MaxVersion))
	if !it.Valid() {
		return false, nil
	}
	item := it.Item()
	if item == nil || item.Entry() == nil {
		return false, nil
	}
	entry := item.Entry()
	cf, userKey, _, ok := entrykv.SplitInternalKey(entry.Key)
	if !ok || cf != entrykv.CFDefault || !bytes.Equal(userKey, indexKey) {
		return false, nil
	}
	record, err := fsperas.DecodePerasSegmentCatalogIndexRecord(entry.Value)
	if err != nil {
		return false, err
	}
	if record.Root != root || !bytes.Equal(record.ObjectKey, canonicalObjectKey) {
		return false, fsperas.ErrInvalidPerasSegment
	}
	return true, nil
}

func LoadPerasSegmentCatalogForObjectKey(store SegmentCatalogStore, segment fsperas.PerasSegment, catalogKey []byte) (fsperas.SegmentCatalogRecord, bool, error) {
	if store == nil {
		return fsperas.SegmentCatalogRecord{}, false, fsperas.ErrSegmentCatalogStoreRequired
	}
	if _, err := perasSegmentObjectBucket(segment, catalogKey); err != nil {
		return fsperas.SegmentCatalogRecord{}, false, err
	}
	it := store.NewInternalIterator(&entrykv.Options{IsAsc: true})
	if it == nil {
		return fsperas.SegmentCatalogRecord{}, false, fsperas.ErrSegmentCatalogStoreRequired
	}
	defer func() { _ = it.Close() }()

	it.Seek(entrykv.InternalKey(entrykv.CFDefault, catalogKey, entrykv.MaxVersion))
	if !it.Valid() {
		return fsperas.SegmentCatalogRecord{}, false, nil
	}
	item := it.Item()
	if item == nil || item.Entry() == nil {
		return fsperas.SegmentCatalogRecord{}, false, nil
	}
	entry := item.Entry()
	cf, userKey, _, ok := entrykv.SplitInternalKey(entry.Key)
	if !ok || cf != entrykv.CFDefault || !bytes.Equal(userKey, catalogKey) {
		return fsperas.SegmentCatalogRecord{}, false, nil
	}
	record, err := fsperas.DecodePerasSegmentCatalogRecord(entry.Value)
	if err != nil {
		return fsperas.SegmentCatalogRecord{}, false, err
	}
	if record.Root != segment.Root {
		return fsperas.SegmentCatalogRecord{}, false, fsperas.ErrInvalidPerasSegment
	}
	return record, true, nil
}
