// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"bytes"

	entrykv "github.com/feichai0017/NoKV/engine/kv"
	fsperas "github.com/feichai0017/NoKV/experimental/peras/exec"
	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/model"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	txnmvcc "github.com/feichai0017/NoKV/txn/mvcc"
)

func BuildMVCCSegmentInstallEntries(segment fsperas.PerasSegment, version uint64) ([]*entrykv.Entry, error) {
	if version == 0 || version == entrykv.MaxVersion {
		return nil, fsperas.ErrReplayVersionRequired
	}
	payload, err := fsperas.EncodePerasSegment(segment)
	if err != nil {
		return nil, err
	}
	digest, err := fsperas.PerasSegmentPayloadDigest(payload)
	if err != nil {
		return nil, err
	}
	return buildMVCCSegmentInstallEntriesWithVerifiedPayload(segment, version, payload, digest)
}

func buildMVCCSegmentInstallEntriesWithVerifiedPayload(segment fsperas.PerasSegment, version uint64, payload []byte, digest [32]byte) ([]*entrykv.Entry, error) {
	if version == 0 || version == entrykv.MaxVersion {
		return nil, fsperas.ErrReplayVersionRequired
	}
	stats := segment.Stats()
	entries := make([]*entrykv.Entry, 0, int(stats.EntryCount)*3+1)
	err := segment.ForEachEntry(func(entry fsperas.SegmentKV) error {
		var err error
		entries, err = appendMutationMVCCReplayEntries(entries, fsperas.ReplayMutation{
			Key:    entry.Key,
			Value:  entry.Value,
			Delete: entry.Delete,
		}, version)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		releaseMVCCReplayEntries(entries)
		return nil, err
	}
	catalogValue, err := fsperas.EncodePerasSegmentCatalogRecordWithPayload(segment, version, payload, digest)
	if err != nil {
		releaseMVCCReplayEntries(entries)
		return nil, err
	}
	catalogEntries, err := buildMVCCSegmentCatalogInstallEntries(segment, version, catalogValue, digest, uint64(len(payload)))
	if err != nil {
		releaseMVCCReplayEntries(entries)
		return nil, err
	}
	entries = append(entries, catalogEntries...)
	return entries, nil
}

func BuildMVCCSegmentCatalogInstallEntries(segment fsperas.PerasSegment, version uint64) ([]*entrykv.Entry, error) {
	payload, err := fsperas.EncodePerasSegment(segment)
	if err != nil {
		return nil, err
	}
	digest, err := fsperas.PerasSegmentPayloadDigest(payload)
	if err != nil {
		return nil, err
	}
	return BuildMVCCSegmentCatalogInstallEntriesWithPayload(segment, version, payload, digest)
}

func BuildMVCCSegmentCatalogInstallEntriesWithPayload(segment fsperas.PerasSegment, version uint64, payload []byte, digest [32]byte) ([]*entrykv.Entry, error) {
	if version == 0 || version == entrykv.MaxVersion {
		return nil, fsperas.ErrReplayVersionRequired
	}
	if _, err := fsperas.VerifyPerasSegmentPayload(payload, segment.Root, digest); err != nil {
		return nil, err
	}
	return buildMVCCSegmentCatalogInstallEntriesWithVerifiedPayload(segment, version, payload, digest)
}

func buildMVCCSegmentCatalogInstallEntriesWithVerifiedPayload(segment fsperas.PerasSegment, version uint64, payload []byte, digest [32]byte) ([]*entrykv.Entry, error) {
	if version == 0 || version == entrykv.MaxVersion {
		return nil, fsperas.ErrReplayVersionRequired
	}
	catalogValue, err := fsperas.EncodePerasSegmentCatalogRecordWithPayload(segment, version, payload, digest)
	if err != nil {
		return nil, err
	}
	return buildMVCCSegmentCatalogInstallEntries(segment, version, catalogValue, digest, uint64(len(payload)))
}

func BuildMVCCSegmentCatalogInstallEntriesWithPayloadForObjectKey(segment fsperas.PerasSegment, version uint64, payload []byte, digest [32]byte, objectKey []byte) ([]*entrykv.Entry, error) {
	if version == 0 || version == entrykv.MaxVersion {
		return nil, fsperas.ErrReplayVersionRequired
	}
	if _, err := fsperas.VerifyPerasSegmentPayload(payload, segment.Root, digest); err != nil {
		return nil, err
	}
	return buildMVCCSegmentCatalogInstallEntriesWithVerifiedPayloadForObjectKey(segment, version, payload, digest, objectKey)
}

func buildMVCCSegmentCatalogInstallEntriesWithVerifiedPayloadForObjectKey(segment fsperas.PerasSegment, version uint64, payload []byte, digest [32]byte, objectKey []byte) ([]*entrykv.Entry, error) {
	if version == 0 || version == entrykv.MaxVersion {
		return nil, fsperas.ErrReplayVersionRequired
	}
	routeBucket, canonicalBucket, err := perasSegmentObjectBuckets(segment, objectKey)
	if err != nil {
		return nil, err
	}
	canonicalObjectKey, err := fsmeta.EncodeSegmentObjectKey(canonicalBucket.mount, canonicalBucket.bucket, segment.Root)
	if err != nil {
		return nil, err
	}
	indexValue, err := fsperas.EncodePerasSegmentCatalogIndexRecordFields(segment.EpochID, version, segment.Root, digest, uint64(len(payload)), canonicalObjectKey)
	if err != nil {
		return nil, err
	}
	indexKey, err := fsmeta.EncodeSegmentCatalogIndexKey(routeBucket.mount, routeBucket.bucket, segment.Root)
	if err != nil {
		return nil, err
	}
	entries := []*entrykv.Entry{
		entrykv.NewInternalEntry(entrykv.CFDefault, indexKey, version, indexValue, 0, 0),
	}
	if bytes.Equal(objectKey, canonicalObjectKey) {
		catalogValue, err := fsperas.EncodePerasSegmentCatalogRecordWithPayload(segment, version, payload, digest)
		if err != nil {
			releaseMVCCReplayEntries(entries)
			return nil, err
		}
		entries = append(entries, entrykv.NewInternalEntry(entrykv.CFDefault, objectKey, version, catalogValue, 0, 0))
	}
	return entries, nil
}

func buildMVCCSegmentCatalogInstallEntriesWithVerifiedPayloadForObjectKeys(segment fsperas.PerasSegment, version uint64, payload []byte, digest [32]byte, objectKeys [][]byte) ([]*entrykv.Entry, error) {
	if len(objectKeys) == 0 {
		return nil, fsperas.ErrInvalidPerasSegment
	}
	entries := make([]*entrykv.Entry, 0, len(objectKeys)+1)
	for _, objectKey := range objectKeys {
		routeEntries, err := buildMVCCSegmentCatalogInstallEntriesWithVerifiedPayloadForObjectKey(segment, version, payload, digest, objectKey)
		if err != nil {
			releaseMVCCReplayEntries(entries)
			return nil, err
		}
		entries = append(entries, routeEntries...)
	}
	return dedupeInternalEntries(entries), nil
}

func buildMVCCSegmentCatalogIndexInstallEntries(root, digest [32]byte, epochID, version, payloadSize uint64, routingKey, canonicalObjectKey []byte) ([]*entrykv.Entry, error) {
	if epochID == 0 || version == 0 || version == entrykv.MaxVersion || payloadSize == 0 {
		return nil, fsperas.ErrInvalidPerasSegment
	}
	route, err := inspectPerasSegmentObjectKey(root, routingKey)
	if err != nil {
		return nil, err
	}
	if _, err := inspectPerasSegmentObjectKey(root, canonicalObjectKey); err != nil {
		return nil, err
	}
	indexValue, err := fsperas.EncodePerasSegmentCatalogIndexRecordFields(epochID, version, root, digest, payloadSize, canonicalObjectKey)
	if err != nil {
		return nil, err
	}
	indexKey, err := fsmeta.EncodeSegmentCatalogIndexKey(route.mount, route.bucket, root)
	if err != nil {
		return nil, err
	}
	return []*entrykv.Entry{entrykv.NewInternalEntry(entrykv.CFDefault, indexKey, version, indexValue, 0, 0)}, nil
}

func buildMVCCSegmentCatalogInstallEntries(segment fsperas.PerasSegment, version uint64, objectValue []byte, digest [32]byte, payloadSize uint64) ([]*entrykv.Entry, error) {
	objectKey, err := fsperas.PerasSegmentObjectKey(segment)
	if err != nil {
		return nil, err
	}
	indexValue, err := fsperas.EncodePerasSegmentCatalogIndexRecordFields(segment.EpochID, version, segment.Root, digest, payloadSize, objectKey)
	if err != nil {
		return nil, err
	}
	indexKeys, err := fsperas.PerasSegmentCatalogIndexKeys(segment)
	if err != nil {
		return nil, err
	}
	entries := make([]*entrykv.Entry, 0, len(indexKeys)+1)
	entries = append(entries, entrykv.NewInternalEntry(entrykv.CFDefault, objectKey, version, objectValue, 0, 0))
	for _, key := range indexKeys {
		entries = append(entries, entrykv.NewInternalEntry(entrykv.CFDefault, key, version, indexValue, 0, 0))
	}
	_ = payloadSize
	return entries, nil
}

func dedupeInternalEntries(entries []*entrykv.Entry) []*entrykv.Entry {
	if len(entries) < 2 {
		return entries
	}
	out := entries[:0]
	for _, entry := range entries {
		if entry == nil {
			continue
		}
		duplicate := false
		for _, kept := range out {
			if kept != nil && bytes.Equal(kept.Key, entry.Key) {
				duplicate = true
				break
			}
		}
		if duplicate {
			entry.DecrRef()
			continue
		}
		out = append(out, entry)
	}
	return out
}

type perasSegmentCatalogBucket struct {
	mount  model.MountKeyID
	bucket fsmeta.AffinityBucket
}

func inspectPerasSegmentObjectKey(root [32]byte, key []byte) (perasSegmentCatalogBucket, error) {
	parts, ok := fsmeta.InspectKey(key)
	if !ok || parts.Kind != fsmeta.KeyKindSegment || parts.SegmentRecord != fsmeta.SegmentRecordObject || parts.SegmentRoot != root {
		return perasSegmentCatalogBucket{}, fsperas.ErrInvalidPerasSegment
	}
	return perasSegmentCatalogBucket{mount: parts.MountKeyID, bucket: parts.Bucket}, nil
}

func perasSegmentObjectBuckets(segment fsperas.PerasSegment, objectKey []byte) (perasSegmentCatalogBucket, perasSegmentCatalogBucket, error) {
	route, err := inspectPerasSegmentObjectKey(segment.Root, objectKey)
	if err != nil {
		return perasSegmentCatalogBucket{}, perasSegmentCatalogBucket{}, err
	}
	var canonical perasSegmentCatalogBucket
	seenRoute := false
	err = segment.ForEachEntry(func(entry fsperas.SegmentKV) error {
		entryParts, ok := fsmeta.InspectKey(entry.Key)
		if !ok {
			return fsperas.ErrInvalidPerasSegment
		}
		key := perasSegmentCatalogBucket{mount: entryParts.MountKeyID, bucket: entryParts.Bucket}
		if canonical.mount == 0 || key.mount < canonical.mount || (key.mount == canonical.mount && key.bucket < canonical.bucket) {
			canonical = key
		}
		if key == route {
			seenRoute = true
		}
		return nil
	})
	if err != nil {
		return perasSegmentCatalogBucket{}, perasSegmentCatalogBucket{}, err
	}
	if !seenRoute || canonical.mount == 0 {
		return perasSegmentCatalogBucket{}, perasSegmentCatalogBucket{}, fsperas.ErrInvalidPerasSegment
	}
	return route, canonical, nil
}

func perasSegmentObjectBucket(segment fsperas.PerasSegment, objectKey []byte) (perasSegmentCatalogBucket, error) {
	route, _, err := perasSegmentObjectBuckets(segment, objectKey)
	return route, err
}

func appendMutationMVCCReplayEntries(entries []*entrykv.Entry, mutation fsperas.ReplayMutation, version uint64) ([]*entrykv.Entry, error) {
	if len(mutation.Key) == 0 {
		return nil, fsperas.ErrInvalidPerasSegment
	}
	if mutation.Delete {
		if mutation.Value != nil {
			return nil, fsperas.ErrInvalidPerasSegment
		}
		write := txnmvcc.EncodeWrite(txnmvcc.Write{Kind: kvrpcpb.Mutation_Delete, StartTs: version})
		entries = append(entries,
			entrykv.NewInternalEntry(entrykv.CFDefault, mutation.Key, version, nil, entrykv.BitDelete, 0),
			entrykv.NewInternalEntry(entrykv.CFWrite, mutation.Key, version, write, 0, 0),
		)
		return entries, nil
	}
	if mutation.Value == nil {
		return nil, fsperas.ErrInvalidPerasSegment
	}
	write := txnmvcc.Write{Kind: kvrpcpb.Mutation_Put, StartTs: version}
	if txnmvcc.CanInlineShortValue(kvrpcpb.Mutation_Put, mutation.Value) {
		write.ShortValue = cloneBytes(mutation.Value)
		entries = append(entries, entrykv.NewInternalEntry(entrykv.CFWrite, mutation.Key, version, txnmvcc.EncodeWrite(write), 0, 0))
		return entries, nil
	}
	entries = append(entries,
		entrykv.NewInternalEntry(entrykv.CFDefault, mutation.Key, version, nil, entrykv.BitDelete, 0),
		entrykv.NewInternalEntry(entrykv.CFDefault, mutation.Key, version, cloneBytes(mutation.Value), 0, 0),
		entrykv.NewInternalEntry(entrykv.CFWrite, mutation.Key, version, txnmvcc.EncodeWrite(write), 0, 0),
	)
	return entries, nil
}

func releaseMVCCReplayEntries(entries []*entrykv.Entry) {
	for _, entry := range entries {
		if entry != nil {
			entry.DecrRef()
		}
	}
}

func cloneBytes(in []byte) []byte {
	if in == nil {
		return nil
	}
	return append([]byte(nil), in...)
}
