// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"bytes"

	fsperas "github.com/feichai0017/NoKV/experimental/peras/exec"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	entrykv "github.com/feichai0017/NoKV/txn/storage"
)

const segmentInstallDiagnosticLabel = "peras_segment_install"

type SegmentPreparedInstallRequest struct {
	RoutingKey         []byte
	RoutingKeys        [][]byte
	DependencyKeys     [][]byte
	MaterializedKeys   [][]byte
	CanonicalObjectKey []byte
	Segment            fsperas.PerasSegment
	Payload            []byte
	PayloadDigest      [32]byte
	InstallVersion     uint64
	MaterializeMVCC    bool
}

func BuildSegmentPreparedInstallRequest(req SegmentPreparedInstallRequest) (*kvrpcpb.InstallPreparedMVCCEntriesRequest, error) {
	if len(req.RoutingKey) == 0 || req.InstallVersion == 0 || req.InstallVersion == entrykv.MaxVersion {
		return nil, ErrInvalidInstallRequest
	}
	if _, err := fsperas.VerifyPerasSegmentPayload(req.Payload, req.Segment.Root, req.PayloadDigest); err != nil {
		return nil, err
	}
	var entries []*entrykv.Entry
	var err error
	if req.MaterializeMVCC {
		entries, err = buildMVCCSegmentInstallEntriesWithVerifiedPayload(req.Segment, req.InstallVersion, req.Payload, req.PayloadDigest)
	} else {
		entries, err = buildMVCCSegmentCatalogInstallEntriesWithVerifiedPayloadForObjectKeys(
			req.Segment,
			req.InstallVersion,
			req.Payload,
			req.PayloadDigest,
			req.RoutingKeys,
		)
	}
	if err != nil {
		return nil, err
	}
	defer releaseMVCCReplayEntries(entries)
	return buildPreparedMVCCInstallRequest(
		req.RoutingKey,
		req.InstallVersion,
		entries,
		req.DependencyKeys,
		segmentInstallIdempotencyKey(req.Segment.Root, req.RoutingKey),
		segmentInstallDiagnosticLabel,
		segmentInstallWatchKeys(req),
	)
}

func buildPreparedMVCCInstallRequest(
	routingKey []byte,
	commitVersion uint64,
	entries []*entrykv.Entry,
	dependencyKeys [][]byte,
	idempotencyKey []byte,
	diagnosticLabel string,
	watchKeys [][]byte,
) (*kvrpcpb.InstallPreparedMVCCEntriesRequest, error) {
	req := &kvrpcpb.InstallPreparedMVCCEntriesRequest{
		RoutingKey:      cloneInstallBytes(routingKey),
		CommitVersion:   commitVersion,
		Entries:         make([]*kvrpcpb.PreparedMVCCEntry, 0, len(entries)),
		DependencyKeys:  cloneInstallKeySet(dependencyKeys),
		IdempotencyKey:  cloneInstallBytes(idempotencyKey),
		DiagnosticLabel: diagnosticLabel,
		WatchKeys:       cloneInstallKeySet(watchKeys),
	}
	for _, entry := range entries {
		prepared, err := preparedMVCCEntryFromInternal(commitVersion, entry)
		if err != nil {
			return nil, err
		}
		req.Entries = append(req.Entries, prepared)
	}
	return req, nil
}

func preparedMVCCEntryFromInternal(commitVersion uint64, entry *entrykv.Entry) (*kvrpcpb.PreparedMVCCEntry, error) {
	if entry == nil {
		return nil, ErrInvalidInstallRequest
	}
	cf, userKey, version, ok := entrykv.SplitInternalKey(entry.Key)
	if !ok || version != commitVersion {
		return nil, ErrInvalidInstallRequest
	}
	outCF, ok := preparedMVCCColumnFamilyFromInternal(cf)
	if !ok {
		return nil, ErrInvalidInstallRequest
	}
	var value []byte
	if entry.Value != nil {
		value = cloneInstallBytes(entry.Value)
	}
	return &kvrpcpb.PreparedMVCCEntry{
		ColumnFamily: outCF,
		Key:          cloneInstallBytes(userKey),
		Version:      version,
		Value:        value,
		Meta:         uint32(entry.Meta),
		ExpiresAt:    entry.ExpiresAt,
		HasValue:     entry.Value != nil,
	}, nil
}

func preparedMVCCColumnFamilyFromInternal(cf entrykv.ColumnFamily) (kvrpcpb.PreparedMVCCEntry_ColumnFamily, bool) {
	switch cf {
	case entrykv.CFDefault:
		return kvrpcpb.PreparedMVCCEntry_DEFAULT, true
	case entrykv.CFLock:
		return kvrpcpb.PreparedMVCCEntry_LOCK, true
	case entrykv.CFWrite:
		return kvrpcpb.PreparedMVCCEntry_WRITE, true
	default:
		return kvrpcpb.PreparedMVCCEntry_DEFAULT, false
	}
}

func segmentInstallIdempotencyKey(root [32]byte, routingKey []byte) []byte {
	key := make([]byte, 0, len(root)+len(routingKey))
	key = append(key, root[:]...)
	key = append(key, routingKey...)
	return key
}

func segmentInstallWatchKeys(req SegmentPreparedInstallRequest) [][]byte {
	if req.MaterializeMVCC && len(req.MaterializedKeys) > 0 {
		return dentryKeysFromHeader(req.MaterializedKeys)
	}
	if !req.MaterializeMVCC && len(req.CanonicalObjectKey) > 0 && !installKeysContainAll(req.RoutingKeys, [][]byte{req.CanonicalObjectKey}) {
		return nil
	}
	dentries := req.Segment.Dentries
	out := make([][]byte, 0, len(dentries))
	for _, entry := range dentries {
		out = append(out, cloneInstallBytes(entry.Key))
	}
	return out
}

func cloneInstallBytes(in []byte) []byte {
	if in == nil {
		return nil
	}
	return append([]byte(nil), in...)
}

func cloneInstallKeySet(keys [][]byte) [][]byte {
	if len(keys) == 0 {
		return nil
	}
	out := make([][]byte, 0, len(keys))
	for _, key := range keys {
		out = append(out, cloneInstallBytes(key))
	}
	return out
}

func installKeysContainAll(have, want [][]byte) bool {
	if len(want) == 0 {
		return true
	}
	for _, wanted := range want {
		if len(wanted) == 0 {
			return false
		}
		found := false
		for _, key := range have {
			if bytes.Equal(key, wanted) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func dentryKeysFromHeader(keys [][]byte) [][]byte {
	if len(keys) == 0 {
		return nil
	}
	out := make([][]byte, 0, len(keys))
	for _, key := range keys {
		parts, ok := layout.InspectKey(key)
		if !ok || parts.Kind != layout.KeyKindDentry {
			continue
		}
		out = append(out, cloneInstallBytes(key))
	}
	return out
}
