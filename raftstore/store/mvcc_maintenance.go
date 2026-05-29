// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"fmt"

	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	entrykv "github.com/feichai0017/NoKV/txn/storage"
)

// ProposeMVCCMaintenance submits prebuilt MVCC tombstones through Raft. This
// command is only for internal GC cleanup; user-visible transaction resolution
// must use the semantic Percolator commands.
//
// The operation is atomic per region batch, not across regions. If one region
// succeeds and a later region fails, the caller sees the error and a lower
// applied count; the next GC pass must rescan and converge via idempotent
// tombstones.
func (s *Store) ProposeMVCCMaintenance(ctx context.Context, entries []*entrykv.Entry) (uint64, uint64, uint64, error) {
	if len(entries) == 0 {
		return 0, 0, 0, nil
	}
	batches := make([]maintenanceRegionBatch, 0, 1)
	regionIndex := make(map[uint64]int)
	for _, entry := range entries {
		if entry == nil || len(entry.Key) == 0 {
			return 0, 0, 0, fmt.Errorf("raftstore: empty MVCC maintenance entry")
		}
		cf, userKey, version, ok := entrykv.SplitInternalKey(entry.Key)
		if !ok || len(userKey) == 0 {
			return 0, 0, 0, fmt.Errorf("raftstore: invalid MVCC maintenance key %x", entry.Key)
		}
		meta, ok := s.RegionMetaByKey(userKey)
		if !ok {
			return 0, 0, 0, fmt.Errorf("raftstore: no region for MVCC maintenance key %x", userKey)
		}
		idx, ok := regionIndex[meta.ID]
		if !ok {
			idx = len(batches)
			regionIndex[meta.ID] = idx
			batches = append(batches, maintenanceRegionBatch{meta: meta})
		}
		tombstone, err := newInternalEntryTombstone(cf, userKey, version, entry)
		if err != nil {
			return 0, 0, 0, err
		}
		batches[idx].entries = append(batches[idx].entries, tombstone)
		switch cf {
		case entrykv.CFWrite:
			batches[idx].writeDeletes++
		case entrykv.CFDefault:
			batches[idx].defaultDeletes++
		}
	}

	var applied, writeDeletes, defaultDeletes uint64
	for _, batch := range batches {
		count, err := s.proposeMVCCMaintenanceBatch(ctx, batch)
		if err != nil {
			return applied, writeDeletes, defaultDeletes, err
		}
		applied += count
		writeDeletes += batch.writeDeletes
		defaultDeletes += batch.defaultDeletes
	}
	return applied, writeDeletes, defaultDeletes, nil
}

type maintenanceRegionBatch struct {
	meta           localmeta.RegionMeta
	entries        []*kvrpcpb.InternalEntryTombstone
	writeDeletes   uint64
	defaultDeletes uint64
}

func newInternalEntryTombstone(cf entrykv.ColumnFamily, userKey []byte, version uint64, entry *entrykv.Entry) (*kvrpcpb.InternalEntryTombstone, error) {
	pbCF, ok := maintenanceColumnFamilyProto(cf)
	if !ok {
		return nil, fmt.Errorf("raftstore: invalid MVCC maintenance column family %v", cf)
	}
	if entry.Meta&entrykv.BitDelete == 0 {
		return nil, fmt.Errorf("raftstore: MVCC maintenance entry is not a tombstone")
	}
	if len(entry.Value) > 0 || entry.ExpiresAt != 0 {
		return nil, fmt.Errorf("raftstore: MVCC maintenance tombstone carries payload")
	}
	return &kvrpcpb.InternalEntryTombstone{
		ColumnFamily: pbCF,
		Key:          entrykv.SafeCopy(nil, userKey),
		Version:      version,
	}, nil
}

func maintenanceColumnFamilyProto(cf entrykv.ColumnFamily) (kvrpcpb.InternalEntryTombstone_ColumnFamily, bool) {
	switch cf {
	case entrykv.CFDefault:
		return kvrpcpb.InternalEntryTombstone_DEFAULT, true
	case entrykv.CFWrite:
		return kvrpcpb.InternalEntryTombstone_WRITE, true
	default:
		return 0, false
	}
}

func (s *Store) proposeMVCCMaintenanceBatch(ctx context.Context, batch maintenanceRegionBatch) (uint64, error) {
	resp, err := s.ProposeCommand(ctx, &raftcmdpb.RaftCmdRequest{
		Header: &raftcmdpb.CmdHeader{
			RegionId: batch.meta.ID,
			RegionEpoch: &metapb.RegionEpoch{
				Version:     batch.meta.Epoch.Version,
				ConfVersion: batch.meta.Epoch.ConfVersion,
			},
		},
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_MVCC_MAINTENANCE,
			Cmd: &raftcmdpb.Request_MvccMaintenance{MvccMaintenance: &kvrpcpb.MVCCMaintenanceRequest{
				Tombstones: batch.entries,
			}},
		}},
	})
	if err != nil {
		return 0, err
	}
	if regionErr := resp.GetRegionError(); regionErr != nil {
		return 0, fmt.Errorf("raftstore: MVCC maintenance region %d failed: %v", batch.meta.ID, regionErr)
	}
	responses := resp.GetResponses()
	if len(responses) != 1 || responses[0].GetMvccMaintenance() == nil {
		return 0, fmt.Errorf("raftstore: MVCC maintenance region %d returned invalid response", batch.meta.ID)
	}
	out := responses[0].GetMvccMaintenance()
	if keyErr := out.GetError(); keyErr != nil {
		return 0, errRegionKeyError("MVCC maintenance", batch.meta.ID, keyErr)
	}
	return out.GetAppliedEntries(), nil
}
