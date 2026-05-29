// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"fmt"

	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/kv"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"github.com/feichai0017/NoKV/raftstore/peer"
	"github.com/feichai0017/NoKV/raftstore/snapshot"
	"github.com/feichai0017/NoKV/raftstore/store"
	"github.com/feichai0017/NoKV/raftstore/transport"
)

func defaultPeerBuilder(storage Storage, localMeta *localmeta.Store, storeID uint64, baseRaft myraft.Config, tr transport.Transport) store.PeerBuilder {
	return func(meta localmeta.RegionMeta) (*peer.Config, error) {
		var peerID uint64
		for _, p := range meta.Peers {
			if p.StoreID == storeID {
				peerID = p.PeerID
				break
			}
		}
		if peerID == 0 {
			return nil, fmt.Errorf("raftstore/server: store %d missing peer in region %d", storeID, meta.ID)
		}
		peerStorage, err := storage.Raft.Open(meta.ID, localMeta)
		if err != nil {
			return nil, fmt.Errorf("raftstore/server: open peer storage for region %d: %w", meta.ID, err)
		}
		snapshotStore := snapshot.NewStore(storage.MVCC)
		snapshotApply := func(payload []byte) (localmeta.RegionMeta, error) {
			result, err := snapshotStore.ImportSnapshot(payload)
			if err != nil {
				return localmeta.RegionMeta{}, err
			}
			return result.Descriptor.Region, nil
		}
		return &peer.Config{
			RaftConfig:     defaultRaftConfig(baseRaft, peerID),
			Transport:      tr,
			Apply:          kv.NewEntryApplier(storage.MVCC),
			SnapshotExport: snapshotStore.ExportSnapshot,
			SnapshotApply:  snapshotApply,
			Storage:        peerStorage,
			GroupID:        meta.ID,
			Region:         localmeta.CloneRegionMetaPtr(&meta),
			FastLeaseRead:  true,
		}, nil
	}
}

func defaultRaftConfig(base myraft.Config, peerID uint64) myraft.Config {
	base.ID = peerID
	if base.ElectionTick == 0 {
		base.ElectionTick = 10
	}
	if base.HeartbeatTick == 0 {
		base.HeartbeatTick = 2
	}
	if base.MaxSizePerMsg == 0 {
		base.MaxSizePerMsg = 1 << 20
	}
	if base.MaxInflightMsgs == 0 {
		base.MaxInflightMsgs = 256
	}
	return peer.EnableLeaseRead(base)
}
