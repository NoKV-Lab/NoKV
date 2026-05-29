// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package store

import (
	metaregion "github.com/feichai0017/NoKV/meta/region"
	"testing"

	entrykv "github.com/feichai0017/NoKV/engine/kv"
	local "github.com/feichai0017/NoKV/local"
	myraft "github.com/feichai0017/NoKV/raft"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"github.com/feichai0017/NoKV/raftstore/peer"
	snapshotpkg "github.com/feichai0017/NoKV/raftstore/snapshot/sst"
	"github.com/feichai0017/NoKV/raftstore/store/router"
	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

func testSSTApply(t *testing.T, db *local.DB) peer.SnapshotApplyFunc {
	t.Helper()
	return func(payload []byte) (localmeta.RegionMeta, error) {
		result, err := snapshotpkg.NewDBStore(db).ImportSnapshot(payload)
		if err != nil {
			return localmeta.RegionMeta{}, err
		}
		return result.Meta.Region, nil
	}
}

func TestStoreStepBootstrapsPeerFromSnapshotPayload(t *testing.T) {
	sourceDB, _ := openStoreDB(t)
	value := entrykv.NewInternalEntry(entrykv.CFDefault, []byte("apple"), 5, []byte("value"), 0, 0)
	t.Cleanup(func() { value.DecrRef() })
	require.NoError(t, sourceDB.ApplyInternalEntries([]*entrykv.Entry{value}))

	region := localmeta.RegionMeta{
		ID:       77,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 2},
		Peers: []metaregion.Peer{
			{StoreID: 1, PeerID: 11},
			{StoreID: 2, PeerID: 22},
		},
		State: metaregion.ReplicaStateRunning,
	}
	payload, err := snapshotpkg.NewDBStore(sourceDB).ExportSnapshot(region)
	require.NoError(t, err)

	targetDB, targetMeta := openStoreDB(t)
	builder := func(meta localmeta.RegionMeta) (*peer.Config, error) {
		return &peer.Config{
			RaftConfig: myraft.Config{
				ID:              22,
				ElectionTick:    5,
				HeartbeatTick:   1,
				MaxSizePerMsg:   1 << 20,
				MaxInflightMsgs: 256,
				PreVote:         true,
			},
			Transport:     noopTransport{},
			Apply:         func([]myraft.Entry) error { return nil },
			SnapshotApply: testSSTApply(t, targetDB),
			Storage:       mustPeerStorage(t, targetDB, targetMeta, meta.ID),
			GroupID:       meta.ID,
			Region:        localmeta.CloneRegionMetaPtr(&meta),
		}, nil
	}
	st := NewStore(Config{
		StoreID:     2,
		LocalMeta:   targetMeta,
		PeerBuilder: builder,
	})
	t.Cleanup(st.Close)

	msg := myraft.Message{
		Type: myraft.MsgSnapshot,
		From: 11,
		To:   22,
		Snapshot: &raftpb.Snapshot{
			Data: payload,
			Metadata: raftpb.SnapshotMetadata{
				Index: 5,
				Term:  2,
				ConfState: raftpb.ConfState{
					Voters: []uint64{11, 22},
				},
			},
		},
	}
	require.NoError(t, st.Step(msg))

	p, ok := st.Peer(22)
	require.True(t, ok)
	require.NotNil(t, p)
	meta, ok := st.RegionMetaByID(region.ID)
	require.True(t, ok)
	require.Equal(t, uint64(22), meta.Peers[1].PeerID)

	got, err := targetDB.GetInternalEntry(entrykv.CFDefault, []byte("apple"), 5)
	require.NoError(t, err)
	require.NotNil(t, got)
	defer got.DecrRef()
	require.Equal(t, []byte("value"), got.Value)
}

func TestStoreInstallRegionSnapshotBootstrapsPeer(t *testing.T) {
	sourceDB, _ := openStoreDB(t)
	value := entrykv.NewInternalEntry(entrykv.CFDefault, []byte("banana"), 7, []byte("payload"), 0, 0)
	t.Cleanup(func() { value.DecrRef() })
	require.NoError(t, sourceDB.ApplyInternalEntries([]*entrykv.Entry{value}))

	region := localmeta.RegionMeta{
		ID:       78,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 2},
		Peers: []metaregion.Peer{
			{StoreID: 1, PeerID: 11},
			{StoreID: 2, PeerID: 22},
		},
		State: metaregion.ReplicaStateRunning,
	}
	payload, err := snapshotpkg.NewDBStore(sourceDB).ExportSnapshot(region)
	require.NoError(t, err)

	targetDB, targetMeta := openStoreDB(t)
	builder := func(meta localmeta.RegionMeta) (*peer.Config, error) {
		return &peer.Config{
			RaftConfig: myraft.Config{
				ID:              22,
				ElectionTick:    5,
				HeartbeatTick:   1,
				MaxSizePerMsg:   1 << 20,
				MaxInflightMsgs: 256,
				PreVote:         true,
			},
			Transport:     noopTransport{},
			Apply:         func([]myraft.Entry) error { return nil },
			SnapshotApply: testSSTApply(t, targetDB),
			Storage:       mustPeerStorage(t, targetDB, targetMeta, meta.ID),
			GroupID:       meta.ID,
			Region:        localmeta.CloneRegionMetaPtr(&meta),
		}, nil
	}
	st := NewStore(Config{
		StoreID:     2,
		LocalMeta:   targetMeta,
		PeerBuilder: builder,
	})
	t.Cleanup(st.Close)

	installed, err := st.InstallRegionSnapshot(myraft.Snapshot{
		Data: payload,
		Metadata: raftpb.SnapshotMetadata{
			Index: 5,
			Term:  2,
			ConfState: raftpb.ConfState{
				Voters: []uint64{11, 22},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, region.ID, installed.ID)

	status, ok := st.RegionRuntimeStatus(region.ID)
	require.True(t, ok)
	require.True(t, status.Hosted)
	require.Equal(t, uint64(22), status.LocalPeerID)
	require.Equal(t, uint64(5), status.AppliedIndex)

	got, err := targetDB.GetInternalEntry(entrykv.CFDefault, []byte("banana"), 7)
	require.NoError(t, err)
	require.NotNil(t, got)
	defer got.DecrRef()
	require.Equal(t, []byte("payload"), got.Value)
}

func TestStoreInstallRegionSnapshotRejectsCorruptPayloadWithoutHostingPeer(t *testing.T) {
	targetDB, targetMeta := openStoreDB(t)
	builder := func(meta localmeta.RegionMeta) (*peer.Config, error) {
		return &peer.Config{
			RaftConfig: myraft.Config{
				ID:              22,
				ElectionTick:    5,
				HeartbeatTick:   1,
				MaxSizePerMsg:   1 << 20,
				MaxInflightMsgs: 256,
				PreVote:         true,
			},
			Transport:     noopTransport{},
			Apply:         func([]myraft.Entry) error { return nil },
			SnapshotApply: testSSTApply(t, targetDB),
			Storage:       mustPeerStorage(t, targetDB, targetMeta, meta.ID),
			GroupID:       meta.ID,
			Region:        localmeta.CloneRegionMetaPtr(&meta),
		}, nil
	}
	st := NewStore(Config{StoreID: 2, LocalMeta: targetMeta, PeerBuilder: builder})
	t.Cleanup(st.Close)

	_, err := st.InstallRegionSnapshot(myraft.Snapshot{
		Data: []byte("broken-payload"),
		Metadata: raftpb.SnapshotMetadata{
			Index: 5,
			Term:  2,
			ConfState: raftpb.ConfState{
				Voters: []uint64{11, 22},
			},
		},
	})
	require.Error(t, err)
	_, ok := st.RegionRuntimeStatus(0)
	require.False(t, ok)
	require.Empty(t, st.RegionMetas())
	_, hosted := st.Peer(22)
	require.False(t, hosted)
}

func TestStorePeerLifecycle(t *testing.T) {
	rt := router.New()
	rs := NewStore(Config{Router: rt})

	cfg := &peer.Config{
		RaftConfig: myraft.Config{
			ID:              1,
			ElectionTick:    10,
			HeartbeatTick:   2,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
		},
		Transport: noopTransport{},
		Apply:     func([]myraft.Entry) error { return nil },
		Region: &localmeta.RegionMeta{
			ID:       100,
			StartKey: []byte("a"),
			EndKey:   []byte("b"),
		},
	}

	peer, err := rs.StartPeer(cfg, []myraft.Peer{{ID: 1}})
	require.NoError(t, err)
	require.NotNil(t, peer)

	metas := rs.RegionMetas()
	require.Len(t, metas, 1)
	require.Equal(t, uint64(100), metas[0].ID)

	require.NoError(t, rt.SendTick(peer.ID()))
	require.NoError(t, rt.BroadcastTick())
	require.NoError(t, rt.BroadcastFlush())

	rs.StopPeer(peer.ID())
	_, ok := rt.Peer(peer.ID())
	require.False(t, ok)
}

func TestStoreDuplicatePeer(t *testing.T) {
	rs := NewStore(Config{})
	cfg := &peer.Config{
		RaftConfig: myraft.Config{
			ID:              1,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
		},
		Transport: noopTransport{},
		Apply:     func([]myraft.Entry) error { return nil },
	}

	peer, err := rs.StartPeer(cfg, nil)
	require.NoError(t, err)
	require.NotNil(t, peer)

	defer rs.StopPeer(peer.ID())

	_, err = rs.StartPeer(cfg, nil)
	require.Error(t, err)
}
