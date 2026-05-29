// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	local "github.com/feichai0017/NoKV/local"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	errorpb "github.com/feichai0017/NoKV/pb/error"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"

	"github.com/feichai0017/NoKV/coordinator/storecontrol"
	myraft "github.com/feichai0017/NoKV/raft"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"github.com/feichai0017/NoKV/raftstore/peer"
	txnmvcc "github.com/feichai0017/NoKV/txn/mvcc"
	"github.com/feichai0017/NoKV/txn/percolator"
	txnstore "github.com/feichai0017/NoKV/txn/storage"
	"github.com/stretchr/testify/require"
)

func TestStoreProposeCommandPrewriteCommit(t *testing.T) {
	db, localMeta := openStoreDB(t)
	coord := newTestSchedulerSink()
	applier := newTestMVCCApplier(db)
	st := NewStore(Config{Scheduler: coord, StoreID: 1, CommandApplier: applier})
	t.Cleanup(func() { st.Close() })

	region := &localmeta.RegionMeta{
		ID:       101,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 1}},
	}
	cfg := &peer.Config{
		RaftConfig: myraft.Config{
			ID:              1,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
			PreVote:         true,
		},
		Transport: noopTransport{},
		Storage:   mustPeerStorage(t, db, localMeta, 101),
		GroupID:   101,
		Region:    region,
	}
	peer, err := st.StartPeer(cfg, []myraft.Peer{{ID: 1}})
	require.NoError(t, err)
	t.Cleanup(func() { st.StopPeer(peer.ID()) })
	require.NoError(t, peer.Campaign())

	epoch := &metapb.RegionEpoch{Version: 1, ConfVersion: 1}
	prewrite := &raftcmdpb.RaftCmdRequest{
		Header: &raftcmdpb.CmdHeader{RegionId: region.ID, RegionEpoch: epoch},
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_PREWRITE,
			Cmd: &raftcmdpb.Request_Prewrite{Prewrite: &kvrpcpb.PrewriteRequest{
				Mutations: []*kvrpcpb.Mutation{{
					Op:    kvrpcpb.Mutation_Put,
					Key:   []byte("cmd-key"),
					Value: []byte("cmd-value"),
				}},
				PrimaryLock:  []byte("cmd-key"),
				StartVersion: 20,
				LockTtl:      3000,
			}},
		}},
	}
	resp, err := st.ProposeCommand(context.Background(), prewrite)
	require.NoError(t, err)
	require.Nil(t, resp.GetRegionError())
	require.Len(t, resp.GetResponses(), 1)
	require.Empty(t, resp.GetResponses()[0].GetPrewrite().GetErrors())
	require.NotZero(t, resp.GetHeader().GetRequestId())

	commit := &raftcmdpb.RaftCmdRequest{
		Header: &raftcmdpb.CmdHeader{RegionId: region.ID, RegionEpoch: epoch},
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_COMMIT,
			Cmd: &raftcmdpb.Request_Commit{Commit: &kvrpcpb.CommitRequest{
				Keys:          [][]byte{[]byte("cmd-key")},
				StartVersion:  20,
				CommitVersion: 40,
			}},
		}},
	}
	resp, err = st.ProposeCommand(context.Background(), commit)
	require.NoError(t, err)
	require.Nil(t, resp.GetRegionError())
	require.Len(t, resp.GetResponses(), 1)
	require.Nil(t, resp.GetResponses()[0].GetCommit().GetError())

	reader := percolator.NewReader(db)
	val, _, err := reader.GetValue([]byte("cmd-key"), 50)
	require.NoError(t, err)
	require.Equal(t, []byte("cmd-value"), val)
}

func TestStoreProposeMVCCMaintenance(t *testing.T) {
	db, localMeta := openStoreDB(t)
	coord := newTestSchedulerSink()
	st := NewStore(Config{Scheduler: coord, StoreID: 1, CommandApplier: newTestMVCCApplier(db)})
	t.Cleanup(func() { st.Close() })

	region := &localmeta.RegionMeta{
		ID:       111,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 1}},
	}
	cfg := &peer.Config{
		RaftConfig: myraft.Config{
			ID:              1,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
			PreVote:         true,
		},
		Transport: noopTransport{},
		Storage:   mustPeerStorage(t, db, localMeta, region.ID),
		GroupID:   region.ID,
		Region:    region,
	}
	p, err := st.StartPeer(cfg, []myraft.Peer{{ID: 1}})
	require.NoError(t, err)
	t.Cleanup(func() { st.StopPeer(p.ID()) })
	require.NoError(t, p.Campaign())

	entry := txnstore.NewInternalEntry(txnstore.CFWrite, []byte("gc-key"), 33, nil, txnstore.BitDelete, 0)
	defer entry.DecrRef()
	applied, writes, defaults, err := st.ProposeMVCCMaintenance(context.Background(), []*txnstore.Entry{entry})
	require.NoError(t, err)
	require.Equal(t, uint64(1), applied)
	require.Equal(t, uint64(1), writes)
	require.Zero(t, defaults)

	got, err := db.GetInternalEntry(txnstore.CFWrite, []byte("gc-key"), 33)
	require.NoError(t, err)
	defer got.DecrRef()
	require.NotZero(t, got.Meta&txnstore.BitDelete)
}

func TestStoreProposeMVCCMaintenanceFailsClosedWhenNotLeader(t *testing.T) {
	db, localMeta := openStoreDB(t)
	st := NewStore(Config{Scheduler: newTestSchedulerSink(), StoreID: 1, CommandApplier: newTestMVCCApplier(db)})
	t.Cleanup(func() { st.Close() })

	region := &localmeta.RegionMeta{
		ID:       119,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 19}},
	}
	cfg := &peer.Config{
		RaftConfig: myraft.Config{
			ID:              19,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
			PreVote:         true,
		},
		Transport: noopTransport{},
		Storage:   mustPeerStorage(t, db, localMeta, region.ID),
		GroupID:   region.ID,
		Region:    region,
	}
	p, err := st.StartPeer(cfg, []myraft.Peer{{ID: 19}})
	require.NoError(t, err)
	t.Cleanup(func() { st.StopPeer(p.ID()) })

	entry := txnstore.NewInternalEntry(txnstore.CFWrite, []byte("gc-not-leader-key"), 33, nil, txnstore.BitDelete, 0)
	defer entry.DecrRef()
	applied, writes, defaults, err := st.ProposeMVCCMaintenance(context.Background(), []*txnstore.Entry{entry})
	require.ErrorContains(t, err, "region 119")
	require.Zero(t, applied)
	require.Zero(t, writes)
	require.Zero(t, defaults)
	_, err = db.GetInternalEntry(txnstore.CFWrite, []byte("gc-not-leader-key"), 33)
	require.Error(t, err)
}

func TestStoreProposeMVCCMaintenanceConvergesAfterPartialRegionFailure(t *testing.T) {
	db, localMeta := openStoreDB(t)
	coord := newTestSchedulerSink()
	st := NewStore(Config{Scheduler: coord, StoreID: 1, CommandApplier: newTestMVCCApplier(db)})
	t.Cleanup(func() { st.Close() })

	left := &localmeta.RegionMeta{
		ID:       121,
		StartKey: []byte("a"),
		EndKey:   []byte("m"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 21}},
	}
	leftCfg := &peer.Config{
		RaftConfig: myraft.Config{
			ID:              21,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
			PreVote:         true,
		},
		Transport: noopTransport{},
		Storage:   mustPeerStorage(t, db, localMeta, left.ID),
		GroupID:   left.ID,
		Region:    left,
	}
	leftPeer, err := st.StartPeer(leftCfg, []myraft.Peer{{ID: 21}})
	require.NoError(t, err)
	t.Cleanup(func() { st.StopPeer(leftPeer.ID()) })
	require.NoError(t, leftPeer.Campaign())

	right := localmeta.RegionMeta{
		ID:       122,
		StartKey: []byte("m"),
		EndKey:   []byte("z"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 22}},
	}
	require.NoError(t, st.applyRegionMetaSilent(right))

	leftEntry := txnstore.NewInternalEntry(txnstore.CFWrite, []byte("b-gc-key"), 33, nil, txnstore.BitDelete, 0)
	defer leftEntry.DecrRef()
	rightEntry := txnstore.NewInternalEntry(txnstore.CFWrite, []byte("t-gc-key"), 44, nil, txnstore.BitDelete, 0)
	defer rightEntry.DecrRef()
	entries := []*txnstore.Entry{leftEntry, rightEntry}

	applied, writes, defaults, err := st.ProposeMVCCMaintenance(context.Background(), entries)
	require.ErrorContains(t, err, "region 122")
	require.Equal(t, uint64(1), applied)
	require.Equal(t, uint64(1), writes)
	require.Zero(t, defaults)

	gotLeft, err := db.GetInternalEntry(txnstore.CFWrite, []byte("b-gc-key"), 33)
	require.NoError(t, err)
	defer gotLeft.DecrRef()
	require.NotZero(t, gotLeft.Meta&txnstore.BitDelete)
	_, err = db.GetInternalEntry(txnstore.CFWrite, []byte("t-gc-key"), 44)
	require.Error(t, err)

	rightCfg := &peer.Config{
		RaftConfig: myraft.Config{
			ID:              22,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
			PreVote:         true,
		},
		Transport: noopTransport{},
		Storage:   mustPeerStorage(t, db, localMeta, right.ID),
		GroupID:   right.ID,
		Region:    &right,
	}
	rightPeer, err := st.StartPeer(rightCfg, []myraft.Peer{{ID: 22}})
	require.NoError(t, err)
	t.Cleanup(func() { st.StopPeer(rightPeer.ID()) })
	require.NoError(t, rightPeer.Campaign())

	applied, writes, defaults, err = st.ProposeMVCCMaintenance(context.Background(), entries)
	require.NoError(t, err)
	require.Equal(t, uint64(2), applied)
	require.Equal(t, uint64(2), writes)
	require.Zero(t, defaults)
	gotRight, err := db.GetInternalEntry(txnstore.CFWrite, []byte("t-gc-key"), 44)
	require.NoError(t, err)
	defer gotRight.DecrRef()
	require.NotZero(t, gotRight.Meta&txnstore.BitDelete)
}

func TestStoreProposeMVCCMaintenanceRoutesAfterSplit(t *testing.T) {
	db, localMeta := openStoreDB(t)
	coord := newTestSchedulerSink()
	st := NewStore(Config{
		Scheduler:      coord,
		StoreID:        1,
		CommandApplier: newTestMVCCApplier(db),
		PeerBuilder:    mvccTestPeerBuilder(t, db, localMeta, 1),
	})
	t.Cleanup(func() { st.Close() })

	parent := localmeta.RegionMeta{
		ID:       131,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 31}},
	}
	parentCfg, err := mvccTestPeerBuilder(t, db, localMeta, 1)(parent)
	require.NoError(t, err)
	parentPeer, err := st.StartPeer(parentCfg, []myraft.Peer{{ID: 31}})
	require.NoError(t, err)
	t.Cleanup(func() { st.StopPeer(parentPeer.ID()) })
	require.NoError(t, parentPeer.Campaign())

	child := localmeta.RegionMeta{
		ID:       132,
		StartKey: []byte("m"),
		EndKey:   []byte("z"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 32}},
	}
	require.NoError(t, st.ProposeSplit(parent.ID, child, child.StartKey))
	require.Eventually(t, func() bool {
		_, ok := st.RegionMetaByID(child.ID)
		return ok
	}, time.Second, 10*time.Millisecond)
	childPeer, ok := st.Peer(child.Peers[0].PeerID)
	require.True(t, ok)
	require.NoError(t, childPeer.Campaign())
	require.Eventually(t, func() bool {
		status, ok := st.RegionRuntimeStatus(child.ID)
		return ok && status.Hosted && status.Leader
	}, time.Second, 10*time.Millisecond)

	leftEntry := txnstore.NewInternalEntry(txnstore.CFWrite, []byte("b-post-split-gc"), 33, nil, txnstore.BitDelete, 0)
	defer leftEntry.DecrRef()
	rightEntry := txnstore.NewInternalEntry(txnstore.CFWrite, []byte("t-post-split-gc"), 44, nil, txnstore.BitDelete, 0)
	defer rightEntry.DecrRef()

	applied, writes, defaults, err := st.ProposeMVCCMaintenance(context.Background(), []*txnstore.Entry{leftEntry, rightEntry})
	require.NoError(t, err)
	require.Equal(t, uint64(2), applied)
	require.Equal(t, uint64(2), writes)
	require.Zero(t, defaults)

	gotLeft, err := db.GetInternalEntry(txnstore.CFWrite, []byte("b-post-split-gc"), 33)
	require.NoError(t, err)
	defer gotLeft.DecrRef()
	require.NotZero(t, gotLeft.Meta&txnstore.BitDelete)
	gotRight, err := db.GetInternalEntry(txnstore.CFWrite, []byte("t-post-split-gc"), 44)
	require.NoError(t, err)
	defer gotRight.DecrRef()
	require.NotZero(t, gotRight.Meta&txnstore.BitDelete)
}

func TestStoreResolveLocks(t *testing.T) {
	db, localMeta := openStoreDB(t)
	coord := newTestSchedulerSink()
	st := NewStore(Config{Scheduler: coord, StoreID: 1, CommandApplier: newTestMVCCApplier(db)})
	t.Cleanup(func() { st.Close() })

	region := &localmeta.RegionMeta{
		ID:       113,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 1}},
	}
	cfg := &peer.Config{
		RaftConfig: myraft.Config{
			ID:              1,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
			PreVote:         true,
		},
		Transport: noopTransport{},
		Storage:   mustPeerStorage(t, db, localMeta, region.ID),
		GroupID:   region.ID,
		Region:    region,
	}
	p, err := st.StartPeer(cfg, []myraft.Peer{{ID: 1}})
	require.NoError(t, err)
	t.Cleanup(func() { st.StopPeer(p.ID()) })
	require.NoError(t, p.Campaign())

	epoch := &metapb.RegionEpoch{Version: 1, ConfVersion: 1}
	prewrite := &raftcmdpb.RaftCmdRequest{
		Header: &raftcmdpb.CmdHeader{RegionId: region.ID, RegionEpoch: epoch},
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_PREWRITE,
			Cmd: &raftcmdpb.Request_Prewrite{Prewrite: &kvrpcpb.PrewriteRequest{
				Mutations: []*kvrpcpb.Mutation{{
					Op:    kvrpcpb.Mutation_Put,
					Key:   []byte("resolve-key"),
					Value: []byte("resolve-value"),
				}},
				PrimaryLock:  []byte("resolve-key"),
				StartVersion: 20,
				LockTtl:      3000,
			}},
		}},
	}
	resp, err := st.ProposeCommand(context.Background(), prewrite)
	require.NoError(t, err)
	require.Nil(t, resp.GetRegionError())
	require.Empty(t, resp.GetResponses()[0].GetPrewrite().GetErrors())

	resolved, err := st.ResolveLocks(context.Background(), 20, 40, [][]byte{[]byte("resolve-key")})
	require.NoError(t, err)
	require.Equal(t, uint64(1), resolved)

	reader := percolator.NewReader(db)
	val, _, err := reader.GetValue([]byte("resolve-key"), 50)
	require.NoError(t, err)
	require.Equal(t, []byte("resolve-value"), val)
}

func TestStoreResolveLocksConvergesAfterPartialRegionFailure(t *testing.T) {
	db, localMeta := openStoreDB(t)
	coord := newTestSchedulerSink()
	st := NewStore(Config{Scheduler: coord, StoreID: 1, CommandApplier: newTestMVCCApplier(db)})
	t.Cleanup(func() { st.Close() })

	left := &localmeta.RegionMeta{
		ID:       123,
		StartKey: []byte("a"),
		EndKey:   []byte("m"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 23}},
	}
	leftCfg := &peer.Config{
		RaftConfig: myraft.Config{
			ID:              23,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
			PreVote:         true,
		},
		Transport: noopTransport{},
		Storage:   mustPeerStorage(t, db, localMeta, left.ID),
		GroupID:   left.ID,
		Region:    left,
	}
	leftPeer, err := st.StartPeer(leftCfg, []myraft.Peer{{ID: 23}})
	require.NoError(t, err)
	t.Cleanup(func() { st.StopPeer(leftPeer.ID()) })
	require.NoError(t, leftPeer.Campaign())

	right := localmeta.RegionMeta{
		ID:       124,
		StartKey: []byte("m"),
		EndKey:   []byte("z"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 24}},
	}
	require.NoError(t, st.applyRegionMetaSilent(right))

	applyTestLockRecord(t, db, []byte("b-lock-key"), 20, 5)
	applyTestLockRecord(t, db, []byte("t-lock-key"), 20, 5)

	resolved, err := st.ResolveLocks(context.Background(), 20, 0, [][]byte{
		[]byte("b-lock-key"),
		[]byte("t-lock-key"),
	})
	require.ErrorContains(t, err, "region 124")
	require.Equal(t, uint64(1), resolved)
	reader := percolator.NewReader(db)
	leftLock, err := reader.GetLock([]byte("b-lock-key"))
	require.NoError(t, err)
	require.Nil(t, leftLock)
	rightLock, err := reader.GetLock([]byte("t-lock-key"))
	require.NoError(t, err)
	require.NotNil(t, rightLock)

	rightCfg := &peer.Config{
		RaftConfig: myraft.Config{
			ID:              24,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
			PreVote:         true,
		},
		Transport: noopTransport{},
		Storage:   mustPeerStorage(t, db, localMeta, right.ID),
		GroupID:   right.ID,
		Region:    &right,
	}
	rightPeer, err := st.StartPeer(rightCfg, []myraft.Peer{{ID: 24}})
	require.NoError(t, err)
	t.Cleanup(func() { st.StopPeer(rightPeer.ID()) })
	require.NoError(t, rightPeer.Campaign())

	resolved, err = st.ResolveLocks(context.Background(), 20, 0, [][]byte{
		[]byte("b-lock-key"),
		[]byte("t-lock-key"),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), resolved)
	rightLock, err = reader.GetLock([]byte("t-lock-key"))
	require.NoError(t, err)
	require.Nil(t, rightLock)
}

func applyTestLockRecord(t *testing.T, db txnstore.Store, key []byte, startTs, ttl uint64) {
	t.Helper()
	lock := txnmvcc.EncodeLock(txnmvcc.Lock{
		Primary:   key,
		Ts:        startTs,
		StartTime: startTs,
		TTL:       ttl,
		Kind:      kvrpcpb.Mutation_Put,
	})
	entry := txnstore.NewInternalEntry(txnstore.CFLock, key, txnstore.MaxVersion, lock, 0, 0)
	defer entry.DecrRef()
	require.NoError(t, db.ApplyInternalEntries([]*txnstore.Entry{entry}))
}

func mvccTestPeerBuilder(t *testing.T, db *local.DB, localMeta *localmeta.Store, storeID uint64) PeerBuilder {
	t.Helper()
	return func(meta localmeta.RegionMeta) (*peer.Config, error) {
		var peerID uint64
		for _, peerMeta := range meta.Peers {
			if peerMeta.StoreID == storeID {
				peerID = peerMeta.PeerID
				break
			}
		}
		if peerID == 0 {
			return nil, fmt.Errorf("store %d missing peer in region %d", storeID, meta.ID)
		}
		return &peer.Config{
			RaftConfig: myraft.Config{
				ID:              peerID,
				ElectionTick:    5,
				HeartbeatTick:   1,
				MaxSizePerMsg:   1 << 20,
				MaxInflightMsgs: 256,
				PreVote:         true,
			},
			Transport: noopTransport{},
			Storage:   mustPeerStorage(t, db, localMeta, meta.ID),
			GroupID:   meta.ID,
			Region:    localmeta.CloneRegionMetaPtr(&meta),
		}, nil
	}
}

func TestStoreProposeMVCCMaintenanceRejectsNonTombstone(t *testing.T) {
	db, localMeta := openStoreDB(t)
	coord := newTestSchedulerSink()
	st := NewStore(Config{Scheduler: coord, StoreID: 1, CommandApplier: newTestMVCCApplier(db)})
	t.Cleanup(func() { st.Close() })

	region := &localmeta.RegionMeta{
		ID:       112,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 1}},
	}
	cfg := &peer.Config{
		RaftConfig: myraft.Config{
			ID:              1,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
			PreVote:         true,
		},
		Transport: noopTransport{},
		Storage:   mustPeerStorage(t, db, localMeta, region.ID),
		GroupID:   region.ID,
		Region:    region,
	}
	p, err := st.StartPeer(cfg, []myraft.Peer{{ID: 1}})
	require.NoError(t, err)
	t.Cleanup(func() { st.StopPeer(p.ID()) })
	require.NoError(t, p.Campaign())

	entry := txnstore.NewInternalEntry(txnstore.CFWrite, []byte("gc-key"), 33, nil, 0, 0)
	defer entry.DecrRef()
	applied, writes, defaults, err := st.ProposeMVCCMaintenance(context.Background(), []*txnstore.Entry{entry})
	require.ErrorContains(t, err, "not a tombstone")
	require.Zero(t, applied)
	require.Zero(t, writes)
	require.Zero(t, defaults)

	_, err = db.GetInternalEntry(txnstore.CFWrite, []byte("gc-key"), 33)
	require.Error(t, err)
}

func TestStoreProposeCommandRejectsDuplicateRequestID(t *testing.T) {
	db, localMeta := openStoreDB(t)
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	applier := func(req *raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
		return &raftcmdpb.RaftCmdResponse{
			Header: req.GetHeader(),
		}, nil
	}
	st := NewStore(Config{
		StoreID:        1,
		CommandApplier: applier,
		CommandTimeout: time.Second,
	})
	t.Cleanup(func() { st.Close() })

	region := &localmeta.RegionMeta{
		ID:       777,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 17}},
	}
	cfg := &peer.Config{
		RaftConfig: myraft.Config{
			ID:              17,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
			PreVote:         true,
		},
		Transport: noopTransport{},
		Storage:   mustPeerStorage(t, db, localMeta, region.ID),
		GroupID:   region.ID,
		Region:    region,
	}
	p, err := st.StartPeer(cfg, []myraft.Peer{{ID: 17}})
	require.NoError(t, err)
	t.Cleanup(func() { st.StopPeer(p.ID()) })
	require.NoError(t, p.Campaign())

	req := func() *raftcmdpb.RaftCmdRequest {
		return &raftcmdpb.RaftCmdRequest{
			Header: &raftcmdpb.CmdHeader{
				RegionId:    region.ID,
				RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
				RequestId:   9001,
			},
			Requests: []*raftcmdpb.Request{{
				CmdType: raftcmdpb.CmdType_CMD_GET,
				Cmd: &raftcmdpb.Request_Get{Get: &kvrpcpb.GetRequest{
					Key: []byte("dup-key"),
				}},
			}},
		}
	}

	firstDone := make(chan error, 1)
	go func() {
		_, err := st.ProposeCommand(context.Background(), req())
		firstDone <- err
	}()

	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first proposal did not enter apply path in time")
	}

	start := time.Now()
	_, err = st.ProposeCommand(context.Background(), req())
	elapsed := time.Since(start)
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate proposal id")
	require.Less(t, elapsed, 300*time.Millisecond)

	close(release)
	select {
	case err := <-firstDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("first proposal did not finish in time")
	}
}

func TestStoreProposeCommandNotLeader(t *testing.T) {
	db, localMeta := openStoreDB(t)
	applier := newTestMVCCApplier(db)
	st := NewStore(Config{StoreID: 2, CommandApplier: applier})
	t.Cleanup(func() { st.Close() })
	region := &localmeta.RegionMeta{
		ID:       202,
		StartKey: []byte("k"),
		EndKey:   []byte("z"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 2, PeerID: 5}},
	}
	cfg := &peer.Config{
		RaftConfig: myraft.Config{
			ID:              5,
			ElectionTick:    10,
			HeartbeatTick:   2,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
		},
		Transport: noopTransport{},
		Storage:   mustPeerStorage(t, db, localMeta, 202),
		GroupID:   202,
		Region:    region,
	}
	peer, err := st.StartPeer(cfg, []myraft.Peer{{ID: 5}})
	require.NoError(t, err)
	t.Cleanup(func() { st.StopPeer(peer.ID()) })

	req := &raftcmdpb.RaftCmdRequest{
		Header: &raftcmdpb.CmdHeader{RegionId: region.ID, RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVersion: 1}},
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_PREWRITE,
			Cmd:     &raftcmdpb.Request_Prewrite{Prewrite: &kvrpcpb.PrewriteRequest{StartVersion: 1}},
		}},
	}
	resp, err := st.ProposeCommand(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp.GetRegionError())
	require.NotNil(t, resp.GetRegionError().GetNotLeader())
}

func TestStoreProposeCommandEpochMismatch(t *testing.T) {
	db, localMeta := openStoreDB(t)
	applier := newTestMVCCApplier(db)
	st := NewStore(Config{StoreID: 3, CommandApplier: applier})
	t.Cleanup(func() { st.Close() })
	region := &localmeta.RegionMeta{
		ID:       303,
		StartKey: []byte("a"),
		EndKey:   []byte("h"),
		Epoch:    metaregion.Epoch{Version: 2, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 3, PeerID: 7}},
	}
	cfg := &peer.Config{
		RaftConfig: myraft.Config{
			ID:              7,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
		},
		Transport: noopTransport{},
		Storage:   mustPeerStorage(t, db, localMeta, 303),
		GroupID:   303,
		Region:    region,
	}
	peer, err := st.StartPeer(cfg, []myraft.Peer{{ID: 7}})
	require.NoError(t, err)
	t.Cleanup(func() { st.StopPeer(peer.ID()) })
	require.NoError(t, peer.Campaign())

	badReq := &raftcmdpb.RaftCmdRequest{
		Header:   &raftcmdpb.CmdHeader{RegionId: region.ID, RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVersion: 1}},
		Requests: []*raftcmdpb.Request{{CmdType: raftcmdpb.CmdType_CMD_PREWRITE}},
	}
	resp, err := st.ProposeCommand(context.Background(), badReq)
	require.NoError(t, err)
	require.NotNil(t, resp.GetRegionError())
	require.NotNil(t, resp.GetRegionError().GetEpochNotMatch())
}

func TestStoreProposeCommandSurvivesSchedulerUnavailable(t *testing.T) {
	db, localMeta := openStoreDB(t)
	coord := &degradedSchedulerSink{
		testSchedulerSink: *newTestSchedulerSink(),
		status: storecontrol.Status{
			Mode:      storecontrol.ModeUnavailable,
			Degraded:  true,
			LastError: "coordinator unavailable",
		},
	}
	applier := newTestMVCCApplier(db)
	st := NewStore(Config{Scheduler: coord, StoreID: 1, CommandApplier: applier})
	t.Cleanup(func() { st.Close() })

	region := &localmeta.RegionMeta{
		ID:       909,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 1}},
	}
	cfg := &peer.Config{
		RaftConfig: myraft.Config{ID: 1, ElectionTick: 5, HeartbeatTick: 1, MaxSizePerMsg: 1 << 20, MaxInflightMsgs: 256, PreVote: true},
		Transport:  noopTransport{},
		Storage:    mustPeerStorage(t, db, localMeta, 909),
		GroupID:    909,
		Region:     region,
	}
	p, err := st.StartPeer(cfg, []myraft.Peer{{ID: 1}})
	require.NoError(t, err)
	t.Cleanup(func() { st.StopPeer(p.ID()) })
	require.NoError(t, p.Campaign())

	epoch := &metapb.RegionEpoch{Version: 1, ConfVersion: 1}
	prewrite := &raftcmdpb.RaftCmdRequest{
		Header: &raftcmdpb.CmdHeader{RegionId: region.ID, RegionEpoch: epoch},
		Requests: []*raftcmdpb.Request{{CmdType: raftcmdpb.CmdType_CMD_PREWRITE, Cmd: &raftcmdpb.Request_Prewrite{Prewrite: &kvrpcpb.PrewriteRequest{
			Mutations:   []*kvrpcpb.Mutation{{Op: kvrpcpb.Mutation_Put, Key: []byte("sched-key"), Value: []byte("sched-value")}},
			PrimaryLock: []byte("sched-key"), StartVersion: 50, LockTtl: 3000,
		}}}},
	}
	resp, err := st.ProposeCommand(context.Background(), prewrite)
	require.NoError(t, err)
	require.Nil(t, resp.GetRegionError())

	commit := &raftcmdpb.RaftCmdRequest{
		Header: &raftcmdpb.CmdHeader{RegionId: region.ID, RegionEpoch: epoch},
		Requests: []*raftcmdpb.Request{{CmdType: raftcmdpb.CmdType_CMD_COMMIT, Cmd: &raftcmdpb.Request_Commit{Commit: &kvrpcpb.CommitRequest{
			Keys: [][]byte{[]byte("sched-key")}, StartVersion: 50, CommitVersion: 80,
		}}}},
	}
	resp, err = st.ProposeCommand(context.Background(), commit)
	require.NoError(t, err)
	require.Nil(t, resp.GetRegionError())

	status := st.SchedulerStatus()
	require.True(t, status.Degraded)
	require.Equal(t, storecontrol.ModeUnavailable, status.Mode)
	require.Contains(t, status.LastError, "coordinator unavailable")

	reader := percolator.NewReader(db)
	val, _, err := reader.GetValue([]byte("sched-key"), 90)
	require.NoError(t, err)
	require.Equal(t, []byte("sched-value"), val)
}

func TestStoreReadCommandValidation(t *testing.T) {
	store := NewStore(Config{})
	t.Cleanup(func() { store.Close() })

	if _, err := store.ReadCommand(context.Background(), &raftcmdpb.RaftCmdRequest{}); err == nil {
		t.Fatal("expected missing region id error")
	}

	req := &raftcmdpb.RaftCmdRequest{Header: &raftcmdpb.CmdHeader{RegionId: 1}}
	resp, err := store.ReadCommand(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.GetRegionError())
	require.NotNil(t, resp.GetRegionError().GetRegionNotFound())
}

func TestStoreReadCommandStoreNotMatch(t *testing.T) {
	store := NewStore(Config{StoreID: 7})
	t.Cleanup(func() { store.Close() })

	req := &raftcmdpb.RaftCmdRequest{Header: &raftcmdpb.CmdHeader{RegionId: 1, StoreId: 9}}
	resp, err := store.ReadCommand(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.GetRegionError())
	require.NotNil(t, resp.GetRegionError().GetStoreNotMatch())
	require.Equal(t, uint64(9), resp.GetRegionError().GetStoreNotMatch().GetRequestStoreId())
	require.Equal(t, uint64(7), resp.GetRegionError().GetStoreNotMatch().GetActualStoreId())
}

func TestStoreReadCommandLeaderOnlyRejectsFollower(t *testing.T) {
	db, localMeta := openStoreDB(t)
	applier := func(req *raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
		return &raftcmdpb.RaftCmdResponse{Header: req.GetHeader()}, nil
	}
	st := NewStore(Config{StoreID: 1, CommandApplier: applier})
	t.Cleanup(func() { st.Close() })

	region := &localmeta.RegionMeta{
		ID:       405,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 11}, {StoreID: 2, PeerID: 22}},
	}
	p, err := st.StartPeer(&peer.Config{
		RaftConfig: myraft.Config{
			ID:              11,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
			PreVote:         true,
		},
		Transport: noopTransport{},
		Storage:   mustPeerStorage(t, db, localMeta, region.ID),
		GroupID:   region.ID,
		Region:    region,
	}, []myraft.Peer{{ID: 11}, {ID: 22}})
	require.NoError(t, err)
	t.Cleanup(func() { st.StopPeer(p.ID()) })

	resp, err := st.ReadCommand(context.Background(), &raftcmdpb.RaftCmdRequest{
		Header: &raftcmdpb.CmdHeader{
			RegionId:    region.ID,
			RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
		},
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_GET,
			Cmd:     &raftcmdpb.Request_Get{Get: &kvrpcpb.GetRequest{Key: []byte("b")}},
		}},
	})
	require.NoError(t, err)
	require.NotNil(t, resp.GetRegionError())
	require.NotNil(t, resp.GetRegionError().GetNotLeader())
}

func TestStoreReadCommandStrongFollowerPreferPreservesCanceledContext(t *testing.T) {
	db, localMeta := openStoreDB(t)
	applier := func(req *raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
		return &raftcmdpb.RaftCmdResponse{Header: req.GetHeader()}, nil
	}
	st := NewStore(Config{StoreID: 1, CommandApplier: applier})
	t.Cleanup(func() { st.Close() })

	region := &localmeta.RegionMeta{
		ID:       407,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 11}, {StoreID: 2, PeerID: 22}},
	}
	p, err := st.StartPeer(&peer.Config{
		RaftConfig: myraft.Config{
			ID:              11,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
			PreVote:         true,
		},
		Transport: noopTransport{},
		Storage:   mustPeerStorage(t, db, localMeta, region.ID),
		GroupID:   region.ID,
		Region:    region,
	}, []myraft.Peer{{ID: 11}, {ID: 22}})
	require.NoError(t, err)
	t.Cleanup(func() { st.StopPeer(p.ID()) })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = st.ReadCommand(ctx, &raftcmdpb.RaftCmdRequest{
		Header: &raftcmdpb.CmdHeader{
			RegionId:        region.ID,
			RegionEpoch:     &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
			ReadConsistency: kvrpcpb.ReadConsistency_READ_CONSISTENCY_STRONG,
			ReadPreference:  kvrpcpb.ReadPreference_READ_PREFERENCE_FOLLOWER_PREFER,
		},
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_GET,
			Cmd:     &raftcmdpb.Request_Get{Get: &kvrpcpb.GetRequest{Key: []byte("b")}},
		}},
	})
	require.ErrorIs(t, err, context.Canceled)
}

func TestStoreReadCommandBoundedStaleReadsAppliedState(t *testing.T) {
	db, localMeta := openStoreDB(t)
	applier := func(req *raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
		return &raftcmdpb.RaftCmdResponse{
			Header: req.GetHeader(),
			Responses: []*raftcmdpb.Response{{
				Cmd: &raftcmdpb.Response_Get{Get: &kvrpcpb.GetResponse{Value: []byte("ok")}},
			}},
		}, nil
	}
	st := NewStore(Config{StoreID: 1, CommandApplier: applier})
	t.Cleanup(func() { st.Close() })

	region := &localmeta.RegionMeta{
		ID:       406,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 11}},
	}
	p, err := st.StartPeer(&peer.Config{
		RaftConfig: myraft.Config{
			ID:              11,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
			PreVote:         true,
		},
		Transport: noopTransport{},
		Storage:   mustPeerStorage(t, db, localMeta, region.ID),
		GroupID:   region.ID,
		Region:    region,
	}, []myraft.Peer{{ID: 11}})
	require.NoError(t, err)
	t.Cleanup(func() { st.StopPeer(p.ID()) })
	require.NoError(t, p.Campaign())
	require.Greater(t, p.AppliedIndex(), uint64(0))

	resp, err := st.ReadCommand(context.Background(), &raftcmdpb.RaftCmdRequest{
		Header: &raftcmdpb.CmdHeader{
			RegionId:          region.ID,
			RegionEpoch:       &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
			ReadConsistency:   kvrpcpb.ReadConsistency_READ_CONSISTENCY_BOUNDED_STALE,
			ReadPreference:    kvrpcpb.ReadPreference_READ_PREFERENCE_FOLLOWER_PREFER,
			MaxStaleReadIndex: 0,
		},
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_GET,
			Cmd:     &raftcmdpb.Request_Get{Get: &kvrpcpb.GetRequest{Key: []byte("b")}},
		}},
	})
	require.NoError(t, err)
	require.Nil(t, resp.GetRegionError())
	require.Equal(t, []byte("ok"), resp.GetResponses()[0].GetGet().GetValue())
}

func TestReadOnlyRequestPredicate(t *testing.T) {
	require.False(t, isReadOnlyRequest(nil))

	readReq := &raftcmdpb.RaftCmdRequest{Requests: []*raftcmdpb.Request{
		{CmdType: raftcmdpb.CmdType_CMD_GET},
		{CmdType: raftcmdpb.CmdType_CMD_SCAN},
	}}
	require.True(t, isReadOnlyRequest(readReq))

	writeReq := &raftcmdpb.RaftCmdRequest{Requests: []*raftcmdpb.Request{
		{CmdType: raftcmdpb.CmdType_CMD_PREWRITE},
	}}
	require.False(t, isReadOnlyRequest(writeReq))
}

func TestStoreReadCommandExecutesAndTrimsScanResponse(t *testing.T) {
	db, localMeta := openStoreDB(t)
	applier := func(req *raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
		return &raftcmdpb.RaftCmdResponse{
			Header: req.GetHeader(),
			Responses: []*raftcmdpb.Response{{
				Cmd: &raftcmdpb.Response_Scan{Scan: &kvrpcpb.ScanResponse{
					Kvs: []*kvrpcpb.KV{
						{Key: []byte("a"), Value: []byte("drop-left")},
						{Key: []byte("b"), Value: []byte("keep-start")},
						{Key: []byte("l"), Value: []byte("keep-middle")},
						{Key: []byte("m"), Value: []byte("drop-right")},
						nil,
					},
				}},
			}},
		}, nil
	}
	st := NewStore(Config{StoreID: 1, CommandApplier: applier})
	t.Cleanup(func() { st.Close() })

	region := &localmeta.RegionMeta{
		ID:       404,
		StartKey: []byte("b"),
		EndKey:   []byte("m"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 11}},
	}
	cfg := &peer.Config{
		RaftConfig: myraft.Config{
			ID:              11,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
			PreVote:         true,
		},
		Transport: noopTransport{},
		Storage:   mustPeerStorage(t, db, localMeta, region.ID),
		GroupID:   region.ID,
		Region:    region,
	}
	p, err := st.StartPeer(cfg, []myraft.Peer{{ID: 11}})
	require.NoError(t, err)
	t.Cleanup(func() { st.StopPeer(p.ID()) })
	require.NoError(t, p.Campaign())

	req := &raftcmdpb.RaftCmdRequest{
		Header: &raftcmdpb.CmdHeader{
			RegionId:    region.ID,
			RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
		},
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_SCAN,
			Cmd: &raftcmdpb.Request_Scan{Scan: &kvrpcpb.ScanRequest{
				StartKey: []byte("b"),
				Limit:    4,
			}},
		}},
	}
	resp, err := st.ReadCommand(context.Background(), req)
	require.NoError(t, err)
	require.Nil(t, resp.GetRegionError())
	require.Len(t, resp.GetResponses(), 1)
	kvs := resp.GetResponses()[0].GetScan().GetKvs()
	require.Len(t, kvs, 2)
	require.Equal(t, []byte("b"), kvs[0].GetKey())
	require.Equal(t, []byte("l"), kvs[1].GetKey())
}

func TestValidateRequestKeysAcrossCommandKinds(t *testing.T) {
	meta := localmeta.RegionMeta{
		ID:       12,
		StartKey: []byte("b"),
		EndKey:   []byte("m"),
	}

	cases := []struct {
		name string
		req  *raftcmdpb.RaftCmdRequest
		kind func(*errorpb.RegionError) any
	}{
		{
			name: "get out of range",
			req: &raftcmdpb.RaftCmdRequest{Requests: []*raftcmdpb.Request{{
				CmdType: raftcmdpb.CmdType_CMD_GET,
				Cmd:     &raftcmdpb.Request_Get{Get: &kvrpcpb.GetRequest{Key: []byte("a")}},
			}}},
			kind: func(err *errorpb.RegionError) any { return err.GetKeyNotInRegion() },
		},
		{
			name: "scan out of range",
			req: &raftcmdpb.RaftCmdRequest{Requests: []*raftcmdpb.Request{{
				CmdType: raftcmdpb.CmdType_CMD_SCAN,
				Cmd:     &raftcmdpb.Request_Scan{Scan: &kvrpcpb.ScanRequest{StartKey: []byte("z")}},
			}}},
			kind: func(err *errorpb.RegionError) any { return err.GetKeyNotInRegion() },
		},
		{
			name: "prewrite out of range",
			req: &raftcmdpb.RaftCmdRequest{Requests: []*raftcmdpb.Request{{
				CmdType: raftcmdpb.CmdType_CMD_PREWRITE,
				Cmd: &raftcmdpb.Request_Prewrite{Prewrite: &kvrpcpb.PrewriteRequest{
					Mutations: []*kvrpcpb.Mutation{{Op: kvrpcpb.Mutation_Put, Key: []byte("a")}},
				}},
			}}},
			kind: func(err *errorpb.RegionError) any { return err.GetKeyNotInRegion() },
		},
		{
			name: "commit out of range",
			req: &raftcmdpb.RaftCmdRequest{Requests: []*raftcmdpb.Request{{
				CmdType: raftcmdpb.CmdType_CMD_COMMIT,
				Cmd:     &raftcmdpb.Request_Commit{Commit: &kvrpcpb.CommitRequest{Keys: [][]byte{[]byte("a")}}},
			}}},
			kind: func(err *errorpb.RegionError) any { return err.GetKeyNotInRegion() },
		},
		{
			name: "rollback out of range",
			req: &raftcmdpb.RaftCmdRequest{Requests: []*raftcmdpb.Request{{
				CmdType: raftcmdpb.CmdType_CMD_BATCH_ROLLBACK,
				Cmd:     &raftcmdpb.Request_BatchRollback{BatchRollback: &kvrpcpb.BatchRollbackRequest{Keys: [][]byte{[]byte("a")}}},
			}}},
			kind: func(err *errorpb.RegionError) any { return err.GetKeyNotInRegion() },
		},
		{
			name: "resolve lock out of range",
			req: &raftcmdpb.RaftCmdRequest{Requests: []*raftcmdpb.Request{{
				CmdType: raftcmdpb.CmdType_CMD_RESOLVE_LOCK,
				Cmd:     &raftcmdpb.Request_ResolveLock{ResolveLock: &kvrpcpb.ResolveLockRequest{Keys: [][]byte{[]byte("a")}}},
			}}},
			kind: func(err *errorpb.RegionError) any { return err.GetKeyNotInRegion() },
		},
		{
			name: "check txn out of range",
			req: &raftcmdpb.RaftCmdRequest{Requests: []*raftcmdpb.Request{{
				CmdType: raftcmdpb.CmdType_CMD_CHECK_TXN_STATUS,
				Cmd:     &raftcmdpb.Request_CheckTxnStatus{CheckTxnStatus: &kvrpcpb.CheckTxnStatusRequest{PrimaryKey: []byte("a")}},
			}}},
			kind: func(err *errorpb.RegionError) any { return err.GetKeyNotInRegion() },
		},
		{
			name: "txn heartbeat out of range",
			req: &raftcmdpb.RaftCmdRequest{Requests: []*raftcmdpb.Request{{
				CmdType: raftcmdpb.CmdType_CMD_TXN_HEART_BEAT,
				Cmd:     &raftcmdpb.Request_TxnHeartBeat{TxnHeartBeat: &kvrpcpb.TxnHeartBeatRequest{PrimaryKey: []byte("a")}},
			}}},
			kind: func(err *errorpb.RegionError) any { return err.GetKeyNotInRegion() },
		},
		{
			name: "mvcc maintenance out of range",
			req: &raftcmdpb.RaftCmdRequest{Requests: []*raftcmdpb.Request{{
				CmdType: raftcmdpb.CmdType_CMD_MVCC_MAINTENANCE,
				Cmd: &raftcmdpb.Request_MvccMaintenance{MvccMaintenance: &kvrpcpb.MVCCMaintenanceRequest{
					Tombstones: []*kvrpcpb.InternalEntryTombstone{{ColumnFamily: kvrpcpb.InternalEntryTombstone_WRITE, Key: []byte("a"), Version: 1}},
				}},
			}}},
			kind: func(err *errorpb.RegionError) any { return err.GetKeyNotInRegion() },
		},
		{
			name: "mvcc maintenance empty key",
			req: &raftcmdpb.RaftCmdRequest{Requests: []*raftcmdpb.Request{{
				CmdType: raftcmdpb.CmdType_CMD_MVCC_MAINTENANCE,
				Cmd: &raftcmdpb.Request_MvccMaintenance{MvccMaintenance: &kvrpcpb.MVCCMaintenanceRequest{
					Tombstones: []*kvrpcpb.InternalEntryTombstone{{ColumnFamily: kvrpcpb.InternalEntryTombstone_WRITE, Version: 1}},
				}},
			}}},
			kind: func(err *errorpb.RegionError) any { return err.GetKeyNotInRegion() },
		},
		{
			name: "prepared mvcc install routing key out of range",
			req: &raftcmdpb.RaftCmdRequest{Requests: []*raftcmdpb.Request{{
				CmdType: raftcmdpb.CmdType_CMD_INSTALL_PREPARED_MVCC,
				Cmd: &raftcmdpb.Request_InstallPreparedMvcc{InstallPreparedMvcc: &kvrpcpb.InstallPreparedMVCCEntriesRequest{
					RoutingKey: []byte("a"),
				}},
			}}},
			kind: func(err *errorpb.RegionError) any { return err.GetKeyNotInRegion() },
		},
		{
			name: "unknown command",
			req: &raftcmdpb.RaftCmdRequest{Requests: []*raftcmdpb.Request{{
				CmdType: raftcmdpb.CmdType(255),
			}}},
			kind: func(err *errorpb.RegionError) any { return err.GetEpochNotMatch() },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err, _ := validateRequestKeys(meta, tc.req)
			require.NotNil(t, err)
			require.NotNil(t, tc.kind(err))
		})
	}
}

func TestValidateRequestKeysRejectsPreparedMVCCEntriesOutsideRegion(t *testing.T) {
	meta := localmeta.RegionMeta{
		ID:       12,
		StartKey: []byte("b"),
		EndKey:   []byte("m"),
	}
	req := &raftcmdpb.RaftCmdRequest{Requests: []*raftcmdpb.Request{{
		CmdType: raftcmdpb.CmdType_CMD_INSTALL_PREPARED_MVCC,
		Cmd: &raftcmdpb.Request_InstallPreparedMvcc{InstallPreparedMvcc: &kvrpcpb.InstallPreparedMVCCEntriesRequest{
			RoutingKey:    []byte("c"),
			CommitVersion: 10,
			Entries: []*kvrpcpb.PreparedMVCCEntry{
				{ColumnFamily: kvrpcpb.PreparedMVCCEntry_DEFAULT, Key: []byte("c"), Version: 10, Value: []byte("in-region"), HasValue: true},
				{ColumnFamily: kvrpcpb.PreparedMVCCEntry_DEFAULT, Key: []byte("z"), Version: 10, Value: []byte("out-of-region"), HasValue: true},
			},
		}},
	}}}

	regionErr, reason := validateRequestKeys(meta, req)
	require.Equal(t, AdmissionReasonKeyNotInRegion, reason)
	require.NotNil(t, regionErr)
	require.Equal(t, []byte("z"), regionErr.GetKeyNotInRegion().GetKey())
}

func TestValidateRequestKeysRejectsPreparedMVCCDependencyOutsideRegion(t *testing.T) {
	meta := localmeta.RegionMeta{
		ID:       12,
		StartKey: []byte("b"),
		EndKey:   []byte("m"),
	}
	req := &raftcmdpb.RaftCmdRequest{Requests: []*raftcmdpb.Request{{
		CmdType: raftcmdpb.CmdType_CMD_INSTALL_PREPARED_MVCC,
		Cmd: &raftcmdpb.Request_InstallPreparedMvcc{InstallPreparedMvcc: &kvrpcpb.InstallPreparedMVCCEntriesRequest{
			RoutingKey:     []byte("c"),
			CommitVersion:  10,
			DependencyKeys: [][]byte{[]byte("c"), []byte("z")},
		}},
	}}}

	regionErr, reason := validateRequestKeys(meta, req)
	require.Equal(t, AdmissionReasonKeyNotInRegion, reason)
	require.NotNil(t, regionErr)
	require.Equal(t, []byte("z"), regionErr.GetKeyNotInRegion().GetKey())
}

func TestValidateRequestKeysRejectsMalformedPreparedMVCCInstall(t *testing.T) {
	meta := localmeta.RegionMeta{
		ID:       12,
		StartKey: []byte("b"),
		EndKey:   []byte("m"),
	}
	req := &raftcmdpb.RaftCmdRequest{Requests: []*raftcmdpb.Request{{
		CmdType: raftcmdpb.CmdType_CMD_INSTALL_PREPARED_MVCC,
		Cmd: &raftcmdpb.Request_InstallPreparedMvcc{InstallPreparedMvcc: &kvrpcpb.InstallPreparedMVCCEntriesRequest{
			RoutingKey:    []byte("c"),
			CommitVersion: 10,
			Entries:       []*kvrpcpb.PreparedMVCCEntry{nil},
		}},
	}}}

	regionErr, reason := validateRequestKeys(meta, req)
	require.Equal(t, AdmissionReasonInvalid, reason)
	require.NotNil(t, regionErr)
	require.NotNil(t, regionErr.GetEpochNotMatch())
}

func TestValidateRequestKeysAcceptsPreparedMVCCInstallWatchKeysOutsideRegion(t *testing.T) {
	meta := localmeta.RegionMeta{ID: 12, StartKey: []byte("b"), EndKey: []byte("m")}
	req := &raftcmdpb.RaftCmdRequest{Requests: []*raftcmdpb.Request{{
		CmdType: raftcmdpb.CmdType_CMD_INSTALL_PREPARED_MVCC,
		Cmd: &raftcmdpb.Request_InstallPreparedMvcc{InstallPreparedMvcc: &kvrpcpb.InstallPreparedMVCCEntriesRequest{
			RoutingKey:    []byte("c"),
			CommitVersion: 10,
			WatchKeys:     [][]byte{[]byte("z")},
		}},
	}}}

	regionErr, reason := validateRequestKeys(meta, req)
	require.Nil(t, regionErr)
	require.Equal(t, AdmissionReasonUnknown, reason)
}

func TestCommandServiceErrorHelpers(t *testing.T) {
	meta := localmeta.RegionMeta{
		ID:       88,
		StartKey: []byte("b"),
		EndKey:   []byte("m"),
		Peers: []metaregion.Peer{
			{StoreID: 1, PeerID: 1},
			{StoreID: 2, PeerID: 2},
		},
	}

	notLeader := notLeaderError(meta, 2)
	require.NotNil(t, notLeader.GetNotLeader())
	require.Equal(t, uint64(88), notLeader.GetNotLeader().GetRegionId())
	require.Equal(t, uint64(2), notLeader.GetNotLeader().GetLeader().GetPeerId())
	require.Equal(t, uint64(2), notLeader.GetNotLeader().GetLeader().GetStoreId())

	key := []byte("a")
	out := keyNotInRegionError(meta, key)
	key[0] = 'z'
	meta.StartKey[0] = 'x'
	meta.EndKey[0] = 'y'
	require.Equal(t, []byte("a"), out.GetKeyNotInRegion().GetKey())
	require.Equal(t, []byte("b"), out.GetKeyNotInRegion().GetStartKey())
	require.Equal(t, []byte("m"), out.GetKeyNotInRegion().GetEndKey())
}

func TestStoreReadCommandRejectsMissingRequestsAndWriteCommands(t *testing.T) {
	st, region := startReadLeaderStore(t, func(*raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
		return &raftcmdpb.RaftCmdResponse{}, nil
	}, time.Second)

	_, err := st.ReadCommand(context.Background(), &raftcmdpb.RaftCmdRequest{
		Header: &raftcmdpb.CmdHeader{
			RegionId:    region.ID,
			RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
		},
	})
	require.ErrorContains(t, err, "read command missing requests")

	_, err = st.ReadCommand(context.Background(), &raftcmdpb.RaftCmdRequest{
		Header: &raftcmdpb.CmdHeader{
			RegionId:    region.ID,
			RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
		},
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_PREWRITE,
			Cmd:     &raftcmdpb.Request_Prewrite{Prewrite: &kvrpcpb.PrewriteRequest{}},
		}},
	})
	require.ErrorContains(t, err, "read command must be read-only")
}

func TestStoreReadCommandRequiresApplyHandler(t *testing.T) {
	st, region := startReadLeaderStore(t, nil, time.Second)

	_, err := st.ReadCommand(context.Background(), &raftcmdpb.RaftCmdRequest{
		Header: &raftcmdpb.CmdHeader{
			RegionId:    region.ID,
			RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
		},
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_GET,
			Cmd:     &raftcmdpb.Request_Get{Get: &kvrpcpb.GetRequest{Key: []byte("b")}},
		}},
	})
	require.ErrorContains(t, err, "command apply without handler")
}

func TestStoreReadCommandPropagatesApplyError(t *testing.T) {
	applyErr := errors.New("apply read boom")
	st, region := startReadLeaderStore(t, func(*raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
		return nil, applyErr
	}, time.Second)

	_, err := st.ReadCommand(context.Background(), &raftcmdpb.RaftCmdRequest{
		Header: &raftcmdpb.CmdHeader{
			RegionId:    region.ID,
			RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
		},
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_GET,
			Cmd:     &raftcmdpb.Request_Get{Get: &kvrpcpb.GetRequest{Key: []byte("b")}},
		}},
	})
	require.ErrorIs(t, err, applyErr)
}

func TestStoreReadCommandReturnsNilResponseWhenApplierDoes(t *testing.T) {
	st, region := startReadLeaderStore(t, func(*raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
		return nil, nil
	}, 0)

	resp, err := st.ReadCommand(context.Background(), &raftcmdpb.RaftCmdRequest{
		Header: &raftcmdpb.CmdHeader{
			RegionId:    region.ID,
			RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVersion: 1},
		},
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_GET,
			Cmd:     &raftcmdpb.Request_Get{Get: &kvrpcpb.GetRequest{Key: []byte("b")}},
		}},
	})
	require.NoError(t, err)
	require.Nil(t, resp)
}

func TestTrimScanResponseHandlesMismatchedResponses(t *testing.T) {
	meta := localmeta.RegionMeta{ID: 99, StartKey: []byte("b"), EndKey: []byte("m")}

	req := &raftcmdpb.RaftCmdRequest{Requests: []*raftcmdpb.Request{
		{CmdType: raftcmdpb.CmdType_CMD_SCAN, Cmd: &raftcmdpb.Request_Scan{Scan: &kvrpcpb.ScanRequest{StartKey: []byte("b")}}},
		{CmdType: raftcmdpb.CmdType_CMD_SCAN, Cmd: &raftcmdpb.Request_Scan{Scan: &kvrpcpb.ScanRequest{StartKey: []byte("c")}}},
	}}
	resp := &raftcmdpb.RaftCmdResponse{Responses: []*raftcmdpb.Response{
		{Cmd: &raftcmdpb.Response_Scan{Scan: &kvrpcpb.ScanResponse{
			Kvs: []*kvrpcpb.KV{
				{Key: []byte("a")},
				{Key: []byte("c")},
				nil,
			},
		}}},
	}}

	trimScanResponse(meta, req, resp)
	require.Len(t, resp.GetResponses(), 1)
	require.Len(t, resp.GetResponses()[0].GetScan().GetKvs(), 1)
	require.Equal(t, []byte("c"), resp.GetResponses()[0].GetScan().GetKvs()[0].GetKey())
}

func startReadLeaderStore(t *testing.T, applier func(*raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error), timeout time.Duration) (*Store, *localmeta.RegionMeta) {
	t.Helper()

	db, localMeta := openStoreDB(t)
	st := NewStore(Config{StoreID: 1, CommandApplier: applier, CommandTimeout: timeout})
	t.Cleanup(func() { st.Close() })

	region := &localmeta.RegionMeta{
		ID:       451,
		StartKey: []byte("b"),
		EndKey:   []byte("m"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 17}},
	}
	cfg := &peer.Config{
		RaftConfig: myraft.Config{
			ID:              17,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
			PreVote:         true,
		},
		Transport: noopTransport{},
		Storage:   mustPeerStorage(t, db, localMeta, region.ID),
		GroupID:   region.ID,
		Region:    region,
	}
	p, err := st.StartPeer(cfg, []myraft.Peer{{ID: 17}})
	require.NoError(t, err)
	t.Cleanup(func() { st.StopPeer(p.ID()) })
	require.NoError(t, p.Campaign())

	return st, region
}
