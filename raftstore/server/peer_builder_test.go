// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"fmt"
	"io"
	"testing"

	"github.com/feichai0017/NoKV/engine/index"
	entrykv "github.com/feichai0017/NoKV/engine/kv"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	myraft "github.com/feichai0017/NoKV/raft"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"github.com/feichai0017/NoKV/raftstore/raftlog"
	snapshotpkg "github.com/feichai0017/NoKV/raftstore/snapshot/sst"
	txnstore "github.com/feichai0017/NoKV/txn/storage"
	"github.com/stretchr/testify/require"
)

type fakeBuilderMVCCStore struct{}

func (fakeBuilderMVCCStore) ApplyInternalEntries(entries []*entrykv.Entry) error { return nil }
func (fakeBuilderMVCCStore) GetInternalEntry(cf entrykv.ColumnFamily, key []byte, version uint64) (*entrykv.Entry, error) {
	return nil, fmt.Errorf("not implemented")
}
func (fakeBuilderMVCCStore) NewInternalIterator(opt *index.Options) index.Iterator { return nil }

type fakeBuilderRaftLog struct{}

func (fakeBuilderRaftLog) Open(groupID uint64, meta *localmeta.Store) (raftlog.PeerStorage, error) {
	return nil, nil
}

type fakeBuilderSnapshotStore struct{}

func (fakeBuilderSnapshotStore) ExportSnapshot(localmeta.RegionMeta) ([]byte, error) { return nil, nil }
func (fakeBuilderSnapshotStore) ImportSnapshot([]byte) (*snapshotpkg.ImportResult, error) {
	return &snapshotpkg.ImportResult{}, nil
}
func (fakeBuilderSnapshotStore) ExportSnapshotTo(io.Writer, localmeta.RegionMeta) (snapshotpkg.Meta, error) {
	return snapshotpkg.Meta{}, nil
}
func (fakeBuilderSnapshotStore) ImportSnapshotFrom(io.Reader) (*snapshotpkg.ImportResult, error) {
	return &snapshotpkg.ImportResult{}, nil
}

var _ txnstore.Store = fakeBuilderMVCCStore{}
var _ RaftLog = fakeBuilderRaftLog{}
var _ snapshotpkg.SnapshotStore = fakeBuilderSnapshotStore{}

func TestDefaultRaftConfigAppliesDefaults(t *testing.T) {
	cfg := defaultRaftConfig(myraft.Config{}, 17)
	require.Equal(t, uint64(17), cfg.ID)
	require.Equal(t, 10, cfg.ElectionTick)
	require.Equal(t, 2, cfg.HeartbeatTick)
	require.Equal(t, uint64(1<<20), cfg.MaxSizePerMsg)
	require.Equal(t, 256, cfg.MaxInflightMsgs)
	require.True(t, cfg.CheckQuorum)
	require.Equal(t, myraft.ReadOnlyLeaseBased, cfg.ReadOnlyOption)
}

func TestDefaultRaftConfigPreservesExplicitValues(t *testing.T) {
	cfg := defaultRaftConfig(myraft.Config{
		ElectionTick:    21,
		HeartbeatTick:   4,
		MaxSizePerMsg:   4096,
		MaxInflightMsgs: 32,
	}, 99)
	require.Equal(t, uint64(99), cfg.ID)
	require.Equal(t, 21, cfg.ElectionTick)
	require.Equal(t, 4, cfg.HeartbeatTick)
	require.Equal(t, uint64(4096), cfg.MaxSizePerMsg)
	require.Equal(t, 32, cfg.MaxInflightMsgs)
	require.True(t, cfg.CheckQuorum)
	require.Equal(t, myraft.ReadOnlyLeaseBased, cfg.ReadOnlyOption)
}

func TestDefaultPeerBuilderRequiresLocalPeer(t *testing.T) {
	builder := defaultPeerBuilder(Storage{}, nil, 1, myraft.Config{}, nil)
	_, err := builder(localmeta.RegionMeta{
		ID:    7,
		Peers: []metaregion.Peer{{StoreID: 2, PeerID: 22}},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing peer")
}

func TestDefaultPeerBuilderRequiresSnapshotStorage(t *testing.T) {
	builder := defaultPeerBuilder(Storage{
		MVCC: fakeBuilderMVCCStore{},
		Raft: fakeBuilderRaftLog{},
	}, nil, 1, myraft.Config{}, nil)
	_, err := builder(localmeta.RegionMeta{
		ID:    7,
		Peers: []metaregion.Peer{{StoreID: 1, PeerID: 11}},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "snapshot storage")
}

func TestDefaultPeerBuilderEnablesFastLeaseRead(t *testing.T) {
	builder := defaultPeerBuilder(Storage{
		MVCC:     fakeBuilderMVCCStore{},
		Raft:     fakeBuilderRaftLog{},
		Snapshot: fakeBuilderSnapshotStore{},
	}, nil, 1, myraft.Config{}, nil)
	cfg, err := builder(localmeta.RegionMeta{
		ID:    7,
		Peers: []metaregion.Peer{{StoreID: 1, PeerID: 11}},
	})
	require.NoError(t, err)
	require.True(t, cfg.FastLeaseRead)
	require.True(t, cfg.RaftConfig.CheckQuorum)
	require.Equal(t, myraft.ReadOnlyLeaseBased, cfg.RaftConfig.ReadOnlyOption)
}
