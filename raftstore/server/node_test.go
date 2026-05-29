// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package server_test

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/coordinator/storecontrol"
	local "github.com/feichai0017/NoKV/local"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	adminpb "github.com/feichai0017/NoKV/pb/admin"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/client"
	"github.com/feichai0017/NoKV/raftstore/kv"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	storemvcc "github.com/feichai0017/NoKV/raftstore/mvcc"
	"github.com/feichai0017/NoKV/raftstore/peer"
	"github.com/feichai0017/NoKV/raftstore/raftlog"
	serverpkg "github.com/feichai0017/NoKV/raftstore/server"
	raftstorestats "github.com/feichai0017/NoKV/raftstore/stats"
	storepkg "github.com/feichai0017/NoKV/raftstore/store"
	txnmvcc "github.com/feichai0017/NoKV/txn/mvcc"
	entrykv "github.com/feichai0017/NoKV/txn/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestNodeStartsKVService(t *testing.T) {
	db, _ := openTestDB(t)
	node, err := serverpkg.NewNode(serverpkg.Config{
		Storage: serverpkg.Storage{
			MVCC: db,
			Raft: raftlog.NewDBLog(db),
		},
		Store: storepkg.Config{
			StoreID: 1,
		},
		TransportAddr: "127.0.0.1:0",
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = node.Close() })

	require.NotEmpty(t, node.Addr())
	require.NotNil(t, node.Store())
	require.NotNil(t, node.Transport())

	conn, err := grpc.NewClient(node.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	client := kvrpcpb.NewStoreKVClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = client.Get(ctx, &kvrpcpb.KvGetRequest{})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.InvalidArgument, st.Code())
}

func TestNodePreservesConfiguredAdvertiseAddrs(t *testing.T) {
	db, _ := openTestDB(t)
	scheduler := &captureScheduler{}
	node, err := serverpkg.NewNode(serverpkg.Config{
		Storage: serverpkg.Storage{
			MVCC: db,
			Raft: raftlog.NewDBLog(db),
		},
		Store: storepkg.Config{
			StoreID:           1,
			ClientAddr:        "store-1.example:20160",
			RaftAddr:          "store-1-raft.example:20160",
			Scheduler:         scheduler,
			HeartbeatInterval: 10 * time.Millisecond,
		},
		TransportAddr: "127.0.0.1:0",
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = node.Close() })

	require.Eventually(t, func() bool {
		stats, ok := scheduler.latestStore()
		return ok &&
			stats.ClientAddr == "store-1.example:20160" &&
			stats.RaftAddr == "store-1-raft.example:20160"
	}, time.Second, 10*time.Millisecond)
}

func TestNodeAutoStartsMVCCMaintenanceWorker(t *testing.T) {
	db, localMeta := openTestDB(t)
	key := []byte("auto-gc-key")
	applyServerMVCCPutVersion(t, db, key, 150, 140, "new")
	applyServerMVCCPutVersion(t, db, key, 90, 80, "anchor")
	applyServerMVCCPutVersion(t, db, key, 40, 30, "old")

	node, err := serverpkg.NewNode(serverpkg.Config{
		Storage: serverpkg.Storage{
			MVCC: db,
			Raft: raftlog.NewDBLog(db),
		},
		Store: storepkg.Config{
			StoreID:   1,
			LocalMeta: localMeta,
		},
		Raft: myraft.Config{
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
			PreVote:         true,
		},
		TransportAddr: "127.0.0.1:0",
		MVCCMaintenance: serverpkg.MVCCMaintenanceConfig{
			Interval: 10 * time.Millisecond,
			SafePoint: func() uint64 {
				return 100
			},
			Apply: storemvcc.ApplyOptions{BatchEntries: 8},
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = node.Close() })

	region := localmeta.RegionMeta{
		ID:       9,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 901}},
	}
	startRegionPeer(t, testNode{
		storeID:   1,
		peerID:    901,
		region:    region,
		db:        db,
		localMeta: localMeta,
		node:      node,
	})

	require.Eventually(t, func() bool {
		snap := node.MVCCMaintenanceSnapshot()
		return snap.Enabled && snap.Runs > 0
	}, 2*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		droppedWrite, err := db.GetInternalEntry(entrykv.CFWrite, key, 40)
		if err != nil {
			return false
		}
		defer droppedWrite.DecrRef()
		return droppedWrite.Meta&entrykv.BitDelete != 0
	}, 2*time.Second, 10*time.Millisecond)
}

func TestNodeDerivesSnapshotStorageFromMVCC(t *testing.T) {
	node, err := serverpkg.NewNode(serverpkg.Config{
		Storage: serverpkg.Storage{
			MVCC: fakeMVCCStore{},
			Raft: fakeRaftLog{},
		},
		Store: storepkg.Config{
			StoreID: 1,
		},
		TransportAddr: "127.0.0.1:0",
	})
	require.NoError(t, err)
	require.NotNil(t, node)
	require.NoError(t, node.Close())
}

func applyServerMVCCPutVersion(t *testing.T, db *local.DB, key []byte, commitTs, startTs uint64, value string) {
	t.Helper()
	defaultEntry := entrykv.NewInternalEntry(entrykv.CFDefault, key, startTs, []byte(value), 0, 0)
	defer defaultEntry.DecrRef()
	write := txnmvcc.EncodeWrite(txnmvcc.Write{Kind: kvrpcpb.Mutation_Put, StartTs: startTs})
	writeEntry := entrykv.NewInternalEntry(entrykv.CFWrite, key, commitTs, write, 0, 0)
	defer writeEntry.DecrRef()
	require.NoError(t, db.ApplyInternalEntries([]*entrykv.Entry{defaultEntry, writeEntry}))
}

type captureScheduler struct {
	mu    sync.Mutex
	stats storecontrol.StoreStats
	seen  bool
}

func (s *captureScheduler) ReportRegionHeartbeat(context.Context, uint64) {}

func (s *captureScheduler) PublishRootEvent(context.Context, rootevent.Event) error {
	return nil
}

func (s *captureScheduler) StoreHeartbeat(_ context.Context, stats storecontrol.StoreStats) []storecontrol.Operation {
	s.mu.Lock()
	s.stats = stats
	s.seen = true
	s.mu.Unlock()
	return nil
}

func (s *captureScheduler) Status() storecontrol.Status {
	return storecontrol.Status{}
}

func (s *captureScheduler) Close() error {
	return nil
}

func (s *captureScheduler) latestStore() (storecontrol.StoreStats, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats, s.seen
}

func TestNodeStartsRaftAdminService(t *testing.T) {
	db, localMeta := openTestDB(t)
	node, err := serverpkg.NewNode(serverpkg.Config{
		Storage: serverpkg.Storage{
			MVCC: db,
			Raft: raftlog.NewDBLog(db),
		},
		Store: storepkg.Config{
			StoreID:   1,
			LocalMeta: localMeta,
		},
		Raft: myraft.Config{
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
			PreVote:         true,
		},
		TransportAddr: "127.0.0.1:0",
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = node.Close() })

	region := localmeta.RegionMeta{
		ID:       7,
		StartKey: []byte("a"),
		EndKey:   nil,
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 101}},
	}
	startRegionPeer(t, testNode{
		storeID:   1,
		peerID:    101,
		region:    region,
		db:        db,
		localMeta: localMeta,
		node:      node,
	})

	conn, err := grpc.NewClient(node.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	adminClient := adminpb.NewRaftAdminClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	statusResp, err := adminClient.RegionRuntimeStatus(ctx, &adminpb.RegionRuntimeStatusRequest{RegionId: region.ID})
	require.NoError(t, err)
	require.True(t, statusResp.GetKnown())
	require.True(t, statusResp.GetHosted())
	require.True(t, statusResp.GetLeader())
	require.Equal(t, uint64(101), statusResp.GetLocalPeerId())
	require.GreaterOrEqual(t, statusResp.GetAppliedIndex(), uint64(0))

	addResp, err := adminClient.AddPeer(ctx, &adminpb.AddPeerRequest{
		RegionId: region.ID,
		StoreId:  2,
		PeerId:   202,
	})
	require.NoError(t, err)
	require.Len(t, addResp.GetRegion().GetPeers(), 2)

	statusResp, err = adminClient.RegionRuntimeStatus(ctx, &adminpb.RegionRuntimeStatusRequest{RegionId: region.ID})
	require.NoError(t, err)
	require.True(t, statusResp.GetKnown())
	require.Len(t, statusResp.GetRegion().GetPeers(), 2)

	_, err = adminClient.TransferLeader(ctx, &adminpb.TransferLeaderRequest{
		RegionId: region.ID,
		PeerId:   101,
	})
	require.NoError(t, err)

	_, err = adminClient.RemovePeer(ctx, &adminpb.RemovePeerRequest{
		RegionId: region.ID,
		PeerId:   202,
	})
	require.NoError(t, err)
}

func TestNodeWithClientTwoPhaseCommit(t *testing.T) {
	nodes := []testNode{
		{
			storeID: 1,
			peerID:  101,
			region: localmeta.RegionMeta{
				ID:       1,
				StartKey: []byte("a"),
				EndKey:   []byte("m"),
				Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
				Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 101}},
			},
		},
		{
			storeID: 2,
			peerID:  201,
			region: localmeta.RegionMeta{
				ID:       2,
				StartKey: []byte("m"),
				EndKey:   nil,
				Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
				Peers:    []metaregion.Peer{{StoreID: 2, PeerID: 201}},
			},
		},
	}

	for i := range nodes {
		workDir := t.TempDir()
		opt := local.NewDefaultOptions()
		opt.WorkDir = workDir
		localMeta, err := localmeta.OpenLocalStore(workDir, nil)
		require.NoError(t, err)
		opt.ControlLogPointerSnapshot = raftstorestats.ControlLogPointers(localMeta.DurableRaftPointerSnapshot)
		db, err := local.Open(opt)
		require.NoError(t, err)
		nodes[i].db = db
		nodes[i].localMeta = localMeta

		node, err := serverpkg.NewNode(serverpkg.Config{
			Storage: serverpkg.Storage{
				MVCC: db,
				Raft: raftlog.NewDBLog(db),
			},
			Store: storepkg.Config{
				StoreID:   nodes[i].storeID,
				LocalMeta: localMeta,
			},
			Raft: myraft.Config{
				ElectionTick:    5,
				HeartbeatTick:   1,
				MaxSizePerMsg:   1 << 20,
				MaxInflightMsgs: 256,
				PreVote:         true,
			},
			TransportAddr: "127.0.0.1:0",
		})
		require.NoError(t, err)
		nodes[i].node = node
		nodes[i].addr = node.Addr()
	}

	defer func() {
		for i := range nodes {
			if nodes[i].node != nil {
				_ = nodes[i].node.Close()
			}
			if nodes[i].db != nil {
				_ = nodes[i].db.Close()
			}
			if nodes[i].localMeta != nil {
				_ = nodes[i].localMeta.Close()
			}
		}
	}()

	for i := range nodes {
		startRegionPeer(t, nodes[i])
	}

	stores := make([]testStoreEndpoint, 0, len(nodes))
	regions := make([]*metapb.RegionDescriptor, 0, len(nodes))
	for _, n := range nodes {
		stores = append(stores, testStoreEndpoint{StoreID: n.storeID, Addr: n.addr})
		regions = append(regions, regionMetaToPB(n.region))
	}
	sort.Slice(regions, func(i, j int) bool { return regions[i].GetRegionId() < regions[j].GetRegionId() })

	cli, err := client.New(client.Config{
		StoreResolver:  staticStoreResolver(stores),
		RegionResolver: &staticRegionResolver{regions: regions},
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)
	defer func() { _ = cli.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mutations := []*kvrpcpb.Mutation{
		{Op: kvrpcpb.Mutation_Put, Key: []byte("alfa"), Value: []byte("value-a")},
		{Op: kvrpcpb.Mutation_Put, Key: []byte("zoo"), Value: []byte("value-z")},
	}
	err = cli.Mutate(ctx, []byte("alfa"), mutations, 100, 150, 3000)
	require.NoError(t, err)

	resp, err := cli.Get(context.Background(), []byte("alfa"), 200)
	require.NoError(t, err)
	require.False(t, resp.GetNotFound())
	require.Equal(t, []byte("value-a"), resp.GetValue())

	resp2, err := cli.Get(context.Background(), []byte("zoo"), 200)
	require.NoError(t, err)
	require.False(t, resp2.GetNotFound())
	require.Equal(t, []byte("value-z"), resp2.GetValue())

	kvs, err := cli.Scan(context.Background(), []byte("a"), 4, 200)
	require.NoError(t, err)
	require.Len(t, kvs, 2)

	err = cli.Delete(context.Background(), []byte("alfa"), 300, 350, 3000)
	require.NoError(t, err)
	delResp, err := cli.Get(context.Background(), []byte("alfa"), 400)
	require.NoError(t, err)
	require.True(t, delResp.GetNotFound())
}

type fakeMVCCStore struct{}

func (fakeMVCCStore) ApplyInternalEntries(entries []*entrykv.Entry) error { return nil }
func (fakeMVCCStore) GetInternalEntry(cf entrykv.ColumnFamily, key []byte, version uint64) (*entrykv.Entry, error) {
	return nil, fmt.Errorf("not implemented")
}
func (fakeMVCCStore) NewInternalIterator(opt *entrykv.Options) entrykv.Iterator { return nil }

type fakeRaftLog struct{}

func (fakeRaftLog) Open(groupID uint64, meta *localmeta.Store) (raftlog.PeerStorage, error) {
	return nil, nil
}

func openTestDB(t *testing.T) (*local.DB, *localmeta.Store) {
	t.Helper()
	opt := local.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	localMeta, err := localmeta.OpenLocalStore(opt.WorkDir, nil)
	require.NoError(t, err)
	opt.ControlLogPointerSnapshot = raftstorestats.ControlLogPointers(localMeta.DurableRaftPointerSnapshot)
	db, err := local.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	t.Cleanup(func() { _ = localMeta.Close() })
	return db, localMeta
}

type testNode struct {
	storeID   uint64
	peerID    uint64
	region    localmeta.RegionMeta
	db        *local.DB
	localMeta *localmeta.Store
	node      *serverpkg.Node
	addr      string
}

type staticRegionResolver struct {
	regions []*metapb.RegionDescriptor
}

type testStoreEndpoint struct {
	StoreID uint64
	Addr    string
}

type staticStoreResolver []testStoreEndpoint

func (r staticStoreResolver) GetStore(_ context.Context, req *coordpb.GetStoreRequest) (*coordpb.GetStoreResponse, error) {
	for _, endpoint := range r {
		if endpoint.StoreID == req.GetStoreId() {
			return &coordpb.GetStoreResponse{
				Store: &coordpb.StoreInfo{
					StoreId:    endpoint.StoreID,
					ClientAddr: endpoint.Addr,
					State:      coordpb.StoreState_STORE_STATE_UP,
				},
			}, nil
		}
	}
	return &coordpb.GetStoreResponse{NotFound: true}, nil
}

func (r *staticRegionResolver) GetRegionByKey(_ context.Context, req *coordpb.GetRegionByKeyRequest) (*coordpb.GetRegionByKeyResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("resolver: nil request")
	}
	for _, region := range r.regions {
		if regionContainsKey(region, req.GetKey()) {
			return &coordpb.GetRegionByKeyResponse{
				RegionDescriptor: metawire.DescriptorToProto(metawire.DescriptorFromProto(cloneRegionMetaPB(region))),
			}, nil
		}
	}
	return &coordpb.GetRegionByKeyResponse{NotFound: true}, nil
}

func (r *staticRegionResolver) Close() error { return nil }

func startRegionPeer(t *testing.T, n testNode) {
	store := n.node.Store()
	peerStorage, err := raftlog.NewDBLog(n.db).Open(n.region.ID, n.localMeta)
	require.NoError(t, err)
	cfg := &peer.Config{
		RaftConfig: myraft.Config{
			ID:              n.peerID,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
			PreVote:         true,
		},
		Transport: n.node.Transport(),
		Apply:     kv.NewEntryApplier(n.db),
		Storage:   peerStorage,
		GroupID:   n.region.ID,
		Region:    localmeta.CloneRegionMetaPtr(&n.region),
	}
	bootstrap := []myraft.Peer{{ID: n.peerID}}
	p, err := store.StartPeer(cfg, bootstrap)
	require.NoError(t, err)
	require.NoError(t, p.Campaign())
	require.Eventually(t, func() bool {
		status := p.Status()
		return status.RaftState == myraft.StateLeader
	}, time.Second, 10*time.Millisecond)
}

func regionMetaToPB(meta localmeta.RegionMeta) *metapb.RegionDescriptor {
	peers := make([]*metapb.RegionPeer, 0, len(meta.Peers))
	for _, p := range meta.Peers {
		peers = append(peers, &metapb.RegionPeer{StoreId: p.StoreID, PeerId: p.PeerID})
	}
	return &metapb.RegionDescriptor{
		RegionId: meta.ID,
		StartKey: append([]byte(nil), meta.StartKey...),
		EndKey:   append([]byte(nil), meta.EndKey...),
		Epoch:    &metapb.RegionEpoch{Version: meta.Epoch.Version, ConfVersion: meta.Epoch.ConfVersion},
		Peers:    peers,
	}
}

func cloneRegionMetaPB(meta *metapb.RegionDescriptor) *metapb.RegionDescriptor {
	if meta == nil {
		return nil
	}
	return proto.Clone(meta).(*metapb.RegionDescriptor)
}

func regionContainsKey(meta *metapb.RegionDescriptor, key []byte) bool {
	if meta == nil {
		return false
	}
	start := meta.GetStartKey()
	end := meta.GetEndKey()
	if len(start) > 0 && bytesCompare(start, key) > 0 {
		return false
	}
	return len(end) == 0 || bytesCompare(end, key) > 0
}

func bytesCompare(a, b []byte) int {
	minLen := min(len(b), len(a))
	for i := range minLen {
		if a[i] == b[i] {
			continue
		}
		if a[i] < b[i] {
			return -1
		}
		return 1
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}
