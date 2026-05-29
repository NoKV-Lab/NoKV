// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package kv_test

import (
	"context"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"testing"
	"time"

	local "github.com/feichai0017/NoKV/local"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/command"
	"github.com/feichai0017/NoKV/raftstore/kv"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"github.com/feichai0017/NoKV/raftstore/peer"
	"github.com/feichai0017/NoKV/raftstore/raftlog"
	raftstorestats "github.com/feichai0017/NoKV/raftstore/stats"
	"github.com/feichai0017/NoKV/raftstore/store"
	"github.com/feichai0017/NoKV/txn/mvcc"
	entrykv "github.com/feichai0017/NoKV/txn/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type noopTransport struct{}

func (noopTransport) Send(context.Context, myraft.Message) {}

func openTestDB(t *testing.T) (*local.DB, *localmeta.Store) {
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

func mustPeerStorage(t *testing.T, db *local.DB, localMeta *localmeta.Store, groupID uint64) raftlog.PeerStorage {
	t.Helper()
	storage, err := raftlog.NewDBLog(db).Open(groupID, localMeta)
	require.NoError(t, err)
	return storage
}

func applyVersionedEntryForServiceTest(t *testing.T, db *local.DB, cf entrykv.ColumnFamily, key []byte, version uint64, value []byte, meta byte) {
	t.Helper()
	entry := entrykv.NewInternalEntry(cf, key, version, entrykv.SafeCopy(nil, value), meta, 0)
	defer entry.DecrRef()
	require.NoError(t, db.ApplyInternalEntries([]*entrykv.Entry{entry}))
}

func applyToDB(db *local.DB) peer.ApplyFunc {
	return func(entries []myraft.Entry) error {
		for _, entry := range entries {
			if entry.Type != myraft.EntryNormal || len(entry.Data) == 0 {
				continue
			}
			req, ok, err := command.Decode(entry.Data)
			if err != nil {
				return err
			}
			if !ok {
				return status.Error(codes.InvalidArgument, "unsupported unframed raft payload")
			}
			if _, err := kv.Apply(db, nil, req); err != nil {
				return err
			}
		}
		return nil
	}
}

type harnessConfig struct {
	storeID        uint64
	peerID         uint64
	regionID       uint64
	startKey       []byte
	endKey         []byte
	epochVersion   uint64
	epochConfVer   uint64
	campaignLeader bool
}

type serviceHarness struct {
	db      *local.DB
	store   *store.Store
	service *kv.Service
	ctx     *kvrpcpb.Context
	region  *localmeta.RegionMeta
}

func newServiceHarness(t *testing.T, cfg harnessConfig) serviceHarness {
	t.Helper()
	if cfg.storeID == 0 {
		cfg.storeID = 1
	}
	if cfg.peerID == 0 {
		cfg.peerID = 1
	}
	if cfg.regionID == 0 {
		cfg.regionID = 100
	}
	if cfg.startKey == nil {
		cfg.startKey = []byte("a")
	}
	if cfg.endKey == nil {
		cfg.endKey = []byte("z")
	}
	if cfg.epochVersion == 0 {
		cfg.epochVersion = 1
	}
	if cfg.epochConfVer == 0 {
		cfg.epochConfVer = 1
	}

	db, localMeta := openTestDB(t)
	applier := kv.NewApplier(db, nil)
	st := store.NewStore(store.Config{StoreID: cfg.storeID, CommandApplier: applier})
	t.Cleanup(func() { st.Close() })

	meta := &localmeta.RegionMeta{
		ID:       cfg.regionID,
		StartKey: append([]byte(nil), cfg.startKey...),
		EndKey:   append([]byte(nil), cfg.endKey...),
		Epoch:    metaregion.Epoch{Version: cfg.epochVersion, ConfVersion: cfg.epochConfVer},
		Peers:    []metaregion.Peer{{StoreID: cfg.storeID, PeerID: cfg.peerID}},
	}
	cfgPeer := &peer.Config{
		RaftConfig: myraft.Config{
			ID:              cfg.peerID,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
			PreVote:         true,
		},
		Transport: noopTransport{},
		Storage:   mustPeerStorage(t, db, localMeta, cfg.regionID),
		GroupID:   cfg.regionID,
		Region:    meta,
		Apply:     applyToDB(db),
	}
	p, err := st.StartPeer(cfgPeer, []myraft.Peer{{ID: cfg.peerID}})
	require.NoError(t, err)
	t.Cleanup(func() { st.StopPeer(p.ID()) })
	if cfg.campaignLeader {
		require.NoError(t, p.Campaign())
	}

	service := kv.NewService(st)
	ctx := &kvrpcpb.Context{
		RegionId:    meta.ID,
		RegionEpoch: &metapb.RegionEpoch{Version: cfg.epochVersion, ConfVersion: cfg.epochConfVer},
		Peer:        &metapb.RegionPeer{StoreId: cfg.storeID, PeerId: cfg.peerID},
	}
	return serviceHarness{
		db:      db,
		store:   st,
		service: service,
		ctx:     ctx,
		region:  meta,
	}
}

func prewriteKey(t *testing.T, service *kv.Service, ctx *kvrpcpb.Context, key, value []byte, startVersion uint64) {
	t.Helper()
	req := &kvrpcpb.KvPrewriteRequest{
		Context: ctx,
		Request: &kvrpcpb.PrewriteRequest{
			Mutations: []*kvrpcpb.Mutation{{
				Op:    kvrpcpb.Mutation_Put,
				Key:   key,
				Value: value,
			}},
			PrimaryLock:  key,
			StartVersion: startVersion,
			LockTtl:      3000,
		},
	}
	resp, err := service.Prewrite(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.GetRegionError())
	require.NotNil(t, resp.GetResponse())
	require.Empty(t, resp.GetResponse().GetErrors())
}

func commitKey(t *testing.T, service *kv.Service, ctx *kvrpcpb.Context, key []byte, startVersion, commitVersion uint64) {
	t.Helper()
	req := &kvrpcpb.KvCommitRequest{
		Context: ctx,
		Request: &kvrpcpb.CommitRequest{
			Keys:          [][]byte{entrykv.SafeCopy(nil, key)},
			StartVersion:  startVersion,
			CommitVersion: commitVersion,
		},
	}
	resp, err := service.Commit(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.GetRegionError())
	require.NotNil(t, resp.GetResponse())
	require.Nil(t, resp.GetResponse().GetError())
}

func TestServicePrewriteCommit(t *testing.T) {
	db, localMeta := openTestDB(t)
	applier := kv.NewApplier(db, nil)
	st := store.NewStore(store.Config{StoreID: 1, CommandApplier: applier})
	t.Cleanup(func() { st.Close() })

	region := &localmeta.RegionMeta{
		ID:       501,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
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
		Storage:   mustPeerStorage(t, db, localMeta, 501),
		GroupID:   501,
		Region:    region,
		Apply:     applyToDB(db),
	}
	peer, err := st.StartPeer(cfg, []myraft.Peer{{ID: 11}})
	require.NoError(t, err)
	t.Cleanup(func() { st.StopPeer(peer.ID()) })
	require.NoError(t, peer.Campaign())

	service := kv.NewService(st)
	ctx := &kvrpcpb.Context{RegionId: region.ID, RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVersion: 1}}
	prewriteReq := &kvrpcpb.KvPrewriteRequest{
		Context: ctx,
		Request: &kvrpcpb.PrewriteRequest{
			Mutations: []*kvrpcpb.Mutation{{
				Op:    kvrpcpb.Mutation_Put,
				Key:   []byte("sk"),
				Value: []byte("sv"),
			}},
			PrimaryLock:  []byte("sk"),
			StartVersion: 7,
			LockTtl:      1000,
		},
	}
	preResp, err := service.Prewrite(context.Background(), prewriteReq)
	require.NoError(t, err)
	require.Nil(t, preResp.GetRegionError())
	require.Empty(t, preResp.GetResponse().GetErrors())

	commitReq := &kvrpcpb.KvCommitRequest{Context: ctx, Request: &kvrpcpb.CommitRequest{
		Keys:          [][]byte{[]byte("sk")},
		StartVersion:  7,
		CommitVersion: 9,
	}}
	commitResp, err := service.Commit(context.Background(), commitReq)
	require.NoError(t, err)
	require.Nil(t, commitResp.GetRegionError())
	require.Nil(t, commitResp.GetResponse().GetError())
}

func TestServiceBatchGetEmpty(t *testing.T) {
	h := newServiceHarness(t, harnessConfig{campaignLeader: true})
	resp, err := h.service.BatchGet(context.Background(), &kvrpcpb.KvBatchGetRequest{
		Context: h.ctx,
		Request: &kvrpcpb.BatchGetRequest{},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.GetResponse())
	require.Len(t, resp.GetResponse().GetResponses(), 0)
}

func TestServiceInvalidContext(t *testing.T) {
	h := newServiceHarness(t, harnessConfig{campaignLeader: true})
	_, err := h.service.Get(context.Background(), &kvrpcpb.KvGetRequest{})
	require.Error(t, err)

	_, err = h.service.Scan(context.Background(), &kvrpcpb.KvScanRequest{})
	require.Error(t, err)
}

func TestServiceResolveAndCheckStatus(t *testing.T) {
	h := newServiceHarness(t, harnessConfig{campaignLeader: true})

	key := []byte("lock-key")
	prewriteKey(t, h.service, h.ctx, key, []byte("value"), 10)

	heartBeatResp, err := h.service.TxnHeartBeat(context.Background(), &kvrpcpb.KvTxnHeartBeatRequest{
		Context: h.ctx,
		Request: &kvrpcpb.TxnHeartBeatRequest{
			PrimaryKey:   key,
			StartVersion: 10,
			TtlExtension: 5000,
			CurrentTime:  0,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, heartBeatResp)
	require.Nil(t, heartBeatResp.GetRegionError())
	require.Equal(t, kvrpcpb.TxnHeartBeatAction_TxnHeartBeatTTLExtended, heartBeatResp.GetResponse().GetAction())

	resp, err := h.service.CheckTxnStatus(context.Background(), &kvrpcpb.KvCheckTxnStatusRequest{
		Context: h.ctx,
		Request: &kvrpcpb.CheckTxnStatusRequest{
			PrimaryKey:         key,
			LockTs:             10,
			CurrentTs:          20,
			CallerStartTs:      20,
			RollbackIfNotExist: true,
			CurrentTime:        1,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)

	resolveResp, err := h.service.ResolveLock(context.Background(), &kvrpcpb.KvResolveLockRequest{
		Context: h.ctx,
		Request: &kvrpcpb.ResolveLockRequest{
			StartVersion:  10,
			CommitVersion: 0,
			Keys:          [][]byte{entrykv.SafeCopy(nil, key)},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resolveResp)

	rollbackResp, err := h.service.BatchRollback(context.Background(), &kvrpcpb.KvBatchRollbackRequest{
		Context: h.ctx,
		Request: &kvrpcpb.BatchRollbackRequest{
			Keys:         [][]byte{entrykv.SafeCopy(nil, key)},
			StartVersion: 10,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, rollbackResp)
}

func TestServiceCheckTxnStatusUsesServiceTimeWhenCurrentTimeZero(t *testing.T) {
	h := newServiceHarness(t, harnessConfig{campaignLeader: true})
	key := []byte("short-ttl-lock")
	startTs := uint64(40)
	prewriteResp, err := h.service.Prewrite(context.Background(), &kvrpcpb.KvPrewriteRequest{
		Context: h.ctx,
		Request: &kvrpcpb.PrewriteRequest{
			Mutations: []*kvrpcpb.Mutation{{
				Op:    kvrpcpb.Mutation_Put,
				Key:   key,
				Value: []byte("value"),
			}},
			PrimaryLock:  key,
			StartVersion: startTs,
			LockTtl:      1,
		},
	})
	require.NoError(t, err)
	require.Nil(t, prewriteResp.GetRegionError())
	require.Empty(t, prewriteResp.GetResponse().GetErrors())

	time.Sleep(20 * time.Millisecond)
	statusResp, err := h.service.CheckTxnStatus(context.Background(), &kvrpcpb.KvCheckTxnStatusRequest{
		Context: h.ctx,
		Request: &kvrpcpb.CheckTxnStatusRequest{
			PrimaryKey:         key,
			LockTs:             startTs,
			CurrentTs:          startTs + 10,
			CallerStartTs:      startTs + 10,
			RollbackIfNotExist: true,
			CurrentTime:        0,
		},
	})
	require.NoError(t, err)
	require.Nil(t, statusResp.GetRegionError())
	require.Equal(t, kvrpcpb.CheckTxnStatusAction_CheckTxnStatusTTLExpireRollback, statusResp.GetResponse().GetAction())

	// CurrentTime=0 is filled by the StoreKV service, so a read-side resolver can
	// physically expire the lock before the original writer reaches Commit.
	commitResp, err := h.service.Commit(context.Background(), &kvrpcpb.KvCommitRequest{
		Context: h.ctx,
		Request: &kvrpcpb.CommitRequest{
			Keys:          [][]byte{entrykv.SafeCopy(nil, key)},
			StartVersion:  startTs,
			CommitVersion: startTs + 1,
		},
	})
	require.NoError(t, err)
	require.Nil(t, commitResp.GetRegionError())
	require.Contains(t, commitResp.GetResponse().GetError().GetRetryable(), "transaction already rolled back")
}

func TestServiceRegionEpochMismatch(t *testing.T) {
	db, localMeta := openTestDB(t)
	applier := kv.NewApplier(db, nil)
	st := store.NewStore(store.Config{StoreID: 2, CommandApplier: applier})
	t.Cleanup(func() { st.Close() })
	region := &localmeta.RegionMeta{
		ID:       601,
		StartKey: []byte("a"),
		EndKey:   []byte("b"),
		Epoch:    metaregion.Epoch{Version: 2, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 2, PeerID: 22}},
	}
	cfg := &peer.Config{
		RaftConfig: myraft.Config{
			ID:              22,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
		},
		Transport: noopTransport{},
		Storage:   mustPeerStorage(t, db, localMeta, 601),
		GroupID:   601,
		Region:    region,
		Apply:     applyToDB(db),
	}
	peer, err := st.StartPeer(cfg, []myraft.Peer{{ID: 22}})
	require.NoError(t, err)
	t.Cleanup(func() { st.StopPeer(peer.ID()) })
	require.NoError(t, peer.Campaign())

	service := kv.NewService(st)
	badCtx := &kvrpcpb.Context{RegionId: region.ID, RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVersion: 1}}
	resp, err := service.Commit(context.Background(), &kvrpcpb.KvCommitRequest{Context: badCtx, Request: &kvrpcpb.CommitRequest{StartVersion: 1}})
	require.NoError(t, err)
	require.NotNil(t, resp.GetRegionError())
	require.NotNil(t, resp.GetRegionError().GetEpochNotMatch())
}

func TestServiceKvGetLeaderRead(t *testing.T) {
	env := newServiceHarness(t, harnessConfig{
		storeID:        3,
		peerID:         33,
		regionID:       701,
		campaignLeader: true,
	})

	key := []byte("alpha")
	value := []byte("value")
	expiresAt := uint64(time.Now().Add(time.Hour).Unix())
	prewriteReq := &kvrpcpb.KvPrewriteRequest{
		Context: env.ctx,
		Request: &kvrpcpb.PrewriteRequest{
			Mutations: []*kvrpcpb.Mutation{{
				Op:        kvrpcpb.Mutation_Put,
				Key:       key,
				Value:     value,
				ExpiresAt: expiresAt,
			}},
			PrimaryLock:  key,
			StartVersion: 42,
			LockTtl:      3000,
		},
	}
	preResp, err := env.service.Prewrite(context.Background(), prewriteReq)
	require.NoError(t, err)
	require.NotNil(t, preResp)
	require.Nil(t, preResp.GetRegionError())
	require.NotNil(t, preResp.GetResponse())
	require.Empty(t, preResp.GetResponse().GetErrors())

	commitReq := &kvrpcpb.KvCommitRequest{
		Context: env.ctx,
		Request: &kvrpcpb.CommitRequest{
			Keys:          [][]byte{key},
			StartVersion:  42,
			CommitVersion: 46,
		},
	}
	commitResp, err := env.service.Commit(context.Background(), commitReq)
	require.NoError(t, err)
	require.NotNil(t, commitResp)
	require.Nil(t, commitResp.GetRegionError())
	require.NotNil(t, commitResp.GetResponse())
	require.Nil(t, commitResp.GetResponse().GetError())

	readReq := &kvrpcpb.KvGetRequest{
		Context: env.ctx,
		Request: &kvrpcpb.GetRequest{
			Key:     key,
			Version: 50,
		},
	}
	readResp, err := env.service.Get(context.Background(), readReq)
	require.NoError(t, err)
	require.NotNil(t, readResp)
	require.Nil(t, readResp.GetRegionError())
	require.NotNil(t, readResp.GetResponse())
	require.False(t, readResp.GetResponse().GetNotFound())
	require.Nil(t, readResp.GetResponse().GetError())
	require.Equal(t, value, readResp.GetResponse().GetValue())
	require.Equal(t, expiresAt, readResp.GetResponse().GetExpiresAt())
}

func TestServiceKvGetLeaderReadExpiredValue(t *testing.T) {
	env := newServiceHarness(t, harnessConfig{
		storeID:        3,
		peerID:         33,
		regionID:       702,
		campaignLeader: true,
	})

	key := []byte("expired")
	value := []byte("value")
	expiredAt := uint64(time.Now().Add(-time.Second).Unix())
	prewriteReq := &kvrpcpb.KvPrewriteRequest{
		Context: env.ctx,
		Request: &kvrpcpb.PrewriteRequest{
			Mutations: []*kvrpcpb.Mutation{{
				Op:        kvrpcpb.Mutation_Put,
				Key:       key,
				Value:     value,
				ExpiresAt: expiredAt,
			}},
			PrimaryLock:  key,
			StartVersion: 52,
			LockTtl:      3000,
		},
	}
	preResp, err := env.service.Prewrite(context.Background(), prewriteReq)
	require.NoError(t, err)
	require.Nil(t, preResp.GetRegionError())
	require.Empty(t, preResp.GetResponse().GetErrors())

	commitResp, err := env.service.Commit(context.Background(), &kvrpcpb.KvCommitRequest{
		Context: env.ctx,
		Request: &kvrpcpb.CommitRequest{
			Keys:          [][]byte{key},
			StartVersion:  52,
			CommitVersion: 56,
		},
	})
	require.NoError(t, err)
	require.Nil(t, commitResp.GetRegionError())
	require.Nil(t, commitResp.GetResponse().GetError())

	readResp, err := env.service.Get(context.Background(), &kvrpcpb.KvGetRequest{
		Context: env.ctx,
		Request: &kvrpcpb.GetRequest{
			Key:     key,
			Version: 60,
		},
	})
	require.NoError(t, err)
	require.Nil(t, readResp.GetRegionError())
	require.NotNil(t, readResp.GetResponse())
	require.True(t, readResp.GetResponse().GetNotFound())
}

func TestServiceKvGetNotLeader(t *testing.T) {
	env := newServiceHarness(t, harnessConfig{
		storeID:        4,
		peerID:         44,
		regionID:       801,
		campaignLeader: false,
	})

	req := &kvrpcpb.KvGetRequest{
		Context: env.ctx,
		Request: &kvrpcpb.GetRequest{
			Key:     []byte("missing"),
			Version: 1,
		},
	}
	resp, err := env.service.Get(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.GetRegionError())
	notLeader := resp.GetRegionError().GetNotLeader()
	require.NotNil(t, notLeader)
	require.Equal(t, env.region.ID, notLeader.GetRegionId())
	require.Nil(t, resp.GetResponse())
}

func TestServiceKvScanLeaderRead(t *testing.T) {
	env := newServiceHarness(t, harnessConfig{
		storeID:        5,
		peerID:         55,
		regionID:       901,
		campaignLeader: true,
	})

	prewriteKey(t, env.service, env.ctx, []byte("ak"), []byte("value-a"), 100)
	commitKey(t, env.service, env.ctx, []byte("ak"), 100, 150)

	prewriteKey(t, env.service, env.ctx, []byte("bk"), []byte("value-b"), 200)
	commitKey(t, env.service, env.ctx, []byte("bk"), 200, 250)

	resp, err := env.service.Scan(context.Background(), &kvrpcpb.KvScanRequest{
		Context: env.ctx,
		Request: &kvrpcpb.ScanRequest{
			StartKey:     []byte("a"),
			Limit:        4,
			Version:      500,
			IncludeStart: true,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.GetRegionError())
	require.NotNil(t, resp.GetResponse())
	kvs := resp.GetResponse().GetKvs()
	require.Len(t, kvs, 2)
	require.Equal(t, []byte("ak"), kvs[0].GetKey())
	require.Equal(t, []byte("value-a"), kvs[0].GetValue())
	require.Equal(t, []byte("bk"), kvs[1].GetKey())
	require.Equal(t, []byte("value-b"), kvs[1].GetValue())

	next, err := env.service.Scan(context.Background(), &kvrpcpb.KvScanRequest{
		Context: env.ctx,
		Request: &kvrpcpb.ScanRequest{
			StartKey:     []byte("ak"),
			Limit:        1,
			Version:      500,
			IncludeStart: false,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, next)
	require.Nil(t, next.GetRegionError())
	require.NotNil(t, next.GetResponse())
	require.Len(t, next.GetResponse().GetKvs(), 1)
	require.Equal(t, []byte("bk"), next.GetResponse().GetKvs()[0].GetKey())
	require.Equal(t, []byte("value-b"), next.GetResponse().GetKvs()[0].GetValue())
}

func TestServiceKvScanRespectsRegionEnd(t *testing.T) {
	env := newServiceHarness(t, harnessConfig{
		storeID:        7,
		peerID:         77,
		regionID:       1101,
		startKey:       []byte("a"),
		endKey:         []byte("m"),
		campaignLeader: true,
	})

	prewriteKey(t, env.service, env.ctx, []byte("ak"), []byte("value-a"), 100)
	commitKey(t, env.service, env.ctx, []byte("ak"), 100, 120)

	prewriteKey(t, env.service, env.ctx, []byte("bk"), []byte("value-b"), 200)
	commitKey(t, env.service, env.ctx, []byte("bk"), 200, 220)

	startTs := uint64(10)
	commitTs := uint64(20)
	applyVersionedEntryForServiceTest(t, env.db, entrykv.CFDefault, []byte("mz"), startTs, []byte("value-z"), 0)
	write := mvcc.EncodeWrite(mvcc.Write{Kind: kvrpcpb.Mutation_Put, StartTs: startTs})
	applyVersionedEntryForServiceTest(t, env.db, entrykv.CFWrite, []byte("mz"), commitTs, write, 0)

	resp, err := env.service.Scan(context.Background(), &kvrpcpb.KvScanRequest{
		Context: env.ctx,
		Request: &kvrpcpb.ScanRequest{
			StartKey:     []byte("a"),
			Limit:        10,
			Version:      500,
			IncludeStart: true,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.GetRegionError())
	require.NotNil(t, resp.GetResponse())
	for _, kv := range resp.GetResponse().GetKvs() {
		require.Less(t, string(kv.GetKey()), "m")
	}
}

func TestServiceBatchGetEdgeCases(t *testing.T) {
	env := newServiceHarness(t, harnessConfig{
		storeID:        6,
		peerID:         66,
		regionID:       1001,
		campaignLeader: true,
	})

	_, err := env.service.BatchGet(context.Background(), &kvrpcpb.KvBatchGetRequest{})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = env.service.BatchGet(context.Background(), &kvrpcpb.KvBatchGetRequest{Context: env.ctx})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	resp, err := env.service.BatchGet(context.Background(), &kvrpcpb.KvBatchGetRequest{
		Context: env.ctx,
		Request: &kvrpcpb.BatchGetRequest{},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.GetResponse())
	require.Empty(t, resp.GetResponse().GetResponses())

	prewriteKey(t, env.service, env.ctx, []byte("aa"), []byte("value-a"), 200)
	commitKey(t, env.service, env.ctx, []byte("aa"), 200, 250)

	prewriteKey(t, env.service, env.ctx, []byte("bb"), []byte("value-b"), 300)
	commitKey(t, env.service, env.ctx, []byte("bb"), 300, 350)

	batchResp, err := env.service.BatchGet(context.Background(), &kvrpcpb.KvBatchGetRequest{
		Context: env.ctx,
		Request: &kvrpcpb.BatchGetRequest{Requests: []*kvrpcpb.GetRequest{
			{Key: []byte("aa"), Version: 260},
			nil,
			{Key: []byte("missing"), Version: 260},
		}},
	})
	require.NoError(t, err)
	require.NotNil(t, batchResp)
	require.Nil(t, batchResp.GetRegionError())
	require.NotNil(t, batchResp.GetResponse())
	require.Len(t, batchResp.GetResponse().GetResponses(), 2)
}

func TestServiceHeaderAndScanErrors(t *testing.T) {
	env := newServiceHarness(t, harnessConfig{
		storeID:        7,
		peerID:         77,
		regionID:       1101,
		campaignLeader: true,
	})

	_, err := env.service.Get(context.Background(), &kvrpcpb.KvGetRequest{})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = env.service.Get(context.Background(), &kvrpcpb.KvGetRequest{
		Context: &kvrpcpb.Context{RegionId: 0},
		Request: &kvrpcpb.GetRequest{Key: []byte("k"), Version: 1},
	})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = env.service.Scan(context.Background(), &kvrpcpb.KvScanRequest{
		Context: env.ctx,
		Request: &kvrpcpb.ScanRequest{
			StartKey: []byte("a"),
			Limit:    1,
			Reverse:  true,
		},
	})
	require.Error(t, err)
	require.Equal(t, codes.Unimplemented, status.Code(err))
}

func TestServiceKvGetStoreNotMatch(t *testing.T) {
	env := newServiceHarness(t, harnessConfig{
		storeID:        7,
		peerID:         77,
		regionID:       1102,
		campaignLeader: true,
	})

	ctx := proto.Clone(env.ctx).(*kvrpcpb.Context)
	ctx.Peer = &metapb.RegionPeer{StoreId: 999, PeerId: env.ctx.GetPeer().GetPeerId()}

	resp, err := env.service.Get(context.Background(), &kvrpcpb.KvGetRequest{
		Context: ctx,
		Request: &kvrpcpb.GetRequest{Key: []byte("ak"), Version: 1},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.GetRegionError())
	require.NotNil(t, resp.GetRegionError().GetStoreNotMatch())
	require.Equal(t, uint64(999), resp.GetRegionError().GetStoreNotMatch().GetRequestStoreId())
	require.Equal(t, uint64(7), resp.GetRegionError().GetStoreNotMatch().GetActualStoreId())
}

func TestServiceKvGetKeyNotInRegion(t *testing.T) {
	env := newServiceHarness(t, harnessConfig{
		storeID:        8,
		peerID:         88,
		regionID:       1201,
		startKey:       []byte("a"),
		endKey:         []byte("m"),
		campaignLeader: true,
	})

	resp, err := env.service.Get(context.Background(), &kvrpcpb.KvGetRequest{
		Context: env.ctx,
		Request: &kvrpcpb.GetRequest{Key: []byte("z"), Version: 1},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.GetRegionError())
	require.NotNil(t, resp.GetRegionError().GetKeyNotInRegion())
	require.Equal(t, []byte("z"), resp.GetRegionError().GetKeyNotInRegion().GetKey())
	require.Equal(t, env.region.ID, resp.GetRegionError().GetKeyNotInRegion().GetRegionId())
}

func TestServiceKvScanNotLeader(t *testing.T) {
	env := newServiceHarness(t, harnessConfig{
		storeID:        6,
		peerID:         66,
		regionID:       1001,
		campaignLeader: false,
	})

	resp, err := env.service.Scan(context.Background(), &kvrpcpb.KvScanRequest{
		Context: env.ctx,
		Request: &kvrpcpb.ScanRequest{
			StartKey: []byte("a"),
			Limit:    1,
			Version:  10,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.GetRegionError())
	require.NotNil(t, resp.GetRegionError().GetNotLeader())
	require.Nil(t, resp.GetResponse())
}
