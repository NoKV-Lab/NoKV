// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"bytes"
	"context"
	"testing"
	"time"

	coordclient "github.com/feichai0017/NoKV/coordinator/client"
	"github.com/feichai0017/NoKV/coordinator/storecontrol"
	"github.com/feichai0017/NoKV/fsmeta"
	fsmetaexec "github.com/feichai0017/NoKV/fsmeta/exec"
	"github.com/feichai0017/NoKV/fsmeta/model"
	fsmetaraftstore "github.com/feichai0017/NoKV/fsmeta/runtime/raftstore"
	workdirmode "github.com/feichai0017/NoKV/local/workdir"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/feichai0017/NoKV/raftstore/client"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"github.com/feichai0017/NoKV/raftstore/migrate"
	"github.com/feichai0017/NoKV/raftstore/testcluster"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func openRealClusterExecutor(t *testing.T, ctx context.Context) *fsmetaexec.Executor {
	t.Helper()
	return openRealClusterRuntime(t, ctx).executor
}

type realClusterRuntime struct {
	executor      *fsmetaexec.Executor
	node          *testcluster.Node
	mountIdentity model.MountIdentity
}

func openRealClusterRuntime(t *testing.T, ctx context.Context) *realClusterRuntime {
	t.Helper()
	return openRealClusterRuntimeWithOptions(t, ctx)
}

func openRealClusterRuntimeWithOptions(t *testing.T, ctx context.Context, opts ...fsmetaexec.Option) *realClusterRuntime {
	t.Helper()
	coord := testcluster.StartCoordinator(t)
	t.Cleanup(func() { coord.Close(t) })

	seedDir := t.TempDir()
	standalone := testcluster.OpenStandaloneDB(t, seedDir, nil)
	require.NoError(t, standalone.Close())

	const (
		storeID  = uint64(1)
		regionID = uint64(171)
		peerID   = uint64(101)
	)
	coord.JoinStore(t, storeID)
	_, err := migrate.Init(migrate.InitConfig{
		WorkDir:  seedDir,
		StoreID:  storeID,
		RegionID: regionID,
		PeerID:   peerID,
	})
	require.NoError(t, err)

	node := testcluster.StartNodeWithConfig(t, storeID, seedDir, testcluster.NodeConfig{
		AllowedModes:      []workdirmode.Mode{workdirmode.ModeSeeded, workdirmode.ModeCluster},
		StartPeers:        true,
		Scheduler:         testcluster.NewScheduler(t, coord.Addr(), 100*time.Millisecond),
		HeartbeatInterval: 50 * time.Millisecond,
	})
	t.Cleanup(func() { node.Close(t) })

	testcluster.WaitForLeaderPeer(t, ctx, node.Addr(), regionID, peerID)
	testcluster.WaitForSchedulerMode(t, node, storecontrol.ModeHealthy, false)

	coordRPC, err := coordclient.NewGRPCClient(ctx, coord.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = coordRPC.Close() })
	registerMount(t, ctx, coordRPC, "vol")
	kv, err := client.New(client.Config{
		StoreResolver:  coordRPC,
		RegionResolver: coordRPC,
		DialOptions:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = kv.Close() })

	runner, err := fsmetaraftstore.NewRunner(kv, coordRPC)
	require.NoError(t, err)
	seedRootInode(t, ctx, runner, model.MountIdentity{MountID: "vol", MountKeyID: 1})
	inodes, err := fsmetaraftstore.NewShardAffineInodeAllocator(coordRPC, 4)
	require.NoError(t, err)
	executorOpts := []fsmetaexec.Option{
		fsmetaexec.WithInodeAllocator(inodes),
		fsmetaexec.WithMountResolver(testMountResolver{coord: coordRPC}),
	}
	executorOpts = append(executorOpts, opts...)
	executor, err := fsmetaexec.New(runner, executorOpts...)
	require.NoError(t, err)
	return &realClusterRuntime{
		executor:      executor,
		node:          node,
		mountIdentity: model.MountIdentity{MountID: "vol", MountKeyID: 1},
	}
}

func openSplitRealClusterExecutor(t *testing.T, ctx context.Context) *fsmetaexec.Executor {
	t.Helper()
	return openSplitRealClusterExecutorWithOptions(t, ctx)
}

func openSplitRealClusterExecutorWithOptions(t *testing.T, ctx context.Context, opts ...fsmetaexec.Option) *fsmetaexec.Executor {
	t.Helper()

	coord := testcluster.StartCoordinator(t)
	t.Cleanup(func() { coord.Close(t) })

	seedDir := t.TempDir()
	standalone := testcluster.OpenStandaloneDB(t, seedDir, nil)
	require.NoError(t, standalone.Close())

	const (
		storeID        = uint64(1)
		parentRegionID = uint64(131)
		childRegionID  = uint64(132)
		parentPeerID   = uint64(101)
		childPeerID    = uint64(102)
	)
	coord.JoinStore(t, storeID)
	_, err := migrate.Init(migrate.InitConfig{
		WorkDir:  seedDir,
		StoreID:  storeID,
		RegionID: parentRegionID,
		PeerID:   parentPeerID,
	})
	require.NoError(t, err)

	node := testcluster.StartNodeWithConfig(t, storeID, seedDir, testcluster.NodeConfig{
		AllowedModes:      []workdirmode.Mode{workdirmode.ModeSeeded, workdirmode.ModeCluster},
		StartPeers:        true,
		Scheduler:         testcluster.NewScheduler(t, coord.Addr(), 100*time.Millisecond),
		HeartbeatInterval: 50 * time.Millisecond,
	})
	t.Cleanup(func() { node.Close(t) })

	testcluster.WaitForLeaderPeer(t, ctx, node.Addr(), parentRegionID, parentPeerID)
	testcluster.WaitForSchedulerMode(t, node, storecontrol.ModeHealthy, false)

	splitKey, err := fsmeta.EncodeDentryKey(model.MountIdentity{MountID: "vol", MountKeyID: 1}, model.RootInode, "m")
	require.NoError(t, err)
	childMeta := localmeta.RegionMeta{
		ID:       childRegionID,
		StartKey: splitKey,
		EndKey:   nil,
		Epoch: metaregion.Epoch{
			Version:     1,
			ConfVersion: 1,
		},
		Peers: []metaregion.Peer{{StoreID: storeID, PeerID: childPeerID}},
	}
	require.NoError(t, node.Server.Store().ProposeSplit(parentRegionID, childMeta, childMeta.StartKey))
	require.Eventually(t, func() bool {
		status := testcluster.FetchRuntimeStatus(t, ctx, node.Addr(), childRegionID)
		return status.GetKnown() && status.GetHosted()
	}, 5*time.Second, 20*time.Millisecond)
	testcluster.WaitForLeaderPeer(t, ctx, node.Addr(), childRegionID, childPeerID)
	require.Eventually(t, func() bool {
		parent := testcluster.FetchRuntimeStatus(t, ctx, node.Addr(), parentRegionID)
		child := testcluster.FetchRuntimeStatus(t, ctx, node.Addr(), childRegionID)
		return bytes.Equal(parent.GetRegion().GetEndKey(), splitKey) &&
			bytes.Equal(child.GetRegion().GetStartKey(), splitKey)
	}, 5*time.Second, 20*time.Millisecond)

	parentStatus := testcluster.FetchRuntimeStatus(t, ctx, node.Addr(), parentRegionID)
	childStatus := testcluster.FetchRuntimeStatus(t, ctx, node.Addr(), childRegionID)
	coordRPC, err := coordclient.NewGRPCClient(ctx, coord.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = coordRPC.Close() })
	registerMount(t, ctx, coordRPC, "vol")
	kv, err := client.New(client.Config{
		StoreResolver: coordRPC,
		RegionResolver: &staticRegionResolver{regions: []*metapb.RegionDescriptor{
			parentStatus.GetRegion(),
			childStatus.GetRegion(),
		}},
		DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = kv.Close() })

	runner, err := fsmetaraftstore.NewRunner(kv, coordRPC)
	require.NoError(t, err)
	seedRootInode(t, ctx, runner, model.MountIdentity{MountID: "vol", MountKeyID: 1})
	inodes, err := fsmetaraftstore.NewShardAffineInodeAllocator(coordRPC, 4)
	require.NoError(t, err)
	executorOpts := []fsmetaexec.Option{
		fsmetaexec.WithInodeAllocator(inodes),
		fsmetaexec.WithMountResolver(testMountResolver{coord: coordRPC}),
	}
	executorOpts = append(executorOpts, opts...)
	executor, err := fsmetaexec.New(runner, executorOpts...)
	require.NoError(t, err)
	return executor
}

type testMountResolver struct {
	coord *coordclient.GRPCClient
}

func (r testMountResolver) ResolveMount(ctx context.Context, mount model.MountID) (fsmetaexec.MountAdmission, error) {
	resp, err := r.coord.GetMount(ctx, &coordpb.GetMountRequest{MountId: string(mount)})
	if err != nil {
		return fsmetaexec.MountAdmission{}, err
	}
	if resp == nil || resp.GetNotFound() || resp.GetMount() == nil {
		return fsmetaexec.MountAdmission{}, model.ErrMountNotRegistered
	}
	info := resp.GetMount()
	return fsmetaexec.MountAdmission{
		MountID:       model.MountID(info.GetMountId()),
		MountKeyID:    model.MountKeyID(info.GetMountKeyId()),
		RootInode:     model.InodeID(info.GetRootInode()),
		SchemaVersion: info.GetSchemaVersion(),
		Retired:       info.GetState() == coordpb.MountState_MOUNT_STATE_RETIRED,
	}, nil
}

func registerMount(t *testing.T, ctx context.Context, coord *coordclient.GRPCClient, mount model.MountID) {
	t.Helper()
	resp, err := coord.PublishRootEvent(ctx, &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(rootevent.MountRegistered(string(mount), 1, uint64(model.RootInode), 1)),
	})
	require.NoError(t, err)
	require.True(t, resp.GetAccepted())
}

func seedRootInode(t *testing.T, ctx context.Context, runner fsmetaexec.TxnRunner, mount model.MountIdentity) {
	t.Helper()
	key, err := fsmeta.EncodeInodeKey(mount, model.RootInode)
	require.NoError(t, err)
	readVersion, err := runner.ReserveTimestamp(ctx, 1)
	require.NoError(t, err)
	_, ok, err := runner.Get(ctx, key, readVersion)
	require.NoError(t, err)
	if ok {
		return
	}
	now := time.Now().UnixNano()
	value, err := fsmeta.EncodeInodeValue(model.InodeRecord{
		Inode:         model.RootInode,
		Type:          model.InodeTypeDirectory,
		Mode:          0755,
		LinkCount:     1,
		CreatedUnixNs: now,
		UpdatedUnixNs: now,
	})
	require.NoError(t, err)
	startVersion, err := runner.ReserveTimestamp(ctx, 2)
	require.NoError(t, err)
	_, err = runner.Mutate(ctx, key, []*kvrpcpb.Mutation{{
		Op:                kvrpcpb.Mutation_Put,
		Key:               key,
		Value:             value,
		AssertionNotExist: true,
	}}, startVersion, startVersion+1, uint64(30*time.Second/time.Millisecond))
	require.NoError(t, err)
}

type staticRegionResolver struct {
	regions []*metapb.RegionDescriptor
}

func (r *staticRegionResolver) GetRegionByKey(_ context.Context, req *coordpb.GetRegionByKeyRequest) (*coordpb.GetRegionByKeyResponse, error) {
	for _, region := range r.regions {
		if containsRegionKey(region, req.GetKey()) {
			return &coordpb.GetRegionByKeyResponse{
				RegionDescriptor: metawire.DescriptorToProto(metawire.DescriptorFromProto(region)),
			}, nil
		}
	}
	return &coordpb.GetRegionByKeyResponse{NotFound: true}, nil
}

func (r *staticRegionResolver) Close() error { return nil }

func containsRegionKey(region *metapb.RegionDescriptor, key []byte) bool {
	if region == nil {
		return false
	}
	if len(region.GetStartKey()) > 0 && bytes.Compare(key, region.GetStartKey()) < 0 {
		return false
	}
	if len(region.GetEndKey()) > 0 && bytes.Compare(key, region.GetEndKey()) >= 0 {
		return false
	}
	return true
}
