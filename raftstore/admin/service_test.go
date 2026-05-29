// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package admin

import (
	"bytes"
	"context"

	nokverrors "github.com/feichai0017/NoKV/errors"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	adminpb "github.com/feichai0017/NoKV/pb/admin"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"

	"io"
	"sync"
	"testing"

	"github.com/feichai0017/NoKV/coordinator/storecontrol"
	entrykv "github.com/feichai0017/NoKV/engine/kv"
	local "github.com/feichai0017/NoKV/local"
	myraft "github.com/feichai0017/NoKV/raft"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"github.com/feichai0017/NoKV/raftstore/peer"
	"github.com/feichai0017/NoKV/raftstore/raftlog"
	snapshotpkg "github.com/feichai0017/NoKV/raftstore/snapshot/sst"
	raftstorestats "github.com/feichai0017/NoKV/raftstore/stats"
	"github.com/feichai0017/NoKV/raftstore/store"
	"github.com/feichai0017/NoKV/utils"
	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type noopTransport struct{}

type noopSnapshotStore struct{}

func (noopTransport) Send(context.Context, myraft.Message) {}

func (noopSnapshotStore) ExportSnapshot(localmeta.RegionMeta) ([]byte, error) {
	return nil, nil
}

func (noopSnapshotStore) ImportSnapshot([]byte) (*snapshotpkg.ImportResult, error) {
	return nil, nil
}

func (noopSnapshotStore) ExportSnapshotTo(io.Writer, localmeta.RegionMeta) (snapshotpkg.Meta, error) {
	return snapshotpkg.Meta{}, nil
}

func (noopSnapshotStore) ImportSnapshotFrom(io.Reader) (*snapshotpkg.ImportResult, error) {
	return nil, nil
}

type captureSchedulerClient struct {
	mu     sync.Mutex
	events []rootevent.Event
}

func (c *captureSchedulerClient) ReportRegionHeartbeat(context.Context, uint64) {}

func (c *captureSchedulerClient) PublishRootEvent(_ context.Context, event rootevent.Event) error {
	c.mu.Lock()
	c.events = append(c.events, rootevent.CloneEvent(event))
	c.mu.Unlock()
	return nil
}

func (c *captureSchedulerClient) StoreHeartbeat(context.Context, storecontrol.StoreStats) []storecontrol.Operation {
	return nil
}

func (c *captureSchedulerClient) Status() storecontrol.Status { return storecontrol.Status{} }

func (c *captureSchedulerClient) Close() error { return nil }

func (c *captureSchedulerClient) RootEvents() []rootevent.Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]rootevent.Event, 0, len(c.events))
	for _, event := range c.events {
		out = append(out, rootevent.CloneEvent(event))
	}
	return out
}

func (c *captureSchedulerClient) Reset() {
	c.mu.Lock()
	c.events = nil
	c.mu.Unlock()
}

func openAdminTestDBWithTweak(t *testing.T, dir string, tweak func(*local.Options)) (*local.DB, *localmeta.Store) {
	t.Helper()
	localMeta, err := localmeta.OpenLocalStore(dir, nil)
	require.NoError(t, err)
	opt := local.NewDefaultOptions()
	opt.WorkDir = dir
	opt.ControlLogPointerSnapshot = raftstorestats.ControlLogPointers(localMeta.DurableRaftPointerSnapshot)
	if tweak != nil {
		tweak(opt)
	}
	db, err := local.Open(opt)
	require.NoError(t, err)
	return db, localMeta
}

func testSSTExport(db *local.DB) peer.SnapshotExportFunc {
	return func(region localmeta.RegionMeta) ([]byte, error) {
		return snapshotpkg.NewDBStore(db).ExportSnapshot(region)
	}
}

func testSSTApply(db *local.DB) peer.SnapshotApplyFunc {
	return func(payload []byte) (localmeta.RegionMeta, error) {
		result, err := snapshotpkg.NewDBStore(db).ImportSnapshot(payload)
		if err != nil {
			return localmeta.RegionMeta{}, err
		}
		return result.Meta.Region, nil
	}
}

func TestServiceAddPeerPublishesPlannedTarget(t *testing.T) {
	dir := t.TempDir()
	db, localMeta := openAdminTestDBWithTweak(t, dir, nil)
	defer func() {
		require.NoError(t, db.Close())
		require.NoError(t, localMeta.Close())
	}()

	sink := &captureSchedulerClient{}
	st := store.NewStore(store.Config{
		StoreID:     1,
		LocalMeta:   localMeta,
		Scheduler:   sink,
		PeerBuilder: nil,
	})
	defer st.Close()

	region := localmeta.RegionMeta{
		ID:       88,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 101}},
		State:    metaregion.ReplicaStateRunning,
	}
	storage, err := raftlog.NewDBLog(db).Open(region.ID, localMeta)
	require.NoError(t, err)
	cfg := &peer.Config{
		RaftConfig: myraft.Config{
			ID:              101,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
			PreVote:         true,
		},
		Transport: noopTransport{},
		Apply:     func([]myraft.Entry) error { return nil },
		Storage:   storage,
		GroupID:   region.ID,
		Region:    localmeta.CloneRegionMetaPtr(&region),
	}
	p, err := st.StartPeer(cfg, []myraft.Peer{{ID: 101}})
	require.NoError(t, err)
	defer st.StopPeer(p.ID())
	require.NoError(t, p.Campaign())
	sink.Reset()

	svc := NewService(st)
	_, err = svc.AddPeer(context.Background(), &adminpb.AddPeerRequest{
		RegionId: region.ID,
		StoreId:  2,
		PeerId:   201,
	})
	require.NoError(t, err)

	events := sink.RootEvents()
	require.NotEmpty(t, events)
	require.Equal(t, rootevent.KindPeerAdditionPlanned, events[0].Kind)
	require.NotNil(t, events[0].PeerChange)
	require.Equal(t, uint64(2), events[0].PeerChange.StoreID)
	require.Equal(t, uint64(201), events[0].PeerChange.PeerID)
}

func TestServiceExecutionStatusReportsProtocolState(t *testing.T) {
	dir := t.TempDir()
	db, localMeta := openAdminTestDBWithTweak(t, dir, nil)
	defer func() {
		require.NoError(t, db.Close())
		require.NoError(t, localMeta.Close())
	}()

	sink := &captureSchedulerClient{}
	st := store.NewStore(store.Config{
		StoreID:     1,
		LocalMeta:   localMeta,
		Scheduler:   sink,
		PeerBuilder: nil,
	})
	defer st.Close()

	region := localmeta.RegionMeta{
		ID:       88,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 101}},
		State:    metaregion.ReplicaStateRunning,
	}
	storage, err := raftlog.NewDBLog(db).Open(region.ID, localMeta)
	require.NoError(t, err)
	cfg := &peer.Config{
		RaftConfig: myraft.Config{
			ID:              101,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
			PreVote:         true,
		},
		Transport: noopTransport{},
		Apply:     func([]myraft.Entry) error { return nil },
		Storage:   storage,
		GroupID:   region.ID,
		Region:    localmeta.CloneRegionMetaPtr(&region),
	}
	p, err := st.StartPeer(cfg, []myraft.Peer{{ID: 101}})
	require.NoError(t, err)
	defer st.StopPeer(p.ID())
	require.NoError(t, p.Campaign())
	require.NoError(t, localMeta.SaveRaftPointer(localmeta.RaftLogPointer{
		GroupID:      region.ID,
		AppliedIndex: 1,
		AppliedTerm:  1,
	}))

	require.NoError(t, localMeta.SaveRegion(localmeta.RegionMeta{
		ID:       99,
		StartKey: []byte("za"),
		EndKey:   []byte("zz"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 199}},
		State:    metaregion.ReplicaStateRunning,
	}))

	svc := NewService(st)
	addResp, err := svc.AddPeer(context.Background(), &adminpb.AddPeerRequest{
		RegionId: region.ID,
		StoreId:  2,
		PeerId:   201,
	})
	require.NoError(t, err)
	require.NotNil(t, addResp.GetRegion())

	readResp, err := st.ReadCommand(context.Background(), &raftcmdpb.RaftCmdRequest{
		Header: &raftcmdpb.CmdHeader{
			RegionId: region.ID,
			RegionEpoch: &metapb.RegionEpoch{
				Version:     addResp.GetRegion().GetEpoch().GetVersion(),
				ConfVersion: addResp.GetRegion().GetEpoch().GetConfVersion(),
			},
		},
		Requests: []*raftcmdpb.Request{{
			CmdType: raftcmdpb.CmdType_CMD_GET,
			Cmd: &raftcmdpb.Request_Get{
				Get: &kvrpcpb.GetRequest{Key: []byte("zz")},
			},
		}},
	})
	require.NoError(t, err)
	require.NotNil(t, readResp.GetRegionError())

	resp, err := svc.ExecutionStatus(context.Background(), &adminpb.ExecutionStatusRequest{})
	require.NoError(t, err)
	require.NotNil(t, resp.GetLastAdmission())
	require.True(t, resp.GetLastAdmission().GetObserved())
	require.Equal(t, adminpb.ExecutionAdmissionClass_EXECUTION_ADMISSION_CLASS_READ, resp.GetLastAdmission().GetClass())
	require.Equal(t, adminpb.ExecutionAdmissionReason_EXECUTION_ADMISSION_REASON_KEY_NOT_IN_REGION, resp.GetLastAdmission().GetReason())
	require.Equal(t, region.ID, resp.GetLastAdmission().GetRegionId())
	require.NotNil(t, resp.GetRestart())
	require.Equal(t, adminpb.ExecutionRestartState_EXECUTION_RESTART_STATE_DEGRADED, resp.GetRestart().GetState())
	require.Contains(t, resp.GetRestart().GetMissingRaftPointer(), uint64(99))
	require.Len(t, resp.GetTopology(), 1)
	require.Equal(t, region.ID, resp.GetTopology()[0].GetRegionId())
	require.Contains(t, []adminpb.ExecutionTopologyOutcome{
		adminpb.ExecutionTopologyOutcome_EXECUTION_TOPOLOGY_OUTCOME_PROPOSED,
		adminpb.ExecutionTopologyOutcome_EXECUTION_TOPOLOGY_OUTCOME_APPLIED,
	}, resp.GetTopology()[0].GetOutcome())
	require.Contains(t, []adminpb.ExecutionPublishState{
		adminpb.ExecutionPublishState_EXECUTION_PUBLISH_STATE_PLANNED_PUBLISHED,
		adminpb.ExecutionPublishState_EXECUTION_PUBLISH_STATE_TERMINAL_PENDING,
		adminpb.ExecutionPublishState_EXECUTION_PUBLISH_STATE_TERMINAL_PUBLISHED,
	}, resp.GetTopology()[0].GetPublish())
	require.NotEmpty(t, resp.GetTopology()[0].GetTransitionId())
}

func TestServiceValidationAndHelperMappings(t *testing.T) {
	var nilSvc *Service
	_, err := nilSvc.AddPeer(context.Background(), &adminpb.AddPeerRequest{})
	require.ErrorContains(t, err, "raft admin service not configured")
	requireAdminRPCError(t, err, codes.FailedPrecondition, nokverrors.KindProtocolViolation, reasonServiceNotConfigured)
	_, err = nilSvc.RemovePeer(context.Background(), &adminpb.RemovePeerRequest{})
	require.ErrorContains(t, err, "raft admin service not configured")
	requireAdminRPCError(t, err, codes.FailedPrecondition, nokverrors.KindProtocolViolation, reasonServiceNotConfigured)

	svc := &Service{store: new(store.Store)}
	_, err = svc.AddPeer(context.Background(), &adminpb.AddPeerRequest{})
	require.ErrorContains(t, err, "region_id, store_id, and peer_id are required")
	requireAdminRPCError(t, err, codes.InvalidArgument, nokverrors.KindInvalidArgument, reasonInvalidRequest)
	_, err = svc.RemovePeer(context.Background(), &adminpb.RemovePeerRequest{})
	require.ErrorContains(t, err, "region_id and peer_id are required")
	requireAdminRPCError(t, err, codes.InvalidArgument, nokverrors.KindInvalidArgument, reasonInvalidRequest)
	_, err = svc.TransferLeader(context.Background(), &adminpb.TransferLeaderRequest{})
	require.ErrorContains(t, err, "region_id and peer_id are required")
	requireAdminRPCError(t, err, codes.InvalidArgument, nokverrors.KindInvalidArgument, reasonInvalidRequest)

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = svc.TransferLeader(canceled, &adminpb.TransferLeaderRequest{RegionId: 1, PeerId: 2})
	require.ErrorContains(t, err, context.Canceled.Error())
	requireAdminRPCError(t, err, codes.Canceled, nokverrors.KindAborted, reasonCanceled)
	_, err = svc.RegionRuntimeStatus(canceled, &adminpb.RegionRuntimeStatusRequest{RegionId: 1})
	require.ErrorContains(t, err, context.Canceled.Error())
	requireAdminRPCError(t, err, codes.Canceled, nokverrors.KindAborted, reasonCanceled)
	_, err = svc.ExecutionStatus(canceled, &adminpb.ExecutionStatusRequest{})
	require.ErrorContains(t, err, context.Canceled.Error())
	requireAdminRPCError(t, err, codes.Canceled, nokverrors.KindAborted, reasonCanceled)

	_, err = svc.RegionRuntimeStatus(context.Background(), &adminpb.RegionRuntimeStatusRequest{})
	require.ErrorContains(t, err, "region_id is required")
	requireAdminRPCError(t, err, codes.InvalidArgument, nokverrors.KindInvalidArgument, reasonInvalidRequest)

	resp, err := svc.RegionRuntimeStatus(context.Background(), &adminpb.RegionRuntimeStatusRequest{RegionId: 99})
	require.NoError(t, err)
	require.False(t, resp.GetKnown())

	require.Equal(t, adminpb.ExecutionAdmissionClass_EXECUTION_ADMISSION_CLASS_UNSPECIFIED, executionAdmissionClassProto(store.AdmissionClassUnknown))
	require.Equal(t, adminpb.ExecutionAdmissionClass_EXECUTION_ADMISSION_CLASS_READ, executionAdmissionClassProto(store.AdmissionClassRead))
	require.Equal(t, adminpb.ExecutionAdmissionClass_EXECUTION_ADMISSION_CLASS_WRITE, executionAdmissionClassProto(store.AdmissionClassWrite))
	require.Equal(t, adminpb.ExecutionAdmissionClass_EXECUTION_ADMISSION_CLASS_TOPOLOGY, executionAdmissionClassProto(store.AdmissionClassTopology))

	reasons := map[store.AdmissionReason]adminpb.ExecutionAdmissionReason{
		store.AdmissionReasonUnknown:        adminpb.ExecutionAdmissionReason_EXECUTION_ADMISSION_REASON_UNSPECIFIED,
		store.AdmissionReasonAccepted:       adminpb.ExecutionAdmissionReason_EXECUTION_ADMISSION_REASON_ACCEPTED,
		store.AdmissionReasonInvalid:        adminpb.ExecutionAdmissionReason_EXECUTION_ADMISSION_REASON_INVALID,
		store.AdmissionReasonStoreNotMatch:  adminpb.ExecutionAdmissionReason_EXECUTION_ADMISSION_REASON_STORE_NOT_MATCH,
		store.AdmissionReasonNotHosted:      adminpb.ExecutionAdmissionReason_EXECUTION_ADMISSION_REASON_NOT_HOSTED,
		store.AdmissionReasonEpochMismatch:  adminpb.ExecutionAdmissionReason_EXECUTION_ADMISSION_REASON_EPOCH_MISMATCH,
		store.AdmissionReasonKeyNotInRegion: adminpb.ExecutionAdmissionReason_EXECUTION_ADMISSION_REASON_KEY_NOT_IN_REGION,
		store.AdmissionReasonNotLeader:      adminpb.ExecutionAdmissionReason_EXECUTION_ADMISSION_REASON_NOT_LEADER,
		store.AdmissionReasonCanceled:       adminpb.ExecutionAdmissionReason_EXECUTION_ADMISSION_REASON_CANCELED,
		store.AdmissionReasonTimedOut:       adminpb.ExecutionAdmissionReason_EXECUTION_ADMISSION_REASON_TIMED_OUT,
	}
	for reason, expected := range reasons {
		require.Equal(t, expected, executionAdmissionReasonProto(reason))
	}

	outcomes := map[store.ExecutionOutcome]adminpb.ExecutionTopologyOutcome{
		store.ExecutionOutcomeUnknown:  adminpb.ExecutionTopologyOutcome_EXECUTION_TOPOLOGY_OUTCOME_UNSPECIFIED,
		store.ExecutionOutcomeRejected: adminpb.ExecutionTopologyOutcome_EXECUTION_TOPOLOGY_OUTCOME_REJECTED,
		store.ExecutionOutcomeQueued:   adminpb.ExecutionTopologyOutcome_EXECUTION_TOPOLOGY_OUTCOME_QUEUED,
		store.ExecutionOutcomeProposed: adminpb.ExecutionTopologyOutcome_EXECUTION_TOPOLOGY_OUTCOME_PROPOSED,
		store.ExecutionOutcomeApplied:  adminpb.ExecutionTopologyOutcome_EXECUTION_TOPOLOGY_OUTCOME_APPLIED,
		store.ExecutionOutcomeFailed:   adminpb.ExecutionTopologyOutcome_EXECUTION_TOPOLOGY_OUTCOME_FAILED,
	}
	for outcome, expected := range outcomes {
		require.Equal(t, expected, executionTopologyOutcomeProto(outcome))
	}

	publishStates := map[store.PublishState]adminpb.ExecutionPublishState{
		store.PublishStateUnknown:           adminpb.ExecutionPublishState_EXECUTION_PUBLISH_STATE_UNSPECIFIED,
		store.PublishStateNotRequired:       adminpb.ExecutionPublishState_EXECUTION_PUBLISH_STATE_NOT_REQUIRED,
		store.PublishStatePlannedPublished:  adminpb.ExecutionPublishState_EXECUTION_PUBLISH_STATE_PLANNED_PUBLISHED,
		store.PublishStateTerminalPending:   adminpb.ExecutionPublishState_EXECUTION_PUBLISH_STATE_TERMINAL_PENDING,
		store.PublishStateTerminalPublished: adminpb.ExecutionPublishState_EXECUTION_PUBLISH_STATE_TERMINAL_PUBLISHED,
		store.PublishStateTerminalFailed:    adminpb.ExecutionPublishState_EXECUTION_PUBLISH_STATE_TERMINAL_FAILED,
		store.PublishStateTerminalBlocked:   adminpb.ExecutionPublishState_EXECUTION_PUBLISH_STATE_TERMINAL_BLOCKED,
	}
	for state, expected := range publishStates {
		require.Equal(t, expected, executionPublishStateProto(state))
	}

	require.Equal(t, adminpb.ExecutionRestartState_EXECUTION_RESTART_STATE_UNSPECIFIED, executionRestartStateProto(store.RestartStateUnknown))
	require.Equal(t, adminpb.ExecutionRestartState_EXECUTION_RESTART_STATE_READY, executionRestartStateProto(store.RestartStateReady))
	require.Equal(t, adminpb.ExecutionRestartState_EXECUTION_RESTART_STATE_DEGRADED, executionRestartStateProto(store.RestartStateDegraded))

	header := localmeta.RegionMeta{
		ID:       1,
		StartKey: []byte("a"),
		EndKey:   []byte("m"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 2},
		Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 101}},
	}
	payload := localmeta.CloneRegionMeta(header)
	require.True(t, matchesSnapshotRegion(header, payload))
	payload.Peers[0].PeerID = 202
	require.False(t, matchesSnapshotRegion(header, payload))
}

func requireAdminRPCError(t *testing.T, err error, code codes.Code, kind nokverrors.Kind, reason string) {
	t.Helper()
	require.Equal(t, code, status.Code(err))
	require.Equal(t, kind, nokverrors.KindOf(err))
	gotKind, metadata, ok := nokverrors.RPCErrorInfo(err)
	require.True(t, ok)
	require.Equal(t, kind, gotKind)
	require.Equal(t, reason, metadata[adminReasonMetadata])
}

func TestServiceExportsAndInstallsRegionSnapshot(t *testing.T) {
	sourceDir := t.TempDir()
	sourceDB, sourceMeta := openAdminTestDBWithTweak(t, sourceDir, func(opt *local.Options) {
	})
	defer func() {
		require.NoError(t, sourceDB.Close())
		require.NoError(t, sourceMeta.Close())
	}()

	valueBacked := bytes.Repeat([]byte("z"), 4096)
	entry := entrykv.NewInternalEntry(entrykv.CFDefault, []byte("alpha"), 9, valueBacked, 0, 0)
	defer entry.DecrRef()
	require.NoError(t, sourceDB.ApplyInternalEntries([]*entrykv.Entry{entry}))

	region := localmeta.RegionMeta{
		ID:       22,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers: []metaregion.Peer{
			{StoreID: 1, PeerID: 101},
			{StoreID: 2, PeerID: 201},
		},
		State: metaregion.ReplicaStateRunning,
	}
	require.NoError(t, sourceMeta.SaveRegion(region))

	sourceStorage, err := raftlog.NewDBLog(sourceDB).Open(region.ID, sourceMeta)
	require.NoError(t, err)
	require.NoError(t, sourceStorage.ApplySnapshot(myraft.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index: 1,
			Term:  1,
			ConfState: raftpb.ConfState{
				Voters: []uint64{101},
			},
		},
	}))

	sourceStore := store.NewStore(store.Config{
		StoreID:   1,
		LocalMeta: sourceMeta,
		PeerBuilder: func(meta localmeta.RegionMeta) (*peer.Config, error) {
			return &peer.Config{
				RaftConfig: myraft.Config{
					ID:              101,
					ElectionTick:    5,
					HeartbeatTick:   1,
					MaxSizePerMsg:   1 << 20,
					MaxInflightMsgs: 256,
					PreVote:         true,
				},
				Transport:      noopTransport{},
				Apply:          func([]myraft.Entry) error { return nil },
				SnapshotExport: testSSTExport(sourceDB),
				Storage:        sourceStorage,
				GroupID:        meta.ID,
				Region:         localmeta.CloneRegionMetaPtr(&meta),
			}, nil
		},
	})
	defer sourceStore.Close()

	sourcePeerCfg := &peer.Config{
		RaftConfig: myraft.Config{
			ID:              101,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
			PreVote:         true,
		},
		Transport:      noopTransport{},
		Apply:          func([]myraft.Entry) error { return nil },
		SnapshotExport: testSSTExport(sourceDB),
		Storage:        sourceStorage,
		GroupID:        region.ID,
		Region:         localmeta.CloneRegionMetaPtr(&region),
	}
	_, err = sourceStore.StartPeer(sourcePeerCfg, nil)
	require.NoError(t, err)

	sourcePeer, ok := sourceStore.Peer(101)
	require.True(t, ok)
	require.NoError(t, sourcePeer.Campaign())
	require.Eventually(t, func() bool {
		status, ok := sourceStore.RegionRuntimeStatus(region.ID)
		return ok && status.Leader
	}, 2_000_000_000, 20_000_000)

	targetDir := t.TempDir()
	targetDB, targetMeta := openAdminTestDBWithTweak(t, targetDir, func(opt *local.Options) {
	})
	defer func() {
		require.NoError(t, targetDB.Close())
		require.NoError(t, targetMeta.Close())
	}()
	targetStore := store.NewStore(store.Config{
		StoreID:   2,
		LocalMeta: targetMeta,
		PeerBuilder: func(meta localmeta.RegionMeta) (*peer.Config, error) {
			storage, err := raftlog.NewDBLog(targetDB).Open(meta.ID, targetMeta)
			require.NoError(t, err)
			return &peer.Config{
				RaftConfig: myraft.Config{
					ID:              201,
					ElectionTick:    5,
					HeartbeatTick:   1,
					MaxSizePerMsg:   1 << 20,
					MaxInflightMsgs: 256,
					PreVote:         true,
				},
				Transport:     noopTransport{},
				Apply:         func([]myraft.Entry) error { return nil },
				SnapshotApply: testSSTApply(targetDB),
				Storage:       storage,
				GroupID:       meta.ID,
				Region:        localmeta.CloneRegionMetaPtr(&meta),
			}, nil
		},
	})
	defer targetStore.Close()

	sourceSvc := NewServiceWithSnapshot(sourceStore, snapshotpkg.NewDBStore(sourceDB))
	targetSvc := NewServiceWithSnapshot(targetStore, snapshotpkg.NewDBStore(targetDB))

	exported, err := sourceSvc.ExportRegionSnapshot(context.Background(), &adminpb.ExportRegionSnapshotRequest{
		RegionId: region.ID,
	})
	require.NoError(t, err)
	require.NotEmpty(t, exported.GetSnapshot())

	installed, err := targetSvc.ImportRegionSnapshot(context.Background(), &adminpb.ImportRegionSnapshotRequest{
		Snapshot: exported.GetSnapshot(),
	})
	require.NoError(t, err)
	require.Equal(t, region.ID, installed.GetRegion().GetRegionId())

	status, ok := targetStore.RegionRuntimeStatus(region.ID)
	require.True(t, ok)
	require.True(t, status.Hosted)
	require.Equal(t, uint64(201), status.LocalPeerID)
	require.GreaterOrEqual(t, status.AppliedIndex, uint64(1))

	got, err := targetDB.GetInternalEntry(entrykv.CFDefault, []byte("alpha"), 9)
	require.NoError(t, err)
	require.NotNil(t, got)
	defer got.DecrRef()
	require.Equal(t, valueBacked, got.Value)
}

type exportRegionSnapshotStreamCapture struct {
	ctx    context.Context
	chunks []*adminpb.ExportRegionSnapshotStreamResponse
}

func (s *exportRegionSnapshotStreamCapture) Send(resp *adminpb.ExportRegionSnapshotStreamResponse) error {
	copyResp := &adminpb.ExportRegionSnapshotStreamResponse{
		SnapshotHeader: append([]byte(nil), resp.GetSnapshotHeader()...),
		Chunk:          append([]byte(nil), resp.GetChunk()...),
	}
	if resp.GetRegion() != nil {
		copyResp.Region = &metapb.RegionDescriptor{
			RegionId: resp.GetRegion().GetRegionId(),
			StartKey: append([]byte(nil), resp.GetRegion().GetStartKey()...),
			EndKey:   append([]byte(nil), resp.GetRegion().GetEndKey()...),
			Epoch:    &metapb.RegionEpoch{Version: resp.GetRegion().GetEpoch().GetVersion(), ConfVersion: resp.GetRegion().GetEpoch().GetConfVersion()},
			Peers:    append([]*metapb.RegionPeer(nil), resp.GetRegion().GetPeers()...),
		}
	}
	s.chunks = append(s.chunks, copyResp)
	return nil
}

func (s *exportRegionSnapshotStreamCapture) SetHeader(metadata.MD) error  { return nil }
func (s *exportRegionSnapshotStreamCapture) SendHeader(metadata.MD) error { return nil }
func (s *exportRegionSnapshotStreamCapture) SetTrailer(metadata.MD)       {}
func (s *exportRegionSnapshotStreamCapture) Context() context.Context     { return s.ctx }
func (s *exportRegionSnapshotStreamCapture) SendMsg(any) error            { return nil }
func (s *exportRegionSnapshotStreamCapture) RecvMsg(any) error            { return nil }

type importRegionSnapshotStreamFeed struct {
	ctx  context.Context
	reqs []*adminpb.ImportRegionSnapshotStreamRequest
	idx  int
	resp *adminpb.ImportRegionSnapshotResponse
}

func (s *importRegionSnapshotStreamFeed) Recv() (*adminpb.ImportRegionSnapshotStreamRequest, error) {
	if s.idx >= len(s.reqs) {
		return nil, io.EOF
	}
	req := s.reqs[s.idx]
	s.idx++
	return req, nil
}

func (s *importRegionSnapshotStreamFeed) SendAndClose(resp *adminpb.ImportRegionSnapshotResponse) error {
	s.resp = resp
	return nil
}

func (s *importRegionSnapshotStreamFeed) SetHeader(metadata.MD) error  { return nil }
func (s *importRegionSnapshotStreamFeed) SendHeader(metadata.MD) error { return nil }
func (s *importRegionSnapshotStreamFeed) SetTrailer(metadata.MD)       {}
func (s *importRegionSnapshotStreamFeed) Context() context.Context     { return s.ctx }
func (s *importRegionSnapshotStreamFeed) SendMsg(any) error            { return nil }
func (s *importRegionSnapshotStreamFeed) RecvMsg(any) error            { return nil }

func TestServiceImportRegionSnapshotStreamRejectsMissingHeader(t *testing.T) {
	svc := &Service{store: &store.Store{}, snapshot: noopSnapshotStore{}}
	stream := &importRegionSnapshotStreamFeed{
		ctx: context.Background(),
		reqs: []*adminpb.ImportRegionSnapshotStreamRequest{{
			Region: &metapb.RegionDescriptor{RegionId: 1},
			Chunk:  []byte("payload"),
		}},
	}
	err := svc.ImportRegionSnapshotStream(stream)
	require.Error(t, err)
	require.ErrorContains(t, err, "snapshot_header is required")
}

func TestServiceImportRegionSnapshotStreamRejectsMissingRegion(t *testing.T) {
	svc := &Service{store: &store.Store{}, snapshot: noopSnapshotStore{}}
	header, err := (&raftpb.Snapshot{}).Marshal()
	require.NoError(t, err)
	stream := &importRegionSnapshotStreamFeed{
		ctx: context.Background(),
		reqs: []*adminpb.ImportRegionSnapshotStreamRequest{{
			SnapshotHeader: header,
			Chunk:          []byte("payload"),
		}},
	}
	err = svc.ImportRegionSnapshotStream(stream)
	require.Error(t, err)
	require.ErrorContains(t, err, "region is required")
}

func TestServiceImportRegionSnapshotStreamRejectsRepeatedHeader(t *testing.T) {
	svc := &Service{store: &store.Store{}, snapshot: noopSnapshotStore{}}
	header, err := (&raftpb.Snapshot{}).Marshal()
	require.NoError(t, err)
	stream := &importRegionSnapshotStreamFeed{
		ctx: context.Background(),
		reqs: []*adminpb.ImportRegionSnapshotStreamRequest{
			{
				SnapshotHeader: header,
				Region: &metapb.RegionDescriptor{
					RegionId: 1,
					StartKey: []byte("a"),
					EndKey:   []byte("z"),
				},
			},
			{
				SnapshotHeader: header,
				Chunk:          []byte("payload"),
			},
		},
	}
	err = svc.ImportRegionSnapshotStream(stream)
	require.Error(t, err)
	require.ErrorContains(t, err, "snapshot header may only appear in the first chunk")
}

func TestServiceExportsAndImportsRegionSnapshotStream(t *testing.T) {
	sourceDir := t.TempDir()
	sourceDB, sourceMeta := openAdminTestDBWithTweak(t, sourceDir, func(opt *local.Options) {
	})
	defer func() {
		require.NoError(t, sourceDB.Close())
		require.NoError(t, sourceMeta.Close())
	}()

	valueBacked := bytes.Repeat([]byte("z"), 4096)
	entry := entrykv.NewInternalEntry(entrykv.CFDefault, []byte("alpha"), 9, valueBacked, 0, 0)
	defer entry.DecrRef()
	require.NoError(t, sourceDB.ApplyInternalEntries([]*entrykv.Entry{entry}))

	region := localmeta.RegionMeta{
		ID:       32,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers: []metaregion.Peer{
			{StoreID: 1, PeerID: 101},
			{StoreID: 2, PeerID: 201},
		},
		State: metaregion.ReplicaStateRunning,
	}
	require.NoError(t, sourceMeta.SaveRegion(region))

	sourceStorage, err := raftlog.NewDBLog(sourceDB).Open(region.ID, sourceMeta)
	require.NoError(t, err)
	require.NoError(t, sourceStorage.ApplySnapshot(myraft.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index: 1,
			Term:  1,
			ConfState: raftpb.ConfState{
				Voters: []uint64{101},
			},
		},
	}))

	sourceStore := store.NewStore(store.Config{
		StoreID:   1,
		LocalMeta: sourceMeta,
		PeerBuilder: func(meta localmeta.RegionMeta) (*peer.Config, error) {
			return &peer.Config{
				RaftConfig: myraft.Config{
					ID:              101,
					ElectionTick:    5,
					HeartbeatTick:   1,
					MaxSizePerMsg:   1 << 20,
					MaxInflightMsgs: 256,
					PreVote:         true,
				},
				Transport:      noopTransport{},
				Apply:          func([]myraft.Entry) error { return nil },
				SnapshotExport: testSSTExport(sourceDB),
				Storage:        sourceStorage,
				GroupID:        meta.ID,
				Region:         localmeta.CloneRegionMetaPtr(&meta),
			}, nil
		},
	})
	defer sourceStore.Close()

	sourcePeerCfg := &peer.Config{
		RaftConfig: myraft.Config{
			ID:              101,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
			PreVote:         true,
		},
		Transport:      noopTransport{},
		Apply:          func([]myraft.Entry) error { return nil },
		SnapshotExport: testSSTExport(sourceDB),
		Storage:        sourceStorage,
		GroupID:        region.ID,
		Region:         localmeta.CloneRegionMetaPtr(&region),
	}
	_, err = sourceStore.StartPeer(sourcePeerCfg, nil)
	require.NoError(t, err)

	sourcePeer, ok := sourceStore.Peer(101)
	require.True(t, ok)
	require.NoError(t, sourcePeer.Campaign())
	require.Eventually(t, func() bool {
		status, ok := sourceStore.RegionRuntimeStatus(region.ID)
		return ok && status.Leader
	}, 2_000_000_000, 20_000_000)

	targetDir := t.TempDir()
	targetDB, targetMeta := openAdminTestDBWithTweak(t, targetDir, func(opt *local.Options) {
	})
	defer func() {
		require.NoError(t, targetDB.Close())
		require.NoError(t, targetMeta.Close())
	}()
	targetStore := store.NewStore(store.Config{
		StoreID:   2,
		LocalMeta: targetMeta,
		PeerBuilder: func(meta localmeta.RegionMeta) (*peer.Config, error) {
			storage, err := raftlog.NewDBLog(targetDB).Open(meta.ID, targetMeta)
			require.NoError(t, err)
			return &peer.Config{
				RaftConfig: myraft.Config{
					ID:              201,
					ElectionTick:    5,
					HeartbeatTick:   1,
					MaxSizePerMsg:   1 << 20,
					MaxInflightMsgs: 256,
					PreVote:         true,
				},
				Transport:     noopTransport{},
				Apply:         func([]myraft.Entry) error { return nil },
				SnapshotApply: testSSTApply(targetDB),
				Storage:       storage,
				GroupID:       meta.ID,
				Region:        localmeta.CloneRegionMetaPtr(&meta),
			}, nil
		},
	})
	defer targetStore.Close()

	sourceSvc := NewServiceWithSnapshot(sourceStore, snapshotpkg.NewDBStore(sourceDB))
	targetSvc := NewServiceWithSnapshot(targetStore, snapshotpkg.NewDBStore(targetDB))

	exportStream := &exportRegionSnapshotStreamCapture{ctx: context.Background()}
	require.NoError(t, sourceSvc.ExportRegionSnapshotStream(&adminpb.ExportRegionSnapshotStreamRequest{
		RegionId: region.ID,
	}, exportStream))
	require.NotEmpty(t, exportStream.chunks)

	importReqs := make([]*adminpb.ImportRegionSnapshotStreamRequest, 0, len(exportStream.chunks))
	for _, chunk := range exportStream.chunks {
		importReqs = append(importReqs, &adminpb.ImportRegionSnapshotStreamRequest{
			SnapshotHeader: append([]byte(nil), chunk.GetSnapshotHeader()...),
			Region:         chunk.GetRegion(),
			Chunk:          append([]byte(nil), chunk.GetChunk()...),
		})
	}
	importStream := &importRegionSnapshotStreamFeed{ctx: context.Background(), reqs: importReqs}
	require.NoError(t, targetSvc.ImportRegionSnapshotStream(importStream))
	require.NotNil(t, importStream.resp)
	require.Equal(t, region.ID, importStream.resp.GetRegion().GetRegionId())

	status, ok := targetStore.RegionRuntimeStatus(region.ID)
	require.True(t, ok)
	require.True(t, status.Hosted)
	require.Equal(t, uint64(201), status.LocalPeerID)
	require.GreaterOrEqual(t, status.AppliedIndex, uint64(1))

	got, err := targetDB.GetInternalEntry(entrykv.CFDefault, []byte("alpha"), 9)
	require.NoError(t, err)
	require.NotNil(t, got)
	defer got.DecrRef()
	require.Equal(t, valueBacked, got.Value)
}

func TestServiceImportRegionSnapshotStreamRejectsMismatchedRegionMeta(t *testing.T) {
	sourceDir := t.TempDir()
	sourceDB, sourceMeta := openAdminTestDBWithTweak(t, sourceDir, func(opt *local.Options) {
	})
	defer func() {
		require.NoError(t, sourceDB.Close())
		require.NoError(t, sourceMeta.Close())
	}()

	valueBacked := bytes.Repeat([]byte("z"), 4096)
	entry := entrykv.NewInternalEntry(entrykv.CFDefault, []byte("alpha"), 9, valueBacked, 0, 0)
	defer entry.DecrRef()
	require.NoError(t, sourceDB.ApplyInternalEntries([]*entrykv.Entry{entry}))

	region := localmeta.RegionMeta{
		ID:       42,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers: []metaregion.Peer{
			{StoreID: 1, PeerID: 101},
			{StoreID: 2, PeerID: 201},
		},
		State: metaregion.ReplicaStateRunning,
	}
	require.NoError(t, sourceMeta.SaveRegion(region))

	sourceStorage, err := raftlog.NewDBLog(sourceDB).Open(region.ID, sourceMeta)
	require.NoError(t, err)
	require.NoError(t, sourceStorage.ApplySnapshot(myraft.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index: 1,
			Term:  1,
			ConfState: raftpb.ConfState{
				Voters: []uint64{101},
			},
		},
	}))

	sourceStore := store.NewStore(store.Config{
		StoreID:   1,
		LocalMeta: sourceMeta,
		PeerBuilder: func(meta localmeta.RegionMeta) (*peer.Config, error) {
			return &peer.Config{
				RaftConfig: myraft.Config{
					ID:              101,
					ElectionTick:    5,
					HeartbeatTick:   1,
					MaxSizePerMsg:   1 << 20,
					MaxInflightMsgs: 256,
					PreVote:         true,
				},
				Transport:      noopTransport{},
				Apply:          func([]myraft.Entry) error { return nil },
				SnapshotExport: testSSTExport(sourceDB),
				Storage:        sourceStorage,
				GroupID:        meta.ID,
				Region:         localmeta.CloneRegionMetaPtr(&meta),
			}, nil
		},
	})
	defer sourceStore.Close()

	sourcePeerCfg := &peer.Config{
		RaftConfig: myraft.Config{
			ID:              101,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
			PreVote:         true,
		},
		Transport:      noopTransport{},
		Apply:          func([]myraft.Entry) error { return nil },
		SnapshotExport: testSSTExport(sourceDB),
		Storage:        sourceStorage,
		GroupID:        region.ID,
		Region:         localmeta.CloneRegionMetaPtr(&region),
	}
	_, err = sourceStore.StartPeer(sourcePeerCfg, nil)
	require.NoError(t, err)

	sourcePeer, ok := sourceStore.Peer(101)
	require.True(t, ok)
	require.NoError(t, sourcePeer.Campaign())
	require.Eventually(t, func() bool {
		status, ok := sourceStore.RegionRuntimeStatus(region.ID)
		return ok && status.Leader
	}, 2_000_000_000, 20_000_000)

	targetDir := t.TempDir()
	targetDB, targetMeta := openAdminTestDBWithTweak(t, targetDir, func(opt *local.Options) {
	})
	defer func() {
		require.NoError(t, targetDB.Close())
		require.NoError(t, targetMeta.Close())
	}()
	targetStore := store.NewStore(store.Config{
		StoreID:   2,
		LocalMeta: targetMeta,
		PeerBuilder: func(meta localmeta.RegionMeta) (*peer.Config, error) {
			storage, err := raftlog.NewDBLog(targetDB).Open(meta.ID, targetMeta)
			require.NoError(t, err)
			return &peer.Config{
				RaftConfig: myraft.Config{
					ID:              201,
					ElectionTick:    5,
					HeartbeatTick:   1,
					MaxSizePerMsg:   1 << 20,
					MaxInflightMsgs: 256,
					PreVote:         true,
				},
				Transport:     noopTransport{},
				Apply:         func([]myraft.Entry) error { return nil },
				SnapshotApply: testSSTApply(targetDB),
				Storage:       storage,
				GroupID:       meta.ID,
				Region:        localmeta.CloneRegionMetaPtr(&meta),
			}, nil
		},
	})
	defer targetStore.Close()

	sourceSvc := NewServiceWithSnapshot(sourceStore, snapshotpkg.NewDBStore(sourceDB))
	targetSvc := NewServiceWithSnapshot(targetStore, snapshotpkg.NewDBStore(targetDB))

	exportStream := &exportRegionSnapshotStreamCapture{ctx: context.Background()}
	require.NoError(t, sourceSvc.ExportRegionSnapshotStream(&adminpb.ExportRegionSnapshotStreamRequest{
		RegionId: region.ID,
	}, exportStream))
	require.NotEmpty(t, exportStream.chunks)

	importReqs := make([]*adminpb.ImportRegionSnapshotStreamRequest, 0, len(exportStream.chunks))
	for i, chunk := range exportStream.chunks {
		req := &adminpb.ImportRegionSnapshotStreamRequest{
			SnapshotHeader: append([]byte(nil), chunk.GetSnapshotHeader()...),
			Region:         chunk.GetRegion(),
			Chunk:          append([]byte(nil), chunk.GetChunk()...),
		}
		if i == 0 {
			req.Region = &metapb.RegionDescriptor{
				RegionId: chunk.GetRegion().GetRegionId(),
				StartKey: append([]byte(nil), chunk.GetRegion().GetStartKey()...),
				EndKey:   []byte("zz"),
				Epoch:    &metapb.RegionEpoch{Version: chunk.GetRegion().GetEpoch().GetVersion(), ConfVersion: chunk.GetRegion().GetEpoch().GetConfVersion()},
				Peers:    append([]*metapb.RegionPeer(nil), chunk.GetRegion().GetPeers()...),
			}
		}
		importReqs = append(importReqs, req)
	}
	importStream := &importRegionSnapshotStreamFeed{ctx: context.Background(), reqs: importReqs}
	err = targetSvc.ImportRegionSnapshotStream(importStream)
	require.Error(t, err)
	require.ErrorContains(t, err, "region snapshot metadata mismatch")

	status, ok := targetStore.RegionRuntimeStatus(region.ID)
	require.False(t, ok && status.Hosted)

	got, err := targetDB.GetInternalEntry(entrykv.CFDefault, []byte("alpha"), 9)
	require.ErrorIs(t, err, utils.ErrKeyNotFound)
	require.Nil(t, got)
}
