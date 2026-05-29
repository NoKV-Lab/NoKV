// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package admin

import (
	"context"

	nokverrors "github.com/feichai0017/NoKV/errors"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	adminpb "github.com/feichai0017/NoKV/pb/admin"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"

	"sync"
	"testing"

	"github.com/feichai0017/NoKV/coordinator/storecontrol"
	local "github.com/feichai0017/NoKV/local"
	myraft "github.com/feichai0017/NoKV/raft"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"github.com/feichai0017/NoKV/raftstore/peer"
	"github.com/feichai0017/NoKV/raftstore/raftlog"
	raftstorestats "github.com/feichai0017/NoKV/raftstore/stats"
	"github.com/feichai0017/NoKV/raftstore/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type noopTransport struct{}

func (noopTransport) Send(context.Context, myraft.Message) {}

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
