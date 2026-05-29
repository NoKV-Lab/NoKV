// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package admin

import (
	"context"

	metaregion "github.com/feichai0017/NoKV/meta/region"
	adminpb "github.com/feichai0017/NoKV/pb/admin"

	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"github.com/feichai0017/NoKV/raftstore/store"
)

// Service exposes raftstore admin operations needed for membership management.
type Service struct {
	adminpb.UnimplementedRaftAdminServer
	store *store.Store
}

// NewService constructs an admin service bound to one raftstore store.
func NewService(st *store.Store) *Service {
	return &Service{store: st}
}

// AddPeer issues one raft configuration change on the region leader.
func (s *Service) AddPeer(_ context.Context, req *adminpb.AddPeerRequest) (*adminpb.AddPeerResponse, error) {
	if s == nil || s.store == nil {
		return nil, rpcServiceNotConfigured("raft admin service not configured")
	}
	if req.GetRegionId() == 0 || req.GetStoreId() == 0 || req.GetPeerId() == 0 {
		return nil, rpcInvalidArgument("region_id, store_id, and peer_id are required")
	}
	if err := s.store.AddPeer(req.GetRegionId(), metaregion.Peer{StoreID: req.GetStoreId(), PeerID: req.GetPeerId()}); err != nil {
		return nil, rpcPrecondition(err)
	}
	runtime, ok := s.store.RegionRuntimeStatus(req.GetRegionId())
	if !ok {
		return &adminpb.AddPeerResponse{}, nil
	}
	return &adminpb.AddPeerResponse{Region: localmeta.DescriptorToProto(runtime.Meta)}, nil
}

// RemovePeer issues one raft configuration change removing the specified peer.
func (s *Service) RemovePeer(_ context.Context, req *adminpb.RemovePeerRequest) (*adminpb.RemovePeerResponse, error) {
	if s == nil || s.store == nil {
		return nil, rpcServiceNotConfigured("raft admin service not configured")
	}
	if req.GetRegionId() == 0 || req.GetPeerId() == 0 {
		return nil, rpcInvalidArgument("region_id and peer_id are required")
	}
	if err := s.store.RemovePeer(req.GetRegionId(), req.GetPeerId()); err != nil {
		return nil, rpcPrecondition(err)
	}
	runtime, ok := s.store.RegionRuntimeStatus(req.GetRegionId())
	if !ok {
		return &adminpb.RemovePeerResponse{}, nil
	}
	return &adminpb.RemovePeerResponse{Region: localmeta.DescriptorToProto(runtime.Meta)}, nil
}

// TransferLeader requests leader transfer on the specified region.
func (s *Service) TransferLeader(ctx context.Context, req *adminpb.TransferLeaderRequest) (*adminpb.TransferLeaderResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, rpcCanceled(err)
	}
	if s == nil || s.store == nil {
		return nil, rpcServiceNotConfigured("raft admin service not configured")
	}
	if req.GetRegionId() == 0 || req.GetPeerId() == 0 {
		return nil, rpcInvalidArgument("region_id and peer_id are required")
	}
	if err := s.store.TransferLeader(req.GetRegionId(), req.GetPeerId()); err != nil {
		return nil, rpcPrecondition(err)
	}
	runtime, ok := s.store.RegionRuntimeStatus(req.GetRegionId())
	if !ok {
		return &adminpb.TransferLeaderResponse{}, nil
	}
	return &adminpb.TransferLeaderResponse{Region: localmeta.DescriptorToProto(runtime.Meta)}, nil
}

// RegionRuntimeStatus returns store-local runtime information for one region.
func (s *Service) RegionRuntimeStatus(ctx context.Context, req *adminpb.RegionRuntimeStatusRequest) (*adminpb.RegionRuntimeStatusResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, rpcCanceled(err)
	}
	if s == nil || s.store == nil {
		return nil, rpcServiceNotConfigured("raft admin service not configured")
	}
	if req.GetRegionId() == 0 {
		return nil, rpcInvalidArgument("region_id is required")
	}
	runtime, ok := s.store.RegionRuntimeStatus(req.GetRegionId())
	if !ok {
		return &adminpb.RegionRuntimeStatusResponse{}, nil
	}
	return &adminpb.RegionRuntimeStatusResponse{
		Known:        true,
		Hosted:       runtime.Hosted,
		LocalPeerId:  runtime.LocalPeerID,
		LeaderPeerId: runtime.LeaderPeerID,
		Leader:       runtime.Leader,
		Region:       localmeta.DescriptorToProto(runtime.Meta),
		AppliedIndex: runtime.AppliedIndex,
		AppliedTerm:  runtime.AppliedTerm,
	}, nil
}

// ExecutionStatus returns store-local execution-plane diagnostics derived from
// the store's admission, topology, and restart runtime state.
func (s *Service) ExecutionStatus(ctx context.Context, req *adminpb.ExecutionStatusRequest) (*adminpb.ExecutionStatusResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, rpcCanceled(err)
	}
	_ = req
	if s == nil || s.store == nil {
		return nil, rpcServiceNotConfigured("raft admin service not configured")
	}
	lastAdmission := s.store.LastAdmission()
	topology := s.store.TopologyExecutions()
	resp := &adminpb.ExecutionStatusResponse{
		LastAdmission: buildExecutionAdmissionStatus(lastAdmission),
		Restart:       buildExecutionRestartStatus(s.store.RestartStatus()),
		Topology:      make([]*adminpb.ExecutionTopologyStatus, 0, len(topology)),
	}
	for _, entry := range topology {
		resp.Topology = append(resp.Topology, buildExecutionTopologyStatus(entry))
	}
	return resp, nil
}

func buildExecutionAdmissionStatus(admission store.Admission) *adminpb.ExecutionAdmissionStatus {
	return &adminpb.ExecutionAdmissionStatus{
		Observed:   executionAdmissionObserved(admission),
		Class:      executionAdmissionClassProto(admission.Class),
		Reason:     executionAdmissionReasonProto(admission.Reason),
		Accepted:   admission.Accepted,
		RegionId:   admission.RegionID,
		PeerId:     admission.PeerID,
		RequestId:  admission.RequestID,
		Detail:     admission.Detail,
		AtUnixNano: admission.At.UnixNano(),
	}
}

func executionAdmissionObserved(admission store.Admission) bool {
	return !admission.At.IsZero() ||
		admission.Class != store.AdmissionClassUnknown ||
		admission.Reason != store.AdmissionReasonUnknown ||
		admission.Accepted ||
		admission.RegionID != 0 ||
		admission.PeerID != 0 ||
		admission.RequestID != 0 ||
		admission.Detail != ""
}

func executionAdmissionClassProto(class store.AdmissionClass) adminpb.ExecutionAdmissionClass {
	switch class {
	case store.AdmissionClassRead:
		return adminpb.ExecutionAdmissionClass_EXECUTION_ADMISSION_CLASS_READ
	case store.AdmissionClassWrite:
		return adminpb.ExecutionAdmissionClass_EXECUTION_ADMISSION_CLASS_WRITE
	case store.AdmissionClassTopology:
		return adminpb.ExecutionAdmissionClass_EXECUTION_ADMISSION_CLASS_TOPOLOGY
	default:
		return adminpb.ExecutionAdmissionClass_EXECUTION_ADMISSION_CLASS_UNSPECIFIED
	}
}

func executionAdmissionReasonProto(reason store.AdmissionReason) adminpb.ExecutionAdmissionReason {
	switch reason {
	case store.AdmissionReasonAccepted:
		return adminpb.ExecutionAdmissionReason_EXECUTION_ADMISSION_REASON_ACCEPTED
	case store.AdmissionReasonInvalid:
		return adminpb.ExecutionAdmissionReason_EXECUTION_ADMISSION_REASON_INVALID
	case store.AdmissionReasonStoreNotMatch:
		return adminpb.ExecutionAdmissionReason_EXECUTION_ADMISSION_REASON_STORE_NOT_MATCH
	case store.AdmissionReasonNotHosted:
		return adminpb.ExecutionAdmissionReason_EXECUTION_ADMISSION_REASON_NOT_HOSTED
	case store.AdmissionReasonEpochMismatch:
		return adminpb.ExecutionAdmissionReason_EXECUTION_ADMISSION_REASON_EPOCH_MISMATCH
	case store.AdmissionReasonKeyNotInRegion:
		return adminpb.ExecutionAdmissionReason_EXECUTION_ADMISSION_REASON_KEY_NOT_IN_REGION
	case store.AdmissionReasonNotLeader:
		return adminpb.ExecutionAdmissionReason_EXECUTION_ADMISSION_REASON_NOT_LEADER
	case store.AdmissionReasonCanceled:
		return adminpb.ExecutionAdmissionReason_EXECUTION_ADMISSION_REASON_CANCELED
	case store.AdmissionReasonTimedOut:
		return adminpb.ExecutionAdmissionReason_EXECUTION_ADMISSION_REASON_TIMED_OUT
	default:
		return adminpb.ExecutionAdmissionReason_EXECUTION_ADMISSION_REASON_UNSPECIFIED
	}
}

func buildExecutionTopologyStatus(entry store.TopologyExecution) *adminpb.ExecutionTopologyStatus {
	return &adminpb.ExecutionTopologyStatus{
		TransitionId:      entry.TransitionID,
		RegionId:          entry.RegionID,
		Action:            entry.Action,
		Outcome:           executionTopologyOutcomeProto(entry.Outcome),
		Publish:           executionPublishStateProto(entry.Publish),
		LastError:         entry.LastError,
		UpdatedAtUnixNano: entry.UpdatedAt.UnixNano(),
	}
}

func executionTopologyOutcomeProto(outcome store.ExecutionOutcome) adminpb.ExecutionTopologyOutcome {
	switch outcome {
	case store.ExecutionOutcomeRejected:
		return adminpb.ExecutionTopologyOutcome_EXECUTION_TOPOLOGY_OUTCOME_REJECTED
	case store.ExecutionOutcomeQueued:
		return adminpb.ExecutionTopologyOutcome_EXECUTION_TOPOLOGY_OUTCOME_QUEUED
	case store.ExecutionOutcomeProposed:
		return adminpb.ExecutionTopologyOutcome_EXECUTION_TOPOLOGY_OUTCOME_PROPOSED
	case store.ExecutionOutcomeApplied:
		return adminpb.ExecutionTopologyOutcome_EXECUTION_TOPOLOGY_OUTCOME_APPLIED
	case store.ExecutionOutcomeFailed:
		return adminpb.ExecutionTopologyOutcome_EXECUTION_TOPOLOGY_OUTCOME_FAILED
	default:
		return adminpb.ExecutionTopologyOutcome_EXECUTION_TOPOLOGY_OUTCOME_UNSPECIFIED
	}
}

func executionPublishStateProto(state store.PublishState) adminpb.ExecutionPublishState {
	switch state {
	case store.PublishStateNotRequired:
		return adminpb.ExecutionPublishState_EXECUTION_PUBLISH_STATE_NOT_REQUIRED
	case store.PublishStatePlannedPublished:
		return adminpb.ExecutionPublishState_EXECUTION_PUBLISH_STATE_PLANNED_PUBLISHED
	case store.PublishStateTerminalPending:
		return adminpb.ExecutionPublishState_EXECUTION_PUBLISH_STATE_TERMINAL_PENDING
	case store.PublishStateTerminalPublished:
		return adminpb.ExecutionPublishState_EXECUTION_PUBLISH_STATE_TERMINAL_PUBLISHED
	case store.PublishStateTerminalFailed:
		return adminpb.ExecutionPublishState_EXECUTION_PUBLISH_STATE_TERMINAL_FAILED
	case store.PublishStateTerminalBlocked:
		return adminpb.ExecutionPublishState_EXECUTION_PUBLISH_STATE_TERMINAL_BLOCKED
	default:
		return adminpb.ExecutionPublishState_EXECUTION_PUBLISH_STATE_UNSPECIFIED
	}
}

func buildExecutionRestartStatus(restart store.RestartStatus) *adminpb.ExecutionRestartStatus {
	return &adminpb.ExecutionRestartStatus{
		State:                          executionRestartStateProto(restart.State),
		RegionCount:                    uint64(restart.RegionCount),
		RaftGroupCount:                 uint64(restart.RaftGroupCount),
		MissingRaftPointer:             append([]uint64(nil), restart.MissingRaftPointer...),
		PendingRootEventCount:          uint64(restart.PendingRootEventCount),
		BlockedRootEventCount:          uint64(restart.BlockedRootEventCount),
		PendingSchedulerOperationCount: uint64(restart.PendingSchedulerOpCount),
	}
}

func executionRestartStateProto(state store.RestartState) adminpb.ExecutionRestartState {
	switch state {
	case store.RestartStateReady:
		return adminpb.ExecutionRestartState_EXECUTION_RESTART_STATE_READY
	case store.RestartStateDegraded:
		return adminpb.ExecutionRestartState_EXECUTION_RESTART_STATE_DEGRADED
	default:
		return adminpb.ExecutionRestartState_EXECUTION_RESTART_STATE_UNSPECIFIED
	}
}
