// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package admin

import (
	"bytes"
	"context"
	"fmt"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	adminpb "github.com/feichai0017/NoKV/pb/admin"
	"io"

	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	snapshotpkg "github.com/feichai0017/NoKV/raftstore/snapshot/sst"
	"github.com/feichai0017/NoKV/raftstore/store"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

const snapshotStreamChunkSize = 64 << 10

// Service exposes raftstore admin operations needed for migration and
// membership management.
type Service struct {
	adminpb.UnimplementedRaftAdminServer
	store    *store.Store
	snapshot snapshotpkg.SnapshotStore
}

// NewService constructs an admin service bound to one raftstore store.
func NewService(st *store.Store) *Service {
	return &Service{store: st}
}

// NewServiceWithSnapshot constructs an admin service with direct access to the
// storage-side snapshot bridge needed for SST export/import.
func NewServiceWithSnapshot(st *store.Store, snapshot snapshotpkg.SnapshotStore) *Service {
	return &Service{store: st, snapshot: snapshot}
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

// ExportRegionSnapshot returns the current region snapshot from the leader,
// encoded as one migration-only SST snapshot payload.
func (s *Service) ExportRegionSnapshot(ctx context.Context, req *adminpb.ExportRegionSnapshotRequest) (*adminpb.ExportRegionSnapshotResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, rpcCanceled(err)
	}
	region, header, reader, waitExport, err := s.startExportRegionSnapshot(ctx, req.GetRegionId())
	if err != nil {
		return nil, err
	}
	defer func() { _ = reader.Close() }()
	payload, err := io.ReadAll(reader)
	if err != nil {
		return nil, rpcInternalf("export region snapshot payload: %v", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, rpcCanceled(err)
	}
	if err := waitExport(); err != nil {
		return nil, rpcInternalf("export sst region snapshot: %v", err)
	}
	var snap raftpb.Snapshot
	if err := snap.Unmarshal(header); err != nil {
		return nil, rpcInternalf("unmarshal region snapshot header: %v", err)
	}
	snap.Data = payload
	data, err := (&snap).Marshal()
	if err != nil {
		return nil, rpcInternalf("marshal region snapshot: %v", err)
	}
	return &adminpb.ExportRegionSnapshotResponse{Snapshot: data, Region: localmeta.DescriptorToProto(region)}, nil
}

// ExportRegionSnapshotStream streams one migration-only SST snapshot payload.
// The first message carries the raft snapshot header and region metadata.
func (s *Service) ExportRegionSnapshotStream(req *adminpb.ExportRegionSnapshotStreamRequest, stream adminpb.RaftAdmin_ExportRegionSnapshotStreamServer) error {
	ctx := stream.Context()
	if err := ctx.Err(); err != nil {
		return rpcCanceled(err)
	}
	region, header, reader, waitExport, err := s.startExportRegionSnapshot(ctx, req.GetRegionId())
	if err != nil {
		return err
	}
	defer func() { _ = reader.Close() }()
	buf := make([]byte, snapshotStreamChunkSize)
	first := true
	for {
		if err := ctx.Err(); err != nil {
			return rpcCanceled(err)
		}
		n, readErr := reader.Read(buf)
		if n > 0 || first {
			resp := &adminpb.ExportRegionSnapshotStreamResponse{Chunk: append([]byte(nil), buf[:n]...)}
			if first {
				resp.SnapshotHeader = header
				resp.Region = localmeta.DescriptorToProto(region)
				first = false
			}
			if err := stream.Send(resp); err != nil {
				return err
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return rpcInternalf("export region snapshot stream: %v", readErr)
		}
	}
	if err := waitExport(); err != nil {
		return rpcInternalf("export sst region snapshot stream: %v", err)
	}
	return nil
}

// ImportRegionSnapshot imports one leader-exported region snapshot on the local
// store. The local peer is bootstrapped on demand from the payload.
func (s *Service) ImportRegionSnapshot(ctx context.Context, req *adminpb.ImportRegionSnapshotRequest) (*adminpb.ImportRegionSnapshotResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, rpcCanceled(err)
	}
	if s == nil || s.store == nil {
		return nil, rpcServiceNotConfigured("raft admin service not configured")
	}
	if len(req.GetSnapshot()) == 0 {
		return nil, rpcInvalidArgument("snapshot is required")
	}
	var snap raftpb.Snapshot
	if err := snap.Unmarshal(req.GetSnapshot()); err != nil {
		return nil, rpcInvalidArgumentf("unmarshal region snapshot: %v", err)
	}
	meta, err := s.importRegionSnapshot(ctx, snap, nil)
	if err != nil {
		return nil, err
	}
	return &adminpb.ImportRegionSnapshotResponse{Region: localmeta.DescriptorToProto(meta)}, nil
}

// ImportRegionSnapshotStream imports one leader-exported region snapshot from a
// streamed payload. The first chunk must carry the raft snapshot header and
// region metadata.
func (s *Service) ImportRegionSnapshotStream(stream adminpb.RaftAdmin_ImportRegionSnapshotStreamServer) error {
	ctx := stream.Context()
	if err := ctx.Err(); err != nil {
		return rpcCanceled(err)
	}
	if s == nil || s.store == nil {
		return rpcServiceNotConfigured("raft admin service not configured")
	}
	if s.snapshot == nil {
		return rpcSnapshotNotConfigured("sst snapshot import is not configured")
	}
	first, err := stream.Recv()
	if err == io.EOF {
		return rpcInvalidArgument("snapshot stream is empty")
	}
	if err != nil {
		return err
	}
	if len(first.GetSnapshotHeader()) == 0 {
		return rpcInvalidArgument("snapshot_header is required")
	}
	if first.GetRegion() == nil {
		return rpcInvalidArgument("region is required")
	}
	var snap raftpb.Snapshot
	if err := snap.Unmarshal(first.GetSnapshotHeader()); err != nil {
		return rpcInvalidArgumentf("unmarshal region snapshot header: %v", err)
	}
	meta, err := localmeta.FromDescriptorProto(first.GetRegion())
	if err != nil {
		return rpcInvalidArgumentf("decode region snapshot metadata: %v", err)
	}
	pr, pw := io.Pipe()
	resultCh := make(chan struct {
		meta localmeta.RegionMeta
		err  error
	}, 1)
	go func() {
		installed, importErr := s.importRegionSnapshot(ctx, snap, &streamedImport{meta: meta, reader: pr})
		resultCh <- struct {
			meta localmeta.RegionMeta
			err  error
		}{meta: installed, err: importErr}
	}()
	writeChunk := func(chunk []byte) error {
		if len(chunk) == 0 {
			return nil
		}
		_, err := pw.Write(chunk)
		return err
	}
	if err := writeChunk(first.GetChunk()); err != nil {
		_ = pw.CloseWithError(err)
		outcome := <-resultCh
		if outcome.err != nil {
			return outcome.err
		}
		return rpcInternalf("write region snapshot stream: %v", err)
	}
	for {
		if err := ctx.Err(); err != nil {
			_ = pw.CloseWithError(err)
			<-resultCh
			return rpcCanceled(err)
		}
		req, recvErr := stream.Recv()
		if recvErr == io.EOF {
			_ = pw.Close()
			break
		}
		if recvErr != nil {
			_ = pw.CloseWithError(recvErr)
			outcome := <-resultCh
			if outcome.err != nil {
				return outcome.err
			}
			return recvErr
		}
		if len(req.GetSnapshotHeader()) != 0 || req.GetRegion() != nil {
			streamErr := fmt.Errorf("snapshot header repeated")
			_ = pw.CloseWithError(streamErr)
			<-resultCh
			return rpcInvalidArgument("snapshot header may only appear in the first chunk")
		}
		if err := writeChunk(req.GetChunk()); err != nil {
			_ = pw.CloseWithError(err)
			outcome := <-resultCh
			if outcome.err != nil {
				return outcome.err
			}
			return rpcInternalf("write region snapshot stream: %v", err)
		}
	}
	outcome := <-resultCh
	if outcome.err != nil {
		return outcome.err
	}
	return stream.SendAndClose(&adminpb.ImportRegionSnapshotResponse{Region: localmeta.DescriptorToProto(outcome.meta)})
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

func matchesSnapshotRegion(header, payload localmeta.RegionMeta) bool {
	if header.ID != payload.ID {
		return false
	}
	if !bytes.Equal(header.StartKey, payload.StartKey) {
		return false
	}
	if !bytes.Equal(header.EndKey, payload.EndKey) {
		return false
	}
	if header.Epoch != payload.Epoch {
		return false
	}
	if len(header.Peers) != len(payload.Peers) {
		return false
	}
	for i := range header.Peers {
		if header.Peers[i] != payload.Peers[i] {
			return false
		}
	}
	return true
}

func (s *Service) prepareExportRegionSnapshot(regionID uint64) (localmeta.RegionMeta, []byte, error) {
	runtime, snap, err := s.exportRegionSnapshot(regionID)
	if err != nil {
		return localmeta.RegionMeta{}, nil, err
	}
	headerSnap := snap
	headerSnap.Data = nil
	header, err := (&headerSnap).Marshal()
	if err != nil {
		return localmeta.RegionMeta{}, nil, rpcInternalf("marshal region snapshot header: %v", err)
	}
	return runtime.Meta, header, nil
}

func (s *Service) startExportRegionSnapshot(ctx context.Context, regionID uint64) (localmeta.RegionMeta, []byte, io.ReadCloser, func() error, error) {
	region, header, err := s.prepareExportRegionSnapshot(regionID)
	if err != nil {
		return localmeta.RegionMeta{}, nil, nil, nil, err
	}
	pr, pw := io.Pipe()
	errCh := make(chan error, 1)
	go func() {
		_, writeErr := s.snapshot.ExportSnapshotTo(pw, region)
		_ = pw.CloseWithError(writeErr)
		errCh <- writeErr
	}()
	go func() {
		<-ctx.Done()
		_ = pw.CloseWithError(ctx.Err())
	}()
	wait := func() error { return <-errCh }
	return region, header, pr, wait, nil
}

func (s *Service) exportRegionSnapshot(regionID uint64) (store.RegionRuntimeStatus, raftpb.Snapshot, error) {
	if s == nil || s.store == nil {
		return store.RegionRuntimeStatus{}, raftpb.Snapshot{}, rpcServiceNotConfigured("raft admin service not configured")
	}
	if regionID == 0 {
		return store.RegionRuntimeStatus{}, raftpb.Snapshot{}, rpcInvalidArgument("region_id is required")
	}
	runtime, ok := s.store.RegionRuntimeStatus(regionID)
	if !ok || !runtime.Hosted {
		return store.RegionRuntimeStatus{}, raftpb.Snapshot{}, rpcNotFoundf("region %d is not hosted on this store", regionID)
	}
	if !runtime.Leader {
		return store.RegionRuntimeStatus{}, raftpb.Snapshot{}, rpcNotLeaderf("region %d is not led by this store", regionID)
	}
	peerRef, ok := s.store.Peer(runtime.LocalPeerID)
	if !ok || peerRef == nil {
		return store.RegionRuntimeStatus{}, raftpb.Snapshot{}, rpcPreconditionf("leader peer %d is not registered", runtime.LocalPeerID)
	}
	snap, err := peerRef.Snapshot()
	if err != nil {
		return store.RegionRuntimeStatus{}, raftpb.Snapshot{}, rpcInternalf("export region snapshot: %v", err)
	}
	if s.snapshot == nil {
		return store.RegionRuntimeStatus{}, raftpb.Snapshot{}, rpcSnapshotNotConfigured("sst snapshot export is not configured")
	}
	return runtime, raftpb.Snapshot(snap), nil
}

type streamedImport struct {
	meta   localmeta.RegionMeta
	reader io.Reader
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *contextReader) Read(p []byte) (int, error) {
	if r == nil || r.reader == nil {
		return 0, io.EOF
	}
	if r.ctx == nil {
		return r.reader.Read(p)
	}
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := r.reader.Read(p)
	if err == nil && r.ctx.Err() != nil {
		return n, r.ctx.Err()
	}
	return n, err
}

func (s *Service) importRegionSnapshot(ctx context.Context, snap raftpb.Snapshot, streamed *streamedImport) (localmeta.RegionMeta, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return localmeta.RegionMeta{}, rpcCanceled(err)
	}
	if s.snapshot == nil {
		return localmeta.RegionMeta{}, rpcSnapshotNotConfigured("sst snapshot import is not configured")
	}
	var meta localmeta.RegionMeta
	if streamed == nil {
		metaFile, metaErr := snapshotpkg.ReadPayloadMeta(snap.Data)
		if metaErr != nil {
			return localmeta.RegionMeta{}, rpcInvalidArgumentf("decode sst snapshot payload: %v", metaErr)
		}
		meta = metaFile.Region
	} else {
		meta = streamed.meta
	}
	installImport := func() (*snapshotpkg.ImportResult, error) {
		if streamed != nil {
			return s.snapshot.ImportSnapshotFrom(&contextReader{ctx: ctx, reader: streamed.reader})
		}
		return s.snapshot.ImportSnapshot(snap.Data)
	}
	installed, err := s.store.InstallRegionSSTSnapshot(ctx, raftpb.Snapshot(snap), meta, func() (func() error, error) {
		result, importErr := installImport()
		if importErr != nil {
			return nil, importErr
		}
		if result == nil {
			return nil, nil
		}
		if !matchesSnapshotRegion(meta, result.Meta.Region) {
			if rollbackErr := result.Rollback(); rollbackErr != nil {
				return nil, fmt.Errorf("region snapshot metadata mismatch and rollback failed: %w", rollbackErr)
			}
			return nil, fmt.Errorf("region snapshot metadata mismatch: header=%+v payload=%+v", meta, result.Meta.Region)
		}
		if len(result.ImportedFileIDs) == 0 {
			return nil, nil
		}
		return result.Rollback, nil
	})
	if err != nil {
		return localmeta.RegionMeta{}, rpcPrecondition(err)
	}
	runtime, ok := s.store.RegionRuntimeStatus(installed.ID)
	if !ok {
		return installed, nil
	}
	return runtime.Meta, nil
}
