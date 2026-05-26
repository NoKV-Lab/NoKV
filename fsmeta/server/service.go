// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/model"
	fsmetapb "github.com/feichai0017/NoKV/pb/fsmeta"
	"google.golang.org/grpc"
)

// Executor is the fsmeta operation surface exported by the gRPC service.
// fsmeta/exec.Executor satisfies this interface.
type Executor interface {
	Create(ctx context.Context, req model.CreateRequest) (model.CreateResult, error)
	UpdateInode(ctx context.Context, req model.UpdateInodeRequest) (model.InodeRecord, error)
	Lookup(ctx context.Context, req model.LookupRequest) (model.DentryRecord, error)
	LookupPlus(ctx context.Context, req model.LookupRequest) (model.DentryAttrPair, error)
	ReadDir(ctx context.Context, req model.ReadDirRequest) ([]model.DentryRecord, error)
	ReadDirPlus(ctx context.Context, req model.ReadDirRequest) ([]model.DentryAttrPair, error)
	GetReadVersion(ctx context.Context, req model.ReadVersionRequest) (uint64, error)
	SnapshotSubtree(ctx context.Context, req model.SnapshotSubtreeRequest) (model.SnapshotSubtreeToken, error)
	ResolveSnapshotSubtreeToken(ctx context.Context, token model.SnapshotSubtreeToken) (model.SnapshotSubtreeToken, error)
	GetQuotaUsage(ctx context.Context, req model.QuotaUsageRequest) (model.UsageRecord, error)
	Rename(ctx context.Context, req model.RenameRequest) error
	RenameReplace(ctx context.Context, req model.RenameReplaceRequest) (model.RenameReplaceResult, error)
	RenameSubtree(ctx context.Context, req model.RenameSubtreeRequest) error
	Link(ctx context.Context, req model.LinkRequest) error
	Unlink(ctx context.Context, req model.UnlinkRequest) error
	Remove(ctx context.Context, req model.RemoveRequest) (model.RemoveResult, error)
	RemoveDirectory(ctx context.Context, req model.RemoveDirectoryRequest) error
	OpenWriteSession(ctx context.Context, req model.OpenWriteSessionRequest) (model.SessionRecord, error)
	HeartbeatWriteSession(ctx context.Context, req model.HeartbeatWriteSessionRequest) (model.SessionRecord, error)
	CloseWriteSession(ctx context.Context, req model.CloseWriteSessionRequest) error
	ExpireWriteSessions(ctx context.Context, req model.ExpireWriteSessionsRequest) (model.ExpireWriteSessionsResult, error)
}

type visibleSnapshotRetirer interface {
	RetireVisibleSnapshot(version uint64)
}

// Service exposes NoKV-native filesystem metadata operations over gRPC.
// It is intentionally a thin transport layer; all transaction semantics stay in
// the Executor implementation.
type Service struct {
	fsmetapb.UnimplementedFSMetadataServer

	executor Executor
	watcher  fsmeta.Watcher
	snapshot fsmeta.SnapshotPublisher
}

// Option configures an FSMetadata service.
type Option func(*Service)

// WithWatcher enables WatchSubtree streams for the service.
func WithWatcher(watcher fsmeta.Watcher) Option {
	return func(s *Service) {
		s.watcher = watcher
	}
}

// WithSnapshotPublisher records SnapshotSubtree epochs in rooted truth.
func WithSnapshotPublisher(publisher fsmeta.SnapshotPublisher) Option {
	return func(s *Service) {
		s.snapshot = publisher
	}
}

// NewService constructs an FSMetadata service around executor.
func NewService(executor Executor, opts ...Option) *Service {
	svc := &Service{executor: executor}
	for _, opt := range opts {
		if opt != nil {
			opt(svc)
		}
	}
	return svc
}

// Register registers one FSMetadata service.
func Register(reg grpc.ServiceRegistrar, executor Executor, opts ...Option) {
	fsmetapb.RegisterFSMetadataServer(reg, NewService(executor, opts...))
}

func (s *Service) Create(ctx context.Context, req *fsmetapb.CreateRequest) (*fsmetapb.CreateResponse, error) {
	if err := s.requireExecutor(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, rpcInvalidArgument("fsmeta create request is required")
	}
	result, err := s.executor.Create(ctx, createRequestFromProto(req))
	if err != nil {
		return nil, rpcError(err)
	}
	return &fsmetapb.CreateResponse{
		Dentry: dentryToProto(result.Dentry),
		Inode:  inodeToProto(result.Inode),
	}, nil
}

func (s *Service) UpdateInode(ctx context.Context, req *fsmetapb.UpdateInodeRequest) (*fsmetapb.UpdateInodeResponse, error) {
	if err := s.requireExecutor(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, rpcInvalidArgument("fsmeta update inode request is required")
	}
	inode, err := s.executor.UpdateInode(ctx, updateInodeRequestFromProto(req))
	if err != nil {
		return nil, rpcError(err)
	}
	return &fsmetapb.UpdateInodeResponse{Inode: inodeToProto(inode)}, nil
}

func (s *Service) Lookup(ctx context.Context, req *fsmetapb.LookupRequest) (*fsmetapb.LookupResponse, error) {
	if err := s.requireExecutor(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, rpcInvalidArgument("fsmeta lookup request is required")
	}
	record, err := s.executor.Lookup(ctx, lookupRequestFromProto(req))
	if err != nil {
		return nil, rpcError(err)
	}
	return &fsmetapb.LookupResponse{Dentry: dentryToProto(record)}, nil
}

func (s *Service) LookupPlus(ctx context.Context, req *fsmetapb.LookupRequest) (*fsmetapb.LookupPlusResponse, error) {
	if err := s.requireExecutor(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, rpcInvalidArgument("fsmeta lookup plus request is required")
	}
	pair, err := s.executor.LookupPlus(ctx, lookupRequestFromProto(req))
	if err != nil {
		return nil, rpcError(err)
	}
	return &fsmetapb.LookupPlusResponse{Entry: pairToProto(pair)}, nil
}

func (s *Service) ReadDir(ctx context.Context, req *fsmetapb.ReadDirRequest) (*fsmetapb.ReadDirResponse, error) {
	if err := s.requireExecutor(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, rpcInvalidArgument("fsmeta readdir request is required")
	}
	entries, err := s.executor.ReadDir(ctx, readDirRequestFromProto(req))
	if err != nil {
		return nil, rpcError(err)
	}
	resp := &fsmetapb.ReadDirResponse{Entries: make([]*fsmetapb.DentryRecord, 0, len(entries))}
	for _, entry := range entries {
		resp.Entries = append(resp.Entries, dentryToProto(entry))
	}
	return resp, nil
}

func (s *Service) ReadDirPlus(ctx context.Context, req *fsmetapb.ReadDirRequest) (*fsmetapb.ReadDirPlusResponse, error) {
	if err := s.requireExecutor(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, rpcInvalidArgument("fsmeta readdirplus request is required")
	}
	entries, err := s.executor.ReadDirPlus(ctx, readDirRequestFromProto(req))
	if err != nil {
		return nil, rpcError(err)
	}
	resp := &fsmetapb.ReadDirPlusResponse{Entries: make([]*fsmetapb.DentryAttrPair, 0, len(entries))}
	for _, entry := range entries {
		resp.Entries = append(resp.Entries, pairToProto(entry))
	}
	return resp, nil
}

func (s *Service) WatchSubtree(stream fsmetapb.FSMetadata_WatchSubtreeServer) error {
	if err := s.requireWatcher(); err != nil {
		return err
	}
	first, err := stream.Recv()
	if err != nil {
		return rpcStreamError(err)
	}
	subscribe := first.GetSubscribe()
	if subscribe == nil {
		return rpcInvalidArgument("fsmeta watch first message must subscribe")
	}
	watchReq := watchRequestFromProto(subscribe)
	sub, err := s.watcher.Subscribe(stream.Context(), watchReq)
	if err != nil {
		return rpcError(err)
	}
	defer sub.Close()
	if err := stream.Send(&fsmetapb.WatchSubtreeResponse{
		Payload: &fsmetapb.WatchSubtreeResponse_Ready{
			Ready: &fsmetapb.WatchReady{Cursor: watchCursorToProto(sub.ReadyCursor())},
		},
	}); err != nil {
		return rpcStreamError(err)
	}

	recvErr := make(chan error, 1)
	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				recvErr <- err
				return
			}
			if ack := msg.GetAck(); ack != nil {
				sub.Ack(watchCursorFromProto(ack.GetCursor()))
				continue
			}
			recvErr <- rpcInvalidArgument("fsmeta watch stream only accepts ack after subscribe")
			return
		}
	}()

	for {
		select {
		case err := <-recvErr:
			return rpcStreamError(err)
		case <-stream.Context().Done():
			return rpcStreamError(stream.Context().Err())
		case evt, ok := <-sub.Events():
			if !ok {
				return rpcError(sub.Err())
			}
			if err := stream.Send(&fsmetapb.WatchSubtreeResponse{
				Payload: &fsmetapb.WatchSubtreeResponse_Event{Event: watchEventToProto(evt)},
			}); err != nil {
				return rpcStreamError(err)
			}
		}
	}
}

func (s *Service) GetReadVersion(ctx context.Context, req *fsmetapb.GetReadVersionRequest) (*fsmetapb.GetReadVersionResponse, error) {
	if err := s.requireExecutor(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, rpcInvalidArgument("fsmeta get read version request is required")
	}
	version, err := s.executor.GetReadVersion(ctx, getReadVersionRequestFromProto(req))
	if err != nil {
		return nil, rpcError(err)
	}
	return &fsmetapb.GetReadVersionResponse{ReadVersion: version}, nil
}

func (s *Service) SnapshotSubtree(ctx context.Context, req *fsmetapb.SnapshotSubtreeRequest) (*fsmetapb.SnapshotSubtreeResponse, error) {
	if err := s.requireExecutor(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, rpcInvalidArgument("fsmeta snapshot subtree request is required")
	}
	token, err := s.executor.SnapshotSubtree(ctx, snapshotSubtreeRequestFromProto(req))
	if err != nil {
		return nil, rpcError(err)
	}
	if s.snapshot != nil {
		if err := s.snapshot.PublishSnapshotSubtree(ctx, token); err != nil {
			if retireErr := s.snapshot.RetireSnapshotSubtree(ctx, token); retireErr != nil {
				return nil, rpcError(errors.Join(err, fmt.Errorf("retire snapshot epoch after publish failure: %w", retireErr)))
			}
			return nil, rpcError(err)
		}
	}
	return snapshotSubtreeResponseToProto(token), nil
}

func (s *Service) RetireSnapshotSubtree(ctx context.Context, req *fsmetapb.RetireSnapshotSubtreeRequest) (*fsmetapb.RetireSnapshotSubtreeResponse, error) {
	if err := s.requireExecutor(); err != nil {
		return nil, err
	}
	if s == nil || s.snapshot == nil {
		return nil, rpcServiceUnavailable("fsmeta snapshot publisher is not configured")
	}
	if req == nil {
		return nil, rpcInvalidArgument("fsmeta retire snapshot subtree request is required")
	}
	retireToken, err := retireSnapshotSubtreeRequestFromProto(req)
	if err != nil {
		return nil, rpcError(err)
	}
	token, err := s.executor.ResolveSnapshotSubtreeToken(ctx, retireToken)
	if err != nil {
		return nil, rpcError(err)
	}
	if err := s.snapshot.RetireSnapshotSubtree(ctx, token); err != nil {
		return nil, rpcError(err)
	}
	if retirer, ok := s.executor.(visibleSnapshotRetirer); ok {
		retirer.RetireVisibleSnapshot(token.ReadVersion)
	}
	return &fsmetapb.RetireSnapshotSubtreeResponse{}, nil
}

func (s *Service) GetQuotaUsage(ctx context.Context, req *fsmetapb.QuotaUsageRequest) (*fsmetapb.QuotaUsageResponse, error) {
	if err := s.requireExecutor(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, rpcInvalidArgument("fsmeta quota usage request is required")
	}
	usage, err := s.executor.GetQuotaUsage(ctx, quotaUsageRequestFromProto(req))
	if err != nil {
		return nil, rpcError(err)
	}
	return quotaUsageResponseToProto(usage), nil
}

func (s *Service) Rename(ctx context.Context, req *fsmetapb.RenameRequest) (*fsmetapb.RenameResponse, error) {
	if err := s.requireExecutor(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, rpcInvalidArgument("fsmeta rename request is required")
	}
	if err := s.executor.Rename(ctx, renameRequestFromProto(req)); err != nil {
		return nil, rpcError(err)
	}
	return &fsmetapb.RenameResponse{}, nil
}

func (s *Service) RenameReplace(ctx context.Context, req *fsmetapb.RenameReplaceRequest) (*fsmetapb.RenameReplaceResponse, error) {
	if err := s.requireExecutor(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, rpcInvalidArgument("fsmeta rename replace request is required")
	}
	result, err := s.executor.RenameReplace(ctx, renameReplaceRequestFromProto(req))
	if err != nil {
		return nil, rpcError(err)
	}
	return renameReplaceResponseToProto(result), nil
}

func (s *Service) RenameSubtree(ctx context.Context, req *fsmetapb.RenameSubtreeRequest) (*fsmetapb.RenameSubtreeResponse, error) {
	if err := s.requireExecutor(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, rpcInvalidArgument("fsmeta rename subtree request is required")
	}
	if err := s.executor.RenameSubtree(ctx, renameSubtreeRequestFromProto(req)); err != nil {
		return nil, rpcError(err)
	}
	return &fsmetapb.RenameSubtreeResponse{}, nil
}

func (s *Service) Link(ctx context.Context, req *fsmetapb.LinkRequest) (*fsmetapb.LinkResponse, error) {
	if err := s.requireExecutor(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, rpcInvalidArgument("fsmeta link request is required")
	}
	if err := s.executor.Link(ctx, linkRequestFromProto(req)); err != nil {
		return nil, rpcError(err)
	}
	return &fsmetapb.LinkResponse{}, nil
}

func (s *Service) Unlink(ctx context.Context, req *fsmetapb.UnlinkRequest) (*fsmetapb.UnlinkResponse, error) {
	if err := s.requireExecutor(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, rpcInvalidArgument("fsmeta unlink request is required")
	}
	if err := s.executor.Unlink(ctx, unlinkRequestFromProto(req)); err != nil {
		return nil, rpcError(err)
	}
	return &fsmetapb.UnlinkResponse{}, nil
}

func (s *Service) Remove(ctx context.Context, req *fsmetapb.RemoveRequest) (*fsmetapb.RemoveResponse, error) {
	if err := s.requireExecutor(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, rpcInvalidArgument("fsmeta remove request is required")
	}
	result, err := s.executor.Remove(ctx, removeRequestFromProto(req))
	if err != nil {
		return nil, rpcError(err)
	}
	return removeResponseToProto(result), nil
}

func (s *Service) RemoveDirectory(ctx context.Context, req *fsmetapb.RemoveDirectoryRequest) (*fsmetapb.RemoveDirectoryResponse, error) {
	if err := s.requireExecutor(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, rpcInvalidArgument("fsmeta remove directory request is required")
	}
	if err := s.executor.RemoveDirectory(ctx, removeDirectoryRequestFromProto(req)); err != nil {
		return nil, rpcError(err)
	}
	return &fsmetapb.RemoveDirectoryResponse{}, nil
}

func (s *Service) OpenWriteSession(ctx context.Context, req *fsmetapb.OpenWriteSessionRequest) (*fsmetapb.OpenWriteSessionResponse, error) {
	if err := s.requireExecutor(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, rpcInvalidArgument("fsmeta open write session request is required")
	}
	record, err := s.executor.OpenWriteSession(ctx, openWriteSessionRequestFromProto(req))
	if err != nil {
		return nil, rpcError(err)
	}
	return &fsmetapb.OpenWriteSessionResponse{Session: sessionToProto(record)}, nil
}

func (s *Service) HeartbeatWriteSession(ctx context.Context, req *fsmetapb.HeartbeatWriteSessionRequest) (*fsmetapb.HeartbeatWriteSessionResponse, error) {
	if err := s.requireExecutor(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, rpcInvalidArgument("fsmeta heartbeat write session request is required")
	}
	record, err := s.executor.HeartbeatWriteSession(ctx, heartbeatWriteSessionRequestFromProto(req))
	if err != nil {
		return nil, rpcError(err)
	}
	return &fsmetapb.HeartbeatWriteSessionResponse{Session: sessionToProto(record)}, nil
}

func (s *Service) CloseWriteSession(ctx context.Context, req *fsmetapb.CloseWriteSessionRequest) (*fsmetapb.CloseWriteSessionResponse, error) {
	if err := s.requireExecutor(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, rpcInvalidArgument("fsmeta close write session request is required")
	}
	if err := s.executor.CloseWriteSession(ctx, closeWriteSessionRequestFromProto(req)); err != nil {
		return nil, rpcError(err)
	}
	return &fsmetapb.CloseWriteSessionResponse{}, nil
}

func (s *Service) ExpireWriteSessions(ctx context.Context, req *fsmetapb.ExpireWriteSessionsRequest) (*fsmetapb.ExpireWriteSessionsResponse, error) {
	if err := s.requireExecutor(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, rpcInvalidArgument("fsmeta expire write sessions request is required")
	}
	result, err := s.executor.ExpireWriteSessions(ctx, expireWriteSessionsRequestFromProto(req))
	if err != nil {
		return nil, rpcError(err)
	}
	return &fsmetapb.ExpireWriteSessionsResponse{Expired: result.Expired}, nil
}

func (s *Service) requireExecutor() error {
	if s == nil || s.executor == nil {
		return rpcServiceUnavailable("fsmeta executor is not configured")
	}
	return nil
}

func (s *Service) requireWatcher() error {
	if s == nil || s.watcher == nil {
		return rpcServiceUnavailable("fsmeta watcher is not configured")
	}
	return nil
}

func rpcStreamError(err error) error {
	if err == nil || errors.Is(err, io.EOF) {
		return nil
	}
	return rpcError(err)
}
