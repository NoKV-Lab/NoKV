// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package admin

import (
	"context"
	adminpb "github.com/feichai0017/NoKV/pb/admin"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client captures the admin control-plane calls exposed by one raftstore node.
type Client interface {
	AddPeer(ctx context.Context, req *adminpb.AddPeerRequest) (*adminpb.AddPeerResponse, error)
	RemovePeer(ctx context.Context, req *adminpb.RemovePeerRequest) (*adminpb.RemovePeerResponse, error)
	TransferLeader(ctx context.Context, req *adminpb.TransferLeaderRequest) (*adminpb.TransferLeaderResponse, error)
	RegionRuntimeStatus(ctx context.Context, req *adminpb.RegionRuntimeStatusRequest) (*adminpb.RegionRuntimeStatusResponse, error)
	ExecutionStatus(ctx context.Context, req *adminpb.ExecutionStatusRequest) (*adminpb.ExecutionStatusResponse, error)
}

// DialFunc connects one admin client to one store address.
type DialFunc func(ctx context.Context, addr string) (Client, func() error, error)

type grpcClient struct {
	client adminpb.RaftAdminClient
}

// forwarding-ok: grpcClient adapts adminpb.RaftAdminClient onto the local admin.Client interface.
func (c *grpcClient) AddPeer(ctx context.Context, req *adminpb.AddPeerRequest) (*adminpb.AddPeerResponse, error) {
	return c.client.AddPeer(ctx, req)
}

// forwarding-ok: grpcClient adapts adminpb.RaftAdminClient onto the local admin.Client interface.
func (c *grpcClient) RemovePeer(ctx context.Context, req *adminpb.RemovePeerRequest) (*adminpb.RemovePeerResponse, error) {
	return c.client.RemovePeer(ctx, req)
}

// forwarding-ok: grpcClient adapts adminpb.RaftAdminClient onto the local admin.Client interface.
func (c *grpcClient) TransferLeader(ctx context.Context, req *adminpb.TransferLeaderRequest) (*adminpb.TransferLeaderResponse, error) {
	return c.client.TransferLeader(ctx, req)
}

// forwarding-ok: grpcClient adapts adminpb.RaftAdminClient onto the local admin.Client interface.
func (c *grpcClient) RegionRuntimeStatus(ctx context.Context, req *adminpb.RegionRuntimeStatusRequest) (*adminpb.RegionRuntimeStatusResponse, error) {
	return c.client.RegionRuntimeStatus(ctx, req)
}

// forwarding-ok: grpcClient adapts adminpb.RaftAdminClient onto the local admin.Client interface.
func (c *grpcClient) ExecutionStatus(ctx context.Context, req *adminpb.ExecutionStatusRequest) (*adminpb.ExecutionStatusResponse, error) {
	return c.client.ExecutionStatus(ctx, req)
}

// Dial connects to one raftstore admin endpoint over gRPC.
func Dial(ctx context.Context, addr string) (Client, func() error, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	closeFn := func() error { return conn.Close() }
	return &grpcClient{client: adminpb.NewRaftAdminClient(conn)}, closeFn, nil
}
