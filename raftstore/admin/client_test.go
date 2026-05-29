// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package admin

import (
	"context"
	"testing"

	adminpb "github.com/feichai0017/NoKV/pb/admin"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type fakeRaftAdminClient struct {
	addResp       *adminpb.AddPeerResponse
	removeResp    *adminpb.RemovePeerResponse
	transferResp  *adminpb.TransferLeaderResponse
	runtimeResp   *adminpb.RegionRuntimeStatusResponse
	executionResp *adminpb.ExecutionStatusResponse
	addReq        *adminpb.AddPeerRequest
	removeReq     *adminpb.RemovePeerRequest
	transferReq   *adminpb.TransferLeaderRequest
	runtimeReq    *adminpb.RegionRuntimeStatusRequest
	executionReq  *adminpb.ExecutionStatusRequest
}

func (f *fakeRaftAdminClient) AddPeer(_ context.Context, req *adminpb.AddPeerRequest, _ ...grpc.CallOption) (*adminpb.AddPeerResponse, error) {
	f.addReq = req
	if f.addResp == nil {
		return &adminpb.AddPeerResponse{}, nil
	}
	return f.addResp, nil
}

func (f *fakeRaftAdminClient) RemovePeer(_ context.Context, req *adminpb.RemovePeerRequest, _ ...grpc.CallOption) (*adminpb.RemovePeerResponse, error) {
	f.removeReq = req
	if f.removeResp == nil {
		return &adminpb.RemovePeerResponse{}, nil
	}
	return f.removeResp, nil
}

func (f *fakeRaftAdminClient) TransferLeader(_ context.Context, req *adminpb.TransferLeaderRequest, _ ...grpc.CallOption) (*adminpb.TransferLeaderResponse, error) {
	f.transferReq = req
	if f.transferResp == nil {
		return &adminpb.TransferLeaderResponse{}, nil
	}
	return f.transferResp, nil
}

func (f *fakeRaftAdminClient) RegionRuntimeStatus(_ context.Context, req *adminpb.RegionRuntimeStatusRequest, _ ...grpc.CallOption) (*adminpb.RegionRuntimeStatusResponse, error) {
	f.runtimeReq = req
	if f.runtimeResp == nil {
		return &adminpb.RegionRuntimeStatusResponse{}, nil
	}
	return f.runtimeResp, nil
}

func (f *fakeRaftAdminClient) ExecutionStatus(_ context.Context, req *adminpb.ExecutionStatusRequest, _ ...grpc.CallOption) (*adminpb.ExecutionStatusResponse, error) {
	f.executionReq = req
	if f.executionResp == nil {
		return &adminpb.ExecutionStatusResponse{}, nil
	}
	return f.executionResp, nil
}

func TestGRPCClientUnaryWrappersAndDial(t *testing.T) {
	fake := &fakeRaftAdminClient{
		addResp:       &adminpb.AddPeerResponse{},
		removeResp:    &adminpb.RemovePeerResponse{},
		transferResp:  &adminpb.TransferLeaderResponse{},
		runtimeResp:   &adminpb.RegionRuntimeStatusResponse{},
		executionResp: &adminpb.ExecutionStatusResponse{},
	}
	client := &grpcClient{client: fake}

	_, err := client.AddPeer(context.Background(), &adminpb.AddPeerRequest{RegionId: 1, StoreId: 2, PeerId: 3})
	require.NoError(t, err)
	_, err = client.RemovePeer(context.Background(), &adminpb.RemovePeerRequest{RegionId: 1, PeerId: 3})
	require.NoError(t, err)
	_, err = client.TransferLeader(context.Background(), &adminpb.TransferLeaderRequest{RegionId: 1, PeerId: 3})
	require.NoError(t, err)
	_, err = client.RegionRuntimeStatus(context.Background(), &adminpb.RegionRuntimeStatusRequest{RegionId: 1})
	require.NoError(t, err)
	_, err = client.ExecutionStatus(context.Background(), &adminpb.ExecutionStatusRequest{})
	require.NoError(t, err)

	require.NotNil(t, fake.addReq)
	require.NotNil(t, fake.removeReq)
	require.NotNil(t, fake.transferReq)
	require.NotNil(t, fake.runtimeReq)
	require.NotNil(t, fake.executionReq)

	dialed, closeFn, err := Dial(context.Background(), "127.0.0.1:1")
	require.NoError(t, err)
	require.NotNil(t, dialed)
	require.NotNil(t, closeFn)
	require.NoError(t, closeFn())
}
