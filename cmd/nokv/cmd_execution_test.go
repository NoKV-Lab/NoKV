// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	adminpb "github.com/feichai0017/NoKV/pb/admin"
	adminclient "github.com/feichai0017/NoKV/raftstore/admin"
	"github.com/stretchr/testify/require"
)

type fakeAdminClient struct {
	executionResp *adminpb.ExecutionStatusResponse
	executionErr  error
}

func (f *fakeAdminClient) AddPeer(context.Context, *adminpb.AddPeerRequest) (*adminpb.AddPeerResponse, error) {
	return nil, nil
}

func (f *fakeAdminClient) RemovePeer(context.Context, *adminpb.RemovePeerRequest) (*adminpb.RemovePeerResponse, error) {
	return nil, nil
}

func (f *fakeAdminClient) TransferLeader(context.Context, *adminpb.TransferLeaderRequest) (*adminpb.TransferLeaderResponse, error) {
	return nil, nil
}

func (f *fakeAdminClient) RegionRuntimeStatus(context.Context, *adminpb.RegionRuntimeStatusRequest) (*adminpb.RegionRuntimeStatusResponse, error) {
	return nil, nil
}

func (f *fakeAdminClient) ExecutionStatus(context.Context, *adminpb.ExecutionStatusRequest) (*adminpb.ExecutionStatusResponse, error) {
	return f.executionResp, f.executionErr
}

func TestRunExecutionCmdJSON(t *testing.T) {
	origDial := dialAdmin
	dialAdmin = func(ctx context.Context, addr string) (adminclient.Client, func() error, error) {
		require.Equal(t, "127.0.0.1:21170", addr)
		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		require.WithinDuration(t, time.Now().Add(2*time.Second), deadline, 300*time.Millisecond)
		return &fakeAdminClient{
			executionResp: &adminpb.ExecutionStatusResponse{
				LastAdmission: &adminpb.ExecutionAdmissionStatus{
					Observed:   true,
					Class:      adminpb.ExecutionAdmissionClass_EXECUTION_ADMISSION_CLASS_WRITE,
					Reason:     adminpb.ExecutionAdmissionReason_EXECUTION_ADMISSION_REASON_ACCEPTED,
					Accepted:   true,
					RegionId:   9,
					PeerId:     19,
					RequestId:  29,
					AtUnixNano: time.Unix(1710000000, 123).UnixNano(),
				},
				Restart: &adminpb.ExecutionRestartStatus{
					State:              adminpb.ExecutionRestartState_EXECUTION_RESTART_STATE_DEGRADED,
					RegionCount:        3,
					RaftGroupCount:     2,
					MissingRaftPointer: []uint64{7},
				},
				Topology: []*adminpb.ExecutionTopologyStatus{
					{
						TransitionId:      "tr-1",
						RegionId:          9,
						Action:            "peer change",
						Outcome:           adminpb.ExecutionTopologyOutcome_EXECUTION_TOPOLOGY_OUTCOME_APPLIED,
						Publish:           adminpb.ExecutionPublishState_EXECUTION_PUBLISH_STATE_TERMINAL_PUBLISHED,
						UpdatedAtUnixNano: time.Unix(1710000001, 456).UnixNano(),
					},
					{
						TransitionId: "tr-2",
						RegionId:     12,
						Action:       "split",
						Outcome:      adminpb.ExecutionTopologyOutcome_EXECUTION_TOPOLOGY_OUTCOME_PROPOSED,
						Publish:      adminpb.ExecutionPublishState_EXECUTION_PUBLISH_STATE_PLANNED_PUBLISHED,
					},
				},
			},
		}, func() error { return nil }, nil
	}
	t.Cleanup(func() { dialAdmin = origDial })

	var buf bytes.Buffer
	err := runExecutionCmd(&buf, []string{"-addr", "127.0.0.1:21170", "-timeout", "2s", "-region", "9", "-json"})
	require.NoError(t, err)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &payload))
	require.Equal(t, "127.0.0.1:21170", payload["addr"])
	admission := payload["last_admission"].(map[string]any)
	require.Equal(t, true, admission["observed"])
	require.Equal(t, "write", admission["class"])
	require.Equal(t, "accepted", admission["reason"])
	restart := payload["restart"].(map[string]any)
	require.Equal(t, "degraded", restart["state"])
	topology := payload["topology"].([]any)
	require.Len(t, topology, 1)
	entry := topology[0].(map[string]any)
	require.Equal(t, "tr-1", entry["transition_id"])
	require.Equal(t, "applied", entry["outcome"])
	require.Equal(t, "terminal-published", entry["publish"])
}

func TestRunExecutionCmdTransitionFilter(t *testing.T) {
	origDial := dialAdmin
	dialAdmin = func(ctx context.Context, addr string) (adminclient.Client, func() error, error) {
		return &fakeAdminClient{
			executionResp: &adminpb.ExecutionStatusResponse{
				Restart: &adminpb.ExecutionRestartStatus{
					State:          adminpb.ExecutionRestartState_EXECUTION_RESTART_STATE_READY,
					RegionCount:    2,
					RaftGroupCount: 2,
				},
				Topology: []*adminpb.ExecutionTopologyStatus{
					{
						TransitionId: "keep-me",
						RegionId:     88,
						Action:       "split",
						Outcome:      adminpb.ExecutionTopologyOutcome_EXECUTION_TOPOLOGY_OUTCOME_PROPOSED,
						Publish:      adminpb.ExecutionPublishState_EXECUTION_PUBLISH_STATE_PLANNED_PUBLISHED,
					},
					{
						TransitionId: "drop-me",
						RegionId:     99,
						Action:       "merge",
						Outcome:      adminpb.ExecutionTopologyOutcome_EXECUTION_TOPOLOGY_OUTCOME_FAILED,
						Publish:      adminpb.ExecutionPublishState_EXECUTION_PUBLISH_STATE_TERMINAL_FAILED,
					},
				},
			},
		}, func() error { return nil }, nil
	}
	t.Cleanup(func() { dialAdmin = origDial })

	var buf bytes.Buffer
	err := runExecutionCmd(&buf, []string{"-addr", "127.0.0.1:21170", "-transition", "keep-me"})
	require.NoError(t, err)
	out := buf.String()
	require.Contains(t, out, "Topology.Count        1")
	require.Contains(t, out, "transition=keep-me")
	require.NotContains(t, out, "transition=drop-me")
}

func TestRunExecutionCmdText(t *testing.T) {
	origDial := dialAdmin
	dialAdmin = func(ctx context.Context, addr string) (adminclient.Client, func() error, error) {
		return &fakeAdminClient{
			executionResp: &adminpb.ExecutionStatusResponse{
				LastAdmission: &adminpb.ExecutionAdmissionStatus{
					Observed: true,
					Class:    adminpb.ExecutionAdmissionClass_EXECUTION_ADMISSION_CLASS_READ,
					Reason:   adminpb.ExecutionAdmissionReason_EXECUTION_ADMISSION_REASON_KEY_NOT_IN_REGION,
					RegionId: 88,
					Detail:   "request keys failed local validation",
				},
				Restart: &adminpb.ExecutionRestartStatus{
					State:              adminpb.ExecutionRestartState_EXECUTION_RESTART_STATE_READY,
					RegionCount:        1,
					RaftGroupCount:     1,
					MissingRaftPointer: nil,
				},
				Topology: []*adminpb.ExecutionTopologyStatus{{
					TransitionId: "tr-2",
					RegionId:     88,
					Action:       "split",
					Outcome:      adminpb.ExecutionTopologyOutcome_EXECUTION_TOPOLOGY_OUTCOME_PROPOSED,
					Publish:      adminpb.ExecutionPublishState_EXECUTION_PUBLISH_STATE_PLANNED_PUBLISHED,
					LastError:    "stalled",
				}},
			},
		}, func() error { return nil }, nil
	}
	t.Cleanup(func() { dialAdmin = origDial })

	var buf bytes.Buffer
	err := runExecutionCmd(&buf, []string{"-addr", "127.0.0.1:21170"})
	require.NoError(t, err)
	out := buf.String()
	require.Contains(t, out, "Admission.Class       read")
	require.Contains(t, out, "Admission.Reason      key-not-in-region")
	require.Contains(t, out, "Restart.State         ready")
	require.Contains(t, out, "Topology.Count        1")
	require.Contains(t, out, "transition=tr-2")
	require.Contains(t, out, "publish=planned-published")
}

func TestRunExecutionCmdRequiresAddr(t *testing.T) {
	var buf bytes.Buffer
	err := runExecutionCmd(&buf, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "--addr is required")
}
