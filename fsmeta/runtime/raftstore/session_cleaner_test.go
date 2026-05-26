// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"context"
	"errors"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta/model"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"github.com/stretchr/testify/require"
)

type fakeSessionMountLister struct {
	mounts []*coordpb.MountInfo
	err    error
}

func (l fakeSessionMountLister) ListMounts(context.Context, *coordpb.ListMountsRequest) (*coordpb.ListMountsResponse, error) {
	if l.err != nil {
		return nil, l.err
	}
	return &coordpb.ListMountsResponse{Mounts: l.mounts}, nil
}

type fakeSessionCleanupExecutor struct {
	results map[model.MountID]model.ExpireWriteSessionsResult
	errs    map[model.MountID]error
	calls   []model.ExpireWriteSessionsRequest
}

func (e *fakeSessionCleanupExecutor) ExpireWriteSessions(_ context.Context, req model.ExpireWriteSessionsRequest) (model.ExpireWriteSessionsResult, error) {
	e.calls = append(e.calls, req)
	if err := e.errs[req.Mount]; err != nil {
		return model.ExpireWriteSessionsResult{}, err
	}
	return e.results[req.Mount], nil
}

func TestSessionCleanerExpiresActiveMounts(t *testing.T) {
	exec := &fakeSessionCleanupExecutor{
		results: map[model.MountID]model.ExpireWriteSessionsResult{
			"vol-a": {Expired: 2},
			"vol-b": {Expired: 3},
		},
	}
	cleaner := &sessionCleaner{
		mounts: fakeSessionMountLister{mounts: []*coordpb.MountInfo{
			{MountId: "vol-a", State: coordpb.MountState_MOUNT_STATE_ACTIVE},
			{MountId: "retired", State: coordpb.MountState_MOUNT_STATE_RETIRED},
			{MountId: "unknown", State: coordpb.MountState_MOUNT_STATE_UNKNOWN},
			{MountId: "vol-b", State: coordpb.MountState_MOUNT_STATE_ACTIVE},
		}},
		exec:  exec,
		limit: 7,
	}

	mounts, expired, err := cleaner.expire(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(2), mounts)
	require.Equal(t, uint64(5), expired)
	require.Equal(t, []model.ExpireWriteSessionsRequest{
		{Mount: "vol-a", Limit: 7},
		{Mount: "vol-b", Limit: 7},
	}, exec.calls)
}

func TestSessionCleanerContinuesAfterMountError(t *testing.T) {
	exec := &fakeSessionCleanupExecutor{
		results: map[model.MountID]model.ExpireWriteSessionsResult{
			"vol-b": {Expired: 4},
		},
		errs: map[model.MountID]error{
			"vol-a": errors.New("boom"),
		},
	}
	cleaner := &sessionCleaner{
		mounts: fakeSessionMountLister{mounts: []*coordpb.MountInfo{
			{MountId: "vol-a", State: coordpb.MountState_MOUNT_STATE_ACTIVE},
			{MountId: "vol-b", State: coordpb.MountState_MOUNT_STATE_ACTIVE},
		}},
		exec: exec,
	}

	mounts, expired, err := cleaner.expire(context.Background())
	require.ErrorContains(t, err, "mount vol-a")
	require.Equal(t, uint64(2), mounts)
	require.Equal(t, uint64(4), expired)
	require.Len(t, exec.calls, 2)
}

func TestSessionCleanerRecordsStats(t *testing.T) {
	cleaner := &sessionCleaner{
		mounts: fakeSessionMountLister{mounts: []*coordpb.MountInfo{
			{MountId: "vol", State: coordpb.MountState_MOUNT_STATE_ACTIVE},
		}},
		exec: &fakeSessionCleanupExecutor{
			results: map[model.MountID]model.ExpireWriteSessionsResult{"vol": {Expired: 1}},
		},
		stats: sessionCleanerStats{Enabled: true},
	}

	require.NoError(t, cleaner.runOnce(context.Background()))

	stats := cleaner.Stats()
	require.Equal(t, true, stats["enabled"])
	require.Equal(t, uint64(1), stats["runs"])
	require.Equal(t, uint64(1), stats["last_mounts"])
	require.Equal(t, uint64(1), stats["last_expired"])
	require.Equal(t, uint64(1), stats["total_expired"])
	require.Empty(t, stats["last_error"])
}
