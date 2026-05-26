// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"fmt"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/feichai0017/NoKV/fsmeta/observe"
	"github.com/stretchr/testify/require"
)

type reconcileClient struct {
	watchErrs []error
	watches   []observe.WatchRequest
	pages     [][]model.DentryAttrPair
	reads     []model.ReadDirRequest
}

func (c *reconcileClient) WatchSubtree(_ context.Context, req observe.WatchRequest) (WatchSubscription, error) {
	c.watches = append(c.watches, req)
	if len(c.watchErrs) > 0 {
		err := c.watchErrs[0]
		c.watchErrs = c.watchErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	return &stubWatchSubscription{ready: observe.WatchCursor{RegionID: 1, Term: 1, Index: uint64(len(c.watches))}}, nil
}

func (c *reconcileClient) ReadDirPlus(_ context.Context, req model.ReadDirRequest) ([]model.DentryAttrPair, error) {
	c.reads = append(c.reads, req)
	if len(c.pages) == 0 {
		return nil, fmt.Errorf("unexpected ReadDirPlus")
	}
	page := c.pages[0]
	c.pages = c.pages[1:]
	return page, nil
}

func TestReadDirPlusAllPaginates(t *testing.T) {
	cli := &reconcileClient{
		pages: [][]model.DentryAttrPair{
			{
				{Dentry: model.DentryRecord{Name: "a"}},
				{Dentry: model.DentryRecord{Name: "b"}},
			},
			{
				{Dentry: model.DentryRecord{Name: "c"}},
			},
		},
	}
	got, err := ReadDirPlusAll(context.Background(), cli, model.ReadDirRequest{
		Mount: "vol", Parent: model.RootInode, Limit: 2,
	})
	require.NoError(t, err)
	require.Len(t, got, 3)
	require.Len(t, cli.reads, 2)
	require.Equal(t, "", cli.reads[0].StartAfter)
	require.Equal(t, "b", cli.reads[1].StartAfter)
}

func TestWatchDirectoryWithReconcileOnExpiredCursor(t *testing.T) {
	cli := &reconcileClient{
		watchErrs: []error{fmt.Errorf("%w: stale", model.ErrWatchCursorExpired), nil},
		pages: [][]model.DentryAttrPair{
			{{Dentry: model.DentryRecord{Name: "artifact"}}},
		},
	}
	watchReq := observe.WatchRequest{
		Mount:        "vol",
		RootInode:    model.RootInode,
		ResumeCursor: observe.WatchCursor{RegionID: 7, Term: 1, Index: 100},
	}
	readReq := model.ReadDirRequest{Mount: "vol", Parent: model.RootInode, Limit: 100}

	result, err := WatchDirectoryWithReconcile(context.Background(), cli, watchReq, readReq)
	require.NoError(t, err)
	require.True(t, result.Reconciled)
	require.Len(t, result.Snapshot, 1)
	require.NotNil(t, result.Subscription)
	require.Len(t, cli.watches, 2)
	require.Equal(t, watchReq.ResumeCursor, cli.watches[0].ResumeCursor)
	require.Equal(t, observe.WatchCursor{}, cli.watches[1].ResumeCursor,
		"fresh watch must drop the expired cursor before full-state reconcile")
	require.Len(t, cli.reads, 1)
}

func TestWatchDirectoryWithReconcileRejectsMismatchedRequest(t *testing.T) {
	_, err := WatchDirectoryWithReconcile(context.Background(), &reconcileClient{},
		observe.WatchRequest{Mount: "vol", RootInode: 10},
		model.ReadDirRequest{Mount: "vol", Parent: 11},
	)
	require.ErrorIs(t, err, model.ErrInvalidRequest)
}

func TestWatchDirectoryWithReconcileRejectsPartialBaseline(t *testing.T) {
	cli := &reconcileClient{}
	watchReq := observe.WatchRequest{Mount: "vol", RootInode: model.RootInode}

	_, err := WatchDirectoryWithReconcile(context.Background(), cli, watchReq,
		model.ReadDirRequest{Mount: "vol", Parent: model.RootInode, StartAfter: "a"},
	)
	require.ErrorIs(t, err, model.ErrInvalidRequest)

	_, err = WatchDirectoryWithReconcile(context.Background(), cli, watchReq,
		model.ReadDirRequest{Mount: "vol", Parent: model.RootInode, SnapshotVersion: 10},
	)
	require.ErrorIs(t, err, model.ErrInvalidRequest)
	require.Empty(t, cli.watches)
	require.Empty(t, cli.reads)
}
