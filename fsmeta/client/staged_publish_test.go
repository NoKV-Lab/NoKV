// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"errors"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/stretchr/testify/require"
)

type stagedPublishFake struct {
	createErr error
	renameErr error
	unlinkErr error

	calls        []string
	created      model.CreateRequest
	renamed      model.RenameRequest
	unlinked     model.UnlinkRequest
	unlinkCtxErr error
}

func (f *stagedPublishFake) Create(_ context.Context, req model.CreateRequest) (model.CreateResult, error) {
	f.calls = append(f.calls, "create")
	f.created = req
	if f.createErr != nil {
		return model.CreateResult{}, f.createErr
	}
	inode := req.Attrs.InodeRecord(99)
	return model.CreateResult{
		Dentry: model.DentryRecord{Parent: req.Parent, Name: req.Name, Inode: inode.Inode, Type: inode.Type},
		Inode:  inode,
	}, nil
}

func (f *stagedPublishFake) Rename(_ context.Context, req model.RenameRequest) error {
	f.calls = append(f.calls, "rename")
	f.renamed = req
	return f.renameErr
}

func (f *stagedPublishFake) Unlink(ctx context.Context, req model.UnlinkRequest) error {
	f.calls = append(f.calls, "unlink")
	f.unlinked = req
	f.unlinkCtxErr = ctx.Err()
	return f.unlinkErr
}

func TestPublishStagedNamespaceEntryCommitsAfterPrepare(t *testing.T) {
	cli := &stagedPublishFake{}
	var prepared model.CreateResult
	req := stagedPublishRequest()

	err := PublishStagedNamespaceEntry(context.Background(), cli, req, func(_ context.Context, stage model.CreateResult) error {
		cli.calls = append(cli.calls, "prepare")
		prepared = stage
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"create", "prepare", "rename"}, cli.calls)
	require.Equal(t, model.CreateRequest{
		Mount:  "vol",
		Parent: 7,
		Name:   ".stage-artifact",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile, Mode: 0o644},
	}, cli.created)
	require.Equal(t, model.InodeID(99), prepared.Inode.Inode)
	require.Equal(t, model.RenameRequest{
		Mount:      "vol",
		FromParent: 7,
		FromName:   ".stage-artifact",
		ToParent:   8,
		ToName:     "artifact",
	}, cli.renamed)
}

func TestPublishStagedNamespaceEntryCleansUpWhenPrepareFails(t *testing.T) {
	cli := &stagedPublishFake{}
	prepareErr := errors.New("body upload failed")
	req := stagedPublishRequest()

	err := PublishStagedNamespaceEntry(context.Background(), cli, req, func(context.Context, model.CreateResult) error {
		cli.calls = append(cli.calls, "prepare")
		return prepareErr
	})
	require.ErrorIs(t, err, prepareErr)
	require.Equal(t, []string{"create", "prepare", "unlink"}, cli.calls)
	require.Equal(t, model.UnlinkRequest{Mount: "vol", Parent: 7, Name: ".stage-artifact"}, cli.unlinked)
}

func TestPublishStagedNamespaceEntryReportsCleanupFailure(t *testing.T) {
	cli := &stagedPublishFake{unlinkErr: errors.New("unlink failed")}
	prepareErr := errors.New("prepare failed")

	err := PublishStagedNamespaceEntry(context.Background(), cli, stagedPublishRequest(), func(context.Context, model.CreateResult) error {
		return prepareErr
	})
	require.ErrorIs(t, err, prepareErr)
	require.ErrorIs(t, err, cli.unlinkErr)
}

func TestPublishStagedNamespaceEntryCleanupIgnoresCanceledRequestContext(t *testing.T) {
	cli := &stagedPublishFake{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := PublishStagedNamespaceEntry(ctx, cli, stagedPublishRequest(), func(context.Context, model.CreateResult) error {
		return context.Canceled
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, []string{"create", "unlink"}, cli.calls)
	require.NoError(t, cli.unlinkCtxErr)
}

func TestPublishStagedNamespaceEntryLeavesStageOnRenameFailure(t *testing.T) {
	renameErr := errors.New("rename failed")
	cli := &stagedPublishFake{renameErr: renameErr}

	err := PublishStagedNamespaceEntry(context.Background(), cli, stagedPublishRequest(), nil)
	require.ErrorIs(t, err, renameErr)
	require.Equal(t, []string{"create", "rename"}, cli.calls)
	require.Zero(t, cli.unlinked)
}

func TestPublishStagedNamespaceEntryRejectsInvalidRequestBeforeCreate(t *testing.T) {
	cli := &stagedPublishFake{}
	req := stagedPublishRequest()
	req.StageName = ""

	err := PublishStagedNamespaceEntry(context.Background(), cli, req, nil)
	require.Error(t, err)
	require.Empty(t, cli.calls)
}

func stagedPublishRequest() StagedPublishRequest {
	return StagedPublishRequest{
		Mount:       "vol",
		StageParent: 7,
		StageName:   ".stage-artifact",
		FinalParent: 8,
		FinalName:   "artifact",
		Attrs:       model.CreateAttrs{Type: model.InodeTypeFile, Mode: 0o644},
	}
}
