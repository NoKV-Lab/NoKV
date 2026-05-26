// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/stretchr/testify/require"
)

func TestRaftstoreRuntimeExecutorContractOnRealCluster(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	executor := openRealClusterExecutor(t, ctx)

	req := model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "checkpoint-0001",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile, Size: 4096, Mode: 0o644},
	}
	created, err := executor.Create(ctx, req)
	require.NoError(t, err)

	record, err := executor.Lookup(ctx, model.LookupRequest{
		Mount:  req.Mount,
		Parent: req.Parent,
		Name:   req.Name,
	})
	require.NoError(t, err)
	require.Equal(t, model.DentryRecord{
		Parent: req.Parent,
		Name:   req.Name,
		Inode:  created.Inode.Inode,
		Type:   model.InodeTypeFile,
	}, record)

	entries, err := executor.ReadDir(ctx, model.ReadDirRequest{
		Mount:  req.Mount,
		Parent: req.Parent,
		Limit:  8,
	})
	require.NoError(t, err)
	require.Equal(t, []model.DentryRecord{{
		Parent: req.Parent,
		Name:   req.Name,
		Inode:  created.Inode.Inode,
		Type:   model.InodeTypeFile,
	}}, entries)

	pairs, err := executor.ReadDirPlus(ctx, model.ReadDirRequest{
		Mount:  req.Mount,
		Parent: req.Parent,
		Limit:  8,
	})
	require.NoError(t, err)
	require.Equal(t, []model.DentryAttrPair{{
		Dentry: model.DentryRecord{
			Parent: req.Parent,
			Name:   req.Name,
			Inode:  created.Inode.Inode,
			Type:   model.InodeTypeFile,
		},
		Inode: model.InodeRecord{
			Inode:     created.Inode.Inode,
			Type:      model.InodeTypeFile,
			Size:      4096,
			Mode:      0o644,
			LinkCount: 1,
		},
	}}, pairs)

	_, err = executor.Create(ctx, req)
	require.True(t, errors.Is(err, model.ErrExists), "duplicate create error = %v", err)
}

func TestRaftstoreRuntimeRenameAcrossRegionsOnRealCluster(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	executor := openSplitRealClusterExecutor(t, ctx)

	created, err := executor.Create(ctx, model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "alpha",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	})
	require.NoError(t, err)

	err = executor.RenameSubtree(ctx, model.RenameSubtreeRequest{
		Mount:      "vol",
		FromParent: model.RootInode,
		FromName:   "alpha",
		ToParent:   model.RootInode,
		ToName:     "zulu",
	})
	require.NoError(t, err)

	_, err = executor.Lookup(ctx, model.LookupRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "alpha",
	})
	require.ErrorIs(t, err, model.ErrNotFound)

	record, err := executor.Lookup(ctx, model.LookupRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "zulu",
	})
	require.NoError(t, err)
	require.Equal(t, model.DentryRecord{
		Parent: model.RootInode,
		Name:   "zulu",
		Inode:  created.Inode.Inode,
		Type:   model.InodeTypeFile,
	}, record)

	require.NoError(t, executor.Unlink(ctx, model.UnlinkRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "zulu",
	}))
	_, err = executor.Lookup(ctx, model.LookupRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "zulu",
	})
	require.ErrorIs(t, err, model.ErrNotFound)
}
