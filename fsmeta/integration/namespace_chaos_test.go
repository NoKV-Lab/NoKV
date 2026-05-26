// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"context"
	"testing"
	"time"

	fsmetaexec "github.com/feichai0017/NoKV/fsmeta/exec"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/stretchr/testify/require"
)

func TestFSMetadataNamespaceChaosSurvivesGatewayRestartAndMixedMutations(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	runtime := openRealClusterRuntime(t, ctx)
	cli, cleanup := openFSMetadataClient(t, ctx, runtime.executor)
	defer func() {
		if cleanup != nil {
			cleanup()
		}
	}()

	mount := model.MountID("vol")
	_, err := cli.Create(ctx, model.CreateRequest{
		Mount:  mount,
		Parent: model.RootInode,
		Name:   "alpha",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile, Size: 64, Mode: 0o644},
	})
	require.NoError(t, err)
	_, err = cli.Create(ctx, model.CreateRequest{
		Mount:  mount,
		Parent: model.RootInode,
		Name:   "beta",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile, Size: 128, Mode: 0o644},
	})
	require.NoError(t, err)
	require.NoError(t, cli.Link(ctx, model.LinkRequest{
		Mount:      mount,
		FromParent: model.RootInode,
		FromName:   "alpha",
		ToParent:   model.RootInode,
		ToName:     "alpha-link",
	}))
	require.NoError(t, cli.Rename(ctx, model.RenameRequest{
		Mount:      mount,
		FromParent: model.RootInode,
		FromName:   "beta",
		ToParent:   model.RootInode,
		ToName:     "zeta",
	}))

	// Simulate an fsmeta gateway restart while the raftstore and coordinator
	// stay alive. The new gateway must observe the same persisted namespace
	// state and preserve inode/link invariants after more mutations.
	cleanup()
	cleanup = nil
	cli, cleanup = openFSMetadataClient(t, ctx, runtime.executor)

	require.NoError(t, cli.Unlink(ctx, model.UnlinkRequest{
		Mount:  mount,
		Parent: model.RootInode,
		Name:   "alpha",
	}))
	_, err = cli.Create(ctx, model.CreateRequest{
		Mount:  mount,
		Parent: model.RootInode,
		Name:   "gamma",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile, Size: 256, Mode: 0o644},
	})
	require.NoError(t, err)

	pairs, err := cli.ReadDirPlus(ctx, model.ReadDirRequest{
		Mount:  mount,
		Parent: model.RootInode,
		Limit:  16,
	})
	require.NoError(t, err)
	require.Equal(t, []string{"alpha-link", "gamma", "zeta"}, dentryNames(pairs))
	assertNamespaceInvariants(t, pairs)
}

func TestFSMetadataRenameSubtreePublishesSingleHandoffOnRealCluster(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	publisher := &recordingSubtreeHandoffPublisher{}
	runtime := openRealClusterRuntimeWithOptions(t, ctx, fsmetaexec.WithSubtreeHandoffPublisher(publisher))
	cli, cleanup := openFSMetadataClient(t, ctx, runtime.executor)
	defer cleanup()

	mount := model.MountID("vol")
	_, err := cli.Create(ctx, model.CreateRequest{
		Mount:  mount,
		Parent: model.RootInode,
		Name:   "source",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile, Size: 512, Mode: 0o644},
	})
	require.NoError(t, err)
	require.NoError(t, cli.RenameSubtree(ctx, model.RenameSubtreeRequest{
		Mount:      mount,
		FromParent: model.RootInode,
		FromName:   "source",
		ToParent:   model.RootInode,
		ToName:     "target",
	}))

	require.Equal(t, 1, publisher.starts)
	require.Equal(t, 1, publisher.completes)
	require.Equal(t, mount, publisher.mount)
	require.Equal(t, model.RootInode, publisher.root)
	require.NotZero(t, publisher.startFrontier)
	// Real raftstore 2PC may allocate commit_ts after handoff start. The
	// successor frontier must cover the predecessor frontier and may be later.
	require.GreaterOrEqual(t, publisher.completeFrontier, publisher.startFrontier)
}

func assertNamespaceInvariants(t *testing.T, pairs []model.DentryAttrPair) {
	t.Helper()
	seenNames := make(map[string]struct{}, len(pairs))
	linkRefs := make(map[model.InodeID]uint32)
	linkCounts := make(map[model.InodeID]uint32)
	for _, pair := range pairs {
		require.NotEmpty(t, pair.Dentry.Name)
		if _, ok := seenNames[pair.Dentry.Name]; ok {
			t.Fatalf("duplicate dentry name %q", pair.Dentry.Name)
		}
		seenNames[pair.Dentry.Name] = struct{}{}
		require.Equal(t, pair.Dentry.Inode, pair.Inode.Inode)
		require.NotZero(t, pair.Inode.LinkCount)
		linkRefs[pair.Inode.Inode]++
		linkCounts[pair.Inode.Inode] = pair.Inode.LinkCount
	}
	for inode, refs := range linkRefs {
		require.Equalf(t, refs, linkCounts[inode], "inode %d link count", inode)
	}
}

type recordingSubtreeHandoffPublisher struct {
	starts           int
	completes        int
	mount            model.MountID
	root             model.InodeID
	startFrontier    uint64
	completeFrontier uint64
}

func (p *recordingSubtreeHandoffPublisher) StartSubtreeHandoff(_ context.Context, mount model.MountID, root model.InodeID, frontier uint64) error {
	p.starts++
	p.mount = mount
	p.root = root
	p.startFrontier = frontier
	return nil
}

func (p *recordingSubtreeHandoffPublisher) CompleteSubtreeHandoff(_ context.Context, mount model.MountID, root model.InodeID, frontier uint64) error {
	p.completes++
	p.mount = mount
	p.root = root
	p.completeFrontier = frontier
	return nil
}
