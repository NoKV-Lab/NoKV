// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package local

import (
	"context"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/stretchr/testify/require"
)

func TestLocalRuntimeRenameReplaceOverwritesFileAtomically(t *testing.T) {
	ctx := context.Background()
	rt, err := Open(ctx, Options{WorkDir: t.TempDir(), Mount: testMount()})
	require.NoError(t, err)
	defer func() { require.NoError(t, rt.Close()) }()

	oldFinal, err := rt.Executor.Create(ctx, fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "artifact",
		Attrs: fsmeta.CreateAttrs{
			Type:        fsmeta.InodeTypeFile,
			Size:        3,
			OpaqueAttrs: []byte("old-body"),
		},
	})
	require.NoError(t, err)
	staged, err := rt.Executor.Create(ctx, fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   ".stage-artifact",
		Attrs: fsmeta.CreateAttrs{
			Type:        fsmeta.InodeTypeFile,
			Size:        7,
			OpaqueAttrs: []byte("new-body"),
		},
	})
	require.NoError(t, err)

	result, err := rt.Executor.RenameReplace(ctx, fsmeta.RenameReplaceRequest{
		Mount:      "vol",
		FromParent: fsmeta.RootInode,
		FromName:   ".stage-artifact",
		ToParent:   fsmeta.RootInode,
		ToName:     "artifact",
	})
	require.NoError(t, err)
	require.True(t, result.Replaced)
	require.Equal(t, oldFinal.Dentry, result.OldDentry)
	require.Equal(t, oldFinal.Inode, result.OldInode)
	require.True(t, result.OldInodeDeleted)

	_, err = rt.Executor.Lookup(ctx, fsmeta.LookupRequest{Mount: "vol", Parent: fsmeta.RootInode, Name: ".stage-artifact"})
	require.ErrorIs(t, err, fsmeta.ErrNotFound)
	final, err := rt.Executor.LookupPlus(ctx, fsmeta.LookupRequest{Mount: "vol", Parent: fsmeta.RootInode, Name: "artifact"})
	require.NoError(t, err)
	require.Equal(t, staged.Inode.Inode, final.Inode.Inode)
	require.Equal(t, fsmeta.InodeRecord{
		Inode:       staged.Inode.Inode,
		Type:        fsmeta.InodeTypeFile,
		Size:        7,
		LinkCount:   1,
		OpaqueAttrs: []byte("new-body"),
	}, final.Inode)

	oldInodeKey, err := fsmeta.EncodeInodeKey(testMount(), oldFinal.Inode.Inode)
	require.NoError(t, err)
	readVersion, err := rt.Runner.ReserveTimestamp(ctx, 1)
	require.NoError(t, err)
	_, ok, err := rt.Runner.Get(ctx, oldInodeKey, readVersion)
	require.NoError(t, err)
	require.False(t, ok, "old overwritten inode must be removed in the same namespace mutation")
}

func TestLocalRuntimeRenameReplacePreservesRemainingHardLinks(t *testing.T) {
	ctx := context.Background()
	rt, err := Open(ctx, Options{WorkDir: t.TempDir(), Mount: testMount()})
	require.NoError(t, err)
	defer func() { require.NoError(t, rt.Close()) }()

	oldFinal, err := rt.Executor.Create(ctx, fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "artifact",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile, Size: 3},
	})
	require.NoError(t, err)
	err = rt.Executor.Link(ctx, fsmeta.LinkRequest{
		Mount:      "vol",
		FromParent: fsmeta.RootInode,
		FromName:   "artifact",
		ToParent:   fsmeta.RootInode,
		ToName:     "artifact-link",
	})
	require.NoError(t, err)
	staged, err := rt.Executor.Create(ctx, fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   ".stage-artifact",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile, Size: 7},
	})
	require.NoError(t, err)

	result, err := rt.Executor.RenameReplace(ctx, fsmeta.RenameReplaceRequest{
		Mount:      "vol",
		FromParent: fsmeta.RootInode,
		FromName:   ".stage-artifact",
		ToParent:   fsmeta.RootInode,
		ToName:     "artifact",
	})
	require.NoError(t, err)
	require.True(t, result.Replaced)
	require.Equal(t, oldFinal.Inode.Inode, result.OldInode.Inode)
	require.Equal(t, uint32(2), result.OldInode.LinkCount)
	require.False(t, result.OldInodeDeleted)

	final, err := rt.Executor.LookupPlus(ctx, fsmeta.LookupRequest{Mount: "vol", Parent: fsmeta.RootInode, Name: "artifact"})
	require.NoError(t, err)
	require.Equal(t, staged.Inode.Inode, final.Inode.Inode)
	remaining, err := rt.Executor.LookupPlus(ctx, fsmeta.LookupRequest{Mount: "vol", Parent: fsmeta.RootInode, Name: "artifact-link"})
	require.NoError(t, err)
	require.Equal(t, oldFinal.Inode.Inode, remaining.Inode.Inode)
	require.Equal(t, uint32(1), remaining.Inode.LinkCount)
}

func TestLocalRuntimeRenameReplaceMissingTargetActsLikeRename(t *testing.T) {
	ctx := context.Background()
	rt, err := Open(ctx, Options{WorkDir: t.TempDir(), Mount: testMount()})
	require.NoError(t, err)
	defer func() { require.NoError(t, rt.Close()) }()

	staged, err := rt.Executor.Create(ctx, fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   ".stage-artifact",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile, Size: 11},
	})
	require.NoError(t, err)

	result, err := rt.Executor.RenameReplace(ctx, fsmeta.RenameReplaceRequest{
		Mount:      "vol",
		FromParent: fsmeta.RootInode,
		FromName:   ".stage-artifact",
		ToParent:   fsmeta.RootInode,
		ToName:     "artifact",
	})
	require.NoError(t, err)
	require.False(t, result.Replaced)

	final, err := rt.Executor.LookupPlus(ctx, fsmeta.LookupRequest{Mount: "vol", Parent: fsmeta.RootInode, Name: "artifact"})
	require.NoError(t, err)
	require.Equal(t, staged.Inode.Inode, final.Inode.Inode)
}

func TestLocalRuntimeRenameReplaceRejectsDirectorySources(t *testing.T) {
	ctx := context.Background()
	rt, err := Open(ctx, Options{WorkDir: t.TempDir(), Mount: testMount()})
	require.NoError(t, err)
	defer func() { require.NoError(t, rt.Close()) }()

	_, err = rt.Executor.Create(ctx, fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   ".stage-artifact",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeDirectory},
	})
	require.NoError(t, err)

	_, err = rt.Executor.RenameReplace(ctx, fsmeta.RenameReplaceRequest{
		Mount:      "vol",
		FromParent: fsmeta.RootInode,
		FromName:   ".stage-artifact",
		ToParent:   fsmeta.RootInode,
		ToName:     "artifact",
	})
	require.ErrorIs(t, err, fsmeta.ErrInvalidRequest)
}

func TestLocalRuntimeRenameReplaceRejectsDirectoryTargets(t *testing.T) {
	ctx := context.Background()
	rt, err := Open(ctx, Options{WorkDir: t.TempDir(), Mount: testMount()})
	require.NoError(t, err)
	defer func() { require.NoError(t, rt.Close()) }()

	_, err = rt.Executor.Create(ctx, fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   ".stage-artifact",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	})
	require.NoError(t, err)
	_, err = rt.Executor.Create(ctx, fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "artifact",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeDirectory},
	})
	require.NoError(t, err)

	_, err = rt.Executor.RenameReplace(ctx, fsmeta.RenameReplaceRequest{
		Mount:      "vol",
		FromParent: fsmeta.RootInode,
		FromName:   ".stage-artifact",
		ToParent:   fsmeta.RootInode,
		ToName:     "artifact",
	})
	require.ErrorIs(t, err, fsmeta.ErrInvalidRequest)
}
