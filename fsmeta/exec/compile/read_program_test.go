// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package compile

import (
	"testing"

	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/stretchr/testify/require"
)

func TestGeneratedReadProgramsCarryPointPlans(t *testing.T) {
	lookup, err := CompileLookupReadProgram(model.LookupRequest{Mount: "vol", Parent: 7, Name: "file"}, testMount)
	require.NoError(t, err)
	require.Equal(t, ReadProgramLookup, lookup.Kind)
	require.Equal(t, model.OperationLookup, lookup.Plan.Kind)
	require.Len(t, lookup.Footprint.Reads, 1)
	require.Equal(t, lookup.Key, lookup.Footprint.Reads[0].Key)
	require.Equal(t, []model.InodeID{7}, lookup.Authority.Scope.Parents)

	attr, err := CompileGetAttrReadProgram(testMount, 44)
	require.NoError(t, err)
	require.Equal(t, ReadProgramGetAttr, attr.Kind)
	require.Equal(t, model.OperationGetAttr, attr.Plan.Kind)
	require.Equal(t, layout.KeyKindInode, attr.Footprint.Reads[0].Kind)
	require.Equal(t, []model.InodeID{44}, attr.Authority.Scope.Inodes)

	session, err := CompileReadSessionProgram(testMount, 44, "writer-1")
	require.NoError(t, err)
	require.Equal(t, ReadProgramReadSession, session.Kind)
	require.Equal(t, model.OperationReadSession, session.Plan.Kind)
	require.Equal(t, layout.KeyKindSession, session.Footprint.Reads[0].Kind)
	require.Equal(t, []model.InodeID{44}, session.Authority.Scope.Inodes)

	owner, err := CompileReadSessionOwnerProgram(testMount, 44)
	require.NoError(t, err)
	require.Equal(t, ReadProgramReadSession, owner.Kind)
	require.Equal(t, model.OperationReadSession, owner.Plan.Kind)
	require.Equal(t, layout.KeyKindSession, owner.Footprint.Reads[0].Kind)
	require.Equal(t, []model.InodeID{44}, owner.Authority.Scope.Inodes)
}

func TestGeneratedReadSessionKeyProgramValidatesMountAndKeyShape(t *testing.T) {
	key, err := layout.EncodeSessionKey(testMount, 44, "writer-1")
	require.NoError(t, err)

	program, err := CompileReadSessionKeyProgram(testMount, key)
	require.NoError(t, err)
	require.Equal(t, ReadProgramReadSession, program.Kind)
	require.Equal(t, model.OperationReadSession, program.Plan.Kind)
	require.Equal(t, layout.KeyKindSession, program.Footprint.Reads[0].Kind)
	require.Equal(t, testMount.MountID, program.Authority.Scope.Mount)
	require.Equal(t, testMount.MountID, program.Plan.Mount)
	require.Equal(t, []model.InodeID{44}, program.Authority.Scope.Inodes)

	wrongMount := testMount
	wrongMount.MountKeyID++
	_, err = CompileReadSessionKeyProgram(wrongMount, key)
	require.ErrorIs(t, err, model.ErrInvalidRequest)

	dentryKey, err := layout.EncodeDentryKey(testMount, 7, "file")
	require.NoError(t, err)
	_, err = CompileReadSessionKeyProgram(testMount, dentryKey)
	require.ErrorIs(t, err, model.ErrInvalidRequest)
}

func TestGeneratedReadDirPlusInodeKeysPreserveDentryOrder(t *testing.T) {
	dentries := []model.DentryRecord{
		{Parent: 7, Name: "b", Inode: 44},
		{Parent: 7, Name: "a", Inode: 42},
	}

	keys, err := CompileReadDirPlusInodeKeys(testMount, dentries)
	require.NoError(t, err)
	require.Len(t, keys, len(dentries))

	first, err := layout.EncodeInodeKey(testMount, 44)
	require.NoError(t, err)
	second, err := layout.EncodeInodeKey(testMount, 42)
	require.NoError(t, err)
	require.Equal(t, first, keys[0])
	require.Equal(t, second, keys[1])
}

func TestGeneratedDirectoryReadPlanNormalizesOverlaySource(t *testing.T) {
	base, err := CompileDirectoryReadPlan(model.ReadDirRequest{Mount: "vol", Parent: 7, Limit: 16}, testMount, false, false)
	require.NoError(t, err)
	require.Equal(t, DirectoryReadSourceBase, base.Source)
	require.False(t, base.IncludeOverlay)
	require.False(t, base.OverlayOnly)
	require.Len(t, base.Footprint.Reads, 1)
	require.True(t, base.Footprint.HasPrefixRead)

	overlay, err := CompileDirectoryReadPlan(model.ReadDirRequest{Mount: "vol", Parent: 7, Limit: 16}, testMount, true, false)
	require.NoError(t, err)
	require.Equal(t, DirectoryReadSourceOverlay, overlay.Source)
	require.True(t, overlay.IncludeOverlay)
	require.False(t, overlay.OverlayOnly)

	overlayOnly, err := CompileDirectoryReadPlan(model.ReadDirRequest{Mount: "vol", Parent: 7, Limit: 16}, testMount, false, true)
	require.NoError(t, err)
	require.Equal(t, DirectoryReadSourceOverlayOnly, overlayOnly.Source)
	require.True(t, overlayOnly.IncludeOverlay)
	require.True(t, overlayOnly.OverlayOnly)
}
