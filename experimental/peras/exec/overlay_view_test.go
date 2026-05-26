// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"testing"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/stretchr/testify/require"
)

func TestOverlayViewGetScanFactsAndRemove(t *testing.T) {
	view := NewOverlayView()
	dentryKey, err := fsmeta.EncodeDentryKey(testMount, 9, "a")
	require.NoError(t, err)
	parentKey, err := fsmeta.EncodeInodeKey(testMount, 9)
	require.NoError(t, err)
	inodeKey, err := fsmeta.EncodeInodeKey(testMount, 10)
	require.NoError(t, err)
	op := testGeneratedCreateOpForInodes(t, 9, 10, "a")
	dentryValue := op.Effects[1].Value
	opID := OperationID{ClientID: "c", Seq: 1}
	require.NoError(t, view.Add(opID, op))

	value, deleted, ok := view.Get(dentryKey)
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, dentryValue, value)
	value[0] ^= 0xff
	viewValue, deleted, ok := view.GetView(dentryKey)
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, dentryValue, viewValue)
	present, known := view.KeyState(dentryKey)
	require.True(t, present)
	require.True(t, known)
	present, known = view.KeyState([]byte("missing"))
	require.False(t, present)
	require.False(t, known)
	require.Len(t, view.Scan(nil, 8), 3)

	plan := ReplayPlan{Operations: []ReplayOperation{{
		OpID: opID,
		Mutations: []ReplayMutation{
			{Key: parentKey},
			{Key: dentryKey},
			{Key: inodeKey},
		},
	}}}
	view.RemovePlan(plan)
	_, _, ok = view.Get(dentryKey)
	require.False(t, ok)
	overlayKeys, knownKeys, _, _, _ := view.Stats()
	require.Zero(t, overlayKeys)
	require.NotZero(t, knownKeys)
}

func TestOverlayViewScanReusesSortedIndexAcrossReads(t *testing.T) {
	view := NewOverlayView()
	for _, item := range []struct {
		name string
		seq  uint64
	}{
		{name: "c", seq: 1},
		{name: "a", seq: 2},
		{name: "b", seq: 3},
	} {
		op := testGeneratedCreateOpForInodes(t, 9, model.InodeID(20+item.seq), item.name)
		require.NoError(t, view.Add(OperationID{ClientID: "c", Seq: item.seq}, op))
	}
	prefix, err := fsmeta.EncodeDentryPrefix(testMount, 9)
	require.NoError(t, err)

	first := view.Scan(prefix, 3)
	second := view.Scan(prefix, 3)
	require.Equal(t, first, second)
	require.Len(t, first, 3)
	require.Less(t, string(first[0].Key), string(first[1].Key))
	require.Less(t, string(first[1].Key), string(first[2].Key))
}

func TestOverlayViewScanDirectoryUsesDirectoryIndex(t *testing.T) {
	view := NewOverlayView()
	require.NoError(t, view.Add(OperationID{ClientID: "c", Seq: 1}, testGeneratedCreateOpForInodes(t, 9, 21, "b")))
	require.NoError(t, view.Add(OperationID{ClientID: "c", Seq: 2}, testGeneratedCreateOpForInodes(t, 10, 22, "a")))
	require.NoError(t, view.Add(OperationID{ClientID: "c", Seq: 3}, testGeneratedCreateOpForInodes(t, 9, 23, "a")))
	prefix, err := fsmeta.EncodeDentryPrefix(testMount, 9)
	require.NoError(t, err)

	first := view.ScanDirectory(prefix, prefix, 8)
	second := view.ScanDirectory(prefix, prefix, 8)
	require.Equal(t, first, second)
	require.Len(t, first, 2)
	require.Equal(t, "a", mustDentryName(t, first[0].Key))
	require.Equal(t, "b", mustDentryName(t, first[1].Key))

	dirs, dirty := view.ReadIndexStats()
	require.Equal(t, 2, dirs)
	require.Equal(t, 1, dirty)
}

func TestOverlaySnapshotDirectoryPinsGenerationWithoutCloningValues(t *testing.T) {
	view := NewOverlayView()
	require.NoError(t, view.Add(OperationID{ClientID: "c", Seq: 1}, testGeneratedCreateOpForInodes(t, 9, 21, "a")))
	prefix, err := fsmeta.EncodeDentryPrefix(testMount, 9)
	require.NoError(t, err)
	parentKey, err := fsmeta.EncodeInodeKey(testMount, 9)
	require.NoError(t, err)
	inodeKey, err := fsmeta.EncodeInodeKey(testMount, 21)
	require.NoError(t, err)

	snapshot := view.SnapshotDirectory(testMount, prefix)
	require.NotNil(t, snapshot)
	plan := ReplayPlan{Operations: []ReplayOperation{{
		OpID: OperationID{ClientID: "c", Seq: 1},
		Mutations: []ReplayMutation{
			{Key: parentKey},
			{Key: mustDentryKey(t, 9, "a")},
			{Key: inodeKey},
		},
	}}}
	view.RemovePlan(plan)
	require.NoError(t, view.Add(OperationID{ClientID: "c", Seq: 2}, testGeneratedCreateOpForInodes(t, 9, 22, "b")))

	rows := snapshot.ScanDirectory(prefix, prefix, 8)
	require.Len(t, rows, 1)
	require.Equal(t, "a", mustDentryName(t, rows[0].Key))
	_, deleted, ok := snapshot.GetView(inodeKey)
	require.True(t, ok)
	require.False(t, deleted)
	require.True(t, snapshot.HasDirectory(prefix))
}

func TestOverlayViewPrunesRetiredHistoryBeforeGeneration(t *testing.T) {
	view := NewOverlayView()
	require.NoError(t, view.Add(OperationID{ClientID: "c", Seq: 1}, testGeneratedCreateOpForInodes(t, 9, 21, "a")))
	prefix, err := fsmeta.EncodeDentryPrefix(testMount, 9)
	require.NoError(t, err)
	snapshot := view.SnapshotDirectory(testMount, prefix)
	require.NotNil(t, snapshot)
	parentKey, err := fsmeta.EncodeInodeKey(testMount, 9)
	require.NoError(t, err)
	inodeKey, err := fsmeta.EncodeInodeKey(testMount, 21)
	require.NoError(t, err)
	plan := ReplayPlan{Operations: []ReplayOperation{{
		OpID: OperationID{ClientID: "c", Seq: 1},
		Mutations: []ReplayMutation{
			{Key: parentKey},
			{Key: mustDentryKey(t, 9, "a")},
			{Key: inodeKey},
		},
	}}}

	view.RemovePlan(plan)
	require.NotZero(t, view.HistoryLen())
	view.PruneHistoryBefore(snapshot.Generation())
	require.NotZero(t, view.HistoryLen())
	view.PruneHistoryBefore(^uint64(0))
	require.Zero(t, view.HistoryLen())
}

func TestOverlayViewDirectoryBaseEmptySurvivesCurrentEmptyForget(t *testing.T) {
	view := NewOverlayView()
	view.RememberEmptyDirectory(testMount, 9)

	require.True(t, view.DirectoryEmpty(testMount, 9))
	require.True(t, view.DirectoryBaseEmpty(testMount, 9))

	view.ForgetEmptyDirectory(testMount, 9)

	require.False(t, view.DirectoryEmpty(testMount, 9))
	require.True(t, view.DirectoryBaseEmpty(testMount, 9))
	require.True(t, view.Clone().DirectoryBaseEmpty(testMount, 9))
}

func mustDentryKey(t *testing.T, parent model.InodeID, name string) []byte {
	t.Helper()
	key, err := fsmeta.EncodeDentryKey(testMount, parent, name)
	require.NoError(t, err)
	return key
}

func mustDentryName(t *testing.T, key []byte) string {
	t.Helper()
	name, ok := fsmeta.DentryNameOfKey(key)
	require.True(t, ok)
	return name
}
