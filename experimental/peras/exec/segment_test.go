// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"fmt"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/stretchr/testify/require"
)

func TestBuildPerasSegmentBuildsSortedQueryableRuns(t *testing.T) {
	plan := workspaceCreateReplayPlan(t, 3)

	segment, err := BuildPerasSegmentFromReplayPlan(plan)
	require.NoError(t, err)

	require.Equal(t, uint64(11), segment.EpochID)
	require.Equal(t, ReplayVersionRange{First: 1000, Count: 3}, segment.Versions)
	require.NotZero(t, segment.Root)
	require.Len(t, segment.Dentries, 3)
	require.Len(t, segment.Inodes, 4)
	require.Empty(t, segment.Tombstones)
	header := segment.ReadHeaderView()
	require.Equal(t, uint64(7), header.EntryCount)
	require.Equal(t, uint64(3), header.DentryCount)
	require.Equal(t, uint64(4), header.InodeCount)
	require.NotEmpty(t, header.FirstKey)
	require.NotEmpty(t, header.LastKey)

	firstDentry := segment.Dentries[0]
	value, deleted, ok := segment.Get(firstDentry.Key)
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, firstDentry.Value, value)

	entries := segment.Entries()
	require.Len(t, entries, 7)
	for i := 1; i < len(entries); i++ {
		require.LessOrEqual(t, string(entries[i-1].Key), string(entries[i].Key))
	}
	firstKey, err := segment.FirstKey()
	require.NoError(t, err)
	require.Equal(t, entries[0].Key, firstKey)
	firstKey[0] ^= 0xff
	again, err := segment.FirstKey()
	require.NoError(t, err)
	require.Equal(t, entries[0].Key, again)
}

func TestBuildPerasSegmentCoalescesLatestValueAndDelete(t *testing.T) {
	key := []byte("workspace/run/checkpoint/file")
	plan := ReplayPlan{
		EpochID: 1,
		Operations: []ReplayOperation{
			{
				OpID: OperationID{ClientID: "client-a", Seq: 1},
				Kind: model.OperationCreate,
				Mutations: []ReplayMutation{
					{Key: key, Value: []byte("v1")},
				},
			},
			{
				OpID: OperationID{ClientID: "client-a", Seq: 2},
				Kind: model.OperationUpdateInode,
				Mutations: []ReplayMutation{
					{Key: key, Value: []byte("v2")},
				},
			},
			{
				OpID: OperationID{ClientID: "client-a", Seq: 3},
				Kind: model.OperationUnlink,
				Mutations: []ReplayMutation{
					{Key: key, Delete: true},
				},
			},
		},
	}

	segment, err := BuildPerasSegmentFromReplayPlan(plan)
	require.NoError(t, err)

	_, deleted, ok := segment.Get(key)
	require.True(t, ok)
	require.True(t, deleted)
	require.Len(t, segment.Tombstones, 1)
	require.Empty(t, segment.Other)
	stats := segment.Stats()
	require.Equal(t, uint64(3), stats.InputMutationCount)
	require.Equal(t, uint64(1), stats.EntryCount)
	require.Equal(t, uint64(2), stats.CoalescedMutations)
	require.Equal(t, 3.0, stats.CompressionRatio)
}

func TestBuildPerasSegmentRecordsCompletionAndVersions(t *testing.T) {
	plan := workspaceCreateReplayPlan(t, 2)

	segment, err := BuildPerasSegmentFromReplayPlan(plan)
	require.NoError(t, err)

	first, ok := segment.Completion(OperationID{ClientID: "workspace-writer", Seq: 1})
	require.True(t, ok)
	require.Equal(t, model.OperationCreate, first.Kind)
	require.Equal(t, uint64(1000), first.Version)
	require.Equal(t, uint32(3), first.MutationCount)
	require.Equal(t, plan.Operations[0].DescriptorDigest, first.DescriptorDigest)
	require.Equal(t, plan.Operations[0].PredicateProofDigest, first.PredicateProofDigest)
	require.Equal(t, plan.Operations[0].ExecutionPlanDigest, first.ExecutionPlanDigest)
	require.NotZero(t, first.DescriptorDigest)
	require.NotZero(t, first.ExecutionPlanDigest)

	second, ok := segment.Completion(OperationID{ClientID: "workspace-writer", Seq: 2})
	require.True(t, ok)
	require.Equal(t, uint64(1001), second.Version)
}

func TestBuildPerasSegmentRootIsStableAndSensitive(t *testing.T) {
	plan := workspaceCreateReplayPlan(t, 4)
	left, err := BuildPerasSegmentFromReplayPlan(plan)
	require.NoError(t, err)
	right, err := BuildPerasSegmentFromReplayPlan(plan)
	require.NoError(t, err)
	require.Equal(t, left.Root, right.Root)

	plan.Operations[0].Mutations[1].Value = []byte("changed")
	changed, err := BuildPerasSegmentFromReplayPlan(plan)
	require.NoError(t, err)
	require.NotEqual(t, left.Root, changed.Root)

	plan = workspaceCreateReplayPlan(t, 4)
	plan.Operations[0].DescriptorDigest[0] ^= 0xff
	changed, err = BuildPerasSegmentFromReplayPlan(plan)
	require.NoError(t, err)
	require.NotEqual(t, left.Root, changed.Root)
}

func TestPerasSegmentPayloadRoundTrip(t *testing.T) {
	segment, err := BuildPerasSegmentFromReplayPlan(workspaceCreateReplayPlan(t, 3))
	require.NoError(t, err)

	payload, err := EncodePerasSegment(segment)
	require.NoError(t, err)
	digest, err := PerasSegmentPayloadDigest(payload)
	require.NoError(t, err)
	decoded, err := VerifyPerasSegmentPayload(payload, segment.Root, digest)
	require.NoError(t, err)

	require.Equal(t, segment.Root, decoded.Root)
	require.Equal(t, segment.Stats(), decoded.Stats())
	require.Equal(t, segment.ReadHeaderView(), decoded.ReadHeaderView())
	require.Equal(t, segment.Entries(), decoded.Entries())
	require.Equal(t, segment.Completions, decoded.Completions)
	value, deleted, ok := decoded.Get(segment.Dentries[0].Key)
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, segment.Dentries[0].Value, value)
}

func TestPerasSegmentPayloadRejectsTampering(t *testing.T) {
	segment, err := BuildPerasSegmentFromReplayPlan(workspaceCreateReplayPlan(t, 2))
	require.NoError(t, err)
	payload, err := EncodePerasSegment(segment)
	require.NoError(t, err)
	digest, err := PerasSegmentPayloadDigest(payload)
	require.NoError(t, err)

	tampered := append([]byte(nil), payload...)
	tampered[len(tampered)-1] ^= 0xff
	_, err = VerifyPerasSegmentPayload(tampered, segment.Root, digest)
	require.ErrorIs(t, err, ErrInvalidPerasSegment)

	wrongDigest := digest
	wrongDigest[0] ^= 0xff
	_, err = VerifyPerasSegmentPayload(payload, segment.Root, wrongDigest)
	require.ErrorIs(t, err, ErrInvalidPerasSegment)
}

func TestBuildPerasSegmentRejectsInvalidPlans(t *testing.T) {
	_, err := BuildPerasSegmentFromReplayPlan(ReplayPlan{})
	require.ErrorIs(t, err, ErrInvalidPerasSegment)

	_, err = BuildPerasSegmentFromReplayPlan(ReplayPlan{
		EpochID:  1,
		Versions: ReplayVersionRange{First: 1, Count: 2},
		Operations: []ReplayOperation{
			{OpID: OperationID{ClientID: "c", Seq: 1}, Kind: model.OperationCreate, Mutations: []ReplayMutation{{Key: []byte("k"), Value: []byte("v")}}},
		},
	})
	require.ErrorIs(t, err, ErrReplayVersionRequired)

	_, err = BuildPerasSegmentFromReplayPlan(ReplayPlan{
		EpochID: 1,
		Operations: []ReplayOperation{
			{OpID: OperationID{ClientID: "c", Seq: 1}, Kind: model.OperationCreate, Mutations: []ReplayMutation{{Key: []byte("k")}}},
		},
	})
	require.ErrorIs(t, err, ErrInvalidPerasSegment)
}

func BenchmarkBuildWorkspaceSegment1000(b *testing.B) {
	plan := workspaceCreateReplayPlan(b, 1000)

	b.ReportAllocs()
	for b.Loop() {
		segment, err := BuildPerasSegmentFromReplayPlan(plan)
		if err != nil {
			b.Fatal(err)
		}
		if len(segment.Dentries) == 0 {
			b.Fatal("empty segment")
		}
	}
}

func BenchmarkSegmentLookup(b *testing.B) {
	segment, err := BuildPerasSegmentFromReplayPlan(workspaceCreateReplayPlan(b, 1000))
	if err != nil {
		b.Fatal(err)
	}
	key := segment.Dentries[len(segment.Dentries)/2].Key

	b.ReportAllocs()
	for b.Loop() {
		_, _, ok := segment.Get(key)
		if !ok {
			b.Fatal("missing key")
		}
	}
}

func BenchmarkSegmentLookupView(b *testing.B) {
	segment, err := BuildPerasSegmentFromReplayPlan(workspaceCreateReplayPlan(b, 1000))
	if err != nil {
		b.Fatal(err)
	}
	key := segment.Dentries[len(segment.Dentries)/2].Key

	b.ReportAllocs()
	for b.Loop() {
		_, _, ok := segment.GetView(key)
		if !ok {
			b.Fatal("missing key")
		}
	}
}

func BenchmarkSegmentScan(b *testing.B) {
	segment, err := BuildPerasSegmentFromReplayPlan(workspaceCreateReplayPlan(b, 1000))
	if err != nil {
		b.Fatal(err)
	}
	start := segment.Dentries[0].Key

	b.ReportAllocs()
	for b.Loop() {
		out := segment.Scan(start, 128)
		if len(out) == 0 {
			b.Fatal("empty scan")
		}
	}
}

func BenchmarkSegmentScanView(b *testing.B) {
	segment, err := BuildPerasSegmentFromReplayPlan(workspaceCreateReplayPlan(b, 1000))
	if err != nil {
		b.Fatal(err)
	}
	start := segment.Dentries[0].Key

	b.ReportAllocs()
	for b.Loop() {
		out := segment.ScanView(start, 128)
		if len(out) == 0 {
			b.Fatal("empty scan")
		}
	}
}

func BenchmarkSegmentEntriesClone(b *testing.B) {
	segment, err := BuildPerasSegmentFromReplayPlan(workspaceCreateReplayPlan(b, 1000))
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	for b.Loop() {
		out := segment.Entries()
		if len(out) == 0 {
			b.Fatal("empty entries")
		}
	}
}

func BenchmarkSegmentEntriesView(b *testing.B) {
	segment, err := BuildPerasSegmentFromReplayPlan(workspaceCreateReplayPlan(b, 1000))
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	for b.Loop() {
		out := segment.EntriesView()
		if len(out) == 0 {
			b.Fatal("empty entries")
		}
	}
}

func workspaceCreateReplayPlan(tb testing.TB, count int) ReplayPlan {
	tb.Helper()
	mount := model.MountIdentity{MountID: "workspace", MountKeyID: 42}
	ops := make([]ReplayOperation, 0, count)
	for i := range count {
		name := fmt.Sprintf("checkpoint-%06d", i)
		program, err := compile.CompileCreateProgram(model.CreateRequest{
			Mount:  mount.MountID,
			Parent: model.RootInode,
			Name:   name,
			Attrs: model.CreateAttrs{
				Type:          model.InodeTypeFile,
				Size:          uint64(i),
				Mode:          0o644,
				CreatedUnixNs: int64(i + 1),
				UpdatedUnixNs: int64(i + 1),
			},
		}, mount, model.InodeID(1000+i))
		require.NoError(tb, err)
		parentValue, err := layout.EncodeInodeValue(model.InodeRecord{
			Inode:      model.RootInode,
			Type:       model.InodeTypeDirectory,
			LinkCount:  1,
			ChildCount: uint64(i + 1),
		})
		require.NoError(tb, err)
		materialized, err := compile.MaterializeCreate(program, compile.CreateValues{
			ParentInodeValue: parentValue,
			DentryValue:      program.Compiled.Delta.WriteEffects[1].Value,
			InodeValue:       program.Compiled.Delta.WriteEffects[2].Value,
		})
		require.NoError(tb, err)
		materialized = sealTestMaterializedOp(materialized)
		op, err := replayOperationFromMaterialized(OperationID{ClientID: "workspace-writer", Seq: uint64(i + 1)}, materialized)
		require.NoError(tb, err)
		ops = append(ops, op)
	}
	return ReplayPlan{
		EpochID:    11,
		Versions:   ReplayVersionRange{First: 1000, Count: uint64(count)},
		Operations: ops,
	}
}
