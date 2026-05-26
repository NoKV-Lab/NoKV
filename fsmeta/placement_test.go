// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package fsmeta

import (
	"bytes"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/stretchr/testify/require"
)

func TestPlanBucketPlacementReturnsContinuousRanges(t *testing.T) {
	ranges, err := PlanBucketPlacement([]model.MountIdentity{
		{MountID: "default", MountKeyID: 1},
		{MountID: "fsmeta-bench", MountKeyID: 2},
	}, DefaultAffinityBucketCount)
	require.NoError(t, err)
	require.Len(t, ranges, DefaultAffinityBucketCount*2+3)

	for i := 0; i+1 < len(ranges); i++ {
		require.Equal(t, ranges[i].EndKey, ranges[i+1].StartKey)
		if len(ranges[i].EndKey) != 0 {
			require.Less(t, bytes.Compare(ranges[i].StartKey, ranges[i].EndKey), 0)
		}
	}
	require.Empty(t, ranges[0].StartKey)
	require.Empty(t, ranges[len(ranges)-1].EndKey)
}

func TestPlanBucketPlacementKeepsBucketKeysInsideBucketRange(t *testing.T) {
	ranges, err := PlanBucketPlacement([]model.MountIdentity{testMount}, DefaultAffinityBucketCount)
	require.NoError(t, err)

	var bucketRange PlacementRange
	for _, r := range ranges {
		if r.Bucketed && r.Bucket == 7 {
			bucketRange = r
			break
		}
	}
	require.True(t, bucketRange.Bucketed)

	inode := findInodeOnBucket(t, testMount, 7)
	key, err := EncodeInodeKey(testMount, inode)
	require.NoError(t, err)
	require.GreaterOrEqual(t, bytes.Compare(key, bucketRange.StartKey), 0)
	require.Less(t, bytes.Compare(key, bucketRange.EndKey), 0)
}

func TestPlanBucketPlacementRejectsInvalidInput(t *testing.T) {
	_, err := PlanBucketPlacement([]model.MountIdentity{testMount}, 0)
	require.ErrorIs(t, err, model.ErrInvalidRequest)

	_, err = PlanBucketPlacement([]model.MountIdentity{testMount, testMount}, DefaultAffinityBucketCount)
	require.ErrorIs(t, err, model.ErrInvalidRequest)
}

func TestBucketSplitBoundariesUseBucketEdges(t *testing.T) {
	boundaries, err := BucketSplitBoundaries([]model.MountIdentity{testMount}, 4)
	require.NoError(t, err)
	require.Len(t, boundaries, 5)

	bucket0, _, err := EncodeBucketRange(testMount, 0)
	require.NoError(t, err)
	require.Equal(t, bucket0, boundaries[0])

	_, bucket3End, err := EncodeBucketRange(testMount, 3)
	require.NoError(t, err)
	require.Equal(t, bucket3End, boundaries[4])
}
