// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package layout

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/feichai0017/NoKV/fsmeta/model"
)

// PlacementRange describes one bootstrap byte range. Bucket ranges are fsmeta
// locality ranges; gap ranges keep the whole keyspace covered without teaching
// raftstore or coordinator about fsmeta record families.
type PlacementRange struct {
	StartKey []byte
	EndKey   []byte
	Mount    model.MountID
	MountKey model.MountKeyID
	Bucket   AffinityBucket
	Bucketed bool
}

// PlanBucketPlacement returns continuous byte ranges that isolate each
// mount-local affinity bucket while preserving full keyspace coverage.
func PlanBucketPlacement(mounts []model.MountIdentity, bucketCount int) ([]PlacementRange, error) {
	if bucketCount <= 0 || bucketCount > 1<<16 {
		return nil, fmt.Errorf("%w: affinity bucket count out of range", model.ErrInvalidRequest)
	}
	buckets := make([]PlacementRange, 0, len(mounts)*bucketCount)
	seenMount := make(map[model.MountID]struct{}, len(mounts))
	seenKey := make(map[model.MountKeyID]struct{}, len(mounts))
	for _, mount := range mounts {
		if err := model.ValidateMountIdentity(mount); err != nil {
			return nil, err
		}
		if _, ok := seenMount[mount.MountID]; ok {
			return nil, fmt.Errorf("%w: duplicate mount %q", model.ErrInvalidRequest, mount.MountID)
		}
		if _, ok := seenKey[mount.MountKeyID]; ok {
			return nil, fmt.Errorf("%w: duplicate mount_key_id %d", model.ErrInvalidRequest, mount.MountKeyID)
		}
		seenMount[mount.MountID] = struct{}{}
		seenKey[mount.MountKeyID] = struct{}{}
		for bucket := range bucketCount {
			start, end, err := EncodeBucketRange(mount, AffinityBucket(bucket))
			if err != nil {
				return nil, err
			}
			buckets = append(buckets, PlacementRange{
				StartKey: start,
				EndKey:   end,
				Mount:    mount.MountID,
				MountKey: mount.MountKeyID,
				Bucket:   AffinityBucket(bucket),
				Bucketed: true,
			})
		}
	}
	sort.Slice(buckets, func(i, j int) bool {
		return bytes.Compare(buckets[i].StartKey, buckets[j].StartKey) < 0
	})

	out := make([]PlacementRange, 0, len(buckets)*2+1)
	var cursor []byte
	for _, bucket := range buckets {
		if len(cursor) == 0 {
			if len(bucket.StartKey) > 0 {
				out = append(out, PlacementRange{
					StartKey: nil,
					EndKey:   append([]byte(nil), bucket.StartKey...),
				})
			}
		} else {
			cmp := bytes.Compare(cursor, bucket.StartKey)
			if cmp > 0 {
				return nil, fmt.Errorf("%w: overlapping fsmeta bucket ranges", model.ErrInvalidRequest)
			}
			if cmp < 0 {
				out = append(out, PlacementRange{
					StartKey: append([]byte(nil), cursor...),
					EndKey:   append([]byte(nil), bucket.StartKey...),
				})
			}
		}
		out = append(out, bucket)
		cursor = append([]byte(nil), bucket.EndKey...)
	}
	out = append(out, PlacementRange{
		StartKey: append([]byte(nil), cursor...),
		EndKey:   nil,
	})
	return out, nil
}

// BucketSplitBoundaries returns byte boundaries that can split fsmeta bootstrap
// ranges without cutting through one affinity bucket.
func BucketSplitBoundaries(mounts []model.MountIdentity, bucketCount int) ([][]byte, error) {
	ranges, err := PlanBucketPlacement(mounts, bucketCount)
	if err != nil {
		return nil, err
	}
	boundaries := make([][]byte, 0, len(ranges)-1)
	for _, r := range ranges[1:] {
		if len(r.StartKey) == 0 {
			continue
		}
		boundaries = append(boundaries, append([]byte(nil), r.StartKey...))
	}
	return boundaries, nil
}
