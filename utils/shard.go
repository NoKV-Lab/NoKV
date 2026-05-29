// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package utils

// NormalizeShardCount returns the power-of-two shard count used by local write
// routing. Non-positive and single-shard configurations collapse to 1 so
// callers can safely use the result in placement diagnostics.
func NormalizeShardCount(shards int) int {
	if shards <= 1 {
		return 1
	}
	out := 1
	for out*2 <= shards {
		out *= 2
	}
	return out
}

// ShardForUserKey returns the local data-plane shard for a user key. The hash
// is shared by local apply routing and fsmeta placement decisions; changing it
// is therefore a storage-layout change, not a benchmark-only knob.
func ShardForUserKey(userKey []byte, shardCount int) int {
	shardCount = NormalizeShardCount(shardCount)
	if shardCount <= 1 || len(userKey) == 0 {
		return 0
	}
	return int(fnv1a32(userKey)) & (shardCount - 1)
}

func fnv1a32(b []byte) uint32 {
	var h uint32 = 2166136261
	for _, c := range b {
		h ^= uint32(c)
		h *= 16777619
	}
	return h
}
