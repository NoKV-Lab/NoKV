// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package commit

import (
	"fmt"
	"testing"

	kv "github.com/feichai0017/NoKV/txn/storage"
	"github.com/feichai0017/NoKV/utils"
	"github.com/stretchr/testify/require"
)

func TestShardForBatchUsesConfiguredShardKey(t *testing.T) {
	entry := kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("semantic-key"), 7), []byte("value"))
	defer entry.DecrRef()

	req := &Request{Entries: []*kv.Entry{entry}}
	batch := &CommitBatch{Reqs: []*CommitRequest{{Req: req}}}
	rr := 0

	shardKey := keyForShard(t, 4, 3)
	shard := shardForBatch(batch, 4, &rr, func(userKey []byte) []byte {
		require.Equal(t, []byte("semantic-key"), userKey)
		return shardKey
	})

	require.Equal(t, 3, shard)
	require.Equal(t, 0, rr)
}

func TestShardForBatchFallsBackWhenShardKeyIsEmpty(t *testing.T) {
	userKey := []byte("semantic-key")
	entry := kv.NewEntry(kv.InternalKey(kv.CFDefault, userKey, 7), []byte("value"))
	defer entry.DecrRef()

	req := &Request{Entries: []*kv.Entry{entry}}
	batch := &CommitBatch{Reqs: []*CommitRequest{{Req: req}}}
	rr := 0

	shard := shardForBatch(batch, 4, &rr, func([]byte) []byte { return nil })

	require.Equal(t, utils.ShardForUserKey(userKey, 4), shard)
}

func keyForShard(t *testing.T, shards int, target int) []byte {
	t.Helper()
	for i := range 10_000 {
		key := fmt.Appendf(nil, "shard-key-%d", i)
		if utils.ShardForUserKey(key, shards) == target {
			return key
		}
	}
	t.Fatalf("no key found for shard %d", target)
	return nil
}
