// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package commit

import (
	"testing"

	kv "github.com/feichai0017/NoKV/txn/storage"
	"github.com/feichai0017/NoKV/utils"
	"github.com/stretchr/testify/require"
)

func TestShardForBatchUsesFirstUserKey(t *testing.T) {
	userKey := []byte("semantic-key")
	entry := kv.NewEntry(kv.InternalKey(kv.CFDefault, userKey, 7), []byte("value"))
	defer entry.DecrRef()

	req := &Request{Entries: []*kv.Entry{entry}}
	batch := &CommitBatch{Reqs: []*CommitRequest{{Req: req}}}
	rr := 0

	shard := shardForBatch(batch, 4, &rr)

	require.Equal(t, utils.ShardForUserKey(userKey, 4), shard)
	require.Equal(t, 0, rr)
}

func TestShardForBatchRoundRobinsWhenNoUserKey(t *testing.T) {
	batch := &CommitBatch{}
	rr := 0

	shard := shardForBatch(batch, 4, &rr)

	require.Equal(t, 0, shard)
	require.Equal(t, 1, rr)
}
