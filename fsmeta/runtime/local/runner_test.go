// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package local

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/feichai0017/NoKV/engine/kv"
	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/fsmeta/backend"
	"github.com/feichai0017/NoKV/fsmeta/model"
	localdb "github.com/feichai0017/NoKV/local"
	"github.com/stretchr/testify/require"
)

func TestRunnerProvidesSnapshotReads(t *testing.T) {
	db := openTestDB(t, nil)
	defer func() { require.NoError(t, db.Close()) }()
	runner, err := NewRunner(db)
	require.NoError(t, err)

	ctx := context.Background()
	key := []byte("alpha")
	start, err := runner.ReserveTimestamp(ctx, 2)
	require.NoError(t, err)
	commit, err := runner.Mutate(ctx, key, []*backend.Mutation{{
		Op:    backend.MutationPut,
		Key:   key,
		Value: []byte("one"),
	}}, start, start+1, 0)
	require.NoError(t, err)
	require.Greater(t, commit, start)

	_, ok, err := runner.Get(ctx, key, start)
	require.NoError(t, err)
	require.False(t, ok)
	value, ok, err := runner.Get(ctx, key, commit)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("one"), value)

	rows, err := runner.Scan(ctx, []byte("a"), 8, commit)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, key, rows[0].Key)
	require.Equal(t, []byte("one"), rows[0].Value)
}

func TestRunnerRejectsCrossShardMutationWhenCallerOwnedDBIsNotAtomic(t *testing.T) {
	opts := localdb.NewDefaultOptions()
	opts.WorkDir = t.TempDir()
	opts.LSMShardCount = 4
	opts.UserKeyShapeExtractor = nil
	db := openTestDB(t, opts)
	defer func() { require.NoError(t, db.Close()) }()
	runner, err := NewRunner(db)
	require.NoError(t, err)

	first, second := keysOnDifferentLocalShards(t, db, 4)
	start, err := runner.ReserveTimestamp(context.Background(), 2)
	require.NoError(t, err)
	_, err = runner.Mutate(context.Background(), first, []*backend.Mutation{
		{Op: backend.MutationPut, Key: first, Value: []byte("a")},
		{Op: backend.MutationPut, Key: second, Value: []byte("b")},
	}, start, start+1, 0)
	require.ErrorIs(t, err, errNonAtomicApplyGroup)
}

func TestRunnerInstallMutationsAtCommitAcceptsCrossShard(t *testing.T) {
	opts := localdb.NewDefaultOptions()
	opts.WorkDir = t.TempDir()
	opts.LSMShardCount = 4
	opts.UserKeyShapeExtractor = nil
	db := openTestDB(t, opts)
	defer func() { require.NoError(t, db.Close()) }()
	runner, err := NewRunner(db)
	require.NoError(t, err)

	first, second := keysOnDifferentLocalShards(t, db, 4)
	ctx := context.Background()
	start, err := runner.ReserveTimestamp(ctx, 2)
	require.NoError(t, err)
	commit, err := runner.InstallMutationsAtCommit(ctx, first, []*backend.Mutation{
		{Op: backend.MutationPut, Key: first, Value: []byte("a")},
		{Op: backend.MutationPut, Key: second, Value: []byte("b")},
	}, start, start+1)
	require.NoError(t, err, "install must tolerate cross-shard groups that percolator commits reject")
	require.Equal(t, start+1, commit)

	got, ok, err := runner.Get(ctx, first, commit)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("a"), got)
	got, ok, err = runner.Get(ctx, second, commit)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("b"), got)
}

func TestRunnerInstallMutationsAtCommitAllowsMissingDeletePrimary(t *testing.T) {
	db := openTestDB(t, nil)
	defer func() { require.NoError(t, db.Close()) }()
	runner, err := NewRunner(db)
	require.NoError(t, err)

	ctx := context.Background()
	key := []byte("install-delete-missing")
	start, err := runner.ReserveTimestamp(ctx, 2)
	require.NoError(t, err)
	commit, err := runner.InstallMutationsAtCommit(ctx, key, []*backend.Mutation{{
		Op:  backend.MutationDelete,
		Key: key,
	}}, start, start+1)
	require.NoError(t, err, "segment install must accept tombstones for keys already absent")
	require.Equal(t, start+1, commit)

	_, ok, err := runner.Get(ctx, key, commit)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestRunnerInstallMutationsAtCommitChunksLargeGroups(t *testing.T) {
	opts := localdb.NewDefaultOptions()
	opts.WorkDir = t.TempDir()
	opts.LSMShardCount = 1
	opts.UserKeyShapeExtractor = nil
	// Squeeze the per-batch budget so the install path is forced to chunk.
	opts.MaxBatchCount = 8
	opts.MaxBatchSize = 64 << 10
	db := openTestDB(t, opts)
	defer func() { require.NoError(t, db.Close()) }()
	runner, err := NewRunner(db)
	require.NoError(t, err)

	ctx := context.Background()
	start, err := runner.ReserveTimestamp(ctx, 2)
	require.NoError(t, err)
	const n = 64
	mutations := make([]*backend.Mutation, 0, n)
	for i := range n {
		k := []byte("install-chunk-" + string(rune('A'+i/26)) + string(rune('a'+i%26)))
		mutations = append(mutations, &backend.Mutation{
			Op:    backend.MutationPut,
			Key:   append([]byte(nil), k...),
			Value: []byte("v"),
		})
	}
	commit, err := runner.InstallMutationsAtCommit(ctx, mutations[0].Key, mutations, start, start+1)
	require.NoError(t, err, "install must chunk to fit MaxBatchCount=%d", opts.MaxBatchCount)
	require.Equal(t, start+1, commit)

	for _, mutation := range mutations {
		got, ok, err := runner.Get(ctx, mutation.Key, commit)
		require.NoError(t, err)
		require.True(t, ok, "key %q must be visible after chunked install", mutation.Key)
		require.Equal(t, []byte("v"), got)
	}
}

func TestRunnerInstallMutationsAtCommitRespectsSmallMaxBatchSize(t *testing.T) {
	opts := localdb.NewDefaultOptions()
	opts.WorkDir = t.TempDir()
	opts.LSMShardCount = 1
	opts.UserKeyShapeExtractor = nil
	opts.MaxBatchCount = 10_000
	opts.MaxBatchSize = 64
	db := openTestDB(t, opts)
	defer func() { require.NoError(t, db.Close()) }()
	runner, err := NewRunner(db)
	require.NoError(t, err)

	mutations := make([]*backend.Mutation, 0, 5)
	for i := range 5 {
		mutations = append(mutations, &backend.Mutation{
			Op:    backend.MutationPut,
			Key:   []byte{byte('a' + i), byte('k')},
			Value: bytes.Repeat([]byte{byte('v')}, 18),
		})
	}
	chunks, err := runner.chunkInstallMutations(mutations)
	require.NoError(t, err)
	require.Greater(t, len(chunks), 1)
	for _, chunk := range chunks {
		var size int64
		for _, mutation := range chunk {
			size += int64(len(mutation.Key) + len(mutation.Value))
		}
		require.LessOrEqual(t, size, int64(56))
	}
}

func TestRunnerInstallMutationsAtCommitChunksDeleteGroupsBelowStrictLimit(t *testing.T) {
	opts := localdb.NewDefaultOptions()
	opts.WorkDir = t.TempDir()
	opts.LSMShardCount = 1
	opts.UserKeyShapeExtractor = nil
	opts.MaxBatchCount = 8
	opts.MaxBatchSize = 64 << 10
	db := openTestDB(t, opts)
	defer func() { require.NoError(t, db.Close()) }()
	runner, err := NewRunner(db)
	require.NoError(t, err)

	ctx := context.Background()
	start, err := runner.ReserveTimestamp(ctx, 2)
	require.NoError(t, err)
	const n = 16
	mutations := make([]*backend.Mutation, 0, n)
	for i := range n {
		k := []byte("install-delete-" + string(rune('a'+i)))
		mutations = append(mutations, &backend.Mutation{
			Op:  backend.MutationDelete,
			Key: append([]byte(nil), k...),
		})
	}
	commit, err := runner.InstallMutationsAtCommit(ctx, mutations[0].Key, mutations, start, start+1)
	require.NoError(t, err, "install chunks must stay strictly below MaxBatchCount=%d after delete expansion", opts.MaxBatchCount)
	require.Equal(t, start+1, commit)
}

func TestRunnerInstallMutationsAtCommitRejectsBadCommitVersion(t *testing.T) {
	db := openTestDB(t, nil)
	defer func() { require.NoError(t, db.Close()) }()
	runner, err := NewRunner(db)
	require.NoError(t, err)

	ctx := context.Background()
	start, err := runner.ReserveTimestamp(ctx, 1)
	require.NoError(t, err)
	// commitVersion must be strictly greater than startVersion — same
	// contract as MutateAtCommit so callers can't accidentally clobber
	// MVCC ordering through the install path.
	_, err = runner.InstallMutationsAtCommit(ctx, []byte("x"), []*backend.Mutation{
		{Op: backend.MutationPut, Key: []byte("x"), Value: []byte("v")},
	}, start, start)
	require.Error(t, err)
}

func TestRunnerInstallMutationsAtCommitEmptyGroupIsNoop(t *testing.T) {
	db := openTestDB(t, nil)
	defer func() { require.NoError(t, db.Close()) }()
	runner, err := NewRunner(db)
	require.NoError(t, err)

	ctx := context.Background()
	start, err := runner.ReserveTimestamp(ctx, 2)
	require.NoError(t, err)
	commit, err := runner.InstallMutationsAtCommit(ctx, []byte("primary"), nil, start, start+1)
	require.NoError(t, err)
	// An empty install must still complete cleanly; the chain layers that
	// produce zero mutations (e.g., a future SealedTrackingLayer that only
	// updates in-memory state) need this path to succeed.
	require.GreaterOrEqual(t, commit, uint64(0))
}

// Note: in-process retry of InstallMutationsAtCommit at the same
// commitVersion is intentionally not supported — the runner's nextTS
// advances past the commit on the first successful call and the second
// call returns commit_ts_expired. The crash-recovery story uses a fresh
// Runner instance (process restart), where nextTS is rebuilt from disk state.

func TestRunnerTryAtomicMutateAppliesPredicateCheckedGroup(t *testing.T) {
	db := openTestDB(t, nil)
	defer func() { require.NoError(t, db.Close()) }()
	runner, err := NewRunner(db)
	require.NoError(t, err)

	ctx := context.Background()
	start, err := runner.ReserveTimestamp(ctx, 2)
	require.NoError(t, err)
	handled, err := runner.TryAtomicMutate(ctx, []byte("alpha"), []*backend.Predicate{
		{Key: []byte("alpha"), Kind: backend.PredicateNotExists},
		{Key: []byte("beta"), Kind: backend.PredicateNotExists},
	}, []*backend.Mutation{
		{Op: backend.MutationPut, Key: []byte("alpha"), Value: []byte("one")},
		{Op: backend.MutationPut, Key: []byte("beta"), Value: []byte("two")},
	}, start, start+1)
	require.NoError(t, err)
	require.True(t, handled)

	value, ok, err := runner.Get(ctx, []byte("alpha"), start+1)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("one"), value)
	stats := runner.Stats()
	require.Equal(t, uint64(1), stats["atomic_mutate_total"])
	require.Equal(t, uint64(0), stats["atomic_predicate_rejected_total"])
}

func TestRunnerTryAtomicMutateRejectsValuePredicateMismatch(t *testing.T) {
	db := openTestDB(t, nil)
	defer func() { require.NoError(t, db.Close()) }()
	runner, err := NewRunner(db)
	require.NoError(t, err)

	ctx := context.Background()
	start, err := runner.ReserveTimestamp(ctx, 2)
	require.NoError(t, err)
	_, err = runner.Mutate(ctx, []byte("alpha"), []*backend.Mutation{{
		Op:    backend.MutationPut,
		Key:   []byte("alpha"),
		Value: []byte("one"),
	}}, start, start+1, 0)
	require.NoError(t, err)

	nextStart, err := runner.ReserveTimestamp(ctx, 2)
	require.NoError(t, err)
	handled, err := runner.TryAtomicMutate(ctx, []byte("beta"), []*backend.Predicate{{
		Key:           []byte("alpha"),
		Kind:          backend.PredicateValueEquals,
		ExpectedValue: []byte("stale"),
	}}, []*backend.Mutation{{
		Op:    backend.MutationPut,
		Key:   []byte("beta"),
		Value: []byte("two"),
	}}, nextStart, nextStart+1)
	require.Error(t, err)
	require.True(t, handled)
	require.True(t, nokverrors.Retryable(err))
	_, ok, getErr := runner.Get(ctx, []byte("beta"), nextStart+1)
	require.NoError(t, getErr)
	require.False(t, ok)
	require.Equal(t, uint64(1), runner.Stats()["atomic_predicate_rejected_total"])
}

func TestRunnerTryAtomicMutateFallsBackWhenCallerOwnedDBIsNotAtomic(t *testing.T) {
	opts := localdb.NewDefaultOptions()
	opts.WorkDir = t.TempDir()
	opts.LSMShardCount = 4
	opts.UserKeyShapeExtractor = nil
	db := openTestDB(t, opts)
	defer func() { require.NoError(t, db.Close()) }()
	runner, err := NewRunner(db)
	require.NoError(t, err)

	first, second := keysOnDifferentLocalShards(t, db, 4)
	start, err := runner.ReserveTimestamp(context.Background(), 2)
	require.NoError(t, err)
	handled, err := runner.TryAtomicMutate(context.Background(), first, nil, []*backend.Mutation{
		{Op: backend.MutationPut, Key: first, Value: []byte("a")},
		{Op: backend.MutationPut, Key: second, Value: []byte("b")},
	}, start, start+1)
	require.NoError(t, err)
	require.False(t, handled)
	require.Equal(t, uint64(1), runner.Stats()["atomic_apply_group_rejected_total"])
}

func TestRunnerTryAtomicMutateSerializesConcurrentSameKey(t *testing.T) {
	db := openTestDB(t, nil)
	defer func() { require.NoError(t, db.Close()) }()
	runner, err := NewRunner(db)
	require.NoError(t, err)

	ctx := context.Background()
	key := []byte("same-dentry")
	start := make(chan struct{})
	errCh := make(chan error, 32)
	var successes atomic.Uint64
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			ts, reserveErr := runner.ReserveTimestamp(ctx, 2)
			if reserveErr != nil {
				errCh <- reserveErr
				return
			}
			handled, mutateErr := runner.TryAtomicMutate(ctx, key, []*backend.Predicate{{
				Key:  key,
				Kind: backend.PredicateNotExists,
			}}, []*backend.Mutation{{
				Op:    backend.MutationPut,
				Key:   key,
				Value: []byte{byte(i)},
			}}, ts, ts+1)
			if !handled {
				errCh <- errNonAtomicApplyGroup
				return
			}
			if mutateErr == nil {
				successes.Add(1)
				return
			}
		}(i)
	}
	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}

	require.Equal(t, uint64(1), successes.Load())
	readVersion, err := runner.ReserveTimestamp(ctx, 1)
	require.NoError(t, err)
	_, ok, err := runner.Get(ctx, key, readVersion)
	require.NoError(t, err)
	require.True(t, ok)
}

func keysOnDifferentLocalShards(t *testing.T, db *localdb.DB, shards int) ([]byte, []byte) {
	t.Helper()
	var first []byte
	for i := range 1024 {
		key := []byte{byte('a' + i%26), byte(i / 26)}
		entry := kv.NewInternalEntry(kv.CFDefault, key, 1, []byte("v"), 0, 0)
		same := db.CanApplyInternalEntriesAtomically([]*kv.Entry{entry})
		entry.DecrRef()
		require.True(t, same)
		if first == nil {
			first = append([]byte(nil), key...)
			continue
		}
		a := kv.NewInternalEntry(kv.CFDefault, first, 1, []byte("a"), 0, 0)
		b := kv.NewInternalEntry(kv.CFDefault, key, 1, []byte("b"), 0, 0)
		atomic := db.CanApplyInternalEntriesAtomically([]*kv.Entry{a, b})
		a.DecrRef()
		b.DecrRef()
		if !atomic {
			return first, append([]byte(nil), key...)
		}
	}
	t.Fatalf("failed to find keys on different local shards for shard count %d", shards)
	return nil, nil
}

func TestRunnerRestartsAboveObservedTimestamp(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, testDBOptions(dir, 1))
	runner, err := NewRunner(db)
	require.NoError(t, err)
	start, err := runner.ReserveTimestamp(context.Background(), 2)
	require.NoError(t, err)
	_, err = runner.Mutate(context.Background(), []byte("k"), []*backend.Mutation{{
		Op:    backend.MutationPut,
		Key:   []byte("k"),
		Value: []byte("v"),
	}}, start, start+1, 0)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	db = openTestDB(t, testDBOptions(dir, 1))
	defer func() { require.NoError(t, db.Close()) }()
	recovered, err := NewRunner(db)
	require.NoError(t, err)
	next, err := recovered.ReserveTimestamp(context.Background(), 1)
	require.NoError(t, err)
	require.Greater(t, next, start+1)
}

func openTestDB(t *testing.T, opts *localdb.Options) *localdb.DB {
	t.Helper()
	if opts == nil {
		opts = localdb.NewDefaultOptions()
		opts.WorkDir = t.TempDir()
		opts.LSMShardCount = 1
	}
	db, err := localdb.Open(opts)
	require.NoError(t, err)
	return db
}

func testDBOptions(dir string, shards int) *localdb.Options {
	opts := localdb.NewDefaultOptions()
	opts.WorkDir = dir
	opts.LSMShardCount = shards
	return opts
}

func testMount() model.MountIdentity {
	return model.MountIdentity{MountID: "vol", MountKeyID: 1}
}
