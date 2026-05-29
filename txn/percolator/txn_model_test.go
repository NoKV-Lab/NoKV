// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package percolator_test

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	local "github.com/feichai0017/NoKV/local"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/feichai0017/NoKV/txn/latch"
	"github.com/feichai0017/NoKV/txn/percolator"
	"github.com/feichai0017/NoKV/utils"
	"github.com/stretchr/testify/require"
)

type txnModelRead struct {
	key    string
	readTs uint64
	value  string
	found  bool
}

type txnModelTxn struct {
	id        int
	startTs   uint64
	commitTs  uint64
	reads     []txnModelRead
	writes    map[string]txnModelWrite
	committed bool
}

type txnModelWrite struct {
	value    string
	deleted  bool
	lockOnly bool
}

type txnModelVersion struct {
	commitTs uint64
	value    string
	deleted  bool
}

type txnModel struct {
	versions map[string][]txnModelVersion
}

func TestTxnModelGeneratedScheduleIsSerializable(t *testing.T) {
	seeds := percolatorModelEnvInt("NOKV_PERCOLATOR_MODEL_SEEDS", 8)
	steps := percolatorModelEnvInt("NOKV_PERCOLATOR_MODEL_STEPS", 64)
	for seed := int64(1); seed <= int64(seeds); seed++ {
		t.Run(fmt.Sprintf("seed_%03d", seed), func(t *testing.T) {
			db := openPercolatorModelDB(t)
			latches := latch.NewManager(64)
			model := newTxnModel()
			history := runGeneratedTxnSchedule(t, db, latches, model, seed, steps)

			require.NoError(t, checkSerializableByTimestamp(history))
			reader := percolator.NewReader(db)
			for _, key := range txnModelKeys() {
				assertReaderMatchesTxnModel(t, reader, model, key, uint64(steps*20+10_000))
			}
		})
	}
}

func TestTxnModelConcurrentHistoryIsSerializable(t *testing.T) {
	seeds := percolatorModelEnvInt("NOKV_PERCOLATOR_CONCURRENT_SEEDS", 4)
	waves := percolatorModelEnvInt("NOKV_PERCOLATOR_CONCURRENT_WAVES", 8)
	batch := percolatorModelEnvInt("NOKV_PERCOLATOR_CONCURRENT_BATCH", 4)
	for seed := int64(1); seed <= int64(seeds); seed++ {
		t.Run(fmt.Sprintf("seed_%03d", seed), func(t *testing.T) {
			db := openPercolatorModelDB(t)
			latches := latch.NewManager(64)
			history := runConcurrentTxnHistory(t, db, latches, seed, waves, batch)
			require.NoError(t, checkSerializableByTimestamp(history))

			model := newTxnModel()
			committed := append([]txnModelTxn(nil), history...)
			sort.Slice(committed, func(i, j int) bool {
				if committed[i].commitTs != committed[j].commitTs {
					return committed[i].commitTs < committed[j].commitTs
				}
				return committed[i].id < committed[j].id
			})
			for _, txn := range committed {
				if txn.committed {
					model.apply(txn)
				}
			}
			assertAllKeysMatchTxnModel(t, percolator.NewReader(db), model, uint64(1<<62))
		})
	}
}

func openPercolatorModelDB(t *testing.T) *local.DB {
	t.Helper()
	opt := local.NewDefaultOptions()
	opt.WorkDir = filepath.Join(t.TempDir(), "db")
	opt.MemTableSize = 1 << 12
	db, err := local.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func runGeneratedTxnSchedule(t *testing.T, db *local.DB, latches *latch.Manager, model *txnModel, seed int64, steps int) []txnModelTxn {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	reader := percolator.NewReader(db)
	history := make([]txnModelTxn, 0, steps)
	nextStartTs := uint64(10)
	for step := range steps {
		startTs := nextStartTs
		commitTs := startTs + uint64(1+rng.Intn(3))
		nextStartTs = commitTs + uint64(1+rng.Intn(3))

		mutations := generatedTxnMutations(rng, step)
		txn := txnModelTxn{
			id:       step,
			startTs:  startTs,
			commitTs: commitTs,
			reads:    readTxnSnapshot(t, reader, mutations, startTs),
			writes:   writesFromMutations(mutations),
		}
		for _, read := range txn.reads {
			assertReadMatchesTxnModel(t, model, read)
		}

		errs := percolator.Prewrite(db, latches, &kvrpcpb.PrewriteRequest{
			Mutations:    mutations,
			PrimaryLock:  mutations[0].GetKey(),
			StartVersion: startTs,
			LockTtl:      3000,
		})
		require.Empty(t, errs, "seed=%d step=%d startTs=%d commitTs=%d", seed, step, startTs, commitTs)

		rollback := rng.Intn(100) < 25
		if rollback {
			require.Nil(t, percolator.BatchRollback(db, latches, &kvrpcpb.BatchRollbackRequest{
				Keys:         mutationKeys(mutations),
				StartVersion: startTs,
			}))
			history = append(history, txn)
			assertAllKeysMatchTxnModel(t, reader, model, commitTs+1)
			continue
		}

		require.Nil(t, percolator.Commit(db, latches, &kvrpcpb.CommitRequest{
			Keys:          mutationKeys(mutations),
			StartVersion:  startTs,
			CommitVersion: commitTs,
		}))
		require.Nil(t, percolator.Commit(db, latches, &kvrpcpb.CommitRequest{
			Keys:          mutationKeys(mutations),
			StartVersion:  startTs,
			CommitVersion: commitTs,
		}))
		txn.committed = true
		model.apply(txn)
		history = append(history, txn)
		assertAllKeysMatchTxnModel(t, reader, model, commitTs+1)
	}
	return history
}

func runConcurrentTxnHistory(t *testing.T, db *local.DB, latches *latch.Manager, seed int64, waves, batch int) []txnModelTxn {
	t.Helper()
	if batch < 2 {
		batch = 2
	}
	rng := rand.New(rand.NewSource(seed))
	var ts atomic.Uint64
	ts.Store(10)
	history := make([]txnModelTxn, 0, waves*batch)
	for wave := range waves {
		results := make([]txnModelTxn, batch)
		var wg sync.WaitGroup
		start := make(chan struct{})
		prewriteDone := make(chan struct{}, batch)
		commitStart := make(chan struct{})
		for slot := range batch {
			step := wave*batch + slot
			mutations := generatedTxnMutations(rng, step)
			rollback := rng.Intn(100) < 25
			wg.Add(1)
			go func(slot int, step int, mutations []*kvrpcpb.Mutation, rollback bool) {
				defer wg.Done()
				<-start
				results[slot] = runConcurrentTxnAttempt(t, db, latches, &ts, step, mutations, rollback, prewriteDone, commitStart)
			}(slot, step, mutations, rollback)
		}
		close(start)
		for range batch {
			<-prewriteDone
		}
		close(commitStart)
		wg.Wait()
		history = append(history, results...)
	}
	return history
}

func runConcurrentTxnAttempt(
	t *testing.T,
	db *local.DB,
	latches *latch.Manager,
	ts *atomic.Uint64,
	step int,
	mutations []*kvrpcpb.Mutation,
	rollback bool,
	prewriteDone chan<- struct{},
	commitStart <-chan struct{},
) txnModelTxn {
	t.Helper()
	startTs := ts.Add(2)
	txn := txnModelTxn{
		id:      step,
		startTs: startTs,
		reads:   readTxnSnapshot(t, percolator.NewReader(db), mutations, startTs),
		writes:  writesFromMutations(mutations),
	}
	errs := percolator.Prewrite(db, latches, &kvrpcpb.PrewriteRequest{
		Mutations:    mutations,
		PrimaryLock:  mutations[0].GetKey(),
		StartVersion: startTs,
		LockTtl:      3000,
	})
	prewriteDone <- struct{}{}
	if len(errs) != 0 {
		<-commitStart
		require.Nil(t, percolator.BatchRollback(db, latches, &kvrpcpb.BatchRollbackRequest{
			Keys:         mutationKeys(mutations),
			StartVersion: startTs,
		}))
		return txn
	}
	<-commitStart
	if rollback {
		require.Nil(t, percolator.BatchRollback(db, latches, &kvrpcpb.BatchRollbackRequest{
			Keys:         mutationKeys(mutations),
			StartVersion: startTs,
		}))
		return txn
	}
	commitTs := ts.Add(2)
	txn.commitTs = commitTs
	keyErr := percolator.Commit(db, latches, &kvrpcpb.CommitRequest{
		Keys:          mutationKeys(mutations),
		StartVersion:  startTs,
		CommitVersion: commitTs,
	})
	if keyErr != nil {
		require.Nil(t, percolator.BatchRollback(db, latches, &kvrpcpb.BatchRollbackRequest{
			Keys:         mutationKeys(mutations),
			StartVersion: startTs,
		}))
		return txn
	}
	txn.committed = true
	return txn
}

func generatedTxnMutations(rng *rand.Rand, step int) []*kvrpcpb.Mutation {
	keys := txnModelKeys()
	rng.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
	count := 1 + rng.Intn(3)
	mutations := make([]*kvrpcpb.Mutation, 0, count)
	for i := range count {
		key := []byte(keys[i])
		switch roll := rng.Intn(100); {
		case roll < 75:
			mutations = append(mutations, &kvrpcpb.Mutation{
				Op:    kvrpcpb.Mutation_Put,
				Key:   key,
				Value: fmt.Appendf(nil, "value-%03d-%s", step, keys[i]),
			})
		default:
			mutations = append(mutations, &kvrpcpb.Mutation{Op: kvrpcpb.Mutation_Delete, Key: key})
		}
	}
	return mutations
}

func readTxnSnapshot(t *testing.T, reader *percolator.Reader, mutations []*kvrpcpb.Mutation, readTs uint64) []txnModelRead {
	t.Helper()
	reads := make([]txnModelRead, 0, len(mutations))
	seen := make(map[string]struct{})
	for _, mutation := range mutations {
		key := string(mutation.GetKey())
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		value, _, err := reader.GetValue(mutation.GetKey(), readTs)
		read := txnModelRead{key: key, readTs: readTs}
		if err == nil {
			read.value = string(value)
			read.found = true
		} else {
			require.True(t, errors.Is(err, utils.ErrKeyNotFound), "unexpected read error for key=%s ts=%d: %v", key, readTs, err)
		}
		reads = append(reads, read)
	}
	return reads
}

func writesFromMutations(mutations []*kvrpcpb.Mutation) map[string]txnModelWrite {
	writes := make(map[string]txnModelWrite, len(mutations))
	for _, mutation := range mutations {
		key := string(mutation.GetKey())
		switch mutation.GetOp() {
		case kvrpcpb.Mutation_Put:
			writes[key] = txnModelWrite{value: string(mutation.GetValue())}
		case kvrpcpb.Mutation_Delete:
			writes[key] = txnModelWrite{deleted: true}
		case kvrpcpb.Mutation_Lock:
			writes[key] = txnModelWrite{lockOnly: true}
		}
	}
	return writes
}

func checkSerializableByTimestamp(history []txnModelTxn) error {
	committed := append([]txnModelTxn(nil), history...)
	sort.Slice(committed, func(i, j int) bool {
		if committed[i].commitTs != committed[j].commitTs {
			return committed[i].commitTs < committed[j].commitTs
		}
		return committed[i].id < committed[j].id
	})
	model := newTxnModel()
	for _, txn := range committed {
		if txn.committed {
			model.apply(txn)
		}
	}
	for _, txn := range history {
		for _, read := range txn.reads {
			value, found := model.visible(read.key, read.readTs)
			if found != read.found || value != read.value {
				return fmt.Errorf("txn %d read key=%s ts=%d got=(%v,%q) serial=(%v,%q)", txn.id, read.key, read.readTs, read.found, read.value, found, value)
			}
		}
	}
	return nil
}

func newTxnModel() *txnModel {
	return &txnModel{versions: make(map[string][]txnModelVersion)}
}

func (m *txnModel) apply(txn txnModelTxn) {
	for key, write := range txn.writes {
		if write.lockOnly {
			continue
		}
		m.versions[key] = append(m.versions[key], txnModelVersion{
			commitTs: txn.commitTs,
			value:    write.value,
			deleted:  write.deleted,
		})
	}
	for key := range txn.writes {
		sort.Slice(m.versions[key], func(i, j int) bool {
			return m.versions[key][i].commitTs < m.versions[key][j].commitTs
		})
	}
}

func (m *txnModel) visible(key string, readTs uint64) (string, bool) {
	versions := m.versions[key]
	for i := len(versions) - 1; i >= 0; i-- {
		version := versions[i]
		if version.commitTs > readTs {
			continue
		}
		if version.deleted {
			return "", false
		}
		return version.value, true
	}
	return "", false
}

func assertReaderMatchesTxnModel(t *testing.T, reader *percolator.Reader, model *txnModel, key string, readTs uint64) {
	t.Helper()
	got, _, err := reader.GetValue([]byte(key), readTs)
	want, found := model.visible(key, readTs)
	if !found {
		require.ErrorIs(t, err, utils.ErrKeyNotFound, "key=%s ts=%d", key, readTs)
		return
	}
	require.NoError(t, err, "key=%s ts=%d", key, readTs)
	require.Equal(t, want, string(got), "key=%s ts=%d", key, readTs)
}

func assertAllKeysMatchTxnModel(t *testing.T, reader *percolator.Reader, model *txnModel, readTs uint64) {
	t.Helper()
	for _, key := range txnModelKeys() {
		assertReaderMatchesTxnModel(t, reader, model, key, readTs)
	}
}

func assertReadMatchesTxnModel(t *testing.T, model *txnModel, read txnModelRead) {
	t.Helper()
	want, found := model.visible(read.key, read.readTs)
	require.Equal(t, found, read.found, "key=%s ts=%d", read.key, read.readTs)
	require.Equal(t, want, read.value, "key=%s ts=%d", read.key, read.readTs)
}

func mutationKeys(mutations []*kvrpcpb.Mutation) [][]byte {
	keys := make([][]byte, 0, len(mutations))
	for _, mutation := range mutations {
		keys = append(keys, append([]byte(nil), mutation.GetKey()...))
	}
	return keys
}

func txnModelKeys() []string {
	return []string{"key-a", "key-b", "key-c", "key-d", "key-e"}
}

func percolatorModelEnvInt(name string, fallback int) int {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}
