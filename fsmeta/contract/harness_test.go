// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package contract

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/fsmeta/backend"
	fsmetaexec "github.com/feichai0017/NoKV/fsmeta/exec"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/stretchr/testify/require"
)

var contractMountIdentity = model.MountIdentity{MountID: "vol", MountKeyID: 1}

type contractMountResolver struct{}

func (contractMountResolver) ResolveMount(context.Context, model.MountID) (fsmetaexec.MountAdmission, error) {
	return fsmetaexec.MountAdmission{
		MountID:       contractMountIdentity.MountID,
		MountKeyID:    contractMountIdentity.MountKeyID,
		RootInode:     model.RootInode,
		SchemaVersion: 1,
	}, nil
}

func TestFSMetaExecutorModelContract(t *testing.T) {
	seeds := envInt("NOKV_CONTRACT_SEEDS", 16)
	steps := envInt("NOKV_CONTRACT_STEPS", 80)
	for seed := int64(1); seed <= int64(seeds); seed++ {
		t.Run(fmt.Sprintf("seed_%03d", seed), func(t *testing.T) {
			state := NewModel("vol")
			runner := newVersionedRunner()
			ops := GenerateScript(seed, steps)
			executor, err := fsmetaexec.New(runner,
				fsmetaexec.WithMountResolver(contractMountResolver{}),
				fsmetaexec.WithInodeAllocator(newScriptInodeAllocator(ops)),
				fsmetaexec.WithClock(func() time.Time {
					return time.Unix(0, state.NowUnixNs)
				}),
			)
			require.NoError(t, err)

			err = Run(context.Background(), executor, state, ops)
			require.NoError(t, err, "seed=%d steps=%d", seed, steps)
		})
	}
}

func envInt(name string, fallback int) int {
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

type versionedRunner struct {
	mu               sync.Mutex
	nextTS           uint64
	latestObservedTS uint64
	data             map[string][]versionedValue
}

type versionedValue struct {
	version uint64
	value   []byte
	deleted bool
}

type versionedTxnError struct {
	errors []*kvrpcpb.KeyError
}

func (e versionedTxnError) Error() string {
	return "fsmeta/contract: transaction contention"
}

func (e versionedTxnError) KeyErrors() []*kvrpcpb.KeyError {
	return e.errors
}

func newVersionedRunner() *versionedRunner {
	runner := &versionedRunner{
		nextTS: 1,
		data:   make(map[string][]versionedValue),
	}
	seedVersionedInode(runner, model.InodeRecord{
		Inode:     model.RootInode,
		Type:      model.InodeTypeDirectory,
		Mode:      0o755,
		LinkCount: 1,
	}, 0)
	return runner
}

func seedVersionedInode(runner *versionedRunner, record model.InodeRecord, version uint64) {
	key, err := layout.EncodeInodeKey(contractMountIdentity, record.Inode)
	if err != nil {
		panic(err)
	}
	value, err := layout.EncodeInodeValue(record)
	if err != nil {
		panic(err)
	}
	runner.data[string(key)] = append(runner.data[string(key)], versionedValue{
		version: version,
		value:   value,
	})
}

func (r *versionedRunner) ReserveTimestamp(_ context.Context, count uint64) (uint64, error) {
	if count == 0 {
		return 0, errors.New("zero timestamp reservation")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	first := r.nextTS
	r.nextTS += count
	last := first + count - 1
	if last > r.latestObservedTS {
		r.latestObservedTS = last
	}
	return first, nil
}

func (r *versionedRunner) Get(_ context.Context, key []byte, version uint64) ([]byte, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	value, ok := r.visibleLocked(key, version)
	return value, ok, nil
}

func (r *versionedRunner) BatchGet(_ context.Context, keys [][]byte, version uint64) (map[string][]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string][]byte, len(keys))
	for _, key := range keys {
		if value, ok := r.visibleLocked(key, version); ok {
			out[string(key)] = value
		}
	}
	return out, nil
}

func (r *versionedRunner) Scan(_ context.Context, startKey []byte, limit uint32, version uint64) ([]backend.KV, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	keys := make([][]byte, 0, len(r.data))
	for key := range r.data {
		raw := []byte(key)
		if bytes.Compare(raw, startKey) < 0 {
			continue
		}
		if _, ok := r.visibleLocked(raw, version); ok {
			keys = append(keys, append([]byte(nil), raw...))
		}
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 })
	out := make([]backend.KV, 0, limit)
	for _, key := range keys {
		if uint32(len(out)) >= limit {
			break
		}
		value, _ := r.visibleLocked(key, version)
		out = append(out, backend.KV{
			Key:   append([]byte(nil), key...),
			Value: value,
		})
	}
	return out, nil
}

func (r *versionedRunner) Mutate(_ context.Context, primary []byte, mutations []*backend.Mutation, startVersion, commitVersion, _ uint64) (uint64, error) {
	return r.applyMutations(primary, mutations, startVersion, commitVersion, true)
}

func (r *versionedRunner) MutateAtCommit(_ context.Context, primary []byte, mutations []*backend.Mutation, startVersion, commitVersion, _ uint64) (uint64, error) {
	return r.applyMutations(primary, mutations, startVersion, commitVersion, false)
}

func (r *versionedRunner) applyMutations(primary []byte, mutations []*backend.Mutation, startVersion, commitVersion uint64, allowCommitPush bool) (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// The contract fake has no lock table, so it models Percolator's
	// min-commit push by placing late commits after any timestamp that was
	// allocated while the transaction was in flight.
	effectiveCommitVersion := commitVersion
	if allowCommitPush && r.latestObservedTS >= effectiveCommitVersion {
		effectiveCommitVersion = r.latestObservedTS + 1
		if r.nextTS <= effectiveCommitVersion {
			r.nextTS = effectiveCommitVersion + 1
		}
	}
	for _, mut := range mutations {
		if latest, ok := r.latestVersionLocked(mut.Key); ok && latest > startVersion {
			return 0, versionedTxnError{errors: []*kvrpcpb.KeyError{{
				CommitTsExpired: &kvrpcpb.CommitTsExpired{
					Key:         append([]byte(nil), mut.Key...),
					CommitTs:    commitVersion,
					MinCommitTs: latest + 1,
				},
			}}}
		}
		if mut.AssertionNotExist {
			if _, ok := r.visibleLocked(mut.Key, startVersion); ok {
				return 0, model.ErrExists
			}
			if _, ok := r.visibleLatestLocked(mut.Key); ok {
				return 0, model.ErrExists
			}
		}
		if bytes.Equal(mut.Key, primary) && mut.Op == backend.MutationDelete {
			if _, ok := r.visibleLatestLocked(mut.Key); !ok {
				return 0, model.ErrNotFound
			}
		}
	}
	for _, mut := range mutations {
		key := string(mut.Key)
		switch mut.Op {
		case backend.MutationPut:
			r.data[key] = append(r.data[key], versionedValue{
				version: effectiveCommitVersion,
				value:   append([]byte(nil), mut.Value...),
			})
		case backend.MutationDelete:
			r.data[key] = append(r.data[key], versionedValue{
				version: effectiveCommitVersion,
				deleted: true,
			})
		default:
			return 0, model.ErrInvalidRequest
		}
	}
	return effectiveCommitVersion, nil
}

func TestVersionedRunnerDelaysPreallocatedCommitPastConcurrentRead(t *testing.T) {
	ctx := context.Background()
	runner := newVersionedRunner()

	epsilonKey, err := layout.EncodeDentryKey(contractMountIdentity, model.RootInode, "epsilon")
	require.NoError(t, err)
	etaKey, err := layout.EncodeDentryKey(contractMountIdentity, model.RootInode, "eta")
	require.NoError(t, err)
	inodeKey, err := layout.EncodeInodeKey(contractMountIdentity, 10)
	require.NoError(t, err)
	epsilonValue, err := layout.EncodeDentryValue(model.DentryRecord{
		Parent: model.RootInode,
		Name:   "epsilon",
		Inode:  10,
		Type:   model.InodeTypeFile,
	})
	require.NoError(t, err)
	etaValue, err := layout.EncodeDentryValue(model.DentryRecord{
		Parent: model.RootInode,
		Name:   "eta",
		Inode:  10,
		Type:   model.InodeTypeFile,
	})
	require.NoError(t, err)
	inodeValueOneLink, err := layout.EncodeInodeValue(model.InodeRecord{
		Inode:     10,
		Type:      model.InodeTypeFile,
		LinkCount: 1,
	})
	require.NoError(t, err)
	inodeValueTwoLinks, err := layout.EncodeInodeValue(model.InodeRecord{
		Inode:     10,
		Type:      model.InodeTypeFile,
		LinkCount: 2,
	})
	require.NoError(t, err)

	seedStart, err := runner.ReserveTimestamp(ctx, 2)
	require.NoError(t, err)
	_, err = runner.Mutate(ctx, epsilonKey, []*backend.Mutation{
		{Op: backend.MutationPut, Key: epsilonKey, Value: epsilonValue},
		{Op: backend.MutationPut, Key: inodeKey, Value: inodeValueOneLink},
	}, seedStart, seedStart+1, 0)
	require.NoError(t, err)

	linkStart, err := runner.ReserveTimestamp(ctx, 2)
	require.NoError(t, err)
	readVersion, err := runner.ReserveTimestamp(ctx, 1)
	require.NoError(t, err)
	_, err = runner.Mutate(ctx, etaKey, []*backend.Mutation{
		{Op: backend.MutationPut, Key: etaKey, Value: etaValue, AssertionNotExist: true},
		{Op: backend.MutationPut, Key: inodeKey, Value: inodeValueTwoLinks},
	}, linkStart, linkStart+1, 0)
	require.NoError(t, err)

	_, ok, err := runner.Get(ctx, etaKey, readVersion)
	require.NoError(t, err)
	require.False(t, ok)
	values, err := runner.BatchGet(ctx, [][]byte{inodeKey}, readVersion)
	require.NoError(t, err)
	inode, err := layout.DecodeInodeValue(values[string(inodeKey)])
	require.NoError(t, err)
	require.Equal(t, uint32(1), inode.LinkCount)

	afterVersion, err := runner.ReserveTimestamp(ctx, 1)
	require.NoError(t, err)
	_, ok, err = runner.Get(ctx, etaKey, afterVersion)
	require.NoError(t, err)
	require.True(t, ok)
	values, err = runner.BatchGet(ctx, [][]byte{inodeKey}, afterVersion)
	require.NoError(t, err)
	inode, err = layout.DecodeInodeValue(values[string(inodeKey)])
	require.NoError(t, err)
	require.Equal(t, uint32(2), inode.LinkCount)
}

func TestVersionedRunnerRejectsStaleConcurrentMutation(t *testing.T) {
	ctx := context.Background()
	runner := newVersionedRunner()
	key := []byte("owner-key")

	firstStart, err := runner.ReserveTimestamp(ctx, 2)
	require.NoError(t, err)
	staleStart, err := runner.ReserveTimestamp(ctx, 2)
	require.NoError(t, err)
	_, err = runner.Mutate(ctx, key, []*backend.Mutation{{
		Op:    backend.MutationPut,
		Key:   key,
		Value: []byte("first"),
	}}, firstStart, firstStart+1, 0)
	require.NoError(t, err)

	_, err = runner.Mutate(ctx, key, []*backend.Mutation{{
		Op:    backend.MutationPut,
		Key:   key,
		Value: []byte("stale"),
	}}, staleStart, staleStart+1, 0)
	require.Error(t, err)
	var carrier interface {
		KeyErrors() []*kvrpcpb.KeyError
	}
	require.ErrorAs(t, err, &carrier)
	require.NotEmpty(t, carrier.KeyErrors())
	require.NotNil(t, carrier.KeyErrors()[0].GetCommitTsExpired())
}

func (r *versionedRunner) visibleLatestLocked(key []byte) ([]byte, bool) {
	return r.visibleLocked(key, ^uint64(0))
}

func (r *versionedRunner) latestVersionLocked(key []byte) (uint64, bool) {
	versions := r.data[string(key)]
	var latest uint64
	for _, candidate := range versions {
		if candidate.version > latest {
			latest = candidate.version
		}
	}
	return latest, latest != 0
}

func (r *versionedRunner) visibleLocked(key []byte, version uint64) ([]byte, bool) {
	versions := r.data[string(key)]
	var (
		best      versionedValue
		bestFound bool
	)
	for _, candidate := range versions {
		if candidate.version > version {
			continue
		}
		if !bestFound || candidate.version > best.version {
			best = candidate
			bestFound = true
		}
	}
	if !bestFound || best.deleted {
		return nil, false
	}
	return append([]byte(nil), best.value...), true
}
