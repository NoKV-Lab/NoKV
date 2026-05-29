// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"context"
	"testing"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/fsmeta/backend"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/stretchr/testify/require"
)

type fakeRunnerKV struct {
	getResp      *kvrpcpb.GetResponse
	batchGetResp map[string]*kvrpcpb.GetResponse
}

func (f *fakeRunnerKV) Get(context.Context, []byte, uint64) (*kvrpcpb.GetResponse, error) {
	return f.getResp, nil
}

func (f *fakeRunnerKV) BatchGet(context.Context, [][]byte, uint64) (map[string]*kvrpcpb.GetResponse, error) {
	return f.batchGetResp, nil
}

func (f *fakeRunnerKV) Scan(context.Context, []byte, uint32, uint64) ([]*kvrpcpb.KV, error) {
	return nil, nil
}

func (f *fakeRunnerKV) Mutate(context.Context, []byte, []*kvrpcpb.Mutation, uint64, uint64, uint64) error {
	return nil
}

type fakeCommitTimestampKV struct {
	fakeRunnerKV
	commitVersion uint64
}

func (f *fakeCommitTimestampKV) MutateWithCommitTimestamp(ctx context.Context, _ []byte, _ []*kvrpcpb.Mutation, _ uint64, _ uint64, allocateCommitVersion func(context.Context) (uint64, error)) (uint64, error) {
	ts, err := allocateCommitVersion(ctx)
	if err != nil {
		return 0, err
	}
	f.commitVersion = ts
	return ts, nil
}

type fakeRunnerTSO struct {
	resp  *coordpb.TsoResponse
	err   error
	errs  []error
	calls int
}

func (f *fakeRunnerTSO) Tso(context.Context, *coordpb.TsoRequest) (*coordpb.TsoResponse, error) {
	f.calls++
	if len(f.errs) > 0 {
		err := f.errs[0]
		f.errs = f.errs[1:]
		if err != nil {
			return nil, err
		}
	}
	return f.resp, f.err
}

func TestNewRunnerClassifiesMissingDependencies(t *testing.T) {
	_, err := NewRunner(nil, &fakeRunnerTSO{})
	require.Error(t, err)
	require.True(t, nokverrors.IsKind(err, nokverrors.KindInvalidArgument))

	_, err = NewRunner(&fakeRunnerKV{}, nil)
	require.Error(t, err)
	require.True(t, nokverrors.IsKind(err, nokverrors.KindInvalidArgument))
}

func TestReserveTimestampClassifiesBoundaryErrors(t *testing.T) {
	runner, err := NewRunner(&fakeRunnerKV{}, &fakeRunnerTSO{resp: &coordpb.TsoResponse{Timestamp: 10, Count: 2}})
	require.NoError(t, err)

	_, err = runner.ReserveTimestamp(context.Background(), 0)
	require.Error(t, err)
	require.True(t, nokverrors.IsKind(err, nokverrors.KindInvalidArgument))

	for _, tc := range []struct {
		name string
		resp *coordpb.TsoResponse
	}{
		{name: "nil response"},
		{name: "wrong count", resp: &coordpb.TsoResponse{Timestamp: 10, Count: 1}},
		{name: "zero timestamp", resp: &coordpb.TsoResponse{Timestamp: 0, Count: 2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runner, err := NewRunner(&fakeRunnerKV{}, &fakeRunnerTSO{resp: tc.resp})
			require.NoError(t, err)

			_, err = runner.ReserveTimestamp(context.Background(), 2)
			require.Error(t, err)
			require.True(t, nokverrors.IsKind(err, nokverrors.KindProtocolViolation))
		})
	}

	ts, err := runner.ReserveTimestamp(context.Background(), 2)
	require.NoError(t, err)
	require.Equal(t, uint64(10), ts)
}

func TestRunnerGetKeyErrorsPreserveKind(t *testing.T) {
	locked := &kvrpcpb.KeyError{Locked: &kvrpcpb.Locked{
		Key:         []byte("k"),
		PrimaryLock: []byte("p"),
		LockVersion: 7,
		LockTtl:     100,
	}}
	runner, err := NewRunner(&fakeRunnerKV{getResp: &kvrpcpb.GetResponse{Error: locked}}, &fakeRunnerTSO{})
	require.NoError(t, err)

	_, _, err = runner.Get(context.Background(), []byte("k"), 9)
	require.Error(t, err)
	require.True(t, nokverrors.IsKind(err, nokverrors.KindLockConflict))
	require.True(t, nokverrors.Retryable(err))
}

func TestRunnerBatchGetKeyErrorsPreserveKind(t *testing.T) {
	expired := &kvrpcpb.KeyError{CommitTsExpired: &kvrpcpb.CommitTsExpired{
		Key:         []byte("k"),
		CommitTs:    10,
		MinCommitTs: 11,
	}}
	runner, err := NewRunner(&fakeRunnerKV{batchGetResp: map[string]*kvrpcpb.GetResponse{
		"k": {Error: expired},
	}}, &fakeRunnerTSO{})
	require.NoError(t, err)

	_, err = runner.BatchGet(context.Background(), [][]byte{[]byte("k")}, 9)
	require.Error(t, err)
	require.True(t, nokverrors.IsKind(err, nokverrors.KindCommitTsExpired))
	require.True(t, nokverrors.Retryable(err))
}

func TestRunnerTryAtomicMutateRecordsUnsupportedKV(t *testing.T) {
	runner, err := NewRunner(&fakeRunnerKV{}, &fakeRunnerTSO{resp: &coordpb.TsoResponse{Timestamp: 10, Count: 2}})
	require.NoError(t, err)

	handled, err := runner.TryAtomicMutate(context.Background(), []byte("p"), nil, []*backend.Mutation{{
		Op:  backend.MutationPut,
		Key: []byte("p"),
	}}, 10, 11)
	require.NoError(t, err)
	require.False(t, handled)
	stats := runner.Stats()
	require.Equal(t, uint64(1), stats["atomic_runner_unsupported_total"])
}

func TestRunnerStatsExposeTSOCoalescer(t *testing.T) {
	runner, err := NewRunner(&fakeRunnerKV{}, &fakeRunnerTSO{resp: &coordpb.TsoResponse{Timestamp: 10, Count: 1}})
	require.NoError(t, err)

	ts, err := runner.ReserveTimestamp(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, uint64(10), ts)

	stats := runner.Stats()
	tsoStats, ok := stats["tso"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, uint64(1), tsoStats["tso_coalesce_requests_total"])
	require.Equal(t, uint64(1), tsoStats["tso_coalesce_batches_total"])
	require.Equal(t, uint64(0), tsoStats["tso_direct_requests_total"])
	require.Equal(t, uint64(1), tsoStats["tso_coalesce_allocated_total"])
}

func TestRunnerMutateAllocatesCommitTimestampAfterPrewrite(t *testing.T) {
	kv := &fakeCommitTimestampKV{}
	runner, err := NewRunner(kv, &fakeRunnerTSO{resp: &coordpb.TsoResponse{Timestamp: 20, Count: 1}})
	require.NoError(t, err)

	actual, err := runner.Mutate(context.Background(), []byte("p"), []*backend.Mutation{{
		Op:  backend.MutationPut,
		Key: []byte("p"),
	}}, 10, 11, 3000)
	require.NoError(t, err)
	require.Equal(t, uint64(20), actual)
	require.Equal(t, uint64(20), kv.commitVersion)
}

var _ KVClient = (*fakeRunnerKV)(nil)
var _ TSOClient = (*fakeRunnerTSO)(nil)
var _ backend.Store = (*Runner)(nil)
