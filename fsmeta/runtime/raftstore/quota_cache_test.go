// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"context"
	"testing"
	"time"

	fsmetaexec "github.com/feichai0017/NoKV/fsmeta/exec"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/stretchr/testify/require"
)

var quotaTestMount = model.MountIdentity{MountID: "vol", MountKeyID: 1}

type fakeTxnRunner struct {
	data map[string][]byte
}

func newFakeTxnRunner() *fakeTxnRunner {
	return &fakeTxnRunner{data: make(map[string][]byte)}
}

func (r *fakeTxnRunner) ReserveTimestamp(context.Context, uint64) (uint64, error) {
	return 1, nil
}

func (r *fakeTxnRunner) Get(_ context.Context, key []byte, _ uint64) ([]byte, bool, error) {
	value, ok := r.data[string(key)]
	if !ok {
		return nil, false, nil
	}
	return append([]byte(nil), value...), true, nil
}

func (r *fakeTxnRunner) BatchGet(context.Context, [][]byte, uint64) (map[string][]byte, error) {
	return nil, nil
}

func (r *fakeTxnRunner) Scan(context.Context, []byte, uint32, uint64) ([]fsmetaexec.KV, error) {
	return nil, nil
}

func (r *fakeTxnRunner) Mutate(context.Context, []byte, []*kvrpcpb.Mutation, uint64, uint64, uint64) (uint64, error) {
	return 0, nil
}

func (r *fakeTxnRunner) MutateAtCommit(context.Context, []byte, []*kvrpcpb.Mutation, uint64, uint64, uint64) (uint64, error) {
	return 0, nil
}

type fakeQuotaLookup struct {
	fences map[quotaSubject]*coordpb.QuotaFenceInfo
	calls  int
}

func (f *fakeQuotaLookup) GetQuotaFence(_ context.Context, req *coordpb.GetQuotaFenceRequest) (*coordpb.GetQuotaFenceResponse, error) {
	f.calls++
	subject := req.GetSubject()
	key := quotaSubject{mount: model.MountID(subject.GetMountId()), scope: model.InodeID(subject.GetSubtreeRoot())}
	if fence := f.fences[key]; fence != nil {
		return &coordpb.GetQuotaFenceResponse{Fence: fence}, nil
	}
	return &coordpb.GetQuotaFenceResponse{NotFound: true}, nil
}

func TestQuotaReserveWritesUsageCountersInTransaction(t *testing.T) {
	runner := newFakeTxnRunner()
	lookup := &fakeQuotaLookup{fences: map[quotaSubject]*coordpb.QuotaFenceInfo{
		{mount: "vol"}:           {Subject: &coordpb.QuotaSubject{MountId: "vol"}, LimitBytes: 8192, LimitInodes: 10, Era: 1},
		{mount: "vol", scope: 7}: {Subject: &coordpb.QuotaSubject{MountId: "vol", SubtreeRoot: 7}, LimitBytes: 4096, LimitInodes: 2, Era: 1},
	}}
	cache := &quotaCache{coord: lookup, ttl: time.Minute}

	mutations, err := cache.ReserveQuota(context.Background(), runner, []fsmetaexec.QuotaChange{{Mount: "vol", MountKeyID: 1, Scope: 7, Bytes: 1024, Inodes: 1}}, 1)
	require.NoError(t, err)
	require.Len(t, mutations, 2)

	for _, mut := range mutations {
		require.Equal(t, kvrpcpb.Mutation_Put, mut.GetOp())
		usage, err := layout.DecodeUsageValue(mut.GetValue())
		require.NoError(t, err)
		require.Equal(t, model.UsageRecord{Bytes: 1024, Inodes: 1}, usage)
	}
}

func TestQuotaReserveRejectsClusterWideLimit(t *testing.T) {
	runner := newFakeTxnRunner()
	key, err := layout.EncodeUsageKey(quotaTestMount, 0)
	require.NoError(t, err)
	value, err := layout.EncodeUsageValue(model.UsageRecord{Bytes: 900, Inodes: 1})
	require.NoError(t, err)
	runner.data[string(key)] = value
	lookup := &fakeQuotaLookup{fences: map[quotaSubject]*coordpb.QuotaFenceInfo{
		{mount: "vol"}: {Subject: &coordpb.QuotaSubject{MountId: "vol"}, LimitBytes: 1000, LimitInodes: 10, Era: 1},
	}}
	cache := &quotaCache{coord: lookup, ttl: time.Minute}

	_, err = cache.ReserveQuota(context.Background(), runner, []fsmetaexec.QuotaChange{{Mount: "vol", MountKeyID: 1, Scope: 7, Bytes: 200, Inodes: 1}}, 1)
	require.ErrorIs(t, err, model.ErrQuotaExceeded)
}

func TestQuotaReserveCoalescesRenameTransfer(t *testing.T) {
	runner := newFakeTxnRunner()
	lookup := &fakeQuotaLookup{fences: map[quotaSubject]*coordpb.QuotaFenceInfo{
		{mount: "vol"}:           {Subject: &coordpb.QuotaSubject{MountId: "vol"}, LimitBytes: 1000, LimitInodes: 10, Era: 1},
		{mount: "vol", scope: 7}: {Subject: &coordpb.QuotaSubject{MountId: "vol", SubtreeRoot: 7}, LimitBytes: 1000, LimitInodes: 10, Era: 1},
		{mount: "vol", scope: 8}: {Subject: &coordpb.QuotaSubject{MountId: "vol", SubtreeRoot: 8}, LimitBytes: 1000, LimitInodes: 10, Era: 1},
	}}
	cache := &quotaCache{coord: lookup, ttl: time.Minute}

	mutations, err := cache.ReserveQuota(context.Background(), runner, []fsmetaexec.QuotaChange{
		{Mount: "vol", MountKeyID: 1, Scope: 7, Bytes: -100, Inodes: -1},
		{Mount: "vol", MountKeyID: 1, Scope: 8, Bytes: 100, Inodes: 1},
	}, 1)
	require.NoError(t, err)
	require.Len(t, mutations, 2, "mount-wide zero delta should be elided")
}

func TestQuotaAllowsVisibleWhenNoFenceExists(t *testing.T) {
	lookup := &fakeQuotaLookup{fences: map[quotaSubject]*coordpb.QuotaFenceInfo{}}
	cache := &quotaCache{coord: lookup, ttl: time.Minute}

	ok, err := cache.AllowVisibleQuota(context.Background(), []fsmetaexec.QuotaChange{{
		Mount:      "vol",
		MountKeyID: 1,
		Scope:      7,
		Bytes:      1024,
		Inodes:     1,
	}})
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 2, lookup.calls, "mount-wide and scoped quota subjects are both checked")
}

func TestQuotaBlocksVisibleWhenFenceExists(t *testing.T) {
	lookup := &fakeQuotaLookup{fences: map[quotaSubject]*coordpb.QuotaFenceInfo{
		{mount: "vol"}: {Subject: &coordpb.QuotaSubject{MountId: "vol"}, LimitBytes: 8192, LimitInodes: 10, Era: 1},
	}}
	cache := &quotaCache{coord: lookup, ttl: time.Minute}

	ok, err := cache.AllowVisibleQuota(context.Background(), []fsmetaexec.QuotaChange{{
		Mount:      "vol",
		MountKeyID: 1,
		Scope:      7,
		Bytes:      1024,
		Inodes:     1,
	}})
	require.NoError(t, err)
	require.False(t, ok)
}

func BenchmarkQuotaAllowVisibleNoFenceCached(b *testing.B) {
	lookup := &fakeQuotaLookup{fences: map[quotaSubject]*coordpb.QuotaFenceInfo{}}
	cache := &quotaCache{coord: lookup, ttl: time.Minute}
	changes := []fsmetaexec.QuotaChange{{
		Mount:      "vol",
		MountKeyID: 1,
		Scope:      7,
		Bytes:      4096,
		Inodes:     1,
	}}
	ok, err := cache.AllowVisibleQuota(context.Background(), changes)
	require.NoError(b, err)
	require.True(b, ok)

	b.ReportAllocs()
	for b.Loop() {
		ok, err := cache.AllowVisibleQuota(context.Background(), changes)
		if err != nil || !ok {
			b.Fatalf("AllowVisibleQuota() = %v, %v", ok, err)
		}
	}
}
