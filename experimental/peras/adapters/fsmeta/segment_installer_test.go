// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package fsmeta

import (
	"context"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	nokverrors "github.com/feichai0017/NoKV/errors"
	fsperas "github.com/feichai0017/NoKV/experimental/peras/exec"
	runtimeperas "github.com/feichai0017/NoKV/experimental/peras/runtime"
	fsmetamodel "github.com/feichai0017/NoKV/fsmeta"
	fsmetawatch "github.com/feichai0017/NoKV/fsmeta/exec/watch"
	"github.com/feichai0017/NoKV/fsmeta/model"
	stable "github.com/feichai0017/NoKV/fsmeta/runtime/raftstore"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/feichai0017/NoKV/raftstore/client"
	"github.com/stretchr/testify/require"
)

func TestValidatePreparedSegmentInstallResponseChecksVersionAndAppliedCount(t *testing.T) {
	req := &kvrpcpb.InstallPreparedMVCCEntriesRequest{
		CommitVersion: 10,
		Entries: []*kvrpcpb.PreparedMVCCEntry{
			{Key: []byte("a"), Version: 10},
			{Key: []byte("b"), Version: 10},
		},
	}
	resp := &kvrpcpb.InstallPreparedMVCCEntriesResponse{
		AppliedEntries: 2,
		CommitVersion:  10,
	}
	require.NoError(t, validatePreparedSegmentInstallResponse(req, resp))

	resp.CommitVersion = 11
	require.ErrorIs(t, validatePreparedSegmentInstallResponse(req, resp), runtimeperas.ErrRuntimeInvalid)

	resp.CommitVersion = 10
	resp.AppliedEntries = 1
	require.ErrorIs(t, validatePreparedSegmentInstallResponse(req, resp), runtimeperas.ErrRuntimeInvalid)
}

func TestRaftstoreSegmentInstallerUsesLocalInstallVersion(t *testing.T) {
	mount := model.MountIdentity{MountID: "vol", MountKeyID: 1}
	dentryKey, err := fsmetamodel.EncodeDentryKey(mount, model.RootInode, "a")
	require.NoError(t, err)
	inodeKey, err := fsmetamodel.EncodeInodeKey(mount, 10)
	require.NoError(t, err)
	segment := testRaftstoreInstallSegment(t, [][]byte{dentryKey, inodeKey})
	payload, digest := encodeRaftstoreInstallSegment(t, segment)
	kv := &fakeRaftstorePerasInstallKV{resp: &kvrpcpb.InstallPreparedMVCCEntriesResponse{
		RegionId:      7,
		Term:          3,
		Index:         99,
		CommitVersion: 1,
	}}
	runner, err := stable.NewRunner(kv, &fakeRunnerTSO{resp: &coordpb.TsoResponse{Timestamp: 77, Count: 1}})
	require.NoError(t, err)

	installer := newRaftstoreSegmentInstaller(kv, runner, nil)
	cursor, err := installer.InstallSegment(context.Background(), runtimeperas.SegmentInstallRequest{
		Segment:         segment,
		Payload:         payload,
		PayloadDigest:   digest,
		MaterializeMVCC: true,
	})
	require.NoError(t, err)
	require.Equal(t, runtimeperas.InstallCursor{RegionID: 7, Term: 3, Index: 99, InstallVersion: 1}, cursor)

	req := kv.lastRequest()
	require.NotNil(t, req)
	require.Equal(t, uint64(1), req.GetCommitVersion())
	require.NotEmpty(t, req.GetDependencyKeys())
	require.NotEmpty(t, req.GetEntries())
	require.Equal(t, "peras_segment_install", req.GetDiagnosticLabel())
}

func TestRaftstoreSegmentInstallerPublishesInstalledDentries(t *testing.T) {
	mount := model.MountIdentity{MountID: "vol", MountKeyID: 1}
	dentryKey, err := fsmetamodel.EncodeDentryKey(mount, model.RootInode, "a")
	require.NoError(t, err)
	inodeKey, err := fsmetamodel.EncodeInodeKey(mount, 10)
	require.NoError(t, err)
	segment := testRaftstoreInstallSegment(t, [][]byte{dentryKey, inodeKey})
	payload, digest := encodeRaftstoreInstallSegment(t, segment)
	kv := &fakeRaftstorePerasInstallKV{resp: &kvrpcpb.InstallPreparedMVCCEntriesResponse{
		RegionId:      7,
		Term:          3,
		Index:         99,
		CommitVersion: 1,
	}}
	runner, err := stable.NewRunner(kv, &fakeRunnerTSO{resp: &coordpb.TsoResponse{Timestamp: 77, Count: 1}})
	require.NoError(t, err)
	router := fsmetawatch.NewRouter()
	sub, err := router.Subscribe(context.Background(), fsmetamodel.WatchRequest{
		KeyPrefix:          dentryKey,
		BackPressureWindow: 4,
	})
	require.NoError(t, err)
	defer sub.Close()

	installer := newRaftstoreSegmentInstaller(kv, runner, router)
	cursor, err := installer.InstallSegment(context.Background(), runtimeperas.SegmentInstallRequest{
		Segment:       segment,
		Payload:       payload,
		PayloadDigest: digest,
	})
	require.NoError(t, err)
	require.Equal(t, runtimeperas.InstallCursor{RegionID: 7, Term: 3, Index: 99, InstallVersion: 1}, cursor)

	select {
	case evt := <-sub.Events():
		require.Equal(t, dentryKey, evt.Key)
		require.Equal(t, fsmetamodel.WatchCursor{RegionID: 7, Term: 3, Index: 99}, evt.Cursor)
		require.Equal(t, uint64(1), evt.CommitVersion)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for installed segment watch event")
	}
}

func TestRaftstoreSegmentInstallerInstallsCatalogRoutesInParallel(t *testing.T) {
	oldProcs := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(oldProcs)

	mount := model.MountIdentity{MountID: "vol", MountKeyID: 1}
	rootKey, err := fsmetamodel.EncodeInodeKey(mount, model.RootInode)
	require.NoError(t, err)
	var otherKey []byte
	for inode := model.InodeID(2); inode < 100_000; inode++ {
		if fsmetamodel.BucketForInodeID(inode) == fsmetamodel.BucketForInodeID(model.RootInode) {
			continue
		}
		otherKey, err = fsmetamodel.EncodeInodeKey(mount, inode)
		require.NoError(t, err)
		break
	}
	require.NotEmpty(t, otherKey)

	segment := testRaftstoreInstallSegment(t, [][]byte{rootKey, otherKey})
	routingKeys, err := runtimeperas.SegmentInstallRoutingKeys(segment, false)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(routingKeys), 2)
	payload, digest := encodeRaftstoreInstallSegment(t, segment)

	kv := &parallelRaftstorePerasInstallKV{
		stats: segment.Stats(),
		root:  segment.Root,
		delay: 20 * time.Millisecond,
	}
	runner, err := stable.NewRunner(kv, &fakeRunnerTSO{resp: &coordpb.TsoResponse{Timestamp: 77, Count: 1}})
	require.NoError(t, err)

	installer := newRaftstoreSegmentInstaller(kv, runner, nil)
	cursor, err := installer.InstallSegment(context.Background(), runtimeperas.SegmentInstallRequest{
		Segment:       segment,
		Payload:       payload,
		PayloadDigest: digest,
	})
	require.NoError(t, err)
	require.True(t, cursor.Valid())
	require.Equal(t, len(routingKeys), kv.callCount())
	require.Greater(t, kv.maxInFlight(), int32(1))
	require.Equal(t, int32(len(routingKeys)), kv.headerRouteCount())
}

func TestRaftstoreSegmentInstallerGroupsCatalogRoutesByRegion(t *testing.T) {
	segment := testRaftstoreInstallSegment(t, testRaftstoreInstallKeysAcrossBuckets(t, 4))
	routingKeys, err := runtimeperas.SegmentInstallRoutingKeys(segment, false)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(routingKeys), 4)
	payload, digest := encodeRaftstoreInstallSegment(t, segment)

	kv := &groupingRaftstorePerasInstallKV{
		parallelRaftstorePerasInstallKV: parallelRaftstorePerasInstallKV{
			stats: segment.Stats(),
			root:  segment.Root,
		},
		groups: []client.RouteKeyGroup{
			{RegionID: 11, LeaderStoreID: 1, Keys: cloneRuntimeKeySet(routingKeys[:2])},
			{RegionID: 12, LeaderStoreID: 2, Keys: cloneRuntimeKeySet(routingKeys[2:])},
		},
	}
	runner, err := stable.NewRunner(kv, &fakeRunnerTSO{resp: &coordpb.TsoResponse{Timestamp: 77, Count: 1}})
	require.NoError(t, err)

	installer := newRaftstoreSegmentInstaller(kv, runner, nil)
	cursor, err := installer.InstallSegment(context.Background(), runtimeperas.SegmentInstallRequest{
		Segment:       segment,
		Payload:       payload,
		PayloadDigest: digest,
	})
	require.NoError(t, err)
	require.True(t, cursor.Valid())
	require.Equal(t, 2, kv.callCount())
	require.Len(t, kv.requestRouteCounts(), 2)
}

func TestRaftstoreSegmentInstallerLimitsRouteFanoutPerStore(t *testing.T) {
	oldProcs := runtime.GOMAXPROCS(6)
	defer runtime.GOMAXPROCS(oldProcs)

	segment := testRaftstoreInstallSegment(t, testRaftstoreInstallKeysAcrossBuckets(t, 4))
	routingKeys, err := runtimeperas.SegmentInstallRoutingKeys(segment, false)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(routingKeys), 4)
	payload, digest := encodeRaftstoreInstallSegment(t, segment)

	groups := make([]client.RouteKeyGroup, 0, 4)
	for _, key := range routingKeys[:4] {
		groups = append(groups, client.RouteKeyGroup{RegionID: uint64(len(groups) + 1), LeaderStoreID: 1, Keys: [][]byte{runtimeCloneBytes(key)}})
	}
	kv := &groupingRaftstorePerasInstallKV{
		parallelRaftstorePerasInstallKV: parallelRaftstorePerasInstallKV{
			stats: segment.Stats(),
			root:  segment.Root,
			delay: 20 * time.Millisecond,
		},
		groups: groups,
	}
	runner, err := stable.NewRunner(kv, &fakeRunnerTSO{resp: &coordpb.TsoResponse{Timestamp: 77, Count: 1}})
	require.NoError(t, err)

	installer := newRaftstoreSegmentInstaller(kv, runner, nil)
	cursor, err := installer.InstallSegment(context.Background(), runtimeperas.SegmentInstallRequest{
		Segment:       segment,
		Payload:       payload,
		PayloadDigest: digest,
	})
	require.NoError(t, err)
	require.True(t, cursor.Valid())
	require.Equal(t, 4, kv.callCount())
	require.LessOrEqual(t, kv.maxInFlight(), int32(2))
	require.Greater(t, kv.maxInFlight(), int32(1))
}

func TestRaftstoreSegmentInstallerReturnsWhenRouteErrorsExceedWorkers(t *testing.T) {
	oldProcs := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(oldProcs)

	segment := testRaftstoreInstallSegment(t, testRaftstoreInstallKeysAcrossBuckets(t, 4))
	routingKeys, err := runtimeperas.SegmentInstallRoutingKeys(segment, false)
	require.NoError(t, err)
	require.Greater(t, len(routingKeys), 2)
	payload, digest := encodeRaftstoreInstallSegment(t, segment)

	kv := &cancelingRaftstorePerasInstallKV{}
	runner, err := stable.NewRunner(kv, &fakeRunnerTSO{resp: &coordpb.TsoResponse{Timestamp: 77, Count: 1}})
	require.NoError(t, err)
	installer := newRaftstoreSegmentInstaller(kv, runner, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := installer.InstallSegment(ctx, runtimeperas.SegmentInstallRequest{
			Segment:       segment,
			Payload:       payload,
			PayloadDigest: digest,
		})
		done <- err
	}()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(time.Second):
		t.Fatal("InstallSegment blocked after more route errors than install workers")
	}
	require.Positive(t, kv.callCount())
	require.LessOrEqual(t, kv.callCount(), int32(len(routingKeys)))
}

func TestRaftstoreSegmentInstallerMarksInstallRetryExhaustedRoutingRetryable(t *testing.T) {
	mount := model.MountIdentity{MountID: "vol", MountKeyID: 1}
	dentryKey, err := fsmetamodel.EncodeDentryKey(mount, model.RootInode, "a")
	require.NoError(t, err)
	inodeKey, err := fsmetamodel.EncodeInodeKey(mount, 10)
	require.NoError(t, err)
	segment := testRaftstoreInstallSegment(t, [][]byte{dentryKey, inodeKey})
	payload, digest := encodeRaftstoreInstallSegment(t, segment)
	kv := &fakeRaftstorePerasInstallKV{
		err: &client.RetryExhaustedError{
			Operation: "peras install segment",
			Key:       dentryKey,
			Detail:    "region 7 returned not_leader",
		},
	}
	runner, err := stable.NewRunner(kv, &fakeRunnerTSO{resp: &coordpb.TsoResponse{Timestamp: 77, Count: 1}})
	require.NoError(t, err)
	installer := newRaftstoreSegmentInstaller(kv, runner, nil)

	_, err = installer.InstallSegment(context.Background(), runtimeperas.SegmentInstallRequest{
		Segment:         segment,
		Payload:         payload,
		PayloadDigest:   digest,
		MaterializeMVCC: true,
	})
	require.Error(t, err)
	require.True(t, client.IsRetryExhausted(err), "%T %v", err, err)
	require.Equal(t, nokverrors.KindRegionRouting, nokverrors.KindOf(err))
	require.True(t, nokverrors.Retryable(err), "%T %v", err, err)
}

func testRaftstoreInstallSegment(t *testing.T, keys [][]byte) fsperas.PerasSegment {
	t.Helper()
	mutations := make([]fsperas.ReplayMutation, 0, len(keys))
	for _, key := range keys {
		mutations = append(mutations, fsperas.ReplayMutation{Key: key, Value: []byte("value")})
	}
	segment, err := fsperas.BuildPerasSegmentFromReplayPlan(fsperas.ReplayPlan{
		EpochID: 1,
		Operations: []fsperas.ReplayOperation{{
			OpID:      fsperas.OperationID{ClientID: "client", Seq: 1},
			Kind:      model.OperationCreate,
			Mutations: mutations,
		}},
	})
	require.NoError(t, err)
	return segment
}

func testRaftstoreInstallKeysAcrossBuckets(t *testing.T, count int) [][]byte {
	t.Helper()
	mount := model.MountIdentity{MountID: "vol", MountKeyID: 1}
	seen := make(map[fsmetamodel.AffinityBucket]struct{}, count)
	keys := make([][]byte, 0, count)
	for inode := model.InodeID(1); len(keys) < count && inode < 1_000_000; inode++ {
		bucket := fsmetamodel.BucketForInodeID(inode)
		if _, ok := seen[bucket]; ok {
			continue
		}
		key, err := fsmetamodel.EncodeInodeKey(mount, inode)
		require.NoError(t, err)
		seen[bucket] = struct{}{}
		keys = append(keys, key)
	}
	require.Len(t, keys, count)
	return keys
}

func encodeRaftstoreInstallSegment(t *testing.T, segment fsperas.PerasSegment) ([]byte, [32]byte) {
	t.Helper()
	payload, err := fsperas.EncodePerasSegment(segment)
	require.NoError(t, err)
	digest, err := fsperas.PerasSegmentPayloadDigest(payload)
	require.NoError(t, err)
	return payload, digest
}

type fakeRunnerKV struct{}

func (f *fakeRunnerKV) Get(context.Context, []byte, uint64) (*kvrpcpb.GetResponse, error) {
	return nil, nil
}

func (f *fakeRunnerKV) BatchGet(context.Context, [][]byte, uint64) (map[string]*kvrpcpb.GetResponse, error) {
	return nil, nil
}

func (f *fakeRunnerKV) Scan(context.Context, []byte, uint32, uint64) ([]*kvrpcpb.KV, error) {
	return nil, nil
}

func (f *fakeRunnerKV) Mutate(context.Context, []byte, []*kvrpcpb.Mutation, uint64, uint64, uint64) error {
	return nil
}

type fakeRunnerTSO struct {
	resp *coordpb.TsoResponse
	err  error
}

func (f *fakeRunnerTSO) Tso(context.Context, *coordpb.TsoRequest) (*coordpb.TsoResponse, error) {
	return f.resp, f.err
}

type fakeRaftstorePerasInstallKV struct {
	fakeRunnerKV
	mu   sync.Mutex
	req  *kvrpcpb.InstallPreparedMVCCEntriesRequest
	resp *kvrpcpb.InstallPreparedMVCCEntriesResponse
	err  error
}

func (f *fakeRaftstorePerasInstallKV) InstallPreparedMVCCEntries(_ context.Context, _ []byte, req *kvrpcpb.InstallPreparedMVCCEntriesRequest) (*kvrpcpb.InstallPreparedMVCCEntriesResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.req = req
	if f.resp == nil {
		return nil, f.err
	}
	resp := &kvrpcpb.InstallPreparedMVCCEntriesResponse{
		Error:          f.resp.GetError(),
		AppliedEntries: uint64(len(req.GetEntries())),
		CommitVersion:  req.GetCommitVersion(),
		RegionId:       f.resp.GetRegionId(),
		Term:           f.resp.GetTerm(),
		Index:          f.resp.GetIndex(),
	}
	return resp, f.err
}

func (f *fakeRaftstorePerasInstallKV) lastRequest() *kvrpcpb.InstallPreparedMVCCEntriesRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.req
}

type parallelRaftstorePerasInstallKV struct {
	fakeRunnerKV
	root            [32]byte
	stats           fsperas.SegmentStats
	delay           time.Duration
	active          atomic.Int32
	max             atomic.Int32
	calls           atomic.Int32
	payloadRoutes   atomic.Int32
	indexOnlyRoutes atomic.Int32
	headerRoutes    atomic.Int32
}

type groupingRaftstorePerasInstallKV struct {
	parallelRaftstorePerasInstallKV
	groups []client.RouteKeyGroup
	mu     sync.Mutex
	counts []int
}

func (f *groupingRaftstorePerasInstallKV) GroupKeysByRoute(context.Context, [][]byte) ([]client.RouteKeyGroup, error) {
	return cloneRouteKeyGroups(f.groups), nil
}

func (f *groupingRaftstorePerasInstallKV) InstallPreparedMVCCEntries(ctx context.Context, key []byte, req *kvrpcpb.InstallPreparedMVCCEntriesRequest) (*kvrpcpb.InstallPreparedMVCCEntriesResponse, error) {
	f.mu.Lock()
	f.counts = append(f.counts, len(req.GetDependencyKeys()))
	f.mu.Unlock()
	return f.parallelRaftstorePerasInstallKV.InstallPreparedMVCCEntries(ctx, key, req)
}

func (f *groupingRaftstorePerasInstallKV) requestRouteCounts() []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := append([]int(nil), f.counts...)
	sort.Ints(out)
	return out
}

func (f *parallelRaftstorePerasInstallKV) InstallPreparedMVCCEntries(ctx context.Context, _ []byte, req *kvrpcpb.InstallPreparedMVCCEntriesRequest) (*kvrpcpb.InstallPreparedMVCCEntriesResponse, error) {
	if len(req.GetWatchKeys()) > 0 {
		f.payloadRoutes.Add(1)
	} else {
		f.indexOnlyRoutes.Add(1)
	}
	if len(req.GetDependencyKeys()) > 0 {
		f.headerRoutes.Add(1)
	}
	active := f.active.Add(1)
	for {
		current := f.max.Load()
		if active <= current || f.max.CompareAndSwap(current, active) {
			break
		}
	}
	defer f.active.Add(-1)
	if f.delay > 0 {
		timer := time.NewTimer(f.delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		}
	}
	call := f.calls.Add(1)
	regionID := uint64(call)
	if len(req.GetWatchKeys()) > 0 {
		regionID = 1000
	}
	return &kvrpcpb.InstallPreparedMVCCEntriesResponse{
		AppliedEntries: uint64(len(req.GetEntries())),
		RegionId:       regionID,
		Term:           1,
		Index:          uint64(call),
		CommitVersion:  req.GetCommitVersion(),
	}, nil
}

func (f *parallelRaftstorePerasInstallKV) callCount() int {
	return int(f.calls.Load())
}

func (f *parallelRaftstorePerasInstallKV) maxInFlight() int32 {
	return f.max.Load()
}

func (f *parallelRaftstorePerasInstallKV) headerRouteCount() int32 {
	return f.headerRoutes.Load()
}

type cancelingRaftstorePerasInstallKV struct {
	fakeRunnerKV
	calls atomic.Int32
}

func (f *cancelingRaftstorePerasInstallKV) InstallPreparedMVCCEntries(ctx context.Context, _ []byte, _ *kvrpcpb.InstallPreparedMVCCEntriesRequest) (*kvrpcpb.InstallPreparedMVCCEntriesResponse, error) {
	f.calls.Add(1)
	<-ctx.Done()
	return nil, ctx.Err()
}

func (f *cancelingRaftstorePerasInstallKV) callCount() int32 {
	return f.calls.Load()
}
