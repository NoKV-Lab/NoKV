// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/engine/wal"
	fsperas "github.com/feichai0017/NoKV/experimental/peras/exec"
	runtimeperas "github.com/feichai0017/NoKV/experimental/peras/runtime"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	"github.com/stretchr/testify/require"
)

var _ fsperas.WitnessReplica = (*WitnessNode)(nil)
var _ fsperas.WitnessReplica = (*LocalWitnessReplica)(nil)
var _ fsperas.WitnessSegmentProber = (*WitnessNode)(nil)

func TestWitnessNodeAppendSegmentsSingleRecordAndProbe(t *testing.T) {
	node, cleanup := openTestWitnessNode(t, wal.DurabilityFsync)
	defer cleanup()
	scope := testAuthorityScope()
	record := testSegmentRecord()

	require.NoError(t, appendWitnessSegment(context.Background(), node, scope, record))

	snapshot, err := node.Probe(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, []fsperas.SegmentWitnessRecord{record}, snapshot.Segments)
}

func TestWitnessNodeAppendSegmentsAndProbe(t *testing.T) {
	node, cleanup := openTestWitnessNode(t, wal.DurabilityFsync)
	defer cleanup()
	scope := testAuthorityScope()
	first := testSegmentRecord()
	second := testSegmentRecord()
	second.SegmentRoot[0] = 3
	second.SegmentPayloadDigest[0] = 4

	require.NoError(t, node.AppendSegments(context.Background(), scope, []fsperas.SegmentWitnessRecord{first, second}))

	snapshot, err := node.Probe(context.Background(), 1)
	require.NoError(t, err)
	require.ElementsMatch(t, []fsperas.SegmentWitnessRecord{first, second}, snapshot.Segments)
}

func TestWitnessNodeStatsBreakDownAppendPath(t *testing.T) {
	node, cleanup := openTestWitnessNode(t, wal.DurabilityFsync)
	defer cleanup()
	scope := testAuthorityScope()
	first := testSegmentRecord()
	second := testSegmentRecord()
	second.SegmentRoot[0] = 3
	second.SegmentPayloadDigest[0] = 4

	require.NoError(t, node.AppendSegments(context.Background(), scope, []fsperas.SegmentWitnessRecord{first, second}))
	require.NoError(t, node.AppendSegments(context.Background(), scope, []fsperas.SegmentWitnessRecord{first, second}))

	stats := node.Stats()
	require.Equal(t, uint64(2), stats["append_total"])
	require.Equal(t, uint64(4), stats["append_records_total"])
	require.Equal(t, uint64(0), stats["append_error_total"])
	require.Equal(t, uint64(2), stats["authority_check_batches_total"])
	require.Equal(t, uint64(4), stats["authority_check_records_total"])
	require.Equal(t, uint64(2), stats["pending_append_records_total"])
	require.Equal(t, uint64(2), stats["dedupe_skip_total"])
	require.Equal(t, uint64(1), stats["wal_append_total"])
	require.Equal(t, uint64(2), stats["wal_append_records_total"])
	require.Greater(t, stats["wal_append_bytes_total"].(uint64), uint64(0))
	require.Contains(t, stats, "wal_encode_latency_total_nanosecond")
	require.Contains(t, stats, "wal_manager_append_latency_total_nanosecond")
}

func TestWitnessNodeRejectsMissingAuthority(t *testing.T) {
	now := time.Unix(100, 0)
	authorities := runtimeperas.NewActiveAuthorities()
	require.NoError(t, authorities.Replace(nil))
	node, cleanup := openTestWitnessNodeWithAuthorityView(t, wal.DurabilityFsync, authorities, func() time.Time { return now })
	defer cleanup()

	err := appendWitnessSegment(context.Background(), node, testAuthorityScope(), testSegmentRecord())
	require.ErrorIs(t, err, ErrWitnessAuthorityMissing)
}

func TestWitnessNodeRefreshesAuthorityBeforeRejecting(t *testing.T) {
	now := time.Unix(100, 0)
	manager, err := wal.Open(wal.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	defer func() { require.NoError(t, manager.Close()) }()
	log, err := NewWALWitnessLog(manager, wal.DurabilityFsync)
	require.NoError(t, err)
	authorities := runtimeperas.NewActiveAuthorities()
	refreshed := false
	node, err := NewWitnessNode(WitnessNodeConfig{
		NodeID:        "store-1",
		Log:           log,
		AuthorityView: authorities,
		AuthorityRefresh: func(context.Context) error {
			refreshed = true
			return authorities.Replace([]rootproto.VisibleAuthorityGrant{testAuthorityGrant()})
		},
		Now: func() time.Time { return now },
	})
	require.NoError(t, err)

	require.NoError(t, appendWitnessSegment(context.Background(), node, testAuthorityScope(), testSegmentRecord()))
	require.True(t, refreshed)
}

func TestWitnessNodeRefreshFailureIsFatal(t *testing.T) {
	now := time.Unix(100, 0)
	manager, err := wal.Open(wal.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	defer func() { require.NoError(t, manager.Close()) }()
	log, err := NewWALWitnessLog(manager, wal.DurabilityFsync)
	require.NoError(t, err)
	authorities := runtimeperas.NewActiveAuthorities()
	refreshErr := errors.New("refresh failed")
	node, err := NewWitnessNode(WitnessNodeConfig{
		NodeID:        "store-1",
		Log:           log,
		AuthorityView: authorities,
		AuthorityRefresh: func(context.Context) error {
			return refreshErr
		},
		Now: func() time.Time { return now },
	})
	require.NoError(t, err)

	err = appendWitnessSegment(context.Background(), node, testAuthorityScope(), testSegmentRecord())
	require.ErrorIs(t, err, refreshErr)
}

func TestWitnessNodeRejectsWrongHolderAndEpoch(t *testing.T) {
	node, cleanup := openTestWitnessNode(t, wal.DurabilityFsync)
	defer cleanup()
	scope := testAuthorityScope()

	wrongHolder := testSegmentRecord()
	wrongHolder.HolderID = "holder-b"
	require.ErrorIs(t, appendWitnessSegment(context.Background(), node, scope, wrongHolder), ErrWitnessAuthorityMismatch)

	wrongEpoch := testSegmentRecord()
	wrongEpoch.EpochID = 2
	require.ErrorIs(t, appendWitnessSegment(context.Background(), node, scope, wrongEpoch), ErrWitnessAuthorityMismatch)
}

func TestWitnessNodeAcceptsSameHolderOldEpochDrain(t *testing.T) {
	now := time.Unix(100, 0)
	authorities := runtimeperas.NewActiveAuthorities()
	grant := testAuthorityGrant()
	grant.GrantID = "grant-2"
	grant.EpochID = 2
	grant.ExpiresUnixNano = now.Add(time.Hour).UnixNano()
	require.NoError(t, authorities.Replace([]rootproto.VisibleAuthorityGrant{grant}))
	node, cleanup := openTestWitnessNodeWithAuthorityView(t, wal.DurabilityFsync, authorities, func() time.Time { return now })
	defer cleanup()

	record := testSegmentRecord()
	record.EpochID = 1
	require.NoError(t, appendWitnessSegment(context.Background(), node, testAuthorityScope(), record))
}

func TestWitnessNodeRejectsSameHolderOldEpochPredecessorMismatch(t *testing.T) {
	now := time.Unix(100, 0)
	authorities := runtimeperas.NewActiveAuthorities()
	grant := testAuthorityGrant()
	grant.GrantID = "grant-2"
	grant.EpochID = 2
	grant.PredecessorDigest[0] = 9
	grant.ExpiresUnixNano = now.Add(time.Hour).UnixNano()
	require.NoError(t, authorities.Replace([]rootproto.VisibleAuthorityGrant{grant}))
	node, cleanup := openTestWitnessNodeWithAuthorityView(t, wal.DurabilityFsync, authorities, func() time.Time { return now })
	defer cleanup()

	record := testSegmentRecord()
	record.EpochID = 1
	record.PredecessorDigest[0] = 8
	require.ErrorIs(t, appendWitnessSegment(context.Background(), node, testAuthorityScope(), record), ErrWitnessAuthorityMismatch)
}

func TestWitnessNodeRejectsExpiredAuthority(t *testing.T) {
	now := time.Unix(100, 0)
	authorities := runtimeperas.NewActiveAuthorities()
	expired := testAuthorityGrant()
	expired.ExpiresUnixNano = now.Add(-time.Nanosecond).UnixNano()
	require.NoError(t, authorities.Replace([]rootproto.VisibleAuthorityGrant{expired}))
	node, cleanup := openTestWitnessNodeWithAuthorityView(t, wal.DurabilityFsync, authorities, func() time.Time { return now })
	defer cleanup()

	err := appendWitnessSegment(context.Background(), node, testAuthorityScope(), testSegmentRecord())
	require.ErrorIs(t, err, ErrWitnessAuthorityMissing)
}

func TestWitnessNodeAcceptsRenewedSameEpochAuthority(t *testing.T) {
	now := time.Unix(100, 0)
	authorities := runtimeperas.NewActiveAuthorities()
	renewed := testAuthorityGrant()
	renewed.ExpiresUnixNano = now.Add(time.Hour).UnixNano()
	require.NoError(t, authorities.Replace([]rootproto.VisibleAuthorityGrant{renewed}))
	node, cleanup := openTestWitnessNodeWithAuthorityView(t, wal.DurabilityFsync, authorities, func() time.Time { return now })
	defer cleanup()

	record := testSegmentRecord()
	require.Equal(t, renewed.EpochID, record.EpochID)
	require.Equal(t, renewed.HolderID, record.HolderID)
	require.NoError(t, appendWitnessSegment(context.Background(), node, testAuthorityScope(), record))
}

func TestWitnessNodeDuplicateSegmentIsIdempotent(t *testing.T) {
	node, cleanup := openTestWitnessNode(t, wal.DurabilityFsync)
	defer cleanup()
	scope := testAuthorityScope()
	record := testSegmentRecord()

	require.NoError(t, appendWitnessSegment(context.Background(), node, scope, record))
	require.NoError(t, appendWitnessSegment(context.Background(), node, scope, record))

	snapshot, err := node.Probe(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, []fsperas.SegmentWitnessRecord{record}, snapshot.Segments)
}

func TestWitnessNodeCachesLoadedEpoch(t *testing.T) {
	node, cleanup := openTestWitnessNode(t, wal.DurabilityFsync)
	defer cleanup()
	scope := testAuthorityScope()
	first := testSegmentRecord()
	second := testSegmentRecord()
	second.SegmentRoot[0] = 3
	second.SegmentPayloadDigest[0] = 4

	require.NoError(t, appendWitnessSegment(context.Background(), node, scope, first))
	node.mu.Lock()
	_, loaded := node.loaded[first.EpochID]
	require.True(t, loaded)
	require.Len(t, node.loaded, 1)
	node.mu.Unlock()

	require.NoError(t, appendWitnessSegment(context.Background(), node, scope, second))
	node.mu.Lock()
	_, loaded = node.loaded[first.EpochID]
	require.True(t, loaded)
	require.Len(t, node.loaded, 1)
	node.mu.Unlock()

	snapshot, err := node.Probe(context.Background(), first.EpochID)
	require.NoError(t, err)
	require.Len(t, snapshot.Segments, 2)
}

func TestWitnessNodeProbeSegmentReturnsTargetOnly(t *testing.T) {
	node, cleanup := openTestWitnessNode(t, wal.DurabilityFsync)
	defer cleanup()
	scope := testAuthorityScope()
	first := testSegmentRecord()
	second := testSegmentRecord()
	second.SegmentRoot[0] = 3
	second.SegmentPayloadDigest[0] = 4

	require.NoError(t, appendWitnessSegment(context.Background(), node, scope, first))
	require.NoError(t, appendWitnessSegment(context.Background(), node, scope, second))

	record, found, err := node.ProbeSegment(context.Background(), fsperas.WitnessSegmentRef{
		EpochID:              second.EpochID,
		SegmentRoot:          second.SegmentRoot,
		SegmentPayloadDigest: second.SegmentPayloadDigest,
	})
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, second, record)

	missing := second
	missing.SegmentPayloadDigest[0] = 9
	_, found, err = node.ProbeSegment(context.Background(), fsperas.WitnessSegmentRef{
		EpochID:              missing.EpochID,
		SegmentRoot:          missing.SegmentRoot,
		SegmentPayloadDigest: missing.SegmentPayloadDigest,
	})
	require.NoError(t, err)
	require.False(t, found)
}

func TestWitnessNodeConcurrentDuplicateSegmentIsSingleWALRecord(t *testing.T) {
	node, cleanup := openTestWitnessNode(t, wal.DurabilityFsyncBatched)
	defer cleanup()
	scope := testAuthorityScope()
	record := testSegmentRecord()
	start := make(chan struct{})
	errCh := make(chan error, 16)
	for range 16 {
		go func() {
			<-start
			errCh <- appendWitnessSegment(context.Background(), node, scope, record)
		}()
	}
	close(start)
	for range 16 {
		require.NoError(t, <-errCh)
	}

	snapshot, err := node.Probe(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, []fsperas.SegmentWitnessRecord{record}, snapshot.Segments)
}

func TestWitnessNodeLoadsSegmentsFromWAL(t *testing.T) {
	dir := t.TempDir()
	now := time.Unix(100, 0)
	manager, err := wal.Open(wal.Config{Dir: dir})
	require.NoError(t, err)
	log, err := NewWALWitnessLog(manager, wal.DurabilityFsync)
	require.NoError(t, err)
	authorities := testActiveAuthorities(t, now)
	node, err := NewWitnessNode(WitnessNodeConfig{
		NodeID:        "store-1",
		Log:           log,
		AuthorityView: authorities,
		Now:           func() time.Time { return now },
	})
	require.NoError(t, err)
	record := testSegmentRecord()
	require.NoError(t, appendWitnessSegment(context.Background(), node, testAuthorityScope(), record))
	require.NoError(t, manager.Close())

	reopened, err := wal.Open(wal.Config{Dir: dir})
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()
	reopenedLog, err := NewWALWitnessLog(reopened, wal.DurabilityFsync)
	require.NoError(t, err)
	reopenedNode, err := NewWitnessNode(WitnessNodeConfig{
		NodeID:        "store-1",
		Log:           reopenedLog,
		AuthorityView: authorities,
		Now:           func() time.Time { return now },
	})
	require.NoError(t, err)

	require.NoError(t, appendWitnessSegment(context.Background(), reopenedNode, testAuthorityScope(), record))
	snapshot, err := reopenedNode.Probe(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, []fsperas.SegmentWitnessRecord{record}, snapshot.Segments)
}

func TestWitnessNodeContextCancellationStopsAppend(t *testing.T) {
	node, cleanup := openTestWitnessNode(t, wal.DurabilityFsync)
	defer cleanup()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := appendWitnessSegment(ctx, node, testAuthorityScope(), testSegmentRecord())
	require.True(t, errors.Is(err, context.Canceled))
}

func BenchmarkWitnessNodeAppendSegmentsSingleRecord(b *testing.B) {
	node, cleanup := openBenchWitnessNode(b)
	defer cleanup()
	scope := testAuthorityScope()
	record := testSegmentRecord()

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(record.SegmentRoot[:8], uint64(i+1))
		if err := appendWitnessSegment(context.Background(), node, scope, record); err != nil {
			b.Fatal(err)
		}
	}
}

func appendWitnessSegment(ctx context.Context, node *WitnessNode, scope compile.AuthorityScope, record fsperas.SegmentWitnessRecord) error {
	return node.AppendSegments(ctx, scope, []fsperas.SegmentWitnessRecord{record})
}

func openTestWitnessNode(t *testing.T, durability wal.DurabilityPolicy) (*WitnessNode, func()) {
	t.Helper()
	now := time.Unix(100, 0)
	return openTestWitnessNodeWithNow(t, durability, func() time.Time { return now })
}

func openTestWitnessNodeWithNow(t *testing.T, durability wal.DurabilityPolicy, now func() time.Time) (*WitnessNode, func()) {
	t.Helper()
	return openTestWitnessNodeWithAuthorityView(t, durability, testActiveAuthorities(t, now()), now)
}

func openTestWitnessNodeWithAuthorityView(t *testing.T, durability wal.DurabilityPolicy, authorities AuthorityView, now func() time.Time) (*WitnessNode, func()) {
	t.Helper()
	manager, err := wal.Open(wal.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	log, err := NewWALWitnessLog(manager, durability)
	require.NoError(t, err)
	node, err := NewWitnessNode(WitnessNodeConfig{
		NodeID:        "store-1",
		Log:           log,
		AuthorityView: authorities,
		Now:           now,
	})
	require.NoError(t, err)
	return node, func() { require.NoError(t, manager.Close()) }
}

func openBenchWitnessNode(b *testing.B) (*WitnessNode, func()) {
	b.Helper()
	now := time.Unix(100, 0)
	manager, err := wal.Open(wal.Config{Dir: b.TempDir()})
	require.NoError(b, err)
	log, err := NewWALWitnessLog(manager, wal.DurabilityBuffered)
	require.NoError(b, err)
	node, err := NewWitnessNode(WitnessNodeConfig{
		NodeID:        "store-1",
		Log:           log,
		AuthorityView: testActiveAuthorities(b, now),
		Now:           func() time.Time { return now },
	})
	require.NoError(b, err)
	return node, func() { require.NoError(b, manager.Close()) }
}

func testActiveAuthorities(tb testing.TB, now time.Time) *runtimeperas.ActiveAuthorities {
	tb.Helper()
	table := runtimeperas.NewActiveAuthorities()
	grant := testAuthorityGrant()
	grant.ExpiresUnixNano = now.Add(time.Hour).UnixNano()
	require.NoError(tb, table.Replace([]rootproto.VisibleAuthorityGrant{grant}))
	return table
}

func testAuthorityGrant() rootproto.VisibleAuthorityGrant {
	return rootproto.VisibleAuthorityGrant{
		GrantID:          "grant-1",
		EpochID:          1,
		HolderID:         "holder-a",
		Scope:            runtimeperas.AuthorityScopeFromDelta(testAuthorityScope()),
		ExpiresUnixNano:  time.Unix(101, 0).UnixNano(),
		RootClusterEpoch: 1,
		IssuedRootToken: rootproto.AuthorityRootToken{
			Term:     1,
			Index:    1,
			Revision: 1,
		},
	}
}

func testAuthorityScope() compile.AuthorityScope {
	return compile.AuthorityScope{
		Mount:      model.MountID("vol"),
		MountKeyID: model.MountKeyID(7),
		Buckets:    []layout.AffinityBucket{3},
		Parents:    []model.InodeID{11},
		Inodes:     []model.InodeID{29},
	}
}

func testSegmentRecord() fsperas.SegmentWitnessRecord {
	var root [32]byte
	root[0] = 1
	var digest [32]byte
	digest[0] = 2
	return fsperas.SegmentWitnessRecord{
		EpochID:              1,
		SegmentRoot:          root,
		SegmentPayloadDigest: digest,
		SegmentPayloadSize:   4096,
		SegmentPointer:       "inline",
		OperationCount:       64,
		EntryCount:           128,
		TimestampUnixNano:    100,
		HolderID:             "holder-a",
	}
}
