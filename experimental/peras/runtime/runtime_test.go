// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/engine/wal"
	nokverrors "github.com/feichai0017/NoKV/errors"
	fsperas "github.com/feichai0017/NoKV/experimental/peras/exec"
	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	fsmetawatch "github.com/feichai0017/NoKV/fsmeta/exec/watch"
	"github.com/feichai0017/NoKV/fsmeta/proof"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	"github.com/stretchr/testify/require"
)

func TestRuntimeCommitsAndServesOverlay(t *testing.T) {
	provider := &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimeSegmentWitnesses(t, 3),
		VisibleLog:        &recordingVisibleLog{},
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	delta := testRuntimePerasOp([]byte("dentry/a"), []byte("inode/a"))
	_, err = committer.SubmitVisible(context.Background(), fsperas.OperationID{ClientID: "client", Seq: 1}, delta, nil)
	require.NoError(t, err)

	value, deleted, ok := committer.GetPerasOverlay(testRuntimeDentryEffect(delta).Key)
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, []byte("dentry-value"), value)
	require.Equal(t, uint64(1), committer.Stats()["commit_total"])
}

func TestRuntimeSubmitVisibleRequiresVisibleLog(t *testing.T) {
	provider := &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimeSegmentWitnesses(t, 3),
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	op := testRuntimePerasOp([]byte("dentry/a"), []byte("inode/a"))
	_, err = committer.SubmitVisible(context.Background(), fsperas.OperationID{ClientID: "client", Seq: 1}, op, nil)
	require.ErrorIs(t, err, fsperas.ErrVisibleLogRequired)

	_, _, ok := committer.GetPerasOverlay(testRuntimeDentryEffect(op).Key)
	require.False(t, ok)
	require.Equal(t, 0, committer.Stats()["pending"])
	require.Equal(t, uint64(0), committer.Stats()["commit_total"])
}

func TestRuntimePublishesVisibleWatch(t *testing.T) {
	mount := fsmeta.MountIdentity{MountID: "vol", MountKeyID: 1}
	parent := fsmeta.InodeID(1)
	inode := testRuntimeInodeForBucket(t, fsmeta.BucketForInodeID(parent))
	dentryKey, err := fsmeta.EncodeDentryKey(mount, parent, "visible")
	require.NoError(t, err)
	inodeKey, err := fsmeta.EncodeInodeKey(mount, inode)
	require.NoError(t, err)
	prefix, err := fsmeta.EncodeDentryPrefix(mount, parent)
	require.NoError(t, err)
	router := fsmetawatch.NewRouter()
	sub, err := router.Subscribe(context.Background(), fsmeta.WatchRequest{KeyPrefix: prefix})
	require.NoError(t, err)
	defer sub.Close()

	provider := &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimeSegmentWitnesses(t, 3),
		WatchPublisher:    router,
		VisibleLog:        &recordingVisibleLog{},
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	delta := testRuntimePerasOp(dentryKey, inodeKey)
	delta = testRuntimePerasOpWithScope(delta, compile.AuthorityScope{
		Mount:      delta.Delta.Authority.Mount,
		MountKeyID: delta.Delta.Authority.MountKeyID,
		Parents:    []fsmeta.InodeID{parent},
		Inodes:     []fsmeta.InodeID{inode},
	})
	_, err = committer.SubmitVisible(context.Background(), fsperas.OperationID{ClientID: "client", Seq: 1}, delta, nil)
	require.NoError(t, err)

	got := <-sub.Events()
	require.Equal(t, fsmeta.WatchEventSourceRuntimeVisible, got.Source)
	require.Equal(t, dentryKey, got.Key)
	require.Zero(t, got.Cursor.RegionID)
	require.Equal(t, uint64(1), got.Cursor.Term)
	require.Equal(t, uint64(1), got.Cursor.Index)
}

func TestRuntimeFlushesSegmentAndKeepsReadsVisible(t *testing.T) {
	provider := &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	installer := &fakeRuntimePerasSegmentInstaller{}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimeSegmentWitnesses(t, 3),
		Installer:         installer,
		VisibleLog:        &recordingVisibleLog{},
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	ctx := context.Background()
	require.NoError(t, commitRuntimePeras(ctx, committer, 1, []byte("dentry/a"), []byte("inode/a")))
	require.NoError(t, commitRuntimePeras(ctx, committer, 2, []byte("dentry/b"), []byte("inode/b")))
	require.NoError(t, committer.FlushDurable(ctx))

	stats := committer.Stats()
	require.Equal(t, uint64(1), stats["flush_total"])
	require.Equal(t, uint64(1), stats["segment_total"])
	require.Equal(t, uint64(2), stats["segment_operations_total"])
	require.Equal(t, 0, stats["overlay_keys"])
	require.Equal(t, 5, stats["segment_keys"])
	require.Equal(t, 0, stats["pending"])
	require.Equal(t, 1, installer.calls)
	require.True(t, ScopesOverlap(installer.scope, testRuntimePerasOp([]byte("dentry/a"), []byte("inode/a")).Delta.Authority))
	require.True(t, ScopesOverlap(installer.scope, testRuntimePerasOp([]byte("dentry/b"), []byte("inode/b")).Delta.Authority))
	require.NotZero(t, installer.segment.Root)
	require.NotEmpty(t, installer.payload)
	require.NotZero(t, installer.digest)
	require.False(t, installer.materialize)
	decoded, err := fsperas.VerifyPerasSegmentPayload(installer.payload, installer.segment.Root, installer.digest)
	require.NoError(t, err)
	require.Equal(t, installer.segment.Root, decoded.Root)

	dentryA := testRuntimeDentryKeyForLabel("a")
	dentryB := testRuntimeDentryKeyForLabel("b")
	value, deleted, ok := committer.GetPerasOverlay(dentryA)
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, []byte("dentry-value"), value)

	scan := committer.ScanPerasOverlay(testRuntimeRootDentryPrefix(), 2)
	require.Len(t, scan, 2)
	require.Equal(t, dentryA, scan[0].Key)
	require.Equal(t, dentryB, scan[1].Key)

}

func TestRuntimeDirectoryEmptyFactsDoNotIgnoreSealedRows(t *testing.T) {
	provider := &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimeSegmentWitnesses(t, 3),
		Installer:         &fakeRuntimePerasSegmentInstaller{},
		VisibleLog:        &recordingVisibleLog{},
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	ctx := context.Background()
	committer.RememberEmptyDirectory(testRuntimeMount, fsmeta.RootInode)
	require.True(t, committer.DirectoryEmpty(testRuntimeMount, fsmeta.RootInode))
	require.True(t, committer.DirectoryBaseEmpty(testRuntimeMount, fsmeta.RootInode))

	require.NoError(t, commitRuntimePeras(ctx, committer, 1, testRuntimeDentryKeyForLabel("sealed"), testRuntimeDentryKeyForLabel("sealed-inode")))
	committer.ForgetEmptyDirectory(testRuntimeMount, fsmeta.RootInode)
	require.NoError(t, committer.FlushDurable(ctx))

	require.False(t, committer.DirectoryEmpty(testRuntimeMount, fsmeta.RootInode))
	require.False(t, committer.DirectoryBaseEmpty(testRuntimeMount, fsmeta.RootInode))
}

func TestRuntimeWithoutWitnessLayerFlushesVisibleLogToInstall(t *testing.T) {
	provider := &nonSealingRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	installer := &fakeRuntimePerasSegmentInstaller{}
	visibleLog := &replayingVisibleLog{}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Installer:         installer,
		VisibleLog:        visibleLog,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	ctx := context.Background()
	require.NoError(t, commitRuntimePeras(ctx, committer, 1, []byte("dentry/a"), []byte("inode/a")))
	require.NoError(t, committer.FlushDurable(ctx))

	stats := committer.Stats()
	require.Equal(t, "disabled", stats["witness_mode"])
	require.Equal(t, 0, stats["witness_count"])
	require.Equal(t, 0, stats["quorum"])
	require.Equal(t, uint64(0), stats["witness_batch_total"])
	require.Equal(t, uint64(0), stats["witness_quorum_total"])
	require.Equal(t, uint64(1), stats["visible_log_apply_marker_total"])
	require.Equal(t, 0, stats["pending"])
	require.Equal(t, 1, installer.calls)
	require.Len(t, visibleLog.applied, 1)
}

func TestRuntimeRootSealAuthorityRequiresWitnessLayer(t *testing.T) {
	provider := &publishingRuntimeVisibleGrantProvider{
		fakeRuntimeVisibleGrantProvider: fakeRuntimeVisibleGrantProvider{
			holderID: "holder-a",
			grant:    testRuntimeCommitterGrant(),
		},
	}
	_, err := NewRuntime(Config{
		Authority:         provider,
		Installer:         &fakeRuntimePerasSegmentInstaller{},
		VisibleLog:        &recordingVisibleLog{},
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.ErrorIs(t, err, ErrRuntimeInvalid)
}

func TestRuntimeMaterializedNoCatalogRequiresAppliedVisibleReplay(t *testing.T) {
	provider := &nonSealingRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	_, err := NewRuntime(Config{
		Authority:         provider,
		Installer:         &fakeRuntimePerasSegmentInstaller{materializes: true},
		VisibleLog:        &recordingVisibleLog{},
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.ErrorIs(t, err, ErrRuntimeInvalid)
}

func TestRuntimeMaterializedFlushRemovesReadOverlay(t *testing.T) {
	provider := &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	installer := &fakeRuntimePerasSegmentInstaller{materializes: true}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimeSegmentWitnesses(t, 3),
		Installer:         installer,
		VisibleLog:        &replayingVisibleLog{},
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	ctx := context.Background()
	require.NoError(t, commitRuntimePeras(ctx, committer, 1, []byte("dentry/a"), []byte("inode/a")))
	require.NoError(t, committer.FlushDurable(ctx))

	stats := committer.Stats()
	require.Equal(t, uint64(1), stats["flush_total"])
	require.Equal(t, 0, stats["overlay_keys"])
	require.Equal(t, 0, stats["segment_keys"])
	require.Equal(t, 0, stats["pending"])
	require.Equal(t, 1, installer.calls)
	require.True(t, installer.materialize)

	_, _, ok := committer.GetPerasOverlay(testRuntimeDentryKeyForLabel("a"))
	require.False(t, ok)
	require.Empty(t, committer.ScanPerasOverlay(testRuntimeRootDentryPrefix(), 2))
}

func TestRuntimeScanPerasOverlayMergesViewsByLimit(t *testing.T) {
	provider := &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimeSegmentWitnesses(t, 3),
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	keyA := testRuntimeDentryKeyForLabel("a")
	keyB := testRuntimeDentryKeyForLabel("b")
	keyC := testRuntimeDentryKeyForLabel("c")
	keyD := testRuntimeDentryKeyForLabel("d")
	require.NoError(t, committer.read.sealed.AddSegment(testRuntimePerasSegmentForOverlay(keyA, []byte("sealed-a"))))
	require.NoError(t, committer.read.sealed.AddSegment(testRuntimePerasSegmentForOverlay(keyB, []byte("sealed-b"))))
	require.NoError(t, committer.read.sealed.AddSegment(testRuntimePerasSegmentForOverlay(keyD, []byte("sealed-d"))))
	require.NoError(t, committer.read.overlay.Add(fsperas.OperationID{ClientID: "test", Seq: 1}, testRuntimeRenameDentryOp("b", "c", []byte("overlay-c"))))

	scan := committer.ScanPerasOverlay(testRuntimeRootDentryPrefix(), 4)
	require.Equal(t, []fsperas.OverlayKV{
		{Key: keyA, Value: []byte("sealed-a")},
		{Key: keyB, Delete: true},
		{Key: keyC, Value: []byte("overlay-c")},
		{Key: keyD, Value: []byte("sealed-d")},
	}, scan)

	scan = committer.ScanPerasOverlay(keyB, 2)
	require.Equal(t, []fsperas.OverlayKV{
		{Key: keyB, Delete: true},
		{Key: keyC, Value: []byte("overlay-c")},
	}, scan)
}

func TestRuntimeScanPerasDirectoryUsesDirectoryIndex(t *testing.T) {
	committer := &Runtime{read: newReadState()}
	prefix, err := fsmeta.EncodeDentryPrefix(testRuntimeMount, 9)
	require.NoError(t, err)
	keyA, err := fsmeta.EncodeDentryKey(testRuntimeMount, 9, "a")
	require.NoError(t, err)
	keyB, err := fsmeta.EncodeDentryKey(testRuntimeMount, 9, "b")
	require.NoError(t, err)
	other, err := fsmeta.EncodeDentryKey(testRuntimeMount, 10, "a")
	require.NoError(t, err)
	require.NoError(t, committer.read.sealed.AddSegment(testRuntimePerasSegmentForOverlay(keyB, []byte("sealed-b"))))
	require.NoError(t, committer.read.sealed.AddSegment(testRuntimePerasSegmentForOverlay(other, []byte("other"))))
	require.NoError(t, committer.read.overlay.Add(fsperas.OperationID{ClientID: "test", Seq: 1}, testRuntimeCreateOp(testRuntimeMount, 9, "a", 41, []byte("overlay-a"), nil)))

	scan := committer.ScanPerasDirectory(prefix, prefix, 8)
	require.Equal(t, []fsperas.OverlayKV{
		{Key: keyA, Value: []byte("overlay-a")},
		{Key: keyB, Value: []byte("sealed-b")},
	}, scan)
}

func TestRuntimeCapturePerasSnapshotUsesSealedSegmentGeneration(t *testing.T) {
	committer := &Runtime{read: newReadState()}
	prefix := testRuntimeRootDentryPrefix()
	keyA := testRuntimeDentryKeyForLabel("a")
	keyB := testRuntimeDentryKeyForLabel("b")

	installRuntimeSealedSegment(t, committer, testRuntimePerasSegmentForOverlay(keyA, []byte("sealed-a-v1")))
	require.NoError(t, committer.CapturePerasSnapshot(10))
	installRuntimeSealedSegment(t, committer, testRuntimePerasSegmentForOverlay(keyA, []byte("sealed-a-v2")))
	installRuntimeSealedSegment(t, committer, testRuntimePerasSegmentForOverlay(keyB, []byte("sealed-b")))

	value, deleted, ok := committer.GetPerasSnapshotOverlayView(10, keyA)
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, []byte("sealed-a-v1"), value)

	_, _, ok = committer.GetPerasSnapshotOverlayView(10, keyB)
	require.False(t, ok)
	require.True(t, committer.HasPerasSnapshotDirectory(10, prefix))
	require.Equal(t, []fsperas.OverlayKV{
		{Key: keyA, Value: []byte("sealed-a-v1")},
	}, committer.ScanPerasSnapshotDirectory(10, prefix, prefix, 8))
}

func TestRuntimeCapturePerasVisibleSnapshotIncludesPendingOverlay(t *testing.T) {
	committer := &Runtime{read: newReadState(), visibleSnapshots: true}
	prefix := testRuntimeRootDentryPrefix()
	keyA := testRuntimeDentryKeyForLabel("a")
	keyB := testRuntimeDentryKeyForLabel("b")
	keyC := testRuntimeDentryKeyForLabel("c")
	inodeBKey, err := fsmeta.EncodeInodeKey(testRuntimeMount, 42)
	require.NoError(t, err)
	opB := testRuntimeCreateOp(testRuntimeMount, fsmeta.RootInode, "b", 42, nil, nil)
	dentryBValue := testRuntimeDentryEffect(opB).Value

	installRuntimeSealedSegment(t, committer, testRuntimePerasSegmentForOverlay(keyA, []byte("sealed-a")))
	require.NoError(t, committer.read.overlay.Add(fsperas.OperationID{ClientID: "test", Seq: 1}, opB))
	capture, captured, err := committer.CapturePerasVisibleSnapshot(context.Background(), 11, compile.AuthorityScope{
		Mount:      testRuntimeMount.MountID,
		MountKeyID: testRuntimeMount.MountKeyID,
		Parents:    []fsmeta.InodeID{fsmeta.RootInode},
	})
	require.NoError(t, err)
	require.True(t, captured)
	require.Empty(t, capture.Evidence)
	require.NoError(t, committer.read.overlay.Add(fsperas.OperationID{ClientID: "test", Seq: 2}, testRuntimeCreateOp(testRuntimeMount, fsmeta.RootInode, "c", 43, []byte("visible-c"), nil)))

	_, _, ok := committer.GetPerasSnapshotOverlayView(11, keyC)
	require.False(t, ok)
	value, deleted, ok := committer.GetPerasSnapshotOverlayView(11, inodeBKey)
	require.True(t, ok)
	require.False(t, deleted)
	_, err = fsmeta.DecodeInodeValue(value)
	require.NoError(t, err)
	require.Equal(t, []fsperas.OverlayKV{
		{Key: keyA, Value: []byte("sealed-a")},
		{Key: keyB, Value: dentryBValue},
	}, committer.ScanPerasSnapshotDirectory(11, prefix, prefix, 8))
}

func TestRuntimeCaptureQuorumVisibleSnapshotWitnessesPendingFrontier(t *testing.T) {
	witness := &recordingRuntimeSegmentWitness{id: "witness-1"}
	committer, err := NewRuntime(Config{
		Authority:                    &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()},
		Witnesses:                    []fsperas.WitnessReplica{witness},
		Installer:                    &fakeRuntimePerasSegmentInstaller{},
		VisibleLog:                   &recordingVisibleLog{},
		Quorum:                       1,
		SegmentBatchSize:             1024,
		SegmentFlushEvery:            time.Hour,
		QuorumVisibleSnapshotCapture: true,
	})
	require.NoError(t, err)
	defer committer.Close()

	ctx := context.Background()
	_, err = committer.SubmitVisible(ctx, fsperas.OperationID{ClientID: "client", Seq: 1}, testRuntimeCreateOp(testRuntimeMount, fsmeta.RootInode, "a", 41, nil, nil), nil)
	require.NoError(t, err)
	capture, captured, err := committer.CapturePerasVisibleSnapshot(ctx, 12, compile.AuthorityScope{
		Mount:      testRuntimeMount.MountID,
		MountKeyID: testRuntimeMount.MountKeyID,
		Parents:    []fsmeta.InodeID{fsmeta.RootInode},
	})
	require.NoError(t, err)
	require.True(t, captured)
	require.NotEmpty(t, capture.Evidence)
	witnessSnapshot, err := witness.Probe(ctx, capture.Evidence[0].EpochID)
	require.NoError(t, err)
	require.NotEmpty(t, witnessSnapshot.Segments)
	require.Equal(t, witnessSnapshot.Segments[0].EpochID, capture.Evidence[0].EpochID)
	require.Equal(t, witnessSnapshot.Segments[0].SegmentRoot, capture.Evidence[0].EvidenceRoot)
	require.Equal(t, witnessSnapshot.Segments[0].SegmentPayloadDigest, capture.Evidence[0].PayloadDigest)
	require.Greater(t, witness.recordCount(), 0)
	_, err = committer.SubmitVisible(ctx, fsperas.OperationID{ClientID: "client", Seq: 2}, testRuntimeCreateOp(testRuntimeMount, fsmeta.RootInode, "b", 42, nil, nil), nil)
	require.NoError(t, err)

	prefix := testRuntimeRootDentryPrefix()
	rows := committer.ScanPerasSnapshotDirectory(12, prefix, prefix, 8)
	require.Len(t, rows, 1)
	require.Equal(t, testRuntimeDentryKeyForLabel("a"), rows[0].Key)
	require.True(t, committer.HasPerasSnapshotDirectory(12, prefix))
}

func TestRuntimeAppendsVisibleLogBeforeOverlay(t *testing.T) {
	log := &recordingVisibleLog{}
	provider := &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimeSegmentWitnesses(t, 3),
		VisibleLog:        log,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	op := testRuntimePerasOp([]byte("dentry/a"), []byte("inode/a"))
	_, err = committer.SubmitVisible(context.Background(), fsperas.OperationID{ClientID: "client", Seq: 1}, op, nil)
	require.NoError(t, err)
	require.Len(t, log.records, 1)
	require.Equal(t, fsmeta.OperationCreate, log.records[0].Operation.Kind)
	require.Equal(t, op.Delta.Authority, log.records[0].Scope)
	value, deleted, ok := committer.GetPerasOverlay(testRuntimeDentryEffect(op).Key)
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, testRuntimeDentryEffect(op).Value, value)
}

func TestRuntimeRecoversVisibleLogRecords(t *testing.T) {
	id := fsperas.OperationID{ClientID: "client", Seq: 1}
	op := testRuntimePerasOp([]byte("dentry/a"), []byte("inode/a"))
	replay, err := fsperas.ReplayOperationFromMaterialized(id, op)
	require.NoError(t, err)
	grant := testRuntimeCommitterGrant()
	log := &replayingVisibleLog{records: []fsperas.VisibleOperationRecord{
		testRuntimeVisibleRecord(grant, op.Delta.Authority, replay),
	}}
	provider := &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: grant}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimeSegmentWitnesses(t, 3),
		VisibleLog:        log,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	require.Equal(t, 1, committer.pendingOperations())
	value, deleted, ok := committer.GetPerasOverlay(testRuntimeDentryEffect(op).Key)
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, testRuntimeDentryEffect(op).Value, value)
	stats := committer.Stats()
	require.Equal(t, uint64(1), stats["visible_log_recover_total"])
}

func TestRuntimeRecoversVisibleLogOldEpochForSameHolder(t *testing.T) {
	id := fsperas.OperationID{ClientID: "client", Seq: 1}
	op := testRuntimePerasOp([]byte("dentry/a"), []byte("inode/a"))
	replay, err := fsperas.ReplayOperationFromMaterialized(id, op)
	require.NoError(t, err)
	old := testRuntimeCommitterGrant()
	old.GrantID = "grant-old"
	active := testRuntimeCommitterGrant()
	active.GrantID = "grant-new"
	active.EpochID = 2
	active.IssuedRootToken.Index = 2
	active.IssuedRootToken.Revision = 2
	log := &replayingVisibleLog{records: []fsperas.VisibleOperationRecord{
		testRuntimeVisibleRecord(old, op.Delta.Authority, replay),
	}}
	provider := &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: active}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimeSegmentWitnesses(t, 3),
		VisibleLog:        log,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	require.Equal(t, 1, committer.pendingOperations())
	stats := committer.Stats()
	require.Equal(t, uint64(1), stats["visible_log_recover_total"])
	require.Equal(t, uint64(1), stats["visible_log_recover_old_epoch_total"])
}

func TestRuntimeSkipsVisibleLogOldEpochWithPredecessorMismatch(t *testing.T) {
	id := fsperas.OperationID{ClientID: "client", Seq: 1}
	op := testRuntimePerasOp([]byte("dentry/a"), []byte("inode/a"))
	replay, err := fsperas.ReplayOperationFromMaterialized(id, op)
	require.NoError(t, err)
	old := testRuntimeCommitterGrant()
	old.GrantID = "grant-old"
	old.PredecessorDigest[0] = 1
	active := testRuntimeCommitterGrant()
	active.GrantID = "grant-new"
	active.EpochID = 2
	active.PredecessorDigest[0] = 2
	active.IssuedRootToken.Index = 2
	active.IssuedRootToken.Revision = 2
	log := &replayingVisibleLog{records: []fsperas.VisibleOperationRecord{
		testRuntimeVisibleRecord(old, op.Delta.Authority, replay),
	}}
	committer, err := NewRuntime(Config{
		Authority:         &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: active},
		Witnesses:         testRuntimeSegmentWitnesses(t, 3),
		VisibleLog:        log,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	require.Zero(t, committer.pendingOperations())
	stats := committer.Stats()
	require.Equal(t, uint64(0), stats["visible_log_recover_total"])
	require.Equal(t, uint64(1), stats["visible_log_recover_skip_total"])
}

func TestRuntimeSkipsVisibleLogFromDifferentRootLineage(t *testing.T) {
	id := fsperas.OperationID{ClientID: "client", Seq: 1}
	op := testRuntimePerasOp([]byte("dentry/a"), []byte("inode/a"))
	replay, err := fsperas.ReplayOperationFromMaterialized(id, op)
	require.NoError(t, err)
	recordGrant := testRuntimeCommitterGrant()
	recordGrant.RootClusterEpoch = 7
	active := testRuntimeCommitterGrant()
	active.RootClusterEpoch = 8
	log := &replayingVisibleLog{records: []fsperas.VisibleOperationRecord{
		testRuntimeVisibleRecord(recordGrant, op.Delta.Authority, replay),
	}}
	committer, err := NewRuntime(Config{
		Authority:         &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: active},
		Witnesses:         testRuntimeSegmentWitnesses(t, 3),
		VisibleLog:        log,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	require.Zero(t, committer.pendingOperations())
	stats := committer.Stats()
	require.Equal(t, uint64(0), stats["visible_log_recover_total"])
	require.Equal(t, uint64(1), stats["visible_log_recover_skip_total"])
}

func TestRuntimeFlushDurableDrainsRecoveredOldEpochWithoutRootPublish(t *testing.T) {
	id := fsperas.OperationID{ClientID: "client", Seq: 1}
	op := testRuntimePerasOp([]byte("dentry/a"), []byte("inode/a"))
	replay, err := fsperas.ReplayOperationFromMaterialized(id, op)
	require.NoError(t, err)
	old := testRuntimeCommitterGrant()
	old.GrantID = "grant-old"
	active := testRuntimeCommitterGrant()
	active.GrantID = "grant-new"
	active.EpochID = 2
	active.IssuedRootToken.Index = 2
	active.IssuedRootToken.Revision = 2
	log := &replayingVisibleLog{records: []fsperas.VisibleOperationRecord{
		testRuntimeVisibleRecord(old, op.Delta.Authority, replay),
	}}
	provider := &publishingRuntimeVisibleGrantProvider{
		fakeRuntimeVisibleGrantProvider: fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: active},
	}
	installer := &fakeRuntimePerasSegmentInstaller{}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimeSegmentWitnesses(t, 3),
		Installer:         installer,
		VisibleLog:        log,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	require.NoError(t, committer.FlushDurable(context.Background()))
	require.Equal(t, 1, installer.calls)
	require.Equal(t, 0, provider.sealCalls)
	require.Len(t, log.applied, 1)
	require.Equal(t, 0, committer.pendingOperations())
}

func TestRuntimeFlushPublishedFailsWhenRecoveredOldEpochCannotPublish(t *testing.T) {
	id := fsperas.OperationID{ClientID: "client", Seq: 1}
	op := testRuntimePerasOp([]byte("dentry/a"), []byte("inode/a"))
	replay, err := fsperas.ReplayOperationFromMaterialized(id, op)
	require.NoError(t, err)
	old := testRuntimeCommitterGrant()
	old.GrantID = "grant-old"
	active := testRuntimeCommitterGrant()
	active.GrantID = "grant-new"
	active.EpochID = 2
	active.IssuedRootToken.Index = 2
	active.IssuedRootToken.Revision = 2
	log := &replayingVisibleLog{records: []fsperas.VisibleOperationRecord{
		testRuntimeVisibleRecord(old, op.Delta.Authority, replay),
	}}
	provider := &publishingRuntimeVisibleGrantProvider{
		fakeRuntimeVisibleGrantProvider: fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: active},
	}
	installer := &fakeRuntimePerasSegmentInstaller{}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimeSegmentWitnesses(t, 3),
		Installer:         installer,
		VisibleLog:        log,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	require.ErrorIs(t, committer.FlushPublished(context.Background()), ErrPublishRequired)
	require.Equal(t, 1, installer.calls)
	require.Equal(t, 0, provider.sealCalls)
	require.Empty(t, log.applied)
	require.Equal(t, 1, committer.pendingOperations())
	_, ok := committer.Completion(id)
	require.False(t, ok, "publish-denied segment must not update retry dedup")
	committer.read.mu.RLock()
	require.Empty(t, committer.read.sealedSegments, "publish-denied segment must not enter sealed overlay")
	require.Empty(t, committer.read.segments, "publish-denied segment must not enter installed segment list")
	committer.read.mu.RUnlock()
}

func TestRuntimeVisibleLogFailureDoesNotPublishOverlay(t *testing.T) {
	logErr := errors.New("visible log unavailable")
	log := &recordingVisibleLog{err: logErr}
	provider := &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimeSegmentWitnesses(t, 3),
		VisibleLog:        log,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	op := testRuntimePerasOp([]byte("dentry/a"), []byte("inode/a"))
	_, err = committer.SubmitVisible(context.Background(), fsperas.OperationID{ClientID: "client", Seq: 1}, op, nil)
	require.ErrorIs(t, err, logErr)
	_, _, ok := committer.GetPerasOverlay(testRuntimeDentryEffect(op).Key)
	require.False(t, ok)
	require.Zero(t, committer.pendingOperations())
}

func TestRuntimePublishesRootSealAfterInstall(t *testing.T) {
	provider := &publishingRuntimeVisibleGrantProvider{
		fakeRuntimeVisibleGrantProvider: fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()},
	}
	installer := &fakeRuntimePerasSegmentInstaller{}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimeSegmentWitnesses(t, 3),
		Installer:         installer,
		VisibleLog:        &recordingVisibleLog{},
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	ctx := context.Background()
	require.NoError(t, commitRuntimePeras(ctx, committer, 1, []byte("dentry/a"), []byte("inode/a")))
	require.NoError(t, committer.FlushPublished(ctx))

	provider.mu.Lock()
	defer provider.mu.Unlock()
	require.Equal(t, 1, provider.sealCalls)
	require.Equal(t, provider.grant.GrantID, provider.sealedGrant.GrantID)
	require.Equal(t, installer.segment.Root, provider.sealedSegment.Root)
	require.Equal(t, installer.digest, provider.sealedDigest)
	stats := committer.Stats()
	require.Equal(t, uint64(1), stats["seal_total"])
	require.NotZero(t, stats["flush_latency_total_nanosecond"])
	require.NotZero(t, stats["witness_latency_total_nanosecond"])
	require.Equal(t, uint64(2), stats["witness_replica_append_total"])
	require.Equal(t, uint64(0), stats["witness_replica_append_error_total"])
	require.Contains(t, stats, "witness_replica_latency_total_nanosecond")
	require.Equal(t, uint64(1), stats["witness_quorum_total"])
	require.Contains(t, stats, "witness_quorum_latency_total_nanosecond")
	require.Equal(t, uint64(2), stats["witness_quorum_acks_last"])
	require.Equal(t, uint64(2), stats["witness_quorum_acks_max"])
	require.NotZero(t, stats["install_latency_total_nanosecond"])
	require.NotZero(t, stats["seal_latency_total_nanosecond"])
	require.NotZero(t, stats["flush_latency_max_nanosecond"])
	require.NotZero(t, stats["witness_latency_max_nanosecond"])
	require.NotZero(t, stats["install_latency_max_nanosecond"])
	require.NotZero(t, stats["seal_latency_max_nanosecond"])
	require.NotZero(t, stats["flush_latency_average_nanosecond"])
	require.NotZero(t, stats["witness_latency_average_nanosecond"])
	require.NotZero(t, stats["install_latency_average_nanosecond"])
	require.NotZero(t, stats["seal_latency_average_nanosecond"])
	require.Equal(t, uint64(1), stats["flush_batch_total"])
	require.Equal(t, uint64(1), stats["flush_jobs_total"])
	require.Equal(t, uint64(1), stats["flush_jobs_last"])
	require.Equal(t, uint64(1), stats["flush_jobs_max"])
}

func TestRuntimeWitnessQuorumSelectionRotatesBySegmentRoot(t *testing.T) {
	provider := &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	witnesses := testRuntimeSegmentWitnesses(t, 5)
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         witnesses,
		Quorum:            2,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	scope := compile.AuthorityScope{Mount: "vol", MountKeyID: 1}
	for i := uint64(1); i <= 64; i++ {
		var root [32]byte
		var digest [32]byte
		binary.BigEndian.PutUint64(root[24:], i)
		binary.BigEndian.PutUint64(digest[24:], i*17)
		require.NoError(t, committer.appendSegmentWitnessRecords(context.Background(), scope, []fsperas.SegmentWitnessRecord{{
			EpochID:              1,
			SegmentRoot:          root,
			SegmentPayloadDigest: digest,
			SegmentPayloadSize:   1,
		}}))
	}

	total := 0
	used := 0
	maxRecords := 0
	for _, witness := range witnesses {
		recorder := witness.(*recordingRuntimeSegmentWitness)
		count := recorder.recordCount()
		total += count
		if count > 0 {
			used++
		}
		if count > maxRecords {
			maxRecords = count
		}
	}
	require.Equal(t, 64*committer.quorum, total)
	require.Equal(t, len(witnesses), used)
	require.Less(t, maxRecords, 64)
}

func TestRuntimeCanStopAtDurablePersistence(t *testing.T) {
	provider := &publishingRuntimeVisibleGrantProvider{
		fakeRuntimeVisibleGrantProvider: fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()},
	}
	installer := &fakeRuntimePerasSegmentInstaller{}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimeSegmentWitnesses(t, 3),
		Installer:         installer,
		VisibleLog:        &recordingVisibleLog{},
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	ctx := context.Background()
	require.NoError(t, commitRuntimePeras(ctx, committer, 1, []byte("dentry/a"), []byte("inode/a")))
	require.NoError(t, committer.FlushTo(ctx, fsperas.SegmentPersistenceDurable))

	provider.mu.Lock()
	require.Equal(t, 0, provider.sealCalls)
	provider.mu.Unlock()
	stats := committer.Stats()
	require.Equal(t, uint64(1), stats["flush_total"])
	require.Equal(t, uint64(0), stats["seal_total"])
	require.NotZero(t, stats["witness_latency_total_nanosecond"])
	require.NotZero(t, stats["install_latency_total_nanosecond"])
	require.Zero(t, stats["seal_latency_total_nanosecond"])
	require.True(t, committer.segmentInstalled(installer.segment.Root))
	require.Equal(t, 0, stats["pending"])
}

func TestRuntimeFlushBatchCarriesMultipleSegmentJobs(t *testing.T) {
	mount := fsmeta.MountIdentity{MountID: "vol", MountKeyID: 1}
	keys := make([][2][]byte, 0, 4)
	for bucket := fsmeta.AffinityBucket(0); len(keys) < cap(keys); bucket++ {
		first, second := testRuntimeBucketKeys(t, mount, bucket)
		keys = append(keys, [2][]byte{first, second})
	}
	witness := newGatedRuntimeSegmentWitness("witness-0", 1)
	installer := &fakeRuntimePerasSegmentInstaller{}
	committer, err := NewRuntime(Config{
		Authority:                 &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()},
		Witnesses:                 []fsperas.WitnessReplica{witness},
		Installer:                 installer,
		VisibleLog:                &recordingVisibleLog{},
		Quorum:                    1,
		SegmentBatchSize:          1024,
		SegmentMaxReplayMutations: 2,
		SegmentInstallParallelism: 1,
		SegmentFlushParallelism:   2,
		SegmentFlushEvery:         time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for idx, pair := range keys {
		require.NoError(t, commitRuntimePeras(ctx, committer, uint64(idx+1), pair[0], pair[1]))
	}
	require.NoError(t, committer.FlushDurable(ctx))
	require.Equal(t, len(keys), witness.Count())
	require.Equal(t, 1, witness.BatchCount())
	require.Equal(t, 1, witness.MaxConcurrent())
	require.Equal(t, len(keys), installer.calls)
	stats := committer.Stats()
	require.Equal(t, uint64(1), stats["flush_batch_total"])
	require.Equal(t, uint64(len(keys)), stats["flush_jobs_total"])
	require.Equal(t, uint64(len(keys)), stats["flush_jobs_max"])
	require.Equal(t, uint64(1), stats["witness_batch_total"])
	require.Equal(t, uint64(len(keys)), stats["witness_batch_records_last"])
	require.Equal(t, uint64(len(keys)), stats["witness_batch_records_max"])
	require.Equal(t, 2, stats["segment_flush_parallelism"])
}

func TestRuntimeCommitsConcurrentFlushBatchesInFreezeOrder(t *testing.T) {
	installer := &sequenceDelayingRuntimePerasSegmentInstaller{}
	committer, err := NewRuntime(Config{
		Authority:                 &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()},
		Witnesses:                 testRuntimeSegmentWitnesses(t, 1),
		Installer:                 installer,
		VisibleLog:                &recordingVisibleLog{},
		Quorum:                    1,
		SegmentBatchSize:          1024,
		SegmentMaxReplayMutations: 2,
		SegmentInstallParallelism: 2,
		SegmentFlushParallelism:   2,
		SegmentFlushEvery:         time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	ctx := context.Background()
	require.NoError(t, commitRuntimePeras(ctx, committer, 1, []byte("dentry/a"), []byte("inode/a")))
	require.NoError(t, commitRuntimePeras(ctx, committer, 2, []byte("dentry/b"), []byte("inode/b")))
	require.NoError(t, committer.FlushDurable(ctx))

	committer.read.mu.RLock()
	defer committer.read.mu.RUnlock()
	require.Len(t, committer.read.segments, 2)
	require.Equal(t, uint64(1), committer.read.segments[0].Completions[0].OpID.Seq)
	require.Equal(t, uint64(2), committer.read.segments[1].Completions[0].OpID.Seq)
}

func TestRuntimeBackgroundFlushLimitScalesWithInstallParallelism(t *testing.T) {
	committer, err := NewRuntime(Config{
		Authority:                  &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()},
		Witnesses:                  testRuntimeSegmentWitnesses(t, 1),
		Installer:                  &fakeRuntimePerasSegmentInstaller{},
		VisibleLog:                 &recordingVisibleLog{},
		Quorum:                     1,
		SegmentBatchSize:           1,
		SegmentMaxReplayOperations: 3,
		SegmentInstallParallelism:  4,
		SegmentFlushParallelism:    4,
		SegmentFlushEvery:          time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	stats := committer.Stats()
	require.Equal(t, 4, stats["segment_flush_parallelism"])
	require.Equal(t, 12, stats["background_flush_operation_limit"])
}

func TestRuntimeAdmissionWaitsForPendingDrain(t *testing.T) {
	installer := &delayingRuntimePerasSegmentInstaller{delay: 50 * time.Millisecond}
	committer, err := NewRuntime(Config{
		Authority:              &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()},
		Witnesses:              testRuntimeSegmentWitnesses(t, 1),
		Installer:              installer,
		VisibleLog:             &recordingVisibleLog{},
		Quorum:                 1,
		SegmentBatchSize:       1024,
		AdmissionPendingLimit:  1,
		SegmentFlushEvery:      time.Hour,
		BackgroundFlushTimeout: time.Second,
	})
	require.NoError(t, err)
	defer committer.Close()

	ctx := context.Background()
	require.NoError(t, commitRuntimePeras(ctx, committer, 1, []byte("dentry/a"), []byte("inode/a")))
	require.Equal(t, 1, committer.Stats()["pending"])

	commitDone := make(chan error, 1)
	go func() {
		commitDone <- commitRuntimePeras(ctx, committer, 2, []byte("dentry/b"), []byte("inode/b"))
	}()

	require.Eventually(t, func() bool {
		stats := committer.Stats()
		return stats["admission_wait_total"] == uint64(1) && stats["admission_waiting"] == int64(1)
	}, time.Second, 10*time.Millisecond)

	select {
	case err := <-commitDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("admission did not resume after pending drain")
	}
	stats := committer.Stats()
	require.Equal(t, uint64(1), stats["admission_wait_total"])
	require.Equal(t, int64(0), stats["admission_waiting"])
	require.NotZero(t, stats["admission_wait_latency_total_nanosecond"])
	require.Equal(t, 1, stats["pending"])
	require.Equal(t, int32(1), installer.calls.Load())
}

func TestRuntimeReturnsInstalledCompletionOnRetry(t *testing.T) {
	provider := &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	installer := &fakeRuntimePerasSegmentInstaller{}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimeSegmentWitnesses(t, 3),
		Installer:         installer,
		VisibleLog:        &recordingVisibleLog{},
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	ctx := context.Background()
	opID := fsperas.OperationID{ClientID: "client", Seq: 7}
	delta := testRuntimePerasOp([]byte("dentry/a"), []byte("inode/a"))
	ack, err := committer.SubmitVisible(ctx, opID, delta, nil)
	require.NoError(t, err)
	require.Equal(t, opID, ack.OpID)
	require.NoError(t, committer.FlushDurable(ctx))
	completion, ok := committer.Completion(opID)
	require.True(t, ok)
	require.Equal(t, opID, completion.OpID)

	retryAck, err := committer.SubmitVisible(ctx, opID, delta, nil)
	require.NoError(t, err)
	require.Equal(t, ack.OpID, retryAck.OpID)
	require.Equal(t, ack.EpochID, retryAck.EpochID)
	require.Equal(t, 1, installer.calls)
	require.Equal(t, 0, committer.Stats()["pending"])
}

func TestRuntimeRecoversAppliedVisibleCompletionWhenMaterialized(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "wal")
	mgr, err := wal.Open(wal.Config{Dir: dir})
	require.NoError(t, err)
	log, err := NewWALVisibleLog(mgr, wal.DurabilityFlushed)
	require.NoError(t, err)
	log.SetRetainAppliedRecords(true)
	provider := &nonSealingRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	installer := &fakeRuntimePerasSegmentInstaller{materializes: true}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Installer:         installer,
		VisibleLog:        log,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)

	opID := fsperas.OperationID{ClientID: "client", Seq: 7}
	delta := testRuntimePerasOp([]byte("dentry/a"), []byte("inode/a"))
	ack, err := committer.SubmitVisible(ctx, opID, delta, nil)
	require.NoError(t, err)
	require.NoError(t, committer.FlushDurable(ctx))
	require.True(t, installer.materialize)
	require.Len(t, log.Records(), 0)
	committer.Close()
	require.NoError(t, mgr.Close())

	mgr, err = wal.Open(wal.Config{Dir: dir})
	require.NoError(t, err)
	defer func() { require.NoError(t, mgr.Close()) }()
	reopenLog, err := NewWALVisibleLog(mgr, wal.DurabilityFlushed)
	require.NoError(t, err)
	reopenLog.SetRetainAppliedRecords(true)
	defer reopenLog.Close()
	reopenedInstaller := &fakeRuntimePerasSegmentInstaller{materializes: true}
	reopened, err := NewRuntime(Config{
		Authority:         provider,
		Installer:         reopenedInstaller,
		VisibleLog:        reopenLog,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer reopened.Close()

	completion, ok := reopened.Completion(opID)
	require.True(t, ok)
	require.Equal(t, opID, completion.OpID)
	retryAck, err := reopened.SubmitVisible(ctx, opID, delta, nil)
	require.NoError(t, err)
	require.Equal(t, ack.OpID, retryAck.OpID)
	require.Equal(t, 0, reopenedInstaller.calls)
	require.Equal(t, 0, reopened.pendingOperations())
}

func TestRuntimeRejectsInstalledCompletionIDCollision(t *testing.T) {
	provider := &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	installer := &fakeRuntimePerasSegmentInstaller{}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimeSegmentWitnesses(t, 3),
		Installer:         installer,
		VisibleLog:        &recordingVisibleLog{},
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	ctx := context.Background()
	opID := fsperas.OperationID{ClientID: "client", Seq: 7}
	first := testRuntimePerasOp([]byte("dentry/a"), []byte("inode/a"))
	_, err = committer.SubmitVisible(ctx, opID, first, nil)
	require.NoError(t, err)
	require.NoError(t, committer.FlushDurable(ctx))

	colliding := testRuntimePerasOp([]byte("dentry/b"), []byte("inode/b"))
	_, err = committer.SubmitVisible(ctx, opID, colliding, nil)
	require.ErrorIs(t, err, fsperas.ErrDuplicateOperation)
	require.Equal(t, 1, installer.calls)
	require.Equal(t, uint64(1), committer.Stats()["commit_total"])
}

func TestRuntimeReturnsPendingAckOnRetry(t *testing.T) {
	provider := &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimeSegmentWitnesses(t, 3),
		VisibleLog:        &recordingVisibleLog{},
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	ctx := context.Background()
	opID := fsperas.OperationID{ClientID: "client", Seq: 9}
	delta := testRuntimePerasOp([]byte("dentry/a"), []byte("inode/a"))
	first, err := committer.SubmitVisible(ctx, opID, delta, nil)
	require.NoError(t, err)
	second, err := committer.SubmitVisible(ctx, opID, delta, func(context.Context, compile.MaterializedOp, fsperas.AdmissionContext) (fsperas.AdmissionResult, bool, error) {
		t.Fatal("pending retry should not re-run admission")
		return fsperas.AdmissionResult{}, false, nil
	})
	require.NoError(t, err)

	require.Equal(t, first, second)
	stats := committer.Stats()
	require.Equal(t, uint64(1), stats["commit_total"])
	require.Equal(t, 1, stats["pending"])
	require.Equal(t, 3, stats["overlay_keys"])
}

func TestRuntimeShutdownFlushesPendingSegment(t *testing.T) {
	provider := &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	installer := &fakeRuntimePerasSegmentInstaller{}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimeSegmentWitnesses(t, 3),
		Installer:         installer,
		VisibleLog:        &recordingVisibleLog{},
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, commitRuntimePeras(ctx, committer, 1, []byte("dentry/a"), []byte("inode/a")))
	require.Equal(t, 1, committer.Stats()["pending"])
	require.NoError(t, committer.Shutdown(ctx))
	require.Equal(t, 0, committer.Stats()["pending"])
	require.Equal(t, 1, installer.calls)

	err = commitRuntimePeras(ctx, committer, 2, []byte("dentry/b"), []byte("inode/b"))
	require.ErrorIs(t, err, ErrRuntimeClosed)
}

func TestRuntimeFlushRenewsAuthorityBeforeWitness(t *testing.T) {
	provider := &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	installer := &fakeRuntimePerasSegmentInstaller{}
	witnesses := []fsperas.WitnessReplica{
		&authorityRenewCheckingRuntimeSegmentWitness{id: "witness-1", provider: provider},
		&authorityRenewCheckingRuntimeSegmentWitness{id: "witness-2", provider: provider},
		&authorityRenewCheckingRuntimeSegmentWitness{id: "witness-3", provider: provider},
	}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         witnesses,
		Installer:         installer,
		VisibleLog:        &recordingVisibleLog{},
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	ctx := context.Background()
	require.NoError(t, commitRuntimePeras(ctx, committer, 1, []byte("dentry/a"), []byte("inode/a")))
	beforeFlush := provider.acquireCalls.Load()
	for _, witness := range witnesses {
		witness.(*authorityRenewCheckingRuntimeSegmentWitness).minAcquireCalls = beforeFlush + 1
	}
	require.NoError(t, committer.FlushDurable(ctx))
	require.Greater(t, provider.acquireCalls.Load(), beforeFlush)
	require.Equal(t, 0, committer.Stats()["pending"])
	require.Equal(t, 1, installer.calls)
}

func TestRuntimeFlushPreservesCatalogSegmentAcrossFSMetaBuckets(t *testing.T) {
	provider := &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	installer := &fakeRuntimePerasSegmentInstaller{}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimeSegmentWitnesses(t, 3),
		Installer:         installer,
		VisibleLog:        &recordingVisibleLog{},
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	mount := fsmeta.MountIdentity{MountID: "vol", MountKeyID: 1}
	leftA, leftB := testRuntimeBucketKeys(t, mount, 1)
	rightA, rightB := testRuntimeBucketKeys(t, mount, 2)
	ctx := context.Background()
	require.NoError(t, commitRuntimePeras(ctx, committer, 1, leftA, leftB))
	require.NoError(t, commitRuntimePeras(ctx, committer, 2, rightA, rightB))
	require.NoError(t, committer.FlushDurable(ctx))

	stats := committer.Stats()
	require.Equal(t, uint64(1), stats["flush_total"])
	require.Equal(t, uint64(1), stats["segment_total"])
	require.Equal(t, uint64(2), stats["segment_operations_total"])
	require.Equal(t, 1, installer.calls)
}

func TestRuntimeAcceptsCrossBucketCreateForCatalogInstall(t *testing.T) {
	provider := &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	installer := &fakeRuntimePerasSegmentInstaller{}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimeSegmentWitnesses(t, 3),
		Installer:         installer,
		VisibleLog:        &recordingVisibleLog{},
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	mount := fsmeta.MountIdentity{MountID: "vol", MountKeyID: 1}
	leftA, _ := testRuntimeBucketKeys(t, mount, 1)
	rightA, _ := testRuntimeBucketKeys(t, mount, 2)
	ctx := context.Background()
	op := testRuntimePerasOp(leftA, rightA)
	_, err = committer.SubmitVisible(ctx, fsperas.OperationID{ClientID: "client", Seq: 1}, op, nil)
	require.NoError(t, err)

	stats := committer.Stats()
	require.Equal(t, uint64(0), stats["flush_total"])
	require.Equal(t, uint64(0), stats["segment_total"])
	require.Equal(t, uint64(0), stats["segment_operations_total"])
	require.Equal(t, 0, installer.calls)
	require.Equal(t, 1, stats["pending"])
	_, _, ok := committer.GetPerasOverlay(testRuntimeDentryEffect(op).Key)
	require.True(t, ok)
	_, _, ok = committer.GetPerasOverlay(testRuntimeInodeEffect(op).Key)
	require.True(t, ok)

	require.NoError(t, committer.FlushDurable(ctx))
	stats = committer.Stats()
	require.Equal(t, uint64(1), stats["flush_total"])
	require.Equal(t, uint64(1), stats["segment_total"])
	require.Equal(t, 1, installer.calls)
	require.Equal(t, 0, stats["pending"])
}

func TestRuntimeFlushHonorsReplayMutationBudget(t *testing.T) {
	provider := &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	installer := &fakeRuntimePerasSegmentInstaller{}
	committer, err := NewRuntime(Config{
		Authority:                 provider,
		Witnesses:                 testRuntimeSegmentWitnesses(t, 3),
		Installer:                 installer,
		VisibleLog:                &recordingVisibleLog{},
		SegmentBatchSize:          1024,
		SegmentMaxReplayMutations: 4,
		SegmentFlushEvery:         time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	ctx := context.Background()
	require.NoError(t, commitRuntimePeras(ctx, committer, 1, []byte("dentry/a"), []byte("inode/a")))
	require.NoError(t, commitRuntimePeras(ctx, committer, 2, []byte("dentry/b"), []byte("inode/b")))
	require.NoError(t, commitRuntimePeras(ctx, committer, 3, []byte("dentry/c"), []byte("inode/c")))
	require.NoError(t, committer.FlushDurable(ctx))

	stats := committer.Stats()
	require.Equal(t, uint64(3), stats["flush_total"])
	require.Equal(t, uint64(3), stats["segment_total"])
	require.Equal(t, uint64(3), stats["segment_operations_total"])
	require.Equal(t, 3, installer.calls)
}

func TestRuntimeRetriesRetryableSegmentInstall(t *testing.T) {
	provider := &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	installer := &flakyRuntimePerasSegmentInstaller{failures: 2}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimeSegmentWitnesses(t, 3),
		Installer:         installer,
		VisibleLog:        &recordingVisibleLog{},
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	require.NoError(t, commitRuntimePeras(context.Background(), committer, 1, []byte("dentry/a"), []byte("inode/a")))
	require.NoError(t, committer.FlushDurable(context.Background()))

	require.Equal(t, 3, installer.calls)
	stats := committer.Stats()
	require.Equal(t, uint64(2), stats["retry_total"])
	require.Equal(t, uint64(2), stats["retry_stale_epoch_total"])
	require.Equal(t, uint64(1), stats["flush_total"])
}

func TestPerasSegmentInstallRetryDelayKeepsStaleEpochShort(t *testing.T) {
	stale := nokverrors.New(nokverrors.KindStaleEpoch, "stale")
	require.Equal(t, defaultPerasSegmentInstallStaleBackoff, perasSegmentInstallRetryDelay(stale, 0))
	require.Equal(t, defaultPerasSegmentInstallStaleMaxBackoff, perasSegmentInstallRetryDelay(stale, 20))

	unavailable := nokverrors.New(nokverrors.KindUnavailable, "down")
	require.Equal(t, defaultPerasSegmentInstallRetryBackoff, perasSegmentInstallRetryDelay(unavailable, 0))
	require.Equal(t, defaultPerasSegmentInstallMaxBackoff, perasSegmentInstallRetryDelay(unavailable, 20))
}

func TestRuntimeFlushRequiresInstaller(t *testing.T) {
	provider := &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimeSegmentWitnesses(t, 3),
		VisibleLog:        &recordingVisibleLog{},
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	ctx := context.Background()
	require.NoError(t, commitRuntimePeras(ctx, committer, 1, []byte("dentry/a"), []byte("inode/a")))
	require.ErrorIs(t, committer.FlushDurable(ctx), ErrRuntimeInvalid)

	stats := committer.Stats()
	require.Equal(t, uint64(0), stats["flush_total"])
	require.Equal(t, uint64(0), stats["segment_total"])
	require.Equal(t, 1, stats["pending"])
	value, deleted, ok := committer.GetPerasOverlay(testRuntimeDentryKeyForLabel("a"))
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, []byte("dentry-value"), value)
}

func TestRuntimeRecoversWitnessSegment(t *testing.T) {
	witnesses := testRuntimeSegmentWitnesses(t, 3)
	provider := &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         witnesses,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	holder, err := fsperas.NewHolder(fsperas.HolderConfig{EpochID: 1, HolderID: "holder-a"})
	require.NoError(t, err)
	delta := testRuntimePerasOp([]byte("dentry/recovered"), []byte("inode/recovered"))
	recoveredKey := testRuntimeDentryEffect(delta).Key
	_, _, err = holder.Submit(context.Background(), fsperas.OperationID{ClientID: "client", Seq: 1}, delta)
	require.NoError(t, err)
	plan, scope, err := holder.BuildPendingReplayPlan(10)
	require.NoError(t, err)
	segment, err := fsperas.BuildPerasSegmentFromReplayPlan(plan)
	require.NoError(t, err)
	payload, err := fsperas.EncodePerasSegment(segment)
	require.NoError(t, err)
	digest, err := fsperas.PerasSegmentPayloadDigest(payload)
	require.NoError(t, err)
	requireRuntimeSegmentWitness(t, committer, scope, holder, segment, payload, digest)

	installer := &fakeRuntimePerasSegmentInstaller{}
	recoverer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         witnesses,
		Installer:         installer,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer recoverer.Close()

	require.NoError(t, recoverer.RecoverWitnessSegments(context.Background(), scope, holder.EpochID()))
	require.Equal(t, 1, installer.calls)
	require.Equal(t, segment.Root, installer.segment.Root)
	require.Equal(t, uint64(1), recoverer.Stats()["segment_recovery_install_total"])
	require.Equal(t, uint64(0), recoverer.Stats()["segment_recovery_skip_total"])
	value, deleted, ok := recoverer.GetPerasOverlay(recoveredKey)
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, []byte("dentry-value"), value)
}

func TestRuntimeRecoveryPrefersInstalledCatalog(t *testing.T) {
	mount := fsmeta.MountIdentity{MountID: "vol", MountKeyID: 1}
	dentryKey, err := fsmeta.EncodeDentryKey(mount, fsmeta.RootInode, "restored")
	require.NoError(t, err)
	inodeKey, err := fsmeta.EncodeInodeKey(mount, 10)
	require.NoError(t, err)
	witnesses := testRuntimeSegmentWitnesses(t, 3)
	provider := &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         witnesses,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	holder, err := fsperas.NewHolder(fsperas.HolderConfig{EpochID: 1, HolderID: "holder-a"})
	require.NoError(t, err)
	delta := testRuntimePerasOp(dentryKey, inodeKey)
	delta = testRuntimePerasOpWithScope(delta, compile.AuthorityScope{
		Mount:      delta.Delta.Authority.Mount,
		MountKeyID: delta.Delta.Authority.MountKeyID,
		Parents:    []fsmeta.InodeID{fsmeta.RootInode},
		Inodes:     []fsmeta.InodeID{10},
	})
	_, _, err = holder.Submit(context.Background(), fsperas.OperationID{ClientID: "client", Seq: 1}, delta)
	require.NoError(t, err)
	plan, scope, err := holder.BuildPendingReplayPlan(10)
	require.NoError(t, err)
	segment, err := fsperas.BuildPerasSegmentFromReplayPlan(plan)
	require.NoError(t, err)
	payload, err := fsperas.EncodePerasSegment(segment)
	require.NoError(t, err)
	digest, err := fsperas.PerasSegmentPayloadDigest(payload)
	require.NoError(t, err)
	requireRuntimeSegmentWitness(t, committer, scope, holder, segment, payload, digest)

	seal := testRuntimeSegmentRootSeal("grant-1", "holder-a", scope, time.Now())
	seal.SegmentRoot = segment.Root
	seal.SegmentPayloadDigest = digest
	scanner := &fakeRuntimePerasCatalogScanner{rows: testRuntimePerasCatalogRows(t, segment, 99)}
	installer := &fakeRuntimePerasSegmentInstaller{}
	provider.seals = []rootproto.VisibleAuthoritySeal{seal}
	recoverer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         witnesses,
		Installer:         installer,
		CatalogScanner:    scanner,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer recoverer.Close()

	require.NoError(t, recoverer.RecoverWitnessSegments(context.Background(), scope, holder.EpochID()))
	require.Equal(t, 0, installer.calls)
	require.Positive(t, scanner.calls)
	require.Equal(t, uint64(1), recoverer.Stats()["segment_total"])
	require.Equal(t, uint64(1), recoverer.Stats()["segment_catalog_load_total"])
	require.Equal(t, uint64(0), recoverer.Stats()["segment_recovery_install_total"])
	require.Equal(t, uint64(1), recoverer.Stats()["segment_recovery_skip_total"])
	value, deleted, ok := recoverer.GetPerasOverlay(dentryKey)
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, []byte("dentry-value"), value)
}

func TestRuntimeRecoversRootSealedSegmentFromWitnessWhenCatalogMissing(t *testing.T) {
	mount := fsmeta.MountIdentity{MountID: "vol", MountKeyID: 1}
	dentryKey, err := fsmeta.EncodeDentryKey(mount, fsmeta.RootInode, "rooted")
	require.NoError(t, err)
	inodeKey, err := fsmeta.EncodeInodeKey(mount, 11)
	require.NoError(t, err)
	witnesses := testRuntimeSegmentWitnesses(t, 3)
	provider := &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         witnesses,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	holder, err := fsperas.NewHolder(fsperas.HolderConfig{EpochID: 1, HolderID: "holder-a"})
	require.NoError(t, err)
	delta := testRuntimePerasOp(dentryKey, inodeKey)
	delta = testRuntimePerasOpWithScope(delta, compile.AuthorityScope{
		Mount:      delta.Delta.Authority.Mount,
		MountKeyID: delta.Delta.Authority.MountKeyID,
		Parents:    []fsmeta.InodeID{fsmeta.RootInode},
		Inodes:     []fsmeta.InodeID{11},
	})
	_, _, err = holder.Submit(context.Background(), fsperas.OperationID{ClientID: "client", Seq: 1}, delta)
	require.NoError(t, err)
	plan, scope, err := holder.BuildPendingReplayPlan(10)
	require.NoError(t, err)
	segment, err := fsperas.BuildPerasSegmentFromReplayPlan(plan)
	require.NoError(t, err)
	payload, err := fsperas.EncodePerasSegment(segment)
	require.NoError(t, err)
	digest, err := fsperas.PerasSegmentPayloadDigest(payload)
	require.NoError(t, err)
	requireRuntimeSegmentWitness(t, committer, scope, holder, segment, payload, digest)

	seal := testRuntimeSegmentRootSeal("grant-1", "holder-a", scope, time.Now())
	seal.SegmentRoot = segment.Root
	seal.SegmentPayloadDigest = digest
	seal.OperationCount = segment.Stats().OperationCount
	seal.EntryCount = segment.Stats().EntryCount
	provider.seals = []rootproto.VisibleAuthoritySeal{seal}
	installer := &fakeRuntimePerasSegmentInstaller{}
	recoverer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         witnesses,
		Installer:         installer,
		CatalogScanner:    &fakeRuntimePerasCatalogScanner{},
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer recoverer.Close()

	require.NoError(t, recoverer.LoadRootSealedSegments(context.Background(), scope))
	require.Equal(t, 1, installer.calls)
	require.Equal(t, segment.Root, installer.segment.Root)
	for _, witness := range witnesses {
		recording := witness.(*recordingRuntimeSegmentWitness)
		require.Zero(t, recording.probeCalls)
		require.Equal(t, 1, recording.probeSegmentCalls)
	}
	stats := recoverer.Stats()
	require.Equal(t, uint64(1), stats["root_sealed_segment_total"])
	require.Equal(t, uint64(1), stats["root_sealed_segment_missing_total"])
	require.Equal(t, uint64(1), stats["segment_recovery_install_total"])
	value, deleted, ok := recoverer.GetPerasOverlay(dentryKey)
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, []byte("dentry-value"), value)
}

func TestRuntimeRecoversRootSealedSegmentWithWitnessScanFallback(t *testing.T) {
	mount := fsmeta.MountIdentity{MountID: "vol", MountKeyID: 1}
	dentryKey, err := fsmeta.EncodeDentryKey(mount, fsmeta.RootInode, "scan-fallback")
	require.NoError(t, err)
	inodeKey, err := fsmeta.EncodeInodeKey(mount, 12)
	require.NoError(t, err)
	recorders, witnesses := scanOnlyRuntimeSegmentWitnesses(t, 3)
	provider := &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         witnesses,
		Quorum:            3,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	holder, err := fsperas.NewHolder(fsperas.HolderConfig{EpochID: 1, HolderID: "holder-a"})
	require.NoError(t, err)
	delta := testRuntimePerasOp(dentryKey, inodeKey)
	delta = testRuntimePerasOpWithScope(delta, compile.AuthorityScope{
		Mount:      delta.Delta.Authority.Mount,
		MountKeyID: delta.Delta.Authority.MountKeyID,
		Parents:    []fsmeta.InodeID{fsmeta.RootInode},
		Inodes:     []fsmeta.InodeID{12},
	})
	_, _, err = holder.Submit(context.Background(), fsperas.OperationID{ClientID: "client", Seq: 1}, delta)
	require.NoError(t, err)
	plan, scope, err := holder.BuildPendingReplayPlan(10)
	require.NoError(t, err)
	segment, err := fsperas.BuildPerasSegmentFromReplayPlan(plan)
	require.NoError(t, err)
	payload, err := fsperas.EncodePerasSegment(segment)
	require.NoError(t, err)
	digest, err := fsperas.PerasSegmentPayloadDigest(payload)
	require.NoError(t, err)
	requireRuntimeSegmentWitness(t, committer, scope, holder, segment, payload, digest)

	seal := testRuntimeSegmentRootSeal("grant-1", "holder-a", scope, time.Now())
	seal.SegmentRoot = segment.Root
	seal.SegmentPayloadDigest = digest
	seal.OperationCount = segment.Stats().OperationCount
	seal.EntryCount = segment.Stats().EntryCount
	provider.seals = []rootproto.VisibleAuthoritySeal{seal}
	installer := &fakeRuntimePerasSegmentInstaller{}
	recoverer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         witnesses,
		Installer:         installer,
		CatalogScanner:    &fakeRuntimePerasCatalogScanner{},
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer recoverer.Close()

	require.NoError(t, recoverer.LoadRootSealedSegments(context.Background(), scope))
	require.Equal(t, 1, installer.calls)
	require.Equal(t, segment.Root, installer.segment.Root)
	for _, recorder := range recorders {
		require.Equal(t, 1, recorder.probeSegmentCalls)
		require.Equal(t, 1, recorder.probeCalls)
	}
	stats := recoverer.Stats()
	require.Equal(t, uint64(1), stats["root_sealed_segment_missing_total"])
	require.Equal(t, uint64(1), stats["segment_recovery_install_total"])
	value, deleted, ok := recoverer.GetPerasOverlay(dentryKey)
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, []byte("dentry-value"), value)
}

func TestRuntimeRecoversRootSealedSegmentWhenCatalogScanFails(t *testing.T) {
	mount := fsmeta.MountIdentity{MountID: "vol", MountKeyID: 1}
	dentryKey, err := fsmeta.EncodeDentryKey(mount, fsmeta.RootInode, "catalog-error")
	require.NoError(t, err)
	inodeKey, err := fsmeta.EncodeInodeKey(mount, 13)
	require.NoError(t, err)
	witnesses := testRuntimeSegmentWitnesses(t, 3)
	provider := &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         witnesses,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	holder, err := fsperas.NewHolder(fsperas.HolderConfig{EpochID: 1, HolderID: "holder-a"})
	require.NoError(t, err)
	delta := testRuntimePerasOp(dentryKey, inodeKey)
	delta = testRuntimePerasOpWithScope(delta, compile.AuthorityScope{
		Mount:      delta.Delta.Authority.Mount,
		MountKeyID: delta.Delta.Authority.MountKeyID,
		Parents:    []fsmeta.InodeID{fsmeta.RootInode},
		Inodes:     []fsmeta.InodeID{13},
	})
	_, _, err = holder.Submit(context.Background(), fsperas.OperationID{ClientID: "client", Seq: 1}, delta)
	require.NoError(t, err)
	plan, scope, err := holder.BuildPendingReplayPlan(10)
	require.NoError(t, err)
	segment, err := fsperas.BuildPerasSegmentFromReplayPlan(plan)
	require.NoError(t, err)
	payload, err := fsperas.EncodePerasSegment(segment)
	require.NoError(t, err)
	digest, err := fsperas.PerasSegmentPayloadDigest(payload)
	require.NoError(t, err)
	requireRuntimeSegmentWitness(t, committer, scope, holder, segment, payload, digest)

	seal := testRuntimeSegmentRootSeal("grant-1", "holder-a", scope, time.Now())
	seal.SegmentRoot = segment.Root
	seal.SegmentPayloadDigest = digest
	seal.OperationCount = segment.Stats().OperationCount
	seal.EntryCount = segment.Stats().EntryCount
	provider.seals = []rootproto.VisibleAuthoritySeal{seal}
	installer := &fakeRuntimePerasSegmentInstaller{}
	scanner := &fakeRuntimePerasCatalogScanner{err: nokverrors.New(nokverrors.KindRetryExhausted, "catalog scan retries exhausted")}
	recoverer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         witnesses,
		Installer:         installer,
		CatalogScanner:    scanner,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer recoverer.Close()

	require.NoError(t, recoverer.LoadRootSealedSegments(context.Background(), scope))
	require.Positive(t, scanner.calls)
	require.Equal(t, 1, installer.calls)
	require.Equal(t, segment.Root, installer.segment.Root)
	require.NotEmpty(t, installer.install.RoutingKeys)
	stats := recoverer.Stats()
	require.Equal(t, uint64(1), stats["root_sealed_segment_missing_total"])
	require.Equal(t, uint64(1), stats["segment_recovery_install_total"])
	value, deleted, ok := recoverer.GetPerasOverlay(dentryKey)
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, []byte("dentry-value"), value)
}

func TestRuntimeRootSealedRecoveryWaitsOutRouteRetryExhaustion(t *testing.T) {
	mount := fsmeta.MountIdentity{MountID: "vol", MountKeyID: 1}
	dentryKey, err := fsmeta.EncodeDentryKey(mount, fsmeta.RootInode, "route-retry")
	require.NoError(t, err)
	inodeKey, err := fsmeta.EncodeInodeKey(mount, 14)
	require.NoError(t, err)
	witnesses := testRuntimeSegmentWitnesses(t, 3)
	provider := &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         witnesses,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	holder, err := fsperas.NewHolder(fsperas.HolderConfig{EpochID: 1, HolderID: "holder-a"})
	require.NoError(t, err)
	delta := testRuntimePerasOp(dentryKey, inodeKey)
	delta = testRuntimePerasOpWithScope(delta, compile.AuthorityScope{
		Mount:      delta.Delta.Authority.Mount,
		MountKeyID: delta.Delta.Authority.MountKeyID,
		Parents:    []fsmeta.InodeID{fsmeta.RootInode},
		Inodes:     []fsmeta.InodeID{14},
	})
	_, _, err = holder.Submit(context.Background(), fsperas.OperationID{ClientID: "client", Seq: 1}, delta)
	require.NoError(t, err)
	plan, scope, err := holder.BuildPendingReplayPlan(10)
	require.NoError(t, err)
	segment, err := fsperas.BuildPerasSegmentFromReplayPlan(plan)
	require.NoError(t, err)
	payload, err := fsperas.EncodePerasSegment(segment)
	require.NoError(t, err)
	digest, err := fsperas.PerasSegmentPayloadDigest(payload)
	require.NoError(t, err)
	requireRuntimeSegmentWitness(t, committer, scope, holder, segment, payload, digest)

	seal := testRuntimeSegmentRootSeal("grant-1", "holder-a", scope, time.Now())
	seal.SegmentRoot = segment.Root
	seal.SegmentPayloadDigest = digest
	seal.OperationCount = segment.Stats().OperationCount
	seal.EntryCount = segment.Stats().EntryCount
	provider.seals = []rootproto.VisibleAuthoritySeal{seal}
	installer := &flakyRuntimePerasSegmentInstaller{
		failures: defaultPerasSegmentInstallRetries + 1,
		kind:     nokverrors.KindRegionRouting,
	}
	recoverer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         witnesses,
		Installer:         installer,
		CatalogScanner:    &fakeRuntimePerasCatalogScanner{},
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer recoverer.Close()

	require.NoError(t, recoverer.LoadRootSealedSegments(context.Background(), scope))
	require.Equal(t, defaultPerasSegmentInstallRetries+2, installer.calls)
	stats := recoverer.Stats()
	require.Positive(t, stats["retry_routing_total"])
	require.Equal(t, uint64(1), stats["segment_recovery_install_total"])
	value, deleted, ok := recoverer.GetPerasOverlay(dentryKey)
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, []byte("dentry-value"), value)
}

func TestRuntimeReturnsUnrecoverableRootSealedCatalogScanError(t *testing.T) {
	segment := testRuntimeSegmentRootSegment(t)
	scope := compile.AuthorityScope{Mount: "vol", MountKeyID: 7}
	seal := testRuntimeSegmentRootSeal("grant-1", "holder-a", scope, time.Now())
	seal.SegmentRoot = segment.Root
	seal.SegmentPayloadDigest = [32]byte{1}
	provider := &fakeRuntimeVisibleGrantProvider{
		holderID: "holder-a",
		grant:    testRuntimeCommitterGrant(),
		seals:    []rootproto.VisibleAuthoritySeal{seal},
	}
	scanErr := errors.New("catalog decode failed")
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimeSegmentWitnesses(t, 3),
		Installer:         &fakeRuntimePerasSegmentInstaller{},
		CatalogScanner:    &fakeRuntimePerasCatalogScanner{err: scanErr},
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	err = committer.LoadRootSealedSegments(context.Background(), scope)
	require.ErrorIs(t, err, scanErr)
}

func TestRuntimeRecoversBroadRootSealForNarrowRecoveryScope(t *testing.T) {
	mount := fsmeta.MountIdentity{MountID: "vol", MountKeyID: 1}
	dentryA, err := fsmeta.EncodeDentryKey(mount, 1, "a")
	require.NoError(t, err)
	inodeA, err := fsmeta.EncodeInodeKey(mount, 11)
	require.NoError(t, err)
	dentryB, err := fsmeta.EncodeDentryKey(mount, 2, "b")
	require.NoError(t, err)
	inodeB, err := fsmeta.EncodeInodeKey(mount, 12)
	require.NoError(t, err)
	witnesses := testRuntimeSegmentWitnesses(t, 3)
	provider := &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         witnesses,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	holder, err := fsperas.NewHolder(fsperas.HolderConfig{EpochID: 1, HolderID: "holder-a"})
	require.NoError(t, err)
	first := testRuntimePerasOpWithScope(testRuntimePerasOp(dentryA, inodeA), compile.AuthorityScope{
		Mount:      mount.MountID,
		MountKeyID: mount.MountKeyID,
		Parents:    []fsmeta.InodeID{1},
		Inodes:     []fsmeta.InodeID{11},
	})
	second := testRuntimePerasOpWithScope(testRuntimePerasOp(dentryB, inodeB), compile.AuthorityScope{
		Mount:      mount.MountID,
		MountKeyID: mount.MountKeyID,
		Parents:    []fsmeta.InodeID{2},
		Inodes:     []fsmeta.InodeID{12},
	})
	_, _, err = holder.Submit(context.Background(), fsperas.OperationID{ClientID: "client", Seq: 1}, first)
	require.NoError(t, err)
	_, _, err = holder.Submit(context.Background(), fsperas.OperationID{ClientID: "client", Seq: 2}, second)
	require.NoError(t, err)
	plan, _, err := holder.BuildPendingReplayPlan(10)
	require.NoError(t, err)
	segment, err := fsperas.BuildPerasSegmentFromReplayPlan(plan)
	require.NoError(t, err)
	payload, err := fsperas.EncodePerasSegment(segment)
	require.NoError(t, err)
	digest, err := fsperas.PerasSegmentPayloadDigest(payload)
	require.NoError(t, err)
	broadScope := compile.AuthorityScope{Mount: mount.MountID, MountKeyID: mount.MountKeyID}
	requireRuntimeSegmentWitness(t, committer, broadScope, holder, segment, payload, digest)

	seal := testRuntimeSegmentRootSeal("grant-1", "holder-a", broadScope, time.Now())
	seal.SegmentRoot = segment.Root
	seal.SegmentPayloadDigest = digest
	seal.OperationCount = segment.Stats().OperationCount
	seal.EntryCount = segment.Stats().EntryCount
	provider.seals = []rootproto.VisibleAuthoritySeal{seal}
	installer := &fakeRuntimePerasSegmentInstaller{}
	recoverer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         witnesses,
		Installer:         installer,
		CatalogScanner:    &fakeRuntimePerasCatalogScanner{},
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer recoverer.Close()

	narrowScope := compile.AuthorityScope{Mount: mount.MountID, MountKeyID: mount.MountKeyID, Parents: []fsmeta.InodeID{1}}
	require.NoError(t, recoverer.LoadRootSealedSegments(context.Background(), narrowScope))
	require.Equal(t, 1, installer.calls)
	valueA, deleted, ok := recoverer.GetPerasOverlay(dentryA)
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, []byte("dentry-value"), valueA)
	valueB, deleted, ok := recoverer.GetPerasOverlay(dentryB)
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, []byte("dentry-value"), valueB)
}

func TestRuntimeLoadsInstalledSegmentCatalog(t *testing.T) {
	mount := fsmeta.MountIdentity{MountID: "vol", MountKeyID: 1}
	dentryKey, err := fsmeta.EncodeDentryKey(mount, fsmeta.RootInode, "restored")
	require.NoError(t, err)
	inodeKey, err := fsmeta.EncodeInodeKey(mount, 10)
	require.NoError(t, err)
	segment, err := fsperas.BuildPerasSegmentFromReplayPlan(fsperas.ReplayPlan{
		EpochID: 1,
		Operations: []fsperas.ReplayOperation{{
			OpID: fsperas.OperationID{ClientID: "client", Seq: 1},
			Kind: fsmeta.OperationCreate,
			Mutations: []fsperas.ReplayMutation{
				{Key: dentryKey, Value: []byte("dentry-value")},
				{Key: inodeKey, Value: []byte("inode-value")},
			},
		}},
	})
	require.NoError(t, err)

	provider := &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	scanner := &fakeRuntimePerasCatalogScanner{rows: testRuntimePerasCatalogRows(t, segment, 99)}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimeSegmentWitnesses(t, 3),
		CatalogScanner:    scanner,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	scope := compile.AuthorityScope{
		Mount:      mount.MountID,
		MountKeyID: mount.MountKeyID,
	}
	require.NoError(t, committer.LoadInstalledSegments(context.Background(), scope))
	value, deleted, ok := committer.GetPerasOverlay(dentryKey)
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, []byte("dentry-value"), value)
	require.Equal(t, uint64(1), committer.Stats()["segment_total"])
	require.Equal(t, 2, committer.Stats()["segment_keys"])
	require.Equal(t, uint64(1), committer.Stats()["segment_catalog_load_total"])
	completion, ok := committer.Completion(fsperas.OperationID{ClientID: "client", Seq: 1})
	require.True(t, ok)
	require.Equal(t, fsmeta.OperationCreate, completion.Kind)

	require.NoError(t, committer.LoadInstalledSegments(context.Background(), scope))
	require.Equal(t, uint64(1), committer.Stats()["segment_total"])
	require.Equal(t, uint64(1), committer.Stats()["segment_catalog_load_total"])
}

func TestRuntimeLoadsRootSealedSegments(t *testing.T) {
	segment := testRuntimeSegmentRootSegment(t)
	scope := compile.AuthorityScope{Mount: "vol", MountKeyID: 7}
	seal := testRuntimeSegmentRootSeal("grant-1", "holder-a", scope, time.Now())
	seal.SegmentRoot = segment.Root
	provider := &fakeRuntimeVisibleGrantProvider{
		holderID: "holder-a",
		grant:    testRuntimeCommitterGrant(),
		seals:    []rootproto.VisibleAuthoritySeal{seal},
	}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimeSegmentWitnesses(t, 3),
		CatalogScanner:    &fakeRuntimePerasCatalogScanner{rows: testRuntimePerasCatalogRows(t, segment, 99)},
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	require.NoError(t, committer.LoadRootSealedSegments(context.Background(), scope))
	stats := committer.Stats()
	require.Equal(t, uint64(1), stats["segment_catalog_load_total"])
	require.Equal(t, uint64(1), stats["root_sealed_segment_total"])
	require.Equal(t, uint64(0), stats["root_sealed_segment_missing_total"])
	require.Equal(t, uint64(1), stats["segment_total"])
}

func TestRuntimeLoadRootSealedSegmentsSkipsCatalogWithoutRootSeal(t *testing.T) {
	scope := compile.AuthorityScope{Mount: "vol", MountKeyID: 7}
	provider := &fakeRuntimeVisibleGrantProvider{
		holderID: "holder-a",
		grant:    testRuntimeCommitterGrant(),
	}
	scanner := &fakeRuntimePerasCatalogScanner{rows: testRuntimePerasCatalogRows(t, testRuntimeSegmentRootSegment(t), 99)}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimeSegmentWitnesses(t, 3),
		CatalogScanner:    scanner,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	require.NoError(t, committer.LoadRootSealedSegments(context.Background(), scope))
	require.Zero(t, scanner.calls)
	stats := committer.Stats()
	require.Equal(t, uint64(0), stats["segment_catalog_load_total"])
	require.Equal(t, uint64(0), stats["root_sealed_segment_total"])
}

func TestRuntimeRejectsMissingRootSealedSegmentCatalog(t *testing.T) {
	segment := testRuntimeSegmentRootSegment(t)
	scope := compile.AuthorityScope{Mount: "vol", MountKeyID: 7}
	seal := testRuntimeSegmentRootSeal("grant-1", "holder-a", scope, time.Now())
	seal.SegmentRoot = segment.Root
	provider := &fakeRuntimeVisibleGrantProvider{
		holderID: "holder-a",
		grant:    testRuntimeCommitterGrant(),
		seals:    []rootproto.VisibleAuthoritySeal{seal},
	}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimeSegmentWitnesses(t, 3),
		CatalogScanner:    &fakeRuntimePerasCatalogScanner{},
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	err = committer.LoadRootSealedSegments(context.Background(), scope)
	require.ErrorIs(t, err, fsperas.ErrInvalidPerasSegment)
	stats := committer.Stats()
	require.Equal(t, uint64(1), stats["root_sealed_segment_total"])
	require.Equal(t, uint64(1), stats["root_sealed_segment_missing_total"])
}

func TestRuntimeRecoversPredecessorBeforeOpeningNewEpoch(t *testing.T) {
	witnesses := testRuntimeSegmentWitnesses(t, 3)
	predecessorProvider := &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	predecessor, err := NewRuntime(Config{
		Authority:         predecessorProvider,
		Witnesses:         witnesses,
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer predecessor.Close()

	holder, err := fsperas.NewHolder(fsperas.HolderConfig{EpochID: 1, HolderID: "holder-a"})
	require.NoError(t, err)
	recoveredDelta := testRuntimePerasOp([]byte("dentry/recovered"), []byte("inode/recovered"))
	recoveredDelta = testRuntimePerasOpWithScope(recoveredDelta, compile.AuthorityScope{
		Mount:      "vol",
		MountKeyID: 1,
		Parents:    []fsmeta.InodeID{1},
		Inodes:     []fsmeta.InodeID{2},
	})
	recoveredKey := testRuntimeDentryEffect(recoveredDelta).Key
	_, _, err = holder.Submit(context.Background(), fsperas.OperationID{ClientID: "client", Seq: 1}, recoveredDelta)
	require.NoError(t, err)
	plan, scope, err := holder.BuildPendingReplayPlan(10)
	require.NoError(t, err)
	segment, err := fsperas.BuildPerasSegmentFromReplayPlan(plan)
	require.NoError(t, err)
	payload, err := fsperas.EncodePerasSegment(segment)
	require.NoError(t, err)
	digest, err := fsperas.PerasSegmentPayloadDigest(payload)
	require.NoError(t, err)
	requireRuntimeSegmentWitness(t, predecessor, scope, holder, segment, payload, digest)

	nextGrant := testRuntimeCommitterGrant()
	nextGrant.GrantID = "grant-2"
	nextGrant.EpochID = 2
	nextGrant.PredecessorDigest = segment.Root
	installer := &fakeRuntimePerasSegmentInstaller{}
	recoverer, err := NewRuntime(Config{
		Authority:         &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: nextGrant},
		Witnesses:         witnesses,
		Installer:         installer,
		VisibleLog:        &recordingVisibleLog{},
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer recoverer.Close()

	require.NoError(t, commitRuntimePeras(context.Background(), recoverer, 2, []byte("dentry/new"), []byte("inode/new")))
	require.Equal(t, 1, installer.calls)
	require.Equal(t, segment.Root, installer.segment.Root)

	value, deleted, ok := recoverer.GetPerasOverlay(recoveredKey)
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, []byte("dentry-value"), value)
	value, deleted, ok = recoverer.GetPerasOverlay(testRuntimeDentryKeyForLabel("new"))
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, []byte("dentry-value"), value)
}

func TestPerasSegmentWithinScopeRejectsDifferentBucket(t *testing.T) {
	mount := fsmeta.MountIdentity{MountID: "vol", MountKeyID: 1}
	leftA, leftB := testRuntimeBucketKeys(t, mount, 1)
	rightA, _ := testRuntimeBucketKeys(t, mount, 2)
	segment, err := fsperas.BuildPerasSegmentFromReplayPlan(fsperas.ReplayPlan{
		EpochID: 1,
		Operations: []fsperas.ReplayOperation{{
			OpID: fsperas.OperationID{ClientID: "client", Seq: 1},
			Kind: fsmeta.OperationUpdateInode,
			Mutations: []fsperas.ReplayMutation{
				{Key: leftA, Value: []byte("a")},
				{Key: leftB, Value: []byte("b")},
			},
		}},
	})
	require.NoError(t, err)

	require.True(t, SegmentWithinScope(segment, compile.AuthorityScope{
		Mount:      mount.MountID,
		MountKeyID: mount.MountKeyID,
		Buckets:    []fsmeta.AffinityBucket{1},
	}))
	require.False(t, SegmentWithinScope(segment, compile.AuthorityScope{
		Mount:      mount.MountID,
		MountKeyID: mount.MountKeyID,
		Buckets:    []fsmeta.AffinityBucket{2},
	}))
	require.NotEqual(t, leftA, rightA)
}

func TestRuntimeFlushAuthorityFlushesOnlyOverlappingPendingOps(t *testing.T) {
	provider := &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	installer := &fakeRuntimePerasSegmentInstaller{}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimeSegmentWitnesses(t, 3),
		Installer:         installer,
		VisibleLog:        &recordingVisibleLog{},
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	requestedScopeA := compile.AuthorityScope{
		Mount:           "vol",
		MountKeyID:      1,
		Parents:         []fsmeta.InodeID{1},
		Inodes:          []fsmeta.InodeID{2},
		AllowOpaqueKeys: true,
	}
	requestedScopeB := compile.AuthorityScope{
		Mount:           "vol",
		MountKeyID:      1,
		Parents:         []fsmeta.InodeID{2},
		Inodes:          []fsmeta.InodeID{3},
		AllowOpaqueKeys: true,
	}
	deltaA := testRuntimePerasOp([]byte("dentry/a"), []byte("inode/a"))
	deltaA = testRuntimePerasOpWithScope(deltaA, requestedScopeA)
	scopeA := deltaA.Delta.Authority
	deltaB := testRuntimePerasOp([]byte("dentry/b"), []byte("inode/b"))
	deltaB = testRuntimePerasOpWithScope(deltaB, requestedScopeB)

	ctx := context.Background()
	_, err = committer.SubmitVisible(ctx, fsperas.OperationID{ClientID: "client", Seq: 1}, deltaA, nil)
	require.NoError(t, err)
	_, err = committer.SubmitVisible(ctx, fsperas.OperationID{ClientID: "client", Seq: 2}, deltaB, nil)
	require.NoError(t, err)

	require.NoError(t, committer.FlushAuthority(ctx, scopeA))
	stats := committer.Stats()
	require.Equal(t, uint64(1), stats["flush_total"])
	require.Equal(t, uint64(1), stats["segment_total"])
	require.Equal(t, uint64(1), stats["segment_operations_total"])
	require.Equal(t, 1, stats["pending"])
	require.Equal(t, 3, stats["overlay_keys"])
	require.Equal(t, 3, stats["segment_keys"])
	require.Equal(t, 1, installer.calls)
	require.Equal(t, scopeA, installer.scope)
	require.Equal(t, uint64(1), installer.segment.Stats().OperationCount)

	value, deleted, ok := committer.GetPerasOverlay(testRuntimeDentryEffect(deltaA).Key)
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, []byte("dentry-value"), value)
	value, deleted, ok = committer.GetPerasOverlay(testRuntimeDentryEffect(deltaB).Key)
	require.True(t, ok)
	require.False(t, deleted)
	require.Equal(t, []byte("dentry-value"), value)

	require.NoError(t, committer.FlushDurable(ctx))
	stats = committer.Stats()
	require.Equal(t, uint64(2), stats["flush_total"])
	require.Equal(t, uint64(2), stats["segment_total"])
	require.Equal(t, uint64(2), stats["segment_operations_total"])
	require.Equal(t, 0, stats["pending"])
	require.Equal(t, 0, stats["overlay_keys"])
	require.Equal(t, 2, installer.calls)
}

func TestRuntimeDrainAuthorityFlushesAndRetires(t *testing.T) {
	provider := &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	installer := &fakeRuntimePerasSegmentInstaller{}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimeSegmentWitnesses(t, 3),
		Installer:         installer,
		VisibleLog:        &recordingVisibleLog{},
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	requestedScope := compile.AuthorityScope{
		Mount:      "vol",
		MountKeyID: 1,
		Parents:    []fsmeta.InodeID{1},
		Inodes:     []fsmeta.InodeID{testRuntimeInodeForBucketValue(fsmeta.BucketForInodeID(1))},
	}
	delta := testRuntimePerasOp([]byte("dentry/a"), []byte("inode/a"))
	delta = testRuntimePerasOpWithScope(delta, requestedScope)
	scope := delta.Delta.Authority
	requestedOtherScope := compile.AuthorityScope{
		Mount:      "vol",
		MountKeyID: 1,
		Parents:    []fsmeta.InodeID{9},
		Inodes:     []fsmeta.InodeID{10},
	}
	otherDelta := testRuntimePerasOp([]byte("dentry/b"), []byte("inode/b"))
	otherDelta = testRuntimePerasOpWithScope(otherDelta, requestedOtherScope)

	ctx := context.Background()
	_, err = committer.SubmitVisible(ctx, fsperas.OperationID{ClientID: "client", Seq: 1}, delta, nil)
	require.NoError(t, err)
	_, err = committer.SubmitVisible(ctx, fsperas.OperationID{ClientID: "client", Seq: 2}, otherDelta, nil)
	require.NoError(t, err)
	retirer := &fakeRuntimePerasRetirer{}
	require.NoError(t, committer.DrainAuthority(ctx, retirer, scope))

	stats := committer.Stats()
	require.Equal(t, uint64(1), stats["flush_total"])
	require.Equal(t, uint64(1), stats["segment_total"])
	require.Equal(t, uint64(1), stats["segment_operations_total"])
	require.Equal(t, 1, stats["pending"])
	require.Equal(t, 1, installer.calls)
	require.True(t, installer.materialize)
	require.Equal(t, 1, retirer.calls)
	require.Equal(t, []compile.AuthorityScope{scope}, retirer.scopes)
}

func TestRuntimeDrainAuthorityUsesMaterializeBudget(t *testing.T) {
	provider := &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	installer := &fakeRuntimePerasSegmentInstaller{}
	committer, err := NewRuntime(Config{
		Authority:                 provider,
		Witnesses:                 testRuntimeSegmentWitnesses(t, 3),
		Installer:                 installer,
		VisibleLog:                &recordingVisibleLog{},
		SegmentBatchSize:          1024,
		SegmentMaxReplayMutations: defaultPerasSegmentMaxReplayMutations,
		SegmentFlushEvery:         time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	ctx := context.Background()
	for i := range 11 {
		seq := uint64(i + 1)
		require.NoError(t, commitRuntimePeras(ctx, committer, seq, appendUvarintKey("dentry/", seq), appendUvarintKey("inode/", seq)))
	}
	require.NoError(t, committer.DrainAuthority(ctx, &fakeRuntimePerasRetirer{}))

	require.Equal(t, 2, installer.calls)
	require.Equal(t, []bool{true, true}, installer.modes)
	require.Equal(t, uint64(2), committer.Stats()["segment_total"])
	require.Equal(t, uint64(11), committer.Stats()["segment_operations_total"])
}

func TestRuntimeBackgroundFlushTimesOutAndBacksOff(t *testing.T) {
	provider := &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	installer := &blockingRuntimePerasSegmentInstaller{}
	committer, err := NewRuntime(Config{
		Authority:              provider,
		Witnesses:              testRuntimeSegmentWitnesses(t, 3),
		Installer:              installer,
		VisibleLog:             &recordingVisibleLog{},
		SegmentBatchSize:       1,
		SegmentFlushEvery:      time.Hour,
		BackgroundFlushTimeout: 10 * time.Millisecond,
		BackgroundErrorBackoff: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	ctx := context.Background()
	require.NoError(t, commitRuntimePeras(ctx, committer, 1, []byte("dentry/a"), []byte("inode/a")))
	require.Eventually(t, func() bool {
		stats := committer.Stats()
		return stats["background_error_total"] == uint64(1) && installer.calls.Load() == 1
	}, time.Second, 10*time.Millisecond)

	require.NoError(t, commitRuntimePeras(ctx, committer, 2, []byte("dentry/b"), []byte("inode/b")))
	require.Eventually(t, func() bool {
		return committer.Stats()["background_skip_total"].(uint64) > 0
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, int32(1), installer.calls.Load())
	require.Equal(t, 2, committer.Stats()["pending"])
}

func TestRuntimeBackgroundFlushDrainsInstallParallelWindow(t *testing.T) {
	provider := &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	installer := &fakeRuntimePerasSegmentInstaller{}
	committer, err := NewRuntime(Config{
		Authority:                  provider,
		Witnesses:                  testRuntimeSegmentWitnesses(t, 3),
		Installer:                  installer,
		VisibleLog:                 &recordingVisibleLog{},
		SegmentBatchSize:           1 << 30,
		SegmentMaxReplayOperations: 3,
		SegmentInstallParallelism:  4,
		SegmentFlushEvery:          time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	ctx := context.Background()
	for i := range 12 {
		seq := uint64(i + 1)
		require.NoError(t, commitRuntimePeras(ctx, committer, seq, appendUvarintKey("dentry/", seq), appendUvarintKey("inode/", seq)))
	}
	require.Equal(t, 12, committer.Stats()["pending"])

	committer.batchSize = 2
	committer.flushBackground()

	stats := committer.Stats()
	require.Equal(t, 0, stats["pending"])
	require.Equal(t, uint64(1), stats["flush_batch_total"])
	require.Equal(t, uint64(4), stats["flush_jobs_total"])
	require.Equal(t, uint64(4), stats["flush_jobs_last"])
	require.Equal(t, uint64(4), stats["flush_jobs_max"])
	require.Greater(t, stats["flush_jobs_max"], uint64(1))
	require.Equal(t, 4, installer.calls)
}

func TestRuntimeBackgroundFlushCommitsBoundedJobBatches(t *testing.T) {
	provider := &nonSealingRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	installer := &fakeRuntimePerasSegmentInstaller{materializes: true}
	committer, err := NewRuntime(Config{
		Authority:                  provider,
		Installer:                  installer,
		VisibleLog:                 &replayingVisibleLog{},
		SegmentBatchSize:           1 << 30,
		SegmentMaxReplayOperations: 1,
		SegmentInstallParallelism:  4,
		SegmentFlushEvery:          time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	ctx := context.Background()
	for i := range 40 {
		seq := uint64(i + 1)
		require.NoError(t, commitRuntimePeras(ctx, committer, seq, appendUvarintKey("dentry/", seq), appendUvarintKey("inode/", seq)))
	}
	require.Equal(t, 40, committer.Stats()["pending"])

	committer.flushBackground()

	stats := committer.Stats()
	require.Equal(t, 0, stats["pending"])
	require.Equal(t, uint64(10), stats["flush_batch_total"])
	require.Equal(t, uint64(40), stats["flush_jobs_total"])
	require.Equal(t, uint64(4), stats["flush_jobs_max"])
	require.Equal(t, 40, installer.calls)
}

func TestRuntimeCloseCancelsInstallLane(t *testing.T) {
	provider := &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	installer := &blockingRuntimePerasSegmentInstaller{}
	committer, err := NewRuntime(Config{
		Authority:                  provider,
		Witnesses:                  testRuntimeSegmentWitnesses(t, 3),
		Installer:                  installer,
		VisibleLog:                 &recordingVisibleLog{},
		SegmentBatchSize:           1024,
		SegmentInstallParallelism:  1,
		SegmentFlushEvery:          time.Hour,
		BackgroundFlushTimeout:     time.Hour,
		BackgroundErrorBackoff:     time.Hour,
		SegmentMaxReplayMutations:  defaultPerasSegmentMaxReplayMutations,
		SegmentWitnessRetryBackoff: time.Millisecond,
	})
	require.NoError(t, err)

	require.NoError(t, commitRuntimePeras(context.Background(), committer, 1, []byte("dentry/a"), []byte("inode/a")))
	flushDone := make(chan error, 1)
	go func() {
		flushDone <- committer.FlushDurable(context.Background())
	}()
	require.Eventually(t, func() bool {
		return installer.calls.Load() == 1
	}, time.Second, 10*time.Millisecond)
	stats := committer.Stats()
	require.Equal(t, 1, stats["segment_install_parallelism"])
	require.Equal(t, 4, stats["segment_install_queue_capacity"])

	closeDone := make(chan struct{})
	go func() {
		committer.Close()
		close(closeDone)
	}()
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("committer close did not cancel blocked segment install")
	}
	require.Error(t, <-flushDone)
}

func TestRuntimeFlushChainsBoundedReplayWindows(t *testing.T) {
	provider := &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	installer := &fakeRuntimePerasSegmentInstaller{}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimeSegmentWitnesses(t, 3),
		Installer:         installer,
		VisibleLog:        &recordingVisibleLog{},
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	for seq := uint64(1); seq <= 5; seq++ {
		require.NoError(t, commitRuntimePeras(context.Background(), committer, seq, appendUvarintKey("dentry/", seq), appendUvarintKey("inode/", seq)))
	}

	for committer.Stats()["pending"].(int) > 0 {
		committer.commitMu.Lock()
		batches, err := committer.freezeFlushBatchesLocked(nil, false, 2)
		committer.commitMu.Unlock()
		require.NoError(t, err)
		require.NotEmpty(t, batches)
		require.NoError(t, (flushPipeline{runtime: committer, level: fsperas.SegmentPersistencePublished}).run(context.Background(), batches))
	}
	require.Equal(t, uint64(3), committer.Stats()["segment_total"])
	require.Equal(t, uint64(5), committer.Stats()["segment_operations_total"])
}

func TestRuntimeFlushAllowsConcurrentCommitsDuringInstall(t *testing.T) {
	provider := &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	installer := &blockingRuntimePerasSegmentInstaller{}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimeSegmentWitnesses(t, 3),
		Installer:         installer,
		VisibleLog:        &recordingVisibleLog{},
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, commitRuntimePeras(context.Background(), committer, 1, []byte("dentry/a"), []byte("inode/a")))

	flushDone := make(chan error, 1)
	go func() {
		flushDone <- committer.FlushDurable(ctx)
	}()
	require.Eventually(t, func() bool {
		return installer.calls.Load() == 1
	}, time.Second, 10*time.Millisecond)

	commitDone := make(chan error, 1)
	go func() {
		commitDone <- commitRuntimePeras(context.Background(), committer, 2, []byte("dentry/b"), []byte("inode/b"))
	}()
	select {
	case err := <-commitDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("concurrent commit did not complete while background segment install was in progress")
	}

	cancel()
	require.ErrorIs(t, <-flushDone, context.Canceled)
	require.Equal(t, 2, committer.Stats()["pending"])
}

func TestRuntimeDrainAuthorityBlocksConcurrentCommitsUntilInstallFinishes(t *testing.T) {
	provider := &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	installer := &blockingRuntimePerasSegmentInstaller{}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimeSegmentWitnesses(t, 3),
		Installer:         installer,
		VisibleLog:        &recordingVisibleLog{},
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, commitRuntimePeras(context.Background(), committer, 1, []byte("dentry/a"), []byte("inode/a")))

	drainDone := make(chan error, 1)
	go func() {
		drainDone <- committer.DrainAuthority(ctx, &fakeRuntimePerasRetirer{})
	}()
	require.Eventually(t, func() bool {
		return installer.calls.Load() == 1
	}, time.Second, 10*time.Millisecond)

	commitDone := make(chan error, 1)
	go func() {
		commitDone <- commitRuntimePeras(context.Background(), committer, 2, []byte("dentry/b"), []byte("inode/b"))
	}()
	select {
	case err := <-commitDone:
		t.Fatalf("concurrent commit completed while authority drain install was still in progress: %v", err)
	case <-time.After(30 * time.Millisecond):
	}

	cancel()
	require.ErrorIs(t, <-drainDone, context.Canceled)
	require.NoError(t, <-commitDone)
}

func TestRuntimeDrainAuthorityAllowsDisjointCommitsDuringInstall(t *testing.T) {
	provider := &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	installer := &blockingRuntimePerasSegmentInstaller{}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimeSegmentWitnesses(t, 3),
		Installer:         installer,
		VisibleLog:        &recordingVisibleLog{},
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	mount := fsmeta.MountIdentity{MountID: "vol", MountKeyID: 1}
	leftA, leftB := testRuntimeBucketKeys(t, mount, 1)
	rightA, rightB := testRuntimeBucketKeys(t, mount, 2)
	left := testRuntimePerasOpForBucket(leftA, leftB, 1)
	right := testRuntimePerasOpForBucket(rightA, rightB, 2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err = committer.SubmitVisible(context.Background(), fsperas.OperationID{ClientID: "client", Seq: 1}, left, nil)
	require.NoError(t, err)

	drainDone := make(chan error, 1)
	go func() {
		drainDone <- committer.DrainAuthority(ctx, &fakeRuntimePerasRetirer{}, left.Delta.Authority)
	}()
	require.Eventually(t, func() bool {
		return installer.calls.Load() == 1
	}, time.Second, 10*time.Millisecond)

	disjointDone := make(chan error, 1)
	go func() {
		_, err := committer.SubmitVisible(context.Background(), fsperas.OperationID{ClientID: "client", Seq: 2}, right, nil)
		disjointDone <- err
	}()
	select {
	case err := <-disjointDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("disjoint commit blocked behind scoped authority drain")
	}

	cancel()
	require.ErrorIs(t, <-drainDone, context.Canceled)
}

func TestRuntimeRollsBackHolderOnOverlayAdmissionFailure(t *testing.T) {
	provider := &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimeSegmentWitnesses(t, 3),
		SegmentBatchSize:  1024,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(t, err)
	defer committer.Close()

	delta := testRuntimePerasOp([]byte("dentry/a"), []byte("inode/a"))
	committer.read = nil
	_, err = committer.SubmitVisible(context.Background(), fsperas.OperationID{ClientID: "client", Seq: 1}, delta, nil)
	require.Error(t, err)
	require.Equal(t, 0, committer.Stats()["pending"])
}

func BenchmarkRuntimeCreate(b *testing.B) {
	provider := &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimeSegmentWitnesses(b, 3),
		VisibleLog:        &recordingVisibleLog{},
		SegmentBatchSize:  1 << 30,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(b, err)
	defer committer.Close()

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dentryKey := appendUvarintKey("dentry/", uint64(i))
		inodeKey := appendUvarintKey("inode/", uint64(i))
		_, err := committer.SubmitVisible(ctx, fsperas.OperationID{ClientID: "bench", Seq: uint64(i + 1)}, testRuntimePerasOp(dentryKey, inodeKey), nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRuntimeCreateParallel(b *testing.B) {
	provider := &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
	committer, err := NewRuntime(Config{
		Authority:         provider,
		Witnesses:         testRuntimeSegmentWitnesses(b, 3),
		VisibleLog:        &recordingVisibleLog{},
		SegmentBatchSize:  1 << 30,
		SegmentFlushEvery: time.Hour,
	})
	require.NoError(b, err)
	defer committer.Close()

	ctx := context.Background()
	var seq atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			current := seq.Add(1)
			dentryKey := appendUvarintKey("dentry/", current)
			inodeKey := appendUvarintKey("inode/", current)
			_, err := committer.SubmitVisible(ctx, fsperas.OperationID{ClientID: "bench", Seq: current}, testRuntimePerasOp(dentryKey, inodeKey), nil)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkRuntimeScanPerasOverlay(b *testing.B) {
	committer := &Runtime{
		read: newReadState(),
	}
	for i := range 100_000 {
		key, err := fsmeta.EncodeDentryKey(testRuntimeMount, fsmeta.RootInode, fmt.Sprintf("%08d", i))
		require.NoError(b, err)
		require.NoError(b, committer.read.sealed.AddSegment(testRuntimePerasSegmentForOverlay(key, []byte("sealed"))))
	}
	for i := range 1024 {
		name := fmt.Sprintf("%08d", i*16)
		op := testRuntimeCreateOp(testRuntimeMount, fsmeta.RootInode, name, fsmeta.InodeID(200_000+i), []byte("overlay"), []byte("inode-value"))
		require.NoError(b, committer.read.overlay.Add(fsperas.OperationID{ClientID: "bench", Seq: uint64(i + 1)}, op))
	}
	start, err := fsmeta.EncodeDentryKey(testRuntimeMount, fsmeta.RootInode, "00000000")
	require.NoError(b, err)
	require.Len(b, committer.ScanPerasOverlay(start, 128), 128)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		out := committer.ScanPerasOverlay(start, 128)
		if len(out) != 128 {
			b.Fatalf("scan returned %d rows", len(out))
		}
	}
}

func BenchmarkRuntimeFlushInstallParallelism(b *testing.B) {
	mount := fsmeta.MountIdentity{MountID: "vol", MountKeyID: 1}
	keys := make([][2][]byte, 0, 16)
	for bucket := fsmeta.AffinityBucket(0); len(keys) < cap(keys); bucket++ {
		first, second := testRuntimeBucketKeys(b, mount, bucket)
		keys = append(keys, [2][]byte{first, second})
	}
	for _, parallelism := range []int{1, 4} {
		b.Run(fmt.Sprintf("parallel_%d", parallelism), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				provider := &fakeRuntimeVisibleGrantProvider{holderID: "holder-a", grant: testRuntimeCommitterGrant()}
				installer := &delayingRuntimePerasSegmentInstaller{delay: time.Millisecond}
				committer, err := NewRuntime(Config{
					Authority:                 provider,
					Witnesses:                 testRuntimeSegmentWitnesses(b, 3),
					Installer:                 installer,
					VisibleLog:                &recordingVisibleLog{},
					SegmentBatchSize:          1 << 30,
					SegmentMaxReplayMutations: 2,
					SegmentInstallParallelism: parallelism,
					SegmentFlushEvery:         time.Hour,
				})
				require.NoError(b, err)
				for idx, pair := range keys {
					require.NoError(b, commitRuntimePeras(context.Background(), committer, uint64(idx+1), pair[0], pair[1]))
				}
				require.NoError(b, committer.FlushDurable(context.Background()))
				committer.Close()
				if got := installer.calls.Load(); got != int32(len(keys)) {
					b.Fatalf("installed %d segments, want %d", got, len(keys))
				}
			}
		})
	}
}

func commitRuntimePeras(ctx context.Context, committer *Runtime, seq uint64, dentryKey, inodeKey []byte) error {
	_, err := committer.SubmitVisible(ctx, fsperas.OperationID{ClientID: "client", Seq: seq}, testRuntimePerasOp(dentryKey, inodeKey), nil)
	return err
}

func testRuntimePerasSegmentForOverlay(key, value []byte) fsperas.PerasSegment {
	segment, err := fsperas.BuildPerasSegmentFromReplayPlan(fsperas.ReplayPlan{
		EpochID: 1,
		Operations: []fsperas.ReplayOperation{{
			OpID: fsperas.OperationID{ClientID: "overlay", Seq: 1},
			Kind: fsmeta.OperationUpdateInode,
			Mutations: []fsperas.ReplayMutation{
				{Key: key, Value: value},
			},
		}},
	})
	if err != nil {
		panic(err)
	}
	return segment
}

func installRuntimeSealedSegment(tb testing.TB, committer *Runtime, segment fsperas.PerasSegment) {
	tb.Helper()
	committer.read.mu.Lock()
	err := committer.read.sealed.AddSegment(segment)
	if err == nil {
		committer.read.sealedSegments = append(committer.read.sealedSegments, segment)
	}
	committer.read.mu.Unlock()
	require.NoError(tb, err)
}

func testRuntimePerasCatalogRows(tb testing.TB, segment fsperas.PerasSegment, installVersion uint64) []KV {
	tb.Helper()
	payload, err := fsperas.EncodePerasSegment(segment)
	require.NoError(tb, err)
	digest, err := fsperas.PerasSegmentPayloadDigest(payload)
	require.NoError(tb, err)
	rows, err := runtimePerasCatalogRows(segment, installVersion, payload, digest)
	require.NoError(tb, err)
	return rows
}

func runtimePerasCatalogRows(segment fsperas.PerasSegment, installVersion uint64, payload []byte, digest [32]byte) ([]KV, error) {
	catalogKeys, err := fsperas.PerasSegmentCatalogIndexKeys(segment)
	if err != nil {
		return nil, err
	}
	objectKey, err := fsperas.PerasSegmentObjectKey(segment)
	if err != nil {
		return nil, err
	}
	objectValue, err := fsperas.EncodePerasSegmentCatalogRecordWithPayload(segment, installVersion, payload, digest)
	if err != nil {
		return nil, err
	}
	objectRecord, err := fsperas.DecodePerasSegmentCatalogRecord(objectValue)
	if err != nil {
		return nil, err
	}
	indexValue, err := fsperas.EncodePerasSegmentCatalogIndexRecord(objectRecord, objectKey)
	if err != nil {
		return nil, err
	}
	rows := make([]KV, 0, len(catalogKeys)+1)
	for _, key := range catalogKeys {
		rows = append(rows, KV{Key: key, Value: indexValue})
	}
	return append(rows, KV{Key: objectKey, Value: objectValue}), nil
}

type fakeRuntimePerasSegmentInstaller struct {
	mu           sync.Mutex
	calls        int
	scope        compile.AuthorityScope
	segment      fsperas.PerasSegment
	payload      []byte
	digest       [32]byte
	install      compile.InstallPlan
	materialize  bool
	materializes bool
	modes        []bool
}

func (i *fakeRuntimePerasSegmentInstaller) InstallSegment(_ context.Context, req SegmentInstallRequest) (InstallCursor, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.calls++
	i.scope = req.Scope
	i.segment = req.Segment
	i.payload = append([]byte(nil), req.Payload...)
	i.digest = req.PayloadDigest
	i.install = req.Install
	i.materialize = req.MaterializeMVCC
	i.modes = append(i.modes, req.MaterializeMVCC)
	return testPerasInstallCursor(uint64(i.calls)), nil
}

func (i *fakeRuntimePerasSegmentInstaller) MaterializesSegments() bool {
	return i != nil && i.materializes
}

type recordingVisibleLog struct {
	mu      sync.Mutex
	err     error
	records []fsperas.VisibleOperationRecord
}

func (l *recordingVisibleLog) AppendVisible(_ context.Context, record fsperas.VisibleOperationRecord) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err != nil {
		return l.err
	}
	l.records = append(l.records, record)
	return nil
}

type replayingVisibleLog struct {
	mu      sync.Mutex
	records []fsperas.VisibleOperationRecord
	applied []fsperas.VisibleAppliedRecord
}

func (l *replayingVisibleLog) AppendVisible(_ context.Context, record fsperas.VisibleOperationRecord) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.records = append(l.records, record)
	return nil
}

func (l *replayingVisibleLog) ReplayVisible(context.Context) ([]fsperas.VisibleOperationRecord, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]fsperas.VisibleOperationRecord, len(l.records))
	copy(out, l.records)
	return out, nil
}

func (l *replayingVisibleLog) ReplayVisibleState(context.Context) ([]VisibleLogStateRecord, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]VisibleLogStateRecord, 0, len(l.records))
	for idx, record := range l.records {
		out = append(out, VisibleLogStateRecord{
			Record:  record,
			Applied: replayingVisibleLogAppliedAt(l.applied, record, uint64(idx)),
		})
	}
	return out, nil
}

func (l *replayingVisibleLog) AppendVisibleApplied(_ context.Context, record fsperas.VisibleAppliedRecord) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.applied = append(l.applied, record)
	return nil
}

func (l *replayingVisibleLog) AppendVisibleReplayPlanApplied(_ context.Context, epochID uint64, holderID string, plan fsperas.ReplayPlan) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	startOffset := uint64(0)
	for _, marker := range l.applied {
		for _, span := range marker.Ranges {
			if span.EndOffset > startOffset {
				startOffset = span.EndOffset
			}
		}
	}
	l.applied = append(l.applied, fsperas.VisibleAppliedRecord{
		EpochID:  epochID,
		HolderID: holderID,
		Ranges: []fsperas.VisibleAppliedRange{{
			SegmentID:   1,
			StartOffset: startOffset,
			EndOffset:   startOffset + uint64(len(plan.Operations)),
		}},
	})
	return nil
}

func replayingVisibleLogAppliedAt(applied []fsperas.VisibleAppliedRecord, record fsperas.VisibleOperationRecord, offset uint64) bool {
	for _, marker := range applied {
		if marker.EpochID != record.EpochID || marker.HolderID != record.HolderID {
			continue
		}
		for _, span := range marker.Ranges {
			if span.StartOffset <= offset && offset < span.EndOffset {
				return true
			}
		}
	}
	return false
}

type fakeRuntimePerasCatalogScanner struct {
	rows  []KV
	calls int
	err   error
}

func (s *fakeRuntimePerasCatalogScanner) Scan(_ context.Context, startKey []byte, limit uint32, _ uint64) ([]KV, error) {
	s.calls++
	if s.err != nil {
		return nil, s.err
	}
	return scanRuntimePerasRows(s.rows, startKey, limit), nil
}

func scanRuntimePerasRows(rows []KV, startKey []byte, limit uint32) []KV {
	rows = append([]KV(nil), rows...)
	sort.Slice(rows, func(i, j int) bool {
		return bytes.Compare(rows[i].Key, rows[j].Key) < 0
	})
	out := make([]KV, 0, limit)
	for _, row := range rows {
		if bytes.Compare(row.Key, startKey) < 0 {
			continue
		}
		out = append(out, KV{
			Key:   append([]byte(nil), row.Key...),
			Value: append([]byte(nil), row.Value...),
		})
		if uint32(len(out)) >= limit {
			break
		}
	}
	return out
}

type flakyRuntimePerasSegmentInstaller struct {
	mu       sync.Mutex
	calls    int
	failures int
	kind     nokverrors.Kind
}

func (i *flakyRuntimePerasSegmentInstaller) InstallSegment(context.Context, SegmentInstallRequest) (InstallCursor, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.calls++
	if i.calls <= i.failures {
		kind := i.kind
		if kind == nokverrors.KindUnknown {
			kind = nokverrors.KindStaleEpoch
		}
		return InstallCursor{}, nokverrors.New(kind, "transient install error")
	}
	return testPerasInstallCursor(uint64(i.calls)), nil
}

type blockingRuntimePerasSegmentInstaller struct {
	calls atomic.Int32
}

func (i *blockingRuntimePerasSegmentInstaller) InstallSegment(ctx context.Context, _ SegmentInstallRequest) (InstallCursor, error) {
	i.calls.Add(1)
	<-ctx.Done()
	return InstallCursor{}, ctx.Err()
}

type delayingRuntimePerasSegmentInstaller struct {
	calls atomic.Int32
	delay time.Duration
}

func (i *delayingRuntimePerasSegmentInstaller) InstallSegment(ctx context.Context, _ SegmentInstallRequest) (InstallCursor, error) {
	i.calls.Add(1)
	timer := time.NewTimer(i.delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return InstallCursor{}, ctx.Err()
	case <-timer.C:
		return testPerasInstallCursor(uint64(i.calls.Load())), nil
	}
}

type gatedRuntimeSegmentWitness struct {
	id      string
	gate    int
	release chan struct{}
	once    sync.Once

	mu          sync.Mutex
	records     int
	batches     int
	inflight    int
	maxInflight int
}

func newGatedRuntimeSegmentWitness(id string, gate int) *gatedRuntimeSegmentWitness {
	if gate <= 0 {
		gate = 1
	}
	return &gatedRuntimeSegmentWitness{id: id, gate: gate, release: make(chan struct{})}
}

func (w *gatedRuntimeSegmentWitness) ID() string { return w.id }

func (w *gatedRuntimeSegmentWitness) AppendSegments(ctx context.Context, _ compile.AuthorityScope, records []fsperas.SegmentWitnessRecord) error {
	w.mu.Lock()
	w.records += len(records)
	w.batches++
	w.inflight++
	if w.inflight > w.maxInflight {
		w.maxInflight = w.inflight
	}
	if w.maxInflight >= w.gate {
		w.once.Do(func() { close(w.release) })
	}
	w.mu.Unlock()
	defer func() {
		w.mu.Lock()
		w.inflight--
		w.mu.Unlock()
	}()
	select {
	case <-w.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *gatedRuntimeSegmentWitness) Probe(context.Context, uint64) (fsperas.WitnessSnapshot, error) {
	return fsperas.WitnessSnapshot{}, nil
}

func (w *gatedRuntimeSegmentWitness) Count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.records
}

func (w *gatedRuntimeSegmentWitness) BatchCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.batches
}

func (w *gatedRuntimeSegmentWitness) MaxConcurrent() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.maxInflight
}

type sequenceDelayingRuntimePerasSegmentInstaller struct {
	calls atomic.Int32
}

func (i *sequenceDelayingRuntimePerasSegmentInstaller) InstallSegment(ctx context.Context, req SegmentInstallRequest) (InstallCursor, error) {
	if len(req.Segment.Completions) > 0 && req.Segment.Completions[0].OpID.Seq == 1 {
		timer := time.NewTimer(50 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return InstallCursor{}, ctx.Err()
		case <-timer.C:
		}
	}
	call := i.calls.Add(1)
	return testPerasInstallCursor(uint64(call)), nil
}

type fakeRuntimeVisibleGrantProvider struct {
	holderID string
	grant    rootproto.VisibleAuthorityGrant
	seals    []rootproto.VisibleAuthoritySeal
	owned    bool
	err      error
	sealErr  error

	acquireCalls atomic.Int64
}

type nonSealingRuntimeVisibleGrantProvider struct {
	holderID string
	grant    rootproto.VisibleAuthorityGrant
	owned    bool
	err      error
}

func (p *nonSealingRuntimeVisibleGrantProvider) HolderID() string {
	return p.holderID
}

func (p *nonSealingRuntimeVisibleGrantProvider) Acquire(context.Context, compile.AuthorityScope) (rootproto.VisibleAuthorityGrant, bool, error) {
	owned := p.owned
	if !owned {
		owned = true
	}
	return p.grant, owned, p.err
}

func (p *fakeRuntimeVisibleGrantProvider) HolderID() string {
	return p.holderID
}

func (p *fakeRuntimeVisibleGrantProvider) Acquire(context.Context, compile.AuthorityScope) (rootproto.VisibleAuthorityGrant, bool, error) {
	p.acquireCalls.Add(1)
	owned := p.owned
	if !owned {
		owned = true
	}
	return p.grant, owned, p.err
}

func (p *fakeRuntimeVisibleGrantProvider) ListVisibleAuthoritySeals(context.Context, compile.AuthorityScope) ([]rootproto.VisibleAuthoritySeal, error) {
	if p.sealErr != nil {
		return nil, p.sealErr
	}
	out := make([]rootproto.VisibleAuthoritySeal, len(p.seals))
	for i, seal := range p.seals {
		out[i] = rootproto.CloneVisibleAuthoritySeal(seal)
	}
	return out, nil
}

type publishingRuntimeVisibleGrantProvider struct {
	fakeRuntimeVisibleGrantProvider
	mu            sync.Mutex
	sealCalls     int
	sealedGrant   rootproto.VisibleAuthorityGrant
	sealedSegment fsperas.PerasSegment
	sealedDigest  [32]byte
	sealedCursor  InstallCursor
	sealErr       error
}

func (p *publishingRuntimeVisibleGrantProvider) PublishSegmentSeal(_ context.Context, grant rootproto.VisibleAuthorityGrant, segment fsperas.PerasSegment, digest [32]byte, cursor InstallCursor) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sealCalls++
	p.sealedGrant = grant
	p.sealedSegment = segment
	p.sealedDigest = digest
	p.sealedCursor = cursor
	return p.sealErr
}

type fakeRuntimePerasRetirer struct {
	calls  int
	scopes []compile.AuthorityScope
	err    error
}

func (r *fakeRuntimePerasRetirer) RetireVisibleAuthority(_ context.Context, scopes ...compile.AuthorityScope) error {
	r.calls++
	r.scopes = append(r.scopes, scopes...)
	return r.err
}

func requireRuntimeSegmentWitness(t *testing.T, c *Runtime, scope compile.AuthorityScope, holder *fsperas.Holder, segment fsperas.PerasSegment, payload []byte, digest [32]byte) {
	t.Helper()
	grant, err := c.segmentWitnessGrant(context.Background(), scope, holder)
	require.NoError(t, err)
	record := c.segmentWitnessRecord(grant, holder, segment, payload, digest, c.nextWitnessUnixNano())
	require.NoError(t, c.appendSegmentWitnessRecords(context.Background(), scope, []fsperas.SegmentWitnessRecord{record}))
}

func testRuntimeSegmentWitnesses(tb testing.TB, n int) []fsperas.WitnessReplica {
	tb.Helper()
	witnesses := make([]fsperas.WitnessReplica, 0, n)
	for i := range n {
		witnesses = append(witnesses, &recordingRuntimeSegmentWitness{id: fmt.Sprintf("witness-%d", i)})
	}
	return witnesses
}

func scanOnlyRuntimeSegmentWitnesses(tb testing.TB, n int) ([]*recordingRuntimeSegmentWitness, []fsperas.WitnessReplica) {
	tb.Helper()
	recorders := make([]*recordingRuntimeSegmentWitness, 0, n)
	witnesses := make([]fsperas.WitnessReplica, 0, n)
	for i := range n {
		recorder := &recordingRuntimeSegmentWitness{id: fmt.Sprintf("witness-%d", i)}
		recorders = append(recorders, recorder)
		witnesses = append(witnesses, &scanOnlyRuntimeSegmentWitness{base: recorder})
	}
	return recorders, witnesses
}

type recordingRuntimeSegmentWitness struct {
	id                string
	mu                sync.Mutex
	records           []fsperas.SegmentWitnessRecord
	probeCalls        int
	probeSegmentCalls int
}

func (w *recordingRuntimeSegmentWitness) ID() string { return w.id }

func (w *recordingRuntimeSegmentWitness) AppendSegments(_ context.Context, _ compile.AuthorityScope, records []fsperas.SegmentWitnessRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.records = append(w.records, records...)
	return nil
}

func (w *recordingRuntimeSegmentWitness) recordCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.records)
}

func (w *recordingRuntimeSegmentWitness) Probe(_ context.Context, epochID uint64) (fsperas.WitnessSnapshot, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.probeCalls++
	out := fsperas.WitnessSnapshot{}
	for _, record := range w.records {
		if record.EpochID == epochID {
			out.Segments = append(out.Segments, record)
		}
	}
	return out, nil
}

func (w *recordingRuntimeSegmentWitness) ProbeSegment(_ context.Context, ref fsperas.WitnessSegmentRef) (fsperas.SegmentWitnessRecord, bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.probeSegmentCalls++
	for _, record := range w.records {
		if record.EpochID == ref.EpochID && record.SegmentRoot == ref.SegmentRoot && record.SegmentPayloadDigest == ref.SegmentPayloadDigest {
			return record, true, nil
		}
	}
	return fsperas.SegmentWitnessRecord{}, false, nil
}

type scanOnlyRuntimeSegmentWitness struct {
	base *recordingRuntimeSegmentWitness
}

func (w *scanOnlyRuntimeSegmentWitness) ID() string {
	if w == nil || w.base == nil {
		return ""
	}
	return w.base.ID()
}

func (w *scanOnlyRuntimeSegmentWitness) AppendSegments(ctx context.Context, scope compile.AuthorityScope, records []fsperas.SegmentWitnessRecord) error {
	if w == nil || w.base == nil {
		return fsperas.ErrWitnessReplicaInvalid
	}
	return w.base.AppendSegments(ctx, scope, records)
}

func (w *scanOnlyRuntimeSegmentWitness) Probe(ctx context.Context, epochID uint64) (fsperas.WitnessSnapshot, error) {
	if w == nil || w.base == nil {
		return fsperas.WitnessSnapshot{}, fsperas.ErrWitnessReplicaInvalid
	}
	return w.base.Probe(ctx, epochID)
}

func (w *scanOnlyRuntimeSegmentWitness) ProbeSegment(context.Context, fsperas.WitnessSegmentRef) (fsperas.SegmentWitnessRecord, bool, error) {
	if w == nil || w.base == nil {
		return fsperas.SegmentWitnessRecord{}, false, fsperas.ErrWitnessReplicaInvalid
	}
	w.base.mu.Lock()
	w.base.probeSegmentCalls++
	w.base.mu.Unlock()
	return fsperas.SegmentWitnessRecord{}, false, nil
}

type authorityRenewCheckingRuntimeSegmentWitness struct {
	id              string
	provider        *fakeRuntimeVisibleGrantProvider
	minAcquireCalls int64
	recordingRuntimeSegmentWitness
}

func (w *authorityRenewCheckingRuntimeSegmentWitness) ID() string { return w.id }

func (w *authorityRenewCheckingRuntimeSegmentWitness) AppendSegments(ctx context.Context, scope compile.AuthorityScope, records []fsperas.SegmentWitnessRecord) error {
	if w.provider.acquireCalls.Load() < w.minAcquireCalls {
		return fmt.Errorf("witness started before authority renewal")
	}
	w.recordingRuntimeSegmentWitness.id = w.id
	return w.recordingRuntimeSegmentWitness.AppendSegments(ctx, scope, records)
}

func testRuntimeCommitterGrant() rootproto.VisibleAuthorityGrant {
	return rootproto.VisibleAuthorityGrant{
		GrantID:          "grant-1",
		EpochID:          1,
		HolderID:         "holder-a",
		ExpiresUnixNano:  time.Now().Add(time.Hour).UnixNano(),
		RootClusterEpoch: 1,
		IssuedRootToken: rootproto.AuthorityRootToken{
			Term:     1,
			Index:    1,
			Revision: 1,
		},
		Scope: rootproto.VisibleAuthorityScope{
			MountID:    "vol",
			MountKeyID: 1,
			Parents:    []uint64{1},
			Inodes:     []uint64{1, 2},
		},
	}
}

func testRuntimeVisibleRecord(grant rootproto.VisibleAuthorityGrant, scope compile.AuthorityScope, replay fsperas.ReplayOperation) fsperas.VisibleOperationRecord {
	return fsperas.VisibleOperationRecord{
		EpochID:           grant.EpochID,
		HolderID:          grant.HolderID,
		GrantID:           grant.GrantID,
		GrantExpiresNanos: grant.ExpiresUnixNano,
		PredecessorDigest: grant.PredecessorDigest,
		RootLineage:       visibleRootLineageFromGrant(grant),
		Scope:             scope,
		Operation:         replay,
		TimestampUnixNano: time.Now().UnixNano(),
	}
}

func testRuntimeSegmentRootSeal(id, holder string, scope compile.AuthorityScope, sealed time.Time) rootproto.VisibleAuthoritySeal {
	return rootproto.VisibleAuthoritySeal{
		GrantID:              id,
		EpochID:              1,
		HolderID:             holder,
		Scope:                AuthorityScopeFromDelta(scope),
		SegmentRoot:          [32]byte{1},
		SegmentPayloadDigest: [32]byte{2},
		OperationCount:       3,
		EntryCount:           4,
		SealedUnixNano:       sealed.UnixNano(),
		InstallRegionID:      5,
		InstallTerm:          6,
		InstallIndex:         7,
		InstallVersion:       8,
	}
}

func testRuntimeSegmentRootSegment(t *testing.T) fsperas.PerasSegment {
	t.Helper()
	mount := fsmeta.MountIdentity{MountID: "vol", MountKeyID: 7}
	dentry, err := fsmeta.EncodeDentryKey(mount, 99, "a")
	require.NoError(t, err)
	inode, err := fsmeta.EncodeInodeKey(mount, 100)
	require.NoError(t, err)
	segment, err := fsperas.BuildPerasSegmentFromReplayPlan(fsperas.ReplayPlan{
		EpochID: 1,
		Operations: []fsperas.ReplayOperation{
			{
				OpID: fsperas.OperationID{ClientID: "client", Seq: 1},
				Mutations: []fsperas.ReplayMutation{
					{Key: dentry, Value: []byte("dentry-value")},
					{Key: inode, Value: []byte("inode-value")},
				},
			},
		},
	})
	require.NoError(t, err)
	return segment
}

func testPerasInstallCursor(offset uint64) InstallCursor {
	if offset == 0 {
		offset = 1
	}
	return InstallCursor{
		RegionID:       10 + offset,
		Term:           20 + offset,
		Index:          30 + offset,
		InstallVersion: 40 + offset,
	}
}

var testRuntimeMount = fsmeta.MountIdentity{MountID: "vol", MountKeyID: 1}

func testRuntimePerasOp(dentryKey, inodeKey []byte) compile.MaterializedOp {
	mount, parent, name, inode := testRuntimeCreateArgs(dentryKey, inodeKey)
	return testRuntimeCreateOp(mount, parent, name, inode, []byte("dentry-value"), []byte("inode-value"))
}

func testRuntimeCreateOp(mount fsmeta.MountIdentity, parent fsmeta.InodeID, name string, inode fsmeta.InodeID, dentryValue, inodeValue []byte) compile.MaterializedOp {
	program, err := compile.CompileCreateProgram(fsmeta.CreateRequest{
		Mount:  mount.MountID,
		Parent: parent,
		Name:   name,
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile, Mode: 0o644},
	}, mount, inode)
	if err != nil {
		panic(err)
	}
	parentValue, err := fsmeta.EncodeInodeValue(fsmeta.InodeRecord{
		Inode:      parent,
		Type:       fsmeta.InodeTypeDirectory,
		LinkCount:  1,
		ChildCount: 1,
	})
	if err != nil {
		panic(err)
	}
	if dentryValue == nil {
		dentryValue = program.Compiled.Delta.WriteEffects[1].Value
	}
	if _, err := fsmeta.DecodeInodeValue(inodeValue); inodeValue == nil || err != nil {
		inodeValue = program.Compiled.Delta.WriteEffects[2].Value
	}
	op, err := compile.MaterializeCreate(program, compile.CreateValues{
		ParentInodeValue: parentValue,
		DentryValue:      dentryValue,
		InodeValue:       inodeValue,
	})
	if err != nil {
		panic(err)
	}
	return testRuntimeSealMaterializedOp(op)
}

func testRuntimeSealMaterializedOp(op compile.MaterializedOp) compile.MaterializedOp {
	proofs := testRuntimePredicateProofsForOp(op)
	guardProofs, err := compile.GuardProofsFor(op.CompiledOp, proofs, op.Delta.RuntimeGuards)
	if err != nil {
		panic(err)
	}
	return compile.WithAdmissionProofs(op, proofs, guardProofs)
}

func testRuntimePredicateProofsForOp(op compile.MaterializedOp) []proof.PredicateProof {
	if len(op.Delta.ReadPredicates) == 0 {
		return nil
	}
	proofs := make([]proof.PredicateProof, 0, len(op.Delta.ReadPredicates))
	seen := make(map[string]struct{}, len(op.Delta.ReadPredicates))
	for _, predicate := range op.Delta.ReadPredicates {
		if _, ok := seen[string(predicate.Key)]; ok {
			continue
		}
		seen[string(predicate.Key)] = struct{}{}
		frontier := proof.ProofFrontier{EpochID: 1, Sequence: 1}
		switch predicate.Kind {
		case compile.PredicateExists:
			proofs = append(proofs, proof.NewPredicateProof(predicate.Key, nil, true, 0, proof.ReadSourceOverlay, frontier))
		case compile.PredicateNotExists:
			proofs = append(proofs, proof.NewPredicateProof(predicate.Key, nil, false, 0, proof.ReadSourceOverlay, frontier))
		case compile.PredicateObservedValue:
			proofs = append(proofs, proof.NewPredicateProof(predicate.Key, predicate.ExpectedValue, true, 0, proof.ReadSourceOverlay, frontier))
		}
	}
	return proofs
}

func testRuntimeDentryEffect(op compile.MaterializedOp) compile.EffectPlan {
	if len(op.Effects) < 2 {
		panic("test runtime create op missing dentry effect")
	}
	return op.Effects[1]
}

func testRuntimeInodeEffect(op compile.MaterializedOp) compile.EffectPlan {
	if len(op.Effects) < 3 {
		panic("test runtime create op missing inode effect")
	}
	return op.Effects[2]
}

func testRuntimeCreateArgs(dentryKey, inodeKey []byte) (fsmeta.MountIdentity, fsmeta.InodeID, string, fsmeta.InodeID) {
	mount := testRuntimeMount
	parent := fsmeta.RootInode
	name := testRuntimeNameFromKey(dentryKey)
	inode := fsmeta.InodeID(0)
	inodeFromStorageKey := false
	if parts, ok := fsmeta.InspectKey(dentryKey); ok {
		mount.MountKeyID = parts.MountKeyID
		switch parts.Kind {
		case fsmeta.KeyKindDentry:
			parent = parts.Parent
			if dentryName, ok := fsmeta.DentryNameOfKey(dentryKey); ok {
				name = dentryName
			}
		case fsmeta.KeyKindInode, fsmeta.KeyKindChunk, fsmeta.KeyKindSession:
			parent = parts.Inode
		}
	}
	if parts, ok := fsmeta.InspectKey(inodeKey); ok {
		if mount.MountKeyID == 0 {
			mount.MountKeyID = parts.MountKeyID
		}
		switch parts.Kind {
		case fsmeta.KeyKindInode, fsmeta.KeyKindChunk, fsmeta.KeyKindSession:
			inode = parts.Inode
			inodeFromStorageKey = true
		}
	}
	if !inodeFromStorageKey {
		inode = testRuntimeInodeForBucketSeed(fsmeta.BucketForInodeID(parent), inodeKey)
	}
	if inode == 0 || inode == parent {
		inode = testRuntimeInodeForBucketSeed(fsmeta.BucketForInodeID(parent), append(append([]byte(nil), inodeKey...), '#'))
	}
	return mount, parent, name, inode
}

func testRuntimeNameFromKey(key []byte) string {
	if name, ok := fsmeta.DentryNameOfKey(key); ok {
		return name
	}
	label := string(key)
	if slash := strings.LastIndex(label, "/"); slash >= 0 {
		label = label[slash+1:]
	}
	if label == "" || label == "." || label == ".." || strings.ContainsAny(label, "/\x00") {
		return "k-" + hex.EncodeToString(key)
	}
	return label
}

func testRuntimeInodeFromKey(key []byte) fsmeta.InodeID {
	var hash uint64 = 1469598103934665603
	for _, b := range key {
		hash ^= uint64(b)
		hash *= 1099511628211
	}
	return fsmeta.InodeID(2 + hash%1_000_000)
}

func testRuntimeDentryKeyForLabel(label string) []byte {
	return testRuntimeDentryEffect(testRuntimePerasOp([]byte("dentry/"+label), []byte("inode/"+label))).Key
}

func testRuntimeRootDentryPrefix() []byte {
	prefix, err := fsmeta.EncodeDentryPrefix(testRuntimeMount, fsmeta.RootInode)
	if err != nil {
		panic(err)
	}
	return prefix
}

func testRuntimeRenameDentryOp(fromName, toName string, toValue []byte) compile.MaterializedOp {
	program, err := compile.CompileRenameProgram(fsmeta.RenameRequest{
		Mount:      testRuntimeMount.MountID,
		FromParent: fsmeta.RootInode,
		FromName:   fromName,
		ToParent:   fsmeta.RootInode,
		ToName:     toName,
	}, testRuntimeMount)
	if err != nil {
		panic(err)
	}
	fromKey, err := fsmeta.EncodeDentryKey(testRuntimeMount, fsmeta.RootInode, fromName)
	if err != nil {
		panic(err)
	}
	toKey, err := fsmeta.EncodeDentryKey(testRuntimeMount, fsmeta.RootInode, toName)
	if err != nil {
		panic(err)
	}
	parentKey, err := fsmeta.EncodeInodeKey(testRuntimeMount, fsmeta.RootInode)
	if err != nil {
		panic(err)
	}
	parentValue, err := fsmeta.EncodeInodeValue(fsmeta.InodeRecord{
		Inode:      fsmeta.RootInode,
		Type:       fsmeta.InodeTypeDirectory,
		LinkCount:  1,
		ChildCount: 1,
	})
	if err != nil {
		panic(err)
	}
	frontier := proof.ProofFrontier{EpochID: 1, Sequence: 1}
	op, err := compile.MaterializeCompiledOpWithEvidence(program.Compiled, []compile.WriteEffect{
		{Kind: compile.EffectDelete, Key: fromKey},
		{Kind: compile.EffectPut, Key: toKey, Value: toValue},
		{Kind: compile.EffectPut, Key: parentKey, Value: parentValue},
		{Kind: compile.EffectPut, Key: parentKey, Value: parentValue},
	}, compile.PredicateEvidence{Proofs: []proof.PredicateProof{
		proof.NewPredicateProof(fromKey, []byte("from"), true, 0, proof.ReadSourceOverlay, frontier),
		proof.NewPredicateProof(toKey, nil, false, 0, proof.ReadSourceOverlay, frontier),
		proof.NewPredicateProof(parentKey, parentValue, true, 0, proof.ReadSourceOverlay, frontier),
	}}, nil)
	if err != nil {
		panic(err)
	}
	return testRuntimeSealMaterializedOp(op)
}

func testRuntimePerasOpForBucket(dentryKey, inodeKey []byte, bucket fsmeta.AffinityBucket) compile.MaterializedOp {
	parent := testRuntimeInodeForBucketValue(bucket)
	inode := parent + fsmeta.InodeID(fsmeta.DefaultAffinityBucketCount)
	name := testRuntimeNameFromKey(dentryKey)
	return testRuntimeCreateOp(testRuntimeMount, parent, name, inode, []byte("dentry-value"), []byte("inode-value"))
}

func testRuntimeInodeForBucketValue(bucket fsmeta.AffinityBucket) fsmeta.InodeID {
	for inode := fsmeta.InodeID(2); inode < 100_000; inode++ {
		if fsmeta.BucketForInodeID(inode) == bucket {
			return inode
		}
	}
	panic(fmt.Sprintf("no inode found for bucket %d", bucket))
}

func testRuntimeInodeForBucketSeed(bucket fsmeta.AffinityBucket, seed []byte) fsmeta.InodeID {
	start := uint64(testRuntimeInodeFromKey(seed))
	for offset := range uint64(1_000_000) {
		inode := fsmeta.InodeID(2 + (start+offset)%1_000_000)
		if fsmeta.BucketForInodeID(inode) == bucket {
			return inode
		}
	}
	panic(fmt.Sprintf("no inode found for bucket %d", bucket))
}

func testRuntimePerasOpWithScope(op compile.MaterializedOp, scope compile.AuthorityScope) compile.MaterializedOp {
	mount, parent, name, inode := testRuntimeCreateArgs(testRuntimeDentryEffect(op).Key, testRuntimeInodeEffect(op).Key)
	if scope.Mount != "" {
		mount.MountID = scope.Mount
	}
	if scope.MountKeyID != 0 {
		mount.MountKeyID = scope.MountKeyID
	}
	if len(scope.Parents) > 0 {
		parent = scope.Parents[0]
	}
	if len(scope.Inodes) > 0 {
		inode = scope.Inodes[0]
	}
	if name == "" {
		name = "entry"
	}
	return testRuntimeCreateOp(mount, parent, name, inode, []byte("dentry-value"), []byte("inode-value"))
}

func appendUvarintKey(prefix string, v uint64) []byte {
	out := append([]byte(prefix), 0)
	return binary.AppendUvarint(out, v)
}

func testRuntimeBucketKeys(tb testing.TB, mount fsmeta.MountIdentity, bucket fsmeta.AffinityBucket) ([]byte, []byte) {
	tb.Helper()
	var first, second []byte
	for inode := fsmeta.InodeID(2); inode < 100_000; inode++ {
		if fsmeta.BucketForInodeID(inode) != bucket {
			continue
		}
		key, err := fsmeta.EncodeInodeKey(mount, inode)
		require.NoError(tb, err)
		if first == nil {
			first = key
			continue
		}
		second = key
		break
	}
	require.NotNil(tb, first)
	require.NotNil(tb, second)
	return first, second
}

func testRuntimeInodeForBucket(tb testing.TB, bucket fsmeta.AffinityBucket) fsmeta.InodeID {
	tb.Helper()
	for inode := fsmeta.InodeID(2); inode < 100_000; inode++ {
		if fsmeta.BucketForInodeID(inode) == bucket {
			return inode
		}
	}
	tb.Fatalf("no inode found for bucket %d", bucket)
	return 0
}
