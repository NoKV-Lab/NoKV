// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package wal_test

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/storage/vfs"
	"github.com/feichai0017/NoKV/storage/wal"
	"github.com/stretchr/testify/require"
)

func TestManagerFsyncBatchedGroupsConcurrentAppends(t *testing.T) {
	dir := t.TempDir()
	segment := filepath.Join(dir, "00001.wal")
	syncBlocked := make(chan struct{})
	releaseSync := make(chan struct{})
	syncSeen := make(chan struct{})
	var once sync.Once
	policy := vfs.NewFaultPolicy()
	policy.SetHook(func(op vfs.Op, path string) error {
		if op == vfs.OpFileSync && path == segment {
			once.Do(func() {
				close(syncSeen)
				<-syncBlocked
				<-releaseSync
			})
		}
		return nil
	})
	mgr, err := wal.Open(wal.Config{Dir: dir, FS: vfs.NewFaultFSWithPolicy(vfs.OSFS{}, policy)})
	require.NoError(t, err)
	defer func() { _ = mgr.Close() }()

	const writers = 8
	start := make(chan struct{})
	errs := make(chan error, writers)
	for i := range writers {
		go func() {
			<-start
			_, err := mgr.AppendRecords(wal.DurabilityFsyncBatched, wal.Record{
				Type:    wal.RecordTypeRaftEntry,
				Payload: []byte{byte(i)},
			})
			errs <- err
		}()
	}
	close(start)

	select {
	case <-syncSeen:
	case <-time.After(time.Second):
		t.Fatalf("expected first batched fsync")
	}
	close(syncBlocked)
	time.Sleep(10 * time.Millisecond)

	var pending int
	for {
		select {
		case err := <-errs:
			require.NoError(t, err)
		default:
			goto done
		}
		pending++
	}
done:
	require.Less(t, pending, writers, "batched append returned before fsync was released")

	close(releaseSync)
	for range writers - pending {
		require.NoError(t, <-errs)
	}

	got := visiblePayloads(t, dir, wal.RecordTypeRaftEntry)
	require.Len(t, got, writers)
}

func TestManagerFsyncBatchedPropagatesSyncErrorToBatch(t *testing.T) {
	dir := t.TempDir()
	segment := filepath.Join(dir, "00001.wal")
	injected := errors.New("sync failed")
	policy := vfs.NewFaultPolicy(vfs.FailOnceRule(vfs.OpFileSync, segment, injected))
	mgr, err := wal.Open(wal.Config{Dir: dir, FS: vfs.NewFaultFSWithPolicy(vfs.OSFS{}, policy)})
	require.NoError(t, err)
	defer func() { _ = mgr.Close() }()

	_, err = mgr.AppendRecords(wal.DurabilityFsyncBatched, wal.Record{
		Type:    wal.RecordTypeRaftEntry,
		Payload: []byte("x"),
	})
	require.ErrorIs(t, err, injected)
}

func TestManagerRotateWaitsForInflightBatchedFsync(t *testing.T) {
	dir := t.TempDir()
	segment := filepath.Join(dir, "00001.wal")
	releaseSync := make(chan struct{})
	syncSeen := make(chan struct{})
	var once sync.Once
	policy := vfs.NewFaultPolicy()
	policy.SetHook(func(op vfs.Op, path string) error {
		if op == vfs.OpFileSync && path == segment {
			once.Do(func() {
				close(syncSeen)
				<-releaseSync
			})
		}
		return nil
	})
	mgr, err := wal.Open(wal.Config{Dir: dir, FS: vfs.NewFaultFSWithPolicy(vfs.OSFS{}, policy)})
	require.NoError(t, err)
	defer func() { _ = mgr.Close() }()

	appendDone := make(chan error, 1)
	go func() {
		_, err := mgr.AppendRecords(wal.DurabilityFsyncBatched, wal.Record{
			Type:    wal.RecordTypeRaftEntry,
			Payload: []byte("pinned"),
		})
		appendDone <- err
	}()

	select {
	case <-syncSeen:
	case <-time.After(time.Second):
		t.Fatalf("expected batched fsync to start")
	}

	rotateDone := make(chan error, 1)
	go func() {
		rotateDone <- mgr.Rotate()
	}()

	select {
	case err := <-rotateDone:
		t.Fatalf("rotate completed while batched fsync still held active segment: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	close(releaseSync)
	require.NoError(t, <-appendDone)
	require.NoError(t, <-rotateDone)
	require.Equal(t, uint32(2), mgr.ActiveSegment())
}
