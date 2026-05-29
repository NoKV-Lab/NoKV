// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package wal

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newTestCommitBatch() (*sync.Mutex, *commitBatch) {
	mu := &sync.Mutex{}
	b := &commitBatch{}
	b.cond = sync.NewCond(mu)
	return mu, b
}

func TestCommitBatchEnqueueAssignsSequentialSeqs(t *testing.T) {
	mu, b := newTestCommitBatch()
	mu.Lock()
	defer mu.Unlock()

	seq1, leader1, err := b.enqueueLocked()
	require.NoError(t, err)
	require.True(t, leader1, "first writer in a round must be the leader")
	require.Equal(t, uint64(1), seq1)

	seq2, leader2, err := b.enqueueLocked()
	require.NoError(t, err)
	require.False(t, leader2, "subsequent writers in the same round must not be leaders")
	require.Equal(t, uint64(2), seq2)
}

func TestCommitBatchEnqueueRejectsClosed(t *testing.T) {
	mu, b := newTestCommitBatch()
	mu.Lock()
	defer mu.Unlock()
	b.closeLocked()

	_, _, err := b.enqueueLocked()
	require.Error(t, err)
}

func TestCommitBatchCompleteWakesWaiters(t *testing.T) {
	mu, b := newTestCommitBatch()

	mu.Lock()
	seq, leader, err := b.enqueueLocked()
	require.NoError(t, err)
	require.True(t, leader)
	mu.Unlock()

	waitDone := make(chan error, 1)
	go func() {
		mu.Lock()
		waitDone <- b.waitLocked(seq)
		mu.Unlock()
	}()

	// Give the waiter a chance to park on the cond.
	time.Sleep(10 * time.Millisecond)
	mu.Lock()
	b.completeLocked(seq, nil)
	mu.Unlock()

	select {
	case err := <-waitDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatalf("waiter never returned after completeLocked")
	}
}

func TestCommitBatchCompletePropagatesError(t *testing.T) {
	mu, b := newTestCommitBatch()
	mu.Lock()
	defer mu.Unlock()

	seq, _, err := b.enqueueLocked()
	require.NoError(t, err)

	want := errors.New("flush failed")
	b.completeLocked(seq, want)
	require.ErrorIs(t, b.waitLocked(seq), want)
}

func TestCommitBatchCloseUnblocksPendingWaiter(t *testing.T) {
	mu, b := newTestCommitBatch()

	mu.Lock()
	seq, _, err := b.enqueueLocked()
	require.NoError(t, err)
	mu.Unlock()

	waitDone := make(chan error, 1)
	go func() {
		mu.Lock()
		waitDone <- b.waitLocked(seq)
		mu.Unlock()
	}()

	time.Sleep(10 * time.Millisecond)
	mu.Lock()
	b.closeLocked()
	mu.Unlock()

	select {
	case err := <-waitDone:
		require.Error(t, err, "closed batch must surface an error to pending waiters")
	case <-time.After(time.Second):
		t.Fatalf("waiter never returned after closeLocked")
	}
}

func TestCommitBatchPendingTracksDelta(t *testing.T) {
	mu, b := newTestCommitBatch()
	mu.Lock()
	defer mu.Unlock()

	require.False(t, b.pendingLocked(), "fresh batch has no pending work")

	seq, _, err := b.enqueueLocked()
	require.NoError(t, err)
	require.True(t, b.pendingLocked(), "enqueued seq must show as pending")

	b.completeLocked(seq, nil)
	require.False(t, b.pendingLocked(), "fully drained batch must not show pending")

	_, _, err = b.enqueueLocked()
	require.NoError(t, err)
	b.closeLocked()
	require.False(t, b.pendingLocked(), "closed batch must never report pending work")
}

func TestCommitBatchLeaderResetsAfterRound(t *testing.T) {
	mu, b := newTestCommitBatch()
	mu.Lock()
	defer mu.Unlock()

	seq1, leader1, err := b.enqueueLocked()
	require.NoError(t, err)
	require.True(t, leader1)
	b.completeLocked(seq1, nil)

	// New round: the next writer to arrive after a fully-drained round
	// must again become the leader.
	_, leader2, err := b.enqueueLocked()
	require.NoError(t, err)
	require.True(t, leader2, "first writer of a fresh round must be the leader")
}
