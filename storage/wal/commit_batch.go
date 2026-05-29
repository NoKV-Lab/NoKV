// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package wal

import (
	"fmt"
	"sync"
)

// commitBatch coordinates a single flavor of group-commit work
// (page-cache flush, fsync, ...). Writers assign themselves a sequence
// number and wait until that seq has been persisted by a worker
// goroutine. The first writer in a round becomes the leader and is
// responsible for spawning the worker; subsequent writers in the same
// round just queue and wait. After a debounce window the worker drains
// every queued seq in a single round of physical work.
//
// All fields are guarded by Manager.mu; the cond is associated with
// that same mutex. The struct itself owns no locking.
type commitBatch struct {
	cond       *sync.Cond
	batchSeq   uint64
	durableSeq uint64
	err        error
	closed     bool
}

// enqueueLocked claims the next sequence number, returns it together
// with whether the caller is the leader of this round (the writer
// that must spawn the worker). Caller must hold Manager.mu.
func (b *commitBatch) enqueueLocked() (seq uint64, leader bool, err error) {
	if b.closed {
		return 0, false, fmt.Errorf("wal: manager closed")
	}
	b.batchSeq++
	return b.batchSeq, b.batchSeq-b.durableSeq == 1, nil
}

// waitLocked blocks until the given seq has been persisted or the
// batch is closed. Caller must hold Manager.mu.
func (b *commitBatch) waitLocked(seq uint64) error {
	for b.durableSeq < seq && !b.closed {
		b.cond.Wait()
	}
	if b.durableSeq >= seq {
		return b.err
	}
	return fmt.Errorf("wal: manager closed")
}

// completeLocked records the result of a finished round and wakes
// every waiter. Caller must hold Manager.mu.
func (b *commitBatch) completeLocked(target uint64, err error) {
	b.err = err
	b.durableSeq = target
	b.cond.Broadcast()
}

// closeLocked stops accepting new work and wakes any pending waiters
// so they observe the closed state. Caller must hold Manager.mu.
func (b *commitBatch) closeLocked() {
	b.closed = true
	if b.cond != nil {
		b.cond.Broadcast()
	}
}

// pendingLocked reports whether there are seqs still waiting for the
// next round. Caller must hold Manager.mu.
func (b *commitBatch) pendingLocked() bool {
	return b.batchSeq > b.durableSeq && !b.closed
}
