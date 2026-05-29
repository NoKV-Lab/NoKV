// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package slab

import (
	"sync"
	"sync/atomic"
)

// managedSegment wraps a Segment with the lifecycle state machine the
// Manager uses to coordinate seal / read pin / close. Consumers see the
// raw *Segment; the wrapper is private to the manager.
//
// Lifecycle:
//
//	active  → seal()        → sealed
//	sealed  → beginClose()  → closing
//	active  → beginClose()  → closing  (Rewind path: re-activates after)
type managedSegment struct {
	store    *Segment
	state    atomic.Uint32
	pinMu    sync.Mutex
	pinCond  *sync.Cond
	pinCount atomic.Int32
}

type segmentState uint32

const (
	segmentActive segmentState = iota
	segmentSealed
	segmentClosing
)

func newManagedSegment(store *Segment, sealed bool) *managedSegment {
	st := segmentActive
	if sealed {
		st = segmentSealed
	}
	seg := &managedSegment{store: store}
	seg.state.Store(uint32(st))
	seg.pinCond = sync.NewCond(&seg.pinMu)
	return seg
}

func (s *managedSegment) isSealed() bool  { return s.state.Load() == uint32(segmentSealed) }
func (s *managedSegment) isClosing() bool { return s.state.Load() == uint32(segmentClosing) }
func (s *managedSegment) seal()           { s.state.Store(uint32(segmentSealed)) }
func (s *managedSegment) activate()       { s.state.Store(uint32(segmentActive)) }
func (s *managedSegment) beginClose()     { s.state.Store(uint32(segmentClosing)) }

// pinRead bumps the read-pin count after re-checking closing state. Returns
// false if the segment was concurrently closed.
func (s *managedSegment) pinRead() bool {
	if s.isClosing() {
		return false
	}
	s.pinCount.Add(1)
	if s.isClosing() {
		s.pinCount.Add(-1)
		return false
	}
	return true
}

// unpinRead decrements the read pin and wakes any waiter blocked on
// waitForNoPins.
func (s *managedSegment) unpinRead() {
	if s.pinCount.Add(-1) == 0 {
		s.pinMu.Lock()
		s.pinCond.Broadcast()
		s.pinMu.Unlock()
	}
}

// waitForNoPins blocks until in-flight reads release their pins. Used by
// Remove and Rewind before closing the underlying file.
func (s *managedSegment) waitForNoPins() {
	if s.pinCount.Load() == 0 {
		return
	}
	s.pinMu.Lock()
	for s.pinCount.Load() > 0 {
		s.pinCond.Wait()
	}
	s.pinMu.Unlock()
}
