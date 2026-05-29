// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package thermos implements a lightweight bounded-memory hot-key
// detector + early-skew observer for NoKV's write path.
//
// Design lineage: the original HotRing paper (FAST '20) inspired the
// hot-aware data-structure idea, but this package is deliberately a
// side-channel observer rather than a primary index. It uses bucketed
// atomic lists + rotating time windows to maintain bounded-memory
// statistics with no allocation in the hot path.
//
// Consumers: stats reporting (expvar), write-path admission (via
// ErrHotKeyWriteThrottle), and benchmarks. The decision of
// what to do with a detected hot key (throttle / reject / reroute) is
// a caller-side policy, not something this package imposes.
package thermos

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	xxhash "github.com/cespare/xxhash/v2"
)

type HashFn func(string) uint32

// Item captures a hot key and its access counter.
type Item struct {
	Key   string
	Count int32
}

// Thermos keeps track of frequently accessed keys using lock-free bucketed lists.
type Thermos struct {
	hashFn   HashFn
	hashMask uint32
	buckets  []atomic.Pointer[Node]

	windowSlots   atomic.Int32
	windowSlotDur atomic.Int64

	decayInterval atomic.Int64
	decayShift    atomic.Uint32
	lastDecayUnix atomic.Int64

	touches   atomic.Uint64
	inserts   atomic.Uint64
	removes   atomic.Uint64
	clamps    atomic.Uint64
	decayRuns atomic.Uint64
	nodes     atomic.Uint64
	nodeCap   atomic.Uint64

	sampleMask  atomic.Uint32
	sampleDrops atomic.Uint64

	observer atomic.Pointer[observerHolder]

	decayMu   sync.Mutex
	decayStop chan struct{}
	decayWG   sync.WaitGroup
}

const defaultTableBits = 12 // 4096 buckets by default

// NewThermos builds a ring with 2^bits buckets. When fn is nil a fast xxhash-based hash is used.
func NewThermos(bits uint8, fn HashFn) *Thermos {
	if bits == 0 || bits > 20 {
		bits = defaultTableBits
	}
	if fn == nil {
		fn = defaultHash
	}
	size := 1 << bits
	mask := uint32(size - 1)
	return &Thermos{
		hashFn:   fn,
		hashMask: mask,
		buckets:  make([]atomic.Pointer[Node], size),
	}
}

// EnableSlidingWindow configures the ring to maintain a time-based sliding window.
// slots specifies how many buckets to retain, while slotDuration controls how long
// each bucket remains active. Passing non-positive values disables the window.
func (h *Thermos) EnableSlidingWindow(slots int, slotDuration time.Duration) {
	if slots <= 0 || slotDuration <= 0 {
		h.windowSlots.Store(0)
		h.windowSlotDur.Store(0)
		return
	}
	h.windowSlots.Store(int32(slots))
	h.windowSlotDur.Store(slotDuration.Nanoseconds())
}

// EnableDecay applies periodic right-shift decay to the raw counters.
// interval <= 0 or shift == 0 disables background decay.
func (h *Thermos) EnableDecay(interval time.Duration, shift uint32) {
	h.stopDecay()
	if interval <= 0 || shift == 0 {
		h.decayInterval.Store(0)
		h.decayShift.Store(0)
		return
	}
	h.decayInterval.Store(interval.Nanoseconds())
	h.decayShift.Store(shift)
	h.decayMu.Lock()
	stop := make(chan struct{})
	h.decayStop = stop
	h.decayWG.Add(1)
	h.decayMu.Unlock()

	go h.decayLoop(stop, interval, shift)
}

// EnableNodeSampling caps node growth and applies stable sampling once the cap is reached.
// cap <= 0 disables the cap. sampleBits controls the sampling rate (1/2^sampleBits).
// sampleBits == 0 means no sampling when the cap is exceeded (strict cap).
func (h *Thermos) EnableNodeSampling(cap uint64, sampleBits uint8) {
	if h == nil {
		return
	}
	if cap == 0 {
		h.nodeCap.Store(0)
		h.sampleMask.Store(0)
		return
	}
	if sampleBits > 31 {
		sampleBits = 31
	}
	var mask uint32
	if sampleBits > 0 {
		mask = (1 << sampleBits) - 1
	}
	h.nodeCap.Store(cap)
	h.sampleMask.Store(mask)
}

// Close releases background resources attached to the ring.
func (h *Thermos) Close() {
	h.stopDecay()
}

func defaultHash(key string) uint32 {
	return uint32(xxhash.Sum64String(key))
}

func (h *Thermos) stopDecay() {
	h.decayMu.Lock()
	stop := h.decayStop
	if stop != nil {
		close(stop)
		h.decayStop = nil
	}
	h.decayMu.Unlock()
	if stop != nil {
		h.decayWG.Wait()
	}
}

func (h *Thermos) decayLoop(stop <-chan struct{}, interval time.Duration, shift uint32) {
	ticker := time.NewTicker(interval)
	defer func() {
		ticker.Stop()
		h.decayWG.Done()
	}()
	for {
		select {
		case <-ticker.C:
			h.applyDecay(shift)
		case <-stop:
			return
		}
	}
}

func (h *Thermos) applyDecay(shift uint32) {
	if shift == 0 {
		return
	}
	h.decayRuns.Add(1)
	h.lastDecayUnix.Store(time.Now().Unix())
	if obs := h.getObserver(); obs != nil {
		obs.OnDecay(shift)
	}
	for i := range h.buckets {
		for node := h.buckets[i].Load(); node != nil; node = node.Next() {
			node.decay(shift)
		}
	}
}

func (h *Thermos) slotState() (slot int64, slots int) {
	slots = int(h.windowSlots.Load())
	slotDur := h.windowSlotDur.Load()
	if slots <= 0 || slotDur <= 0 {
		return 0, 0
	}
	return time.Now().UnixNano() / slotDur, slots
}

func (h *Thermos) nodeCount(node *Node, slots int, slot int64) int32 {
	if node == nil {
		return 0
	}
	if slots > 0 {
		return node.windowTotalAt(slots, slot)
	}
	return node.GetCounter()
}

func (h *Thermos) incrementNode(node *Node, slots int, slot int64) int32 {
	if node == nil {
		return 0
	}
	node.Increment()
	if slots > 0 {
		return node.incrementWindow(slots, slot)
	}
	return node.GetCounter()
}

// Touch records a key access and returns the updated counter.
func (h *Thermos) Touch(key string) int32 {
	if h == nil || key == "" {
		return 0
	}
	h.touches.Add(1)
	slot, slots := h.slotState()
	hashVal, index, tag := h.hashParts(key)
	compareItem := NewNode(key, tag)
	node, inserted := h.findOrInsert(index, compareItem, slots, slot, hashVal)
	if node == nil {
		return 0
	}
	if inserted {
		h.inserts.Add(1)
		h.nodes.Add(1)
	}
	if inserted && slots == 0 {
		node.ResetCounter()
	}
	count := h.incrementNode(node, slots, slot)
	if obs := h.getObserver(); obs != nil {
		obs.OnTouch(key, count)
	}
	return count
}

// Frequency returns the current access counter for key without mutating state.
func (h *Thermos) Frequency(key string) int32 {
	if h == nil || key == "" {
		return 0
	}
	slot, slots := h.slotState()
	_, index, tag := h.hashParts(key)
	node := h.search(index, NewNode(key, tag))
	return h.nodeCount(node, slots, slot)
}

// TouchAndClamp increments the counter if below the provided limit and reports
// whether the key should be considered throttled.
func (h *Thermos) TouchAndClamp(key string, limit int32) (count int32, limited bool) {
	if h == nil || key == "" {
		return 0, false
	}
	if limit <= 0 {
		return h.Touch(key), false
	}
	h.touches.Add(1)
	slot, slots := h.slotState()
	hashVal, index, tag := h.hashParts(key)
	compareItem := NewNode(key, tag)
	node, inserted := h.findOrInsert(index, compareItem, slots, slot, hashVal)
	if node == nil {
		return 0, false
	}
	if inserted {
		h.inserts.Add(1)
		h.nodes.Add(1)
	}
	if inserted && slots == 0 {
		node.ResetCounter()
	}
	current := h.nodeCount(node, slots, slot)
	if current >= limit {
		h.clamps.Add(1)
		if obs := h.getObserver(); obs != nil {
			obs.OnClamp(key, limit, current)
		}
		return current, true
	}
	count = h.incrementNode(node, slots, slot)
	limited = count >= limit
	if limited {
		h.clamps.Add(1)
		if obs := h.getObserver(); obs != nil {
			obs.OnClamp(key, limit, count)
		}
	} else if obs := h.getObserver(); obs != nil {
		obs.OnTouch(key, count)
	}
	return count, limited
}

func (h *Thermos) Remove(key string) {
	if h == nil || key == "" {
		return
	}
	_, index, tag := h.hashParts(key)
	compareItem := NewNode(key, tag)
	bucket := &h.buckets[index]
	for {
		head := bucket.Load()
		var prev *Node
		curr := head
		for curr != nil && !compareItem.Less(curr) {
			if compareItem.Equal(curr) {
				next := curr.Next()
				if prev == nil {
					if bucket.CompareAndSwap(head, next) {
						h.removes.Add(1)
						h.nodes.Add(^uint64(0))
						return
					}
					break
				}
				if prev.CompareAndSwapNext(curr, next) {
					h.removes.Add(1)
					h.nodes.Add(^uint64(0))
					return
				}
				break
			}
			prev = curr
			curr = curr.Next()
		}
		if curr == nil || compareItem.Less(curr) {
			return
		}
	}
}

// TopN returns at most n hot keys ordered by access count (descending).
func (h *Thermos) TopN(n int) []Item {
	if h == nil || n <= 0 {
		return nil
	}
	slot, slots := h.slotState()

	var items []Item
	for i := range h.buckets {
		for node := h.buckets[i].Load(); node != nil; node = node.Next() {
			items = append(items, Item{
				Key:   node.key,
				Count: h.nodeCount(node, slots, slot),
			})
		}
	}
	if len(items) == 0 {
		return nil
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Count == items[j].Count {
			return items[i].Key < items[j].Key
		}
		return items[i].Count > items[j].Count
	})
	if len(items) > n {
		items = append([]Item(nil), items[:n]...)
	} else {
		items = append([]Item(nil), items...)
	}
	return items
}

// KeysAbove returns all keys whose counters are at least threshold.
func (h *Thermos) KeysAbove(threshold int32) []Item {
	if h == nil || threshold <= 0 {
		return nil
	}
	slot, slots := h.slotState()
	var items []Item
	for i := range h.buckets {
		for node := h.buckets[i].Load(); node != nil; node = node.Next() {
			if cnt := h.nodeCount(node, slots, slot); cnt >= threshold {
				items = append(items, Item{Key: node.key, Count: cnt})
			}
		}
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Count == items[j].Count {
			return items[i].Key < items[j].Key
		}
		return items[i].Count > items[j].Count
	})
	return items
}

func (h *Thermos) hashParts(key string) (hashVal uint32, index uint32, tag uint32) {
	hashVal = h.hashFn(key)
	return hashVal, hashVal & h.hashMask, hashVal & (^h.hashMask)
}

func (h *Thermos) search(index uint32, compareItem *Node) *Node {
	for node := h.buckets[index].Load(); node != nil; node = node.Next() {
		if compareItem.Equal(node) {
			return node
		}
		if compareItem.Less(node) {
			return nil
		}
	}
	return nil
}

// findOrInsert keeps the bucket sorted by (tag,key) using CAS on the head or predecessor.
func (h *Thermos) findOrInsert(index uint32, compareItem *Node, slots int, slot int64, hashVal uint32) (*Node, bool) {
	bucket := &h.buckets[index]
	for {
		head := bucket.Load()
		var prev *Node
		curr := head
		for curr != nil {
			if compareItem.Equal(curr) {
				return curr, false
			}
			if compareItem.Less(curr) {
				break
			}
			prev = curr
			curr = curr.Next()
		}

		if !h.allowInsert(hashVal) {
			h.sampleDrops.Add(1)
			return nil, false
		}

		newNode := NewNode(compareItem.key, compareItem.tag)
		if slots > 0 {
			newNode.ResetCounterWithWindow(slots, slot)
		}
		newNode.SetNext(curr)

		if prev == nil {
			if bucket.CompareAndSwap(head, newNode) {
				return newNode, true
			}
		} else if prev.CompareAndSwapNext(curr, newNode) {
			return newNode, true
		}
	}
}

func (h *Thermos) allowInsert(hashVal uint32) bool {
	cap := h.nodeCap.Load()
	if cap == 0 {
		return true
	}
	if h.nodes.Load() < cap {
		return true
	}
	mask := h.sampleMask.Load()
	if mask == 0 {
		return false
	}
	return (hashVal & mask) == 0
}
