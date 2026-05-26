// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package watch

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/model"
)

const defaultWindow uint32 = 256
const defaultRecentEvents = 4096

// Router fans durable and live fsmeta key events out to prefix subscribers.
type Router struct {
	mu      sync.RWMutex
	next    uint64
	subs    map[uint64]*Subscription
	regions map[uint64]*regionHistory

	published atomic.Uint64
	delivered atomic.Uint64
	dropped   atomic.Uint64
	overflow  atomic.Uint64
}

// NewRouter constructs an empty watch router.
func NewRouter() *Router {
	return &Router{
		subs:    make(map[uint64]*Subscription),
		regions: make(map[uint64]*regionHistory),
	}
}

// Subscribe registers one prefix watch.
func (r *Router) Subscribe(ctx context.Context, req fsmeta.WatchRequest) (fsmeta.WatchSubscription, error) {
	if r == nil {
		return nil, model.ErrInvalidRequest
	}
	var prefix []byte
	var err error
	if len(req.KeyPrefix) > 0 {
		// Runtime watchers resolve public mount/root requests into concrete
		// prefixes but keep req.Mount attached so RetireMount can close stale
		// subscribers when root retires a mount.
		prefix = append([]byte(nil), req.KeyPrefix...)
	} else {
		prefix, err = fsmeta.WatchPrefix(req)
		if err != nil {
			return nil, err
		}
	}
	window := req.BackPressureWindow
	if window == 0 {
		window = defaultWindow
	}
	sub := &Subscription{
		router:  r,
		mount:   req.Mount,
		prefix:  prefix,
		events:  make(chan fsmeta.WatchEvent, window),
		window:  window,
		pending: make(map[fsmeta.WatchCursor]uint32),
	}
	r.mu.Lock()
	replay, ready, err := r.replayLocked(req.ResumeCursor, prefix)
	if err != nil {
		r.mu.Unlock()
		return nil, err
	}
	for _, evt := range replay {
		if !sub.enqueueReplayLocked(evt) {
			r.overflow.Add(1)
			r.dropped.Add(1)
			r.mu.Unlock()
			return nil, model.ErrWatchOverflow
		}
	}
	sub.ready = ready
	r.next++
	sub.id = r.next
	r.subs[sub.id] = sub
	r.mu.Unlock()
	if ctx != nil {
		go func() {
			<-ctx.Done()
			sub.Close()
		}()
	}
	return sub, nil
}

// Publish fans one key event out to matching subscribers. Durable storage
// events are kept for resume replay; visible events are live-only because
// their replay boundary is the later durable segment frontier.
func (r *Router) Publish(evt fsmeta.WatchEvent) {
	if r == nil || len(evt.Key) == 0 {
		return
	}
	r.mu.Lock()
	if eventIsReplayable(evt) {
		id := eventID(evt)
		history := r.regionLocked(evt.Cursor.RegionID)
		if history.remembered(id) {
			r.mu.Unlock()
			return
		}
		history.remember(id, cloneEvent(evt))
	}
	subs := make([]*Subscription, 0, len(r.subs))
	for _, sub := range r.subs {
		subs = append(subs, sub)
	}
	r.mu.Unlock()
	r.published.Add(1)
	for _, sub := range subs {
		if bytes.HasPrefix(evt.Key, sub.prefix) {
			if eventIsReplayable(evt) {
				sub.enqueue(evt)
			} else {
				sub.enqueueLive(evt)
			}
		}
	}
}

func eventIsReplayable(evt fsmeta.WatchEvent) bool {
	return evt.Source != fsmeta.WatchEventSourceRuntimeVisible
}

// OnApply publishes one storage-apply event after the runtime adapter has
// converted it into fsmeta's neutral ApplyEvent shape.
func (r *Router) OnApply(evt fsmeta.ApplyEvent) {
	switch evt.Source {
	case fsmeta.WatchEventSourceCommit, fsmeta.WatchEventSourceResolveLock:
	default:
		return
	}
	for _, key := range evt.Keys {
		r.Publish(fsmeta.WatchEvent{
			Cursor: fsmeta.WatchCursor{
				RegionID: evt.RegionID,
				Term:     evt.Term,
				Index:    evt.Index,
			},
			CommitVersion: evt.CommitVersion,
			Source:        evt.Source,
			Key:           key,
		})
	}
}

// Dropped returns how many matching events were dropped due to slow subscribers.
func (r *Router) Dropped() uint64 {
	if r == nil {
		return 0
	}
	return r.dropped.Load()
}

// Stats returns a point-in-time router metrics snapshot.
func (r *Router) Stats() map[string]any {
	if r == nil {
		return map[string]any{
			"subscribers":     0,
			"events_total":    uint64(0),
			"delivered_total": uint64(0),
			"dropped_total":   uint64(0),
			"overflow_total":  uint64(0),
		}
	}
	r.mu.RLock()
	subscribers := len(r.subs)
	recent := 0
	regions := len(r.regions)
	for _, history := range r.regions {
		recent += history.len
	}
	r.mu.RUnlock()
	return map[string]any{
		"subscribers":     subscribers,
		"regions":         regions,
		"recent_events":   recent,
		"events_total":    r.published.Load(),
		"delivered_total": r.delivered.Load(),
		"dropped_total":   r.dropped.Load(),
		"overflow_total":  r.overflow.Load(),
	}
}

func (r *Router) regionLocked(regionID uint64) *regionHistory {
	history := r.regions[regionID]
	if history == nil {
		history = &regionHistory{
			recent: make(map[eventKey]struct{}),
			ring:   make([]historyEntry, defaultRecentEvents),
		}
		r.regions[regionID] = history
	}
	return history
}

func (r *Router) replayLocked(cursor fsmeta.WatchCursor, prefix []byte) ([]fsmeta.WatchEvent, fsmeta.WatchCursor, error) {
	if cursor.RegionID == 0 {
		return nil, fsmeta.WatchCursor{}, nil
	}
	history := r.regions[cursor.RegionID]
	if history == nil || history.len == 0 {
		return nil, fsmeta.WatchCursor{}, model.ErrWatchCursorExpired
	}
	entries := history.ordered()
	latest := entries[len(entries)-1].event.Cursor
	found := false
	for _, entry := range entries {
		if sameCursor(entry.event.Cursor, cursor) {
			found = true
			break
		}
	}
	if !found {
		if compareCursor(cursor, latest) > 0 {
			return nil, latest, nil
		}
		return nil, latest, model.ErrWatchCursorExpired
	}
	replay := make([]fsmeta.WatchEvent, 0, len(entries))
	for _, entry := range entries {
		if compareCursor(entry.event.Cursor, cursor) > 0 && bytes.HasPrefix(entry.event.Key, prefix) {
			replay = append(replay, cloneEvent(entry.event))
		}
	}
	return replay, latest, nil
}

// RetireMount closes all subscriptions attached to a retired mount.
func (r *Router) RetireMount(mount model.MountID) int {
	if r == nil || mount == "" {
		return 0
	}
	var retired []*Subscription
	r.mu.Lock()
	for id, sub := range r.subs {
		if sub.mount == mount {
			delete(r.subs, id)
			retired = append(retired, sub)
		}
	}
	r.mu.Unlock()
	for _, sub := range retired {
		sub.closeWith(model.ErrMountRetired)
	}
	return len(retired)
}

func (r *Router) unregister(id uint64, sub *Subscription) {
	r.mu.Lock()
	if current := r.subs[id]; current == sub {
		delete(r.subs, id)
	}
	r.mu.Unlock()
}

// Subscription is one router subscriber.
type Subscription struct {
	router *Router
	id     uint64
	mount  model.MountID
	prefix []byte
	events chan fsmeta.WatchEvent
	window uint32
	ready  fsmeta.WatchCursor

	mu          sync.Mutex
	outstanding uint32
	pending     map[fsmeta.WatchCursor]uint32
	closed      bool
	err         error
}

// Events returns the event stream.
func (s *Subscription) Events() <-chan fsmeta.WatchEvent {
	if s == nil {
		return nil
	}
	return s.events
}

// ReadyCursor returns the router frontier after any catch-up replay queued for
// this subscription.
func (s *Subscription) ReadyCursor() fsmeta.WatchCursor {
	if s == nil {
		return fsmeta.WatchCursor{}
	}
	return s.ready
}

// Ack releases outstanding event budget up to cursor within the same region.
func (s *Subscription) Ack(cursor fsmeta.WatchCursor) {
	if s == nil {
		return
	}
	s.mu.Lock()
	var released uint32
	for pending, count := range s.pending {
		if cursorCovers(pending, cursor) {
			released += count
			delete(s.pending, pending)
		}
	}
	if released > s.outstanding {
		s.outstanding = 0
	} else {
		s.outstanding -= released
	}
	s.mu.Unlock()
}

// Err returns the terminal subscription error, if any.
func (s *Subscription) Err() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// Close unregisters the subscription.
func (s *Subscription) Close() {
	s.closeWith(nil)
}

func (s *Subscription) enqueue(evt fsmeta.WatchEvent) {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	if s.outstanding >= s.window {
		s.markClosedLocked(model.ErrWatchOverflow)
		id := s.id
		router := s.router
		s.mu.Unlock()
		if router != nil {
			router.unregister(id, s)
		}
		if s.router != nil {
			s.router.dropped.Add(1)
			s.router.overflow.Add(1)
		}
		return
	}
	s.outstanding++
	s.pending[evt.Cursor]++
	s.mu.Unlock()

	select {
	case s.events <- cloneEvent(evt):
		if s.router != nil {
			s.router.delivered.Add(1)
		}
	default:
		s.closeWith(model.ErrWatchOverflow)
		if s.router != nil {
			s.router.dropped.Add(1)
			s.router.overflow.Add(1)
		}
	}
}

func (s *Subscription) enqueueLive(evt fsmeta.WatchEvent) {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	select {
	case s.events <- cloneEvent(evt):
		if s.router != nil {
			s.router.delivered.Add(1)
		}
	default:
		s.closeWith(model.ErrWatchOverflow)
		if s.router != nil {
			s.router.dropped.Add(1)
			s.router.overflow.Add(1)
		}
	}
}

func (s *Subscription) enqueueReplayLocked(evt fsmeta.WatchEvent) bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	if s.outstanding >= s.window {
		s.markClosedLocked(model.ErrWatchOverflow)
		return false
	}
	s.outstanding++
	s.pending[evt.Cursor]++
	select {
	case s.events <- cloneEvent(evt):
		if s.router != nil {
			s.router.delivered.Add(1)
		}
		return true
	default:
		s.markClosedLocked(model.ErrWatchOverflow)
		return false
	}
}

func (s *Subscription) closeWith(err error) {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.markClosedLocked(err)
	id := s.id
	router := s.router
	s.mu.Unlock()
	if router != nil {
		router.unregister(id, s)
	}
}

func (s *Subscription) markClosedLocked(err error) {
	if s.closed {
		return
	}
	s.closed = true
	s.err = err
	close(s.events)
}

func cloneEvent(evt fsmeta.WatchEvent) fsmeta.WatchEvent {
	evt.Key = append([]byte(nil), evt.Key...)
	return evt
}

type eventKey struct {
	regionID      uint64
	term          uint64
	index         uint64
	source        fsmeta.WatchEventSource
	commitVersion uint64
	key           string
}

func eventID(evt fsmeta.WatchEvent) eventKey {
	return eventKey{
		regionID:      evt.Cursor.RegionID,
		term:          evt.Cursor.Term,
		index:         evt.Cursor.Index,
		source:        evt.Source,
		commitVersion: evt.CommitVersion,
		key:           string(evt.Key),
	}
}

type historyEntry struct {
	id    eventKey
	event fsmeta.WatchEvent
}

type regionHistory struct {
	recent map[eventKey]struct{}
	ring   []historyEntry
	next   int
	len    int
}

func (h *regionHistory) remembered(id eventKey) bool {
	_, ok := h.recent[id]
	return ok
}

func (h *regionHistory) remember(id eventKey, evt fsmeta.WatchEvent) {
	h.recent[id] = struct{}{}
	if h.len < len(h.ring) {
		h.ring[h.next] = historyEntry{id: id, event: evt}
		h.next = (h.next + 1) % len(h.ring)
		h.len++
		return
	}
	old := h.ring[h.next]
	delete(h.recent, old.id)
	h.ring[h.next] = historyEntry{id: id, event: evt}
	h.next = (h.next + 1) % len(h.ring)
}

func (h *regionHistory) ordered() []historyEntry {
	if h == nil || h.len == 0 {
		return nil
	}
	out := make([]historyEntry, 0, h.len)
	start := h.next - h.len
	if start < 0 {
		start += len(h.ring)
	}
	for i := 0; i < h.len; i++ {
		out = append(out, h.ring[(start+i)%len(h.ring)])
	}
	return out
}

func sameCursor(a, b fsmeta.WatchCursor) bool {
	return a.RegionID == b.RegionID && a.Term == b.Term && a.Index == b.Index
}

func compareCursor(a, b fsmeta.WatchCursor) int {
	if a.RegionID != b.RegionID {
		if a.RegionID < b.RegionID {
			return -1
		}
		return 1
	}
	if a.Term != b.Term {
		if a.Term < b.Term {
			return -1
		}
		return 1
	}
	if a.Index != b.Index {
		if a.Index < b.Index {
			return -1
		}
		return 1
	}
	return 0
}

func cursorCovers(pending, ack fsmeta.WatchCursor) bool {
	if pending.RegionID != ack.RegionID || ack.RegionID == 0 {
		return false
	}
	if pending.Term < ack.Term {
		return true
	}
	return pending.Term == ack.Term && pending.Index <= ack.Index
}
