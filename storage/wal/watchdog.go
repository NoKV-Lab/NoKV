// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package wal

import (
	"errors"
	"log/slog"
	"math"
	"os"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/metrics"
	"github.com/feichai0017/NoKV/utils"
)

const (
	defaultWatchdogInterval     = 15 * time.Second
	defaultWatchdogMinRemovable = 1
	defaultWatchdogMaxBatch     = 4
)

// WatchdogConfig controls WAL watchdog behavior.
type WatchdogConfig struct {
	Manager      *Manager
	Interval     time.Duration
	MinRemovable int
	MaxBatch     int
	WarnRatio    float64
	WarnSegments int64
}

// resolveDefaults resolves constructor-boundary defaults for the watchdog config.
func (cfg WatchdogConfig) resolveDefaults() WatchdogConfig {
	if cfg.Interval <= 0 {
		cfg.Interval = defaultWatchdogInterval
	}
	if cfg.MinRemovable <= 0 {
		cfg.MinRemovable = defaultWatchdogMinRemovable
	}
	if cfg.MaxBatch <= 0 {
		cfg.MaxBatch = defaultWatchdogMaxBatch
	}
	return cfg
}

// WatchdogSnapshot captures WAL watchdog state for reporting.
type WatchdogSnapshot struct {
	AutoRuns          uint64
	SegmentsRemoved   uint64
	LastAutoUnix      int64
	LastTickUnix      int64
	RemovableSegments int
	TypedRatio        float64
	Warning           bool
	WarningReason     string
}

// Watchdog periodically inspects WAL backlog and can remove stale segments.
type Watchdog struct {
	manager      *Manager
	interval     time.Duration
	minRemovable int
	maxBatch     int
	warnRatio    float64
	warnSegments int64
	autoEnabled  bool
	closer       *utils.Closer

	autoRuns        atomic.Uint64
	segmentsRemoved atomic.Uint64
	lastAutoUnix    atomic.Int64
	lastTickUnix    atomic.Int64
	removableCount  atomic.Int64
	lastRatioBits   atomic.Uint64
	warnActive      atomic.Bool
	warnReason      atomic.Value
}

// NewWatchdog constructs a watchdog from the provided configuration.
func NewWatchdog(cfg WatchdogConfig) *Watchdog {
	if cfg.Manager == nil {
		return nil
	}
	cfg = cfg.resolveDefaults()
	w := &Watchdog{
		manager:      cfg.Manager,
		interval:     cfg.Interval,
		minRemovable: cfg.MinRemovable,
		maxBatch:     cfg.MaxBatch,
		warnRatio:    cfg.WarnRatio,
		warnSegments: cfg.WarnSegments,
		autoEnabled:  cfg.MinRemovable > 0 && cfg.MaxBatch > 0,
		closer:       utils.NewCloser(),
	}
	w.warnReason.Store("")
	return w
}

// Start launches the background watchdog loop.
func (w *Watchdog) Start() {
	if w == nil {
		return
	}
	w.closer.Add(1)
	go w.run()
}

// Stop terminates the background watchdog loop.
func (w *Watchdog) Stop() {
	if w == nil {
		return
	}
	w.closer.Close()
}

// RunOnce executes a single watchdog inspection cycle.
func (w *Watchdog) RunOnce() {
	if w == nil {
		return
	}
	w.observe()
}

// Snapshot returns the current watchdog snapshot.
func (w *Watchdog) Snapshot() WatchdogSnapshot {
	if w == nil {
		return WatchdogSnapshot{}
	}
	ratioBits := w.lastRatioBits.Load()
	snap := WatchdogSnapshot{
		AutoRuns:          w.autoRuns.Load(),
		SegmentsRemoved:   w.segmentsRemoved.Load(),
		LastAutoUnix:      w.lastAutoUnix.Load(),
		LastTickUnix:      w.lastTickUnix.Load(),
		RemovableSegments: int(w.removableCount.Load()),
		TypedRatio:        math.Float64frombits(ratioBits),
		Warning:           w.warnActive.Load(),
	}
	if reason, ok := w.warnReason.Load().(string); ok {
		snap.WarningReason = reason
	}
	return snap
}

func (w *Watchdog) run() {
	defer w.closer.Done()

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	w.observe()

	for {
		select {
		case <-ticker.C:
			w.observe()
		case <-w.closer.Closed():
			return
		}
	}
}

func (w *Watchdog) observe() {
	if w == nil || w.manager == nil {
		return
	}
	w.lastTickUnix.Store(time.Now().Unix())

	wmetrics := w.manager.Metrics()
	segmentMetrics := w.manager.SegmentMetrics()
	analysis := metrics.AnalyzeWALBacklog(wmetrics, segmentMetrics)
	removable := w.filterRemovable(analysis.RemovableSegments)

	w.removableCount.Store(int64(len(removable)))
	w.lastRatioBits.Store(math.Float64bits(analysis.TypedRecordRatio))

	warning, reason := metrics.WALTypedWarning(analysis.TypedRecordRatio, analysis.SegmentsWithRaft, w.warnRatio, w.warnSegments)
	w.warnActive.Store(warning)
	if warning {
		w.warnReason.Store(reason)
	} else {
		w.warnReason.Store("")
	}

	if !w.autoEnabled {
		return
	}
	if len(removable) < w.minRemovable {
		return
	}

	batch := removable
	if len(batch) > w.maxBatch {
		batch = batch[:w.maxBatch]
	}

	removed := 0
	for _, id := range batch {
		if err := w.manager.RemoveSegment(id); err != nil {
			if os.IsNotExist(err) || errors.Is(err, ErrSegmentRetained) {
				continue
			}
			slog.Default().Warn("wal watchdog remove segment failed", "segment", id, "err", err)
			continue
		}
		removed++
	}
	if removed == 0 {
		return
	}
	w.autoRuns.Add(1)
	w.segmentsRemoved.Add(uint64(removed))
	w.lastAutoUnix.Store(time.Now().Unix())
}

func (w *Watchdog) filterRemovable(candidates []uint32) []uint32 {
	if len(candidates) == 0 {
		return nil
	}
	out := make([]uint32, 0, len(candidates))
	for _, id := range candidates {
		if w.manager.CanRemoveSegment(id) {
			out = append(out, id)
		}
	}
	return out
}
