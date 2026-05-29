// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package ycsb

import (
	"fmt"
	"path/filepath"
	"sort"
	"sync"
)

// ycsbEngine represents a storage engine implementation that can participate
// in the YCSB benchmark. The methods are designed to be concurrency-safe;
// the benchmark runner will call them from multiple goroutines.
type ycsbEngine interface {
	Name() string
	Open(clean bool) error
	Close() error
	// Read copies the value of key into dst (if possible) and returns the
	// resulting slice (may alias dst). Callers reuse dst to avoid allocations.
	Read(key []byte, dst []byte) ([]byte, error)
	Insert(key, value []byte) error
	Update(key, value []byte) error
	Scan(startKey []byte, count int) (int, error)
}

// ycsbEngineOptions captures configuration that is fed into each engine
// implementation. It focuses on the knobs that influence performance so the
// benchmark runner can keep the engines aligned.
type ycsbEngineOptions struct {
	BaseDir          string
	ValueSize        int
	ValueThreshold   int
	SyncWrites       bool
	BlockCacheMB     int
	NoKVIndexCacheMB int
	MemtableMB       int
	SSTableMB        int
	VlogFileMB       int

	// Badger specific cache sizes (in MB).
	BadgerBlockCacheMB int
	BadgerIndexCacheMB int
	BadgerCompression  string

	// RocksDB installation prefix (contains include/ and lib/).
	RocksDBCompression string
	PebbleCompression  string
}

// engineDir derives an engine-specific work directory rooted at BaseDir.
func (o ycsbEngineOptions) engineDir(engine string) string {
	return filepath.Join(o.BaseDir, fmt.Sprintf("%s_ycsb", engine))
}

// latencyRecorder accumulates latency samples (nanoseconds) and can compute
// percentiles. It is safe for concurrent use.
type latencyRecorder struct {
	mu      sync.Mutex
	samples []int64
}

func newLatencyRecorder(capacity int) *latencyRecorder {
	return &latencyRecorder{
		samples: make([]int64, 0, capacity),
	}
}

func (r *latencyRecorder) Record(ns int64) {
	r.mu.Lock()
	r.samples = append(r.samples, ns)
	r.mu.Unlock()
}

func (r *latencyRecorder) Percentile(p float64) float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.samples) == 0 || p <= 0 {
		return 0
	}
	if p >= 100 {
		p = 100
	}
	sorted := make([]int64, len(r.samples))
	copy(sorted, r.samples)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(float64(len(sorted)-1) * (p / 100.0))
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return float64(sorted[idx])
}

func (r *latencyRecorder) Samples() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]int64, len(r.samples))
	copy(out, r.samples)
	return out
}

type intRecorder struct {
	mu      sync.Mutex
	samples []int
}

func newIntRecorder(capacity int) *intRecorder {
	return &intRecorder{
		samples: make([]int, 0, capacity),
	}
}

func (r *intRecorder) Record(v int) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.samples = append(r.samples, v)
	r.mu.Unlock()
}

func (r *intRecorder) Percentile(p float64) float64 {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.samples) == 0 || p <= 0 {
		return 0
	}
	if p >= 100 {
		p = 100
	}
	sorted := make([]int, len(r.samples))
	copy(sorted, r.samples)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(float64(len(sorted)-1) * (p / 100.0))
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return float64(sorted[idx])
}

func (r *intRecorder) Average() float64 {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.samples) == 0 {
		return 0
	}
	var sum int64
	for _, v := range r.samples {
		sum += int64(v)
	}
	return float64(sum) / float64(len(r.samples))
}
