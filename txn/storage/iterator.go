// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package storage

import "github.com/feichai0017/NoKV/utils"

// Iterator abstracts ordered iteration over internal storage entries.
type Iterator interface {
	Next()
	Valid() bool
	Rewind()
	Item() Item
	Close() error
	Seek(key []byte)
}

// Item is the current entry view returned by an Iterator.
type Item interface {
	Entry() *Entry
}

// Options controls ordered scans over canonical internal keys.
type Options struct {
	Prefix []byte
	IsAsc  bool
	// AccessPattern lets storage implementations tune sequential or random IO.
	AccessPattern utils.AccessPattern
	// PrefetchBlocks controls eager read-ahead when the backend supports it.
	PrefetchBlocks int
	// PrefetchWorkers optionally overrides backend read-ahead worker count.
	PrefetchWorkers int
	// PrefetchAdaptive enables dynamic prefetch tuning where supported.
	PrefetchAdaptive bool
	// MetricTag tags iterator metrics for observability.
	MetricTag string
	// LowerBound is an inclusive lower bound in internal-key order.
	LowerBound []byte
	// UpperBound is an exclusive upper bound in internal-key order.
	UpperBound []byte
}
