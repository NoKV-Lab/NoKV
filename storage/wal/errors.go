// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package wal

import "errors"

var (
	// ErrPartialRecord indicates a WAL record ended prematurely (typically due to EOF/corruption).
	ErrPartialRecord = errors.New("wal: partial record")
	// ErrEmptyRecord indicates a WAL record header advertised zero payload length.
	ErrEmptyRecord = errors.New("wal: empty record")
	// ErrSegmentRetained indicates a retention participant still needs the WAL segment.
	ErrSegmentRetained = errors.New("wal: segment retained")
	// ErrWALBackpressure indicates the WAL reached its configured hard cap.
	ErrWALBackpressure = errors.New("wal: backpressure")

	errStaleCatalog = errors.New("wal: stale catalog")
)
