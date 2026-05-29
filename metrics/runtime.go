// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package metrics

import "sync/atomic"

// TablePrefetchSnapshot captures iterator block-prefetch scheduling counters.
type TablePrefetchSnapshot struct {
	Launched  uint64 `json:"launched"`
	Aborted   uint64 `json:"aborted"`
	Completed uint64 `json:"completed"`
}

var (
	tablePrefetchLaunched  atomic.Uint64
	tablePrefetchAborted   atomic.Uint64
	tablePrefetchCompleted atomic.Uint64
)

// RecordTablePrefetchLaunched records one successfully queued prefetch block.
func RecordTablePrefetchLaunched() {
	tablePrefetchLaunched.Add(1)
}

// RecordTablePrefetchAborted records one prefetch that was skipped or failed.
func RecordTablePrefetchAborted() {
	tablePrefetchAborted.Add(1)
}

// RecordTablePrefetchCompleted records one prefetch that loaded a block.
func RecordTablePrefetchCompleted() {
	tablePrefetchCompleted.Add(1)
}

// TablePrefetchStats returns process-wide iterator prefetch counters.
func TablePrefetchStats() TablePrefetchSnapshot {
	return TablePrefetchSnapshot{
		Launched:  tablePrefetchLaunched.Load(),
		Aborted:   tablePrefetchAborted.Load(),
		Completed: tablePrefetchCompleted.Load(),
	}
}
