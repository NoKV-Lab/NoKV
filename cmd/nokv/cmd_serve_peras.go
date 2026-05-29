// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"strings"

	"github.com/feichai0017/NoKV/storage/wal"
)

func parseSegmentWitnessWALPolicy(value string) (wal.DurabilityPolicy, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "fsync-batched", "fsync_batched", "batched":
		return wal.DurabilityFsyncBatched, nil
	case "fsync":
		return wal.DurabilityFsync, nil
	case "flushed":
		return wal.DurabilityFlushed, nil
	case "buffered":
		return wal.DurabilityBuffered, nil
	default:
		return 0, fmt.Errorf("invalid peras witness WAL sync policy %q", value)
	}
}
