// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"io"
	"time"

	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
)

// Format names the concrete encoding used by a region snapshot payload.
type Format string

const (
	// FormatSST is implemented by raftstore/snapshot/sst using external SST
	// export and ingest hooks.
	FormatSST Format = "sst"
)

// Descriptor is the backend-neutral identity of one region snapshot.
type Descriptor struct {
	Format     Format
	Region     localmeta.RegionMeta
	EntryCount uint64
	CreatedAt  time.Time
}

// ImportResult is the backend-neutral result of staging one region snapshot.
// Concrete implementations may attach rollback state privately.
type ImportResult struct {
	Descriptor Descriptor
	Rollback   func() error
}

// RegionStore streams region snapshots without exposing how a storage backend
// materializes the payload.
type RegionStore interface {
	ExportRegionSnapshotTo(w io.Writer, region localmeta.RegionMeta) (Descriptor, error)
	ImportRegionSnapshotFrom(r io.Reader) (ImportResult, error)
}
