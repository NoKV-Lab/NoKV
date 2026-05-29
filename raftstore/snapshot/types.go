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
	// FormatEntries stores canonical NoKV MVCC internal entries. It is the
	// built-in raft snapshot payload format and does not depend on a physical
	// LSM/SST implementation.
	FormatEntries Format = "entries"
)

// Descriptor is the backend-neutral identity of one region snapshot.
type Descriptor struct {
	Format     Format
	Region     localmeta.RegionMeta
	EntryCount uint64
	CreatedAt  time.Time
}

// ImportResult reports one imported region snapshot.
type ImportResult struct {
	Descriptor Descriptor
}

// Store exports and imports raftstore-internal region snapshot payloads.
type Store interface {
	ExportSnapshot(region localmeta.RegionMeta) ([]byte, error)
	ImportSnapshot(payload []byte) (*ImportResult, error)
	ExportSnapshotTo(w io.Writer, region localmeta.RegionMeta) (Descriptor, error)
	ImportSnapshotFrom(r io.Reader) (*ImportResult, error)
}
