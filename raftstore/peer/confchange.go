// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peer

import (
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

// ConfChangeEvent captures context for a configuration change entry that has
// been committed and applied by the raft peer.
type ConfChangeEvent struct {
	Peer       *Peer
	RegionMeta *localmeta.RegionMeta
	ConfChange raftpb.ConfChangeV2
	Index      uint64
	Term       uint64
}

// ConfChangeHandler is invoked whenever a configuration change entry is
// applied. Returning an error aborts Ready processing so callers can surface
// local catalog persistence failures to the raftstore.
type ConfChangeHandler func(ConfChangeEvent) error
