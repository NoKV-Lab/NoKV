// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"context"
	"math"

	"github.com/feichai0017/NoKV/fsmeta/backend"
	"github.com/feichai0017/NoKV/fsmeta/model"
)

// QuotaChange describes one logical quota delta. Scope 0 means the change only
// affects the mount-wide subject; non-zero scopes also affect that direct
// accounting scope.
type QuotaChange struct {
	Mount      model.MountID
	MountKeyID model.MountKeyID
	Scope      model.InodeID
	Bytes      int64
	Inodes     int64
}

// QuotaResolver resolves rooted quota fences and plans usage-counter mutations
// that must be committed in the same transaction as the metadata mutation.
type QuotaResolver interface {
	ReserveQuota(context.Context, backend.Store, []QuotaChange, uint64) ([]*backend.Mutation, error)
}

// QuotaUsageResolver lets a runtime derive usage without storing quota counter
// keys in the write transaction. Runtimes that do not implement it keep the
// persisted counter-key behavior in GetQuotaUsage.
type QuotaUsageResolver interface {
	ReadQuotaUsage(context.Context, backend.Store, model.MountIdentity, model.InodeID, uint64) (model.UsageRecord, bool, error)
}

func inodeSizeDelta(size uint64) int64 {
	if size > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(size)
}

func inodeSizeChange(oldSize, newSize uint64) int64 {
	if newSize >= oldSize {
		return inodeSizeDelta(newSize - oldSize)
	}
	return -inodeSizeDelta(oldSize - newSize)
}
