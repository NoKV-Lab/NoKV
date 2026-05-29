// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package mvcc

import (
	"context"
	"fmt"

	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	entrykv "github.com/feichai0017/NoKV/txn/storage"
)

// MaintenanceProposer submits MVCC maintenance entries through the replicated
// raft command path. Implementations must treat each entry key as a user-key
// scoped mutation after decoding its internal key.
//
// A call may span multiple regions. Implementations are allowed to commit a
// subset of region-local batches before returning an error; MVCC maintenance is
// intentionally at-least-once and idempotent, not cross-region atomic.
type MaintenanceProposer interface {
	ProposeMVCCMaintenance(context.Context, []*entrykv.Entry) (entries, writeDeletes, defaultDeletes uint64, err error)
}

// LockResolver submits semantic transaction-resolution commands through the
// authoritative region for each key. Implementations must preserve the normal
// Percolator apply path instead of translating locks into raw tombstones.
type LockResolver interface {
	CheckTxnStatus(ctx context.Context, primary []byte, lockTs, currentTs, currentTime uint64) (*kvrpcpb.CheckTxnStatusResponse, error)
	ResolveLocks(ctx context.Context, startVersion, commitVersion uint64, keys [][]byte) (uint64, error)
}

type maintenanceSubmitResult struct {
	entries        uint64
	writeDeletes   uint64
	defaultDeletes uint64
}

func proposeMaintenanceEntries(ctx context.Context, proposer MaintenanceProposer, entries []*entrykv.Entry) (maintenanceSubmitResult, error) {
	if len(entries) == 0 {
		return maintenanceSubmitResult{}, nil
	}
	if proposer == nil {
		return maintenanceSubmitResult{}, errNilMaintenanceProposer
	}
	applied, writeDeletes, defaultDeletes, err := proposer.ProposeMVCCMaintenance(ctx, entries)
	result := maintenanceSubmitResult{
		entries:        applied,
		writeDeletes:   writeDeletes,
		defaultDeletes: defaultDeletes,
	}
	if err != nil {
		return result, err
	}
	if writeDeletes+defaultDeletes != applied {
		return result, fmt.Errorf("raftstore/mvcc: maintenance proposer applied %d entries but reported %d write and %d default deletes", applied, writeDeletes, defaultDeletes)
	}
	if applied != uint64(len(entries)) {
		return result, fmt.Errorf("raftstore/mvcc: maintenance proposer applied %d entries, expected %d", applied, len(entries))
	}
	return result, nil
}

func checkTxnStatus(ctx context.Context, resolver LockResolver, primary []byte, lockTs, currentTs, currentTime uint64) (*kvrpcpb.CheckTxnStatusResponse, error) {
	if resolver == nil {
		return nil, errNilLockResolver
	}
	return resolver.CheckTxnStatus(ctx, primary, lockTs, currentTs, currentTime)
}

func proposeResolveLocks(ctx context.Context, resolver LockResolver, startVersion, commitVersion uint64, keys [][]byte) (uint64, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	if resolver == nil {
		return 0, errNilLockResolver
	}
	return resolver.ResolveLocks(ctx, startVersion, commitVersion, keys)
}
