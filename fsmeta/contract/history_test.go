// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package contract

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/fsmeta"
	fsmetaexec "github.com/feichai0017/NoKV/fsmeta/exec"
	"github.com/stretchr/testify/require"
)

type scriptInodeAllocator struct {
	mu   sync.Mutex
	next fsmeta.InodeID
}

func newScriptInodeAllocator(ops []Operation) *scriptInodeAllocator {
	alloc := &scriptInodeAllocator{next: 1_000_000}
	for _, op := range ops {
		if op.Kind != OpCreate {
			continue
		}
		if op.Inode >= alloc.next {
			alloc.next = op.Inode + 1
		}
	}
	return alloc
}

func (a *scriptInodeAllocator) AllocateCreateInode(ctx context.Context, _ fsmeta.MountIdentity, _ fsmeta.InodeID, _ string) (fsmeta.InodeID, error) {
	if inode, ok := plannedCreateInode(ctx); ok {
		return inode, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	id := a.next
	a.next++
	return id, nil
}

func TestFSMetaExecutorConcurrentHistoryContract(t *testing.T) {
	seeds := envInt("NOKV_CONTRACT_HISTORY_SEEDS", 8)
	steps := envInt("NOKV_CONTRACT_HISTORY_STEPS", 48)
	batchSize := envInt("NOKV_CONTRACT_HISTORY_BATCH", 3)
	for seed := int64(1); seed <= int64(seeds); seed++ {
		t.Run(fmt.Sprintf("seed_%03d", seed), func(t *testing.T) {
			model := NewModel("vol")
			runner := newVersionedRunner()
			ops := GenerateScript(seed, steps)
			executor, err := fsmetaexec.New(runner,
				fsmetaexec.WithMountResolver(contractMountResolver{}),
				fsmetaexec.WithInodeAllocator(newScriptInodeAllocator(ops)),
				fsmetaexec.WithClock(func() time.Time {
					return time.Unix(0, model.NowUnixNs)
				}),
			)
			require.NoError(t, err)

			err = RunConcurrentBatches(context.Background(), executor, model, ops, batchSize, HistoryOptions{})
			require.NoError(t, err, "seed=%d steps=%d batch=%d", seed, steps, batchSize)
		})
	}
}

func TestRunConcurrentBatchesHonorsIndeterminateErrorsWithBatchOne(t *testing.T) {
	model := NewModel("vol")
	err := RunConcurrentBatches(context.Background(), unavailableCreateExecutor{}, model, []Operation{{
		Kind:   OpCreate,
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "alpha",
		Inode:  10,
		Type:   fsmeta.InodeTypeFile,
	}}, 1, HistoryOptions{AllowIndeterminateErrors: true})
	require.NoError(t, err)
}

func TestIndeterminateHistoryErrorExcludesAborted(t *testing.T) {
	require.False(t, isIndeterminateHistoryError(nokverrors.New(nokverrors.KindAborted, "client canceled")))
	require.True(t, isIndeterminateHistoryError(nokverrors.New(nokverrors.KindRetryExhausted, "retry budget exhausted")))
}

type unavailableCreateExecutor struct{}

func (unavailableCreateExecutor) Create(context.Context, fsmeta.CreateRequest) (fsmeta.CreateResult, error) {
	return fsmeta.CreateResult{}, nokverrors.New(nokverrors.KindRetryExhausted, "store unavailable after retry")
}

func (unavailableCreateExecutor) UpdateInode(context.Context, fsmeta.UpdateInodeRequest) (fsmeta.InodeRecord, error) {
	return fsmeta.InodeRecord{}, fsmeta.ErrInvalidRequest
}

func (unavailableCreateExecutor) Lookup(context.Context, fsmeta.LookupRequest) (fsmeta.DentryRecord, error) {
	return fsmeta.DentryRecord{}, fsmeta.ErrInvalidRequest
}

func (unavailableCreateExecutor) ReadDirPlus(context.Context, fsmeta.ReadDirRequest) ([]fsmeta.DentryAttrPair, error) {
	return nil, fsmeta.ErrInvalidRequest
}

func (unavailableCreateExecutor) SnapshotSubtree(context.Context, fsmeta.SnapshotSubtreeRequest) (fsmeta.SnapshotSubtreeToken, error) {
	return fsmeta.SnapshotSubtreeToken{}, fsmeta.ErrInvalidRequest
}

func (unavailableCreateExecutor) Rename(context.Context, fsmeta.RenameRequest) error {
	return fsmeta.ErrInvalidRequest
}

func (unavailableCreateExecutor) RenameSubtree(context.Context, fsmeta.RenameSubtreeRequest) error {
	return fsmeta.ErrInvalidRequest
}

func (unavailableCreateExecutor) Link(context.Context, fsmeta.LinkRequest) error {
	return fsmeta.ErrInvalidRequest
}

func (unavailableCreateExecutor) Unlink(context.Context, fsmeta.UnlinkRequest) error {
	return fsmeta.ErrInvalidRequest
}

func (unavailableCreateExecutor) Remove(context.Context, fsmeta.RemoveRequest) error {
	return fsmeta.ErrInvalidRequest
}

func (unavailableCreateExecutor) OpenWriteSession(context.Context, fsmeta.OpenWriteSessionRequest) (fsmeta.SessionRecord, error) {
	return fsmeta.SessionRecord{}, fsmeta.ErrInvalidRequest
}

func (unavailableCreateExecutor) HeartbeatWriteSession(context.Context, fsmeta.HeartbeatWriteSessionRequest) (fsmeta.SessionRecord, error) {
	return fsmeta.SessionRecord{}, fsmeta.ErrInvalidRequest
}

func (unavailableCreateExecutor) CloseWriteSession(context.Context, fsmeta.CloseWriteSessionRequest) error {
	return fsmeta.ErrInvalidRequest
}

func (unavailableCreateExecutor) ExpireWriteSessions(context.Context, fsmeta.ExpireWriteSessionsRequest) (fsmeta.ExpireWriteSessionsResult, error) {
	return fsmeta.ExpireWriteSessionsResult{}, fsmeta.ErrInvalidRequest
}
