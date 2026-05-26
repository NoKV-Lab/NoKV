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
	fsmetaexec "github.com/feichai0017/NoKV/fsmeta/exec"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/stretchr/testify/require"
)

type scriptInodeAllocator struct {
	mu   sync.Mutex
	next model.InodeID
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

func (a *scriptInodeAllocator) AllocateCreateInode(ctx context.Context, _ model.MountIdentity, _ model.InodeID, _ string) (model.InodeID, error) {
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
			state := NewModel("vol")
			runner := newVersionedRunner()
			ops := GenerateScript(seed, steps)
			executor, err := fsmetaexec.New(runner,
				fsmetaexec.WithMountResolver(contractMountResolver{}),
				fsmetaexec.WithInodeAllocator(newScriptInodeAllocator(ops)),
				fsmetaexec.WithClock(func() time.Time {
					return time.Unix(0, state.NowUnixNs)
				}),
			)
			require.NoError(t, err)

			err = RunConcurrentBatches(context.Background(), executor, state, ops, batchSize, HistoryOptions{})
			require.NoError(t, err, "seed=%d steps=%d batch=%d", seed, steps, batchSize)
		})
	}
}

func TestRunConcurrentBatchesHonorsIndeterminateErrorsWithBatchOne(t *testing.T) {
	state := NewModel("vol")
	err := RunConcurrentBatches(context.Background(), unavailableCreateExecutor{}, state, []Operation{{
		Kind:   OpCreate,
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "alpha",
		Inode:  10,
		Type:   model.InodeTypeFile,
	}}, 1, HistoryOptions{AllowIndeterminateErrors: true})
	require.NoError(t, err)
}

func TestIndeterminateHistoryErrorExcludesAborted(t *testing.T) {
	require.False(t, isIndeterminateHistoryError(nokverrors.New(nokverrors.KindAborted, "client canceled")))
	require.True(t, isIndeterminateHistoryError(nokverrors.New(nokverrors.KindRetryExhausted, "retry budget exhausted")))
}

type unavailableCreateExecutor struct{}

func (unavailableCreateExecutor) Create(context.Context, model.CreateRequest) (model.CreateResult, error) {
	return model.CreateResult{}, nokverrors.New(nokverrors.KindRetryExhausted, "store unavailable after retry")
}

func (unavailableCreateExecutor) UpdateInode(context.Context, model.UpdateInodeRequest) (model.InodeRecord, error) {
	return model.InodeRecord{}, model.ErrInvalidRequest
}

func (unavailableCreateExecutor) Lookup(context.Context, model.LookupRequest) (model.DentryRecord, error) {
	return model.DentryRecord{}, model.ErrInvalidRequest
}

func (unavailableCreateExecutor) ReadDirPlus(context.Context, model.ReadDirRequest) ([]model.DentryAttrPair, error) {
	return nil, model.ErrInvalidRequest
}

func (unavailableCreateExecutor) SnapshotSubtree(context.Context, model.SnapshotSubtreeRequest) (model.SnapshotSubtreeToken, error) {
	return model.SnapshotSubtreeToken{}, model.ErrInvalidRequest
}

func (unavailableCreateExecutor) Rename(context.Context, model.RenameRequest) error {
	return model.ErrInvalidRequest
}

func (unavailableCreateExecutor) RenameReplace(context.Context, model.RenameReplaceRequest) (model.RenameReplaceResult, error) {
	return model.RenameReplaceResult{}, model.ErrInvalidRequest
}

func (unavailableCreateExecutor) RenameSubtree(context.Context, model.RenameSubtreeRequest) error {
	return model.ErrInvalidRequest
}

func (unavailableCreateExecutor) Link(context.Context, model.LinkRequest) error {
	return model.ErrInvalidRequest
}

func (unavailableCreateExecutor) Unlink(context.Context, model.UnlinkRequest) error {
	return model.ErrInvalidRequest
}

func (unavailableCreateExecutor) Remove(context.Context, model.RemoveRequest) (model.RemoveResult, error) {
	return model.RemoveResult{}, model.ErrInvalidRequest
}

func (unavailableCreateExecutor) OpenWriteSession(context.Context, model.OpenWriteSessionRequest) (model.SessionRecord, error) {
	return model.SessionRecord{}, model.ErrInvalidRequest
}

func (unavailableCreateExecutor) HeartbeatWriteSession(context.Context, model.HeartbeatWriteSessionRequest) (model.SessionRecord, error) {
	return model.SessionRecord{}, model.ErrInvalidRequest
}

func (unavailableCreateExecutor) CloseWriteSession(context.Context, model.CloseWriteSessionRequest) error {
	return model.ErrInvalidRequest
}

func (unavailableCreateExecutor) ExpireWriteSessions(context.Context, model.ExpireWriteSessionsRequest) (model.ExpireWriteSessionsResult, error) {
	return model.ExpireWriteSessionsResult{}, model.ErrInvalidRequest
}
