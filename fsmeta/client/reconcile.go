// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"errors"
	"fmt"

	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/feichai0017/NoKV/fsmeta/observe"
)

// DirectoryWatchClient is the narrow client surface needed for directory
// watch recovery. GRPCClient satisfies it.
type DirectoryWatchClient interface {
	ReadDirPlus(context.Context, model.ReadDirRequest) ([]model.DentryAttrPair, error)
	WatchSubtree(context.Context, observe.WatchRequest) (WatchSubscription, error)
}

// WatchReconcileResult is returned by WatchDirectoryWithReconcile.
type WatchReconcileResult struct {
	Subscription WatchSubscription
	// Snapshot is populated only when the server rejected ResumeCursor and
	// the helper had to re-baseline the directory.
	Snapshot []model.DentryAttrPair
	// Reconciled reports whether Snapshot contains a fresh full directory
	// view. Live events may overlap that view and must be applied
	// idempotently by callers.
	Reconciled bool
}

// ReadDirPlusAll scans one directory through ReadDirPlus pagination.
func ReadDirPlusAll(ctx context.Context, cli interface {
	ReadDirPlus(context.Context, model.ReadDirRequest) ([]model.DentryAttrPair, error)
}, req model.ReadDirRequest) ([]model.DentryAttrPair, error) {
	if cli == nil {
		return nil, errDirectoryReaderRequired
	}
	limit := req.Limit
	if limit == 0 {
		limit = model.DefaultReadDirLimit
	}
	if limit > model.MaxReadDirLimit {
		limit = model.MaxReadDirLimit
	}
	req.Limit = limit
	var out []model.DentryAttrPair
	for {
		page, err := cli.ReadDirPlus(ctx, req)
		if err != nil {
			return nil, err
		}
		out = append(out, page...)
		if uint32(len(page)) < limit {
			return out, nil
		}
		req.StartAfter = page[len(page)-1].Dentry.Name
	}
}

// WatchDirectoryWithReconcile opens a WatchSubtree stream for one direct
// directory. If the resume cursor has expired, it opens a fresh stream first
// and then returns a full ReadDirPlus baseline for the caller to reconcile.
//
// Ordering rule: the fresh watch is established before the full directory
// read. Events that race with the baseline can therefore be duplicated, but
// they are not silently lost; callers should treat the returned Snapshot as
// the new base state and apply subsequent events idempotently.
func WatchDirectoryWithReconcile(ctx context.Context, cli DirectoryWatchClient, watchReq observe.WatchRequest, readReq model.ReadDirRequest) (WatchReconcileResult, error) {
	if cli == nil {
		return WatchReconcileResult{}, errWatchClientRequired
	}
	if len(watchReq.KeyPrefix) != 0 || watchReq.DescendRecursively || watchReq.Mount != readReq.Mount || watchReq.RootInode != readReq.Parent {
		return WatchReconcileResult{}, fmt.Errorf("%w: watch/read directory mismatch", model.ErrInvalidRequest)
	}
	if readReq.StartAfter != "" || readReq.SnapshotVersion != 0 {
		return WatchReconcileResult{}, fmt.Errorf("%w: watch reconcile requires a fresh full-directory read", model.ErrInvalidRequest)
	}
	sub, err := cli.WatchSubtree(ctx, watchReq)
	if err == nil {
		return WatchReconcileResult{Subscription: sub}, nil
	}
	if !errors.Is(err, model.ErrWatchCursorExpired) {
		return WatchReconcileResult{}, err
	}

	freshReq := watchReq
	freshReq.ResumeCursor = observe.WatchCursor{}
	sub, err = cli.WatchSubtree(ctx, freshReq)
	if err != nil {
		return WatchReconcileResult{}, err
	}
	snapshot, err := ReadDirPlusAll(ctx, cli, readReq)
	if err != nil {
		_ = sub.Close()
		return WatchReconcileResult{}, err
	}
	return WatchReconcileResult{
		Subscription: sub,
		Snapshot:     snapshot,
		Reconciled:   true,
	}, nil
}
