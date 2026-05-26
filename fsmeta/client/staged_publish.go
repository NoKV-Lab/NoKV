// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/feichai0017/NoKV/fsmeta/model"
)

const stagedPublishCleanupTimeout = 5 * time.Second

// StagedPublishClient is the narrow namespace surface needed for staged
// publish. GRPCClient satisfies it.
type StagedPublishClient interface {
	Create(context.Context, model.CreateRequest) (model.CreateResult, error)
	Rename(context.Context, model.RenameRequest) error
	Unlink(context.Context, model.UnlinkRequest) error
}

// StagedPublishRequest describes one namespace-only stage -> commit publish.
//
// The helper creates (StageParent, StageName), lets the caller prepare any
// external body/object state, then atomically renames the staged entry to
// (FinalParent, FinalName). NoKV does not write or validate the body itself;
// callers typically store body references inside Inode.OpaqueAttrs.
type StagedPublishRequest struct {
	Mount       model.MountID
	StageParent model.InodeID
	StageName   string
	FinalParent model.InodeID
	FinalName   string
	Attrs       model.CreateAttrs
}

// PrepareStagedFunc is called after the staged namespace entry is created and
// before the final rename. The callback is where a caller writes external body
// data, uploads an object, or validates a content-addressed reference.
type PrepareStagedFunc func(context.Context, model.CreateResult) error

// PublishStagedNamespaceEntry implements the stage -> commit namespace pattern.
//
// Failure semantics:
//   - create failure: no callback is called;
//   - prepare failure: the staged entry is best-effort unlinked before
//     returning the original error, joined with cleanup failure if any;
//   - final rename failure: the staged entry is intentionally left in place so
//     the caller can inspect or retry without losing prepared external state.
func PublishStagedNamespaceEntry(ctx context.Context, cli StagedPublishClient, req StagedPublishRequest, prepare PrepareStagedFunc) error {
	if cli == nil {
		return errStagedPublishClientRequired
	}
	create := req.createRequest()
	if err := model.ValidateCreateRequest(create); err != nil {
		return err
	}
	rename := req.renameRequest()
	if err := model.ValidateRenameRequest(rename); err != nil {
		return err
	}

	result, err := cli.Create(ctx, create)
	if err != nil {
		return err
	}
	if prepare != nil {
		if err := prepare(ctx, result); err != nil {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), stagedPublishCleanupTimeout)
			defer cancel()
			if cleanupErr := cli.Unlink(cleanupCtx, model.UnlinkRequest{
				Mount:  req.Mount,
				Parent: req.StageParent,
				Name:   req.StageName,
			}); cleanupErr != nil {
				return errors.Join(err, fmt.Errorf("cleanup staged namespace entry: %w", cleanupErr))
			}
			return err
		}
	}
	return cli.Rename(ctx, rename)
}

func (r StagedPublishRequest) createRequest() model.CreateRequest {
	return model.CreateRequest{
		Mount:  r.Mount,
		Parent: r.StageParent,
		Name:   r.StageName,
		Attrs:  r.Attrs,
	}
}

func (r StagedPublishRequest) renameRequest() model.RenameRequest {
	return model.RenameRequest{
		Mount:      r.Mount,
		FromParent: r.StageParent,
		FromName:   r.StageName,
		ToParent:   r.FinalParent,
		ToName:     r.FinalName,
	}
}
