// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"context"

	"github.com/feichai0017/NoKV/fsmeta/backend"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
)

// GetQuotaUsage returns the current persisted usage counter for one quota
// subject. Missing usage keys represent zero usage.
func (e *Executor) GetQuotaUsage(ctx context.Context, req model.QuotaUsageRequest) (model.UsageRecord, error) {
	if req.Mount == "" {
		return model.UsageRecord{}, model.ErrInvalidMountID
	}
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return model.UsageRecord{}, err
	}
	version, err := e.reserveReadVersion(ctx)
	if err != nil {
		return model.UsageRecord{}, err
	}
	if reader, ok := e.quotas.(QuotaUsageResolver); ok {
		usage, handled, err := reader.ReadQuotaUsage(ctx, e.runner, mountRecord.Identity(), req.Scope, version)
		if err != nil || handled {
			return usage, err
		}
	}
	key, err := layout.EncodeUsageKey(mountRecord.Identity(), req.Scope)
	if err != nil {
		return model.UsageRecord{}, err
	}
	value, ok, err := e.runner.Get(ctx, key, version)
	if err != nil {
		return model.UsageRecord{}, err
	}
	if !ok {
		return model.UsageRecord{}, nil
	}
	return layout.DecodeUsageValue(value)
}

func (e *Executor) reserveQuota(ctx context.Context, changes []QuotaChange, startVersion uint64) ([]*backend.Mutation, error) {
	if e == nil || e.quotas == nil {
		return nil, nil
	}
	return e.quotas.ReserveQuota(ctx, e.runner, changes, startVersion)
}
