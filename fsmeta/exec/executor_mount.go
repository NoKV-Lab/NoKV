// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"context"

	"github.com/feichai0017/NoKV/fsmeta/model"
)

func (e *Executor) requireActiveMount(ctx context.Context, mount model.MountID) error {
	_, err := e.resolveActiveMount(ctx, mount)
	return err
}

func (e *Executor) resolveActiveMount(ctx context.Context, mount model.MountID) (MountAdmission, error) {
	record, err := e.resolveKnownMount(ctx, mount)
	if err != nil {
		return MountAdmission{}, err
	}
	if record.Retired {
		return MountAdmission{}, model.ErrMountRetired
	}
	return record, nil
}

func (e *Executor) resolveKnownMount(ctx context.Context, mount model.MountID) (MountAdmission, error) {
	if e == nil || e.mounts == nil {
		return MountAdmission{}, model.ErrMountNotRegistered
	}
	record, err := e.mounts.ResolveMount(ctx, mount)
	if err != nil {
		return MountAdmission{}, err
	}
	if record.MountID == "" {
		return MountAdmission{}, model.ErrMountNotRegistered
	}
	if record.MountKeyID == 0 {
		return MountAdmission{}, model.ErrMountNotRegistered
	}
	return record, nil
}
