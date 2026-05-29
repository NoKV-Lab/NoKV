// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"context"

	"github.com/feichai0017/NoKV/fsmeta/backend"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
)

func (e *Executor) tryVisibleUpdateInode(ctx context.Context, program compile.UpdateInodeProgram, mount model.MountIdentity, req model.UpdateInodeRequest) (model.InodeRecord, bool, error) {
	delta := program.Compiled.Delta
	if e == nil || e.visibleCommitter == nil || e.visibleAuthority == nil || delta.Eligibility != compile.EligibilityVisibleCommit {
		return model.InodeRecord{}, false, nil
	}
	plan := delta.Plan
	view := e.newVisibleReadView(ctx)
	dentry, err := view.readDentry(plan.ReadKeys[0])
	if err != nil {
		return model.InodeRecord{}, false, err
	}
	if dentry.Inode != req.Inode {
		return model.InodeRecord{}, false, model.ErrInvalidRequest
	}
	inode, ok, err := view.readInode(mount, req.Inode)
	if err != nil {
		return model.InodeRecord{}, false, err
	}
	if !ok {
		return model.InodeRecord{}, false, model.ErrNotFound
	}
	if dentry.Type != inode.Type {
		return model.InodeRecord{}, false, model.ErrInvalidValue
	}
	if inode.LinkCount != 1 {
		return model.InodeRecord{}, false, model.ErrInvalidRequest
	}
	sizeDelta := int64(0)
	if req.SetSize {
		sizeDelta = inodeSizeChange(inode.Size, req.Size)
		if sizeDelta != 0 {
			quotaOK, err := e.visibleQuotaAllowsCommit(ctx, []QuotaChange{{
				Mount:      req.Mount,
				MountKeyID: mount.MountKeyID,
				Scope:      req.Parent,
				Bytes:      sizeDelta,
			}})
			if err != nil {
				return model.InodeRecord{}, false, err
			}
			if !quotaOK {
				return model.InodeRecord{}, false, nil
			}
		}
	}
	if req.SetMode {
		inode.Mode = req.Mode
	}
	if req.SetSize {
		inode.Size = req.Size
	}
	if req.SetUpdatedUnixNs {
		inode.UpdatedUnixNs = req.UpdatedUnixNs
	}
	if req.SetOpaqueAttrs {
		inode.OpaqueAttrs = append([]byte(nil), req.OpaqueAttrs...)
	}
	value, err := layout.EncodeInodeValue(inode)
	if err != nil {
		return model.InodeRecord{}, false, err
	}
	concrete, err := view.materializeVisibleCompiledOp(program.Compiled, []compile.WriteEffect{visiblePutEffect(plan.MutateKeys[0], value)})
	if err != nil {
		return model.InodeRecord{}, false, err
	}
	committed, err := e.tryVisibleCommitAfterRead(ctx, view, concrete)
	if err != nil {
		return model.InodeRecord{}, committed, err
	}
	if !committed {
		return model.InodeRecord{}, false, nil
	}
	return inode, true, nil
}

// UpdateInode updates mutable inode attributes and applies the size quota delta
// in the same transaction. The parent field is required because quota and
// DirPage invalidation are directory-scoped by parent inode and page token.
func (e *Executor) UpdateInode(ctx context.Context, req model.UpdateInodeRequest) (model.InodeRecord, error) {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return model.InodeRecord{}, err
	}
	mount := mountRecord.Identity()
	program, err := compile.CompileUpdateInodeProgram(req, mount, compile.WithQuotaMode(e.visibleQuotaMode()))
	if err != nil {
		return model.InodeRecord{}, err
	}
	delta := program.Compiled.Delta
	if err := e.admitVisibleAuthority(ctx, delta); err != nil {
		return model.InodeRecord{}, err
	}
	plan := delta.Plan
	if !req.SetSize && !req.SetMode && !req.SetUpdatedUnixNs && !req.SetOpaqueAttrs {
		return model.InodeRecord{}, model.ErrInvalidRequest
	}
	if updated, committed, err := e.tryVisibleUpdateInode(ctx, program, mount, req); committed || err != nil {
		if err != nil {
			return model.InodeRecord{}, err
		}
		e.invalidateDirPages(req.Mount, req.Parent)
		return updated, nil
	}
	var updated model.InodeRecord
	if err := e.withTxnRetry(ctx, func(startVersion, commitVersion uint64) error {
		dentry, err := e.readDentry(ctx, plan.ReadKeys[0], startVersion)
		if err != nil {
			return err
		}
		dentryValue, err := layout.EncodeDentryValue(dentry)
		if err != nil {
			return err
		}
		if dentry.Inode != req.Inode {
			return model.ErrInvalidRequest
		}
		inode, ok, err := e.readInode(ctx, mount, req.Inode, startVersion)
		if err != nil {
			return err
		}
		if !ok {
			return model.ErrNotFound
		}
		if dentry.Type != inode.Type {
			return model.ErrInvalidValue
		}
		// fsmeta does not maintain an inode->parents reverse index. Updating a
		// hard-linked inode would require invalidating and quota-adjusting every
		// parent, so reject it rather than silently corrupting accounting.
		if inode.LinkCount != 1 {
			return model.ErrInvalidRequest
		}
		oldInodeValue, err := layout.EncodeInodeValue(inode)
		if err != nil {
			return err
		}
		sizeDelta := int64(0)
		if req.SetSize {
			sizeDelta = inodeSizeChange(inode.Size, req.Size)
			inode.Size = req.Size
		}
		if req.SetMode {
			inode.Mode = req.Mode
		}
		if req.SetUpdatedUnixNs {
			inode.UpdatedUnixNs = req.UpdatedUnixNs
		}
		if req.SetOpaqueAttrs {
			inode.OpaqueAttrs = append([]byte(nil), req.OpaqueAttrs...)
		}
		value, err := layout.EncodeInodeValue(inode)
		if err != nil {
			return err
		}
		mutations := []*backend.Mutation{{
			Op:    backend.MutationPut,
			Key:   cloneBytes(plan.MutateKeys[0]),
			Value: value,
		}}
		if sizeDelta != 0 {
			quotaMutations, err := e.reserveQuota(ctx, []QuotaChange{{
				Mount:      req.Mount,
				MountKeyID: mount.MountKeyID,
				Scope:      req.Parent,
				Bytes:      sizeDelta,
			}}, startVersion)
			if err != nil {
				return err
			}
			mutations = append(mutations, quotaMutations...)
		}
		if sizeDelta == 0 || len(mutations) == 1 {
			predicates := []*backend.Predicate{
				atomicValueEquals(plan.ReadKeys[0], dentryValue),
				atomicValueEquals(plan.MutateKeys[0], oldInodeValue),
			}
			if err := e.mutateWithAtomicOnePhase(ctx, plan.Kind, plan.PrimaryKey, predicates, mutations, startVersion, commitVersion); err != nil {
				return err
			}
		} else if err := e.mutateWithoutAtomicOnePhase(ctx, plan.Kind, plan.PrimaryKey, mutations, startVersion, commitVersion); err != nil {
			return err
		}
		updated = inode
		return nil
	}, delta.Authority); err != nil {
		return model.InodeRecord{}, err
	}
	e.invalidateDirPages(req.Mount, req.Parent)
	return updated, nil
}
