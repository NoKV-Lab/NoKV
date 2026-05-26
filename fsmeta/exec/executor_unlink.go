// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"context"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
)

type removeDentryRequest struct {
	Mount  model.MountID
	Parent model.InodeID
	Name   string
}

func (e *Executor) tryVisibleRemoveDentry(ctx context.Context, compiled compile.CompiledOp, mount model.MountIdentity, req removeDentryRequest) (model.RemoveResult, bool, error) {
	delta := compiled.Delta
	plan := delta.Plan
	if e == nil || e.visibleCommitter == nil || e.visibleAuthority == nil || delta.Eligibility != compile.EligibilityVisibleCommit {
		return model.RemoveResult{}, false, nil
	}
	view := e.newVisibleReadView(ctx)
	record, err := view.readDentry(plan.PrimaryKey)
	if err != nil {
		return model.RemoveResult{}, false, err
	}
	inode, ok, err := view.readInode(mount, record.Inode)
	if err != nil {
		return model.RemoveResult{}, false, err
	}
	if !ok {
		return model.RemoveResult{}, false, nil
	}
	if inode.Type == model.InodeTypeDirectory {
		return model.RemoveResult{}, false, model.ErrInvalidRequest
	}
	parent, err := readVisibleDirectoryInode(view, mount, req.Parent)
	if err != nil {
		return model.RemoveResult{}, false, err
	}
	parent, err = decrementDirectoryChildCount(parent)
	if err != nil {
		return model.RemoveResult{}, false, err
	}
	quotaOK, err := e.visibleQuotaAllowsCommit(ctx, []QuotaChange{{
		Mount:      req.Mount,
		MountKeyID: mount.MountKeyID,
		Scope:      req.Parent,
		Bytes:      -inodeSizeDelta(inode.Size),
		Inodes:     -1,
	}})
	if err != nil {
		return model.RemoveResult{}, false, err
	}
	if !quotaOK {
		return model.RemoveResult{}, false, nil
	}
	inodeKey, err := layout.EncodeInodeKey(mount, inode.Inode)
	if err != nil {
		return model.RemoveResult{}, false, err
	}
	result := model.RemoveResult{
		RemovedDentry: record,
		OldInode:      inode,
		InodeDeleted:  inode.LinkCount <= 1,
	}
	effects := []compile.WriteEffect{visibleDeleteEffect(plan.MutateKeys[0])}
	if result.InodeDeleted {
		effects = append(effects, visibleDeleteEffect(inodeKey))
	} else {
		inode.LinkCount--
		inodeValue, err := layout.EncodeInodeValue(inode)
		if err != nil {
			return model.RemoveResult{}, false, err
		}
		effects = append(effects, visiblePutEffect(inodeKey, inodeValue))
	}
	parentValue, err := layout.EncodeInodeValue(parent)
	if err != nil {
		return model.RemoveResult{}, false, err
	}
	effects = append(effects, visiblePutEffect(plan.MutateKeys[1], parentValue))
	concrete, err := view.materializeVisibleCompiledOp(compiled, effects)
	if err != nil {
		return model.RemoveResult{}, false, err
	}
	committed, err := e.tryVisibleCommitAfterRead(ctx, view, concrete)
	if err != nil || !committed {
		return model.RemoveResult{}, committed, err
	}
	return result, true, nil
}

func (e *Executor) removeDentry(ctx context.Context, mount model.MountIdentity, compiled compile.CompiledOp, req removeDentryRequest) (model.RemoveResult, error) {
	delta := compiled.Delta
	plan := delta.Plan
	if err := e.admitVisibleAuthority(ctx, delta); err != nil {
		return model.RemoveResult{}, err
	}
	if result, committed, err := e.tryVisibleRemoveDentry(ctx, compiled, mount, req); committed || err != nil {
		if err != nil {
			return model.RemoveResult{}, err
		}
		e.invalidateNegative(plan.MutateKeys[0])
		e.invalidateDirPages(req.Mount, req.Parent)
		return result, nil
	}
	var result model.RemoveResult
	if err := e.withTxnRetry(ctx, func(startVersion, commitVersion uint64) error {
		record, err := e.readDentry(ctx, plan.PrimaryKey, startVersion)
		if err != nil {
			return err
		}
		dentryValue, err := layout.EncodeDentryValue(record)
		if err != nil {
			return err
		}
		mutations := []*kvrpcpb.Mutation{{
			Op:  kvrpcpb.Mutation_Delete,
			Key: cloneBytes(plan.MutateKeys[0]),
		}}
		attemptResult := model.RemoveResult{RemovedDentry: record}
		predicates := []*kvrpcpb.AtomicPredicate{atomicValueEquals(plan.PrimaryKey, dentryValue)}
		parent, err := e.readDirectoryInode(ctx, mount, req.Parent, startVersion)
		if err != nil {
			return err
		}
		nextParent, err := decrementDirectoryChildCount(parent.record)
		if err != nil {
			return err
		}
		parentValue, err := layout.EncodeInodeValue(nextParent)
		if err != nil {
			return err
		}
		predicates = append(predicates, atomicValueEquals(parent.key, parent.value))
		if inode, ok, err := e.readInode(ctx, mount, record.Inode, startVersion); err != nil {
			return err
		} else if ok {
			attemptResult.OldInode = inode
			inodeKey, err := layout.EncodeInodeKey(mount, inode.Inode)
			if err != nil {
				return err
			}
			if inode.Type == model.InodeTypeDirectory {
				return model.ErrInvalidRequest
			}
			oldInodeValue, err := layout.EncodeInodeValue(inode)
			if err != nil {
				return err
			}
			predicates = append(predicates, atomicValueEquals(inodeKey, oldInodeValue))
			if inode.LinkCount <= 1 {
				attemptResult.InodeDeleted = true
				mutations = append(mutations, &kvrpcpb.Mutation{Op: kvrpcpb.Mutation_Delete, Key: inodeKey})
			} else {
				inode.LinkCount--
				inodeValue, err := layout.EncodeInodeValue(inode)
				if err != nil {
					return err
				}
				mutations = append(mutations, &kvrpcpb.Mutation{Op: kvrpcpb.Mutation_Put, Key: inodeKey, Value: inodeValue})
			}
			quotaMutations, err := e.reserveQuota(ctx, []QuotaChange{{
				Mount:      req.Mount,
				MountKeyID: mount.MountKeyID,
				Scope:      req.Parent,
				Bytes:      -inodeSizeDelta(inode.Size),
				Inodes:     -1,
			}}, startVersion)
			if err != nil {
				return err
			}
			mutations = append(mutations, quotaMutations...)
		}
		result = attemptResult
		mutations = append(mutations, &kvrpcpb.Mutation{Op: kvrpcpb.Mutation_Put, Key: cloneBytes(plan.MutateKeys[1]), Value: parentValue})
		if len(mutations) == len(predicates) {
			return e.mutateWithAtomicOnePhase(ctx, plan.Kind, plan.PrimaryKey, predicates, mutations, startVersion, commitVersion)
		}
		return e.mutateWithoutAtomicOnePhase(ctx, plan.Kind, plan.PrimaryKey, mutations, startVersion, commitVersion)
	}, delta.Authority); err != nil {
		return model.RemoveResult{}, err
	}
	// Removing the dentry must invalidate both point misses and any
	// materialized directory page under its parent.
	e.invalidateNegative(plan.MutateKeys[0])
	e.invalidateDirPages(req.Mount, req.Parent)
	return result, nil
}

// Unlink removes one non-directory dentry, decrements its inode link count,
// and deletes the inode record when the last dentry goes away.
func (e *Executor) Unlink(ctx context.Context, req model.UnlinkRequest) error {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return err
	}
	mount := mountRecord.Identity()
	program, err := compile.CompileUnlinkProgram(req, mount, compile.WithQuotaMode(e.visibleQuotaMode()))
	if err != nil {
		return err
	}
	_, err = e.removeDentry(ctx, mount, program.Compiled, removeDentryRequest(req))
	return err
}

// RemoveDirectory removes one empty directory dentry and its directory inode.
// Empty is checked through the directory inode ChildCount, which every child
// membership mutation updates in the same metadata transaction.
func (e *Executor) RemoveDirectory(ctx context.Context, req model.RemoveDirectoryRequest) error {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return err
	}
	mount := mountRecord.Identity()
	program, err := compile.CompileRemoveDirectoryProgram(req, mount)
	if err != nil {
		return err
	}
	delta := program.Compiled.Delta
	if err := e.admitVisibleAuthority(ctx, delta); err != nil {
		return err
	}
	plan := delta.Plan
	if committed, err := e.tryVisibleRemoveDirectory(ctx, program.Compiled, mount, req); committed || err != nil {
		if err != nil {
			return err
		}
		e.invalidateNegative(plan.MutateKeys[1])
		e.invalidateDirPages(req.Mount, req.Parent)
		return nil
	}
	if err := e.withTxnRetry(ctx, func(startVersion, commitVersion uint64) error {
		parent, err := e.readDirectoryInode(ctx, mount, req.Parent, startVersion)
		if err != nil {
			return err
		}
		nextParent, err := decrementDirectoryChildCount(parent.record)
		if err != nil {
			return err
		}
		parentValue, err := layout.EncodeInodeValue(nextParent)
		if err != nil {
			return err
		}
		record, err := e.readDentry(ctx, plan.PrimaryKey, startVersion)
		if err != nil {
			return err
		}
		if record.Type != model.InodeTypeDirectory {
			return model.ErrInvalidRequest
		}
		dentryValue, err := layout.EncodeDentryValue(record)
		if err != nil {
			return err
		}
		inode, ok, err := e.readInode(ctx, mount, record.Inode, startVersion)
		if err != nil {
			return err
		}
		if !ok {
			return model.ErrNotFound
		}
		if inode.Type != model.InodeTypeDirectory || inode.ChildCount != 0 || inode.Inode == model.RootInode {
			return model.ErrInvalidRequest
		}
		quotaMutations, err := e.reserveQuota(ctx, []QuotaChange{{
			Mount:      req.Mount,
			MountKeyID: mount.MountKeyID,
			Scope:      req.Parent,
			Bytes:      -inodeSizeDelta(inode.Size),
			Inodes:     -1,
		}}, startVersion)
		if err != nil {
			return err
		}
		inodeKey, err := layout.EncodeInodeKey(mount, inode.Inode)
		if err != nil {
			return err
		}
		inodeValue, err := layout.EncodeInodeValue(inode)
		if err != nil {
			return err
		}
		mutations := []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Mutation_Put, Key: cloneBytes(plan.MutateKeys[0]), Value: parentValue},
			{Op: kvrpcpb.Mutation_Delete, Key: cloneBytes(plan.MutateKeys[1])},
			{Op: kvrpcpb.Mutation_Delete, Key: inodeKey},
		}
		mutations = append(mutations, quotaMutations...)
		predicates := []*kvrpcpb.AtomicPredicate{
			atomicValueEquals(parent.key, parent.value),
			atomicValueEquals(plan.PrimaryKey, dentryValue),
			atomicValueEquals(inodeKey, inodeValue),
		}
		if len(quotaMutations) == 0 {
			return e.mutateWithAtomicOnePhase(ctx, plan.Kind, plan.PrimaryKey, predicates, mutations, startVersion, commitVersion)
		}
		return e.mutateWithoutAtomicOnePhase(ctx, plan.Kind, plan.PrimaryKey, mutations, startVersion, commitVersion)
	}, delta.Authority); err != nil {
		return err
	}
	e.invalidateNegative(plan.MutateKeys[1])
	e.invalidateDirPages(req.Mount, req.Parent)
	return nil
}

func (e *Executor) tryVisibleRemoveDirectory(ctx context.Context, compiled compile.CompiledOp, mount model.MountIdentity, req model.RemoveDirectoryRequest) (bool, error) {
	delta := compiled.Delta
	plan := delta.Plan
	if e == nil || e.visibleCommitter == nil || e.visibleAuthority == nil || delta.Eligibility != compile.EligibilityVisibleCommit {
		return false, nil
	}
	view := e.newVisibleReadView(ctx)
	parent, err := readVisibleDirectoryInode(view, mount, req.Parent)
	if err != nil {
		return false, err
	}
	parent, err = decrementDirectoryChildCount(parent)
	if err != nil {
		return false, err
	}
	record, err := view.readDentry(plan.PrimaryKey)
	if err != nil {
		return false, err
	}
	if record.Type != model.InodeTypeDirectory {
		return false, model.ErrInvalidRequest
	}
	inode, ok, err := view.readInode(mount, record.Inode)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, model.ErrNotFound
	}
	if inode.Type != model.InodeTypeDirectory || inode.ChildCount != 0 || inode.Inode == model.RootInode {
		return false, model.ErrInvalidRequest
	}
	quotaOK, err := e.visibleQuotaAllowsCommit(ctx, []QuotaChange{{
		Mount:      req.Mount,
		MountKeyID: mount.MountKeyID,
		Scope:      req.Parent,
		Bytes:      -inodeSizeDelta(inode.Size),
		Inodes:     -1,
	}})
	if err != nil {
		return false, err
	}
	if !quotaOK {
		return false, nil
	}
	parentValue, err := layout.EncodeInodeValue(parent)
	if err != nil {
		return false, err
	}
	inodeKey, err := layout.EncodeInodeKey(mount, inode.Inode)
	if err != nil {
		return false, err
	}
	concrete, err := view.materializeVisibleCompiledOp(compiled, []compile.WriteEffect{
		visiblePutEffect(plan.MutateKeys[0], parentValue),
		visibleDeleteEffect(plan.MutateKeys[1]),
		visibleDeleteEffect(inodeKey),
	})
	if err != nil {
		return false, err
	}
	return e.tryVisibleCommitAfterRead(ctx, view, concrete)
}

// Remove is the product-facing primitive for removing one non-directory
// namespace entry. Directory removal needs a separate directory-emptiness
// contract, so v1 keeps directory targets invalid instead of orphaning children.
func (e *Executor) Remove(ctx context.Context, req model.RemoveRequest) (model.RemoveResult, error) {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return model.RemoveResult{}, err
	}
	mount := mountRecord.Identity()
	program, err := compile.CompileRemoveProgram(req, mount, compile.WithQuotaMode(e.visibleQuotaMode()))
	if err != nil {
		return model.RemoveResult{}, err
	}
	return e.removeDentry(ctx, mount, program.Compiled, removeDentryRequest(req))
}
