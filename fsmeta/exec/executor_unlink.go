// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"context"
	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
)

type removeDentryRequest struct {
	Mount  fsmeta.MountID
	Parent fsmeta.InodeID
	Name   string
}

func (e *Executor) tryVisibleRemoveDentry(ctx context.Context, compiled compile.CompiledOp, mount fsmeta.MountIdentity, req removeDentryRequest) (bool, error) {
	delta := compiled.Delta
	plan := delta.Plan
	if e == nil || e.visibleCommitter == nil || e.visibleAuthority == nil || delta.Eligibility != compile.EligibilityVisibleCommit {
		return false, nil
	}
	view := e.newVisibleReadView(ctx)
	record, err := view.readDentry(plan.PrimaryKey)
	if err != nil {
		return false, err
	}
	inode, ok, err := view.readInode(mount, record.Inode)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	if inode.Type == fsmeta.InodeTypeDirectory {
		return false, fsmeta.ErrInvalidRequest
	}
	parent, err := readVisibleDirectoryInode(view, mount, req.Parent)
	if err != nil {
		return false, err
	}
	parent, err = decrementDirectoryChildCount(parent)
	if err != nil {
		return false, err
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
	inodeKey, err := fsmeta.EncodeInodeKey(mount, inode.Inode)
	if err != nil {
		return false, err
	}
	effects := []compile.WriteEffect{visibleDeleteEffect(plan.MutateKeys[0])}
	if inode.LinkCount <= 1 {
		effects = append(effects, visibleDeleteEffect(inodeKey))
	} else {
		inode.LinkCount--
		inodeValue, err := fsmeta.EncodeInodeValue(inode)
		if err != nil {
			return false, err
		}
		effects = append(effects, visiblePutEffect(inodeKey, inodeValue))
	}
	parentValue, err := fsmeta.EncodeInodeValue(parent)
	if err != nil {
		return false, err
	}
	effects = append(effects, visiblePutEffect(plan.MutateKeys[1], parentValue))
	concrete, err := view.materializeVisibleCompiledOp(compiled, effects)
	if err != nil {
		return false, err
	}
	return e.tryVisibleCommitAfterRead(ctx, view, concrete)
}

func (e *Executor) removeDentry(ctx context.Context, mount fsmeta.MountIdentity, compiled compile.CompiledOp, req removeDentryRequest) error {
	delta := compiled.Delta
	plan := delta.Plan
	if err := e.admitVisibleAuthority(ctx, delta); err != nil {
		return err
	}
	if committed, err := e.tryVisibleRemoveDentry(ctx, compiled, mount, req); committed || err != nil {
		if err != nil {
			return err
		}
		e.invalidateNegative(plan.MutateKeys[0])
		e.invalidateDirPages(req.Mount, req.Parent)
		return nil
	}
	if err := e.withTxnRetry(ctx, func(startVersion, commitVersion uint64) error {
		record, err := e.readDentry(ctx, plan.PrimaryKey, startVersion)
		if err != nil {
			return err
		}
		dentryValue, err := fsmeta.EncodeDentryValue(record)
		if err != nil {
			return err
		}
		mutations := []*kvrpcpb.Mutation{{
			Op:  kvrpcpb.Mutation_Delete,
			Key: cloneBytes(plan.MutateKeys[0]),
		}}
		predicates := []*kvrpcpb.AtomicPredicate{atomicValueEquals(plan.PrimaryKey, dentryValue)}
		parent, err := e.readDirectoryInode(ctx, mount, req.Parent, startVersion)
		if err != nil {
			return err
		}
		nextParent, err := decrementDirectoryChildCount(parent.record)
		if err != nil {
			return err
		}
		parentValue, err := fsmeta.EncodeInodeValue(nextParent)
		if err != nil {
			return err
		}
		predicates = append(predicates, atomicValueEquals(parent.key, parent.value))
		if inode, ok, err := e.readInode(ctx, mount, record.Inode, startVersion); err != nil {
			return err
		} else if ok {
			inodeKey, err := fsmeta.EncodeInodeKey(mount, inode.Inode)
			if err != nil {
				return err
			}
			if inode.Type == fsmeta.InodeTypeDirectory {
				return fsmeta.ErrInvalidRequest
			}
			oldInodeValue, err := fsmeta.EncodeInodeValue(inode)
			if err != nil {
				return err
			}
			predicates = append(predicates, atomicValueEquals(inodeKey, oldInodeValue))
			if inode.LinkCount <= 1 {
				mutations = append(mutations, &kvrpcpb.Mutation{Op: kvrpcpb.Mutation_Delete, Key: inodeKey})
			} else {
				inode.LinkCount--
				inodeValue, err := fsmeta.EncodeInodeValue(inode)
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
		mutations = append(mutations, &kvrpcpb.Mutation{Op: kvrpcpb.Mutation_Put, Key: cloneBytes(plan.MutateKeys[1]), Value: parentValue})
		if len(mutations) == len(predicates) {
			return e.mutateWithAtomicOnePhase(ctx, plan.Kind, plan.PrimaryKey, predicates, mutations, startVersion, commitVersion)
		}
		return e.mutateWithoutAtomicOnePhase(ctx, plan.Kind, plan.PrimaryKey, mutations, startVersion, commitVersion)
	}, delta.Authority); err != nil {
		return err
	}
	// Removing the dentry must invalidate both point misses and any
	// materialized directory page under its parent.
	e.invalidateNegative(plan.MutateKeys[0])
	e.invalidateDirPages(req.Mount, req.Parent)
	return nil
}

// Unlink removes one non-directory dentry, decrements its inode link count,
// and deletes the inode record when the last dentry goes away.
func (e *Executor) Unlink(ctx context.Context, req fsmeta.UnlinkRequest) error {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return err
	}
	mount := mountRecord.Identity()
	program, err := compile.CompileUnlinkProgram(req, mount, compile.WithQuotaMode(e.visibleQuotaMode()))
	if err != nil {
		return err
	}
	return e.removeDentry(ctx, mount, program.Compiled, removeDentryRequest(req))
}

// RemoveDirectory removes one empty directory dentry and its directory inode.
// Empty is checked through the directory inode ChildCount, which every child
// membership mutation updates in the same metadata transaction.
func (e *Executor) RemoveDirectory(ctx context.Context, req fsmeta.RemoveDirectoryRequest) error {
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
		parentValue, err := fsmeta.EncodeInodeValue(nextParent)
		if err != nil {
			return err
		}
		record, err := e.readDentry(ctx, plan.PrimaryKey, startVersion)
		if err != nil {
			return err
		}
		if record.Type != fsmeta.InodeTypeDirectory {
			return fsmeta.ErrInvalidRequest
		}
		dentryValue, err := fsmeta.EncodeDentryValue(record)
		if err != nil {
			return err
		}
		inode, ok, err := e.readInode(ctx, mount, record.Inode, startVersion)
		if err != nil {
			return err
		}
		if !ok {
			return fsmeta.ErrNotFound
		}
		if inode.Type != fsmeta.InodeTypeDirectory || inode.ChildCount != 0 || inode.Inode == fsmeta.RootInode {
			return fsmeta.ErrInvalidRequest
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
		inodeKey, err := fsmeta.EncodeInodeKey(mount, inode.Inode)
		if err != nil {
			return err
		}
		inodeValue, err := fsmeta.EncodeInodeValue(inode)
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

func (e *Executor) tryVisibleRemoveDirectory(ctx context.Context, compiled compile.CompiledOp, mount fsmeta.MountIdentity, req fsmeta.RemoveDirectoryRequest) (bool, error) {
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
	if record.Type != fsmeta.InodeTypeDirectory {
		return false, fsmeta.ErrInvalidRequest
	}
	inode, ok, err := view.readInode(mount, record.Inode)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, fsmeta.ErrNotFound
	}
	if inode.Type != fsmeta.InodeTypeDirectory || inode.ChildCount != 0 || inode.Inode == fsmeta.RootInode {
		return false, fsmeta.ErrInvalidRequest
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
	parentValue, err := fsmeta.EncodeInodeValue(parent)
	if err != nil {
		return false, err
	}
	inodeKey, err := fsmeta.EncodeInodeKey(mount, inode.Inode)
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
func (e *Executor) Remove(ctx context.Context, req fsmeta.RemoveRequest) error {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return err
	}
	mount := mountRecord.Identity()
	program, err := compile.CompileRemoveProgram(req, mount, compile.WithQuotaMode(e.visibleQuotaMode()))
	if err != nil {
		return err
	}
	return e.removeDentry(ctx, mount, program.Compiled, removeDentryRequest(req))
}
