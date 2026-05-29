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

func (e *Executor) tryVisibleCreate(ctx context.Context, program compile.CreateProgram, mount model.MountIdentity, req model.CreateRequest, dentryValue, inodeValue []byte) (bool, error) {
	delta := program.Compiled.Delta
	if e == nil || e.visibleCommitter == nil || e.visibleAuthority == nil || delta.Eligibility != compile.EligibilityVisibleCommit {
		return false, nil
	}
	view := e.newVisibleReadView(ctx)
	parent, err := readVisibleDirectoryInode(view, mount, req.Parent)
	if err != nil {
		return false, err
	}
	parent, err = incrementDirectoryChildCount(parent)
	if err != nil {
		return false, err
	}
	parentValue, err := layout.EncodeInodeValue(parent)
	if err != nil {
		return false, err
	}
	concrete, err := view.materializeVisibleCompiledOp(program.Compiled, []compile.WriteEffect{
		visiblePutEffect(delta.Plan.MutateKeys[0], parentValue),
		visiblePutEffect(delta.Plan.MutateKeys[1], dentryValue),
		visiblePutEffect(delta.Plan.MutateKeys[2], inodeValue),
	})
	if err != nil {
		return false, err
	}
	return e.tryVisibleCommitAfterRead(ctx, view, concrete)
}

// Create creates one dentry and its inode record in a single transaction.
func (e *Executor) Create(ctx context.Context, req model.CreateRequest) (model.CreateResult, error) {
	if e.inodes == nil {
		return model.CreateResult{}, errInodeAllocatorRequired
	}
	if _, err := layout.EncodeInodeValue(req.Attrs.InodeRecord(model.RootInode)); err != nil {
		return model.CreateResult{}, err
	}
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return model.CreateResult{}, err
	}
	mount := mountRecord.Identity()
	// Allocate after cheap semantic validation and mount admission. Transaction
	// retries below reuse this single ID; failed creates may leave coordinator
	// ID gaps, but they cannot publish a different inode on retry.
	inodeID, err := e.inodes.AllocateCreateInode(ctx, mount, req.Parent, req.Name)
	if err != nil {
		return model.CreateResult{}, err
	}
	program, err := compile.CompileCreateProgram(req, mount, inodeID, compile.WithQuotaMode(e.visibleQuotaMode()))
	if err != nil {
		return model.CreateResult{}, err
	}
	delta := program.Compiled.Delta
	if err := e.admitVisibleAuthority(ctx, delta); err != nil {
		return model.CreateResult{}, err
	}
	plan := delta.Plan
	inode := req.Attrs.InodeRecord(inodeID)
	dentry := model.DentryRecord{
		Parent: req.Parent,
		Name:   req.Name,
		Inode:  inodeID,
		Type:   inode.Type,
	}
	dentryValue := delta.WriteEffects[1].Value
	inodeValue := delta.WriteEffects[2].Value
	e.createTotal.Add(1)
	quotaChanges := []QuotaChange{{
		Mount:      req.Mount,
		MountKeyID: mount.MountKeyID,
		Scope:      req.Parent,
		Bytes:      inodeSizeDelta(inode.Size),
		Inodes:     1,
	}}
	quotaOK := true
	if e.visibleCommitter != nil && e.visibleAuthority != nil && delta.Eligibility == compile.EligibilityVisibleCommit {
		var err error
		quotaOK, err = e.visibleQuotaAllowsCommit(ctx, quotaChanges)
		if err != nil {
			return model.CreateResult{}, err
		}
	}
	if quotaOK {
		if committed, err := e.tryVisibleCreate(ctx, program, mount, req, dentryValue, inodeValue); committed || err != nil {
			if err != nil {
				return model.CreateResult{}, err
			}
			e.rememberVisibleCreate(mount, plan, inode)
			e.forgetVisibleEmptyDirectory(mount, req.Parent)
			return model.CreateResult{Dentry: dentry, Inode: inode}, nil
		}
	}
	if err := e.withTxnRetry(ctx, func(startVersion, commitVersion uint64) error {
		parent, err := e.readDirectoryInode(ctx, mount, req.Parent, startVersion)
		if err != nil {
			return err
		}
		nextParent, err := incrementDirectoryChildCount(parent.record)
		if err != nil {
			return err
		}
		parentValue, err := layout.EncodeInodeValue(nextParent)
		if err != nil {
			return err
		}
		mutations := []*backend.Mutation{
			{
				Op:    backend.MutationPut,
				Key:   cloneBytes(plan.MutateKeys[0]),
				Value: parentValue,
			},
			{
				Op:                backend.MutationPut,
				Key:               cloneBytes(plan.MutateKeys[1]),
				Value:             dentryValue,
				AssertionNotExist: true,
			},
			{
				Op:                backend.MutationPut,
				Key:               cloneBytes(plan.MutateKeys[2]),
				Value:             inodeValue,
				AssertionNotExist: true,
			},
		}
		predicates := []*backend.Predicate{
			atomicValueEquals(parent.key, parent.value),
			atomicNotExists(plan.MutateKeys[1]),
			atomicNotExists(plan.MutateKeys[2]),
		}
		quotaMutations, err := e.reserveQuota(ctx, []QuotaChange{{
			Mount:      req.Mount,
			MountKeyID: mount.MountKeyID,
			Scope:      req.Parent,
			Bytes:      inodeSizeDelta(inode.Size),
			Inodes:     1,
		}}, startVersion)
		if err != nil {
			return err
		}
		all := append(cloneMutations(mutations), quotaMutations...)
		if len(quotaMutations) == 0 {
			// One-phase counters are per transaction attempt, not per logical
			// Create, so contention retries and admission misses stay visible.
			return e.mutateWithAtomicOnePhase(ctx, plan.Kind, plan.PrimaryKey, predicates, all, startVersion, commitVersion)
		}
		return e.mutateWithoutAtomicOnePhase(ctx, plan.Kind, plan.PrimaryKey, all, startVersion, commitVersion)
	}, delta.Authority); err != nil {
		return model.CreateResult{}, err
	}
	e.rememberVisibleCreate(mount, plan, inode)
	e.forgetVisibleEmptyDirectory(mount, req.Parent)
	return model.CreateResult{Dentry: dentry, Inode: inode}, nil
}
