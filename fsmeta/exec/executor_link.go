// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"context"
	"errors"
	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
)

func (e *Executor) tryVisibleLink(ctx context.Context, program compile.LinkProgram, mount fsmeta.MountIdentity, req fsmeta.LinkRequest) (bool, error) {
	compiled := program.Compiled
	delta := compiled.Delta
	plan := delta.Plan
	if e == nil || e.visibleCommitter == nil || e.visibleAuthority == nil || delta.Eligibility != compile.EligibilityVisibleCommit {
		return false, nil
	}
	view := e.newVisibleReadView(ctx)
	record, err := view.readDentry(plan.ReadKeys[0])
	if err != nil {
		return false, err
	}
	if record.Type == fsmeta.InodeTypeDirectory {
		return false, fsmeta.ErrInvalidRequest
	}
	if !e.visibleNotExistsKnown(delta.Authority, plan.ReadKeys[1], e.visiblePredicateIndex()) {
		if _, err := view.readDentry(plan.ReadKeys[1]); err == nil {
			return false, fsmeta.ErrExists
		} else if !errors.Is(err, fsmeta.ErrNotFound) {
			return false, err
		}
	}
	inode, ok, err := view.readInode(mount, record.Inode)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, fsmeta.ErrNotFound
	}
	if inode.Type == fsmeta.InodeTypeDirectory || inode.LinkCount == ^uint32(0) {
		return false, fsmeta.ErrInvalidRequest
	}
	if inode.LinkCount == 0 {
		inode.LinkCount = 1
	}
	parent, err := readVisibleDirectoryInode(view, mount, req.ToParent)
	if err != nil {
		return false, err
	}
	parent, err = incrementDirectoryChildCount(parent)
	if err != nil {
		return false, err
	}
	quotaOK, err := e.visibleQuotaAllowsCommit(ctx, []QuotaChange{{
		Mount:      req.Mount,
		MountKeyID: mount.MountKeyID,
		Scope:      req.ToParent,
		Bytes:      inodeSizeDelta(inode.Size),
		Inodes:     1,
	}})
	if err != nil {
		return false, err
	}
	if !quotaOK {
		return false, nil
	}
	inode.LinkCount++
	dentryValue, err := fsmeta.EncodeDentryValue(fsmeta.DentryRecord{
		Parent: req.ToParent,
		Name:   req.ToName,
		Inode:  record.Inode,
		Type:   record.Type,
	})
	if err != nil {
		return false, err
	}
	inodeKey, err := fsmeta.EncodeInodeKey(mount, inode.Inode)
	if err != nil {
		return false, err
	}
	inodeValue, err := fsmeta.EncodeInodeValue(inode)
	if err != nil {
		return false, err
	}
	parentValue, err := fsmeta.EncodeInodeValue(parent)
	if err != nil {
		return false, err
	}
	concrete, err := view.materializeVisibleCompiledOp(compiled, []compile.WriteEffect{
		visiblePutEffect(plan.ReadKeys[1], dentryValue),
		visiblePutEffect(inodeKey, inodeValue),
		visiblePutEffect(plan.MutateKeys[1], parentValue),
	})
	if err != nil {
		return false, err
	}
	return e.tryVisibleCommitAfterRead(ctx, view, concrete)
}

// Link creates a second dentry for an existing non-directory inode and bumps
// the inode link count in the same transaction.
func (e *Executor) Link(ctx context.Context, req fsmeta.LinkRequest) error {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return err
	}
	mount := mountRecord.Identity()
	program, err := compile.CompileLinkProgram(req, mount, compile.WithQuotaMode(e.visibleQuotaMode()))
	if err != nil {
		return err
	}
	delta := program.Compiled.Delta
	if err := e.admitVisibleAuthority(ctx, delta); err != nil {
		return err
	}
	plan := delta.Plan
	if committed, err := e.tryVisibleLink(ctx, program, mount, req); committed || err != nil {
		if err != nil {
			return err
		}
		e.forgetVisibleEmptyDirectory(mount, req.ToParent)
		e.invalidateNegative(plan.ReadKeys[1])
		e.invalidateDirPages(req.Mount, req.ToParent)
		return nil
	}
	if err := e.withTxnRetry(ctx, func(startVersion, commitVersion uint64) error {
		record, err := e.readDentry(ctx, plan.ReadKeys[0], startVersion)
		if err != nil {
			return err
		}
		sourceDentryValue, err := fsmeta.EncodeDentryValue(record)
		if err != nil {
			return err
		}
		if record.Type == fsmeta.InodeTypeDirectory {
			return fsmeta.ErrInvalidRequest
		}
		if _, err := e.readDentry(ctx, plan.ReadKeys[1], startVersion); err == nil {
			return fsmeta.ErrExists
		} else if !errors.Is(err, fsmeta.ErrNotFound) {
			return err
		}
		inode, ok, err := e.readInode(ctx, mount, record.Inode, startVersion)
		if err != nil {
			return err
		}
		if !ok {
			return fsmeta.ErrNotFound
		}
		if inode.Type == fsmeta.InodeTypeDirectory {
			return fsmeta.ErrInvalidRequest
		}
		if inode.LinkCount == ^uint32(0) {
			return fsmeta.ErrInvalidRequest
		}
		if inode.LinkCount == 0 {
			inode.LinkCount = 1
		}
		parent, err := e.readDirectoryInode(ctx, mount, req.ToParent, startVersion)
		if err != nil {
			return err
		}
		nextParent, err := incrementDirectoryChildCount(parent.record)
		if err != nil {
			return err
		}
		parentValue, err := fsmeta.EncodeInodeValue(nextParent)
		if err != nil {
			return err
		}
		oldInodeValue, err := fsmeta.EncodeInodeValue(inode)
		if err != nil {
			return err
		}
		inode.LinkCount++

		dentryValue, err := fsmeta.EncodeDentryValue(fsmeta.DentryRecord{
			Parent: req.ToParent,
			Name:   req.ToName,
			Inode:  record.Inode,
			Type:   record.Type,
		})
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
			{
				Op:                kvrpcpb.Mutation_Put,
				Key:               cloneBytes(plan.ReadKeys[1]),
				Value:             dentryValue,
				AssertionNotExist: true,
			},
			{
				Op:    kvrpcpb.Mutation_Put,
				Key:   inodeKey,
				Value: inodeValue,
			},
			{
				Op:    kvrpcpb.Mutation_Put,
				Key:   cloneBytes(plan.MutateKeys[1]),
				Value: parentValue,
			},
		}
		quotaMutations, err := e.reserveQuota(ctx, []QuotaChange{{
			Mount:      req.Mount,
			MountKeyID: mount.MountKeyID,
			Scope:      req.ToParent,
			Bytes:      inodeSizeDelta(inode.Size),
			Inodes:     1,
		}}, startVersion)
		if err != nil {
			return err
		}
		mutations = append(mutations, quotaMutations...)
		if len(quotaMutations) == 0 {
			// Link is safe on 1PC only when the source dentry and inode still
			// equal the records read by this attempt. These value predicates are
			// the correctness boundary that prevents overwriting a concurrent
			// UpdateInode with an older inode body.
			predicates := []*kvrpcpb.AtomicPredicate{
				atomicValueEquals(plan.ReadKeys[0], sourceDentryValue),
				atomicNotExists(plan.ReadKeys[1]),
				atomicValueEquals(inodeKey, oldInodeValue),
				atomicValueEquals(parent.key, parent.value),
			}
			return e.mutateWithAtomicOnePhase(ctx, plan.Kind, plan.PrimaryKey, predicates, mutations, startVersion, commitVersion)
		}
		return e.mutateWithoutAtomicOnePhase(ctx, plan.Kind, plan.PrimaryKey, mutations, startVersion, commitVersion)
	}, delta.Authority); err != nil {
		return err
	}
	// Link writes a fresh dentry at ReadKeys[1]; drop any negative memo
	// and visible-derived empty-directory fact, then bump the destination
	// parent's dirpage epoch so the new dentry shows up on the next
	// ReadDirPlus.
	e.forgetVisibleEmptyDirectory(mount, req.ToParent)
	e.invalidateNegative(plan.ReadKeys[1])
	e.invalidateDirPages(req.Mount, req.ToParent)
	return nil
}
