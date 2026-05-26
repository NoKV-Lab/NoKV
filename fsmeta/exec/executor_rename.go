// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"context"
	"errors"
	"fmt"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
)

func (e *Executor) tryVisibleRename(ctx context.Context, compiled compile.CompiledOp, move renameMove) (bool, error) {
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
	sourceFromVisible := view.observedKeyFromVisibleOverlay(plan.ReadKeys[0])
	if !e.visibleNotExistsKnown(delta.Authority, plan.ReadKeys[1], e.visiblePredicateIndex()) {
		if _, err := view.readDentry(plan.ReadKeys[1]); err == nil {
			return false, model.ErrExists
		} else if !errors.Is(err, model.ErrNotFound) {
			return false, err
		}
	}
	if !sourceFromVisible {
		if view.observedVisibleOverlay() {
			return false, errVisibleOverlayFallbackUnsafe
		}
		return false, nil
	}
	fromParent, err := readVisibleDirectoryInode(view, move.identity, move.fromParent)
	if err != nil {
		return false, err
	}
	toParent := fromParent
	if move.fromParent != move.toParent {
		fromParent, err = decrementDirectoryChildCount(fromParent)
		if err != nil {
			return false, err
		}
		toParent, err = readVisibleDirectoryInode(view, move.identity, move.toParent)
		if err != nil {
			return false, err
		}
		toParent, err = incrementDirectoryChildCount(toParent)
		if err != nil {
			return false, err
		}
	}
	if move.fromParent != move.toParent {
		if inode, ok, err := view.readInode(move.identity, record.Inode); err != nil {
			return false, err
		} else if ok {
			quotaOK, err := e.visibleQuotaAllowsCommit(ctx, []QuotaChange{
				{Mount: move.mount, MountKeyID: move.identity.MountKeyID, Scope: move.fromParent, Bytes: -inodeSizeDelta(inode.Size), Inodes: -1},
				{Mount: move.mount, MountKeyID: move.identity.MountKeyID, Scope: move.toParent, Bytes: inodeSizeDelta(inode.Size), Inodes: 1},
			})
			if err != nil {
				return false, err
			}
			if !quotaOK {
				return false, nil
			}
		}
	}
	record.Parent = move.toParent
	record.Name = move.toName
	value, err := layout.EncodeDentryValue(record)
	if err != nil {
		return false, err
	}
	fromParentValue, err := layout.EncodeInodeValue(fromParent)
	if err != nil {
		return false, err
	}
	toParentValue, err := layout.EncodeInodeValue(toParent)
	if err != nil {
		return false, err
	}
	concrete, err := view.materializeVisibleCompiledOp(compiled, []compile.WriteEffect{
		visibleDeleteEffect(plan.MutateKeys[0]),
		visiblePutEffect(plan.MutateKeys[1], value),
		visiblePutEffect(plan.MutateKeys[2], fromParentValue),
		visiblePutEffect(plan.MutateKeys[3], toParentValue),
	})
	if err != nil {
		return false, err
	}
	return e.tryVisibleCommitAfterRead(ctx, view, concrete)
}

type renameMove struct {
	mount      model.MountID
	identity   model.MountIdentity
	fromParent model.InodeID
	fromName   string
	toParent   model.InodeID
	toName     string
}

func renameMoveFromRename(req model.RenameRequest, identity model.MountIdentity) renameMove {
	return renameMove{
		mount:      req.Mount,
		identity:   identity,
		fromParent: req.FromParent,
		fromName:   req.FromName,
		toParent:   req.ToParent,
		toName:     req.ToName,
	}
}

func renameMoveFromRenameReplace(req model.RenameReplaceRequest, identity model.MountIdentity) renameMove {
	return renameMove{
		mount:      req.Mount,
		identity:   identity,
		fromParent: req.FromParent,
		fromName:   req.FromName,
		toParent:   req.ToParent,
		toName:     req.ToName,
	}
}

func renameMoveFromRenameSubtree(req model.RenameSubtreeRequest, identity model.MountIdentity) renameMove {
	return renameMove{
		mount:      req.Mount,
		identity:   identity,
		fromParent: req.FromParent,
		fromName:   req.FromName,
		toParent:   req.ToParent,
		toName:     req.ToName,
	}
}

// Rename moves one dentry inside the same subtree authority. It is deliberately
// a data-plane transaction: no rooted handoff is published, so common staged
// publish paths do not serialize through the control plane.
func (e *Executor) Rename(ctx context.Context, req model.RenameRequest) error {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return err
	}
	mount := mountRecord.Identity()
	program, err := compile.CompileRenameProgram(req, mount)
	if err != nil {
		return err
	}
	delta := program.Compiled.Delta
	if err := e.requireSameAuthority(ctx, req.Mount, req.FromParent, req.ToParent); err != nil {
		return err
	}
	if err := e.admitVisibleAuthority(ctx, delta); err != nil {
		return err
	}
	plan := delta.Plan
	move := renameMoveFromRename(req, mount)
	var movedSize uint64
	var movedInode bool
	if committed, err := e.tryVisibleRename(ctx, program.Compiled, move); committed || err != nil {
		if err != nil {
			return err
		}
		e.forgetVisibleEmptyDirectory(mount, req.ToParent)
		e.invalidateNegative(plan.ReadKeys...)
		e.invalidateNegative(plan.MutateKeys...)
		e.invalidateDirPages(req.Mount, req.FromParent, req.ToParent)
		return nil
	}
	if err := e.withTxnRetry(ctx, func(startVersion, commitVersion uint64) error {
		mutations, predicates, err := e.prepareRenameMutations(ctx, plan, move, startVersion, &movedSize, &movedInode)
		if err != nil {
			return err
		}
		if len(mutations) == len(predicates) {
			return e.mutateWithAtomicOnePhase(ctx, plan.Kind, plan.PrimaryKey, predicates, mutations, startVersion, commitVersion)
		}
		return e.mutateWithoutAtomicOnePhase(ctx, plan.Kind, plan.PrimaryKey, mutations, startVersion, commitVersion)
	}, delta.Authority); err != nil {
		return err
	}
	e.forgetVisibleEmptyDirectory(mount, req.ToParent)
	e.invalidateNegative(plan.ReadKeys...)
	e.invalidateNegative(plan.MutateKeys...)
	e.invalidateDirPages(req.Mount, req.FromParent, req.ToParent)
	return nil
}

// RenameReplace atomically publishes the source dentry at the destination name,
// replacing an existing non-directory destination dentry when present. It is the
// artifact publish primitive: no subtree handoff or durability barrier is
// emitted, and all namespace/index mutations commit in one KV transaction.
func (e *Executor) RenameReplace(ctx context.Context, req model.RenameReplaceRequest) (model.RenameReplaceResult, error) {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return model.RenameReplaceResult{}, err
	}
	mount := mountRecord.Identity()
	program, err := compile.CompileRenameReplaceProgram(req, mount)
	if err != nil {
		return model.RenameReplaceResult{}, err
	}
	delta := program.Compiled.Delta
	if err := e.requireSameAuthority(ctx, req.Mount, req.FromParent, req.ToParent); err != nil {
		return model.RenameReplaceResult{}, err
	}
	if err := e.admitVisibleAuthority(ctx, delta); err != nil {
		return model.RenameReplaceResult{}, err
	}
	plan := delta.Plan
	move := renameMoveFromRenameReplace(req, mount)
	var result model.RenameReplaceResult
	if err := e.withTxnRetry(ctx, func(startVersion, commitVersion uint64) error {
		nextResult, mutations, err := e.prepareRenameReplaceMutations(ctx, plan, move, startVersion)
		if err != nil {
			return fmt.Errorf("prepare rename replace: %w", err)
		}
		if err := e.mutateWithoutAtomicOnePhase(ctx, plan.Kind, plan.PrimaryKey, mutations, startVersion, commitVersion); err != nil {
			return fmt.Errorf("commit rename replace: %w", err)
		}
		result = nextResult
		return nil
	}, delta.Authority); err != nil {
		return model.RenameReplaceResult{}, err
	}
	e.forgetVisibleEmptyDirectory(mount, req.ToParent)
	e.invalidateNegative(plan.ReadKeys...)
	e.invalidateNegative(plan.MutateKeys...)
	e.invalidateDirPages(req.Mount, req.FromParent, req.ToParent)
	return result, nil
}

// RenameSubtree moves the subtree root dentry from source to destination.
// Descendants follow through inode parent links rather than key rewrites.
func (e *Executor) RenameSubtree(ctx context.Context, req model.RenameSubtreeRequest) error {
	mountRecord, err := e.resolveActiveMount(ctx, req.Mount)
	if err != nil {
		return err
	}
	mount := mountRecord.Identity()
	program, err := compile.CompileRenameSubtreeProgram(req, mount)
	if err != nil {
		return err
	}
	delta := program.Compiled.Delta
	if err := e.admitVisibleAuthority(ctx, delta); err != nil {
		return err
	}
	plan := delta.Plan
	authorityRoot := mountRecord.RootInode
	if e.subtrees != nil && authorityRoot == 0 {
		return model.ErrInvalidInodeID
	}
	var movedSize uint64
	var movedInode bool
	var committedAt uint64
	var handoffStarted bool
	move := renameMoveFromRenameSubtree(req, mount)
	if err := e.withTxnRetry(ctx, func(startVersion, commitVersion uint64) error {
		mutations, _, err := e.prepareRenameMutations(ctx, plan, move, startVersion, &movedSize, &movedInode)
		if err != nil {
			return err
		}
		if err := e.startSubtreeHandoff(ctx, req.Mount, authorityRoot, commitVersion); err != nil {
			return err
		}
		handoffStarted = true
		actualCommitVersion, mutationErr := e.runner.MutateAtCommit(ctx, plan.PrimaryKey, mutations, startVersion, commitVersion, e.lockTTL)
		// Subtree handoff start publishes a rooted predecessor frontier before the
		// data mutation runs. That external frontier must be the same commit_ts
		// used by the data transaction; otherwise concurrent handoffs can observe a
		// later completed frontier and reject the older pending handoff.
		// Once StartSubtreeHandoff is rooted, a Mutate error may still be
		// ambiguous with respect to primary commit. Complete closes the rooted
		// pending state; at worst this advances an empty era rather than leaving
		// an unrecoverable handoff.
		completeErr := e.completeSubtreeHandoff(ctx, req.Mount, authorityRoot, actualCommitVersion)
		if mutationErr != nil {
			if completeErr != nil {
				return errors.Join(mutationErr, fmt.Errorf("complete subtree handoff: %w", completeErr))
			}
			return mutationErr
		}
		if completeErr != nil {
			return completeErr
		}
		committedAt = actualCommitVersion
		return nil
	}, delta.Authority); err != nil {
		return err
	}
	if handoffStarted && committedAt == 0 {
		return errSubtreeHandoffWithoutFrontier
	}
	// Only the subtree root dentry moves; descendants follow inode parent links.
	// Invalidate both old and new dentry keys plus the two parent directory
	// epochs so negative and materialized directory-page caches cannot serve the
	// pre-rename view.
	e.forgetVisibleEmptyDirectory(mount, req.ToParent)
	e.invalidateNegative(plan.ReadKeys...)
	e.invalidateNegative(plan.MutateKeys...)
	e.invalidateDirPages(req.Mount, req.FromParent, req.ToParent)
	return nil
}

func (e *Executor) prepareRenameReplaceMutations(ctx context.Context, plan layout.OperationPlan, move renameMove, startVersion uint64) (model.RenameReplaceResult, []*kvrpcpb.Mutation, error) {
	sourceDentry, err := e.readDentry(ctx, plan.ReadKeys[0], startVersion)
	if err != nil {
		return model.RenameReplaceResult{}, nil, fmt.Errorf("source dentry: %w", err)
	}
	if sourceDentry.Type == model.InodeTypeDirectory {
		return model.RenameReplaceResult{}, nil, model.ErrInvalidRequest
	}
	sourceInode, ok, err := e.readInode(ctx, move.identity, sourceDentry.Inode, startVersion)
	if err != nil {
		return model.RenameReplaceResult{}, nil, fmt.Errorf("source inode: %w", err)
	}
	if !ok {
		return model.RenameReplaceResult{}, nil, fmt.Errorf("%w: inode %d", model.ErrNotFound, sourceDentry.Inode)
	}
	if err := validateRenameReplaceDentryInode(sourceDentry, sourceInode); err != nil {
		return model.RenameReplaceResult{}, nil, err
	}
	if sourceInode.Type == model.InodeTypeDirectory {
		return model.RenameReplaceResult{}, nil, model.ErrInvalidRequest
	}
	fromParent, err := e.readDirectoryInode(ctx, move.identity, move.fromParent, startVersion)
	if err != nil {
		return model.RenameReplaceResult{}, nil, fmt.Errorf("from parent inode: %w", err)
	}

	result := model.RenameReplaceResult{}
	destinationExisted := false
	destinationDentry, err := e.readDentry(ctx, plan.ReadKeys[1], startVersion)
	if err == nil {
		destinationExisted = true
		result.Replaced = true
		result.OldDentry = destinationDentry
	} else if !errors.Is(err, model.ErrNotFound) {
		return model.RenameReplaceResult{}, nil, err
	} else {
		err = nil
	}

	var destinationInode model.InodeRecord
	if destinationExisted {
		if destinationDentry.Type == model.InodeTypeDirectory {
			return model.RenameReplaceResult{}, nil, model.ErrInvalidRequest
		}
		destinationInode, ok, err = e.readInode(ctx, move.identity, destinationDentry.Inode, startVersion)
		if err != nil {
			return model.RenameReplaceResult{}, nil, err
		}
		if !ok {
			return model.RenameReplaceResult{}, nil, fmt.Errorf("%w: inode %d", model.ErrNotFound, destinationDentry.Inode)
		}
		if err := validateRenameReplaceDentryInode(destinationDentry, destinationInode); err != nil {
			return model.RenameReplaceResult{}, nil, err
		}
		if destinationInode.Type == model.InodeTypeDirectory {
			return model.RenameReplaceResult{}, nil, model.ErrInvalidRequest
		}
		result.OldInode = destinationInode
	}
	toParent := fromParent
	if move.fromParent != move.toParent {
		toParent, err = e.readDirectoryInode(ctx, move.identity, move.toParent, startVersion)
		if err != nil {
			return model.RenameReplaceResult{}, nil, fmt.Errorf("to parent inode: %w", err)
		}
	}
	nextFromParent := fromParent.record
	nextToParent := toParent.record
	switch {
	case move.fromParent == move.toParent && destinationExisted:
		nextFromParent, err = decrementDirectoryChildCount(nextFromParent)
	case move.fromParent != move.toParent:
		nextFromParent, err = decrementDirectoryChildCount(nextFromParent)
		if err == nil && !destinationExisted {
			nextToParent, err = incrementDirectoryChildCount(nextToParent)
		}
	}
	if err != nil {
		return model.RenameReplaceResult{}, nil, err
	}

	replacementDentry := sourceDentry
	replacementDentry.Parent = move.toParent
	replacementDentry.Name = move.toName
	replacementValue, err := layout.EncodeDentryValue(replacementDentry)
	if err != nil {
		return model.RenameReplaceResult{}, nil, err
	}
	putDestination := &kvrpcpb.Mutation{
		Op:    kvrpcpb.Mutation_Put,
		Key:   cloneBytes(plan.MutateKeys[1]),
		Value: replacementValue,
	}
	if !destinationExisted {
		putDestination.AssertionNotExist = true
	}
	fromParentValue, err := layout.EncodeInodeValue(nextFromParent)
	if err != nil {
		return model.RenameReplaceResult{}, nil, err
	}
	toParentValue, err := layout.EncodeInodeValue(nextToParent)
	if err != nil {
		return model.RenameReplaceResult{}, nil, err
	}
	mutations := []*kvrpcpb.Mutation{
		{
			Op:  kvrpcpb.Mutation_Delete,
			Key: cloneBytes(plan.MutateKeys[0]),
		},
		putDestination,
	}
	if move.fromParent == move.toParent {
		mutations = append(mutations, &kvrpcpb.Mutation{Op: kvrpcpb.Mutation_Put, Key: cloneBytes(plan.MutateKeys[2]), Value: fromParentValue})
	} else {
		mutations = append(mutations,
			&kvrpcpb.Mutation{Op: kvrpcpb.Mutation_Put, Key: cloneBytes(plan.MutateKeys[2]), Value: fromParentValue},
			&kvrpcpb.Mutation{Op: kvrpcpb.Mutation_Put, Key: cloneBytes(plan.MutateKeys[3]), Value: toParentValue},
		)
	}

	quotaChanges := make([]QuotaChange, 0, 3)
	if move.fromParent != move.toParent {
		quotaChanges = append(quotaChanges,
			QuotaChange{Mount: move.mount, MountKeyID: move.identity.MountKeyID, Scope: move.fromParent, Bytes: -inodeSizeDelta(sourceInode.Size), Inodes: -1},
			QuotaChange{Mount: move.mount, MountKeyID: move.identity.MountKeyID, Scope: move.toParent, Bytes: inodeSizeDelta(sourceInode.Size), Inodes: 1},
		)
	}
	if destinationExisted {
		destinationInodeKey, err := layout.EncodeInodeKey(move.identity, destinationInode.Inode)
		if err != nil {
			return model.RenameReplaceResult{}, nil, err
		}
		if destinationInode.Inode == sourceInode.Inode && destinationInode.LinkCount <= 1 {
			return model.RenameReplaceResult{}, nil, model.ErrInvalidValue
		}
		if destinationInode.LinkCount <= 1 {
			result.OldInodeDeleted = true
			mutations = append(mutations, &kvrpcpb.Mutation{
				Op:  kvrpcpb.Mutation_Delete,
				Key: cloneBytes(destinationInodeKey),
			})
		} else {
			destinationInode.LinkCount--
			destinationValue, err := layout.EncodeInodeValue(destinationInode)
			if err != nil {
				return model.RenameReplaceResult{}, nil, err
			}
			mutations = append(mutations, &kvrpcpb.Mutation{
				Op:    kvrpcpb.Mutation_Put,
				Key:   cloneBytes(destinationInodeKey),
				Value: destinationValue,
			})
		}
		quotaChanges = append(quotaChanges, QuotaChange{
			Mount:      move.mount,
			MountKeyID: move.identity.MountKeyID,
			Scope:      move.toParent,
			Bytes:      -inodeSizeDelta(result.OldInode.Size),
			Inodes:     -1,
		})
	}
	if len(quotaChanges) > 0 {
		quotaMutations, err := e.reserveQuota(ctx, quotaChanges, startVersion)
		if err != nil {
			return model.RenameReplaceResult{}, nil, err
		}
		mutations = append(mutations, quotaMutations...)
	}
	return result, mutations, nil
}

func validateRenameReplaceDentryInode(dentry model.DentryRecord, inode model.InodeRecord) error {
	if dentry.Inode != inode.Inode {
		return fmt.Errorf("%w: dentry inode=%d value inode=%d", model.ErrInvalidValue, dentry.Inode, inode.Inode)
	}
	if dentry.Type != inode.Type {
		return fmt.Errorf("%w: dentry type=%s inode type=%s", model.ErrInvalidValue, dentry.Type, inode.Type)
	}
	return nil
}

func (e *Executor) prepareRenameMutations(ctx context.Context, plan layout.OperationPlan, move renameMove, startVersion uint64, movedSize *uint64, movedInode *bool) ([]*kvrpcpb.Mutation, []*kvrpcpb.AtomicPredicate, error) {
	record, err := e.readDentry(ctx, plan.ReadKeys[0], startVersion)
	if err != nil {
		return nil, nil, err
	}
	sourceDentryValue, err := layout.EncodeDentryValue(record)
	if err != nil {
		return nil, nil, err
	}
	if _, err := e.readDentry(ctx, plan.ReadKeys[1], startVersion); err == nil {
		return nil, nil, model.ErrExists
	} else if !errors.Is(err, model.ErrNotFound) {
		return nil, nil, err
	}
	record.Parent = move.toParent
	record.Name = move.toName
	value, err := layout.EncodeDentryValue(record)
	if err != nil {
		return nil, nil, err
	}
	*movedSize = 0
	*movedInode = false
	if inode, ok, err := e.readInode(ctx, move.identity, record.Inode, startVersion); err != nil {
		return nil, nil, err
	} else if ok {
		*movedSize = inode.Size
		*movedInode = true
	}
	fromParent, err := e.readDirectoryInode(ctx, move.identity, move.fromParent, startVersion)
	if err != nil {
		return nil, nil, err
	}
	toParent := fromParent
	if move.fromParent != move.toParent {
		nextFrom, err := decrementDirectoryChildCount(fromParent.record)
		if err != nil {
			return nil, nil, err
		}
		fromParent.record = nextFrom
		toParent, err = e.readDirectoryInode(ctx, move.identity, move.toParent, startVersion)
		if err != nil {
			return nil, nil, err
		}
		nextTo, err := incrementDirectoryChildCount(toParent.record)
		if err != nil {
			return nil, nil, err
		}
		toParent.record = nextTo
	}
	fromParentValue, err := layout.EncodeInodeValue(fromParent.record)
	if err != nil {
		return nil, nil, err
	}
	toParentValue, err := layout.EncodeInodeValue(toParent.record)
	if err != nil {
		return nil, nil, err
	}
	mutations := []*kvrpcpb.Mutation{
		{
			Op:  kvrpcpb.Mutation_Delete,
			Key: cloneBytes(plan.MutateKeys[0]),
		},
		{
			Op:                kvrpcpb.Mutation_Put,
			Key:               cloneBytes(plan.MutateKeys[1]),
			Value:             value,
			AssertionNotExist: true,
		},
	}
	if move.fromParent == move.toParent {
		mutations = append(mutations, &kvrpcpb.Mutation{Op: kvrpcpb.Mutation_Put, Key: cloneBytes(plan.MutateKeys[2]), Value: fromParentValue})
	} else {
		mutations = append(mutations,
			&kvrpcpb.Mutation{Op: kvrpcpb.Mutation_Put, Key: cloneBytes(plan.MutateKeys[2]), Value: fromParentValue},
			&kvrpcpb.Mutation{Op: kvrpcpb.Mutation_Put, Key: cloneBytes(plan.MutateKeys[3]), Value: toParentValue},
		)
	}
	if *movedInode {
		quotaMutations, err := e.reserveQuota(ctx, []QuotaChange{
			{Mount: move.mount, MountKeyID: move.identity.MountKeyID, Scope: move.fromParent, Bytes: -inodeSizeDelta(*movedSize), Inodes: -1},
			{Mount: move.mount, MountKeyID: move.identity.MountKeyID, Scope: move.toParent, Bytes: inodeSizeDelta(*movedSize), Inodes: 1},
		}, startVersion)
		if err != nil {
			return nil, nil, err
		}
		mutations = append(mutations, quotaMutations...)
	}
	predicates := []*kvrpcpb.AtomicPredicate{
		atomicValueEquals(plan.ReadKeys[0], sourceDentryValue),
		atomicNotExists(plan.ReadKeys[1]),
		atomicValueEquals(fromParent.key, fromParent.value),
	}
	if move.fromParent != move.toParent {
		predicates = append(predicates, atomicValueEquals(toParent.key, toParent.value))
	}
	return mutations, predicates, nil
}

func (e *Executor) startSubtreeHandoff(ctx context.Context, mount model.MountID, root model.InodeID, frontier uint64) error {
	if e == nil || e.subtrees == nil || mount == "" || root == 0 || frontier == 0 {
		return nil
	}
	return e.subtrees.StartSubtreeHandoff(ctx, mount, root, frontier)
}

func (e *Executor) completeSubtreeHandoff(ctx context.Context, mount model.MountID, root model.InodeID, frontier uint64) error {
	if e == nil || e.subtrees == nil || mount == "" || root == 0 || frontier == 0 {
		return nil
	}
	return e.subtrees.CompleteSubtreeHandoff(ctx, mount, root, frontier)
}

func (e *Executor) requireSameAuthority(ctx context.Context, mount model.MountID, fromParent, toParent model.InodeID) error {
	if e == nil || e.authorities == nil {
		return nil
	}
	same, err := e.authorities.SameAuthority(ctx, mount, fromParent, toParent)
	if err != nil {
		return err
	}
	if !same {
		return model.ErrCrossAuthorityRename
	}
	return nil
}
