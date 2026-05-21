// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"context"

	"github.com/feichai0017/NoKV/fsmeta"
)

type directoryInodeSnapshot struct {
	key    []byte
	record fsmeta.InodeRecord
	value  []byte
}

func (e *Executor) readDirectoryInode(ctx context.Context, mount fsmeta.MountIdentity, inode fsmeta.InodeID, version uint64) (directoryInodeSnapshot, error) {
	key, err := fsmeta.EncodeInodeKey(mount, inode)
	if err != nil {
		return directoryInodeSnapshot{}, err
	}
	value, ok, err := e.runner.Get(ctx, key, version)
	if err != nil {
		return directoryInodeSnapshot{}, err
	}
	if !ok {
		return directoryInodeSnapshot{}, fsmeta.ErrNotFound
	}
	record, err := fsmeta.DecodeInodeValue(value)
	if err != nil {
		return directoryInodeSnapshot{}, err
	}
	if record.Type != fsmeta.InodeTypeDirectory {
		return directoryInodeSnapshot{}, fsmeta.ErrInvalidRequest
	}
	return directoryInodeSnapshot{
		key:    key,
		record: record,
		value:  cloneBytes(value),
	}, nil
}

func readVisibleDirectoryInode(view *visibleReadView, mount fsmeta.MountIdentity, inode fsmeta.InodeID) (fsmeta.InodeRecord, error) {
	record, ok, err := view.readInode(mount, inode)
	if err != nil {
		return fsmeta.InodeRecord{}, err
	}
	if !ok {
		return fsmeta.InodeRecord{}, fsmeta.ErrNotFound
	}
	if record.Type != fsmeta.InodeTypeDirectory {
		return fsmeta.InodeRecord{}, fsmeta.ErrInvalidRequest
	}
	return record, nil
}

func incrementDirectoryChildCount(record fsmeta.InodeRecord) (fsmeta.InodeRecord, error) {
	if record.Type != fsmeta.InodeTypeDirectory || record.ChildCount == ^uint64(0) {
		return fsmeta.InodeRecord{}, fsmeta.ErrInvalidRequest
	}
	record.ChildCount++
	return record, nil
}

func decrementDirectoryChildCount(record fsmeta.InodeRecord) (fsmeta.InodeRecord, error) {
	if record.Type != fsmeta.InodeTypeDirectory || record.ChildCount == 0 {
		return fsmeta.InodeRecord{}, fsmeta.ErrInvalidRequest
	}
	record.ChildCount--
	return record, nil
}
