// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"context"

	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
)

type directoryInodeSnapshot struct {
	key    []byte
	record model.InodeRecord
	value  []byte
}

func (e *Executor) readDirectoryInode(ctx context.Context, mount model.MountIdentity, inode model.InodeID, version uint64) (directoryInodeSnapshot, error) {
	key, err := layout.EncodeInodeKey(mount, inode)
	if err != nil {
		return directoryInodeSnapshot{}, err
	}
	value, ok, err := e.runner.Get(ctx, key, version)
	if err != nil {
		return directoryInodeSnapshot{}, err
	}
	if !ok {
		return directoryInodeSnapshot{}, model.ErrNotFound
	}
	record, err := layout.DecodeInodeValue(value)
	if err != nil {
		return directoryInodeSnapshot{}, err
	}
	if record.Type != model.InodeTypeDirectory {
		return directoryInodeSnapshot{}, model.ErrInvalidRequest
	}
	return directoryInodeSnapshot{
		key:    key,
		record: record,
		value:  cloneBytes(value),
	}, nil
}

func readVisibleDirectoryInode(view *visibleReadView, mount model.MountIdentity, inode model.InodeID) (model.InodeRecord, error) {
	record, ok, err := view.readInode(mount, inode)
	if err != nil {
		return model.InodeRecord{}, err
	}
	if !ok {
		return model.InodeRecord{}, model.ErrNotFound
	}
	if record.Type != model.InodeTypeDirectory {
		return model.InodeRecord{}, model.ErrInvalidRequest
	}
	return record, nil
}

func incrementDirectoryChildCount(record model.InodeRecord) (model.InodeRecord, error) {
	if record.Type != model.InodeTypeDirectory || record.ChildCount == ^uint64(0) {
		return model.InodeRecord{}, model.ErrInvalidRequest
	}
	record.ChildCount++
	return record, nil
}

func decrementDirectoryChildCount(record model.InodeRecord) (model.InodeRecord, error) {
	if record.Type != model.InodeTypeDirectory || record.ChildCount == 0 {
		return model.InodeRecord{}, model.ErrInvalidRequest
	}
	record.ChildCount--
	return record, nil
}
