// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package file

import (
	"context"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	"github.com/feichai0017/NoKV/storage/vfs"
)

// store is a single-owner file-backed VirtualLog. Callers must serialize
// mutation through the owning backend; this type is not a general concurrent
// log object.
type store struct {
	checkpt fileCheckpointStore
	log     fileEventLog
}

func NewStore(fs vfs.FS, workdir string) rootstorage.VirtualLog {
	return store{
		checkpt: fileCheckpointStore{fs: fs, workdir: workdir},
		log:     fileEventLog{fs: fs, workdir: workdir},
	}
}

// forwarding-ok: store satisfies rootstorage.Storage via the wrapped checkpoint helper.
func (s store) LoadCheckpoint() (rootstorage.Checkpoint, error) {
	return s.checkpt.LoadCheckpoint()
}

// forwarding-ok: store satisfies rootstorage.Storage via the wrapped checkpoint helper.
func (s store) SaveCheckpoint(checkpoint rootstorage.Checkpoint) error {
	return s.checkpt.SaveCheckpoint(checkpoint)
}

func (s store) ReadCommitted(requestedOffset int64) (rootstorage.CommittedTail, error) {
	checkpoint, err := s.checkpt.LoadCheckpoint()
	if err != nil {
		return rootstorage.CommittedTail{}, err
	}
	startOffset := max(requestedOffset, checkpoint.TailOffset)
	tail, err := s.log.ReadCommitted(startOffset)
	if err != nil {
		return rootstorage.CommittedTail{}, err
	}
	tail.RequestedOffset = requestedOffset
	tail.StartOffset = startOffset
	return tail, nil
}

func (s store) AppendCommitted(_ context.Context, records ...rootstorage.CommittedEvent) (int64, error) {
	return s.log.AppendCommitted(records...)
}

// forwarding-ok: store satisfies rootstorage.Storage via the wrapped log helper.
func (s store) CompactCommitted(stream rootstorage.CommittedTail) error {
	return s.log.CompactCommitted(stream)
}

func (s store) InstallBootstrap(observed rootstorage.ObservedCommitted) error {
	observed = observed.Installable()
	if err := s.log.CompactCommitted(observed.Tail); err != nil {
		return err
	}
	return s.checkpt.SaveCheckpoint(observed.Checkpoint)
}

func (s store) Size() (int64, error) {
	return s.log.Size()
}
