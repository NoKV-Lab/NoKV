// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package artifact

import (
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
)

const (
	defaultFileMode        uint32 = 0o644
	defaultDirectoryMode   uint32 = 0o755
	defaultStageNamePrefix        = ".nokv-stage-"
)

// Options configures an artifact namespace store.
type Options struct {
	Namespace NamespaceClient
	BodyStore BodyStore
	Mount     fsmeta.MountID

	// RootInode is the artifact namespace root. Zero uses fsmeta.RootInode.
	RootInode fsmeta.InodeID

	FileMode      uint32
	DirectoryMode uint32

	// StageNamePrefix is used for short-lived hidden entries before final
	// fsmeta rename. Empty uses ".nokv-stage-".
	StageNamePrefix string

	Clock func() time.Time
}

func (opts Options) validate() error {
	if opts.Namespace == nil {
		return errNamespaceRequired
	}
	if opts.BodyStore == nil {
		return errBodyStoreRequired
	}
	if opts.Mount == "" {
		return errMountRequired
	}
	return nil
}

func (opts Options) rootInode() fsmeta.InodeID {
	if opts.RootInode != 0 {
		return opts.RootInode
	}
	return fsmeta.RootInode
}

func (opts Options) fileMode() uint32 {
	if opts.FileMode != 0 {
		return opts.FileMode
	}
	return defaultFileMode
}

func (opts Options) directoryMode() uint32 {
	if opts.DirectoryMode != 0 {
		return opts.DirectoryMode
	}
	return defaultDirectoryMode
}

func (opts Options) stageNamePrefix() string {
	if opts.StageNamePrefix != "" {
		return opts.StageNamePrefix
	}
	return defaultStageNamePrefix
}

func (opts Options) clock() func() time.Time {
	if opts.Clock != nil {
		return opts.Clock
	}
	return time.Now
}
