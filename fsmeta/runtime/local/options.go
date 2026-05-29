// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package local

import (
	"time"

	"github.com/feichai0017/NoKV/fsmeta/model"
	localdb "github.com/feichai0017/NoKV/local"
)

// Options configures the embedded fsmeta runtime.
type Options struct {
	// DB reuses an already-open embedded database. When nil, Open creates and
	// owns a DB from DBOptions/WorkDir.
	DB *localdb.DB

	// DBOptions configures a runtime-owned DB. Open copies these options before
	// filling fsmeta-specific storage settings.
	DBOptions *localdb.Options

	// WorkDir is required when DB is nil. It overrides DBOptions.WorkDir.
	WorkDir string

	// Mount is the single local fsmeta mount admitted by this runtime.
	Mount model.MountIdentity

	// RootInode is the mount root inode. Zero uses model.RootInode.
	RootInode model.InodeID

	// LockTTL overrides fsmeta/exec's lock TTL, in the same units expected by
	// fsmeta/exec.WithLockTTL. Local MVCC commits are one-phase, but keeping the
	// option lets callers share runtime configuration.
	LockTTL time.Duration

	// Clock overrides fsmeta/exec's wall clock for write-session expiry.
	Clock func() time.Time
}

func (opts Options) rootInode() model.InodeID {
	if opts.RootInode != 0 {
		return opts.RootInode
	}
	return model.RootInode
}

func (opts Options) validate() error {
	if opts.Mount.MountID == "" || opts.Mount.MountKeyID == 0 {
		return errMountRequired
	}
	if opts.DB == nil && opts.WorkDir == "" && (opts.DBOptions == nil || opts.DBOptions.WorkDir == "") {
		return errWorkDirRequired
	}
	return nil
}

func localDBOptions(opts Options) *localdb.Options {
	cfg := localdb.NewDefaultOptions()
	if opts.DBOptions != nil {
		copy := *opts.DBOptions
		cfg = &copy
	}
	if opts.WorkDir != "" {
		cfg.WorkDir = opts.WorkDir
	}
	return cfg
}
