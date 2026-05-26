// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package local

import (
	"path/filepath"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/model"
	localdb "github.com/feichai0017/NoKV/local"
)

// CacheMode controls whether the embedded runtime opens an optional slab cache
// (negative dentry cache or ReadDirPlus page cache). The zero value enables
// the cache when WorkDir is available.
type CacheMode uint8

const (
	CacheModeDefault CacheMode = iota
	CacheModeEnabled
	CacheModeDisabled
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

	// NegativeCacheMode controls the slab-backed negative dentry cache. The
	// zero value enables the cache and persists it under WorkDir/neg-cache.
	NegativeCacheMode CacheMode
	// NegativeCacheDir overrides the persistence directory for the negative
	// dentry cache when set. Empty falls back to WorkDir/neg-cache.
	NegativeCacheDir string

	// DirPageCacheMode controls the slab-backed ReadDirPlus page cache. The
	// zero value enables the cache and persists it under WorkDir/dir-pages.
	DirPageCacheMode CacheMode
	// DirPageCacheDir overrides the persistence directory for the ReadDirPlus
	// page cache when set. Empty falls back to WorkDir/dir-pages.
	DirPageCacheDir string
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
	if !validCacheMode(opts.NegativeCacheMode) || !validCacheMode(opts.DirPageCacheMode) {
		return errInvalidCacheMode
	}
	return nil
}

func validCacheMode(mode CacheMode) bool {
	switch mode {
	case CacheModeDefault, CacheModeEnabled, CacheModeDisabled:
		return true
	default:
		return false
	}
}

func (opts Options) negativeCacheEnabled() bool {
	return opts.NegativeCacheMode != CacheModeDisabled
}

func (opts Options) dirPageCacheEnabled() bool {
	return opts.DirPageCacheMode != CacheModeDisabled
}

func localNegativeCacheDir(opts Options) string {
	if !opts.negativeCacheEnabled() {
		return ""
	}
	if opts.NegativeCacheDir != "" {
		return opts.NegativeCacheDir
	}
	workDir := localWorkDir(opts)
	if workDir == "" {
		return ""
	}
	return filepath.Join(workDir, "neg-cache")
}

func localDirPageCacheDir(opts Options) string {
	if !opts.dirPageCacheEnabled() {
		return ""
	}
	if opts.DirPageCacheDir != "" {
		return opts.DirPageCacheDir
	}
	workDir := localWorkDir(opts)
	if workDir == "" {
		return ""
	}
	return filepath.Join(workDir, "dir-pages")
}

func localWorkDir(opts Options) string {
	if opts.WorkDir != "" {
		return opts.WorkDir
	}
	if opts.DBOptions != nil {
		return opts.DBOptions.WorkDir
	}
	return ""
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
	cfg.UserKeyShapeExtractor = fsmeta.MountAtomicUserKeyShape
	return cfg
}
