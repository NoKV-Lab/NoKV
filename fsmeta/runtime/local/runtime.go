// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package local

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/feichai0017/NoKV/engine/slab/dirpage"
	"github.com/feichai0017/NoKV/engine/slab/negativecache"
	"github.com/feichai0017/NoKV/fsmeta"
	fsmetaexec "github.com/feichai0017/NoKV/fsmeta/exec"
	"github.com/feichai0017/NoKV/fsmeta/model"
	localdb "github.com/feichai0017/NoKV/local"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
)

// Runtime is a complete fsmeta runtime backed by one embedded local.DB.
type Runtime struct {
	DB            *localdb.DB
	Runner        *Runner
	Executor      *fsmetaexec.Executor
	Mounts        *MountCatalog
	Quotas        *QuotaLedger
	Watcher       *Watcher
	Snapshots     *SnapshotRegistry
	NegativeCache *negativecache.Cache
	DirPageCache  *dirpage.Cache

	closeDB    bool
	negPersist *negativecache.Persistence
	once       sync.Once
}

// Open builds a local fsmeta runtime without coordinator, root, or raftstore.
func Open(ctx context.Context, opts Options) (*Runtime, error) {
	if err := opts.validate(); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	db := opts.DB
	closeDB := false
	if db == nil {
		var err error
		db, err = localdb.Open(localDBOptions(opts))
		if err != nil {
			return nil, err
		}
		closeDB = true
	}
	runner, err := NewRunner(db)
	if err != nil {
		if closeDB {
			_ = db.Close()
		}
		return nil, err
	}
	mounts := NewMountCatalog(MountConfig{
		Mount:     opts.Mount,
		RootInode: opts.rootInode(),
	})
	if err := bootstrapRootInode(ctx, runner, mounts.Admission(), opts.Clock); err != nil {
		if closeDB {
			_ = db.Close()
		}
		return nil, err
	}
	watcher := NewWatcher(mounts)
	runner.SetMutationObserver(watcher)
	quotas := NewQuotaLedger()
	snapshots, err := OpenSnapshotRegistry(ctx, runner, mounts.Admission().Identity())
	if err != nil {
		if closeDB {
			_ = db.Close()
		}
		return nil, err
	}
	inodes, err := NewInodeAllocator(db, opts.Mount)
	if err != nil {
		if closeDB {
			_ = db.Close()
		}
		return nil, err
	}
	execOpts := []fsmetaexec.Option{
		fsmetaexec.WithMountResolver(mounts),
		fsmetaexec.WithSubtreeAuthorityResolver(mounts),
		fsmetaexec.WithSubtreeHandoffPublisher(mounts),
		fsmetaexec.WithInodeAllocator(inodes),
		fsmetaexec.WithQuotaResolver(quotas),
	}
	negCache, negPersist, err := openLocalNegativeCache(opts)
	if err != nil {
		if closeDB {
			_ = db.Close()
		}
		return nil, err
	}
	if negCache != nil {
		execOpts = append(execOpts, fsmetaexec.WithNegativeCache(negCache))
	}
	dirPages, err := openLocalDirPageCache(opts)
	if err != nil {
		if closeDB {
			_ = db.Close()
		}
		return nil, err
	}
	if dirPages != nil {
		execOpts = append(execOpts, fsmetaexec.WithDirPageCache(dirPages))
	}
	if opts.LockTTL > 0 {
		execOpts = append(execOpts, fsmetaexec.WithLockTTL(uint64((opts.LockTTL+time.Millisecond-1)/time.Millisecond)))
	}
	if opts.Clock != nil {
		execOpts = append(execOpts, fsmetaexec.WithClock(opts.Clock))
	}
	executor, err := fsmetaexec.New(runner, execOpts...)
	if err != nil {
		if dirPages != nil {
			_ = dirPages.Close()
		}
		if closeDB {
			_ = db.Close()
		}
		return nil, err
	}
	return &Runtime{
		DB:            db,
		Runner:        runner,
		Executor:      executor,
		Mounts:        mounts,
		Quotas:        quotas,
		Watcher:       watcher,
		Snapshots:     snapshots,
		NegativeCache: negCache,
		DirPageCache:  dirPages,
		closeDB:       closeDB,
		negPersist:    negPersist,
	}, nil
}

func openLocalNegativeCache(opts Options) (*negativecache.Cache, *negativecache.Persistence, error) {
	dir := localNegativeCacheDir(opts)
	if dir == "" {
		return nil, nil, nil
	}
	cache, persist, err := negativecache.OpenWithPersistence(
		negativecache.Config{
			GroupKeyFn: func(k []byte) []byte { return k },
		},
		negativecache.PersistConfig{Dir: dir},
	)
	if err != nil {
		return nil, nil, fmt.Errorf("init local negative cache: %w", err)
	}
	return cache, persist, nil
}

func openLocalDirPageCache(opts Options) (*dirpage.Cache, error) {
	dir := localDirPageCacheDir(opts)
	if dir == "" {
		return nil, nil
	}
	cache, err := dirpage.Open(dirpage.Config{Dir: dir})
	if err != nil {
		return nil, fmt.Errorf("init local dirpage cache: %w", err)
	}
	return cache, nil
}

// Close releases the runtime-owned DB. Caller-owned DB handles are left open.
func (r *Runtime) Close() error {
	if r == nil {
		return nil
	}
	var err error
	r.once.Do(func() {
		if r.DirPageCache != nil {
			err = errors.Join(err, r.DirPageCache.Close())
		}
		if r.negPersist != nil {
			if _, snapErr := r.negPersist.Snapshot(); snapErr != nil {
				err = errors.Join(err, snapErr)
			}
		}
		if r.closeDB && r.DB != nil {
			err = errors.Join(err, r.DB.Close())
		}
	})
	return err
}

func bootstrapRootInode(ctx context.Context, runner *Runner, mount fsmetaexec.MountAdmission, now func() time.Time) error {
	key, err := fsmeta.EncodeInodeKey(mount.Identity(), mount.RootInode)
	if err != nil {
		return err
	}
	readVersion, err := runner.ReserveTimestamp(ctx, 1)
	if err != nil {
		return err
	}
	if _, ok, err := runner.Get(ctx, key, readVersion); err != nil || ok {
		return err
	}
	ts := time.Now()
	if now != nil {
		ts = now()
	}
	value, err := fsmeta.EncodeInodeValue(model.InodeRecord{
		Inode:         mount.RootInode,
		Type:          model.InodeTypeDirectory,
		LinkCount:     1,
		CreatedUnixNs: ts.UnixNano(),
		UpdatedUnixNs: ts.UnixNano(),
	})
	if err != nil {
		return err
	}
	startVersion, err := runner.ReserveTimestamp(ctx, 2)
	if err != nil {
		return err
	}
	_, err = runner.Mutate(ctx, key, []*kvrpcpb.Mutation{{
		Op:                kvrpcpb.Mutation_Put,
		Key:               key,
		Value:             value,
		AssertionNotExist: true,
	}}, startVersion, startVersion+1, 0)
	return err
}
