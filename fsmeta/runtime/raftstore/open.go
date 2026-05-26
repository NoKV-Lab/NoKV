// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"context"
	"fmt"
	"sync"
	"time"

	coordclient "github.com/feichai0017/NoKV/coordinator/client"
	"github.com/feichai0017/NoKV/engine/slab/dirpage"
	"github.com/feichai0017/NoKV/engine/slab/negativecache"
	"github.com/feichai0017/NoKV/fsmeta"
	fsmetaexec "github.com/feichai0017/NoKV/fsmeta/exec"
	fsmetawatch "github.com/feichai0017/NoKV/fsmeta/exec/watch"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/feichai0017/NoKV/raftstore/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultDialTimeout             = 3 * time.Second
	defaultRouteRetryAttempts      = 32
	defaultRouteUnavailableBackoff = 50 * time.Millisecond
	defaultRegionErrorRetryBackoff = 10 * time.Millisecond
	defaultTransportRetryBackoff   = 10 * time.Millisecond
	defaultLockResolveRetryBackoff = 10 * time.Millisecond
)

// Options configures the default NoKV-backed fsmeta runtime.
type Options struct {
	CoordinatorAddr string
	DialOptions     []grpc.DialOption
	DialTimeout     time.Duration
	MountTTL        time.Duration
	QuotaTTL        time.Duration

	// MonitorInterval controls rooted lifecycle stream reconnect backoff.
	// Zero uses the package default; negative disables the monitor.
	MonitorInterval time.Duration

	// SessionCleanupInterval controls stale writer-session cleanup. Zero uses
	// the package default; negative disables automatic cleanup. Set this to
	// roughly half of the smallest expected writer-session TTL when lease
	// takeover latency matters; expired sessions may remain visible until the next
	// cleanup pass.
	SessionCleanupInterval time.Duration

	// SessionCleanupLimit bounds one stale-session cleanup pass per mount. Zero
	// uses model.DefaultSessionExpireLimit.
	SessionCleanupLimit uint32

	// LockTTL bounds Percolator primary-lock liveness for fsmeta mutations.
	// Zero uses fsmeta/exec's default. Set this above the p99 raft apply and
	// coordinator TSO window; otherwise read-side lock resolution can roll back
	// a live transaction that is only delayed in the commit path.
	LockTTL time.Duration

	// NegativeCacheDir enables the slab-backed negative dentry cache. Empty
	// disables it. This is a Derived cache: authoritative reads still fall
	// back to raftstore plus txn/percolator on miss or invalidation.
	NegativeCacheDir string

	// DirPageCacheDir enables the slab-backed ReadDirPlus page cache. Empty
	// disables it. Pages are derived from authoritative LSM reads and are
	// invalidated by fsmeta mutations.
	DirPageCacheDir string

	// AffinityBuckets is the fsmeta key-placement bucket count used when
	// choosing Create inode IDs. The local engine receives only generic key
	// shape hints; this value belongs to fsmeta placement policy.
	AffinityBuckets int

	// Extension attaches an optional experimental runtime to fsmeta assembly.
	// Nil keeps the raftstore path on the stable durable MVCC contract.
	Extension Extension
}

// Runtime is a complete fsmeta runtime backed by the NoKV raftstore. It owns
// every client and goroutine it creates; Close releases all of them.
type Runtime struct {
	Executor          *fsmetaexec.Executor
	Watcher           fsmeta.Watcher
	SnapshotPublisher fsmeta.SnapshotPublisher
	MountResolver     fsmetaexec.MountResolver
	QuotaResolver     fsmetaexec.QuotaResolver
	SessionCleaner    interface{ Stats() map[string]any }
	ExtensionStats    []ExtensionStats

	close func() error
	once  sync.Once
}

// Close releases watch streams, raftstore connections, the mount monitor, and
// coordinator client resources owned by the runtime.
func (r *Runtime) Close() error {
	if r == nil || r.close == nil {
		return nil
	}
	var err error
	r.once.Do(func() {
		err = r.close()
	})
	return err
}

// Open builds an fsmeta runtime backed by NoKV's raftstore and coordinator.
// It is the embedded-user entry point; lower-level NewRunner remains available
// for tests and custom wiring.
func Open(ctx context.Context, opts Options) (*Runtime, error) {
	if opts.CoordinatorAddr == "" {
		return nil, errCoordinatorAddrRequired
	}
	if opts.SessionCleanupLimit > model.MaxSessionExpireLimit {
		return nil, errSessionCleanupLimitExceeded
	}
	if opts.LockTTL < 0 {
		return nil, errLockTTLInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	dialTimeout := opts.DialTimeout
	if dialTimeout <= 0 {
		dialTimeout = defaultDialTimeout
	}
	dialOpts := dialOptions(opts.DialOptions)
	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	coord, err := coordclient.NewGRPCClient(dialCtx, opts.CoordinatorAddr, dialOpts...)
	cancel()
	if err != nil {
		return nil, fmt.Errorf("dial coordinator: %w", err)
	}

	kv, err := client.New(client.Config{
		Context:        ctx,
		StoreResolver:  coord,
		RegionResolver: coord,
		DialOptions:    dialOpts,
		Retry: client.RetryPolicy{
			MaxAttempts:                 defaultRouteRetryAttempts,
			RouteUnavailableBackoff:     defaultRouteUnavailableBackoff,
			TransportUnavailableBackoff: defaultTransportRetryBackoff,
			RegionErrorBackoff:          defaultRegionErrorRetryBackoff,
			LockResolveBackoff:          defaultLockResolveRetryBackoff,
		},
	})
	if err != nil {
		_ = coord.Close()
		return nil, fmt.Errorf("dial raftstore: %w", err)
	}
	runner, err := NewRunner(kv, coord)
	if err != nil {
		_ = kv.Close()
		_ = coord.Close()
		return nil, fmt.Errorf("init runner: %w", err)
	}
	buckets := opts.AffinityBuckets
	if buckets == 0 {
		buckets = defaultInodeAffinityBuckets
	}
	inodes, err := NewShardAffineInodeAllocator(coord, buckets)
	if err != nil {
		_ = kv.Close()
		_ = coord.Close()
		return nil, fmt.Errorf("init inode allocator: %w", err)
	}

	mountTTL := opts.MountTTL
	if mountTTL == 0 {
		mountTTL = defaultMountTTL
	}
	mounts := &mountCache{coord: coord, ttl: mountTTL}
	quotaTTL := opts.QuotaTTL
	if quotaTTL == 0 {
		quotaTTL = defaultQuotaTTL
	}
	quotas := &quotaCache{coord: coord, ttl: quotaTTL}
	pub := rootPublisher{coord: coord}
	router := fsmetawatch.NewRouter()
	execOpts := []fsmetaexec.Option{
		fsmetaexec.WithInodeAllocator(inodes),
		fsmetaexec.WithMountResolver(mounts),
		fsmetaexec.WithSubtreeAuthorityResolver(mounts),
		fsmetaexec.WithQuotaResolver(quotas),
		fsmetaexec.WithSubtreeHandoffPublisher(pub),
	}
	var extensionAttachments []*ExtensionAttachment
	var extensionStats []ExtensionStats
	if opts.Extension != nil {
		attachment, err := opts.Extension.Attach(ctx, ExtensionContext{
			Coordinator: coord,
			KV:          kv,
			Runner:      runner,
			WatchRouter: router,
			DialOptions: dialOpts,
		})
		if err != nil {
			_ = kv.Close()
			_ = coord.Close()
			return nil, err
		}
		if attachment != nil {
			extensionAttachments = append(extensionAttachments, attachment)
			execOpts = append(execOpts, attachment.ExecutorOptions...)
			extensionStats = append(extensionStats, attachment.Stats...)
		}
	}
	if opts.LockTTL > 0 {
		execOpts = append(execOpts, fsmetaexec.WithLockTTL(uint64((opts.LockTTL+time.Millisecond-1)/time.Millisecond)))
	}
	var negPersist *negativecache.Persistence
	if opts.NegativeCacheDir != "" {
		neg, persist, err := negativecache.OpenWithPersistence(
			negativecache.Config{
				GroupKeyFn: func(k []byte) []byte { return k },
			},
			negativecache.PersistConfig{
				Dir: opts.NegativeCacheDir,
			},
		)
		if err != nil {
			_ = closeExtensionAttachments(extensionAttachments)
			_ = kv.Close()
			_ = coord.Close()
			return nil, fmt.Errorf("init negative cache: %w", err)
		}
		negPersist = persist
		execOpts = append(execOpts, fsmetaexec.WithNegativeCache(neg))
	}
	var dirPages *dirpage.Cache
	if opts.DirPageCacheDir != "" {
		dirPages, err = dirpage.Open(dirpage.Config{
			Dir: opts.DirPageCacheDir,
		})
		if err != nil {
			_ = closeExtensionAttachments(extensionAttachments)
			_ = kv.Close()
			_ = coord.Close()
			return nil, fmt.Errorf("init dirpage cache: %w", err)
		}
		execOpts = append(execOpts, fsmetaexec.WithDirPageCache(dirPages))
	}
	exec, err := fsmetaexec.New(runner, execOpts...)
	if err != nil {
		if dirPages != nil {
			_ = dirPages.Close()
		}
		_ = closeExtensionAttachments(extensionAttachments)
		_ = kv.Close()
		_ = coord.Close()
		return nil, fmt.Errorf("init executor: %w", err)
	}

	source, err := StartRemoteSource(ctx, coord, router, dialOpts...)
	if err != nil {
		if dirPages != nil {
			_ = dirPages.Close()
		}
		_ = closeExtensionAttachments(extensionAttachments)
		_ = kv.Close()
		_ = coord.Close()
		return nil, fmt.Errorf("init watch source: %w", err)
	}

	var mon *monitor
	if opts.MonitorInterval >= 0 {
		mon = startMonitor(ctx, coord, router, mounts, quotas, pub, opts.MonitorInterval)
	}
	var sessions *sessionCleaner
	if opts.SessionCleanupInterval >= 0 {
		sessions = startSessionCleaner(ctx, coord, exec, opts.SessionCleanupInterval, opts.SessionCleanupLimit)
	}

	rt := &Runtime{
		Executor:          exec,
		Watcher:           watcher{Router: router, source: source, mounts: mounts},
		SnapshotPublisher: pub,
		MountResolver:     mounts,
		QuotaResolver:     quotas,
		SessionCleaner:    sessions,
		ExtensionStats:    extensionStats,
	}
	rt.close = func() error {
		var first error
		if sessions != nil {
			if err := sessions.Close(); err != nil {
				first = err
			}
		}
		if mon != nil {
			if err := mon.Close(); err != nil {
				first = err
			}
		}
		if err := source.Close(); err != nil && first == nil {
			first = err
		}
		if negPersist != nil {
			if _, err := negPersist.Snapshot(); err != nil && first == nil {
				first = err
			}
		}
		if dirPages != nil {
			if err := dirPages.Close(); err != nil && first == nil {
				first = err
			}
		}
		if err := closeExtensionAttachments(extensionAttachments); err != nil && first == nil {
			first = err
		}
		if err := kv.Close(); err != nil && first == nil {
			first = err
		}
		if err := coord.Close(); err != nil && first == nil {
			first = err
		}
		return first
	}
	return rt, nil
}

func dialOptions(opts []grpc.DialOption) []grpc.DialOption {
	out := make([]grpc.DialOption, 0, len(opts)+2)
	if len(opts) > 0 {
		out = append(out, opts...)
	} else {
		out = append(out, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	out = append(out, grpc.WithDefaultCallOptions(
		grpc.MaxCallRecvMsgSize(client.DefaultMaxMessageBytes),
		grpc.MaxCallSendMsgSize(client.DefaultMaxMessageBytes),
	))
	return out
}
