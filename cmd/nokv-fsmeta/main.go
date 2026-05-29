// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"expvar"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	perasfsmeta "github.com/feichai0017/NoKV/experimental/peras/adapters/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/feichai0017/NoKV/fsmeta/observe"
	fsmetalocal "github.com/feichai0017/NoKV/fsmeta/runtime/local"
	fsmetaraftstore "github.com/feichai0017/NoKV/fsmeta/runtime/raftstore"
	fsmetaserver "github.com/feichai0017/NoKV/fsmeta/server"
	metricspkg "github.com/feichai0017/NoKV/metrics"
	"github.com/feichai0017/NoKV/storage/wal"
	"google.golang.org/grpc"
)

var (
	exit                 = os.Exit
	listen               = net.Listen
	signalNotify         = signal.Notify
	openRaftstoreRuntime = fsmetaraftstore.Open
	openLocalRuntime     = fsmetalocal.Open
)

type fsmetaBackend string

const (
	fsmetaBackendRaftstore fsmetaBackend = "raftstore"
	fsmetaBackendLocal     fsmetaBackend = "local"
)

type fsmetaServerRuntime struct {
	executor       fsmetaserver.Executor
	watcher        observe.Watcher
	snapshot       observe.SnapshotPublisher
	close          func() error
	publishStats   func()
	contractLog    string
	startupSummary string
}

func fatalf(format string, args ...any) {
	log.Printf(format, args...)
	exit(1)
}

func defaultPerasHolderID() string {
	if hostname, err := os.Hostname(); err == nil {
		hostname = strings.TrimSpace(hostname)
		if hostname != "" {
			return "fsmeta/" + hostname
		}
	}
	return "fsmeta/default"
}

func main() {
	var (
		addr                            = flag.String("addr", "127.0.0.1:8090", "listen address for FSMetadata gRPC server")
		backendName                     = flag.String("backend", string(fsmetaBackendRaftstore), "fsmeta storage backend: raftstore|local")
		coordAddr                       = flag.String("coordinator-addr", "", "coordinator gRPC endpoint used for TSO, routing, and store discovery")
		metricsAddr                     = flag.String("metrics-addr", "", "optional HTTP address to expose /debug/vars expvar endpoint")
		localWorkDir                    = flag.String("local-work-dir", "", "embedded DB work directory when --backend=local")
		localMountID                    = flag.String("local-mount-id", "default", "single mount id admitted by --backend=local")
		localMountKeyID                 = flag.Uint64("local-mount-key-id", 1, "single mount key id admitted by --backend=local")
		localRootInode                  = flag.Uint64("local-root-inode", uint64(model.RootInode), "root inode for --backend=local")
		localNegCache                   = flag.Bool("local-negative-cache", true, "enable the slab-backed negative dentry cache when --backend=local; use --local-negative-cache=false to disable")
		localDirPageCache               = flag.Bool("local-dirpage-cache", true, "enable the slab-backed ReadDirPlus page cache when --backend=local; use --local-dirpage-cache=false to disable")
		negCacheDir                     = flag.String("negative-cache-dir", "", "optional slab directory for persistent negative dentry cache")
		dirPageDir                      = flag.String("dirpage-cache-dir", "", "optional slab directory for ReadDirPlus page cache")
		affinityBuckets                 = flag.Int("affinity-buckets", layout.DefaultAffinityBucketCount, "fsmeta placement bucket count used to choose Create inode IDs")
		lockTTL                         = flag.Duration("lock-ttl", 0, "Percolator primary-lock TTL for fsmeta mutations; zero uses the fsmeta default")
		sessionCleanupInterval          = flag.Duration("session-cleanup-interval", 30*time.Second, "interval for expired write-session cleanup; choose about half the smallest expected session TTL; negative disables")
		sessionCleanupLimit             = flag.Uint("session-cleanup-limit", 0, "maximum session records scanned per mount per cleanup pass; zero uses fsmeta default")
		experimentalPeras               = flag.Bool("experimental-peras", false, "enable the experimental Peras visible-commit runtime for --backend=raftstore")
		perasHolderID                   = flag.String("peras-holder-id", "", "Peras holder id; empty derives a stable local holder id")
		visibleAuthorityTTL             = flag.Duration("peras-authority-ttl", 0, "Peras authority grant TTL; zero uses runtime default")
		segmentWitnessStores            = flag.String("peras-witness-stores", "", "comma-separated store IDs used as Peras witnesses; empty uses all UP stores")
		segmentWitnessQuorum            = flag.Int("peras-witness-quorum", 0, "Peras witness quorum; zero uses majority")
		perasSegmentWitnessRetries      = flag.Int("peras-segment-witness-retries", 3, "Peras segment witness retries for transient authority lag")
		perasSegmentWitnessRetryBackoff = flag.Duration("peras-segment-witness-retry-backoff", 20*time.Millisecond, "Peras segment witness retry backoff")
		perasSegmentBatchSize           = flag.Int("peras-segment-batch-size", 0, "Peras pending visible operations that trigger background flush; zero uses runtime default")
		perasAdmissionPendingLimit      = flag.Int("peras-admission-pending-limit", 0, "Peras pending visible operations allowed before foreground commits wait for drain; zero uses runtime default")
		perasSegmentMaxReplayMutations  = flag.Int("peras-segment-max-replay-mutations", 0, "Peras replay mutations per installed segment; zero uses runtime default")
		perasSegmentCatalogRouteBudget  = flag.Int("peras-segment-catalog-route-budget", 0, "Peras catalog install routes per segment; zero uses runtime default")
		perasSegmentInstallParallelism  = flag.Int("peras-segment-install-parallelism", 0, "Peras segment installs per flush; zero uses GOMAXPROCS")
		perasSegmentFlushParallelism    = flag.Int("peras-segment-flush-parallelism", 0, "Peras flush batches prepared concurrently; zero follows install parallelism")
		perasSegmentFlushEvery          = flag.Duration("peras-segment-flush-every", 0, "Peras opportunistic segment flush interval; zero uses runtime default")
		perasBackgroundFlushTimeout     = flag.Duration("peras-background-flush-timeout", 0, "timeout for opportunistic Peras background segment install; zero uses runtime default")
		perasBackgroundErrorBackoff     = flag.Duration("peras-background-error-backoff", 0, "backoff after failed opportunistic Peras background segment install; zero uses runtime default")
		perasVisibleLogDir              = flag.String("peras-visible-log-dir", "peras-visible-log", "local WAL directory for holder visible acknowledgements")
		perasVisibleLogPolicy           = flag.String("peras-visible-log-policy", "flushed", "holder visible WAL sync policy: flushed|fsync-batched|fsync|buffered")
	)
	flag.Parse()
	backend, err := parseFSMetaBackend(*backendName)
	if err != nil {
		fatalf("parse backend: %v", err)
		return
	}
	if *lockTTL < 0 {
		fatalf("lock-ttl must be non-negative")
		return
	}
	if *sessionCleanupLimit > uint(model.MaxSessionExpireLimit) {
		fatalf("session-cleanup-limit exceeds maximum %d", model.MaxSessionExpireLimit)
		return
	}
	if *visibleAuthorityTTL < 0 || *perasSegmentWitnessRetryBackoff < 0 || *perasSegmentWitnessRetries < 0 || *segmentWitnessQuorum < 0 ||
		*perasSegmentBatchSize < 0 || *perasAdmissionPendingLimit < 0 || *perasSegmentMaxReplayMutations < 0 || *perasSegmentCatalogRouteBudget < 0 || *perasSegmentInstallParallelism < 0 || *perasSegmentFlushParallelism < 0 || *perasSegmentFlushEvery < 0 ||
		*perasBackgroundFlushTimeout < 0 || *perasBackgroundErrorBackoff < 0 {
		fatalf("peras options must be non-negative")
		return
	}
	perasStoreIDs, err := parseUintList(*segmentWitnessStores)
	if err != nil {
		fatalf("parse peras-witness-stores: %v", err)
		return
	}
	visibleLogDurability, err := parsePerasVisibleLogPolicy(*perasVisibleLogPolicy)
	if err != nil {
		fatalf("parse peras-visible-log-policy: %v", err)
		return
	}
	var localMount model.MountIdentity
	if backend == fsmetaBackendLocal {
		localMount, err = localMountIdentity(*localMountID, *localMountKeyID)
		if err != nil {
			fatalf("parse local mount: %v", err)
			return
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var extension fsmetaraftstore.Extension
	if *experimentalPeras {
		holderID := strings.TrimSpace(*perasHolderID)
		if holderID == "" {
			holderID = defaultPerasHolderID()
		}
		extension = perasfsmeta.NewExtension(perasfsmeta.Config{
			HolderID:                   holderID,
			AuthorityTTL:               *visibleAuthorityTTL,
			WitnessStoreIDs:            perasStoreIDs,
			WitnessQuorum:              *segmentWitnessQuorum,
			SegmentWitnessRetries:      *perasSegmentWitnessRetries,
			SegmentWitnessRetryBackoff: *perasSegmentWitnessRetryBackoff,
			SegmentBatchSize:           *perasSegmentBatchSize,
			AdmissionPendingLimit:      *perasAdmissionPendingLimit,
			SegmentMaxReplayMutations:  *perasSegmentMaxReplayMutations,
			SegmentCatalogRouteBudget:  *perasSegmentCatalogRouteBudget,
			SegmentInstallParallelism:  *perasSegmentInstallParallelism,
			SegmentFlushParallelism:    *perasSegmentFlushParallelism,
			SegmentFlushEvery:          *perasSegmentFlushEvery,
			BackgroundFlushTimeout:     *perasBackgroundFlushTimeout,
			BackgroundErrorBackoff:     *perasBackgroundErrorBackoff,
			VisibleLogDir:              *perasVisibleLogDir,
			VisibleLogDurability:       visibleLogDurability,
		})
	}

	rt, err := openConfiguredRuntime(ctx, configuredRuntimeOptions{
		Backend: backend,
		Raftstore: fsmetaraftstore.Options{
			CoordinatorAddr:        *coordAddr,
			NegativeCacheDir:       *negCacheDir,
			DirPageCacheDir:        *dirPageDir,
			AffinityBuckets:        *affinityBuckets,
			LockTTL:                *lockTTL,
			SessionCleanupInterval: *sessionCleanupInterval,
			SessionCleanupLimit:    uint32(*sessionCleanupLimit),
			Extension:              extension,
		},
		Local: fsmetalocal.Options{
			WorkDir:           *localWorkDir,
			Mount:             localMount,
			RootInode:         model.InodeID(*localRootInode),
			LockTTL:           *lockTTL,
			NegativeCacheMode: localCacheMode(*localNegCache),
			NegativeCacheDir:  *negCacheDir,
			DirPageCacheMode:  localCacheMode(*localDirPageCache),
			DirPageCacheDir:   *dirPageDir,
		},
	})
	if err != nil {
		fatalf("open fsmeta runtime: %v", err)
		return
	}
	defer func() {
		if err := rt.close(); err != nil {
			log.Printf("close fsmeta runtime: %v", err)
		}
	}()
	// From here on, prefer logging + return so the deferred Close runs and
	// the raftstore + coordinator clients are released.
	ln, err := listen("tcp", *addr)
	if err != nil {
		log.Printf("listen: %v", err)
		return
	}
	defer func() { _ = ln.Close() }()

	srv := grpc.NewServer()
	registerFSMetadataServer(srv, rt)
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()

	if *metricsAddr != "" {
		rt.publishStats()
		mln, err := metricspkg.StartExpvarServer(*metricsAddr)
		if err != nil {
			log.Printf("start metrics endpoint: %v", err)
			srv.GracefulStop()
			return
		}
		defer func() {
			if mln != nil {
				_ = mln.Close()
			}
		}()
		log.Printf("expvar metrics listening on http://%s/debug/vars", mln.Addr().String())
	}

	log.Printf("NoKV FSMetadata server listening on %s", ln.Addr().String())
	log.Print(rt.startupSummary)
	log.Print(rt.contractLog)

	sigCh := make(chan os.Signal, 1)
	signalNotify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		log.Printf("received signal %s, shutting down", sig)
	case err := <-errCh:
		if err != nil {
			log.Printf("serve loop exited: %v", err)
		}
	}
	cancel()
	srv.GracefulStop()
}

type configuredRuntimeOptions struct {
	Backend   fsmetaBackend
	Raftstore fsmetaraftstore.Options
	Local     fsmetalocal.Options
}

func openConfiguredRuntime(ctx context.Context, opts configuredRuntimeOptions) (*fsmetaServerRuntime, error) {
	switch opts.Backend {
	case fsmetaBackendRaftstore:
		rt, err := openRaftstoreRuntime(ctx, opts.Raftstore)
		if err != nil {
			return nil, err
		}
		return raftstoreServerRuntime(rt), nil
	case fsmetaBackendLocal:
		rt, err := openLocalRuntime(ctx, opts.Local)
		if err != nil {
			return nil, err
		}
		return localServerRuntime(rt, opts.Local), nil
	default:
		return nil, fmt.Errorf("unsupported fsmeta backend %q", opts.Backend)
	}
}

func raftstoreServerRuntime(rt *fsmetaraftstore.Runtime) *fsmetaServerRuntime {
	return &fsmetaServerRuntime{
		executor: rt.Executor,
		watcher:  rt.Watcher,
		snapshot: rt.SnapshotPublisher,
		close:    rt.Close,
		publishStats: func() {
			publishExpvarOnce("nokv_fsmeta_executor", expvar.Func(func() any { return rt.Executor.Stats() }))
			if stats, ok := rt.Watcher.(interface{ Stats() map[string]any }); ok {
				publishExpvarOnce("nokv_fsmeta_watch", expvar.Func(func() any { return stats.Stats() }))
			}
			if stats, ok := rt.MountResolver.(interface{ Stats() map[string]any }); ok {
				publishExpvarOnce("nokv_fsmeta_mount", expvar.Func(func() any { return stats.Stats() }))
			}
			if stats, ok := rt.QuotaResolver.(interface{ Stats() map[string]any }); ok {
				publishExpvarOnce("nokv_fsmeta_quota", expvar.Func(func() any { return stats.Stats() }))
			}
			for _, stats := range rt.ExtensionStats {
				if stats.Name == "" || stats.Snapshot == nil {
					continue
				}
				publishExpvarOnce(stats.Name, expvar.Func(func() any { return stats.Snapshot() }))
			}
			if rt.SessionCleaner != nil {
				publishExpvarOnce("nokv_fsmeta_sessions", expvar.Func(func() any { return rt.SessionCleaner.Stats() }))
			}
		},
		startupSummary: "fsmeta backend: raftstore",
		contractLog:    "fsmeta commit contract: raftstore backend uses durable MVCC by default; experimental extensions must be enabled explicitly",
	}
}

func localServerRuntime(rt *fsmetalocal.Runtime, opts fsmetalocal.Options) *fsmetaServerRuntime {
	contractLog := "fsmeta commit contract: local backend uses one embedded MVCC store; successful metadata writes are durable after the local WAL/apply group completes"
	return &fsmetaServerRuntime{
		executor: rt.Executor,
		watcher:  rt.Watcher,
		snapshot: rt.Snapshots,
		close:    rt.Close,
		publishStats: func() {
			publishExpvarOnce("nokv_fsmeta_executor", expvar.Func(func() any { return localExecutorStats(rt) }))
			publishExpvarOnce("nokv_fsmeta_local_runner", expvar.Func(func() any { return rt.Runner.Stats() }))
			publishExpvarOnce("nokv_fsmeta_quota", expvar.Func(func() any { return rt.Quotas.Stats() }))
			publishExpvarOnce("nokv_fsmeta_watch", expvar.Func(func() any { return rt.Watcher.Stats() }))
			publishExpvarOnce("nokv_fsmeta_local_snapshot", expvar.Func(func() any { return rt.Snapshots.Stats() }))
		},
		startupSummary: fmt.Sprintf("fsmeta backend: local work_dir=%q mount=%q mount_key_id=%d", opts.WorkDir, opts.Mount.MountID, opts.Mount.MountKeyID),
		contractLog:    contractLog,
	}
}

func localExecutorStats(rt *fsmetalocal.Runtime) map[string]any {
	if rt == nil || rt.Executor == nil {
		return map[string]any{"commit_contract": localCommitContractStats()}
	}
	stats := rt.Executor.Stats()
	stats["commit_contract"] = localCommitContractStats()
	return stats
}

func localCommitContractStats() map[string]any {
	return map[string]any{
		"default_write_path":        "local_mvcc",
		"successful_write_boundary": "durable",
		"durable_boundary":          "embedded_mvcc_apply_group",
	}
}

func registerFSMetadataServer(reg grpc.ServiceRegistrar, rt *fsmetaServerRuntime) {
	opts := make([]fsmetaserver.Option, 0, 2)
	if rt.watcher != nil {
		opts = append(opts, fsmetaserver.WithWatcher(rt.watcher))
	}
	if rt.snapshot != nil {
		opts = append(opts, fsmetaserver.WithSnapshotPublisher(rt.snapshot))
	}
	fsmetaserver.Register(reg, rt.executor, opts...)
}

func parseFSMetaBackend(value string) (fsmetaBackend, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", string(fsmetaBackendRaftstore):
		return fsmetaBackendRaftstore, nil
	case string(fsmetaBackendLocal):
		return fsmetaBackendLocal, nil
	default:
		return "", fmt.Errorf("invalid fsmeta backend %q", value)
	}
}

func localMountIdentity(mountID string, mountKeyID uint64) (model.MountIdentity, error) {
	id := model.MountID(strings.TrimSpace(mountID))
	if id == "" {
		return model.MountIdentity{}, model.ErrInvalidMountID
	}
	if mountKeyID == 0 {
		return model.MountIdentity{}, model.ErrInvalidMountID
	}
	return model.MountIdentity{MountID: id, MountKeyID: model.MountKeyID(mountKeyID)}, nil
}

func localCacheMode(enabled bool) fsmetalocal.CacheMode {
	if enabled {
		return fsmetalocal.CacheModeEnabled
	}
	return fsmetalocal.CacheModeDisabled
}

func publishExpvarOnce(name string, value expvar.Var) {
	if expvar.Get(name) != nil {
		return
	}
	expvar.Publish(name, value)
}

func parseUintList(value string) ([]uint64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	parts := strings.Split(value, ",")
	out := make([]uint64, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, err := strconv.ParseUint(part, 10, 64)
		if err != nil {
			return nil, err
		}
		if id != 0 {
			out = append(out, id)
		}
	}
	return out, nil
}

func parsePerasVisibleLogPolicy(value string) (wal.DurabilityPolicy, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "flushed":
		return wal.DurabilityFlushed, nil
	case "fsync-batched", "fsync_batched", "batched":
		return wal.DurabilityFsyncBatched, nil
	case "fsync":
		return wal.DurabilityFsync, nil
	case "buffered":
		return wal.DurabilityBuffered, nil
	default:
		return 0, fmt.Errorf("invalid peras visible WAL sync policy %q", value)
	}
}
