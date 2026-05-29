// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/feichai0017/NoKV/config"
	coordclient "github.com/feichai0017/NoKV/coordinator/client"
	"github.com/feichai0017/NoKV/coordinator/storecontrol"
	perasraftstore "github.com/feichai0017/NoKV/experimental/peras/adapters/raftstore"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	local "github.com/feichai0017/NoKV/local"
	workdirmode "github.com/feichai0017/NoKV/local/workdir"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	myraft "github.com/feichai0017/NoKV/raft"
	raftclient "github.com/feichai0017/NoKV/raftstore/client"
	"github.com/feichai0017/NoKV/raftstore/kv"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	storemvcc "github.com/feichai0017/NoKV/raftstore/mvcc"
	"github.com/feichai0017/NoKV/raftstore/peer"
	"github.com/feichai0017/NoKV/raftstore/raftlog"
	serverpkg "github.com/feichai0017/NoKV/raftstore/server"
	snapshotpkg "github.com/feichai0017/NoKV/raftstore/snapshot"
	raftstorestats "github.com/feichai0017/NoKV/raftstore/stats"
	storepkg "github.com/feichai0017/NoKV/raftstore/store"
	"github.com/feichai0017/NoKV/storage/wal"
	"google.golang.org/grpc"
)

var notifyContext = signal.NotifyContext

func runServeCmd(w io.Writer, args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	workDir := fs.String("workdir", "", "database work directory")
	listenAddr := fs.String("addr", "127.0.0.1:20160", "gRPC listen address for NoKV + raft traffic")
	storeID := fs.Uint64("store-id", 0, "store ID assigned to this node")
	electionTick := fs.Int("election-tick", 10, "raft election tick")
	heartbeatTick := fs.Int("heartbeat-tick", 2, "raft heartbeat tick")
	maxMsgBytes := fs.Int("raft-max-msg-bytes", 1<<20, "raft max message bytes")
	maxInflight := fs.Int("raft-max-inflight", 256, "raft max inflight messages")
	raftTickInterval := fs.Duration("raft-tick-interval", 0, "interval between raft ticks (default 100ms)")
	raftDebugLog := fs.Bool("raft-debug-log", false, "enable verbose raft debug logging")
	coordAddr := fs.String("coordinator-addr", "", "coordinator gRPC endpoint for cluster mode (required)")
	configPath := fs.String("config", "", "optional raft configuration file used to resolve listen/workdir/store transport addresses")
	scope := fs.String("scope", "host", "scope for config-resolved addresses: host|docker")
	coordTimeout := fs.Duration("coordinator-timeout", 2*time.Second, "timeout for coordinator heartbeat RPCs")
	metricsAddr := fs.String("metrics-addr", "", "optional HTTP address to expose /debug/vars expvar endpoint")
	mvccGCPlanInterval := fs.Duration("mvcc-gc-plan-interval", 0, "interval for read-only MVCC GC planning; zero disables")
	mvccGCMaintenanceInterval := fs.Duration("mvcc-gc-maintenance-interval", 0, "interval for replicated MVCC GC maintenance; zero disables")
	mvccGCSafePointLag := fs.Uint64("mvcc-gc-safe-point-lag", 0, "TSO lag retained behind the coordinator timestamp before MVCC GC may reclaim versions")
	mvccGCTSOCacheTTL := fs.Duration("mvcc-gc-tso-cache-ttl", time.Minute, "maximum age of the last successful coordinator TSO reused for MVCC GC; zero disables cache")
	mvccGCTimeout := fs.Duration("mvcc-gc-timeout", 30*time.Second, "timeout for one MVCC GC maintenance pass")
	mvccGCBatchEntries := fs.Int("mvcc-gc-batch-entries", 0, "maximum MVCC GC tombstones per replicated maintenance batch")
	mvccGCMaxKeys := fs.Uint64("mvcc-gc-max-keys", 0, "maximum MVCC user keys scanned by one destructive maintenance pass; zero means unlimited")
	mvccGCResolveBatchLocks := fs.Int("mvcc-gc-resolve-batch-locks", 0, "maximum expired locks resolved per replicated maintenance batch")
	mvccGCResolveMaxLocks := fs.Uint64("mvcc-gc-resolve-max-locks", 0, "maximum MVCC locks scanned by one lock-resolution pass; zero means unlimited")
	mvccGCMetaRootAddr := fs.String("mvcc-gc-meta-root-addr", "", "metadata-root gRPC address for snapshot retention floors; config meta_root is used when empty")
	storageMaxBatchCount := fs.Int64("storage-max-batch-count", 0, "maximum internal entries accepted by one local storage batch; zero uses engine default")
	storageMaxBatchSize := fs.Int64("storage-max-batch-size", 0, "maximum bytes accepted by one local storage batch; zero uses engine default")
	experimentalSegmentWitness := fs.Bool("experimental-peras-witness", false, "enable the experimental Peras witness service and write fence")
	segmentWitnessWALPolicy := fs.String("peras-witness-wal-policy", "fsync-batched", "peras witness WAL sync policy: fsync-batched|fsync|flushed|buffered")
	var storeAddrFlags []string
	fs.Func("store-addr", "remote store transport mapping in the form storeID=address (repeatable)", func(value string) error {
		value = strings.TrimSpace(value)
		if value == "" {
			return fmt.Errorf("store address value cannot be empty")
		}
		storeAddrFlags = append(storeAddrFlags, value)
		return nil
	})
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return err
	}

	var cfg *config.File
	scopeNorm := strings.ToLower(strings.TrimSpace(*scope))
	if strings.TrimSpace(*configPath) != "" {
		if scopeNorm != "host" && scopeNorm != "docker" {
			return fmt.Errorf("invalid serve scope %q (expected host|docker)", *scope)
		}
		loaded, err := config.LoadFile(strings.TrimSpace(*configPath))
		if err != nil {
			return fmt.Errorf("serve load config %q: %w", strings.TrimSpace(*configPath), err)
		}
		if err := loaded.Validate(); err != nil {
			return fmt.Errorf("serve validate config %q: %w", strings.TrimSpace(*configPath), err)
		}
		cfg = loaded
	}

	if *storeID == 0 {
		return fmt.Errorf("--store-id is required")
	}
	if cfg != nil {
		scopeNorm := strings.ToLower(strings.TrimSpace(*scope))
		if !flagPassed(fs, "workdir") {
			if resolved := cfg.ResolveStoreWorkDir(*storeID, scopeNorm); resolved != "" {
				*workDir = resolved
			}
		}
		if !flagPassed(fs, "addr") {
			if resolved := cfg.ResolveStoreListenAddr(*storeID, scopeNorm); resolved != "" {
				*listenAddr = resolved
			}
		}
		if !flagPassed(fs, "coordinator-addr") {
			if resolved := cfg.ResolveCoordinatorAddr(scopeNorm); resolved != "" {
				*coordAddr = resolved
			}
		}
	}
	if *workDir == "" {
		return fmt.Errorf("--workdir is required")
	}
	if *electionTick <= 0 || *heartbeatTick <= 0 {
		return fmt.Errorf("heartbeat and election ticks must be > 0")
	}
	if *mvccGCPlanInterval < 0 || *mvccGCMaintenanceInterval < 0 || *mvccGCTimeout < 0 {
		return fmt.Errorf("mvcc-gc intervals and timeout must be non-negative")
	}
	if *mvccGCBatchEntries < 0 || *mvccGCResolveBatchLocks < 0 {
		return fmt.Errorf("mvcc-gc batch limits must be non-negative")
	}
	if *storageMaxBatchCount < 0 || *storageMaxBatchSize < 0 {
		return fmt.Errorf("storage batch limits must be non-negative")
	}
	var perasDurability wal.DurabilityPolicy
	if *experimentalSegmentWitness {
		var parseErr error
		perasDurability, parseErr = parseSegmentWitnessWALPolicy(*segmentWitnessWALPolicy)
		if parseErr != nil {
			return parseErr
		}
	}
	explicitStoreAddrs := make(map[uint64]string, len(storeAddrFlags))
	for _, mapping := range storeAddrFlags {
		parts := strings.SplitN(mapping, "=", 2)
		if len(parts) != 2 {
			return fmt.Errorf("invalid --store-addr value %q (want storeID=address)", mapping)
		}
		id, err := parseUint(parts[0])
		if err != nil {
			return fmt.Errorf("invalid store id in --store-addr %q: %w", mapping, err)
		}
		if id == 0 {
			return fmt.Errorf("invalid --store-addr value %q (store id must be > 0)", mapping)
		}
		addr := strings.TrimSpace(parts[1])
		if addr == "" {
			return fmt.Errorf("invalid --store-addr value %q (empty address)", mapping)
		}
		explicitStoreAddrs[id] = addr
	}
	if strings.TrimSpace(*coordAddr) == "" {
		return fmt.Errorf("--coordinator-addr is required (coordinator is the only scheduler/control-plane source)")
	}
	if _, err := validateServeMode(*workDir, *storeID); err != nil {
		return err
	}

	// Cluster mode only: route scheduler heartbeats and operations through the Coordinator.
	dialCtx, cancelDial := context.WithTimeout(context.Background(), 5*time.Second)
	coordCli, err := coordclient.NewGRPCClient(dialCtx, strings.TrimSpace(*coordAddr))
	cancelDial()
	if err != nil {
		return fmt.Errorf("dial coordinator %q: %w", *coordAddr, err)
	}
	defer func() { _ = coordCli.Close() }()

	txnClient, err := raftclient.New(raftclient.Config{
		RegionResolver: coordCli,
		StoreResolver:  coordCli,
	})
	if err != nil {
		return fmt.Errorf("create transaction lock resolver: %w", err)
	}
	defer func() { _ = txnClient.Close() }()

	var tsoSource *serveTSOSource
	var retentionSource *serveRootRetentionSource
	mvccGCEnabled := *mvccGCPlanInterval > 0 || *mvccGCMaintenanceInterval > 0
	if mvccGCEnabled {
		if *mvccGCSafePointLag == 0 {
			return fmt.Errorf("--mvcc-gc-safe-point-lag is required when MVCC GC is enabled")
		}
		if *mvccGCTSOCacheTTL < 0 {
			return fmt.Errorf("--mvcc-gc-tso-cache-ttl must be non-negative")
		}
		tsoSource = newServeTSOSource(coordCli, *coordTimeout, *mvccGCSafePointLag, *mvccGCTSOCacheTTL)
		rootCtx, cancelRoot := context.WithTimeout(context.Background(), 5*time.Second)
		retentionSource, err = newServeRootRetentionSource(rootCtx, cfg, scopeNorm, *mvccGCMetaRootAddr)
		cancelRoot()
		if err != nil {
			return fmt.Errorf("open MVCC GC snapshot-retention source: %w", err)
		}
		defer func() { _ = retentionSource.Close() }()
	}

	localMeta, err := localmeta.OpenLocalStore(*workDir, nil)
	if err != nil {
		return fmt.Errorf("open raftstore local metadata: %w", err)
	}
	defer func() {
		_ = localMeta.Close()
	}()

	opt := local.NewDefaultOptions()
	opt.WorkDir = *workDir
	if *storageMaxBatchCount > 0 {
		opt.MaxBatchCount = *storageMaxBatchCount
	}
	if *storageMaxBatchSize > 0 {
		opt.MaxBatchSize = *storageMaxBatchSize
	}
	opt.ControlLogPointerSnapshot = raftstorestats.ControlLogPointers(localMeta.DurableRaftPointerSnapshot)
	opt.AllowedModes = []workdirmode.Mode{
		workdirmode.ModeStandalone,
		workdirmode.ModeSeeded,
		workdirmode.ModeCluster,
	}
	db, err := local.Open(opt)
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	defer func() {
		_ = db.Close()
	}()

	var segmentWitness *perasraftstore.StoreWitnessRuntime
	if *experimentalSegmentWitness {
		segmentWitness, err = perasraftstore.StartStoreWitness(context.Background(), *storeID, coordCli, db, perasDurability)
		if err != nil {
			return err
		}
		defer func() { _ = segmentWitness.Close() }()
	}
	var writeFence kv.WriteFence
	var extraServices []func(grpc.ServiceRegistrar)
	if segmentWitness != nil {
		writeFence = segmentWitness.WriteFence()
		extraServices = append(extraServices, segmentWitness.RegisterGRPCService)
	}

	coordScheduler := storecontrol.NewClient(storecontrol.Config{
		Coordinator: coordCli,
		Timeout:     *coordTimeout,
	})

	server, err := serverpkg.NewNode(serverpkg.Config{
		Storage: serverpkg.Storage{
			MVCC: db,
			Raft: raftlog.NewDBLog(db),
		},
		Store: storepkg.Config{
			StoreID:    *storeID,
			ClientAddr: resolveStoreAdvertiseAddr(cfg, *storeID, scopeNorm),
			RaftAddr:   resolveStoreAdvertiseAddr(cfg, *storeID, scopeNorm),
			LocalMeta:  localMeta,
			WorkDir:    *workDir,
			Scheduler:  coordScheduler,
		},
		EnableRaftDebugLog: *raftDebugLog,
		RaftTickInterval:   *raftTickInterval,
		Raft: myraft.Config{
			ElectionTick:    *electionTick,
			HeartbeatTick:   *heartbeatTick,
			MaxSizePerMsg:   uint64(*maxMsgBytes),
			MaxInflightMsgs: *maxInflight,
			PreVote:         true,
		},
		MVCCMaintenance: serverpkg.MVCCMaintenanceConfig{
			Interval: *mvccGCMaintenanceInterval,
			Timeout:  *mvccGCTimeout,
			SafePoint: func() uint64 {
				if tsoSource == nil {
					return 0
				}
				return tsoSource.SafePoint()
			},
			CurrentTs: func() uint64 {
				if tsoSource == nil {
					return 0
				}
				return tsoSource.Current()
			},
			CurrentTime: func() uint64 {
				return uint64(time.Now().UnixMilli())
			},
			Retention: func() rootstate.SnapshotRetentionIndex {
				if retentionSource == nil {
					return rootstate.SnapshotRetentionIndex{}
				}
				return retentionSource.Retention()
			},
			Mount: layout.MountKeyResolver,
			Apply: storemvcc.ApplyOptions{
				BatchEntries: *mvccGCBatchEntries,
				MaxKeys:      *mvccGCMaxKeys,
			},
			ResolveLocks: storemvcc.ResolveLocksOptions{
				BatchLocks: *mvccGCResolveBatchLocks,
				MaxLocks:   *mvccGCResolveMaxLocks,
			},
			LockResolver:      txnClient,
			RunOrphanDefaults: *mvccGCMaintenanceInterval > 0,
			OrphanDefaults: storemvcc.OrphanDefaultOptions{
				BatchEntries: *mvccGCBatchEntries,
			},
		},
		MVCCGCPlan: serverpkg.MVCCGCPlanConfig{
			Interval: *mvccGCPlanInterval,
			SafePoint: func() uint64 {
				if tsoSource == nil {
					return 0
				}
				return tsoSource.SafePoint()
			},
			Retention: func() rootstate.SnapshotRetentionIndex {
				if retentionSource == nil {
					return rootstate.SnapshotRetentionIndex{}
				}
				return retentionSource.Retention()
			},
			Mount: layout.MountKeyResolver,
		},
		TransportAddr: *listenAddr,
		WriteFence:    writeFence,
		ExtraServices: extraServices,
	})
	if err != nil {
		return err
	}
	registerRuntimeStore(server.Store())
	defer unregisterRuntimeStore(server.Store())
	defer func() {
		_ = server.Close()
	}()
	installStorePercolatorExpvar()
	installStoreKVExpvar(server.KVStats)
	metricsLn, err := startExpvarServer(*metricsAddr)
	if err != nil {
		return fmt.Errorf("start serve metrics endpoint: %w", err)
	}
	if metricsLn != nil {
		defer func() { _ = metricsLn.Close() }()
	}

	transport := server.Transport()
	snapshot := localMeta.Snapshot()
	transportPeers, err := resolveTransportPeers(snapshot, *storeID, cfg, strings.ToLower(strings.TrimSpace(*scope)), explicitStoreAddrs)
	if err != nil {
		return err
	}
	for peerID, addr := range transportPeers {
		if strings.TrimSpace(addr) == "" {
			continue
		}
		transport.SetPeer(peerID, addr)
	}

	startedRegions, totalRegions, err := startStorePeers(server, serverpkg.Storage{
		MVCC: db,
		Raft: raftlog.NewDBLog(db),
	}, localMeta, *storeID, *electionTick, *heartbeatTick, *maxMsgBytes, *maxInflight)
	if err != nil {
		return err
	}
	if err := promoteClusterMode(*workDir, *storeID); err != nil {
		return fmt.Errorf("persist cluster mode: %w", err)
	}
	if totalRegions == 0 {
		_, _ = fmt.Fprintln(w, "Local peer catalog contains no regions; waiting for bootstrap")
		_, _ = fmt.Fprintln(w, "Serve lifecycle: bootstrap-wait (runtime topology will come from local metadata once seeded)")
	} else {
		_, _ = fmt.Fprintf(w, "Local peer catalog regions: %d, local peers started: %d\n", totalRegions, len(startedRegions))
		_, _ = fmt.Fprintln(w, "Serve lifecycle: restart-recover (runtime topology sourced from local metadata)")
		if missing := totalRegions - len(startedRegions); missing > 0 {
			_, _ = fmt.Fprintf(w, "Store %d not present in %d region(s)\n", *storeID, missing)
		}
		if len(startedRegions) > 0 {
			_, _ = fmt.Fprintln(w, "Sample regions:")
			for i, meta := range startedRegions {
				if i >= 5 {
					_, _ = fmt.Fprintf(w, "  ... (%d more)\n", len(startedRegions)-i)
					break
				}
				_, _ = fmt.Fprintf(w, "  - id=%d range=[%s,%s) peers=%s\n", meta.ID, formatKey(meta.StartKey, true), formatKey(meta.EndKey, false), formatPeers(meta.Peers))
			}
		}
	}

	_, _ = fmt.Fprintf(w, "StoreKV service listening on %s (store=%d)\n", server.Addr(), *storeID)
	if segmentWitness != nil {
		_, _ = fmt.Fprintf(w, "Peras witness enabled (wal_policy=%s)\n", strings.TrimSpace(*segmentWitnessWALPolicy))
	}
	if metricsLn != nil {
		_, _ = fmt.Fprintf(w, "Serve metrics endpoint listening on http://%s/debug/vars\n", metricsLn.Addr().String())
	}
	_, _ = fmt.Fprintf(w, "Serve mode: cluster (coordinator enabled, addr=%s)\n", strings.TrimSpace(*coordAddr))
	if len(storeAddrFlags) > 0 {
		_, _ = fmt.Fprintf(w, "Configured store address overrides: %s\n", strings.Join(storeAddrFlags, ", "))
	}
	_, _ = fmt.Fprintf(w, "coordinator heartbeat sink enabled: %s\n", strings.TrimSpace(*coordAddr))
	_, _ = fmt.Fprintln(w, "Press Ctrl+C to stop")

	ctx, cancel := notifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	<-ctx.Done()
	_, _ = fmt.Fprintln(w, "\nShutting down...")
	return nil
}

func resolveTransportPeers(snapshot map[uint64]localmeta.RegionMeta, localStoreID uint64, cfg *config.File, scope string, explicitStoreAddrs map[uint64]string) (map[uint64]string, error) {
	needed := collectRemotePeers(snapshot, localStoreID)
	if len(needed) == 0 {
		if len(explicitStoreAddrs) > 0 {
			return nil, fmt.Errorf("serve received --store-addr overrides but local metadata has no remote stores to resolve")
		}
		return nil, nil
	}
	out := make(map[uint64]string, len(needed))
	usedOverrides := make(map[uint64]struct{}, len(explicitStoreAddrs))
	for peerID, storeID := range needed {
		addr := strings.TrimSpace(explicitStoreAddrs[storeID])
		if addr != "" {
			usedOverrides[storeID] = struct{}{}
		} else if cfg != nil {
			addr = strings.TrimSpace(cfg.ResolveStoreAddr(storeID, scope))
		}
		if addr == "" {
			return nil, fmt.Errorf("serve missing transport address for remote store %d (peer %d): provide --config store address or --store-addr override", storeID, peerID)
		}
		out[peerID] = addr
	}
	for storeID := range explicitStoreAddrs {
		if _, ok := usedOverrides[storeID]; ok {
			continue
		}
		return nil, fmt.Errorf("serve unused --store-addr override for store %d: local metadata does not reference that remote store", storeID)
	}
	return out, nil
}

func resolveStoreAdvertiseAddr(cfg *config.File, storeID uint64, scope string) string {
	if cfg == nil || storeID == 0 {
		return ""
	}
	return strings.TrimSpace(cfg.ResolveStoreAddr(storeID, scope))
}

func collectRemotePeers(snapshot map[uint64]localmeta.RegionMeta, localStoreID uint64) map[uint64]uint64 {
	if len(snapshot) == 0 || localStoreID == 0 {
		return nil
	}
	out := make(map[uint64]uint64)
	for _, meta := range snapshot {
		for _, p := range meta.Peers {
			if p.StoreID == 0 || p.PeerID == 0 || p.StoreID == localStoreID {
				continue
			}
			out[p.PeerID] = p.StoreID
		}
	}
	return out
}

func validateServeMode(workDir string, storeID uint64) (workdirmode.State, error) {
	state, err := workdirmode.Read(workDir)
	if err != nil {
		return workdirmode.State{}, fmt.Errorf("read workdir mode: %w", err)
	}
	if state.StoreID != 0 && storeID != 0 && state.StoreID != storeID {
		return workdirmode.State{}, fmt.Errorf("serve store-id mismatch: workdir %q is bound to store %d, not store %d", workDir, state.StoreID, storeID)
	}
	return state, nil
}

func promoteClusterMode(workDir string, storeID uint64) error {
	state, err := workdirmode.Read(workDir)
	if err != nil {
		return err
	}
	if state.Mode == workdirmode.ModeCluster && state.StoreID == storeID {
		return nil
	}
	state.Mode = workdirmode.ModeCluster
	if state.StoreID == 0 {
		state.StoreID = storeID
	}
	state.RegionID = 0
	state.PeerID = 0
	return workdirmode.Write(workDir, state)
}

func startStorePeers(server *serverpkg.Node, storage serverpkg.Storage, localMeta *localmeta.Store, storeID uint64, electionTick, heartbeatTick, maxMsgBytes, maxInflight int) ([]localmeta.RegionMeta, int, error) {
	if server == nil || storage.MVCC == nil || storage.Raft == nil || localMeta == nil {
		return nil, 0, fmt.Errorf("raftstore: server, storage, or local metadata is nil")
	}
	snapshot := localMeta.Snapshot()
	total := len(snapshot)
	if total == 0 {
		return nil, 0, nil
	}

	store := server.Store()
	transport := server.Transport()
	ids := make([]uint64, 0, len(snapshot))
	for id := range snapshot {
		if id != 0 {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)

	var started []localmeta.RegionMeta
	for _, id := range ids {
		meta := snapshot[id]
		var peerID uint64
		for _, p := range meta.Peers {
			if p.StoreID == storeID {
				peerID = p.PeerID
				break
			}
		}
		if peerID == 0 {
			continue
		}
		peerStorage, err := storage.Raft.Open(meta.ID, localMeta)
		if err != nil {
			return nil, total, fmt.Errorf("raftstore: open peer storage for region %d: %w", meta.ID, err)
		}
		snapshotStore := snapshotpkg.NewStore(storage.MVCC)
		snapshotApply := func(payload []byte) (localmeta.RegionMeta, error) {
			result, err := snapshotStore.ImportSnapshot(payload)
			if err != nil {
				return localmeta.RegionMeta{}, err
			}
			return result.Descriptor.Region, nil
		}
		cfg := &peer.Config{
			RaftConfig: peer.EnableLeaseRead(myraft.Config{
				ID:              peerID,
				ElectionTick:    electionTick,
				HeartbeatTick:   heartbeatTick,
				MaxSizePerMsg:   uint64(maxMsgBytes),
				MaxInflightMsgs: maxInflight,
				PreVote:         true,
			}),
			Transport:      transport,
			Apply:          kv.NewEntryApplier(storage.MVCC),
			SnapshotExport: snapshotStore.ExportSnapshot,
			SnapshotApply:  snapshotApply,
			Storage:        peerStorage,
			GroupID:        meta.ID,
			Region:         localmeta.CloneRegionMetaPtr(&meta),
		}
		var bootstrapPeers []myraft.Peer
		for _, p := range meta.Peers {
			bootstrapPeers = append(bootstrapPeers, myraft.Peer{ID: p.PeerID})
		}
		if _, err := store.StartPeer(cfg, bootstrapPeers); err != nil {
			return started, total, fmt.Errorf("raftstore: start peer for region %d: %w", meta.ID, err)
		}
		started = append(started, meta)
	}
	return started, total, nil
}

func formatKey(key []byte, isStart bool) string {
	if len(key) == 0 {
		if isStart {
			return "-inf"
		}
		return "+inf"
	}
	return fmt.Sprintf("%q", string(key))
}

func parseUint(value string) (uint64, error) {
	return strconv.ParseUint(strings.TrimSpace(value), 10, 64)
}
