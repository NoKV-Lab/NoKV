// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	localstats "github.com/feichai0017/NoKV/local/stats"
	"github.com/feichai0017/NoKV/metrics"
	adminpb "github.com/feichai0017/NoKV/pb/admin"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/admin"
	"github.com/feichai0017/NoKV/raftstore/kv"
	storemvcc "github.com/feichai0017/NoKV/raftstore/mvcc"
	raftstorestats "github.com/feichai0017/NoKV/raftstore/stats"
	"github.com/feichai0017/NoKV/raftstore/store"
	"github.com/feichai0017/NoKV/raftstore/store/router"
	"github.com/feichai0017/NoKV/raftstore/transport"
	"github.com/feichai0017/NoKV/utils"
	"google.golang.org/grpc"
)

var raftDebugLoggerOnce sync.Once

func enableRaftDebugLogging() {
	raftDebugLoggerOnce.Do(func() {
		logger := &myraft.DefaultLogger{Logger: log.New(os.Stderr, "raft ", log.LstdFlags)}
		logger.EnableTimestamps()
		logger.EnableDebug()
		myraft.SetLogger(logger)
	})
}

// Node hosts one raftstore store together with the shared gRPC transport,
// data-plane KV service, and admin service.
type Node struct {
	store           *store.Store
	transport       *transport.GRPCTransport
	kvService       *kv.Service
	mvccMaintenance *storemvcc.MaintenanceWorker
	mvccGCPlan      *storemvcc.GCPlanner
	mvccGCPlanTask  *utils.PeriodicTask
	tickStop        chan struct{}
	tickWG          sync.WaitGroup
	tickEvery       time.Duration
}

type mvccGCStatsSink interface {
	SetMVCCGCStatsSnapshotSource(func() localstats.MVCCGCStatsSnapshot)
}

type transportMetricsSink interface {
	SetTransportMetricsSource(func() metrics.GRPCTransportMetrics)
}

// NewNode constructs one raftstore node using the provided configuration.
func NewNode(cfg Config) (*Node, error) {
	if cfg.Storage.MVCC == nil {
		return nil, fmt.Errorf("raftstore/server: MVCC storage is required")
	}
	if cfg.Store.StoreID == 0 {
		return nil, fmt.Errorf("raftstore/server: StoreID must be set")
	}
	storeCfg := cfg.Store
	applyOpts := []kv.ApplyOption(nil)
	if cfg.WriteFence != nil {
		applyOpts = append(applyOpts, kv.WithWriteFence(cfg.WriteFence))
	}
	if storeCfg.CommandApplier == nil {
		storeCfg.CommandApplier = kv.NewApplier(cfg.Storage.MVCC, nil, applyOpts...)
	}
	if storeCfg.CommandBatchApplier == nil {
		storeCfg.CommandBatchApplier = kv.NewBatchApplier(cfg.Storage.MVCC, nil, applyOpts...)
	}
	rt := storeCfg.Router
	if rt == nil {
		rt = router.New()
		storeCfg.Router = rt
	}

	if cfg.EnableRaftDebugLog {
		enableRaftDebugLogging()
	}

	tr, err := transport.NewUnstartedGRPCTransport(storeCfg.StoreID, cfg.TransportAddr, cfg.TransportOptions...)
	if err != nil {
		return nil, err
	}

	builder := storeCfg.PeerBuilder
	if builder == nil {
		if cfg.Storage.Raft == nil {
			_ = tr.Close()
			return nil, fmt.Errorf("raftstore/server: raft log storage is required")
		}
		builder = defaultPeerBuilder(cfg.Storage, storeCfg.LocalMeta, storeCfg.StoreID, cfg.Raft, tr)
	}
	storeCfg.PeerBuilder = builder

	st := store.NewStore(storeCfg)
	kvService := kv.NewService(st)
	adminService := admin.NewService(st)
	if err := tr.RegisterServer(func(reg grpc.ServiceRegistrar) {
		kvrpcpb.RegisterStoreKVServer(reg, kvService)
		adminpb.RegisterRaftAdminServer(reg, adminService)
		for _, register := range cfg.ExtraServices {
			if register != nil {
				register(reg)
			}
		}
	}); err != nil {
		_ = tr.Close()
		return nil, err
	}
	tr.SetHandler(func(msg myraft.Message) error {
		return st.Step(msg)
	})
	if err := tr.Start(); err != nil {
		_ = tr.Close()
		return nil, err
	}
	clientAddr := storeCfg.ClientAddr
	if clientAddr == "" {
		clientAddr = tr.Addr()
	}
	raftAddr := storeCfg.RaftAddr
	if raftAddr == "" {
		raftAddr = clientAddr
	}
	st.SetAdvertiseAddrs(clientAddr, raftAddr)

	node := &Node{
		store:     st,
		transport: tr,
		kvService: kvService,
	}
	if sink, ok := cfg.Storage.MVCC.(transportMetricsSink); ok {
		sink.SetTransportMetricsSource(transport.GRPCMetricsSnapshot)
	}
	if taskCfg, planner, ok := storemvcc.NewGCPlanTask(storemvcc.GCPlanConfig{
		MVCCStore: cfg.Storage.MVCC,
		Interval:  cfg.MVCCGCPlan.Interval,
		SafePoint: cfg.MVCCGCPlan.SafePoint,
		Retention: cfg.MVCCGCPlan.Retention,
		Mount:     cfg.MVCCGCPlan.Mount,
	}); ok {
		node.mvccGCPlan = planner
		node.mvccGCPlanTask = utils.NewPeriodicTask(taskCfg)
		node.mvccGCPlanTask.Start()
	}
	if worker, ok := storemvcc.NewMaintenanceWorker(storemvcc.MaintenanceWorkerConfig{
		MVCCStore:           cfg.Storage.MVCC,
		MaintenanceProposer: st,
		LockResolver:        cfg.MVCCMaintenance.LockResolver,
		Interval:            cfg.MVCCMaintenance.Interval,
		Timeout:             cfg.MVCCMaintenance.Timeout,
		SafePoint:           cfg.MVCCMaintenance.SafePoint,
		CurrentTs:           cfg.MVCCMaintenance.CurrentTs,
		CurrentTime:         cfg.MVCCMaintenance.CurrentTime,
		Retention:           cfg.MVCCMaintenance.Retention,
		Mount:               cfg.MVCCMaintenance.Mount,
		Apply:               cfg.MVCCMaintenance.Apply,
		ResolveLocks:        cfg.MVCCMaintenance.ResolveLocks,
		RunOrphanDefaults:   cfg.MVCCMaintenance.RunOrphanDefaults,
		OrphanDefaults:      cfg.MVCCMaintenance.OrphanDefaults,
	}); ok {
		node.mvccMaintenance = worker
		worker.Start()
	}
	if sink, ok := cfg.Storage.MVCC.(mvccGCStatsSink); ok {
		sink.SetMVCCGCStatsSnapshotSource(node.MVCCGCStatsSnapshot)
	}
	interval := cfg.RaftTickInterval
	if interval <= 0 {
		interval = defaultRaftTickInterval
	}
	node.startRaftTickLoop(interval)
	return node, nil
}

// Addr returns the address NoKV clients (and raft peers) should dial.
func (n *Node) Addr() string {
	if n == nil || n.transport == nil {
		return ""
	}
	return n.transport.Addr()
}

// Store exposes the underlying raftstore Store.
func (n *Node) Store() *store.Store {
	if n == nil {
		return nil
	}
	return n.store
}

// KVStats returns low-cardinality StoreKV service counters for diagnostics.
func (n *Node) KVStats() map[string]any {
	if n == nil || n.kvService == nil {
		return map[string]any{}
	}
	return n.kvService.Stats()
}

// Transport returns the shared raft/StoreKV gRPC transport.
func (n *Node) Transport() *transport.GRPCTransport {
	if n == nil {
		return nil
	}
	return n.transport
}

// MVCCMaintenanceSnapshot returns the last replicated MVCC maintenance pass.
func (n *Node) MVCCMaintenanceSnapshot() storemvcc.MaintenanceSnapshot {
	if n == nil || n.mvccMaintenance == nil {
		return storemvcc.MaintenanceSnapshot{}
	}
	return n.mvccMaintenance.Snapshot()
}

// MVCCGCStatsSnapshot returns the runtime stats view of raftstore MVCC GC state.
func (n *Node) MVCCGCStatsSnapshot() localstats.MVCCGCStatsSnapshot {
	if n == nil {
		return localstats.MVCCGCStatsSnapshot{}
	}
	var plan storemvcc.GCPlanSnapshot
	if n.mvccGCPlan != nil && n.mvccGCPlanTask != nil {
		plan = n.mvccGCPlan.Snapshot(n.mvccGCPlanTask.Snapshot())
	}
	return raftstorestats.MVCCGC(plan, n.MVCCMaintenanceSnapshot())
}

// Close stops the node transport. The caller remains responsible for closing
// the DB and store once outstanding operations are drained.
func (n *Node) Close() error {
	if n == nil {
		return nil
	}
	if n.mvccGCPlanTask != nil {
		n.mvccGCPlanTask.Close()
		n.mvccGCPlanTask = nil
		n.mvccGCPlan = nil
	}
	if n.mvccMaintenance != nil {
		n.mvccMaintenance.Close()
		n.mvccMaintenance = nil
	}
	if n.transport != nil {
		if err := n.transport.Close(); err != nil {
			return err
		}
	}
	if n.tickStop != nil {
		close(n.tickStop)
		n.tickWG.Wait()
		n.tickStop = nil
	}
	if n.store != nil {
		n.store.Close()
	}
	return nil
}
