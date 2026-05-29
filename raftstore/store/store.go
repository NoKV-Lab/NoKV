// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package store is the multi-region execution runtime of NoKV's raft
// side. Each Store hosts a Router, a set of Peers, a region runtime,
// a command runtime, an execution runtime, and a scheduler runtime.
//
// Authority boundary: this package is an EXECUTOR. It applies committed
// raft commands and publishes terminal truth back to the coordinator;
// it never fabricates cluster-wide metadata. Any durable state owned
// here lives in raftstore/localmeta/ as a store-local recovery mirror.
//
// Transitions (peer add/remove, region split/merge, descriptor publish)
// are expressed as typed events through transition_builder.go and
// transition_executor.go, then published to coordinator via PublishRootEvent.
package store

import (
	"context"
	"sync"
	"time"

	"github.com/feichai0017/NoKV/meta/topology"
	"github.com/feichai0017/NoKV/metrics"
	"github.com/feichai0017/NoKV/raftstore/store/observer"
	"github.com/feichai0017/NoKV/raftstore/store/region"
	"github.com/feichai0017/NoKV/raftstore/store/router"
)

// Store hosts a collection of peers and exposes the concrete runtime helpers
// used by raftstore. It owns peer registration, region metadata, optional
// control-plane heartbeats, and command application.
type Store struct {
	router      *router.Router
	peerBuilder PeerBuilder
	workDir     string
	storeID     uint64
	addressMu   sync.RWMutex
	clientAddr  string
	raftAddr    string
	ctx         context.Context
	cancel      context.CancelFunc
	regions     *region.Manager
	sched       *schedulerRuntime
	cmds        *commandRuntime
	exec        *executionRuntime
	observers   *observer.Runtime
	regionStats *region.Stats
}

// NewStore constructs a Store using concrete dependencies. It keeps peer
// construction, region tracking, and scheduler heartbeats explicit rather than
// routing them through callback chains.
func NewStore(cfg Config) *Store {
	ctx, cancel := context.WithCancel(context.Background())
	rt := cfg.Router
	if rt == nil {
		rt = router.New()
	}
	regionMetrics := metrics.NewRegionMetrics()
	queueSize := max(cfg.OperationQueueSize, 0)
	operationCooldown := max(cfg.OperationCooldown, 0)
	if operationCooldown == 0 {
		operationCooldown = 5 * time.Second
	}
	operationInterval := cfg.OperationInterval
	if operationInterval <= 0 {
		operationInterval = cfg.HeartbeatInterval
	}
	if operationInterval <= 0 {
		operationInterval = 200 * time.Millisecond
	}
	operationBurst := max(cfg.OperationBurst, 0)
	if operationBurst == 0 {
		operationBurst = 4
	}
	heartbeatTimeout := cfg.HeartbeatTimeout
	if heartbeatTimeout <= 0 {
		heartbeatTimeout = 2 * time.Second
	}
	publishTimeout := cfg.PublishTimeout
	if publishTimeout <= 0 {
		publishTimeout = 2 * time.Second
	}
	commandTimeout := cfg.CommandTimeout
	if commandTimeout <= 0 {
		commandTimeout = 3 * time.Second
	}
	s := &Store{
		router:      rt,
		peerBuilder: cfg.PeerBuilder,
		workDir:     cfg.WorkDir,
		storeID:     cfg.StoreID,
		clientAddr:  cfg.ClientAddr,
		raftAddr:    cfg.RaftAddr,
		ctx:         ctx,
		cancel:      cancel,
		sched: &schedulerRuntime{
			client: cfg.Scheduler,
			operation: operationRuntime{
				cooldown:  operationCooldown,
				interval:  operationInterval,
				burst:     operationBurst,
				pending:   make(map[operationKey]bool),
				lastApply: make(map[operationKey]time.Time),
			},
			publish: publishRuntime{
				descriptors:      make(map[uint64]topology.Descriptor),
				regionUpdates:    make(map[uint64]regionEvent),
				heartbeatTimeout: heartbeatTimeout,
				publishTimeout:   publishTimeout,
			},
		},
		cmds: &commandRuntime{
			pipe:    newCommandPipelineWithBatch(cfg.CommandApplier, cfg.CommandBatchApplier, cfg.CommandApplyParallelism),
			timeout: commandTimeout,
		},
		exec:        newExecutionRuntime(),
		observers:   observer.New(),
		regionStats: region.NewStats(),
	}
	s.regions = region.NewManager(cfg.LocalMeta, regionMetrics, s.enqueueRegionRootEvent)
	if s.workDir == "" && cfg.LocalMeta != nil {
		s.workDir = cfg.LocalMeta.WorkDir()
	}
	if queueSize > 0 {
		s.sched.operation.input = make(chan scheduledOp, queueSize)
		s.sched.operation.stop = make(chan struct{})
		s.sched.operation.wg.Add(1)
		go s.runOperationLoop()
	}
	if cfg.LocalMeta != nil {
		s.regions.LoadBootstrap(cfg.LocalMeta.Snapshot())
		s.enqueueRecoveredPendingRegionEvents(cfg.LocalMeta.PendingRootEvents())
		s.enqueueRecoveredPendingSchedulerOperations(cfg.LocalMeta.PendingSchedulerOperations())
	}
	if s.schedulerClient() != nil {
		s.sched.publish.heartbeat = cfg.HeartbeatInterval
		if s.sched.publish.heartbeat <= 0 {
			s.sched.publish.heartbeat = 3 * time.Second
		}
		s.startHeartbeatLoop()
		s.signalRegionFlush()
	}
	return s
}

// SetAdvertiseAddrs updates the runtime endpoints reported to Coordinator.
func (s *Store) SetAdvertiseAddrs(clientAddr, raftAddr string) {
	if s == nil {
		return
	}
	s.addressMu.Lock()
	s.clientAddr = clientAddr
	s.raftAddr = raftAddr
	s.addressMu.Unlock()
}

// WorkDir returns the store-local workdir used for metadata and landing.
func (s *Store) WorkDir() string {
	if s == nil {
		return ""
	}
	return s.workDir
}

func (s *Store) runtimeContext() context.Context {
	if s == nil {
		return context.Background()
	}
	return s.ctx
}

func (s *Store) schedulerHeartbeatContext() (context.Context, context.CancelFunc) {
	if s == nil || s.sched == nil || s.sched.publish.heartbeatTimeout <= 0 {
		return context.WithCancel(s.runtimeContext())
	}
	return context.WithTimeout(s.runtimeContext(), s.sched.publish.heartbeatTimeout)
}

func (s *Store) schedulerPublishContext() (context.Context, context.CancelFunc) {
	if s == nil || s.sched == nil || s.sched.publish.publishTimeout <= 0 {
		return context.WithCancel(s.runtimeContext())
	}
	return context.WithTimeout(s.runtimeContext(), s.sched.publish.publishTimeout)
}
