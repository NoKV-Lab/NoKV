// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package fsmeta wires experimental Peras into the stable fsmeta runtime
// extension point. It adapts Peras holder, witness, and segment-install
// protocols to the raftstore-backed fsmeta runtime without making the stable
// runtime own Peras state.
package fsmeta

import (
	"context"
	"fmt"
	"strings"
	"time"

	execperas "github.com/feichai0017/NoKV/experimental/peras/exec"
	runtimeperas "github.com/feichai0017/NoKV/experimental/peras/runtime"
	fsmetaexec "github.com/feichai0017/NoKV/fsmeta/exec"
	stable "github.com/feichai0017/NoKV/fsmeta/runtime/raftstore"
	"github.com/feichai0017/NoKV/storage/wal"
	"google.golang.org/grpc"
)

// Config configures the experimental Peras runtime attached to distributed
// fsmeta. A zero Config is invalid; callers must opt in with a holder id and a
// visible log.
type Config struct {
	HolderID     string
	AuthorityTTL time.Duration

	WitnessStoreIDs            []uint64
	WitnessQuorum              int
	SegmentWitnessRetries      int
	SegmentWitnessRetryBackoff time.Duration
	SegmentBatchSize           int
	AdmissionPendingLimit      int
	SegmentMaxReplayOperations int
	SegmentMaxReplayMutations  int
	SegmentMaxPayloadBytes     uint64
	SegmentCatalogRouteBudget  int
	SegmentInstallParallelism  int
	SegmentFlushParallelism    int
	SegmentFlushEvery          time.Duration
	BackgroundFlushTimeout     time.Duration
	BackgroundErrorBackoff     time.Duration

	VisibleLog           execperas.VisibleLog
	VisibleLogDir        string
	VisibleLogDurability wal.DurabilityPolicy
}

// Extension attaches Peras as an experimental fsmeta raftstore extension.
type Extension struct {
	Config Config
}

// NewExtension returns an optional raftstore runtime extension for Peras.
func NewExtension(cfg Config) *Extension {
	return &Extension{Config: cfg}
}

type coordinatorClient interface {
	runtimeperas.Client
	runtimeperas.RootAuthoritySource
	witnessStoreLister
}

func (e *Extension) Attach(ctx context.Context, env stable.ExtensionContext) (*stable.ExtensionAttachment, error) {
	if e == nil {
		return nil, nil
	}
	cfg := e.Config
	holderID := strings.TrimSpace(cfg.HolderID)
	if holderID == "" {
		return nil, runtimeperas.ErrHolderRequired
	}
	if cfg.AuthorityTTL < 0 {
		return nil, runtimeperas.ErrTTLInvalid
	}
	if cfg.SegmentBatchSize < 0 || cfg.AdmissionPendingLimit < 0 || cfg.SegmentMaxReplayOperations < 0 || cfg.SegmentMaxReplayMutations < 0 || cfg.SegmentCatalogRouteBudget < 0 || cfg.SegmentInstallParallelism < 0 || cfg.SegmentFlushParallelism < 0 || cfg.SegmentFlushEvery < 0 ||
		cfg.BackgroundFlushTimeout < 0 || cfg.BackgroundErrorBackoff < 0 || cfg.WitnessQuorum < 0 || cfg.SegmentWitnessRetries < 0 || cfg.SegmentWitnessRetryBackoff < 0 {
		return nil, runtimeperas.ErrRuntimeInvalid
	}
	coord, ok := env.Coordinator.(coordinatorClient)
	if !ok {
		return nil, runtimeperas.ErrClientRequired
	}
	if env.Runner == nil || env.KV == nil || env.WatchRouter == nil {
		return nil, runtimeperas.ErrRuntimeInvalid
	}

	table := runtimeperas.NewActiveAuthorities()
	authority, err := runtimeperas.NewAuthorityManager(coord, table, holderID, cfg.AuthorityTTL, nil)
	if err != nil {
		return nil, fmt.Errorf("init peras authority manager: %w", err)
	}
	feed := runtimeperas.StartRootAuthorityFeed(ctx, coord, table, 0)
	visibleLog, visibleWAL, err := openVisibleLog(cfg)
	if err != nil {
		if feed != nil {
			_ = feed.Close()
		}
		return nil, err
	}
	cfg.VisibleLog = visibleLog
	perasRuntime, witnessConns, err := buildRuntime(ctx, coord, env.KV, env.Runner, env.WatchRouter, authority, env.DialOptions, cfg)
	if err != nil {
		if visibleWAL != nil {
			_ = visibleWAL.Close()
		}
		if feed != nil {
			_ = feed.Close()
		}
		return nil, err
	}
	executorAdapter := newExecutorAdapter(authority, perasRuntime)
	return &stable.ExtensionAttachment{
		ExecutorOptions: []fsmetaexec.Option{
			fsmetaexec.WithVisibleAuthorityAdmitter(executorAdapter),
			fsmetaexec.WithVisibleCommitter(executorAdapter),
		},
		Stats: []stable.ExtensionStats{{
			Name:     "nokv_fsmeta_peras",
			Snapshot: perasRuntime.Stats,
		}},
		Close: func() error {
			var first error
			if err := perasRuntime.Shutdown(context.Background()); err != nil && first == nil {
				first = err
			}
			if feed != nil {
				if err := feed.Close(); err != nil && first == nil {
					first = err
				}
			}
			if visibleWAL != nil {
				if err := visibleWAL.Close(); err != nil && first == nil {
					first = err
				}
			}
			if witnessConns != nil {
				if err := witnessConns.Close(); err != nil && first == nil {
					first = err
				}
			}
			return first
		},
	}, nil
}

func openVisibleLog(cfg Config) (execperas.VisibleLog, *wal.Manager, error) {
	if cfg.VisibleLog != nil {
		return cfg.VisibleLog, nil, nil
	}
	if strings.TrimSpace(cfg.VisibleLogDir) == "" {
		return nil, nil, execperas.ErrVisibleLogRequired
	}
	durability := cfg.VisibleLogDurability
	if durability == 0 {
		durability = wal.DurabilityFlushed
	}
	manager, err := wal.Open(wal.Config{Dir: cfg.VisibleLogDir})
	if err != nil {
		return nil, nil, fmt.Errorf("init peras visible log wal: %w", err)
	}
	visible, err := runtimeperas.NewWALVisibleLog(manager, durability)
	if err != nil {
		_ = manager.Close()
		return nil, nil, fmt.Errorf("init peras visible log: %w", err)
	}
	return visible, manager, nil
}

func buildRuntime(
	ctx context.Context,
	lister witnessStoreLister,
	kv stable.KVClient,
	runner *stable.Runner,
	router fsmetaWatchRouter,
	authority *runtimeperas.AuthorityManager,
	dialOpts []grpc.DialOption,
	cfg Config,
) (*runtimeperas.Runtime, *witnessConnections, error) {
	witnessConns, err := buildWitnessConnections(ctx, lister, dialOpts, cfg.WitnessStoreIDs)
	if err != nil {
		return nil, nil, fmt.Errorf("init peras witnesses: %w", err)
	}
	perasRuntime, err := runtimeperas.NewRuntime(runtimeperas.Config{
		Authority:                    authority,
		Witnesses:                    witnessConns.witnesses,
		Installer:                    newRaftstoreSegmentInstaller(kv, runner, router),
		CatalogScanner:               raftstoreSegmentCatalogScanner{runner: runner},
		WatchPublisher:               router,
		VisibleLog:                   cfg.VisibleLog,
		Quorum:                       cfg.WitnessQuorum,
		SegmentWitnessRetries:        cfg.SegmentWitnessRetries,
		SegmentWitnessRetryBackoff:   cfg.SegmentWitnessRetryBackoff,
		SegmentBatchSize:             cfg.SegmentBatchSize,
		AdmissionPendingLimit:        cfg.AdmissionPendingLimit,
		SegmentMaxReplayOperations:   cfg.SegmentMaxReplayOperations,
		SegmentMaxReplayMutations:    cfg.SegmentMaxReplayMutations,
		SegmentMaxPayloadBytes:       cfg.SegmentMaxPayloadBytes,
		SegmentCatalogRouteBudget:    cfg.SegmentCatalogRouteBudget,
		SegmentInstallParallelism:    cfg.SegmentInstallParallelism,
		SegmentFlushParallelism:      cfg.SegmentFlushParallelism,
		SegmentFlushEvery:            cfg.SegmentFlushEvery,
		BackgroundFlushTimeout:       cfg.BackgroundFlushTimeout,
		BackgroundErrorBackoff:       cfg.BackgroundErrorBackoff,
		QuorumVisibleSnapshotCapture: true,
	})
	if err != nil {
		_ = witnessConns.Close()
		return nil, nil, fmt.Errorf("init peras runtime: %w", err)
	}
	return perasRuntime, witnessConns, nil
}
