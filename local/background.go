// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package local

import (
	stderrors "errors"
	"fmt"

	"github.com/feichai0017/NoKV/metrics"
	"github.com/feichai0017/NoKV/storage/wal"
	"github.com/feichai0017/NoKV/utils"
)

// backgroundStatsCollector is the narrow observer surface required by background
// DB services. The concrete implementation lives in local/stats while
// the lifecycle orchestration lives here.
type backgroundStatsCollector interface {
	StartStats()
	SetRegionMetrics(*metrics.RegionMetrics)
	Close() error
}

// backgroundConfig describes the runtime hooks needed to start DB-scoped
// background services.
//
// WALWatchdogConfigs supports the local data plane's per-shard WAL
// Managers: one watchdog per Manager keeps backlog metrics and auto-GC
// scoped to that shard's segments.
type backgroundConfig struct {
	StartCompacter     func()
	EnableWALWatchdog  bool
	WALWatchdogConfigs []wal.WatchdogConfig
	PeriodicTasks      []utils.PeriodicTaskConfig
}

// backgroundServices owns DB-scoped background runtime services that
// are not part of the DB's truth or public API surface.
type backgroundServices struct {
	stats         backgroundStatsCollector
	walWatchdogs  []*wal.Watchdog
	periodicTasks map[string]*utils.PeriodicTask
}

func (s *backgroundServices) Init(stats backgroundStatsCollector) {
	if s == nil {
		return
	}
	s.stats = stats
}

func (s *backgroundServices) Start(cfg backgroundConfig) {
	if s == nil {
		return
	}
	if cfg.StartCompacter != nil {
		cfg.StartCompacter()
	}
	if cfg.EnableWALWatchdog {
		for _, wcfg := range cfg.WALWatchdogConfigs {
			wd := wal.NewWatchdog(wcfg)
			if wd == nil {
				continue
			}
			wd.Start()
			s.walWatchdogs = append(s.walWatchdogs, wd)
		}
	}
	if s.stats != nil {
		s.stats.StartStats()
	}
	for _, tcfg := range cfg.PeriodicTasks {
		task := utils.NewPeriodicTask(tcfg)
		if task == nil {
			continue
		}
		if s.periodicTasks == nil {
			s.periodicTasks = make(map[string]*utils.PeriodicTask)
		}
		task.Start()
		s.periodicTasks[task.Name()] = task
	}
}

func (s *backgroundServices) Close() error {
	if s == nil {
		return nil
	}
	var errs []error
	for name, task := range s.periodicTasks {
		if task != nil {
			task.Close()
		}
		delete(s.periodicTasks, name)
	}
	s.periodicTasks = nil
	if s.stats != nil {
		if err := s.stats.Close(); err != nil {
			errs = append(errs, fmt.Errorf("stats close: %w", err))
		}
		s.stats = nil
	}
	for i, wd := range s.walWatchdogs {
		if wd != nil {
			wd.Stop()
			s.walWatchdogs[i] = nil
		}
	}
	s.walWatchdogs = nil
	if len(errs) > 0 {
		return stderrors.Join(errs...)
	}
	return nil
}

func (s *backgroundServices) StatsCollector() backgroundStatsCollector {
	if s == nil {
		return nil
	}
	return s.stats
}

func (s *backgroundServices) SetRegionMetrics(rm *metrics.RegionMetrics) {
	if s == nil || s.stats == nil {
		return
	}
	s.stats.SetRegionMetrics(rm)
}

// WALWatchdogs returns every running WAL watchdog (one per local commit
// data-plane shard). Stats collectors that surface backlog metrics
// aggregate across the slice.
func (s *backgroundServices) WALWatchdogs() []*wal.Watchdog {
	if s == nil {
		return nil
	}
	return s.walWatchdogs
}

func (s *backgroundServices) TaskSnapshot(name string) utils.PeriodicTaskSnapshot {
	if s == nil || s.periodicTasks == nil {
		return utils.PeriodicTaskSnapshot{}
	}
	task := s.periodicTasks[name]
	if task == nil {
		return utils.PeriodicTaskSnapshot{}
	}
	return task.Snapshot()
}
