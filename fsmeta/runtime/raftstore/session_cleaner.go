// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/feichai0017/NoKV/fsmeta/model"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"github.com/feichai0017/NoKV/utils"
)

const defaultSessionCleanupInterval = 30 * time.Second

type sessionMountLister interface {
	ListMounts(context.Context, *coordpb.ListMountsRequest) (*coordpb.ListMountsResponse, error)
}

type sessionCleanupExecutor interface {
	ExpireWriteSessions(context.Context, model.ExpireWriteSessionsRequest) (model.ExpireWriteSessionsResult, error)
}

// sessionCleaner periodically removes expired writer-session records for every
// active mount. It lives in fsmeta/exec because it interprets fsmeta mount and
// session semantics; the lower KV layers only provide the transactional write.
type sessionCleaner struct {
	mounts   sessionMountLister
	exec     sessionCleanupExecutor
	interval time.Duration
	limit    uint32

	periodic *utils.PeriodicTask

	mu    sync.RWMutex
	stats sessionCleanerStats
}

type sessionCleanerStats struct {
	Enabled        bool    `json:"enabled"`
	Runs           uint64  `json:"runs"`
	LastUnix       int64   `json:"last_unix,omitempty"`
	LastDurationMs float64 `json:"last_duration_ms,omitempty"`
	LastError      string  `json:"last_error,omitempty"`
	LastMounts     uint64  `json:"last_mounts,omitempty"`
	LastExpired    uint64  `json:"last_expired,omitempty"`
	TotalExpired   uint64  `json:"total_expired,omitempty"`
}

func startSessionCleaner(ctx context.Context, mounts sessionMountLister, exec sessionCleanupExecutor, interval time.Duration, limit uint32) *sessionCleaner {
	if mounts == nil || exec == nil || interval < 0 {
		return nil
	}
	if interval == 0 {
		interval = defaultSessionCleanupInterval
	}
	c := &sessionCleaner{
		mounts:   mounts,
		exec:     exec,
		interval: interval,
		limit:    limit,
		stats: sessionCleanerStats{
			Enabled: true,
		},
	}
	c.periodic = utils.NewPeriodicTask(utils.PeriodicTaskConfig{
		Name:     "fsmeta-session-cleaner",
		Interval: interval,
		Run:      c.runOnce,
		Context:  ctx,
	})
	c.periodic.Start()
	return c
}

func (c *sessionCleaner) runOnce(ctx context.Context) error {
	start := time.Now()
	mounts, expired, err := c.expire(ctx)
	c.record(start, mounts, expired, err)
	return err
}

func (c *sessionCleaner) expire(ctx context.Context) (uint64, uint64, error) {
	if c == nil || c.mounts == nil || c.exec == nil {
		return 0, 0, nil
	}
	resp, err := c.mounts.ListMounts(ctx, &coordpb.ListMountsRequest{})
	if err != nil {
		return 0, 0, err
	}
	var (
		visited uint64
		expired uint64
		errs    []error
	)
	for _, mount := range resp.GetMounts() {
		id := model.MountID(mount.GetMountId())
		if id == "" || mount.GetState() != coordpb.MountState_MOUNT_STATE_ACTIVE {
			continue
		}
		visited++
		result, err := c.exec.ExpireWriteSessions(ctx, model.ExpireWriteSessionsRequest{
			Mount: id,
			Limit: c.limit,
		})
		if err != nil {
			errs = append(errs, fmt.Errorf("mount %s: %w", id, err))
			continue
		}
		expired += result.Expired
	}
	return visited, expired, errors.Join(errs...)
}

func (c *sessionCleaner) record(start time.Time, mounts, expired uint64, err error) {
	if c == nil {
		return
	}
	errText := ""
	if err != nil {
		errText = err.Error()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stats.Enabled = true
	c.stats.Runs++
	c.stats.LastUnix = time.Now().Unix()
	c.stats.LastDurationMs = float64(time.Since(start).Microseconds()) / 1000
	c.stats.LastMounts = mounts
	c.stats.LastExpired = expired
	c.stats.TotalExpired += expired
	c.stats.LastError = errText
}

func (c *sessionCleaner) Stats() map[string]any {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return map[string]any{
		"enabled":          c.stats.Enabled,
		"runs":             c.stats.Runs,
		"last_unix":        c.stats.LastUnix,
		"last_duration_ms": c.stats.LastDurationMs,
		"last_error":       c.stats.LastError,
		"last_mounts":      c.stats.LastMounts,
		"last_expired":     c.stats.LastExpired,
		"total_expired":    c.stats.TotalExpired,
	}
}

func (c *sessionCleaner) Close() error {
	if c == nil {
		return nil
	}
	c.periodic.Close()
	return nil
}
