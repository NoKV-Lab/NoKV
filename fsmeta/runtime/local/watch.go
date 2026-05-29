// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package local

import (
	"context"
	"maps"
	"sync/atomic"

	"github.com/feichai0017/NoKV/fsmeta/backend"
	fsmetaexec "github.com/feichai0017/NoKV/fsmeta/exec"
	fsmetawatch "github.com/feichai0017/NoKV/fsmeta/exec/watch"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/feichai0017/NoKV/fsmeta/observe"
)

const (
	localWatchRegionID uint64 = 1
	localWatchTerm     uint64 = 1
)

// Watcher turns local MVCC commits into fsmeta watch events.
type Watcher struct {
	*fsmetawatch.Router
	mounts fsmetaexec.MountResolver
	next   atomic.Uint64
}

// NewWatcher constructs a local watch router.
func NewWatcher(mounts fsmetaexec.MountResolver) *Watcher {
	return &Watcher{
		Router: fsmetawatch.NewRouter(),
		mounts: mounts,
	}
}

// Subscribe implements observe.Watcher with local mount admission.
func (w *Watcher) Subscribe(ctx context.Context, req observe.WatchRequest) (observe.WatchSubscription, error) {
	if w == nil || w.Router == nil {
		return nil, model.ErrInvalidRequest
	}
	if req.Mount != "" && w.mounts != nil {
		record, err := w.mounts.ResolveMount(ctx, req.Mount)
		if err != nil {
			return nil, err
		}
		if record.MountID == "" {
			return nil, model.ErrMountNotRegistered
		}
		if record.Retired {
			return nil, model.ErrMountRetired
		}
		prefix, err := observe.WatchPrefixForMount(req, record.Identity())
		if err != nil {
			return nil, err
		}
		req.KeyPrefix = prefix
	}
	return w.Router.Subscribe(ctx, req)
}

// ObserveMutation publishes one replayable local watch cursor after a mutation
// group has been durably applied.
func (w *Watcher) ObserveMutation(commitVersion uint64, mutations []*backend.Mutation) {
	if w == nil || w.Router == nil || commitVersion == 0 {
		return
	}
	keys := mutationWatchKeys(mutations)
	if len(keys) == 0 {
		return
	}
	cursor := observe.WatchCursor{
		RegionID: localWatchRegionID,
		Term:     localWatchTerm,
		Index:    w.next.Add(1),
	}
	for _, key := range keys {
		w.Publish(observe.WatchEvent{
			Cursor:        cursor,
			CommitVersion: commitVersion,
			Source:        observe.WatchEventSourceCommit,
			Key:           key,
		})
	}
}

// Stats returns local watch diagnostics.
func (w *Watcher) Stats() map[string]any {
	if w == nil || w.Router == nil {
		return map[string]any{
			"subscribers":     0,
			"regions":         0,
			"recent_events":   0,
			"events_total":    uint64(0),
			"delivered_total": uint64(0),
			"dropped_total":   uint64(0),
			"overflow_total":  uint64(0),
			"next_cursor":     uint64(0),
		}
	}
	out := copyStats(w.Router.Stats())
	out["next_cursor"] = w.next.Load()
	return out
}

func mutationWatchKeys(mutations []*backend.Mutation) [][]byte {
	seen := map[string]struct{}{}
	keys := make([][]byte, 0, len(mutations))
	for _, mutation := range mutations {
		if mutation == nil {
			continue
		}
		switch mutation.Op {
		case backend.MutationPut, backend.MutationDelete:
		default:
			continue
		}
		key := mutation.Key
		if len(key) == 0 {
			continue
		}
		id := string(key)
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		keys = append(keys, cloneBytes(key))
	}
	return keys
}

func copyStats(src map[string]any) map[string]any {
	out := make(map[string]any, len(src))
	maps.Copy(out, src)
	return out
}
