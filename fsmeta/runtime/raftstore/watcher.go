// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"context"

	"github.com/feichai0017/NoKV/fsmeta"
	fsmetaexec "github.com/feichai0017/NoKV/fsmeta/exec"
	fsmetawatch "github.com/feichai0017/NoKV/fsmeta/exec/watch"
	"github.com/feichai0017/NoKV/fsmeta/model"
)

// watcher wraps the in-process router with mount admission and pulls watch
// metrics together with remote-source metrics under a single Stats() surface
// for expvar exposure.
type watcher struct {
	*fsmetawatch.Router
	source *RemoteSource
	mounts fsmetaexec.MountResolver
}

func (w watcher) Subscribe(ctx context.Context, req fsmeta.WatchRequest) (fsmeta.WatchSubscription, error) {
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
		prefix, err := fsmeta.WatchPrefixForMount(req, record.Identity())
		if err != nil {
			return nil, err
		}
		req.KeyPrefix = prefix
	}
	if w.Router == nil {
		return nil, model.ErrInvalidRequest
	}
	return w.Router.Subscribe(ctx, req)
}

func (w watcher) Stats() map[string]any {
	out := map[string]any{}
	if w.Router != nil {
		copyStats(out, w.Router.Stats())
	}
	if w.source != nil {
		copyStats(out, w.source.Stats())
	}
	return out
}

func copyStats(dst, src map[string]any) {
	for key := range src {
		dst[key] = src[key]
	}
}
