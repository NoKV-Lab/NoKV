// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	fsmetawatch "github.com/feichai0017/NoKV/fsmeta/exec/watch"
	"github.com/feichai0017/NoKV/fsmeta/observe"
	storepkg "github.com/feichai0017/NoKV/raftstore/store"
)

// ApplyObserver adapts raftstore apply notifications into fsmeta watch events.
// The router itself stays storage-runtime neutral.
type ApplyObserver struct {
	router *fsmetawatch.Router
}

func NewApplyObserver(router *fsmetawatch.Router) *ApplyObserver {
	return &ApplyObserver{router: router}
}

func (o *ApplyObserver) OnApply(evt storepkg.ApplyEvent) {
	if o == nil || o.router == nil {
		return
	}
	source := observe.WatchEventSource(0)
	switch evt.Source {
	case storepkg.ApplyEventSourceCommit:
		source = observe.WatchEventSourceCommit
	case storepkg.ApplyEventSourceResolveLock:
		source = observe.WatchEventSourceResolveLock
	default:
		return
	}
	o.router.OnApply(observe.ApplyEvent{
		RegionID:      evt.RegionID,
		Term:          evt.Term,
		Index:         evt.Index,
		Source:        source,
		CommitVersion: evt.CommitVersion,
		Keys:          evt.Keys,
	})
}
