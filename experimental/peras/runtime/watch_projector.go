// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	fsperas "github.com/feichai0017/NoKV/experimental/peras/exec"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/observe"
)

type perasWatchPublisher interface {
	Publish(observe.WatchEvent)
}

func (c *Runtime) publishVisibleWatch(op compile.MaterializedOp, ack fsperas.VisibleAck) {
	if c == nil || c.watch == nil {
		return
	}
	if len(op.Watch) == 0 {
		return
	}
	cursor := observe.WatchCursor{
		Term:  ack.EpochID,
		Index: c.visibleSeq.Add(1),
	}
	seen := make(map[string]struct{}, len(op.Watch))
	for _, projection := range op.Watch {
		if projection.EmitAt != compile.WatchEmitVisible || len(projection.Key) == 0 {
			continue
		}
		keyID := string(projection.Key)
		if _, ok := seen[keyID]; ok {
			continue
		}
		seen[keyID] = struct{}{}
		evt := observe.WatchEvent{
			Cursor: cursor,
			Source: observe.WatchEventSourceRuntimeVisible,
			Key:    cloneBytes(projection.Key),
		}
		if c.watchQueue != nil && c.watchQueue.TryPush(evt) {
			continue
		}
		c.watch.Publish(evt)
	}
}

func (c *Runtime) visibleWatchLoop() {
	if c == nil || c.closer == nil {
		return
	}
	defer c.closer.Done()
	if c.watch == nil || c.watchQueue == nil {
		return
	}
	consumer := c.watchQueue.AcquireConsumer()
	if consumer == nil {
		return
	}
	defer consumer.Close()
	for {
		evt, ok := consumer.Pop()
		if !ok {
			return
		}
		c.watch.Publish(evt)
	}
}
