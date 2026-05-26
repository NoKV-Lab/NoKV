// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"context"
	"log"
	"sync"
	"time"

	fsmetaexec "github.com/feichai0017/NoKV/fsmeta/exec"
	"github.com/feichai0017/NoKV/fsmeta/model"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"google.golang.org/grpc"
)

const defaultMonitorInterval = time.Second

type lifecycleSource interface {
	ListMounts(context.Context, *coordpb.ListMountsRequest) (*coordpb.ListMountsResponse, error)
	ListSubtreeAuthorities(context.Context, *coordpb.ListSubtreeAuthoritiesRequest) (*coordpb.ListSubtreeAuthoritiesResponse, error)
	ListQuotaFences(context.Context, *coordpb.ListQuotaFencesRequest) (*coordpb.ListQuotaFencesResponse, error)
	WatchRootEvents(context.Context, *coordpb.WatchRootEventsRequest, ...grpc.CallOption) (coordpb.Coordinator_WatchRootEventsClient, error)
}

type retireRouter interface {
	RetireMount(model.MountID) int
}

// monitor bootstraps lifecycle state once, then follows coordinator rooted
// event streams for mount retire, quota fence, and pending subtree handoff
// updates. It is owned by Runtime and shut down by Runtime.Close.
type monitor struct {
	coord    lifecycleSource
	router   retireRouter
	cache    *mountCache
	quotas   *quotaCache
	subtrees fsmetaexec.SubtreeHandoffPublisher
	interval time.Duration
	stop     chan struct{}
	done     chan struct{}
	once     sync.Once
}

func startMonitor(ctx context.Context, coord lifecycleSource, router retireRouter, cache *mountCache, quotas *quotaCache, subtrees fsmetaexec.SubtreeHandoffPublisher, interval time.Duration) *monitor {
	if coord == nil || router == nil {
		return nil
	}
	if interval <= 0 {
		interval = defaultMonitorInterval
	}
	m := &monitor{
		coord:    coord,
		router:   router,
		cache:    cache,
		quotas:   quotas,
		subtrees: subtrees,
		interval: interval,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	go m.run(ctx)
	return m
}

func (m *monitor) run(ctx context.Context) {
	defer close(m.done)
	var after rootstorage.TailToken
	if err := m.bootstrap(ctx); err != nil && ctx.Err() == nil {
		log.Printf("fsmeta monitor bootstrap: %v", err)
	}
	for {
		if err := m.watch(ctx, &after); err != nil && ctx.Err() == nil {
			log.Printf("fsmeta monitor watch: %v", err)
		}
		if !m.wait(ctx, m.interval) {
			return
		}
	}
}

func (m *monitor) bootstrap(ctx context.Context) error {
	if m == nil || m.coord == nil || m.router == nil {
		return nil
	}
	resp, err := m.coord.ListMounts(ctx, &coordpb.ListMountsRequest{})
	if err != nil {
		return err
	}
	for _, mount := range resp.GetMounts() {
		if mount.GetMountId() == "" || mount.GetState() != coordpb.MountState_MOUNT_STATE_RETIRED {
			continue
		}
		id := model.MountID(mount.GetMountId())
		if m.cache != nil {
			m.cache.markRetired(id)
		}
		if m.quotas != nil {
			m.quotas.purgeMount(id)
		}
		m.router.RetireMount(id)
	}
	quotas, err := m.coord.ListQuotaFences(ctx, &coordpb.ListQuotaFencesRequest{})
	if err != nil {
		return err
	}
	if m.quotas != nil {
		for _, fence := range quotas.GetFences() {
			m.quotas.markFenceUpdated(fence)
		}
	}
	subtrees, err := m.coord.ListSubtreeAuthorities(ctx, &coordpb.ListSubtreeAuthoritiesRequest{})
	if err != nil {
		return err
	}
	if m.subtrees != nil {
		for _, subtree := range subtrees.GetSubtrees() {
			if subtree.GetState() != coordpb.SubtreeAuthorityState_SUBTREE_AUTHORITY_STATE_HANDOFF {
				continue
			}
			if subtree.GetMountId() == "" || subtree.GetRootInode() == 0 || subtree.GetPredecessorFrontier() == 0 {
				continue
			}
			if err := m.subtrees.CompleteSubtreeHandoff(ctx, model.MountID(subtree.GetMountId()), model.InodeID(subtree.GetRootInode()), subtree.GetPredecessorFrontier()); err != nil {
				log.Printf("fsmeta monitor: complete pending subtree handoff mount=%s root=%d frontier=%d: %v",
					subtree.GetMountId(), subtree.GetRootInode(), subtree.GetPredecessorFrontier(), err)
			}
		}
	}
	return nil
}

func (m *monitor) watch(ctx context.Context, after *rootstorage.TailToken) error {
	if m == nil || m.coord == nil {
		return nil
	}
	var token rootstorage.TailToken
	if after != nil {
		token = *after
	}
	stream, err := m.coord.WatchRootEvents(ctx, &coordpb.WatchRootEventsRequest{
		After: metawire.RootTailTokenToProto(token),
	})
	if err != nil {
		return err
	}
	for {
		resp, err := stream.Recv()
		if err != nil {
			return err
		}
		if resp.GetBootstrapRequired() {
			if err := m.bootstrap(ctx); err != nil {
				return err
			}
		}
		token = metawire.RootTailTokenFromProto(resp.GetToken())
		if after != nil {
			*after = token
		}
		for _, record := range resp.GetEvents() {
			m.applyRootEvent(ctx, metawire.RootEventFromProto(record.GetEvent()))
		}
	}
}

func (m *monitor) applyRootEvent(ctx context.Context, event rootevent.Event) {
	switch event.Kind {
	case rootevent.KindMountRetired:
		if event.Mount == nil || event.Mount.MountID == "" {
			return
		}
		m.retireMount(model.MountID(event.Mount.MountID))
	case rootevent.KindQuotaFenceUpdated:
		if m.quotas == nil || event.QuotaFence == nil {
			return
		}
		m.quotas.markFenceUpdated(&coordpb.QuotaFenceInfo{
			Subject: &coordpb.QuotaSubject{
				MountId:     event.QuotaFence.Mount,
				SubtreeRoot: event.QuotaFence.RootInode,
			},
			LimitBytes:  event.QuotaFence.LimitBytes,
			LimitInodes: event.QuotaFence.LimitInodes,
			Era:         event.QuotaFence.Era,
			Frontier:    event.QuotaFence.Frontier,
		})
	case rootevent.KindSubtreeHandoffStarted:
		if event.SubtreeAuthority == nil {
			return
		}
		m.completePendingSubtreeHandoff(ctx, event.SubtreeAuthority.Mount, event.SubtreeAuthority.RootInode, event.SubtreeAuthority.Frontier)
	}
}

func (m *monitor) retireMount(id model.MountID) {
	if id == "" {
		return
	}
	if m.cache != nil {
		m.cache.markRetired(id)
	}
	if m.quotas != nil {
		m.quotas.purgeMount(id)
	}
	if m.router != nil {
		m.router.RetireMount(id)
	}
}

func (m *monitor) completePendingSubtreeHandoff(ctx context.Context, mount string, rootInode, frontier uint64) {
	if m.subtrees == nil || mount == "" || rootInode == 0 || frontier == 0 {
		return
	}
	if err := m.subtrees.CompleteSubtreeHandoff(ctx, model.MountID(mount), model.InodeID(rootInode), frontier); err != nil {
		log.Printf("fsmeta monitor: complete pending subtree handoff mount=%s root=%d frontier=%d: %v",
			mount, rootInode, frontier, err)
	}
}

func (m *monitor) wait(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		d = defaultMonitorInterval
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-m.stop:
		return false
	case <-timer.C:
		return true
	}
}

func (m *monitor) Close() error {
	if m == nil {
		return nil
	}
	m.once.Do(func() { close(m.stop) })
	<-m.done
	return nil
}
