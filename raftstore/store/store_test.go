// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"bytes"
	"context"
	"fmt"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootmaterialize "github.com/feichai0017/NoKV/meta/root/materialize"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"
	"sync"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/coordinator/storecontrol"
	local "github.com/feichai0017/NoKV/local"
	"github.com/feichai0017/NoKV/meta/topology"
	myraft "github.com/feichai0017/NoKV/raft"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"github.com/feichai0017/NoKV/raftstore/peer"
	"github.com/feichai0017/NoKV/raftstore/raftlog"
	raftstorestats "github.com/feichai0017/NoKV/raftstore/stats"
	"github.com/feichai0017/NoKV/txn/latch"
	"github.com/feichai0017/NoKV/txn/percolator"
	txnstore "github.com/feichai0017/NoKV/txn/storage"
	"github.com/stretchr/testify/require"
)

func TestStoreSchedulerReceivesRegionHeartbeats(t *testing.T) {
	const eventuallyTimeout = 3 * time.Second

	sink := newTestSchedulerSink()
	rs := NewStore(Config{Scheduler: sink, StoreID: 1})
	defer rs.Close()

	cfg := &peer.Config{
		RaftConfig: myraft.Config{
			ID:              1,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
		},
		Transport: noopTransport{},
		Apply:     func([]myraft.Entry) error { return nil },
		Region: &localmeta.RegionMeta{
			ID:       42,
			StartKey: []byte("a"),
			EndKey:   []byte("b"),
		},
	}

	peer, err := rs.StartPeer(cfg, []myraft.Peer{{ID: 1}})
	require.NoError(t, err)
	defer rs.StopPeer(peer.ID())

	var snapshot []regionHeartbeat
	require.Eventually(t, func() bool {
		snapshot = sink.RegionSnapshot()
		return len(snapshot) == 1
	}, eventuallyTimeout, 10*time.Millisecond)
	require.Equal(t, uint64(42), snapshot[0].Descriptor.RegionID)
	require.False(t, snapshot[0].LastHeartbeat.IsZero())

	require.NoError(t, rs.applyRegionState(42, metaregion.ReplicaStateRemoving))
	require.Eventually(t, func() bool {
		snapshot = sink.RegionSnapshot()
		return len(snapshot) == 1 && snapshot[0].Descriptor.State == metaregion.ReplicaStateRemoving
	}, eventuallyTimeout, 10*time.Millisecond)
	require.Equal(t, metaregion.ReplicaStateRemoving, snapshot[0].Descriptor.State)

	require.NoError(t, rs.applyRegionRemoval(42))
	require.Eventually(t, func() bool {
		return len(sink.RegionSnapshot()) == 0
	}, eventuallyTimeout, 10*time.Millisecond)
}

func TestStoreRegionApplyDoesNotBlockOnSchedulerPublish(t *testing.T) {
	sink := &slowSchedulerSink{
		testSchedulerSink: *newTestSchedulerSink(),
		publishDelay:      200 * time.Millisecond,
	}
	rs := NewStore(Config{Scheduler: sink, StoreID: 1})
	defer rs.Close()

	cfg := &peer.Config{
		RaftConfig: myraft.Config{
			ID:              1,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
		},
		Transport: noopTransport{},
		Apply:     func([]myraft.Entry) error { return nil },
		Region: &localmeta.RegionMeta{
			ID:       88,
			StartKey: []byte("a"),
			EndKey:   []byte("b"),
		},
	}

	start := time.Now()
	peer, err := rs.StartPeer(cfg, []myraft.Peer{{ID: 1}})
	elapsed := time.Since(start)
	require.NoError(t, err)
	defer rs.StopPeer(peer.ID())
	require.Less(t, elapsed, sink.delay()/2, "region apply path should not block on slow coordinator publish")
	require.Eventually(t, func() bool {
		snapshot := sink.RegionSnapshot()
		return len(snapshot) == 1 && snapshot[0].Descriptor.RegionID == 88
	}, time.Second, 10*time.Millisecond)
}

func TestStoreSchedulerPeriodicHeartbeats(t *testing.T) {
	coord := newTestSchedulerSink()
	rs := NewStore(Config{
		Scheduler:         coord,
		StoreID:           9,
		PeerBuilder:       testPeerBuilder(9),
		HeartbeatInterval: 25 * time.Millisecond,
	})
	defer rs.Close()

	cfg := &peer.Config{
		RaftConfig: myraft.Config{
			ID:              7,
			ElectionTick:    5,
			HeartbeatTick:   1,
			MaxSizePerMsg:   1 << 20,
			MaxInflightMsgs: 256,
		},
		Transport: noopTransport{},
		Apply:     func([]myraft.Entry) error { return nil },
		Region: &localmeta.RegionMeta{
			ID:       77,
			StartKey: []byte("alpha"),
			EndKey:   []byte("beta"),
		},
	}
	peer, err := rs.StartPeer(cfg, []myraft.Peer{{ID: 7}})
	require.NoError(t, err)
	defer rs.StopPeer(peer.ID())
	require.NoError(t, peer.Campaign())

	require.Eventually(t, func() bool {
		_, ok := coord.LastUpdate(77)
		return ok
	}, time.Second, 10*time.Millisecond)

	first, _ := coord.LastUpdate(77)
	time.Sleep(80 * time.Millisecond)
	second, ok := coord.LastUpdate(77)
	require.True(t, ok)
	require.True(t, second.After(first))

	storeSnap := coord.StoreSnapshot()
	require.Len(t, storeSnap, 1)
	require.Equal(t, uint64(1), storeSnap[0].LeaderNum)
	regionSnap := coord.RegionSnapshot()
	require.NotEmpty(t, regionSnap)
	require.Equal(t, uint64(77), regionSnap[0].Descriptor.RegionID)
	require.False(t, regionSnap[0].LastHeartbeat.IsZero())
}

func TestStoreProposeSplitApplies(t *testing.T) {
	storeID := uint64(11)
	sink := newTestSchedulerSink()
	rs := NewStore(Config{
		Scheduler:         sink,
		PeerBuilder:       testPeerBuilder(storeID),
		StoreID:           storeID,
		HeartbeatInterval: 10 * time.Millisecond,
	})
	defer rs.Close()

	parentMeta := localmeta.RegionMeta{
		ID:       3000,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Peers:    []metaregion.Peer{{StoreID: storeID, PeerID: 31}},
	}
	parentCfg, err := testPeerBuilder(storeID)(parentMeta)
	require.NoError(t, err)
	parentPeer, err := rs.StartPeer(parentCfg, []myraft.Peer{{ID: 31}})
	require.NoError(t, err)
	defer rs.StopPeer(parentPeer.ID())
	require.NoError(t, parentPeer.Campaign())
	require.Eventually(t, func() bool { return len(sink.RegionSnapshot()) >= 1 }, time.Second, 10*time.Millisecond)
	sink.ResetHistory()

	childMeta := localmeta.RegionMeta{
		ID:       3001,
		StartKey: []byte("m"),
		EndKey:   []byte("z"),
		Peers:    []metaregion.Peer{{StoreID: storeID, PeerID: 32}},
	}
	require.NoError(t, rs.ProposeSplit(parentMeta.ID, childMeta, childMeta.StartKey))

	require.Eventually(t, func() bool {
		_, ok := rs.RegionMetaByID(childMeta.ID)
		return ok
	}, time.Second, 10*time.Millisecond)

	parentUpdated, ok := rs.RegionMetaByID(parentMeta.ID)
	require.True(t, ok)
	require.Equal(t, []byte("m"), parentUpdated.EndKey)

	childUpdated, ok := rs.RegionMetaByID(childMeta.ID)
	require.True(t, ok)
	require.Equal(t, []byte("m"), childUpdated.StartKey)
	require.Eventually(t, func() bool {
		snapshot := sink.RegionSnapshot()
		if len(snapshot) < 2 {
			return false
		}
		for _, info := range snapshot {
			if info.Descriptor.RegionID == childMeta.ID {
				return len(info.Descriptor.Lineage) == 1 &&
					info.Descriptor.Lineage[0].RegionID == parentMeta.ID &&
					info.Descriptor.Lineage[0].Kind == topology.LineageKindSplitParent
			}
		}
		return false
	}, time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		return hasSchedulerEventSubsequence(sink.EventHistory(),
			schedulerEvent{kind: "root", regionID: parentMeta.ID, rootKind: rootevent.KindRegionSplitPlanned},
			schedulerEvent{kind: "root", regionID: parentMeta.ID, rootKind: rootevent.KindRegionSplitCommitted},
		)
	}, time.Second, 10*time.Millisecond)
	for _, ev := range sink.EventHistory() {
		require.NotEqual(t, rootevent.KindRegionBootstrap, ev.rootKind)
		require.NotEqual(t, rootevent.KindRegionDescriptorPublished, ev.rootKind)
		require.NotEqual(t, rootevent.KindRegionTombstoned, ev.rootKind)
	}

	sink.ResetHistory()
	require.NoError(t, rs.ProposeSplit(parentMeta.ID, childMeta, childMeta.StartKey))
	time.Sleep(50 * time.Millisecond)
	for _, ev := range sink.EventHistory() {
		require.NotEqual(t, rootevent.KindRegionSplitPlanned, ev.rootKind)
		require.NotEqual(t, rootevent.KindRegionSplitCommitted, ev.rootKind)
	}
}

func TestStoreProposeMergeApplies(t *testing.T) {
	storeID := uint64(12)
	sink := newTestSchedulerSink()
	rs := NewStore(Config{
		Scheduler:         sink,
		PeerBuilder:       testPeerBuilder(storeID),
		StoreID:           storeID,
		HeartbeatInterval: 10 * time.Millisecond,
	})
	defer rs.Close()

	parentMeta := localmeta.RegionMeta{
		ID:       4000,
		StartKey: []byte("a"),
		EndKey:   []byte("m"),
		Peers:    []metaregion.Peer{{StoreID: storeID, PeerID: 41}},
	}
	parentCfg, err := testPeerBuilder(storeID)(parentMeta)
	require.NoError(t, err)
	parentPeer, err := rs.StartPeer(parentCfg, []myraft.Peer{{ID: 41}})
	require.NoError(t, err)
	defer rs.StopPeer(parentPeer.ID())
	require.NoError(t, parentPeer.Campaign())

	sourceMeta := localmeta.RegionMeta{
		ID:       4001,
		StartKey: []byte("m"),
		EndKey:   []byte("z"),
		Peers:    []metaregion.Peer{{StoreID: storeID, PeerID: 42}},
	}
	sourceCfg, err := testPeerBuilder(storeID)(sourceMeta)
	require.NoError(t, err)
	sourcePeer, err := rs.StartPeer(sourceCfg, []myraft.Peer{{ID: 42}})
	require.NoError(t, err)
	defer rs.StopPeer(sourcePeer.ID())
	require.NoError(t, sourcePeer.Campaign())
	require.Eventually(t, func() bool { return len(sink.RegionSnapshot()) >= 2 }, time.Second, 10*time.Millisecond)
	sink.ResetHistory()

	require.NoError(t, rs.ProposeMerge(parentMeta.ID, sourceMeta.ID))

	require.Eventually(t, func() bool {
		_, ok := rs.RegionMetaByID(sourceMeta.ID)
		return !ok
	}, time.Second, 10*time.Millisecond)

	parentUpdated, ok := rs.RegionMetaByID(parentMeta.ID)
	require.True(t, ok)
	require.True(t, bytes.Equal(parentUpdated.EndKey, []byte("z")))

	_, ok = rs.RegionMetaByID(sourceMeta.ID)
	require.False(t, ok)
	if peer, exists := rs.Peer(sourceMeta.Peers[0].PeerID); exists {
		rs.StopPeer(peer.ID())
	}
	require.Eventually(t, func() bool {
		snapshot := sink.RegionSnapshot()
		for _, info := range snapshot {
			if info.Descriptor.RegionID == parentMeta.ID {
				return len(info.Descriptor.Lineage) == 1 &&
					info.Descriptor.Lineage[0].RegionID == sourceMeta.ID &&
					info.Descriptor.Lineage[0].Kind == topology.LineageKindMergeSource
			}
		}
		return false
	}, time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		return hasSchedulerEventSubsequence(sink.EventHistory(),
			schedulerEvent{kind: "root", regionID: parentMeta.ID, rootKind: rootevent.KindRegionMergePlanned},
			schedulerEvent{kind: "root", regionID: parentMeta.ID, rootKind: rootevent.KindRegionMerged},
		)
	}, time.Second, 10*time.Millisecond)
	for _, ev := range sink.EventHistory() {
		require.NotEqual(t, rootevent.KindRegionBootstrap, ev.rootKind)
		require.NotEqual(t, rootevent.KindRegionDescriptorPublished, ev.rootKind)
		require.NotEqual(t, rootevent.KindRegionTombstoned, ev.rootKind)
	}
}

func hasSchedulerEventSubsequence(history []schedulerEvent, want ...schedulerEvent) bool {
	if len(want) == 0 {
		return true
	}
	idx := 0
	for _, got := range history {
		if schedulerEventMatches(got, want[idx]) {
			idx++
			if idx == len(want) {
				return true
			}
		}
	}
	return false
}

func schedulerEventMatches(got, want schedulerEvent) bool {
	if want.kind != "" && got.kind != want.kind {
		return false
	}
	if want.regionID != 0 && got.regionID != want.regionID {
		return false
	}
	if want.rootKind != 0 && got.rootKind != want.rootKind {
		return false
	}
	return true
}

func TestStoreSplitMergeLifecycle(t *testing.T) {
	storeID := uint64(13)
	rs := NewStore(Config{
		PeerBuilder:       testPeerBuilder(storeID),
		StoreID:           storeID,
		HeartbeatInterval: 15 * time.Millisecond,
	})
	defer rs.Close()

	parentMeta := localmeta.RegionMeta{
		ID:       5000,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Peers:    []metaregion.Peer{{StoreID: storeID, PeerID: 51}},
	}
	parentCfg, err := testPeerBuilder(storeID)(parentMeta)
	require.NoError(t, err)
	parentPeer, err := rs.StartPeer(parentCfg, []myraft.Peer{{ID: 51}})
	require.NoError(t, err)
	defer rs.StopPeer(parentPeer.ID())
	require.NoError(t, parentPeer.Campaign())

	childMeta := localmeta.RegionMeta{
		ID:       5001,
		StartKey: []byte("m"),
		EndKey:   []byte("z"),
		Peers:    []metaregion.Peer{{StoreID: storeID, PeerID: 52}},
	}
	require.NoError(t, rs.ProposeSplit(parentMeta.ID, childMeta, childMeta.StartKey))
	require.Eventually(t, func() bool {
		_, ok := rs.RegionMetaByID(childMeta.ID)
		return ok
	}, time.Second, 10*time.Millisecond)

	require.NoError(t, rs.ProposeMerge(parentMeta.ID, childMeta.ID))
	require.Eventually(t, func() bool {
		_, ok := rs.RegionMetaByID(childMeta.ID)
		return !ok
	}, time.Second, 10*time.Millisecond)

	parentUpdated, ok := rs.RegionMetaByID(parentMeta.ID)
	require.True(t, ok)
	require.True(t, bytes.Equal(parentUpdated.EndKey, []byte("z")))
	if peer, exists := rs.Peer(childMeta.Peers[0].PeerID); exists {
		rs.StopPeer(peer.ID())
	}
}

func TestStoreRestartPreservesSplitMergeLocalMeta(t *testing.T) {
	storeID := uint64(21)
	dir := t.TempDir()
	localMeta, err := localmeta.OpenLocalStore(dir, nil)
	require.NoError(t, err)
	defer func() { _ = localMeta.Close() }()

	rs := NewStore(Config{
		PeerBuilder: testPeerBuilder(storeID),
		StoreID:     storeID,
		LocalMeta:   localMeta,
	})

	parentMeta := localmeta.RegionMeta{
		ID:       7000,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		State:    metaregion.ReplicaStateRunning,
		Peers:    []metaregion.Peer{{StoreID: storeID, PeerID: 71}},
	}
	parentCfg, err := testPeerBuilder(storeID)(parentMeta)
	require.NoError(t, err)
	parentPeer, err := rs.StartPeer(parentCfg, []myraft.Peer{{ID: 71}})
	require.NoError(t, err)
	require.NoError(t, parentPeer.Campaign())

	childMeta := localmeta.RegionMeta{
		ID:       7001,
		StartKey: []byte("m"),
		EndKey:   []byte("z"),
		State:    metaregion.ReplicaStateRunning,
		Peers:    []metaregion.Peer{{StoreID: storeID, PeerID: 72}},
	}
	require.NoError(t, rs.ProposeSplit(parentMeta.ID, childMeta, childMeta.StartKey))
	require.Eventually(t, func() bool {
		_, ok := rs.RegionMetaByID(childMeta.ID)
		return ok
	}, time.Second, 10*time.Millisecond)

	require.NoError(t, rs.ProposeMerge(parentMeta.ID, childMeta.ID))
	require.Eventually(t, func() bool {
		_, ok := rs.RegionMetaByID(childMeta.ID)
		return !ok
	}, time.Second, 10*time.Millisecond)

	rs.Close()
	require.NoError(t, localMeta.Close())

	reopenedMeta, err := localmeta.OpenLocalStore(dir, nil)
	require.NoError(t, err)
	defer func() { _ = reopenedMeta.Close() }()

	reopened := NewStore(Config{
		PeerBuilder: testPeerBuilder(storeID),
		StoreID:     storeID,
		LocalMeta:   reopenedMeta,
	})
	defer reopened.Close()

	metas := reopened.RegionMetas()
	require.Len(t, metas, 1)
	require.Equal(t, uint64(7000), metas[0].ID)
	require.Equal(t, []byte("z"), metas[0].EndKey)
	_, ok := reopened.RegionMetaByID(childMeta.ID)
	require.False(t, ok)
}

func TestStoreHandleSplitCommandReplayIsIdempotent(t *testing.T) {
	storeID := uint64(31)
	rs := NewStore(Config{
		PeerBuilder: testPeerBuilder(storeID),
		StoreID:     storeID,
	})
	defer rs.Close()

	parentMeta := localmeta.RegionMeta{
		ID:       8100,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		State:    metaregion.ReplicaStateRunning,
		Peers:    []metaregion.Peer{{StoreID: storeID, PeerID: 81}},
	}
	require.NoError(t, rs.applyRegionMeta(parentMeta))

	childMeta := localmeta.RegionMeta{
		ID:       8101,
		StartKey: []byte("m"),
		EndKey:   []byte("z"),
		State:    metaregion.ReplicaStateRunning,
		Peers:    []metaregion.Peer{{StoreID: storeID, PeerID: 82}},
	}
	_, err := rs.splitRegionLocal(parentMeta.ID, childMeta)
	require.NoError(t, err)

	cmd := &raftcmdpb.SplitCommand{
		ParentRegionId: parentMeta.ID,
		SplitKey:       []byte("m"),
		Child:          localmeta.DescriptorToProto(childMeta),
	}
	require.NoError(t, rs.handleSplitCommand(cmd))

	parentUpdated, ok := rs.RegionMetaByID(parentMeta.ID)
	require.True(t, ok)
	require.Equal(t, []byte("m"), parentUpdated.EndKey)
	childUpdated, ok := rs.RegionMetaByID(childMeta.ID)
	require.True(t, ok)
	require.Equal(t, childMeta.Peers, childUpdated.Peers)
}

func TestStoreHandleMergeCommandReplayIsIdempotent(t *testing.T) {
	storeID := uint64(32)
	rs := NewStore(Config{
		PeerBuilder: testPeerBuilder(storeID),
		StoreID:     storeID,
	})
	defer rs.Close()

	parentMeta := localmeta.RegionMeta{
		ID:       8200,
		StartKey: []byte("a"),
		EndKey:   []byte("m"),
		State:    metaregion.ReplicaStateRunning,
		Peers:    []metaregion.Peer{{StoreID: storeID, PeerID: 91}},
	}
	sourceMeta := localmeta.RegionMeta{
		ID:       8201,
		StartKey: []byte("m"),
		EndKey:   []byte("z"),
		State:    metaregion.ReplicaStateRunning,
		Peers:    []metaregion.Peer{{StoreID: storeID, PeerID: 92}},
	}
	require.NoError(t, rs.applyRegionMeta(parentMeta))
	require.NoError(t, rs.applyRegionMeta(sourceMeta))
	require.NoError(t, rs.applyRegionRemoval(sourceMeta.ID))

	require.NoError(t, rs.handleMergeCommand(&raftcmdpb.MergeCommand{
		TargetRegionId: parentMeta.ID,
		SourceRegionId: sourceMeta.ID,
	}))
}

type noopTransport struct{}

func (noopTransport) Send(context.Context, myraft.Message) {}

type testSchedulerSink struct {
	mu      sync.RWMutex
	regions map[uint64]regionHeartbeat
	stores  map[uint64]storecontrol.StoreStats
	history []schedulerEvent
}

type slowSchedulerSink struct {
	testSchedulerSink
	mu           sync.Mutex
	publishDelay time.Duration
}

type degradedSchedulerSink struct {
	testSchedulerSink
	status storecontrol.Status
}

type regionHeartbeat struct {
	Descriptor    topology.Descriptor
	LastHeartbeat time.Time
}

type schedulerEvent struct {
	kind     string
	regionID uint64
	rootKind rootevent.Kind
}

func newTestSchedulerSink() *testSchedulerSink {
	return &testSchedulerSink{
		regions: make(map[uint64]regionHeartbeat),
		stores:  make(map[uint64]storecontrol.StoreStats),
	}
}

func (s *testSchedulerSink) ReportRegionHeartbeat(_ context.Context, regionID uint64) {
	if s == nil || regionID == 0 {
		return
	}
	s.mu.Lock()
	info := s.regions[regionID]
	info.Descriptor.RegionID = regionID
	info.LastHeartbeat = time.Now()
	s.regions[regionID] = regionHeartbeat{
		Descriptor:    info.Descriptor,
		LastHeartbeat: info.LastHeartbeat,
	}
	s.history = append(s.history, schedulerEvent{kind: "publish", regionID: regionID})
	s.mu.Unlock()
}

func (s *testSchedulerSink) PublishRootEvent(_ context.Context, event rootevent.Event) error {
	if s == nil || event.Kind == rootevent.KindUnknown {
		return nil
	}
	s.mu.Lock()
	descriptors := make(map[uint64]topology.Descriptor, len(s.regions))
	for id, info := range s.regions {
		descriptors[id] = info.Descriptor.Clone()
	}
	rootmaterialize.ApplyEventToDescriptors(descriptors, event)
	now := time.Now()
	s.regions = make(map[uint64]regionHeartbeat, len(descriptors))
	for id, desc := range descriptors {
		s.regions[id] = regionHeartbeat{
			Descriptor:    desc.Clone(),
			LastHeartbeat: now,
		}
	}
	s.history = append(s.history, schedulerEvent{kind: "root", regionID: rootEventRegionID(event), rootKind: event.Kind})
	s.mu.Unlock()
	return nil
}

func (s *testSchedulerSink) StoreHeartbeat(_ context.Context, stats storecontrol.StoreStats) []storecontrol.Operation {
	if s == nil || stats.StoreID == 0 {
		return nil
	}
	stats.UpdatedAt = time.Now()
	s.mu.Lock()
	s.stores[stats.StoreID] = stats
	s.mu.Unlock()
	return nil
}

func (s *testSchedulerSink) Status() storecontrol.Status {
	return storecontrol.Status{}
}

func (s *testSchedulerSink) RegionSnapshot() []regionHeartbeat {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	out := make([]regionHeartbeat, 0, len(s.regions))
	for _, info := range s.regions {
		out = append(out, regionHeartbeat{
			Descriptor:    info.Descriptor.Clone(),
			LastHeartbeat: info.LastHeartbeat,
		})
	}
	s.mu.RUnlock()
	return out
}

func (s *testSchedulerSink) StoreSnapshot() []storecontrol.StoreStats {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	out := make([]storecontrol.StoreStats, 0, len(s.stores))
	for _, st := range s.stores {
		out = append(out, st)
	}
	s.mu.RUnlock()
	return out
}

func (s *testSchedulerSink) LastUpdate(regionID uint64) (time.Time, bool) {
	if s == nil || regionID == 0 {
		return time.Time{}, false
	}
	s.mu.RLock()
	info, ok := s.regions[regionID]
	s.mu.RUnlock()
	if !ok {
		return time.Time{}, false
	}
	return info.LastHeartbeat, true
}

func (s *testSchedulerSink) EventHistory() []schedulerEvent {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	out := append([]schedulerEvent(nil), s.history...)
	s.mu.RUnlock()
	return out
}

func (s *testSchedulerSink) ResetHistory() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.history = nil
	s.mu.Unlock()
}

func (s *testSchedulerSink) Close() error {
	return nil
}

func (s *degradedSchedulerSink) Status() storecontrol.Status {
	return s.status
}

func (s *slowSchedulerSink) ReportRegionHeartbeat(ctx context.Context, regionID uint64) {
	if delay := s.delay(); delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return
		}
	}
	s.testSchedulerSink.ReportRegionHeartbeat(ctx, regionID)
}

func (s *slowSchedulerSink) PublishRootEvent(ctx context.Context, event rootevent.Event) error {
	if delay := s.delay(); delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.testSchedulerSink.PublishRootEvent(ctx, event)
}

func (s *slowSchedulerSink) delay() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.publishDelay
}

func (s *slowSchedulerSink) setPublishDelay(delay time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.publishDelay = delay
}

func rootEventRegionID(event rootevent.Event) uint64 {
	switch {
	case event.RegionDescriptor != nil:
		return event.RegionDescriptor.Descriptor.RegionID
	case event.RegionRemoval != nil:
		return event.RegionRemoval.RegionID
	case event.RangeSplit != nil:
		return event.RangeSplit.ParentRegionID
	case event.RangeMerge != nil:
		return event.RangeMerge.Merged.RegionID
	case event.PeerChange != nil:
		return event.PeerChange.RegionID
	default:
		return 0
	}
}

func testPeerBuilder(storeID uint64) PeerBuilder {
	return func(meta localmeta.RegionMeta) (*peer.Config, error) {
		var peerID uint64
		for _, peerMeta := range meta.Peers {
			if peerMeta.StoreID == storeID {
				peerID = peerMeta.PeerID
				break
			}
		}
		if peerID == 0 {
			return nil, fmt.Errorf("store %d missing peer in region %d", storeID, meta.ID)
		}
		cfg := &peer.Config{
			RaftConfig: myraft.Config{
				ID:              peerID,
				ElectionTick:    5,
				HeartbeatTick:   1,
				MaxSizePerMsg:   1 << 20,
				MaxInflightMsgs: 256,
				PreVote:         true,
			},
			Transport: noopTransport{},
			Apply:     func([]myraft.Entry) error { return nil },
			GroupID:   meta.ID,
			Region:    localmeta.CloneRegionMetaPtr(&meta),
		}
		return cfg, nil
	}
}

func openStoreDB(t *testing.T) (*local.DB, *localmeta.Store) {
	t.Helper()
	opt := local.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	localMeta, err := localmeta.OpenLocalStore(opt.WorkDir, nil)
	require.NoError(t, err)
	opt.ControlLogPointerSnapshot = raftstorestats.ControlLogPointers(localMeta.DurableRaftPointerSnapshot)
	db, err := local.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
		_ = localMeta.Close()
	})
	return db, localMeta
}

func mustPeerStorage(t *testing.T, db *local.DB, localMeta *localmeta.Store, groupID uint64) raftlog.PeerStorage {
	t.Helper()
	storage, err := raftlog.NewDBLog(db).Open(groupID, localMeta)
	require.NoError(t, err)
	return storage
}

func newTestMVCCApplier(db txnstore.Store) func(*raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
	latches := latch.NewManager(512)
	return func(req *raftcmdpb.RaftCmdRequest) (*raftcmdpb.RaftCmdResponse, error) {
		resp := &raftcmdpb.RaftCmdResponse{Header: req.GetHeader()}
		for _, r := range req.GetRequests() {
			if r == nil {
				continue
			}
			switch r.GetCmdType() {
			case raftcmdpb.CmdType_CMD_PREWRITE:
				result := &kvrpcpb.PrewriteResponse{Errors: percolator.Prewrite(db, latches, r.GetPrewrite())}
				resp.Responses = append(resp.Responses, &raftcmdpb.Response{Cmd: &raftcmdpb.Response_Prewrite{Prewrite: result}})
			case raftcmdpb.CmdType_CMD_COMMIT:
				err := percolator.Commit(db, latches, r.GetCommit())
				resp.Responses = append(resp.Responses, &raftcmdpb.Response{Cmd: &raftcmdpb.Response_Commit{Commit: &kvrpcpb.CommitResponse{Error: err}}})
			case raftcmdpb.CmdType_CMD_RESOLVE_LOCK:
				count, err := percolator.ResolveLock(db, latches, r.GetResolveLock())
				resp.Responses = append(resp.Responses, &raftcmdpb.Response{Cmd: &raftcmdpb.Response_ResolveLock{ResolveLock: &kvrpcpb.ResolveLockResponse{
					Error:         err,
					ResolvedLocks: count,
				}}})
			case raftcmdpb.CmdType_CMD_CHECK_TXN_STATUS:
				result := percolator.CheckTxnStatus(db, latches, r.GetCheckTxnStatus())
				resp.Responses = append(resp.Responses, &raftcmdpb.Response{Cmd: &raftcmdpb.Response_CheckTxnStatus{CheckTxnStatus: result}})
			case raftcmdpb.CmdType_CMD_TXN_HEART_BEAT:
				result := percolator.TxnHeartBeat(db, latches, r.GetTxnHeartBeat())
				resp.Responses = append(resp.Responses, &raftcmdpb.Response{Cmd: &raftcmdpb.Response_TxnHeartBeat{TxnHeartBeat: result}})
			case raftcmdpb.CmdType_CMD_MVCC_MAINTENANCE:
				count, keyErr, err := applyTestMVCCMaintenance(db, r.GetMvccMaintenance())
				if err != nil {
					return nil, err
				}
				resp.Responses = append(resp.Responses, &raftcmdpb.Response{Cmd: &raftcmdpb.Response_MvccMaintenance{MvccMaintenance: &kvrpcpb.MVCCMaintenanceResponse{
					Error:          keyErr,
					AppliedEntries: count,
				}}})
			default:
				return nil, fmt.Errorf("unsupported test command %v", r.GetCmdType())
			}
		}
		return resp, nil
	}
}

func applyTestMVCCMaintenance(db txnstore.Store, req *kvrpcpb.MVCCMaintenanceRequest) (uint64, *kvrpcpb.KeyError, error) {
	var entries []*txnstore.Entry
	for _, tombstone := range req.GetTombstones() {
		if tombstone == nil {
			continue
		}
		cf, ok := testMaintenanceColumnFamily(tombstone.GetColumnFamily())
		if !ok {
			releaseTestEntries(entries)
			return 0, &kvrpcpb.KeyError{Abort: "invalid column family"}, nil
		}
		if len(tombstone.GetKey()) == 0 {
			releaseTestEntries(entries)
			return 0, &kvrpcpb.KeyError{Abort: "empty key"}, nil
		}
		entries = append(entries, txnstore.NewInternalEntry(cf, tombstone.GetKey(), tombstone.GetVersion(), nil, txnstore.BitDelete, 0))
	}
	defer releaseTestEntries(entries)
	if err := db.ApplyInternalEntries(entries); err != nil {
		return 0, nil, err
	}
	return uint64(len(entries)), nil, nil
}

func releaseTestEntries(entries []*txnstore.Entry) {
	for _, entry := range entries {
		if entry != nil {
			entry.DecrRef()
		}
	}
}

func testMaintenanceColumnFamily(cf kvrpcpb.InternalEntryTombstone_ColumnFamily) (txnstore.ColumnFamily, bool) {
	switch cf {
	case kvrpcpb.InternalEntryTombstone_DEFAULT:
		return txnstore.CFDefault, true
	case kvrpcpb.InternalEntryTombstone_WRITE:
		return txnstore.CFWrite, true
	default:
		return 0, false
	}
}
