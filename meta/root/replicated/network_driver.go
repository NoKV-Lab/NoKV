// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package replicated

import (
	"math"
	"os"
	"slices"
	"sync"
	"time"

	rootfile "github.com/feichai0017/NoKV/meta/root/storage/file"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/storage/vfs"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

const defaultNetworkTickInterval = 1000 * time.Millisecond
const defaultNetworkElectionTick = 10
const defaultNetworkHeartbeatTick = 1
const defaultAppendWaitTimeout = 5 * time.Second

// NetworkConfig wires one local raft node to a transport and a fixed peer set.
type NetworkConfig struct {
	ID                uint64
	WorkDir           string
	PeerIDs           []uint64
	Transport         Transport
	TickInterval      time.Duration
	AppendWaitTimeout time.Duration
}

// NetworkDriver hosts one local raft rawnode and exchanges messages through a
// transport, which is the first real landing point for multi-process metadata
// replication.
type NetworkDriver struct {
	mu                sync.Mutex
	closeOnce         sync.Once
	id                uint64
	workdir           string
	adapter           *virtualLogAdapter
	node              *networkNode
	transport         Transport
	appendWaitTimeout time.Duration
	ticksPaused       bool
	stopCh            chan struct{}
	wg                sync.WaitGroup
}

// NewNetworkDriver creates one transport-backed local metadata replication node.
func NewNetworkDriver(cfg NetworkConfig) (*NetworkDriver, error) {
	if cfg.ID == 0 {
		return nil, errNetworkDriverRequiresID
	}
	if cfg.WorkDir == "" {
		return nil, errNetworkDriverRequiresWorkDir
	}
	if cfg.Transport == nil {
		return nil, errNetworkDriverRequiresTransport
	}
	if len(cfg.PeerIDs) == 0 {
		cfg.PeerIDs = []uint64{cfg.ID}
	}
	if !slices.Contains(cfg.PeerIDs, cfg.ID) {
		return nil, errLocalNodeMissingFromPeerSet(cfg.ID, cfg.PeerIDs)
	}
	if cfg.TickInterval <= 0 {
		cfg.TickInterval = defaultNetworkTickInterval
	}
	if cfg.AppendWaitTimeout <= 0 {
		cfg.AppendWaitTimeout = defaultAppendWaitTimeout
	}
	driver := &NetworkDriver{
		id:                cfg.ID,
		workdir:           cfg.WorkDir,
		transport:         cfg.Transport,
		appendWaitTimeout: cfg.AppendWaitTimeout,
		stopCh:            make(chan struct{}),
	}
	if err := os.MkdirAll(cfg.WorkDir, 0o755); err != nil {
		return nil, err
	}
	adapter, err := newVirtualLogAdapter(rootfile.NewStore(vfs.Ensure(nil), cfg.WorkDir))
	if err != nil {
		return nil, err
	}
	driver.adapter = adapter
	node, err := newNetworkNode(cfg, driver.handleTransportMessage)
	if err != nil {
		return nil, err
	}
	driver.node = node
	driver.mu.Lock()
	_, outbound, err := driver.drainLocked()
	driver.mu.Unlock()
	if err != nil {
		_ = driver.Close()
		return nil, err
	}
	if err := driver.sendMessages(outbound); err != nil {
		_ = driver.Close()
		return nil, err
	}
	driver.wg.Add(1)
	go driver.tickLoop(cfg.TickInterval)
	return driver, nil
}

func (d *NetworkDriver) IsLeader() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.node == nil {
		return false
	}
	return d.node.raw.Status().RaftState == myraft.StateLeader
}

func (d *NetworkDriver) LeaderID() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.node == nil {
		return 0
	}
	return d.node.raw.Status().Lead
}

func (d *NetworkDriver) Campaign() error {
	d.mu.Lock()
	if d.node == nil {
		d.mu.Unlock()
		return errNetworkDriverClosed
	}
	if err := d.node.raw.Campaign(); err != nil {
		d.mu.Unlock()
		return err
	}
	_, outbound, err := d.drainLocked()
	d.mu.Unlock()
	if err != nil {
		return err
	}
	// Campaign has already advanced the local raft state. Transport sends are
	// best-effort and will be retried by subsequent ticks; surfacing a transient
	// send timeout here makes callers observe a failure for an election that may
	// still legitimately complete.
	_ = d.sendMessages(outbound)
	return nil
}

func (d *NetworkDriver) PauseTicks() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ticksPaused = true
}

func (d *NetworkDriver) ResumeTicks() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ticksPaused = false
}

func (d *NetworkDriver) Tick() error {
	d.mu.Lock()
	if d.node == nil {
		d.mu.Unlock()
		return errNetworkDriverClosed
	}
	d.node.raw.Tick()
	_, outbound, err := d.drainLocked()
	d.mu.Unlock()
	if err != nil {
		return err
	}
	return d.sendMessages(outbound)
}

func (d *NetworkDriver) Close() error {
	var err error
	d.closeOnce.Do(func() {
		close(d.stopCh)
		d.wg.Wait()
		d.mu.Lock()
		transport := d.transport
		if d.transport != nil {
			d.transport = nil
		}
		d.node = nil
		d.mu.Unlock()
		if transport != nil {
			err = transport.Close()
		}
	})
	return err
}

func (d *NetworkDriver) tickLoop(interval time.Duration) {
	defer d.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-d.stopCh:
			return
		case <-ticker.C:
			d.mu.Lock()
			var outbound []myraft.Message
			if d.node != nil && !d.ticksPaused {
				d.node.raw.Tick()
				_, outbound, _ = d.drainLocked()
			}
			d.mu.Unlock()
			_ = d.sendMessages(outbound)
		}
	}
}

func (d *NetworkDriver) handleTransportMessage(msg myraft.Message) error {
	d.mu.Lock()
	if d.node == nil {
		d.mu.Unlock()
		return errNetworkDriverClosed
	}
	if err := d.node.raw.Step(msg); err != nil {
		d.mu.Unlock()
		return err
	}
	_, outbound, err := d.drainLocked()
	d.mu.Unlock()
	if err != nil {
		return err
	}
	// Incoming raft messages are accepted once Step and Ready processing
	// succeed. Outbound replies are transport best-effort; rejecting the inbound
	// RPC because a nested reply send timed out can make an already-applied
	// message look like it failed at the sender.
	_ = d.sendMessages(outbound)
	return nil
}

type networkNode struct {
	id      uint64
	peerIDs []uint64
	storage *myraft.MemoryStorage
	raw     *myraft.RawNode
}

func newNetworkNode(cfg NetworkConfig, handler MessageHandler) (*networkNode, error) {
	storage := myraft.NewMemoryStorage()
	state, err := loadProtocolState(cfg.WorkDir)
	if err != nil {
		return nil, err
	}
	if !myraft.IsEmptySnap(state.Snapshot) {
		if err := storage.ApplySnapshot(state.Snapshot); err != nil {
			return nil, err
		}
	}
	if !myraft.IsEmptyHardState(state.HardState) {
		if err := storage.SetHardState(state.HardState); err != nil {
			return nil, err
		}
	}
	if len(state.Entries) > 0 {
		if err := storage.Append(state.Entries); err != nil {
			return nil, err
		}
	}
	rcfg := &myraft.Config{
		ID:              cfg.ID,
		ElectionTick:    defaultNetworkElectionTick,
		HeartbeatTick:   defaultNetworkHeartbeatTick,
		Storage:         storage,
		MaxSizePerMsg:   math.MaxUint64,
		MaxInflightMsgs: 256,
		PreVote:         true,
	}
	raw, err := myraft.NewRawNode(rcfg)
	if err != nil {
		return nil, err
	}
	restarted := !myraft.IsEmptyHardState(state.HardState) || !myraft.IsEmptySnap(state.Snapshot) || len(state.Entries) > 0
	if !restarted {
		peers := make([]myraft.Peer, 0, len(cfg.PeerIDs))
		for _, id := range cfg.PeerIDs {
			peers = append(peers, myraft.Peer{ID: id})
		}
		if err := raw.Bootstrap(peers); err != nil {
			return nil, err
		}
	} else {
		for _, id := range cfg.PeerIDs {
			raw.ApplyConfChange(raftpb.ConfChange{NodeID: id, Type: raftpb.ConfChangeAddNode}.AsV2())
		}
	}
	cfg.Transport.SetHandler(handler)
	return &networkNode{
		id:      cfg.ID,
		peerIDs: slices.Clone(cfg.PeerIDs),
		storage: storage,
		raw:     raw,
	}, nil
}

func (d *NetworkDriver) sendMessages(msgs []myraft.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	d.mu.Lock()
	transport := d.transport
	d.mu.Unlock()
	if transport == nil {
		return errNetworkDriverClosed
	}
	return transport.Send(msgs...)
}
