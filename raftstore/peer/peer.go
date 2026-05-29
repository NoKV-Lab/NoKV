// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peer

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"

	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/command"
	"github.com/feichai0017/NoKV/raftstore/failpoints"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"github.com/feichai0017/NoKV/raftstore/raftlog"
	"github.com/feichai0017/NoKV/raftstore/transport"
	"github.com/feichai0017/NoKV/utils"
	raftpb "go.etcd.io/raft/v3/raftpb"
	proto "google.golang.org/protobuf/proto"
)

// ApplyFunc consumes committed raft log entries and applies them to the user
// state machine.
type ApplyFunc func(entries []myraft.Entry) error

// ApplyTask is the committed-entry unit handed from the raft peer into the
// apply runner. The peer owns raft ordering, watermarks, and compaction; the
// runner owns state-machine scheduling and apply execution.
type ApplyTask struct {
	Entries []myraft.Entry
}

// ApplyResult reports one ApplyTask after the runner made it externally
// visible or hit a fatal apply error.
type ApplyResult struct {
	Entries []myraft.Entry
	Err     error
}

// ApplyRunner decouples committed-entry ingestion from apply completion. It is
// the production path for async apply: peers submit committed tasks and handle
// results uniformly when the runner finishes them.
type ApplyRunner interface {
	SubmitApply(task ApplyTask, done func(ApplyResult)) error
}

// AdminApplyFunc consumes admin commands (split, merge, etc.).
type AdminApplyFunc func(cmd *raftcmdpb.AdminCommand) error

// SnapshotExportFunc materializes region state for one outgoing raft snapshot
// message as an opaque payload.
type SnapshotExportFunc func(region localmeta.RegionMeta) ([]byte, error)

// SnapshotApplyFunc imports region state from one incoming raft snapshot
// payload and returns the region metadata carried by that payload.
type SnapshotApplyFunc func(payload []byte) (localmeta.RegionMeta, error)

// Peer wraps a RawNode with simple storage and apply plumbing.
type Peer struct {
	mu                        sync.Mutex
	readyMu                   sync.Mutex
	id                        uint64
	node                      *myraft.RawNode
	storage                   raftlog.PeerStorage
	transport                 transport.Transport
	apply                     ApplyFunc
	applyRunner               ApplyRunner
	adminApply                AdminApplyFunc
	raftLog                   *raftLogTracker
	snapshotQueue             *snapshotResendQueue
	logRetainEntries          uint64
	confChangeHook            ConfChangeHandler
	snapshotExport            SnapshotExportFunc
	snapshotApply             SnapshotApplyFunc
	applyMark                 *utils.WaterMark
	applyCloser               *utils.Closer
	applyLimit                uint64
	stopCtx                   context.Context
	stopCancel                context.CancelFunc
	region                    *localmeta.RegionMeta
	readSeq                   atomic.Uint64
	readMu                    sync.Mutex
	pendingReads              map[string]chan uint64
	allowSnapshotInstallRetry bool
	fastLeaseRead             bool
	lastLeaderContactUnixNano atomic.Int64
	batcher                   *proposalBatcher
	applyErr                  atomic.Value // stores error
}

const defaultMaxInFlightApply = 8192

const adminCommandPrefix byte = 0xAD

func isAdminEntry(data []byte) bool {
	return len(data) > 0 && data[0] == adminCommandPrefix
}

func decodeAdminCommand(data []byte) (*raftcmdpb.AdminCommand, error) {
	if len(data) <= 1 {
		return nil, errAdminCommandPayloadTooShort()
	}
	var cmd raftcmdpb.AdminCommand
	if err := proto.Unmarshal(data[1:], &cmd); err != nil {
		return nil, err
	}
	return &cmd, nil
}

// NewPeer constructs a peer using the provided configuration. The caller must
// register the peer with the transport before invoking Bootstrap.
func NewPeer(cfg *Config) (*Peer, error) {
	if cfg == nil {
		return nil, errNilConfig
	}
	if cfg.Transport == nil {
		return nil, errNilTransport
	}
	if cfg.Apply == nil {
		return nil, errNilApplyFunc
	}
	storage, err := ResolveStorage(cfg)
	if err != nil {
		return nil, err
	}
	raftCfg := cfg.RaftConfig
	if raftCfg.ID == 0 {
		return nil, errZeroRaftID
	}
	if cfg.FastLeaseRead && (raftCfg.ReadOnlyOption != myraft.ReadOnlyLeaseBased || !raftCfg.CheckQuorum) {
		return nil, errFastLeaseReadRequiresLeaseRead
	}
	raftCfg.Storage = storage
	node, err := myraft.NewRawNode(&raftCfg)
	if err != nil {
		return nil, err
	}
	stopCtx, stopCancel := context.WithCancel(context.Background())
	peer := &Peer{
		id:                        raftCfg.ID,
		node:                      node,
		storage:                   storage,
		transport:                 cfg.Transport,
		apply:                     cfg.Apply,
		applyRunner:               cfg.ApplyRunner,
		adminApply:                cfg.AdminApply,
		confChangeHook:            cfg.ConfChange,
		snapshotExport:            cfg.SnapshotExport,
		snapshotApply:             cfg.SnapshotApply,
		raftLog:                   newRaftLogTracker(nonZeroGroupID(cfg.GroupID)),
		snapshotQueue:             newSnapshotResendQueue(),
		logRetainEntries:          cfg.LogRetainEntries,
		applyCloser:               utils.NewCloserInitial(1),
		stopCtx:                   stopCtx,
		stopCancel:                stopCancel,
		region:                    localmeta.CloneRegionMetaPtr(cfg.Region),
		pendingReads:              make(map[string]chan uint64),
		allowSnapshotInstallRetry: cfg.AllowSnapshotInstallRetry,
		fastLeaseRead:             cfg.FastLeaseRead,
	}
	if peer.logRetainEntries == 0 {
		peer.logRetainEntries = defaultLogRetainEntries
	}
	peer.applyMark = &utils.WaterMark{Name: fmt.Sprintf("raft.%d.apply", peer.id)}
	peer.applyMark.Init(peer.applyCloser)
	if cfg.MaxInFlightApply > 0 {
		peer.applyLimit = cfg.MaxInFlightApply
	} else {
		peer.applyLimit = defaultMaxInFlightApply
	}
	peer.batcher = newProposalBatcher(peer, cfg.BatchMaxSize, cfg.BatchMaxWait)
	return peer, nil
}

// ID returns the peer ID.
func (p *Peer) ID() uint64 {
	return p.id
}

// RegionMeta returns a clone of the region metadata associated with this
// peer. It mirrors TinyKV's approach of surfacing region layout through the
// store for schedulers and debugging endpoints.
func (p *Peer) RegionMeta() *localmeta.RegionMeta {
	if p == nil || p.region == nil {
		return nil
	}
	return localmeta.CloneRegionMetaPtr(p.region)
}

// ApplyRegionMetaMirror replaces the peer's in-memory region metadata mirror.
// It exists only so raftstore can synchronize peer-local snapshots after
// apply/bootstrap has already advanced the store-local region truth. It must
// not be treated as a consensus state mutation entrypoint.
func (p *Peer) ApplyRegionMetaMirror(meta localmeta.RegionMeta) {
	if p == nil {
		return
	}
	cp := localmeta.CloneRegionMetaPtr(&meta)
	p.mu.Lock()
	p.region = cp
	p.mu.Unlock()
}

// Bootstrap injects the initial configuration into the node. It must be called
// after the peer has been registered with the transport.
func (p *Peer) Bootstrap(peers []myraft.Peer) error {
	if len(peers) == 0 {
		return nil
	}
	last, err := p.storage.LastIndex()
	if err == nil && last > 0 {
		return nil
	}
	if hs, cs, err := p.storage.InitialState(); err == nil {
		if !myraft.IsEmptyHardState(hs) || len(cs.Voters) > 0 || len(cs.Learners) > 0 {
			return nil
		}
	}
	p.mu.Lock()
	err = p.node.Bootstrap(peers)
	p.mu.Unlock()
	if err != nil {
		return err
	}
	return p.processReady()
}

// Tick increments the logical clock, driving elections and heartbeats.
func (p *Peer) Tick() error {
	p.mu.Lock()
	p.node.Tick()
	p.mu.Unlock()
	p.resendPendingSnapshots()
	return p.processReady()
}

// Step forwards a received raft message to the underlying node.
func (p *Peer) Step(msg myraft.Message) error {
	if msg.Type == myraft.MsgSnapshotStatus && !msg.Reject {
		if q := p.snapshotQueue; q != nil {
			q.drop(msg.From)
		}
	}
	if msg.Type == myraft.MsgSnapshot && msg.Snapshot != nil && !myraft.IsEmptySnap(*msg.Snapshot) && len(msg.Snapshot.Data) > 0 {
		if err := p.ensureEmptySnapshotPayloadTarget(); err != nil {
			return err
		}
	}
	p.mu.Lock()
	err := p.node.Step(msg)
	p.mu.Unlock()
	if err != nil {
		return err
	}
	p.observeLeaderContact(msg)
	return p.processReady()
}

// Propose submits application data to the raft log.
func (p *Peer) Propose(data []byte) error {
	if err := p.waitForApplyBacklog(); err != nil {
		return err
	}
	return p.batcher.propose(data).Wait()
}

// ProposeCommand encodes the provided raft command request and submits it to
// the raft log.
func (p *Peer) ProposeCommand(req *raftcmdpb.RaftCmdRequest) error {
	payload, err := command.Encode(req)
	if err != nil {
		return err
	}
	return p.Propose(payload)
}

// ProposeAdmin submits an admin command encoded as raftcmdpb.AdminCommand payload.
func (p *Peer) ProposeAdmin(cmdData []byte) error {
	if len(cmdData) == 0 {
		return errEmptyAdminCommand
	}
	if err := p.waitForApplyBacklog(); err != nil {
		return err
	}
	payload := append([]byte{adminCommandPrefix}, cmdData...)
	p.mu.Lock()
	err := p.node.Propose(payload)
	p.mu.Unlock()
	if err != nil {
		return err
	}
	return p.processReady()
}

// ProposeConfChange submits a configuration change entry to the raft log.
func (p *Peer) ProposeConfChange(cc raftpb.ConfChangeV2) error {
	if err := p.waitForApplyBacklog(); err != nil {
		return err
	}
	p.mu.Lock()
	err := p.node.ProposeConfChange(cc)
	p.mu.Unlock()
	if err != nil {
		return err
	}
	return p.processReady()
}

// TransferLeader requests leadership transfer to the provided peer ID.
func (p *Peer) TransferLeader(target uint64) error {
	if target == 0 {
		return errZeroTransferTarget
	}
	if err := p.waitForApplyBacklog(); err != nil {
		return err
	}
	p.mu.Lock()
	p.node.TransferLeader(target)
	p.mu.Unlock()
	return p.processReady()
}

// Campaign transitions this peer into candidate state.
func (p *Peer) Campaign() error {
	if err := p.waitForApplyBacklog(); err != nil {
		return err
	}
	p.mu.Lock()
	err := p.node.Campaign()
	p.mu.Unlock()
	if err != nil {
		return err
	}
	return p.processReady()
}

// Flush forces processing of any pending Ready state.
func (p *Peer) Flush() error {
	return p.processReady()
}

// Status returns the raft status snapshot.
func (p *Peer) Status() myraft.Status {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.node.Status()
}

// AppliedIndex returns the highest raft log index fully applied to the local
// state machine. Follower-read admission uses this as the actual read
// freshness boundary; commit index alone is not sufficient.
func (p *Peer) AppliedIndex() uint64 {
	if p == nil || p.applyMark == nil {
		return 0
	}
	return p.applyMark.DoneUntil()
}

// Snapshot returns the current raft snapshot enriched with the configured
// snapshot payload when snapshot export is configured.
func (p *Peer) Snapshot() (myraft.Snapshot, error) {
	if p == nil || p.storage == nil {
		return myraft.Snapshot{}, errSnapshotRequiresStorage
	}
	snap, err := p.storage.Snapshot()
	if err != nil {
		return myraft.Snapshot{}, err
	}
	meta := p.RegionMeta()
	if meta != nil {
		voters := make([]uint64, 0, len(meta.Peers))
		for _, peerMeta := range meta.Peers {
			if peerMeta.PeerID != 0 {
				voters = append(voters, peerMeta.PeerID)
			}
		}
		snap.Metadata.ConfState = raftpb.ConfState{Voters: voters}
	}
	if last, err := p.storage.LastIndex(); err == nil && last > snap.Metadata.Index {
		if term, termErr := p.storage.Term(last); termErr == nil {
			snap.Metadata.Index = last
			snap.Metadata.Term = term
		}
	}
	if myraft.IsEmptySnap(snap) || len(snap.Data) > 0 || p.snapshotExport == nil {
		return snap, nil
	}
	if meta == nil {
		return myraft.Snapshot{}, errSnapshotExportRequiresRegionMeta
	}
	payload, err := p.snapshotExport(*meta)
	if err != nil {
		return myraft.Snapshot{}, errExportSnapshotPayload(err)
	}
	snap.Data = payload
	return snap, nil
}

func (p *Peer) processReady() error {
	if err := p.currentApplyError(); err != nil {
		return err
	}
	p.readyMu.Lock()
	for {
		if err := p.currentApplyError(); err != nil {
			p.readyMu.Unlock()
			return err
		}
		p.mu.Lock()
		hasReady := p.node.HasReady()
		var rd myraft.Ready
		if hasReady {
			rd = p.node.Ready()
		}
		p.mu.Unlock()

		if !hasReady {
			p.readyMu.Unlock()
			return nil
		}

		msgs := rd.Messages
		if err := p.handleReady(rd); err != nil {
			p.readyMu.Unlock()
			return err
		}
		if err := p.prepareMessages(msgs); err != nil {
			p.readyMu.Unlock()
			return err
		}

		p.mu.Lock()
		p.node.Advance(rd)
		p.mu.Unlock()
		if failpoints.ShouldFailAfterReadyAdvanceBeforeSend() {
			p.readyMu.Unlock()
			return fmt.Errorf("raftstore: failpoint after ready advance before send")
		}

		p.readyMu.Unlock()
		p.sendMessages(msgs)
		p.readyMu.Lock()
	}
}

func (p *Peer) handleReady(rd myraft.Ready) error {
	if info := p.raftLog; info != nil {
		info.setInjected(failpoints.ShouldFailBeforeStorage())
	}

	hardStateStored := false
	entriesStored := false
	if !myraft.IsEmptyHardState(rd.HardState) && len(rd.Entries) > 0 && myraft.IsEmptySnap(rd.Snapshot) {
		if storage, ok := p.storage.(raftlog.AppendWithHardStateStorage); ok {
			if err := p.raftLog.injectFailure("before_hard_state"); err != nil {
				return err
			}
			if err := p.raftLog.injectFailure("before_entries"); err != nil {
				return err
			}
			if err := storage.AppendWithHardState(rd.Entries, rd.HardState); err != nil {
				return err
			}
			hardStateStored = true
			entriesStored = true
			if info := p.raftLog; info != nil {
				info.capturePointer(localmeta.RaftLogPointer{
					GroupID:      info.groupID,
					AppliedIndex: rd.Commit,
					AppliedTerm:  rd.Term,
				})
				last := rd.Entries[len(rd.Entries)-1]
				info.capturePointer(localmeta.RaftLogPointer{
					GroupID:      info.groupID,
					AppliedIndex: last.Index,
					AppliedTerm:  last.Term,
				})
			}
		}
	}

	if !hardStateStored && !myraft.IsEmptyHardState(rd.HardState) {
		if err := p.raftLog.injectFailure("before_hard_state"); err != nil {
			return err
		}
		if err := p.storage.SetHardState(rd.HardState); err != nil {
			return err
		}
		if info := p.raftLog; info != nil {
			info.capturePointer(localmeta.RaftLogPointer{
				GroupID:      info.groupID,
				AppliedIndex: rd.Commit,
				AppliedTerm:  rd.Term,
			})
		}
	}
	if !myraft.IsEmptySnap(rd.Snapshot) {
		if len(rd.Snapshot.Data) > 0 && p.snapshotApply != nil {
			if err := p.raftLog.injectFailure("before_snapshot_data"); err != nil {
				return err
			}
			region, err := p.snapshotApply(rd.Snapshot.Data)
			if err != nil {
				return err
			}
			p.ApplyRegionMetaMirror(region)
		}
		if err := p.raftLog.injectFailure("before_snapshot"); err != nil {
			return err
		}
		if err := p.storage.ApplySnapshot(rd.Snapshot); err != nil {
			return err
		}
		if info := p.raftLog; info != nil {
			meta := rd.Snapshot.Metadata
			info.capturePointer(localmeta.RaftLogPointer{
				GroupID:       info.groupID,
				SnapshotIndex: meta.Index,
				SnapshotTerm:  meta.Term,
			})
		}
		if meta := rd.Snapshot.Metadata; meta.Index > 0 {
			p.markSnapshotApplied(meta.Index)
		}
	}
	if !entriesStored && len(rd.Entries) > 0 {
		if err := p.raftLog.injectFailure("before_entries"); err != nil {
			return err
		}
		if err := p.storage.Append(rd.Entries); err != nil {
			return err
		}
		if info := p.raftLog; info != nil {
			last := rd.Entries[len(rd.Entries)-1]
			info.capturePointer(localmeta.RaftLogPointer{
				GroupID:      info.groupID,
				AppliedIndex: last.Index,
				AppliedTerm:  last.Term,
			})
		}
	}
	if len(rd.ReadStates) > 0 {
		p.handleReadStates(rd.ReadStates)
	}
	if len(rd.CommittedEntries) > 0 {
		p.beginApply(rd.CommittedEntries)
		var toApply []myraft.Entry
		flushApply := func(wait bool) error {
			if len(toApply) == 0 {
				return nil
			}
			entries := append([]myraft.Entry(nil), toApply...)
			toApply = toApply[:0]
			if p.applyRunner != nil {
				if err := p.applyRunner.SubmitApply(ApplyTask{Entries: entries}, p.handleApplyResult); err != nil {
					return err
				}
				if wait {
					last := entries[len(entries)-1].Index
					if err := p.WaitApplied(p.stopCtx, last); err != nil {
						return err
					}
				}
				return nil
			}
			if p.apply != nil {
				if err := p.apply(entries); err != nil {
					return err
				}
			}
			p.finishApply(entries)
			return nil
		}
		finishSync := func(entry myraft.Entry) {
			p.finishApply([]myraft.Entry{entry})
		}
		for _, entry := range rd.CommittedEntries {
			switch entry.Type {
			case myraft.EntryConfChange:
				if err := flushApply(true); err != nil {
					return err
				}
				var cc raftpb.ConfChange
				if err := cc.Unmarshal(entry.Data); err != nil {
					return err
				}
				ccV2 := cc.AsV2()
				p.mu.Lock()
				p.node.ApplyConfChange(ccV2)
				p.mu.Unlock()
				if err := p.handleConfChange(ccV2, entry); err != nil {
					return err
				}
				finishSync(entry)
			case myraft.EntryConfChangeV2:
				if err := flushApply(true); err != nil {
					return err
				}
				var cc raftpb.ConfChangeV2
				if err := cc.Unmarshal(entry.Data); err != nil {
					return err
				}
				p.mu.Lock()
				p.node.ApplyConfChange(cc)
				p.mu.Unlock()
				if err := p.handleConfChange(cc, entry); err != nil {
					return err
				}
				finishSync(entry)
			default:
				if len(entry.Data) == 0 {
					finishSync(entry)
					continue
				}
				if isAdminEntry(entry.Data) {
					if err := flushApply(true); err != nil {
						return err
					}
					cmd, err := decodeAdminCommand(entry.Data)
					if err != nil {
						return err
					}
					if err := p.applyAdminCommand(cmd); err != nil {
						return err
					}
					finishSync(entry)
					continue
				}
				toApply = append(toApply, entry)
			}
		}
		if err := flushApply(false); err != nil {
			return err
		}
		if compacted := p.AppliedIndex(); compacted > 0 {
			if err := p.maybeCompact(compacted); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *Peer) handleApplyResult(result ApplyResult) {
	if p == nil {
		return
	}
	if result.Err != nil {
		p.noteApplyError(result.Err)
		return
	}
	p.finishApply(result.Entries)
}

func (p *Peer) ensureEmptySnapshotPayloadTarget() error {
	if p == nil || p.storage == nil {
		return nil
	}
	if p.allowSnapshotInstallRetry {
		return nil
	}
	hs, cs, err := p.storage.InitialState()
	if err != nil {
		return fmt.Errorf("raftstore: inspect snapshot target state: %w", err)
	}
	if !myraft.IsEmptyHardState(hs) || len(cs.Voters) > 0 || len(cs.Learners) > 0 {
		return errSnapshotPayloadInstallRequiresEmptyPeerState
	}
	last, err := p.storage.LastIndex()
	if err != nil {
		return fmt.Errorf("raftstore: inspect snapshot target log: %w", err)
	}
	if last > 0 {
		return errSnapshotPayloadInstallRequiresEmptyPeerLog
	}
	return nil
}

func (p *Peer) prepareMessages(msgs []myraft.Message) error {
	if p == nil || len(msgs) == 0 || p.snapshotExport == nil {
		return nil
	}
	for i := range msgs {
		msg := &msgs[i]
		if msg.Type != myraft.MsgSnapshot || msg.Snapshot == nil || myraft.IsEmptySnap(*msg.Snapshot) || len(msg.Snapshot.Data) > 0 {
			continue
		}
		meta := p.RegionMeta()
		if meta == nil {
			return errSnapshotPayloadExportRequiresRegionMeta
		}
		payload, err := p.snapshotExport(*meta)
		if err != nil {
			return errExportSnapshotPayload(err)
		}
		msg.Snapshot.Data = payload
	}
	return nil
}

func (p *Peer) sendMessages(msgs []myraft.Message) {
	if p == nil || len(msgs) == 0 || p.transport == nil {
		return
	}
	for _, msg := range msgs {
		if msg.Type == myraft.MsgSnapshot {
			if q := p.snapshotQueue; q != nil {
				q.record(msg)
			}
		}
		p.transport.Send(p.stopCtx, msg)
	}
}

func (p *Peer) handleConfChange(cc raftpb.ConfChangeV2, entry raftpb.Entry) error {
	if p == nil || p.confChangeHook == nil {
		return nil
	}
	event := ConfChangeEvent{
		Peer:       p,
		RegionMeta: p.RegionMeta(),
		ConfChange: cc,
		Index:      entry.Index,
		Term:       entry.Term,
	}
	return p.confChangeHook(event)
}

func (p *Peer) maybeCompact(applied uint64) error {
	if applied == 0 {
		return nil
	}
	ws, ok := p.storage.(*raftlog.WALStorage)
	if !ok {
		return nil
	}
	return ws.MaybeCompact(applied, p.logRetainEntries)
}

func (p *Peer) applyAdminCommand(cmd *raftcmdpb.AdminCommand) error {
	if p == nil || cmd == nil {
		return nil
	}
	if p.adminApply == nil {
		return nil
	}
	return p.adminApply(cmd)
}

func (p *Peer) noteApplyError(err error) {
	if p == nil || err == nil {
		return
	}
	if p.applyErr.Load() == nil {
		p.applyErr.Store(err)
		if p.stopCancel != nil {
			p.stopCancel()
		}
	}
}

func (p *Peer) currentApplyError() error {
	if p == nil {
		return nil
	}
	err, _ := p.applyErr.Load().(error)
	return err
}

// WaitApplied blocks until the provided raft log index has been fully applied.
func (p *Peer) WaitApplied(ctx context.Context, index uint64) error {
	if p == nil || p.applyMark == nil || index == 0 {
		return nil
	}
	if err := p.currentApplyError(); err != nil && p.AppliedIndex() < index {
		return err
	}
	return p.applyMark.WaitForMark(ctx, index)
}

// Close releases resources associated with the peer, including background
// watermark processors.
func (p *Peer) Close() error {
	if p == nil {
		return nil
	}
	p.batcher.close()
	p.stopCancel()
	if p.applyCloser != nil {
		p.applyCloser.Close()
	}
	p.readMu.Lock()
	for key, ch := range p.pendingReads {
		if ch != nil {
			close(ch)
		}
		delete(p.pendingReads, key)
	}
	p.readMu.Unlock()
	return nil
}

func (p *Peer) waitForApplyBacklog() error {
	if p == nil || p.applyMark == nil {
		return nil
	}
	if err := p.currentApplyError(); err != nil {
		return err
	}
	limit := p.applyLimit
	if limit == 0 {
		return nil
	}
	for {
		done := p.applyMark.DoneUntil()
		last := p.applyMark.LastIndex()
		if last <= done || last-done < limit {
			return nil
		}
		target := done + 1
		if err := p.applyMark.WaitForMark(p.stopCtx, target); err != nil {
			if errors.Is(err, context.Canceled) {
				return errPeerStopped
			}
			return err
		}
	}
}

func (p *Peer) beginApply(entries []myraft.Entry) {
	if p == nil || p.applyMark == nil || len(entries) == 0 {
		return
	}
	indices := make([]uint64, 0, len(entries))
	for _, entry := range entries {
		if entry.Index == 0 {
			continue
		}
		indices = append(indices, entry.Index)
	}
	if len(indices) == 0 {
		return
	}
	if len(indices) == 1 {
		p.applyMark.Begin(indices[0])
		return
	}
	p.applyMark.BeginMany(indices)
}

func (p *Peer) finishApply(entries []myraft.Entry) {
	if p == nil || p.applyMark == nil || len(entries) == 0 {
		return
	}
	indices := make([]uint64, 0, len(entries))
	for _, entry := range entries {
		if entry.Index == 0 {
			continue
		}
		indices = append(indices, entry.Index)
	}
	if len(indices) == 0 {
		return
	}
	if len(indices) == 1 {
		p.applyMark.Done(indices[0])
		return
	}
	p.applyMark.DoneMany(indices)
}

// LinearizableRead asks raft for a read index and returns the committed index
// that must be applied before serving local state. With EnableLeaseRead the
// underlying RawNode uses etcd/raft's lease-based ReadIndex path, so the leader
// can answer from its quorum lease without broadcasting every read; without it,
// ReadIndex falls back to the quorum-confirmed safe path.
func (p *Peer) LinearizableRead(ctx context.Context) (uint64, error) {
	if p == nil {
		return 0, errNilPeer
	}
	if ctx == nil {
		ctx = p.stopCtx
	}
	select {
	case <-p.stopCtx.Done():
		return 0, errPeerStopped
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}
	key, ch := p.startReadIndex()
	if err := p.Flush(); err != nil {
		p.cancelReadIndex(key)
		return 0, err
	}
	select {
	case idx, ok := <-ch:
		if !ok {
			return 0, errPeerStopped
		}
		return idx, nil
	case <-ctx.Done():
		p.cancelReadIndex(key)
		return 0, ctx.Err()
	case <-p.stopCtx.Done():
		p.cancelReadIndex(key)
		return 0, errPeerStopped
	}
}

// BoundedStaleReadIndex admits a local read without ReadIndex only when the
// local applied index is within the caller's explicit lag budget. On followers,
// the optional wall-clock budget also requires recent contact from the current
// raft leader. This is intentionally separate from LinearizableRead: bounded
// stale reads are not linearizable and must be requested explicitly.
func (p *Peer) BoundedStaleReadIndex(maxStaleReadIndex uint64, maxStaleReadAge time.Duration) (uint64, bool) {
	if p == nil {
		return 0, false
	}
	p.mu.Lock()
	status := p.node.BasicStatus()
	p.mu.Unlock()
	if status.RaftState != myraft.StateLeader && status.RaftState != myraft.StateFollower {
		return 0, false
	}
	applied := p.AppliedIndex()
	if applied == 0 || status.Commit < applied {
		return 0, false
	}
	if status.Commit-applied > maxStaleReadIndex {
		return 0, false
	}
	if status.RaftState == myraft.StateFollower && maxStaleReadAge > 0 {
		lastContact := p.lastLeaderContactUnixNano.Load()
		if lastContact <= 0 || time.Since(time.Unix(0, lastContact)) > maxStaleReadAge {
			return 0, false
		}
	}
	return applied, true
}

func (p *Peer) observeLeaderContact(msg myraft.Message) {
	if p == nil || msg.From == 0 {
		return
	}
	switch msg.Type {
	case myraft.MsgAppend, myraft.MsgHeartbeat, myraft.MsgSnapshot:
	default:
		return
	}
	p.mu.Lock()
	status := p.node.BasicStatus()
	p.mu.Unlock()
	if status.Lead == msg.From {
		p.lastLeaderContactUnixNano.Store(time.Now().UnixNano())
	}
}

func (p *Peer) startReadIndex() (string, chan uint64) {
	reqCtx := make([]byte, 16)
	binary.BigEndian.PutUint64(reqCtx[:8], p.id)
	seq := p.readSeq.Add(1)
	binary.BigEndian.PutUint64(reqCtx[8:], seq)
	key := string(reqCtx)
	ch := make(chan uint64, 1)

	p.readMu.Lock()
	p.pendingReads[key] = ch
	p.readMu.Unlock()

	p.mu.Lock()
	p.node.ReadIndex(reqCtx)
	p.mu.Unlock()
	return key, ch
}

func (p *Peer) cancelReadIndex(key string) {
	p.readMu.Lock()
	ch, ok := p.pendingReads[key]
	if ok {
		delete(p.pendingReads, key)
	}
	p.readMu.Unlock()
	if ok && ch != nil {
		close(ch)
	}
}

func (p *Peer) handleReadStates(states []myraft.ReadState) {
	if len(states) == 0 {
		return
	}
	for _, state := range states {
		if len(state.RequestCtx) == 0 {
			continue
		}
		key := string(state.RequestCtx)
		p.readMu.Lock()
		ch, ok := p.pendingReads[key]
		if ok {
			delete(p.pendingReads, key)
		}
		p.readMu.Unlock()
		if ok && ch != nil {
			ch <- state.Index
			close(ch)
		}
	}
}

func (p *Peer) markSnapshotApplied(index uint64) {
	if p == nil || p.applyMark == nil || index == 0 {
		return
	}
	// Snapshot application makes all indices <= snapshot index finished.
	p.applyMark.SetDoneUntil(index)
}

// PopPendingSnapshot returns the most recent snapshot recorded during Ready
// handling, clearing the queue. It returns false when no snapshot is pending.
func (p *Peer) PopPendingSnapshot() (myraft.Snapshot, bool) {
	if p == nil || p.snapshotQueue == nil {
		return myraft.Snapshot{}, false
	}
	msg, ok := p.snapshotQueue.first()
	if !ok || msg.Snapshot == nil || myraft.IsEmptySnap(*msg.Snapshot) {
		return myraft.Snapshot{}, false
	}
	p.snapshotQueue.drop(msg.To)
	return *msg.Snapshot, true
}

// PendingSnapshot returns the snapshot retained for resend without removing it
// from the queue.
func (p *Peer) PendingSnapshot() (myraft.Snapshot, bool) {
	if p == nil || p.snapshotQueue == nil {
		return myraft.Snapshot{}, false
	}
	msg, ok := p.snapshotQueue.first()
	if !ok || msg.Snapshot == nil {
		return myraft.Snapshot{}, false
	}
	return *msg.Snapshot, true
}

// ResendSnapshot attempts to resend the last snapshot destined for the provided
// peer ID. It returns true when a snapshot message was re-enqueued.
func (p *Peer) ResendSnapshot(to uint64) bool {
	if p == nil || p.transport == nil || p.snapshotQueue == nil || to == 0 {
		return false
	}
	msg, ok := p.snapshotQueue.pendingFor(to)
	if !ok {
		return false
	}
	p.transport.Send(p.stopCtx, msg)
	return true
}

func (p *Peer) resendPendingSnapshots() {
	if p == nil || p.transport == nil || p.snapshotQueue == nil {
		return
	}
	p.snapshotQueue.forEach(func(msg myraft.Message) {
		p.transport.Send(p.stopCtx, msg)
	})
}
