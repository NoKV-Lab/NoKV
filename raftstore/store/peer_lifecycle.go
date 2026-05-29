// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"fmt"

	metaregion "github.com/feichai0017/NoKV/meta/region"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/failpoints"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"github.com/feichai0017/NoKV/raftstore/peer"
	snapshotpkg "github.com/feichai0017/NoKV/raftstore/snapshot"
	"github.com/feichai0017/NoKV/raftstore/store/router"
)

// Router exposes the underlying router reference so transports can reuse the
// same registration hub.
func (s *Store) Router() *router.Router {
	if s == nil {
		return nil
	}
	return s.router
}

// StartPeer builds and registers a peer according to the supplied
// configuration. The peer is automatically registered with the Store router.
// When bootstrapPeers is non-empty StartPeer will call Bootstrap with those
// peers after the peer is registered.
func (s *Store) StartPeer(cfg *peer.Config, bootstrapPeers []myraft.Peer) (*peer.Peer, error) {
	return s.startPeer(cfg, bootstrapPeers, true)
}

func (s *Store) startPeer(cfg *peer.Config, bootstrapPeers []myraft.Peer, publishCatalog bool) (*peer.Peer, error) {
	if s == nil {
		return nil, errNilStore
	}
	if cfg == nil {
		return nil, errNilPeerConfig
	}
	var regionMeta *localmeta.RegionMeta
	if cfg.Region != nil {
		if cfg.Region.State == 0 {
			cfg.Region.State = metaregion.ReplicaStateRunning
		}
		regionMeta = localmeta.CloneRegionMetaPtr(cfg.Region)
	}
	cfgCopy := *cfg
	cfgCopy.ConfChange = s.handlePeerConfChange
	cfgCopy = s.wirePeerConfig(cfgCopy, false)
	p, err := peer.NewPeer(&cfgCopy)
	if err != nil {
		return nil, err
	}
	id := p.ID()
	if err := s.router.Register(p); err != nil {
		_ = p.Close()
		return nil, err
	}
	if regionMeta != nil && regionMeta.ID != 0 {
		s.regions.SetPeer(regionMeta.ID, p)
	}

	if regionMeta != nil {
		applyMeta := s.applyRegionMeta
		if !publishCatalog {
			applyMeta = s.applyRegionMetaSilent
		}
		if err := applyMeta(*regionMeta); err != nil {
			s.router.Deregister(id)
			s.regions.SetPeer(regionMeta.ID, nil)
			_ = p.Close()
			return nil, err
		}
	}
	if len(bootstrapPeers) > 0 {
		if err := p.Bootstrap(bootstrapPeers); err != nil {
			s.StopPeer(id)
			return nil, err
		}
	}
	return p, nil
}

// StopPeer removes the peer from the router and closes it.
func (s *Store) StopPeer(id uint64) {
	s.stopPeer(id, true)
}

func (s *Store) stopPeer(id uint64, publishCatalog bool) {
	if s == nil || id == 0 {
		return
	}
	p := s.router.Deregister(id)
	var regionID uint64
	if p != nil {
		if meta := p.RegionMeta(); meta != nil {
			regionID = meta.ID
		}
	}
	if regionID != 0 {
		s.regions.SetPeer(regionID, nil)
		if publishCatalog {
			_ = s.applyRegionState(regionID, metaregion.ReplicaStateRemoving)
		}
	}
	if p != nil {
		_ = p.Close()
	}
}

// Close stops background workers associated with the store.
func (s *Store) Close() {
	if s == nil {
		return
	}
	if s.schedulerClient() != nil {
		s.flushRegionUpdates()
		s.stopHeartbeatLoop()
	}
	s.stopOperationLoop()
	if s.schedulerClient() != nil {
		_ = s.reportStoreHeartbeat()
		_ = s.schedulerClient().Close()
	}
	if s.cancel != nil {
		s.cancel()
	}
	if s.cmds != nil && s.cmds.pipe != nil {
		s.cmds.pipe.close()
	}
	if s.observers != nil {
		s.observers.Close()
	}
}

// VisitPeers executes the provided callback for every peer registered with the
// store. The callback receives a snapshot of the peer pointer so callers can
// perform operations without holding the store lock for extended periods.
func (s *Store) VisitPeers(fn func(*peer.Peer)) {
	if s == nil || fn == nil {
		return
	}
	s.router.Visit(fn)
}

// Peer returns the peer registered with the provided ID.
func (s *Store) Peer(id uint64) (*peer.Peer, bool) {
	if s == nil || id == 0 {
		return nil, false
	}
	return s.router.Peer(id)
}

// Step forwards the provided raft message to the target peer hosted on this
// store.
func (s *Store) Step(msg myraft.Message) error {
	if s == nil {
		return errNilStore
	}
	if msg.To == 0 {
		return errRaftMessageMissingRecipient
	}
	if s.router == nil {
		return errNilRouter
	}
	if _, ok := s.router.Peer(msg.To); !ok && msg.Type == myraft.MsgSnapshot {
		if err := s.startPeerFromSnapshot(msg); err != nil {
			return err
		}
	}
	return s.router.SendRaft(msg.To, msg)
}

func (s *Store) startPeerFromSnapshot(msg myraft.Message) error {
	if s == nil {
		return errNilStore
	}
	if msg.Type != myraft.MsgSnapshot || msg.To == 0 || msg.Snapshot == nil || myraft.IsEmptySnap(*msg.Snapshot) {
		return errSnapshotPeerBootstrapRequiresSnapshot
	}
	if s.peerBuilder == nil {
		return errSnapshotPeerBootstrapRequiresPeerBuild
	}
	if _, ok := s.router.Peer(msg.To); ok {
		return nil
	}
	metaFile, err := snapshotpkg.ReadPayloadMeta(msg.Snapshot.Data)
	if err != nil {
		return fmt.Errorf("raftstore: decode snapshot payload meta: %w", err)
	}
	meta := metaFile.Region
	if meta.ID == 0 {
		return errSnapshotPayloadMissingRegionMeta
	}
	var localPeer metaregion.Peer
	for _, peerMeta := range meta.Peers {
		if peerMeta.PeerID == msg.To {
			localPeer = peerMeta
			break
		}
	}
	if localPeer.PeerID == 0 {
		return fmt.Errorf("raftstore: snapshot payload missing peer %d", msg.To)
	}
	if s.storeID != 0 && localPeer.StoreID != 0 && localPeer.StoreID != s.storeID {
		return fmt.Errorf("raftstore: snapshot peer %d belongs to store %d, local store is %d", msg.To, localPeer.StoreID, s.storeID)
	}
	cfg, err := s.peerBuilder(meta)
	if err != nil {
		return fmt.Errorf("raftstore: build peer from snapshot region %d: %w", meta.ID, err)
	}
	if cfg == nil {
		return errPeerBuilderReturnedNilConfig(meta.ID)
	}
	if cfg.RaftConfig.ID != msg.To {
		return fmt.Errorf("raftstore: snapshot bootstrap peer mismatch want=%d got=%d", msg.To, cfg.RaftConfig.ID)
	}
	if _, err := s.StartPeer(cfg, nil); err != nil {
		return fmt.Errorf("raftstore: start snapshot peer %d for region %d: %w", msg.To, meta.ID, err)
	}
	return nil
}

// InstallRegionSnapshot installs one leader-exported raft snapshot on the
// local store, bootstrapping the target peer on demand from the snapshot
// payload carried in Snapshot.Data.
func (s *Store) InstallRegionSnapshot(snap myraft.Snapshot) (localmeta.RegionMeta, error) {
	if s == nil {
		return localmeta.RegionMeta{}, errNilStore
	}
	if myraft.IsEmptySnap(snap) {
		return localmeta.RegionMeta{}, errInstallRegionSnapshotRequiresSnapshot
	}
	if len(snap.Data) == 0 {
		return localmeta.RegionMeta{}, errInstallRegionSnapshotRequiresPayload
	}
	metaFile, err := snapshotpkg.ReadPayloadMeta(snap.Data)
	if err != nil {
		return localmeta.RegionMeta{}, fmt.Errorf("raftstore: decode install snapshot payload: %w", err)
	}
	meta := metaFile.Region
	if meta.ID == 0 {
		return localmeta.RegionMeta{}, fmt.Errorf("raftstore: install snapshot payload missing region metadata")
	}
	var localPeer metaregion.Peer
	for _, peerMeta := range meta.Peers {
		if peerMeta.StoreID == s.storeID {
			localPeer = peerMeta
			break
		}
	}
	if localPeer.PeerID == 0 {
		return localmeta.RegionMeta{}, errRegionDoesNotAssignPeer(meta.ID, s.storeID)
	}
	if existing, ok := s.Peer(localPeer.PeerID); ok && existing != nil {
		return localmeta.RegionMeta{}, errPeerAlreadyHosted(localPeer.PeerID, meta.ID)
	}
	if s.peerBuilder == nil {
		return localmeta.RegionMeta{}, errInstallRegionSnapshotRequiresPeerBuild
	}
	cfg, err := s.peerBuilder(meta)
	if err != nil {
		return localmeta.RegionMeta{}, fmt.Errorf("raftstore: build install peer for region %d: %w", meta.ID, err)
	}
	if cfg == nil {
		return localmeta.RegionMeta{}, errPeerBuilderReturnedNilConfig(meta.ID)
	}
	if cfg.RaftConfig.ID != localPeer.PeerID {
		return localmeta.RegionMeta{}, fmt.Errorf("raftstore: install snapshot peer mismatch want=%d got=%d", localPeer.PeerID, cfg.RaftConfig.ID)
	}
	cfgCopy := *cfg
	cfgCopy.ConfChange = s.handlePeerConfChange
	cfgCopy.AllowSnapshotInstallRetry = true
	cfgCopy = s.wirePeerConfig(cfgCopy, false)
	p, err := peer.NewPeer(&cfgCopy)
	if err != nil {
		return localmeta.RegionMeta{}, fmt.Errorf("raftstore: new install peer %d for region %d: %w", localPeer.PeerID, meta.ID, err)
	}
	msg := myraft.Message{Type: myraft.MsgSnapshot, To: localPeer.PeerID, Snapshot: &snap}
	if err := p.Step(msg); err != nil {
		_ = p.Close()
		return localmeta.RegionMeta{}, err
	}
	if failpoints.ShouldFailAfterSnapshotApplyBeforePublish() {
		_ = p.Close()
		return localmeta.RegionMeta{}, errFailpointAfterSnapshotApply
	}
	if err := s.router.Register(p); err != nil {
		_ = p.Close()
		return localmeta.RegionMeta{}, err
	}
	s.regions.SetPeer(meta.ID, p)
	if err := s.applyRegionMeta(meta); err != nil {
		s.router.Deregister(localPeer.PeerID)
		s.regions.SetPeer(meta.ID, nil)
		_ = p.Close()
		return localmeta.RegionMeta{}, err
	}
	return meta, nil
}

// Peers returns a snapshot describing every peer managed by the store.
func (s *Store) Peers() []PeerHandle {
	if s == nil {
		return nil
	}
	raw := s.router.List()
	handles := make([]PeerHandle, 0, len(raw))
	for _, p := range raw {
		handles = append(handles, PeerHandle{
			ID:     p.ID(),
			Peer:   p,
			Region: localmeta.CloneRegionMetaPtr(p.RegionMeta()),
		})
	}
	return handles
}

func (s *Store) wirePeerConfig(cfg peer.Config, allowSnapshotInstallRetry bool) peer.Config {
	cfg.ConfChange = s.handlePeerConfChange
	cfg.AllowSnapshotInstallRetry = allowSnapshotInstallRetry
	if cfg.AdminApply == nil {
		cfg.AdminApply = s.handleAdminCommand
	}
	cfg.Apply = s.applyEntries
	cfg.ApplyRunner = s
	return cfg
}
