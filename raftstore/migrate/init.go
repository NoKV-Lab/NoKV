// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package migrate

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/feichai0017/NoKV/engine/vfs"
	local "github.com/feichai0017/NoKV/local"
	workdirmode "github.com/feichai0017/NoKV/local/workdir"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/failpoints"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"github.com/feichai0017/NoKV/raftstore/raftlog"
	snapshotpkg "github.com/feichai0017/NoKV/raftstore/snapshot/sst"
	raftstorestats "github.com/feichai0017/NoKV/raftstore/stats"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

const snapshotRootDirName = "RAFTSTORE_SNAPSHOTS"

// InitConfig defines the standalone -> seed conversion inputs.
type InitConfig struct {
	WorkDir  string
	StoreID  uint64
	RegionID uint64
	PeerID   uint64
}

// InitResult describes the initialized seed directory.
type InitResult struct {
	WorkDir     string           `json:"workdir"`
	Mode        workdirmode.Mode `json:"mode"`
	StoreID     uint64           `json:"store_id"`
	RegionID    uint64           `json:"region_id"`
	PeerID      uint64           `json:"peer_id"`
	SnapshotDir string           `json:"snapshot_dir"`
}

// Init converts a standalone workdir into a single-store seeded cluster
// directory. It exports one full-range SST seed snapshot, persists the local
// region catalog, and initializes the raft durable metadata for the single
// local peer.
func Init(cfg InitConfig) (InitResult, error) {
	cfg.WorkDir = filepath.Clean(cfg.WorkDir)
	if cfg.WorkDir == "" || cfg.WorkDir == "." {
		return InitResult{}, fmt.Errorf("migrate: workdir is required")
	}
	if cfg.StoreID == 0 {
		return InitResult{}, fmt.Errorf("migrate: store id is required")
	}
	if cfg.RegionID == 0 {
		return InitResult{}, fmt.Errorf("migrate: region id is required")
	}
	if cfg.PeerID == 0 {
		return InitResult{}, fmt.Errorf("migrate: peer id is required")
	}

	state, err := readState(cfg.WorkDir)
	if err != nil {
		return InitResult{}, err
	}
	switch state.Mode {
	case workdirmode.ModeStandalone:
		if err := writeState(cfg.WorkDir, workdirmode.State{
			Mode:     workdirmode.ModePreparing,
			StoreID:  cfg.StoreID,
			RegionID: cfg.RegionID,
			PeerID:   cfg.PeerID,
		}); err != nil {
			return InitResult{}, err
		}
		if err := writeCheckpoint(cfg.WorkDir, Checkpoint{
			Stage:    CheckpointPreparingWritten,
			StoreID:  cfg.StoreID,
			RegionID: cfg.RegionID,
			PeerID:   cfg.PeerID,
		}); err != nil {
			return InitResult{}, err
		}
		if failpoints.ShouldFailAfterInitModePreparing() {
			return InitResult{}, fmt.Errorf("migrate: failpoint after init mode preparing")
		}
	case workdirmode.ModePreparing:
		if state.StoreID != 0 && state.StoreID != cfg.StoreID {
			return InitResult{}, fmt.Errorf("migrate: preparing state store mismatch want=%d got=%d", cfg.StoreID, state.StoreID)
		}
		if state.RegionID != 0 && state.RegionID != cfg.RegionID {
			return InitResult{}, fmt.Errorf("migrate: preparing state region mismatch want=%d got=%d", cfg.RegionID, state.RegionID)
		}
		if state.PeerID != 0 && state.PeerID != cfg.PeerID {
			return InitResult{}, fmt.Errorf("migrate: preparing state peer mismatch want=%d got=%d", cfg.PeerID, state.PeerID)
		}
	case workdirmode.ModeSeeded:
		if state.StoreID == cfg.StoreID && state.RegionID == cfg.RegionID && state.PeerID == cfg.PeerID {
			return InitResult{
				WorkDir:     cfg.WorkDir,
				Mode:        workdirmode.ModeSeeded,
				StoreID:     cfg.StoreID,
				RegionID:    cfg.RegionID,
				PeerID:      cfg.PeerID,
				SnapshotDir: seedSnapshotDir(cfg.WorkDir, cfg.RegionID),
			}, nil
		}
		return InitResult{}, fmt.Errorf("migrate: workdir already seeded for store=%d region=%d peer=%d", state.StoreID, state.RegionID, state.PeerID)
	case workdirmode.ModeCluster:
		return InitResult{}, fmt.Errorf("migrate: workdir already in cluster mode")
	default:
		return InitResult{}, fmt.Errorf("migrate: unsupported mode %q", state.Mode)
	}

	region := localmeta.RegionMeta{
		ID:       cfg.RegionID,
		StartKey: nil,
		EndKey:   nil,
		Epoch: metaregion.Epoch{
			Version:     1,
			ConfVersion: 1,
		},
		Peers: []metaregion.Peer{{
			StoreID: cfg.StoreID,
			PeerID:  cfg.PeerID,
		}},
		State: metaregion.ReplicaStateRunning,
	}

	localMeta, err := localmeta.OpenLocalStore(cfg.WorkDir, nil)
	if err != nil {
		return InitResult{}, fmt.Errorf("migrate: open local catalog: %w", err)
	}
	defer func() { _ = localMeta.Close() }()

	snapshotCatalog := localMeta.Snapshot()
	if len(snapshotCatalog) > 0 {
		if existing, ok := snapshotCatalog[cfg.RegionID]; !ok || len(snapshotCatalog) != 1 || existing.ID != region.ID || len(existing.Peers) != 1 || existing.Peers[0] != region.Peers[0] {
			return InitResult{}, fmt.Errorf("migrate: local catalog already contains conflicting region state")
		}
	}
	if err := localMeta.SaveRegion(region); err != nil {
		return InitResult{}, fmt.Errorf("migrate: save local catalog: %w", err)
	}
	if err := writeCheckpoint(cfg.WorkDir, Checkpoint{
		Stage:    CheckpointCatalogPersisted,
		StoreID:  cfg.StoreID,
		RegionID: cfg.RegionID,
		PeerID:   cfg.PeerID,
	}); err != nil {
		return InitResult{}, err
	}
	if failpoints.ShouldFailAfterInitCatalogPersist() {
		return InitResult{}, fmt.Errorf("migrate: failpoint after init catalog persist")
	}

	opts := local.NewDefaultOptions()
	opts.WorkDir = cfg.WorkDir
	opts.ControlLogPointerSnapshot = raftstorestats.ControlLogPointers(localMeta.DurableRaftPointerSnapshot)
	opts.AllowedModes = []workdirmode.Mode{workdirmode.ModePreparing}
	db, err := local.Open(opts)
	if err != nil {
		return InitResult{}, fmt.Errorf("migrate: open db: %w", err)
	}
	defer func() { _ = db.Close() }()

	snapshotDir := seedSnapshotDir(cfg.WorkDir, cfg.RegionID)
	fs := vfs.Ensure(nil)
	if _, err := fs.Stat(snapshotDir); err == nil {
		if err := fs.RemoveAll(snapshotDir); err != nil {
			return InitResult{}, fmt.Errorf("migrate: remove existing seed snapshot dir %s: %w", snapshotDir, err)
		}
	} else if !os.IsNotExist(err) {
		return InitResult{}, fmt.Errorf("migrate: stat seed snapshot dir %s: %w", snapshotDir, err)
	}
	if _, err := snapshotpkg.NewDBStore(db).ExportSnapshotDir(snapshotDir, region); err != nil {
		return InitResult{}, fmt.Errorf("migrate: export seed snapshot: %w", err)
	}
	if err := writeCheckpoint(cfg.WorkDir, Checkpoint{
		Stage:    CheckpointSeedExported,
		StoreID:  cfg.StoreID,
		RegionID: cfg.RegionID,
		PeerID:   cfg.PeerID,
	}); err != nil {
		return InitResult{}, err
	}
	if failpoints.ShouldFailAfterInitSeedSnapshot() {
		return InitResult{}, fmt.Errorf("migrate: failpoint after init seed snapshot")
	}

	storage, err := raftlog.NewDBLog(db).Open(cfg.RegionID, localMeta)
	if err != nil {
		return InitResult{}, fmt.Errorf("migrate: open raft storage: %w", err)
	}
	snap := myraft.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index: 1,
			Term:  1,
			ConfState: raftpb.ConfState{
				Voters: []uint64{cfg.PeerID},
			},
		},
	}
	if err := storage.ApplySnapshot(snap); err != nil {
		return InitResult{}, fmt.Errorf("migrate: apply initial raft snapshot: %w", err)
	}
	if err := storage.SetHardState(myraft.HardState{
		Term:   1,
		Commit: 1,
	}); err != nil {
		return InitResult{}, fmt.Errorf("migrate: set initial hard state: %w", err)
	}
	if err := writeCheckpoint(cfg.WorkDir, Checkpoint{
		Stage:    CheckpointRaftSeeded,
		StoreID:  cfg.StoreID,
		RegionID: cfg.RegionID,
		PeerID:   cfg.PeerID,
	}); err != nil {
		return InitResult{}, err
	}
	if err := writeState(cfg.WorkDir, workdirmode.State{
		Mode:     workdirmode.ModeSeeded,
		StoreID:  cfg.StoreID,
		RegionID: cfg.RegionID,
		PeerID:   cfg.PeerID,
	}); err != nil {
		return InitResult{}, fmt.Errorf("migrate: finalize seeded mode: %w", err)
	}
	if err := writeCheckpoint(cfg.WorkDir, Checkpoint{
		Stage:    CheckpointSeededFinalized,
		StoreID:  cfg.StoreID,
		RegionID: cfg.RegionID,
		PeerID:   cfg.PeerID,
	}); err != nil {
		return InitResult{}, err
	}
	if err := validateSeedArtifacts(cfg.WorkDir, cfg.StoreID, cfg.RegionID, cfg.PeerID); err != nil {
		return InitResult{}, err
	}
	return InitResult{
		WorkDir:     cfg.WorkDir,
		Mode:        workdirmode.ModeSeeded,
		StoreID:     cfg.StoreID,
		RegionID:    cfg.RegionID,
		PeerID:      cfg.PeerID,
		SnapshotDir: snapshotDir,
	}, nil
}

func seedSnapshotDir(workDir string, regionID uint64) string {
	return filepath.Join(filepath.Clean(workDir), snapshotRootDirName, fmt.Sprintf("region-%020d", regionID))
}
