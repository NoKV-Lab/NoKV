// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package migrate

import (
	"fmt"

	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	snapshotpkg "github.com/feichai0017/NoKV/raftstore/snapshot/sst"
)

func validateSeedArtifacts(workDir string, storeID, regionID, peerID uint64) error {
	localMeta, err := localmeta.OpenLocalStore(workDir, nil)
	if err != nil {
		return fmt.Errorf("migrate: validate local catalog: %w", err)
	}
	defer func() { _ = localMeta.Close() }()

	regions := localMeta.Snapshot()
	region, ok := regions[regionID]
	if !ok {
		return fmt.Errorf("migrate: validate seed region %d: local catalog missing region", regionID)
	}
	if len(region.Peers) != 1 {
		return fmt.Errorf("migrate: validate seed region %d: expected 1 peer, got %d", regionID, len(region.Peers))
	}
	if region.Peers[0].StoreID != storeID || region.Peers[0].PeerID != peerID {
		return fmt.Errorf("migrate: validate seed region %d: local catalog peer mismatch got store=%d peer=%d want store=%d peer=%d",
			regionID, region.Peers[0].StoreID, region.Peers[0].PeerID, storeID, peerID)
	}

	meta, err := snapshotpkg.ReadMeta(seedSnapshotDir(workDir, regionID), nil)
	if err != nil {
		return fmt.Errorf("migrate: validate seed snapshot meta: %w", err)
	}
	if meta.Region.ID != regionID {
		return fmt.Errorf("migrate: validate seed snapshot meta: region id mismatch got=%d want=%d", meta.Region.ID, regionID)
	}
	if len(meta.Region.Peers) != 1 || meta.Region.Peers[0].StoreID != storeID || meta.Region.Peers[0].PeerID != peerID {
		return fmt.Errorf("migrate: validate seed snapshot meta: peer mismatch")
	}

	if _, ok := localMeta.RaftPointer(regionID); !ok {
		return fmt.Errorf("migrate: validate seed raft state: missing local raft pointer for region %d", regionID)
	}
	return nil
}

func validateExpandResult(step ExpandResult) error {
	if !regionContainsPeer(step.LeaderRegion, step.PeerID) {
		return fmt.Errorf("migrate: validate expand result: leader metadata still missing peer %d in region %d", step.PeerID, step.RegionID)
	}
	if step.Waited && step.TargetAdminAddr != "" {
		if !step.TargetKnown || !step.TargetHosted || step.TargetLocalPeerID != step.PeerID || step.TargetAppliedIdx == 0 {
			return fmt.Errorf("migrate: validate expand result: target %s has inconsistent hosted state known=%t hosted=%t local_peer=%d applied=%d",
				step.TargetAdminAddr, step.TargetKnown, step.TargetHosted, step.TargetLocalPeerID, step.TargetAppliedIdx)
		}
	}
	return nil
}

func validateTransferLeaderResult(result TransferLeaderResult) error {
	if result.Waited && result.LeaderPeerID != result.PeerID {
		return fmt.Errorf("migrate: validate transfer result: leader peer mismatch got=%d want=%d", result.LeaderPeerID, result.PeerID)
	}
	if result.Waited && result.TargetAdminAddr != "" {
		if !result.TargetKnown || !result.TargetHosted || !result.TargetLeader || result.TargetLocalID != result.PeerID {
			return fmt.Errorf("migrate: validate transfer result: target %s has inconsistent leader state known=%t hosted=%t leader=%t local_peer=%d want=%d",
				result.TargetAdminAddr, result.TargetKnown, result.TargetHosted, result.TargetLeader, result.TargetLocalID, result.PeerID)
		}
	}
	return nil
}

func validateRemovePeerResult(result RemovePeerResult) error {
	if result.Waited && result.LeaderKnown && regionContainsPeer(result.LeaderRegion, result.PeerID) {
		return fmt.Errorf("migrate: validate remove result: leader metadata still contains peer %d in region %d", result.PeerID, result.RegionID)
	}
	if result.Waited && result.TargetAdminAddr != "" {
		if result.TargetKnown && result.TargetHosted && result.TargetLocalPeer == result.PeerID {
			return fmt.Errorf("migrate: validate remove result: target %s still hosts peer %d", result.TargetAdminAddr, result.PeerID)
		}
	}
	return nil
}
