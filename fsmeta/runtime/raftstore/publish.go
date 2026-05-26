// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"context"

	coordclient "github.com/feichai0017/NoKV/coordinator/client"
	"github.com/feichai0017/NoKV/fsmeta/model"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
)

// rootPublisher implements both observe.SnapshotPublisher and the
// SubtreeHandoffPublisher interface using the coordinator PublishRootEvent RPC.
type rootPublisher struct {
	coord *coordclient.GRPCClient
}

func (p rootPublisher) PublishSnapshotSubtree(ctx context.Context, t model.SnapshotSubtreeToken) error {
	return p.send(ctx, rootevent.SnapshotEpochPublishedWithRuntimeEvidence(
		string(t.Mount),
		uint64(t.MountKeyID),
		uint64(t.RootInode),
		t.ReadVersion,
		rootSnapshotEvidenceRefsFromToken(t.RuntimeEvidence),
	))
}

func (p rootPublisher) RetireSnapshotSubtree(ctx context.Context, t model.SnapshotSubtreeToken) error {
	return p.send(ctx, rootevent.SnapshotEpochRetired(string(t.Mount), uint64(t.MountKeyID), uint64(t.RootInode), t.ReadVersion))
}

func (p rootPublisher) StartSubtreeHandoff(ctx context.Context, mount model.MountID, root model.InodeID, frontier uint64) error {
	return p.send(ctx, rootevent.SubtreeHandoffStarted(string(mount), uint64(root), frontier))
}

func (p rootPublisher) CompleteSubtreeHandoff(ctx context.Context, mount model.MountID, root model.InodeID, frontier uint64) error {
	return p.send(ctx, rootevent.SubtreeHandoffCompleted(string(mount), uint64(root), frontier))
}

func (p rootPublisher) send(ctx context.Context, event rootevent.Event) error {
	if p.coord == nil {
		return errRootPublisherNotConfigured
	}
	resp, err := p.coord.PublishRootEvent(ctx, &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(event),
	})
	if err != nil {
		return err
	}
	if resp == nil || !resp.GetAccepted() {
		return errRootEventNotAccepted
	}
	return nil
}

func rootSnapshotEvidenceRefsFromToken(refs []model.SnapshotEvidenceRef) []rootproto.SnapshotEvidenceRef {
	if len(refs) == 0 {
		return nil
	}
	out := make([]rootproto.SnapshotEvidenceRef, 0, len(refs))
	for _, ref := range refs {
		out = append(out, rootproto.SnapshotEvidenceRef{
			EpochID:       ref.EpochID,
			EvidenceRoot:  ref.EvidenceRoot,
			PayloadDigest: ref.PayloadDigest,
		})
	}
	return out
}
