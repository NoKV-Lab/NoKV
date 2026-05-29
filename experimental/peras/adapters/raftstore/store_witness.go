// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package raftstore

import (
	"context"
	"fmt"
	"time"

	runtimeperas "github.com/feichai0017/NoKV/experimental/peras/runtime"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/feichai0017/NoKV/raftstore/kv"
	"github.com/feichai0017/NoKV/storage/wal"
	"google.golang.org/grpc"
)

const storeWitnessControlWALBase uint64 = 1 << 63

// ControlWALOpener is the storage capability needed by the store-side Peras
// witness. The stable serve command supplies this as an opaque dependency.
type ControlWALOpener interface {
	OpenControlWAL(uint64) (*wal.Manager, error)
}

// StoreWitnessRuntime is the store-side Peras witness attachment for one
// raftstore process.
type StoreWitnessRuntime struct {
	Witness     *WitnessNode
	Authorities *runtimeperas.ActiveAuthorities
	Feed        *runtimeperas.RootAuthorityFeed
}

// StartStoreWitness attaches the experimental Peras witness service and write
// fence state to one raftstore process.
func StartStoreWitness(ctx context.Context, storeID uint64, coord runtimeperas.RootAuthoritySource, db ControlWALOpener, durability wal.DurabilityPolicy) (*StoreWitnessRuntime, error) {
	if storeID == 0 || coord == nil || db == nil {
		return nil, fmt.Errorf("peras raftstore: witness requires store id, coordinator, and db")
	}
	manager, err := db.OpenControlWAL(storeWitnessControlGroupID(storeID))
	if err != nil {
		return nil, fmt.Errorf("peras raftstore: open witness WAL: %w", err)
	}
	log, err := NewWALWitnessLog(manager, durability)
	if err != nil {
		return nil, fmt.Errorf("peras raftstore: open witness log: %w", err)
	}
	authorities := runtimeperas.NewActiveAuthorities()
	feed := runtimeperas.StartRootAuthorityFeed(ctx, coord, authorities, time.Second)
	witness, err := NewWitnessNode(WitnessNodeConfig{
		NodeID:           fmt.Sprintf("store-%d", storeID),
		Log:              log,
		AuthorityView:    authorities,
		AuthorityRefresh: feed.Refresh,
	})
	if err != nil {
		if feed != nil {
			_ = feed.Close()
		}
		return nil, err
	}
	return &StoreWitnessRuntime{
		Witness:     witness,
		Authorities: authorities,
		Feed:        feed,
	}, nil
}

func (r *StoreWitnessRuntime) Close() error {
	if r == nil || r.Feed == nil {
		return nil
	}
	return r.Feed.Close()
}

func (r *StoreWitnessRuntime) WriteFence() kv.WriteFence {
	if r == nil || r.Authorities == nil {
		return nil
	}
	return storeWitnessWriteFence{authorities: r.Authorities}
}

func (r *StoreWitnessRuntime) RegisterGRPCService(reg grpc.ServiceRegistrar) {
	if r == nil || r.Witness == nil {
		return
	}
	kvrpcpb.RegisterSegmentWitnessServer(reg, NewWitnessService(r.Witness))
}

type storeWitnessWriteFence struct {
	authorities *runtimeperas.ActiveAuthorities
}

func (f storeWitnessWriteFence) FenceKey(key []byte, now time.Time) (kv.WriteFenceDecision, error) {
	if f.authorities == nil {
		return kv.WriteFenceDecision{}, nil
	}
	grant, ok, err := f.authorities.FencesKey(key, now)
	if err != nil || !ok {
		return kv.WriteFenceDecision{}, err
	}
	return kv.WriteFenceDecision{Fenced: true, Reason: grant.GrantID}, nil
}

func storeWitnessControlGroupID(storeID uint64) uint64 {
	return storeWitnessControlWALBase | storeID
}
