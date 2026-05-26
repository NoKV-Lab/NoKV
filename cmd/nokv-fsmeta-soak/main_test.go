// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"
	"time"

	fsmetacontract "github.com/feichai0017/NoKV/fsmeta/contract"
	"github.com/feichai0017/NoKV/fsmeta/model"
)

func TestSoakHistoryOpsScopesExternalNamespaceOperations(t *testing.T) {
	const (
		mount      = model.MountID("prod")
		scopeInode = model.InodeID(8001)
	)
	ops := soakHistoryOps([]fsmetacontract.Operation{
		{Kind: fsmetacontract.OpHeartbeatSession, Mount: "vol", Session: "writer-a"},
		{Kind: fsmetacontract.OpLookup, Mount: "vol", Parent: model.RootInode, Name: "alpha"},
		{Kind: fsmetacontract.OpReadDirPlus, Mount: "vol", Parent: model.RootInode, StartAfter: "a", Limit: 10},
		{Kind: fsmetacontract.OpUnlink, Mount: "vol", Parent: model.RootInode, Name: "alpha"},
		{Kind: fsmetacontract.OpExpireSessions, Mount: "vol", Limit: 1},
	}, mount, scopeInode)

	if len(ops) != 3 {
		t.Fatalf("filtered op count=%d, want 3: %#v", len(ops), ops)
	}
	for _, op := range ops {
		if op.Mount != mount || op.Parent != scopeInode {
			t.Fatalf("op was not scoped into generated root: %#v", op)
		}
	}
	if ops[1].StartAfter != "a" || ops[1].Limit != 10 {
		t.Fatalf("ReadDirPlus pagination fields changed: %#v", ops[1])
	}
	if ops[2].Inode != 0 {
		t.Fatalf("zero inode remapped to %d", ops[2].Inode)
	}
}

func TestShouldRunSoakRoundKeepsFinalDeadlineForCleanup(t *testing.T) {
	now := time.Unix(100, 0)
	if !shouldRunSoakRound(now, now.Add(minSoakRoundBudget), minSoakRoundBudget) {
		t.Fatal("expected exact minimum budget to allow one more round")
	}
	if shouldRunSoakRound(now, now.Add(minSoakRoundBudget-time.Nanosecond), minSoakRoundBudget) {
		t.Fatal("expected short remaining budget to stop before probes inherit an expiring context")
	}
}
