// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	nokverrors "github.com/feichai0017/NoKV/errors"
	fsmetacontract "github.com/feichai0017/NoKV/fsmeta/contract"
	"github.com/feichai0017/NoKV/fsmeta/model"
)

func TestExternalHistoryOpsScopesRootOperationsAndFiltersInternalSessions(t *testing.T) {
	const (
		mount      = model.MountID("prod")
		scopeName  = "history-scope"
		scopeInode = model.InodeID(9001)
	)
	scopeOp := scopeCreateOperation(mount, scopeName, scopeInode)
	requireOp(t, scopeOp, fsmetacontract.OpCreate, mount, model.RootInode, scopeName, scopeInode)

	ops := externalHistoryOps([]fsmetacontract.Operation{
		{Kind: fsmetacontract.OpOpenWriteSession, Mount: "vol", Parent: model.RootInode, Inode: 10},
		{Kind: fsmetacontract.OpCreate, Mount: "vol", Parent: model.RootInode, Name: "alpha", Inode: 11},
		{Kind: fsmetacontract.OpRenameSubtree, Mount: "vol", FromParent: model.RootInode, FromName: "alpha", ToParent: model.RootInode, ToName: "beta"},
		{Kind: fsmetacontract.OpLink, Mount: "vol", Parent: model.RootInode, Name: "link", Inode: 11},
		{Kind: fsmetacontract.OpAdvanceTime, Mount: "vol", AdvanceNs: 1},
	}, mount, scopeInode, scopeInode)

	if len(ops) != 3 {
		t.Fatalf("filtered op count=%d, want 3: %#v", len(ops), ops)
	}
	requireOp(t, ops[0], fsmetacontract.OpCreate, mount, scopeInode, "alpha", scopeInode+11)
	if ops[1].Mount != mount || ops[1].FromParent != scopeInode || ops[1].ToParent != scopeInode {
		t.Fatalf("rename was not scoped into generated root: %#v", ops[1])
	}
	requireOp(t, ops[2], fsmetacontract.OpLink, mount, scopeInode, "link", scopeInode+11)
	if got := scopeGeneratedInode(scopeInode, 0); got != 0 {
		t.Fatalf("zero inode remapped to %d", got)
	}
}

func TestRetryScopeCreateErrorIncludesStartupAvailabilityWindows(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "mount not registered", err: model.ErrMountNotRegistered, want: true},
		{name: "not found", err: nokverrors.New(nokverrors.KindNotFound, "root not admitted"), want: true},
		{name: "inner retry exhausted", err: nokverrors.New(nokverrors.KindRetryExhausted, "client: kv get retries exhausted"), want: true},
		{name: "unavailable", err: nokverrors.New(nokverrors.KindUnavailable, "store restarting"), want: true},
		{name: "invalid", err: nokverrors.New(nokverrors.KindInvalidArgument, "bad request"), want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := retryScopeCreateError(tc.err); got != tc.want {
				t.Fatalf("retryScopeCreateError(%v)=%v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func requireOp(t *testing.T, op fsmetacontract.Operation, kind fsmetacontract.OperationKind, mount model.MountID, parent model.InodeID, name string, inode model.InodeID) {
	t.Helper()
	if op.Kind != kind || op.Mount != mount || op.Parent != parent || op.Name != name || op.Inode != inode {
		t.Fatalf("op=%#v, want kind=%s mount=%s parent=%d name=%q inode=%d", op, kind, mount, parent, name, inode)
	}
}
