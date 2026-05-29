// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
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

func TestPrepareAndSignalHistoryRunsWritesReadyAfterAllScopes(t *testing.T) {
	readyFile := filepath.Join(t.TempDir(), "history.ready")
	cli := &readyAwareScopeClient{
		t:         t,
		readyFile: readyFile,
		nextInode: 1000,
	}
	runs, err := prepareAndSignalHistoryRuns(context.Background(), cli, "vol", 1, 2, 4, "history", readyFile)
	if err != nil {
		t.Fatalf("prepareAndSignalHistoryRuns() error=%v", err)
	}
	if len(runs) != 2 {
		t.Fatalf("prepared runs=%d, want 2", len(runs))
	}
	if cli.creates != 2 {
		t.Fatalf("scope creates=%d, want 2", cli.creates)
	}
	if _, err := os.Stat(readyFile); err != nil {
		t.Fatalf("ready file was not written after scope preparation: %v", err)
	}
}

type readyAwareScopeClient struct {
	t         *testing.T
	readyFile string
	creates   int
	nextInode model.InodeID
}

func (c *readyAwareScopeClient) Create(_ context.Context, req model.CreateRequest) (model.CreateResult, error) {
	c.t.Helper()
	if _, err := os.Stat(c.readyFile); err == nil {
		c.t.Fatalf("ready file was written before all scope creates completed")
	} else if !errors.Is(err, os.ErrNotExist) {
		c.t.Fatalf("stat ready file: %v", err)
	}
	c.creates++
	inode := c.nextInode
	c.nextInode++
	return model.CreateResult{
		Dentry: model.DentryRecord{
			Parent: req.Parent,
			Name:   req.Name,
			Inode:  inode,
			Type:   req.Attrs.Type,
		},
		Inode: req.Attrs.InodeRecord(inode),
	}, nil
}

func (c *readyAwareScopeClient) Lookup(context.Context, model.LookupRequest) (model.DentryRecord, error) {
	c.t.Helper()
	c.t.Fatalf("Lookup should not be needed after successful scope create")
	return model.DentryRecord{}, nil
}

func requireOp(t *testing.T, op fsmetacontract.Operation, kind fsmetacontract.OperationKind, mount model.MountID, parent model.InodeID, name string, inode model.InodeID) {
	t.Helper()
	if op.Kind != kind || op.Mount != mount || op.Parent != parent || op.Name != name || op.Inode != inode {
		t.Fatalf("op=%#v, want kind=%s mount=%s parent=%d name=%q inode=%d", op, kind, mount, parent, name, inode)
	}
}
