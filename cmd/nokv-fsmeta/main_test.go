// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"expvar"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	fsmetaclient "github.com/feichai0017/NoKV/fsmeta/client"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/feichai0017/NoKV/fsmeta/observe"
	fsmetalocal "github.com/feichai0017/NoKV/fsmeta/runtime/local"
	"github.com/feichai0017/NoKV/storage/wal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestPublishExpvarOnceDoesNotOverwriteExistingMetric(t *testing.T) {
	name := fmt.Sprintf("nokv_fsmeta_test_%s", t.Name())
	first := expvar.Func(func() any { return "first" })
	second := expvar.Func(func() any { return "second" })

	publishExpvarOnce(name, first)
	publishExpvarOnce(name, second)

	got := expvar.Get(name)
	if got == nil {
		t.Fatal("expected expvar metric to be published")
	}
	if got.String() != `"first"` {
		t.Fatalf("metric was overwritten, got %s", got.String())
	}
}

func TestParsePerasVisibleLogPolicy(t *testing.T) {
	cases := map[string]wal.DurabilityPolicy{
		"":              wal.DurabilityFlushed,
		"flushed":       wal.DurabilityFlushed,
		"fsync-batched": wal.DurabilityFsyncBatched,
		"fsync_batched": wal.DurabilityFsyncBatched,
		"batched":       wal.DurabilityFsyncBatched,
		"fsync":         wal.DurabilityFsync,
		"buffered":      wal.DurabilityBuffered,
	}
	for input, want := range cases {
		got, err := parsePerasVisibleLogPolicy(input)
		if err != nil {
			t.Fatalf("parse %q: %v", input, err)
		}
		if got != want {
			t.Fatalf("parse %q got %v want %v", input, got, want)
		}
	}
	if _, err := parsePerasVisibleLogPolicy("bad"); err == nil {
		t.Fatal("expected invalid policy to fail")
	}
}

func TestParseFSMetaBackend(t *testing.T) {
	cases := map[string]fsmetaBackend{
		"":          fsmetaBackendRaftstore,
		"raftstore": fsmetaBackendRaftstore,
		"RAFTSTORE": fsmetaBackendRaftstore,
		"local":     fsmetaBackendLocal,
		" LOCAL ":   fsmetaBackendLocal,
	}
	for input, want := range cases {
		got, err := parseFSMetaBackend(input)
		if err != nil {
			t.Fatalf("parse %q: %v", input, err)
		}
		if got != want {
			t.Fatalf("parse %q got %q want %q", input, got, want)
		}
	}
	if _, err := parseFSMetaBackend("memory"); err == nil {
		t.Fatal("expected invalid backend to fail")
	}
}

func TestLocalMountIdentity(t *testing.T) {
	got, err := localMountIdentity(" vol ", 7)
	if err != nil {
		t.Fatalf("local mount identity: %v", err)
	}
	if got != (model.MountIdentity{MountID: "vol", MountKeyID: 7}) {
		t.Fatalf("got %+v", got)
	}
	if _, err := localMountIdentity("", 1); !errors.Is(err, model.ErrInvalidMountID) {
		t.Fatalf("empty mount err=%v", err)
	}
	if _, err := localMountIdentity("vol", 0); !errors.Is(err, model.ErrInvalidMountID) {
		t.Fatalf("zero mount key err=%v", err)
	}
}

func TestOpenConfiguredRuntimeLocal(t *testing.T) {
	ctx := context.Background()
	rt, err := openConfiguredRuntime(ctx, configuredRuntimeOptions{
		Backend: fsmetaBackendLocal,
		Local: fsmetalocal.Options{
			WorkDir: t.TempDir(),
			Mount:   model.MountIdentity{MountID: "vol", MountKeyID: 1},
		},
	})
	if err != nil {
		t.Fatalf("open local runtime: %v", err)
	}
	defer func() {
		if err := rt.close(); err != nil {
			t.Fatalf("close local runtime: %v", err)
		}
	}()
	if rt.executor == nil {
		t.Fatal("expected executor")
	}
	if rt.watcher == nil {
		t.Fatal("expected local watcher")
	}
	if rt.snapshot == nil {
		t.Fatal("expected local snapshot publisher")
	}
	if !strings.Contains(rt.startupSummary, "fsmeta backend: local") || strings.Contains(rt.startupSummary, "peras=") {
		t.Fatalf("local runtime summary should stay direct-only: %s", rt.startupSummary)
	}
	result, err := rt.executor.Create(ctx, model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "alpha",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	})
	if err != nil {
		t.Fatalf("create through local runtime: %v", err)
	}
	got, err := rt.executor.Lookup(ctx, model.LookupRequest{Mount: "vol", Parent: model.RootInode, Name: "alpha"})
	if err != nil {
		t.Fatalf("lookup through local runtime: %v", err)
	}
	if got != result.Dentry {
		t.Fatalf("lookup got %+v want %+v", got, result.Dentry)
	}
}

func TestOpenConfiguredRuntimeLocalCommitContract(t *testing.T) {
	ctx := context.Background()
	rt, err := openConfiguredRuntime(ctx, configuredRuntimeOptions{
		Backend: fsmetaBackendLocal,
		Local: fsmetalocal.Options{
			WorkDir: t.TempDir(),
			Mount:   model.MountIdentity{MountID: "vol", MountKeyID: 1},
		},
	})
	if err != nil {
		t.Fatalf("open local runtime: %v", err)
	}
	defer func() {
		if err := rt.close(); err != nil {
			t.Fatalf("close local runtime: %v", err)
		}
	}()
	if !strings.Contains(rt.contractLog, "one embedded MVCC store") {
		t.Fatalf("unexpected local contract log: %s", rt.contractLog)
	}
	contract := localCommitContractStats()
	if got := contract["default_write_path"]; got != "local_mvcc" {
		t.Fatalf("default write path got %v", got)
	}
	if got := contract["successful_write_boundary"]; got != "durable" {
		t.Fatalf("successful write boundary got %v", got)
	}
}

func TestLocalRuntimeRegistersWatchAndSnapshot(t *testing.T) {
	ctx := context.Background()
	rt, err := openConfiguredRuntime(ctx, configuredRuntimeOptions{
		Backend: fsmetaBackendLocal,
		Local: fsmetalocal.Options{
			WorkDir: t.TempDir(),
			Mount:   model.MountIdentity{MountID: "vol", MountKeyID: 1},
		},
	})
	if err != nil {
		t.Fatalf("open local runtime: %v", err)
	}
	defer func() {
		if err := rt.close(); err != nil {
			t.Fatalf("close local runtime: %v", err)
		}
	}()

	cli, cleanup := openLocalBufconnClient(t, rt)
	defer cleanup()

	watch, err := cli.WatchSubtree(ctx, observe.WatchRequest{
		Mount:     "vol",
		RootInode: model.RootInode,
	})
	if err != nil {
		t.Fatalf("watch local runtime: %v", err)
	}
	defer func() { _ = watch.Close() }()

	_, err = cli.Create(ctx, model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "over-grpc",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	})
	if err != nil {
		t.Fatalf("create over local grpc: %v", err)
	}
	evt := recvClientWatchEvent(t, watch)
	wantKey, err := layout.EncodeDentryKey(model.MountIdentity{MountID: "vol", MountKeyID: 1}, model.RootInode, "over-grpc")
	if err != nil {
		t.Fatalf("encode dentry key: %v", err)
	}
	if string(evt.Key) != string(wantKey) {
		t.Fatalf("watch key got %q want %q", evt.Key, wantKey)
	}
	if err := watch.Ack(evt.Cursor); err != nil {
		t.Fatalf("ack watch event: %v", err)
	}
	usage, err := cli.GetQuotaUsage(ctx, model.QuotaUsageRequest{
		Mount: "vol",
		Scope: model.RootInode,
	})
	if err != nil {
		t.Fatalf("get local quota usage: %v", err)
	}
	if usage.Inodes != 1 {
		t.Fatalf("quota usage got %+v", usage)
	}

	token, err := cli.SnapshotSubtree(ctx, model.SnapshotSubtreeRequest{
		Mount:     "vol",
		RootInode: model.RootInode,
	})
	if err != nil {
		t.Fatalf("snapshot over local grpc: %v", err)
	}
	if token.Mount != "vol" || token.RootInode != model.RootInode || token.ReadVersion == 0 {
		t.Fatalf("bad snapshot token: %+v", token)
	}
	if err := cli.RetireSnapshotSubtree(ctx, token); err != nil {
		t.Fatalf("retire local snapshot: %v", err)
	}
}

func openLocalBufconnClient(t *testing.T, rt *fsmetaServerRuntime) (*fsmetaclient.GRPCClient, func()) {
	t.Helper()
	const bufSize = 1 << 20
	listener := bufconn.Listen(bufSize)
	grpcServer := grpc.NewServer()
	registerFSMetadataServer(grpcServer, rt)
	go func() {
		_ = grpcServer.Serve(listener)
	}()
	dialer := func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}
	cli, err := fsmetaclient.NewGRPCClient(context.Background(), "passthrough:///fsmeta-local-bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dialer),
	)
	if err != nil {
		t.Fatalf("dial local bufconn fsmeta: %v", err)
	}
	return cli, func() {
		_ = cli.Close()
		grpcServer.Stop()
		_ = listener.Close()
	}
}

func recvClientWatchEvent(t *testing.T, watch fsmetaclient.WatchSubscription) observe.WatchEvent {
	t.Helper()
	type result struct {
		evt observe.WatchEvent
		err error
	}
	ch := make(chan result, 1)
	go func() {
		evt, err := watch.Recv()
		ch <- result{evt: evt, err: err}
	}()
	select {
	case got := <-ch:
		if got.err != nil {
			t.Fatalf("receive watch event: %v", got.err)
		}
		return got.evt
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for local watch event")
		return observe.WatchEvent{}
	}
}
