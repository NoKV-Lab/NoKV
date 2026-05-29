// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/feichai0017/NoKV/config"
	"github.com/feichai0017/NoKV/coordinator/catalog"
	"github.com/feichai0017/NoKV/coordinator/idalloc"
	pdserver "github.com/feichai0017/NoKV/coordinator/server"
	"github.com/feichai0017/NoKV/coordinator/tso"
	local "github.com/feichai0017/NoKV/local"
	workdirmode "github.com/feichai0017/NoKV/local/workdir"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"github.com/feichai0017/NoKV/raftstore/raftlog"
	serverpkg "github.com/feichai0017/NoKV/raftstore/server"
	storepkg "github.com/feichai0017/NoKV/raftstore/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func testStorage(db *local.DB) serverpkg.Storage {
	if db == nil {
		return serverpkg.Storage{}
	}
	return serverpkg.Storage{
		MVCC: db,
		Raft: raftlog.NewDBLog(db),
	}
}

func TestRunServeCmdErrors(t *testing.T) {
	var buf bytes.Buffer
	require.Error(t, runServeCmd(&buf, nil))
	require.Error(t, runServeCmd(&buf, []string{"-workdir", t.TempDir()}))
	require.Error(t, runServeCmd(&buf, []string{"-workdir", t.TempDir(), "-store-id", "1", "-election-tick", "0"}))
	require.Error(t, runServeCmd(&buf, []string{"-workdir", t.TempDir(), "-store-id", "1", "-store-addr", "bad"}))
	require.Error(t, runServeCmd(&buf, []string{"-workdir", t.TempDir(), "-store-id", "1", "-store-addr", "x=addr"}))
	require.ErrorContains(t, runServeCmd(&buf, []string{"-workdir", t.TempDir(), "-store-id", "1", "-store-addr", "0=addr"}), "store id must be > 0")
	require.ErrorContains(t, runServeCmd(&buf, []string{"-workdir", t.TempDir(), "-store-id", "1", "-store-addr", "2="}), "empty address")
	require.ErrorContains(t, runServeCmd(&buf, []string{"-workdir", t.TempDir(), "-store-id", "1", "-store-addr", "2=127.0.0.1:20160"}), "--coordinator-addr is required")
}

func TestRunServeCmdResolvesStoreDefaultsFromConfig(t *testing.T) {
	withNotifyContext(t, true, func() {
		dir := t.TempDir()
		coordAddr, stopCoordinator := startTestCoordinatorServer(t)
		defer stopCoordinator()

		cfgPath := writeServeConfig(t, config.File{
			Coordinator: &config.Coordinator{
				Addr: coordAddr,
			},
			Stores: []config.Store{{
				StoreID:    1,
				Addr:       "127.0.0.1:20170",
				ListenAddr: "127.0.0.1:0",
				WorkDir:    dir,
			}},
		})

		var buf bytes.Buffer
		err := runServeCmd(&buf, []string{
			"-config", cfgPath,
			"-scope", "host",
			"-store-id", "1",
		})
		require.NoError(t, err)
		require.Contains(t, buf.String(), "Serve mode: cluster (coordinator enabled, addr="+coordAddr+")")
	})
}

func TestRunServeCmdInvalidMetricsAddr(t *testing.T) {
	withNotifyContext(t, true, func() {
		dir := t.TempDir()
		coordAddr, stopCoordinator := startTestCoordinatorServer(t)
		defer stopCoordinator()
		var buf bytes.Buffer
		err := runServeCmd(&buf, []string{
			"-workdir", dir,
			"-store-id", "1",
			"-addr", "127.0.0.1:0",
			"-coordinator-addr", coordAddr,
			"-metrics-addr", "bad",
		})
		require.ErrorContains(t, err, "start serve metrics endpoint")
	})
}

func TestStartStorePeersNil(t *testing.T) {
	_, _, err := startStorePeers(nil, serverpkg.Storage{}, nil, 1, 1, 1, 1, 1)
	require.Error(t, err)
}

func TestStartStorePeersLocalMetadataMissing(t *testing.T) {
	realDB := newTestDB(t)
	server := newTestServer(t, realDB, 1)
	defer func() {
		_ = server.Close()
		_ = realDB.Close()
	}()

	_, _, err := startStorePeers(server, testStorage(&local.DB{}), nil, 1, 10, 1, 1, 1)
	require.Error(t, err)
}

func TestStartStorePeersEmpty(t *testing.T) {
	dir := t.TempDir()
	db := newTestDBWithDir(t, dir)
	server := newTestServer(t, db, 1)
	defer func() {
		_ = server.Close()
		_ = db.Close()
	}()

	localMeta := openLocalMetaStore(t, dir)
	started, total, err := startStorePeers(server, testStorage(db), localMeta, 1, 10, 1, 1, 1)
	require.NoError(t, err)
	require.Equal(t, 0, total)
	require.Empty(t, started)
}

func TestStartStorePeersSkipsMissing(t *testing.T) {
	dir := t.TempDir()
	db := newTestDBWithDir(t, dir)
	localMeta := openLocalMetaStore(t, dir)
	server := newTestServerWithMeta(t, db, 1, localMeta)
	defer func() {
		_ = server.Close()
		_ = db.Close()
	}()

	meta := localmeta.RegionMeta{
		ID:    10,
		State: metaregion.ReplicaStateRunning,
		Epoch: metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers: []metaregion.Peer{{StoreID: 2, PeerID: 200}},
	}
	require.NoError(t, localMeta.SaveRegion(meta))

	started, total, err := startStorePeers(server, testStorage(db), localMeta, 1, 10, 1, 1, 1)
	require.NoError(t, err)
	require.Equal(t, 1, total)
	require.Empty(t, started)
}

func TestStartStorePeersStartsPeer(t *testing.T) {
	dir := t.TempDir()
	db := newTestDBWithDir(t, dir)
	localMeta := openLocalMetaStore(t, dir)
	server := newTestServerWithMeta(t, db, 1, localMeta)
	defer func() {
		_ = server.Close()
		_ = db.Close()
	}()

	meta := localmeta.RegionMeta{
		ID:       11,
		State:    metaregion.ReplicaStateRunning,
		StartKey: []byte("a"),
		EndKey:   []byte("b"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 101}},
	}
	require.NoError(t, localMeta.SaveRegion(meta))
	require.NoError(t, server.Close())
	server = newTestServerWithMeta(t, db, 1, localMeta)

	started, total, err := startStorePeers(server, testStorage(db), localMeta, 1, 10, 1, 1, 1)
	require.NoError(t, err)
	require.Equal(t, 1, total)
	require.Len(t, started, 1)
}

func TestRunServeCmdNoRegions(t *testing.T) {
	withNotifyContext(t, true, func() {
		dir := t.TempDir()
		coordAddr, stopCoordinator := startTestCoordinatorServer(t)
		defer stopCoordinator()
		var buf bytes.Buffer
		err := runServeCmd(&buf, []string{
			"-workdir", dir,
			"-store-id", "1",
			"-addr", "127.0.0.1:0",
			"-coordinator-addr", coordAddr,
			"-metrics-addr", "127.0.0.1:0",
		})
		require.NoError(t, err)
		require.Contains(t, buf.String(), "Local peer catalog contains no regions")
		require.Contains(t, buf.String(), "Serve lifecycle: bootstrap-wait")
		require.Contains(t, buf.String(), "Serve metrics endpoint listening on http://")
		require.Contains(t, buf.String(), "Serve mode: cluster (coordinator enabled, addr="+coordAddr+")")
		state, err := workdirmode.Read(dir)
		require.NoError(t, err)
		require.Equal(t, workdirmode.ModeCluster, state.Mode)
		require.Equal(t, uint64(1), state.StoreID)
	})
}

func TestRunServeCmdWithRegions(t *testing.T) {
	withNotifyContext(t, true, func() {
		dir := t.TempDir()
		coordAddr, stopCoordinator := startTestCoordinatorServer(t)
		defer stopCoordinator()
		localMeta := openLocalMetaStore(t, dir)
		require.NoError(t, localMeta.SaveRegion(localmeta.RegionMeta{
			ID:       1,
			State:    metaregion.ReplicaStateRunning,
			StartKey: nil,
			EndKey:   nil,
			Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
			Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 101}},
		}))
		require.NoError(t, localMeta.SaveRegion(localmeta.RegionMeta{
			ID:       2,
			State:    metaregion.ReplicaStateRunning,
			StartKey: []byte("b"),
			EndKey:   []byte("c"),
			Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
			Peers:    []metaregion.Peer{{StoreID: 2, PeerID: 201}},
		}))

		var buf bytes.Buffer
		err := runServeCmd(&buf, []string{
			"-workdir", dir,
			"-store-id", "1",
			"-addr", "127.0.0.1:0",
			"-store-addr", "2=127.0.0.1:20160",
			"-coordinator-addr", coordAddr,
		})
		require.NoError(t, err)
		out := buf.String()
		require.Contains(t, out, "Local peer catalog regions: 2, local peers started: 1")
		require.Contains(t, out, "Serve lifecycle: restart-recover")
		require.Contains(t, out, "Store 1 not present in 1 region(s)")
		require.Contains(t, out, "Sample regions:")
		require.Contains(t, out, "Configured store address overrides:")
		require.Contains(t, out, "Serve mode: cluster (coordinator enabled, addr="+coordAddr+")")
		require.Contains(t, out, "coordinator heartbeat sink enabled: "+coordAddr)
		state, err := workdirmode.Read(dir)
		require.NoError(t, err)
		require.Equal(t, workdirmode.ModeCluster, state.Mode)
		require.Equal(t, uint64(1), state.StoreID)
	})
}

func TestResolveTransportPeersFromConfig(t *testing.T) {
	cfg := &config.File{
		Stores: []config.Store{
			{StoreID: 1, Addr: "127.0.0.1:20170"},
			{StoreID: 2, Addr: "127.0.0.1:20171"},
			{StoreID: 3, Addr: "127.0.0.1:20172"},
		},
	}
	snapshot := map[uint64]localmeta.RegionMeta{
		1: {
			ID:    1,
			Peers: []metaregion.Peer{{StoreID: 1, PeerID: 101}, {StoreID: 2, PeerID: 201}, {StoreID: 3, PeerID: 301}},
		},
	}
	peers, err := resolveTransportPeers(snapshot, 1, cfg, "host", nil)
	require.NoError(t, err)
	require.Equal(t, map[uint64]string{
		201: "127.0.0.1:20171",
		301: "127.0.0.1:20172",
	}, peers)
}

func TestResolveTransportPeersFromConfigHonorsExplicitOverride(t *testing.T) {
	cfg := &config.File{
		Stores: []config.Store{
			{StoreID: 1, Addr: "127.0.0.1:20170"},
			{StoreID: 2, Addr: "127.0.0.1:20171"},
		},
	}
	snapshot := map[uint64]localmeta.RegionMeta{
		1: {
			ID:    1,
			Peers: []metaregion.Peer{{StoreID: 1, PeerID: 101}, {StoreID: 2, PeerID: 201}},
		},
	}
	peers, err := resolveTransportPeers(snapshot, 1, cfg, "host", map[uint64]string{2: "127.0.0.1:29999"})
	require.NoError(t, err)
	require.Equal(t, map[uint64]string{201: "127.0.0.1:29999"}, peers)
}

func TestResolveTransportPeersFromConfigRequiresStoreAddr(t *testing.T) {
	cfg := &config.File{
		Stores: []config.Store{
			{StoreID: 1, Addr: "127.0.0.1:20170"},
			{StoreID: 2},
		},
	}
	snapshot := map[uint64]localmeta.RegionMeta{
		1: {
			ID:    1,
			Peers: []metaregion.Peer{{StoreID: 1, PeerID: 101}, {StoreID: 2, PeerID: 201}},
		},
	}
	_, err := resolveTransportPeers(snapshot, 1, cfg, "host", nil)
	require.ErrorContains(t, err, "missing transport address for remote store 2")
}

func TestResolveTransportPeersRequiresExplicitStoreOverrideWithoutConfig(t *testing.T) {
	snapshot := map[uint64]localmeta.RegionMeta{
		1: {
			ID:    1,
			Peers: []metaregion.Peer{{StoreID: 1, PeerID: 101}, {StoreID: 2, PeerID: 201}},
		},
	}
	_, err := resolveTransportPeers(snapshot, 1, nil, "host", nil)
	require.ErrorContains(t, err, "missing transport address for remote store 2")
	peers, err := resolveTransportPeers(snapshot, 1, nil, "host", map[uint64]string{2: "127.0.0.1:20171"})
	require.NoError(t, err)
	require.Equal(t, map[uint64]string{201: "127.0.0.1:20171"}, peers)
}

func TestResolveTransportPeersRejectsUnusedStoreOverride(t *testing.T) {
	snapshot := map[uint64]localmeta.RegionMeta{
		1: {
			ID:    1,
			Peers: []metaregion.Peer{{StoreID: 1, PeerID: 101}, {StoreID: 2, PeerID: 201}},
		},
	}
	_, err := resolveTransportPeers(snapshot, 1, nil, "host", map[uint64]string{
		2: "127.0.0.1:20171",
		9: "127.0.0.1:20179",
	})
	require.ErrorContains(t, err, "unused --store-addr override for store 9")
}

func TestResolveTransportPeersRejectsOverridesWithoutRemoteStores(t *testing.T) {
	_, err := resolveTransportPeers(nil, 1, nil, "host", map[uint64]string{2: "127.0.0.1:20171"})
	require.ErrorContains(t, err, "local metadata has no remote stores")
}

func TestResolveStoreAdvertiseAddrFromConfig(t *testing.T) {
	cfg := &config.File{
		Stores: []config.Store{{
			StoreID:    1,
			Addr:       "127.0.0.1:20170",
			DockerAddr: "nokv-store-1:20160",
		}},
	}
	require.Equal(t, "127.0.0.1:20170", resolveStoreAdvertiseAddr(cfg, 1, "host"))
	require.Equal(t, "nokv-store-1:20160", resolveStoreAdvertiseAddr(cfg, 1, "docker"))
	require.Empty(t, resolveStoreAdvertiseAddr(cfg, 2, "docker"))
	require.Empty(t, resolveStoreAdvertiseAddr(nil, 1, "docker"))
}

func TestValidateServeModeRejectsStoreMismatch(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, workdirmode.Write(dir, workdirmode.State{
		Mode:    workdirmode.ModeCluster,
		StoreID: 2,
	}))
	_, err := validateServeMode(dir, 1)
	require.ErrorContains(t, err, "bound to store 2, not store 1")
}

func TestPromoteClusterModeClearsBootstrapIdentity(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, workdirmode.Write(dir, workdirmode.State{
		Mode:     workdirmode.ModeSeeded,
		StoreID:  1,
		RegionID: 9,
		PeerID:   19,
	}))
	require.NoError(t, promoteClusterMode(dir, 1))
	state, err := workdirmode.Read(dir)
	require.NoError(t, err)
	require.Equal(t, workdirmode.ModeCluster, state.Mode)
	require.Equal(t, uint64(1), state.StoreID)
	require.Zero(t, state.RegionID)
	require.Zero(t, state.PeerID)
}

func TestFormatKeyNonEmpty(t *testing.T) {
	require.Equal(t, "\"a\"", formatKey([]byte("a"), true))
	require.Equal(t, "\"b\"", formatKey([]byte("b"), false))
}

func withNotifyContext(t *testing.T, cancelImmediately bool, fn func()) {
	t.Helper()
	origNotify := notifyContext
	defer func() { notifyContext = origNotify }()
	notifyContext = func(parent context.Context, _ ...os.Signal) (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(parent)
		if cancelImmediately {
			cancel()
		}
		return ctx, cancel
	}
	fn()
}

func newTestDB(t *testing.T) *local.DB {
	t.Helper()
	return newTestDBWithDir(t, t.TempDir())
}

func newTestDBWithDir(t *testing.T, dir string) *local.DB {
	t.Helper()
	opt := local.NewDefaultOptions()
	opt.WorkDir = dir
	db, err := local.Open(opt)
	require.NoError(t, err)
	return db
}

func openLocalMetaStore(t *testing.T, dir string) *localmeta.Store {
	t.Helper()
	store, err := localmeta.OpenLocalStore(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store
}

func newTestServer(t *testing.T, db *local.DB, storeID uint64) *serverpkg.Node {
	return newTestServerWithMeta(t, db, storeID, nil)
}

func newTestServerWithMeta(t *testing.T, db *local.DB, storeID uint64, localMeta *localmeta.Store) *serverpkg.Node {
	t.Helper()
	server, err := serverpkg.NewNode(serverpkg.Config{
		Storage: serverpkg.Storage{
			MVCC: db,
			Raft: raftlog.NewDBLog(db),
		},
		Store: storepkg.Config{
			StoreID:   storeID,
			LocalMeta: localMeta,
		},
		TransportAddr: "127.0.0.1:0",
	})
	require.NoError(t, err)
	return server
}

func startTestCoordinatorServer(t *testing.T) (addr string, stop func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	svc := pdserver.NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1))
	srv := grpc.NewServer()
	coordpb.RegisterCoordinatorServer(srv, svc)

	go func() {
		_ = srv.Serve(lis)
	}()
	return lis.Addr().String(), func() {
		srv.Stop()
		_ = lis.Close()
	}
}

func writeServeConfig(t *testing.T, cfg config.File) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "raft_config.json")
	content, err := json.MarshalIndent(cfg, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, content, 0o600))
	return path
}
