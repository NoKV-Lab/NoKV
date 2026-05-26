// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
	metaregion "github.com/feichai0017/NoKV/meta/region"

	"github.com/feichai0017/NoKV/config"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"github.com/stretchr/testify/require"
)

func TestRunStoresSimpleFormat(t *testing.T) {
	cfgPath := writeSampleConfig(t)

	output, err := captureStdout(t, func() error {
		return runStores([]string{"--config", cfgPath, "--format", "simple"})
	})
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(output), "\n")
	require.Len(t, lines, 2)
	require.Equal(t, "1 127.0.0.1:10170 127.0.0.1:10170 10.0.0.1:20160 store1-docker ./artifacts/cluster/store-1 /var/lib/nokv/store-1", lines[0])
	require.Equal(t, "2 127.0.0.1:10171 127.0.0.1:10171 127.0.0.1:10171 127.0.0.1:10171 ./custom/store-2 /var/lib/custom/store-2", lines[1])
}

func TestRunRegionsSimpleFormat(t *testing.T) {
	cfgPath := writeSampleConfig(t)

	output, err := captureStdout(t, func() error {
		return runRegions([]string{"--config", cfgPath, "--format", "simple"})
	})
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(output), "\n")
	require.Len(t, lines, 2)
	require.Equal(t, "1 - m 1 1 1:101,2:201 1", lines[0])
	require.Equal(t, "2 m hex:0001 2 3 2:202 2", lines[1])
}

func TestRunStoresJSONFormat(t *testing.T) {
	cfgPath := writeSampleConfig(t)
	output, err := captureStdout(t, func() error {
		return runStores([]string{"--config", cfgPath, "--format", "json"})
	})
	require.NoError(t, err)
	var stores []config.Store
	require.NoError(t, json.Unmarshal([]byte(output), &stores))
	require.Len(t, stores, 2)
}

func TestRunRegionsJSONFormat(t *testing.T) {
	cfgPath := writeSampleConfig(t)
	output, err := captureStdout(t, func() error {
		return runRegions([]string{"--config", cfgPath, "--format", "json"})
	})
	require.NoError(t, err)
	var regions []config.Region
	require.NoError(t, json.Unmarshal([]byte(output), &regions))
	require.Len(t, regions, 2)
}

func TestRunRegionsExpandsFSMetaBootstrapLayout(t *testing.T) {
	cfg := config.File{
		Stores: []config.Store{
			{StoreID: 1, Addr: "store-1"},
			{StoreID: 2, Addr: "store-2"},
			{StoreID: 3, Addr: "store-3"},
		},
		FSMetaRegionBootstrap: &config.FSMetaRegionBootstrap{
			Mounts:         []config.FSMetaRegionBootstrapMount{{MountID: "fsmeta-bench", MountKeyID: 2}},
			BucketCount:    layout.DefaultAffinityBucketCount,
			RegionIDBase:   1000,
			PeerIDBase:     10_000,
			LeaderStoreIDs: []uint64{1, 2, 3},
		},
	}
	data, err := json.Marshal(cfg)
	require.NoError(t, err)
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.json")
	require.NoError(t, os.WriteFile(cfgPath, data, 0o600))

	output, err := captureStdout(t, func() error {
		return runRegions([]string{"--config", cfgPath, "--format", "json"})
	})
	require.NoError(t, err)
	var regions []config.Region
	require.NoError(t, json.Unmarshal([]byte(output), &regions))
	require.Len(t, regions, layout.DefaultAffinityBucketCount+2)
	require.Equal(t, uint64(1000), regions[0].ID)
	require.Equal(t, uint64(1), regions[0].LeaderStoreID)
	require.Equal(t, uint64(2), regions[1].LeaderStoreID)
	require.Len(t, regions[1].Peers, 3)
	require.Equal(t, uint64(10_003), regions[1].Peers[0].PeerID)

	start, end, err := layout.EncodeBucketRange(model.MountIdentity{MountID: "fsmeta-bench", MountKeyID: 2}, 0)
	require.NoError(t, err)
	require.Equal(t, start, []byte(regions[1].StartKey))
	require.Equal(t, end, []byte(regions[1].EndKey))
}

func TestRunMetaRootSimpleFormat(t *testing.T) {
	cfgPath := writeSampleConfig(t)

	output, err := captureStdout(t, func() error {
		return runMetaRoot([]string{"--config", cfgPath, "--format", "simple", "--scope", "host"})
	})
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(output), "\n")
	require.Len(t, lines, 3)
	require.Equal(t, "1 127.0.0.1:2380 127.0.0.1:3380 ./artifacts/cluster/meta-root-1", lines[0])
	require.Equal(t, "2 127.0.0.1:2381 127.0.0.1:3381 ./artifacts/cluster/meta-root-2", lines[1])
	require.Equal(t, "3 127.0.0.1:2382 127.0.0.1:3382 ./artifacts/cluster/meta-root-3", lines[2])

	output, err = captureStdout(t, func() error {
		return runMetaRoot([]string{"--config", cfgPath, "--format", "simple", "--scope", "docker"})
	})
	require.NoError(t, err)
	lines = strings.Split(strings.TrimSpace(output), "\n")
	require.Equal(t, "1 nokv-meta-root-1:2380 nokv-meta-root-1:2480 /var/lib/nokv-meta-root-1", lines[0])
}

func TestRunMetaRootJSONFormat(t *testing.T) {
	cfgPath := writeSampleConfig(t)
	output, err := captureStdout(t, func() error {
		return runMetaRoot([]string{"--config", cfgPath, "--format", "json"})
	})
	require.NoError(t, err)
	var peers []config.MetaRootPeer
	require.NoError(t, json.Unmarshal([]byte(output), &peers))
	require.Len(t, peers, 3)
	require.Equal(t, uint64(1), peers[0].NodeID)
}

func TestRunPDSimpleFormat(t *testing.T) {
	cfgPath := writeSampleConfig(t)

	output, err := captureStdout(t, func() error {
		return runCoordinator([]string{"--config", cfgPath, "--format", "simple", "--scope", "host"})
	})
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1:2379", strings.TrimSpace(output))

	output, err = captureStdout(t, func() error {
		return runCoordinator([]string{"--config", cfgPath, "--format", "simple", "--scope", "docker"})
	})
	require.NoError(t, err)
	require.Equal(t, "nokv-pd:2379", strings.TrimSpace(output))

	output, err = captureStdout(t, func() error {
		return runCoordinator([]string{"--config", cfgPath, "--format", "simple", "--scope", "host", "--field", "workdir"})
	})
	require.NoError(t, err)
	require.Equal(t, "./artifacts/cluster/coordinator", strings.TrimSpace(output))

	output, err = captureStdout(t, func() error {
		return runCoordinator([]string{"--config", cfgPath, "--format", "simple", "--scope", "docker", "--field", "workdir"})
	})
	require.NoError(t, err)
	require.Equal(t, "/var/lib/nokv-coordinator", strings.TrimSpace(output))
}

func TestRunPDJSONFormat(t *testing.T) {
	cfgPath := writeSampleConfig(t)
	output, err := captureStdout(t, func() error {
		return runCoordinator([]string{"--config", cfgPath, "--format", "json"})
	})
	require.NoError(t, err)
	var endpoint config.Coordinator
	require.NoError(t, json.Unmarshal([]byte(output), &endpoint))
	require.Equal(t, "127.0.0.1:2379", endpoint.Addr)
	require.Equal(t, "nokv-pd:2379", endpoint.DockerAddr)
	require.Equal(t, "./artifacts/cluster/coordinator", endpoint.WorkDir)
	require.Equal(t, "/var/lib/nokv-coordinator", endpoint.DockerWorkDir)
}

func TestRunRegionCatalogWritesRegion(t *testing.T) {
	dir := t.TempDir()
	manifestDir := filepath.Join(dir, "manifest")
	args := []string{
		"--workdir", manifestDir,
		"--region-id", "99",
		"--start-key", "hex:6161",
		"--end-key", "zz",
		"--epoch-version", "7",
		"--epoch-conf-version", "5",
		"--peer", "1:1001",
		"--peer", "2:1002",
	}

	output, err := captureStdout(t, func() error {
		return runCatalog(args)
	})
	require.NoError(t, err)
	require.Contains(t, output, "stored region 99 in local peer catalog")

	metaStore, err := localmeta.OpenLocalStore(manifestDir, nil)
	require.NoError(t, err)
	defer func() {
		_ = metaStore.Close()
	}()

	regions := metaStore.Snapshot()
	meta, ok := regions[99]
	require.True(t, ok)
	require.Equal(t, []byte("aa"), meta.StartKey)
	require.Equal(t, []byte("zz"), meta.EndKey)
	require.Equal(t, uint64(7), meta.Epoch.Version)
	require.Equal(t, uint64(5), meta.Epoch.ConfVersion)
	require.Len(t, meta.Peers, 2)
	require.Equal(t, metaregion.Peer{StoreID: 1, PeerID: 1001}, meta.Peers[0])
}

func TestMainStoresCommand(t *testing.T) {
	cfgPath := writeSampleConfig(t)
	origArgs := os.Args
	defer func() { os.Args = origArgs }()
	os.Args = []string{
		"nokv-config",
		"stores",
		"--config",
		cfgPath,
		"--format",
		"json",
	}

	_, err := captureStdout(t, func() error {
		main()
		return nil
	})
	require.NoError(t, err)
}

func TestMainRegionCatalogCommand(t *testing.T) {
	dir := t.TempDir()
	manifestDir := filepath.Join(dir, "manifest")
	origArgs := os.Args
	defer func() { os.Args = origArgs }()
	os.Args = []string{
		"nokv-config",
		"catalog",
		"--workdir",
		manifestDir,
		"--region-id",
		"1",
		"--peer",
		"1:1",
	}
	code := captureExitCode(t, func() {
		main()
	})
	require.Equal(t, 0, code)
}

func TestMainRegionsCommand(t *testing.T) {
	cfgPath := writeSampleConfig(t)
	origArgs := os.Args
	defer func() { os.Args = origArgs }()
	os.Args = []string{
		"nokv-config",
		"regions",
		"--config",
		cfgPath,
		"--format",
		"json",
	}
	code := captureExitCode(t, func() {
		main()
	})
	require.Equal(t, 0, code)
}

func TestMainCoordinatorCommand(t *testing.T) {
	cfgPath := writeSampleConfig(t)
	origArgs := os.Args
	defer func() { os.Args = origArgs }()
	os.Args = []string{
		"nokv-config",
		"coordinator",
		"--config",
		cfgPath,
		"--format",
		"simple",
	}
	code := captureExitCode(t, func() {
		main()
	})
	require.Equal(t, 0, code)
}

func TestMainMetaRootCommand(t *testing.T) {
	cfgPath := writeSampleConfig(t)
	origArgs := os.Args
	defer func() { os.Args = origArgs }()
	os.Args = []string{
		"nokv-config",
		"meta-root",
		"--config",
		cfgPath,
		"--format",
		"simple",
	}
	code := captureExitCode(t, func() {
		main()
	})
	require.Equal(t, 0, code)
}

func TestMainMissingArgs(t *testing.T) {
	code := captureExitCode(t, func() {
		origArgs := os.Args
		defer func() { os.Args = origArgs }()
		os.Args = []string{"nokv-config"}
		main()
	})
	require.Equal(t, 1, code)
}

func TestMainUnknownCommand(t *testing.T) {
	code := captureExitCode(t, func() {
		origArgs := os.Args
		defer func() { os.Args = origArgs }()
		os.Args = []string{"nokv-config", "unknown"}
		main()
	})
	require.Equal(t, 1, code)
}

func TestMainCommandError(t *testing.T) {
	cfgPath := writeSampleConfig(t)
	origArgs := os.Args
	defer func() { os.Args = origArgs }()
	os.Args = []string{
		"nokv-config",
		"stores",
		"--config",
		cfgPath,
		"--format",
		"bad",
	}
	code := captureExitCode(t, func() {
		main()
	})
	require.Equal(t, 1, code)

	cfg := config.File{
		Stores: []config.Store{{StoreID: 0, Addr: "127.0.0.1:1"}},
	}
	dir := t.TempDir()
	raw, err := json.Marshal(cfg)
	require.NoError(t, err)
	path := filepath.Join(dir, "config.json")
	require.NoError(t, os.WriteFile(path, raw, 0o600))

	os.Args = []string{
		"nokv-config",
		"stores",
		"--config",
		path,
	}
	code = captureExitCode(t, func() {
		main()
	})
	require.Equal(t, 1, code)
}

func TestPrintUsage(t *testing.T) {
	output, err := captureStdout(t, func() error {
		printUsage()
		return nil
	})
	require.NoError(t, err)
	require.Contains(t, output, "Usage: nokv-config")
}

func TestDefaultConfigPathFallback(t *testing.T) {
	orig := getwd
	getwd = func() (string, error) {
		return "", errors.New("fail")
	}
	t.Cleanup(func() { getwd = orig })
	require.Equal(t, "raft_config.example.json", defaultConfigPath())
}

func TestDefaultConfigPathUsesCwd(t *testing.T) {
	orig := getwd
	getwd = func() (string, error) {
		return "/tmp", nil
	}
	t.Cleanup(func() { getwd = orig })
	require.Equal(t, filepath.Join("/tmp", "raft_config.example.json"), defaultConfigPath())
}

func TestRunStoresUnknownFormat(t *testing.T) {
	cfgPath := writeSampleConfig(t)
	err := runStores([]string{"--config", cfgPath, "--format", "oops"})
	require.Error(t, err)
}

func TestRunStoresLoadConfigError(t *testing.T) {
	cfg := config.File{
		Stores: []config.Store{{StoreID: 0, Addr: "bad"}},
	}
	dir := t.TempDir()
	raw, err := json.Marshal(cfg)
	require.NoError(t, err)
	path := filepath.Join(dir, "config.json")
	require.NoError(t, os.WriteFile(path, raw, 0o600))
	require.Error(t, runStores([]string{"--config", path}))
}

func TestRunRegionsUnknownFormat(t *testing.T) {
	cfgPath := writeSampleConfig(t)
	err := runRegions([]string{"--config", cfgPath, "--format", "oops"})
	require.Error(t, err)
}

func TestRunRegionsLoadConfigError(t *testing.T) {
	cfg := config.File{
		Stores: []config.Store{{StoreID: 0, Addr: "bad"}},
	}
	dir := t.TempDir()
	raw, err := json.Marshal(cfg)
	require.NoError(t, err)
	path := filepath.Join(dir, "config.json")
	require.NoError(t, os.WriteFile(path, raw, 0o600))
	require.Error(t, runRegions([]string{"--config", path}))
}

func TestRunPDMissingBlock(t *testing.T) {
	cfg := config.File{
		Stores: []config.Store{{StoreID: 1, Addr: "127.0.0.1:1"}},
	}
	dir := t.TempDir()
	raw, err := json.Marshal(cfg)
	require.NoError(t, err)
	path := filepath.Join(dir, "config.json")
	require.NoError(t, os.WriteFile(path, raw, 0o600))

	err = runCoordinator([]string{"--config", path})
	require.Error(t, err)
}

func TestRunPDUnknownFormatAndScope(t *testing.T) {
	cfgPath := writeSampleConfig(t)
	require.Error(t, runCoordinator([]string{"--config", cfgPath, "--format", "bad"}))
	require.Error(t, runCoordinator([]string{"--config", cfgPath, "--scope", "oops"}))
	require.Error(t, runCoordinator([]string{"--config", cfgPath, "--field", "bad"}))
}

func TestRunMetaRootUnknownFormatScopeAndMissingBlock(t *testing.T) {
	cfgPath := writeSampleConfig(t)
	require.Error(t, runMetaRoot([]string{"--config", cfgPath, "--format", "bad"}))
	require.Error(t, runMetaRoot([]string{"--config", cfgPath, "--scope", "oops"}))

	cfg := config.File{
		Stores: []config.Store{{StoreID: 1, Addr: "127.0.0.1:1"}},
	}
	dir := t.TempDir()
	raw, err := json.Marshal(cfg)
	require.NoError(t, err)
	path := filepath.Join(dir, "config.json")
	require.NoError(t, os.WriteFile(path, raw, 0o600))
	require.Error(t, runMetaRoot([]string{"--config", path}))
}

func TestLoadConfigMissingFile(t *testing.T) {
	_, err := loadConfig(filepath.Join(t.TempDir(), "missing.json"))
	require.Error(t, err)
}

func TestParsePeerErrors(t *testing.T) {
	_, _, err := parsePeer("bad")
	require.Error(t, err)
	_, _, err = parsePeer("1:bad")
	require.Error(t, err)
	_, _, err = parsePeer("1:")
	require.Error(t, err)
}

func TestParseUintErrors(t *testing.T) {
	_, err := parseUint("")
	require.Error(t, err)
	_, err = parseUint("nope")
	require.Error(t, err)
}

func TestParseRegionState(t *testing.T) {
	require.Equal(t, metaregion.ReplicaStateRunning, parseRegionState(""))
	require.Equal(t, metaregion.ReplicaStateTombstone, parseRegionState("tombstone"))
	require.Equal(t, metaregion.ReplicaStateRunning, parseRegionState("unknown"))
}

func TestDecodeKeyInvalidHexPanics(t *testing.T) {
	require.Panics(t, func() {
		_ = decodeKey("hex:zz")
	})
}

func TestMultiValueSetEmpty(t *testing.T) {
	var mv multiValue
	require.Error(t, mv.Set(" "))
}

func TestFirstNonEmptyAllBlank(t *testing.T) {
	require.Equal(t, "-", firstNonEmpty("", " ", "\t"))
}

func TestLoadConfigInvalid(t *testing.T) {
	cfg := config.File{
		Stores: []config.Store{
			{StoreID: 1, Addr: "a"},
			{StoreID: 1, Addr: "b"},
		},
	}
	dir := t.TempDir()
	raw, err := json.Marshal(cfg)
	require.NoError(t, err)
	path := filepath.Join(dir, "config.json")
	require.NoError(t, os.WriteFile(path, raw, 0o600))
	_, err = loadConfig(path)
	require.Error(t, err)
}

func TestRunRegionCatalogErrors(t *testing.T) {
	require.Error(t, runCatalog([]string{}))
	require.Error(t, runCatalog([]string{"--workdir", t.TempDir()}))
	require.Error(t, runCatalog([]string{"--workdir", t.TempDir(), "--region-id", "1"}))
	require.Error(t, runCatalog([]string{"--workdir", t.TempDir(), "--region-id", "1", "--peer", "bad"}))

	tmpFile := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(tmpFile, []byte("x"), 0o600))
	require.Error(t, runCatalog([]string{"--workdir", tmpFile, "--region-id", "1", "--peer", "1:1"}))
}

func TestRunRegionCatalogDefaults(t *testing.T) {
	dir := t.TempDir()
	args := []string{
		"--workdir", filepath.Join(dir, "manifest"),
		"--region-id", "10",
		"--start-key", "-",
		"--end-key", "hex:6162",
		"--state", "tombstone",
		"--peer", "1:11",
		"--peer", "2:22",
	}
	output, err := captureStdout(t, func() error {
		return runCatalog(args)
	})
	require.NoError(t, err)
	require.Contains(t, output, "stored region 10 in local peer catalog")
}

func captureExitCode(t *testing.T, fn func()) (code int) {
	t.Helper()
	origExit := exit
	defer func() { exit = origExit }()
	exit = func(code int) {
		panic(code)
	}
	defer func() {
		if r := recover(); r != nil {
			if c, ok := r.(int); ok {
				code = c
				return
			}
			panic(r)
		}
	}()
	fn()
	return code
}

func captureStdout(t *testing.T, fn func() error) (string, error) {
	t.Helper()
	orig := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w
	runErr := fn()
	if closeErr := w.Close(); closeErr != nil && runErr == nil {
		runErr = closeErr
	}
	os.Stdout = orig
	out, readErr := io.ReadAll(r)
	require.NoError(t, r.Close())
	if runErr == nil && readErr != nil {
		runErr = readErr
	}
	return string(out), runErr
}

func writeSampleConfig(t *testing.T) string {
	t.Helper()
	cfg := config.File{
		MaxRetries: 3,
		MetaRoot: &config.MetaRoot{Peers: []config.MetaRootPeer{
			{
				NodeID:              1,
				Addr:                "127.0.0.1:2380",
				DockerAddr:          "nokv-meta-root-1:2380",
				TransportAddr:       "127.0.0.1:3380",
				DockerTransportAddr: "nokv-meta-root-1:2480",
				WorkDir:             "./artifacts/cluster/meta-root-1",
				DockerWorkDir:       "/var/lib/nokv-meta-root-1",
			},
			{
				NodeID:              2,
				Addr:                "127.0.0.1:2381",
				DockerAddr:          "nokv-meta-root-2:2380",
				TransportAddr:       "127.0.0.1:3381",
				DockerTransportAddr: "nokv-meta-root-2:2480",
				WorkDir:             "./artifacts/cluster/meta-root-2",
				DockerWorkDir:       "/var/lib/nokv-meta-root-2",
			},
			{
				NodeID:              3,
				Addr:                "127.0.0.1:2382",
				DockerAddr:          "nokv-meta-root-3:2380",
				TransportAddr:       "127.0.0.1:3382",
				DockerTransportAddr: "nokv-meta-root-3:2480",
				WorkDir:             "./artifacts/cluster/meta-root-3",
				DockerWorkDir:       "/var/lib/nokv-meta-root-3",
			},
		}},
		Coordinator: &config.Coordinator{
			Addr:          "127.0.0.1:2379",
			DockerAddr:    "nokv-pd:2379",
			WorkDir:       "./artifacts/cluster/coordinator",
			DockerWorkDir: "/var/lib/nokv-coordinator",
		},
		StoreWorkDirTemplate:       "./artifacts/cluster/store-{id}",
		StoreDockerWorkDirTemplate: "/var/lib/nokv/store-{id}",
		Stores: []config.Store{
			{
				StoreID:          1,
				ListenAddr:       "127.0.0.1:10170",
				Addr:             "127.0.0.1:10170",
				DockerListenAddr: "10.0.0.1:20160",
				DockerAddr:       "store1-docker",
			},
			{
				StoreID:       2,
				ListenAddr:    "127.0.0.1:10171",
				Addr:          "127.0.0.1:10171",
				WorkDir:       "./custom/store-2",
				DockerWorkDir: "/var/lib/custom/store-2",
			},
		},
		Regions: []config.Region{
			{
				ID:       1,
				StartKey: "",
				EndKey:   "m",
				Epoch: config.RegionEpoch{
					Version:     1,
					ConfVersion: 1,
				},
				Peers: []config.Peer{
					{StoreID: 1, PeerID: 101},
					{StoreID: 2, PeerID: 201},
				},
				LeaderStoreID: 1,
			},
			{
				ID:       2,
				StartKey: "m",
				EndKey:   string([]byte{0x00, 0x01}),
				Epoch: config.RegionEpoch{
					Version:     2,
					ConfVersion: 3,
				},
				Peers: []config.Peer{
					{StoreID: 2, PeerID: 202},
				},
				LeaderStoreID: 2,
			},
		},
	}
	data, err := json.Marshal(cfg)
	require.NoError(t, err)

	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	require.NoError(t, os.WriteFile(path, data, 0o600))
	return path
}
