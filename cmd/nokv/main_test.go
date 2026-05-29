// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/json"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	local "github.com/feichai0017/NoKV/local"
	"github.com/feichai0017/NoKV/local/stats"
	workdirmode "github.com/feichai0017/NoKV/local/workdir"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	storepkg "github.com/feichai0017/NoKV/raftstore/store"
	"github.com/feichai0017/NoKV/storage/wal"
	"github.com/stretchr/testify/require"
)

func TestRunStatsCmd(t *testing.T) {
	dir := t.TempDir()
	opt := local.NewDefaultOptions()
	opt.WorkDir = dir
	db, err := local.Open(opt)
	require.NoError(t, err)
	if err := db.Set([]byte("cli-stats"), []byte("value")); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	var buf bytes.Buffer
	if err := runStatsCmd(&buf, []string{"-workdir", dir, "-json"}); err != nil {
		t.Fatalf("runStatsCmd: %v", err)
	}
	var snap stats.StatsSnapshot
	if err := json.Unmarshal(buf.Bytes(), &snap); err != nil {
		t.Fatalf("unmarshal snapshot: %v", err)
	}
	if snap.Storage.SizeBytes == 0 {
		t.Fatalf("expected storage size > 0")
	}
}

func TestLocalStatsSnapshotAllowsSeededWorkdir(t *testing.T) {
	dir := prepareDBWorkdir(t)
	require.NoError(t, workdirmode.Write(dir, workdirmode.State{
		Mode:     workdirmode.ModeSeeded,
		StoreID:  1,
		RegionID: 2,
		PeerID:   3,
	}))

	snap, err := localStatsSnapshot(dir, false)
	require.NoError(t, err)
	require.NotNil(t, snap)
}

func TestRenderStatsWarnLine(t *testing.T) {
	var buf bytes.Buffer
	snap := stats.StatsSnapshot{
		Storage: stats.StorageStatsSnapshot{KeysEstimate: 1},
		ControlWAL: stats.ControlWALStatsSnapshot{
			ActiveSegment:   7,
			SegmentCount:    3,
			SegmentsRemoved: 1,
			ActiveSize:      4096,
		},
		Raft: stats.RaftStatsSnapshot{
			GroupCount:       2,
			LaggingGroups:    1,
			MaxLagSegments:   5,
			LagWarnThreshold: 3,
			LagWarning:       true,
		},
	}
	if err := renderStats(&buf, snap, false); err != nil {
		t.Fatalf("renderStats: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "Raft.Warning") {
		t.Fatalf("expected Raft.Warning line in output, got: %q", out)
	}
	if !strings.Contains(out, "ControlWAL.ActiveSize") {
		t.Fatalf("expected ControlWAL.ActiveSize line in output, got: %q", out)
	}
	if !strings.Contains(out, "Regions.Total") {
		t.Fatalf("expected Regions.Total line in output, got: %q", out)
	}
	if !strings.Contains(out, "Storage.SizeBytes") {
		t.Fatalf("expected Storage.SizeBytes line in output, got: %q", out)
	}
}
func TestRunRegionsCmd(t *testing.T) {
	dir := t.TempDir()
	opt := local.NewDefaultOptions()
	opt.WorkDir = dir
	db, err := local.Open(opt)
	require.NoError(t, err)
	if err := db.Set([]byte("cli-region"), []byte("value")); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	var buf bytes.Buffer
	if err := runRegionsCmd(&buf, []string{"-workdir", dir, "-json"}); err != nil {
		t.Fatalf("runRegionsCmd: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(buf.Bytes(), &payload); err != nil {
		t.Fatalf("decode regions output: %v", err)
	}
	regions, ok := payload["regions"].([]any)
	if !ok {
		t.Fatalf("expected regions array in output: %v", payload)
	}
	_ = len(regions)
}

func TestFetchExpvarSnapshot(t *testing.T) {
	handler := http.NewServeMux()
	handler.HandleFunc("/debug/vars", func(w http.ResponseWriter, r *http.Request) {
		payload := map[string]any{
			"NoKV.Local.Stats": map[string]any{
				"storage": map[string]any{
					"keys_estimate": float64(12),
				},
				"hot": map[string]any{
					"write_keys": []any{
						map[string]any{"key": "k1", "count": float64(3)},
					},
				},
			},
		}
		_ = json.NewEncoder(w).Encode(payload)
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	url := strings.TrimPrefix(server.URL, "http://")
	snap, err := fetchExpvarSnapshot(url)
	require.NoError(t, err)
	require.Equal(t, uint64(12), snap.Storage.KeysEstimate)
	require.Len(t, snap.Hot.WriteKeys, 1)
	require.Equal(t, "k1", snap.Hot.WriteKeys[0].Key)
}

func TestFetchExpvarSnapshotWithPath(t *testing.T) {
	handler := http.NewServeMux()
	handler.HandleFunc("/debug/vars", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"NoKV.Local.Stats": map[string]any{
				"storage": map[string]any{"keys_estimate": float64(2)},
			},
		})
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	snap, err := fetchExpvarSnapshot(server.URL + "/debug/vars")
	require.NoError(t, err)
	require.Equal(t, uint64(2), snap.Storage.KeysEstimate)
}

func TestParseExpvarSnapshotHotKeysList(t *testing.T) {
	snap := parseExpvarSnapshot(map[string]any{
		"hot": map[string]any{
			"write_keys": []any{
				map[string]any{"key": "k2", "count": float64(4)},
			},
		},
	})
	require.Len(t, snap.Hot.WriteKeys, 1)
	require.Equal(t, "k2", snap.Hot.WriteKeys[0].Key)
	require.Equal(t, int32(4), snap.Hot.WriteKeys[0].Count)
}

func TestParseExpvarSnapshotHotKeysMap(t *testing.T) {
	snap := parseExpvarSnapshot(map[string]any{
		"NoKV.Local.Stats": map[string]any{
			"hot": map[string]any{
				"write_keys": []any{
					map[string]any{"key": "k3", "count": float64(7)},
				},
			},
		},
	})
	require.Len(t, snap.Hot.WriteKeys, 1)
	require.Equal(t, "k3", snap.Hot.WriteKeys[0].Key)
	require.Equal(t, int32(7), snap.Hot.WriteKeys[0].Count)
}

func TestParseExpvarSnapshotHotKeysMapFloat(t *testing.T) {
	snap := parseExpvarSnapshot(map[string]any{
		"NoKV.Local.Stats": map[string]any{
			"hot": map[string]any{
				"write_keys": []any{
					map[string]any{"key": "k4", "count": float64(3)},
				},
			},
		},
	})
	require.Len(t, snap.Hot.WriteKeys, 1)
	require.Equal(t, "k4", snap.Hot.WriteKeys[0].Key)
	require.Equal(t, int32(3), snap.Hot.WriteKeys[0].Count)
}

func TestFormatHelpers(t *testing.T) {
	require.Equal(t, "new", formatRegionState(metaregion.ReplicaStateNew))
	require.Equal(t, "running", formatRegionState(metaregion.ReplicaStateRunning))
	require.Equal(t, "removing", formatRegionState(metaregion.ReplicaStateRemoving))
	require.Equal(t, "tombstone", formatRegionState(metaregion.ReplicaStateTombstone))
	require.Equal(t, "unknown(99)", formatRegionState(99))

	peers := []metaregion.Peer{{StoreID: 1, PeerID: 2}}
	require.Equal(t, "[{store:1 peer:2}]", formatPeers(peers))
	require.Equal(t, "[]", formatPeers(nil))

}

func TestPrintUsage(t *testing.T) {
	var buf bytes.Buffer
	printUsage(&buf)
	out := buf.String()
	if !strings.Contains(out, "Usage: nokv") {
		t.Fatalf("expected usage header, got %q", out)
	}
	if !strings.Contains(out, "serve") {
		t.Fatalf("expected serve command in usage, got %q", out)
	}
	if !strings.Contains(out, "meta-root") {
		t.Fatalf("expected meta-root command in usage, got %q", out)
	}
}
func TestFirstRegionMetricsNone(t *testing.T) {
	withStoreRegistry(t, func() {
		if got := firstRegionMetrics(); got != nil {
			t.Fatalf("expected nil region metrics")
		}
	})
}

func TestMainHelp(t *testing.T) {
	oldArgs := os.Args
	os.Args = []string{"nokv", "help"}
	defer func() { os.Args = oldArgs }()
	main()
}

func TestMainMissingArgs(t *testing.T) {
	code := captureExitCode(t, func() {
		oldArgs := os.Args
		os.Args = []string{"nokv"}
		defer func() { os.Args = oldArgs }()
		main()
	})
	require.Equal(t, 1, code)
}

func TestMainUnknownCommand(t *testing.T) {
	code := captureExitCode(t, func() {
		oldArgs := os.Args
		os.Args = []string{"nokv", "nope"}
		defer func() { os.Args = oldArgs }()
		main()
	})
	require.Equal(t, 1, code)
}

func TestMainStatsError(t *testing.T) {
	code := captureExitCode(t, func() {
		oldArgs := os.Args
		os.Args = []string{"nokv", "stats"}
		defer func() { os.Args = oldArgs }()
		main()
	})
	require.Equal(t, 1, code)
}
func TestMainRegionsCommand(t *testing.T) {
	dir := t.TempDir()
	metaStore, err := localmeta.OpenLocalStore(dir, nil)
	require.NoError(t, err)
	require.NoError(t, metaStore.SaveRegion(localmeta.RegionMeta{
		ID:       1,
		State:    metaregion.ReplicaStateRunning,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 10}},
	}))
	require.NoError(t, metaStore.Close())
	code := captureExitCode(t, func() {
		oldArgs := os.Args
		os.Args = []string{"nokv", "regions", "-workdir", dir}
		defer func() { os.Args = oldArgs }()
		main()
	})
	require.Equal(t, 0, code)
}
func TestMainServeCommand(t *testing.T) {
	origNotify := notifyContext
	notifyContext = func(parent context.Context, _ ...os.Signal) (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(parent)
		cancel()
		return ctx, cancel
	}
	t.Cleanup(func() { notifyContext = origNotify })

	dir := t.TempDir()
	coordAddr, stopCoordinator := startTestCoordinatorServer(t)
	defer stopCoordinator()
	code := captureExitCode(t, func() {
		oldArgs := os.Args
		os.Args = []string{"nokv", "serve", "-workdir", dir, "-store-id", "1", "-addr", "127.0.0.1:0", "-coordinator-addr", coordAddr}
		defer func() { os.Args = oldArgs }()
		main()
	})
	require.Equal(t, 0, code)
}

func TestRunStatsCmdMissingFlags(t *testing.T) {
	var buf bytes.Buffer
	err := runStatsCmd(&buf, nil)
	require.Error(t, err)
}

func TestRunStatsCmdParseError(t *testing.T) {
	var buf bytes.Buffer
	err := runStatsCmd(&buf, []string{"-bad-flag"})
	require.Error(t, err)
}

func TestRunStatsCmdNoRegionMetrics(t *testing.T) {
	dir := prepareDBWorkdir(t)
	var buf bytes.Buffer
	err := runStatsCmd(&buf, []string{"-workdir", dir, "-no-region-metrics", "-json"})
	require.NoError(t, err)
}

func TestRunStatsCmdExpvarPlain(t *testing.T) {
	handler := http.NewServeMux()
	handler.HandleFunc("/debug/vars", func(w http.ResponseWriter, r *http.Request) {
		payload := map[string]any{
			"NoKV.Local.Stats": map[string]any{
				"storage": map[string]any{"keys_estimate": float64(9)},
			},
		}
		_ = json.NewEncoder(w).Encode(payload)
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	var buf bytes.Buffer
	err := runStatsCmd(&buf, []string{"-expvar", server.URL})
	require.NoError(t, err)
	require.Contains(t, buf.String(), "Storage.KeysEstimate")
}

func TestFetchExpvarSnapshotBadStatus(t *testing.T) {
	handler := http.NewServeMux()
	handler.HandleFunc("/debug/vars", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusInternalServerError)
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	_, err := fetchExpvarSnapshot(server.URL)
	require.Error(t, err)
}

func TestFetchExpvarSnapshotBadJSON(t *testing.T) {
	handler := http.NewServeMux()
	handler.HandleFunc("/debug/vars", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("{bad-json"))
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	_, err := fetchExpvarSnapshot(server.URL)
	require.Error(t, err)
}

func TestFetchExpvarSnapshotTrailingSlash(t *testing.T) {
	handler := http.NewServeMux()
	handler.HandleFunc("/debug/vars", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"NoKV.Local.Stats": map[string]any{
				"storage": map[string]any{"keys_estimate": float64(1)},
			},
		})
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	url := strings.TrimPrefix(server.URL, "http://") + "/"
	snap, err := fetchExpvarSnapshot(url)
	require.NoError(t, err)
	require.Equal(t, uint64(1), snap.Storage.KeysEstimate)
}

func TestParseExpvarSnapshotFull(t *testing.T) {
	data := map[string]any{
		"NoKV.Local.Stats": map[string]any{
			"storage": map[string]any{
				"keys_estimate": float64(11),
				"size_bytes":    float64(4096),
			},
			"write": map[string]any{
				"hot_key_limited": float64(4),
			},
			"raft": map[string]any{
				"group_count":      float64(2),
				"lagging_groups":   float64(1),
				"max_lag_segments": float64(5),
				"min_log_segment":  float64(1),
				"max_log_segment":  float64(9),
			},
			"region": map[string]any{
				"total":     float64(4),
				"new":       float64(1),
				"running":   float64(1),
				"removing":  float64(1),
				"tombstone": float64(1),
				"other":     float64(0),
			},
			"txn": map[string]any{
				"active":    float64(2),
				"started":   float64(3),
				"committed": float64(4),
				"conflicts": float64(5),
			},
			"hot": map[string]any{
				"write_keys": []any{
					map[string]any{"key": "hot", "count": float64(9)},
				},
			},
		},
	}
	snap := parseExpvarSnapshot(data)
	require.Equal(t, uint64(11), snap.Storage.KeysEstimate)
	require.Equal(t, uint64(4096), snap.Storage.SizeBytes)
	require.Equal(t, uint64(4), snap.Write.HotKeyLimited)
	require.Len(t, snap.Hot.WriteKeys, 1)
}

func TestRenderStatsFull(t *testing.T) {
	var buf bytes.Buffer
	snap := stats.StatsSnapshot{
		Storage: stats.StorageStatsSnapshot{
			KeysEstimate: 1,
			SizeBytes:    4096,
		},
		Write: stats.WriteStatsSnapshot{
			HotKeyLimited: 2,
		},
		ControlWAL: stats.ControlWALStatsSnapshot{
			ActiveSegment:           1,
			SegmentCount:            2,
			ActiveSize:              4096,
			SegmentsRemoved:         1,
			RecordCounts:            wal.RecordMetrics{Entries: 1},
			SegmentsWithRaftRecords: 1,
			RemovableRaftSegments:   1,
			TypedRecordRatio:        0.5,
			TypedRecordWarning:      true,
			TypedRecordReason:       "ratio low",
			AutoGCRuns:              1,
			AutoGCRemoved:           2,
			AutoGCLastUnix:          time.Now().Unix(),
		},
		Raft: stats.RaftStatsSnapshot{
			GroupCount:       1,
			LaggingGroups:    1,
			MaxLagSegments:   2,
			MinLogSegment:    1,
			MaxLogSegment:    2,
			LagWarnThreshold: 1,
			LagWarning:       true,
		},
		Region: stats.RegionStatsSnapshot{
			Total:     5,
			New:       1,
			Running:   1,
			Removing:  1,
			Tombstone: 1,
			Other:     1,
		},
		MVCCGC: stats.MVCCGCStatsSnapshot{
			Enabled:               true,
			Runs:                  2,
			LastDurationMs:        3,
			ActiveLocks:           1,
			OldestStartTs:         10,
			MaxStartTs:            20,
			ScannedKeys:           4,
			DroppableKeys:         1,
			WriteVersions:         6,
			DroppableWrites:       2,
			SafePointClampedKeys:  1,
			MaxVersionsPerKey:     3,
			MinEffectiveSafePoint: 10,
			MaxEffectiveSafePoint: 50,
			MaintenanceEnabled:    true,
			MaintenanceRuns:       4,
			ResolvedLocks:         2,
			CommittedLocks:        1,
			RolledBackLocks:       1,
			AppliedWriteDeletes:   5,
			AppliedDefaultDeletes: 6,
			OrphanDefaults:        7,
			AppliedOrphanDefaults: 7,
		},
		Hot: stats.HotStatsSnapshot{
			WriteKeys: []stats.HotKeyStat{{Key: "k", Count: 1}},
		},
	}
	require.NoError(t, renderStats(&buf, snap, false))
	out := buf.String()
	require.Contains(t, out, "MVCCGC.Plan")
	require.Contains(t, out, "MVCCGC.Candidates")
	require.Contains(t, out, "MVCCGC.Maintenance")
	require.Contains(t, out, "MVCCGC.ResolveLocks")
	require.Contains(t, out, "MVCCGC.Apply")
	require.Contains(t, out, "MVCCGC.OrphanDefaults")
	require.Contains(t, out, "Storage.SizeBytes")
	require.Contains(t, out, "WriteHotKeys:")
}

func TestLocalStatsSnapshotMissingWorkdir(t *testing.T) {
	_, err := localStatsSnapshot("", false)
	require.Error(t, err)
}

func TestRunRegionsCmdPlainNoRegions(t *testing.T) {
	var buf bytes.Buffer
	err := runRegionsCmd(&buf, []string{"-workdir", t.TempDir()})
	require.NoError(t, err)
	require.Contains(t, buf.String(), "Regions: (none)")
}

func TestRunRegionsCmdMissingWorkdir(t *testing.T) {
	var buf bytes.Buffer
	require.Error(t, runRegionsCmd(&buf, nil))
}

func TestRunRegionsCmdPlainWithRegion(t *testing.T) {
	dir := t.TempDir()
	metaStore, err := localmeta.OpenLocalStore(dir, nil)
	require.NoError(t, err)
	meta := localmeta.RegionMeta{
		ID:       10,
		State:    metaregion.ReplicaStateTombstone,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 10}},
	}
	require.NoError(t, metaStore.SaveRegion(meta))
	require.NoError(t, metaStore.Close())

	var buf bytes.Buffer
	err = runRegionsCmd(&buf, []string{"-workdir", dir})
	require.NoError(t, err)
	require.Contains(t, buf.String(), "tombstone")
}
func TestFirstRegionMetricsFound(t *testing.T) {
	withStoreRegistry(t, func() {
		store := storepkg.NewStore(storepkg.Config{})
		defer store.Close()
		registerRuntimeStore(store)
		defer unregisterRuntimeStore(store)
		require.NotNil(t, firstRegionMetrics())
	})
}

func TestLocalStatsSnapshotWithMetrics(t *testing.T) {
	withStoreRegistry(t, func() {
		store := storepkg.NewStore(storepkg.Config{})
		defer store.Close()
		registerRuntimeStore(store)
		defer unregisterRuntimeStore(store)
		dir := prepareDBWorkdir(t)
		_, err := localStatsSnapshot(dir, true)
		require.NoError(t, err)
	})
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

func withStoreRegistry(t *testing.T, fn func()) {
	t.Helper()
	original := runtimeStoreSnapshot()
	for _, st := range original {
		unregisterRuntimeStore(st)
	}
	defer func() {
		for _, st := range runtimeStoreSnapshot() {
			unregisterRuntimeStore(st)
		}
		for _, st := range original {
			registerRuntimeStore(st)
		}
	}()
	fn()
}

func prepareDBWorkdir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	opt := local.NewDefaultOptions()
	opt.WorkDir = dir
	db, err := local.Open(opt)
	require.NoError(t, err)
	require.NoError(t, db.Set([]byte("seed"), []byte("value")))
	require.NoError(t, db.Close())
	return dir
}
