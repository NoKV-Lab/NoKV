// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/engine/manifest"
	"github.com/feichai0017/NoKV/engine/wal"
	local "github.com/feichai0017/NoKV/local"
	"github.com/feichai0017/NoKV/local/stats"
	workdirmode "github.com/feichai0017/NoKV/local/workdir"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	migratepkg "github.com/feichai0017/NoKV/raftstore/migrate"
	snapshotpkg "github.com/feichai0017/NoKV/raftstore/snapshot/sst"
	storepkg "github.com/feichai0017/NoKV/raftstore/store"
	"github.com/stretchr/testify/require"
)

func TestRunManifestCmd(t *testing.T) {
	dir := t.TempDir()
	opt := local.NewDefaultOptions()
	opt.WorkDir = dir
	db, err := local.Open(opt)
	require.NoError(t, err)
	if err := db.Set([]byte("cli-manifest"), []byte("value")); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	var buf bytes.Buffer
	if err := runManifestCmd(&buf, []string{"-workdir", dir, "-json"}); err != nil {
		t.Fatalf("runManifestCmd: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(buf.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := payload["levels"]; !ok {
		t.Fatalf("expected levels in manifest output")
	}
	levels, _ := payload["levels"].([]any)
	if len(levels) > 0 {
		if lvl, ok := levels[0].(map[string]any); ok {
			if _, ok := lvl["value_bytes"]; !ok {
				t.Fatalf("expected value_bytes in manifest level entry")
			}
		}
	}
}

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
	if snap.Entries == 0 {
		t.Fatalf("expected entry count > 0")
	}
	if len(snap.LSM.Levels) == 0 {
		t.Fatalf("expected LSM level metrics")
	}
	if snap.LSM.ValueBytesTotal < 0 {
		t.Fatalf("expected aggregated LSM value bytes to be non-negative")
	}
	if snap.Compaction.ValueWeight <= 0 {
		t.Fatalf("expected compaction value weight > 0")
	}
	if snap.LSM.ValueDensityMax < 0 {
		t.Fatalf("expected non-negative value density max")
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
	require.Greater(t, snap.Entries, int64(0))
}

func TestRenderStatsWarnLine(t *testing.T) {
	var buf bytes.Buffer
	snap := stats.StatsSnapshot{
		Entries: 1,
		WAL: stats.WALStatsSnapshot{
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
	if !strings.Contains(out, "WAL.ActiveSize") {
		t.Fatalf("expected WAL.ActiveSize line in output, got: %q", out)
	}
	if !strings.Contains(out, "Regions.Total") {
		t.Fatalf("expected Regions.Total line in output, got: %q", out)
	}
	if !strings.Contains(out, "Compaction.ValueWeight") {
		t.Fatalf("expected Compaction.ValueWeight line in output, got: %q", out)
	}
}

func TestRunManifestCmdPlain(t *testing.T) {
	dir := prepareDBWorkdir(t)
	var buf bytes.Buffer
	err := runManifestCmd(&buf, []string{"-workdir", dir})
	require.NoError(t, err)
	// The plain-text manifest output starts with "Levels:" and lists
	// any per-level files.
	require.Contains(t, buf.String(), "Levels:")
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
				"entries": float64(12),
				"hot": map[string]any{
					"write_keys": []any{
						map[string]any{"key": "k1", "count": float64(3)},
					},
				},
				"lsm": map[string]any{
					"levels": []any{
						map[string]any{"level": float64(0), "tables": float64(1)},
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
	require.Equal(t, int64(12), snap.Entries)
	require.Len(t, snap.Hot.WriteKeys, 1)
	require.Equal(t, "k1", snap.Hot.WriteKeys[0].Key)
	require.Len(t, snap.LSM.Levels, 1)
	require.Equal(t, 0, snap.LSM.Levels[0].Level)
}

func TestFetchExpvarSnapshotWithPath(t *testing.T) {
	handler := http.NewServeMux()
	handler.HandleFunc("/debug/vars", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"NoKV.Local.Stats": map[string]any{"entries": float64(2)},
		})
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	snap, err := fetchExpvarSnapshot(server.URL + "/debug/vars")
	require.NoError(t, err)
	require.Equal(t, int64(2), snap.Entries)
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

	files := []manifest.FileMeta{
		{FileID: 1, Size: 10, ValueSize: 5},
		{FileID: 2, Size: 20, ValueSize: 7},
	}
	require.Equal(t, []uint64{1, 2}, fileIDs(files))
	require.Equal(t, uint64(30), totalSize(files))
	require.Equal(t, uint64(12), totalValue(files))
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

func TestEnsureManifestExists(t *testing.T) {
	dir := t.TempDir()
	if err := ensureManifestExists(dir); err == nil {
		t.Fatalf("expected missing manifest error")
	}

	path := filepath.Join(dir, "CURRENT")
	if err := os.WriteFile(path, []byte("MANIFEST-000001"), 0o644); err != nil {
		t.Fatalf("write CURRENT: %v", err)
	}
	if err := ensureManifestExists(dir); err != nil {
		t.Fatalf("expected manifest to exist: %v", err)
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

func TestMainManifestCommand(t *testing.T) {
	dir := prepareDBWorkdir(t)
	code := captureExitCode(t, func() {
		oldArgs := os.Args
		os.Args = []string{"nokv", "manifest", "-workdir", dir}
		defer func() { os.Args = oldArgs }()
		main()
	})
	require.Equal(t, 0, code)
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

func TestMainMigratePlanCommand(t *testing.T) {
	dir := prepareDBWorkdir(t)
	code := captureExitCode(t, func() {
		oldArgs := os.Args
		os.Args = []string{"nokv", "migrate", "plan", "-workdir", dir}
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
				"entries": float64(9),
			},
		}
		_ = json.NewEncoder(w).Encode(payload)
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	var buf bytes.Buffer
	err := runStatsCmd(&buf, []string{"-expvar", server.URL})
	require.NoError(t, err)
	require.Contains(t, buf.String(), "Entries")
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
			"NoKV.Local.Stats": map[string]any{"entries": float64(1)},
		})
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	url := strings.TrimPrefix(server.URL, "http://") + "/"
	snap, err := fetchExpvarSnapshot(url)
	require.NoError(t, err)
	require.Equal(t, int64(1), snap.Entries)
}

func TestParseExpvarSnapshotFull(t *testing.T) {
	data := map[string]any{
		"NoKV.Local.Stats": map[string]any{
			"entries": float64(11),
			"flush": map[string]any{
				"pending": float64(2),
			},
			"compaction": map[string]any{
				"max_score":              float64(1.5),
				"value_weight":           float64(2.0),
				"value_weight_suggested": float64(2.4),
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
			"lsm": map[string]any{
				"value_bytes_total":   float64(10),
				"value_density_max":   float64(3.5),
				"value_density_alert": true,
				"levels": []any{
					map[string]any{
						"level":               float64(0),
						"tables":              float64(1),
						"size_bytes":          float64(10),
						"value_bytes":         float64(5),
						"stale_bytes":         float64(2),
						"landing_tables":      float64(1),
						"landing_size_bytes":  float64(3),
						"landing_value_bytes": float64(4),
					},
				},
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
	require.Equal(t, int64(11), snap.Entries)
	require.Equal(t, uint64(4), snap.Write.HotKeyLimited)
	require.True(t, snap.LSM.ValueDensityAlert)
	require.Len(t, snap.Hot.WriteKeys, 1)
	require.Len(t, snap.LSM.Levels, 1)
}

func TestRenderStatsFull(t *testing.T) {
	var buf bytes.Buffer
	snap := stats.StatsSnapshot{
		Entries: 1,
		Flush: stats.FlushStatsSnapshot{
			Pending:       2,
			LastWaitMs:    1,
			MaxWaitMs:     2,
			LastBuildMs:   3,
			MaxBuildMs:    4,
			LastReleaseMs: 5,
			MaxReleaseMs:  6,
		},
		Compaction: stats.CompactionStatsSnapshot{
			Backlog:              3,
			MaxScore:             4.5,
			LastDurationMs:       1.2,
			MaxDurationMs:        2.3,
			Runs:                 1,
			ValueWeight:          1.0,
			ValueWeightSuggested: 2.0,
		},
		Write: stats.WriteStatsSnapshot{
			HotKeyLimited: 2,
		},
		LSM: stats.LSMStatsSnapshot{
			ValueDensityMax:   1.5,
			ValueDensityAlert: true,
			ValueBytesTotal:   10,
			Levels: []stats.LSMLevelStats{{
				Level:             0,
				TableCount:        1,
				SizeBytes:         2,
				ValueBytes:        3,
				StaleBytes:        4,
				LandingTables:     1,
				LandingSizeBytes:  2,
				LandingValueBytes: 3,
			}},
		},
		WAL: stats.WALStatsSnapshot{
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
	require.Contains(t, out, "LSM.Levels:")
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

func TestRunManifestCmdMissingWorkdir(t *testing.T) {
	var buf bytes.Buffer
	err := runManifestCmd(&buf, nil)
	require.Error(t, err)
}

func TestRunManifestCmdMissingManifest(t *testing.T) {
	var buf bytes.Buffer
	err := runManifestCmd(&buf, []string{"-workdir", t.TempDir()})
	require.Error(t, err)
}

func TestRunMigratePlanCmd(t *testing.T) {
	dir := prepareDBWorkdir(t)
	var buf bytes.Buffer
	err := runMigratePlanCmd(&buf, []string{"-workdir", dir, "-json"})
	require.NoError(t, err)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &payload))
	require.Equal(t, true, payload["eligible"])
	require.Equal(t, "standalone", payload["mode"])
}

func TestRunMigratePlanCmdBlocksNonEmptyCatalog(t *testing.T) {
	dir := prepareDBWorkdir(t)
	metaStore, err := localmeta.OpenLocalStore(dir, nil)
	require.NoError(t, err)
	require.NoError(t, metaStore.SaveRegion(localmeta.RegionMeta{
		ID:       1,
		State:    metaregion.ReplicaStateRunning,
		StartKey: []byte(""),
		EndKey:   nil,
		Epoch:    metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:    []metaregion.Peer{{StoreID: 1, PeerID: 10}},
	}))
	require.NoError(t, metaStore.Close())

	var buf bytes.Buffer
	err = runMigratePlanCmd(&buf, []string{"-workdir", dir, "-json"})
	require.NoError(t, err)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &payload))
	require.Equal(t, false, payload["eligible"])
	require.Equal(t, float64(1), payload["local_catalog_regions"])
}

func TestRunMigratePlanCmdBlocksModeFile(t *testing.T) {
	dir := prepareDBWorkdir(t)
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, workdirmode.FileName),
		[]byte(`{"mode":"seeded","store_id":1,"region_id":1,"peer_id":10}`),
		0o644,
	))

	var buf bytes.Buffer
	err := runMigratePlanCmd(&buf, []string{"-workdir", dir, "-json"})
	require.NoError(t, err)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &payload))
	require.Equal(t, false, payload["eligible"])
	require.Equal(t, "seeded", payload["mode"])
}

func TestRunMigrateCmdMissingSubcommand(t *testing.T) {
	var buf bytes.Buffer
	require.Error(t, runMigrateCmd(&buf, nil))
}

func TestRunMigrateCmdUnknownSubcommand(t *testing.T) {
	var buf bytes.Buffer
	require.Error(t, runMigrateCmd(&buf, []string{"nope"}))
}

func TestRunMigrateStatusCmdStandalone(t *testing.T) {
	dir := prepareDBWorkdir(t)
	var buf bytes.Buffer
	err := runMigrateStatusCmd(&buf, []string{"-workdir", dir, "-json"})
	require.NoError(t, err)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &payload))
	require.Equal(t, "standalone", payload["mode"])
}

func TestRunMigrateStatusCmdSeeded(t *testing.T) {
	dir := prepareDBWorkdir(t)
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, workdirmode.FileName),
		[]byte(`{"mode":"seeded","store_id":1,"region_id":2,"peer_id":3}`),
		0o644,
	))

	var buf bytes.Buffer
	err := runMigrateStatusCmd(&buf, []string{"-workdir", dir, "-json"})
	require.NoError(t, err)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &payload))
	require.Equal(t, "seeded", payload["mode"])
	require.Equal(t, float64(1), payload["store_id"])
	require.Equal(t, float64(2), payload["region_id"])
	require.Equal(t, float64(3), payload["peer_id"])
}

func TestRunMigrateInitCmd(t *testing.T) {
	dir := prepareDBWorkdir(t)

	var buf bytes.Buffer
	err := runMigrateInitCmd(&buf, []string{
		"-workdir", dir,
		"-store", "1",
		"-region", "2",
		"-peer", "3",
		"-json",
	})
	require.NoError(t, err)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &payload))
	require.Equal(t, "seeded", payload["mode"])
	require.Equal(t, float64(1), payload["store_id"])
	require.Equal(t, float64(2), payload["region_id"])
	require.Equal(t, float64(3), payload["peer_id"])

	status, err := migratepkg.ReadStatus(dir)
	require.NoError(t, err)
	require.Equal(t, workdirmode.ModeSeeded, status.Mode)
	require.Equal(t, 1, status.LocalCatalogRegions)
	require.True(t, status.SeedSnapshotPresent)
	require.Contains(t, status.Next, "nokv serve")

	metaStore, err := localmeta.OpenLocalStore(dir, nil)
	require.NoError(t, err)
	defer func() { _ = metaStore.Close() }()
	snapshot := metaStore.Snapshot()
	require.Len(t, snapshot, 1)
	meta := snapshot[2]
	require.Equal(t, uint64(2), meta.ID)
	require.Len(t, meta.Peers, 1)
	require.Equal(t, uint64(1), meta.Peers[0].StoreID)
	require.Equal(t, uint64(3), meta.Peers[0].PeerID)

	ptr, ok := metaStore.RaftPointer(2)
	require.True(t, ok)
	require.Equal(t, uint64(1), ptr.SnapshotIndex)
	require.Equal(t, uint64(1), ptr.SnapshotTerm)
	require.Equal(t, uint64(1), ptr.Committed)

	snapshotMeta, err := snapshotpkg.ReadMeta(status.SeedSnapshotDir, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(2), snapshotMeta.Region.ID)
	require.Greater(t, snapshotMeta.EntryCount, uint64(0))
}

func TestRunMigrateStatusCmdSeededJSONIncludesOperationalHints(t *testing.T) {
	dir := prepareDBWorkdir(t)
	require.NoError(t, runMigrateInitCmd(&bytes.Buffer{}, []string{
		"-workdir", dir,
		"-store", "1",
		"-region", "9",
		"-peer", "109",
	}))

	var buf bytes.Buffer
	err := runMigrateStatusCmd(&buf, []string{"-workdir", dir, "-json"})
	require.NoError(t, err)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &payload))
	require.Equal(t, "seeded", payload["mode"])
	require.Equal(t, float64(1), payload["store_id"])
	require.Equal(t, float64(9), payload["region_id"])
	require.Equal(t, float64(109), payload["peer_id"])
	require.Equal(t, float64(1), payload["local_catalog_regions"])
	require.Equal(t, true, payload["seed_snapshot_present"])
	require.Contains(t, payload["seed_snapshot_dir"], "RAFTSTORE_SNAPSHOTS")
	checkpoint := payload["checkpoint"].(map[string]any)
	require.Equal(t, string(migratepkg.CheckpointSeededFinalized), checkpoint["stage"])
	require.Contains(t, payload["resume_hint"], "promotion already completed")
	require.Contains(t, payload["next"], "nokv serve")
}

func TestRunMigrateReportCmdStandalone(t *testing.T) {
	dir := prepareDBWorkdir(t)

	var buf bytes.Buffer
	err := runMigrateReportCmd(&buf, []string{"-workdir", dir, "-json"})
	require.NoError(t, err)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &payload))
	require.Equal(t, "standalone", payload["mode"])
	require.Equal(t, "standalone-ready", payload["stage"])
	require.Equal(t, true, payload["ready_for_init"])
	require.Equal(t, false, payload["ready_for_serve"])
	nextSteps := payload["next_steps"].([]any)
	require.NotEmpty(t, nextSteps)
	require.Contains(t, nextSteps[0].(string), "nokv migrate init")
}

func TestRunMigrateReportCmdSeeded(t *testing.T) {
	dir := prepareDBWorkdir(t)
	require.NoError(t, runMigrateInitCmd(&bytes.Buffer{}, []string{
		"-workdir", dir,
		"-store", "1",
		"-region", "9",
		"-peer", "109",
	}))

	var buf bytes.Buffer
	err := runMigrateReportCmd(&buf, []string{"-workdir", dir, "-json"})
	require.NoError(t, err)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &payload))
	require.Equal(t, "seeded", payload["mode"])
	require.Equal(t, "seed-ready", payload["stage"])
	require.Equal(t, false, payload["ready_for_init"])
	require.Equal(t, true, payload["ready_for_serve"])
	require.Contains(t, payload["resume_hint"], "promotion already completed")
	require.Contains(t, payload["summary"], "promoted into a seed")
	status := payload["status"].(map[string]any)
	require.Equal(t, true, status["seed_snapshot_present"])
	nextSteps := payload["next_steps"].([]any)
	require.Len(t, nextSteps, 2)
	require.Contains(t, nextSteps[0].(string), "nokv serve")
	require.Contains(t, nextSteps[1].(string), "nokv migrate expand")
}

func TestRunMigrateStatusCmdPassesRemoteViewFlags(t *testing.T) {
	orig := runReadStatus
	runReadStatus = func(cfg migratepkg.StatusConfig) (migratepkg.StatusResult, error) {
		require.Equal(t, "/tmp/store-1", cfg.WorkDir)
		require.Equal(t, "127.0.0.1:20170", cfg.AdminAddr)
		require.Equal(t, uint64(9), cfg.RegionID)
		require.Equal(t, 2*time.Second, cfg.Timeout)
		return migratepkg.StatusResult{
			WorkDir: "/tmp/store-1",
			Mode:    workdirmode.ModeCluster,
			Runtime: &migratepkg.RuntimeStatus{
				Addr:            cfg.AdminAddr,
				RegionID:        cfg.RegionID,
				Known:           true,
				Hosted:          true,
				Leader:          true,
				LeaderPeerID:    201,
				LocalPeerID:     201,
				MembershipPeers: 3,
				AppliedIndex:    7,
				AppliedTerm:     1,
			},
		}, nil
	}
	t.Cleanup(func() { runReadStatus = orig })

	var buf bytes.Buffer
	err := runMigrateStatusCmd(&buf, []string{
		"-workdir", "/tmp/store-1",
		"-addr", "127.0.0.1:20170",
		"-region", "9",
		"-timeout", "2s",
	})
	require.NoError(t, err)
	require.Contains(t, buf.String(), "Runtime")
	require.Contains(t, buf.String(), "Peers    3")
}

func TestRunMigrateReportCmdPassesRemoteViewFlags(t *testing.T) {
	orig := runBuildReport
	runBuildReport = func(cfg migratepkg.StatusConfig) (migratepkg.ReportResult, error) {
		require.Equal(t, "/tmp/store-1", cfg.WorkDir)
		require.Equal(t, "127.0.0.1:20170", cfg.AdminAddr)
		require.Equal(t, uint64(9), cfg.RegionID)
		require.Equal(t, 2*time.Second, cfg.Timeout)
		return migratepkg.ReportResult{
			WorkDir:       cfg.WorkDir,
			Mode:          workdirmode.ModeCluster,
			Stage:         "cluster-active",
			Summary:       "cluster is active",
			ReadyForInit:  false,
			ReadyForServe: false,
			NextSteps:     []string{"nokv migrate expand"},
			Status: migratepkg.StatusResult{
				WorkDir: cfg.WorkDir,
				Mode:    workdirmode.ModeCluster,
				Runtime: &migratepkg.RuntimeStatus{
					Addr:            cfg.AdminAddr,
					RegionID:        cfg.RegionID,
					Known:           true,
					Hosted:          true,
					Leader:          true,
					LeaderPeerID:    201,
					LocalPeerID:     201,
					MembershipPeers: 3,
					AppliedIndex:    7,
					AppliedTerm:     1,
				},
			},
			Cluster: &migratepkg.ClusterSummary{
				Source:          "single-admin-endpoint",
				AdminAddr:       cfg.AdminAddr,
				RegionID:        cfg.RegionID,
				Known:           true,
				Hosted:          true,
				Leader:          true,
				LeaderStoreID:   2,
				LeaderPeerID:    201,
				LocalPeerID:     201,
				MembershipPeers: 3,
				Membership: []migratepkg.MembershipPeerSummary{
					{StoreID: 1, PeerID: 101},
					{StoreID: 2, PeerID: 201},
					{StoreID: 3, PeerID: 301},
				},
				AppliedIndex: 7,
				AppliedTerm:  1,
			},
		}, nil
	}
	t.Cleanup(func() { runBuildReport = orig })

	var buf bytes.Buffer
	err := runMigrateReportCmd(&buf, []string{
		"-workdir", "/tmp/store-1",
		"-addr", "127.0.0.1:20170",
		"-region", "9",
		"-timeout", "2s",
	})
	require.NoError(t, err)
	require.Contains(t, buf.String(), "Cluster")
	require.Contains(t, buf.String(), "leader_store=2")
	require.Contains(t, buf.String(), "store=3 peer=301")
	require.Contains(t, buf.String(), "Runtime")
	require.Contains(t, buf.String(), "leader_peer=201")
}

func TestRunMigrateInitCmdIdempotentForSeededWorkdir(t *testing.T) {
	dir := prepareDBWorkdir(t)
	var buf bytes.Buffer
	require.NoError(t, runMigrateInitCmd(&buf, []string{
		"-workdir", dir,
		"-store", "1",
		"-region", "2",
		"-peer", "3",
		"-json",
	}))
	buf.Reset()
	require.NoError(t, runMigrateInitCmd(&buf, []string{
		"-workdir", dir,
		"-store", "1",
		"-region", "2",
		"-peer", "3",
		"-json",
	}))
	var payload map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &payload))
	require.Equal(t, "seeded", payload["mode"])
}

func TestRunMigrateExpandCmd(t *testing.T) {
	origExpand := runExpand
	runExpand = func(ctx context.Context, cfg migratepkg.ExpandConfig) (migratepkg.ExpandResultSet, error) {
		require.Equal(t, "/tmp/store-1", cfg.WorkDir)
		require.Equal(t, "127.0.0.1:20160", cfg.Addr)
		require.Equal(t, uint64(9), cfg.RegionID)
		require.Equal(t, 5*time.Second, cfg.WaitTimeout)
		require.Equal(t, 100*time.Millisecond, cfg.PollInterval)
		require.Len(t, cfg.Targets, 1)
		require.Equal(t, migratepkg.PeerTarget{StoreID: 2, PeerID: 22, TargetAdminAddr: "127.0.0.1:20161"}, cfg.Targets[0])
		return migratepkg.ExpandResultSet{
			Addr:     cfg.Addr,
			RegionID: cfg.RegionID,
			Results: []migratepkg.ExpandResult{
				{
					StoreID:           2,
					PeerID:            22,
					LeaderKnown:       true,
					TargetKnown:       true,
					TargetHosted:      true,
					TargetLocalPeerID: 22,
				},
			},
		}, nil
	}
	t.Cleanup(func() {
		runExpand = origExpand
	})

	var buf bytes.Buffer
	err := runMigrateExpandCmd(&buf, []string{
		"-workdir", "/tmp/store-1",
		"-addr", "127.0.0.1:20160",
		"-region", "9",
		"-target", "2:22@127.0.0.1:20161",
		"-wait", "5s",
		"-poll-interval", "100ms",
		"-json",
	})
	require.NoError(t, err)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &payload))
	results := payload["results"].([]any)
	require.Len(t, results, 1)
	first := results[0].(map[string]any)
	require.Equal(t, true, first["leader_known"])
	require.Equal(t, true, first["target_hosted"])
	require.Equal(t, float64(22), first["target_local_peer_id"])
}

func TestRunMigrateExpandCmdMultiTarget(t *testing.T) {
	origExpand := runExpand
	runExpand = func(ctx context.Context, cfg migratepkg.ExpandConfig) (migratepkg.ExpandResultSet, error) {
		require.Equal(t, "/tmp/store-1", cfg.WorkDir)
		require.Equal(t, "127.0.0.1:20160", cfg.Addr)
		require.Equal(t, uint64(9), cfg.RegionID)
		require.Len(t, cfg.Targets, 2)
		require.Equal(t, migratepkg.PeerTarget{StoreID: 2, PeerID: 22, TargetAdminAddr: "127.0.0.1:20161"}, cfg.Targets[0])
		require.Equal(t, migratepkg.PeerTarget{StoreID: 3, PeerID: 33, TargetAdminAddr: "127.0.0.1:20162"}, cfg.Targets[1])
		return migratepkg.ExpandResultSet{
			Addr:     cfg.Addr,
			RegionID: cfg.RegionID,
			Results: []migratepkg.ExpandResult{
				{StoreID: 2, PeerID: 22, TargetHosted: true, TargetAppliedIdx: 1},
				{StoreID: 3, PeerID: 33, TargetHosted: true, TargetAppliedIdx: 1},
			},
		}, nil
	}
	t.Cleanup(func() {
		runExpand = origExpand
	})

	var buf bytes.Buffer
	err := runMigrateExpandCmd(&buf, []string{
		"-workdir", "/tmp/store-1",
		"-addr", "127.0.0.1:20160",
		"-region", "9",
		"-target", "2:22@127.0.0.1:20161",
		"-target", "3:33@127.0.0.1:20162",
		"-json",
	})
	require.NoError(t, err)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &payload))
	results := payload["results"].([]any)
	require.Len(t, results, 2)
}

func TestRunMigrateRemovePeerCmd(t *testing.T) {
	orig := runRemovePeer
	runRemovePeer = func(ctx context.Context, cfg migratepkg.RemovePeerConfig) (migratepkg.RemovePeerResult, error) {
		require.Equal(t, "/tmp/store-1", cfg.WorkDir)
		require.Equal(t, "127.0.0.1:20160", cfg.Addr)
		require.Equal(t, "127.0.0.1:20161", cfg.TargetAdminAddr)
		require.Equal(t, uint64(9), cfg.RegionID)
		require.Equal(t, uint64(22), cfg.PeerID)
		return migratepkg.RemovePeerResult{
			Addr:            cfg.Addr,
			TargetAdminAddr: cfg.TargetAdminAddr,
			RegionID:        cfg.RegionID,
			PeerID:          cfg.PeerID,
			LeaderKnown:     true,
			TargetKnown:     false,
			TargetHosted:    false,
		}, nil
	}
	t.Cleanup(func() { runRemovePeer = orig })

	var buf bytes.Buffer
	err := runMigrateRemovePeerCmd(&buf, []string{
		"-workdir", "/tmp/store-1",
		"-addr", "127.0.0.1:20160",
		"-target-addr", "127.0.0.1:20161",
		"-region", "9",
		"-peer", "22",
		"-json",
	})
	require.NoError(t, err)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &payload))
	require.Equal(t, true, payload["leader_known"])
	require.Equal(t, false, payload["target_hosted"])
}

func TestRunMigrateTransferLeaderCmd(t *testing.T) {
	orig := runTransferLeader
	runTransferLeader = func(ctx context.Context, cfg migratepkg.TransferLeaderConfig) (migratepkg.TransferLeaderResult, error) {
		require.Equal(t, "/tmp/store-1", cfg.WorkDir)
		require.Equal(t, "127.0.0.1:20160", cfg.Addr)
		require.Equal(t, "127.0.0.1:20161", cfg.TargetAdminAddr)
		require.Equal(t, uint64(9), cfg.RegionID)
		require.Equal(t, uint64(22), cfg.PeerID)
		return migratepkg.TransferLeaderResult{
			Addr:            cfg.Addr,
			TargetAdminAddr: cfg.TargetAdminAddr,
			RegionID:        cfg.RegionID,
			PeerID:          cfg.PeerID,
			LeaderKnown:     true,
			LeaderPeerID:    cfg.PeerID,
			TargetLeader:    true,
		}, nil
	}
	t.Cleanup(func() { runTransferLeader = orig })

	var buf bytes.Buffer
	err := runMigrateTransferLeaderCmd(&buf, []string{
		"-workdir", "/tmp/store-1",
		"-addr", "127.0.0.1:20160",
		"-target-addr", "127.0.0.1:20161",
		"-region", "9",
		"-peer", "22",
		"-json",
	})
	require.NoError(t, err)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &payload))
	require.Equal(t, true, payload["target_leader"])
	require.Equal(t, float64(22), payload["leader_peer_id"])
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

func TestEnsureManifestExistsStatError(t *testing.T) {
	origStat := stat
	stat = func(string) (os.FileInfo, error) {
		return nil, errors.New("boom")
	}
	t.Cleanup(func() { stat = origStat })
	require.Error(t, ensureManifestExists(t.TempDir()))
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
