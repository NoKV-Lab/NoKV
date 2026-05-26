// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package fsmetabench

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/benchmark/fsmeta/workload"
	coordclient "github.com/feichai0017/NoKV/coordinator/client"
	"github.com/feichai0017/NoKV/fsmeta"
	fsmetaclient "github.com/feichai0017/NoKV/fsmeta/client"
	"github.com/feichai0017/NoKV/fsmeta/model"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const benchEnvKey = "NOKV_FSMETA_BENCH"

var (
	fsmetaAddr            = flag.String("fsmeta_addr", "127.0.0.1:8090", "FSMetadata gRPC endpoint")
	fsmetaCoordAddr       = flag.String("fsmeta_coordinator_addr", "127.0.0.1:2379", "Coordinator gRPC endpoint for mount bootstrap")
	fsmetaWorkloads       = flag.String("fsmeta_workloads", workload.DefaultWorkloadSuite, "comma-separated workloads: "+workload.DefaultWorkloadSuite)
	fsmetaScaleProfile    = flag.String("fsmeta_scale_profile", workload.DefaultScaleProfile, "scale profile from benchmark/fsmeta/profiles/official/workloads.yaml: median,long,official")
	fsmetaMount           = flag.String("fsmeta_mount", "fsmeta-bench", "fsmeta mount id")
	fsmetaClients         = flag.Int("fsmeta_clients", 0, "concurrent clients; 0 uses selected scale profile")
	fsmetaDirs            = flag.Int("fsmeta_dirs", 0, "mdtest/mimesis directory count; 0 uses selected scale profile")
	fsmetaFilesPerDir     = flag.Int("fsmeta_files_per_dir", 0, "mdtest/mimesis files per directory; 0 uses selected scale profile")
	fsmetaPageLimit       = flag.Uint("fsmeta_page_limit", 0, "ReadDirPlus page limit; 0 uses workload default")
	fsmetaWatchWindow     = flag.Uint("fsmeta_watch_window", 0, "AI checkpoint watch back-pressure window; 0 uses workload default")
	fsmetaMountWait       = flag.Duration("fsmeta_mount_wait", 30*time.Second, "maximum time to wait for fsmeta gateway to observe benchmark mount")
	fsmetaUsers           = flag.Int("fsmeta_users", 0, "filebench-varmail user mailbox count; 0 uses selected scale profile")
	fsmetaMessagesPerUser = flag.Int("fsmeta_messages_per_user", 0, "filebench-varmail messages per user mailbox; 0 uses selected scale profile")
	fsmetaWorkspaces      = flag.Int("fsmeta_workspaces", 0, "AI checkpoint workspace count; 0 uses selected scale profile")
	fsmetaCheckpoints     = flag.Int("fsmeta_checkpoints_per_workspace", 0, "AI checkpoints per workspace; 0 uses selected scale profile")
	fsmetaFilesPerCkpt    = flag.Int("fsmeta_files_per_checkpoint", 0, "AI checkpoint artifact files per checkpoint; 0 uses selected scale profile")
	fsmetaSessionTTL      = flag.Duration("fsmeta_session_ttl", 0, "writer session TTL for varmail and AI checkpoint workloads; 0 uses selected scale profile")
	fsmetaLookupCache     = flag.Int("fsmeta_lookup_cache_entries", 4096, "client-side positive Lookup cache entries; 0 disables")
	fsmetaLookupCacheTTL  = flag.Duration("fsmeta_lookup_cache_ttl", time.Second, "client-side positive Lookup cache TTL")
	fsmetaTimeout         = flag.Duration("fsmeta_timeout", 5*time.Minute, "overall benchmark timeout")
	fsmetaOutput          = flag.String("fsmeta_output", "", "summary CSV output path")
)

func TestBenchmarkFSMeta(t *testing.T) {
	if os.Getenv(benchEnvKey) != "1" {
		t.Skipf("set %s=1 to run fsmeta benchmarks", benchEnvKey)
	}
	ctx, cancel := context.WithTimeout(context.Background(), *fsmetaTimeout)
	defer cancel()
	ensureBenchmarkMount(t, ctx)

	runID := workload.NewRunID()
	var results []workload.Result
	cli, cleanup := openBenchmarkClient(t, ctx)
	defer cleanup()
	waitForFSMetaMount(t, ctx, cli)
	workloads := parseWorkloads(*fsmetaWorkloads)
	for _, workloadName := range workloads {
		result, err := runBenchmarkWorkload(ctx, cli, workloadName, runID)
		if err != nil {
			t.Fatalf("run %s: %v", workloadName, err)
		}
		results = append(results, result)
	}

	rows := make([]workload.SummaryRow, 0)
	for _, result := range results {
		rows = append(rows, workload.SummaryRows(result)...)
	}
	output := *fsmetaOutput
	if output == "" {
		output = filepath.Join("..", "data", "fsmeta", "results", "fsmeta_results_"+runID+".csv")
	}
	if err := os.MkdirAll(filepath.Dir(output), 0o755); err != nil {
		t.Fatalf("mkdir output dir: %v", err)
	}
	f, err := os.Create(output)
	if err != nil {
		t.Fatalf("create output: %v", err)
	}
	defer func() { _ = f.Close() }()
	if err := workload.WriteSummaryCSV(f, rows); err != nil {
		t.Fatalf("write summary CSV: %v", err)
	}
	t.Logf("wrote fsmeta benchmark summary: %s", output)
	manifest := output + ".manifest.txt"
	mf, err := os.Create(manifest)
	if err != nil {
		t.Fatalf("create manifest: %v", err)
	}
	defer func() { _ = mf.Close() }()
	if err := writeBenchmarkManifest(mf, runID, workloads); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	t.Logf("wrote fsmeta benchmark manifest: %s", manifest)
}

func ensureBenchmarkMount(t *testing.T, ctx context.Context) {
	t.Helper()
	if strings.TrimSpace(*fsmetaCoordAddr) == "" || strings.TrimSpace(*fsmetaMount) == "" {
		return
	}
	coordRPC, err := coordclient.NewGRPCClient(ctx, *fsmetaCoordAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial coordinator for mount bootstrap: %v", err)
	}
	defer func() { _ = coordRPC.Close() }()
	resp, err := coordRPC.GetMount(ctx, &coordpb.GetMountRequest{MountId: strings.TrimSpace(*fsmetaMount)})
	if err != nil {
		t.Fatalf("get benchmark mount: %v", err)
	}
	if resp != nil && !resp.GetNotFound() {
		if resp.GetMount().GetState() == coordpb.MountState_MOUNT_STATE_RETIRED {
			t.Fatalf("benchmark mount %q is retired", *fsmetaMount)
		}
		return
	}
	alloc, err := coordRPC.AllocID(ctx, &coordpb.AllocIDRequest{Count: 1})
	if err != nil {
		t.Fatalf("allocate benchmark mount key id: %v", err)
	}
	if alloc == nil || alloc.GetFirstId() == 0 || alloc.GetCount() != 1 {
		t.Fatalf("coordinator returned invalid benchmark mount key id allocation")
	}
	mountKeyID := alloc.GetFirstId()
	publishResp, err := coordRPC.PublishRootEvent(ctx, &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(rootevent.MountRegistered(strings.TrimSpace(*fsmetaMount), mountKeyID, uint64(model.RootInode), 1)),
	})
	if err != nil {
		t.Fatalf("register benchmark mount: %v", err)
	}
	if publishResp == nil || !publishResp.GetAccepted() {
		t.Fatalf("benchmark mount root event was not accepted")
	}
	// MountRegistered materializes the root subtree authority in rooted truth;
	// publishing a second explicit declaration would not be idempotent after
	// the frontier advances during a long benchmark run.
}

func openBenchmarkClient(t *testing.T, ctx context.Context) (workload.MetadataClient, func()) {
	t.Helper()
	cfg := fsmetaclient.ClientConfig{
		DisableLookupCache: *fsmetaLookupCache <= 0,
	}
	if *fsmetaLookupCache > 0 {
		cfg.LookupCache = fsmetaclient.LookupCacheConfig{
			MaxEntries: *fsmetaLookupCache,
			TTL:        *fsmetaLookupCacheTTL,
		}
	}
	cli, err := fsmetaclient.NewGRPCClientWithConfig(ctx, *fsmetaAddr, cfg)
	if err != nil {
		t.Fatalf("dial fsmeta: %v", err)
	}
	return cli, func() { _ = cli.Close() }
}

func waitForFSMetaMount(t *testing.T, ctx context.Context, cli workload.MetadataClient) {
	t.Helper()
	if *fsmetaMountWait <= 0 {
		return
	}
	deadline := time.Now().Add(*fsmetaMountWait)
	var lastErr error
	for {
		stream, err := cli.WatchSubtree(ctx, fsmeta.WatchRequest{
			Mount:     model.MountID(*fsmetaMount),
			RootInode: model.RootInode,
		})
		if err == nil {
			_ = stream.Close()
			return
		}
		lastErr = err
		if !isMountVisibilityPending(err) {
			t.Fatalf("wait for fsmeta mount visibility: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for fsmeta gateway to observe mount %q: %v", *fsmetaMount, lastErr)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for fsmeta mount visibility: %v", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func isMountVisibilityPending(err error) bool {
	return errors.Is(err, model.ErrMountNotRegistered)
}

func runBenchmarkWorkload(ctx context.Context, cli workload.MetadataClient, workloadName, runID string) (workload.Result, error) {
	var (
		result workload.Result
		err    error
	)
	switch workloadName {
	case workload.MDTestEasy:
		scale := resolvedScale(workloadName)
		result, err = workload.RunMDTestEasy(ctx, cli, workload.MDTestConfig{
			Mount:             model.MountID(*fsmetaMount),
			RunID:             runID,
			Clients:           scale.Clients,
			Directories:       scale.Directories,
			FilesPerDirectory: scale.FilesPerDirectory,
			PageLimit:         scale.PageLimit,
		})
	case workload.MDTestHard:
		scale := resolvedScale(workloadName)
		result, err = workload.RunMDTestHard(ctx, cli, workload.MDTestConfig{
			Mount:             model.MountID(*fsmetaMount),
			RunID:             runID,
			Clients:           scale.Clients,
			Directories:       scale.Directories,
			FilesPerDirectory: scale.FilesPerDirectory,
			PageLimit:         scale.PageLimit,
		})
	case workload.FilebenchVarmail:
		scale := resolvedScale(workloadName)
		result, err = workload.RunFilebenchVarmail(ctx, cli, workload.FilebenchVarmailConfig{
			Mount:           model.MountID(*fsmetaMount),
			RunID:           runID,
			Clients:         scale.Clients,
			Users:           scale.Users,
			MessagesPerUser: scale.MessagesPerUser,
			PageLimit:       scale.PageLimit,
			SessionTTL:      scale.SessionTTLDuration(0),
		})
	case workload.MimesisNamespace:
		scale := resolvedScale(workloadName)
		result, err = workload.RunMimesisNamespace(ctx, cli, workload.MimesisNamespaceConfig{
			Mount:             model.MountID(*fsmetaMount),
			RunID:             runID,
			Clients:           scale.Clients,
			Directories:       scale.Directories,
			FilesPerDirectory: scale.FilesPerDirectory,
			PageLimit:         scale.PageLimit,
		})
	case workload.AICheckpointAgent:
		scale := resolvedScale(workloadName)
		result, err = workload.RunAICheckpointAgent(ctx, cli, workload.AICheckpointAgentConfig{
			Mount:                   model.MountID(*fsmetaMount),
			RunID:                   runID,
			Clients:                 scale.Clients,
			Workspaces:              scale.Workspaces,
			CheckpointsPerWorkspace: scale.CheckpointsPerWorkspace,
			FilesPerCheckpoint:      scale.FilesPerCheckpoint,
			PageLimit:               scale.PageLimit,
			WatchWindow:             scale.WatchWindow,
			SessionTTL:              scale.SessionTTLDuration(0),
		})
	default:
		return workload.Result{}, fmt.Errorf("unknown workload %q", workloadName)
	}
	result.Driver = workload.DriverNativeFSMetadata
	return result, err
}

func resolvedScale(workloadName string) workload.OfficialScale {
	scale := workload.ScaleFor(workloadName, strings.TrimSpace(*fsmetaScaleProfile))
	scale.Clients = chooseInt(*fsmetaClients, scale.Clients)
	scale.Directories = chooseInt(*fsmetaDirs, scale.Directories)
	scale.FilesPerDirectory = chooseInt(*fsmetaFilesPerDir, scale.FilesPerDirectory)
	scale.Users = chooseInt(*fsmetaUsers, scale.Users)
	scale.MessagesPerUser = chooseInt(*fsmetaMessagesPerUser, scale.MessagesPerUser)
	scale.Workspaces = chooseInt(*fsmetaWorkspaces, scale.Workspaces)
	scale.CheckpointsPerWorkspace = chooseInt(*fsmetaCheckpoints, scale.CheckpointsPerWorkspace)
	scale.FilesPerCheckpoint = chooseInt(*fsmetaFilesPerCkpt, scale.FilesPerCheckpoint)
	scale.PageLimit = chooseUint32(uint32(*fsmetaPageLimit), scale.PageLimit)
	scale.WatchWindow = chooseUint32(uint32(*fsmetaWatchWindow), scale.WatchWindow)
	if *fsmetaSessionTTL > 0 {
		scale.SessionTTL = fsmetaSessionTTL.String()
	}
	return scale
}

func chooseInt(override, profile int) int {
	if override > 0 {
		return override
	}
	return profile
}

func chooseUint32(override, profile uint32) uint32 {
	if override > 0 {
		return override
	}
	return profile
}

func parseWorkloads(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		name := strings.TrimSpace(part)
		if name != "" {
			out = append(out, name)
		}
	}
	if len(out) == 0 {
		return parseWorkloads(workload.DefaultWorkloadSuite)
	}
	return out
}

func writeBenchmarkManifest(w io.Writer, runID string, workloads []string) error {
	lines := []string{
		"run_id=" + runID,
		"benchmark=fsmeta",
		"driver=" + workload.DriverNativeFSMetadata,
		"profile_file=" + workload.OfficialProfilePath(),
		"scale_profile=" + strings.TrimSpace(*fsmetaScaleProfile),
		"mount=" + *fsmetaMount,
		"fsmeta_addr=" + *fsmetaAddr,
		"coordinator_addr=" + *fsmetaCoordAddr,
		"workloads=" + strings.Join(workloads, ","),
		fmt.Sprintf("lookup_cache_entries=%d", *fsmetaLookupCache),
		"lookup_cache_ttl=" + fsmetaLookupCacheTTL.String(),
		"timeout=" + fsmetaTimeout.String(),
		"",
	}
	for _, line := range lines {
		if _, err := fmt.Fprintln(w, line); err != nil {
			return err
		}
	}
	for _, name := range workloads {
		profile := workload.ProfileFor(name)
		scale := resolvedScale(name)
		if _, err := fmt.Fprintf(w, "[workload.%s]\nsource=%s\nsource_url=%s\nofficial_shape=%s\nfsmeta_projection=%s\n",
			profile.Workload,
			profile.Source,
			profile.SourceURL,
			profile.Shape,
			profile.Projection,
		); err != nil {
			return err
		}
		for _, line := range scale.FormatLines("scale.") {
			if _, err := fmt.Fprintln(w, line); err != nil {
				return err
			}
		}
		keys := make([]string, 0, len(profile.Official))
		for key := range profile.Official {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			value := profile.Official[key]
			if _, err := fmt.Fprintf(w, "official.%s=%s\n", key, value); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
	}
	return nil
}

func TestWriteBenchmarkManifestDocumentsOfficialSources(t *testing.T) {
	var buf bytes.Buffer
	err := writeBenchmarkManifest(&buf, "run-1", []string{workload.MDTestEasy, workload.AICheckpointAgent})
	if err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "source=IO500 mdtest-easy") {
		t.Fatalf("manifest missing mdtest source: %s", out)
	}
	if !strings.Contains(out, "source=MLPerf Storage checkpointing") {
		t.Fatalf("manifest missing MLPerf source: %s", out)
	}
}
