// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/feichai0017/NoKV/benchmark/fsmeta/workload"
	fsmetaclient "github.com/feichai0017/NoKV/fsmeta/client"
	"github.com/feichai0017/NoKV/fsmeta/model"
)

func main() {
	var (
		addr                    = flag.String("addr", "127.0.0.1:8090", "FSMetadata gRPC endpoint")
		workloadName            = flag.String("workload", workload.MDTestEasy, "workload: "+workload.DefaultWorkloadSuite)
		scaleProfile            = flag.String("scale-profile", workload.DefaultScaleProfile, "scale profile from benchmark/fsmeta/profiles/official/workloads.yaml: median,long,official")
		mount                   = flag.String("mount", "fsmeta-demo", "fsmeta mount id")
		runID                   = flag.String("run-id", "", "run id suffix; defaults to current UTC timestamp")
		clients                 = flag.Int("clients", 0, "concurrent clients; 0 uses selected scale profile")
		dirs                    = flag.Int("dirs", 0, "mdtest/mimesis directory count; 0 uses selected scale profile")
		filesPerDir             = flag.Int("files-per-dir", 0, "mdtest/mimesis files per directory; 0 uses selected scale profile")
		pageLimit               = flag.Uint("page-limit", 0, "ReadDirPlus page limit; 0 uses selected scale profile")
		users                   = flag.Int("users", 0, "filebench-varmail user mailbox count; 0 uses selected scale profile")
		messagesPerUser         = flag.Int("messages-per-user", 0, "filebench-varmail messages per user; 0 uses selected scale profile")
		workspaces              = flag.Int("workspaces", 0, "AI checkpoint workspace count; 0 uses selected scale profile")
		checkpointsPerWorkspace = flag.Int("checkpoints-per-workspace", 0, "AI checkpoints per workspace; 0 uses selected scale profile")
		filesPerCheckpoint      = flag.Int("files-per-checkpoint", 0, "AI checkpoint artifact files per checkpoint; 0 uses selected scale profile")
		watchWindow             = flag.Uint("watch-window", 0, "AI checkpoint watch back-pressure window; 0 uses selected scale profile")
		sessionTTL              = flag.Duration("session-ttl", 0, "writer session TTL for varmail and AI checkpoint workloads; 0 uses selected scale profile")
		timeout                 = flag.Duration("timeout", 2*time.Minute, "overall workload timeout")
		printCSV                = flag.Bool("csv", false, "print summary as CSV instead of a table")
	)
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	cli, err := fsmetaclient.NewGRPCClient(ctx, *addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial fsmeta: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = cli.Close() }()

	id := *runID
	if id == "" {
		id = workload.NewRunID()
	}
	result, err := run(ctx, cli, runConfig{
		name:                    *workloadName,
		scaleProfile:            *scaleProfile,
		mount:                   model.MountID(*mount),
		runID:                   id,
		clients:                 *clients,
		dirs:                    *dirs,
		filesPerDir:             *filesPerDir,
		pageLimit:               uint32(*pageLimit),
		users:                   *users,
		messagesPerUser:         *messagesPerUser,
		workspaces:              *workspaces,
		checkpointsPerWorkspace: *checkpointsPerWorkspace,
		filesPerCheckpoint:      *filesPerCheckpoint,
		watchWindow:             uint32(*watchWindow),
		sessionTTL:              *sessionTTL,
	})
	if *printCSV {
		_ = workload.WriteSummaryCSV(os.Stdout, workload.SummaryRows(result))
	} else {
		printSummary(result)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "workload failed: %v\n", err)
		os.Exit(1)
	}
}

type runConfig struct {
	name                    string
	scaleProfile            string
	mount                   model.MountID
	runID                   string
	clients                 int
	dirs                    int
	filesPerDir             int
	pageLimit               uint32
	users                   int
	messagesPerUser         int
	workspaces              int
	checkpointsPerWorkspace int
	filesPerCheckpoint      int
	watchWindow             uint32
	sessionTTL              time.Duration
}

func run(ctx context.Context, cli workload.MetadataClient, cfg runConfig) (workload.Result, error) {
	switch cfg.name {
	case workload.MDTestEasy:
		scale := resolvedScale(cfg, workload.MDTestEasy)
		return workload.RunMDTestEasy(ctx, cli, workload.MDTestConfig{
			Mount:             cfg.mount,
			RunID:             cfg.runID,
			Clients:           scale.Clients,
			Directories:       scale.Directories,
			FilesPerDirectory: scale.FilesPerDirectory,
			PageLimit:         scale.PageLimit,
		})
	case workload.MDTestHard:
		scale := resolvedScale(cfg, workload.MDTestHard)
		return workload.RunMDTestHard(ctx, cli, workload.MDTestConfig{
			Mount:             cfg.mount,
			RunID:             cfg.runID,
			Clients:           scale.Clients,
			Directories:       scale.Directories,
			FilesPerDirectory: scale.FilesPerDirectory,
			PageLimit:         scale.PageLimit,
		})
	case workload.FilebenchVarmail:
		scale := resolvedScale(cfg, workload.FilebenchVarmail)
		return workload.RunFilebenchVarmail(ctx, cli, workload.FilebenchVarmailConfig{
			Mount:           cfg.mount,
			RunID:           cfg.runID,
			Clients:         scale.Clients,
			Users:           scale.Users,
			MessagesPerUser: scale.MessagesPerUser,
			PageLimit:       scale.PageLimit,
			SessionTTL:      scale.SessionTTLDuration(0),
		})
	case workload.MimesisNamespace:
		scale := resolvedScale(cfg, workload.MimesisNamespace)
		return workload.RunMimesisNamespace(ctx, cli, workload.MimesisNamespaceConfig{
			Mount:             cfg.mount,
			RunID:             cfg.runID,
			Clients:           scale.Clients,
			Directories:       scale.Directories,
			FilesPerDirectory: scale.FilesPerDirectory,
			PageLimit:         scale.PageLimit,
		})
	case workload.AICheckpointAgent:
		scale := resolvedScale(cfg, workload.AICheckpointAgent)
		return workload.RunAICheckpointAgent(ctx, cli, workload.AICheckpointAgentConfig{
			Mount:                   cfg.mount,
			RunID:                   cfg.runID,
			Clients:                 scale.Clients,
			Workspaces:              scale.Workspaces,
			CheckpointsPerWorkspace: scale.CheckpointsPerWorkspace,
			FilesPerCheckpoint:      scale.FilesPerCheckpoint,
			PageLimit:               scale.PageLimit,
			WatchWindow:             scale.WatchWindow,
			SessionTTL:              scale.SessionTTLDuration(0),
		})
	default:
		return workload.Result{}, fmt.Errorf("unknown workload %q", cfg.name)
	}
}

func resolvedScale(cfg runConfig, name string) workload.OfficialScale {
	scale := workload.ScaleFor(name, strings.TrimSpace(cfg.scaleProfile))
	scale.Clients = chooseInt(cfg.clients, scale.Clients)
	scale.Directories = chooseInt(cfg.dirs, scale.Directories)
	scale.FilesPerDirectory = chooseInt(cfg.filesPerDir, scale.FilesPerDirectory)
	scale.Users = chooseInt(cfg.users, scale.Users)
	scale.MessagesPerUser = chooseInt(cfg.messagesPerUser, scale.MessagesPerUser)
	scale.Workspaces = chooseInt(cfg.workspaces, scale.Workspaces)
	scale.CheckpointsPerWorkspace = chooseInt(cfg.checkpointsPerWorkspace, scale.CheckpointsPerWorkspace)
	scale.FilesPerCheckpoint = chooseInt(cfg.filesPerCheckpoint, scale.FilesPerCheckpoint)
	scale.PageLimit = chooseUint32(cfg.pageLimit, scale.PageLimit)
	scale.WatchWindow = chooseUint32(cfg.watchWindow, scale.WatchWindow)
	if cfg.sessionTTL > 0 {
		scale.SessionTTL = cfg.sessionTTL.String()
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

func printSummary(result workload.Result) {
	fmt.Printf("workload=%s run_id=%s ops=%d errors=%d duration=%s\n", result.Name, result.RunID, result.Ops, result.Errors, result.Duration)
	tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "operation\tcount\terrors\twall_ops/s\tactive_ops/s\tavg_us\tp50_us\tp95_us\tp99_us")
	for _, row := range workload.SummaryRows(result) {
		fmt.Fprintf(tw, "%s\t%d\t%d\t%.2f\t%.2f\t%.1f\t%.1f\t%.1f\t%.1f\n",
			row.Operation,
			row.Count,
			row.Errors,
			row.Throughput,
			row.ActiveThroughput,
			row.AverageUS,
			row.P50US,
			row.P95US,
			row.P99US,
		)
	}
	_ = tw.Flush()
}
