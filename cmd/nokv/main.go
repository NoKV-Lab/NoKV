// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"io"
	"os"
)

var exit = os.Exit

func main() {
	if len(os.Args) < 2 {
		printUsage(os.Stdout)
		exit(1)
	}

	cmd := os.Args[1]
	args := os.Args[2:]

	var err error
	switch cmd {
	case "stats":
		err = runStatsCmd(os.Stdout, args)
	case "execution":
		err = runExecutionCmd(os.Stdout, args)
	case "regions":
		err = runRegionsCmd(os.Stdout, args)
	case "serve":
		err = runServeCmd(os.Stdout, args)
	case "coordinator":
		err = runCoordinatorCmd(os.Stdout, args)
	case "meta-root":
		err = runMetaRootCmd(os.Stdout, args)
	case "mount":
		err = runMountCmd(os.Stdout, args)
	case "quota":
		err = runQuotaCmd(os.Stdout, args)
	case "audit":
		err = runAuditCmd(os.Stdout, args)
	case "help", "-h", "--help":
		printUsage(os.Stdout)
	default:
		err = fmt.Errorf("unknown command %q", cmd)
	}

	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error: %v\n", err)
		exit(1)
	}
}

func printUsage(w io.Writer) {
	_, _ = fmt.Fprintln(w, `Usage: nokv <command> [flags]

	Commands:
	  stats     Dump runtime backlog metrics (requires working directory or expvar endpoint)
	  execution Query raftstore execution-plane diagnostics from admin RPC
	  regions   Show the local peer catalog used for store recovery
	  serve     Start StoreKV gRPC service backed by a local raftstore
	  coordinator Start coordinator gRPC service (control plane)
	  meta-root Start metadata root gRPC service
	  mount     Register, retire, or list rooted fsmeta mounts
	  quota     Set, clear, or list rooted fsmeta quota fences
	  audit     Surface rooted authority/finality anomalies from a meta-root snapshot

Run "nokv <command> -h" for command-specific flags.`)
}
