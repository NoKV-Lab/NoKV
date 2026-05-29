// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	nokverrors "github.com/feichai0017/NoKV/errors"
	fsmetaclient "github.com/feichai0017/NoKV/fsmeta/client"
	fsmetacontract "github.com/feichai0017/NoKV/fsmeta/contract"
	"github.com/feichai0017/NoKV/fsmeta/model"
)

type historyRun struct {
	seed  int64
	state *fsmetacontract.Model
	ops   []fsmetacontract.Operation
}

type historyScopeClient interface {
	Create(context.Context, model.CreateRequest) (model.CreateResult, error)
	Lookup(context.Context, model.LookupRequest) (model.DentryRecord, error)
}

func main() {
	var (
		addr               = flag.String("addr", "127.0.0.1:8090", "FSMetadata gRPC address")
		mount              = flag.String("mount", "default", "registered mount ID")
		seeds              = flag.Int("seeds", 1, "number of deterministic seeds to run")
		start              = flag.Int64("seed-start", 1, "first deterministic seed")
		steps              = flag.Int("steps", 64, "generated operations per seed before external filtering")
		batch              = flag.Int("batch", 3, "concurrent history batch size")
		timeout            = flag.Duration("timeout", 60*time.Second, "overall command timeout")
		scope              = flag.String("scope-prefix", "history", "unique root directory prefix for isolating each generated history")
		readyFile          = flag.String("ready-file", "", "optional file written after all history scopes are prepared")
		allowIndeterminate = flag.Bool("allow-indeterminate-errors", false, "treat retryable availability errors as operations with unknown commit outcome")
	)
	flag.Parse()
	if *seeds <= 0 || *start <= 0 || *steps <= 0 {
		log.Fatalf("seeds, seed-start, and steps must be positive")
	}
	if *batch <= 0 {
		log.Fatalf("batch must be positive")
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	cli, err := fsmetaclient.NewGRPCClient(ctx, *addr)
	if err != nil {
		log.Fatalf("dial fsmeta %s: %v", *addr, err)
	}
	defer func() { _ = cli.Close() }()

	mountID := model.MountID(*mount)
	runs, err := prepareAndSignalHistoryRuns(ctx, cli, mountID, *start, *seeds, *steps, *scope, *readyFile)
	if err != nil {
		log.Fatal(err)
	}
	opts := fsmetacontract.HistoryOptions{AllowIndeterminateErrors: *allowIndeterminate}
	if err := runHistoryRuns(ctx, cli, runs, *batch, opts); err != nil {
		log.Fatal(err)
	}
}

func prepareAndSignalHistoryRuns(ctx context.Context, cli historyScopeClient, mountID model.MountID, start int64, seeds, steps int, scopePrefix, readyFile string) ([]historyRun, error) {
	runs, err := prepareHistoryRuns(ctx, cli, mountID, start, seeds, steps, scopePrefix)
	if err != nil {
		return nil, err
	}
	if err := writeReadyFile(readyFile); err != nil {
		return nil, fmt.Errorf("write history ready file: %w", err)
	}
	return runs, nil
}

func prepareHistoryRuns(ctx context.Context, cli historyScopeClient, mountID model.MountID, start int64, seeds, steps int, scopePrefix string) ([]historyRun, error) {
	runs := make([]historyRun, 0, seeds)
	for seed := start; seed < start+int64(seeds); seed++ {
		state := fsmetacontract.NewModel(mountID)
		unique := time.Now().UnixNano()
		scopeName := fmt.Sprintf("%s-%06d-%d", scopePrefix, seed, unique)
		scopeInode := model.InodeID(9_000_000_000 + seed*1_000_000 + unique%1_000_000)
		scopeOp := scopeCreateOperation(mountID, scopeName, scopeInode)
		scopeResult, err := createScopeWithRetry(ctx, cli, scopeOp)
		if err != nil {
			return nil, fmt.Errorf("create history scope seed=%d: %w", seed, err)
		}
		scopeOp.Inode = scopeResult.Inode.Inode
		scopeInode = scopeResult.Inode.Inode
		if got := state.Apply(scopeOp); got.Err != nil {
			return nil, fmt.Errorf("apply history scope seed=%d: %w", seed, got.Err)
		}
		ops := externalHistoryOps(fsmetacontract.GenerateScript(seed, steps), mountID, scopeInode, scopeInode)
		if len(ops) == 0 {
			return nil, fmt.Errorf("seed %d generated no external-safe operations", seed)
		}
		runs = append(runs, historyRun{seed: seed, state: state, ops: ops})
	}
	return runs, nil
}

func runHistoryRuns(ctx context.Context, exec fsmetacontract.Executor, runs []historyRun, batch int, opts fsmetacontract.HistoryOptions) error {
	for _, run := range runs {
		historyExec, err := fsmetacontract.NewInodeMappingExecutor(exec)
		if err != nil {
			return fmt.Errorf("open history inode mapper: %w", err)
		}
		if err := fsmetacontract.RunConcurrentBatches(ctx, historyExec, run.state, run.ops, batch, opts); err != nil {
			fmt.Fprintf(os.Stderr, "fsmeta history failed seed=%d filtered_ops=%d\n", run.seed, len(run.ops))
			return err
		}
		log.Printf("fsmeta history passed seed=%d filtered_ops=%d", run.seed, len(run.ops))
	}
	return nil
}

func writeReadyFile(path string) error {
	if path == "" {
		return nil
	}
	return os.WriteFile(path, []byte(time.Now().Format(time.RFC3339Nano)+"\n"), 0o644)
}

func scopeCreateOperation(mount model.MountID, scopeName string, scopeInode model.InodeID) fsmetacontract.Operation {
	return fsmetacontract.Operation{
		Kind:   fsmetacontract.OpCreate,
		Mount:  mount,
		Parent: model.RootInode,
		Name:   scopeName,
		Inode:  scopeInode,
		Type:   model.InodeTypeDirectory,
		Mode:   0o755,
	}
}

func createScopeWithRetry(ctx context.Context, cli historyScopeClient, op fsmetacontract.Operation) (model.CreateResult, error) {
	delay := 100 * time.Millisecond
	for {
		req := model.CreateRequest{
			Mount:  op.Mount,
			Parent: op.Parent,
			Name:   op.Name,
			Attrs: model.CreateAttrs{
				Type: op.Type,
				Mode: op.Mode,
			},
		}
		result, err := cli.Create(ctx, req)
		if err == nil || errors.Is(err, model.ErrExists) {
			if err == nil {
				return result, nil
			}
			dentry, lookupErr := cli.Lookup(ctx, model.LookupRequest{Mount: op.Mount, Parent: op.Parent, Name: op.Name})
			if lookupErr == nil {
				return model.CreateResult{Dentry: dentry, Inode: req.Attrs.InodeRecord(dentry.Inode)}, nil
			}
		}
		if !retryScopeCreateError(err) {
			return model.CreateResult{}, err
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return model.CreateResult{}, ctx.Err()
		case <-timer.C:
		}
		if delay < time.Second {
			delay *= 2
		}
	}
}

func retryScopeCreateError(err error) bool {
	if err == nil {
		return false
	}
	// The scope create is a startup/admission barrier, not part of the
	// generated correctness history. Let the outer command timeout absorb
	// transient root, coordinator, and store recovery windows.
	if errors.Is(err, model.ErrMountNotRegistered) {
		return true
	}
	switch nokverrors.KindOf(err) {
	case nokverrors.KindNotFound,
		nokverrors.KindRetryExhausted:
		return true
	default:
		return nokverrors.Retryable(err)
	}
}

func externalHistoryOps(in []fsmetacontract.Operation, mount model.MountID, scopeInode, inodeBase model.InodeID) []fsmetacontract.Operation {
	out := make([]fsmetacontract.Operation, 0, len(in))
	for _, op := range in {
		switch op.Kind {
		case fsmetacontract.OpOpenWriteSession,
			fsmetacontract.OpHeartbeatSession,
			fsmetacontract.OpCloseSession,
			fsmetacontract.OpExpireSessions,
			fsmetacontract.OpAdvanceTime:
			continue
		default:
			op.Mount = mount
			// The generated inodes are unique only within one in-memory script.
			// Docker chaos runs multiple seeds against the same mounted system,
			// so external histories must shift inode ids into the per-seed scope
			// to avoid cross-seed namespace pollution.
			op.Inode = scopeGeneratedInode(inodeBase, op.Inode)
			if op.Parent == model.RootInode {
				op.Parent = scopeInode
			} else {
				op.Parent = scopeGeneratedInode(inodeBase, op.Parent)
			}
			if op.FromParent == model.RootInode {
				op.FromParent = scopeInode
			} else {
				op.FromParent = scopeGeneratedInode(inodeBase, op.FromParent)
			}
			if op.ToParent == model.RootInode {
				op.ToParent = scopeInode
			} else {
				op.ToParent = scopeGeneratedInode(inodeBase, op.ToParent)
			}
			out = append(out, op)
		}
	}
	return out
}

func scopeGeneratedInode(base, inode model.InodeID) model.InodeID {
	if inode == 0 {
		return 0
	}
	return base + inode
}
