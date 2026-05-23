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
	"github.com/feichai0017/NoKV/fsmeta"
	fsmetaclient "github.com/feichai0017/NoKV/fsmeta/client"
	fsmetacontract "github.com/feichai0017/NoKV/fsmeta/contract"
)

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

	mountID := fsmeta.MountID(*mount)
	for seed := *start; seed < *start+int64(*seeds); seed++ {
		model := fsmetacontract.NewModel(mountID)
		unique := time.Now().UnixNano()
		scopeName := fmt.Sprintf("%s-%06d-%d", *scope, seed, unique)
		scopeInode := fsmeta.InodeID(9_000_000_000 + seed*1_000_000 + unique%1_000_000)
		scopeOp := scopeCreateOperation(mountID, scopeName, scopeInode)
		scopeResult, err := createScopeWithRetry(ctx, cli, scopeOp)
		if err != nil {
			log.Fatalf("create history scope seed=%d: %v", seed, err)
		}
		scopeOp.Inode = scopeResult.Inode.Inode
		scopeInode = scopeResult.Inode.Inode
		if got := model.Apply(scopeOp); got.Err != nil {
			log.Fatalf("apply history scope seed=%d: %v", seed, got.Err)
		}
		ops := externalHistoryOps(fsmetacontract.GenerateScript(seed, *steps), mountID, scopeInode, scopeInode)
		if len(ops) == 0 {
			log.Fatalf("seed %d generated no external-safe operations", seed)
		}
		historyExec, err := fsmetacontract.NewInodeMappingExecutor(cli)
		if err != nil {
			log.Fatalf("open history inode mapper: %v", err)
		}
		opts := fsmetacontract.HistoryOptions{AllowIndeterminateErrors: *allowIndeterminate}
		if err := fsmetacontract.RunConcurrentBatches(ctx, historyExec, model, ops, *batch, opts); err != nil {
			fmt.Fprintf(os.Stderr, "fsmeta history failed seed=%d steps=%d filtered_ops=%d\n", seed, *steps, len(ops))
			log.Fatal(err)
		}
		log.Printf("fsmeta history passed seed=%d filtered_ops=%d", seed, len(ops))
	}
}

func scopeCreateOperation(mount fsmeta.MountID, scopeName string, scopeInode fsmeta.InodeID) fsmetacontract.Operation {
	return fsmetacontract.Operation{
		Kind:   fsmetacontract.OpCreate,
		Mount:  mount,
		Parent: fsmeta.RootInode,
		Name:   scopeName,
		Inode:  scopeInode,
		Type:   fsmeta.InodeTypeDirectory,
		Mode:   0o755,
	}
}

func createScopeWithRetry(ctx context.Context, cli fsmetaclient.Client, op fsmetacontract.Operation) (fsmeta.CreateResult, error) {
	delay := 100 * time.Millisecond
	for {
		req := fsmeta.CreateRequest{
			Mount:  op.Mount,
			Parent: op.Parent,
			Name:   op.Name,
			Attrs: fsmeta.CreateAttrs{
				Type: op.Type,
				Mode: op.Mode,
			},
		}
		result, err := cli.Create(ctx, req)
		if err == nil || errors.Is(err, fsmeta.ErrExists) {
			if err == nil {
				return result, nil
			}
			dentry, lookupErr := cli.Lookup(ctx, fsmeta.LookupRequest{Mount: op.Mount, Parent: op.Parent, Name: op.Name})
			if lookupErr == nil {
				return fsmeta.CreateResult{Dentry: dentry, Inode: req.Attrs.InodeRecord(dentry.Inode)}, nil
			}
		}
		if !retryScopeCreateError(err) {
			return fsmeta.CreateResult{}, err
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fsmeta.CreateResult{}, ctx.Err()
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
	if errors.Is(err, fsmeta.ErrMountNotRegistered) {
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

func externalHistoryOps(in []fsmetacontract.Operation, mount fsmeta.MountID, scopeInode, inodeBase fsmeta.InodeID) []fsmetacontract.Operation {
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
			if op.Parent == fsmeta.RootInode {
				op.Parent = scopeInode
			} else {
				op.Parent = scopeGeneratedInode(inodeBase, op.Parent)
			}
			if op.FromParent == fsmeta.RootInode {
				op.FromParent = scopeInode
			} else {
				op.FromParent = scopeGeneratedInode(inodeBase, op.FromParent)
			}
			if op.ToParent == fsmeta.RootInode {
				op.ToParent = scopeInode
			} else {
				op.ToParent = scopeGeneratedInode(inodeBase, op.ToParent)
			}
			out = append(out, op)
		}
	}
	return out
}

func scopeGeneratedInode(base, inode fsmeta.InodeID) fsmeta.InodeID {
	if inode == 0 {
		return 0
	}
	return base + inode
}
