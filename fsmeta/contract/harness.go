// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package contract

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/feichai0017/NoKV/fsmeta/model"
)

// Executor is the fsmeta API surface exercised by the contract harness.
type Executor interface {
	Create(context.Context, model.CreateRequest) (model.CreateResult, error)
	UpdateInode(context.Context, model.UpdateInodeRequest) (model.InodeRecord, error)
	Lookup(context.Context, model.LookupRequest) (model.DentryRecord, error)
	ReadDirPlus(context.Context, model.ReadDirRequest) ([]model.DentryAttrPair, error)
	SnapshotSubtree(context.Context, model.SnapshotSubtreeRequest) (model.SnapshotSubtreeToken, error)
	Rename(context.Context, model.RenameRequest) error
	RenameReplace(context.Context, model.RenameReplaceRequest) (model.RenameReplaceResult, error)
	RenameSubtree(context.Context, model.RenameSubtreeRequest) error
	Link(context.Context, model.LinkRequest) error
	Unlink(context.Context, model.UnlinkRequest) error
	Remove(context.Context, model.RemoveRequest) (model.RemoveResult, error)
	OpenWriteSession(context.Context, model.OpenWriteSessionRequest) (model.SessionRecord, error)
	HeartbeatWriteSession(context.Context, model.HeartbeatWriteSessionRequest) (model.SessionRecord, error)
	CloseWriteSession(context.Context, model.CloseWriteSessionRequest) error
	ExpireWriteSessions(context.Context, model.ExpireWriteSessionsRequest) (model.ExpireWriteSessionsResult, error)
}

type plannedCreateInodeContextKey struct{}

func withPlannedCreateInode(ctx context.Context, inode model.InodeID) context.Context {
	if inode == 0 {
		return ctx
	}
	return context.WithValue(ctx, plannedCreateInodeContextKey{}, inode)
}

func plannedCreateInode(ctx context.Context) (model.InodeID, bool) {
	inode, ok := ctx.Value(plannedCreateInodeContextKey{}).(model.InodeID)
	return inode, ok && inode != 0
}

// Run executes operations against the system under test and the reference
// model, comparing every externally visible result.
func Run(ctx context.Context, exec Executor, state *Model, ops []Operation) error {
	if exec == nil {
		return errExecutorRequired
	}
	if state == nil {
		return errModelRequired
	}
	history := make([]string, 0, len(ops))
	for i, op := range ops {
		got := execute(ctx, exec, state, op)
		if op.Kind == OpCreate && got.Err == nil {
			op.Inode = got.Inode.Inode
		}
		var want Result
		if op.Kind == OpSnapshotSubtree {
			if got.Err == nil {
				want = state.ApplySnapshot(op, got.Token)
			} else {
				want = state.Apply(op)
			}
		} else {
			want = state.Apply(op)
		}
		history = append(history, fmt.Sprintf("%03d %s -> got=%s want=%s", i, op, summarize(got), summarize(want)))
		if err := compareResult(got, want); err != nil {
			return fmt.Errorf("step %d failed: %w\nhistory:\n%s", i, err, strings.Join(history, "\n"))
		}
		if err := state.CheckInvariants(); err != nil {
			return fmt.Errorf("step %d corrupted model invariants: %w\nhistory:\n%s", i, err, strings.Join(history, "\n"))
		}
	}
	return nil
}

func execute(ctx context.Context, exec Executor, state *Model, op Operation) Result {
	switch op.Kind {
	case OpCreate:
		// The real API allocates Create inode IDs server-side. The contract
		// harness still pins a model inode per generated operation so future
		// Update/Link/Session operations can target a stable object even when
		// concurrent duplicate-name creates race.
		result, err := exec.Create(withPlannedCreateInode(ctx, op.Inode), model.CreateRequest{
			Mount:  op.Mount,
			Parent: op.Parent,
			Name:   op.Name,
			Attrs: model.CreateAttrs{
				Type: op.Type,
				Size: op.Size,
				Mode: op.Mode,
			},
		})
		return Result{Err: err, Dentry: result.Dentry, Inode: result.Inode}
	case OpUpdateInode:
		inode, err := exec.UpdateInode(ctx, model.UpdateInodeRequest{
			Mount:   op.Mount,
			Parent:  op.Parent,
			Inode:   op.Inode,
			Name:    op.Name,
			SetSize: true,
			Size:    op.Size,
			SetMode: true,
			Mode:    op.Mode,
		})
		return Result{Err: err, Inode: inode}
	case OpLookup:
		dentry, err := exec.Lookup(ctx, model.LookupRequest{
			Mount:  op.Mount,
			Parent: op.Parent,
			Name:   op.Name,
		})
		return Result{Err: err, Dentry: dentry}
	case OpReadDirPlus:
		pairs, err := exec.ReadDirPlus(ctx, model.ReadDirRequest{
			Mount:           op.Mount,
			Parent:          op.Parent,
			StartAfter:      op.StartAfter,
			Limit:           op.Limit,
			SnapshotVersion: state.SnapshotVersion(op.SnapshotRef),
		})
		return Result{Err: err, Pairs: pairs}
	case OpSnapshotSubtree:
		token, err := exec.SnapshotSubtree(ctx, model.SnapshotSubtreeRequest{
			Mount:     op.Mount,
			RootInode: op.Parent,
		})
		return Result{Err: err, Token: token}
	case OpRename:
		err := exec.Rename(ctx, model.RenameRequest{
			Mount:      op.Mount,
			FromParent: op.FromParent,
			FromName:   op.FromName,
			ToParent:   op.ToParent,
			ToName:     op.ToName,
		})
		return Result{Err: err}
	case OpRenameReplace:
		result, err := exec.RenameReplace(ctx, model.RenameReplaceRequest{
			Mount:      op.Mount,
			FromParent: op.FromParent,
			FromName:   op.FromName,
			ToParent:   op.ToParent,
			ToName:     op.ToName,
		})
		return Result{Err: err, RenameReplace: result}
	case OpRenameSubtree:
		err := exec.RenameSubtree(ctx, model.RenameSubtreeRequest{
			Mount:      op.Mount,
			FromParent: op.FromParent,
			FromName:   op.FromName,
			ToParent:   op.ToParent,
			ToName:     op.ToName,
		})
		return Result{Err: err}
	case OpLink:
		err := exec.Link(ctx, model.LinkRequest{
			Mount:      op.Mount,
			FromParent: op.FromParent,
			FromName:   op.FromName,
			ToParent:   op.ToParent,
			ToName:     op.ToName,
		})
		return Result{Err: err}
	case OpUnlink:
		err := exec.Unlink(ctx, model.UnlinkRequest{
			Mount:  op.Mount,
			Parent: op.Parent,
			Name:   op.Name,
		})
		return Result{Err: err}
	case OpRemove:
		result, err := exec.Remove(ctx, model.RemoveRequest{
			Mount:  op.Mount,
			Parent: op.Parent,
			Name:   op.Name,
		})
		return Result{Err: err, Remove: result}
	case OpOpenWriteSession:
		session, err := exec.OpenWriteSession(ctx, model.OpenWriteSessionRequest{
			Mount:   op.Mount,
			Inode:   op.Inode,
			Session: op.Session,
			TTL:     time.Duration(op.ExpiresNs - state.NowUnixNs),
		})
		return Result{Err: err, Session: session}
	case OpHeartbeatSession:
		session, err := exec.HeartbeatWriteSession(ctx, model.HeartbeatWriteSessionRequest{
			Mount:   op.Mount,
			Inode:   op.Inode,
			Session: op.Session,
			TTL:     time.Duration(op.ExpiresNs - state.NowUnixNs),
		})
		return Result{Err: err, Session: session}
	case OpCloseSession:
		err := exec.CloseWriteSession(ctx, model.CloseWriteSessionRequest{
			Mount:   op.Mount,
			Inode:   op.Inode,
			Session: op.Session,
		})
		return Result{Err: err}
	case OpExpireSessions:
		result, err := exec.ExpireWriteSessions(ctx, model.ExpireWriteSessionsRequest{
			Mount: op.Mount,
			Limit: op.Limit,
		})
		return Result{Err: err, Expired: result.Expired}
	case OpAdvanceTime:
		return Result{}
	default:
		return Result{Err: model.ErrInvalidRequest}
	}
}

func compareResult(got, want Result) error {
	if !EquivalentError(got.Err, want.Err) {
		return fmt.Errorf("error mismatch: got %v want %v", got.Err, want.Err)
	}
	if got.Err != nil || want.Err != nil {
		return nil
	}
	if !reflect.DeepEqual(got.Token, want.Token) {
		return fmt.Errorf("token mismatch: got %+v want %+v", got.Token, want.Token)
	}
	if got.Dentry != want.Dentry {
		return fmt.Errorf("dentry mismatch: got %+v want %+v", got.Dentry, want.Dentry)
	}
	if !reflect.DeepEqual(got.Pairs, want.Pairs) {
		return fmt.Errorf("pairs mismatch: got %+v want %+v", got.Pairs, want.Pairs)
	}
	if !reflect.DeepEqual(got.Inode, want.Inode) {
		return fmt.Errorf("inode mismatch: got %+v want %+v", got.Inode, want.Inode)
	}
	if !reflect.DeepEqual(got.RenameReplace, want.RenameReplace) {
		return fmt.Errorf("rename replace mismatch: got %+v want %+v", got.RenameReplace, want.RenameReplace)
	}
	if !reflect.DeepEqual(got.Remove, want.Remove) {
		return fmt.Errorf("remove mismatch: got %+v want %+v", got.Remove, want.Remove)
	}
	if got.Session != want.Session {
		return fmt.Errorf("session mismatch: got %+v want %+v", got.Session, want.Session)
	}
	if got.Expired != want.Expired {
		return fmt.Errorf("expired mismatch: got %d want %d", got.Expired, want.Expired)
	}
	return nil
}

func summarize(result Result) string {
	if result.Err != nil {
		return "err=" + result.Err.Error()
	}
	if result.Token.ReadVersion != 0 {
		return fmt.Sprintf("token=%+v", result.Token)
	}
	if len(result.Pairs) != 0 {
		names := make([]string, 0, len(result.Pairs))
		for _, pair := range result.Pairs {
			names = append(names, fmt.Sprintf("%s:%d/%d", pair.Dentry.Name, pair.Dentry.Inode, pair.Inode.LinkCount))
		}
		return "pairs=[" + strings.Join(names, ",") + "]"
	}
	if result.Dentry.Name != "" {
		return fmt.Sprintf("dentry=%s:%d", result.Dentry.Name, result.Dentry.Inode)
	}
	if result.Inode.Inode != 0 {
		return fmt.Sprintf("inode=%d size=%d links=%d", result.Inode.Inode, result.Inode.Size, result.Inode.LinkCount)
	}
	if result.RenameReplace.Replaced {
		return fmt.Sprintf("rename_replace old=%s:%d deleted=%t", result.RenameReplace.OldDentry.Name, result.RenameReplace.OldDentry.Inode, result.RenameReplace.OldInodeDeleted)
	}
	if result.Remove.RemovedDentry.Name != "" {
		return fmt.Sprintf("remove old=%s:%d deleted=%t", result.Remove.RemovedDentry.Name, result.Remove.RemovedDentry.Inode, result.Remove.InodeDeleted)
	}
	if result.Session.Session != "" {
		return fmt.Sprintf("session=%s inode=%d expires=%d", result.Session.Session, result.Session.Inode, result.Session.ExpiresUnixNs)
	}
	if result.Expired != 0 {
		return fmt.Sprintf("expired=%d", result.Expired)
	}
	return "ok"
}
