// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package contract

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta"
)

func TestInodeMappingExecutorTranslatesGeneratedHistoryIDs(t *testing.T) {
	base := newFakeExternalExecutor()
	exec, err := NewInodeMappingExecutor(base)
	if err != nil {
		t.Fatalf("NewInodeMappingExecutor: %v", err)
	}
	model := NewModel("vol")
	ops := []Operation{
		{Kind: OpCreate, Mount: "vol", Parent: fsmeta.RootInode, Name: "alpha", Inode: 10, Type: fsmeta.InodeTypeFile, Mode: 0o644},
		{Kind: OpUpdateInode, Mount: "vol", Parent: fsmeta.RootInode, Name: "alpha", Inode: 10, Size: 42, Mode: 0o600},
		{Kind: OpReadDirPlus, Mount: "vol", Parent: fsmeta.RootInode, Limit: 16},
	}
	if err := Run(context.Background(), exec, model, ops); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if base.lastUpdated != 1000 {
		t.Fatalf("base update used inode %d, want real inode 1000", base.lastUpdated)
	}
}

func TestInodeMappingExecutorRecoversCommittedCreate(t *testing.T) {
	base := newFakeExternalExecutor()
	base.createErrAfterCommit = context.DeadlineExceeded
	exec, err := NewInodeMappingExecutor(base)
	if err != nil {
		t.Fatalf("NewInodeMappingExecutor: %v", err)
	}
	result, err := exec.Create(withPlannedCreateInode(context.Background(), 10), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "alpha",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile, Mode: 0o644},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if result.Dentry.Inode != 10 || result.Inode.Inode != 10 {
		t.Fatalf("result was not translated to planned inode: %+v", result)
	}
}

func TestInodeMappingExecutorDoesNotRecoverSemanticCreateError(t *testing.T) {
	base := newFakeExternalExecutor()
	_, err := base.Create(context.Background(), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "alpha",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeDirectory, Mode: 0o755},
	})
	if err != nil {
		t.Fatalf("seed Create: %v", err)
	}
	exec, err := NewInodeMappingExecutor(base)
	if err != nil {
		t.Fatalf("NewInodeMappingExecutor: %v", err)
	}
	_, err = exec.Create(withPlannedCreateInode(context.Background(), 10), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "alpha",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile, Mode: 0o644},
	})
	if !errors.Is(err, fsmeta.ErrExists) {
		t.Fatalf("Create error = %v, want ErrExists", err)
	}
}

func TestInodeMappingExecutorTranslatesCreateObservedBeforeReturn(t *testing.T) {
	base := newFakeExternalExecutor()
	createCommitted := make(chan struct{})
	releaseCreate := make(chan struct{})
	base.createCommitted = createCommitted
	base.releaseCreate = releaseCreate
	exec, err := NewInodeMappingExecutor(base)
	if err != nil {
		t.Fatalf("NewInodeMappingExecutor: %v", err)
	}

	type createResult struct {
		result fsmeta.CreateResult
		err    error
	}
	done := make(chan createResult, 1)
	go func() {
		result, err := exec.Create(withPlannedCreateInode(context.Background(), 10), fsmeta.CreateRequest{
			Mount:  "vol",
			Parent: fsmeta.RootInode,
			Name:   "alpha",
			Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile, Mode: 0o644},
		})
		done <- createResult{result: result, err: err}
	}()
	<-createCommitted

	pairs, err := exec.ReadDirPlus(context.Background(), fsmeta.ReadDirRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Limit:  16,
	})
	if err != nil {
		t.Fatalf("ReadDirPlus: %v", err)
	}
	if len(pairs) != 1 {
		t.Fatalf("ReadDirPlus returned %d pairs, want 1: %#v", len(pairs), pairs)
	}
	if pairs[0].Dentry.Inode != 10 || pairs[0].Inode.Inode != 10 {
		t.Fatalf("pending create was not translated to planned inode: %#v", pairs[0])
	}

	close(releaseCreate)
	created := <-done
	if created.err != nil {
		t.Fatalf("Create: %v", created.err)
	}
	if created.result.Dentry.Inode != 10 || created.result.Inode.Inode != 10 {
		t.Fatalf("Create result was not translated to planned inode: %+v", created.result)
	}
}

func TestInodeMappingExecutorUpdatesCreateObservedBeforeReturn(t *testing.T) {
	base := newFakeExternalExecutor()
	createCommitted := make(chan struct{})
	releaseCreate := make(chan struct{})
	base.createCommitted = createCommitted
	base.releaseCreate = releaseCreate
	exec, err := NewInodeMappingExecutor(base)
	if err != nil {
		t.Fatalf("NewInodeMappingExecutor: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := exec.Create(withPlannedCreateInode(context.Background(), 10), fsmeta.CreateRequest{
			Mount:  "vol",
			Parent: fsmeta.RootInode,
			Name:   "alpha",
			Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile, Mode: 0o644},
		})
		done <- err
	}()
	<-createCommitted

	updated, err := exec.UpdateInode(context.Background(), fsmeta.UpdateInodeRequest{
		Mount:   "vol",
		Parent:  fsmeta.RootInode,
		Name:    "alpha",
		Inode:   10,
		SetSize: true,
		Size:    128,
	})
	if err != nil {
		t.Fatalf("UpdateInode: %v", err)
	}
	if updated.Inode != 10 {
		t.Fatalf("UpdateInode returned inode %d, want planned inode 10", updated.Inode)
	}
	if base.lastUpdated == 10 {
		t.Fatalf("UpdateInode reached base with planned inode instead of actual inode")
	}

	close(releaseCreate)
	if err := <-done; err != nil {
		t.Fatalf("Create: %v", err)
	}
}

func TestInodeMappingExecutorTranslatesRenameReplaceResult(t *testing.T) {
	base := newFakeExternalExecutor()
	exec, err := NewInodeMappingExecutor(base)
	if err != nil {
		t.Fatalf("NewInodeMappingExecutor: %v", err)
	}
	_, err = exec.Create(withPlannedCreateInode(context.Background(), 10), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "old",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile, Mode: 0o644},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	actualDentry, err := base.Lookup(context.Background(), fsmeta.LookupRequest{Mount: "vol", Parent: fsmeta.RootInode, Name: "old"})
	if err != nil {
		t.Fatalf("base Lookup: %v", err)
	}
	base.renameReplaceResult = fsmeta.RenameReplaceResult{
		Replaced: true,
		OldDentry: fsmeta.DentryRecord{
			Parent: fsmeta.RootInode,
			Name:   "old",
			Inode:  actualDentry.Inode,
			Type:   fsmeta.InodeTypeFile,
		},
		OldInode: fsmeta.InodeRecord{
			Inode:     actualDentry.Inode,
			Type:      fsmeta.InodeTypeFile,
			LinkCount: 1,
		},
		OldInodeDeleted: true,
	}

	result, err := exec.RenameReplace(context.Background(), fsmeta.RenameReplaceRequest{
		Mount:      "vol",
		FromParent: fsmeta.RootInode,
		FromName:   "stage",
		ToParent:   fsmeta.RootInode,
		ToName:     "old",
	})
	if err != nil {
		t.Fatalf("RenameReplace: %v", err)
	}
	if result.OldDentry.Inode != 10 || result.OldInode.Inode != 10 {
		t.Fatalf("RenameReplace result was not translated to planned inode: %+v", result)
	}
}

type fakeExternalExecutor struct {
	mu                   sync.Mutex
	next                 fsmeta.InodeID
	dentries             map[[2]any]fsmeta.DentryRecord
	inodes               map[fsmeta.InodeID]fsmeta.InodeRecord
	lastUpdated          fsmeta.InodeID
	createErrAfterCommit error
	createCommitted      chan struct{}
	releaseCreate        chan struct{}
	renameReplaceResult  fsmeta.RenameReplaceResult
}

func newFakeExternalExecutor() *fakeExternalExecutor {
	f := &fakeExternalExecutor{
		next:     1000,
		dentries: make(map[[2]any]fsmeta.DentryRecord),
		inodes:   map[fsmeta.InodeID]fsmeta.InodeRecord{fsmeta.RootInode: {Inode: fsmeta.RootInode, Type: fsmeta.InodeTypeDirectory, LinkCount: 1}},
	}
	return f
}

func (f *fakeExternalExecutor) Create(_ context.Context, req fsmeta.CreateRequest) (fsmeta.CreateResult, error) {
	f.mu.Lock()
	key := [2]any{req.Parent, req.Name}
	if _, ok := f.dentries[key]; ok {
		f.mu.Unlock()
		return fsmeta.CreateResult{}, fsmeta.ErrExists
	}
	inodeID := f.next
	f.next++
	inode := req.Attrs.InodeRecord(inodeID)
	dentry := fsmeta.DentryRecord{Parent: req.Parent, Name: req.Name, Inode: inodeID, Type: req.Attrs.Type}
	f.dentries[key] = dentry
	f.inodes[inodeID] = inode
	if f.createCommitted != nil {
		close(f.createCommitted)
		f.createCommitted = nil
	}
	release := f.releaseCreate
	errAfterCommit := f.createErrAfterCommit
	f.mu.Unlock()
	if release != nil {
		<-release
	}
	if errAfterCommit != nil {
		return fsmeta.CreateResult{}, errAfterCommit
	}
	return fsmeta.CreateResult{Dentry: dentry, Inode: inode}, nil
}

func (f *fakeExternalExecutor) UpdateInode(_ context.Context, req fsmeta.UpdateInodeRequest) (fsmeta.InodeRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	dentry, ok := f.dentries[[2]any{req.Parent, req.Name}]
	if !ok || dentry.Inode != req.Inode {
		return fsmeta.InodeRecord{}, fsmeta.ErrNotFound
	}
	inode := f.inodes[req.Inode]
	inode.Size = req.Size
	inode.Mode = req.Mode
	f.inodes[req.Inode] = inode
	f.lastUpdated = req.Inode
	return inode, nil
}

func (f *fakeExternalExecutor) Lookup(_ context.Context, req fsmeta.LookupRequest) (fsmeta.DentryRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	dentry, ok := f.dentries[[2]any{req.Parent, req.Name}]
	if !ok {
		return fsmeta.DentryRecord{}, fsmeta.ErrNotFound
	}
	return dentry, nil
}

func (f *fakeExternalExecutor) ReadDirPlus(_ context.Context, req fsmeta.ReadDirRequest) ([]fsmeta.DentryAttrPair, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []fsmeta.DentryAttrPair
	for key, dentry := range f.dentries {
		if key[0] != req.Parent {
			continue
		}
		out = append(out, fsmeta.DentryAttrPair{Dentry: dentry, Inode: f.inodes[dentry.Inode]})
	}
	return out, nil
}

func (f *fakeExternalExecutor) SnapshotSubtree(_ context.Context, req fsmeta.SnapshotSubtreeRequest) (fsmeta.SnapshotSubtreeToken, error) {
	return fsmeta.SnapshotSubtreeToken{Mount: req.Mount, RootInode: req.RootInode, ReadVersion: 1}, nil
}

func (f *fakeExternalExecutor) Rename(context.Context, fsmeta.RenameRequest) error {
	return nil
}

func (f *fakeExternalExecutor) RenameReplace(context.Context, fsmeta.RenameReplaceRequest) (fsmeta.RenameReplaceResult, error) {
	return f.renameReplaceResult, nil
}

func (f *fakeExternalExecutor) RenameSubtree(context.Context, fsmeta.RenameSubtreeRequest) error {
	return nil
}

func (f *fakeExternalExecutor) Link(context.Context, fsmeta.LinkRequest) error {
	return nil
}

func (f *fakeExternalExecutor) Unlink(context.Context, fsmeta.UnlinkRequest) error {
	return nil
}

func (f *fakeExternalExecutor) Remove(context.Context, fsmeta.RemoveRequest) error {
	return nil
}

func (f *fakeExternalExecutor) OpenWriteSession(context.Context, fsmeta.OpenWriteSessionRequest) (fsmeta.SessionRecord, error) {
	return fsmeta.SessionRecord{}, errors.New("not implemented")
}

func (f *fakeExternalExecutor) HeartbeatWriteSession(context.Context, fsmeta.HeartbeatWriteSessionRequest) (fsmeta.SessionRecord, error) {
	return fsmeta.SessionRecord{}, errors.New("not implemented")
}

func (f *fakeExternalExecutor) CloseWriteSession(context.Context, fsmeta.CloseWriteSessionRequest) error {
	return errors.New("not implemented")
}

func (f *fakeExternalExecutor) ExpireWriteSessions(context.Context, fsmeta.ExpireWriteSessionsRequest) (fsmeta.ExpireWriteSessionsResult, error) {
	return fsmeta.ExpireWriteSessionsResult{}, nil
}
