// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package contract

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta/model"
)

func TestInodeMappingExecutorTranslatesGeneratedHistoryIDs(t *testing.T) {
	base := newFakeExternalExecutor()
	exec, err := NewInodeMappingExecutor(base)
	if err != nil {
		t.Fatalf("NewInodeMappingExecutor: %v", err)
	}
	state := NewModel("vol")
	ops := []Operation{
		{Kind: OpCreate, Mount: "vol", Parent: model.RootInode, Name: "alpha", Inode: 10, Type: model.InodeTypeFile, Mode: 0o644},
		{Kind: OpUpdateInode, Mount: "vol", Parent: model.RootInode, Name: "alpha", Inode: 10, Size: 42, Mode: 0o600},
		{Kind: OpReadDirPlus, Mount: "vol", Parent: model.RootInode, Limit: 16},
	}
	if err := Run(context.Background(), exec, state, ops); err != nil {
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
	result, err := exec.Create(withPlannedCreateInode(context.Background(), 10), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "alpha",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile, Mode: 0o644},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if result.Dentry.Inode != 10 || result.Inode.Inode != 10 {
		t.Fatalf("result was not translated to planned inode: %+v", result)
	}
}

func TestInodeMappingExecutorTranslatesRemoveResult(t *testing.T) {
	base := newFakeExternalExecutor()
	exec, err := NewInodeMappingExecutor(base)
	if err != nil {
		t.Fatalf("NewInodeMappingExecutor: %v", err)
	}
	_, err = exec.Create(withPlannedCreateInode(context.Background(), 10), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "alpha",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile, Mode: 0o644},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	result, err := exec.Remove(context.Background(), model.RemoveRequest{Mount: "vol", Parent: model.RootInode, Name: "alpha"})
	if err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if result.RemovedDentry.Inode != 10 || result.OldInode.Inode != 10 {
		t.Fatalf("remove result was not translated to planned inode: %+v", result)
	}
	if !result.InodeDeleted {
		t.Fatalf("Remove result did not report deleted inode: %+v", result)
	}
}

func TestInodeMappingExecutorDoesNotRecoverSemanticCreateError(t *testing.T) {
	base := newFakeExternalExecutor()
	_, err := base.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "alpha",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeDirectory, Mode: 0o755},
	})
	if err != nil {
		t.Fatalf("seed Create: %v", err)
	}
	exec, err := NewInodeMappingExecutor(base)
	if err != nil {
		t.Fatalf("NewInodeMappingExecutor: %v", err)
	}
	_, err = exec.Create(withPlannedCreateInode(context.Background(), 10), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "alpha",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile, Mode: 0o644},
	})
	if !errors.Is(err, model.ErrExists) {
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
		result model.CreateResult
		err    error
	}
	done := make(chan createResult, 1)
	go func() {
		result, err := exec.Create(withPlannedCreateInode(context.Background(), 10), model.CreateRequest{
			Mount:  "vol",
			Parent: model.RootInode,
			Name:   "alpha",
			Attrs:  model.CreateAttrs{Type: model.InodeTypeFile, Mode: 0o644},
		})
		done <- createResult{result: result, err: err}
	}()
	<-createCommitted

	pairs, err := exec.ReadDirPlus(context.Background(), model.ReadDirRequest{
		Mount:  "vol",
		Parent: model.RootInode,
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
		_, err := exec.Create(withPlannedCreateInode(context.Background(), 10), model.CreateRequest{
			Mount:  "vol",
			Parent: model.RootInode,
			Name:   "alpha",
			Attrs:  model.CreateAttrs{Type: model.InodeTypeFile, Mode: 0o644},
		})
		done <- err
	}()
	<-createCommitted

	updated, err := exec.UpdateInode(context.Background(), model.UpdateInodeRequest{
		Mount:   "vol",
		Parent:  model.RootInode,
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
	_, err = exec.Create(withPlannedCreateInode(context.Background(), 10), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "old",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile, Mode: 0o644},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	actualDentry, err := base.Lookup(context.Background(), model.LookupRequest{Mount: "vol", Parent: model.RootInode, Name: "old"})
	if err != nil {
		t.Fatalf("base Lookup: %v", err)
	}
	base.renameReplaceResult = model.RenameReplaceResult{
		Replaced: true,
		OldDentry: model.DentryRecord{
			Parent: model.RootInode,
			Name:   "old",
			Inode:  actualDentry.Inode,
			Type:   model.InodeTypeFile,
		},
		OldInode: model.InodeRecord{
			Inode:     actualDentry.Inode,
			Type:      model.InodeTypeFile,
			LinkCount: 1,
		},
		OldInodeDeleted: true,
	}

	result, err := exec.RenameReplace(context.Background(), model.RenameReplaceRequest{
		Mount:      "vol",
		FromParent: model.RootInode,
		FromName:   "stage",
		ToParent:   model.RootInode,
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
	next                 model.InodeID
	dentries             map[[2]any]model.DentryRecord
	inodes               map[model.InodeID]model.InodeRecord
	lastUpdated          model.InodeID
	createErrAfterCommit error
	createCommitted      chan struct{}
	releaseCreate        chan struct{}
	renameReplaceResult  model.RenameReplaceResult
}

func newFakeExternalExecutor() *fakeExternalExecutor {
	f := &fakeExternalExecutor{
		next:     1000,
		dentries: make(map[[2]any]model.DentryRecord),
		inodes:   map[model.InodeID]model.InodeRecord{model.RootInode: {Inode: model.RootInode, Type: model.InodeTypeDirectory, LinkCount: 1}},
	}
	return f
}

func (f *fakeExternalExecutor) Create(_ context.Context, req model.CreateRequest) (model.CreateResult, error) {
	f.mu.Lock()
	key := [2]any{req.Parent, req.Name}
	if _, ok := f.dentries[key]; ok {
		f.mu.Unlock()
		return model.CreateResult{}, model.ErrExists
	}
	inodeID := f.next
	f.next++
	inode := req.Attrs.InodeRecord(inodeID)
	dentry := model.DentryRecord{Parent: req.Parent, Name: req.Name, Inode: inodeID, Type: req.Attrs.Type}
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
		return model.CreateResult{}, errAfterCommit
	}
	return model.CreateResult{Dentry: dentry, Inode: inode}, nil
}

func (f *fakeExternalExecutor) UpdateInode(_ context.Context, req model.UpdateInodeRequest) (model.InodeRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	dentry, ok := f.dentries[[2]any{req.Parent, req.Name}]
	if !ok || dentry.Inode != req.Inode {
		return model.InodeRecord{}, model.ErrNotFound
	}
	inode := f.inodes[req.Inode]
	inode.Size = req.Size
	inode.Mode = req.Mode
	f.inodes[req.Inode] = inode
	f.lastUpdated = req.Inode
	return inode, nil
}

func (f *fakeExternalExecutor) Lookup(_ context.Context, req model.LookupRequest) (model.DentryRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	dentry, ok := f.dentries[[2]any{req.Parent, req.Name}]
	if !ok {
		return model.DentryRecord{}, model.ErrNotFound
	}
	return dentry, nil
}

func (f *fakeExternalExecutor) ReadDirPlus(_ context.Context, req model.ReadDirRequest) ([]model.DentryAttrPair, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []model.DentryAttrPair
	for key, dentry := range f.dentries {
		if key[0] != req.Parent {
			continue
		}
		out = append(out, model.DentryAttrPair{Dentry: dentry, Inode: f.inodes[dentry.Inode]})
	}
	return out, nil
}

func (f *fakeExternalExecutor) SnapshotSubtree(_ context.Context, req model.SnapshotSubtreeRequest) (model.SnapshotSubtreeToken, error) {
	return model.SnapshotSubtreeToken{Mount: req.Mount, RootInode: req.RootInode, ReadVersion: 1}, nil
}

func (f *fakeExternalExecutor) Rename(context.Context, model.RenameRequest) error {
	return nil
}

func (f *fakeExternalExecutor) RenameReplace(context.Context, model.RenameReplaceRequest) (model.RenameReplaceResult, error) {
	return f.renameReplaceResult, nil
}

func (f *fakeExternalExecutor) RenameSubtree(context.Context, model.RenameSubtreeRequest) error {
	return nil
}

func (f *fakeExternalExecutor) Link(context.Context, model.LinkRequest) error {
	return nil
}

func (f *fakeExternalExecutor) Unlink(context.Context, model.UnlinkRequest) error {
	return nil
}

func (f *fakeExternalExecutor) Remove(_ context.Context, req model.RemoveRequest) (model.RemoveResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := [2]any{req.Parent, req.Name}
	dentry, ok := f.dentries[key]
	if !ok {
		return model.RemoveResult{}, model.ErrNotFound
	}
	if dentry.Type == model.InodeTypeDirectory {
		return model.RemoveResult{}, model.ErrInvalidRequest
	}
	result := model.RemoveResult{RemovedDentry: dentry}
	inode, ok := f.inodes[dentry.Inode]
	if !ok {
		delete(f.dentries, key)
		return result, nil
	}
	if inode.Type == model.InodeTypeDirectory {
		return model.RemoveResult{}, model.ErrInvalidRequest
	}
	result.OldInode = inode
	delete(f.dentries, key)
	if inode.LinkCount <= 1 {
		result.InodeDeleted = true
		delete(f.inodes, inode.Inode)
	} else {
		inode.LinkCount--
		f.inodes[inode.Inode] = inode
	}
	return result, nil
}

func (f *fakeExternalExecutor) OpenWriteSession(context.Context, model.OpenWriteSessionRequest) (model.SessionRecord, error) {
	return model.SessionRecord{}, errors.New("not implemented")
}

func (f *fakeExternalExecutor) HeartbeatWriteSession(context.Context, model.HeartbeatWriteSessionRequest) (model.SessionRecord, error) {
	return model.SessionRecord{}, errors.New("not implemented")
}

func (f *fakeExternalExecutor) CloseWriteSession(context.Context, model.CloseWriteSessionRequest) error {
	return errors.New("not implemented")
}

func (f *fakeExternalExecutor) ExpireWriteSessions(context.Context, model.ExpireWriteSessionsRequest) (model.ExpireWriteSessionsResult, error) {
	return model.ExpireWriteSessionsResult{}, nil
}
