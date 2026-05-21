// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package contract

import (
	"context"
	"errors"
	"sync"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/fsmeta"
)

// NewInodeMappingExecutor adapts an external fsmeta service to the contract
// harness. Generated histories use deterministic inode ids so later operations
// can refer to objects before the real service allocates storage ids; the
// adapter translates those planned ids to server-assigned ids at the API
// boundary and translates returned records back before the model checker sees
// them.
func NewInodeMappingExecutor(base Executor) (Executor, error) {
	if base == nil {
		return nil, errMappingRequired
	}
	m := &inodeMappingExecutor{
		base:            base,
		plannedToActual: make(map[fsmeta.InodeID]fsmeta.InodeID),
		actualToPlanned: make(map[fsmeta.InodeID]fsmeta.InodeID),
		pendingCreates:  make(map[dentryKey]fsmeta.InodeID),
	}
	m.rememberLocked(fsmeta.RootInode, fsmeta.RootInode)
	return m, nil
}

type inodeMappingExecutor struct {
	base Executor

	mu              sync.RWMutex
	plannedToActual map[fsmeta.InodeID]fsmeta.InodeID
	actualToPlanned map[fsmeta.InodeID]fsmeta.InodeID
	pendingCreates  map[dentryKey]fsmeta.InodeID
}

func (m *inodeMappingExecutor) Create(ctx context.Context, req fsmeta.CreateRequest) (fsmeta.CreateResult, error) {
	planned, hasPlanned := plannedCreateInode(ctx)
	req.Parent = m.actualInode(req.Parent)
	pendingKey := dentryKey{parent: req.Parent, name: req.Name}
	if hasPlanned {
		m.rememberPendingCreate(pendingKey, planned)
	}
	result, err := m.base.Create(ctx, req)
	if err != nil {
		if hasPlanned && createOutcomeAmbiguous(err) {
			if recovered, ok := m.recoverCreate(ctx, req, planned); ok {
				m.clearPendingCreate(planned)
				return recovered, nil
			}
		}
		if hasPlanned {
			m.clearPendingCreate(planned)
		}
		return fsmeta.CreateResult{}, err
	}
	if !hasPlanned {
		planned = result.Inode.Inode
	}
	m.remember(planned, result.Inode.Inode)
	if hasPlanned {
		m.clearPendingCreate(planned)
	}
	return m.translateCreateResult(result), nil
}

func createOutcomeAmbiguous(err error) bool {
	return nokverrors.Retryable(err)
}

func (m *inodeMappingExecutor) UpdateInode(ctx context.Context, req fsmeta.UpdateInodeRequest) (fsmeta.InodeRecord, error) {
	planned := req.Inode
	req.Parent = m.actualInode(req.Parent)
	if actual, ok := m.resolvePendingCreateActual(ctx, req.Mount, req.Parent, req.Name, planned); ok {
		req.Inode = actual
	} else {
		req.Inode = m.actualInode(planned)
	}
	record, err := m.base.UpdateInode(ctx, req)
	if err != nil {
		if (errors.Is(err, fsmeta.ErrInvalidRequest) || errors.Is(err, fsmeta.ErrNotFound)) && planned != 0 {
			if actual, ok := m.resolvePendingCreateActual(ctx, req.Mount, req.Parent, req.Name, planned); ok && actual != req.Inode {
				req.Inode = actual
				record, err = m.base.UpdateInode(ctx, req)
				if err == nil {
					return m.translateInodeRecord(record), nil
				}
			}
		}
		return fsmeta.InodeRecord{}, err
	}
	return m.translateInodeRecord(record), nil
}

func (m *inodeMappingExecutor) Lookup(ctx context.Context, req fsmeta.LookupRequest) (fsmeta.DentryRecord, error) {
	req.Parent = m.actualInode(req.Parent)
	record, err := m.base.Lookup(ctx, req)
	if err != nil {
		return fsmeta.DentryRecord{}, err
	}
	return m.translateDentryRecord(record), nil
}

func (m *inodeMappingExecutor) ReadDirPlus(ctx context.Context, req fsmeta.ReadDirRequest) ([]fsmeta.DentryAttrPair, error) {
	req.Parent = m.actualInode(req.Parent)
	pairs, err := m.base.ReadDirPlus(ctx, req)
	if err != nil {
		return nil, err
	}
	out := make([]fsmeta.DentryAttrPair, len(pairs))
	for i, pair := range pairs {
		out[i] = fsmeta.DentryAttrPair{
			Dentry: m.translateDentryRecord(pair.Dentry),
			Inode:  m.translateInodeRecord(pair.Inode),
		}
	}
	return out, nil
}

func (m *inodeMappingExecutor) SnapshotSubtree(ctx context.Context, req fsmeta.SnapshotSubtreeRequest) (fsmeta.SnapshotSubtreeToken, error) {
	req.RootInode = m.actualInode(req.RootInode)
	token, err := m.base.SnapshotSubtree(ctx, req)
	if err != nil {
		return fsmeta.SnapshotSubtreeToken{}, err
	}
	token.RootInode = m.plannedInode(token.RootInode)
	return token, nil
}

func (m *inodeMappingExecutor) Rename(ctx context.Context, req fsmeta.RenameRequest) error {
	req.FromParent = m.actualInode(req.FromParent)
	req.ToParent = m.actualInode(req.ToParent)
	return m.base.Rename(ctx, req)
}

func (m *inodeMappingExecutor) RenameReplace(ctx context.Context, req fsmeta.RenameReplaceRequest) (fsmeta.RenameReplaceResult, error) {
	req.FromParent = m.actualInode(req.FromParent)
	req.ToParent = m.actualInode(req.ToParent)
	result, err := m.base.RenameReplace(ctx, req)
	if err != nil {
		return fsmeta.RenameReplaceResult{}, err
	}
	if result.Replaced {
		result.OldDentry = m.translateDentryRecord(result.OldDentry)
		result.OldInode = m.translateInodeRecord(result.OldInode)
	}
	return result, nil
}

func (m *inodeMappingExecutor) RenameSubtree(ctx context.Context, req fsmeta.RenameSubtreeRequest) error {
	req.FromParent = m.actualInode(req.FromParent)
	req.ToParent = m.actualInode(req.ToParent)
	return m.base.RenameSubtree(ctx, req)
}

func (m *inodeMappingExecutor) Link(ctx context.Context, req fsmeta.LinkRequest) error {
	req.FromParent = m.actualInode(req.FromParent)
	req.ToParent = m.actualInode(req.ToParent)
	return m.base.Link(ctx, req)
}

func (m *inodeMappingExecutor) Unlink(ctx context.Context, req fsmeta.UnlinkRequest) error {
	req.Parent = m.actualInode(req.Parent)
	return m.base.Unlink(ctx, req)
}

func (m *inodeMappingExecutor) Remove(ctx context.Context, req fsmeta.RemoveRequest) (fsmeta.RemoveResult, error) {
	req.Parent = m.actualInode(req.Parent)
	result, err := m.base.Remove(ctx, req)
	if err != nil {
		return fsmeta.RemoveResult{}, err
	}
	result.RemovedDentry = m.translateDentryRecord(result.RemovedDentry)
	result.OldInode = m.translateInodeRecord(result.OldInode)
	return result, nil
}

func (m *inodeMappingExecutor) OpenWriteSession(ctx context.Context, req fsmeta.OpenWriteSessionRequest) (fsmeta.SessionRecord, error) {
	req.Inode = m.actualInode(req.Inode)
	record, err := m.base.OpenWriteSession(ctx, req)
	if err != nil {
		return fsmeta.SessionRecord{}, err
	}
	return m.translateSessionRecord(record), nil
}

func (m *inodeMappingExecutor) HeartbeatWriteSession(ctx context.Context, req fsmeta.HeartbeatWriteSessionRequest) (fsmeta.SessionRecord, error) {
	req.Inode = m.actualInode(req.Inode)
	record, err := m.base.HeartbeatWriteSession(ctx, req)
	if err != nil {
		return fsmeta.SessionRecord{}, err
	}
	return m.translateSessionRecord(record), nil
}

func (m *inodeMappingExecutor) CloseWriteSession(ctx context.Context, req fsmeta.CloseWriteSessionRequest) error {
	req.Inode = m.actualInode(req.Inode)
	return m.base.CloseWriteSession(ctx, req)
}

// forwarding-ok: inode-remap adapter passes ExpireWriteSessions through unchanged (no inode rewrite needed).
func (m *inodeMappingExecutor) ExpireWriteSessions(ctx context.Context, req fsmeta.ExpireWriteSessionsRequest) (fsmeta.ExpireWriteSessionsResult, error) {
	return m.base.ExpireWriteSessions(ctx, req)
}

func (m *inodeMappingExecutor) recoverCreate(ctx context.Context, req fsmeta.CreateRequest, planned fsmeta.InodeID) (fsmeta.CreateResult, bool) {
	dentry, err := m.base.Lookup(ctx, fsmeta.LookupRequest{
		Mount:  req.Mount,
		Parent: req.Parent,
		Name:   req.Name,
	})
	if err != nil {
		return fsmeta.CreateResult{}, false
	}
	m.remember(planned, dentry.Inode)
	return m.translateCreateResult(fsmeta.CreateResult{
		Dentry: dentry,
		Inode:  req.Attrs.InodeRecord(dentry.Inode),
	}), true
}

func (m *inodeMappingExecutor) translateCreateResult(result fsmeta.CreateResult) fsmeta.CreateResult {
	return fsmeta.CreateResult{
		Dentry: m.translateDentryRecord(result.Dentry),
		Inode:  m.translateInodeRecord(result.Inode),
	}
}

func (m *inodeMappingExecutor) translateDentryRecord(record fsmeta.DentryRecord) fsmeta.DentryRecord {
	actualParent := record.Parent
	record.Parent = m.plannedInode(actualParent)
	record.Inode = m.plannedInodeForDentry(actualParent, record.Name, record.Inode)
	return record
}

func (m *inodeMappingExecutor) translateInodeRecord(record fsmeta.InodeRecord) fsmeta.InodeRecord {
	record.Inode = m.plannedInode(record.Inode)
	return record
}

func (m *inodeMappingExecutor) translateSessionRecord(record fsmeta.SessionRecord) fsmeta.SessionRecord {
	record.Inode = m.plannedInode(record.Inode)
	return record
}

func (m *inodeMappingExecutor) actualInode(planned fsmeta.InodeID) fsmeta.InodeID {
	if planned == 0 {
		return 0
	}
	m.mu.RLock()
	actual, ok := m.plannedToActual[planned]
	m.mu.RUnlock()
	if ok {
		return actual
	}
	return planned
}

func (m *inodeMappingExecutor) resolvePendingCreateActual(ctx context.Context, mount fsmeta.MountID, parent fsmeta.InodeID, name string, planned fsmeta.InodeID) (fsmeta.InodeID, bool) {
	if planned == 0 || name == "" {
		return 0, false
	}
	if actual, ok := m.actualInodeIfKnown(planned); ok {
		return actual, true
	}
	key := dentryKey{parent: parent, name: name}
	if !m.pendingCreateMatches(key, planned) {
		return 0, false
	}
	dentry, err := m.base.Lookup(ctx, fsmeta.LookupRequest{
		Mount:  mount,
		Parent: parent,
		Name:   name,
	})
	if err != nil {
		return 0, false
	}
	m.remember(planned, dentry.Inode)
	return dentry.Inode, true
}

func (m *inodeMappingExecutor) actualInodeIfKnown(planned fsmeta.InodeID) (fsmeta.InodeID, bool) {
	if planned == 0 {
		return 0, false
	}
	m.mu.RLock()
	actual, ok := m.plannedToActual[planned]
	m.mu.RUnlock()
	return actual, ok
}

func (m *inodeMappingExecutor) pendingCreateMatches(key dentryKey, planned fsmeta.InodeID) bool {
	if planned == 0 {
		return false
	}
	m.mu.RLock()
	pending, ok := m.pendingCreates[key]
	m.mu.RUnlock()
	return ok && pending == planned
}

func (m *inodeMappingExecutor) plannedInode(actual fsmeta.InodeID) fsmeta.InodeID {
	if actual == 0 {
		return 0
	}
	m.mu.RLock()
	planned, ok := m.actualToPlanned[actual]
	m.mu.RUnlock()
	if ok {
		return planned
	}
	return actual
}

func (m *inodeMappingExecutor) plannedInodeForDentry(parent fsmeta.InodeID, name string, actual fsmeta.InodeID) fsmeta.InodeID {
	if actual == 0 {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if planned, ok := m.actualToPlanned[actual]; ok {
		return planned
	}
	if planned, ok := m.pendingCreates[dentryKey{parent: parent, name: name}]; ok {
		m.rememberLocked(planned, actual)
		return planned
	}
	return actual
}

func (m *inodeMappingExecutor) remember(planned, actual fsmeta.InodeID) {
	if planned == 0 || actual == 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rememberLocked(planned, actual)
}

func (m *inodeMappingExecutor) rememberLocked(planned, actual fsmeta.InodeID) {
	m.plannedToActual[planned] = actual
	m.actualToPlanned[actual] = planned
}

func (m *inodeMappingExecutor) rememberPendingCreate(key dentryKey, planned fsmeta.InodeID) {
	if planned == 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pendingCreates[key] = planned
}

func (m *inodeMappingExecutor) clearPendingCreate(planned fsmeta.InodeID) {
	if planned == 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for key, pending := range m.pendingCreates {
		if pending == planned {
			delete(m.pendingCreates, key)
		}
	}
}
