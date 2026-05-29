// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package local

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"

	"github.com/feichai0017/NoKV/fsmeta/backend"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/feichai0017/NoKV/txn/storage"
)

// SnapshotRegistry tracks locally published snapshot tokens. When constructed
// with OpenSnapshotRegistry, the active set is backed by hidden fsmeta MVCC
// records and can be recovered after restart.
type SnapshotRegistry struct {
	mu     sync.Mutex
	active map[localSnapshotKey]struct{}
	runner *Runner
	mount  model.MountIdentity

	publishTotal   atomic.Uint64
	retireTotal    atomic.Uint64
	recoveredTotal atomic.Uint64
}

type localSnapshotKey struct {
	mount       model.MountID
	mountKeyID  model.MountKeyID
	rootInode   model.InodeID
	readVersion uint64
}

// NewSnapshotRegistry constructs an empty local snapshot registry.
func NewSnapshotRegistry() *SnapshotRegistry {
	return &SnapshotRegistry{active: make(map[localSnapshotKey]struct{})}
}

// OpenSnapshotRegistry loads the persisted local snapshot-retention records
// for one mount.
func OpenSnapshotRegistry(ctx context.Context, runner *Runner, mount model.MountIdentity) (*SnapshotRegistry, error) {
	if runner == nil {
		return nil, errDBRequired
	}
	if _, err := layout.EncodeSnapshotPrefix(mount); err != nil {
		return nil, err
	}
	r := &SnapshotRegistry{
		active: make(map[localSnapshotKey]struct{}),
		runner: runner,
		mount:  mount,
	}
	if err := r.load(ctx); err != nil {
		return nil, err
	}
	return r, nil
}

// PublishSnapshotSubtree records a local snapshot token.
func (r *SnapshotRegistry) PublishSnapshotSubtree(ctx context.Context, token model.SnapshotSubtreeToken) error {
	if err := validateSnapshotToken(token); err != nil {
		return err
	}
	if r == nil {
		return nil
	}
	key := localSnapshotKeyFromToken(token)
	if r.hasActive(key) {
		r.publishTotal.Add(1)
		return nil
	}
	if err := r.validateRegistryMount(token); err != nil {
		return err
	}
	if r.runner != nil {
		storageKey, err := layout.EncodeSnapshotKey(r.mount, token.RootInode, token.ReadVersion)
		if err != nil {
			return err
		}
		value, err := layout.EncodeSnapshotValue(token)
		if err != nil {
			return err
		}
		if err := r.applySnapshotMutation(ctx, storageKey, &backend.Mutation{
			Op:    backend.MutationPut,
			Key:   storageKey,
			Value: value,
		}); err != nil {
			return err
		}
	}
	r.mu.Lock()
	if r.active == nil {
		r.active = make(map[localSnapshotKey]struct{})
	}
	r.active[key] = struct{}{}
	r.mu.Unlock()
	r.publishTotal.Add(1)
	return nil
}

// RetireSnapshotSubtree removes a local snapshot token when it is still known.
func (r *SnapshotRegistry) RetireSnapshotSubtree(ctx context.Context, token model.SnapshotSubtreeToken) error {
	if err := validateSnapshotToken(token); err != nil {
		return err
	}
	if r == nil {
		return nil
	}
	key := localSnapshotKeyFromToken(token)
	if !r.hasActive(key) {
		r.retireTotal.Add(1)
		return nil
	}
	if err := r.validateRegistryMount(token); err != nil {
		return err
	}
	if r.runner != nil {
		storageKey, err := layout.EncodeSnapshotKey(r.mount, token.RootInode, token.ReadVersion)
		if err != nil {
			return err
		}
		if err := r.applySnapshotMutation(ctx, storageKey, &backend.Mutation{
			Op:  backend.MutationDelete,
			Key: storageKey,
		}); err != nil {
			return err
		}
	}
	r.mu.Lock()
	delete(r.active, key)
	r.mu.Unlock()
	r.retireTotal.Add(1)
	return nil
}

// SnapshotRetentionFloor returns the oldest active local snapshot read version.
func (r *SnapshotRegistry) SnapshotRetentionFloor() (uint64, bool) {
	index := r.SnapshotRetentionIndex()
	return index.GlobalFloor, index.Active()
}

// SnapshotRetentionIndex returns the mount-scoped GC retention floors currently
// pinned by active local snapshot tokens.
func (r *SnapshotRegistry) SnapshotRetentionIndex() rootstate.SnapshotRetentionIndex {
	index := rootstate.SnapshotRetentionIndex{MountFloors: make(map[uint64]uint64)}
	if r == nil {
		return index
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for key := range r.active {
		if key.readVersion == 0 {
			continue
		}
		if index.GlobalFloor == 0 || key.readVersion < index.GlobalFloor {
			index.GlobalFloor = key.readVersion
		}
		mountKeyID := uint64(key.mountKeyID)
		if mountKeyID == 0 {
			continue
		}
		if current := index.MountFloors[mountKeyID]; current == 0 || key.readVersion < current {
			index.MountFloors[mountKeyID] = key.readVersion
		}
	}
	return index
}

// Stats returns local snapshot registry diagnostics.
func (r *SnapshotRegistry) Stats() map[string]any {
	if r == nil {
		return map[string]any{
			"active_snapshots":     0,
			"publish_total":        uint64(0),
			"retire_total":         uint64(0),
			"recovered_total":      uint64(0),
			"retention_floor":      uint64(0),
			"persistent":           false,
			"durability_authority": "memory",
		}
	}
	r.mu.Lock()
	active := len(r.active)
	floor, _ := r.snapshotRetentionFloorLocked()
	r.mu.Unlock()
	authority := "memory"
	if r.runner != nil {
		authority = "local_mvcc_snapshot_registry"
	}
	return map[string]any{
		"active_snapshots":     active,
		"publish_total":        r.publishTotal.Load(),
		"retire_total":         r.retireTotal.Load(),
		"recovered_total":      r.recoveredTotal.Load(),
		"retention_floor":      floor,
		"persistent":           r.runner != nil,
		"durability_authority": authority,
	}
}

func (r *SnapshotRegistry) load(ctx context.Context) error {
	prefix, err := layout.EncodeSnapshotPrefix(r.mount)
	if err != nil {
		return err
	}
	iter := r.runner.db.NewInternalIterator(&storage.Options{IsAsc: true})
	if iter == nil {
		return nil
	}
	defer func() { _ = iter.Close() }()
	var (
		loaded      uint64
		lastUserKey []byte
	)
	iter.Seek(storage.InternalKey(storage.CFWrite, prefix, storage.MaxVersion))
	for iter.Valid() {
		if err := ctxErr(ctx); err != nil {
			return err
		}
		item := iter.Item()
		if item == nil || item.Entry() == nil {
			iter.Next()
			continue
		}
		cf, userKey, _, ok := storage.SplitInternalKey(item.Entry().Key)
		if !ok {
			return errInvalidInternalEntry
		}
		if cf != storage.CFWrite || !bytes.HasPrefix(userKey, prefix) {
			break
		}
		if bytes.Equal(userKey, lastUserKey) {
			iter.Next()
			continue
		}
		lastUserKey = cloneBytes(userKey)
		token, ok, err := r.readSnapshotRecord(userKey)
		if err != nil {
			return err
		}
		if ok {
			r.active[localSnapshotKeyFromToken(token)] = struct{}{}
			loaded++
		}
		iter.Next()
	}
	r.recoveredTotal.Add(loaded)
	return nil
}

func (r *SnapshotRegistry) readSnapshotRecord(key []byte) (model.SnapshotSubtreeToken, bool, error) {
	parts, ok := layout.InspectKey(key)
	if !ok || parts.Kind != layout.KeyKindSnapshot {
		return model.SnapshotSubtreeToken{}, false, layout.ErrInvalidKey
	}
	value, ok, err := r.runner.readValue(key, storage.MaxVersion)
	if err != nil || !ok {
		return model.SnapshotSubtreeToken{}, ok, err
	}
	token, err := layout.DecodeSnapshotValue(value)
	if err != nil {
		return model.SnapshotSubtreeToken{}, false, err
	}
	if err := r.validateRegistryMount(token); err != nil {
		return model.SnapshotSubtreeToken{}, false, err
	}
	if token.RootInode != parts.SnapshotRoot || token.ReadVersion != parts.SnapshotReadVersion {
		return model.SnapshotSubtreeToken{}, false, model.ErrInvalidValue
	}
	return token, true, nil
}

func (r *SnapshotRegistry) applySnapshotMutation(ctx context.Context, primary []byte, mutation *backend.Mutation) error {
	startVersion, err := r.runner.ReserveTimestamp(ctx, 2)
	if err != nil {
		return err
	}
	_, _, err = r.runner.applyMutationGroup(primary, []*backend.Mutation{mutation}, startVersion, startVersion+1, false)
	return err
}

func (r *SnapshotRegistry) validateRegistryMount(token model.SnapshotSubtreeToken) error {
	if r == nil || r.runner == nil {
		return nil
	}
	if token.Mount != r.mount.MountID || token.MountKeyID != r.mount.MountKeyID {
		return model.ErrInvalidRequest
	}
	return nil
}

func (r *SnapshotRegistry) hasActive(key localSnapshotKey) bool {
	r.mu.Lock()
	_, ok := r.active[key]
	r.mu.Unlock()
	return ok
}

func (r *SnapshotRegistry) snapshotRetentionFloorLocked() (uint64, bool) {
	var floor uint64
	for key := range r.active {
		if key.readVersion == 0 {
			continue
		}
		if floor == 0 || key.readVersion < floor {
			floor = key.readVersion
		}
	}
	return floor, floor != 0
}

func validateSnapshotToken(token model.SnapshotSubtreeToken) error {
	if token.Mount == "" || token.MountKeyID == 0 || token.RootInode == 0 || token.ReadVersion == 0 {
		return model.ErrInvalidRequest
	}
	for _, ref := range token.RuntimeEvidence {
		if !ref.Valid() {
			return model.ErrInvalidRequest
		}
	}
	return nil
}

func localSnapshotKeyFromToken(token model.SnapshotSubtreeToken) localSnapshotKey {
	return localSnapshotKey{
		mount:       token.Mount,
		mountKeyID:  token.MountKeyID,
		rootInode:   token.RootInode,
		readVersion: token.ReadVersion,
	}
}
