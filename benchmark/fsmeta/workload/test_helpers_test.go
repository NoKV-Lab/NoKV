// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package workload

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	fsmetaclient "github.com/feichai0017/NoKV/fsmeta/client"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/feichai0017/NoKV/fsmeta/observe"
)

type fakeMetadataClient struct {
	mu        sync.Mutex
	dentries  map[string]model.DentryRecord
	inodes    map[model.InodeID]model.InodeRecord
	sessions  map[fakeSessionKey]model.SessionRecord
	snapshots map[uint64]model.SnapshotSubtreeToken
	reads     []model.ReadDirRequest
	next      model.InodeID
	version   uint64
	streams   []*fakeWatchStream
}

type fakeSessionKey struct {
	inode   model.InodeID
	session model.SessionID
}

func newFakeMetadataClient() *fakeMetadataClient {
	return &fakeMetadataClient{
		dentries:  make(map[string]model.DentryRecord),
		inodes:    make(map[model.InodeID]model.InodeRecord),
		sessions:  make(map[fakeSessionKey]model.SessionRecord),
		snapshots: make(map[uint64]model.SnapshotSubtreeToken),
		next:      100,
		version:   1,
	}
}

func (c *fakeMetadataClient) Create(_ context.Context, req model.CreateRequest) (model.CreateResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := dentryID(req.Parent, req.Name)
	if _, ok := c.dentries[id]; ok {
		return model.CreateResult{}, model.ErrExists
	}
	inode := req.Attrs.InodeRecord(c.next)
	c.next++
	dentry := model.DentryRecord{
		Parent: req.Parent,
		Name:   req.Name,
		Inode:  inode.Inode,
		Type:   inode.Type,
	}
	c.inodes[inode.Inode] = inode
	c.dentries[id] = dentry
	c.emitDentryEventLocked(req.Mount, dentry)
	return model.CreateResult{Dentry: dentry, Inode: inode}, nil
}

func (c *fakeMetadataClient) UpdateInode(_ context.Context, req model.UpdateInodeRequest) (model.InodeRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.dentries[dentryID(req.Parent, req.Name)]
	if !ok || entry.Inode != req.Inode {
		return model.InodeRecord{}, model.ErrNotFound
	}
	inode, ok := c.inodes[req.Inode]
	if !ok {
		return model.InodeRecord{}, model.ErrNotFound
	}
	if req.SetSize {
		inode.Size = req.Size
	}
	if req.SetMode {
		inode.Mode = req.Mode
	}
	if req.SetUpdatedUnixNs {
		inode.UpdatedUnixNs = req.UpdatedUnixNs
	}
	if req.SetOpaqueAttrs {
		inode.OpaqueAttrs = append([]byte(nil), req.OpaqueAttrs...)
	}
	c.inodes[req.Inode] = inode
	return inode, nil
}

func (c *fakeMetadataClient) Lookup(_ context.Context, req model.LookupRequest) (model.DentryRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.dentries[dentryID(req.Parent, req.Name)]
	if !ok {
		return model.DentryRecord{}, model.ErrNotFound
	}
	return entry, nil
}

func (c *fakeMetadataClient) LookupPlus(_ context.Context, req model.LookupRequest) (model.DentryAttrPair, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.dentries[dentryID(req.Parent, req.Name)]
	if !ok {
		return model.DentryAttrPair{}, model.ErrNotFound
	}
	inode, ok := c.inodes[entry.Inode]
	if !ok {
		return model.DentryAttrPair{}, model.ErrNotFound
	}
	return model.DentryAttrPair{Dentry: entry, Inode: inode}, nil
}

func (c *fakeMetadataClient) ReadDir(_ context.Context, req model.ReadDirRequest) ([]model.DentryRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reads = append(c.reads, req)
	return c.readDirLocked(req), nil
}

func (c *fakeMetadataClient) ReadDirPlus(_ context.Context, req model.ReadDirRequest) ([]model.DentryAttrPair, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reads = append(c.reads, req)
	entries := c.readDirLocked(req)
	out := make([]model.DentryAttrPair, 0, len(entries))
	for _, entry := range entries {
		inode, ok := c.inodes[entry.Inode]
		if !ok {
			return nil, model.ErrNotFound
		}
		out = append(out, model.DentryAttrPair{Dentry: entry, Inode: inode})
	}
	return out, nil
}

func (c *fakeMetadataClient) readDirLocked(req model.ReadDirRequest) []model.DentryRecord {
	out := make([]model.DentryRecord, 0)
	for _, entry := range c.dentries {
		if entry.Parent != req.Parent {
			continue
		}
		if req.StartAfter != "" && entry.Name <= req.StartAfter {
			continue
		}
		out = append(out, entry)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	if req.Limit > 0 && len(out) > int(req.Limit) {
		out = out[:req.Limit]
	}
	return out
}

func (c *fakeMetadataClient) WatchSubtree(_ context.Context, req observe.WatchRequest) (fsmetaclient.WatchSubscription, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	prefix, err := observe.WatchPrefixForMount(req, testMountIdentity(req.Mount))
	if err != nil {
		return nil, err
	}
	size := int(req.BackPressureWindow)
	if size <= 0 {
		size = 4096
	}
	stream := newFakeWatchStream(size)
	stream.prefix = prefix
	c.streams = append(c.streams, stream)
	return stream, nil
}

func (c *fakeMetadataClient) GetReadVersion(context.Context, model.ReadVersionRequest) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.version++
	return c.version, nil
}

func (c *fakeMetadataClient) SnapshotSubtree(_ context.Context, req model.SnapshotSubtreeRequest) (model.SnapshotSubtreeToken, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.version++
	token := model.SnapshotSubtreeToken{Mount: req.Mount, RootInode: req.RootInode, ReadVersion: c.version}
	c.snapshots[token.ReadVersion] = token
	return token, nil
}

func (c *fakeMetadataClient) RetireSnapshotSubtree(_ context.Context, token model.SnapshotSubtreeToken) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.snapshots, token.ReadVersion)
	return nil
}

func (c *fakeMetadataClient) Rename(_ context.Context, req model.RenameRequest) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	fromID := dentryID(req.FromParent, req.FromName)
	toID := dentryID(req.ToParent, req.ToName)
	entry, ok := c.dentries[fromID]
	if !ok {
		return model.ErrNotFound
	}
	if _, exists := c.dentries[toID]; exists {
		return model.ErrExists
	}
	delete(c.dentries, fromID)
	entry.Parent = req.ToParent
	entry.Name = req.ToName
	c.dentries[toID] = entry
	c.emitDentryEventLocked(req.Mount, entry)
	return nil
}

func (c *fakeMetadataClient) Unlink(_ context.Context, req model.UnlinkRequest) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := dentryID(req.Parent, req.Name)
	entry, ok := c.dentries[id]
	if !ok {
		return model.ErrNotFound
	}
	delete(c.dentries, id)
	inode, ok := c.inodes[entry.Inode]
	if !ok {
		return nil
	}
	if inode.LinkCount <= 1 {
		delete(c.inodes, entry.Inode)
		return nil
	}
	inode.LinkCount--
	c.inodes[entry.Inode] = inode
	return nil
}

func (c *fakeMetadataClient) OpenWriteSession(_ context.Context, req model.OpenWriteSessionRequest) (model.SessionRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.inodes[req.Inode]; !ok {
		return model.SessionRecord{}, model.ErrNotFound
	}
	record := model.SessionRecord{Session: req.Session, Inode: req.Inode, ExpiresUnixNs: time.Now().Add(req.TTL).UnixNano()}
	c.sessions[fakeSessionKey{inode: req.Inode, session: req.Session}] = record
	return record, nil
}

func (c *fakeMetadataClient) HeartbeatWriteSession(_ context.Context, req model.HeartbeatWriteSessionRequest) (model.SessionRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := fakeSessionKey{inode: req.Inode, session: req.Session}
	record, ok := c.sessions[key]
	if !ok || record.Inode != req.Inode {
		return model.SessionRecord{}, model.ErrNotFound
	}
	record.ExpiresUnixNs = time.Now().Add(req.TTL).UnixNano()
	c.sessions[key] = record
	return record, nil
}

func (c *fakeMetadataClient) CloseWriteSession(_ context.Context, req model.CloseWriteSessionRequest) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := fakeSessionKey{inode: req.Inode, session: req.Session}
	if _, ok := c.sessions[key]; !ok {
		return model.ErrNotFound
	}
	delete(c.sessions, key)
	return nil
}

func (c *fakeMetadataClient) ExpireWriteSessions(_ context.Context, req model.ExpireWriteSessionsRequest) (model.ExpireWriteSessionsResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now().UnixNano()
	limit := req.Limit
	if limit == 0 {
		limit = model.DefaultSessionExpireLimit
	}
	var expired uint64
	for id, session := range c.sessions {
		if expired >= uint64(limit) {
			break
		}
		if session.ExpiresUnixNs <= now {
			delete(c.sessions, id)
			expired++
		}
	}
	return model.ExpireWriteSessionsResult{Expired: expired}, nil
}

func (c *fakeMetadataClient) emitDentryEventLocked(mount model.MountID, entry model.DentryRecord) {
	if len(c.streams) == 0 {
		return
	}
	key, err := layout.EncodeDentryKey(testMountIdentity(mount), entry.Parent, entry.Name)
	if err != nil {
		return
	}
	c.version++
	evt := observe.WatchEvent{
		Cursor:        observe.WatchCursor{RegionID: 1, Term: 1, Index: c.version},
		CommitVersion: c.version,
		Source:        observe.WatchEventSourceCommit,
		Key:           key,
	}
	for _, stream := range c.streams {
		stream.mu.Lock()
		if !stream.closed && bytes.HasPrefix(key, stream.prefix) {
			stream.events <- evt
		}
		stream.mu.Unlock()
	}
}

type fakeWatchStream struct {
	mu     sync.Mutex
	prefix []byte
	events chan observe.WatchEvent
	closed bool
}

func newFakeWatchStream(size int) *fakeWatchStream {
	return &fakeWatchStream{events: make(chan observe.WatchEvent, size)}
}

func (s *fakeWatchStream) Recv() (observe.WatchEvent, error) {
	evt, ok := <-s.events
	if !ok {
		return observe.WatchEvent{}, io.EOF
	}
	return evt, nil
}

func (s *fakeWatchStream) ReadyCursor() observe.WatchCursor {
	return observe.WatchCursor{}
}

func (s *fakeWatchStream) Ack(observe.WatchCursor) error {
	return nil
}

func (s *fakeWatchStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		close(s.events)
		s.closed = true
	}
	return nil
}

func dentryID(parent model.InodeID, name string) string {
	return fmt.Sprintf("%d/%s", parent, name)
}

func testMountIdentity(mount model.MountID) model.MountIdentity {
	return model.MountIdentity{MountID: mount, MountKeyID: 1}
}
