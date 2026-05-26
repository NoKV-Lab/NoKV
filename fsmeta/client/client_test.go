// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/model"
	fsmetaserver "github.com/feichai0017/NoKV/fsmeta/server"
	fsmetapb "github.com/feichai0017/NoKV/pb/fsmeta"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type fakeExecutor struct {
	err              error
	lookupCount      int
	readDirPlusCount int
	snapshotRefs     []model.SnapshotEvidenceRef
}

func (e *fakeExecutor) Create(_ context.Context, req model.CreateRequest) (model.CreateResult, error) {
	if e.err != nil {
		return model.CreateResult{}, e.err
	}
	inode := req.Attrs.InodeRecord(42)
	return model.CreateResult{
		Dentry: model.DentryRecord{Parent: req.Parent, Name: req.Name, Inode: inode.Inode, Type: inode.Type},
		Inode:  inode,
	}, nil
}

func (e *fakeExecutor) UpdateInode(_ context.Context, req model.UpdateInodeRequest) (model.InodeRecord, error) {
	if e.err != nil {
		return model.InodeRecord{}, e.err
	}
	return model.InodeRecord{
		Inode:         req.Inode,
		Type:          model.InodeTypeFile,
		Size:          req.Size,
		Mode:          req.Mode,
		LinkCount:     1,
		UpdatedUnixNs: req.UpdatedUnixNs,
		OpaqueAttrs:   append([]byte(nil), req.OpaqueAttrs...),
	}, nil
}

func (e *fakeExecutor) Lookup(context.Context, model.LookupRequest) (model.DentryRecord, error) {
	e.lookupCount++
	if e.err != nil {
		return model.DentryRecord{}, e.err
	}
	return model.DentryRecord{Parent: model.RootInode, Name: "checkpoint", Inode: 42, Type: model.InodeTypeFile}, nil
}

func (e *fakeExecutor) LookupPlus(ctx context.Context, req model.LookupRequest) (model.DentryAttrPair, error) {
	dentry, err := e.Lookup(ctx, req)
	if err != nil {
		return model.DentryAttrPair{}, err
	}
	return model.DentryAttrPair{
		Dentry: dentry,
		Inode: model.InodeRecord{
			Inode:     dentry.Inode,
			Type:      dentry.Type,
			Size:      4096,
			Mode:      0o644,
			LinkCount: 1,
		},
	}, nil
}

func (e *fakeExecutor) ReadDir(context.Context, model.ReadDirRequest) ([]model.DentryRecord, error) {
	if e.err != nil {
		return nil, e.err
	}
	return []model.DentryRecord{{Parent: model.RootInode, Name: "checkpoint", Inode: 42, Type: model.InodeTypeFile}}, nil
}

func (e *fakeExecutor) ReadDirPlus(context.Context, model.ReadDirRequest) ([]model.DentryAttrPair, error) {
	e.readDirPlusCount++
	if e.err != nil {
		return nil, e.err
	}
	return []model.DentryAttrPair{{
		Dentry: model.DentryRecord{Parent: model.RootInode, Name: "checkpoint", Inode: 42, Type: model.InodeTypeFile},
		Inode: model.InodeRecord{
			Inode:       42,
			Type:        model.InodeTypeFile,
			Size:        4096,
			Mode:        0o644,
			LinkCount:   1,
			OpaqueAttrs: []byte(`{"body_ref":"cas://checkpoint"}`),
		},
	}}, nil
}

func TestTypedClientDefaultLookupCacheHit(t *testing.T) {
	exec := &fakeExecutor{}
	cli, cleanup := openBufconnClient(t, exec)
	defer cleanup()

	req := model.LookupRequest{Mount: "vol", Parent: model.RootInode, Name: "checkpoint"}
	first, err := cli.Lookup(context.Background(), req)
	require.NoError(t, err)
	second, err := cli.Lookup(context.Background(), req)
	require.NoError(t, err)

	require.Equal(t, first, second)
	require.Equal(t, 1, exec.lookupCount)
	require.Equal(t, LookupCacheStats{Hits: 1, Misses: 1, Inserts: 1}, cli.LookupCacheStats())
}

func TestTypedClientCanDisableLookupCache(t *testing.T) {
	exec := &fakeExecutor{}
	cli, cleanup := openBufconnClientWithConfig(t, exec, ClientConfig{DisableLookupCache: true})
	defer cleanup()

	req := model.LookupRequest{Mount: "vol", Parent: model.RootInode, Name: "checkpoint"}
	_, err := cli.Lookup(context.Background(), req)
	require.NoError(t, err)
	_, err = cli.Lookup(context.Background(), req)
	require.NoError(t, err)

	require.Equal(t, 2, exec.lookupCount)
	require.Equal(t, LookupCacheStats{}, cli.LookupCacheStats())
}

func TestTypedClientPopulatesLookupCacheFromReadDirPlus(t *testing.T) {
	exec := &fakeExecutor{}
	cli, cleanup := openBufconnClient(t, exec)
	defer cleanup()

	_, err := cli.ReadDirPlus(context.Background(), model.ReadDirRequest{Mount: "vol", Parent: model.RootInode})
	require.NoError(t, err)
	record, err := cli.Lookup(context.Background(), model.LookupRequest{Mount: "vol", Parent: model.RootInode, Name: "checkpoint"})
	require.NoError(t, err)

	require.Equal(t, model.InodeID(42), record.Inode)
	require.Equal(t, 0, exec.lookupCount)
	require.Equal(t, 1, exec.readDirPlusCount)
	require.Equal(t, uint64(1), cli.LookupCacheStats().Hits)
}

func TestTypedClientMovesLookupCacheAfterRename(t *testing.T) {
	exec := &fakeExecutor{}
	cli, cleanup := openBufconnClient(t, exec)
	defer cleanup()

	_, err := cli.Lookup(context.Background(), model.LookupRequest{Mount: "vol", Parent: model.RootInode, Name: "checkpoint"})
	require.NoError(t, err)
	require.NoError(t, cli.Rename(context.Background(), model.RenameRequest{
		Mount:      "vol",
		FromParent: model.RootInode,
		FromName:   "checkpoint",
		ToParent:   8,
		ToName:     "published",
	}))
	record, err := cli.Lookup(context.Background(), model.LookupRequest{Mount: "vol", Parent: 8, Name: "published"})
	require.NoError(t, err)

	require.Equal(t, model.InodeID(42), record.Inode)
	require.Equal(t, 1, exec.lookupCount)
	require.Equal(t, uint64(1), cli.LookupCacheStats().Hits)
}

func (e *fakeExecutor) GetReadVersion(context.Context, model.ReadVersionRequest) (uint64, error) {
	if e.err != nil {
		return 0, e.err
	}
	return 5678, nil
}

func (e *fakeExecutor) SnapshotSubtree(_ context.Context, req model.SnapshotSubtreeRequest) (model.SnapshotSubtreeToken, error) {
	if e.err != nil {
		return model.SnapshotSubtreeToken{}, e.err
	}
	return model.SnapshotSubtreeToken{
		Mount:           req.Mount,
		MountKeyID:      1,
		RootInode:       req.RootInode,
		ReadVersion:     5678,
		RuntimeEvidence: append([]model.SnapshotEvidenceRef(nil), e.snapshotRefs...),
	}, nil
}

func (e *fakeExecutor) ResolveSnapshotSubtreeToken(_ context.Context, token model.SnapshotSubtreeToken) (model.SnapshotSubtreeToken, error) {
	if e.err != nil {
		return model.SnapshotSubtreeToken{}, e.err
	}
	token.MountKeyID = 1
	return token, nil
}

func (e *fakeExecutor) GetQuotaUsage(context.Context, model.QuotaUsageRequest) (model.UsageRecord, error) {
	if e.err != nil {
		return model.UsageRecord{}, e.err
	}
	return model.UsageRecord{Bytes: 4096, Inodes: 2}, nil
}

func (e *fakeExecutor) Rename(context.Context, model.RenameRequest) error {
	return e.err
}

func (e *fakeExecutor) RenameReplace(_ context.Context, req model.RenameReplaceRequest) (model.RenameReplaceResult, error) {
	if e.err != nil {
		return model.RenameReplaceResult{}, e.err
	}
	return model.RenameReplaceResult{
		Replaced:        true,
		OldDentry:       model.DentryRecord{Parent: req.ToParent, Name: req.ToName, Inode: 41, Type: model.InodeTypeFile},
		OldInode:        model.InodeRecord{Inode: 41, Type: model.InodeTypeFile, LinkCount: 1},
		OldInodeDeleted: true,
	}, nil
}

func (e *fakeExecutor) RenameSubtree(context.Context, model.RenameSubtreeRequest) error {
	return e.err
}

func (e *fakeExecutor) Link(context.Context, model.LinkRequest) error {
	return e.err
}

func (e *fakeExecutor) Unlink(context.Context, model.UnlinkRequest) error {
	return e.err
}

func (e *fakeExecutor) Remove(_ context.Context, req model.RemoveRequest) (model.RemoveResult, error) {
	if e.err != nil {
		return model.RemoveResult{}, e.err
	}
	return model.RemoveResult{
		RemovedDentry: model.DentryRecord{Parent: req.Parent, Name: req.Name, Inode: 61, Type: model.InodeTypeFile},
		OldInode:      model.InodeRecord{Inode: 61, Type: model.InodeTypeFile, LinkCount: 1},
		InodeDeleted:  true,
	}, nil
}

func (e *fakeExecutor) RemoveDirectory(context.Context, model.RemoveDirectoryRequest) error {
	return e.err
}

func (e *fakeExecutor) OpenWriteSession(_ context.Context, req model.OpenWriteSessionRequest) (model.SessionRecord, error) {
	if e.err != nil {
		return model.SessionRecord{}, e.err
	}
	return model.SessionRecord{Session: req.Session, Inode: req.Inode, ExpiresUnixNs: int64(req.TTL)}, nil
}

func (e *fakeExecutor) HeartbeatWriteSession(_ context.Context, req model.HeartbeatWriteSessionRequest) (model.SessionRecord, error) {
	if e.err != nil {
		return model.SessionRecord{}, e.err
	}
	return model.SessionRecord{Session: req.Session, Inode: req.Inode, ExpiresUnixNs: int64(req.TTL)}, nil
}

func (e *fakeExecutor) CloseWriteSession(context.Context, model.CloseWriteSessionRequest) error {
	return e.err
}

func (e *fakeExecutor) ExpireWriteSessions(context.Context, model.ExpireWriteSessionsRequest) (model.ExpireWriteSessionsResult, error) {
	if e.err != nil {
		return model.ExpireWriteSessionsResult{}, e.err
	}
	return model.ExpireWriteSessionsResult{Expired: 2}, nil
}

func TestTypedClientRoundTrip(t *testing.T) {
	cli, cleanup := openBufconnClient(t, &fakeExecutor{})
	defer cleanup()

	record, err := cli.Lookup(context.Background(), model.LookupRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "checkpoint",
	})
	require.NoError(t, err)
	require.Equal(t, model.DentryRecord{
		Parent: model.RootInode,
		Name:   "checkpoint",
		Inode:  42,
		Type:   model.InodeTypeFile,
	}, record)

	pair, err := cli.LookupPlus(context.Background(), model.LookupRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "checkpoint",
	})
	require.NoError(t, err)
	require.Equal(t, model.DentryAttrPair{
		Dentry: model.DentryRecord{
			Parent: model.RootInode,
			Name:   "checkpoint",
			Inode:  42,
			Type:   model.InodeTypeFile,
		},
		Inode: model.InodeRecord{
			Inode:     42,
			Type:      model.InodeTypeFile,
			Size:      4096,
			Mode:      0o644,
			LinkCount: 1,
		},
	}, pair)

	records, err := cli.ReadDir(context.Background(), model.ReadDirRequest{
		Mount:      "vol",
		Parent:     model.RootInode,
		StartAfter: "batch-0001",
		Limit:      16,
	})
	require.NoError(t, err)
	require.Equal(t, []model.DentryRecord{{
		Parent: model.RootInode,
		Name:   "checkpoint",
		Inode:  42,
		Type:   model.InodeTypeFile,
	}}, records)

	pairs, err := cli.ReadDirPlus(context.Background(), model.ReadDirRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Limit:  8,
	})
	require.NoError(t, err)
	require.Equal(t, []model.DentryAttrPair{{
		Dentry: model.DentryRecord{Parent: model.RootInode, Name: "checkpoint", Inode: 42, Type: model.InodeTypeFile},
		Inode: model.InodeRecord{
			Inode:       42,
			Type:        model.InodeTypeFile,
			Size:        4096,
			Mode:        0o644,
			LinkCount:   1,
			OpaqueAttrs: []byte(`{"body_ref":"cas://checkpoint"}`),
		},
	}}, pairs)
}

func TestTypedClientMutationRPCs(t *testing.T) {
	cli, cleanup := openBufconnClient(t, &fakeExecutor{})
	defer cleanup()

	created, err := cli.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "created",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile, Mode: 0o644},
	})
	require.NoError(t, err)
	require.Equal(t, model.InodeID(42), created.Inode.Inode)
	require.Equal(t, "created", created.Dentry.Name)

	require.NoError(t, cli.RenameSubtree(context.Background(), model.RenameSubtreeRequest{
		Mount:      "vol",
		FromParent: 1,
		FromName:   "old",
		ToParent:   2,
		ToName:     "new",
	}))
	replaced, err := cli.RenameReplace(context.Background(), model.RenameReplaceRequest{
		Mount:      "vol",
		FromParent: 1,
		FromName:   ".stage-new",
		ToParent:   2,
		ToName:     "new",
	})
	require.NoError(t, err)
	require.True(t, replaced.Replaced)
	require.True(t, replaced.OldInodeDeleted)
	require.Equal(t, model.InodeID(41), replaced.OldDentry.Inode)
	require.NoError(t, cli.Link(context.Background(), model.LinkRequest{
		Mount:      "vol",
		FromParent: 1,
		FromName:   "file",
		ToParent:   2,
		ToName:     "alias",
	}))
	require.NoError(t, cli.Unlink(context.Background(), model.UnlinkRequest{
		Mount:  "vol",
		Parent: 2,
		Name:   "alias",
	}))
	removed, err := cli.Remove(context.Background(), model.RemoveRequest{
		Mount:  "vol",
		Parent: 2,
		Name:   "old-file",
	})
	require.NoError(t, err)
	require.Equal(t, model.InodeID(61), removed.RemovedDentry.Inode)
	require.Equal(t, model.InodeID(61), removed.OldInode.Inode)
	require.True(t, removed.InodeDeleted)
	require.NoError(t, cli.RemoveDirectory(context.Background(), model.RemoveDirectoryRequest{
		Mount:  "vol",
		Parent: 2,
		Name:   "empty-dir",
	}))

	updated, err := cli.UpdateInode(context.Background(), model.UpdateInodeRequest{
		Mount:            "vol",
		Parent:           2,
		Inode:            42,
		Name:             "alias",
		SetSize:          true,
		Size:             8192,
		SetMode:          true,
		Mode:             0o600,
		SetUpdatedUnixNs: true,
		UpdatedUnixNs:    99,
		SetOpaqueAttrs:   true,
		OpaqueAttrs:      []byte("body=cas://2"),
	})
	require.NoError(t, err)
	require.Equal(t, model.InodeRecord{
		Inode:         42,
		Type:          model.InodeTypeFile,
		Size:          8192,
		Mode:          0o600,
		LinkCount:     1,
		UpdatedUnixNs: 99,
		OpaqueAttrs:   []byte("body=cas://2"),
	}, updated)

	session, err := cli.OpenWriteSession(context.Background(), model.OpenWriteSessionRequest{
		Mount:   "vol",
		Inode:   42,
		Session: "writer-1",
		TTL:     time.Microsecond,
	})
	require.NoError(t, err)
	require.Equal(t, model.SessionRecord{Session: "writer-1", Inode: 42, ExpiresUnixNs: 1000}, session)

	session, err = cli.HeartbeatWriteSession(context.Background(), model.HeartbeatWriteSessionRequest{
		Mount:   "vol",
		Inode:   42,
		Session: "writer-1",
		TTL:     2 * time.Microsecond,
	})
	require.NoError(t, err)
	require.Equal(t, int64(2000), session.ExpiresUnixNs)
	require.NoError(t, cli.CloseWriteSession(context.Background(), model.CloseWriteSessionRequest{Mount: "vol", Inode: 42, Session: "writer-1"}))
	expired, err := cli.ExpireWriteSessions(context.Background(), model.ExpireWriteSessionsRequest{Mount: "vol", Limit: 64})
	require.NoError(t, err)
	require.Equal(t, model.ExpireWriteSessionsResult{Expired: 2}, expired)
}

func TestTypedClientErrorTranslation(t *testing.T) {
	cli, cleanup := openBufconnClient(t, &fakeExecutor{err: model.ErrExists})
	defer cleanup()

	_, err := cli.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "checkpoint",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile},
	})
	require.ErrorIs(t, err, model.ErrExists)

	cli, cleanup = openBufconnClient(t, &fakeExecutor{err: model.ErrNotFound})
	defer cleanup()
	_, err = cli.Lookup(context.Background(), model.LookupRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "missing",
	})
	require.ErrorIs(t, err, model.ErrNotFound)

	cli, cleanup = openBufconnClient(t, &fakeExecutor{err: model.ErrMountNotRegistered})
	defer cleanup()
	_, err = cli.Lookup(context.Background(), model.LookupRequest{Mount: "missing", Parent: model.RootInode, Name: "x"})
	require.ErrorIs(t, err, model.ErrMountNotRegistered)

	cli, cleanup = openBufconnClient(t, &fakeExecutor{err: model.ErrMountRetired})
	defer cleanup()
	_, err = cli.Lookup(context.Background(), model.LookupRequest{Mount: "retired", Parent: model.RootInode, Name: "x"})
	require.ErrorIs(t, err, model.ErrMountRetired)

	cli, cleanup = openBufconnClient(t, &fakeExecutor{err: model.ErrCrossAuthorityRename})
	defer cleanup()
	err = cli.Rename(context.Background(), model.RenameRequest{Mount: "vol", FromParent: 1, FromName: "a", ToParent: 2, ToName: "b"})
	require.ErrorIs(t, err, model.ErrCrossAuthorityRename)

	cli, cleanup = openBufconnClient(t, &fakeExecutor{err: model.ErrInvalidRequest})
	defer cleanup()
	err = cli.RenameSubtree(context.Background(), model.RenameSubtreeRequest{Mount: "vol", FromParent: 1, FromName: "a", ToParent: 1, ToName: "a"})
	require.ErrorIs(t, err, model.ErrInvalidRequest)

	cli, cleanup = openBufconnClient(t, &fakeExecutor{err: model.ErrInvalidName})
	defer cleanup()
	_, err = cli.Lookup(context.Background(), model.LookupRequest{Mount: "vol", Parent: model.RootInode, Name: ""})
	require.ErrorIs(t, err, model.ErrInvalidName)

	cli, cleanup = openBufconnClient(t, &fakeExecutor{err: model.ErrQuotaExceeded})
	defer cleanup()
	_, err = cli.GetQuotaUsage(context.Background(), model.QuotaUsageRequest{Mount: "vol"})
	require.ErrorIs(t, err, model.ErrQuotaExceeded)

	cli, cleanup = openBufconnClient(t, &fakeExecutor{err: model.ErrWatchOverflow})
	defer cleanup()
	_, err = cli.ReadDir(context.Background(), model.ReadDirRequest{Mount: "vol", Parent: model.RootInode})
	require.ErrorIs(t, err, model.ErrWatchOverflow)
}

func TestTypedClientPreservesUnknownStatus(t *testing.T) {
	errClient := New(fsmetapb.NewFSMetadataClient(&failingConn{}))
	_, err := errClient.Lookup(context.Background(), model.LookupRequest{})
	require.Error(t, err)
	require.False(t, errors.Is(err, model.ErrNotFound))
	require.Equal(t, codes.Internal, status.Code(err))
}

func TestTypedClientWatchSubtree(t *testing.T) {
	watcher := &fakeWatcher{sub: newFakeWatchSub(1)}
	cli, cleanup := openBufconnClient(t, &fakeExecutor{}, fsmetaserver.WithWatcher(watcher))
	defer cleanup()

	stream, err := cli.WatchSubtree(context.Background(), fsmeta.WatchRequest{
		KeyPrefix:          []byte("fsm/"),
		BackPressureWindow: 4,
	})
	require.NoError(t, err)
	require.Equal(t, watcher.sub.ready, stream.ReadyCursor())

	require.Eventually(t, func() bool {
		req := watcher.request()
		return string(req.KeyPrefix) == "fsm/" && req.BackPressureWindow == 4
	}, time.Second, 10*time.Millisecond)
	evt := fsmeta.WatchEvent{
		Cursor:        fsmeta.WatchCursor{RegionID: 8, Term: 1, Index: 2},
		CommitVersion: 90,
		Source:        fsmeta.WatchEventSourceResolveLock,
		Key:           []byte("fsm/checkpoint"),
	}
	watcher.sub.events <- evt
	got, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, evt, got)
	require.NoError(t, stream.Ack(got.Cursor))
	require.Eventually(t, func() bool {
		acks := watcher.sub.acked()
		return len(acks) == 1 && acks[0] == evt.Cursor
	}, time.Second, 10*time.Millisecond)
	watchStream, ok := stream.(*WatchStream)
	require.True(t, ok)
	require.NoError(t, watchStream.AckEvent(got))
	require.NoError(t, stream.Close())
}

func TestWatchSessionHelpers(t *testing.T) {
	sub := &stubWatchSubscription{
		ready: fsmeta.WatchCursor{RegionID: 1, Term: 2, Index: 3},
		events: []fsmeta.WatchEvent{{
			Cursor:        fsmeta.WatchCursor{RegionID: 1, Term: 2, Index: 4},
			CommitVersion: 99,
			Source:        fsmeta.WatchEventSourceCommit,
			Key:           []byte("fsm/key"),
		}},
	}
	session := NewWatchSession(sub)

	require.Equal(t, sub.ready, session.ReadyCursor())
	want := sub.events[0]
	evt, err := session.Recv()
	require.NoError(t, err)
	require.Equal(t, want, evt)
	require.NoError(t, session.Ack(evt))
	require.Equal(t, []fsmeta.WatchCursor{evt.Cursor}, sub.acks)
	require.NoError(t, session.Close())
	require.True(t, sub.closed)

	var nilSession *WatchSession
	_, err = nilSession.Recv()
	require.Error(t, err)
	require.Error(t, nilSession.Ack(evt))
	require.Equal(t, fsmeta.WatchCursor{}, nilSession.ReadyCursor())
	require.NoError(t, nilSession.Close())
}

func TestTypedClientSnapshotSubtree(t *testing.T) {
	publisher := &fakeSnapshotPublisher{}
	ref := testClientSnapshotEvidenceRef(4, 0x10)
	cli, cleanup := openBufconnClient(t, &fakeExecutor{snapshotRefs: []model.SnapshotEvidenceRef{ref}}, fsmetaserver.WithSnapshotPublisher(publisher))
	defer cleanup()

	token, err := cli.SnapshotSubtree(context.Background(), model.SnapshotSubtreeRequest{
		Mount:     "vol",
		RootInode: 42,
	})
	require.NoError(t, err)
	require.Equal(t, model.SnapshotSubtreeToken{Mount: "vol", RootInode: 42, ReadVersion: 5678, RuntimeEvidence: []model.SnapshotEvidenceRef{ref}}, token)
	require.NoError(t, cli.RetireSnapshotSubtree(context.Background(), token))
	require.Equal(t, token.Mount, publisher.retired.Mount)
	require.Equal(t, model.MountKeyID(1), publisher.retired.MountKeyID)
	require.Equal(t, token.RootInode, publisher.retired.RootInode)
	require.Equal(t, token.ReadVersion, publisher.retired.ReadVersion)
	require.Equal(t, []model.SnapshotEvidenceRef{ref}, publisher.retired.RuntimeEvidence)
}

func TestClientSnapshotEvidenceFromProtoSkipsNil(t *testing.T) {
	ref := testClientSnapshotEvidenceRef(4, 0x10)
	got := snapshotEvidenceRefsFromProto([]*fsmetapb.SnapshotEvidenceRef{
		nil,
		snapshotEvidenceRefsToProto([]model.SnapshotEvidenceRef{ref})[0],
	})
	require.Equal(t, []model.SnapshotEvidenceRef{ref}, got)
	require.Nil(t, snapshotEvidenceRefsFromProto([]*fsmetapb.SnapshotEvidenceRef{nil}))
}

func TestTypedClientGetReadVersion(t *testing.T) {
	cli, cleanup := openBufconnClient(t, &fakeExecutor{})
	defer cleanup()

	version, err := cli.GetReadVersion(context.Background(), model.ReadVersionRequest{Mount: "vol"})
	require.NoError(t, err)
	require.Equal(t, uint64(5678), version)
}

func TestTypedClientGetQuotaUsage(t *testing.T) {
	cli, cleanup := openBufconnClient(t, &fakeExecutor{})
	defer cleanup()

	usage, err := cli.GetQuotaUsage(context.Background(), model.QuotaUsageRequest{Mount: "vol", Scope: 7})
	require.NoError(t, err)
	require.Equal(t, model.UsageRecord{Bytes: 4096, Inodes: 2}, usage)
}

type fakeSnapshotPublisher struct {
	retired model.SnapshotSubtreeToken
}

func (p *fakeSnapshotPublisher) PublishSnapshotSubtree(context.Context, model.SnapshotSubtreeToken) error {
	return nil
}

func (p *fakeSnapshotPublisher) RetireSnapshotSubtree(_ context.Context, token model.SnapshotSubtreeToken) error {
	p.retired = token
	return nil
}

func testClientSnapshotEvidenceRef(epoch uint64, seed byte) model.SnapshotEvidenceRef {
	var root [32]byte
	var digest [32]byte
	root[0] = seed
	digest[0] = seed + 1
	return model.SnapshotEvidenceRef{EpochID: epoch, EvidenceRoot: root, PayloadDigest: digest}
}

type fakeWatcher struct {
	mu  sync.Mutex
	req fsmeta.WatchRequest
	sub *fakeWatchSub
}

func (w *fakeWatcher) Subscribe(_ context.Context, req fsmeta.WatchRequest) (fsmeta.WatchSubscription, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.req = req
	return w.sub, nil
}

func (w *fakeWatcher) request() fsmeta.WatchRequest {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.req
}

type fakeWatchSub struct {
	mu     sync.Mutex
	events chan fsmeta.WatchEvent
	acks   []fsmeta.WatchCursor
	ready  fsmeta.WatchCursor
}

func newFakeWatchSub(buffer int) *fakeWatchSub {
	return &fakeWatchSub{
		events: make(chan fsmeta.WatchEvent, buffer),
		ready:  fsmeta.WatchCursor{RegionID: 8, Term: 1, Index: 1},
	}
}

func (s *fakeWatchSub) Events() <-chan fsmeta.WatchEvent {
	return s.events
}

func (s *fakeWatchSub) ReadyCursor() fsmeta.WatchCursor {
	return s.ready
}

func (s *fakeWatchSub) Ack(cursor fsmeta.WatchCursor) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.acks = append(s.acks, cursor)
}

func (s *fakeWatchSub) acked() []fsmeta.WatchCursor {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]fsmeta.WatchCursor(nil), s.acks...)
}

func (s *fakeWatchSub) Close() {
	close(s.events)
}

func (s *fakeWatchSub) Err() error {
	return nil
}

type stubWatchSubscription struct {
	events []fsmeta.WatchEvent
	acks   []fsmeta.WatchCursor
	ready  fsmeta.WatchCursor
	closed bool
}

func (s *stubWatchSubscription) Recv() (fsmeta.WatchEvent, error) {
	if len(s.events) == 0 {
		return fsmeta.WatchEvent{}, errors.New("empty")
	}
	evt := s.events[0]
	s.events = s.events[1:]
	return evt, nil
}

func (s *stubWatchSubscription) ReadyCursor() fsmeta.WatchCursor {
	return s.ready
}

func (s *stubWatchSubscription) Ack(cursor fsmeta.WatchCursor) error {
	s.acks = append(s.acks, cursor)
	return nil
}

func (s *stubWatchSubscription) Close() error {
	s.closed = true
	return nil
}

func openBufconnClient(t *testing.T, executor fsmetaserver.Executor, opts ...fsmetaserver.Option) (*GRPCClient, func()) {
	return openBufconnClientWithConfig(t, executor, ClientConfig{}, opts...)
}

func openBufconnClientWithConfig(t *testing.T, executor fsmetaserver.Executor, cfg ClientConfig, opts ...fsmetaserver.Option) (*GRPCClient, func()) {
	t.Helper()
	const bufSize = 1 << 20
	listener := bufconn.Listen(bufSize)
	grpcServer := grpc.NewServer()
	fsmetaserver.Register(grpcServer, executor, opts...)
	go func() {
		_ = grpcServer.Serve(listener)
	}()
	dialer := func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}
	ctx := context.Background()
	cli, err := NewGRPCClientWithConfig(ctx, "passthrough:///fsmeta-bufnet", cfg,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dialer),
	)
	require.NoError(t, err)
	return cli, func() {
		_ = cli.Close()
		grpcServer.Stop()
		_ = listener.Close()
	}
}

type failingConn struct{}

func (f *failingConn) Invoke(context.Context, string, any, any, ...grpc.CallOption) error {
	return status.Error(codes.Internal, "boom")
}

func (f *failingConn) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, status.Error(codes.Internal, "boom")
}
