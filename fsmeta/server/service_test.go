// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/feichai0017/NoKV/fsmeta/observe"
	fsmetapb "github.com/feichai0017/NoKV/pb/fsmeta"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type fakeExecutor struct {
	createReq        model.CreateRequest
	createInode      model.InodeRecord
	updateReq        model.UpdateInodeRequest
	readDirReq       model.ReadDirRequest
	readVersionReq   model.ReadVersionRequest
	snapshotReq      model.SnapshotSubtreeRequest
	quotaReq         model.QuotaUsageRequest
	renameReq        model.RenameRequest
	renameReplaceReq model.RenameReplaceRequest
	renameSubtreeReq model.RenameSubtreeRequest
	linkReq          model.LinkRequest
	unlinkReq        model.UnlinkRequest
	removeReq        model.RemoveRequest
	removeDirReq     model.RemoveDirectoryRequest
	openReq          model.OpenWriteSessionRequest
	heartbeatReq     model.HeartbeatWriteSessionRequest
	closeReq         model.CloseWriteSessionRequest
	expireReq        model.ExpireWriteSessionsRequest
	err              error
	snapshotRefs     []model.SnapshotEvidenceRef
}

func (e *fakeExecutor) Create(_ context.Context, req model.CreateRequest) (model.CreateResult, error) {
	e.createReq = req
	if e.err != nil {
		return model.CreateResult{}, e.err
	}
	e.createInode = req.Attrs.InodeRecord(42)
	result := model.CreateResult{
		Dentry: model.DentryRecord{Parent: req.Parent, Name: req.Name, Inode: e.createInode.Inode, Type: e.createInode.Type},
		Inode:  e.createInode,
	}
	return result, nil
}

func (e *fakeExecutor) UpdateInode(_ context.Context, req model.UpdateInodeRequest) (model.InodeRecord, error) {
	e.updateReq = req
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
	if e.err != nil {
		return model.DentryRecord{}, e.err
	}
	return model.DentryRecord{
		Parent: model.RootInode,
		Name:   "checkpoint",
		Inode:  42,
		Type:   model.InodeTypeFile,
	}, nil
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

func (e *fakeExecutor) ReadDir(_ context.Context, req model.ReadDirRequest) ([]model.DentryRecord, error) {
	e.readDirReq = req
	if e.err != nil {
		return nil, e.err
	}
	return []model.DentryRecord{{
		Parent: req.Parent,
		Name:   "checkpoint",
		Inode:  42,
		Type:   model.InodeTypeFile,
	}}, nil
}

func (e *fakeExecutor) ReadDirPlus(_ context.Context, req model.ReadDirRequest) ([]model.DentryAttrPair, error) {
	e.readDirReq = req
	if e.err != nil {
		return nil, e.err
	}
	return []model.DentryAttrPair{{
		Dentry: model.DentryRecord{
			Parent: req.Parent,
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
	}}, nil
}

func (e *fakeExecutor) GetReadVersion(_ context.Context, req model.ReadVersionRequest) (uint64, error) {
	e.readVersionReq = req
	if e.err != nil {
		return 0, e.err
	}
	return 1234, nil
}

func (e *fakeExecutor) SnapshotSubtree(_ context.Context, req model.SnapshotSubtreeRequest) (model.SnapshotSubtreeToken, error) {
	e.snapshotReq = req
	if e.err != nil {
		return model.SnapshotSubtreeToken{}, e.err
	}
	return model.SnapshotSubtreeToken{
		Mount:           req.Mount,
		MountKeyID:      1,
		RootInode:       req.RootInode,
		ReadVersion:     1234,
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

func (e *fakeExecutor) GetQuotaUsage(_ context.Context, req model.QuotaUsageRequest) (model.UsageRecord, error) {
	e.quotaReq = req
	if e.err != nil {
		return model.UsageRecord{}, e.err
	}
	return model.UsageRecord{Bytes: 4096, Inodes: 2}, nil
}

func (e *fakeExecutor) Rename(_ context.Context, req model.RenameRequest) error {
	e.renameReq = req
	return e.err
}

func (e *fakeExecutor) RenameReplace(_ context.Context, req model.RenameReplaceRequest) (model.RenameReplaceResult, error) {
	e.renameReplaceReq = req
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

func (e *fakeExecutor) RenameSubtree(_ context.Context, req model.RenameSubtreeRequest) error {
	e.renameSubtreeReq = req
	return e.err
}

func (e *fakeExecutor) Link(_ context.Context, req model.LinkRequest) error {
	e.linkReq = req
	return e.err
}

func (e *fakeExecutor) Unlink(_ context.Context, req model.UnlinkRequest) error {
	e.unlinkReq = req
	return e.err
}

func (e *fakeExecutor) Remove(_ context.Context, req model.RemoveRequest) (model.RemoveResult, error) {
	e.removeReq = req
	if e.err != nil {
		return model.RemoveResult{}, e.err
	}
	return model.RemoveResult{
		RemovedDentry: model.DentryRecord{Parent: req.Parent, Name: req.Name, Inode: 61, Type: model.InodeTypeFile},
		OldInode:      model.InodeRecord{Inode: 61, Type: model.InodeTypeFile, LinkCount: 1},
		InodeDeleted:  true,
	}, nil
}

func (e *fakeExecutor) RemoveDirectory(_ context.Context, req model.RemoveDirectoryRequest) error {
	e.removeDirReq = req
	return e.err
}

func (e *fakeExecutor) OpenWriteSession(_ context.Context, req model.OpenWriteSessionRequest) (model.SessionRecord, error) {
	e.openReq = req
	if e.err != nil {
		return model.SessionRecord{}, e.err
	}
	return model.SessionRecord{Session: req.Session, Inode: req.Inode, ExpiresUnixNs: int64(req.TTL)}, nil
}

func (e *fakeExecutor) HeartbeatWriteSession(_ context.Context, req model.HeartbeatWriteSessionRequest) (model.SessionRecord, error) {
	e.heartbeatReq = req
	if e.err != nil {
		return model.SessionRecord{}, e.err
	}
	return model.SessionRecord{Session: req.Session, Inode: req.Inode, ExpiresUnixNs: int64(req.TTL)}, nil
}

func (e *fakeExecutor) CloseWriteSession(_ context.Context, req model.CloseWriteSessionRequest) error {
	e.closeReq = req
	return e.err
}

func (e *fakeExecutor) ExpireWriteSessions(_ context.Context, req model.ExpireWriteSessionsRequest) (model.ExpireWriteSessionsResult, error) {
	e.expireReq = req
	if e.err != nil {
		return model.ExpireWriteSessionsResult{}, e.err
	}
	return model.ExpireWriteSessionsResult{Expired: 2}, nil
}

func TestGRPCServiceCreateAndReadDirPlus(t *testing.T) {
	executor := &fakeExecutor{}
	client, cleanup := openBufconnClient(t, executor)
	defer cleanup()

	createResp, err := client.Create(context.Background(), &fsmetapb.CreateRequest{
		Mount:  "vol",
		Parent: uint64(model.RootInode),
		Name:   "checkpoint",
		Attrs: &fsmetapb.CreateInodeAttrs{
			Type:        fsmetapb.InodeType_INODE_TYPE_FILE,
			Size:        4096,
			Mode:        0o644,
			OpaqueAttrs: []byte(`{"body_ref":"cas://checkpoint"}`),
		},
	})
	require.NoError(t, err)
	require.Equal(t, uint64(42), createResp.GetDentry().GetInode())
	require.Equal(t, uint64(42), createResp.GetInode().GetInode())
	require.Equal(t, model.CreateRequest{
		Mount:  "vol",
		Parent: model.RootInode,
		Name:   "checkpoint",
		Attrs: model.CreateAttrs{
			Type:        model.InodeTypeFile,
			Size:        4096,
			Mode:        0o644,
			OpaqueAttrs: []byte(`{"body_ref":"cas://checkpoint"}`),
		},
	}, executor.createReq)
	require.Equal(t, model.InodeRecord{
		Inode:       42,
		Type:        model.InodeTypeFile,
		Size:        4096,
		Mode:        0o644,
		LinkCount:   1,
		OpaqueAttrs: []byte(`{"body_ref":"cas://checkpoint"}`),
	}, executor.createInode)

	resp, err := client.ReadDirPlus(context.Background(), &fsmetapb.ReadDirRequest{
		Mount:      "vol",
		Parent:     uint64(model.RootInode),
		StartAfter: "batch-0001",
		Limit:      64,
	})
	require.NoError(t, err)
	require.Equal(t, model.ReadDirRequest{
		Mount:      "vol",
		Parent:     model.RootInode,
		StartAfter: "batch-0001",
		Limit:      64,
	}, executor.readDirReq)
	require.Len(t, resp.GetEntries(), 1)
	require.Equal(t, "checkpoint", resp.GetEntries()[0].GetDentry().GetName())
	require.Equal(t, uint64(4096), resp.GetEntries()[0].GetInode().GetSize())
	require.Equal(t, []byte(nil), resp.GetEntries()[0].GetInode().GetOpaqueAttrs())

	lookupPlus, err := client.LookupPlus(context.Background(), &fsmetapb.LookupRequest{
		Mount:  "vol",
		Parent: uint64(model.RootInode),
		Name:   "checkpoint",
	})
	require.NoError(t, err)
	require.Equal(t, uint64(42), lookupPlus.GetEntry().GetDentry().GetInode())
	require.Equal(t, uint64(4096), lookupPlus.GetEntry().GetInode().GetSize())
}

func TestGRPCServiceReadDirAndMutationRPCs(t *testing.T) {
	executor := &fakeExecutor{}
	client, cleanup := openBufconnClient(t, executor)
	defer cleanup()

	readDirResp, err := client.ReadDir(context.Background(), &fsmetapb.ReadDirRequest{
		Mount:      "vol",
		Parent:     uint64(model.RootInode),
		StartAfter: "a",
		Limit:      32,
	})
	require.NoError(t, err)
	require.Equal(t, model.ReadDirRequest{
		Mount:      "vol",
		Parent:     model.RootInode,
		StartAfter: "a",
		Limit:      32,
	}, executor.readDirReq)
	require.Len(t, readDirResp.GetEntries(), 1)
	require.Equal(t, "checkpoint", readDirResp.GetEntries()[0].GetName())

	_, err = client.Rename(context.Background(), &fsmetapb.RenameRequest{
		Mount:      "vol",
		FromParent: 1,
		FromName:   "old",
		ToParent:   2,
		ToName:     "new",
	})
	require.NoError(t, err)
	require.Equal(t, model.RenameRequest{
		Mount:      "vol",
		FromParent: 1,
		FromName:   "old",
		ToParent:   2,
		ToName:     "new",
	}, executor.renameReq)

	replaceResp, err := client.RenameReplace(context.Background(), &fsmetapb.RenameReplaceRequest{
		Mount:      "vol",
		FromParent: 1,
		FromName:   ".stage-new",
		ToParent:   2,
		ToName:     "new",
	})
	require.NoError(t, err)
	require.True(t, replaceResp.GetReplaced())
	require.True(t, replaceResp.GetOldInodeDeleted())
	require.Equal(t, uint64(41), replaceResp.GetOldDentry().GetInode())
	require.Equal(t, model.RenameReplaceRequest{
		Mount:      "vol",
		FromParent: 1,
		FromName:   ".stage-new",
		ToParent:   2,
		ToName:     "new",
	}, executor.renameReplaceReq)

	_, err = client.RenameSubtree(context.Background(), &fsmetapb.RenameSubtreeRequest{
		Mount:      "vol",
		FromParent: 3,
		FromName:   "subtree-old",
		ToParent:   4,
		ToName:     "subtree-new",
	})
	require.NoError(t, err)
	require.Equal(t, model.RenameSubtreeRequest{
		Mount:      "vol",
		FromParent: 3,
		FromName:   "subtree-old",
		ToParent:   4,
		ToName:     "subtree-new",
	}, executor.renameSubtreeReq)

	_, err = client.Link(context.Background(), &fsmetapb.LinkRequest{
		Mount:      "vol",
		FromParent: 1,
		FromName:   "file",
		ToParent:   2,
		ToName:     "alias",
	})
	require.NoError(t, err)
	require.Equal(t, model.LinkRequest{
		Mount:      "vol",
		FromParent: 1,
		FromName:   "file",
		ToParent:   2,
		ToName:     "alias",
	}, executor.linkReq)

	_, err = client.Unlink(context.Background(), &fsmetapb.UnlinkRequest{
		Mount:  "vol",
		Parent: 2,
		Name:   "alias",
	})
	require.NoError(t, err)
	require.Equal(t, model.UnlinkRequest{
		Mount:  "vol",
		Parent: 2,
		Name:   "alias",
	}, executor.unlinkReq)

	removed, err := client.Remove(context.Background(), &fsmetapb.RemoveRequest{
		Mount:  "vol",
		Parent: 2,
		Name:   "alias",
	})
	require.NoError(t, err)
	require.Equal(t, model.RemoveRequest{
		Mount:  "vol",
		Parent: 2,
		Name:   "alias",
	}, executor.removeReq)
	require.Equal(t, uint64(61), removed.GetRemovedDentry().GetInode())
	require.Equal(t, uint64(61), removed.GetOldInode().GetInode())
	require.True(t, removed.GetInodeDeleted())

	_, err = client.RemoveDirectory(context.Background(), &fsmetapb.RemoveDirectoryRequest{
		Mount:  "vol",
		Parent: 2,
		Name:   "empty-dir",
	})
	require.NoError(t, err)
	require.Equal(t, model.RemoveDirectoryRequest{
		Mount:  "vol",
		Parent: 2,
		Name:   "empty-dir",
	}, executor.removeDirReq)

	updateResp, err := client.UpdateInode(context.Background(), &fsmetapb.UpdateInodeRequest{
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
	require.Equal(t, model.UpdateInodeRequest{
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
	}, executor.updateReq)
	require.Equal(t, uint64(8192), updateResp.GetInode().GetSize())

	openResp, err := client.OpenWriteSession(context.Background(), &fsmetapb.OpenWriteSessionRequest{
		Mount:   "vol",
		Inode:   42,
		Session: "writer-1",
		TtlNs:   1000,
	})
	require.NoError(t, err)
	require.Equal(t, model.OpenWriteSessionRequest{Mount: "vol", Inode: 42, Session: "writer-1", TTL: time.Microsecond}, executor.openReq)
	require.Equal(t, "writer-1", openResp.GetSession().GetSession())

	heartbeatResp, err := client.HeartbeatWriteSession(context.Background(), &fsmetapb.HeartbeatWriteSessionRequest{
		Mount:   "vol",
		Inode:   42,
		Session: "writer-1",
		TtlNs:   2000,
	})
	require.NoError(t, err)
	require.Equal(t, model.HeartbeatWriteSessionRequest{Mount: "vol", Inode: 42, Session: "writer-1", TTL: 2 * time.Microsecond}, executor.heartbeatReq)
	require.Equal(t, int64(2000), heartbeatResp.GetSession().GetExpiresUnixNs())

	_, err = client.CloseWriteSession(context.Background(), &fsmetapb.CloseWriteSessionRequest{Mount: "vol", Inode: 42, Session: "writer-1"})
	require.NoError(t, err)
	require.Equal(t, model.CloseWriteSessionRequest{Mount: "vol", Inode: 42, Session: "writer-1"}, executor.closeReq)

	expireResp, err := client.ExpireWriteSessions(context.Background(), &fsmetapb.ExpireWriteSessionsRequest{Mount: "vol", Limit: 64})
	require.NoError(t, err)
	require.Equal(t, model.ExpireWriteSessionsRequest{Mount: "vol", Limit: 64}, executor.expireReq)
	require.Equal(t, uint64(2), expireResp.GetExpired())
}

func TestGRPCServiceErrorMapping(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		code   codes.Code
		reason string
	}{
		{name: "exists", err: model.ErrExists, code: codes.AlreadyExists, reason: reasonNamespaceExists},
		{name: "not found", err: model.ErrNotFound, code: codes.NotFound, reason: reasonNamespaceNotFound},
		{name: "invalid", err: model.ErrInvalidName, code: codes.InvalidArgument, reason: reasonInvalidName},
		{name: "quota exceeded", err: model.ErrQuotaExceeded, code: codes.ResourceExhausted, reason: reasonQuotaExceeded},
		{name: "watch overflow", err: model.ErrWatchOverflow, code: codes.ResourceExhausted, reason: reasonWatchOverflow},
		{name: "watch cursor expired", err: model.ErrWatchCursorExpired, code: codes.OutOfRange, reason: reasonWatchCursorExpired},
		{name: "mount retired", err: model.ErrMountRetired, code: codes.FailedPrecondition, reason: reasonMountRetired},
		{name: "cross authority rename", err: model.ErrCrossAuthorityRename, code: codes.FailedPrecondition, reason: reasonCrossAuthorityRename},
		{name: "retry exhausted", err: nokverrors.New(nokverrors.KindRetryExhausted, "fsmeta: retry exhausted"), code: codes.Unavailable},
		{name: "internal", err: errors.New("boom"), code: codes.Internal},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, cleanup := openBufconnClient(t, &fakeExecutor{err: tt.err})
			defer cleanup()
			_, err := client.Lookup(context.Background(), &fsmetapb.LookupRequest{
				Mount:  "vol",
				Parent: uint64(model.RootInode),
				Name:   "checkpoint",
			})
			require.Error(t, err)
			require.Equal(t, tt.code, status.Code(err))
			if tt.reason != "" {
				kind, metadata, ok := nokverrors.RPCErrorInfo(err)
				require.True(t, ok)
				require.Equal(t, nokverrors.KindOf(tt.err), kind)
				require.Equal(t, tt.reason, metadata[fsmetaReasonMetadata])
			}
		})
	}
}

func TestGRPCServiceWatchSubtree(t *testing.T) {
	watcher := &fakeWatcher{sub: newFakeWatchSub(1)}
	client, cleanup := openBufconnClient(t, &fakeExecutor{}, WithWatcher(watcher))
	defer cleanup()

	stream, err := client.WatchSubtree(context.Background())
	require.NoError(t, err)
	require.NoError(t, stream.Send(&fsmetapb.WatchAckOrSubscribe{
		Body: &fsmetapb.WatchAckOrSubscribe_Subscribe{Subscribe: &fsmetapb.WatchSubtreeRequest{
			KeyPrefix:          []byte("fsm/"),
			BackPressureWindow: 8,
		}},
	}))
	require.Eventually(t, func() bool {
		req := watcher.request()
		return string(req.KeyPrefix) == "fsm/" && req.BackPressureWindow == 8
	}, time.Second, 10*time.Millisecond)
	ready, err := stream.Recv()
	require.NoError(t, err)
	require.NotNil(t, ready.GetReady())
	require.Equal(t, uint64(3), ready.GetReady().GetCursor().GetIndex())

	evt := observe.WatchEvent{
		Cursor:        observe.WatchCursor{RegionID: 1, Term: 2, Index: 3},
		CommitVersion: 44,
		Source:        observe.WatchEventSourceCommit,
		Key:           []byte("fsm/a"),
	}
	watcher.sub.events <- evt
	resp, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, uint64(44), resp.GetEvent().GetCommitVersion())
	require.Equal(t, []byte("fsm/a"), resp.GetEvent().GetKey())

	require.NoError(t, stream.Send(&fsmetapb.WatchAckOrSubscribe{
		Body: &fsmetapb.WatchAckOrSubscribe_Ack{Ack: &fsmetapb.WatchAck{Cursor: resp.GetEvent().GetRaftCursor()}},
	}))
	require.Eventually(t, func() bool {
		acks := watcher.sub.acked()
		return len(acks) == 1 && acks[0] == evt.Cursor
	}, time.Second, 10*time.Millisecond)
	require.NoError(t, stream.CloseSend())
}

func TestGRPCServiceSnapshotSubtreePublishesToken(t *testing.T) {
	ref := testServerSnapshotEvidenceRef(5, 0x20)
	executor := &fakeExecutor{snapshotRefs: []model.SnapshotEvidenceRef{ref}}
	publisher := &fakeSnapshotPublisher{}
	client, cleanup := openBufconnClient(t, executor, WithSnapshotPublisher(publisher))
	defer cleanup()

	resp, err := client.SnapshotSubtree(context.Background(), &fsmetapb.SnapshotSubtreeRequest{
		Mount:     "vol",
		RootInode: 42,
	})
	require.NoError(t, err)
	require.Equal(t, model.SnapshotSubtreeRequest{Mount: "vol", RootInode: 42}, executor.snapshotReq)
	require.Equal(t, uint64(1234), resp.GetReadVersion())
	refs, err := snapshotEvidenceRefsFromProto(resp.GetRuntimeEvidence())
	require.NoError(t, err)
	require.Equal(t, []model.SnapshotEvidenceRef{ref}, refs)
	require.Equal(t, model.SnapshotSubtreeToken{
		Mount:           "vol",
		MountKeyID:      1,
		RootInode:       42,
		ReadVersion:     1234,
		RuntimeEvidence: []model.SnapshotEvidenceRef{ref},
	}, publisher.token)
}

func TestGRPCServiceGetReadVersionDoesNotPublishSnapshot(t *testing.T) {
	executor := &fakeExecutor{}
	publisher := &fakeSnapshotPublisher{}
	client, cleanup := openBufconnClient(t, executor, WithSnapshotPublisher(publisher))
	defer cleanup()

	resp, err := client.GetReadVersion(context.Background(), &fsmetapb.GetReadVersionRequest{Mount: "vol"})
	require.NoError(t, err)
	require.Equal(t, model.ReadVersionRequest{Mount: "vol"}, executor.readVersionReq)
	require.Equal(t, uint64(1234), resp.GetReadVersion())
	require.Zero(t, publisher.token)
	require.Zero(t, publisher.retired)
}

func TestGRPCServiceSnapshotSubtreeRetiresTokenAfterPublishFailure(t *testing.T) {
	executor := &fakeExecutor{}
	publisher := &fakeSnapshotPublisher{err: errors.New("publish failed")}
	client, cleanup := openBufconnClient(t, executor, WithSnapshotPublisher(publisher))
	defer cleanup()

	_, err := client.SnapshotSubtree(context.Background(), &fsmetapb.SnapshotSubtreeRequest{
		Mount:     "vol",
		RootInode: 42,
	})
	require.Error(t, err)
	want := model.SnapshotSubtreeToken{Mount: "vol", MountKeyID: 1, RootInode: 42, ReadVersion: 1234}
	require.Equal(t, want, publisher.token)
	require.Equal(t, want, publisher.retired)
}

func TestGRPCServiceRetireSnapshotSubtree(t *testing.T) {
	publisher := &fakeSnapshotPublisher{}
	client, cleanup := openBufconnClient(t, &fakeExecutor{}, WithSnapshotPublisher(publisher))
	defer cleanup()
	ref := testServerSnapshotEvidenceRef(6, 0x30)

	_, err := client.RetireSnapshotSubtree(context.Background(), &fsmetapb.RetireSnapshotSubtreeRequest{
		Mount:           "vol",
		RootInode:       42,
		ReadVersion:     1234,
		RuntimeEvidence: snapshotEvidenceRefsToProto([]model.SnapshotEvidenceRef{ref}),
	})
	require.NoError(t, err)
	require.Equal(t, model.SnapshotSubtreeToken{
		Mount:           "vol",
		MountKeyID:      1,
		RootInode:       42,
		ReadVersion:     1234,
		RuntimeEvidence: []model.SnapshotEvidenceRef{ref},
	}, publisher.retired)
}

func TestGRPCServiceRetireSnapshotSubtreeRejectsMalformedPerasRef(t *testing.T) {
	publisher := &fakeSnapshotPublisher{}
	client, cleanup := openBufconnClient(t, &fakeExecutor{}, WithSnapshotPublisher(publisher))
	defer cleanup()

	_, err := client.RetireSnapshotSubtree(context.Background(), &fsmetapb.RetireSnapshotSubtreeRequest{
		Mount:       "vol",
		RootInode:   42,
		ReadVersion: 1234,
		RuntimeEvidence: []*fsmetapb.SnapshotEvidenceRef{{
			EpochId:       6,
			EvidenceRoot:  []byte{1},
			PayloadDigest: make([]byte, 32),
		}},
	})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Zero(t, publisher.retired)
}

func TestGRPCServiceGetQuotaUsage(t *testing.T) {
	executor := &fakeExecutor{}
	client, cleanup := openBufconnClient(t, executor)
	defer cleanup()

	resp, err := client.GetQuotaUsage(context.Background(), &fsmetapb.QuotaUsageRequest{
		Mount: "vol",
		Scope: 7,
	})
	require.NoError(t, err)
	require.Equal(t, model.QuotaUsageRequest{Mount: "vol", Scope: 7}, executor.quotaReq)
	require.Equal(t, uint64(4096), resp.GetBytes())
	require.Equal(t, uint64(2), resp.GetInodes())
}

type fakeSnapshotPublisher struct {
	token       model.SnapshotSubtreeToken
	retired     model.SnapshotSubtreeToken
	err         error
	retireError error
}

func (p *fakeSnapshotPublisher) PublishSnapshotSubtree(_ context.Context, token model.SnapshotSubtreeToken) error {
	p.token = token
	return p.err
}

func (p *fakeSnapshotPublisher) RetireSnapshotSubtree(_ context.Context, token model.SnapshotSubtreeToken) error {
	p.retired = token
	return p.retireError
}

func testServerSnapshotEvidenceRef(epoch uint64, seed byte) model.SnapshotEvidenceRef {
	var root [32]byte
	var digest [32]byte
	root[0] = seed
	digest[0] = seed + 1
	return model.SnapshotEvidenceRef{EpochID: epoch, EvidenceRoot: root, PayloadDigest: digest}
}

type fakeWatcher struct {
	mu  sync.Mutex
	req observe.WatchRequest
	sub *fakeWatchSub
	err error
}

func (w *fakeWatcher) Subscribe(_ context.Context, req observe.WatchRequest) (observe.WatchSubscription, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.req = req
	if w.err != nil {
		return nil, w.err
	}
	return w.sub, nil
}

func (w *fakeWatcher) request() observe.WatchRequest {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.req
}

type fakeWatchSub struct {
	mu     sync.Mutex
	events chan observe.WatchEvent
	acks   []observe.WatchCursor
	err    error
	ready  observe.WatchCursor
}

func newFakeWatchSub(buffer int) *fakeWatchSub {
	return &fakeWatchSub{
		events: make(chan observe.WatchEvent, buffer),
		ready:  observe.WatchCursor{RegionID: 1, Term: 2, Index: 3},
	}
}

func (s *fakeWatchSub) Events() <-chan observe.WatchEvent {
	return s.events
}

func (s *fakeWatchSub) ReadyCursor() observe.WatchCursor {
	return s.ready
}

func (s *fakeWatchSub) Ack(cursor observe.WatchCursor) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.acks = append(s.acks, cursor)
}

func (s *fakeWatchSub) acked() []observe.WatchCursor {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]observe.WatchCursor(nil), s.acks...)
}

func (s *fakeWatchSub) Close() {
	close(s.events)
}

func (s *fakeWatchSub) Err() error {
	return s.err
}

func openBufconnClient(t *testing.T, executor Executor, opts ...Option) (fsmetapb.FSMetadataClient, func()) {
	t.Helper()
	const bufSize = 1 << 20
	listener := bufconn.Listen(bufSize)
	grpcServer := grpc.NewServer()
	Register(grpcServer, executor, opts...)
	go func() {
		_ = grpcServer.Serve(listener)
	}()
	dialer := func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}
	conn, err := grpc.NewClient("passthrough:///fsmeta-bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dialer),
	)
	require.NoError(t, err)
	return fsmetapb.NewFSMetadataClient(conn), func() {
		_ = conn.Close()
		grpcServer.Stop()
		_ = listener.Close()
	}
}
