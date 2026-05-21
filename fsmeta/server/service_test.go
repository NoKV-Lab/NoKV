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
	"github.com/feichai0017/NoKV/fsmeta"
	fsmetapb "github.com/feichai0017/NoKV/pb/fsmeta"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type fakeExecutor struct {
	createReq        fsmeta.CreateRequest
	createInode      fsmeta.InodeRecord
	updateReq        fsmeta.UpdateInodeRequest
	readDirReq       fsmeta.ReadDirRequest
	readVersionReq   fsmeta.ReadVersionRequest
	snapshotReq      fsmeta.SnapshotSubtreeRequest
	quotaReq         fsmeta.QuotaUsageRequest
	renameReq        fsmeta.RenameRequest
	renameReplaceReq fsmeta.RenameReplaceRequest
	renameSubtreeReq fsmeta.RenameSubtreeRequest
	linkReq          fsmeta.LinkRequest
	unlinkReq        fsmeta.UnlinkRequest
	removeReq        fsmeta.RemoveRequest
	removeDirReq     fsmeta.RemoveDirectoryRequest
	openReq          fsmeta.OpenWriteSessionRequest
	heartbeatReq     fsmeta.HeartbeatWriteSessionRequest
	closeReq         fsmeta.CloseWriteSessionRequest
	expireReq        fsmeta.ExpireWriteSessionsRequest
	err              error
	snapshotRefs     []fsmeta.SnapshotEvidenceRef
}

func (e *fakeExecutor) Create(_ context.Context, req fsmeta.CreateRequest) (fsmeta.CreateResult, error) {
	e.createReq = req
	if e.err != nil {
		return fsmeta.CreateResult{}, e.err
	}
	e.createInode = req.Attrs.InodeRecord(42)
	result := fsmeta.CreateResult{
		Dentry: fsmeta.DentryRecord{Parent: req.Parent, Name: req.Name, Inode: e.createInode.Inode, Type: e.createInode.Type},
		Inode:  e.createInode,
	}
	return result, nil
}

func (e *fakeExecutor) UpdateInode(_ context.Context, req fsmeta.UpdateInodeRequest) (fsmeta.InodeRecord, error) {
	e.updateReq = req
	if e.err != nil {
		return fsmeta.InodeRecord{}, e.err
	}
	return fsmeta.InodeRecord{
		Inode:         req.Inode,
		Type:          fsmeta.InodeTypeFile,
		Size:          req.Size,
		Mode:          req.Mode,
		LinkCount:     1,
		UpdatedUnixNs: req.UpdatedUnixNs,
		OpaqueAttrs:   append([]byte(nil), req.OpaqueAttrs...),
	}, nil
}

func (e *fakeExecutor) Lookup(context.Context, fsmeta.LookupRequest) (fsmeta.DentryRecord, error) {
	if e.err != nil {
		return fsmeta.DentryRecord{}, e.err
	}
	return fsmeta.DentryRecord{
		Parent: fsmeta.RootInode,
		Name:   "checkpoint",
		Inode:  42,
		Type:   fsmeta.InodeTypeFile,
	}, nil
}

func (e *fakeExecutor) LookupPlus(ctx context.Context, req fsmeta.LookupRequest) (fsmeta.DentryAttrPair, error) {
	dentry, err := e.Lookup(ctx, req)
	if err != nil {
		return fsmeta.DentryAttrPair{}, err
	}
	return fsmeta.DentryAttrPair{
		Dentry: dentry,
		Inode: fsmeta.InodeRecord{
			Inode:     dentry.Inode,
			Type:      dentry.Type,
			Size:      4096,
			Mode:      0o644,
			LinkCount: 1,
		},
	}, nil
}

func (e *fakeExecutor) ReadDir(_ context.Context, req fsmeta.ReadDirRequest) ([]fsmeta.DentryRecord, error) {
	e.readDirReq = req
	if e.err != nil {
		return nil, e.err
	}
	return []fsmeta.DentryRecord{{
		Parent: req.Parent,
		Name:   "checkpoint",
		Inode:  42,
		Type:   fsmeta.InodeTypeFile,
	}}, nil
}

func (e *fakeExecutor) ReadDirPlus(_ context.Context, req fsmeta.ReadDirRequest) ([]fsmeta.DentryAttrPair, error) {
	e.readDirReq = req
	if e.err != nil {
		return nil, e.err
	}
	return []fsmeta.DentryAttrPair{{
		Dentry: fsmeta.DentryRecord{
			Parent: req.Parent,
			Name:   "checkpoint",
			Inode:  42,
			Type:   fsmeta.InodeTypeFile,
		},
		Inode: fsmeta.InodeRecord{
			Inode:     42,
			Type:      fsmeta.InodeTypeFile,
			Size:      4096,
			Mode:      0o644,
			LinkCount: 1,
		},
	}}, nil
}

func (e *fakeExecutor) GetReadVersion(_ context.Context, req fsmeta.ReadVersionRequest) (uint64, error) {
	e.readVersionReq = req
	if e.err != nil {
		return 0, e.err
	}
	return 1234, nil
}

func (e *fakeExecutor) SnapshotSubtree(_ context.Context, req fsmeta.SnapshotSubtreeRequest) (fsmeta.SnapshotSubtreeToken, error) {
	e.snapshotReq = req
	if e.err != nil {
		return fsmeta.SnapshotSubtreeToken{}, e.err
	}
	return fsmeta.SnapshotSubtreeToken{
		Mount:           req.Mount,
		MountKeyID:      1,
		RootInode:       req.RootInode,
		ReadVersion:     1234,
		RuntimeEvidence: append([]fsmeta.SnapshotEvidenceRef(nil), e.snapshotRefs...),
	}, nil
}

func (e *fakeExecutor) ResolveSnapshotSubtreeToken(_ context.Context, token fsmeta.SnapshotSubtreeToken) (fsmeta.SnapshotSubtreeToken, error) {
	if e.err != nil {
		return fsmeta.SnapshotSubtreeToken{}, e.err
	}
	token.MountKeyID = 1
	return token, nil
}

func (e *fakeExecutor) GetQuotaUsage(_ context.Context, req fsmeta.QuotaUsageRequest) (fsmeta.UsageRecord, error) {
	e.quotaReq = req
	if e.err != nil {
		return fsmeta.UsageRecord{}, e.err
	}
	return fsmeta.UsageRecord{Bytes: 4096, Inodes: 2}, nil
}

func (e *fakeExecutor) Rename(_ context.Context, req fsmeta.RenameRequest) error {
	e.renameReq = req
	return e.err
}

func (e *fakeExecutor) RenameReplace(_ context.Context, req fsmeta.RenameReplaceRequest) (fsmeta.RenameReplaceResult, error) {
	e.renameReplaceReq = req
	if e.err != nil {
		return fsmeta.RenameReplaceResult{}, e.err
	}
	return fsmeta.RenameReplaceResult{
		Replaced:        true,
		OldDentry:       fsmeta.DentryRecord{Parent: req.ToParent, Name: req.ToName, Inode: 41, Type: fsmeta.InodeTypeFile},
		OldInode:        fsmeta.InodeRecord{Inode: 41, Type: fsmeta.InodeTypeFile, LinkCount: 1},
		OldInodeDeleted: true,
	}, nil
}

func (e *fakeExecutor) RenameSubtree(_ context.Context, req fsmeta.RenameSubtreeRequest) error {
	e.renameSubtreeReq = req
	return e.err
}

func (e *fakeExecutor) Link(_ context.Context, req fsmeta.LinkRequest) error {
	e.linkReq = req
	return e.err
}

func (e *fakeExecutor) Unlink(_ context.Context, req fsmeta.UnlinkRequest) error {
	e.unlinkReq = req
	return e.err
}

func (e *fakeExecutor) Remove(_ context.Context, req fsmeta.RemoveRequest) error {
	e.removeReq = req
	return e.err
}

func (e *fakeExecutor) RemoveDirectory(_ context.Context, req fsmeta.RemoveDirectoryRequest) error {
	e.removeDirReq = req
	return e.err
}

func (e *fakeExecutor) OpenWriteSession(_ context.Context, req fsmeta.OpenWriteSessionRequest) (fsmeta.SessionRecord, error) {
	e.openReq = req
	if e.err != nil {
		return fsmeta.SessionRecord{}, e.err
	}
	return fsmeta.SessionRecord{Session: req.Session, Inode: req.Inode, ExpiresUnixNs: int64(req.TTL)}, nil
}

func (e *fakeExecutor) HeartbeatWriteSession(_ context.Context, req fsmeta.HeartbeatWriteSessionRequest) (fsmeta.SessionRecord, error) {
	e.heartbeatReq = req
	if e.err != nil {
		return fsmeta.SessionRecord{}, e.err
	}
	return fsmeta.SessionRecord{Session: req.Session, Inode: req.Inode, ExpiresUnixNs: int64(req.TTL)}, nil
}

func (e *fakeExecutor) CloseWriteSession(_ context.Context, req fsmeta.CloseWriteSessionRequest) error {
	e.closeReq = req
	return e.err
}

func (e *fakeExecutor) ExpireWriteSessions(_ context.Context, req fsmeta.ExpireWriteSessionsRequest) (fsmeta.ExpireWriteSessionsResult, error) {
	e.expireReq = req
	if e.err != nil {
		return fsmeta.ExpireWriteSessionsResult{}, e.err
	}
	return fsmeta.ExpireWriteSessionsResult{Expired: 2}, nil
}

func TestGRPCServiceCreateAndReadDirPlus(t *testing.T) {
	executor := &fakeExecutor{}
	client, cleanup := openBufconnClient(t, executor)
	defer cleanup()

	createResp, err := client.Create(context.Background(), &fsmetapb.CreateRequest{
		Mount:  "vol",
		Parent: uint64(fsmeta.RootInode),
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
	require.Equal(t, fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "checkpoint",
		Attrs: fsmeta.CreateAttrs{
			Type:        fsmeta.InodeTypeFile,
			Size:        4096,
			Mode:        0o644,
			OpaqueAttrs: []byte(`{"body_ref":"cas://checkpoint"}`),
		},
	}, executor.createReq)
	require.Equal(t, fsmeta.InodeRecord{
		Inode:       42,
		Type:        fsmeta.InodeTypeFile,
		Size:        4096,
		Mode:        0o644,
		LinkCount:   1,
		OpaqueAttrs: []byte(`{"body_ref":"cas://checkpoint"}`),
	}, executor.createInode)

	resp, err := client.ReadDirPlus(context.Background(), &fsmetapb.ReadDirRequest{
		Mount:      "vol",
		Parent:     uint64(fsmeta.RootInode),
		StartAfter: "batch-0001",
		Limit:      64,
	})
	require.NoError(t, err)
	require.Equal(t, fsmeta.ReadDirRequest{
		Mount:      "vol",
		Parent:     fsmeta.RootInode,
		StartAfter: "batch-0001",
		Limit:      64,
	}, executor.readDirReq)
	require.Len(t, resp.GetEntries(), 1)
	require.Equal(t, "checkpoint", resp.GetEntries()[0].GetDentry().GetName())
	require.Equal(t, uint64(4096), resp.GetEntries()[0].GetInode().GetSize())
	require.Equal(t, []byte(nil), resp.GetEntries()[0].GetInode().GetOpaqueAttrs())

	lookupPlus, err := client.LookupPlus(context.Background(), &fsmetapb.LookupRequest{
		Mount:  "vol",
		Parent: uint64(fsmeta.RootInode),
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
		Parent:     uint64(fsmeta.RootInode),
		StartAfter: "a",
		Limit:      32,
	})
	require.NoError(t, err)
	require.Equal(t, fsmeta.ReadDirRequest{
		Mount:      "vol",
		Parent:     fsmeta.RootInode,
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
	require.Equal(t, fsmeta.RenameRequest{
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
	require.Equal(t, fsmeta.RenameReplaceRequest{
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
	require.Equal(t, fsmeta.RenameSubtreeRequest{
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
	require.Equal(t, fsmeta.LinkRequest{
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
	require.Equal(t, fsmeta.UnlinkRequest{
		Mount:  "vol",
		Parent: 2,
		Name:   "alias",
	}, executor.unlinkReq)

	_, err = client.Remove(context.Background(), &fsmetapb.RemoveRequest{
		Mount:  "vol",
		Parent: 2,
		Name:   "alias",
	})
	require.NoError(t, err)
	require.Equal(t, fsmeta.RemoveRequest{
		Mount:  "vol",
		Parent: 2,
		Name:   "alias",
	}, executor.removeReq)

	_, err = client.RemoveDirectory(context.Background(), &fsmetapb.RemoveDirectoryRequest{
		Mount:  "vol",
		Parent: 2,
		Name:   "empty-dir",
	})
	require.NoError(t, err)
	require.Equal(t, fsmeta.RemoveDirectoryRequest{
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
	require.Equal(t, fsmeta.UpdateInodeRequest{
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
	require.Equal(t, fsmeta.OpenWriteSessionRequest{Mount: "vol", Inode: 42, Session: "writer-1", TTL: time.Microsecond}, executor.openReq)
	require.Equal(t, "writer-1", openResp.GetSession().GetSession())

	heartbeatResp, err := client.HeartbeatWriteSession(context.Background(), &fsmetapb.HeartbeatWriteSessionRequest{
		Mount:   "vol",
		Inode:   42,
		Session: "writer-1",
		TtlNs:   2000,
	})
	require.NoError(t, err)
	require.Equal(t, fsmeta.HeartbeatWriteSessionRequest{Mount: "vol", Inode: 42, Session: "writer-1", TTL: 2 * time.Microsecond}, executor.heartbeatReq)
	require.Equal(t, int64(2000), heartbeatResp.GetSession().GetExpiresUnixNs())

	_, err = client.CloseWriteSession(context.Background(), &fsmetapb.CloseWriteSessionRequest{Mount: "vol", Inode: 42, Session: "writer-1"})
	require.NoError(t, err)
	require.Equal(t, fsmeta.CloseWriteSessionRequest{Mount: "vol", Inode: 42, Session: "writer-1"}, executor.closeReq)

	expireResp, err := client.ExpireWriteSessions(context.Background(), &fsmetapb.ExpireWriteSessionsRequest{Mount: "vol", Limit: 64})
	require.NoError(t, err)
	require.Equal(t, fsmeta.ExpireWriteSessionsRequest{Mount: "vol", Limit: 64}, executor.expireReq)
	require.Equal(t, uint64(2), expireResp.GetExpired())
}

func TestGRPCServiceErrorMapping(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		code   codes.Code
		reason string
	}{
		{name: "exists", err: fsmeta.ErrExists, code: codes.AlreadyExists, reason: reasonNamespaceExists},
		{name: "not found", err: fsmeta.ErrNotFound, code: codes.NotFound, reason: reasonNamespaceNotFound},
		{name: "invalid", err: fsmeta.ErrInvalidName, code: codes.InvalidArgument, reason: reasonInvalidName},
		{name: "quota exceeded", err: fsmeta.ErrQuotaExceeded, code: codes.ResourceExhausted, reason: reasonQuotaExceeded},
		{name: "watch overflow", err: fsmeta.ErrWatchOverflow, code: codes.ResourceExhausted, reason: reasonWatchOverflow},
		{name: "watch cursor expired", err: fsmeta.ErrWatchCursorExpired, code: codes.OutOfRange, reason: reasonWatchCursorExpired},
		{name: "mount retired", err: fsmeta.ErrMountRetired, code: codes.FailedPrecondition, reason: reasonMountRetired},
		{name: "cross authority rename", err: fsmeta.ErrCrossAuthorityRename, code: codes.FailedPrecondition, reason: reasonCrossAuthorityRename},
		{name: "retry exhausted", err: nokverrors.New(nokverrors.KindRetryExhausted, "fsmeta: retry exhausted"), code: codes.Unavailable},
		{name: "internal", err: errors.New("boom"), code: codes.Internal},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, cleanup := openBufconnClient(t, &fakeExecutor{err: tt.err})
			defer cleanup()
			_, err := client.Lookup(context.Background(), &fsmetapb.LookupRequest{
				Mount:  "vol",
				Parent: uint64(fsmeta.RootInode),
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

	evt := fsmeta.WatchEvent{
		Cursor:        fsmeta.WatchCursor{RegionID: 1, Term: 2, Index: 3},
		CommitVersion: 44,
		Source:        fsmeta.WatchEventSourceCommit,
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
	executor := &fakeExecutor{snapshotRefs: []fsmeta.SnapshotEvidenceRef{ref}}
	publisher := &fakeSnapshotPublisher{}
	client, cleanup := openBufconnClient(t, executor, WithSnapshotPublisher(publisher))
	defer cleanup()

	resp, err := client.SnapshotSubtree(context.Background(), &fsmetapb.SnapshotSubtreeRequest{
		Mount:     "vol",
		RootInode: 42,
	})
	require.NoError(t, err)
	require.Equal(t, fsmeta.SnapshotSubtreeRequest{Mount: "vol", RootInode: 42}, executor.snapshotReq)
	require.Equal(t, uint64(1234), resp.GetReadVersion())
	refs, err := snapshotEvidenceRefsFromProto(resp.GetRuntimeEvidence())
	require.NoError(t, err)
	require.Equal(t, []fsmeta.SnapshotEvidenceRef{ref}, refs)
	require.Equal(t, fsmeta.SnapshotSubtreeToken{
		Mount:           "vol",
		MountKeyID:      1,
		RootInode:       42,
		ReadVersion:     1234,
		RuntimeEvidence: []fsmeta.SnapshotEvidenceRef{ref},
	}, publisher.token)
}

func TestGRPCServiceGetReadVersionDoesNotPublishSnapshot(t *testing.T) {
	executor := &fakeExecutor{}
	publisher := &fakeSnapshotPublisher{}
	client, cleanup := openBufconnClient(t, executor, WithSnapshotPublisher(publisher))
	defer cleanup()

	resp, err := client.GetReadVersion(context.Background(), &fsmetapb.GetReadVersionRequest{Mount: "vol"})
	require.NoError(t, err)
	require.Equal(t, fsmeta.ReadVersionRequest{Mount: "vol"}, executor.readVersionReq)
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
	want := fsmeta.SnapshotSubtreeToken{Mount: "vol", MountKeyID: 1, RootInode: 42, ReadVersion: 1234}
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
		RuntimeEvidence: snapshotEvidenceRefsToProto([]fsmeta.SnapshotEvidenceRef{ref}),
	})
	require.NoError(t, err)
	require.Equal(t, fsmeta.SnapshotSubtreeToken{
		Mount:           "vol",
		MountKeyID:      1,
		RootInode:       42,
		ReadVersion:     1234,
		RuntimeEvidence: []fsmeta.SnapshotEvidenceRef{ref},
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
	require.Equal(t, fsmeta.QuotaUsageRequest{Mount: "vol", Scope: 7}, executor.quotaReq)
	require.Equal(t, uint64(4096), resp.GetBytes())
	require.Equal(t, uint64(2), resp.GetInodes())
}

type fakeSnapshotPublisher struct {
	token       fsmeta.SnapshotSubtreeToken
	retired     fsmeta.SnapshotSubtreeToken
	err         error
	retireError error
}

func (p *fakeSnapshotPublisher) PublishSnapshotSubtree(_ context.Context, token fsmeta.SnapshotSubtreeToken) error {
	p.token = token
	return p.err
}

func (p *fakeSnapshotPublisher) RetireSnapshotSubtree(_ context.Context, token fsmeta.SnapshotSubtreeToken) error {
	p.retired = token
	return p.retireError
}

func testServerSnapshotEvidenceRef(epoch uint64, seed byte) fsmeta.SnapshotEvidenceRef {
	var root [32]byte
	var digest [32]byte
	root[0] = seed
	digest[0] = seed + 1
	return fsmeta.SnapshotEvidenceRef{EpochID: epoch, EvidenceRoot: root, PayloadDigest: digest}
}

type fakeWatcher struct {
	mu  sync.Mutex
	req fsmeta.WatchRequest
	sub *fakeWatchSub
	err error
}

func (w *fakeWatcher) Subscribe(_ context.Context, req fsmeta.WatchRequest) (fsmeta.WatchSubscription, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.req = req
	if w.err != nil {
		return nil, w.err
	}
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
	err    error
	ready  fsmeta.WatchCursor
}

func newFakeWatchSub(buffer int) *fakeWatchSub {
	return &fakeWatchSub{
		events: make(chan fsmeta.WatchEvent, buffer),
		ready:  fsmeta.WatchCursor{RegionID: 1, Term: 2, Index: 3},
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
