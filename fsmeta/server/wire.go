// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"fmt"
	"time"

	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/feichai0017/NoKV/fsmeta/observe"
	fsmetapb "github.com/feichai0017/NoKV/pb/fsmeta"
)

func createRequestFromProto(req *fsmetapb.CreateRequest) model.CreateRequest {
	return model.CreateRequest{
		Mount:  model.MountID(req.GetMount()),
		Parent: model.InodeID(req.GetParent()),
		Name:   req.GetName(),
		Attrs:  createAttrsFromProto(req.GetAttrs()),
	}
}

func createAttrsFromProto(pb *fsmetapb.CreateInodeAttrs) model.CreateAttrs {
	if pb == nil {
		return model.CreateAttrs{}
	}
	return model.CreateAttrs{
		Type:          inodeTypeFromProto(pb.GetType()),
		Size:          pb.GetSize(),
		Mode:          pb.GetMode(),
		CreatedUnixNs: pb.GetCreatedUnixNs(),
		UpdatedUnixNs: pb.GetUpdatedUnixNs(),
		OpaqueAttrs:   append([]byte(nil), pb.GetOpaqueAttrs()...),
	}
}

func updateInodeRequestFromProto(req *fsmetapb.UpdateInodeRequest) model.UpdateInodeRequest {
	if req == nil {
		return model.UpdateInodeRequest{}
	}
	return model.UpdateInodeRequest{
		Mount:            model.MountID(req.GetMount()),
		Parent:           model.InodeID(req.GetParent()),
		Inode:            model.InodeID(req.GetInode()),
		Name:             req.GetName(),
		SetSize:          req.GetSetSize(),
		Size:             req.GetSize(),
		SetMode:          req.GetSetMode(),
		Mode:             req.GetMode(),
		SetUpdatedUnixNs: req.GetSetUpdatedUnixNs(),
		UpdatedUnixNs:    req.GetUpdatedUnixNs(),
		SetOpaqueAttrs:   req.GetSetOpaqueAttrs(),
		OpaqueAttrs:      append([]byte(nil), req.GetOpaqueAttrs()...),
	}
}

func lookupRequestFromProto(req *fsmetapb.LookupRequest) model.LookupRequest {
	return model.LookupRequest{
		Mount:  model.MountID(req.GetMount()),
		Parent: model.InodeID(req.GetParent()),
		Name:   req.GetName(),
	}
}

func readDirRequestFromProto(req *fsmetapb.ReadDirRequest) model.ReadDirRequest {
	return model.ReadDirRequest{
		Mount:           model.MountID(req.GetMount()),
		Parent:          model.InodeID(req.GetParent()),
		StartAfter:      req.GetStartAfter(),
		Limit:           req.GetLimit(),
		SnapshotVersion: req.GetSnapshotVersion(),
	}
}

func getReadVersionRequestFromProto(req *fsmetapb.GetReadVersionRequest) model.ReadVersionRequest {
	if req == nil {
		return model.ReadVersionRequest{}
	}
	return model.ReadVersionRequest{Mount: model.MountID(req.GetMount())}
}

func snapshotSubtreeRequestFromProto(req *fsmetapb.SnapshotSubtreeRequest) model.SnapshotSubtreeRequest {
	if req == nil {
		return model.SnapshotSubtreeRequest{}
	}
	return model.SnapshotSubtreeRequest{
		Mount:     model.MountID(req.GetMount()),
		RootInode: model.InodeID(req.GetRootInode()),
	}
}

func snapshotSubtreeResponseToProto(token model.SnapshotSubtreeToken) *fsmetapb.SnapshotSubtreeResponse {
	return &fsmetapb.SnapshotSubtreeResponse{
		Mount:           string(token.Mount),
		RootInode:       uint64(token.RootInode),
		ReadVersion:     token.ReadVersion,
		RuntimeEvidence: snapshotEvidenceRefsToProto(token.RuntimeEvidence),
	}
}

func retireSnapshotSubtreeRequestFromProto(req *fsmetapb.RetireSnapshotSubtreeRequest) (model.SnapshotSubtreeToken, error) {
	if req == nil {
		return model.SnapshotSubtreeToken{}, nil
	}
	refs, err := snapshotEvidenceRefsFromProto(req.GetRuntimeEvidence())
	if err != nil {
		return model.SnapshotSubtreeToken{}, err
	}
	return model.SnapshotSubtreeToken{
		Mount:           model.MountID(req.GetMount()),
		RootInode:       model.InodeID(req.GetRootInode()),
		ReadVersion:     req.GetReadVersion(),
		RuntimeEvidence: refs,
	}, nil
}

func snapshotEvidenceRefsToProto(refs []model.SnapshotEvidenceRef) []*fsmetapb.SnapshotEvidenceRef {
	if len(refs) == 0 {
		return nil
	}
	out := make([]*fsmetapb.SnapshotEvidenceRef, 0, len(refs))
	for _, ref := range refs {
		out = append(out, &fsmetapb.SnapshotEvidenceRef{
			EpochId:       ref.EpochID,
			EvidenceRoot:  append([]byte(nil), ref.EvidenceRoot[:]...),
			PayloadDigest: append([]byte(nil), ref.PayloadDigest[:]...),
		})
	}
	return out
}

func snapshotEvidenceRefsFromProto(refs []*fsmetapb.SnapshotEvidenceRef) ([]model.SnapshotEvidenceRef, error) {
	if len(refs) == 0 {
		return nil, nil
	}
	out := make([]model.SnapshotEvidenceRef, 0, len(refs))
	for idx, ref := range refs {
		if ref == nil {
			continue
		}
		root := ref.GetEvidenceRoot()
		digest := ref.GetPayloadDigest()
		if len(root) != 32 || len(digest) != 32 {
			return nil, fmt.Errorf("%w: snapshot evidence ref %d epoch=%d root_len=%d digest_len=%d", model.ErrInvalidRequest, idx, ref.GetEpochId(), len(root), len(digest))
		}
		var parsed model.SnapshotEvidenceRef
		parsed.EpochID = ref.GetEpochId()
		copy(parsed.EvidenceRoot[:], root)
		copy(parsed.PayloadDigest[:], digest)
		out = append(out, parsed)
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

func quotaUsageRequestFromProto(req *fsmetapb.QuotaUsageRequest) model.QuotaUsageRequest {
	if req == nil {
		return model.QuotaUsageRequest{}
	}
	return model.QuotaUsageRequest{
		Mount: model.MountID(req.GetMount()),
		Scope: model.InodeID(req.GetScope()),
	}
}

func quotaUsageResponseToProto(record model.UsageRecord) *fsmetapb.QuotaUsageResponse {
	return &fsmetapb.QuotaUsageResponse{
		Bytes:  record.Bytes,
		Inodes: record.Inodes,
	}
}

func renameRequestFromProto(req *fsmetapb.RenameRequest) model.RenameRequest {
	return model.RenameRequest{
		Mount:      model.MountID(req.GetMount()),
		FromParent: model.InodeID(req.GetFromParent()),
		FromName:   req.GetFromName(),
		ToParent:   model.InodeID(req.GetToParent()),
		ToName:     req.GetToName(),
	}
}

func renameReplaceRequestFromProto(req *fsmetapb.RenameReplaceRequest) model.RenameReplaceRequest {
	return model.RenameReplaceRequest{
		Mount:      model.MountID(req.GetMount()),
		FromParent: model.InodeID(req.GetFromParent()),
		FromName:   req.GetFromName(),
		ToParent:   model.InodeID(req.GetToParent()),
		ToName:     req.GetToName(),
	}
}

func renameReplaceResponseToProto(result model.RenameReplaceResult) *fsmetapb.RenameReplaceResponse {
	return &fsmetapb.RenameReplaceResponse{
		Replaced:        result.Replaced,
		OldDentry:       dentryToProto(result.OldDentry),
		OldInode:        inodeToProto(result.OldInode),
		OldInodeDeleted: result.OldInodeDeleted,
	}
}

func renameSubtreeRequestFromProto(req *fsmetapb.RenameSubtreeRequest) model.RenameSubtreeRequest {
	return model.RenameSubtreeRequest{
		Mount:      model.MountID(req.GetMount()),
		FromParent: model.InodeID(req.GetFromParent()),
		FromName:   req.GetFromName(),
		ToParent:   model.InodeID(req.GetToParent()),
		ToName:     req.GetToName(),
	}
}

func linkRequestFromProto(req *fsmetapb.LinkRequest) model.LinkRequest {
	return model.LinkRequest{
		Mount:      model.MountID(req.GetMount()),
		FromParent: model.InodeID(req.GetFromParent()),
		FromName:   req.GetFromName(),
		ToParent:   model.InodeID(req.GetToParent()),
		ToName:     req.GetToName(),
	}
}

func unlinkRequestFromProto(req *fsmetapb.UnlinkRequest) model.UnlinkRequest {
	return model.UnlinkRequest{
		Mount:  model.MountID(req.GetMount()),
		Parent: model.InodeID(req.GetParent()),
		Name:   req.GetName(),
	}
}

func removeRequestFromProto(req *fsmetapb.RemoveRequest) model.RemoveRequest {
	return model.RemoveRequest{
		Mount:  model.MountID(req.GetMount()),
		Parent: model.InodeID(req.GetParent()),
		Name:   req.GetName(),
	}
}

func removeResponseToProto(result model.RemoveResult) *fsmetapb.RemoveResponse {
	return &fsmetapb.RemoveResponse{
		RemovedDentry: dentryToProto(result.RemovedDentry),
		OldInode:      inodeToProto(result.OldInode),
		InodeDeleted:  result.InodeDeleted,
	}
}

func removeDirectoryRequestFromProto(req *fsmetapb.RemoveDirectoryRequest) model.RemoveDirectoryRequest {
	return model.RemoveDirectoryRequest{
		Mount:  model.MountID(req.GetMount()),
		Parent: model.InodeID(req.GetParent()),
		Name:   req.GetName(),
	}
}

func openWriteSessionRequestFromProto(req *fsmetapb.OpenWriteSessionRequest) model.OpenWriteSessionRequest {
	if req == nil {
		return model.OpenWriteSessionRequest{}
	}
	return model.OpenWriteSessionRequest{
		Mount:   model.MountID(req.GetMount()),
		Inode:   model.InodeID(req.GetInode()),
		Session: model.SessionID(req.GetSession()),
		TTL:     time.Duration(req.GetTtlNs()),
	}
}

func heartbeatWriteSessionRequestFromProto(req *fsmetapb.HeartbeatWriteSessionRequest) model.HeartbeatWriteSessionRequest {
	if req == nil {
		return model.HeartbeatWriteSessionRequest{}
	}
	return model.HeartbeatWriteSessionRequest{
		Mount:   model.MountID(req.GetMount()),
		Inode:   model.InodeID(req.GetInode()),
		Session: model.SessionID(req.GetSession()),
		TTL:     time.Duration(req.GetTtlNs()),
	}
}

func closeWriteSessionRequestFromProto(req *fsmetapb.CloseWriteSessionRequest) model.CloseWriteSessionRequest {
	if req == nil {
		return model.CloseWriteSessionRequest{}
	}
	return model.CloseWriteSessionRequest{
		Mount:   model.MountID(req.GetMount()),
		Inode:   model.InodeID(req.GetInode()),
		Session: model.SessionID(req.GetSession()),
	}
}

func expireWriteSessionsRequestFromProto(req *fsmetapb.ExpireWriteSessionsRequest) model.ExpireWriteSessionsRequest {
	if req == nil {
		return model.ExpireWriteSessionsRequest{}
	}
	return model.ExpireWriteSessionsRequest{
		Mount: model.MountID(req.GetMount()),
		Limit: req.GetLimit(),
	}
}

func dentryToProto(record model.DentryRecord) *fsmetapb.DentryRecord {
	return &fsmetapb.DentryRecord{
		Parent: uint64(record.Parent),
		Name:   record.Name,
		Inode:  uint64(record.Inode),
		Type:   inodeTypeToProto(record.Type),
	}
}

func inodeToProto(record model.InodeRecord) *fsmetapb.InodeRecord {
	return &fsmetapb.InodeRecord{
		Inode:         uint64(record.Inode),
		Type:          inodeTypeToProto(record.Type),
		Size:          record.Size,
		Mode:          record.Mode,
		LinkCount:     record.LinkCount,
		ChildCount:    record.ChildCount,
		CreatedUnixNs: record.CreatedUnixNs,
		UpdatedUnixNs: record.UpdatedUnixNs,
		OpaqueAttrs:   append([]byte(nil), record.OpaqueAttrs...),
	}
}

func sessionToProto(record model.SessionRecord) *fsmetapb.SessionRecord {
	return &fsmetapb.SessionRecord{
		Session:       string(record.Session),
		Inode:         uint64(record.Inode),
		ExpiresUnixNs: record.ExpiresUnixNs,
	}
}

func pairToProto(pair model.DentryAttrPair) *fsmetapb.DentryAttrPair {
	return &fsmetapb.DentryAttrPair{
		Dentry: dentryToProto(pair.Dentry),
		Inode:  inodeToProto(pair.Inode),
	}
}

func watchRequestFromProto(req *fsmetapb.WatchSubtreeRequest) observe.WatchRequest {
	if req == nil {
		return observe.WatchRequest{}
	}
	return observe.WatchRequest{
		Mount:              model.MountID(req.GetMount()),
		RootInode:          model.InodeID(req.GetRootInode()),
		KeyPrefix:          append([]byte(nil), req.GetKeyPrefix()...),
		DescendRecursively: req.GetDescendRecursively(),
		ResumeCursor:       watchCursorFromProto(req.GetResumeCursor()),
		BackPressureWindow: req.GetBackPressureWindow(),
	}
}

func watchCursorFromProto(cursor *fsmetapb.WatchCursor) observe.WatchCursor {
	if cursor == nil {
		return observe.WatchCursor{}
	}
	return observe.WatchCursor{
		RegionID: cursor.GetRegionId(),
		Term:     cursor.GetTerm(),
		Index:    cursor.GetIndex(),
	}
}

func watchCursorToProto(cursor observe.WatchCursor) *fsmetapb.WatchCursor {
	return &fsmetapb.WatchCursor{
		RegionId: cursor.RegionID,
		Term:     cursor.Term,
		Index:    cursor.Index,
	}
}

func watchEventToProto(evt observe.WatchEvent) *fsmetapb.WatchEvent {
	return &fsmetapb.WatchEvent{
		RaftCursor:    watchCursorToProto(evt.Cursor),
		CommitVersion: evt.CommitVersion,
		Source:        watchEventSourceToProto(evt.Source),
		Key:           append([]byte(nil), evt.Key...),
	}
}

func watchEventSourceToProto(source observe.WatchEventSource) fsmetapb.WatchEventSource {
	switch source {
	case observe.WatchEventSourceCommit:
		return fsmetapb.WatchEventSource_WATCH_EVENT_SOURCE_COMMIT
	case observe.WatchEventSourceResolveLock:
		return fsmetapb.WatchEventSource_WATCH_EVENT_SOURCE_RESOLVE_LOCK
	case observe.WatchEventSourceRuntimeVisible:
		return fsmetapb.WatchEventSource_WATCH_EVENT_SOURCE_RUNTIME_VISIBLE
	default:
		return fsmetapb.WatchEventSource_WATCH_EVENT_SOURCE_UNSPECIFIED
	}
}

func inodeTypeFromProto(typ fsmetapb.InodeType) model.InodeType {
	switch typ {
	case fsmetapb.InodeType_INODE_TYPE_FILE:
		return model.InodeTypeFile
	case fsmetapb.InodeType_INODE_TYPE_DIRECTORY:
		return model.InodeTypeDirectory
	default:
		return ""
	}
}

func inodeTypeToProto(typ model.InodeType) fsmetapb.InodeType {
	switch typ {
	case model.InodeTypeFile:
		return fsmetapb.InodeType_INODE_TYPE_FILE
	case model.InodeTypeDirectory:
		return fsmetapb.InodeType_INODE_TYPE_DIRECTORY
	default:
		return fsmetapb.InodeType_INODE_TYPE_UNSPECIFIED
	}
}
